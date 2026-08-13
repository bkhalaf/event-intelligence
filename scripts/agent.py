import sys
sys.stdout.reconfigure(line_buffering=True)
from kafka import KafkaConsumer
import psycopg2
import json
import requests
import yaml
import os
import time
import re
from concurrent.futures import ThreadPoolExecutor

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'agent_config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

TOPIC_NAME = config['kafka']['topic']

ACTIVE_PROVIDER = config['active_provider']
PROVIDER_CONFIG = config['providers'][ACTIVE_PROVIDER]

# Number of messages processed concurrently. Kept as a simple constant for now;
# could be moved into agent_config.yaml later if it needs to be tuned per environment.
MAX_WORKERS = 5

DB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(DB_CONFIG_PATH, 'r') as f:
    DB_CONFIG = yaml.safe_load(f)


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def build_prompt(message):
    return f"""
You are an AI agent analyzing a customer order.
Analyze this order and provide a brief analysis:

Order: {message}

Provide a short analysis of this order in 2-3 sentences.
"""

def analyze_with_gemini(message):
    headers = {
        'x-goog-api-key': PROVIDER_CONFIG['api_key'],
        'Content-Type': 'application/json'
    }
    body = {
        'contents': [
            {'parts': [{'text': build_prompt(message)}]}
        ]
    }
    response = requests.post(PROVIDER_CONFIG['url'], headers=headers, json=body)
    result = response.json()

    if 'candidates' not in result:
        print(f"Gemini raw response (status {response.status_code}): {result}")
        raise RuntimeError(f"Gemini API did not return candidates: {result}")

    return result['candidates'][0]['content']['parts'][0]['text']


def analyze_with_ollama(message):
    response = requests.post(PROVIDER_CONFIG['url'], json={
        'model': PROVIDER_CONFIG['model'],
        'prompt': build_prompt(message),
        'stream': False
    })
    result = response.json()
    return result.get('response', 'No analysis available')


def analyze_with_groq(message):
    headers = {
        'Authorization': f"Bearer {PROVIDER_CONFIG['api_key']}",
        'Content-Type': 'application/json'
    }
    body = {
        'model': PROVIDER_CONFIG['model'],
        'messages': [
            {'role': 'user', 'content': build_prompt(message)}
        ]
    }
    response = requests.post(PROVIDER_CONFIG['url'], headers=headers, json=body)
    result = response.json()
    return result['choices'][0]['message']['content']


def analyze_with_openai(message):
    headers = {
        'Authorization': f"Bearer {PROVIDER_CONFIG['api_key']}",
        'Content-Type': 'application/json'
    }
    body = {
        'model': PROVIDER_CONFIG['model'],
        'messages': [
            {'role': 'user', 'content': build_prompt(message)}
        ]
    }
    response = requests.post(PROVIDER_CONFIG['url'], headers=headers, json=body)
    result = response.json()
    return result['choices'][0]['message']['content']


PROVIDER_HANDLERS = {
    'gemini': analyze_with_gemini,
    'ollama': analyze_with_ollama,
    'groq': analyze_with_groq,
    'openai': analyze_with_openai,
}


def analyze_with_model(message, max_retries=3):
    handler = PROVIDER_HANDLERS.get(ACTIVE_PROVIDER)
    if handler is None:
        raise ValueError(f"Unknown provider: {ACTIVE_PROVIDER}")

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            return handler(message)
        except Exception as e:
            last_error = e
            # If the provider's error response includes its own recommended
            # retry delay (as Gemini does), honor that instead of guessing.
            match = re.search(r"'retryDelay': '(\d+)s'", str(e))
            wait_seconds = int(match.group(1)) + 1 if match else 2 ** attempt
            print(f"Attempt {attempt}/{max_retries} failed: {e}")
            if attempt < max_retries:
                print(f"Retrying in {wait_seconds}s...")
                time.sleep(wait_seconds)

    raise last_error


def save_result(original_message, analysis, duration_seconds):
    # Each worker thread opens and closes its own connection: psycopg2
    # connections are not safe to share across concurrently executing threads.
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO agent_results (original_message, agent_analysis)
            VALUES (%s, %s)
            """,
            (original_message, analysis)
        )
        conn.commit()
        cursor.close()
        print(f"Analysis took {duration_seconds:.2f} seconds (provider: {ACTIVE_PROVIDER})")
    finally:
        conn.close()


def process_message(message_value):
    original = message_value.get('original_message', str(message_value))
    print(f"Analyzing with {ACTIVE_PROVIDER}...")
    start_time = time.monotonic()
    try:
        analysis = analyze_with_model(original)
        duration = time.monotonic() - start_time
        print(f"Analysis: {analysis}")
        save_result(original, analysis, duration)
        print("Saved to PostgreSQL")
    except Exception as msg_error:
        print(f"Failed to process message after retries: {msg_error}")
    print("-" * 50)


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[config['kafka']['bootstrap_servers']],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config['kafka']['group_id'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"AI Agent started, listening to topic: {TOPIC_NAME}")
print(f"Using provider: {ACTIVE_PROVIDER} | model: {PROVIDER_CONFIG.get('model')}")
print(f"Concurrency: {MAX_WORKERS} worker threads")
print("Waiting for messages...")

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

try:
    for message in consumer:
        print(f"Received message: {message.value}")
        # Hand off to a worker thread immediately and go back to reading
        # the next message right away, instead of blocking here.
        executor.submit(process_message, message.value)
except KeyboardInterrupt:
    print("\nAgent stopped")
except Exception as e:
    print(f"Error: {e}")
finally:
    print("Waiting for in-flight analyses to finish...")
    executor.shutdown(wait=True)
    consumer.close()