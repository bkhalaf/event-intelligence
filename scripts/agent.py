import sys
sys.stdout.reconfigure(line_buffering=True)
from kafka import KafkaConsumer
import psycopg2
import json
import requests
import yaml
import os
import time

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'agent_config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

TOPIC_NAME = config['kafka']['topic']

PROVIDERS = config['providers']
PROVIDER_ROUTING_ORDER = config['provider_routing_order']
REQUEST_TIMEOUT = config['request_timeout_seconds']
MAX_RETRIES_PER_PROVIDER = config['max_retries_per_provider']

db_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(db_config_path, 'r') as f:
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

def analyze_with_provider(message, provider_name):
    provider_config = PROVIDERS[provider_name]

    url = provider_config['url']
    model = provider_config.get('model')
    api_key = provider_config.get('api_key')

    prompt = build_prompt(message)

    # Gemini
    if provider_name == 'gemini':
        headers = {
            'x-goog-api-key': api_key,
            'Content-Type': 'application/json'
        }

        body = {
            'contents': [
                {
                    'parts': [
                        {'text': prompt}
                    ]
                }
            ]
        }

        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        result = response.json()

        return result['candidates'][0]['content']['parts'][0]['text']

    # OpenAI and Groq
    elif provider_name in ['openai', 'groq']:
        headers = {
            'Authorization': f"Bearer {api_key}",
            'Content-Type': 'application/json'
        }

        body = {
            'model': model,
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ]
        }

        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        result = response.json()

        return result['choices'][0]['message']['content']

    # Ollama
    elif provider_name == 'ollama':
        body = {
            'model': model,
            'prompt': prompt,
            'stream': False
        }

        response = requests.post(
            url,
            json=body,
            timeout=REQUEST_TIMEOUT
        )

        response.raise_for_status()

        result = response.json()

        return result.get(
            'response',
            'No analysis available'
        )

    else:
        raise ValueError(
            f"Unsupported provider: {provider_name}"
        )


def analyze_with_model(message):
    last_error = None

    for provider_name in PROVIDER_ROUTING_ORDER:
        print(f"\nTrying provider: {provider_name}")

        for attempt in range(1, MAX_RETRIES_PER_PROVIDER + 1):
            try:
                analysis = analyze_with_provider(
                    message,
                    provider_name
                )

                print(
                    f"{provider_name} succeeded "
                    f"on attempt {attempt}"
                )

                return analysis, provider_name

            except Exception as e:
                last_error = e

                print(
                    f"{provider_name} failed "
                    f"on attempt "
                    f"{attempt}/{MAX_RETRIES_PER_PROVIDER}: {e}"
                )

                if attempt < MAX_RETRIES_PER_PROVIDER:
                    wait_seconds = 2 ** attempt

                    print(
                        f"Retrying {provider_name} "
                        f"in {wait_seconds} seconds..."
                    )

                    time.sleep(wait_seconds)

        print(
            f"{provider_name} is unavailable. "
            f"Moving to the next provider..."
        )

    raise RuntimeError(
        "All providers failed. "
        f"Last error: {last_error}"
    )


def save_result(
    conn,
    original_message,
    analysis,
    duration_seconds,
    used_provider
):
    cursor = conn.cursor()

    cursor.execute(
        """
        INSERT INTO agent_results (
            original_message,
            agent_analysis
        )
        VALUES (%s, %s)
        """,
        (
            original_message,
            analysis
        )
    )

    conn.commit()
    cursor.close()

    print(
        f"Analysis took {duration_seconds:.2f} seconds "
        f"(provider used: {used_provider})"
    )

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[config['kafka']['bootstrap_servers']],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config['kafka']['group_id'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"AI Agent started, listening to topic: {TOPIC_NAME}")
print(
    "Provider routing order: "
    + " -> ".join(PROVIDER_ROUTING_ORDER)
)
print("Waiting for messages...")

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")
    for message in consumer:
        print(f"Received message: {message.value}")
        original = message.value.get('original_message', str(message.value))

        try:
            print("Analyzing message using provider routing...")
            start_time = time.time()
            analysis, used_provider = analyze_with_model(original)
            duration = time.time() - start_time

            print(f"Analysis: {analysis}")
            print(f"Provider used: {used_provider}")
            save_result(conn, original, analysis, duration, used_provider)
            print("Saved to PostgreSQL")
        except Exception as msg_error:
            print(f"Failed to process message after retries: {msg_error}")

        print("-" * 50)
except KeyboardInterrupt:
    print("\nAgent stopped")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    if 'conn' in locals():
        conn.close()