import sys
sys.stdout.reconfigure(line_buffering=True)
from kafka import KafkaConsumer
import psycopg2
import json
import requests
import yaml
import os

# Load config
config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'agent_config.yaml')
with open(config_path, 'r') as f:
    config = yaml.safe_load(f)

TOPIC_NAME = config['kafka']['topic']
OLLAMA_URL = config['ollama']['url']
OLLAMA_MODEL = config['ollama']['model']

DB_CONFIG = {
    'host': config['database']['host'],
    'port': config['database']['port'],
    'database': config['database']['name'],
    'user': config['database']['user'],
    'password': config['database']['password']
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def analyze_with_ollama(message):
    prompt = f"""
You are an AI agent analyzing a customer order.
Analyze this order and provide a brief analysis:

Order: {message}

Provide a short analysis of this order in 2-3 sentences.
"""
    response = requests.post(OLLAMA_URL, json={
        'model': OLLAMA_MODEL,
        'prompt': prompt,
        'stream': False
    })
    result = response.json()
    return result.get('response', 'No analysis available')

def save_result(conn, original_message, analysis):
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

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=[config['kafka']['bootstrap_servers']],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config['kafka']['group_id'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"AI Agent started, listening to topic: {TOPIC_NAME}")
print(f"Using ollama model: {OLLAMA_MODEL}")
print("Waiting for messages...")

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")
    for message in consumer:
        print(f"Received message: {message.value}")
        original = message.value.get('original_message', str(message.value))
        print("Analyzing with ollama...")
        analysis = analyze_with_ollama(original)
        print(f"Analysis: {analysis}")
        save_result(conn, original, analysis)
        print("Saved to PostgreSQL")
        print("-" * 50)
except KeyboardInterrupt:
    print("\nAgent stopped")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    if 'conn' in locals():
        conn.close()