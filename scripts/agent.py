import sys
sys.stdout.reconfigure(line_buffering=True)
from kafka import KafkaConsumer
import psycopg2
import json
import requests

TOPIC_NAME = 'output-topic'
OLLAMA_URL = 'http://host.docker.internal:11434/api/generate'
OLLAMA_MODEL = 'llama3'

DB_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'kafka_events',
    'user': 'admin',
    'password': 'admin123'
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
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='agent-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"AI Agent started, listening to topic: {TOPIC_NAME}")
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