from kafka import KafkaConsumer
from langfuse import Langfuse
import requests
import psycopg2
import json
import sys
import os
import time

# Get topic name from command line argument, default to 'output-topic'
TOPIC_NAME = sys.argv[1] if len(sys.argv) > 1 else 'output-topic'

# PostgreSQL connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'kafka_events',
    'user': 'admin',
    'password': 'admin123'
}

# Ollama configuration
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "gemma3:1b"

# Langfuse configuration
langfuse = Langfuse(
    public_key=os.getenv('LANGFUSE_PUBLIC_KEY'),
    secret_key=os.getenv('LANGFUSE_SECRET_KEY'),
    host="http://localhost:3000"
)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def analyze_with_llm(message_text, trace_name):
    """Send message to Ollama for analysis with Langfuse tracing"""
    start_time = time.time()

    prompt = f"""You are a message analyzer. Analyze the given message and provide:
1) Category (e.g., greeting, question, command, information)
2) Sentiment (positive/negative/neutral)
3) Brief summary

Keep response concise in JSON format.

Analyze this message: {message_text}"""

    # Create Langfuse trace (v2 API)
    trace = langfuse.trace(
        name=trace_name,
        metadata={"source": "kafka-consumer", "topic": TOPIC_NAME}
    )

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": OLLAMA_MODEL,
                "prompt": prompt,
                "stream": False
            },
            timeout=60
        )
        response.raise_for_status()
        result = response.json().get("response", "No response")

        latency_ms = int((time.time() - start_time) * 1000)

        # Log generation to Langfuse (v2 API - create from trace)
        generation = trace.generation(
            name="ollama-analysis",
            model=OLLAMA_MODEL,
            input=prompt,
            output=result,
            metadata={"latency_ms": latency_ms}
        )
        generation.end()

        return result

    except Exception as e:
        error_msg = f"LLM Error: {str(e)}"
        generation = trace.generation(
            name="ollama-analysis",
            model=OLLAMA_MODEL,
            input=prompt,
            output=error_msg,
            metadata={"error": str(e)}
        )
        generation.end()
        return error_msg

def insert_message(conn, message_data, ai_analysis):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO kafka_messages (original_message, transformed_message, processed_at, ai_analysis)
        VALUES (%s, %s, %s, %s)
        """,
        (
            message_data.get('original_message'),
            message_data.get('transformed_message'),
            message_data.get('processed_at'),
            ai_analysis
        )
    )
    conn.commit()
    cursor.close()

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f'consumer-{TOPIC_NAME}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Starting consumer, listening to topic: {TOPIC_NAME}")
print(f"Using Ollama ({OLLAMA_MODEL}) for AI analysis")
print("Langfuse tracing: http://localhost:3000")
print("Waiting for messages... (Press Ctrl+C to stop)")

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")

    for message in consumer:
        print(f"Received: {message.value}")
        print(f"  Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")

        # Generate trace name
        trace_name = f"kafka-{message.topic}-{message.partition}-{message.offset}"

        # Get AI analysis
        original_msg = message.value.get('original_message', '')
        print(f"  -> Analyzing with Ollama (trace: {trace_name})...")
        ai_analysis = analyze_with_llm(original_msg, trace_name)
        print(f"  -> AI Analysis: {ai_analysis}")

        # Write to PostgreSQL
        try:
            insert_message(conn, message.value, ai_analysis)
            print("  -> Saved to PostgreSQL")
        except Exception as db_error:
            print(f"  -> DB Error: {db_error}")
            conn = get_db_connection()

        # Flush Langfuse
        langfuse.flush()

        print("-" * 50)

except KeyboardInterrupt:
    print("\nConsumer stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    langfuse.shutdown()
    if 'conn' in locals():
        conn.close()
    print("Consumer and database connection closed")
