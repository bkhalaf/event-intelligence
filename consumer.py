from kafka import KafkaConsumer
import psycopg2
import json
import sys

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

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_message(conn, message_data):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO kafka_messages (original_message, transformed_message, processed_at)
        VALUES (%s, %s, %s)
        """,
        (
            message_data.get('original_message'),
            message_data.get('transformed_message'),
            message_data.get('processed_at'),
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
print("Waiting for messages... (Press Ctrl+C to stop)")

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")

    for message in consumer:
        print(f"Received: {message.value}")
        print(f"  Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")

        try:
            insert_message(conn, message.value)
            print("  -> Saved to PostgreSQL")
        except Exception as db_error:
            print(f"  -> DB Error: {db_error}")
            conn = get_db_connection()

        print("-" * 50)

except KeyboardInterrupt:
    print("\nConsumer stopped by user")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()
    if 'conn' in locals():
        conn.close()
    print("Consumer and database connection closed")
