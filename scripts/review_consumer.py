from kafka import KafkaConsumer
import psycopg2
import json
import sys
import os
import yaml

# Get topic name from command line argument, default to 'customer-review'
TOPIC_NAME = sys.argv[1] if len(sys.argv) > 1 else 'customer-review'

# PostgreSQL connection
DB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(DB_CONFIG_PATH, 'r') as f:
    DB_CONFIG = yaml.safe_load(f)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def insert_review(conn, review_data):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO product_reviews (product_id, product_name, customer_name, rating, review_text)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            review_data.get('product_id'),
            review_data.get('product_name'),
            review_data.get('customer_name'),
            review_data.get('rating'),
            review_data.get('review_text'),
        )
    )
    conn.commit()
    cursor.close()

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=['kafka:29092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=f'consumer-{TOPIC_NAME}',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Starting review consumer, listening to topic: {TOPIC_NAME}")
print("Waiting for messages... (Press Ctrl+C to stop)")

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")

    for message in consumer:
        print(f"Received: {message.value}")
        print(f"  Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}")

        try:
            insert_review(conn, message.value)
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
