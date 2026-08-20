import time
import psycopg2
import random
import os
import yaml
from datetime import datetime, timedelta

DB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(DB_CONFIG_PATH, 'r') as f:
    DB_CONFIG = yaml.safe_load(f)

CUSTOMERS = ["Ahmad", "Sara", "Lina", "Omar", "Noor", "Malak", "Yousef", "Maha"]
BRANCHES = ["Gaza", "Rafah", "Khan Younis", "Deir al-Balah"]
PAYMENTS = ["Cash", "Card", "Jawwal Pay"]
PRODUCTS = [
    "Gaming Mouse",
    "Smart Watch",
    "Wireless Headphones",
    "Mechanical Keyboard",
    "Anker Power Bank",
    "Lenovo IdeaPad Laptop"
]
NOTES = ["No notes", "Urgent delivery", "Customer will pick up", "Call before delivery"]

REVIEW_SEED_DATA = [
    # Lenovo IdeaPad Laptop
    {
        "product_id": "E1001",
        "product_name": "Lenovo IdeaPad Laptop",
        "customer_name": "Sara",
        "rating": 5,
        "review_text": "Great laptop for studying and programming. The performance is excellent."
    },
    {
        "product_id": "E1001",
        "product_name": "Lenovo IdeaPad Laptop",
        "customer_name": "Omar",
        "rating": 4,
        "review_text": "Good performance and a nice screen. I use it daily for university work."
    },
    {
        "product_id": "E1001",
        "product_name": "Lenovo IdeaPad Laptop",
        "customer_name": "Lina",
        "rating": 3,
        "review_text": "The laptop works fine, but it is heavier than I expected."
    },
    {
        "product_id": "E1001",
        "product_name": "Lenovo IdeaPad Laptop",
        "customer_name": "Ahmad",
        "rating": 2,
        "review_text": "Performance is acceptable, but the battery does not last as long as I expected."
    },
    {
        "product_id": "E1001",
        "product_name": "Lenovo IdeaPad Laptop",
        "customer_name": "Noor",
        "rating": 5,
        "review_text": "Very reliable laptop with great performance for development tasks."
    },

    # Anker Power Bank
    {
        "product_id": "E1002",
        "product_name": "Anker Power Bank",
        "customer_name": "Maha",
        "rating": 5,
        "review_text": "Excellent power bank. The battery capacity is great and it is very useful while traveling."
    },
    {
        "product_id": "E1002",
        "product_name": "Anker Power Bank",
        "customer_name": "Yousef",
        "rating": 4,
        "review_text": "Reliable and easy to carry. It charges my phone more than once."
    },
    {
        "product_id": "E1002",
        "product_name": "Anker Power Bank",
        "customer_name": "Malak",
        "rating": 3,
        "review_text": "It works well, but charging the power bank itself takes a long time."
    },
    {
        "product_id": "E1002",
        "product_name": "Anker Power Bank",
        "customer_name": "Sara",
        "rating": 2,
        "review_text": "The capacity is okay, but it feels a little heavy for carrying every day."
    },
    {
        "product_id": "E1002",
        "product_name": "Anker Power Bank",
        "customer_name": "Omar",
        "rating": 1,
        "review_text": "I expected better charging speed. It did not meet my expectations."
    },

    # Wireless Headphones
    {
        "product_id": "E1003",
        "product_name": "Wireless Headphones",
        "customer_name": "Noor",
        "rating": 5,
        "review_text": "Excellent sound quality and very comfortable even after using them for hours."
    },
    {
        "product_id": "E1003",
        "product_name": "Wireless Headphones",
        "customer_name": "Lina",
        "rating": 4,
        "review_text": "Good sound and battery life for the price."
    },
    {
        "product_id": "E1003",
        "product_name": "Wireless Headphones",
        "customer_name": "Ahmad",
        "rating": 3,
        "review_text": "Sound quality is fine, but the microphone could be better."
    },
    {
        "product_id": "E1003",
        "product_name": "Wireless Headphones",
        "customer_name": "Maha",
        "rating": 2,
        "review_text": "They sound good, but they become uncomfortable after long use."
    },
    {
        "product_id": "E1003",
        "product_name": "Wireless Headphones",
        "customer_name": "Yousef",
        "rating": 1,
        "review_text": "The connection sometimes drops and the microphone quality is disappointing."
    },

    # Gaming Mouse
    {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Yousef",
        "rating": 5,
        "review_text": "Very responsive mouse and comfortable for gaming."
    },
    {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Malak",
        "rating": 4,
        "review_text": "Good accuracy and comfortable design. I like it overall."
    },
    {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Omar",
        "rating": 3,
        "review_text": "It works fine for normal use, but the build quality feels average."
    },
    {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Sara",
        "rating": 2,
        "review_text": "The mouse is responsive, but the buttons feel a little cheap."
    },
    {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Noor",
        "rating": 4,
        "review_text": "Nice gaming mouse for the price and the tracking is accurate."
    },

    # Mechanical Keyboard
    {
        "product_id": "E1005",
        "product_name": "Mechanical Keyboard",
        "customer_name": "Omar",
        "rating": 5,
        "review_text": "Excellent keyboard. The keys feel great and the response is very fast."
    },
    {
        "product_id": "E1005",
        "product_name": "Mechanical Keyboard",
        "customer_name": "Sara",
        "rating": 4,
        "review_text": "Very comfortable for typing and programming."
    },
    {
        "product_id": "E1005",
        "product_name": "Mechanical Keyboard",
        "customer_name": "Maha",
        "rating": 3,
        "review_text": "The keyboard feels good, but the keys are louder than I prefer."
    },
    {
        "product_id": "E1005",
        "product_name": "Mechanical Keyboard",
        "customer_name": "Ahmad",
        "rating": 2,
        "review_text": "Typing feels nice, but the keyboard is too noisy for my workspace."
    },
    {
        "product_id": "E1005",
        "product_name": "Mechanical Keyboard",
        "customer_name": "Lina",
        "rating": 5,
        "review_text": "Great build quality and very satisfying keys. I would recommend it."
    },

    # Smart Watch
    {
        "product_id": "E1006",
        "product_name": "Smart Watch",
        "customer_name": "Lina",
        "rating": 5,
        "review_text": "Great display and useful fitness features. I use it every day."
    },
    {
        "product_id": "E1006",
        "product_name": "Smart Watch",
        "customer_name": "Noor",
        "rating": 4,
        "review_text": "Good battery life and the notifications are very useful."
    },
    {
        "product_id": "E1006",
        "product_name": "Smart Watch",
        "customer_name": "Malak",
        "rating": 3,
        "review_text": "It is useful for basic tracking, but some measurements are not always accurate."
    },
    {
        "product_id": "E1006",
        "product_name": "Smart Watch",
        "customer_name": "Yousef",
        "rating": 2,
        "review_text": "The design is nice, but the fitness tracking needs improvement."
    },
    {
        "product_id": "E1006",
        "product_name": "Smart Watch",
        "customer_name": "Maha",
        "rating": 1,
        "review_text": "The watch sometimes disconnects from my phone and the tracking is unreliable."
    }
]


SEED_COUNT = 12


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def table_has_data(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM kafka_messages;")
        count = cursor.fetchone()[0]
        return count > 0


def generate_order_message():
    customer = random.choice(CUSTOMERS)
    branch = random.choice(BRANCHES)
    payment = random.choice(PAYMENTS)
    notes = random.choice(NOTES)

    selected_products = random.sample(PRODUCTS, random.randint(1, 3))
    items_summary = ", ".join([f"{product} x1" for product in selected_products])

    original = (
        f"Order from {customer} | "
        f"Branch: {branch} | "
        f"Payment: {payment} | "
        f"Items: {items_summary} | "
        f"Notes: {notes}"
    )

    transformed = original.upper()
    return original, transformed


def seed_data(conn):
    now = datetime.now()

    with conn.cursor() as cursor:
        for i in range(SEED_COUNT):
            original_message, transformed_message = generate_order_message()
            processed_at = now - timedelta(minutes=(SEED_COUNT - i) * 3)

            cursor.execute(
                """
                INSERT INTO kafka_messages (original_message, transformed_message, processed_at)
                VALUES (%s, %s, %s)
                """,
                (original_message, transformed_message, processed_at)
            )

    conn.commit()


def seed_reviews(conn):
    now = datetime.now()
    inserted_count = 0

    with conn.cursor() as cursor:
        for i, review in enumerate(REVIEW_SEED_DATA):
            cursor.execute(
                """
                SELECT 1
                FROM product_reviews
                WHERE product_id = %s
                  AND customer_name = %s
                  AND rating = %s
                  AND review_text = %s
                LIMIT 1
                """,
                (
                    review["product_id"],
                    review["customer_name"],
                    review["rating"],
                    review["review_text"]
                )
            )

            if cursor.fetchone():
                continue

            submitted_at = now - timedelta(
                minutes=(len(REVIEW_SEED_DATA) - i) * 2
            )

            cursor.execute(
                """
                INSERT INTO product_reviews (
                    product_id,
                    product_name,
                    customer_name,
                    rating,
                    review_text,
                    submitted_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    review["product_id"],
                    review["product_name"],
                    review["customer_name"],
                    review["rating"],
                    review["review_text"],
                    submitted_at
                )
            )

            inserted_count += 1

    conn.commit()
    return inserted_count

    
def main():
    conn = None

    for attempt in range(10):
        try:
            conn = get_db_connection()
            print("Connected to PostgreSQL")
            break
        except Exception as e:
            print(f"Waiting for PostgreSQL... attempt {attempt + 1}/10")
            time.sleep(5)

    if conn is None:
        print("Seed error: Could not connect to PostgreSQL after multiple attempts.")
        return

    try:
        # Seed demo orders
        if table_has_data(conn):
            print("kafka_messages already contains data. Skipping order seed.")
        else:
            seed_data(conn)
            print("Fake order seed data inserted successfully.")

        # Seed demo product reviews
        inserted_reviews = seed_reviews(conn)

        if inserted_reviews > 0:
            print(
                f"Inserted {inserted_reviews} missing product review seed records."
            )
        else:
            print("All product review seed records already exist.")

    except Exception as e:
        print(f"Seed error: {e}")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()