import time
import psycopg2
import random
from datetime import datetime, timedelta

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "kafka_events",
    "user": "admin",
    "password": "admin123"
}

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
        if table_has_data(conn):
            print("kafka_messages already contains data. Skipping seed.")
        else:
            seed_data(conn)
            print("Fake seed data inserted successfully.")
    except Exception as e:
        print(f"Seed error: {e}")
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()