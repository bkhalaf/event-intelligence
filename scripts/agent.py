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

TOPIC_NAMES = config['kafka']['topics']

PROVIDERS = config['providers']
PROVIDER_ROUTING_ORDER = config['provider_routing_order']
REQUEST_TIMEOUT = config['request_timeout_seconds']
MAX_RETRIES_PER_PROVIDER = config['max_retries_per_provider']

# Number of order messages processed concurrently. Kept as a simple constant for
# now; could be moved into agent_config.yaml later if it needs to be tuned per
# environment. Only the order-processing path (output-topic) is parallelized;
# review-summary processing stays sequential to avoid a race condition on the
# product_review_summaries upsert when multiple reviews for the same product
# arrive close together.
MAX_WORKERS = 5

db_config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(db_config_path, 'r') as f:
    DB_CONFIG = yaml.safe_load(f)


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_product_reviews(conn, product_id):
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT customer_name, rating, review_text
        FROM product_reviews
        WHERE product_id = %s
        ORDER BY submitted_at ASC
        """,
        (product_id,)
    )

    reviews = cursor.fetchall()
    cursor.close()

    return reviews


def build_order_prompt(message):
    return f"""
You are an AI agent analyzing a customer order.
Analyze this order and provide a brief analysis:

Order: {message}

Provide a short analysis of this order in 2-3 sentences.
"""


def build_review_summary_prompt(product_name, reviews):
    review_lines = []

    for customer_name, rating, review_text in reviews:
        customer = customer_name or "Anonymous"
        text = review_text or "No written review"

        review_lines.append(
            f"- Customer: {customer} | "
            f"Rating: {rating}/5 | "
            f"Review: {text}"
        )

    formatted_reviews = "\n".join(review_lines)

    return f"""
You are an AI agent summarizing customer reviews for a product.

Product: {product_name}

Customer reviews:
{formatted_reviews}

Write a concise summary in 2-3 sentences that:
- describes the overall customer opinion,
- mentions the most common positive points,
- mentions the most common complaints if they exist,
- considers both the star ratings and written reviews,
- does not invent any information.
"""


def analyze_with_provider(prompt, provider_name):
    provider_config = PROVIDERS[provider_name]

    url = provider_config['url']
    model = provider_config.get('model')
    api_key = provider_config.get('api_key')

    # Gemini
    if provider_name == 'gemini':
        headers = {
            'x-goog-api-key': api_key,
            'Content-Type': 'application/json'
        }
        body = {
            'contents': [
                {'parts': [{'text': prompt}]}
            ]
        }
        response = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT)

        # Raised manually (instead of response.raise_for_status()) so the full
        # response body, including any provider-reported retry delay, is
        # preserved in the exception message for analyze_with_model to parse.
        if response.status_code != 200:
            raise RuntimeError(f"gemini API error (status {response.status_code}): {response.text}")

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
                {'role': 'user', 'content': prompt}
            ]
        }
        response = requests.post(url, headers=headers, json=body, timeout=REQUEST_TIMEOUT)

        if response.status_code != 200:
            raise RuntimeError(f"{provider_name} API error (status {response.status_code}): {response.text}")

        result = response.json()
        return result['choices'][0]['message']['content']

    # Ollama
    elif provider_name == 'ollama':
        body = {
            'model': model,
            'prompt': prompt,
            'stream': False
        }
        response = requests.post(url, json=body, timeout=REQUEST_TIMEOUT)

        if response.status_code != 200:
            raise RuntimeError(f"ollama API error (status {response.status_code}): {response.text}")

        result = response.json()
        return result.get('response', 'No analysis available')

    else:
        raise ValueError(f"Unsupported provider: {provider_name}")


def analyze_with_model(prompt):
    """Tries each provider in PROVIDER_ROUTING_ORDER in turn, retrying each one
    up to MAX_RETRIES_PER_PROVIDER times before moving to the next provider."""
    last_error = None

    for provider_name in PROVIDER_ROUTING_ORDER:
        print(f"\nTrying provider: {provider_name}")

        for attempt in range(1, MAX_RETRIES_PER_PROVIDER + 1):
            try:
                analysis = analyze_with_provider(prompt, provider_name)
                print(f"{provider_name} succeeded on attempt {attempt}")
                return analysis, provider_name

            except Exception as e:
                last_error = e
                print(f"{provider_name} failed on attempt {attempt}/{MAX_RETRIES_PER_PROVIDER}: {e}")

                if attempt < MAX_RETRIES_PER_PROVIDER:
                    # If the provider's error response includes its own
                    # recommended retry delay (as Gemini does), honor that
                    # instead of guessing with a fixed exponential backoff.
                    match = re.search(r"'retryDelay': '(\d+)s'", str(e))
                    wait_seconds = int(match.group(1)) + 1 if match else 2 ** attempt
                    print(f"Retrying {provider_name} in {wait_seconds} seconds...")
                    time.sleep(wait_seconds)

        print(f"{provider_name} is unavailable. Moving to the next provider...")

    raise RuntimeError(f"All providers failed. Last error: {last_error}")


def save_result(conn, original_message, analysis, duration_seconds, used_provider):
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

    # Kept as "(provider: X)", matching the original log format, so that
    # scripts/parse_agent_timings.py continues to parse these lines correctly.
    print(f"Analysis took {duration_seconds:.2f} seconds (provider: {used_provider})")


def save_review_summary(conn, product_id, product_name, summary, used_provider, review_count):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO product_review_summaries (
            product_id, product_name, ai_summary, provider_used, review_count
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (product_id)
        DO UPDATE SET
            product_name = EXCLUDED.product_name,
            ai_summary = EXCLUDED.ai_summary,
            provider_used = EXCLUDED.provider_used,
            review_count = EXCLUDED.review_count,
            updated_at = CURRENT_TIMESTAMP
        """,
        (product_id, product_name, summary, used_provider, review_count)
    )
    conn.commit()
    cursor.close()


def process_order_message(message_value):
    """Runs inside a worker thread: analyze one order end-to-end and save it.
    Opens its own DB connection, since psycopg2 connections are not safe to
    share across concurrently executing threads."""
    original = message_value.get('original_message', str(message_value))
    print("Analyzing order using provider routing...")
    start_time = time.monotonic()
    try:
        prompt = build_order_prompt(original)
        analysis, used_provider = analyze_with_model(prompt)
        duration = time.monotonic() - start_time

        print(f"Analysis: {analysis}")
        print(f"Provider used: {used_provider}")

        conn = get_db_connection()
        try:
            save_result(conn, original, analysis, duration, used_provider)
        finally:
            conn.close()

        print("Order analysis saved to PostgreSQL")
    except Exception as msg_error:
        print(f"Failed to process order message after all providers: {msg_error}")
    print("-" * 50)


def process_review_message(conn, review_message):
    product_id = review_message.get("product_id")
    product_name = review_message.get("product_name")

    print(f"Processing reviews for: {product_name}")

    if not product_id or not product_name:
        raise ValueError("Review message is missing product_id or product_name")

    reviews = get_product_reviews(conn, product_id)
    print(f"Found {len(reviews)} reviews for product: {product_name}")

    new_review = (
        review_message.get("customer_name"),
        review_message.get("rating"),
        review_message.get("review_text")
    )

    if new_review not in reviews:
        reviews.append(new_review)
        print("Added the new review to the summary input")

    prompt = build_review_summary_prompt(product_name, reviews)
    print("Generating AI review summary...")

    start_time = time.monotonic()
    try:
        summary, used_provider = analyze_with_model(prompt)
    except Exception:
        print("AI review summary skipped - no available AI provider.")
        return
    duration = time.monotonic() - start_time

    print(f"Summary generated using: {used_provider}")
    print(f"Generation took {duration:.2f} seconds")

    save_review_summary(conn, product_id, product_name, summary, used_provider, len(reviews))
    print("AI review summary saved to PostgreSQL")


def generate_initial_review_summaries(conn):
    products = []

    for attempt in range(5):
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT product_id, product_name
            FROM product_reviews
            ORDER BY product_id
            """
        )
        products = cursor.fetchall()
        cursor.close()

        if products:
            break

        print(f"No product reviews found yet. Waiting for seed... ({attempt + 1}/5)")
        time.sleep(3)

    if not products:
        print("No existing product reviews found. Skipping initial AI summaries.")
        return

    print(f"Found {len(products)} products with existing reviews.")

    for product_id, product_name in products:
        reviews = get_product_reviews(conn, product_id)
        if not reviews:
            continue

        print(f"Generating initial review summary for: {product_name} ({len(reviews)} reviews)")
        prompt = build_review_summary_prompt(product_name, reviews)

        try:
            summary, used_provider = analyze_with_model(prompt)
        except Exception:
            print("Initial AI summaries stopped - no AI provider is currently available.")
            break

        save_review_summary(conn, product_id, product_name, summary, used_provider, len(reviews))
        print(f"Initial AI summary saved for {product_name} using {used_provider}")


consumer = KafkaConsumer(
    *TOPIC_NAMES,
    bootstrap_servers=[config['kafka']['bootstrap_servers']],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=config['kafka']['group_id'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("AI Agent started, listening to topics: " + ", ".join(TOPIC_NAMES))
print("Provider routing order: " + " -> ".join(PROVIDER_ROUTING_ORDER))
print(f"Order-processing concurrency: {MAX_WORKERS} worker threads")
print("Waiting for messages...")

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)

try:
    conn = get_db_connection()
    print("Connected to PostgreSQL")

    print("Checking for existing product reviews...")
    generate_initial_review_summaries(conn)
    print("Initial review summary check completed.")
    print("Waiting for new Kafka messages...")

    for message in consumer:
        print(f"Received message: {message.value}")
        topic_name = message.topic
        print(f"Message topic: {topic_name}")

        if topic_name == "output-topic":
            # Hand off to a worker thread immediately and go back to reading
            # the next message right away, instead of blocking here.
            executor.submit(process_order_message, message.value)

        elif topic_name == "customer-review":
            print("Processing a review message...")
            try:
                process_review_message(conn, message.value)
            except Exception as msg_error:
                print(f"Failed to process review message: {msg_error}")
            print("-" * 50)

        else:
            print(f"Unsupported topic: {topic_name}")
            print("-" * 50)

except KeyboardInterrupt:
    print("\nAgent stopped")
except Exception as e:
    print(f"Error: {e}")
finally:
    print("Waiting for in-flight order analyses to finish...")
    executor.shutdown(wait=True)
    consumer.close()
    if 'conn' in locals():
        conn.close()