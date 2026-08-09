import json
import psycopg2
import re
import os
import yaml

DB_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'postgres_db_config.yaml')
with open(DB_CONFIG_PATH, 'r') as f:
    DB_CONFIG = yaml.safe_load(f)

PRODUCT_PRICES = {
    "Anker Power Bank": 120,
    "Lenovo IdeaPad Laptop": 3200,
    "Gaming Mouse": 150,
    "Wireless Headphones": 250,
    "Mechanical Keyboard": 300,
    "Smart Watch": 500,
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_orders():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, original_message, transformed_message, processed_at, inserted_at
        FROM kafka_messages
        ORDER BY id DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def parse_items(items_text):
    items = []
    if not items_text.strip():
        return items

    parts = [item.strip() for item in items_text.split(",")]
    for part in parts:
        match = re.match(r"(.+?) x(\d+)$", part)
        if match:
            product_name = match.group(1).strip()
            quantity = int(match.group(2))
            unit_price = PRODUCT_PRICES.get(product_name, 0)

            items.append({
                "product_name": product_name,
                "quantity": quantity,
                "unit_price": unit_price,
            })

    return items

def parse_order(row):
    order_id, original_message, transformed_message, processed_at, inserted_at = row

    pattern = r"Order from (.*?) \| Branch: (.*?) \| Payment: (.*?) \| Items: (.*?) \| Notes: (.*)"
    match = re.match(pattern, original_message)

    if not match:
        return {
            "id": order_id,
            "branch": "Unknown",
            "customer_name": "Unknown",
            "payment_method": "Unknown",
            "notes": "",
            "items": [],
            "processed_at": processed_at,
            "inserted_at": inserted_at,
            "original_message": original_message,
            "transformed_message": transformed_message,
        }

    customer_name = match.group(1).strip()
    branch = match.group(2).strip()
    payment_method = match.group(3).strip()
    items_text = match.group(4).strip()
    notes = match.group(5).strip()

    return {
        "id": order_id,
        "branch": branch,
        "customer_name": customer_name,
        "payment_method": payment_method,
        "notes": notes,
        "items": parse_items(items_text),
        "processed_at": processed_at,
        "inserted_at": inserted_at,
        "original_message": original_message,
        "transformed_message": transformed_message,
    }

def fetch_dashboard_data():
    rows = fetch_orders()
    return [parse_order(row) for row in rows]

def calculate_metrics(orders):
    total_orders = len(orders)
    total_revenue = 0
    total_products = 0
    branches = set()

    last_updated = None

    for order in orders:
        branches.add(order["branch"])

        current_time = order["processed_at"] or order["inserted_at"]

        if current_time:
            if last_updated is None or current_time > last_updated:
                last_updated = current_time

        for item in order["items"]:
            qty = item.get("quantity", 0) or 0
            price = item.get("unit_price", 0) or 0

            total_revenue += qty * price
            total_products += qty

    return {
        "orders": total_orders,
        "revenue": total_revenue,
        "products": total_products,
        "branches": len(branches),
        "last_updated": last_updated.strftime("%Y-%m-%d %H:%M:%S") if last_updated else "N/A",
    }

def get_latest_orders(orders, limit=10):
    return sorted(
        orders,
        key=lambda x: x["processed_at"] or x["inserted_at"],
        reverse=True
    )[:limit]

def get_sales_by_branch(orders):
    result = {}
    for order in orders:
        branch = order["branch"] or "Unknown"
        branch_total = 0

        for item in order["items"]:
            qty = item.get("quantity", 0) or 0
            price = item.get("unit_price", 0) or 0
            branch_total += qty * price

        result[branch] = result.get(branch, 0) + branch_total

    return [{"branch": branch, "sales": total} for branch, total in result.items()]

def get_branch_performance(orders):
    stats = {}

    for order in orders:
        branch = order["branch"] or "Unknown"
        if branch not in stats:
            stats[branch] = {
                "branch": branch,
                "orders": 0,
                "revenue": 0,
                "products": 0,
            }

        stats[branch]["orders"] += 1

        for item in order["items"]:
            qty = item.get("quantity", 0) or 0
            price = item.get("unit_price", 0) or 0
            stats[branch]["revenue"] += qty * price
            stats[branch]["products"] += qty

    for branch in stats.values():
        branch["avg_order"] = round(branch["revenue"] / branch["orders"], 2) if branch["orders"] else 0

    return list(stats.values())
def get_sales_by_payment(orders):
    result = {}

    for order in orders:
        payment = order["payment_method"] or "Unknown"

        total = 0
        for item in order["items"]:
            qty = item.get("quantity", 0) or 0
            price = item.get("unit_price", 0) or 0
            total += qty * price

        result[payment] = result.get(payment, 0) + total

    return [
        {
            "payment_method": payment,
            "sales": sales
        }
        for payment, sales in result.items()
    ]

def fetch_product_reviews():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT product_id, product_name, customer_name, rating, review_text, submitted_at
        FROM product_reviews
        ORDER BY submitted_at DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "product_id": product_id,
            "product_name": product_name,
            "customer_name": customer_name,
            "rating": rating,
            "review_text": review_text,
            "submitted_at": submitted_at,
        }
        for product_id, product_name, customer_name, rating, review_text, submitted_at in rows
    ]

def fetch_ai_review_summaries():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            product_id,
            ai_summary,
            provider_used,
            review_count,
            updated_at
        FROM product_review_summaries
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return {
        product_id: {
            "ai_summary": ai_summary,
            "summary_provider": provider_used,
            "summary_review_count": review_count,
            "summary_updated_at": updated_at,
        }
        for (
            product_id,
            ai_summary,
            provider_used,
            review_count,
            updated_at
        ) in rows
    }


def get_product_reviews_summary(reviews, recent_limit=3):
    ai_summaries = fetch_ai_review_summaries()
    stats = {}

    for review in reviews:
        product_id = review["product_id"]
        if product_id not in stats:
            stats[product_id] = {
                "product_id": product_id,
                "product_name": review["product_name"],
                "review_count": 0,
                "rating_total": 0,
                "recent_reviews": [],
            }

        entry = stats[product_id]
        entry["review_count"] += 1
        entry["rating_total"] += review["rating"]

        if len(entry["recent_reviews"]) < recent_limit:
            entry["recent_reviews"].append({
                "customer_name": review["customer_name"] or "Anonymous",
                "rating": review["rating"],
                "review_text": review["review_text"] or "",
                "submitted_at": review["submitted_at"],
            })

    summary = []
    for entry in stats.values():
        ai_summary = ai_summaries.get(entry["product_id"], {})
        summary.append({
            "product_id": entry["product_id"],
            "product_name": entry["product_name"],
            "review_count": entry["review_count"],
            "avg_rating": round(entry["rating_total"] / entry["review_count"], 1),
            "recent_reviews": entry["recent_reviews"],
            "ai_summary": ai_summary.get("ai_summary"),
            "summary_provider": ai_summary.get("summary_provider"),
            "summary_updated_at": ai_summary.get("summary_updated_at"),
        })

    return summary

def fetch_agent_results(limit=10):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, original_message, agent_analysis, analyzed_at
        FROM agent_results
        ORDER BY analyzed_at DESC
        LIMIT %s
        """,
        (limit,)
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [
        {
            "id": row[0],
            "original_message": row[1],
            "agent_analysis": row[2],
            "analyzed_at": row[3],
        }
        for row in rows
    ]