import json
import psycopg2
import re

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "kafka_events",
    "user": "admin",
    "password": "admin123",
}

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