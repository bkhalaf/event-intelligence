import argparse
import asyncio
import random
import time

import httpx

BRANCHES = ["Gaza", "Rafah", "Khan Younis", "Deir al-Balah"]
PAYMENTS = ["Cash", "Card", "Jawwal Pay"]
CUSTOMERS = ["Ahmad", "Sara", "Lina", "Omar", "Noor", "Malak", "Yousef", "Maha"]
PRODUCTS = [
    ("E1001", "Smart Watch", 500),
    ("E1002", "Mechanical Keyboard", 300),
    ("E1003", "Wireless Headphones", 250),
    ("E1004", "Gaming Mouse", 150),
    ("E1005", "Anker Power Bank", 120),
    ("E1006", "Lenovo IdeaPad Laptop", 3200),
]


def build_order():
    product_id, product_name, unit_price = random.choice(PRODUCTS)
    return {
        "branch": random.choice(BRANCHES),
        "customer_name": random.choice(CUSTOMERS),
        "payment_method": random.choice(PAYMENTS),
        "notes": "Load test order",
        "items": [
            {
                "product_id": product_id,
                "product_name": product_name,
                "quantity": random.randint(1, 3),
                "unit_price": unit_price,
            }
        ],
    }


async def send_order(client, url, semaphore, stats):
    async with semaphore:
        try:
            response = await client.post(url, json=build_order(), timeout=10)
            if response.status_code == 200:
                stats["success"] += 1
            else:
                stats["failed"] += 1
        except Exception:
            stats["failed"] += 1


async def run_load_test(url, rate_per_min, duration_min, concurrency):
    interval = 60 / rate_per_min
    total_orders = int(rate_per_min * duration_min)
    semaphore = asyncio.Semaphore(concurrency)
    stats = {"success": 0, "failed": 0}

    print(f"Sending {total_orders} orders over {duration_min} min (~{rate_per_min}/min, one every {interval:.2f}s)")
    print(f"Target: {url}")

    async with httpx.AsyncClient() as client:
        tasks = []
        start = time.monotonic()

        for i in range(total_orders):
            tasks.append(asyncio.create_task(send_order(client, url, semaphore, stats)))
            await asyncio.sleep(interval)

            if (i + 1) % rate_per_min == 0:
                elapsed = time.monotonic() - start
                print(f"  {i + 1}/{total_orders} orders sent ({elapsed:.0f}s elapsed) | ok={stats['success']} failed={stats['failed']}")

        await asyncio.gather(*tasks)

    elapsed = time.monotonic() - start
    print(f"Done. Sent {total_orders} orders in {elapsed:.1f}s | success={stats['success']} failed={stats['failed']}")


def main():
    parser = argparse.ArgumentParser(description="Load-test the Producer API by sending randomized orders at a target rate.")
    parser.add_argument("--url", default="http://localhost:5050/order", help="Producer API /order endpoint")
    parser.add_argument("--rate", type=int, default=50000, help="Orders per minute (default: 500)")
    parser.add_argument("--duration", type=float, default=2, help="Duration in minutes (default: 2)")
    parser.add_argument("--concurrency", type=int, default=20, help="Max in-flight requests (default: 20)")
    args = parser.parse_args()

    asyncio.run(run_load_test(args.url, args.rate, args.duration, args.concurrency))


if __name__ == "__main__":
    main()
