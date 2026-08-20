import argparse
import time
import uuid
import statistics
import requests
import psycopg2
from datetime import datetime

API_URL = "http://localhost:5000/order"

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "kafka_events",
    "user": "admin",
    "password": "admin123",
}


def send_and_measure(timeout=30, poll_interval=0.2):
    marker = f"LatencyTest-{uuid.uuid4().hex[:8]}"
    payload = {
        "branch": "Gaza",
        "customer_name": marker,
        "payment_method": "Cash",
        "notes": "latency probe",
        "items": [
            {"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
        ],
    }

    # Wall-clock timestamp for record-keeping / reporting purposes only.
    send_datetime = datetime.now().isoformat(timespec="milliseconds")
    # Monotonic clock for the actual elapsed-time calculation (immune to
    # system clock adjustments, which a wall-clock diff would not be).
    send_time = time.monotonic()

    response = requests.post(API_URL, json=payload, timeout=10)
    if response.status_code != 200:
        print(f"  [{send_datetime}] send failed: {response.status_code} {response.text}")
        return None

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    deadline = time.monotonic() + timeout
    found_time = None
    found_datetime = None
    try:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT 1 FROM kafka_messages WHERE original_message LIKE %s",
                (f"%{marker}%",),
            )
            if cur.fetchone():
                found_time = time.monotonic()
                found_datetime = datetime.now().isoformat(timespec="milliseconds")
                break
            time.sleep(poll_interval)
    finally:
        cur.execute("DELETE FROM kafka_messages WHERE original_message LIKE %s", (f"%{marker}%",))
        conn.commit()
        cur.close()
        conn.close()

    if found_time is None:
        print(f"  [{send_datetime}] {marker}: TIMEOUT after {timeout}s")
        return None

    latency = found_time - send_time
    print(f"  [{send_datetime} -> {found_datetime}] {marker}: {latency:.2f}s")
    return latency


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between samples")
    args = parser.parse_args()

    latencies = []
    for i in range(args.samples):
        print(f"Sample {i+1}/{args.samples}")
        latency = send_and_measure()
        if latency is not None:
            latencies.append(latency)
        time.sleep(args.pause)

    if not latencies:
        print("No successful samples.")
        return

    latencies.sort()
    p95_index = max(0, int(len(latencies) * 0.95) - 1)
    print("\n--- Results ---")
    print(f"Samples: {len(latencies)}/{args.samples}")
    print(f"Min:     {min(latencies):.2f}s")
    print(f"Max:     {max(latencies):.2f}s")
    print(f"Avg:     {statistics.mean(latencies):.2f}s")
    print(f"P95:     {latencies[p95_index]:.2f}s")


if __name__ == "__main__":
    main()