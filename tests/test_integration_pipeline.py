import time
import uuid

import psycopg2
import pytest
import requests

pytestmark = pytest.mark.integration

API_URL = "http://localhost:5000"

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "dbname": "kafka_events",
    "user": "admin",
    "password": "admin123",
}


def _wait_for_message(marker, timeout=30, interval=2):
    """يستعلم قاعدة البيانات بشكل متكرر لغاية ما تظهر الرسالة أو ينتهي الوقت المسموح."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            cur.execute(
                "SELECT original_message, transformed_message "
                "FROM kafka_messages WHERE original_message LIKE %s",
                (f"%{marker}%",),
            )
            row = cur.fetchone()
            if row:
                return row
            time.sleep(interval)
        return None
    finally:
        cur.close()
        conn.close()


def _cleanup_message(marker):
    """ينضف الصف التجريبي بعد الاختبار حتى ما تتراكم بيانات وهمية بقاعدة البيانات."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("DELETE FROM kafka_messages WHERE original_message LIKE %s", (f"%{marker}%",))
    conn.commit()
    cur.close()
    conn.close()


def test_full_pipeline_order_reaches_database():
    # علامة فريدة لكل تشغيل حتى نميز رسالتنا عن بيانات الـ seed أو تشغيلات سابقة
    marker = f"IntegrationTest-{uuid.uuid4().hex[:8]}"

    payload = {
        "branch": "Gaza",
        "customer_name": marker,
        "payment_method": "Cash",
        "notes": "integration test",
        "items": [
            {"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
        ],
    }

    try:
        response = requests.post(f"{API_URL}/order", json=payload, timeout=10)
        assert response.status_code == 200

        result = _wait_for_message(marker, timeout=30)
        assert result is not None, "الرسالة ما وصلت لقاعدة البيانات خلال 30 ثانية — تأكدي إنه Docker شغال بالكامل"

        original_message, transformed_message = result
        assert marker in original_message
        assert transformed_message == original_message.upper()
    finally:
        _cleanup_message(marker)