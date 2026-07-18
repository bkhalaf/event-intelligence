from unittest.mock import MagicMock
from fastapi.testclient import TestClient

import scripts.producer as producer_module
from scripts.producer import app

client = TestClient(app)


# ---------- test /health ----------

def test_health_endpoint_returns_healthy():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}


# ---------- test /order ----------

def test_order_endpoint_sends_to_kafka_and_returns_success(monkeypatch):
    # build a fake version of the Kafka send result instead of the real connection
    fake_result = MagicMock()
    fake_result.partition = 0
    fake_result.offset = 42

    fake_future = MagicMock()
    fake_future.get.return_value = fake_result

    fake_producer = MagicMock()
    fake_producer.send.return_value = fake_future

    # replace the get_producer function so that it returns the fake_producer instead of actually calling Kafka
    monkeypatch.setattr(producer_module, "get_producer", lambda: fake_producer)

    payload = {
        "branch": "Gaza",
        "customer_name": "Ahmad",
        "payment_method": "Cash",
        "notes": "Test order",
        "items": [
            {"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
        ],
    }

    response = client.post("/order", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["partition"] == 0
    assert data["offset"] == 42


def test_order_endpoint_missing_required_field_returns_422():
    # Incomplete payload (without customer_name) — FastAPI/Pydantic must reject it automatically
    payload = {
        "branch": "Gaza",
        "payment_method": "Cash",
        "items": [],
    }

    response = client.post("/order", json=payload)
    assert response.status_code == 422  # Unprocessable Entity


def test_order_endpoint_kafka_failure_returns_500(monkeypatch):
    # نحاكي حالة فشل الاتصال بـ Kafka
    def raise_error():
        raise Exception("Kafka connection failed")

    monkeypatch.setattr(producer_module, "get_producer", raise_error)

    payload = {
        "branch": "Gaza",
        "customer_name": "Ahmad",
        "payment_method": "Cash",
        "notes": "",
        "items": [{"product_id": "E1004", "product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}],
    }

    response = client.post("/order", json=payload)
    assert response.status_code == 500