from unittest.mock import MagicMock
from fastapi.testclient import TestClient

import backend.api as producer_module
from backend.api import app

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


# ---------- test /review ----------

def test_review_endpoint_sends_to_kafka_and_returns_success(monkeypatch):
    fake_result = MagicMock()
    fake_result.partition = 0
    fake_result.offset = 7

    fake_future = MagicMock()
    fake_future.get.return_value = fake_result

    fake_producer = MagicMock()
    fake_producer.send.return_value = fake_future

    monkeypatch.setattr(producer_module, "get_producer", lambda: fake_producer)

    payload = {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "customer_name": "Ahmad",
        "rating": 5,
        "review_text": "Great mouse!",
    }

    response = client.post("/review", json=payload)

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    assert data["topic"] == "customer-review"
    assert data["partition"] == 0
    assert data["offset"] == 7

    fake_producer.send.assert_called_once_with(
        "customer-review",
        value={
            "product_id": "E1004",
            "product_name": "Gaming Mouse",
            "customer_name": "Ahmad",
            "rating": 5,
            "review_text": "Great mouse!",
        },
    )


def test_review_endpoint_missing_rating_returns_422():
    payload = {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
    }

    response = client.post("/review", json=payload)
    assert response.status_code == 422


def test_review_endpoint_rating_out_of_range_returns_422():
    payload = {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "rating": 6,
    }

    response = client.post("/review", json=payload)
    assert response.status_code == 422


def test_review_endpoint_defaults_blank_customer_name_to_anonymous(monkeypatch):
    fake_result = MagicMock()
    fake_result.partition = 0
    fake_result.offset = 1

    fake_future = MagicMock()
    fake_future.get.return_value = fake_result

    fake_producer = MagicMock()
    fake_producer.send.return_value = fake_future

    monkeypatch.setattr(producer_module, "get_producer", lambda: fake_producer)

    payload = {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "rating": 4,
    }

    response = client.post("/review", json=payload)

    assert response.status_code == 200
    sent_value = fake_producer.send.call_args.kwargs["value"]
    assert sent_value["customer_name"] == "Anonymous"


def test_review_endpoint_kafka_failure_returns_500(monkeypatch):
    def raise_error():
        raise Exception("Kafka connection failed")

    monkeypatch.setattr(producer_module, "get_producer", raise_error)

    payload = {
        "product_id": "E1004",
        "product_name": "Gaming Mouse",
        "rating": 4,
    }

    response = client.post("/review", json=payload)
    assert response.status_code == 500


# ---------- test /product_reviews_summary ----------

def test_product_reviews_summary_endpoint_returns_data(monkeypatch):
    fake_reviews = [{"product_id": "E1004", "product_name": "Gaming Mouse"}]
    fake_summary = [{"product_id": "E1004", "avg_rating": 4.5, "review_count": 2, "recent_reviews": []}]

    monkeypatch.setattr(producer_module, "fetch_product_reviews", lambda: fake_reviews)
    monkeypatch.setattr(producer_module, "get_product_reviews_summary", lambda reviews: fake_summary)

    response = client.get("/product_reviews_summary")

    assert response.status_code == 200
    assert response.json() == fake_summary