"""Unit tests for producer.py core functions."""

import json
from unittest.mock import Mock, call, patch
import pytest
from fastapi.testclient import TestClient
from hamcrest import assert_that, equal_to, has_key, instance_of, contains_string
import producer as service
from producer import OrderRequest, OrderItem

client = TestClient(service.app)


# --- Fixtures ---

@pytest.fixture
def sample_order_payload():
    return {
        "branch": "Main Branch",
        "customer_name": "Ahmad",
        "payment_method": "Credit Card",
        "notes": "Deliver fast!",
        "items": [
            {
                "product_id": "p1",
                "product_name": "Laptop",
                "quantity": 1,
                "unit_price": 1200.0,
            }
        ],
    }


@pytest.fixture
def mock_producer():
    """KafkaProducer"""
    return Mock()


@pytest.fixture
def mock_get_producer():
    with patch("producer.get_producer") as mock_get:
        yield mock_get


# --- (Helper Functions) ---

def test_build_order_message_structure():
    
    order_data = OrderRequest(
        branch="Main Branch",
        customer_name="Ahmad",
        payment_method="Credit Card",
        notes="Deliver fast!",
        items=[
            {
                "product_id": "p1",
                "product_name": "Laptop",
                "quantity": 1,
                "unit_price": 1200.0,
            }
        ],
    )
    message = service.build_order_message(order_data)
    
    # PyHamcrest
    assert_that(message, instance_of(str))
    assert_that(
        message,
        equal_to(
            "Order from Ahmad | Branch: Main Branch | Payment: Credit Card | Items: Laptop x1 | Notes: Deliver fast!"
        ),
    )


def test_build_order_message_defaults_empty_notes_to_no_notes():
    order_data = OrderRequest(
        branch="Rafah",
        customer_name="Sara",
        payment_method="Card",
        notes="",
        items=[OrderItem(product_id="E1001", product_name="Smart Watch", quantity=1, unit_price=500)],
    )
    message = service.build_order_message(order_data)

    assert_that(message, contains_string("Notes: No notes"))


def test_build_order_message_joins_multiple_items_with_comma():
    order_data = OrderRequest(
        branch="Khan Younis",
        customer_name="Lina",
        payment_method="Jawwal Pay",
        notes="",
        items=[
            OrderItem(product_id="E1004", product_name="Gaming Mouse", quantity=2, unit_price=150),
            OrderItem(product_id="E1003", product_name="Wireless Headphones", quantity=1, unit_price=250),
        ],
    )
    message = service.build_order_message(order_data)

    assert_that(message, contains_string("Items: Gaming Mouse x2, Wireless Headphones x1"))


def test_health_endpoint():
    response = client.get("/health")
    assert_that(response.status_code, equal_to(200))
    assert_that(response.json(), has_key("status"))
    assert_that(response.json()["status"], equal_to("healthy"))


# --- Endpoints ---

def test_create_order_success(sample_order_payload, mock_get_producer, mock_producer):
    mock_future = Mock()
    mock_future.get.return_value.partition = 0
    mock_future.get.return_value.offset = 100
    mock_producer.send.return_value = mock_future
    
    mock_get_producer.return_value = mock_producer

    response = client.post("/order", json=sample_order_payload)

    assert_that(response.status_code, equal_to(200))
    assert_that(response.json()["status"], equal_to("success"))
    assert_that(response.json()["partition"], equal_to(0))
    assert_that(response.json()["offset"], equal_to(100))
    
    mock_producer.send.assert_called_once()


def test_create_order_missing_required_field_returns_422():
    payload = {
        "branch": "Gaza",
        "payment_method": "Cash",
        "items": [],
    }

    response = client.post("/order", json=payload)
    assert_that(response.status_code, equal_to(422))


def test_create_order_failure(sample_order_payload, mock_get_producer, mock_producer):
    mock_producer.send.side_effect = Exception("Kafka connection error")
    mock_get_producer.return_value = mock_producer

    response = client.post("/order", json=sample_order_payload)

    assert_that(response.status_code, equal_to(500))
    assert_that(response.json()["detail"], equal_to("Kafka connection error"))


# --- (Dashboard Endpoints) ---

def test_get_metrics_success():
    with patch("producer.fetch_dashboard_data") as mock_fetch, \
         patch("producer.calculate_metrics") as mock_calc:
        
        mock_fetch.return_value = ["dummy_order"]
        mock_calc.return_value = {"total_sales": 5000}

        response = client.get("/get_metrics")

        assert_that(response.status_code, equal_to(200))
        assert_that(response.json(), has_key("total_sales"))
        assert_that(response.json()["total_sales"], equal_to(5000))
        mock_fetch.assert_called_once()
        mock_calc.assert_called_once_with(["dummy_order"])


def test_latest_orders_success():
    with patch("producer.fetch_dashboard_data") as mock_fetch, \
         patch("producer.get_latest_orders") as mock_latest:
        
        mock_fetch.return_value = ["dummy_order"]
        mock_latest.return_value = [{"id": 1}]

        response = client.get("/latest_orders")

        assert_that(response.status_code, equal_to(200))
        assert_that(response.json(), instance_of(list))
        mock_fetch.assert_called_once()
        mock_latest.assert_called_once_with(["dummy_order"])


def test_sales_branch_success():
    with patch("producer.fetch_dashboard_data") as mock_fetch, \
         patch("producer.get_sales_by_branch") as mock_sales:
        
        mock_fetch.return_value = ["dummy_order"]
        mock_sales.return_value = {"Gaza": 1200}

        response = client.get("/sales_branch")

        assert_that(response.status_code, equal_to(200))
        assert_that(response.json(), has_key("Gaza"))
        assert_that(response.json()["Gaza"], equal_to(1200))
        mock_fetch.assert_called_once()
        mock_sales.assert_called_once_with(["dummy_order"])


def test_branch_performance_success():
    with patch("producer.fetch_dashboard_data") as mock_fetch, \
         patch("producer.get_branch_performance") as mock_perf:
        
        mock_fetch.return_value = ["dummy_order"]
        mock_perf.return_value = {"Gaza": "High"}

        response = client.get("/branch_performance")

        assert_that(response.status_code, equal_to(200))
        assert_that(response.json(), has_key("Gaza"))
        assert_that(response.json()["Gaza"], equal_to("High"))
        mock_fetch.assert_called_once()
        mock_perf.assert_called_once_with(["dummy_order"])