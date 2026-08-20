from datetime import datetime
from unittest.mock import patch
from backend.dashboard_utils import parse_items, parse_order, calculate_metrics, get_sales_by_branch, get_branch_performance, get_latest_orders, get_product_reviews_summary

# ---------- test parse_items ----------

def test_parse_items_single_item():
    
    result = parse_items("Gaming Mouse x1")
    assert result == [
        {"product_name": "Gaming Mouse", "quantity": 1, "unit_price": 150}
    ]


def test_parse_items_multiple_items():
    result = parse_items("Gaming Mouse x2, Smart Watch x1")
    assert len(result) == 2
    assert result[0]["product_name"] == "Gaming Mouse"
    assert result[0]["quantity"] == 2
    assert result[1]["product_name"] == "Smart Watch"
    assert result[1]["quantity"] == 1


def test_parse_items_unknown_product_defaults_price_to_zero():
    # A Product that is not in PRODUCT_PRICES
    result = parse_items("Unknown Product x3")
    assert result[0]["unit_price"] == 0


def test_parse_items_empty_string_returns_empty_list():
    assert parse_items("") == []
    assert parse_items("   ") == []


# ---------- test parse_order ----------

def test_parse_order_valid_message():
    row = (
        1,
        "Order from Ahmad | Branch: Gaza | Payment: Cash | Items: Gaming Mouse x1 | Notes: Urgent delivery",
        "ORDER FROM AHMAD | ...",
        None,
        None,
    )
    result = parse_order(row)

    assert result["customer_name"] == "Ahmad"
    assert result["branch"] == "Gaza"
    assert result["payment_method"] == "Cash"
    assert result["notes"] == "Urgent delivery"
    assert len(result["items"]) == 1


def test_parse_order_malformed_message_returns_unknown_fallback():
    # A message does not match the expected pattern
    row = (2, "this is not a valid order message", None, None, None)
    result = parse_order(row)

    assert result["customer_name"] == "Unknown"
    assert result["branch"] == "Unknown"
    assert result["items"] == []
    
    
# ---------- test calculate_metrics ----------

def make_order(branch, items, processed_at=None, inserted_at=None):
    """A small helper function to make it easier to construct a fake order in the format that parse_order returns."""
    return {
        "branch": branch,
        "items": items,
        "processed_at": processed_at,
        "inserted_at": inserted_at,
    }


def test_calculate_metrics_empty_orders_returns_zeros_and_na():
    result = calculate_metrics([])

    assert result["orders"] == 0
    assert result["revenue"] == 0
    assert result["products"] == 0
    assert result["branches"] == 0
    assert result["last_updated"] == "N/A"


def test_calculate_metrics_normal_case():
    orders = [
        make_order(
            branch="Gaza",
            items=[{"quantity": 2, "unit_price": 150}],  # Gaming Mouse x2
            processed_at=datetime(2026, 1, 1, 10, 0, 0),
        ),
        make_order(
            branch="Rafah",
            items=[{"quantity": 1, "unit_price": 500}],  # Smart Watch x1
            processed_at=datetime(2026, 1, 1, 12, 0, 0),
        ),
    ]

    result = calculate_metrics(orders)

    assert result["orders"] == 2
    assert result["revenue"] == 800          # (2*150) + (1*500)
    assert result["products"] == 3           # 2 + 1
    assert result["branches"] == 2           # Gaza, Rafah
    assert result["last_updated"] == "2026-01-01 12:00:00"  # Most recent


def test_calculate_metrics_order_with_no_items_still_counted():
    orders = [
        make_order(branch="Gaza", items=[], processed_at=datetime(2026, 1, 1, 9, 0, 0)),
    ]

    result = calculate_metrics(orders)

    assert result["orders"] == 1       # order is available even without products.
    assert result["revenue"] == 0
    assert result["products"] == 0
    assert result["branches"] == 1


def test_calculate_metrics_falls_back_to_inserted_at_when_processed_at_is_none():
    orders = [
        make_order(
            branch="Gaza",
            items=[{"quantity": 1, "unit_price": 100}],
            processed_at=None,
            inserted_at=datetime(2026, 1, 5, 8, 30, 0),
        ),
    ]

    result = calculate_metrics(orders)

    assert result["last_updated"] == "2026-01-05 08:30:00"
    

def test_get_sales_by_branch_aggregates_same_branch():
    orders = [
        make_order(branch="Gaza", items=[{"quantity": 2, "unit_price": 150}]),
        make_order(branch="Gaza", items=[{"quantity": 1, "unit_price": 500}]),
    ]
    result = get_sales_by_branch(orders)
    assert result == [{"branch": "Gaza", "sales": 800}]  # (2*150) + (1*500)


def test_get_sales_by_branch_separates_different_branches():
    orders = [
        make_order(branch="Gaza", items=[{"quantity": 1, "unit_price": 100}]),
        make_order(branch="Rafah", items=[{"quantity": 1, "unit_price": 200}]),
    ]
    result = get_sales_by_branch(orders)

    branches = {entry["branch"]: entry["sales"] for entry in result}
    assert branches == {"Gaza": 100, "Rafah": 200}


def test_get_sales_by_branch_handles_missing_branch_name():
    orders = [make_order(branch=None, items=[{"quantity": 1, "unit_price": 50}])]
    result = get_sales_by_branch(orders)
    assert result == [{"branch": "Unknown", "sales": 50}]
    
    
def test_get_branch_performance_calculates_avg_order_correctly():
    orders = [
        make_order(branch="Gaza", items=[{"quantity": 2, "unit_price": 150}]),  # 300
        make_order(branch="Gaza", items=[{"quantity": 1, "unit_price": 500}]),  # 500
    ]
    result = get_branch_performance(orders)

    gaza = next(b for b in result if b["branch"] == "Gaza")
    assert gaza["orders"] == 2
    assert gaza["revenue"] == 800
    assert gaza["products"] == 3
    assert gaza["avg_order"] == 400.0  # 800 / 2


def test_get_branch_performance_rounds_avg_order_to_two_decimals():
    orders = [
        make_order(branch="Rafah", items=[{"quantity": 1, "unit_price": 100}]),
        make_order(branch="Rafah", items=[{"quantity": 1, "unit_price": 100}]),
        make_order(branch="Rafah", items=[{"quantity": 1, "unit_price": 100}]),
    ]
    result = get_branch_performance(orders)

    rafah = next(b for b in result if b["branch"] == "Rafah")
    assert rafah["avg_order"] == 100.0


def test_get_branch_performance_separates_multiple_branches():
    orders = [
        make_order(branch="Gaza", items=[{"quantity": 1, "unit_price": 100}]),
        make_order(branch="Rafah", items=[{"quantity": 1, "unit_price": 200}]),
    ]
    result = get_branch_performance(orders)
    assert len(result) == 2
    
    
def test_get_latest_orders_sorts_newest_first():
    orders = [
        make_order(branch="Gaza", items=[], processed_at=datetime(2026, 1, 1, 10, 0, 0)),
        make_order(branch="Rafah", items=[], processed_at=datetime(2026, 1, 3, 10, 0, 0)),
        make_order(branch="Khan Younis", items=[], processed_at=datetime(2026, 1, 2, 10, 0, 0)),
    ]
    result = get_latest_orders(orders)

    assert [o["branch"] for o in result] == ["Rafah", "Khan Younis", "Gaza"]


def test_get_latest_orders_respects_limit():
    orders = [
        make_order(branch=f"Branch{i}", items=[], processed_at=datetime(2026, 1, i + 1, 0, 0, 0))
        for i in range(5)
    ]
    result = get_latest_orders(orders, limit=2)

    assert len(result) == 2
    assert result[0]["branch"] == "Branch4"  # الأحدث (يوم 5، عند i=4)
    assert result[1]["branch"] == "Branch3"  # الثاني (يوم 4، عند i=3)


def test_get_latest_orders_uses_inserted_at_when_processed_at_missing():
    orders = [
        make_order(branch="Gaza", items=[], processed_at=None, inserted_at=datetime(2026, 1, 1, 0, 0, 0)),
        make_order(branch="Rafah", items=[], processed_at=None, inserted_at=datetime(2026, 1, 5, 0, 0, 0)),
    ]
    result = get_latest_orders(orders)

    assert result[0]["branch"] == "Rafah"  # الأحدث حسب inserted_at


# ---------- test get_product_reviews_summary ----------

def make_review(product_id, product_name, rating, review_text="", customer_name="Ahmad", submitted_at=None):
    return {
        "product_id": product_id,
        "product_name": product_name,
        "customer_name": customer_name,
        "rating": rating,
        "review_text": review_text,
        "submitted_at": submitted_at,
    }


def test_get_product_reviews_summary_empty_returns_empty_list():
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        assert get_product_reviews_summary([]) == []


def test_get_product_reviews_summary_calculates_avg_rating_and_count():
    reviews = [
        make_review("E1001", "Lenovo IdeaPad Laptop", 5),
        make_review("E1001", "Lenovo IdeaPad Laptop", 3),
    ]
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        result = get_product_reviews_summary(reviews)

    assert len(result) == 1
    assert result[0]["product_id"] == "E1001"
    assert result[0]["review_count"] == 2
    assert result[0]["avg_rating"] == 4.0


def test_get_product_reviews_summary_rounds_avg_rating_to_one_decimal():
    reviews = [
        make_review("E1002", "Anker Power Bank", 5),
        make_review("E1002", "Anker Power Bank", 4),
        make_review("E1002", "Anker Power Bank", 4),
    ]
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        result = get_product_reviews_summary(reviews)

    assert result[0]["avg_rating"] == 4.3


def test_get_product_reviews_summary_separates_multiple_products():
    reviews = [
        make_review("E1001", "Lenovo IdeaPad Laptop", 5),
        make_review("E1002", "Anker Power Bank", 3),
    ]
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        result = get_product_reviews_summary(reviews)

    product_ids = {entry["product_id"] for entry in result}
    assert product_ids == {"E1001", "E1002"}


def test_get_product_reviews_summary_limits_recent_reviews():
    reviews = [
        make_review("E1001", "Lenovo IdeaPad Laptop", 5, review_text=f"review {i}")
        for i in range(5)
    ]
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        result = get_product_reviews_summary(reviews, recent_limit=3)

    assert len(result[0]["recent_reviews"]) == 3


def test_get_product_reviews_summary_defaults_missing_customer_name_to_anonymous():
    reviews = [make_review("E1001", "Lenovo IdeaPad Laptop", 5, customer_name=None)]
    with patch("backend.dashboard_utils.fetch_ai_review_summaries", return_value={}):
        result = get_product_reviews_summary(reviews)

    assert result[0]["recent_reviews"][0]["customer_name"] == "Anonymous"