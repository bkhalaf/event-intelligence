from scripts.producer import build_order_message, OrderRequest, OrderItem


def test_build_order_message_matches_expected_format():
    order = OrderRequest(
        branch="Gaza",
        customer_name="Ahmad",
        payment_method="Cash",
        notes="Urgent delivery",
        items=[
            OrderItem(product_id="E1004", product_name="Gaming Mouse", quantity=1, unit_price=150)
        ],
    )
    message = build_order_message(order)

    assert message == (
        "Order from Ahmad | Branch: Gaza | Payment: Cash | "
        "Items: Gaming Mouse x1 | Notes: Urgent delivery"
    )


def test_build_order_message_defaults_empty_notes_to_no_notes():
    order = OrderRequest(
        branch="Rafah",
        customer_name="Sara",
        payment_method="Card",
        notes="",
        items=[OrderItem(product_id="E1001", product_name="Smart Watch", quantity=1, unit_price=500)],
    )
    message = build_order_message(order)

    assert "Notes: No notes" in message


def test_build_order_message_joins_multiple_items_with_comma():
    order = OrderRequest(
        branch="Khan Younis",
        customer_name="Lina",
        payment_method="Jawwal Pay",
        notes="",
        items=[
            OrderItem(product_id="E1004", product_name="Gaming Mouse", quantity=2, unit_price=150),
            OrderItem(product_id="E1003", product_name="Wireless Headphones", quantity=1, unit_price=250),
        ],
    )
    message = build_order_message(order)

    assert "Items: Gaming Mouse x2, Wireless Headphones x1" in message