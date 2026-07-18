import re
from scripts.seed import generate_order_message


def test_generate_order_message_matches_parse_order_pattern():
    # The same regex pattern actually used in parse_order inside dashboard_utils.py
    pattern = r"Order from (.*?) \| Branch: (.*?) \| Payment: (.*?) \| Items: (.*?) \| Notes: (.*)"

    for _ in range(20):  # run it several times to cover different random possibilities.
        original, _ = generate_order_message()
        assert re.match(pattern, original) is not None


def test_generate_order_message_transformed_is_uppercase_of_original():
    original, transformed = generate_order_message()
    assert transformed == original.upper()


def test_generate_order_message_selects_one_to_three_products():
    for _ in range(20):
        original, _ = generate_order_message()
        items_part = original.split("Items: ")[1].split(" | Notes:")[0]
        products_count = len(items_part.split(", "))
        assert 1 <= products_count <= 3