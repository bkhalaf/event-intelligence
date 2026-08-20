import os
import sys
import unittest
from unittest.mock import ANY, MagicMock, patch
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


class _EmptyKafkaConsumer:
    def __init__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return iter([])

    def close(self):
        pass


with patch("kafka.KafkaConsumer", _EmptyKafkaConsumer), patch(
    "psycopg2.connect", MagicMock()
), patch.object(sys, "argv", ["review_consumer.py"]):
    from scripts.review_consumer import DB_CONFIG, get_db_connection, insert_review


class TestReviewConsumer(unittest.TestCase):
    def test_insert_review(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        data = {
            "product_id": "E1004",
            "product_name": "Gaming Mouse",
            "customer_name": "Ahmad",
            "rating": 5,
            "review_text": "Great mouse!",
        }

        insert_review(conn, data)

        cursor.execute.assert_called_once_with(
            ANY,
            (
                data["product_id"],
                data["product_name"],
                data["customer_name"],
                data["rating"],
                data["review_text"],
            ),
        )
        conn.commit.assert_called_once()
        cursor.close.assert_called_once()

    def test_insert_review_missing_keys_defaults_to_none(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        insert_review(conn, {})

        cursor.execute.assert_called_once_with(ANY, (None, None, None, None, None))
        conn.commit.assert_called_once()

    def test_get_db_connection_uses_db_config(self):
        with patch("scripts.review_consumer.psycopg2.connect") as mock_connect:
            get_db_connection()
            mock_connect.assert_called_once_with(**DB_CONFIG)


if __name__ == "__main__":
    unittest.main()
