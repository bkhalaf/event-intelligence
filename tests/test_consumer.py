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
), patch.object(sys, "argv", ["consumer.py"]):
    from scripts.consumer import DB_CONFIG, get_db_connection, insert_message


class TestConsumer(unittest.TestCase):
    def test_insert_message(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        data = {
            "original_message": "Order from Ahmad | Branch: Gaza | Payment: Cash | "
            "Items: Gaming Mouse x2 | Notes: No notes",
            "transformed_message": "ORDER FROM AHMAD | BRANCH: GAZA | PAYMENT: CASH | "
            "ITEMS: GAMING MOUSE X2 | NOTES: NO NOTES",
            "processed_at": "2024-01-01T10:00:00",
        }

        insert_message(conn, data)

        cursor.execute.assert_called_once_with(
            ANY,
            (
                data["original_message"],
                data["transformed_message"],
                data["processed_at"],
            ),
        )
        conn.commit.assert_called_once()
        cursor.close.assert_called_once()

    def test_insert_message_missing_keys_defaults_to_none(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value

        insert_message(conn, {})

        cursor.execute.assert_called_once_with(ANY, (None, None, None))
        conn.commit.assert_called_once()

    def test_get_db_connection_uses_db_config(self):
        with patch("scripts.consumer.psycopg2.connect") as mock_connect:
            get_db_connection()
            mock_connect.assert_called_once_with(**DB_CONFIG)


if __name__ == "__main__":
    unittest.main()