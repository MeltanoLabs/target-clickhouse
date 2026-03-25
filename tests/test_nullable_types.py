"""Tests for nullable type handling in to_sql_type."""

import sqlalchemy.types
from clickhouse_sqlalchemy import types as clickhouse_sqlalchemy_types
from unittest.mock import MagicMock

from target_clickhouse.connectors import ClickhouseConnector


def _make_connector():
    """Create a ClickhouseConnector with mocked config."""
    connector = ClickhouseConnector.__new__(ClickhouseConnector)
    connector.config = {}
    connector.logger = MagicMock()
    return connector


class TestNullableTypeMapping:
    def test_nullable_bool_returns_nullable(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["boolean", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected Nullable, got {type(sql_type)}"
        )

    def test_non_nullable_bool_returns_plain_bool(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["boolean"]})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected non-Nullable, got {type(sql_type)}"
        )

    def test_nullable_integer_returns_nullable(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["integer", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected Nullable, got {type(sql_type)}"
        )

    def test_nullable_string_returns_nullable(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["string", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected Nullable, got {type(sql_type)}"
        )

    def test_nullable_number_returns_nullable(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["number", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected Nullable, got {type(sql_type)}"
        )

    def test_nullable_datetime_stays_nullable(self):
        """Datetime was already wrapped in Nullable — should not double-wrap."""
        connector = _make_connector()
        sql_type = connector.to_sql_type(
            {"type": ["string", "null"], "format": "date-time"}
        )
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected Nullable, got {type(sql_type)}"
        )

    def test_primary_key_not_nullable(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type(
            {"type": ["boolean", "null"]}, is_primary_key=True
        )
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Primary keys should not be Nullable, got {type(sql_type)}"
        )

    def test_non_nullable_integer_returns_int64(self):
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": ["integer"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Int64), (
            f"Expected Int64, got {type(sql_type)}"
        )

    def test_string_type_not_list(self):
        """Handle case where type is a string, not a list."""
        connector = _make_connector()
        sql_type = connector.to_sql_type({"type": "boolean"})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable), (
            f"Expected non-Nullable for string type, got {type(sql_type)}"
        )
