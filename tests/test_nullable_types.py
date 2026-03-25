"""Tests for nullable type handling in to_sql_type."""

from clickhouse_sqlalchemy import types as clickhouse_sqlalchemy_types
from singer_sdk import typing as th

from target_clickhouse.connectors import ClickhouseConnector


def _to_sql_type(jsonschema_type, is_primary_key=False):
    """Call to_sql_type as an unbound method — avoids full connector init."""
    return ClickhouseConnector.to_sql_type(
        None, jsonschema_type, is_primary_key=is_primary_key
    )


class TestNullableTypeMapping:
    def test_nullable_bool_returns_nullable(self):
        sql_type = _to_sql_type({"type": ["boolean", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_non_nullable_bool_returns_plain_bool(self):
        sql_type = _to_sql_type({"type": ["boolean"]})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_integer_returns_nullable(self):
        sql_type = _to_sql_type({"type": ["integer", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_string_returns_nullable(self):
        sql_type = _to_sql_type({"type": ["string", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_number_returns_nullable(self):
        sql_type = _to_sql_type({"type": ["number", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_datetime_stays_nullable(self):
        """Datetime was already wrapped — should not double-wrap."""
        sql_type = _to_sql_type(
            {"type": ["string", "null"], "format": "date-time"}
        )
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_primary_key_not_nullable(self):
        sql_type = _to_sql_type(
            {"type": ["boolean", "null"]}, is_primary_key=True
        )
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_non_nullable_integer_returns_int64(self):
        sql_type = _to_sql_type({"type": ["integer"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Int64)

    def test_string_type_not_list(self):
        """Handle case where type is a string, not a list."""
        sql_type = _to_sql_type({"type": "boolean"})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)
