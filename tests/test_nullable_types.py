"""Tests for nullable type handling in to_sql_type."""

from clickhouse_sqlalchemy import types as clickhouse_sqlalchemy_types

from target_clickhouse.connectors import ClickhouseConnector

# ClickhouseConnector.__init__ does not open a DB connection (it only stores
# config), so a real instance is cheap here -- needed since to_sql_type() now
# reads self.config via the jsonschema_to_sql cached_property (for the
# enable_json object-type override), which an unbound `self=None` call can't do.
_CONNECTOR = ClickhouseConnector(config={})


def _to_sql_type(jsonschema_type, is_primary_key=False):
    return _CONNECTOR.to_sql_type(
        jsonschema_type,
        is_primary_key=is_primary_key,
    )


class TestNullableTypeMapping:
    """Tests for nullable type wrapping in ClickhouseConnector.to_sql_type."""

    def test_nullable_bool_returns_nullable(self):
        """Nullable bool schema should produce Nullable(Bool)."""
        sql_type = _to_sql_type({"type": ["boolean", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_non_nullable_bool_returns_plain_bool(self):
        """Non-nullable bool schema should not be wrapped."""
        sql_type = _to_sql_type({"type": ["boolean"]})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_integer_returns_nullable(self):
        """Nullable integer schema should produce Nullable(Int64)."""
        sql_type = _to_sql_type({"type": ["integer", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_string_returns_nullable(self):
        """Nullable string schema should produce Nullable(String)."""
        sql_type = _to_sql_type({"type": ["string", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_number_returns_nullable(self):
        """Nullable number schema should produce Nullable(Float)."""
        sql_type = _to_sql_type({"type": ["number", "null"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_nullable_datetime_stays_nullable(self):
        """Datetime was already wrapped — should not double-wrap."""
        sql_type = _to_sql_type(
            {"type": ["string", "null"], "format": "date-time"},
        )
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_primary_key_not_nullable(self):
        """Primary key columns should never be Nullable."""
        sql_type = _to_sql_type(
            {"type": ["boolean", "null"]},
            is_primary_key=True,
        )
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)

    def test_non_nullable_integer_returns_int64(self):
        """Non-nullable integer should produce Int64."""
        sql_type = _to_sql_type({"type": ["integer"]})
        assert isinstance(sql_type, clickhouse_sqlalchemy_types.Int64)

    def test_string_type_not_list(self):
        """Handle case where type is a string, not a list."""
        sql_type = _to_sql_type({"type": "boolean"})
        assert not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)
