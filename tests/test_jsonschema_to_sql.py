"""Tests for the JSONSchemaToSQL-based type mapping (connectors.py).

ClickhouseConnector.__init__ does not open a DB connection (it only stores
config), so a real instance is used throughout -- cheap, and exercises the
actual `jsonschema_to_sql` cached_property wiring rather than calling
`to_sql_type` unbound.
"""

from __future__ import annotations

import sqlalchemy.types
from clickhouse_sqlalchemy import types as clickhouse_sqlalchemy_types
from singer_sdk.sql.connector import JSONSchemaToSQL

from target_clickhouse.connectors import ClickhouseConnector, ClickHouseJSON


def _connector(**config: object) -> ClickhouseConnector:
    return ClickhouseConnector(config=config)


def test_jsonschema_to_sql_is_a_json_schema_to_sql_instance() -> None:
    connector = _connector()
    assert isinstance(connector.jsonschema_to_sql, JSONSchemaToSQL)


def test_date_maps_to_date32() -> None:
    sql_type = _connector().to_sql_type(
        {"type": ["string"], "format": "date"},
        is_primary_key=True,
    )
    assert isinstance(sql_type, clickhouse_sqlalchemy_types.Date32)

    sql_type = _connector().to_sql_type({"type": ["string"], "format": "date"})
    assert isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)
    assert isinstance(sql_type.nested_type, clickhouse_sqlalchemy_types.Date32)


def test_integer_maps_to_int64() -> None:
    sql_type = _connector().to_sql_type({"type": ["integer"]})
    assert isinstance(sql_type, clickhouse_sqlalchemy_types.Int64)


def test_plain_number_maps_to_float() -> None:
    """Plain "number" schemas (no x-singer.decimal format) use FLOAT."""
    sql_type = _connector().to_sql_type({"type": ["number"]})
    assert isinstance(sql_type, sqlalchemy.types.FLOAT)


def test_x_singer_decimal_format_preserves_precision_and_scale() -> None:
    """The x-singer.decimal format (on a string-typed schema) isn't clobbered
    by the plain "number" -> FLOAT handler, since it's a different JSON Schema
    type ("string") and format handlers are checked first.
    """  # noqa: D205
    sql_type = _connector().to_sql_type(
        {"type": ["string"], "format": "x-singer.decimal", "precision": 10, "scale": 2},
    )
    assert isinstance(sql_type, sqlalchemy.types.DECIMAL)
    assert sql_type.precision == 10  # noqa: PLR2004
    assert sql_type.scale == 2  # noqa: PLR2004


def test_object_maps_to_varchar_by_default() -> None:
    sql_type = _connector().to_sql_type({"type": ["object"]})
    assert isinstance(sql_type, sqlalchemy.types.VARCHAR)


def test_object_maps_to_clickhouse_json_when_enabled() -> None:
    sql_type = _connector(enable_json=True).to_sql_type({"type": ["object"]})
    assert isinstance(sql_type, ClickHouseJSON)


def test_enable_json_is_per_connector_not_global_state() -> None:
    """Two connector instances with different configs don't leak handlers."""
    plain = _connector()
    json_enabled = _connector(enable_json=True)

    assert isinstance(json_enabled.to_sql_type({"type": ["object"]}), ClickHouseJSON)
    assert isinstance(plain.to_sql_type({"type": ["object"]}), sqlalchemy.types.VARCHAR)


def test_boolean_type_unaffected_by_custom_handlers() -> None:
    sql_type = _connector().to_sql_type({"type": ["boolean"]})
    assert isinstance(sql_type, sqlalchemy.types.BOOLEAN)
