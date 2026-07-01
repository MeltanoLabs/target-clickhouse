r"""Tests for enable_json (ClickHouse's native JSON column type).

Only stabilized in ClickHouse 24.8+ -- the standard CI ClickHouse service
(clickhouse-server:23.4-alpine, see .github/workflows/ci_workflow.yml)
rejects it outright, so these tests target a separate, modern instance and
are skipped unless CH_JSON_TEST_HOST is set.

To run locally against a modern ClickHouse:

    docker run -d --name clickhouse-json-test -p 28123:8123 \
      clickhouse/clickhouse-server:latest
    export CH_JSON_TEST_HOST=localhost
    export CH_JSON_TEST_PORT=28123
    # only needed if your instance has a password set (a fresh
    # clickhouse-server:latest container with no config does not):
    export CH_JSON_TEST_PASSWORD=...
"""

from __future__ import annotations

import io
import json
import os

import pytest
from clickhouse_sqlalchemy.drivers.base import clickhouse_dialect

from target_clickhouse.connectors import ClickhouseConnector, ClickHouseJSON
from target_clickhouse.target import TargetClickhouse

JSON_TEST_HOST = os.environ.get("CH_JSON_TEST_HOST")
JSON_TEST_PORT = int(os.environ.get("CH_JSON_TEST_PORT", "8123"))
JSON_TEST_PASSWORD = os.environ.get("CH_JSON_TEST_PASSWORD", "")

pytestmark = pytest.mark.skipif(
    not JSON_TEST_HOST,
    reason=(
        "Requires a ClickHouse 24.8+ instance -- set CH_JSON_TEST_HOST to run "
        "(see module docstring for setup)."
    ),
)


def _config(**overrides: object) -> dict:
    return {
        "host": JSON_TEST_HOST,
        "port": JSON_TEST_PORT,
        "driver": "http",
        "username": "default",
        "password": JSON_TEST_PASSWORD,
        "database": "default",
        "secure": False,
        "verify": True,
        **overrides,
    }


def test_to_sql_type_maps_object_to_clickhouse_json_when_enabled() -> None:
    connector = ClickhouseConnector(config=_config(enable_json=True))
    sql_type = connector.to_sql_type({"type": ["object", "null"]})
    # wrapped in Nullable since the schema allows null
    assert "JSON" in sql_type.compile(dialect=clickhouse_dialect)


def test_to_sql_type_leaves_object_as_string_when_disabled() -> None:
    """Default behavior (enable_json unset) is unchanged."""
    connector = ClickhouseConnector(config=_config())
    sql_type = connector.to_sql_type({"type": ["object", "null"]})
    compiled = sql_type.compile(dialect=clickhouse_dialect)
    assert "JSON" not in compiled


def test_object_property_round_trips_as_native_json_end_to_end() -> None:
    """Real target run: object properties land in a genuine JSON column."""
    stream = "json_type_e2e_test"
    schema_msg = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {
            "properties": {
                "id": {"type": "integer"},
                "metadata": {"type": ["object", "null"]},
            },
        },
        "key_properties": ["id"],
    }
    record_msgs = [
        {
            "type": "RECORD",
            "stream": stream,
            "record": {"id": 1, "metadata": {"nested": {"x": [1, 2, 3]}}},
        },
    ]
    state_msg = {"type": "STATE", "value": {}}
    lines = [
        json.dumps(schema_msg),
        *[json.dumps(r) for r in record_msgs],
        json.dumps(state_msg),
    ]

    target = TargetClickhouse(config=_config(enable_json=True))
    target.listen(io.StringIO("\n".join(lines) + "\n"))

    connector = ClickhouseConnector(config=_config(enable_json=True))
    try:
        with connector.create_engine().connect() as conn:
            result = conn.exec_driver_sql(
                f"SELECT id, metadata, toTypeName(metadata) FROM {stream}",  # noqa: S608
            )
            row = result.fetchone()
            assert row[0] == 1
            assert "JSON" in row[2]
            conn.exec_driver_sql(f"DROP TABLE {stream}")
            conn.commit()
    finally:
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_clickhouse_json_get_col_spec() -> None:
    assert ClickHouseJSON().get_col_spec() == "JSON"
