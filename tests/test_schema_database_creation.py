"""End-to-end tests for schema-derived database creation.

ClickHouse has no schema namespace distinct from a database: a stream name
like ``analytics-my_stream`` (or the ``default_target_schema`` setting) maps
to the ClickHouse database ``analytics``, not the connector's configured
``database``. That database must be created on demand, or table/insert DDL
against it fails with ``UNKNOWN_DATABASE``.

Uses the standard CI ClickHouse (localhost:18123), the same instance test_core
uses. Skipped if it isn't reachable.
"""

from __future__ import annotations

import io
import json
import socket
from typing import Any

import pytest

from target_clickhouse.connectors import ClickhouseConnector
from target_clickhouse.target import TargetClickhouse

CH_HOST = "localhost"
CH_PORT = 18123


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(CH_HOST, CH_PORT),
    reason="Requires the standard test ClickHouse on localhost:18123",
)


def _config(**overrides: object) -> dict:
    return {
        "driver": "http",
        "host": CH_HOST,
        "port": CH_PORT,
        "username": "default",
        "password": "",
        "database": "default",
        "secure": False,
        "verify": False,
        **overrides,
    }


def _run(config: dict, stream: str, schema: dict, records: list[dict]) -> None:
    msgs: list[dict] = [
        {"type": "SCHEMA", "stream": stream, "schema": schema, "key_properties": []},
    ]
    msgs += [{"type": "RECORD", "stream": stream, "record": r} for r in records]
    msgs.append({"type": "STATE", "value": {}})
    lines = "\n".join(json.dumps(m) for m in msgs) + "\n"
    TargetClickhouse(config=config).listen(io.StringIO(lines))


def _query_one(connector: ClickhouseConnector, sql: str) -> Any:  # noqa: ANN401
    with connector.create_engine().connect() as conn:
        return conn.exec_driver_sql(sql).fetchone()


def _exec(connector: ClickhouseConnector, sql: str) -> None:
    with connector.create_engine().connect() as conn:
        conn.exec_driver_sql(sql)
        conn.commit()


def test_stream_name_schema_prefix_creates_database() -> None:
    """A ``<schema>-<stream>`` name creates and writes to that database."""
    database = "schema_db_creation_test"
    table = "my_stream"
    schema = {"properties": {"id": {"type": "integer"}}}
    config = _config()
    connector = ClickhouseConnector(config=config)
    try:
        _exec(connector, f"DROP DATABASE IF EXISTS {database}")
        _run(config, f"{database}-{table}", schema, [{"id": 1}, {"id": 2}])

        count = _query_one(
            connector,
            f"SELECT count() FROM {database}.{table}",  # noqa: S608
        )[0]
        assert count == 2  # noqa: PLR2004
    finally:
        _exec(connector, f"DROP DATABASE IF EXISTS {database}")
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_default_target_schema_creates_database() -> None:
    """``default_target_schema`` creates and writes to that database."""
    database = "default_target_schema_creation_test"
    table = "some_stream"
    schema = {"properties": {"id": {"type": "integer"}}}
    config = _config(default_target_schema=database)
    connector = ClickhouseConnector(config=config)
    try:
        _exec(connector, f"DROP DATABASE IF EXISTS {database}")
        _run(config, table, schema, [{"id": 1}])

        count = _query_one(
            connector,
            f"SELECT count() FROM {database}.{table}",  # noqa: S608
        )[0]
        assert count == 1
    finally:
        _exec(connector, f"DROP DATABASE IF EXISTS {database}")
        connector._stop_ssh_tunnel()  # noqa: SLF001
