"""End-to-end tests for upsert (ReplacingMergeTree) behaviour.

Covers the two properties that matter for making upsert the default:

1. A keyed stream deduplicates on its primary key (upsert works).
2. A keyless stream does NOT collapse -- the engine falls back to MergeTree so
   all rows are preserved, instead of every row sharing an empty sorting key and
   OPTIMIZE FINAL reducing the table to a single row.

Uses the standard CI ClickHouse (localhost:18123), the same instance test_core
uses. Skipped if it isn't reachable.
"""

from __future__ import annotations

import io
import json
import socket

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


def _run(config: dict, stream: str, schema: dict, records: list[dict],
         *, key_properties: list[str]) -> None:
    """Feed one SCHEMA + records + STATE through a fresh target invocation."""
    msgs: list[dict] = [
        {"type": "SCHEMA", "stream": stream, "schema": schema,
         "key_properties": key_properties},
    ]
    msgs += [{"type": "RECORD", "stream": stream, "record": r} for r in records]
    msgs.append({"type": "STATE", "value": {}})
    lines = "\n".join(json.dumps(m) for m in msgs) + "\n"
    TargetClickhouse(config=config).listen(io.StringIO(lines))


def _query_one(connector: ClickhouseConnector, sql: str):
    with connector.create_engine().connect() as conn:
        return conn.exec_driver_sql(sql).fetchone()  # noqa: S608


def _exec(connector: ClickhouseConnector, sql: str) -> None:
    with connector.create_engine().connect() as conn:
        conn.exec_driver_sql(sql)  # noqa: S608
        conn.commit()


def test_keyed_stream_upserts_on_primary_key() -> None:
    stream = "upsert_dedup_keyed_test"
    schema = {
        "properties": {
            "id": {"type": "integer"},
            "value": {"type": ["string", "null"]},
        },
    }
    config = _config(
        engine_type="ReplacingMergeTree",
        optimize_after=True,
        load_method="upsert",
    )
    connector = ClickhouseConnector(config=config)
    try:
        _exec(connector, f"DROP TABLE IF EXISTS {stream}")  # noqa: S608
        # Two loads of id=1 (plus a distinct id=2) across separate runs.
        _run(config, stream, schema,
             [{"id": 1, "value": "v1"}, {"id": 2, "value": "x"}],
             key_properties=["id"])
        _run(config, stream, schema, [{"id": 1, "value": "v2"}],
             key_properties=["id"])
        _exec(connector, f"OPTIMIZE TABLE {stream} FINAL")  # noqa: S608

        engine = _query_one(
            connector,
            "SELECT engine FROM system.tables "
            f"WHERE database='default' AND name='{stream}'",  # noqa: S608
        )[0]
        count = _query_one(connector, f"SELECT count() FROM {stream}")[0]  # noqa: S608

        assert engine == "ReplacingMergeTree"
        # id=1 collapsed to one row, id=2 distinct -> 2 rows total.
        assert count == 2
    finally:
        _exec(connector, f"DROP TABLE IF EXISTS {stream}")  # noqa: S608
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_keyless_stream_does_not_collapse() -> None:
    stream = "upsert_dedup_keyless_test"
    schema = {
        "properties": {
            "a": {"type": ["string", "null"]},
            "b": {"type": ["integer", "null"]},
        },
    }
    # engine_type requests a collapsing engine, but the stream has no key.
    config = _config(engine_type="ReplacingMergeTree", optimize_after=True)
    connector = ClickhouseConnector(config=config)
    try:
        _exec(connector, f"DROP TABLE IF EXISTS {stream}")  # noqa: S608
        _run(config, stream, schema,
             [{"a": "x", "b": 1}, {"a": "y", "b": 2}, {"a": "z", "b": 3}],
             key_properties=[])
        _exec(connector, f"OPTIMIZE TABLE {stream} FINAL")  # noqa: S608

        engine = _query_one(
            connector,
            "SELECT engine FROM system.tables "
            f"WHERE database='default' AND name='{stream}'",  # noqa: S608
        )[0]
        count = _query_one(connector, f"SELECT count() FROM {stream}")[0]  # noqa: S608

        # Fallback engaged: stored as MergeTree, so all three rows survive.
        assert engine == "MergeTree"
        assert count == 3
    finally:
        _exec(connector, f"DROP TABLE IF EXISTS {stream}")  # noqa: S608
        connector._stop_ssh_tunnel()  # noqa: SLF001
