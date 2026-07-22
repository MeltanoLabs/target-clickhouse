r"""Tests for ssh_tunnel support against a real, network-isolated ClickHouse.

These tests need a bastion + a ClickHouse instance that is *only* reachable
through that bastion (not from the test host directly) -- otherwise a test
could "pass" even if tunneling silently did nothing. That infrastructure
isn't part of the standard CI ClickHouse service container, so these tests
are skipped unless a key is available at the path below (or
TARGET_CLICKHOUSE_SSH_TEST_KEY points elsewhere).

To provision locally:

    ./gen_ssh_tunnel_test_key.sh
    docker compose --profile ssh-tunnel up -d

That's it -- the other env vars below already default to match the
`ssh-tunnel-bastion` / `clickhouse-tunnel-target` services in compose.yml.
"""

from __future__ import annotations

import io
import json
import os
import socket
from pathlib import Path

import pytest

from target_clickhouse.connectors import ClickhouseConnector
from target_clickhouse.target import TargetClickhouse

_REPO_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_SSH_KEY_PATH = _REPO_ROOT / ".ssh_tunnel_test_keys" / "id_ed25519"

SSH_KEY_PATH = os.environ.get(
    "TARGET_CLICKHOUSE_SSH_TEST_KEY",
    str(_DEFAULT_SSH_KEY_PATH),
)
BASTION_HOST = os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_HOST", "localhost")
BASTION_PORT = int(os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_PORT", "2222"))
BASTION_USER = os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_USER", "testuser")
TUNNEL_TARGET_HOST = os.environ.get(
    "TARGET_CLICKHOUSE_SSH_TEST_TARGET_HOST",
    "clickhouse-tunnel-target",
)

pytestmark = pytest.mark.skipif(
    not SSH_KEY_PATH or not Path(SSH_KEY_PATH).exists(),
    reason=(
        "Requires a real bastion + network-isolated ClickHouse -- run "
        "./gen_ssh_tunnel_test_key.sh && docker compose --profile ssh-tunnel "
        "up -d to set up (see module docstring)."
    ),
)


def _config() -> dict:
    return {
        "host": TUNNEL_TARGET_HOST,
        "port": 8123,
        "driver": "http",
        "username": "default",
        "password": "",
        "database": "default",
        "secure": False,
        "verify": True,
        "ssh_tunnel": {
            "enable": True,
            "host": BASTION_HOST,
            "port": BASTION_PORT,
            "username": BASTION_USER,
            "private_key": Path(SSH_KEY_PATH).read_text(),  # type:ignore[arg-type] # ty:ignore[invalid-argument-type]
        },
    }


def test_target_host_is_unreachable_without_tunnel() -> None:
    """Sanity check: the isolation this test relies on is real, not assumed."""
    with pytest.raises(socket.gaierror):
        socket.gethostbyname(TUNNEL_TARGET_HOST)


def test_connect_and_query_through_tunnel() -> None:
    """The connector can reach a network-isolated ClickHouse via the tunnel."""
    connector = ClickhouseConnector(config=_config())
    try:
        engine = connector.create_engine()
        with engine.connect() as conn:
            result = conn.exec_driver_sql("SELECT 1")
            assert result.fetchone() == (1,)
    finally:
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_create_table_and_insert_through_tunnel() -> None:
    """A real DDL + DML round trip works end-to-end over the tunnel."""
    connector = ClickhouseConnector(config=_config())
    try:
        engine = connector.create_engine()
        with engine.connect() as conn:
            conn.exec_driver_sql(
                "CREATE TABLE IF NOT EXISTS ssh_tunnel_test "
                "(id UInt32, name String) ENGINE = MergeTree ORDER BY id",
            )
            conn.exec_driver_sql("INSERT INTO ssh_tunnel_test VALUES (1, 'via-tunnel')")
            conn.commit()
            result = conn.exec_driver_sql("SELECT id, name FROM ssh_tunnel_test")
            assert result.fetchall() == [(1, "via-tunnel")]
            conn.exec_driver_sql("DROP TABLE ssh_tunnel_test")
            conn.commit()
    finally:
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_bulk_insert_through_tunnel() -> None:
    """The native bulk-insert path (not just DDL) respects ssh_tunnel.enable.

    Regression test: bulk_insert_records built its own clickhouse-connect
    client straight from config["host"]/config["port"], bypassing the
    connector's tunnel entirely -- so DDL/schema operations (routed through
    the SQLAlchemy connector) worked over the tunnel, but the actual data
    load (the connector's primary, "5-7x faster" native insert path,
    default for the http driver) did not. This runs a real target through
    its normal listen() entrypoint (the same code path `meltano run` uses)
    against the network-isolated ClickHouse and confirms rows actually land.
    """
    schema_msg = {
        "type": "SCHEMA",
        "stream": "ssh_tunnel_bulk_test",
        "schema": {
            "properties": {"id": {"type": "integer"}, "name": {"type": "string"}},
        },
        "key_properties": ["id"],
    }
    record_msgs = [
        {
            "type": "RECORD",
            "stream": "ssh_tunnel_bulk_test",
            "record": {"id": i, "name": f"row-{i}"},
        }
        for i in range(1, 6)
    ]
    state_msg = {"type": "STATE", "value": {}}
    lines = [
        json.dumps(schema_msg),
        *[json.dumps(r) for r in record_msgs],
        json.dumps(state_msg),
    ]

    target = TargetClickhouse(config=_config())
    try:
        target.listen(io.StringIO("\n".join(lines) + "\n"))

        connector = ClickhouseConnector(config=_config())
        try:
            engine = connector.create_engine()
            with engine.connect() as conn:
                result = conn.exec_driver_sql(
                    "SELECT count() FROM ssh_tunnel_bulk_test",
                )
                assert result.fetchone() == (5,)
                conn.exec_driver_sql("DROP TABLE ssh_tunnel_bulk_test")
                conn.commit()
        finally:
            connector._stop_ssh_tunnel()  # noqa: SLF001
    finally:
        for sink in target._sinks_active.values():  # noqa: SLF001
            connector_obj = getattr(sink, "connector", None)
            if connector_obj is not None:
                connector_obj._stop_ssh_tunnel()  # noqa: SLF001


def test_tunnel_reused_across_multiple_connections() -> None:
    """The connector starts one tunnel and reuses it, not a fresh one per call."""
    connector = ClickhouseConnector(config=_config())
    try:
        connector.create_engine().connect().close()
        first_tunnel = connector._ssh_tunnel  # noqa: SLF001
        assert first_tunnel is not None

        connector.create_engine().connect().close()
        assert connector._ssh_tunnel is first_tunnel  # noqa: SLF001
    finally:
        connector._stop_ssh_tunnel()  # noqa: SLF001


def test_disabled_ssh_tunnel_does_not_start_one() -> None:
    """Without ssh_tunnel.enable, no tunnel is created (and the raw host is used)."""
    config = _config()
    config["ssh_tunnel"]["enable"] = False
    connector = ClickhouseConnector(config=config)
    host, port = connector._tunneled_host_port(config)  # noqa: SLF001
    assert (host, port) == (TUNNEL_TARGET_HOST, 8123)
    assert connector._ssh_tunnel is None  # noqa: SLF001
