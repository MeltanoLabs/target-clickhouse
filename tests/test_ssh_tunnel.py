r"""Tests for ssh_tunnel support against a real, network-isolated ClickHouse.

These tests need a bastion + a ClickHouse instance that is *only* reachable
through that bastion (not from the test host directly) -- otherwise a test
could "pass" even if tunneling silently did nothing. That infrastructure
isn't part of the standard CI ClickHouse service container, so these tests
are skipped unless the environment variables below point at real containers.

To provision locally (mirrors the isolation used to validate this feature):

    docker network create sshtunnel-test-net
    docker run -d --name clickhouse-tunnel-target \
      --network sshtunnel-test-net clickhouse/clickhouse-server:23.4-alpine
    ssh-keygen -t ed25519 -f /tmp/ssh-tunnel-test-keys/id_ed25519 -N ""
    docker run -d --name sshtunnel-bastion \
      --network sshtunnel-test-net -p 2222:2222 \
      -e PUBLIC_KEY="$(cat /tmp/ssh-tunnel-test-keys/id_ed25519.pub)" \
      -e USER_NAME=testuser -e PASSWORD_ACCESS=false -e SUDO_ACCESS=false \
      lscr.io/linuxserver/openssh-server:latest
    # linuxserver/openssh-server disables TCP forwarding by default:
    docker exec sshtunnel-bastion sed -i \
      's/AllowTcpForwarding no/AllowTcpForwarding yes/' /config/sshd/sshd_config
    docker restart sshtunnel-bastion

    export TARGET_CLICKHOUSE_SSH_TEST_KEY=/tmp/ssh-tunnel-test-keys/id_ed25519
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from target_clickhouse.connectors import ClickhouseConnector

SSH_KEY_PATH = os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_KEY")
BASTION_HOST = os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_HOST", "localhost")
BASTION_PORT = int(os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_PORT", "2222"))
BASTION_USER = os.environ.get("TARGET_CLICKHOUSE_SSH_TEST_BASTION_USER", "testuser")
TUNNEL_TARGET_HOST = os.environ.get(
    "TARGET_CLICKHOUSE_SSH_TEST_TARGET_HOST", "clickhouse-tunnel-target",
)

pytestmark = pytest.mark.skipif(
    not SSH_KEY_PATH or not Path(SSH_KEY_PATH).exists(),
    reason=(
        "Requires a real bastion + network-isolated ClickHouse -- set "
        "TARGET_CLICKHOUSE_SSH_TEST_KEY to run (see module docstring for setup)."
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
            "private_key": Path(SSH_KEY_PATH).read_text(),
        },
    }


def test_target_host_is_unreachable_without_tunnel() -> None:
    """Sanity check: the isolation this test relies on is real, not assumed."""
    import socket

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
