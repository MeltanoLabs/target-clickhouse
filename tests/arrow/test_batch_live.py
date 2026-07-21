"""Live-ClickHouse integration tests for Arrow BATCH ingestion.

Unlike the rest of the suite (which relies on a ClickHouse instance already
running -- via the CI `services:` block, or manually locally), these tests
spin up their own ephemeral container with testcontainers, so they need
nothing but a local Docker daemon to run.
"""

from __future__ import annotations

import os
from pathlib import Path

import pyarrow as pa
import pytest
from pyarrow import ipc
from sqlalchemy import create_engine, text
from testcontainers.community.clickhouse import ClickHouseContainer

from target_clickhouse.arrow_batch import ArrowEncoding
from target_clickhouse.sinks import ClickhouseSink
from target_clickhouse.target import TargetClickhouse

SCHEMA = {
    "properties": {
        "id": {"type": "integer"},
        "camelName": {"type": "string"},
        "score": {"type": "number"},
    },
}


@pytest.fixture(scope="module")
def clickhouse_container():
    with ClickHouseContainer("clickhouse/clickhouse-server:26.6-alpine") as container:
        yield container


def _arrow_ipc_file(tmp_path, table: pa.Table) -> str:
    path = tmp_path / "batch.arrow"
    with ipc.new_file(str(path), table.schema) as writer:
        writer.write_table(table)
    return str(path)


@pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning")
@pytest.mark.parametrize("driver", ["http", "native"])
def test_arrow_batch_lands_rows(
    clickhouse_container: ClickHouseContainer,
    tmp_path: Path,
    driver: str,
) -> None:
    port = 8123 if driver == "http" else 9000
    config = {
        "driver": driver,
        "host": clickhouse_container.get_container_host_ip(),
        "port": int(clickhouse_container.get_exposed_port(port)),
        "username": clickhouse_container.username,
        "password": clickhouse_container.password,
        "database": clickhouse_container.dbname,
        "secure": False,
        "verify": False,
    }

    target = TargetClickhouse(config=config)
    sink = ClickhouseSink(
        target=target,
        stream_name=f"arrow_live_{driver}",
        schema=SCHEMA,
        key_properties=["id"],
    )
    sink.setup()

    table = pa.table(
        {
            "id": [1, 2, 3],
            "camelName": ["alpha", "beta", "gamma"],
            "score": [1.5, 2.5, 3.5],
        },
    )
    file_path = _arrow_ipc_file(tmp_path, table)

    try:
        sink.process_batch_files(ArrowEncoding(), [f"file://{file_path}"])

        engine = create_engine(
            f"clickhouse+http://{config['username']}:{config['password']}@"
            f"{config['host']}:{int(clickhouse_container.get_exposed_port(8123))}/"
            f"{config['database']}",
        )
        try:
            with engine.connect() as conn:
                # full_table_name is internally derived (stream name + config),
                # not user input, so string interpolation carries no injection
                # risk.
                query = (
                    f"SELECT id, camelname, score FROM {sink.full_table_name} "  # noqa: S608
                    "ORDER BY id"
                )
                rows = conn.execute(text(query)).fetchall()
        finally:
            engine.dispose()
    finally:
        # clickhouse-sqlalchemy's native dialect Connection.close() is a no-op
        # (it relies on cursor-level disconnects, which the columnar insert path
        # bypasses) -- engine.dispose() alone never closes the underlying
        # clickhouse-driver socket. Disconnect it explicitly before the
        # container is torn down at module-fixture end, otherwise GC closes it
        # after the container/port is already gone, surfacing as an
        # unraisable-exception failure under this project's
        # filterwarnings=["error"] policy.
        if driver == "native":
            with sink.connector.native_driver_client() as client:
                client.disconnect()
        sink.connector._engine.dispose()  # noqa: SLF001

    assert rows == [(1, "alpha", 1.5), (2, "beta", 2.5), (3, "gamma", 3.5)]


def test_arrow_batch_manifest_file_is_cleaned_up_by_default(
    clickhouse_container,
    tmp_path,
) -> None:
    config = {
        "driver": "http",
        "host": clickhouse_container.get_container_host_ip(),
        "port": int(clickhouse_container.get_exposed_port(8123)),
        "username": clickhouse_container.username,
        "password": clickhouse_container.password,
        "database": clickhouse_container.dbname,
        "secure": False,
        "verify": False,
    }

    target = TargetClickhouse(config=config)
    sink = ClickhouseSink(
        target=target,
        stream_name="arrow_live_cleanup",
        schema=SCHEMA,
        key_properties=["id"],
    )
    sink.setup()

    table = pa.table({"id": [1], "camelName": ["alpha"], "score": [1.0]})
    file_path = _arrow_ipc_file(tmp_path, table)

    try:
        sink.process_batch_files(ArrowEncoding(), [f"file://{file_path}"])
    finally:
        sink.connector._engine.dispose()  # noqa: SLF001

    assert not os.path.exists(file_path)  # noqa: PTH110
