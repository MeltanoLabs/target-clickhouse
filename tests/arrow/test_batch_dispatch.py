"""Mock-based tests for Arrow BATCH driver dispatch (no ClickHouse required).

These assert that `_insert_arrow_table` picks the right insert path for each
driver configuration, without needing a live ClickHouse instance -- mirroring
the `_make_sink` pattern in test_batch_bytes.py / test_retry.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
from singer_sdk.helpers._batch import BaseBatchFileEncoding

from target_clickhouse.arrow_batch import ArrowEncoding
from target_clickhouse.sinks import ClickhouseSink
from target_clickhouse.target import TargetClickhouse

SCHEMA = {"properties": {"id": {"type": "integer"}, "camelName": {"type": "string"}}}

HTTP_CONFIG = {
    "host": "localhost",
    "port": 18123,
    "driver": "http",
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
}

NATIVE_CONFIG = {
    **HTTP_CONFIG,
    "driver": "native",
    "port": 19000,
}

ASYNCH_CONFIG = {
    **HTTP_CONFIG,
    "driver": "asynch",
}

SQLALCHEMY_URL_CONFIG = {
    "sqlalchemy_url": "clickhouse+http://default:@localhost:18123/default",
}


def _make_sink(config: dict) -> ClickhouseSink:
    target = TargetClickhouse(config=config)
    return ClickhouseSink(
        target=target,
        stream_name="arrow_dispatch_test_stream",
        schema=SCHEMA,
        key_properties=["id"],
    )


def _table() -> pa.Table:
    return pa.table({"id": [1, 2], "camelName": ["a", "b"]})


def test_conforms_arrow_column_names() -> None:
    sink = _make_sink(HTTP_CONFIG)
    conformed = sink._conform_arrow_table_columns(_table())  # noqa: SLF001
    assert conformed.schema.names == ["id", "camelname"]


def test_http_driver_uses_clickhouse_connect_insert_arrow() -> None:
    sink = _make_sink(HTTP_CONFIG)

    with (
        patch.object(
            sink,
            "_insert_arrow_via_clickhouse_connect_with_retry",
            return_value=2,
        ) as mock_insert_arrow,
        patch.object(sink, "bulk_insert_records") as mock_bulk_insert,
    ):
        count = sink._insert_arrow_table(_table())  # noqa: SLF001

    mock_insert_arrow.assert_called_once()
    mock_bulk_insert.assert_not_called()
    assert count == 2  # noqa: PLR2004


def test_http_driver_falls_back_to_bulk_insert_on_failure() -> None:
    sink = _make_sink(HTTP_CONFIG)

    with (
        patch.object(
            sink,
            "_insert_arrow_via_clickhouse_connect_with_retry",
            side_effect=RuntimeError("boom"),
        ),
        patch.object(sink, "bulk_insert_records", return_value=2) as mock_bulk_insert,
    ):
        count = sink._insert_arrow_table(_table())  # noqa: SLF001

    mock_bulk_insert.assert_called_once()
    # Row dicts, not the Arrow table, are handed to the generic fallback.
    _, _, records = mock_bulk_insert.call_args.args
    assert records == [{"id": 1, "camelname": "a"}, {"id": 2, "camelname": "b"}]
    assert count == 2  # noqa: PLR2004


def test_native_driver_uses_columnar_insert() -> None:
    sink = _make_sink(NATIVE_CONFIG)

    with (
        patch.object(
            sink,
            "_insert_arrow_via_native_driver",
            return_value=2,
        ) as mock_native_insert,
        patch.object(sink, "bulk_insert_records") as mock_bulk_insert,
    ):
        count = sink._insert_arrow_table(_table())  # noqa: SLF001

    mock_native_insert.assert_called_once()
    mock_bulk_insert.assert_not_called()
    assert count == 2  # noqa: PLR2004


def test_native_driver_columnar_insert_calls_client_execute() -> None:
    sink = _make_sink(NATIVE_CONFIG)
    mock_client = MagicMock()

    with patch.object(sink.connector, "native_driver_client") as mock_ctx:
        mock_ctx.return_value.__enter__.return_value = mock_client
        count = sink._insert_arrow_via_native_driver(  # noqa: SLF001
            sink._conform_arrow_table_columns(_table()),  # noqa: SLF001
        )

    assert count == 2  # noqa: PLR2004
    mock_client.execute.assert_called_once()
    query, column_data = mock_client.execute.call_args.args
    assert "id" in query
    assert "camelname" in query
    assert column_data == [[1, 2], ["a", "b"]]
    assert mock_client.execute.call_args.kwargs == {"columnar": True}


def test_asynch_driver_falls_back_to_bulk_insert() -> None:
    sink = _make_sink(ASYNCH_CONFIG)

    with patch.object(sink, "bulk_insert_records", return_value=2) as mock_bulk_insert:
        count = sink._insert_arrow_table(_table())  # noqa: SLF001

    mock_bulk_insert.assert_called_once()
    assert count == 2  # noqa: PLR2004


def test_explicit_sqlalchemy_url_falls_back_to_bulk_insert() -> None:
    sink = _make_sink(SQLALCHEMY_URL_CONFIG)

    with patch.object(sink, "bulk_insert_records", return_value=2) as mock_bulk_insert:
        count = sink._insert_arrow_table(_table())  # noqa: SLF001

    mock_bulk_insert.assert_called_once()
    assert count == 2  # noqa: PLR2004


def test_empty_table_is_a_no_op() -> None:
    sink = _make_sink(HTTP_CONFIG)
    empty_table = pa.table({"id": [], "camelName": []})

    with (
        patch.object(
            sink,
            "_insert_arrow_via_clickhouse_connect_with_retry",
        ) as mock_insert,
        patch.object(sink, "bulk_insert_records") as mock_bulk_insert,
    ):
        count = sink._insert_arrow_table(empty_table)  # noqa: SLF001

    mock_insert.assert_not_called()
    mock_bulk_insert.assert_not_called()
    assert count == 0


def test_process_batch_files_dispatches_arrow_encoding_and_returns_row_count() -> None:
    sink = _make_sink(HTTP_CONFIG)

    with (
        patch.object(
            sink,
            "_insert_arrow_table",
            return_value=2,
        ) as mock_insert_table,
        patch(
            "target_clickhouse.sinks.resolve_manifest_path",
            return_value="/tmp/fake.arrow",  # noqa: S108
        ) as mock_resolve,
        patch(
            "target_clickhouse.sinks.read_arrow_batch_file",
            return_value=_table(),
        ) as mock_read,
        patch("os.path.exists", return_value=False),
    ):
        record_count = sink._process_arrow_batch_files(  # noqa: SLF001
            ["file:///tmp/fake.arrow"],
        )

    mock_resolve.assert_called_once_with("file:///tmp/fake.arrow")
    mock_read.assert_called_once_with("/tmp/fake.arrow")  # noqa: S108
    mock_insert_table.assert_called_once()
    assert record_count == 2  # noqa: PLR2004


def test_process_batch_files_routes_arrow_encoding_via_insert_table() -> None:
    """The public entrypoint (as called by singer-sdk) reaches _insert_arrow_table."""
    sink = _make_sink(HTTP_CONFIG)

    with (
        patch.object(sink, "_insert_arrow_table", return_value=2) as mock_insert_table,
        patch(
            "target_clickhouse.sinks.resolve_manifest_path",
            return_value="/tmp/fake.arrow",  # noqa: S108
        ),
        patch(
            "target_clickhouse.sinks.read_arrow_batch_file",
            return_value=_table(),
        ),
        patch("os.path.exists", return_value=False),
    ):
        sink.process_batch_files(ArrowEncoding(), ["file:///tmp/fake.arrow"])

    mock_insert_table.assert_called_once()


def test_process_batch_files_routes_jsonl_encoding_to_base_implementation() -> None:
    sink = _make_sink(HTTP_CONFIG)

    with patch(
        "target_clickhouse.sinks.SQLSink.process_batch_files",
    ) as mock_super:
        encoding = BaseBatchFileEncoding(format="jsonl", compression="gzip")
        sink.process_batch_files(encoding, ["file:///tmp/fake.jsonl.gz"])

    mock_super.assert_called_once_with(encoding, ["file:///tmp/fake.jsonl.gz"])
