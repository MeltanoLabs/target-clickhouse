"""Unit tests for the DB-free Arrow BATCH helpers (no ClickHouse required)."""

from __future__ import annotations

import pyarrow as pa
from pyarrow import ipc
from singer_sdk.helpers._batch import BaseBatchFileEncoding

from target_clickhouse.arrow_batch import (
    ArrowEncoding,
    read_arrow_batch_file,
    resolve_manifest_path,
)


def test_resolve_manifest_path_strips_file_scheme() -> None:
    assert (
        resolve_manifest_path("file:///tmp/batch.arrow") == "/tmp/batch.arrow"  # noqa: S108
    )


def test_resolve_manifest_path_passes_through_bare_path() -> None:
    # urlparse on a schemeless path returns it verbatim in `.path`. Not a real
    # temp-file usage -- just a string round-tripped through the function.
    assert resolve_manifest_path("/tmp/batch.arrow") == "/tmp/batch.arrow"  # noqa: S108


def test_read_arrow_batch_file_round_trips_table(tmp_path) -> None:
    table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    path = tmp_path / "batch.arrow"

    with ipc.new_file(str(path), table.schema) as writer:
        writer.write_table(table)

    result = read_arrow_batch_file(str(path))

    assert result.equals(table)


def test_arrow_encoding_defaults_to_arrow_format() -> None:
    assert ArrowEncoding().format == "arrow"


def test_base_batch_file_encoding_from_dict_round_trips_arrow_format() -> None:
    # singer-sdk's _process_batch_message builds a plain BaseBatchFileEncoding
    # (no per-format subclass registry), so ClickhouseSink.process_batch_files
    # dispatches on encoding.format directly -- verify that string survives the
    # from_dict() round trip singer-sdk actually performs on an incoming BATCH
    # message's `encoding` field.
    encoding = BaseBatchFileEncoding.from_dict({"format": "arrow"})
    assert encoding.format == "arrow"
