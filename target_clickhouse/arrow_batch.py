"""Pure, DB-free helpers for consuming Arrow-encoded Singer BATCH messages.

Taps/mappers such as pipelinewise-tap-mysql and mapper-fivetran can emit BATCH
messages with ``encoding: {"format": "arrow"}``, whose manifest entries are
local ``file://`` URIs pointing to Arrow IPC file format files. None of the
functions here talk to ClickHouse -- they only deal with pyarrow tables and the
local filesystem, so they can be unit tested without any DB connection.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlparse

import pyarrow as pa
from pyarrow import ipc
from singer_sdk.helpers._batch import BaseBatchFileEncoding

ARROW_ENCODING_FORMAT = "arrow"


@dataclass
class ArrowEncoding(BaseBatchFileEncoding):
    """A convenience constructor for an ``{"format": "arrow"}`` batch encoding.

    singer-sdk's ``_process_batch_message`` always builds a plain
    ``BaseBatchFileEncoding(**data)`` from the incoming BATCH message -- there
    is no per-format subclass registry to hook into. Dispatch in
    ``ClickhouseSink.process_batch_files`` therefore checks
    ``encoding.format == ARROW_ENCODING_FORMAT`` directly rather than
    ``isinstance(...)``; this class exists only so callers/tests can build an
    encoding object without repeating the literal.
    """

    format: str = ARROW_ENCODING_FORMAT


def resolve_manifest_path(file_uri: str) -> str:
    """Resolve a manifest ``file://`` URI (or bare local path) to a filesystem path.

    Args:
        file_uri: A manifest entry, e.g. ``"file:///tmp/batch.arrow"``.

    Returns:
        The local filesystem path.

    """
    return urlparse(file_uri).path or file_uri


def read_arrow_batch_file(path: str) -> pa.Table:
    """Read an Arrow IPC file format file into a table.

    Args:
        path: Local filesystem path to the Arrow IPC file.

    Returns:
        The file's contents as a single table.

    """
    with ipc.open_file(path) as reader:
        return reader.read_all()
