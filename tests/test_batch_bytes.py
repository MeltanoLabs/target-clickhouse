"""Tests for the byte-size batch cap (a safety net alongside row-count batching)."""

from __future__ import annotations

import datetime

from target_clickhouse.sinks import ClickhouseSink
from target_clickhouse.target import TargetClickhouse

CONFIG = {
    "host": "localhost",
    "port": 18123,
    "driver": "http",
    "username": "default",
    "password": "",
    "database": "default",
    "secure": False,
    "verify": False,
}


def _make_sink(config: dict) -> ClickhouseSink:
    target = TargetClickhouse(config=config)
    schema = {
        "properties": {"id": {"type": "integer"}, "payload": {"type": "string"}},
    }
    return ClickhouseSink(
        target=target,
        stream_name="batch_bytes_test_stream",
        schema=schema,
        key_properties=["id"],
    )


def test_not_full_below_both_caps() -> None:
    sink = _make_sink({**CONFIG, "batch_size_rows": 1000, "max_batch_bytes": 1_000_000})
    sink.process_record({"id": 1, "payload": "x"}, {})
    assert sink.is_full is False


def test_full_by_row_count_default_behavior_unchanged() -> None:
    """Row-count batching still works exactly as before when bytes stay small.

    tally_record_read() is normally invoked by the SDK's internal message
    dispatch (outside the Sink class), separately from process_record() --
    call both here to accurately simulate current_size incrementing.
    """
    sink = _make_sink({**CONFIG, "batch_size_rows": 2, "max_batch_bytes": 1_000_000})
    sink.tally_record_read()
    sink.process_record({"id": 1, "payload": "x"}, {})
    assert sink.is_full is False
    sink.tally_record_read()
    sink.process_record({"id": 2, "payload": "x"}, {})
    assert sink.is_full is True


def test_full_by_byte_size_before_row_count_reached() -> None:
    """A small max_batch_bytes flushes early even with very few rows."""
    sink = _make_sink({**CONFIG, "batch_size_rows": 10_000, "max_batch_bytes": 100})
    sink.process_record({"id": 1, "payload": "x" * 50}, {})
    assert sink.is_full is False
    sink.process_record({"id": 2, "payload": "x" * 50}, {})
    assert sink.is_full is True


def test_byte_tally_resets_after_drain() -> None:
    """mark_drained() resets the byte counter so the next batch starts fresh."""
    sink = _make_sink({**CONFIG, "batch_size_rows": 10_000, "max_batch_bytes": 100})
    sink.process_record({"id": 1, "payload": "x" * 50}, {})
    sink.process_record({"id": 2, "payload": "x" * 50}, {})
    assert sink.is_full is True

    sink.mark_drained()
    assert sink._batch_bytes == 0  # noqa: SLF001
    assert sink.is_full is False


def test_default_max_batch_bytes_matches_documented_default() -> None:
    sink = _make_sink(CONFIG)
    default_max_batch_bytes = 70_000_000
    assert sink._max_batch_bytes == default_max_batch_bytes  # noqa: SLF001


def test_process_record_does_not_crash_on_unnormalized_date_values() -> None:
    """Raw datetime.date values reach process_record() before normalization.

    The pipeline's own JSON normalization runs later on. The byte-size
    estimate must not crash on them (it did, with plain json.dumps(), before
    switching to default=str).
    """
    config = {**CONFIG, "batch_size_rows": 10_000, "max_batch_bytes": 1_000_000}
    sink = _make_sink(config)
    sink.process_record({"id": 1, "when": datetime.date(2024, 3, 15)}, {})
    assert sink._batch_bytes > 0  # noqa: SLF001
