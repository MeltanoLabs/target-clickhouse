"""Tests for retry/backoff on the native bulk-insert path.

Runs against the ClickHouse instance at ``CH_TEST_URI`` (default matches
test_core.py's local/CI instance at port 18123) -- these tests mock the
network call itself, so no real flaky network is needed, but the sink still
needs a real connector/config to construct.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ReadTimeout

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


def _make_sink(config: dict | None = None) -> ClickhouseSink:
    target = TargetClickhouse(config=config or CONFIG)
    return ClickhouseSink(
        target=target,
        stream_name="retry_test_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=["id"],
    )


def test_retries_on_connection_error_then_succeeds() -> None:
    """A ConnectionError (connection never established) is retried and can succeed."""
    sink = _make_sink({**CONFIG, "insert_retry_max_tries": 3})

    call_count = 0

    def flaky_insert(full_table_name, schema, records):  # noqa: ARG001
        nonlocal call_count
        call_count += 1
        if call_count < 3:  # noqa: PLR2004
            msg = "simulated: connection refused"
            raise RequestsConnectionError(msg)
        return len(records)

    with patch.object(
        sink, "_bulk_insert_via_clickhouse_connect", side_effect=flaky_insert,
    ):
        result = sink._bulk_insert_via_clickhouse_connect_with_retry(  # noqa: SLF001
            "default.retry_test_stream", {}, [{"id": 1}],
        )

    assert call_count == 3  # noqa: PLR2004
    assert result == 1


def test_gives_up_after_max_tries() -> None:
    """Exhausting insert_retry_max_tries re-raises, letting the caller fall back."""
    sink = _make_sink({**CONFIG, "insert_retry_max_tries": 2})

    call_count = 0

    def always_fails(full_table_name, schema, records):  # noqa: ARG001
        nonlocal call_count
        call_count += 1
        msg = "simulated: connection refused"
        raise RequestsConnectionError(msg)

    with (
        patch.object(
            sink, "_bulk_insert_via_clickhouse_connect", side_effect=always_fails,
        ),
        pytest.raises(RequestsConnectionError),
    ):
        sink._bulk_insert_via_clickhouse_connect_with_retry(  # noqa: SLF001
            "default.retry_test_stream", {}, [{"id": 1}],
        )

    # respected insert_retry_max_tries, didn't retry forever
    assert call_count == 2  # noqa: PLR2004


def test_does_not_retry_read_timeout() -> None:
    """ReadTimeout (data may already be in flight) is not in the retry set.

    Regression guard for the duplication-risk boundary: only connection-
    establishment failures (RETRYABLE_INSERT_EXCEPTIONS) are safe to retry
    blindly. If ReadTimeout were added to that set, this test would fail.
    """
    sink = _make_sink({**CONFIG, "insert_retry_max_tries": 5})

    call_count = 0

    def timeout_once(full_table_name, schema, records):  # noqa: ARG001
        nonlocal call_count
        call_count += 1
        msg = "simulated: server slow to respond"
        raise ReadTimeout(msg)

    with (
        patch.object(
            sink, "_bulk_insert_via_clickhouse_connect", side_effect=timeout_once,
        ),
        pytest.raises(ReadTimeout),
    ):
        sink._bulk_insert_via_clickhouse_connect_with_retry(  # noqa: SLF001
            "default.retry_test_stream", {}, [{"id": 1}],
        )

    assert call_count == 1  # not retried at all


def test_bulk_insert_records_falls_back_after_retries_exhausted() -> None:
    """The full bulk_insert_records path still falls back to SQLAlchemy eventually."""
    sink = _make_sink({**CONFIG, "insert_retry_max_tries": 2})

    fast_path_calls = 0

    def always_fails(full_table_name, schema, records):  # noqa: ARG001
        nonlocal fast_path_calls
        fast_path_calls += 1
        msg = "simulated: connection refused"
        raise RequestsConnectionError(msg)

    with (
        patch.object(
            sink, "_bulk_insert_via_clickhouse_connect", side_effect=always_fails,
        ),
        patch(
            "target_clickhouse.sinks.SQLSink.bulk_insert_records",
            return_value=1,
        ) as mock_fallback,
    ):
        result = sink.bulk_insert_records("default.retry_test_stream", {}, [{"id": 1}])

    assert fast_path_calls == 2  # noqa: PLR2004  # retried per insert_retry_max_tries, then gave up
    mock_fallback.assert_called_once()
    assert result == 1
