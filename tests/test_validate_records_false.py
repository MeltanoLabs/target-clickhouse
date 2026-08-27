"""Regression test: `validate_records: false` must not crash `_validate_and_parse`."""

from __future__ import annotations

from target_clickhouse.sinks import ClickhouseSink
from target_clickhouse.target import TargetClickhouse

SCHEMA = {
    "type": "object",
    "properties": {
        "id": {"type": "integer"},
        "name": {"type": ["string", "null"]},
        "signup_date": {"type": "string", "format": "date"},
    },
    "required": ["id"],
}


def _make_sink(*, validate_records: bool) -> ClickhouseSink:
    config = {
        "sqlalchemy_url": "clickhouse+http://default:@localhost:18123",
        "validate_records": validate_records,
    }
    target = TargetClickhouse(config=config)
    return ClickhouseSink(target, "test_stream", SCHEMA, ["id"])


def test_validate_and_parse_with_validation_enabled():
    sink = _make_sink(validate_records=True)
    record = {"id": 1, "name": "Ada", "signup_date": "2024-01-01"}
    result = sink._validate_and_parse(record)  # noqa: SLF001
    assert result["id"] == 1


def test_validate_and_parse_with_validation_disabled_does_not_raise():
    """Regression test for the missing None-guard.

    `get_validator()` returns None when validate_records is False (see
    singer_sdk.sinks.core.RecordSinkBase) -- `_validate_and_parse` must guard
    against that instead of calling `.validate()` on None.
    """
    sink = _make_sink(validate_records=False)
    assert sink._validator is None  # noqa: SLF001
    record = {"id": 1, "name": "Ada", "signup_date": "2024-01-01"}
    result = sink._validate_and_parse(record)  # noqa: SLF001
    assert result["id"] == 1
