"""Unit tests for keyless-stream engine fallback in create_engine_wrapper.

A stream with no primary key is created with ORDER BY tuple() (an empty sorting
key). Under a collapsing engine (ReplacingMergeTree and friends) every row then
shares the same key, so OPTIMIZE FINAL would reduce the whole table to a single
row. create_engine_wrapper must fall back to a non-collapsing engine for keyless
streams, while leaving keyed streams on the requested engine.

Pure unit tests -- they only construct the engine object and check its class, so
no ClickHouse instance is required.
"""

from __future__ import annotations

from clickhouse_sqlalchemy import engines

from target_clickhouse.engine_class import SupportedEngines, create_engine_wrapper


def test_keyless_replacingmergetree_falls_back_to_mergetree() -> None:
    engine = create_engine_wrapper(
        engine_type=SupportedEngines.REPLACING_MERGE_TREE,
        primary_keys=[],
        table_name="keyless_stream",
    )
    # Not merely "an instance of MergeTree" (ReplacingMergeTree subclasses it) --
    # the exact class must be the non-collapsing MergeTree.
    assert type(engine) is engines.MergeTree


def test_keyless_summingmergetree_falls_back_to_mergetree() -> None:
    engine = create_engine_wrapper(
        engine_type=SupportedEngines.SUMMING_MERGE_TREE,
        primary_keys=[],
        table_name="keyless_stream",
    )
    assert type(engine) is engines.MergeTree


def test_keyed_replacingmergetree_is_preserved() -> None:
    engine = create_engine_wrapper(
        engine_type=SupportedEngines.REPLACING_MERGE_TREE,
        primary_keys=["id"],
        table_name="keyed_stream",
    )
    assert type(engine) is engines.ReplacingMergeTree


def test_keyless_plain_mergetree_is_unchanged() -> None:
    engine = create_engine_wrapper(
        engine_type=SupportedEngines.MERGE_TREE,
        primary_keys=[],
        table_name="keyless_stream",
    )
    assert type(engine) is engines.MergeTree


def test_keyless_string_engine_type_falls_back() -> None:
    """Config supplies engine_type as a plain string, not the enum member."""
    engine = create_engine_wrapper(
        engine_type="ReplacingMergeTree",
        primary_keys=[],
        table_name="keyless_stream",
    )
    assert type(engine) is engines.MergeTree


def test_keyless_replicated_replacing_falls_back_to_replicated_mergetree() -> None:
    engine = create_engine_wrapper(
        engine_type=SupportedEngines.REPLICATED_REPLACING_MERGE_TREE,
        primary_keys=[],
        table_name="keyless_stream",
        config={
            "table_path": "/clickhouse/tables/{shard}/keyless_stream",
            "replica_name": "{replica}",
        },
    )
    assert type(engine) is engines.ReplicatedMergeTree
