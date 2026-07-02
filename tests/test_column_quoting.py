"""Tests for get_column_alter_ddl's identifier quoting.

Before this fix, column_name was interpolated into the ALTER TABLE ...
MODIFY COLUMN DDL unquoted. column_name comes from the stream's schema
(user-controlled), so a column named a ClickHouse reserved word (e.g.
"order") produced invalid SQL. These tests compile the DDL against a real
dialect-bound engine and inspect the resulting SQL string -- no live
ClickHouse connection needed, matching get_column_add_ddl's sibling coverage.
"""

from __future__ import annotations

import sqlalchemy

from target_clickhouse.connectors import ClickhouseConnector

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


def test_reserved_word_column_name_is_quoted() -> None:
    connector = ClickhouseConnector(config=CONFIG)
    ddl = connector.get_column_alter_ddl("my_table", "order", sqlalchemy.types.String())

    compiled = str(ddl.compile(connector._engine))  # noqa: SLF001

    assert '"order"' in compiled
    assert "MODIFY COLUMN order " not in compiled


def test_reserved_word_column_name_is_quoted_with_cluster_name() -> None:
    config = {**CONFIG, "cluster_name": "my_cluster"}
    connector = ClickhouseConnector(config=config)
    ddl = connector.get_column_alter_ddl("my_table", "order", sqlalchemy.types.String())

    compiled = str(ddl.compile(connector._engine))  # noqa: SLF001

    assert '"order"' in compiled
    assert "ON CLUSTER my_cluster" in compiled
    assert "MODIFY COLUMN order " not in compiled


def test_ordinary_column_name_still_alters_correctly() -> None:
    """Regression check: a normal, non-reserved column name still works."""
    connector = ClickhouseConnector(config=CONFIG)
    ddl = connector.get_column_alter_ddl("my_table", "email", sqlalchemy.types.String())

    compiled = str(ddl.compile(connector._engine))  # noqa: SLF001

    assert "ALTER TABLE my_table MODIFY COLUMN" in compiled
    assert "email" in compiled
