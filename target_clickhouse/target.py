from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_clickhouse.engine_class import SupportedEngines
from target_clickhouse.sinks import (
    ClickhouseSink,
)


class TargetClickhouse(SQLTarget):
    """SQL-based target for Clickhouse."""

    name = "target-clickhouse"

    config_jsonschema = th.PropertiesList(
        # connection properties
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="The SQLAlchemy connection string for the ClickHouse database. "
                        "Used if set, otherwise separate settings are used",
        ),
        th.Property(
            "driver",
            th.StringType,
            required=False,
            description="Driver type",
            default="http",
            allowed_values=["http", "native", "asynch"],
        ),
        th.Property(
            "username",
            th.StringType,
            required=False,
            description="Database user",
            default="default",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="Username password",
            secret=True,
        ),
        th.Property(
            "host",
            th.StringType,
            required=False,
            description="Database host",
            default="localhost",
        ),
        th.Property(
            "port",
            th.IntegerType,
            required=False,
            description="Database connection port",
            default=8123,
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=(
                        "Enable an ssh tunnel (also known as bastion server), see the "
                        "other ssh_tunnel.* properties for more details"
                    ),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description=(
                        "Host of the bastion server, this is the host we'll connect "
                        "to via ssh"
                    ),
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion server",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion server",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion server",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description=(
                        "Private Key Password, leave None if no password is set"
                    ),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
        ),
        th.Property(
            "database",
            th.StringType,
            required=False,
            description="Database name",
            default="default",
        ),
        th.Property(
            "secure",
            th.BooleanType,
            required=False,
            description="Should the connection be secure",
            default=False,
        ),
        th.Property(
            "verify",
            th.BooleanType,
            description="Should secure connection need to verify SSL/TLS",
            default=True,
        ),

        # other settings
        th.Property(
            "engine_type",
            th.StringType,
            required=False,
            description="The engine type to use for the table.",
            allowed_values=[e.value for e in SupportedEngines],
        ),
        th.Property(
            "table_name",
            th.StringType,
            required=False,
            description="The name of the table to write to. Defaults to stream name.",
        ),
        th.Property(
            "table_path",
            th.StringType,
            required=False,
            description="The table path for replicated tables. This is required when "
                        "using any of the replication engines. Check out the "
                        "[documentation](https://clickhouse.com/docs/en/engines/table-engines/"
                        "mergetree-family/replication#replicatedmergetree-parameters) "
                        "for more information. Use `$table_name` to substitute the "
                        "table name.",
        ),
        th.Property(
            "replica_name",
            th.StringType,
            required=False,
            description="The `replica_name` for replicated tables. This is required "
                        "when using any of the replication engines.",
        ),
        th.Property(
            "cluster_name",
            th.StringType,
            required=False,
            description="The cluster to create tables in. This is passed as the "
                        "`clickhouse_cluster` argument when creating a table. "
                        "[Documentation]"
                        "(https://clickhouse.com/docs/en/"
                        "sql-reference/distributed-ddl) "
                        "can be found here.",
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            required=False,
            description="The default target database schema name to use for "
                        "all streams.",
        ),
        th.Property(
            "optimize_after",
            th.BooleanType,
            required=False,
            default=False,
            description="Run 'OPTIMIZE TABLE' after data insert. Useful when"
                        "table engine removes duplicate rows.",
        ),
        th.Property(
            "async_insert",
            th.BooleanType,
            required=False,
            default=False,
            description="Enable ClickHouse server-side async inserts for the "
                        "(default) http driver. Coalesces inserts into larger parts "
                        "to reduce part churn on high-volume ingestion. The target "
                        "waits for the async insert to flush before reporting success.",
        ),
        th.Property(
            "order_by_keys",
            th.ArrayType(th.StringType),
            required=False,
            description="List of columns to order by. Used for engines that require "
                        "ordering.",
        ),
        th.Property(
            "insert_retry_max_tries",
            th.IntegerType,
            required=False,
            default=3,
            description="Max attempts for the native (http driver) bulk insert when "
                        "the connection to ClickHouse cannot be established (DNS "
                        "failure, connection refused, connect timeout). Uses "
                        "exponential backoff between attempts. Only retries "
                        "connection-establishment failures, where no data could "
                        "have been sent yet -- a timeout or error response after the "
                        "server started receiving the batch is not retried here, to "
                        "avoid risking a duplicate insert (ClickHouse has no "
                        "transactions to roll back a partial one). Set to 1 to "
                        "disable retries.",
        ),
        th.Property(
            "max_batch_bytes",
            th.IntegerType,
            required=False,
            default=70_000_000,
            description="Flush the current batch once its accumulated (approximate, "
                        "serialized) record size reaches this many bytes, even if "
                        "batch_size_rows hasn't been reached yet. A safety net "
                        "alongside row-count batching for streams with unusually "
                        "wide records (large JSON blobs, long strings).",
        ),
        th.Property(
            "enable_json",
            th.BooleanType,
            required=False,
            default=False,
            description="Use ClickHouse's native JSON column type for object-typed "
                        "properties, instead of storing them as a JSON-encoded "
                        "string. Requires ClickHouse 24.8 or later -- the JSON type "
                        "only stabilized there, and older servers reject it "
                        "outright. Leave disabled (default) unless you've confirmed "
                        "your ClickHouse server version supports it.",
        ),
    ).to_dict()

    default_sink_class = ClickhouseSink


if __name__ == "__main__":
    TargetClickhouse.cli()
