from __future__ import annotations

import atexit
import contextlib
import signal
import typing
from typing import TYPE_CHECKING, Any

import sqlalchemy.types
from clickhouse_sqlalchemy import (
    Table,
)
from clickhouse_sqlalchemy import (
    types as clickhouse_sqlalchemy_types,
)
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from sqlalchemy import Column, MetaData, create_engine, text
>>>>>>> 0066845 ([MEL-508] auto create db)

from target_clickhouse.engine_class import SupportedEngines, create_engine_wrapper
from target_clickhouse.ssh_tunnel import SSHTunnelForwarder, start_tunnel_if_enabled

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class ClickHouseJSON(sqlalchemy.types.UserDefinedType):
    """ClickHouse's native ``JSON`` column type.

    clickhouse_sqlalchemy has no built-in JSON type (unlike Array/Tuple/Map,
    which it does define). UserDefinedType.get_col_spec() is SQLAlchemy's
    standard, dialect-agnostic hook for "just emit this literal DDL string",
    avoiding the need to register a dialect-specific type-compiler visitor for
    a single type.

    Only stabilized in ClickHouse 24.8+; older servers (e.g. this connector's
    own CI, pinned to 23.4) reject it outright unless the deprecated
    `allow_experimental_object_type` setting is set -- which enables a
    different, older `Object('json')` type, not this one. That's why this
    type is only ever used when the user opts in via `enable_json`, never by
    default.
    """

    def get_col_spec(self, **kw: Any) -> str:  # noqa: ARG002
        """Return the literal DDL type name.

        Args:
            kw: Unused; required by the UserDefinedType interface.

        Returns:
            The ClickHouse column type string.

        """
        return "JSON"


class ClickhouseConnector(SQLConnector):
    """Clickhouse Meltano Connector.

    Inherits from `SQLConnector` class, overriding methods where needed
    for Clickhouse compatibility.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the connector, tracking any SSH tunnel it starts."""
        super().__init__(*args, **kwargs)
        self._ssh_tunnel: SSHTunnelForwarder | None = None

    def _tunneled_host_port(self, config: dict) -> tuple[str, int]:
        """Return the (host, port) to connect to, starting an SSH tunnel if configured.

        The connector is a singleton reused for the life of the sync, so the
        tunnel (if any) is started once on first use and cached on `self`.

        Args:
            config: The configuration for the connector.

        Returns:
            The (host, port) to use, transformed to the tunnel's local bind
            address if `ssh_tunnel.enable` is set, otherwise the config's own
            `host`/`port` unchanged.

        """
        if self._ssh_tunnel is None and (config.get("ssh_tunnel") or {}).get("enable"):
            self._ssh_tunnel = start_tunnel_if_enabled(config)
            # Clean up on process exit/signal, mirroring tap-postgres.
            atexit.register(self._stop_ssh_tunnel)
            signal.signal(signal.SIGTERM, lambda *_: self._stop_ssh_tunnel())
            signal.signal(signal.SIGINT, lambda *_: self._stop_ssh_tunnel())

        if self._ssh_tunnel is not None:
            return self._ssh_tunnel.local_bind_host, self._ssh_tunnel.local_bind_port

        return config["host"], config["port"]

    def _stop_ssh_tunnel(self) -> None:
        """Stop the SSH tunnel, if one was started."""
        if self._ssh_tunnel is not None:
            self._ssh_tunnel.stop()

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for clickhouse.

        Args:
            config: The configuration for the connector.

        """
        if config.get("sqlalchemy_url"):
            return super().get_sqlalchemy_url(config)

        host, port = self._tunneled_host_port(config)

        # clickhouse_sqlalchemy's HTTP driver only special-cases the literal string
        # "False"/"false" for `verify` (clickhouse_sqlalchemy/drivers/http/base.py) --
        # anything else, including the string "True", is forwarded to `requests`
        # verbatim. `requests` only treats `verify` as "use the default CA bundle"
        # when it *is* the `True` singleton (an `is not True` identity check); any
        # other value is treated as a literal path to a CA bundle file, so a
        # stringified "True" crashes with "Could not find a suitable TLS CA
        # certificate bundle, invalid path: True" -- verified against a real TLS
        # ClickHouse endpoint. So verify=True must be conveyed by *omitting* the
        # query param (letting the driver's own real-bool default apply), never by
        # sending the string "True".
        query: dict[str, str] = {}
        if config["driver"] == "http":
            if config["secure"]:
                query["protocol"] = "https"

                if not config["verify"]:
                    query["verify"] = "False"
                    # disable urllib3 warning
                    import urllib3

                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            else:
                query["protocol"] = "http"
        else:
            query["secure"] = str(config["secure"])
            if not config["verify"]:
                query["verify"] = "False"

        # Built via URL.create (not an f-string) so username/password/database are
        # properly percent-encoded. A raw password containing "@", ":", or "/"
        # would otherwise be misparsed as URL delimiters, breaking the connection
        # string before any connection is attempted.
        return URL.create(
            drivername=f"clickhouse+{config['driver']}",
            username=config["username"],
            password=config["password"],
            host=host,
            port=port,
            database=config["database"],
            query=query,
        ).render_as_string(hide_password=False)

    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine for clickhouse.

        ClickHouse has no schema namespace — a database *is* the schema — so the
        configured ``database`` must exist before any tables can be created in
        it. ``prepare_schema`` cannot do this: by the time it runs the engine is
        already bound to the (possibly missing) target database. We therefore
        ensure the database exists here first, bootstrapping against the
        always-present ``default`` database.
        """
        self._ensure_database_exists()
        return create_engine(self.get_sqlalchemy_url(self.config))

    def _ensure_database_exists(self) -> None:
        """Create the configured target database if it does not already exist."""
        database = self.config.get("database")
        # ``default`` always exists; nothing to create (and nothing to bootstrap
        # against if that is also the target).
        if not database or database == "default":
            return

        bootstrap_url = self.get_sqlalchemy_url({**self.config, "database": "default"})
        bootstrap_engine = create_engine(bootstrap_url)
        try:
            with bootstrap_engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{database}`"))
        finally:
            bootstrap_engine.dispose()

    @contextlib.contextmanager
    def _connect(self) -> typing.Iterator[sqlalchemy.engine.Connection]:
        # patch to overcome error in sqlalchemy-clickhouse driver
        if self.config.get("driver") == "native":
            kwargs = {"stream_results": True, "max_row_buffer": 1000}
        else:
            kwargs = {"stream_results": True}
        with self._engine.connect().execution_options(**kwargs) as conn:
            yield conn

    def to_sql_type(
        self,
        jsonschema_type: dict,
        **kwargs,
    ) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.

        """
        sql_type = th.to_sql_type(jsonschema_type)
        is_primary_key = kwargs.get("is_primary_key", False)

        # th.to_sql_type() already resolved "object" schemas to a generic VARCHAR
        # (indistinguishable at this point from a genuine string property), so
        # object detection has to look at the original JSON Schema type, not
        # sql_type. Opt-in only (enable_json): ClickHouse's JSON type only
        # stabilized in 24.8+, and this connector's own CI target (23.4) rejects
        # it outright, so it must never be the default.
        schema_type_raw = jsonschema_type.get("type", [])
        is_object_schema = "object" in (
            schema_type_raw if isinstance(schema_type_raw, list) else [schema_type_raw]
        )
        if is_object_schema and self.config.get("enable_json", False):
            sql_type = typing.cast(sqlalchemy.types.TypeEngine, ClickHouseJSON())
        # Clickhouse does not support the DECIMAL type without providing precision,
        # so we need to use the FLOAT type.
        elif type(sql_type) == sqlalchemy.types.DECIMAL:
            sql_type = typing.cast(
                sqlalchemy.types.TypeEngine,
                sqlalchemy.types.FLOAT(),
            )
        elif type(sql_type) == sqlalchemy.types.INTEGER:
            sql_type = typing.cast(
                sqlalchemy.types.TypeEngine,
                clickhouse_sqlalchemy_types.Int64(),
            )
        elif type(sql_type) == sqlalchemy.types.DATE:
            sql_type = typing.cast(
                sqlalchemy.types.TypeEngine,
                clickhouse_sqlalchemy_types.Nullable(clickhouse_sqlalchemy_types.Date32)
                if not is_primary_key
                else clickhouse_sqlalchemy_types.Date32,
            )
        # All date and time types should be flagged as Nullable to allow for NULL value.
        elif (
            type(sql_type)
            in [
                sqlalchemy.types.TIMESTAMP,
                sqlalchemy.types.TIME,
                sqlalchemy.types.DATETIME,
            ]
            and not is_primary_key
        ):
            sql_type = clickhouse_sqlalchemy_types.Nullable(sql_type)

        # Wrap any type in Nullable if the JSON schema allows null values
        # and it's not already Nullable and not a primary key.
        schema_type = jsonschema_type.get("type", [])
        if (
            isinstance(schema_type, list)
            and "null" in schema_type
            and not is_primary_key
            and not isinstance(sql_type, clickhouse_sqlalchemy_types.Nullable)
        ):
            sql_type = clickhouse_sqlalchemy_types.Nullable(sql_type)

        return sql_type

    def create_empty_table(
        self,
        full_table_name: str,
        schema: dict,
        primary_keys: list[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> None:
        """Create an empty target table, using Clickhouse Engine.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.

        """
        if as_temp_table:
            msg = "Temporary tables are not supported."
            raise NotImplementedError(msg)

        _ = partition_keys  # Not supported in generic implementation.

        _, _, table_name = self.parse_full_table_name(full_table_name)

        # If config table name is set, then use it instead of the table name.
        if self.config.get("table_name"):
            table_name = self.config.get("table_name")

        # Do not set schema, as it is not supported by Clickhouse.
        meta = MetaData(schema=None)

        columns: list[Column] = []
        primary_keys = primary_keys or []

        # If config engine type is set, then use it instead of the default engine type.
        if self.config.get("engine_type"):
            engine_type = self.config.get("engine_type")
        else:
            engine_type = SupportedEngines.MERGE_TREE

        try:
            properties: dict = schema["properties"]
        except KeyError as e:
            msg = f"Schema for '{full_table_name}' does not define properties: {schema}"
            raise RuntimeError(msg) from e
        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            sql_type = self.to_sql_type(
                property_jsonschema,
                is_primary_key=is_primary_key,
            )
            columns.append(
                Column(
                    property_name,
                    sql_type,
                    primary_key=is_primary_key,
                ),
            )

        table_engine = create_engine_wrapper(
            engine_type=engine_type,
            primary_keys=primary_keys,
            table_name=table_name,
            config=self.config,
            order_by_keys=self.config.get("order_by_keys"),
        )

        table_args = {}
        if self.config.get("cluster_name"):
            table_args["clickhouse_cluster"] = self.config.get("cluster_name")

        _ = Table(table_name, meta, *columns, table_engine, **table_args)
        meta.create_all(self._engine)

    def prepare_schema(self, _: str) -> None:
        """Create the target database schema.

        In Clickhouse, a schema is a database, so this method is a no-op.

        Args:
            schema_name: The target schema name.

        """
        return

    def prepare_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.

        """
        if not self.column_exists(full_table_name, column_name):
            self._create_empty_column(
                full_table_name=full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )
            return

        with contextlib.suppress(NotImplementedError):
            self._adapt_column_type(
                full_table_name,
                column_name=column_name,
                sql_type=sql_type,
            )

    @staticmethod
    def get_column_add_ddl(
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the create column DDL statement.

        Override this if your database uses a different syntax for creating columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.

        """
        create_column_clause = sqlalchemy.schema.CreateColumn(
            sqlalchemy.Column(
                column_name,
                column_type,
            ),
        )
        return sqlalchemy.DDL(
            (
                "ALTER TABLE %(table_name)s ADD COLUMN IF NOT EXISTS "
                "%(create_column_clause)s"
            ),
            {
                "table_name": table_name,
                "create_column_clause": create_column_clause,
            },
        )

    def get_column_alter_ddl(
        self,
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Overrides the static method in the base class to support ON CLUSTER.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.

        """
        # column_name comes from the stream's schema (user-controlled), so it must
        # be dialect-quoted -- otherwise a column named a ClickHouse reserved word
        # (e.g. "order") produces invalid SQL. get_column_add_ddl avoids this by
        # going through sqlalchemy.Column/CreateColumn, which quote internally;
        # this method builds the DDL string directly, so it must quote explicitly.
        quoted_column_name = self._dialect.identifier_preparer.quote(column_name)
        if self.config.get("cluster_name"):
            return sqlalchemy.DDL(
                "ALTER TABLE %(table_name)s ON CLUSTER %(cluster_name)s "
                "MODIFY COLUMN %(column_name)s %(column_type)s",
                {
                    "table_name": table_name,
                    "column_name": quoted_column_name,
                    "column_type": column_type,
                    "cluster_name": self.config.get("cluster_name"),
                },
            )
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s MODIFY COLUMN %(column_name)s %(column_type)s",
            {
                "table_name": table_name,
                "column_name": quoted_column_name,
                "column_type": column_type,
            },
        )
