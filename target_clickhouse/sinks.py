"""clickhouse target sink class, which handles writing streams."""

from __future__ import annotations

import contextlib
from logging import Logger
from typing import Any, Iterable

import backoff
import jsonschema.exceptions as jsonschema_exceptions
import simplejson as json
import sqlalchemy
from pendulum import now
from requests.exceptions import ConnectionError as RequestsConnectionError
from singer_sdk.helpers._compat import (
    date_fromisoformat,
    datetime_fromisoformat,
    time_fromisoformat,
)
from singer_sdk.helpers._typing import (
    DatetimeErrorTreatmentEnum,
    get_datelike_property_type,
    handle_invalid_timestamp_in_record,
)
from singer_sdk.sinks import SQLSink
from sqlalchemy.sql.expression import bindparam

from target_clickhouse.connectors import ClickhouseConnector

# Retried on the native insert path -- deliberately narrow. requests.ConnectionError
# (and its ConnectTimeout subclass) fire when the TCP connection itself couldn't be
# established, i.e. before any data was sent, so a retry can never duplicate rows.
# Excludes ReadTimeout/HTTPError and everything else: those can happen *after* the
# server started receiving the batch, where a blind retry risks inserting it twice
# (ClickHouse has no transactions to roll back a partially-received insert).
RETRYABLE_INSERT_EXCEPTIONS = (RequestsConnectionError,)


class ClickhouseSink(SQLSink):
    """clickhouse target sink class."""

    connector_class = ClickhouseConnector

    # ClickHouse strongly prefers large, infrequent inserts (each insert becomes a
    # part). Default to a larger batch than the SDK's 10k so the columnar insert path
    # amortises well and avoids "too many parts". Users can override with the
    # standard `batch_size_rows` setting.
    MAX_SIZE_DEFAULT = 50000

    # Row-count batching alone can't bound a batch's memory/payload footprint --
    # a stream of unusually wide records (large JSON blobs, long strings) can
    # blow past available memory or a reasonable HTTP payload size long before
    # MAX_SIZE_DEFAULT rows accumulate. This is a safety net alongside the
    # row-count cap, not a replacement for it. Override with the
    # `max_batch_bytes` setting.
    MAX_BATCH_BYTES_DEFAULT = 70_000_000

    @property
    def _max_batch_bytes(self) -> int:
        return self.config.get("max_batch_bytes", self.MAX_BATCH_BYTES_DEFAULT)

    @property
    def is_full(self) -> bool:
        """Whether the current batch is full by row count or accumulated byte size."""
        return super().is_full or self._batch_bytes >= self._max_batch_bytes

    def process_record(self, record: dict, context: dict) -> None:
        """Stage the record for batch processing, tracking its serialized size.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary.

        """
        # Records reaching process_record() may still contain raw, not-yet-normalized
        # values (e.g. datetime.date) that plain json.dumps() can't serialize -- that
        # normalization happens later in the pipeline. This is only a size estimate,
        # so default=str keeps it robust: every value gets *some* string form, close
        # enough in length to its eventual serialized size, and it never raises.
        self._batch_bytes = getattr(self, "_batch_bytes", 0) + len(
            json.dumps(record, default=str),
        )
        super().process_record(record, context)

    def mark_drained(self) -> None:
        """Reset `records_to_drain` and the byte-size tally for the next batch."""
        super().mark_drained()
        self._batch_bytes = 0

    @property
    def full_table_name(self) -> str:
        """Return the fully qualified table name.

        Returns
            The fully qualified table name.

        """
        # Use the config table name if set.
        _table_name = self.config.get("table_name")

        if _table_name is not None:
            return _table_name

        return self.connector.get_fully_qualified_name(
            table_name=self.table_name,
            schema_name=self.schema_name,
            db_name=self.database_name,
        )

    @property
    def datetime_error_treatment(self) -> DatetimeErrorTreatmentEnum:
        """Return a treatment to use for datetime parse errors: ERROR. MAX, or NULL."""
        return DatetimeErrorTreatmentEnum.NULL

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[dict[str, Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        For the (default) ``http`` driver this uses ``clickhouse-connect``'s native
        columnar insert, which is dramatically faster than the generic row-oriented
        SQLAlchemy path for large datasets (millions-billions of rows) and avoids the
        per-row bind-parameter overhead. The ``native``/``asynch`` drivers, and the
        explicit ``sqlalchemy_url`` escape hatch, fall back to the SQLAlchemy path.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            The number of records inserted, or None if undetectable.

        """
        # Materialise once; we iterate the records more than once below.
        records = list(records)

        # Need to convert any records with a dict/list type to a JSON string.
        for record in records:
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    record[key] = json.dumps(value)

        if self._use_clickhouse_connect():
            try:
                res = self._bulk_insert_via_clickhouse_connect_with_retry(
                    full_table_name,
                    schema,
                    records,
                )
            except Exception as e:  # noqa: BLE001
                # The columnar fast path is strict about column names/types. For any
                # incompatibility (e.g. quoted/camelCase identifiers) -- or a
                # connection failure that outlasted the retries above -- fall back to
                # the proven SQLAlchemy insert so correctness is never compromised.
                self.logger.warning(
                    "clickhouse-connect fast insert failed (%s); "
                    "falling back to the SQLAlchemy insert path.",
                    e,
                )
                res = super().bulk_insert_records(full_table_name, schema, records)
        else:
            res = super().bulk_insert_records(full_table_name, schema, records)

        if self.config.get("optimize_after", False):
            with self.connector._connect() as conn, conn.begin():  # noqa: SLF001
                self.logger.info("Optimizing table: %s", self.full_table_name)
                conn.execute(sqlalchemy.text(f"OPTIMIZE TABLE {self.full_table_name}"))

        return res

    def _use_clickhouse_connect(self) -> bool:
        """Whether to use the fast clickhouse-connect columnar insert path."""
        # clickhouse-connect speaks the HTTP protocol; only use it for the http
        # driver and when the user has not pinned an explicit sqlalchemy_url.
        return (
            self.config.get("driver", "http") == "http"
            and not self.config.get("sqlalchemy_url")
        )

    @property
    def _clickhouse_connect_client(self):
        """Lazily build and cache a clickhouse-connect client from config."""
        client = getattr(self, "_ch_connect_client", None)
        if client is not None:
            return client

        import clickhouse_connect

        # Route through the connector's tunnel-aware host/port -- this is the
        # native bulk-insert path (the connector's whole reason for existing),
        # so it must respect ssh_tunnel.enable the same way the SQLAlchemy/DDL
        # path (self.connector._connect()) already does. Using the connector's
        # own helper (rather than duplicating tunnel-start logic here) also
        # means both paths share the *same* tunnel instance -- the connector
        # is a singleton for the sync's lifetime, so this returns the tunnel
        # already started by whichever path ran first.
        host, port = self.connector._tunneled_host_port(self.config)  # noqa: SLF001

        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=self.config["username"],
            password=self.config.get("password") or "",
            database=self.config["database"],
            secure=self.config.get("secure", False),
            verify=self.config.get("verify", True),
        )
        self._ch_connect_client = client
        return client

    @staticmethod
    def _split_database_table(full_table_name: str) -> tuple[str | None, str]:
        """Split ``db.table`` (with optional backticks) into (database, table)."""
        parts = [p.strip().strip("`") for p in full_table_name.split(".")]
        if len(parts) == 2:  # noqa: PLR2004
            return parts[0], parts[1]
        return None, parts[-1]

    def _bulk_insert_via_clickhouse_connect_with_retry(
        self,
        full_table_name: str,
        schema: dict,
        records: list[dict[str, Any]],
    ) -> int | None:
        """Retry the native insert on connection-establishment failures.

        See RETRYABLE_INSERT_EXCEPTIONS for why only that narrow class is
        retried here -- anything else re-raises immediately so the caller's
        existing fallback-to-SQLAlchemy behavior is unchanged.
        """

        @backoff.on_exception(
            backoff.expo,
            RETRYABLE_INSERT_EXCEPTIONS,
            max_tries=lambda: self.config.get("insert_retry_max_tries", 3),
            max_value=30,
            logger=self.logger,
        )
        def _attempt() -> int | None:
            return self._bulk_insert_via_clickhouse_connect(
                full_table_name, schema, records,
            )

        return _attempt()

    def _bulk_insert_via_clickhouse_connect(
        self,
        full_table_name: str,
        schema: dict,
        records: list[dict[str, Any]],
    ) -> int:
        """Insert records using clickhouse-connect's columnar insert over HTTP.

        Data is sent column-oriented (``column_oriented=True``), which is the fastest
        path for clickhouse-connect — it matches ClickHouse's native columnar wire
        format and avoids a server-side row→column transpose.

        Column names and record keys are conformed with the SDK's name-conforming
        rules (``conform_schema``/``conform_record``) so they match the table columns
        the connector actually created (e.g. camelCase ``Id`` → ``id``).
        """
        if not records:
            return 0

        column_names = list(self.conform_schema(schema)["properties"].keys())
        conformed_records = [self.conform_record(record) for record in records]
        # Build column-major data: one list per column, in schema order.
        column_data = [
            [record.get(column) for record in conformed_records]
            for column in column_names
        ]

        database, table = self._split_database_table(full_table_name)

        settings: dict[str, Any] = {}
        if self.config.get("async_insert", False):
            # Buffer inserts server-side and coalesce into larger parts — recommended
            # for high-frequency/large-volume ingestion to reduce part churn.
            settings["async_insert"] = 1
            settings["wait_for_async_insert"] = 1

        self._clickhouse_connect_client.insert(
            table=table,
            data=column_data,
            column_names=column_names,
            database=database,
            column_oriented=True,
            settings=settings or None,
        )
        return len(records)

    def clean_up(self) -> None:
        """Close the clickhouse-connect client (if opened) at end of stream."""
        client = getattr(self, "_ch_connect_client", None)
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()
            self._ch_connect_client = None
        super().clean_up()

    def activate_version(self, new_version: int) -> None:
        """Bump the active version of the target table.

        Args:
            new_version: The version number to activate.

        """
        # There's nothing to do if the table doesn't exist yet
        # (which it won't the first time the stream is processed)
        if not self.connector.table_exists(self.full_table_name):
            return

        deleted_at = now()

        if not self.connector.column_exists(
            full_table_name=self.full_table_name,
            column_name=self.version_column_name,
        ):
            self.connector.prepare_column(
                self.full_table_name,
                self.version_column_name,
                sql_type=sqlalchemy.types.Integer(),
            )

        if self.config.get("hard_delete", True):
            with self.connector._connect() as conn, conn.begin():  # noqa: SLF001
                conn.execute(
                    sqlalchemy.text(
                        f"ALTER TABLE {self.full_table_name} DELETE "
                        f"WHERE {self.version_column_name} <= {new_version}",
                    ),
                )
            return

        if not self.connector.column_exists(
            full_table_name=self.full_table_name,
            column_name=self.soft_delete_column_name,
        ):
            self.connector.prepare_column(
                self.full_table_name,
                self.soft_delete_column_name,
                sql_type=sqlalchemy.types.DateTime(),
            )

        query = sqlalchemy.text(
            f"ALTER TABLE {self.full_table_name} \n"
            f"UPDATE {self.soft_delete_column_name} = :deletedate \n"
            f"WHERE {self.version_column_name} < :version \n"
            f"  AND {self.soft_delete_column_name} IS NULL\n",
        )
        query = query.bindparams(
            bindparam("deletedate", value=deleted_at, type_=sqlalchemy.types.DateTime),
            bindparam("version", value=new_version, type_=sqlalchemy.types.Integer),
        )
        with self.connector._connect() as conn, conn.begin():  # noqa: SLF001
            conn.execute(query)

    def _validate_and_parse(self, record: dict) -> dict:
        """Pre-validate and repair records for string type mismatches, then validate.

        Args:
            record: Individual record in the stream.

        Returns:
            Validated record.

        """
        # Pre-validate and correct string type mismatches.
        record = pre_validate_for_string_type(record, self.schema, self.logger)

        try:
            self._validator.validate(record)
            self._parse_timestamps_in_record(
                record=record,
                schema=self.schema,
                treatment=self.datetime_error_treatment,
            )
        except jsonschema_exceptions.ValidationError as e:
            if self.logger:
                self.logger.exception(f"Record failed validation: {record}")
            raise e  # : RERAISES

        return record

    def _parse_timestamps_in_record(
        self,
        record: dict,
        schema: dict,
        treatment: DatetimeErrorTreatmentEnum,
    ) -> None:
        """Parse strings to datetime.datetime values, repairing or erroring on failure.

        Attempts to parse every field that is of type date/datetime/time. If its value
        is out of range, repair logic will be driven by the `treatment` input arg:
        MAX, NULL, or ERROR.

        Args:
            record: Individual record in the stream.
            schema: TODO
            treatment: TODO

        """
        for key, value in record.items():
            if key not in schema["properties"]:
                self.logger.warning("No schema for record field '%s'", key)
                continue
            datelike_type = get_datelike_property_type(schema["properties"][key])
            if datelike_type:
                date_val = value
                try:
                    if value is not None:
                        if datelike_type == "time":
                            date_val = time_fromisoformat(date_val)
                        elif datelike_type == "date":
                            # Trim time value from date fields.
                            if "T" in date_val:
                                # Split on T and get the first part.
                                date_val = date_val.split("T")[0]
                                self.logger.warning(
                                    "Trimmed time value from date field '%s': %s",
                                    key,
                                    date_val,
                                )
                            date_val = date_fromisoformat(date_val)
                        else:
                            date_val = datetime_fromisoformat(date_val)
                except ValueError as ex:
                    date_val = handle_invalid_timestamp_in_record(
                        record,
                        [key],
                        date_val,
                        datelike_type,
                        ex,
                        treatment,
                        self.logger,
                    )
                record[key] = date_val


def pre_validate_for_string_type(
    record: dict,
    schema: dict,
    logger: Logger | None = None,
) -> dict:
    """Pre-validate record for string type mismatches and correct them.

    Args:
        record: Individual record in the stream.
        schema: JSON schema for the stream.
        logger: Logger to use for logging.

    Returns:
        Record with corrected string type mismatches.

    """
    if schema is None:
        if logger:
            logger.debug("Schema is None, skipping pre-validation.")
        return record

    for key, value in record.items():
        # Checking if the schema expects a string for this key.
        key_properties = schema.get("properties", {}).get(key, {})
        expected_type = key_properties.get("type")
        if expected_type is None:
            continue
        if not isinstance(expected_type, list):
            expected_type = [expected_type]

        if "null" in expected_type and value is None:
            continue

        if "object" in expected_type and isinstance(value, dict):
            pre_validate_for_string_type(
                value,
                schema.get("properties", {}).get(key),
                logger,
            )
        elif "array" in expected_type and isinstance(value, list):
            items_schema = key_properties.get("items")
            for i, item in enumerate(value):
                if "object" in items_schema["type"] and isinstance(item, dict):
                    value[i] = pre_validate_for_string_type(
                        item,
                        key_properties.get("items"),
                        logger,
                    )
        elif "string" in expected_type and not isinstance(value, str):
            # Convert the value to string if it's not already a string.
            record[key] = (
                json.dumps(record[key])
                if isinstance(value, (dict, list))
                else str(value)
            )
            if logger:
                logger.debug(
                    f"Converted field {key} to string: {record[key]}",
                )

    return record
