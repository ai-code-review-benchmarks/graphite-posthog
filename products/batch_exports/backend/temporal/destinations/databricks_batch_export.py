import json
import time
import typing as t
import asyncio
import datetime as dt
import contextlib
import dataclasses

from django.conf import settings

from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sql.exc import OperationalError
from structlog.contextvars import bind_contextvars
from temporalio import activity, workflow
from temporalio.common import RetryPolicy

from posthog.batch_exports.service import (
    BatchExportField,
    BatchExportInsertInputs,
    BatchExportModel,
    BatchExportSchema,
    DatabricksBatchExportInputs,
)
from posthog.temporal.common.base import PostHogWorkflow
from posthog.temporal.common.heartbeat import Heartbeater
from posthog.temporal.common.logger import get_produce_only_logger, get_write_only_logger

from products.batch_exports.backend.temporal.batch_exports import (
    StartBatchExportRunInputs,
    default_fields,
    get_data_interval,
    start_batch_export_run,
)
from products.batch_exports.backend.temporal.pipeline.consumer import Consumer, run_consumer_from_stage
from products.batch_exports.backend.temporal.pipeline.entrypoint import execute_batch_export_using_internal_stage
from products.batch_exports.backend.temporal.pipeline.producer import Producer
from products.batch_exports.backend.temporal.pipeline.types import BatchExportResult
from products.batch_exports.backend.temporal.spmc import RecordBatchQueue, wait_for_schema_or_producer
from products.batch_exports.backend.temporal.utils import handle_non_retryable_errors

LOGGER = get_write_only_logger(__name__)
EXTERNAL_LOGGER = get_produce_only_logger("EXTERNAL")


NON_RETRYABLE_ERROR_TYPES: list[str] = []


class DatabricksConnectionError(Exception):
    """Error for Databricks connection."""

    pass


@dataclasses.dataclass(kw_only=True)
class DatabricksInsertInputs(BatchExportInsertInputs):
    """Inputs for Databricks.

    server_hostname: the Server Hostname value for user's all-purpose compute or SQL warehouse.
    http_path: HTTP Path value for user's all-purpose compute or SQL warehouse.
    client_id: the service principal's UUID or Application ID value.
    client_secret: the Secret value for the service principal's OAuth secret.
    """

    # TODO - some of this will go in the integration model once ready

    server_hostname: str
    http_path: str
    client_id: str
    client_secret: str
    catalog: str
    schema: str
    table_name: str


# TODO
class DatabricksClient:
    # How often to poll for query status. This is a trade-off between responsiveness and number of
    # queries we make to Databricks. 1 second has been chosen rather arbitrarily.
    DEFAULT_POLL_INTERVAL = 1.0

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        client_id: str,
        client_secret: str,
        catalog: str,
        schema: str,
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.client_id = client_id
        self.client_secret = client_secret
        self.catalog = catalog
        self.schema = schema

        self._connection: None | sql.Connection = None

        self.logger = LOGGER.bind(server_hostname=server_hostname, http_path=http_path)

    @classmethod
    def from_inputs(cls, inputs: DatabricksInsertInputs) -> t.Self:
        return cls(
            server_hostname=inputs.server_hostname,
            http_path=inputs.http_path,
            client_id=inputs.client_id,
            client_secret=inputs.client_secret,
            catalog=inputs.catalog,
            schema=inputs.schema,
        )

    @property
    def connection(self) -> sql.Connection:
        """Raise if a `sql.Connection` hasn't been established, else return it."""
        if self._connection is None:
            # this should never happen and inidicates a bug in our code (i.e. trying to execute a query before
            # establishing a connection)
            raise Exception("Not connected, open a connection by calling `connect`")
        return self._connection

    def get_credential_provider(self):
        config = Config(
            host=f"https://{self.server_hostname}", client_id=self.client_id, client_secret=self.client_secret
        )
        return oauth_service_principal(config)

    @contextlib.asynccontextmanager
    async def connect(self):
        """Manage a Databricks connection.

        Methods that require a connection should be ran within this block.

        We call `use_catalog` and `use_schema` to ensure that all queries are run in the correct catalog and schema.
        """
        self.logger.debug("Initializing Databricks connection")

        try:
            self._connection = await asyncio.to_thread(
                sql.connect,
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                credentials_provider=self.get_credential_provider(),
                # user agent can be used for usage tracking
                user_agent_entry="PostHog batch exports",
                # TODO
                # staging_allowed_local_path=str(staging_allowed_local_path),
            )
        except OperationalError as err:
            # TODO: check what kinds of errors we get here
            raise DatabricksConnectionError(f"Failed to connect to Databricks: {err}") from err

        self.logger.debug("Connected to Databricks")

        await self.use_catalog(self.catalog)
        await self.use_schema(self.schema)

        try:
            yield self
        finally:
            await asyncio.to_thread(self._connection.close)
            self._connection = None

    async def execute_query(self, query: str, parameters: dict | None = None, fetch_results: bool = True):
        """Execute a query and wait for it to complete.

        We run the query in a separate thread to avoid blocking the event loop in the main thread.
        """
        # TODO - ensure errors are raised correctly
        query_start_time = time.time()
        self.logger.debug("Executing query: %s", query)

        with self.connection.cursor() as cursor:
            try:
                await asyncio.to_thread(cursor.execute, query, parameters)
            finally:
                query_execution_time = time.time() - query_start_time
                self.logger.debug("Query completed in %.2fs", query_execution_time)

            if not fetch_results:
                return

            results = await asyncio.to_thread(cursor.fetchall)
            description = cursor.description
            return results, description

    async def execute_async_query(
        self,
        query: str,
        parameters: dict | None = None,
        poll_interval: float | None = None,
        fetch_results: bool = True,
        timeout: float = 60 * 60,  # 1 hour
    ):
        """Execute a query asynchronously and poll for results.

        This is useful for long running queries as it means we don't need to maintain a network connection to the
        Databricks server, which could timeout or be interrupted.

        Executing the query and polling for results are done in separate threads in order to avoid blocking the event
        loop in the main thread.

        Args:
            query: The query to execute.
            parameters: The parameters to bind to the query.
            poll_interval: The interval (in seconds) to poll for results.
            fetch_results: Whether to fetch results.
            timeout: The timeout (in seconds) to wait for the query to complete.
                This is more of a safeguard than anything else, just to prevent us waiting forever.

        Returns:
            If `fetch_results` is `True`, a tuple containing:
            - The query results as a list of tuples or dicts
            - The cursor description (containing list of fields in result)
            Else when `fetch_results` is `False` we return `None`.
        """
        # TODO - ensure errors are raised correctly
        poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL

        query_start_time = time.time()
        self.logger.debug("Executing async query: %s", query)

        with self.connection.cursor() as cursor:
            await asyncio.to_thread(cursor.execute_async, query, parameters)

            self.logger.debug("Waiting for async query to complete")

            while await asyncio.to_thread(cursor.is_query_pending):
                await asyncio.sleep(poll_interval)
                if time.time() - query_start_time > timeout:
                    raise TimeoutError(f"Timed out waiting for query to complete after {timeout} seconds")

            # this should return an exception if the query failed so ensure we log the query time
            try:
                await asyncio.to_thread(cursor.get_async_execution_result)
            finally:
                query_execution_time = time.time() - query_start_time
                self.logger.debug("Async query completed in %.2fs", query_execution_time)

            if fetch_results is False:
                return

            self.logger.debug("Fetching query results")

            results = await asyncio.to_thread(cursor.fetchall)
            description = cursor.description

            self.logger.debug("Finished fetching query results")

            return results, description

    async def use_catalog(self, catalog: str):
        await self.execute_query(f"USE CATALOG {catalog}", fetch_results=False)

    async def use_schema(self, schema: str):
        await self.execute_query(f"USE SCHEMA {schema}", fetch_results=False)


# TODO
def databricks_default_fields() -> list[BatchExportField]:
    """Default fields for a Databricks batch export.

    Starting from the common default fields, we add and tweak some fields for
    backwards compatibility.
    """
    batch_export_fields = default_fields()
    batch_export_fields.append(
        {
            "expression": "nullIf(JSONExtractString(properties, '$ip'), '')",
            "alias": "ip",
        }
    )
    # Fields kept for backwards compatibility with legacy apps schema.
    batch_export_fields.append({"expression": "elements_chain", "alias": "elements"})
    batch_export_fields.append({"expression": "''", "alias": "site_url"})
    batch_export_fields.pop(batch_export_fields.index({"expression": "created_at", "alias": "created_at"}))

    # For historical reasons, 'set' and 'set_once' are prefixed with 'people_'.
    set_field = batch_export_fields.pop(batch_export_fields.index(BatchExportField(expression="set", alias="set")))
    set_field["alias"] = "people_set"

    set_once_field = batch_export_fields.pop(
        batch_export_fields.index(BatchExportField(expression="set_once", alias="set_once"))
    )
    set_once_field["alias"] = "people_set_once"

    batch_export_fields.append(set_field)
    batch_export_fields.append(set_once_field)

    return batch_export_fields


# TODO
class DatabricksConsumer(Consumer):
    """A consumer that uploads data to Databricks from the internal stage."""

    # def __init__(
    #     self,
    #     snowflake_client: SnowflakeClient,
    #     snowflake_table: str,
    #     snowflake_table_stage_prefix: str,
    # ):
    #     super().__init__()

    #     self.snowflake_client = snowflake_client
    #     self.snowflake_table = snowflake_table
    #     self.snowflake_table_stage_prefix = snowflake_table_stage_prefix

    #     # Simple file management - no concurrent uploads for now
    #     self.current_file_index = 0
    #     self.current_buffer = NamedBytesIO(
    #         b"", name=f"{self.snowflake_table_stage_prefix}/{self.current_file_index}.parquet.zst"
    #     )

    # async def consume_chunk(self, data: bytes):
    #     """Consume a chunk of data by writing it to the current buffer."""
    #     self.current_buffer.write(data)

    # async def finalize_file(self):
    #     """Finalize the current file and start a new one."""
    #     await self._upload_current_buffer()

    # def _start_new_file(self):
    #     """Start a new file (reset state for file splitting)."""
    #     self.current_file_index += 1
    #     self.current_buffer = NamedBytesIO(
    #         b"", name=f"{self.snowflake_table_stage_prefix}/{self.current_file_index}.parquet.zst"
    #     )

    # async def _upload_current_buffer(self):
    #     """Upload the current buffer to Snowflake, then start a new one."""
    #     buffer_size = self.current_buffer.tell()
    #     if buffer_size == 0:
    #         return  # Nothing to upload

    #     self.logger.info(
    #         "Uploading file %d with %d bytes to Snowflake table '%s'",
    #         self.current_file_index,
    #         buffer_size,
    #         self.snowflake_table,
    #     )

    #     self.current_buffer.seek(0)

    #     await self.snowflake_client.put_file_to_snowflake_table_stage(
    #         file=self.current_buffer,
    #         table_stage_prefix=self.snowflake_table_stage_prefix,
    #         table_name=self.snowflake_table,
    #     )

    #     self.external_logger.info(
    #         "File %d with %d bytes uploaded to Snowflake table '%s'",
    #         self.current_file_index,
    #         buffer_size,
    #         self.snowflake_table,
    #     )

    #     self._start_new_file()

    # async def finalize(self):
    #     """Finalize by uploading any remaining data."""
    #     await self._upload_current_buffer()


@activity.defn
@handle_non_retryable_errors(NON_RETRYABLE_ERROR_TYPES)
async def insert_into_databricks_activity_from_stage(inputs: DatabricksInsertInputs) -> BatchExportResult:
    """Activity to batch export data from internal S3 stage to Databricks."""
    bind_contextvars(
        team_id=inputs.team_id,
        destination="Databricks",
        data_interval_start=inputs.data_interval_start,
        data_interval_end=inputs.data_interval_end,
        batch_export_id=inputs.batch_export_id,
        catalog=inputs.catalog,
        schema=inputs.schema,
        table_name=inputs.table_name,
    )
    external_logger = EXTERNAL_LOGGER.bind()

    external_logger.info(
        "Batch exporting range %s - %s to Databricks: %s.%s.%s",
        inputs.data_interval_start or "START",
        inputs.data_interval_end or "END",
        inputs.catalog,
        inputs.schema,
        inputs.table_name,
    )

    async with Heartbeater():
        model: BatchExportModel | BatchExportSchema | None = None
        if inputs.batch_export_schema is None:
            model = inputs.batch_export_model
        else:
            model = inputs.batch_export_schema

        queue = RecordBatchQueue(max_size_bytes=settings.BATCH_EXPORT_DATABRICKS_RECORD_BATCH_QUEUE_MAX_SIZE_BYTES)
        producer = Producer()
        assert inputs.batch_export_id is not None
        producer_task = await producer.start(
            queue=queue,
            batch_export_id=inputs.batch_export_id,
            data_interval_start=inputs.data_interval_start,
            data_interval_end=inputs.data_interval_end,
            max_record_batch_size_bytes=1024 * 1024 * 10,  # 10MB
        )

        record_batch_schema = await wait_for_schema_or_producer(queue, producer_task)
        if record_batch_schema is None:
            external_logger.info(
                "Batch export will finish early as there is no data matching specified filters in range %s - %s",
                inputs.data_interval_start or "START",
                inputs.data_interval_end or "END",
            )

            return BatchExportResult(records_completed=0, bytes_exported=0)

        # TODO from here
        table_fields, record_batch_schema, known_variant_columns = _get_snowflake_table_settings(
            model=model, record_batch_schema=record_batch_schema
        )

        requires_merge, merge_key, update_key = _get_snowflake_merge_config(model=model)

        data_interval_end_str = dt.datetime.fromisoformat(inputs.data_interval_end).strftime("%Y-%m-%d_%H-%M-%S")
        stage_table_name = (
            f"stage_{inputs.table_name}_{data_interval_end_str}_{inputs.team_id}"
            if requires_merge
            else inputs.table_name
        )

        async with SnowflakeClient.from_inputs(inputs).connect() as snow_client:
            async with (
                snow_client.managed_table(
                    inputs.table_name, data_interval_end_str, table_fields, delete=False
                ) as snow_table,
                snow_client.managed_table(
                    stage_table_name,
                    data_interval_end_str,
                    table_fields,
                    create=requires_merge,
                    delete=requires_merge,
                ) as snow_stage_table,
            ):
                consumer = DatabricksConsumerFromStage(
                    snowflake_client=snow_client,
                    snowflake_table=snow_stage_table if requires_merge else snow_table,
                    snowflake_table_stage_prefix=data_interval_end_str,
                )

                result = await run_consumer_from_stage(
                    queue=queue,
                    consumer=consumer,
                    producer_task=producer_task,
                    schema=record_batch_schema,
                    file_format="Parquet",
                    # TODO - add compression
                    # compression="zstd",
                    include_inserted_at=False,
                    max_file_size_bytes=settings.BATCH_EXPORT_SNOWFLAKE_UPLOAD_CHUNK_SIZE_BYTES,
                    json_columns=known_variant_columns,
                )

                # TODO - maybe move this into the consumer finalize method?
                # Copy all staged files to the table
                await snow_client.copy_loaded_files_to_snowflake_table(
                    snow_stage_table if requires_merge else snow_table,
                    data_interval_end_str,
                    table_fields,
                    file_format="Parquet",
                    known_json_columns=known_variant_columns,
                )

                if requires_merge:
                    await snow_client.amerge_mutable_tables(
                        final_table=snow_table,
                        stage_table=snow_stage_table,
                        update_when_matched=table_fields,
                        merge_key=merge_key,
                        update_key=update_key,
                    )

                return result


@workflow.defn(name="databricks-export", failure_exception_types=[workflow.NondeterminismError])
class DatabricksBatchExportWorkflow(PostHogWorkflow):
    """A Temporal Workflow to export ClickHouse data into Databricks.

    This Workflow is intended to be executed both manually and by a Temporal
    Schedule. When ran by a schedule, `data_interval_end` should be set to
    `None` so that we will fetch the end of the interval from the Temporal
    search attribute `TemporalScheduledStartTime`.
    """

    @staticmethod
    def parse_inputs(inputs: list[str]) -> DatabricksBatchExportInputs:
        """Parse inputs from the management command CLI."""
        loaded = json.loads(inputs[0])
        return DatabricksBatchExportInputs(**loaded)

    @workflow.run
    async def run(self, inputs: DatabricksBatchExportInputs):
        """Workflow implementation to export data to Databricks table."""
        is_backfill = inputs.get_is_backfill()
        is_earliest_backfill = inputs.get_is_earliest_backfill()
        data_interval_start, data_interval_end = get_data_interval(inputs.interval, inputs.data_interval_end)
        should_backfill_from_beginning = is_backfill and is_earliest_backfill

        start_batch_export_run_inputs = StartBatchExportRunInputs(
            team_id=inputs.team_id,
            batch_export_id=inputs.batch_export_id,
            data_interval_start=data_interval_start.isoformat() if not should_backfill_from_beginning else None,
            data_interval_end=data_interval_end.isoformat(),
            exclude_events=inputs.exclude_events,
            include_events=inputs.include_events,
            backfill_id=inputs.backfill_details.backfill_id if inputs.backfill_details else None,
        )
        run_id = await workflow.execute_activity(
            start_batch_export_run,
            start_batch_export_run_inputs,
            start_to_close_timeout=dt.timedelta(minutes=5),
            retry_policy=RetryPolicy(
                initial_interval=dt.timedelta(seconds=10),
                maximum_interval=dt.timedelta(seconds=60),
                maximum_attempts=0,
                non_retryable_error_types=["NotNullViolation", "IntegrityError"],
            ),
        )

        insert_inputs = DatabricksInsertInputs(
            team_id=inputs.team_id,
            server_hostname=inputs.server_hostname,
            http_path=inputs.http_path,
            client_id=inputs.client_id,
            client_secret=inputs.client_secret,
            catalog=inputs.catalog,
            schema=inputs.schema,
            table_name=inputs.table_name,
            data_interval_start=data_interval_start.isoformat() if not should_backfill_from_beginning else None,
            data_interval_end=data_interval_end.isoformat(),
            exclude_events=inputs.exclude_events,
            include_events=inputs.include_events,
            run_id=run_id,
            backfill_details=inputs.backfill_details,
            is_backfill=is_backfill,
            batch_export_model=inputs.batch_export_model,
            batch_export_schema=inputs.batch_export_schema,
            batch_export_id=inputs.batch_export_id,
            destination_default_fields=databricks_default_fields(),
        )

        await execute_batch_export_using_internal_stage(
            insert_into_databricks_activity_from_stage,
            insert_inputs,
            interval=inputs.interval,
        )
