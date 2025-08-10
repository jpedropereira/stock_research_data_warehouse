from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class EnforceLatestFileOperator(BaseOperator):
    """
    Generic operator for deduplicating staging tables by keeping only the latest record per business key.

    This operator:
    1. Creates a view of the latest records for each unique combination of deduplication columns
    2. Deletes any rows from the table that are not present in this view, ensuring only the most
    recent data is kept for each key (based on extraction timestamp and/or file name).

    :param deduplication_columns: List of column names that define the business key for deduplication
        (e.g., ["symbol", "date"])
    :param schema_name: Target schema name
    :param table_name: Target table name
    :param timestamp_in_file_name:
        If True, prioritizes file_name as the primary ordering for recency;
        otherwise, uses extraction_datetime (default: True)
    :param kwargs: Additional keyword arguments passed to BaseOperator

    Example:
        EnforceLatestFileOperator(
            task_id="deduplicate_staging",
            deduplication_columns=["symbol", "date"],
            schema_name="staging",
            table_name="historical_data_sp500",
            timestamp_in_file_name=True,
        )
    """

    def __init__(
        self,
        deduplication_columns: list[str],
        postgres_conn_id,
        schema_name: str,
        table_name: str,
        timestamp_in_file_name: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.deduplication_columns = deduplication_columns
        self.postgres_conn_id = postgres_conn_id
        self.schema_name = schema_name
        self.table_name = table_name
        self.timestamp_in_file_name = timestamp_in_file_name

    def execute(self, context: Context):
        if not self.deduplication_columns:
            raise ValueError("deduplication_columns must not be empty")

        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        deduplication_condition = ", ".join(self.deduplication_columns)

        if self.timestamp_in_file_name:
            order_condition = "file_name DESC, extraction_datetime DESC"
            self.log.info(
                "Removing records from older files based on timestamp in file name "
                f"using the columns: {deduplication_condition}"
            )
        else:
            order_condition = "extraction_datetime DESC, file_name DESC"
            self.log.info(
                "Removing records from older files based on extraction timestamp "
                f"using the columns: {deduplication_condition}"
            )

        exists_condition = "\nAND ".join([f"t1.{col} = t2.{col}" for col in self.deduplication_columns])

        view_sql = f"""
        CREATE OR REPLACE VIEW {self.schema_name}.v_{self.table_name}_latest_files AS
        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                PARTITION BY {deduplication_condition}
                ORDER BY {order_condition}
            ) as file_rank
            FROM {self.schema_name}.{self.table_name}
        )

        SELECT *
        FROM ranked
        WHERE file_rank = 1
        """

        postgres_hook.run(view_sql)

        deletion_sql = f"""
            DELETE FROM {self.schema_name}.{self.table_name} t1
            WHERE NOT EXISTS (
                SELECT 1
                FROM {self.schema_name}.v_{self.table_name}_latest_files t2
                WHERE {exists_condition}
            )
        """

        postgres_hook.run(deletion_sql)
