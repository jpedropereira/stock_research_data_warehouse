from datetime import datetime, timedelta
from os import path
from tempfile import NamedTemporaryFile

import yaml
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.config import PROJECT_ROOT, STAGING_SCHEMA, now_tz
from include.stocks.constants import BUCKET_STOCKS_YAHOO_FINANCE
from include.stocks.index_history import (
    get_index_symbols_from_wikipedia,
    get_stocks_historical_data,
)
from include.utils import render_ddl
from plugins.operators import EnforceLatestFileOperator, ExtractToStagingOperator

yaml_path = path.join(PROJECT_ROOT, "include", "raw_files_column_mappings.yaml")
table_name = "historical_data_sp500"


@dag(
    start_date=datetime(2025, 7, 28),
    schedule="@daily",
    catchup=False,
    tags=["sp_500", "stocks", "data_extraction"],
    params={
        "extract_start_date": (now_tz() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "extract_end_date": now_tz().strftime("%Y-%m-%d"),
    },
)
def extract_sp500():
    """
    Airflow DAG to extract S&P 500 historical stock data, upload to MinIO,
    and load into a Postgres staging table.
    Steps:
      1. Get S&P 500 ticker symbols from Wikipedia
      2. Extract historical stock data for all tickers
      3. Upload extracted data as CSV to MinIO
      4. Create staging table if not exists
      5. Load CSV data from MinIO into Postgres staging table using a custom operator

    """

    @task()
    def start_task():
        return None

    @task()
    def end_task():
        return None

    @task()
    def extract_symbols():
        with open(path.join(PROJECT_ROOT, "include", "stocks", "indexes.yaml"), "r") as f:
            indexes = yaml.safe_load(f)
        url = indexes["SP500"]["wikipage_url"]
        return get_index_symbols_from_wikipedia(url)

    @task()
    def extract_historical_data(symbols, **kwargs):
        """Extract stock data and upload to S3/MinIO."""
        s3_hook = S3Hook(aws_conn_id="minio_conn")

        conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}
        start_date = conf.get("extract_start_date") or kwargs["params"]["extract_start_date"]
        end_date = conf.get("extract_end_date") or kwargs["params"]["extract_end_date"]

        timestamp = now_tz().strftime("%Y%m%d_%H%M%S")
        file_name = f"sp500_data/{timestamp}_from_{start_date}_to_{end_date}.csv"

        data = get_stocks_historical_data(symbols, start_date, end_date)
        data["extraction_datetime"] = now_tz()

        with NamedTemporaryFile(suffix=".csv") as tmp:
            data.to_csv(tmp.name, index=False)

            s3_hook.load_file(
                filename=tmp.name,
                key=file_name,
                bucket_name=BUCKET_STOCKS_YAHOO_FINANCE,
                replace=True,
            )

        return file_name

    create_table = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id="datawarehouse_conn",
        sql=render_ddl(
            column_mapping_yaml_path=yaml_path, table_name=table_name, schema_name=STAGING_SCHEMA
        ),
    )

    load_to_staging = ExtractToStagingOperator(
        task_id="load_to_staging",
        s3_conn_id="minio_conn",
        bucket_name=BUCKET_STOCKS_YAHOO_FINANCE,
        object_key="{{ ti.xcom_pull(task_ids='extract_historical_data') }}",
        postgres_conn_id="datawarehouse_conn",
        schema_name=STAGING_SCHEMA,
        table_name=table_name,
        column_mapping_yaml_path=yaml_path,
    )

    enforce_lastest_files = EnforceLatestFileOperator(
        task_id="enforce_lastest_files",
        postgres_conn_id="datawarehouse_conn",
        deduplication_columns=["date", "ticker"],
        schema_name=STAGING_SCHEMA,
        table_name=table_name,
    )

    start_task = start_task()
    end_task = end_task()
    extract_symbols = extract_symbols()
    extract_historical_data = extract_historical_data(extract_symbols)

    (
        start_task
        >> extract_symbols
        >> [extract_historical_data, create_table]
        >> load_to_staging
        >> enforce_lastest_files
        >> end_task
    )


extract_sp500()
