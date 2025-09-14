import logging
from datetime import datetime, timedelta
from os import path
from tempfile import NamedTemporaryFile
from time import sleep

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.common.constants import COMMON_ARGS, POSTGRES_CONN_ID
from include.config import PROJECT_ROOT, STAGING_SCHEMA, now_tz
from include.stocks.constants import BUCKET_STOCKS_YAHOO_FINANCE
from include.stocks.stock_history import (
    get_batch_size,
    get_stocks_historical_data,
    yield_tickers_batches,
)
from include.utils import render_ddl
from plugins.operators import EnforceLatestFileOperator, ExtractToStagingOperator

# Define the default configuration
dag_args = {
    "start_date": datetime(2025, 9, 9, 1),
    "catchup": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

default_args = {
    **COMMON_ARGS,
    **dag_args,
}


dag = DAG(
    dag_id="extract_index_holdings_ticker_history",
    default_args=default_args,
    schedule="0 12 * * 1-5",  # runs at noon UTC, Monday to Friday only
    tags=["stocks", "data_extraction"],
    params={
        "extract_start_date": (now_tz() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "extract_end_date": now_tz().strftime("%Y-%m-%d"),
    },
)

holdings_table_name = "index_holdings"
sufix_table_name = "yfinance_sufixes"

yaml_path = path.join(PROJECT_ROOT, "include", "raw_files_column_mappings.yaml")
table_name = "stock_historical_data"

with dag:

    @task()
    def start_task():
        return None

    start_task = start_task()

    @task()
    def end_task():
        return None

    end_task = end_task()

    @task
    def seed_yfinance_sufixes():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        # TODO: Replace with dbt seed once dbt is introduced"""

        ddl_sql = f"""
                    CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{sufix_table_name}(
                    exchange VARCHAR(50),
                    yahoo_finance_suffix VARCHAR(20)
                    )
                  """

        hook.run(ddl_sql)

        hook.run(f"TRUNCATE {STAGING_SCHEMA}.{sufix_table_name};")

        seed_path = path.join(PROJECT_ROOT, "include", "seeds", "yfinance_sufixes.csv")

        copy_command = (
            f"COPY  {STAGING_SCHEMA}.{sufix_table_name} FROM STDIN DELIMITER ',' CSV HEADER;"
        )
        hook.copy_expert(sql=copy_command, filename=seed_path)

    seed_yfinance_sufixes = seed_yfinance_sufixes()

    @task()
    def get_index_stock_tickers():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = f"""
                WITH tickers AS (
                    SELECT
                        a.ticker,
                        b.yahoo_finance_suffix
                    FROM {STAGING_SCHEMA}.{holdings_table_name} a
                    LEFT JOIN {STAGING_SCHEMA}.{sufix_table_name} b
                    ON a.exchange = b.exchange
                    WHERE a.asset_class ILIKE 'equity'
                )

                SELECT DISTINCT CONCAT(ticker, COALESCE(yahoo_finance_suffix, ''))
                FROM tickers

              """
        records = hook.get_records(sql)
        # Flatten list of tuples to list of strings
        tickers = [r[0] for r in records if r[0] and r[0] != "-"]
        return tickers

    get_index_stock_tickers = get_index_stock_tickers()

    @task()
    def extract_tickers_history(tickers: list, **kwargs) -> str:
        """Extract stock historical data and upload to S3/MinIO."""
        s3_hook = S3Hook(aws_conn_id="minio_conn")

        conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}
        start_date = conf.get("extract_start_date") or kwargs["params"]["extract_start_date"]
        end_date = conf.get("extract_end_date") or kwargs["params"]["extract_end_date"]

        batch_size = get_batch_size(start_date, end_date)

        data = pd.DataFrame()

        for batch in yield_tickers_batches(tickers, batch_size):
            logging.info(f"Extracting history for the following tickers: {batch}")
            batch_data = get_stocks_historical_data(batch, start_date, end_date)
            if not batch_data.empty:
                data = pd.concat([data, batch_data], ignore_index=True)
            else:
                logging.warning(f"No data returned for batch: {batch}")

            sleep(0.5)

        data["extraction_datetime"] = now_tz()

        logging.info("Data stored and consolidated in a single dataframe.")

        timestamp = now_tz().strftime("%Y%m%d_%H%M%S")
        file_name = f"index_holdings_stock_history/{timestamp}_from_{start_date}_to_{end_date}.csv"

        logging.info(f"Data will be loaded into the bucket: {BUCKET_STOCKS_YAHOO_FINANCE}")
        logging.info(f"Data will be loaded into the file: {file_name}")

        with NamedTemporaryFile(suffix=".csv") as tmp:
            data.to_csv(tmp.name, index=False)

            s3_hook.load_file(
                filename=tmp.name,
                key=file_name,
                bucket_name=BUCKET_STOCKS_YAHOO_FINANCE,
                replace=True,
            )

        return file_name

    extract_tickers_history = extract_tickers_history(get_index_stock_tickers)

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
        object_key="{{ ti.xcom_pull(task_ids='extract_tickers_history') }}",
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

    (
        start_task
        >> seed_yfinance_sufixes
        >> get_index_stock_tickers
        >> extract_tickers_history
        >> create_table
        >> load_to_staging
        >> enforce_lastest_files
        >> end_task
    )
