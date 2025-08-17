import logging
from datetime import datetime
from os import path
from tempfile import NamedTemporaryFile

import yaml
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.config import PROJECT_ROOT, STAGING_SCHEMA, now_tz
from include.indexes.constants import BUCKET_INDEXES_ISHARES_HOLDINGS
from include.indexes.index_holdings import (
    get_ishares_etf_holdings,
    get_ishares_etf_holdings_csv_url,
)
from include.utils import render_ddl
from plugins.operators import ExtractToStagingOperator

tables_yaml_path = path.join(PROJECT_ROOT, "include", "raw_files_column_mappings.yaml")
indexes_yaml_path = path.join(PROJECT_ROOT, "include", "indexes", "indexes.yaml")
table_name = "index_holdings"


@dag(
    start_date=datetime(2025, 8, 9),
    schedule="@weekly",
    catchup=False,
    tags=["stocks", "data_extraction", "index", "etf"],
)
def extract_index_holdings():
    @task()
    def start_task():
        return None

    start_task = start_task()

    @task()
    def end_task():
        return None

    end_task = end_task()

    @task()
    def get_ishares_indexes_task():
        with open(indexes_yaml_path) as f:
            indexes_raw = yaml.safe_load(f)["ISHARES"]

        if len(indexes_raw) == 0:
            raise ValueError(f"{indexes_yaml_path} file is empty")

        return [
            {"index_name": index, "etf_url": data.get("ishares_etf_url")}
            for index, data in indexes_raw.items()
            if "ishares_etf_url" in data
        ]

    get_ishares_indexes_task = get_ishares_indexes_task()

    @task()
    def get_ishares_holdings_csv_url(index_name: str, etf_url: str) -> dict:
        logging.info(f"Finding holdings csv url for {index_name} index.")
        url = get_ishares_etf_holdings_csv_url(etf_url)
        logging.info(f"Holdings csv url for {index_name} index is {url}")
        return {"index_name": index_name, "csv_url": url}

    @task()
    def get_ishares_holdings_data(index_name: str, csv_url: str):
        logging.info(f"Reading holdings url for {index_name} index from {csv_url}")

        s3_hook = S3Hook(aws_conn_id="minio_conn")
        data = get_ishares_etf_holdings(csv_url)
        data["extraction_datetime"] = now_tz()

        timestamp = now_tz().strftime("%Y%m%d_%H%M%S")
        file_name = f"{timestamp}_{index_name}_holdings.csv"

        logging.info(
            f"Writting {index_name} holdings in bucket {BUCKET_INDEXES_ISHARES_HOLDINGS} {file_name}"
        )

        with NamedTemporaryFile(suffix=".csv") as tmp:
            data.to_csv(tmp.name, index=False)

            s3_hook.load_file(
                filename=tmp.name,
                key=file_name,
                bucket_name=BUCKET_INDEXES_ISHARES_HOLDINGS,
                replace=True,
            )
            # Return file_name as object_key after upload
            return {"object_key": file_name}

    get_ishares_holdings_csv_urls = get_ishares_holdings_csv_url.expand_kwargs(
        get_ishares_indexes_task
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table_if_not_exists",
        conn_id="datawarehouse_conn",
        sql=render_ddl(
            column_mapping_yaml_path=tables_yaml_path,
            table_name=table_name,
            schema_name=STAGING_SCHEMA,
        ),
    )

    get_ishares_holdings_data_sets = get_ishares_holdings_data.expand_kwargs(
        get_ishares_holdings_csv_urls
    )

    load_to_staging = ExtractToStagingOperator.partial(
        task_id="load_to_staging",
        s3_conn_id="minio_conn",
        bucket_name=BUCKET_INDEXES_ISHARES_HOLDINGS,
        postgres_conn_id="datawarehouse_conn",
        schema_name=STAGING_SCHEMA,
        table_name=table_name,
        column_mapping_yaml_path=tables_yaml_path,
    ).expand_kwargs(get_ishares_holdings_data_sets)

    (
        start_task
        >> get_ishares_indexes_task
        >> get_ishares_holdings_csv_urls
        >> get_ishares_holdings_data_sets
        >> create_table
        >> load_to_staging
        >> end_task
    )


extract_index_holdings()
