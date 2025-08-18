import logging
from datetime import datetime, timedelta
from os import path
from tempfile import NamedTemporaryFile

import yaml
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from include.config import PROJECT_ROOT, STAGING_SCHEMA, now_tz
from include.indexes.constants import BUCKET_INDEXES_ISHARES_HOLDINGS, COMMON_ARGS
from include.indexes.index_holdings import (
    get_ishares_etf_holdings,
    get_ishares_etf_holdings_csv_url,
)
from include.utils import render_ddl
from plugins.operators import EnforceLatestFileOperator, ExtractToStagingOperator

tables_yaml_path = path.join(PROJECT_ROOT, "include", "raw_files_column_mappings.yaml")
indexes_yaml_path = path.join(PROJECT_ROOT, "include", "indexes", "indexes.yaml")
table_name = "index_holdings"

with open(indexes_yaml_path) as f:
    indexes_raw = yaml.safe_load(f)["ISHARES"]


# Define the default configuration
dag_args = {
    "start_date": datetime(2025, 8, 18, 1),
    "catchup": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
}

default_args = {
    **COMMON_ARGS,
    **dag_args,
}


def create_extract_index_holdings_dag(index, dag_id, default_args, schedule):
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=schedule,
        tags=["stocks", "data_extraction", "indexes", "etf", f"{index}"],
    )

    index_configs = indexes_raw.get(index)

    with dag:

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
            return index_configs.get("ishares_etf_url")

        get_ishares_indexes_task = get_ishares_indexes_task()

        @task()
        def get_ishares_holdings_csv_url(etf_url: str) -> dict:
            logging.info(f"Finding holdings csv url for {index} index.")
            url = get_ishares_etf_holdings_csv_url(etf_url)
            logging.info(f"Holdings csv url for {index} index is {url}")
            return url

        get_ishares_holdings_csv_urls = get_ishares_holdings_csv_url(get_ishares_indexes_task)

        @task()
        def get_ishares_holdings_data(csv_url: str):
            logging.info(f"Reading holdings url for {index} index from {csv_url}")

            s3_hook = S3Hook(aws_conn_id="minio_conn")
            data = get_ishares_etf_holdings(csv_url)
            data["extraction_datetime"] = now_tz()

            timestamp = now_tz().strftime("%Y%m%d_%H%M%S")
            file_name = f"{timestamp}_{index}_holdings.csv"

            logging.info(
                f"Writting {index} holdings in bucket {BUCKET_INDEXES_ISHARES_HOLDINGS} {file_name}"
            )

            with NamedTemporaryFile(suffix=".csv") as tmp:
                data.to_csv(tmp.name, index=False)

                s3_hook.load_file(
                    filename=tmp.name,
                    key=file_name,
                    bucket_name=BUCKET_INDEXES_ISHARES_HOLDINGS,
                    replace=True,
                )

                return file_name

        get_ishares_holdings_data = get_ishares_holdings_data(get_ishares_holdings_csv_urls)

        create_table = SQLExecuteQueryOperator(
            task_id="create_table_if_not_exists",
            conn_id="datawarehouse_conn",
            sql=render_ddl(
                column_mapping_yaml_path=tables_yaml_path,
                table_name=table_name,
                schema_name=STAGING_SCHEMA,
            ),
        )

        load_to_staging = ExtractToStagingOperator(
            task_id="load_to_staging",
            s3_conn_id="minio_conn",
            bucket_name=BUCKET_INDEXES_ISHARES_HOLDINGS,
            postgres_conn_id="datawarehouse_conn",
            schema_name=STAGING_SCHEMA,
            table_name=table_name,
            column_mapping_yaml_path=tables_yaml_path,
            object_key="{{ ti.xcom_pull(task_ids='get_ishares_holdings_data') }}",
            add_label_columns={"index": index},
        )

        enforce_lates_file = EnforceLatestFileOperator(
            task_id="enforce_lastest_files",
            postgres_conn_id="datawarehouse_conn",
            deduplication_columns=["index", "ticker"],
            schema_name=STAGING_SCHEMA,
            table_name=table_name,
        )

        (
            start_task
            >> get_ishares_indexes_task
            >> get_ishares_holdings_csv_urls
            >> get_ishares_holdings_data
            >> create_table
            >> load_to_staging
            >> enforce_lates_file
            >> end_task
        )

        return dag


for index in indexes_raw.keys():

    dag_id = f"extract_{index}_holdings"
    schedule = indexes_raw[index].get("extraction_schedule", "@weekly")

    # register dag in global namespace
    globals()[dag_id] = create_extract_index_holdings_dag(
        index=index,
        dag_id=dag_id,
        default_args=default_args,
        schedule=schedule,
    )
