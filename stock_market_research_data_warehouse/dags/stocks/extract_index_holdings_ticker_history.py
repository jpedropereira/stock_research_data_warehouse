from datetime import datetime, timedelta
from os import path

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.common.constants import COMMON_ARGS, POSTGRES_CONN_ID
from include.config import PROJECT_ROOT, now_tz

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
    schedule="0 12 * * *",  # runs daily at noon
    tags=["stocks", "data_extraction"],
    params={
        "extract_start_date": (now_tz() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "extract_end_date": now_tz().strftime("%Y-%m-%d"),
    },
)

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

        sufix_table_name = "staging.yfinance_sufixes"

        ddl_sql = f"""
                    CREATE TABLE IF NOT EXISTS {sufix_table_name}(
                    exchange VARCHAR(50),
                    yahoo_finance_suffix VARCHAR(20)
                    )
                  """

        hook.run(ddl_sql)

        hook.run(f"TRUNCATE {sufix_table_name};")

        seed_path = path.join(PROJECT_ROOT, "include", "seeds", "yfinance_sufixes.csv")

        copy_command = f"COPY {sufix_table_name} FROM STDIN DELIMITER ',' CSV HEADER;"
        hook.copy_expert(sql=copy_command, filename=seed_path)

    seed_yfinance_sufixes = seed_yfinance_sufixes()

    @task()
    def get_index_tickers():
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        sql = """
                WITH tickers AS (
                    SELECT
                        a.ticker,
                        b.yahoo_finance_suffix
                    FROM staging.index_holdings a
                    LEFT JOIN staging.yfinance_sufixes b
                    ON a.exchange = b.exchange
                    WHERE a.asset_class ILIKE 'equity'
                )

                SELECT DISTINCT CONCAT(ticker, COALESCE(yahoo_finance_suffix, ''))
                FROM tickers

              """
        records = hook.get_records(sql)
        return records

    get_index_tickers = get_index_tickers()

    (start_task >> seed_yfinance_sufixes >> get_index_tickers >> end_task)
