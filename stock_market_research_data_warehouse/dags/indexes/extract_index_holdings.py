from datetime import datetime

from airflow.decorators import dag


@dag(
    start_date=datetime(2025, 8, 9),
    schedule="@weekly",
    catchup=False,
    tags=["stocks", "data_extraction", "index", "etf"],
)
def extract_index_holdings():
    pass
