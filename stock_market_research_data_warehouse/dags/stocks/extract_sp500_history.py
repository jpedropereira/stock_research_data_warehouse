from datetime import datetime

from airflow.decorators import dag
from airflow.decorators import task
from include.stocks.constants import INDEXES
from include.stocks.index_history import get_index_symbols_from_wikipedia


@dag(
    start_date=datetime(2025, 7, 28),
    schedule="@daily",
    catchup=False,
    tags=["sp_500", "stocks", "data_extraction"],
)
def extract_sp500():

    @task()
    def get_ticker_symbols():
        url = INDEXES["SP500"]["wikipage_url"]
        return get_index_symbols_from_wikipedia(url)

    @task
    def get_symbol_data():
        pass

    get_ticker_symbols() >> get_symbol_data()


extract_sp500()
