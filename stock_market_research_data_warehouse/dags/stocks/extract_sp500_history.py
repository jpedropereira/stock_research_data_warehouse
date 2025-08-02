from datetime import datetime
from datetime import timedelta
from tempfile import NamedTemporaryFile

from airflow.decorators import dag
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from include.stocks.constants import BUCKET_STOCKS_YAHOO_FINANCE
from include.stocks.constants import INDEXES
from include.stocks.index_history import get_index_symbols_from_wikipedia
from include.stocks.index_history import get_stocks_historical_data

s3_hook = S3Hook(aws_conn_id="minio_conn")


@dag(
    start_date=datetime(2025, 7, 28),
    schedule="@daily",
    catchup=False,
    tags=["sp_500", "stocks", "data_extraction"],
    params={
        "extract_start_date": (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        "extract_end_date": datetime.now().strftime("%Y-%m-%d"),
    },
)
def extract_sp500():

    @task()
    def get_ticker_symbols():
        url = INDEXES["SP500"]["wikipage_url"]
        return get_index_symbols_from_wikipedia(url)

    @task()
    def get_stocks_data(symbols, **kwargs):
        conf = kwargs.get("dag_run").conf if kwargs.get("dag_run") else {}
        start_date = (
            conf.get("extract_start_date") or kwargs["params"]["extract_start_date"]
        )
        end_date = conf.get("extract_end_date") or kwargs["params"]["extract_end_date"]
        data = get_stocks_historical_data(symbols, start_date, end_date)
        data["extraction_datetime"] = datetime.now()

        with NamedTemporaryFile(suffix=".csv") as tmp:
            data.to_csv(tmp.name, index=False)
            file_path = tmp.name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            object_key = f"sp500_data/{timestamp}_from_{start_date}_to_{end_date}.csv"

            s3_hook.load_file(
                filename=file_path,
                key=object_key,
                bucket_name=BUCKET_STOCKS_YAHOO_FINANCE,
                replace=True,
            )
            return object_key

    get_ticker_symbols = get_ticker_symbols()
    get_stocks_data = get_stocks_data(get_ticker_symbols)

    get_ticker_symbols >> get_stocks_data


extract_sp500()
