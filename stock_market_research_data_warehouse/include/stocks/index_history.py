import datetime

import pandas as pd
import yfinance as yf


def get_index_symbols_from_wikipedia(url: str) -> list[str]:
    """
    Scrapes the S&P 500 ticker symbols from the first table
    on the given Wikipedia page.
    """
    tables = pd.read_html(url)
    table = tables[0]
    if "Symbol" not in table.columns:
        raise ValueError("Column 'Symbol' not found in the first table.")
    symbols = [symbol.replace(".", "-") for symbol in table["Symbol"].tolist()]
    if not symbols:
        raise ValueError("No ticker symbols found in the table.")
    return symbols


def get_stocks_historical_data(
    symbols: list[str],
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """
    Fetches historical stock data for the given symbols from Yahoo Finance.
    """
    if not symbols:
        raise ValueError("No symbols provided for historical data retrieval.")
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")
    if start_date > datetime.datetime.now().strftime("%Y-%m-%d"):
        raise ValueError("Start date cannot be in the future.")

    data = yf.download(symbols, start=start_date, end=end_date)

    if data.empty:
        raise ValueError(
            f"No historical data found for the given symbols and date range "
            f"{start_date} to {end_date}."
        )

    data = data.stack(level=1, future_stack=True).reset_index()
    data = data.rename(
        columns={
            "level_1": "Ticker",
            "Date": "date",
        }
    )
    metric_cols = [col for col in data.columns if col not in ["date", "Ticker"]]
    data = data[["date", "Ticker"] + metric_cols]

    return data
