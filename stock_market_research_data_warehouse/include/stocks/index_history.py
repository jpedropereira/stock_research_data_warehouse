import pandas as pd


def get_index_symbols_from_wikipedia(url: str) -> list[str]:
    """
    Scrapes the S&P 500 ticker symbols from the first table
    on the given Wikipedia page.
    """
    tables = pd.read_html(url)
    table = tables[0]
    if "Symbol" not in table.columns:
        raise ValueError("Column 'Symbol' not found in the first table.")
    tickers = table["Symbol"].tolist()
    if not tickers:
        raise ValueError("No ticker symbols found in the table.")
    return tickers
