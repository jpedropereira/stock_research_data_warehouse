import datetime
import logging
import random
import time

import pandas as pd
import yfinance as yf
from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout


def get_batch_size(start_date: str, end_date: str) -> int:
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    days = (end - start).days
    if days <= 90:
        return 50
    elif days <= 365:
        return 20
    elif days <= 5 * 365:
        return 10
    else:
        return 5


def yield_tickers_batches(tickers: list, batch_size: int = 50):
    for i in range(0, len(tickers), batch_size):
        yield tickers[i : i + batch_size]


def get_stocks_historical_data(
    symbols: list[str],
    start_date: str,
    end_date: str,
    *,
    max_retries: int = 3,
    backoff_base_seconds: float = 1.0,
) -> pd.DataFrame:
    """
    Fetch historical stock data for the given symbols from Yahoo Finance with retry logic.

    Parameters
    - symbols: List of ticker symbols (with exchange suffix when needed, e.g., "SHOP.TO").
    - start_date: Inclusive start date in format YYYY-MM-DD.
    - end_date: Exclusive end date in format YYYY-MM-DD (yfinance semantics).
    - max_retries: Maximum number of retry attempts on transient network errors. Default 3.
    - backoff_base_seconds: Initial backoff seconds; doubled each attempt with a small jitter. Default 1.0s.

    Behavior
    - Retries on timeouts, connection errors, and common transient signals (429/rate limiting, resets).
    - Does NOT retry when yfinance returns an empty DataFrame (interpreted as no data for symbols/range).
    - Raises ValueError when no data is available after execution (matches prior behavior), chaining the
      original exception when retries exhausted due to transient failures.
    """
    if not symbols:
        raise ValueError("No symbols provided for historical data retrieval.")
    if start_date > end_date:
        raise ValueError("Start date must be before end date.")
    if start_date > datetime.datetime.now().strftime("%Y-%m-%d"):
        raise ValueError("Start date cannot be in the future.")

    # Retry only on transient network errors (timeouts, connection resets, 5xx/429 surfaced as exceptions).
    attempt = 0
    last_exception: Exception | None = None
    while attempt < max_retries:
        attempt += 1
        try:
            data = yf.download(
                symbols,
                start=start_date,
                end=end_date,
                progress=False,
                threads=True,
            )
            # Keep original behavior: if yfinance returns an empty DataFrame, do not retry here.
            break
        except (RequestsTimeout, RequestsConnectionError) as e:
            last_exception = e
            sleep_seconds = backoff_base_seconds * (2 ** (attempt - 1))
            # Small jitter to avoid thundering herd
            sleep_seconds += random.uniform(0, 0.25)
            logging.warning(
                f"[yfinance] Transient network error on attempt {attempt}/{max_retries} for {symbols}: {e}. "
                f"Retrying in {sleep_seconds:.2f}s."
            )
            time.sleep(sleep_seconds)
        except Exception as e:  # Broad catch: yfinance can raise other transient errors
            last_exception = e
            # Heuristically retry on common transient error messages
            msg = str(e).lower()
            transient_markers = [
                "timed out",
                "timeout",
                "connection aborted",
                "connection reset",
                "remote disconnected",
                "temporarily unavailable",
                "try again",
                "429",
                "too many requests",
                "rate limit",
            ]
            if any(m in msg for m in transient_markers) and attempt < max_retries:
                sleep_seconds = backoff_base_seconds * (2 ** (attempt - 1)) + random.uniform(
                    0, 0.25
                )
                logging.warning(
                    f"[yfinance] Possible transient error on attempt {attempt}/{max_retries} for {symbols}: {e}. "
                    f"Retrying in {sleep_seconds:.2f}s."
                )
                time.sleep(sleep_seconds)
                continue
            # Non-transient or last attempt -> re-raise
            if attempt >= max_retries:
                break
            else:
                # Treat as non-transient and stop retrying
                break

    if "data" not in locals():
        # All attempts failed due to exceptions
        raise ValueError(
            f"No historical data found for the given symbols and date range {start_date} to {end_date}."
        ) from last_exception

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
