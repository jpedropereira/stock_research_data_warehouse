from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest
from include.stocks.stock_history import (
    get_batch_size,
    get_index_symbols_from_wikipedia,
    get_stocks_historical_data,
    yield_tickers_batches,
)


class TestGetIndexSymbolsFromWikipedia:
    @pytest.fixture
    def sample_url(self):
        return "https://test-wiki.com/test-sp500"

    @pytest.fixture
    def valid_dataframe(self):
        """Valid DataFrame with Symbol column."""
        return pd.DataFrame(
            {
                "Symbol": ["AAA", "BBB", "CCC", "DDD"],
                "Company": ["CompanyA", "CompanyB", "CompanyC", "CompanyD"],
                "Sector": ["SectorX", "SectorY", "SectorX", "SectorZ"],
            }
        )

    def test_success_case(self, sample_url, valid_dataframe):
        with patch("pandas.read_html") as mock_read_html:
            mock_read_html.return_value = [valid_dataframe]
            result = get_index_symbols_from_wikipedia(sample_url)
            assert result == ["AAA", "BBB", "CCC", "DDD"]
            mock_read_html.assert_called_once_with(sample_url)

    @pytest.fixture
    def invalid_dataframe(self):
        """Invalid DataFrame missing 'Symbol' column"""
        return pd.DataFrame(
            {
                "Ticker": ["XXX", "YYY"],  # Wrong column name
                "Company": ["CompanyX", "CompanyY"],
            }
        )

    def test_missing_symbol_column(self, sample_url, invalid_dataframe):
        with patch("pandas.read_html") as mock_read_html:
            mock_read_html.return_value = [invalid_dataframe]
            with pytest.raises(ValueError, match="Column 'Symbol' not found"):
                get_index_symbols_from_wikipedia(sample_url)

    @pytest.fixture
    def empty_dataframe(self):
        """Empty DataFrame with Symbol column only."""
        return pd.DataFrame(columns=["Symbol"])

    def test_empty_dataframe(self, sample_url, empty_dataframe):
        with patch("pandas.read_html") as mock_read_html:
            mock_read_html.return_value = [empty_dataframe]
            with pytest.raises(ValueError, match="No ticker symbols found in the table."):
                get_index_symbols_from_wikipedia(sample_url)


class TestGetBatchSize:
    def test_one_day(self):
        assert get_batch_size(start_date="2025-09-11", end_date="2025-09-12") == 50

    def test_four_months(self):
        assert get_batch_size(start_date="2025-05-12", end_date="2025-09-12") == 20

    def test_15_months(self):
        assert get_batch_size(start_date="2024-06-12", end_date="2025-09-12") == 10

    def test_6_years(self):
        assert get_batch_size(start_date="2019-09-12", end_date="2025-09-12") == 5


class TestYeldTickersBatches:
    def test_yield_tickers_batches_exact_multiple(self):
        tickers = [f"TICKER{i}" for i in range(100)]
        batches = list(yield_tickers_batches(tickers, batch_size=20))
        assert len(batches) == 5
        for batch in batches:
            assert len(batch) == 20

    def test_yield_tickers_batches_non_multiple(self):
        tickers = [f"TICKER{i}" for i in range(53)]
        batches = list(yield_tickers_batches(tickers, batch_size=20))
        assert len(batches) == 3
        assert len(batches[0]) == 20
        assert len(batches[1]) == 20
        assert len(batches[2]) == 13

    def test_yield_tickers_batches_batch_size_greater_than_list(self):
        tickers = [f"TICKER{i}" for i in range(10)]
        batches = list(yield_tickers_batches(tickers, batch_size=20))
        assert len(batches) == 1
        assert batches[0] == tickers

    def test_yield_tickers_batches_empty_list(self):
        tickers = []
        batches = list(yield_tickers_batches(tickers, batch_size=10))
        assert batches == []


class TestGetStocksHistoricalData:
    @pytest.fixture
    def sample_symbols(self):
        return ["TCKR", "SBL"]

    @pytest.fixture
    def mock_yfinance_multiindex_data(self):
        """Mock MultiIndex DataFrame as returned by yfinance for multiple symbols"""
        dates = pd.date_range("2025-01-01", "2025-01-03", freq="D")
        symbols = ["TCKR", "SBL"]

        # Create MultiIndex columns like yfinance returns
        columns = pd.MultiIndex.from_product(
            [["Open", "High", "Low", "Close", "Volume"], symbols],
            names=["Price", "Ticker"],
        )

        data = pd.DataFrame(
            {
                ("Open", "TCKR"): [150.0, 151.0, 152.0],
                ("High", "TCKR"): [155.0, 156.0, 157.0],
                ("Low", "TCKR"): [149.0, 150.0, 151.0],
                ("Close", "TCKR"): [154.0, 155.0, 156.0],
                ("Volume", "TCKR"): [1000000, 1100000, 1200000],
                ("Open", "SBL"): [300.0, 301.0, 302.0],
                ("High", "SBL"): [305.0, 306.0, 307.0],
                ("Low", "SBL"): [299.0, 300.0, 301.0],
                ("Close", "SBL"): [304.0, 305.0, 306.0],
                ("Volume", "SBL"): [2000000, 2100000, 2200000],
            },
            index=dates,
            columns=columns,
        )
        data.index.name = "Date"
        return data

    def test_success_case_multiindex(self, sample_symbols, mock_yfinance_multiindex_data):
        with patch("yfinance.download") as mock_download:
            mock_download.return_value = mock_yfinance_multiindex_data

            result = get_stocks_historical_data(
                symbols=sample_symbols, start_date="2025-01-01", end_date="2025-01-03"
            )

            # Verify the function was called correctly
            mock_download.assert_called_once_with(
                sample_symbols, start="2025-01-01", end="2025-01-03"
            )

            # Verify DataFrame structure
            assert isinstance(result, pd.DataFrame)
            assert len(result) == 6  # 3 dates × 2 symbols

            # Verify columns
            expected_columns = [
                "date",
                "Ticker",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
            assert list(result.columns) == expected_columns

            # Verify data content
            assert set(result["Ticker"].unique()) == {"TCKR", "SBL"}
            assert len(result[result["Ticker"] == "TCKR"]) == 3
            assert len(result[result["Ticker"] == "SBL"]) == 3

            # Verify date column is properly formatted
            assert pd.api.types.is_datetime64_any_dtype(result["date"])

            # Verify some data values
            tckr_data = result[result["Ticker"] == "TCKR"].iloc[0]
            assert tckr_data["Open"] == 150.0
            assert tckr_data["Close"] == 154.0
            assert tckr_data["Volume"] == 1000000

    @pytest.fixture
    def mock_yfinance_single_symbol_data(self):
        """Mock single symbol DataFrame"""
        dates = pd.date_range("2025-01-01", "2025-01-02", freq="D")

        columns = pd.MultiIndex.from_product(
            [["Open", "High", "Low", "Close", "Volume"], ["TCKR"]],
            names=["Price", "Ticker"],
        )

        data = pd.DataFrame(
            {
                ("Open", "TCKR"): [150.0, 151.0],
                ("High", "TCKR"): [155.0, 156.0],
                ("Low", "TCKR"): [149.0, 150.0],
                ("Close", "TCKR"): [154.0, 155.0],
                ("Volume", "TCKR"): [1000000, 1100000],
            },
            index=dates,
            columns=columns,
        )
        data.index.name = "Date"
        return data

    def test_single_symbol(self, mock_yfinance_single_symbol_data):
        with patch("yfinance.download") as mock_download:
            mock_download.return_value = mock_yfinance_single_symbol_data

            result = get_stocks_historical_data(
                symbols=["TCKR"], start_date="2025-01-01", end_date="2025-01-02"
            )

            # Verify DataFrame structure
            assert len(result) == 2  # 2 dates × 1 symbol
            assert set(result["Ticker"].unique()) == {"TCKR"}

            # Verify columns are in correct order
            expected_columns = [
                "date",
                "Ticker",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
            assert list(result.columns) == expected_columns

    def test_empty_symbols_list(self):
        # Test the new validation for empty symbols list
        with pytest.raises(ValueError, match="No symbols provided for historical data retrieval."):
            get_stocks_historical_data(symbols=[], start_date="2025-01-01", end_date="2025-01-02")

    def test_start_date_after_end_date(self):
        with pytest.raises(ValueError, match="Start date must be before end date."):
            get_stocks_historical_data(
                symbols=["TCKR"], start_date="2025-01-03", end_date="2025-01-01"
            )

    def test_future_start_date(self):
        future_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
        future_end_date = (datetime.now() + timedelta(days=366)).strftime("%Y-%m-%d")
        with pytest.raises(ValueError, match="Start date cannot be in the future."):
            get_stocks_historical_data(
                symbols=["TCKR"], start_date=future_date, end_date=future_end_date
            )

    def test_date_parameters_passed_correctly(self, sample_symbols, mock_yfinance_multiindex_data):
        with patch("yfinance.download") as mock_download:
            mock_download.return_value = mock_yfinance_multiindex_data

            start_date = "2024-12-01"
            end_date = "2024-12-31"

            get_stocks_historical_data(
                symbols=sample_symbols, start_date=start_date, end_date=end_date
            )

            # Verify dates are passed correctly to yfinance
            mock_download.assert_called_once_with(sample_symbols, start=start_date, end=end_date)

    def test_empty_dataframe_from_yfinance(self):
        with patch("yfinance.download") as mock_download:
            # Simulate yfinance returning empty DataFrame (e.g., invalid symbols or date range)
            mock_download.return_value = pd.DataFrame()

            with pytest.raises(
                ValueError,
                match="No historical data found for the given symbols and date range "
                "2025-01-01 to 2025-01-02.",
            ):
                get_stocks_historical_data(
                    symbols=["INVALID"], start_date="2025-01-01", end_date="2025-01-02"
                )
