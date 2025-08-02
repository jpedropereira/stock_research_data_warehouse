from unittest.mock import patch

import pandas as pd
import pytest
from include.stocks.index_history import get_index_symbols_from_wikipedia


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
            with pytest.raises(
                ValueError, match="No ticker symbols found in the table."
            ):
                get_index_symbols_from_wikipedia(sample_url)
