from unittest.mock import patch

import pytest
from include.indexes.index_holdings import get_ishares_etf_holdings_csv_url


class TestGetIsharesEtfHoldingsCsvUrl:
    etf_url = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf"
    csv_relative = "/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"
    csv_full = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund"

    html_with_link = f'<html><body><a class="icon-xls-export" href="{csv_relative}">Detailed Holdings and Analytics</a></body></html>'
    html_without_link = "<html><body>No CSV here</body></html>"

    @patch("include.indexes.index_holdings.get_ishares_csv_download_link")
    def test_get_ishares_etf_holdings_csv_url_success(self, mock_scraper):
        # Simulate successful scraper returning the final CSV URL
        mock_scraper.return_value = self.csv_full
        result = get_ishares_etf_holdings_csv_url(self.etf_url)
        assert result == self.csv_full

    @patch("include.indexes.index_holdings.get_ishares_csv_download_link")
    def test_get_ishares_etf_holdings_csv_url_not_found(self, mock_scraper):
        # Simulate scraper failing to find CSV and raising the expected error
        mock_scraper.side_effect = ValueError("URL for holdings csv was not found")
        with pytest.raises(ValueError, match="URL for holdings csv was not found"):
            get_ishares_etf_holdings_csv_url(self.etf_url)
