from unittest.mock import MagicMock, patch

import pytest
from include.web_scraping.ishares import (
    get_ishares_csv_download_link,
    get_ishares_download_link,
)


def _make_mock_page():
    page = MagicMock()
    page.title.return_value = "Some ETF | iShares"
    page.url = "https://www.ishares.com/uk/products/12345"
    page.context = MagicMock()
    return page


class TestIsharesDownloadLink:
    def test_invalid_url_raises(self):
        with pytest.raises(ValueError):
            get_ishares_download_link("https://example.com/not-ishares")

    @patch("include.web_scraping.ishares.WebScrapingService")
    def test_finds_csv_link_via_selector(self, mock_service):
        # Arrange page
        page = _make_mock_page()
        link_el = MagicMock()
        link_el.get_attribute.return_value = "/uk/prodDownload?fileType=csv&dataType=fund"

        # wait_for_selector returns element immediately
        page.wait_for_selector.return_value = link_el

        # Service context manager -> returns object with get_page
        svc = MagicMock()
        svc.__enter__.return_value = svc
        svc.__exit__.return_value = False
        svc.get_page.return_value = page
        mock_service.return_value = svc

        # Act
        href = get_ishares_download_link(
            "https://www.ishares.com/uk/products/12345", file_type="csv"
        )

        # Assert
        assert href.startswith("https://www.ishares.com/uk/prodDownload")
        page.wait_for_selector.assert_called()  # called with selectors
        # Page and context closed in finally
        page.close.assert_called_once()
        page.context.close.assert_called_once()

    @patch("include.web_scraping.ishares.WebScrapingService")
    def test_relative_href_is_made_absolute(self, mock_service):
        page = _make_mock_page()
        link_el = MagicMock()
        link_el.get_attribute.return_value = "/uk/prodDownload?fileType=csv"
        page.wait_for_selector.return_value = link_el

        svc = MagicMock()
        svc.__enter__.return_value = svc
        svc.__exit__.return_value = False
        svc.get_page.return_value = page
        mock_service.return_value = svc

        href = get_ishares_download_link(
            "https://www.ishares.com/uk/products/12345", file_type=None
        )
        assert href.startswith("https://www.ishares.com/")

    @patch("include.web_scraping.ishares.WebScrapingService")
    def test_no_selector_tries_ajax_then_raises(self, mock_service):
        # Arrange a page without matching selectors and with failing ajax check
        page = _make_mock_page()

        # First, waiting for selector always raises
        def _raise_timeout(*args, **kwargs):
            raise TimeoutError("not found")

        page.wait_for_selector.side_effect = _raise_timeout
        # page.goto for ajax returns object without headers indicating invalid content
        ajax_resp = MagicMock()
        ajax_resp.status = 200
        ajax_resp.headers.return_value = {"content-type": "text/html"}
        page.goto.return_value = ajax_resp

        svc = MagicMock()
        svc.__enter__.return_value = svc
        svc.__exit__.return_value = False
        svc.get_page.return_value = page
        mock_service.return_value = svc

        with pytest.raises(ValueError):
            get_ishares_download_link("https://www.ishares.com/uk/products/12345", file_type="csv")

        # Ensure cleanup called
        page.close.assert_called_once()

    @patch("include.web_scraping.ishares.WebScrapingService")
    def test_get_ishares_csv_download_link_fallback(self, mock_service):
        # First attempt raises, fallback returns value
        page1 = _make_mock_page()
        page2 = _make_mock_page()
        el2 = MagicMock()
        el2.get_attribute.return_value = "/uk/prodDownload?fileType=csv"
        page2.wait_for_selector.return_value = el2

        svc = MagicMock()
        svc.__enter__.side_effect = [svc, svc]
        svc.__exit__.return_value = False
        # Two sequential get_page returns
        svc.get_page.side_effect = [page1, page2]
        mock_service.return_value = svc

        # Make first call raise: simulate by making page1 wait_for_selector raise
        page1.wait_for_selector.side_effect = TimeoutError("not found")

        href = get_ishares_csv_download_link("https://www.ishares.com/uk/products/12345")
        assert href.startswith("https://www.ishares.com/")
