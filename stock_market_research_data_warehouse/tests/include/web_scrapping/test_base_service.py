import os
from unittest.mock import MagicMock, patch

import pytest
from include.web_scrapping.base_service import (
    WebScrapingService,
    get_page_with_url_overrides,
)


class TestWebScrapingService:
    @patch("include.web_scrapping.base_service.sync_playwright")
    def test_context_manager_connects_remote(self, mock_sync_pl):
        # Arrange: default env (no BROWSER_SERVICE_URL=local)
        os.environ.pop("BROWSER_SERVICE_URL", None)

        mock_pl = MagicMock()
        mock_browser = MagicMock()
        mock_pl.chromium.connect.return_value = mock_browser
        mock_sync_pl.return_value.start.return_value = mock_pl

        # Act
        with WebScrapingService(browser_service_url="ws://test:9222") as svc:
            # Assert
            assert svc._browser is mock_browser
            mock_pl.chromium.connect.assert_called_once_with("ws://test:9222")

        # After exit, ensure playwright.stop was called
        mock_sync_pl.return_value.start.return_value.stop.assert_called_once()

    @patch("include.web_scrapping.base_service.sync_playwright")
    def test_context_manager_falls_back_to_local(self, mock_sync_pl):
        # Arrange: remote connect raises, should fall back to launch(headless=True)
        os.environ.pop("BROWSER_SERVICE_URL", None)

        mock_pl = MagicMock()
        mock_pl.chromium.connect.side_effect = RuntimeError("conn error")
        mock_local_browser = MagicMock()
        mock_pl.chromium.launch.return_value = mock_local_browser
        mock_sync_pl.return_value.start.return_value = mock_pl

        # Act
        with WebScrapingService(browser_service_url="ws://bad:9222") as svc:
            assert svc._browser is mock_local_browser
            mock_pl.chromium.launch.assert_called_once_with(headless=True)

    @patch("include.web_scrapping.base_service.sync_playwright")
    def test_force_local_via_env(self, mock_sync_pl, monkeypatch):
        # Arrange: BROWSER_SERVICE_URL=local forces launch
        monkeypatch.setenv("BROWSER_SERVICE_URL", "local")
        mock_pl = MagicMock()
        mock_local_browser = MagicMock()
        mock_pl.chromium.launch.return_value = mock_local_browser
        mock_sync_pl.return_value.start.return_value = mock_pl

        # Act
        with WebScrapingService() as svc:
            assert svc._browser is mock_local_browser
            mock_pl.chromium.launch.assert_called_once_with(headless=True)

    @patch("include.web_scrapping.base_service.sync_playwright")
    def test_get_page_default_and_overrides(self, mock_sync_pl):
        # Arrange
        mock_pl = MagicMock()
        mock_browser = MagicMock()
        mock_context = MagicMock()
        mock_page = MagicMock()
        mock_browser.new_context.return_value = mock_context
        mock_context.new_page.return_value = mock_page
        mock_pl.chromium.connect.return_value = mock_browser
        mock_sync_pl.return_value.start.return_value = mock_pl

        url = "https://example.com/uk/products/12345"
        with WebScrapingService("ws://test:9222") as svc:
            page = get_page_with_url_overrides(svc, url)
            assert page is mock_page
            # Ensure the UK overrides were applied in context options
            args, kwargs = mock_browser.new_context.call_args
            assert kwargs["locale"] == "en-GB"
            assert kwargs["timezone_id"] == "Europe/London"
            assert "viewport" in kwargs  # default options preserved
            mock_page.set_default_timeout.assert_called()  # default timeout set

    @patch("include.web_scrapping.base_service.sync_playwright")
    def test_get_page_raises_if_not_initialized(self, mock_sync_pl):
        svc = WebScrapingService("ws://x")
        with pytest.raises(
            RuntimeError,
            match=r"WebScrapingService not properly initialized\. Use as context manager\.",
        ):
            svc.get_page()
