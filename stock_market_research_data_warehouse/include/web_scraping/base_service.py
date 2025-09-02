import logging
import os

from playwright.sync_api import Page, sync_playwright

from .helpers import get_context_overrides_for_url


class WebScrapingService:
    """
    Shared Playwright browser service (local or remote) usable across scrapers.
    """

    def __init__(self, browser_service_url: str | None = None):
        self.browser_service_url = browser_service_url or os.getenv(
            "BROWSER_SERVICE_URL", "ws://browser-service:9222"
        )
        self._playwright = None
        self._browser = None

    def __enter__(self):
        self._playwright = sync_playwright().start()
        force_local = os.getenv("BROWSER_SERVICE_URL") == "local"
        if force_local:
            logging.getLogger(__name__).debug("Forcing local browser usage (WSL/testing mode)")
            self._browser = self._playwright.chromium.launch(headless=True)
            logging.getLogger(__name__).debug("Started local browser instance")
        else:
            try:
                self._browser = self._playwright.chromium.connect(self.browser_service_url)
                logging.getLogger(__name__).debug(
                    f"Connected to remote browser service: {self.browser_service_url}"
                )
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"Failed to connect to browser service ({e}), falling back to local browser"
                )
                self._browser = self._playwright.chromium.launch(headless=True)
                logging.getLogger(__name__).debug("Started local browser instance")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._browser:
            try:
                if hasattr(self._browser, "close"):
                    self._browser.close()
                self._browser = None
            except Exception as e:
                logging.getLogger(__name__).warning(f"Error during browser cleanup: {e}")
        if self._playwright:
            self._playwright.stop()

    def get_page(self, **kwargs) -> Page:
        if not self._browser:
            raise RuntimeError(
                "WebScrapingService not properly initialized. Use as context manager."
            )

        default_context_options = {
            "viewport": {"width": 1920, "height": 1080},
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "locale": "en-GB",
            "timezone_id": "Europe/London",
            "extra_http_headers": {"Accept-Language": "en-GB,en;q=0.9"},
        }

        context_options = {**default_context_options, **kwargs}
        context = self._browser.new_context(**context_options)
        page = context.new_page()
        page.set_default_timeout(int(os.getenv("ISHARES_PAGE_DEFAULT_TIMEOUT_MS", "120000")))
        return page


def get_page_with_url_overrides(service: WebScrapingService, url: str) -> Page:
    return service.get_page(**get_context_overrides_for_url(url))
