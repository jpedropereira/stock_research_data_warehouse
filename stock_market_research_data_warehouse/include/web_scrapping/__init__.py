"""Web scraping package: shared browser service and site-specific scrapers."""

from .base_service import WebScrapingService
from .ishares import get_ishares_csv_download_link, get_ishares_download_link

__all__ = [
    "WebScrapingService",
    "get_ishares_download_link",
    "get_ishares_csv_download_link",
]
