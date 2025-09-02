"""
Central configuration module for environment variables and settings.
This module loads environment variables once and provides them to other modules.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

# Load environment variables once at module level
load_dotenv()

# Environment Configuration
ENV = os.getenv("ENV")
if ENV is None:
    raise ValueError(
        "ENV environment variable is required but not set. "
        "Please set ENV=DEV or ENV=PRD in your .env file."
    )

if ENV not in ["DEV", "PRD"]:
    raise ValueError(f"ENV must be either 'DEV' or 'PRD', got '{ENV}'")

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

OWNER = os.getenv("ENV", "Unknown")

# Database Configuration
DATABASE_NAME = f"db_sr_{ENV}"
STAGING_SCHEMA = "staging"


# Timezone Configuration
PIPELINE_TZ = os.getenv("DATA_PIPELINE_TZ", "UTC")


def now_tz():
    """Return current datetime in pipeline timezone."""
    return datetime.now(ZoneInfo(PIPELINE_TZ))


# iShares (web scraping) timeouts (milliseconds)
ISHARES_PAGE_DEFAULT_TIMEOUT_MS = int(os.getenv("ISHARES_PAGE_DEFAULT_TIMEOUT_MS", "120000"))
ISHARES_NAV_TIMEOUT_MS = int(os.getenv("ISHARES_NAV_TIMEOUT_MS", "120000"))
ISHARES_NAV_FALLBACK_TIMEOUT_MS = int(os.getenv("ISHARES_NAV_FALLBACK_TIMEOUT_MS", "90000"))
ISHARES_EXTRA_JS_WAIT_MS = int(os.getenv("ISHARES_EXTRA_JS_WAIT_MS", "5000"))
ISHARES_SELECTOR_TIMEOUT_MS = int(os.getenv("ISHARES_SELECTOR_TIMEOUT_MS", "30000"))
ISHARES_QUICK_WAIT_MS = int(os.getenv("ISHARES_QUICK_WAIT_MS", "2500"))


__all__ = [
    "ENV",
    "OWNER",
    "PROJECT_ROOT",
    "DATABASE_NAME",
    "STAGING_SCHEMA",
    "PIPELINE_TZ",
    "now_tz",
    # iShares timeouts
    "ISHARES_PAGE_DEFAULT_TIMEOUT_MS",
    "ISHARES_NAV_TIMEOUT_MS",
    "ISHARES_NAV_FALLBACK_TIMEOUT_MS",
    "ISHARES_EXTRA_JS_WAIT_MS",
    "ISHARES_SELECTOR_TIMEOUT_MS",
    "ISHARES_QUICK_WAIT_MS",
]
