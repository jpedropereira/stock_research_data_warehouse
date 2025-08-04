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

# Database Configuration
DATABASE_NAME = f"db_sr_{ENV}"
STAGING_SCHEMA = "staging"


# Timezone Configuration
PIPELINE_TZ = os.getenv("DATA_PIPELINE_TZ", "UTC")


def now_tz():
    """Return current datetime in pipeline timezone."""
    return datetime.now(ZoneInfo(PIPELINE_TZ))


__all__ = ["ENV", "DATABASE_NAME", "STAGING_SCHEMA", "PIPELINE_TZ", "now_tz"]
