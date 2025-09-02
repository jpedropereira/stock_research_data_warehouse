from io import StringIO

import pandas as pd
import requests
from include.web_scraping import get_ishares_csv_download_link

BASE_ISHARES_URL = "https://www.ishares.com"


def get_ishares_etf_holdings_csv_url(etf_url: str) -> str:
    """
    Finds and returns the CSV holdings URL for a given iShares ETF page.

    Uses the shared web scraping service to handle both US and UK iShares pages
    reliably, including pages that load download links via JavaScript.

    Args:
        etf_url: The URL of the iShares ETF page

    Disclaimer:
    The holdings data is owned by BlackRock and/or its third-party information providers.
    Use of this data is subject to BlackRock's copyright and licensing terms.
    This function is intended for personal, non-commercial use only.
    Do not redistribute or use the data for commercial purposes.
    """
    return get_ishares_csv_download_link(etf_url)


def get_ishares_etf_holdings(csv_url: str) -> pd.DataFrame:
    """
    Downloads and parses iShares ETF holdings CSV from the given URL.
    Assumes the CSV data is located between two U+00A0 (non-breaking space) characters in the response text.
    Disclaimer:
    The holdings data is owned by BlackRock and/or its third-party information providers.
    Use of this data is subject to BlackRock's copyright and licensing terms.
    This function is intended for personal, non-commercial use only.
    Do not redistribute or use the data for commercial purposes.
    """
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch CSV from {csv_url}: {e}")

    response.encoding = "utf-8"
    content = response.text

    # Find positions of U+00A0 (non-breaking space)
    first = content.find("\u00a0")
    second = content.find("\u00a0", first + 1)

    if first == -1 or second == -1:
        raise ValueError(
            "Content does not contain two U+00A0 characters. The format may have changed."
        )

    # Extract content between first and second U+00A0
    data_segment = content[first + 1 : second]

    try:
        df = pd.read_csv(StringIO(data_segment))
    except Exception as e:
        raise ValueError(f"Failed to parse CSV data: {e}")

    return df
