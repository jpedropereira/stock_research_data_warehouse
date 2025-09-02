def get_context_overrides_for_url(url: str) -> dict:
    try:
        if "/us/" in url:
            return {
                "locale": "en-US",
                "timezone_id": "America/New_York",
                "extra_http_headers": {"Accept-Language": "en-US,en;q=0.9"},
            }
        if "/uk/" in url:
            return {
                "locale": "en-GB",
                "timezone_id": "Europe/London",
                "extra_http_headers": {"Accept-Language": "en-GB,en;q=0.9"},
            }
    except Exception:
        pass
    return {}
