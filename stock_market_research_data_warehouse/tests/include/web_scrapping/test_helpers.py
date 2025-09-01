from include.web_scrapping.helpers import get_context_overrides_for_url


class TestGetContextOverridesForUrl:
    def test_us_overrides(self):
        url = "https://www.ishares.com/us/products/12345"
        overrides = get_context_overrides_for_url(url)
        assert overrides["locale"] == "en-US"
        assert overrides["timezone_id"] == "America/New_York"
        assert overrides["extra_http_headers"]["Accept-Language"].startswith("en-US")

    def test_uk_overrides(self):
        url = "https://www.ishares.com/uk/products/12345"
        overrides = get_context_overrides_for_url(url)
        assert overrides["locale"] == "en-GB"
        assert overrides["timezone_id"] == "Europe/London"
        assert overrides["extra_http_headers"]["Accept-Language"].startswith("en-GB")

    def test_other_returns_empty(self):
        url = "https://www.ishares.com/de/products/12345"
        assert get_context_overrides_for_url(url) == {}
