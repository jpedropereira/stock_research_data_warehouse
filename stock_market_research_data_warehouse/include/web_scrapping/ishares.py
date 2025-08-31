import logging
import os
from urllib.parse import urlparse

from playwright.sync_api import Page

from .base_service import WebScrapingService, get_page_with_url_overrides


def get_ishares_download_link(etf_url: str, file_type: str | None = None) -> str:
    logger = logging.getLogger(__name__)
    logger.info(f"Starting iShares download link extraction for URL: {etf_url}")
    if not etf_url or not etf_url.startswith("https://www.ishares.com"):
        raise ValueError(f"Invalid iShares URL: {etf_url}")
    if etf_url.rstrip("/").split("/")[-1].isdigit():
        logger.warning(f"URL appears incomplete (ends with product ID): {etf_url}")
        logger.warning("This may cause timeouts. Please provide the full ETF page URL.")

    with WebScrapingService() as scraper:
        logger.info("Created WebScrapingService instance, connecting to browser")
        page: Page | None = None
        try:
            logger.info(f"Navigating to page: {etf_url}")

            # Timeouts configurable via environment variables
            default_timeout_ms = int(os.getenv("ISHARES_PAGE_DEFAULT_TIMEOUT_MS", "120000"))
            nav_timeout_ms = int(os.getenv("ISHARES_NAV_TIMEOUT_MS", "120000"))
            nav_fallback_timeout_ms = int(os.getenv("ISHARES_NAV_FALLBACK_TIMEOUT_MS", "90000"))
            extra_js_wait_ms = int(os.getenv("ISHARES_EXTRA_JS_WAIT_MS", "5000"))
            selector_timeout_ms = int(os.getenv("ISHARES_SELECTOR_TIMEOUT_MS", "30000"))

            page = get_page_with_url_overrides(scraper, etf_url)
            page.set_default_timeout(default_timeout_ms)

            try:
                page.goto(etf_url, wait_until="networkidle", timeout=nav_timeout_ms)
                logger.info("Page loaded successfully with networkidle")
            except Exception as e:
                logger.warning(f"networkidle failed ({e}), trying with domcontentloaded")
                page.goto(etf_url, wait_until="domcontentloaded", timeout=nav_fallback_timeout_ms)
                logger.info("Page loaded successfully with domcontentloaded")
                logger.info("Waiting additional time for JavaScript components to load...")
                page.wait_for_timeout(extra_js_wait_ms)

            logger.info(f"Page title: {page.title()}")
            logger.info(f"Final page URL: {page.url}")

            # Cookies
            try:
                cookie_btn = "#onetrust-accept-btn-handler"
                if page.is_visible(cookie_btn, timeout=3000):
                    logger.info("Cookie banner detected. Accepting...")
                    page.click(cookie_btn)
                    page.wait_for_timeout(1000)
            except Exception as cookie_e:
                logger.debug(f"Cookie banner handling skipped/failed: {cookie_e}")

            # T&C modal
            try:
                tc_selector = ".accept-terms-condition-footer .cta a"
                if page.is_visible(tc_selector, timeout=3000):
                    logger.info("Found T&C modal, clicking Accept...")
                    try:
                        accept_el = page.query_selector(tc_selector)
                        href = accept_el.get_attribute("href") if accept_el else None
                        if href:
                            if href.startswith("/"):
                                href = "https://www.ishares.com" + href
                            logger.info(f"Navigating to T&C accept href: {href}")
                            page.goto(href, wait_until="networkidle", timeout=30000)
                        else:
                            page.click(tc_selector)
                            page.wait_for_load_state("networkidle", timeout=15000)
                        logger.info("Accepted Terms & Conditions")
                    except Exception as nav_accept_e:
                        logger.info(
                            f"Direct nav accept failed ({nav_accept_e}), attempting JS click"
                        )
                        page.evaluate("sel => document.querySelector(sel)?.click()", tc_selector)
                        page.wait_for_load_state("networkidle", timeout=15000)
                        logger.info("Accepted Terms & Conditions via JS click")
                else:
                    if "switchLocale=" not in page.url:
                        try:
                            append = (
                                "&" if "?" in page.url else "?"
                            ) + "switchLocale=y&siteEntryPassthrough=true"
                            target = page.url + append
                            logger.info(
                                f"No T&C modal visible. Forcing locale via redirect: {target}"
                            )
                            page.goto(target, wait_until="networkidle", timeout=30000)
                        except Exception as force_locale_e:
                            logger.debug(f"Locale force redirect failed: {force_locale_e}")
                    else:
                        logger.info("No T&C modal detected")
            except Exception as tc_error:
                logger.info(f"T&C handling failed: {tc_error}")

            logger.info(f"After T&C handling - Page title: {page.title()}")
            logger.info(f"After T&C handling - Page URL: {page.url}")

            # Lightweight holdings snippet for debug
            try:
                holdings_section = page.query_selector("#holdings")
                if holdings_section:
                    holdings_html = holdings_section.inner_html()
                    snippet = (
                        (holdings_html[:1200] + "...")
                        if len(holdings_html) > 1200
                        else holdings_html
                    )
                    logger.debug(f"HOLDINGS SECTION (truncated):\n{snippet}")
                else:
                    logger.debug("No #holdings section found on page")
            except Exception as holdings_e:
                logger.debug(f"Failed to get holdings section: {holdings_e}")

            # Build selector list
            if file_type == "csv":
                selectors = [
                    '#holdings .fund-component-data-export a.icon-xls-export[href*="csv"]',
                    '#holdings a.icon-xls-export[href*="holdings"]',
                    "#holdings a.icon-xls-export",
                    'a.icon-xls-export[href*="holdings&fileType=csv"]',
                    'a.icon-xls-export[href*="fileType=csv"]',
                    "a.icon-xls-export",
                    'a[data-download-type="holdings"][href*="csv"]',
                ]
                logger.info(f"Looking for CSV holdings download with {len(selectors)} selectors")
            else:
                selectors = ["a.icon-xls-export"]
                logger.info("Looking for any download link")

            logger.info("Waiting for download link to appear...")
            download_link = None
            successful_selector = None

            quick_wait_ms = int(os.getenv("ISHARES_QUICK_WAIT_MS", "2500"))
            stable_targets = [
                "#holdings .fund-component-data-export",
                "#holdings",
                "a.icon-xls-export",
            ]
            try:
                for tgt in stable_targets:
                    try:
                        page.wait_for_selector(tgt, timeout=quick_wait_ms)
                        break
                    except Exception:
                        try:
                            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                            page.wait_for_timeout(500)
                            page.evaluate("window.scrollTo(0, 0)")
                        except Exception:
                            pass
            except Exception:
                pass

            for i, selector in enumerate(selectors):
                logger.info(f"Trying selector {i + 1}/{len(selectors)}: {selector}")
                try:
                    download_link = page.wait_for_selector(selector, timeout=selector_timeout_ms)
                    if download_link:
                        successful_selector = selector
                        logger.info(f"✅ Download link found with selector: {selector}")
                        break
                except Exception as e:
                    logger.info(f"Selector {i + 1} failed: {e}")
                    if i == 0:
                        logger.info("Scrolling to trigger lazy loading...")
                        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        page.wait_for_timeout(3000)
                        page.evaluate("window.scrollTo(0, 0)")
                        page.wait_for_timeout(2000)
                        try:
                            download_link = page.wait_for_selector(selector, timeout=15000)
                            if download_link:
                                successful_selector = selector
                                logger.info(
                                    f"✅ Download link found with selector after scrolling: {selector}"
                                )
                                break
                        except Exception:
                            pass

            if not download_link:
                logger.error("No download link found with any selector")
                page_title = page.title()
                current_url = page.url
                logger.info(f"Page title: {page_title}")
                logger.info(f"Current URL: {current_url}")
                if "Exchange-Traded Funds (ETFs)" in page_title and "BlackRock" in page_title:
                    logger.error("Still on Terms & Conditions page - T&C acceptance failed")
                else:
                    logger.error("On different page - check selectors")

                try:
                    parsed = urlparse(current_url)
                    ajax_suffix = "/1467271812596.ajax?fileType=csv&dataType=fund"
                    base = (
                        f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}{ajax_suffix}"
                    )
                    logger.info(f"Synthesized AJAX CSV URL candidate: {base}")
                    resp = page.goto(base, wait_until="commit", timeout=15000)
                    content_type = ""
                    disposition = ""
                    status_ok = False
                    if resp:
                        try:
                            headers = getattr(resp, "headers", {}) or {}
                            if callable(headers):
                                headers = headers()  # type: ignore[assignment]
                            content_type = (
                                headers.get("content-type") or headers.get("Content-Type") or ""
                            ).lower()
                            disposition = (
                                headers.get("content-disposition")
                                or headers.get("Content-Disposition")
                                or ""
                            ).lower()
                            status_ok = 200 <= (resp.status or 0) < 300
                        except Exception:
                            pass
                    if status_ok and (
                        "csv" in content_type
                        or content_type.startswith("text/plain")
                        or ("octet-stream" in content_type and "filename=" in disposition)
                    ):
                        logger.info("AJAX CSV URL appears valid based on response headers")
                        return base
                    else:
                        logger.info(
                            "AJAX CSV URL headers not conclusive; discarding synthetic fallback"
                        )
                        try:
                            page.goto(current_url, wait_until="domcontentloaded", timeout=15000)
                        except Exception:
                            pass
                except Exception as synth_e:
                    logger.debug(f"AJAX CSV synthesis failed: {synth_e}")

                raise ValueError(
                    f"Could not find download link for file type '{file_type}' on {etf_url}"
                )

            href = download_link.get_attribute("href")
            logger.info(f"Raw href attribute: {href}")
            logger.info(f"Successful selector was: {successful_selector}")
            if not href:
                raise ValueError("Download link found but href is empty")
            if href.startswith("/"):
                href = "https://www.ishares.com" + href
                logger.info(f"Constructed full URL: {href}")
            logger.info(f"Successfully extracted download link: {href}")
            return href
        except Exception as e:
            file_type_msg = f" for file type '{file_type}'" if file_type else ""
            error_msg = f"Could not find download link{file_type_msg} on {etf_url}: {e}"
            logger.error(error_msg)
            try:
                if page:
                    page_title = page.title()
                    page_url = page.url
                    logger.error(f"Page title: {page_title}")
                    logger.error(f"Final page URL: {page_url}")
                    all_download_links = page.query_selector_all("a.icon-xls-export")
                    logger.error(
                        f"Found {len(all_download_links)} total icon-xls-export links on page"
                    )
                    for i, link in enumerate(all_download_links):
                        link_href = link.get_attribute("href")
                        link_text = link.inner_text().strip()
                        logger.error(
                            f"Available link {i + 1}: href='{link_href}', text='{link_text}'"
                        )
                else:
                    logger.error("Page object not available for debugging")
            except Exception as debug_e:
                logger.error(f"Error during debugging: {debug_e}")
            raise ValueError(error_msg)
        finally:
            if page:
                logger.info("Closing browser page")
                try:
                    ctx = page.context
                except Exception:
                    ctx = None
                page.close()
                try:
                    if ctx:
                        ctx.close()
                except Exception:
                    pass


def get_ishares_csv_download_link(etf_url: str) -> str:
    logger = logging.getLogger(__name__)
    try:
        return get_ishares_download_link(etf_url, file_type="csv")
    except Exception as e:
        logger.warning(f"Primary CSV extraction failed: {e}")
        logger.info("Attempting fallback: looking for any download link...")
        try:
            return get_ishares_download_link(etf_url, file_type=None)
        except Exception as e2:
            logger.error(f"Fallback also failed: {e2}")
            error_msg = f"Could not extract CSV download link from {etf_url}. "
            error_msg += "This may be due to: 1) Page loading timeout, 2) Changed page structure, "
            error_msg += "3) Network connectivity issues, or 4) Rate limiting. "
            error_msg += f"Original error: {e}, Fallback error: {e2}"
            raise ValueError(error_msg)
