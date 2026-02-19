#!/usr/bin/env python3
"""
Business Listing Scraper
Scrapes 3 business listing websites and emails new keyword-matching listings.
Runs via GitHub Actions cron schedule (no local PC required).

- BizMLS: GET form to extract hidden fields, then POST per county
- BizBuySell: RSS feed (XML, no bot protection) + Playwright fallback
- BusinessesForSale.com: Playwright + stealth (Cloudflare bypass)
"""

import json
import os
import re
import smtplib
import time
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import Optional
from xml.etree import ElementTree

import cloudscraper
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from playwright_stealth import stealth_sync

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Shared cloudscraper instance ──────────────────────────────────────────────
cs = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False}
)

# ── Configuration ──────────────────────────────────────────────────────────────
KEYWORDS_FILE = "keywords.json"
SEEN_FILE = "seen_listings.json"

def load_keywords() -> list:
    """Load keywords from keywords.json. Falls back to empty list if missing."""
    try:
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                kws = [str(k).strip().lower() for k in data if str(k).strip()]
                log.info("Loaded %d keywords from %s: %s", len(kws), KEYWORDS_FILE, kws)
                return kws
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error("Could not load %s: %s — no keywords active!", KEYWORDS_FILE, e)
    return []

KEYWORDS = load_keywords()
GMAIL_USER = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT = "bekker.igor@gmail.com"

REQUEST_TIMEOUT = 20
RETRY_ATTEMPTS = 3
RETRY_DELAY = 6
INTER_SITE_DELAY = 4


# ── Utility: cloudscraper fetch with retry ────────────────────────────────────
def fetch_page(url: str, method: str = "GET", data: dict = None) -> Optional[str]:
    """Fetch URL using cloudscraper with retry logic. Returns HTML or None."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            if method == "POST":
                resp = cs.post(url, data=data, timeout=REQUEST_TIMEOUT)
            else:
                resp = cs.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, RETRY_ATTEMPTS, url, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
    log.error("All retries exhausted for %s", url)
    return None


# ── Utility: Playwright fetch (bypasses JS bot detection) ─────────────────────
def fetch_with_playwright(url: str, wait_for: str = "networkidle") -> Optional[str]:
    """Fetch a page using headless Playwright with stealth mode. Returns HTML or None."""
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 800},
                locale="en-US",
            )
            page = context.new_page()
            stealth_sync(page)
            page.goto(url, wait_until=wait_for, timeout=45000)
            # Extra wait to let dynamic content render
            time.sleep(5)
            html = page.content()
            browser.close()
            return html
    except PlaywrightTimeout:
        log.warning("Playwright timeout for %s, retrying with domcontentloaded...", url)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/121.0.0.0 Safari/537.36"
                    ),
                    viewport={"width": 1280, "height": 800},
                )
                page = context.new_page()
                stealth_sync(page)
                page.goto(url, wait_until="domcontentloaded", timeout=45000)
                time.sleep(5)
                html = page.content()
                browser.close()
                return html
        except Exception as exc:
            log.error("Playwright retry also failed for %s: %s", url, exc)
            return None
    except Exception as exc:
        log.error("Playwright failed for %s: %s", url, exc)
        return None


# ── Utility: Keyword matching ──────────────────────────────────────────────────
def matches_keywords(title: str) -> bool:
    lower = title.lower()
    return any(kw in lower for kw in KEYWORDS)


# ── Seen-listings persistence ──────────────────────────────────────────────────
def load_seen() -> dict:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_seen(seen: dict) -> None:
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(seen, f, indent=2)


# ── Site 1: BizMLS ────────────────────────────────────────────────────────────
# Results page requires JavaScript to render — use Playwright
# County values exactly as they appear in the BizMLS dropdown
BIZMLS_COUNTIES = {
    "Miami-Dade": "Miami-Dade",
    "Palm Beach":  "Palm Beach",
    "Broward":     "Broward",
}

def scrape_bizmls() -> list:
    """
    Scrape BizMLS listings for Miami-Dade, Palm Beach, and Broward.
    Strategy:
      - Results are JavaScript-rendered, so use Playwright.
      - Load the search page, select county from dropdown, submit, parse table.
      - Also try direct GET URL with county param as fallback.
    """
    log.info("Scraping BizMLS (3 counties via Playwright)...")
    listings = []
    seen_ids = set()

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            page = context.new_page()
            stealth_sync(page)

            for county_label, county_value in BIZMLS_COUNTIES.items():
                log.info("BizMLS: loading county = %s", county_label)
                try:
                    # Load the business search page
                    page.goto(
                        "https://bizmls.com/business-search.asp",
                        wait_until="domcontentloaded",
                        timeout=30000,
                    )
                    time.sleep(2)

                    # Log all form elements to understand the page structure
                    forms_info = page.evaluate("""() => {
                        const forms = document.querySelectorAll('form');
                        return Array.from(forms).map(f => ({
                            action: f.action,
                            method: f.method,
                            inputs: Array.from(f.querySelectorAll('input,select')).map(i => ({
                                tag: i.tagName,
                                name: i.name,
                                type: i.type,
                                value: i.value,
                            }))
                        }));
                    }""")
                    log.info("BizMLS: forms found: %s", json.dumps(forms_info)[:800])

                    # Try to select county in dropdown
                    county_selected = False
                    try:
                        # Try common select element names for county
                        for sel_name in ["county", "County", "COUNTY", "cty", "region"]:
                            try:
                                page.select_option(f'select[name="{sel_name}"]', label=county_value, timeout=3000)
                                log.info("BizMLS: selected county via select[name=%s]", sel_name)
                                county_selected = True
                                break
                            except Exception:
                                pass
                        if not county_selected:
                            # Try by visible text
                            page.select_option("select", label=county_value, timeout=3000)
                            county_selected = True
                    except Exception as e:
                        log.warning("BizMLS: could not select county dropdown: %s", e)

                    # Submit the form / click search button
                    try:
                        page.click('input[type="submit"], button[type="submit"], button:has-text("Search")', timeout=5000)
                    except Exception:
                        log.warning("BizMLS: could not click submit, trying Enter key")
                        page.keyboard.press("Enter")

                    page.wait_for_load_state("networkidle", timeout=15000)
                    time.sleep(2)

                except Exception as e:
                    log.warning("BizMLS: form interaction failed for %s: %s — trying direct URL", county_label, e)
                    # Fallback: direct URL with county as query param
                    page.goto(
                        f"https://bizmls.com/cgi-bin/a-bus2.asp?state=Florida&process=search"
                        f"&lgassnc=BIZMLS&folder=BIZMLS&county={county_value}",
                        wait_until="networkidle",
                        timeout=30000,
                    )
                    time.sleep(3)

                html = page.content()
                log.info("BizMLS %s HTML (first 1500 chars): %s", county_label, html[:1500])

                soup = BeautifulSoup(html, "lxml")
                all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
                log.info("BizMLS %s: hrefs (%d): %s", county_label, len(all_hrefs), str(all_hrefs[:50]))

                # Parse listing links — BizMLS uses a-bus3.asp or a-bus4.asp for detail pages
                # Also check table rows for business data
                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    is_listing_link = any(p in href.lower() for p in [
                        "listno=", "a-bus3", "a-bus4", "a-bus5",
                        "detail", "id=", "lid=", "bno=", "busno=", "bizno=", "listid=",
                    ])
                    if not is_listing_link:
                        continue

                    title = a_tag.get_text(strip=True)
                    if not title or len(title) < 3:
                        # Try to get title from parent row
                        row = a_tag.find_parent("tr")
                        if row:
                            title = row.get_text(separator=" ", strip=True)[:100]
                    if not title or len(title) < 3:
                        continue
                    if title.lower() in {"home", "search", "login", "join", "contact", "about", "finance", "brokers"}:
                        continue

                    if href.startswith("http"):
                        full_url = href
                    elif href.startswith("/"):
                        full_url = "https://bizmls.com" + href
                    else:
                        full_url = "https://bizmls.com/cgi-bin/" + href.lstrip("../")

                    match = re.search(r"listno=(\w+)", href, re.IGNORECASE)
                    listing_id = match.group(1) if match else href

                    if listing_id not in seen_ids:
                        seen_ids.add(listing_id)
                        listings.append({
                            "id": listing_id,
                            "title": title,
                            "url": full_url,
                            "source": f"BizMLS ({county_label})"
                        })

                time.sleep(2)

            browser.close()

    except Exception as exc:
        log.error("BizMLS Playwright failed: %s", exc, exc_info=True)

    log.info("BizMLS: found %d total candidate listings across 3 counties", len(listings))
    return listings


# ── Site 2: BizBuySell ────────────────────────────────────────────────────────
# BizBuySell uses Akamai bot protection that blocks all headless browsers.
# Strategy: use their internal JSON search API (same one their website calls via XHR).
# Miami-Dade=30, Palm Beach=40, Broward=80 are the county location type IDs.
BIZBUYSELL_API_URL = (
    "https://www.bizbuysell.com/searchsuggest/typeahead/"
)
# Fallback HTML page
BIZBUYSELL_URL = (
    "https://www.bizbuysell.com/florida-businesses-for-sale/"
    "?q=bGM9SmtjOU5EQW1RejFWVXlaVFBVWk1Kazg5TXpJMVB5WkhQVFF3SmtNOVZWTW1VejFHVENaUFBUTXlORDhtUnowME1DWkRQVlZUSmxNOVJrd21UejB6TXpnPSZsdD0zMCw0MCw4MA%3D%3D"
)

# Encoded search query covering Miami-Dade (lt=30), Palm Beach (lt=40), Broward (lt=80)
# This is the base64 search query from the user's URL decoded:
# lc=Jkc9NDAmQz1VUyZUWlBVWk1Kk89MzI1UHlZKHPTQwJkM9VVMmUz1HTCZPTMyND8mRz00MCZDPVVUJlM9RkwmVz1HVCZaTlBUTXlORDgmRz00MCZDPVZUJlM9Rkwm
BIZBUYSELL_SEARCH_URL = (
    "https://www.bizbuysell.com/florida-businesses-for-sale/"
    "?q=bGM9SmtjOU5EQW1RejFWVXlaVFBVWk1Kazg5TXpJMVB5WkhQVFF3SmtNOVZWTW1VejFHVENaUFBUTXlORDhtUnowME1DWkRQVlZUSmxNOVJrd21UejB6TXpnPSZsdD0zMCw0MCw4MA%3D%3D"
)


def scrape_bizbuysell() -> list:
    """
    Scrape BizBuySell Florida listings.
    BizBuySell uses Akamai CDN which blocks all standard scrapers.
    Strategy: Try their internal search API with spoofed headers, then Playwright.
    """
    log.info("Scraping BizBuySell...")
    listings = []
    seen_ids = set()

    # ── Strategy 1: Internal JSON search API ─────────────────────────────────
    # BizBuySell exposes a search API endpoint used by their own frontend
    api_urls = [
        # Search API with Florida + 3 counties (lt=30,40,80)
        "https://www.bizbuysell.com/bbs-search/listings/search?lt=30,40,80&st=FL&pg=1&pgSize=100&SortOrder=0",
        # Alternative: county-specific search
        "https://www.bizbuysell.com/bbs-search/listings/search?CountyIds=30,40,80&StateId=9&pg=1&pgSize=100",
    ]

    for api_url in api_urls:
        log.info("BizBuySell: trying API: %s", api_url)
        try:
            resp = cs.get(
                api_url,
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.bizbuysell.com/florida-businesses-for-sale/",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=REQUEST_TIMEOUT,
            )
            log.info("BizBuySell API status: %d, response (first 500): %s", resp.status_code, resp.text[:500])
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    items = (
                        data.get("listings", [])
                        or data.get("results", [])
                        or data.get("data", [])
                        or (data if isinstance(data, list) else [])
                    )
                    log.info("BizBuySell API: found %d items in JSON", len(items))
                    for item in items:
                        title = (
                            item.get("businessName") or item.get("title")
                            or item.get("name") or item.get("BusinessName", "")
                        )
                        url = (
                            item.get("url") or item.get("link")
                            or item.get("detailUrl") or item.get("Url", "")
                        )
                        listing_id = str(
                            item.get("listingId") or item.get("id")
                            or item.get("ListingId") or url
                        )
                        if not title or not url:
                            continue
                        if not url.startswith("http"):
                            url = "https://www.bizbuysell.com" + url
                        if listing_id not in seen_ids:
                            seen_ids.add(listing_id)
                            listings.append({
                                "id": listing_id,
                                "title": title,
                                "url": url,
                                "source": "BizBuySell",
                            })
                    if listings:
                        log.info("BizBuySell API: returning %d listings", len(listings))
                        return listings
                except Exception as je:
                    log.warning("BizBuySell API JSON parse error: %s", je)
        except Exception as e:
            log.warning("BizBuySell API request failed: %s", e)

    # ── Strategy 2: Playwright with extended wait ─────────────────────────────
    log.info("BizBuySell: API failed, trying Playwright with extended wait...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
            )
            page = context.new_page()
            stealth_sync(page)

            # First visit homepage to get cookies
            page.goto("https://www.bizbuysell.com/", wait_until="domcontentloaded", timeout=30000)
            time.sleep(3)

            # Now navigate to search results
            page.goto(BIZBUYSELL_SEARCH_URL, wait_until="networkidle", timeout=45000)
            time.sleep(5)

            html = page.content()
            log.info("BizBuySell Playwright HTML (first 1000 chars): %s", html[:1000])

            soup = BeautifulSoup(html, "lxml")
            all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
            log.info("BizBuySell Playwright: total hrefs: %d, sample: %s", len(all_hrefs), str(all_hrefs[:20]))

            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "business-for-sale" not in href and "bizbuysell.com" not in href:
                    continue
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 5:
                    continue
                full_url = href if href.startswith("http") else "https://www.bizbuysell.com" + href
                path_parts = [p for p in href.rstrip("/").split("/") if p]
                listing_id = path_parts[-1] if path_parts else href
                if listing_id not in seen_ids:
                    seen_ids.add(listing_id)
                    listings.append({
                        "id": listing_id,
                        "title": title,
                        "url": full_url,
                        "source": "BizBuySell"
                    })

            browser.close()
    except Exception as exc:
        log.error("BizBuySell Playwright failed: %s", exc, exc_info=True)

    log.info("BizBuySell: found %d candidate listings", len(listings))
    return listings


# ── Site 3: BusinessesForSale.com ─────────────────────────────────────────────
BIZFORSALE_URL = (
    "https://us.businessesforsale.com/us/search/"
    "businesses-for-sale-in-miami-dade-palm-beach-county-and-broward-county"
    "?PageSize=100"
)

def scrape_businessesforsale() -> list:
    """
    Scrape BusinessesForSale.com for Miami-Dade, Palm Beach, Broward.
    Strategy:
      1. Try Playwright with extended wait for Cloudflare JS challenge to resolve.
         Cloudflare's "Just a moment..." page auto-redirects after ~5 seconds of JS execution.
      2. Also try their internal JSON API.
    """
    log.info("Scraping BusinessesForSale.com...")
    listings = []

    # ── Strategy 1: JSON API ──────────────────────────────────────────────────
    # BusinessesForSale.com has an internal API used by their search
    api_urls = [
        "https://us.businessesforsale.com/api/search?CountyIds=miami-dade,palm-beach,broward&PageSize=100&PageIndex=0",
        "https://us.businessesforsale.com/api/v1/listings?location=miami-dade-palm-beach-county-broward-county&pageSize=100",
    ]
    for api_url in api_urls:
        log.info("BusinessesForSale: trying API: %s", api_url)
        try:
            resp = cs.get(
                api_url,
                headers={"Accept": "application/json", "Referer": "https://us.businessesforsale.com/"},
                timeout=REQUEST_TIMEOUT,
            )
            log.info("BusinessesForSale API status: %d, response (first 300): %s", resp.status_code, resp.text[:300])
            if resp.status_code == 200 and resp.text.strip().startswith("{"):
                data = resp.json()
                log.info("BusinessesForSale API JSON keys: %s", list(data.keys())[:10])
        except Exception as e:
            log.warning("BusinessesForSale API attempt failed: %s", e)

    # ── Strategy 2: Playwright with long Cloudflare wait ─────────────────────
    log.info("BusinessesForSale: trying Playwright with Cloudflare bypass wait...")
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/121.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                },
            )
            page = context.new_page()
            stealth_sync(page)

            # Navigate and wait for Cloudflare challenge to auto-pass
            # Cloudflare's JS challenge typically resolves within 5-10 seconds
            page.goto(BIZFORSALE_URL, wait_until="domcontentloaded", timeout=45000)

            # Wait and check repeatedly — Cloudflare auto-redirects after solving
            for wait_round in range(6):  # wait up to 30 seconds total
                time.sleep(5)
                title = page.title()
                html_check = page.content()
                log.info("BusinessesForSale wait round %d: page title='%s', hrefs=%d",
                         wait_round + 1, title,
                         len(BeautifulSoup(html_check, "lxml").find_all("a", href=True)))
                if "just a moment" not in title.lower() and "cloudflare" not in html_check.lower()[:500]:
                    log.info("BusinessesForSale: Cloudflare challenge passed!")
                    break
                # Simulate human-like mouse movement
                try:
                    page.mouse.move(640, 400)
                    page.mouse.move(700, 450)
                except Exception:
                    pass

            html = page.content()
            log.info("BusinessesForSale final HTML (first 1500 chars): %s", html[:1500])

            soup = BeautifulSoup(html, "lxml")
            all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
            log.info("BusinessesForSale Playwright final: total hrefs: %d, sample: %s",
                     len(all_hrefs), str(all_hrefs[:30]))

            listings = _parse_bizforsale_html(soup)
            browser.close()

    except Exception as exc:
        log.error("BusinessesForSale Playwright failed: %s", exc, exc_info=True)

    log.info("BusinessesForSale.com: found %d candidate listings", len(listings))
    return listings


def _parse_bizforsale_html(soup: BeautifulSoup) -> list:
    """Parse BusinessesForSale HTML and return listing dicts."""
    listings = []
    seen_ids = set()

    card_selectors = [
        "article.listing a",
        "div.listing-result a",
        "div[class*='listing'] h3 a",
        "div[class*='AdItem'] a",
        "ul.listing-list li a",
        "div[class*='search-result'] a",
        "h3 a[href*='businesses-for-sale']",
        "h2 a[href*='businesses-for-sale']",
        "a[href*='/us/businesses-for-sale/']",
        "a[href*='businesses-for-sale-in']",
    ]

    for selector in card_selectors:
        cards = soup.select(selector)
        if not cards:
            continue
        log.info("BusinessesForSale: selector '%s' matched %d elements", selector, len(cards))
        for a_tag in cards:
            href = a_tag.get("href", "")
            if not href or "businesses-for-sale" not in href:
                continue
            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
                continue
            full_url = href if href.startswith("http") else "https://us.businessesforsale.com" + href
            match = re.search(r"/(\d+)(?:/|\?|$)", href)
            listing_id = match.group(1) if match else href
            if listing_id not in seen_ids:
                seen_ids.add(listing_id)
                listings.append({
                    "id": listing_id,
                    "title": title,
                    "url": full_url,
                    "source": "BusinessesForSale.com"
                })
        if listings:
            return listings

    # Fallback: broad link scan
    log.info("BusinessesForSale: trying fallback broad link scan")
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "businesses-for-sale" not in href:
            continue
        title = a_tag.get_text(strip=True)
        if not title or len(title) < 5:
            continue
        full_url = href if href.startswith("http") else "https://us.businessesforsale.com" + href
        match = re.search(r"/(\d+)(?:/|\?|$)", href)
        listing_id = match.group(1) if match else href
        if listing_id not in seen_ids:
            seen_ids.add(listing_id)
            listings.append({
                "id": listing_id,
                "title": title,
                "url": full_url,
                "source": "BusinessesForSale.com"
            })

    return listings


# ── Email ─────────────────────────────────────────────────────────────────────
def build_html_email(new_listings: list) -> str:
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    by_source = {}
    for listing in new_listings:
        by_source.setdefault(listing["source"], []).append(listing)

    sections = ""
    for source, items in by_source.items():
        rows = ""
        for item in items:
            rows += f"""
            <tr>
              <td style="padding:10px 12px; border-bottom:1px solid #e5e7eb;">
                <a href="{item['url']}" style="color:#1d4ed8; text-decoration:none; font-weight:600;">
                  {item['title']}
                </a>
              </td>
              <td style="padding:10px 12px; border-bottom:1px solid #e5e7eb; color:#6b7280; font-size:13px;">
                <a href="{item['url']}" style="color:#6b7280; word-break:break-all;">{item['url']}</a>
              </td>
            </tr>"""
        sections += f"""
        <h2 style="font-family:Arial,sans-serif; font-size:16px; color:#374151;
                   margin:24px 0 8px; border-left:4px solid #3b82f6; padding-left:10px;">
          {source} &mdash; {len(items)} new listing(s)
        </h2>
        <table style="width:100%; border-collapse:collapse; background:#fff;
                      border:1px solid #e5e7eb; border-radius:6px; overflow:hidden;">
          <thead>
            <tr style="background:#f3f4f6;">
              <th style="padding:10px 12px; text-align:left; font-family:Arial,sans-serif;
                         font-size:13px; color:#374151; width:45%;">Business Name</th>
              <th style="padding:10px 12px; text-align:left; font-family:Arial,sans-serif;
                         font-size:13px; color:#374151;">Link</th>
            </tr>
          </thead>
          <tbody>{rows}
          </tbody>
        </table>"""

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif; background:#f9fafb; padding:20px; color:#111827;">
  <div style="max-width:800px; margin:0 auto; background:#fff; border-radius:8px;
              box-shadow:0 1px 3px rgba(0,0,0,0.1); padding:30px;">
    <h1 style="font-size:20px; color:#111827; margin:0 0 4px;">New Business Listings Alert</h1>
    <p style="color:#6b7280; font-size:14px; margin:0 0 20px;">
      Run time: {run_time} &nbsp;|&nbsp; {len(new_listings)} new matching listing(s) found
    </p>
    <hr style="border:none; border-top:1px solid #e5e7eb; margin:0 0 20px;">
    {sections}
    <hr style="border:none; border-top:1px solid #e5e7eb; margin:24px 0 16px;">
    <p style="font-size:12px; color:#9ca3af; margin:0;">
      Keywords: {', '.join(KEYWORDS)}<br>
      Sources: BizMLS (Miami-Dade, Palm Beach, Broward) &bull; BizBuySell &bull; BusinessesForSale.com
    </p>
  </div>
</body>
</html>"""


def send_email(new_listings: list) -> None:
    subject = (
        f"[Business Alert] {len(new_listings)} New Listing(s) Found "
        f"– {datetime.utcnow().strftime('%Y-%m-%d')}"
    )
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = RECIPIENT
    msg.attach(MIMEText(build_html_email(new_listings), "html", "utf-8"))

    log.info("Sending email to %s...", RECIPIENT)
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls()
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, RECIPIENT, msg.as_string())
    log.info("Email sent successfully.")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    log.info("=== Business Listing Scraper starting ===")
    seen = load_seen()
    all_new = []

    scrapers = [
        ("bizmls", scrape_bizmls),
        ("bizbuysell", scrape_bizbuysell),
        ("businessesforsale", scrape_businessesforsale),
    ]

    for site_key, scraper_fn in scrapers:
        try:
            time.sleep(INTER_SITE_DELAY)
            site_listings = scraper_fn()
            site_seen_set = set(seen.get(site_key, []))

            new_for_site = []
            for listing in site_listings:
                if not matches_keywords(listing["title"]):
                    continue
                if listing["id"] not in site_seen_set:
                    new_for_site.append(listing)
                    site_seen_set.add(listing["id"])

            log.info("%s: %d keyword-matching NEW listings", site_key, len(new_for_site))
            seen[site_key] = list(site_seen_set)
            all_new.extend(new_for_site)

        except Exception as exc:
            log.error("Scraper failed for %s: %s", site_key, exc, exc_info=True)

    save_seen(seen)
    log.info("Saved %s", SEEN_FILE)

    if all_new:
        log.info("Total new listings to email: %d", len(all_new))
        try:
            send_email(all_new)
        except Exception as exc:
            log.error("Email send failed: %s", exc, exc_info=True)
    else:
        log.info("No new keyword-matching listings found. No email sent.")

    log.info("=== Scraper complete ===")


if __name__ == "__main__":
    main()
