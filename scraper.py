#!/usr/bin/env python3
"""
Business Listing Scraper
Scrapes 3 business listing websites and emails new keyword-matching listings.
Runs via GitHub Actions cron schedule (no local PC required).

- BizMLS: POST form (server-side rendered, no bot protection)
- BizBuySell: Playwright + stealth (heavy bot protection)
- BusinessesForSale.com: Playwright + stealth (heavy bot protection)
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

# ── Shared cloudscraper instance (used for BizMLS) ────────────────────────────
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
            time.sleep(3)
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
BIZMLS_POST_URL = "https://bizmls.com/cgi-bin/a-bus2.asp"
BIZMLS_COUNTIES = ["Miami-Dade", "Palm Beach", "Broward"]

def scrape_bizmls() -> list:
    """Scrape BizMLS listings via POST for Miami-Dade, Palm Beach, and Broward."""
    log.info("Scraping BizMLS (3 counties)...")
    listings = []
    seen_ids = set()

    for county in BIZMLS_COUNTIES:
        log.info("BizMLS: fetching county = %s", county)
        post_data = {
            "state": "Florida",
            "process": "search",
            "lgassnc": "BIZMLS",
            "folder": "BIZMLS",
            "county": county,
        }
        html = fetch_page(BIZMLS_POST_URL, method="POST", data=post_data)
        if not html:
            log.warning("BizMLS: no response for county %s", county)
            continue

        soup = BeautifulSoup(html, "lxml")

        # Log ALL hrefs found to diagnose which patterns contain listings
        all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
        log.info("BizMLS %s: all hrefs found (%d): %s", county, len(all_hrefs), str(all_hrefs[:30]))

        # Try broad matching — any link containing .asp with a query string,
        # OR containing common listing ID patterns
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]

            # Match listing detail links — try many possible patterns
            is_listing_link = any(p in href.lower() for p in [
                "listno=", "a-bus3", "a-bus4", "a-bus5",
                "detail", "listing", "id=", "lid=", "bno=",
                "busno=", "bizno=", "listid=",
            ])

            # Also match any .asp link that has a query string (likely a detail page)
            if not is_listing_link and ".asp?" in href.lower():
                is_listing_link = True

            if not is_listing_link:
                continue

            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Skip nav links by title
            if title.lower() in {"home", "search", "login", "join", "contact", "about"}:
                continue

            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = "https://bizmls.com" + href
            else:
                full_url = "https://bizmls.com/cgi-bin/" + href

            match = re.search(r"listno=(\w+)", href, re.IGNORECASE)
            listing_id = match.group(1) if match else href

            if listing_id not in seen_ids:
                seen_ids.add(listing_id)
                listings.append({
                    "id": listing_id,
                    "title": title,
                    "url": full_url,
                    "source": f"BizMLS ({county})"
                })

        time.sleep(2)

    log.info("BizMLS: found %d total candidate listings across 3 counties", len(listings))
    return listings


# ── Site 2: BizBuySell ────────────────────────────────────────────────────────
BIZBUYSELL_URL = (
    "https://www.bizbuysell.com/florida-businesses-for-sale/"
    "?q=bGM9SmtjOU5EQW1RejFWVXlaVFBVWk1Kazg5TXpJMVB5WkhQVFF3SmtNOVZWTW1VejFHVENaUFBUTXlORDhtUnowME1DWkRQVlZUSmxNOVJrd21UejB6TXpnPSZsdD0zMCw0MCw4MA%3D%3D"
)

def scrape_bizbuysell() -> list:
    """Scrape BizBuySell Florida listings using Playwright stealth."""
    log.info("Scraping BizBuySell with Playwright...")
    html = fetch_with_playwright(BIZBUYSELL_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    listings = []
    seen_ids = set()

    # Log a sample of hrefs for diagnostics
    all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
    log.info("BizBuySell: total hrefs found: %d, sample: %s", len(all_hrefs), str(all_hrefs[:20]))

    # Try multiple selector strategies
    card_selectors = [
        "a[href*='/business-for-sale/']",
        "a[href*='/businesses-for-sale/']",
        "div[class*='listing'] a[href*='sale']",
        "article a[href*='sale']",
        "h3 a",
        "h2 a",
    ]

    for selector in card_selectors:
        cards = soup.select(selector)
        if not cards:
            continue
        log.info("BizBuySell: selector '%s' matched %d elements", selector, len(cards))
        for a_tag in cards:
            href = a_tag.get("href", "")
            if not href or ("business-for-sale" not in href and "bizbuysell.com" not in href):
                continue
            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
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
        if listings:
            break

    # Fallback: broad link scan
    if not listings:
        log.info("BizBuySell: trying fallback broad link scan")
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "business-for-sale" not in href:
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

    log.info("BizBuySell: found %d candidate listings", len(listings))
    return listings


# ── Site 3: BusinessesForSale.com ─────────────────────────────────────────────
BIZFORSALE_URL = (
    "https://us.businessesforsale.com/us/search/"
    "businesses-for-sale-in-miami-dade-palm-beach-county-and-broward-county"
    "?PageSize=100"
)

def scrape_businessesforsale() -> list:
    """Scrape BusinessesForSale.com using Playwright stealth."""
    log.info("Scraping BusinessesForSale.com with Playwright...")
    html = fetch_with_playwright(BIZFORSALE_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    listings = []
    seen_ids = set()

    # Log hrefs for diagnostics
    all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
    log.info("BusinessesForSale: total hrefs: %d, sample: %s", len(all_hrefs), str(all_hrefs[:20]))

    card_selectors = [
        "article.listing a",
        "div.listing-result a",
        "div[class*='listing'] h3 a",
        "div[class*='AdItem'] a",
        "ul.listing-list li a",
        "div[class*='search-result'] a",
        "h3 a[href*='businesses-for-sale']",
        "h2 a[href*='businesses-for-sale']",
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
            break

    # Fallback: broad link scan
    if not listings:
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

    log.info("BusinessesForSale.com: found %d candidate listings", len(listings))
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
