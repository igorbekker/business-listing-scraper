#!/usr/bin/env python3
"""
Business Listing Scraper
Scrapes 3 business listing websites and emails new keyword-matching listings.
Runs via GitHub Actions cron schedule (no local PC required).
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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Shared cloudscraper instance (bypasses Cloudflare/bot protection) ─────────
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False}
)

# ── Configuration ──────────────────────────────────────────────────────────────
KEYWORDS = [
    "adult", "home care", "coin", "laundromat", "car wash",
    "nemt", "medical transportation", "laundry", "psychology", "adult care"
]

SEEN_FILE = "seen_listings.json"
GMAIL_USER = os.environ["GMAIL_USER"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
RECIPIENT = "bekker.igor@gmail.com"

REQUEST_TIMEOUT = 20        # seconds per request
RETRY_ATTEMPTS = 3
RETRY_DELAY = 6             # seconds between retries
INTER_SITE_DELAY = 4        # seconds between sites


# ── Utility: fetch with retry ──────────────────────────────────────────────────
def fetch_page(url: str, method: str = "GET", data: dict = None) -> Optional[str]:
    """Fetch URL with retry logic using cloudscraper. Returns HTML or None."""
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            if method == "POST":
                resp = scraper.post(url, data=data, timeout=REQUEST_TIMEOUT)
            else:
                resp = scraper.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            log.warning("Attempt %d/%d failed for %s: %s", attempt, RETRY_ATTEMPTS, url, exc)
            if attempt < RETRY_ATTEMPTS:
                time.sleep(RETRY_DELAY)
    log.error("All retries exhausted for %s", url)
    return None


# ── Utility: Keyword matching ──────────────────────────────────────────────────
def matches_keywords(title: str) -> bool:
    """Return True if the listing title contains any target keyword."""
    lower = title.lower()
    return any(kw in lower for kw in KEYWORDS)


# ── Seen-listings persistence ──────────────────────────────────────────────────
def load_seen() -> dict:
    """Load seen_listings.json. Returns empty dict if file missing or corrupt."""
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_seen(seen: dict) -> None:
    """Persist updated seen_listings.json."""
    with open(SEEN_FILE, "w", encoding="utf-8") as f:
        json.dump(seen, f, indent=2)


# ── Site 1: BizMLS ────────────────────────────────────────────────────────────
# BizMLS uses a POST form to return results, filtered by county.
# We submit 3 separate POST requests for Miami-Dade, Palm Beach, and Broward.
BIZMLS_POST_URL = "https://bizmls.com/cgi-bin/a-bus2.asp"
BIZMLS_COUNTIES = ["Miami-Dade", "Palm Beach", "Broward"]

def scrape_bizmls() -> list:
    """Scrape BizMLS listings for Miami-Dade, Palm Beach, and Broward counties."""
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

        # Debug: show first 300 chars to verify we got results not just a form
        log.info("BizMLS %s HTML preview: %s", county, html[:300].replace("\n", " "))

        soup = BeautifulSoup(html, "lxml")

        # Scan all <a> tags — listing links typically contain listno= or a-bus3.asp
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]

            # Match any link that looks like a listing detail page
            if not any(p in href for p in ["listno=", "a-bus3", "a-bus4", "detail"]):
                continue

            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Build absolute URL
            if href.startswith("http"):
                full_url = href
            elif href.startswith("/"):
                full_url = "https://bizmls.com" + href
            else:
                full_url = "https://bizmls.com/cgi-bin/" + href

            # Use listno value as ID, fall back to full href
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

        time.sleep(2)  # small delay between county requests

    log.info("BizMLS: found %d total candidate listings across 3 counties", len(listings))
    return listings


# ── Site 2: BizBuySell ────────────────────────────────────────────────────────
BIZBUYSELL_URL = (
    "https://www.bizbuysell.com/florida-businesses-for-sale/"
    "?q=bGM9SmtjOU5EQW1RejFWVXlaVFBVWk1Kazg5TXpJMVB5WkhQVFF3SmtNOVZWTW1VejFHVENaUFBUTXlORDhtUnowME1DWkRQVlZUSmxNOVJrd21UejB6TXpnPSZsdD0zMCw0MCw4MA%3D%3D"
)

def scrape_bizbuysell() -> list:
    """Scrape BizBuySell Florida listings."""
    log.info("Scraping BizBuySell...")
    html = fetch_page(BIZBUYSELL_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
    listings = []
    seen_ids = set()

    # Try multiple CSS selector patterns
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
        log.info("BizBuySell: selector '%s' found %d elements", selector, len(cards))
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
        log.info("BizBuySell: trying fallback link scan")
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
    """Scrape BusinessesForSale.com Miami-Dade/Palm Beach/Broward listings."""
    log.info("Scraping BusinessesForSale.com...")
    html = fetch_page(BIZFORSALE_URL)
    if not html:
        return []

    soup = BeautifulSoup(html, "lxml")
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
    ]

    for selector in card_selectors:
        cards = soup.select(selector)
        if not cards:
            continue
        log.info("BusinessesForSale: selector '%s' found %d elements", selector, len(cards))
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
        log.info("BusinessesForSale: trying fallback link scan")
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
    """Build a nicely formatted HTML email body."""
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
    <h1 style="font-size:20px; color:#111827; margin:0 0 4px;">
      New Business Listings Alert
    </h1>
    <p style="color:#6b7280; font-size:14px; margin:0 0 20px;">
      Run time: {run_time} &nbsp;|&nbsp; {len(new_listings)} new matching listing(s) found
    </p>
    <hr style="border:none; border-top:1px solid #e5e7eb; margin:0 0 20px;">
    {sections}
    <hr style="border:none; border-top:1px solid #e5e7eb; margin:24px 0 16px;">
    <p style="font-size:12px; color:#9ca3af; margin:0;">
      Keywords monitored: {', '.join(KEYWORDS)}<br>
      Sources: BizMLS (Miami-Dade, Palm Beach, Broward) &bull; BizBuySell &bull; BusinessesForSale.com
    </p>
  </div>
</body>
</html>"""


def send_email(new_listings: list) -> None:
    """Send HTML email via Gmail SMTP."""
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
