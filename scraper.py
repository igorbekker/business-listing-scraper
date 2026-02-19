#!/usr/bin/env python3
"""
Business Listing Scraper
Scrapes business listing websites and emails new keyword-matching listings.
Runs via GitHub Actions cron schedule (no local PC required).

- BizMLS: direct POST to a-bus2ff.asp with correct form fields and county values
"""

import json
import os
import re
import smtplib
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

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
# Form POSTs to a-bus2ff.asp. County values use "USA/Florida/<County>" format.
# All three counties are submitted in a single POST as a multi-value field.
BIZMLS_POST_URL = "https://bizmls.com/cgi-bin/a-bus2ff.asp"
BIZMLS_COUNTIES = [
    "USA/Florida/Miami-Dade",
    "USA/Florida/Palm Beach",
    "USA/Florida/Broward",
]
BIZMLS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Referer": (
        "https://bizmls.com/cgi-bin/a-bus2.asp"
        "?state=Florida&process=search&lgassnc=BIZMLS&folder=BIZMLS"
    ),
    "Origin": "https://bizmls.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
}

def scrape_bizmls() -> list:
    """
    Scrape BizMLS listings for Miami-Dade, Palm Beach, and Broward.
    Uses a direct POST to a-bus2ff.asp with the correct hidden fields and
    county values discovered by inspecting the actual form HTML.
    """
    log.info("Scraping BizMLS via direct POST...")
    listings = []
    seen_ids = set()

    # Build the POST body as a list of tuples to support multi-value county field
    post_data = [
        ("folder",      "BIZMLS"),
        ("state",       "FLORIDA"),
        ("org",         "BIZMLS"),
        ("process",     "search"),
        ("country",     "USA"),
        ("disp_cat",    "est"),
        ("howdisplay",  "ol"),
        ("pricemin",    "-99999999"),
        ("pricemax",    "999999999"),
        ("net_income",  "-999999999"),
        ("adstr",       ""),
        ("src",         ""),
        ("copypaste",   ""),
        ("rpp",         ""),
        ("org_only",    ""),
        ("gen_hp",      ""),
        ("local",       ""),
        ("list",        ""),
        ("sp",          ""),
        ("lastcatval",  ""),
        ("displayall",  "Y"),
        ("usealt",      ""),
        ("sic_code",    ""),
        ("salesmin",    ""),
        ("salesmax",    ""),
        ("listnum",     ""),
        ("relocate",    ""),
        ("h_based",     ""),
        ("franchise",   ""),
        ("sba_pq",      ""),
        ("re_incl",     ""),
        ("re_nincl",    ""),
        ("re_avail",    ""),
        ("down",        ""),
        ("days_changed",""),
        ("evisa_qualified", ""),
        ("altdownmax",  ""),
        ("altgrossmin", ""),
        ("altgrossmax", ""),
        ("keyword",     ""),
    ]
    # Add all three counties as repeated field
    for county in BIZMLS_COUNTIES:
        post_data.append(("county", county))

    try:
        session = requests.Session()
        # First GET the form page to pick up any session cookies
        session.get(
            "https://bizmls.com/cgi-bin/a-bus2.asp"
            "?state=Florida&process=search&lgassnc=BIZMLS&folder=BIZMLS",
            headers=BIZMLS_HEADERS,
            timeout=20,
        )
        resp = session.post(
            BIZMLS_POST_URL,
            data=post_data,
            headers=BIZMLS_HEADERS,
            params={"forsale": "go"},
            timeout=30,
        )
        resp.raise_for_status()
        log.info("BizMLS POST status: %d, content length: %d", resp.status_code, len(resp.text))
        log.info("BizMLS POST response (first 2000 chars): %s", resp.text[:2000])

        soup = BeautifulSoup(resp.text, "lxml")
        all_hrefs = [a.get("href", "") for a in soup.find_all("a", href=True)]
        log.info("BizMLS: total hrefs found: %d, ALL: %s", len(all_hrefs), str(all_hrefs))

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            is_listing_link = any(p in href.lower() for p in [
                "listno=", "a-bus3", "a-bus4", "a-bus5",
                "detail", "lid=", "bno=", "busno=", "bizno=", "listid=",
            ])
            if not is_listing_link:
                continue

            title = a_tag.get_text(strip=True)
            if not title or len(title) < 3:
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
                # Determine county label from listing URL if possible
                listings.append({
                    "id": listing_id,
                    "title": title,
                    "url": full_url,
                    "source": "BizMLS",
                })

    except Exception as exc:
        log.error("BizMLS POST failed: %s", exc, exc_info=True)

    log.info("BizMLS: found %d total candidate listings", len(listings))
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
      Sources: BizMLS (Miami-Dade, Palm Beach, Broward)
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
    ]

    for site_key, scraper_fn in scrapers:
        try:
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
