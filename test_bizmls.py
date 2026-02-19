#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup

post_data = [
    ("folder", "BIZMLS"), ("state", "FLORIDA"), ("adstr", ""), ("disp", ""),
    ("src", ""), ("copypaste", ""), ("rpp", ""), ("org_only", ""), ("gen_hp", ""),
    ("org", "BIZMLS"), ("process", "search"), ("local", ""), ("list", ""),
    ("sp", ""), ("lastcatval", ""), ("displayall", ""), ("usealt", ""),
    ("howdisplay", "freeform"), ("country", "USA"),
    ("county", "USA/Florida/Broward"),
    ("sic_code", ""), ("salesmin", ""), ("salesmax", ""), ("listnum", ""),
    ("relocate", ""), ("h_based", ""), ("franchise", ""), ("sba_pq", ""),
    ("re_incl", ""), ("re_nincl", ""), ("re_avail", ""), ("down", ""),
    ("days_changed", ""), ("evisa_qualified", ""), ("altdownmax", ""),
    ("altgrossmin", ""), ("altgrossmax", ""),
    ("pricemin", "-99999999"), ("altpricemin", "Enter Amount"),
    ("pricemax", "999999999"), ("altpricemax", "Enter Amount"),
    ("net_income", "-999999999"), ("altnetmin", "Enter Amount"), ("keyword", ""),
]

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Referer": "https://bizmls.com/cgi-bin/a-bus2.asp?state=Florida&process=search&lgassnc=BIZMLS&folder=BIZMLS",
    "Origin": "https://bizmls.com",
    "Content-Type": "application/x-www-form-urlencoded",
}

session = requests.Session()
session.get("https://bizmls.com/cgi-bin/a-bus2.asp?state=Florida&process=search&lgassnc=BIZMLS&folder=BIZMLS", headers=headers, timeout=20)
resp = session.post("https://bizmls.com/cgi-bin/a-bus2ff.asp", data=post_data, headers=headers, params={"forsale": "go"}, timeout=30)

print(f"Status: {resp.status_code}, Length: {len(resp.text)}")

soup = BeautifulSoup(resp.text, "lxml")

# Print every link with its text
print("\n=== ALL LINKS ===")
for a in soup.find_all("a", href=True):
    txt = a.get_text(strip=True)
    href = a["href"]
    if txt:
        print(f"{txt!r:60s} {href}")
