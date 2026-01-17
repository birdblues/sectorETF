#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client


TICKERS = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLK", "XLU", "XLRE"]

SOURCE_URLS = {
    "XLC": "https://www.ssga.com/us/en/individual/etfs/state-street-communication-services-select-sector-spdr-etf-xlc",
    "XLY": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-discretionary-select-sector-spdr-etf-xly",
    "XLP": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-staples-select-sector-spdr-etf-xlp",
    "XLE": "https://www.ssga.com/us/en/individual/etfs/state-street-energy-select-sector-spdr-etf-xle",
    "XLF": "https://www.ssga.com/us/en/individual/etfs/funds/financial-select-sector-spdr-fund-xlf",
    "XLV": "https://www.ssga.com/us/en/individual/etfs/state-street-healthcare-select-sector-spdr-etf-xlv",
    "XLI": "https://www.ssga.com/us/en/individual/etfs/state-street-industrial-select-sector-spdr-etf-xli",
    "XLB": "https://www.ssga.com/us/en/individual/etfs/state-street-materials-select-sector-spdr-etf-xlb",
    "XLK": "https://www.ssga.com/us/en/individual/etfs/funds/technology-select-sector-spdr-fund-xlk",
    "XLU": "https://www.ssga.com/us/en/individual/etfs/state-street-utilities-select-sector-spdr-etf-xlu",  # FIXED
    "XLRE": "https://www.ssga.com/us/en/individual/etfs/state-street-real-estate-select-sector-spdr-etf-xlre",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome Safari"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# number + optional suffix (B/M/K)
AMT_RE = re.compile(r"([-+]?\$?\s?\d[\d,]*\.?\d*)\s*([BbMmKk])?")

def parse_amount(s: str) -> Optional[float]:
    if not s:
        return None
    m = AMT_RE.search(s)
    if not m:
        return None
    num = m.group(1).replace("$", "").replace(",", "").strip()
    unit = (m.group(2) or "").upper()

    try:
        x = float(num)
    except ValueError:
        return None

    mult = {"K": 1e3, "M": 1e6, "B": 1e9}.get(unit, 1.0)
    return x * mult


def find_value_near_label(text: str, label_patterns: Tuple[str, ...]) -> Optional[float]:
    lower = text.lower()
    for pat in label_patterns:
        idx = lower.find(pat.lower())
        if idx >= 0:
            window = text[idx : idx + 600]
            val = parse_amount(window)
            if val is not None:
                return val
    return None


def try_parse_asof_date(text: str) -> Optional[dt.date]:
    # Very light heuristic; if not found, falls back to UTC date
    m = re.search(r"As of\s+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", text)
    if m:
        raw = m.group(1).replace(",", "")
        for fmt in ("%b %d %Y", "%B %d %Y"):
            try:
                return dt.datetime.strptime(raw, fmt).date()
            except ValueError:
                pass

    m = re.search(r"As of\s+(\d{1,2}/\d{1,2}/\d{4})", text)
    if m:
        try:
            return dt.datetime.strptime(m.group(1), "%m/%d/%Y").date()
        except ValueError:
            pass

    return None


def dig_for_numbers(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        return ""


@dataclass
class EtfSnapshot:
    asof_date: dt.date
    nav: Optional[float]
    aum: Optional[float]
    shares_outstanding: Optional[float]
    source_url: str
    raw_payload: Dict[str, Any]


def fetch_html(url: str, retries: int = 2) -> str:
    last_err = None
    for _ in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
    raise last_err  # type: ignore


def fetch_snapshot(url: str) -> EtfSnapshot:
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text(" ", strip=True)
    asof = try_parse_asof_date(text) or dt.datetime.utcnow().date()

    # Try Next.js JSON blob first (more stable if present)
    next_data = None
    s = soup.find("script", id="__NEXT_DATA__")
    if s and s.string:
        try:
            next_data = json.loads(s.string)
        except Exception:
            next_data = None

    hay = text
    raw_payload: Dict[str, Any] = {}

    if next_data is not None:
        raw_payload["next_data_present"] = True
        raw_payload["next_data_keys"] = list(next_data.keys())
        hay = hay + " " + dig_for_numbers(next_data)
    else:
        raw_payload["next_data_present"] = False

    nav = find_value_near_label(hay, ("NAV", "Net Asset Value"))
    aum = find_value_near_label(hay, ("Net Assets", "AUM", "Total Net Assets", "Total net assets"))
    shares = find_value_near_label(hay, ("Shares Outstanding", "Shares out", "Shares outstanding"))

    raw_payload.update({
        "nav_guess": nav,
        "aum_guess": aum,
        "shares_guess": shares,
    })

    return EtfSnapshot(
        asof_date=asof,
        nav=nav,
        aum=aum,
        shares_outstanding=shares,
        source_url=url,
        raw_payload=raw_payload,
    )


def supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]  # ✅ unified name
    return create_client(url, key)


def upsert_daily(sb: Client, ticker: str, snap: EtfSnapshot) -> None:
    payload = {
        "asof_date": snap.asof_date.isoformat(),
        "ticker": ticker,
        "nav": snap.nav,
        "aum": snap.aum,
        "shares_outstanding": snap.shares_outstanding,
        "source_url": snap.source_url,
        "raw_payload": snap.raw_payload,
    }
    sb.table("sector_etf_daily").upsert(payload).execute()


def main():
    sb = supabase_client()

    missing = []
    for t in TICKERS:
        url = SOURCE_URLS.get(t)
        if not url:
            missing.append(t)
            continue

        try:
            snap = fetch_snapshot(url)
            upsert_daily(sb, t, snap)
            print(f"[OK] {t} asof={snap.asof_date} nav={snap.nav} aum={snap.aum} shares={snap.shares_outstanding}")
        except Exception as e:
            print(f"[ERR] {t} {e}")

    if missing:
        print("[WARN] SOURCE_URLS missing tickers:", ",".join(missing))


if __name__ == "__main__":
    main()