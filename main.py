#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import datetime as dt
from dataclasses import dataclass
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client


TICKERS = ["XLC", "XLY", "XLP", "XLE", "XLF", "XLV", "XLI", "XLB", "XLK", "XLU", "XLRE", "GLD", "SPAB", "SPY"]

SOURCE_URLS = {
    "XLC": "https://www.ssga.com/us/en/individual/etfs/state-street-communication-services-select-sector-spdr-etf-xlc",
    "XLY": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-discretionary-select-sector-spdr-etf-xly",
    "XLP": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-staples-select-sector-spdr-etf-xlp",
    "XLE": "https://www.ssga.com/us/en/individual/etfs/state-street-energy-select-sector-spdr-etf-xle",
    "XLF": "https://www.ssga.com/us/en/individual/etfs/funds/financial-select-sector-spdr-fund-xlf",
    "XLV": "https://www.ssga.com/us/en/individual/etfs/state-street-healthcare-select-sector-spdr-etf-xlv",
    "XLI": "https://www.ssga.com/us/en/individual/etfs/state-street-industrial-select-sector-spdr-etf-xli",
    "XLB": "https://www.ssga.com/us/en/individual/etfs/state-street-materials-select-sector-spdr-etf-xlb",
    "XLK": "https://www.ssga.com/us/en/individual/etfs/state-street-technology-select-sector-spdr-etf-xlk",
    "XLU": "https://www.ssga.com/us/en/individual/etfs/state-street-utilities-select-sector-spdr-etf-xlu",
    "XLRE": "https://www.ssga.com/us/en/individual/etfs/state-street-real-estate-select-sector-spdr-etf-xlre",
    "GLD": "https://www.ssga.com/us/en/individual/etfs/spdr-gold-shares-gld",
    "SPAB": "https://www.ssga.com/us/en/individual/etfs/state-street-spdr-portfolio-aggregate-bond-etf-spab",
    "SPY": "https://www.ssga.com/us/en/individual/etfs/state-street-spdr-sp-500-etf-trust-spy",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome Safari"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# number + optional suffix (B/M/K)
AMT_RE = re.compile(r"\$?\s*([\d,]+(?:\.\d+)?)\s*([BbMmKk])\b")


def unit_to_mult(unit: str) -> float:
    u = unit.upper()
    return {"K": 1e3, "M": 1e6, "B": 1e9}.get(u, 1.0)


def parse_unit_amount(num: str, unit: str) -> Optional[float]:
    try:
        x = float(num.replace(",", ""))
    except ValueError:
        return None
    return x * unit_to_mult(unit)


def extract_asof_date(text: str) -> Optional[dt.date]:
    """
    Prefer 'Fund Net Asset Value as of <Mon> <DD> <YYYY>' date.
    Falls back to other 'As of ...' formats if present.
    """
    t = re.sub(r"\s+", " ", text)

    # Most specific: "Fund Net Asset Value as of Jan 15 2026"
    m = re.search(
        r"Fund Net Asset Value as of\s+([A-Za-z]{3}\s+\d{1,2}\s+\d{4})",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        try:
            return dt.datetime.strptime(m.group(1), "%b %d %Y").date()
        except ValueError:
            pass

    # Generic: "As of Jan 15, 2026" or "As of Jan 15 2026"
    m = re.search(
        r"As of\s+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        raw = m.group(1).replace(",", "")
        for fmt in ("%b %d %Y", "%B %d %Y"):
            try:
                return dt.datetime.strptime(raw, fmt).date()
            except ValueError:
                pass

    # Numeric: "As of 01/15/2026"
    m = re.search(r"As of\s+(\d{1,2}/\d{1,2}/\d{4})", t, flags=re.IGNORECASE)
    if m:
        try:
            return dt.datetime.strptime(m.group(1), "%m/%d/%Y").date()
        except ValueError:
            pass

    return None


def extract_labeled_amount(text: str, label: str) -> Optional[float]:
    """
    Extract an amount like:
      "<label>  $93,602.55 M"
      "<label>  643.66 M"
    """
    t = re.sub(r"\s+", " ", text)
    m = re.search(rf"{re.escape(label)}\s+{AMT_RE.pattern}", t, flags=re.IGNORECASE)
    if not m:
        return None
    # groups: (num, unit) because AMT_RE pattern is embedded
    num = m.group(1)
    unit = m.group(2)
    return parse_unit_amount(num, unit)


def extract_nav(text: str) -> Optional[float]:
    """
    Try to extract NAV value: "... NAV ... $145.42"
    """
    t = re.sub(r"\s+", " ", text)
    m = re.search(r"\bNAV\b.{0,200}?\$\s*([\d,]+(?:\.\d+)?)", t, flags=re.IGNORECASE)
    if not m:
        # fallback: "Net Asset Value ... $145.42"
        m = re.search(r"Net Asset Value.{0,200}?\$\s*([\d,]+(?:\.\d+)?)", t, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def dig_next_data(soup: BeautifulSoup) -> Optional[dict]:
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            return json.loads(tag.string)
        except Exception:
            return None
    return None


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

    # Prefer stable extraction from human-readable text (SSGA has the labeled blocks)
    asof = extract_asof_date(text) or dt.datetime.utcnow().date()
    nav = extract_nav(text)

    # These are the key fixes for your None issue:
    shares = extract_labeled_amount(text, "Shares Outstanding")
    aum = extract_labeled_amount(text, "Assets Under Management") or extract_labeled_amount(text, "Net Assets")

    # Optional: include NEXT_DATA existence for debugging
    nd = dig_next_data(soup)

    raw_payload: Dict[str, Any] = {
        "nav": nav,
        "aum": aum,
        "shares": shares,
        "next_data_present": bool(nd),
        "url": url,
    }

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
    # import dotenv
    # dotenv.load_dotenv()

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
            print(
                f"[OK] {t} asof={snap.asof_date} "
                f"nav={snap.nav} aum={snap.aum} shares={snap.shares_outstanding}"
            )
        except Exception as e:
            print(f"[ERR] {t} {e}")

    if missing:
        print("[WARN] SOURCE_URLS missing tickers:", ",".join(missing))


if __name__ == "__main__":
    main()