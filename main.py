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


TICKERS = [
    "XLC","XLY","XLP","XLE","XLF","XLV","XLI","XLB","XLK","XLU","XLRE"
]

# TODO: 여기 URL 맵은 네가 실제로 긁을 “공식 소스”로 바꿔줘.
# (예: 발행사/ETF 페이지 URL을 ticker별로 매핑)
SOURCE_URLS = {
    "XLC": "https://www.ssga.com/us/en/individual/etfs/state-street-communication-services-select-sector-spdr-etf-xlc",
    "XLY": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-discretionary-select-sector-spdr-etf-xly",
    "XLP": "https://www.ssga.com/us/en/individual/etfs/state-street-consumer-staples-select-sector-spdr-etf-xlp",
    "XLE": "https://www.ssga.com/us/en/individual/etfs/state-street-energy-select-sector-spdr-etf-xle",
    "XLV": "https://www.ssga.com/us/en/individual/etfs/state-street-healthcare-select-sector-spdr-etf-xlv",
    "XLI": "https://www.ssga.com/us/en/individual/etfs/state-street-industrial-select-sector-spdr-etf-xli",
    "XLB": "https://www.ssga.com/us/en/individual/etfs/state-street-materials-select-sector-spdr-etf-xlb",
    "XLK": "https://www.ssga.com/us/en/individual/etfs/funds/technology-select-sector-spdr-fund-xlk",
    "XLU": "https://www.ssga.com/us/en/individual/etfs/state-street-utilities-select-sector-spdr-etf-xl",
    "XLRE": "https://www.ssga.com/us/en/individual/etfs/state-street-real-estate-select-sector-spdr-etf-xlre",
    "XLF": "https://www.ssga.com/us/en/individual/etfs/funds/financial-select-sector-spdr-fund-xlf",
    # 나머지도 같은 패턴으로 추가
}

USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"

NUM_RE = re.compile(r"([-+]?\$?\s?\d[\d,]*\.?\d*)")

def parse_number(s: str) -> Optional[float]:
    if not s:
        return None
    m = NUM_RE.search(s)
    if not m:
        return None
    raw = m.group(1).replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except ValueError:
        return None

def find_value_near_label(text: str, label_patterns: Tuple[str, ...]) -> Optional[float]:
    """
    label 근처에 나오는 숫자를 뽑는 간단한 휴리스틱.
    """
    lower = text.lower()
    for pat in label_patterns:
        idx = lower.find(pat.lower())
        if idx >= 0:
            window = text[idx: idx + 400]  # label 이후 근처 범위
            val = parse_number(window)
            if val is not None:
                return val
    return None

@dataclass
class EtfSnapshot:
    nav: Optional[float]
    aum: Optional[float]
    shares_outstanding: Optional[float]
    source_url: str
    raw_excerpt: Dict[str, Any]

def fetch_snapshot(url: str) -> EtfSnapshot:
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)

    # 키워드 후보들 (사이트마다 표현이 조금 다름)
    nav = find_value_near_label(text, ("NAV", "Net Asset Value"))
    aum = find_value_near_label(text, ("Net Assets", "AUM", "Total Net Assets"))
    shares = find_value_near_label(text, ("Shares Outstanding", "Shares out", "Shares Outstanding (M)"))

    raw_excerpt = {
        "nav_guess": nav,
        "aum_guess": aum,
        "shares_guess": shares,
    }
    return EtfSnapshot(nav=nav, aum=aum, shares_outstanding=shares, source_url=url, raw_excerpt=raw_excerpt)

def supabase_client() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_KEY"]
    return create_client(url, key)

def upsert_daily(sb: Client, asof_date: dt.date, ticker: str, snap: EtfSnapshot) -> None:
    payload = {
        "asof_date": asof_date.isoformat(),
        "ticker": ticker,
        "nav": snap.nav,
        "aum": snap.aum,
        "shares_outstanding": snap.shares_outstanding,
        "source_url": snap.source_url,
        "raw_payload": snap.raw_excerpt,
    }
    sb.table("sector_etf_daily").upsert(payload).execute()

def main():
    sb = supabase_client()
    asof_date = dt.date.today()

    missing = []
    for t in TICKERS:
        url = SOURCE_URLS.get(t)
        if not url:
            missing.append(t)
            continue

        try:
            snap = fetch_snapshot(url)
            upsert_daily(sb, asof_date, t, snap)
            print(f"[OK] {t} nav={snap.nav} aum={snap.aum} shares={snap.shares_outstanding}")
        except Exception as e:
            print(f"[ERR] {t} {e}")

    if missing:
        print("[WARN] SOURCE_URLS missing tickers:", ",".join(missing))

if __name__ == "__main__":
    main()