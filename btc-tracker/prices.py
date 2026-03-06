"""
prices.py — Fetch BTC daily price history from CoinGecko and store in DB.

Fetches both USD and CAD prices so the user can switch display currency.
On first run (or when the table is nearly empty) we fetch the full history.
Subsequent daily calls only fetch the most recent few days.
CoinGecko free tier requires a demo API key (free, no credit card).
Set the COINGECKO_API_KEY environment variable with your key.
"""

import logging
import os
import time
from datetime import date

import requests

import db

log = logging.getLogger(__name__)

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")
REQUEST_TIMEOUT = 30


def _fetch_history(vs_currency, days="max"):
    """
    Fetch BTC price history from CoinGecko for a given fiat currency.
    Returns a dict of {date_str: price}.

    Note: CoinGecko auto-selects granularity based on 'days':
      1 day = 5-min, 2-90 days = hourly, >90 days = daily.
    The 'interval' parameter is NOT used (removed from free/demo tier).
    """
    url = (
        f"{COINGECKO_BASE}/coins/bitcoin/market_chart"
        f"?vs_currency={vs_currency}&days={days}"
    )
    headers = {}
    if COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = COINGECKO_API_KEY
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    results = {}
    for ts_ms, price in data.get("prices", []):
        d = date.fromtimestamp(ts_ms / 1000).isoformat()
        results[d] = round(price, 2)
    return results


def backfill_prices():
    """
    Called on startup and during the daily sync.
    If we have fewer than 30 days of price data, fetch the full history.
    Otherwise, fetch only the last 3 days (to fill any gap and update today).

    Fetches both USD and CAD prices from CoinGecko.
    """
    with db.get_conn() as conn:
        prices = db.get_prices(conn)

    is_full = len(prices) < 30
    # CoinGecko free/demo tier does not support days=max.
    # 365 days covers over a year of history, which is enough for most wallets.
    days = 365 if is_full else 3
    label = "full" if is_full else "recent"

    # Fetch USD prices
    try:
        usd_prices = _fetch_history("usd", days)
    except Exception as exc:
        log.error("Failed to fetch %s USD prices: %s", label, exc)
        return

    # Brief pause to respect rate limits
    time.sleep(2)

    # Fetch CAD prices
    try:
        cad_prices = _fetch_history("cad", days)
    except Exception as exc:
        log.warning("Failed to fetch %s CAD prices: %s (USD saved anyway)", label, exc)
        cad_prices = {}

    # Merge and store
    all_dates = set(usd_prices.keys()) | set(cad_prices.keys())

    with db.get_conn() as conn:
        for d_str in sorted(all_dates):
            usd = usd_prices.get(d_str)
            cad = cad_prices.get(d_str)
            if usd is not None:
                db.upsert_price(conn, d_str, usd, cad)
            elif cad is not None:
                # Rare edge case: have CAD but not USD for a date
                # Skip — we require USD as the base
                pass

    log.info("Price data updated (%s): %d dates stored.", label, len(all_dates))
