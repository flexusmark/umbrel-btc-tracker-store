"""
prices.py — Fetch BTC/USD daily price history from CoinGecko and store in DB.

On first run (or when the table is empty) we fetch the full history in one
API call.  Subsequent daily calls only fetch the most recent few days.
CoinGecko free tier: no API key needed, ~30 req/min rate limit.
"""

import logging
from datetime import date, timedelta

import requests

import db

log = logging.getLogger(__name__)

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
REQUEST_TIMEOUT = 30


def _fetch_full_history():
    """
    Fetch the complete BTC/USD daily price history from CoinGecko.
    Returns a list of (date_str, price) tuples.
    """
    url = (
        f"{COINGECKO_BASE}/coins/bitcoin/market_chart"
        "?vs_currency=usd&days=max&interval=daily"
    )
    log.info("Fetching full BTC price history from CoinGecko…")
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    results = []
    for ts_ms, price in data.get("prices", []):
        # CoinGecko returns ms timestamps at midnight UTC for daily data
        d = date.fromtimestamp(ts_ms / 1000).isoformat()
        results.append((d, price))
    return results


def _fetch_recent_prices(days=3):
    """
    Fetch the last N days of BTC/USD price data from CoinGecko.
    Returns a list of (date_str, price) tuples.
    """
    url = (
        f"{COINGECKO_BASE}/coins/bitcoin/market_chart"
        f"?vs_currency=usd&days={days}&interval=daily"
    )
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    results = []
    for ts_ms, price in data.get("prices", []):
        d = date.fromtimestamp(ts_ms / 1000).isoformat()
        results.append((d, price))
    return results


def backfill_prices():
    """
    Called on startup and during the daily sync.
    If we have fewer than 30 days of price data, fetch the full history.
    Otherwise, fetch only the last 3 days (to fill any gap and update today).
    """
    with db.get_conn() as conn:
        prices = db.get_prices(conn)

    if len(prices) < 30:
        try:
            rows = _fetch_full_history()
        except Exception as exc:
            log.error("Failed to fetch full price history: %s", exc)
            return
    else:
        try:
            rows = _fetch_recent_prices(days=3)
        except Exception as exc:
            log.error("Failed to fetch recent prices: %s", exc)
            return

    with db.get_conn() as conn:
        for d_str, price in rows:
            db.upsert_price(conn, d_str, round(price, 2))

    log.info("Price data updated: %d rows stored.", len(rows))
