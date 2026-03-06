"""
balances.py — Reconstruct daily BTC balances from on-chain transaction history.

Algorithm for each wallet:
  1. Fetch all stored transactions sorted by block_time ascending.
  2. Walk through them in order, accumulating a running balance.
  3. Bucket each day (UTC) with the balance at end of that day.
  4. Forward-fill missing days so every date from the first tx to today has a row.
  5. Upsert into daily_balances table (overwrite to pick up any new txs).
"""

import logging
from datetime import date, timedelta

import db

log = logging.getLogger(__name__)


def _unix_to_date(ts):
    """Convert a Unix timestamp (int) to a 'YYYY-MM-DD' UTC string."""
    from datetime import datetime, timezone
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def rebuild_balances_for_wallet(wallet_id):
    """Recompute and store daily_balances for a single wallet."""
    log.info("Rebuilding daily balances for wallet %s", wallet_id)
    with db.get_conn() as conn:
        txs = db.get_transactions_for_wallet(conn, wallet_id)

    if not txs:
        log.info("Wallet %s has no transactions; skipping.", wallet_id)
        return

    # Group net sats by UTC date
    # Only include confirmed transactions (block_time not None)
    day_delta = {}
    for tx in txs:
        if not tx["block_time"]:
            continue
        d = _unix_to_date(tx["block_time"])
        day_delta[d] = day_delta.get(d, 0) + tx["value_sats"]

    if not day_delta:
        return

    # Build a sorted list of all dates from first tx to today
    all_dates = sorted(day_delta.keys())
    start = date.fromisoformat(all_dates[0])
    end = date.today()

    running_balance = 0
    current = start
    rows = []
    while current <= end:
        d_str = current.isoformat()
        running_balance += day_delta.get(d_str, 0)
        # Clamp to zero (shouldn't go negative but protects against edge cases)
        running_balance = max(0, running_balance)
        rows.append((d_str, running_balance))
        current += timedelta(days=1)

    with db.get_conn() as conn:
        for d_str, bal in rows:
            db.upsert_daily_balance(conn, wallet_id, d_str, bal)

    log.info(
        "Wallet %s: %d daily balance rows written (%s → %s)",
        wallet_id, len(rows), rows[0][0], rows[-1][0]
    )


def rebuild_all_balances():
    """Rebuild daily balances for every wallet."""
    with db.get_conn() as conn:
        wallets = db.get_wallets(conn)
    for w in wallets:
        try:
            rebuild_balances_for_wallet(w["id"])
        except Exception as exc:
            log.exception("Error rebuilding balances for wallet %s: %s", w["id"], exc)
