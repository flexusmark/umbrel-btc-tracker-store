"""
scheduler.py — APScheduler configuration for the daily sync job.

The job runs at 02:00 UTC every day and:
  1. Fetches the latest BTC prices from CoinGecko.
  2. Scans all wallets via Electrs to pick up new transactions.
  3. Rebuilds daily balance history for all wallets.
"""

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

log = logging.getLogger(__name__)

_scheduler = None


def _daily_job():
    # Import here to avoid circular imports at module load time
    import prices
    import scanner
    import balances

    log.info("Daily sync starting…")
    try:
        prices.backfill_prices()
    except Exception:
        log.exception("Price fetch failed during daily sync")
    try:
        scanner.scan_all_wallets()
    except Exception:
        log.exception("Wallet scan failed during daily sync")
    try:
        balances.rebuild_all_balances()
    except Exception:
        log.exception("Balance rebuild failed during daily sync")
    log.info("Daily sync complete.")


def start():
    global _scheduler
    _scheduler = BackgroundScheduler(timezone="UTC")
    _scheduler.add_job(
        _daily_job,
        trigger=CronTrigger(hour=2, minute=0),
        id="daily_sync",
        replace_existing=True,
    )
    _scheduler.start()
    log.info("Scheduler started. Daily sync at 02:00 UTC.")


def shutdown():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)


def run_now():
    """Trigger the sync job immediately (called by the manual sync button)."""
    _daily_job()
