"""
db.py — SQLite schema initialization and query helpers.
Database file lives at /data/btc_tracker.db inside the container,
which maps to a persistent Docker volume on the host.
"""

import sqlite3
import os

DB_PATH = os.environ.get("DB_PATH", "/data/btc_tracker.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    with get_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wallets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                label       TEXT NOT NULL,
                xpub        TEXT NOT NULL UNIQUE,
                created_at  TEXT NOT NULL DEFAULT (datetime('now')),
                last_scanned_at TEXT
            );

            CREATE TABLE IF NOT EXISTS addresses (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id   INTEGER NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
                address     TEXT NOT NULL UNIQUE,
                chain       INTEGER NOT NULL,   -- 0=external/receiving, 1=internal/change
                idx         INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                address_id  INTEGER NOT NULL REFERENCES addresses(id) ON DELETE CASCADE,
                txid        TEXT NOT NULL,
                block_height INTEGER,
                block_time  INTEGER,            -- Unix timestamp
                value_sats  INTEGER NOT NULL    -- net sats: positive=received, negative=sent
            );

            CREATE TABLE IF NOT EXISTS daily_balances (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                wallet_id   INTEGER NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
                date        TEXT NOT NULL,      -- 'YYYY-MM-DD' UTC
                balance_sats INTEGER NOT NULL,
                UNIQUE(wallet_id, date)
            );

            CREATE TABLE IF NOT EXISTS btc_prices (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        TEXT NOT NULL UNIQUE,  -- 'YYYY-MM-DD' UTC
                usd_price   REAL NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_tx_address ON transactions(address_id);
            CREATE INDEX IF NOT EXISTS idx_daily_wallet_date ON daily_balances(wallet_id, date);
        """)


# --- Wallet helpers ---

def get_wallets(conn):
    return conn.execute("SELECT * FROM wallets ORDER BY label").fetchall()


def get_wallet(conn, wallet_id):
    return conn.execute("SELECT * FROM wallets WHERE id=?", (wallet_id,)).fetchone()


def add_wallet(conn, label, xpub):
    conn.execute(
        "INSERT INTO wallets (label, xpub) VALUES (?, ?)",
        (label.strip(), xpub.strip())
    )


def delete_wallet(conn, wallet_id):
    conn.execute("DELETE FROM wallets WHERE id=?", (wallet_id,))


def touch_wallet_scanned(conn, wallet_id):
    conn.execute(
        "UPDATE wallets SET last_scanned_at=datetime('now') WHERE id=?",
        (wallet_id,)
    )


# --- Address helpers ---

def upsert_address(conn, wallet_id, address, chain, idx):
    """Insert address if not already stored; return its id."""
    existing = conn.execute(
        "SELECT id FROM addresses WHERE address=?", (address,)
    ).fetchone()
    if existing:
        return existing["id"]
    cur = conn.execute(
        "INSERT INTO addresses (wallet_id, address, chain, idx) VALUES (?,?,?,?)",
        (wallet_id, address, chain, idx)
    )
    return cur.lastrowid


def get_addresses_for_wallet(conn, wallet_id):
    return conn.execute(
        "SELECT * FROM addresses WHERE wallet_id=? ORDER BY chain, idx",
        (wallet_id,)
    ).fetchall()


def get_all_addresses_set(conn, wallet_id):
    rows = conn.execute(
        "SELECT address FROM addresses WHERE wallet_id=?", (wallet_id,)
    ).fetchall()
    return {r["address"] for r in rows}


# --- Transaction helpers ---

def upsert_transaction(conn, address_id, txid, block_height, block_time, value_sats):
    existing = conn.execute(
        "SELECT id FROM transactions WHERE address_id=? AND txid=?",
        (address_id, txid)
    ).fetchone()
    if existing:
        return
    conn.execute(
        "INSERT INTO transactions (address_id, txid, block_height, block_time, value_sats) "
        "VALUES (?,?,?,?,?)",
        (address_id, txid, block_height, block_time, value_sats)
    )


def get_transactions_for_wallet(conn, wallet_id):
    """Return all transactions for a wallet, joined with address info."""
    return conn.execute("""
        SELECT t.txid, t.block_height, t.block_time, t.value_sats,
               a.address, a.chain, a.idx
        FROM transactions t
        JOIN addresses a ON a.id = t.address_id
        WHERE a.wallet_id = ?
        ORDER BY t.block_time ASC
    """, (wallet_id,)).fetchall()


# --- Daily balance helpers ---

def upsert_daily_balance(conn, wallet_id, date_str, balance_sats):
    conn.execute(
        "INSERT INTO daily_balances (wallet_id, date, balance_sats) VALUES (?,?,?) "
        "ON CONFLICT(wallet_id, date) DO UPDATE SET balance_sats=excluded.balance_sats",
        (wallet_id, date_str, balance_sats)
    )


def get_daily_balances_for_wallet(conn, wallet_id):
    return conn.execute(
        "SELECT date, balance_sats FROM daily_balances WHERE wallet_id=? ORDER BY date",
        (wallet_id,)
    ).fetchall()


def get_current_balance_sats(conn, wallet_id):
    """Most recent known balance for this wallet."""
    row = conn.execute(
        "SELECT balance_sats FROM daily_balances WHERE wallet_id=? ORDER BY date DESC LIMIT 1",
        (wallet_id,)
    ).fetchone()
    return row["balance_sats"] if row else 0


# --- Price helpers ---

def upsert_price(conn, date_str, usd_price):
    conn.execute(
        "INSERT INTO btc_prices (date, usd_price) VALUES (?,?) "
        "ON CONFLICT(date) DO UPDATE SET usd_price=excluded.usd_price",
        (date_str, usd_price)
    )


def get_prices(conn):
    return conn.execute(
        "SELECT date, usd_price FROM btc_prices ORDER BY date"
    ).fetchall()


def get_latest_price(conn):
    row = conn.execute(
        "SELECT usd_price FROM btc_prices ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return row["usd_price"] if row else None


# --- Portfolio chart data ---

def get_portfolio_chart_data(conn):
    """
    Return a list of {date, total_sats, usd_price, total_usd} dicts,
    one per day where we have both balance and price data.
    """
    rows = conn.execute("""
        SELECT db.date,
               SUM(db.balance_sats) AS total_sats,
               p.usd_price
        FROM daily_balances db
        JOIN btc_prices p ON p.date = db.date
        GROUP BY db.date
        ORDER BY db.date
    """).fetchall()
    result = []
    for r in rows:
        total_usd = round((r["total_sats"] / 1e8) * r["usd_price"], 2)
        result.append({
            "date": r["date"],
            "total_sats": r["total_sats"],
            "usd_price": r["usd_price"],
            "total_usd": total_usd,
        })
    return result


def get_per_wallet_chart_data(conn):
    """Return dict of wallet_id -> list of {date, balance_sats}."""
    rows = conn.execute(
        "SELECT wallet_id, date, balance_sats FROM daily_balances ORDER BY date"
    ).fetchall()
    data = {}
    for r in rows:
        data.setdefault(r["wallet_id"], []).append({
            "date": r["date"],
            "balance_sats": r["balance_sats"],
        })
    return data
