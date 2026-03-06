"""
scanner.py — BIP84 (Native SegWit / zpub) address derivation and Electrs querying.

For each wallet:
  1. Derive receiving (chain=0) and change (chain=1) addresses using embit.
  2. Query Electrs REST API for transaction history per address.
  3. Compute the net value_sats each transaction contributed to that address.
  4. Store addresses and transactions in the database.

Gap limit: stop deriving new addresses once 20 consecutive addresses have no
transactions.
"""

import os
import logging
import requests
from embit import bip32, script
from embit.networks import NETWORKS

import db

log = logging.getLogger(__name__)

ELECTRS_HOST = os.environ.get("ELECTRS_HOST", "127.0.0.1")
ELECTRS_PORT = os.environ.get("ELECTRS_PORT", "3000")
ELECTRS_BASE = f"http://{ELECTRS_HOST}:{ELECTRS_PORT}"

GAP_LIMIT = 20
REQUEST_TIMEOUT = 30  # seconds


def _electrs_get(path):
    """GET from Electrs REST API; return parsed JSON or raise."""
    url = f"{ELECTRS_BASE}{path}"
    resp = requests.get(url, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def _get_all_txs(address):
    """
    Retrieve all confirmed transactions for an address from Electrs,
    handling the 25-per-page pagination of the /txs/chain endpoint.
    Returns a list of raw tx dicts.
    """
    txs = []
    last_txid = None
    while True:
        path = f"/address/{address}/txs/chain"
        if last_txid:
            path += f"/{last_txid}"
        try:
            page = _electrs_get(path)
        except requests.HTTPError as exc:
            log.warning("Electrs HTTP error for %s: %s", address, exc)
            break
        if not page:
            break
        txs.extend(page)
        if len(page) < 25:
            break
        last_txid = page[-1]["txid"]
    return txs


def _net_value_for_address(tx, address):
    """
    Compute the net satoshi value of a transaction for a specific address.
    Positive = received, negative = sent.
    """
    received = sum(
        out["value"]
        for out in tx.get("vout", [])
        if out.get("scriptpubkey_address") == address
    )
    spent = sum(
        inp["prevout"]["value"]
        for inp in tx.get("vin", [])
        if inp.get("prevout", {}).get("scriptpubkey_address") == address
    )
    return received - spent


def _derive_address(root_key, chain, idx):
    """Derive a single BIP84 bc1q... address for the given chain and index."""
    child = root_key.derive([chain, idx])
    return script.p2wpkh(child).address(NETWORKS["main"])


def scan_wallet(wallet_id, xpub):
    """
    Full scan of a wallet: derive addresses, fetch transactions, store results.
    Called during initial setup and daily sync.
    """
    log.info("Scanning wallet %s (xpub: %s...)", wallet_id, xpub[:20])

    try:
        root_key = bip32.HDKey.from_base58(xpub)
    except Exception as exc:
        log.error("Invalid xpub for wallet %s: %s", wallet_id, exc)
        return

    with db.get_conn() as conn:
        for chain in (0, 1):  # 0=receiving, 1=change
            gap = 0
            idx = 0
            while gap < GAP_LIMIT:
                address = _derive_address(root_key, chain, idx)
                addr_id = db.upsert_address(conn, wallet_id, address, chain, idx)

                txs = _get_all_txs(address)
                if not txs:
                    gap += 1
                else:
                    gap = 0
                    for tx in txs:
                        status = tx.get("status", {})
                        block_height = status.get("block_height")
                        block_time = status.get("block_time")
                        value_sats = _net_value_for_address(tx, address)
                        db.upsert_transaction(
                            conn,
                            addr_id,
                            tx["txid"],
                            block_height,
                            block_time,
                            value_sats,
                        )
                    log.debug(
                        "Wallet %s chain=%d idx=%d address=%s txs=%d",
                        wallet_id, chain, idx, address, len(txs)
                    )

                idx += 1

        db.touch_wallet_scanned(conn, wallet_id)
    log.info("Wallet %s scan complete.", wallet_id)


def scan_all_wallets():
    """Scan every wallet stored in the database."""
    with db.get_conn() as conn:
        wallets = db.get_wallets(conn)
    for w in wallets:
        try:
            scan_wallet(w["id"], w["xpub"])
        except Exception as exc:
            log.exception("Error scanning wallet %s: %s", w["id"], exc)
