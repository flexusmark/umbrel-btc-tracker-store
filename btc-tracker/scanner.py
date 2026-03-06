"""
scanner.py — BIP84 (Native SegWit / zpub) address derivation and Electrs querying.

Uses the Electrum TCP protocol (JSON-RPC over TCP, port 50001) to communicate
with the local Electrs server on Umbrel, since romanz/electrs does NOT provide
an Esplora REST API.

For each wallet:
  1. Derive receiving (chain=0) and change (chain=1) addresses using embit.
  2. Compute the Electrum-style scripthash for each address.
  3. Query Electrs via TCP for transaction history per scripthash.
  4. Parse raw transactions with embit to compute net sats per address.
  5. Store addresses and transactions in the database.

Gap limit: stop deriving new addresses once 20 consecutive addresses have no
transactions.
"""

import os
import json
import socket
import struct
import hashlib
import logging

from embit import bip32, script
from embit.networks import NETWORKS
from embit.transaction import Transaction

import db

log = logging.getLogger(__name__)

ELECTRS_HOST = os.environ.get("ELECTRS_HOST", "127.0.0.1")
ELECTRS_PORT = int(os.environ.get("ELECTRS_PORT", "50001"))

GAP_LIMIT = 20
SOCKET_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# Electrum TCP client
# ---------------------------------------------------------------------------

class ElectrumClient:
    """Simple Electrum JSON-RPC client over TCP (newline-delimited JSON)."""

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self._sock = None
        self._buffer = b""
        self._id = 0
        self._tx_cache = {}      # txid (hex str) -> raw hex str
        self._header_cache = {}  # block height -> timestamp (int)

    def connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.settimeout(SOCKET_TIMEOUT)
        self._sock.connect((self.host, self.port))
        self._buffer = b""
        log.info("Connected to Electrum server at %s:%d", self.host, self.port)

    def close(self):
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def call(self, method, params=None):
        """Send a JSON-RPC request and return the result."""
        if self._sock is None:
            self.connect()

        self._id += 1
        req = {
            "id": self._id,
            "jsonrpc": "2.0",
            "method": method,
            "params": params or [],
        }
        self._sock.sendall((json.dumps(req) + "\n").encode())

        # Read until we get a full line (newline-delimited response)
        while b"\n" not in self._buffer:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("Electrum server closed connection")
            self._buffer += chunk

        line, self._buffer = self._buffer.split(b"\n", 1)
        resp = json.loads(line)

        if "error" in resp and resp["error"]:
            raise Exception(f"Electrum error: {resp['error']}")

        return resp.get("result")

    def get_history(self, scripthash):
        """Get transaction history for a scripthash.
        Returns list of {"tx_hash": str, "height": int}."""
        return self.call("blockchain.scripthash.get_history", [scripthash])

    def get_raw_tx(self, txid):
        """Get raw transaction hex, with caching."""
        if txid in self._tx_cache:
            return self._tx_cache[txid]
        raw = self.call("blockchain.transaction.get", [txid])
        self._tx_cache[txid] = raw
        return raw

    def get_block_time(self, height):
        """Get block timestamp for a given height, with caching."""
        if height in self._header_cache:
            return self._header_cache[height]
        header_hex = self.call("blockchain.block.header", [height])
        # Timestamp is at byte offset 68 in the 80-byte block header
        header_bytes = bytes.fromhex(header_hex)
        timestamp = struct.unpack("<I", header_bytes[68:72])[0]
        self._header_cache[height] = timestamp
        return timestamp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scripthash(addr_script):
    """
    Compute the Electrum-style scripthash from a Script object.
    = SHA256(raw_scriptPubKey), bytes reversed, hex-encoded.
    """
    raw = addr_script.data
    return hashlib.sha256(raw).digest()[::-1].hex()


def _derive_address_and_script(root_key, chain, idx):
    """Derive a BIP84 bc1q... address and its Script (scriptPubKey) object."""
    child = root_key.derive([chain, idx])
    sc = script.p2wpkh(child)
    address = sc.address(NETWORKS["main"])
    return address, sc


def _net_value_for_address(client, raw_hex, addr_script_bytes):
    """
    Compute the net satoshi value of a raw transaction for a specific address.
    Positive = received, negative = sent.

    Parses the raw transaction with embit and compares output/input
    scriptPubKeys against the target address's scriptPubKey bytes.
    """
    tx = Transaction.parse(bytes.fromhex(raw_hex))

    # Sum outputs paying to our address
    received = 0
    for vout in tx.vout:
        if vout.script_pubkey.data == addr_script_bytes:
            received += vout.value

    # Sum inputs spending from our address
    spent = 0
    for vin in tx.vin:
        # Coinbase inputs have txid = 32 zero bytes
        if vin.txid == b'\x00' * 32:
            continue
        # embit stores txid in display order (already reversed)
        prev_txid = vin.txid.hex()
        prev_raw = client.get_raw_tx(prev_txid)
        prev_tx = Transaction.parse(bytes.fromhex(prev_raw))
        prev_output = prev_tx.vout[vin.vout]
        if prev_output.script_pubkey.data == addr_script_bytes:
            spent += prev_output.value

    return received - spent


# ---------------------------------------------------------------------------
# Wallet scanning
# ---------------------------------------------------------------------------

def scan_wallet(wallet_id, xpub):
    """
    Full scan of a wallet: derive addresses, fetch transactions, store results.
    Uses the Electrum TCP protocol to communicate with Electrs.
    """
    log.info("Scanning wallet %s (xpub: %s...)", wallet_id, xpub[:20])

    try:
        root_key = bip32.HDKey.from_base58(xpub)
    except Exception as exc:
        log.error("Invalid xpub for wallet %s: %s", wallet_id, exc)
        return

    client = ElectrumClient(ELECTRS_HOST, ELECTRS_PORT)

    try:
        client.connect()

        with db.get_conn() as conn:
            for chain in (0, 1):  # 0=receiving, 1=change
                gap = 0
                idx = 0
                while gap < GAP_LIMIT:
                    address, addr_sc = _derive_address_and_script(
                        root_key, chain, idx
                    )
                    addr_script_bytes = addr_sc.data
                    sh = _scripthash(addr_sc)

                    addr_id = db.upsert_address(
                        conn, wallet_id, address, chain, idx
                    )

                    try:
                        history = client.get_history(sh)
                    except Exception as exc:
                        log.warning(
                            "Electrum error for %s: %s", address, exc
                        )
                        history = []

                    if not history:
                        gap += 1
                    else:
                        gap = 0
                        for item in history:
                            tx_hash = item["tx_hash"]
                            height = item.get("height", 0)

                            # Get block timestamp
                            block_time = None
                            if height and height > 0:
                                try:
                                    block_time = client.get_block_time(height)
                                except Exception:
                                    pass

                            # Parse raw tx and compute net value
                            try:
                                raw_hex = client.get_raw_tx(tx_hash)
                                value_sats = _net_value_for_address(
                                    client, raw_hex, addr_script_bytes
                                )
                            except Exception as exc:
                                log.warning(
                                    "Error processing tx %s: %s",
                                    tx_hash, exc,
                                )
                                continue

                            db.upsert_transaction(
                                conn,
                                addr_id,
                                tx_hash,
                                height if height > 0 else None,
                                block_time,
                                value_sats,
                            )

                        log.debug(
                            "Wallet %s chain=%d idx=%d address=%s txs=%d",
                            wallet_id, chain, idx, address, len(history),
                        )

                    idx += 1

            db.touch_wallet_scanned(conn, wallet_id)

        log.info("Wallet %s scan complete.", wallet_id)

    finally:
        client.close()


def scan_all_wallets():
    """Scan every wallet stored in the database."""
    with db.get_conn() as conn:
        wallets = db.get_wallets(conn)
    for w in wallets:
        try:
            scan_wallet(w["id"], w["xpub"])
        except Exception as exc:
            log.exception("Error scanning wallet %s: %s", w["id"], exc)
