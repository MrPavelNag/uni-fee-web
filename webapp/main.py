#!/usr/bin/env python3
"""
Cloud-ready web MVP for pool analysis.

- Run existing v3/v4 agents in background jobs
- Collect JSON outputs and return merged data for UI
- Show results on-screen (no PDF required)
"""

from __future__ import annotations

import os
import base64
import hashlib
import json
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, getcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.request import Request as UrlRequest, urlopen

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel, Field
from eth_account import Account
from eth_account.messages import encode_defunct

from agent_common import load_chart_data_json, pairs_to_filename_suffix
from config import GOLDSKY_ENDPOINTS, TOKEN_ADDRESSES, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS
from uniswap_client import get_graph_endpoint, graphql_query

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def _resolve_catalog_dir() -> Path:
    # Highest priority: explicit path from env.
    explicit = os.environ.get("CATALOG_STORAGE_DIR", "").strip()
    if explicit:
        return Path(explicit).expanduser()

    # Render disk mount can be exposed under different env names.
    for env_name in ("RENDER_DISK_PATH", "RENDER_DISK_MOUNT_PATH", "RENDER_DISK_MOUNT"):
        mount = os.environ.get(env_name, "").strip()
        if mount:
            return Path(mount) / "uni_fee_cache"

    # Common Render persistent disk mount point.
    default_render_mount = Path("/var/data")
    if default_render_mount.exists():
        return default_render_mount / "uni_fee_cache"

    # Local/dev fallback.
    return DATA_DIR


CATALOG_DIR = _resolve_catalog_dir()
try:
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    # Fallback for platforms without mounted persistent disk (or no write access).
    CATALOG_DIR = DATA_DIR
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
TOKEN_CATALOG_PATH = CATALOG_DIR / "token_catalog.json"
CHAIN_CATALOG_PATH = CATALOG_DIR / "chain_catalog.json"
UNISWAP_TOKEN_LIST_URL = os.environ.get("UNISWAP_TOKEN_LIST_URL", "https://tokens.uniswap.org")
TOKENS_MIN_TVL_USD = float(os.environ.get("TOKENS_MIN_TVL_USD", "1000000"))
CHAIN_ID_TO_NAME = {
    1: "ethereum",
    10: "optimism",
    56: "bnb",
    130: "unichain",
    1301: "unichain",
    137: "polygon",
    324: "zksync",
    8453: "base",
    42161: "arbitrum-one",
    43114: "avalanche",
    81457: "blast",
}

APP_VERSION = "0.0.2"
app = FastAPI(title="Uni Fee Web", version=APP_VERSION)


@app.on_event("startup")
def _on_startup() -> None:
    _start_catalog_auto_refresh()
    _start_analytics()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_catalog_auto_refresh()
    _stop_analytics()

# Simple in-memory job storage (MVP)
JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()  # prevent collisions in shared data/*.json files
RUN_HISTORY: dict[str, list[dict[str, Any]]] = {}
RUN_HISTORY_LIMIT = 10
SESSION_COOKIE_NAME = "uni_fee_sid"
SESSION_TTL_SEC = int(os.environ.get("SESSION_TTL_SEC", str(30 * 24 * 60 * 60)))
CATALOG_REFRESH_INTERVAL_SEC = max(60, int(os.environ.get("CATALOG_REFRESH_INTERVAL_SEC", str(24 * 60 * 60))))
CATALOG_REFRESH_ON_STARTUP = os.environ.get("CATALOG_REFRESH_ON_STARTUP", "0").strip().lower() in ("1", "true", "yes", "on")
CATALOG_REFRESH_STOP = threading.Event()
CATALOG_REFRESH_THREAD: threading.Thread | None = None
AUTH_NONCE_TTL_SEC = int(os.environ.get("AUTH_NONCE_TTL_SEC", "300"))
AUTH_NONCES: dict[str, dict[str, Any]] = {}
AUTH_SESSIONS: dict[str, dict[str, Any]] = {}
AUTH_LOCK = threading.Lock()
ADMIN_WALLETS_DEFAULT = "0xD3155f6A7525F81595609E83789bbb95d91aaEdf"
ADMIN_WALLET_ADDRESSES_ENV = os.environ.get("ADMIN_WALLET_ADDRESSES", ADMIN_WALLETS_DEFAULT)
ADMIN_WALLETS_ENC_KEY = os.environ.get("ADMIN_WALLETS_ENC_KEY", "").strip()
ADMIN_WALLETS_STATE_KEY_PLAIN = "admin_wallets_csv"
ADMIN_WALLETS_STATE_KEY_ENC = "admin_wallets_csv_enc_v1"
ANALYTICS_DB_PATH = Path(os.environ.get("ANALYTICS_DB_PATH", str(CATALOG_DIR / "analytics.sqlite3")))
ANALYTICS_ENABLED = os.environ.get("ANALYTICS_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
WALLETCONNECT_PROJECT_ID = (
    os.environ.get("WALLETCONNECT_PROJECT_ID", "").strip()
    or os.environ.get("REOWN_PROJECT_ID", "").strip()
    or os.environ.get("NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID", "").strip()
)
AAVE_V3_GRAPHQL_ENDPOINT = os.environ.get("AAVE_V3_GRAPHQL_ENDPOINT", "https://api.v3.aave.com/graphql").strip()
CHAIN_ID_TO_KEY: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    56: "bsc",
    130: "unichain",
    1301: "unichain",
    137: "polygon",
    324: "zksync",
    8453: "base",
    42161: "arbitrum-one",
    43114: "avalanche",
    81457: "blast",
}
AAVE_CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    10: "optimism",
    56: "bsc",
    100: "gnosis",
    137: "polygon",
    146: "sonic",
    324: "zksync",
    1088: "metis",
    1868: "soneium",
    42220: "celo",
    43114: "avalanche",
    5000: "mantle",
    8453: "base",
    9745: "plasma",
    59144: "linea",
    42161: "arbitrum-one",
    534352: "scroll",
    57073: "ink",
}
_POSITION_SCHEMA_SUPPORT_CACHE: dict[str, bool] = {}
_POOL_LIQUIDITY_SCHEMA_SUPPORT_CACHE: dict[str, bool] = {}
_POSITION_LIQUIDITY_SCHEMA_SUPPORT_CACHE: dict[str, bool] = {}
POSITIONS_DEBUG_ERRORS = os.environ.get("POSITIONS_DEBUG_ERRORS", "0").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_MAX_PAGES_PER_QUERY = max(1, int(os.environ.get("POSITIONS_MAX_PAGES_PER_QUERY", "3")))
POSITIONS_MAX_QUERY_ATTEMPTS = max(12, int(os.environ.get("POSITIONS_MAX_QUERY_ATTEMPTS", "36")))
POSITIONS_SCAN_MAX_SECONDS = max(8, int(os.environ.get("POSITIONS_SCAN_MAX_SECONDS", "45")))
POSITIONS_OWNER_MAX_SECONDS = max(2, int(os.environ.get("POSITIONS_OWNER_MAX_SECONDS", "4")))
POSITIONS_PARALLEL_WORKERS = max(1, int(os.environ.get("POSITIONS_PARALLEL_WORKERS", "4")))
POSITIONS_ADDRESS_PARALLEL_WORKERS = max(1, int(os.environ.get("POSITIONS_ADDRESS_PARALLEL_WORKERS", "3")))
POSITIONS_EXTENDED_QUERY_FALLBACK = os.environ.get("POSITIONS_EXTENDED_QUERY_FALLBACK", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_TRY_BYTES_TYPE = os.environ.get("POSITIONS_TRY_BYTES_TYPE", "0").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_ONCHAIN_TIMEOUT_SEC = max(2, int(os.environ.get("POSITIONS_ONCHAIN_TIMEOUT_SEC", "4")))
POSITIONS_ONCHAIN_MAX_NFTS = max(1, int(os.environ.get("POSITIONS_ONCHAIN_MAX_NFTS", "120")))
POSITIONS_INFINITY_OWNER_LOOKBACK = max(200, int(os.environ.get("POSITIONS_INFINITY_OWNER_LOOKBACK", "800")))
POSITIONS_FILTER_SPAM_TOKENS = os.environ.get("POSITIONS_FILTER_SPAM_TOKENS", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_SPAM_MAX_TVL_USD = max(0.0, float(os.environ.get("POSITIONS_SPAM_MAX_TVL_USD", "50")))
POSITIONS_MAX_TOKEN_PRICE_USD = max(100.0, float(os.environ.get("POSITIONS_MAX_TOKEN_PRICE_USD", "1000000")))
POSITIONS_MIN_TOKEN_PRICE_USD = max(0.0, float(os.environ.get("POSITIONS_MIN_TOKEN_PRICE_USD", "0.000000000001")))
POSITIONS_MAX_TOKEN_PRICE_RATIO = max(10.0, float(os.environ.get("POSITIONS_MAX_TOKEN_PRICE_RATIO", "10000000000")))
POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS = {
    int(x.strip())
    for x in os.environ.get("POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS", "42161,8453,130,56").split(",")
    if x.strip().isdigit()
}
PRICE_CACHE_TTL_SEC = max(60, int(os.environ.get("PRICE_CACHE_TTL_SEC", "600")))
TOKEN_PRICE_CACHE: dict[tuple[int, str], tuple[float, float]] = {}
TOKEN_PRICE_CACHE_LOCK = threading.Lock()
TOKEN_SYMBOL_CACHE: dict[tuple[int, str], str] = {}
TOKEN_SYMBOL_CACHE_LOCK = threading.Lock()
MAJOR_ASSET_PRICE_CACHE: dict[str, tuple[float, float]] = {}
MAJOR_ASSET_PRICE_CACHE_TTL_SEC = max(60, int(os.environ.get("MAJOR_ASSET_PRICE_CACHE_TTL_SEC", "300")))
getcontext().prec = 48

UNISWAP_V3_NPM_BY_CHAIN_ID: dict[int, str] = {
    1: "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    10: "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    56: "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364",
    130: "0x943e6e07a7e8e791dafc44083e54041d743c46e9",
    1301: "0xb7f724d6dddfd008eff5cc2834edde5f9ef0d075",
    137: "0xc36442b4a4522e871399cd717abdd847ab11fe88",
    8453: "0x03a520b32c04bf3beef7beb72e919cf822ed34f1",
    42161: "0xc36442b4a4522e871399cd717abdd847ab11fe88",
}
UNISWAP_V3_FACTORY_BY_CHAIN_ID: dict[int, str] = {
    1: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    10: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    56: "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865",
    130: "0x1f98400000000000000000000000000000000003",
    1301: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    137: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    8453: "0x33128a8fc17869897dce68ed026d694621f6fdfd",
    42161: "0x1f98431c8ad98523631ae4a59f267346ea31f984",
}
V3_PROTOCOL_LABEL_BY_CHAIN_ID: dict[int, str] = {
    56: "pancake_v3",
}
PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID: dict[int, str] = {
    56: "0x556b9306565093c855aea9ae92a594704c2cd59e",
    1: "0x556b9306565093c855aea9ae92a594704c2cd59e",
    42161: "0x5e09acf80c0296740ec5d6f643005a4ef8daa694",
    8453: "0xc6a2db661d5a5690172d8eb0a7dea2d3008665a3",
}
PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID: dict[int, str] = {
    56: "0x55f4c8aba71a1e923edc303eb4feff14608cc226",
    8453: "0x55f4c8aba71a1e923edc303eb4feff14608cc226",
}
PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID: dict[int, str] = {
    56: "0x3d311d6283dd8ab90bb0031835c8e606349e2850",
    8453: "0x3d311d6283dd8ab90bb0031835c8e606349e2850",
}
DEFAULT_RPC_URLS_BY_CHAIN_ID: dict[int, list[str]] = {
    1: ["https://ethereum-rpc.publicnode.com"],
    10: ["https://optimism-rpc.publicnode.com"],
    56: ["https://bsc-dataseed.binance.org", "https://bsc-rpc.publicnode.com"],
    130: ["https://unichain-rpc.publicnode.com", "https://mainnet.unichain.org"],
    1301: ["https://sepolia.unichain.org"],
    137: ["https://polygon-bor-rpc.publicnode.com"],
    8453: ["https://base-rpc.publicnode.com"],
    42161: ["https://arbitrum-one-rpc.publicnode.com"],
}

_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN: dict[str, dict[str, str]] = {}
for _ck, _per_chain in TOKEN_ADDRESSES.items():
    addr_map: dict[str, str] = {}
    if isinstance(_per_chain, dict):
        for _sym, _addr in _per_chain.items():
            a = str(_addr or "").strip().lower()
            s = str(_sym or "").strip().upper()
            if not a or not s or not (a.startswith("0x") and len(a) == 42):
                continue
            # Prefer human-readable aliases for common addresses.
            if a in addr_map:
                prev = addr_map[a]
                if prev in {"WETH", "WMATIC"} and s in {"ETH", "MATIC", "POL"}:
                    addr_map[a] = s
                continue
            addr_map[a] = s
    _TOKEN_ADDR_TO_SYMBOL_BY_CHAIN[str(_ck).strip().lower()] = addr_map


def _analytics_conn() -> sqlite3.Connection:
    global ANALYTICS_DB_PATH
    try:
        ANALYTICS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(ANALYTICS_DB_PATH), timeout=10)
    except Exception:
        # Runtime-safe fallback to local writable path to prevent admin/help 500s.
        fallback = DATA_DIR / "analytics.sqlite3"
        fallback.parent.mkdir(parents=True, exist_ok=True)
        if ANALYTICS_DB_PATH != fallback and not fallback.exists() and ANALYTICS_DB_PATH.exists():
            try:
                shutil.copy2(str(ANALYTICS_DB_PATH), str(fallback))
            except Exception:
                pass
        ANALYTICS_DB_PATH = fallback
        conn = sqlite3.connect(str(ANALYTICS_DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _migrate_analytics_db_if_needed() -> None:
    if ANALYTICS_DB_PATH.exists():
        return
    legacy_candidates = [
        DATA_DIR / "analytics.sqlite3",
        BASE_DIR / "analytics.sqlite3",
    ]
    for legacy in legacy_candidates:
        if legacy.exists() and legacy.is_file():
            try:
                ANALYTICS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(legacy), str(ANALYTICS_DB_PATH))
                return
            except Exception:
                continue


def _init_analytics_db() -> None:
    if not ANALYTICS_ENABLED:
        return
    with _analytics_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              session_id TEXT NOT NULL,
              event_type TEXT NOT NULL,
              path TEXT NOT NULL DEFAULT '',
              payload TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_ts ON analytics_events(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_analytics_events_type ON analytics_events(event_type)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS analytics_state (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS help_tickets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              session_id TEXT NOT NULL,
              wallet_address TEXT NOT NULL DEFAULT '',
              name TEXT NOT NULL DEFAULT '',
              email TEXT NOT NULL DEFAULT '',
              subject TEXT NOT NULL DEFAULT '',
              message TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'open',
              admin_note TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_help_tickets_ts ON help_tickets(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_help_tickets_status ON help_tickets(status)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS help_ticket_messages (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ticket_id INTEGER NOT NULL,
              ts TEXT NOT NULL,
              author_type TEXT NOT NULL,
              message TEXT NOT NULL,
              session_id TEXT NOT NULL DEFAULT '',
              wallet_address TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_help_ticket_messages_ticket_id ON help_ticket_messages(ticket_id, id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS help_feedback (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              session_id TEXT NOT NULL,
              wallet_address TEXT NOT NULL DEFAULT '',
              name TEXT NOT NULL DEFAULT '',
              email TEXT NOT NULL DEFAULT '',
              subject TEXT NOT NULL DEFAULT '',
              message TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT 'new',
              reviewed_at TEXT NOT NULL DEFAULT '',
              reviewed_by TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_help_feedback_ts ON help_feedback(ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_help_feedback_status ON help_feedback(status)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS faq_items (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              question TEXT NOT NULL,
              answer TEXT NOT NULL,
              is_published INTEGER NOT NULL DEFAULT 1,
              is_featured INTEGER NOT NULL DEFAULT 0,
              sort_order INTEGER NOT NULL DEFAULT 100
            )
            """
        )
        cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(faq_items)").fetchall()}
        if "is_featured" not in cols:
            conn.execute("ALTER TABLE faq_items ADD COLUMN is_featured INTEGER NOT NULL DEFAULT 0")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_faq_items_pub ON faq_items(is_published)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_faq_items_feat ON faq_items(is_featured)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_faq_items_sort ON faq_items(sort_order, id)")
        conn.commit()


def _analytics_set_state(key: str, value: str) -> None:
    if not ANALYTICS_ENABLED:
        return
    with _analytics_conn() as conn:
        conn.execute(
            "INSERT INTO analytics_state(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        conn.commit()


def _analytics_get_state(key: str) -> str:
    if not ANALYTICS_ENABLED:
        return ""
    with _analytics_conn() as conn:
        row = conn.execute("SELECT value FROM analytics_state WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else ""


def _analytics_log_event(session_id: str, event_type: str, path: str = "", payload: str = "") -> None:
    if not ANALYTICS_ENABLED:
        return
    safe_sid = session_id if _is_valid_session_id(session_id) else "unknown"
    try:
        with _analytics_conn() as conn:
            conn.execute(
                "INSERT INTO analytics_events(ts, session_id, event_type, path, payload) VALUES(?, ?, ?, ?, ?)",
                (_iso_now(), safe_sid, event_type, (path or "")[:128], (payload or "")[:2000]),
            )
            conn.commit()
    except Exception:
        pass


def _analytics_bucket_expr(period: str) -> str:
    p = (period or "day").strip().lower()
    # ts is ISO-like text in UTC: 2026-03-15T17:00:00Z
    # For week bucketing convert into SQLite datetime-friendly form first.
    if p == "week":
        return "strftime('%Y-W%W', replace(substr(ts, 1, 19), 'T', ' '))"
    if p == "month":
        return "substr(ts, 1, 7)"
    if p == "year":
        return "substr(ts, 1, 4)"
    return "substr(ts, 1, 10)"


def _start_analytics() -> None:
    _migrate_analytics_db_if_needed()
    _init_analytics_db()


def _stop_analytics() -> None:
    return


def _public_base_url(request: Request) -> str:
    env_base = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if env_base:
        return env_base
    return f"{request.url.scheme}://{request.url.netloc}"


def _walletconnect_js_value() -> str:
    return WALLETCONNECT_PROJECT_ID.replace("\\", "\\\\").replace('"', '\\"')


def _short_addr(address: str) -> str:
    a = (address or "").strip()
    if len(a) < 12:
        return a
    return f"{a[:6]}...{a[-4:]}"


def _is_eth_address(value: str) -> bool:
    return bool(re.fullmatch(r"0x[a-fA-F0-9]{40}", (value or "").strip()))


def _new_nonce() -> str:
    return secrets.token_urlsafe(18)


def _siwe_message(*, domain: str, uri: str, address: str, chain_id: int, nonce: str, issued_at: str) -> str:
    return (
        f"{domain} wants you to sign in with your Ethereum account:\n"
        f"{address}\n\n"
        "Sign in to Uni Fee.\n\n"
        f"URI: {uri}\n"
        "Version: 1\n"
        f"Chain ID: {chain_id}\n"
        f"Nonce: {nonce}\n"
        f"Issued At: {issued_at}"
    )


def _is_admin_address(address: str) -> bool:
    return (address or "").strip().lower() in set(_admin_wallets_value())


def _require_admin(request: Request, response: Response) -> tuple[str, dict[str, Any]]:
    sid = _ensure_session_cookie(request, response)
    with AUTH_LOCK:
        auth = dict(AUTH_SESSIONS.get(sid, {}))
    if not auth or not _is_admin_address(str(auth.get("address") or "")):
        raise HTTPException(status_code=403, detail="Admin access required.")
    return sid, auth


def _require_authenticated_wallet(request: Request, response: Response) -> tuple[str, str, dict[str, Any]]:
    sid = _ensure_session_cookie(request, response)
    with AUTH_LOCK:
        auth = dict(AUTH_SESSIONS.get(sid, {}))
    wallet = str(auth.get("address") or "").strip().lower()
    if not auth or not _is_eth_address(wallet):
        raise HTTPException(status_code=403, detail="Wallet authorization required. Connect wallet first.")
    return sid, wallet, auth


def _parse_admin_wallets_csv(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for x in (raw or "").split(","):
        addr = x.strip().lower()
        if not _is_eth_address(addr):
            continue
        if addr in seen:
            continue
        seen.add(addr)
        out.append(addr)
    return out


def _default_admin_wallets() -> list[str]:
    items = _parse_admin_wallets_csv(ADMIN_WALLET_ADDRESSES_ENV)
    if items:
        return items
    return _parse_admin_wallets_csv(ADMIN_WALLETS_DEFAULT)


def _admin_wallets_fernet():
    key_raw = (ADMIN_WALLETS_ENC_KEY or "").strip()
    if not key_raw:
        return None
    try:
        from cryptography.fernet import Fernet
    except Exception:
        return None
    # Accept either a Fernet key directly or derive one from a passphrase.
    try:
        if len(key_raw) == 44:
            return Fernet(key_raw.encode("utf-8"))
    except Exception:
        pass
    derived = base64.urlsafe_b64encode(hashlib.sha256(key_raw.encode("utf-8")).digest())
    try:
        return Fernet(derived)
    except Exception:
        return None


def _encrypt_admin_wallets_csv(csv_value: str) -> str:
    fernet = _admin_wallets_fernet()
    if not fernet:
        return ""
    try:
        token = fernet.encrypt((csv_value or "").encode("utf-8"))
        return token.decode("utf-8")
    except Exception:
        return ""


def _decrypt_admin_wallets_csv(token_value: str) -> str:
    fernet = _admin_wallets_fernet()
    if not fernet:
        return ""
    try:
        raw = fernet.decrypt((token_value or "").encode("utf-8"))
        return raw.decode("utf-8")
    except Exception:
        return ""


def _admin_wallets_value() -> list[str]:
    fernet = _admin_wallets_fernet()
    if fernet:
        enc_saved = _analytics_get_state(ADMIN_WALLETS_STATE_KEY_ENC)
        items = _parse_admin_wallets_csv(_decrypt_admin_wallets_csv(enc_saved))
        if items:
            return items
        # Backward-compatible migration from plaintext state.
        plain_saved = _analytics_get_state(ADMIN_WALLETS_STATE_KEY_PLAIN)
        items = _parse_admin_wallets_csv(plain_saved)
        if items:
            enc = _encrypt_admin_wallets_csv(",".join(items))
            if enc:
                _analytics_set_state(ADMIN_WALLETS_STATE_KEY_ENC, enc)
                _analytics_set_state(ADMIN_WALLETS_STATE_KEY_PLAIN, "")
            return items
    else:
        saved = _analytics_get_state(ADMIN_WALLETS_STATE_KEY_PLAIN)
        items = _parse_admin_wallets_csv(saved)
        if items:
            return items
    return _default_admin_wallets()


def _set_admin_wallets(addresses: list[str]) -> None:
    clean = _parse_admin_wallets_csv(",".join(addresses))
    if not clean:
        raise HTTPException(status_code=400, detail="At least one admin address is required.")
    csv_value = ",".join(clean)
    fernet = _admin_wallets_fernet()
    if fernet:
        enc = _encrypt_admin_wallets_csv(csv_value)
        if not enc:
            raise HTTPException(status_code=500, detail="Failed to encrypt admin wallets.")
        _analytics_set_state(ADMIN_WALLETS_STATE_KEY_ENC, enc)
        _analytics_set_state(ADMIN_WALLETS_STATE_KEY_PLAIN, "")
        return
    _analytics_set_state(ADMIN_WALLETS_STATE_KEY_PLAIN, csv_value)


def _is_valid_email(value: str) -> bool:
    return bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", (value or "").strip()))


def _ticket_number(ticket_id: int) -> int:
    # First ticket id=1 -> number 12000.
    return 11999 + max(1, int(ticket_id))


def _feedback_number(feedback_id: int) -> int:
    # First feedback id=1 -> number 22000.
    return 21999 + max(1, int(feedback_id))


def _create_help_ticket(
    *,
    session_id: str,
    wallet_address: str,
    name: str,
    email: str,
    subject: str,
    message: str,
) -> int:
    with _analytics_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO help_tickets(ts, session_id, wallet_address, name, email, subject, message, status, admin_note)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'open', '')
            """,
            (
                _iso_now(),
                session_id,
                (wallet_address or "")[:64],
                (name or "").strip()[:120],
                (email or "").strip()[:200],
                (subject or "").strip()[:300],
                (message or "").strip()[:4000],
            ),
        )
        conn.commit()
        ticket_id = int(cur.lastrowid or 0)
    _append_help_ticket_message(
        ticket_id=ticket_id,
        author_type="user",
        message=message,
        session_id=session_id,
        wallet_address=wallet_address,
    )
    return ticket_id


def _append_help_ticket_message(
    *,
    ticket_id: int,
    author_type: str,
    message: str,
    session_id: str = "",
    wallet_address: str = "",
) -> None:
    text = (message or "").strip()
    if not text:
        return
    who = (author_type or "").strip().lower()
    if who not in {"user", "admin"}:
        who = "user"
    with _analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO help_ticket_messages(ticket_id, ts, author_type, message, session_id, wallet_address)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                int(ticket_id),
                _iso_now(),
                who,
                text[:4000],
                (session_id or "")[:128],
                (wallet_address or "").strip().lower()[:64],
            ),
        )
        conn.commit()


def _ticket_messages_map(ticket_ids: list[int], limit_per_ticket: int = 200) -> dict[int, list[dict[str, Any]]]:
    ids = [int(x) for x in ticket_ids if int(x) > 0]
    if not ids:
        return {}
    placeholders = ",".join(["?"] * len(ids))
    query = f"""
        SELECT ticket_id, ts, author_type, message
        FROM help_ticket_messages
        WHERE ticket_id IN ({placeholders})
        ORDER BY ticket_id ASC, id ASC
    """
    out: dict[int, list[dict[str, Any]]] = {}
    with _analytics_conn() as conn:
        rows = conn.execute(query, tuple(ids)).fetchall()
    for r in rows:
        tid = int(r[0])
        bucket = out.setdefault(tid, [])
        if len(bucket) >= limit_per_ticket:
            continue
        bucket.append(
            {
                "ts": str(r[1]),
                "author_type": str(r[2]),
                "message": str(r[3]),
            }
        )
    return out


def _merge_ticket_thread(
    *,
    ts: str,
    base_message: str,
    base_admin_note: str,
    thread: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for item in thread or []:
        msg = str(item.get("message") or "").strip()
        if not msg:
            continue
        who = str(item.get("author_type") or "user").strip().lower()
        if who not in {"user", "admin"}:
            who = "user"
        merged.append(
            {
                "ts": str(item.get("ts") or ""),
                "author_type": who,
                "message": msg,
            }
        )

    has_user = any(str(x.get("author_type") or "") == "user" for x in merged)
    has_admin = any(str(x.get("author_type") or "") == "admin" for x in merged)

    base_user = (base_message or "").strip()
    base_admin = (base_admin_note or "").strip()
    if base_user and not has_user:
        merged.insert(
            0,
            {
                "ts": ts or "",
                "author_type": "user",
                "message": base_user,
            },
        )
    if base_admin and not has_admin:
        merged.append(
            {
                "ts": "",
                "author_type": "admin",
                "message": base_admin,
            }
        )
    return merged


def _auto_close_stale_help_tickets(inactivity_days: int = 3) -> int:
    """Auto-close tickets when the latest message is admin and user is inactive."""
    days = max(1, int(inactivity_days or 3))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, message, admin_note
            FROM help_tickets
            WHERE status IN ('open', 'in_progress')
            ORDER BY id DESC
            LIMIT 1000
            """
        ).fetchall()
        if not rows:
            return 0
        base_map: dict[int, dict[str, str]] = {
            int(r[0]): {
                "ts": str(r[1] or ""),
                "message": str(r[2] or ""),
                "admin_note": str(r[3] or ""),
            }
            for r in rows
        }
        ticket_ids = list(base_map.keys())
        threads = _ticket_messages_map(ticket_ids, limit_per_ticket=300)
        to_close: list[int] = []
        for ticket_id in ticket_ids:
            base = base_map.get(ticket_id) or {}
            merged = _merge_ticket_thread(
                ts=str(base.get("ts") or ""),
                base_message=str(base.get("message") or ""),
                base_admin_note=str(base.get("admin_note") or ""),
                thread=threads.get(ticket_id, []),
            )
            if not merged:
                continue
            last_msg = merged[-1]
            if str(last_msg.get("author_type") or "").strip().lower() != "admin":
                continue
            last_ts = _parse_utc_iso(str(last_msg.get("ts") or "")) or _parse_utc_iso(str(base.get("ts") or ""))
            if not last_ts:
                continue
            if last_ts <= cutoff:
                to_close.append(ticket_id)
        if not to_close:
            return 0
        conn.executemany("UPDATE help_tickets SET status = 'done' WHERE id = ?", ((x,) for x in to_close))
        conn.commit()
    return len(to_close)


def _list_help_tickets(limit: int = 100) -> list[dict[str, Any]]:
    _auto_close_stale_help_tickets(inactivity_days=3)
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, session_id, wallet_address, name, email, subject, message, status, admin_note
            FROM help_tickets
            ORDER BY id DESC
            LIMIT ?
            """,
            (max(1, min(500, int(limit))),),
        ).fetchall()
    items = [
        {
            "id": int(r[0]),
            "ts": str(r[1]),
            "session_id": str(r[2]),
            "wallet_address": str(r[3]),
            "name": str(r[4]),
            "email": str(r[5]),
            "subject": str(r[6]),
            "message": str(r[7]),
            "status": str(r[8]),
            "admin_note": str(r[9]),
            "ticket_no": _ticket_number(int(r[0])),
        }
        for r in rows
    ]
    threads = _ticket_messages_map([int(x["id"]) for x in items])
    for item in items:
        item["thread"] = _merge_ticket_thread(
            ts=str(item.get("ts") or ""),
            base_message=str(item.get("message") or ""),
            base_admin_note=str(item.get("admin_note") or ""),
            thread=threads.get(int(item["id"]), []),
        )
    return items


def _list_help_tickets_for_session(session_id: str, limit: int = 50, wallet_address: str = "") -> list[dict[str, Any]]:
    _auto_close_stale_help_tickets(inactivity_days=3)
    lim = max(1, min(200, int(limit)))
    wallet = (wallet_address or "").strip().lower()
    with _analytics_conn() as conn:
        if _is_eth_address(wallet):
            rows = conn.execute(
                """
                SELECT id, ts, subject, message, status, admin_note
                FROM help_tickets
                WHERE session_id = ? OR lower(wallet_address) = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, wallet, lim),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT id, ts, subject, message, status, admin_note
                FROM help_tickets
                WHERE session_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (session_id, lim),
            ).fetchall()
    items = [
        {
            "id": int(r[0]),
            "ticket_no": _ticket_number(int(r[0])),
            "ts": str(r[1]),
            "subject": str(r[2]),
            "message": str(r[3]),
            "status": str(r[4]),
            "admin_reply": str(r[5]),
        }
        for r in rows
    ]
    threads = _ticket_messages_map([int(x["id"]) for x in items])
    for item in items:
        item["thread"] = _merge_ticket_thread(
            ts=str(item.get("ts") or ""),
            base_message=str(item.get("message") or ""),
            base_admin_note=str(item.get("admin_reply") or ""),
            thread=threads.get(int(item["id"]), []),
        )
    return items


def _list_faq_items(*, include_unpublished: bool = False, limit: int = 200) -> list[dict[str, Any]]:
    query = """
        SELECT id, created_at, updated_at, question, answer, is_published, is_featured, sort_order
        FROM faq_items
        {where}
        ORDER BY is_featured DESC, sort_order ASC, id ASC
        LIMIT ?
    """
    where = "" if include_unpublished else "WHERE is_published = 1"
    with _analytics_conn() as conn:
        rows = conn.execute(query.format(where=where), (max(1, min(500, int(limit))),)).fetchall()
    return [
        {
            "id": int(r[0]),
            "created_at": str(r[1]),
            "updated_at": str(r[2]),
            "question": str(r[3]),
            "answer": str(r[4]),
            "is_published": bool(int(r[5])),
            "is_featured": bool(int(r[6])),
            "sort_order": int(r[7]),
        }
        for r in rows
    ]


def _create_help_feedback(
    *,
    session_id: str,
    wallet_address: str,
    name: str,
    email: str,
    subject: str,
    message: str,
) -> int:
    with _analytics_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO help_feedback(ts, session_id, wallet_address, name, email, subject, message, status, reviewed_at, reviewed_by)
            VALUES(?, ?, ?, ?, ?, ?, ?, 'new', '', '')
            """,
            (
                _iso_now(),
                (session_id or "")[:128],
                (wallet_address or "").strip().lower()[:64],
                (name or "").strip()[:120],
                (email or "").strip()[:200],
                (subject or "").strip()[:300],
                (message or "").strip()[:4000],
            ),
        )
        conn.commit()
        return int(cur.lastrowid or 0)


def _list_help_feedback(limit: int = 200) -> list[dict[str, Any]]:
    lim = max(1, min(500, int(limit)))
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT id, ts, session_id, wallet_address, name, email, subject, message, status, reviewed_at, reviewed_by
            FROM help_feedback
            ORDER BY id DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
    return [
        {
            "id": int(r[0]),
            "feedback_no": _feedback_number(int(r[0])),
            "ts": str(r[1] or ""),
            "session_id": str(r[2] or ""),
            "wallet_address": str(r[3] or ""),
            "name": str(r[4] or ""),
            "email": str(r[5] or ""),
            "subject": str(r[6] or ""),
            "message": str(r[7] or ""),
            "status": str(r[8] or "new"),
            "reviewed_at": str(r[9] or ""),
            "reviewed_by": str(r[10] or ""),
        }
        for r in rows
    ]


def _mark_help_feedback_reviewed(feedback_id: int, reviewer_address: str = "") -> dict[str, Any]:
    fid = int(feedback_id or 0)
    if fid <= 0:
        raise HTTPException(status_code=400, detail="feedback_id is required.")
    reviewed_at = _iso_now()
    reviewer = (reviewer_address or "").strip().lower()[:64]
    with _analytics_conn() as conn:
        row = conn.execute("SELECT id FROM help_feedback WHERE id = ?", (fid,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Feedback not found.")
        conn.execute(
            "UPDATE help_feedback SET status = 'reviewed', reviewed_at = ?, reviewed_by = ? WHERE id = ?",
            (reviewed_at, reviewer, fid),
        )
        conn.commit()
    return {"feedback_id": fid, "feedback_no": _feedback_number(fid), "reviewed_at": reviewed_at, "reviewed_by": reviewer}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _positions_chain_catalog() -> list[dict[str, Any]]:
    chain_ids = set(CHAIN_ID_TO_KEY.keys()) | set(AAVE_CHAIN_ID_TO_NAME.keys())
    items: list[dict[str, Any]] = []
    for chain_id in sorted(chain_ids):
        key = CHAIN_ID_TO_KEY.get(int(chain_id), "")
        display = AAVE_CHAIN_ID_TO_NAME.get(int(chain_id), CHAIN_ID_TO_NAME.get(int(chain_id), key or str(chain_id)))
        has_pools = bool(key and (key in UNISWAP_V3_SUBGRAPHS or key in UNISWAP_V4_SUBGRAPHS or key in GOLDSKY_ENDPOINTS))
        has_lending = int(chain_id) in AAVE_CHAIN_ID_TO_NAME
        items.append(
            {
                "chain_id": int(chain_id),
                "key": key,
                "name": str(display),
                "has_pools": has_pools,
                "has_lending": has_lending,
            }
        )
    return items


def _parse_positions_addresses(raw_items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items or []:
        for token in re.split(r"[,\s;]+", str(raw or "").strip()):
            addr_raw = token.strip()
            addr_lc = addr_raw.lower()
            if not _is_eth_address(addr_raw):
                continue
            if addr_lc in seen:
                continue
            seen.add(addr_lc)
            # Keep original checksum/case to improve compatibility with indexers
            # that store owner/account ids case-sensitively.
            out.append(addr_raw)
    return out


def _parse_solana_addresses(raw_items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items or []:
        for token in re.split(r"[,\s;]+", str(raw or "").strip()):
            addr = token.strip()
            if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", addr):
                continue
            if addr in seen:
                continue
            seen.add(addr)
            out.append(addr)
    return out


def _parse_tron_addresses(raw_items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_items or []:
        for token in re.split(r"[,\s;]+", str(raw or "").strip()):
            addr = token.strip()
            if not re.fullmatch(r"T[1-9A-HJ-NP-Za-km-z]{33}", addr):
                continue
            if addr in seen:
                continue
            seen.add(addr)
            out.append(addr)
    return out


def _tick_to_sqrt_price_x96(tick: int) -> Decimal:
    # sqrt(1.0001^tick) * 2^96
    base = Decimal("1.0001")
    v = base ** Decimal(int(tick))
    sqrt_v = v.sqrt()
    return sqrt_v * (Decimal(2) ** 96)


def _position_amounts_from_detail(position: dict[str, Any], pool: dict[str, Any]) -> tuple[Decimal, Decimal] | None:
    try:
        liq = Decimal(str(position.get("liquidity") or "0"))
        if liq <= 0:
            return None
        t0 = pool.get("token0") or {}
        t1 = pool.get("token1") or {}
        dec0 = int(t0.get("decimals") or 18)
        dec1 = int(t1.get("decimals") or 18)
        if dec0 < 0 or dec0 > 36 or dec1 < 0 or dec1 > 36:
            return None
        sqrt_current_raw = str(pool.get("sqrtPrice") or "").strip()
        if not sqrt_current_raw:
            return None
        sqrt_current = Decimal(sqrt_current_raw)
        if sqrt_current <= 0:
            return None
        lower = position.get("tickLower") or {}
        upper = position.get("tickUpper") or {}
        tick_lower = int(lower.get("tickIdx"))
        tick_upper = int(upper.get("tickIdx"))
        if tick_lower >= tick_upper:
            return None
        sqrt_a = _tick_to_sqrt_price_x96(tick_lower)
        sqrt_b = _tick_to_sqrt_price_x96(tick_upper)
        if sqrt_a <= 0 or sqrt_b <= 0:
            return None
        q96 = Decimal(2) ** 96
        if sqrt_current <= sqrt_a:
            amount0_raw = (liq * q96 * (sqrt_b - sqrt_a)) / (sqrt_b * sqrt_a)
            amount1_raw = Decimal(0)
        elif sqrt_current < sqrt_b:
            amount0_raw = (liq * q96 * (sqrt_b - sqrt_current)) / (sqrt_b * sqrt_current)
            amount1_raw = (liq * (sqrt_current - sqrt_a)) / q96
        else:
            amount0_raw = Decimal(0)
            amount1_raw = (liq * (sqrt_b - sqrt_a)) / q96
        amount0 = amount0_raw / (Decimal(10) ** dec0)
        amount1 = amount1_raw / (Decimal(10) ** dec1)
        if amount0 < 0 or amount1 < 0:
            return None
        return amount0, amount1
    except Exception:
        return None


def _estimate_position_tvl_usd_from_detail(position: dict[str, Any], pool: dict[str, Any]) -> float | None:
    try:
        amounts = _position_amounts_from_detail(position, pool)
        if not amounts:
            return None
        amount0, amount1 = amounts
        token0_price_in_token1 = Decimal(str(pool.get("token0Price") or "0"))
        tvl_usd = Decimal(str(pool.get("totalValueLockedUSD") or "0"))
        tvl0 = Decimal(str(pool.get("totalValueLockedToken0") or "0"))
        tvl1 = Decimal(str(pool.get("totalValueLockedToken1") or "0"))
        max_ratio = Decimal(str(POSITIONS_MAX_TOKEN_PRICE_RATIO))
        min_ratio = Decimal(1) / max_ratio
        if token0_price_in_token1 <= 0 or token0_price_in_token1 < min_ratio or token0_price_in_token1 > max_ratio or tvl_usd <= 0:
            return None
        denom = tvl0 * token0_price_in_token1 + tvl1
        if denom <= 0:
            return None
        price1_usd = tvl_usd / denom
        price0_usd = token0_price_in_token1 * price1_usd
        value = amount0 * price0_usd + amount1 * price1_usd
        if value < 0:
            return None
        return float(value)
    except Exception:
        return None


def _estimate_position_tvl_usd_from_detail_external(
    position: dict[str, Any], pool: dict[str, Any], chain_id: int
) -> float | None:
    try:
        amounts = _position_amounts_from_detail(position, pool)
        if not amounts:
            return None
        amount0, amount1 = amounts
        t0 = pool.get("token0") or {}
        t1 = pool.get("token1") or {}
        token0 = str(t0.get("id") or "").strip().lower()
        token1 = str(t1.get("id") or "").strip().lower()
        prices = _get_token_prices_usd(int(chain_id), [token0, token1])
        p0 = prices.get(token0)
        p1 = prices.get(token1)
        token0_price_in_token1 = _safe_float(pool.get("token0Price"))
        ratio_ok = (
            token0_price_in_token1 > 0
            and token0_price_in_token1 >= (1.0 / POSITIONS_MAX_TOKEN_PRICE_RATIO)
            and token0_price_in_token1 <= POSITIONS_MAX_TOKEN_PRICE_RATIO
        )
        if p0 is None and p1 is not None and ratio_ok:
            p0 = p1 * token0_price_in_token1
        if p1 is None and p0 is not None and ratio_ok:
            p1 = p0 / token0_price_in_token1
        if p0 is None and p1 is None:
            return None
        value = Decimal(0)
        if p0 is not None:
            value += amount0 * Decimal(str(p0))
        if p1 is not None:
            value += amount1 * Decimal(str(p1))
        if value <= 0:
            return None
        return float(value)
    except Exception:
        return None


def _coingecko_platform_for_chain_id(chain_id: int) -> str:
    mapping: dict[int, str] = {
        1: "ethereum",
        10: "optimism",
        56: "binance-smart-chain",
        130: "unichain",
        137: "polygon-pos",
        324: "zksync",
        8453: "base",
        42161: "arbitrum-one",
        43114: "avalanche",
        81457: "blast",
    }
    return mapping.get(int(chain_id), "")


def _dexscreener_chain_for_chain_id(chain_id: int) -> str:
    mapping: dict[int, str] = {
        1: "ethereum",
        10: "optimism",
        56: "bsc",
        130: "unichain",
        137: "polygon",
        8453: "base",
        42161: "arbitrum",
        43114: "avalanche",
    }
    return mapping.get(int(chain_id), "")


def _fetch_token_prices_usd_coingecko(chain_id: int, token_addresses: list[str]) -> dict[str, float]:
    platform = _coingecko_platform_for_chain_id(int(chain_id))
    addrs: list[str] = []
    for a in token_addresses or []:
        s = str(a or "").strip().lower()
        if _is_eth_address(s) and s not in addrs:
            addrs.append(s)
    if not platform or not addrs:
        return {}
    # Keep batch size conservative for URL length and API stability.
    chunks = [addrs[i : i + 40] for i in range(0, len(addrs), 40)]
    out: dict[str, float] = {}
    for chunk in chunks:
        try:
            url = (
                f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
                f"?contract_addresses={','.join(chunk)}&vs_currencies=usd"
            )
            req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
            with urlopen(req, timeout=12) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if not isinstance(payload, dict):
                continue
            for addr, item in payload.items():
                try:
                    price = float((item or {}).get("usd"))
                except Exception:
                    continue
                if price > 0:
                    out[str(addr).strip().lower()] = price
        except Exception:
            # Silent fallback to subgraph-derived valuation.
            continue
    return out


def _fetch_token_price_usd_dexscreener(chain_id: int, token_address: str) -> float | None:
    chain_slug = _dexscreener_chain_for_chain_id(int(chain_id))
    addr = str(token_address or "").strip().lower()
    if not chain_slug or not _is_eth_address(addr):
        return None
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{addr}"
        req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
        with urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        pairs = payload.get("pairs") or []
        best_price: float | None = None
        best_liq = -1.0
        for p in pairs:
            if str((p or {}).get("chainId") or "").strip().lower() != chain_slug:
                continue
            try:
                price = float((p or {}).get("priceUsd") or 0)
            except Exception:
                price = 0.0
            if price <= 0:
                continue
            liq_usd = _safe_float(((p or {}).get("liquidity") or {}).get("usd"))
            if liq_usd > best_liq:
                best_liq = liq_usd
                best_price = price
        return best_price
    except Exception:
        return None


def _get_token_prices_usd(chain_id: int, token_addresses: list[str]) -> dict[str, float]:
    def _symbol_for_addr(addr: str) -> str:
        chain_key = CHAIN_ID_TO_KEY.get(int(chain_id), "")
        from_cfg = (_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(str(chain_key).lower(), {}) or {}).get(addr)
        if from_cfg:
            return str(from_cfg).upper()
        onchain = _fetch_erc20_symbol_onchain(int(chain_id), addr)
        return str(onchain or "").upper()

    def _is_stable_symbol(sym: str) -> bool:
        s = str(sym or "").strip().upper()
        return s in {"USDC", "USDT", "DAI", "USDE", "FDUSD", "USDC.E", "USDT0", "USD₮"}

    def _major_coingecko_id(sym: str) -> str:
        s = str(sym or "").strip().upper()
        mapping = {
            "ETH": "ethereum",
            "WETH": "ethereum",
            "BTC": "bitcoin",
            "WBTC": "bitcoin",
            "BNB": "binancecoin",
            "WBNB": "binancecoin",
            "UNI": "uniswap",
            "USDC": "usd-coin",
            "USDT": "tether",
            "USD₮": "tether",
            "DAI": "dai",
        }
        return mapping.get(s, "")

    def _fetch_major_price_usd(coin_id: str) -> float | None:
        cid = str(coin_id or "").strip().lower()
        if not cid:
            return None
        now_ts = time.time()
        cached = MAJOR_ASSET_PRICE_CACHE.get(cid)
        if cached and (now_ts - cached[0]) <= MAJOR_ASSET_PRICE_CACHE_TTL_SEC and cached[1] > 0:
            return float(cached[1])
        try:
            url = f"https://api.coingecko.com/api/v3/simple/price?ids={cid}&vs_currencies=usd"
            req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
            with urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            px = float(((payload or {}).get(cid) or {}).get("usd") or 0.0)
            if px > 0:
                MAJOR_ASSET_PRICE_CACHE[cid] = (now_ts, px)
                return px
        except Exception:
            return None
        return None

    now = time.time()
    requested: list[str] = []
    result: dict[str, float] = {}
    missing: list[str] = []
    with TOKEN_PRICE_CACHE_LOCK:
        for token in token_addresses or []:
            addr = str(token or "").strip().lower()
            if not _is_eth_address(addr):
                continue
            if addr not in requested:
                requested.append(addr)
            cached = TOKEN_PRICE_CACHE.get((int(chain_id), addr))
            if cached and (now - cached[0]) <= PRICE_CACHE_TTL_SEC and cached[1] > 0:
                result[addr] = cached[1]
            else:
                missing.append(addr)
    if missing:
        fetched = _fetch_token_prices_usd_coingecko(chain_id, missing)
        with TOKEN_PRICE_CACHE_LOCK:
            for addr, price in fetched.items():
                TOKEN_PRICE_CACHE[(int(chain_id), addr)] = (now, float(price))
                result[addr] = float(price)
    still_missing = [a for a in requested if a not in result]
    if still_missing:
        updates: dict[str, float] = {}
        for addr in still_missing:
            price = _fetch_token_price_usd_dexscreener(chain_id, addr)
            if price is not None and price > 0:
                updates[addr] = float(price)
        if updates:
            with TOKEN_PRICE_CACHE_LOCK:
                for addr, price in updates.items():
                    TOKEN_PRICE_CACHE[(int(chain_id), addr)] = (now, float(price))
                    result[addr] = float(price)

    # Sanity normalization and major-asset fallback.
    for addr in requested:
        sym = _symbol_for_addr(addr)
        px = result.get(addr)
        major_id = _major_coingecko_id(sym)
        if _is_stable_symbol(sym):
            if px is None or px <= 0 or px < 0.7 or px > 1.3:
                result[addr] = 1.0
                with TOKEN_PRICE_CACHE_LOCK:
                    TOKEN_PRICE_CACHE[(int(chain_id), addr)] = (now, 1.0)
            continue
        if px is not None and px > 0 and (px < POSITIONS_MIN_TOKEN_PRICE_USD or px > POSITIONS_MAX_TOKEN_PRICE_USD) and not major_id:
            # Drop clearly abnormal non-major prices (common for illiquid/spam quotes).
            result.pop(addr, None)
            px = None
        if px is None or px <= 0:
            if major_id:
                fallback_px = _fetch_major_price_usd(major_id)
                if fallback_px is not None and fallback_px > 0:
                    result[addr] = float(fallback_px)
                    with TOKEN_PRICE_CACHE_LOCK:
                        TOKEN_PRICE_CACHE[(int(chain_id), addr)] = (now, float(fallback_px))
            continue
    return {a: result[a] for a in requested if a in result}


def _rpc_urls_for_chain(chain_id: int) -> list[str]:
    env_key = f"POSITIONS_RPC_URLS_{int(chain_id)}"
    custom = os.environ.get(env_key, "").strip()
    if custom:
        out = [x.strip() for x in custom.split(",") if x.strip()]
        if out:
            return out
    return list(DEFAULT_RPC_URLS_BY_CHAIN_ID.get(int(chain_id), []))


def _json_rpc_call(chain_id: int, method: str, params: list[Any]) -> Any:
    rpc_urls = _rpc_urls_for_chain(chain_id)
    if not rpc_urls:
        raise RuntimeError(f"No RPC configured for chain_id={chain_id}")
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode("utf-8")
    last_err = "unknown rpc error"
    for rpc_url in rpc_urls:
        try:
            req = UrlRequest(
                rpc_url,
                data=body,
                headers={"Content-Type": "application/json", "User-Agent": "uni-fee-web/0.0.2"},
            )
            with urlopen(req, timeout=POSITIONS_ONCHAIN_TIMEOUT_SEC) as resp:
                parsed = json.loads(resp.read().decode("utf-8"))
            if isinstance(parsed, dict) and parsed.get("error"):
                last_err = str((parsed.get("error") or {}).get("message") or parsed.get("error"))
                continue
            if isinstance(parsed, dict) and "result" in parsed:
                return parsed.get("result")
            last_err = "malformed rpc response"
        except Exception as e:
            last_err = str(e)
    raise RuntimeError(last_err)


def _eth_call_hex(chain_id: int, to: str, data_hex: str) -> str:
    result = _json_rpc_call(
        chain_id,
        "eth_call",
        [{"to": str(to).strip(), "data": str(data_hex).strip()}, "latest"],
    )
    out = str(result or "").strip().lower()
    if not out.startswith("0x"):
        raise RuntimeError("Invalid eth_call result")
    return out


def _encode_address_word(addr: str) -> str:
    raw = str(addr or "").strip().lower()
    if not raw.startswith("0x"):
        raise ValueError("Address must be 0x-prefixed")
    h = raw[2:]
    if len(h) != 40:
        raise ValueError("Address length mismatch")
    return ("0" * 24) + h


def _encode_uint_word(value: int) -> str:
    v = int(value)
    if v < 0:
        raise ValueError("uint must be >= 0")
    return f"{v:064x}"


def _hex_words(data_hex: str) -> list[str]:
    h = str(data_hex or "").strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    if not h:
        return []
    if len(h) % 64 != 0:
        h = h[: len(h) - (len(h) % 64)]
    return [h[i : i + 64] for i in range(0, len(h), 64)]


def _decode_uint_from_word(word: str) -> int:
    return int(str(word or "0"), 16)


def _decode_int_from_word(word: str) -> int:
    v = int(str(word or "0"), 16)
    if v >= (1 << 255):
        v -= (1 << 256)
    return v


def _decode_address_from_word(word: str) -> str:
    w = str(word or "").strip().lower().rjust(64, "0")
    return "0x" + w[-40:]


def _decode_uint_eth_call(data_hex: str) -> int:
    words = _hex_words(data_hex)
    if not words:
        return 0
    return _decode_uint_from_word(words[0])


def _decode_abi_string(data_hex: str) -> str | None:
    h = str(data_hex or "").strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    if not h:
        return None
    try:
        # Dynamic ABI string encoding.
        if len(h) >= 128:
            offset_bytes = int(h[0:64], 16)
            offset = offset_bytes * 2
            if offset + 64 <= len(h):
                strlen = int(h[offset : offset + 64], 16)
                start = offset + 64
                end = min(len(h), start + max(0, int(strlen)) * 2)
                if end > start:
                    raw = bytes.fromhex(h[start:end])
                    s = raw.decode("utf-8", errors="ignore").strip("\x00").strip()
                    if s:
                        return s
        # bytes32-like fallback.
        first = h[:64]
        if first:
            raw = bytes.fromhex(first)
            s = raw.split(b"\x00")[0].decode("utf-8", errors="ignore").strip()
            if s:
                return s
    except Exception:
        return None
    return None


def _fetch_erc20_symbol_onchain(chain_id: int, token_address: str) -> str | None:
    addr = str(token_address or "").strip().lower()
    if not _is_eth_address(addr):
        return None
    key = (int(chain_id), addr)
    with TOKEN_SYMBOL_CACHE_LOCK:
        cached = TOKEN_SYMBOL_CACHE.get(key)
    if cached is not None:
        return cached or None
    try:
        out = _eth_call_hex(int(chain_id), addr, "0x95d89b41")
        sym = _decode_abi_string(out)
        if sym:
            sym = sym.strip()
        if not sym:
            sym = ""
    except Exception:
        sym = ""
    with TOKEN_SYMBOL_CACHE_LOCK:
        TOKEN_SYMBOL_CACHE[key] = sym
    return sym or None


def _token_display_symbol(chain_id: int, chain_key: str, token_obj: dict[str, Any]) -> str:
    sym = str((token_obj or {}).get("symbol") or "").strip()
    if sym:
        return sym
    addr = str((token_obj or {}).get("id") or "").strip().lower()
    if not addr:
        return "?"
    cfg_sym = (_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(str(chain_key or "").strip().lower(), {}) or {}).get(addr)
    if cfg_sym:
        return cfg_sym
    onchain = _fetch_erc20_symbol_onchain(int(chain_id), addr)
    if onchain:
        return onchain
    return (addr[:8] if len(addr) >= 8 else addr) or "?"


def _is_probably_spam_symbol(symbol: str) -> bool:
    s = str(symbol or "").strip()
    if not s or s == "?":
        return True
    low = s.lower()
    if len(s) > 16:
        return True
    if any(x in low for x in ("http", "www", ".com", ".io", ".org", "t.me", "telegram", "twitter", "x.com")):
        return True
    if any(ch in s for ch in (":", "/", "\\", "|", "@")):
        return True
    if " " in s:
        return True
    if any(x in low for x in ("swap", " dao", "dao", " org", "org", "airdrop", "claim")):
        return True
    if re.fullmatch(r"0x[0-9a-f]{6,12}", low):
        return True
    return False


def _is_suspected_spam_pair(
    chain_key: str,
    token0_obj: dict[str, Any],
    token1_obj: dict[str, Any],
    s0: str,
    s1: str,
    position_tvl_usd: float | None,
) -> bool:
    if not POSITIONS_FILTER_SPAM_TOKENS:
        return False
    chain_addr_map = _TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(str(chain_key or "").strip().lower(), {}) or {}
    a0 = str((token0_obj or {}).get("id") or "").strip().lower()
    a1 = str((token1_obj or {}).get("id") or "").strip().lower()
    both_curated = (a0 in chain_addr_map) and (a1 in chain_addr_map)
    if both_curated:
        return False
    spam0 = _is_probably_spam_symbol(s0)
    spam1 = _is_probably_spam_symbol(s1)
    # If pair tokens are not from curated list, mark clearly suspicious symbols.
    return bool(spam0 or spam1)


def _pool_token0_price_from_sqrt_x96(sqrt_price_x96: int, dec0: int, dec1: int) -> float | None:
    try:
        if sqrt_price_x96 <= 0:
            return None
        p = (Decimal(int(sqrt_price_x96)) ** 2) / (Decimal(2) ** 192)
        p *= Decimal(10) ** Decimal(int(dec0) - int(dec1))
        if p <= 0:
            return None
        return float(p)
    except Exception:
        return None


def _scan_v3_positions_onchain(
    owner: str,
    chain_id: int,
    deadline_ts: float | None = None,
    include_price_details: bool = True,
    protocol_label: str = "uniswap_v3",
    source_tag: str = "onchain_npm",
) -> list[dict[str, Any]]:
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    npm = UNISWAP_V3_NPM_BY_CHAIN_ID.get(int(chain_id))
    factory = UNISWAP_V3_FACTORY_BY_CHAIN_ID.get(int(chain_id))
    if not npm or not factory:
        return []
    if deadline_ts is not None and time.monotonic() >= deadline_ts:
        return []

    # balanceOf(address)
    bal_data = "0x70a08231" + _encode_address_word(owner_addr)
    balance = _decode_uint_eth_call(_eth_call_hex(int(chain_id), npm, bal_data))
    if balance <= 0:
        return []
    limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    out: list[dict[str, Any]] = []

    def _build_position_from_token_id(token_id: int) -> dict[str, Any] | None:
        # positions(uint256)
        pos_data = "0x99fbab88" + _encode_uint_word(token_id)
        pos_words = _hex_words(_eth_call_hex(int(chain_id), npm, pos_data))
        if len(pos_words) < 8:
            return None
        token0 = _decode_address_from_word(pos_words[2])
        token1 = _decode_address_from_word(pos_words[3])
        fee = _decode_uint_from_word(pos_words[4])
        tick_lower = _decode_int_from_word(pos_words[5])
        tick_upper = _decode_int_from_word(pos_words[6])
        liq = _decode_uint_from_word(pos_words[7])
        if liq <= 0:
            return None

        # getPool(address,address,uint24)
        pool_data = "0x1698ee82" + _encode_address_word(token0) + _encode_address_word(token1) + _encode_uint_word(fee)
        pool_words = _hex_words(_eth_call_hex(int(chain_id), factory, pool_data))
        if not pool_words:
            return None
        pool_addr = _decode_address_from_word(pool_words[0])
        if not _is_eth_address(pool_addr):
            return None

        sqrt_price_x96 = 0
        dec0 = 18
        dec1 = 18
        token0_price = None
        if include_price_details:
            # slot0() => sqrtPriceX96 is first word
            slot0_words = _hex_words(_eth_call_hex(int(chain_id), pool_addr, "0x3850c7bd"))
            sqrt_price_x96 = _decode_uint_from_word(slot0_words[0]) if slot0_words else 0

            # decimals()
            dec0 = _decode_uint_eth_call(_eth_call_hex(int(chain_id), token0, "0x313ce567"))
            dec1 = _decode_uint_eth_call(_eth_call_hex(int(chain_id), token1, "0x313ce567"))
            if dec0 < 0 or dec0 > 36:
                dec0 = 18
            if dec1 < 0 or dec1 > 36:
                dec1 = 18
            token0_price = _pool_token0_price_from_sqrt_x96(sqrt_price_x96, int(dec0), int(dec1))

        return {
            "id": str(token_id),
            "liquidity": str(liq),
            "tickLower": {"tickIdx": str(tick_lower)},
            "tickUpper": {"tickIdx": str(tick_upper)},
            "pool": {
                "id": str(pool_addr).lower(),
                "feeTier": str(int(fee)),
                "sqrtPrice": str(int(sqrt_price_x96)) if int(sqrt_price_x96) > 0 else "",
                "token0Price": float(token0_price) if token0_price is not None else 0.0,
                "token0": {"id": str(token0).lower(), "decimals": int(dec0)},
                "token1": {"id": str(token1).lower(), "decimals": int(dec1)},
            },
            "_skip_enrich": not include_price_details,
            "_protocol_label": str(protocol_label or "uniswap_v3"),
            "_source": str(source_tag or "onchain_npm"),
        }
    # Scan latest NFTs first: active user positions are typically near the end.
    start_idx = max(0, int(balance) - limit)
    scan_indices = range(int(balance) - 1, start_idx - 1, -1)

    for idx in scan_indices:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            # tokenOfOwnerByIndex(address,uint256)
            token_data = "0x2f745c59" + _encode_address_word(owner_addr) + _encode_uint_word(idx)
            token_id = _decode_uint_eth_call(_eth_call_hex(int(chain_id), npm, token_data))
            if token_id <= 0:
                continue
            built = _build_position_from_token_id(int(token_id))
            if built:
                out.append(built)
        except Exception:
            continue
    return out


def _fetch_v3_position_onchain_by_token_id(
    chain_id: int,
    token_id: str,
    *,
    include_price_details: bool = True,
    protocol_label: str = "uniswap_v3",
    source_tag: str = "onchain_tokenid",
) -> dict[str, Any] | None:
    try:
        cid = int(chain_id)
        tid = int(str(token_id or "").strip())
    except Exception:
        return None
    if tid <= 0:
        return None
    npm = UNISWAP_V3_NPM_BY_CHAIN_ID.get(cid)
    factory = UNISWAP_V3_FACTORY_BY_CHAIN_ID.get(cid)
    if not npm or not factory:
        return None
    try:
        pos_data = "0x99fbab88" + _encode_uint_word(tid)
        pos_words = _hex_words(_eth_call_hex(cid, npm, pos_data))
        if len(pos_words) < 8:
            return None
        token0 = _decode_address_from_word(pos_words[2])
        token1 = _decode_address_from_word(pos_words[3])
        fee = _decode_uint_from_word(pos_words[4])
        tick_lower = _decode_int_from_word(pos_words[5])
        tick_upper = _decode_int_from_word(pos_words[6])
        liq = _decode_uint_from_word(pos_words[7])
        if liq <= 0:
            return None
        pool_data = "0x1698ee82" + _encode_address_word(token0) + _encode_address_word(token1) + _encode_uint_word(fee)
        pool_words = _hex_words(_eth_call_hex(cid, factory, pool_data))
        if not pool_words:
            return None
        pool_addr = _decode_address_from_word(pool_words[0])
        if not _is_eth_address(pool_addr):
            return None
        sqrt_price_x96 = 0
        dec0 = 18
        dec1 = 18
        token0_price = None
        if include_price_details:
            slot0_words = _hex_words(_eth_call_hex(cid, pool_addr, "0x3850c7bd"))
            sqrt_price_x96 = _decode_uint_from_word(slot0_words[0]) if slot0_words else 0
            dec0 = _decode_uint_eth_call(_eth_call_hex(cid, token0, "0x313ce567"))
            dec1 = _decode_uint_eth_call(_eth_call_hex(cid, token1, "0x313ce567"))
            if dec0 < 0 or dec0 > 36:
                dec0 = 18
            if dec1 < 0 or dec1 > 36:
                dec1 = 18
            token0_price = _pool_token0_price_from_sqrt_x96(sqrt_price_x96, int(dec0), int(dec1))
        return {
            "id": str(tid),
            "liquidity": str(liq),
            "tickLower": {"tickIdx": str(tick_lower)},
            "tickUpper": {"tickIdx": str(tick_upper)},
            "pool": {
                "id": str(pool_addr).lower(),
                "feeTier": str(int(fee)),
                "sqrtPrice": str(int(sqrt_price_x96)) if int(sqrt_price_x96) > 0 else "",
                "token0Price": float(token0_price) if token0_price is not None else 0.0,
                "token0": {"id": str(token0).lower(), "decimals": int(dec0)},
                "token1": {"id": str(token1).lower(), "decimals": int(dec1)},
            },
            "_skip_enrich": not include_price_details,
            "_protocol_label": str(protocol_label or "uniswap_v3"),
            "_source": str(source_tag or "onchain_tokenid"),
        }
    except Exception:
        return None


def _scan_pancake_staked_v3_positions_onchain(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    mc = PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID.get(cid)
    if not mc:
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    try:
        bal_data = "0x70a08231" + _encode_address_word(owner_addr)
        balance = _decode_uint_eth_call(_eth_call_hex(cid, mc, bal_data))
    except Exception:
        return []
    if balance <= 0:
        return []
    limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    start_idx = max(0, int(balance) - limit)
    scan_indices = range(int(balance) - 1, start_idx - 1, -1)
    out: list[dict[str, Any]] = []
    for idx in scan_indices:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            tok_data = "0x2f745c59" + _encode_address_word(owner_addr) + _encode_uint_word(idx)
            token_id = _decode_uint_eth_call(_eth_call_hex(cid, mc, tok_data))
            if token_id <= 0:
                continue
            pos = _fetch_v3_position_onchain_by_token_id(
                cid,
                str(token_id),
                include_price_details=True,
                protocol_label="pancake_v3",
                source_tag="onchain_pancake_masterchef",
            )
            if pos:
                out.append(pos)
        except Exception:
            continue
    return out


def _keccak256_hex(data: bytes) -> str | None:
    try:
        from eth_hash.auto import keccak as _keccak

        return "0x" + _keccak(data).hex()
    except Exception:
        return None


def _normalize_infinity_currency(chain_id: int, token: str) -> tuple[str, str, int]:
    t = str(token or "").strip().lower()
    if _is_eth_address(t) and t != "0x0000000000000000000000000000000000000000":
        return t, (_fetch_erc20_symbol_onchain(int(chain_id), t) or t[:8]).upper(), _fetch_erc20_decimals_onchain(int(chain_id), t)
    chain_key = CHAIN_ID_TO_KEY.get(int(chain_id), "")
    wrapped = ((_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(chain_key, {}) or {}).items())
    # Prefer wrapped native aliases from config when Infinity uses native currency address(0).
    for addr, sym in wrapped:
        us = str(sym or "").upper()
        if us in {"WBNB", "WETH", "WMATIC", "WPOL"} and _is_eth_address(addr):
            return str(addr).lower(), us, _fetch_erc20_decimals_onchain(int(chain_id), str(addr))
    native_sym = "BNB" if int(chain_id) == 56 else "NATIVE"
    return "0x0000000000000000000000000000000000000000", native_sym, 18


def _scan_infinity_position_ids_for_owner(
    position_manager: str,
    owner_addr: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[int]:
    cid = int(chain_id)
    owner = str(owner_addr or "").strip().lower()
    if not _is_eth_address(owner):
        return []
    pm = str(position_manager or "").strip().lower()
    if not _is_eth_address(pm):
        return []
    try:
        bal_data = "0x70a08231" + _encode_address_word(owner)
        balance = _decode_uint_eth_call(_eth_call_hex(cid, pm, bal_data))
    except Exception:
        return []
    if balance <= 0:
        return []
    limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    token_ids: list[int] = []
    enumerable_ok = True
    for idx in range(int(balance) - 1, max(-1, int(balance) - limit - 1), -1):
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            tok_data = "0x2f745c59" + _encode_address_word(owner) + _encode_uint_word(idx)
            tid = _decode_uint_eth_call(_eth_call_hex(cid, pm, tok_data))
            if tid > 0:
                token_ids.append(int(tid))
        except Exception:
            enumerable_ok = False
            break
    if token_ids or enumerable_ok:
        return token_ids
    # Fallback for contracts without ERC721Enumerable: scan recent IDs by ownerOf.
    # Keep this path lightweight to avoid starving other chain scans.
    now_ts = time.monotonic()
    ownerof_deadline = now_ts + 1.5
    if deadline_ts is not None:
        ownerof_deadline = min(ownerof_deadline, deadline_ts)
    if ownerof_deadline <= time.monotonic():
        return token_ids
    try:
        next_token_id = _decode_uint_eth_call(_eth_call_hex(cid, pm, "0x75794a3c"))
    except Exception:
        return token_ids
    if next_token_id <= 0:
        return token_ids
    lookback = max(POSITIONS_INFINITY_OWNER_LOOKBACK, limit * 40)
    start_tid = max(1, int(next_token_id) - int(lookback))
    owner_word = _encode_address_word(owner)[-40:]
    for tid in range(int(next_token_id) - 1, start_tid - 1, -1):
        if time.monotonic() >= ownerof_deadline:
            break
        if len(token_ids) >= limit:
            break
        try:
            owner_data = "0x6352211e" + _encode_uint_word(tid)
            owner_hex = _eth_call_hex(cid, pm, owner_data)
            owner_words = _hex_words(owner_hex)
            if not owner_words:
                continue
            current_owner = owner_words[0][-40:].lower()
            if current_owner == owner_word:
                token_ids.append(int(tid))
        except Exception:
            continue
    return token_ids


def _scan_pancake_infinity_cl_positions_onchain(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    pm = PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid)
    if not pm:
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    token_ids = _scan_infinity_position_ids_for_owner(pm, owner_addr, cid, deadline_ts=deadline_ts)
    if not token_ids:
        return []
    out: list[dict[str, Any]] = []
    for token_id in token_ids:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            pos_data = "0x99fbab88" + _encode_uint_word(token_id)
            pos_words = _hex_words(_eth_call_hex(cid, pm, pos_data))
            if len(pos_words) < 12:
                continue
            raw0 = _decode_address_from_word(pos_words[0])
            raw1 = _decode_address_from_word(pos_words[1])
            pool_manager = _decode_address_from_word(pos_words[3])
            fee = _decode_uint_from_word(pos_words[4])
            tick_lower = _decode_int_from_word(pos_words[6])
            tick_upper = _decode_int_from_word(pos_words[7])
            liq = _decode_uint_from_word(pos_words[8])
            if liq <= 0:
                continue
            token0_addr, token0_sym, dec0 = _normalize_infinity_currency(cid, raw0)
            token1_addr, token1_sym, dec1 = _normalize_infinity_currency(cid, raw1)
            poolkey_blob = bytes.fromhex("".join(pos_words[:6]))
            pool_id_hex = _keccak256_hex(poolkey_blob) or ("0x" + _encode_uint_word(token_id))
            sqrt_price_x96 = 0
            pool_liquidity = 0
            try:
                slot_words = _hex_words(_eth_call_hex(cid, pool_manager, "0xc815641c" + pool_id_hex[2:]))
                if slot_words:
                    sqrt_price_x96 = _decode_uint_from_word(slot_words[0])
            except Exception:
                pass
            try:
                pool_liquidity = _decode_uint_eth_call(_eth_call_hex(cid, pool_manager, "0xfa6793d5" + pool_id_hex[2:]))
            except Exception:
                pool_liquidity = 0
            token0_price = _token0_price_from_sqrt_price_x96(int(sqrt_price_x96), dec0, dec1)
            out.append(
                {
                    "id": f"inf-cl:{token_id}",
                    "liquidity": str(liq),
                    "tickLower": {"tickIdx": str(tick_lower)},
                    "tickUpper": {"tickIdx": str(tick_upper)},
                    "pool": {
                        "id": pool_id_hex,
                        "feeTier": str(fee),
                        "liquidity": str(pool_liquidity),
                        "sqrtPrice": str(sqrt_price_x96),
                        "token0Price": str(token0_price) if token0_price else "0",
                        "totalValueLockedUSD": "0",
                        "totalValueLockedToken0": "0",
                        "totalValueLockedToken1": "0",
                        "token0": {"id": token0_addr, "decimals": str(dec0), "symbol": token0_sym},
                        "token1": {"id": token1_addr, "decimals": str(dec1), "symbol": token1_sym},
                    },
                    "_protocol_label": "pancake_infinity_cl",
                    "_source": "onchain_pancake_infinity_cl",
                    "_skip_enrich": True,
                }
            )
        except Exception:
            continue
    return out


def _scan_pancake_infinity_bin_positions_onchain(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    pm = PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid)
    if not pm:
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    token_ids = _scan_infinity_position_ids_for_owner(pm, owner_addr, cid, deadline_ts=deadline_ts)
    if not token_ids:
        return []
    out: list[dict[str, Any]] = []
    for token_id in token_ids:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            pos_data = "0x99fbab88" + _encode_uint_word(token_id)
            pos_words = _hex_words(_eth_call_hex(cid, pm, pos_data))
            if len(pos_words) < 7:
                continue
            raw0 = _decode_address_from_word(pos_words[0])
            raw1 = _decode_address_from_word(pos_words[1])
            fee = _decode_uint_from_word(pos_words[4])
            token0_addr, token0_sym, dec0 = _normalize_infinity_currency(cid, raw0)
            token1_addr, token1_sym, dec1 = _normalize_infinity_currency(cid, raw1)
            poolkey_blob = bytes.fromhex("".join(pos_words[:6]))
            pool_id_hex = _keccak256_hex(poolkey_blob) or ("0x" + _encode_uint_word(token_id))
            out.append(
                {
                    "id": f"inf-bin:{token_id}",
                    "liquidity": "1",
                    "tickLower": {"tickIdx": "0"},
                    "tickUpper": {"tickIdx": "1"},
                    "pool": {
                        "id": pool_id_hex,
                        "feeTier": str(fee),
                        "liquidity": "0",
                        "sqrtPrice": "0",
                        "token0Price": "0",
                        "totalValueLockedUSD": "0",
                        "totalValueLockedToken0": "0",
                        "totalValueLockedToken1": "0",
                        "token0": {"id": token0_addr, "decimals": str(dec0), "symbol": token0_sym},
                        "token1": {"id": token1_addr, "decimals": str(dec1), "symbol": token1_sym},
                    },
                    "_protocol_label": "pancake_infinity_bin",
                    "_source": "onchain_pancake_infinity_bin",
                    "_skip_enrich": True,
                }
            )
        except Exception:
            continue
    return out


def _estimate_position_tvl_usd_from_share_external(
    position: dict[str, Any], pool: dict[str, Any], chain_id: int
) -> float | None:
    try:
        pos_liq = _safe_float(position.get("liquidity"))
        pool_liq = _safe_float(pool.get("liquidity"))
        if pos_liq <= 0 or pool_liq <= 0:
            return None
        share = pos_liq / pool_liq
        if share <= 0:
            return None
        tvl0 = _safe_float(pool.get("totalValueLockedToken0"))
        tvl1 = _safe_float(pool.get("totalValueLockedToken1"))
        if tvl0 <= 0 and tvl1 <= 0:
            return None
        t0 = pool.get("token0") or {}
        t1 = pool.get("token1") or {}
        token0 = str(t0.get("id") or "").strip().lower()
        token1 = str(t1.get("id") or "").strip().lower()
        prices = _get_token_prices_usd(int(chain_id), [token0, token1])
        p0 = prices.get(token0)
        p1 = prices.get(token1)
        # If only one token price is available, infer the second from pool token0Price.
        token0_price_in_token1 = _safe_float(pool.get("token0Price"))
        ratio_ok = (
            token0_price_in_token1 > 0
            and token0_price_in_token1 >= (1.0 / POSITIONS_MAX_TOKEN_PRICE_RATIO)
            and token0_price_in_token1 <= POSITIONS_MAX_TOKEN_PRICE_RATIO
        )
        if p0 is None and p1 is not None and ratio_ok:
            p0 = p1 * token0_price_in_token1
        if p1 is None and p0 is not None and ratio_ok:
            p1 = p0 / token0_price_in_token1
        if p0 is None and p1 is None:
            return None
        amount0 = max(0.0, tvl0 * share)
        amount1 = max(0.0, tvl1 * share)
        value = 0.0
        if p0 is not None:
            value += amount0 * float(p0)
        if p1 is not None:
            value += amount1 * float(p1)
        if value <= 0:
            return None
        return value
    except Exception:
        return None


def _query_uniswap_positions_for_owner(
    endpoint: str,
    owner: str,
    *,
    include_pool_liquidity: bool = False,
    include_position_liquidity: bool = True,
    debug_steps: list[dict[str, Any]] | None = None,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    def _build_id_queries(owner_type: str, *, extended: bool = False) -> list[tuple[str, str]]:
        queries: list[tuple[str, str]] = [
            (
                "positions",
                f"""
                query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                  positions(first: 200, skip: $skip, where: {{ owner: $owner }}) {{ id }}
                }}
                """,
            ),
            (
                "positions_in",
                f"""
                query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                  positions(first: 200, skip: $skip, where: {{ owner_in: [$owner] }}) {{ id }}
                }}
                """,
            ),
            (
                "snapshots",
                f"""
                query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                  positionSnapshots(first: 200, skip: $skip, where: {{ owner: $owner }}) {{ position {{ id }} }}
                }}
                """,
            ),
            (
                "snapshots_in",
                f"""
                query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                  positionSnapshots(first: 200, skip: $skip, where: {{ owner_in: [$owner] }}) {{ position {{ id }} }}
                }}
                """,
            ),
        ]
        if extended:
            queries.extend(
                [
                    (
                        "positions_rel",
                        f"""
                        query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                          positions(first: 200, skip: $skip, where: {{ owner_: {{ id: $owner }} }}) {{ id }}
                        }}
                        """,
                    ),
                    (
                        "account",
                        f"""
                        query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                          account(id: $owner) {{ positions(first: 200, skip: $skip) {{ id }} }}
                        }}
                        """,
                    ),
                    (
                        "accounts",
                        f"""
                        query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                          accounts(first: 1, where: {{ id: $owner }}) {{ positions(first: 200, skip: $skip) {{ id }} }}
                        }}
                        """,
                    ),
                    (
                        "snapshots_rel",
                        f"""
                        query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                          positionSnapshots(first: 200, skip: $skip, where: {{ owner_: {{ id: $owner }} }}) {{ position {{ id }} }}
                        }}
                        """,
                    ),
                ]
            )
        return queries

    owner_raw = str(owner or "").strip()
    owner_lc = owner_raw.lower()
    owner_candidates: list[str] = []
    owner_no_prefix = owner_raw[2:] if owner_raw.startswith("0x") else owner_raw
    owner_no_prefix_lc = owner_no_prefix.lower()
    for candidate in (owner_raw, owner_lc, owner_no_prefix, owner_no_prefix_lc):
        if candidate and candidate not in owner_candidates:
            owner_candidates.append(candidate)

    def _run_ids(q: str, mode: str, owner_value: str) -> list[str]:
        out: list[str] = []
        skip = 0
        pages = 0
        while True:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            data = graphql_query(endpoint, q, {"owner": owner_value, "skip": skip}, retries=1)
            payload = data.get("data", {}) or {}
            batch_ids: list[str] = []
            if mode.startswith("positions"):
                for x in (payload.get("positions", []) or []):
                    if isinstance(x, dict) and x.get("id"):
                        batch_ids.append(str(x.get("id")))
            elif mode == "account":
                for x in (((payload.get("account") or {}).get("positions") or [])):
                    if isinstance(x, dict) and x.get("id"):
                        batch_ids.append(str(x.get("id")))
            elif mode == "accounts":
                accounts = payload.get("accounts", []) or []
                first = accounts[0] if accounts else {}
                for x in ((first or {}).get("positions") or []):
                    if isinstance(x, dict) and x.get("id"):
                        batch_ids.append(str(x.get("id")))
            else:
                for s in (payload.get("positionSnapshots", []) or []):
                    pos = (s or {}).get("position") or {}
                    if isinstance(pos, dict) and pos.get("id"):
                        batch_ids.append(str(pos.get("id")))
            out.extend(batch_ids)
            if len(batch_ids) < 200:
                break
            pages += 1
            if pages >= POSITIONS_MAX_PAGES_PER_QUERY:
                break
            skip += 200
            time.sleep(0.15)
        return out

    # Fast profile by default: prefer filters that work across most indexers.
    owner_types_primary = ["ID", "String"]
    if POSITIONS_TRY_BYTES_TYPE:
        owner_types_primary.append("Bytes")
    query_sets_primary = [(x, _build_id_queries(x, extended=False)) for x in owner_types_primary]
    query_sets_extended = [(x, _build_id_queries(x, extended=True)) for x in ["ID", "String", "Bytes"]]
    attempts_count = 0
    found_ids: list[str] = []
    for owner_value in owner_candidates:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        owner_deadline = time.monotonic() + float(POSITIONS_OWNER_MAX_SECONDS)
        if deadline_ts is not None:
            owner_deadline = min(owner_deadline, deadline_ts)

        def _run_query_sets(query_sets: list[tuple[str, list[tuple[str, str]]]]) -> bool:
            nonlocal attempts_count, found_ids
            for owner_type, queries in query_sets:
                if time.monotonic() >= owner_deadline:
                    return False
                for mode, q in queries:
                    if found_ids:
                        return True
                    if time.monotonic() >= owner_deadline:
                        return False
                    if attempts_count >= POSITIONS_MAX_QUERY_ATTEMPTS:
                        return False
                    try:
                        attempts_count += 1
                        ids = _run_ids(q, mode, owner_value)
                        if debug_steps is not None:
                            debug_steps.append(
                                {
                                    "owner_value": owner_value,
                                    "owner_type": owner_type,
                                    "query_mode": mode,
                                    "count": len(ids),
                                    "ok": True,
                                }
                            )
                        if ids:
                            seen = set(found_ids)
                            for pid in ids:
                                if pid not in seen:
                                    seen.add(pid)
                                    found_ids.append(pid)
                            return True
                    except Exception as e:
                        err_text = str(e).lower()
                        if debug_steps is not None:
                            debug_steps.append(
                                {
                                    "owner_value": owner_value,
                                    "owner_type": owner_type,
                                    "query_mode": mode,
                                    "count": 0,
                                    "ok": False,
                                    "error": str(e)[:220],
                                }
                            )
                        # Fast-fail on endpoint auth gating: extra query variants won't help.
                        if ("api key not found" in err_text) or ("auth error" in err_text):
                            return False
            return bool(found_ids)

        _run_query_sets(query_sets_primary)
        if not found_ids and POSITIONS_EXTENDED_QUERY_FALLBACK and time.monotonic() < owner_deadline:
            _run_query_sets(query_sets_extended)
        if found_ids:
            break
        if attempts_count >= POSITIONS_MAX_QUERY_ATTEMPTS:
            break
    if not found_ids:
        return []

    target_ids = found_ids[:400]
    details = _fetch_positions_by_ids_with_detail(
        endpoint,
        target_ids,
        include_pool_liquidity=include_pool_liquidity,
        include_position_liquidity=include_position_liquidity,
    )
    fetched_ids = {str(x.get("id") or "").strip() for x in details if isinstance(x, dict)}
    # Fill missing ids with per-id fetch to keep compatibility with stricter indexers.
    missing_ids = [pid for pid in target_ids if pid not in fetched_ids]
    for pid in missing_ids:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        item = _fetch_position_by_id_with_detail(
            endpoint,
            pid,
            include_pool_liquidity=include_pool_liquidity,
            include_position_liquidity=include_position_liquidity,
        )
        if item:
            details.append(item)
    return details


def _position_has_full_detail(position: dict[str, Any]) -> bool:
    pool = position.get("pool") or {}
    has_ticks = bool((position.get("tickLower") or {}).get("tickIdx") is not None and (position.get("tickUpper") or {}).get("tickIdx") is not None)
    has_pool_price = bool(str(pool.get("sqrtPrice") or "").strip())
    has_decimals = bool((pool.get("token0") or {}).get("decimals") is not None and (pool.get("token1") or {}).get("decimals") is not None)
    return has_ticks and has_pool_price and has_decimals


def _fetch_position_by_id_with_detail(
    endpoint: str,
    position_id: str,
    include_pool_liquidity: bool,
    include_position_liquidity: bool = True,
) -> dict[str, Any] | None:
    pid = str(position_id or "").strip()
    if not pid:
        return None
    pool_liq_field = "liquidity" if include_pool_liquidity else ""
    pos_liq_field = "liquidity" if include_position_liquidity else ""
    detailed_fields = f"""
      id
      {pos_liq_field}
      tickLower {{ tickIdx }}
      tickUpper {{ tickIdx }}
      pool {{
        id
        feeTier
        {pool_liq_field}
        sqrtPrice
        token0Price
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        token0 {{ symbol id decimals }}
        token1 {{ symbol id decimals }}
      }}
    """
    basic_fields = f"""
      id
      {pos_liq_field}
      pool {{
        id
        feeTier
        {pool_liq_field}
        totalValueLockedUSD
        token0Price
        token0 {{ symbol id decimals }}
        token1 {{ symbol id decimals }}
      }}
    """
    for id_type in ("ID", "String"):
        q_detailed = f"""
        query PositionById($id: {id_type}!) {{
          position(id: $id) {{
            {detailed_fields}
          }}
        }}
        """
        try:
            data = graphql_query(endpoint, q_detailed, {"id": pid}, retries=1)
            pos = ((data.get("data") or {}).get("position") or {})
            if pos:
                return pos
        except Exception:
            pass
        q_basic = f"""
        query PositionById($id: {id_type}!) {{
          position(id: $id) {{
            {basic_fields}
          }}
        }}
        """
        try:
            data = graphql_query(endpoint, q_basic, {"id": pid}, retries=1)
            pos = ((data.get("data") or {}).get("position") or {})
            if pos:
                return pos
        except Exception:
            pass
    return None


def _fetch_positions_by_ids_with_detail(
    endpoint: str,
    position_ids: list[str],
    *,
    include_pool_liquidity: bool,
    include_position_liquidity: bool = True,
) -> list[dict[str, Any]]:
    ids = [str(x).strip() for x in (position_ids or []) if str(x).strip()]
    if not ids:
        return []
    pool_liq_field = "liquidity" if include_pool_liquidity else ""
    pos_liq_field = "liquidity" if include_position_liquidity else ""
    detailed_fields = f"""
      id
      {pos_liq_field}
      tickLower {{ tickIdx }}
      tickUpper {{ tickIdx }}
      pool {{
        id
        feeTier
        {pool_liq_field}
        sqrtPrice
        token0Price
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        token0 {{ symbol id decimals }}
        token1 {{ symbol id decimals }}
      }}
    """
    basic_fields = f"""
      id
      {pos_liq_field}
      pool {{
        id
        feeTier
        {pool_liq_field}
        totalValueLockedUSD
        token0Price
        token0 {{ symbol id decimals }}
        token1 {{ symbol id decimals }}
      }}
    """

    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    chunks = [ids[i : i + 80] for i in range(0, len(ids), 80)]
    for chunk in chunks:
        for list_type in ("ID", "String"):
            q_detailed = f"""
            query PositionsByIds($ids: [{list_type}!]!) {{
              positions(first: 200, where: {{ id_in: $ids }}) {{
                {detailed_fields}
              }}
            }}
            """
            try:
                data = graphql_query(endpoint, q_detailed, {"ids": chunk}, retries=1)
                rows = ((data.get("data") or {}).get("positions") or [])
                if rows:
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        pid = str(r.get("id") or "").strip()
                        if pid and pid not in seen:
                            seen.add(pid)
                            out.append(r)
                    break
            except Exception:
                pass
            q_basic = f"""
            query PositionsByIds($ids: [{list_type}!]!) {{
              positions(first: 200, where: {{ id_in: $ids }}) {{
                {basic_fields}
              }}
            }}
            """
            try:
                data = graphql_query(endpoint, q_basic, {"ids": chunk}, retries=1)
                rows = ((data.get("data") or {}).get("positions") or [])
                if rows:
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        pid = str(r.get("id") or "").strip()
                        if pid and pid not in seen:
                            seen.add(pid)
                            out.append(r)
                    break
            except Exception:
                pass
    return out


def _endpoint_supports_uniswap_positions(endpoint: str) -> bool:
    cached = _POSITION_SCHEMA_SUPPORT_CACHE.get(endpoint)
    if cached is not None:
        return cached
    query = """
    query PositionFields {
      __type(name: "Position") {
        fields { name }
      }
    }
    """
    ok = True
    try:
        data = graphql_query(endpoint, query, {}, retries=1)
        fields = ((data.get("data") or {}).get("__type") or {}).get("fields") or []
        if fields:
            names = {str(x.get("name") or "") for x in fields}
            ok = ("pool" in names)
        else:
            # Some providers restrict introspection. Treat as unknown/supportive
            # and try real position queries instead of skipping silently.
            ok = True
    except Exception:
        # Introspection failure is not enough to conclude unsupported schema.
        ok = True
    _POSITION_SCHEMA_SUPPORT_CACHE[endpoint] = ok
    return ok


def _endpoint_supports_position_liquidity(endpoint: str) -> bool:
    cached = _POSITION_LIQUIDITY_SCHEMA_SUPPORT_CACHE.get(endpoint)
    if cached is not None:
        return cached
    query = """
    query PositionFields {
      __type(name: "Position") {
        fields { name }
      }
    }
    """
    ok = False
    try:
        data = graphql_query(endpoint, query, {}, retries=1)
        fields = ((data.get("data") or {}).get("__type") or {}).get("fields") or []
        names = {str(x.get("name") or "") for x in fields}
        ok = ("liquidity" in names)
    except Exception:
        ok = False
    _POSITION_LIQUIDITY_SCHEMA_SUPPORT_CACHE[endpoint] = ok
    return ok


def _endpoint_supports_pool_liquidity(endpoint: str) -> bool:
    cached = _POOL_LIQUIDITY_SCHEMA_SUPPORT_CACHE.get(endpoint)
    if cached is not None:
        return cached
    query = """
    query PoolFields {
      __type(name: "Pool") {
        fields { name }
      }
    }
    """
    ok = False
    try:
        data = graphql_query(endpoint, query, {}, retries=1)
        fields = ((data.get("data") or {}).get("__type") or {}).get("fields") or []
        names = {str(x.get("name") or "") for x in fields}
        ok = ("liquidity" in names)
    except Exception:
        ok = False
    _POOL_LIQUIDITY_SCHEMA_SUPPORT_CACHE[endpoint] = ok
    return ok


def _scan_pool_positions_chain(
    chain_id: int,
    addresses: list[str],
    deadline_ts: float,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    timed_out = False
    chain_key = CHAIN_ID_TO_KEY.get(int(chain_id), "")
    if not chain_key:
        return rows, errors, debug_rows, timed_out

    def _scan_pool_positions_owner(
        owner: str,
        *,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        has_position_liquidity: bool,
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, Any], bool]:
        owner_rows: list[dict[str, Any]] = []
        owner_errors: list[str] = []
        owner_attempts: list[dict[str, Any]] = []
        positions: list[dict[str, Any]] = []
        graph_failed = False
        owner_timed_out = time.monotonic() >= deadline_ts
        if owner_timed_out:
            return owner_rows, owner_errors, {
                "chain": chain_key,
                "version": version,
                "owner": owner,
                "positions_found": 0,
                "failed": False,
                "attempts": owner_attempts,
            }, True

        if (
            version == "v3"
            and int(chain_id) in UNISWAP_V3_NPM_BY_CHAIN_ID
            and int(chain_id) in POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS
        ):
            try:
                onchain_prefetch = _scan_v3_positions_onchain(
                    owner,
                    int(chain_id),
                    deadline_ts=deadline_ts,
                    protocol_label=V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(int(chain_id), "uniswap_v3"),
                    source_tag="onchain_prefetch",
                )
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_v3_prefetch",
                        "count": len(onchain_prefetch),
                        "ok": True,
                    }
                )
                if onchain_prefetch:
                    positions = onchain_prefetch
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_v3_prefetch",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )

        # Pancake v3 farming positions are often staked in MasterChefV3 and
        # therefore invisible in PositionManager owner lists.
        if version == "v3" and int(chain_id) in PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID:
            try:
                farm_positions = _scan_pancake_staked_v3_positions_onchain(
                    owner,
                    int(chain_id),
                    deadline_ts=deadline_ts,
                )
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_masterchef_v3",
                        "count": len(farm_positions),
                        "ok": True,
                    }
                )
                if farm_positions:
                    seen = {str(x.get("id") or "") for x in positions if isinstance(x, dict)}
                    for p in farm_positions:
                        pid = str((p or {}).get("id") or "")
                        if pid and pid in seen:
                            continue
                        if pid:
                            seen.add(pid)
                        positions.append(p)
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_masterchef_v3",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )

        # Pancake Infinity CL positions live in a dedicated position manager and are
        # not visible through Uniswap-v3/v4 subgraph queries.
        if version == "v3" and int(chain_id) in PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID:
            try:
                infinity_cl_positions = _scan_pancake_infinity_cl_positions_onchain(
                    owner,
                    int(chain_id),
                    deadline_ts=deadline_ts,
                )
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_infinity_cl",
                        "count": len(infinity_cl_positions),
                        "ok": True,
                    }
                )
                if infinity_cl_positions:
                    seen = {str(x.get("id") or "") for x in positions if isinstance(x, dict)}
                    for p in infinity_cl_positions:
                        pid = str((p or {}).get("id") or "")
                        if pid and pid in seen:
                            continue
                        if pid:
                            seen.add(pid)
                        positions.append(p)
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_infinity_cl",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )

        if version == "v3" and int(chain_id) in PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID:
            try:
                infinity_bin_positions = _scan_pancake_infinity_bin_positions_onchain(
                    owner,
                    int(chain_id),
                    deadline_ts=deadline_ts,
                )
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_infinity_bin",
                        "count": len(infinity_bin_positions),
                        "ok": True,
                    }
                )
                if infinity_bin_positions:
                    seen = {str(x.get("id") or "") for x in positions if isinstance(x, dict)}
                    for p in infinity_bin_positions:
                        pid = str((p or {}).get("id") or "")
                        if pid and pid in seen:
                            continue
                        if pid:
                            seen.add(pid)
                        positions.append(p)
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_infinity_bin",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )

        if endpoint and not positions:
            try:
                positions = _query_uniswap_positions_for_owner(
                    endpoint,
                    owner,
                    include_pool_liquidity=has_pool_liquidity,
                    include_position_liquidity=has_position_liquidity,
                    debug_steps=owner_attempts,
                    deadline_ts=deadline_ts,
                )
                for _p in positions:
                    if isinstance(_p, dict) and not str(_p.get("_source") or "").strip():
                        _p["_source"] = "graph_query"
                if not positions and not has_position_liquidity:
                    positions = _query_uniswap_positions_for_owner(
                        endpoint,
                        owner,
                        include_pool_liquidity=has_pool_liquidity,
                        include_position_liquidity=True,
                        debug_steps=owner_attempts,
                        deadline_ts=deadline_ts,
                    )
                    for _p in positions:
                        if isinstance(_p, dict) and not str(_p.get("_source") or "").strip():
                            _p["_source"] = "graph_query"
            except Exception as e:
                graph_failed = True
                owner_errors.append(f"Pool scan failed [{chain_key}/{version}] for {owner}: {e}")
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "fallback",
                        "query_mode": "graph_exception",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )
        if version == "v3" and (graph_failed or not positions):
            if time.monotonic() < deadline_ts:
                try:
                    onchain_positions = _scan_v3_positions_onchain(
                        owner,
                        int(chain_id),
                        deadline_ts=deadline_ts,
                        include_price_details=(int(chain_id) in POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS),
                            protocol_label=V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(int(chain_id), "uniswap_v3"),
                        source_tag="onchain_fallback",
                    )
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "onchain_v3_npm",
                            "count": len(onchain_positions),
                            "ok": True,
                        }
                    )
                    if onchain_positions:
                        positions = onchain_positions
                except Exception as e:
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "onchain_v3_npm",
                            "count": 0,
                            "ok": False,
                            "error": str(e)[:220],
                        }
                    )
        owner_debug = {
            "chain": chain_key,
            "version": version,
            "owner": owner,
            "positions_found": len(positions),
            "failed": False,
            "attempts": owner_attempts,
        }
        onchain_ids = {
            str((x or {}).get("id") or "")
            for x in positions
            if isinstance(x, dict) and str((x or {}).get("_source") or "").startswith("onchain")
        }
        graph_ids = {
            str((x or {}).get("id") or "")
            for x in positions
            if isinstance(x, dict) and str((x or {}).get("_source") or "").startswith("graph")
        }
        graph_checked = any(
            str((a or {}).get("owner_type") or "").upper() in {"ID", "STRING", "BYTES"}
            and str((a or {}).get("query_mode") or "").startswith(("positions", "snapshots", "account", "accounts"))
            for a in owner_attempts
            if isinstance(a, dict)
        )
        owner_debug["compare"] = {
            "graph_checked": bool(graph_checked),
            "onchain_count": len(onchain_ids),
            "graph_count": len(graph_ids),
            "only_onchain_count": len(onchain_ids - graph_ids) if graph_checked else 0,
            "only_graph_count": len(graph_ids - onchain_ids) if graph_checked else 0,
            "only_onchain_ids": sorted([x for x in (onchain_ids - graph_ids) if x][:5]) if graph_checked else [],
            "only_graph_ids": sorted([x for x in (graph_ids - onchain_ids) if x][:5]) if graph_checked else [],
        }

        for p in positions:
            try:
                if not isinstance(p, dict):
                    continue
                if not _position_has_full_detail(p) and not bool(p.get("_skip_enrich")):
                    enriched = _fetch_position_by_id_with_detail(
                        endpoint,
                        str(p.get("id") or ""),
                        include_pool_liquidity=has_pool_liquidity,
                    )
                    if enriched:
                        p = enriched
                liq_raw = p.get("liquidity")
                if liq_raw not in (None, "") and _safe_float(liq_raw) <= 0:
                    continue
                pool = p.get("pool") or {}
                tvl_usd = _safe_float(pool.get("totalValueLockedUSD"))
                valuation_mode = "exact-external"
                detailed_external_tvl = _estimate_position_tvl_usd_from_detail_external(p, pool, int(chain_id))
                if detailed_external_tvl is None or detailed_external_tvl <= 0:
                    position_tvl_usd = None
                    valuation_mode = "no-exact-external"
                else:
                    position_tvl_usd = detailed_external_tvl
                fee_raw = str(pool.get("feeTier") or "").strip()
                fee_disp = fee_raw
                try:
                    fee_int = int(fee_raw)
                    if fee_int > 0:
                        fee_disp = f"{fee_int / 10000.0:.2f}%"
                except Exception:
                    fee_disp = fee_raw or "-"
                t0_obj = pool.get("token0") or {}
                t1_obj = pool.get("token1") or {}
                t0 = _token_display_symbol(int(chain_id), chain_key, t0_obj)
                t1 = _token_display_symbol(int(chain_id), chain_key, t1_obj)
                # Prefer exact-external, then exact-subgraph, then share-external fallback.
                if detailed_external_tvl is None or detailed_external_tvl <= 0:
                    if version == "v3":
                        onchain_enriched = _fetch_v3_position_onchain_by_token_id(
                            int(chain_id),
                            str(p.get("id") or ""),
                            include_price_details=True,
                            protocol_label=str(p.get("_protocol_label") or f"uniswap_{version}"),
                        )
                        if onchain_enriched:
                            p = onchain_enriched
                            pool = p.get("pool") or {}
                            t0_obj = pool.get("token0") or {}
                            t1_obj = pool.get("token1") or {}
                            t0 = _token_display_symbol(int(chain_id), chain_key, t0_obj)
                            t1 = _token_display_symbol(int(chain_id), chain_key, t1_obj)
                            detailed_external_tvl = _estimate_position_tvl_usd_from_detail_external(p, pool, int(chain_id))
                            if detailed_external_tvl is not None and detailed_external_tvl > 0:
                                position_tvl_usd = detailed_external_tvl
                                valuation_mode = "exact-external-onchain"
                    if detailed_external_tvl is not None and detailed_external_tvl > 0:
                        pass
                    else:
                        detailed_subgraph_tvl = _estimate_position_tvl_usd_from_detail(p, pool)
                        if detailed_subgraph_tvl is not None and detailed_subgraph_tvl > 0:
                            position_tvl_usd = detailed_subgraph_tvl
                            valuation_mode = "exact-subgraph"
                        else:
                            share_external_tvl = _estimate_position_tvl_usd_from_share_external(p, pool, int(chain_id))
                            if share_external_tvl is not None and share_external_tvl > 0:
                                position_tvl_usd = share_external_tvl
                                valuation_mode = "estimated-share-external"
                if position_tvl_usd is not None and position_tvl_usd > 0 and tvl_usd > 0:
                    # A single position cannot reasonably exceed the whole pool TVL by large margin.
                    pool_cap = float(tvl_usd) * 1.25
                    if position_tvl_usd > pool_cap:
                        share_sanity_tvl = _estimate_position_tvl_usd_from_share_external(p, pool, int(chain_id))
                        if share_sanity_tvl is not None and share_sanity_tvl > 0 and share_sanity_tvl <= pool_cap:
                            position_tvl_usd = share_sanity_tvl
                            valuation_mode = "sanity-share"
                        else:
                            position_tvl_usd = pool_cap
                            valuation_mode = "sanity-cap"
                suspected_spam = _is_suspected_spam_pair(chain_key, t0_obj, t1_obj, t0, t1, position_tvl_usd)
                if suspected_spam and position_tvl_usd is not None and position_tvl_usd > POSITIONS_SPAM_MAX_TVL_USD:
                    # Prevent clearly manipulated valuations from dominating output for suspicious pairs.
                    position_tvl_usd = float(POSITIONS_SPAM_MAX_TVL_USD)
                    valuation_mode = "spam-capped"
                owner_rows.append(
                    {
                        "address": owner,
                        "protocol": str(p.get("_protocol_label") or f"uniswap_{version}"),
                        "chain": chain_key,
                        "chain_id": int(chain_id),
                        "kind": "pool",
                        "pool_id": str(pool.get("id") or ""),
                        "pair": f"{t0}/{t1}",
                        "position_id": str(p.get("id") or ""),
                        "position_ids": [str(p.get("id") or "")] if str(p.get("id") or "").strip() else [],
                        "fee_tier": fee_disp,
                        "fee_tier_raw": fee_raw,
                        "liquidity": str(p.get("liquidity") or "0"),
                        "pool_liquidity": str(pool.get("liquidity") or "0"),
                        "pool_tvl_usd": tvl_usd,
                        "tvl_usd": position_tvl_usd,
                        "valuation_mode": valuation_mode,
                        "suspected_spam": bool(suspected_spam),
                    }
                )
            except Exception as e:
                if POSITIONS_DEBUG_ERRORS:
                    owner_errors.append(f"Pool row skipped [{chain_key}/{version}] for {owner}: {e}")
                continue
            if time.monotonic() >= deadline_ts:
                owner_timed_out = True
                break
        return owner_rows, owner_errors, owner_debug, owner_timed_out

    for version in ("v3", "v4"):
        if time.monotonic() >= deadline_ts:
            timed_out = True
            break
        endpoint = get_graph_endpoint(chain_key, version=version)
        if not endpoint and version != "v3":
            continue
        if endpoint and not _endpoint_supports_uniswap_positions(endpoint):
            # Keep v3 flow alive via on-chain fallback even if endpoint introspection says unsupported.
            if version != "v3":
                continue
        has_pool_liquidity = _endpoint_supports_pool_liquidity(endpoint) if endpoint else False
        has_position_liquidity = _endpoint_supports_position_liquidity(endpoint) if endpoint else True
        owner_workers = max(1, min(int(POSITIONS_ADDRESS_PARALLEL_WORKERS), len(addresses)))
        if owner_workers <= 1 or len(addresses) <= 1:
            for owner in addresses:
                if time.monotonic() >= deadline_ts:
                    timed_out = True
                    break
                owner_rows, owner_errors, owner_debug, owner_timed_out = _scan_pool_positions_owner(
                    owner,
                    version=version,
                    endpoint=endpoint,
                    has_pool_liquidity=has_pool_liquidity,
                    has_position_liquidity=has_position_liquidity,
                )
                rows.extend(owner_rows)
                errors.extend(owner_errors)
                debug_rows.append(owner_debug)
                if owner_timed_out:
                    timed_out = True
                    break
        else:
            with ThreadPoolExecutor(max_workers=owner_workers) as owner_executor:
                owner_futures = [
                    owner_executor.submit(
                        _scan_pool_positions_owner,
                        owner,
                        version=version,
                        endpoint=endpoint,
                        has_pool_liquidity=has_pool_liquidity,
                        has_position_liquidity=has_position_liquidity,
                    )
                    for owner in addresses
                ]
                for owner_fut in as_completed(owner_futures):
                    try:
                        owner_rows, owner_errors, owner_debug, owner_timed_out = owner_fut.result()
                    except Exception as e:
                        errors.append(f"Pool owner worker failed [{chain_key}/{version}]: {e}")
                        continue
                    rows.extend(owner_rows)
                    errors.extend(owner_errors)
                    debug_rows.append(owner_debug)
                    if owner_timed_out:
                        timed_out = True
                    if time.monotonic() >= deadline_ts:
                        timed_out = True
                        break
        if timed_out:
            break
    return rows, errors, debug_rows, timed_out


def _scan_pool_positions(addresses: list[str], chain_ids: list[int]) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    deadline_ts = time.monotonic() + float(POSITIONS_SCAN_MAX_SECONDS)
    timed_out = False

    valid_chain_ids = [int(cid) for cid in chain_ids if CHAIN_ID_TO_KEY.get(int(cid), "")]
    max_workers = max(1, min(int(POSITIONS_PARALLEL_WORKERS), len(valid_chain_ids)))

    if max_workers <= 1 or len(valid_chain_ids) <= 1:
        for chain_id in valid_chain_ids:
            if time.monotonic() >= deadline_ts:
                timed_out = True
                break
            chain_rows, chain_errors, chain_debug, chain_timed_out = _scan_pool_positions_chain(
                chain_id,
                addresses,
                deadline_ts,
            )
            rows.extend(chain_rows)
            errors.extend(chain_errors)
            debug_rows.extend(chain_debug)
            if chain_timed_out:
                timed_out = True
                break
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_scan_pool_positions_chain, chain_id, addresses, deadline_ts)
                for chain_id in valid_chain_ids
            ]
            for fut in as_completed(futures):
                try:
                    chain_rows, chain_errors, chain_debug, chain_timed_out = fut.result()
                except Exception as e:
                    errors.append(f"Pool scan worker failed: {e}")
                    continue
                rows.extend(chain_rows)
                errors.extend(chain_errors)
                debug_rows.extend(chain_debug)
                if chain_timed_out:
                    timed_out = True
                if time.monotonic() >= deadline_ts:
                    timed_out = True

    # Aggregate by owner/protocol/pool id (wallet can hold multiple NFT positions in one pool).
    uniq: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("address")), str(row.get("protocol")), str(row.get("pool_id")))
        if key not in uniq:
            base = dict(row)
            base["position_count"] = 1
            uniq[key] = base
            continue
        acc = uniq[key]
        acc["position_count"] = int(acc.get("position_count") or 1) + 1
        acc_ids = [str(x) for x in (acc.get("position_ids") or []) if str(x).strip()]
        row_ids = [str(x) for x in (row.get("position_ids") or []) if str(x).strip()]
        if row_ids:
            seen_ids = set(acc_ids)
            for pid in row_ids:
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    acc_ids.append(pid)
            acc["position_ids"] = acc_ids
        cur_tvl = acc.get("tvl_usd")
        row_tvl = row.get("tvl_usd")
        if cur_tvl is None:
            acc["tvl_usd"] = row_tvl
        elif row_tvl is not None:
            acc["tvl_usd"] = _safe_float(cur_tvl) + _safe_float(row_tvl)
        m1 = str(acc.get("valuation_mode") or "")
        m2 = str(row.get("valuation_mode") or "")
        if m1 and m2 and m1 != m2:
            acc["valuation_mode"] = "mixed"
        elif not m1 and m2:
            acc["valuation_mode"] = m2
        # Keep pool level metadata stable; liquidity fields are not additive across NFTs.
    dedup_errors = list(dict.fromkeys(errors))
    if timed_out:
        dedup_errors.append(
            f"Pool scan timed out after {POSITIONS_SCAN_MAX_SECONDS}s. Showing partial results."
        )
    return list(uniq.values()), dedup_errors, debug_rows


def _fetch_pool_tvl_series(chain_key: str, version: str, pool_id: str, days: int) -> list[tuple[int, float]]:
    endpoint = get_graph_endpoint(chain_key, version=version)
    if not endpoint:
        return []
    now_ts = int(time.time())
    since_ts = now_ts - max(1, int(days)) * 86400
    vars_base = {"pool": str(pool_id or "").strip().lower(), "since": int(since_ts)}
    attempts = [
        """
        query PoolDays($pool: String!, $since: Int!) {
          poolDayDatas(
            first: 400
            orderBy: date
            orderDirection: asc
            where: { pool: $pool, date_gte: $since }
          ) {
            date
            tvlUSD
          }
        }
        """,
        """
        query PoolDays($pool: String!, $since: Int!) {
          poolDayDatas(
            first: 400
            orderBy: date
            orderDirection: asc
            where: { pool: $pool, date_gte: $since }
          ) {
            date
            totalValueLockedUSD
          }
        }
        """,
    ]
    for q in attempts:
        try:
            data = graphql_query(endpoint, q, vars_base, retries=1)
            rows = (data.get("data") or {}).get("poolDayDatas") or []
            out: list[tuple[int, float]] = []
            for r in rows:
                ts = int(r.get("date") or 0)
                if ts <= 0:
                    continue
                tvl = _safe_float(r.get("tvlUSD"))
                if tvl <= 0:
                    tvl = _safe_float(r.get("totalValueLockedUSD"))
                if tvl <= 0:
                    continue
                out.append((ts, tvl))
            if out:
                return out
        except Exception:
            continue
    return []


def _chain_id_by_chain_key(chain_key: str) -> int:
    key = str(chain_key or "").strip().lower()
    for cid, ck in CHAIN_ID_TO_KEY.items():
        if str(ck).strip().lower() == key:
            return int(cid)
    return 0


def _fetch_position_snapshot_series_exact(
    endpoint: str,
    position_id: str,
    *,
    since_ts: int,
    chain_id: int,
) -> list[tuple[int, float]]:
    pid = str(position_id or "").strip()
    if not pid:
        return []
    queries = [
        (
            "ID",
            """
            query Snapshots($pid: ID!, $since: Int!) {
              positionSnapshots(
                first: 500,
                orderBy: timestamp,
                orderDirection: asc,
                where: { position: $pid, timestamp_gte: $since }
              ) {
                timestamp
                liquidity
                position { tickLower { tickIdx } tickUpper { tickIdx } }
                pool { sqrtPrice token0Price token0 { id decimals } token1 { id decimals } }
              }
            }
            """,
        ),
        (
            "String",
            """
            query Snapshots($pid: String!, $since: Int!) {
              positionSnapshots(
                first: 500,
                orderBy: timestamp,
                orderDirection: asc,
                where: { position: $pid, timestamp_gte: $since }
              ) {
                timestamp
                liquidity
                position { tickLower { tickIdx } tickUpper { tickIdx } }
                pool { sqrtPrice token0Price token0 { id decimals } token1 { id decimals } }
              }
            }
            """,
        ),
        (
            "ID",
            """
            query Snapshots($pid: ID!, $since: Int!) {
              positionSnapshots(
                first: 500,
                orderBy: timestamp,
                orderDirection: asc,
                where: { position_: { id: $pid }, timestamp_gte: $since }
              ) {
                timestamp
                liquidity
                position { tickLower { tickIdx } tickUpper { tickIdx } }
                pool { sqrtPrice token0Price token0 { id decimals } token1 { id decimals } }
              }
            }
            """,
        ),
    ]
    rows: list[dict[str, Any]] = []
    for _typ, q in queries:
        try:
            data = graphql_query(endpoint, q, {"pid": pid, "since": int(since_ts)}, retries=1)
            rows = ((data.get("data") or {}).get("positionSnapshots") or [])
            if rows:
                break
        except Exception:
            continue
    if not rows:
        return []

    # Keep only the last snapshot per day for this position.
    by_day: dict[int, tuple[int, float]] = {}
    for s in rows:
        try:
            ts = int((s or {}).get("timestamp") or 0)
            if ts <= 0:
                continue
            liq = str((s or {}).get("liquidity") or "").strip()
            pos = (s or {}).get("position") or {}
            pool = (s or {}).get("pool") or {}
            if not liq:
                continue
            p = {
                "liquidity": liq,
                "tickLower": (pos.get("tickLower") or {}),
                "tickUpper": (pos.get("tickUpper") or {}),
            }
            val = _estimate_position_tvl_usd_from_detail_external(p, pool, int(chain_id))
            if val is None or val <= 0:
                continue
            day_ts = (ts // 86400) * 86400
            prev = by_day.get(day_ts)
            if prev is None or ts > prev[0]:
                by_day[day_ts] = (ts, float(val))
        except Exception:
            continue
    out = [(day_ts, val) for day_ts, (_real_ts, val) in by_day.items()]
    out.sort(key=lambda x: x[0])
    return out


def _aave_markets_for_chains(chain_ids: list[int]) -> list[dict[str, Any]]:
    if not chain_ids:
        return []
    query = """
    query AaveMarkets($ids: [ChainId!]!) {
      markets(request: { chainIds: $ids }) {
        address
        chain { chainId name }
        name
      }
    }
    """
    data = graphql_query(AAVE_V3_GRAPHQL_ENDPOINT, query, {"ids": [int(x) for x in chain_ids]}, retries=1)
    markets = data.get("data", {}).get("markets", []) or []
    out: list[dict[str, Any]] = []
    for m in markets:
        addr = str(m.get("address") or "").strip()
        chain_id = int((m.get("chain") or {}).get("chainId") or 0)
        if not _is_eth_address(addr) or chain_id <= 0:
            continue
        out.append({"address": addr, "chainId": chain_id, "name": str(m.get("name") or "")})
    return out


def _scan_aave_positions(addresses: list[str], chain_ids: list[int]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    supported_ids = [int(x) for x in chain_ids if int(x) in AAVE_CHAIN_ID_TO_NAME]
    if not supported_ids:
        return rows, errors
    try:
        markets = _aave_markets_for_chains(supported_ids)
    except Exception as e:
        return rows, [f"Lending scan failed (markets): {e}"]
    if not markets:
        return rows, errors
    query = """
    query AaveUserPositions($user: EvmAddress!, $markets: [MarketInput!]!) {
      userSupplies(request: { user: $user, markets: $markets, collateralsOnly: false, orderBy: { balance: DESC } }) {
        market { name chain { chainId name } }
        currency { symbol }
        balance { amount { value } usd }
        apy { value }
        isCollateral
      }
      userBorrows(request: { user: $user, markets: $markets, orderBy: { debt: DESC } }) {
        market { name chain { chainId name } }
        currency { symbol }
        debt { amount { value } usd }
        apy { value }
      }
    }
    """
    for owner in addresses:
        try:
            data = graphql_query(
                AAVE_V3_GRAPHQL_ENDPOINT,
                query,
                {"user": owner, "markets": [{"address": m["address"], "chainId": m["chainId"]} for m in markets]},
                retries=1,
            )
        except Exception as e:
            errors.append(f"Lending scan failed for {owner}: {e}")
            continue
        payload = data.get("data", {}) or {}
        for s in payload.get("userSupplies", []) or []:
            usd = _safe_float((s.get("balance") or {}).get("usd"))
            if usd <= 0:
                continue
            chain_obj = ((s.get("market") or {}).get("chain") or {})
            chain_id = int(chain_obj.get("chainId") or 0)
            rows.append(
                {
                    "address": owner,
                    "protocol": "aave_v3",
                    "chain": str(chain_obj.get("name") or AAVE_CHAIN_ID_TO_NAME.get(chain_id, chain_id)),
                    "chain_id": chain_id,
                    "kind": "lending_supply",
                    "market": str((s.get("market") or {}).get("name") or ""),
                    "asset": str((s.get("currency") or {}).get("symbol") or ""),
                    "amount": _safe_float(((s.get("balance") or {}).get("amount") or {}).get("value")),
                    "usd": usd,
                    "apy": _safe_float((s.get("apy") or {}).get("value")),
                    "is_collateral": bool(s.get("isCollateral")),
                }
            )
        for b in payload.get("userBorrows", []) or []:
            usd = _safe_float((b.get("debt") or {}).get("usd"))
            if usd <= 0:
                continue
            chain_obj = ((b.get("market") or {}).get("chain") or {})
            chain_id = int(chain_obj.get("chainId") or 0)
            rows.append(
                {
                    "address": owner,
                    "protocol": "aave_v3",
                    "chain": str(chain_obj.get("name") or AAVE_CHAIN_ID_TO_NAME.get(chain_id, chain_id)),
                    "chain_id": chain_id,
                    "kind": "lending_borrow",
                    "market": str((b.get("market") or {}).get("name") or ""),
                    "asset": str((b.get("currency") or {}).get("symbol") or ""),
                    "amount": _safe_float(((b.get("debt") or {}).get("amount") or {}).get("value")),
                    "usd": usd,
                    "apy": _safe_float((b.get("apy") or {}).get("value")),
                    "is_collateral": False,
                }
            )
    return rows, errors


def _scan_aave_merit_rewards(addresses: list[str], chain_ids: list[int]) -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    supported_ids = [int(x) for x in chain_ids if int(x) in AAVE_CHAIN_ID_TO_NAME]
    if not supported_ids:
        return rows, errors
    query = """
    query MeritRewards($user: EvmAddress!, $chainId: ChainId!) {
      userMeritRewards(request: { user: $user, chainId: $chainId }) {
        chain
        claimable {
          currency { symbol }
          amount { amount { value } usd }
        }
      }
    }
    """
    for owner in addresses:
        for chain_id in supported_ids:
            try:
                data = graphql_query(
                    AAVE_V3_GRAPHQL_ENDPOINT,
                    query,
                    {"user": owner, "chainId": int(chain_id)},
                    retries=1,
                )
            except Exception:
                continue
            payload = ((data.get("data") or {}).get("userMeritRewards") or {})
            claimable = payload.get("claimable") or []
            for item in claimable:
                amount_obj = item.get("amount") or {}
                usd = _safe_float(amount_obj.get("usd"))
                amount = _safe_float((amount_obj.get("amount") or {}).get("value"))
                if usd <= 0 and amount <= 0:
                    continue
                rows.append(
                    {
                        "address": owner,
                        "protocol": "aave_v3",
                        "chain": AAVE_CHAIN_ID_TO_NAME.get(int(chain_id), str(chain_id)),
                        "chain_id": int(chain_id),
                        "asset": str((item.get("currency") or {}).get("symbol") or ""),
                        "amount": amount,
                        "usd": usd,
                    }
                )
    return rows, errors


def _parse_pairs_str(raw: str) -> list[tuple[str, str]]:
    out = []
    seen = set()
    for part in (raw or "").replace(" ", "").lower().split(";"):
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        a, b = a.strip(), b.strip()
        if not a or not b or a == b:
            continue
        key = (a, b)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _pairs_to_string(pairs: list[tuple[str, str]]) -> str:
    return ";".join(f"{a},{b}" for a, b in pairs)


def _supported_tokens() -> list[str]:
    tokens = set()
    for per_chain in TOKEN_ADDRESSES.values():
        tokens.update(per_chain.keys())
    return sorted(tokens)


def _supported_chains() -> list[str]:
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(UNISWAP_V4_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    return sorted(chains)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_utc_iso(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        import json

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _write_json(path: Path, data: dict[str, Any]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def _new_session_id() -> str:
    return secrets.token_urlsafe(24)


def _is_valid_session_id(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9_-]{16,128}", value or ""))


def _ensure_session_cookie(request: Request, response: Response) -> str:
    sid = request.cookies.get(SESSION_COOKIE_NAME, "")
    if _is_valid_session_id(sid):
        return sid
    sid = _new_session_id()
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=sid,
        max_age=SESSION_TTL_SEC,
        httponly=True,
        samesite="lax",
        secure=(request.url.scheme == "https"),
        path="/",
    )
    return sid


def _load_chain_catalog(refresh: bool = False) -> dict[str, Any]:
    if not refresh:
        cached = _read_json(CHAIN_CATALOG_PATH)
        if cached and isinstance(cached.get("items"), list):
            return cached
    items = _supported_chains()
    out = {"updated_at": _iso_now(), "count": len(items), "items": items}
    _write_json(CHAIN_CATALOG_PATH, out)
    return out


def _is_clean_symbol(symbol: str) -> bool:
    # keep only plain alnum symbols to avoid spam/malicious names
    return bool(re.fullmatch(r"[A-Za-z0-9]{2,20}", symbol))


def _fetch_uniswap_verified_tokens() -> tuple[set[str], dict[str, list[str]]]:
    import json

    with urlopen(UNISWAP_TOKEN_LIST_URL, timeout=20) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    supported = set(_supported_chains())
    by_chain_set: dict[str, set[str]] = {}
    all_tokens: set[str] = set()
    for item in payload.get("tokens", []):
        symbol = str(item.get("symbol") or "").strip()
        if not _is_clean_symbol(symbol):
            continue
        chain_name = CHAIN_ID_TO_NAME.get(int(item.get("chainId") or 0))
        if not chain_name or chain_name not in supported:
            continue
        sym = symbol.lower()
        by_chain_set.setdefault(chain_name, set()).add(sym)
        all_tokens.add(sym)

    by_chain = {k: sorted(v) for k, v in by_chain_set.items()}
    return all_tokens, by_chain


def _fetch_tokens_by_tvl_endpoint(endpoint: str, min_tvl_usd: float) -> set[str]:
    query = """
    query TokenListByTvl($minTvl: BigDecimal!, $skip: Int!) {
      tokens(
        first: 1000,
        skip: $skip,
        orderBy: totalValueLockedUSD,
        orderDirection: desc,
        where: { totalValueLockedUSD_gte: $minTvl }
      ) {
        symbol
      }
    }
    """
    out: set[str] = set()
    skip = 0
    while skip < 10000:
        data = graphql_query(endpoint, query, {"minTvl": str(min_tvl_usd), "skip": skip})
        rows = data.get("data", {}).get("tokens", [])
        if not rows:
            break
        for row in rows:
            sym = str(row.get("symbol") or "").strip()
            if _is_clean_symbol(sym):
                out.add(sym.lower())
        if len(rows) < 1000:
            break
        skip += 1000
    return out


def _fetch_tokens_by_chain_tvl(min_tvl_usd: float) -> tuple[set[str], dict[str, list[str]]]:
    all_tokens: set[str] = set()
    by_chain: dict[str, set[str]] = {}
    for chain in _supported_chains():
        chain_set: set[str] = set()
        for version in ("v3", "v4"):
            endpoint = get_graph_endpoint(chain, version)
            if not endpoint:
                continue
            try:
                chain_set.update(_fetch_tokens_by_tvl_endpoint(endpoint, min_tvl_usd))
            except Exception:
                continue
        if chain_set:
            by_chain[chain] = chain_set
            all_tokens.update(chain_set)
    return all_tokens, {k: sorted(v) for k, v in by_chain.items()}


def _load_token_catalog(refresh: bool = False) -> dict[str, Any]:
    cached = _read_json(TOKEN_CATALOG_PATH)
    if not refresh and cached and isinstance(cached.get("items"), list):
        try:
            cached_min_tvl = float(cached.get("min_tvl_usd") or 0)
        except (TypeError, ValueError):
            cached_min_tvl = 0.0
        # Rebuild cache automatically when threshold changed (e.g. 10k -> 1M).
        if abs(cached_min_tvl - TOKENS_MIN_TVL_USD) < 1e-9:
            return cached

    by_chain: dict[str, list[str]] = {}
    all_tokens: set[str] = set()
    source = "tokens-tvl-threshold"
    try:
        all_tokens, by_chain = _fetch_tokens_by_chain_tvl(TOKENS_MIN_TVL_USD)
        try:
            verified_all, verified_by_chain = _fetch_uniswap_verified_tokens()
            if verified_all:
                all_tokens = {t for t in all_tokens if t in verified_all}
                filtered_by_chain: dict[str, list[str]] = {}
                for chain, items in by_chain.items():
                    allowed = set(verified_by_chain.get(chain, []))
                    keep = [t for t in items if t in allowed]
                    if keep:
                        filtered_by_chain[chain] = keep
                by_chain = filtered_by_chain
                source = "tokens-tvl-threshold+verified"
        except Exception:
            pass
    except Exception as e:
        print(f"[catalog-refresh] active pools tokens fetch failed: {e}")

    # If live refresh fails, keep previously cached catalog (avoid shrinking to tiny fallback sets).
    if not all_tokens and cached and isinstance(cached.get("items"), list):
        return cached

    # Last-resort fallback.
    if not all_tokens:
        source = "local-config-fallback"
        all_tokens.update(_supported_tokens())
    out = {
        "updated_at": _iso_now(),
        "count": len(all_tokens),
        "items": sorted(all_tokens),
        "by_chain": by_chain,
        "source": source,
        "min_tvl_usd": TOKENS_MIN_TVL_USD,
    }
    _write_json(TOKEN_CATALOG_PATH, out)
    return out


def _refresh_catalogs_once() -> None:
    """Refresh tokens/chains catalogs and keep app responsive on partial failures."""
    try:
        _load_chain_catalog(refresh=True)
    except Exception as e:
        print(f"[catalog-refresh] chains refresh failed: {e}")
    try:
        _load_token_catalog(refresh=True)
    except Exception as e:
        print(f"[catalog-refresh] tokens refresh failed: {e}")


def _catalog_stale(path: Path, max_age_sec: int) -> bool:
    cached = _read_json(path)
    if not cached:
        return True
    updated_at_raw = str(cached.get("updated_at") or "").strip()
    if not updated_at_raw:
        return True
    try:
        updated_at = datetime.fromisoformat(updated_at_raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age_sec = (datetime.now(timezone.utc) - updated_at.astimezone(timezone.utc)).total_seconds()
    return age_sec >= max_age_sec


def _catalog_refresh_loop(interval_sec: int, run_on_startup: bool) -> None:
    # Optional refresh on startup, but avoid refetch on each deploy when cache exists.
    # Startup refresh runs only when cache files are missing.
    if run_on_startup and (not TOKEN_CATALOG_PATH.is_file() or not CHAIN_CATALOG_PATH.is_file()):
        _refresh_catalogs_once()
    while not CATALOG_REFRESH_STOP.wait(interval_sec):
        _refresh_catalogs_once()


def _start_catalog_auto_refresh() -> None:
    global CATALOG_REFRESH_THREAD
    if CATALOG_REFRESH_THREAD and CATALOG_REFRESH_THREAD.is_alive():
        return
    CATALOG_REFRESH_STOP.clear()
    CATALOG_REFRESH_THREAD = threading.Thread(
        target=_catalog_refresh_loop,
        args=(CATALOG_REFRESH_INTERVAL_SEC, CATALOG_REFRESH_ON_STARTUP),
        daemon=True,
        name="catalog-auto-refresh",
    )
    CATALOG_REFRESH_THREAD.start()


def _stop_catalog_auto_refresh() -> None:
    CATALOG_REFRESH_STOP.set()


def _final_income(data: dict) -> float:
    fees = data.get("fees") or []
    return float(fees[-1][1]) if fees else 0.0


def _fee_over_threshold(data: dict, threshold_pct: float) -> bool:
    """True when pool fee is above configured threshold."""
    try:
        if float(data.get("fee_pct") or 0) > threshold_pct:
            return True
    except (TypeError, ValueError):
        pass
    raw = data.get("raw_fee_tier")
    if raw is None:
        return False
    try:
        raw_int = int(raw)
    except (TypeError, ValueError):
        return False
    if (raw_int / 10000.0) > threshold_pct:
        return True
    if raw_int > 100000 and (raw_int / 1e6) > threshold_pct:
        return True
    return False


def _merge_for_web(
    token_pairs: str,
    include_chains: list[str],
    min_fee_pct: float,
    max_fee_pct: float,
    exclude_suffixes: list[str],
    merged_override: dict[str, dict] | None = None,
) -> dict[str, Any]:
    suffix = pairs_to_filename_suffix(token_pairs)
    if merged_override is None:
        v3_path = DATA_DIR / f"pools_v3_{suffix}.json"
        v4_path = DATA_DIR / f"pools_v4_{suffix}.json"
        v3_data = load_chart_data_json(str(v3_path))
        v4_data = load_chart_data_json(str(v4_path))
        merged: dict[str, dict] = {}
        merged.update(v3_data)
        merged.update(v4_data)
    else:
        merged = dict(merged_override)

    in_chains = {x.strip().lower() for x in include_chains if x.strip()}
    if in_chains:
        merged = {k: v for k, v in merged.items() if v.get("chain", "").lower() in in_chains}

    suffix_set = {str(s).strip().lower().replace("0x", "")[-4:] for s in exclude_suffixes if str(s).strip()}

    def in_fee_range(item: dict) -> bool:
        try:
            pct = float(item.get("fee_pct") or 0)
        except (TypeError, ValueError):
            pct = 0.0
        return min_fee_pct <= pct <= max_fee_pct

    def excluded_by_suffix(item: dict, pid: str) -> bool:
        if not suffix_set:
            return False
        pool_id = str(item.get("pool_id") or pid or "").lower().replace("0x", "")
        return any(pool_id.endswith(suf) for suf in suffix_set)

    all_items = sorted(merged.items(), key=lambda kv: _final_income(kv[1]), reverse=True)

    rows = []
    chart_series = []
    filtered_out = 0
    default_visible = 0
    for pool_id, v in all_items:
        fees = v.get("fees") or []
        tvl = v.get("tvl") or []
        if excluded_by_suffix(v, pool_id):
            status = "filtered_suffix"
        elif in_fee_range(v):
            status = "ok"
        else:
            status = "filtered_fee_range"
        if status != "ok":
            filtered_out += 1
        row = {
            "pool_id": v.get("pool_id", pool_id),
            "chain": v.get("chain", ""),
            "version": v.get("version", ""),
            "pair": v.get("pair", ""),
            "fee_pct": float(v.get("fee_pct") or 0),
            "final_income": _final_income(v),
            "last_tvl": float(tvl[-1][1]) if tvl else 0.0,
            "status": status,
        }
        rows.append(row)
        if fees or tvl:
            chart_series.append(
                {
                    "label": f"{row['chain']} {row['version']} {row['pair']} ...{row['pool_id'][-4:]}",
                    "chain": row["chain"],
                    "version": row["version"],
                    "pair": row["pair"],
                    "fee_pct": row["fee_pct"],
                    "status": status,
                    "pool_id": row["pool_id"],
                    "fees": fees,
                    "tvl": tvl,
                }
            )
            if status == "ok":
                default_visible += 1

    return {
        "suffix": suffix,
        "total": len(merged),
        "chart_pools": default_visible,
        "error_pools": filtered_out,
        "rows": rows,
        "series": chart_series,
    }


def _canonical_token_symbol(sym: str) -> str:
    s = (sym or "").strip().lower()
    if s in {"eth", "weth", "weth.e", "weth9"}:
        return "eth"
    return s


def _requested_pair_key(a: str, b: str) -> tuple[str, str]:
    x = _canonical_token_symbol(a)
    y = _canonical_token_symbol(b)
    return tuple(sorted((x, y)))


def _pair_label_key(pair_label: str) -> tuple[str, str] | None:
    if "/" not in (pair_label or ""):
        return None
    a, b = pair_label.split("/", 1)
    return _requested_pair_key(a, b)


def _run_subprocess(script_name: str, env: dict[str, str], min_tvl: float, logs: list[str]) -> None:
    cmd = [sys.executable, str(BASE_DIR / script_name), "--min-tvl", str(min_tvl)]
    timeout_sec = int(env.get("AGENT_TIMEOUT_SEC", "480"))
    logs.append(f"$ {' '.join(cmd)}  # timeout={timeout_sec}s")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(BASE_DIR),
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_sec,
        )
        if proc.stdout:
            logs.append(proc.stdout[-4000:])
        if proc.stderr:
            logs.append(proc.stderr[-4000:])
        if proc.returncode != 0:
            raise RuntimeError(f"{script_name} failed with code {proc.returncode}")
    except subprocess.TimeoutExpired as e:
        if e.stdout:
            logs.append(str(e.stdout)[-4000:])
        if e.stderr:
            logs.append(str(e.stderr)[-4000:])
        raise RuntimeError(
            f"{script_name} timed out after {timeout_sec}s. "
            "Try fewer chains or smaller history window."
        )


def _push_run_history(
    *,
    session_id: str,
    status: str,
    token_pairs: str,
    include_chains: list[str],
    min_tvl: float,
    days: int,
    include_versions: list[str],
    speed_mode: str,
    min_fee_pct: float,
    max_fee_pct: float,
    exclude_suffixes: list[str],
    logs: list[str],
    error: str | None = None,
) -> None:
    item = {
        "ts": _iso_now(),
        "status": status,
        "request": {
            "pairs": token_pairs,
            "include_chains": include_chains,
            "min_tvl": min_tvl,
            "days": days,
            "include_versions": include_versions,
            "speed_mode": speed_mode,
            "min_fee_pct": min_fee_pct,
            "max_fee_pct": max_fee_pct,
            "exclude_suffixes": exclude_suffixes,
        },
        "error": error or "",
        "logs": logs[-8:],
    }
    with JOB_LOCK:
        bucket = RUN_HISTORY.setdefault(session_id, [])
        bucket.insert(0, item)
        del bucket[RUN_HISTORY_LIMIT:]
    _analytics_log_event(
        session_id=session_id,
        event_type="run_done" if status == "done" else "run_failed",
        path="/api/pools/run",
        payload=token_pairs,
    )


def _recent_failed_runs(limit: int = 50) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with JOB_LOCK:
        for sid, items in RUN_HISTORY.items():
            for item in items:
                if str(item.get("status") or "") != "failed":
                    continue
                req = item.get("request") or {}
                logs = item.get("logs") or []
                rows.append(
                    {
                        "ts": item.get("ts", ""),
                        "session_id": sid,
                        "pairs": req.get("pairs", ""),
                        "chains": ",".join(req.get("include_chains") or []),
                        "versions": ",".join(req.get("include_versions") or []),
                        "days": req.get("days"),
                        "min_tvl": req.get("min_tvl"),
                        "error": item.get("error", ""),
                        "logs": "\n\n".join(str(x) for x in logs if str(x).strip())[:6000],
                    }
                )
    rows.sort(key=lambda x: str(x.get("ts") or ""), reverse=True)
    return rows[: max(1, min(200, int(limit)))]


def _run_pool_job(job_id: str, req: "PoolsRunRequest", session_id: str) -> None:
    def _set_stage(stage: str, label: str, progress: int) -> None:
        with JOB_LOCK:
            j = JOBS.get(job_id)
            if not j:
                return
            j["stage"] = stage
            j["stage_label"] = label
            j["progress"] = max(0, min(100, progress))

    speed_mode = str(req.speed_mode or "normal").strip().lower()
    if speed_mode not in {"normal", "fast"}:
        speed_mode = "normal"

    with JOB_LOCK:
        job = JOBS[job_id]
        job["status"] = "running"
        job["started_at"] = time.time()
        job["stage"] = "prepare"
        job["stage_label"] = f"Preparing parameters ({speed_mode})"
        job["progress"] = 5

    # Build up to 4 selected token pairs
    pairs = []
    for pair in req.pairs[:4]:
        part = (pair or "").strip().lower()
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        a, b = a.strip(), b.strip()
        if a and b and a != b:
            pairs.append((a, b))
    pairs = list(dict.fromkeys(pairs))
    if not pairs:
        with JOB_LOCK:
            job["status"] = "failed"
            job["error"] = "No valid pairs provided."
            job["stage"] = "failed"
            job["stage_label"] = "Validation failed"
            job["progress"] = 100
        return
    token_pairs = _pairs_to_string(pairs)
    requested_pair_keys = {_requested_pair_key(a, b) for a, b in pairs}

    include_chains = [c.strip().lower() for c in req.include_chains if c.strip()]
    include_versions = [v.strip().lower() for v in req.include_versions if v.strip()]
    run_v3 = "v3" in include_versions
    run_v4 = "v4" in include_versions

    logs: list[str] = []
    env = os.environ.copy()
    env["TOKEN_PAIRS"] = token_pairs
    env["FEE_DAYS"] = str(req.days)
    env["INCLUDE_CHAINS"] = ",".join(include_chains)
    env["DISABLE_PDF_OUTPUT"] = "1"
    env["GRAPHQL_RETRIES"] = os.environ.get("WEB_GRAPHQL_RETRIES", "1")
    env["POOL_SERIES_WORKERS"] = os.environ.get(
        "WEB_POOL_SERIES_WORKERS_FAST" if speed_mode == "fast" else "WEB_POOL_SERIES_WORKERS_NORMAL",
        "16" if speed_mode == "fast" else "8",
    )

    # v4 endpoint config uses this list at import-time
    if run_v4 and include_chains:
        v4_supported = {c for c in include_chains if c in UNISWAP_V4_SUBGRAPHS}
        env["V4_CHAINS"] = ",".join(sorted(v4_supported))

    try:
        merged_raw: dict[str, dict] = {}
        with RUN_LOCK:
            total_pairs = max(1, len(pairs))
            for idx, (a, b) in enumerate(pairs, start=1):
                pair_str = f"{a},{b}"
                pair_suffix = pairs_to_filename_suffix(pair_str)
                env["TOKEN_PAIRS"] = pair_str

                base_progress = int(10 + (idx - 1) * (65 / total_pairs))
                if run_v3:
                    _set_stage("v3", f"Running v3 ({idx}/{total_pairs}): {pair_str}", min(70, base_progress + 10))
                    _run_subprocess("agent_v3.py", env, req.min_tvl, logs)
                    v3_path = DATA_DIR / f"pools_v3_{pair_suffix}.json"
                    merged_raw.update(load_chart_data_json(str(v3_path)))
                if run_v4:
                    _set_stage("v4", f"Running v4 ({idx}/{total_pairs}): {pair_str}", min(78, base_progress + 20))
                    _run_subprocess("agent_v4.py", env, req.min_tvl, logs)
                    v4_path = DATA_DIR / f"pools_v4_{pair_suffix}.json"
                    merged_raw.update(load_chart_data_json(str(v4_path)))

        # Keep only pools that really match requested pairs.
        # Protects against occasional cross-token resolution artifacts (e.g. POL instead of ETH).
        if requested_pair_keys:
            merged_raw = {
                k: v
                for k, v in merged_raw.items()
                if (_pair_label_key(str(v.get("pair") or "")) in requested_pair_keys)
            }

        _set_stage("merge", "Merging results for web", 85)
        result = _merge_for_web(
            token_pairs,
            include_chains=include_chains,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            merged_override=merged_raw,
        )
        with JOB_LOCK:
            job["status"] = "done"
            job["stage"] = "done"
            job["stage_label"] = "Completed"
            job["progress"] = 100
            job["result"] = {
                "request": {
                    "pairs": token_pairs,
                    "days": req.days,
                    "min_tvl": req.min_tvl,
                    "include_chains": include_chains,
                    "include_versions": include_versions,
                    "speed_mode": speed_mode,
                    "min_fee_pct": req.min_fee_pct,
                    "max_fee_pct": req.max_fee_pct,
                    "exclude_suffixes": req.exclude_suffixes,
                    "lp_allocation_usd": float(env.get("LP_ALLOCATION_USD", "1000")),
                },
                **result,
                "logs": logs[-8:],
            }
        _push_run_history(
            session_id=session_id,
            status="done",
            token_pairs=token_pairs,
            include_chains=include_chains,
            min_tvl=req.min_tvl,
            days=req.days,
            include_versions=include_versions,
            speed_mode=speed_mode,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            logs=logs,
        )
    except Exception as e:
        with JOB_LOCK:
            job["status"] = "failed"
            job["error"] = str(e)
            job["stage"] = "failed"
            job["stage_label"] = "Failed"
            job["progress"] = 100
            job["result"] = {"logs": logs[-8:]}
        _push_run_history(
            session_id=session_id,
            status="failed",
            token_pairs=token_pairs,
            include_chains=include_chains,
            min_tvl=req.min_tvl,
            days=req.days,
            include_versions=include_versions,
            speed_mode=speed_mode,
            min_fee_pct=req.min_fee_pct,
            max_fee_pct=req.max_fee_pct,
            exclude_suffixes=req.exclude_suffixes,
            logs=logs,
            error=str(e),
        )
    finally:
        with JOB_LOCK:
            job["finished_at"] = time.time()


class PoolsRunRequest(BaseModel):
    pairs: list[str] = Field(default_factory=list, description="Up to 4 pairs: tokenA,tokenB")
    include_chains: list[str] = Field(default_factory=list)
    include_versions: list[str] = Field(default_factory=lambda: ["v3", "v4"])
    min_tvl: float = 1000.0
    days: int = 30
    speed_mode: str = "normal"
    min_fee_pct: float = 0.0
    max_fee_pct: float = 2.0
    exclude_suffixes: list[str] = Field(default_factory=list, description="Exclude pool ids by last 4 chars")


class AuthNonceRequest(BaseModel):
    address: str
    chain_id: int = 1
    wallet: str = "injected"


class AuthVerifyRequest(BaseModel):
    address: str
    chain_id: int = 1
    wallet: str = "injected"
    message: str
    signature: str


class AdminWalletUpdate(BaseModel):
    action: str
    address: str


class HelpTicketCreate(BaseModel):
    name: str = ""
    email: str = ""
    subject: str
    message: str


class HelpTicketUpdate(BaseModel):
    ticket_id: int
    status: str | None = None
    admin_note: str | None = None


class HelpTicketReply(BaseModel):
    ticket_id: int
    message: str


class HelpTicketClose(BaseModel):
    ticket_id: int


class HelpTicketDelete(BaseModel):
    ticket_id: int


class HelpFeedbackCreate(BaseModel):
    message: str


class AdminFeedbackReview(BaseModel):
    feedback_id: int


class AdminFeedbackDelete(BaseModel):
    feedback_id: int


class AdminFaqUpsert(BaseModel):
    faq_id: int | None = None
    question: str
    answer: str
    is_published: bool = True
    is_featured: bool = False
    sort_order: int = 100


class AdminFaqDelete(BaseModel):
    faq_id: int


class AdminFaqPublish(BaseModel):
    faq_id: int
    is_published: bool


class PositionsScanRequest(BaseModel):
    evm_addresses: list[str] = Field(default_factory=list)
    solana_addresses: list[str] = Field(default_factory=list)
    tron_addresses: list[str] = Field(default_factory=list)
    include_pools: bool = True
    include_lending: bool = True
    include_rewards: bool = True
    # Backward-compatible fields from the previous UI version.
    addresses: list[str] = Field(default_factory=list)
    chain_ids: list[int] = Field(default_factory=list)


class PositionPoolSeriesRequest(BaseModel):
    chain: str
    protocol: str
    pool_id: str
    address: str
    position_ids: list[str] = Field(default_factory=list)
    position_liquidity: str = "0"
    pool_liquidity: str = "0"
    days: int = 30


INTENT_OPTIONS: list[tuple[str, str]] = [
    ("/", "Find the best fee on Uniswap"),
    ("/pancake", "Find the best pool on PancakeSwap"),
    ("/stables", "Optimize my lending positions"),
    ("/positions", "Optimize my pool positions"),
    ("/help", "Send wishes or report issues"),
]


def _intent_label_for_path(path: str) -> str:
    for p, label in INTENT_OPTIONS:
        if p == path:
            return label
    return "Find the best fee on Uniswap"


def _intent_options_html(selected_path: str) -> str:
    rows: list[str] = []
    for path, label in INTENT_OPTIONS:
        sel = " selected" if path == selected_path else ""
        rows.append(f'<option value="{path}"{sel}>{label}</option>')
    return "\n".join(rows)


def _render_placeholder_page(
    page_title: str,
    subtitle: str,
    selected_path: str,
    *,
    extra_css: str = "",
    extra_html: str = "",
    extra_script: str = "",
    show_intro: bool = True,
) -> str:
    options_html = _intent_options_html(selected_path)
    selected_label = _intent_label_for_path(selected_path)
    intro_html = (
        f"""
    <section class="card">
      <h2>{page_title}</h2>
      <p class="hint">{subtitle}</p>
    </section>
"""
        if show_intro
        else ""
    )
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Uni Fee - {page_title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    html {{
      overflow-y: scroll;
      scrollbar-gutter: stable;
      overflow-x: hidden;
    }}
    body {{
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: linear-gradient(180deg, #d9e3f5 0%, #ecf2ff 100%);
      color: #0f172a;
      min-height: 100vh;
      overflow-x: hidden;
    }}
    .container {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 18px;
      min-height: calc(100vh - 36px);
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }}
    .title {{
      margin: 0;
      font-size: 30px;
      font-weight: 800;
      letter-spacing: 0.2px;
    }}
    .subtitle {{
      margin: 4px 0 0;
      color: #64748b;
      font-size: 14px;
    }}
    .top-controls {{
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: nowrap;
    }}
    .intent-prefix {{
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      white-space: nowrap;
    }}
    .intent-select {{
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 38px 10px 12px;
      font-size: 14px;
      font-weight: 600;
      color: #1f3a8a;
      background: linear-gradient(180deg, #f8fbff 0%, #eff6ff 100%);
      min-width: 320px;
      max-width: 360px;
      appearance: none;
      -webkit-appearance: none;
      background-image:
        linear-gradient(45deg, transparent 50%, #1d4ed8 50%),
        linear-gradient(135deg, #1d4ed8 50%, transparent 50%);
      background-position:
        calc(100% - 18px) calc(50% + 1px),
        calc(100% - 12px) calc(50% + 1px);
      background-size: 6px 6px, 6px 6px;
      background-repeat: no-repeat;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
    }}
    .intent-select option {{
      background: #eef4ff;
      color: #1f3a8a;
    }}
    .connect-btn {{
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      background: #eff6ff;
      cursor: pointer;
      white-space: nowrap;
      width: 190px;
      box-sizing: border-box;
      overflow: hidden;
      text-overflow: ellipsis;
    }}
    .wallet-modal-backdrop {{
      position: fixed;
      inset: 0;
      background: linear-gradient(180deg, rgba(217, 227, 245, 0.82) 0%, rgba(236, 242, 255, 0.82) 100%);
      backdrop-filter: blur(3px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
    }}
    .wallet-modal {{
      width: min(460px, calc(100vw - 24px));
      background: #f8fbff;
      border: 1px solid #cbd5e1;
      border-radius: 14px;
      box-shadow: 0 12px 36px rgba(15, 23, 42, 0.25);
      padding: 14px;
    }}
    .wallet-modal h3 {{
      margin: 0 0 8px;
      font-size: 18px;
    }}
    .wallet-list {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      margin-top: 10px;
    }}
    .wallet-item {{
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 12px;
      background: #eff6ff;
      color: #1d4ed8;
      font-weight: 700;
      text-align: left;
      cursor: pointer;
    }}
    .wallet-item.disabled {{
      opacity: 0.6;
      cursor: not-allowed;
    }}
    .wallet-note {{
      color: #64748b;
      font-size: 12px;
      margin-top: 8px;
    }}
    .card {{
      background: #f3f7ff;
      border: 1px solid #cfdcec;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
    }}
    .card h2 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    .hint {{
      color: #475569;
      margin: 0;
    }}
    {extra_css}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">DeFi Pools</h1>
        <p class="subtitle">{selected_label}</p>
      </div>
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">
          {options_html}
        </select>
        <button class="connect-btn" id="connectWalletBtn" onclick="onConnectWalletClick()">Connect Wallet</button>
      </div>
    </div>
    {intro_html}
    {extra_html}
  </div>
  <div id="walletModalBackdrop" class="wallet-modal-backdrop" onclick="closeWalletModal(event)">
    <div class="wallet-modal">
      <h3>Connect wallet</h3>
      <div class="wallet-list" id="walletList"></div>
      <div class="wallet-note">Rabby and Phantom are supported. Sign-in uses a gasless message signature.</div>
    </div>
  </div>
  <script>
    let authState = {{authenticated: false}};
    const WALLETCONNECT_PROJECT_ID = "__WALLETCONNECT_PROJECT_ID__";
    const WALLET_LABELS = {{
      injected: "Browser Wallet",
      walletconnect: "WalletConnect (QR)",
      rabby: "Rabby",
      metamask: "MetaMask",
      phantom: "Phantom",
      coinbase: "Coinbase Wallet",
    }};

    function navigateIntent(path) {{
      if (!path) return;
      window.location.href = path;
    }}
    function refreshIntentMenu() {{
      const sel = document.getElementById("intentSelect");
      if (!sel) return;
      sel.style.position = "absolute";
      sel.style.left = "-9999px";
      sel.style.opacity = "0";
      sel.style.pointerEvents = "none";
      let wrap = document.getElementById("intentMenuWrap");
      if (!wrap) {{
        wrap = document.createElement("div");
        wrap.id = "intentMenuWrap";
        wrap.style.cssText = "position:relative;min-width:320px;max-width:360px;";
        const btn = document.createElement("button");
        btn.type = "button";
        btn.id = "intentMenuBtn";
        btn.style.cssText = "width:100%;border:1px solid #bfdbfe;border-radius:10px;padding:10px 38px 10px 12px;font-size:14px;font-weight:600;color:#1f3a8a;background:linear-gradient(180deg,#f8fbff 0%,#eff6ff 100%);text-align:left;cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,0.7);";
        const list = document.createElement("div");
        list.id = "intentMenuList";
        list.style.cssText = "display:none;position:absolute;z-index:12000;left:0;right:0;top:calc(100% + 6px);background:#eef4ff;border:1px solid #bfdbfe;border-radius:10px;box-shadow:0 10px 24px rgba(15,23,42,0.15);padding:6px;max-height:320px;overflow:auto;";
        wrap.appendChild(btn);
        wrap.appendChild(list);
        sel.insertAdjacentElement("afterend", wrap);
        btn.onclick = () => {{
          list.style.display = list.style.display === "block" ? "none" : "block";
        }};
        document.addEventListener("click", (e) => {{
          if (!wrap.contains(e.target)) list.style.display = "none";
        }});
      }}
      const btn = document.getElementById("intentMenuBtn");
      const list = document.getElementById("intentMenuList");
      const options = Array.from(sel.options || []);
      const selected = options.find((o) => o.selected) || options[0];
      btn.textContent = selected ? selected.textContent : "Select";
      list.innerHTML = options.map((o) => {{
        const active = o.value === sel.value;
        const style = active
          ? "display:block;width:100%;padding:9px 10px;border:none;background:#dbeafe;color:#1e3a8a;font-weight:700;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;"
          : "display:block;width:100%;padding:9px 10px;border:none;background:#eef4ff;color:#1f3a8a;font-weight:600;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;";
        return `<button type="button" data-v="${{o.value}}" style="${{style}}">${{o.textContent}}</button>`;
      }}).join("");
      Array.from(list.querySelectorAll("button[data-v]")).forEach((b) => {{
        b.onclick = () => {{
          const v = b.getAttribute("data-v") || "";
          sel.value = v;
          list.style.display = "none";
          navigateIntent(v);
        }};
      }});
    }}

    function getEthereumProviders() {{
      const out = [];
      const eth = window.ethereum;
      if (!eth) return out;
      if (Array.isArray(eth.providers) && eth.providers.length) return eth.providers;
      out.push(eth);
      return out;
    }}

    function getWalletProvider(wallet) {{
      const providers = getEthereumProviders();
      const pick = (pred) => providers.find(pred) || null;
      if (wallet === "injected") {{
        return (
          pick((p) => !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          pick((p) => !!p?.isMetaMask && !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          pick((p) => !!p?.isCoinbaseWallet) ||
          providers[0] ||
          window.ethereum ||
          null
        );
      }}
      if (wallet === "rabby") {{
        return pick((p) => !!p?.isRabby) || (window.ethereum?.isRabby ? window.ethereum : null);
      }}
      if (wallet === "phantom") {{
        if (window.phantom?.ethereum?.request) return window.phantom.ethereum;
        return pick((p) => !!p?.isPhantom) || (window.ethereum?.isPhantom ? window.ethereum : null);
      }}
      if (wallet === "metamask") {{
        return (
          pick((p) => !!p?.isMetaMask && !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          ((window.ethereum?.isMetaMask && !window.ethereum?.isRabby && !window.ethereum?.isPhantom && !window.ethereum?.isCoinbaseWallet) ? window.ethereum : null)
        );
      }}
      if (wallet === "coinbase") {{
        return pick((p) => !!p?.isCoinbaseWallet) || (window.ethereum?.isCoinbaseWallet ? window.ethereum : null);
      }}
      return null;
    }}

    function getWalletChoices() {{
      const order = ["walletconnect", "rabby", "phantom", "metamask", "coinbase", "injected"];
      return order.map((id) => {{
        const isWc = id === "walletconnect";
        const available = isWc ? true : !!getWalletProvider(id);
        let label = WALLET_LABELS[id];
        if (isWc && !WALLETCONNECT_PROJECT_ID) label += " (setup required)";
        else if (!available) label += " (not detected)";
        return {{id, label, available}};
      }});
    }}

    function openWalletModal() {{
      const list = document.getElementById("walletList");
      const choices = getWalletChoices();
      list.innerHTML = choices.map((w) => {{
        const cls = w.available ? "wallet-item" : "wallet-item disabled";
        const dis = w.available ? "" : "disabled";
        return `<button class="${{cls}}" ${{dis}} onclick="connectWalletFlow('${{w.id}}')">${{w.label}}</button>`;
      }}).join("");
      document.getElementById("walletModalBackdrop").style.display = "flex";
    }}

    function closeWalletModal(event) {{
      if (event && event.target && event.target.id !== "walletModalBackdrop") return;
      document.getElementById("walletModalBackdrop").style.display = "none";
    }}

    async function postJson(url, payload) {{
      const r = await fetch(url, {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: JSON.stringify(payload || {{}})
      }});
      const data = await r.json().catch(() => ({{}}));
      if (!r.ok) {{
        throw new Error(data.detail || data.info || "Request failed");
      }}
      return data;
    }}

    function syncAdminIntentOption() {{
      const sel = document.getElementById("intentSelect");
      if (!sel) return;
      const existing = Array.from(sel.options).find((o) => o.value === "/admin");
      const isAdmin = !!authState?.authenticated && !!authState?.is_admin;
      if (isAdmin && !existing) {{
        const opt = document.createElement("option");
        opt.value = "/admin";
        opt.textContent = "Administer project";
        sel.appendChild(opt);
      }} else if (!isAdmin && existing) {{
        existing.remove();
      }}
      refreshIntentMenu();
    }}

    function setAuthUI() {{
      const btn = document.getElementById("connectWalletBtn");
      if (!btn) return;
      if (authState?.authenticated) {{
        btn.textContent = authState.address_short || "Wallet connected";
      }} else {{
        btn.textContent = "Connect Wallet";
      }}
      syncAdminIntentOption();
    }}

    async function loadAuthState() {{
      try {{
        const r = await fetch("/api/auth/me");
        authState = await r.json();
      }} catch (_) {{
        authState = {{authenticated: false}};
      }}
      setAuthUI();
    }}

    async function onConnectWalletClick() {{
      if (authState?.authenticated) {{
        if (!confirm("Disconnect wallet?")) return;
        try {{
          await postJson("/api/auth/logout", {{}});
          authState = {{authenticated: false}};
          setAuthUI();
        }} catch (e) {{
          console.warn("disconnect failed", e);
        }}
        return;
      }}
      openWalletModal();
    }}

    async function connectWalletFlow(wallet) {{
      if (wallet === "walletconnect") {{
        return connectWalletConnect();
      }}
      const provider = getWalletProvider(wallet);
      if (!provider) return;
      try {{
        const accounts = await provider.request({{method: "eth_requestAccounts"}});
        const address = String((accounts || [])[0] || "").trim();
        if (!address) throw new Error("Wallet did not return an address");
        const chainHex = await provider.request({{method: "eth_chainId"}});
        const chainId = Number.parseInt(String(chainHex || "0x1"), 16) || 1;
        const nonceResp = await postJson("/api/auth/nonce", {{address, chain_id: chainId, wallet}});
        const signature = await provider.request({{method: "personal_sign", params: [nonceResp.message, address]}});
        const verifyResp = await postJson("/api/auth/verify", {{
          address,
          chain_id: chainId,
          wallet,
          message: nonceResp.message,
          signature,
        }});
        authState = {{authenticated: true, ...verifyResp}};
        setAuthUI();
        closeWalletModal({{target: {{id: "walletModalBackdrop"}}}});
      }} catch (e) {{
        console.warn("wallet auth failed", e);
      }}
    }}

    function showWcQrModal(uri) {{
      let el = document.getElementById("wcQrBackdrop");
      if (!el) {{
        el = document.createElement("div");
        el.id = "wcQrBackdrop";
        el.style.cssText = "position:fixed;inset:0;background:linear-gradient(180deg,rgba(217,227,245,0.95),rgba(236,242,255,0.95));backdrop-filter:blur(4px);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:10001;";
        el.innerHTML = '<div style="background:#f8fbff;border:1px solid #cbd5e1;border-radius:14px;padding:20px;text-align:center;box-shadow:0 12px 36px rgba(15,23,42,0.2)"><p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#0f172a">Scan with your wallet app</p><img id="wcQrImg" alt="QR" style="display:block;background:#fff;padding:10px;border-radius:10px;width:260px;height:260px"/><p style="margin:10px 0 0;font-size:12px;color:#64748b">Or open the link on your phone</p><button id="wcQrCancel" type="button" style="margin-top:14px;padding:8px 16px;border-radius:10px;border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;font-weight:700;cursor:pointer">Cancel</button></div>';
        document.body.appendChild(el);
        document.getElementById("wcQrCancel").onclick = closeWcQrModal;
      }}
      document.getElementById("wcQrImg").src = "https://api.qrserver.com/v1/create-qr-code/?size=260x260&data=" + encodeURIComponent(uri);
      el.style.display = "flex";
    }}
    function closeWcQrModal() {{
      const el = document.getElementById("wcQrBackdrop");
      if (el) el.style.display = "none";
      if (window._wcProvider) try {{ window._wcProvider.disconnect(); }} catch (_) {{}}
      window._wcProvider = null;
    }}
    async function connectWalletConnect() {{
      if (!WALLETCONNECT_PROJECT_ID) {{
        alert("WalletConnect is not configured on server (WALLETCONNECT_PROJECT_ID).");
        return;
      }}
      const normalizeAddress = (value) => {{
        const raw = String(value || "").trim();
        if (!raw) return "";
        const parts = raw.split(":");
        return String(parts[parts.length - 1] || "").trim();
      }};
      const toHexMessage = (msg) => {{
        try {{
          return "0x" + Array.from(new TextEncoder().encode(String(msg || ""))).map((b) => b.toString(16).padStart(2, "0")).join("");
        }} catch (_) {{
          return "";
        }}
      }};
      try {{
        const EthereumProviderModule = await import("https://esm.sh/@walletconnect/ethereum-provider@2.23.8");
        const wcChains = [1, 10, 56, 137, 8453, 42161, 43114];
        const wcMetadata = {{
          name: "DeFi Pools",
          description: "DeFi Pools wallet sign-in",
          url: window.location.origin,
          icons: [window.location.origin + "/favicon.ico"],
        }};
        const provider = await EthereumProviderModule.EthereumProvider.init({{
          projectId: WALLETCONNECT_PROJECT_ID,
          optionalChains: wcChains,
          showQrModal: false,
          optionalMethods: ["eth_requestAccounts", "eth_accounts", "eth_chainId", "personal_sign", "wallet_switchEthereumChain"],
          optionalEvents: ["accountsChanged", "chainChanged", "disconnect"],
          metadata: wcMetadata,
          rpcMap: {{}},
        }});
        provider.on("display_uri", showWcQrModal);
        window._wcProvider = provider;
        let connected = false;
        try {{
          await provider.connect();
          connected = true;
        }} catch (_) {{}}
        if (!connected) {{
          try {{
            await provider.enable();
          }} catch (connErr) {{
            closeWcQrModal();
            throw connErr;
          }}
        }}
        window._wcProvider = null;
        closeWcQrModal();
        let accounts = provider.accounts || [];
        if (!accounts.length) accounts = (await provider.request({{method: "eth_accounts"}})) || [];
        if (!accounts.length) accounts = (await provider.request({{method: "eth_requestAccounts"}})) || [];
        const address = normalizeAddress(accounts[0] || "");
        if (!/^0x[a-fA-F0-9]{{40}}$/.test(address)) throw new Error("WalletConnect did not return a valid EVM address");
        const chainHex = await provider.request({{method: "eth_chainId"}});
        const chainId = Number.parseInt(String(chainHex || "0x1"), 16) || 1;
        const nonceResp = await postJson("/api/auth/nonce", {{address, chain_id: chainId, wallet: "walletconnect"}});
        const messageHex = toHexMessage(nonceResp.message || "");
        const signVariants = [
          [nonceResp.message, address],
          [address, nonceResp.message],
          [messageHex, address],
          [address, messageHex],
        ];
        let signature = "";
        for (const params of signVariants) {{
          try {{
            if (!params[0]) continue;
            signature = await provider.request({{method: "personal_sign", params}});
            if (signature) break;
          }} catch (_) {{}}
        }}
        if (!signature) throw new Error("Failed to sign auth message via WalletConnect");
        const verifyResp = await postJson("/api/auth/verify", {{
          address,
          chain_id: chainId,
          wallet: "walletconnect",
          message: nonceResp.message,
          signature,
        }});
        authState = {{authenticated: true, ...verifyResp}};
        setAuthUI();
        closeWalletModal({{target: {{id: "walletModalBackdrop"}}}});
      }} catch (e) {{
        closeWcQrModal();
        alert("WalletConnect failed: " + (e?.message || "unknown error") + ". Add this site to Reown Domain allowlist and try again.");
      }}
    }}

    {extra_script}
    loadAuthState();
    refreshIntentMenu();
  </script>
</body>
</html>
"""


def _render_positions_page() -> str:
    extra_css = """
    .positions-grid { display:grid; gap:14px; margin-top:4px; }
    .positions-form, .result-card {
      background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%);
      border: 1px solid #d4deee;
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 20px rgba(15,23,42,0.06);
    }
    .positions-form h3, .result-card h3 { margin:0; font-size:17px; color:#1f3a8a; }
    .section-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; }
    .address-columns { display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:10px; }
    .addr-box { border:1px solid #d7e1ef; border-radius:12px; background:#f8fbff; padding:10px; box-shadow: inset 0 1px 0 rgba(255,255,255,0.7); }
    .addr-input-row { display:grid; grid-template-columns:1fr auto; gap:8px; }
    .addr-input-row input { width:100%; background:#fff; border:1px solid #cbd5e1; border-radius:8px; padding:8px; font-size:13px; }
    .btn-plus { width:26px; min-width:26px; height:26px; border:none; border-radius:0; padding:0; font-size:22px; line-height:1; display:inline-flex; align-items:center; justify-content:center; background:transparent; color:#2563eb; font-weight:800; box-shadow:none; }
    .btn-plus:hover { color:#1d4ed8; background:transparent; }
    .chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; min-height:26px; }
    .chip { display:inline-flex; align-items:center; gap:6px; border:1px solid #bfdbfe; border-radius:999px; padding:4px 8px; background:#eff6ff; color:#1f3a8a; font-size:12px; }
    .chip .x { border:none; background:transparent; color:#1d4ed8; cursor:pointer; font-weight:700; padding:0; line-height:1; }
    .chip.muted { border-style:dashed; color:#64748b; background:#f8fbff; }
    .search-link-btn { border:none; background:transparent; color:#1d4ed8; font-size:13px; font-weight:700; cursor:pointer; padding:0; text-decoration:underline; text-underline-offset:2px; position:relative; z-index:2; pointer-events:auto; }
    .search-link-btn:hover { color:#1e40af; }
    .collapse-btn { border:none; background:transparent; color:#334155; font-size:14px; font-weight:800; cursor:pointer; padding:0 2px; min-width:16px; text-align:center; }
    .section-actions { display:flex; align-items:center; gap:10px; }
    .section-head .section-actions { margin-left: auto; justify-content: flex-end; }
    .section-body { display:block; }
    .section-body.collapsed { display:none; }
    .copy-btn { border:none; background:transparent; color:#2563eb; cursor:pointer; font-size:12px; padding:0 0 0 4px; }
    .pos-progress { width: 140px; height: 6px; border-radius: 999px; background: #e2e8f0; overflow: hidden; display: none; }
    .pos-progress .bar { width: 40%; height: 100%; background: linear-gradient(90deg, #93c5fd, #2563eb); animation: posLoad 1s linear infinite; }
    @keyframes posLoad { 0% { transform: translateX(-120%); } 100% { transform: translateX(280%); } }
    .pos-status { color:#475569; font-size:13px; }
    .table-wrap { overflow-x:auto; border:1px solid #dbe3ef; border-radius:10px; background:#f8fbff; }
    table { width:100%; border-collapse:collapse; font-size:12px; min-width:900px; }
    th, td { border-bottom:1px solid #e2e8f0; padding:7px; text-align:left; vertical-align:top; }
    th { background:#eff6ff; color:#1e3a8a; position:sticky; top:0; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:11px; }
    .errors-box { margin-top:10px; border:1px dashed #fca5a5; background:#fff1f2; color:#881337; border-radius:10px; padding:8px; font-size:12px; white-space:pre-wrap; }
    .info-box { margin-top:10px; border:1px dashed #bfdbfe; background:#eff6ff; color:#1e3a8a; border-radius:10px; padding:8px; font-size:12px; white-space:pre-wrap; }
    @media (max-width: 1100px) {
      .address-columns { grid-template-columns:1fr; }
      .positions-form, .result-card { padding: 12px; }
      .section-head { flex-wrap: wrap; align-items: center; }
      .section-head h3 { font-size: 16px; }
      .search-link-btn { font-size: 12px; }
    }
    @media (max-width: 720px) {
      .positions-grid { gap: 10px; }
      .positions-form, .result-card { padding: 10px; border-radius: 12px; }
      .addr-box { padding: 8px; }
      .addr-input-row { gap: 6px; }
      .addr-input-row input { font-size: 12px; padding: 7px; }
      .btn-plus { width: 24px; min-width: 24px; height: 24px; font-size: 20px; }
      .chip { font-size: 11px; padding: 3px 7px; }
      .pos-status { font-size: 12px; }
      .table-wrap { border-radius: 8px; }
      table { min-width: 740px; font-size: 11px; }
      th, td { padding: 6px; }
      .mono { font-size: 10px; }
      .errors-box, .info-box { font-size: 11px; padding: 7px; }
    }
    """
    extra_html = """
    <div class="positions-grid">
      <section class="positions-form">
        <div class="section-head"><h3>My Crypto Portfolio</h3></div>
        <div class="address-columns">
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="evmInput" placeholder="0x..." />
              <button class="btn btn-plus" type="button" onclick="addAddress('evm')" aria-label="Add EVM address">+</button>
            </div>
            <div class="chips" id="evmChips"></div>
          </div>
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="solanaInput" placeholder="Solana address" />
              <button class="btn btn-plus" type="button" onclick="addAddress('solana')" aria-label="Add Solana address">+</button>
            </div>
            <div class="chips" id="solanaChips"></div>
          </div>
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="tronInput" placeholder="TRON address" />
              <button class="btn btn-plus" type="button" onclick="addAddress('tron')" aria-label="Add TRON address">+</button>
            </div>
            <div class="chips" id="tronChips"></div>
          </div>
        </div>
      </section>
      <section class="result-card">
        <div class="section-head">
          <h3>Pool positions</h3>
          <div class="section-actions">
            <div id="posProgress" class="pos-progress"><div class="bar"></div></div>
            <span class="pos-status" id="posStatus">Ready</span>
            <button id="posSearchBtn" class="search-link-btn" type="button" onclick="scanPositions('pools')">Scan</button>
            <button class="collapse-btn" id="togglePoolsBtn" type="button" onclick="togglePosSection('pools')" title="Collapse/expand">▾</button>
          </div>
        </div>
        <div id="posPoolsBody" class="section-body">
          <div class="table-wrap"><table id="posPoolsTable"></table></div>
          <div id="posErrors"></div>
        </div>
      </section>
      <section class="result-card">
        <div class="section-head">
          <h3>Show history <input id="posHistoryDays" type="number" min="1" max="3650" step="1" value="30" style="width:72px;margin-left:6px"/></h3>
          <div class="section-actions">
            <span class="pos-status" id="posHistoryStatus">Select pools and click Search</span>
            <button class="search-link-btn" type="button" onclick="showSelectedPoolSeries()">Search</button>
          </div>
        </div>
        <div id="posPoolChart" style="height:340px;border:1px solid #dbe3ef;border-radius:10px;background:#f8fbff;padding:6px"></div>
      </section>
    </div>
    """
    extra_script = """
    function esc(v) { return String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }
    const posState = { evm: [], solana: [], tron: [] };
    const POS_STORAGE_KEY = "positions_form_v2";
    function validAddress(kind, value) {
      const v = String(value || "").trim();
      if (!v) return false;
      if (kind === "evm") return /^0x[a-fA-F0-9]{40}$/.test(v);
      if (kind === "solana") return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(v);
      if (kind === "tron") return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(v);
      return false;
    }
    function inputId(kind) {
      if (kind === "evm") return "evmInput";
      if (kind === "solana") return "solanaInput";
      return "tronInput";
    }
    function chipsId(kind) {
      if (kind === "evm") return "evmChips";
      if (kind === "solana") return "solanaChips";
      return "tronChips";
    }
    function savePosState() {
      localStorage.setItem(POS_STORAGE_KEY, JSON.stringify(posState));
    }
    function loadPosState() {
      try {
        const raw = localStorage.getItem(POS_STORAGE_KEY);
        if (!raw) return;
        const parsed = JSON.parse(raw);
        for (const k of ["evm", "solana", "tron"]) {
          if (Array.isArray(parsed[k])) posState[k] = parsed[k].map((x) => String(x || "").trim()).filter(Boolean);
        }
      } catch (_) {}
    }
    function renderChips(kind) {
      const wrap = document.getElementById(chipsId(kind));
      if (!wrap) return;
      const rows = posState[kind] || [];
      if (!rows.length) {
        wrap.innerHTML = "<span class='chip muted'>No addresses</span>";
        return;
      }
      wrap.innerHTML = rows.map((addr, i) => `<span class='chip'><span class='mono'>${esc(addr)}</span><button class='x' type='button' onclick=\"removeAddress('${kind}', ${i})\">×</button></span>`).join("");
    }
    function renderAllChips() {
      renderChips("evm");
      renderChips("solana");
      renderChips("tron");
    }
    function addAddress(kind) {
      const el = document.getElementById(inputId(kind));
      const addrRaw = String(el?.value || "").trim();
      if (!validAddress(kind, addrRaw)) {
        setPosStatus(`Invalid ${kind.toUpperCase()} address format.`, true);
        return;
      }
      const addr = addrRaw;
      const dup = (posState[kind] || []).some((x) => kind === "evm" ? String(x).toLowerCase() === addr.toLowerCase() : String(x) === addr);
      if (dup) {
        setPosStatus("Address already added.", true);
        return;
      }
      posState[kind].push(addr);
      if (el) el.value = "";
      savePosState();
      renderChips(kind);
      setPosStatus("Address added.", false);
    }
    function removeAddress(kind, idx) {
      if (!Array.isArray(posState[kind])) return;
      posState[kind].splice(Number(idx) || 0, 1);
      savePosState();
      renderChips(kind);
    }
    const posSectionState = {pools: false};
    const posCache = {pools: []};
    const posHistorySelected = new Set();
    const POS_RESULTS_STORAGE_KEY = "positions_scan_results_v1";
    const POS_HIDDEN_EXPANDED_KEY = "positions_hidden_expanded_v1";
    let posHasScannedOnce = false;
    let posScanTicker = null;
    let posScanStartedAt = 0;
    function setPosStatus(text, isErr) {
      const el = document.getElementById("posStatus");
      if (!el) return;
      el.textContent = text || "";
      el.style.color = isErr ? "#b91c1c" : "#475569";
    }
    function setPosBusy(flag) {
      const el = document.getElementById("posProgress");
      if (!el) return;
      el.style.display = flag ? "block" : "none";
    }
    function updatePosSearchButton() {
      const btn = document.getElementById("posSearchBtn");
      if (!btn) return;
      btn.textContent = posHasScannedOnce ? "Scan again" : "Scan";
    }
    function setPosHistoryStatus(text, isErr) {
      const el = document.getElementById("posHistoryStatus");
      if (!el) return;
      el.textContent = text || "";
      el.style.color = isErr ? "#b91c1c" : "#475569";
    }
    function stopPosScanProgressTicker() {
      if (posScanTicker) {
        clearInterval(posScanTicker);
        posScanTicker = null;
      }
      posScanStartedAt = 0;
    }
    function startPosScanProgressTicker() {
      stopPosScanProgressTicker();
      posScanStartedAt = Date.now();
      const plannedChains = ["Arbitrum", "Base", "Ethereum", "Optimism", "Polygon", "BSC", "Unichain"];
      const steps = [
        "Validate addresses",
        "Start chain workers",
        "On-chain position discovery",
        "Subgraph fallback queries",
        "Price lookup and valuation",
        "Merge and finalize table",
      ];
      const tick = () => {
        const elapsed = Math.max(0, Math.floor((Date.now() - posScanStartedAt) / 1000));
        let phase = steps[Math.min(steps.length - 1, Math.floor(elapsed / 5))];
        const chainHint = plannedChains[Math.floor(elapsed / 4) % plannedChains.length];
        if (elapsed >= 20) phase = "Heavy indexer/rpc responses";
        if (elapsed >= 28) phase = "Final merge and response";
        setPosStatus(`Scanning positions... ${elapsed}s | ${phase} (${chainHint})`, false);
      };
      tick();
      posScanTicker = setInterval(tick, 900);
    }
    function copyText(value) {
      const text = String(value || "").trim();
      if (!text) return;
      navigator.clipboard.writeText(text).then(() => setPosStatus("Copied to clipboard", false)).catch(() => {});
    }
    function shortAddr4(value) {
      const v = String(value || "").trim();
      if (!v) return "";
      return v.length <= 4 ? v : ("..." + v.slice(-4));
    }
    function getTrustedSpamKeys() {
      try {
        const raw = localStorage.getItem("positions_trusted_spam_keys");
        const arr = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(arr)) return new Set();
        return new Set(arr.map((x) => String(x)));
      } catch (_) {
        return new Set();
      }
    }
    function setTrustedSpamKeys(setObj) {
      const arr = Array.from(setObj || []);
      localStorage.setItem("positions_trusted_spam_keys", JSON.stringify(arr.slice(0, 1000)));
    }
    function getManualHiddenKeys() {
      try {
        const raw = localStorage.getItem("positions_manual_hidden_keys");
        const arr = raw ? JSON.parse(raw) : [];
        if (!Array.isArray(arr)) return new Set();
        return new Set(arr.map((x) => String(x)));
      } catch (_) {
        return new Set();
      }
    }
    function setManualHiddenKeys(setObj) {
      const arr = Array.from(setObj || []);
      localStorage.setItem("positions_manual_hidden_keys", JSON.stringify(arr.slice(0, 1000)));
    }
    function poolRowKey(r) {
      return [
        String(r.address || "").toLowerCase(),
        String(r.chain || "").toLowerCase(),
        String(r.protocol || "").toLowerCase(),
        String(r.position_id || ""),
        String(r.pool_id || "").toLowerCase(),
      ].join("|");
    }
    function trustSpamRow(key) {
      const trusted = getTrustedSpamKeys();
      trusted.add(String(key || ""));
      setTrustedSpamKeys(trusted);
      renderPools(posCache.pools || []);
      setPosStatus("Spam mark removed for this position.", false);
    }
    function untrustSpamRow(key) {
      const trusted = getTrustedSpamKeys();
      trusted.delete(String(key || ""));
      setTrustedSpamKeys(trusted);
      renderPools(posCache.pools || []);
      setPosStatus("Position moved back to suspected spam.", false);
    }
    function setHideRow(key, hidden, suspected) {
      const rowKey = String(key || "");
      const manual = getManualHiddenKeys();
      const trusted = getTrustedSpamKeys();
      if (hidden) {
        manual.add(rowKey);
        if (suspected) trusted.delete(rowKey);
      } else {
        manual.delete(rowKey);
        if (suspected) trusted.add(rowKey);
      }
      setManualHiddenKeys(manual);
      setTrustedSpamKeys(trusted);
      renderPools(posCache.pools || []);
      setPosStatus(hidden ? "Position hidden." : "Position shown.", false);
    }
    function setSectionCollapsed(key, collapsed) {
      const bodyMap = {pools: "posPoolsBody"};
      const btnMap = {pools: "togglePoolsBtn"};
      const body = document.getElementById(bodyMap[key]);
      const btn = document.getElementById(btnMap[key]);
      if (!body || !btn) return;
      posSectionState[key] = !!collapsed;
      body.classList.toggle("collapsed", !!collapsed);
      btn.textContent = collapsed ? "▸" : "▾";
    }
    function togglePosSection(key) {
      const next = !posSectionState[key];
      setSectionCollapsed(key, next);
      if (!next) {
        if (key === "pools") renderPools(posCache.pools || []);
      }
    }
    async function ensurePlotly() {
      if (window.Plotly && typeof window.Plotly.newPlot === "function") return true;
      return new Promise((resolve) => {
        const existing = document.getElementById("plotlyLoader");
        if (existing) {
          existing.addEventListener("load", () => resolve(!!window.Plotly), {once: true});
          existing.addEventListener("error", () => resolve(false), {once: true});
          return;
        }
        const s = document.createElement("script");
        s.id = "plotlyLoader";
        s.src = "https://cdn.plot.ly/plotly-2.35.2.min.js";
        s.onload = () => resolve(true);
        s.onerror = () => resolve(false);
        document.head.appendChild(s);
      });
    }
    function getHistoryDays() {
      const v = Number(document.getElementById("posHistoryDays")?.value || 30);
      return Math.max(1, Math.min(3650, Math.round(v)));
    }
    function savePosResults(payload) {
      try {
        localStorage.setItem(POS_RESULTS_STORAGE_KEY, JSON.stringify(payload || {}));
      } catch (_) {}
    }
    function loadPosResults() {
      try {
        const raw = localStorage.getItem(POS_RESULTS_STORAGE_KEY);
        return raw ? JSON.parse(raw) : null;
      } catch (_) {
        return null;
      }
    }
    function renderScanMessages(data) {
      const errWrap = document.getElementById("posErrors");
      if (!errWrap) return;
      const errs = data?.errors || [];
      const infos = data?.infos || [];
      const dbg = data?.debug || {};
      const dbgSummary = Array.isArray(dbg.summary) ? dbg.summary : [];
      const dbgRows = Array.isArray(dbg.pool_scan) ? dbg.pool_scan : [];
      let dbgHtml = "";
      if (dbgSummary.length || dbgRows.length) {
        const sumLines = dbgSummary.slice(0, 40).map((x) =>
          `${esc(x.chain || "?")}/${esc(x.version || "?")} ${esc(x.query_mode || "?")} [${esc(x.status || "?")}]: ${Number(x.count || 0)}`
        );
        const zeroRows = dbgRows
          .filter((x) => Number(x.positions_found || 0) === 0)
          .slice(0, 30)
          .map((x) => `${esc(x.owner || "?")} -> ${esc(x.chain || "?")}/${esc(x.version || "?")} (0)`);
        const cmpRows = dbgRows
          .filter((x) => {
            const c = x.compare || {};
            return Boolean(c.graph_checked) && (Number(c.only_onchain_count || 0) > 0 || Number(c.only_graph_count || 0) > 0);
          })
          .slice(0, 30)
          .map((x) => {
            const c = x.compare || {};
            const o = Number(c.only_onchain_count || 0);
            const g = Number(c.only_graph_count || 0);
            const oid = (Array.isArray(c.only_onchain_ids) ? c.only_onchain_ids : []).slice(0, 2).join(",");
            const gid = (Array.isArray(c.only_graph_ids) ? c.only_graph_ids : []).slice(0, 2).join(",");
            return `${esc(x.owner || "?")} -> ${esc(x.chain || "?")}/${esc(x.version || "?")} only_onchain=${o}${oid ? ` [${esc(oid)}]` : ""}, only_graph=${g}${gid ? ` [${esc(gid)}]` : ""}`;
          });
        const body = []
          .concat(sumLines.length ? ["Summary:", ...sumLines] : [])
          .concat(zeroRows.length ? ["", "Zero-found owners:", ...zeroRows] : [])
          .concat(cmpRows.length ? ["", "Source compare (onchain vs graph):", ...cmpRows] : [])
          .join("\\n");
        if (body.trim()) {
          dbgHtml = `<div class='info-box'><details><summary>Debug scan details</summary><pre style='margin:8px 0 0;white-space:pre-wrap'>${body}</pre></details></div>`;
        }
      }
      const errHtml = errs.length ? `<div class='errors-box'>${esc(errs.join("\\n"))}</div>` : "";
      const infoHtml = infos.length ? `<div class='info-box'>${esc(infos.join("\\n"))}</div>` : "";
      errWrap.innerHTML = errHtml + infoHtml + dbgHtml;
    }
    function setHistorySelected(idx, checked) {
      const n = Number(idx) || 0;
      if (checked) posHistorySelected.add(n);
      else posHistorySelected.delete(n);
    }
    async function showSelectedPoolSeries() {
      const chartEl = document.getElementById("posPoolChart");
      if (!chartEl) return;
      const selected = Array.from(posHistorySelected).sort((a, b) => a - b).slice(0, 12);
      if (!selected.length) {
        setPosHistoryStatus("Select at least one checkbox in Pool positions table.", true);
        return;
      }
      try {
        setPosHistoryStatus("Loading history...", false);
        const ok = await ensurePlotly();
        if (!ok) throw new Error("Failed to load chart library");
        const palette = ["#1d4ed8", "#7c3aed", "#059669", "#dc2626", "#0f766e", "#b45309", "#4338ca", "#be123c"];
        const traces = [];
        for (let i = 0; i < selected.length; i++) {
          const idx = selected[i];
          const row = (posCache.pools || [])[idx];
          if (!row) continue;
          const payload = {
            chain: row.chain,
            protocol: row.protocol,
            pool_id: row.pool_id,
            address: row.address,
            position_ids: Array.isArray(row.position_ids) ? row.position_ids : [],
            position_liquidity: row.liquidity,
            pool_liquidity: row.pool_liquidity,
            days: getHistoryDays(),
          };
          const res = await fetch("/api/positions/pool-value-series", {
            method: "POST",
            headers: {"Content-Type":"application/json"},
            body: JSON.stringify(payload),
          });
          const data = await res.json().catch(() => ({}));
          if (!res.ok) continue;
          const items = Array.isArray(data.items) ? data.items : [];
          if (!items.length) continue;
          traces.push({
            x: items.map((x) => new Date(Number(x.ts || 0) * 1000)),
            y: items.map((x) => Number(x.position_tvl_usd || 0)),
            mode: "lines",
            line: {color: palette[i % palette.length], width: 2},
            name: String(row.pair || row.pool_id || `Pool ${i + 1}`),
            hovertemplate: "%{x|%b %d, %Y}<br>$%{y:.2f}<extra>%{fullData.name}</extra>",
          });
        }
        if (!traces.length) {
          chartEl.innerHTML = "<div class='hint'>No historical data found for selected rows.</div>";
          setPosHistoryStatus("No history data", false);
          return;
        }
        Plotly.newPlot("posPoolChart", traces, {
          title: "Position TVL history",
          paper_bgcolor: "#ffffff",
          plot_bgcolor: "#f8fbff",
          margin: {t: 34, b: 42, l: 54, r: 12},
          xaxis: {showgrid: true, gridcolor: "#d9e2f0"},
          yaxis: {showgrid: true, gridcolor: "#d9e2f0", tickprefix: "$"},
          showlegend: true,
          legend: {orientation: "h", y: -0.2},
        }, {displaylogo: false, responsive: true});
        setPosHistoryStatus(`History loaded for ${traces.length} position(s).`, false);
      } catch (e) {
        chartEl.innerHTML = `<div class='hint'>Failed to load chart: ${esc(e?.message || "unknown")}</div>`;
        setPosHistoryStatus("History load failed", true);
      }
    }
    function renderPools(rows) {
      const table = document.getElementById("posPoolsTable");
      const trustedSpamKeys = getTrustedSpamKeys();
      const manualHiddenKeys = getManualHiddenKeys();
      const hiddenExpanded = localStorage.getItem(POS_HIDDEN_EXPANDED_KEY) === "1";
      let html = `<tr><th>Address</th><th>Chain</th><th>Protocol</th><th>Pair</th><th>Fee tier</th><th>Pool ID</th><th title='Estimated by liquidity share in pool; shown as - when pool liquidity is unavailable'>Position TVL</th><th>Hide</th><th>History</th></tr>`;
      const list = rows || [];
      const visible = [];
      const hiddenRows = [];
      for (let i = 0; i < list.length; i++) {
        const r0 = list[i];
        const row = Object.assign({_src_idx: i}, r0 || {});
        row._row_key = poolRowKey(row);
        const trusted = trustedSpamKeys.has(row._row_key);
        const manual = manualHiddenKeys.has(row._row_key);
        const suspected = Boolean(row && row.suspected_spam);
        row._is_trusted_spam = trusted;
        row._is_manual_hidden = manual;
        row._is_suspected_spam = suspected;
        if (manual || (suspected && !trusted)) {
          hiddenRows.push(row);
          continue;
        }
        visible.push(row);
      }
      for (let i = 0; i < visible.length; i++) {
        const r = visible[i];
        html += "<tr>";
        html += `<td class='mono' style='font-weight:700'>${esc(shortAddr4(r.address || ""))}<button class='copy-btn' type='button' onclick="copyText('${esc(String(r.address || "").replace(/'/g, "\\\\'"))}')" title='Copy address'>⧉</button></td>`;
        html += `<td>${esc(r.chain || "")}</td>`;
        html += `<td>${esc(r.protocol || "")}</td>`;
        const rowKeyEsc = esc(String(r._row_key || "").replace(/'/g, "\\\\'"));
        html += `<td>${esc(r.pair || "")}</td>`;
        const feeRaw = String(r.fee_tier_raw || "").trim();
        const feeTip = feeRaw ? ` title="raw: ${esc(feeRaw)}"` : "";
        html += `<td${feeTip}>${esc(r.fee_tier || "")}</td>`;
        html += `<td class='mono'>${esc(r.pool_id || "")}<button class='copy-btn' type='button' onclick="copyText('${esc(String(r.pool_id || "").replace(/'/g, "\\\\'"))}')" title='Copy pool id'>⧉</button></td>`;
        const tvlVal = (r.tvl_usd == null) ? "-" : Number(r.tvl_usd).toLocaleString(undefined, {maximumFractionDigits: 2});
        html += `<td>${tvlVal}</td>`;
        html += `<td><input type='checkbox' onchange="setHideRow('${rowKeyEsc}', this.checked, ${Boolean(r._is_suspected_spam) ? "true" : "false"})" /></td>`;
        const checked = posHistorySelected.has(Number(r._src_idx) || 0) ? "checked" : "";
        html += `<td><input type='checkbox' ${checked} onchange='setHistorySelected(${Number(r._src_idx) || 0}, this.checked)' /></td>`;
        html += "</tr>";
      }
      if (!visible.length) html += "<tr><td colspan='9'>No pool positions found.</td></tr>";
      if (hiddenRows.length) {
        let hiddenInner = "<table style='width:100%;border-collapse:collapse;font-size:12px'>";
        hiddenInner += "<tr><th style='text-align:left;padding:4px 6px'>Address</th><th style='text-align:left;padding:4px 6px'>Chain</th><th style='text-align:left;padding:4px 6px'>Protocol</th><th style='text-align:left;padding:4px 6px'>Pair</th><th style='text-align:left;padding:4px 6px'>Pool ID</th><th style='text-align:left;padding:4px 6px'>Position TVL</th><th style='text-align:left;padding:4px 6px'>Hide</th><th style='text-align:left;padding:4px 6px'>History</th></tr>";
        for (let i = 0; i < hiddenRows.length; i++) {
          const r = hiddenRows[i];
          const rowKey = String(r._row_key || "");
          const tvlVal = "-";
          const rowKeyEsc = esc(rowKey.replace(/'/g, "\\\\'"));
          const checked = posHistorySelected.has(Number(r._src_idx) || 0) ? "checked" : "";
          hiddenInner += `<tr><td class='mono' style='padding:3px 6px;font-weight:700'>${esc(shortAddr4(r.address || ""))}</td><td style='padding:3px 6px'>${esc(r.chain || "")}</td><td style='padding:3px 6px'>${esc(r.protocol || "")}</td><td style='padding:3px 6px'>${esc(r.pair || "")}</td><td class='mono' style='padding:3px 6px'>${esc(r.pool_id || "")}</td><td style='padding:3px 6px'>${tvlVal}</td><td style='padding:3px 6px'><input type='checkbox' checked onchange="setHideRow('${rowKeyEsc}', this.checked, ${Boolean(r._is_suspected_spam) ? "true" : "false"})" /></td><td style='padding:3px 6px'><input type='checkbox' ${checked} onchange='setHistorySelected(${Number(r._src_idx) || 0}, this.checked)' /></td></tr>`;
        }
        hiddenInner += "</table>";
        const openAttr = hiddenExpanded ? " open" : "";
        html += `<tr><td colspan='9'><details id='posHiddenDetails'${openAttr}><summary>Hidden positions (${hiddenRows.length})</summary><div style='margin-top:8px;max-height:220px;overflow:auto'>${hiddenInner}</div></details></td></tr>`;
      }
      table.innerHTML = html;
      const detailsEl = document.getElementById("posHiddenDetails");
      if (detailsEl) {
        detailsEl.addEventListener("toggle", () => {
          localStorage.setItem(POS_HIDDEN_EXPANDED_KEY, detailsEl.open ? "1" : "0");
        });
      }
      if (hiddenRows.length > 0) {
        setPosStatus(`Showing ${visible.length} pools (+ ${hiddenRows.length} hidden)`, false);
      }
    }
    async function scanPositions(targetSection = "all") {
      if (posHasScannedOnce) {
        const ok = window.confirm("Run scan again and replace current results?");
        if (!ok) return;
      }
      if (!posState.evm.length && !posState.solana.length && !posState.tron.length) {
        setPosStatus("Add at least one address first.", true);
        return;
      }
      // Search should also reveal the requested section when cache is empty/collapsed.
      if (targetSection === "pools") {
        setSectionCollapsed(targetSection, false);
      } else {
        setSectionCollapsed("pools", false);
      }
      try {
        setPosBusy(true);
        startPosScanProgressTicker();
        const res = await fetch("/api/positions/scan", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({
            evm_addresses: posState.evm,
            solana_addresses: posState.solana,
            tron_addresses: posState.tron,
            include_pools: true,
            include_lending: false,
            include_rewards: false,
          }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || "Scan failed");
        posCache.pools = data.pool_positions || [];
        renderPools(posCache.pools);
        renderScanMessages(data);
        savePosResults({
          saved_at: Date.now(),
          pool_positions: data.pool_positions || [],
          errors: data.errors || [],
          infos: data.infos || [],
          debug: data.debug || {},
        });
        posHasScannedOnce = true;
        updatePosSearchButton();
        setPosStatus(`Done. Pools: ${(data.pool_positions || []).length}`, false);
      } catch (e) {
        setPosStatus("Scan failed: " + (e?.message || "unknown"), true);
      } finally {
        stopPosScanProgressTicker();
        setPosBusy(false);
      }
    }
    loadPosState();
    renderAllChips();
    setSectionCollapsed("pools", false);
    const saved = loadPosResults();
    if (saved && Array.isArray(saved.pool_positions) && saved.pool_positions.length) {
      posCache.pools = saved.pool_positions;
      renderPools(posCache.pools);
      renderScanMessages(saved);
      posHasScannedOnce = true;
      updatePosSearchButton();
      setPosStatus(`Restored cached results. Pools: ${saved.pool_positions.length}`, false);
      setPosHistoryStatus("Cached results restored.", false);
    } else {
      updatePosSearchButton();
      setPosStatus("Ready", false);
      setPosHistoryStatus("Select pools and click Search", false);
    }
    """
    return _render_placeholder_page(
        "DeFi Positions",
        "Find where wallet funds are in pools and lending protocols.",
        "/positions",
        extra_css=extra_css,
        extra_html=extra_html,
        extra_script=extra_script,
        show_intro=False,
    )


def _render_stables_page() -> str:
    extra_css = """
    .positions-grid { display:grid; gap:14px; margin-top:4px; }
    .positions-form, .result-card {
      background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%);
      border: 1px solid #d4deee;
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 20px rgba(15,23,42,0.06);
    }
    .positions-form h3, .result-card h3 { margin:0; font-size:17px; color:#1f3a8a; }
    .section-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:8px; }
    .address-columns { display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:10px; }
    .addr-box { border:1px solid #d7e1ef; border-radius:12px; background:#f8fbff; padding:10px; box-shadow: inset 0 1px 0 rgba(255,255,255,0.7); }
    .addr-input-row { display:grid; grid-template-columns:1fr auto; gap:8px; }
    .addr-input-row input { width:100%; background:#fff; border:1px solid #cbd5e1; border-radius:8px; padding:8px; font-size:13px; }
    .btn-plus { width:26px; min-width:26px; height:26px; border:none; border-radius:0; padding:0; font-size:22px; line-height:1; display:inline-flex; align-items:center; justify-content:center; background:transparent; color:#2563eb; font-weight:800; box-shadow:none; }
    .btn-plus:hover { color:#1d4ed8; background:transparent; }
    .chips { display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; min-height:26px; }
    .chip { display:inline-flex; align-items:center; gap:6px; border:1px solid #bfdbfe; border-radius:999px; padding:4px 8px; background:#eff6ff; color:#1f3a8a; font-size:12px; }
    .chip .x { border:none; background:transparent; color:#1d4ed8; cursor:pointer; font-weight:700; padding:0; line-height:1; }
    .chip.muted { border-style:dashed; color:#64748b; background:#f8fbff; }
    .search-link-btn { border:none; background:transparent; color:#1d4ed8; font-size:13px; font-weight:700; cursor:pointer; padding:0; text-decoration:underline; text-underline-offset:2px; position:relative; z-index:2; pointer-events:auto; }
    .search-link-btn:hover { color:#1e40af; }
    .collapse-btn { border:none; background:transparent; color:#334155; font-size:14px; font-weight:800; cursor:pointer; padding:0 2px; min-width:16px; text-align:center; }
    .section-actions { display:flex; align-items:center; gap:10px; }
    .section-body { display:block; }
    .section-body.collapsed { display:none; }
    .pos-progress { width: 140px; height: 6px; border-radius: 999px; background: #e2e8f0; overflow: hidden; display: none; }
    .pos-progress .bar { width: 40%; height: 100%; background: linear-gradient(90deg, #93c5fd, #2563eb); animation: posLoad 1s linear infinite; }
    @keyframes posLoad { 0% { transform: translateX(-120%); } 100% { transform: translateX(280%); } }
    .pos-status { color:#475569; font-size:13px; }
    .table-wrap { overflow-x:auto; border:1px solid #dbe3ef; border-radius:10px; background:#f8fbff; }
    table { width:100%; border-collapse:collapse; font-size:12px; min-width:900px; }
    th, td { border-bottom:1px solid #e2e8f0; padding:7px; text-align:left; vertical-align:top; }
    th { background:#eff6ff; color:#1e3a8a; position:sticky; top:0; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:11px; }
    .errors-box { margin-top:10px; border:1px dashed #fca5a5; background:#fff1f2; color:#881337; border-radius:10px; padding:8px; font-size:12px; white-space:pre-wrap; }
    .info-box { margin-top:10px; border:1px dashed #bfdbfe; background:#eff6ff; color:#1e3a8a; border-radius:10px; padding:8px; font-size:12px; white-space:pre-wrap; }
    @media (max-width: 1100px) {
      .address-columns { grid-template-columns:1fr; }
      .positions-form, .result-card { padding: 12px; }
      .section-head { flex-wrap: wrap; align-items: center; }
      .section-head h3 { font-size: 16px; }
      .search-link-btn { font-size: 12px; }
    }
    @media (max-width: 720px) {
      .positions-grid { gap: 10px; }
      .positions-form, .result-card { padding: 10px; border-radius: 12px; }
      .addr-box { padding: 8px; }
      .addr-input-row { gap: 6px; }
      .addr-input-row input { font-size: 12px; padding: 7px; }
      .btn-plus { width: 24px; min-width: 24px; height: 24px; font-size: 20px; }
      .chip { font-size: 11px; padding: 3px 7px; }
      .pos-status { font-size: 12px; }
      .table-wrap { border-radius: 8px; }
      table { min-width: 740px; font-size: 11px; }
      th, td { padding: 6px; }
      .mono { font-size: 10px; }
      .errors-box, .info-box { font-size: 11px; padding: 7px; }
    }
    """
    extra_html = """
    <div class="positions-grid">
      <section class="positions-form">
        <div class="section-head"><h3>Lending Stablecoin</h3></div>
        <div class="address-columns">
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="evmInput" placeholder="0x..." />
              <button class="btn btn-plus" type="button" onclick="addAddress('evm')" aria-label="Add EVM address">+</button>
            </div>
            <div class="chips" id="evmChips"></div>
          </div>
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="solanaInput" placeholder="Solana address" />
              <button class="btn btn-plus" type="button" onclick="addAddress('solana')" aria-label="Add Solana address">+</button>
            </div>
            <div class="chips" id="solanaChips"></div>
          </div>
          <div class="addr-box">
            <div class="addr-input-row">
              <input id="tronInput" placeholder="TRON address" />
              <button class="btn btn-plus" type="button" onclick="addAddress('tron')" aria-label="Add TRON address">+</button>
            </div>
            <div class="chips" id="tronChips"></div>
          </div>
        </div>
      </section>
      <section class="result-card">
        <div class="section-head">
          <h3 id="stableCombinedTitle">Lending positions and unclaimed rewards</h3>
          <div class="section-actions">
            <div id="stableProgress" class="pos-progress"><div class="bar"></div></div>
            <span class="pos-status" id="stableStatus">Ready</span>
            <button class="search-link-btn" type="button" onclick="scanStable()">Search</button>
            <button class="collapse-btn" id="toggleStableCombinedBtn" type="button" onclick="toggleStableSection()" title="Collapse/expand">▾</button>
          </div>
        </div>
        <div id="stableCombinedBody" class="section-body">
          <div id="stableLendingHeading" style="margin-bottom:8px;font-weight:700;color:#1e3a8a;display:none">Lending positions</div>
          <div class="table-wrap"><table id="stableLendingTable"></table></div>
          <div id="stableRewardsHeading" style="margin:12px 0 8px;font-weight:700;color:#1e3a8a;display:none">Unclaimed lending rewards</div>
          <div class="table-wrap"><table id="stableRewardsTable"></table></div>
          <div id="stableErrors"></div>
        </div>
      </section>
    </div>
    """
    extra_script = """
    function esc(v) { return String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }
    const stableState = { evm: [], solana: [], tron: [] };
    const STABLE_STORAGE_KEY = "stables_form_v1";
    function validAddress(kind, value) {
      const v = String(value || "").trim();
      if (!v) return false;
      if (kind === "evm") return /^0x[a-fA-F0-9]{40}$/.test(v);
      if (kind === "solana") return /^[1-9A-HJ-NP-Za-km-z]{32,44}$/.test(v);
      if (kind === "tron") return /^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(v);
      return false;
    }
    function inputId(kind) {
      if (kind === "evm") return "evmInput";
      if (kind === "solana") return "solanaInput";
      return "tronInput";
    }
    function chipsId(kind) {
      if (kind === "evm") return "evmChips";
      if (kind === "solana") return "solanaChips";
      return "tronChips";
    }
    function saveStableState() { localStorage.setItem(STABLE_STORAGE_KEY, JSON.stringify(stableState)); }
    function loadStableState() {
      try {
        const raw = localStorage.getItem(STABLE_STORAGE_KEY);
        if (!raw) return;
        const parsed = JSON.parse(raw);
        for (const k of ["evm", "solana", "tron"]) {
          if (Array.isArray(parsed[k])) stableState[k] = parsed[k].map((x) => String(x || "").trim()).filter(Boolean);
        }
      } catch (_) {}
    }
    function renderChips(kind) {
      const wrap = document.getElementById(chipsId(kind));
      if (!wrap) return;
      const rows = stableState[kind] || [];
      if (!rows.length) { wrap.innerHTML = "<span class='chip muted'>No addresses</span>"; return; }
      wrap.innerHTML = rows.map((addr, i) => `<span class='chip'><span class='mono'>${esc(addr)}</span><button class='x' type='button' onclick=\"removeAddress('${kind}', ${i})\">×</button></span>`).join("");
    }
    function renderAllChips() { renderChips("evm"); renderChips("solana"); renderChips("tron"); }
    function addAddress(kind) {
      const el = document.getElementById(inputId(kind));
      const addrRaw = String(el?.value || "").trim();
      if (!validAddress(kind, addrRaw)) { setStableStatus(`Invalid ${kind.toUpperCase()} address format.`, true); return; }
      const addr = addrRaw;
      const dup = (stableState[kind] || []).some((x) => kind === "evm" ? String(x).toLowerCase() === addr.toLowerCase() : String(x) === addr);
      if (dup) { setStableStatus("Address already added.", true); return; }
      stableState[kind].push(addr);
      if (el) el.value = "";
      saveStableState();
      renderChips(kind);
      setStableStatus("Address added.", false);
    }
    function removeAddress(kind, idx) {
      if (!Array.isArray(stableState[kind])) return;
      stableState[kind].splice(Number(idx) || 0, 1);
      saveStableState();
      renderChips(kind);
    }
    const stableSectionState = {combined: false};
    const stableCache = {lending: [], rewards: []};
    function setStableStatus(text, isErr) {
      const el = document.getElementById("stableStatus");
      if (!el) return;
      el.textContent = text || "";
      el.style.color = isErr ? "#b91c1c" : "#475569";
    }
    function setStableBusy(flag) {
      const el = document.getElementById("stableProgress");
      if (!el) return;
      el.style.display = flag ? "block" : "none";
    }
    function setStableSectionCollapsed(collapsed) {
      const body = document.getElementById("stableCombinedBody");
      const btn = document.getElementById("toggleStableCombinedBtn");
      const title = document.getElementById("stableCombinedTitle");
      const hL = document.getElementById("stableLendingHeading");
      const hR = document.getElementById("stableRewardsHeading");
      if (!body || !btn) return;
      stableSectionState.combined = !!collapsed;
      body.classList.toggle("collapsed", !!collapsed);
      btn.textContent = collapsed ? "▸" : "▾";
      if (title) title.style.visibility = collapsed ? "visible" : "hidden";
      if (hL) hL.style.display = collapsed ? "none" : "block";
      if (hR) hR.style.display = collapsed ? "none" : "block";
    }
    function toggleStableSection() {
      const next = !stableSectionState.combined;
      setStableSectionCollapsed(next);
      if (!next) {
        renderLending(stableCache.lending || []);
        renderRewards(stableCache.rewards || []);
      }
    }
    function renderLending(rows) {
      const table = document.getElementById("stableLendingTable");
      let html = "<tr><th>Address</th><th>Chain</th><th>Protocol</th><th>Type</th><th>Market</th><th>Asset</th><th>Amount</th><th>USD</th><th>APY</th><th>Collateral</th></tr>";
      for (const r of (rows || [])) {
        html += "<tr>";
        html += `<td class='mono'>${esc(r.address || "")}</td>`;
        html += `<td>${esc(r.chain || "")}</td>`;
        html += `<td>${esc(r.protocol || "")}</td>`;
        html += `<td>${esc(r.kind || "")}</td>`;
        html += `<td>${esc(r.market || "")}</td>`;
        html += `<td>${esc(r.asset || "")}</td>`;
        html += `<td>${Number(r.amount || 0).toLocaleString(undefined, {maximumFractionDigits: 6})}</td>`;
        html += `<td>${Number(r.usd || 0).toLocaleString(undefined, {maximumFractionDigits: 2})}</td>`;
        html += `<td>${Number(r.apy || 0).toFixed(4)}</td>`;
        html += `<td>${r.is_collateral ? "yes" : ""}</td>`;
        html += "</tr>";
      }
      if (!(rows || []).length) html += "<tr><td colspan='10'>No lending positions found.</td></tr>";
      table.innerHTML = html;
    }
    function renderRewards(rows) {
      const table = document.getElementById("stableRewardsTable");
      let html = "<tr><th>Address</th><th>Chain</th><th>Protocol</th><th>Asset</th><th>Amount</th><th>USD</th></tr>";
      for (const r of (rows || [])) {
        html += "<tr>";
        html += `<td class='mono'>${esc(r.address || "")}</td>`;
        html += `<td>${esc(r.chain || "")}</td>`;
        html += `<td>${esc(r.protocol || "")}</td>`;
        html += `<td>${esc(r.asset || "")}</td>`;
        html += `<td>${Number(r.amount || 0).toLocaleString(undefined, {maximumFractionDigits: 6})}</td>`;
        html += `<td>${Number(r.usd || 0).toLocaleString(undefined, {maximumFractionDigits: 2})}</td>`;
        html += "</tr>";
      }
      if (!(rows || []).length) html += "<tr><td colspan='6'>No unclaimed rewards found.</td></tr>";
      table.innerHTML = html;
    }
    async function scanStable() {
      if (!stableState.evm.length && !stableState.solana.length && !stableState.tron.length) {
        setStableStatus("Add at least one address first.", true);
        return;
      }
      setStableSectionCollapsed(false);
      try {
        setStableBusy(true);
        setStableStatus("Scanning latest lending and rewards...", false);
        const res = await fetch("/api/positions/scan", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({
            evm_addresses: stableState.evm,
            solana_addresses: stableState.solana,
            tron_addresses: stableState.tron,
            include_pools: false,
            include_lending: true,
            include_rewards: true,
          }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.detail || "Scan failed");
        stableCache.lending = data.lending_positions || [];
        stableCache.rewards = data.reward_positions || [];
        renderLending(stableCache.lending);
        renderRewards(stableCache.rewards);
        const errWrap = document.getElementById("stableErrors");
        const errs = data.errors || [];
        const infos = data.infos || [];
        const errHtml = errs.length ? `<div class='errors-box'>${esc(errs.join("\\n"))}</div>` : "";
        const infoHtml = infos.length ? `<div class='info-box'>${esc(infos.join("\\n"))}</div>` : "";
        if (errWrap) errWrap.innerHTML = errHtml + infoHtml;
        setStableStatus(`Done. Lending: ${(data.lending_positions || []).length}, Rewards: ${(data.reward_positions || []).length}`, false);
      } catch (e) {
        setStableStatus("Scan failed: " + (e?.message || "unknown"), true);
      } finally {
        setStableBusy(false);
      }
    }
    loadStableState();
    renderAllChips();
    setStableSectionCollapsed(false);
    setStableStatus("Ready", false);
    """
    return _render_placeholder_page(
        "Lending Stablecoin",
        "Scan lending positions and unclaimed rewards.",
        "/stables",
        extra_css=extra_css,
        extra_html=extra_html,
        extra_script=extra_script,
        show_intro=False,
    )


def _render_admin_page() -> str:
    options_html = _intent_options_html("/admin") + '\n<option value="/admin" selected>Administer project</option>'
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Uni Fee - Admin</title>
  <style>
    * {{ box-sizing: border-box; }}
    html {{ overflow-y: scroll; scrollbar-gutter: stable; overflow-x: hidden; }}
    body {{
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: linear-gradient(180deg, #d9e3f5 0%, #ecf2ff 100%);
      color: #0f172a;
      min-height: 100vh;
      overflow-x: hidden;
    }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 18px; min-height: calc(100vh - 36px); }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 14px; }}
    .title {{ margin: 0; font-size: 30px; font-weight: 800; letter-spacing: 0.2px; }}
    .subtitle {{ margin: 4px 0 0; color: #64748b; font-size: 14px; }}
    .top-controls {{ display: flex; gap: 10px; align-items: center; justify-content: flex-end; flex-wrap: nowrap; }}
    .intent-prefix {{ font-size: 14px; font-weight: 700; color: #1d4ed8; white-space: nowrap; }}
    .intent-select {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 38px 10px 12px; font-size: 14px; font-weight: 600; color: #1f3a8a; background: linear-gradient(180deg, #f8fbff 0%, #eff6ff 100%); min-width: 320px; max-width: 360px; appearance: none; -webkit-appearance: none; background-image: linear-gradient(45deg, transparent 50%, #1d4ed8 50%), linear-gradient(135deg, #1d4ed8 50%, transparent 50%); background-position: calc(100% - 18px) calc(50% + 1px), calc(100% - 12px) calc(50% + 1px); background-size: 6px 6px, 6px 6px; background-repeat: no-repeat; box-shadow: inset 0 1px 0 rgba(255,255,255,0.7); }}
    .intent-select option {{ background: #eef4ff; color: #1f3a8a; }}
    .connect-btn {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 16px; font-size: 14px; font-weight: 700; color: #1d4ed8; background: #eff6ff; cursor: pointer; white-space: nowrap; width: 190px; box-sizing: border-box; overflow: hidden; text-overflow: ellipsis; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; }}
    .tabs {{ display: flex; gap: 8px; margin-bottom: 8px; }}
    .tab-btn {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 8px 12px; font-size: 13px; font-weight: 700; color: #1d4ed8; background: #eff6ff; cursor: pointer; }}
    .tab-btn.active {{ background: #dbeafe; border-color: #93c5fd; }}
    .card {{ background: #f3f7ff; border: 1px solid #cfdcec; border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06); }}
    .card h3 {{ margin: 0 0 10px; font-size: 18px; }}
    .hint {{ color: #64748b; font-size: 13px; margin: 0 0 10px; }}
    input, select, textarea {{ width: 100%; background: #f8fbff; border: 1px solid #cbd5e1; color: #0f172a; border-radius: 8px; padding: 8px; font-size: 14px; }}
    .row {{ display: grid; grid-template-columns: 170px 1fr; gap: 10px; align-items: center; margin-bottom: 8px; }}
    .btn {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 9px 14px; font-size: 14px; font-weight: 700; color: #1d4ed8; background: #eff6ff; cursor: pointer; }}
    .btn-soft {{ background: #f8fbff; }}
    .status {{ font-size: 13px; color: #475569; margin-left: 8px; }}
    .table-wrap {{ overflow-x: auto; border: 1px solid #dbe3ef; border-radius: 10px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; min-width: 1100px; }}
    th, td {{ border-bottom: 1px solid #e2e8f0; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #eff6ff; color: #1e3a8a; position: sticky; top: 0; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11px; }}
    pre {{ max-height: 320px; overflow: auto; background: #f8fafc; border: 1px solid #dbe3ef; border-radius: 8px; padding: 10px; color: #334155; font-size: 12px; white-space: pre-wrap; }}
    .wallet-modal-backdrop {{ position: fixed; inset: 0; background: linear-gradient(180deg, rgba(217,227,245,0.82) 0%, rgba(236,242,255,0.82) 100%); backdrop-filter: blur(3px); display: none; align-items: center; justify-content: center; z-index: 9999; }}
    .wallet-modal {{ width: min(460px, calc(100vw - 24px)); background: #f8fbff; border: 1px solid #cbd5e1; border-radius: 14px; box-shadow: 0 12px 36px rgba(15,23,42,0.25); padding: 14px; }}
    .wallet-modal h3 {{ margin: 0 0 8px; font-size: 18px; }}
    .wallet-list {{ display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 10px; }}
    .wallet-item {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 12px; background: #eff6ff; color: #1d4ed8; font-weight: 700; text-align: left; cursor: pointer; }}
    .wallet-item.disabled {{ opacity: 0.6; cursor: not-allowed; }}
    .wallet-note {{ color: #64748b; font-size: 12px; margin-top: 8px; }}
    .ticket-controls {{ margin-top: 8px; padding: 10px; border: 1px solid #dbeafe; border-radius: 10px; background: #f8fbff; }}
    .ticket-toolbar {{ display: grid; grid-template-columns: 150px 220px 130px minmax(220px, 1fr) auto; gap: 8px; align-items: end; margin-top: 2px; }}
    .ticket-filters {{ display: contents; }}
    .ticket-filter-item label {{ display: block; font-size: 11px; color: #64748b; margin-bottom: 4px; }}
    .ticket-filter-actions {{ display: contents; }}
    .ticket-intro {{ margin: 8px 0 14px; font-size: 16px; font-weight: 700; color: #475569; line-height: 1.35; }}
    .tickets-results {{ margin-top: 14px; border-top: 2px solid #dbeafe; padding-top: 12px; }}
    .tickets-board {{ display: grid; gap: 10px; margin-top: 10px; }}
    .ticket-card {{ border: 1px solid #dbe3ef; border-radius: 10px; background: #f8fbff; padding: 10px; }}
    .ticket-head {{ display: grid; grid-template-columns: 120px 1fr 190px; gap: 10px; align-items: start; }}
    .ticket-head .meta {{ font-size: 12px; color: #334155; }}
    .ticket-head .mono {{ font-size: 11px; }}
    .ticket-status-badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; border: 1px solid #cbd5e1; font-weight: 700; font-size: 11px; }}
    .ticket-status-badge.active {{ color: #15803d !important; border-color: #86efac; background: #f0fdf4; }}
    .ticket-status-badge.done {{ color: #475569 !important; border-color: #cbd5e1; background: #f8fafc; }}
    .ticket-author {{ margin-top: 2px; border: 1px dashed #bfdbfe; border-radius: 10px; background: #eef4ff; padding: 10px; display: grid; grid-template-columns: 1fr 1fr; gap: 8px 12px; }}
    .ticket-author .label {{ font-size: 12px; color: #64748b; margin-right: 4px; font-weight: 600; }}
    .ticket-author .value {{ font-size: 14px; color: #0f172a; word-break: break-all; }}
    .ticket-message-block {{ margin-top: 8px; }}
    .ticket-message-block .label, .ticket-reply-block .label {{ font-size: 11px; color: #64748b; margin-bottom: 4px; }}
    .ticket-message {{ margin: 0; max-height: 120px; overflow: auto; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 8px; font-size: 12px; white-space: pre-wrap; color: #334155; }}
    .ticket-thread-details > summary {{ cursor: pointer; color: #1d4ed8; font-size: 12px; font-weight: 700; margin-bottom: 4px; }}
    .admin-thread {{ display: grid; gap: 8px; }}
    .admin-msg-bubble {{ border: 1px solid #dbe3ef; border-radius: 10px; padding: 8px 10px; white-space: pre-wrap; font-size: 13px; color: #334155; background: #f8fbff; }}
    .admin-msg-bubble.user {{ background: #eef4ff; border-color: #c7dbff; }}
    .admin-msg-bubble.admin {{ background: #f0fdf4; border-color: #bbf7d0; }}
    .admin-msg-head {{ font-size: 11px; color: #64748b; margin-bottom: 4px; display: flex; justify-content: space-between; gap: 8px; }}
    .ticket-reply-block {{ margin-top: 8px; display: grid; grid-template-columns: 1fr auto; gap: 10px; align-items: start; }}
    .ticket-reply {{ min-height: 150px; resize: vertical; font-size: 13px; line-height: 1.35; }}
    .ticket-actions {{ display: flex; flex-direction: column; gap: 8px; min-width: 190px; }}
    .ticket-actions .btn {{ width: 100%; text-align: center; }}
    .btn-soft {{ background: #f8fbff; }}
    .btn-danger {{ border-color: #fecaca; color: #b91c1c; background: #fef2f2; }}
    .feedback-board {{ display: grid; gap: 10px; margin-top: 10px; }}
    .feedback-meta {{ margin-top: 6px; font-size: 12px; color: #64748b; }}
    @media (max-width: 980px) {{ .row {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 1100px) {{
      .ticket-controls {{ padding: 8px; }}
      .ticket-toolbar {{ grid-template-columns: 1fr 1fr; }}
      .ticket-head {{ grid-template-columns: 1fr 1fr; }}
      .ticket-author {{ grid-template-columns: 1fr; }}
      .ticket-reply-block {{ grid-template-columns: 1fr; }}
      .ticket-actions {{ flex-direction: row; flex-wrap: wrap; }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">DeFi Pools</h1>
        <p class="subtitle">Administer project</p>
      </div>
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">{options_html}</select>
        <button class="connect-btn" id="connectWalletBtn" onclick="onConnectWalletClick()">Connect Wallet</button>
      </div>
    </div>
    <div class="tabs">
      <button class="tab-btn active" id="tabBtnSettings" onclick="switchTab('settings')">Settings</button>
      <button class="tab-btn" id="tabBtnStats" onclick="switchTab('stats')">Stats</button>
      <button class="tab-btn" id="tabBtnFailures" onclick="switchTab('failures')">Failed runs</button>
      <button class="tab-btn" id="tabBtnTickets" onclick="switchTab('tickets')">Help tickets</button>
      <button class="tab-btn" id="tabBtnFeedback" onclick="switchTab('feedback')">Feedback</button>
      <button class="tab-btn" id="tabBtnFaq" onclick="switchTab('faq')">FAQ</button>
    </div>
    <div class="grid" id="tabSettings">
      <section class="card">
        <h3>Admin access</h3>
        <p class="hint">Manage wallets with admin rights.</p>
        <div class="row"><label>Add admin wallet</label><input id="newAdminWallet" type="text" placeholder="0x..."/></div>
        <button class="btn" onclick="addAdminWallet()">Add admin</button>
        <div class="row"><label>Admin wallets</label><div id="adminWalletsList">-</div></div>
      </section>
      <section class="card">
        <h3>Project summary</h3>
        <div class="row"><label>Analytics DB</label><div id="dbPath">-</div></div>
        <div class="row"><label>Tracked events</label><div id="eventsCount">-</div></div>
        <div class="row"><label>Token catalog</label><div id="tokenCatalogInfo">-</div></div>
        <span id="adminStatus" class="status">Ready</span>
      </section>
    </div>
    <div class="grid" id="tabStats" style="display:none">
      <section class="card">
        <h3>Stats by period</h3>
        <p class="hint">Browse analytics by day, week, month, or year.</p>
        <div class="row"><label>Period</label><select id="statsPeriod"><option value="day">day</option><option value="week">week</option><option value="month">month</option><option value="year">year</option></select></div>
        <div class="row"><label>Rows</label><input id="statsLimit" type="number" min="1" max="365" step="1" value="30"/></div>
        <button class="btn" onclick="loadStats()">Refresh stats</button>
        <span id="statsStatus" class="status">Ready</span>
        <div class="table-wrap" style="margin-top:10px">
          <table id="statsTable"></table>
        </div>
      </section>
    </div>
    <div class="grid" id="tabFailures" style="display:none">
      <section class="card">
        <h3>Recent failed runs</h3>
        <p class="hint">Manual review of latest run_failed records.</p>
        <button class="btn" onclick="loadFailures()">Refresh failures</button>
        <span id="failStatus" class="status">Ready</span>
        <div class="table-wrap" style="margin-top:10px">
          <table id="failuresTable"></table>
        </div>
      </section>
    </div>
    <div class="grid" id="tabTickets" style="display:none">
      <section class="card">
        <p class="ticket-intro">Tickets submitted from Send wishes or report issues page. Use quick actions and collapsed conversation view.</p>
        <div class="ticket-controls">
          <div class="ticket-toolbar">
            <div class="ticket-filters">
            <div class="ticket-filter-item">
              <label>Status</label>
              <select id="ticketFilterStatus"><option value="">all</option><option value="open">active</option><option value="in_progress">in progress</option><option value="done">closed</option></select>
            </div>
            <div class="ticket-filter-item">
              <label>Email</label>
              <input id="ticketFilterEmail" type="text" placeholder="example@domain.com"/>
            </div>
            <div class="ticket-filter-item">
              <label>Ticket #</label>
              <input id="ticketFilterNo" type="text" placeholder="12000"/>
            </div>
            <div class="ticket-filter-item">
              <label>Text</label>
              <input id="ticketFilterText" type="text" placeholder="search in subject/message/reply"/>
            </div>
            </div>
            <div class="ticket-filter-actions">
            <button class="btn" onclick="resetTicketsFilter()">Reset filters</button>
            </div>
          </div>
          <span id="ticketsStatus" class="status">Ready</span>
        </div>
        <div class="tickets-results"><div id="ticketsBoard" class="tickets-board"></div></div>
      </section>
    </div>
    <div class="grid" id="tabFeedback" style="display:none">
      <section class="card">
        <h3>Wishes and issue reports</h3>
        <p class="hint">Messages from Send wishes or report issues page. Press Reviewed to archive and collapse.</p>
        <span id="feedbackStatus" class="status">Ready</span>
        <div id="feedbackBoard" class="feedback-board"></div>
      </section>
    </div>
    <div class="grid" id="tabFaq" style="display:none">
      <section class="card">
        <h3>FAQ management</h3>
        <p class="hint">Create, edit, publish FAQ items shown on Send wishes or report issues page.</p>
        <input id="faqId" type="hidden" value="" />
        <input id="faqPublished" type="hidden" value="true" />
        <div class="row"><label>Question</label><input id="faqQuestion" type="text" placeholder="Question"/></div>
        <div class="row"><label>Answer</label><textarea id="faqAnswer" placeholder="Answer text"></textarea></div>
        <div class="row"><label>Featured</label><select id="faqFeatured"><option value="true">yes</option><option value="false">no</option></select></div>
        <div class="row"><label>Sort order</label><input id="faqSortOrder" type="number" min="0" step="1" value="100"/></div>
        <button class="btn" onclick="saveFaq()">Save FAQ</button>
        <button class="btn" onclick="clearFaqForm()">New FAQ</button>
        <span id="faqStatus" class="status">Ready</span>
        <div class="table-wrap" style="margin-top:10px">
          <table id="faqTable"></table>
        </div>
      </section>
    </div>
  </div>
  <div id="walletModalBackdrop" class="wallet-modal-backdrop" onclick="closeWalletModal(event)">
    <div class="wallet-modal">
      <h3>Connect wallet</h3>
      <div class="wallet-list" id="walletList"></div>
      <div class="wallet-note">Rabby and Phantom are supported. Sign-in uses a gasless message signature.</div>
    </div>
  </div>
  <script>
    let authState = {{authenticated: false}};
    const WALLETCONNECT_PROJECT_ID = "__WALLETCONNECT_PROJECT_ID__";
    const WALLET_LABELS = {{ injected: "Browser Wallet", walletconnect: "WalletConnect (QR)", rabby: "Rabby", metamask: "MetaMask", phantom: "Phantom", coinbase: "Coinbase Wallet" }};
    function navigateIntent(path) {{ if (!path) return; window.location.href = path; }}
    function refreshIntentMenu() {{ const sel=document.getElementById("intentSelect"); if(!sel) return; sel.style.position="absolute"; sel.style.left="-9999px"; sel.style.opacity="0"; sel.style.pointerEvents="none"; let wrap=document.getElementById("intentMenuWrap"); if(!wrap) {{ wrap=document.createElement("div"); wrap.id="intentMenuWrap"; wrap.style.cssText="position:relative;min-width:320px;max-width:360px;"; const btn=document.createElement("button"); btn.type="button"; btn.id="intentMenuBtn"; btn.style.cssText="width:100%;border:1px solid #bfdbfe;border-radius:10px;padding:10px 38px 10px 12px;font-size:14px;font-weight:600;color:#1f3a8a;background:linear-gradient(180deg,#f8fbff 0%,#eff6ff 100%);text-align:left;cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,0.7);"; const list=document.createElement("div"); list.id="intentMenuList"; list.style.cssText="display:none;position:absolute;z-index:12000;left:0;right:0;top:calc(100% + 6px);background:#eef4ff;border:1px solid #bfdbfe;border-radius:10px;box-shadow:0 10px 24px rgba(15,23,42,0.15);padding:6px;max-height:320px;overflow:auto;"; wrap.appendChild(btn); wrap.appendChild(list); sel.insertAdjacentElement("afterend",wrap); btn.onclick=()=>{{ list.style.display=list.style.display==="block"?"none":"block"; }}; document.addEventListener("click",(e)=>{{ if(!wrap.contains(e.target)) list.style.display="none"; }}); }} const btn=document.getElementById("intentMenuBtn"); const list=document.getElementById("intentMenuList"); const options=Array.from(sel.options||[]); const selected=options.find((o)=>o.selected)||options[0]; btn.textContent=selected?selected.textContent:"Select"; list.innerHTML=options.map((o)=>{{ const active=o.value===sel.value; const style=active?"display:block;width:100%;padding:9px 10px;border:none;background:#dbeafe;color:#1e3a8a;font-weight:700;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;":"display:block;width:100%;padding:9px 10px;border:none;background:#eef4ff;color:#1f3a8a;font-weight:600;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;"; return `<button type="button" data-v="${{o.value}}" style="${{style}}">${{o.textContent}}</button>`; }}).join(""); Array.from(list.querySelectorAll("button[data-v]")).forEach((b)=>{{ b.onclick=()=>{{ const v=b.getAttribute("data-v")||""; sel.value=v; list.style.display="none"; navigateIntent(v); }}; }}); }}
    function getEthereumProviders() {{ const out=[]; const eth=window.ethereum; if(!eth) return out; if(Array.isArray(eth.providers)&&eth.providers.length) return eth.providers; out.push(eth); return out; }}
    function getWalletProvider(wallet) {{ const providers=getEthereumProviders(); const pick=(pred)=>providers.find(pred)||null; if(wallet==="injected") return pick((p)=>!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||pick((p)=>!!p?.isMetaMask&&!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||pick((p)=>!!p?.isCoinbaseWallet)||providers[0]||window.ethereum||null; if(wallet==="rabby") return pick((p)=>!!p?.isRabby)||(window.ethereum?.isRabby?window.ethereum:null); if(wallet==="phantom"){{ if(window.phantom?.ethereum?.request) return window.phantom.ethereum; return pick((p)=>!!p?.isPhantom)||(window.ethereum?.isPhantom?window.ethereum:null); }} if(wallet==="metamask") return pick((p)=>!!p?.isMetaMask&&!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||((window.ethereum?.isMetaMask&&!window.ethereum?.isRabby&&!window.ethereum?.isPhantom&&!window.ethereum?.isCoinbaseWallet)?window.ethereum:null); if(wallet==="coinbase") return pick((p)=>!!p?.isCoinbaseWallet)||(window.ethereum?.isCoinbaseWallet?window.ethereum:null); return null; }}
    function getWalletChoices() {{ const order=["walletconnect","rabby","phantom","metamask","coinbase","injected"]; return order.map((id)=>{{ const isWc=id==="walletconnect"; const available=isWc?true:!!getWalletProvider(id); let label=WALLET_LABELS[id]; if(isWc&&!WALLETCONNECT_PROJECT_ID) label+=" (setup required)"; else if(!available) label+=" (not detected)"; return {{id,label,available}}; }}); }}
    function openWalletModal() {{ const list=document.getElementById("walletList"); const choices=getWalletChoices(); list.innerHTML=choices.map((w)=>{{ const cls=w.available?"wallet-item":"wallet-item disabled"; const dis=w.available?"":"disabled"; return `<button class="${{cls}}" ${{dis}} onclick="connectWalletFlow('${{w.id}}')">${{w.label}}</button>`; }}).join(""); document.getElementById("walletModalBackdrop").style.display="flex"; }}
    function closeWalletModal(event) {{ if(event&&event.target&&event.target.id!=="walletModalBackdrop") return; document.getElementById("walletModalBackdrop").style.display="none"; }}
    async function postJson(url,payload) {{ const r=await fetch(url,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify(payload||{{}})}}); const data=await r.json().catch(()=>({{}})); if(!r.ok) throw new Error(data.detail||data.info||"Request failed"); return data; }}
    function syncAdminIntentOption() {{ const sel=document.getElementById("intentSelect"); if(!sel) return; const existing=Array.from(sel.options).find((o)=>o.value==="/admin"); const isAdmin=!!authState?.authenticated&&!!authState?.is_admin; if(isAdmin&&!existing) {{ const opt=document.createElement("option"); opt.value="/admin"; opt.textContent="Administer project"; sel.appendChild(opt); }} else if(!isAdmin&&existing) {{ existing.remove(); }} refreshIntentMenu(); }}
    function setAuthUI() {{ const btn=document.getElementById("connectWalletBtn"); if(!btn) return; if(authState?.authenticated) {{ btn.textContent=authState.address_short||"Wallet connected"; }} else {{ btn.textContent="Connect Wallet"; }} syncAdminIntentOption(); }}
    async function loadAuthState() {{ try {{ const r=await fetch("/api/auth/me"); authState=await r.json(); }} catch(_) {{ authState={{authenticated:false}}; }} setAuthUI(); }}
    async function onConnectWalletClick() {{ if(authState?.authenticated) {{ if(!confirm("Disconnect wallet?")) return; try {{ await postJson("/api/auth/logout",{{}}); authState={{authenticated:false}}; setAuthUI(); }} catch(e) {{ console.warn("disconnect failed",e); }} return; }} openWalletModal(); }}
    async function connectWalletFlow(wallet) {{ if(wallet==="walletconnect") return connectWalletConnect(); const provider=getWalletProvider(wallet); if(!provider) return; try {{ const accounts=await provider.request({{method:"eth_requestAccounts"}}); const address=String((accounts||[])[0]||"").trim(); if(!address) throw new Error("Wallet did not return an address"); const chainHex=await provider.request({{method:"eth_chainId"}}); const chainId=Number.parseInt(String(chainHex||"0x1"),16)||1; const nonceResp=await postJson("/api/auth/nonce",{{address,chain_id:chainId,wallet}}); const signature=await provider.request({{method:"personal_sign",params:[nonceResp.message,address]}}); const verifyResp=await postJson("/api/auth/verify",{{address,chain_id:chainId,wallet,message:nonceResp.message,signature}}); authState={{authenticated:true,...verifyResp}}; setAuthUI(); closeWalletModal({{target:{{id:"walletModalBackdrop"}}}}); location.reload(); }} catch(e) {{ console.warn("wallet auth failed",e); }} }}
    function showWcQrModal(uri){{ let el=document.getElementById("wcQrBackdrop"); if(!el){{ el=document.createElement("div"); el.id="wcQrBackdrop"; el.style.cssText="position:fixed;inset:0;background:linear-gradient(180deg,rgba(217,227,245,0.95),rgba(236,242,255,0.95));backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:10001;"; el.innerHTML='<div style="background:#f8fbff;border:1px solid #cbd5e1;border-radius:14px;padding:20px;text-align:center"><p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#0f172a">Scan with your wallet app</p><img id="wcQrImg" alt="QR" style="display:block;background:#fff;padding:10px;border-radius:10px;width:260px;height:260px"/><button id="wcQrCancel" type="button" style="margin-top:14px;padding:8px 16px;border-radius:10px;border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;font-weight:700;cursor:pointer">Cancel</button></div>'; document.body.appendChild(el); document.getElementById("wcQrCancel").onclick=closeWcQrModal; }} document.getElementById("wcQrImg").src="https://api.qrserver.com/v1/create-qr-code/?size=260x260&data="+encodeURIComponent(uri); el.style.display="flex"; }}
    function closeWcQrModal(){{ const el=document.getElementById("wcQrBackdrop"); if(el) el.style.display="none"; if(window._wcProvider) try {{ window._wcProvider.disconnect(); }} catch(_) {{}} window._wcProvider=null; }}
    async function connectWalletConnect() {{
      if(!WALLETCONNECT_PROJECT_ID) return alert("WalletConnect is not configured (WALLETCONNECT_PROJECT_ID).");
      const normalizeAddress=(value)=>{{ const raw=String(value||"").trim(); if(!raw) return ""; const parts=raw.split(":"); return String(parts[parts.length-1]||"").trim(); }};
      const toHexMessage=(msg)=>{{ try {{ return "0x"+Array.from(new TextEncoder().encode(String(msg||""))).map((b)=>b.toString(16).padStart(2,"0")).join(""); }} catch(_) {{ return ""; }} }};
      try {{
        const EthereumProviderModule=await import("https://esm.sh/@walletconnect/ethereum-provider@2.23.8");
        const wcChains=[1,10,56,137,8453,42161,43114];
        const wcMetadata={{name:"DeFi Pools",description:"DeFi Pools wallet sign-in",url:window.location.origin,icons:[window.location.origin+"/favicon.ico"]}};
        const provider=await EthereumProviderModule.EthereumProvider.init({{projectId:WALLETCONNECT_PROJECT_ID,optionalChains:wcChains,showQrModal:false,optionalMethods:["eth_requestAccounts","eth_accounts","eth_chainId","personal_sign","wallet_switchEthereumChain"],optionalEvents:["accountsChanged","chainChanged","disconnect"],metadata:wcMetadata,rpcMap:{{}}}});
        provider.on("display_uri",showWcQrModal);
        window._wcProvider=provider;
        let connected=false; try {{ await provider.connect(); connected=true; }} catch(_) {{}}
        if(!connected) {{ try {{ await provider.enable(); }} catch(connErr) {{ closeWcQrModal(); throw connErr; }} }}
        window._wcProvider=null;
        closeWcQrModal();
        let accounts=provider.accounts||[];
        if(!accounts.length) accounts=(await provider.request({{method:"eth_accounts"}}))||[];
        if(!accounts.length) accounts=(await provider.request({{method:"eth_requestAccounts"}}))||[];
        const address=normalizeAddress(accounts[0]||"");
        if(!/^0x[a-fA-F0-9]{{40}}$/.test(address)) throw new Error("WalletConnect did not return a valid EVM address");
        const chainHex=await provider.request({{method:"eth_chainId"}});
        const chainId=Number.parseInt(String(chainHex||"0x1"),16)||1;
        const nonceResp=await postJson("/api/auth/nonce",{{address,chain_id:chainId,wallet:"walletconnect"}});
        const messageHex=toHexMessage(nonceResp.message||"");
        const signVariants=[[nonceResp.message,address],[address,nonceResp.message],[messageHex,address],[address,messageHex]];
        let signature="";
        for (const params of signVariants) {{ try {{ if(!params[0]) continue; signature=await provider.request({{method:"personal_sign",params}}); if(signature) break; }} catch (_) {{}} }}
        if(!signature) throw new Error("Failed to sign auth message via WalletConnect");
        const verifyResp=await postJson("/api/auth/verify",{{address,chain_id:chainId,wallet:"walletconnect",message:nonceResp.message,signature}});
        authState={{authenticated:true,...verifyResp}};
        setAuthUI();
        closeWalletModal({{target:{{id:"walletModalBackdrop"}}}});
        location.reload();
      }} catch(e) {{ closeWcQrModal(); alert("WalletConnect failed: "+(e?.message||"unknown error")+". Add this site to Reown Domain allowlist and try again."); }}
    }}
    function setAdminStatus(text, isErr) {{ const el=document.getElementById("adminStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setStatsStatus(text, isErr) {{ const el=document.getElementById("statsStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setFailStatus(text, isErr) {{ const el=document.getElementById("failStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setTicketsStatus(text, isErr) {{ const el=document.getElementById("ticketsStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setFeedbackStatus(text, isErr) {{ const el=document.getElementById("feedbackStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setFaqStatus(text, isErr) {{ const el=document.getElementById("faqStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function normStatus(v) {{ return String(v || "").trim().toLowerCase().replace(/[\\s-]+/g, "_"); }}
    function getTicketsFilter() {{
      const statusEl = document.getElementById("ticketFilterStatus");
      const statusRaw = statusEl ? (statusEl.value || "") : "";
      return {{
        status: normStatus(statusRaw),
        email: (document.getElementById("ticketFilterEmail")?.value || "").trim().toLowerCase(),
        ticketNo: (document.getElementById("ticketFilterNo")?.value || "").trim(),
        text: (document.getElementById("ticketFilterText")?.value || "").trim().toLowerCase(),
      }};
    }}
    function applyTicketsFilter() {{
      const rows = window._ticketRows || [];
      renderTickets(rows);
    }}
    function resetTicketsFilter() {{
      if (document.getElementById("ticketFilterStatus")) document.getElementById("ticketFilterStatus").value = "";
      if (document.getElementById("ticketFilterEmail")) document.getElementById("ticketFilterEmail").value = "";
      if (document.getElementById("ticketFilterNo")) document.getElementById("ticketFilterNo").value = "";
      if (document.getElementById("ticketFilterText")) document.getElementById("ticketFilterText").value = "";
      renderTickets(window._ticketRows || []);
      setTicketsStatus("Filters reset", false);
    }}
    function setupTicketFiltersAutoApply() {{
      const ids = ["ticketFilterStatus", "ticketFilterEmail", "ticketFilterNo", "ticketFilterText"];
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (!el || el.dataset.bound === "1") continue;
        const evt = el.tagName === "SELECT" ? "change" : "input";
        el.addEventListener(evt, () => applyTicketsFilter());
        el.dataset.bound = "1";
      }}
    }}
    function startTicketsAutoRefresh() {{
      if (window._ticketsAutoRefreshStarted) return;
      window._ticketsAutoRefreshStarted = true;
      setInterval(() => {{
        const tab = document.getElementById("tabTickets");
        if (!tab) return;
        if (tab.style.display !== "none") loadTickets();
      }}, 60000);
    }}
    function startFeedbackAutoRefresh() {{
      if (window._feedbackAutoRefreshStarted) return;
      window._feedbackAutoRefreshStarted = true;
      setInterval(() => {{
        const tab = document.getElementById("tabFeedback");
        if (!tab) return;
        if (tab.style.display !== "none") loadFeedback();
      }}, 3600000);
    }}
    function switchTab(tab) {{
      const isSettings = tab === "settings";
      const isStats = tab === "stats";
      const isFailures = tab === "failures";
      const isTickets = tab === "tickets";
      const isFeedback = tab === "feedback";
      const isFaq = tab === "faq";
      document.getElementById("tabSettings").style.display = isSettings ? "grid" : "none";
      document.getElementById("tabStats").style.display = isStats ? "grid" : "none";
      document.getElementById("tabFailures").style.display = isFailures ? "grid" : "none";
      document.getElementById("tabTickets").style.display = isTickets ? "grid" : "none";
      document.getElementById("tabFeedback").style.display = isFeedback ? "grid" : "none";
      document.getElementById("tabFaq").style.display = isFaq ? "grid" : "none";
      document.getElementById("tabBtnSettings").classList.toggle("active", isSettings);
      document.getElementById("tabBtnStats").classList.toggle("active", isStats);
      document.getElementById("tabBtnFailures").classList.toggle("active", isFailures);
      document.getElementById("tabBtnTickets").classList.toggle("active", isTickets);
      document.getElementById("tabBtnFeedback").classList.toggle("active", isFeedback);
      document.getElementById("tabBtnFaq").classList.toggle("active", isFaq);
      if (isStats) loadStats();
      if (isFailures) loadFailures();
      if (isTickets) loadTickets();
      if (isFeedback) loadFeedback();
      if (isFaq) loadFaqAdmin();
    }}
    function renderStats(rows) {{
      const table = document.getElementById("statsTable");
      let html = "<tr><th>Period</th><th>Unique sessions</th><th>Page views</th><th>Run start</th><th>Run done</th><th>Run failed</th><th>Wallet auth</th><th>Help tickets</th><th>Total events</th></tr>";
      for (const r of (rows || [])) {{
        html += "<tr>";
        html += `<td class="mono">${{esc(r.bucket)}}</td>`;
        html += `<td>${{Number(r.unique_sessions || 0)}}</td>`;
        html += `<td>${{Number(r.page_views || 0)}}</td>`;
        html += `<td>${{Number(r.runs_started || 0)}}</td>`;
        html += `<td>${{Number(r.runs_done || 0)}}</td>`;
        html += `<td>${{Number(r.runs_failed || 0)}}</td>`;
        html += `<td>${{Number(r.wallet_auth || 0)}}</td>`;
        html += `<td>${{Number(r.help_tickets || 0)}}</td>`;
        html += `<td>${{Number(r.total_events || 0)}}</td>`;
        html += "</tr>";
      }}
      if (!(rows || []).length) html += '<tr><td colspan="9">No stats yet.</td></tr>';
      table.innerHTML = html;
    }}
    async function loadStats() {{
      try {{
        const period = (document.getElementById("statsPeriod")?.value || "day").trim();
        const limit = Number(document.getElementById("statsLimit")?.value || 30) || 30;
        const r = await fetch(`/api/admin/stats?period=${{encodeURIComponent(period)}}&limit=${{encodeURIComponent(limit)}}`);
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load stats");
        renderStats(data.items || []);
        setStatsStatus(`Loaded ${{(data.items || []).length}} ${{period}} rows`, false);
      }} catch (e) {{
        setStatsStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function renderFailures(rows) {{
      const table = document.getElementById("failuresTable");
      let html = "<tr><th>Time</th><th>Pairs</th><th>Chains</th><th>Versions</th><th>Days</th><th>Min TVL</th><th>Error</th><th>Logs</th><th>Session</th></tr>";
      for (const r of (rows || [])) {{
        html += "<tr>";
        html += `<td class="mono">${{r.ts || ""}}</td>`;
        html += `<td>${{r.pairs || ""}}</td>`;
        html += `<td>${{r.chains || ""}}</td>`;
        html += `<td>${{r.versions || ""}}</td>`;
        html += `<td>${{r.days ?? ""}}</td>`;
        html += `<td>${{r.min_tvl ?? ""}}</td>`;
        html += `<td>${{(r.error || "").replace(/</g, "&lt;")}}</td>`;
        html += `<td><pre style="max-height:140px;margin:0">${{(r.logs || "").replace(/</g, "&lt;")}}</pre></td>`;
        html += `<td class="mono">${{r.session_id || ""}}</td>`;
        html += "</tr>";
      }}
      if (!(rows || []).length) {{
        html += '<tr><td colspan="9">No failed runs yet.</td></tr>';
      }}
      table.innerHTML = html;
    }}
    async function loadFailures() {{
      try {{
        const r = await fetch("/api/admin/failures?limit=50");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load failures");
        renderFailures(data.items || []);
        setFailStatus(`Loaded ${{(data.items || []).length}} rows`, false);
      }} catch (e) {{
        setFailStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function esc(v) {{ return String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }}
    function renderTickets(rows) {{
      const board = document.getElementById("ticketsBoard");
      const f = getTicketsFilter();
      const filtered = (rows || []).filter((r) => {{
        const status = normStatus(r.status || "");
        const email = String(r.email || "").toLowerCase();
        const ticketNo = String(r.ticket_no || r.id || "");
        const threadText = Array.isArray(r.thread) ? r.thread.map((m) => String(m?.message || "")).join("\\n") : "";
        const hay = `${{String(r.subject || "").toLowerCase()}}\\n${{String(r.message || "").toLowerCase()}}\\n${{String(r.admin_note || "").toLowerCase()}}\\n${{threadText.toLowerCase()}}`;
        if (f.status && status !== f.status) return false;
        if (f.email && !email.includes(f.email)) return false;
        if (f.ticketNo && ticketNo !== f.ticketNo) return false;
        if (f.text && !hay.includes(f.text)) return false;
        return true;
      }});
      let html = "";
      for (let idx = 0; idx < filtered.length; idx++) {{
        const r = filtered[idx];
        const statusNorm = normStatus(r.status || "");
        const isClosed = statusNorm === "done" || statusNorm === "closed";
        const subject = esc(r.subject || "");
        const thread = Array.isArray(r.thread) && r.thread.length
          ? r.thread
          : [
              {{author_type: "user", ts: r.ts || "", message: r.message || ""}},
              ...(r.admin_note ? [{{author_type: "admin", ts: "", message: r.admin_note}}] : [])
            ];
        if (isClosed) {{
          html += `<details class="ticket-card"><summary style="cursor:pointer;font-weight:700;color:#334155"><b>#${{esc(r.ticket_no || r.id)}}</b> [closed] ${{subject}} <span class="hint">(${{thread.length}} messages)</span></summary>`;
        }} else {{
          html += `<div class="ticket-card">`;
        }}
        html += `<div class="ticket-author"><div><span class="label">Email:</span><span class="value">${{esc(r.email || "-")}}</span></div><div><span class="label">Wallet:</span><span class="value mono">${{esc(r.wallet_address || "-")}}</span></div><div><span class="label">Ticket #:</span><span class="value">${{esc(r.ticket_no || r.id)}}</span></div><div><span class="label">Created:</span><span class="value">${{esc(r.ts || "-")}}</span></div></div>`;
        const threadHtml = thread.map((m) => {{
          const who = String(m.author_type || "user").toLowerCase() === "admin" ? "admin" : "user";
          const whoLabel = who === "admin" ? "Admin" : (String(r.name || "").trim() || "User");
          const ts = esc(m.ts || "");
          const msg = esc(m.message || "");
          return `<div class="admin-msg-bubble ${{who}}"><div class="admin-msg-head"><span>${{whoLabel}}</span><span>${{ts}}</span></div>${{msg}}</div>`;
        }}).join("\\n\\n");
        if (idx === 0 && !isClosed) {{
          html += `<div class="ticket-message-block"><div class="label">Conversation (${{thread.length}} messages)</div><div class="admin-thread">${{threadHtml}}</div></div>`;
        }} else {{
          html += `<div class="ticket-message-block"><details class="ticket-thread-details"><summary>Conversation (${{thread.length}} messages)</summary><div class="admin-thread">${{threadHtml}}</div></details></div>`;
        }}
        html += `<div class="ticket-reply-block">`;
        html += `<div><textarea class="ticket-reply" id="note_${{r.id}}" placeholder="Reply to user...">${{esc(r.admin_note)}}</textarea></div>`;
        html += `<div class="ticket-actions"><button class="btn btn-soft" style="padding:6px 10px;font-size:12px" onclick="setTicketStatusAction(${{r.id}}, 'in_progress', true)">Send + progress</button><button class="btn" style="padding:6px 10px;font-size:12px" onclick="setTicketStatusAction(${{r.id}}, 'done', true)">Send + close</button><button class="btn btn-soft" style="padding:6px 10px;font-size:12px" onclick="setTicketStatusAction(${{r.id}}, 'done', false)">Close</button><button class="btn btn-danger" style="padding:6px 10px;font-size:12px" onclick="deleteTicketAction(${{r.id}}, '${{esc(r.ticket_no || r.id)}}')">Delete ticket</button></div>`;
        html += `</div>`;
        html += isClosed ? `</details>` : `</div>`;
      }}
      if (!filtered.length) {{
        html = '<div class="hint">No tickets yet.</div>';
      }}
      board.innerHTML = html;
      setTicketsStatus(`Showing ${{filtered.length}} / ${{(rows || []).length}} tickets`, false);
    }}
    async function loadTickets() {{
      try {{
        const r = await fetch("/api/admin/help-tickets?limit=100");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load tickets");
        window._ticketRows = data.items || [];
        renderTickets(window._ticketRows);
      }} catch (e) {{
        setTicketsStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function setTicketStatusAction(ticketId, status, sendReply = true) {{
      try {{
        const note = (document.getElementById(`note_${{ticketId}}`)?.value || "").trim();
        const payload = {{ticket_id: ticketId, status}};
        if (sendReply) payload.admin_note = note;
        const data = await postJson("/api/admin/help-tickets/update", payload);
        const no = data.ticket_no || ticketId;
        const verb = sendReply ? "updated" : "closed";
        setTicketsStatus(`Ticket #${{no}} ${{verb}}`, false);
        if (sendReply) {{
          const area = document.getElementById(`note_${{ticketId}}`);
          if (area) area.value = "";
        }}
        await loadTickets();
      }} catch (e) {{
        setTicketsStatus("Update failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function deleteTicketAction(ticketId, ticketNo) {{
      const no = String(ticketNo || ticketId);
      const ok = confirm(`Delete ticket #${{no}}?\\n\\nThis action is permanent and will remove all messages in this thread.`);
      if (!ok) return;
      try {{
        const data = await postJson("/api/admin/help-tickets/delete", {{ticket_id: Number(ticketId)}});
        setTicketsStatus(data.info || `Ticket #${{no}} deleted`, false);
        await loadTickets();
      }} catch (e) {{
        setTicketsStatus("Delete failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function renderFeedback(rows) {{
      const board = document.getElementById("feedbackBoard");
      const items = rows || [];
      let html = "";
      for (const r of items) {{
        const isReviewed = normStatus(r.status || "") === "reviewed";
        const no = esc(r.feedback_no || r.id);
        const subject = esc(r.subject || "");
        const message = esc(r.message || "");
        const reviewedByTxt = r.reviewed_by ? ` | by: <span class="mono">${{esc(r.reviewed_by)}}</span>` : "";
        const reviewedMeta = isReviewed
          ? `<div class="feedback-meta">Reviewed at: ${{esc(r.reviewed_at || "-")}}${{reviewedByTxt}}</div>`
          : "";
        const author = `<div class="ticket-author"><div><span class="label">Name:</span><span class="value">${{esc(r.name || "-")}}</span></div><div><span class="label">Email:</span><span class="value">${{esc(r.email || "-")}}</span></div><div><span class="label">Wallet:</span><span class="value mono">${{esc(r.wallet_address || "-")}}</span></div><div><span class="label">Created:</span><span class="value">${{esc(r.ts || "-")}}</span></div></div>`;
        const body = `<div class="ticket-message-block"><div class="label">Message</div><pre class="ticket-message">${{message}}</pre>${{reviewedMeta}}</div>`;
        const actions = isReviewed
          ? `<div class="ticket-actions" style="margin-top:8px"><button class="btn btn-danger" style="padding:6px 10px;font-size:12px" onclick="deleteFeedback(${{Number(r.id) || 0}}, '${{no}}')">Delete</button></div>`
          : `<div class="ticket-actions" style="margin-top:8px"><button class="btn" style="padding:6px 10px;font-size:12px" onclick="reviewFeedback(${{Number(r.id) || 0}})">Reviewed</button><button class="btn btn-danger" style="padding:6px 10px;font-size:12px" onclick="deleteFeedback(${{Number(r.id) || 0}}, '${{no}}')">Delete</button></div>`;
        if (isReviewed) {{
          html += `<details class="ticket-card"><summary style="cursor:pointer;font-weight:700;color:#334155"><b>#${{no}}</b> [reviewed] ${{subject}}</summary>${{author}}${{body}}${{actions}}</details>`;
        }} else {{
          html += `<div class="ticket-card"><div style="font-weight:700;color:#334155"><b>#${{no}}</b> [new] ${{subject}}</div>${{author}}${{body}}${{actions}}</div>`;
        }}
      }}
      if (!items.length) html = '<div class="hint">No feedback yet.</div>';
      board.innerHTML = html;
      const pending = items.filter((x) => normStatus(x.status || "") !== "reviewed").length;
      setFeedbackStatus(`Loaded ${{items.length}} messages (${{pending}} new)`, false);
    }}
    async function loadFeedback() {{
      try {{
        const r = await fetch("/api/admin/help-feedback?limit=200");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load feedback");
        window._feedbackRows = data.items || [];
        renderFeedback(window._feedbackRows);
      }} catch (e) {{
        setFeedbackStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function reviewFeedback(feedbackId) {{
      try {{
        const data = await postJson("/api/admin/help-feedback/review", {{feedback_id: Number(feedbackId)}});
        setFeedbackStatus(`Feedback #${{data.feedback_no || feedbackId}} archived`, false);
        await loadFeedback();
      }} catch (e) {{
        setFeedbackStatus("Archive failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function deleteFeedback(feedbackId, feedbackNo) {{
      const no = String(feedbackNo || feedbackId);
      if (!confirm(`Delete feedback #${{no}}?\\n\\nThis action is permanent.`)) return;
      try {{
        const data = await postJson("/api/admin/help-feedback/delete", {{feedback_id: Number(feedbackId)}});
        setFeedbackStatus(data.info || `Feedback #${{no}} deleted`, false);
        await loadFeedback();
      }} catch (e) {{
        setFeedbackStatus("Delete failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function clearFaqForm() {{
      document.getElementById("faqId").value = "";
      document.getElementById("faqQuestion").value = "";
      document.getElementById("faqAnswer").value = "";
      document.getElementById("faqFeatured").value = "false";
      document.getElementById("faqSortOrder").value = 100;
      document.getElementById("faqPublished").value = "true";
    }}
    function renderFaqTable(rows) {{
      const table = document.getElementById("faqTable");
      let html = "<tr><th>ID</th><th>Featured</th><th>Sort</th><th>Published</th><th>Question</th><th>Answer</th><th>Updated</th><th>Actions</th></tr>";
      for (const r of (rows || [])) {{
        html += "<tr>";
        html += `<td class="mono">${{esc(r.id)}}</td>`;
        html += `<td>${{r.is_featured ? "yes" : "no"}}</td>`;
        html += `<td>${{esc(r.sort_order)}}</td>`;
        html += `<td>${{r.is_published ? "yes" : "no"}}</td>`;
        html += `<td>${{esc(r.question)}}</td>`;
        html += `<td><pre style="max-height:120px;margin:0">${{esc(r.answer)}}</pre></td>`;
        html += `<td class="mono">${{esc(r.updated_at)}}</td>`;
        const pubBtn = r.is_published
          ? `<button class="btn" style="padding:5px 10px;font-size:12px" onclick="setFaqPublished(${{r.id}}, false)">Unpublish</button>`
          : `<button class="btn" style="padding:5px 10px;font-size:12px" onclick="setFaqPublished(${{r.id}}, true)">Publish</button>`;
        html += `<td><button class="btn" style="padding:5px 10px;font-size:12px" onclick="editFaq(${{r.id}})">Edit</button> ${{pubBtn}} <button class="btn" style="padding:5px 10px;font-size:12px" onclick="deleteFaq(${{r.id}})">Delete</button></td>`;
        html += "</tr>";
      }}
      if (!(rows || []).length) html += '<tr><td colspan="8">No FAQ items yet.</td></tr>';
      table.innerHTML = html;
      window._faqRows = rows || [];
    }}
    function editFaq(id) {{
      const row = (window._faqRows || []).find((x) => Number(x.id) === Number(id));
      if (!row) return;
      document.getElementById("faqId").value = row.id;
      document.getElementById("faqQuestion").value = row.question || "";
      document.getElementById("faqAnswer").value = row.answer || "";
      document.getElementById("faqFeatured").value = row.is_featured ? "true" : "false";
      document.getElementById("faqSortOrder").value = Number(row.sort_order || 100);
      document.getElementById("faqPublished").value = row.is_published ? "true" : "false";
    }}
    async function loadFaqAdmin() {{
      try {{
        const r = await fetch("/api/admin/faq?limit=200");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load FAQ");
        renderFaqTable(data.items || []);
        setFaqStatus(`Loaded ${{(data.items || []).length}} FAQ items`, false);
      }} catch (e) {{
        setFaqStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function saveFaq() {{
      try {{
        const payload = {{
          faq_id: Number(document.getElementById("faqId").value || 0) || null,
          question: (document.getElementById("faqQuestion").value || "").trim(),
          answer: (document.getElementById("faqAnswer").value || "").trim(),
          is_featured: document.getElementById("faqFeatured").value === "true",
          sort_order: Number(document.getElementById("faqSortOrder").value || 100),
          is_published: document.getElementById("faqPublished").value === "true",
        }};
        const data = await postJson("/api/admin/faq/upsert", payload);
        setFaqStatus(data.info || "Saved", false);
        clearFaqForm();
        await loadFaqAdmin();
      }} catch (e) {{
        setFaqStatus("Save failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function deleteFaq(id) {{
      if (!confirm(`Delete FAQ #${{id}}?`)) return;
      try {{
        const data = await postJson("/api/admin/faq/delete", {{faq_id: Number(id)}});
        setFaqStatus(data.info || "Deleted", false);
        clearFaqForm();
        await loadFaqAdmin();
      }} catch (e) {{
        setFaqStatus("Delete failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function setFaqPublished(id, isPublished) {{
      try {{
        const data = await postJson("/api/admin/faq/publish", {{faq_id: Number(id), is_published: !!isPublished}});
        setFaqStatus(data.info || "Status updated", false);
        await loadFaqAdmin();
      }} catch (e) {{
        setFaqStatus("Publish update failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function loadAdmin() {{
      try {{
        const r = await fetch("/api/admin/settings");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load admin settings");
        document.getElementById("dbPath").textContent = data.analytics_db_path || "-";
        document.getElementById("eventsCount").textContent = String(data.events_count || 0);
        document.getElementById("tokenCatalogInfo").textContent = `updated: ${{data.token_catalog_updated_at || "-"}}, count: ${{data.token_catalog_count || 0}}`;
        renderAdminWallets(data.admin_wallets || []);
      }} catch (e) {{
        setAdminStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function renderAdminWallets(items) {{
      const wrap = document.getElementById("adminWalletsList");
      const rows = (items || []).map((a) => `<div style="display:flex;gap:8px;align-items:center;margin-bottom:6px"><span class="mono">${{a}}</span><button class="btn" style="padding:5px 10px;font-size:12px" onclick="removeAdminWallet('${{a}}')">Remove</button></div>`);
      wrap.innerHTML = rows.length ? rows.join("") : "-";
    }}
    async function addAdminWallet() {{
      const address = (document.getElementById("newAdminWallet").value || "").trim();
      if (!address) {{
        setAdminStatus("Enter wallet address before adding admin.", true);
        return;
      }}
      try {{
        const data = await postJson("/api/admin/admin-wallets", {{action: "add", address}});
        setAdminStatus(data.info || "Added", false);
        document.getElementById("newAdminWallet").value = "";
        await loadAdmin();
      }} catch (e) {{
        setAdminStatus("Add admin failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function removeAdminWallet(address) {{
      if (!confirm(`Remove admin wallet ${{address}}?`)) return;
      try {{
        const data = await postJson("/api/admin/admin-wallets", {{action: "remove", address}});
        setAdminStatus(data.info || "Removed", false);
        await loadAdmin();
      }} catch (e) {{
        setAdminStatus("Remove admin failed: " + (e?.message || "unknown"), true);
      }}
    }}
    loadAuthState();
    loadAdmin();
    setupTicketFiltersAutoApply();
    startTicketsAutoRefresh();
    startFeedbackAutoRefresh();
    refreshIntentMenu();
  </script>
</body>
</html>
"""


def _render_help_page() -> str:
    options_html = _intent_options_html("/help")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Uni Fee - Help</title>
  <style>
    * {{ box-sizing: border-box; }}
    html {{ overflow-y: scroll; scrollbar-gutter: stable; overflow-x: hidden; }}
    body {{ margin: 0; font-family: Inter, Arial, sans-serif; background: linear-gradient(180deg, #d9e3f5 0%, #ecf2ff 100%); color: #0f172a; overflow-x: hidden; }}
    body {{ min-height: 100vh; }}
    .container {{ max-width: 1200px; margin: 0 auto; padding: 18px; min-height: calc(100vh - 36px); }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 14px; }}
    .title {{ margin: 0; font-size: 30px; font-weight: 800; letter-spacing: 0.2px; }}
    .subtitle {{ margin: 4px 0 0; color: #64748b; font-size: 14px; }}
    .top-controls {{ display: flex; gap: 10px; align-items: center; justify-content: flex-end; flex-wrap: nowrap; }}
    .intent-prefix {{ font-size: 14px; font-weight: 700; color: #1d4ed8; white-space: nowrap; }}
    .intent-select {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 38px 10px 12px; font-size: 14px; font-weight: 600; color: #1f3a8a; background: linear-gradient(180deg, #f8fbff 0%, #eff6ff 100%); min-width: 320px; max-width: 360px; appearance: none; -webkit-appearance: none; background-image: linear-gradient(45deg, transparent 50%, #1d4ed8 50%), linear-gradient(135deg, #1d4ed8 50%, transparent 50%); background-position: calc(100% - 18px) calc(50% + 1px), calc(100% - 12px) calc(50% + 1px); background-size: 6px 6px, 6px 6px; background-repeat: no-repeat; box-shadow: inset 0 1px 0 rgba(255,255,255,0.7); }}
    .intent-select option {{ background: #eef4ff; color: #1f3a8a; }}
    .connect-btn {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 16px; font-size: 14px; font-weight: 700; color: #1d4ed8; background: #eff6ff; cursor: pointer; white-space: nowrap; width: 190px; box-sizing: border-box; overflow: hidden; text-overflow: ellipsis; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; }}
    .card {{ background: #f3f7ff; border: 1px solid #cfdcec; border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06); }}
    .card h3 {{ margin: 0 0 10px; font-size: 18px; }}
    .hint {{ color: #64748b; font-size: 13px; margin: 0 0 10px; }}
    .row {{ display: grid; grid-template-columns: 170px 1fr; gap: 10px; align-items: center; margin-bottom: 8px; }}
    input, textarea {{ width: 100%; background: #f8fbff; border: 1px solid #cbd5e1; color: #0f172a; border-radius: 8px; padding: 8px; font-size: 14px; }}
    textarea {{ min-height: 140px; resize: vertical; }}
    .compose-row {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:start; }}
    .compose-row .btn {{ min-width: 140px; }}
    .auth-note {{ margin-top: 6px; font-size: 12px; color: #64748b; }}
    .auth-note-accent {{ font-weight: 700; }}
    .auth-note-accent.disconnected {{ color: #b91c1c; }}
    .auth-note-accent.connected {{ color: #15803d; }}
    .ticket-meta-wrap {{ max-width: 760px; }}
    .btn {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 9px 14px; font-size: 14px; font-weight: 700; color: #1d4ed8; background: #eff6ff; cursor: pointer; }}
    .status {{ font-size: 13px; color: #475569; margin-left: 8px; }}
    .thread {{ display: grid; gap: 8px; }}
    .msg-bubble {{ border: 1px solid #dbe3ef; border-radius: 10px; padding: 8px 10px; background: #f8fbff; white-space: pre-wrap; font-size: 13px; color: #334155; }}
    .msg-bubble.user {{ background: #eef4ff; border-color: #c7dbff; }}
    .msg-bubble.admin {{ background: #f0fdf4; border-color: #bbf7d0; }}
    .msg-head {{ font-size: 11px; color: #64748b; margin-bottom: 4px; display: flex; justify-content: space-between; gap: 8px; }}
    .ticket-last-preview {{ margin-top: 6px; font-size: 12px; color: #334155; padding: 6px 8px; border: 1px dashed #cbd5e1; border-radius: 8px; background: #f8fbff; }}
    .ticket-last-preview .who {{ display: inline-block; padding: 1px 6px; border-radius: 999px; border: 1px solid #bfdbfe; background: #eff6ff; color: #1d4ed8; margin-right: 6px; font-size: 11px; }}
    .ticket-last-preview .who.admin {{ border-color: #bbf7d0; background: #f0fdf4; color: #15803d; }}
    .reply-compose {{ margin-top: 8px; display: grid; grid-template-columns: 1fr auto; gap: 8px; align-items: start; }}
    .reply-compose textarea {{ min-height: 90px; resize: vertical; }}
    .reply-actions {{ display: flex; flex-direction: column; gap: 8px; min-width: 150px; }}
    .reply-actions .btn {{ width: 100%; text-align: center; }}
    .ticket-reply-warning {{ margin-top: 6px; color: #64748b; font-size: 11px; }}
    .feedback-form-card {{ border: 1px dashed #bfdbfe; background: #eef4ff; }}
    .wallet-modal-backdrop {{ position: fixed; inset: 0; background: linear-gradient(180deg, rgba(217,227,245,0.82) 0%, rgba(236,242,255,0.82) 100%); backdrop-filter: blur(3px); display: none; align-items: center; justify-content: center; z-index: 9999; }}
    .wallet-modal {{ width: min(460px, calc(100vw - 24px)); background: #f8fbff; border: 1px solid #cbd5e1; border-radius: 14px; box-shadow: 0 12px 36px rgba(15,23,42,0.25); padding: 14px; }}
    .wallet-modal h3 {{ margin: 0 0 8px; font-size: 18px; }}
    .wallet-list {{ display: grid; grid-template-columns: 1fr; gap: 8px; margin-top: 10px; }}
    .wallet-item {{ border: 1px solid #bfdbfe; border-radius: 10px; padding: 10px 12px; background: #eff6ff; color: #1d4ed8; font-weight: 700; text-align: left; cursor: pointer; }}
    .wallet-item.disabled {{ opacity: 0.6; cursor: not-allowed; }}
    .wallet-note {{ color: #64748b; font-size: 12px; margin-top: 8px; }}
    @media (max-width: 980px) {{ .row {{ grid-template-columns: 1fr; }} .compose-row {{ grid-template-columns: 1fr; }} .compose-row .btn {{ width: 100%; }} }}
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">DeFi Pools</h1>
        <p class="subtitle">Send wishes or report issues</p>
      </div>
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">{options_html}</select>
        <button class="connect-btn" id="connectWalletBtn" onclick="onConnectWalletClick()">Connect Wallet</button>
      </div>
    </div>
    <div class="grid">
      <section class="card">
        <h3>FAQ</h3>
        <p class="hint">Published answers.</p>
        <div id="faqList">Loading FAQ...</div>
      </section>
      <section class="card feedback-form-card">
        <h3>Feedback</h3>
        <p class="hint">Share product ideas or report problems. These messages go to admin review.</p>
        <div class="compose-row"><textarea id="fMessage" placeholder="Tell us what to improve or what is broken."></textarea><button class="btn" onclick="sendFeedback()">Send message</button></div>
        <div class="auth-note" id="feedbackAuthNote">Only wallet-authorized sessions can send tickets and feedback.</div>
        <span class="status" id="feedbackFormStatus"></span>
      </section>
      <section class="card">
        <h3>Send a ticket</h3>
        <p class="hint">Your ticket goes to the admin panel.</p>
        <div class="ticket-meta-wrap">
          <div class="row"><label>Name</label><input id="tName" type="text" placeholder="Optional"/></div>
          <div class="row"><label>Email</label><input id="tEmail" type="email" placeholder="Optional"/></div>
          <div class="row"><label>Subject</label><input id="tSubject" type="text" placeholder="Required"/></div>
        </div>
        <div class="compose-row"><textarea id="tMessage" placeholder="Describe the issue or request."></textarea><button class="btn" onclick="sendTicket()">Send ticket</button></div>
        <div class="auth-note" id="ticketAuthNote">Only wallet-authorized sessions can send tickets and feedback.</div>
        <span class="status" id="ticketStatus"></span>
      </section>
      <section class="card">
        <h3>My tickets</h3>
        <p class="hint">Track status and admin replies.</p>
        <div id="myTickets">Loading tickets...</div>
      </section>
    </div>
  </div>
  <div id="walletModalBackdrop" class="wallet-modal-backdrop" onclick="closeWalletModal(event)">
    <div class="wallet-modal">
      <h3>Connect wallet</h3>
      <div class="wallet-list" id="walletList"></div>
      <div class="wallet-note">Rabby and Phantom are supported. Sign-in uses a gasless message signature.</div>
    </div>
  </div>
  <script>
    let authState = {{authenticated: false}};
    const WALLETCONNECT_PROJECT_ID = "__WALLETCONNECT_PROJECT_ID__";
    const WALLET_LABELS = {{ injected: "Browser Wallet", walletconnect: "WalletConnect (QR)", rabby: "Rabby", metamask: "MetaMask", phantom: "Phantom", coinbase: "Coinbase Wallet" }};
    function navigateIntent(path) {{ if (!path) return; window.location.href = path; }}
    function refreshIntentMenu() {{ const sel=document.getElementById("intentSelect"); if(!sel) return; sel.style.position="absolute"; sel.style.left="-9999px"; sel.style.opacity="0"; sel.style.pointerEvents="none"; let wrap=document.getElementById("intentMenuWrap"); if(!wrap) {{ wrap=document.createElement("div"); wrap.id="intentMenuWrap"; wrap.style.cssText="position:relative;min-width:320px;max-width:360px;"; const btn=document.createElement("button"); btn.type="button"; btn.id="intentMenuBtn"; btn.style.cssText="width:100%;border:1px solid #bfdbfe;border-radius:10px;padding:10px 38px 10px 12px;font-size:14px;font-weight:600;color:#1f3a8a;background:linear-gradient(180deg,#f8fbff 0%,#eff6ff 100%);text-align:left;cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,0.7);"; const list=document.createElement("div"); list.id="intentMenuList"; list.style.cssText="display:none;position:absolute;z-index:12000;left:0;right:0;top:calc(100% + 6px);background:#eef4ff;border:1px solid #bfdbfe;border-radius:10px;box-shadow:0 10px 24px rgba(15,23,42,0.15);padding:6px;max-height:320px;overflow:auto;"; wrap.appendChild(btn); wrap.appendChild(list); sel.insertAdjacentElement("afterend",wrap); btn.onclick=()=>{{ list.style.display=list.style.display==="block"?"none":"block"; }}; document.addEventListener("click",(e)=>{{ if(!wrap.contains(e.target)) list.style.display="none"; }}); }} const btn=document.getElementById("intentMenuBtn"); const list=document.getElementById("intentMenuList"); const options=Array.from(sel.options||[]); const selected=options.find((o)=>o.selected)||options[0]; btn.textContent=selected?selected.textContent:"Select"; list.innerHTML=options.map((o)=>{{ const active=o.value===sel.value; const style=active?"display:block;width:100%;padding:9px 10px;border:none;background:#dbeafe;color:#1e3a8a;font-weight:700;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;":"display:block;width:100%;padding:9px 10px;border:none;background:#eef4ff;color:#1f3a8a;font-weight:600;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;"; return `<button type="button" data-v="${{o.value}}" style="${{style}}">${{o.textContent}}</button>`; }}).join(""); Array.from(list.querySelectorAll("button[data-v]")).forEach((b)=>{{ b.onclick=()=>{{ const v=b.getAttribute("data-v")||""; sel.value=v; list.style.display="none"; navigateIntent(v); }}; }}); }}
    function getEthereumProviders() {{ const out=[]; const eth=window.ethereum; if(!eth) return out; if(Array.isArray(eth.providers)&&eth.providers.length) return eth.providers; out.push(eth); return out; }}
    function getWalletProvider(wallet) {{ const providers=getEthereumProviders(); const pick=(pred)=>providers.find(pred)||null; if(wallet==="injected") return pick((p)=>!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||pick((p)=>!!p?.isMetaMask&&!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||pick((p)=>!!p?.isCoinbaseWallet)||providers[0]||window.ethereum||null; if(wallet==="rabby") return pick((p)=>!!p?.isRabby)||(window.ethereum?.isRabby?window.ethereum:null); if(wallet==="phantom"){{ if(window.phantom?.ethereum?.request) return window.phantom.ethereum; return pick((p)=>!!p?.isPhantom)||(window.ethereum?.isPhantom?window.ethereum:null); }} if(wallet==="metamask") return pick((p)=>!!p?.isMetaMask&&!p?.isRabby&&!p?.isPhantom&&!p?.isCoinbaseWallet)||((window.ethereum?.isMetaMask&&!window.ethereum?.isRabby&&!window.ethereum?.isPhantom&&!window.ethereum?.isCoinbaseWallet)?window.ethereum:null); if(wallet==="coinbase") return pick((p)=>!!p?.isCoinbaseWallet)||(window.ethereum?.isCoinbaseWallet?window.ethereum:null); return null; }}
    function getWalletChoices() {{ const order=["walletconnect","rabby","phantom","metamask","coinbase","injected"]; return order.map((id)=>{{ const isWc=id==="walletconnect"; const available=isWc?true:!!getWalletProvider(id); let label=WALLET_LABELS[id]; if(isWc&&!WALLETCONNECT_PROJECT_ID) label+=" (setup required)"; else if(!available) label+=" (not detected)"; return {{id,label,available}}; }}); }}
    function openWalletModal() {{ const list=document.getElementById("walletList"); const choices=getWalletChoices(); list.innerHTML=choices.map((w)=>{{ const cls=w.available?"wallet-item":"wallet-item disabled"; const dis=w.available?"":"disabled"; return `<button class="${{cls}}" ${{dis}} onclick="connectWalletFlow('${{w.id}}')">${{w.label}}</button>`; }}).join(""); document.getElementById("walletModalBackdrop").style.display="flex"; }}
    function closeWalletModal(event) {{ if(event&&event.target&&event.target.id!=="walletModalBackdrop") return; document.getElementById("walletModalBackdrop").style.display="none"; }}
    async function postJson(url,payload) {{ const r=await fetch(url,{{method:"POST",headers:{{"Content-Type":"application/json"}},body:JSON.stringify(payload||{{}})}}); const data=await r.json().catch(()=>({{}})); if(!r.ok) throw new Error(data.detail||data.info||"Request failed"); return data; }}
    function syncAdminIntentOption() {{ const sel=document.getElementById("intentSelect"); if(!sel) return; const existing=Array.from(sel.options).find((o)=>o.value==="/admin"); const isAdmin=!!authState?.authenticated&&!!authState?.is_admin; if(isAdmin&&!existing) {{ const opt=document.createElement("option"); opt.value="/admin"; opt.textContent="Administer project"; sel.appendChild(opt); }} else if(!isAdmin&&existing) {{ existing.remove(); }} refreshIntentMenu(); }}
    function updateHelpAuthNotes() {{
      const connected = !!authState?.authenticated;
      const html = connected
        ? 'Only <span class="auth-note-accent connected">wallet-authorized sessions</span> can send tickets and feedback.'
        : 'Only <span class="auth-note-accent disconnected">wallet-authorized sessions</span> can send tickets and feedback.';
      const ids = ["feedbackAuthNote", "ticketAuthNote"];
      for (const id of ids) {{
        const el = document.getElementById(id);
        if (el) el.innerHTML = html;
      }}
    }}
    function setAuthUI() {{ const btn=document.getElementById("connectWalletBtn"); if(!btn) return; btn.textContent = authState?.authenticated ? (authState.address_short || "Wallet connected") : "Connect Wallet"; syncAdminIntentOption(); updateHelpAuthNotes(); }}
    async function loadAuthState() {{ try {{ const r=await fetch("/api/auth/me"); authState=await r.json(); }} catch(_) {{ authState={{authenticated:false}}; }} setAuthUI(); }}
    async function onConnectWalletClick() {{ if(authState?.authenticated) {{ if(!confirm("Disconnect wallet?")) return; try {{ await postJson("/api/auth/logout",{{}}); authState={{authenticated:false}}; setAuthUI(); }} catch(e) {{ console.warn("disconnect failed",e); }} return; }} openWalletModal(); }}
    async function connectWalletFlow(wallet) {{ if(wallet==="walletconnect") return connectWalletConnect(); const provider=getWalletProvider(wallet); if(!provider) return; try {{ const accounts=await provider.request({{method:"eth_requestAccounts"}}); const address=String((accounts||[])[0]||"").trim(); if(!address) throw new Error("Wallet did not return an address"); const chainHex=await provider.request({{method:"eth_chainId"}}); const chainId=Number.parseInt(String(chainHex||"0x1"),16)||1; const nonceResp=await postJson("/api/auth/nonce",{{address,chain_id:chainId,wallet}}); const signature=await provider.request({{method:"personal_sign",params:[nonceResp.message,address]}}); const verifyResp=await postJson("/api/auth/verify",{{address,chain_id:chainId,wallet,message:nonceResp.message,signature}}); authState={{authenticated:true,...verifyResp}}; setAuthUI(); closeWalletModal({{target:{{id:"walletModalBackdrop"}}}}); }} catch(e) {{ console.warn("wallet auth failed",e); }} }}
    function showWcQrModal(uri){{ let el=document.getElementById("wcQrBackdrop"); if(!el){{ el=document.createElement("div"); el.id="wcQrBackdrop"; el.style.cssText="position:fixed;inset:0;background:linear-gradient(180deg,rgba(217,227,245,0.95),rgba(236,242,255,0.95));backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center;z-index:10001;"; el.innerHTML='<div style="background:#f8fbff;border:1px solid #cbd5e1;border-radius:14px;padding:20px;text-align:center"><p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#0f172a">Scan with your wallet app</p><img id="wcQrImg" alt="QR" style="display:block;background:#fff;padding:10px;border-radius:10px;width:260px;height:260px"/><button id="wcQrCancel" type="button" style="margin-top:14px;padding:8px 16px;border-radius:10px;border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;font-weight:700;cursor:pointer">Cancel</button></div>'; document.body.appendChild(el); document.getElementById("wcQrCancel").onclick=closeWcQrModal; }} document.getElementById("wcQrImg").src="https://api.qrserver.com/v1/create-qr-code/?size=260x260&data="+encodeURIComponent(uri); el.style.display="flex"; }}
    function closeWcQrModal(){{ const el=document.getElementById("wcQrBackdrop"); if(el) el.style.display="none"; if(window._wcProvider) try {{ window._wcProvider.disconnect(); }} catch(_) {{}} window._wcProvider=null; }}
    async function connectWalletConnect() {{
      if(!WALLETCONNECT_PROJECT_ID) return alert("WalletConnect is not configured (WALLETCONNECT_PROJECT_ID).");
      const normalizeAddress=(value)=>{{ const raw=String(value||"").trim(); if(!raw) return ""; const parts=raw.split(":"); return String(parts[parts.length-1]||"").trim(); }};
      const toHexMessage=(msg)=>{{ try {{ return "0x"+Array.from(new TextEncoder().encode(String(msg||""))).map((b)=>b.toString(16).padStart(2,"0")).join(""); }} catch(_) {{ return ""; }} }};
      try {{
        const EthereumProviderModule=await import("https://esm.sh/@walletconnect/ethereum-provider@2.23.8");
        const wcChains=[1,10,56,137,8453,42161,43114];
        const wcMetadata={{name:"DeFi Pools",description:"DeFi Pools wallet sign-in",url:window.location.origin,icons:[window.location.origin+"/favicon.ico"]}};
        const provider=await EthereumProviderModule.EthereumProvider.init({{projectId:WALLETCONNECT_PROJECT_ID,optionalChains:wcChains,showQrModal:false,optionalMethods:["eth_requestAccounts","eth_accounts","eth_chainId","personal_sign","wallet_switchEthereumChain"],optionalEvents:["accountsChanged","chainChanged","disconnect"],metadata:wcMetadata,rpcMap:{{}}}});
        provider.on("display_uri",showWcQrModal);
        window._wcProvider=provider;
        let connected=false; try {{ await provider.connect(); connected=true; }} catch(_) {{}}
        if(!connected) {{ try {{ await provider.enable(); }} catch(connErr) {{ closeWcQrModal(); throw connErr; }} }}
        window._wcProvider=null;
        closeWcQrModal();
        let accounts=provider.accounts||[];
        if(!accounts.length) accounts=(await provider.request({{method:"eth_accounts"}}))||[];
        if(!accounts.length) accounts=(await provider.request({{method:"eth_requestAccounts"}}))||[];
        const address=normalizeAddress(accounts[0]||"");
        if(!/^0x[a-fA-F0-9]{{40}}$/.test(address)) throw new Error("WalletConnect did not return a valid EVM address");
        const chainHex=await provider.request({{method:"eth_chainId"}});
        const chainId=Number.parseInt(String(chainHex||"0x1"),16)||1;
        const nonceResp=await postJson("/api/auth/nonce",{{address,chain_id:chainId,wallet:"walletconnect"}});
        const messageHex=toHexMessage(nonceResp.message||"");
        const signVariants=[[nonceResp.message,address],[address,nonceResp.message],[messageHex,address],[address,messageHex]];
        let signature="";
        for (const params of signVariants) {{ try {{ if(!params[0]) continue; signature=await provider.request({{method:"personal_sign",params}}); if(signature) break; }} catch (_) {{}} }}
        if(!signature) throw new Error("Failed to sign auth message via WalletConnect");
        const verifyResp=await postJson("/api/auth/verify",{{address,chain_id:chainId,wallet:"walletconnect",message:nonceResp.message,signature}});
        authState={{authenticated:true,...verifyResp}};
        setAuthUI();
        closeWalletModal({{target:{{id:"walletModalBackdrop"}}}});
      }} catch(e) {{ closeWcQrModal(); alert("WalletConnect failed: "+(e?.message||"unknown error")+". Add this site to Reown Domain allowlist and try again."); }}
    }}
    function setTicketStatus(text, isErr) {{ const el=document.getElementById("ticketStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function setFeedbackFormStatus(text, isErr) {{ const el=document.getElementById("feedbackFormStatus"); el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    async function sendFeedback() {{
      try {{
        if (!authState?.authenticated) throw new Error("Connect wallet first.");
        const payload = {{
          message: (document.getElementById("fMessage").value || "").trim(),
        }};
        const data = await postJson("/api/help/feedback", payload);
        const no = data.feedback_no || data.feedback_id;
        setFeedbackFormStatus(`Message #${{no}} sent`, false);
        document.getElementById("fMessage").value = "";
      }} catch (e) {{
        setFeedbackFormStatus("Send failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function sendTicket() {{
      try {{
        if (!authState?.authenticated) throw new Error("Connect wallet first.");
        const payload = {{
          name: (document.getElementById("tName").value || "").trim(),
          email: (document.getElementById("tEmail").value || "").trim(),
          subject: (document.getElementById("tSubject").value || "").trim(),
          message: (document.getElementById("tMessage").value || "").trim(),
        }};
        const data = await postJson("/api/help/tickets", payload);
        const no = data.ticket_no || data.ticket_id;
        setTicketStatus(`Ticket #${{no}} sent`, false);
        document.getElementById("tSubject").value = "";
        document.getElementById("tMessage").value = "";
        await loadMyTickets();
      }} catch (e) {{
        setTicketStatus("Send failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function closeMyTicket(ticketId) {{
      try {{
        if (!authState?.authenticated) throw new Error("Connect wallet first.");
        await postJson("/api/help/tickets/close", {{ ticket_id: Number(ticketId) }});
        await loadMyTickets();
      }} catch (e) {{
        setTicketStatus("Close failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function renderFaqList(items) {{
      const wrap = document.getElementById("faqList");
      if (!(items || []).length) {{
        wrap.innerHTML = '<p class="hint">No FAQ entries yet.</p>';
        return;
      }}
      wrap.innerHTML = (items || []).map((f) => {{
        const q = String(f.question || "").replace(/</g, "&lt;");
        const a = String(f.answer || "").replace(/</g, "&lt;");
        const badge = f.is_featured ? ' <span style="font-size:11px;color:#1d4ed8;border:1px solid #bfdbfe;border-radius:999px;padding:2px 6px;background:#eff6ff">featured</span>' : '';
        return `<details style="margin-bottom:8px"><summary><b>${{q}}</b>${{badge}}</summary><div style="margin-top:6px;white-space:pre-wrap">${{a}}</div></details>`;
      }}).join("");
    }}
    async function loadFaq() {{
      try {{
        const r = await fetch("/api/faq?limit=200");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load FAQ");
        renderFaqList(data.items || []);
      }} catch (_) {{
        document.getElementById("faqList").innerHTML = '<p class="hint">FAQ failed to load.</p>';
      }}
    }}
    function escHtml(v) {{ return String(v == null ? "" : v).replace(/&/g, "&amp;").replace(/</g, "&lt;"); }}
    function renderMyTickets(items) {{
      const wrap = document.getElementById("myTickets");
      if (!(items || []).length) {{
        wrap.innerHTML = '<p class="hint">No tickets yet.</p>';
        return;
      }}
      wrap.innerHTML = (items || []).map((t, idx) => {{
        const subject = escHtml(t.subject || "");
        const rawStatus = String(t.status || "open").toLowerCase();
        const isClosed = rawStatus === "done" || rawStatus === "closed";
        const status = escHtml(isClosed ? "closed" : rawStatus);
        const thread = Array.isArray(t.thread) && t.thread.length
          ? t.thread
          : [
              {{author_type: "user", ts: t.ts || "", message: t.message || ""}},
              ...(t.admin_reply ? [{{author_type: "admin", ts: "", message: t.admin_reply}}] : [])
            ];
        const threadHtml = thread.map((m) => {{
          const who = String(m.author_type || "user").toLowerCase() === "admin" ? "admin" : "user";
          const whoLabel = who === "admin" ? "Admin" : "You";
          const ts = escHtml(m.ts || "");
          const msg = escHtml(m.message || "");
          return `<div class="msg-bubble ${{who}}"><div class="msg-head"><span>${{whoLabel}}</span><span>${{ts}}</span></div>${{msg}}</div>`;
        }}).join("");
        const msgCount = thread.length;
        const lastMsgObj = thread.length ? thread[thread.length - 1] : null;
        const lastWhoRaw = String(lastMsgObj?.author_type || "user").toLowerCase();
        const lastWhoLabel = lastWhoRaw === "admin" ? "Admin" : "You";
        const lastBody = String(lastMsgObj?.message || "").replace(/\\s+/g, " ").trim();
        const lastShort = escHtml(lastBody.length > 90 ? (lastBody.slice(0, 90) + "...") : (lastBody || ""));
        const summaryTail = lastShort ? ` - ${{lastWhoLabel}}: ${{lastShort}}` : "";
        const warning = isClosed
          ? ""
          : '<div class="ticket-reply-warning">Please reply within three days, otherwise this ticket will be closed automatically.</div>';
        const replyBlock = isClosed
          ? ""
          : `<div class="reply-compose"><textarea id="myReply_${{t.id}}" placeholder="Reply to admin in this ticket thread..."></textarea><div class="reply-actions"><button class="btn" onclick="replyToTicket(${{t.id}})">Send reply</button><button class="btn btn-soft" onclick="closeMyTicket(${{t.id}})">Close</button></div></div>${{warning}}`;
        if (idx === 0 && !isClosed) {{
          return `<div style="margin-bottom:10px;border:1px solid #dbe3ef;border-radius:10px;padding:8px;background:#f8fbff"><div style="font-weight:700"><b>#${{t.ticket_no}}</b> [${{status}}] ${{subject}} <span class="hint">(${{msgCount}} messages)</span><span class="hint">${{summaryTail}}</span></div><div style="margin-top:8px" class="thread">${{threadHtml}}</div>${{replyBlock}}</div>`;
        }}
        return `<details style="margin-bottom:10px"><summary><b>#${{t.ticket_no}}</b> [${{status}}] ${{subject}} <span class="hint">(${{msgCount}} messages)</span><span class="hint">${{summaryTail}}</span></summary><div style="margin-top:8px" class="thread">${{threadHtml}}</div>${{replyBlock}}</details>`;
      }}).join("");
    }}
    async function replyToTicket(ticketId) {{
      try {{
        if (!authState?.authenticated) throw new Error("Connect wallet first.");
        const el = document.getElementById(`myReply_${{ticketId}}`);
        const message = (el?.value || "").trim();
        if (message.length < 2) throw new Error("Reply is too short");
        const data = await postJson("/api/help/tickets/reply", {{ticket_id: Number(ticketId), message}});
        const no = data.ticket_no || ticketId;
        setTicketStatus(`Reply sent for ticket #${{no}}`, false);
        if (el) el.value = "";
        await loadMyTickets();
      }} catch (e) {{
        setTicketStatus("Reply failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function loadMyTickets() {{
      try {{
        const r = await fetch("/api/help/my-tickets?limit=50");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load my tickets");
        renderMyTickets(data.items || []);
      }} catch (_) {{
        document.getElementById("myTickets").innerHTML = '<p class="hint">Tickets failed to load.</p>';
      }}
    }}
    loadAuthState();
    loadFaq();
    loadMyTickets();
    refreshIntentMenu();
    setInterval(() => loadMyTickets(), 30000);
  </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def home(request: Request) -> HTMLResponse:
    html = HTML_PAGE.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/")
    return resp


@app.get("/stables", response_class=HTMLResponse)
def stables_page(request: Request) -> HTMLResponse:
    html = _render_stables_page()
    html = html.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/stables")
    return resp


@app.get("/positions", response_class=HTMLResponse)
def positions_page(request: Request) -> HTMLResponse:
    html = _render_positions_page()
    html = html.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/positions")
    return resp


@app.get("/pancake", response_class=HTMLResponse)
def pancake_page(request: Request) -> HTMLResponse:
    html = _render_placeholder_page("Pancake Pool Finder", "This page is a placeholder for Pancake pool analysis.", "/pancake")
    html = html.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/pancake")
    return resp


@app.get("/help", response_class=HTMLResponse)
def help_page(request: Request) -> HTMLResponse:
    html = _render_help_page().replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/help")
    return resp


@app.get("/connect", response_class=HTMLResponse)
def connect_page(request: Request) -> HTMLResponse:
    html = _render_placeholder_page("Connect", "Connect is a placeholder for wallet/account integration.", "")
    html = html.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
    resp = HTMLResponse(html)
    sid = _ensure_session_cookie(request, resp)
    _analytics_log_event(session_id=sid, event_type="page_view", path="/connect")
    return resp


@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request, response: Response) -> HTMLResponse:
    try:
        html = _render_admin_page().replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
        resp = HTMLResponse(html)
        _require_admin(request, resp)
        sid = _ensure_session_cookie(request, resp)
        _analytics_log_event(session_id=sid, event_type="page_view", path="/admin")
        return resp
    except HTTPException:
        raise
    except Exception as e:
        # Keep page available even if admin template rendering has unexpected issue.
        fallback = _render_placeholder_page("Admin page", f"Temporary error: {e}", "/admin")
        fallback = fallback.replace("__WALLETCONNECT_PROJECT_ID__", _walletconnect_js_value())
        return HTMLResponse(fallback)


@app.get("/api/auth/me")
def auth_me(request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    with AUTH_LOCK:
        auth = dict(AUTH_SESSIONS.get(sid, {}))
    if not auth:
        return {"authenticated": False}
    return {
        "authenticated": True,
        "address": auth.get("address"),
        "address_short": _short_addr(str(auth.get("address") or "")),
        "wallet": auth.get("wallet", "injected"),
        "chain_id": auth.get("chain_id"),
        "authenticated_at": auth.get("authenticated_at"),
        "is_admin": _is_admin_address(str(auth.get("address") or "")),
    }


@app.post("/api/auth/nonce")
def auth_nonce(req: AuthNonceRequest, request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    address = (req.address or "").strip()
    if not _is_eth_address(address):
        raise HTTPException(status_code=400, detail="Invalid wallet address.")
    chain_id = int(req.chain_id or 1)
    if chain_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid chain id.")
    nonce = _new_nonce()
    now_iso = _iso_now()
    base = _public_base_url(request)
    message = _siwe_message(
        domain=request.url.hostname or "uni-fee.local",
        uri=f"{base}/",
        address=address,
        chain_id=chain_id,
        nonce=nonce,
        issued_at=now_iso,
    )
    with AUTH_LOCK:
        AUTH_NONCES[sid] = {
            "nonce": nonce,
            "message": message,
            "address": address.lower(),
            "chain_id": chain_id,
            "wallet": (req.wallet or "injected")[:32],
            "issued_at_ts": time.time(),
        }
    return {"nonce": nonce, "message": message, "issued_at": now_iso}


@app.post("/api/auth/verify")
def auth_verify(req: AuthVerifyRequest, request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    address = (req.address or "").strip().lower()
    signature = (req.signature or "").strip()
    if not _is_eth_address(address):
        raise HTTPException(status_code=400, detail="Invalid wallet address.")
    if not signature:
        raise HTTPException(status_code=400, detail="Signature is required.")
    with AUTH_LOCK:
        pending = dict(AUTH_NONCES.get(sid, {}))
    if not pending:
        raise HTTPException(status_code=400, detail="Nonce is missing or expired.")
    age = time.time() - float(pending.get("issued_at_ts") or 0)
    if age < 0 or age > AUTH_NONCE_TTL_SEC:
        with AUTH_LOCK:
            AUTH_NONCES.pop(sid, None)
        raise HTTPException(status_code=400, detail="Nonce expired. Please retry.")
    if (pending.get("address") or "") != address:
        raise HTTPException(status_code=400, detail="Address mismatch.")
    if int(pending.get("chain_id") or 1) != int(req.chain_id or 1):
        raise HTTPException(status_code=400, detail="Chain mismatch.")
    expected_message = str(pending.get("message") or "")
    if expected_message != (req.message or ""):
        raise HTTPException(status_code=400, detail="Message mismatch.")
    try:
        recovered = Account.recover_message(encode_defunct(text=expected_message), signature=signature).lower()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Signature verification failed: {e}") from e
    if recovered != address:
        raise HTTPException(status_code=400, detail="Invalid signature signer.")
    auth = {
        "address": address,
        "wallet": (req.wallet or pending.get("wallet") or "injected")[:32],
        "chain_id": int(req.chain_id or pending.get("chain_id") or 1),
        "authenticated_at": _iso_now(),
    }
    with AUTH_LOCK:
        AUTH_NONCES.pop(sid, None)
        AUTH_SESSIONS[sid] = auth
    _analytics_log_event(session_id=sid, event_type="wallet_auth", path="/api/auth/verify", payload=address)
    return {
        "ok": True,
        "address": auth["address"],
        "address_short": _short_addr(auth["address"]),
        "wallet": auth["wallet"],
        "chain_id": auth["chain_id"],
        "authenticated_at": auth["authenticated_at"],
        "is_admin": _is_admin_address(auth["address"]),
    }


@app.post("/api/auth/logout")
def auth_logout(request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    with AUTH_LOCK:
        AUTH_SESSIONS.pop(sid, None)
        AUTH_NONCES.pop(sid, None)
    return {"ok": True}


@app.get("/api/admin/settings")
def admin_settings(request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    try:
        token_catalog = _load_token_catalog(refresh=False)
        events_count = 0
        if ANALYTICS_ENABLED:
            with _analytics_conn() as conn:
                events_count = int(conn.execute("SELECT COUNT(*) FROM analytics_events").fetchone()[0])
        return {
            "admin_wallets": _admin_wallets_value(),
            "admin_wallets_encrypted": bool(_admin_wallets_fernet()),
            "analytics_db_path": str(ANALYTICS_DB_PATH),
            "events_count": events_count,
            "token_catalog_updated_at": token_catalog.get("updated_at"),
            "token_catalog_count": token_catalog.get("count", 0),
        }
    except Exception as e:
        return {
            "admin_wallets": _admin_wallets_value(),
            "admin_wallets_encrypted": bool(_admin_wallets_fernet()),
            "analytics_db_path": str(ANALYTICS_DB_PATH),
            "events_count": 0,
            "token_catalog_updated_at": None,
            "token_catalog_count": 0,
            "info": f"Admin settings fallback mode. Error: {e}",
        }


@app.get("/api/admin/stats")
def admin_stats(request: Request, response: Response, period: str = "day", limit: int = 30) -> dict[str, Any]:
    _require_admin(request, response)
    p = (period or "day").strip().lower()
    if p not in {"day", "week", "month", "year"}:
        raise HTTPException(status_code=400, detail="period must be day, week, month, or year.")
    lim = max(1, min(365, int(limit)))
    if not ANALYTICS_ENABLED:
        return {"period": p, "limit": lim, "items": [], "count": 0}

    bucket_expr = _analytics_bucket_expr(p)
    query = f"""
        SELECT
          bucket,
          COUNT(*) AS total_events,
          COUNT(DISTINCT session_id) AS unique_sessions,
          SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
          SUM(CASE WHEN event_type = 'run_start' THEN 1 ELSE 0 END) AS runs_started,
          SUM(CASE WHEN event_type = 'run_done' THEN 1 ELSE 0 END) AS runs_done,
          SUM(CASE WHEN event_type = 'run_failed' THEN 1 ELSE 0 END) AS runs_failed,
          SUM(CASE WHEN event_type = 'wallet_auth' THEN 1 ELSE 0 END) AS wallet_auth,
          SUM(CASE WHEN event_type = 'help_ticket' THEN 1 ELSE 0 END) AS help_tickets
        FROM (
          SELECT {bucket_expr} AS bucket, session_id, event_type
          FROM analytics_events
        ) e
        WHERE bucket IS NOT NULL AND bucket != ''
        GROUP BY bucket
        ORDER BY bucket DESC
        LIMIT ?
    """
    with _analytics_conn() as conn:
        rows = conn.execute(query, (lim,)).fetchall()
    items = []
    for r in reversed(rows):
        items.append(
            {
                "bucket": str(r[0]),
                "total_events": int(r[1] or 0),
                "unique_sessions": int(r[2] or 0),
                "page_views": int(r[3] or 0),
                "runs_started": int(r[4] or 0),
                "runs_done": int(r[5] or 0),
                "runs_failed": int(r[6] or 0),
                "wallet_auth": int(r[7] or 0),
                "help_tickets": int(r[8] or 0),
            }
        )
    return {"period": p, "limit": lim, "items": items, "count": len(items)}


@app.post("/api/admin/admin-wallets")
def admin_wallets_update(req: AdminWalletUpdate, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    action = (req.action or "").strip().lower()
    address = (req.address or "").strip().lower()
    if action not in {"add", "remove"}:
        raise HTTPException(status_code=400, detail="action must be add or remove.")
    if not _is_eth_address(address):
        raise HTTPException(status_code=400, detail="Invalid wallet address.")
    wallets = _admin_wallets_value()
    if action == "add":
        if address not in wallets:
            wallets.append(address)
            _set_admin_wallets(wallets)
        return {"ok": True, "info": "Admin wallet added.", "items": _admin_wallets_value()}

    # remove
    if address not in wallets:
        return {"ok": True, "info": "Wallet already absent.", "items": wallets}
    if len(wallets) <= 1:
        raise HTTPException(status_code=400, detail="Cannot remove the last admin wallet.")
    wallets = [w for w in wallets if w != address]
    _set_admin_wallets(wallets)
    return {"ok": True, "info": "Admin wallet removed.", "items": _admin_wallets_value()}


@app.get("/api/positions/chains")
def positions_chains() -> dict[str, Any]:
    items = _positions_chain_catalog()
    return {"items": items, "count": len(items)}


@app.post("/api/positions/scan")
def scan_positions(req: PositionsScanRequest, request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    evm_raw = list(req.evm_addresses or []) + list(req.addresses or [])
    evm_addresses = _parse_positions_addresses(evm_raw)
    solana_addresses = _parse_solana_addresses(req.solana_addresses or [])
    tron_addresses = _parse_tron_addresses(req.tron_addresses or [])

    if not evm_addresses and not solana_addresses and not tron_addresses:
        raise HTTPException(status_code=400, detail="Provide at least one valid address.")
    if len(evm_addresses) > 20:
        raise HTTPException(status_code=400, detail="Too many addresses. Max 20.")

    # By default scan all supported EVM chains; if chain_ids provided, preserve user order.
    # Prioritize chains where users most often track active LPs, and keep heavy
    # ethereum scan later so partial results include L2 pools under timeout.
    preferred_order = [42161, 8453, 130, 56, 1, 10, 137]
    preferred_rank = {cid: idx for idx, cid in enumerate(preferred_order)}
    all_chain_ids = sorted(
        (
            {
                int(x.get("chain_id") or 0)
                for x in _positions_chain_catalog()
                if int(x.get("chain_id") or 0) > 0
            }
            | {int(cid) for cid in CHAIN_ID_TO_KEY.keys() if int(cid) > 0}
        ),
        key=lambda cid: (preferred_rank.get(int(cid), 999), int(cid)),
    )[:64]
    requested_chain_ids: list[int] = []
    for x in (req.chain_ids or []):
        cid = int(x)
        if cid > 0 and cid not in requested_chain_ids:
            requested_chain_ids.append(cid)
    if requested_chain_ids:
        allowed = set(all_chain_ids)
        selected_chain_ids = [c for c in requested_chain_ids if c in allowed]
        if not selected_chain_ids:
            selected_chain_ids = all_chain_ids
    else:
        selected_chain_ids = all_chain_ids
    scan_pools = bool(req.include_pools)
    scan_lending = bool(req.include_lending)
    scan_rewards = bool(req.include_rewards)
    pool_debug_rows: list[dict[str, Any]] = []
    if evm_addresses:
        if scan_pools:
            pool_rows, pool_errs, pool_debug_rows = _scan_pool_positions(evm_addresses, selected_chain_ids)
        else:
            pool_rows, pool_errs, pool_debug_rows = [], [], []
        lending_rows, lending_errs = (_scan_aave_positions(evm_addresses, selected_chain_ids) if scan_lending else ([], []))
        reward_rows, reward_errs = (_scan_aave_merit_rewards(evm_addresses, selected_chain_ids) if scan_rewards else ([], []))
    else:
        pool_rows, pool_errs = [], []
        lending_rows, lending_errs = [], []
        reward_rows, reward_errs = [], []
        pool_debug_rows = []

    info_notes: list[str] = []
    if solana_addresses:
        info_notes.append("Solana scanning is not available yet in this build.")
    if tron_addresses:
        info_notes.append("TRON scanning is not available yet in this build.")

    pool_rows.sort(
        key=lambda x: (
            str(x.get("address") or ""),
            str(x.get("chain") or ""),
            -_safe_float(x.get("tvl_usd")),
        )
    )
    lending_rows.sort(
        key=lambda x: (
            str(x.get("address") or ""),
            str(x.get("chain") or ""),
            -_safe_float(x.get("usd")),
        )
    )
    reward_rows.sort(
        key=lambda x: (
            str(x.get("address") or ""),
            str(x.get("chain") or ""),
            -_safe_float(x.get("usd")),
        )
    )

    _analytics_log_event(
        session_id=sid,
        event_type="positions_scan",
        path="/api/positions/scan",
        payload=f"evm={len(evm_addresses)} sol={len(solana_addresses)} tron={len(tron_addresses)} chains={len(selected_chain_ids)}",
    )
    debug_summary: dict[tuple[str, str, str, str], int] = {}
    for d in pool_debug_rows:
        chain = str(d.get("chain") or "")
        version = str(d.get("version") or "")
        attempts = d.get("attempts") or []
        for a in attempts:
            if not isinstance(a, dict):
                continue
            mode = f"{a.get('query_mode') or ''}:{a.get('owner_type') or ''}"
            key = (chain, version, mode, "ok" if a.get("ok") else "fail")
            debug_summary[key] = int(debug_summary.get(key, 0)) + int(a.get("count") or 0)
    debug_summary_rows = [
        {
            "chain": chain,
            "version": version,
            "query_mode": mode,
            "status": status,
            "count": count,
        }
        for (chain, version, mode, status), count in sorted(debug_summary.items())
    ]
    return {
        "pool_positions": pool_rows,
        "lending_positions": lending_rows,
        "reward_positions": reward_rows,
        "errors": (pool_errs + lending_errs + reward_errs)[:40],
        "infos": info_notes[:20],
        "debug": {
            "pool_scan": pool_debug_rows[:500],
            "summary": debug_summary_rows[:500],
        },
        "summary": {
            "evm_addresses": len(evm_addresses),
            "solana_addresses": len(solana_addresses),
            "tron_addresses": len(tron_addresses),
            "chains": len(selected_chain_ids),
            "pool_count": len(pool_rows),
            "lending_count": len(lending_rows),
            "reward_count": len(reward_rows),
        },
    }


@app.post("/api/positions/pool-value-series")
def positions_pool_value_series(req: PositionPoolSeriesRequest) -> dict[str, Any]:
    chain_key = str(req.chain or "").strip().lower()
    protocol = str(req.protocol or "").strip().lower()
    pool_id = str(req.pool_id or "").strip().lower()
    if not chain_key or not pool_id:
        raise HTTPException(status_code=400, detail="chain and pool_id are required.")
    if protocol not in {"uniswap_v3", "uniswap_v4", "pancake_v3"}:
        raise HTTPException(status_code=400, detail="protocol must be uniswap_v3, uniswap_v4 or pancake_v3.")
    version = "v4" if protocol.endswith("_v4") else "v3"
    days = max(1, min(3650, int(req.days or 30)))
    now_ts = int(time.time())
    since_ts = now_ts - int(days) * 86400

    chain_id = _chain_id_by_chain_key(chain_key)
    endpoint = get_graph_endpoint(chain_key, version=version)
    position_ids = [str(x).strip() for x in (req.position_ids or []) if str(x).strip()]

    # 1) Try exact snapshots history first (best effort).
    if endpoint and chain_id > 0 and position_ids:
        exact_by_day: dict[int, float] = {}
        for pid in position_ids[:20]:
            series = _fetch_position_snapshot_series_exact(
                endpoint,
                pid,
                since_ts=since_ts,
                chain_id=chain_id,
            )
            for ts, value in series:
                exact_by_day[int(ts)] = float(exact_by_day.get(int(ts), 0.0) + float(value))
        if exact_by_day:
            items = [
                {"ts": int(ts), "position_tvl_usd": float(v), "pool_tvl_usd": None}
                for ts, v in sorted(exact_by_day.items(), key=lambda x: x[0])
            ]
            return {
                "items": items,
                "count": len(items),
                "mode": "exact-snapshots",
                "note": "built from position snapshots",
            }

    # 2) Fallback to estimated share-based history.
    position_liq = _safe_float(req.position_liquidity)
    pool_liq = _safe_float(req.pool_liquidity)
    if position_liq <= 0 or pool_liq <= 0:
        return {
            "items": [],
            "count": 0,
            "mode": "unavailable",
            "note": "snapshots missing and liquidity share unavailable",
        }
    share = position_liq / pool_liq if pool_liq > 0 else 0.0
    if share <= 0:
        raise HTTPException(status_code=400, detail="Liquidity share is zero.")
    series_raw = _fetch_pool_tvl_series(chain_key, version, pool_id, days)
    if not series_raw:
        return {"items": [], "count": 0, "share": share, "mode": "estimated-share", "note": "no pool day data"}
    items = [{"ts": int(ts), "pool_tvl_usd": float(tvl), "position_tvl_usd": float(max(0.0, tvl * share))} for ts, tvl in series_raw]
    return {
        "items": items,
        "count": len(items),
        "share": share,
        "mode": "estimated-share",
        "note": "fallback: snapshots missing/incomplete",
    }


@app.post("/api/help/tickets")
def create_help_ticket(req: HelpTicketCreate, request: Request, response: Response) -> dict[str, Any]:
    sid, wallet, _auth = _require_authenticated_wallet(request, response)
    subject = (req.subject or "").strip()
    message = (req.message or "").strip()
    if len(subject) < 3:
        raise HTTPException(status_code=400, detail="Subject is too short.")
    if len(message) < 10:
        raise HTTPException(status_code=400, detail="Message is too short.")
    if req.email and not _is_valid_email(req.email):
        raise HTTPException(status_code=400, detail="Invalid email format.")
    ticket_id = _create_help_ticket(
        session_id=sid,
        wallet_address=wallet,
        name=req.name,
        email=req.email,
        subject=subject,
        message=message,
    )
    _analytics_log_event(session_id=sid, event_type="help_ticket", path="/api/help/tickets", payload=str(ticket_id))
    ticket_no = _ticket_number(ticket_id)
    return {"ok": True, "ticket_id": ticket_id, "ticket_no": ticket_no}


@app.post("/api/help/feedback")
def create_help_feedback(req: HelpFeedbackCreate, request: Request, response: Response) -> dict[str, Any]:
    sid, wallet, _auth = _require_authenticated_wallet(request, response)
    message = (req.message or "").strip()
    if len(message) < 5:
        raise HTTPException(status_code=400, detail="Message is too short.")
    feedback_id = _create_help_feedback(
        session_id=sid,
        wallet_address=wallet,
        name="",
        email="",
        subject="Feedback / issue report",
        message=message,
    )
    _analytics_log_event(session_id=sid, event_type="help_feedback", path="/api/help/feedback", payload=str(feedback_id))
    return {"ok": True, "feedback_id": feedback_id, "feedback_no": _feedback_number(feedback_id)}


@app.get("/api/help/my-tickets")
def my_help_tickets(request: Request, response: Response, limit: int = 50) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    with AUTH_LOCK:
        auth = dict(AUTH_SESSIONS.get(sid, {}))
    wallet = str(auth.get("address") or "")
    items = _list_help_tickets_for_session(session_id=sid, limit=limit, wallet_address=wallet)
    return {"items": items, "count": len(items)}


@app.post("/api/help/tickets/reply")
def reply_help_ticket(req: HelpTicketReply, request: Request, response: Response) -> dict[str, Any]:
    sid, wallet, _auth = _require_authenticated_wallet(request, response)
    msg = (req.message or "").strip()
    if len(msg) < 2:
        raise HTTPException(status_code=400, detail="Reply is too short.")
    with _analytics_conn() as conn:
        row = conn.execute(
            "SELECT id, session_id, wallet_address FROM help_tickets WHERE id = ?",
            (int(req.ticket_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        ticket_id = int(row[0])
        owner_sid = str(row[1] or "")
        owner_wallet = str(row[2] or "").strip().lower()
    can_reply = (sid == owner_sid) or (_is_eth_address(wallet) and wallet == owner_wallet)
    if not can_reply:
        raise HTTPException(status_code=403, detail="You can reply only to your own ticket.")

    _append_help_ticket_message(
        ticket_id=ticket_id,
        author_type="user",
        message=msg,
        session_id=sid,
        wallet_address=wallet,
    )
    with _analytics_conn() as conn:
        conn.execute("UPDATE help_tickets SET status = ? WHERE id = ?", ("open", ticket_id))
        conn.commit()
    _analytics_log_event(session_id=sid, event_type="help_ticket", path="/api/help/tickets/reply", payload=str(ticket_id))
    ticket_no = _ticket_number(ticket_id)
    return {"ok": True, "ticket_no": ticket_no}


@app.post("/api/help/tickets/close")
def close_help_ticket(req: HelpTicketClose, request: Request, response: Response) -> dict[str, Any]:
    sid, wallet, _auth = _require_authenticated_wallet(request, response)
    with _analytics_conn() as conn:
        row = conn.execute(
            "SELECT id, session_id, wallet_address FROM help_tickets WHERE id = ?",
            (int(req.ticket_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        ticket_id = int(row[0])
        owner_sid = str(row[1] or "")
        owner_wallet = str(row[2] or "").strip().lower()
        can_close = (sid == owner_sid) or (_is_eth_address(wallet) and wallet == owner_wallet)
        if not can_close:
            raise HTTPException(status_code=403, detail="You can close only your own ticket.")
        conn.execute("UPDATE help_tickets SET status = ? WHERE id = ?", ("done", ticket_id))
        conn.commit()
    return {"ok": True, "ticket_no": _ticket_number(ticket_id), "status": "closed"}


@app.get("/api/faq")
def public_faq(limit: int = 200) -> dict[str, Any]:
    items = _list_faq_items(include_unpublished=False, limit=limit)
    return {"items": items, "count": len(items)}


@app.get("/api/admin/failures")
def admin_failures(request: Request, response: Response, limit: int = 50) -> dict[str, Any]:
    _require_admin(request, response)
    items = _recent_failed_runs(limit=limit)
    return {"items": items, "count": len(items)}


@app.get("/api/admin/help-tickets")
def admin_help_tickets(request: Request, response: Response, limit: int = 100) -> dict[str, Any]:
    _require_admin(request, response)
    items = _list_help_tickets(limit=limit)
    return {"items": items, "count": len(items)}


@app.post("/api/admin/help-tickets/update")
def admin_help_ticket_update(req: HelpTicketUpdate, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    status = (req.status or "").strip().lower()
    if status and status not in {"open", "in_progress", "done"}:
        raise HTTPException(status_code=400, detail="status must be open, in_progress, or done.")
    ticket_no = 0
    prev_admin_note = ""
    ticket_session_id = ""
    ticket_wallet = ""
    admin_note = req.admin_note if req.admin_note is not None else None
    with _analytics_conn() as conn:
        row = conn.execute(
            "SELECT id, session_id, wallet_address, admin_note FROM help_tickets WHERE id = ?",
            (int(req.ticket_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        ticket_no = _ticket_number(int(row[0]))
        ticket_session_id = str(row[1] or "")
        ticket_wallet = str(row[2] or "")
        prev_admin_note = str(row[3] or "").strip()
        if status:
            conn.execute("UPDATE help_tickets SET status = ? WHERE id = ?", (status, int(req.ticket_id)))
        if admin_note is not None:
            conn.execute("UPDATE help_tickets SET admin_note = ? WHERE id = ?", ((admin_note or "")[:1000], int(req.ticket_id)))
        conn.commit()
    new_admin_note = (admin_note or "").strip() if admin_note is not None else ""
    if new_admin_note and new_admin_note != prev_admin_note:
        _append_help_ticket_message(
            ticket_id=int(req.ticket_id),
            author_type="admin",
            message=new_admin_note,
            session_id=ticket_session_id,
            wallet_address=ticket_wallet,
        )
    return {"ok": True, "ticket_no": ticket_no}


@app.post("/api/admin/help-tickets/delete")
def admin_help_ticket_delete(req: HelpTicketDelete, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    ticket_id = int(req.ticket_id or 0)
    if ticket_id <= 0:
        raise HTTPException(status_code=400, detail="ticket_id is required.")
    with _analytics_conn() as conn:
        row = conn.execute("SELECT id FROM help_tickets WHERE id = ?", (ticket_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Ticket not found.")
        ticket_no = _ticket_number(ticket_id)
        conn.execute("DELETE FROM help_ticket_messages WHERE ticket_id = ?", (ticket_id,))
        conn.execute("DELETE FROM help_tickets WHERE id = ?", (ticket_id,))
        conn.commit()
    return {"ok": True, "ticket_no": ticket_no, "info": f"Ticket #{ticket_no} deleted."}


@app.get("/api/admin/help-feedback")
def admin_help_feedback(request: Request, response: Response, limit: int = 200) -> dict[str, Any]:
    _require_admin(request, response)
    items = _list_help_feedback(limit=limit)
    return {"items": items, "count": len(items)}


@app.post("/api/admin/help-feedback/review")
def admin_help_feedback_review(req: AdminFeedbackReview, request: Request, response: Response) -> dict[str, Any]:
    admin_address, _auth = _require_admin(request, response)
    row = _mark_help_feedback_reviewed(req.feedback_id, reviewer_address=admin_address)
    return {"ok": True, **row}


@app.post("/api/admin/help-feedback/delete")
def admin_help_feedback_delete(req: AdminFeedbackDelete, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    fid = int(req.feedback_id or 0)
    if fid <= 0:
        raise HTTPException(status_code=400, detail="feedback_id is required.")
    with _analytics_conn() as conn:
        row = conn.execute("SELECT id FROM help_feedback WHERE id = ?", (fid,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Feedback not found.")
        feedback_no = _feedback_number(fid)
        conn.execute("DELETE FROM help_feedback WHERE id = ?", (fid,))
        conn.commit()
    return {"ok": True, "feedback_no": feedback_no, "info": f"Feedback #{feedback_no} deleted."}


@app.get("/api/admin/faq")
def admin_faq_list(request: Request, response: Response, limit: int = 200) -> dict[str, Any]:
    _require_admin(request, response)
    items = _list_faq_items(include_unpublished=True, limit=limit)
    return {"items": items, "count": len(items)}


@app.post("/api/admin/faq/upsert")
def admin_faq_upsert(req: AdminFaqUpsert, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    question = (req.question or "").strip()
    answer = (req.answer or "").strip()
    if len(question) < 3:
        raise HTTPException(status_code=400, detail="Question is too short.")
    if len(answer) < 3:
        raise HTTPException(status_code=400, detail="Answer is too short.")
    sort_order = int(req.sort_order or 100)
    now = _iso_now()
    with _analytics_conn() as conn:
        if req.faq_id:
            row = conn.execute("SELECT id FROM faq_items WHERE id = ?", (int(req.faq_id),)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="FAQ item not found.")
            conn.execute(
                """
                UPDATE faq_items
                SET updated_at = ?, question = ?, answer = ?, is_published = ?, is_featured = ?, sort_order = ?
                WHERE id = ?
                """,
                (
                    now,
                    question[:300],
                    answer[:8000],
                    1 if req.is_published else 0,
                    1 if req.is_featured else 0,
                    sort_order,
                    int(req.faq_id),
                ),
            )
            conn.commit()
            return {"ok": True, "info": "FAQ updated.", "faq_id": int(req.faq_id)}
        cur = conn.execute(
            """
            INSERT INTO faq_items(created_at, updated_at, question, answer, is_published, is_featured, sort_order)
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now,
                now,
                question[:300],
                answer[:8000],
                1 if req.is_published else 0,
                1 if req.is_featured else 0,
                sort_order,
            ),
        )
        conn.commit()
        return {"ok": True, "info": "FAQ created.", "faq_id": int(cur.lastrowid or 0)}


@app.post("/api/admin/faq/delete")
def admin_faq_delete(req: AdminFaqDelete, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    with _analytics_conn() as conn:
        row = conn.execute("SELECT id FROM faq_items WHERE id = ?", (int(req.faq_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="FAQ item not found.")
        conn.execute("DELETE FROM faq_items WHERE id = ?", (int(req.faq_id),))
        conn.commit()
    return {"ok": True, "info": "FAQ deleted."}


@app.post("/api/admin/faq/publish")
def admin_faq_publish(req: AdminFaqPublish, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    with _analytics_conn() as conn:
        row = conn.execute("SELECT id FROM faq_items WHERE id = ?", (int(req.faq_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="FAQ item not found.")
        conn.execute(
            "UPDATE faq_items SET is_published = ?, updated_at = ? WHERE id = ?",
            (1 if req.is_published else 0, _iso_now(), int(req.faq_id)),
        )
        conn.commit()
    return {"ok": True, "info": "FAQ publish status updated."}


@app.get("/api/meta")
def meta() -> dict[str, Any]:
    token_catalog = _load_token_catalog(refresh=False)
    chain_catalog = _load_chain_catalog(refresh=False)
    return {
        "tokens": token_catalog.get("items", []),
        "chains": chain_catalog.get("items", []),
        "token_catalog": {
            "count": token_catalog.get("count", 0),
            "updated_at": token_catalog.get("updated_at"),
            "source": token_catalog.get("source", ""),
            "min_tvl_usd": token_catalog.get("min_tvl_usd", TOKENS_MIN_TVL_USD),
        },
        "chain_catalog": {
            "count": chain_catalog.get("count", 0),
            "updated_at": chain_catalog.get("updated_at"),
        },
    }


@app.post("/api/catalog/tokens/review")
def review_tokens() -> dict[str, Any]:
    data = _load_token_catalog(refresh=True)
    return {
        "ok": True,
        "count": data.get("count", 0),
        "updated_at": data.get("updated_at"),
    }


@app.post("/api/catalog/chains/review")
def review_chains() -> dict[str, Any]:
    data = _load_chain_catalog(refresh=True)
    return {
        "ok": True,
        "count": data.get("count", 0),
        "updated_at": data.get("updated_at"),
    }


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/robots.txt", response_class=PlainTextResponse)
def robots_txt(request: Request) -> str:
    base = _public_base_url(request)
    return "\n".join(
        [
            "User-agent: *",
            "Allow: /",
            "Disallow: /api/",
            f"Sitemap: {base}/sitemap.xml",
            "",
        ]
    )


@app.get("/sitemap.xml")
def sitemap_xml(request: Request) -> Response:
    base = _public_base_url(request)
    urls = ["/", "/pancake", "/stables", "/positions", "/help", "/connect"]
    now = datetime.now(timezone.utc).date().isoformat()
    body = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in urls:
        body.extend(
            [
                "  <url>",
                f"    <loc>{base}{path}</loc>",
                f"    <lastmod>{now}</lastmod>",
                "    <changefreq>daily</changefreq>",
                "  </url>",
            ]
        )
    body.append("</urlset>")
    return Response(content="\n".join(body), media_type="application/xml")


@app.post("/api/pools/run")
def run_pools(req: PoolsRunRequest, request: Request, response: Response) -> dict[str, str]:
    session_id = _ensure_session_cookie(request, response)
    if not os.environ.get("THE_GRAPH_API_KEY"):
        raise HTTPException(status_code=400, detail="Missing THE_GRAPH_API_KEY on server.")
    if req.days < 1 or req.days > 3650:
        raise HTTPException(status_code=400, detail="days must be an integer between 1 and 3650")
    if req.min_tvl < 0 or req.min_tvl > 10_000_000:
        raise HTTPException(status_code=400, detail="min_tvl must be in range 0..10000000")
    if req.min_fee_pct < 0 or req.min_fee_pct > 1:
        raise HTTPException(status_code=400, detail="min_fee_pct must be in range 0..1")
    if req.max_fee_pct < 1 or req.max_fee_pct > 3:
        raise HTTPException(status_code=400, detail="max_fee_pct must be in range 1..3")
    if req.min_fee_pct >= req.max_fee_pct:
        raise HTTPException(status_code=400, detail="min_fee_pct must be lower than max_fee_pct")
    req.speed_mode = str(req.speed_mode or "normal").strip().lower()
    if req.speed_mode not in {"normal", "fast"}:
        raise HTTPException(status_code=400, detail="speed_mode must be normal or fast")
    req.include_versions = [str(v).strip().lower() for v in (req.include_versions or []) if str(v).strip()]
    req.include_versions = [v for v in req.include_versions if v in {"v3", "v4"}]
    req.include_versions = list(dict.fromkeys(req.include_versions))
    if not req.include_versions:
        raise HTTPException(status_code=400, detail="Select at least one protocol version (v3 or v4)")
    req.exclude_suffixes = [
        str(x).strip().lower().replace("0x", "")[-4:]
        for x in req.exclude_suffixes
        if str(x).strip()
    ]
    _analytics_log_event(
        session_id=session_id,
        event_type="run_start",
        path="/api/pools/run",
        payload=_pairs_to_string(_parse_pairs_str(";".join(req.pairs))),
    )

    job_id = str(uuid.uuid4())
    with JOB_LOCK:
        JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "stage": "queued",
            "stage_label": "Queued",
            "progress": 0,
            "created_at": time.time(),
            "result": None,
            "error": None,
        }
    t = threading.Thread(target=_run_pool_job, args=(job_id, req, session_id), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/jobs/{job_id}")
def job_status(job_id: str) -> dict[str, Any]:
    with JOB_LOCK:
        job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@app.get("/api/runs/recent")
def recent_runs(request: Request, response: Response) -> dict[str, Any]:
    session_id = _ensure_session_cookie(request, response)
    with JOB_LOCK:
        items = list(RUN_HISTORY.get(session_id, []))
    return {"items": items}


@app.post("/api/runs/reset")
def reset_runs(request: Request, response: Response) -> dict[str, Any]:
    session_id = _ensure_session_cookie(request, response)
    with JOB_LOCK:
        RUN_HISTORY.pop(session_id, None)
    return {"ok": True}


HTML_PAGE = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Uni Fee - Pools</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    html {
      overflow-y: scroll;
      scrollbar-gutter: stable;
      overflow-x: hidden;
    }
    :root {
      --bg: #e2eaf8;
      --card: #f4f7fc;
      --muted: #64748b;
      --text: #0f172a;
      --border: #d9e2f0;
      --ok: #22c55e;
      --warn: #eab308;
      --danger: #ef4444;
      --accent: #2563eb;
      --accent-2: #06b6d4;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Inter, Arial, sans-serif;
      background: linear-gradient(180deg, #d9e3f5 0%, var(--bg) 100%);
      color: var(--text);
      min-height: 100vh;
      overflow-x: hidden;
    }
    .container {
      max-width: 1200px;
      margin: 0 auto;
      padding: 18px;
      min-height: calc(100vh - 36px);
    }
    .header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 14px;
    }
    .title {
      margin: 0;
      font-size: 30px;
      font-weight: 800;
      letter-spacing: 0.2px;
    }
    .subtitle {
      margin: 4px 0 0;
      color: var(--muted);
      font-size: 14px;
    }
    .top-controls {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: flex-end;
      flex-wrap: nowrap;
    }
    .intent-prefix {
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      white-space: nowrap;
    }
    .intent-select {
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 38px 10px 12px;
      font-size: 14px;
      font-weight: 600;
      color: #1f3a8a;
      background: linear-gradient(180deg, #f8fbff 0%, #eff6ff 100%);
      min-width: 320px;
      max-width: 360px;
      appearance: none;
      -webkit-appearance: none;
      background-image:
        linear-gradient(45deg, transparent 50%, #1d4ed8 50%),
        linear-gradient(135deg, #1d4ed8 50%, transparent 50%);
      background-position:
        calc(100% - 18px) calc(50% + 1px),
        calc(100% - 12px) calc(50% + 1px);
      background-size: 6px 6px, 6px 6px;
      background-repeat: no-repeat;
      box-shadow: inset 0 1px 0 rgba(255,255,255,0.7);
    }
    .intent-select option {
      background: #eef4ff;
      color: #1f3a8a;
    }
    .connect-btn {
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 16px;
      font-size: 14px;
      font-weight: 700;
      color: #1d4ed8;
      background: #eff6ff;
      cursor: pointer;
      white-space: nowrap;
      width: 190px;
      box-sizing: border-box;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .wallet-modal-backdrop {
      position: fixed;
      inset: 0;
      background: linear-gradient(180deg, rgba(217,227,245,0.82) 0%, rgba(236,242,255,0.82) 100%);
      backdrop-filter: blur(3px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 9999;
    }
    .wallet-modal {
      width: min(460px, calc(100vw - 24px));
      background: #f8fbff;
      border: 1px solid #cbd5e1;
      border-radius: 14px;
      box-shadow: 0 12px 36px rgba(15, 23, 42, 0.25);
      padding: 14px;
    }
    .wallet-modal h3 {
      margin: 0 0 8px;
      font-size: 18px;
    }
    .wallet-list {
      display: grid;
      grid-template-columns: 1fr;
      gap: 8px;
      margin-top: 10px;
    }
    .wallet-item {
      border: 1px solid #bfdbfe;
      border-radius: 10px;
      padding: 10px 12px;
      background: #eff6ff;
      color: #1d4ed8;
      font-weight: 700;
      text-align: left;
      cursor: pointer;
    }
    .wallet-item.disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .wallet-note {
      color: #64748b;
      font-size: 12px;
      margin-top: 8px;
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 14px;
    }
    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px;
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
    }
    .control-card {
      background: linear-gradient(180deg, #f4f8ff 0%, #eef4ff 100%);
      border-color: #cfdcec;
    }
    .card h3 {
      margin: 0 0 10px;
      font-size: 18px;
    }
    .form-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .control-card .form-grid { gap: 10px; }
    .row {
      display: grid;
      grid-template-columns: 180px 1fr;
      gap: 12px;
      align-items: start;
    }
    .control-card .row {
      background: #f8fbff;
      border: 1px solid #d7e1ef;
      border-radius: 12px;
      padding: 10px 12px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7);
    }
    .row label {
      font-weight: 700;
      font-size: 14px;
      padding-top: 8px;
    }
    .control-card .row label {
      color: #1f2f4a;
      letter-spacing: 0.2px;
    }
    .row .hint {
      color: var(--muted);
      font-size: 12px;
      margin-top: 5px;
    }
    input, textarea, select {
      width: 100%;
      background: #f8fbff;
      border: 1px solid #cbd5e1;
      color: var(--text);
      border-radius: 8px;
      padding: 8px;
      font-size: 14px;
    }
    select { appearance: auto; }
    .actions {
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
    }
    .actions-left, .actions-right {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
    }
    .control-card .actions {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid #d7e1ef;
    }
    .actions .run-btn { order: 99; }
    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .section-head h3 {
      margin: 0;
      font-size: 17px;
      color: #1f3a8a;
    }
    .section-actions {
      display: flex;
      gap: 10px;
      align-items: center;
      margin-left: auto;
      justify-content: flex-end;
    }
    .btn {
      border: 0;
      border-radius: 10px;
      padding: 10px 14px;
      font-weight: 700;
      font-size: 14px;
      cursor: pointer;
      color: #fff;
      background: linear-gradient(90deg, var(--accent), #4338ca);
    }
    .btn.secondary {
      background: #f8fafc;
      border: 1px solid #d1d5db;
      color: #334155;
    }
    .search-link-btn {
      border: none;
      background: transparent;
      color: #1d4ed8;
      font-size: 13px;
      font-weight: 700;
      cursor: pointer;
      padding: 0;
      text-decoration: underline;
      text-underline-offset: 2px;
    }
    .search-link-btn:hover { color: #1e40af; }
    .chips {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      margin-top: 8px;
    }
    .chip {
      border-radius: 999px;
      border: 1px solid #cbd5e1;
      background: #f8fafc;
      color: #334155;
      padding: 5px 10px;
      font-size: 12px;
      cursor: pointer;
    }
    .status {
      font-size: 13px;
      color: #cbd5e1;
      display: inline-block;
      width: 280px;
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .status.running { color: var(--warn); }
    .status.ok { color: var(--ok); }
    .status.fail { color: var(--danger); }
    .progress-wrap { width: 100%; margin-top: 10px; }
    .control-card .progress-wrap {
      margin-top: 8px;
      padding: 8px 10px 0;
      border-top: 1px dashed #d7e1ef;
    }
    .progress-meta {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
    }
    .progress-bar {
      width: 100%;
      height: 10px;
      border-radius: 999px;
      background: #1f2937;
      border: 1px solid #334155;
      overflow: hidden;
    }
    .progress-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      transition: width 0.3s ease;
    }
    .summary-strip {
      display: flex;
      gap: 8px;
      flex-wrap: nowrap;
      align-items: stretch;
      width: 100%;
      overflow-x: auto;
      padding-bottom: 2px;
    }
    .summary-box {
      flex: 1 1 0;
      min-width: 0;
      background: #f8fbff;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      padding: 8px 10px;
      height: 56px;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .summary-box .k {
      color: #64748b;
      font-size: 11px;
      line-height: 1.1;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .summary-box .v {
      font-size: 17px;
      font-weight: 800;
      line-height: 1.1;
      margin-top: 2px;
      color: #0f172a;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .charts-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }
    .plot {
      width: 100%;
      min-height: 330px;
      border: 1px solid #dbe3ef;
      border-radius: 10px;
      background: #f8fbff;
    }
    .table-wrap {
      overflow-x: auto;
      border: 1px solid #dbe3ef;
      border-radius: 10px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      min-width: 900px;
    }
    th, td {
      border-bottom: 1px solid #e2e8f0;
      padding: 8px;
      text-align: left;
    }
    th {
      background: #eff6ff;
      color: #1e3a8a;
      position: sticky;
      top: 0;
      cursor: pointer;
      white-space: nowrap;
    }
    tr.ok-row:hover td { background: rgba(34,197,94,0.10); }
    tr.error-row td { background: rgba(239,68,68,0.08); }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
    details { margin-top: 10px; }
    pre {
      max-height: 220px;
      overflow: auto;
      background: #f8fafc;
      border: 1px solid #dbe3ef;
      border-radius: 8px;
      padding: 8px;
      color: #334155;
      font-size: 12px;
    }
    .pair-row { display: grid; grid-template-columns: repeat(4, minmax(160px, 1fr)); gap: 6px; }
    .pair-item {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 0;
      border: 1px solid #cbd5e1;
      border-radius: 10px;
      overflow: hidden;
      background: #eef2f7;
    }
    .token-input-wrap { display: flex; align-items: center; gap: 0; }
    .token-input-wrap:first-child { border-right: 1px solid #cbd5e1; }
    .token-input-wrap input {
      border: 0;
      border-radius: 0;
      background: #f8fbff;
    }
    /* Hide native datalist indicator to avoid double arrows with custom button */
    .token-input-wrap input[list]::-webkit-calendar-picker-indicator { display: none !important; }
    .token-input-wrap .dd-btn {
      border: 0;
      background: #eef2f7;
      color: #334155;
      height: 100%;
      min-width: 32px;
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
    }
    .invalid-input {
      border: 1px solid #ef4444 !important;
      box-shadow: 0 0 0 2px rgba(239, 68, 68, 0.18);
    }
    .top-line { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }
    .meta-badge { border: 1px solid #cbd5e1; border-radius: 999px; padding: 4px 10px; font-size: 12px; color: #475569; background: #f8fafc; }
    .info-chip {
      border: 1px solid #cbd5e1;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      color: #475569;
      background: #f1f5f9;
      white-space: nowrap;
    }
    .small-btn { border: 1px solid #d1d5db; background: #f8fafc; color: #334155; border-radius: 8px; padding: 6px 10px; font-size: 12px; cursor: pointer; font-weight: 600; }
    .line-swatch {
      display: inline-block;
      width: 28px;
      height: 0;
      border-top-width: 3px;
      border-top-style: solid;
      border-top-color: #64748b;
      vertical-align: middle;
    }

    .chain-grid {
      display: grid;
      grid-template-rows: repeat(2, minmax(22px, auto));
      grid-auto-flow: column;
      grid-auto-columns: minmax(110px, max-content);
      gap: 4px 8px;
      margin-top: 2px;
    }
    .check {
      display: flex;
      align-items: center;
      gap: 6px;
      font-size: 12px;
      color: #334155;
    }
    .check input {
      width: auto;
      padding: 0;
    }
    
    .inline-grid { display: grid; grid-template-columns: 90px 90px 120px 120px 220px 130px; gap: 6px; align-items: end; }
    .filter-item .hint {
      margin-bottom: 4px !important;
      min-height: 34px;
      display: flex;
      align-items: flex-end;
      line-height: 1.15;
      white-space: normal;
    }
    .proto-checks {
      display: flex;
      gap: 10px;
      align-items: center;
      height: 34px;
      padding: 0 4px;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      background: #f8fbff;
    }
    .proto-checks label {
      display: inline-flex;
      align-items: center;
      gap: 5px;
      font-size: 12px;
      font-weight: 600;
      color: #334155;
      padding-top: 0;
      line-height: 1;
      white-space: nowrap;
    }
    .proto-checks input[type="checkbox"] {
      width: auto;
      min-width: 0;
      padding: 0;
      margin: 0;
      flex: 0 0 auto;
      accent-color: #2563eb;
      transform: translateY(-1px);
    }
    @media (max-width: 980px) {
      .row { grid-template-columns: 1fr; }
      .row label { padding-top: 0; }
      .pair-row { grid-template-columns: 1fr 1fr; }
      .inline-grid { grid-template-columns: 1fr 1fr; }
      .summary-strip { gap: 6px; }
      .summary-box { min-width: 150px; }
    }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <div>
        <h1 class="title">DeFi Pools</h1>
        <p class="subtitle">Find the best fee on Uniswap</p>
      </div>
      <div class="top-controls">
        <span class="intent-prefix">I want to</span>
        <select class="intent-select" id="intentSelect" onchange="navigateIntent(this.value)">
          <option value="/" selected>Find the best fee on Uniswap</option>
          <option value="/pancake">Find the best pool on PancakeSwap</option>
          <option value="/stables">Optimize my lending positions</option>
          <option value="/positions">Optimize my pool positions</option>
          <option value="/help">Send wishes or report issues</option>
        </select>
        <button class="connect-btn" id="connectWalletBtn" onclick="onConnectWalletClick()">Connect Wallet</button>
      </div>
    </div>

    <div class="grid">
      <section class="card control-card">
        <div class="form-grid">
          <div class="row">
            <label title="Select up to 4 pairs for analysis">Pairs</label>
            <div>
              <div class="top-line">
                <button class="small-btn" onclick="addPairRow()">+ pair</button>
                <button class="small-btn" id="removePairBtn" onclick="removePairRow()">- pair</button>
                <span class="meta-badge" id="tokensMeta">popular tokens: -</span>
                <span class="info-chip">Popular list only - manual token input supported</span>
              </div>
              <div class="pair-row" id="pairRows">
                <div class="pair-item" id="pairRow1">
                  <div class="token-input-wrap"><input id="pair1a" list="tokenHints" placeholder="base token" value="usdt"/><button class="dd-btn" onclick="openTokenList('pair1a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair1b" list="tokenHints" placeholder="quote token" value="usdc"/><button class="dd-btn" onclick="openTokenList('pair1b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow2" style="display:none">
                  <div class="token-input-wrap"><input id="pair2a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair2a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair2b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair2b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow3" style="display:none">
                  <div class="token-input-wrap"><input id="pair3a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair3a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair3b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair3b')">▼</button></div>
                </div>
                <div class="pair-item" id="pairRow4" style="display:none">
                  <div class="token-input-wrap"><input id="pair4a" list="tokenHints" placeholder="base token"/><button class="dd-btn" onclick="openTokenList('pair4a')">▼</button></div>
                  <div class="token-input-wrap"><input id="pair4b" list="tokenHints" placeholder="quote token"/><button class="dd-btn" onclick="openTokenList('pair4b')">▼</button></div>
                </div>
              </div>
              <datalist id="tokenHints"></datalist>
            </div>
          </div>

          <div class="row">
            <label title="Choose chains for analysis">Include chains</label>
            <div>
              <div class="top-line">
                <span class="meta-badge" id="chainsMeta">chains: -</span>
              </div>
              <div class="chain-grid" id="chainChecks"></div>
            </div>
          </div>

          <div class="row">
            <label>Filters</label>
            <div>
              <div class="inline-grid">
                <div class="filter-item">
                  <div class="hint">Min TVL<br/>(USD)</div>
                  <input id="minTvl" value="1000" type="number" min="0" max="10000000" step="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">History<br/>days</div>
                  <input id="days" value="30" type="number" min="1" max="3650" step="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Exclude below<br/>X% fee</div>
                  <input id="minFeePct" value="0" type="number" step="0.1" min="0" max="1"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Exclude above<br/>X% fee</div>
                  <input id="maxFeePct" value="2" type="number" step="0.1" min="1" max="3"/>
                </div>
                <div class="filter-item">
                  <div class="hint">Protocol<br/>version</div>
                  <div class="proto-checks">
                    <label><input id="protoV3" type="checkbox" checked/> V3</label>
                    <label><input id="protoV4" type="checkbox" checked/> V4</label>
                  </div>
                </div>
                <div class="filter-item">
                  <div class="hint">Speed<br/>mode</div>
                  <select id="speedMode">
                    <option value="normal" selected>Normal</option>
                    <option value="fast">Fast</option>
                  </select>
                </div>
              </div>
            </div>
          </div>
        </div>

      </section>

      <section class="card">
        <div class="section-head">
          <h3>Fee Performance History</h3>
          <div class="section-actions">
            <button class="btn run-btn" id="scanBtn" onclick="runJob()">Scan</button>
            <span id="status" class="status">Ready</span>
          </div>
        </div>
        <div class="progress-wrap" style="margin-top:0">
          <div class="progress-meta">
            <span id="stageText">Stage: waiting</span>
            <span id="progressText">0%</span>
          </div>
          <div class="progress-bar"><div id="progressFill" class="progress-fill"></div></div>
        </div>
        <div class="charts-grid">
          <div id="feesChart" class="plot"></div>
          <div id="tvlChart" class="plot"></div>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>Pools Table</h3>
          <div class="section-actions">
            <button class="search-link-btn" type="button" onclick="exportCsv()">Export CSV</button>
          </div>
        </div>
        <div class="table-wrap">
          <table id="resultTable"></table>
        </div>
      </section>
    </div>
  </div>

  <div id="walletModalBackdrop" class="wallet-modal-backdrop" onclick="closeWalletModal(event)">
    <div class="wallet-modal">
      <h3>Connect wallet</h3>
      <div class="wallet-list" id="walletList"></div>
      <div class="wallet-note">Rabby and Phantom are supported. Sign-in uses a gasless message signature.</div>
    </div>
  </div>

  <script>
    const SORTABLE = {
      color: (r) => r.pool_id || "",
      visibility: (r) => r.pool_id || "",
      chain: (r) => r.chain || "",
      version: (r) => r.version || "",
      pair: (r) => r.pair || "",
      fee_pct: (r) => Number(r.fee_pct || 0),
      final_income: (r) => Number(r.final_income || 0),
      apy_pct: (r) => Number(r.apy_pct || 0),
      last_tvl: (r) => Number(r.last_tvl || 0),
      status: (r) => r.status || ""
    };

    let lastRows = [];
    let renderedRows = [];
    let sortKey = "final_income";
    let sortDesc = true;
    const FORM_STORAGE_KEY = "uni_fee_form_v4";
    const RESULT_STORAGE_KEY = "uni_fee_result_v1";
    const FIELD_IDS = ["pair1a", "pair1b", "pair2a", "pair2b", "pair3a", "pair3b", "pair4a", "pair4b", "minTvl", "days", "maxFeePct", "minFeePct", "protoV3", "protoV4", "speedMode", "allChains"];
    let availableChains = [];
    let colorMap = {};
    let dashMap = {};
    let visibilityMap = {};
    let seriesByPool = {};
    let currentRequest = {};
    let pairRowsVisible = 1;
    let authState = {authenticated: false};
    let hasScanRun = false;
    const WALLETCONNECT_PROJECT_ID = "__WALLETCONNECT_PROJECT_ID__";

    const WALLET_LABELS = {
      injected: "Browser Wallet",
      walletconnect: "WalletConnect (QR)",
      rabby: "Rabby",
      metamask: "MetaMask",
      phantom: "Phantom",
      coinbase: "Coinbase Wallet",
    };

    function splitCSV(v) {
      return (v || "").split(",").map(x => x.trim().toLowerCase()).filter(Boolean);
    }

    function formatUsd(v) {
      return new Intl.NumberFormat("en-US", {maximumFractionDigits: 0}).format(Number(v || 0));
    }

    function formatUsdShort(v) {
      const n = Number(v || 0);
      if (n >= 1000000) return "$" + (n / 1000000).toFixed(n % 1000000 === 0 ? 0 : 1) + "M";
      if (n >= 1000) return "$" + (n / 1000).toFixed(n % 1000 === 0 ? 0 : 1) + "k";
      return "$" + String(Math.round(n));
    }

    function getDaysValue() {
      const v = Number(document.getElementById("days")?.value || 30);
      return Number.isFinite(v) && v > 0 ? v : 30;
    }

    function setDays(v) {
      document.getElementById("days").value = v;
      saveFormState();
    }

    function navigateIntent(path) {
      if (!path) return;
      window.location.href = path;
    }
    function refreshIntentMenu() {
      const sel = document.getElementById("intentSelect");
      if (!sel) return;
      sel.style.position = "absolute";
      sel.style.left = "-9999px";
      sel.style.opacity = "0";
      sel.style.pointerEvents = "none";
      let wrap = document.getElementById("intentMenuWrap");
      if (!wrap) {
        wrap = document.createElement("div");
        wrap.id = "intentMenuWrap";
        wrap.style.cssText = "position:relative;min-width:320px;max-width:360px;";
        const btn = document.createElement("button");
        btn.type = "button";
        btn.id = "intentMenuBtn";
        btn.style.cssText = "width:100%;border:1px solid #bfdbfe;border-radius:10px;padding:10px 38px 10px 12px;font-size:14px;font-weight:600;color:#1f3a8a;background:linear-gradient(180deg,#f8fbff 0%,#eff6ff 100%);text-align:left;cursor:pointer;box-shadow:inset 0 1px 0 rgba(255,255,255,0.7);";
        const list = document.createElement("div");
        list.id = "intentMenuList";
        list.style.cssText = "display:none;position:absolute;z-index:12000;left:0;right:0;top:calc(100% + 6px);background:#eef4ff;border:1px solid #bfdbfe;border-radius:10px;box-shadow:0 10px 24px rgba(15,23,42,0.15);padding:6px;max-height:320px;overflow:auto;";
        wrap.appendChild(btn);
        wrap.appendChild(list);
        sel.insertAdjacentElement("afterend", wrap);
        btn.onclick = () => {
          list.style.display = list.style.display === "block" ? "none" : "block";
        };
        document.addEventListener("click", (e) => {
          if (!wrap.contains(e.target)) list.style.display = "none";
        });
      }
      const btn = document.getElementById("intentMenuBtn");
      const list = document.getElementById("intentMenuList");
      const options = Array.from(sel.options || []);
      const selected = options.find((o) => o.selected) || options[0];
      btn.textContent = selected ? selected.textContent : "Select";
      list.innerHTML = options.map((o) => {
        const active = o.value === sel.value;
        const style = active
          ? "display:block;width:100%;padding:9px 10px;border:none;background:#dbeafe;color:#1e3a8a;font-weight:700;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;"
          : "display:block;width:100%;padding:9px 10px;border:none;background:#eef4ff;color:#1f3a8a;font-weight:600;text-align:left;border-radius:8px;margin-bottom:4px;cursor:pointer;white-space:nowrap;";
        return `<button type="button" data-v="${o.value}" style="${style}">${o.textContent}</button>`;
      }).join("");
      Array.from(list.querySelectorAll("button[data-v]")).forEach((b) => {
        b.onclick = () => {
          const v = b.getAttribute("data-v") || "";
          sel.value = v;
          list.style.display = "none";
          navigateIntent(v);
        };
      });
    }

    function getEthereumProviders() {
      const out = [];
      const eth = window.ethereum;
      if (!eth) return out;
      if (Array.isArray(eth.providers) && eth.providers.length) return eth.providers;
      out.push(eth);
      return out;
    }

    function getWalletProvider(wallet) {
      const providers = getEthereumProviders();
      const pick = (pred) => providers.find(pred) || null;
      if (wallet === "injected") {
        return (
          pick((p) => !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          pick((p) => !!p?.isMetaMask && !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          pick((p) => !!p?.isCoinbaseWallet) ||
          providers[0] ||
          window.ethereum ||
          null
        );
      }
      if (wallet === "rabby") {
        return pick((p) => !!p?.isRabby) || (window.ethereum?.isRabby ? window.ethereum : null);
      }
      if (wallet === "phantom") {
        if (window.phantom?.ethereum?.request) return window.phantom.ethereum;
        return pick((p) => !!p?.isPhantom) || (window.ethereum?.isPhantom ? window.ethereum : null);
      }
      if (wallet === "metamask") {
        return (
          pick((p) => !!p?.isMetaMask && !p?.isRabby && !p?.isPhantom && !p?.isCoinbaseWallet) ||
          ((window.ethereum?.isMetaMask && !window.ethereum?.isRabby && !window.ethereum?.isPhantom && !window.ethereum?.isCoinbaseWallet) ? window.ethereum : null)
        );
      }
      if (wallet === "coinbase") {
        return pick((p) => !!p?.isCoinbaseWallet) || (window.ethereum?.isCoinbaseWallet ? window.ethereum : null);
      }
      return null;
    }

    function getWalletChoices() {
      const order = ["walletconnect", "rabby", "phantom", "metamask", "coinbase", "injected"];
      return order.map((id) => {
        const isWc = id === "walletconnect";
        const available = isWc ? true : !!getWalletProvider(id);
        let label = WALLET_LABELS[id];
        if (isWc && !WALLETCONNECT_PROJECT_ID) label += " (setup required)";
        else if (!available) label += " (not detected)";
        return {id, label, available};
      });
    }

    function openWalletModal() {
      const list = document.getElementById("walletList");
      const choices = getWalletChoices();
      list.innerHTML = choices.map((w) => {
        const cls = w.available ? "wallet-item" : "wallet-item disabled";
        const dis = w.available ? "" : "disabled";
        return `<button class="${cls}" ${dis} onclick="connectWalletFlow('${w.id}')">${w.label}</button>`;
      }).join("");
      document.getElementById("walletModalBackdrop").style.display = "flex";
    }

    function closeWalletModal(event) {
      if (event && event.target && event.target.id !== "walletModalBackdrop") return;
      document.getElementById("walletModalBackdrop").style.display = "none";
    }

    async function postJson(url, payload) {
      const r = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload || {})
      });
      const data = await r.json().catch(() => ({}));
      if (!r.ok) {
        throw new Error(data.detail || data.info || "Request failed");
      }
      return data;
    }

    function syncAdminIntentOption() {
      const sel = document.getElementById("intentSelect");
      if (!sel) return;
      const existing = Array.from(sel.options).find((o) => o.value === "/admin");
      const isAdmin = !!authState?.authenticated && !!authState?.is_admin;
      if (isAdmin && !existing) {
        const opt = document.createElement("option");
        opt.value = "/admin";
        opt.textContent = "Administer project";
        sel.appendChild(opt);
      } else if (!isAdmin && existing) {
        existing.remove();
      }
      refreshIntentMenu();
    }

    function setAuthUI() {
      const btn = document.getElementById("connectWalletBtn");
      if (!btn) return;
      if (authState?.authenticated) {
        btn.textContent = authState.address_short || "Wallet connected";
      } else {
        btn.textContent = "Connect Wallet";
      }
      syncAdminIntentOption();
    }

    async function loadAuthState() {
      try {
        const r = await fetch("/api/auth/me");
        authState = await r.json();
      } catch (_) {
        authState = {authenticated: false};
      }
      setAuthUI();
    }

    async function onConnectWalletClick() {
      if (authState?.authenticated) {
        if (!confirm("Disconnect wallet?")) return;
        try {
          await postJson("/api/auth/logout", {});
          authState = {authenticated: false};
          setAuthUI();
          setStatus("Wallet disconnected", "ok");
        } catch (e) {
          setStatus("Disconnect failed: " + (e?.message || "unknown"), "fail");
        }
        return;
      }
      openWalletModal();
    }

    async function connectWalletFlow(wallet) {
      if (wallet === "walletconnect") {
        return connectWalletConnect();
      }
      const provider = getWalletProvider(wallet);
      if (!provider) {
        setStatus(`${WALLET_LABELS[wallet] || wallet} is not available in this browser`, "fail");
        return;
      }
      try {
        setStatus(`Connecting ${WALLET_LABELS[wallet] || wallet}...`, "running");
        const accounts = await provider.request({method: "eth_requestAccounts"});
        const address = String((accounts || [])[0] || "").trim();
        if (!address) throw new Error("Wallet did not return an address");
        const chainHex = await provider.request({method: "eth_chainId"});
        const chainId = Number.parseInt(String(chainHex || "0x1"), 16) || 1;

        const nonceResp = await postJson("/api/auth/nonce", {address, chain_id: chainId, wallet});
        const signature = await provider.request({method: "personal_sign", params: [nonceResp.message, address]});
        const verifyResp = await postJson("/api/auth/verify", {
          address,
          chain_id: chainId,
          wallet,
          message: nonceResp.message,
          signature,
        });
        authState = {authenticated: true, ...verifyResp};
        setAuthUI();
        closeWalletModal({target: {id: "walletModalBackdrop"}});
        setStatus(`Connected: ${verifyResp.address_short}`, "ok");
      } catch (e) {
        setStatus("Wallet auth failed: " + (e?.message || "unknown"), "fail");
      }
    }

    function showWcQrModal(uri) {
      let el = document.getElementById("wcQrBackdrop");
      if (!el) {
        el = document.createElement("div");
        el.id = "wcQrBackdrop";
        el.style.cssText = "position:fixed;inset:0;background:linear-gradient(180deg,rgba(217,227,245,0.95),rgba(236,242,255,0.95));backdrop-filter:blur(4px);display:flex;flex-direction:column;align-items:center;justify-content:center;z-index:10001;";
        el.innerHTML = '<div style="background:#f8fbff;border:1px solid #cbd5e1;border-radius:14px;padding:20px;text-align:center;box-shadow:0 12px 36px rgba(15,23,42,0.2)"><p style="margin:0 0 12px;font-size:16px;font-weight:700;color:#0f172a">Scan with your wallet app</p><img id="wcQrImg" alt="QR" style="display:block;background:#fff;padding:10px;border-radius:10px;width:260px;height:260px"/><p style="margin:10px 0 0;font-size:12px;color:#64748b">Or open the link on your phone</p><button id="wcQrCancel" type="button" style="margin-top:14px;padding:8px 16px;border-radius:10px;border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;font-weight:700;cursor:pointer">Cancel</button></div>';
        document.body.appendChild(el);
        document.getElementById("wcQrCancel").onclick = closeWcQrModal;
      }
      document.getElementById("wcQrImg").src = "https://api.qrserver.com/v1/create-qr-code/?size=260x260&data=" + encodeURIComponent(uri);
      el.style.display = "flex";
    }
    function closeWcQrModal() {
      const el = document.getElementById("wcQrBackdrop");
      if (el) el.style.display = "none";
      if (window._wcProvider) try { window._wcProvider.disconnect(); } catch (_) {}
      window._wcProvider = null;
    }
    async function connectWalletConnect() {
      if (!WALLETCONNECT_PROJECT_ID) {
        setStatus("WalletConnect is not configured (WALLETCONNECT_PROJECT_ID).", "fail");
        return;
      }
      const normalizeAddress = (value) => {
        const raw = String(value || "").trim();
        if (!raw) return "";
        const parts = raw.split(":");
        return String(parts[parts.length - 1] || "").trim();
      };
      const toHexMessage = (msg) => {
        try {
          return "0x" + Array.from(new TextEncoder().encode(String(msg || ""))).map((b) => b.toString(16).padStart(2, "0")).join("");
        } catch (_) {
          return "";
        }
      };
      try {
        setStatus("Connecting WalletConnect...", "running");
        const EthereumProviderModule = await import("https://esm.sh/@walletconnect/ethereum-provider@2.23.8");
        const wcChains = [1, 10, 56, 137, 8453, 42161, 43114];
        const wcMetadata = {
          name: "DeFi Pools",
          description: "DeFi Pools wallet sign-in",
          url: window.location.origin,
          icons: [window.location.origin + "/favicon.ico"],
        };
        const provider = await EthereumProviderModule.EthereumProvider.init({
          projectId: WALLETCONNECT_PROJECT_ID,
          optionalChains: wcChains,
          showQrModal: false,
          optionalMethods: ["eth_requestAccounts", "eth_accounts", "eth_chainId", "personal_sign", "wallet_switchEthereumChain"],
          optionalEvents: ["accountsChanged", "chainChanged", "disconnect"],
          metadata: wcMetadata,
          rpcMap: {},
        });
        provider.on("display_uri", showWcQrModal);
        window._wcProvider = provider;
        let connected = false;
        try {
          await provider.connect();
          connected = true;
        } catch (_) {}
        if (!connected) {
          try {
            await provider.enable();
          } catch (connErr) {
            closeWcQrModal();
            throw connErr;
          }
        }
        window._wcProvider = null;
        closeWcQrModal();
        let accounts = provider.accounts || [];
        if (!accounts.length) accounts = (await provider.request({method: "eth_accounts"})) || [];
        if (!accounts.length) accounts = (await provider.request({method: "eth_requestAccounts"})) || [];
        const address = normalizeAddress(accounts[0] || "");
        if (!/^0x[a-fA-F0-9]{40}$/.test(address)) throw new Error("WalletConnect did not return a valid EVM address");
        const chainHex = await provider.request({method: "eth_chainId"});
        const chainId = Number.parseInt(String(chainHex || "0x1"), 16) || 1;
        const nonceResp = await postJson("/api/auth/nonce", {address, chain_id: chainId, wallet: "walletconnect"});
        const messageHex = toHexMessage(nonceResp.message || "");
        const signVariants = [
          [nonceResp.message, address],
          [address, nonceResp.message],
          [messageHex, address],
          [address, messageHex],
        ];
        let signature = "";
        for (const params of signVariants) {
          try {
            if (!params[0]) continue;
            signature = await provider.request({method: "personal_sign", params});
            if (signature) break;
          } catch (_) {}
        }
        if (!signature) throw new Error("Failed to sign auth message via WalletConnect");
        const verifyResp = await postJson("/api/auth/verify", {
          address,
          chain_id: chainId,
          wallet: "walletconnect",
          message: nonceResp.message,
          signature,
        });
        authState = {authenticated: true, ...verifyResp};
        setAuthUI();
        closeWalletModal({target: {id: "walletModalBackdrop"}});
        setStatus(`Connected: ${verifyResp.address_short}`, "ok");
      } catch (e) {
        closeWcQrModal();
        setStatus("WalletConnect failed: " + (e?.message || "unknown") + ". Add this site to Reown Domain allowlist.", "fail");
      }
    }

    function setStatus(text, cssClass) {
      const el = document.getElementById("status");
      el.textContent = text;
      el.className = "status " + (cssClass || "");
    }

    function setBusy(flag) {
      const btn = document.getElementById("scanBtn");
      if (!btn) return;
      btn.disabled = flag;
      btn.style.opacity = flag ? "0.7" : "1";
      if (flag) {
        btn.textContent = "Scanning...";
      } else {
        btn.textContent = hasScanRun ? "Scan again" : "Scan";
      }
    }

    function updateProgress(progress, stageLabel) {
      const p = Math.max(0, Math.min(100, Number(progress || 0)));
      document.getElementById("progressFill").style.width = p + "%";
      document.getElementById("progressText").textContent = p + "%";
      document.getElementById("stageText").textContent = "Stage: " + (stageLabel || "running");
    }

    function saveFormState() {
      const state = {};
      for (const id of FIELD_IDS) {
        const el = document.getElementById(id);
        if (!el) continue;
        if (el.type === "checkbox") {
          state[id] = !!el.checked;
        } else {
          state[id] = el.value;
        }
      }
      state.selectedChains = getSelectedChains();
      state.pairRowsVisible = pairRowsVisible;
      localStorage.setItem(FORM_STORAGE_KEY, JSON.stringify(state));
    }

    function saveResultState(result) {
      try {
        if (!result || typeof result !== "object") return;
        const slim = {
          suffix: result.suffix || "",
          total: Number(result.total || 0),
          chart_pools: Number(result.chart_pools || 0),
          error_pools: Number(result.error_pools || 0),
          request: result.request || {},
          rows: Array.isArray(result.rows) ? result.rows : [],
          series: Array.isArray(result.series) ? result.series : [],
        };
        localStorage.setItem(RESULT_STORAGE_KEY, JSON.stringify({saved_at: Date.now(), result: slim}));
      } catch (e) {
        console.warn("save result state failed", e);
      }
    }

    function loadResultState() {
      try {
        const raw = localStorage.getItem(RESULT_STORAGE_KEY);
        if (!raw) return null;
        const parsed = JSON.parse(raw);
        if (!parsed || typeof parsed !== "object") return null;
        const result = parsed.result || null;
        if (!result || typeof result !== "object") return null;
        if (!Array.isArray(result.rows) || !Array.isArray(result.series)) return null;
        return result;
      } catch (e) {
        console.warn("load result state failed", e);
        return null;
      }
    }

    function loadFormState() {
      try {
        const raw = localStorage.getItem(FORM_STORAGE_KEY);
        if (!raw) return;
        const state = JSON.parse(raw);
        for (const id of FIELD_IDS) {
          const el = document.getElementById(id);
          if (!el || state[id] == null) continue;
          if (el.type === "checkbox") {
            el.checked = !!state[id];
          } else if (typeof state[id] === "string") {
            el.value = state[id];
          }
        }
        if (Array.isArray(state.selectedChains)) {
          for (const c of state.selectedChains) {
            const box = document.getElementById("chain_" + c);
            if (box) box.checked = true;
          }
        }
        // Always start with one visible pair row.
        pairRowsVisible = 1;
        updatePairRows();
      } catch (e) {
        console.warn("load form state failed", e);
      }
    }

    function attachAutosave() {
      for (const id of FIELD_IDS) {
        const el = document.getElementById(id);
        if (el) {
          el.addEventListener("input", saveFormState);
          el.addEventListener("change", saveFormState);
        }
      }
    }

    function updatePairRows() {
      for (let i = 1; i <= 4; i++) {
        const row = document.getElementById(`pairRow${i}`);
        if (!row) continue;
        row.style.display = i <= pairRowsVisible ? "grid" : "none";
      }
      const removeBtn = document.getElementById("removePairBtn");
      if (removeBtn) removeBtn.disabled = pairRowsVisible <= 1;
    }

    function addPairRow() {
      if (pairRowsVisible < 4) {
        pairRowsVisible += 1;
        updatePairRows();
        saveFormState();
      }
    }

    function removePairRow() {
      if (pairRowsVisible <= 1) return;
      for (const side of ["a", "b"]) {
        const el = document.getElementById(`pair${pairRowsVisible}${side}`);
        if (el) {
          el.value = "";
          el.classList.remove("invalid-input");
        }
      }
      pairRowsVisible -= 1;
      updatePairRows();
      saveFormState();
    }

    function openTokenList(inputId) {
      const el = document.getElementById(inputId);
      if (!el) return;
      el.value = "";
      try {
        if (typeof el.showPicker === "function") {
          el.showPicker();
          return;
        }
      } catch (_) {}
      el.focus();
      el.dispatchEvent(new KeyboardEvent("keydown", {key: "ArrowDown"}));
      saveFormState();
    }

    function clearPairErrors() {
      for (let i = 1; i <= 4; i++) {
        for (const side of ["a", "b"]) {
          const el = document.getElementById(`pair${i}${side}`);
          if (el) el.classList.remove("invalid-input");
        }
      }
    }

    function validatePairs() {
      clearPairErrors();
      const out = [];
      let hasError = false;
      for (let i = 1; i <= 4; i++) {
        const aEl = document.getElementById(`pair${i}a`);
        const bEl = document.getElementById(`pair${i}b`);
        const a = (aEl?.value || "").trim().toLowerCase();
        const b = (bEl?.value || "").trim().toLowerCase();
        if (!a && !b) continue;
        if (!a || !b || a === b) {
          if (aEl) aEl.classList.add("invalid-input");
          if (bEl) bEl.classList.add("invalid-input");
          hasError = true;
          continue;
        }
        out.push(`${a},${b}`);
      }
      const unique = Array.from(new Set(out));
      return {pairs: unique, valid: !hasError && unique.length > 0};
    }

    function getSelectedPairs() {
      return validatePairs().pairs;
    }

    function getSelectedProtocols() {
      const out = [];
      if (document.getElementById("protoV3")?.checked) out.push("v3");
      if (document.getElementById("protoV4")?.checked) out.push("v4");
      return out;
    }

    function getSelectedChains() {
      const allEl = document.getElementById("allChains");
      if (!allEl) return [];
      if (allEl.checked) return [];
      const out = [];
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el && el.checked) out.push(c);
      }
      return out;
    }

    function toggleAllChains() {
      const allEl = document.getElementById("allChains");
      if (!allEl) return;
      const all = allEl.checked;
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el) el.checked = all;
      }
      saveFormState();
    }

    function onChainToggle() {
      let checkedCount = 0;
      for (const c of availableChains) {
        const el = document.getElementById("chain_" + c);
        if (el && el.checked) checkedCount += 1;
      }
      const allEl = document.getElementById("allChains");
      if (!allEl) return;
      allEl.checked = checkedCount === availableChains.length && availableChains.length > 0;
      saveFormState();
    }

    function togglePoolVisibility(el) {
      const poolId = el?.dataset?.poolId;
      if (!poolId) return;
      visibilityMap[poolId] = !!el.checked;
      redrawCharts();
    }

    function redrawCharts() {
      const palette = ["#1e3a8a", "#155e75", "#14532d", "#7e22ce", "#7f1d1d", "#1d4ed8", "#0e7490", "#166534", "#6d28d9", "#be123c"];
      const dashes = ["dash", "dot", "dashdot", "longdash", "longdashdot"];
      const feeTraces = [];
      const tvlTraces = [];
      let maxTs = 0;

      const pools = Object.keys(seriesByPool);
      for (let i = 0; i < pools.length; i++) {
        const poolId = pools[i];
        const s = seriesByPool[poolId];
        if (!visibilityMap[poolId]) continue;
        const feeX = (s.fees || []).map(p => new Date(p[0] * 1000));
        const feeY = (s.fees || []).map(p => p[1]);
        const tvlX = (s.tvl || []).map(p => new Date(p[0] * 1000));
        const tvlY = (s.tvl || []).map(p => p[1] / 1000.0);
        const localMax = Math.max(
          ...((s.fees || []).map(p => Number(p[0] || 0))),
          ...((s.tvl || []).map(p => Number(p[0] || 0))),
          0
        );
        if (localMax > maxTs) maxTs = localMax;
        const c = colorMap[poolId] || palette[i % palette.length];
        const d = dashMap[poolId] || (i < palette.length ? "solid" : dashes[(i - palette.length) % dashes.length]);
        const hoverData = feeX.map(() => [s.chain || "", s.version || "", Number(s.fee_pct || 0).toFixed(2), s.pair || ""]);
        feeTraces.push({
          x: feeX, y: feeY, mode: "lines", name: s.label, customdata: hoverData,
          hovertemplate: "%{x|%b %d}<br>%{customdata[0]} %{customdata[1]} | %{customdata[2]}% | %{customdata[3]}<extra></extra>",
          line: {color: c, width: 2, dash: d}
        });
        tvlTraces.push({
          x: tvlX, y: tvlY, mode: "lines", name: s.label, customdata: hoverData,
          hovertemplate: "%{x|%b %d}<br>%{customdata[0]} %{customdata[1]} | %{customdata[2]}% | %{customdata[3]}<extra></extra>",
          line: {color: c, width: 2, dash: d}
        });
      }

      const alloc = Number(currentRequest?.lp_allocation_usd || 1000);
      const days = Number(currentRequest?.days || getDaysValue());
      const endDate = maxTs > 0 ? new Date(maxTs * 1000) : new Date();
      const startDate = new Date(endDate.getTime() - days * 24 * 3600 * 1000);
      Plotly.newPlot("feesChart", feeTraces, {
        title: `Cumulative Fees (LP allocation: $${formatUsd(alloc)})`,
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", automargin: true, range: [startDate, endDate]},
        yaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false}
      }, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", tvlTraces, {
        title: "TVL dynamics (thousands USD)",
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", automargin: true, range: [startDate, endDate]},
        yaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false}
      }, {displaylogo: false, responsive: true});
    }

    function renderTable(rows) {
      const table = document.getElementById("resultTable");
      const hdr = [
        ["color", ""], ["visibility", "Visibility"], ["chain", "Chain"], ["version", "Version"], ["pair", "Pair"], ["pool_id", "Pool ID"],
        ["fee_pct", "Fee %"], ["final_income", "Cumul $"], ["apy_pct", "APY"], ["last_tvl", "TVL"], ["status", "Status"]
      ];

      let html = "<tr>";
      for (const h of hdr) {
        const marker = sortKey === h[0] ? (sortDesc ? " ▼" : " ▲") : "";
        html += `<th onclick="sortBy('${h[0]}')">${h[1]}${marker}</th>`;
      }
      html += "</tr>";

      for (const r of rows) {
        const cls = r.status === "ok" ? "ok-row" : "error-row";
        const color = colorMap[r.pool_id] || "#94a3b8";
        const dash = dashMap[r.pool_id] || "solid";
        const cssDash = (dash === "solid") ? "solid" : (dash === "dot" ? "dotted" : "dashed");
        const visible = !!visibilityMap[r.pool_id];
        const hasSeries = !!seriesByPool[r.pool_id];
        const disabled = hasSeries ? "" : "disabled";
        const poolIdDisplay = (r.version === "v4" && (r.pool_id || "").length > 24)
          ? `${r.pool_id.slice(0, 12)}...${r.pool_id.slice(-8)}`
          : r.pool_id;
        html += `<tr class="${cls}">`;
        html += `<td><span class="line-swatch" style="border-top-color:${color};border-top-style:${cssDash};"></span></td>`;
        html += `<td><input type="checkbox" data-pool-id="${r.pool_id}" ${visible ? "checked" : ""} ${disabled} onchange="togglePoolVisibility(this)"/></td>`;
        html += `<td>${r.chain}</td>`;
        html += `<td>${r.version}</td>`;
        html += `<td>${r.pair}</td>`;
        html += `<td class="mono">${poolIdDisplay}</td>`;
        html += `<td>${Number(r.fee_pct).toFixed(2)}</td>`;
        html += `<td>$${formatUsd(r.final_income)}</td>`;
        html += `<td>${Number(r.apy_pct || 0).toFixed(1)}%</td>`;
        html += `<td>$${formatUsd(r.last_tvl)}</td>`;
        const statusLabel = r.status === "ok"
          ? "ok"
          : (r.status === "filtered_suffix" ? "excluded by suffix" : "filtered by fee range");
        html += `<td>${statusLabel}</td>`;
        html += "</tr>";
      }
      table.innerHTML = html;
    }

    function sortBy(key) {
      if (!SORTABLE[key]) return;
      if (sortKey === key) {
        sortDesc = !sortDesc;
      } else {
        sortKey = key;
        sortDesc = key === "final_income" || key === "apy_pct" || key === "last_tvl" || key === "fee_pct";
      }
      const fn = SORTABLE[sortKey];
      const sorted = [...lastRows].sort((a, b) => {
        const x = fn(a), y = fn(b);
        if (x < y) return sortDesc ? 1 : -1;
        if (x > y) return sortDesc ? -1 : 1;
        return 0;
      });
      renderedRows = sorted;
      renderTable(sorted);
    }

    function exportCsv() {
      const rows = renderedRows.length ? renderedRows : lastRows;
      if (!rows.length) {
        setStatus("No rows to export yet.", "fail");
        return;
      }
      const headers = ["visibility", "chain", "version", "pair", "pool_id", "fee_pct", "final_income", "apy_pct", "last_tvl", "status"];
      const lines = [headers.join(",")];
      for (const r of rows) {
        const vals = headers.map(h => {
          const rawVal = (h === "visibility") ? (!!visibilityMap[r.pool_id]) : r[h];
          const val = rawVal == null ? "" : String(rawVal);
          return `"${val.replace(/"/g, '""')}"`;
        });
        lines.push(vals.join(","));
      }
      const blob = new Blob([lines.join("\\n")], {type: "text/csv;charset=utf-8;"});
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "pools_analysis.csv";
      a.click();
      URL.revokeObjectURL(url);
    }

    async function loadMeta() {
      try {
        const r = await fetch("/api/meta");
        const meta = await r.json();
        const tokenHints = document.getElementById("tokenHints");
        tokenHints.innerHTML = (meta.tokens || []).map(t => `<option value="${t}"></option>`).join("");
        const minTvl = Number(meta.token_catalog?.min_tvl_usd || 10000);
        document.getElementById("tokensMeta").textContent = `popular tokens (TVL>${formatUsdShort(minTvl)}): ${meta.token_catalog?.count || 0}, updated: ${meta.token_catalog?.updated_at || "-"}`;

        availableChains = meta.chains || [];
        document.getElementById("chainsMeta").textContent = `chains: ${meta.chain_catalog?.count || 0}, updated: ${meta.chain_catalog?.updated_at || "-"}`;
        const checks = document.getElementById("chainChecks");
        checks.innerHTML = [
          `<label class="check"><input type="checkbox" id="allChains" checked onchange="toggleAllChains()"> all</label>`,
          ...availableChains.map(c => `<label class="check"><input type="checkbox" id="chain_${c}" checked onchange="onChainToggle()"> ${c}</label>`)
        ].join("");
        loadFormState();
        if (document.getElementById("allChains").checked) toggleAllChains();
        else onChainToggle();
      } catch (e) {
        console.warn("meta load failed", e);
      }
    }

    async function reviewTokens() {
      setStatus("Refreshing token catalog...", "running");
      const r = await fetch("/api/catalog/tokens/review", {method: "POST"});
      if (!r.ok) {
        setStatus("Token refresh failed", "fail");
        return;
      }
      await loadMeta();
      setStatus("Token catalog updated", "ok");
    }

    async function reviewChains() {
      setStatus("Refreshing chain catalog...", "running");
      const r = await fetch("/api/catalog/chains/review", {method: "POST"});
      if (!r.ok) {
        setStatus("Chain refresh failed", "fail");
        return;
      }
      await loadMeta();
      setStatus("Chain catalog updated", "ok");
    }

    async function runJob() {
      try {
        const pairCheck = validatePairs();
        const minTvlRaw = String(document.getElementById("minTvl").value ?? "").trim();
        const daysRaw = String(document.getElementById("days").value ?? "").trim();
        const maxFeeRaw = String(document.getElementById("maxFeePct").value ?? "").trim();
        const minFeeRaw = String(document.getElementById("minFeePct").value ?? "").trim();
        if (!minTvlRaw || !daysRaw || !minFeeRaw || !maxFeeRaw) {
          setStatus("Fill all filter fields before running analysis.", "fail");
          return;
        }
        const payload = {
          pairs: pairCheck.pairs,
          include_chains: getSelectedChains(),
          include_versions: getSelectedProtocols(),
          min_tvl: Number(minTvlRaw),
          days: Number(daysRaw),
          speed_mode: String(document.getElementById("speedMode")?.value || "normal").trim().toLowerCase(),
          max_fee_pct: Number(maxFeeRaw),
          min_fee_pct: Number(minFeeRaw),
        };
        if (!payload.include_versions.length) {
          setStatus("Select at least one protocol (V3/V4).", "fail");
          return;
        }
        if (!Number.isInteger(payload.days) || payload.days < 1 || payload.days > 3650) {
          setStatus("History days must be an integer in range 1..3650.", "fail");
          return;
        }
        if (!Number.isFinite(payload.min_tvl) || payload.min_tvl < 0 || payload.min_tvl > 10000000) {
          setStatus("Min TVL must be in range 0..10000000.", "fail");
          return;
        }
        if (!Number.isFinite(payload.min_fee_pct) || payload.min_fee_pct < 0 || payload.min_fee_pct > 1) {
          setStatus("Exclude below must be in range 0..1.", "fail");
          return;
        }
        if (!Number.isFinite(payload.max_fee_pct) || payload.max_fee_pct < 1 || payload.max_fee_pct > 3) {
          setStatus("Exclude above must be in range 1..3.", "fail");
          return;
        }
        if (payload.min_fee_pct >= payload.max_fee_pct) {
          setStatus("Exclude below must be lower than Exclude above.", "fail");
          return;
        }
        if (!["normal", "fast"].includes(payload.speed_mode)) {
          setStatus("Speed mode must be Normal or Fast.", "fail");
          return;
        }
        if (!pairCheck.valid) {
          setStatus("Invalid pairs: fill both tokens and avoid duplicates in a pair.", "fail");
          return;
        }

        saveFormState();
        setBusy(true);
        setStatus("Starting...", "running");
        updateProgress(2, "Submitting job");
        const r = await fetch("/api/pools/run", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify(payload)
        });
        const data = await r.json();
        if (!r.ok) {
          setBusy(false);
          setStatus("Error: " + (data.detail || "request failed"), "fail");
          return;
        }
        pollJob(data.job_id);
      } catch (e) {
        setBusy(false);
        setStatus("Frontend error: " + (e?.message || "unknown"), "fail");
      }
    }

    async function pollJob(jobId) {
      const timer = setInterval(async () => {
        const r = await fetch("/api/jobs/" + jobId);
        const job = await r.json();
        updateProgress(job.progress, job.stage_label || job.stage);
        if (job.status === "done") {
          clearInterval(timer);
          hasScanRun = true;
          setBusy(false);
          setStatus("Completed", "ok");
          renderResult(job.result);
        } else if (job.status === "failed") {
          clearInterval(timer);
          hasScanRun = true;
          setBusy(false);
          setStatus("Failed: " + (job.error || "unknown"), "fail");
        } else {
          setStatus(job.stage_label || job.status, "running");
        }
      }, 2000);
    }

    function renderResult(result) {
      const mSuffix = document.getElementById("mSuffix");
      const mTotal = document.getElementById("mTotal");
      const mChart = document.getElementById("mChart");
      const mErr = document.getElementById("mErr");
      if (mSuffix) mSuffix.textContent = result.suffix;
      if (mTotal) mTotal.textContent = result.total;
      if (mChart) mChart.textContent = result.chart_pools;
      if (mErr) mErr.textContent = result.error_pools;

      currentRequest = result?.request || {};
      colorMap = {};
      dashMap = {};
      visibilityMap = {};
      seriesByPool = {};
      const palette = ["#1e3a8a", "#155e75", "#14532d", "#7e22ce", "#7f1d1d", "#1d4ed8", "#0e7490", "#166534", "#6d28d9", "#be123c"];
      const dashes = ["dash", "dot", "dashdot", "longdash", "longdashdot"];
      for (let i = 0; i < (result.series || []).length; i++) {
        const s = result.series[i];
        const c = palette[i % palette.length];
        const d = i < palette.length ? "solid" : dashes[(i - palette.length) % dashes.length];
        colorMap[s.pool_id] = c;
        dashMap[s.pool_id] = d;
        seriesByPool[s.pool_id] = s;
        visibilityMap[s.pool_id] = (s.status === "ok");
      }

      const daysForApy = Number(result?.request?.days || getDaysValue());
      lastRows = (result.rows || []).map((r) => {
        const income = Number(r.final_income || 0);
        const alloc = Number(currentRequest?.lp_allocation_usd || 1000);
        const apyPct = (alloc > 0 && daysForApy > 0) ? (income / alloc) * (365 / daysForApy) * 100 : 0;
        return {...r, apy_pct: apyPct};
      });
      redrawCharts();
      sortKey = "final_income";
      sortDesc = true;
      sortBy("final_income");
      saveResultState(result);
    }

    function renderEmptyCharts() {
      const now = new Date();
      const start = new Date(now.getTime() - getDaysValue() * 24 * 3600 * 1000);
      const baseline = [{x: [start, now], y: [0, 0], mode: "lines", line: {color: "rgba(0,0,0,0)", width: 1}, hoverinfo: "skip", showlegend: false}];
      const emptyLayout = {
        paper_bgcolor: "#ffffff",
        plot_bgcolor: "#f8fbff",
        font: {color: "#0f172a"},
        showlegend: false,
        margin: {t: 30, b: 42, l: 50, r: 14},
        xaxis: {showgrid: true, gridcolor: "#d9e2f0", nticks: 18, tickformat: "%b %d", range: [start, now], automargin: true},
        yaxis: {title: "Value", showgrid: true, gridcolor: "#d9e2f0", nticks: 12, zeroline: false, range: [0, 1]},
        annotations: [{text: "Scan to load data", x: 0.5, y: 0.5, xref: "paper", yref: "paper", showarrow: false, font: {color: "#64748b"}}],
      };
      Plotly.newPlot("feesChart", baseline, {title: "Cumulative Fees", ...emptyLayout, yaxis: {...emptyLayout.yaxis, title: "Cumulative fee (USD)"}}, {displaylogo: false, responsive: true});
      Plotly.newPlot("tvlChart", baseline, {title: "TVL dynamics (thousands USD)", ...emptyLayout, yaxis: {...emptyLayout.yaxis, title: "TVL (k USD)"}}, {displaylogo: false, responsive: true});
    }

    attachAutosave();
    updatePairRows();
    renderEmptyCharts();
    loadAuthState();
    refreshIntentMenu();
    loadMeta().then(() => {
      const cached = loadResultState();
      if (cached) {
        hasScanRun = true;
        setBusy(false);
        renderResult(cached);
        setStatus("Restored last result", "ok");
      }
    });
  </script>
</body>
</html>
"""
