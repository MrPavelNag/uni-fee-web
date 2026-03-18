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
from queue import Empty, Queue
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
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
    _start_positions_index_workers()
    _start_infinity_indexer_daily()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    _stop_catalog_auto_refresh()
    _stop_positions_index_workers()
    _stop_infinity_indexer_daily()
    _stop_analytics()

# Simple in-memory job storage (MVP)
JOBS: dict[str, dict[str, Any]] = {}
JOB_LOCK = threading.Lock()
RUN_LOCK = threading.Lock()  # prevent collisions in shared data/*.json files
INDEXER_LOCK = threading.Lock()
INDEXER_ACTIVITY_LOCK = threading.Lock()
INDEXER_ACTIVITY: dict[str, Any] = {
    "running": False,
    "name": "infinity_bsc",
    "started_at": 0.0,
    "processed": 0,
    "targets": 0,
    "updated": 0,
    "errors": 0,
    "current_owner": "",
    "current_chain_id": 0,
    "last_event": "",
    "last_error": "",
    "updated_at": 0.0,
}
POS_JOBS: dict[str, dict[str, Any]] = {}
POS_JOB_LOCK = threading.Lock()
POS_JOB_TTL_SEC = 60 * 60
POSITIONS_INDEX_QUEUE: Queue[tuple[int, str]] = Queue()
POSITIONS_INDEX_INFLIGHT: set[tuple[int, str]] = set()
POSITIONS_INDEX_INFLIGHT_LOCK = threading.Lock()
POSITIONS_INDEX_STOP = threading.Event()
POSITIONS_INDEX_WORKERS: list[threading.Thread] = []
RUN_HISTORY: dict[str, list[dict[str, Any]]] = {}
RUN_HISTORY_LIMIT = 10
SESSION_COOKIE_NAME = "uni_fee_sid"
SESSION_TTL_SEC = int(os.environ.get("SESSION_TTL_SEC", str(30 * 24 * 60 * 60)))
CATALOG_REFRESH_INTERVAL_SEC = max(60, int(os.environ.get("CATALOG_REFRESH_INTERVAL_SEC", str(24 * 60 * 60))))
CATALOG_REFRESH_ON_STARTUP = os.environ.get("CATALOG_REFRESH_ON_STARTUP", "0").strip().lower() in ("1", "true", "yes", "on")
CATALOG_REFRESH_STOP = threading.Event()
CATALOG_REFRESH_THREAD: threading.Thread | None = None
INFINITY_INDEXER_DAILY_ENABLED = os.environ.get("INFINITY_INDEXER_DAILY_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
INFINITY_INDEXER_DAILY_INTERVAL_SEC = max(3600, int(os.environ.get("INFINITY_INDEXER_DAILY_INTERVAL_SEC", str(24 * 60 * 60))))
INFINITY_INDEXER_DAILY_ON_STARTUP = os.environ.get("INFINITY_INDEXER_DAILY_ON_STARTUP", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
INFINITY_INDEXER_DAILY_MAX_TARGETS = max(1, min(2000, int(os.environ.get("INFINITY_INDEXER_DAILY_MAX_TARGETS", "400"))))
INFINITY_INDEXER_DAILY_MAX_SECONDS = max(30, int(os.environ.get("INFINITY_INDEXER_DAILY_MAX_SECONDS", "900")))
INFINITY_INDEXER_DAILY_STOP = threading.Event()
INFINITY_INDEXER_DAILY_THREAD: threading.Thread | None = None
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
POSITIONS_SCAN_MAX_SECONDS_CONTRACT_ONLY = max(
    int(POSITIONS_SCAN_MAX_SECONDS),
    int(os.environ.get("POSITIONS_SCAN_MAX_SECONDS_CONTRACT_ONLY", "300")),
)
POSITIONS_PARALLEL_WORKERS = max(1, int(os.environ.get("POSITIONS_PARALLEL_WORKERS", "6")))
POSITIONS_ADDRESS_PARALLEL_WORKERS = max(1, int(os.environ.get("POSITIONS_ADDRESS_PARALLEL_WORKERS", "6")))
POSITIONS_NFT_PARALLEL_WORKERS = max(1, min(16, int(os.environ.get("POSITIONS_NFT_PARALLEL_WORKERS", "8"))))
POSITIONS_CONTRACT_ONLY_ENABLED = os.environ.get("POSITIONS_CONTRACT_ONLY_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_DISABLE_PARALLELISM = os.environ.get("POSITIONS_DISABLE_PARALLELISM", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_FAST_REMAINING_BUDGET_SEC = max(4, int(os.environ.get("POSITIONS_FAST_REMAINING_BUDGET_SEC", "16")))
POSITIONS_FAST_PER_CHAIN_TIMEOUT_SEC = max(1, int(os.environ.get("POSITIONS_FAST_PER_CHAIN_TIMEOUT_SEC", "2")))
POSITIONS_EXTENDED_QUERY_FALLBACK = os.environ.get("POSITIONS_EXTENDED_QUERY_FALLBACK", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_TRY_BYTES_TYPE = os.environ.get("POSITIONS_TRY_BYTES_TYPE", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_ONCHAIN_TIMEOUT_SEC = max(2, int(os.environ.get("POSITIONS_ONCHAIN_TIMEOUT_SEC", "4")))
POSITIONS_ONCHAIN_MAX_NFTS = max(1, int(os.environ.get("POSITIONS_ONCHAIN_MAX_NFTS", "120")))
POSITIONS_INFINITY_OWNER_LOOKBACK = max(200, int(os.environ.get("POSITIONS_INFINITY_OWNER_LOOKBACK", "800")))
POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS = max(20000, int(os.environ.get("POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS", "2500000")))
POSITIONS_ERC721_LOG_BLOCK_STEP = max(5000, int(os.environ.get("POSITIONS_ERC721_LOG_BLOCK_STEP", "150000")))
POSITIONS_INFINITY_OWNER_SCAN_MAX_CHECKS = max(20, int(os.environ.get("POSITIONS_INFINITY_OWNER_SCAN_MAX_CHECKS", "120")))
POSITIONS_INFINITY_OWNER_SCAN_MAX_ERRORS = max(10, int(os.environ.get("POSITIONS_INFINITY_OWNER_SCAN_MAX_ERRORS", "60")))
POSITIONS_ENABLE_INFINITY = os.environ.get("POSITIONS_ENABLE_INFINITY", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_INFINITY_HEAVY_METHODS = os.environ.get("POSITIONS_INFINITY_HEAVY_METHODS", "0").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_INFINITY_BATCH_SCAN = os.environ.get("POSITIONS_INFINITY_BATCH_SCAN", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_INFINITY_BATCH_SIZE = max(50, min(1000, int(os.environ.get("POSITIONS_INFINITY_BATCH_SIZE", "1000"))))
POSITIONS_INFINITY_BATCH_MAX_CHECKS = max(1000, int(os.environ.get("POSITIONS_INFINITY_BATCH_MAX_CHECKS", "800000")))
POSITIONS_INFINITY_BATCH_WORKERS = max(1, min(8, int(os.environ.get("POSITIONS_INFINITY_BATCH_WORKERS", "4"))))
POSITIONS_RPC_BATCH_MAX_ITEMS = max(10, min(200, int(os.environ.get("POSITIONS_RPC_BATCH_MAX_ITEMS", "80"))))
POSITIONS_INFINITY_DEEP_OWNER_SCAN_FALLBACK = os.environ.get("POSITIONS_INFINITY_DEEP_OWNER_SCAN_FALLBACK", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_SKIP_CHAINS_WITHOUT_NFTS = os.environ.get("POSITIONS_SKIP_CHAINS_WITHOUT_NFTS", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_STRICT_ZERO_BALANCE_FILTER = os.environ.get("POSITIONS_STRICT_ZERO_BALANCE_FILTER", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_LIGHT_GRAPH_QUERIES = os.environ.get("POSITIONS_LIGHT_GRAPH_QUERIES", "1").strip().lower() in ("1", "true", "yes", "on")
POSITIONS_CREATION_DATE_WORKERS = max(1, min(16, int(os.environ.get("POSITIONS_CREATION_DATE_WORKERS", "6"))))
POSITIONS_CREATION_DATE_MAX_SECONDS = max(1, int(os.environ.get("POSITIONS_CREATION_DATE_MAX_SECONDS", "20")))
POSITIONS_DISABLE_V3_ONCHAIN_FALLBACK = os.environ.get("POSITIONS_DISABLE_V3_ONCHAIN_FALLBACK", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_SKIP_PER_ID_DETAIL_FETCH = os.environ.get("POSITIONS_SKIP_PER_ID_DETAIL_FETCH", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
INFINITY_INDEXER_ENABLED_DEFAULT = os.environ.get("INFINITY_INDEXER_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on")
INFINITY_INDEXER_MODE_DEFAULT = os.environ.get("INFINITY_INDEXER_MODE", "auto").strip().lower() or "auto"
INFINITY_INDEXER_MAX_RECEIPTS = max(20, int(os.environ.get("INFINITY_INDEXER_MAX_RECEIPTS", "220")))
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


def _parse_csv_int_set(raw: str) -> set[int]:
    out: set[int] = set()
    for part in str(raw or "").split(","):
        s = str(part).strip()
        if not s:
            continue
        try:
            v = int(s)
            if v > 0:
                out.add(v)
        except Exception:
            continue
    return out


POSITIONS_NOT_SPAM_POSITION_IDS = _parse_csv_int_set(os.environ.get("POSITIONS_NOT_SPAM_IDS", "1227707,1011627"))
POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS = {
    int(x.strip())
    for x in os.environ.get("POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS", "42161,8453,130,56").split(",")
    if x.strip().isdigit()
}
POSITIONS_DISABLE_V3_PREFETCH = os.environ.get("POSITIONS_DISABLE_V3_PREFETCH", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_OWNERSHIP_INDEX_ENABLED = os.environ.get("POSITIONS_OWNERSHIP_INDEX_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_OWNERSHIP_INDEX_WORKERS = max(1, min(8, int(os.environ.get("POSITIONS_OWNERSHIP_INDEX_WORKERS", "2"))))
POSITIONS_OWNERSHIP_INDEX_MAX_NFTS = max(20, min(600, int(os.environ.get("POSITIONS_OWNERSHIP_INDEX_MAX_NFTS", "240"))))
POSITIONS_INDEX_FIRST_STRICT = os.environ.get("POSITIONS_INDEX_FIRST_STRICT", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_DIRECT_INCLUDE_CREATION_DATES = os.environ.get("POSITIONS_DIRECT_INCLUDE_CREATION_DATES", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_INDEX_SYNC_WARMUP_ENABLED = os.environ.get("POSITIONS_INDEX_SYNC_WARMUP_ENABLED", "1").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_INDEX_SYNC_WARMUP_MAX_RECEIPTS = max(20, int(os.environ.get("POSITIONS_INDEX_SYNC_WARMUP_MAX_RECEIPTS", "80")))
POSITIONS_INDEX_SYNC_WARMUP_V4_DEADLINE_SEC = max(
    2,
    int(os.environ.get("POSITIONS_INDEX_SYNC_WARMUP_V4_DEADLINE_SEC", "6")),
)
_POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS_RAW = os.environ.get("POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS", "56,8453")
POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS: set[int] = {
    int(x.strip())
    for x in str(_POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS_RAW or "").split(",")
    if str(x).strip().isdigit()
}
if not POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS:
    POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS = {56, 8453}
POSITIONS_INDEX_MISS_GRAPH_FALLBACK_ENABLED = os.environ.get(
    "POSITIONS_INDEX_MISS_GRAPH_FALLBACK_ENABLED",
    "1",
).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
POSITIONS_INDEX_MISS_GRAPH_FALLBACK_DEADLINE_SEC = max(
    2,
    int(os.environ.get("POSITIONS_INDEX_MISS_GRAPH_FALLBACK_DEADLINE_SEC", "2")),
)
POSITIONS_LEGACY_DISCOVERY_ENABLED = os.environ.get("POSITIONS_LEGACY_DISCOVERY_ENABLED", "0").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)
PRICE_CACHE_TTL_SEC = max(60, int(os.environ.get("PRICE_CACHE_TTL_SEC", "600")))
TOKEN_PRICE_CACHE: dict[tuple[int, str], tuple[float, float]] = {}
TOKEN_PRICE_CACHE_LOCK = threading.Lock()
TOKEN_SYMBOL_CACHE: dict[tuple[int, str], str] = {}
TOKEN_SYMBOL_CACHE_LOCK = threading.Lock()
CONTRACT_CREATION_DATE_CACHE: dict[tuple[int, str], str] = {}
CONTRACT_CREATION_DATE_CACHE_LOCK = threading.Lock()
POSITION_CREATION_DATE_CACHE: dict[tuple[int, str, int], str] = {}
POSITION_CREATION_DATE_CACHE_LOCK = threading.Lock()
POSITION_CONTRACT_SNAPSHOT_TTL_SEC = max(30, int(os.environ.get("POSITION_CONTRACT_SNAPSHOT_TTL_SEC", "600")))
POSITION_CONTRACT_SNAPSHOT_CACHE: dict[tuple[int, str, int], tuple[float, dict[str, Any]]] = {}
POSITION_CONTRACT_SNAPSHOT_CACHE_LOCK = threading.Lock()
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
INFINITY_CL_SUBGRAPH_BY_CHAIN_ID: dict[int, str] = {
    56: "https://api.thegraph.com/subgraphs/id/8jFYxwKP8tNGSDisucpHRK1ojUchZd7ELd8zh2ugHGDN",
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
UNISWAP_V4_POSITION_MANAGER_BY_CHAIN_ID: dict[int, str] = {
    # Uniswap v4 deployments: https://docs.uniswap.org/contracts/v4/deployments
    130: "0x4529a01c7a0410167c5740c487a8de60232617bf",  # Unichain
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS indexers (
              name TEXT PRIMARY KEY,
              enabled INTEGER NOT NULL DEFAULT 1,
              mode TEXT NOT NULL DEFAULT 'auto',
              max_receipts INTEGER NOT NULL DEFAULT 220,
              updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS indexer_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              name TEXT NOT NULL,
              ts TEXT NOT NULL,
              status TEXT NOT NULL,
              details TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_indexer_runs_name_ts ON indexer_runs(name, ts)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS infinity_positions_index (
              chain_id INTEGER NOT NULL,
              owner TEXT NOT NULL,
              token_id TEXT NOT NULL,
              source TEXT NOT NULL DEFAULT 'receipt',
              first_seen_ts TEXT NOT NULL,
              last_seen_ts TEXT NOT NULL,
              PRIMARY KEY(chain_id, owner, token_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inf_pos_owner_chain ON infinity_positions_index(owner, chain_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inf_pos_ts ON infinity_positions_index(last_seen_ts)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS position_ownership_index (
              chain_id INTEGER NOT NULL,
              owner TEXT NOT NULL,
              protocol TEXT NOT NULL,
              manager TEXT NOT NULL DEFAULT '',
              token_id TEXT NOT NULL,
              source TEXT NOT NULL DEFAULT '',
              first_seen_ts TEXT NOT NULL,
              last_seen_ts TEXT NOT NULL,
              PRIMARY KEY(chain_id, owner, protocol, token_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS position_details_cache (
              chain_id INTEGER NOT NULL,
              protocol TEXT NOT NULL,
              token_id TEXT NOT NULL,
              payload_json TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(chain_id, protocol, token_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pos_own_owner_chain ON position_ownership_index(owner, chain_id, protocol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pos_own_seen ON position_ownership_index(last_seen_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pos_details_updated ON position_details_cache(updated_at)")
        # Ensure default infinity indexer config exists.
        conn.execute(
            """
            INSERT INTO indexers(name, enabled, mode, max_receipts, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(name) DO NOTHING
            """,
            (
                "infinity_bsc",
                1 if INFINITY_INDEXER_ENABLED_DEFAULT else 0,
                INFINITY_INDEXER_MODE_DEFAULT if INFINITY_INDEXER_MODE_DEFAULT in {"auto", "manual", "off"} else "auto",
                int(INFINITY_INDEXER_MAX_RECEIPTS),
                _iso_now(),
            ),
        )
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


def _parse_int_like(value: Any) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        return int(raw, 10)
    except Exception:
        pass
    try:
        return int(raw, 16) if raw.lower().startswith("0x") else int(raw)
    except Exception:
        return 0


def _indexer_get(name: str) -> dict[str, Any]:
    safe_name = str(name or "").strip()
    if not safe_name:
        return {}
    with _analytics_conn() as conn:
        row = conn.execute(
            "SELECT name, enabled, mode, max_receipts, updated_at FROM indexers WHERE name = ?",
            (safe_name,),
        ).fetchone()
    if not row:
        return {}
    return {
        "name": str(row[0]),
        "enabled": bool(int(row[1] or 0)),
        "mode": str(row[2] or "auto"),
        "max_receipts": int(row[3] or 220),
        "updated_at": str(row[4] or ""),
    }


def _indexer_upsert(name: str, *, enabled: bool, mode: str, max_receipts: int) -> dict[str, Any]:
    safe_name = str(name or "").strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="Indexer name is required.")
    safe_mode = str(mode or "auto").strip().lower()
    if safe_mode not in {"auto", "manual", "off"}:
        raise HTTPException(status_code=400, detail="mode must be auto, manual, or off.")
    safe_max_receipts = max(20, min(2000, int(max_receipts)))
    now = _iso_now()
    with _analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO indexers(name, enabled, mode, max_receipts, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(name) DO UPDATE SET
              enabled = excluded.enabled,
              mode = excluded.mode,
              max_receipts = excluded.max_receipts,
              updated_at = excluded.updated_at
            """,
            (safe_name, 1 if enabled else 0, safe_mode, safe_max_receipts, now),
        )
        conn.commit()
    return _indexer_get(safe_name)


def _indexer_log_run(name: str, status: str, details: str = "") -> None:
    with _analytics_conn() as conn:
        conn.execute(
            "INSERT INTO indexer_runs(name, ts, status, details) VALUES(?, ?, ?, ?)",
            (str(name or "").strip(), _iso_now(), str(status or "ok")[:32], str(details or "")[:5000]),
        )
        conn.commit()


def _indexer_activity_snapshot() -> dict[str, Any]:
    with INDEXER_ACTIVITY_LOCK:
        return dict(INDEXER_ACTIVITY)


def _indexer_activity_start(name: str, targets: int) -> None:
    with INDEXER_ACTIVITY_LOCK:
        INDEXER_ACTIVITY.clear()
        INDEXER_ACTIVITY.update(
            {
                "running": True,
                "name": str(name or "infinity_bsc"),
                "started_at": time.time(),
                "processed": 0,
                "targets": max(0, int(targets)),
                "updated": 0,
                "errors": 0,
                "current_owner": "",
                "current_chain_id": 0,
                "last_event": "started",
                "last_error": "",
                "updated_at": time.time(),
            }
        )


def _indexer_activity_tick(chain_id: int, owner: str, *, updated_inc: int = 0, error_inc: int = 0) -> None:
    with INDEXER_ACTIVITY_LOCK:
        INDEXER_ACTIVITY["processed"] = int(INDEXER_ACTIVITY.get("processed") or 0) + 1
        INDEXER_ACTIVITY["updated"] = int(INDEXER_ACTIVITY.get("updated") or 0) + max(0, int(updated_inc))
        INDEXER_ACTIVITY["errors"] = int(INDEXER_ACTIVITY.get("errors") or 0) + max(0, int(error_inc))
        INDEXER_ACTIVITY["current_chain_id"] = int(chain_id)
        INDEXER_ACTIVITY["current_owner"] = str(owner or "").strip().lower()
        INDEXER_ACTIVITY["last_event"] = "tick"
        if int(error_inc) > 0:
            INDEXER_ACTIVITY["last_error"] = "owner update failed"
        INDEXER_ACTIVITY["updated_at"] = time.time()


def _indexer_activity_stop(event: str = "stopped", error: str = "") -> None:
    with INDEXER_ACTIVITY_LOCK:
        INDEXER_ACTIVITY["running"] = False
        INDEXER_ACTIVITY["current_owner"] = ""
        INDEXER_ACTIVITY["current_chain_id"] = 0
        INDEXER_ACTIVITY["last_event"] = str(event or "stopped")
        if error:
            INDEXER_ACTIVITY["last_error"] = str(error)[:220]
        INDEXER_ACTIVITY["updated_at"] = time.time()


def _infinity_index_upsert(chain_id: int, owner: str, token_ids: list[int], source: str) -> int:
    if not token_ids:
        return 0
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return 0
    now = _iso_now()
    rows: list[tuple[Any, ...]] = []
    for tid in token_ids:
        token_id = _parse_int_like(tid)
        if token_id <= 0:
            continue
        rows.append((cid, owner_addr, str(token_id), str(source or "receipt")[:24], now, now))
    if not rows:
        return 0
    with _analytics_conn() as conn:
        conn.executemany(
            """
            INSERT INTO infinity_positions_index(chain_id, owner, token_id, source, first_seen_ts, last_seen_ts)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain_id, owner, token_id) DO UPDATE SET
              source = excluded.source,
              last_seen_ts = excluded.last_seen_ts
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def _infinity_index_get_token_ids(chain_id: int, owner: str, limit: int = 180) -> list[int]:
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    lim = max(1, min(500, int(limit)))
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT token_id FROM infinity_positions_index
            WHERE chain_id = ? AND owner = ?
            ORDER BY last_seen_ts DESC
            LIMIT ?
            """,
            (cid, owner_addr, lim),
        ).fetchall()
    out: list[int] = []
    seen: set[int] = set()
    for r in rows:
        tid = _parse_int_like(r[0] if r else 0)
        if tid <= 0 or tid in seen:
            continue
        seen.add(tid)
        out.append(tid)
    return out


def _position_ownership_upsert(
    chain_id: int,
    owner: str,
    protocol: str,
    manager: str,
    token_ids: list[int | str],
    *,
    source: str = "onchain",
) -> int:
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    if cid <= 0 or not _is_eth_address(owner_addr):
        return 0
    proto = str(protocol or "").strip().lower()[:40]
    if not proto:
        return 0
    mgr = str(manager or "").strip().lower()
    now = _iso_now()
    rows: list[tuple[Any, ...]] = []
    for raw_tid in token_ids:
        tid = _parse_int_like(raw_tid)
        if tid <= 0:
            continue
        rows.append((cid, owner_addr, proto, mgr, str(tid), str(source or "onchain")[:24], now, now))
    if not rows:
        return 0
    with _analytics_conn() as conn:
        conn.executemany(
            """
            INSERT INTO position_ownership_index(chain_id, owner, protocol, manager, token_id, source, first_seen_ts, last_seen_ts)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chain_id, owner, protocol, token_id) DO UPDATE SET
              manager = excluded.manager,
              source = excluded.source,
              last_seen_ts = excluded.last_seen_ts
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def _position_details_cache_upsert(chain_id: int, protocol: str, position: dict[str, Any]) -> None:
    try:
        cid = int(chain_id)
    except Exception:
        return
    proto = str(protocol or "").strip().lower()[:40]
    tid = str((position or {}).get("id") or "").strip()
    if cid <= 0 or not proto or not tid:
        return
    payload = dict(position or {})
    # Runtime-only fields should not be persisted in cache.
    for k in ("created", "current_tvl_usd", "valuation_mode", "token0_symbol", "token1_symbol"):
        if k in payload:
            payload.pop(k, None)
    now = _iso_now()
    with _analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO position_details_cache(chain_id, protocol, token_id, payload_json, updated_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(chain_id, protocol, token_id) DO UPDATE SET
              payload_json = excluded.payload_json,
              updated_at = excluded.updated_at
            """,
            (cid, proto, tid, json.dumps(payload, ensure_ascii=True), now),
        )
        conn.commit()


def _position_cached_rows_for_owner(
    chain_id: int,
    owner: str,
    protocol: str,
    *,
    limit: int = 240,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    proto = str(protocol or "").strip().lower()
    if cid <= 0 or not _is_eth_address(owner_addr) or not proto:
        return []
    lim = max(1, min(800, int(limit)))
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT o.token_id, d.payload_json, o.last_seen_ts
            FROM position_ownership_index o
            LEFT JOIN position_details_cache d
              ON d.chain_id = o.chain_id AND d.protocol = o.protocol AND d.token_id = o.token_id
            WHERE o.chain_id = ? AND o.owner = ? AND o.protocol = ?
            ORDER BY o.last_seen_ts DESC
            LIMIT ?
            """,
            (cid, owner_addr, proto, lim),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        try:
            token_id = str(r[0] or "").strip()
            payload = str(r[1] or "").strip()
            if not token_id or not payload:
                continue
            obj = json.loads(payload)
            if not isinstance(obj, dict):
                continue
            obj["id"] = token_id
            obj["_source"] = "ownership_cache"
            obj["_protocol_label"] = str(obj.get("_protocol_label") or proto)
            out.append(obj)
        except Exception:
            continue
    return out


def _position_enqueue_ownership_refresh(chain_id: int, owner: str) -> bool:
    if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
        return False
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    if cid <= 0 or not _is_eth_address(owner_addr):
        return False
    key = (cid, owner_addr)
    with POSITIONS_INDEX_INFLIGHT_LOCK:
        if key in POSITIONS_INDEX_INFLIGHT:
            return False
        POSITIONS_INDEX_INFLIGHT.add(key)
    POSITIONS_INDEX_QUEUE.put(key)
    return True


def _position_index_refresh_owner_chain(
    chain_id: int,
    owner: str,
    *,
    target_version: str = "all",
    warmup_mode: bool = False,
    deep_infinity_scan: bool = False,
) -> dict[str, int]:
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    summary = {"cached": 0, "ownership_upserted": 0}
    if cid <= 0 or not _is_eth_address(owner_addr):
        return summary
    mode = str(target_version or "all").strip().lower()
    if mode not in {"all", "v3", "v4"}:
        mode = "all"
    run_v3 = mode in {"all", "v3"}
    run_v4 = mode in {"all", "v4"}
    chain_key = CHAIN_ID_TO_KEY.get(cid, "")
    try:
        protocol_label = str(V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(cid, "uniswap_v3"))
        npm = UNISWAP_V3_NPM_BY_CHAIN_ID.get(cid, "")
        if run_v3 and npm:
            rows = _scan_v3_positions_onchain(
                owner_addr,
                cid,
                include_price_details=True,
                protocol_label=protocol_label,
                source_tag="ownership_index",
            )
            token_ids: list[int] = []
            for pos in rows:
                tid = _parse_int_like((pos or {}).get("id"))
                if tid <= 0:
                    continue
                token_ids.append(tid)
                _position_details_cache_upsert(cid, protocol_label, pos)
                summary["cached"] += 1
            summary["ownership_upserted"] += _position_ownership_upsert(
                cid,
                owner_addr,
                protocol_label,
                npm,
                token_ids,
                source="onchain_npm",
            )
        mc = PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID.get(cid, "")
        if run_v3 and mc:
            staked = _scan_pancake_staked_v3_positions_onchain(owner_addr, cid)
            staked_ids: list[int] = []
            for pos in staked:
                tid = _parse_int_like((pos or {}).get("id"))
                if tid <= 0:
                    continue
                staked_ids.append(tid)
                _position_details_cache_upsert(cid, "pancake_v3_staked", pos)
                summary["cached"] += 1
            summary["ownership_upserted"] += _position_ownership_upsert(
                cid,
                owner_addr,
                "pancake_v3_staked",
                mc,
                staked_ids,
                source="onchain_masterchef",
            )
        # Infinity CL ids are maintained by the dedicated indexer. Mirror them into the unified ownership index.
        inf_mgr = PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid, "")
        if run_v3 and inf_mgr:
            idx_cfg = _indexer_get("infinity_bsc")
            idx_enabled = bool(idx_cfg.get("enabled")) and str(idx_cfg.get("mode") or "off") != "off"
            if idx_enabled and str(idx_cfg.get("mode") or "auto") == "auto":
                try:
                    max_receipts = int(idx_cfg.get("max_receipts") or INFINITY_INDEXER_MAX_RECEIPTS)
                    if warmup_mode:
                        max_receipts = min(max_receipts, int(POSITIONS_INDEX_SYNC_WARMUP_MAX_RECEIPTS))
                    run_stats = _update_infinity_index_for_owner(
                        cid,
                        owner_addr,
                        max_receipts=max_receipts,
                        deep_infinity_scan=deep_infinity_scan,
                    )
                    summary["cached"] += int(run_stats.get("merged_ids") or 0)
                except Exception:
                    pass
            inf_ids = _infinity_index_get_token_ids(cid, owner_addr, limit=POSITIONS_OWNERSHIP_INDEX_MAX_NFTS)
            summary["ownership_upserted"] += _position_ownership_upsert(
                cid,
                owner_addr,
                "pancake_infinity_cl",
                inf_mgr,
                inf_ids,
                source="infinity_index",
            )
            if inf_ids:
                try:
                    inf_cl_positions = _scan_pancake_infinity_cl_positions_onchain(
                        owner_addr,
                        cid,
                        deadline_ts=time.monotonic() + (8.0 if warmup_mode else 20.0),
                        debug_out={},
                        token_ids_override=inf_ids,
                    )
                except Exception:
                    inf_cl_positions = []
                inf_cl_ids: list[int] = []
                for pos in inf_cl_positions:
                    tid = _parse_int_like((pos or {}).get("id"))
                    if tid <= 0:
                        continue
                    inf_cl_ids.append(tid)
                    _position_details_cache_upsert(cid, "pancake_infinity_cl", pos)
                    summary["cached"] += 1
                if inf_cl_ids:
                    summary["ownership_upserted"] += _position_ownership_upsert(
                        cid,
                        owner_addr,
                        "pancake_infinity_cl",
                        inf_mgr,
                        inf_cl_ids,
                        source="onchain_infinity_cl",
                    )
                # If token IDs are known but full position decoding fails, keep lightweight
                # cache stubs so strict index-first scans can still surface Infinity rows.
                if not inf_cl_ids and inf_ids:
                    for tid in inf_ids:
                        tid_int = _parse_int_like(tid)
                        if tid_int <= 0:
                            continue
                        stub = {
                            "id": str(tid_int),
                            "liquidity": "1",
                            "tickLower": {"tickIdx": "0"},
                            "tickUpper": {"tickIdx": "1"},
                            "pool": {
                                "id": "0x" + _encode_uint_word(tid_int),
                                "feeTier": "",
                                "liquidity": "0",
                                "sqrtPrice": "0",
                                "token0Price": "0",
                                "totalValueLockedUSD": "0",
                                "totalValueLockedToken0": "0",
                                "totalValueLockedToken1": "0",
                                "token0": {"id": "infinity", "decimals": "18", "symbol": "Infinity"},
                                "token1": {"id": f"position-{tid_int}", "decimals": "18", "symbol": f"#{tid_int}"},
                            },
                            "_protocol_label": "pancake_infinity_cl",
                            "_source": "ownership_index_stub",
                            "_skip_enrich": True,
                        }
                        _position_details_cache_upsert(cid, "pancake_infinity_cl", stub)
                        summary["cached"] += 1
        inf_bin_mgr = PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid, "")
        if run_v3 and inf_bin_mgr:
            try:
                bin_ids = _scan_erc721_token_ids_by_explorer_api(
                    cid,
                    inf_bin_mgr,
                    owner_addr,
                    max_ids=max(40, POSITIONS_OWNERSHIP_INDEX_MAX_NFTS // 2),
                    protocol="pancake_infinity_bin",
                )
            except Exception:
                bin_ids = []
            if bin_ids:
                summary["ownership_upserted"] += _position_ownership_upsert(
                    cid,
                    owner_addr,
                    "pancake_infinity_bin",
                    inf_bin_mgr,
                    bin_ids,
                    source="explorer_infinity_bin",
                )
                try:
                    inf_bin_positions = _scan_pancake_infinity_bin_positions_onchain(
                        owner_addr,
                        cid,
                        deadline_ts=time.monotonic() + (8.0 if warmup_mode else 20.0),
                        debug_out={},
                        token_ids_override=bin_ids,
                    )
                except Exception:
                    inf_bin_positions = []
                inf_bin_ids: list[int] = []
                for pos in inf_bin_positions:
                    tid = _parse_int_like((pos or {}).get("id"))
                    if tid <= 0:
                        continue
                    inf_bin_ids.append(tid)
                    _position_details_cache_upsert(cid, "pancake_infinity_bin", pos)
                    summary["cached"] += 1
                if inf_bin_ids:
                    summary["ownership_upserted"] += _position_ownership_upsert(
                        cid,
                        owner_addr,
                        "pancake_infinity_bin",
                        inf_bin_mgr,
                        inf_bin_ids,
                        source="onchain_infinity_bin",
                    )
        # V4 ownership/cache refresh runs from graph in light mode.
        if run_v4 and chain_key:
            ep_v4 = get_graph_endpoint(chain_key, version="v4")
            if ep_v4 and _endpoint_supports_uniswap_positions(ep_v4):
                v4_deadline = time.monotonic() + (
                    float(POSITIONS_INDEX_SYNC_WARMUP_V4_DEADLINE_SEC) if warmup_mode else 14.0
                )
                v4_rows = _query_uniswap_positions_for_owner(
                    ep_v4,
                    owner_addr,
                    include_pool_liquidity=False,
                    include_position_liquidity=True,
                    debug_steps=None,
                    deadline_ts=v4_deadline,
                    light_mode=True,
                )
                v4_ids: list[int] = []
                for pos in v4_rows:
                    tid = _parse_int_like((pos or {}).get("id"))
                    if tid <= 0:
                        continue
                    v4_ids.append(tid)
                    if isinstance(pos, dict):
                        pos["_protocol_label"] = "uniswap_v4"
                        pos["_source"] = str(pos.get("_source") or "ownership_index_graph")
                    _position_details_cache_upsert(cid, "uniswap_v4", pos)
                    summary["cached"] += 1
                if v4_ids:
                    summary["ownership_upserted"] += _position_ownership_upsert(
                        cid,
                        owner_addr,
                        "uniswap_v4",
                        "",
                        v4_ids,
                        source="graph_v4",
                    )
    except Exception:
        pass
    return summary


def _positions_index_worker_loop() -> None:
    while not POSITIONS_INDEX_STOP.is_set():
        try:
            cid, owner = POSITIONS_INDEX_QUEUE.get(timeout=1.0)
        except Empty:
            continue
        try:
            _position_index_refresh_owner_chain(int(cid), str(owner))
        except Exception:
            pass
        finally:
            with POSITIONS_INDEX_INFLIGHT_LOCK:
                POSITIONS_INDEX_INFLIGHT.discard((int(cid), str(owner).strip().lower()))
            POSITIONS_INDEX_QUEUE.task_done()


def _start_positions_index_workers() -> None:
    if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
        return
    if POSITIONS_INDEX_WORKERS:
        return
    POSITIONS_INDEX_STOP.clear()
    for idx in range(int(POSITIONS_OWNERSHIP_INDEX_WORKERS)):
        t = threading.Thread(target=_positions_index_worker_loop, name=f"positions-index-{idx+1}", daemon=True)
        t.start()
        POSITIONS_INDEX_WORKERS.append(t)


def _stop_positions_index_workers() -> None:
    POSITIONS_INDEX_STOP.set()
    for _ in POSITIONS_INDEX_WORKERS:
        POSITIONS_INDEX_QUEUE.put((0, ""))
    POSITIONS_INDEX_WORKERS.clear()


def _update_infinity_index_for_owner(
    chain_id: int,
    owner: str,
    max_receipts: int = 220,
    *,
    deep_infinity_scan: bool = False,
) -> dict[str, Any]:
    cid = int(chain_id)
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        raise HTTPException(status_code=400, detail="Invalid owner address.")
    pm = PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid)
    if not pm:
        raise HTTPException(status_code=400, detail=f"Infinity CL position manager is not configured for chain {cid}.")

    deadline_ts = time.monotonic() + 25.0
    explorer_ids = _scan_erc721_token_ids_by_explorer_api(
        cid,
        pm,
        owner_addr,
        max_ids=240,
        protocol="pancake_infinity_cl",
    )
    receipt_ids, receipt_checked = _scan_cl_token_ids_from_owner_receipts(
        cid,
        pm,
        owner_addr,
        deadline_ts=deadline_ts,
        max_ids=240,
        max_receipts=max(20, min(2000, int(max_receipts))),
    )
    log_ids: list[int] = []
    if not explorer_ids and not receipt_ids:
        # Explorer APIs can miss historical NFT transfers; use direct chain logs as fallback.
        try:
            log_ids = _scan_erc721_token_ids_by_incoming_logs(
                cid,
                pm,
                owner_addr,
                deadline_ts=time.monotonic() + 8.0,
                max_ids=240,
                lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 8_000_000),
            )
        except Exception:
            log_ids = []
    onchain_ids: list[int] = []
    if not explorer_ids and not receipt_ids and not log_ids:
        # Final fallback: query CL positions directly and derive token ids from returned position objects.
        try:
            cl_positions = _scan_pancake_infinity_cl_positions_onchain(
                owner_addr,
                cid,
                deadline_ts=time.monotonic() + 12.0,
                debug_out={},
            )
            for p in cl_positions:
                pid = str((p or {}).get("id") or "")
                if pid.startswith("inf-cl:"):
                    tid = _parse_int_like(pid.split(":", 1)[1])
                else:
                    tid = _parse_int_like(pid)
                if tid > 0:
                    onchain_ids.append(int(tid))
        except Exception:
            onchain_ids = []
    owner_scan_ids: list[int] = []
    deep_scan_enabled = bool(POSITIONS_INFINITY_DEEP_OWNER_SCAN_FALLBACK or deep_infinity_scan)
    if (
        deep_scan_enabled
        and not explorer_ids
        and not receipt_ids
        and not log_ids
        and not onchain_ids
        and int(max_receipts) >= 200
    ):
        # Full mode only: run the deep owner scanner (can be expensive) to recover ids
        # for non-enumerable PM contracts when explorer/receipt paths are empty.
        try:
            owner_scan_ids = _scan_infinity_position_ids_for_owner(
                pm,
                owner_addr,
                cid,
                deadline_ts=time.monotonic() + 90.0,
                debug_out={},
            )
        except Exception:
            owner_scan_ids = []
    merged_ids = sorted(
        {int(x) for x in explorer_ids + receipt_ids + log_ids + onchain_ids + owner_scan_ids if _parse_int_like(x) > 0},
        reverse=True,
    )
    source_tag = "logs" if log_ids else "receipt"
    if onchain_ids:
        source_tag = "onchain"
    elif owner_scan_ids:
        source_tag = "owner_scan"
    upserted = _infinity_index_upsert(cid, owner_addr, merged_ids, source_tag)
    indexed_now = _infinity_index_get_token_ids(cid, owner_addr, limit=400)
    return {
        "chain_id": cid,
        "owner": owner_addr,
        "position_manager": pm,
        "receipt_checked": int(receipt_checked),
        "explorer_ids": len(explorer_ids),
        "receipt_ids": len(receipt_ids),
        "log_ids": len(log_ids),
        "onchain_ids": len(onchain_ids),
        "owner_scan_ids": len(owner_scan_ids),
        "merged_ids": len(merged_ids),
        "upserted": int(upserted),
        "indexed_total_for_owner": len(indexed_now),
        "sample_token_ids": indexed_now[:10],
    }


def _indexer_summary(name: str = "infinity_bsc") -> dict[str, Any]:
    cfg = _indexer_get(name) or {
        "name": name,
        "enabled": False,
        "mode": "off",
        "max_receipts": int(INFINITY_INDEXER_MAX_RECEIPTS),
        "updated_at": "",
    }
    with _analytics_conn() as conn:
        total_rows = int(conn.execute("SELECT COUNT(*) FROM infinity_positions_index").fetchone()[0] or 0)
        owners = int(conn.execute("SELECT COUNT(DISTINCT owner) FROM infinity_positions_index").fetchone()[0] or 0)
        ownership_rows = int(conn.execute("SELECT COUNT(*) FROM position_ownership_index").fetchone()[0] or 0)
        ownership_owners = int(conn.execute("SELECT COUNT(DISTINCT owner) FROM position_ownership_index").fetchone()[0] or 0)
        details_rows = int(conn.execute("SELECT COUNT(*) FROM position_details_cache").fetchone()[0] or 0)
        protocol_rows = conn.execute(
            """
            SELECT protocol, COUNT(*) AS cnt
            FROM position_ownership_index
            GROUP BY protocol
            ORDER BY cnt DESC
            LIMIT 6
            """
        ).fetchall()
        runs = conn.execute(
            "SELECT ts, status, details FROM indexer_runs WHERE name = ? ORDER BY id DESC LIMIT 1",
            (str(name),),
        ).fetchone()
    return {
        **cfg,
        "records_total": total_rows,
        "owners_total": owners,
        "last_run_at": str(runs[0]) if runs else "",
        "last_run_status": str(runs[1]) if runs else "",
        "last_run_details": str(runs[2]) if runs else "",
        "activity": _indexer_activity_snapshot(),
        "ownership_index": {
            "records_total": ownership_rows,
            "owners_total": ownership_owners,
            "details_cached_total": details_rows,
            "protocol_breakdown": [
                {"protocol": str(r[0] or ""), "count": int(r[1] or 0)}
                for r in (protocol_rows or [])
            ],
        },
        "ownership_index_queue": {
            "enabled": bool(POSITIONS_OWNERSHIP_INDEX_ENABLED),
            "index_first_strict": bool(POSITIONS_INDEX_FIRST_STRICT),
            "legacy_discovery_enabled": bool(POSITIONS_LEGACY_DISCOVERY_ENABLED),
            "workers": int(POSITIONS_OWNERSHIP_INDEX_WORKERS),
            "queued": int(POSITIONS_INDEX_QUEUE.qsize()),
            "inflight": len(POSITIONS_INDEX_INFLIGHT),
        },
    }


def _parse_owner_chain_from_run_details(details: str) -> tuple[int, str] | None:
    text = str(details or "")
    m_owner = re.search(r"owner=(0x[a-fA-F0-9]{40})", text)
    m_chain = re.search(r"chain=(\d+)", text)
    if not m_owner or not m_chain:
        return None
    owner = str(m_owner.group(1) or "").strip().lower()
    chain_id = _parse_int_like(m_chain.group(1))
    if chain_id <= 0 or not _is_eth_address(owner):
        return None
    return int(chain_id), owner


def _collect_infinity_indexer_targets(limit: int = 400) -> list[tuple[int, str]]:
    lim = max(1, min(2000, int(limit)))
    out: list[tuple[int, str]] = []
    seen: set[tuple[int, str]] = set()
    with _analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT chain_id, owner, MAX(last_seen_ts) AS last_seen
            FROM infinity_positions_index
            GROUP BY chain_id, owner
            ORDER BY last_seen DESC
            LIMIT ?
            """,
            (lim,),
        ).fetchall()
        for r in rows:
            try:
                cid = int(r[0] or 0)
            except Exception:
                cid = 0
            owner = str(r[1] or "").strip().lower()
            key = (cid, owner)
            if cid <= 0 or not _is_eth_address(owner) or key in seen:
                continue
            seen.add(key)
            out.append(key)
        run_rows = conn.execute(
            "SELECT details FROM indexer_runs WHERE name = ? ORDER BY id DESC LIMIT 1200",
            ("infinity_bsc",),
        ).fetchall()
        for rr in run_rows:
            parsed = _parse_owner_chain_from_run_details(rr[0] if rr else "")
            if not parsed:
                continue
            if parsed in seen:
                continue
            seen.add(parsed)
            out.append(parsed)
            if len(out) >= lim:
                break
    return out[:lim]


def _run_infinity_indexer_daily_once(max_targets: int | None = None) -> dict[str, Any]:
    cfg = _indexer_get("infinity_bsc")
    if not cfg:
        _indexer_activity_stop("daily-skip", "missing_config")
        return {"status": "skipped", "reason": "missing_config", "processed": 0, "updated": 0, "errors": 0}
    if not bool(cfg.get("enabled")) or str(cfg.get("mode") or "").strip().lower() == "off":
        _indexer_activity_stop("daily-skip", "disabled")
        return {"status": "skipped", "reason": "disabled", "processed": 0, "updated": 0, "errors": 0}

    if not INDEXER_LOCK.acquire(blocking=False):
        return {"status": "skipped", "reason": "busy", "processed": 0, "updated": 0, "errors": 0}

    try:
        targets = _collect_infinity_indexer_targets(
            limit=max_targets if max_targets is not None else INFINITY_INDEXER_DAILY_MAX_TARGETS
        )
        if not targets:
            _indexer_activity_stop("daily-skip", "no_targets")
            return {"status": "skipped", "reason": "no_targets", "processed": 0, "updated": 0, "errors": 0}
        _indexer_activity_start("infinity_bsc", len(targets))

        deadline_ts = time.monotonic() + float(INFINITY_INDEXER_DAILY_MAX_SECONDS)
        updated_total = 0
        processed = 0
        errors = 0
        first_error = ""
        receipts_cap = int(cfg.get("max_receipts") or INFINITY_INDEXER_MAX_RECEIPTS)
        for chain_id, owner in targets:
            if time.monotonic() >= deadline_ts:
                break
            processed += 1
            try:
                stats = _update_infinity_index_for_owner(chain_id, owner, max_receipts=receipts_cap)
                merged_now = int(stats.get("merged_ids") or 0)
                updated_total += merged_now
                _indexer_activity_tick(chain_id, owner, updated_inc=merged_now, error_inc=0)
            except Exception as e:
                errors += 1
                _indexer_activity_tick(chain_id, owner, updated_inc=0, error_inc=1)
                if not first_error:
                    first_error = str(e)[:220]

        timed_out = processed < len(targets)
        if errors == 0 and not timed_out:
            status = "ok"
        elif processed == 0 and errors > 0:
            status = "error"
        else:
            status = "partial"
        details = (
            f"daily processed={processed}/{len(targets)} updated={updated_total} errors={errors}"
            + (f" first_error={first_error}" if first_error else "")
            + (" timed_out=1" if timed_out else "")
        )
        _indexer_log_run("infinity_bsc", status, details)
        return {
            "status": status,
            "processed": processed,
            "targets": len(targets),
            "updated": updated_total,
            "errors": errors,
            "timed_out": timed_out,
        }
    finally:
        _indexer_activity_stop("daily-finished")
        INDEXER_LOCK.release()


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


def _method_selector(signature: str) -> str:
    sig = str(signature or "").strip()
    if not sig:
        return ""
    h = _keccak256_hex(sig.encode("utf-8"))
    if h and h.startswith("0x") and len(h) >= 10:
        return "0x" + h[2:10]
    # Fallback constants for common methods if keccak helper is unavailable.
    known = {
        "decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))": "0x0c49ccbe",
    }
    return known.get(sig, "")


def _quote_v3_decrease_liquidity_amounts(
    chain_id: int,
    protocol: str,
    token_id: int,
    liquidity: int,
    owner_hint: str,
    dec0: int,
    dec1: int,
) -> tuple[float | None, float | None]:
    cid = int(chain_id)
    tid = int(token_id)
    liq = int(liquidity)
    owner_addr = str(owner_hint or "").strip().lower()
    if tid <= 0 or liq <= 0:
        return None, None
    pm = _position_manager_for_protocol(cid, protocol)
    if not _is_eth_address(pm):
        return None, None
    selector = _method_selector("decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))")
    if not selector:
        return None, None
    deadline = int(time.time()) + 86400
    liq_128 = max(0, min(int(liq), (1 << 128) - 1))
    data = (
        selector
        + _encode_uint_word(int(tid))
        + _encode_uint_word(int(liq_128))
        + _encode_uint_word(0)
        + _encode_uint_word(0)
        + _encode_uint_word(int(deadline))
    )
    callers: list[str] = []
    if _is_eth_address(owner_addr):
        callers.append(owner_addr)
    callers.append("")
    for caller in callers:
        try:
            out = _eth_call_hex(cid, pm, data, from_addr=caller or None)
            words = _hex_words(out)
            if len(words) < 2:
                continue
            a0_raw = max(0, _decode_uint_from_word(words[0]))
            a1_raw = max(0, _decode_uint_from_word(words[1]))
            d0 = int(dec0) if 0 <= int(dec0) <= 36 else 18
            d1 = int(dec1) if 0 <= int(dec1) <= 36 else 18
            a0 = float(Decimal(a0_raw) / (Decimal(10) ** d0))
            a1 = float(Decimal(a1_raw) / (Decimal(10) ** d1))
            return a0, a1
        except Exception:
            continue
    return None, None


def _fetch_v3_tokens_owed_raw(chain_id: int, protocol: str, token_id: int) -> tuple[int, int] | None:
    cid = int(chain_id)
    tid = int(token_id)
    if tid <= 0:
        return None
    pm = _position_manager_for_protocol(cid, protocol)
    if not _is_eth_address(pm):
        return None
    try:
        pos_data = "0x99fbab88" + _encode_uint_word(int(tid))
        pos_words = _hex_words(_eth_call_hex(cid, pm, pos_data))
        if len(pos_words) < 12:
            return None
        owed0 = max(0, _decode_uint_from_word(pos_words[10]))
        owed1 = max(0, _decode_uint_from_word(pos_words[11]))
        return owed0, owed1
    except Exception:
        return None


def _quote_v3_collect_fees_amounts(
    chain_id: int,
    protocol: str,
    token_id: int,
    owner_hint: str,
    dec0: int,
    dec1: int,
) -> tuple[float | None, float | None]:
    cid = int(chain_id)
    tid = int(token_id)
    owner_addr = str(owner_hint or "").strip().lower()
    if tid <= 0:
        return None, None
    pm = _position_manager_for_protocol(cid, protocol)
    if not _is_eth_address(pm):
        return None, None
    selector = _method_selector("collect((uint256,address,uint128,uint128))")
    if not selector:
        return None, None
    amount_max_128 = (1 << 128) - 1
    recipient = owner_addr if _is_eth_address(owner_addr) else "0x0000000000000000000000000000000000000000"
    data = (
        selector
        + _encode_uint_word(int(tid))
        + _encode_address_word(recipient)
        + _encode_uint_word(int(amount_max_128))
        + _encode_uint_word(int(amount_max_128))
    )
    callers: list[str] = []
    if _is_eth_address(owner_addr):
        callers.append(owner_addr)
    callers.append("")
    for caller in callers:
        try:
            out = _eth_call_hex(cid, pm, data, from_addr=caller or None)
            words = _hex_words(out)
            if len(words) < 2:
                continue
            fee0_raw = max(0, _decode_uint_from_word(words[0]))
            fee1_raw = max(0, _decode_uint_from_word(words[1]))
            d0 = int(dec0) if 0 <= int(dec0) <= 36 else 18
            d1 = int(dec1) if 0 <= int(dec1) <= 36 else 18
            fee0 = float(Decimal(fee0_raw) / (Decimal(10) ** d0))
            fee1 = float(Decimal(fee1_raw) / (Decimal(10) ** d1))
            return fee0, fee1
        except Exception:
            continue
    return None, None


def _fetch_v3_position_contract_snapshot(
    chain_id: int,
    protocol: str,
    token_id: int,
    owner_hint: str,
    *,
    include_quotes: bool = True,
) -> dict[str, Any] | None:
    cid = int(chain_id)
    proto = str(protocol or "").strip().lower()
    tid = int(token_id)
    owner_addr = str(owner_hint or "").strip().lower()
    if tid <= 0:
        return None
    cache_key = (cid, proto, tid)
    now = time.time()
    with POSITION_CONTRACT_SNAPSHOT_CACHE_LOCK:
        cached = POSITION_CONTRACT_SNAPSHOT_CACHE.get(cache_key)
    if cached:
        ts, payload = cached
        if (now - float(ts or 0.0)) <= float(POSITION_CONTRACT_SNAPSHOT_TTL_SEC):
            return dict(payload or {})
    pm = _position_manager_for_protocol(cid, proto)
    if not _is_eth_address(pm):
        return None
    try:
        pos_data = "0x99fbab88" + _encode_uint_word(int(tid))
        words = _hex_words(_eth_call_hex(cid, pm, pos_data))
        if len(words) < 12:
            return None
        nonce = _decode_uint_from_word(words[0])
        operator = _decode_address_from_word(words[1])
        token0 = _decode_address_from_word(words[2]).lower()
        token1 = _decode_address_from_word(words[3]).lower()
        fee = _decode_uint_from_word(words[4])
        tick_lower = _decode_int_from_word(words[5])
        tick_upper = _decode_int_from_word(words[6])
        liq = max(0, _decode_uint_from_word(words[7]))
        owed0_raw = max(0, _decode_uint_from_word(words[10]))
        owed1_raw = max(0, _decode_uint_from_word(words[11]))
        dec0 = 18
        dec1 = 18
        sym0 = ""
        sym1 = ""
        primary_batch = _eth_call_hex_batch(
            cid,
            [
                {"to": token0, "data": "0x313ce567"},
                {"to": token1, "data": "0x313ce567"},
                {"to": token0, "data": "0x95d89b41"},
                {"to": token1, "data": "0x95d89b41"},
            ],
        )
        try:
            if primary_batch[0]:
                dec0 = _decode_uint_eth_call(primary_batch[0])
            if primary_batch[1]:
                dec1 = _decode_uint_eth_call(primary_batch[1])
        except Exception:
            dec0, dec1 = 18, 18
        if dec0 <= 0 or dec0 > 36:
            dec0 = 18
        if dec1 <= 0 or dec1 > 36:
            dec1 = 18
        sym0 = _normalize_display_symbol(_decode_abi_string(primary_batch[2] or "") or "")
        sym1 = _normalize_display_symbol(_decode_abi_string(primary_batch[3] or "") or "")
        if not sym0:
            sym0 = _normalize_display_symbol(_fetch_erc20_symbol_onchain(cid, token0) or "")
        if not sym1:
            sym1 = _normalize_display_symbol(_fetch_erc20_symbol_onchain(cid, token1) or "")
        q_amount0: float | None = None
        q_amount1: float | None = None
        q_fee0: float | None = None
        q_fee1: float | None = None
        if bool(include_quotes):
            q_amount0, q_amount1 = _quote_v3_decrease_liquidity_amounts(
                cid, proto, tid, liq, owner_addr, dec0, dec1
            )
            q_fee0, q_fee1 = _quote_v3_collect_fees_amounts(
                cid, proto, tid, owner_addr, dec0, dec1
            )
        payload: dict[str, Any] = {
            "chain_id": cid,
            "protocol": proto,
            "position_manager": pm,
            "token_id": tid,
            "owner_hint": owner_addr,
            "nonce": int(nonce),
            "operator": str(operator or "").lower(),
            "token0": str(token0),
            "token1": str(token1),
            "fee": int(fee),
            "tick_lower": int(tick_lower),
            "tick_upper": int(tick_upper),
            "liquidity": int(liq),
            "tokens_owed0_raw": int(owed0_raw),
            "tokens_owed1_raw": int(owed1_raw),
            "token0_decimals": int(dec0),
            "token1_decimals": int(dec1),
            "token0_symbol": str(sym0 or ""),
            "token1_symbol": str(sym1 or ""),
            "quote_amount0": q_amount0,
            "quote_amount1": q_amount1,
            "quote_fee0": q_fee0,
            "quote_fee1": q_fee1,
        }
        with POSITION_CONTRACT_SNAPSHOT_CACHE_LOCK:
            POSITION_CONTRACT_SNAPSHOT_CACHE[cache_key] = (now, payload)
        return dict(payload)
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


def _json_rpc_call(
    chain_id: int,
    method: str,
    params: list[Any],
    *,
    timeout_sec: float | None = None,
    deadline_ts: float | None = None,
) -> Any:
    rpc_urls = _rpc_urls_for_chain(chain_id)
    if not rpc_urls:
        raise RuntimeError(f"No RPC configured for chain_id={chain_id}")
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode("utf-8")
    last_err = "unknown rpc error"
    for rpc_url in rpc_urls:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            raise RuntimeError("rpc deadline exceeded")
        try:
            req = UrlRequest(
                rpc_url,
                data=body,
                headers={"Content-Type": "application/json", "User-Agent": "uni-fee-web/0.0.2"},
            )
            timeout_s = float(POSITIONS_ONCHAIN_TIMEOUT_SEC)
            if timeout_sec is not None:
                timeout_s = max(1.0, float(timeout_sec))
            if deadline_ts is not None:
                timeout_s = min(timeout_s, max(0.25, float(deadline_ts) - time.monotonic()))
            with urlopen(req, timeout=timeout_s) as resp:
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


def _json_rpc_batch_call(
    chain_id: int,
    payloads: list[dict[str, Any]],
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    rpc_urls = _rpc_urls_for_chain(chain_id)
    if not rpc_urls:
        raise RuntimeError(f"No RPC configured for chain_id={chain_id}")
    if not payloads:
        return []
    # Prefer known batch-friendly public RPCs for batched ownerOf scans.
    if int(chain_id) in (56, 8453):
        rpc_urls = sorted(
            rpc_urls,
            key=lambda u: 0 if "publicnode.com" in str(u).lower() else 1,
        )
    out_rows: list[dict[str, Any]] = []
    chunk_size = max(10, min(int(POSITIONS_RPC_BATCH_MAX_ITEMS), len(payloads)))
    for i in range(0, len(payloads), chunk_size):
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            raise RuntimeError("batch rpc deadline exceeded")
        chunk = payloads[i : i + chunk_size]
        body = json.dumps(chunk).encode("utf-8")
        last_err = "unknown batch rpc error"
        chunk_ok = False
        for rpc_url in rpc_urls:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                raise RuntimeError("batch rpc deadline exceeded")
            try:
                req = UrlRequest(
                    rpc_url,
                    data=body,
                    headers={"Content-Type": "application/json", "User-Agent": "uni-fee-web/0.0.2"},
                )
                timeout_s = max(8, POSITIONS_ONCHAIN_TIMEOUT_SEC * 3)
                if deadline_ts is not None:
                    left = max(0.25, deadline_ts - time.monotonic())
                    timeout_s = min(timeout_s, left)
                with urlopen(req, timeout=timeout_s) as resp:
                    parsed = json.loads(resp.read().decode("utf-8"))
                if not isinstance(parsed, list):
                    last_err = "batch rpc did not return list"
                    continue
                out_rows.extend([x for x in parsed if isinstance(x, dict)])
                chunk_ok = True
                break
            except Exception as e:
                last_err = str(e)
                continue
        if chunk_ok:
            continue
        # Hard fallback: execute requests one-by-one when providers reject batch.
        for p in chunk:
            rid = p.get("id")
            method = str(p.get("method") or "").strip()
            params = p.get("params")
            if not method or not isinstance(params, list):
                out_rows.append({"id": rid, "error": {"message": "invalid batch payload item"}})
                continue
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                out_rows.append({"id": rid, "error": {"message": "rpc deadline exceeded"}})
                continue
            try:
                single_result = _json_rpc_call(
                    int(chain_id),
                    method,
                    params,
                    timeout_sec=max(2.0, float(POSITIONS_ONCHAIN_TIMEOUT_SEC)),
                    deadline_ts=deadline_ts,
                )
                out_rows.append({"id": rid, "result": single_result})
            except Exception as e:
                out_rows.append({"id": rid, "error": {"message": str(e) or last_err}})
    return out_rows


def _scan_pancake_infinity_cl_positions_graph(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    ep = INFINITY_CL_SUBGRAPH_BY_CHAIN_ID.get(cid)
    owner_raw = str(owner or "").strip()
    if not ep or not _is_eth_address(owner_raw):
        return []
    if deadline_ts is not None and time.monotonic() >= deadline_ts:
        return []
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    skip = 0
    owner_bytes = owner_raw.lower()

    def _rows_from_payload(payload: dict[str, Any], mode: str) -> list[dict[str, Any]]:
        data = payload.get("data") or {}
        if mode in ("positions", "positions_in"):
            return (data.get("positions") or []) or []
        if mode == "account":
            acc = data.get("account") or {}
            return (acc.get("positions") or []) or []
        if mode == "accounts":
            accs = (data.get("accounts") or []) or []
            first = accs[0] if accs else {}
            return (first.get("positions") or []) or []
        return []

    queries: list[tuple[str, str]] = [
        (
            "positions",
            """
            query InfinityPositions($owner: Bytes!, $skip: Int!) {
              positions(first: 200, skip: $skip, where: { owner: $owner }) {
                id
                liquidity
                tickLower
                tickUpper
                pool {
                  id
                  feeTier
                  liquidity
                  sqrtPrice
                  token0Price
                  totalValueLockedUSD
                  token0 { id decimals symbol }
                  token1 { id decimals symbol }
                }
              }
            }
            """,
        ),
        (
            "positions_in",
            """
            query InfinityPositions($owner: Bytes!, $skip: Int!) {
              positions(first: 200, skip: $skip, where: { owner_in: [$owner] }) {
                id
                liquidity
                tickLower
                tickUpper
                pool {
                  id
                  feeTier
                  liquidity
                  sqrtPrice
                  token0Price
                  totalValueLockedUSD
                  token0 { id decimals symbol }
                  token1 { id decimals symbol }
                }
              }
            }
            """,
        ),
        (
            "account",
            """
            query InfinityPositions($owner: Bytes!, $skip: Int!) {
              account(id: $owner) {
                positions(first: 200, skip: $skip) {
                  id
                  liquidity
                  tickLower
                  tickUpper
                  pool {
                    id
                    feeTier
                    liquidity
                    sqrtPrice
                    token0Price
                    totalValueLockedUSD
                    token0 { id decimals symbol }
                    token1 { id decimals symbol }
                  }
                }
              }
            }
            """,
        ),
    ]

    for mode, query in queries:
        skip = 0
        while True:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            payload = graphql_query(ep, query, {"owner": owner_bytes, "skip": skip}, retries=1)
            rows = _rows_from_payload(payload, mode)
            if not rows:
                break
            for r in rows:
                if not isinstance(r, dict):
                    continue
                pid = str(r.get("id") or "").strip()
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                pool = r.get("pool") or {}
                t0 = pool.get("token0") or {}
                t1 = pool.get("token1") or {}
                pos_liq = str(r.get("liquidity") or "0")
                out.append(
                    {
                        "id": f"inf-cl:{pid}",
                        "liquidity": pos_liq,
                        "tickLower": {"tickIdx": str(r.get("tickLower") or "0")},
                        "tickUpper": {"tickIdx": str(r.get("tickUpper") or "0")},
                        "pool": {
                            "id": str(pool.get("id") or "").strip().lower(),
                            "feeTier": str(pool.get("feeTier") or "0"),
                            "liquidity": str(pool.get("liquidity") or "0"),
                            "sqrtPrice": str(pool.get("sqrtPrice") or "0"),
                            "token0Price": str(pool.get("token0Price") or "0"),
                            "totalValueLockedUSD": str(pool.get("totalValueLockedUSD") or "0"),
                            "token0": {
                                "id": str(t0.get("id") or "").strip().lower(),
                                "decimals": str(t0.get("decimals") or "18"),
                                "symbol": str(t0.get("symbol") or "") or None,
                            },
                            "token1": {
                                "id": str(t1.get("id") or "").strip().lower(),
                                "decimals": str(t1.get("decimals") or "18"),
                                "symbol": str(t1.get("symbol") or "") or None,
                            },
                        },
                        "_protocol_label": "pancake_infinity_cl",
                        "_source": "graph_pancake_infinity_cl",
                        "_skip_enrich": True,
                    }
                )
            if len(rows) < 200:
                break
            skip += 200
        if out:
            break
    return out


def _eth_call_hex(chain_id: int, to: str, data_hex: str, *, from_addr: str | None = None) -> str:
    call_obj: dict[str, Any] = {"to": str(to).strip(), "data": str(data_hex).strip()}
    fa = str(from_addr or "").strip().lower()
    if _is_eth_address(fa):
        call_obj["from"] = fa
    result = _json_rpc_call(
        chain_id,
        "eth_call",
        [call_obj, "latest"],
    )
    out = str(result or "").strip().lower()
    if not out.startswith("0x"):
        raise RuntimeError("Invalid eth_call result")
    return out


def _eth_call_hex_batch(
    chain_id: int,
    calls: list[dict[str, Any]],
) -> list[str | None]:
    if not calls:
        return []
    payloads: list[dict[str, Any]] = []
    for idx, call in enumerate(calls, start=1):
        to = str((call or {}).get("to") or "").strip()
        data_hex = str((call or {}).get("data") or "").strip()
        from_addr = str((call or {}).get("from") or "").strip().lower()
        if not _is_eth_address(to) or not str(data_hex).startswith("0x"):
            payloads.append({})
            continue
        call_obj: dict[str, Any] = {"to": to, "data": data_hex}
        if _is_eth_address(from_addr):
            call_obj["from"] = from_addr
        payloads.append(
            {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "eth_call",
                "params": [call_obj, "latest"],
            }
        )
    try:
        resp = _json_rpc_batch_call(int(chain_id), [p for p in payloads if p])
    except Exception:
        return [None for _ in calls]
    by_id: dict[int, dict[str, Any]] = {}
    for row in resp:
        try:
            rid = int(row.get("id"))
        except Exception:
            continue
        by_id[rid] = row
    out: list[str | None] = []
    for idx, p in enumerate(payloads, start=1):
        if not p:
            out.append(None)
            continue
        row = by_id.get(idx) or {}
        raw = str(row.get("result") or "").strip().lower()
        if raw.startswith("0x") and row.get("error") is None:
            out.append(raw)
        else:
            out.append(None)
    return out


def _eth_block_number(chain_id: int) -> int:
    out = str(_json_rpc_call(int(chain_id), "eth_blockNumber", []) or "0x0").strip().lower()
    if out.startswith("0x"):
        return int(out, 16)
    return int(out or "0")


def _eth_get_logs(
    chain_id: int,
    params: dict[str, Any],
    *,
    deadline_ts: float | None = None,
    timeout_sec: float | None = None,
    max_attempts: int = 2,
    debug_out: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    last_exc: Exception | None = None
    attempts = max(1, int(max_attempts))
    req_started = time.perf_counter()
    first_try_failed = False
    if isinstance(debug_out, dict):
        debug_out["rpc_getlogs_requests"] = int(debug_out.get("rpc_getlogs_requests") or 0) + 1
    for attempt_idx in range(attempts):
        try:
            if isinstance(debug_out, dict):
                debug_out["rpc_getlogs_attempts"] = int(debug_out.get("rpc_getlogs_attempts") or 0) + 1
            out = _json_rpc_call(
                int(chain_id),
                "eth_getLogs",
                [params],
                timeout_sec=(timeout_sec if timeout_sec is not None else max(12.0, float(POSITIONS_ONCHAIN_TIMEOUT_SEC) * 4.0)),
                deadline_ts=deadline_ts,
            )
            if isinstance(out, list):
                if isinstance(debug_out, dict):
                    debug_out["rpc_getlogs_success"] = int(debug_out.get("rpc_getlogs_success") or 0) + 1
                    debug_out["rpc_getlogs_ms"] = int(debug_out.get("rpc_getlogs_ms") or 0) + int(
                        max(0.0, (time.perf_counter() - req_started) * 1000.0)
                    )
                    if first_try_failed:
                        debug_out["rpc_getlogs_retry_success"] = int(debug_out.get("rpc_getlogs_retry_success") or 0) + 1
                return [x for x in out if isinstance(x, dict)]
            return []
        except Exception as e:
            last_exc = e
            if attempt_idx == 0:
                first_try_failed = True
            if isinstance(debug_out, dict):
                debug_out["rpc_getlogs_failures"] = int(debug_out.get("rpc_getlogs_failures") or 0) + 1
                if attempt_idx == 0:
                    debug_out["rpc_getlogs_first_try_fail"] = int(debug_out.get("rpc_getlogs_first_try_fail") or 0) + 1
                debug_out["rpc_getlogs_last_error"] = str(e)[:180]
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            continue
    if last_exc is not None:
        raise last_exc
    return []


def _eth_get_code(chain_id: int, address: str, block_tag: str = "latest") -> str:
    out = str(_json_rpc_call(int(chain_id), "eth_getCode", [str(address).strip(), str(block_tag)]) or "").strip().lower()
    return out if out.startswith("0x") else "0x"


def _eth_get_block_timestamp(chain_id: int, block_number: int) -> int:
    out = _json_rpc_call(int(chain_id), "eth_getBlockByNumber", [hex(max(0, int(block_number))), False])
    if not isinstance(out, dict):
        return 0
    ts_hex = str(out.get("timestamp") or "0x0").strip().lower()
    try:
        return int(ts_hex, 16) if ts_hex.startswith("0x") else int(ts_hex or "0")
    except Exception:
        return 0


def _explorer_contract_creation_block(chain_id: int, contract_address: str) -> int:
    cid = int(chain_id)
    addr = str(contract_address or "").strip().lower()
    if not _is_eth_address(addr):
        return 0
    # Prefer Etherscan V2 when key exists (supports multiple chains via chainid).
    eth_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    bsc_key = os.environ.get("BSCSCAN_API_KEY", "").strip()
    base_key = os.environ.get("BASESCAN_API_KEY", "").strip() or eth_key
    chainid_for_v2 = {1: "1", 10: "10", 56: "56", 137: "137", 8453: "8453", 42161: "42161"}.get(cid, "")
    urls: list[str] = []
    if chainid_for_v2 and eth_key:
        urls.append(
            "https://api.etherscan.io/v2/api"
            f"?chainid={chainid_for_v2}&module=contract&action=getcontractcreation"
            f"&contractaddresses={addr}&apikey={eth_key}"
        )
    if cid == 56 and bsc_key:
        urls.append(
            f"https://api.bscscan.com/api?module=contract&action=getcontractcreation"
            f"&contractaddresses={addr}&apikey={bsc_key}"
        )
    elif cid == 8453 and base_key:
        urls.append(
            f"https://api.basescan.org/api?module=contract&action=getcontractcreation"
            f"&contractaddresses={addr}&apikey={base_key}"
        )
    for url in urls:
        try:
            req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
            with urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            rows = (payload or {}).get("result")
            if not isinstance(rows, list) or not rows:
                continue
            first = rows[0] if isinstance(rows[0], dict) else {}
            blk = _parse_int_like(first.get("blockNumber") or 0)
            if blk > 0:
                return int(blk)
        except Exception:
            continue
    return 0


def _contract_creation_date_ymd(chain_id: int, contract_address: str) -> str:
    addr = str(contract_address or "").strip().lower()
    if not _is_eth_address(addr):
        return ""
    key = (int(chain_id), addr)
    with CONTRACT_CREATION_DATE_CACHE_LOCK:
        cached = CONTRACT_CREATION_DATE_CACHE.get(key)
    if cached is not None:
        return cached
    try:
        explorer_block = _explorer_contract_creation_block(int(chain_id), addr)
        if explorer_block > 0:
            ts = _eth_get_block_timestamp(int(chain_id), int(explorer_block))
            if ts > 0:
                ymd = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                with CONTRACT_CREATION_DATE_CACHE_LOCK:
                    CONTRACT_CREATION_DATE_CACHE[key] = ymd
                return ymd
        latest = _eth_block_number(int(chain_id))
        if latest <= 0:
            with CONTRACT_CREATION_DATE_CACHE_LOCK:
                CONTRACT_CREATION_DATE_CACHE[key] = ""
            return ""
        if _eth_get_code(int(chain_id), addr, "latest") in {"0x", "0x0"}:
            with CONTRACT_CREATION_DATE_CACHE_LOCK:
                CONTRACT_CREATION_DATE_CACHE[key] = ""
            return ""
        lo, hi = 0, int(latest)
        first = int(latest)
        while lo <= hi:
            mid = (lo + hi) // 2
            code = _eth_get_code(int(chain_id), addr, hex(int(mid)))
            if code not in {"0x", "0x0"}:
                first = mid
                hi = mid - 1
            else:
                lo = mid + 1
        ts = _eth_get_block_timestamp(int(chain_id), int(first))
        date_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat() if ts > 0 else ""
    except Exception:
        date_str = ""
    with CONTRACT_CREATION_DATE_CACHE_LOCK:
        CONTRACT_CREATION_DATE_CACHE[key] = date_str
    return date_str


def _contract_creation_date_cached(chain_id: int, contract_address: str) -> str:
    addr = str(contract_address or "").strip().lower()
    if not _is_eth_address(addr):
        return ""
    key = (int(chain_id), addr)
    with CONTRACT_CREATION_DATE_CACHE_LOCK:
        cached = CONTRACT_CREATION_DATE_CACHE.get(key)
    if cached is not None:
        return str(cached or "")
    # Lazy compute on first access so Created column is populated.
    return _contract_creation_date_ymd(int(chain_id), addr)


def _contract_creation_date_peek(chain_id: int, contract_address: str) -> str:
    addr = str(contract_address or "").strip().lower()
    if not _is_eth_address(addr):
        return ""
    key = (int(chain_id), addr)
    with CONTRACT_CREATION_DATE_CACHE_LOCK:
        cached = CONTRACT_CREATION_DATE_CACHE.get(key)
    return str(cached or "")


def _position_token_id_from_raw(position_id: Any) -> int:
    raw = str(position_id or "").strip()
    if not raw:
        return 0
    if ":" in raw:
        raw = raw.split(":", 1)[1].strip()
    return _parse_int_like(raw)


def _position_manager_for_protocol(chain_id: int, protocol: str) -> str:
    cid = int(chain_id)
    proto = str(protocol or "").strip().lower()
    if proto == "uniswap_v3":
        return str(UNISWAP_V3_NPM_BY_CHAIN_ID.get(cid) or "").strip().lower()
    if proto in {"pancake_v3", "pancake_v3_staked"}:
        # Pancake v3 NPM is stored in chain-specific v3 NPM map.
        return str(UNISWAP_V3_NPM_BY_CHAIN_ID.get(cid) or "").strip().lower()
    if proto == "pancake_infinity_cl":
        return str(PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid) or "").strip().lower()
    if proto == "pancake_infinity_bin":
        return str(PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid) or "").strip().lower()
    return ""


def _position_creation_date_ymd(chain_id: int, protocol: str, position_id: Any) -> str:
    cid = int(chain_id)
    proto = str(protocol or "").strip().lower()
    tid = _position_token_id_from_raw(position_id)
    if cid <= 0 or not proto or tid <= 0:
        return ""
    key = (cid, proto, int(tid))
    with POSITION_CREATION_DATE_CACHE_LOCK:
        cached = POSITION_CREATION_DATE_CACHE.get(key)
    if cached is not None:
        return str(cached or "")
    pm = _position_manager_for_protocol(cid, proto)
    if not _is_eth_address(pm):
        with POSITION_CREATION_DATE_CACHE_LOCK:
            POSITION_CREATION_DATE_CACHE[key] = ""
        return ""
    date_str = ""
    try:
        topic_transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        topic_tid = "0x" + _encode_uint_word(int(tid))
        logs = _eth_get_logs(
            cid,
            {
                "address": pm,
                "fromBlock": "0x0",
                "toBlock": "latest",
                "topics": [topic_transfer, None, None, topic_tid],
            },
        )
        first_block = 0
        for lg in (logs or []):
            try:
                bn = int(str((lg or {}).get("blockNumber") or "0x0"), 16)
            except Exception:
                bn = 0
            if bn > 0 and (first_block <= 0 or bn < first_block):
                first_block = bn
        if first_block > 0:
            ts = _eth_get_block_timestamp(cid, first_block)
            if ts > 0:
                date_str = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
    except Exception:
        date_str = ""
    with POSITION_CREATION_DATE_CACHE_LOCK:
        POSITION_CREATION_DATE_CACHE[key] = date_str
    return date_str


def _position_creation_date_peek(chain_id: int, protocol: str, position_id: Any) -> str:
    cid = int(chain_id)
    proto = str(protocol or "").strip().lower()
    tid = _position_token_id_from_raw(position_id)
    if cid <= 0 or not proto or tid <= 0:
        return ""
    key = (cid, proto, int(tid))
    with POSITION_CREATION_DATE_CACHE_LOCK:
        cached = POSITION_CREATION_DATE_CACHE.get(key)
    return str(cached or "")


def _position_creation_date_cache_set(chain_id: int, protocol: str, position_id: Any, date_str: str) -> None:
    cid = int(chain_id)
    proto = str(protocol or "").strip().lower()
    tid = _position_token_id_from_raw(position_id)
    d = str(date_str or "").strip()
    if cid <= 0 or not proto or tid <= 0 or not d:
        return
    with POSITION_CREATION_DATE_CACHE_LOCK:
        POSITION_CREATION_DATE_CACHE[(cid, proto, int(tid))] = d


def _populate_creation_dates_parallel(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    keys: list[tuple[int, str, str]] = []
    seen: set[tuple[int, str, str]] = set()
    for r in rows:
        try:
            cid = int(r.get("chain_id") or 0)
        except Exception:
            cid = 0
        proto = str(r.get("protocol") or "").strip().lower()
        pid = str(r.get("position_id") or "").strip()
        if cid <= 0 or not proto or not pid:
            continue
        k = (cid, proto, pid)
        if k in seen:
            continue
        seen.add(k)
        keys.append(k)
    if not keys:
        return
    deadline = time.monotonic() + float(POSITIONS_CREATION_DATE_MAX_SECONDS)
    workers = max(1, min(int(POSITIONS_CREATION_DATE_WORKERS), len(keys)))
    if workers <= 1:
        for cid, proto, pid in keys:
            if time.monotonic() >= deadline:
                break
            _position_creation_date_ymd(cid, proto, pid)
    else:
        ex = ThreadPoolExecutor(max_workers=workers)
        futures = [ex.submit(_position_creation_date_ymd, cid, proto, pid) for cid, proto, pid in keys]
        aborted = False
        try:
            pending = set(futures)
            while pending:
                if time.monotonic() >= deadline:
                    aborted = True
                    break
                timeout_left = max(0.05, min(0.8, deadline - time.monotonic()))
                done, pending = wait(pending, timeout=timeout_left, return_when=FIRST_COMPLETED)
                for fut in done:
                    try:
                        fut.result(timeout=0)
                    except Exception:
                        continue
        finally:
            ex.shutdown(wait=not aborted, cancel_futures=aborted)
    for r in rows:
        try:
            cid = int(r.get("chain_id") or 0)
        except Exception:
            cid = 0
        proto = str(r.get("protocol") or "").strip().lower()
        pid = str(r.get("position_id") or "").strip()
        if cid <= 0 or not proto or not pid:
            continue
        d = _position_creation_date_peek(cid, proto, pid)
        if d:
            r["position_created_date"] = d


def _enrich_pair_symbols_background(rows: list[dict[str, Any]], max_seconds: int = 20) -> None:
    if not rows:
        return
    deadline = time.monotonic() + max(2, int(max_seconds))
    for r in rows:
        if time.monotonic() >= deadline:
            break
        try:
            cid = int(r.get("chain_id") or 0)
        except Exception:
            cid = 0
        if cid <= 0:
            continue
        pair = str(r.get("pair") or "").strip()
        token0 = str(r.get("token0_id") or "").strip().lower()
        token1 = str(r.get("token1_id") or "").strip().lower()
        if not token0 or not token1:
            continue
        needs_refresh = ("0x" in pair.lower()) or ("?" in pair) or (not pair)
        if not needs_refresh:
            continue
        chain_key = str(r.get("chain") or CHAIN_ID_TO_KEY.get(cid, "")).strip().lower()
        sym0 = (_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(chain_key, {}) or {}).get(token0) or _fetch_erc20_symbol_onchain(cid, token0) or token0[:8]
        sym1 = (_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(chain_key, {}) or {}).get(token1) or _fetch_erc20_symbol_onchain(cid, token1) or token1[:8]
        r["pair"] = f"{str(sym0).upper()}/{str(sym1).upper()}"


def _enrich_tvl_background(rows: list[dict[str, Any]], max_seconds: int = 25) -> None:
    if not rows:
        return
    deadline = time.monotonic() + max(3, int(max_seconds))
    touched = 0
    for r in rows:
        if time.monotonic() >= deadline or touched >= 120:
            break
        if not isinstance(r, dict):
            continue
        protocol = str(r.get("protocol") or "").strip().lower()
        if protocol not in {"uniswap_v3", "uniswap_v4", "pancake_v3"}:
            continue
        current_tvl = _safe_float(r.get("tvl_usd"))
        mode = str(r.get("valuation_mode") or "").strip().lower()
        needs_recalc = (current_tvl <= 0) or (mode in {"no-exact-external", "exact-unavailable"})
        if not needs_recalc:
            continue
        chain_key = str(r.get("chain") or "").strip().lower()
        version = "v4" if protocol.endswith("_v4") else "v3"
        endpoint = get_graph_endpoint(chain_key, version=version)
        if not endpoint:
            continue
        pos_ids = [str(x).strip() for x in (r.get("position_ids") or []) if str(x).strip()]
        if not pos_ids:
            continue
        chain_id = int(r.get("chain_id") or 0)
        if chain_id <= 0:
            continue
        try:
            pos = _fetch_position_by_id_with_detail(
                endpoint,
                pos_ids[0],
                include_pool_liquidity=True,
                include_position_liquidity=True,
            )
            if not pos:
                continue
            pool = pos.get("pool") or {}
            pool_tvl_usd = _safe_float(pool.get("totalValueLockedUSD")) or _safe_float(r.get("pool_tvl_usd"))
            new_tvl: float | None = _estimate_position_tvl_usd_from_detail_external(pos, pool, chain_id)
            new_mode = "exact-external-bg"
            if new_tvl is None or new_tvl <= 0:
                continue
            r["tvl_usd"] = float(new_tvl)
            if pool_tvl_usd > 0:
                r["pool_tvl_usd"] = float(pool_tvl_usd)
            r["valuation_mode"] = new_mode
            touched += 1
        except Exception:
            continue


def _enrich_missing_creation_dates(rows: list[dict[str, Any]], max_seconds: int = 40, max_rows: int = 400) -> None:
    if not rows:
        return
    deadline = time.monotonic() + max(5, int(max_seconds))
    checked = 0
    for r in rows:
        if checked >= int(max_rows) or time.monotonic() >= deadline:
            break
        if str(r.get("position_created_date") or "").strip() not in {"", "-"}:
            continue
        try:
            cid = int(r.get("chain_id") or 0)
        except Exception:
            cid = 0
        proto = str(r.get("protocol") or "").strip().lower()
        pid = str(r.get("position_id") or "").strip()
        if cid <= 0 or not proto or not pid:
            continue
        d = _position_creation_date_ymd(cid, proto, pid)
        if d:
            r["position_created_date"] = d
        checked += 1


def _scan_erc721_token_ids_by_incoming_logs(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    deadline_ts: float | None = None,
    max_ids: int = 120,
    lookback_blocks: int | None = None,
    debug_out: dict[str, Any] | None = None,
) -> list[int]:
    cid = int(chain_id)
    c = str(contract or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(c) or not _is_eth_address(o):
        return []
    try:
        latest = _eth_block_number(cid)
    except Exception:
        return []
    if latest <= 0:
        return []
    lb = int(lookback_blocks) if lookback_blocks is not None else int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS)
    min_block = max(0, int(latest) - max(1, lb))
    step = int(POSITIONS_ERC721_LOG_BLOCK_STEP)
    topic_transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    topic_to_owner = "0x" + ("0" * 24) + o[2:]
    end_block = int(latest)
    out: list[int] = []
    seen: set[int] = set()
    while end_block >= min_block:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        start_block = max(min_block, end_block - step + 1)
        params = {
            "address": c,
            "fromBlock": hex(int(start_block)),
            "toBlock": hex(int(end_block)),
            "topics": [topic_transfer, None, topic_to_owner],
        }
        try:
            logs = _eth_get_logs(cid, params, deadline_ts=deadline_ts, debug_out=debug_out)
        except Exception:
            # Reduce chunk size on provider "too many results" style failures.
            if step > 5000:
                step = max(5000, step // 2)
                continue
            logs = []
        logs_sorted = sorted(
            logs,
            key=lambda x: (
                int(str(x.get("blockNumber") or "0x0"), 16),
                int(str(x.get("logIndex") or "0x0"), 16),
            ),
            reverse=True,
        )
        for lg in logs_sorted:
            topics = lg.get("topics") or []
            if not isinstance(topics, list) or len(topics) < 4:
                continue
            try:
                tid = int(str(topics[3]), 16)
            except Exception:
                continue
            if tid <= 0 or tid in seen:
                continue
            seen.add(tid)
            out.append(int(tid))
            if len(out) >= int(max_ids):
                return out
        end_block = int(start_block) - 1
    return out


def _scan_erc721_token_ids_by_recent_transfers_ownerof(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    deadline_ts: float | None = None,
    max_ids: int = 120,
    lookback_blocks: int | None = None,
) -> list[int]:
    cid = int(chain_id)
    c = str(contract or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(c) or not _is_eth_address(o):
        return []
    try:
        latest = _eth_block_number(cid)
    except Exception:
        return []
    if latest <= 0:
        return []
    lb = int(lookback_blocks) if lookback_blocks is not None else int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS)
    min_block = max(0, int(latest) - max(1, lb))
    step = int(POSITIONS_ERC721_LOG_BLOCK_STEP)
    topic_transfer = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    owner_word = _encode_address_word(o)[-40:]
    end_block = int(latest)
    out: list[int] = []
    seen_candidates: set[int] = set()
    while end_block >= min_block:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        start_block = max(min_block, end_block - step + 1)
        params = {
            "address": c,
            "fromBlock": hex(int(start_block)),
            "toBlock": hex(int(end_block)),
            "topics": [topic_transfer],
        }
        try:
            logs = _eth_get_logs(cid, params, deadline_ts=deadline_ts)
        except Exception:
            if step > 5000:
                step = max(5000, step // 2)
                continue
            logs = []
        logs_sorted = sorted(
            logs,
            key=lambda x: (
                int(str(x.get("blockNumber") or "0x0"), 16),
                int(str(x.get("logIndex") or "0x0"), 16),
            ),
            reverse=True,
        )
        for lg in logs_sorted:
            topics = lg.get("topics") or []
            if not isinstance(topics, list) or len(topics) < 4:
                continue
            try:
                tid = int(str(topics[3]), 16)
            except Exception:
                continue
            if tid <= 0 or tid in seen_candidates:
                continue
            seen_candidates.add(tid)
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, c, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    continue
            except Exception:
                continue
            out.append(int(tid))
            if len(out) >= int(max_ids):
                return out
        end_block = int(start_block) - 1
    return out


def _scan_cl_mintposition_token_ids_by_owner(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    deadline_ts: float | None = None,
    max_ids: int = 120,
    lookback_blocks: int | None = None,
) -> list[int]:
    cid = int(chain_id)
    c = str(contract or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(c) or not _is_eth_address(o):
        return []
    try:
        latest = _eth_block_number(cid)
    except Exception:
        return []
    if latest <= 0:
        return []
    lb = int(lookback_blocks) if lookback_blocks is not None else int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS)
    min_block = max(0, int(latest) - max(1, lb))
    step = int(POSITIONS_ERC721_LOG_BLOCK_STEP)
    # event MintPosition(uint256 indexed tokenId)
    topic_mint = "0x2c0223eed283e194c1112e080d31bdec9e2760ba1454153666cd9d7d6a877964"
    owner_word = _encode_address_word(o)[-40:]
    end_block = int(latest)
    out: list[int] = []
    seen: set[int] = set()
    while end_block >= min_block:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        start_block = max(min_block, end_block - step + 1)
        params = {
            "address": c,
            "fromBlock": hex(int(start_block)),
            "toBlock": hex(int(end_block)),
            "topics": [topic_mint],
        }
        try:
            logs = _eth_get_logs(cid, params, deadline_ts=deadline_ts)
        except Exception:
            if step > 5000:
                step = max(5000, step // 2)
                continue
            logs = []
        logs_sorted = sorted(
            logs,
            key=lambda x: (
                int(str(x.get("blockNumber") or "0x0"), 16),
                int(str(x.get("logIndex") or "0x0"), 16),
            ),
            reverse=True,
        )
        for lg in logs_sorted:
            topics = lg.get("topics") or []
            tid = 0
            try:
                if isinstance(topics, list) and len(topics) >= 2:
                    tid = int(str(topics[1]), 16)
                elif str(lg.get("data") or "").startswith("0x"):
                    words = _hex_words(str(lg.get("data") or "0x"))
                    if words:
                        tid = int(words[0], 16)
            except Exception:
                tid = 0
            if tid <= 0 or tid in seen:
                continue
            seen.add(tid)
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, c, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    continue
            except Exception:
                continue
            out.append(int(tid))
            if len(out) >= int(max_ids):
                return out
        end_block = int(start_block) - 1
    return out


def _explorer_nfttx_rows(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    max_rows: int = 200,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    c = str(contract or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(c) or not _is_eth_address(o):
        return []
    chainid_for_v2 = {
        1: "1",
        10: "10",
        56: "56",
        137: "137",
        8453: "8453",
        42161: "42161",
    }.get(cid, "")
    if not chainid_for_v2:
        return []
    eth_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    bsc_key = os.environ.get("BSCSCAN_API_KEY", "").strip()
    base_key = os.environ.get("BASESCAN_API_KEY", "").strip()
    urls: list[tuple[str, bool]] = []
    offset = max(20, min(1000, int(max_rows)))
    if cid == 56 and bsc_key:
        urls.append((
            "https://api.bscscan.com/api"
            f"?module=account&action=tokennfttx&contractaddress={c}"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={bsc_key}",
            True,
        ))
        urls.append((
            "https://api.bscscan.com/api"
            f"?module=account&action=tokennfttx"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={bsc_key}",
            False,
        ))
    elif cid == 8453 and base_key:
        urls.append((
            "https://api.basescan.org/api"
            f"?module=account&action=tokennfttx&contractaddress={c}"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={base_key}",
            True,
        ))
        urls.append((
            "https://api.basescan.org/api"
            f"?module=account&action=tokennfttx"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={base_key}",
            False,
        ))
    # Secondary fallback via Etherscan V2 unified endpoint.
    if eth_key:
        urls.append((
            "https://api.etherscan.io/v2/api"
            f"?chainid={chainid_for_v2}&module=account&action=tokennfttx"
            f"&contractaddress={c}&address={o}&page=1&offset={offset}&sort=desc&apikey={eth_key}",
            True,
        ))
        urls.append((
            "https://api.etherscan.io/v2/api"
            f"?chainid={chainid_for_v2}&module=account&action=tokennfttx"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={eth_key}",
            False,
        ))
    if not urls:
        return []
    rows: list[dict[str, Any]] = []
    for url, has_contract_filter in urls:
        try:
            req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
            with urlopen(req, timeout=12) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            result_rows = (payload or {}).get("result")
            if isinstance(result_rows, list):
                rows = [x for x in result_rows if isinstance(x, dict)]
                if not has_contract_filter:
                    rows = [
                        x
                        for x in rows
                        if str(
                            x.get("contractAddress")
                            or x.get("contractaddress")
                            or x.get("tokenAddress")
                            or ""
                        ).strip().lower() == c
                    ]
                if rows:
                    break
        except Exception:
            continue
    return rows


def _scan_erc721_token_ids_by_explorer_api(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    max_ids: int = 120,
    protocol: str = "",
) -> list[int]:
    cid = int(chain_id)
    rows = _explorer_nfttx_rows(
        cid,
        contract,
        owner,
        max_rows=max(200, int(max_ids) * 4),
    )
    if not rows:
        return []
    out: list[int] = []
    seen: set[int] = set()
    proto = str(protocol or "").strip().lower()
    min_ts_by_tid: dict[int, int] = {}
    for r in rows:
        tid_raw = str(r.get("tokenID") or "").strip()
        if not tid_raw:
            continue
        try:
            tid = int(tid_raw, 10)
        except Exception:
            try:
                tid = int(tid_raw, 16) if tid_raw.lower().startswith("0x") else int(tid_raw)
            except Exception:
                continue
        if tid <= 0 or tid in seen:
            pass
        else:
            seen.add(tid)
            out.append(int(tid))
        ts = _parse_int_like(r.get("timeStamp") or 0)
        if ts > 0:
            prev = int(min_ts_by_tid.get(int(tid), 0) or 0)
            if prev <= 0 or int(ts) < prev:
                min_ts_by_tid[int(tid)] = int(ts)
        if len(out) >= int(max_ids):
            # keep scanning current rows for earlier timestamps of already collected tokenIds
            continue
    if len(out) > int(max_ids):
        out = out[: int(max_ids)]
    if proto:
        for tid in out:
            ts = int(min_ts_by_tid.get(int(tid), 0) or 0)
            if ts <= 0:
                continue
            try:
                d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
            except Exception:
                d = ""
            if d:
                _position_creation_date_cache_set(cid, proto, tid, d)
    return out


def _explorer_txlist_hashes_for_owner(chain_id: int, owner: str, max_items: int = 400) -> list[str]:
    cid = int(chain_id)
    o = str(owner or "").strip().lower()
    if not _is_eth_address(o):
        return []
    chainid_for_v2 = {
        1: "1",
        10: "10",
        56: "56",
        137: "137",
        8453: "8453",
        42161: "42161",
    }.get(cid, "")
    if not chainid_for_v2:
        return []
    eth_key = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    bsc_key = os.environ.get("BSCSCAN_API_KEY", "").strip()
    base_key = os.environ.get("BASESCAN_API_KEY", "").strip()
    urls: list[str] = []
    offset = max(50, int(max_items))
    if cid == 56 and bsc_key:
        urls.append(
            "https://api.bscscan.com/api"
            f"?module=account&action=txlist&address={o}&page=1&offset={offset}&sort=desc&apikey={bsc_key}"
        )
    elif cid == 8453 and base_key:
        urls.append(
            "https://api.basescan.org/api"
            f"?module=account&action=txlist&address={o}&page=1&offset={offset}&sort=desc&apikey={base_key}"
        )
    # Secondary fallback via Etherscan V2 unified endpoint.
    if eth_key:
        urls.append(
            "https://api.etherscan.io/v2/api"
            f"?chainid={chainid_for_v2}&module=account&action=txlist"
            f"&address={o}&page=1&offset={offset}&sort=desc&apikey={eth_key}"
        )
    if not urls:
        return []
    for url in urls:
        try:
            req = UrlRequest(url, headers={"User-Agent": "uni-fee-web/0.0.2"})
            with urlopen(req, timeout=12) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            rows = (payload or {}).get("result")
            if not isinstance(rows, list):
                continue
            out: list[str] = []
            seen: set[str] = set()
            for r in rows:
                if not isinstance(r, dict):
                    continue
                txh = str(r.get("hash") or "").strip().lower()
                if not (txh.startswith("0x") and len(txh) == 66):
                    continue
                if txh in seen:
                    continue
                seen.add(txh)
                out.append(txh)
                if len(out) >= int(max_items):
                    break
            if out:
                return out
        except Exception:
            continue
    return []


def _scan_cl_token_ids_from_owner_receipts(
    chain_id: int,
    position_manager: str,
    owner: str,
    *,
    deadline_ts: float | None = None,
    max_ids: int = 120,
    max_receipts: int = 220,
) -> tuple[list[int], int]:
    cid = int(chain_id)
    pm = str(position_manager or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(pm) or not _is_eth_address(o):
        return [], 0
    tx_hashes = _explorer_txlist_hashes_for_owner(cid, o, max_items=max_receipts)
    if not tx_hashes:
        return [], 0
    topic_mint = "0x2c0223eed283e194c1112e080d31bdec9e2760ba1454153666cd9d7d6a877964"
    owner_word = _encode_address_word(o)[-40:]
    out: list[int] = []
    seen: set[int] = set()
    checked = 0
    for txh in tx_hashes:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        checked += 1
        try:
            rcpt = _json_rpc_call(cid, "eth_getTransactionReceipt", [txh])
        except Exception:
            continue
        if not isinstance(rcpt, dict):
            continue
        logs = rcpt.get("logs") or []
        if not isinstance(logs, list):
            continue
        for lg in logs:
            if not isinstance(lg, dict):
                continue
            if str(lg.get("address") or "").strip().lower() != pm:
                continue
            topics = lg.get("topics") or []
            if not isinstance(topics, list) or not topics:
                continue
            if str(topics[0]).strip().lower() != topic_mint:
                continue
            tid = 0
            try:
                if len(topics) >= 2:
                    tid = int(str(topics[1]), 16)
                elif str(lg.get("data") or "").startswith("0x"):
                    ws = _hex_words(str(lg.get("data") or "0x"))
                    if ws:
                        tid = int(ws[0], 16)
            except Exception:
                tid = 0
            if tid <= 0 or tid in seen:
                continue
            try:
                owner_hex = _eth_call_hex(cid, pm, "0x6352211e" + _encode_uint_word(tid))
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    continue
            except Exception:
                continue
            seen.add(tid)
            out.append(int(tid))
            if len(out) >= int(max_ids):
                return out, checked
    return out, checked


def _scan_erc721_token_ids_by_ownerof_batch(
    chain_id: int,
    contract: str,
    owner: str,
    *,
    max_ids: int = 120,
    max_checks: int = 800000,
    batch_size: int = 1000,
    deadline_ts: float | None = None,
    start_from_token_id: int = 0,
) -> tuple[list[int], int, int]:
    cid = int(chain_id)
    c = str(contract or "").strip().lower()
    o = str(owner or "").strip().lower()
    if not _is_eth_address(c) or not _is_eth_address(o):
        return [], 0, 0
    try:
        next_token_id = _decode_uint_eth_call(_eth_call_hex(cid, c, "0x75794a3c"))
    except Exception:
        return [], 0, 1
    if next_token_id <= 1:
        return [], 0, 0
    owner_word = _encode_address_word(o)[-40:]
    hi = int(next_token_id) - 1
    if int(start_from_token_id) > 0:
        hi = min(hi, int(start_from_token_id))
    lo = 1
    bsz = max(50, min(1000, int(batch_size)))
    to_check = min(max(1, int(max_checks)), max(0, hi - lo + 1))
    ranges: list[list[int]] = []
    cur = hi
    remaining = int(to_check)
    while cur >= lo and remaining > 0:
        chunk_start = max(lo, cur - bsz + 1)
        tids = list(range(cur, chunk_start - 1, -1))
        if len(tids) > remaining:
            tids = tids[:remaining]
        if not tids:
            break
        ranges.append(tids)
        remaining -= len(tids)
        cur = chunk_start - 1

    def _scan_chunk(tids: list[int]) -> tuple[list[int], int]:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            return [], 0
        payloads = [
            {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "eth_call",
                "params": [{"to": c, "data": "0x6352211e" + _encode_uint_word(int(tid))}, "latest"],
            }
            for idx, tid in enumerate(tids)
        ]
        try:
            resp = _json_rpc_batch_call(cid, payloads, deadline_ts=deadline_ts)
        except Exception:
            return [], len(tids)
        by_id: dict[int, dict[str, Any]] = {}
        for row in resp:
            try:
                rid = int(row.get("id"))
            except Exception:
                continue
            by_id[rid] = row
        found: list[int] = []
        errs = 0
        for idx, tid in enumerate(tids):
            row = by_id.get(idx) or {}
            raw = str(row.get("result") or "").strip().lower()
            if not raw.startswith("0x") or len(raw) < 42:
                if row.get("error") is not None:
                    errs += 1
                continue
            if raw[-40:] == owner_word:
                found.append(int(tid))
        return found, errs

    checked = 0
    errors = 0
    out: list[int] = []
    seen: set[int] = set()
    workers = max(1, min(int(POSITIONS_INFINITY_BATCH_WORKERS), len(ranges)))
    if POSITIONS_DISABLE_PARALLELISM:
        workers = 1
    if workers <= 1:
        for tids in ranges:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            found, errs = _scan_chunk(tids)
            checked += len(tids)
            errors += int(errs)
            for tid in found:
                if tid in seen:
                    continue
                seen.add(tid)
                out.append(tid)
                if len(out) >= int(max_ids):
                    return out, checked, errors
        return out, checked, errors

    ex = ThreadPoolExecutor(max_workers=workers)
    futures = [ex.submit(_scan_chunk, tids) for tids in ranges]
    aborted = False
    pending: set[Any] = set(futures)
    try:
        while pending:
            now = time.monotonic()
            if deadline_ts is not None and now >= deadline_ts:
                aborted = True
                break
            timeout = 0.25
            if deadline_ts is not None:
                timeout = min(timeout, max(0.01, deadline_ts - now))
            done, not_done = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)
            if not done:
                pending = set(not_done)
                continue
            for fut in done:
                try:
                    found, errs = fut.result()
                except Exception:
                    found, errs = [], bsz
                # checked approximation by fixed chunk size is enough for debug/progress.
                checked += bsz
                errors += int(errs)
                for tid in found:
                    if tid in seen:
                        continue
                    seen.add(tid)
                    out.append(tid)
                    if len(out) >= int(max_ids):
                        aborted = True
                        return out, checked, errors
            pending = set(not_done)
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                aborted = True
                break
    finally:
        ex.shutdown(wait=not aborted, cancel_futures=aborted)
    return out, checked, errors


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


def _position_manager_contracts_for_chain(chain_id: int) -> list[str]:
    cid = int(chain_id)
    out: list[str] = []
    contracts = [
        UNISWAP_V3_NPM_BY_CHAIN_ID.get(cid, ""),
        PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID.get(cid, ""),
    ]
    if POSITIONS_ENABLE_INFINITY:
        contracts.extend(
            [
                PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid, ""),
                PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid, ""),
            ]
        )
    for c in contracts:
        cc = str(c or "").strip().lower()
        if _is_eth_address(cc) and cc not in out:
            out.append(cc)
    return out


def _erc721_balance_of(chain_id: int, contract: str, owner: str) -> int | None:
    try:
        data = "0x70a08231" + _encode_address_word(owner)
        return int(_decode_uint_eth_call(_eth_call_hex(int(chain_id), str(contract).strip(), data)))
    except Exception:
        return None


def _chain_has_any_position_nft_balance(chain_id: int, addresses: list[str]) -> bool:
    managers = _position_manager_contracts_for_chain(int(chain_id))
    if not managers:
        return True
    had_unknown = False
    for owner in addresses:
        o = str(owner or "").strip().lower()
        if not _is_eth_address(o):
            continue
        for pm in managers:
            bal = _erc721_balance_of(int(chain_id), pm, o)
            if bal is None:
                had_unknown = True
                continue
            if int(bal) > 0:
                return True
    # Strict mode: treat inconclusive RPC as zero to aggressively skip old-empty chains.
    if POSITIONS_STRICT_ZERO_BALANCE_FILTER:
        return False
    # Relaxed mode: inconclusive precheck should not drop a chain.
    return had_unknown


def _owner_has_any_position_nft_balance(chain_id: int, owner: str) -> bool:
    o = str(owner or "").strip().lower()
    if not _is_eth_address(o):
        return False
    # Only apply strict precheck on chains where we know it helps
    # with Infinity/Pancake load (BSC/Base). For other chains rely
    # on graph/on-chain discovery.
    if int(chain_id) not in (56, 8453):
        return True
    managers = _position_manager_contracts_for_chain(int(chain_id))
    if not managers:
        return True
    had_unknown = False
    for pm in managers:
        bal = _erc721_balance_of(int(chain_id), pm, o)
        if bal is None:
            had_unknown = True
            continue
        if int(bal) > 0:
            return True
    if POSITIONS_STRICT_ZERO_BALANCE_FILTER:
        return False
    return had_unknown


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


def _token_display_symbol_with_source(chain_id: int, chain_key: str, token_obj: dict[str, Any]) -> tuple[str, str]:
    sym = str((token_obj or {}).get("symbol") or "").strip()
    if sym:
        return sym, "token.symbol"
    addr = str((token_obj or {}).get("id") or "").strip().lower()
    if not addr:
        return "?", "missing"
    cfg_sym = (_TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(str(chain_key or "").strip().lower(), {}) or {}).get(addr)
    if cfg_sym:
        return cfg_sym, "curated_map"
    onchain = _fetch_erc20_symbol_onchain(int(chain_id), addr)
    if onchain:
        return onchain, "onchain_symbol"
    return (addr[:8] if len(addr) >= 8 else addr) or "?", "address_fallback"


def _token_display_symbol(chain_id: int, chain_key: str, token_obj: dict[str, Any]) -> str:
    sym, _src = _token_display_symbol_with_source(chain_id, chain_key, token_obj)
    return sym


def _canonical_wrapped_native_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().lower()
    aliases: dict[str, str] = {
        "eth": "eth",
        "weth": "eth",
        "weth.e": "eth",
        "weth9": "eth",
        "bnb": "bnb",
        "wbnb": "bnb",
        "matic": "pol",
        "wmatic": "pol",
        "pol": "pol",
        "wpol": "pol",
        "avax": "avax",
        "wavax": "avax",
        "ftm": "ftm",
        "wftm": "ftm",
        "celo": "celo",
        "wcelo": "celo",
    }
    return aliases.get(s, s)


def _normalize_display_symbol(symbol: str) -> str:
    s = str(symbol or "").strip()
    c = _canonical_wrapped_native_symbol(s)
    if c == "eth":
        return "ETH"
    if c == "bnb":
        return "BNB"
    if c == "pol":
        return "POL"
    if c == "avax":
        return "AVAX"
    if c == "ftm":
        return "FTM"
    if c == "celo":
        return "CELO"
    return s


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
    # Human-like names are a common spam pattern in token symbols
    # (e.g. JamesThomas, MariaWilliams) and should not be shown as pair tickers.
    if re.fullmatch(r"[A-Z][a-z]+(?:[A-Z][a-z]+)+", s):
        return True
    if re.fullmatch(r"[A-Za-z]{8,}", s) and s != s.upper():
        return True
    # Very long all-caps "word" symbols are usually garbage/noise as well.
    if re.fullmatch(r"[A-Z]{13,}", s):
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
    spam0 = _is_probably_spam_symbol(s0)
    spam1 = _is_probably_spam_symbol(s1)
    # In contract-only mode do not blindly trust "curated" addresses:
    # if symbol itself looks spammy, treat as spam early and skip heavy calls.
    if both_curated and not POSITIONS_CONTRACT_ONLY_ENABLED:
        return False
    # If pair tokens are not from curated list, mark clearly suspicious symbols.
    return bool(spam0 or spam1)


def _format_usd_compact(value: float | None) -> str:
    v = _safe_float(value)
    if v <= 0:
        return "-"
    if v < 1:
        return f"${v:.3f}".rstrip("0").rstrip(".")
    if v < 1000:
        return f"${v:,.2f}".rstrip("0").rstrip(".")
    if v < 1_000_000:
        return f"${(v / 1000.0):,.2f}K".rstrip("0").rstrip(".")
    if v < 1_000_000_000:
        return f"${(v / 1_000_000.0):,.2f}M".rstrip("0").rstrip(".")
    if v < 1_000_000_000_000:
        return f"${(v / 1_000_000_000.0):,.2f}B".rstrip("0").rstrip(".")
    return f"${(v / 1_000_000_000_000.0):,.2f}T".rstrip("0").rstrip(".")


def _enrich_rows_liquidity_usd(rows: list[dict[str, Any]], *, max_seconds: int = 4) -> None:
    if not isinstance(rows, list) or not rows:
        return
    deadline_ts = time.monotonic() + max(1, int(max_seconds))
    by_chain_tokens: dict[int, set[str]] = {}
    for r in rows:
        if time.monotonic() >= deadline_ts:
            break
        if bool(r.get("suspected_spam")) or bool(r.get("spam_skipped")):
            r["liquidity_usd"] = None
            r["liquidity_display"] = "-"
            continue
        cid = int(r.get("chain_id") or 0)
        if cid <= 0:
            continue
        t0 = str(r.get("token0_id") or "").strip().lower()
        t1 = str(r.get("token1_id") or "").strip().lower()
        if _is_eth_address(t0):
            by_chain_tokens.setdefault(cid, set()).add(t0)
        if _is_eth_address(t1):
            by_chain_tokens.setdefault(cid, set()).add(t1)
    prices_by_chain: dict[int, dict[str, float]] = {}
    for cid, toks in by_chain_tokens.items():
        if time.monotonic() >= deadline_ts:
            break
        try:
            prices_by_chain[int(cid)] = _get_token_prices_usd(int(cid), sorted(list(toks)))
        except Exception:
            prices_by_chain[int(cid)] = {}
    for r in rows:
        if bool(r.get("suspected_spam")) or bool(r.get("spam_skipped")):
            r["liquidity_usd"] = None
            r["liquidity_display"] = "-"
            continue
        cid = int(r.get("chain_id") or 0)
        if cid <= 0:
            continue
        prices = prices_by_chain.get(int(cid), {})
        if not prices:
            continue
        a0 = _safe_float(r.get("position_amount0"))
        a1 = _safe_float(r.get("position_amount1"))
        t0 = str(r.get("token0_id") or "").strip().lower()
        t1 = str(r.get("token1_id") or "").strip().lower()
        p0 = _safe_float(prices.get(t0))
        p1 = _safe_float(prices.get(t1))
        usd = 0.0
        if a0 > 0 and p0 > 0:
            usd += a0 * p0
        if a1 > 0 and p1 > 0:
            usd += a1 * p1
        if usd > 0:
            r["liquidity_usd"] = float(usd)
            r["liquidity_display"] = _format_usd_compact(float(usd))


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
    debug_out: dict[str, Any] | None = None,
    token_ids_override: list[int] | None = None,
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

    limit = int(POSITIONS_ONCHAIN_MAX_NFTS)
    token_ids_prefetched: list[int] = []
    if isinstance(token_ids_override, list):
        seen_pref: set[int] = set()
        for tid in token_ids_override:
            t = _parse_int_like(tid)
            if t <= 0 or t in seen_pref:
                continue
            seen_pref.add(int(t))
            token_ids_prefetched.append(int(t))
            if len(token_ids_prefetched) >= int(POSITIONS_ONCHAIN_MAX_NFTS):
                break
    if not token_ids_prefetched:
        # balanceOf(address)
        bal_data = "0x70a08231" + _encode_address_word(owner_addr)
        balance = _decode_uint_eth_call(_eth_call_hex(int(chain_id), npm, bal_data))
        if balance <= 0:
            return []
        limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    else:
        limit = min(len(token_ids_prefetched), int(POSITIONS_ONCHAIN_MAX_NFTS))
    out: list[dict[str, Any]] = []
    dbg = debug_out if isinstance(debug_out, dict) else None
    if dbg is not None:
        dbg["scanned_token_ids"] = 0
        dbg["kept_positions"] = 0
        dbg["skipped_zero_liq"] = 0
        dbg["invalid_positions"] = 0

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
        tokens_owed0 = _decode_uint_from_word(pos_words[10]) if len(pos_words) > 10 else 0
        tokens_owed1 = _decode_uint_from_word(pos_words[11]) if len(pos_words) > 11 else 0
        if liq <= 0:
            if dbg is not None:
                dbg["skipped_zero_liq"] = int(dbg.get("skipped_zero_liq") or 0) + 1
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
            "tokensOwed0": str(max(0, int(tokens_owed0))),
            "tokensOwed1": str(max(0, int(tokens_owed1))),
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
    if token_ids_prefetched:
        scan_token_ids = list(token_ids_prefetched[: int(limit)])
    else:
        # Scan latest NFTs first: active user positions are typically near the end.
        start_idx = max(0, int(balance) - limit)
        scan_indices = range(int(balance) - 1, start_idx - 1, -1)
        scan_token_ids = []
        for idx in scan_indices:
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            try:
                # tokenOfOwnerByIndex(address,uint256)
                token_data = "0x2f745c59" + _encode_address_word(owner_addr) + _encode_uint_word(idx)
                token_id = _decode_uint_eth_call(_eth_call_hex(int(chain_id), npm, token_data))
                if token_id <= 0:
                    continue
                scan_token_ids.append(int(token_id))
            except Exception:
                continue

    for token_id in scan_token_ids:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            if dbg is not None:
                dbg["scanned_token_ids"] = int(dbg.get("scanned_token_ids") or 0) + 1
            built = _build_position_from_token_id(int(token_id))
            if built:
                out.append(built)
                if dbg is not None:
                    dbg["kept_positions"] = int(dbg.get("kept_positions") or 0) + 1
            elif dbg is not None:
                dbg["invalid_positions"] = int(dbg.get("invalid_positions") or 0) + 1
        except Exception:
            if dbg is not None:
                dbg["invalid_positions"] = int(dbg.get("invalid_positions") or 0) + 1
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
        tokens_owed0 = _decode_uint_from_word(pos_words[10]) if len(pos_words) > 10 else 0
        tokens_owed1 = _decode_uint_from_word(pos_words[11]) if len(pos_words) > 11 else 0
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
            "tokensOwed0": str(max(0, int(tokens_owed0))),
            "tokensOwed1": str(max(0, int(tokens_owed1))),
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
    debug_out: dict[str, Any] | None = None,
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
    dbg = debug_out if isinstance(debug_out, dict) else None
    if dbg is not None:
        dbg["scanned_token_ids"] = 0
        dbg["kept_positions"] = 0
        dbg["invalid_positions"] = 0
    for idx in scan_indices:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            tok_data = "0x2f745c59" + _encode_address_word(owner_addr) + _encode_uint_word(idx)
            token_id = _decode_uint_eth_call(_eth_call_hex(cid, mc, tok_data))
            if token_id <= 0:
                continue
            if dbg is not None:
                dbg["scanned_token_ids"] = int(dbg.get("scanned_token_ids") or 0) + 1
            pos = _fetch_v3_position_onchain_by_token_id(
                cid,
                str(token_id),
                include_price_details=True,
                protocol_label="pancake_v3",
                source_tag="onchain_pancake_masterchef",
            )
            if pos:
                out.append(pos)
                if dbg is not None:
                    dbg["kept_positions"] = int(dbg.get("kept_positions") or 0) + 1
            elif dbg is not None:
                dbg["invalid_positions"] = int(dbg.get("invalid_positions") or 0) + 1
        except Exception:
            if dbg is not None:
                dbg["invalid_positions"] = int(dbg.get("invalid_positions") or 0) + 1
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
    debug_out: dict[str, Any] | None = None,
) -> list[int]:
    cid = int(chain_id)
    owner = str(owner_addr or "").strip().lower()
    if not _is_eth_address(owner):
        return []
    owner_word = _encode_address_word(owner)[-40:]
    pm = str(position_manager or "").strip().lower()
    if not _is_eth_address(pm):
        return []
    balance = 0
    dbg = debug_out if isinstance(debug_out, dict) else None
    if dbg is not None:
        dbg.clear()
        dbg.update(
            {
                "pm": pm,
                "balance": 0,
                "enumerable_ok": False,
                "enumerable_token_ids": 0,
                "log_token_ids": 0,
                "ownerof_checked_from_logs": 0,
                "ownerof_matched_from_logs": 0,
                "ownerof_mismatched_from_logs": 0,
                "ownerof_errors_from_logs": 0,
                "deep_log_token_ids": 0,
                "recent_transfer_token_ids": 0,
                "recent_transfer_ownerof_checked": 0,
                "recent_transfer_ownerof_matched": 0,
                "explorer_token_ids": 0,
                "explorer_ownerof_checked": 0,
                "explorer_ownerof_matched": 0,
                "mint_log_ids": 0,
                "mint_log_ownerof_checked": 0,
                "mint_log_ownerof_matched": 0,
                "receipt_mint_ids": 0,
                "receipt_checked": 0,
                "total_supply": 0,
                "tokenbyindex_checked": 0,
                "tokenbyindex_matched": 0,
                "tokenbyindex_errors": 0,
                "batch_ownerof_checked": 0,
                "batch_ownerof_matched": 0,
                "batch_ownerof_errors": 0,
                "rpc_ms_balance": 0,
                "rpc_ms_logs": 0,
                "rpc_ms_batch_ownerof": 0,
                "rpc_getlogs_requests": 0,
                "rpc_getlogs_attempts": 0,
                "rpc_getlogs_success": 0,
                "rpc_getlogs_failures": 0,
                "rpc_getlogs_first_try_fail": 0,
                "rpc_getlogs_retry_success": 0,
                "rpc_getlogs_ms": 0,
                "rpc_getlogs_last_error": "",
                "owner_scan_checked": 0,
                "owner_scan_matched": 0,
                "owner_scan_errors": 0,
                "final_token_ids": 0,
                "early_exit_no_balance": False,
            }
        )
    t_balance0 = time.perf_counter()
    try:
        bal_data = "0x70a08231" + _encode_address_word(owner)
        balance = _decode_uint_eth_call(_eth_call_hex(cid, pm, bal_data))
    except Exception:
        balance = 0
    if dbg is not None:
        dbg["rpc_ms_balance"] = int(max(0.0, (time.perf_counter() - t_balance0) * 1000.0))
    if dbg is not None:
        dbg["balance"] = int(balance)
    if int(balance) <= 0:
        # Avoid expensive log scans when contract reports no owned NFTs.
        # This keeps overall pool scan responsive and prevents global timeouts.
        if dbg is not None:
            dbg["early_exit_no_balance"] = True
        return []
    limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    token_ids: list[int] = []
    seen_ids: set[int] = set()
    # Infinity position managers use Solmate ERC721 (non-enumerable), so tokenOfOwnerByIndex is not available.
    enumerable_ok = False
    if dbg is not None:
        dbg["enumerable_ok"] = bool(enumerable_ok)
        dbg["enumerable_token_ids"] = len(token_ids)
    if len(token_ids) < limit and POSITIONS_INFINITY_BATCH_SCAN and not POSITIONS_CONTRACT_ONLY_ENABLED:
        t_batch0 = time.perf_counter()
        batch_ids, batch_checked, batch_errors = _scan_erc721_token_ids_by_ownerof_batch(
            cid,
            pm,
            owner,
            max_ids=max(limit * 2, limit),
            max_checks=int(POSITIONS_INFINITY_BATCH_MAX_CHECKS),
            batch_size=int(POSITIONS_INFINITY_BATCH_SIZE),
            deadline_ts=deadline_ts,
        )
        if dbg is not None:
            dbg["batch_ownerof_checked"] = int(batch_checked)
            dbg["batch_ownerof_errors"] = int(batch_errors)
            dbg["batch_ownerof_matched"] = int(len(batch_ids))
            dbg["rpc_ms_batch_ownerof"] = int(max(0.0, (time.perf_counter() - t_batch0) * 1000.0))
        for tid in batch_ids:
            if len(token_ids) >= limit:
                break
            if int(tid) in seen_ids:
                continue
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
    if POSITIONS_CONTRACT_ONLY_ENABLED:
        # Stage 1 (fast): explorer NFT transfers for candidate token IDs.
        # Keep final ownership strict via direct ownerOf verification.
        proto_hint = ""
        if pm == str(PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid, "") or "").strip().lower():
            proto_hint = "pancake_infinity_cl"
        elif pm == str(PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid, "") or "").strip().lower():
            proto_hint = "pancake_infinity_bin"
        if len(token_ids) < limit:
            explorer_ids = _scan_erc721_token_ids_by_explorer_api(
                cid,
                pm,
                owner,
                max_ids=max(limit * 4, limit),
                protocol=proto_hint,
            )
            if dbg is not None:
                dbg["explorer_token_ids"] = int(len(explorer_ids))
            for tid in explorer_ids:
                if len(token_ids) >= limit:
                    break
                if int(tid) in seen_ids:
                    continue
                if dbg is not None:
                    dbg["explorer_ownerof_checked"] = int(dbg.get("explorer_ownerof_checked") or 0) + 1
                try:
                    owner_data = "0x6352211e" + _encode_uint_word(int(tid))
                    owner_hex = _eth_call_hex(cid, pm, owner_data)
                    owner_words = _hex_words(owner_hex)
                    if not owner_words:
                        continue
                    current_owner = owner_words[0][-40:].lower()
                    if current_owner != owner_word:
                        continue
                except Exception:
                    continue
                if dbg is not None:
                    dbg["explorer_ownerof_matched"] = int(dbg.get("explorer_ownerof_matched") or 0) + 1
                seen_ids.add(int(tid))
                token_ids.append(int(tid))
        # Stage 2 fallback: direct Transfer(to=owner) logs (contract-native discovery).
        if len(token_ids) < limit:
            t_logs0 = time.perf_counter()
            log_ids = _scan_erc721_token_ids_by_incoming_logs(
                cid,
                pm,
                owner,
                deadline_ts=deadline_ts,
                max_ids=max(limit * 4, limit),
                lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 20_000_000),
                debug_out=dbg,
            )
            if dbg is not None:
                dbg["log_token_ids"] = len(log_ids)
                dbg["rpc_ms_logs"] = int(max(0.0, (time.perf_counter() - t_logs0) * 1000.0))
            for tid in log_ids:
                if len(token_ids) >= limit:
                    break
                if int(tid) in seen_ids:
                    continue
                seen_ids.add(int(tid))
                token_ids.append(int(tid))
        if len(token_ids) < limit and POSITIONS_INFINITY_BATCH_SCAN:
            t_batch0 = time.perf_counter()
            batch_ids, batch_checked, batch_errors = _scan_erc721_token_ids_by_ownerof_batch(
                cid,
                pm,
                owner,
                max_ids=max(limit * 4, limit),
                max_checks=int(POSITIONS_INFINITY_BATCH_MAX_CHECKS),
                batch_size=int(POSITIONS_INFINITY_BATCH_SIZE),
                deadline_ts=deadline_ts,
            )
            if dbg is not None:
                dbg["batch_ownerof_checked"] = int((dbg.get("batch_ownerof_checked") or 0) + int(batch_checked))
                dbg["batch_ownerof_errors"] = int((dbg.get("batch_ownerof_errors") or 0) + int(batch_errors))
                dbg["batch_ownerof_matched"] = int((dbg.get("batch_ownerof_matched") or 0) + len(batch_ids))
                dbg["rpc_ms_batch_ownerof"] = int((dbg.get("rpc_ms_batch_ownerof") or 0) + int(max(0.0, (time.perf_counter() - t_batch0) * 1000.0)))
            for tid in batch_ids:
                if len(token_ids) >= limit:
                    break
                if int(tid) in seen_ids:
                    continue
                seen_ids.add(int(tid))
                token_ids.append(int(tid))
        if dbg is not None:
            dbg["final_token_ids"] = len(token_ids)
        return token_ids
    if not POSITIONS_INFINITY_HEAVY_METHODS:
        if dbg is not None:
            dbg["final_token_ids"] = len(token_ids)
        return token_ids
    if len(token_ids) < limit:
        # Wallets often discover Infinity NFTs by Transfer logs.
        owner_word = _encode_address_word(owner)[-40:]
        log_ids = _scan_erc721_token_ids_by_incoming_logs(
            cid,
            pm,
            owner,
            deadline_ts=deadline_ts,
            max_ids=max(limit * 3, limit),
            debug_out=dbg,
        )
        if dbg is not None:
            dbg["log_token_ids"] = len(log_ids)
        for tid in log_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            if dbg is not None:
                dbg["ownerof_checked_from_logs"] = int(dbg.get("ownerof_checked_from_logs") or 0) + 1
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, pm, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    if dbg is not None:
                        dbg["ownerof_errors_from_logs"] = int(dbg.get("ownerof_errors_from_logs") or 0) + 1
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    if dbg is not None:
                        dbg["ownerof_mismatched_from_logs"] = int(dbg.get("ownerof_mismatched_from_logs") or 0) + 1
                    continue
            except Exception:
                if dbg is not None:
                    dbg["ownerof_errors_from_logs"] = int(dbg.get("ownerof_errors_from_logs") or 0) + 1
                continue
            if dbg is not None:
                dbg["ownerof_matched_from_logs"] = int(dbg.get("ownerof_matched_from_logs") or 0) + 1
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
    if len(token_ids) < limit and balance > 0:
        # If balance>0 but neither enumerable nor regular recent logs yielded ids,
        # run a deeper log lookup over a wider history window.
        owner_word = _encode_address_word(owner)[-40:]
        deep_ids = _scan_erc721_token_ids_by_incoming_logs(
            cid,
            pm,
            owner,
            deadline_ts=deadline_ts,
            max_ids=max(limit * 10, limit),
            lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 20_000_000),
            debug_out=dbg,
        )
        if dbg is not None:
            dbg["deep_log_token_ids"] = len(deep_ids)
        for tid in deep_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            if dbg is not None:
                dbg["ownerof_checked_from_logs"] = int(dbg.get("ownerof_checked_from_logs") or 0) + 1
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, pm, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    if dbg is not None:
                        dbg["ownerof_errors_from_logs"] = int(dbg.get("ownerof_errors_from_logs") or 0) + 1
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    if dbg is not None:
                        dbg["ownerof_mismatched_from_logs"] = int(dbg.get("ownerof_mismatched_from_logs") or 0) + 1
                    continue
            except Exception:
                if dbg is not None:
                    dbg["ownerof_errors_from_logs"] = int(dbg.get("ownerof_errors_from_logs") or 0) + 1
                continue
            if dbg is not None:
                dbg["ownerof_matched_from_logs"] = int(dbg.get("ownerof_matched_from_logs") or 0) + 1
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
    if len(token_ids) < limit and balance > 0:
        # Independent fallback: walk recent Transfer logs for this NFT contract
        # and validate candidates via ownerOf, without filtering logs by owner.
        recent_ids = _scan_erc721_token_ids_by_recent_transfers_ownerof(
            cid,
            pm,
            owner,
            deadline_ts=deadline_ts,
            max_ids=max(limit * 8, limit),
            lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 30_000_000),
        )
        if dbg is not None:
            dbg["recent_transfer_token_ids"] = len(recent_ids)
            dbg["recent_transfer_ownerof_checked"] = len(recent_ids)
        for tid in recent_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
            if dbg is not None:
                dbg["recent_transfer_ownerof_matched"] = int(dbg.get("recent_transfer_ownerof_matched") or 0) + 1
    if len(token_ids) < limit and balance > 0:
        # Final fallback: explorer NFT tx index (BscScan/BaseScan style APIs).
        owner_word = _encode_address_word(owner)[-40:]
        explorer_ids = _scan_erc721_token_ids_by_explorer_api(
            cid,
            pm,
            owner,
            max_ids=max(limit * 8, limit),
        )
        if dbg is not None:
            dbg["explorer_token_ids"] = len(explorer_ids)
        for tid in explorer_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            if dbg is not None:
                dbg["explorer_ownerof_checked"] = int(dbg.get("explorer_ownerof_checked") or 0) + 1
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, pm, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner != owner_word:
                    continue
            except Exception:
                continue
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
            if dbg is not None:
                dbg["explorer_ownerof_matched"] = int(dbg.get("explorer_ownerof_matched") or 0) + 1
    if len(token_ids) < limit and balance > 0:
        # CL-specific event fallback: MintPosition(tokenId) + ownerOf verification.
        mint_ids = _scan_cl_mintposition_token_ids_by_owner(
            cid,
            pm,
            owner,
            deadline_ts=deadline_ts,
            max_ids=max(limit * 8, limit),
            lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 30_000_000),
        )
        if dbg is not None:
            dbg["mint_log_ids"] = len(mint_ids)
            dbg["mint_log_ownerof_checked"] = len(mint_ids)
        for tid in mint_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
            if dbg is not None:
                dbg["mint_log_ownerof_matched"] = int(dbg.get("mint_log_ownerof_matched") or 0) + 1
    if len(token_ids) < limit and balance > 0:
        # Explorer txlist + receipt logs fallback for providers that heavily limit eth_getLogs.
        receipt_ids, receipt_checked = _scan_cl_token_ids_from_owner_receipts(
            cid,
            pm,
            owner,
            deadline_ts=deadline_ts,
            max_ids=max(limit * 8, limit),
            max_receipts=220,
        )
        if dbg is not None:
            dbg["receipt_mint_ids"] = len(receipt_ids)
            dbg["receipt_checked"] = int(receipt_checked)
        for tid in receipt_ids:
            if len(token_ids) >= limit:
                break
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if int(tid) in seen_ids:
                continue
            seen_ids.add(int(tid))
            token_ids.append(int(tid))
    if token_ids or enumerable_ok:
        if dbg is not None:
            dbg["final_token_ids"] = len(token_ids)
        return token_ids
    # Last resort: scan recent IDs by ownerOf.
    # This path is bounded by attempt/error counters, not local timers.
    if deadline_ts is not None and deadline_ts <= time.monotonic():
        return token_ids
    already_owner_scanned = int((dbg or {}).get("owner_scan_checked") or 0) > 0
    next_token_id = 0
    if not already_owner_scanned:
        try:
            next_token_id = _decode_uint_eth_call(_eth_call_hex(cid, pm, "0x75794a3c"))
        except Exception:
            next_token_id = 0
    if next_token_id > 0 and not already_owner_scanned:
        lookback = max(POSITIONS_INFINITY_OWNER_LOOKBACK, limit * 40, 5000)
        start_tid = max(1, int(next_token_id) - int(lookback))
        owner_word = _encode_address_word(owner)[-40:]
        max_checks = int(POSITIONS_INFINITY_OWNER_SCAN_MAX_CHECKS)
        max_errors = int(POSITIONS_INFINITY_OWNER_SCAN_MAX_ERRORS)
        local_checked = 0
        local_errors = 0
        for tid in range(int(next_token_id) - 1, start_tid - 1, -1):
            if deadline_ts is not None and time.monotonic() >= deadline_ts:
                break
            if len(token_ids) >= limit:
                break
            if local_checked >= max_checks or local_errors >= max_errors:
                break
            local_checked += 1
            if dbg is not None:
                dbg["owner_scan_checked"] = int(dbg.get("owner_scan_checked") or 0) + 1
            try:
                owner_data = "0x6352211e" + _encode_uint_word(tid)
                owner_hex = _eth_call_hex(cid, pm, owner_data)
                owner_words = _hex_words(owner_hex)
                if not owner_words:
                    local_errors += 1
                    if dbg is not None:
                        dbg["owner_scan_errors"] = int(dbg.get("owner_scan_errors") or 0) + 1
                    continue
                current_owner = owner_words[0][-40:].lower()
                if current_owner == owner_word:
                    token_ids.append(int(tid))
                    if dbg is not None:
                        dbg["owner_scan_matched"] = int(dbg.get("owner_scan_matched") or 0) + 1
            except Exception:
                local_errors += 1
                if dbg is not None:
                    dbg["owner_scan_errors"] = int(dbg.get("owner_scan_errors") or 0) + 1
                continue
    elif len(token_ids) < limit:
        # Fallback for contracts exposing ERC721Enumerable tokenByIndex/totalSupply,
        # even when nextTokenId/tokenOfOwnerByIndex are unavailable.
        total_supply = 0
        try:
            total_supply = _decode_uint_eth_call(_eth_call_hex(cid, pm, "0x18160ddd"))
        except Exception:
            total_supply = 0
        if dbg is not None:
            dbg["total_supply"] = int(total_supply)
        if total_supply > 0:
            scan_cnt = min(int(total_supply), max(POSITIONS_INFINITY_OWNER_LOOKBACK, limit * 200, 10000))
            start_idx = max(0, int(total_supply) - scan_cnt)
            owner_word = _encode_address_word(owner)[-40:]
            for idx in range(int(total_supply) - 1, start_idx - 1, -1):
                if deadline_ts is not None and time.monotonic() >= deadline_ts:
                    break
                if len(token_ids) >= limit:
                    break
                if dbg is not None:
                    dbg["tokenbyindex_checked"] = int(dbg.get("tokenbyindex_checked") or 0) + 1
                try:
                    tid = _decode_uint_eth_call(_eth_call_hex(cid, pm, "0x4f6ccce7" + _encode_uint_word(idx)))
                    if tid <= 0 or int(tid) in seen_ids:
                        continue
                    owner_data = "0x6352211e" + _encode_uint_word(tid)
                    owner_hex = _eth_call_hex(cid, pm, owner_data)
                    owner_words = _hex_words(owner_hex)
                    if not owner_words:
                        if dbg is not None:
                            dbg["tokenbyindex_errors"] = int(dbg.get("tokenbyindex_errors") or 0) + 1
                        continue
                    current_owner = owner_words[0][-40:].lower()
                    if current_owner != owner_word:
                        continue
                    seen_ids.add(int(tid))
                    token_ids.append(int(tid))
                    if dbg is not None:
                        dbg["tokenbyindex_matched"] = int(dbg.get("tokenbyindex_matched") or 0) + 1
                except Exception:
                    if dbg is not None:
                        dbg["tokenbyindex_errors"] = int(dbg.get("tokenbyindex_errors") or 0) + 1
                    continue
    if dbg is not None:
        dbg["final_token_ids"] = len(token_ids)
    return token_ids


def _scan_pancake_infinity_cl_positions_onchain(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
    debug_out: dict[str, Any] | None = None,
    token_ids_override: list[int] | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    pm = PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(cid)
    if not pm:
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    if token_ids_override:
        token_ids = [int(x) for x in token_ids_override if _parse_int_like(x) > 0]
    else:
        token_ids = _scan_infinity_position_ids_for_owner(pm, owner_addr, cid, deadline_ts=deadline_ts, debug_out=debug_out)
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
    debug_out: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    pm = PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(cid)
    if not pm:
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    token_ids = _scan_infinity_position_ids_for_owner(pm, owner_addr, cid, deadline_ts=deadline_ts, debug_out=debug_out)
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


def _decode_signed_int24_from_packed_word(word_value: int, shift_bits: int) -> int:
    raw = int((int(word_value) >> int(shift_bits)) & 0xFFFFFF)
    return raw - 0x1000000 if raw >= 0x800000 else raw


def _scan_uniswap_v4_positions_onchain(
    owner: str,
    chain_id: int,
    *,
    deadline_ts: float | None = None,
) -> list[dict[str, Any]]:
    cid = int(chain_id)
    pm = str(UNISWAP_V4_POSITION_MANAGER_BY_CHAIN_ID.get(cid) or "").strip().lower()
    if not _is_eth_address(pm):
        return []
    owner_addr = str(owner or "").strip().lower()
    if not _is_eth_address(owner_addr):
        return []
    if deadline_ts is not None and time.monotonic() >= deadline_ts:
        return []

    # Run only if owner has v4 NFTs.
    try:
        bal_data = "0x70a08231" + _encode_address_word(owner_addr)
        balance = _decode_uint_eth_call(_eth_call_hex(cid, pm, bal_data))
    except Exception:
        return []
    if int(balance) <= 0:
        return []

    limit = min(int(balance), POSITIONS_ONCHAIN_MAX_NFTS)
    # v4 PositionManager is non-enumerable: discover tokenIds via Transfer(to=owner) logs.
    token_ids = _scan_erc721_token_ids_by_incoming_logs(
        cid,
        pm,
        owner_addr,
        deadline_ts=deadline_ts,
        max_ids=max(limit * 4, limit),
        lookback_blocks=max(int(POSITIONS_ERC721_LOG_LOOKBACK_BLOCKS), 20_000_000),
    )
    if not token_ids:
        return []

    out: list[dict[str, Any]] = []
    owner_word = _encode_address_word(owner_addr)[-40:]
    sel_pool_and_info = "0x7ba03aad"  # getPoolAndPositionInfo(uint256)
    sel_liq = "0x1efeed33"  # getPositionLiquidity(uint256)
    for token_id in token_ids:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break
        try:
            owner_hex = _eth_call_hex(cid, pm, "0x6352211e" + _encode_uint_word(int(token_id)))
            owner_words = _hex_words(owner_hex)
            if not owner_words or owner_words[0][-40:].lower() != owner_word:
                continue

            batch = _eth_call_hex_batch(
                cid,
                [
                    {"to": pm, "data": sel_pool_and_info + _encode_uint_word(int(token_id))},
                    {"to": pm, "data": sel_liq + _encode_uint_word(int(token_id))},
                ],
            )
            p_words = _hex_words(batch[0] or "")
            if len(p_words) < 6:
                continue
            liq = _decode_uint_eth_call(batch[1] or "0x0")
            if int(liq) <= 0:
                continue

            raw0 = _decode_address_from_word(p_words[0])
            raw1 = _decode_address_from_word(p_words[1])
            fee = _decode_uint_from_word(p_words[2])
            tick_spacing = _decode_int_from_word(p_words[3])
            hooks = _decode_address_from_word(p_words[4]).lower()
            info_val = int(p_words[5], 16)
            tick_upper = _decode_signed_int24_from_packed_word(info_val, 32)
            tick_lower = _decode_signed_int24_from_packed_word(info_val, 8)

            token0_addr, token0_sym, dec0 = _normalize_infinity_currency(cid, raw0)
            token1_addr, token1_sym, dec1 = _normalize_infinity_currency(cid, raw1)
            poolkey_blob = bytes.fromhex("".join(p_words[:5]))
            pool_id_hex = _keccak256_hex(poolkey_blob) or ("0x" + _encode_uint_word(int(token_id)))

            out.append(
                {
                    "id": str(int(token_id)),
                    "liquidity": str(int(liq)),
                    "tickLower": {"tickIdx": str(int(tick_lower))},
                    "tickUpper": {"tickIdx": str(int(tick_upper))},
                    "pool": {
                        "id": str(pool_id_hex).lower(),
                        "feeTier": str(int(fee)),
                        "liquidity": str(int(liq)),
                        "sqrtPrice": "0",
                        "token0Price": "0",
                        "totalValueLockedUSD": "0",
                        "totalValueLockedToken0": "0",
                        "totalValueLockedToken1": "0",
                        "token0": {"id": token0_addr, "decimals": str(dec0), "symbol": token0_sym},
                        "token1": {"id": token1_addr, "decimals": str(dec1), "symbol": token1_sym},
                        "tickSpacing": str(int(tick_spacing)),
                        "hooks": str(hooks or ""),
                    },
                    "_protocol_label": "uniswap_v4",
                    "_source": "onchain_uniswap_v4_pm",
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
    light_mode: bool = False,
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
        ]
        # snapshots* queries are intentionally disabled: they are expensive and noisy.
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
                        "positions_rel_in",
                        f"""
                        query UserPositions($owner: {owner_type}!, $skip: Int!) {{
                          positions(first: 200, skip: $skip, where: {{ owner_: {{ id_in: [$owner] }} }}) {{ id }}
                        }}
                        """,
                    )
                ]
            )
        return queries

    owner_raw = str(owner or "").strip()
    owner_lc = owner_raw.lower()
    owner_candidates: list[str] = []
    owner_no_prefix = owner_raw[2:] if owner_raw.startswith("0x") else owner_raw
    owner_no_prefix_lc = owner_no_prefix.lower()
    candidates = (owner_raw, owner_lc) if light_mode else (owner_raw, owner_lc, owner_no_prefix, owner_no_prefix_lc)
    for candidate in candidates:
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
        return out

    # Fast profile by default: prefer filters that work across most indexers.
    owner_types_primary = ["ID", "String"]
    if POSITIONS_TRY_BYTES_TYPE:
        owner_types_primary.append("Bytes")
    query_sets_primary = [(x, _build_id_queries(x, extended=False)) for x in owner_types_primary]
    query_sets_extended = [] if light_mode else [(x, _build_id_queries(x, extended=True)) for x in ["ID", "String", "Bytes"]]
    attempts_count = 0
    found_ids: list[str] = []
    for owner_value in owner_candidates:
        if deadline_ts is not None and time.monotonic() >= deadline_ts:
            break

        def _run_query_sets(query_sets: list[tuple[str, list[tuple[str, str]]]]) -> bool:
            nonlocal attempts_count, found_ids
            for owner_type, queries in query_sets:
                if deadline_ts is not None and time.monotonic() >= deadline_ts:
                    return False
                for mode, q in queries:
                    if found_ids:
                        return True
                    if deadline_ts is not None and time.monotonic() >= deadline_ts:
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
        if query_sets_extended and not found_ids and POSITIONS_EXTENDED_QUERY_FALLBACK and (deadline_ts is None or time.monotonic() < deadline_ts):
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
    if POSITIONS_SKIP_PER_ID_DETAIL_FETCH:
        return details
    fetched_ids = {str(x.get("id") or "").strip() for x in details if isinstance(x, dict)}
    # Optional per-id fallback is expensive; keep it disabled by default for speed.
    if not POSITIONS_SKIP_PER_ID_DETAIL_FETCH:
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
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
    progress_out: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    timed_out = False
    chain_key = CHAIN_ID_TO_KEY.get(int(chain_id), "")
    if not chain_key:
        return rows, errors, debug_rows, timed_out

    progress_slot = progress_out if isinstance(progress_out, dict) else None

    def _set_chain_progress(stage: str, **extra: Any) -> None:
        if not isinstance(progress_slot, dict):
            return
        progress_slot["chain"] = str(chain_key)
        progress_slot["chain_id"] = int(chain_id)
        progress_slot["stage"] = str(stage or "")
        progress_slot["ts"] = float(time.monotonic())
        for k, v in (extra or {}).items():
            progress_slot[str(k)] = v

    _set_chain_progress("init", status="running", owners_total=len(addresses))
    owner_has_nft_balance: dict[str, bool] = {}
    if not hard_scan:
        for owner in addresses:
            owner_has_nft_balance[str(owner).strip().lower()] = True
    # Owner-level NFT balance precheck stays only for hard scan.
    elif int(chain_id) in (56, 8453):
        for owner in addresses:
            owner_has_nft_balance[str(owner).strip().lower()] = _owner_has_any_position_nft_balance(int(chain_id), owner)
    else:
        for owner in addresses:
            owner_has_nft_balance[str(owner).strip().lower()] = True
    # Guard sync warmup from running repeatedly for the same owner/version
    # when owner scans are processed in parallel workers.
    sync_warmup_seen: set[tuple[str, str]] = set()
    sync_warmup_lock = threading.Lock()

    def _claim_sync_warmup(owner: str, version: str) -> bool:
        key = (str(owner).strip().lower(), str(version).strip().lower())
        with sync_warmup_lock:
            if key in sync_warmup_seen:
                return False
            sync_warmup_seen.add(key)
            return True

    def _run_owner_legacy_infinity_discovery(
        owner: str,
        *,
        version: str,
        positions: list[dict[str, Any]],
        owner_attempts: list[dict[str, Any]],
        deadline_ts: float,
    ) -> list[dict[str, Any]]:
        if version != "v3":
            return positions
        # Run Pancake Infinity at the very end so it cannot starve normal v3/v4 discovery.
        if int(chain_id) in PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID:
            infinity_cl_debug: dict[str, Any] = {}
            index_positions: list[dict[str, Any]] = []
            try:
                idx_cfg = _indexer_get("infinity_bsc")
                idx_enabled = bool(idx_cfg.get("enabled")) and str(idx_cfg.get("mode") or "off") != "off"
                if idx_enabled and int(chain_id) in {56, 8453} and time.monotonic() < deadline_ts:
                    # Request thread does read-only from index. Heavy index updates are handled by background worker.
                    if not pre_enqueued_ownership_refresh:
                        _position_enqueue_ownership_refresh(int(chain_id), owner)
                    indexed_token_ids = _infinity_index_get_token_ids(int(chain_id), owner, limit=POSITIONS_ONCHAIN_MAX_NFTS)
                    infinity_cl_debug["indexer_token_ids"] = len(indexed_token_ids)
                    if indexed_token_ids:
                        index_positions = _scan_pancake_infinity_cl_positions_onchain(
                            owner,
                            int(chain_id),
                            deadline_ts=deadline_ts,
                            debug_out=infinity_cl_debug,
                            token_ids_override=indexed_token_ids,
                        )
            except Exception as e:
                infinity_cl_debug["indexer_error"] = str(e)[:220]
            if index_positions:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "indexer",
                        "query_mode": "indexer_pancake_infinity_cl",
                        "count": len(index_positions),
                        "ok": True,
                        "infinity_debug": infinity_cl_debug,
                    }
                )
                seen = {str(x.get("id") or "") for x in positions if isinstance(x, dict)}
                for p in index_positions:
                    pid = str((p or {}).get("id") or "")
                    if pid and pid in seen:
                        continue
                    if pid:
                        seen.add(pid)
                    positions.append(p)
            graph_positions: list[dict[str, Any]] = []
            try:
                if not index_positions:
                    graph_positions = _scan_pancake_infinity_cl_positions_graph(
                        owner,
                        int(chain_id),
                        deadline_ts=deadline_ts,
                    )
            except Exception as e:
                infinity_cl_debug["graph_error"] = str(e)[:220]
            if graph_positions:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "graph",
                        "query_mode": "graph_pancake_infinity_cl",
                        "count": len(graph_positions),
                        "ok": True,
                        "infinity_debug": infinity_cl_debug,
                    }
                )
                seen = {str(x.get("id") or "") for x in positions if isinstance(x, dict)}
                for p in graph_positions:
                    pid = str((p or {}).get("id") or "")
                    if pid and pid in seen:
                        continue
                    if pid:
                        seen.add(pid)
                    positions.append(p)

        if int(chain_id) in PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID:
            try:
                infinity_bin_debug: dict[str, Any] = {}
                infinity_bin_positions = _scan_pancake_infinity_bin_positions_onchain(
                    owner,
                    int(chain_id),
                    deadline_ts=deadline_ts,
                    debug_out=infinity_bin_debug,
                )
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "onchain_pancake_infinity_bin",
                        "count": len(infinity_bin_positions),
                        "ok": True,
                        "infinity_debug": infinity_bin_debug,
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
        return positions

    def _run_owner_legacy_core_discovery(
        owner: str,
        *,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        has_position_liquidity: bool,
        owner_has_nft: bool,
        positions: list[dict[str, Any]],
        index_cache_hit: bool,
        owner_attempts: list[dict[str, Any]],
        owner_errors: list[str],
        deadline_ts: float,
    ) -> list[dict[str, Any]]:
        graph_failed = False
        if (
            version == "v3"
            and int(chain_id) in UNISWAP_V3_NPM_BY_CHAIN_ID
            and int(chain_id) in POSITIONS_ONCHAIN_PREFETCH_CHAIN_IDS
            and not POSITIONS_DISABLE_V3_PREFETCH
            and not index_cache_hit
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
        if version == "v3" and int(chain_id) in PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID and not index_cache_hit:
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

        if endpoint and not positions:
            if version == "v3" and not owner_has_nft:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "onchain",
                        "query_mode": "graph_skipped_no_nft_balance",
                        "count": 0,
                        "ok": True,
                    }
                )
            else:
                try:
                    positions = _query_uniswap_positions_for_owner(
                        endpoint,
                        owner,
                        include_pool_liquidity=has_pool_liquidity,
                        include_position_liquidity=has_position_liquidity,
                        debug_steps=owner_attempts,
                        deadline_ts=deadline_ts,
                        light_mode=POSITIONS_LIGHT_GRAPH_QUERIES,
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
                            light_mode=POSITIONS_LIGHT_GRAPH_QUERIES,
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
        if version == "v3" and (graph_failed or not positions) and not POSITIONS_DISABLE_V3_ONCHAIN_FALLBACK:
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
        return positions

    def _persist_positions_to_ownership_index(
        owner: str,
        *,
        version: str,
        positions: list[dict[str, Any]],
    ) -> None:
        if version not in {"v3", "v4"} or not POSITIONS_OWNERSHIP_INDEX_ENABLED or not positions:
            return
        by_protocol: dict[str, list[int]] = {}
        for p in positions:
            if not isinstance(p, dict):
                continue
            pid = _parse_int_like(p.get("id"))
            if pid <= 0:
                continue
            default_proto = "uniswap_v4" if version == "v4" else V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(int(chain_id), "uniswap_v3")
            proto = str(p.get("_protocol_label") or default_proto).strip().lower()
            if not proto:
                proto = "uniswap_v4" if version == "v4" else "uniswap_v3"
            by_protocol.setdefault(proto, []).append(pid)
            _position_details_cache_upsert(int(chain_id), proto, p)
        for proto, token_ids in by_protocol.items():
            manager = ""
            if proto == "pancake_v3_staked":
                manager = str(PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID.get(int(chain_id), "") or "")
            elif proto == "pancake_infinity_cl":
                manager = str(PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID.get(int(chain_id), "") or "")
            elif proto == "pancake_infinity_bin":
                manager = str(PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID.get(int(chain_id), "") or "")
            elif proto == "uniswap_v4":
                manager = ""
            else:
                manager = str(UNISWAP_V3_NPM_BY_CHAIN_ID.get(int(chain_id), "") or "")
            _position_ownership_upsert(
                int(chain_id),
                owner,
                proto,
                manager,
                token_ids,
                source="scan_passive",
            )

    def _build_owner_compare_debug(
        positions: list[dict[str, Any]],
        owner_attempts: list[dict[str, Any]],
    ) -> dict[str, Any]:
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
        return {
            "graph_checked": bool(graph_checked),
            "onchain_count": len(onchain_ids),
            "graph_count": len(graph_ids),
            "only_onchain_count": len(onchain_ids - graph_ids) if graph_checked else 0,
            "only_graph_count": len(graph_ids - onchain_ids) if graph_checked else 0,
            "only_onchain_ids": sorted([x for x in (onchain_ids - graph_ids) if x][:5]) if graph_checked else [],
            "only_graph_ids": sorted([x for x in (graph_ids - onchain_ids) if x][:5]) if graph_checked else [],
        }

    def _make_owner_debug(
        owner: str,
        version: str,
        positions: list[dict[str, Any]],
        owner_attempts: list[dict[str, Any]],
        *,
        compare: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        attempts_for_payload = owner_attempts if (hard_scan or POSITIONS_CONTRACT_ONLY_ENABLED) else []
        total_attempt_ms = 0
        for a in attempts_for_payload:
            if not isinstance(a, dict):
                continue
            total_attempt_ms += max(0, int(a.get("elapsed_ms") or 0))
        return {
            "chain": chain_key,
            "version": version,
            "owner": owner,
            "positions_found": len(positions),
            "failed": False,
            "attempts": attempts_for_payload,
            "attempts_total_ms": int(total_attempt_ms),
            "compare": (
                compare
                if isinstance(compare, dict)
                else (_build_owner_compare_debug(positions, attempts_for_payload) if hard_scan else {})
            ),
        }

    def _make_no_nft_owner_debug(owner: str, version: str, owner_attempts: list[dict[str, Any]]) -> dict[str, Any]:
        return _make_owner_debug(
            owner,
            version,
            [],
            owner_attempts,
            compare={
                "graph_checked": False,
                "onchain_count": 0,
                "graph_count": 0,
                "only_onchain_count": 0,
                "only_graph_count": 0,
                "only_onchain_ids": [],
                "only_graph_ids": [],
            },
        )

    def _resolve_live_discovery_flag(
        *,
        owner: str,
        skip_live_discovery: bool,
        positions: list[dict[str, Any]],
        owner_attempts: list[dict[str, Any]],
    ) -> bool:
        can_use_live_discovery = bool(POSITIONS_LEGACY_DISCOVERY_ENABLED and not skip_live_discovery)
        if hard_scan and not can_use_live_discovery and not positions:
            owner_attempts.append(
                {
                    "owner_value": owner,
                    "owner_type": "legacy",
                    "query_mode": "legacy_discovery_disabled",
                    "count": 0,
                    "ok": True,
                }
            )
        return can_use_live_discovery

    def _load_cached_positions_for_owner(owner: str, version: str) -> list[dict[str, Any]]:
        protocol_label = (
            str(V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(int(chain_id), "uniswap_v3"))
            if version == "v3"
            else "uniswap_v4"
        )
        cached_positions = _position_cached_rows_for_owner(
            int(chain_id),
            owner,
            protocol_label,
            limit=POSITIONS_OWNERSHIP_INDEX_MAX_NFTS,
        )
        if version == "v3" and int(chain_id) in PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID:
            cached_positions.extend(
                _position_cached_rows_for_owner(
                    int(chain_id),
                    owner,
                    "pancake_v3_staked",
                    limit=max(40, POSITIONS_OWNERSHIP_INDEX_MAX_NFTS // 2),
                )
            )
        if version == "v3":
            cached_positions.extend(
                _position_cached_rows_for_owner(
                    int(chain_id),
                    owner,
                    "pancake_infinity_cl",
                    limit=max(20, POSITIONS_OWNERSHIP_INDEX_MAX_NFTS // 3),
                )
            )
            cached_positions.extend(
                _position_cached_rows_for_owner(
                    int(chain_id),
                    owner,
                    "pancake_infinity_bin",
                    limit=max(20, POSITIONS_OWNERSHIP_INDEX_MAX_NFTS // 3),
                )
            )
        uniq: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for cp in cached_positions:
            if not isinstance(cp, dict):
                continue
            key = "|".join(
                [
                    str(cp.get("_protocol_label") or "").lower(),
                    str(cp.get("id") or ""),
                    str(((cp.get("pool") or {}).get("id") or "")).lower(),
                ]
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            uniq.append(cp)
        return uniq

    def _apply_ownership_cache(
        owner: str,
        *,
        version: str,
        owner_attempts: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool, bool]:
        cached_positions: list[dict[str, Any]] = []
        index_cache_hit = False
        skip_live_discovery = False
        if version in {"v3", "v4"} and POSITIONS_OWNERSHIP_INDEX_ENABLED:
            try:
                if not pre_enqueued_ownership_refresh:
                    _position_enqueue_ownership_refresh(int(chain_id), owner)
                cached_positions = _load_cached_positions_for_owner(owner, version)
                if cached_positions:
                    index_cache_hit = True
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "ownership_cache",
                        "count": len(cached_positions),
                        "ok": True,
                    }
                )
                if hard_scan and POSITIONS_INDEX_FIRST_STRICT and index_cache_hit:
                    skip_live_discovery = True
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "index",
                            "query_mode": "index_first_skip_live",
                            "count": len(cached_positions),
                            "ok": True,
                        }
                    )
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "ownership_cache",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )
        return cached_positions, index_cache_hit, skip_live_discovery

    def _discover_owner_positions(
        owner: str,
        *,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        has_position_liquidity: bool,
        owner_has_nft: bool,
        owner_attempts: list[dict[str, Any]],
        owner_errors: list[str],
        deadline_ts: float,
    ) -> tuple[list[dict[str, Any]], bool]:
        if POSITIONS_CONTRACT_ONLY_ENABLED:
            positions: list[dict[str, Any]] = []

            def _merge_positions(add_rows: list[dict[str, Any]]) -> None:
                if not add_rows:
                    return
                seen_keys = {
                    f"{str((x or {}).get('_protocol_label') or '').strip().lower()}|{str((x or {}).get('id') or '').strip()}"
                    for x in positions
                    if isinstance(x, dict)
                }
                for row in add_rows:
                    if not isinstance(row, dict):
                        continue
                    k = f"{str((row or {}).get('_protocol_label') or '').strip().lower()}|{str((row or {}).get('id') or '').strip()}"
                    if k in seen_keys:
                        continue
                    seen_keys.add(k)
                    positions.append(row)

            if version == "v3" and time.monotonic() < deadline_ts:
                try:
                    t_call = time.monotonic()
                    _set_chain_progress("call_contract_only_onchain_v3_npm", version=version, owner=str(owner))
                    v3_dbg: dict[str, Any] = {}
                    v3_proto = str(V3_PROTOCOL_LABEL_BY_CHAIN_ID.get(int(chain_id), "uniswap_v3") or "uniswap_v3").strip().lower()
                    explorer_v3_ids: list[int] = []
                    npm = str(UNISWAP_V3_NPM_BY_CHAIN_ID.get(int(chain_id), "") or "").strip().lower()
                    if _is_eth_address(npm):
                        explorer_v3_ids = _scan_erc721_token_ids_by_explorer_api(
                            int(chain_id),
                            npm,
                            owner,
                            max_ids=max(20, int(POSITIONS_ONCHAIN_MAX_NFTS)),
                            protocol=v3_proto,
                        )
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "explorer",
                                "query_mode": "contract_only_explorer_v3_ids",
                                "count": len(explorer_v3_ids),
                                "ok": True,
                                "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                            }
                        )
                    t_call = time.monotonic()
                    v3_rows = _scan_v3_positions_onchain(
                        owner,
                        int(chain_id),
                        deadline_ts=deadline_ts,
                        include_price_details=False,
                        protocol_label=v3_proto,
                        source_tag="contract_only_onchain_v3_npm",
                        debug_out=v3_dbg,
                        token_ids_override=explorer_v3_ids,
                    )
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "contract_only_onchain_v3_npm",
                            "count": len(v3_rows),
                            "ok": True,
                            "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                            "v3_debug": v3_dbg,
                        }
                    )
                    _merge_positions(v3_rows)
                except Exception as e:
                    elapsed_ms = int(round(max(0.0, time.monotonic() - t_call) * 1000.0)) if "t_call" in locals() else 0
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "contract_only_onchain_v3_npm",
                            "count": 0,
                            "ok": False,
                            "elapsed_ms": int(elapsed_ms),
                            "error": str(e)[:220],
                        }
                    )

                if int(chain_id) in PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID and time.monotonic() < deadline_ts:
                    try:
                        t_call = time.monotonic()
                        _set_chain_progress("call_contract_only_onchain_pancake_masterchef_v3", version=version, owner=str(owner))
                        pc_dbg: dict[str, Any] = {}
                        staked_rows = _scan_pancake_staked_v3_positions_onchain(
                            owner,
                            int(chain_id),
                            deadline_ts=deadline_ts,
                            debug_out=pc_dbg,
                        )
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_masterchef_v3",
                                "count": len(staked_rows),
                                "ok": True,
                                "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                                "pancake_v3_debug": pc_dbg,
                            }
                        )
                        _merge_positions(staked_rows)
                    except Exception as e:
                        elapsed_ms = int(round(max(0.0, time.monotonic() - t_call) * 1000.0)) if "t_call" in locals() else 0
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_masterchef_v3",
                                "count": 0,
                                "ok": False,
                                "elapsed_ms": int(elapsed_ms),
                                "error": str(e)[:220],
                            }
                        )

                if int(chain_id) in PANCAKE_INFINITY_CL_POSITION_MANAGER_BY_CHAIN_ID and time.monotonic() < deadline_ts:
                    try:
                        t_call = time.monotonic()
                        _set_chain_progress("call_contract_only_onchain_pancake_infinity_cl", version=version, owner=str(owner))
                        inf_dbg: dict[str, Any] = {}
                        inf_cl_rows = _scan_pancake_infinity_cl_positions_onchain(
                            owner,
                            int(chain_id),
                            deadline_ts=deadline_ts,
                            debug_out=inf_dbg,
                        )
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_infinity_cl",
                                "count": len(inf_cl_rows),
                                "ok": True,
                                "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                                "infinity_debug": inf_dbg,
                            }
                        )
                        _merge_positions(inf_cl_rows)
                    except Exception as e:
                        elapsed_ms = int(round(max(0.0, time.monotonic() - t_call) * 1000.0)) if "t_call" in locals() else 0
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_infinity_cl",
                                "count": 0,
                                "ok": False,
                                "elapsed_ms": int(elapsed_ms),
                                "error": str(e)[:220],
                            }
                        )

                if int(chain_id) in PANCAKE_INFINITY_BIN_POSITION_MANAGER_BY_CHAIN_ID and time.monotonic() < deadline_ts:
                    try:
                        t_call = time.monotonic()
                        _set_chain_progress("call_contract_only_onchain_pancake_infinity_bin", version=version, owner=str(owner))
                        inf_dbg: dict[str, Any] = {}
                        inf_bin_rows = _scan_pancake_infinity_bin_positions_onchain(
                            owner,
                            int(chain_id),
                            deadline_ts=deadline_ts,
                            debug_out=inf_dbg,
                        )
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_infinity_bin",
                                "count": len(inf_bin_rows),
                                "ok": True,
                                "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                                "infinity_debug": inf_dbg,
                            }
                        )
                        _merge_positions(inf_bin_rows)
                    except Exception as e:
                        elapsed_ms = int(round(max(0.0, time.monotonic() - t_call) * 1000.0)) if "t_call" in locals() else 0
                        owner_attempts.append(
                            {
                                "owner_value": owner,
                                "owner_type": "onchain",
                                "query_mode": "contract_only_onchain_pancake_infinity_bin",
                                "count": 0,
                                "ok": False,
                                "elapsed_ms": int(elapsed_ms),
                                "error": str(e)[:220],
                            }
                        )
            if version == "v4" and int(chain_id) in UNISWAP_V4_POSITION_MANAGER_BY_CHAIN_ID and time.monotonic() < deadline_ts:
                try:
                    t_call = time.monotonic()
                    _set_chain_progress("call_contract_only_onchain_uniswap_v4_pm", version=version, owner=str(owner))
                    v4_rows = _scan_uniswap_v4_positions_onchain(
                        owner,
                        int(chain_id),
                        deadline_ts=deadline_ts,
                    )
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "contract_only_onchain_uniswap_v4_pm",
                            "count": len(v4_rows),
                            "ok": True,
                            "elapsed_ms": int(round(max(0.0, time.monotonic() - t_call) * 1000.0)),
                        }
                    )
                    _merge_positions(v4_rows)
                except Exception as e:
                    elapsed_ms = int(round(max(0.0, time.monotonic() - t_call) * 1000.0)) if "t_call" in locals() else 0
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "onchain",
                            "query_mode": "contract_only_onchain_uniswap_v4_pm",
                            "count": 0,
                            "ok": False,
                            "elapsed_ms": int(elapsed_ms),
                            "error": str(e)[:220],
                        }
                    )

            return positions, False

        cached_positions, index_cache_hit, skip_live_discovery = _apply_ownership_cache(
            owner,
            version=version,
            owner_attempts=owner_attempts,
        )
        positions: list[dict[str, Any]] = list(cached_positions) if cached_positions else []
        has_cached_infinity = any(
            str((cp or {}).get("_protocol_label") or "").strip().lower().startswith("pancake_infinity_")
            for cp in positions
            if isinstance(cp, dict)
        )
        needs_infinity_gap_warmup = bool(
            version == "v3"
            and int(chain_id) in POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS
            and index_cache_hit
            and not has_cached_infinity
        )
        can_use_live_discovery = _resolve_live_discovery_flag(
            owner=owner,
            skip_live_discovery=skip_live_discovery,
            positions=positions,
            owner_attempts=owner_attempts,
        )
        if not hard_scan:
            # Fast mode: keep strict index-only path; heavy discovery/fallback is reserved for hard scan.
            can_use_live_discovery = False
            # Lightweight fast fallback: try Pancake v3 staked positions from MasterChefV3.
            # Run even when other v3 positions already exist, so Pancake rows are not missed.
            if (
                version == "v3"
                and int(chain_id) in PANCAKE_MASTERCHEF_V3_BY_CHAIN_ID
                and not any(
                    str((pp or {}).get("_protocol_label") or "").strip().lower() == "pancake_v3_staked"
                    for pp in (positions or [])
                    if isinstance(pp, dict)
                )
                and time.monotonic() < (deadline_ts - 0.1)
            ):
                try:
                    fast_deadline = min(deadline_ts, time.monotonic() + 1.6)
                    fallback_positions = _scan_pancake_staked_v3_positions_onchain(
                        owner,
                        int(chain_id),
                        deadline_ts=fast_deadline,
                    )
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "fast",
                            "query_mode": "fast_onchain_pancake_masterchef_v3",
                            "count": len(fallback_positions),
                            "ok": True,
                        }
                    )
                    if fallback_positions:
                        seen_keys: set[str] = set()
                        for pp in positions:
                            if not isinstance(pp, dict):
                                continue
                            k = f"{str(pp.get('_protocol_label') or '').lower()}|{str(pp.get('id') or '')}"
                            seen_keys.add(k)
                        for fp in fallback_positions:
                            if not isinstance(fp, dict):
                                continue
                            fk = f"{str(fp.get('_protocol_label') or '').lower()}|{str(fp.get('id') or '')}"
                            if fk in seen_keys:
                                continue
                            seen_keys.add(fk)
                            positions.append(fp)
                except Exception as e:
                    owner_attempts.append(
                        {
                            "owner_value": owner,
                            "owner_type": "fast",
                            "query_mode": "fast_onchain_pancake_masterchef_v3",
                            "count": 0,
                            "ok": False,
                            "error": str(e)[:220],
                        }
                    )
        # Cold-cache safety: when strict index-first disables live discovery and cache is empty,
        # do one bounded synchronous warmup for this owner/chain to avoid empty first response.
        # Also warm up once when cache hit has v3 rows but is missing Infinity rows.
        if (
            version == "v3"
            and ((not positions) or needs_infinity_gap_warmup)
            and not can_use_live_discovery
            and hard_scan
            and POSITIONS_OWNERSHIP_INDEX_ENABLED
            and POSITIONS_INDEX_SYNC_WARMUP_ENABLED
            and int(chain_id) in POSITIONS_INDEX_SYNC_WARMUP_CHAIN_IDS
            and _claim_sync_warmup(owner, version)
            and time.monotonic() < (deadline_ts - 1.0)
        ):
            try:
                warm_stats = _position_index_refresh_owner_chain(
                    int(chain_id),
                    owner,
                    target_version=version,
                    warmup_mode=True,
                    deep_infinity_scan=deep_infinity_scan,
                )
                cached_positions = _load_cached_positions_for_owner(owner, version)
                if cached_positions:
                    positions = cached_positions
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "index_sync_warmup",
                        "count": len(cached_positions),
                        "ok": True,
                        "reason": "infinity_gap" if needs_infinity_gap_warmup else "cold_cache",
                        "cached": int(warm_stats.get("cached") or 0),
                        "ownership_upserted": int(warm_stats.get("ownership_upserted") or 0),
                    }
                )
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "index_sync_warmup",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )
        # Strict index-first fallback for non-infinity paths: allow one lightweight graph read
        # on cache miss so broad-chain scans do not return empty while keeping legacy paths disabled.
        if (
            version == "v3"
            and not positions
            and not can_use_live_discovery
            and hard_scan
            and POSITIONS_INDEX_MISS_GRAPH_FALLBACK_ENABLED
            and str(endpoint or "").strip()
            and _endpoint_supports_uniswap_positions(endpoint)
            and time.monotonic() < (deadline_ts - 0.5)
        ):
            try:
                graph_deadline = min(
                    deadline_ts - 0.2,
                    time.monotonic() + float(POSITIONS_INDEX_MISS_GRAPH_FALLBACK_DEADLINE_SEC),
                )
                graph_positions = _query_uniswap_positions_for_owner(
                    endpoint,
                    owner,
                    include_pool_liquidity=False,
                    include_position_liquidity=True,
                    debug_steps=None,
                    deadline_ts=graph_deadline,
                    light_mode=True,
                )
                if graph_positions:
                    positions = graph_positions
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "index_graph_fallback",
                        "count": len(graph_positions or []),
                        "ok": True,
                    }
                )
            except Exception as e:
                owner_attempts.append(
                    {
                        "owner_value": owner,
                        "owner_type": "index",
                        "query_mode": "index_graph_fallback",
                        "count": 0,
                        "ok": False,
                        "error": str(e)[:220],
                    }
                )
        if version == "v4" and not str(endpoint or "").strip():
            owner_attempts.append(
                {
                    "owner_value": owner,
                    "owner_type": "index",
                    "query_mode": "v4_no_endpoint_cache_only",
                    "count": len(positions),
                    "ok": True,
                }
            )
        if hard_scan and positions and not can_use_live_discovery:
            owner_attempts.append(
                {
                    "owner_value": owner,
                    "owner_type": "index",
                    "query_mode": "row_live_enrich_disabled",
                    "count": len(positions),
                    "ok": True,
                }
            )
        if can_use_live_discovery:
            positions = _run_owner_legacy_core_discovery(
                owner,
                version=version,
                endpoint=endpoint,
                has_pool_liquidity=has_pool_liquidity,
                has_position_liquidity=has_position_liquidity,
                owner_has_nft=owner_has_nft,
                positions=positions,
                index_cache_hit=index_cache_hit,
                owner_attempts=owner_attempts,
                owner_errors=owner_errors,
                deadline_ts=deadline_ts,
            )
            positions = _run_owner_legacy_infinity_discovery(
                owner,
                version=version,
                positions=positions,
                owner_attempts=owner_attempts,
                deadline_ts=deadline_ts,
            )
        return positions, can_use_live_discovery

    def _build_owner_pool_row(
        p: dict[str, Any],
        *,
        owner: str,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        allow_live_enrich: bool,
        deadline_ts: float,
    ) -> dict[str, Any] | None:
        if time.monotonic() >= deadline_ts:
            return None
        if not isinstance(p, dict):
            return None
        if hard_scan and allow_live_enrich and not _position_has_full_detail(p) and not bool(p.get("_skip_enrich")):
            if time.monotonic() >= deadline_ts:
                return None
            enriched = _fetch_position_by_id_with_detail(
                endpoint,
                str(p.get("id") or ""),
                include_pool_liquidity=has_pool_liquidity,
            )
            if enriched:
                p = enriched
        liq_raw = p.get("liquidity")
        if liq_raw not in (None, "") and _safe_float(liq_raw) <= 0:
            return None
        pool = p.get("pool") or {}
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
        chain_addr_map = _TOKEN_ADDR_TO_SYMBOL_BY_CHAIN.get(str(chain_key or "").strip().lower(), {}) or {}

        def _token_symbol_hint(token_obj: dict[str, Any]) -> tuple[str, str]:
            sym = str((token_obj or {}).get("symbol") or "").strip()
            if sym:
                return _normalize_display_symbol(sym), "token.symbol"
            addr = str((token_obj or {}).get("id") or "").strip().lower()
            cfg = str(chain_addr_map.get(addr) or "").strip()
            if cfg:
                return _normalize_display_symbol(cfg), "curated_map"
            return "?", "missing"

        # Fast pre-check for suspected spam based on existing/cached symbols only.
        # Do this before expensive on-chain quote/snapshot calls.
        t0, t0_src = _token_symbol_hint(t0_obj)
        t1, t1_src = _token_symbol_hint(t1_obj)
        proto_label = str(p.get("_protocol_label") or f"uniswap_{version}").strip().lower()
        is_v3_npm_protocol = proto_label in {"uniswap_v3", "pancake_v3", "pancake_v3_staked"}
        is_infinity_protocol = proto_label.startswith("pancake_infinity_")
        if POSITIONS_CONTRACT_ONLY_ENABLED and not (
            (version == "v3" and (is_v3_npm_protocol or is_infinity_protocol))
            or proto_label == "uniswap_v4"
        ):
            return None
        t0_hint_u = str(t0 or "").strip().upper()
        t1_hint_u = str(t1 or "").strip().upper()
        t0_info = bool(t0_hint_u not in {"", "?", "UNK"})
        t1_info = bool(t1_hint_u not in {"", "?", "UNK"})
        # Early spam pre-filter:
        # - full check when both symbols are informative
        # - or when any informative symbol already looks spammy
        #   (to avoid tokenId/contract requests for obvious spam rows).
        has_spam_hint = bool(
            (t0_info and _is_probably_spam_symbol(str(t0 or "")))
            or (t1_info and _is_probably_spam_symbol(str(t1 or "")))
        )
        precheck_symbols_ready = bool((t0_info and t1_info) or has_spam_hint)
        suspected_spam_pre = (
            _is_suspected_spam_pair(
                chain_key,
                t0_obj if isinstance(t0_obj, dict) else {},
                t1_obj if isinstance(t1_obj, dict) else {},
                str(t0 or ""),
                str(t1 or ""),
                None,
            )
            if precheck_symbols_ready
            else False
        )
        if proto_label.startswith("pancake_"):
            suspected_spam_pre = False
        pos_token_id = _position_token_id_from_raw(p.get("id"))
        if int(pos_token_id) in POSITIONS_NOT_SPAM_POSITION_IDS:
            suspected_spam_pre = False
        contract_snapshot: dict[str, Any] | None = None
        if proto_label.startswith("pancake_infinity_"):
            raw_pid = str(p.get("id") or "").strip()
            pid = raw_pid.split(":", 1)[1] if ":" in raw_pid else raw_pid
            if str(t0).upper() == "UNK" and str(t1).upper() == "UNK":
                t0 = "Infinity"
                t1 = f"#{pid}" if pid else "Position"
                t0_src = "infinity_placeholder"
                t1_src = "infinity_placeholder"
            elif str(t0).upper() == "UNK":
                t0 = "Infinity"
                t0_src = "infinity_placeholder"
            elif str(t1).upper() == "UNK":
                t1 = f"#{pid}" if pid else "Position"
                t1_src = "infinity_placeholder"
        if suspected_spam_pre:
            # Early spam path must sanitize displayed pair symbols too,
            # otherwise hidden rows can show garbage names (e.g. ETH/JamesThomas).
            if not proto_label.startswith("pancake_infinity_"):
                t0_addr = str((t0_obj or {}).get("id") or "").strip().lower() if isinstance(t0_obj, dict) else ""
                t1_addr = str((t1_obj or {}).get("id") or "").strip().lower() if isinstance(t1_obj, dict) else ""
                t0_curated = bool(t0_addr and (t0_addr in chain_addr_map))
                t1_curated = bool(t1_addr and (t1_addr in chain_addr_map))
                if (not t0_curated) and _is_probably_spam_symbol(str(t0 or "")):
                    t0 = "UNK"
                    t0_src = f"{str(t0_src or '')}|precheck_sanitized_spam"
                if (not t1_curated) and _is_probably_spam_symbol(str(t1 or "")):
                    t1 = "UNK"
                    t1_src = f"{str(t1_src or '')}|precheck_sanitized_spam"
            liq_spam = max(0, _parse_int_like(p.get("liquidity") or 0))
            return {
                "address": owner,
                "protocol": str(p.get("_protocol_label") or f"uniswap_{version}"),
                "chain": chain_key,
                "chain_id": int(chain_id),
                "kind": "pool",
                "pool_id": str(pool.get("id") or ""),
                "pair": f"{t0}/{t1}",
                "token0_id": str((pool.get("token0") or {}).get("id") or ""),
                "token1_id": str((pool.get("token1") or {}).get("id") or ""),
                "position_id": str(p.get("id") or ""),
                "position_ids": [str(p.get("id") or "")] if str(p.get("id") or "").strip() else [],
                "fee_tier": "-",
                "fee_tier_raw": "",
                "position_status": "inactive",
                "liquidity_display": _format_usd_compact(None),
                "liquidity": str(p.get("liquidity") or "0"),
                "pool_liquidity": str(pool.get("liquidity") or "0"),
                "position_amount0": 0.0,
                "position_amount1": 0.0,
                "position_symbol0": str(t0 or ""),
                "position_symbol1": str(t1 or ""),
                "pair_symbol_source0": str(t0_src or ""),
                "pair_symbol_source1": str(t1_src or ""),
                "pair_symbol_source": f"0:{str(t0_src or '-')} | 1:{str(t1_src or '-')}",
                "position_amounts_display": "0 / 0",
                "fees_owed0": 0.0,
                "fees_owed1": 0.0,
                "fees_owed_display": "0 / 0",
                "suspected_spam": True,
                "spam_skipped": True,
                "liquidity_usd": None,
            }
        # For non-spam rows, allow on-chain symbol fallback.
        # In contract-only mode for v3 NPM-like rows, final pair is taken from snapshot.
        if not proto_label.startswith("pancake_infinity_") and not (
            POSITIONS_CONTRACT_ONLY_ENABLED and version == "v3" and is_v3_npm_protocol
        ):
            t0_raw, t0_src = _token_display_symbol_with_source(int(chain_id), chain_key, t0_obj)
            t1_raw, t1_src = _token_display_symbol_with_source(int(chain_id), chain_key, t1_obj)
            t0 = _normalize_display_symbol(t0_raw)
            t1 = _normalize_display_symbol(t1_raw)
        if version == "v3" and is_v3_npm_protocol:
            if time.monotonic() >= deadline_ts:
                return None
            try:
                contract_snapshot = _fetch_v3_position_contract_snapshot(
                    int(chain_id),
                    proto_label,
                    _position_token_id_from_raw(p.get("id")),
                    owner,
                    include_quotes=False,
                )
            except Exception:
                contract_snapshot = None
            if contract_snapshot:
                try:
                    p["liquidity"] = str(int(contract_snapshot.get("liquidity") or 0))
                    p["tokensOwed0"] = str(int(contract_snapshot.get("tokens_owed0_raw") or 0))
                    p["tokensOwed1"] = str(int(contract_snapshot.get("tokens_owed1_raw") or 0))
                except Exception:
                    pass
                t0_snap = _normalize_display_symbol(str(contract_snapshot.get("token0_symbol") or ""))
                t1_snap = _normalize_display_symbol(str(contract_snapshot.get("token1_symbol") or ""))
                if t0_snap:
                    t0 = t0_snap
                    t0_src = "snapshot_symbol"
                if t1_snap:
                    t1 = t1_snap
                    t1_src = "snapshot_symbol"
                if not isinstance(pool, dict):
                    pool = {}
                pool = dict(pool)
                pool["feeTier"] = str(contract_snapshot.get("fee") or pool.get("feeTier") or "")
                t0_obj = dict(t0_obj or {})
                t1_obj = dict(t1_obj or {})
                t0_obj["id"] = str(contract_snapshot.get("token0") or t0_obj.get("id") or "")
                t1_obj["id"] = str(contract_snapshot.get("token1") or t1_obj.get("id") or "")
                t0_obj["decimals"] = str(contract_snapshot.get("token0_decimals") or t0_obj.get("decimals") or "18")
                t1_obj["decimals"] = str(contract_snapshot.get("token1_decimals") or t1_obj.get("decimals") or "18")
                t0_obj["symbol"] = str(contract_snapshot.get("token0_symbol") or t0_obj.get("symbol") or "")
                t1_obj["symbol"] = str(contract_snapshot.get("token1_symbol") or t1_obj.get("symbol") or "")
                pool["token0"] = t0_obj
                pool["token1"] = t1_obj
                fee_raw = str(contract_snapshot.get("fee") or fee_raw or "").strip()
                fee_disp = fee_raw
                try:
                    fee_int = int(fee_raw)
                    if fee_int > 0:
                        fee_disp = f"{fee_int / 10000.0:.2f}%"
                except Exception:
                    fee_disp = fee_raw or "-"
        if POSITIONS_CONTRACT_ONLY_ENABLED and version == "v3" and is_v3_npm_protocol and not contract_snapshot:
            # Strict contract-only mode: skip rows that cannot be confirmed from contract.
            return None
        # Exact-only output: show on-chain token amounts and owed fees.
        if hard_scan and allow_live_enrich and version == "v3":
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
                t0_raw, t0_src = _token_display_symbol_with_source(int(chain_id), chain_key, t0_obj)
                t1_raw, t1_src = _token_display_symbol_with_source(int(chain_id), chain_key, t1_obj)
                t0 = _normalize_display_symbol(t0_raw)
                t1 = _normalize_display_symbol(t1_raw)

        def _sanitize_pair_symbol(sym: str, token_obj: dict[str, Any], sym_src: str) -> tuple[str, str]:
            s = _normalize_display_symbol(str(sym or ""))
            if proto_label.startswith("pancake_infinity_"):
                return s, sym_src
            addr = str((token_obj or {}).get("id") or "").strip().lower()
            curated = bool(addr and (addr in chain_addr_map))
            if curated:
                return s, sym_src
            if _is_probably_spam_symbol(s):
                return "UNK", f"{sym_src}|sanitized_spam"
            return s, sym_src

        t0, t0_src = _sanitize_pair_symbol(t0, t0_obj if isinstance(t0_obj, dict) else {}, str(t0_src or ""))
        t1, t1_src = _sanitize_pair_symbol(t1, t1_obj if isinstance(t1_obj, dict) else {}, str(t1_src or ""))

        suspected_spam_now = _is_suspected_spam_pair(
            chain_key,
            t0_obj if isinstance(t0_obj, dict) else {},
            t1_obj if isinstance(t1_obj, dict) else {},
            str(t0 or ""),
            str(t1 or ""),
            None,
        )
        if proto_label.startswith("pancake_"):
            suspected_spam_now = False
        if int(pos_token_id) in POSITIONS_NOT_SPAM_POSITION_IDS:
            suspected_spam_now = False
        if bool(suspected_spam_now):
            # Do not query In position / Unclaimed fees for spam-marked rows.
            return {
                "address": owner,
                "protocol": str(p.get("_protocol_label") or f"uniswap_{version}"),
                "chain": chain_key,
                "chain_id": int(chain_id),
                "kind": "pool",
                "pool_id": str(pool.get("id") or ""),
                "pair": f"{t0}/{t1}",
                "token0_id": str((pool.get("token0") or {}).get("id") or ""),
                "token1_id": str((pool.get("token1") or {}).get("id") or ""),
                "position_id": str(p.get("id") or ""),
                "position_ids": [str(p.get("id") or "")] if str(p.get("id") or "").strip() else [],
                "fee_tier": "-",
                "fee_tier_raw": "",
                "position_status": "inactive",
                "liquidity_display": _format_usd_compact(None),
                "liquidity": str(p.get("liquidity") or "0"),
                "pool_liquidity": str(pool.get("liquidity") or "0"),
                "position_amount0": 0.0,
                "position_amount1": 0.0,
                "position_symbol0": str(t0 or ""),
                "position_symbol1": str(t1 or ""),
                "pair_symbol_source0": str(t0_src or ""),
                "pair_symbol_source1": str(t1_src or ""),
                "pair_symbol_source": f"0:{str(t0_src or '-')} | 1:{str(t1_src or '-')}",
                "position_amounts_display": "0 / 0",
                "fees_owed0": 0.0,
                "fees_owed1": 0.0,
                "fees_owed_display": "0 / 0",
                "suspected_spam": True,
                "spam_skipped": True,
                "liquidity_usd": None,
            }

        amount0_val: float | None = None
        amount1_val: float | None = None
        if contract_snapshot:
            try:
                q0 = contract_snapshot.get("quote_amount0")
                q1 = contract_snapshot.get("quote_amount1")
                if q0 is not None:
                    amount0_val = float(q0)
                if q1 is not None:
                    amount1_val = float(q1)
            except Exception:
                pass
        dec0 = _parse_int_like((pool.get("token0") or {}).get("decimals") or 18)
        dec1 = _parse_int_like((pool.get("token1") or {}).get("decimals") or 18)
        if dec0 <= 0 or dec0 > 36:
            dec0 = 18
        if dec1 <= 0 or dec1 > 36:
            dec1 = 18
        liq_int = max(0, _parse_int_like(p.get("liquidity") or 0))
        # Prefer direct on-chain quote from position manager instead of local math.
        need_direct_quote = bool(
            version == "v3"
            and is_v3_npm_protocol
            and (amount0_val is None or amount1_val is None)
        )
        if need_direct_quote:
            if time.monotonic() >= deadline_ts:
                return None
            try:
                quoted0, quoted1 = _quote_v3_decrease_liquidity_amounts(
                    int(chain_id),
                    str(p.get("_protocol_label") or f"uniswap_{version}"),
                    _position_token_id_from_raw(p.get("id")),
                    liq_int,
                    owner,
                    dec0,
                    dec1,
                )
                if quoted0 is not None:
                    amount0_val = quoted0
                if quoted1 is not None:
                    amount1_val = quoted1
            except Exception:
                pass
        owed0_raw = max(0, _parse_int_like((contract_snapshot or {}).get("tokens_owed0_raw") or 0))
        owed1_raw = max(0, _parse_int_like((contract_snapshot or {}).get("tokens_owed1_raw") or 0))
        if version == "v3" and is_v3_npm_protocol and owed0_raw == 0 and owed1_raw == 0 and liq_int > 0:
            owed_direct = _fetch_v3_tokens_owed_raw(
                int(chain_id),
                str(p.get("_protocol_label") or f"uniswap_{version}"),
                _position_token_id_from_raw(p.get("id")),
            )
            if owed_direct:
                owed0_raw = int(owed_direct[0] or 0)
                owed1_raw = int(owed_direct[1] or 0)
        fees0_val: float | None = None
        fees1_val: float | None = None
        if contract_snapshot:
            try:
                fq0 = contract_snapshot.get("quote_fee0")
                fq1 = contract_snapshot.get("quote_fee1")
                if fq0 is not None:
                    fees0_val = float(fq0)
                if fq1 is not None:
                    fees1_val = float(fq1)
            except Exception:
                pass
        if version == "v3" and is_v3_npm_protocol and (fees0_val is None or fees1_val is None):
            if time.monotonic() >= deadline_ts:
                return None
            try:
                qf0, qf1 = _quote_v3_collect_fees_amounts(
                    int(chain_id),
                    str(p.get("_protocol_label") or f"uniswap_{version}"),
                    _position_token_id_from_raw(p.get("id")),
                    owner,
                    dec0,
                    dec1,
                )
                if qf0 is not None:
                    fees0_val = float(qf0)
                if qf1 is not None:
                    fees1_val = float(qf1)
            except Exception:
                pass
        try:
            if fees0_val is None and owed0_raw > 0:
                fees0_val = float(Decimal(owed0_raw) / (Decimal(10) ** dec0))
            if fees1_val is None and owed1_raw > 0:
                fees1_val = float(Decimal(owed1_raw) / (Decimal(10) ** dec1))
        except Exception:
            if fees0_val is None:
                fees0_val = None
            if fees1_val is None:
                fees1_val = None

        def _fmt_amt(v: float | None, *, zero_if_missing: bool = False) -> str:
            if v is None:
                return "0" if zero_if_missing else "-"
            av = abs(float(v))
            if av >= 1000:
                s = f"{float(v):,.1f}"
            elif av >= 1:
                s = f"{float(v):,.2f}"
            elif av >= 0.01:
                s = f"{float(v):,.3f}"
            else:
                s = f"{float(v):,.4f}"
            return s.rstrip("0").rstrip(".")

        position_amounts_display = f"{_fmt_amt(amount0_val)} / {_fmt_amt(amount1_val)}"
        fees_owed_display = f"{_fmt_amt(fees0_val, zero_if_missing=True)} / {_fmt_amt(fees1_val, zero_if_missing=True)}"
        a0_now = max(0.0, float(amount0_val or 0.0))
        a1_now = max(0.0, float(amount1_val or 0.0))
        # Status is derived only from "In position":
        # inactive when either side is zero; active otherwise.
        position_status = "inactive" if (a0_now <= 0.0 or a1_now <= 0.0) else "active"

        def _fmt_liq_compact(v: int) -> str:
            x = max(0, int(v or 0))
            if x < 1000:
                return str(x)
            if x < 1_000_000:
                return f"{x / 1000.0:.1f}K".rstrip("0").rstrip(".")
            if x < 1_000_000_000:
                return f"{x / 1_000_000.0:.1f}M".rstrip("0").rstrip(".")
            if x < 1_000_000_000_000:
                return f"{x / 1_000_000_000.0:.1f}B".rstrip("0").rstrip(".")
            return f"{x / 1_000_000_000_000.0:.1f}T".rstrip("0").rstrip(".")

        liquidity_display = _format_usd_compact(None)
        suspected_spam = _is_suspected_spam_pair(
            chain_key,
            t0_obj if isinstance(t0_obj, dict) else {},
            t1_obj if isinstance(t1_obj, dict) else {},
            str(t0 or ""),
            str(t1 or ""),
            None,
        )
        if proto_label.startswith("pancake_"):
            suspected_spam = False
        if int(pos_token_id) in POSITIONS_NOT_SPAM_POSITION_IDS:
            suspected_spam = False

        return {
            "address": owner,
            "protocol": str(p.get("_protocol_label") or f"uniswap_{version}"),
            "chain": chain_key,
            "chain_id": int(chain_id),
            "kind": "pool",
            "pool_id": str(pool.get("id") or ""),
            "pair": f"{t0}/{t1}",
            "token0_id": str((pool.get("token0") or {}).get("id") or ""),
            "token1_id": str((pool.get("token1") or {}).get("id") or ""),
            "position_id": str(p.get("id") or ""),
            "position_ids": [str(p.get("id") or "")] if str(p.get("id") or "").strip() else [],
            "fee_tier": fee_disp,
            "fee_tier_raw": fee_raw,
            "position_status": position_status,
            "liquidity_display": liquidity_display,
            "liquidity": str(p.get("liquidity") or "0"),
            "pool_liquidity": str(pool.get("liquidity") or "0"),
            "position_amount0": amount0_val,
            "position_amount1": amount1_val,
            "position_symbol0": str(t0 or ""),
            "position_symbol1": str(t1 or ""),
            "pair_symbol_source0": str(t0_src or ""),
            "pair_symbol_source1": str(t1_src or ""),
            "pair_symbol_source": f"0:{str(t0_src or '-')} | 1:{str(t1_src or '-')}",
            "position_amounts_display": position_amounts_display,
            "fees_owed0": fees0_val,
            "fees_owed1": fees1_val,
            "fees_owed_display": fees_owed_display,
            "suspected_spam": bool(suspected_spam),
            "liquidity_usd": None,
        }

    def _build_owner_rows_from_positions(
        positions: list[dict[str, Any]],
        *,
        owner: str,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        allow_live_enrich: bool,
        owner_errors: list[str],
        deadline_ts: float,
    ) -> tuple[list[dict[str, Any]], bool]:
        out_rows: list[dict[str, Any]] = []
        owner_timed_out = False
        workers = 1 if POSITIONS_DISABLE_PARALLELISM else max(1, min(int(POSITIONS_NFT_PARALLEL_WORKERS), len(positions)))
        if workers <= 1:
            for p in positions:
                try:
                    row = _build_owner_pool_row(
                        p,
                        owner=owner,
                        version=version,
                        endpoint=endpoint,
                        has_pool_liquidity=has_pool_liquidity,
                        allow_live_enrich=allow_live_enrich,
                        deadline_ts=deadline_ts,
                    )
                    if row:
                        out_rows.append(row)
                except Exception as e:
                    if POSITIONS_DEBUG_ERRORS:
                        owner_errors.append(f"Pool row skipped [{chain_key}/{version}] for {owner}: {e}")
                    continue
                if time.monotonic() >= deadline_ts:
                    owner_timed_out = True
                    break
            return out_rows, owner_timed_out

        indexed_rows: list[tuple[int, dict[str, Any]]] = []
        with ThreadPoolExecutor(max_workers=workers) as ex:
            fut_to_idx: dict[Any, int] = {}
            for idx, p in enumerate(positions):
                if time.monotonic() >= deadline_ts:
                    owner_timed_out = True
                    break
                fut = ex.submit(
                    _build_owner_pool_row,
                    p,
                    owner=owner,
                    version=version,
                    endpoint=endpoint,
                    has_pool_liquidity=has_pool_liquidity,
                    allow_live_enrich=allow_live_enrich,
                    deadline_ts=deadline_ts,
                )
                fut_to_idx[fut] = idx
            pending: set[Any] = set(fut_to_idx.keys())
            aborted = False
            while pending:
                now = time.monotonic()
                if now >= deadline_ts:
                    owner_timed_out = True
                    aborted = True
                    break
                timeout = min(0.25, max(0.01, deadline_ts - now))
                done, not_done = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)
                if not done:
                    pending = set(not_done)
                    continue
                for fut in done:
                    idx = int(fut_to_idx.get(fut, -1))
                    try:
                        row = fut.result()
                        if row and idx >= 0:
                            indexed_rows.append((idx, row))
                    except Exception as e:
                        if POSITIONS_DEBUG_ERRORS:
                            owner_errors.append(f"Pool row skipped [{chain_key}/{version}] for {owner}: {e}")
                        continue
                pending = set(not_done)
            if aborted:
                ex.shutdown(wait=False, cancel_futures=True)
        if indexed_rows:
            indexed_rows.sort(key=lambda x: x[0])
            out_rows.extend([r for _, r in indexed_rows])
        return out_rows, owner_timed_out

    def _scan_pool_positions_owner(
        owner: str,
        *,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        has_position_liquidity: bool,
    ) -> tuple[list[dict[str, Any]], list[str], dict[str, Any], bool]:
        owner_started = time.monotonic()
        _set_chain_progress("owner_scan_start", version=version, owner=str(owner))
        owner_rows: list[dict[str, Any]] = []
        owner_errors: list[str] = []
        owner_attempts: list[dict[str, Any]] = []
        positions: list[dict[str, Any]] = []
        owner_timed_out = time.monotonic() >= deadline_ts
        if owner_timed_out:
            _set_chain_progress(
                "owner_scan_timeout",
                version=version,
                owner=str(owner),
                owner_elapsed_ms=int(round(max(0.0, time.monotonic() - owner_started) * 1000.0)),
            )
            return owner_rows, owner_errors, _make_owner_debug(owner, version, [], owner_attempts), True
        owner_key = str(owner).strip().lower()
        owner_has_nft = bool(owner_has_nft_balance.get(owner_key, True))
        if hard_scan and int(chain_id) in (56, 8453):
            owner_attempts.append(
                {
                    "owner_value": owner,
                    "owner_type": "onchain",
                    "query_mode": "owner_nft_balance_precheck",
                    "count": 1 if owner_has_nft else 0,
                    "ok": True,
                }
            )
        if hard_scan and version == "v3" and not owner_has_nft:
            owner_debug = _make_no_nft_owner_debug(owner, version, owner_attempts)
            return owner_rows, owner_errors, owner_debug, False
        positions, allow_live_enrich = _discover_owner_positions(
            owner,
            version=version,
            endpoint=endpoint,
            has_pool_liquidity=has_pool_liquidity,
            has_position_liquidity=has_position_liquidity,
            owner_has_nft=owner_has_nft,
            owner_attempts=owner_attempts,
            owner_errors=owner_errors,
            deadline_ts=deadline_ts,
        )
        try:
            _persist_positions_to_ownership_index(
                owner,
                version=version,
                positions=positions,
            )
        except Exception:
            pass
        owner_debug = _make_owner_debug(owner, version, positions, owner_attempts)
        owner_rows, owner_timed_out = _build_owner_rows_from_positions(
            positions,
            owner=owner,
            version=version,
            endpoint=endpoint,
            has_pool_liquidity=has_pool_liquidity,
            allow_live_enrich=allow_live_enrich,
            owner_errors=owner_errors,
            deadline_ts=deadline_ts,
        )
        _set_chain_progress(
            "owner_scan_done",
            version=version,
            owner=str(owner),
            owner_elapsed_ms=int(round(max(0.0, time.monotonic() - owner_started) * 1000.0)),
            owner_rows=len(owner_rows),
            owner_positions=len(positions),
            owner_timed_out=bool(owner_timed_out),
            owner_attempts_ms=int(owner_debug.get("attempts_total_ms") or 0),
        )
        return owner_rows, owner_errors, owner_debug, owner_timed_out

    def _scan_version_owners(
        *,
        version: str,
        endpoint: str,
        has_pool_liquidity: bool,
        has_position_liquidity: bool,
    ) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool]:
        v_rows: list[dict[str, Any]] = []
        v_errors: list[str] = []
        v_debug: list[dict[str, Any]] = []
        v_timed_out = False
        owner_workers = 1 if POSITIONS_DISABLE_PARALLELISM else max(1, min(int(POSITIONS_ADDRESS_PARALLEL_WORKERS), len(addresses)))
        if owner_workers <= 1 or len(addresses) <= 1:
            for owner in addresses:
                _set_chain_progress("version_owner_loop", version=version, owner=str(owner), owner_workers=int(owner_workers))
                if time.monotonic() >= deadline_ts:
                    v_timed_out = True
                    break
                owner_rows, owner_errors, owner_debug, owner_timed_out = _scan_pool_positions_owner(
                    owner,
                    version=version,
                    endpoint=endpoint,
                    has_pool_liquidity=has_pool_liquidity,
                    has_position_liquidity=has_position_liquidity,
                )
                v_rows.extend(owner_rows)
                v_errors.extend(owner_errors)
                v_debug.append(owner_debug)
                if owner_timed_out:
                    v_timed_out = True
                    break
        else:
            _set_chain_progress("version_owner_parallel", version=version, owner_workers=int(owner_workers), owners=len(addresses))
            owner_executor = ThreadPoolExecutor(max_workers=owner_workers)
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
            aborted = False
            pending: set[Any] = set(owner_futures)
            try:
                while pending:
                    now = time.monotonic()
                    if now >= deadline_ts:
                        v_timed_out = True
                        aborted = True
                        break
                    timeout = min(0.25, max(0.01, deadline_ts - now))
                    done, not_done = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)
                    if not done:
                        pending = set(not_done)
                        continue
                    for owner_fut in done:
                        try:
                            owner_rows, owner_errors, owner_debug, owner_timed_out = owner_fut.result()
                        except Exception as e:
                            v_errors.append(f"Pool owner worker failed [{chain_key}/{version}]: {e}")
                            continue
                        v_rows.extend(owner_rows)
                        v_errors.extend(owner_errors)
                        v_debug.append(owner_debug)
                        if owner_timed_out:
                            v_timed_out = True
                    pending = set(not_done)
            finally:
                owner_executor.shutdown(wait=not aborted, cancel_futures=aborted)
        return v_rows, v_errors, v_debug, v_timed_out

    def _prepare_version_scan_context(version: str) -> tuple[str, bool, bool] | None:
        if POSITIONS_CONTRACT_ONLY_ENABLED:
            # Strict contract-only mode: endpoint/graph capabilities are irrelevant.
            return "", False, True
        endpoint = get_graph_endpoint(chain_key, version=version)
        if not hard_scan:
            # Fast mode: do not spend requests on endpoint schema introspection.
            if not endpoint and version != "v3" and not POSITIONS_OWNERSHIP_INDEX_ENABLED:
                return None
            return endpoint, False, True
        if not endpoint and version != "v3":
            # In index-first mode we can still serve cached v4 positions even without live graph endpoint.
            if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
                return None
        if endpoint and not _endpoint_supports_uniswap_positions(endpoint):
            # Keep v3 flow alive via on-chain fallback even if endpoint introspection says unsupported.
            if version != "v3":
                if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
                    return None
        has_pool_liquidity = _endpoint_supports_pool_liquidity(endpoint) if endpoint else False
        has_position_liquidity = _endpoint_supports_position_liquidity(endpoint) if endpoint else True
        return endpoint, has_pool_liquidity, has_position_liquidity

    for version in ("v3", "v4"):
        _set_chain_progress("version_start", version=version)
        if time.monotonic() >= deadline_ts:
            timed_out = True
            break
        v_ctx = _prepare_version_scan_context(version)
        if v_ctx is None:
            continue
        endpoint, has_pool_liquidity, has_position_liquidity = v_ctx
        v_rows, v_errors, v_debug, v_timed_out = _scan_version_owners(
            version=version,
            endpoint=endpoint,
            has_pool_liquidity=has_pool_liquidity,
            has_position_liquidity=has_position_liquidity,
        )
        rows.extend(v_rows)
        errors.extend(v_errors)
        debug_rows.extend(v_debug)
        if v_timed_out:
            timed_out = True
        if timed_out:
            break
    _set_chain_progress("done" if not timed_out else "timed_out", status=("done" if not timed_out else "timed_out"))
    return rows, errors, debug_rows, timed_out


def _aggregate_pool_rows_by_owner_protocol_pool(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _opt_float(v: Any) -> float | None:
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def _fmt_amt(v: float | None, *, zero_if_missing: bool = False) -> str:
        if v is None:
            return "0" if zero_if_missing else "-"
        av = abs(float(v))
        if av >= 1000:
            s = f"{float(v):,.1f}"
        elif av >= 1:
            s = f"{float(v):,.2f}"
        elif av >= 0.01:
            s = f"{float(v):,.3f}"
        else:
            s = f"{float(v):,.4f}"
        return s.rstrip("0").rstrip(".")

    uniq: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("address")), str(row.get("protocol")), str(row.get("pool_id")))
        if key not in uniq:
            base = dict(row)
            base["position_count"] = 1
            if bool(base.get("suspected_spam")) or bool(base.get("spam_skipped")):
                base["liquidity_usd"] = None
                base["liquidity_display"] = "-"
            uniq[key] = base
            continue
        acc = uniq[key]
        acc["position_count"] = int(acc.get("position_count") or 1) + 1
        # Keep spam mark sticky across merged rows.
        acc["suspected_spam"] = bool(acc.get("suspected_spam")) or bool(row.get("suspected_spam"))
        acc["spam_skipped"] = bool(acc.get("spam_skipped")) or bool(row.get("spam_skipped"))
        acc_ids = [str(x) for x in (acc.get("position_ids") or []) if str(x).strip()]
        row_ids = [str(x) for x in (row.get("position_ids") or []) if str(x).strip()]
        if row_ids:
            seen_ids = set(acc_ids)
            for pid in row_ids:
                if pid not in seen_ids:
                    seen_ids.add(pid)
                    acc_ids.append(pid)
            acc["position_ids"] = acc_ids
        # Sum position values across all NFTs in the same owner/protocol/pool group.
        for fld in ("position_amount0", "position_amount1", "fees_owed0", "fees_owed1"):
            av = _opt_float(acc.get(fld))
            bv = _opt_float(row.get(fld))
            if av is None and bv is None:
                acc[fld] = None
            else:
                acc[fld] = float((av or 0.0) + (bv or 0.0))
        acc["position_amounts_display"] = (
            f"{_fmt_amt(_opt_float(acc.get('position_amount0')))} / "
            f"{_fmt_amt(_opt_float(acc.get('position_amount1')))}"
        )
        acc["fees_owed_display"] = (
            f"{_fmt_amt(_opt_float(acc.get('fees_owed0')), zero_if_missing=True)} / "
            f"{_fmt_amt(_opt_float(acc.get('fees_owed1')), zero_if_missing=True)}"
        )
        a0 = max(0.0, float(_opt_float(acc.get("position_amount0")) or 0.0))
        a1 = max(0.0, float(_opt_float(acc.get("position_amount1")) or 0.0))
        liq_raw = max(0, _parse_int_like(acc.get("liquidity") or 0))
        acc["position_status"] = "inactive" if (a0 <= 0.0 or a1 <= 0.0) else "active"

        def _fmt_liq_compact(v: int) -> str:
            x = max(0, int(v or 0))
            if x < 1000:
                return str(x)
            if x < 1_000_000:
                return f"{x / 1000.0:.1f}K".rstrip("0").rstrip(".")
            if x < 1_000_000_000:
                return f"{x / 1_000_000.0:.1f}M".rstrip("0").rstrip(".")
            if x < 1_000_000_000_000:
                return f"{x / 1_000_000_000.0:.1f}B".rstrip("0").rstrip(".")
            return f"{x / 1_000_000_000_000.0:.1f}T".rstrip("0").rstrip(".")

        if bool(acc.get("suspected_spam")) or bool(acc.get("spam_skipped")):
            acc["liquidity_usd"] = None
            acc["liquidity_display"] = "-"
        else:
            acc["liquidity_display"] = _format_usd_compact(_opt_float(acc.get("liquidity_usd")))
        # Keep pool level metadata stable; liquidity fields are not additive across NFTs.
    return list(uniq.values())


def _apply_creation_dates_phase(rows: list[dict[str, Any]], *, include_creation_dates: bool) -> None:
    return


def _run_pool_chain_scan(
    chain_id: int,
    addresses: list[str],
    deadline_ts: float,
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
    progress_out: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool]:
    return _scan_pool_positions_chain(
        chain_id,
        addresses,
        deadline_ts,
        pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
        hard_scan=hard_scan,
        deep_infinity_scan=deep_infinity_scan,
        progress_out=progress_out,
    )


def _run_pool_chain_scan_timed(
    chain_id: int,
    addresses: list[str],
    deadline_ts: float,
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
    progress_out: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool, float]:
    started = time.monotonic()
    rows, errors, debug_rows, timed_out = _run_pool_chain_scan(
        chain_id,
        addresses,
        deadline_ts,
        pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
        hard_scan=hard_scan,
        deep_infinity_scan=deep_infinity_scan,
        progress_out=progress_out,
    )
    elapsed = round(max(0.0, time.monotonic() - started), 3)
    return rows, errors, debug_rows, timed_out, elapsed


def _run_pool_chain_batch_serial(
    chain_ids: list[int],
    addresses: list[str],
    deadline_ts: float,
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
    per_chain_timeout_sec: int | None = None,
    per_chain_timeout_overrides: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool, dict[str, float], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    timed_out = False
    chain_durations_sec: dict[str, float] = {}
    chain_progress: dict[str, dict[str, Any]] = {}
    for chain_id in chain_ids:
        if time.monotonic() >= deadline_ts:
            timed_out = True
            break
        chain_deadline = float(deadline_ts)
        chain_timeout = per_chain_timeout_sec
        if isinstance(per_chain_timeout_overrides, dict):
            ov = int(per_chain_timeout_overrides.get(int(chain_id), 0) or 0)
            if ov > 0:
                chain_timeout = ov
        if chain_timeout is not None and int(chain_timeout) > 0:
            chain_deadline = min(chain_deadline, time.monotonic() + float(int(chain_timeout)))
        chain_key = str(CHAIN_ID_TO_KEY.get(int(chain_id), str(chain_id)) or str(chain_id))
        progress_slot: dict[str, Any] = {
            "chain": chain_key,
            "chain_id": int(chain_id),
            "stage": "queued",
            "status": "queued",
            "started_at_monotonic": float(time.monotonic()),
        }
        chain_progress[chain_key] = progress_slot
        chain_rows, chain_errors, chain_debug, chain_timed_out, chain_elapsed = _run_pool_chain_scan_timed(
            int(chain_id),
            addresses,
            chain_deadline,
            pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
            hard_scan=hard_scan,
            deep_infinity_scan=deep_infinity_scan,
            progress_out=progress_slot,
        )
        rows.extend(chain_rows)
        errors.extend(chain_errors)
        debug_rows.extend(chain_debug)
        chain_durations_sec[chain_key] = float(chain_elapsed)
        progress_slot["elapsed_sec"] = float(chain_elapsed)
        progress_slot["status"] = "timed_out" if bool(chain_timed_out) else "done"
        if chain_timed_out:
            timed_out = True
            break
    return rows, errors, debug_rows, timed_out, chain_durations_sec, chain_progress


def _run_pool_chain_batch_parallel(
    chain_ids: list[int],
    addresses: list[str],
    deadline_ts: float,
    max_workers: int,
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
    per_chain_timeout_sec: int | None = None,
    per_chain_timeout_overrides: dict[int, int] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], bool, dict[str, float], dict[str, dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    timed_out = False
    chain_durations_sec: dict[str, float] = {}
    chain_progress: dict[str, dict[str, Any]] = {}
    executor = ThreadPoolExecutor(max_workers=max_workers)
    future_map: dict[Any, int] = {}
    futures = []
    for chain_id in chain_ids:
        chain_key = str(CHAIN_ID_TO_KEY.get(int(chain_id), str(chain_id)) or str(chain_id))
        progress_slot: dict[str, Any] = {
            "chain": chain_key,
            "chain_id": int(chain_id),
            "stage": "queued",
            "status": "queued",
            "started_at_monotonic": float(time.monotonic()),
        }
        chain_progress[chain_key] = progress_slot
        chain_deadline = float(deadline_ts)
        chain_timeout = per_chain_timeout_sec
        if isinstance(per_chain_timeout_overrides, dict):
            ov = int(per_chain_timeout_overrides.get(int(chain_id), 0) or 0)
            if ov > 0:
                chain_timeout = ov
        if chain_timeout is not None and int(chain_timeout) > 0:
            chain_deadline = min(chain_deadline, time.monotonic() + float(int(chain_timeout)))
        fut = executor.submit(
            _run_pool_chain_scan_timed,
            int(chain_id),
            addresses,
            chain_deadline,
            pre_enqueued_ownership_refresh,
            hard_scan,
            deep_infinity_scan,
            progress_slot,
        )
        futures.append(fut)
        future_map[fut] = int(chain_id)
    aborted = False
    pending: set[Any] = set(futures)
    try:
        while pending:
            now = time.monotonic()
            if now >= deadline_ts:
                timed_out = True
                aborted = True
                break
            timeout = min(0.25, max(0.01, deadline_ts - now))
            done, not_done = wait(pending, timeout=timeout, return_when=FIRST_COMPLETED)
            if not done:
                continue
            for fut in done:
                chain_id = int(future_map.get(fut, 0))
                try:
                    chain_rows, chain_errors, chain_debug, chain_timed_out, chain_elapsed = fut.result()
                except Exception as e:
                    errors.append(f"Pool scan worker failed: {e}")
                    continue
                chain_key = str(CHAIN_ID_TO_KEY.get(int(chain_id), str(chain_id)) or str(chain_id))
                chain_durations_sec[chain_key] = float(chain_elapsed)
                slot = chain_progress.get(chain_key)
                if isinstance(slot, dict):
                    slot["elapsed_sec"] = float(chain_elapsed)
                    slot["status"] = "timed_out" if bool(chain_timed_out) else "done"
                rows.extend(chain_rows)
                errors.extend(chain_errors)
                debug_rows.extend(chain_debug)
                if chain_timed_out:
                    timed_out = True
            pending = set(not_done)
            if time.monotonic() >= deadline_ts:
                timed_out = True
                aborted = True
                break
    finally:
        executor.shutdown(wait=not aborted, cancel_futures=aborted)
    return rows, errors, debug_rows, timed_out, chain_durations_sec, chain_progress


def _filter_chain_ids_for_pool_scan(chain_ids: list[int], addresses: list[str], *, hard_scan: bool = False) -> list[int]:
    valid_chain_ids = [int(cid) for cid in chain_ids if CHAIN_ID_TO_KEY.get(int(cid), "")]
    if not valid_chain_ids:
        return []
    # In strict index-first mode, scan only owner-known chains from ownership index.
    # No fallback to broad chain fanout when index has no chains for the owners.
    if (not POSITIONS_CONTRACT_ONLY_ENABLED) and POSITIONS_OWNERSHIP_INDEX_ENABLED and POSITIONS_INDEX_FIRST_STRICT:
        known_owner_addrs = [str(a or "").strip().lower() for a in addresses if _is_eth_address(str(a or "").strip().lower())]
        if known_owner_addrs:
            try:
                placeholders = ",".join(["?"] * len(known_owner_addrs))
                with _analytics_conn() as conn:
                    rows = conn.execute(
                        f"""
                        SELECT DISTINCT chain_id
                        FROM position_ownership_index
                        WHERE owner IN ({placeholders})
                        """,
                        tuple(known_owner_addrs),
                    ).fetchall()
                known_chain_ids = {int(r[0] or 0) for r in (rows or []) if int(r[0] or 0) > 0}
                narrowed = [cid for cid in valid_chain_ids if int(cid) in known_chain_ids]
                if POSITIONS_CONTRACT_ONLY_ENABLED and not hard_scan and POSITIONS_SKIP_CHAINS_WITHOUT_NFTS:
                    filtered: list[int] = []
                    for cid in narrowed:
                        if _chain_has_any_position_nft_balance(int(cid), addresses):
                            filtered.append(int(cid))
                    return filtered
                return narrowed
            except Exception:
                return []
    if not hard_scan:
        if POSITIONS_CONTRACT_ONLY_ENABLED and POSITIONS_SKIP_CHAINS_WITHOUT_NFTS:
            filtered_fast: list[int] = []
            for cid in valid_chain_ids:
                if _chain_has_any_position_nft_balance(int(cid), addresses):
                    filtered_fast.append(int(cid))
            return filtered_fast
        return valid_chain_ids
    if not POSITIONS_SKIP_CHAINS_WITHOUT_NFTS:
        return valid_chain_ids
    filtered_chain_ids: list[int] = []
    for cid in valid_chain_ids:
        if _chain_has_any_position_nft_balance(int(cid), addresses):
            filtered_chain_ids.append(int(cid))
    return filtered_chain_ids


def _order_chain_ids_for_pool_scan(valid_chain_ids: list[int], priority_chain_ids: list[int]) -> list[int]:
    ordered_chain_ids: list[int] = []
    seen_chain_ids: set[int] = set()
    allowed_chain_ids = {int(cid) for cid in (valid_chain_ids or []) if int(cid) > 0}
    for cid in list(priority_chain_ids) + list(valid_chain_ids):
        cc = int(cid)
        if cc not in allowed_chain_ids:
            continue
        if cc in seen_chain_ids:
            continue
        seen_chain_ids.add(cc)
        ordered_chain_ids.append(cc)
    return ordered_chain_ids


def _scan_pool_positions(
    addresses: list[str],
    chain_ids: list[int],
    *,
    include_creation_dates: bool = True,
    pre_enqueued_ownership_refresh: bool = False,
    hard_scan: bool = False,
    deep_infinity_scan: bool = False,
) -> tuple[list[dict[str, Any]], list[str], list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    debug_rows: list[dict[str, Any]] = []
    timings: dict[str, Any] = {}
    scan_started = time.monotonic()
    scan_budget_sec = int(POSITIONS_SCAN_MAX_SECONDS)
    if POSITIONS_CONTRACT_ONLY_ENABLED:
        scan_budget_sec = int(POSITIONS_SCAN_MAX_SECONDS_CONTRACT_ONLY)
    deadline_ts = time.monotonic() + float(scan_budget_sec)
    timed_out = False

    t_filter = time.monotonic()
    valid_chain_ids = _filter_chain_ids_for_pool_scan(chain_ids, addresses, hard_scan=hard_scan)
    timings["filter_chain_ids_sec"] = round(max(0.0, time.monotonic() - t_filter), 3)
    if not valid_chain_ids:
        timings["total_sec"] = round(max(0.0, time.monotonic() - scan_started), 3)
        return [], [], [], timings
    priority_chain_ids = [130, 56, 8453]  # Prioritize Unichain, then BSC/Base
    ordered_chain_ids = _order_chain_ids_for_pool_scan(valid_chain_ids, priority_chain_ids)
    max_workers = 1 if POSITIONS_DISABLE_PARALLELISM else max(1, min(int(POSITIONS_PARALLEL_WORKERS), len(ordered_chain_ids)))
    run_all_chains_parallel = bool(POSITIONS_CONTRACT_ONLY_ENABLED and (not hard_scan))
    if POSITIONS_CONTRACT_ONLY_ENABLED and not hard_scan:
        max_workers = max(3, int(max_workers))
    timings["chains_total"] = len(ordered_chain_ids)
    timings["chain_workers"] = int(max_workers)
    chain_durations_sec: dict[str, float] = {}

    # Run priority chains first in sequence to avoid deadline starvation.
    priority_ids = [c for c in ordered_chain_ids if c in priority_chain_ids]
    if run_all_chains_parallel:
        priority_ids = []
    t_prio = time.monotonic()
    p_rows, p_errors, p_debug, p_timed_out, p_chain_durations, p_chain_progress = _run_pool_chain_batch_serial(
        priority_ids,
        addresses,
        deadline_ts,
        pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
        hard_scan=hard_scan,
        deep_infinity_scan=deep_infinity_scan,
        per_chain_timeout_sec=None,
    )
    timings["priority_chains_sec"] = round(max(0.0, time.monotonic() - t_prio), 3)
    rows.extend(p_rows)
    errors.extend(p_errors)
    debug_rows.extend(p_debug)
    chain_durations_sec.update(p_chain_durations or {})
    chain_progress: dict[str, dict[str, Any]] = {}
    chain_progress.update(p_chain_progress or {})
    timed_out = bool(p_timed_out)

    remaining_chain_ids = (
        list(ordered_chain_ids)
        if run_all_chains_parallel
        else [c for c in ordered_chain_ids if c not in set(priority_chain_ids)]
    )
    remaining_deadline_ts = float(deadline_ts)
    use_fast_chain_caps = bool((not hard_scan) and (not POSITIONS_CONTRACT_ONLY_ENABLED))
    if use_fast_chain_caps and remaining_chain_ids:
        fast_timeout_base = int(POSITIONS_FAST_PER_CHAIN_TIMEOUT_SEC)
        fast_timeout_overrides: dict[int, int] = {}
        for cid in remaining_chain_ids:
            cc = int(cid)
            timeout_s = int(fast_timeout_base)
            if cc == 130:
                timeout_s = max(timeout_s, 4)  # unichain often needs slightly longer RPC window
            elif cc in (1, 42161, 137, 10):
                timeout_s = max(timeout_s, 3)  # keep major chains from being cut too early
            fast_timeout_overrides[cc] = int(timeout_s)
        remaining_deadline_ts = min(
            float(deadline_ts),
            time.monotonic() + float(POSITIONS_FAST_REMAINING_BUDGET_SEC),
        )
        timings["remaining_budget_sec"] = int(POSITIONS_FAST_REMAINING_BUDGET_SEC)
        timings["per_chain_timeout_sec"] = int(fast_timeout_base)
        timings["per_chain_timeout_overrides"] = {
            str(CHAIN_ID_TO_KEY.get(int(cid), str(cid))): int(sec)
            for cid, sec in fast_timeout_overrides.items()
            if int(sec) != int(fast_timeout_base)
        }
    else:
        fast_timeout_overrides = {}

    if not timed_out and (max_workers <= 1 or len(remaining_chain_ids) <= 1):
        t_rest = time.monotonic()
        r_rows, r_errors, r_debug, r_timed_out, r_chain_durations, r_chain_progress = _run_pool_chain_batch_serial(
            remaining_chain_ids,
            addresses,
            remaining_deadline_ts,
            pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
            hard_scan=hard_scan,
            deep_infinity_scan=deep_infinity_scan,
            per_chain_timeout_sec=(None if (hard_scan or POSITIONS_CONTRACT_ONLY_ENABLED) else int(POSITIONS_FAST_PER_CHAIN_TIMEOUT_SEC)),
            per_chain_timeout_overrides=(None if (hard_scan or POSITIONS_CONTRACT_ONLY_ENABLED) else fast_timeout_overrides),
        )
        rows.extend(r_rows)
        errors.extend(r_errors)
        debug_rows.extend(r_debug)
        chain_durations_sec.update(r_chain_durations or {})
        chain_progress.update(r_chain_progress or {})
        timed_out = bool(r_timed_out)
        timings["remaining_chains_sec"] = round(max(0.0, time.monotonic() - t_rest), 3)
    elif not timed_out:
        t_rest = time.monotonic()
        r_rows, r_errors, r_debug, r_timed_out, r_chain_durations, r_chain_progress = _run_pool_chain_batch_parallel(
            remaining_chain_ids,
            addresses,
            remaining_deadline_ts,
            max_workers=max_workers,
            pre_enqueued_ownership_refresh=pre_enqueued_ownership_refresh,
            hard_scan=hard_scan,
            deep_infinity_scan=deep_infinity_scan,
            per_chain_timeout_sec=(None if (hard_scan or POSITIONS_CONTRACT_ONLY_ENABLED) else int(POSITIONS_FAST_PER_CHAIN_TIMEOUT_SEC)),
            per_chain_timeout_overrides=(None if (hard_scan or POSITIONS_CONTRACT_ONLY_ENABLED) else fast_timeout_overrides),
        )
        rows.extend(r_rows)
        errors.extend(r_errors)
        debug_rows.extend(r_debug)
        chain_durations_sec.update(r_chain_durations or {})
        chain_progress.update(r_chain_progress or {})
        timed_out = bool(r_timed_out)
        timings["remaining_chains_sec"] = round(max(0.0, time.monotonic() - t_rest), 3)

    # Aggregate by owner/protocol/pool id (wallet can hold multiple NFT positions in one pool).
    rows_before_aggregate = len(rows)
    t_agg = time.monotonic()
    uniq_rows = _aggregate_pool_rows_by_owner_protocol_pool(rows)
    timings["aggregate_rows_sec"] = round(max(0.0, time.monotonic() - t_agg), 3)
    t_liq = time.monotonic()
    _enrich_rows_liquidity_usd(uniq_rows, max_seconds=4 if not hard_scan else 10)
    timings["liquidity_usd_enrich_sec"] = round(max(0.0, time.monotonic() - t_liq), 3)
    _apply_creation_dates_phase(uniq_rows, include_creation_dates=include_creation_dates)
    v3_ctr = {"scanned": 0, "kept": 0, "skipped0": 0, "invalid": 0}
    for d in debug_rows:
        attempts = (d or {}).get("attempts") or []
        if not isinstance(attempts, list):
            continue
        for a in attempts:
            if not isinstance(a, dict):
                continue
            vd = a.get("v3_debug") or a.get("pancake_v3_debug") or {}
            if not isinstance(vd, dict):
                continue
            v3_ctr["scanned"] += int(vd.get("scanned_token_ids") or 0)
            v3_ctr["kept"] += int(vd.get("kept_positions") or 0)
            v3_ctr["skipped0"] += int(vd.get("skipped_zero_liq") or 0)
            v3_ctr["invalid"] += int(vd.get("invalid_positions") or 0)
    if POSITIONS_CONTRACT_ONLY_ENABLED or int(v3_ctr["scanned"]) > 0 or int(v3_ctr["kept"]) > 0:
        timings["v3_contract_scan"] = {
            "scanned_token_ids": int(v3_ctr["scanned"]),
            "kept_positions": int(v3_ctr["kept"]),
            "skipped_zero_liq": int(v3_ctr["skipped0"]),
            "invalid_positions": int(v3_ctr["invalid"]),
        }
    dedup_errors = list(dict.fromkeys(errors))
    if timed_out:
        elapsed_total = max(0.0, time.monotonic() - scan_started)
        if use_fast_chain_caps and elapsed_total < float(POSITIONS_SCAN_MAX_SECONDS) - 1.0:
            dedup_errors.append("Pool scan reached fast-mode budget. Showing partial results.")
        else:
            dedup_errors.append(
                f"Pool scan timed out after {int(scan_budget_sec)}s. Showing partial results."
            )
    timings["rows_before_aggregate"] = int(rows_before_aggregate)
    timings["rows_after_aggregate"] = int(len(uniq_rows))
    timings["contract_only_mode"] = bool(POSITIONS_CONTRACT_ONLY_ENABLED)
    timings["parallelism_disabled"] = bool(POSITIONS_DISABLE_PARALLELISM)
    timings["chain_durations_sec"] = chain_durations_sec
    if chain_progress:
        now_mono = time.monotonic()
        inflight: dict[str, dict[str, Any]] = {}
        for ck, st in chain_progress.items():
            if not isinstance(st, dict):
                continue
            item = dict(st)
            try:
                started_at = float(item.get("started_at_monotonic") or 0.0)
            except Exception:
                started_at = 0.0
            if started_at > 0:
                item["running_for_sec"] = round(max(0.0, now_mono - started_at), 3)
            if str(item.get("status") or "").strip().lower() != "done":
                inflight[str(ck)] = item
        if inflight:
            timings["unfinished_chain_progress"] = inflight
    finished_chain_keys = {str(k).strip().lower() for k in (chain_durations_sec or {}).keys() if str(k).strip()}
    unfinished_chain_keys: list[str] = []
    unfinished_seen: set[str] = set()
    for cid in ordered_chain_ids:
        ck = str(CHAIN_ID_TO_KEY.get(int(cid), str(cid)) or str(cid)).strip().lower()
        if ck and ck not in finished_chain_keys and ck not in unfinished_seen:
            unfinished_seen.add(ck)
            unfinished_chain_keys.append(ck)
    timings["unfinished_chains"] = unfinished_chain_keys
    timings["timed_out"] = bool(timed_out)
    timings["total_sec"] = round(max(0.0, time.monotonic() - scan_started), 3)
    return uniq_rows, dedup_errors, debug_rows, timings


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


def _infinity_indexer_daily_loop(interval_sec: int, run_on_startup: bool) -> None:
    if run_on_startup:
        try:
            result = _run_infinity_indexer_daily_once()
            if str(result.get("status") or "") == "skipped":
                _indexer_log_run("infinity_bsc", "skip", f"daily_startup reason={result.get('reason') or 'unknown'}")
        except Exception as e:
            _indexer_log_run("infinity_bsc", "error", f"daily_startup error={str(e)[:220]}")
    while not INFINITY_INDEXER_DAILY_STOP.wait(interval_sec):
        try:
            result = _run_infinity_indexer_daily_once()
            if str(result.get("status") or "") == "skipped":
                _indexer_log_run("infinity_bsc", "skip", f"daily reason={result.get('reason') or 'unknown'}")
        except Exception as e:
            _indexer_log_run("infinity_bsc", "error", f"daily_loop error={str(e)[:220]}")


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


def _start_infinity_indexer_daily() -> None:
    global INFINITY_INDEXER_DAILY_THREAD
    if not INFINITY_INDEXER_DAILY_ENABLED:
        return
    if INFINITY_INDEXER_DAILY_THREAD and INFINITY_INDEXER_DAILY_THREAD.is_alive():
        return
    INFINITY_INDEXER_DAILY_STOP.clear()
    INFINITY_INDEXER_DAILY_THREAD = threading.Thread(
        target=_infinity_indexer_daily_loop,
        args=(INFINITY_INDEXER_DAILY_INTERVAL_SEC, INFINITY_INDEXER_DAILY_ON_STARTUP),
        daemon=True,
        name="infinity-indexer-daily",
    )
    INFINITY_INDEXER_DAILY_THREAD.start()


def _stop_catalog_auto_refresh() -> None:
    CATALOG_REFRESH_STOP.set()


def _stop_infinity_indexer_daily() -> None:
    INFINITY_INDEXER_DAILY_STOP.set()


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
    return _canonical_wrapped_native_symbol(sym)


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
                if run_v3 and run_v4:
                    _set_stage("v3v4", f"Running v3+v4 ({idx}/{total_pairs}): {pair_str}", min(78, base_progress + 18))

                    def _run_agent(script_name: str) -> dict[str, dict]:
                        env_local = dict(env)
                        _run_subprocess(script_name, env_local, req.min_tvl, logs)
                        if script_name == "agent_v3.py":
                            p = DATA_DIR / f"pools_v3_{pair_suffix}.json"
                        else:
                            p = DATA_DIR / f"pools_v4_{pair_suffix}.json"
                        return load_chart_data_json(str(p))

                    with ThreadPoolExecutor(max_workers=2) as ex:
                        f_v3 = ex.submit(_run_agent, "agent_v3.py")
                        f_v4 = ex.submit(_run_agent, "agent_v4.py")
                        merged_raw.update(f_v3.result())
                        merged_raw.update(f_v4.result())
                else:
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


class AdminIndexerUpdate(BaseModel):
    name: str = "infinity_bsc"
    enabled: bool
    mode: str = "auto"
    max_receipts: int = INFINITY_INDEXER_MAX_RECEIPTS


class AdminIndexerRunRequest(BaseModel):
    name: str = "infinity_bsc"
    chain_id: int = 56
    owner: str
    max_receipts: int | None = None


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
    hard_scan: bool = False
    deep_infinity_scan: bool = False
    infinity_scan: bool = False
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


class PositionsRowEnrichRequest(BaseModel):
    row: dict[str, Any] = Field(default_factory=dict)


def _build_row_updates_from_snapshot(row: dict[str, Any], snap: dict[str, Any], chain_id: int) -> dict[str, Any]:
    dec0 = _parse_int_like(snap.get("token0_decimals") or 18)
    dec1 = _parse_int_like(snap.get("token1_decimals") or 18)
    if dec0 <= 0 or dec0 > 36:
        dec0 = 18
    if dec1 <= 0 or dec1 > 36:
        dec1 = 18
    amount0 = snap.get("quote_amount0")
    amount1 = snap.get("quote_amount1")
    fee0 = snap.get("quote_fee0")
    fee1 = snap.get("quote_fee1")
    try:
        if fee0 is None:
            fee0 = float(Decimal(int(snap.get("tokens_owed0_raw") or 0)) / (Decimal(10) ** dec0))
        if fee1 is None:
            fee1 = float(Decimal(int(snap.get("tokens_owed1_raw") or 0)) / (Decimal(10) ** dec1))
    except Exception:
        pass

    def _fmt_amt(v: float | None, *, zero_if_missing: bool = False) -> str:
        if v is None:
            return "0" if zero_if_missing else "-"
        av = abs(float(v))
        if av >= 1000:
            s = f"{float(v):,.1f}"
        elif av >= 1:
            s = f"{float(v):,.2f}"
        elif av >= 0.01:
            s = f"{float(v):,.3f}"
        else:
            s = f"{float(v):,.4f}"
        return s.rstrip("0").rstrip(".")

    liq_raw = max(0, int(snap.get("liquidity") or 0))
    a0_now = max(0.0, float(amount0 or 0.0))
    a1_now = max(0.0, float(amount1 or 0.0))
    status = "inactive" if (a0_now <= 0.0 or a1_now <= 0.0) else "active"
    token0_addr = str(snap.get("token0") or row.get("token0_id") or "").strip().lower()
    token1_addr = str(snap.get("token1") or row.get("token1_id") or "").strip().lower()
    liq_usd = None
    try:
        prices = _get_token_prices_usd(int(chain_id), [token0_addr, token1_addr])
        p0 = _safe_float(prices.get(token0_addr))
        p1 = _safe_float(prices.get(token1_addr))
        usd = 0.0
        if a0_now > 0 and p0 > 0:
            usd += a0_now * p0
        if a1_now > 0 and p1 > 0:
            usd += a1_now * p1
        if usd > 0:
            liq_usd = float(usd)
    except Exception:
        liq_usd = None
    liq_disp = _format_usd_compact(liq_usd)
    fee_raw = str(snap.get("fee") or "").strip()
    fee_disp = fee_raw
    try:
        fee_int = int(fee_raw)
        if fee_int > 0:
            fee_disp = f"{fee_int / 10000.0:.2f}%"
    except Exception:
        fee_disp = fee_raw or "-"
    return {
        "position_amount0": (float(amount0) if amount0 is not None else 0.0),
        "position_amount1": (float(amount1) if amount1 is not None else 0.0),
        "fees_owed0": (float(fee0) if fee0 is not None else 0.0),
        "fees_owed1": (float(fee1) if fee1 is not None else 0.0),
        "fee_tier_raw": fee_raw,
        "fee_tier": fee_disp,
        "position_amounts_display": f"{_fmt_amt(amount0)} / {_fmt_amt(amount1)}",
        "fees_owed_display": f"{_fmt_amt(fee0, zero_if_missing=True)} / {_fmt_amt(fee1, zero_if_missing=True)}",
        "position_status": status,
        "liquidity": str(liq_raw),
        "liquidity_display": liq_disp,
        "liquidity_usd": liq_usd,
        "token0_id": token0_addr,
        "token1_id": token1_addr,
        "position_symbol0": _normalize_display_symbol(str(snap.get("token0_symbol") or row.get("position_symbol0") or "")),
        "position_symbol1": _normalize_display_symbol(str(snap.get("token1_symbol") or row.get("position_symbol1") or "")),
        "pair": (
            f"{_normalize_display_symbol(str(snap.get('token0_symbol') or row.get('position_symbol0') or '?'))}/"
            f"{_normalize_display_symbol(str(snap.get('token1_symbol') or row.get('position_symbol1') or '?'))}"
        ),
        "pair_symbol_source0": "snapshot_symbol",
        "pair_symbol_source1": "snapshot_symbol",
        "pair_symbol_source": "0:snapshot_symbol | 1:snapshot_symbol",
        "suspected_spam": False,
        "spam_skipped": False,
    }


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
    .pos-status {
      color:#475569;
      font-size:13px;
      display:inline-block;
      min-width: 300px;
      max-width: 520px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      vertical-align: middle;
    }
    .table-wrap { overflow-x:auto; border:1px solid #dbe3ef; border-radius:10px; background:#f8fbff; }
    table { width:100%; border-collapse:collapse; font-size:12px; min-width:860px; }
    th, td { border-bottom:1px solid #e2e8f0; padding:5px 7px; text-align:left; vertical-align:top; }
    th { background:#eff6ff; color:#1e3a8a; position:sticky; top:0; font-size:12px; font-weight:700; padding:6px 6px; }
    #posPoolsTable { min-width: 1200px; table-layout: auto; }
    #posPoolsTable th, #posPoolsTable td { white-space: nowrap; }
    #posPoolsTable th:nth-child(1), #posPoolsTable td:nth-child(1) {
      position: sticky; left: 0; z-index: 4; background: #f8fbff;
    }
    #posPoolsTable th:nth-child(1) { background: #eff6ff; z-index: 5; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size:11px; }
    .status-dot {
      display:inline-block;
      width:8px;
      height:8px;
      border-radius:999px;
      vertical-align:middle;
      border:1px solid transparent;
    }
    .status-dot.active { background:#7d9f89; border-color:#6f8e7a; }
    .status-dot.inactive { background:#ab8787; border-color:#987676; }
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
      .pos-status { font-size: 12px; min-width: 180px; max-width: 72vw; }
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
            <button id="posInfinityScanBtn" class="search-link-btn" type="button" onclick="scanPositions('pools', {infinityScan:true})">Infinity scan</button>
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
      if (kind === "evm") scheduleBackgroundWarmup("add");
    }
    function removeAddress(kind, idx) {
      if (!Array.isArray(posState[kind])) return;
      posState[kind].splice(Number(idx) || 0, 1);
      savePosState();
      renderChips(kind);
      if (kind === "evm" && (posState.evm || []).length) scheduleBackgroundWarmup("remove");
    }
    const posSectionState = {pools: false};
    const posCache = {pools: []};
    const posHistorySelected = new Set();
    const POS_RESULTS_STORAGE_KEY = "positions_scan_results_v1";
    const POS_HIDDEN_EXPANDED_KEY = "positions_hidden_expanded_v1";
    let posHasScannedOnce = false;
    let posScanTicker = null;
    let posScanStartedAt = 0;
    let posScanPlannedOwnerChainTotal = 0;
    let posLastStatusText = "";
    let posLastStatusErr = false;
    let posAutoWarmupTimer = null;
    let posAutoWarmupInFlight = false;
    function setPosStatus(text, isErr) {
      const el = document.getElementById("posStatus");
      if (!el) return;
      const nextText = String(text || "");
      const nextErr = !!isErr;
      if (nextText === posLastStatusText && nextErr === posLastStatusErr) return;
      posLastStatusText = nextText;
      posLastStatusErr = nextErr;
      el.textContent = nextText;
      el.style.color = nextErr ? "#b91c1c" : "#475569";
    }
    function setPosBusy(flag) {
      const el = document.getElementById("posProgress");
      const scanBtn = document.getElementById("posSearchBtn");
      const infBtn = document.getElementById("posInfinityScanBtn");
      if (el) el.style.display = flag ? "block" : "none";
      if (scanBtn) scanBtn.disabled = !!flag;
      if (infBtn) infBtn.disabled = !!flag;
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
    async function startBackgroundWarmup(reason = "auto") {
      if (posAutoWarmupInFlight) return;
      if (!Array.isArray(posState.evm) || !posState.evm.length) return;
      const existing = loadActivePosJob();
      if (existing) return;
      posAutoWarmupInFlight = true;
      try {
        const res = await fetch("/api/positions/scan/start", {
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
        if (!res.ok) return;
        const jobId = String(data.job_id || "").trim();
        if (!jobId) return;
        saveActivePosJob(jobId);
        if (reason !== "silent") setPosStatus("Background analysis started.", false);
      } catch (_) {
      } finally {
        posAutoWarmupInFlight = false;
      }
    }
    function scheduleBackgroundWarmup(reason = "auto") {
      // Temporarily disabled: no additional background scan passes.
      return;
    }
    function stopPosScanProgressTicker() {
      if (posScanTicker) {
        clearInterval(posScanTicker);
        posScanTicker = null;
      }
      posScanStartedAt = 0;
      posScanPlannedOwnerChainTotal = 0;
    }
    function startPosScanProgressTicker() {
      stopPosScanProgressTicker();
      posScanStartedAt = Date.now();
      const plannedChains = ["Arbitrum", "Base", "Ethereum", "Optimism", "Polygon", "BSC", "Unichain"];
      const evmOwners = Math.max(1, Number((posState.evm || []).length || 0));
      posScanPlannedOwnerChainTotal = Math.max(1, evmOwners * plannedChains.length);
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
        const totalChecks = Math.max(1, posScanPlannedOwnerChainTotal || 1);
        const estRate = totalChecks / 35.0;
        const estDone = Math.max(0, Math.min(totalChecks, Math.floor(elapsed * estRate)));
        const progress = `Progress ~${estDone}/${totalChecks} owner-chain checks`;
        if (elapsed >= 20) phase = "Indexer/rpc responses";
        if (elapsed >= 28) {
          phase = "Final merge and response";
          setPosStatus(`Scanning positions... ${elapsed}s | ${phase} (${progress}; Aggregating results)`, false);
          return;
        }
        setPosStatus(`Scanning positions... ${elapsed}s | ${phase} (${chainHint}; ${progress})`, false);
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
    async function enrichTrustedSpamRow(key) {
      const rowKey = String(key || "");
      if (!rowKey) return;
      const list = Array.isArray(posCache?.pools) ? posCache.pools : [];
      let idx = -1;
      for (let i = 0; i < list.length; i++) {
        if (poolRowKey(list[i] || {}) === rowKey) { idx = i; break; }
      }
      if (idx < 0) return;
      const row = Object.assign({}, list[idx] || {});
      try {
        const res = await fetch("/api/positions/row/enrich", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({row}),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data?.ok || typeof data?.row_updates !== "object") return;
        list[idx] = Object.assign({}, row, data.row_updates || {});
        posCache.pools = list;
        renderPools(posCache.pools || []);
        setPosStatus("Position details loaded.", false);
      } catch (_) {}
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
      if (!hidden && suspected) {
        enrichTrustedSpamRow(rowKey);
      }
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
      const dbgTimings = (dbg && typeof dbg.timings === "object" && dbg.timings) ? dbg.timings : {};
      let dbgHtml = "";
      if (dbgSummary.length || Object.keys(dbgTimings).length) {
        const infoText = infos.map((x) => String(x || "")).join(" | ").toLowerCase();
        const infinityMode = infoText.includes("infinity scan mode");
        const summaryFiltered = dbgSummary.filter((x) => String(x?.query_mode || "") !== "hard_scan_required_for_live_discovery");
        const cacheHits = summaryFiltered
          .filter((x) => String(x?.query_mode || "") === "ownership_cache")
          .reduce((acc, x) => acc + Math.max(0, Number(x?.count || 0)), 0);
        const v4Rows = summaryFiltered.filter((x) => String(x?.query_mode || "") === "contract_only_onchain_uniswap_v4_pm:onchain");
        const v4Attempted = v4Rows.length;
        const v4Found = v4Rows
          .filter((x) => String(x?.status || "") === "ok")
          .reduce((acc, x) => acc + Math.max(0, Number(x?.count || 0)), 0);
        const v4Errors = v4Rows.filter((x) => String(x?.status || "") !== "ok").length;
        const modeText = infinityMode ? "infinity" : "fast";
        const timingLines = [];
        const ts = (k) => Number(dbgTimings?.[k] || 0);
        if (ts("total_sec") > 0) timingLines.push(`total=${ts("total_sec")}s`);
        if (ts("prepare_request_sec") > 0) timingLines.push(`prepare=${ts("prepare_request_sec")}s`);
        if (ts("evm_components_sec") > 0) timingLines.push(`evm=${ts("evm_components_sec")}s`);
        if (ts("sort_rows_sec") > 0) timingLines.push(`sort=${ts("sort_rows_sec")}s`);
        if (ts("build_debug_sec") > 0) timingLines.push(`debug=${ts("build_debug_sec")}s`);
        if (ts("analytics_sec") > 0) timingLines.push(`analytics=${ts("analytics_sec")}s`);
        const pool = (dbgTimings?.evm && typeof dbgTimings.evm.pool === "object") ? dbgTimings.evm.pool : {};
        if (Number(pool?.total_sec || 0) > 0) {
          timingLines.push(`pool=${Number(pool.total_sec)}s`);
        }
        if (Number(pool?.priority_chains_sec || 0) > 0 || Number(pool?.remaining_chains_sec || 0) > 0) {
          timingLines.push(`chains(prio/rest)=${Number(pool.priority_chains_sec || 0)}s/${Number(pool.remaining_chains_sec || 0)}s`);
        }
        const chainDur = (pool && typeof pool.chain_durations_sec === "object" && pool.chain_durations_sec) ? pool.chain_durations_sec : {};
        const slowChains = Object.entries(chainDur)
          .map(([k, v]) => [String(k), Number(v || 0)])
          .filter((x) => Number(x[1]) > 0)
          .sort((a, b) => Number(b[1]) - Number(a[1]))
          .slice(0, 5)
          .map((x) => `${x[0]}=${x[1]}s`);
        if (slowChains.length) {
          timingLines.push(`slow_chains: ${slowChains.join(", ")}`);
        }
        const unfinished = Array.isArray(pool?.unfinished_chains) ? pool.unfinished_chains.map((x) => String(x || "")).filter(Boolean) : [];
        if (unfinished.length) timingLines.push(`unfinished_chains: ${unfinished.join(", ")}`);
        const unfinishedProgress = (pool && typeof pool.unfinished_chain_progress === "object" && pool.unfinished_chain_progress) ? pool.unfinished_chain_progress : {};
        const unfinishedDetails = Object.entries(unfinishedProgress)
          .map(([k, v]) => {
            const d = (v && typeof v === "object") ? v : {};
            const stage = String(d.stage || "?");
            const ver = String(d.version || "");
            const owner = String(d.owner || "");
            const t = Number(d.running_for_sec || 0);
            const ownerPart = owner ? ` owner=${shortAddr4(owner)}` : "";
            const verPart = ver ? ` ${ver}` : "";
            return `${String(k)}:${stage}${verPart}${ownerPart}${t > 0 ? ` ${t}s` : ""}`;
          })
          .filter(Boolean);
        if (unfinishedDetails.length) timingLines.push(`unfinished_detail: ${unfinishedDetails.join(", ")}`);
        const v3Scan = (pool && typeof pool.v3_contract_scan === "object" && pool.v3_contract_scan) ? pool.v3_contract_scan : {};
        timingLines.push(
          `v3_scan: scanned=${Number(v3Scan.scanned_token_ids || 0)} `
          + `kept=${Number(v3Scan.kept_positions || 0)} `
          + `skipped0=${Number(v3Scan.skipped_zero_liq || 0)} `
          + `invalid=${Number(v3Scan.invalid_positions || 0)}`
        );
        if (!infinityMode) {
          const compactLines = summaryFiltered
            .map((x) => `${esc(x.chain || "?")}/${esc(x.version || "?")} ${esc(x.query_mode || "?")}: ${Number(x.count || 0)}`);
          const body = []
            .concat([`mode=${modeText} | cache_hits=${cacheHits}`])
            .concat([`v4: attempted=${v4Attempted} found=${v4Found} errors=${v4Errors}`])
            .concat(timingLines.length ? [`time: ${timingLines.join(" | ")}`] : [])
            .concat(compactLines.length ? compactLines : [])
            .join("\\n");
          if (body.trim()) {
            dbgHtml = `<div class='info-box'><details><summary>Debug scan (compact)</summary><pre style='margin:8px 0 0;white-space:pre-wrap'>${body}</pre></details></div>`;
          }
        } else {
          const detailedLines = summaryFiltered
            .map((x) => `${esc(x.chain || "?")}/${esc(x.version || "?")} ${esc(x.query_mode || "?")} [${esc(x.status || "?")}]: ${Number(x.count || 0)}`);
          const body = []
            .concat([`mode=${modeText} | cache_hits=${cacheHits}`])
            .concat([`v4: attempted=${v4Attempted} found=${v4Found} errors=${v4Errors}`])
            .concat(timingLines.length ? [`time: ${timingLines.join(" | ")}`] : [])
            .concat(detailedLines.length ? detailedLines : [])
            .join("\\n");
          if (body.trim()) {
            dbgHtml = `<div class='info-box'><details><summary>Debug scan details</summary><pre style='margin:8px 0 0;white-space:pre-wrap'>${body}</pre></details></div>`;
          }
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
    function normPosSym(v) {
      const s = String(v || "").trim().toUpperCase();
      if (s === "WETH" || s === "WETH.E" || s === "WETH9") return "ETH";
      if (s === "WBNB") return "BNB";
      if (s === "WMATIC" || s === "MATIC" || s === "WPOL") return "POL";
      if (s === "WAVAX") return "AVAX";
      if (s === "WFTM") return "FTM";
      if (s === "WCELO") return "CELO";
      return s;
    }
    function pairParts(v) {
      const s = String(v || "");
      if (!s.includes("/")) return ["", ""];
      const parts = s.split("/", 2);
      return [normPosSym(parts[0]), normPosSym(parts[1])];
    }
    function hasPairMismatch(r) {
      const [p0, p1] = pairParts(r?.pair || "");
      const s0 = normPosSym(r?.position_symbol0 || "");
      const s1 = normPosSym(r?.position_symbol1 || "");
      if (!p0 || !p1 || !s0 || !s1) return false;
      return p0 !== s0 || p1 !== s1;
    }
    function mismatchHint(r) {
      return `Pair=${String(r?.pair || "-")} | Position symbols=${String(r?.position_symbol0 || "-")}/${String(r?.position_symbol1 || "-")}`;
    }
    function shortProtocol(v) {
      const p = String(v || "").trim().toLowerCase();
      if (!p) return "";
      if (p === "uniswap_v3") return "UNI_V3";
      if (p === "uniswap_v4") return "UNI_V4";
      if (p === "pancake_v3" || p === "pancake_v3_staked") return "PanC_V3";
      if (p === "pancake_infinity_cl" || p === "pancake_infinity_bin") return "PanC_INF";
      return String(v || "");
    }
    function statusDot(status) {
      const s = String(status || "").trim().toLowerCase();
      const isActive = s === "active";
      const cls = isActive ? "active" : "inactive";
      const title = isActive ? "active" : "inactive";
      return `<span class="status-dot ${cls}" title="${title}"></span>`;
    }
    function escAttr(v) {
      return esc(v).replace(/"/g, "&quot;");
    }
    function renderPools(rows) {
      const table = document.getElementById("posPoolsTable");
      const trustedSpamKeys = getTrustedSpamKeys();
      const manualHiddenKeys = getManualHiddenKeys();
      const hiddenExpanded = localStorage.getItem(POS_HIDDEN_EXPANDED_KEY) === "1";
      let html = `<tr><th>Address</th><th>Position ID</th><th>Chain</th><th>Protocol</th><th>Pair</th><th>Fee tier</th><th>Created</th><th>Status</th><th title='Exact amounts currently in the position'>In position</th><th>Liquidity</th><th title='Unclaimed fees currently owed by position NFT'>Unclaimed fees</th><th>Hide</th><th>History</th></tr>`;
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
        const mismatch = hasPairMismatch(r);
        const mismatchCellStyle = mismatch ? " style='background:#fff7ed;color:#9a3412;font-weight:600'" : "";
        const pairTrace = String(r.pair_symbol_source || "").trim();
        const pairTitleRaw = mismatch
          ? `${mismatchHint(r)}${pairTrace ? ` | source: ${pairTrace}` : ""}`
          : (pairTrace ? `source: ${pairTrace}` : "");
        const mismatchTitle = pairTitleRaw ? ` title="${escAttr(pairTitleRaw)}"` : "";
        html += "<tr>";
        html += `<td class='mono' style='font-weight:700'>${esc(shortAddr4(r.address || ""))}<button class='copy-btn' type='button' onclick="copyText('${esc(String(r.address || "").replace(/'/g, "\\\\'"))}')" title='Copy address'>⧉</button></td>`;
        html += `<td class='mono'>${esc(shortAddr4(r.position_id || ""))}<button class='copy-btn' type='button' onclick="copyText('${esc(String(r.position_id || "").replace(/'/g, "\\\\'"))}')" title='Copy position id'>⧉</button></td>`;
        html += `<td>${esc(r.chain || "")}</td>`;
        html += `<td>${esc(shortProtocol(r.protocol || ""))}</td>`;
        const rowKeyEsc = esc(String(r._row_key || "").replace(/'/g, "\\\\'"));
        html += `<td${mismatchCellStyle}${mismatchTitle}>${esc(r.pair || "")}${mismatch ? " ⚠" : ""}</td>`;
        const feeRaw = String(r.fee_tier_raw || "").trim();
        const feeTip = feeRaw ? ` title="raw: ${esc(feeRaw)}"` : "";
        html += `<td${feeTip}>${esc(r.fee_tier || "")}</td>`;
        html += `<td>${esc(r.position_created_date || "-")}</td>`;
        html += `<td>${statusDot(r.position_status || "-")}</td>`;
        html += `<td${mismatchCellStyle}${mismatchTitle}>${esc(r.position_amounts_display || "-")}</td>`;
        html += `<td>${esc(String(r.liquidity_display || "0"))}</td>`;
        html += `<td${mismatchCellStyle}${mismatchTitle}>${esc(r.fees_owed_display || "-")}</td>`;
        html += `<td><input type='checkbox' onchange="setHideRow('${rowKeyEsc}', this.checked, ${Boolean(r._is_suspected_spam) ? "true" : "false"})" /></td>`;
        const checked = posHistorySelected.has(Number(r._src_idx) || 0) ? "checked" : "";
        html += `<td><input type='checkbox' ${checked} onchange='setHistorySelected(${Number(r._src_idx) || 0}, this.checked)' /></td>`;
        html += "</tr>";
      }
      if (!visible.length) html += "<tr><td colspan='13'>No pool positions found.</td></tr>";
      if (hiddenRows.length) {
        let hiddenInner = "<table style='width:100%;border-collapse:collapse;font-size:12px'>";
        hiddenInner += "<tr><th style='text-align:left;padding:4px 6px'>Address</th><th style='text-align:left;padding:4px 6px'>Position ID</th><th style='text-align:left;padding:4px 6px'>Chain</th><th style='text-align:left;padding:4px 6px'>Protocol</th><th style='text-align:left;padding:4px 6px'>Pair</th><th style='text-align:left;padding:4px 6px'>Fee tier</th><th style='text-align:left;padding:4px 6px'>Status</th><th style='text-align:left;padding:4px 6px'>In position</th><th style='text-align:left;padding:4px 6px'>Liquidity</th><th style='text-align:left;padding:4px 6px'>Unclaimed fees</th><th style='text-align:left;padding:4px 6px'>Hide</th><th style='text-align:left;padding:4px 6px'>History</th></tr>";
        for (let i = 0; i < hiddenRows.length; i++) {
          const r = hiddenRows[i];
          const mismatch = hasPairMismatch(r);
          const mismatchStyle = mismatch ? "background:#fff7ed;color:#9a3412;font-weight:600;" : "";
          const pairTrace = String(r.pair_symbol_source || "").trim();
          const pairTitleRaw = mismatch
            ? `${mismatchHint(r)}${pairTrace ? ` | source: ${pairTrace}` : ""}`
            : (pairTrace ? `source: ${pairTrace}` : "");
          const mismatchTitle = pairTitleRaw ? ` title="${escAttr(pairTitleRaw)}"` : "";
          const rowKey = String(r._row_key || "");
          const rowKeyEsc = esc(rowKey.replace(/'/g, "\\\\'"));
          const checked = posHistorySelected.has(Number(r._src_idx) || 0) ? "checked" : "";
          hiddenInner += `<tr><td class='mono' style='padding:3px 6px;font-weight:700'>${esc(shortAddr4(r.address || ""))}</td><td class='mono' style='padding:3px 6px'>${esc(shortAddr4(r.position_id || ""))}</td><td style='padding:3px 6px'>${esc(r.chain || "")}</td><td style='padding:3px 6px'>${esc(shortProtocol(r.protocol || ""))}</td><td style='padding:3px 6px;${mismatchStyle}'${mismatchTitle}>${esc(r.pair || "")}${mismatch ? " ⚠" : ""}</td><td style='padding:3px 6px'>${esc(r.fee_tier || "")}</td><td style='padding:3px 6px'>${statusDot(r.position_status || "-")}</td><td style='padding:3px 6px;${mismatchStyle}'${mismatchTitle}>${esc(r.position_amounts_display || "-")}</td><td style='padding:3px 6px'>${esc(String(r.liquidity_display || "0"))}</td><td style='padding:3px 6px;${mismatchStyle}'${mismatchTitle}>${esc(r.fees_owed_display || "-")}</td><td style='padding:3px 6px'><input type='checkbox' checked onchange="setHideRow('${rowKeyEsc}', this.checked, ${Boolean(r._is_suspected_spam) ? "true" : "false"})" /></td><td style='padding:3px 6px'><input type='checkbox' ${checked} onchange='setHistorySelected(${Number(r._src_idx) || 0}, this.checked)' /></td></tr>`;
        }
        hiddenInner += "</table>";
        const openAttr = hiddenExpanded ? " open" : "";
        html += `<tr><td colspan='13'><details id='posHiddenDetails'${openAttr}><summary>Hidden positions (${hiddenRows.length})</summary><div style='margin-top:8px;max-height:220px;overflow:auto'>${hiddenInner}</div></details></td></tr>`;
      }
      table.innerHTML = html;
      const detailsEl = document.getElementById("posHiddenDetails");
      if (detailsEl) {
        detailsEl.addEventListener("toggle", () => {
          localStorage.setItem(POS_HIDDEN_EXPANDED_KEY, detailsEl.open ? "1" : "0");
        });
      }
    }
    const POS_ACTIVE_JOB_KEY = "positions_active_job_v1";
    function saveActivePosJob(jobId) {
      try {
        if (jobId) localStorage.setItem(POS_ACTIVE_JOB_KEY, String(jobId));
        else localStorage.removeItem(POS_ACTIVE_JOB_KEY);
      } catch (_) {}
    }
    function loadActivePosJob() {
      try { return String(localStorage.getItem(POS_ACTIVE_JOB_KEY) || "").trim(); } catch (_) { return ""; }
    }
    async function pollPosJob(jobId, allowPartialReturn = false) {
      const jid = String(jobId || "").trim();
      if (!jid) throw new Error("Missing job id");
      let partialRendered = false;
      function statusTailFromPayload(payload) {
        const errs = Array.isArray(payload?.errors) ? payload.errors : [];
        const infos = Array.isArray(payload?.infos) ? payload.infos : [];
        const maxTail = 52;
        const cut = (v) => {
          const s = String(v || "").replace(/\s+/g, " ").trim();
          if (!s) return "";
          return s.length > maxTail ? (s.slice(0, maxTail - 1) + "…") : s;
        };
        if (errs.length) return ` | ${cut(errs[0] || "")}`;
        if (infos.length) return ` | ${cut(infos[0] || "")}`;
        return "";
      }
      function statusMetrics(payload) {
        const pools = Array.isArray(payload?.pool_positions) ? payload.pool_positions.length : 0;
        let cacheHits = 0;
        const sum = Array.isArray(payload?.debug?.summary) ? payload.debug.summary : [];
        for (const s of sum) {
          if (String(s?.query_mode || "") === "ownership_cache") {
            cacheHits += Math.max(0, Number(s?.count || 0));
          }
        }
        return ` | p=${pools}${cacheHits ? ` c=${cacheHits}` : ""}`;
      }
      function statusStageLabel(raw, st, partialRendered) {
        const src = String(raw || "").trim().toLowerCase();
        if (src.includes("enrich")) return "Background enrich";
        if (src.includes("finaliz")) return "Finalizing";
        if (src.includes("infinity")) return "Infinity scan";
        if (src.includes("scan")) return partialRendered ? "Background scan" : "Scanning";
        if (String(st || "") === "running") return partialRendered ? "Background scan" : "Scanning";
        return partialRendered ? "Background scan" : "Scanning";
      }
      while (true) {
        const r = await fetch(`/api/positions/scan/job/${encodeURIComponent(jid)}`);
        const data = await r.json().catch(() => ({}));
        if (!r.ok) throw new Error(data.detail || "Job polling failed");
        const st = String(data.status || "");
        const stageLabel = String(data.stage_label || data.stage || "");
        const progress = Number(data.progress || 0);
        const startedAt = Number(data.started_at || 0);
        const elapsedSec = startedAt > 0 ? Math.max(0, Math.floor(Date.now() / 1000 - startedAt)) : 0;
        const partial = data.result || {};
        if (partial && Array.isArray(partial.pool_positions) && partial.pool_positions.length) {
          posCache.pools = partial.pool_positions || [];
          renderPools(posCache.pools);
          renderScanMessages(partial);
          partialRendered = true;
        }
        if (st === "done") return data.result || {};
        if (st === "failed") throw new Error(data.error || "Scan failed");
        // Handoff as soon as base table is ready; continue heavier enrich in background.
        const stageKey = String(data.stage || "");
        const backgroundPhase = stageKey.startsWith("enrich_") || stageKey === "finalize" || progress >= 65;
        if (allowPartialReturn && partialRendered && backgroundPhase) {
          return Object.assign({__partial: true}, partial);
        }
        const statusTail = statusTailFromPayload(partial);
        const metrics = statusMetrics(partial);
        const elapsedTxt = elapsedSec > 0 ? ` ${elapsedSec}s` : "";
        let uiProgress = progress;
        if (st === "running" && progress <= 5) {
          // Backend can stay low during core scan; show a smooth front-end estimate meanwhile.
          uiProgress = Math.min(94, 5 + Math.floor(elapsedSec * 2.2));
        }
        const sLabel = statusStageLabel(stageLabel, st, partialRendered);
        const liveTag = partialRendered ? " | live" : "";
        setPosStatus(`${sLabel}${elapsedTxt} | ${uiProgress}%${metrics}${liveTag}${statusTail}`, false);
        await new Promise((resolve) => setTimeout(resolve, 1200));
      }
    }
    async function scanPositions(targetSection = "all", opts = {}) {
      let handoffToBackground = false;
      const options = (typeof opts === "boolean") ? {infinityScan: !!opts} : (opts || {});
      const infinityScan = !!options.infinityScan;
      const modeLabel = infinityScan ? "Infinity scan" : "scan";
      if (posHasScannedOnce) {
        const ok = window.confirm(`Run ${modeLabel} again and replace current results?`);
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
        const startRes = await fetch("/api/positions/scan/start", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify({
            evm_addresses: posState.evm,
            solana_addresses: posState.solana,
            tron_addresses: posState.tron,
            include_pools: true,
            include_lending: false,
            include_rewards: false,
            hard_scan: false,
            deep_infinity_scan: false,
            infinity_scan: !!infinityScan,
          }),
        });
        const startData = await startRes.json().catch(() => ({}));
        if (!startRes.ok) throw new Error(startData.detail || "Failed to start scan");
        const jobId = String(startData.job_id || "").trim();
        if (!jobId) throw new Error("Invalid job id");
        saveActivePosJob(jobId);
        const data = await pollPosJob(jobId, true);
        if (data && data.__partial) {
          handoffToBackground = true;
          setPosStatus(`Base results loaded (${modeLabel}). Hard-like enrich continues in background...`, false);
          setTimeout(() => { resumePosJobIfAny(); }, 300);
          return;
        }
        saveActivePosJob("");
        posCache.pools = data.pool_positions || [];
        renderPools(posCache.pools);
        renderScanMessages(data);
        const finishedChecks = Array.isArray(data?.debug?.pool_scan) ? data.debug.pool_scan.length : 0;
        savePosResults({
          saved_at: Date.now(),
          pool_positions: data.pool_positions || [],
          errors: data.errors || [],
          infos: data.infos || [],
          debug: data.debug || {},
        });
        posHasScannedOnce = true;
        updatePosSearchButton();
        const errCount = Array.isArray(data?.errors) ? data.errors.length : 0;
        const infoCount = Array.isArray(data?.infos) ? data.infos.length : 0;
        const firstInfo = infoCount ? String(data.infos[0] || "") : "";
        setPosStatus(
          `Done. Pools: ${(data.pool_positions || []).length}${finishedChecks ? ` | Owner-chain checks: ${finishedChecks}` : ""}${errCount ? ` | warnings: ${errCount}` : ""}${firstInfo ? ` | ${firstInfo}` : ""}`,
          false
        );
      } catch (e) {
        saveActivePosJob("");
        setPosStatus("Scan failed: " + (e?.message || "unknown"), true);
      } finally {
        if (!handoffToBackground) {
          setPosBusy(false);
        }
      }
    }
    async function resumePosJobIfAny() {
      const jobId = loadActivePosJob();
      if (!jobId) return;
      try {
        setPosBusy(true);
        const data = await pollPosJob(jobId);
        saveActivePosJob("");
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
        const errCount = Array.isArray(data?.errors) ? data.errors.length : 0;
        const firstInfo = Array.isArray(data?.infos) && data.infos.length ? String(data.infos[0] || "") : "";
        setPosStatus(
          `Done. Pools: ${(data.pool_positions || []).length}${errCount ? ` | warnings: ${errCount}` : ""}${firstInfo ? ` | ${firstInfo}` : ""}`,
          false
        );
      } catch (e) {
        saveActivePosJob("");
        setPosStatus("Background scan failed: " + (e?.message || "unknown"), true);
      } finally {
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
    resumePosJobIfAny();
    if ((posState.evm || []).length && !loadActivePosJob()) {
      scheduleBackgroundWarmup("silent");
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
    th, td { border-bottom:1px solid #e2e8f0; padding:5px 7px; text-align:left; vertical-align:top; }
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
      <section class="card">
        <h3>Indexers</h3>
        <p class="hint">Configure and run transaction-history indexers.</p>
        <div class="row"><label>Infinity indexer</label><div id="idxName">infinity_bsc</div></div>
        <div class="row"><label>Enabled</label><select id="idxEnabled"><option value="true">yes</option><option value="false">no</option></select></div>
        <div class="row"><label>Mode</label><select id="idxMode"><option value="auto">auto</option><option value="manual">manual</option><option value="off">off</option></select></div>
        <div class="row"><label>Max receipts</label><input id="idxMaxReceipts" type="number" min="20" max="2000" step="1" value="220"/></div>
        <div style="display:flex;gap:8px;flex-wrap:wrap">
          <button class="btn" onclick="saveIndexerConfig()">Save config</button>
          <button class="btn" onclick="loadIndexers()">Refresh indexer</button>
        </div>
        <hr style="margin:12px 0;border:none;border-top:1px solid #dbe3ef" />
        <div class="row"><label>Run for owner</label><input id="idxRunOwner" type="text" placeholder="0x..."/></div>
        <div class="row"><label>Chain id</label><input id="idxRunChainId" type="number" min="1" step="1" value="56"/></div>
        <button class="btn" onclick="runIndexerForOwner()">Run index now</button>
        <div style="margin-top:10px;font-size:13px;color:#334155">
          <div><b>Total records:</b> <span id="idxRecordsTotal">-</span></div>
          <div><b>Total owners:</b> <span id="idxOwnersTotal">-</span></div>
          <div><b>Ownership index records:</b> <span id="idxOwnershipRecordsTotal">-</span></div>
          <div><b>Ownership index owners:</b> <span id="idxOwnershipOwnersTotal">-</span></div>
          <div><b>Details cache rows:</b> <span id="idxDetailsCacheTotal">-</span></div>
          <div><b>Ownership by protocol:</b> <span id="idxOwnershipBreakdown">-</span></div>
          <div><b>Running now:</b> <span id="idxRunningNow">-</span></div>
          <div><b>Elapsed:</b> <span id="idxElapsedNow">-</span></div>
          <div><b>Progress:</b> <span id="idxProgressNow">-</span></div>
          <div><b>Current owner:</b> <span id="idxCurrentOwner">-</span></div>
          <div><b>Last run:</b> <span id="idxLastRun">-</span></div>
          <div><b>Last status:</b> <span id="idxLastStatus">-</span></div>
          <div><b>Last details:</b> <span id="idxLastDetails">-</span></div>
        </div>
        <div style="margin-top:8px">
          <div style="font-weight:700;font-size:13px;color:#334155;margin-bottom:4px">Indexer debug log (recent)</div>
          <pre id="idxDebugLog" style="max-height:160px;overflow:auto;margin:0">-</pre>
        </div>
        <span id="idxStatus" class="status">Ready</span>
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
    function setIndexerStatus(text, isErr) {{ const el=document.getElementById("idxStatus"); if(!el) return; el.textContent=text; el.style.color=isErr?"#b91c1c":"#475569"; }}
    function renderIndexerCard(item) {{
      const d = item || {{}};
      document.getElementById("idxEnabled").value = d.enabled ? "true" : "false";
      document.getElementById("idxMode").value = (d.mode || "auto");
      document.getElementById("idxMaxReceipts").value = String(Number(d.max_receipts || 220));
      document.getElementById("idxRecordsTotal").textContent = String(Number(d.records_total || 0));
      document.getElementById("idxOwnersTotal").textContent = String(Number(d.owners_total || 0));
      const own = d.ownership_index || {{}};
      document.getElementById("idxOwnershipRecordsTotal").textContent = String(Number(own.records_total || 0));
      document.getElementById("idxOwnershipOwnersTotal").textContent = String(Number(own.owners_total || 0));
      document.getElementById("idxDetailsCacheTotal").textContent = String(Number(own.details_cached_total || 0));
      const ownBreakdown = Array.isArray(own.protocol_breakdown) ? own.protocol_breakdown : [];
      document.getElementById("idxOwnershipBreakdown").textContent = ownBreakdown.length
        ? ownBreakdown.map((x) => `${{String(x.protocol || "-")}}:${{Number(x.count || 0)}}`).join(", ")
        : "-";
      const a = d.activity || {{}};
      const q = d.ownership_index_queue || {{}};
      const running = !!a.running;
      document.getElementById("idxRunningNow").textContent = running ? "yes" : "no";
      const processed = Number(a.processed || 0);
      const targets = Number(a.targets || 0);
      const updated = Number(a.updated || 0);
      const errors = Number(a.errors || 0);
      const startedAt = Number(a.started_at || 0);
      const elapsedSec = running && startedAt > 0 ? Math.max(0, Math.floor((Date.now()/1000) - startedAt)) : 0;
      document.getElementById("idxElapsedNow").textContent = running ? `${{elapsedSec}}s` : "-";
      const ev = String(a.last_event || "-");
      const err = String(a.last_error || "");
      const qInfo = q.enabled
        ? ` | strict=${{q.index_first_strict ? "on" : "off"}} legacy=${{q.legacy_discovery_enabled ? "on" : "off"}} queue=${{Number(q.queued||0)}} inflight=${{Number(q.inflight||0)}} workers=${{Number(q.workers||0)}}`
        : "";
      document.getElementById("idxProgressNow").textContent = running
        ? `${{processed}}/${{targets}} | updated=${{updated}} errors=${{errors}} | event=${{ev}}${{err ? ` | err=${{err}}` : ""}}${{qInfo}}`
        : (ev !== "-" ? `${{ev}}${{err ? ` | err=${{err}}` : ""}}${{qInfo}}` : (`-${{qInfo}}`));
      document.getElementById("idxCurrentOwner").textContent = running ? (a.current_owner || "-") : "-";
      document.getElementById("idxLastRun").textContent = d.last_run_at || "-";
      document.getElementById("idxLastStatus").textContent = d.last_run_status || "-";
      document.getElementById("idxLastDetails").textContent = d.last_run_details || "-";
      if (running) setIndexerStatus(`Indexer running: ${{processed}}/${{targets}}`, false);
    }}
    function renderIndexerRuns(items) {{
      const el = document.getElementById("idxDebugLog");
      if (!el) return;
      const rows = (items || []).slice(0, 12).map((r) => {{
        const ts = String(r.ts || "-");
        const st = String(r.status || "-");
        const d = String(r.details || "");
        return `[${{ts}}] ${{st}}  ${{d}}`;
      }});
      el.textContent = rows.length ? rows.join("\\n") : "-";
    }}
    async function loadIndexerRuns() {{
      try {{
        const r = await fetch("/api/admin/indexers/runs?name=infinity_bsc&limit=20");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load indexer runs");
        renderIndexerRuns(data.items || []);
      }} catch (_) {{
        renderIndexerRuns([]);
      }}
    }}
    async function loadIndexers() {{
      try {{
        const r = await fetch("/api/admin/indexers");
        const data = await r.json();
        if (!r.ok) throw new Error(data.detail || "Failed to load indexers");
        const item = ((data.items || [])[0] || null);
        if (item) {{
          renderIndexerCard(item);
          const running = !!(item.activity && item.activity.running);
          if (!running) setIndexerStatus("Loaded", false);
        }} else {{
          setIndexerStatus("Loaded", false);
        }}
        await loadIndexerRuns();
      }} catch (e) {{
        setIndexerStatus("Load failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function saveIndexerConfig() {{
      try {{
        const payload = {{
          name: "infinity_bsc",
          enabled: document.getElementById("idxEnabled").value === "true",
          mode: document.getElementById("idxMode").value || "auto",
          max_receipts: Number(document.getElementById("idxMaxReceipts").value || 220),
        }};
        const data = await postJson("/api/admin/indexers/config", payload);
        if (data?.item) renderIndexerCard(data.item);
        setIndexerStatus("Indexer config saved", false);
      }} catch (e) {{
        setIndexerStatus("Save failed: " + (e?.message || "unknown"), true);
      }}
    }}
    async function runIndexerForOwner() {{
      const owner = (document.getElementById("idxRunOwner").value || "").trim();
      const chainId = Number(document.getElementById("idxRunChainId").value || 56);
      if (!owner) {{
        setIndexerStatus("Enter owner address", true);
        return;
      }}
      try {{
        setIndexerStatus("Running indexer...", false);
        const data = await postJson("/api/admin/indexers/run", {{ name: "infinity_bsc", owner, chain_id: chainId }});
        if (data?.item) renderIndexerCard(data.item);
        const run = data?.run || {{}};
        if (data?.started) {{
          setIndexerStatus("Started in background. You can switch pages.", false);
        }} else {{
          setIndexerStatus(`Done: merged=${{Number(run.merged_ids || 0)}}, checked=${{Number(run.receipt_checked || 0)}}`, false);
        }}
      }} catch (e) {{
        setIndexerStatus("Run failed: " + (e?.message || "unknown"), true);
      }}
    }}
    function startIndexerLiveRefresh() {{
      if (window._idxLiveRefreshStarted) return;
      window._idxLiveRefreshStarted = true;
      setInterval(() => {{
        const tab = document.getElementById("tabSettings");
        if (!tab || tab.style.display === "none") return;
        loadIndexers();
      }}, 3000);
    }}
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
      let html = "<tr><th>Period</th><th>Unique sessions</th><th>Page views</th><th>Run start</th><th>Run done</th><th>Run failed</th><th>Wallet auth</th><th>Help tickets</th><th>Position scans</th><th>Pool scans</th><th>Scan light mode</th><th>Index cache hits</th><th>Index cache misses</th><th>Index skip live</th><th>Legacy disabled</th><th>Row enrich off</th><th>Total events</th></tr>";
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
        html += `<td>${{Number(r.positions_scans || 0)}}</td>`;
        html += `<td>${{Number(r.positions_pool_scans || 0)}}</td>`;
        html += `<td>${{Number(r.positions_scan_light_mode || 0)}}</td>`;
        html += `<td>${{Number(r.index_cache_hits || 0)}}</td>`;
        html += `<td>${{Number(r.index_cache_misses || 0)}}</td>`;
        html += `<td>${{Number(r.index_skip_live || 0)}}</td>`;
        html += `<td>${{Number(r.index_legacy_disabled || 0)}}</td>`;
        html += `<td>${{Number(r.index_row_live_enrich_disabled || 0)}}</td>`;
        html += `<td>${{Number(r.total_events || 0)}}</td>`;
        html += "</tr>";
      }}
      if (!(rows || []).length) html += '<tr><td colspan="17">No stats yet.</td></tr>';
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
        await loadIndexers();
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
    startIndexerLiveRefresh();
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
            "indexer": _indexer_summary("infinity_bsc"),
        }
    except Exception as e:
        return {
            "admin_wallets": _admin_wallets_value(),
            "admin_wallets_encrypted": bool(_admin_wallets_fernet()),
            "analytics_db_path": str(ANALYTICS_DB_PATH),
            "events_count": 0,
            "token_catalog_updated_at": None,
            "token_catalog_count": 0,
            "indexer": _indexer_summary("infinity_bsc"),
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
          SUM(CASE WHEN event_type = 'help_ticket' THEN 1 ELSE 0 END) AS help_tickets,
          SUM(CASE WHEN event_type = 'positions_scan' THEN 1 ELSE 0 END) AS positions_scans
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
                "positions_scans": int(r[9] or 0),
                "positions_pool_scans": 0,
                "index_cache_hits": 0,
                "index_cache_misses": 0,
                "index_skip_live": 0,
                "index_legacy_disabled": 0,
                "index_row_live_enrich_disabled": 0,
                "positions_scan_light_mode": 0,
            }
        )
    bucket_map: dict[str, dict[str, Any]] = {str(it.get("bucket") or ""): it for it in items}
    payload_query = f"""
        SELECT bucket, payload
        FROM (
          SELECT {bucket_expr} AS bucket, event_type, payload
          FROM analytics_events
        ) x
        WHERE bucket IS NOT NULL AND bucket != '' AND event_type = 'positions_scan'
    """
    with _analytics_conn() as conn:
        payload_rows = conn.execute(payload_query).fetchall()
    for r in payload_rows:
        bucket = str(r[0] or "")
        raw_payload = str(r[1] or "").strip()
        if not bucket or not raw_payload:
            continue
        item = bucket_map.get(bucket)
        if not item:
            continue
        try:
            obj = json.loads(raw_payload)
            if not isinstance(obj, dict):
                continue
            item["index_cache_hits"] = int(item.get("index_cache_hits") or 0) + int(obj.get("index_cache_hits") or 0)
            item["index_cache_misses"] = int(item.get("index_cache_misses") or 0) + int(obj.get("index_cache_misses") or 0)
            item["index_skip_live"] = int(item.get("index_skip_live") or 0) + int(obj.get("index_skip_live") or 0)
            item["index_legacy_disabled"] = int(item.get("index_legacy_disabled") or 0) + int(obj.get("legacy_disabled") or 0)
            item["index_row_live_enrich_disabled"] = int(item.get("index_row_live_enrich_disabled") or 0) + int(obj.get("row_live_enrich_disabled") or 0)
            is_pool_scan = bool(obj.get("scan_pools"))
            if is_pool_scan:
                item["positions_pool_scans"] = int(item.get("positions_pool_scans") or 0) + 1
            if is_pool_scan and not bool(obj.get("include_creation_dates")):
                item["positions_scan_light_mode"] = int(item.get("positions_scan_light_mode") or 0) + 1
        except Exception:
            continue
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


@app.get("/api/admin/indexers")
def admin_indexers(request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    return {"ok": True, "items": [_indexer_summary("infinity_bsc")]}


@app.get("/api/admin/indexers/runs")
def admin_indexer_runs(
    request: Request,
    response: Response,
    name: str = "infinity_bsc",
    limit: int = 20,
) -> dict[str, Any]:
    _require_admin(request, response)
    lim = max(1, min(200, int(limit)))
    with _analytics_conn() as conn:
        rows = conn.execute(
            "SELECT ts, status, details FROM indexer_runs WHERE name = ? ORDER BY id DESC LIMIT ?",
            (str(name or "infinity_bsc"), lim),
        ).fetchall()
    items = [{"ts": str(r[0] or ""), "status": str(r[1] or ""), "details": str(r[2] or "")} for r in rows]
    return {"ok": True, "items": items, "count": len(items)}


@app.post("/api/admin/indexers/config")
def admin_indexers_config(req: AdminIndexerUpdate, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    if str(req.name or "").strip() != "infinity_bsc":
        raise HTTPException(status_code=400, detail="Unknown indexer name.")
    item = _indexer_upsert(
        "infinity_bsc",
        enabled=bool(req.enabled),
        mode=str(req.mode or "auto"),
        max_receipts=int(req.max_receipts),
    )
    _indexer_log_run(
        "infinity_bsc",
        "config",
        f"enabled={1 if item.get('enabled') else 0} mode={item.get('mode')} max_receipts={item.get('max_receipts')}",
    )
    return {"ok": True, "item": _indexer_summary("infinity_bsc")}


def _run_indexer_owner_task(chain_id: int, owner: str, max_receipts: int) -> None:
    if not INDEXER_LOCK.acquire(blocking=False):
        _indexer_log_run("infinity_bsc", "skip", "manual run skipped: busy")
        return
    try:
        _indexer_activity_start("infinity_bsc", 1)
        run_stats = _update_infinity_index_for_owner(int(chain_id), owner, max_receipts=int(max_receipts))
        _indexer_activity_tick(int(chain_id), owner, updated_inc=int(run_stats.get("merged_ids") or 0), error_inc=0)
        _indexer_log_run(
            "infinity_bsc",
            "ok",
            f"manual owner={owner} chain={chain_id} merged={run_stats.get('merged_ids', 0)} checked={run_stats.get('receipt_checked', 0)}",
        )
    except Exception as e:
        _indexer_activity_tick(int(chain_id), owner, updated_inc=0, error_inc=1)
        _indexer_log_run("infinity_bsc", "error", f"manual owner={owner} chain={chain_id} error={str(e)[:220]}")
    finally:
        _indexer_activity_stop("manual-finished")
        INDEXER_LOCK.release()


@app.post("/api/admin/indexers/run")
def admin_indexers_run(req: AdminIndexerRunRequest, request: Request, response: Response) -> dict[str, Any]:
    _require_admin(request, response)
    if str(req.name or "").strip() != "infinity_bsc":
        raise HTTPException(status_code=400, detail="Unknown indexer name.")
    owner = str(req.owner or "").strip().lower()
    if not _is_eth_address(owner):
        raise HTTPException(status_code=400, detail="Invalid owner address.")
    max_receipts = req.max_receipts
    if max_receipts is None:
        cfg = _indexer_get("infinity_bsc")
        max_receipts = int(cfg.get("max_receipts") or INFINITY_INDEXER_MAX_RECEIPTS)
    if _indexer_activity_snapshot().get("running"):
        raise HTTPException(status_code=409, detail="Indexer is already running. Please wait until it finishes.")
    t = threading.Thread(
        target=_run_indexer_owner_task,
        args=(int(req.chain_id), owner, int(max_receipts)),
        daemon=True,
        name="indexer-manual-owner-run",
    )
    t.start()
    return {"ok": True, "started": True, "item": _indexer_summary("infinity_bsc")}


@app.get("/api/positions/chains")
def positions_chains() -> dict[str, Any]:
    items = _positions_chain_catalog()
    return {"items": items, "count": len(items)}


def _extract_index_scan_counters(pool_debug_rows: list[dict[str, Any]]) -> tuple[int, int, int, int, int]:
    cache_hits = 0
    cache_misses = 0
    skip_live = 0
    legacy_disabled = 0
    row_live_enrich_disabled = 0
    for row in pool_debug_rows:
        attempts = row.get("attempts") or []
        if not isinstance(attempts, list):
            continue
        has_cache_hit = False
        has_cache_miss = False
        has_skip_live = False
        has_legacy_disabled = False
        has_row_live_enrich_disabled = False
        for a in attempts:
            if not isinstance(a, dict):
                continue
            qmode = str(a.get("query_mode") or "")
            if qmode == "ownership_cache":
                if int(a.get("count") or 0) > 0:
                    has_cache_hit = True
                else:
                    has_cache_miss = True
            elif qmode == "index_first_skip_live":
                has_skip_live = True
            elif qmode == "legacy_discovery_disabled":
                has_legacy_disabled = True
            elif qmode == "row_live_enrich_disabled":
                has_row_live_enrich_disabled = True
        if has_cache_hit:
            cache_hits += 1
        elif has_cache_miss:
            cache_misses += 1
        if has_skip_live:
            skip_live += 1
        if has_legacy_disabled:
            legacy_disabled += 1
        if has_row_live_enrich_disabled:
            row_live_enrich_disabled += 1
    return cache_hits, cache_misses, skip_live, legacy_disabled, row_live_enrich_disabled


def _build_pool_debug_summary_rows(pool_debug_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
            elapsed_ms = max(0, int(a.get("elapsed_ms") or 0))
            if elapsed_ms > 0:
                tm_key = (chain, version, f"{mode}#elapsed_ms", "ok")
                debug_summary[tm_key] = int(debug_summary.get(tm_key, 0)) + int(elapsed_ms)
            calls_key = (chain, version, f"{mode}#calls", "ok")
            debug_summary[calls_key] = int(debug_summary.get(calls_key, 0)) + 1
            inf_dbg = a.get("infinity_debug") if isinstance(a.get("infinity_debug"), dict) else None
            if isinstance(inf_dbg, dict):
                metric_map = [
                    ("infinity_rpc_getlogs_requests", "rpc_getlogs_requests"),
                    ("infinity_rpc_getlogs_attempts", "rpc_getlogs_attempts"),
                    ("infinity_rpc_getlogs_success", "rpc_getlogs_success"),
                    ("infinity_rpc_getlogs_failures", "rpc_getlogs_failures"),
                    ("infinity_rpc_getlogs_first_try_fail", "rpc_getlogs_first_try_fail"),
                    ("infinity_rpc_getlogs_retry_success", "rpc_getlogs_retry_success"),
                    ("infinity_rpc_getlogs_ms", "rpc_getlogs_ms"),
                ]
                for m_name, src_key in metric_map:
                    try:
                        m_val = int(inf_dbg.get(src_key) or 0)
                    except Exception:
                        m_val = 0
                    if m_val <= 0:
                        continue
                    mk = (chain, version, m_name, "ok")
                    debug_summary[mk] = int(debug_summary.get(mk, 0)) + int(m_val)
    return [
        {
            "chain": chain,
            "version": version,
            "query_mode": mode,
            "status": status,
            "count": count,
        }
        for (chain, version, mode, status), count in sorted(debug_summary.items())
    ]


def _build_positions_scan_analytics_payload(
    *,
    evm_count: int,
    sol_count: int,
    tron_count: int,
    chains_count: int,
    scan_pools: bool,
    include_creation_dates: bool,
    cache_hits: int,
    cache_misses: int,
    skip_live: int,
    legacy_disabled: int,
    row_live_enrich_disabled: int,
) -> str:
    return json.dumps(
        {
            "evm": int(evm_count),
            "sol": int(sol_count),
            "tron": int(tron_count),
            "chains": int(chains_count),
            "scan_pools": bool(scan_pools),
            "include_creation_dates": bool(include_creation_dates),
            "index_cache_hits": int(cache_hits),
            "index_cache_misses": int(cache_misses),
            "index_skip_live": int(skip_live),
            "legacy_disabled": int(legacy_disabled),
            "row_live_enrich_disabled": int(row_live_enrich_disabled),
            "legacy_enabled": bool(POSITIONS_LEGACY_DISCOVERY_ENABLED),
            "index_first_strict": bool(POSITIONS_INDEX_FIRST_STRICT),
        },
        ensure_ascii=True,
        separators=(",", ":"),
    )


def _sort_positions_scan_rows(
    pool_rows: list[dict[str, Any]],
    lending_rows: list[dict[str, Any]],
    reward_rows: list[dict[str, Any]],
) -> None:
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


def _append_index_mode_info_note(
    info_notes: list[str],
    *,
    cache_hits: int,
    cache_misses: int,
    skip_live: int,
    row_live_enrich_disabled: int,
) -> None:
    if POSITIONS_CONTRACT_ONLY_ENABLED:
        return
    if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
        return
    mode_text = "strict" if POSITIONS_INDEX_FIRST_STRICT else "hybrid"
    legacy_text = "enabled" if POSITIONS_LEGACY_DISCOVERY_ENABLED else "disabled"
    info_notes.append(
        f"Ownership index mode: {mode_text}; legacy discovery: {legacy_text}; "
        f"cache hits={cache_hits}, misses={cache_misses}, skip_live={skip_live}, row_live_enrich_off={row_live_enrich_disabled}."
    )


def _enqueue_positions_ownership_refresh_for_addresses(
    chain_ids: list[int],
    evm_addresses: list[str],
) -> None:
    if not POSITIONS_OWNERSHIP_INDEX_ENABLED:
        return
    for cid in chain_ids:
        for owner in evm_addresses:
            _position_enqueue_ownership_refresh(int(cid), owner)


def _select_positions_chain_ids(requested_chain_ids_raw: list[int]) -> list[int]:
    # By default scan all supported EVM chains; if chain_ids provided, preserve user order.
    # Prioritize chains where users most often track active LPs, and keep heavy
    # ethereum scan later so partial results include L2 pools under timeout.
    preferred_order = [56, 42161, 8453, 130, 1, 10, 137]
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
    for x in (requested_chain_ids_raw or []):
        cid = int(x)
        if cid > 0 and cid not in requested_chain_ids:
            requested_chain_ids.append(cid)
    if requested_chain_ids:
        allowed = set(all_chain_ids)
        selected_chain_ids = [c for c in requested_chain_ids if c in allowed]
        return selected_chain_ids if selected_chain_ids else all_chain_ids
    return all_chain_ids


def _scan_positions_evm_components(
    evm_addresses: list[str],
    selected_chain_ids: list[int],
    *,
    scan_pools: bool,
    scan_lending: bool,
    scan_rewards: bool,
    include_creation_dates: bool,
    hard_scan: bool,
    deep_infinity_scan: bool,
    infinity_scan: bool,
) -> tuple[
    list[dict[str, Any]],
    list[str],
    list[dict[str, Any]],
    list[dict[str, Any]],
    list[str],
    list[dict[str, Any]],
    list[str],
    dict[str, Any],
]:
    timings: dict[str, Any] = {}
    if scan_pools:
        t_enqueue = time.monotonic()
        _enqueue_positions_ownership_refresh_for_addresses(selected_chain_ids, evm_addresses)
        timings["enqueue_refresh_sec"] = round(max(0.0, time.monotonic() - t_enqueue), 3)
    if scan_pools:
        t_pool = time.monotonic()
        pool_rows, pool_errs, pool_debug_rows, pool_timings = _scan_pool_positions(
            evm_addresses,
            selected_chain_ids,
            include_creation_dates=include_creation_dates,
            pre_enqueued_ownership_refresh=bool(POSITIONS_OWNERSHIP_INDEX_ENABLED),
            hard_scan=hard_scan,
            deep_infinity_scan=deep_infinity_scan,
        )
        if infinity_scan:
            pool_rows = [
                r for r in pool_rows if str((r or {}).get("protocol") or "").strip().lower().startswith("pancake_infinity_")
            ]
        timings["pool_scan_sec"] = round(max(0.0, time.monotonic() - t_pool), 3)
        if isinstance(pool_timings, dict):
            timings["pool"] = pool_timings
    else:
        pool_rows, pool_errs, pool_debug_rows = [], [], []
    t_lending = time.monotonic()
    lending_rows, lending_errs = (_scan_aave_positions(evm_addresses, selected_chain_ids) if scan_lending else ([], []))
    timings["lending_scan_sec"] = round(max(0.0, time.monotonic() - t_lending), 3)
    t_rewards = time.monotonic()
    reward_rows, reward_errs = (_scan_aave_merit_rewards(evm_addresses, selected_chain_ids) if scan_rewards else ([], []))
    timings["reward_scan_sec"] = round(max(0.0, time.monotonic() - t_rewards), 3)
    return (
        pool_rows,
        pool_errs,
        pool_debug_rows,
        lending_rows,
        lending_errs,
        reward_rows,
        reward_errs,
        timings,
    )


def _prepare_positions_scan_request(
    req: PositionsScanRequest,
) -> tuple[list[str], list[str], list[str], list[int], bool, bool, bool]:
    evm_raw = list(req.evm_addresses or []) + list(req.addresses or [])
    evm_addresses = _parse_positions_addresses(evm_raw)
    solana_addresses = _parse_solana_addresses(req.solana_addresses or [])
    tron_addresses = _parse_tron_addresses(req.tron_addresses or [])
    if not evm_addresses and not solana_addresses and not tron_addresses:
        raise HTTPException(status_code=400, detail="Provide at least one valid address.")
    if len(evm_addresses) > 20:
        raise HTTPException(status_code=400, detail="Too many addresses. Max 20.")
    scan_pools = bool(req.include_pools)
    scan_lending = bool(req.include_lending)
    scan_rewards = bool(req.include_rewards)
    need_evm_chain_context = bool(evm_addresses) and bool(scan_pools or scan_lending or scan_rewards)
    selected_chain_ids = _select_positions_chain_ids(list(req.chain_ids or [])) if need_evm_chain_context else []
    return (
        evm_addresses,
        solana_addresses,
        tron_addresses,
        selected_chain_ids,
        scan_pools,
        scan_lending,
        scan_rewards,
    )


def _build_positions_info_notes(
    solana_addresses: list[str],
    tron_addresses: list[str],
    *,
    scan_pools: bool,
    include_creation_dates: bool,
    deep_infinity_scan: bool,
    infinity_scan: bool,
) -> list[str]:
    info_notes: list[str] = []
    if solana_addresses:
        info_notes.append("Solana scanning is not available yet in this build.")
    if tron_addresses:
        info_notes.append("TRON scanning is not available yet in this build.")
    if infinity_scan:
        info_notes.append("Infinity scan mode: showing only Pancake Infinity positions.")
    if POSITIONS_CONTRACT_ONLY_ENABLED:
        info_notes.append("Contract-only mode: Pair/Fee/In position/Unclaimed are from on-chain contract calls only.")
    return info_notes


def _build_positions_scan_response(
    *,
    pool_rows: list[dict[str, Any]],
    lending_rows: list[dict[str, Any]],
    reward_rows: list[dict[str, Any]],
    pool_errs: list[str],
    lending_errs: list[str],
    reward_errs: list[str],
    info_notes: list[str],
    pool_debug_rows: list[dict[str, Any]],
    debug_summary_rows: list[dict[str, Any]],
    debug_timings: dict[str, Any],
    include_debug_details: bool,
    evm_count: int,
    sol_count: int,
    tron_count: int,
    chains_count: int,
) -> dict[str, Any]:
    if POSITIONS_CONTRACT_ONLY_ENABLED:
        summary_limit = 5000
        pool_scan_limit = 5000
    else:
        summary_limit = 120 if include_debug_details else 20
        pool_scan_limit = 500 if include_debug_details else 0
    debug_payload = {
        "pool_scan": pool_debug_rows[:pool_scan_limit],
        "summary": debug_summary_rows[:summary_limit],
        "timings": debug_timings if isinstance(debug_timings, dict) else {},
    }
    return {
        "pool_positions": pool_rows,
        "lending_positions": lending_rows,
        "reward_positions": reward_rows,
        "errors": (pool_errs + lending_errs + reward_errs)[:40],
        "infos": info_notes[:20],
        "debug": debug_payload,
        "summary": {
            "evm_addresses": int(evm_count),
            "solana_addresses": int(sol_count),
            "tron_addresses": int(tron_count),
            "chains": int(chains_count),
            "pool_count": len(pool_rows),
            "lending_count": len(lending_rows),
            "reward_count": len(reward_rows),
        },
    }


def _record_positions_scan_analytics(
    *,
    sid: str,
    evm_addresses: list[str],
    solana_addresses: list[str],
    tron_addresses: list[str],
    selected_chain_ids: list[int],
    scan_pools: bool,
    include_creation_dates: bool,
    cache_hits: int,
    cache_misses: int,
    skip_live: int,
    legacy_disabled: int,
    row_live_enrich_disabled: int,
    info_notes: list[str],
) -> None:
    _analytics_log_event(
        session_id=sid,
        event_type="positions_scan",
        path="/api/positions/scan",
        payload=_build_positions_scan_analytics_payload(
            evm_count=len(evm_addresses),
            sol_count=len(solana_addresses),
            tron_count=len(tron_addresses),
            chains_count=len(selected_chain_ids),
            scan_pools=bool(scan_pools),
            include_creation_dates=bool(include_creation_dates),
            cache_hits=cache_hits,
            cache_misses=cache_misses,
            skip_live=skip_live,
            legacy_disabled=legacy_disabled,
            row_live_enrich_disabled=row_live_enrich_disabled,
        ),
    )


def _update_pos_job(job_id: str, **updates: Any) -> dict[str, Any] | None:
    with POS_JOB_LOCK:
        job = POS_JOBS.get(job_id)
        if not job:
            return None
        job.update(updates)
        return job


def _run_positions_scan_enrich_phases(job_id: str, result: dict[str, Any], *, hard_scan: bool = False) -> None:
    pool_rows = result.get("pool_positions") or []
    if not isinstance(pool_rows, list) or not pool_rows:
        return
    if _update_pos_job(
        job_id,
        result=result,
        stage="enrich_anomalies",
        stage_label="Background: enriching symbol anomalies (UNK)",
        progress=74,
    ) is None:
        return

    def _is_symbol_anomaly(row: dict[str, Any]) -> bool:
        # Strict rule: once row is marked as spam, do not query it anymore
        # in background phases. Manual unspam uses dedicated row/enrich endpoint.
        if bool(row.get("suspected_spam")) or bool(row.get("spam_skipped")):
            return False
        s0 = str(row.get("position_symbol0") or "").strip().upper()
        s1 = str(row.get("position_symbol1") or "").strip().upper()
        pair = str(row.get("pair") or "").strip().upper()
        return bool(
            s0 in {"", "?", "UNK"}
            or s1 in {"", "?", "UNK"}
            or "UNK" in pair
            or "/?" in pair
        )

    start = time.monotonic()
    max_seconds = 18
    max_rows = 80
    checked = 0
    updated = 0
    for row in pool_rows:
        if time.monotonic() >= (start + max_seconds):
            break
        if checked >= max_rows:
            break
        if not isinstance(row, dict) or not _is_symbol_anomaly(row):
            continue
        proto = str(row.get("protocol") or "").strip().lower()
        if proto not in {"uniswap_v3", "pancake_v3", "pancake_v3_staked"}:
            continue
        chain_id = int(row.get("chain_id") or _chain_id_by_chain_key(str(row.get("chain") or "")) or 0)
        token_id = _position_token_id_from_raw(row.get("position_id"))
        owner = str(row.get("address") or "").strip().lower()
        if chain_id <= 0 or token_id <= 0:
            continue
        checked += 1
        try:
            snap = _fetch_v3_position_contract_snapshot(chain_id, proto, token_id, owner)
            if not isinstance(snap, dict):
                continue
            row_updates = _build_row_updates_from_snapshot(row, snap, chain_id)
            if row_updates:
                row.update(row_updates)
                row["spam_skipped"] = False
                row["suspected_spam"] = False
                updated += 1
        except Exception:
            continue

    _enrich_rows_liquidity_usd(pool_rows, max_seconds=4)
    infos = result.get("infos") if isinstance(result.get("infos"), list) else []
    infos = list(infos)
    infos.append(f"Background anomaly enrich: checked={checked}, updated={updated}.")
    result["infos"] = infos[:20]
    rows_count = int(len(pool_rows))
    if _update_pos_job(
        job_id,
        result=result,
        stage="finalize",
        stage_label=f"Finalizing after anomaly enrich ({rows_count} rows)",
        progress=92,
    ) is None:
        return
    _update_pos_job(job_id, result=result, progress=95)


def _scan_positions_core(
    req: PositionsScanRequest,
    sid: str = "unknown",
    *,
    include_creation_dates: bool = True,
) -> dict[str, Any]:
    core_started = time.monotonic()
    debug_timings: dict[str, Any] = {}
    t_prepare = time.monotonic()
    hard_scan_enabled = False
    infinity_scan_enabled = bool(req.infinity_scan)
    deep_infinity_enabled = False
    (
        evm_addresses,
        solana_addresses,
        tron_addresses,
        selected_chain_ids,
        scan_pools,
        scan_lending,
        scan_rewards,
    ) = _prepare_positions_scan_request(req)
    debug_timings["prepare_request_sec"] = round(max(0.0, time.monotonic() - t_prepare), 3)
    scan_pools_effective = bool(scan_pools and evm_addresses)
    pool_debug_rows: list[dict[str, Any]] = []
    evm_timings: dict[str, Any] = {}
    if evm_addresses:
        t_evm = time.monotonic()
        (
            pool_rows,
            pool_errs,
            pool_debug_rows,
            lending_rows,
            lending_errs,
            reward_rows,
            reward_errs,
            evm_timings,
        ) = _scan_positions_evm_components(
            evm_addresses,
            selected_chain_ids,
            scan_pools=scan_pools,
            scan_lending=scan_lending,
            scan_rewards=scan_rewards,
            include_creation_dates=include_creation_dates,
            hard_scan=hard_scan_enabled,
            deep_infinity_scan=deep_infinity_enabled,
            infinity_scan=infinity_scan_enabled,
        )
        debug_timings["evm_components_sec"] = round(max(0.0, time.monotonic() - t_evm), 3)
        if isinstance(evm_timings, dict):
            debug_timings["evm"] = evm_timings
    else:
        pool_rows, pool_errs = [], []
        lending_rows, lending_errs = [], []
        reward_rows, reward_errs = [], []
        pool_debug_rows = []

    info_notes = _build_positions_info_notes(
        solana_addresses,
        tron_addresses,
        scan_pools=scan_pools_effective,
        include_creation_dates=include_creation_dates,
        deep_infinity_scan=deep_infinity_enabled,
        infinity_scan=infinity_scan_enabled,
    )

    t_sort = time.monotonic()
    _sort_positions_scan_rows(pool_rows, lending_rows, reward_rows)
    debug_timings["sort_rows_sec"] = round(max(0.0, time.monotonic() - t_sort), 3)

    t_debug = time.monotonic()
    debug_summary_rows = _build_pool_debug_summary_rows(pool_debug_rows)
    cache_hits, cache_misses, skip_live, legacy_disabled, row_live_enrich_disabled = _extract_index_scan_counters(
        pool_debug_rows
    )
    rpc_logs_req = 0
    rpc_logs_attempts = 0
    rpc_logs_first_fail = 0
    rpc_logs_retry_ok = 0
    rpc_logs_ms = 0
    for d in pool_debug_rows:
        if not isinstance(d, dict):
            continue
        for a in (d.get("attempts") or []):
            if not isinstance(a, dict):
                continue
            inf_dbg = a.get("infinity_debug") if isinstance(a.get("infinity_debug"), dict) else None
            if not isinstance(inf_dbg, dict):
                continue
            rpc_logs_req += int(inf_dbg.get("rpc_getlogs_requests") or 0)
            rpc_logs_attempts += int(inf_dbg.get("rpc_getlogs_attempts") or 0)
            rpc_logs_first_fail += int(inf_dbg.get("rpc_getlogs_first_try_fail") or 0)
            rpc_logs_retry_ok += int(inf_dbg.get("rpc_getlogs_retry_success") or 0)
            rpc_logs_ms += int(inf_dbg.get("rpc_getlogs_ms") or 0)
    if rpc_logs_req > 0:
        info_notes.append(
            "RPC getLogs debug: "
            f"requests={rpc_logs_req}, attempts={rpc_logs_attempts}, "
            f"first_try_fail={rpc_logs_first_fail}, retry_success={rpc_logs_retry_ok}, total_ms={rpc_logs_ms}."
        )
    debug_timings["build_debug_sec"] = round(max(0.0, time.monotonic() - t_debug), 3)
    t_analytics = time.monotonic()
    _record_positions_scan_analytics(
        sid=sid,
        evm_addresses=evm_addresses,
        solana_addresses=solana_addresses,
        tron_addresses=tron_addresses,
        selected_chain_ids=selected_chain_ids,
        scan_pools=scan_pools_effective,
        include_creation_dates=include_creation_dates,
        cache_hits=cache_hits,
        cache_misses=cache_misses,
        skip_live=skip_live,
        legacy_disabled=legacy_disabled,
        row_live_enrich_disabled=row_live_enrich_disabled,
        info_notes=info_notes,
    )
    debug_timings["analytics_sec"] = round(max(0.0, time.monotonic() - t_analytics), 3)
    debug_timings["total_sec"] = round(max(0.0, time.monotonic() - core_started), 3)
    return _build_positions_scan_response(
        pool_rows=pool_rows,
        lending_rows=lending_rows,
        reward_rows=reward_rows,
        pool_errs=pool_errs,
        lending_errs=lending_errs,
        reward_errs=reward_errs,
        info_notes=info_notes,
        pool_debug_rows=pool_debug_rows,
        debug_summary_rows=debug_summary_rows,
        debug_timings=debug_timings,
        include_debug_details=hard_scan_enabled,
        evm_count=len(evm_addresses),
        sol_count=len(solana_addresses),
        tron_count=len(tron_addresses),
        chains_count=len(selected_chain_ids),
    )


def _run_positions_scan_job(job_id: str, req: PositionsScanRequest, session_id: str) -> None:
    hard_scan_enabled = False
    infinity_scan_enabled = bool(req.infinity_scan)
    if _update_pos_job(
        job_id,
        status="running",
        stage="scan",
        stage_label=("Infinity scan: scanning positions" if infinity_scan_enabled else "Scanning positions"),
        progress=15,
        started_at=time.time(),
    ) is None:
        return
    try:
        result = _scan_positions_core(req, sid=session_id, include_creation_dates=False)
        if _update_pos_job(
            job_id,
            result=result,
            stage="enrich_dates",
            stage_label=("Infinity scan: finalizing" if infinity_scan_enabled else "Fast mode: finalizing"),
            progress=65,
        ) is None:
            return
        _run_positions_scan_enrich_phases(job_id, result, hard_scan=hard_scan_enabled)
        _update_pos_job(
            job_id,
            status="done",
            stage="done",
            stage_label="Completed",
            progress=100,
            finished_at=time.time(),
            result=result,
        )
    except Exception as e:
        _update_pos_job(
            job_id,
            status="failed",
            stage="failed",
            stage_label="Failed",
            progress=100,
            finished_at=time.time(),
            error=str(e)[:400],
        )


def _create_positions_job() -> str:
    job_id = str(uuid.uuid4())
    with POS_JOB_LOCK:
        now = time.time()
        stale = [jid for jid, j in POS_JOBS.items() if (now - float(j.get("created_at") or now)) > float(POS_JOB_TTL_SEC)]
        for jid in stale:
            POS_JOBS.pop(jid, None)
        POS_JOBS[job_id] = {
            "id": job_id,
            "status": "queued",
            "stage": "queued",
            "stage_label": "Queued",
            "progress": 0,
            "created_at": now,
            "started_at": None,
            "finished_at": None,
            "error": "",
            "result": None,
        }
    return job_id


@app.post("/api/positions/scan/start")
def scan_positions_start(req: PositionsScanRequest, request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    job_id = _create_positions_job()
    t = threading.Thread(target=_run_positions_scan_job, args=(job_id, req, sid), daemon=True)
    t.start()
    return {"job_id": job_id}


@app.get("/api/positions/scan/job/{job_id}")
def scan_positions_job(job_id: str) -> dict[str, Any]:
    with POS_JOB_LOCK:
        job = POS_JOBS.get(str(job_id))
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return dict(job)


@app.post("/api/positions/scan")
def scan_positions(req: PositionsScanRequest, request: Request, response: Response) -> dict[str, Any]:
    sid = _ensure_session_cookie(request, response)
    return _scan_positions_core(
        req,
        sid=sid,
        include_creation_dates=bool(POSITIONS_DIRECT_INCLUDE_CREATION_DATES),
    )


@app.post("/api/positions/row/enrich")
def positions_row_enrich(req: PositionsRowEnrichRequest) -> dict[str, Any]:
    row = dict(req.row or {})
    chain_key = str(row.get("chain") or "").strip().lower()
    protocol = str(row.get("protocol") or "").strip().lower()
    owner = str(row.get("address") or "").strip().lower()
    pos_id = str(row.get("position_id") or "").strip()
    chain_id = _chain_id_by_chain_key(chain_key)
    token_id = _position_token_id_from_raw(pos_id)
    if chain_id <= 0 or token_id <= 0:
        return {"ok": False, "reason": "invalid_row"}
    if protocol not in {"uniswap_v3", "pancake_v3", "pancake_v3_staked"}:
        return {"ok": False, "reason": "unsupported_protocol"}
    snap = _fetch_v3_position_contract_snapshot(int(chain_id), protocol, int(token_id), owner)
    if not isinstance(snap, dict):
        return {"ok": False, "reason": "snapshot_unavailable"}
    updates = _build_row_updates_from_snapshot(row, snap, int(chain_id))
    return {"ok": True, "row_updates": updates}


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
    .scan-progress {
      width: 140px;
      height: 6px;
      border-radius: 999px;
      background: #e2e8f0;
      overflow: hidden;
      display: none;
    }
    .scan-progress .bar {
      width: 40%;
      height: 100%;
      background: linear-gradient(90deg, #93c5fd, #2563eb);
      animation: scanLoad 1s linear infinite;
    }
    @keyframes scanLoad {
      0% { transform: translateX(-120%); }
      100% { transform: translateX(280%); }
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
    .collapse-btn {
      border: none;
      background: transparent;
      color: #334155;
      font-size: 14px;
      font-weight: 800;
      cursor: pointer;
      padding: 0 2px;
      min-width: 16px;
      text-align: center;
    }
    .section-body { display: block; }
    .section-body.collapsed { display: none; }
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
      color: #111111;
      display: inline-block;
      width: 280px;
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
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
            <div id="scanProgress" class="scan-progress"><div class="bar"></div></div>
            <span id="status" class="status">Ready</span>
            <button class="search-link-btn" type="button" id="scanBtn" onclick="runJob()">Scan</button>
            <button class="collapse-btn" id="toggleFeeHistoryBtn" type="button" onclick="toggleHomeSection('feeHistory')" title="Collapse/expand">▾</button>
          </div>
        </div>
        <div id="feeHistoryBody" class="section-body">
          <div class="charts-grid">
            <div id="feesChart" class="plot"></div>
            <div id="tvlChart" class="plot"></div>
          </div>
        </div>
      </section>

      <section class="card">
        <div class="section-head">
          <h3>Pools Table</h3>
          <div class="section-actions">
            <button class="search-link-btn" type="button" onclick="exportCsv()">Export CSV</button>
            <button class="collapse-btn" id="togglePoolsTableBtn" type="button" onclick="toggleHomeSection('poolsTable')" title="Collapse/expand">▾</button>
          </div>
        </div>
        <div id="poolsTableBody" class="section-body">
          <div class="table-wrap">
            <table id="resultTable"></table>
          </div>
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
    let scanTicker = null;
    let scanStartedAt = 0;
    let scanStageLabel = "waiting";
    const homeSectionState = { feeHistory: false, poolsTable: false };
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

    function setHomeSectionCollapsed(key, collapsed) {
      const bodyMap = {feeHistory: "feeHistoryBody", poolsTable: "poolsTableBody"};
      const btnMap = {feeHistory: "toggleFeeHistoryBtn", poolsTable: "togglePoolsTableBtn"};
      const body = document.getElementById(bodyMap[key]);
      const btn = document.getElementById(btnMap[key]);
      if (!body || !btn) return;
      homeSectionState[key] = !!collapsed;
      body.classList.toggle("collapsed", !!collapsed);
      btn.textContent = collapsed ? "▸" : "▾";
    }

    function toggleHomeSection(key) {
      setHomeSectionCollapsed(key, !homeSectionState[key]);
    }

    function setScanProgressVisible(flag) {
      const el = document.getElementById("scanProgress");
      if (!el) return;
      el.style.display = flag ? "block" : "none";
    }

    function stopScanTicker() {
      if (scanTicker) {
        clearInterval(scanTicker);
        scanTicker = null;
      }
      scanStartedAt = 0;
      scanStageLabel = "waiting";
    }

    function startScanTicker() {
      stopScanTicker();
      scanStartedAt = Date.now();
      const selected = getSelectedChains();
      const chainHints = selected.length ? selected : (availableChains.length ? availableChains : ["all chains"]);
      const tick = () => {
        const elapsed = Math.max(0, Math.floor((Date.now() - scanStartedAt) / 1000));
        const chainHint = chainHints[Math.floor(elapsed / 4) % chainHints.length];
        setStatus(`Scanning positions... ${elapsed}s | ${scanStageLabel} (${chainHint})`, "running");
      };
      tick();
      scanTicker = setInterval(tick, 900);
    }

    function setBusy(flag) {
      const btn = document.getElementById("scanBtn");
      if (!btn) return;
      btn.disabled = flag;
      btn.style.opacity = flag ? "0.7" : "1";
      setScanProgressVisible(flag);
      if (flag) {
        btn.textContent = "Scan again";
      } else {
        btn.textContent = hasScanRun ? "Scan again" : "Scan";
      }
    }

    function updateProgress(progress, stageLabel) {
      scanStageLabel = String(stageLabel || "running");
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
        scanStageLabel = "Submitting job";
        startScanTicker();
        const r = await fetch("/api/pools/run", {
          method: "POST",
          headers: {"Content-Type":"application/json"},
          body: JSON.stringify(payload)
        });
        const data = await r.json();
        if (!r.ok) {
          stopScanTicker();
          setBusy(false);
          setStatus("Error: " + (data.detail || "request failed"), "fail");
          return;
        }
        pollJob(data.job_id);
      } catch (e) {
        stopScanTicker();
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
          stopScanTicker();
          hasScanRun = true;
          setBusy(false);
          setStatus("Completed", "ok");
          renderResult(job.result);
        } else if (job.status === "failed") {
          clearInterval(timer);
          stopScanTicker();
          hasScanRun = true;
          setBusy(false);
          setStatus("Failed: " + (job.error || "unknown"), "fail");
        } else {
          scanStageLabel = String(job.stage_label || job.status || "running");
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
    setHomeSectionCollapsed("feeHistory", false);
    setHomeSectionCollapsed("poolsTable", false);
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
