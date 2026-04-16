#!/usr/bin/env python3
"""
Agent 1: Uniswap v3 (base version).

- Discovers v3 pools by TOKEN_PAIRS
- Applies TVL filter
- Saves pool list PDF
- Saves chart data to data/pools_v3_{suffix}.json
"""

from __future__ import annotations

import argparse
import os
import threading
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import (
    DEFAULT_TOKEN_PAIRS,
    FEE_DAYS,
    LP_ALLOCATION_USD,
    MIN_TVL_USD,
    UNISWAP_V3_SUBGRAPHS,
    GOLDSKY_ENDPOINTS,
)

from agent_common import (
    build_exact_day_window,
    get_token_addresses,
    load_dynamic_tokens,
    pairs_to_filename_suffix,
    parse_token_pairs,
    poolday_lp_fees_usd_exact,
    resolve_pool_tvl_now_external,
    save_chart_data_json,
    save_dynamic_token,
)
from uniswap_client import (
    graphql_query,
    get_graph_endpoint,
    query_pool_day_data,
    query_pools_containing_both_tokens,
    query_pools_by_token_symbols,
    query_token_by_symbol,
)


def _pool_tvl_usd(pool: dict) -> float:
    try:
        return float(pool.get("effectiveTvlUSD") or pool.get("pool_tvl_now_usd") or pool.get("totalValueLockedUSD") or 0)
    except Exception:
        return 0.0


def _resolve_pool_tvl_now_onchain_v3(pool: dict, chain: str) -> tuple[float, str, str]:
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return 0.0, "", "unsupported_chain"
    pool_id = str((pool or {}).get("id") or "").strip().lower()
    token0 = str((((pool or {}).get("token0") or {}).get("id") or "").strip().lower())
    token1 = str((((pool or {}).get("token1") or {}).get("id") or "").strip().lower())
    if not (pool_id and token0 and token1):
        return 0.0, "", "missing_pool_tokens"
    try:
        latest_hex = str((_rpc_json(chain_id, "eth_blockNumber", [], timeout_sec=8.0) or {}).get("result") or "0x0")
        latest_block = int(latest_hex, 16)
    except Exception as e:
        return 0.0, "", f"latest_block_failed:{e}"
    if latest_block <= 0:
        return 0.0, "", "latest_block_not_found"
    try:
        d0 = int((((pool or {}).get("token0") or {}).get("decimals") or 0))
        d1 = int((((pool or {}).get("token1") or {}).get("decimals") or 0))
        if d0 <= 0 or d0 > 36:
            d0 = int(_erc20_decimals(chain_id, token0))
        if d1 <= 0 or d1 > 36:
            d1 = int(_erc20_decimals(chain_id, token1))
        b0 = int(_balance_of_raw(chain_id, token0, pool_id, latest_block, timeout_sec=8.0))
        b1 = int(_balance_of_raw(chain_id, token1, pool_id, latest_block, timeout_sec=8.0))
    except Exception as e:
        return 0.0, "", f"onchain_balance_failed:{e}"

    # Reuse canonical external pricing path, but with on-chain reserve amounts.
    p = dict(pool or {})
    t0 = dict((p.get("token0") or {}))
    t1 = dict((p.get("token1") or {}))
    t0["id"] = token0
    t1["id"] = token1
    t0["decimals"] = int(d0)
    t1["decimals"] = int(d1)
    p["token0"] = t0
    p["token1"] = t1
    p["totalValueLockedToken0"] = str(float(b0) / float(10 ** max(0, d0)))
    p["totalValueLockedToken1"] = str(float(b1) / float(10 ** max(0, d1)))
    tvl_usd, price_source, err = resolve_pool_tvl_now_external(p, ck, write_back=True)
    if float(tvl_usd) > 0:
        try:
            pool["effectiveTvlUSD"] = float(tvl_usd)
            pool["tvl_price_source"] = str(price_source or "")
            pool["pool_tvl_now_usd"] = float(tvl_usd)
            pool["pool_tvl_now_source"] = "onchain_balance*price"
        except Exception:
            pass
    return float(tvl_usd), str(price_source or ""), str(err or "")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except Exception:
        return float(default)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_eth_address(v: str) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("0x") and len(s) == 42


V3_EXACT_TVL_ENABLE = _env_flag("V3_EXACT_TVL_ENABLE", False)
V3_EXACT_TVL_CHAINS = {
    c.strip().lower()
    for c in str(os.environ.get("V3_EXACT_TVL_CHAINS", "ethereum")).split(",")
    if c.strip()
}
V3_EXACT_TVL_MAX_POOLS = max(0, _env_int("V3_EXACT_TVL_MAX_POOLS", 0))
# By default exact-days cap follows the same history window as agent day-data (FEE_DAYS),
# i.e. the value selected in UI "History days".
V3_EXACT_TVL_DAYS_MAX = max(1, _env_int("V3_EXACT_TVL_DAYS_MAX", int(FEE_DAYS)))
V3_EXACT_TVL_POOL_BUDGET_SEC = max(2.0, _env_float("V3_EXACT_TVL_POOL_BUDGET_SEC", 90.0))
V3_EXACT_TVL_MIN_COVERAGE_EXACT = min(1.0, max(0.0, _env_float("V3_EXACT_TVL_MIN_COVERAGE_EXACT", 0.98)))
V3_EXACT_TVL_MIN_COVERAGE_PARTIAL = min(1.0, max(0.0, _env_float("V3_EXACT_TVL_MIN_COVERAGE_PARTIAL", 0.25)))
V3_EXACT_TVL_PROBE_DAYS = max(1, _env_int("V3_EXACT_TVL_PROBE_DAYS", 6))
V3_EXACT_TVL_SPARSE_POINTS = max(3, _env_int("V3_EXACT_TVL_SPARSE_POINTS", 9))
V3_EXACT_TVL_SPARSE_MIN_SUCCESS = min(1.0, max(0.0, _env_float("V3_EXACT_TVL_SPARSE_MIN_SUCCESS", 0.45)))
V3_EXACT_TVL_SPARSE_MAX_SCALE = max(1.0, _env_float("V3_EXACT_TVL_SPARSE_MAX_SCALE", 4.0))
V3_EXACT_TVL_STRICT_MODE = _env_flag("V3_EXACT_TVL_STRICT_MODE", False)
V3_EXACT_STRICT_BLOCK_WINDOW = max(0, _env_int("V3_EXACT_STRICT_BLOCK_WINDOW", 10))
V3_EXACT_STRICT_RPC_RETRIES = max(1, _env_int("V3_EXACT_STRICT_RPC_RETRIES", 3))
V3_EXACT_TVL_DEBUG = _env_flag("V3_EXACT_TVL_DEBUG", False)

CHAIN_ID_BY_KEY: dict[str, int] = {
    "ethereum": 1,
    "optimism": 10,
    "bsc": 56,
    "unichain": 130,
    "polygon": 137,
    "base": 8453,
    "arbitrum": 42161,
    "avalanche": 43114,
    "celo": 42220,
}
LLAMA_CHAIN_BY_KEY: dict[str, str] = {
    "ethereum": "ethereum",
    "optimism": "optimism",
    "bsc": "bsc",
    "unichain": "unichain",
    "polygon": "polygon",
    "base": "base",
    "arbitrum": "arbitrum",
    "avalanche": "avax",
    "celo": "celo",
}
COINGECKO_PLATFORM_BY_KEY: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "polygon": "polygon-pos",
    "base": "base",
    "bsc": "binance-smart-chain",
    "avalanche": "avalanche",
    "celo": "celo",
    "unichain": "unichain",
}
DEFAULT_RPC_URLS_BY_CHAIN_ID: dict[int, list[str]] = {
    1: ["https://ethereum-rpc.publicnode.com"],
    10: ["https://optimism-rpc.publicnode.com"],
    56: ["https://bsc-rpc.publicnode.com", "https://bsc-dataseed.binance.org"],
    130: ["https://unichain-rpc.publicnode.com", "https://mainnet.unichain.org"],
    137: ["https://polygon-bor-rpc.publicnode.com"],
    8453: ["https://base-rpc.publicnode.com", "https://mainnet.base.org"],
    42161: ["https://arbitrum-one-rpc.publicnode.com", "https://arb1.arbitrum.io/rpc"],
    43114: ["https://avalanche-c-chain-rpc.publicnode.com", "https://api.avax.network/ext/bc/C/rpc"],
    42220: ["https://forno.celo.org"],
}

_EXACT_CACHE_LOCK = threading.Lock()
_BLOCK_BY_DAY_CACHE: dict[tuple[str, int], int] = {}
_DECIMALS_CACHE: dict[tuple[int, str], int] = {}
_CG_DAY_PRICE_CACHE: dict[tuple[str, str, int], float] = {}
_LLAMA_DAY_PRICE_CACHE: dict[tuple[str, str, int], float] = {}
_CG_BACKOFF_UNTIL: dict[tuple[str, str], float] = {}
_CG_GLOBAL_BACKOFF_UNTIL: float = 0.0
_CG_LAST_REQ_TS: float = 0.0
_CG_HTTP_LOCK = threading.Lock()
_FALLBACK_WARNED: set[str] = set()
_RPC_PRIMARY_URL: dict[int, str] = {}


def _warn_fallback_once(key: str, message: str) -> None:
    k = str(key or "").strip()
    if not k:
        return
    with _EXACT_CACHE_LOCK:
        if k in _FALLBACK_WARNED:
            return
        _FALLBACK_WARNED.add(k)
    print(f"[WARN][fallback] {message}")


def _alchemy_api_key() -> str:
    for k in (
        "ALCHEMY_API_KEY",
        "ALCHEMY_KEY",
        "ALCHEMY_TRANSFERS_API_KEY",
        "ALCHEMY_APP_KEY",
        "ALCHEMY_HTTP_KEY",
    ):
        v = str(os.environ.get(k, "") or "").strip()
        if v:
            return v
    return ""


def _chain_name_for_id(chain_id: int) -> str:
    return {
        1: "ethereum",
        10: "optimism",
        56: "bsc",
        130: "unichain",
        137: "polygon",
        8453: "base",
        42161: "arbitrum",
        43114: "avalanche",
        42220: "celo",
    }.get(int(chain_id), "")


def _env_rpc_url_for_chainid(chain_id: int) -> str:
    cid = int(chain_id)
    cname = _chain_name_for_id(cid).upper()
    keys = [
        f"ALCHEMY_RPC_URL_{cid}",
        f"RPC_URL_{cid}",
    ]
    if cname:
        keys.extend(
            [
                f"ALCHEMY_RPC_URL_{cname}",
                f"RPC_URL_{cname}",
                f"WEB3_RPC_URL_{cname}",
            ]
        )
    for k in keys:
        v = str(os.environ.get(k, "") or "").strip()
        if v:
            return v
    return ""


def _alchemy_rpc_url(chain_id: int) -> str:
    direct = _env_rpc_url_for_chainid(int(chain_id))
    if direct:
        return str(direct)
    key = _alchemy_api_key()
    if not key:
        return ""
    net = {
        1: "eth-mainnet",
        10: "opt-mainnet",
        137: "polygon-mainnet",
        8453: "base-mainnet",
        42161: "arb-mainnet",
    }.get(int(chain_id), "")
    if not net:
        return ""
    return f"https://{net}.g.alchemy.com/v2/{key}"


def _rpc_urls(chain_id: int) -> list[str]:
    env_key = f"V3_RPC_URLS_{int(chain_id)}"
    raw = str(os.environ.get(env_key, "") or "").strip()
    defaults = list(DEFAULT_RPC_URLS_BY_CHAIN_ID.get(int(chain_id), []))
    auto_alchemy = _alchemy_rpc_url(int(chain_id))
    if not raw:
        urls = ([auto_alchemy] if auto_alchemy else []) + defaults
        if auto_alchemy:
            _warn_fallback_once(
                f"rpc_auto_alchemy:{int(chain_id)}",
                f"strict rpc source priority: auto alchemy enabled for chain_id={int(chain_id)}",
            )
        return urls
    custom = [x.strip() for x in raw.split(",") if x.strip()]
    # Do not let a single broken custom RPC (e.g. invalid Alchemy key URL) remove
    # resilient public fallbacks; keep custom endpoints first for priority.
    merged: list[str] = []
    seen: set[str] = set()
    for u in (([auto_alchemy] if auto_alchemy else []) + custom + defaults):
        k = str(u or "").strip()
        if not k or k in seen:
            continue
        seen.add(k)
        merged.append(k)
    if auto_alchemy:
        _warn_fallback_once(
            f"rpc_auto_alchemy:{int(chain_id)}",
            f"strict rpc source priority: auto alchemy enabled for chain_id={int(chain_id)}",
        )
    return merged


def _rpc_json(chain_id: int, method: str, params: list, timeout_sec: float = 8.0) -> dict:
    last = None
    urls = _rpc_urls(chain_id)
    if not urls:
        raise RuntimeError(f"no rpc urls for chain_id={chain_id}")
    cid = int(chain_id)
    with _EXACT_CACHE_LOCK:
        primary = str(_RPC_PRIMARY_URL.get(cid) or "").strip()
    if primary and primary in urls:
        urls = [primary] + [u for u in urls if u != primary]
    elif not primary:
        # one-shot lightweight probe to avoid repeatedly hitting a dead first URL
        # on every strict call (which burns the whole budget).
        for u in list(urls):
            try:
                r = requests.post(
                    u,
                    json={"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []},
                    timeout=(2.0, 3.5),
                )
                r.raise_for_status()
                d = r.json() if isinstance(r.json(), dict) else {}
                if str(d.get("result") or "").startswith("0x"):
                    with _EXACT_CACHE_LOCK:
                        _RPC_PRIMARY_URL[cid] = str(u)
                    urls = [u] + [x for x in urls if x != u]
                    break
            except Exception:
                continue
    for url in urls:
        try:
            r = requests.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=(3.0, max(3.0, float(timeout_sec))),
            )
            r.raise_for_status()
            d = r.json() if isinstance(r.json(), dict) else {}
            if d.get("error"):
                last = RuntimeError(str(d.get("error")))
                continue
            with _EXACT_CACHE_LOCK:
                _RPC_PRIMARY_URL[cid] = str(url)
            return d
        except Exception as e:
            last = e
            continue
    raise RuntimeError(str(last or "rpc failed"))


def _rpc_primary_url(chain_id: int) -> str:
    cid = int(chain_id)
    with _EXACT_CACHE_LOCK:
        return str(_RPC_PRIMARY_URL.get(cid) or "").strip()


def _rpc_eth_call_hex(chain_id: int, to: str, data_hex: str, block_hex: str, timeout_sec: float = 8.0) -> str:
    d = _rpc_json(
        int(chain_id),
        "eth_call",
        [{"to": str(to).strip(), "data": str(data_hex).strip()}, str(block_hex)],
        timeout_sec=timeout_sec,
    )
    out = str(d.get("result") or "")
    if not out.startswith("0x"):
        raise RuntimeError("invalid eth_call result")
    return out


def _decode_uint_hex(data_hex: str) -> int:
    s = str(data_hex or "").strip().lower()
    if not s.startswith("0x"):
        return 0
    if s == "0x":
        return 0
    return int(s, 16)


def _erc20_decimals(chain_id: int, token: str) -> int:
    key = (int(chain_id), str(token).strip().lower())
    with _EXACT_CACHE_LOCK:
        if key in _DECIMALS_CACHE:
            return int(_DECIMALS_CACHE[key])
    ok = False
    try:
        out = _rpc_eth_call_hex(int(chain_id), str(token), "0x313ce567", "latest", timeout_sec=6.0)
        dec = int(_decode_uint_hex(out))
        if dec <= 0 or dec > 36:
            dec = 18
        else:
            ok = True
    except Exception:
        dec = 18
    # Cache only verified on-chain decimals; do not persist fallback=18 on transient RPC failures.
    if ok:
        with _EXACT_CACHE_LOCK:
            _DECIMALS_CACHE[key] = int(dec)
    return int(dec)


def _llama_block_for_day(chain_key: str, day_ts: int) -> int:
    ck = str(chain_key or "").strip().lower()
    lk = LLAMA_CHAIN_BY_KEY.get(ck, ck)
    key = (lk, int(day_ts))
    with _EXACT_CACHE_LOCK:
        b = _BLOCK_BY_DAY_CACHE.get(key)
        if b:
            return int(b)
    url = f"https://coins.llama.fi/block/{lk}/{int(day_ts)}"
    r = requests.get(url, timeout=(3.0, 8.0))
    r.raise_for_status()
    d = r.json() if isinstance(r.json(), dict) else {}
    block = int((d.get("height") or ((d.get("block") or {}).get("height")) or 0))
    if block <= 0:
        raise RuntimeError(f"llama block not found: chain={lk} ts={day_ts}")
    with _EXACT_CACHE_LOCK:
        _BLOCK_BY_DAY_CACHE[key] = int(block)
    return int(block)


def _coingecko_fill_price_cache(chain_key: str, token: str, day_start: int, day_end: int) -> None:
    global _CG_GLOBAL_BACKOFF_UNTIL
    global _CG_LAST_REQ_TS
    ck = str(chain_key or "").strip().lower()
    tk = str(token or "").strip().lower()
    if not (_is_eth_address(tk) and day_start > 0 and day_end >= day_start):
        return
    with _EXACT_CACHE_LOCK:
        exists = all((ck, tk, d) in _CG_DAY_PRICE_CACHE for d in range(day_start, day_end + 1, 86400))
        backoff_until = float(_CG_BACKOFF_UNTIL.get((ck, tk), 0.0))
        global_backoff_until = float(_CG_GLOBAL_BACKOFF_UNTIL)
    if exists:
        return
    if time.time() < backoff_until:
        _warn_fallback_once(
            f"cg_token_backoff:{ck}:{tk}",
            f"coingecko token cooldown active; skip token={tk} chain={ck}",
        )
        return
    if time.time() < global_backoff_until:
        _warn_fallback_once(
            f"cg_global_backoff:{ck}",
            f"coingecko global cooldown active; skip request on chain={ck}",
        )
        return
    platform = COINGECKO_PLATFORM_BY_KEY.get(ck, "")
    if not platform:
        return
    url = f"https://api.coingecko.com/api/v3/coins/{platform}/contract/{tk}/market_chart/range"
    headers = {"User-Agent": "uni-fee-agent/1.0"}
    params = {"vs_currency": "usd", "from": int(day_start), "to": int(day_end + 86399)}
    try:
        # Global throttle to avoid burst 429 from concurrent strict runs.
        with _CG_HTTP_LOCK:
            with _EXACT_CACHE_LOCK:
                wait_global_until = float(_CG_GLOBAL_BACKOFF_UNTIL)
                now = float(time.time())
                if now < wait_global_until:
                    return
                min_gap = max(0.25, float(os.environ.get("COINGECKO_MIN_REQ_GAP_SEC", "1.2")))
                last_ts = float(_CG_LAST_REQ_TS)
            sleep_sec = max(0.0, min_gap - max(0.0, now - last_ts))
            if sleep_sec > 0:
                time.sleep(min(2.5, sleep_sec))
            r = requests.get(url, headers=headers, params=params, timeout=(4.0, 14.0))
            with _EXACT_CACHE_LOCK:
                _CG_LAST_REQ_TS = float(time.time())
        r.raise_for_status()
    except requests.HTTPError as e:
        resp = getattr(e, "response", None)
        status = int(getattr(resp, "status_code", 0) or 0)
        if status == 429:
            retry_after = str((getattr(resp, "headers", {}) or {}).get("Retry-After", "")).strip()
            try:
                cool_sec = max(20.0, min(300.0, float(retry_after)))
            except Exception:
                cool_sec = 90.0
            with _EXACT_CACHE_LOCK:
                _CG_BACKOFF_UNTIL[(ck, tk)] = float(time.time() + cool_sec)
                _CG_GLOBAL_BACKOFF_UNTIL = max(float(_CG_GLOBAL_BACKOFF_UNTIL), float(time.time() + cool_sec))
            _warn_fallback_once(
                f"cg_429:{ck}:{tk}",
                f"coingecko rate-limited (429); fallback to llama for token={tk} chain={ck}; cooldown={cool_sec:.0f}s",
            )
            return
        _warn_fallback_once(
            f"cg_http_error:{ck}:{tk}:{status}",
            f"coingecko http error={status}; fallback to llama for token={tk} chain={ck}",
        )
        return
    except Exception:
        _warn_fallback_once(
            f"cg_request_error:{ck}:{tk}",
            f"coingecko request failed; fallback to llama for token={tk} chain={ck}",
        )
        return
    try:
        d = r.json() if isinstance(r.json(), dict) else {}
    except Exception:
        return
    pts = d.get("prices") if isinstance(d, dict) else None
    if not isinstance(pts, list):
        return
    by_day: dict[int, list[float]] = {}
    for it in pts:
        if not (isinstance(it, list) and len(it) >= 2):
            continue
        ms = int(float(it[0] or 0))
        px = float(it[1] or 0.0)
        if px <= 0:
            continue
        day = int((ms // 1000) // 86400 * 86400)
        by_day.setdefault(day, []).append(px)
    with _EXACT_CACHE_LOCK:
        for day, arr in by_day.items():
            if arr:
                _CG_DAY_PRICE_CACHE[(ck, tk, int(day))] = float(sum(arr) / len(arr))


def _historical_price_usd(chain_key: str, token: str, day_ts: int, day_start: int, day_end: int) -> float:
    ck = str(chain_key or "").strip().lower()
    tk = str(token or "").strip().lower()
    dts = int((int(day_ts) // 86400) * 86400)
    try:
        _coingecko_fill_price_cache(ck, tk, int(day_start), int(day_end))
    except Exception:
        _warn_fallback_once(
            f"cg_fill_exception:{ck}:{tk}",
            f"coingecko cache fill exception; fallback to llama for token={tk} chain={ck}",
        )
        pass
    cg_px = 0.0
    with _EXACT_CACHE_LOCK:
        px = _CG_DAY_PRICE_CACHE.get((ck, tk, dts))
        if px and float(px) > 0:
            cg_px = float(px)
    lk = LLAMA_CHAIN_BY_KEY.get(ck, ck)
    ll_px = 0.0
    with _EXACT_CACHE_LOCK:
        lp = _LLAMA_DAY_PRICE_CACHE.get((lk, tk, dts))
        if lp and float(lp) > 0:
            ll_px = float(lp)
    if ll_px <= 0.0:
        try:
            u = f"https://coins.llama.fi/prices/historical/{int(dts + 86399)}/{lk}:{tk}"
            r = requests.get(u, timeout=(2.0, 4.0))
            r.raise_for_status()
            data = r.json() if isinstance(r.json(), dict) else {}
            coin = (data.get("coins") or {}).get(f"{lk}:{tk}") or {}
            p = float(coin.get("price") or 0.0)
            if p > 0:
                ll_px = float(p)
                with _EXACT_CACHE_LOCK:
                    _LLAMA_DAY_PRICE_CACHE[(lk, tk, dts)] = float(p)
        except Exception:
            pass
    if cg_px > 0.0 and ll_px > 0.0:
        try:
            max_div = max(0.05, min(2.0, float(os.environ.get("HIST_PRICE_MAX_SOURCE_DIVERGENCE", "0.35"))))
        except Exception:
            max_div = 0.35
        rel = abs(float(cg_px) - float(ll_px)) / max(1e-12, max(float(cg_px), float(ll_px)))
        if rel <= float(max_div):
            return float((float(cg_px) + float(ll_px)) / 2.0)
        _warn_fallback_once(
            f"hist_price_source_conflict:{ck}:{tk}:{dts}",
            f"historical price conflict cg={cg_px:.6g} llama={ll_px:.6g}; using llama for token={tk} chain={ck}",
        )
        return float(ll_px)
    if ll_px > 0.0:
        _warn_fallback_once(
            f"price_fallback_llama:{ck}:{tk}",
            f"historical price source fallback: llama used for token={tk} chain={ck}",
        )
        return float(ll_px)
    if cg_px > 0.0:
        return float(cg_px)
    _warn_fallback_once(
        f"price_fallback_zero:{ck}:{tk}",
        f"historical price unavailable from coingecko+llama; returning 0 for token={tk} chain={ck}",
    )
    return 0.0


def _balance_of_raw(chain_id: int, token: str, owner: str, block_num: int, timeout_sec: float = 8.0) -> int:
    data = "0x70a08231" + str(owner).strip().lower().replace("0x", "").rjust(64, "0")
    out = _rpc_eth_call_hex(int(chain_id), str(token), data, hex(int(block_num)), timeout_sec=float(timeout_sec))
    return int(_decode_uint_hex(out))


def _strict_block_deltas(window: int) -> list[int]:
    w = int(max(0, window))
    deltas = [0]
    for i in range(1, w + 1):
        deltas.append(-i)
        deltas.append(i)
    return deltas


def _pool_balances_near_block(
    *,
    chain_id: int,
    token0: str,
    token1: str,
    pool_id: str,
    block_num: int,
    window: int,
    retries: int,
    deadline_ts: float | None = None,
    timeout_sec: float = 8.0,
) -> tuple[int, int, int]:
    last_err: Exception | None = None
    for delta in _strict_block_deltas(window):
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            raise TimeoutError("strict_balance_deadline")
        b = int(block_num) + int(delta)
        if b <= 0:
            continue
        for _ in range(max(1, int(retries))):
            if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
                raise TimeoutError("strict_balance_deadline")
            try:
                b0 = _balance_of_raw(chain_id, token0, pool_id, b, timeout_sec=float(timeout_sec))
                b1 = _balance_of_raw(chain_id, token1, pool_id, b, timeout_sec=float(timeout_sec))
                return int(b0), int(b1), int(b)
            except Exception as e:
                last_err = e
                continue
    raise RuntimeError(str(last_err or "strict balance window failed"))


def _sample_sparse_indices(total: int, points: int) -> list[int]:
    n = int(max(0, total))
    if n <= 0:
        return []
    if n <= 2:
        return list(range(n))
    k = int(max(2, min(int(points), n)))
    if k >= n:
        return list(range(n))
    idxs = {0, n - 1}
    if k > 2:
        step = float(n - 1) / float(k - 1)
        for i in range(1, k - 1):
            idxs.add(int(round(i * step)))
    return sorted(int(min(n - 1, max(0, x))) for x in idxs)


def _interpolate_scales(total: int, anchors: list[tuple[int, float]]) -> list[float]:
    n = int(max(0, total))
    if n <= 0:
        return []
    if not anchors:
        return [1.0] * n
    pts = sorted((int(i), float(s)) for i, s in anchors)
    out = [1.0] * n
    first_i, first_s = pts[0]
    for i in range(0, min(n, max(0, first_i))):
        out[i] = first_s
    for j in range(len(pts) - 1):
        i0, s0 = pts[j]
        i1, s1 = pts[j + 1]
        if i1 <= i0:
            continue
        for i in range(max(0, i0), min(n, i1 + 1)):
            t = float(i - i0) / float(max(1, i1 - i0))
            out[i] = float(s0 + (s1 - s0) * t)
    last_i, last_s = pts[-1]
    for i in range(max(0, last_i), n):
        out[i] = last_s
    return out


def _cap_pools(pools: list[dict], max_per_pair_chain: int, max_total: int) -> list[dict]:
    if not pools:
        return []
    out: list[dict] = []
    if max_per_pair_chain > 0:
        by_key: dict[tuple[str, str], list[dict]] = {}
        for p in pools:
            key = (str(p.get("chain") or ""), str(p.get("pair_label") or ""))
            by_key.setdefault(key, []).append(p)
        for items in by_key.values():
            items.sort(key=_pool_tvl_usd, reverse=True)
            out.extend(items[:max_per_pair_chain])
    else:
        out = list(pools)
    if max_total > 0 and len(out) > max_total:
        out.sort(key=_pool_tvl_usd, reverse=True)
        out = out[:max_total]
    return out


def _min_tvl(cli_value: Optional[float] = None) -> float:
    if cli_value is not None:
        return float(cli_value)
    v = os.environ.get("MIN_TVL")
    if v is not None:
        try:
            return float(v)
        except ValueError:
            pass
    return MIN_TVL_USD


def _base_v3_goldsky_endpoint() -> str:
    return str(GOLDSKY_ENDPOINTS.get("base") or "").strip()


def _is_base_v3_isolated_enabled() -> bool:
    # Default to The Graph path for Base v3 when a valid key is present.
    # Goldsky isolated route is opt-in only (BASE_V3_ISOLATED_PIPELINE=1).
    raw = str(os.environ.get("BASE_V3_ISOLATED_PIPELINE", "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_base_chain_enabled() -> bool:
    # Base is disabled by default due to frequent endpoint instability in production runs.
    raw = str(os.environ.get("ENABLE_BASE_CHAIN", "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _quick_graphql_healthcheck(endpoint: str) -> tuple[bool, str]:
    """Return (ok, detail). Detail is empty on success or a short reason on failure."""
    if not endpoint:
        return False, "no_endpoint"
    try:
        timeout_sec = max(2.0, float(os.environ.get("V3_ENDPOINT_HEALTHCHECK_TIMEOUT_SEC", "4")))
    except Exception:
        timeout_sec = 4.0
    try:
        r = requests.post(endpoint, json={"query": "query { __typename }"}, timeout=(3.0, timeout_sec))
        r.raise_for_status()
        data = r.json() if isinstance(r.json(), dict) else {}
        if not isinstance(data, dict):
            return False, "invalid_json"
        errs = data.get("errors")
        if errs:
            first = errs[0] if isinstance(errs, list) and errs else errs
            msg = str((first or {}).get("message", first) if isinstance(first, dict) else first)[:400]
            return False, msg or "graphql_errors"
        if "data" not in data:
            return False, "no_data_field"
        return True, ""
    except Exception as e:
        return False, str(e)[:400]


def _is_goldsky_public_endpoint(endpoint: str) -> bool:
    ep = str(endpoint or "").strip().lower()
    return ("api.goldsky.com" in ep) and ("/api/public/" in ep)


def _skip_healthcheck_for_endpoint(endpoint: str) -> bool:
    # Goldsky public endpoints are heavily rate-limited; extra healthcheck probes
    # consume quota and can trigger 429 before the real discovery query.
    if _is_goldsky_public_endpoint(endpoint):
        raw = str(os.environ.get("V3_SKIP_HEALTHCHECK_GOLDSKY", "1")).strip().lower()
        return raw in {"1", "true", "yes", "on"}
    return False


def _base_graphql_retries() -> int:
    try:
        return max(1, int(os.environ.get("V3_BASE_GRAPHQL_RETRIES", "3")))
    except Exception:
        return 3


def _base_goldsky_page_size() -> int:
    try:
        return max(20, min(100, int(os.environ.get("V3_BASE_GOLDSKY_PAGE_SIZE", "40"))))
    except Exception:
        return 40


def _base_goldsky_scan_pages() -> int:
    try:
        return max(1, min(200, int(os.environ.get("V3_BASE_GOLDSKY_SCAN_PAGES", "12"))))
    except Exception:
        return 12


def _base_goldsky_use_filtered_query() -> bool:
    # Filtered query with inputTokens_contains is often slower on Goldsky and can timeout.
    raw = str(os.environ.get("V3_BASE_GOLDSKY_USE_FILTERED_QUERY", "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _discover_base_v3_goldsky_pools(
    endpoint: str,
    token_a: str,
    token_b: str,
    max_results: int,
) -> list[dict]:
    # Goldsky Base v3 uses Messari-like schema (liquidityPools / inputTokens / inputTokenBalances).
    token_a = str(token_a or "").strip().lower()
    token_b = str(token_b or "").strip().lower()
    if not token_a or not token_b or token_a == token_b:
        return []
    page_size = _base_goldsky_page_size()
    q_filtered = """
    query BasePools($skip: Int!) {
      liquidityPools(
        first: %d,
        skip: $skip,
        orderBy: totalValueLockedUSD,
        orderDirection: desc,
        where: { inputTokens_contains: ["%s", "%s"] }
      ) {
        id
        totalValueLockedUSD
        inputTokenBalances
        inputTokens { id symbol decimals }
      }
    }
    """ % (page_size, token_a, token_b)
    q_unfiltered = """
    query BasePools($skip: Int!) {
      liquidityPools(first: %d, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id
        totalValueLockedUSD
        inputTokenBalances
        inputTokens { id symbol decimals }
      }
    }
    """ % (page_size,)
    use_filtered = _base_goldsky_use_filtered_query()
    query_text = q_filtered if use_filtered else q_unfiltered
    used_unfiltered = not use_filtered
    out: list[dict] = []
    skip = 0
    pages = 0
    max_pages = _base_goldsky_scan_pages()
    while pages < max_pages:
        try:
            data = graphql_query(endpoint, query_text, {"skip": skip}, retries=_base_graphql_retries())
        except Exception as e:
            msg = str(e).lower()
            if (not used_unfiltered) and (("inputtokens_contains" in msg) or ("timed out" in msg) or ("timeout" in msg)):
                query_text = q_unfiltered
                used_unfiltered = True
                continue
            # Do not drop already discovered pools because one later page timed out.
            if out:
                _warn_fallback_once(
                    "base_v3_goldsky_paging_timeout_partial",
                    "base v3 goldsky paging interrupted; returning partial discovery result",
                )
                break
            raise
        rows = data.get("data", {}).get("liquidityPools", []) or []
        if not rows:
            break
        added_this_page = 0
        for r in rows:
            toks = r.get("inputTokens") or []
            if not isinstance(toks, list) or len(toks) < 2:
                continue
            ids = [str((t or {}).get("id") or "").strip().lower() for t in toks[:2]]
            if token_a not in ids or token_b not in ids:
                continue
            balances = r.get("inputTokenBalances") or []
            bal0 = float(balances[0] or 0.0) if len(balances) > 0 else 0.0
            bal1 = float(balances[1] or 0.0) if len(balances) > 1 else 0.0
            p = {
                "id": str(r.get("id") or ""),
                "feeTier": 3000,  # fallback for alternative schema
                "token0": {
                    "id": ids[0],
                    "symbol": str((toks[0] or {}).get("symbol") or "?"),
                    "decimals": str((toks[0] or {}).get("decimals") or "18"),
                    "name": "",
                },
                "token1": {
                    "id": ids[1],
                    "symbol": str((toks[1] or {}).get("symbol") or "?"),
                    "decimals": str((toks[1] or {}).get("decimals") or "18"),
                    "name": "",
                },
                "totalValueLockedUSD": str(r.get("totalValueLockedUSD") or "0"),
                "totalValueLockedToken0": str(bal0),
                "totalValueLockedToken1": str(bal1),
                "volumeUSD": "0",
                "feesUSD": "0",
                "_base_schema": "goldsky_v3_alt",
            }
            if p["id"]:
                out.append(p)
                added_this_page += 1
                if int(max_results or 0) > 0 and len(out) >= int(max_results):
                    return out[: int(max_results)]
        if out and added_this_page == 0:
            # Rows are sorted by TVL desc; once matches stop appearing, deeper pages are unlikely useful.
            break
        if len(rows) < int(page_size):
            break
        skip += int(page_size)
        pages += 1
    return out


def _query_base_v3_goldsky_day_data(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> list[dict]:
    q = """
    query BasePoolDays($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      liquidityPoolDailySnapshots(
        first: 100,
        skip: $skip,
        orderBy: timestamp,
        orderDirection: asc,
        where: { pool: $pool, timestamp_gte: $start, timestamp_lte: $end }
      ) {
        timestamp
        totalValueLockedUSD
        dailySupplySideRevenueUSD
      }
    }
    """
    out: list[dict] = []
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            q,
            {"pool": str(pool_id or "").strip().lower(), "start": int(start_ts), "end": int(end_ts), "skip": int(skip)},
            retries=_base_graphql_retries(),
        )
        rows = data.get("data", {}).get("liquidityPoolDailySnapshots", []) or []
        if not rows:
            break
        for r in rows:
            ts = int(float(r.get("timestamp") or 0))
            out.append(
                {
                    "date": ts,
                    "tvlUSD": float(r.get("totalValueLockedUSD") or 0.0),
                    "feesUSD": float(r.get("dailySupplySideRevenueUSD") or 0.0),
                }
            )
        if len(rows) < 100:
            break
        skip += 100
    return out


def _normalize_light_pool_shape(pool: dict) -> dict:
    """
    Ensure pools returned by light discovery query have fields expected by downstream code.
    """
    p = dict(pool or {})
    p.setdefault("liquidity", "0")
    p.setdefault("token0Price", "0")
    p.setdefault("token1Price", "0")
    p.setdefault("volumeUSD", "0")
    p.setdefault("feesUSD", "0")
    p.setdefault("txCount", "0")
    return p


def _query_base_v3_pools_light(
    endpoint: str,
    token_a: str,
    token_b: str,
    max_results: int = 0,
) -> list[dict]:
    """
    Base v3 light discovery query for The Graph route.
    Uses reduced field set to avoid flaky indexer responses on heavy pool selections.
    """
    a = str(token_a or "").strip().lower()
    b = str(token_b or "").strip().lower()
    if not a or not b or a == b:
        return []
    q = """
    query PoolsLight($skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id
        feeTier
        token0 { id symbol }
        token1 { id symbol }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        volumeUSD
        feesUSD
        txCount
      }
      pools1: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id
        feeTier
        token0 { id symbol }
        token1 { id symbol }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        volumeUSD
        feesUSD
        txCount
      }
    }
    """ % (a, b, b, a)
    q_unfiltered = """
    query PoolsLightUnfiltered($skip: Int!) {
      pools(
        first: 100,
        skip: $skip,
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id
        feeTier
        token0 { id symbol }
        token1 { id symbol }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
      }
    }
    """

    def _is_match(row: dict) -> bool:
        t0 = str(((row.get("token0") or {}).get("id") or "")).strip().lower()
        t1 = str(((row.get("token1") or {}).get("id") or "")).strip().lower()
        return (t0 == a and t1 == b) or (t0 == b and t1 == a)

    out: list[dict] = []
    skip = 0
    try:
        while True:
            data = graphql_query(endpoint, q, {"skip": int(skip)}, retries=_base_graphql_retries())
            d = data.get("data", {}) or {}
            p0 = d.get("pools0", []) or []
            p1 = d.get("pools1", []) or []
            for row in p0 + p1:
                out.append(_normalize_light_pool_shape(row))
            if int(max_results or 0) > 0 and len(out) >= int(max_results):
                return out[: int(max_results)]
            if len(p0) < 100 and len(p1) < 100:
                break
            skip += 100
        return out
    except Exception:
        # Some indexers fail on filtered pool queries for Base; retry with unfiltered scan and local filtering.
        pass
    out = []
    skip = 0
    max_scan_pages = max(1, _env_int("V3_BASE_LIGHT_SCAN_PAGES", 3))
    pages = 0
    while pages < max_scan_pages:
        data = graphql_query(endpoint, q_unfiltered, {"skip": int(skip)}, retries=_base_graphql_retries())
        rows = (data.get("data", {}) or {}).get("pools", []) or []
        if not rows:
            break
        added_this_page = 0
        for row in rows:
            if _is_match(row):
                out.append(_normalize_light_pool_shape(row))
                added_this_page += 1
        if int(max_results or 0) > 0 and len(out) >= int(max_results):
            return out[: int(max_results)]
        if out and added_this_page == 0:
            # Sorted by TVL desc: if current page brought no matches after we already found some,
            # the next pages are unlikely to add useful results; stop early to cut tail latency.
            break
        if len(rows) < 100:
            break
        skip += 100
        pages += 1
    return out


def discover_pools_v3(
    token_pairs_str: str,
    min_tvl: Optional[float] = None,
    fresh_token_lookup: bool = False,
) -> list[dict]:
    """Discover v3 pools only."""
    pairs = parse_token_pairs(token_pairs_str)
    dynamic_tokens = load_dynamic_tokens()
    all_pools: list[dict] = []
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = {c for c in chains if c.lower() in include}
    if "base" in chains and not _is_base_chain_enabled():
        chains = {c for c in chains if c.lower() != "base"}
        print("  [base] v3: excluded by default (set ENABLE_BASE_CHAIN=1 to include)")
    discovery_cap = max(0, _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN", 0))
    chain_workers = max(1, min(_env_int("V3_DISCOVERY_CHAIN_WORKERS", 4), len(chains) or 1))
    dyn_lock = threading.Lock()

    def _discover_for_chain(chain: str) -> list[dict]:
        out_chain: list[dict] = []
        use_base_goldsky = bool(
            chain == "base"
            and _is_base_v3_isolated_enabled()
            and _base_v3_goldsky_endpoint()
        )
        endpoint = _base_v3_goldsky_endpoint() if use_base_goldsky else get_graph_endpoint(chain, "v3")
        # If endpoint resolver returns Goldsky (e.g. missing/invalid Graph key),
        # force Goldsky schema path even when isolated pipeline flag is off.
        if chain == "base" and _is_goldsky_public_endpoint(endpoint):
            use_base_goldsky = True
        if not endpoint:
            return out_chain
        check_enabled = str(os.environ.get("V3_ENDPOINT_HEALTHCHECK", "1")).strip().lower() in {"1", "true", "yes", "on"}
        if check_enabled and _skip_healthcheck_for_endpoint(endpoint):
            check_enabled = False
        if check_enabled:
            health_ok, health_detail = _quick_graphql_healthcheck(endpoint)
        else:
            health_ok, health_detail = True, ""
        if check_enabled and not health_ok:
            detail = f": {health_detail}" if health_detail else ""
            print(f"  [{chain}] v3: skip (endpoint healthcheck failed{detail})")
            return out_chain

        def _resolve_with_cache(sym: str) -> list[str]:
            out: list[str] = []
            for c in [chain, "ethereum"]:
                with dyn_lock:
                    out.extend(get_token_addresses(c, sym, dynamic_tokens))
            out = list(dict.fromkeys(out))[:1]
            if not out:
                addr = query_token_by_symbol(endpoint, sym)
                if addr:
                    with dyn_lock:
                        save_dynamic_token(chain, sym, addr)
                        dynamic_tokens.setdefault(chain, {})[sym.lower()] = addr
                    out = [addr]
            return out

        for base, quote in pairs:
            base_addrs, quote_addrs = [], []

            if fresh_token_lookup:
                base_addrs = _resolve_with_cache(base)
                quote_addrs = _resolve_with_cache(quote)
            else:
                for c in [chain, "ethereum"]:
                    with dyn_lock:
                        base_addrs.extend(get_token_addresses(c, base, dynamic_tokens))
                        quote_addrs.extend(get_token_addresses(c, quote, dynamic_tokens))
                base_addrs = list(dict.fromkeys(base_addrs))[:1]
                quote_addrs = list(dict.fromkeys(quote_addrs))[:1]
                if not base_addrs:
                    base_addrs = _resolve_with_cache(base)
                if not quote_addrs:
                    quote_addrs = _resolve_with_cache(quote)

            if not base_addrs or not quote_addrs:
                continue
            try:
                if use_base_goldsky:
                    pools = _discover_base_v3_goldsky_pools(
                        endpoint,
                        base_addrs[0],
                        quote_addrs[0],
                        max_results=int(discovery_cap),
                    )
                else:
                    if chain == "base":
                        # For Base on The Graph route use lightweight query shape first.
                        pools = _query_base_v3_pools_light(
                            endpoint,
                            base_addrs[0],
                            quote_addrs[0],
                            max_results=int(discovery_cap),
                        )
                    else:
                        pools = query_pools_containing_both_tokens(
                            endpoint,
                            base_addrs[0],
                            quote_addrs[0],
                            0.0,
                            max_results=int(discovery_cap),
                        )
                    if not pools:
                        # Fallback for ambiguous symbol->address mappings (e.g. multiple tokens with same symbol).
                        pools = query_pools_by_token_symbols(
                            endpoint,
                            base,
                            quote,
                            0.0,
                            max_results=int(discovery_cap),
                        )
                min_tvl_now = _min_tvl(min_tvl)
                filtered_pools: list[dict] = []
                skipped_missing_price = 0
                for p in pools:
                    ext_tvl, price_source, price_err = _resolve_pool_tvl_now_onchain_v3(p, chain)
                    if float(ext_tvl) <= 0:
                        skipped_missing_price += 1
                        pid = str((p or {}).get("id") or "")
                        print(
                            f"[warn] PRICE_UNAVAILABLE chain={chain} pair={base}/{quote} "
                            f"pool={pid} reason={price_err or 'unknown'}"
                        )
                        continue
                    if float(ext_tvl) >= float(min_tvl_now):
                        filtered_pools.append(p)
                pools = filtered_pools
                if skipped_missing_price > 0:
                    print(
                        f"[warn] PRICE_FILTER_DROPPED chain={chain} pair={base}/{quote} "
                        f"count={skipped_missing_price}"
                    )
                for p in pools:
                    p["chain"] = chain
                    p["version"] = "v3"
                    p["pair_label"] = f"{base}/{quote}"
                    out_chain.append(p)
            except Exception as e:
                print(f"  [{chain}] v3 {base}/{quote}: {e}")
        return out_chain

    if chains:
        with ThreadPoolExecutor(max_workers=chain_workers) as ex:
            futs = [ex.submit(_discover_for_chain, c) for c in sorted(chains)]
            for fut in as_completed(futs):
                try:
                    all_pools.extend(fut.result() or [])
                except Exception as e:
                    print(f"  [v3-discovery] chain worker error: {e}")
    seen = set()
    unique = []
    for p in all_pools:
        key = (p.get("chain", ""), p.get("id", ""))
        if key[1] and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def compute_fee_and_tvl_series(pool: dict, endpoint: str) -> dict:
    """Load day-level fees; TVL/income are rebuilt from external TVL later."""
    # Exact inclusive window of FEE_DAYS calendar days (UTC day starts).
    start_ts, end_ts = build_exact_day_window(int(FEE_DAYS))

    if str(pool.get("_base_schema") or "") == "goldsky_v3_alt":
        day_data = _query_base_v3_goldsky_day_data(endpoint, pool["id"], start_ts, end_ts)
    else:
        day_data = query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fees_usd_series = []
    raw_tvl_series = []
    try:
        fee_tier_ppm = int(pool.get("feeTier") or 0)
    except (TypeError, ValueError):
        fee_tier_ppm = 0
    for d in day_data:
        if str(pool.get("_base_schema") or "") == "goldsky_v3_alt":
            fees = float(d.get("feesUSD") or 0)
        else:
            fees = poolday_lp_fees_usd_exact(d, fee_tier_ppm)
        if fees <= 0:
            fees = 0.0
        ts = int(d["date"])
        fees_usd_series.append((ts, fees))
        try:
            raw_tvl = float(d.get("tvlUSD") or 0.0)
        except Exception:
            raw_tvl = 0.0
        raw_tvl_series.append((ts, max(0.0, raw_tvl)))
    return {"_fees_usd": fees_usd_series, "_raw_tvl_usd": raw_tvl_series}


def _build_exact_tvl_series_v3(
    *,
    chain: str,
    pool: dict,
    fees_usd: list[tuple[int, float]],
    baseline_tvl: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str, float]:
    if not fees_usd:
        return [], "no_fees_series", 0.0
    if not baseline_tvl or len(baseline_tvl) != len(fees_usd):
        return [], "baseline_mismatch", 0.0
    ck = str(chain or "").strip().lower()
    if ck not in V3_EXACT_TVL_CHAINS:
        return [], "chain_not_enabled", 0.0
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return [], "chain_id_unknown", 0.0
    pool_id = str(pool.get("id") or "").strip().lower()
    t0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    t1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if not (_is_eth_address(pool_id) and _is_eth_address(t0) and _is_eth_address(t1)):
        return [], "pool_or_tokens_not_address", 0.0

    t0_dec = _erc20_decimals(chain_id, t0)
    t1_dec = _erc20_decimals(chain_id, t1)

    # Strict mode: full day-by-day exact reconstruction (slow, highest rigor).
    if V3_EXACT_TVL_STRICT_MODE:
        out_strict: list[tuple[int, float]] = []
        non_zero_strict = 0
        timed_out = False
        t_started = time.perf_counter()
        strict_deadline = time.monotonic() + float(max(1.0, budget_sec))
        for ts, _ in fees_usd:
            if (time.perf_counter() - t_started) > float(max(1.0, budget_sec)):
                timed_out = True
                break
            dts = int((int(ts) // 86400) * 86400)
            try:
                block_num = _llama_block_for_day(ck, int(dts + 86399))
                raw0, raw1, _resolved_block = _pool_balances_near_block(
                    chain_id=chain_id,
                    token0=t0,
                    token1=t1,
                    pool_id=pool_id,
                    block_num=int(block_num),
                    window=int(V3_EXACT_STRICT_BLOCK_WINDOW),
                    retries=int(V3_EXACT_STRICT_RPC_RETRIES),
                    deadline_ts=float(strict_deadline),
                    timeout_sec=float(max(1.5, min(4.0, float(max(1.0, budget_sec) / 20.0)))),
                )
                b0 = float(raw0) / (10 ** int(t0_dec))
                b1 = float(raw1) / (10 ** int(t1_dec))
                p0 = _historical_price_usd(ck, t0, dts, day_start_ts, day_end_ts)
                p1 = _historical_price_usd(ck, t1, dts, day_start_ts, day_end_ts)
                tvl = max(0.0, float(b0) * max(0.0, float(p0))) + max(0.0, float(b1) * max(0.0, float(p1)))
                out_strict.append((int(ts), float(tvl)))
                if float(tvl) > 0.0:
                    non_zero_strict += 1
            except Exception:
                out_strict.append((int(ts), 0.0))
        if not out_strict:
            return [], ("strict_budget_timeout" if timed_out else "strict_no_points"), 0.0
        coverage = float(non_zero_strict) / float(max(1, len(out_strict)))
        if non_zero_strict <= 0:
            return [], ("strict_budget_timeout" if timed_out else "strict_insufficient_non_zero"), float(coverage)
        if coverage < float(V3_EXACT_TVL_MIN_COVERAGE_PARTIAL):
            return [], ("strict_budget_timeout" if timed_out else "strict_insufficient_non_zero"), float(coverage)
        return out_strict, ("strict_budget_partial" if timed_out else "strict_ok"), float(coverage)

    total_days = len(fees_usd)
    sparse_idxs = _sample_sparse_indices(total_days, int(V3_EXACT_TVL_SPARSE_POINTS))
    if not sparse_idxs:
        return [], "no_sparse_points", 0.0
    probe_days = min(int(V3_EXACT_TVL_PROBE_DAYS), len(sparse_idxs))
    points: list[tuple[int, float, float]] = []  # idx, exact_tvl, baseline_tvl
    non_zero = 0
    timed_out = False
    t_started = time.perf_counter()
    for idx in sparse_idxs:
        if (time.perf_counter() - t_started) > float(max(1.0, budget_sec)):
            timed_out = True
            break
        ts, _ = fees_usd[idx]
        dts = int((int(ts) // 86400) * 86400)
        try:
            block_num = _llama_block_for_day(ck, int(dts + 86399))
            b0 = _balance_of_raw(chain_id, t0, pool_id, block_num) / (10 ** int(t0_dec))
            b1 = _balance_of_raw(chain_id, t1, pool_id, block_num) / (10 ** int(t1_dec))
            p0 = _historical_price_usd(ck, t0, dts, day_start_ts, day_end_ts)
            p1 = _historical_price_usd(ck, t1, dts, day_start_ts, day_end_ts)
            tvl = max(0.0, float(b0) * max(0.0, float(p0))) + max(0.0, float(b1) * max(0.0, float(p1)))
            base_v = float(baseline_tvl[idx][1] or 0.0)
            points.append((int(idx), float(tvl), float(base_v)))
            if float(tvl) > 0.0:
                non_zero += 1
        except Exception:
            base_v = float(baseline_tvl[idx][1] or 0.0)
            points.append((int(idx), 0.0, float(base_v)))
        processed = len(points)
        if processed >= probe_days and non_zero <= 0:
            return [], "insufficient_non_zero_days_probe", 0.0
        remaining = max(0, len(sparse_idxs) - processed)
        max_possible_success = float(non_zero + remaining) / float(max(1, len(sparse_idxs)))
        if max_possible_success < float(V3_EXACT_TVL_SPARSE_MIN_SUCCESS):
            current_success = float(non_zero) / float(max(1, processed))
            return [], "insufficient_sparse_success_impossible", float(current_success)
    if not points:
        return [], ("budget_timeout" if timed_out else "insufficient_non_zero_days"), 0.0
    sparse_success = float(non_zero) / float(max(1, len(points)))
    if non_zero <= 0:
        return [], ("budget_timeout" if timed_out else "insufficient_non_zero_days"), float(sparse_success)
    if sparse_success < float(V3_EXACT_TVL_SPARSE_MIN_SUCCESS):
        return [], ("budget_timeout" if timed_out else "insufficient_sparse_success"), float(sparse_success)
    max_scale = float(V3_EXACT_TVL_SPARSE_MAX_SCALE)
    min_scale = 1.0 / max(1.0, max_scale)
    anchors: list[tuple[int, float]] = []
    for i, ex_v, base_v in points:
        if ex_v <= 0.0 or base_v <= 0.0:
            continue
        scale = max(min_scale, min(max_scale, float(ex_v) / float(base_v)))
        anchors.append((int(i), float(scale)))
    if not anchors:
        return [], "insufficient_scale_anchors", float(sparse_success)
    scales = _interpolate_scales(total_days, anchors)
    out: list[tuple[int, float]] = []
    for i, (ts, _) in enumerate(fees_usd):
        base_v = float(baseline_tvl[i][1] or 0.0)
        out.append((int(ts), max(0.0, float(base_v) * float(scales[i]))))
    reason = "sparse_budget_partial" if timed_out else "sparse_ok"
    return out, reason, float(sparse_success)


def save_pdf(pools: list[dict], path: str) -> None:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    doc = SimpleDocTemplate(path, pagesize=landscape(A4), rightMargin=1.5 * cm, leftMargin=1.5 * cm)
    story = [Paragraph("Uniswap v3 Pools (base version, TVL &gt; $1000)", getSampleStyleSheet()["Title"]), Spacer(1, 0.5 * cm)]
    if not pools:
        story.append(Paragraph("No pools found.", getSampleStyleSheet()["Normal"]))
    else:
        data = [["Chain", "Pair", "Pool", "Fee %", "TVL USD", "Volume USD"]]
        for p in pools:
            t0 = (p.get("token0") or {}).get("symbol", "?")
            t1 = (p.get("token1") or {}).get("symbol", "?")
            fee_pct = int(p.get("feeTier") or 0) / 10000
            tvl = float(p.get("effectiveTvlUSD") or p.get("pool_tvl_now_usd") or p.get("totalValueLockedUSD") or 0)
            vol = float(p.get("volumeUSD") or 0)
            pid = p.get("id", "")
            data.append([
                p.get("chain", ""),
                p.get("pair_label", f"{t0}/{t1}"),
                pid,  # full pool id
                f"{fee_pct}%",
                f"${tvl:,.0f}",
                f"${vol:,.0f}",
            ])
        t = Table(data, colWidths=[2.5 * cm, 3 * cm, 10 * cm, 2 * cm, 4 * cm, 4 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ]))
        story.append(t)
    doc.build(story)
    print(f"Saved: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 1: Uniswap v3 (base version)")
    parser.add_argument("--min-tvl", type=float, default=None, help="Min TVL USD")
    args = parser.parse_args()

    token_pairs = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    min_tvl_val = _min_tvl(args.min_tvl)
    suffix = pairs_to_filename_suffix(token_pairs)
    os.makedirs("data", exist_ok=True)

    print("Agent 1: Uniswap v3 (base version)")
    print("Token pairs:", token_pairs, "| Min TVL: $%s" % f"{min_tvl_val:,.0f}".replace(",", " "))
    print("Discovering v3 pools...")
    fresh = "TOKEN_PAIRS" in os.environ
    pools = discover_pools_v3(token_pairs, args.min_tvl, fresh_token_lookup=fresh)
    max_per_pair_chain = max(0, _env_int("MAX_POOLS_PER_PAIR_CHAIN", 40))
    max_total = max(0, _env_int("MAX_POOLS_TOTAL", 300))
    pools = _cap_pools(pools, max_per_pair_chain=max_per_pair_chain, max_total=max_total)
    print(f"Found {len(pools)} v3 pools")

    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, f"data/available_pairs_v3_{suffix}.pdf")
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    pool_chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))
    exact_slots_lock = threading.Lock()
    exact_slots_left = int(V3_EXACT_TVL_MAX_POOLS if V3_EXACT_TVL_ENABLE else 0)

    def _process_pool(idx: int, pool: dict) -> tuple[int, str | None, dict | None, str]:
        nonlocal exact_slots_left
        chain = pool.get("chain", "unknown")
        t0 = (pool.get("token0") or {}).get("symbol", "?")
        t1 = (pool.get("token1") or {}).get("symbol", "?")
        pool_id = pool.get("id", "")
        raw_fee_tier = int(pool.get("feeTier") or 0)
        fee_pct = raw_fee_tier / 10000
        pair = f"{t0}/{t1}"
        if chain == "base" and str(pool.get("_base_schema") or "") == "goldsky_v3_alt" and _base_v3_goldsky_endpoint():
            endpoint = _base_v3_goldsky_endpoint()
        else:
            endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair}: skipped (no endpoint)"
        data = compute_fee_and_tvl_series(pool, endpoint)
        # Hard rule: TVL "now" must always come from reserves * external prices.
        # Never trust prefilled pool TVL fields for current-point valuation.
        pool_tvl_now_usd, price_source, price_err = _resolve_pool_tvl_now_onchain_v3(pool, chain)
        if float(pool_tvl_now_usd) <= 0:
            msg = (
                f"  [{idx+1}/{len(pools)}] {chain} {pair}: "
                f"skipped (external TVL unavailable: {price_err or 'unknown'})"
            )
            return idx, None, None, msg
        # Build TVL and profitability strictly from external TVL (no subgraph TVL dependency).
        data["fees"] = []
        data["tvl"] = []
        tvl_quality = "estimated"
        tvl_quality_reason = "default_scaled"
        estimated_tvl_compare: list[tuple[int, float]] = []
        estimated_fees_compare: list[tuple[int, float]] = []
        try:
            fees_usd = data.get("_fees_usd") or []
            raw_tvl = data.get("_raw_tvl_usd") or []
            if fees_usd and pool_tvl_now_usd > 0:
                tvl_series = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]
                if raw_tvl and len(raw_tvl) == len(fees_usd):
                    anchor = 0.0
                    for _, rv in reversed(raw_tvl):
                        rvf = float(rv or 0.0)
                        if rvf > 0:
                            anchor = rvf
                            break
                    if anchor > 0:
                        shaped: list[tuple[int, float]] = []
                        for i, (ts, _) in enumerate(fees_usd):
                            rv = float(raw_tvl[i][1] or 0.0)
                            if rv > 0:
                                day_tvl = float(pool_tvl_now_usd) * (rv / anchor)
                                day_tvl = max(float(pool_tvl_now_usd) * 0.2, min(float(pool_tvl_now_usd) * 5.0, day_tvl))
                            else:
                                day_tvl = float(pool_tvl_now_usd)
                            shaped.append((int(ts), float(day_tvl)))
                        tvl_series = shaped
                        if tvl_quality_reason == "default_scaled":
                            tvl_quality_reason = "scaled_by_raw_tvl"
                estimated_tvl_compare = list(tvl_series)
                estimated_cumul = 0.0
                for i, (ts, fees_day) in enumerate(fees_usd):
                    tvl_day_est = float(estimated_tvl_compare[i][1] or 0.0) if i < len(estimated_tvl_compare) else float(pool_tvl_now_usd)
                    if float(fees_day) > 0:
                        estimated_cumul += float(fees_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day_est))
                    estimated_fees_compare.append((int(ts), float(estimated_cumul)))
                exact_used = False
                if V3_EXACT_TVL_ENABLE and str(chain).strip().lower() in V3_EXACT_TVL_CHAINS and fees_usd:
                    chain_id = int(CHAIN_ID_BY_KEY.get(str(chain).strip().lower(), 0) or 0)
                    rpc_ready = bool(chain_id > 0 and _rpc_urls(chain_id))
                    if not rpc_ready:
                        tvl_quality_reason = "exact_skipped:no_rpc_urls"
                    else:
                        take_slot = False
                        slot_reserved = False
                        with exact_slots_lock:
                            if exact_slots_left > 0:
                                exact_slots_left -= 1
                                take_slot = True
                                slot_reserved = True
                        if take_slot:
                            exact_days = max(1, int(V3_EXACT_TVL_DAYS_MAX))
                            fees_exact = fees_usd[-min(len(fees_usd), exact_days):]
                            day_start_ts = int((int(fees_exact[0][0]) // 86400) * 86400)
                            day_end_ts = int((int(fees_exact[-1][0]) // 86400) * 86400)
                            exact_series, exact_reason, exact_cov = _build_exact_tvl_series_v3(
                                chain=str(chain),
                                pool=pool,
                                fees_usd=fees_exact,
                                baseline_tvl=tvl_series[-len(fees_exact):],
                                day_start_ts=day_start_ts,
                                day_end_ts=day_end_ts,
                                budget_sec=float(V3_EXACT_TVL_POOL_BUDGET_SEC),
                            )
                            if exact_series and len(exact_series) == len(fees_exact):
                                merged_exact = list(tvl_series)
                                exact_points = 0
                                exact_start = len(fees_usd) - len(fees_exact)
                                for i, (ts, ex_v) in enumerate(exact_series):
                                    tvl_idx = exact_start + i
                                    if float(ex_v) > 0:
                                        exact_points += 1
                                        merged_exact[tvl_idx] = (int(ts), float(ex_v))
                                    else:
                                        merged_exact[tvl_idx] = (
                                            int(ts),
                                            float(tvl_series[tvl_idx][1] if tvl_idx < len(tvl_series) else pool_tvl_now_usd),
                                        )
                                coverage = float(exact_points) / float(max(1, len(exact_series)))
                                tvl_series = merged_exact
                                exact_used = True
                                full_window = len(fees_exact) == len(fees_usd)
                                full_exact_mode = not str(exact_reason).startswith("sparse_")
                                if full_exact_mode and full_window and coverage >= float(V3_EXACT_TVL_MIN_COVERAGE_EXACT) and exact_points == len(exact_series):
                                    tvl_quality = "exact"
                                    tvl_quality_reason = "ok"
                                else:
                                    tvl_quality = "exact_partial"
                                    tvl_quality_reason = f"{exact_reason}:{coverage:.2f}"
                            else:
                                tvl_quality_reason = f"exact_failed:{exact_reason}:{exact_cov:.2f}"
                            # Return exact slot when no exact data was produced for this pool.
                            if slot_reserved and not exact_used:
                                with exact_slots_lock:
                                    exact_slots_left += 1
                        else:
                            tvl_quality_reason = "exact_skipped:no_slots"
                data["tvl"] = tvl_series
                cumul = 0.0
                fees_rebuilt = []
                for i, (ts, fees_day) in enumerate(fees_usd):
                    tvl_day = float(data["tvl"][i][1] or 0.0) if i < len(data["tvl"]) else float(pool_tvl_now_usd)
                    if float(fees_day) > 0:
                        cumul += float(fees_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day))
                    fees_rebuilt.append((ts, cumul))
                data["fees"] = fees_rebuilt
        except Exception:
            pass
        data.pop("_fees_usd", None)
        data.pop("_raw_tvl_usd", None)
        try:
            raw_pool_tvl = float(pool.get("totalValueLockedUSD") or 0.0)
        except Exception:
            raw_pool_tvl = 0.0
        payload = {
            **data,
            "pool_id": pool_id,
            "fee_pct": fee_pct,
            "raw_fee_tier": raw_fee_tier,
            "pool_tvl_now_usd": pool_tvl_now_usd,
            "pool_tvl_subgraph_usd": raw_pool_tvl,
            "tvl_multiplier": 1.0,
            "tvl_price_source": str(pool.get("tvl_price_source") or ""),
            "pair": pair,
            "chain": chain,
            "version": "v3",
            "data_quality": tvl_quality,
            "data_quality_reason": tvl_quality_reason,
        }
        payload["strict_compare_estimated_tvl"] = estimated_tvl_compare
        payload["strict_compare_estimated_fees"] = estimated_fees_compare
        msg = f"  [{idx+1}/{len(pools)}] {chain} {pair}: {len(data.get('fees') or [])} days"
        if V3_EXACT_TVL_DEBUG:
            msg += f" | quality={tvl_quality} reason={tvl_quality_reason}"
        return idx, pool_id, payload, msg

    if pools:
        pools_for_series = sorted(pools, key=_pool_tvl_usd, reverse=True)
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_process_pool, i, p) for i, p in enumerate(pools_for_series)]
            for fut in as_completed(futures):
                try:
                    _, pool_id, payload, msg = fut.result()
                    if pool_id and payload:
                        pool_chart_data[pool_id] = payload
                    print(msg)
                except Exception as e:
                    print(f"  [series] error - {e}")

    out_json = f"data/pools_v3_{suffix}.json"
    save_chart_data_json(pool_chart_data, out_json)
    print("Done. Output:", out_json)


if __name__ == "__main__":
    main()
