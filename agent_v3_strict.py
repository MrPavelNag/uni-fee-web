#!/usr/bin/env python3
"""
Strict single-pool v3 agent.

- Requires TARGET_POOL_ID (pool contract address).
- Probes chains by pool id (no broad pair discovery).
- Produces both estimated and exact compare series.
- Outputs to data/pools_v3_{suffix}.json (same format as v3 agent).
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
import threading
import requests
from datetime import datetime
from typing import Any

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V3_SUBGRAPHS
from agent_common import build_exact_day_window, pairs_to_filename_suffix, resolve_pool_tvl_now_external, save_chart_data_json
from uniswap_client import get_graph_endpoint, graphql_query, query_pool_day_data
from agent_v3 import (
    _balance_of_raw,
    CHAIN_ID_BY_KEY,
    _erc20_decimals,
    _historical_price_usd,
    _llama_block_for_day,
    _rpc_primary_url,
    _rpc_json,
    _rpc_urls,
)


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_POOL_CHAIN_CACHE_PATH = os.environ.get("STRICT_POOL_CHAIN_CACHE_PATH", "data/strict_pool_chain_cache.json")
_EXACT2_CACHE_DB_PATH = os.environ.get("STRICT_EXACT2_CACHE_DB_PATH", "data/exact2_daily_cache.sqlite3")
_WARN_LOCK = threading.Lock()
_FALLBACK_WARNED: set[str] = set()


def _warn_fallback_once(key: str, message: str) -> None:
    k = str(key or "").strip()
    if not k:
        return
    with _WARN_LOCK:
        if k in _FALLBACK_WARNED:
            return
        _FALLBACK_WARNED.add(k)
    print(f"[WARN][strict] {message}")


def _strict_debug_enabled() -> bool:
    return str(os.environ.get("STRICT_DEBUG_TIMING", "1")).strip().lower() in {"1", "true", "yes", "on"}


def _strict_debug(msg: str) -> None:
    if _strict_debug_enabled():
        print(f"[strict][timing] {msg}")


def _is_eth_address(v: str) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("0x") and len(s) == 42


def _target_pool_id() -> str:
    raw = str(os.environ.get("TARGET_POOL_ID", "") or "").strip().lower()
    return raw if _is_eth_address(raw) else ""


def _token_decimals_from_pool(pool: dict, key: str) -> int:
    try:
        t = pool.get(key) or {}
        d = int(t.get("decimals") or 0)
        if 0 < d <= 36:
            return int(d)
    except Exception:
        pass
    return 0


def _symbol_decimals_hint(symbol: str) -> int:
    s = str(symbol or "").strip().upper()
    hints = {
        "USDT": 6,
        "USDC": 6,
        "USDC.E": 6,
        "WBTC": 8,
    }
    return int(hints.get(s, 0) or 0)


def _include_chains() -> list[str]:
    include = [c.strip().lower() for c in str(os.environ.get("INCLUDE_CHAINS", "")).split(",") if c.strip()]
    if include:
        return include
    return sorted(set(UNISWAP_V3_SUBGRAPHS.keys()))


def _pool_chain_cache_load() -> dict[str, dict]:
    p = str(_POOL_CHAIN_CACHE_PATH or "").strip()
    if not p or not os.path.isfile(p):
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
        return raw if isinstance(raw, dict) else {}
    except Exception:
        return {}


def _pool_chain_cache_save(cache: dict[str, dict]) -> None:
    p = str(_POOL_CHAIN_CACHE_PATH or "").strip()
    if not p:
        return
    try:
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=True, indent=2)
    except Exception:
        pass


def _pool_chain_cache_get(pool_id: str) -> str:
    pid = str(pool_id or "").strip().lower()
    if not _is_eth_address(pid):
        return ""
    rec = _pool_chain_cache_load().get(pid) or {}
    chain = str(rec.get("chain") or "").strip().lower()
    if chain in UNISWAP_V3_SUBGRAPHS:
        return chain
    return ""


def _pool_chain_cache_put(pool_id: str, chain: str) -> None:
    pid = str(pool_id or "").strip().lower()
    ck = str(chain or "").strip().lower()
    if not (_is_eth_address(pid) and ck in UNISWAP_V3_SUBGRAPHS):
        return
    cache = _pool_chain_cache_load()
    cache[pid] = {"chain": ck, "updated_at": int(time.time())}
    _pool_chain_cache_save(cache)


def _rpc_eth_call_latest(chain_id: int, to_addr: str, data_hex: str) -> str:
    params = [{"to": str(to_addr).strip().lower(), "data": str(data_hex).strip().lower()}, "latest"]
    d = _rpc_json(int(chain_id), "eth_call", params, timeout_sec=6.0)
    out = str(d.get("result") or "")
    return out if out.startswith("0x") else ""


def _is_probably_v3_pool_on_chain(pool_id: str, chain: str) -> bool:
    ck = str(chain or "").strip().lower()
    cid = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if cid <= 0:
        return False
    pid = str(pool_id or "").strip().lower()
    try:
        code_res = _rpc_json(int(cid), "eth_getCode", [pid, "latest"], timeout_sec=6.0)
        code = str(code_res.get("result") or "").strip().lower()
        if not code or code in {"0x", "0x0"}:
            return False
        token0 = _rpc_eth_call_latest(cid, pid, "0x0dfe1681")
        token1 = _rpc_eth_call_latest(cid, pid, "0xd21220a7")
        fee = _rpc_eth_call_latest(cid, pid, "0xddca3f43")
        if not (token0 and token1 and fee):
            return False
        t0 = "0x" + token0[-40:]
        t1 = "0x" + token1[-40:]
        try:
            fee_i = int(fee, 16)
        except Exception:
            return False
        return _is_eth_address(t0) and _is_eth_address(t1) and t0 != t1 and fee_i > 0
    except Exception:
        return False


def _detect_chain_by_rpc(pool_id: str, chains: list[str]) -> str:
    for chain in chains:
        if _is_probably_v3_pool_on_chain(pool_id, chain):
            return str(chain).strip().lower()
    return ""


def _probe_pool_on_chains(pool_id: str, chains: list[str]) -> tuple[dict | None, str, str]:
    for chain in chains:
        endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            continue
        try:
            p = _query_pool_by_id(endpoint, pool_id)
        except Exception:
            p = None
        if p:
            return p, chain, endpoint
    return None, "", ""


def _query_pool_by_id(endpoint: str, pool_id: str) -> dict | None:
    q = """
    query PoolById($id: String!) {
      pool(id: $id) {
        id
        feeTier
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        volumeUSD
        feesUSD
      }
    }
    """
    pid = str(pool_id).lower()
    try:
        data = graphql_query(endpoint, q, {"id": pid}, retries=1)
        pool = (data.get("data") or {}).get("pool")
        if isinstance(pool, dict) and str(pool.get("id") or ""):
            return pool
    except Exception:
        pass
    # Fallback for indexers where singular entity resolver is flaky.
    q2 = """
    query PoolByWhere($id: String!) {
      pools(where: { id: $id }, first: 1) {
        id
        feeTier
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        volumeUSD
        feesUSD
      }
    }
    """
    try:
        data2 = graphql_query(endpoint, q2, {"id": pid}, retries=1)
        rows = (data2.get("data") or {}).get("pools") or []
        if isinstance(rows, list) and rows:
            p = rows[0]
            if isinstance(p, dict) and str(p.get("id") or ""):
                return p
    except Exception:
        pass
    return None


def _strict_unavailable_payload(pool_id: str, reason: str, chain: str = "") -> dict:
    return {
        "fees": [],
        "tvl": [],
        "pool_id": str(pool_id or ""),
        "fee_pct": 0.0,
        "raw_fee_tier": 0,
        "pool_tvl_now_usd": 0.0,
        "pool_tvl_subgraph_usd": 0.0,
        "tvl_multiplier": 1.0,
        "tvl_price_source": "",
        "pair": "target_pool",
        "chain": str(chain or ""),
        "version": "v3",
        "data_quality": "strict_unavailable",
        "data_quality_reason": str(reason or "strict_required:unknown"),
        "strict_compare_estimated_tvl": [],
        "strict_compare_estimated_fees": [],
        "strict_compare_exact_tvl": [],
        "strict_compare_exact_fees": [],
    }


def _compute_fee_and_raw_tvl(pool_id: str, endpoint: str) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    start_ts, end_ts = build_exact_day_window(int(FEE_DAYS))
    day_data = query_pool_day_data(endpoint, pool_id, start_ts, end_ts)
    fees_usd: list[tuple[int, float]] = []
    raw_tvl: list[tuple[int, float]] = []
    for d in day_data:
        ts = int(d.get("date") or 0)
        if ts <= 0:
            continue
        f = float(d.get("feesUSD") or 0.0)
        fees_usd.append((ts, max(0.0, f)))
        rv = float(d.get("tvlUSD") or 0.0)
        raw_tvl.append((ts, max(0.0, rv)))
    return fees_usd, raw_tvl


def _build_estimated_tvl(fees_usd: list[tuple[int, float]], raw_tvl: list[tuple[int, float]], pool_tvl_now_usd: float) -> list[tuple[int, float]]:
    tvl_series = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]
    if raw_tvl and len(raw_tvl) == len(fees_usd):
        anchor = 0.0
        for _, rv in reversed(raw_tvl):
            if float(rv) > 0:
                anchor = float(rv)
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
    return tvl_series


def _rebuild_fees_cumulative(fees_usd: list[tuple[int, float]], tvl_series: list[tuple[int, float]]) -> list[tuple[int, float]]:
    cumul = 0.0
    out: list[tuple[int, float]] = []
    for i, (ts, fees_day) in enumerate(fees_usd):
        tvl_day = float(tvl_series[i][1] or 0.0) if i < len(tvl_series) else 0.0
        if float(fees_day) > 0 and tvl_day > 0:
            cumul += float(fees_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day))
        out.append((int(ts), float(cumul)))
    return out


def _normalize_exact_series_to_fees(
    fees_usd: list[tuple[int, float]],
    exact_series: list[tuple[int, float]],
) -> list[tuple[int, float]]:
    by_ts: dict[int, float] = {}
    for ts, v in (exact_series or []):
        by_ts[int(ts)] = float(v or 0.0)
    return [(int(ts), float(by_ts.get(int(ts), 0.0))) for ts, _ in (fees_usd or [])]


def _blend_exact_with_estimated(
    exact_series: list[tuple[int, float]],
    estimated_series: list[tuple[int, float]],
) -> tuple[list[tuple[int, float]], int]:
    out: list[tuple[int, float]] = []
    filled = 0
    n = min(len(exact_series or []), len(estimated_series or []))
    for i in range(n):
        ts = int(exact_series[i][0])
        ex_v = float(exact_series[i][1] or 0.0)
        if ex_v > 0:
            out.append((ts, ex_v))
            continue
        est_v = float(estimated_series[i][1] or 0.0)
        if est_v > 0:
            out.append((ts, est_v))
            filled += 1
        else:
            out.append((ts, 0.0))
    return out, filled


def _median(values: list[float]) -> float:
    arr = sorted(float(x) for x in (values or []) if float(x) > 0.0)
    if not arr:
        return 0.0
    n = len(arr)
    mid = n // 2
    if (n % 2) == 1:
        return float(arr[mid])
    return float((arr[mid - 1] + arr[mid]) / 2.0)


def _series_scale_ratio(
    exact_series: list[tuple[int, float]],
    estimated_series: list[tuple[int, float]],
) -> float:
    n = min(len(exact_series or []), len(estimated_series or []))
    ratios: list[float] = []
    for i in range(n):
        ex_v = float(exact_series[i][1] or 0.0)
        est_v = float(estimated_series[i][1] or 0.0)
        if ex_v > 0.0 and est_v > 0.0:
            ratios.append(float(ex_v / est_v))
    return _median(ratios)


def _series_clone_ratio(
    exact_series: list[tuple[int, float]],
    estimated_series: list[tuple[int, float]],
    *,
    abs_eps: float = 0.01,
) -> float:
    n = min(len(exact_series or []), len(estimated_series or []))
    if n <= 0:
        return 0.0
    same = 0
    used = 0
    for i in range(n):
        ex_v = float(exact_series[i][1] or 0.0)
        est_v = float(estimated_series[i][1] or 0.0)
        if ex_v <= 0.0 or est_v <= 0.0:
            continue
        used += 1
        if abs(ex_v - est_v) <= float(abs_eps):
            same += 1
    if used <= 0:
        return 0.0
    return float(same / used)


def _series_tail_median(series: list[tuple[int, float]], tail_n: int = 5) -> float:
    arr = [float(v or 0.0) for _ts, v in (series or []) if float(v or 0.0) > 0.0]
    if not arr:
        return 0.0
    n = max(1, int(tail_n))
    return _median(arr[-n:])


def _append_now_tvl_anchor(tvl_series: list[tuple[int, float]], now_tvl_usd: float) -> list[tuple[int, float]]:
    out = list(tvl_series or [])
    now_ts = int(datetime.utcnow().timestamp())
    now_tvl = float(max(0.0, now_tvl_usd))
    if out and int(out[-1][0]) >= now_ts - 3600:
        out[-1] = (int(out[-1][0]), now_tvl)
    else:
        out.append((now_ts, now_tvl))
    return out


def _append_now_fee_anchor(fee_series: list[tuple[int, float]]) -> list[tuple[int, float]]:
    out = list(fee_series or [])
    now_ts = int(datetime.utcnow().timestamp())
    last_val = float(out[-1][1]) if out else 0.0
    if out and int(out[-1][0]) >= now_ts - 3600:
        out[-1] = (int(out[-1][0]), last_val)
    else:
        out.append((now_ts, last_val))
    return out


def _addr_topic(addr: str) -> str:
    clean = str(addr or "").strip().lower().replace("0x", "")
    return "0x" + clean.rjust(64, "0")


def _safe_hex_to_int(v: Any) -> int:
    s = str(v or "0x0").strip().lower()
    if not s.startswith("0x"):
        return 0
    try:
        return int(s, 16)
    except Exception:
        return 0


def _alchemy_api_key() -> str:
    for k in ("ALCHEMY_TRANSFERS_API_KEY", "ALCHEMY_API_KEY", "ALCHEMY_KEY"):
        v = str(os.environ.get(k, "") or "").strip()
        if v:
            return v
    return ""


def _alchemy_url_for_chain(chain: str) -> str:
    ck = str(chain or "").strip().lower()
    key = _alchemy_api_key()
    if not key:
        return ""
    net = {
        "ethereum": "eth-mainnet",
        "arbitrum": "arb-mainnet",
        "optimism": "opt-mainnet",
        "base": "base-mainnet",
        "polygon": "polygon-mainnet",
    }.get(ck, "")
    if not net:
        return ""
    return f"https://{net}.g.alchemy.com/v2/{key}"


def _alchemy_fetch_transfer_deltas(
    *,
    chain: str,
    token: str,
    pool_id: str,
    from_block: int,
    to_block: int,
    deadline_ts: float | None = None,
) -> dict[int, int]:
    url = _alchemy_url_for_chain(chain)
    if not url:
        raise RuntimeError("alchemy_not_configured_for_chain")
    try:
        req_timeout = max(1.5, min(8.0, float(os.environ.get("STRICT_ALCHEMY_REQ_TIMEOUT_SEC", "4.0"))))
    except Exception:
        req_timeout = 6.0
    try:
        max_pages = max(10, int(os.environ.get("STRICT_ALCHEMY_MAX_PAGES", "60")))
    except Exception:
        max_pages = 120

    by_block: dict[int, int] = {}
    t0 = time.monotonic()
    for direction, sign in (("from", -1), ("to", 1)):
        page_key = ""
        pages_used = 0
        items_used = 0
        seen_keys: set[str] = set()
        dir_t0 = time.monotonic()
        while True:
            if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
                raise TimeoutError("exact2_budget_timeout:alchemy_transfers")
            if pages_used >= int(max_pages):
                raise TimeoutError("exact2_budget_timeout:alchemy_page_cap")
            left = float(req_timeout)
            if deadline_ts is not None:
                left = max(0.5, min(float(req_timeout), float(deadline_ts) - time.monotonic()))
                if left <= 0.55:
                    raise TimeoutError("exact2_budget_timeout:alchemy_transfers")
            params: dict[str, Any] = {
                "fromBlock": hex(int(from_block)),
                "toBlock": hex(int(to_block)),
                "contractAddresses": [str(token).strip().lower()],
                "category": ["erc20"],
                "excludeZeroValue": True,
                "withMetadata": False,
                "maxCount": "0x3e8",
            }
            if direction == "to":
                params["toAddress"] = str(pool_id).strip().lower()
            else:
                params["fromAddress"] = str(pool_id).strip().lower()
            if page_key:
                if page_key in seen_keys:
                    raise RuntimeError("alchemy_pagekey_loop")
                seen_keys.add(page_key)
                params["pageKey"] = page_key
            body = {"jsonrpc": "2.0", "id": 1, "method": "alchemy_getAssetTransfers", "params": [params]}
            r = requests.post(url, json=body, timeout=(2.0, float(left)))
            r.raise_for_status()
            payload = r.json()
            data = payload if isinstance(payload, dict) else {}
            if data.get("error"):
                raise RuntimeError(str(data.get("error")))
            res = data.get("result") or {}
            items = res.get("transfers") or []
            if isinstance(items, list):
                items_used += int(len(items))
                for tr in items:
                    if not isinstance(tr, dict):
                        continue
                    bn = _safe_hex_to_int(tr.get("blockNum"))
                    if bn <= 0:
                        continue
                    raw = ((tr.get("rawContract") or {}).get("value"))
                    val = _safe_hex_to_int(raw)
                    if val <= 0:
                        continue
                    by_block[bn] = int(by_block.get(bn, 0)) + int(sign * val)
            page_key = str(res.get("pageKey") or "").strip()
            pages_used += 1
            if _strict_debug_enabled() and pages_used % 25 == 0:
                _strict_debug(
                    f"alchemy dir={direction} token={str(token)[:10]}.. pages={pages_used} items={items_used} "
                    f"elapsed={time.monotonic() - dir_t0:.1f}s"
                )
            if not page_key:
                break
        _strict_debug(
            f"alchemy dir={direction} token={str(token)[:10]}.. done pages={pages_used} items={items_used} "
            f"elapsed={time.monotonic() - dir_t0:.1f}s"
        )
    _strict_debug(f"alchemy token={str(token)[:10]}.. total elapsed={time.monotonic() - t0:.1f}s blocks={len(by_block)}")
    return by_block


def _fetch_logs_adaptive(
    chain_id: int,
    token: str,
    topics: list[Any],
    from_block: int,
    to_block: int,
    deadline_ts: float | None = None,
    on_log: Any | None = None,
) -> tuple[list[dict], int, bool]:
    start = int(max(1, from_block))
    end = int(max(start, to_block))
    logs: list[dict] = []
    step = int(max(2000, int(os.environ.get("V3_EXACT2_LOG_BLOCK_STEP", "50000"))))
    min_step = int(max(200, int(os.environ.get("V3_EXACT2_LOG_MIN_STEP", "1000"))))
    # Scan backwards: when budget is tight, we preserve most recent blocks first.
    cursor = end
    timed_out = False
    while cursor >= start:
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            timed_out = True
            break
        lo = max(start, cursor - step + 1)
        params = [{
            "address": str(token).strip().lower(),
            "fromBlock": hex(int(lo)),
            "toBlock": hex(int(cursor)),
            "topics": topics,
        }]
        try:
            data = _rpc_json(int(chain_id), "eth_getLogs", params, timeout_sec=22.0)
            part = data.get("result") or []
            if isinstance(part, list):
                if callable(on_log):
                    for x in part:
                        if isinstance(x, dict):
                            on_log(x)
                else:
                    logs.extend([x for x in part if isinstance(x, dict)])
            cursor = lo - 1
            if len(part) < 500 and step < 200000:
                step = int(step * 1.2)
        except Exception:
            if step <= min_step:
                raise
            step = max(min_step, step // 2)
    covered_from = int(max(start, cursor + 1))
    return logs, covered_from, timed_out


def _collect_pool_transfer_deltas(
    chain_id: int,
    chain: str,
    token: str,
    pool_id: str,
    from_block: int,
    to_block: int,
    deadline_ts: float | None = None,
) -> tuple[dict[int, int], int, bool]:
    out = _alchemy_fetch_transfer_deltas(
        chain=str(chain),
        token=str(token),
        pool_id=str(pool_id),
        from_block=int(from_block),
        to_block=int(to_block),
        deadline_ts=deadline_ts,
    )
    return out, int(from_block), False


def _latest_block_number(chain_id: int) -> int:
    d = _rpc_json(int(chain_id), "eth_blockNumber", [], timeout_sec=8.0)
    b = _safe_hex_to_int(d.get("result"))
    if b <= 0:
        raise RuntimeError("latest_block_not_found")
    return int(b)


def _balances_by_boundaries_desc(latest_balance: int, deltas_by_block: dict[int, int], boundaries_desc: list[int]) -> dict[int, int]:
    blocks_desc = sorted((int(b) for b in deltas_by_block.keys() if int(b) > 0), reverse=True)
    out: dict[int, int] = {}
    idx = 0
    future_delta = 0
    for boundary in boundaries_desc:
        bnd = int(boundary)
        while idx < len(blocks_desc) and int(blocks_desc[idx]) > bnd:
            future_delta += int(deltas_by_block.get(int(blocks_desc[idx]), 0))
            idx += 1
        out[bnd] = int(max(0, int(latest_balance) - int(future_delta)))
    return out


def _exact2_cache_conn() -> sqlite3.Connection:
    p = str(_EXACT2_CACHE_DB_PATH or "").strip()
    os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
    conn = sqlite3.connect(p)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS exact2_daily_tvl (
            chain TEXT NOT NULL,
            pool_id TEXT NOT NULL,
            day_ts INTEGER NOT NULL,
            tvl_usd REAL NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY(chain, pool_id, day_ts)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_exact2_daily_tvl_lookup ON exact2_daily_tvl(chain, pool_id, day_ts)"
    )
    return conn


def _exact2_cache_load(
    *,
    chain: str,
    pool_id: str,
    day_ts_list: list[int],
) -> dict[int, float]:
    days = sorted({int(x) for x in (day_ts_list or []) if int(x) > 0})
    if not days:
        return {}
    ck = str(chain or "").strip().lower()
    pid = str(pool_id or "").strip().lower()
    if not (ck and pid):
        return {}
    qmarks = ",".join(["?"] * len(days))
    sql = (
        "SELECT day_ts, tvl_usd FROM exact2_daily_tvl "
        f"WHERE chain=? AND pool_id=? AND day_ts IN ({qmarks})"
    )
    try:
        with _exact2_cache_conn() as conn:
            rows = conn.execute(sql, (ck, pid, *days)).fetchall()
    except Exception:
        return {}
    out: dict[int, float] = {}
    for r in rows:
        try:
            d = int(r[0])
            v = float(r[1] or 0.0)
        except Exception:
            continue
        if d > 0:
            out[d] = max(0.0, v)
    return out


def _exact2_cache_upsert(
    *,
    chain: str,
    pool_id: str,
    series: list[tuple[int, float]],
) -> None:
    ck = str(chain or "").strip().lower()
    pid = str(pool_id or "").strip().lower()
    if not (ck and pid):
        return
    now_i = int(time.time())
    rows: list[tuple[str, str, int, float, int]] = []
    for ts, tvl in (series or []):
        day_ts = int((int(ts) // 86400) * 86400)
        try:
            v = float(tvl or 0.0)
        except Exception:
            v = 0.0
        if day_ts <= 0:
            continue
        rows.append((ck, pid, day_ts, max(0.0, v), now_i))
    if not rows:
        return
    try:
        with _exact2_cache_conn() as conn:
            conn.executemany(
                """
                INSERT INTO exact2_daily_tvl(chain, pool_id, day_ts, tvl_usd, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chain, pool_id, day_ts) DO UPDATE SET
                    tvl_usd=excluded.tvl_usd,
                    updated_at=excluded.updated_at
                """,
                rows,
            )
            conn.commit()
    except Exception:
        return


def _strict_exact2_source() -> str:
    """Select exact2 source: rpc (default), goldsky, auto."""
    src = str(os.environ.get("STRICT_EXACT2_SOURCE", "rpc") or "").strip().lower()
    if src in {"goldsky", "auto", "rpc"}:
        return src
    return "rpc"


def _strict_exact2_balance_mode() -> str:
    v = str(os.environ.get("STRICT_EXACT2_BALANCE_MODE", "snapshot") or "").strip().lower()
    return "transfer" if v == "transfer" else "snapshot"


def _goldsky_graphql_url() -> str:
    return str(os.environ.get("GOLDSKY_GRAPHQL_URL", "") or "").strip()


def _goldsky_headers() -> dict[str, str]:
    h: dict[str, str] = {"Content-Type": "application/json"}
    api_key = str(
        os.environ.get("GOLDSKY_API_KEY")
        or os.environ.get("GOLDSKY_KEY")
        or os.environ.get("GOLDSKY_TOKEN")
        or ""
    ).strip()
    if api_key:
        # Some endpoints expect x-api-key, some accept bearer token.
        h["x-api-key"] = api_key
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _goldsky_fetch_exact2_tvl_series(
    *,
    pool_id: str,
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str, float]:
    """
    Read precomputed daily TVL from Goldsky GraphQL.
    Expects Uniswap-like pool day entities (poolDayDatas / poolDayData).
    """
    url = _goldsky_graphql_url()
    if not url:
        return [], "exact2_goldsky_not_configured:url_missing", 0.0
    if not fees_usd:
        return [], "exact2_goldsky_no_fee_days", 0.0

    timeout_total = max(6.0, float(min(25.0, budget_sec)))
    timeout_tuple = (3.0, timeout_total)
    p = str(pool_id or "").strip().lower()
    ts_from = int(day_start_ts)
    ts_to = int(day_end_ts + 86399)

    # Try common schema variants. Unknown fields are normal across deployments.
    attempts: list[tuple[str, str]] = [
        (
            "poolDayDatas",
            """
            query Q($pool:String!, $from:Int!, $to:Int!) {
              poolDayDatas(
                where: { pool: $pool, date_gte: $from, date_lte: $to }
                orderBy: date
                orderDirection: asc
                first: 2000
              ) {
                date
                tvlUSD
                totalValueLockedUSD
              }
            }
            """,
        ),
        (
            "poolDayData",
            """
            query Q($pool:String!, $from:Int!, $to:Int!) {
              poolDayData(
                where: { pool: $pool, date_gte: $from, date_lte: $to }
                orderBy: date
                orderDirection: asc
                first: 2000
              ) {
                date
                tvlUSD
                totalValueLockedUSD
              }
            }
            """,
        ),
        (
            "poolDayDatas",
            """
            query Q($pool:String!, $from:Int!, $to:Int!) {
              poolDayDatas(
                where: { pool: $pool, dayStartTimestamp_gte: $from, dayStartTimestamp_lte: $to }
                orderBy: dayStartTimestamp
                orderDirection: asc
                first: 2000
              ) {
                dayStartTimestamp
                tvlUSD
                totalValueLockedUSD
              }
            }
            """,
        ),
    ]

    rows: list[dict[str, Any]] = []
    last_err = ""
    for key, query in attempts:
        try:
            body = {
                "query": query,
                "variables": {"pool": p, "from": int(ts_from), "to": int(ts_to)},
            }
            r = requests.post(url, json=body, headers=_goldsky_headers(), timeout=timeout_tuple)
            r.raise_for_status()
            payload = r.json() if isinstance(r.json(), dict) else {}
            errs = payload.get("errors") or []
            if errs:
                msg = str(errs[0]) if isinstance(errs, list) and errs else str(errs)
                last_err = f"{key}:{msg}"[:180]
                continue
            data = payload.get("data") or {}
            got = data.get(key) or []
            if isinstance(got, list) and got:
                rows = [x for x in got if isinstance(x, dict)]
                break
        except Exception as e:
            last_err = f"{key}:{e}"[:180]
            continue

    if not rows:
        return [], f"exact2_goldsky_no_rows:{last_err or 'empty'}", 0.0

    by_day_tvl: dict[int, float] = {}
    for r in rows:
        raw_day = r.get("date")
        if raw_day is None:
            raw_day = r.get("dayStartTimestamp")
        try:
            d = int(raw_day)
        except Exception:
            continue
        day_ts = int((int(d) // 86400) * 86400)
        tvl_raw = r.get("tvlUSD")
        if tvl_raw is None:
            tvl_raw = r.get("totalValueLockedUSD")
        try:
            tvl = float(tvl_raw or 0.0)
        except Exception:
            tvl = 0.0
        if tvl < 0:
            tvl = 0.0
        by_day_tvl[day_ts] = float(tvl)

    out: list[tuple[int, float]] = []
    non_zero = 0
    for ts, _ in fees_usd:
        day_ts = int((int(ts) // 86400) * 86400)
        v = float(by_day_tvl.get(day_ts, 0.0))
        if v > 0:
            non_zero += 1
        out.append((int(ts), v))
    cov = float(non_zero / max(1, len(fees_usd)))
    if non_zero == 0:
        return out, "exact2_goldsky_non_positive_days", cov
    if non_zero < len(fees_usd):
        return out, f"exact2_goldsky_partial:{non_zero}/{len(fees_usd)}", cov
    return out, "ok", cov


def _build_exact2_tvl_series_v3_ledger(
    *,
    chain: str,
    pool: dict,
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str, float]:
    run_t0 = time.monotonic()
    t_blocks_elapsed = 0.0
    t_transfers_elapsed = 0.0
    t_pricing_elapsed = 0.0

    def _reason_with_timing(reason: str) -> str:
        return (
            f"{str(reason or '').strip()}"
            f":dbg=t_blocks={t_blocks_elapsed:.1f},"
            f"t_alchemy={t_transfers_elapsed:.1f},"
            f"t_pricing={t_pricing_elapsed:.1f},"
            f"t_total={max(0.0, time.monotonic() - run_t0):.1f}"
        )

    pool_id = str(pool.get("id") or "").strip().lower()
    day_keys = [int((int(ts) // 86400) * 86400) for ts, _ in (fees_usd or []) if int(ts) > 0]
    use_cache = str(os.environ.get("STRICT_EXACT2_USE_CACHE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    cached_by_day: dict[int, float] = {}
    if use_cache:
        cached_by_day = _exact2_cache_load(chain=str(chain), pool_id=pool_id, day_ts_list=day_keys)
        if day_keys and all(int(d) in cached_by_day for d in day_keys):
            hit_series = [(int(ts), float(cached_by_day.get(int((int(ts) // 86400) * 86400), 0.0))) for ts, _ in fees_usd]
            hit_cov = float(sum(1 for _ts, v in hit_series if float(v) > 0.0) / max(1, len(hit_series)))
            return hit_series, _reason_with_timing("exact2_cache:ok"), hit_cov

    # Strict path is deterministic: only RPC snapshot source.

    total_budget = max(10.0, float(budget_sec))
    # Snapshot mode: reserve time for price valuation after balance snapshots.
    try:
        pricing_share = max(0.35, min(0.95, float(os.environ.get("STRICT_EXACT2_PRICING_BUDGET_SHARE", "0.75"))))
    except Exception:
        pricing_share = 0.75
    pricing_budget = min(max(20.0, total_budget * pricing_share), max(10.0, total_budget - 10.0))
    balance_budget = max(10.0, total_budget - pricing_budget)
    deadline_balance = time.monotonic() + balance_budget
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return [], _reason_with_timing("exact2_unsupported_chain"), 0.0
    rpc_source = "alchemy" if any("g.alchemy.com" in str(u or "").lower() for u in _rpc_urls(int(chain_id))) else "public"
    rpc_host = ""
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    token0_symbol = str(((pool.get("token0") or {}).get("symbol") or "")).strip()
    token1_symbol = str(((pool.get("token1") or {}).get("symbol") or "")).strip()
    if not (pool_id and token0 and token1):
        return [], "exact2_missing_pool_tokens", 0.0

    try:
        latest_block = _latest_block_number(chain_id)
        rpc_host = str(_rpc_primary_url(chain_id) or "").strip().lower()
        if rpc_host:
            rpc_source = ("alchemy" if "g.alchemy.com" in rpc_host else "public")
        dec0 = int(_token_decimals_from_pool(pool, "token0") or _erc20_decimals(chain_id, token0))
        dec1 = int(_token_decimals_from_pool(pool, "token1") or _erc20_decimals(chain_id, token1))
        h0 = _symbol_decimals_hint(token0_symbol)
        h1 = _symbol_decimals_hint(token1_symbol)
        if dec0 == 18 and h0 in {6, 8}:
            dec0 = int(h0)
        if dec1 == 18 and h1 in {6, 8}:
            dec1 = int(h1)
    except Exception as e:
        return [], _reason_with_timing(f"exact2_latest_point_failed:{e}"), 0.0

    day_blocks: dict[int, int] = {}
    block_missing = 0
    t_blocks = time.monotonic()
    for ts, _ in fees_usd:
        if time.monotonic() >= deadline_balance:
            t_blocks_elapsed = max(0.0, time.monotonic() - t_blocks)
            return [], _reason_with_timing("exact2_budget_timeout:block_resolution"), 0.0
        day_ts = int((int(ts) // 86400) * 86400)
        if day_ts in day_blocks:
            continue
        try:
            day_blocks[day_ts] = int(_llama_block_for_day(ck, day_ts + 86399))
        except Exception:
            block_missing += 1
            day_blocks[day_ts] = 0
    t_blocks_elapsed = max(0.0, time.monotonic() - t_blocks)
    _strict_debug(
        f"block_resolution done days={len(day_blocks)} missing={block_missing} elapsed={time.monotonic() - t_blocks:.1f}s"
    )

    valid_blocks = [int(b) for b in day_blocks.values() if int(b) > 0]
    if not valid_blocks:
        return [], _reason_with_timing("exact2_no_day_blocks"), 0.0
    from_block = int(min(valid_blocks))
    to_block = int(latest_block)
    t_transfers = time.monotonic()
    try:
        deltas0, covered0_from, timeout0 = _collect_pool_transfer_deltas(
            chain_id, ck, token0, pool_id, from_block, to_block, deadline_ts=deadline_balance
        )
        deltas1, covered1_from, timeout1 = _collect_pool_transfer_deltas(
            chain_id, ck, token1, pool_id, from_block, to_block, deadline_ts=deadline_balance
        )
    except Exception as e:
        t_transfers_elapsed = max(0.0, time.monotonic() - t_transfers)
        return [], _reason_with_timing(f"exact2_alchemy_transfers_failed:{e}"), 0.0
    t_transfers_elapsed = max(0.0, time.monotonic() - t_transfers)
    _strict_debug(
        f"transfer_collection done blocks0={len(deltas0)} blocks1={len(deltas1)} "
        f"elapsed={time.monotonic() - t_transfers:.1f}s"
    )

    boundaries = sorted(set(valid_blocks), reverse=True)
    bal0_latest = int(_balance_of_raw(chain_id, token0, pool_id, latest_block, timeout_sec=6.0))
    bal1_latest = int(_balance_of_raw(chain_id, token1, pool_id, latest_block, timeout_sec=6.0))
    bal0_by_block = _balances_by_boundaries_desc(bal0_latest, deltas0, boundaries)
    bal1_by_block = _balances_by_boundaries_desc(bal1_latest, deltas1, boundaries)
    covered_from = int(max(int(from_block), int(covered0_from), int(covered1_from)))
    transfer_logs_timed_out = bool(timeout0 or timeout1)
    balance_mode = "alchemy"

    tvl_series: list[tuple[int, float]] = []
    non_zero = 0
    priced_days = 0
    priced_days_both = 0
    imputed_days = 0
    covered_days = 0
    snapshot_fail_days = 0
    one_leg_days_0 = 0
    one_leg_days_1 = 0
    ratio01 = float(pool.get("token0Price") or 0.0) if pool.get("token0Price") is not None else 0.0
    deadline_pricing = time.monotonic() + pricing_budget
    t_pricing = time.monotonic()
    for idx, (ts, _fees) in enumerate(fees_usd):
        if time.monotonic() >= deadline_pricing:
            tvl_series.append((int(ts), 0.0))
            for j in range(idx + 1, len(fees_usd)):
                tvl_series.append((int(fees_usd[j][0]), 0.0))
            processed_cov = float(sum(1 for _ts, v in tvl_series if float(v) > 0.0) / max(1, len(fees_usd)))
            t_pricing_elapsed = max(0.0, time.monotonic() - t_pricing)
            _strict_debug(
                f"pricing timeout processed={idx+1}/{len(fees_usd)} elapsed={time.monotonic() - t_pricing:.1f}s "
                f"total_elapsed={time.monotonic() - run_t0:.1f}s"
            )
            return tvl_series, _reason_with_timing(f"exact2_budget_timeout:pricing:mode={balance_mode}"), processed_cov
        day_ts = int((int(ts) // 86400) * 86400)
        b = int(day_blocks.get(day_ts, 0))
        if b <= 0 or int(b) < int(covered_from):
            tvl_series.append((int(ts), 0.0))
            continue
        amt0 = float(bal0_by_block.get(int(b), 0)) / float(10 ** max(0, dec0))
        amt1 = float(bal1_by_block.get(int(b), 0)) / float(10 ** max(0, dec1))
        if amt0 <= 0.0 and amt1 <= 0.0:
            tvl_series.append((int(ts), 0.0))
            continue
        if amt0 <= 0.0 and amt1 > 0.0:
            one_leg_days_0 += 1
        elif amt1 <= 0.0 and amt0 > 0.0:
            one_leg_days_1 += 1
        covered_days += 1
        p0 = float(_historical_price_usd(ck, token0, int(ts), int(day_start_ts), int(day_end_ts)))
        p1 = float(_historical_price_usd(ck, token1, int(ts), int(day_start_ts), int(day_end_ts)))
        # Keep historical valuation rule aligned with TVL-now rule:
        # if one leg price is missing, infer from pool token ratio.
        if p0 <= 0 and p1 > 0 and ratio01 > 0:
            p0 = p1 * ratio01
            imputed_days += 1
        if p1 <= 0 and p0 > 0 and ratio01 > 0:
            p1 = p0 / ratio01
            imputed_days += 1
        if p0 > 0 or p1 > 0:
            priced_days += 1
        if p0 > 0 and p1 > 0:
            priced_days_both += 1
        tvl = max(0.0, amt0 * max(0.0, p0)) + max(0.0, amt1 * max(0.0, p1))
        if tvl > 0:
            non_zero += 1
        tvl_series.append((int(ts), float(tvl)))
    t_pricing_elapsed = max(0.0, time.monotonic() - t_pricing)
    _strict_debug(
        f"pricing done processed={len(fees_usd)}/{len(fees_usd)} non_zero={non_zero} "
        f"elapsed={time.monotonic() - t_pricing:.1f}s total_elapsed={time.monotonic() - run_t0:.1f}s"
    )

    if tvl_series and cached_by_day:
        rpc_by_day = {
            int((int(ts) // 86400) * 86400): float(v)
            for ts, v in tvl_series
            if int(ts) > 0
        }
        tvl_series = [
            (
                int(ts),
                float((
                    rpc_by_day.get(int((int(ts) // 86400) * 86400), 0.0)
                    if float(rpc_by_day.get(int((int(ts) // 86400) * 86400), 0.0)) > 0.0
                    else cached_by_day.get(int((int(ts) // 86400) * 86400), 0.0)
                )),
            )
            for ts, _ in fees_usd
        ]
        non_zero = sum(1 for _ts, v in tvl_series if float(v) > 0.0)
    if not transfer_logs_timed_out and covered_days > 0:
        _exact2_cache_upsert(chain=str(chain), pool_id=pool_id, series=tvl_series)
    # Coverage for strict quality gates must reflect usable exact TVL points,
    # not just successful quantity snapshot reads.
    cov = float(non_zero / max(1, len(fees_usd)))
    qty_cov = float(covered_days / max(1, len(fees_usd)))
    dominant_one_leg = max(int(one_leg_days_0), int(one_leg_days_1))
    one_leg_ratio = float(dominant_one_leg / max(1, covered_days))
    if non_zero == 0:
        return (
            tvl_series,
            _reason_with_timing(
                f"exact2_non_positive_days:mode={balance_mode}:block_missing={block_missing}:"
                f"priced_days={priced_days}:qty_cov={qty_cov:.2f}:snapshot_fail_days={snapshot_fail_days}:rpc={rpc_source}"
            ),
            cov,
        )
    try:
        one_leg_max_ratio = max(0.5, min(1.0, float(os.environ.get("STRICT_EXACT2_ONE_LEG_MAX_RATIO", "0.95"))))
    except Exception:
        one_leg_max_ratio = 0.95
    if covered_days >= max(3, len(fees_usd) // 2) and one_leg_ratio >= float(one_leg_max_ratio):
        return (
            tvl_series,
            _reason_with_timing(
                f"exact2_partial:one_leg_quantity_collapse:mode={balance_mode}:one_leg_ratio={one_leg_ratio:.2f}:"
                f"leg0={one_leg_days_0}:leg1={one_leg_days_1}:covered_days={covered_days}:qty_cov={qty_cov:.2f}:rpc={rpc_source}"
            ),
            cov,
        )
    if transfer_logs_timed_out:
        return (
            tvl_series,
            _reason_with_timing(
                f"exact2_partial:transfer_logs_timeout:mode={balance_mode}:covered_days={covered_days}/{len(fees_usd)}:"
                f"priced_both={priced_days_both}:imputed={imputed_days}:qty_cov={qty_cov:.2f}:rpc={rpc_source}"
            ),
            cov,
        )
    if block_missing > 0:
        return (
            tvl_series,
            _reason_with_timing(
                f"exact2_partial:block_missing={block_missing}:mode={balance_mode}:priced_days={priced_days}:"
                f"priced_both={priced_days_both}:imputed={imputed_days}:qty_cov={qty_cov:.2f}:snapshot_fail_days={snapshot_fail_days}:rpc={rpc_source}"
            ),
            cov,
        )
    if priced_days < max(1, len(fees_usd) // 2):
        return (
            tvl_series,
            _reason_with_timing(
                f"exact2_partial:insufficient_prices={priced_days}:mode={balance_mode}:qty_cov={qty_cov:.2f}:snapshot_fail_days={snapshot_fail_days}:rpc={rpc_source}"
            ),
            cov,
        )
    return (
        tvl_series,
        _reason_with_timing(
            f"ok:mode={balance_mode}:qty_cov={qty_cov:.2f}:snapshot_fail_days={snapshot_fail_days}:"
            f"rpc={rpc_source}:dec0={dec0}:dec1={dec1}:rpc_host={rpc_host}"
        ),
        cov,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Uniswap v3 strict exact single-pool agent")
    ap.add_argument("--min-tvl", type=float, default=None, help="Min TVL USD")
    args = ap.parse_args()

    target_pool = _target_pool_id()
    token_pairs = str(os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS))
    suffix = pairs_to_filename_suffix(token_pairs)
    out_path = f"data/pools_v3_{suffix}.json"
    os.makedirs("data", exist_ok=True)

    if not target_pool:
        print("TARGET_POOL_ID is required for strict agent")
        save_chart_data_json({"strict_target": _strict_unavailable_payload("", "strict_required:missing_target_pool_id")}, out_path)
        return
    os.environ["V3_EXACT_TVL_ENABLE"] = "1"
    os.environ["V3_EXACT_TVL_STRICT_MODE"] = "1"

    include_chains = _include_chains()
    print("Uniswap v3 strict agent")
    print(f"Target pool: {target_pool}")
    cached_chain = _pool_chain_cache_get(target_pool)
    rpc_detected_chain = ""
    probe_chains = list(include_chains)
    if cached_chain:
        probe_chains = [cached_chain] + [c for c in probe_chains if c != cached_chain]
        print(f"Chain cache hit: {cached_chain}")
    else:
        rpc_detected_chain = _detect_chain_by_rpc(target_pool, probe_chains)
        if rpc_detected_chain:
            _pool_chain_cache_put(target_pool, rpc_detected_chain)
            probe_chains = [rpc_detected_chain] + [c for c in probe_chains if c != rpc_detected_chain]
            print(f"RPC chain detected: {rpc_detected_chain}")
    print("Probing chains:", ",".join(probe_chains))

    found_pool, found_chain, found_endpoint = _probe_pool_on_chains(target_pool, probe_chains)

    if not found_pool:
        print("Pool not found on selected chains")
        save_chart_data_json({target_pool: _strict_unavailable_payload(target_pool, "strict_required:pool_not_found_on_selected_chains")}, out_path)
        return
    _pool_chain_cache_put(target_pool, found_chain)

    pool = dict(found_pool)
    pool["chain"] = found_chain
    pool["version"] = "v3"
    t0 = str(((pool.get("token0") or {}).get("symbol") or "?"))
    t1 = str(((pool.get("token1") or {}).get("symbol") or "?"))
    pool["pair_label"] = f"{t0}/{t1}"

    pool_tvl_now_usd, price_source, price_err = resolve_pool_tvl_now_external(pool, found_chain, write_back=True)
    if float(pool_tvl_now_usd) <= 0:
        print(f"external TVL unavailable: {price_err or 'unknown'}")
        save_chart_data_json(
            {
                str(pool.get("id") or target_pool): _strict_unavailable_payload(
                    str(pool.get("id") or target_pool),
                    f"strict_required:external_tvl_unavailable:{price_err or 'unknown'}",
                    found_chain,
                )
            },
            out_path,
        )
        return
    pool["effectiveTvlUSD"] = float(pool_tvl_now_usd)
    pool["tvl_price_source"] = str(price_source or "external")

    fees_usd, raw_tvl = _compute_fee_and_raw_tvl(str(pool.get("id") or ""), found_endpoint)
    if not fees_usd:
        print("No day-level data for pool")
        save_chart_data_json(
            {
                str(pool.get("id") or target_pool): _strict_unavailable_payload(
                    str(pool.get("id") or target_pool),
                    "strict_required:no_day_data",
                    found_chain,
                )
            },
            out_path,
        )
        return

    estimated_tvl_base = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
    estimated_fees_base = _rebuild_fees_cumulative(fees_usd, estimated_tvl_base)
    estimated_tvl = _append_now_tvl_anchor(estimated_tvl_base, float(pool_tvl_now_usd))
    estimated_fees = _append_now_fee_anchor(estimated_fees_base)

    day_start_ts = int((int(fees_usd[0][0]) // 86400) * 86400)
    day_end_ts = int((int(fees_usd[-1][0]) // 86400) * 86400)
    print(f"exact2 source: {_strict_exact2_source()}")
    exact_series, exact_reason, exact_cov = _build_exact2_tvl_series_v3_ledger(
        chain=found_chain,
        pool=pool,
        fees_usd=fees_usd,
        day_start_ts=day_start_ts,
        day_end_ts=day_end_ts,
        budget_sec=float(os.environ.get("V3_EXACT_TVL_POOL_BUDGET_SEC", "180")),
    )

    try:
        partial_min_cov = max(0.0, min(1.0, float(os.environ.get("STRICT_EXACT2_PARTIAL_MIN_COVERAGE", "0.50"))))
    except Exception:
        partial_min_cov = 0.50

    exact_base = _normalize_exact_series_to_fees(fees_usd, list(exact_series or []))
    structural_cov = float(exact_cov)
    one_leg_conflict = bool(str(exact_reason).startswith("exact2_partial:one_leg_quantity_collapse"))
    scale_ratio = _series_scale_ratio(exact_base, estimated_tvl_base)
    try:
        max_scale_drift = max(1.05, float(os.environ.get("STRICT_EXACT2_MAX_SCALE_DRIFT", "1.8")))
    except Exception:
        max_scale_drift = 1.8
    scale_conflict = bool(
        scale_ratio > 0.0
        and float(exact_cov) >= float(partial_min_cov)
        and (float(scale_ratio) > float(max_scale_drift) or float(scale_ratio) < (1.0 / float(max_scale_drift)))
    )
    try:
        max_anchor_drift = max(1.05, float(os.environ.get("STRICT_EXACT2_MAX_ANCHOR_DRIFT", "1.8")))
    except Exception:
        max_anchor_drift = 1.8
    tail_med = _series_tail_median(exact_base, tail_n=5)
    whole_med = _series_tail_median(exact_base, tail_n=max(1, len(exact_base)))
    anchor_ratio = (float(pool_tvl_now_usd) / max(1e-12, float(tail_med))) if tail_med > 0 else 0.0
    whole_ratio = (float(pool_tvl_now_usd) / max(1e-12, float(whole_med))) if whole_med > 0 else 0.0
    anchor_conflict = bool(
        tail_med > 0.0
        and float(exact_cov) >= float(partial_min_cov)
        and (float(anchor_ratio) > float(max_anchor_drift) or float(anchor_ratio) < (1.0 / float(max_anchor_drift)))
    )
    level_conflict = bool(
        whole_med > 0.0
        and float(exact_cov) >= float(partial_min_cov)
        and (float(whole_ratio) > float(max_anchor_drift) or float(whole_ratio) < (1.0 / float(max_anchor_drift)))
    )
    partial_filled_days = 0
    try:
        max_clone_ratio = max(0.5, min(1.0, float(os.environ.get("STRICT_EXACT_MAX_CLONE_RATIO", "0.90"))))
    except Exception:
        max_clone_ratio = 0.90
    clone_ratio = _series_clone_ratio(exact_base, estimated_tvl_base, abs_eps=0.01)
    clone_conflict = bool(
        float(clone_ratio) >= float(max_clone_ratio)
        and float(exact_cov) >= float(partial_min_cov)
    )
    if clone_conflict:
        _warn_fallback_once(
            f"exact_clone_conflict:{target_pool}",
            f"exact series clones estimated to cents; clone_ratio={clone_ratio:.2f}",
        )

    exact_has_signal = bool(exact_base and any(float(v) > 0.0 for _ts, v in exact_base))
    partial_blended = False
    exact_effective = list(exact_base)
    if exact_base and len(exact_base) == len(estimated_tvl_base) and float(exact_cov) >= float(partial_min_cov):
        blended, filled = _blend_exact_with_estimated(exact_base, estimated_tvl_base)
        if filled > 0:
            exact_effective = list(blended)
            partial_filled_days = int(filled)
            partial_blended = True
    strict_exact_ok = bool(
        exact_effective
        and len(exact_effective) == len(fees_usd)
        and all(float(v) > 0.0 for _ts, v in exact_effective)
        and not one_leg_conflict
        and not scale_conflict
        and not anchor_conflict
        and not level_conflict
        and not clone_conflict
    )
    strict_exact_partial_ok = bool(
        exact_has_signal
        and len(exact_base) == len(fees_usd)
    )
    if strict_exact_ok:
        data_quality = ("exact_partial" if partial_blended else "exact")
        data_quality_reason = (
            f"exact:partial_blend:cov={exact_cov:.2f}:filled={partial_filled_days}"
            if partial_blended
            else "exact:ok"
        )
        final_tvl_base = list(exact_effective)
        final_fees_base = _rebuild_fees_cumulative(fees_usd, final_tvl_base)
        final_tvl = _append_now_tvl_anchor(final_tvl_base, float(pool_tvl_now_usd))
        final_fees = _append_now_fee_anchor(final_fees_base)
        strict_exact_tvl = list(final_tvl_base)
        strict_exact_fees = list(final_fees_base)
    elif strict_exact_partial_ok:
        data_quality = "exact_partial"
        missing_days = int(sum(1 for _ts, v in exact_base if float(v or 0.0) <= 0.0))
        flags: list[str] = []
        if one_leg_conflict:
            flags.append("one_leg")
        if scale_conflict:
            flags.append("scale")
        if anchor_conflict:
            flags.append("anchor")
        if level_conflict:
            flags.append("level")
        if clone_conflict:
            flags.append("clone")
        flg = f":conflicts={','.join(flags)}" if flags else ""
        data_quality_reason = f"exact:partial_raw:cov={exact_cov:.2f}:missing={missing_days}:reason={exact_reason}{flg}"
        final_tvl = []
        final_fees = []
        strict_exact_tvl_base = list(exact_base)
        strict_exact_fees_base = _rebuild_fees_cumulative(fees_usd, strict_exact_tvl_base)
        strict_exact_tvl = list(strict_exact_tvl_base)
        strict_exact_fees = list(strict_exact_fees_base)
    else:
        data_quality = "strict_unavailable"
        if scale_conflict:
            data_quality_reason = (
                f"strict_required:exact:source_conflict_scale:ratio={scale_ratio:.2f}:"
                f"cov={exact_cov:.2f}:scov={structural_cov:.2f}"
            )
        elif one_leg_conflict:
            data_quality_reason = f"strict_required:exact:{exact_reason}:cov={exact_cov:.2f}:scov={structural_cov:.2f}"
        elif anchor_conflict:
            data_quality_reason = (
                f"strict_required:exact:source_conflict_anchor_jump:anchor_ratio={anchor_ratio:.2f}:"
                f"tail_med={tail_med:.2f}:cov={exact_cov:.2f}:scov={structural_cov:.2f}"
            )
        elif level_conflict:
            data_quality_reason = (
                f"strict_required:exact:source_conflict_level_shift:whole_ratio={whole_ratio:.2f}:"
                f"whole_med={whole_med:.2f}:cov={exact_cov:.2f}:scov={structural_cov:.2f}"
            )
        elif clone_conflict:
            data_quality_reason = (
                f"strict_required:exact:source_conflict_clone:clone_ratio={clone_ratio:.2f}:"
                f"cov={exact_cov:.2f}:scov={structural_cov:.2f}"
            )
        else:
            data_quality_reason = f"strict_required:exact:{exact_reason}:{exact_cov:.2f}"
        _warn_fallback_once(
            f"strict_unavailable:{target_pool}:{data_quality_reason}",
            f"strict exact unavailable; fallback to compare-only output, reason={data_quality_reason}",
        )
        final_tvl = []
        final_fees = []
        if exact_base:
            strict_exact_tvl_base = list(exact_base)
            strict_exact_fees_base = _rebuild_fees_cumulative(fees_usd, strict_exact_tvl_base)
            strict_exact_tvl = list(strict_exact_tvl_base)
            strict_exact_fees = list(strict_exact_fees_base)
        else:
            # No exact points available for strict compare series.
            strict_exact_tvl = []
            strict_exact_fees = []

    payload = {
        "fees": final_fees,
        "tvl": final_tvl,
        "pool_id": str(pool.get("id") or ""),
        "fee_pct": int(pool.get("feeTier") or 0) / 10000.0,
        "raw_fee_tier": int(pool.get("feeTier") or 0),
        "pool_tvl_now_usd": float(pool_tvl_now_usd),
        "pool_tvl_subgraph_usd": float(pool.get("totalValueLockedUSD") or 0.0),
        "tvl_multiplier": 1.0,
        "tvl_price_source": str(pool.get("tvl_price_source") or ""),
        "pair": str(pool.get("pair_label") or ""),
        "chain": str(found_chain),
        "version": "v3",
        "data_quality": data_quality,
        "data_quality_reason": data_quality_reason,
        "strict_compare_estimated_tvl": estimated_tvl,
        "strict_compare_estimated_fees": estimated_fees,
        "strict_compare_exact_tvl": strict_exact_tvl,
        "strict_compare_exact_fees": strict_exact_fees,
    }

    save_chart_data_json({str(pool.get("id") or ""): payload}, out_path)
    print(f"Saved strict output: {out_path}")


if __name__ == "__main__":
    main()
