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
import math
import os
import sqlite3
import time
import threading
import requests
from datetime import datetime
from decimal import Decimal, getcontext
from typing import Any

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V3_SUBGRAPHS
from agent_common import build_exact_day_window, pairs_to_filename_suffix, save_chart_data_json
from uniswap_client import get_graph_endpoint, graphql_query, query_pool_day_data
from price_oracle import get_token_prices_usd
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
V3_MINT_TOPIC = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
V3_BURN_TOPIC = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"
_POOL_CHAIN_CACHE_PATH = os.environ.get("STRICT_POOL_CHAIN_CACHE_PATH", "data/strict_pool_chain_cache.json")
_EXACT2_CACHE_DB_PATH = os.environ.get("STRICT_EXACT2_CACHE_DB_PATH", "data/exact2_daily_cache.sqlite3")
_WARN_LOCK = threading.Lock()
_FALLBACK_WARNED: set[str] = set()
_Q96 = Decimal(2**96)
getcontext().prec = 80


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


def _hex_words(data_hex: str) -> list[str]:
    h = str(data_hex or "").strip().lower()
    if h.startswith("0x"):
        h = h[2:]
    if not h:
        return []
    if len(h) % 64 != 0:
        h = h.zfill((len(h) + 63) // 64 * 64)
    return [h[i : i + 64] for i in range(0, len(h), 64)]


def _decode_uint_word(word: str) -> int:
    try:
        return int(str(word or "0"), 16)
    except Exception:
        return 0


def _decode_int24_from_word(word: str) -> int:
    x = _decode_uint_word(word) & 0xFFFFFF
    if x & 0x800000:
        x -= 0x1000000
    return int(x)


def _sqrt_ratio_x96_at_tick(tick: int) -> Decimal:
    power = Decimal(int(tick)) / Decimal(2)
    return (Decimal("1.0001") ** power) * _Q96


def _amounts_from_liquidity_segment(liq: Decimal, sqrt_lo: Decimal, sqrt_hi: Decimal) -> tuple[Decimal, Decimal]:
    if liq <= 0 or sqrt_hi <= sqrt_lo:
        return Decimal(0), Decimal(0)
    amount0 = liq * (_Q96 * (sqrt_hi - sqrt_lo) / (sqrt_hi * sqrt_lo))
    amount1 = liq * ((sqrt_hi - sqrt_lo) / _Q96)
    return amount0, amount1


def _v3_active_positions_amounts_from_subgraph(
    endpoint: str,
    pool_id: str,
    current_tick: int,
    sqrt_price_x96: int,
) -> tuple[float, float, str, dict[str, Any]]:
    ep = str(endpoint or "").strip()
    pid = str(pool_id or "").strip().lower()
    if not ep:
        return 0.0, 0.0, "active_positions_no_endpoint", {"active_positions_count": 0}
    if not pid:
        return 0.0, 0.0, "active_positions_invalid_pool", {"active_positions_count": 0}
    if int(sqrt_price_x96) <= 0:
        return 0.0, 0.0, "active_positions_invalid_price", {"active_positions_count": 0}

    q_filtered = """
    query ActivePositions($pool: String!, $tick: BigInt!, $last: String!) {
      positions(
        first: 1000
        orderBy: id
        orderDirection: asc
        where: {
          pool: $pool
          liquidity_gt: "0"
          id_gt: $last
          tickLower_: { tickIdx_lte: $tick }
          tickUpper_: { tickIdx_gt: $tick }
        }
      ) {
        id
        liquidity
        tickLower { tickIdx }
        tickUpper { tickIdx }
      }
    }
    """
    q_unfiltered = """
    query ActivePositionsUnfiltered($pool: String!, $last: String!) {
      positions(
        first: 1000
        orderBy: id
        orderDirection: asc
        where: {
          pool: $pool
          liquidity_gt: "0"
          id_gt: $last
        }
      ) {
        id
        liquidity
        tickLower { tickIdx }
        tickUpper { tickIdx }
      }
    }
    """

    sqrt_p = Decimal(int(sqrt_price_x96))
    sqrt_cache: dict[int, Decimal] = {}

    def _sqrt_tick(t: int) -> Decimal:
        x = int(t)
        v = sqrt_cache.get(x)
        if v is not None:
            return v
        v = _sqrt_ratio_x96_at_tick(x)
        sqrt_cache[x] = v
        return v

    max_pages = int(max(1, int(os.environ.get("STRICT_V3_ACTIVE_POS_MAX_PAGES", "40") or 40)))
    max_rows = int(max(1000, int(os.environ.get("STRICT_V3_ACTIVE_POS_MAX_ROWS", "50000") or 50000)))

    def _scan(query_text: str, *, use_tick_filter: bool) -> tuple[Decimal, Decimal, int, int, int, str]:
        total0 = Decimal(0)
        total1 = Decimal(0)
        scanned = 0
        visited = 0
        last_id = ""
        pages = 0
        err = ""
        while pages < max_pages and visited < max_rows:
            vars_payload = {"pool": pid, "last": str(last_id)}
            if use_tick_filter:
                vars_payload["tick"] = str(int(current_tick))
            try:
                data = graphql_query(ep, query_text, vars_payload, retries=1)
            except Exception as e:
                err = f"active_positions_query_failed:{e}"
                break
            rows = ((data or {}).get("data") or {}).get("positions") or []
            if not isinstance(rows, list) or not rows:
                break
            pages += 1
            for r in rows:
                try:
                    liq = int(str((r or {}).get("liquidity") or "0"))
                    lo = int(str((((r or {}).get("tickLower") or {}).get("tickIdx") or "0")))
                    hi = int(str((((r or {}).get("tickUpper") or {}).get("tickIdx") or "0")))
                except Exception:
                    continue
                visited += 1
                if liq <= 0 or hi <= lo:
                    continue
                # In-range liquidity criterion (v3 canonical): tickLower <= currentTick < tickUpper.
                if not (lo <= int(current_tick) < hi):
                    continue
                sqrt_lo = _sqrt_tick(lo)
                sqrt_hi = _sqrt_tick(hi)
                if not (sqrt_lo <= sqrt_p < sqrt_hi):
                    continue
                a0, _ = _amounts_from_liquidity_segment(Decimal(liq), sqrt_p, sqrt_hi)
                _, a1 = _amounts_from_liquidity_segment(Decimal(liq), sqrt_lo, sqrt_p)
                if a0 <= 0 and a1 <= 0:
                    continue
                total0 += a0
                total1 += a1
                scanned += 1
                if visited >= max_rows:
                    break
            last_id = str((rows[-1] or {}).get("id") or last_id)
            if len(rows) < 1000:
                break
        return total0, total1, scanned, pages, visited, err

    total0, total1, scanned, pages, visited, err = _scan(q_filtered, use_tick_filter=True)
    source_base = "active_positions_subgraph_filtered"
    if err:
        source_base = "active_positions_filtered_error"
    # Some indexers can ignore/deny nested tick filters and return no rows.
    # Fallback: scan pool positions and apply strict in-range check locally.
    if scanned <= 0:
        f_total0, f_total1, f_scanned, f_pages, f_visited, f_err = _scan(q_unfiltered, use_tick_filter=False)
        if f_scanned > 0 or (not err and not f_err):
            total0, total1, scanned, pages, visited = f_total0, f_total1, f_scanned, f_pages, f_visited
            source_base = "active_positions_subgraph_local_filter"
            err = f_err
        elif f_err and not err:
            err = f_err
            source_base = "active_positions_local_filter_error"

    src = source_base
    if scanned >= max_rows or visited >= max_rows:
        src = f"{source_base}_partial_row_cap"
    elif pages >= max_pages:
        src = f"{source_base}_partial_page_cap"
    if err:
        src = f"{src}:{err}"
    return float(total0), float(total1), src, {
        "active_positions_count": int(scanned),
        "active_positions_pages": int(pages),
        "active_positions_rows_visited": int(visited),
    }


def _v3_active_positions_amounts_onchain_logs(
    *,
    chain_id: int,
    pool_id: str,
    current_tick: int,
    sqrt_price_x96: int,
    from_block: int,
    to_block: int,
) -> tuple[float, float, str, dict[str, Any]]:
    if int(sqrt_price_x96) <= 0:
        return 0.0, 0.0, "active_positions_onchain_invalid_price", {"active_positions_ranges": 0}
    start_block = int(max(1, int(from_block)))
    end_block = int(max(start_block, int(to_block)))
    by_range_liq: dict[tuple[int, int], int] = {}
    scanned_logs = 0

    def _on_log(log: dict) -> None:
        nonlocal scanned_logs
        scanned_logs += 1
        topics = log.get("topics") or []
        if not isinstance(topics, list) or len(topics) < 4:
            return
        t0 = str(topics[0] or "").strip().lower()
        if t0 not in {V3_MINT_TOPIC, V3_BURN_TOPIC}:
            return
        try:
            lo = int(_decode_int24_from_word(str(topics[2] or "")))
            hi = int(_decode_int24_from_word(str(topics[3] or "")))
        except Exception:
            return
        if hi <= lo:
            return
        words = _hex_words(str(log.get("data") or ""))
        amt = 0
        if t0 == V3_MINT_TOPIC:
            # Mint data: sender, amount, amount0, amount1.
            amt = int(_decode_uint_word(words[1])) if len(words) >= 2 else 0
            sign = 1
        else:
            # Burn data: amount, amount0, amount1.
            amt = int(_decode_uint_word(words[0])) if len(words) >= 1 else 0
            sign = -1
        if amt <= 0:
            return
        k = (int(lo), int(hi))
        by_range_liq[k] = int(by_range_liq.get(k, 0)) + int(sign) * int(amt)

    try:
        _logs, _covered_from, _timed_out = _fetch_logs_adaptive(
            int(chain_id),
            str(pool_id).strip().lower(),
            [V3_MINT_TOPIC],
            int(start_block),
            int(end_block),
            deadline_ts=(time.monotonic() + max(3.0, float(os.environ.get("STRICT_V3_ACTIVE_ONCHAIN_BUDGET_SEC", "20")))),
            on_log=_on_log,
        )
        _logs2, _covered_from2, _timed_out2 = _fetch_logs_adaptive(
            int(chain_id),
            str(pool_id).strip().lower(),
            [V3_BURN_TOPIC],
            int(start_block),
            int(end_block),
            deadline_ts=(time.monotonic() + max(3.0, float(os.environ.get("STRICT_V3_ACTIVE_ONCHAIN_BUDGET_SEC", "20")))),
            on_log=_on_log,
        )
        timed_out = bool(_timed_out or _timed_out2)
        covered_from = int(min(_covered_from, _covered_from2))
    except Exception as e:
        return 0.0, 0.0, f"active_positions_onchain_logs_failed:{e}", {"active_positions_ranges": 0}

    sqrt_p = Decimal(int(sqrt_price_x96))
    total0 = Decimal(0)
    total1 = Decimal(0)
    ranges_used = 0
    for (lo, hi), liq_raw in by_range_liq.items():
        liq = int(liq_raw)
        if liq <= 0:
            continue
        if not (int(lo) <= int(current_tick) < int(hi)):
            continue
        sqrt_lo = _sqrt_ratio_x96_at_tick(int(lo))
        sqrt_hi = _sqrt_ratio_x96_at_tick(int(hi))
        if not (sqrt_lo <= sqrt_p < sqrt_hi):
            continue
        a0, _ = _amounts_from_liquidity_segment(Decimal(liq), sqrt_p, sqrt_hi)
        _, a1 = _amounts_from_liquidity_segment(Decimal(liq), sqrt_lo, sqrt_p)
        if a0 <= 0 and a1 <= 0:
            continue
        total0 += a0
        total1 += a1
        ranges_used += 1
    src = "active_positions_onchain_logs"
    if timed_out:
        src = "active_positions_onchain_logs_partial_timeout"
    return float(total0), float(total1), src, {
        "active_positions_ranges": int(ranges_used),
        "active_positions_logs_scanned": int(scanned_logs),
        "active_positions_covered_from_block": int(covered_from),
        "active_positions_timed_out": bool(timed_out),
    }


def _v3_pool_state_at_block(chain_id: int, pool_id: str, block_tag: str = "latest") -> tuple[int, int, int, int, str]:
    p = str(pool_id or "").strip().lower()
    if not _is_eth_address(p):
        return 0, 0, 0, 0, "invalid_pool_id"
    try:
        slot0 = _rpc_json(
            int(chain_id),
            "eth_call",
            [{"to": p, "data": "0x3850c7bd"}, str(block_tag or "latest")],
            timeout_sec=8.0,
        )
        liq = _rpc_json(
            int(chain_id),
            "eth_call",
            [{"to": p, "data": "0x1a686502"}, str(block_tag or "latest")],
            timeout_sec=8.0,
        )
        spacing = _rpc_json(
            int(chain_id),
            "eth_call",
            [{"to": p, "data": "0xd0c93a7c"}, str(block_tag or "latest")],
            timeout_sec=8.0,
        )
    except Exception as e:
        return 0, 0, 0, 0, f"pool_state_failed:{e}"
    slot_words = _hex_words(str((slot0 or {}).get("result") or ""))
    liq_words = _hex_words(str((liq or {}).get("result") or ""))
    sp_words = _hex_words(str((spacing or {}).get("result") or ""))
    if len(slot_words) < 2:
        return 0, 0, 0, 0, "slot0_missing"
    sqrt_price_x96 = int(_decode_uint_word(slot_words[0]))
    tick = int(_decode_int24_from_word(slot_words[1]))
    liquidity = int(_decode_uint_word(liq_words[0])) if liq_words else 0
    tick_spacing = int(_decode_int24_from_word(sp_words[0])) if sp_words else 0
    if tick_spacing <= 0:
        tick_spacing = 10
    return sqrt_price_x96, tick, liquidity, tick_spacing, ""


def _v3_pool_state_now(chain_id: int, pool_id: str) -> tuple[int, int, int, int, str]:
    return _v3_pool_state_at_block(chain_id, pool_id, "latest")


def _nearby_day_candidates(target_day_ts: int, available_days: list[int], max_offset_days: int) -> list[int]:
    day = int((int(target_day_ts) // 86400) * 86400)
    days = sorted({int(d) for d in (available_days or []) if int(d) > 0})
    if not days:
        return [int(day)]
    day_set = set(days)
    out: list[int] = []
    if day in day_set:
        out.append(int(day))
    max_off = max(0, int(max_offset_days))
    for off in range(1, max_off + 1):
        d_prev = int(day - off * 86400)
        d_next = int(day + off * 86400)
        if d_prev in day_set:
            out.append(d_prev)
        if d_next in day_set:
            out.append(d_next)
    if out:
        return out
    nearest = min(days, key=lambda d: abs(int(d) - int(day)))
    return [int(nearest)]


def _v3_active_tvl_onchain_sparse_snapshots(
    *,
    chain: str,
    pool: dict[str, Any],
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    step_days: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str]:
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    pool_id = str(pool.get("id") or "").strip().lower()
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if chain_id <= 0 or not (_is_eth_address(pool_id) and token0 and token1):
        return [], "active_onchain_sparse:invalid_inputs"
    if not fees_usd:
        return [], "active_onchain_sparse:no_fee_days"
    t0 = time.monotonic()
    deadline = t0 + max(8.0, float(budget_sec))
    # Sparse day targets (same cadence as exact TVL).
    sparse = _sparse_series_every_n_days([(int(ts), 1.0) for ts, _ in fees_usd], max(1, int(step_days)))
    targets = [int(ts) for ts, _ in sparse if int(ts) > 0]
    if fees_usd and (not targets or int(targets[-1]) != int(fees_usd[-1][0])):
        targets.append(int(fees_usd[-1][0]))
    targets = sorted({int(x) for x in targets if int(x) > 0})
    fee_days = sorted({int((int(ts) // 86400) * 86400) for ts, _ in fees_usd if int(ts) > 0})
    if not targets:
        return [], "active_onchain_sparse:no_targets"
    latest_block = _latest_block_number(chain_id)
    from_block = int(pool.get("createdAtBlockNumber") or 0)
    if from_block <= 0:
        from_block = max(1, int(latest_block) - 3_500_000)

    day_blocks: dict[int, int] = {}
    for ts in targets:
        if time.monotonic() >= deadline:
            return [], "active_onchain_sparse:budget_timeout:block_resolution"
        day_ts = int((int(ts) // 86400) * 86400)
        if day_ts in day_blocks:
            continue
        try:
            day_blocks[day_ts] = int(_llama_block_for_day(ck, day_ts + 86399))
        except Exception:
            day_blocks[day_ts] = 0
    boundary_blocks = sorted({int(b) for b in day_blocks.values() if int(b) > 0}, reverse=True)
    if not boundary_blocks:
        return [], "active_onchain_sparse:no_day_blocks"

    # Build per-range liquidity deltas from Mint/Burn logs once.
    range_deltas: dict[tuple[int, int], dict[int, int]] = {}
    range_latest_net: dict[tuple[int, int], int] = {}
    scanned_logs = 0

    def _on_log(log: dict) -> None:
        nonlocal scanned_logs
        scanned_logs += 1
        topics = log.get("topics") or []
        if not isinstance(topics, list) or len(topics) < 4:
            return
        t0x = str(topics[0] or "").strip().lower()
        if t0x not in {V3_MINT_TOPIC, V3_BURN_TOPIC}:
            return
        try:
            lo = int(_decode_int24_from_word(str(topics[2] or "")))
            hi = int(_decode_int24_from_word(str(topics[3] or "")))
            bn = int(_safe_hex_to_int(log.get("blockNumber")))
        except Exception:
            return
        if hi <= lo or bn <= 0:
            return
        words = _hex_words(str(log.get("data") or ""))
        amt = 0
        if t0x == V3_MINT_TOPIC:
            amt = int(_decode_uint_word(words[1])) if len(words) >= 2 else 0
            sign = 1
        else:
            amt = int(_decode_uint_word(words[0])) if len(words) >= 1 else 0
            sign = -1
        if amt <= 0:
            return
        k = (int(lo), int(hi))
        rd = range_deltas.setdefault(k, {})
        rd[bn] = int(rd.get(bn, 0)) + int(sign) * int(amt)
        range_latest_net[k] = int(range_latest_net.get(k, 0)) + int(sign) * int(amt)

    try:
        _fetch_logs_adaptive(
            int(chain_id),
            str(pool_id).strip().lower(),
            [V3_MINT_TOPIC],
            int(from_block),
            int(latest_block),
            deadline_ts=deadline,
            on_log=_on_log,
        )
        _fetch_logs_adaptive(
            int(chain_id),
            str(pool_id).strip().lower(),
            [V3_BURN_TOPIC],
            int(from_block),
            int(latest_block),
            deadline_ts=deadline,
            on_log=_on_log,
        )
    except Exception as e:
        return [], f"active_onchain_sparse:logs_failed:{e}"
    if not range_latest_net:
        return [], "active_onchain_sparse:no_active_ranges"

    dec0 = int(_token_decimals_from_pool(pool, "token0") or _erc20_decimals(chain_id, token0))
    dec1 = int(_token_decimals_from_pool(pool, "token1") or _erc20_decimals(chain_id, token1))
    sqrt_cache: dict[int, Decimal] = {}

    def _sqrt_tick(t: int) -> Decimal:
        x = int(t)
        v = sqrt_cache.get(x)
        if v is None:
            v = _sqrt_ratio_x96_at_tick(x)
            sqrt_cache[x] = v
        return v

    # Historical prices cache by day_ts.
    day_price: dict[int, tuple[float, float]] = {}
    out: list[tuple[int, float]] = []
    points_done = 0
    points_nearby = 0
    try:
        nearby_days = max(1, int(os.environ.get("STRICT_V3_ACTIVE_NEARBY_DAYS", str(max(2, int(step_days) // 2)))))
    except Exception:
        nearby_days = max(2, int(step_days) // 2)
    for ts in targets:
        if time.monotonic() >= deadline:
            break
        target_day_ts = int((int(ts) // 86400) * 86400)
        cand_days = _nearby_day_candidates(target_day_ts, fee_days, nearby_days)
        picked = False
        for cand_day_ts in cand_days:
            if time.monotonic() >= deadline:
                break
            b = int(day_blocks.get(cand_day_ts, 0))
            if b <= 0:
                try:
                    day_blocks[cand_day_ts] = int(_llama_block_for_day(ck, int(cand_day_ts) + 86399))
                except Exception:
                    day_blocks[cand_day_ts] = 0
                b = int(day_blocks.get(cand_day_ts, 0))
            if b <= 0:
                continue
            # Price per day (cheap cache).
            pp = day_price.get(cand_day_ts)
            if pp is None:
                p0 = float(_historical_price_usd(ck, token0, int(cand_day_ts), int(day_start_ts), int(day_end_ts)))
                p1 = float(_historical_price_usd(ck, token1, int(cand_day_ts), int(day_start_ts), int(day_end_ts)))
                day_price[cand_day_ts] = (float(max(0.0, p0)), float(max(0.0, p1)))
                pp = day_price[cand_day_ts]
            p0, p1 = pp
            sqrt_p_x96, tick_now, _liq_now, _spacing, st_err = _v3_pool_state_at_block(chain_id, pool_id, hex(int(b)))
            if int(sqrt_p_x96) <= 0 or st_err:
                continue
            sqrt_p = Decimal(int(sqrt_p_x96))
            total0_raw = Decimal(0)
            total1_raw = Decimal(0)
            for (lo, hi), latest_liq in range_latest_net.items():
                if time.monotonic() >= deadline:
                    break
                liq = int(latest_liq)
                if liq <= 0:
                    continue
                # Remove future deltas to reconstruct liquidity at block b.
                for bn, dv in (range_deltas.get((lo, hi), {}) or {}).items():
                    if int(bn) > int(b):
                        liq -= int(dv)
                if liq <= 0:
                    continue
                if not (int(lo) <= int(tick_now) < int(hi)):
                    continue
                sqrt_lo = _sqrt_tick(int(lo))
                sqrt_hi = _sqrt_tick(int(hi))
                if not (sqrt_lo <= sqrt_p < sqrt_hi):
                    continue
                a0, _ = _amounts_from_liquidity_segment(Decimal(int(liq)), sqrt_p, sqrt_hi)
                _, a1 = _amounts_from_liquidity_segment(Decimal(int(liq)), sqrt_lo, sqrt_p)
                if a0 <= 0 and a1 <= 0:
                    continue
                total0_raw += max(Decimal(0), a0)
                total1_raw += max(Decimal(0), a1)
            amt0 = float(total0_raw) / float(10 ** max(0, int(dec0)))
            amt1 = float(total1_raw) / float(10 ** max(0, int(dec1)))
            tvl = max(0.0, amt0 * max(0.0, float(p0))) + max(0.0, amt1 * max(0.0, float(p1)))
            if tvl <= 0.0:
                continue
            out.append((int(ts), float(max(0.0, tvl))))
            points_done += 1
            if int(cand_day_ts) != int(target_day_ts):
                points_nearby += 1
            picked = True
            break
        if not picked:
            continue
    if not out:
        return [], f"active_onchain_sparse:no_points:logs={int(scanned_logs)}"
    # Ensure latest day anchor exists if available in targets.
    out = sorted({int(ts): float(v) for ts, v in out}.items(), key=lambda x: int(x[0]))
    reason = (
        f"active_onchain_sparse:ok:points={int(points_done)}/{len(targets)}:"
        f"nearby={int(points_nearby)}:ranges={len(range_latest_net)}:logs={int(scanned_logs)}:"
        f"elapsed={max(0.0, time.monotonic() - t0):.1f}"
    )
    return [(int(ts), float(v)) for ts, v in out], reason


def _v3_active_positions_amounts_from_subgraph_at_block(
    endpoint: str,
    pool_id: str,
    current_tick: int,
    sqrt_price_x96: int,
    block_number: int,
) -> tuple[float, float, str, dict[str, Any]]:
    ep = str(endpoint or "").strip()
    pid = str(pool_id or "").strip().lower()
    bn = int(block_number or 0)
    if not ep:
        return 0.0, 0.0, "active_positions_hist_no_endpoint", {"active_positions_count": 0}
    if not pid or bn <= 0:
        return 0.0, 0.0, "active_positions_hist_invalid_inputs", {"active_positions_count": 0}
    if int(sqrt_price_x96) <= 0:
        return 0.0, 0.0, "active_positions_hist_invalid_price", {"active_positions_count": 0}

    q_filtered = """
    query ActivePositionsAtBlock($pool: String!, $tick: BigInt!, $last: String!, $block: Int!) {
      positions(
        block: { number: $block }
        first: 1000
        orderBy: id
        orderDirection: asc
        where: {
          pool: $pool
          liquidity_gt: "0"
          id_gt: $last
          tickLower_: { tickIdx_lte: $tick }
          tickUpper_: { tickIdx_gt: $tick }
        }
      ) {
        id
        liquidity
        tickLower { tickIdx }
        tickUpper { tickIdx }
      }
    }
    """
    q_unfiltered = """
    query ActivePositionsUnfilteredAtBlock($pool: String!, $last: String!, $block: Int!) {
      positions(
        block: { number: $block }
        first: 1000
        orderBy: id
        orderDirection: asc
        where: {
          pool: $pool
          liquidity_gt: "0"
          id_gt: $last
        }
      ) {
        id
        liquidity
        tickLower { tickIdx }
        tickUpper { tickIdx }
      }
    }
    """

    sqrt_p = Decimal(int(sqrt_price_x96))
    sqrt_cache: dict[int, Decimal] = {}

    def _sqrt_tick(t: int) -> Decimal:
        x = int(t)
        v = sqrt_cache.get(x)
        if v is not None:
            return v
        v = _sqrt_ratio_x96_at_tick(x)
        sqrt_cache[x] = v
        return v

    max_pages = int(max(1, int(os.environ.get("STRICT_V3_ACTIVE_HIST_POS_MAX_PAGES", "12") or 12)))
    max_rows = int(max(1000, int(os.environ.get("STRICT_V3_ACTIVE_HIST_POS_MAX_ROWS", "12000") or 12000)))

    def _scan(query_text: str, *, use_tick_filter: bool) -> tuple[Decimal, Decimal, int, int, int, str]:
        total0 = Decimal(0)
        total1 = Decimal(0)
        scanned = 0
        visited = 0
        last_id = ""
        pages = 0
        err = ""
        while pages < max_pages and visited < max_rows:
            vars_payload: dict[str, Any] = {"pool": pid, "last": str(last_id), "block": int(bn)}
            if use_tick_filter:
                vars_payload["tick"] = str(int(current_tick))
            try:
                data = graphql_query(ep, query_text, vars_payload, retries=1)
            except Exception as e:
                err = f"active_positions_hist_query_failed:{e}"
                break
            rows = ((data or {}).get("data") or {}).get("positions") or []
            if not isinstance(rows, list) or not rows:
                break
            pages += 1
            for r in rows:
                try:
                    liq = int(str((r or {}).get("liquidity") or "0"))
                    lo = int(str((((r or {}).get("tickLower") or {}).get("tickIdx") or "0")))
                    hi = int(str((((r or {}).get("tickUpper") or {}).get("tickIdx") or "0")))
                except Exception:
                    continue
                visited += 1
                if liq <= 0 or hi <= lo:
                    continue
                if not (lo <= int(current_tick) < hi):
                    continue
                sqrt_lo = _sqrt_tick(lo)
                sqrt_hi = _sqrt_tick(hi)
                if not (sqrt_lo <= sqrt_p < sqrt_hi):
                    continue
                a0, _ = _amounts_from_liquidity_segment(Decimal(liq), sqrt_p, sqrt_hi)
                _, a1 = _amounts_from_liquidity_segment(Decimal(liq), sqrt_lo, sqrt_p)
                if a0 <= 0 and a1 <= 0:
                    continue
                total0 += a0
                total1 += a1
                scanned += 1
                if visited >= max_rows:
                    break
            last_id = str((rows[-1] or {}).get("id") or last_id)
            if len(rows) < 1000:
                break
        return total0, total1, scanned, pages, visited, err

    total0, total1, scanned, pages, visited, err = _scan(q_filtered, use_tick_filter=True)
    source_base = "active_positions_hist_subgraph_filtered"
    if err:
        source_base = "active_positions_hist_filtered_error"
    if scanned <= 0:
        f_total0, f_total1, f_scanned, f_pages, f_visited, f_err = _scan(q_unfiltered, use_tick_filter=False)
        if f_scanned > 0 or (not err and not f_err):
            total0, total1, scanned, pages, visited = f_total0, f_total1, f_scanned, f_pages, f_visited
            source_base = "active_positions_hist_subgraph_local_filter"
            err = f_err
        elif f_err and not err:
            err = f_err
            source_base = "active_positions_hist_local_filter_error"
    src = source_base
    if scanned >= max_rows or visited >= max_rows:
        src = f"{source_base}_partial_row_cap"
    elif pages >= max_pages:
        src = f"{source_base}_partial_page_cap"
    if err:
        src = f"{src}:{err}"
    return float(total0), float(total1), src, {
        "active_positions_count": int(scanned),
        "active_positions_pages": int(pages),
        "active_positions_rows_visited": int(visited),
    }


def _v3_active_tvl_subgraph_sparse_snapshots(
    *,
    chain: str,
    endpoint: str,
    pool: dict[str, Any],
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    step_days: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str]:
    ck = str(chain or "").strip().lower()
    ep = str(endpoint or "").strip()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    pool_id = str(pool.get("id") or "").strip().lower()
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if chain_id <= 0 or not (_is_eth_address(pool_id) and token0 and token1 and ep):
        return [], "active_subgraph_sparse:invalid_inputs"
    if not fees_usd:
        return [], "active_subgraph_sparse:no_fee_days"
    t0 = time.monotonic()
    deadline = t0 + max(8.0, float(budget_sec))
    sparse = _sparse_series_every_n_days([(int(ts), 1.0) for ts, _ in fees_usd], max(1, int(step_days)))
    targets = [int(ts) for ts, _ in sparse if int(ts) > 0]
    if fees_usd and (not targets or int(targets[-1]) != int(fees_usd[-1][0])):
        targets.append(int(fees_usd[-1][0]))
    targets = sorted({int(x) for x in targets if int(x) > 0})
    fee_days = sorted({int((int(ts) // 86400) * 86400) for ts, _ in fees_usd if int(ts) > 0})
    if not targets:
        return [], "active_subgraph_sparse:no_targets"

    day_blocks: dict[int, int] = {}
    for ts in targets:
        if time.monotonic() >= deadline:
            return [], "active_subgraph_sparse:budget_timeout:block_resolution"
        day_ts = int((int(ts) // 86400) * 86400)
        if day_ts in day_blocks:
            continue
        try:
            day_blocks[day_ts] = int(_llama_block_for_day(ck, day_ts + 86399))
        except Exception:
            day_blocks[day_ts] = 0
    if not any(int(x) > 0 for x in day_blocks.values()):
        return [], "active_subgraph_sparse:no_day_blocks"

    dec0 = int(_token_decimals_from_pool(pool, "token0") or _erc20_decimals(chain_id, token0))
    dec1 = int(_token_decimals_from_pool(pool, "token1") or _erc20_decimals(chain_id, token1))
    day_price: dict[int, tuple[float, float]] = {}
    out: list[tuple[int, float]] = []
    points_done = 0
    points_nearby = 0
    try:
        nearby_days = max(1, int(os.environ.get("STRICT_V3_ACTIVE_NEARBY_DAYS", str(max(2, int(step_days) // 2)))))
    except Exception:
        nearby_days = max(2, int(step_days) // 2)
    for ts in targets:
        if time.monotonic() >= deadline:
            break
        target_day_ts = int((int(ts) // 86400) * 86400)
        cand_days = _nearby_day_candidates(target_day_ts, fee_days, nearby_days)
        picked = False
        for cand_day_ts in cand_days:
            if time.monotonic() >= deadline:
                break
            b = int(day_blocks.get(cand_day_ts, 0))
            if b <= 0:
                try:
                    day_blocks[cand_day_ts] = int(_llama_block_for_day(ck, int(cand_day_ts) + 86399))
                except Exception:
                    day_blocks[cand_day_ts] = 0
                b = int(day_blocks.get(cand_day_ts, 0))
            if b <= 0:
                continue
            pp = day_price.get(cand_day_ts)
            if pp is None:
                p0 = float(_historical_price_usd(ck, token0, int(cand_day_ts), int(day_start_ts), int(day_end_ts)))
                p1 = float(_historical_price_usd(ck, token1, int(cand_day_ts), int(day_start_ts), int(day_end_ts)))
                day_price[cand_day_ts] = (float(max(0.0, p0)), float(max(0.0, p1)))
                pp = day_price[cand_day_ts]
            p0, p1 = pp
            sqrt_p_x96, tick_now, _liq_now, _spacing, st_err = _v3_pool_state_at_block(chain_id, pool_id, hex(int(b)))
            if int(sqrt_p_x96) <= 0 or st_err:
                continue
            act0_raw, act1_raw, _src, _dbg = _v3_active_positions_amounts_from_subgraph_at_block(
                endpoint=ep,
                pool_id=pool_id,
                current_tick=int(tick_now),
                sqrt_price_x96=int(sqrt_p_x96),
                block_number=int(b),
            )
            amt0 = float(act0_raw) / float(10 ** max(0, int(dec0)))
            amt1 = float(act1_raw) / float(10 ** max(0, int(dec1)))
            if amt0 <= 0.0 and amt1 <= 0.0:
                continue
            tvl = max(0.0, amt0 * max(0.0, float(p0))) + max(0.0, amt1 * max(0.0, float(p1)))
            if tvl <= 0.0:
                continue
            out.append((int(ts), float(tvl)))
            points_done += 1
            if int(cand_day_ts) != int(target_day_ts):
                points_nearby += 1
            picked = True
            break
        if not picked:
            continue
    if not out:
        return [], "active_subgraph_sparse:no_points"
    out = sorted({int(ts): float(v) for ts, v in out}.items(), key=lambda x: int(x[0]))
    reason = (
        f"active_subgraph_sparse:ok:points={int(points_done)}/{len(targets)}:"
        f"nearby={int(points_nearby)}:elapsed={max(0.0, time.monotonic() - t0):.1f}"
    )
    return [(int(ts), float(v)) for ts, v in out], reason


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
        createdAtBlockNumber
        createdAtTimestamp
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
        createdAtBlockNumber
        createdAtTimestamp
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
        "strict_compare_estimated_active_tvl": [],
        "strict_compare_estimated_active_fees": [],
        "strict_compare_exact_tvl": [],
        "strict_compare_exact_active_tvl": [],
        "strict_compare_exact_active_fees": [],
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


def _resolve_pool_tvl_now_onchain(pool: dict, chain: str, endpoint: str = "") -> tuple[float, str, str, dict[str, Any]]:
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return 0.0, "", "unsupported_chain", {}
    pool_id = str(pool.get("id") or "").strip().lower()
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if not (pool_id and token0 and token1):
        return 0.0, "", "missing_pool_tokens", {}
    try:
        latest_block = int(_rpc_json(chain_id, "eth_blockNumber", [], timeout_sec=8.0).get("result", "0x0"), 16)
    except Exception as e:
        return 0.0, "", f"latest_block_failed:{e}", {}
    if latest_block <= 0:
        return 0.0, "", "latest_block_not_found", {}
    try:
        dec0 = int(_token_decimals_from_pool(pool, "token0") or _erc20_decimals(chain_id, token0))
        dec1 = int(_token_decimals_from_pool(pool, "token1") or _erc20_decimals(chain_id, token1))
        b0 = int(_balance_of_raw(chain_id, token0, pool_id, latest_block, timeout_sec=8.0))
        b1 = int(_balance_of_raw(chain_id, token1, pool_id, latest_block, timeout_sec=8.0))
    except Exception as e:
        return 0.0, "", f"onchain_balance_failed:{e}", {}
    amt0 = float(b0) / float(10 ** max(0, dec0))
    amt1 = float(b1) / float(10 ** max(0, dec1))

    sqrt_p, tick_now, liq_now, tick_spacing, state_err = _v3_pool_state_now(chain_id, pool_id)
    act0_raw, act1_raw, _act_src, _act_dbg = _v3_active_positions_amounts_from_subgraph(
        endpoint=str(endpoint or ""),
        pool_id=pool_id,
        current_tick=int(tick_now),
        sqrt_price_x96=int(sqrt_p),
    )
    if float(act0_raw) <= 0.0 and float(act1_raw) <= 0.0 and "no field `positions`" in str(_act_src).lower():
        created_at_block = int(pool.get("createdAtBlockNumber") or 1)
        og0, og1, og_src, og_dbg = _v3_active_positions_amounts_onchain_logs(
            chain_id=int(chain_id),
            pool_id=pool_id,
            current_tick=int(tick_now),
            sqrt_price_x96=int(sqrt_p),
            from_block=int(created_at_block),
            to_block=int(latest_block),
        )
        if float(og0) > 0.0 or float(og1) > 0.0:
            act0_raw, act1_raw = float(og0), float(og1)
        _act_src = str(og_src or _act_src)
        _act_dbg = dict(og_dbg or {})
    act0 = float(act0_raw) / float(10 ** max(0, dec0))
    act1 = float(act1_raw) / float(10 ** max(0, dec1))
    # Root-cause fix: active liquidity amounts are a subset of full pool balances.
    # Some upstream active-position sources can overcount; clamp by token balances.
    act0_pre_cap = float(act0)
    act1_pre_cap = float(act1)
    act0 = float(min(max(0.0, act0), max(0.0, amt0)))
    act1 = float(min(max(0.0, act1), max(0.0, amt1)))

    prices, sources, errs = get_token_prices_usd(ck, [token0, token1])
    p0 = float(prices.get(token0) or 0.0)
    p1 = float(prices.get(token1) or 0.0)
    ratio01 = float(pool.get("token0Price") or 0.0) if pool.get("token0Price") is not None else 0.0
    if p0 <= 0 and p1 > 0 and ratio01 > 0:
        p0 = p1 * ratio01
    if p1 <= 0 and p0 > 0 and ratio01 > 0:
        p1 = p0 / ratio01
    if p0 <= 0 and p1 <= 0:
        return 0.0, "", ";".join([str(x) for x in errs if str(x).strip()]) or "price_unavailable", {}
    tvl_full = max(0.0, amt0 * max(0.0, p0)) + max(0.0, amt1 * max(0.0, p1))
    tvl_active = max(0.0, act0 * max(0.0, p0)) + max(0.0, act1 * max(0.0, p1))
    tvl = float(tvl_full if tvl_full > 0.0 else tvl_active)
    src0 = str(sources.get(token0) or "")
    src1 = str(sources.get(token1) or "")
    src = src0 or src1
    if src0 and src1 and src0 != src1:
        src = f"{src0}+{src1}"
    state_src = "slot0+liquidity" if not state_err else f"state_unavailable:{state_err}"
    source = f"onchain_full_pool+price:{src}:alt=active_positions:{_act_src}:{state_src}"
    dbg = {
        "latest_block": int(latest_block),
        "decimals0": int(dec0),
        "decimals1": int(dec1),
        "amount0_full_pool": float(amt0),
        "amount1_full_pool": float(amt1),
        "amount0_active_window_raw": float(max(0.0, act0_pre_cap)),
        "amount1_active_window_raw": float(max(0.0, act1_pre_cap)),
        "amount0_active_window": float(max(0.0, act0)),
        "amount1_active_window": float(max(0.0, act1)),
        "tvl_full_pool_usd": float(tvl_full),
        "tvl_active_window_usd": float(tvl_active),
        "quantity_source": "full_pool_balanceOf",
        "quantity_source_secondary": "active_positions",
        "active_positions_source": str(_act_src or ""),
        "active_positions_count": int((_act_dbg or {}).get("active_positions_count") or 0),
        "active_positions_pages": int((_act_dbg or {}).get("active_positions_pages") or 0),
        "active_positions_ranges": int((_act_dbg or {}).get("active_positions_ranges") or 0),
        "active_positions_logs_scanned": int((_act_dbg or {}).get("active_positions_logs_scanned") or 0),
        "pool_state_error": str(state_err or ""),
        "current_tick": int(tick_now),
        "pool_liquidity": int(liq_now),
        "tick_spacing": int(tick_spacing),
    }
    return float(tvl), source, "", dbg


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
    *,
    fallback_value: float = 0.0,
) -> list[tuple[int, float]]:
    # exact_series can be sparse (one point per N days). For fee replay we need a
    # daily shape; carry the latest known exact TVL forward instead of zero-filling
    # missing days, otherwise cumulative exact fees are artificially suppressed.
    return _expand_sparse_tvl_to_fee_days(
        fees_usd,
        exact_series,
        fallback_value=float(fallback_value or 0.0),
    )


def _expand_sparse_tvl_to_fee_days(
    fees_usd: list[tuple[int, float]],
    sparse_series: list[tuple[int, float]],
    *,
    fallback_value: float = 0.0,
) -> list[tuple[int, float]]:
    days = [int(ts) for ts, _ in (fees_usd or [])]
    if not days:
        return []
    sparse = sorted(
        [(int(ts), float(v or 0.0)) for ts, v in (sparse_series or []) if int(ts) > 0 and float(v or 0.0) > 0.0],
        key=lambda x: int(x[0]),
    )
    if not sparse:
        fv = float(max(0.0, float(fallback_value or 0.0)))
        return [(int(ts), fv) for ts in days]
    out: list[tuple[int, float]] = []
    j = 0
    last_v = float(sparse[0][1])
    for ts in days:
        while j < len(sparse) and int(sparse[j][0]) <= int(ts):
            last_v = float(sparse[j][1])
            j += 1
        out.append((int(ts), float(max(0.0, last_v))))
    return out


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


def _smooth_sparse_exact_series(
    series: list[tuple[int, float]],
    *,
    max_missing_ratio: float = 0.50,
) -> tuple[list[tuple[int, float]], int]:
    """Fill gaps in exact series from neighboring exact points only.

    No estimate blending here: linear interpolation between known exact points,
    and edge carry from the closest known point. Applied only when missing ratio
    is within tolerated threshold (default 50%).
    """
    arr = [(int(ts), float(v or 0.0)) for ts, v in (series or [])]
    n = len(arr)
    if n == 0:
        return arr, 0
    miss_idx = [i for i, (_ts, v) in enumerate(arr) if float(v) <= 0.0]
    if not miss_idx:
        return arr, 0
    miss_ratio = float(len(miss_idx) / max(1, n))
    if miss_ratio > float(max_missing_ratio):
        return arr, 0

    out = list(arr)
    known = [i for i, (_ts, v) in enumerate(arr) if float(v) > 0.0]
    if not known:
        return arr, 0

    filled = 0
    for i in miss_idx:
        left = next((k for k in range(i - 1, -1, -1) if float(out[k][1]) > 0.0), None)
        right = next((k for k in range(i + 1, n) if float(out[k][1]) > 0.0), None)
        ts_i = int(out[i][0])
        if left is not None and right is not None and right != left:
            ts_l, v_l = out[left]
            ts_r, v_r = out[right]
            if int(ts_r) > int(ts_l):
                alpha = float((ts_i - int(ts_l)) / max(1, int(ts_r) - int(ts_l)))
                v = float(v_l + (v_r - v_l) * alpha)
            else:
                v = float((float(v_l) + float(v_r)) / 2.0)
            out[i] = (ts_i, max(0.0, v))
            filled += 1
            continue
        if left is not None:
            out[i] = (ts_i, max(0.0, float(out[left][1])))
            filled += 1
            continue
        if right is not None:
            out[i] = (ts_i, max(0.0, float(out[right][1])))
            filled += 1
            continue
    return out, int(filled)


def _suppress_exact_outliers(
    series: list[tuple[int, float]],
    *,
    estimated_series: list[tuple[int, float]] | None = None,
    pool_tvl_now_usd: float = 0.0,
    max_ratio_vs_est: float = 2.2,
    max_ratio_vs_now: float = 2.2,
    max_jump_ratio: float = 2.5,
) -> tuple[list[tuple[int, float]], int]:
    """Drop clearly broken exact spikes by setting them to zero."""
    arr = [(int(ts), float(v or 0.0)) for ts, v in (series or [])]
    n = len(arr)
    if n == 0:
        return arr, 0
    est_by_ts = {int(ts): float(v or 0.0) for ts, v in (estimated_series or [])}
    out = list(arr)
    dropped = 0
    for i, (ts, v) in enumerate(arr):
        if float(v) <= 0.0:
            continue
        upper_bounds: list[float] = []
        est_v = float(est_by_ts.get(int(ts), 0.0))
        if est_v > 0.0:
            upper_bounds.append(float(est_v) * float(max_ratio_vs_est))
        if float(pool_tvl_now_usd) > 0.0:
            upper_bounds.append(float(pool_tvl_now_usd) * float(max_ratio_vs_now))
        if upper_bounds and float(v) > min(upper_bounds):
            out[i] = (int(ts), 0.0)
            dropped += 1
            continue
        # Local spike check against neighbors.
        left = next((float(out[k][1]) for k in range(i - 1, -1, -1) if float(out[k][1]) > 0.0), 0.0)
        right = next((float(out[k][1]) for k in range(i + 1, n) if float(out[k][1]) > 0.0), 0.0)
        neigh_max = max(left, right)
        if neigh_max > 0.0 and float(v) > float(neigh_max) * float(max_jump_ratio):
            out[i] = (int(ts), 0.0)
            dropped += 1
    return out, int(dropped)


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


def _sparse_series_every_n_days(series: list[tuple[int, float]], step_days: int = 5) -> list[tuple[int, float]]:
    arr: list[tuple[int, float]] = []
    seen_ts: set[int] = set()
    for ts, v in sorted((series or []), key=lambda x: int(x[0])):
        t = int(ts)
        if t in seen_ts:
            continue
        seen_ts.add(t)
        arr.append((t, float(v)))
    if not arr:
        return []
    step_sec = max(86400, int(max(1, int(step_days)) * 86400))
    first_ts = int(arr[0][0])
    last_ts = int(arr[-1][0])
    # Build target timeline: one point every N days + force TVL NOW (last point).
    targets: list[int] = []
    cur = int((first_ts // 86400) * 86400)
    end = int((last_ts // 86400) * 86400)
    while cur <= end:
        targets.append(int(cur))
        cur += step_sec
    if not targets or targets[-1] != int(last_ts):
        targets.append(int(last_ts))

    # For each target day pick nearest available point.
    out: list[tuple[int, float]] = []
    used_idx: set[int] = set()
    for target in targets:
        best_i = -1
        best_d = 10**30
        for i, (ts, _v) in enumerate(arr):
            if i in used_idx:
                continue
            d = abs(int(ts) - int(target))
            if d < best_d:
                best_d = d
                best_i = i
        if best_i < 0:
            continue
        used_idx.add(best_i)
        out.append((int(arr[best_i][0]), float(arr[best_i][1])))
    out = sorted(out, key=lambda x: int(x[0]))
    if out and int(out[-1][0]) != int(last_ts):
        out.append((int(last_ts), float(arr[-1][1])))
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
    # Fallback: extract key tail from configured Alchemy RPC URL(s).
    for k, v in os.environ.items():
        kk = str(k or "").strip().upper()
        if not kk.startswith("ALCHEMY_RPC_URL_"):
            continue
        url = str(v or "").strip()
        if ".g.alchemy.com/v2/" not in url:
            continue
        tail = url.rsplit("/v2/", 1)[-1].strip().strip("/")
        if tail:
            return tail
    return ""


def _alchemy_url_for_chain(chain: str) -> str:
    ck = str(chain or "").strip().lower()
    cu = ck.upper()
    direct = str(os.environ.get(f"ALCHEMY_RPC_URL_{cu}", "") or "").strip()
    if ".g.alchemy.com/v2/" in direct:
        return direct
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
) -> tuple[dict[int, int], bool, int]:
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
    timed_out = False
    t0 = time.monotonic()
    directions = [("from", -1), ("to", 1)]
    for idx, (direction, sign) in enumerate(directions):
        page_key = ""
        pages_used = 0
        items_used = 0
        seen_keys: set[str] = set()
        dir_t0 = time.monotonic()
        dir_deadline_ts = deadline_ts
        if deadline_ts is not None:
            now = time.monotonic()
            left_total = max(0.0, float(deadline_ts) - now)
            dirs_left = max(1, len(directions) - int(idx))
            # Reserve a fair share of remaining time for each direction.
            dir_deadline_ts = now + max(0.8, left_total / float(dirs_left))
        while True:
            if dir_deadline_ts is not None and time.monotonic() >= float(dir_deadline_ts):
                timed_out = True
                break
            if pages_used >= int(max_pages):
                timed_out = True
                break
            left = float(req_timeout)
            if dir_deadline_ts is not None:
                left = max(0.5, min(float(req_timeout), float(dir_deadline_ts) - time.monotonic()))
                if left <= 0.55:
                    timed_out = True
                    break
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
            try:
                r = requests.post(url, json=body, timeout=(2.0, float(left)))
            except requests.Timeout:
                timed_out = True
                break
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
        if timed_out:
            _strict_debug(
                f"alchemy dir={direction} token={str(token)[:10]}.. budget timeout "
                f"pages={pages_used} items={items_used} elapsed={time.monotonic() - dir_t0:.1f}s"
            )
            # Continue with the opposite direction to salvage additional history.
            continue
        _strict_debug(
            f"alchemy dir={direction} token={str(token)[:10]}.. done pages={pages_used} items={items_used} "
            f"elapsed={time.monotonic() - dir_t0:.1f}s"
        )
    covered_from = int(from_block)
    if timed_out:
        # Conservative: only trust history down to the earliest block actually seen.
        # Older days become uncovered and should not be rendered as exact.
        if by_block:
            covered_from = int(min(int(b) for b in by_block.keys() if int(b) > 0))
        else:
            covered_from = int(to_block) + 1
    _strict_debug(
        f"alchemy token={str(token)[:10]}.. total elapsed={time.monotonic() - t0:.1f}s "
        f"blocks={len(by_block)} covered_from={covered_from}"
    )
    return by_block, bool(timed_out), int(covered_from)


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
    out, timed_out, covered_from = _alchemy_fetch_transfer_deltas(
        chain=str(chain),
        token=str(token),
        pool_id=str(pool_id),
        from_block=int(from_block),
        to_block=int(to_block),
        deadline_ts=deadline_ts,
    )
    return out, int(covered_from), bool(timed_out)


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
    pool_id = str(pool.get("id") or "").strip().lower()
    day_keys = [int((int(ts) // 86400) * 86400) for ts, _ in (fees_usd or []) if int(ts) > 0]
    use_cache = str(os.environ.get("STRICT_EXACT2_USE_CACHE", "0")).strip().lower() in {"1", "true", "yes", "on"}
    cached_by_day: dict[int, float] = {}
    if use_cache:
        cached_by_day = _exact2_cache_load(chain=str(chain), pool_id=pool_id, day_ts_list=day_keys)
        if day_keys and all(int(d) in cached_by_day for d in day_keys):
            hit_series = [(int(ts), float(cached_by_day.get(int((int(ts) // 86400) * 86400), 0.0))) for ts, _ in fees_usd]
            hit_cov = float(sum(1 for _ts, v in hit_series if float(v) > 0.0) / max(1, len(hit_series)))
            return hit_series, "exact2_cache:ok", hit_cov

    # Sparse exact-point mode: never fetch exact for every day by default.
    # Keep cadence around 5 days, and relax to 10 days on long windows.
    try:
        sparse_points = int(os.environ.get("STRICT_EXACT2_SPARSE_POINTS", "").strip() or "0")
    except Exception:
        sparse_points = 0
    if sparse_points <= 0:
        n_days = int(len(fees_usd or []))
        try:
            sparse_step_days = max(5, int(os.environ.get("STRICT_EXACT2_SPARSE_STEP_DAYS", "5")))
        except Exception:
            sparse_step_days = 5
        if n_days > 180:
            sparse_step_days = max(sparse_step_days, 10)
        auto_points = max(2, int(math.ceil(float(max(1, n_days)) / float(max(1, sparse_step_days)))))
        try:
            max_points = max(8, int(os.environ.get("STRICT_EXACT2_SPARSE_MAX_POINTS", "48")))
        except Exception:
            max_points = 48
        sparse_points = min(int(max_points), int(auto_points))
    return _build_exact2_tvl_series_v3_sparse_points(
            chain=chain,
            pool=pool,
            fees_usd=fees_usd,
            day_start_ts=day_start_ts,
            day_end_ts=day_end_ts,
            points=int(sparse_points),
            budget_sec=float(budget_sec),
        )



def _build_exact2_tvl_series_v3_sparse_points(
    *,
    chain: str,
    pool: dict,
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    points: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str, float]:
    run_t0 = time.monotonic()
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return [], "exact2_sparse_points:unsupported_chain", 0.0
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    pool_id = str(pool.get("id") or "").strip().lower()
    if not (token0 and token1 and pool_id):
        return [], "exact2_sparse_points:missing_pool_tokens", 0.0

    try:
        dec0 = int(_token_decimals_from_pool(pool, "token0") or _erc20_decimals(chain_id, token0))
        dec1 = int(_token_decimals_from_pool(pool, "token1") or _erc20_decimals(chain_id, token1))
    except Exception as e:
        return [], f"exact2_sparse_points:decimals_failed:{e}", 0.0

    try:
        ratio01 = float(pool.get("token0Price") or 0.0) if pool.get("token0Price") is not None else 0.0
    except Exception:
        ratio01 = 0.0

    day_blocks: dict[int, int] = {}
    for ts, _ in fees_usd:
        d = int((int(ts) // 86400) * 86400)
        if d in day_blocks:
            continue
        try:
            day_blocks[d] = int(_llama_block_for_day(ck, d + 86399))
        except Exception:
            day_blocks[d] = 0

    n = len(fees_usd)
    if n <= 0:
        return [], "exact2_sparse_points:no_days", 0.0
    valid_idx = [i for i, (ts, _f) in enumerate(fees_usd) if int(day_blocks.get(int((int(ts) // 86400) * 86400), 0)) > 0]
    if not valid_idx:
        return [(int(ts), 0.0) for ts, _ in fees_usd], "exact2_sparse_points:no_day_blocks", 0.0

    if int(points) <= 0:
        picked_idx = list(valid_idx)
    else:
        p = max(2, min(int(points), len(valid_idx)))
        if p == 1:
            pick_positions = [0]
        else:
            pick_positions = sorted({int(round(i * (len(valid_idx) - 1) / (p - 1))) for i in range(p)})
        picked_idx = [valid_idx[pos] for pos in pick_positions if 0 <= pos < len(valid_idx)]
        picked_idx = sorted(set(picked_idx))

    deadline = time.monotonic() + max(8.0, float(budget_sec))
    out = [(int(ts), 0.0) for ts, _ in fees_usd]
    ok_points = 0
    for i in picked_idx:
        if time.monotonic() >= deadline:
            break
        ts = int(fees_usd[i][0])
        day_ts = int((ts // 86400) * 86400)
        b = int(day_blocks.get(day_ts, 0))
        if b <= 0:
            continue
        try:
            bal0 = int(_balance_of_raw(chain_id, token0, pool_id, b, timeout_sec=6.0))
            bal1 = int(_balance_of_raw(chain_id, token1, pool_id, b, timeout_sec=6.0))
        except Exception:
            continue
        amt0 = float(bal0) / float(10 ** max(0, dec0))
        amt1 = float(bal1) / float(10 ** max(0, dec1))
        p0 = float(_historical_price_usd(ck, token0, ts, int(day_start_ts), int(day_end_ts)))
        p1 = float(_historical_price_usd(ck, token1, ts, int(day_start_ts), int(day_end_ts)))
        if p0 <= 0 and p1 > 0 and ratio01 > 0:
            p0 = p1 * ratio01
        if p1 <= 0 and p0 > 0 and ratio01 > 0:
            p1 = p0 / ratio01
        tvl = max(0.0, amt0 * max(0.0, p0)) + max(0.0, amt1 * max(0.0, p1))
        if tvl > 0.0:
            out[i] = (ts, float(tvl))
            ok_points += 1

    cov = float(ok_points / max(1, n))
    mode = "all_days" if int(points) <= 0 else f"points={len(picked_idx)}"
    reason = (
        f"exact2_sparse_points:{mode}:ok={ok_points}:"
        f"cov={cov:.2f}:budget={float(budget_sec):.1f}:elapsed={max(0.0, time.monotonic()-run_t0):.1f}"
    )
    return out, reason, cov


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

    pool_tvl_now_usd, price_source, price_err, tvl_now_debug = _resolve_pool_tvl_now_onchain(pool, found_chain, found_endpoint)
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

    # Estimated compare: keep historical shape from subgraph raw TVL,
    # but anchor level with on-chain TVL-now (Alchemy).
    estimated_tvl_base = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
    estimated_fees_base = _rebuild_fees_cumulative(fees_usd, estimated_tvl_base)
    estimated_tvl = _append_now_tvl_anchor(estimated_tvl_base, float(pool_tvl_now_usd))
    estimated_fees = _append_now_fee_anchor(estimated_fees_base)
    tvl_active_now = float((tvl_now_debug or {}).get("tvl_active_window_usd") or 0.0)
    estimated_active_tvl: list[tuple[int, float]] = []
    estimated_active_fees: list[tuple[int, float]] = []
    exact_active_tvl: list[tuple[int, float]] = []
    exact_active_fees: list[tuple[int, float]] = []
    if tvl_active_now > 0.0:
        estimated_active_tvl_base = _build_estimated_tvl(fees_usd, raw_tvl, float(tvl_active_now))
        estimated_active_fees_base = _rebuild_fees_cumulative(fees_usd, estimated_active_tvl_base)
        estimated_active_tvl = _append_now_tvl_anchor(estimated_active_tvl_base, float(tvl_active_now))
        estimated_active_fees = _append_now_fee_anchor(estimated_active_fees_base)
        # exact-active branch is intentionally disabled by product decision:
        # keep only estimated active for strict compare.
        exact_active_tvl = []
        exact_active_fees = []
        if isinstance(tvl_now_debug, dict):
            tvl_now_debug["active_history_source"] = "disabled_exact_active_removed"

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

    exact_base = _normalize_exact_series_to_fees(
        fees_usd,
        list(exact_series or []),
        fallback_value=float(pool_tvl_now_usd),
    )
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
    allow_estimate_fill = str(os.environ.get("STRICT_EXACT_ALLOW_ESTIMATE_FILL", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    exact_effective = list(exact_base)
    if allow_estimate_fill and exact_base and len(exact_base) == len(estimated_tvl_base) and float(exact_cov) >= float(partial_min_cov):
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
        try:
            smooth_enabled = str(os.environ.get("STRICT_EXACT2_SMOOTH_ENABLE", "0")).strip().lower() in {
                "1", "true", "yes", "on"
            }
        except Exception:
            smooth_enabled = True
        try:
            smooth_max_missing_ratio = max(
                0.0,
                min(1.0, float(os.environ.get("STRICT_EXACT2_SMOOTH_MAX_MISSING_RATIO", "0.50"))),
            )
        except Exception:
            smooth_max_missing_ratio = 0.50
        smooth_filled_days = 0
        outlier_dropped_days = 0
        exact_for_output = list(exact_base)
        if smooth_enabled and missing_days > 0:
            smoothed, smooth_filled_days = _smooth_sparse_exact_series(
                exact_base,
                max_missing_ratio=float(smooth_max_missing_ratio),
            )
            if int(smooth_filled_days) > 0:
                exact_for_output = list(smoothed)
                missing_days = int(sum(1 for _ts, v in exact_for_output if float(v or 0.0) <= 0.0))
        try:
            outlier_filter_enabled = str(os.environ.get("STRICT_EXACT2_OUTLIER_FILTER_ENABLE", "1")).strip().lower() in {
                "1", "true", "yes", "on"
            }
        except Exception:
            outlier_filter_enabled = True
        if outlier_filter_enabled and exact_for_output:
            filtered, outlier_dropped_days = _suppress_exact_outliers(
                exact_for_output,
                estimated_series=estimated_tvl_base,
                pool_tvl_now_usd=float(pool_tvl_now_usd),
            )
            if int(outlier_dropped_days) > 0:
                exact_for_output = list(filtered)
                # Re-smooth only points we intentionally dropped as outliers.
                sm2, fill2 = _smooth_sparse_exact_series(
                    exact_for_output,
                    max_missing_ratio=float(smooth_max_missing_ratio),
                )
                if int(fill2) > 0:
                    exact_for_output = list(sm2)
                    smooth_filled_days += int(fill2)
                missing_days = int(sum(1 for _ts, v in exact_for_output if float(v or 0.0) <= 0.0))
        # Diagnostics only (no value re-scaling):
        # helps understand why exact partial level differs from estimated / TVL-now.
        ex_last = 0.0
        est_last = 0.0
        for _ts, _v in reversed(exact_for_output):
            if float(_v) > 0.0:
                ex_last = float(_v)
                break
        for _ts, _v in reversed(estimated_tvl_base):
            if float(_v) > 0.0:
                est_last = float(_v)
                break
        lvl_ratio_est = (float(ex_last) / max(1e-12, float(est_last))) if est_last > 0.0 else 0.0
        lvl_ratio_now = (float(ex_last) / max(1e-12, float(pool_tvl_now_usd))) if float(pool_tvl_now_usd) > 0.0 else 0.0
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
        sm = f":smoothed={int(smooth_filled_days)}" if int(smooth_filled_days) > 0 else ""
        od = f":outliers={int(outlier_dropped_days)}" if int(outlier_dropped_days) > 0 else ""
        lv = (
            f":lvl_ex_est={lvl_ratio_est:.2f}:lvl_ex_now={lvl_ratio_now:.2f}:"
            f"last_ex={ex_last:.2f}:last_est={est_last:.2f}:now={float(pool_tvl_now_usd):.2f}"
        )
        data_quality_reason = f"exact:partial_raw:cov={exact_cov:.2f}:missing={missing_days}{sm}{od}{lv}:reason={exact_reason}{flg}"
        final_tvl = []
        final_fees = []
        strict_exact_tvl_base = list(exact_for_output)
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

    # Keep exact full-pool compare sparse for performance/readability:
    # one point every N days (nearest day if target day missing).
    try:
        exact_step_days = max(1, int(os.environ.get("STRICT_V3_EXACT_STEP_DAYS", "5")))
    except Exception:
        exact_step_days = 5
    if strict_exact_tvl:
        strict_exact_tvl = _sparse_series_every_n_days(strict_exact_tvl, exact_step_days)
        strict_exact_tvl = _append_now_tvl_anchor(strict_exact_tvl, float(pool_tvl_now_usd))
    if strict_exact_fees:
        strict_exact_fees = _sparse_series_every_n_days(strict_exact_fees, exact_step_days)
        strict_exact_fees = _append_now_fee_anchor(strict_exact_fees)

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
        "strict_debug": {"v3_tvl_now": (tvl_now_debug or {})},
        "strict_compare_estimated_tvl": estimated_tvl,
        "strict_compare_estimated_fees": estimated_fees,
        "strict_compare_estimated_active_tvl": estimated_active_tvl,
        "strict_compare_estimated_active_fees": estimated_active_fees,
        "strict_compare_exact_tvl": strict_exact_tvl,
        "strict_compare_exact_active_tvl": exact_active_tvl,
        "strict_compare_exact_active_fees": exact_active_fees,
        "strict_compare_exact_fees": strict_exact_fees,
    }

    save_chart_data_json({str(pool.get("id") or ""): payload}, out_path)
    print(f"Saved strict output: {out_path}")


if __name__ == "__main__":
    main()
