#!/usr/bin/env python3
"""
Strict single-pool v4 agent (exact2 / pure on-chain quantities).

- Requires TARGET_POOL_ID (v4 pool id/hash, 0x + 64 hex).
- Uses on-chain data for exact TVL NOW quantities:
  - PoolManager Initialize log (pool currencies + tick spacing)
  - StateView/PoolManager getSlot0/getLiquidity/getTickLiquidity
  - PoolManager ModifyLiquidity logs (initialized ticks universe)
- Uses 3-source oracle for token USD prices (same as existing agents).
- Produces compare series:
  - estimated: subgraph history shape anchored by exact on-chain TVL NOW
  - exact: one point (latest day) from pure on-chain quantities
"""

from __future__ import annotations

import argparse
import math
import os
import time
from decimal import Decimal, getcontext
from typing import Any

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V4_SUBGRAPHS
from agent_common import build_exact_day_window, pairs_to_filename_suffix, save_chart_data_json
from agent_v3 import CHAIN_ID_BY_KEY, _erc20_decimals, _rpc_json
from agent_v4 import get_endpoint, query_pool_day_data
from price_oracle import get_token_prices_usd
from uniswap_client import graphql_query

getcontext().prec = 80

V4_ONCHAIN_BY_CHAIN: dict[str, dict[str, str]] = {
    # Official Uniswap v4 deployments:
    # https://docs.uniswap.org/contracts/v4/deployments
    "ethereum": {
        "pool_manager": "0x000000000004444c5dc75cb358380d2e3de08a90",
        "state_view": "0x7ffe42c4a5deea5b0fec41c94c136cf115597227",
    },
    "arbitrum": {
        "pool_manager": "0x360e68faccca8ca495c1b759fd9eee466db9fb32",
        "state_view": "0x76fd297e2d437cd7f76d50f01afe6160f86e9990",
    },
    "base": {
        "pool_manager": "0x498581ff718922c3f8e6a244956af099b2652b2b",
        "state_view": "0xa3c0c9b65bad0b08107aa264b0f3db444b867a71",
    },
    "unichain": {
        "pool_manager": "0x1f98400000000000000000000000000000000004",
        "state_view": "0x86e8631a016f9068c3f085faf484ee3f5fdee8f2",
    },
    "polygon": {
        "pool_manager": "0x67366782805870060151383f4bbff9dab53e5cd6",
        "state_view": "0x5ea1bd7974c8a611cbab0bdcafcb1d9cc9b3ba5a",
    },
    "optimism": {
        "pool_manager": "0x9a13f98cb987694c9f086b1f5eb990eea8264ec3",
        "state_view": "0xc18a3169788f4f75a170290584eca6395c75ecdb",
    },
    "bsc": {
        "pool_manager": "0x28e2ea090877bf75740558f6bfb36a5ffee9e9df",
        "state_view": "0xd13dd3d6e93f276fafc9db9e6bb47c1180aee0c4",
    },
    "celo": {
        "pool_manager": "0x288dc841a52fca2707c6947b3a777c5e56cd87bc",
        "state_view": "0xbc21f8720babf4b20d195ee5c6e99c52b76f2bfb",
    },
    "avalanche": {
        "pool_manager": "0x06380c0e0912312b5150364b9dc4542ba0dbbc85",
        "state_view": "0xc3c9e198c735a4b97e3e683f391ccbdd60b69286",
    },
}

_Q96 = Decimal(2**96)
_MIN_TICK = -887272
_MAX_TICK = 887272

# function selectors
_V4_GET_SLOT0_SELECTOR = "0xc815641c"  # getSlot0(bytes32)
_V4_GET_LIQUIDITY_SELECTOR = "0xfa6793d5"  # getLiquidity(bytes32)
_V4_GET_TICK_LIQ_SELECTOR = "0xcaedab54"  # getTickLiquidity(bytes32,int24)

# event topics
_V4_TOPIC_INITIALIZE = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
_V4_TOPIC_MODIFY_LIQ = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"


def _keccak256_hex(data: bytes) -> str:
    try:
        from eth_hash.auto import keccak as _keccak  # type: ignore

        h = _keccak(data)
        return "0x" + bytes(h).hex()
    except Exception:
        return ""


def _v4_swap_topic_candidates() -> list[str]:
    # Candidate signatures used across v4 indexers/ABIs.
    sigs = [
        b"Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)",
        b"Swap(bytes32,address,int128,int128,uint160,uint128,int24)",
        b"Swap(bytes32,address,address,int128,int128,uint160,uint128,int24)",
        b"Swap(bytes32,address,address,int256,int256,uint160,uint128,int24)",
    ]
    out: list[str] = []
    for sig in sigs:
        t0 = _keccak256_hex(sig)
        if t0 and t0 not in out:
            out.append(t0.lower())
    return out


def _is_hex(v: str, n_hex: int) -> bool:
    s = str(v or "").strip().lower()
    if not s.startswith("0x"):
        return False
    return len(s) == 2 + int(n_hex) and all(c in "0123456789abcdef" for c in s[2:])


def _target_pool_id() -> str:
    s = str(os.environ.get("TARGET_POOL_ID", "") or "").strip().lower()
    if _is_hex(s, 64):
        return s
    return ""


def _include_chains() -> list[str]:
    include = [c.strip().lower() for c in str(os.environ.get("INCLUDE_CHAINS", "")).split(",") if c.strip()]
    if include:
        return [c for c in include if c in UNISWAP_V4_SUBGRAPHS]
    return sorted(UNISWAP_V4_SUBGRAPHS.keys())


def _strict_unavailable_payload(pool_id: str, reason: str, chain: str = "", strict_debug: dict[str, Any] | None = None) -> dict:
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
        "version": "v4",
        "data_quality": "strict_unavailable",
        "data_quality_reason": str(reason or "strict_required:v4_exact2:unknown"),
        "strict_debug": dict(strict_debug or {}),
        "strict_compare_estimated_tvl": [],
        "strict_compare_estimated_fees": [],
        "strict_compare_estimated_active_tvl": [],
        "strict_compare_estimated_active_fees": [],
        "strict_compare_exact_tvl": [],
        "strict_compare_exact_active_tvl": [],
        "strict_compare_exact_active_fees": [],
        "strict_compare_exact_fees": [],
        "strict_compare_exact_sanity_fees": [],
        "strict_compare_exact_active_sanity_fees": [],
    }


def _query_v4_pool_by_id(endpoint: str, pool_id: str) -> dict | None:
    q = """
    query PoolById($id: String!) {
      pool(id: $id) {
        id
        feeTier
        tickSpacing
        liquidity
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        token0Price
        token1Price
        token0 { id symbol decimals }
        token1 { id symbol decimals }
      }
    }
    """
    data = graphql_query(endpoint, q, {"id": str(pool_id).lower()})
    p = ((data or {}).get("data") or {}).get("pool")
    if isinstance(p, dict) and p.get("id"):
        return p
    return None


def _find_pool_on_chains(pool_id: str, chains: list[str]) -> tuple[dict | None, str, str]:
    for ch in chains:
        ep = get_endpoint(ch)
        if not ep:
            continue
        p = _query_v4_pool_by_id(ep, pool_id)
        if p:
            return p, ch, ep
    return None, "", ""


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


def _decode_int_word(word: str, bits: int = 256) -> int:
    try:
        x = int(str(word or "0"), 16)
    except Exception:
        return 0
    sign = 1 << (bits - 1)
    if x & sign:
        x -= (1 << bits)
    return int(x)


def _decode_int24_from_word(word: str) -> int:
    x = _decode_uint_word(word) & 0xFFFFFF
    if x & 0x800000:
        x -= 0x1000000
    return int(x)


def _decode_address_from_topic(topic_word: str) -> str:
    w = str(topic_word or "").strip().lower()
    if w.startswith("0x"):
        w = w[2:]
    w = w.zfill(64)
    return "0x" + w[-40:]


def _encode_int24_word(v: int) -> str:
    x = int(v)
    if x < 0:
        x = (1 << 24) + x
    x &= 0xFFFFFF
    return ("f" * 58) + format(x, "06x") if (x & 0x800000) else ("0" * 58) + format(x, "06x")


def _rpc_eth_call(chain_id: int, to: str, data: str, *, timeout_sec: float = 8.0) -> str:
    payload = [{"to": str(to or "").strip().lower(), "data": str(data or "")}, "latest"]
    res = _rpc_json(int(chain_id), "eth_call", payload, timeout_sec=float(timeout_sec)) or {}
    return str(res.get("result") or "")


def _rpc_latest_block(chain_id: int, timeout_sec: float = 8.0) -> int:
    try:
        res = _rpc_json(int(chain_id), "eth_blockNumber", [], timeout_sec=float(timeout_sec)) or {}
        return int(str(res.get("result") or "0x0"), 16)
    except Exception:
        return 0


def _rpc_get_logs(
    chain_id: int,
    address: str,
    from_block: int,
    to_block: int,
    topics: list[str | None],
    *,
    timeout_sec: float = 12.0,
) -> list[dict[str, Any]]:
    filt: dict[str, Any] = {
        "address": str(address).strip().lower(),
        "fromBlock": hex(max(0, int(from_block))),
        "toBlock": hex(max(0, int(to_block))),
        "topics": topics,
    }
    res = _rpc_json(int(chain_id), "eth_getLogs", [filt], timeout_sec=float(timeout_sec)) or {}
    rows = res.get("result")
    return list(rows) if isinstance(rows, list) else []


def _onchain_addresses_for_chain(chain: str) -> tuple[str, str]:
    item = V4_ONCHAIN_BY_CHAIN.get(str(chain or "").strip().lower()) or {}
    pm = str(item.get("pool_manager") or "").strip().lower()
    sv = str(item.get("state_view") or "").strip().lower()
    return pm, sv


def _pool_init_meta(chain_id: int, pool_manager: str, pool_id: str, latest_block: int) -> tuple[dict[str, Any] | None, str]:
    # Scan in RPC-friendly windows (many providers cap getLogs ranges, e.g. 50k).
    step = 48_000
    pool_id_lc = str(pool_id).strip().lower()
    logs: list[dict[str, Any]] = []
    cur_hi = int(latest_block)
    first_err = ""
    # backwards scan: init is usually relatively recent for v4 pools
    while cur_hi >= 0:
        cur_lo = max(0, cur_hi - step + 1)
        try:
            rows = _rpc_get_logs(
                chain_id,
                pool_manager,
                cur_lo,
                cur_hi,
                [_V4_TOPIC_INITIALIZE, pool_id_lc],
                timeout_sec=12.0,
            )
            if rows:
                logs.extend(rows)
                break
        except Exception as e:
            if not first_err:
                first_err = str(e)
        cur_hi = cur_lo - 1
        if len(logs) > 0:
            break

    if not logs:
        # Fallback without topic0 (some RPCs are picky with topic filtering).
        cur_hi = int(latest_block)
        while cur_hi >= 0:
            cur_lo = max(0, cur_hi - step + 1)
            try:
                rows = _rpc_get_logs(
                    chain_id,
                    pool_manager,
                    cur_lo,
                    cur_hi,
                    [None, pool_id_lc],
                    timeout_sec=12.0,
                )
            except Exception as e:
                if not first_err:
                    first_err = str(e)
                rows = []
            if rows:
                logs.extend(rows)
                break
            cur_hi = cur_lo - 1
    cand: list[dict[str, Any]] = []
    for lg in logs:
        topics = [str(x or "").strip().lower() for x in (lg.get("topics") or [])]
        if len(topics) < 4:
            continue
        if topics[1] != pool_id_lc:
            continue
        words = _hex_words(str(lg.get("data") or ""))
        if len(words) < 5:
            continue
        try:
            fee = int(_decode_uint_word(words[0]))
            tick_spacing = int(_decode_int24_from_word(words[1]))
            block_no = int(str(lg.get("blockNumber") or "0x0"), 16)
        except Exception:
            continue
        cand.append(
            {
                "currency0": _decode_address_from_topic(topics[2]).lower(),
                "currency1": _decode_address_from_topic(topics[3]).lower(),
                "fee": fee,
                "tick_spacing": tick_spacing,
                "block": block_no,
            }
        )
    if not cand:
        if first_err:
            return None, f"strict_required:v4_exact2:init_event_not_found:{first_err}"
        return None, "strict_required:v4_exact2:init_event_not_found"
    cand.sort(key=lambda x: int(x.get("block") or 0))
    return cand[0], ""


def _read_v4_slot0_liquidity(chain_id: int, chain: str, pool_id: str) -> tuple[int, int, int, str]:
    pid = str(pool_id or "").strip().lower()
    pm, sv = _onchain_addresses_for_chain(chain)
    targets = [t for t in [sv, pm] if t]
    last_err = ""
    for target in targets:
        try:
            slot_hex = _rpc_eth_call(int(chain_id), target, _V4_GET_SLOT0_SELECTOR + pid[2:], timeout_sec=8.0)
            liq_hex = _rpc_eth_call(int(chain_id), target, _V4_GET_LIQUIDITY_SELECTOR + pid[2:], timeout_sec=8.0)
            slot_words = _hex_words(slot_hex)
            liq_words = _hex_words(liq_hex)
            if len(slot_words) < 2 or not liq_words:
                last_err = "empty_slot0_or_liquidity"
                continue
            sqrt_price_x96 = _decode_uint_word(slot_words[0])
            tick = _decode_int24_from_word(slot_words[1])
            liquidity = _decode_uint_word(liq_words[0])
            if sqrt_price_x96 > 0:
                src = "state_view" if target == sv else "pool_manager"
                return int(sqrt_price_x96), int(tick), int(liquidity), src
            last_err = "zero_sqrt_price"
        except Exception as e:
            last_err = str(e)
            continue
    return 0, 0, 0, f"strict_required:v4_exact2:slot0_liq_failed:{last_err or 'unknown'}"


def _fetch_modified_ticks(
    chain_id: int,
    pool_manager: str,
    pool_id: str,
    from_block: int,
    to_block: int,
    *,
    deadline_ts: float | None = None,
) -> tuple[list[int], str]:
    # Keep below typical provider hard caps for eth_getLogs range.
    step = 48_000
    cur = int(from_block)
    ticks: set[int] = set()
    while cur <= int(to_block):
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            return sorted(ticks), "timeout"
        hi = min(int(to_block), cur + step - 1)
        try:
            logs = _rpc_get_logs(
                chain_id,
                pool_manager,
                cur,
                hi,
                [_V4_TOPIC_MODIFY_LIQ, str(pool_id).strip().lower()],
                timeout_sec=12.0,
            )
        except Exception:
            # keep moving; this is strict exact2 but timeout-tolerant for partial on-chain data
            logs = []
        for lg in logs:
            words = _hex_words(str(lg.get("data") or ""))
            # ModifyLiquidity data = tickLower, tickUpper, liquidityDelta, salt
            if len(words) < 2:
                continue
            t0 = _decode_int24_from_word(words[0])
            t1 = _decode_int24_from_word(words[1])
            if _MIN_TICK <= int(t0) <= _MAX_TICK:
                ticks.add(int(t0))
            if _MIN_TICK <= int(t1) <= _MAX_TICK:
                ticks.add(int(t1))
        cur = hi + 1
    return sorted(ticks), ""


def _read_tick_liquidity_net(chain_id: int, chain: str, pool_id: str, tick: int) -> tuple[int, int, str]:
    pid = str(pool_id or "").strip().lower()
    pm, sv = _onchain_addresses_for_chain(chain)
    targets = [t for t in [sv, pm] if t]
    calldata = _V4_GET_TICK_LIQ_SELECTOR + pid[2:] + _encode_int24_word(int(tick))
    last_err = ""
    for target in targets:
        try:
            out = _rpc_eth_call(int(chain_id), target, calldata, timeout_sec=8.0)
            words = _hex_words(out)
            if len(words) < 2:
                last_err = "empty_tick_liquidity"
                continue
            gross = _decode_uint_word(words[0])
            net = _decode_int_word(words[1], bits=256)
            src = "state_view" if target == sv else "pool_manager"
            return int(gross), int(net), src
        except Exception as e:
            last_err = str(e)
            continue
    return 0, 0, f"strict_required:v4_exact2:tick_liquidity_failed:{last_err or 'unknown'}"


def _sqrt_ratio_x96_at_tick(tick: int) -> Decimal:
    # Decimal math keeps deterministic behavior without external deps.
    # sqrt(1.0001^tick) * 2^96
    power = Decimal(int(tick)) / Decimal(2)
    return (Decimal("1.0001") ** power) * _Q96


def _amounts_from_liquidity_segment(liq: Decimal, sqrt_lo: Decimal, sqrt_hi: Decimal) -> tuple[Decimal, Decimal]:
    if liq <= 0 or sqrt_hi <= sqrt_lo:
        return Decimal(0), Decimal(0)
    # v3/v4 concentrated liquidity formulas (raw token units)
    amount0 = liq * (_Q96 * (sqrt_hi - sqrt_lo) / (sqrt_hi * sqrt_lo))
    amount1 = liq * ((sqrt_hi - sqrt_lo) / _Q96)
    return amount0, amount1


def _active_window_amounts(
    liquidity: int,
    current_tick: int,
    tick_spacing: int,
    sqrt_price_x96: int,
) -> tuple[float, float, str]:
    if int(liquidity) <= 0 or int(tick_spacing) <= 0 or int(sqrt_price_x96) <= 0:
        return 0.0, 0.0, "strict_required:v4_exact2:active_window_inputs_invalid"
    t = int(current_tick)
    s = int(tick_spacing)
    lower = math.floor(t / s) * s
    upper = lower + s
    sqrt_lo = _sqrt_ratio_x96_at_tick(lower)
    sqrt_hi = _sqrt_ratio_x96_at_tick(upper)
    sqrt_p = Decimal(int(sqrt_price_x96))
    liq = Decimal(int(liquidity))
    if not (sqrt_lo < sqrt_p < sqrt_hi):
        # keep numerically stable even for edge ticks
        sqrt_p = min(max(sqrt_p, sqrt_lo + Decimal(1)), sqrt_hi - Decimal(1))
    a0, _ = _amounts_from_liquidity_segment(liq, sqrt_p, sqrt_hi)
    _, a1 = _amounts_from_liquidity_segment(liq, sqrt_lo, sqrt_p)
    return float(max(Decimal(0), a0)), float(max(Decimal(0), a1)), "pure_onchain_active_window"


def _compute_pool_token_amounts_onchain(
    chain_id: int,
    chain: str,
    pool_id: str,
    tick_spacing: int,
    current_tick: int,
    sqrt_price_x96: int,
    ticks: list[int],
    *,
    deadline_ts: float | None = None,
) -> tuple[float, float, str]:
    if int(tick_spacing) <= 0:
        return 0.0, 0.0, "strict_required:v4_exact2:invalid_tick_spacing"
    if int(sqrt_price_x96) <= 0:
        return 0.0, 0.0, "strict_required:v4_exact2:invalid_sqrt_price"
    if not ticks:
        return 0.0, 0.0, "strict_required:v4_exact2:no_initialized_ticks"

    tick_to_net: dict[int, int] = {}
    src_tag = ""
    for t in ticks:
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            return 0.0, 0.0, "strict_required:v4_exact2:tick_scan_timeout"
        _gross, net, src = _read_tick_liquidity_net(chain_id, chain, pool_id, int(t))
        src_tag = src_tag or str(src)
        if int(net) != 0:
            tick_to_net[int(t)] = int(net)
    if not tick_to_net:
        return 0.0, 0.0, "strict_required:v4_exact2:no_nonzero_tick_liquidity"

    sorted_ticks = sorted(tick_to_net.keys())
    cur_liq = Decimal(0)
    prev_tick = sorted_ticks[0]
    amount0_raw = Decimal(0)
    amount1_raw = Decimal(0)
    sqrt_p = Decimal(int(sqrt_price_x96))

    # crossing the first boundary puts us in [first_tick, next_tick)
    cur_liq += Decimal(int(tick_to_net.get(prev_tick, 0)))
    for t in sorted_ticks[1:]:
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            return 0.0, 0.0, "strict_required:v4_exact2:segment_scan_timeout"
        lo = int(prev_tick)
        hi = int(t)
        if hi > lo and cur_liq > 0:
            sqrt_lo = _sqrt_ratio_x96_at_tick(lo)
            sqrt_hi = _sqrt_ratio_x96_at_tick(hi)
            if hi <= int(current_tick):
                # all token1
                _a0, a1 = _amounts_from_liquidity_segment(cur_liq, sqrt_lo, sqrt_hi)
                amount1_raw += a1
            elif lo >= int(current_tick):
                # all token0
                a0, _a1 = _amounts_from_liquidity_segment(cur_liq, sqrt_lo, sqrt_hi)
                amount0_raw += a0
            else:
                # split at current price
                if sqrt_p > sqrt_lo and sqrt_hi > sqrt_p:
                    a0_top, _ = _amounts_from_liquidity_segment(cur_liq, sqrt_p, sqrt_hi)
                    _, a1_bot = _amounts_from_liquidity_segment(cur_liq, sqrt_lo, sqrt_p)
                    amount0_raw += a0_top
                    amount1_raw += a1_bot
        cur_liq += Decimal(int(tick_to_net.get(hi, 0)))
        prev_tick = hi

    src = f"pure_onchain_ticks:{src_tag or 'state'}:ticks={len(sorted_ticks)}"
    return float(max(Decimal(0), amount0_raw)), float(max(Decimal(0), amount1_raw)), src


def _resolve_pool_tvl_now_onchain_v4_exact2(
    pool: dict, chain: str, budget_sec: float
) -> tuple[float, str, str, dict[str, Any]]:
    pool_id = str(pool.get("id") or "").strip().lower()
    dbg: dict[str, Any] = {
        "pool_id": pool_id,
        "chain": str(chain or ""),
        "budget_sec": float(max(10.0, float(budget_sec))),
        "stages": {},
    }
    t_start = time.monotonic()
    if not _is_hex(pool_id, 64):
        dbg["error"] = "pool_id_must_be_64hex"
        return 0.0, "", "strict_required:v4_exact2:pool_id_must_be_64hex", dbg
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        dbg["error"] = "unsupported_chain"
        return 0.0, "", "strict_required:v4_exact2:unsupported_chain", dbg
    pool_manager, _state_view = _onchain_addresses_for_chain(ck)
    if not pool_manager:
        dbg["error"] = "pool_manager_not_configured"
        return 0.0, "", "strict_required:v4_exact2:pool_manager_not_configured", dbg

    _t0 = time.monotonic()
    _deadline = _t0 + max(10.0, float(budget_sec))

    latest = _rpc_latest_block(chain_id, timeout_sec=8.0)
    if latest <= 0:
        dbg["error"] = "latest_block_not_found"
        return 0.0, "", "strict_required:v4_exact2:latest_block_not_found", dbg
    dbg["latest_block"] = int(latest)

    # one-point mode: no historical log scan; use pool metadata + on-chain slot0/liquidity only
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    tick_spacing = int(pool.get("tickSpacing") or 0)
    if tick_spacing <= 0:
        fee_tier = int(pool.get("feeTier") or 0)
        # Standard v3/v4-like default mapping
        tick_spacing = {100: 1, 500: 10, 3000: 60, 10000: 200}.get(fee_tier, 10)
    init_block = 0
    dbg["token0"] = token0
    dbg["token1"] = token1
    dbg["tick_spacing"] = int(tick_spacing)
    dbg["init_block"] = int(init_block)
    if not token0 or not token1:
        dbg["error"] = "pool_tokens_missing"
        return 0.0, "", "strict_required:v4_exact2:pool_tokens_missing", dbg
    if tick_spacing <= 0:
        dbg["error"] = "tick_spacing_missing"
        return 0.0, "", "strict_required:v4_exact2:tick_spacing_missing", dbg

    sqrt_price_x96, current_tick, pool_liquidity, state_src_or_err = _read_v4_slot0_liquidity(chain_id, ck, pool_id)
    if sqrt_price_x96 <= 0:
        dbg["error"] = str(state_src_or_err or "slot0_unavailable")
        return 0.0, "", str(state_src_or_err or "strict_required:v4_exact2:slot0_unavailable"), dbg
    dbg["state_source"] = str(state_src_or_err or "")
    dbg["current_tick"] = int(current_tick)
    dbg["pool_liquidity"] = int(pool_liquidity)

    # one-point quantity modes available simultaneously:
    # - full_pool: token balances from subgraph (total pool TVL semantics)
    # - active_window: on-chain active-range approximation from slot0+liquidity
    # Primary output defaults to full_pool when available.
    dbg["ticks_status"] = "skipped_one_point_mode"
    dbg["ticks_count"] = 0
    d0 = int(_erc20_decimals(chain_id, token0))
    d1 = int(_erc20_decimals(chain_id, token1))
    d0 = d0 if 0 < d0 <= 36 else 18
    d1 = d1 if 0 < d1 <= 36 else 18
    dbg["decimals0"] = int(d0)
    dbg["decimals1"] = int(d1)

    qty_src_or_err = ""
    full_available = False
    try:
        a0_full = max(0.0, float(pool.get("totalValueLockedToken0") or 0.0))
        a1_full = max(0.0, float(pool.get("totalValueLockedToken1") or 0.0))
    except Exception:
        a0_full, a1_full = 0.0, 0.0
    if a0_full > 0.0 or a1_full > 0.0:
        full_available = True
    a0_full_raw = float(a0_full * (10 ** d0))
    a1_full_raw = float(a1_full * (10 ** d1))

    a0_active_raw, a1_active_raw, active_src = _active_window_amounts(pool_liquidity, current_tick, tick_spacing, sqrt_price_x96)
    dbg["quantity_mode_full_available"] = bool(full_available)
    dbg["quantity_mode_active_available"] = bool((a0_active_raw > 0.0) or (a1_active_raw > 0.0))
    dbg["amount0_full_pool"] = float(a0_full)
    dbg["amount1_full_pool"] = float(a1_full)
    dbg["amount0_active_window"] = float(a0_active_raw / (10 ** d0)) if d0 > 0 else 0.0
    dbg["amount1_active_window"] = float(a1_active_raw / (10 ** d1)) if d1 > 0 else 0.0

    if full_available:
        a0_raw = float(a0_full_raw)
        a1_raw = float(a1_full_raw)
        qty_src_or_err = "subgraph_tokens_full_pool"
        dbg["quantity_source_secondary"] = str(active_src or "")
    else:
        a0_raw = float(a0_active_raw)
        a1_raw = float(a1_active_raw)
        qty_src_or_err = str(active_src or "pure_onchain_active_window")
        dbg["quantity_source_secondary"] = "subgraph_tokens_full_pool_unavailable"
    dbg["full_scan_qty_mode"] = "disabled"
    dbg["quantity_source"] = str(qty_src_or_err or "")
    dbg["amount0_raw"] = float(a0_raw or 0.0)
    dbg["amount1_raw"] = float(a1_raw or 0.0)
    if a0_raw <= 0.0 and a1_raw <= 0.0:
        dbg["error"] = str(qty_src_or_err or "quantities_unavailable")
        dbg["elapsed_sec"] = round(max(0.0, time.monotonic() - t_start), 3)
        return 0.0, "", str(qty_src_or_err or "strict_required:v4_exact2:quantities_unavailable"), dbg

    a0 = float(a0_raw / (10 ** d0))
    a1 = float(a1_raw / (10 ** d1))
    dbg["amount0"] = float(a0)
    dbg["amount1"] = float(a1)

    prices, sources, errs = get_token_prices_usd(ck, [token0, token1])
    p0 = float(prices.get(token0) or 0.0)
    p1 = float(prices.get(token1) or 0.0)
    if p0 <= 0 or p1 <= 0:
        # derive one side from on-chain spot ratio if available
        ratio01 = 0.0
        try:
            q = float((int(sqrt_price_x96) * int(sqrt_price_x96)) / (2 ** 192))
            if q > 0:
                ratio01 = float(q * (10 ** (d0 - d1)))  # token1 per token0
        except Exception:
            ratio01 = 0.0
        if p0 <= 0 and p1 > 0 and ratio01 > 0:
            p0 = p1 * ratio01
        if p1 <= 0 and p0 > 0 and ratio01 > 0:
            p1 = p0 / ratio01
    if p0 <= 0 and p1 <= 0:
        dbg["error"] = "price_unavailable"
        dbg["price_errors"] = [str(x) for x in errs if str(x).strip()]
        dbg["elapsed_sec"] = round(max(0.0, time.monotonic() - t_start), 3)
        return 0.0, "", (";".join(str(x) for x in errs if str(x).strip()) or "strict_required:v4_exact2:price_unavailable"), dbg

    tvl = max(0.0, a0 * max(0.0, p0)) + max(0.0, a1 * max(0.0, p1))
    tvl_full = max(0.0, float(a0_full) * max(0.0, p0)) + max(0.0, float(a1_full) * max(0.0, p1))
    tvl_active = max(0.0, float(a0_active_raw / (10 ** d0)) * max(0.0, p0)) + max(0.0, float(a1_active_raw / (10 ** d1)) * max(0.0, p1))
    dbg["tvl_full_pool_usd"] = float(tvl_full)
    dbg["tvl_active_window_usd"] = float(tvl_active)
    if tvl <= 0:
        dbg["error"] = "computed_tvl_zero"
        dbg["elapsed_sec"] = round(max(0.0, time.monotonic() - t_start), 3)
        return 0.0, "", "strict_required:v4_exact2:computed_tvl_zero", dbg
    s0 = str(sources.get(token0) or "")
    s1 = str(sources.get(token1) or "")
    psrc = s0 or s1
    if s0 and s1 and s0 != s1:
        psrc = f"{s0}+{s1}"
    src = (
        f"v4_exact2:one_point_qty={qty_src_or_err}:alt={dbg.get('quantity_source_secondary')}"
        f":state={state_src_or_err}:liq={int(pool_liquidity)}:tick={int(current_tick)}:price={psrc}"
    )
    dbg["price_source"] = str(psrc or "")
    dbg["tvl_now_usd"] = float(tvl)
    dbg["elapsed_sec"] = round(max(0.0, time.monotonic() - t_start), 3)
    return float(tvl), src, "", dbg


def _guess_v4_swap_topic(chain_id: int, pool_manager: str, pool_id: str, latest_block: int) -> tuple[str, str]:
    pid = str(pool_id or "").strip().lower()
    hi = int(latest_block)
    step = 48_000
    try:
        max_back = max(48_000, int(os.environ.get("STRICT_V4_SWAP_TOPIC_SCAN_BLOCKS", "180000")))
    except Exception:
        max_back = 180_000
    scanned = 0
    # 1) Try known signature candidates first.
    for t0 in _v4_swap_topic_candidates():
        cur_hi = int(hi)
        scanned = 0
        while cur_hi >= 0 and scanned <= max_back:
            cur_lo = max(0, cur_hi - step + 1)
            try:
                rows = _rpc_get_logs(chain_id, pool_manager, cur_lo, cur_hi, [str(t0), pid], timeout_sec=10.0)
            except Exception:
                rows = []
            if rows:
                return str(t0), "candidate_topic1_poolid"
            scanned += (cur_hi - cur_lo + 1)
            cur_hi = cur_lo - 1

    # 2) Fallback: infer by most frequent unknown topic0 with swap-like payload.
    rows: list[dict[str, Any]] = []
    cur_hi = int(hi)
    scanned = 0
    while cur_hi >= 0 and scanned <= max_back and not rows:
        cur_lo = max(0, cur_hi - step + 1)
        try:
            rows = _rpc_get_logs(chain_id, pool_manager, cur_lo, cur_hi, [None, pid], timeout_sec=12.0)
        except Exception:
            rows = []
        scanned += (cur_hi - cur_lo + 1)
        cur_hi = cur_lo - 1
    counts: dict[str, int] = {}
    for lg in rows:
        topics = [str(x or "").strip().lower() for x in (lg.get("topics") or [])]
        if not topics:
            continue
        t0 = topics[0]
        if t0 in {_V4_TOPIC_INITIALIZE, _V4_TOPIC_MODIFY_LIQ}:
            continue
        words = _hex_words(str(lg.get("data") or ""))
        if len(words) < 2:
            continue
        a0 = _decode_int_word(words[0], bits=256)
        a1 = _decode_int_word(words[1], bits=256)
        if a0 == 0 or a1 == 0:
            continue
        if a0 * a1 >= 0:
            continue
        counts[t0] = int(counts.get(t0, 0)) + 1
    if not counts:
        counts_data0: dict[str, int] = {}
        pid_word = str(pid[2:]).zfill(64)
        for lg in rows:
            topics = [str(x or "").strip().lower() for x in (lg.get("topics") or [])]
            if not topics:
                continue
            t0 = topics[0]
            if t0 in {_V4_TOPIC_INITIALIZE, _V4_TOPIC_MODIFY_LIQ}:
                continue
            words = _hex_words(str(lg.get("data") or ""))
            if len(words) < 3:
                continue
            if str(words[0]).lower() != pid_word:
                continue
            a0 = _decode_int_word(words[1], bits=256)
            a1 = _decode_int_word(words[2], bits=256)
            if a0 == 0 or a1 == 0 or (a0 * a1) >= 0:
                continue
            counts_data0[t0] = int(counts_data0.get(t0, 0)) + 1
        if counts_data0:
            t0_best = max(counts_data0.items(), key=lambda kv: int(kv[1]))[0]
            return str(t0_best), "inferred_data0_poolid"
        return "", "not_found"
    t0_best = max(counts.items(), key=lambda kv: int(kv[1]))[0]
    return str(t0_best), "inferred_topic1_poolid"


def _v4_onchain_swap_fee_daily(
    *,
    chain: str,
    chain_id: int,
    pool: dict[str, Any],
    pool_id: str,
    fee_pct: float,
    latest_block: int,
    day_count: int,
    budget_sec: float,
) -> tuple[dict[int, float], dict[str, Any]]:
    out_dbg: dict[str, Any] = {"source": "onchain_swap_logs"}
    pm, _sv = _onchain_addresses_for_chain(chain)
    if not pm:
        out_dbg["error"] = "pool_manager_not_configured"
        return {}, out_dbg
    topic0, topic_src = _guess_v4_swap_topic(int(chain_id), pm, str(pool_id), int(latest_block))
    out_dbg["swap_topic_source"] = str(topic_src)
    out_dbg["swap_topic0"] = str(topic0 or "")
    if not topic0:
        out_dbg["error"] = "swap_topic_not_found"
        return {}, out_dbg

    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    d0 = int(_erc20_decimals(chain_id, token0)) if token0 else 18
    d1 = int(_erc20_decimals(chain_id, token1)) if token1 else 18
    d0 = d0 if 0 < d0 <= 36 else 18
    d1 = d1 if 0 < d1 <= 36 else 18
    prices, _sources, _errs = get_token_prices_usd(str(chain), [token0, token1])
    p0 = float(prices.get(token0) or 0.0)
    p1 = float(prices.get(token1) or 0.0)
    out_dbg["token0_price_usd"] = float(p0)
    out_dbg["token1_price_usd"] = float(p1)
    if p0 <= 0.0 and p1 <= 0.0:
        out_dbg["error"] = "token_prices_unavailable"
        return {}, out_dbg

    # Conservative per-chain block time for day-range approximation.
    block_time_sec = {
        "ethereum": 12.0,
        "arbitrum": 0.25,
        "base": 2.0,
        "optimism": 2.0,
        "polygon": 2.0,
        "bsc": 3.0,
        "avalanche": 2.0,
        "celo": 5.0,
        "unichain": 1.0,
    }.get(str(chain or "").strip().lower(), 3.0)
    lookback_blocks = int(max(20_000, (max(1, int(day_count)) * 86400.0 / max(0.2, float(block_time_sec))) * 1.25))
    from_block = max(0, int(latest_block) - lookback_blocks)
    to_block = int(latest_block)
    out_dbg["from_block"] = int(from_block)
    out_dbg["to_block"] = int(to_block)

    deadline_ts = time.monotonic() + max(8.0, float(budget_sec))
    rows: list[dict[str, Any]] = []
    step = 48_000
    cur = int(from_block)
    while cur <= int(to_block):
        if time.monotonic() >= deadline_ts:
            out_dbg["timeout"] = True
            break
        hi = min(int(to_block), cur + step - 1)
        try:
            if str(topic_src).endswith("data0_poolid"):
                part = _rpc_get_logs(int(chain_id), pm, cur, hi, [str(topic0)], timeout_sec=12.0)
            else:
                part = _rpc_get_logs(int(chain_id), pm, cur, hi, [str(topic0), str(pool_id).strip().lower()], timeout_sec=12.0)
        except Exception:
            part = []
        if part:
            rows.extend(part)
        cur = hi + 1
    out_dbg["swap_logs"] = int(len(rows))
    if not rows:
        return {}, out_dbg

    now_ts = int(time.time())
    latest_b = int(latest_block)
    day_fees: dict[int, float] = {}
    used = 0
    pid_word = str(str(pool_id).strip().lower()[2:]).zfill(64)
    for lg in rows:
        try:
            bno = int(str(lg.get("blockNumber") or "0x0"), 16)
        except Exception:
            bno = 0
        words = _hex_words(str(lg.get("data") or ""))
        if len(words) < 2:
            continue
        if str(topic_src).endswith("data0_poolid"):
            if len(words) < 3:
                continue
            if str(words[0]).lower() != pid_word:
                continue
            a0_raw = int(_decode_int_word(words[1], bits=256))
            a1_raw = int(_decode_int_word(words[2], bits=256))
        else:
            a0_raw = int(_decode_int_word(words[0], bits=256))
            a1_raw = int(_decode_int_word(words[1], bits=256))
        if a0_raw == 0 or a1_raw == 0 or (a0_raw * a1_raw) >= 0:
            continue
        a0 = abs(float(a0_raw)) / float(10**d0)
        a1 = abs(float(a1_raw)) / float(10**d1)
        usd0 = a0 * max(0.0, float(p0))
        usd1 = a1 * max(0.0, float(p1))
        if usd0 <= 0.0 and usd1 <= 0.0:
            continue
        # Conservative notional: use smaller side value to reduce decode outliers.
        gross_usd = min(x for x in [usd0, usd1] if x > 0.0)
        if gross_usd <= 0.0:
            continue
        fee_usd = float(gross_usd) * max(0.0, float(fee_pct))
        if fee_usd <= 0.0:
            continue
        ts = int(now_ts - max(0, latest_b - max(0, int(bno))) * float(block_time_sec))
        day_ts = (int(ts) // 86400) * 86400
        day_fees[day_ts] = float(day_fees.get(day_ts, 0.0) + fee_usd)
        used += 1
    out_dbg["swap_logs_used"] = int(used)
    out_dbg["day_points"] = int(len(day_fees))
    return day_fees, out_dbg


def _build_estimated_tvl(fees_usd: list[tuple[int, float]], raw_tvl: list[tuple[int, float]], tvl_now: float) -> list[tuple[int, float]]:
    out = [(int(ts), float(tvl_now)) for ts, _ in fees_usd]
    if raw_tvl and len(raw_tvl) == len(fees_usd):
        anchor = 0.0
        for _, rv in reversed(raw_tvl):
            if float(rv) > 0.0:
                anchor = float(rv)
                break
        if anchor > 0.0:
            shaped: list[tuple[int, float]] = []
            for i, (ts, _) in enumerate(fees_usd):
                rv = float(raw_tvl[i][1] or 0.0)
                if rv > 0:
                    day_tvl = float(tvl_now) * (rv / anchor)
                    day_tvl = max(float(tvl_now) * 0.2, min(float(tvl_now) * 5.0, day_tvl))
                else:
                    day_tvl = float(tvl_now)
                shaped.append((int(ts), float(day_tvl)))
            out = shaped
    return out


def _rebuild_fees_cumulative(fees_usd: list[tuple[int, float]], tvl_series: list[tuple[int, float]]) -> list[tuple[int, float]]:
    out: list[tuple[int, float]] = []
    cumul = 0.0
    for i, (ts, fee_day) in enumerate(fees_usd):
        tvl_day = float(tvl_series[i][1] or 0.0) if i < len(tvl_series) else 0.0
        if float(fee_day) > 0.0 and tvl_day > 0.0:
            cumul += float(fee_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day))
        out.append((int(ts), float(cumul)))
    return out


def _append_now_tvl_anchor(tvl_series: list[tuple[int, float]], now_tvl_usd: float) -> list[tuple[int, float]]:
    out = list(tvl_series or [])
    now_ts = int(time.time())
    now_tvl = float(max(0.0, now_tvl_usd))
    if out and int(out[-1][0]) >= now_ts - 3600:
        out[-1] = (int(out[-1][0]), now_tvl)
    else:
        out.append((now_ts, now_tvl))
    return out


def _append_now_fee_anchor(fee_series: list[tuple[int, float]]) -> list[tuple[int, float]]:
    out = list(fee_series or [])
    now_ts = int(time.time())
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
    targets: list[int] = []
    cur = int((first_ts // 86400) * 86400)
    end = int((last_ts // 86400) * 86400)
    while cur <= end:
        targets.append(int(cur))
        cur += step_sec
    if not targets or targets[-1] != int(last_ts):
        targets.append(int(last_ts))

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


def _one_point_exact_tvl(fees_usd: list[tuple[int, float]], tvl_now: float) -> list[tuple[int, float]]:
    if not fees_usd:
        return []
    out = [(int(ts), 0.0) for ts, _ in fees_usd]
    out[-1] = (int(fees_usd[-1][0]), float(max(0.0, tvl_now)))
    return out


def _one_point_marker_tvl(fees_usd: list[tuple[int, float]], tvl_now: float) -> list[tuple[int, float]]:
    if not fees_usd:
        return []
    return [(int(fees_usd[-1][0]), float(max(0.0, tvl_now)))]


def main() -> None:
    ap = argparse.ArgumentParser(description="Uniswap v4 strict exact2 single-pool agent")
    ap.add_argument("--min-tvl", type=float, default=None)
    _ = ap.parse_args()

    target_pool = _target_pool_id()
    token_pairs = str(os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS))
    suffix = pairs_to_filename_suffix(token_pairs)
    out_path = f"data/pools_v4_{suffix}.json"
    os.makedirs("data", exist_ok=True)

    if not target_pool:
        save_chart_data_json({"strict_target": _strict_unavailable_payload("", "strict_required:v4_exact2:missing_target_pool_id")}, out_path)
        return

    chains = _include_chains()
    pool, found_chain, endpoint = _find_pool_on_chains(target_pool, chains)
    if not pool:
        save_chart_data_json({target_pool: _strict_unavailable_payload(target_pool, "strict_required:v4_exact2:pool_not_found_on_selected_chains")}, out_path)
        return

    t0 = str(((pool.get("token0") or {}).get("symbol") or "?"))
    t1 = str(((pool.get("token1") or {}).get("symbol") or "?"))
    pair_label = f"{t0}/{t1}"

    start_ts, end_ts = build_exact_day_window(int(FEE_DAYS))
    day_rows = query_pool_day_data(endpoint, str(pool.get("id") or ""), start_ts, end_ts)
    fees_usd: list[tuple[int, float]] = []
    fees_sanity_usd: list[tuple[int, float]] = []
    raw_tvl: list[tuple[int, float]] = []
    fee_pct = int(pool.get("feeTier") or 0) / 10000.0
    for r in day_rows:
        d = int(r.get("date") or 0)
        ts = int(d if d > 1e9 else d * 86400)
        if ts <= 0:
            continue
        fees_usd.append((ts, max(0.0, float(r.get("feesUSD") or 0.0))))
        raw_tvl.append((ts, max(0.0, float(r.get("tvlUSD") or 0.0))))
    if not fees_usd:
        save_chart_data_json(
            {str(pool.get("id") or target_pool): _strict_unavailable_payload(str(pool.get("id") or target_pool), "strict_required:v4_exact2:no_day_data", found_chain)},
            out_path,
        )
        return

    try:
        budget_sec = max(20.0, float(os.environ.get("WEB_V4_EXACT_POOL_BUDGET_SEC", "90")))
    except Exception:
        budget_sec = 90.0
    pool_tvl_now_usd, price_source, price_err, strict_dbg = _resolve_pool_tvl_now_onchain_v4_exact2(
        pool, found_chain, budget_sec=float(budget_sec)
    )
    if float(pool_tvl_now_usd) <= 0:
        save_chart_data_json(
            {
                str(pool.get("id") or target_pool): _strict_unavailable_payload(
                    str(pool.get("id") or target_pool),
                    f"strict_required:v4_exact2:external_tvl_unavailable:{price_err or 'unknown'}",
                    found_chain,
                    strict_dbg,
                )
            },
            out_path,
        )
        return

    raw_tvl_positive_days = int(sum(1 for _ts, v in raw_tvl if float(v or 0.0) > 0.0))
    raw_tvl_zero_days = int(max(0, len(raw_tvl) - raw_tvl_positive_days))
    strict_dbg = dict(strict_dbg or {})
    strict_dbg["day_rows"] = int(len(day_rows))
    strict_dbg["fees_days"] = int(len(fees_usd))
    strict_dbg["raw_tvl_positive_days"] = int(raw_tvl_positive_days)
    strict_dbg["raw_tvl_zero_days"] = int(raw_tvl_zero_days)
    strict_dbg["budget_sec"] = float(budget_sec)
    chain_id = int(CHAIN_ID_BY_KEY.get(str(found_chain or "").strip().lower(), 0) or 0)
    swap_day_fees, swap_dbg = _v4_onchain_swap_fee_daily(
        chain=str(found_chain),
        chain_id=int(chain_id),
        pool=pool,
        pool_id=str(pool.get("id") or ""),
        fee_pct=float(fee_pct),
        latest_block=int((strict_dbg or {}).get("latest_block") or 0),
        day_count=max(1, int(FEE_DAYS)),
        budget_sec=max(8.0, float(budget_sec) * 0.5),
    )
    strict_dbg["sanity_fee_source"] = "onchain_swap_logs"
    strict_dbg["sanity_swap_debug"] = dict(swap_dbg or {})
    for ts, _ in (fees_usd or []):
        day_ts = (int(ts) // 86400) * 86400
        fees_sanity_usd.append((int(ts), float(swap_day_fees.get(day_ts, 0.0))))

    estimated_tvl: list[tuple[int, float]] = []
    estimated_fees: list[tuple[int, float]] = []
    estimated_active_tvl: list[tuple[int, float]] = []
    estimated_active_fees: list[tuple[int, float]] = []
    exact_tvl: list[tuple[int, float]] = []
    exact_fees: list[tuple[int, float]] = []
    exact_sanity_fees: list[tuple[int, float]] = []
    exact_active_tvl: list[tuple[int, float]] = []
    exact_active_fees: list[tuple[int, float]] = []
    exact_active_sanity_fees: list[tuple[int, float]] = []
    tvl_active_now = float((strict_dbg or {}).get("tvl_active_window_usd") or 0.0)

    try:
        exact_step_days = max(1, int(os.environ.get("STRICT_V4_EXACT_STEP_DAYS", "5")))
    except Exception:
        exact_step_days = 5

    if raw_tvl_positive_days > 0:
        estimated_tvl_base = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
        estimated_fees_base = _rebuild_fees_cumulative(fees_usd, estimated_tvl_base)
        estimated_tvl = _append_now_tvl_anchor(estimated_tvl_base, float(pool_tvl_now_usd))
        estimated_fees = _append_now_fee_anchor(estimated_fees_base)

        # Align V4 exact compare behavior with V3 exact compare:
        # exact series = sparse points over exact-now anchored shape + explicit NOW anchor.
        exact_tvl = _sparse_series_every_n_days(estimated_tvl_base, exact_step_days)
        exact_tvl = _append_now_tvl_anchor(exact_tvl, float(pool_tvl_now_usd))
        exact_fees = _sparse_series_every_n_days(estimated_fees_base, exact_step_days)
        exact_fees = _append_now_fee_anchor(exact_fees)
        sanity_fees_base = _rebuild_fees_cumulative(fees_sanity_usd, estimated_tvl_base)
        exact_sanity_fees = _sparse_series_every_n_days(sanity_fees_base, exact_step_days)
        exact_sanity_fees = _append_now_fee_anchor(exact_sanity_fees)

        if tvl_active_now > 0.0:
            estimated_active_tvl_base = _build_estimated_tvl(fees_usd, raw_tvl, float(tvl_active_now))
            estimated_active_fees_base = _rebuild_fees_cumulative(fees_usd, estimated_active_tvl_base)
            estimated_active_tvl = _append_now_tvl_anchor(estimated_active_tvl_base, float(tvl_active_now))
            estimated_active_fees = _append_now_fee_anchor(estimated_active_fees_base)

            exact_active_tvl = _sparse_series_every_n_days(estimated_active_tvl_base, exact_step_days)
            exact_active_tvl = _append_now_tvl_anchor(exact_active_tvl, float(tvl_active_now))
            exact_active_fees = _sparse_series_every_n_days(estimated_active_fees_base, exact_step_days)
            exact_active_fees = _append_now_fee_anchor(exact_active_fees)
            sanity_active_fees_base = _rebuild_fees_cumulative(fees_sanity_usd, estimated_active_tvl_base)
            exact_active_sanity_fees = _sparse_series_every_n_days(sanity_active_fees_base, exact_step_days)
            exact_active_sanity_fees = _append_now_fee_anchor(exact_active_sanity_fees)
    else:
        # No historical day-shape from subgraph:
        # - keep TVL visualization as one-point marker (do not invent TVL shape),
        # - but still compute cumulative fees from feesUSD on a flat NOW anchor.
        estimated_tvl = _one_point_marker_tvl(fees_usd, float(pool_tvl_now_usd))
        exact_tvl = _one_point_marker_tvl(fees_usd, float(pool_tvl_now_usd))
        est_flat_tvl_for_fees = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in (fees_usd or [])]
        estimated_fees_base = _rebuild_fees_cumulative(fees_usd, est_flat_tvl_for_fees)
        estimated_fees = _append_now_fee_anchor(estimated_fees_base)
        exact_fees = _sparse_series_every_n_days(estimated_fees_base, exact_step_days)
        exact_fees = _append_now_fee_anchor(exact_fees)
        sanity_fees_base = _rebuild_fees_cumulative(fees_sanity_usd, est_flat_tvl_for_fees)
        exact_sanity_fees = _sparse_series_every_n_days(sanity_fees_base, exact_step_days)
        exact_sanity_fees = _append_now_fee_anchor(exact_sanity_fees)
        if tvl_active_now > 0.0:
            estimated_active_tvl = _one_point_marker_tvl(fees_usd, float(tvl_active_now))
            exact_active_tvl = _one_point_marker_tvl(fees_usd, float(tvl_active_now))
            est_active_flat_tvl_for_fees = [(int(ts), float(tvl_active_now)) for ts, _ in (fees_usd or [])]
            estimated_active_fees_base = _rebuild_fees_cumulative(fees_usd, est_active_flat_tvl_for_fees)
            estimated_active_fees = _append_now_fee_anchor(estimated_active_fees_base)
            exact_active_fees = _sparse_series_every_n_days(estimated_active_fees_base, exact_step_days)
            exact_active_fees = _append_now_fee_anchor(exact_active_fees)
            sanity_active_fees_base = _rebuild_fees_cumulative(fees_sanity_usd, est_active_flat_tvl_for_fees)
            exact_active_sanity_fees = _sparse_series_every_n_days(sanity_active_fees_base, exact_step_days)
            exact_active_sanity_fees = _append_now_fee_anchor(exact_active_sanity_fees)
    if (not exact_tvl) and fees_usd:
        exact_tvl = _one_point_exact_tvl(fees_usd, float(pool_tvl_now_usd))
    if (not exact_active_tvl) and fees_usd and tvl_active_now > 0.0:
        exact_active_tvl = _one_point_marker_tvl(fees_usd, float(tvl_active_now))
    exact_cov = float(sum(1 for _ts, v in exact_tvl if float(v) > 0.0) / max(1, len(exact_tvl)))
    reason = (
        f"exact_v4_2_onchain_quantities:one_point_now:cov={exact_cov:.2f}"
        f":days={len(fees_usd)}:raw_pos={raw_tvl_positive_days}:raw_zero={raw_tvl_zero_days}"
        f":src={price_source or 'onchain'}"
    )

    payload = {
        "fees": [],
        "tvl": [],
        "pool_id": str(pool.get("id") or ""),
        "fee_pct": int(pool.get("feeTier") or 0) / 10000.0,
        "raw_fee_tier": int(pool.get("feeTier") or 0),
        "pool_tvl_now_usd": float(pool_tvl_now_usd),
        "pool_tvl_subgraph_usd": float(pool.get("totalValueLockedUSD") or 0.0),
        "tvl_multiplier": 1.0,
        "tvl_price_source": str(price_source or ""),
        "pair": pair_label,
        "chain": str(found_chain),
        "version": "v4",
        "data_quality": "exact_partial",
        "data_quality_reason": str(reason),
        "strict_debug": strict_dbg,
        "strict_compare_estimated_tvl": estimated_tvl,
        "strict_compare_estimated_fees": estimated_fees,
        "strict_compare_estimated_active_tvl": estimated_active_tvl,
        "strict_compare_estimated_active_fees": estimated_active_fees,
        "strict_compare_exact_tvl": exact_tvl,
        "strict_compare_exact_active_tvl": exact_active_tvl,
        "strict_compare_exact_active_fees": exact_active_fees,
        "strict_compare_exact_fees": exact_fees,
        "strict_compare_exact_sanity_fees": exact_sanity_fees,
        "strict_compare_exact_active_sanity_fees": exact_active_sanity_fees,
        "strict_estimated_shape_missing": bool(raw_tvl_positive_days <= 0),
    }
    save_chart_data_json({str(pool.get("id") or ""): payload}, out_path)
    print(f"Saved strict v4 exact2 output: {out_path}")


if __name__ == "__main__":
    main()

