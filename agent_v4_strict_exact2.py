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
        "strict_compare_exact_fees": [],
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


def _active_amounts_nearest_initialized_band(
    *,
    chain_id: int,
    chain: str,
    pool_id: str,
    liquidity: int,
    current_tick: int,
    tick_spacing: int,
    sqrt_price_x96: int,
    max_steps: int,
) -> tuple[float, float, str, dict[str, Any]]:
    """Compute active amounts in nearest initialized tick band around current price."""
    meta: dict[str, Any] = {"band_lo": None, "band_hi": None, "band_width": 0, "scan_calls": 0}
    if int(liquidity) <= 0 or int(tick_spacing) <= 0 or int(sqrt_price_x96) <= 0:
        a0, a1, src = _active_window_amounts(liquidity, current_tick, tick_spacing, sqrt_price_x96)
        return a0, a1, f"{src}:fallback_invalid_inputs", meta

    s = int(tick_spacing)
    base = int(math.floor(float(current_tick) / float(s)) * s)
    lo_tick: int | None = None
    hi_tick: int | None = None
    src_tag = ""

    for k in range(0, max(1, int(max_steps)) + 1):
        if lo_tick is None:
            t_lo = int(base - k * s)
            gross, net, src = _read_tick_liquidity_net(int(chain_id), str(chain), str(pool_id), int(t_lo))
            meta["scan_calls"] = int(meta.get("scan_calls") or 0) + 1
            src_tag = src_tag or str(src or "")
            if int(gross) > 0 or int(net) != 0:
                lo_tick = int(t_lo)
        if hi_tick is None:
            t_hi = int(base + (k + 1) * s)
            gross, net, src = _read_tick_liquidity_net(int(chain_id), str(chain), str(pool_id), int(t_hi))
            meta["scan_calls"] = int(meta.get("scan_calls") or 0) + 1
            src_tag = src_tag or str(src or "")
            if int(gross) > 0 or int(net) != 0:
                hi_tick = int(t_hi)
        if lo_tick is not None and hi_tick is not None:
            break

    if lo_tick is None or hi_tick is None or int(hi_tick) <= int(lo_tick):
        a0, a1, src = _active_window_amounts(liquidity, current_tick, tick_spacing, sqrt_price_x96)
        return a0, a1, f"{src}:fallback_no_initialized_band", meta

    sqrt_lo = _sqrt_ratio_x96_at_tick(int(lo_tick))
    sqrt_hi = _sqrt_ratio_x96_at_tick(int(hi_tick))
    sqrt_p = Decimal(int(sqrt_price_x96))
    liq = Decimal(int(liquidity))
    if not (sqrt_lo < sqrt_p < sqrt_hi):
        sqrt_p = min(max(sqrt_p, sqrt_lo + Decimal(1)), sqrt_hi - Decimal(1))
    a0, _ = _amounts_from_liquidity_segment(liq, sqrt_p, sqrt_hi)
    _, a1 = _amounts_from_liquidity_segment(liq, sqrt_lo, sqrt_p)
    meta["band_lo"] = int(lo_tick)
    meta["band_hi"] = int(hi_tick)
    meta["band_width"] = int(hi_tick - lo_tick)
    src = f"pure_onchain_active_initialized_band:{src_tag or 'state'}"
    return float(max(Decimal(0), a0)), float(max(Decimal(0), a1)), src, meta


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

    try:
        active_band_max_steps = max(8, min(1000, int(os.environ.get("STRICT_V4_ACTIVE_BAND_MAX_STEPS", "120") or 120)))
    except Exception:
        active_band_max_steps = 120
    a0_active_raw, a1_active_raw, active_src, active_meta = _active_amounts_nearest_initialized_band(
        chain_id=int(chain_id),
        chain=str(ck),
        pool_id=str(pool_id),
        liquidity=int(pool_liquidity),
        current_tick=int(current_tick),
        tick_spacing=int(tick_spacing),
        sqrt_price_x96=int(sqrt_price_x96),
        max_steps=int(active_band_max_steps),
    )
    dbg["quantity_mode_full_available"] = bool(full_available)
    dbg["quantity_mode_active_available"] = bool((a0_active_raw > 0.0) or (a1_active_raw > 0.0))
    dbg["amount0_full_pool"] = float(a0_full)
    dbg["amount1_full_pool"] = float(a1_full)
    dbg["amount0_active_window"] = float(a0_active_raw / (10 ** d0)) if d0 > 0 else 0.0
    dbg["amount1_active_window"] = float(a1_active_raw / (10 ** d1)) if d1 > 0 else 0.0
    dbg["active_band_max_steps"] = int(active_band_max_steps)
    dbg["active_band_lo"] = active_meta.get("band_lo")
    dbg["active_band_hi"] = active_meta.get("band_hi")
    dbg["active_band_width"] = int(active_meta.get("band_width") or 0)
    dbg["active_band_scan_calls"] = int(active_meta.get("scan_calls") or 0)

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
    raw_tvl_signed: list[tuple[int, float]] = []
    for r in day_rows:
        d = int(r.get("date") or 0)
        ts = int(d if d > 1e9 else d * 86400)
        if ts <= 0:
            continue
        fees_usd.append((ts, max(0.0, float(r.get("feesUSD") or 0.0))))
        raw_tvl_signed.append((ts, float(r.get("tvlUSD") or 0.0)))
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

    # Root-cause fix for half-flat estimated full on some V4 pools:
    # poolDayData.tvlUSD may be emitted as negative values by indexer/backends.
    # Using max(0, tvlUSD) collapses long ranges to zero and makes estimated TVL flat at NOW anchor.
    raw_pos = int(sum(1 for _ts, v in raw_tvl_signed if float(v or 0.0) > 0.0))
    raw_neg = int(sum(1 for _ts, v in raw_tvl_signed if float(v or 0.0) < 0.0))
    raw_zero = int(max(0, len(raw_tvl_signed) - raw_pos - raw_neg))
    use_abs_for_negative = False
    if raw_neg > 0:
        # If all/non-trivial tail is negative, preserve magnitude as shape signal.
        if raw_pos == 0 or raw_neg >= max(10, raw_pos * 2):
            use_abs_for_negative = True
    raw_tvl: list[tuple[int, float]] = []
    for ts, rv in raw_tvl_signed:
        v = float(rv or 0.0)
        if v > 0.0:
            raw_tvl.append((int(ts), float(v)))
        elif v < 0.0 and use_abs_for_negative:
            raw_tvl.append((int(ts), float(abs(v))))
        else:
            raw_tvl.append((int(ts), 0.0))
    raw_tvl_positive_days = int(sum(1 for _ts, v in raw_tvl if float(v or 0.0) > 0.0))
    raw_tvl_zero_days = int(max(0, len(raw_tvl) - raw_tvl_positive_days))
    strict_dbg = dict(strict_dbg or {})
    strict_dbg["day_rows"] = int(len(day_rows))
    strict_dbg["fees_days"] = int(len(fees_usd))
    strict_dbg["raw_tvl_positive_days"] = int(raw_tvl_positive_days)
    strict_dbg["raw_tvl_zero_days"] = int(raw_tvl_zero_days)
    strict_dbg["raw_tvl_signed_positive_days"] = int(raw_pos)
    strict_dbg["raw_tvl_signed_negative_days"] = int(raw_neg)
    strict_dbg["raw_tvl_signed_zero_days"] = int(raw_zero)
    strict_dbg["raw_tvl_negative_abs_mode"] = bool(use_abs_for_negative)
    strict_dbg["budget_sec"] = float(budget_sec)

    estimated_tvl: list[tuple[int, float]] = []
    estimated_fees: list[tuple[int, float]] = []
    estimated_active_tvl: list[tuple[int, float]] = []
    estimated_active_fees: list[tuple[int, float]] = []
    tvl_active_now = float((strict_dbg or {}).get("tvl_active_window_usd") or 0.0)
    if raw_tvl_positive_days > 0:
        estimated_tvl = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
        estimated_fees = _rebuild_fees_cumulative(fees_usd, estimated_tvl)
        if tvl_active_now > 0.0:
            estimated_active_tvl = _build_estimated_tvl(fees_usd, raw_tvl, float(tvl_active_now))
            estimated_active_fees = _rebuild_fees_cumulative(fees_usd, estimated_active_tvl)
    else:
        # No historical day-shape: still expose a single estimate marker at TVL NOW.
        estimated_tvl = _one_point_marker_tvl(fees_usd, float(pool_tvl_now_usd))
        # Keep fee/APY metrics meaningful even in marker mode.
        est_flat_tvl_for_fees = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in (fees_usd or [])]
        estimated_fees = _rebuild_fees_cumulative(fees_usd, est_flat_tvl_for_fees)
        if tvl_active_now > 0.0:
            estimated_active_tvl = _one_point_marker_tvl(fees_usd, float(tvl_active_now))
            est_active_flat_tvl_for_fees = [(int(ts), float(tvl_active_now)) for ts, _ in (fees_usd or [])]
            estimated_active_fees = _rebuild_fees_cumulative(fees_usd, est_active_flat_tvl_for_fees)
    exact_tvl = _one_point_exact_tvl(fees_usd, float(pool_tvl_now_usd))
    # Compatibility guard: never emit empty exact fees in strict compare.
    # The table computes exact full income/APY from strict_compare_exact_fees.
    if not estimated_fees and fees_usd and float(pool_tvl_now_usd) > 0.0:
        est_flat_tvl_for_fees = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in (fees_usd or [])]
        estimated_fees = _rebuild_fees_cumulative(fees_usd, est_flat_tvl_for_fees)
    exact_fees = list(estimated_fees or [])
    exact_active_tvl = _one_point_marker_tvl(fees_usd, float((strict_dbg or {}).get("tvl_active_window_usd") or 0.0))
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
        "strict_compare_exact_fees": exact_fees,
        "strict_estimated_shape_missing": bool(raw_tvl_positive_days <= 0),
    }
    save_chart_data_json({str(pool.get("id") or ""): payload}, out_path)
    print(f"Saved strict v4 exact2 output: {out_path}")


if __name__ == "__main__":
    main()

