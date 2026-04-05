#!/usr/bin/env python3
"""
Strict single-pool v4 agent.

- Requires TARGET_POOL_ID (v4 pool id/hash).
- Uses subgraph for day-level fees/history.
- Uses Alchemy RPC only for one on-chain point: TVL NOW.
- Produces compare series:
  - estimated: subgraph history shape anchored by on-chain TVL NOW
  - exact: one point (latest day) from on-chain TVL NOW
"""

from __future__ import annotations

import argparse
import os
from typing import Any

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V4_SUBGRAPHS
from agent_common import build_exact_day_window, pairs_to_filename_suffix, save_chart_data_json
from agent_v3 import CHAIN_ID_BY_KEY, _balance_of_raw, _erc20_decimals, _rpc_json
from agent_v4 import get_endpoint, query_pool_day_data
from price_oracle import get_token_prices_usd
from uniswap_client import graphql_query

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

# Function selectors for v4 pool state reads by poolId (bytes32).
# getSlot0(bytes32) -> (sqrtPriceX96, tick, protocolFee, lpFee)
_V4_GET_SLOT0_SELECTOR = "0xc815641c"
# getLiquidity(bytes32) -> uint128
_V4_GET_LIQUIDITY_SELECTOR = "0xfa6793d5"


def _is_hex(v: str, n_hex: int) -> bool:
    s = str(v or "").strip().lower()
    if not s.startswith("0x"):
        return False
    return len(s) == 2 + int(n_hex) and all(c in "0123456789abcdef" for c in s[2:])


def _target_pool_id() -> str:
    s = str(os.environ.get("TARGET_POOL_ID", "") or "").strip().lower()
    # v4 pool id is typically bytes32 (0x + 64 hex). Keep address support as fallback.
    if _is_hex(s, 64) or _is_hex(s, 40):
        return s
    return ""


def _is_pool_id_64(v: str) -> bool:
    return _is_hex(str(v or "").strip().lower(), 64)


def _include_chains() -> list[str]:
    include = [c.strip().lower() for c in str(os.environ.get("INCLUDE_CHAINS", "")).split(",") if c.strip()]
    if include:
        return [c for c in include if c in UNISWAP_V4_SUBGRAPHS]
    return sorted(UNISWAP_V4_SUBGRAPHS.keys())


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
        "version": "v4",
        "data_quality": "strict_unavailable",
        "data_quality_reason": str(reason or "strict_required:v4:unknown"),
        "strict_compare_estimated_tvl": [],
        "strict_compare_estimated_fees": [],
        "strict_compare_exact_tvl": [],
        "strict_compare_exact_fees": [],
    }


def _query_v4_pool_by_id(endpoint: str, pool_id: str) -> dict | None:
    q = """
    query PoolById($id: String!) {
      pool(id: $id) {
        id
        feeTier
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


def _rpc_eth_call(chain_id: int, to: str, data: str, *, timeout_sec: float = 8.0) -> str:
    payload = [{"to": str(to or "").strip().lower(), "data": str(data or "")}, "latest"]
    res = _rpc_json(int(chain_id), "eth_call", payload, timeout_sec=float(timeout_sec)) or {}
    return str(res.get("result") or "")


def _onchain_addresses_for_chain(chain: str) -> tuple[str, str]:
    item = V4_ONCHAIN_BY_CHAIN.get(str(chain or "").strip().lower()) or {}
    pm = str(item.get("pool_manager") or "").strip().lower()
    sv = str(item.get("state_view") or "").strip().lower()
    return pm, sv


def _read_v4_slot0_liquidity(chain_id: int, chain: str, pool_id: str) -> tuple[int, int, str]:
    pid = str(pool_id or "").strip().lower()
    if not _is_hex(pid, 64):
        return 0, 0, "strict_required:v4:pool_id_must_be_64hex"
    pm, sv = _onchain_addresses_for_chain(chain)
    if not sv and not pm:
        return 0, 0, "strict_required:v4:onchain_contracts_not_configured"

    targets = [t for t in [sv, pm] if t]
    last_err = ""
    for target in targets:
        try:
            slot_hex = _rpc_eth_call(int(chain_id), target, _V4_GET_SLOT0_SELECTOR + pid[2:], timeout_sec=8.0)
            liq_hex = _rpc_eth_call(int(chain_id), target, _V4_GET_LIQUIDITY_SELECTOR + pid[2:], timeout_sec=8.0)
            slot_words = _hex_words(slot_hex)
            liq_words = _hex_words(liq_hex)
            sqrt_price_x96 = _decode_uint_word(slot_words[0]) if slot_words else 0
            liquidity = _decode_uint_word(liq_words[0]) if liq_words else 0
            if sqrt_price_x96 > 0:
                src = "state_view" if target == sv else "pool_manager"
                return int(sqrt_price_x96), int(liquidity), src
            last_err = "zero_slot0"
        except Exception as e:
            last_err = str(e)
            continue
    return 0, 0, f"strict_required:v4:onchain_pool_state_failed:{last_err or 'unknown'}"


def _resolve_pool_tvl_now_onchain_v4(pool: dict, chain: str) -> tuple[float, str, str]:
    # For v4 strict agent, use one Alchemy-backed on-chain point for TVL NOW.
    # For bytes32 pool ids, read slot0/liquidity via StateView/PoolManager.
    pool_id = str(pool.get("id") or "").strip().lower()
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return 0.0, "", "strict_required:v4:unsupported_chain"
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if not token0 or not token1:
        return 0.0, "", "strict_required:v4:missing_pool_tokens"
    try:
        latest_hex = str((_rpc_json(chain_id, "eth_blockNumber", [], timeout_sec=8.0) or {}).get("result") or "0x0")
        latest = int(latest_hex, 16)
    except Exception as e:
        return 0.0, "", f"strict_required:v4:latest_block_failed:{e}"
    if latest <= 0:
        return 0.0, "", "strict_required:v4:latest_block_not_found"
    sqrt_price_x96 = 0
    pool_liquidity = 0
    state_source = ""
    if _is_hex(pool_id, 64):
        sqrt_price_x96, pool_liquidity, state_source_or_err = _read_v4_slot0_liquidity(chain_id, ck, pool_id)
        if sqrt_price_x96 <= 0:
            return 0.0, "", str(state_source_or_err or "strict_required:v4:onchain_pool_state_unavailable")
        state_source = str(state_source_or_err or "state_view")

    # token quantities:
    # - bytes32 v4 pool id: use subgraph token balances (no direct per-pool ERC20 balanceOf path)
    # - address-like pool id fallback: use direct balanceOf on pool address
    if _is_hex(pool_id, 64):
        try:
            a0 = max(0.0, float(pool.get("totalValueLockedToken0") or 0.0))
            a1 = max(0.0, float(pool.get("totalValueLockedToken1") or 0.0))
        except Exception:
            a0, a1 = 0.0, 0.0
    else:
        try:
            d0 = int(((pool.get("token0") or {}).get("decimals") or 0))
            d1 = int(((pool.get("token1") or {}).get("decimals") or 0))
            if d0 <= 0 or d0 > 36:
                d0 = int(_erc20_decimals(chain_id, token0))
            if d1 <= 0 or d1 > 36:
                d1 = int(_erc20_decimals(chain_id, token1))
            b0 = int(_balance_of_raw(chain_id, token0, pool_id, latest, timeout_sec=8.0))
            b1 = int(_balance_of_raw(chain_id, token1, pool_id, latest, timeout_sec=8.0))
            a0 = float(b0) / float(10 ** max(0, d0))
            a1 = float(b1) / float(10 ** max(0, d1))
        except Exception as e:
            return 0.0, "", f"strict_required:v4:onchain_balance_failed:{e}"
    if a0 <= 0.0 and a1 <= 0.0:
        return 0.0, "", "strict_required:v4:token_amounts_unavailable"
    prices, sources, errs = get_token_prices_usd(ck, [token0, token1])
    p0 = float(prices.get(token0) or 0.0)
    p1 = float(prices.get(token1) or 0.0)
    try:
        ratio01 = float(pool.get("token0Price") or 0.0)
        if ratio01 <= 0 and sqrt_price_x96 > 0:
            q = float((int(sqrt_price_x96) * int(sqrt_price_x96)) / (2 ** 192))
            if q > 0:
                # pool token0Price in subgraph is token1 per token0.
                dec0 = int(((pool.get("token0") or {}).get("decimals") or 18))
                dec1 = int(((pool.get("token1") or {}).get("decimals") or 18))
                ratio01 = float(q * (10 ** (dec0 - dec1)))
    except Exception:
        ratio01 = 0.0
    if p0 <= 0 and p1 > 0 and ratio01 > 0:
        p0 = p1 * ratio01
    if p1 <= 0 and p0 > 0 and ratio01 > 0:
        p1 = p0 / ratio01
    if p0 <= 0 and p1 <= 0:
        return 0.0, "", (";".join(str(x) for x in errs if str(x).strip()) or "strict_required:v4:price_unavailable")
    tvl = max(0.0, a0 * max(0.0, p0)) + max(0.0, a1 * max(0.0, p1))
    s0 = str(sources.get(token0) or "")
    s1 = str(sources.get(token1) or "")
    src = s0 or s1
    if s0 and s1 and s0 != s1:
        src = f"{s0}+{s1}"
    if _is_hex(pool_id, 64):
        return float(tvl), f"onchain_{state_source}:qty=subgraph_tokens:liq={int(pool_liquidity)}:price={src}", ""
    return float(tvl), f"onchain_balance*price:{src}", ""


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


def main() -> None:
    ap = argparse.ArgumentParser(description="Uniswap v4 strict exact single-pool agent")
    ap.add_argument("--min-tvl", type=float, default=None)
    _ = ap.parse_args()

    target_pool = _target_pool_id()
    token_pairs = str(os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS))
    suffix = pairs_to_filename_suffix(token_pairs)
    out_path = f"data/pools_v4_{suffix}.json"
    os.makedirs("data", exist_ok=True)

    if not target_pool:
        save_chart_data_json({"strict_target": _strict_unavailable_payload("", "strict_required:v4:missing_target_pool_id")}, out_path)
        return
    if not _is_pool_id_64(target_pool):
        save_chart_data_json(
            {
                "strict_target": _strict_unavailable_payload(
                    target_pool,
                    "strict_required:v4:target_pool_id_must_be_pool_id_64hex",
                )
            },
            out_path,
        )
        return

    chains = _include_chains()
    pool, found_chain, endpoint = _find_pool_on_chains(target_pool, chains)
    if not pool:
        save_chart_data_json({target_pool: _strict_unavailable_payload(target_pool, "strict_required:v4:pool_not_found_on_selected_chains")}, out_path)
        return

    t0 = str(((pool.get("token0") or {}).get("symbol") or "?"))
    t1 = str(((pool.get("token1") or {}).get("symbol") or "?"))
    pair_label = f"{t0}/{t1}"

    start_ts, end_ts = build_exact_day_window(int(FEE_DAYS))
    day_rows = query_pool_day_data(endpoint, str(pool.get("id") or ""), start_ts, end_ts)
    fees_usd: list[tuple[int, float]] = []
    raw_tvl: list[tuple[int, float]] = []
    for r in day_rows:
        d = int(r.get("date") or 0)
        ts = int(d if d > 1e9 else d * 86400)
        if ts <= 0:
            continue
        fees_usd.append((ts, max(0.0, float(r.get("feesUSD") or 0.0))))
        raw_tvl.append((ts, max(0.0, float(r.get("tvlUSD") or 0.0))))
    if not fees_usd:
        save_chart_data_json(
            {str(pool.get("id") or target_pool): _strict_unavailable_payload(str(pool.get("id") or target_pool), "strict_required:v4:no_day_data", found_chain)},
            out_path,
        )
        return

    pool_tvl_now_usd, price_source, price_err = _resolve_pool_tvl_now_onchain_v4(pool, found_chain)
    if float(pool_tvl_now_usd) <= 0:
        save_chart_data_json(
            {
                str(pool.get("id") or target_pool): _strict_unavailable_payload(
                    str(pool.get("id") or target_pool),
                    f"strict_required:v4:external_tvl_unavailable:{price_err or 'unknown'}",
                    found_chain,
                )
            },
            out_path,
        )
        return

    estimated_tvl = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
    estimated_fees = _rebuild_fees_cumulative(fees_usd, estimated_tvl)
    exact_tvl = _one_point_exact_tvl(fees_usd, float(pool_tvl_now_usd))
    exact_cov = float(sum(1 for _ts, v in exact_tvl if float(v) > 0.0) / max(1, len(exact_tvl)))
    reason = f"exact_v4_onchain_now_only:points=1:cov={exact_cov:.2f}:src={price_source or 'onchain'}"

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
        "strict_compare_estimated_tvl": estimated_tvl,
        "strict_compare_estimated_fees": estimated_fees,
        "strict_compare_exact_tvl": exact_tvl,
        "strict_compare_exact_fees": [],
    }
    save_chart_data_json({str(pool.get("id") or ""): payload}, out_path)
    print(f"Saved strict v4 output: {out_path}")


if __name__ == "__main__":
    main()

