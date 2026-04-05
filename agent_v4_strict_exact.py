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


def _resolve_pool_tvl_now_onchain_v4(pool: dict, chain: str) -> tuple[float, str, str]:
    # For v4 strict agent, use only one Alchemy-backed on-chain point for TVL NOW.
    # Requires a pool address-like id; bytes32 pool ids cannot be queried via ERC20 balanceOf directly.
    pool_id = str(pool.get("id") or "").strip().lower()
    if not _is_hex(pool_id, 40):
        return 0.0, "", "strict_required:v4:alchemy_tvl_now_requires_address_pool_id"
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
    try:
        d0 = int(((pool.get("token0") or {}).get("decimals") or 0))
        d1 = int(((pool.get("token1") or {}).get("decimals") or 0))
        if d0 <= 0 or d0 > 36:
            d0 = int(_erc20_decimals(chain_id, token0))
        if d1 <= 0 or d1 > 36:
            d1 = int(_erc20_decimals(chain_id, token1))
        b0 = int(_balance_of_raw(chain_id, token0, pool_id, latest, timeout_sec=8.0))
        b1 = int(_balance_of_raw(chain_id, token1, pool_id, latest, timeout_sec=8.0))
    except Exception as e:
        return 0.0, "", f"strict_required:v4:onchain_balance_failed:{e}"
    a0 = float(b0) / float(10 ** max(0, d0))
    a1 = float(b1) / float(10 ** max(0, d1))
    prices, sources, errs = get_token_prices_usd(ck, [token0, token1])
    p0 = float(prices.get(token0) or 0.0)
    p1 = float(prices.get(token1) or 0.0)
    try:
        ratio01 = float(pool.get("token0Price") or 0.0)
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

