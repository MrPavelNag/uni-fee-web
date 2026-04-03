#!/usr/bin/env python3
"""
Strict single-pool v3 agent.

- Requires TARGET_POOL_ID (pool contract address).
- Probes chains by pool id (no broad pair discovery).
- Produces both estimated and exact compare series.
- Outputs to data/pools_v3_{suffix}.json (same format as v3 agent).
"""

import argparse
import os
from datetime import datetime, timedelta

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V3_SUBGRAPHS
from agent_common import estimate_pool_tvl_usd_external_with_meta, pairs_to_filename_suffix, save_chart_data_json
from uniswap_client import get_graph_endpoint, graphql_query, query_pool_day_data
from agent_v3 import _build_exact_tvl_series_v3


def _is_eth_address(v: str) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("0x") and len(s) == 42


def _target_pool_id() -> str:
    raw = str(os.environ.get("TARGET_POOL_ID", "") or "").strip().lower()
    return raw if _is_eth_address(raw) else ""


def _include_chains() -> list[str]:
    include = [c.strip().lower() for c in str(os.environ.get("INCLUDE_CHAINS", "")).split(",") if c.strip()]
    if include:
        return include
    return sorted(set(UNISWAP_V3_SUBGRAPHS.keys()))


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
    data = graphql_query(endpoint, q, {"id": str(pool_id).lower()}, retries=1)
    pool = (data.get("data") or {}).get("pool")
    return pool if isinstance(pool, dict) and str(pool.get("id") or "") else None


def _compute_fee_and_raw_tvl(pool_id: str, endpoint: str) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
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
        save_chart_data_json({}, out_path)
        return
    os.environ["V3_EXACT_TVL_ENABLE"] = "1"
    os.environ["V3_EXACT_TVL_STRICT_MODE"] = "1"

    include_chains = _include_chains()
    print("Uniswap v3 strict agent")
    print(f"Target pool: {target_pool}")
    print("Probing chains:", ",".join(include_chains))

    found_pool = None
    found_chain = ""
    found_endpoint = ""
    for chain in include_chains:
        endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            continue
        try:
            p = _query_pool_by_id(endpoint, target_pool)
        except Exception:
            p = None
        if p:
            found_pool = p
            found_chain = chain
            found_endpoint = endpoint
            break

    if not found_pool:
        print("Pool not found on selected chains")
        save_chart_data_json({}, out_path)
        return

    pool = dict(found_pool)
    pool["chain"] = found_chain
    pool["version"] = "v3"
    t0 = str(((pool.get("token0") or {}).get("symbol") or "?"))
    t1 = str(((pool.get("token1") or {}).get("symbol") or "?"))
    pool["pair_label"] = f"{t0}/{t1}"

    pool_tvl_now_usd, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(pool, found_chain)
    if float(pool_tvl_now_usd) <= 0:
        print(f"external TVL unavailable: {price_err or 'unknown'}")
        save_chart_data_json({}, out_path)
        return
    pool["effectiveTvlUSD"] = float(pool_tvl_now_usd)
    pool["tvl_price_source"] = str(price_source or "external")

    fees_usd, raw_tvl = _compute_fee_and_raw_tvl(str(pool.get("id") or ""), found_endpoint)
    if not fees_usd:
        print("No day-level data for pool")
        save_chart_data_json({}, out_path)
        return

    estimated_tvl = _build_estimated_tvl(fees_usd, raw_tvl, float(pool_tvl_now_usd))
    estimated_fees = _rebuild_fees_cumulative(fees_usd, estimated_tvl)

    day_start_ts = int((int(fees_usd[0][0]) // 86400) * 86400)
    day_end_ts = int((int(fees_usd[-1][0]) // 86400) * 86400)
    exact_series, exact_reason, exact_cov = _build_exact_tvl_series_v3(
        chain=found_chain,
        pool=pool,
        fees_usd=fees_usd,
        baseline_tvl=estimated_tvl,
        day_start_ts=day_start_ts,
        day_end_ts=day_end_ts,
        budget_sec=float(os.environ.get("V3_EXACT_TVL_POOL_BUDGET_SEC", "180")),
    )

    strict_exact_ok = bool(exact_series and len(exact_series) == len(fees_usd) and all(float(v) > 0.0 for _ts, v in exact_series))
    if strict_exact_ok:
        data_quality = "exact"
        data_quality_reason = "ok"
        final_tvl = list(exact_series)
        final_fees = _rebuild_fees_cumulative(fees_usd, final_tvl)
        strict_exact_tvl = list(exact_series)
        strict_exact_fees = list(final_fees)
    else:
        data_quality = "strict_unavailable"
        data_quality_reason = f"strict_required:{exact_reason}:{exact_cov:.2f}"
        final_tvl = []
        final_fees = []
        strict_exact_tvl = list(exact_series) if exact_series else []
        strict_exact_fees = _rebuild_fees_cumulative(fees_usd, strict_exact_tvl) if strict_exact_tvl else []

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
