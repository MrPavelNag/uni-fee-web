#!/usr/bin/env python3
"""
Agent Smart: selects the most attractive pools from v3/v4 outputs.

- Reads data/pools_v3_{suffix}.json and data/pools_v4_{suffix}.json
- Computes a simple risk-adjusted score
- Writes data/pools_smart_{suffix}.json (merge-compatible format)
"""

from __future__ import annotations

import argparse
import math
import os

from agent_common import load_chart_data_json, pairs_to_filename_suffix, save_chart_data_json
from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, MIN_TVL_USD


def _min_tvl(cli_value: float | None = None) -> float:
    if cli_value is not None:
        return float(cli_value)
    raw = os.environ.get("MIN_TVL")
    if raw is not None:
        try:
            return float(raw)
        except ValueError:
            pass
    return float(MIN_TVL_USD)


def _final_cumulative_income(pool: dict) -> float:
    fees = pool.get("fees") or []
    if not fees:
        return 0.0
    try:
        return float(fees[-1][1] or 0.0)
    except Exception:
        return 0.0


def _last_tvl(pool: dict) -> float:
    tvl = pool.get("tvl") or []
    if tvl:
        try:
            return float(tvl[-1][1] or 0.0)
        except Exception:
            pass
    try:
        return float(pool.get("pool_tvl_now_usd") or 0.0)
    except Exception:
        return 0.0


def _series_stability(pool: dict) -> float:
    """
    Fraction of non-decreasing steps in cumulative fee series.
    """
    fees = pool.get("fees") or []
    if len(fees) < 2:
        return 1.0
    non_decreasing = 0
    for i in range(1, len(fees)):
        prev = float(fees[i - 1][1] or 0.0)
        cur = float(fees[i][1] or 0.0)
        if cur >= prev:
            non_decreasing += 1
    return float(non_decreasing) / float(len(fees) - 1)


def _apy_pct(pool: dict) -> float:
    income = _final_cumulative_income(pool)
    if LP_ALLOCATION_USD <= 0 or FEE_DAYS <= 0:
        return 0.0
    return (income / float(LP_ALLOCATION_USD)) * (365.0 / float(FEE_DAYS)) * 100.0


def _smart_score(pool: dict) -> float:
    """
    Score combines profitability, TVL size and smoothness.
    """
    income = max(0.0, _final_cumulative_income(pool))
    tvl = max(0.0, _last_tvl(pool))
    stability = max(0.0, min(1.0, _series_stability(pool)))
    apy = max(0.0, _apy_pct(pool))
    apy_factor = 1.0 + min(2.0, apy / 100.0)
    tvl_factor = math.log10(1.0 + tvl)
    return income * tvl_factor * (0.5 + 0.5 * stability) * apy_factor


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent Smart: rank v3/v4 pools and emit smart subset")
    parser.add_argument("--min-tvl", type=float, default=None, help="Min TVL USD")
    args = parser.parse_args()

    pairs_str = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    suffix = pairs_to_filename_suffix(pairs_str)
    min_tvl = _min_tvl(args.min_tvl)
    max_pools = max(1, int(os.environ.get("SMART_MAX_POOLS", "30")))

    v3_path = f"data/pools_v3_{suffix}.json"
    v4_path = f"data/pools_v4_{suffix}.json"
    out_path = f"data/pools_smart_{suffix}.json"

    print("Agent Smart")
    print("Token pairs:", pairs_str, "| Min TVL: $%.0f" % min_tvl, "| Max pools:", max_pools)

    v3_data = load_chart_data_json(v3_path)
    v4_data = load_chart_data_json(v4_path)
    combined = {**v3_data, **v4_data}

    if not combined:
        print("No source data. Run agent_v3.py and/or agent_v4.py first.")
        save_chart_data_json({}, out_path)
        print("Done:", out_path)
        return

    ranked = []
    for pool_id, payload in combined.items():
        tvl_now = _last_tvl(payload)
        if tvl_now < float(min_tvl):
            continue
        score = _smart_score(payload)
        ranked.append((score, pool_id, payload))
    ranked.sort(key=lambda x: x[0], reverse=True)
    ranked = ranked[:max_pools]

    smart_data: dict[str, dict] = {}
    for score, pool_id, payload in ranked:
        key = f"smart::{pool_id}"
        row = dict(payload)
        row["version"] = f"smart-{payload.get('version', 'v3')}"
        row["smart_score"] = float(score)
        row["smart_rank_reason"] = "score=income*tvl*stability*apy"
        smart_data[key] = row

    os.makedirs("data", exist_ok=True)
    save_chart_data_json(smart_data, out_path)
    print(f"Selected {len(smart_data)} smart pools")
    print("Done:", out_path)


if __name__ == "__main__":
    main()
