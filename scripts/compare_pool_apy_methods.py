#!/usr/bin/env python3
"""
Compare APY methods for one pool in shell mode.

Methods:
1) estimate_scaled_raw_tvl  - legacy estimate based on raw_tvl shape + current external TVL anchor
2) hybrid_anchors_external  - hybrid method (anchor points by historical prices, interpolation by raw_tvl)
3) exact_daily_amount_price - exact daily TVL from daily token amounts * historical prices (if available in subgraph)
"""

import argparse
import os
import sys
from datetime import datetime, timedelta
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import LP_ALLOCATION_USD
from price_oracle import get_token_prices_usd, get_token_prices_usd_at
from uniswap_client import get_graph_endpoint, graphql_query
from agent_common import estimate_pool_tvl_usd_external_with_meta


def _safe_float(v: Any) -> float:
    try:
        x = float(v or 0.0)
        return x if x > 0 else 0.0
    except Exception:
        return 0.0


def _to_ts(day_or_ts: int) -> int:
    d = int(day_or_ts or 0)
    return d if d > 1_000_000_000 else d * 86400


def _query_pool(endpoint: str, pool_id: str) -> dict[str, Any]:
    q = """
    query Pool($id: String!) {
      pool(id: $id) {
        id
        feeTier
        token0 { id symbol }
        token1 { id symbol }
        totalValueLockedUSD
        totalValueLockedToken0
        totalValueLockedToken1
        token0Price
        token1Price
      }
    }
    """
    data = graphql_query(endpoint, q, {"id": str(pool_id).strip().lower()})
    return (data.get("data") or {}).get("pool") or {}


def _query_pool_days(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> tuple[list[dict[str, Any]], bool]:
    q_with_amounts = """
    query PoolDayData($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      poolDayDatas(first: 100, skip: $skip, orderBy: date, orderDirection: asc, where: { pool: $pool, date_gte: $start, date_lte: $end }) {
        date
        feesUSD
        tvlUSD
        totalValueLockedToken0
        totalValueLockedToken1
      }
    }
    """
    q_basic = """
    query PoolDayData($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      poolDayDatas(first: 100, skip: $skip, orderBy: date, orderDirection: asc, where: { pool: $pool, date_gte: $start, date_lte: $end }) {
        date
        feesUSD
        tvlUSD
      }
    }
    """
    out: list[dict[str, Any]] = []
    skip = 0
    with_amounts = True
    while True:
        try:
            data = graphql_query(
                endpoint,
                q_with_amounts if with_amounts else q_basic,
                {"pool": str(pool_id).strip().lower(), "start": int(start_ts), "end": int(end_ts), "skip": int(skip)},
            )
        except Exception:
            if with_amounts:
                # Retry same page without amount fields.
                with_amounts = False
                continue
            raise
        rows = ((data.get("data") or {}).get("poolDayDatas") or [])
        if not rows:
            break
        out.extend(rows)
        if len(rows) < 100:
            break
        skip += 100
    return out, with_amounts


def _build_scaled_tvl_series(fees_usd: list[tuple[int, float]], raw_tvl: list[tuple[int, float]], pool_tvl_now_usd: float) -> list[tuple[int, float]]:
    if not fees_usd:
        return []
    if pool_tvl_now_usd <= 0:
        return [(int(ts), 0.0) for ts, _ in fees_usd]
    if not raw_tvl or len(raw_tvl) != len(fees_usd):
        return [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]
    anchor = 0.0
    for _ts, rv in reversed(raw_tvl):
        rvf = _safe_float(rv)
        if rvf > 0:
            anchor = rvf
            break
    if anchor <= 0:
        return [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]
    out: list[tuple[int, float]] = []
    for i, (ts, _f) in enumerate(fees_usd):
        rv = _safe_float(raw_tvl[i][1] if i < len(raw_tvl) else 0.0)
        if rv > 0:
            day_tvl = float(pool_tvl_now_usd) * (rv / anchor)
            day_tvl = max(float(pool_tvl_now_usd) * 0.2, min(float(pool_tvl_now_usd) * 5.0, day_tvl))
        else:
            day_tvl = float(pool_tvl_now_usd)
        out.append((int(ts), float(day_tvl)))
    return out


def _build_hybrid_tvl_series(
    chain: str,
    pool: dict[str, Any],
    fees_usd: list[tuple[int, float]],
    raw_tvl: list[tuple[int, float]],
    pool_tvl_now_usd: float,
    anchor_days: int,
) -> list[tuple[int, float]]:
    if not fees_usd or pool_tvl_now_usd <= 0:
        return []
    n = len(fees_usd)
    if n == 1:
        return [(int(fees_usd[0][0]), float(pool_tvl_now_usd))]
    raw_aligned = [float(raw_tvl[i][1] or 0.0) if i < len(raw_tvl) else 0.0 for i in range(n)]
    raw_ref = 0.0
    for rv in reversed(raw_aligned):
        if rv > 0:
            raw_ref = rv
            break
    if raw_ref <= 0:
        return [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]

    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    amt0_now = _safe_float(pool.get("totalValueLockedToken0"))
    amt1_now = _safe_float(pool.get("totalValueLockedToken1"))
    if (not token0) or (not token1) or (amt0_now <= 0 and amt1_now <= 0):
        return _build_scaled_tvl_series(fees_usd, raw_tvl, pool_tvl_now_usd)

    ratio01 = _safe_float(pool.get("token0Price"))
    spot_prices, _src, _err = get_token_prices_usd(chain, [token0, token1])
    p0_now = _safe_float(spot_prices.get(token0))
    p1_now = _safe_float(spot_prices.get(token1))
    if p0_now <= 0 and p1_now > 0 and ratio01 > 0:
        p0_now = p1_now * ratio01
    if p1_now <= 0 and p0_now > 0 and ratio01 > 0:
        p1_now = p0_now / ratio01
    if p0_now <= 0 and p1_now <= 0:
        return _build_scaled_tvl_series(fees_usd, raw_tvl, pool_tvl_now_usd)

    step = max(3, min(30, int(anchor_days)))
    anchor_idx = list(range(0, n, step))
    if anchor_idx[-1] != n - 1:
        anchor_idx.append(n - 1)
    anchors: dict[int, float] = {}
    for i in anchor_idx:
        rv = raw_aligned[i]
        ts = int(fees_usd[i][0])
        if rv <= 0:
            continue
        scale = rv / max(1e-12, raw_ref)
        amt0_day = amt0_now * scale
        amt1_day = amt1_now * scale
        hist_prices, _hsrc, _herr = get_token_prices_usd_at(chain, [token0, token1], ts)
        p0 = _safe_float(hist_prices.get(token0)) or p0_now
        p1 = _safe_float(hist_prices.get(token1)) or p1_now
        if p0 <= 0 and p1 > 0 and ratio01 > 0:
            p0 = p1 * ratio01
        if p1 <= 0 and p0 > 0 and ratio01 > 0:
            p1 = p0 / ratio01
        ext_tvl = max(0.0, amt0_day * max(0.0, p0)) + max(0.0, amt1_day * max(0.0, p1))
        if ext_tvl > 0:
            anchors[i] = float(ext_tvl)
    if len(anchors) < 2:
        return _build_scaled_tvl_series(fees_usd, raw_tvl, pool_tvl_now_usd)

    idxs = sorted(anchors.keys())
    out_vals: list[float] = [0.0] * n
    first = idxs[0]
    first_factor = anchors[first] / max(1e-12, raw_aligned[first])
    for i in range(0, first):
        rv = raw_aligned[i]
        out_vals[i] = (rv * first_factor) if rv > 0 else anchors[first]
    last = idxs[-1]
    last_factor = anchors[last] / max(1e-12, raw_aligned[last])
    for i in range(last + 1, n):
        rv = raw_aligned[i]
        out_vals[i] = (rv * last_factor) if rv > 0 else anchors[last]
    for seg in range(len(idxs) - 1):
        a = idxs[seg]
        b = idxs[seg + 1]
        a_ext = anchors[a]
        b_ext = anchors[b]
        a_raw = max(0.0, raw_aligned[a])
        b_raw = max(0.0, raw_aligned[b])
        span = max(1, b - a)
        for i in range(a, b + 1):
            w = float(i - a) / float(span)
            target = a_ext + (b_ext - a_ext) * w
            raw_interp = a_raw + (b_raw - a_raw) * w
            factor = target / max(1e-12, raw_interp) if raw_interp > 0 else 1.0
            rv = raw_aligned[i]
            v = (rv * factor) if rv > 0 else target
            out_vals[i] = max(0.0, float(v))
    return [(int(fees_usd[i][0]), float(out_vals[i])) for i in range(n)]


def _calc_income_and_apy(fees_usd: list[tuple[int, float]], tvl_usd: list[tuple[int, float]], alloc_usd: float, days: int) -> tuple[float, float]:
    n = min(len(fees_usd), len(tvl_usd))
    if n <= 0:
        return 0.0, 0.0
    cumul = 0.0
    for i in range(n):
        fees_day = _safe_float(fees_usd[i][1])
        tvl_day = _safe_float(tvl_usd[i][1])
        if fees_day > 0 and tvl_day > 0 and alloc_usd > 0:
            cumul += fees_day * (float(alloc_usd) / max(1e-12, tvl_day))
    apy_pct = (cumul / float(alloc_usd)) * (365.0 / max(1, int(days))) * 100.0 if alloc_usd > 0 else 0.0
    return float(cumul), float(apy_pct)


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare APY methods for one pool.")
    ap.add_argument("--chain", required=True, help="Chain key, e.g. ethereum")
    ap.add_argument("--version", required=True, choices=["v3", "v4"], help="Protocol version")
    ap.add_argument("--pool-id", required=True, help="Pool id")
    ap.add_argument("--days", type=int, default=31)
    ap.add_argument("--allocation-usd", type=float, default=float(LP_ALLOCATION_USD))
    ap.add_argument("--anchor-days", type=int, default=10)
    args = ap.parse_args()

    endpoint = get_graph_endpoint(args.chain, version=args.version)
    if not endpoint:
        raise SystemExit(f"No endpoint for chain={args.chain} version={args.version}. Check THE_GRAPH_API_KEY or overrides.")

    now = datetime.utcnow()
    start = now - timedelta(days=max(1, int(args.days)))
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    pool = _query_pool(endpoint, args.pool_id)
    if not pool:
        raise SystemExit("Pool not found in subgraph.")
    pool_tvl_now_usd, tvl_src, tvl_err = estimate_pool_tvl_usd_external_with_meta(pool, args.chain)
    if pool_tvl_now_usd <= 0:
        pool_tvl_now_usd = _safe_float(pool.get("totalValueLockedUSD"))
    rows, with_amounts = _query_pool_days(endpoint, args.pool_id, start_ts, end_ts)
    if not rows:
        raise SystemExit("No poolDayDatas rows for selected period.")

    fees_usd: list[tuple[int, float]] = []
    raw_tvl: list[tuple[int, float]] = []
    exact_tvl: list[tuple[int, float]] = []

    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    exact_possible = bool(with_amounts and token0 and token1)

    for r in rows:
        ts = _to_ts(int(r.get("date") or 0))
        f = _safe_float(r.get("feesUSD"))
        rv = _safe_float(r.get("tvlUSD"))
        fees_usd.append((ts, f))
        raw_tvl.append((ts, rv))
        if exact_possible:
            a0 = _safe_float(r.get("totalValueLockedToken0"))
            a1 = _safe_float(r.get("totalValueLockedToken1"))
            pmap, _src, _err = get_token_prices_usd_at(args.chain, [token0, token1], ts)
            p0 = _safe_float(pmap.get(token0))
            p1 = _safe_float(pmap.get(token1))
            if p0 <= 0 and p1 <= 0:
                exact_tvl.append((ts, 0.0))
            else:
                exact_tvl.append((ts, max(0.0, a0 * p0) + max(0.0, a1 * p1)))

    tvl_scaled = _build_scaled_tvl_series(fees_usd, raw_tvl, pool_tvl_now_usd)
    tvl_hybrid = _build_hybrid_tvl_series(args.chain, pool, fees_usd, raw_tvl, pool_tvl_now_usd, args.anchor_days)
    income_scaled, apy_scaled = _calc_income_and_apy(fees_usd, tvl_scaled, args.allocation_usd, args.days)
    income_hybrid, apy_hybrid = _calc_income_and_apy(fees_usd, tvl_hybrid, args.allocation_usd, args.days)

    print(f"pool_id={args.pool_id.lower()} chain={args.chain} version={args.version} days={args.days} allocation_usd={args.allocation_usd:.2f}")
    print(f"pool_tvl_now_usd={pool_tvl_now_usd:.6f} tvl_source={tvl_src or '-'} tvl_error={tvl_err or '-'}")
    print(f"day_rows={len(rows)} day_amount_fields={'yes' if with_amounts else 'no'}")
    print("")
    print("method=estimate_scaled_raw_tvl")
    print(f"  final_income_usd={income_scaled:.6f}")
    print(f"  apy_pct={apy_scaled:.6f}")
    print("")
    print("method=hybrid_anchors_external")
    print(f"  final_income_usd={income_hybrid:.6f}")
    print(f"  apy_pct={apy_hybrid:.6f}")
    print("")

    if exact_possible and exact_tvl and any(v > 0 for _, v in exact_tvl):
        income_exact, apy_exact = _calc_income_and_apy(fees_usd, exact_tvl, args.allocation_usd, args.days)
        zero_days = len([1 for _ts, v in exact_tvl if v <= 0])
        print("method=exact_daily_amount_price")
        print(f"  final_income_usd={income_exact:.6f}")
        print(f"  apy_pct={apy_exact:.6f}")
        print(f"  zero_tvl_days={zero_days}")
    else:
        print("method=exact_daily_amount_price")
        reason = "subgraph_poolDayData_missing_amount_fields" if not with_amounts else "historical_prices_or_amounts_missing"
        print("  status=N/A")
        print(f"  reason={reason}")


if __name__ == "__main__":
    main()

