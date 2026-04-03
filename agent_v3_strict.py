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
import time
from datetime import datetime, timedelta
from typing import Any

from config import DEFAULT_TOKEN_PAIRS, FEE_DAYS, LP_ALLOCATION_USD, UNISWAP_V3_SUBGRAPHS
from agent_common import estimate_pool_tvl_usd_external_with_meta, pairs_to_filename_suffix, save_chart_data_json
from uniswap_client import get_graph_endpoint, graphql_query, query_pool_day_data
from agent_v3 import (
    CHAIN_ID_BY_KEY,
    _build_exact_tvl_series_v3,
    _balance_of_raw,
    _erc20_decimals,
    _historical_price_usd,
    _llama_block_for_day,
    _rpc_json,
)


TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


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


def _exact_variant() -> str:
    v = str(os.environ.get("STRICT_EXACT_VARIANT", "exact2") or "").strip().lower()
    if v in {"exact1", "legacy"}:
        return "exact1"
    return "exact2"


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


def _fetch_logs_adaptive(
    chain_id: int,
    token: str,
    topics: list[Any],
    from_block: int,
    to_block: int,
    deadline_ts: float | None = None,
) -> list[dict]:
    start = int(max(1, from_block))
    end = int(max(start, to_block))
    logs: list[dict] = []
    step = int(max(2000, int(os.environ.get("V3_EXACT2_LOG_BLOCK_STEP", "50000"))))
    min_step = int(max(200, int(os.environ.get("V3_EXACT2_LOG_MIN_STEP", "1000"))))
    cursor = start
    while cursor <= end:
        if deadline_ts is not None and time.monotonic() >= float(deadline_ts):
            raise TimeoutError("exact2_budget_timeout:eth_getLogs")
        hi = min(end, cursor + step - 1)
        params = [{
            "address": str(token).strip().lower(),
            "fromBlock": hex(int(cursor)),
            "toBlock": hex(int(hi)),
            "topics": topics,
        }]
        try:
            data = _rpc_json(int(chain_id), "eth_getLogs", params, timeout_sec=22.0)
            part = data.get("result") or []
            if isinstance(part, list):
                logs.extend([x for x in part if isinstance(x, dict)])
            cursor = hi + 1
            if len(part) < 500 and step < 200000:
                step = int(step * 1.2)
        except Exception:
            if step <= min_step:
                raise
            step = max(min_step, step // 2)
    return logs


def _collect_pool_transfer_deltas(
    chain_id: int,
    token: str,
    pool_id: str,
    from_block: int,
    to_block: int,
    deadline_ts: float | None = None,
) -> dict[int, int]:
    pool_topic = _addr_topic(pool_id)
    by_block: dict[int, int] = {}
    for topics, sign in (
        ([TRANSFER_TOPIC, pool_topic], -1),  # from pool
        ([TRANSFER_TOPIC, None, pool_topic], 1),  # to pool
    ):
        logs = _fetch_logs_adaptive(
            int(chain_id),
            str(token),
            topics,
            int(from_block),
            int(to_block),
            deadline_ts=deadline_ts,
        )
        for lg in logs:
            bn = _safe_hex_to_int(lg.get("blockNumber"))
            if bn <= 0:
                continue
            val = _safe_hex_to_int(lg.get("data"))
            if val <= 0:
                continue
            by_block[bn] = int(by_block.get(bn, 0)) + int(sign * val)
    return by_block


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


def _build_exact2_tvl_series_v3_ledger(
    *,
    chain: str,
    pool: dict,
    fees_usd: list[tuple[int, float]],
    day_start_ts: int,
    day_end_ts: int,
    budget_sec: float,
) -> tuple[list[tuple[int, float]], str, float]:
    deadline_ts = time.monotonic() + max(10.0, float(budget_sec))
    ck = str(chain or "").strip().lower()
    chain_id = int(CHAIN_ID_BY_KEY.get(ck, 0) or 0)
    if chain_id <= 0:
        return [], "exact2_unsupported_chain", 0.0
    pool_id = str(pool.get("id") or "").strip().lower()
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if not (pool_id and token0 and token1):
        return [], "exact2_missing_pool_tokens", 0.0

    try:
        latest_block = _latest_block_number(chain_id)
        dec0 = int(_erc20_decimals(chain_id, token0))
        dec1 = int(_erc20_decimals(chain_id, token1))
        bal0_latest = int(_balance_of_raw(chain_id, token0, pool_id, latest_block))
        bal1_latest = int(_balance_of_raw(chain_id, token1, pool_id, latest_block))
    except Exception as e:
        return [], f"exact2_latest_point_failed:{e}", 0.0

    day_blocks: dict[int, int] = {}
    block_missing = 0
    for ts, _ in fees_usd:
        if time.monotonic() >= deadline_ts:
            return [], "exact2_budget_timeout:block_resolution", 0.0
        day_ts = int((int(ts) // 86400) * 86400)
        if day_ts in day_blocks:
            continue
        try:
            day_blocks[day_ts] = int(_llama_block_for_day(ck, day_ts + 86399))
        except Exception:
            block_missing += 1
            day_blocks[day_ts] = 0

    valid_blocks = [int(b) for b in day_blocks.values() if int(b) > 0]
    if not valid_blocks:
        return [], "exact2_no_day_blocks", 0.0
    from_block = int(min(valid_blocks))
    to_block = int(latest_block)
    try:
        deltas0 = _collect_pool_transfer_deltas(chain_id, token0, pool_id, from_block, to_block, deadline_ts=deadline_ts)
        deltas1 = _collect_pool_transfer_deltas(chain_id, token1, pool_id, from_block, to_block, deadline_ts=deadline_ts)
    except TimeoutError:
        return [], "exact2_budget_timeout:transfer_logs", 0.0
    except Exception as e:
        return [], f"exact2_transfer_logs_failed:{e}", 0.0

    boundaries = sorted(set(valid_blocks), reverse=True)
    bal0_by_block = _balances_by_boundaries_desc(bal0_latest, deltas0, boundaries)
    bal1_by_block = _balances_by_boundaries_desc(bal1_latest, deltas1, boundaries)

    tvl_series: list[tuple[int, float]] = []
    non_zero = 0
    priced_days = 0
    for ts, _fees in fees_usd:
        if time.monotonic() >= deadline_ts:
            return tvl_series, "exact2_budget_timeout:pricing", float(non_zero / max(1, len(fees_usd)))
        day_ts = int((int(ts) // 86400) * 86400)
        b = int(day_blocks.get(day_ts, 0))
        if b <= 0:
            tvl_series.append((int(ts), 0.0))
            continue
        amt0 = float(bal0_by_block.get(b, 0)) / float(10 ** max(0, dec0))
        amt1 = float(bal1_by_block.get(b, 0)) / float(10 ** max(0, dec1))
        p0 = float(_historical_price_usd(ck, token0, int(ts), int(day_start_ts), int(day_end_ts)))
        p1 = float(_historical_price_usd(ck, token1, int(ts), int(day_start_ts), int(day_end_ts)))
        if p0 > 0 or p1 > 0:
            priced_days += 1
        tvl = max(0.0, amt0 * max(0.0, p0)) + max(0.0, amt1 * max(0.0, p1))
        if tvl > 0:
            non_zero += 1
        tvl_series.append((int(ts), float(tvl)))

    cov = float(non_zero / max(1, len(fees_usd)))
    if non_zero == 0:
        return tvl_series, f"exact2_non_positive_days:block_missing={block_missing}:priced_days={priced_days}", cov
    if block_missing > 0:
        return tvl_series, f"exact2_partial:block_missing={block_missing}:priced_days={priced_days}", cov
    if priced_days < max(1, len(fees_usd) // 2):
        return tvl_series, f"exact2_partial:insufficient_prices={priced_days}", cov
    return tvl_series, "ok", cov


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
    print("Probing chains:", ",".join(include_chains))

    found_pool, found_chain, found_endpoint = _probe_pool_on_chains(target_pool, include_chains)
    if not found_pool:
        all_v3_chains = sorted(set(UNISWAP_V3_SUBGRAPHS.keys()))
        fallback_chains = [c for c in all_v3_chains if c not in set(include_chains)]
        if fallback_chains:
            print(f"Pool not found on selected chains, fallback probing: {','.join(fallback_chains)}")
            found_pool, found_chain, found_endpoint = _probe_pool_on_chains(target_pool, fallback_chains)

    if not found_pool:
        print("Pool not found on selected chains")
        save_chart_data_json({target_pool: _strict_unavailable_payload(target_pool, "strict_required:pool_not_found_on_selected_chains")}, out_path)
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
    variant = _exact_variant()
    if variant == "exact1":
        exact_series, exact_reason, exact_cov = _build_exact_tvl_series_v3(
            chain=found_chain,
            pool=pool,
            fees_usd=fees_usd,
            baseline_tvl=estimated_tvl,
            day_start_ts=day_start_ts,
            day_end_ts=day_end_ts,
            budget_sec=float(os.environ.get("V3_EXACT_TVL_POOL_BUDGET_SEC", "180")),
        )
    else:
        exact_series, exact_reason, exact_cov = _build_exact2_tvl_series_v3_ledger(
            chain=found_chain,
            pool=pool,
            fees_usd=fees_usd,
            day_start_ts=day_start_ts,
            day_end_ts=day_end_ts,
            budget_sec=float(os.environ.get("V3_EXACT_TVL_POOL_BUDGET_SEC", "180")),
        )

    strict_exact_ok = bool(exact_series and len(exact_series) == len(fees_usd) and all(float(v) > 0.0 for _ts, v in exact_series))
    if strict_exact_ok:
        data_quality = "exact"
        data_quality_reason = ("exact1:ok" if variant == "exact1" else "exact2.0:ok")
        final_tvl_base = list(exact_series)
        final_fees_base = _rebuild_fees_cumulative(fees_usd, final_tvl_base)
        final_tvl = _append_now_tvl_anchor(final_tvl_base, float(pool_tvl_now_usd))
        final_fees = _append_now_fee_anchor(final_fees_base)
        strict_exact_tvl = list(final_tvl)
        strict_exact_fees = list(final_fees)
    else:
        data_quality = "strict_unavailable"
        data_quality_reason = (
            f"strict_required:{'exact1' if variant == 'exact1' else 'exact2.0'}:{exact_reason}:{exact_cov:.2f}"
        )
        final_tvl = []
        final_fees = []
        if exact_series:
            strict_exact_tvl_base = list(exact_series)
            strict_exact_fees_base = _rebuild_fees_cumulative(fees_usd, strict_exact_tvl_base)
            strict_exact_tvl = _append_now_tvl_anchor(strict_exact_tvl_base, float(pool_tvl_now_usd))
            strict_exact_fees = _append_now_fee_anchor(strict_exact_fees_base)
        else:
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
