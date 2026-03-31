#!/usr/bin/env python3
"""
Agent 1: Uniswap v3 (базовая версия).

- Ищет v3 пулы по TOKEN_PAIRS
- Фильтр по TVL
- Сохраняет PDF со списком пулов
- Сохраняет данные для графика в data/pools_v3_{suffix}.json
"""

import argparse
import os
import threading
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import (
    DEFAULT_TOKEN_PAIRS,
    FEE_DAYS,
    LP_ALLOCATION_USD,
    MIN_TVL_USD,
    UNISWAP_V3_SUBGRAPHS,
    GOLDSKY_ENDPOINTS,
)

from agent_common import (
    estimate_pool_tvl_usd_external_with_meta,
    get_token_addresses,
    load_dynamic_tokens,
    pairs_to_filename_suffix,
    parse_token_pairs,
    save_chart_data_json,
    save_dynamic_token,
)
from uniswap_client import (
    graphql_query,
    get_graph_endpoint,
    query_pool_day_data,
    query_pools_containing_both_tokens,
    query_pools_by_token_symbols,
    query_token_by_symbol,
)


def _pool_tvl_usd(pool: dict) -> float:
    try:
        return float(pool.get("effectiveTvlUSD") or pool.get("pool_tvl_now_usd") or pool.get("totalValueLockedUSD") or 0)
    except Exception:
        return 0.0


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return int(default)


def _cap_pools(pools: list[dict], max_per_pair_chain: int, max_total: int) -> list[dict]:
    if not pools:
        return []
    out: list[dict] = []
    if max_per_pair_chain > 0:
        by_key: dict[tuple[str, str], list[dict]] = {}
        for p in pools:
            key = (str(p.get("chain") or ""), str(p.get("pair_label") or ""))
            by_key.setdefault(key, []).append(p)
        for items in by_key.values():
            items.sort(key=_pool_tvl_usd, reverse=True)
            out.extend(items[:max_per_pair_chain])
    else:
        out = list(pools)
    if max_total > 0 and len(out) > max_total:
        out.sort(key=_pool_tvl_usd, reverse=True)
        out = out[:max_total]
    return out


def _min_tvl(cli_value: Optional[float] = None) -> float:
    if cli_value is not None:
        return float(cli_value)
    v = os.environ.get("MIN_TVL")
    if v is not None:
        try:
            return float(v)
        except ValueError:
            pass
    return MIN_TVL_USD


def _base_v3_goldsky_endpoint() -> str:
    return str(GOLDSKY_ENDPOINTS.get("base") or "").strip()


def _is_base_v3_isolated_enabled() -> bool:
    raw = str(os.environ.get("BASE_V3_ISOLATED_PIPELINE", "1")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _quick_graphql_healthcheck(endpoint: str) -> bool:
    if not endpoint:
        return False
    try:
        timeout_sec = max(2.0, float(os.environ.get("V3_ENDPOINT_HEALTHCHECK_TIMEOUT_SEC", "4")))
    except Exception:
        timeout_sec = 4.0
    try:
        r = requests.post(endpoint, json={"query": "query { __typename }"}, timeout=(3.0, timeout_sec))
        r.raise_for_status()
        data = r.json() if isinstance(r.json(), dict) else {}
        return bool(isinstance(data, dict) and ("data" in data) and not data.get("errors"))
    except Exception:
        return False


def _discover_base_v3_goldsky_pools(
    endpoint: str,
    token_a: str,
    token_b: str,
    max_results: int,
) -> list[dict]:
    # Goldsky Base v3 uses Messari-like schema (liquidityPools / inputTokens / inputTokenBalances).
    token_a = str(token_a or "").strip().lower()
    token_b = str(token_b or "").strip().lower()
    if not token_a or not token_b or token_a == token_b:
        return []
    q_filtered = """
    query BasePools($skip: Int!) {
      liquidityPools(
        first: 100,
        skip: $skip,
        orderBy: totalValueLockedUSD,
        orderDirection: desc,
        where: { inputTokens_contains: ["%s", "%s"] }
      ) {
        id
        totalValueLockedUSD
        inputTokenBalances
        inputTokens { id symbol decimals }
      }
    }
    """ % (token_a, token_b)
    q_unfiltered = """
    query BasePools($skip: Int!) {
      liquidityPools(first: 100, skip: $skip, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id
        totalValueLockedUSD
        inputTokenBalances
        inputTokens { id symbol decimals }
      }
    }
    """
    query_text = q_filtered
    used_unfiltered = False
    out: list[dict] = []
    skip = 0
    while True:
        try:
            data = graphql_query(endpoint, query_text, {"skip": skip}, retries=1)
        except Exception as e:
            if (not used_unfiltered) and "inputtokens_contains" in str(e).lower():
                query_text = q_unfiltered
                used_unfiltered = True
                continue
            raise
        rows = data.get("data", {}).get("liquidityPools", []) or []
        if not rows:
            break
        for r in rows:
            toks = r.get("inputTokens") or []
            if not isinstance(toks, list) or len(toks) < 2:
                continue
            ids = [str((t or {}).get("id") or "").strip().lower() for t in toks[:2]]
            if token_a not in ids or token_b not in ids:
                continue
            balances = r.get("inputTokenBalances") or []
            bal0 = float(balances[0] or 0.0) if len(balances) > 0 else 0.0
            bal1 = float(balances[1] or 0.0) if len(balances) > 1 else 0.0
            p = {
                "id": str(r.get("id") or ""),
                "feeTier": 3000,  # fallback for alternative schema
                "token0": {
                    "id": ids[0],
                    "symbol": str((toks[0] or {}).get("symbol") or "?"),
                    "decimals": str((toks[0] or {}).get("decimals") or "18"),
                    "name": "",
                },
                "token1": {
                    "id": ids[1],
                    "symbol": str((toks[1] or {}).get("symbol") or "?"),
                    "decimals": str((toks[1] or {}).get("decimals") or "18"),
                    "name": "",
                },
                "totalValueLockedUSD": str(r.get("totalValueLockedUSD") or "0"),
                "totalValueLockedToken0": str(bal0),
                "totalValueLockedToken1": str(bal1),
                "volumeUSD": "0",
                "feesUSD": "0",
                "_base_schema": "goldsky_v3_alt",
            }
            if p["id"]:
                out.append(p)
                if int(max_results or 0) > 0 and len(out) >= int(max_results):
                    return out[: int(max_results)]
        if len(rows) < 100:
            break
        skip += 100
    return out


def _query_base_v3_goldsky_day_data(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> list[dict]:
    q = """
    query BasePoolDays($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      liquidityPoolDailySnapshots(
        first: 100,
        skip: $skip,
        orderBy: timestamp,
        orderDirection: asc,
        where: { pool: $pool, timestamp_gte: $start, timestamp_lte: $end }
      ) {
        timestamp
        totalValueLockedUSD
        dailySupplySideRevenueUSD
      }
    }
    """
    out: list[dict] = []
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            q,
            {"pool": str(pool_id or "").strip().lower(), "start": int(start_ts), "end": int(end_ts), "skip": int(skip)},
            retries=1,
        )
        rows = data.get("data", {}).get("liquidityPoolDailySnapshots", []) or []
        if not rows:
            break
        for r in rows:
            ts = int(float(r.get("timestamp") or 0))
            out.append(
                {
                    "date": ts,
                    "tvlUSD": float(r.get("totalValueLockedUSD") or 0.0),
                    "feesUSD": float(r.get("dailySupplySideRevenueUSD") or 0.0),
                }
            )
        if len(rows) < 100:
            break
        skip += 100
    return out


def discover_pools_v3(
    token_pairs_str: str,
    min_tvl: Optional[float] = None,
    fresh_token_lookup: bool = False,
) -> list[dict]:
    """Discover v3 pools only."""
    pairs = parse_token_pairs(token_pairs_str)
    dynamic_tokens = load_dynamic_tokens()
    all_pools: list[dict] = []
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = {c for c in chains if c.lower() in include}
    discovery_cap = max(0, _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN", 0))
    chain_workers = max(1, min(_env_int("V3_DISCOVERY_CHAIN_WORKERS", 4), len(chains) or 1))
    dyn_lock = threading.Lock()

    def _discover_for_chain(chain: str) -> list[dict]:
        out_chain: list[dict] = []
        use_base_goldsky = bool(
            chain == "base"
            and _is_base_v3_isolated_enabled()
            and _base_v3_goldsky_endpoint()
        )
        endpoint = _base_v3_goldsky_endpoint() if use_base_goldsky else get_graph_endpoint(chain, "v3")
        if not endpoint:
            return out_chain
        check_enabled = str(os.environ.get("V3_ENDPOINT_HEALTHCHECK", "1")).strip().lower() in {"1", "true", "yes", "on"}
        if check_enabled and not _quick_graphql_healthcheck(endpoint):
            print(f"  [{chain}] v3: skip (endpoint healthcheck failed)")
            return out_chain

        def _resolve_with_cache(sym: str) -> list[str]:
            out: list[str] = []
            for c in [chain, "ethereum"]:
                with dyn_lock:
                    out.extend(get_token_addresses(c, sym, dynamic_tokens))
            out = list(dict.fromkeys(out))[:1]
            if not out:
                addr = query_token_by_symbol(endpoint, sym)
                if addr:
                    with dyn_lock:
                        save_dynamic_token(chain, sym, addr)
                        dynamic_tokens.setdefault(chain, {})[sym.lower()] = addr
                    out = [addr]
            return out

        for base, quote in pairs:
            base_addrs, quote_addrs = [], []

            if fresh_token_lookup:
                base_addrs = _resolve_with_cache(base)
                quote_addrs = _resolve_with_cache(quote)
            else:
                for c in [chain, "ethereum"]:
                    with dyn_lock:
                        base_addrs.extend(get_token_addresses(c, base, dynamic_tokens))
                        quote_addrs.extend(get_token_addresses(c, quote, dynamic_tokens))
                base_addrs = list(dict.fromkeys(base_addrs))[:1]
                quote_addrs = list(dict.fromkeys(quote_addrs))[:1]
                if not base_addrs:
                    base_addrs = _resolve_with_cache(base)
                if not quote_addrs:
                    quote_addrs = _resolve_with_cache(quote)

            if not base_addrs or not quote_addrs:
                continue
            try:
                if use_base_goldsky:
                    pools = _discover_base_v3_goldsky_pools(
                        endpoint,
                        base_addrs[0],
                        quote_addrs[0],
                        max_results=int(discovery_cap),
                    )
                else:
                    pools = query_pools_containing_both_tokens(
                        endpoint,
                        base_addrs[0],
                        quote_addrs[0],
                        _min_tvl(min_tvl),
                        max_results=int(discovery_cap),
                    )
                    if not pools:
                        # Fallback for ambiguous symbol->address mappings (e.g. multiple tokens with same symbol).
                        pools = query_pools_by_token_symbols(
                            endpoint,
                            base,
                            quote,
                            _min_tvl(min_tvl),
                            max_results=int(discovery_cap),
                        )
                min_tvl_now = _min_tvl(min_tvl)
                filtered_pools: list[dict] = []
                skipped_missing_price = 0
                for p in pools:
                    ext_tvl, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(p, chain)
                    if float(ext_tvl) <= 0:
                        skipped_missing_price += 1
                        pid = str((p or {}).get("id") or "")
                        print(
                            f"[warn] PRICE_UNAVAILABLE chain={chain} pair={base}/{quote} "
                            f"pool={pid} reason={price_err or 'unknown'}"
                        )
                        continue
                    p["effectiveTvlUSD"] = float(ext_tvl)
                    p["tvl_price_source"] = str(price_source or "external")
                    if float(ext_tvl) >= float(min_tvl_now):
                        filtered_pools.append(p)
                pools = filtered_pools
                if skipped_missing_price > 0:
                    print(
                        f"[warn] PRICE_FILTER_DROPPED chain={chain} pair={base}/{quote} "
                        f"count={skipped_missing_price}"
                    )
                for p in pools:
                    p["chain"] = chain
                    p["version"] = "v3"
                    p["pair_label"] = f"{base}/{quote}"
                    out_chain.append(p)
            except Exception as e:
                print(f"  [{chain}] v3 {base}/{quote}: {e}")
        return out_chain

    if chains:
        with ThreadPoolExecutor(max_workers=chain_workers) as ex:
            futs = [ex.submit(_discover_for_chain, c) for c in sorted(chains)]
            for fut in as_completed(futs):
                try:
                    all_pools.extend(fut.result() or [])
                except Exception as e:
                    print(f"  [v3-discovery] chain worker error: {e}")
    seen = set()
    unique = []
    for p in all_pools:
        key = (p.get("chain", ""), p.get("id", ""))
        if key[1] and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def compute_fee_and_tvl_series(pool: dict, endpoint: str) -> dict:
    """Load day-level fees; TVL/income are rebuilt from external TVL later."""
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    # v3 subgraph ожидает Unix timestamps для date_gte/date_lte
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    if str(pool.get("_base_schema") or "") == "goldsky_v3_alt":
        day_data = _query_base_v3_goldsky_day_data(endpoint, pool["id"], start_ts, end_ts)
    else:
        day_data = query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fees_usd_series = []
    raw_tvl_series = []
    for d in day_data:
        fees = float(d.get("feesUSD") or 0)
        # feesUSD=0: не оцениваем из volume — subgraph иногда возвращает неверный volume
        if fees <= 0:
            fees = 0.0
        ts = int(d["date"])
        fees_usd_series.append((ts, fees))
        try:
            raw_tvl = float(d.get("tvlUSD") or 0.0)
        except Exception:
            raw_tvl = 0.0
        raw_tvl_series.append((ts, max(0.0, raw_tvl)))
    return {"_fees_usd": fees_usd_series, "_raw_tvl_usd": raw_tvl_series}


def save_pdf(pools: list[dict], path: str) -> None:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    doc = SimpleDocTemplate(path, pagesize=landscape(A4), rightMargin=1.5 * cm, leftMargin=1.5 * cm)
    story = [Paragraph("Uniswap v3 Pools (базовая версия, TVL &gt; $1000)", getSampleStyleSheet()["Title"]), Spacer(1, 0.5 * cm)]
    if not pools:
        story.append(Paragraph("No pools found.", getSampleStyleSheet()["Normal"]))
    else:
        data = [["Chain", "Pair", "Pool", "Fee %", "TVL USD", "Volume USD"]]
        for p in pools:
            t0 = (p.get("token0") or {}).get("symbol", "?")
            t1 = (p.get("token1") or {}).get("symbol", "?")
            fee_pct = int(p.get("feeTier") or 0) / 10000
            tvl = float(p.get("effectiveTvlUSD") or p.get("pool_tvl_now_usd") or p.get("totalValueLockedUSD") or 0)
            vol = float(p.get("volumeUSD") or 0)
            pid = p.get("id", "")
            data.append([
                p.get("chain", ""),
                p.get("pair_label", f"{t0}/{t1}"),
                pid,  # полный pool id
                f"{fee_pct}%",
                f"${tvl:,.0f}",
                f"${vol:,.0f}",
            ])
        t = Table(data, colWidths=[2.5 * cm, 3 * cm, 10 * cm, 2 * cm, 4 * cm, 4 * cm])
        t.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (0, 0), (-1, -1), "LEFT"),
            ("FONTSIZE", (0, 0), (-1, -1), 6),
            ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
            ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
        ]))
        story.append(t)
    doc.build(story)
    print(f"Saved: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Agent 1: Uniswap v3 (базовая версия)")
    parser.add_argument("--min-tvl", type=float, default=None, help="Min TVL USD")
    args = parser.parse_args()

    token_pairs = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    min_tvl_val = _min_tvl(args.min_tvl)
    suffix = pairs_to_filename_suffix(token_pairs)
    os.makedirs("data", exist_ok=True)

    print("Agent 1: Uniswap v3 (базовая версия)")
    print("Token pairs:", token_pairs, "| Min TVL: $%s" % f"{min_tvl_val:,.0f}".replace(",", " "))
    print("Discovering v3 pools...")
    fresh = "TOKEN_PAIRS" in os.environ
    pools = discover_pools_v3(token_pairs, args.min_tvl, fresh_token_lookup=fresh)
    max_per_pair_chain = max(0, _env_int("MAX_POOLS_PER_PAIR_CHAIN", 40))
    max_total = max(0, _env_int("MAX_POOLS_TOTAL", 300))
    pools = _cap_pools(pools, max_per_pair_chain=max_per_pair_chain, max_total=max_total)
    print(f"Found {len(pools)} v3 pools")

    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, f"data/available_pairs_v3_{suffix}.pdf")
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    pool_chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))

    def _process_pool(idx: int, pool: dict) -> tuple[int, str | None, dict | None, str]:
        chain = pool.get("chain", "unknown")
        t0 = (pool.get("token0") or {}).get("symbol", "?")
        t1 = (pool.get("token1") or {}).get("symbol", "?")
        pool_id = pool.get("id", "")
        raw_fee_tier = int(pool.get("feeTier") or 0)
        fee_pct = raw_fee_tier / 10000
        pair = f"{t0}/{t1}"
        if chain == "base" and str(pool.get("_base_schema") or "") == "goldsky_v3_alt" and _base_v3_goldsky_endpoint():
            endpoint = _base_v3_goldsky_endpoint()
        else:
            endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair}: skipped (no endpoint)"
        data = compute_fee_and_tvl_series(pool, endpoint)
        try:
            pool_tvl_now_usd = float(pool.get("effectiveTvlUSD") or 0.0)
        except Exception:
            pool_tvl_now_usd = 0.0
        if pool_tvl_now_usd <= 0:
            pool_tvl_now_usd, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(pool, chain)
            if float(pool_tvl_now_usd) <= 0:
                msg = (
                    f"  [{idx+1}/{len(pools)}] {chain} {pair}: "
                    f"skipped (external TVL unavailable: {price_err or 'unknown'})"
                )
                return idx, None, None, msg
            pool["tvl_price_source"] = str(price_source or "external")
        # Build TVL and profitability strictly from external TVL (no subgraph TVL dependency).
        data["fees"] = []
        data["tvl"] = []
        try:
            fees_usd = data.get("_fees_usd") or []
            raw_tvl = data.get("_raw_tvl_usd") or []
            if fees_usd and pool_tvl_now_usd > 0:
                tvl_series = [(int(ts), float(pool_tvl_now_usd)) for ts, _ in fees_usd]
                if raw_tvl and len(raw_tvl) == len(fees_usd):
                    anchor = 0.0
                    for _, rv in reversed(raw_tvl):
                        rvf = float(rv or 0.0)
                        if rvf > 0:
                            anchor = rvf
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
                data["tvl"] = tvl_series
                cumul = 0.0
                fees_rebuilt = []
                for i, (ts, fees_day) in enumerate(fees_usd):
                    tvl_day = float(data["tvl"][i][1] or 0.0) if i < len(data["tvl"]) else float(pool_tvl_now_usd)
                    if float(fees_day) > 0:
                        cumul += float(fees_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day))
                    fees_rebuilt.append((ts, cumul))
                data["fees"] = fees_rebuilt
        except Exception:
            pass
        data.pop("_fees_usd", None)
        data.pop("_raw_tvl_usd", None)
        try:
            raw_pool_tvl = float(pool.get("totalValueLockedUSD") or 0.0)
        except Exception:
            raw_pool_tvl = 0.0
        payload = {
            **data,
            "pool_id": pool_id,
            "fee_pct": fee_pct,
            "raw_fee_tier": raw_fee_tier,
            "pool_tvl_now_usd": pool_tvl_now_usd,
            "pool_tvl_subgraph_usd": raw_pool_tvl,
            "tvl_multiplier": 1.0,
            "tvl_price_source": str(pool.get("tvl_price_source") or ""),
            "pair": pair,
            "chain": chain,
            "version": "v3",
        }
        return idx, pool_id, payload, f"  [{idx+1}/{len(pools)}] {chain} {pair}: {len(data.get('fees') or [])} days"

    if pools:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_process_pool, i, p) for i, p in enumerate(pools)]
            for fut in as_completed(futures):
                try:
                    _, pool_id, payload, msg = fut.result()
                    if pool_id and payload:
                        pool_chart_data[pool_id] = payload
                    print(msg)
                except Exception as e:
                    print(f"  [series] error - {e}")

    out_json = f"data/pools_v3_{suffix}.json"
    save_chart_data_json(pool_chart_data, out_json)
    print("Done. Output:", out_json)


if __name__ == "__main__":
    main()
