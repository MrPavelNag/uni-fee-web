#!/usr/bin/env python3
"""
Uniswap v4 Agent: pool discovery, LP fee computation, chart data output.

Uses: The Graph (THE_GRAPH_API_KEY) or V4_OVERRIDE_* for Ormi.
Output: data/pools_v4_{suffix}.json in agent_merge format.
"""

import argparse
import os
import time
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import (
    DEFAULT_TOKEN_PAIRS,
    FEE_DAYS,
    LP_ALLOCATION_USD,
    MIN_TVL_USD,
    UNISWAP_V4_SUBGRAPHS,
    V4_CHAINS,
)
from agent_common import (
    estimate_pool_tvl_usd_external_with_meta,
    get_token_addresses,
    load_dynamic_tokens,
    pairs_to_filename_suffix,
    save_chart_data_json,
    save_dynamic_token,
    _normalize_fee_pct,
)


def save_pdf(pools: list[dict], path: str) -> None:
    """Save v4 pool list to PDF (same style as v3)."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    doc = SimpleDocTemplate(path, pagesize=landscape(A4), rightMargin=1.5 * cm, leftMargin=1.5 * cm)
    story = [Paragraph("Uniswap v4 Pools", getSampleStyleSheet()["Title"]), Spacer(1, 0.5 * cm)]
    if not pools:
        story.append(Paragraph("No pools found.", getSampleStyleSheet()["Normal"]))
    else:
        data = [["Chain", "Pair", "Pool", "Fee %", "TVL USD", "Volume USD"]]
        for p in pools:
            t0 = (p.get("token0") or {}).get("symbol", "?")
            t1 = (p.get("token1") or {}).get("symbol", "?")
            fee_pct = _normalize_fee_pct(int(p.get("feeTier") or 0), "v4")
            tvl = float(p.get("effectiveTvlUSD") or p.get("pool_tvl_now_usd") or p.get("totalValueLockedUSD") or 0)
            vol = float(p.get("volumeUSD") or 0)
            pid = p.get("id", "")
            data.append(
                [
                    p.get("chain", ""),
                    p.get("pair_label", f"{t0}/{t1}"),
                    pid,  # full pool id
                    f"{fee_pct}%",
                    f"${tvl:,.0f}",
                    f"${vol:,.0f}",
                ]
            )
        t = Table(data, colWidths=[2.5 * cm, 3 * cm, 10 * cm, 2 * cm, 4 * cm, 4 * cm])
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("FONTSIZE", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                    ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                ]
            )
        )
        story.append(t)
    doc.build(story)
    print(f"Saved: {path}")

# v4 may use native ETH (0x0) instead of WETH
NATIVE_ETH = "0x0000000000000000000000000000000000000000"


def _native_eth_query_chains() -> set[str]:
    raw = os.environ.get("NATIVE_ETH_QUERY_CHAINS", "ethereum,arbitrum,base,optimism,unichain")
    return {c.strip().lower() for c in str(raw or "").split(",") if c.strip()}


def _base_v4_isolated_enabled() -> bool:
    raw = str(os.environ.get("BASE_V4_ISOLATED_PIPELINE", "1")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_base_chain_enabled() -> bool:
    # Base is disabled by default due to frequent endpoint/indexer instability in production runs.
    raw = str(os.environ.get("ENABLE_BASE_CHAIN", "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _base_v4_override_endpoint() -> str:
    return str(os.environ.get("V4_OVERRIDE_BASE") or "").strip()


def _skip_base_v4_without_override() -> bool:
    # Default: avoid costly known-bad base v4 route when override is absent.
    return _env_flag("V4_SKIP_BASE_WITHOUT_OVERRIDE", True)


def _quick_graphql_healthcheck(endpoint: str) -> bool:
    if not endpoint:
        return False
    try:
        timeout_sec = max(2.0, float(os.environ.get("BASE_V4_HEALTHCHECK_TIMEOUT_SEC", "4")))
    except Exception:
        timeout_sec = 4.0
    try:
        r = requests.post(endpoint, json={"query": "query { __typename }"}, timeout=(3.0, timeout_sec))
        r.raise_for_status()
        data = r.json() if isinstance(r.json(), dict) else {}
        return bool(isinstance(data, dict) and ("data" in data) and not data.get("errors"))
    except Exception:
        return False


def _v4_endpoint_healthcheck_enabled() -> bool:
    return _env_flag("V4_ENDPOINT_HEALTHCHECK", True)


def _page_delay_sec() -> float:
    try:
        return max(0.0, float(os.environ.get("GRAPHQL_PAGE_DELAY_SEC", "0")))
    except Exception:
        return 0.0


def _maybe_page_delay() -> None:
    d = _page_delay_sec()
    if d > 0:
        time.sleep(d)


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


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _is_timeout_error(err: Exception) -> bool:
    msg = str(err or "").lower()
    return ("read timed out" in msg) or ("timed out" in msg) or ("timeout" in msg)


def _is_eth_address(v: str) -> bool:
    s = str(v or "").strip().lower()
    return s.startswith("0x") and len(s) == 42


def _target_pool_id() -> str:
    raw = str(os.environ.get("TARGET_POOL_ID", "") or "").strip().lower()
    if _is_eth_address(raw):
        return raw
    return ""


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


def parse_pairs(s: str) -> list[tuple[str, str]]:
    """Parse 'uni,eth;fluid,usdc' -> [(uni,eth), (fluid,usdc)]. Deduplicates (fluid,eth)==(eth,fluid)."""
    seen = set()
    out = []
    for part in s.replace(" ", "").lower().split(";"):
        if "," not in part:
            continue
        a, b = part.split(",", 1)
        a, b = a.strip(), b.strip()
        key = tuple(sorted([a, b]))
        if key not in seen:
            seen.add(key)
            out.append((a, b))
    return out or [("fluid", "eth")]


def get_endpoint(chain: str) -> Optional[str]:
    """GraphQL endpoint for v4 on a given chain."""
    override = os.environ.get(f"V4_OVERRIDE_{chain.upper().replace('-', '_')}")
    if override:
        return override
    key = os.environ.get("THE_GRAPH_API_KEY")
    if not key or chain not in UNISWAP_V4_SUBGRAPHS:
        return None
    sub_id = UNISWAP_V4_SUBGRAPHS[chain]
    return f"https://gateway.thegraph.com/api/{key}/subgraphs/id/{sub_id}"


def resolve_token(chain: str, symbol: str, endpoint: str, dynamic: dict) -> Optional[str]:
    """Resolve token address: config -> dynamic -> subgraph lookup."""
    sym = symbol.lower()
    for c in [chain, "ethereum"]:
        addrs = get_token_addresses(c, symbol, dynamic)
        if addrs:
            return addrs[0]
    from uniswap_client import query_token_by_symbol
    addr = query_token_by_symbol(endpoint, symbol)
    if addr:
        save_dynamic_token(chain, sym, addr)
        dynamic.setdefault(chain, {})[sym] = addr
    return addr


def query_pools(endpoint: str, token_a: str, token_b: str, max_results: int = 0) -> list[dict]:
    """Find pools containing both tokens (either order)."""
    from uniswap_client import graphql_query

    a, b = token_a.lower(), token_b.lower()
    q = """
    query Pools($skip: Int!) {
      pools0: pools(first: 100, skip: $skip, where: { token0: "%s", token1: "%s" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
      pools1: pools(first: 100, skip: $skip, where: { token0: "%s", token1: "%s" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
    }
    """ % (a, b, b, a)

    result = []
    skip = 0
    while True:
        data = graphql_query(endpoint, q, {"skip": skip}, retries=1)  # fallback path: fail fast
        d = data.get("data", {})
        p0, p1 = d.get("pools0", []), d.get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if int(max_results or 0) > 0 and len(result) >= int(max_results):
            return result[: int(max_results)]
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pools_by_symbols(endpoint: str, symbol_a: str, symbol_b: str, max_results: int = 0) -> list[dict]:
    """Fallback: find pools by token symbols in either order."""
    from uniswap_client import graphql_query

    sa, sb = symbol_a.upper().strip(), symbol_b.upper().strip()
    if not sa or not sb:
        return []
    q = """
    query PoolsBySymbols($skip: Int!) {
      pools0: pools(first: 100, skip: $skip, where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" } }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
      pools1: pools(first: 100, skip: $skip, where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" } }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
    }
    """ % (sa, sb, sb, sa)
    result = []
    skip = 0
    while True:
        data = graphql_query(endpoint, q, {"skip": skip})
        d = data.get("data", {})
        p0, p1 = d.get("pools0", []), d.get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if int(max_results or 0) > 0 and len(result) >= int(max_results):
            return result[: int(max_results)]
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pool_day_data(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> list[dict]:
    """PoolDayData for the selected period."""
    from uniswap_client import graphql_query

    q = """
    query PoolDayData($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      poolDayDatas(first: 100, skip: $skip, orderBy: date, orderDirection: asc, where: { pool: $pool, date_gte: $start, date_lte: $end }) {
        id date tvlUSD volumeUSD feesUSD liquidity
      }
    }
    """
    out = []
    skip = 0
    while True:
        data = graphql_query(endpoint, q, {"pool": pool_id, "start": start_ts, "end": end_ts, "skip": skip})
        items = data.get("data", {}).get("poolDayDatas", [])
        if not items:
            break
        out.extend(items)
        if len(items) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return sorted(out, key=lambda x: int(x["date"]))


def compute_fee_series(pool: dict, endpoint: str) -> dict:
    """Load day-level fees; TVL/income are rebuilt from external TVL later."""
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    # v4 subgraph: use Unix timestamps (same as v3)
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    rows = query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fees_usd_series = []
    raw_tvl_series = []
    for r in rows:
        fees = float(r.get("feesUSD") or 0)
        if fees <= 0:
            fees = 0.0
        # date may be Unix or day index; chart axis needs Unix timestamp
        d = int(r["date"])
        ts = d if d > 1e9 else d * 86400
        fees_usd_series.append((ts, fees))
        try:
            raw_tvl = float(r.get("tvlUSD") or 0.0)
        except Exception:
            raw_tvl = 0.0
        raw_tvl_series.append((ts, max(0.0, raw_tvl)))
    return {"_fees_usd": fees_usd_series, "_raw_tvl_usd": raw_tvl_series}


def discover_pools(pairs: list[tuple[str, str]], min_tvl: float) -> list[dict]:
    """Discover v4 pools across all chains and pairs."""
    chains = [c for c in V4_CHAINS if c in UNISWAP_V4_SUBGRAPHS]
    if "base" in chains and not _is_base_chain_enabled():
        chains = [c for c in chains if c != "base"]
        print("  [base] v4: excluded by default (set ENABLE_BASE_CHAIN=1 to include)")
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = [c for c in chains if c.lower() in include]
    dynamic = load_dynamic_tokens()
    all_pools = []
    disable_symbol_fallback = _env_flag("DISABLE_V4_SYMBOL_FALLBACK", False)
    skip_chain_after_timeout = _env_flag("V4_SKIP_CHAIN_AFTER_TIMEOUT", True)
    discovery_cap = max(0, _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN", 0))
    native_eth_chains = _native_eth_query_chains()
    timeout_chains: set[str] = set()

    def _scan_chain(chain: str, endpoint: str) -> None:
        if chain in timeout_chains:
            print(f"  [{chain}] skip: timeout blacklist")
            return
        if not endpoint:
            print(f"  [{chain}] skip: no endpoint")
            return
        healthcheck_enabled = _v4_endpoint_healthcheck_enabled()
        health_ok = True
        if healthcheck_enabled:
            health_ok = _quick_graphql_healthcheck(endpoint)
        if healthcheck_enabled and not health_ok:
            print(f"  [{chain}] skip: endpoint healthcheck failed")
            return
        chain_abort = False
        for base, quote in pairs:
            if chain_abort:
                break
            addr_a = resolve_token(chain, base, endpoint, dynamic)
            addr_b = resolve_token(chain, quote, endpoint, dynamic)
            if not addr_a or not addr_b:
                continue
            addrs_a = [addr_a]
            if (
                chain.lower() in native_eth_chains
                and base.lower() in ("eth", "weth")
                and addr_a.lower() != NATIVE_ETH
            ):
                addrs_a.append(NATIVE_ETH)
            addrs_b = [addr_b]
            if (
                chain.lower() in native_eth_chains
                and quote.lower() in ("eth", "weth")
                and addr_b.lower() != NATIVE_ETH
            ):
                addrs_b.append(NATIVE_ETH)
            pools = []
            timed_out = False
            for a in addrs_a:
                for b in addrs_b:
                    if a.lower() == b.lower():
                        continue
                    try:
                        pools.extend(query_pools(endpoint, a, b, max_results=int(discovery_cap)))
                        if int(discovery_cap) > 0 and len(pools) >= int(discovery_cap):
                            break
                    except Exception as e:
                        print(f"  [{chain}] {base}/{quote}: {e}")
                        if _is_timeout_error(e):
                            timed_out = True
                            if skip_chain_after_timeout:
                                break
                        continue
                if timed_out and skip_chain_after_timeout:
                    break
                if int(discovery_cap) > 0 and len(pools) >= int(discovery_cap):
                    break
            if (not pools) and (not disable_symbol_fallback) and (not timed_out):
                try:
                    pools.extend(
                        query_pools_by_symbols(
                            endpoint,
                            base,
                            quote,
                            max_results=int(discovery_cap),
                        )
                    )
                except Exception as e:
                    print(f"  [{chain}] {base}/{quote} (symbol fallback): {e}")
                    if _is_timeout_error(e):
                        timed_out = True
            if timed_out and skip_chain_after_timeout:
                timeout_chains.add(chain)
                chain_abort = True
                print(f"  [{chain}] timeout detected: skipping remaining pairs on this chain")
            kept = 0
            skipped_missing_price = 0
            for p in pools:
                ext_tvl, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(p, chain)
                if float(ext_tvl) <= 0:
                    skipped_missing_price += 1
                    continue
                p["effectiveTvlUSD"] = float(ext_tvl)
                p["tvl_price_source"] = str(price_source or "external")
                if float(ext_tvl) < float(min_tvl):
                    continue
                p["chain"] = chain
                p["version"] = "v4"
                p["pair_label"] = f"{base}/{quote}"
                all_pools.append(p)
                kept += 1
            if pools:
                print(f"  [{chain}] {base}/{quote}: {kept} pools")
            if skipped_missing_price > 0:
                print(
                    f"[warn] PRICE_FILTER_DROPPED chain={chain} pair={base}/{quote} "
                    f"count={skipped_missing_price}"
                )

    isolated_base = _base_v4_isolated_enabled() and ("base" in chains)
    base_override_active = False
    base_override_ep = ""
    if isolated_base:
        base_override_ep = _base_v4_override_endpoint()
        if not base_override_ep:
            if _skip_base_v4_without_override():
                print("  [base] v4 isolated: override not set, skipping base discovery")
                chains = [c for c in chains if c != "base"]
            else:
                print("  [base] v4 isolated: override not set, fallback to default endpoint")
        elif not _quick_graphql_healthcheck(base_override_ep):
            print("  [base] v4 isolated: override healthcheck failed, fallback to default endpoint")
        else:
            base_override_active = True

    for chain in chains:
        if base_override_active and chain == "base":
            continue
        _scan_chain(chain, get_endpoint(chain))

    if base_override_active:
        print("  [base] v4 isolated pipeline: enabled")
        _scan_chain("base", base_override_ep)

    # deduplicate by (chain, id)
    seen = set()
    unique = []
    for p in all_pools:
        k = (p.get("chain"), p.get("id"))
        if k[1] and k not in seen:
            seen.add(k)
            unique.append(p)
    return unique


def main() -> None:
    ap = argparse.ArgumentParser(description="Uniswap v4 Agent")
    ap.add_argument("--min-tvl", type=float, default=None)
    args = ap.parse_args()

    min_tvl = args.min_tvl
    if min_tvl is None:
        v = os.environ.get("MIN_TVL")
        min_tvl = float(v) if v else MIN_TVL_USD

    if not os.environ.get("THE_GRAPH_API_KEY") and not any(
        k.startswith("V4_OVERRIDE_") for k in os.environ
    ):
        print("THE_GRAPH_API_KEY or V4_OVERRIDE_* (Ormi) is required")
        return

    pairs_str = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    pairs = parse_pairs(pairs_str)
    suffix = pairs_to_filename_suffix(pairs_str)
    os.makedirs("data", exist_ok=True)

    print("Uniswap v4 Agent")
    print("Pairs:", pairs_str, "| Min TVL: $%.0f" % min_tvl)
    print("Discovering pools...")

    pools = discover_pools(pairs, min_tvl)
    target_pool = _target_pool_id()
    if target_pool:
        pools = [p for p in pools if str((p or {}).get("id") or "").strip().lower() == target_pool]
        print(f"Target pool filter enabled: {target_pool} | matched {len(pools)}")
    max_per_pair_chain = max(0, _env_int("MAX_POOLS_PER_PAIR_CHAIN", 40))
    max_total = max(0, _env_int("MAX_POOLS_TOTAL", 300))
    pools = _cap_pools(pools, max_per_pair_chain=max_per_pair_chain, max_total=max_total)
    print(f"Found {len(pools)} v4 pools")

    # Pool list PDF (same style as v3)
    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, f"data/available_pairs_v4_{suffix}.pdf")
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))

    def _process_pool(idx: int, pool: dict) -> tuple[int, str | None, dict | None, str]:
        chain = pool.get("chain", "?")
        t0 = (pool.get("token0") or {}).get("symbol", "?")
        t1 = (pool.get("token1") or {}).get("symbol", "?")
        pool_id = pool.get("id", "")
        raw_fee_tier = int(pool.get("feeTier") or 0)
        fee_pct = _normalize_fee_pct(raw_fee_tier, "v4")
        pair_label = f"{t0}/{t1}"
        endpoint = get_endpoint(chain)
        if not endpoint:
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair_label}: skipped (no endpoint)"
        series = compute_fee_series(pool, endpoint)
        try:
            pool_tvl_now_usd = float(pool.get("effectiveTvlUSD") or 0.0)
        except Exception:
            pool_tvl_now_usd = 0.0
        if pool_tvl_now_usd <= 0:
            pool_tvl_now_usd, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(pool, chain)
            if float(pool_tvl_now_usd) <= 0:
                msg = (
                    f"  [{idx+1}/{len(pools)}] {chain} {pair_label}: "
                    f"skipped (external TVL unavailable: {price_err or 'unknown'})"
                )
                return idx, None, None, msg
            pool["tvl_price_source"] = str(price_source or "external")
        # Build TVL and profitability strictly from external TVL (no subgraph TVL dependency).
        series["fees"] = []
        series["tvl"] = []
        try:
            fees_usd = series.get("_fees_usd") or []
            raw_tvl = series.get("_raw_tvl_usd") or []
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
                series["tvl"] = tvl_series
                cumul = 0.0
                fees_rebuilt = []
                for i, (ts, fees_day) in enumerate(fees_usd):
                    tvl_day = float(series["tvl"][i][1] or 0.0) if i < len(series["tvl"]) else float(pool_tvl_now_usd)
                    if float(fees_day) > 0:
                        cumul += float(fees_day) * (LP_ALLOCATION_USD / max(1e-12, tvl_day))
                    fees_rebuilt.append((ts, cumul))
                series["fees"] = fees_rebuilt
        except Exception:
            pass
        series.pop("_fees_usd", None)
        series.pop("_raw_tvl_usd", None)
        try:
            raw_pool_tvl = float(pool.get("totalValueLockedUSD") or 0.0)
        except Exception:
            raw_pool_tvl = 0.0
        payload = {
            **series,
            "pool_id": pool_id,
            "fee_pct": fee_pct,
            "raw_fee_tier": raw_fee_tier,
            "pool_tvl_now_usd": pool_tvl_now_usd,
            "pool_tvl_subgraph_usd": raw_pool_tvl,
            "tvl_multiplier": 1.0,
            "tvl_price_source": str(pool.get("tvl_price_source") or ""),
            "pair": pair_label,
            "chain": chain,
            "version": "v4",
        }
        n_days = len(series.get("fees") or [])
        return idx, pool_id, payload, f"  [{idx+1}/{len(pools)}] {chain} {pair_label}: {n_days} days"

    if pools:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = [ex.submit(_process_pool, i, p) for i, p in enumerate(pools)]
            for fut in as_completed(futures):
                try:
                    _, pool_id, payload, msg = fut.result()
                    if pool_id and payload:
                        chart_data[pool_id] = payload
                    print(msg)
                except Exception as e:
                    print(f"  [series] error - {e}")

    out_path = f"data/pools_v4_{suffix}.json"
    save_chart_data_json(chart_data, out_path)
    print("Done:", out_path)


if __name__ == "__main__":
    main()
