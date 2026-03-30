#!/usr/bin/env python3
"""
Uniswap v4 Agent: поиск пулов, расчёт LP-комиссий, сохранение для графика.

Использует: The Graph (THE_GRAPH_API_KEY) или V4_OVERRIDE_* для Ormi.
Выход: data/pools_v4_{suffix}.json — формат для agent_merge.
"""

import argparse
import os
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
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
from uniswap_client import query_pool_day_data_batch


# V4 pools on some chains can be indexed under native ETH zero-address.
# Keep this only for ETH-like chains; exclude Polygon to avoid POL/USDC noise for eth requests.
NATIVE_ETH = "0x0000000000000000000000000000000000000000"
NATIVE_ETH_QUERY_CHAINS = {"ethereum", "arbitrum", "optimism", "base", "unichain"}


def save_pdf(pools: list[dict], path: str) -> None:
    """Сохранить список v4 пулов в PDF (аналогично v3)."""
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
            tvl = float(p.get("totalValueLockedUSD") or 0)
            vol = float(p.get("volumeUSD") or 0)
            pid = p.get("id", "")
            data.append(
                [
                    p.get("chain", ""),
                    p.get("pair_label", f"{t0}/{t1}"),
                    pid,  # полный pool id
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
        return float(pool.get("totalValueLockedUSD") or 0)
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


def _resolve_output_dir() -> Path:
    raw = str(os.environ.get("RUN_OUTPUT_DIR", "")).strip()
    if raw:
        p = Path(raw).expanduser()
    else:
        p = Path("data")
    p.mkdir(parents=True, exist_ok=True)
    return p


def _is_timeout_error(err: Exception) -> bool:
    msg = str(err or "").lower()
    return ("read timed out" in msg) or ("timed out" in msg) or ("timeout" in msg)


def _is_bad_indexer_error(err: Exception) -> bool:
    msg = str(err or "").lower()
    return ("bad indexers" in msg) or ("badresponse" in msg) or ("indexer not available" in msg)


def _normalize_day_timestamp(day_value: int) -> int:
    d = int(day_value or 0)
    if d <= 0:
        return 0
    # Subgraph day index is a small integer; unix day timestamps are already large.
    if d < 200_000:
        return d * 86400
    return d


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
    """Parse 'uni,eth;fluid,usdc' -> [(uni,eth), (fluid,usdc)]. Нормализует дубли (fluid,eth)==(eth,fluid)."""
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
    """GraphQL endpoint для v4 на данной сети."""
    override = os.environ.get(f"V4_OVERRIDE_{chain.upper().replace('-', '_')}")
    if override:
        return override
    key = os.environ.get("THE_GRAPH_API_KEY")
    if not key or chain not in UNISWAP_V4_SUBGRAPHS:
        return None
    sub_id = UNISWAP_V4_SUBGRAPHS[chain]
    return f"https://gateway.thegraph.com/api/{key}/subgraphs/id/{sub_id}"


def resolve_token(chain: str, symbol: str, endpoint: str, dynamic: dict) -> Optional[str]:
    """Адрес токена: config -> dynamic -> subgraph lookup."""
    sym = symbol.lower()
    for c in [chain, "ethereum"]:
        addrs = get_token_addresses(c, symbol, dynamic)
        if addrs:
            return addrs[0]
    from uniswap_client import query_token_by_symbol
    addr = query_token_by_symbol(endpoint, symbol)
    if addr:
        if not _env_flag("DISABLE_DYNAMIC_TOKEN_PERSIST", False):
            save_dynamic_token(chain, sym, addr)
        dynamic.setdefault(chain, {})[sym] = addr
    return addr


def query_pools(endpoint: str, token_a: str, token_b: str) -> list[dict]:
    """Найти пулы с обоими токенами (в любом порядке)."""
    from uniswap_client import graphql_query

    a, b = token_a.lower(), token_b.lower()
    q = """
    query Pools($skip: Int!) {
      pools0: pools(first: 100, skip: $skip, where: { token0: "%s", token1: "%s", totalValueLockedToken0_gt: "0", totalValueLockedToken1_gt: "0" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
      pools1: pools(first: 100, skip: $skip, where: { token0: "%s", token1: "%s", totalValueLockedToken0_gt: "0", totalValueLockedToken1_gt: "0" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
    }
    """ % (a, b, b, a)

    result = []
    skip = 0
    while True:
        data = graphql_query(endpoint, q, {"skip": skip})
        d = data.get("data", {})
        p0, p1 = d.get("pools0", []), d.get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pools_by_symbols(endpoint: str, symbol_a: str, symbol_b: str) -> list[dict]:
    """Fallback: find pools by token symbols in either order."""
    from uniswap_client import graphql_query

    sa, sb = symbol_a.upper().strip(), symbol_b.upper().strip()
    if not sa or not sb:
        return []
    q = """
    query PoolsBySymbols($skip: Int!) {
      pools0: pools(first: 100, skip: $skip, where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" }, totalValueLockedToken0_gt: "0", totalValueLockedToken1_gt: "0" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
      pools1: pools(first: 100, skip: $skip, where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" }, totalValueLockedToken0_gt: "0", totalValueLockedToken1_gt: "0" }, orderBy: totalValueLockedUSD, orderDirection: desc) {
        id feeTier liquidity token0 { id symbol } token1 { id symbol }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price volumeUSD feesUSD
      }
    }
    """ % (sa, sb, sb, sa)
    result = []
    skip = 0
    while True:
        # Symbol fallback is best-effort; keep retries low to avoid long stalls.
        data = graphql_query(endpoint, q, {"skip": skip}, retries=1)
        d = data.get("data", {})
        p0, p1 = d.get("pools0", []), d.get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pool_day_data(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> list[dict]:
    """PoolDayData за период."""
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


def compute_fee_series(
    pool: dict,
    endpoint: str,
    day_rows: Optional[list[dict]] = None,
    tvl_multiplier: float = 1.0,
) -> dict:
    """Серии fees и tvl для графика."""
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    # v4 subgraph: пробуем Unix timestamps (как v3)
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    rows = day_rows if day_rows is not None else query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fee_tier = int(pool.get("feeTier") or 3000)
    fee_series, tvl_series = [], []
    cumul = 0.0
    mult = max(0.01, float(tvl_multiplier or 1.0))
    for r in rows:
        tvl = float(r.get("tvlUSD") or 0) * mult
        fees = float(r.get("feesUSD") or 0)
        if fees <= 0:
            fees = 0.0
        if tvl > 0 and fees > 0:
            cumul += fees * (LP_ALLOCATION_USD / tvl)
        # date может быть Unix или day index — для оси времени нужен Unix
        d = int(r["date"])
        ts = _normalize_day_timestamp(d)
        fee_series.append((ts, cumul))
        tvl_series.append((ts, tvl))
    return {"fees": fee_series, "tvl": tvl_series}


def discover_pools(pairs: list[tuple[str, str]], min_tvl: float) -> list[dict]:
    """Найти v4 пулы по всем сетям и парам."""
    chains = [c for c in V4_CHAINS if c in UNISWAP_V4_SUBGRAPHS]
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = [c for c in chains if c.lower() in include]
    print(f"V4 chains active: {','.join(chains) if chains else '-'}")
    dynamic = load_dynamic_tokens()
    all_pools = []
    disable_symbol_fallback = _env_flag("DISABLE_V4_SYMBOL_FALLBACK", False)
    strict_errors = _env_flag("STRICT_DISCOVERY_ERRORS", False)
    skip_chain_after_timeout = _env_flag("V4_SKIP_CHAIN_AFTER_TIMEOUT", True)
    timeout_chains: set[str] = set()

    for chain in chains:
        if chain in timeout_chains:
            if strict_errors:
                raise RuntimeError(f"[{chain}] timeout blacklist activated")
            print(f"  [{chain}] skip: timeout blacklist")
            continue
        endpoint = get_endpoint(chain)
        if not endpoint:
            if strict_errors:
                raise RuntimeError(f"[{chain}] v4: no endpoint")
            print(f"  [{chain}] skip: no endpoint")
            continue

        chain_abort = False
        for base, quote in pairs:
            if chain_abort:
                break
            addr_a = resolve_token(chain, base, endpoint, dynamic)
            addr_b = resolve_token(chain, quote, endpoint, dynamic)
            if not addr_a or not addr_b:
                continue
            known_a = bool(get_token_addresses(chain, base, dynamic) or get_token_addresses("ethereum", base, dynamic))
            known_b = bool(get_token_addresses(chain, quote, dynamic) or get_token_addresses("ethereum", quote, dynamic))

            addrs_a = [addr_a]
            if chain in NATIVE_ETH_QUERY_CHAINS and base.lower() in ("eth", "weth") and addr_a.lower() != NATIVE_ETH:
                addrs_a.append(NATIVE_ETH)
            addrs_b = [addr_b]
            if chain in NATIVE_ETH_QUERY_CHAINS and quote.lower() in ("eth", "weth") and addr_b.lower() != NATIVE_ETH:
                addrs_b.append(NATIVE_ETH)

            pools = []
            timed_out = False
            bad_indexer = False
            for a in addrs_a:
                if bad_indexer:
                    break
                for b in addrs_b:
                    if a.lower() == b.lower():
                        continue
                    try:
                        pools.extend(query_pools(endpoint, a, b))
                    except Exception as e:
                        if strict_errors:
                            raise RuntimeError(f"[{chain}] {base}/{quote}: {e}") from e
                        print(f"  [{chain}] {base}/{quote}: {e}")
                        if _is_timeout_error(e):
                            timed_out = True
                        if _is_bad_indexer_error(e):
                            bad_indexer = True
                            break
                        continue
            should_try_symbol_fallback = (not disable_symbol_fallback) and (not timed_out) and (not bad_indexer) and (not (known_a and known_b))
            if (not pools) and should_try_symbol_fallback:
                try:
                    pools.extend(query_pools_by_symbols(endpoint, base, quote))
                except Exception as e:
                    if strict_errors:
                        raise RuntimeError(f"[{chain}] {base}/{quote} (symbol fallback): {e}") from e
                    print(f"  [{chain}] {base}/{quote} (symbol fallback): {e}")
                    if _is_timeout_error(e):
                        timed_out = True
                    if _is_bad_indexer_error(e):
                        bad_indexer = True
            elif (not pools) and (known_a and known_b):
                print(f"  [{chain}] {base}/{quote}: skip symbol fallback (known token addresses)")
            if bad_indexer:
                if strict_errors:
                    raise RuntimeError(f"[{chain}] {base}/{quote}: bad indexer route detected")
                print(f"  [{chain}] {base}/{quote}: bad indexer route detected, stopped retries for this pair")
            if timed_out and skip_chain_after_timeout:
                timeout_chains.add(chain)
                chain_abort = True
                if strict_errors:
                    raise RuntimeError(f"[{chain}] {base}/{quote}: timeout detected")
                print(f"  [{chain}] timeout detected: skipping remaining pairs on this chain")

            kept = 0
            skipped_missing_price = 0
            miss_reason_counts: dict[str, int] = {}
            miss_reason_samples: dict[str, list[str]] = {}
            for p in pools:
                ext_tvl, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(p, chain)
                if float(ext_tvl) <= 0:
                    skipped_missing_price += 1
                    pid = str((p or {}).get("id") or "")
                    reason = str(price_err or "unknown").strip().lower()
                    miss_reason_counts[reason] = int(miss_reason_counts.get(reason, 0)) + 1
                    bucket = miss_reason_samples.setdefault(reason, [])
                    if pid and len(bucket) < 3:
                        bucket.append(pid)
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
                for reason, count in sorted(miss_reason_counts.items(), key=lambda kv: int(kv[1]), reverse=True):
                    sample = ",".join(miss_reason_samples.get(reason) or [])
                    print(
                        f"[warn] PRICE_UNAVAILABLE_SUMMARY chain={chain} pair={base}/{quote} "
                        f"reason={reason} count={int(count)} samples={sample}"
                    )

    # дедупликация по (chain, id)
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
        print("Нужен THE_GRAPH_API_KEY или V4_OVERRIDE_* (Ormi)")
        return

    pairs_str = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    pairs = parse_pairs(pairs_str)
    suffix = pairs_to_filename_suffix(pairs_str)
    output_dir = _resolve_output_dir()

    print("Uniswap v4 Agent")
    print("Пары:", pairs_str, "| Min TVL: $%.0f" % min_tvl)
    print("Поиск пулов...")

    pools = discover_pools(pairs, min_tvl)
    discovered_count = len(pools)
    max_per_pair_chain = max(0, _env_int("MAX_POOLS_PER_PAIR_CHAIN", 0))
    max_total = max(0, _env_int("MAX_POOLS_TOTAL", 0))
    pools = _cap_pools(pools, max_per_pair_chain=max_per_pair_chain, max_total=max_total)
    strict_errors = _env_flag("STRICT_DISCOVERY_ERRORS", False)
    if len(pools) < discovered_count:
        if strict_errors:
            raise RuntimeError(
                "CAP_TRIM_APPLIED: trimming pools is forbidden in strict mode "
                f"(discovered={discovered_count}, kept={len(pools)}, "
                f"max_per_pair_chain={max_per_pair_chain}, max_total={max_total})"
            )
        print(
            "[warn] CAP_TRIM_APPLIED "
            f"discovered={discovered_count} kept={len(pools)} "
            f"max_per_pair_chain={max_per_pair_chain} max_total={max_total}"
        )
    print(f"Найдено {len(pools)} v4 пулов")

    # PDF-список пулов (аналогично v3)
    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, str(output_dir / f"available_pairs_v4_{suffix}.pdf"))
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))
    batch_size = max(1, min(40, _env_int("POOL_DAY_BATCH_SIZE", 12)))
    day_data_by_pool: dict[str, list[dict]] = {}
    strict_errors = _env_flag("STRICT_DISCOVERY_ERRORS", False)
    if pools:
        end = datetime.utcnow()
        start = end - timedelta(days=FEE_DAYS)
        start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        ids_by_endpoint: dict[str, list[str]] = {}
        for p in pools:
            chain = p.get("chain", "?")
            endpoint = get_endpoint(chain)
            pid = str(p.get("id") or "").strip().lower()
            if endpoint and pid:
                ids_by_endpoint.setdefault(endpoint, []).append(pid)
        for endpoint, ids in ids_by_endpoint.items():
            try:
                fetched = query_pool_day_data_batch(endpoint, ids, start_ts, end_ts, batch_size=batch_size)
                for pid, rows in fetched.items():
                    day_data_by_pool[str(pid).lower()] = rows
            except Exception as e:
                if strict_errors:
                    raise RuntimeError(f"[v4-batch-daydata] {e}") from e
                print(f"  [v4-batch-daydata] {e}")

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
            if strict_errors:
                raise RuntimeError(f"[{chain}] v4 {pair_label}: no endpoint")
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair_label}: skipped (no endpoint)"
        # Strict batch-only mode: do not fallback to per-pool daydata queries.
        # Missing batch rows become empty series (0 days) instead of slow extra network calls.
        day_rows = day_data_by_pool.get(str(pool_id or "").strip().lower(), [])
        try:
            pool_tvl_now_usd = float(pool.get("effectiveTvlUSD") or 0.0)
        except Exception:
            pool_tvl_now_usd = 0.0
        if pool_tvl_now_usd <= 0:
            pool_tvl_now_usd, price_source, price_err = estimate_pool_tvl_usd_external_with_meta(pool, chain)
            if float(pool_tvl_now_usd) <= 0:
                if strict_errors:
                    raise RuntimeError(
                        f"[{chain}] v4 {pair_label}: external TVL unavailable: {price_err or 'unknown'}"
                    )
                msg = (
                    f"  [{idx+1}/{len(pools)}] {chain} {pair_label}: "
                    f"skipped (external TVL unavailable: {price_err or 'unknown'})"
                )
                return idx, None, None, msg
            pool["tvl_price_source"] = str(price_source or "external")
        raw_last_tvl = 0.0
        if day_rows:
            try:
                raw_last_tvl = float((day_rows[-1] or {}).get("tvlUSD") or 0.0)
            except Exception:
                raw_last_tvl = 0.0
        tvl_multiplier = 1.0
        if raw_last_tvl > 0 and pool_tvl_now_usd > 0:
            tvl_multiplier = max(0.05, min(20.0, float(pool_tvl_now_usd) / float(raw_last_tvl)))
        series = compute_fee_series(pool, endpoint, day_rows=day_rows, tvl_multiplier=tvl_multiplier)
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
            "tvl_multiplier": float(tvl_multiplier),
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
                    if strict_errors:
                        raise RuntimeError(f"[v4-series] {e}") from e
                    print(f"  [series] error - {e}")

    out_path = output_dir / f"pools_v4_{suffix}.json"
    save_chart_data_json(chart_data, str(out_path))
    print("Готово:", str(out_path))


if __name__ == "__main__":
    main()
