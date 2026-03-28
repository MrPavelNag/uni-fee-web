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
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
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
    get_token_addresses,
    load_dynamic_tokens,
    pairs_to_filename_suffix,
    parse_token_pairs,
    save_chart_data_json,
    save_dynamic_token,
)
from uniswap_client import (
    get_graph_endpoint,
    query_pool_day_data,
    query_pool_day_data_batch,
    query_pools_containing_both_tokens,
    query_pools_containing_both_tokens_no_tvl_filter,
    query_pools_by_token_symbols,
    query_token_by_symbol,
)


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


def _discover_cap_hard(default_from_soft: int) -> int:
    env_v = _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN_HARD", 0)
    if env_v > 0:
        return max(env_v, default_from_soft)
    # Safety ceiling: allow automatic cap growth only for boundary-hit cases.
    return max(default_from_soft, 300)


def _discover_with_auto_expand(
    fetch_fn,
    *,
    soft_cap: int,
    hard_cap: int,
    chain: str,
    pair_label: str,
    source: str,
) -> tuple[list[dict], bool]:
    """
    Fetch pools with adaptive cap growth when result size hits the cap boundary.
    Returns (pools, truncated_flag).
    """
    cap = max(0, int(soft_cap or 0))
    if cap <= 0:
        return list(fetch_fn(0) or []), False
    hard = max(cap, int(hard_cap or cap))
    cur = cap
    while True:
        pools = list(fetch_fn(cur) or [])
        if len(pools) < cur:
            return pools, False
        if cur >= hard:
            print(
                f"[warn] DISCOVERY_CAP_HIT chain={chain} pair={pair_label} source={source} "
                f"kept={len(pools)} cap={cur} hard_cap={hard}"
            )
            return pools[:hard], True
        nxt = min(hard, max(cur * 2, cur + 50))
        print(
            f"[info] DISCOVERY_CAP_EXPAND chain={chain} pair={pair_label} source={source} "
            f"from={cur} to={nxt}"
        )
        cur = nxt


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


def _resolve_output_dir() -> Path:
    raw = str(os.environ.get("RUN_OUTPUT_DIR", "")).strip()
    if raw:
        p = Path(raw).expanduser()
    else:
        p = Path("data")
    p.mkdir(parents=True, exist_ok=True)
    return p


def discover_pools_v3(
    token_pairs_str: str,
    min_tvl: Optional[float] = None,
    fresh_token_lookup: bool = False,
) -> list[dict]:
    """Discover v3 pools only."""
    pairs = parse_token_pairs(token_pairs_str)
    dynamic_tokens = load_dynamic_tokens()
    persist_dynamic_tokens = not _env_flag("DISABLE_DYNAMIC_TOKEN_PERSIST", False)
    all_pools: list[dict] = []
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = {c for c in chains if c.lower() in include}
    discovery_cap = max(0, _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN", 0))
    discovery_cap_hard = _discover_cap_hard(max(1, discovery_cap) * 6 if discovery_cap > 0 else 0)
    chain_workers = max(1, min(_env_int("V3_DISCOVERY_CHAIN_WORKERS", 4), len(chains) or 1))
    dyn_lock = threading.Lock()

    def _discover_for_chain(chain: str) -> list[dict]:
        out_chain: list[dict] = []
        endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
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
                        if persist_dynamic_tokens:
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
                pools, truncated = _discover_with_auto_expand(
                    lambda cap: query_pools_containing_both_tokens(
                        endpoint,
                        base_addrs[0],
                        quote_addrs[0],
                        _min_tvl(min_tvl),
                        max_results=int(cap),
                    ),
                    soft_cap=discovery_cap,
                    hard_cap=discovery_cap_hard,
                    chain=chain,
                    pair_label=f"{base}/{quote}",
                    source="address",
                )
                if not pools:
                    # Fallback for ambiguous symbol->address mappings (e.g. multiple tokens with same symbol).
                    pools, trunc_sym = _discover_with_auto_expand(
                        lambda cap: query_pools_by_token_symbols(
                            endpoint,
                            base,
                            quote,
                            _min_tvl(min_tvl),
                            max_results=int(cap),
                        ),
                        soft_cap=discovery_cap,
                        hard_cap=discovery_cap_hard,
                        chain=chain,
                        pair_label=f"{base}/{quote}",
                        source="symbol",
                    )
                    truncated = truncated or trunc_sym
                if not pools:
                    # Safety fallback for subgraphs where totalValueLockedUSD_gte behaves inconsistently.
                    try:
                        raw_no_tvl = query_pools_containing_both_tokens_no_tvl_filter(
                            endpoint,
                            base_addrs[0],
                            quote_addrs[0],
                        )
                        min_tvl_now = _min_tvl(min_tvl)
                        pools = [
                            p for p in (raw_no_tvl or [])
                            if _pool_tvl_usd(p) >= float(min_tvl_now)
                        ]
                        if pools:
                            pools = sorted(pools, key=_pool_tvl_usd, reverse=True)
                            if discovery_cap_hard > 0:
                                pools = pools[: int(discovery_cap_hard)]
                            print(f"[info] v3_no_tvl_fallback_hit chain={chain} pair={base}/{quote} pools={len(pools)}")
                    except Exception as e:
                        print(f"  [{chain}] v3 no-tvl fallback {base}/{quote}: {e}")
                if truncated:
                    print(f"[warn] DISCOVERY_POTENTIALLY_TRUNCATED chain={chain} pair={base}/{quote}")
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


def compute_fee_and_tvl_series(pool: dict, endpoint: str, day_data: Optional[list[dict]] = None) -> dict:
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    # v3 subgraph ожидает Unix timestamps для date_gte/date_lte
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    if day_data is None:
        day_data = query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fee_tier = int(pool.get("feeTier") or 3000)
    fee_series, tvl_series = [], []
    cumul = 0.0
    for d in day_data:
        tvl = float(d.get("tvlUSD") or 0)
        fees = float(d.get("feesUSD") or 0)
        # feesUSD=0: не оцениваем из volume — subgraph иногда возвращает неверный volume
        if fees <= 0:
            fees = 0.0
        if tvl > 0 and fees > 0:
            cumul += fees * (LP_ALLOCATION_USD / tvl)
        ts = int(d["date"])
        fee_series.append((ts, cumul))
        tvl_series.append((ts, tvl))
    return {"fees": fee_series, "tvl": tvl_series}


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
            tvl = float(p.get("totalValueLockedUSD") or 0)
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
    output_dir = _resolve_output_dir()

    print("Agent 1: Uniswap v3 (базовая версия)")
    print("Token pairs:", token_pairs, "| Min TVL: $%s" % f"{min_tvl_val:,.0f}".replace(",", " "))
    print("Discovering v3 pools...")
    fresh = "TOKEN_PAIRS" in os.environ
    pools = discover_pools_v3(token_pairs, args.min_tvl, fresh_token_lookup=fresh)
    discovered_count = len(pools)
    max_per_pair_chain = max(0, _env_int("MAX_POOLS_PER_PAIR_CHAIN", 40))
    max_total = max(0, _env_int("MAX_POOLS_TOTAL", 300))
    pools = _cap_pools(pools, max_per_pair_chain=max_per_pair_chain, max_total=max_total)
    if len(pools) < discovered_count:
        print(
            "[warn] CAP_TRIM_APPLIED "
            f"discovered={discovered_count} kept={len(pools)} "
            f"max_per_pair_chain={max_per_pair_chain} max_total={max_total}"
        )
    print(f"Found {len(pools)} v3 pools")

    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, str(output_dir / f"available_pairs_v3_{suffix}.pdf"))
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    pool_chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))
    batch_size = max(1, min(40, _env_int("POOL_DAY_BATCH_SIZE", 12)))
    day_data_by_pool: dict[str, list[dict]] = {}
    if pools:
        end = datetime.utcnow()
        start = end - timedelta(days=FEE_DAYS)
        start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        ids_by_endpoint: dict[str, list[str]] = {}
        for p in pools:
            chain = p.get("chain", "unknown")
            endpoint = get_graph_endpoint(chain, "v3")
            pid = str(p.get("id") or "").strip().lower()
            if endpoint and pid:
                ids_by_endpoint.setdefault(endpoint, []).append(pid)
        for endpoint, ids in ids_by_endpoint.items():
            try:
                fetched = query_pool_day_data_batch(endpoint, ids, start_ts, end_ts, batch_size=batch_size)
                for pid, rows in fetched.items():
                    day_data_by_pool[str(pid).lower()] = rows
            except Exception as e:
                print(f"  [v3-batch-daydata] {e}")

    def _process_pool(idx: int, pool: dict) -> tuple[int, str | None, dict | None, str]:
        chain = pool.get("chain", "unknown")
        t0 = (pool.get("token0") or {}).get("symbol", "?")
        t1 = (pool.get("token1") or {}).get("symbol", "?")
        pool_id = pool.get("id", "")
        raw_fee_tier = int(pool.get("feeTier") or 0)
        fee_pct = raw_fee_tier / 10000
        pair = f"{t0}/{t1}"
        endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair}: skipped (no endpoint)"
        day_rows = day_data_by_pool.get(str(pool_id or "").strip().lower())
        data = compute_fee_and_tvl_series(pool, endpoint, day_data=day_rows)
        try:
            pool_tvl_now_usd = float(pool.get("totalValueLockedUSD") or 0.0)
        except Exception:
            pool_tvl_now_usd = 0.0
        payload = {
            **data,
            "pool_id": pool_id,
            "fee_pct": fee_pct,
            "raw_fee_tier": raw_fee_tier,
            "pool_tvl_now_usd": pool_tvl_now_usd,
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

    out_json = output_dir / f"pools_v3_{suffix}.json"
    save_chart_data_json(pool_chart_data, str(out_json))
    print("Done. Output:", str(out_json))


if __name__ == "__main__":
    main()
