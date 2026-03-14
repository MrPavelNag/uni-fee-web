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
import time
from datetime import datetime, timedelta
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
    query_pools_containing_both_tokens,
    query_token_by_symbol,
)


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


def discover_pools_v3(
    token_pairs_str: str,
    min_tvl: Optional[float] = None,
    fresh_token_lookup: bool = False,
) -> list[dict]:
    """Discover v3 pools only."""
    pairs = parse_token_pairs(token_pairs_str)
    dynamic_tokens = load_dynamic_tokens()
    all_pools = []
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())
    include = {c.strip().lower() for c in os.environ.get("INCLUDE_CHAINS", "").split(",") if c.strip()}
    if include:
        chains = {c for c in chains if c.lower() in include}

    for chain in chains:
        endpoint = get_graph_endpoint(chain, "v3")
        if not endpoint:
            continue
        for base, quote in pairs:
            base_addrs, quote_addrs = [], []

            if fresh_token_lookup:
                def resolve(sym: str) -> list:
                    out = []
                    for c in [chain, "ethereum"]:
                        out.extend(get_token_addresses(c, sym, dynamic_tokens))
                    out = list(dict.fromkeys(out))[:1]
                    if not out:
                        addr = query_token_by_symbol(endpoint, sym)
                        if addr:
                            save_dynamic_token(chain, sym, addr)
                            dynamic_tokens.setdefault(chain, {})[sym.lower()] = addr
                            out = [addr]
                    return out
                base_addrs = resolve(base)
                quote_addrs = resolve(quote)
            else:
                for c in [chain, "ethereum"]:
                    base_addrs.extend(get_token_addresses(c, base, dynamic_tokens))
                    quote_addrs.extend(get_token_addresses(c, quote, dynamic_tokens))
                base_addrs = list(dict.fromkeys(base_addrs))[:1]
                quote_addrs = list(dict.fromkeys(quote_addrs))[:1]
                if not base_addrs:
                    addr = query_token_by_symbol(endpoint, base)
                    if addr:
                        save_dynamic_token(chain, base, addr)
                        dynamic_tokens.setdefault(chain, {})[base.lower()] = addr
                        base_addrs = [addr]
                if not quote_addrs:
                    addr = query_token_by_symbol(endpoint, quote)
                    if addr:
                        save_dynamic_token(chain, quote, addr)
                        dynamic_tokens.setdefault(chain, {})[quote.lower()] = addr
                        quote_addrs = [addr]

            if not base_addrs or not quote_addrs:
                continue
            try:
                pools = query_pools_containing_both_tokens(
                    endpoint, base_addrs[0], quote_addrs[0], _min_tvl(min_tvl)
                )
                for p in pools:
                    p["chain"] = chain
                    p["version"] = "v3"
                    p["pair_label"] = f"{base}/{quote}"
                    all_pools.append(p)
            except Exception as e:
                print(f"  [{chain}] v3 {base}/{quote}: {e}")
        time.sleep(0.5)

    seen = set()
    unique = []
    for p in all_pools:
        key = (p.get("chain", ""), p.get("id", ""))
        if key[1] and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def compute_fee_and_tvl_series(pool: dict, endpoint: str) -> dict:
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    # v3 subgraph ожидает Unix timestamps для date_gte/date_lte
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

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
    os.makedirs("data", exist_ok=True)

    print("Agent 1: Uniswap v3 (базовая версия)")
    print("Token pairs:", token_pairs, "| Min TVL: $%s" % f"{min_tvl_val:,.0f}".replace(",", " "))
    print("Discovering v3 pools...")
    fresh = "TOKEN_PAIRS" in os.environ
    pools = discover_pools_v3(token_pairs, args.min_tvl, fresh_token_lookup=fresh)
    print(f"Found {len(pools)} v3 pools")

    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, f"data/available_pairs_v3_{suffix}.pdf")
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    pool_chart_data = {}
    for i, p in enumerate(pools):
        chain = p.get("chain", "unknown")
        t0 = (p.get("token0") or {}).get("symbol", "?")
        t1 = (p.get("token1") or {}).get("symbol", "?")
        pool_id = p.get("id", "")
        raw_fee_tier = int(p.get("feeTier") or 0)
        fee_pct = raw_fee_tier / 10000
        pair = f"{t0}/{t1}"
        endpoint = get_graph_endpoint(chain, "v3")
        if endpoint:
            try:
                data = compute_fee_and_tvl_series(p, endpoint)
                pool_chart_data[pool_id] = {
                    **data,
                    "pool_id": pool_id,
                    "fee_pct": fee_pct,
                    "raw_fee_tier": raw_fee_tier,
                    "pair": pair,
                    "chain": chain,
                    "version": "v3",
                }
                print(f"  [{i+1}/{len(pools)}] {chain} {pair}: {len(data.get('fees') or [])} days")
            except Exception as e:
                print(f"  [{i+1}/{len(pools)}] {chain} {pair}: error - {e}")
            time.sleep(0.5)

    out_json = f"data/pools_v3_{suffix}.json"
    save_chart_data_json(pool_chart_data, out_json)
    print("Done. Output:", out_json)


if __name__ == "__main__":
    main()
