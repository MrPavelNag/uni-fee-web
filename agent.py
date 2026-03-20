#!/usr/bin/env python3
"""
Uniswap pool fee agent.

0. Finds Uniswap v3 pools for specified token pairs (e.g. fluid/eth) across all supported chains
1. Filters by TVL >= $1000
2. Saves result to available_pairs.pdf
3. Fetches fee data (PoolDayData) for 6 months, with $10k allocation
4. Outputs combined chart
"""

import argparse
import json
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
    UNISWAP_V4_SUBGRAPHS,
)


def _min_tvl(cli_value: Optional[float] = None) -> float:
    """Min TVL: --min-tvl > MIN_TVL env > config. MIN_TVL=0 = all pools."""
    if cli_value is not None:
        return float(cli_value)
    v = os.environ.get("MIN_TVL")
    if v is not None:
        try:
            return float(v)
        except ValueError:
            pass
    return MIN_TVL_USD
from agent_common import get_token_addresses
from uniswap_client import (
    get_graph_endpoint,
    query_pool_day_data,
    query_pools_containing_both_tokens,
    query_token_by_symbol,
)

DYNAMIC_TOKENS_PATH = "data/dynamic_tokens.json"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"


def load_dynamic_tokens() -> dict:
    """Load chain->symbol->address from data/dynamic_tokens.json. Filters out zero addresses."""
    if os.path.isfile(DYNAMIC_TOKENS_PATH):
        try:
            data = json.load(open(DYNAMIC_TOKENS_PATH))
            # Remove invalid zero addresses
            for chain in list(data.keys()):
                for sym in list(data[chain].keys()):
                    if (data[chain][sym] or "").lower() == ZERO_ADDRESS:
                        del data[chain][sym]
                if not data[chain]:
                    del data[chain]
            return data
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def save_dynamic_token(chain: str, symbol: str, address: str) -> None:
    """Append token to dynamic_tokens.json. Never saves zero address."""
    if not address or address.lower() == ZERO_ADDRESS:
        return
    data = load_dynamic_tokens()
    data.setdefault(chain, {})[symbol.lower()] = address.lower()
    os.makedirs("data", exist_ok=True)
    with open(DYNAMIC_TOKENS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def parse_token_pairs(s: str) -> list[tuple[str, str]]:
    """Parse 'fluid,eth' or 'fluid,eth;fluid,usdc' into [(fluid,eth), (fluid,usdc)]."""
    pairs = []
    for part in s.replace(" ", "").lower().split(";"):
        if "," in part:
            a, b = part.split(",", 1)
            pairs.append((a.strip(), b.strip()))
    return pairs or [("fluid", "eth")]


def pairs_to_filename_suffix(token_pairs_str: str) -> str:
    """Build filename suffix from token pairs: fluid,eth;inst,eth -> fluid_eth_inst_eth."""
    pairs = parse_token_pairs(token_pairs_str)
    parts = [f"{a}_{b}" for a, b in pairs]
    unique = list(dict.fromkeys(parts))
    return "_".join(unique) or "fluid_eth"


def discover_pools(
    token_pairs_str: str,
    min_tvl: Optional[float] = None,
    fresh_token_lookup: bool = False,
) -> list[dict]:
    """Discover pools for given token pairs across all chains (v3 and v4).
    Unknown tokens are resolved from subgraph and saved to data/dynamic_tokens.json.
    When fresh_token_lookup=True (TOKEN_PAIRS env set), always resolve tokens from subgraph.
    """
    pairs = parse_token_pairs(token_pairs_str)
    from config import GOLDSKY_ENDPOINTS

    dynamic_tokens = load_dynamic_tokens()
    all_pools = []
    chains = set(UNISWAP_V3_SUBGRAPHS.keys()) | set(UNISWAP_V4_SUBGRAPHS.keys()) | set(GOLDSKY_ENDPOINTS.keys())

    # Discover v3 and v4 separately, then merge onto one chart
    for version in ("v3", "v4"):
        for chain in chains:
            endpoint = get_graph_endpoint(chain, version)
            if not endpoint:
                continue
            for base, quote in pairs:
                base_addrs = []
                quote_addrs = []

                if fresh_token_lookup:
                    # Use config first; resolve from subgraph only when missing
                    def resolve_token(sym: str) -> list:
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

                    base_addrs = resolve_token(base)
                    quote_addrs = resolve_token(quote)
                else:
                    for c in [chain, "ethereum"]:
                        base_addrs.extend(get_token_addresses(c, base, dynamic_tokens))
                        quote_addrs.extend(get_token_addresses(c, quote, dynamic_tokens))
                    base_addrs = list(dict.fromkeys(base_addrs))[:1]
                    quote_addrs = list(dict.fromkeys(quote_addrs))[:1]

                    # Auto-resolve missing tokens from subgraph
                    if not base_addrs:
                        addr = query_token_by_symbol(endpoint, base)
                        if addr:
                            save_dynamic_token(chain, base, addr)
                            dynamic_tokens.setdefault(chain, {})[base.lower()] = addr
                            base_addrs = [addr]
                            print(f"  Resolved {base} on {chain}: {addr[:16]}...")
                    if not quote_addrs:
                        addr = query_token_by_symbol(endpoint, quote)
                        if addr:
                            save_dynamic_token(chain, quote, addr)
                            dynamic_tokens.setdefault(chain, {})[quote.lower()] = addr
                            quote_addrs = [addr]
                            print(f"  Resolved {quote} on {chain}: {addr[:16]}...")

                if not base_addrs or not quote_addrs:
                    continue
                min_tvl_val = _min_tvl(min_tvl)
                try:
                    pools = query_pools_containing_both_tokens(
                        endpoint, base_addrs[0], quote_addrs[0], min_tvl_val
                    )
                except Exception as e:
                    print(f"  [{chain}] {version} {base}/{quote}: {e}")
                    pools = []
                for p in pools:
                    p["chain"] = chain
                    p["version"] = version
                    p["pair_label"] = f"{base}/{quote}"
                    all_pools.append(p)
            time.sleep(0.5)
    # Deduplicate by (chain, version, pool_id) — v3 and v4 are different pools
    seen = set()
    unique = []
    for p in all_pools:
        key = (p.get("chain", ""), p.get("version", "v3"), p.get("id", ""))
        if key[2] and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def compute_fee_and_tvl_series(pool: dict, endpoint: str) -> dict:
    """
    Get daily fees for $10k allocation and TVL over 6 months.
    When feesUSD is 0, derive from volumeUSD * (feeTier/1e6) — some subgraphs don't populate feesUSD.
    Returns {"fees": [(ts, cumul_fee), ...], "tvl": [(ts, tvl_usd), ...]}.
    """
    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    start_ts = int(start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_ts = int(end.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())

    day_data = query_pool_day_data(endpoint, pool["id"], start_ts, end_ts)
    fee_tier = int(pool.get("feeTier") or 3000)  # 3000 = 0.3%, 10000 = 1%
    fee_series = []
    tvl_series = []
    cumul = 0.0
    for d in day_data:
        tvl = float(d.get("tvlUSD") or 0)
        fees = float(d.get("feesUSD") or 0)
        if fees <= 0:
            fees = 0.0
        if tvl > 0 and fees > 0:
            cumul += fees * (LP_ALLOCATION_USD / tvl)
        ts = int(d["date"])
        fee_series.append((ts, cumul))
        tvl_series.append((ts, tvl))
    return {"fees": fee_series, "tvl": tvl_series}


def save_pdf(pools: list[dict], path: str) -> None:
    """Save pool list to PDF."""
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    doc = SimpleDocTemplate(path, pagesize=A4, rightMargin=2 * cm, leftMargin=2 * cm)
    styles = getSampleStyleSheet()
    story = []
    story.append(Paragraph("Uniswap v3/v4 Pools (TVL &gt; $1000)", styles["Title"]))
    story.append(Spacer(1, 0.5 * cm))

    if not pools:
        story.append(Paragraph("No pools found.", styles["Normal"]))
    else:
        data = [["Chain", "Version", "Pair", "Pool", "Fee %", "TVL USD", "Volume USD"]]
        for p in pools:
            t0 = (p.get("token0") or {}).get("symbol", "?")
            t1 = (p.get("token1") or {}).get("symbol", "?")
            fee_bps = int(p.get("feeTier") or 0)
            # feeTier: 500=0.05%, 3000=0.3%, 10000=1% (value/10000 = %)
            fee_pct = fee_bps / 10000 if fee_bps else 0
            tvl = float(p.get("totalValueLockedUSD") or 0)
            vol = float(p.get("volumeUSD") or 0)
            data.append([
                p.get("chain", ""),
                p.get("version", "v3"),
                p.get("pair_label", f"{t0}/{t1}"),
                (p.get("id", "")[:20] + "...") if len(p.get("id", "")) > 20 else p.get("id", ""),
                f"{fee_pct}%",
                f"${tvl:,.0f}",
                f"${vol:,.0f}",
            ])
        t = Table(data, colWidths=[2.5 * cm, 1.5 * cm, 2.5 * cm, 4.5 * cm, 2 * cm, 3 * cm, 3.5 * cm])
        t.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ])
        )
        story.append(t)
    doc.build(story)
    print(f"Saved: {path}")


def save_chart(
    pool_chart_data: dict[str, dict],
    path: str,
) -> None:
    """
    Plot cumulative fees and TVL dynamics.
    pool_chart_data: {label -> {"fees": [...], "tvl": [...], "pool_id", "fee_pct", "pair"}}
    Same color per pool on both subplots.
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    # Assign one color per pool; reuse for both fee and TVL lines
    colors = plt.cm.tab10.colors
    color_cycle = [colors[i % len(colors)] for i in range(len(pool_chart_data))]

    for idx, (label, data) in enumerate(pool_chart_data.items()):
        fee_series = data.get("fees") or []
        tvl_series = data.get("tvl") or []
        pool_id = data.get("pool_id", "")
        fee_pct = data.get("fee_pct", 0)
        pair = data.get("pair", label)
        chain = data.get("chain", "")
        version = data.get("version", "v3")
        # Full address (v3 ~42 chars) or ...+last4 for v4 pool IDs (66 chars)
        pool_display = pool_id if len(pool_id) <= 42 else ("..." + pool_id[-4:])
        legend_label = f"{chain} {version} {pair} {pool_display} ({fee_pct:.2f}%)"
        color = color_cycle[idx]

        if fee_series:
            dates = [datetime.utcfromtimestamp(x[0]) for x in fee_series]
            values = [x[1] for x in fee_series]
            ax1.plot(dates, values, label=legend_label, linewidth=1.5, color=color)
        if tvl_series:
            dates = [datetime.utcfromtimestamp(x[0]) for x in tvl_series]
            values = [x[1] / 1e3 for x in tvl_series]  # TVL in thousands
            ax2.plot(dates, values, label=legend_label, linewidth=1.5, color=color)

    ax1.set_ylabel(f"Cumulative fees (USD), ${LP_ALLOCATION_USD:,.0f} allocation")
    ax1.set_title("Uniswap v3/v4: Cumulative LP fees (6 months)")
    ax1.legend(loc="upper left", fontsize=7)
    ax1.grid(True, linestyle="-", linewidth=0.3, alpha=0.7)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

    ax2.set_ylabel("TVL (USD, thousands)")
    ax2.set_xlabel("Date")
    ax2.set_title("TVL dynamics")
    ax2.legend(loc="upper left", fontsize=7)
    ax2.grid(True, linestyle="-", linewidth=0.3, alpha=0.7)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(path, format="pdf", bbox_inches="tight")
    plt.close()
    print(f"Saved: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Uniswap v3 pool fee agent")
    parser.add_argument(
        "--min-tvl",
        type=float,
        default=None,
        metavar="USD",
        help="Min TVL in USD (default: 100, 0=all pools). Overrides MIN_TVL env.",
    )
    args = parser.parse_args()

    token_pairs = os.environ.get("TOKEN_PAIRS", DEFAULT_TOKEN_PAIRS)
    min_tvl_val = _min_tvl(args.min_tvl)
    suffix = pairs_to_filename_suffix(token_pairs)
    output_pdf = f"data/available_pairs_{suffix}.pdf"
    output_chart = f"data/fee_chart_{suffix}.pdf"

    os.makedirs("data", exist_ok=True)
    print("Token pairs:", token_pairs)
    print("Min TVL: $%s" % f"{min_tvl_val:,.0f}".replace(",", " "))
    print("Discovering pools...")
    fresh_lookup = "TOKEN_PAIRS" in os.environ
    pools = discover_pools(token_pairs, args.min_tvl, fresh_token_lookup=fresh_lookup)
    print(f"Found {len(pools)} pools with TVL >= ${min_tvl_val:,.0f}")

    save_pdf(pools, output_pdf)

    pool_chart_data = {}
    for i, p in enumerate(pools):
        chain = p.get("chain", "unknown")
        t0 = (p.get("token0") or {}).get("symbol", "?")
        t1 = (p.get("token1") or {}).get("symbol", "?")
        pool_id = p.get("id", "")
        fee_bps = int(p.get("feeTier") or 0)
        fee_pct = fee_bps / 10000 if fee_bps else 0
        pair = f"{t0}/{t1}"
        label = f"{chain} {pair}"  # display label
        key = pool_id  # unique key: each pool is separate
        endpoint = get_graph_endpoint(chain, p.get("version", "v3"))
        if endpoint:
            try:
                data = compute_fee_and_tvl_series(p, endpoint)
                pool_chart_data[key] = {
                    **data,
                    "pool_id": pool_id,
                    "fee_pct": fee_pct,
                    "pair": pair,
                    "chain": chain,
                    "version": p.get("version", "v3"),
                }
                n = len(data.get("fees") or [])
                print(f"  [{i+1}/{len(pools)}] {label} {pool_id}: {n} days")
            except Exception as e:
                print(f"  [{i+1}/{len(pools)}] {label} {pool_id}: error - {e}")
            time.sleep(0.5)

    save_chart(pool_chart_data, output_chart)
    print("Done.")


if __name__ == "__main__":
    main()
