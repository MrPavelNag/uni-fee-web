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
    estimate_pool_tvl_usd_external_with_meta,
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
    query_pool_by_id,
    query_pools_containing_both_tokens,
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


def _is_timeout_error(err: Exception) -> bool:
    msg = str(err or "").lower()
    return ("read timed out" in msg) or ("timed out" in msg) or ("timeout" in msg)


def _discover_cap_hard(default_from_soft: int) -> int:
    env_v = _env_int("MAX_DISCOVERY_POOLS_PER_PAIR_CHAIN_HARD", 0)
    if env_v > 0:
        return max(env_v, default_from_soft)
    # Safety ceiling: allow automatic cap growth only for boundary-hit cases.
    return max(default_from_soft, 300)


def _eth_call_hex(rpc_url: str, to: str, data_hex: str, timeout_sec: float = 8.0) -> str:
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [{"to": str(to), "data": str(data_hex)}, "latest"],
    }
    r = requests.post(str(rpc_url), json=payload, timeout=max(2.0, float(timeout_sec)))
    r.raise_for_status()
    out = (r.json() or {}).get("result")
    return str(out or "")


def _abi_word_address(addr: str) -> str:
    a = str(addr or "").strip().lower().replace("0x", "")
    if len(a) != 40:
        return ""
    return "0" * 24 + a


def _abi_word_uint(x: int) -> str:
    return f"{int(x):064x}"


def _probe_unichain_v3_pool_ids(token_a: str, token_b: str) -> list[tuple[str, int]]:
    """
    Backward-compatible wrapper. Prefer _probe_v3_pool_ids(chain,...).
    """
    return _probe_v3_pool_ids("unichain", token_a, token_b)


_V3_FACTORY_BY_CHAIN: dict[str, str] = {
    "ethereum": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    "arbitrum": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    "optimism": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    "polygon": "0x1f98431c8ad98523631ae4a59f267346ea31f984",
    "base": "0x33128a8fc17869897dce68ed026d694621f6fdfd",
    "bsc": "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865",
    "unichain": "0x1f98400000000000000000000000000000000003",
}
_V3_RPC_BY_CHAIN: dict[str, str] = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
    "optimism": "https://optimism-rpc.publicnode.com",
    "polygon": "https://polygon-bor-rpc.publicnode.com",
    "base": "https://base-rpc.publicnode.com",
    "bsc": "https://bsc-rpc.publicnode.com",
    "unichain": "https://unichain-rpc.publicnode.com",
}


def _probe_v3_pool_ids(chain: str, token_a: str, token_b: str) -> list[tuple[str, int]]:
    chain_key = str(chain or "").strip().lower()
    cu = chain_key.upper().replace("-", "_")
    rpc = os.environ.get(f"V3_RPC_URL_{cu}", _V3_RPC_BY_CHAIN.get(chain_key, "")).strip()
    factory = os.environ.get(f"V3_FACTORY_OVERRIDE_{cu}", _V3_FACTORY_BY_CHAIN.get(chain_key, "")).strip()
    if not rpc or not factory:
        return []
    t0 = str(token_a or "").strip().lower()
    t1 = str(token_b or "").strip().lower()
    if len(t0) != 42 or len(t1) != 42:
        return []
    selector = "1698ee82"  # getPool(address,address,uint24)
    # Include common non-default tiers used by some deployments to avoid pool flapping
    # when subgraph intermittently misses an otherwise valid pool.
    fees = [100, 200, 250, 300, 400, 500, 1000, 2000, 2500, 3000, 4000, 10000]
    out: list[tuple[str, int]] = []
    seen: set[str] = set()
    for a, b in ((t0, t1), (t1, t0)):
        wa = _abi_word_address(a)
        wb = _abi_word_address(b)
        if not wa or not wb:
            continue
        for fee in fees:
            data = "0x" + selector + wa + wb + _abi_word_uint(fee)
            try:
                raw = _eth_call_hex(rpc, factory, data, timeout_sec=5.0)
                if not raw.startswith("0x") or len(raw) < 66:
                    continue
                pid = ("0x" + raw[-40:]).lower()
                if pid == "0x0000000000000000000000000000000000000000":
                    continue
                if pid not in seen:
                    seen.add(pid)
                    out.append((pid, int(fee)))
            except Exception:
                continue
    return out


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
    disable_symbol_fallback = _env_flag("DISABLE_V3_SYMBOL_FALLBACK", True)
    strict_errors = _env_flag("STRICT_DISCOVERY_ERRORS", False)
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

            known_a = bool(get_token_addresses(chain, base, dynamic_tokens) or get_token_addresses("ethereum", base, dynamic_tokens))
            known_b = bool(get_token_addresses(chain, quote, dynamic_tokens) or get_token_addresses("ethereum", quote, dynamic_tokens))
            allow_symbol_fallback = (not disable_symbol_fallback) and (not (known_a and known_b))

            def _discover_on_endpoint(ep: str, *, allow_symbol_fallback_local: bool) -> tuple[list[dict], bool]:
                pools, truncated = _discover_with_auto_expand(
                    lambda cap: query_pools_containing_both_tokens(
                        ep,
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
                print(
                    f"  [{chain}] v3 {base}/{quote}: "
                    f"resolved={str(base_addrs[0])[:10]}..,{str(quote_addrs[0])[:10]}.. "
                    f"pools={len(pools)}"
                )
                if (not pools) and allow_symbol_fallback_local:
                    # Fallback for ambiguous symbol->address mappings (e.g. multiple tokens with same symbol).
                    pools, trunc_sym = _discover_with_auto_expand(
                        lambda cap: query_pools_by_token_symbols(
                            ep,
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
                    print(f"  [{chain}] v3 {base}/{quote}: symbol_fallback_pools={len(pools)}")
                elif (not pools) and (not allow_symbol_fallback_local):
                    print(f"  [{chain}] v3 {base}/{quote}: skip symbol fallback (known token addresses)")
                return pools, truncated

            try:
                pools, truncated = _discover_on_endpoint(endpoint, allow_symbol_fallback_local=allow_symbol_fallback)
                # Root robustness: probe canonical V3 factory onchain and merge missing pool ids.
                try:
                    probed_items = _probe_v3_pool_ids(chain, base_addrs[0], quote_addrs[0])
                    if probed_items:
                        have_ids = {str((p or {}).get("id") or "").strip().lower() for p in (pools or [])}
                        added = 0
                        missing_in_subgraph = 0
                        skipped_no_entity = 0
                        for pid, _fee_tier in probed_items:
                            if pid in have_ids:
                                continue
                            p = query_pool_by_id(endpoint, pid)
                            if p:
                                pools.append(p)
                                have_ids.add(pid)
                                added += 1
                                continue
                            # Pool exists onchain but is absent in subgraph pool(id) response.
                            missing_in_subgraph += 1
                            skipped_no_entity += 1
                        if added > 0 or missing_in_subgraph > 0 or skipped_no_entity > 0:
                            pools = sorted(pools, key=_pool_tvl_usd, reverse=True)
                            if discovery_cap_hard > 0:
                                pools = pools[: int(discovery_cap_hard)]
                            print(
                                f"[probe] {chain} getPool pair={base}/{quote} "
                                f"probed={len(probed_items)} added={added} "
                                f"missing_in_subgraph={missing_in_subgraph} "
                                f"skipped_no_entity={skipped_no_entity} total={len(pools)}"
                            )
                except Exception as e:
                    if strict_errors:
                        raise RuntimeError(f"[{chain}] v3 onchain probe {base}/{quote}: {e}") from e
                    print(f"  [{chain}] v3 onchain probe {base}/{quote}: {e}")
                if truncated:
                    print(f"[warn] DISCOVERY_POTENTIALLY_TRUNCATED chain={chain} pair={base}/{quote}")
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
                if strict_errors:
                    raise RuntimeError(f"[{chain}] v3 {base}/{quote}: {e}") from e
                print(f"  [{chain}] v3 {base}/{quote}: {e}")
        return out_chain

    if chains:
        with ThreadPoolExecutor(max_workers=chain_workers) as ex:
            futs = [ex.submit(_discover_for_chain, c) for c in sorted(chains)]
            for fut in as_completed(futs):
                try:
                    all_pools.extend(fut.result() or [])
                except Exception as e:
                    if strict_errors:
                        raise RuntimeError(f"[v3-discovery] chain worker error: {e}") from e
                    print(f"  [v3-discovery] chain worker error: {e}")
    seen = set()
    unique = []
    for p in all_pools:
        key = (p.get("chain", ""), p.get("id", ""))
        if key[1] and key not in seen:
            seen.add(key)
            unique.append(p)
    return unique


def compute_fee_and_tvl_series(
    pool: dict,
    endpoint: str,
    day_data: Optional[list[dict]] = None,
    tvl_multiplier: float = 1.0,
) -> dict:
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
    mult = max(0.01, float(tvl_multiplier or 1.0))
    for d in day_data:
        tvl = float(d.get("tvlUSD") or 0) * mult
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
    print(
        "[perf] pool_day_batch_size=%s pool_day_retries=%s per_id_fallback=%s"
        % (
            str(max(1, min(40, _env_int("POOL_DAY_BATCH_SIZE", 12)))),
            str(os.environ.get("GRAPHQL_POOL_DAY_RETRIES", "1")),
            str(os.environ.get("GRAPHQL_POOL_DAY_DISABLE_PER_ID_FALLBACK", "1")),
        )
    )
    print("Discovering v3 pools...")
    fresh = "TOKEN_PAIRS" in os.environ
    t_discover0 = time.perf_counter()
    pools = discover_pools_v3(token_pairs, args.min_tvl, fresh_token_lookup=fresh)
    discover_ms = int(round(max(0.0, time.perf_counter() - t_discover0) * 1000.0))
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
    print(f"Found {len(pools)} v3 pools")

    if os.environ.get("DISABLE_PDF_OUTPUT", "").strip().lower() not in ("1", "true", "yes", "on"):
        save_pdf(pools, str(output_dir / f"available_pairs_v3_{suffix}.pdf"))
    else:
        print("PDF output disabled (DISABLE_PDF_OUTPUT=1)")

    pool_chart_data = {}
    max_workers = max(1, min(16, int(os.environ.get("POOL_SERIES_WORKERS", "8"))))
    batch_size = max(1, min(40, _env_int("POOL_DAY_BATCH_SIZE", 12)))
    day_data_by_pool: dict[str, list[dict]] = {}
    strict_errors = _env_flag("STRICT_DISCOVERY_ERRORS", False)
    t_daydata0 = time.perf_counter()
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
                if strict_errors:
                    raise RuntimeError(f"[v3-batch-daydata] {e}") from e
                print(f"  [v3-batch-daydata] {e}")
    daydata_ms = int(round(max(0.0, time.perf_counter() - t_daydata0) * 1000.0))

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
            if strict_errors:
                raise RuntimeError(f"[{chain}] v3 {pair}: no endpoint")
            return idx, None, None, f"  [{idx+1}/{len(pools)}] {chain} {pair}: skipped (no endpoint)"
        # Strict batch-only mode: avoid per-pool GraphQL fallback requests.
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
                        f"[{chain}] v3 {pair}: external TVL unavailable: {price_err or 'unknown'}"
                    )
                msg = (
                    f"  [{idx+1}/{len(pools)}] {chain} {pair}: "
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
        data = compute_fee_and_tvl_series(pool, endpoint, day_data=day_rows, tvl_multiplier=tvl_multiplier)
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
            "tvl_multiplier": float(tvl_multiplier),
            "tvl_price_source": str(pool.get("tvl_price_source") or ""),
            "pair": pair,
            "chain": chain,
            "version": "v3",
        }
        return idx, pool_id, payload, f"  [{idx+1}/{len(pools)}] {chain} {pair}: {len(data.get('fees') or [])} days"

    t_series0 = time.perf_counter()
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
                    if strict_errors:
                        raise RuntimeError(f"[v3-series] {e}") from e
                    print(f"  [series] error - {e}")
    series_ms = int(round(max(0.0, time.perf_counter() - t_series0) * 1000.0))

    out_json = output_dir / f"pools_v3_{suffix}.json"
    save_chart_data_json(pool_chart_data, str(out_json))
    print(
        f"[timing] v3 stages: discover={discover_ms}ms daydata_batch={daydata_ms}ms series={series_ms}ms total={discover_ms + daydata_ms + series_ms}ms"
    )
    print("Done. Output:", str(out_json))


if __name__ == "__main__":
    main()
