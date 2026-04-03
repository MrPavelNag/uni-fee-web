"""
Shared logic for Uniswap pool fee agents (v3, v4, merge).
"""

import json
import os
import math
from datetime import datetime
from typing import Any, Optional

from config import FEE_DAYS, LP_ALLOCATION_USD, TOKEN_ADDRESSES
from price_oracle import get_token_prices_usd

DYNAMIC_TOKENS_PATH = "data/dynamic_tokens.json"
ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"

# Webapp writes top-N-by-TVL token addresses here; agents read via get_token_addresses.
# Override absolute path when spawned from webapp: MAJOR_TOKENS_CACHE_PATH
_MAJOR_SYM_BY_CHAIN: dict[str, dict[str, list[str]]] | None = None


def _major_tokens_cache_path() -> str:
    return os.environ.get("MAJOR_TOKENS_CACHE_PATH") or os.path.join("data", "major_tokens_by_chain.json")


def _load_major_symbol_by_chain() -> dict[str, dict[str, list[str]]]:
    """chain -> symbol(lower) -> [addresses] из major_tokens_by_chain.json (by_chain: addr -> SYMBOL)."""
    path = _major_tokens_cache_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    by_chain = raw.get("by_chain")
    if not isinstance(by_chain, dict):
        return {}
    out: dict[str, dict[str, list[str]]] = {}
    for ck, addr_map in by_chain.items():
        if not isinstance(addr_map, dict):
            continue
        ckey = str(ck).strip().lower()
        sym_bucket: dict[str, list[str]] = {}
        for addr, sym in addr_map.items():
            a = str(addr).strip().lower()
            s = str(sym or "").strip().lower()
            if not a or not s or a == ZERO_ADDRESS:
                continue
            sym_bucket.setdefault(s, []).append(a)
        for s, lst in sym_bucket.items():
            sym_bucket[s] = sorted(set(lst))
        if sym_bucket:
            out[ckey] = sym_bucket
    return out


def _major_symbol_index() -> dict[str, dict[str, list[str]]]:
    global _MAJOR_SYM_BY_CHAIN
    if _MAJOR_SYM_BY_CHAIN is None:
        _MAJOR_SYM_BY_CHAIN = _load_major_symbol_by_chain()
    return _MAJOR_SYM_BY_CHAIN


def parse_token_pairs(s: str) -> list[tuple[str, str]]:
    """Parse 'fluid,eth' or 'fluid,eth;fluid,usdc' into [(fluid,eth), (fluid,usdc)]."""
    pairs = []
    for part in s.replace(" ", "").lower().split(";"):
        if "," in part:
            a, b = part.split(",", 1)
            pairs.append((a.strip(), b.strip()))
    return pairs or [("fluid", "eth")]


def pairs_to_filename_suffix(token_pairs_str: str) -> str:
    """Build filename suffix: fluid,eth;inst,eth -> fluid_eth_inst_eth."""
    pairs = parse_token_pairs(token_pairs_str)
    parts = [f"{a}_{b}" for a, b in pairs]
    unique = list(dict.fromkeys(parts))
    return "_".join(unique) or "fluid_eth"


def build_exact_day_window(days: int) -> tuple[int, int]:
    """
    Return [start_ts, end_ts] aligned to UTC day starts with an inclusive range
    containing exactly `days` calendar days.
    """
    d = max(1, int(days or 1))
    end_day_start = int(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    start_day_start = int(end_day_start - (d - 1) * 86400)
    return int(start_day_start), int(end_day_start)


def load_dynamic_tokens() -> dict:
    """Load chain->symbol->address from data/dynamic_tokens.json."""
    if os.path.isfile(DYNAMIC_TOKENS_PATH):
        try:
            data = json.load(open(DYNAMIC_TOKENS_PATH))
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
    """Append token to dynamic_tokens.json."""
    if not address or address.lower() == ZERO_ADDRESS:
        return
    data = load_dynamic_tokens()
    data.setdefault(chain, {})[symbol.lower()] = address.lower()
    os.makedirs("data", exist_ok=True)
    with open(DYNAMIC_TOKENS_PATH, "w") as f:
        json.dump(data, f, indent=2)


def get_token_addresses(
    chain: str,
    token_symbol: str,
    dynamic_tokens: Optional[dict] = None,
) -> list[str]:
    """Get token address: curated config → top-N cache (same as web catalog) → dynamic_tokens."""
    chain = str(chain or "").strip().lower()
    if chain == "arbitrum-one":
        chain = "arbitrum"
    cfg = TOKEN_ADDRESSES.get(chain, {})
    dyn_all = dynamic_tokens or {}
    dyn = dict(dyn_all.get(chain, {}) or {})
    if chain == "arbitrum":
        legacy = dyn_all.get("arbitrum-one", {})
        if isinstance(legacy, dict):
            for ks, va in legacy.items():
                dyn.setdefault(ks, va)
    sym = token_symbol.lower()
    addrs = []
    a = cfg.get(sym) or cfg.get(token_symbol)
    if a and a.lower() != ZERO_ADDRESS:
        addrs.append(a)
    if token_symbol.lower() == "eth":
        a = cfg.get("weth")
        if a and a.lower() != ZERO_ADDRESS and a not in addrs:
            addrs.append(a)
    ck = str(chain or "").strip().lower()
    major = _major_symbol_index().get(ck, {})
    for addr in major.get(sym, []):
        if addr and addr.lower() != ZERO_ADDRESS and addr not in addrs:
            addrs.append(addr)
    a = dyn.get(sym)
    if a and a.lower() != ZERO_ADDRESS and a not in addrs:
        addrs.append(a)
    return addrs


def _safe_float(v: Any) -> float:
    try:
        x = float(v or 0.0)
        if not math.isfinite(x):
            return 0.0
        return x
    except Exception:
        return 0.0


def estimate_pool_tvl_usd_external_with_meta(pool: dict, chain: str) -> tuple[float, str, str]:
    """
    Canonical TVL estimate from reserve amounts * external USD prices.
    """
    token0 = str(((pool.get("token0") or {}).get("id") or "")).strip().lower()
    token1 = str(((pool.get("token1") or {}).get("id") or "")).strip().lower()
    if not token0 or not token1:
        return 0.0, "", "missing_pool_tokens"
    amt0 = _safe_float(pool.get("totalValueLockedToken0"))
    amt1 = _safe_float(pool.get("totalValueLockedToken1"))
    if amt0 <= 0 and amt1 <= 0:
        return 0.0, "", "missing_pool_reserves"
    prices, sources, errors = get_token_prices_usd(chain, [token0, token1])
    p0 = _safe_float(prices.get(token0))
    p1 = _safe_float(prices.get(token1))
    if p0 <= 0 and p1 <= 0:
        err = "; ".join([e for e in errors if e]) or "external_price_not_found"
        return 0.0, "", err
    # If one leg price is missing, infer by pool ratio when available.
    ratio01 = _safe_float(pool.get("token0Price"))  # token1 per token0
    if p0 <= 0 and p1 > 0 and ratio01 > 0:
        p0 = p1 * ratio01
    if p1 <= 0 and p0 > 0 and ratio01 > 0:
        p1 = p0 / ratio01
    tvl = max(0.0, amt0 * p0) + max(0.0, amt1 * p1)
    source = str(sources.get(token0) or sources.get(token1) or "")
    if p0 > 0 and p1 > 0:
        s0 = str(sources.get(token0) or "")
        s1 = str(sources.get(token1) or "")
        if s0 and s1 and s0 != s1:
            source = f"{s0}+{s1}"
        elif s0 and s1:
            source = s0
    return float(tvl), source, ""


def estimate_pool_tvl_usd_external(pool: dict, chain: str) -> float:
    tvl, _, _ = estimate_pool_tvl_usd_external_with_meta(pool, chain)
    return float(tvl)


def resolve_pool_tvl_now_external(
    pool: dict,
    chain: str,
    *,
    write_back: bool = True,
) -> tuple[float, str, str]:
    """
    Canonical "TVL now" rule used across the whole app:
    current reserves * external token USD prices.

    Returns: (tvl_usd, price_source, error)
    Optionally writes canonical fields back into `pool`.
    """
    tvl_usd, price_source, err = estimate_pool_tvl_usd_external_with_meta(pool, chain)
    if write_back:
        try:
            if float(tvl_usd) > 0:
                pool["effectiveTvlUSD"] = float(tvl_usd)
            pool["tvl_price_source"] = str(price_source or pool.get("tvl_price_source") or "external")
        except Exception:
            pass
    return float(tvl_usd), str(price_source or ""), str(err or "")


def save_chart(pool_chart_data: dict[str, dict], path: str) -> None:
    """Plot cumulative fees and TVL. pool_chart_data: {pool_id -> {fees, tvl, pool_id, fee_pct, pair, chain, version}}."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    # Сортируем по кумулятивному доходу в конце периода (убывание)
    items = sorted(pool_chart_data.items(), key=lambda x: _final_income(x[1]), reverse=True)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)
    # Начинаем с тёмных тонов: Dark2, Set1, tab10
    colors = list(plt.cm.Dark2.colors) + list(plt.cm.Set1.colors) + list(plt.cm.tab10.colors)
    linestyles = ["-", "--", "-.", ":"]

    for idx, (label, data) in enumerate(items):
        fee_series = data.get("fees") or []
        tvl_series = data.get("tvl") or []
        pool_id = data.get("pool_id", "")
        fee_pct = _display_fee_pct(data)
        pair = data.get("pair", label)
        chain = data.get("chain", "")
        version = data.get("version", "v3")
        pool_display = pool_id if len(pool_id) <= 42 else ("..." + pool_id[-4:])

        cumul_usd = _final_income(data)
        apy = (cumul_usd / LP_ALLOCATION_USD) * (365 / FEE_DAYS) * 100 if LP_ALLOCATION_USD > 0 else 0
        legend_label = (
            f"{chain} {version} {pair} {pool_display} "
            f"| fee {fee_pct:.2f}% | ${cumul_usd:,.0f} ({apy:.1f}% APY)"
        )

        color = colors[idx % len(colors)]
        linestyle = linestyles[idx % len(linestyles)]

        if fee_series:
            dates = [datetime.utcfromtimestamp(x[0]) for x in fee_series]
            values = [x[1] for x in fee_series]
            ax1.plot(dates, values, label=legend_label, linewidth=1.5, color=color, linestyle=linestyle)
        if tvl_series:
            dates = [datetime.utcfromtimestamp(x[0]) for x in tvl_series]
            values = [x[1] / 1e3 for x in tvl_series]
            ax2.plot(dates, values, label=legend_label, linewidth=1.5, color=color, linestyle=linestyle)

    ax1.set_ylabel(f"Cumulative fees (USD), ${LP_ALLOCATION_USD:,.0f} allocation")
    ax1.set_title("Uniswap v3/v4: Cumulative LP fees (%d days)" % FEE_DAYS)
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


def save_chart_data_json(pool_chart_data: dict[str, dict], path: str) -> None:
    """Save pool_chart_data to JSON (serializable format)."""
    out = {}
    for k, v in pool_chart_data.items():
        out[k] = {
            "fees": v.get("fees") or [],
            "tvl": v.get("tvl") or [],
            "pool_id": v.get("pool_id", k),
            "fee_pct": v.get("fee_pct", 0),
            "raw_fee_tier": v.get("raw_fee_tier"),
            "pool_tvl_now_usd": v.get("pool_tvl_now_usd"),
            "pool_tvl_subgraph_usd": v.get("pool_tvl_subgraph_usd"),
            "tvl_multiplier": v.get("tvl_multiplier"),
            "tvl_price_source": v.get("tvl_price_source"),
            "pair": v.get("pair", ""),
            "chain": v.get("chain", ""),
            "version": v.get("version", "v3"),
            "data_quality": v.get("data_quality"),
            "data_quality_reason": v.get("data_quality_reason"),
            "strict_compare_estimated_tvl": v.get("strict_compare_estimated_tvl") or [],
            "strict_compare_estimated_fees": v.get("strict_compare_estimated_fees") or [],
            "strict_compare_exact_legacy_tvl": v.get("strict_compare_exact_legacy_tvl") or [],
            "strict_compare_exact_legacy_fees": v.get("strict_compare_exact_legacy_fees") or [],
            "strict_compare_exact_tvl": v.get("strict_compare_exact_tvl") or [],
            "strict_compare_exact_fees": v.get("strict_compare_exact_fees") or [],
        }
    os.makedirs("data", exist_ok=True)
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Saved: {path}")


def _final_income(data: dict) -> float:
    """Кумулятивный доход в конце периода (как в легенде графика)."""
    fees = data.get("fees") or []
    return fees[-1][1] if fees else 0.0


def _normalize_fee_pct(raw_fee_tier: int, version: str) -> float:
    """feeTier → процент. v3: value/10000 (bps). v4 иногда возвращает другие единицы — ограничиваем 3%."""
    pct = (raw_fee_tier or 0) / 10000
    if pct <= 3.0:
        return round(pct, 2)
    # v4 subgraph иногда отдаёт значение в других единицах (например 8388608 → 838%)
    if version == "v4" and raw_fee_tier > 100000:
        pct = raw_fee_tier / 1e6  # 8388608 → 8.39%
    return min(3.0, round(pct, 2))


def _display_fee_pct(data: dict) -> float:
    """Процент комиссии для отображения (не более 3%)."""
    return min(3.0, float(data.get("fee_pct") or 0))


def _is_bad_fee_entry(data: dict) -> bool:
    """fee > 3% — считаем данные subgraph ошибочными."""
    try:
        raw = data.get("raw_fee_tier")
        if raw is not None:
            raw_int = int(raw)
            # v3/v4 bps: value/10000 = %
            if (raw_int or 0) / 10000.0 > 3.0:
                return True
            # для v4 иногда встречаются очень большие числа в других единицах
            if raw_int > 100000 and (raw_int / 1e6) > 3.0:
                return True
    except (TypeError, ValueError):
        pass
    try:
        fee_pct = float(data.get("fee_pct") or 0)
        return fee_pct > 3.0
    except (TypeError, ValueError):
        return False


def save_merged_list_pdf(pool_chart_data: dict[str, dict], path: str) -> None:
    """Единый PDF-список v3+v4 пулов.

    Сначала — «нормальные» пулы (fee ≤ 3%), затем в конце списка — пулы с fee > 3%,
    помеченные как полученные с ошибкой.
    """
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    good_items = []
    bad_items = []
    for key, val in pool_chart_data.items():
        if _is_bad_fee_entry(val):
            bad_items.append((key, val))
        else:
            good_items.append((key, val))

    good_items.sort(key=lambda x: _final_income(x[1]), reverse=True)
    bad_items.sort(key=lambda x: _final_income(x[1]), reverse=True)

    items = good_items + bad_items
    doc = SimpleDocTemplate(path, pagesize=landscape(A4), rightMargin=1.5 * cm, leftMargin=1.5 * cm)
    story = [
        Paragraph("Uniswap v3 + v4 Pools (по убыванию дохода)", getSampleStyleSheet()["Title"]),
        Spacer(1, 0.5 * cm),
    ]
    if not items:
        story.append(Paragraph("No pools.", getSampleStyleSheet()["Normal"]))
    else:
        data = [["Chain", "Version", "Pair", "Pool ID", "Fee %", "Cumul $", "TVL (last)", "Status"]]
        for _pool_id, v in items:
            fees = v.get("fees") or []
            tvl_series = v.get("tvl") or []
            cumul = fees[-1][1] if fees else 0
            last_tvl = tvl_series[-1][1] if tvl_series else 0
            is_bad = _is_bad_fee_entry(v)
            data.append([
                v.get("chain", ""),
                v.get("version", ""),
                v.get("pair", ""),
                v.get("pool_id", _pool_id),
                "%.2f%%" % _display_fee_pct(v),
                "$%.0f" % cumul,
                "$%.0f" % last_tvl,
                "fee>3% (ошибка)" if is_bad else "",
            ])
        t = Table(
            data,
            colWidths=[2.0 * cm, 1.5 * cm, 3.0 * cm, 9.0 * cm, 2.0 * cm, 2.3 * cm, 3.0 * cm, 3.0 * cm],
        )
        t.setStyle(
            TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("FONTSIZE", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
                ("BACKGROUND", (0, 1), (-1, -1), colors.beige),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
            ])
        )
        story.append(t)
    doc.build(story)
    print(f"Saved: {path}")


def load_chart_data_json(path: str) -> dict[str, dict]:
    """Load pool_chart_data from JSON."""
    if not os.path.isfile(path):
        return {}
    with open(path) as f:
        return json.load(f)
