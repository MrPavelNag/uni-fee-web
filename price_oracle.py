"""
External token price oracle for project scripts.

Priority (stability-first for canonical TVL):
1) CoinGecko
2) DefiLlama
3) Dexscreener (last-resort fallback)
"""

import os
import math
import threading
import time
from typing import Any

import requests

_CACHE_LOCK = threading.Lock()
_PRICE_CACHE: dict[tuple[str, str], tuple[float, float, str]] = {}


def _safe_float(v: Any) -> float:
    try:
        x = float(v or 0.0)
        if not math.isfinite(x):
            return 0.0
        return x
    except Exception:
        return 0.0


def _cache_ttl_sec() -> float:
    try:
        return max(30.0, float(os.environ.get("TOKEN_PRICE_CACHE_TTL_SEC", "180")))
    except Exception:
        return 180.0


def _allow_dexscreener_fallback() -> bool:
    v = str(os.environ.get("TOKEN_PRICE_ALLOW_DEXSCREENER_FALLBACK", "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _max_source_divergence() -> float:
    try:
        return max(0.05, min(0.95, float(os.environ.get("TOKEN_PRICE_MAX_SOURCE_DIVERGENCE", "0.35"))))
    except Exception:
        return 0.35


def _stable_peg_enabled() -> bool:
    v = str(os.environ.get("TOKEN_PRICE_DISABLE_STABLE_PEG", "0") or "").strip().lower()
    return v not in {"1", "true", "yes", "on"}


# Curated fiat-pegged symbols whose canonical addresses live in config.TOKEN_ADDRESSES.
_FIAT_STABLE_CONFIG_KEYS = frozenset({"usdc", "usdt", "dai", "frax"})
_STABLE_ADDR_BY_CHAIN: dict[str, frozenset[str]] = {}


def _fiat_stable_addresses_for_chain(chain: str) -> frozenset[str]:
    """Contract addresses we treat as ~$1 for TVL when oracles disagree or are missing."""
    c = _norm_chain(chain)
    cached = _STABLE_ADDR_BY_CHAIN.get(c)
    if cached is not None:
        return cached
    from config import TOKEN_ADDRESSES

    out: set[str] = set()
    for sym, addr in (TOKEN_ADDRESSES.get(c) or {}).items():
        if sym not in _FIAT_STABLE_CONFIG_KEYS:
            continue
        s = str(addr or "").strip().lower()
        if not s.startswith("0x") or len(s) != 42:
            continue
        try:
            if int(s, 16) == 0:
                continue
        except Exception:
            continue
        out.add(s)
    fs = frozenset(out)
    _STABLE_ADDR_BY_CHAIN[c] = fs
    return fs


def _resolve_fiat_stable_usd(cg: float, ll: float, max_div: float) -> tuple[float, str]:
    """USD price for a known fiat stable: tolerate oracle disagreement; clamp single-source spikes."""
    if cg > 0 and ll > 0:
        rel = abs(cg - ll) / max(cg, ll)
        if rel <= max_div:
            return (cg + ll) / 2.0, "coingecko+defillama"
        return 1.0, "stable_peg_divergence"
    v = cg if cg > 0 else ll
    if v > 0:
        lo, hi = 0.92, 1.08
        vc = min(max(v, lo), hi)
        base = "coingecko" if cg > 0 else "defillama"
        if abs(vc - v) > 1e-9:
            return vc, f"{base}+stable_clamp"
        return v, base
    return 1.0, "stable_peg_default"


def _norm_chain(chain: str) -> str:
    c = str(chain or "").strip().lower()
    if c == "arbitrum-one":
        return "arbitrum"
    return c


def _dex_chain_id(chain: str) -> str:
    c = _norm_chain(chain)
    mapping = {
        "ethereum": "ethereum",
        "arbitrum": "arbitrum",
        "optimism": "optimism",
        "polygon": "polygon",
        "base": "base",
        "bsc": "bsc",
        "avalanche": "avalanche",
        "celo": "celo",
        "unichain": "unichain",
    }
    return mapping.get(c, c)


def _coingecko_platform(chain: str) -> str:
    c = _norm_chain(chain)
    mapping = {
        "ethereum": "ethereum",
        "arbitrum": "arbitrum-one",
        "optimism": "optimistic-ethereum",
        "polygon": "polygon-pos",
        "base": "base",
        "bsc": "binance-smart-chain",
        "avalanche": "avalanche",
        "celo": "celo",
        "unichain": "unichain",
    }
    return mapping.get(c, "")


def _llama_chain(chain: str) -> str:
    c = _norm_chain(chain)
    mapping = {
        "ethereum": "ethereum",
        "arbitrum": "arbitrum",
        "optimism": "optimism",
        "polygon": "polygon",
        "base": "base",
        "bsc": "bsc",
        "avalanche": "avax",
        "celo": "celo",
        "unichain": "unichain",
    }
    return mapping.get(c, c)


def _cache_get(chain: str, addr: str) -> tuple[float, str]:
    now = time.time()
    ttl = _cache_ttl_sec()
    key = (_norm_chain(chain), str(addr or "").strip().lower())
    with _CACHE_LOCK:
        rec = _PRICE_CACHE.get(key)
        if not rec:
            return 0.0, ""
        ts, px, src = rec
        if (now - float(ts)) > ttl or float(px) <= 0:
            return 0.0, ""
        # Conservative mode: ignore noisy cache entries sourced from Dexscreener
        # unless explicit fallback is enabled.
        if str(src or "").strip().lower() == "dexscreener" and not _allow_dexscreener_fallback():
            return 0.0, ""
        return float(px), str(src or "")


def _cache_put(chain: str, addr: str, price_usd: float, source: str) -> None:
    px = float(price_usd or 0.0)
    if px <= 0:
        return
    key = (_norm_chain(chain), str(addr or "").strip().lower())
    with _CACHE_LOCK:
        _PRICE_CACHE[key] = (time.time(), px, str(source or ""))


def _fetch_dexscreener(chain: str, addresses: list[str]) -> tuple[dict[str, float], dict[str, str]]:
    out: dict[str, float] = {}
    src: dict[str, str] = {}
    chain_id = _dex_chain_id(chain)
    for addr in addresses:
        try:
            r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{addr}", timeout=8)
            r.raise_for_status()
            payload = r.json() if isinstance(r.json(), dict) else {}
            pairs = payload.get("pairs") if isinstance(payload, dict) else None
            if not isinstance(pairs, list):
                continue
            best_price = 0.0
            best_liq = -1.0
            for p in pairs:
                if not isinstance(p, dict):
                    continue
                if str(p.get("chainId") or "").strip().lower() != chain_id:
                    continue
                price = _safe_float(p.get("priceUsd"))
                if price <= 0:
                    continue
                liq = _safe_float((p.get("liquidity") or {}).get("usd"))
                if liq > best_liq:
                    best_liq = liq
                    best_price = price
            if best_price > 0:
                out[addr] = best_price
                src[addr] = "dexscreener"
        except Exception:
            continue
    return out, src


def _fetch_coingecko(chain: str, addresses: list[str]) -> tuple[dict[str, float], dict[str, str]]:
    out: dict[str, float] = {}
    src: dict[str, str] = {}
    platform = _coingecko_platform(chain)
    if not platform:
        return out, src
    url = f"https://api.coingecko.com/api/v3/simple/token_price/{platform}"
    for addr in addresses:
        try:
            r = requests.get(url, params={"contract_addresses": addr, "vs_currencies": "usd"}, timeout=8)
            r.raise_for_status()
            payload = r.json() if isinstance(r.json(), dict) else {}
            if not isinstance(payload, dict):
                continue
            row = payload.get(addr) or payload.get(addr.lower()) or payload.get(addr.upper())
            price = _safe_float((row or {}).get("usd") if isinstance(row, dict) else 0.0)
            if price > 0:
                out[addr] = price
                src[addr] = "coingecko"
        except Exception:
            continue
    return out, src


def _fetch_defillama(chain: str, addresses: list[str]) -> tuple[dict[str, float], dict[str, str]]:
    out: dict[str, float] = {}
    src: dict[str, str] = {}
    chain_key = _llama_chain(chain)
    keys = [f"{chain_key}:{a}" for a in addresses if a]
    if not keys:
        return out, src
    try:
        joined = ",".join(keys)
        r = requests.get(f"https://coins.llama.fi/prices/current/{joined}", timeout=8)
        r.raise_for_status()
        payload = r.json() if isinstance(r.json(), dict) else {}
        coins = payload.get("coins") if isinstance(payload, dict) else None
        if not isinstance(coins, dict):
            return out, src
        for a in addresses:
            row = coins.get(f"{chain_key}:{a}") or {}
            price = _safe_float((row or {}).get("price") if isinstance(row, dict) else 0.0)
            if price > 0:
                out[a] = price
                src[a] = "defillama"
    except Exception:
        return out, src
    return out, src


def get_token_prices_usd(chain: str, token_addresses: list[str]) -> tuple[dict[str, float], dict[str, str], list[str]]:
    """
    Return (prices, sources, errors).
    - prices: address -> USD price
    - sources: address -> provider name
    - errors: textual diagnostics
    """
    addrs = [str(a or "").strip().lower() for a in (token_addresses or []) if str(a or "").strip()]
    addrs = list(dict.fromkeys(addrs))
    prices: dict[str, float] = {}
    sources: dict[str, str] = {}
    errors: list[str] = []
    if not addrs:
        return prices, sources, errors

    pending: list[str] = []
    for a in addrs:
        px, src = _cache_get(chain, a)
        if px > 0:
            prices[a] = px
            sources[a] = src or "cache"
        else:
            pending.append(a)
    if not pending:
        return prices, sources, errors

    # Canonical TVL should prefer slower but more stable aggregators first.
    # Dexscreener is useful as fallback, but can be noisy for certain tokens/pairs.
    # Stability-first canonical path:
    # 1) coingecko + defillama cross-check
    # 2) optional dexscreener fallback only when explicitly enabled
    cg_prices, _cg_sources = _fetch_coingecko(chain, pending)
    ll_prices, _ll_sources = _fetch_defillama(chain, pending)
    unresolved: list[str] = []
    max_div = _max_source_divergence()
    stable_addrs = _fiat_stable_addresses_for_chain(chain) if _stable_peg_enabled() else frozenset()

    for a in list(pending):
        cg = _safe_float(cg_prices.get(a))
        ll = _safe_float(ll_prices.get(a))
        chosen = 0.0
        src = ""
        if a in stable_addrs:
            chosen, src = _resolve_fiat_stable_usd(cg, ll, max_div)
            if cg > 0 and ll > 0:
                rel = abs(cg - ll) / max(cg, ll)
                if rel > max_div:
                    errors.append(
                        f"price_divergence_stable_peg chain={_norm_chain(chain)} token={a} "
                        f"cg={cg:.10g} llama={ll:.10g} rel={rel:.3f} used_usd={chosen:.6g} src={src}"
                    )
        elif cg > 0 and ll > 0:
            rel = abs(cg - ll) / max(cg, ll)
            if rel <= max_div:
                chosen = (cg + ll) / 2.0
                src = "coingecko+defillama"
            else:
                errors.append(
                    f"price_divergence chain={_norm_chain(chain)} token={a} cg={cg:.10g} llama={ll:.10g} rel={rel:.3f}"
                )
        elif cg > 0:
            chosen = cg
            src = "coingecko"
        elif ll > 0:
            chosen = ll
            src = "defillama"
        if chosen > 0:
            prices[a] = chosen
            sources[a] = src
            _cache_put(chain, a, chosen, src)
        else:
            unresolved.append(a)

    if unresolved and _allow_dexscreener_fallback():
        dex_prices, _dex_sources = _fetch_dexscreener(chain, unresolved)
        still_unresolved: list[str] = []
        for a in unresolved:
            px = _safe_float(dex_prices.get(a))
            if px > 0:
                prices[a] = px
                sources[a] = "dexscreener"
                _cache_put(chain, a, px, "dexscreener")
            else:
                still_unresolved.append(a)
        pending = still_unresolved
    else:
        pending = unresolved

    if pending:
        errors.append(
            f"price_unavailable chain={_norm_chain(chain)} unresolved={','.join(pending)}"
        )
    return prices, sources, errors
