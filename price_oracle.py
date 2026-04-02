"""
External token price oracle for project scripts.

Priority:
1) Dexscreener
2) CoinGecko
3) DefiLlama
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

    for fetcher_name, fetcher in (
        ("dexscreener", _fetch_dexscreener),
        ("coingecko", _fetch_coingecko),
        ("defillama", _fetch_defillama),
    ):
        if not pending:
            break
        got_prices, got_sources = fetcher(chain, pending)
        for a in list(pending):
            px = _safe_float(got_prices.get(a))
            if px <= 0:
                continue
            src = str(got_sources.get(a) or fetcher_name)
            prices[a] = px
            sources[a] = src
            _cache_put(chain, a, px, src)
            pending.remove(a)

    if pending:
        errors.append(
            f"price_unavailable chain={_norm_chain(chain)} unresolved={','.join(pending)}"
        )
    return prices, sources, errors
