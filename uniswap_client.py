"""
Uniswap v3/v4 subgraph GraphQL client.
Uses The Graph (requires API key) or Goldsky public endpoints where available.
"""

import os
import threading
import time
from typing import Optional

import requests


_POOL_DAY_CACHE_LOCK = threading.Lock()
_POOL_DAY_CACHE: dict[tuple[str, str, int, int], list[dict]] = {}
_TOKEN_BY_SYMBOL_CACHE_LOCK = threading.Lock()
_TOKEN_BY_SYMBOL_CACHE: dict[tuple[str, str], Optional[str]] = {}
_ENDPOINT_RATE_LOCK = threading.Lock()
_ENDPOINT_LAST_TS: dict[str, float] = {}


def _page_delay_sec() -> float:
    try:
        return max(0.0, float(os.environ.get("GRAPHQL_PAGE_DELAY_SEC", "0")))
    except Exception:
        return 0.0


def _maybe_page_delay() -> None:
    d = _page_delay_sec()
    if d > 0:
        time.sleep(d)


def _canonical_graph_chain(chain: str) -> str:
    c = (chain or "").strip().lower()
    if c == "arbitrum-one":
        return "arbitrum"
    return c


def _is_placeholder_graph_key(value: str) -> bool:
    v = str(value or "").strip().lower()
    if not v:
        return True
    placeholders = {
        "paste_your_real_key_here",
        "paste_your_key_here",
        "your_key_here",
        "changeme",
    }
    return v in placeholders or ("paste" in v and "key" in v)


def _is_goldsky_public_endpoint(endpoint: str) -> bool:
    ep = str(endpoint or "").strip().lower()
    return ("api.goldsky.com" in ep) and ("/api/public/" in ep)


def _goldsky_min_interval_sec() -> float:
    try:
        return max(0.0, float(os.environ.get("GRAPHQL_GOLDSKY_MIN_INTERVAL_SEC", "0.35")))
    except Exception:
        return 0.35


def _respect_endpoint_rate_limit(endpoint: str) -> None:
    ep = str(endpoint or "").strip()
    if not ep:
        return
    if not _is_goldsky_public_endpoint(ep):
        return
    min_gap = _goldsky_min_interval_sec()
    if min_gap <= 0:
        return
    now = time.time()
    wait = 0.0
    with _ENDPOINT_RATE_LOCK:
        last = float(_ENDPOINT_LAST_TS.get(ep, 0.0) or 0.0)
        delta = now - last
        if delta < min_gap:
            wait = float(min_gap - delta)
    if wait > 0:
        time.sleep(wait)
    with _ENDPOINT_RATE_LOCK:
        _ENDPOINT_LAST_TS[ep] = time.time()


def get_graph_endpoint(chain: str, version: str = "v3") -> Optional[str]:
    """Build GraphQL endpoint URL for a chain and protocol version (v3 or v4)."""
    chain = _canonical_graph_chain(chain)
    api_key = str(os.environ.get("THE_GRAPH_API_KEY") or "").strip()
    if _is_placeholder_graph_key(api_key):
        if version == "v4":
            return None
        # Fallback: Goldsky for Base v3 only if no API key (rate limited)
        from config import GOLDSKY_ENDPOINTS
        if chain in GOLDSKY_ENDPOINTS:
            return GOLDSKY_ENDPOINTS[chain]
        return None

    from config import GOLDSKY_ENDPOINTS, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS

    if version == "v4":
        cu = chain.upper().replace("-", "_")
        override = os.environ.get(f"V4_OVERRIDE_{cu}")
        if not override and chain == "arbitrum":
            override = os.environ.get("V4_OVERRIDE_ARBITRUM_ONE")
        if override:
            return override
        if chain in UNISWAP_V4_SUBGRAPHS:
            return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{UNISWAP_V4_SUBGRAPHS[chain]}"
        return None

    # v3
    if chain in UNISWAP_V3_SUBGRAPHS:
        return f"https://gateway.thegraph.com/api/{api_key}/subgraphs/id/{UNISWAP_V3_SUBGRAPHS[chain]}"
    if chain in GOLDSKY_ENDPOINTS:
        return GOLDSKY_ENDPOINTS[chain]
    return None


def graphql_query(endpoint: str, query: str, variables: Optional[dict] = None, retries: Optional[int] = None) -> dict:
    """Execute GraphQL query. Retries on 'bad indexers' (The Graph transient errors)."""
    payload = {"query": query, "variables": variables or {}}
    last_err = None
    if retries is None:
        try:
            retries = int(os.environ.get("GRAPHQL_RETRIES", "4"))
        except ValueError:
            retries = 4
    retries = max(1, retries)
    ep_low = str(endpoint or "").strip().lower()
    if ("api.goldsky.com" in ep_low) and ("/api/public/" in ep_low):
        # Goldsky public endpoints can return transient 429 under shared quota.
        # Ensure at least a few retry attempts even when caller requests retries=1.
        try:
            retries = max(retries, int(os.environ.get("GRAPHQL_GOLDSKY_429_RETRIES", "3")))
        except Exception:
            retries = max(retries, 3)
    try:
        connect_timeout = float(os.environ.get("GRAPHQL_CONNECT_TIMEOUT_SEC", "8"))
    except Exception:
        connect_timeout = 8.0
    try:
        read_timeout = float(os.environ.get("GRAPHQL_READ_TIMEOUT_SEC", "15"))
    except Exception:
        read_timeout = 15.0
    if _is_goldsky_public_endpoint(endpoint):
        try:
            read_timeout = max(read_timeout, float(os.environ.get("GRAPHQL_GOLDSKY_READ_TIMEOUT_SEC", "35")))
        except Exception:
            read_timeout = max(read_timeout, 35.0)
    connect_timeout = max(2.0, connect_timeout)
    read_timeout = max(5.0, read_timeout)
    for attempt in range(retries):
        try:
            _respect_endpoint_rate_limit(endpoint)
            resp = requests.post(endpoint, json=payload, timeout=(connect_timeout, read_timeout))
            if int(getattr(resp, "status_code", 0) or 0) == 429:
                if attempt < retries - 1:
                    ra = str(resp.headers.get("Retry-After", "") or "").strip()
                    try:
                        wait_s = max(1.0, float(ra))
                    except Exception:
                        wait_s = float(2 * (attempt + 1))
                    time.sleep(wait_s)
                    continue
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                err_msg = str(data["errors"]).lower()
                if "bad indexers" in err_msg or "badresponse" in err_msg:
                    if attempt < retries - 1:
                        time.sleep(5 * (attempt + 1))
                        continue
                raise RuntimeError(f"GraphQL errors: {data['errors']}")
            return data
        except requests.exceptions.ConnectionError:
            raise  # DNS/connection errors: no retry
        except (requests.RequestException, RuntimeError) as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise
    raise last_err or RuntimeError("GraphQL query failed")


def query_pools_containing_both_tokens(
    endpoint: str, token_a: str, token_b: str, min_tvl: float, max_results: int = 0
) -> list[dict]:
    """
    Find pools containing BOTH token_a and token_b (in either order).
    Queries pools directly: (token0=A,token1=B) or (token0=B,token1=A).
    """
    token_a = token_a.lower()
    token_b = token_b.lower()
    result = []

    query_tmpl = """
    query Pools($skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price
        volumeUSD feesUSD txCount
      }
      pools1: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price
        volumeUSD feesUSD txCount
      }
    }
    """
    query = query_tmpl % (token_a, token_b, token_b, token_a)
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            query,
            {"skip": skip},
            retries=1,  # fallback path: fail fast on bad indexers
        )
        p0 = data.get("data", {}).get("pools0", [])
        p1 = data.get("data", {}).get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if int(max_results or 0) > 0 and len(result) >= int(max_results):
            return result[: int(max_results)]
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pools_by_token_symbols(
    endpoint: str, symbol_a: str, symbol_b: str, min_tvl: float, max_results: int = 0
) -> list[dict]:
    """
    Fallback search by token symbols (when address resolution is ambiguous).
    Returns pools where token symbols match in either order.
    """
    sa = symbol_a.upper().strip()
    sb = symbol_b.upper().strip()
    if not sa or not sb:
        return []
    result = []
    query_tmpl = """
    query PoolsBySymbols($skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" } },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price
        volumeUSD feesUSD txCount
      }
      pools1: pools(
        first: 100,
        skip: $skip,
        where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" } },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1 token0Price token1Price
        volumeUSD feesUSD txCount
      }
    }
    """
    query = query_tmpl % (sa, sb, sb, sa)
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            query,
            {"skip": skip},
        )
        p0 = data.get("data", {}).get("pools0", [])
        p1 = data.get("data", {}).get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if int(max_results or 0) > 0 and len(result) >= int(max_results):
            return result[: int(max_results)]
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pools_containing_both_tokens_no_tvl_filter(
    endpoint: str, token_a: str, token_b: str
) -> list[dict]:
    """
    Simpler query without totalValueLockedUSD filter (for v4 when main query fails).
    Filter by min_tvl in Python.
    """
    token_a = token_a.lower()
    token_b = token_b.lower()
    result = []

    query_tmpl = """
    query Pools($skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1
        volumeUSD feesUSD txCount
      }
      pools1: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s" },
        orderBy: totalValueLockedUSD,
        orderDirection: desc
      ) {
        id feeTier liquidity
        token0 { id symbol decimals name }
        token1 { id symbol decimals name }
        totalValueLockedUSD totalValueLockedToken0 totalValueLockedToken1
        volumeUSD feesUSD txCount
      }
    }
    """
    query = query_tmpl % (token_a, token_b, token_b, token_a)
    skip = 0
    while True:
        data = graphql_query(endpoint, query, {"skip": skip})
        p0 = data.get("data", {}).get("pools0", [])
        p1 = data.get("data", {}).get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        _maybe_page_delay()
    return result


def query_pool_day_data(
    endpoint: str, pool_id: str, start_ts: int, end_ts: int
) -> list[dict]:
    """Fetch PoolDayData for a pool for the given date range (date = unix/86400)."""
    query = """
    query PoolDayData($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      poolDayDatas(
        first: 100,
        skip: $skip,
        orderBy: date,
        orderDirection: asc,
        where: { pool: $pool, date_gte: $start, date_lte: $end }
      ) {
        id
        date
        tvlUSD
        volumeUSD
        feesUSD
        liquidity
      }
    }
    """
    cache_key = (str(endpoint), str(pool_id).lower(), int(start_ts), int(end_ts))
    with _POOL_DAY_CACHE_LOCK:
        cached = _POOL_DAY_CACHE.get(cache_key)
    if cached is not None:
        return cached
    all_data = []
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            query,
            {"pool": pool_id, "start": start_ts, "end": end_ts, "skip": skip},
        )
        items = data.get("data", {}).get("poolDayDatas", [])
        if not items:
            break
        all_data.extend(items)
        if len(items) < 100:
            break
        skip += 100
        _maybe_page_delay()
    out = sorted(all_data, key=lambda x: int(x["date"]))
    with _POOL_DAY_CACHE_LOCK:
        _POOL_DAY_CACHE[cache_key] = out
    return out


def query_token_by_symbol(endpoint: str, symbol: str) -> Optional[str]:
    """
    Find token address by symbol. Returns the token with highest TVL (most liquid = real token).
    Scam/fake tokens usually have low TVL; real CVX, CRV, WBTC etc. have high TVL.
    For 'eth' also tries 'WETH' (subgraphs often use WETH).
    """
    endpoint_key = str(endpoint).strip().lower()
    symbol_key = str(symbol).strip().upper()
    cache_key = (endpoint_key, symbol_key)
    with _TOKEN_BY_SYMBOL_CACHE_LOCK:
        if cache_key in _TOKEN_BY_SYMBOL_CACHE:
            return _TOKEN_BY_SYMBOL_CACHE[cache_key]
    syms_to_try = [symbol.upper()]
    if symbol.lower() == "eth":
        syms_to_try.append("WETH")
    query = """
    query TokenBySymbol($symbol: String!) {
      tokens(first: 20, where: { symbol: $symbol }) {
        id
        symbol
        totalValueLockedUSD
        volumeUSD
      }
    }
    """
    for sym in syms_to_try:
        try:
            data = graphql_query(endpoint, query, {"symbol": sym})
            tokens = data.get("data", {}).get("tokens", [])
            if not tokens:
                continue
            # Pick token with highest TVL (real token, not scam copy)
            def tvl(t):
                try:
                    return float(t.get("totalValueLockedUSD") or 0)
                except (ValueError, TypeError):
                    return 0.0
            best = max(tokens, key=tvl)
            addr = best["id"].lower()
            # Reject zero address (invalid)
            if addr == "0x0000000000000000000000000000000000000000":
                continue
            with _TOKEN_BY_SYMBOL_CACHE_LOCK:
                _TOKEN_BY_SYMBOL_CACHE[cache_key] = addr
            return addr
        except Exception:
            continue
    with _TOKEN_BY_SYMBOL_CACHE_LOCK:
        _TOKEN_BY_SYMBOL_CACHE[cache_key] = None
    return None
