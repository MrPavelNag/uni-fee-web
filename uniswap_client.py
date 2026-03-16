"""
Uniswap v3/v4 subgraph GraphQL client.
Uses The Graph (requires API key) or Goldsky public endpoints where available.
"""

import os
import time
from typing import Optional

import requests


def get_graph_endpoint(chain: str, version: str = "v3") -> Optional[str]:
    """Build GraphQL endpoint URL for a chain and protocol version (v3 or v4)."""
    api_key = os.environ.get("THE_GRAPH_API_KEY")
    if not api_key:
        if version == "v4":
            return None
        # Fallback: Goldsky for Base v3 only if no API key (rate limited)
        from config import GOLDSKY_ENDPOINTS
        if chain in GOLDSKY_ENDPOINTS:
            return GOLDSKY_ENDPOINTS[chain]
        return None

    from config import GOLDSKY_ENDPOINTS, UNISWAP_V3_SUBGRAPHS, UNISWAP_V4_SUBGRAPHS

    if version == "v4":
        override = os.environ.get(f"V4_OVERRIDE_{chain.upper().replace('-', '_')}")
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
    try:
        connect_timeout = float(os.environ.get("GRAPHQL_CONNECT_TIMEOUT_SEC", "8"))
    except Exception:
        connect_timeout = 8.0
    try:
        read_timeout = float(os.environ.get("GRAPHQL_READ_TIMEOUT_SEC", "15"))
    except Exception:
        read_timeout = 15.0
    connect_timeout = max(2.0, connect_timeout)
    read_timeout = max(5.0, read_timeout)
    for attempt in range(retries):
        try:
            resp = requests.post(endpoint, json=payload, timeout=(connect_timeout, read_timeout))
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
    endpoint: str, token_a: str, token_b: str, min_tvl: float
) -> list[dict]:
    """
    Find pools containing BOTH token_a and token_b (in either order).
    Queries pools directly: (token0=A,token1=B) or (token0=B,token1=A).
    """
    token_a = token_a.lower()
    token_b = token_b.lower()
    result = []

    query_tmpl = """
    query Pools($minTvl: BigDecimal!, $skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0: "%s", token1: "%s", totalValueLockedUSD_gte: $minTvl },
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
        where: { token0: "%s", token1: "%s", totalValueLockedUSD_gte: $minTvl },
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
        data = graphql_query(
            endpoint,
            query,
            {"minTvl": str(min_tvl), "skip": skip},
            retries=1,  # fallback path: fail fast on bad indexers
        )
        p0 = data.get("data", {}).get("pools0", [])
        p1 = data.get("data", {}).get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        time.sleep(0.3)
    return result


def query_pools_by_token_symbols(
    endpoint: str, symbol_a: str, symbol_b: str, min_tvl: float
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
    query PoolsBySymbols($minTvl: BigDecimal!, $skip: Int!) {
      pools0: pools(
        first: 100,
        skip: $skip,
        where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" }, totalValueLockedUSD_gte: $minTvl },
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
        where: { token0_: { symbol: "%s" }, token1_: { symbol: "%s" }, totalValueLockedUSD_gte: $minTvl },
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
    query = query_tmpl % (sa, sb, sb, sa)
    skip = 0
    while True:
        data = graphql_query(
            endpoint,
            query,
            {"minTvl": str(min_tvl), "skip": skip},
        )
        p0 = data.get("data", {}).get("pools0", [])
        p1 = data.get("data", {}).get("pools1", [])
        result.extend(p0)
        result.extend(p1)
        if len(p0) < 100 and len(p1) < 100:
            break
        skip += 100
        time.sleep(0.3)
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
        time.sleep(0.3)
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
        time.sleep(0.2)
    return sorted(all_data, key=lambda x: int(x["date"]))


def query_token_by_symbol(endpoint: str, symbol: str) -> Optional[str]:
    """
    Find token address by symbol. Returns the token with highest TVL (most liquid = real token).
    Scam/fake tokens usually have low TVL; real CVX, CRV, WBTC etc. have high TVL.
    For 'eth' also tries 'WETH' (subgraphs often use WETH).
    """
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
            return addr
        except Exception:
            continue
    return None
