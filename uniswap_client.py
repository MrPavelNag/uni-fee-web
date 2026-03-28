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


def get_graph_endpoint(chain: str, version: str = "v3") -> Optional[str]:
    """Build GraphQL endpoint URL for a chain and protocol version (v3 or v4)."""
    chain = _canonical_graph_chain(chain)
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
    """Fetch PoolDayData for one pool."""
    pid = str(pool_id or "").strip().lower()
    if not pid:
        return []
    return query_pool_day_data_batch(endpoint, [pid], start_ts, end_ts).get(pid, [])


def _pool_day_batch_size() -> int:
    try:
        return max(1, min(40, int(os.environ.get("GRAPHQL_POOL_DAY_BATCH_SIZE", "12"))))
    except Exception:
        return 12


def query_pool_day_data_batch(
    endpoint: str,
    pool_ids: list[str],
    start_ts: int,
    end_ts: int,
    batch_size: int = 0,
) -> dict[str, list[dict]]:
    """
    Fetch PoolDayData for many pools using batched GraphQL aliases.
    Returns mapping {pool_id_lower: sorted_rows}.
    """
    uniq_ids = list(dict.fromkeys([str(x or "").strip().lower() for x in (pool_ids or []) if str(x or "").strip()]))
    if not uniq_ids:
        return {}
    out: dict[str, list[dict]] = {pid: [] for pid in uniq_ids}
    missing: list[str] = []
    for pid in uniq_ids:
        cache_key = (str(endpoint), str(pid), int(start_ts), int(end_ts))
        with _POOL_DAY_CACHE_LOCK:
            cached = _POOL_DAY_CACHE.get(cache_key)
        if cached is not None:
            out[pid] = list(cached)
        else:
            missing.append(pid)
    if not missing:
        return out

    bsz = int(batch_size or _pool_day_batch_size())
    bsz = max(1, min(40, bsz))

    def _build_query(active_ids: list[str]) -> tuple[str, dict[str, str]]:
        alias_to_pool: dict[str, str] = {}
        parts: list[str] = []
        for i, pid in enumerate(active_ids):
            alias = f"p{i}"
            alias_to_pool[alias] = pid
            parts.append(
                f"""{alias}: poolDayDatas(
        first: 100,
        skip: $skip,
        orderBy: date,
        orderDirection: asc,
        where: {{ pool: "{pid}", date_gte: $start, date_lte: $end }}
      ) {{
        id
        date
        tvlUSD
        volumeUSD
        feesUSD
        liquidity
      }}"""
            )
        query = "query PoolDayDataBatch($start: Int!, $end: Int!, $skip: Int!) {\n" + "\n".join(parts) + "\n}"
        return query, alias_to_pool

    for off in range(0, len(missing), bsz):
        chunk = missing[off : off + bsz]
        active = list(chunk)
        skip = 0
        while active:
            query, alias_map = _build_query(active)
            data = graphql_query(endpoint, query, {"start": int(start_ts), "end": int(end_ts), "skip": int(skip)})
            payload = data.get("data", {}) if isinstance(data, dict) else {}
            next_active: list[str] = []
            for alias, pid in alias_map.items():
                items = payload.get(alias, []) if isinstance(payload, dict) else []
                if not isinstance(items, list):
                    items = []
                if items:
                    out[pid].extend(items)
                if len(items) >= 100:
                    next_active.append(pid)
            if not next_active:
                break
            active = next_active
            skip += 100
            _maybe_page_delay()
        for pid in chunk:
            rows = sorted(out.get(pid, []), key=lambda x: int(x.get("date") or 0))
            out[pid] = rows
            cache_key = (str(endpoint), str(pid), int(start_ts), int(end_ts))
            with _POOL_DAY_CACHE_LOCK:
                _POOL_DAY_CACHE[cache_key] = rows
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
