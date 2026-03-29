"""
Adapter for Messari-style subgraph schema.

Provides reusable mapping into internal v3-like pool/day-data structures.
"""

from typing import Any, Optional

from uniswap_client import graphql_query


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except Exception:
        return 0.0


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _normalize_pool_entity(raw: dict) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    pid = str(raw.get("id") or "").strip().lower()
    if not pid:
        return None
    toks = raw.get("inputTokens") if isinstance(raw.get("inputTokens"), list) else []
    bals = raw.get("inputTokenBalances") if isinstance(raw.get("inputTokenBalances"), list) else []
    if len(toks) < 2:
        return None
    t0 = toks[0] if isinstance(toks[0], dict) else {}
    t1 = toks[1] if isinstance(toks[1], dict) else {}
    b0 = _safe_float(bals[0] if len(bals) > 0 else 0.0)
    b1 = _safe_float(bals[1] if len(bals) > 1 else 0.0)
    fees = raw.get("fees") if isinstance(raw.get("fees"), list) else []
    fee_pct_raw = 0.0
    if fees and isinstance(fees[0], dict):
        fee_pct_raw = _safe_float((fees[0] or {}).get("feePercentage"))
    if fee_pct_raw <= 0:
        fee_tier = 3000
    elif fee_pct_raw <= 1.0:
        # Example: 0.003 -> 0.3% -> 3000 (Uniswap feeTier units)
        fee_tier = int(round(fee_pct_raw * 1_000_000))
    else:
        # Example: 0.3 -> 0.3% -> 3000
        fee_tier = int(round(fee_pct_raw * 10_000))
    ratio01 = (b1 / b0) if (b0 > 0 and b1 > 0) else 0.0
    ratio10 = (b0 / b1) if (b0 > 0 and b1 > 0) else 0.0
    return {
        "id": pid,
        "feeTier": int(max(0, fee_tier)),
        "liquidity": str(raw.get("totalLiquidity") or "0"),
        "token0": {
            "id": str(t0.get("id") or "").strip().lower(),
            "symbol": str(t0.get("symbol") or "").strip(),
            "decimals": str(t0.get("decimals") or "18"),
            "name": str(t0.get("name") or t0.get("symbol") or "").strip(),
        },
        "token1": {
            "id": str(t1.get("id") or "").strip().lower(),
            "symbol": str(t1.get("symbol") or "").strip(),
            "decimals": str(t1.get("decimals") or "18"),
            "name": str(t1.get("name") or t1.get("symbol") or "").strip(),
        },
        "totalValueLockedUSD": float(_safe_float(raw.get("totalValueLockedUSD"))),
        "totalValueLockedToken0": float(max(0.0, b0)),
        "totalValueLockedToken1": float(max(0.0, b1)),
        "token0Price": float(max(0.0, ratio01)),
        "token1Price": float(max(0.0, ratio10)),
        "volumeUSD": float(_safe_float(raw.get("cumulativeVolumeUSD"))),
        "feesUSD": float(_safe_float(raw.get("cumulativeSupplySideRevenueUSD"))),
        "txCount": str(raw.get("cumulativeSwapCount") or "0"),
    }


def is_messari_schema_endpoint(endpoint: str) -> bool:
    ep = str(endpoint or "").strip()
    if not ep:
        return False
    q = '{ __type(name:"Query"){ fields { name } } }'
    try:
        data = graphql_query(ep, q, retries=1)
        fields = (((data or {}).get("data") or {}).get("__type") or {}).get("fields") or []
        names = {str((x or {}).get("name") or "").strip() for x in fields if isinstance(x, dict)}
        return ("liquidityPools" in names) and ("pools" not in names)
    except Exception:
        return False


def query_pool_by_id_messari(endpoint: str, pool_id: str) -> Optional[dict]:
    pid = str(pool_id or "").strip().lower()
    if not pid:
        return None
    q = """
    query PoolById($id: String!) {
      liquidityPool(id: $id) {
        id
        name
        symbol
        totalValueLockedUSD
        totalLiquidity
        cumulativeVolumeUSD
        cumulativeSupplySideRevenueUSD
        cumulativeSwapCount
        inputTokenBalances
        inputTokens { id symbol decimals name }
        fees { feePercentage }
      }
    }
    """
    data = graphql_query(endpoint, q, {"id": pid}, retries=1)
    raw = ((data or {}).get("data") or {}).get("liquidityPool")
    return _normalize_pool_entity(raw if isinstance(raw, dict) else {})


def query_pool_day_data_messari(endpoint: str, pool_id: str, start_ts: int, end_ts: int) -> list[dict]:
    pid = str(pool_id or "").strip().lower()
    if not pid:
        return []
    out: list[dict] = []
    skip = 0
    while True:
        q = """
        query PoolDaily($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
          liquidityPoolDailySnapshots(
            first: 100,
            skip: $skip,
            orderBy: timestamp,
            orderDirection: asc,
            where: { pool: $pool, timestamp_gte: $start, timestamp_lte: $end }
          ) {
            timestamp
            totalValueLockedUSD
            dailySupplySideRevenueUSD
            dailyTotalRevenueUSD
            dailyVolumeUSD
          }
        }
        """
        data = graphql_query(
            endpoint,
            q,
            {"pool": pid, "start": int(start_ts), "end": int(end_ts), "skip": int(skip)},
            retries=1,
        )
        rows = ((data or {}).get("data") or {}).get("liquidityPoolDailySnapshots") or []
        if not rows:
            break
        for r in rows:
            if not isinstance(r, dict):
                continue
            ts = _safe_int(r.get("timestamp"), 0)
            if ts <= 0:
                continue
            fees = _safe_float(r.get("dailySupplySideRevenueUSD"))
            if fees <= 0:
                fees = _safe_float(r.get("dailyTotalRevenueUSD"))
            out.append(
                {
                    "date": int(ts),
                    "tvlUSD": float(_safe_float(r.get("totalValueLockedUSD"))),
                    "feesUSD": float(max(0.0, fees)),
                    "volumeUSD": float(_safe_float(r.get("dailyVolumeUSD"))),
                }
            )
        if len(rows) < 100:
            break
        skip += 100
    out.sort(key=lambda x: int(x.get("date") or 0))
    return out


def query_pool_day_data_batch_messari(
    endpoint: str,
    pool_ids: list[str],
    start_ts: int,
    end_ts: int,
) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for pid in pool_ids or []:
        key = str(pid or "").strip().lower()
        if not key:
            continue
        try:
            out[key] = query_pool_day_data_messari(endpoint, key, int(start_ts), int(end_ts))
        except Exception:
            out[key] = []
    return out


def query_pools_by_token_addresses_messari(
    endpoint: str,
    token_a: str,
    token_b: str,
    *,
    page_size: int = 100,
    max_scan: int = 400,
) -> list[dict]:
    """
    Discover pools by scanning top liquidityPools and filtering by inputTokens.
    This avoids expensive where-filters that may timeout on some Messari endpoints.
    """
    a = str(token_a or "").strip().lower()
    b = str(token_b or "").strip().lower()
    if not a or not b:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    first = max(10, min(200, int(page_size or 100)))
    limit = max(first, int(max_scan or 400))
    skip = 0
    while skip < limit:
        q = """
        query PoolsPage($first: Int!, $skip: Int!) {
          liquidityPools(
            first: $first,
            skip: $skip,
            orderBy: totalValueLockedUSD,
            orderDirection: desc
          ) {
            id
            name
            symbol
            totalValueLockedUSD
            totalLiquidity
            cumulativeVolumeUSD
            cumulativeSupplySideRevenueUSD
            cumulativeSwapCount
            inputTokenBalances
            inputTokens { id symbol decimals name }
            fees { feePercentage }
          }
        }
        """
        data = graphql_query(endpoint, q, {"first": int(first), "skip": int(skip)}, retries=1)
        rows = ((data or {}).get("data") or {}).get("liquidityPools") or []
        if not rows:
            break
        for raw in rows:
            if not isinstance(raw, dict):
                continue
            toks = raw.get("inputTokens") if isinstance(raw.get("inputTokens"), list) else []
            ids = {str((t or {}).get("id") or "").strip().lower() for t in toks if isinstance(t, dict)}
            if a not in ids or b not in ids:
                continue
            p = _normalize_pool_entity(raw)
            if not isinstance(p, dict):
                continue
            pid = str(p.get("id") or "").strip().lower()
            if not pid or pid in seen:
                continue
            seen.add(pid)
            out.append(p)
        if len(rows) < first:
            break
        skip += first
    return out
