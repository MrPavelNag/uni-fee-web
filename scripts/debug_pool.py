#!/usr/bin/env python3
"""Проверить пул в v4 subgraph: есть ли он, какие токены, TVL."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from uniswap_client import get_graph_endpoint, graphql_query

POOL_ID = "0xf8a05200a0a9308212de0605628a5e266ca8b3539e03d76ef694cc1d30bb9689"
CHAIN = "arbitrum"

def main():
    ep = get_graph_endpoint(CHAIN, "v4")
    if not ep:
        print("Нет endpoint. THE_GRAPH_API_KEY?")
        return
    q = """
    query {
      pool(id: "%s") {
        id
        token0 { id symbol }
        token1 { id symbol }
        totalValueLockedUSD
        feeTier
      }
    }
    """ % POOL_ID
    try:
        data = graphql_query(ep, q)
        pool = data.get("data", {}).get("pool")
        if pool:
            print("Пул найден в subgraph:")
            print("  token0:", pool["token0"]["symbol"], pool["token0"]["id"])
            print("  token1:", pool["token1"]["symbol"], pool["token1"]["id"])
            print("  TVL USD:", pool.get("totalValueLockedUSD"))
            print("  feeTier:", pool.get("feeTier"))
        else:
            print("Пул НЕ найден в subgraph (pool=null)")
        if "errors" in data:
            print("Errors:", data["errors"])
    except Exception as e:
        print("Ошибка:", e)

if __name__ == "__main__":
    main()
