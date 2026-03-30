#!/usr/bin/env python3
"""
Проверить raw данные subgraph для пула: volume, fees, TVL по дням.
Сравнить с UI: fees ≤ volume × fee_rate (обычно 0.003 для 0.3%).
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import FEE_DAYS
from uniswap_client import get_graph_endpoint, graphql_query

# v3 pool Arbitrum FLUID/ETH
POOL_ID = "0x6cE19e5b05C0a0416FeB963bcd754C8d99C02248"
CHAIN = "arbitrum"
VERSION = "v3"

def main():
    ep = get_graph_endpoint(CHAIN, VERSION)
    if not ep:
        print("Нет endpoint")
        return

    end = datetime.utcnow()
    start = end - timedelta(days=FEE_DAYS)
    day_start = int(start.timestamp() // 86400)
    day_end = int(end.timestamp() // 86400)

    # Сначала проверим, есть ли pool и его общие данные
    pool_q = """
    query { pool(id: "%s") {
      id token0 { symbol } token1 { symbol }
      totalValueLockedUSD volumeUSD feesUSD
    }}
    """ % POOL_ID
    pool_data = graphql_query(ep, pool_q)
    pool = pool_data.get("data", {}).get("pool")
    if not pool:
        print("Pool not found in subgraph")
    if pool:
        print("Pool:", pool["token0"]["symbol"], "/", pool["token1"]["symbol"])
        print("  TVL:", pool.get("totalValueLockedUSD"))
        print("  volumeUSD (all-time):", pool.get("volumeUSD"))
        print("  feesUSD (all-time):", pool.get("feesUSD"))
        print()

    q = """
    query PoolDayData($pool: String!, $start: Int!, $end: Int!, $skip: Int!) {
      poolDayDatas(first: 200, skip: $skip, orderBy: date, orderDirection: asc,
        where: { pool: $pool, date_gte: $start, date_lte: $end }) {
        date tvlUSD volumeUSD feesUSD
      }
    }
    """
    data = graphql_query(ep, q, {"pool": POOL_ID, "start": day_start, "end": day_end, "skip": 0})
    rows = data.get("data", {}).get("poolDayDatas", [])

    # Если 0 дней — попробуем без фильтра дат (последние записи)
    if not rows:
        q2 = """
        query { poolDayDatas(first: 5, orderBy: date, orderDirection: desc, where: { pool: "%s" }) {
          date tvlUSD volumeUSD feesUSD
        }}
        """ % POOL_ID
        try:
            d2 = graphql_query(ep, q2)
            rows2 = d2.get("data", {}).get("poolDayDatas", [])
            if rows2:
                print("(Найдены последние записи без фильтра дат:)")
                rows = rows2
        except Exception:
            pass

    print(f"Pool {POOL_ID} | {CHAIN} {VERSION}")
    print(f"Period: {day_start} - {day_end} ({len(rows)} days)")
    print("-" * 60)

    total_vol = total_fees = 0.0
    for r in rows:
        vol = float(r.get("volumeUSD") or 0)
        fees = float(r.get("feesUSD") or 0)
        total_vol += vol
        total_fees += fees
        dt = datetime.utcfromtimestamp(int(r["date"]) * 86400).strftime("%Y-%m-%d")
        tvl = float(r.get("tvlUSD") or 0)
        print(f"  {dt}  vol=${vol:,.0f}  fees=${fees:,.2f}  tvl=${tvl:,.0f}")

    print("-" * 60)
    print(f"Итого за {FEE_DAYS} дней: volume=${total_vol:,.0f}  fees=${total_fees:,.2f}")
    print(f"Ожидаемо: fees ≈ volume × 0.003 = ${total_vol * 0.003:,.2f}")
    print("Проекция LP $10k: disabled in this debug script (subgraph TVL denominator removed)")
    if total_vol > 0 and total_fees > total_vol * 0.01:
        print("⚠ Возможная ошибка: fees >> volume×1% — данные subgraph под вопросом")

if __name__ == "__main__":
    main()
