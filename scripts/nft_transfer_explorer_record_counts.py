#!/usr/bin/env python3
"""
Считает **число записей NFT transfers** по каждой сети Etherscan EAAS (как «A total of N records found»
на Arbiscan / аналогичных сканерах): полный проход `account&action=tokennfttx` с пагинацией.

Нужен как минимум ETHERSCAN_API_KEY (v2). Для сетей, где бесплатный v2 не отдаёт tokennfttx,
скрипт пробует **нативный** host (BscScan, BaseScan, Arbiscan, …) с тем же ключом или
переменными BSCSCAN_API_KEY, BASESCAN_API_KEY, … — как в webapp/main.py.

Использование:
  export ETHERSCAN_API_KEY=...
  python3 scripts/nft_transfer_explorer_record_counts.py
  python3 scripts/nft_transfer_explorer_record_counts.py 0xabc... 0xdef...

Опции окружения:
  NFT_COUNT_INCLUDE_TESTNETS=1   — включить тестнеты из chainlist (по умолчанию только «mainnet-like»)
  NFT_COUNT_SLEEP_SEC=0.22       — пауза между запросами (rate limit)
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from typing import Any

CHAINLIST_URL = "https://api.etherscan.io/v2/chainlist"
V2_API = "https://api.etherscan.io/v2/api"
DEFAULT_ADDRS = (
    "0x274aef826a1078e2621e75e08d816f56b2c8475a",
    "0xC3102694c8F321A71BC695DfDa6cF912f455e2E9",
)
OFFSET = 1000
UA = "uni_fee-nft-transfer-count/1"


def _sleep() -> None:
    time.sleep(float(os.environ.get("NFT_COUNT_SLEEP_SEC", "0.22")))


def _http_json(url: str) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


def fetch_chainlist() -> list[dict[str, Any]]:
    raw = _http_json(CHAINLIST_URL)
    res = raw.get("result")
    if not isinstance(res, list):
        raise RuntimeError("chainlist: unexpected shape")
    return res


def is_mainnet_like(chainname: str) -> bool:
    n = (chainname or "").lower()
    for frag in (
        "testnet",
        "sepolia",
        "amoy",
        "hoodi",
        "fuji",
        "alpha testnet",
        "apothem",
        "bepolia",
        "bokuto",
        "curtis testnet",
    ):
        if frag in n:
            return False
    return True


def count_tokennfttx_etherscan_v2(chain_id: int, address: str, apikey: str) -> tuple[int, str]:
    """Returns (total_rows, error_message)."""
    addr = address.strip().lower()
    total = 0
    page = 1
    while True:
        q = urllib.parse.urlencode(
            {
                "chainid": str(int(chain_id)),
                "module": "account",
                "action": "tokennfttx",
                "address": addr,
                "page": str(page),
                "offset": str(OFFSET),
                "sort": "asc",
                "apikey": apikey,
            }
        )
        url = f"{V2_API}?{q}"
        try:
            data = _http_json(url)
        except Exception as e:
            return total, f"http:{e}"
        st = str(data.get("status") or "")
        res = data.get("result")
        if st != "1":
            msg = str(res if not isinstance(res, list) else data.get("message") or res)
            return total, f"v2_status_{st}:{msg[:200]}"
        if not isinstance(res, list):
            return total, "v2_bad_result_type"
        if len(res) == 0:
            break
        total += len(res)
        if len(res) < OFFSET:
            break
        page += 1
        if page > 2000:
            return total, "v2_page_cap"
        _sleep()
    return total, ""


def _env_keys() -> dict[str, str]:
    eth = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    return {
        "eth": eth,
        "bsc": os.environ.get("BSCSCAN_API_KEY", "").strip() or eth,
        "base": os.environ.get("BASESCAN_API_KEY", "").strip() or eth,
        "arb": os.environ.get("ARBISCAN_API_KEY", "").strip() or eth,
        "polygon": os.environ.get("POLYGONSCAN_API_KEY", "").strip() or eth,
        "op": (
            os.environ.get("OPTIMISTIC_ETHERSCAN_API_KEY", "").strip()
            or os.environ.get("OPTIMISM_ETHERSCAN_API_KEY", "").strip()
            or eth
        ),
        "uni": os.environ.get("UNISCAN_API_KEY", "").strip() or eth,
    }


def count_tokennfttx_native(host: str, address: str, apikey: str) -> tuple[int, str]:
    addr = address.strip().lower()
    total = 0
    page = 1
    if not apikey:
        return 0, "native_no_apikey"
    while True:
        q = urllib.parse.urlencode(
            {
                "module": "account",
                "action": "tokennfttx",
                "address": addr,
                "page": str(page),
                "offset": str(OFFSET),
                "sort": "asc",
                "apikey": apikey,
            }
        )
        url = f"{host.rstrip('/')}?{q}"
        try:
            data = _http_json(url)
        except Exception as e:
            return total, f"native_http:{e}"
        st = str(data.get("status") or "")
        res = data.get("result")
        if st != "1":
            msg = str(res if not isinstance(res, list) else data.get("message") or res)
            return total, f"native_status_{st}:{msg[:200]}"
        if not isinstance(res, list):
            return total, "native_bad_result"
        if len(res) == 0:
            break
        total += len(res)
        if len(res) < OFFSET:
            break
        page += 1
        if page > 2000:
            return total, "native_page_cap"
        _sleep()
    return total, ""


def native_explorer_for_chain(chain_id: int) -> tuple[str, str] | None:
    """
    (api_base_url, key_slot) where key_slot is one of bsc, base, arb, polygon, op, uni, eth.
    """
    cid = int(chain_id)
    k = _env_keys()
    uniscan = str(os.environ.get("UNISCAN_API_URL", "https://api.uniscan.xyz/api")).strip().rstrip("/")
    mapping: dict[int, tuple[str, str]] = {
        56: ("https://api.bscscan.com/api", "bsc"),
        8453: ("https://api.basescan.org/api", "base"),
        42161: ("https://api.arbiscan.io/api", "arb"),
        137: ("https://api.polygonscan.com/api", "polygon"),
        10: ("https://api-optimistic.etherscan.io/api", "op"),
        130: (f"{uniscan}", "uni"),
        1301: (f"{uniscan}", "uni"),
    }
    if cid not in mapping:
        return None
    base, slot = mapping[cid]
    return base, k.get(slot) or k["eth"]


def count_for_chain(chain_id: int, _chainname: str, _explorer: str, address: str) -> tuple[int, str, str]:
    """
    Returns (count, source_label, error_or_empty).
    source: etherscan_v2 | native | none
    """
    keys = _env_keys()
    eth = keys["eth"]
    if eth:
        n, err = count_tokennfttx_etherscan_v2(chain_id, address, eth)
        if not err:
            return n, "etherscan_v2", ""
        # v2 failed — try native for known chains
        nat = native_explorer_for_chain(chain_id)
        if nat:
            host, key = nat
            n2, err2 = count_tokennfttx_native(host, address, key)
            if not err2:
                return n2, f"native:{host}", ""
            return 0, f"native:{host}", f"v2:{err}; native:{err2}"
        return 0, "etherscan_v2", err

    # No ETH key — try native only (unlikely to work for most chains)
    nat = native_explorer_for_chain(chain_id)
    if nat:
        host, key = nat
        n2, err2 = count_tokennfttx_native(host, address, key)
        return (n2 if not err2 else 0), f"native:{host}", err2
    return 0, "none", "Set ETHERSCAN_API_KEY (Etherscan API v2)."


def main() -> int:
    include_testnets = os.environ.get("NFT_COUNT_INCLUDE_TESTNETS", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    addrs = [a.strip() for a in (sys.argv[1:] or list(DEFAULT_ADDRS)) if a.strip()]

    chains = fetch_chainlist()
    selected: list[dict[str, Any]] = []
    for c in chains:
        if int(c.get("status") or 0) != 1:
            continue
        name = str(c.get("chainname") or "")
        if not include_testnets and not is_mainnet_like(name):
            continue
        try:
            cid = int(str(c.get("chainid") or "0"), 10)
        except ValueError:
            continue
        if cid <= 0:
            continue
        selected.append(
            {
                "chainid": cid,
                "chainname": name,
                "blockexplorer": str(c.get("blockexplorer") or ""),
            }
        )

    print(
        f"Chains to scan: {len(selected)} "
        f"({'incl. testnets' if include_testnets else 'mainnet-like only'})\n"
        f"ETHERSCAN_API_KEY: {'set' if _env_keys()['eth'] else 'MISSING — v2 will fail'}\n"
        f"Metric: total rows returned by tokennfttx (matches Arbiscan «records found» style)\n"
    )

    for addr in addrs:
        print(f"========== {addr} ==========")
        grand = 0
        rows_out: list[tuple[int, str, int, str, str]] = []
        for ch in sorted(selected, key=lambda x: x["chainid"]):
            cid = ch["chainid"]
            n, src, err = count_for_chain(cid, ch["chainname"], ch["blockexplorer"], addr)
            grand += n
            rows_out.append((cid, ch["chainname"], n, src, err))
            _sleep()
        print("chain_id\trecords\tsource\tchain_name")
        for cid, name, n, src, err in rows_out:
            err_show = f"\t# {err}" if err else ""
            print(f"{cid}\t{n}\t{src}\t{name}{err_show}")
        print(f"TOTAL records (all listed chains): {grand}\n")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
