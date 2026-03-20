#!/usr/bin/env python3
"""
Per-chain ERC-721 count: tokennfttx (indexer) -> last transfer -> verify ownerOf via RPC.
Does not count ERC-1155 balances; may miss NFTs if indexer has no events for that chain.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

ADDRS_DEFAULT = [
    "0x274aef826a1078e2621e75e08d816f56b2c8475a",
    "0xC3102694c8F321A71BC695DfDa6cF912f455e2E9",
]

# Public RPC per chain (same family as webapp DEFAULT_RPC_URLS + common L2s)
RPC_BY_CHAIN: dict[int, str] = {
    1: "https://ethereum-rpc.publicnode.com",
    10: "https://optimism-rpc.publicnode.com",
    56: "https://bsc-rpc.publicnode.com",
    130: "https://unichain-rpc.publicnode.com",
    137: "https://polygon-bor-rpc.publicnode.com",
    324: "https://mainnet.era.zksync.io",
    8453: "https://base-rpc.publicnode.com",
    42161: "https://arbitrum-one-rpc.publicnode.com",
    43114: "https://avalanche-c-chain-rpc.publicnode.com",
    81457: "https://rpc.blast.io",
    59144: "https://linea-rpc.publicnode.com",
    534352: "https://rpc.scroll.io",
    5000: "https://rpc.mantle.xyz",
    34443: "https://rpc.mode.network",
    169: "https://pacific-rpc.manta.network/http",
}

BLOCKSCOUT_API: dict[int, str] = {
    1: "https://eth.blockscout.com/api",
    10: "https://optimism.blockscout.com/api",
    56: "https://bsc.blockscout.com/api",
    137: "https://polygon.blockscout.com/api",
    8453: "https://base.blockscout.com/api",
    42161: "https://arbitrum-one.blockscout.com/api",
    130: "https://unichain.blockscout.com/api",
    324: "https://zksync.blockscout.com/api",
    43114: "https://snowtrace.io/api",
    81457: "https://blast.blockscout.com/api",
    59144: "https://linea.blockscout.com/api",
    534352: "https://scroll.blockscout.com/api",
    5000: "https://mantle.blockscout.com/api",
    34443: "https://mode.blockscout.com/api",
    169: "https://pacific-explorer.manta.network/api",
}

ETHERSCAN_V2 = "https://api.etherscan.io/v2/api"
# Chains where v2 tokennfttx is typically available with one key
ETHERSCAN_V2_CHAINIDS = {1, 10, 56, 130, 137, 8453, 42161}

ZERO = "0x0000000000000000000000000000000000000000"


def rpc_eth_call(url: str, to: str, data: str) -> str:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
        }
    ).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "nft_count_multichain/1"},
    )
    with urllib.request.urlopen(req, timeout=35) as r:
        out = json.loads(r.read().decode())
    if "error" in out:
        return ""
    res = out.get("result")
    return str(res or "")


def owner_of(rpc_url: str, contract: str, token_id: int) -> str:
    data = "0x6352211e" + int(token_id).to_bytes(32, "big").hex()
    try:
        res = rpc_eth_call(rpc_url, contract, data)
    except Exception:
        return ""
    if not res or res == "0x":
        return ""
    h = res.lower().replace("0x", "")
    if len(h) < 40:
        return ""
    return "0x" + h[-40:]


def fetch_tokennfttx_etherscan_v2(chain_id: int, owner: str, apikey: str) -> list[dict]:
    owner = owner.lower()
    all_rows: list[dict] = []
    page = 1
    while True:
        q = urllib.parse.urlencode(
            {
                "chainid": str(chain_id),
                "module": "account",
                "action": "tokennfttx",
                "address": owner,
                "page": str(page),
                "offset": "1000",
                "sort": "asc",
                "apikey": apikey,
            }
        )
        url = f"{ETHERSCAN_V2}?{q}"
        req = urllib.request.Request(url, headers={"User-Agent": "nft_count_multichain/1"})
        with urllib.request.urlopen(req, timeout=50) as r:
            data = json.loads(r.read().decode())
        st = str(data.get("status") or "")
        res = data.get("result")
        if st == "0" or not isinstance(res, list) or len(res) == 0:
            break
        all_rows.extend(res)
        if len(res) < 1000:
            break
        page += 1
        if page > 400:
            break
        time.sleep(0.21)
    return all_rows


def fetch_tokennfttx_blockscout(api_root: str, owner: str) -> list[dict]:
    owner = owner.lower()
    all_rows: list[dict] = []
    page = 1
    while True:
        q = urllib.parse.urlencode(
            {
                "module": "account",
                "action": "tokennfttx",
                "address": owner,
                "page": str(page),
                "offset": "1000",
            }
        )
        url = f"{api_root.rstrip('/')}?{q}"
        req = urllib.request.Request(url, headers={"User-Agent": "nft_count_multichain/1"})
        with urllib.request.urlopen(req, timeout=50) as r:
            data = json.loads(r.read().decode())
        st = str(data.get("status") or "")
        res = data.get("result")
        if st == "0" or not isinstance(res, list) or len(res) == 0:
            break
        all_rows.extend(res)
        if len(res) < 1000:
            break
        page += 1
        if page > 400:
            break
        time.sleep(0.12)
    return all_rows


def latest_events_by_token(rows: list[dict]) -> dict[tuple[str, str], dict]:
    best: dict[tuple[str, str], tuple[tuple[int, int, int], dict]] = {}
    for row in rows:
        c = str(row.get("contractAddress") or row.get("contractaddress") or "").strip().lower()
        tid = str(row.get("tokenID") or row.get("tokenId") or "").strip()
        if not c.startswith("0x") or not tid.isdigit():
            continue
        try:
            bn = int(str(row.get("blockNumber") or "0"), 10)
        except Exception:
            bn = 0
        try:
            ti = int(str(row.get("transactionIndex") or row.get("transactionindex") or "0"), 10)
        except Exception:
            ti = 0
        try:
            li = int(str(row.get("logIndex") or row.get("logindex") or "0"), 10)
        except Exception:
            li = 0
        k = (c, tid)
        score = (bn, ti, li)
        cur = best.get(k)
        if cur is None or score > cur[0]:
            best[k] = (score, row)
    return {k: v[1] for k, v in best.items()}


def verified_erc721_count_parallel(
    chain_id: int, rpc_url: str, owner: str, rows: list[dict], workers: int = 24
) -> tuple[int, int, int]:
    owner = owner.lower()
    by_tok = latest_events_by_token(rows)
    jobs: list[tuple[str, int]] = []
    for (c, tid_s), row in by_tok.items():
        to = str(row.get("to") or "").strip().lower()
        if to != owner or to == ZERO:
            continue
        try:
            tid = int(tid_s, 10)
        except Exception:
            continue
        jobs.append((c, tid))
    if not jobs:
        return 0, 0, len(by_tok)

    ok = 0

    def _check(ct: tuple[str, int]) -> bool:
        c, tid = ct
        ow = owner_of(rpc_url, c, tid).lower()
        return ow == owner

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_check, j) for j in jobs]
        for f in as_completed(futs):
            try:
                if f.result():
                    ok += 1
            except Exception:
                pass
    return ok, len(jobs), len(by_tok)


def main() -> int:
    addrs = [a.strip() for a in (sys.argv[1:] or ADDRS_DEFAULT) if a.strip()]
    apikey = os.environ.get("ETHERSCAN_API_KEY", "").strip()
    chains = sorted(set(RPC_BY_CHAIN.keys()) & set(BLOCKSCOUT_API.keys()))

    print(
        "Method: per chain — full tokennfttx index, unique (contract,tokenId), "
        "last transfer `to` == wallet, then on-chain ownerOf must match.\n"
        "Scope: chains with both Blockscout-compatible API + public RPC in this script.\n"
        f"Chains ({len(chains)}): {chains}\n"
        f"ETHERSCAN_API_KEY: {'yes' if apikey else 'no (Blockscout only)'}\n"
    )

    grand = 0
    for addr in addrs:
        addr_l = addr.lower()
        if not addr_l.startswith("0x") or len(addr_l) != 42:
            print(f"Skip bad address: {addr}")
            continue
        total = 0
        lines: list[tuple[int, int, int, int, str]] = []
        for cid in chains:
            rpc = RPC_BY_CHAIN[cid]
            rows: list[dict] = []
            src = ""
            try:
                if apikey and cid in ETHERSCAN_V2_CHAINIDS:
                    rows = fetch_tokennfttx_etherscan_v2(cid, addr_l, apikey)
                    src = "etherscan_v2"
                if not rows:
                    rows = fetch_tokennfttx_blockscout(BLOCKSCOUT_API[cid], addr_l)
                    src = src or "blockscout"
            except Exception as e:
                lines.append((cid, 0, 0, 0, f"fetch_err:{e}"))
                continue
            if not rows:
                lines.append((cid, 0, 0, 0, "no_rows"))
                continue
            v, cand, nuniq = verified_erc721_count_parallel(cid, rpc, addr_l, rows)
            total += v
            lines.append((cid, v, cand, nuniq, src))
        grand += total
        print(f"=== {addr} ===")
        print("chain_id\tverified\tcandidates\tunique_indexed\tsource")
        for cid, v, cand, nuniq, src in lines:
            print(f"{cid}\t{v}\t{cand}\t{nuniq}\t{src}")
        print(f"TOTAL (ERC-721, verified on-chain, listed chains): {total}\n")

    print(f"SUM both addresses (same chains): {grand}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
