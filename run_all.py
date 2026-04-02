#!/usr/bin/env python3
"""
Run all agents with one command: v3 -> v4 -> merge.

Modes:
  1) Pair mode: python run_all.py "paxg,usdt;paxg,usdc"
  2) Token mode: python run_all.py --tokens "paxg,fluid,wbtc" -> for each token,
     run pair scans against usdt, usdc, eth (separate run and output files).
"""

import argparse
import os
import subprocess
import sys

# Quote tokens for --tokens mode: scan token+quote pairs
DEFAULT_QUOTE_TOKENS = ["usdt", "usdc", "eth"]


def run_pipeline(env: dict, args, token_pairs: str) -> int:
    """Run v3 -> v4 -> merge for given TOKEN_PAIRS. Returns exit code."""
    env = env.copy()
    env["TOKEN_PAIRS"] = token_pairs
    print("TOKEN_PAIRS:", token_pairs)
    if args.min_tvl is not None:
        print("--min-tvl:", args.min_tvl)

    cmd_extra = []
    if args.min_tvl is not None:
        cmd_extra = ["--min-tvl", str(args.min_tvl)]

    agents = [
        ("agent_v3.py", "Agent 1 (v3)", True),
        ("agent_v4.py", "Agent 2 (v4)", True),
        ("agent_merge.py", "Agent 3 (merge)", False),
    ]
    for script, name, takes_min_tvl in agents:
        print("\n" + "=" * 50)
        print(f">>> {name}: python {script}")
        print("=" * 50)
        extra = cmd_extra if takes_min_tvl else []
        if script == "agent_merge.py":
            if args.exclude_chains:
                extra += ["--exclude-chains", args.exclude_chains]
            if args.exclude_pools_suffix:
                extra += ["--exclude-pools-suffix", args.exclude_pools_suffix]
        cmd = [sys.executable, script] + extra
        r = subprocess.run(cmd, env=env)
        if r.returncode != 0:
            return r.returncode
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run all agents: v3 → v4 → merge",
        epilog=(
            "Examples:\n"
            "  python run_all.py uni,eth --min-tvl 500\n"
            "  python run_all.py \"wbtc,usdt;wbtc,usdc\" --min-tvl 500000\n"
            "  python run_all.py --tokens \"paxg,fluid,wbtc\"   # per-token scan (pairs with usdt, usdc, eth)"
        ),
    )
    parser.add_argument(
        "pairs",
        nargs="?",
        default=None,
        help="Token pairs, e.g. uni,eth or fluid,eth;uni,eth",
    )
    parser.add_argument(
        "--tokens",
        type=str,
        default="",
        help="Comma-separated tokens: for each token run pipeline with pairs token+usdt, token+usdc, token+eth",
    )
    parser.add_argument("--min-tvl", type=float, default=None, help="Min TVL USD")
    parser.add_argument(
        "--exclude-chains",
        type=str,
        default="",
        help="Comma-separated chains to exclude from chart (merge only), e.g. base,polygon",
    )
    parser.add_argument(
        "--exclude-pools-suffix",
        type=str,
        default="",
        help="Comma-separated last-4 chars of pool_id to exclude from chart (merge only), e.g. 1a2b,dead",
    )
    args = parser.parse_args()

    env = os.environ.copy()

    if args.tokens.strip():
        # Per-token mode: separate run for each token
        tokens = [t.strip().lower() for t in args.tokens.split(",") if t.strip()]
        if not tokens:
            print("--tokens: token list is empty")
            sys.exit(1)
        print("Token mode. Tokens:", tokens)
        print("Quote tokens:", DEFAULT_QUOTE_TOKENS)
        for i, base in enumerate(tokens):
            pairs_str = ";".join(f"{base},{q}" for q in DEFAULT_QUOTE_TOKENS)
            print("\n" + "#" * 60)
            print(f"# Token [{i+1}/{len(tokens)}]: {base}")
            print("#" * 60)
            code = run_pipeline(env, args, pairs_str)
            if code != 0:
                sys.exit(code)
        print("\n" + "=" * 50)
        print("Done. Check data/fee_chart_*.pdf for each token.")
        print("=" * 50)
    else:
        # Standard mode: one run for provided pairs
        token_pairs = args.pairs or env.get("TOKEN_PAIRS", "fluid,eth;uni,eth")
        code = run_pipeline(env, args, token_pairs)
        if code != 0:
            sys.exit(code)
        print("\n" + "=" * 50)
        print("Done. Check data/fee_chart_*.pdf")
        print("=" * 50)
