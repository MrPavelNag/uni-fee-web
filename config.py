"""
Configuration for Uniswap pool fee agent.
Token pairs are specified as comma-separated: "fluid,eth" or "fluid,usdc"
"""

import os

# Minimum TVL in USD to include a pool (агент fee / пулы). Override via env: MIN_TVL=0 for all pools
MIN_TVL_USD = 100

# LP allocation size for fee calculation (USD)
LP_ALLOCATION_USD = 1_000

# Fee data period (days). 90 = 3 months
FEE_DAYS = int(os.environ.get("FEE_DAYS", "90"))

# Output files (in data/ folder)
OUTPUT_PDF = "data/available_pairs.pdf"
OUTPUT_CHART = "data/fee_chart.pdf"

# Token pairs to search. Override via TOKEN_PAIRS env. INST = FLUID (same token).
DEFAULT_TOKEN_PAIRS = "fluid,eth;eth,fluid;inst,eth;eth,inst;eth,uni;uni,eth"

# Token addresses by chain (lowercase for matching) — фиксированный curated-список, без порога USD.
# В webapp к этой мапе можно добавить топ-N по TVL из сабграфа: TOKENS_MAJOR_TOP_N (см. webapp/main.py).
# FLUID = INST (same token). Ethereum has different address than other EVM chains.
TOKEN_ADDRESSES = {
    "ethereum": {
        "fluid": "0x6f40d4a6237c257fff2db00fa0510deeecd303eb",
        "inst": "0x6f40d4a6237c257fff2db00fa0510deeecd303eb",
        "uni": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "wbtc": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
        "eth": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",  # WETH
        "weth": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "usdc": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        "usdt": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "rlc": "0x607f4c5bb672230e8672085532f7e901544a7375",  # iExec RLC
        "paxg": "0x45804880de22913dafe09f4980848ece6ecbaf78",  # Paxos Gold
        "xaut": "0x68749665ff8d2d112fa859aa293f07a622782f38",  # Tether Gold (XAUt)
    },
    "arbitrum": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0",
        "wbtc": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
        "eth": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
        "weth": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        "usdc": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        "usdt": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        "paxg": "0xfeb4dfc8c4cf7ed305bb08065d08ec6ee6728429",  # Paxos Gold (bridged)
        "xaut": "0x40461291347e1ecbb09499f3371d3f17f10d7159",  # Tether Gold (XAUt)
    },
    "base": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        "wbtc": "0x0555e30da8f9830a4d47e02a04f16b17f5b95b8e",
        "eth": "0x4200000000000000000000000000000000000006",  # WETH
        "weth": "0x4200000000000000000000000000000000000006",
        "usdc": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        "usdt": "0x0000000000000000000000000000000000000000",
        "paxg": "0x149a7407d5beb1b9fa00910802708f55de5ed662",  # Paxos Gold (bridged)
    },
    "optimism": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0x6fd9d7ad17242c41f7131d257212c54a0e816691",
        "wbtc": "0x68f180fcce6836688e9084f035309e29bf0a2095",
        "eth": "0x4200000000000000000000000000000000000006",
        "weth": "0x4200000000000000000000000000000000000006",
        "usdc": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
        "usdt": "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58",
    },
    "polygon": {
        # INST (Instadapp PoS) = FLUID on Polygon - different address than L2s
        "fluid": "0xf50d05a1402d0adafa880d36050736f9f6ee7dee",
        "inst": "0xf50d05a1402d0adafa880d36050736f9f6ee7dee",
        "uni": "0xb33eaad8d922b1083446dc23f610c2567fb5180f",
        "wbtc": "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6",
        "eth": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",  # WETH
        "weth": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619",
        "pol": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",  # WMATIC / Wrapped POL
        "matic": "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270",
        "usdc": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
        "usdt": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
        "paxg": "0x4d44f2f760f2b98ffa74ea04cd3e3f9f102cee1c",  # Paxos Gold (bridged)
        "xaut": "0xf1815bd50389c46847f0bda824ec8da914045d14",  # Tether Gold (XAUt)
    },
    "bsc": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0xbf5140a22578168fd562dccf235e5d43a02ce9b1",
        "wbtc": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c",
        "eth": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",  # ETH on BSC
        "weth": "0x2170ed0880ac9a755fd29b2688956bd959f933f8",
        "bnb": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",  # WBNB
        "wbnb": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
        "usdc": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
        "usdt": "0x55d398326f99059ff775485246999027b3197955",
    },
    "avalanche": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0x8ebaf22b6f053841ffe8beb32287353f1f9dd893",
        "wbtc": "0x50b7545627a5162f82a992c33b87adc75187b218",
        "eth": "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab",  # WETH
        "weth": "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab",
        "usdc": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
        "usdt": "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c6",
    },
    "celo": {
        "fluid": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "inst": "0x61e030a56d33e8260fdd81f03b162a79fe3449cd",
        "uni": "0x22e2225eb2c740519b9638269b21b891914d8a0e",
        "wbtc": "0xbaab46e28388d2779e6e31fd00cf0e5ad95e327b",
        "eth": "0x2def4285787d58a2f811af24755a8150622f4371",  # WETH
        "weth": "0x2def4285787d58a2f811af24755a8150622f4371",
        "usdc": "0xceba9300f2b948710d2653dd7b07f33a8b32118c",
        "usdt": "0x617f3112bf5397d0467d315cc709ef968d9ba546",
    },
    "unichain": {
        "uni": "0x8f187aa05619a017077f5308904739877ce9ea21",
        "wbtc": "0x0555e30da8f9830a4d47e02a04f16b17f5b95b8e",
        "eth": "0x4200000000000000000000000000000000000006",  # WETH
        "weth": "0x4200000000000000000000000000000000000006",
        "usdc": "0x078d782b760474a361dda0af3839290b0ef57ad6",
        "usdt": "0x0000000000000000000000000000000000000000",
    },
}

# Uniswap v3 subgraph IDs (from docs.uniswap.org)
UNISWAP_V3_SUBGRAPHS = {
    "ethereum": "5zvR82QoaXYFyDEKLZ9t6v9adgnptxYpKpSbxtgVENFV",
    "arbitrum": "FbCGRftH4a3yZugY7TnbYgPJVEv2LvMT6oF1fxPe9aJM",
    "base": "43Hwfi3dJSoGpyas9VwNoDAv55yjgGrPpNSmbQZArzMG",
    "optimism": "Cghf4LfVqPiFw6fp6Y5X5Ubc8UpmUhSfJL82zwiBFLaj",
    "polygon": "3hCPRGf4z88VC5rsBKU5AA9FBBq5nF3jbKJG7VZCbhjm",
    "bsc": "F85MNzUGYqgSHSHRGgeVMNsdnW1KtZSVgFULumXRZTw2",
    "avalanche": "GVH9h9KZ9CqheUEL93qMbq7QwgoBu32QXQDPR6bev4Eo",
    "celo": "ESdrTJ3twMwWVoQ1hUE2u7PugEHX3QkenudD6aXCkDQ4",
    "unichain": "BCfy6Vw9No3weqVq9NhyGo4FkVCJep1ZN9RMJj5S32fX",
}

# Uniswap v4 subgraph IDs (from The Graph Explorer, docs.uniswap.org)
UNISWAP_V4_SUBGRAPHS = {
    "ethereum": "DiYPVdygkfjDWhbxGSqAQxwBKmfKnkWQojqeM2rkLb3G",
    "arbitrum": "G5TsTKNi8yhPSV7kycaE23oWbqv9zzNqR49FoEQjzq1r",
    "base": "6UjxSFHTUa98Y4Uh4Tb6suPVyYxgPHpPEPfmFNihzTHp",
    "unichain": "EoCvJ5tyMLMJcTnLQwWpjAtPdn74PcrZgzfcT5bYxNBH",
    "polygon": "CwpebM66AH5uqS5sreKij8yEkkPcHvmyEs7EwFtdM5ND",
}

# Сети для v4 по умолчанию
def _normalize_v4_chain_slug(raw: str) -> str:
    s = (raw or "").strip()
    if s == "arbitrum-one":
        return "arbitrum"
    return s


_V4_CHAINS_RAW = [c.strip() for c in os.environ.get("V4_CHAINS", "ethereum,arbitrum,base,unichain,polygon").split(",") if c.strip()]
V4_CHAINS = [_normalize_v4_chain_slug(c) for c in _V4_CHAINS_RAW]

# Goldsky public endpoints (no API key, rate limited) - alternative to The Graph
# Base: use ``prod`` tag — ``1.0.0`` is a stale snapshot and breaks pool/position data for scans.
GOLDSKY_ENDPOINTS = {
    "base": "https://api.goldsky.com/api/public/project_cl8ylkiw00krx0hvza0qw17vn/subgraphs/uniswap-v3-base/prod/gn",
}

# v4 alternative: set env V4_OVERRIDE_BASE, V4_OVERRIDE_UNICHAIN etc. to use Ormi/other
# Example: export V4_OVERRIDE_BASE="https://..."
