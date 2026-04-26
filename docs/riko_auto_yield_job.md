# RIKO automatic payout bot

This job performs scheduled **off-chain** ERC-20 payouts to current RIKO holders.

Flow:

1. Read latest US Treasury 10Y from official XML feed.
2. Convert annual rate to monthly percent (`annual / 12`) and to `bps/month`.
3. Apply guardrails (min/max BPS).
4. Build holder set from RIKO `Transfer` logs and re-check each holder using live `balanceOf()`.
5. Compute payout amounts and send ERC-20 `transfer(address,uint256)` txs from payout wallet.
6. Send Telegram report with result and transaction hashes.

## Required env vars

- `RIKO_AUTO_YIELD_ENABLED=1`
- `RIKO_VAULT_ADDRESS=0x...` (RIKO token address)
- `RIKO_AUTO_YIELD_RPC_URL=https://...`
- `RIKO_AUTO_YIELD_PRIVATE_KEY=0x...` (private key of payout wallet)

Bot-config values are editable in admin UI:

- payout token address
- holder scan start block

Telegram:

- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## Optional env vars

- `RIKO_AUTO_YIELD_INTERVAL_SEC=86400` (job loop interval)
- `RIKO_AUTO_YIELD_RUN_ON_STARTUP=1` (run immediately after app start)
- `RIKO_AUTO_YIELD_MIN_BPS=0`
- `RIKO_AUTO_YIELD_MAX_BPS=25`

If no admin override is saved yet:

- `RIKO_AUTO_YIELD_PAYOUT_TOKEN_ADDRESS=0x...`
- `RIKO_AUTO_YIELD_HOLDER_SCAN_START_BLOCK=0`

## Safety notes

- Job is skipped when `RIKO_PILOT_CONFIG_FROZEN=1`.
- Job is skipped if payout wallet balance is below required payout total.
- Last run state is stored in `riko_auto_yield_state_v1`.
- Holder cache is stored in `riko_auto_yield_holder_cache_v1`.
