# RIKO automatic daily yield updater

This job updates `dailyYieldRateBps` on-chain from US Treasury 10Y daily data.

Flow:

1. Read latest US Treasury 10Y from official XML feed.
2. Convert to daily percent (`annual / 365`) and to `bps/day`.
3. Apply guardrails (min/max and max step per run).
4. Send on-chain tx `setDailyYieldRateBps(...)`.
5. Send Telegram report with result and tx hash.

## Required env vars

- `RIKO_AUTO_YIELD_ENABLED=1`
- `RIKO_VAULT_ADDRESS=0x...`
- `RIKO_AUTO_YIELD_RPC_URL=https://...`
- `RIKO_AUTO_YIELD_PRIVATE_KEY=0x...`  (must be owner key of `RIKOVault`)

Telegram (already used in project):

- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## Optional env vars

- `RIKO_AUTO_YIELD_INTERVAL_SEC=86400` (default once per day)
- `RIKO_AUTO_YIELD_RUN_ON_STARTUP=1` (run immediately after app start)
- `RIKO_AUTO_YIELD_MIN_BPS=0`
- `RIKO_AUTO_YIELD_MAX_BPS=25`
- `RIKO_AUTO_YIELD_MAX_DELTA_BPS=3`

## Safety notes

- Job is skipped when `RIKO_PILOT_CONFIG_FROZEN=1`.
- Job refuses to send tx if signer is not current `owner()` of vault.
- If target change is too large, it is step-limited by `RIKO_AUTO_YIELD_MAX_DELTA_BPS`.
- Last run state is stored in analytics state key `riko_auto_yield_state_v1`.
