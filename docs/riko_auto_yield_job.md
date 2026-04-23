# RIKO automatic monthly yield updater

This job updates `monthlyYieldRateBps` in `RIKOYieldDistributor` from US Treasury 10Y data.

Flow:

1. Read latest US Treasury 10Y from official XML feed.
2. Convert annual rate to monthly percent (`annual / 12`) and to `bps/month`.
3. Apply guardrails (min/max and max step per run).
4. Send on-chain tx `setMonthlyYieldRateBps(...)`.
5. Send Telegram report with result and tx hash.

## Required env vars

- `RIKO_AUTO_YIELD_ENABLED=1`
- `RIKO_YIELD_DISTRIBUTOR_ADDRESS=0x...`
- `RIKO_AUTO_YIELD_RPC_URL=https://...`
- `RIKO_AUTO_YIELD_PRIVATE_KEY=0x...`  (must be owner key of `RIKOYieldDistributor`)

Telegram (already used in project):

- `TELEGRAM_BOT_TOKEN=...`
- `TELEGRAM_CHAT_ID=...`

## Optional env vars

- `RIKO_AUTO_YIELD_INTERVAL_SEC=86400` (loop interval; tx is only sent on day 1 of each month)
- `RIKO_AUTO_YIELD_RUN_ON_STARTUP=1` (run immediately after app start)
- `RIKO_AUTO_YIELD_MIN_BPS=0`
- `RIKO_AUTO_YIELD_MAX_BPS=25`
- `RIKO_AUTO_YIELD_MAX_DELTA_BPS=3`

## Safety notes

- Job is skipped when `RIKO_PILOT_CONFIG_FROZEN=1`.
- Job refuses to send tx if signer is not current `owner()` of yield distributor.
- If target change is too large, it is step-limited by `RIKO_AUTO_YIELD_MAX_DELTA_BPS`.
- Last run state is stored in analytics state key `riko_auto_yield_state_v1`.
