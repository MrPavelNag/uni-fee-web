# RIKO Pilot Config Freeze

Last updated: 2026-04-22

## Purpose

Freeze pilot configuration to prevent accidental admin changes during private pilot runs.

## Server switch

Use environment variable:

- `RIKO_PILOT_CONFIG_FROZEN=1` -> freeze enabled
- `RIKO_PILOT_CONFIG_FROZEN=0` (or unset) -> freeze disabled

When freeze is enabled, the backend blocks these admin mutations with HTTP `423`:

- `/api/admin/admin-wallets`
- `/api/admin/pair-lists-manual`
- `/api/admin/riko/whitelist/pending`
- `/api/admin/riko/whitelist/apply-nonce`
- `/api/admin/riko/whitelist/apply`

Read-only APIs continue to work.

## Current snapshot (private pilot)

- Admin wallets (effective): `0xd3155f6a7525f81595609e83789bbb95d91aaedf`
- Root admins: `0xd3155f6a7525f81595609e83789bbb95d91aaedf`
- Protected admins: `0xd3155f6a7525f81595609e83789bbb95d91aaedf`
- Pending admin additions: none
- Pending protect updates: none
- Pending removals: none

Whitelist snapshot:

- Total symbols: `38`
- Symbols with known address marker/value: `12`
- Native symbol: `eth` (native marker flow)
- Symbols without known Ethereum address currently: `op,xrp,ada,bch,xlm,hype,leo,okb,wbt,usd1,pyusd,fdusd,rlusd,usdg,tusd,gusd,husd,buidl,usyc,usdy,usdtb,usd0,frxusd,mnee,cgusd`

## Recommended pilot-safe operation

1. Unfreeze only for planned maintenance window.
2. Apply exactly one approved change set.
3. Re-freeze immediately.
4. Confirm `/api/admin/settings` returns `"pilot_config_frozen": true`.
