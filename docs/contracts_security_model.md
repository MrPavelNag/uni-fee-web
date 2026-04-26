# Contracts Security Model

This document summarizes operational roles, permissions, and core invariants for:

- `RIKOVault`
- off-chain RIKO payout bot (server-side scheduler + signer wallet)

It is intended as a quick release checklist reference for pilot/mainnet operations.

## Roles

- `Owner` (`Ownable2Step`)
- `Pending Redemption Operator` (vault-specific)
- `Custody Address` (vault-specific)
- `Payout Wallet` (bot signer, off-chain)
- `User` (depositor/redeemer)

## Permission Matrix

| Role | System | Can do |
|---|---|---|
| `Owner` | `RIKOVault` | `pause/unpause`, set global cap, set per-token cap, set custody, set RIKO price, set token config, set pending redemption operator |
| `Pending Redemption Operator` | `RIKOVault` | call `processPendingRedemption(account, token)` |
| `Custody Address` | `RIKOVault` | holds deposited assets, grants allowance to vault for pull-based redemption settlement |
| `User` | `RIKOVault` | `deposit`, `redeem`, `cancelPendingRedemption`, read quotes |
| `Payout Wallet` | off-chain bot | signs direct ERC-20 `transfer` payouts to current RIKO holders by schedule |

## Core Trust Boundaries

- Vault mint/redeem accounting is fully isolated from payout logic.
- Payout logic is off-chain and configurable in admin panel.
- Vault asset solvency depends on custody balances + custody allowance.
- Bot payout solvency depends on payout-wallet token balance and private key safety.

## Security Invariants

### Vault Invariants (`RIKOVault`)

1. **Whitelist gate**
   - Only configured `allowed=true` tokens can be deposited/redeemed.

2. **Oracle integrity checks**
   - Feed address must match configured description hash.
   - Feed decimals must be within allowed bounds.
   - Price answer must be positive.
   - Round metadata must be internally consistent.
   - Price must not be stale (`maxOracleAge`).

3. **Cap enforcement**
   - Global cap limits total minted supply (in USD6 terms).
   - Per-token cap limits total token exposure (vault + custody) in USD6 terms.

4. **Custody-flow safety**
   - Deposits are forwarded to custody.
   - Redemptions pull shortfall from custody only when needed.
   - If liquidity is insufficient, redeem can queue instead of force-failing the state.

5. **Pending redemption control**
   - Processing is restricted to configured operator.
   - User can cancel own pending redeem and recover locked RIKO.

6. **Reentrancy and pause controls**
   - State-changing user actions are guarded by `nonReentrant` and `whenNotPaused`.

### Off-chain Payout Invariants (Bot)

1. **Holder source of truth**
   - Candidate holder addresses are collected from `RIKO` `Transfer` logs.
   - Final eligibility always uses live on-chain `balanceOf()` per address.

2. **Schedule gate**
   - Bot only executes on configured UTC dates/time.
   - Duplicate execution for same schedule key is blocked by saved run state.

3. **Balance-based payout**
   - Payout is computed from current `RIKO` balances with configured BPS logic.
   - Zero-amount payouts are skipped.

4. **Payout source safety**
   - Payout token is transferred directly from configured payout wallet.
   - Run is skipped when payout-wallet balance is insufficient.

## Operational Notes

- Before enabling user actions:
  - set custody address
  - set pending redemption operator
  - configure and verify token feeds
  - set initial RIKO price

- Before enabling off-chain payouts:
  - set payout token address in admin panel
  - set holder scan start block in admin panel
  - ensure payout wallet is funded
  - ensure server has `RIKO_AUTO_YIELD_RPC_URL` and `RIKO_AUTO_YIELD_PRIVATE_KEY`
  - verify payout schedule (dates + UTC time)

- For incident response:
  - `pause()` vault if mint/redeem behavior is abnormal
  - disable auto-payout bot in admin panel
  - investigate custody and payout-wallet balances first

