# Contracts Security Model

This document summarizes operational roles, permissions, and core invariants for:

- `RIKOVault`
- `RIKOYieldDistributor`

It is intended as a quick release checklist reference for pilot/mainnet operations.

## Roles

- `Owner` (`Ownable2Step`)
- `Pending Redemption Operator` (vault-specific)
- `Custody Address` (vault-specific)
- `Yield Payer Address` (distributor-specific)
- `User` (depositor/redeemer)

## Permission Matrix

| Role | Contract | Can do |
|---|---|---|
| `Owner` | `RIKOVault` | `pause/unpause`, set global cap, set per-token cap, set custody, set RIKO price, set token config, set pending redemption operator |
| `Owner` | `RIKOYieldDistributor` | `pause/unpause`, set yield payer, set yield token, set monthly yield bps (opens new cycle), execute batched push payouts |
| `Pending Redemption Operator` | `RIKOVault` | call `processPendingRedemption(account, token)` |
| `Custody Address` | `RIKOVault` | holds deposited assets, grants allowance to vault for pull-based redemption settlement |
| `Yield Payer Address` | `RIKOYieldDistributor` | provides payout token via `safeTransferFrom` during owner-triggered batch payout |
| `User` | `RIKOVault` | `deposit`, `redeem`, `cancelPendingRedemption`, read quotes |
| `User` | `RIKOYieldDistributor` | no direct payout action (claim paths disabled) |

## Core Trust Boundaries

- `RIKOVault` and `RIKOYieldDistributor` are intentionally decoupled.
- Yield payout logic is not used in vault mint/redeem accounting.
- Vault asset solvency depends on custody balances + custody allowance.
- Distributor payout solvency depends on yield payer balances + yield payer allowance.

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

### Yield Invariants (`RIKOYieldDistributor`)

1. **Cycle-based payout**
   - New cycle opens only when owner updates `monthlyYieldRateBps`.
   - Rate is snapshotted per-cycle (`yieldRateBpsByCycle`) at cycle-open time.

2. **Balance-based push payout**
   - Payout is computed from current `RIKO` balance of each account in the batch.
   - Each account can be paid at most once per cycle (`wasPaidInCycle`).

3. **No pull-claim path**
   - `claim*` functions are disabled by design (`RYD_ClaimDisabled`).

4. **Payout source safety**
   - Payout token transfer is pull-based from `yieldPayerAddress` via `safeTransferFrom`.
   - Batch payout requires non-zero configured yield token and payer address.

5. **Reentrancy and pause controls**
   - Batch payouts are guarded by `nonReentrant` and `whenNotPaused`.

## Operational Notes

- Before enabling user actions:
  - set custody address
  - set pending redemption operator
  - configure and verify token feeds
  - set initial RIKO price

- Before enabling yield payouts:
  - set yield token
  - set yield payer
  - ensure yield payer funded and approved distributor
  - set monthly yield bps (opens payout cycle)
  - run owner batch payout over holder list

- For incident response:
  - `pause()` both contracts if abnormal behavior is detected
  - investigate custody/yield-payer balances and allowances first

