# RIKO Mainnet Readiness Checklist

Last updated: 2026-04-22

Scope: `contracts/RIKOVault.sol` + RIKO admin workflow in `webapp/main.py`.

## Current status summary

- **Overall:** `NOT READY FOR MAINNET`
- **Use now:** testnet / staging / guarded pilot only

## Checklist with status

### 1) Smart contract security hardening

- [x] Whitelist-only token gating
- [x] Reentrancy guard on state-changing flows
- [x] Pause / unpause emergency switch
- [x] Oracle stale-check and non-positive price reject
- [x] Oracle feed binding check (`description` hash)
- [x] Oracle decimals sanity check
- [x] Token decimals sanity check
- [x] Global supply cap and per-token TVL cap
- [ ] Two-step on-chain config changes (propose/execute with delay)
- [ ] On-chain rate limits / per-epoch mint-redeem throttles

### 2) Testing

- [x] Unit tests for deposit/redeem/config paths (Foundry: `test/RIKOVault.t.sol`)
- [x] Negative tests (bad feed, stale feed, bad decimals, pause, insufficient liquidity, disable/re-enable) — `test/RIKOVault.negative.t.sol`
- [x] Invariant tests (supply and accounting invariants) — `test/RIKOVault.invariant.t.sol`
- [x] Fuzz tests (amounts/decimals/oracle edge cases) — `test/RIKOVault.fuzz.t.sol`
- [x] Differential / scenario tests for basket stress — `test/RIKOVault.scenario.t.sol`

Notes:
- Solidity tooling is now present: `foundry.toml` + `forge` tests.
- Current test command: `forge test -vv`
- CI pipeline added: `.github/workflows/solidity-tests.yml`

### 3) Deployment safety

- [ ] Deterministic deployment plan (chain, addresses, constructor params)
- [ ] Multisig as owner/admin (no EOA owner in production)
- [ ] Timelock for critical config updates
- [ ] Chain-specific oracle/feed allowlist runbook
- [ ] Post-deploy verification checklist (read-only asserts)

### 4) Monitoring and operations

- [ ] On-chain event monitoring for config changes, pause, large deposits/redeems
- [ ] Alerting on stale oracles / cap nearing / liquidity shortfall
- [ ] Incident runbook (pause, communication, recovery steps)
- [ ] Key management / signer rotation policy

### 5) External assurance

- [ ] Independent audit (required before public mainnet launch)
- [ ] Fixes from audit implemented and re-reviewed
- [ ] Public report or security disclosure statement
- [ ] Optional bug bounty for launch phase

## Practical go/no-go gates

Mainnet launch is allowed only when all gates below are green:

1. **Tests gate:** unit + invariant + fuzz are passing in CI.
2. **Ops gate:** multisig + timelock + monitoring + runbooks are in place.
3. **Audit gate:** independent audit issues of High/Critical severity are closed.
4. **Launch gate:** successful testnet rehearsal with production-like parameters.

Pilot policy note (current project decision):
- **Multisig migration trigger:** switch owner control to multisig immediately before any public launch, public user onboarding, or public announcement. Until then, run private pilot only with low caps and strict whitelist.

## Recommended execution order

1. Add Solidity toolchain and write unit tests.
2. Add invariant/fuzz suite and CI job.
3. Prepare multisig + timelock deployment plan.
4. Run independent audit.
5. Testnet rehearsal and staged rollout.
