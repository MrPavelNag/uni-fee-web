# RIKO contracts deploy (Foundry)

This guide deploys:

- `contracts/RIKOVault.sol`
- `contracts/RIKOYieldDistributor.sol`

Recommended: deploy both in one step via `script/DeployRIKOStack.s.sol`.

## 1) Prerequisites

- Foundry installed (`forge --version`)
- funded deploy wallet on target network
- RPC URL for target network

## 2) Environment variables

Set variables in your shell (or in `.env` if you use `source .env`):

```bash
export DEPLOYER_PRIVATE_KEY="0x...your_private_key..."
export RIKO_ADMIN="0x...admin_wallet_address..."
```

For Sepolia:

```bash
export ETH_RPC_URL="https://sepolia.infura.io/v3/<key>"
```

For Mainnet:

```bash
export ETH_RPC_URL="https://mainnet.infura.io/v3/<key>"
```

Notes:
- `RIKO_ADMIN` becomes `owner` of `RIKOVault` in constructor.
- `RIKO_ADMIN` must be a valid non-zero EVM address.

## 3) Build

```bash
forge build
```

## 4) Deploy

### Option A (recommended): deploy both contracts in one run

Dry-run (no broadcast):

```bash
forge script script/DeployRIKOStack.s.sol:DeployRIKOStack \
  --rpc-url "$ETH_RPC_URL"
```

Real deploy:

```bash
forge script script/DeployRIKOStack.s.sol:DeployRIKOStack \
  --rpc-url "$ETH_RPC_URL" \
  --broadcast
```

After success you will see:

- `RIKOVault deployed at: 0x...`
- `RIKOYieldDistributor deployed at: 0x...`

### Option B: deploy only vault (legacy / manual flow)

Dry-run (no broadcast):

```bash
forge script script/DeployRIKOVault.s.sol:DeployRIKOVault \
  --rpc-url "$ETH_RPC_URL"
```

Real deploy:

```bash
forge script script/DeployRIKOVault.s.sol:DeployRIKOVault \
  --rpc-url "$ETH_RPC_URL" \
  --broadcast
```

Use this flow only if you explicitly need manual step-by-step deployment.

## 5) Connect address to webapp

```bash
export RIKO_VAULT_ADDRESS="0x...deployed_rikovault_address..."
export RIKO_YIELD_DISTRIBUTOR_ADDRESS="0x...deployed_yield_distributor_address..."
```

Restart your webapp process so it reads the new env.

## 6) First on-chain setup after deploy

From admin UI (`/admin`, RIKO tab):

1. Set `custodyAddress` in `RIKOVault`.
2. Set `pendingRedemptionOperator` in `RIKOVault`.
3. Configure whitelist tokens (`setTokenConfig`) and optional caps.
4. In `RIKOYieldDistributor` set:
   - `yieldPayerAddress`
   - `yieldTokenAddress`
   - `monthlyYieldRateBps` (opens first cycle)

## 7) Optional: Etherscan verification

```bash
export ETHERSCAN_API_KEY="<your_key>"
forge verify-contract \
  --chain-id 11155111 \
  --compiler-version v0.8.24+commit.e11b9ed9 \
  <DEPLOYED_ADDRESS> \
  contracts/RIKOVault.sol:RIKOVault \
  --constructor-args $(cast abi-encode "constructor(address)" "$RIKO_ADMIN") \
  --etherscan-api-key "$ETHERSCAN_API_KEY"
```

For Mainnet, use `--chain-id 1`.

You can verify distributor similarly:

```bash
forge verify-contract \
  --chain-id 11155111 \
  --compiler-version v0.8.24+commit.e11b9ed9 \
  <DEPLOYED_DISTRIBUTOR_ADDRESS> \
  contracts/RIKOYieldDistributor.sol:RIKOYieldDistributor \
  --constructor-args $(cast abi-encode "constructor(address,address)" "$RIKO_ADMIN" "$RIKO_VAULT_ADDRESS") \
  --etherscan-api-key "$ETHERSCAN_API_KEY"
```
