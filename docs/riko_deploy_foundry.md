# RIKO contract deploy (Foundry)

This guide deploys:

- `contracts/RIKOVault.sol`

Recommended: deploy via `script/DeployRIKOStack.s.sol`.

## 1) Prerequisites

- Foundry installed (`forge --version`)
- funded deploy wallet on target network
- RPC URL for target network

## 2) Environment variables

```bash
export DEPLOYER_PRIVATE_KEY="0x...your_private_key..."
export RIKO_ADMIN="0x...admin_wallet_address..."
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

Dry-run:

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

## 5) Connect address to webapp

```bash
export RIKO_VAULT_ADDRESS="0x...deployed_rikovault_address..."
```

Restart webapp process to reload env.

## 6) First on-chain setup after deploy

From admin UI:

1. Set `custodyAddress` in `RIKOVault`.
2. Set `pendingRedemptionOperator` in `RIKOVault`.
3. Configure whitelist tokens (`setTokenConfig`) and caps if needed.
4. Set initial `RIKO` price.

## 7) Optional: Etherscan verification

```bash
export ETHERSCAN_API_KEY="<your_key>"
forge verify-contract \
  --chain-id 1 \
  --compiler-version v0.8.24+commit.e11b9ed9 \
  <DEPLOYED_ADDRESS> \
  contracts/RIKOVault.sol:RIKOVault \
  --constructor-args $(cast abi-encode "constructor(address)" "$RIKO_ADMIN") \
  --etherscan-api-key "$ETHERSCAN_API_KEY"
```
