# RIKOVault deploy (Foundry)

This guide deploys `contracts/RIKOVault.sol` and prints the real contract address.

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

After success you will see:
- `RIKOVault deployed at: 0x...`
- tx hash in Foundry output

Use this `0x...` as runtime contract address.

## 5) Connect address to webapp

```bash
export RIKO_VAULT_ADDRESS="0x...deployed_rikovault_address..."
```

Restart your webapp process so it reads the new env.

## 6) Optional: Etherscan verification

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
