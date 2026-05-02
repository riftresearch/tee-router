# Rift Devnet CLI

The Rift devnet CLI provides a local development environment for testing the Rift protocol.

## Usage

The devnet CLI has two commands:

### Server Command 

Run an interactive devnet server (the default mode):

```bash
cargo run --release --bin devnet server --fork-url <RPC_URL> [OPTIONS]
```

Direct CLI/server mode is deterministic. It binds the local chain RPCs,
provider mocks, token indexers, Bitcoin services, and manifest API to stable
ports so other local services can be wired by configuration instead of by
runtime discovery.

```text
Bitcoin RPC:             50100
Ethereum Anvil RPC/WS:   50101
Base Anvil RPC/WS:       50102
Arbitrum Anvil RPC/WS:   50103
Provider mocks:          50107
Manifest API:            50108
Bitcoin Esplora:         50110
Bitcoin ZMQ rawtx:       50111
Bitcoin ZMQ sequence:    50112
```

The manifest is available at:

```bash
curl http://localhost:50108/manifest.json
```

Options:
- `-f, --fork-url <URL>`: RPC URL to fork from (required)
- `-b, --fork-block-number <NUMBER>`: Block number to fork from (optional, uses latest if not specified)
- `--fund-address <ADDRESS>`: Address to fund with demo assets (can be specified multiple times)
- `--loadgen-evm-key-count <COUNT>`: Number of deterministic EVM private keys to fund for `router-loadgen` (defaults to 16)

The manifest includes `accounts.loadgen_evm_accounts`, a local-only pool of
funded EVM addresses/private keys. These keys are distinct from the Anvil
default/paymaster key and are funded with native gas plus local USDC/USDT on
Ethereum, Base, and Arbitrum.

Example:
```bash
cargo run --release --bin devnet server \
  --fork-url https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY \
  --fund-address 0x1234567890123456789012345678901234567890
```

### Cache Mode

Create a cached devnet for improving the time to boot devnets during integration tests:

```bash
cargo run --release --bin devnet cache
```

This will:
1. Clear your any existing cache
2. Build a fresh devnet then save all state to `~/.cache/rift-devnet/`
3. Exit after successful caching

The cached devnet significantly speeds up subsequent devnet launches by avoiding:
- Re-mining Bitcoin blocks
- Re-deploying contracts
- Re-indexing blockchain data

## State Directories

Persistent cached devnet state is stored in `~/.cache/rift-devnet/` only when `cargo run --release --bin devnet cache` is run. Normal server runs keep Bitcoin, electrs, Anvil dump state, and Anvil fork RPC cache data in devnet-owned temporary directories under `~/.cache/rift-devnet/tmp/`; those directories are removed when the devnet shuts down cleanly.

## Docker

The devnet-cli server is also available as a Docker image:

```bash
docker run -it -p 50101:50101 -p 50100:50100 riftresearch/devnet:latest server --fork-url <RPC_URL>
```
