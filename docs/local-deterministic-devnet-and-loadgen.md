# Local deterministic devnet and router load generation

This is the working plan for the local admin-dashboard/router development loop.
It captures the requirements from the admin dashboard and local backend
discussion, then narrows them into implementation pieces.

## Goal

Run the full router backend locally with deterministic infrastructure, no real
money, and no real third-party APIs:

- router primary database
- router API
- router worker
- router read replica
- Sauron
- router gateway
- admin dashboard
- Ethereum, Base, Arbitrum, and Bitcoin local chains
- local provider mocks for Across, CCTP, HyperUnit, Hyperliquid, Velora, and
  pricing surfaces
- a Rust load-generation CLI that creates quotes, creates orders, and funds
  orders through the public router gateway

The normal developer entrypoint should be:

```sh
docker compose -f etc/compose.local-full.yml up
docker compose -f etc/compose.local-full.yml down
```

## Requirements

- The devnet binary, when run directly as a CLI, should expose deterministic
  ports, chain IDs, token addresses, provider mock URLs, and funded local
  accounts.
- There should not be a `devnet scenario ...` command. Devnet owns local chain
  and mock-provider infrastructure, not router traffic generation.
- The traffic driver should be a separate Rust CLI, currently named
  `router-loadgen`.
- `router-loadgen` should exercise the router through the public router gateway,
  not through the internal router API.
- Switching between local devnet, staging, and production-like environments
  should require only URL/private-key/config changes.
- The load generator should support quote-only, order-only, create-and-fund,
  rate-limited, and concurrent order creation flows.
- Funding behavior should be configurable so the dashboard can show
  `pending_funding`, funded/executing, completed, manual intervention, refund,
  expired, and delayed states.
- Forced provider outcomes belong in provider mock configuration/state, not in
  hidden devnet scenario commands.
- The admin dashboard should read the replica and display router orders sorted
  by `created_at DESC`, not latest activity.
- The local stack should be able to produce dashboard-visible orders without
  spending real money or hitting live APIs.

## Separation of responsibility

Devnet:

- starts local EVM chains and Bitcoin regtest
- deploys deterministic mock contracts/tokens
- starts local provider mocks
- exposes deterministic RPC/API ports
- exposes a manifest describing the local environment
- funds deterministic development accounts

Router services:

- run as normal services against local URLs
- use the same public/internal HTTP contracts as deployed environments
- run database migrations on startup where the service already owns migrations

Router load generator:

- calls `POST /quote` on the router gateway
- calls `POST /order/market` on the router gateway
- funds the returned `orderAddress`
- controls volume, concurrency, amount selection, funding behavior, and request
  pacing
- can run against local or remote gateways by changing config

Provider mocks:

- return deterministic quotes and mock provider responses
- expose knobs for latency, refund/failure probability, and balances
- observe local devnet contracts where appropriate, so execution still follows
  real router code paths

## Deterministic local ports

The interactive devnet should reserve the following ports:

| Surface | Host port |
| --- | ---: |
| Bitcoin RPC | 50100 |
| Ethereum Anvil RPC/WS | 50101 |
| Base Anvil RPC/WS | 50102 |
| Arbitrum Anvil RPC/WS | 50103 |
| Ethereum token indexer | 50104 |
| Base token indexer | 50105 |
| Arbitrum token indexer | 50106 |
| Provider mock API | 50107 |
| Devnet manifest API | 50108 |
| Bitcoin Esplora API | 50110 |
| Bitcoin ZMQ rawtx | 50111 |
| Bitcoin ZMQ sequence | 50112 |

## Local compose shape

`etc/compose.local-full.yml` should own process lifecycle:

- `devnet`
- `router-postgres`
- `router-postgres-replica`
- `router-api`
- `router-worker`
- `sauron-state-db`
- `sauron`
- `router-gateway-db`
- `router-gateway`
- `admin-dashboard-auth-db`
- `admin-dashboard`
- optional `router-loadgen` tool profile

The compose file should configure router services with local URLs:

- `ETH_RPC_URL=http://devnet:50101`
- `BASE_RPC_URL=http://devnet:50102`
- `ARBITRUM_RPC_URL=http://devnet:50103`
- `BITCOIN_RPC_URL=http://devnet:50100`
- `ELECTRUM_HTTP_SERVER_URL=http://devnet:50110`
- provider API URLs pointed at `http://devnet:50107`
- replica/admin URLs pointed at the local logical replica

## Load generator initial command shape

```sh
router-loadgen quote \
  --gateway-url http://localhost:3001 \
  --from Base.USDC \
  --to Ethereum.USDC \
  --from-amount 10000000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111

router-loadgen create-and-fund \
  --gateway-url http://localhost:3001 \
  --from Base.USDC \
  --to Ethereum.USDC \
  --from-amount 10000000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111 \
  --count 25 \
  --rps 5 \
  --evm-rpc evm:8453=http://localhost:50102 \
  --devnet-manifest-url http://localhost:50108/manifest.json
```

For compose-internal execution, the same command should swap only URLs:

```sh
router-loadgen create-and-fund \
  --gateway-url http://router-gateway:3000 \
  --evm-rpc evm:8453=http://devnet:50102 \
  ...
```

For mixed local traffic, use random mode. It picks a random EVM USDC source
chain, a distinct random EVM USDC destination chain, and a raw exact-input
amount within bounds:

```sh
router-loadgen create-and-fund \
  --gateway-url http://router-gateway:3000 \
  --random \
  --random-min-raw-amount 100000 \
  --random-max-raw-amount 10000000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111 \
  --count 100 \
  --concurrency 8 \
  --rps 5 \
  --evm-rpc evm:1=http://devnet:50101 \
  --evm-rpc evm:8453=http://devnet:50102 \
  --evm-rpc evm:42161=http://devnet:50103 \
  --devnet-manifest-url http://devnet:50108/manifest.json
```

## Near-term implementation notes

- The first working loadgen targets are EVM ERC-20/native funding and Bitcoin
  Core wallet-RPC funding. A WIF/descriptor-backed Bitcoin funder can follow if
  we need the same private-key-only mode for Bitcoin that EVM already has.
- The direct devnet server allocates a deterministic local-only EVM key pool for
  loadgen and exposes it in `accounts.loadgen_evm_accounts` in the manifest.
  Loadgen should consume that pool instead of the Anvil/paymaster key.
- Local state coverage starts with `pending_funding` and funded orders. Provider
  failure/delay coverage should be added by extending the provider mock state
  controls, not by adding devnet scenario commands.
- The devnet manifest is informational and tool-friendly. Services in compose
  should still receive explicit environment variables so startup does not depend
  on dynamic manifest fetches.
