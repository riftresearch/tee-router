# tee-router
Cross-chain vault router infrastructure intended to run inside a TEE.

## Components

- `router-api` - Stateless deposit-vault HTTP API. It validates requests, creates vault rows, accepts cancellation/refund requests, and returns status handles.
- `router-worker` - Stateful background processor. Exactly one worker should hold the PostgreSQL leadership lease and process refunds/actions at a time.
- Bitcoin full node for Bitcoin state validation
- EVM RPC endpoints for supported `evm:<chain_id>` networks

The target production architecture is tracked in `docs/router-system-architecture.md`.

## Prerequisites

- [Docker](https://www.docker.com/get-started/)
- [Rust](https://www.rust-lang.org/tools/install)
- [Foundry](https://getfoundry.sh/introduction/installation/)
- [cargo-nextest](https://nexte.st/docs/installation/pre-built-binaries/)
- [pnpm](https://pnpm.io/installation)

## Tests

Run the router workspace tests with:
```bash
cargo nextest run -p router-server
```

For `cargo nextest`, the `vault_creation` integration target uses a setup script to start one shared `postgres:18-alpine` container and export `ROUTER_TEST_DATABASE_URL`. Direct `cargo test` runs still start a Postgres testcontainer from inside the test harness. To use an existing Postgres server instead, set `ROUTER_TEST_DATABASE_URL` to an admin database URL; the harness creates an isolated database per test.

## EVM Vault Gas Sponsorship

Router-worker can top up EVM token deposit vaults with native gas before vault-signed actions such as ERC-20 refunds. Configure a funded sponsor key per chain with `ETHEREUM_PAYMASTER_PRIVATE_KEY` and/or `BASE_PAYMASTER_PRIVATE_KEY`, then set `EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI` to the extra native balance each token vault should keep after the estimated action gas. Vaults are not funded at creation time; top-ups happen only after a refundable token balance is detected. `EVM_PAYMASTER_VAULT_GAS_TARGET_WEI` is still accepted as a deprecated alias for the buffer setting.

## Runtime Split

Run the API node with:

```bash
cargo run -p router-server --bin router-api -- --config-dir ./config
```

Run the worker node with:

```bash
cargo run -p router-server --bin router-worker -- --config-dir ./config
```

`router-api` disables paymaster signing during chain setup. Refund side effects run only in `router-worker`. Worker leadership uses the `router_worker_leases` PostgreSQL table with `owner_id`, `expires_at`, and a monotonic `fencing_token`; tune it with `ROUTER_WORKER_ID`, `ROUTER_WORKER_LEASE_SECONDS`, `ROUTER_WORKER_LEASE_RENEW_SECONDS`, `ROUTER_WORKER_STANDBY_POLL_SECONDS`, and `ROUTER_WORKER_REFUND_POLL_SECONDS`.

## Action Providers

Market-order quoting and execution are composed from provider-specific adapters inside the router. Configure provider base URLs with `ACROSS_API_URL`, `HYPERUNIT_API_URL`, and `HYPERLIQUID_API_URL`.

The current HTTP adapters and mock integrators exercise the router provider contract. They are not yet sufficient to point directly at real money provider APIs. Track the live-money readiness checklist in `docs/live-real-money-readiness.md` before running a funded route.
