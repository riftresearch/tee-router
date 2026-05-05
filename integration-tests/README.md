# Router Integration Tests

This crate holds TEOTC-style router runtime tests. These tests intentionally sit
outside `router-server` and `sauron` so they can start the real service
entrypoints together:

- local `RiftDevnet`
- router API via `router_server::server::run_api`
- router worker via `router_server::worker::run_worker`
- Sauron via `sauron::run`
- devnet mock provider services

The current runtime matrix covers exact-input market orders for:

- ETH -> Bitcoin
- Bitcoin -> ETH
- Bitcoin -> USDC
- ETH -> USDC, single chain
- USDC -> ETH, single chain
- ETH -> USDC, cross chain
- USDC -> ETH, cross chain

Each route creates a quote and order over the router HTTP API, funds the order
vault with real devnet wallet mechanics, lets Sauron and the router worker drive
the order, advances mock provider lifecycle hooks where needed, and asserts the
order reaches `completed`.

Run it with:

```sh
cargo test -p router-integration-tests --test router_runtime_e2e -- --nocapture
```

If Docker is unavailable, provide an existing Postgres admin database with:

```sh
ROUTER_TEST_DATABASE_URL=postgres://postgres:password@127.0.0.1:5432/postgres
```

Next coverage to add:

- exact-output variants of the current route matrix
- expected-failure cases that assert the specific terminal failure state
- route matrix coverage for CCTP-specific paths and additional known assets
