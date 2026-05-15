# Live Provider Differential Tests

Live provider tests are split into two layers:

- read-only live-vs-mock differentials for each router provider surface
- real-funds live lifecycle tests that submit transactions or place orders

The real-funds lifecycle tests are not all mock-vs-live differentials. They are
the spend layer that proves the real venue can be driven safely with dust-sized
amounts and explicit confirmation gates.

## Read-Only Live-Vs-Mock Differentials

These tests call a real provider and the local mock with the same normalized
request, then compare the contract that the router depends on. They must not
submit transactions or move funds.

Each successful run writes a `live_provider_differential` artifact under
`$ROUTER_LIVE_RECOVERY_DIR/provider-differentials`, or under a temp directory if
`ROUTER_LIVE_RECOVERY_DIR` is not set.

Run them with:

```bash
LIVE_PROVIDER_TESTS=1 cargo test -p router-server --test live_across_provider_differential -- --ignored --nocapture
LIVE_PROVIDER_TESTS=1 cargo test -p router-server --test live_cctp_provider_differential -- --ignored --nocapture
LIVE_PROVIDER_TESTS=1 cargo test -p router-server --test live_velora_provider_differential -- --ignored --nocapture
LIVE_PROVIDER_TESTS=1 cargo test -p router-server --test live_hyperliquid_provider_differential -- --ignored --nocapture
LIVE_PROVIDER_TESTS=1 cargo test -p router-server --test live_unit_provider_differential -- --ignored --nocapture
```

Current read-only coverage:

| Venue | Test target | Live contract checked |
| --- | --- | --- |
| Across | [`live_across_provider_differential`](../bin/router-server/tests/live_across_provider_differential.rs) | `/swap/approval` typed response, raw response, amount formats, token identity, quote expiry, swap transaction shape |
| CCTP | [`live_cctp_provider_differential`](../bin/router-server/tests/live_cctp_provider_differential.rs) | bridge quote fields, raw amount passthrough, Iris zero-hash not-found behavior |
| Velora | [`live_velora_provider_differential`](../bin/router-server/tests/live_velora_provider_differential.rs) | quote adapter output, raw `/prices` `priceRoute`, token identity, raw amount formats |
| Hyperliquid | [`live_hyperliquid_provider_differential`](../bin/router-server/tests/live_hyperliquid_provider_differential.rs) | spot quote adapter output, routed legs, raw amount formats, supported asset labels |
| Unit | [`live_unit_provider_differential`](../bin/router-server/tests/live_unit_provider_differential.rs) | `/gen` generated address shape and `/operations` schema/amount formats for supported Unit assets |

Each read-only test must assert router-visible amounts as raw base-unit integer
strings. Provider-only fields may be decimal-compatible strings only when the
real provider returns them that way and the field name is provider-specific.

## Real-Funds Live Lifecycle Tests

These tests are separate from read-only differentials and require an additional
spend-confirmation env var. They may submit transactions, place orders, or move
funds. They should use dust-sized notional, hard max input bounds, transcript
artifacts, expected source account checks, and final balance/status assertions.

Current real-funds lifecycle harnesses:

| Venue | Test target | Notes |
| --- | --- | --- |
| Across | [`live_across_client`](../bin/router-server/tests/live_across_client.rs) | Across transcript plus optional real deposit lifecycle |
| CCTP | [`live_cctp_client`](../bin/router-server/tests/live_cctp_client.rs) | CCTP quote/Iris transcript plus optional burn/receive lifecycle |
| Velora | [`live_velora_client`](../bin/router-server/tests/live_velora_client.rs) | Velora transcript plus optional swap lifecycle |
| Hyperliquid | [`crates/hyperliquid-client/tests/live_differential.rs`](../crates/hyperliquid-client/tests/live_differential.rs) | Hyperliquid read-only observer plus optional spot order, bridge deposit/withdrawal, transfer, cancel, and schedule-cancel lifecycles |
| Unit | [`crates/hyperunit-client/tests/live_differential.rs`](../crates/hyperunit-client/tests/live_differential.rs) | Unit read-only smoke plus optional Ethereum ETH deposit/withdraw and Bitcoin BTC withdrawal lifecycles |

There is no composed direct-provider market-order live E2E harness. The next
composed live-money test should drive the actual router API, worker, database,
funding vault, and provider adapters end to end.

## Adding A Venue

A new venue is not considered mock-parity complete until it has:

1. A provider-specific read-only live-vs-mock differential test target.
2. Amount-format assertions for every router-visible amount and every
   provider-only amount field consumed by the adapter.
3. A real-funds lifecycle test when the venue requires transaction submission or
   asynchronous settlement.
4. Documentation in [`docs/provider-mock-parity.md`](provider-mock-parity.md)
   describing exactly what the mock covers and where it intentionally diverges
   from production.
