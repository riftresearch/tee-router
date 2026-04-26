# Live Real-Money Readiness

This document tracks the remaining gap between the mocked router integration tests and a first dust-sized live-money route.

## Current State

- Router orders are quote-based.
- Funding vaults are created by the router, not by external providers.
- The worker has a funding detector that can move a vault from `pending_funding` to `funded` after observing enough on-chain balance.
- Market-order planning and execution are modeled as provider-specific steps.
- Mock Across, Unit, and Hyperliquid services are stateful and exercise the provider trait boundary.
- The ignored live harness in `bin/router-server/tests/live_market_order_e2e.rs` talks to real providers directly, but it is not yet a full router API plus router worker system test.

## Not Yet Ready For Real Funds

The current router provider HTTP adapters speak the local router provider contract used by the mock services. They are not real Across, Unit, or Hyperliquid API adapters yet.

The execution path also does not yet perform all required custody-side chain actions:

- Unit deposit execution can request or model a Unit deposit address, but the router does not yet sign and send the source vault funds to that Unit deposit address in the production execution path.
- Across bridge execution can model an Across step, but the router does not yet construct/sign/send the real Across transaction from the router-derived source vault.
- Hyperliquid execution is represented in mocks and in the direct live harness helper, but the production provider adapter is not yet wired to the real Hyperliquid SDK/API.
- EVM action gas funding is incomplete for live actions. The existing paymaster actor handles EVM token refund gas; it does not yet generally fund EVM vaults for provider actions such as Across deposits or native Unit deposits.
- Across-to-Unit quote/execution needs concrete recipient and refund custody addresses. The current quote lifecycle creates orders and funding vaults after quote acceptance, so live Across executable quote construction needs a clear pre-execution custody-address strategy.

## Required Before First Dust Run

1. Replace the current provider-contract HTTP adapters with real provider adapters, while keeping mocks that emulate those real request/response shapes.
2. Add a custody action executor that can derive router vault keys and sign/send provider-required chain transactions.
3. Extend paymaster logic from refund-only gas sponsorship to action gas sponsorship.
4. Resolve Across quote recipient/refund address lifecycle for Across-to-Unit routes.
5. Move the live real-money test through the router system:
   - boot `router-api` and `router-worker` or initialize the same components,
   - call `/api/v1/quotes`,
   - call `/api/v1/orders`,
   - fund the returned Rift vault with dust,
   - let the worker detect funding and execute the route,
   - assert final database status and real provider/on-chain outcomes.
6. Keep explicit spend guards:
   - throwaway keys only,
   - hard maximum input amount,
   - expected source address check,
   - explicit confirmation env var,
   - approval transactions disabled unless reviewed.

## Current Confidence

The mock-backed router state machine is in good shape. A full real-money router run is not ready until the provider adapters and custody transaction executor are implemented.
