# Venue Addition Guide

This is the exhaustive checklist for adding a venue to T Router. A venue is any
external execution surface that can move value, swap value, or create a
provider-visible operation inside a market-order route. Follow this list before
claiming the venue is integrated end to end.

The key distinction is:

- **New provider, existing transition kind:** add a provider implementation and
  capability rows, then reuse existing planner/executor semantics.
- **New transition kind:** add a new route edge, quote path, step materializer,
  executor branch, observation branch, refund branch, mocks, migrations, and
  tests.

## 1. Classify the Venue

Decide the venue model before touching code.

Required decisions:

1. Runtime provider id.
2. Venue kind:
   - `ProviderVenueKind::CrossChain`
   - `ProviderVenueKind::MonoChain { FixedPairExchange }`
   - `ProviderVenueKind::MonoChain { UniversalRouter }`
3. Asset support model:
   - `StaticDeclared`
   - `RuntimeEnumerated`
   - `OpenAddressQuote`
4. Transition semantics:
   - existing `MarketOrderTransitionKind`
   - or a new kind with new execution behavior
5. Synchronous or asynchronous settlement:
   - immediate custody receipt
   - provider operation requiring later observation
6. Custody model:
   - user-facing source vault
   - intermediate EVM/Bitcoin/Hyperliquid custody
   - provider-generated address
   - external recipient payout
7. Failure model:
   - provider quote rejection
   - chain transaction revert
   - async provider operation failure
   - partial fill / partial delivery
   - terminal unknown state

Document these decisions in the implementation PR or a provider-specific design
note. Do not let the first implementation hide them in tests.

## 2. Asset Registry And Route Graph

Touchpoints:

- `bin/router-server/src/services/asset_registry.rs`
- `bin/router-server/src/services/mod.rs`
- `bin/router-server/src/services/route_costs.rs`
- `bin/router-server/src/services/route_minimums.rs`
- `bin/router-server/src/services/gas_reimbursement.rs`

Required work:

1. Add the provider to `ProviderId`.
   - `parse`
   - `as_str`
   - `venue_kind`
   - `asset_support_model`

2. Add or reuse the appropriate `ProviderVenueKind`.
   - Cross-chain providers move assets across chains.
   - Monochain fixed-pair exchanges have a finite pair set.
   - Monochain universal routers can attempt arbitrary same-chain token routes.

3. Add provider capability rows in `builtin_provider_assets` when the venue has
   static or enumerated assets.
   - Use the canonical asset identity.
   - Use the router `ChainId`.
   - Use the provider's chain/domain identifier in `provider_chain`.
   - Use the provider's token/symbol/address identifier in `provider_asset`.
   - Set decimals from the provider settlement unit.
   - Set the exact `ProviderAssetCapability` values.

4. Add chain-local asset rows in `builtin_chain_assets` only if the venue makes
   a new external asset representation routable.

5. If the venue should use dynamic same-chain route discovery, add runtime
   transition generation similar to Velora's runtime transition declarations.
   Do not add arbitrary dynamic routes for venues that cannot quote arbitrary
   addresses safely.

6. If the venue introduces a new transition kind, update:
   - `MarketOrderTransitionKind`
   - `MarketOrderTransitionKind::as_str`
   - `MarketOrderTransitionKind::route_edge_kind`
   - `RouteEdgeKind` if the existing edge classes are insufficient
   - `required_roles_for_transition_kind`
   - `AssetRegistry::market_order_transitions`
   - `AssetRegistry::transition_decl_from_edge` if the node mapping changes

7. If the venue introduces a new venue node, update:
   - `MarketOrderNode`
   - `AssetRegistry::asset_for_node`
   - path-id expectations in tests

8. Update route ranking and cached route costs.
   - Add structural estimate handling in `route_costs.rs`.
   - Add refresh sampling if this venue should be part of the anchor/core graph.
   - Map transition kind to provider id in route-cost refresh code.
   - Verify stale/missing snapshots degrade to structural costs.

9. Update route minimums.
   - Make sure the venue either contributes a real minimum or is explicitly
     skipped with a justified default.
   - Add handling for exact-in and exact-out when the venue supports both.

10. Update gas reimbursement if the venue creates chain actions paid by the
    router paymaster.
    - add or verify transition gas estimate
    - mark native-input exceptions
    - ensure retention can happen at an eligible custody site
    - verify refund paths do not accidentally retain forward-route fees

Completion criteria:

- The route graph can discover a path that includes the venue.
- Route-cost ranking sees the venue.
- Route minimum logic does not panic or silently understate minimums.
- Gas reimbursement is correct for every chain action the venue requires.

## 3. Public Models, Database, And Migrations

Touchpoints:

- `bin/router-server/src/models.rs`
- `bin/router-server/src/db/order_repo.rs`
- `crates/router-core/migrations/*.sql`
- `docs/market-order-action-spec.md`

Required work:

1. If adding a new execution step, add `OrderExecutionStepType`.
   - serializer/deserializer string
   - `to_db_string`
   - `from_db_string`
   - tests for db string round trip

2. If adding a new provider operation, add `ProviderOperationType`.
   - serializer/deserializer string
   - `to_db_string`
   - `from_db_string`
   - Sauron compatibility through shared router-server model parsing

3. Add a migration when DB check constraints need the new step or operation
   value.
   - `order_execution_steps.step_type`
   - `order_provider_operations.operation_type`
   - any new relational columns required by the provider

4. Keep provider-specific request/response data in JSON only when the data is
   not used for routing, idempotency, recovery, or operator queries. Promote it
   to relational columns if recovery needs to query it.

5. Update repository read/write methods if new relational columns or typed
   models are added.

6. Update API documentation examples if the venue changes quote/order request
   semantics.

Completion criteria:

- A fresh database can migrate.
- Persisted steps and provider operations round-trip.
- Crash recovery can find all data needed to resume or refund.

## 4. Provider Adapter

Touchpoints:

- `bin/router-server/src/services/action_providers.rs`
- `bin/router-server/src/services/across_client.rs` if adding an Across-like
  client helper
- provider-specific client crates, if the API is large enough to isolate
- `bin/router-server/src/app.rs`
- `bin/router-server/src/lib.rs`

Required work:

1. Pick the correct provider trait:
   - `BridgeProvider`
   - `ExchangeProvider`
   - `UnitProvider`
   - or add a new trait only if the existing traits cannot express the venue

2. Implement quote behavior.
   - exact-in support
   - exact-out support or explicit unsupported error
   - slippage-bound propagation
   - provider fees
   - provider minimums
   - expiry
   - deterministic provider quote descriptor
   - explicit amount-format contract for every provider field:
     - router quote/order/leg amounts must be raw base-unit integer strings
     - provider action payloads may use human decimal strings only when the
       provider API requires them
     - raw and human decimal fields must not reuse the same JSON key name

3. Implement execution intent generation.
   - `CustodyAction`
   - `CustodyActions`
   - `ProviderOnly`
   - provider address intent
   - provider operation intent
   - idempotency key
   - request/response JSON shape for recovery

4. Implement observation when the venue is async.
   - provider operation status lookup
   - terminal success
   - terminal failure
   - retryable unknown/pending
   - amount/output evidence when available

5. Add provider config.
   - CLI/env fields in `RouterServerArgs`
   - validation in `initialize_action_providers`
   - default behavior when the provider is not configured
   - API key/auth/partner/integrator fields
   - network selector if mainnet/testnet signatures differ

6. Wire the provider into `ActionProviderRegistry`.
   - constructor
   - `mock_http` or devnet constructor
   - provider lookup by trait family
   - `is_empty` behavior

7. Add unit tests for:
   - quote parsing
   - execution intent shape
   - provider operation observation
   - exact-in/exact-out math
   - raw base-unit versus human decimal amount conversion
   - unsupported route errors
   - auth/header/query construction

Completion criteria:

- The router can quote through the provider without special test-only branches.
- Execution intent contains enough information to submit, observe, recover, and
  refund.
- Provider config can be omitted without breaking unrelated venues.

## 5. Quote, Order, And Slippage Flow

Touchpoints:

- `bin/router-server/src/services/order_manager.rs`
- `bin/router-server/src/services/quote_legs.rs`
- `bin/router-server/tests/vault_creation.rs`
- `bin/router-server/tests/live_quote_probes.rs`

Required work:

1. Add path-quote composition for the transition kind.
   - quote leg input/output
   - intermediate min/max propagation
   - final slippage-derived hard bound
   - exact-in and exact-out behavior

2. Ensure quote filtering skips providers blocked by provider policy.

3. Ensure quote expiry is populated and validated.

4. Ensure provider quote JSON stores:
   - path id
   - transition ids
   - leg descriptors
   - provider request/response fragments needed later
   - gas reimbursement plan
   - retention actions if any

5. Verify quote amount formats.
   - Top-level `amount_in`, `amount_out`, `min_amount_out`, and
     `max_amount_in` must be raw base-unit integer strings.
   - Every `provider_quote.legs[*]` amount field must be a raw base-unit
     integer string.
   - If a provider requires human decimal strings for execution, store those
     only in provider-specific request/action fields with explicit names such
     as `amount_decimal`.
   - Add tests that fail on decimal points, exponents, commas, whitespace, or
     other non-raw formats in router-visible amount fields.

6. Make sure arbitrary-asset entry/exit behavior is explicit.
   - Universal routers should only appear on the edge unless deliberately
     allowed in the core graph.
   - Fixed-pair venues should never guess unsupported tokens.

Completion criteria:

- A quote created through the venue can be converted into an order.
- The order does not need to rediscover the route template.
- Slippage is enforced as a provider-executable hard bound.

## 6. Planner Materialization

Touchpoints:

- `bin/router-server/src/services/market_order_planner.rs`
- `bin/router-server/src/services/order_executor.rs` refund materialization
  helpers

Required work:

1. Materialize the transition into one or more `order_execution_steps`.
   - Some transitions produce one step.
   - CCTP-like flows may produce burn and receive steps.
   - Async provider flows should persist a provider operation.

2. Set typed step columns.
   - `step_type`
   - `provider`
   - input chain/asset
   - output chain/asset
   - amount fields
   - transition declaration id

3. Store request JSON with all provider execution fields needed later.

4. Store custody-vault references and provider-address references in
   `details_json` only after validating the roles.

5. Enforce intermediate custody invariants.
   - Non-final outputs must land in Rift-controlled custody.
   - External recipient payouts can only happen on final payout steps.
   - Provider-generated addresses must be watch-only/provider-address rows, not
     generic custody vaults, unless the router controls the key.

6. Add the same materialization logic for refund attempts if the transition can
   appear on a refund route.

Completion criteria:

- Materialized plans are deterministic from the persisted quote.
- Planner tests verify step order, roles, and request JSON.
- Refund materialization either supports the transition or explicitly blocks it.

## 7. Execution, Observation, Recovery, And Refunds

Touchpoints:

- `bin/router-server/src/services/order_executor.rs`
- `bin/router-server/src/services/custody_action_executor.rs`
- `bin/router-server/src/services/vault_manager.rs`
- `bin/sauron/src/provider_operations.rs`
- `bin/sauron/src/runtime.rs`

Required work:

1. Add execution dispatch for the step type.
   - quote/request JSON hydration
   - balance precheck
   - custody action execution
   - provider-only operation submission
   - provider operation row creation/update

2. Add provider-operation verifier behavior.
   - Which component validates a hint?
   - Which provider API or on-chain balance is queried?
   - What evidence marks success or failure?

3. Add provider operation recovery.
   - operation persisted before step status update
   - terminal operation persisted before final step settlement
   - worker restart while operation is pending

4. Add failure handling.
   - transaction reverted
   - provider operation failed
   - provider status unknown
   - provider venue quote expired before submission
   - insufficient source/intermediate balance
   - partial fill/delivery

5. Add refund routing support.
   - stranded position detection
   - refund quote composition
   - refund step materialization
   - non-recursive refund failure behavior

6. Add paymaster/retention execution if the venue spends gas on a chain.

7. Add release/sweep handling for any new internal custody role or chain.

8. Verify provider policy enforcement.
   - blocked during quote
   - blocked during planning
   - blocked during execution
   - blocked during refund route selection

Completion criteria:

- A crash at every persisted boundary can resume or refund.
- Provider hints wake the worker, but the worker validates provider truth itself.
- Failed forward execution ends in retry or refund, not a silent stuck state.

## 8. Custody Actions And Chain Calls

Touchpoints:

- `bin/router-server/src/services/custody_action_executor.rs`
- `crates/chains/src/*`
- provider-specific signing/client crates

Required work:

1. If the venue needs an EVM transaction, return `ChainCall::Evm`.
   - target address
   - value
   - calldata
   - approval behavior, if needed
   - receipt/log extraction

2. If the venue needs a Hyperliquid action, return `ChainCall::Hyperliquid`.
   - network/source byte
   - account/vault address semantics
   - payload variant
   - response handle

3. If the venue needs a new chain-call family, add it explicitly.
   - `ChainCall` variant
   - signing/execution implementation
   - receipt model
   - tests

4. Verify router-derived custody identities are used. Shared provider execution
   keys are not acceptable for user order execution.

5. Verify transaction targets and recipients cannot be redirected by provider
   quote data.

Completion criteria:

- The custody executor can submit every required action using router-controlled
  keys.
- The provider cannot cause an action to spend from or pay to an unexpected
  address.

## 9. Sauron And Deposit/Provider Detection

Venue changes can require Sauron work if the venue creates a provider operation
that must be observed later or a provider deposit address that must be watched.

Touchpoints:

- `bin/sauron/src/provider_operations.rs`
- `bin/sauron/src/watch.rs`
- `bin/sauron/src/runtime.rs`
- `bin/sauron/src/discovery/*`
- `bin/router-server/src/api.rs` detector hint request/response types

Required work:

1. If the venue reuses provider-operation hints, verify the new
   `ProviderOperationType` parses from DB in Sauron.

2. If the venue creates a provider deposit address, ensure `WatchRepository`
   selects it.
   - source chain
   - source token
   - address
   - required amount
   - deadline
   - idempotency key/evidence fields

3. If the venue requires a new watch target, add it deliberately.
   - SQL query rows
   - `WatchTarget`
   - detector hint target
   - router API handling

4. If the venue requires chain-specific detection beyond existing EVM/Bitcoin
   backends, add or extend a `DiscoveryBackend`.

5. Add startup and outage backfill behavior.
   - initial full reconcile
   - indexed lookup for active watches
   - cursor recovery
   - retention window assumptions

Completion criteria:

- Sauron can discover missed deposits/operation progress after restart.
- The router worker never polls provider/chain state on a cadence for this
  venue except when validating a received hint or recovering persisted work.

## 10. Devnet Mock And Provider-Mock Parity

Touchpoints:

- `crates/devnet/src/mock_integrators.rs`
- `crates/devnet/src/*_mock`
- `crates/devnet/tests/*`
- `docs/provider-mock-parity.md`

Required work:

1. Create a local mock service or devnet module for the exact external API
   surface consumed by the provider adapter.

2. If the provider returns transaction calldata, the mock must return calldata
   that can execute against local devnet contracts.

3. If worker progression depends on settlement, the mock must materialize local
   settlement state.
   - ERC-20 balance changes
   - native balance changes
   - provider ledger balance changes
   - operation status transitions

4. If the provider uses on-chain contracts, include mock contracts and stable
   deployment addresses.

5. Document exactly what is covered and what is not in
   `docs/provider-mock-parity.md`.

6. Keep mock behavior provider-shaped, not router-shaped. The mock should help
   prove the adapter boundary, not bypass it.

Completion criteria:

- Local integration tests can exercise the provider path without real funds.
- The provider-mock parity doc tells future engineers where the mock diverges
  from production.

## 11. Live Differential Tests

Touchpoints:

- provider/client crate live differential tests
- `bin/router-server/tests/live_*_provider_differential.rs`
- `bin/router-server/tests/live_quote_probes.rs`
- provider-specific ignored tests
- `docs/live-provider-differential-tests.md`
- `docs/provider-mock-parity.md`

Required work:

1. Add a read-only live differential test when possible.
   - use one provider-specific test target, named
     `live_<venue>_provider_differential.rs`
   - live provider call and local mock provider call from the same normalized
     request
   - quote request
   - provider response transcript
   - parsed normalized quote
   - mock-vs-live contract comparison for required fields, optional fields,
     timestamps, token identity, calldata/action shape, and operation status
   - amount-format assertions proving which live fields are raw base-unit
     strings and which provider-only fields are human decimal strings

2. Add a real-funds live differential test when the venue submits transactions.
   - keep it separate from the read-only differential target
   - explicit env gates
   - explicit spend confirmation env var
   - small bounded notional
   - transcript logging
   - final balance/status verification
   - provider operation amount-format assertions when operation status includes
     amount fields

3. Verify the mock was built from observed real provider shapes.
   - A mock-only integration test is not sufficient. The read-only live layer
     must fail when the mock omits a live field that the router treats as part
     of the contract.

4. Keep brittle market values out of assertions. Assert shape, lifecycle, and
   invariants.

Completion criteria:

- There is a provider transcript proving the adapter's assumptions.
- Spending tests cannot run accidentally.

## 12. Observability And Operations

Touchpoints:

- provider policy admin API
- `bin/router-server/src/telemetry.rs`
- service logs and metrics labels
- deployment env docs/scripts

Required work:

1. Provider id must be usable with provider policy endpoints.

2. Logs must include provider id, operation id, step id, and order id at
   important boundaries.

3. Metrics should use the provider id as a label when provider-specific.

4. Config must document required env vars, optional auth fields, and safe
   disabled behavior.

5. Operator runbooks should know how to:
   - disable quotes
   - drain execution
   - resume execution
   - identify stuck provider operations
   - identify manual refund cases

Completion criteria:

- Operators can disable the venue without redeploying.
- A failed live operation can be diagnosed from DB rows and logs.

## 13. Documentation Updates

Required docs:

1. `docs/market-order-system-design.md`
   - add the venue to current alignment
   - mention any new transition kind

2. `docs/provider-mock-parity.md`
   - covered mock surface
   - semantic gaps
   - live differential test location

3. `docs/market-order-action-spec.md`
   - update API or persisted quote examples if the venue changes shape

4. Provider-specific doc if the venue has unusual lifecycle, custody, or
   finality semantics.

Completion criteria:

- A new engineer can understand how the venue fits without reading the whole
  implementation first.

## 14. Test Matrix

Minimum test set:

1. `AssetRegistry` transition/path test.
2. Route-cost ranking test.
3. Route-minimum test.
4. Quote test for exact-in.
5. Quote test for exact-out or explicit unsupported exact-out.
6. Quote expiry/slippage hard-bound test when the venue participates in
   slippage.
7. Amount-format tests proving router-visible amounts are raw base-unit integer
   strings and provider-only human decimals are isolated behind explicit field
   names.
8. Planner materialization test.
9. Custody role validation test.
10. Provider policy block test.
11. Mock provider quote/execute/observe unit tests.
12. Local integration test that completes a forward route through the venue.
13. Failure test that sends the order to retry or refund.
14. Refund route test if the venue can be part of refund routing.
15. Worker restart recovery test for async operations.
16. Sauron hint/detection test when Sauron is involved.
17. Live differential test.

Before merge, run at least:

- `cargo check --workspace --tests`
- `cargo test -p router-server --test vault_creation`
- affected provider/client crate tests
- affected Sauron tests
- the ignored live differential test with the documented env gates

## Current Venue Alignment

| Runtime id | Venue model | Asset support | Transition kind | Adapter trait | Required integration state |
| --- | --- | --- | --- | --- | --- |
| `across` | cross-chain bridge | runtime-enumerated allowlist | `AcrossBridge` | `BridgeProvider` | provider id, capability rows, policy id, HTTP adapter, local mock, planner/executor branches, provider operation observation, route cost/ranking, mock integration tests, live transcript tests |
| `cctp` | cross-chain USDC burn/mint bridge | static declared USDC domains | `CctpBridge` | `BridgeProvider` | provider id, capability rows, policy id, HTTP/Iris adapter, CCTP contract calldata, local contracts/mock Iris, burn/receive materialization, route cost/ranking, mock tests, live differential tests |
| `unit` | cross-chain deposit/withdraw venue | static declared assets | `UnitDeposit`, `UnitWithdrawal` | `UnitProvider` | provider id, capability rows, policy id, HTTP adapter, provider-address watches, Sauron discovery, async observation, mock `/gen` + `/operations`, execution/recovery/refund tests, live differential tests |
| `hyperliquid_bridge` | cross-chain bridge into Hyperliquid | static declared USDC ingress | `HyperliquidBridgeDeposit` | `BridgeProvider` | provider id, capability row, policy id, bridge contract address, Hyperliquid balance observation, mock Bridge2/indexer, route cost/ranking, planner/executor tests, live bridge tests |
| `hyperliquid` | mono-chain fixed-pair exchange | static declared spot assets | `HyperliquidTrade` | `ExchangeProvider` | provider id, capability rows, policy id, HTTP adapter, Hyperliquid custody call execution, timeout/cancel recovery, mock `/info` + `/exchange`, execution/refund tests, live spot/order/cancel tests |
| `velora` | mono-chain universal router | open-address quote check | `UniversalRouterSwap` | `ExchangeProvider` | provider id, venue taxonomy, policy id, runtime edge generation, HTTP adapter, calldata custody action, mock `/prices` + `/transactions/:network`, route edge tests, arbitrary-token integration tests, live differential tests |

Review rule: if a venue appears in `ProviderId`, it must appear in this table
and in `docs/provider-mock-parity.md`. If it submits or causes movement of real
funds, it must have a live differential test before the mock is considered
trusted.
