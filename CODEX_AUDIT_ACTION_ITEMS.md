# Final Abstraction Audit Action Items

## 1. Replace internal JSON execution contracts with typed contracts

Status: complete. First slice completed: provider execution trait methods now
receive typed request structs/enums after executor-side hydration from persisted
JSON.

Second slice completed: quote legs now use a typed `QuoteLeg` model between
quote composition and route materialization, with provider-specific payloads kept
under typed-leg `raw` JSON.

Third slice completed: refund route quote/materialization now reuses the typed
`QuoteLeg` model instead of carrying raw leg JSON through refund planning.

The planner, executor, and provider adapters currently pass `serde_json::Value`
through core internal APIs. JSON should remain acceptable at persistence and API
boundaries, but executable router logic should hydrate into typed request/state
objects before dispatch.

Target areas:
- `bin/router-server/src/services/action_providers.rs`
- `bin/router-server/src/services/order_manager.rs`
- `bin/router-server/src/services/market_order_planner.rs`
- `bin/router-server/src/services/order_executor.rs`
- `bin/router-server/src/models.rs`

Desired shape:
- typed quote-leg and step-request structs/enums
- serialization only at DB/API boundaries
- provider trait methods accepting typed requests where practical
- fewer ad hoc `Value::get(...)` helper paths

## 2. Normalize and type arbitrary EVM asset identity

Status: complete. First slice completed: EVM asset address normalization now
lives on the protocol asset types and is reused by order quote validation, vault
creation, and quote-leg hydration.

Second slice completed: the shared EVM address normalizer now lives in
`router-primitives`, and Sauron watch ingestion uses it for ERC-20 watch tokens.

`AssetId::Reference(String)` is too loose for arbitrary ERC-20 support. EVM token
addresses should be normalized and validated as addresses, ideally with an
explicit typed representation.

Target areas:
- `bin/router-server/src/protocol.rs`
- asset registry dynamic asset handling
- Velora request/quote paths
- Sauron/token-indexer watch ingestion

Desired shape:
- canonical token-address representation per EVM chain
- no casing-based duplicate asset identities
- parsing errors before quote/execution code

## 3. Unify route-cost and gas-reimbursement pricing inputs

Status: complete. Route-cost refresh and paymaster reimbursement now share a
`PricingSnapshot` service primitive for USD micro prices, bps math, and USD/raw
amount conversions. Router-worker route-cost refresh updates that snapshot via
the `market-pricing` crate, which pulls Coinbase unauthenticated spot prices and
EVM `eth_gasPrice` values from the configured RPCs. Route-cost refresh summaries
carry the pricing source used for the run.

Target areas:
- `bin/router-server/src/services/route_costs.rs`
- `bin/router-server/src/services/gas_reimbursement.rs`
- worker route-cost refresh setup

Desired shape:
- shared pricing snapshot plus injected pricing oracle
- one representation for USD micro amounts
- explicit freshness/source metadata

## 4. Remove or rename stale route reliability metadata

Status: complete. `TransitionMetadata` and public `path_metadata` quote output
were removed. Quote tie-breaking now uses hop count after amount comparison
instead of stale reliability/latency priors.

`TransitionMetadata` still exposes reliability/latency priors while actual route
ranking now uses route-cost snapshots. The old metadata should not look like a
live quality signal.

Target areas:
- `bin/router-server/src/services/asset_registry.rs`
- quote response assembly in order manager

Desired shape:
- remove from public quote metadata, or
- rename to structural fallback metadata if retained

## 5. Keep provider identity typed inside router logic

Status: complete. `QuoteLeg` now carries `ProviderId`, and both market-order
and refund materializers validate typed quote-leg providers against typed
transition providers before serializing provider IDs onto execution steps.

The registry uses `ProviderId`, but downstream models and services frequently
revert to raw provider strings. Strings are fine at database boundaries; internal
logic should stay typed.

Target areas:
- `bin/router-server/src/models.rs`
- `bin/router-server/src/services/order_manager.rs`
- `bin/router-server/src/services/market_order_planner.rs`
- `bin/router-server/src/services/order_executor.rs`

Desired shape:
- typed provider identity through planning/execution
- conversion at persistence/API edges
- fewer raw string comparisons

## 6. Replace large argument lists with request/spec structs

Status: complete. Quote/refund/route-minimum helper call sites now use named
spec structs for broad materialization inputs, including live-test recovery
snapshot helpers.

Several route/refund planning helpers have broad positional argument lists.
These should be converted to focused request/spec structs.

Target areas:
- `bin/router-server/src/services/market_order_planner.rs`
- `bin/router-server/src/services/order_executor.rs`
- `bin/router-server/src/services/order_manager.rs`

Desired shape:
- named spec structs for step materialization
- smaller function signatures
- fewer `#[allow(clippy::too_many_arguments)]`

## 7. Name Sauron indexed lookup task results

Status: complete. Indexed lookup JoinSet tasks now return a named
`IndexedLookupTaskResult` with explicit watch/version/result fields.

Sauron indexed lookup plumbing uses a complex tuple type in `JoinSet`. Replace it
with a named struct.

Target areas:
- `bin/sauron/src/discovery/mod.rs`

Desired shape:
- `IndexedLookupTaskResult` or equivalent named type
- clearer field names at spawn/drain sites

## 8. Make Clippy warning-clean

Status: complete. `cargo clippy --workspace --all-targets --locked --offline
-- -D warnings` passes.

Plain Clippy succeeds but emits warnings; `-D warnings` fails. Clean these up so
future warnings surface as real failures.

Target areas:
- `crates/otc-models/src/constants.rs`
- `crates/eip3009-erc20-contract/src/lib.rs`
- `crates/devnet/src/across_spoke_pool_mock/mod.rs`
- `crates/blockchain-utils/src/bitcoin_wallet.rs`
- `bin/router-server/src/**`
- `bin/sauron/src/**`

Desired shape:
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
  passes
- generated-code lint allows are local and intentional
- no avoidable hygiene warnings remain
