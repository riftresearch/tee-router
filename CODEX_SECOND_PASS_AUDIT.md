# Second-Pass Audit Findings

Generated after the first audit action list was completed. The previous
workspace validation remained green at that point:

- `cargo check --workspace --all-targets --locked --offline`
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
- `cargo nextest run --workspace --all-targets --locked --offline`

Recovery note: on 2026-05-05 the filesystem hit 100% usage while appending to
this file and the working-copy audit log was truncated. The committed baseline
above was restored, and the most recent recovered findings are listed below so
the current ticket remains documented.

## 1. Sauron still has a provider-operation polling loop

Severity: high.

`bin/sauron/src/runtime.rs` starts `run_provider_operation_hint_loop` with a
hard-coded 5 second interval. That loop snapshots every active provider
operation and submits a `PossibleProgress` provider-operation hint to the router
API.

The hint does not contain an idempotency key:

```rust
idempotency_key: None,
```

`order_provider_operation_hints` only deduplicates hints where
`idempotency_key IS NOT NULL`, so this loop can create a new hint row for every
watched provider operation every 5 seconds.

Impact:

- This reintroduces router-side provider polling by proxy.
- Provider/API load scales with active provider operations.
- Hint table growth also scales with operation count and time.
- This violates the intended model where Sauron should observe provider state
  itself and only notify the router after a real state transition or concrete
  evidence.

Recommended fix:

- Remove `run_provider_operation_hint_loop`.
- Move provider-state observation into Sauron/provider observer code that shares
  the same provider verification logic as the worker.
- Have Sauron submit hints only when it has observed concrete progress.
- Use deterministic idempotency keys for any resync/replay path.

## 2. Universal-router routes bypass the route-minimum gate

Severity: medium.

`route_minimums.rs` returns `RouteMinimumError::Unsupported` for
`MarketOrderTransitionKind::UniversalRouterSwap`, and `order_manager.rs` treats
that unsupported result as success during quote validation.

Impact:

- Arbitrary-token routes involving Velora skip the router-level minimum-size
  guard.
- Dust/unprofitable routes can proceed if the provider returns a quote.
- The system now depends on quote/provider behavior rather than a router policy
  floor for the arbitrary-asset path.

Recommended fix:

- Add quote-time minimum support for universal-router legs, or
- use conservative per-chain/per-anchor operational floors, or
- block unsupported minimum paths except for explicitly safe cases.

## 3. Route costs and reimbursements still use static pricing forever

Severity: medium.

Status: resolved. Router-worker now refreshes route-cost pricing through the
`market-pricing` crate, using Coinbase unauthenticated spot prices and
configured EVM RPC `eth_gasPrice` calls. Paymaster reimbursement during quote
composition uses the current `RouteCostService` pricing snapshot when available.

## 4. Custody-backed execution has a durability window after tx submission

Severity: medium.

In `prepare_provider_completion`, custody actions are submitted on-chain before
provider state is persisted. The code then captures balances, runs provider
`post_execute`, enforces output minimums, and only then calls
`persist_provider_state`.

Impact:

- A crash after transaction submission but before persistence can lose the tx
  hash/provider operation state.
- A fallible post-transaction balance check can mark the step failed even though
  the external transaction already committed.
- Existing crash injection points cover later state transitions, but not the
  immediate "receipt obtained but provider state not durably recorded" window.

Recommended fix:

- Persist submitted receipt/operation evidence immediately after every custody
  receipt is returned.
- Run balance/post-execute/minimum checks after durable evidence exists.
- Add a crash-injection test between receipt return and provider-state
  settlement.

## 5. Hyperliquid chain operations can still panic if called generically

Severity: medium-low.

`crates/chains/src/hyperliquid.rs` still uses `unimplemented!` for
`get_tx_status`, `dump_to_address`, `get_block_height`, and `get_best_hash`.
The router app registers `HyperliquidChain`, and generic router surfaces call
chain methods such as `get_best_hash`.

Impact:

- A generic chain-tip call for Hyperliquid can panic the request path.
- Any future generic refund/sweep/status path that accidentally uses
  `ChainOperations` for Hyperliquid will crash instead of returning a typed
  unsupported error.

Recommended fix:

- Replace `unimplemented!` with explicit typed errors for unsupported operations.
- Avoid registering Hyperliquid for generic chain endpoints that assume block
  semantics, or make those endpoints reject `hyperliquid` before dispatch.

## 6. Remaining small abstraction hygiene items

Severity: low.

- `velora_quote_descriptor` still has a non-generated
  `#[allow(clippy::too_many_arguments)]`; this should become a named request
  struct.
- `UniversalRouterAssetRef::deposit_asset` parses `ChainId`/`AssetId` but does
  not normalize the resulting `DepositAsset`. Other ingress paths normalize EVM
  token identities, so this boundary is inconsistent.

Recommended fix:

- Convert `velora_quote_descriptor` to a `VeloraQuoteDescriptorSpec`.
- Normalize in `UniversalRouterAssetRef::deposit_asset` before returning.

## 381-390. Recent Recovered Audit Items

The full working-copy text for items 7-390 was lost in the disk-full event
described above. The following recent items were recovered from the active
thread transcript and command output:

- 381. TypeScript backend raw error logging could leak secrets.
- 382. Devnet retained a dead credentialed Bitcoin RPC URL.
- 383. EVM token-indexer write API was unauthenticated.
- 384. EVM token-indexer API accepted unbounded write bodies and error text.
- 385. EVM token-indexer client request errors could render internal URLs.
- 386. Token-indexer transfer lookup counted and offset-paginated hot lookups.
- 387. Token-indexer client rendered oversized response bodies in errors.
- 388. Token-indexer unauthenticated escape hatch was not production-guarded.
- 389. Admin analytics backfill retried completed phases after later failures.
- 390. Market-pricing errors rendered oversized upstream text.

## 391. Router provider clients rendered oversized response bodies and URLs

Severity: medium for provider-boundary hardening. Resolved in this branch.

The router provider clients already had some bounded response reads, but Across
previously used unbounded `response.text()`, provider HTTP errors could still
render large bounded bodies directly into error strings, and request errors
could include full provider URLs. Across base URL construction also used an
assert/expect path rather than returning typed configuration errors.

Impact:

- Bad Across, CCTP, or Velora responses could produce very large router error
  strings and logs.
- Provider URLs with path material could leak through request error chains.
- Across client configuration failures were less controllable than the rest of
  the provider setup path.

Resolution:

- Added a shared UTF-8-safe 4 KiB provider response-body error preview helper.
- Applied it to Across HTTP status bodies, invalid JSON bodies, invalid numeric
  values, CCTP status/JSON errors, and Velora status/missing-field errors.
- Stripped URLs from provider request and streamed-body errors.
- Converted Across client construction to return typed errors, reject
  unsupported/credentialed/query/fragment base URLs, use a timeout, and read
  provider bodies through the bounded helper.

Verification:

- `cargo fmt -p router-server -- --check` passes.
- `cargo test -p router-server response_body_error_preview --lib` passes.
- `cargo test -p router-server parse_optional_u256_rejects_non_numeric --lib` passes.
- `cargo test -p router-server client_rejects_unsupported_base_urls --lib` passes.
- `cargo test -p router-server across_url_errors_redact_path_query_fragment_and_invalid_values --lib` passes.
- `cargo check -p router-server --lib` passes.
