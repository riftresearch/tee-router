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

## 1. Sauron provider-operation hints are observation-backed and idempotent

Severity: high. Status: resolved.

The recovered note referred to a removed `run_provider_operation_hint_loop`.
Current Sauron keeps a CDC-fed active provider-operation watch set, asks the
router/provider observe proxy for a concrete `ProviderOperationObservation`,
fingerprints that observation, and submits `PossibleProgress` only when the
fingerprint changes or the bounded resubmission interval expires.

The submitted hint now carries a deterministic idempotency key:

```rust
idempotency_key: Some(provider_operation_observation_hint_idempotency_key(
    operation_id,
    &fingerprint,
)),
```

Verification:

- `cargo test -p sauron provider_operation_observation --lib` passes.

## 2. Universal-router routes are covered by route-minimum policy

Severity: medium. Status: resolved.

`route_minimums.rs` now computes exact-out route floors through
`MarketOrderTransitionKind::UniversalRouterSwap`, including explicit input and
output decimals. `order_manager.rs` now treats an unsupported minimum for a
configured universal-router path as a hard quote error instead of silently
passing it.

Verification:

- `cargo test -p router-server route_minimum_service --test vault_creation`
  passes.
- `cargo test -p router-server quote_market_order_rejects_universal_router_route_without_minimum_policy --test vault_creation`
  passes.

## 3. Route costs and reimbursements still use static pricing forever

Severity: medium.

Status: resolved. Router-worker now refreshes route-cost pricing through the
`market-pricing` crate, using Coinbase unauthenticated spot prices and
configured EVM RPC `eth_gasPrice` calls. Paymaster reimbursement during quote
composition uses the current `RouteCostService` pricing snapshot when available.

## 4. Custody-backed execution persists receipt checkpoints

Severity: medium. Status: resolved.

`prepare_provider_completion` now writes a provider receipt checkpoint
immediately after each custody receipt is returned. Balance checks,
`post_execute`, and output-minimum enforcement happen after durable receipt
evidence exists. The worker has a crash injection point at
`AfterProviderReceiptPersisted`.

Verification:

- `cargo test -p router-server provider_receipt_checkpoint_forces_submitted_status_and_keeps_receipt --lib`
  passes.
- `cargo test -p router-server worker_restart_after_provider_receipt_checkpoint_recovers_waiting_step --test vault_creation`
  passes.

## 5. Hyperliquid generic chain operations return typed errors

Severity: medium-low. Status: resolved.

`crates/chains/src/hyperliquid.rs` returns explicit unsupported-operation errors
for generic block/status/sweep methods instead of panicking.

## 6. Universal-router asset and Velora descriptor hygiene

Severity: low. Status: resolved.

`velora_quote_descriptor` now accepts a named `VeloraQuoteDescriptorSpec`, and
`UniversalRouterAssetRef::deposit_asset` normalizes EVM token identities before
returning the `DepositAsset`.

Verification:

- `cargo test -p router-server universal_router_asset_ref --lib` passes.

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

## 392. Limit-order idempotency had weaker race coverage than market orders

Severity: medium. Resolved in this branch.

The global `router_orders(idempotency_key)` unique index was intentionally
removed so public client keys are scoped to a specific quote instead of
globally blocking reuse across independent quotes. Market orders already had
coverage for this model, including a same-quote concurrent create race.
Limit orders use the same quote-row serialization pattern, but did not have the
same regression coverage.

Resolution:

- Added a limit-order test proving the same idempotency key may create orders
  for two different quotes.
- Added a 16-way concurrent create test proving same-quote limit-order retries
  all resume the single linked order instead of creating duplicates or
  surfacing false conflicts.

Verification:

- `cargo test -p router-server limit_order_idempotency --test vault_creation`
  passes.
- `cargo test -p router-server concurrent_create --test vault_creation`
  passes.
