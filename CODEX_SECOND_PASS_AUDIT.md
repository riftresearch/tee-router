# Second-Pass Audit Findings

Generated after the first audit action list was completed. The previous
workspace validation remained green at that point:

- `cargo check --workspace --all-targets --locked --offline`
- `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`
- `cargo nextest run --workspace --all-targets --locked --offline`

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

The worker then processes those hints by calling the provider observer:

- Across: `observe_bridge_operation`, which calls Across deposit status
- Unit/HyperUnit: `observe_unit_operation`
- Hyperliquid: `observe_trade_operation`, which calls Hyperliquid order status

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

`PricingSnapshot::static_bootstrap` hard-codes ETH, BTC, stablecoin, and gas
prices. `RouteCostService::new` captures one static snapshot at service
construction with `expires_at: None`. Paymaster reimbursement also creates a
fresh static bootstrap snapshot when no explicit pricing is injected.

Impact:

- Route ranking can drift from real fees and market prices.
- Paymaster reimbursement can undercharge or overcharge as gas/asset prices move.
- The code now has freshness metadata, but no production freshness enforcement.

Recommended fix:

- Make router-worker own a pricing oracle/cache.
- Attach TTLs and source metadata to snapshots.
- Refuse route-cost refresh and reimbursement planning when pricing is stale
  beyond the configured alpha tolerance.

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
