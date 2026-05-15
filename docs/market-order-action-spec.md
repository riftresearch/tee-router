# Market Order Action Specification

## Status

Initial order/quote foundation is implemented. Provider adapters and worker
execution are still pending. Each provider adapter still needs to be checked
against the provider's current API documentation before implementation.

Provider-specific research for the Hyperliquid + Unit + Across path is captured
in `docs/hyperliquid-unit-across-market-order-plan.md`.

## Goals

- Add a market-order vault action for same-chain and cross-chain swaps.
- Support both exact-input and exact-output quote requests at the router API
  level.
- Aggregate quotes across Relay, NEAR Intents, Chainflip, THORChain, and Garden
  Finance.
- Exclude Li.Fi.
- Execute through the router worker, never the API process.
- Support BTC in v0, so the architecture must not be EVM-only.
- Do not compute, collect, or model router fees for this product.
- Refund automatically when an actionable vault cannot find or execute a valid
  route within the configured execution timeout.

## Non-Goals

- No fee logic.
- No provider-specific API commitments in this document.
- No API-level requirement that all providers support all order kinds.
- No fallback route that violates the user's original amount constraints.

## Action-Agnostic Order Storage

The primary database object should be a generic Rift order, not a market-order
row. Market orders, limit orders, and future earn products should all live behind
the same order envelope.

Canonical table:

```text
router_orders
```

Recommended common columns:

- `id`
- `order_type`
- `status`
- `funding_vault_id`
- `source_chain_id`
- `source_asset_id`
- `destination_chain_id`
- `destination_asset_id`
- `recipient_address`
- `action_timeout_at`
- `idempotency_key`
- `created_at`
- `updated_at`

`order_type` is the stable discriminator:

```text
market_order
```

Product-specific constraints live in action child tables rather than in
`router_orders`. Common order identity, status, funding, source asset,
destination asset, recipient, timeout, and idempotency fields stay in the generic
order row.

Current action child table:

```text
market_order_actions
```

Current market-order action columns:

- `order_id`
- `order_kind`
- `amount_in`
- `min_amount_out`
- `amount_out`
- `max_amount_in`
- `created_at`
- `updated_at`

Conceptual Rust shape:

```rust
struct RouterOrder {
    id: Uuid,
    order_type: OrderType,
    status: RouterOrderStatus,
    funding_vault_id: Option<Uuid>,
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    recipient_address: String,
    action: RouterOrderAction,
}

enum RouterOrderAction {
    MarketOrder(MarketOrderAction),
    LimitOrder(LimitOrderAction),
    Earn(EarnAction),
}
```

Market-order-specific provider quotes live in child tables:

```text
market_order_quotes
```

The worker should transition the generic order row and use child rows only for
action intent, quotes, custody vaults, and private execution state.

Private execution state is intentionally generic:

```text
custody_vaults
deposit_vaults
order_execution_steps
```

`custody_vaults` is the canonical table for every route address the router needs
to reason about: front-facing deposit vaults, internal execution accounts,
provider protocol addresses, recovery addresses, and watch-only destinations.
It stores role, visibility, chain, optional asset, address, control type,
non-secret derivation salt, signer reference, status, and private metadata.

`deposit_vaults` is the user-facing lifecycle extension for one-time source
deposit vaults. Its primary key is also the `custody_vaults.id`, so the address
and key-control record is not duplicated.

`order_execution_steps` stores the worker-visible route skeleton. Common worker
fields are relational columns: `order_id`, `step_index`, `step_type`, `provider`,
`status`, chain/asset columns, `tx_hash`, `provider_ref`, `idempotency_key`,
retry fields, and lifecycle timestamps. Provider-specific details stay in
`details_json`, `request_json`, `response_json`, and `error_json` until a field
needs SQL constraints, indexes, joins, or uniqueness.

Initial generic order statuses:

```text
quoted
pending_funding
funded
executing
completed
refund_required
refunding
refunded
manual_intervention_required
refund_manual_intervention_required
expired
```

The exact allowed transitions should be implemented with compare-and-swap
updates in the order repository. Product-specific tables must not bypass these
transitions.

## Order Kinds

### Exact Input

The user fixes `amount_in` and may supply `slippage_bps`. The router first
discovers an expected `amount_out`. If slippage is supplied, it derives
`min_amount_out` from that expected output and the requested slippage. If
slippage is omitted, `min_amount_out` remains unset and provider execution uses a
fresh quote amount for each stale venue leg.

Required user constraints:

- `amount_in`

Optional user constraints:

- `slippage_bps`

Selection rule:

- Choose the valid quote with the greatest `amount_out`.

Execution invariant:

- If `min_amount_out` is set, the worker must not complete the action if the
  settled output would be below it.

### Exact Output

The user fixes `amount_out` and may supply `slippage_bps`. The router first
discovers an expected `amount_in`. If slippage is supplied, it derives
`max_amount_in` from that expected input and the requested slippage. If slippage
is omitted, `max_amount_in` remains unset and provider execution uses a fresh
quote amount for each stale venue leg.

Required user constraints:

- `amount_out`

Optional user constraints:

- `slippage_bps`

Selection rule:

- Choose the valid quote with the lowest `amount_in`.

Execution invariant:

- If `max_amount_in` is set, the worker must not spend more than it.

If no provider supports exact-output for the requested route, the quote endpoint
returns a typed no-route error. It must not silently downgrade to exact-input.

## Quote API

Add:

```http
POST /api/v1/quotes/market-order
```

Request shape:

```json
{
  "from_asset": {
    "chain": "evm:1",
    "asset": "native"
  },
  "to_asset": {
    "chain": "bitcoin",
    "asset": "native"
  },
  "kind": "exact_in",
  "amount_in": "1000000000000000000",
  "slippage_bps": 100,
  "recipient_address": "bc1...",
  "idempotency_key": "client-generated-key"
}
```

Exact-output request shape:

```json
{
  "from_asset": {
    "chain": "evm:1",
    "asset": "native"
  },
  "to_asset": {
    "chain": "bitcoin",
    "asset": "native"
  },
  "kind": "exact_out",
  "amount_out": "100000",
  "slippage_bps": 100,
  "recipient_address": "bc1..."
}
```

Response shape:

```json
{
  "order": {
    "id": "019...",
    "order_type": "market_order",
    "status": "quoted",
    "funding_vault_id": null,
    "source_asset": {
      "chain": "evm:1",
      "asset": "native"
    },
    "destination_asset": {
      "chain": "bitcoin",
      "asset": "native"
    },
    "recipient_address": "bc1...",
    "action": {
      "type": "market_order",
      "payload": {
        "kind": "exact_in",
        "amount_in": "1000000000000000000",
        "min_amount_out": "100000",
        "slippage_bps": 100
      }
    },
    "action_timeout_at": "2026-04-14T00:10:00Z",
    "idempotency_key": "client-generated-key",
    "created_at": "2026-04-14T00:00:00Z",
    "updated_at": "2026-04-14T00:00:00Z"
  },
  "quote": {
    "id": "019...",
    "order_id": "019...",
    "provider_id": "chainflip",
    "order_kind": "exact_in",
    "amount_in": "1000000000000000000",
    "amount_out": "102000",
    "min_amount_out": "100000",
    "max_amount_in": null,
    "slippage_bps": 100,
    "provider_quote": {},
    "expires_at": "2026-04-14T00:01:00Z",
    "created_at": "2026-04-14T00:00:00Z"
  }
}
```

The response should include an explicit `VaultAction::MarketOrder` payload that
contains only market-order-specific fields. The `order_id` belongs to the
generic order envelope, not inside the market-order action payload.

## Quote Persistence

Persist the generic order row and selected market-order quote snapshot before
returning the quote to the client.

The persisted order must include:

- `order_id`
- `order_type = market_order`
- source asset
- destination asset
- recipient address
- market-order action payload
- execution timeout timestamp
- current status

The persisted quote must include:

- `quote_id`
- `order_id`
- order kind
- exact amount constraint
- optional derived minimum or maximum amount constraint
- selected provider
- quote expiration
- provider quote payload or opaque route handle
- creation timestamp

The persisted quote is not the final source of truth for user constraints. The
immutable order constraints on `router_orders` are.

## Vault Action

Extend `VaultAction` with:

```rust
MarketOrder(MarketOrderAction)

enum MarketOrderKind {
    ExactIn {
        amount_in: String,
        min_amount_out: Option<String>,
    },
    ExactOut {
        amount_out: String,
        max_amount_in: Option<String>,
    },
}

struct MarketOrderAction {
    order_kind: MarketOrderKind,
    slippage_bps: Option<u64>,
}
```

The action name remains explicit at the API/vault layer, but order identity sits
outside of the action in the generic `router_orders` envelope. This keeps product
semantics clear without forcing the primary orders table to be market-order
specific.

When a vault is created with `order_id`, the vault repository inserts the vault
and attaches it to the quoted order in a single transaction. The copied
`VaultAction::MarketOrder` payload must match the generic order action and must
not contain the order ID.

## Execution Flow

1. Client requests a market-order quote.
2. API validates assets, recipient address, order kind, and amount constraints.
3. API calls provider adapters in parallel with a short quote aggregation
   timeout.
4. Unsupported providers are skipped, including providers that do not support
   exact-output for an exact-output request.
5. API chooses the best valid quote:
   - exact-input: greatest `amount_out`
   - exact-output: lowest `amount_in`
6. API persists the generic `router_orders` row and selected quote snapshot.
7. API returns `order_id` and a `MarketOrder` action payload.
8. Client creates or binds a funding vault for the returned order and deposits
   the source asset.
9. Vault creation links the funding vault to the order and moves the order to
   `pending_funding`.
10. Worker observes the funded vault and moves the order to `executing`.
11. Worker executes the selected route. The original quote expiry is no longer
    relevant after order creation succeeds.
12. If a provider venue quote is stale before its leg starts, the worker requests
    a refreshed quote for that remaining route suffix and creates a new execution
    attempt.
13. Refreshed attempts are valid only if they satisfy the user's original amount
    constraints.
14. If no valid executable route is found before the action timeout, worker
    transitions the order and its funding vault to `refunding`.
15. Worker executes the provider route.
16. Worker marks the order and vault `completed` only after it has durable
    evidence that the action completed.

## Execution Timeout

Default v0 action timeout:

```text
10 minutes
```

The timeout starts when the vault becomes actionable, not when the quote endpoint
is called.

If the worker cannot find and execute a valid route before this timeout, it must
mark the vault `refunding`.

## Provider Adapter Contract

Each provider adapter should expose:

```rust
trait MarketOrderProvider {
    fn provider_id(&self) -> ProviderId;
    fn capabilities(&self) -> ProviderCapabilities;
    async fn quote(&self, request: MarketOrderQuoteRequest) -> ProviderQuoteResult;
    async fn build_execution(&self, quote: PersistedQuote, vault: VaultExecutionContext)
        -> ProviderExecutionPlan;
}
```

Capabilities must include:

- supported source chains
- supported destination chains
- supported assets
- exact-input support
- exact-output support
- whether execution uses an EVM transaction, BTC payment, memo, provider deposit
  address, or another chain-specific plan

## Execution Plan

Provider adapters must normalize execution into router-owned plan variants, for
example:

```rust
enum ProviderExecutionPlan {
    EvmTransactions(Vec<EvmTransactionPlan>),
    BitcoinPayment(BitcoinPaymentPlan),
}
```

The worker executes these through chain-specific executors. Provider adapters do
not get to broadcast arbitrary operations directly from the vault context.

## Validation Requirements

Before the vault signs or broadcasts anything, the worker must validate:

- source chain and source asset match the vault
- destination chain and destination asset match the quote
- recipient address matches the user-provided recipient
- exact-input `amount_in` is not exceeded
- exact-input `min_amount_out` is preserved
- exact-output `amount_out` is preserved
- exact-output `max_amount_in` is not exceeded
- provider ID matches the selected or replacement provider
- quote and route have not expired
- EVM transaction targets are allowlisted for that provider
- EVM approvals, if required, are exact-amount approvals
- BTC payment address, amount, memo, and network match the provider route
- no provider route can redirect proceeds to the vault, paymaster, or router
  operator unless explicitly required by the provider protocol and validated

## State Machine

Initial v0 states reuse the existing vault status model:

```text
pending_funding -> funded -> executing -> completed
pending_funding -> refunding -> refunded
funded -> refunding -> refunded
executing -> refunding -> refunded
```

Implementation will likely need action-specific columns or a companion action
table:

- `order_attempt_count`
- `order_last_attempt_at`
- `order_next_attempt_at`
- `order_timeout_at`
- `order_last_error`
- `selected_quote_id`
- `execution_tx_hashes`

## Invariants

- API processes never execute market-order actions.
- Exactly one router worker should hold the global worker lease at a time.
- Every market order is represented by a generic `router_orders` row.
- Product-specific tables cannot be the source of truth for generic order
  status.
- User amount constraints are immutable after vault creation.
- Quotes are advisory route snapshots; user constraints are authoritative.
- Exact-input execution cannot settle below `min_amount_out`.
- Exact-output execution cannot spend above `max_amount_in`.
- If no route satisfies the original constraints before timeout, the vault must
  refund.
- Provider responses are untrusted until normalized and validated.
- No router fee logic exists in the market-order product.
