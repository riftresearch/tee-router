# Market Order Action Specification

## Status

Market orders are exact-in at the router API level. Router-level exact-out,
minimum-output, maximum-input, and user execution-tolerance fields are not part of the
market-order contract.

Provider adapters may still carry provider-native execution bounds in their
request detail JSON when the venue API requires them. Those fields are not
exposed as router quote or order concepts.

## Router Shape

`router_orders` stores the common order envelope:

- `id`
- `order_type`
- `status`
- `funding_vault_id`
- `source_chain_id`
- `source_asset_id`
- `destination_chain_id`
- `destination_asset_id`
- `recipient_address`
- `refund_address`
- `idempotency_key`
- `created_at`
- `updated_at`

`market_order_actions` stores the exact input amount for created orders:

- `order_id`
- `amount_in`
- `created_at`
- `updated_at`

`market_order_quotes` stores quote-time informational output:

- `id`
- `order_id`
- `source_chain_id`
- `source_asset_id`
- `destination_chain_id`
- `destination_asset_id`
- `recipient_address`
- `provider_id`
- `amount_in`
- `estimated_amount_out`
- `provider_quote`
- `usd_valuation_json`
- `expires_at`
- `created_at`

## Rust Shape

```rust
struct MarketOrderAction {
    amount_in: String,
}

struct MarketOrderQuote {
    amount_in: String,
    estimated_amount_out: String,
}
```

`estimated_amount_out` is informational. Execution success is based on the route
completing with the best available provider execution path, not on a
router-level user bound.

## Statuses

Orders use this lifecycle:

- `quoted`
- `pending_funding`
- `funded`
- `executing`
- `completed`
- `refund_required`
- `refunding`
- `refunded`

`refund_required` is the admin review queue. The public gateway maps both
`refund_required` and `refunding` to `refund_pending`.

## Funding And Watch Lifetime

Orders do not expire after creation. Funding vault watch lifetime is governed by
the deposit vault `cancel_after` value. Once that watch window closes, Sauron can
remove the vault from the watch set.

Unassociated quotes still have quote expiries because a quote without an order
is not durable order intent.

## Limit Orders

Limit order code is retained but disabled. The router gateway should not expose a
public limit order endpoint, and the router API rejects limit quote/order paths
while the feature is disabled.
