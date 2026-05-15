# Order Executor Rewrite Brief

The current executor model is Temporal-first and exact-in at the router level.
This brief supersedes the older manual-pause executor notes.

## Current Decisions

- Market orders persist only `amount_in`.
- Quote output is stored as `estimated_amount_out` and is informational.
- Order execution does not use an order-level expiry after creation.
- Funding vault watch lifetime is based on deposit vault `cancel_after`.
- Automatic execution failures that cannot be resolved by the workflow move the
  order to `refund_required`.
- Refund handling moves through `refunding` to `refunded`.

## Admin Queue

`refund_required` is the state an operator should review. The operator should
compare Postgres order flow state with Temporal history before taking
out-of-band recovery action.
