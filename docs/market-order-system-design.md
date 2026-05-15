# Market Order System Design

Router market orders are exact-in only. The public and internal router contracts
store the input amount and an informational estimated output amount; they do not
store user-supplied execution tolerance fields.

The canonical field-level specification is
[`market-order-action-spec.md`](./market-order-action-spec.md).

## Execution Model

1. Quote selection estimates the best exact-in route for the requested
   `amount_in`.
2. Order creation persists `MarketOrderAction { amount_in }` and creates the
   deposit vault.
3. The funding vault remains in the watch set until its `cancel_after` window.
4. Once funded, Temporal materializes provider-local execution steps.
5. Provider adapters may include venue-native bounds in provider request detail
   JSON when the venue API requires them.
6. Completion records actual settled amounts on execution legs and steps.
7. If the workflow cannot continue automatically, it moves the order to
   `refund_required`.

## Status Model

Orders use this lifecycle:

- `quoted`
- `pending_funding`
- `funded`
- `executing`
- `completed`
- `refund_required`
- `refunding`
- `refunded`

`refund_required` is the admin review state. The gateway exposes
`refund_required` and `refunding` as `refund_pending`.
