# Market Order Live Path Plan

The live path uses exact-in router market orders.

## Router Contract

- Quote request: source asset, destination asset, recipient address, `amount_in`.
- Quote response: `amount_in`, `estimated_amount_out`, provider quote details,
  expiry.
- Order action: `MarketOrderAction { amount_in }`.

Provider adapters can keep venue-native fields in provider request JSON. Those
fields are not router API parameters.

## Execution

1. Select the best exact-in route.
2. Persist the quote and create the order from that quote.
3. Create the deposit vault at order creation time.
4. Start execution after funding is observed.
5. Complete with actual settled amounts, or move to `refund_required` when the
   workflow cannot continue automatically.
