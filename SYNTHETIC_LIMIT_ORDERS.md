# Synthetic Limit Orders

## Purpose

This note captures a future limit-order design that is separate from the V1
Hyperliquid-resting limit order implementation. V1 should use real resting
orders on Hyperliquid for the narrow BTC/USDC and ETH/USDC scope.

The synthetic model is useful for venues and routes that do not support native
limit orders. Instead of resting an order on an external order book, the router
holds user funds and only starts a normal market/cross-chain execution once the
observed market price makes the user's requested limit achievable.

## Model

1. User funds rest in a Rift deposit vault or equivalent controlled execution
   account.
2. The system monitors the relevant market price.
3. When the market price reaches or crosses the user's limit price, the router
   creates a normal market or cross-chain order.
4. The market order uses slippage/min-output constraints so it only executes if
   the user receives at least the amount implied by the original limit order.

This behaves like a limit order from the user's perspective because execution
does not begin until the target price is likely available. Internally, though,
the fill is still a normal router execution, not a native venue-level limit
order.

## Execution constraints

When the trigger condition is met, the router should create the execution with a
minimum-output or maximum-input constraint derived from the original limit price.
That constraint is what prevents the synthetic order from filling worse than the
user's requested limit.

The system should not fire market orders merely because the spot price briefly
touches the limit. It needs to account for route costs, bridge fees, venue fees,
gas, and slippage before deciding that the limit is actually executable.

## Relationship to V1

This is not the V1 implementation. V1 limit orders should be real Hyperliquid
resting orders for BTC/USDC and ETH/USDC. The synthetic design is a later option
for routes where there is no native limit-order venue or where the router wants
to emulate limit-order behavior across arbitrary cross-chain paths.
