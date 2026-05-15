# Synthetic Limit Orders

## Purpose

This note captures a future limit-order design. Limit orders are currently
disabled in the router gateway and router API, so this document is design-only.

The synthetic model is useful for venues and routes that do not support native
limit orders. Instead of resting an order on an external order book, the router
holds user funds and only starts a normal market/cross-chain execution once the
observed market price makes the user's requested limit achievable.

## Model

1. User funds rest in a Rift deposit vault or equivalent controlled execution
   account.
2. The system monitors the relevant market price.
3. When the market price reaches or crosses the user's limit price, the router
   creates a normal exact-in market or cross-chain order.
4. Execution quality is checked against venue/provider behavior and observed
   settlement, not a router-level user execution-tolerance field.

This behaves like a limit order from the user's perspective because execution
does not begin until the target price is likely available. Internally, though,
the fill is still a normal router execution, not a native venue-level limit
order.

## Execution constraints

The system should not fire market orders merely because the spot price briefly
touches the limit. It needs to account for route costs, bridge fees, venue fees,
gas, and observed venue behavior before deciding that the limit is actually
executable.

## Relationship to V1

The synthetic design is a later option for routes where there is no native
limit-order venue or where the router wants to emulate limit-order behavior
across arbitrary cross-chain paths.
