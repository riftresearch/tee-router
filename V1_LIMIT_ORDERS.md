# V1 Limit Orders

## Scope

The first limit order implementation should live at the TEE router layer: router
API, router worker, and the state machine running inside the TEE. It should not
be a router-gateway-only abstraction.

V1 should support only these pairs, symmetrically:

- BTC / USDC
- ETH / USDC

For EVM-side USDC and ETH support, V1 should work across the currently supported
EVM chains:

- Ethereum
- Arbitrum
- Base

The implementation should make adding another EVM chain, such as Optimism,
mostly a chain-configuration exercise.

Limit orders should not use arbitrary asset routing. If a user starts with
cbBTC, PEPE, or any asset outside the strict V1 limit-order asset set, the order
is unsupported. V1 limit orders should not use Velora or other universal-router
logic.

## User-facing intent

V1 limit orders should be represented as an exact funding intent with a minimum
output guarantee:

```text
Use up to inputAmount of inputAsset to receive at least outputAmount of
outputAsset.
```

The user should not need to specify a Hyperliquid price, side, base asset, quote
asset, or time-in-force. Those are internal execution details derived from the
input/output asset pair and the requested amounts.

There is no user-facing expiry in V1. A limit order rests until it fills or the
user cancels it through the configured cancellation authorization.

## Price improvement and residuals

Any surplus created by price improvement belongs to the user.

For example, if the user locks 100,000 USDC to buy 1 BTC, the Hyperliquid order
is semantically:

```text
Buy 1 BTC for no more than 100,000 USDC.
```

If the order fills at 98,500 USDC, the user receives 1 BTC and has 1,500 USDC of
residual input. V1 should not market-order that residual into BTC and should not
sweep it to the protocol. The residual should remain user-owned and become
refundable or sweepable through the refund/recovery flow.

The V1 residual policy is therefore fixed:

```text
residualPolicy = refund
```

## Fees

`inputAmount` should be treated as the gross amount the user is willing to fund.
`outputAmount` should be treated as the net amount the user wants delivered to
the destination after the route succeeds.

Provider fees, bridge fees, withdrawal fees, and paymaster reimbursements should
therefore be accounted for before deriving the Hyperliquid order parameters.
They should not be hidden by reducing the user's final output after the fact.

For a buy-style order such as EVM USDC to Bitcoin BTC:

```text
hyperliquidInputBudget =
  inputAmount
  - preOrderInputAssetFees
  - paymasterFeeRetentions

hyperliquidOutputSize =
  outputAmount
  + postFillOutputAssetWithdrawalFees

hyperliquidLimitPrice =
  hyperliquidInputBudget / hyperliquidOutputSize
```

For a sell-style order such as Bitcoin BTC to EVM USDC:

```text
hyperliquidInputSize =
  inputAmount
  - preOrderInputAssetDepositFees

requiredHyperliquidOutput =
  outputAmount
  + postFillOutputAssetBridgeFees
  + paymasterFeeRetentionsIfSettledInOutput

hyperliquidLimitPrice =
  requiredHyperliquidOutput / hyperliquidInputSize
```

If runtime fees are lower than estimated, the surplus remains user-owned and is
refundable. If runtime fees are higher than estimated, the worker must not
weaken the user's guarantee by delivering less than `outputAmount`; it should
derive the resting Hyperliquid order from the actual available budget, or fail
the order into the refund path if the order can no longer be placed safely.

## Hyperliquid-resting model

V1 should use Hyperliquid as the actual limit order book. The router first moves
the user's funds into Hyperliquid spot, derives the Hyperliquid order parameters
from the user's input/output amounts, places a real `Gtc` Hyperliquid limit
order, then waits for the order to fill or be cancelled.

The order lifecycle is:

1. Ingress the starting asset into Hyperliquid spot.
2. Derive the Hyperliquid side, limit price, and order size from the requested
   input amount and output amount.
3. Place the Hyperliquid limit order with `tif = Gtc` and a deterministic client
   order id.
4. Monitor the Hyperliquid order state.
5. If the order fills, egress the filled asset to the user's destination chain.
6. If the user cancels with the configured cancellation authorization, cancel the
   resting Hyperliquid order, mark remaining funds refundable, and use the
   existing refund flow.

Hyperliquid order monitoring likely belongs near Sauron. Hyperliquid's public
info surfaces should be preferred where possible so the router worker does not
become a polling loop. The router worker should still validate any hint before
advancing state.

## Example: EVM USDC to BTC

For an order such as Ethereum USDC to BTC:

1. The user deposits USDC on an EVM chain.
2. If the USDC is not already on Arbitrum, bridge it to Arbitrum USDC using the
   cheapest currently preferred USDC bridge path, such as CCTP or Across.
3. Deposit Arbitrum USDC into Hyperliquid spot through the Hyperliquid bridge.
4. Once Hyperliquid credits the USDC balance, place the BTC/USDC limit order
   derived from `inputAmount` and `outputAmount`.
5. If the order fills, withdraw the resulting spot BTC through HyperUnit to
   Bitcoin.
6. If the order is cancelled, make remaining funds refundable and route the
   refund back through the existing refund system.

## Example: BTC to EVM USDC

For an order such as BTC to Base USDC:

1. The user deposits BTC into a Rift Bitcoin deposit vault.
2. The vault deposits BTC through HyperUnit.
3. HyperUnit credits the BTC into Hyperliquid spot.
4. Place the BTC/USDC limit order derived from `inputAmount` and
   `outputAmount`.
5. If the order fills, withdraw the resulting spot USDC from Hyperliquid to
   Arbitrum USDC.
6. Bridge Arbitrum USDC to the user's target EVM chain, such as Base, using the
   cheapest currently preferred USDC bridge path.
7. If the order is cancelled, make remaining funds refundable and route the
   refund through the existing refund system.

## Internal execution model

The internal execution should be modeled as a limit-order-specific state machine
that reuses the existing custody, provider operation, hint, and refund
primitives:

1. `WaitForDeposit`: user funds the source deposit vault.
2. `IngressToHyperliquid`: bridge/deposit the input asset into the per-order
   Hyperliquid spot custody identity.
3. `PlaceHyperliquidLimitOrder`: submit a `Gtc` Hyperliquid limit order and
   persist the Hyperliquid `cloid`, `oid` when available, side, price, size, and
   account identity in provider operation state.
4. `WaitForHyperliquidLimitFill`: wait for Sauron/provider hints that the
   Hyperliquid order is filled, cancelled, rejected, or otherwise terminal.
5. `EgressFilledAsset`: when filled, move the filled output asset to the user's
   destination chain/address.
6. `RefundResiduals`: when cancelled or when price improvement leaves residual
   input, route remaining user-owned funds through the refund/recovery flow.

The worker should not rely on a periodic polling loop for fill detection. It
should progress after Sauron or another detector records a provider-operation
hint, then the worker validates that hint against Hyperliquid before mutating
the order state.

## ETH / USDC support

ETH / USDC should follow the same Hyperliquid-resting model. The exact ingress
and egress paths should be specified during implementation based on the current
Hyperliquid and bridge support for ETH across Ethereum, Arbitrum, and Base.

The important V1 invariant is that ETH / USDC limit orders still rest on
Hyperliquid and still avoid arbitrary-asset routing.
