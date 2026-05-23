# Velora V6 USDC → WETH swap on Base (tier-2 funded)

- **Started**: 2026-05-23T15-02-35Z UTC
- **Wallet**: `0x33f65788aca48d733c2c2444ac9f79b18206aa92`
- **Result**: ✅ swap settled
- **Total wall time**: 8.5s

## Quote
- via `swapExactAmountIn` (V6 direct-route variant chosen by Velora)
- 1.000000 USDC → 0.000486569355928488 WETH (slippage tolerance 100 bps)

## Transactions
| Step | tx hash | Block | Gas used |
|---|---|---|---|
| swap | `0x2626b1ac805f9ff8355f9bd363fdcd9ce496cc746a0f9ffdfb10b1abefce941b` | 46379606 | 273578 |

Explorer: https://basescan.org/tx/0x2626b1ac805f9ff8355f9bd363fdcd9ce496cc746a0f9ffdfb10b1abefce941b

## Balance deltas

| | ETH | USDC | WETH |
|---|---|---|---|
| pre  | 0.000055722030925050 | 52.350055 | 0.000000000000000000 |
| post | 0.000054246652548338 | 51.350055 | 0.000486569355928488 |

USDC delta: -1.000000
WETH delta: +0.000486569355928488

## Validation

End-to-end Velora V6 contract behavior matches the vendored ABI:
- `/prices` returned `version: 6.2`, `contractAddress: AugustusV6`
- `/transactions` returned calldata for `swapExactAmountIn` directed at AugustusV6
- The swap settled on-chain, USDC decreased by exactly 1.0, WETH increased
  by ≥ quote × (1 - slippage). No revert.
