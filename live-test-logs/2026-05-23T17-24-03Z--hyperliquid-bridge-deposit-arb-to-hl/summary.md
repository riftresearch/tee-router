# Hyperliquid bridge deposit (Arb USDC → HL spot, tier-2)

- **Started**: 2026-05-23T17-24-03Z UTC
- **Wallet**: `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92`
- **Direction**: Arbitrum USDC → Hyperliquid spot USDC, 5 USDC
- **Total wall time**: 13.71s
- **Result**: ✅ **deposit credited** — Arb USDC −5.000000, HL spot USDC +5.000000.

## Pre-flight

Wallet only had 1.18 USDC on Arbitrum; HL bridge minimum is 5 USDC. Topped up via another Across run (Base→Arb, 5 USDC, 7.82s, fully filled — `live-test-logs/2026-05-23T17-23-35Z--across-topup-5usdc-for-hl/`). Net Arb USDC available pre-deposit: 6.171672.

## Transactions

| Step | Chain | tx hash | Block |
|---|---|---|---|
| USDC.transfer(bridge, 5_000_000) | Arbitrum | `0x1345501afc0960cb67be28f97677d72925103916e6ae50ca6cbd581b00514e89` | 465906364 |

Explorer: https://arbiscan.io/tx/0x1345501afc0960cb67be28f97677d72925103916e6ae50ca6cbd581b00514e89

## Why `to: USDC` (not Bridge2)

The Hyperliquid bridge deposit pattern is a plain `USDC.transfer(bridge, amount)` on Arbitrum — not a direct call to `Bridge2`. HL's off-chain indexer watches USDC's `Transfer(_, bridge=0x2df1c51e09aecf9cacb7bc98cb1742757f163df7, amount)` event and credits the matching `from` address's HL spot USDC balance. The `Bridge2.batchedDepositWithPermit` flow exists for gasless permit-based deposits but is not the default; our `hyperliquid_client::build_bridge_deposit_tx_to` uses the plain-transfer path.

Confirms the audit-aligned observation in `MockHyperliquidBridge2.sol`:
> "Real Hyperliquid deposits are usually plain ERC20 transfers to the bridge address, so this contract mostly serves as the receiving address plus the `batchedDepositWithPermit` ABI."

## Balance deltas

| | Arb USDC | HL spot USDC |
|---|---|---|
| pre  | 6.171672 | 0.793261 |
| post | 1.171672 | 5.793261 |
| Δ    | **−5.000000** | **+5.000000** |

No HL deposit fee; only Arb gas (gasLimit 45912 — cheap on Arb).

## Validation

End-to-end HL bridge deposit flow validated:
1. `build_usdc_bridge_deposit` produced canonical `USDC.transfer(bridge, amount)` calldata.
2. Tx accepted on Arbitrum (block 465906364).
3. HL's off-chain indexer detected the Transfer event into the bridge address.
4. HL crediting machinery applied + balance updated within seconds.
5. HL spot USDC delta exactly matches the deposited amount with no fee.

This is the canonical primitive the router uses for any HL-as-destination route (e.g., USDC → HL spot → UETH/UBTC for the BTC-via-Hyperunit flow).
