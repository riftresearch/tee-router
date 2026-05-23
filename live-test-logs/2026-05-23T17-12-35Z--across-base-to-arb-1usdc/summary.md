# Across Base → Arbitrum 1 USDC bridge (tier-2, ✅ closure)

- **Started**: 2026-05-23T17-12-35Z UTC
- **Wallet**: `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92`
- **Direction**: Base USDC → Arbitrum USDC, 1 USDC
- **Total wall time**: 7.89s (Across relayers fill L2-to-L2 routes near-instantly)
- **Result**: ✅ **status=filled** — bridge settled, both sides verified on-chain.

## Transactions

| Step | Chain | tx hash | Block |
|---|---|---|---|
| deposit | Base | `0xe76914dab43a31329dba7b2e113c5e7e23f7b86ee1d4377abb2a2519070c5c6d` | 46383522 |
| fill    | Arbitrum | `0x1ca340d3c7e4646ea89d03c99bf37d3e2a747a26a4bb3fe855f783d86f63ced2` | (see fill-tx-receipt.json) |

Explorers:
- Deposit: https://basescan.org/tx/0xe76914dab43a31329dba7b2e113c5e7e23f7b86ee1d4377abb2a2519070c5c6d
- Fill: https://arbiscan.io/tx/0x1ca340d3c7e4646ea89d03c99bf37d3e2a747a26a4bb3fe855f783d86f63ced2

## Balance deltas

| | Base USDC | Arb USDC |
|---|---|---|
| pre  | 51.350055 | 0.184277 |
| post | 50.350055 | 1.178228 |
| Δ    | **−1.000000** | **+0.993951** |

The 0.006049 USDC delta (~$0.006) is Across's bridge fee on this route — matches `expectedOutputAmount: "993951"` from `/swap/approval`.

## API shape validation (Across)

`/swap/approval` returned the documented shape:
- `inputAmount`: "1000000"
- `expectedOutputAmount`: "993951"
- `minOutputAmount`: "993951"
- `swapTx.to`: `0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64` (Across's multicall handler on Base for the recipient-EOA route)

`/deposit/status` returned `status: "filled"` with:
- `depositTxnRef` → our deposit tx
- `fillTxnRef` → the relayer's fill tx
- camelCase field renames matching our typed `AcrossDepositStatusResponse` exactly.

## Validation

This completes Across at tier-2:
1. Real `/swap/approval` quote + calldata used in production form
2. ERC-20 approve + deposit tx on Base accepted
3. `FundsDeposited` event emitted (real event name, matching the rename committed earlier in this session at `5983d6a`)
4. Across relayer filled on Arbitrum within seconds
5. Recipient EOA received +0.993951 USDC on Arbitrum
