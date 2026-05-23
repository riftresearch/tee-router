# Hyperunit UBTC → BTC withdrawal (tier-2, ✅ closure)

- **Started**: 2026-05-23T16-26-41Z UTC
- **Wallet**: `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92`
- **Direction**: HL spot UBTC → Bitcoin (`bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn`)
- **Amount attempted**: 0.0003 UBTC (30000 sats — Hyperunit's effective minimum)
- **Total wall time**: 1428s (23.8 min)
- **Result**: ✅ **state=done** — real BTC delivered on-chain.

## What happened

| Step | Outcome |
|---|---|
| Top-up: sell 0.015 UETH → USDC | ✅ avgPx $2055.1, $30.83 USDC received |
| Top-up: buy 0.0004 UBTC ← USDC | ✅ avgPx $75354.0, $30.14 USDC spent |
| Wallet now holds ≥ 0.00035 UBTC | ✅ 0.0004192584 UBTC |
| `/gen` via SOCKS5 proxy | ✅ protocol address `0xa0756aF705a4C587511a33912AeFbd249b49e2Cc` |
| Guardian signature verification | ✅ all 3 mainnet guardian sigs verified |
| HL `sendAsset` action submitted | ✅ {"response":{"type":"default"},"status":"ok"} |
| UBTC transferred to Hyperunit | ✅ wallet UBTC: 0.0004192584 → 0.0001192584 |
| Hyperunit state progression | sourceTxDiscovered → waitForSrcTxFinalization → readyForWithdrawQueue → queuedForWithdraw → **done** |
| BTC tx broadcast | ✅ `ba2effc652c4417a64ea4117341e0825b8ebcb468aaa41890eb9b5fd13d01823` |
| BTC tx confirmed | ✅ block 950694, 1+ confirmations |
| Our destination received | ✅ **29563 sats** at `bc1qk4m6mpx…` |

## On-chain delivery

Hyperunit batched our 30000-sat withdrawal into a single BTC tx with 5 other user outputs. Our share:

- Source amount: 30000 sats
- Destination fee (our share of the batched tx fee): 364.17 sats
- Received: 29563 sats (~$22)

Tx breakdown (mempool.space confirmed):
```
output 0: bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn (us!)        29563 sats
output 1: bc1q0g4vdc8qkht5cylllps2k7lwmgpcz365c7ypx9               4,999,563 sats
output 2: bc1q7ceax0sf9qf7z0sgkwhgjgn74q68z3pskwa3a6              26,585,944 sats
output 3: 15DomY3arDPw5jD658Mbf3AY3wT3YCDmW2                  3,929,999,563 sats
output 4: bc1qh0xkzq3jsfqec93wn9dn9qdpva79l4xufs3xqq                 549,250 sats
output 5: bc1pdwu79dady576y3fupmm82m3g7p2p9f6hgyeqy0tdg7ztxg7xrayqlkl8j9  6,589,392,548 sats
total fee: 2185 sats over 537 vB
```

Explorer: https://mempool.space/tx/ba2effc652c4417a64ea4117341e0825b8ebcb468aaa41890eb9b5fd13d01823

## Net flow

| Asset/chain | pre | post | delta |
|---|---|---|---|
| HL UETH | 0.02103319 | 0.00603319 | −0.015 (sold for USDC) |
| HL USDC | 0.13 | 0.79 | +0.66 (residual after both swaps) |
| HL UBTC | 0.0000195384 | 0.0001192584 | −0.0003 + 0.0004 (buy) − 0.0003 (Hyperunit) |
| BTC chain | 0 | 29563 sats | **+29563 sats (~$22)** ✓ |

Approximate net to wallet: ~−$8 (sum of HL spot fees + Hyperunit withdraw fee + BTC tx-fee share).

## Validation

This completes the venue closeout at tier-2:
1. Full Hyperunit protocol round-trip: HL→Hyperunit→BTC chain
2. Guardian-signature verifier (the security-critical piece) passed real production sigs in tier-1 (commit `83b975f`) AND in this run's `/gen` (the test verifies before sending UBTC)
3. State machine reached terminal `done` with a real BTC tx broadcast
4. End-to-end value delivered on chain: 29563 sats at our destination address

## Notes

- The previous 2026-05-23T16-04-12Z run's 20000-sat stuck operation is still
  `state: failure` in Hyperunit's records. Manual support contact at
  app.hyperunit.xyz/support is required to recover those funds (separate
  from this plan; not blocked by it).
- The plan's top-up flow (UETH→USDC→UBTC swap on HL spot) used a
  newly-buy-capable variant of `live-hyperliquid-sell-spot`
  (commit `0db7ac6`).
