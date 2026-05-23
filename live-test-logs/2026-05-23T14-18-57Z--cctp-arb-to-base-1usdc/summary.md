# CCTP Arbitrum → Base 1 USDC round trip

- **Started**: 2026-05-23T14-30-52Z UTC
- **Wallet**: `0x33f65788aca48d733c2c2444ac9f79b18206aa92`
- **Result**: ✅ success — burn + Iris attestation + receive all completed
- **Total wall time (this invocation)**: 219.6s
- **Resume mode**: yes (skipped approve+burn; reused prior burn tx)

## Transactions

| Step | Chain | tx hash | Block | Gas used |
|---|---|---|---|---|
| burn  | Arbitrum | `0xbfb277dbc664b59dd37de5108e3e1d73df7b39d5b334c33fe3e90b18bd5d58c8`   | (see burn-tx.json) | (see burn-tx.json) |
| receive | Base    | `0xdf00781063637037dcb17378228b585bdabdd5d03552b1fd5dc7f5439f1b6864` | 46378760 | 154099 |

Explorers:
- Burn: https://arbiscan.io/tx/0xbfb277dbc664b59dd37de5108e3e1d73df7b39d5b334c33fe3e90b18bd5d58c8
- Receive: https://basescan.org/tx/0xdf00781063637037dcb17378228b585bdabdd5d03552b1fd5dc7f5439f1b6864

## Balance deltas (this invocation's pre/post snapshots)

| | Arbitrum ETH | Arbitrum USDC | Base ETH | Base USDC |
|---|---|---|---|---|
| pre  | 0.000840861577053137 | 0.184277 | 0.000059400070261481 | 51.350055 |
| post | 0.000840861577053137 | 0.184277 | 0.000058472076420702 | 52.350055 |

Base USDC delta: +1.000000 ✓

## Validation

This run validates the full CCTP V2 protocol contract against our types:
- `depositForBurn` calldata (vendored ABI binding) accepted by Circle's
  TokenMessengerV2 on Arbitrum
- Iris V2 response shape successfully polled to `complete` (see
  iris-poll-*.json + attestation-complete.json)
- `receiveMessage` calldata (vendored ABI binding) accepted by
  MessageTransmitterV2 on Base; `MintAndWithdraw` event emitted; USDC
  delivered to recipient with the expected delta.
