# Arbitrum — Efficient Observation Strategy

Arbitrum is an Optimistic L2 (Arbitrum Nitro). Same observation patterns as Ethereum and Base. Special note: **Hyperliquid's bridge contract lives on Arbitrum**, making Arbitrum a load-bearing chain for our Hyperliquid integration.

## What we observe

Same general events as Ethereum/Base:
- ERC-20 `Transfer` to watched recipients (USDC primarily on Arbitrum One).
- CCTP `MessageSent` from TokenMessenger on Arbitrum.
- Across `V3FundsDeposited` from Arbitrum SpokePool.
- Velora swap settlement Transfers (the on-chain leg of `universal_router_swap` steps — see `venues/velora.md`).

**Arbitrum-specific events that matter to us:**
- **Hyperliquid bridge deposit events** — the canonical Hyperliquid bridge (Arbitrum USDC → HL spot/perp account) emits events from the HL bridge contract on Arbitrum. **This is the leg the 127-order load stall lived on.** See `venues/hyperliquid.md` for venue-specific design.
- **Hyperliquid bridge withdrawal events** — when HL settles a withdrawal, it lands as a USDC `Transfer` on Arbitrum from the HL bridge address to the recipient.

## Native push mechanisms

Same as Ethereum / Base — `eth_subscribe('newHeads')` and `eth_subscribe('logs', filter)`. Currently unused; same fix recommended.

Arbitrum also exposes Arbitrum-specific subscriptions like `arbtrace_*` calls — out of scope, we don't need execution traces.

## Pull / fallback mechanisms

- `eth_getLogs` HTTP — same.
- Token Indexer gRPC (`ARBITRUM_TOKEN_INDEXER_URL`) — same.
- `eth_getBlockByNumber` for reorg detection.

## Recommended efficient design

Same as Ethereum's design. Arbitrum-specific tunings:

| Parameter | Ethereum | Base | Arbitrum | Why |
|---|---|---|---|---|
| Block time | ~12s | ~2s | **~250ms** | Arbitrum is *very* fast — block-depth math gets weird |
| Required confirmations | 1 | 1 | **1** | Sequencer confirms in < 1s; safe to act |
| Reorg rescan depth | 32 | 8–12 | **48–96** (in blocks) → ~12-24s in time | Sequencer reorgs are rare but want a generous *time-based* safety belt; in blocks this looks deep but is shallow in time |
| Backfill window | 128 blocks | 256 blocks | **1024 blocks (~4 min)** | Compensate for very short block time |
| WS subscription | should add | should add | should add | Same fix |

The key intuition: thinking in *blocks* is misleading on Arbitrum. Always convert to wall-clock time. A 32-block window on Ethereum is ~6 minutes; on Arbitrum it's ~8 seconds. We want safety belts in *time*, not in raw block count.

## Hyperliquid bridge deposit observation (load-bearing)

This is the failure mode the loadgen run exposed. Recommended approach (cross-references `venues/hyperliquid.md`):

1. **Primary path** — WS subscribe to logs filtered by `address = HL_BRIDGE_CONTRACT_ARB` and `topics[0] = DepositEvent.topic0`. Each event carries a depositor address that matches the user's source vault and an amount.
2. **Hint emission** — emit `DetectorHint { execution_step_id, hint_kind: BridgeDepositObserved, tx_hash, log_index, amount, depositor }`.
3. **Verification** (T-router side, unchanged) — verifier matches `depositor` and `amount` to the expected step, then queries HL `/info` for credit confirmation on the perp account (the actual usable balance).

This requires:
- A Hyperliquid venue backend in Sauron (currently absent).
- A new `ProviderOperationHintKind::BridgeDepositObserved` (the enum currently only has `PossibleProgress`).
- Watch registration that includes the HL bridge contract address as a known watched address on Arbitrum (today the EVM backend matches recipients by address, not depositors — emission needs to handle both directions).

## Step-ID disambiguation

Same cross-cutting requirement as `audit.md` §B. Arbitrum-specific note: HL bridge deposits use the user's vault as the depositor; the same vault may have several superseded operations attached. Hint emission must include `execution_step_id` so the verifier routes correctly.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| Arbitrum sequencer downtime | Sauron Arbitrum backend goes silent; HL-bound orders stall visibly. Operator switches to backup RPC. WS reconnect + backfill on resumption |
| RPC node falls behind chain head | Detected via `newHeads` heartbeat; failover to secondary |
| HL bridge contract upgrade (proxy redeployment) | The watched contract address must be pulled from config; on upgrade we update config + reconcile. Rare but operationally important |
| Reorg deeper than rescan window | Alert; pause Arbitrum-side hint acceptance; resume after manual confirmation |

## What's missing today

Same chain-level gaps as Ethereum/Base (no WS, no reorg-aware retraction, no per-chain reorg tuning), plus the venue-level gap:

1. **No Hyperliquid bridge contract observation.** Sauron knows about Arbitrum USDC `Transfer` events but not about HL bridge `DepositEvent` events. This is the most important venue-specific addition for Arbitrum.
2. **`EVM_REORG_RESCAN_DEPTH = 32` blocks** is wildly inappropriate for Arbitrum (~8 seconds). Should be ~48-96 blocks (~12-24s) to give a more useful time-based safety window.

## Out of scope

- Arbitrum L1 batch posting / fraud-proof window monitoring (we don't need 7-day finalization).
- Stylus / WASM contract interactions.
- Arbitrum Orbit chains (we don't use them).
