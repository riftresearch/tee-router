# Base — Efficient Observation Strategy

Base is an Optimistic L2 (OP Stack). Observation patterns are nearly identical to Ethereum, with three meaningful differences: faster blocks, much shallower practical reorg risk, and sequencer-centralized ordering that makes pre-confirmation latency very low.

## What we observe

Same events as Ethereum (`chains/ethereum.md`):
- ERC-20 `Transfer` to watched recipients (USDC primarily, since Base USDC is a major liquidity hub).
- CCTP `MessageSent` from TokenMessenger on Base.
- Across `V3FundsDeposited` from Base SpokePool.
- Velora swap settlement Transfers from Velora's executor to recipient (the on-chain leg of `universal_router_swap` steps — see `venues/velora.md`).

## Native push mechanisms

Same as Ethereum — Base nodes (Base RPC providers + self-hosted Base Geth) expose:
- `eth_subscribe('newHeads')`
- `eth_subscribe('logs', filter)`
- `eth_subscribe('newPendingTransactions')` (out of scope)

Currently Sauron does not use any of these — same gap as Ethereum.

## Pull / fallback mechanisms

- `eth_getLogs` HTTP — same.
- Token Indexer gRPC (`BASE_TOKEN_INDEXER_URL`) — same shape, separate indexer instance.
- `eth_getBlockByNumber` for reorg detection.

## Recommended efficient design

Same shape as Ethereum's design. Base-specific tunings:

| Parameter | Ethereum | Base | Why |
|---|---|---|---|
| Block time | ~12s | **~2s** | Base is 6x faster — rescan window in *time* terms is 6x shorter for the same block depth |
| Required confirmations | 1 | **1** | Sequencer-confirmed is almost always good enough for our values |
| Reorg rescan depth | 32 | **8–12** | Sequencer reorgs above 1 block are practically unheard of; 8 blocks (16s) is a safety belt; 32 (~64s) is paranoid |
| WS subscription | not used today (should be) | not used today (should be) | Same fix |
| Backfill window | 128 blocks (~25min) | **256 blocks (~8.5min)** | Bigger window in blocks compensates for shorter block time |

Reducing reorg depth from 32 → 8–12 on Base means smaller backfill windows on reconnect, fewer log-rescan cycles, less indexer pressure. The gain is small per-event but cumulative.

## Sequencer-aware optimization (Base-specific)

Base's sequencer publishes blocks with strong soft-finality immediately; the formal L1 finalization comes later when the OP Stack settlement window closes (~7 days). For our use case (operational liveness of order execution), **sequencer-confirmation is the right point to act on hints** — waiting for L1 finalization would add days. Our 1-block confirmation policy is correct.

If the sequencer ever produces a deep reorg (operator misconfiguration, bug, or extremely unusual scenario), our 8-12 block rescan window catches it within a minute.

## Step-ID disambiguation

Same cross-cutting requirement as `audit.md` §B. Base-specific note: Base USDC is bridged from Ethereum via Circle's CCTP, so it's possible an inbound CCTP `MessageReceived` event and an inbound user `Transfer` could touch the same recipient address within the same block. The verifier disambiguates by source contract (TokenMessenger vs ERC-20), but the watch entry should still carry the `execution_step_id` of the active step the watch is registered for.

## Failure modes and backstops

Same as Ethereum, with one Base-specific failure:

| Failure | Mitigation |
|---|---|
| Base sequencer downtime (rare, has happened) | Sauron's Base backend goes silent; T-router orders touching Base stall visibly. Operator alert: switch to a backup Base RPC provider, and Sauron-level recovery on sequencer resumption (auto via WS reconnect + backfill) |

## What's missing today

Same gaps as Ethereum (no WS, no reorg-aware retraction, no per-chain reorg tuning), plus:

1. **`EVM_REORG_RESCAN_DEPTH` is uniform across all EVM chains.** Per-chain constants would let Base run with 8–12 instead of 32 — meaningful saving on the indexed-lookup pressure and backfill window.
2. **Backfill window is the same 128 blocks across chains.** Should be tuned per chain by either time (e.g., 25 min) or block count (e.g., 256 on Base).

## Out of scope

- Cross-rollup composability via OP Stack messaging (we don't use it).
- Base-specific precompiles or system contracts.
- L1 settlement-window monitoring (we don't need 7-day finalization for execution decisions).
