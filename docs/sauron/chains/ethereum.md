# Ethereum (Mainnet) — Efficient Observation Strategy

## What we observe

- **ERC-20 `Transfer(from, to, value)` events** to watched addresses for: funding-vault detection, `unit_deposit` (when source asset is on Ethereum), `cctp_burn` source-side observation, `across_bridge` origin-side observation, and Velora swap settlement (the on-chain leg of any `universal_router_swap` step — see `venues/velora.md`).
- **CCTP `MessageSent` events** from the TokenMessenger on-chain (Circle CCTP source leg).
- **Across `V3FundsDeposited` events** on Across SpokePool (origin leg of an Across bridge).
- **Reorgs** within `EVM_REORG_RESCAN_DEPTH = 32` blocks (uniform constant — overkill for L2s, appropriate for L1).

We do *not* care about general blockchain state — only logs at known contract addresses with known topics, indexed by the topic that names the recipient (or the depositor for outbound bridge events).

## Native push mechanisms (currently MISSING)

Ethereum nodes support WebSocket subscriptions over JSON-RPC:

| Subscription | What it gives us | Currently used? |
|---|---|---|
| `eth_subscribe('logs', filter)` | New event logs matching a filter, pushed in real time as blocks arrive | **No** |
| `eth_subscribe('newHeads')` | New block headers as they arrive | **No** |
| `eth_subscribe('newPendingTransactions')` | Mempool tx hashes (provider-dependent reliability; expensive at L1 scale) | **No** — out of scope |

Today, Sauron polls `eth_getLogs` in 128-block windows. This costs RPC requests proportional to chain head movement, adds 5–30s latency per cycle, and is the single biggest unforced inefficiency in the EVM observation path.

## Pull / fallback mechanisms

| Mechanism | Use case |
|---|---|
| `eth_getLogs` HTTP | **Backfill** on cold start, after WS disconnect, or for a freshly-registered watch that needs historical lookup |
| Token Indexer gRPC (`ETHEREUM_TOKEN_INDEXER_URL`) | **Indexed** historical lookup (256-block window per `EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS`) — useful when blocks-back > our cursor |
| `eth_getBlockByNumber` | **Reorg detection**: compare our cursor's block hash to current chain |

## Recommended efficient design

**Primary path (push):**
1. Open a WebSocket connection to the Ethereum RPC endpoint (Alchemy/Infura/own node).
2. Subscribe to `newHeads` → drives the head cursor.
3. Subscribe to `eth_subscribe('logs', { address: [...watched_contracts], topics: [[Transfer.topic0, MessageSent.topic0, V3FundsDeposited.topic0]] })`.
   - One subscription per topic-set is fine; or one per topic with per-topic filtering — both work.
   - Filter `address` by the union of: USDC, USDT, WETH (for `Transfer`); CCTP TokenMessenger (for `MessageSent`); Across SpokePool (for `V3FundsDeposited`).
4. On each log received:
   - Match against the appropriate watch table (recipient for Transfers; depositor for outbound bridge events).
   - Emit `DetectorHint` tagged with venue-appropriate metadata (token, amount, log index, block hash).
5. Also subscribe to `newHeads` to detect reorgs (compare each new head's `parentHash` to our cursor's `hash`).

**Backfill path (one-shot):**
- On startup, on WS reconnect, or on new watch registration: do a single `eth_getLogs` for the relevant address+topic over `[cursor.height − EVM_REORG_RESCAN_DEPTH, latest]`.
- If the gap is wider than 128 blocks, use the token-indexer gRPC for the bulk window and switch to RPC for the recent tail.

**Reorg path:**
- On every new head: if `head.parentHash != cursor.hash`, walk back to common ancestor via `eth_getBlockByHash`, retract any logs whose block is no longer in the chain, then forward-scan to the new tip.

## Step-ID disambiguation

Same cross-cutting requirement as `audit.md` §B: emitted `DetectorHint` must include `execution_step_id` so T-router's verifier can route the hint to the right step even when address/topic match is ambiguous (e.g., a vault is reused across orders, or two superseded steps share an output recipient).

For Ethereum specifically:
- **`Transfer` to a vault address** is unambiguous *only* if the vault is single-use (one order). For reusable vaults (rare for our use case, but possible), the watch entry must carry the `execution_step_id` of the active step the watch was registered for.
- **`MessageSent` (CCTP)** carries a `nonce` field that we should index — pair `(nonce, source_chain)` is globally unique across CCTP and should be the disambiguator.
- **`V3FundsDeposited` (Across)** carries a `depositId` we should match against the operation's `provider_ref`.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| WS connection drops | Reconnect with jittered backoff (1s → 30s); on reconnect, backfill via `eth_getLogs` from `cursor.height − 32` |
| RPC node falls behind chain head | Detected via `newHeads` heartbeat; if no head for N seconds, trigger fallback to a secondary RPC URL |
| Indexer gRPC unavailable | Fall through to RPC `eth_getLogs` for the same window (slower, costs more) |
| Reorg deeper than 32 blocks | Alert and pause acting on Ethereum-side hints; on mainnet this is essentially a chain split |
| Watch registered after the deposit landed | Backfill on watch registration handles it (single bounded `eth_getLogs` call) |

## What's missing today

1. **No WebSocket support whatsoever in the EVM backend.** Pure HTTP polling. This is the biggest single perf win available — switching to WS subscriptions for `logs` and `newHeads` should drop EVM-side hint latency from 5–30s p50 to sub-second.
2. **No reorg-aware retraction.** The current backend rescans the last 32 blocks each cycle but does not emit "this previous hint is no longer valid" — T-router would need to learn that signal too.
3. **Per-chain reorg tuning** — Ethereum=32 is right; same constant for Base/Arbitrum is overkill (see `chains/base.md` and `chains/arbitrum.md`).
4. **Indexer integration is single-source per chain.** Should support a fallback indexer or multi-provider failover.

## Out of scope

- Full state proof verification (we trust the RPC; T-router's verifier re-checks the on-chain state independently for high-value events).
- Mempool monitoring (`newPendingTransactions`) — too noisy at L1 scale and not actionable for our use case.
- MEV / private mempool integration.
