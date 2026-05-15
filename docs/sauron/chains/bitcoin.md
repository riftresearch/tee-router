# Bitcoin — Efficient Observation Strategy

## What we observe

- **Deposit transactions** to specific watched addresses. Each watched address is bound to either:
  - A `unit_deposit` step (HyperUnit's BTC→HL bridge ingress) — provider-generated address, single use.
  - A funding vault (orders where the user's source asset is BTC).
- **Confirmation depth** — currently 6 blocks rescan window, so any reorg within 6 blocks is caught and replayed.

We do *not* care about most blockchain state — only outputs that pay an address in our watch set, and reorgs that retroactively invalidate them.

## Native push mechanisms (preferred)

Bitcoin Core supports three streams that we should treat as first-class:

| Stream | What it gives us | Latency | Currently used? |
|---|---|---|---|
| ZMQ `pubrawtx` | Mempool transactions as raw bytes, the moment Bitcoin Core sees them | ~50–200ms | **Yes** (`bitcoin_zmq_rawtx_endpoint`) |
| ZMQ `pubrawblock` | New confirmed blocks as raw bytes | ~50–200ms after first relay | **No** — block detection is poll-based |
| ZMQ `pubhashblock` | Block hash only (cheaper, then fetch via RPC) | same | **No** |

ZMQ is the canonical Bitcoin Core push mechanism; it is rock-solid in production and cheap to subscribe to. There is no reason the block stream isn't wired up — the rawtx stream already is.

## Pull / fallback mechanisms

| Mechanism | Use case | Notes |
|---|---|---|
| Esplora HTTP `/address/{addr}/utxo` and `/address/{addr}/txs` | **Catch-up** when Sauron starts cold or has been disconnected; **historical lookup** for a specific address that we just started watching | We have this (`ELECTRUM_HTTP_SERVER_URL`). 30s timeout, jittered exponential backoff |
| Bitcoin Core RPC `getblock`, `getrawtransaction`, `getblockchaininfo` | **Verification** of a tx fingerprint after we've seen it via push; **reorg detection** (height mismatch) | We have this. RPC is the source of truth for confirmations |

Polling Esplora repeatedly per address is *acceptable at the Sauron level* but should not be the primary trigger — push wins on latency and cost.

## Recommended efficient design

**Primary path (push):**
1. Subscribe to `pubrawblock` on startup → drives the confirmed-block cursor advance.
2. Subscribe to `pubrawtx` on startup → drives mempool-confirmation hint emission (these are weak hints, marked `Mempool` for the verifier to ignore on-chain risk).
3. On each new block from `pubrawblock`:
   - Decode block, extract tx outputs that pay any address in `WatchStore`.
   - Emit `DetectorHint { confirmation_state: Confirmed, block_height, tx_hash, output_index, amount }`.
   - Update cursor.
4. On each new tx from `pubrawtx`:
   - Decode, extract outputs paying watched addresses.
   - Emit `DetectorHint { confirmation_state: Mempool, tx_hash, output_index, amount }`.
   - T-router decides whether to act on a mempool hint based on amount/risk.

**Catch-up path (one-shot per address):**
- When CDC tells us a new watch was registered, do a single Esplora `/address/{addr}/txs` lookup to capture any deposits that landed *before* we knew about the address (race between user funding and our backend learning about the order).
- Bound to one call per address per registration. Not a polling loop.

**Reorg path:**
- On startup and every 60s, RPC `getblockchaininfo` and compare tip hash to our cursor.
- If divergence: re-scan back `BITCOIN_REORG_RESCAN_DEPTH = 6` blocks via `getblock`.
- Emit a special `Reorg { from_height, to_height }` event that the verifier uses to retract previously-confirmed hints if the underlying block is no longer in the chain.

## Step-ID disambiguation

Per-address watches in Bitcoin are nearly unambiguous because HyperUnit issues *fresh* deposit addresses per request. The address itself maps 1:1 to a step.

Cross-cutting fix needed (per `audit.md` §B): the watch entry must carry `execution_step_id` so the emitted hint references the correct step ID even when address reuse happens (e.g., if HyperUnit ever recycles addresses across superseded refresh attempts, or for funding vaults where the same vault may be re-funded across multiple orders).

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| Bitcoin Core ZMQ disconnects | Sauron-level reconnect with backoff; on reconnect, rescan from cursor – 6 |
| Esplora unavailable for catch-up | Fall through to RPC `getrawtransaction` / `getblock` directly (slower, no index, but works) |
| Bitcoin Core RPC unhealthy | Mark backend degraded, alert; T-router orders that need BTC observation will start to stall — this is the correct visible failure |
| Reorg deeper than 6 blocks | Logged and alerted; manual intervention required (this is essentially a chain split — should never happen on mainnet for our addresses) |
| Address registered AFTER deposit landed in mempool but before block | Catch-up Esplora call within a few seconds of registration handles it; if it lands in a block within that window, the next `pubrawblock` catches it anyway |

## What's missing today

1. No `pubrawblock` / `pubhashblock` subscription — block detection is currently poll-based on RPC `getblockchaininfo` cadence. Add ZMQ subscription to `pubrawblock` for symmetric push behavior.
2. Catch-up Esplora call is not directly tied to CDC watch-registration — it happens inside the periodic scan. Should be triggered immediately on watch creation for new addresses.
3. Per-step-id tagging on emitted `DetectorHint` (cross-cutting; see `audit.md` §B).

## Out of scope

- Full UTXO indexing (we delegate to Esplora).
- Lightning channel observation (no Lightning use case in scope today).
- Inscription / Ordinals filtering (irrelevant; we match on amount + address).
