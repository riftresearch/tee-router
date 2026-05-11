# Sauron v2 — Build Spec

Synthesizes the architectural decisions from the 2026-05-11 design conversation. This document specifies what we're building, how it composes, and the order in which it lands. It supersedes any conflicting design statements in the per-chain and per-venue docs (`docs/sauron/chains/`, `docs/sauron/venues/`) — those describe per-chain/per-venue strategies that remain valid; this doc is the system-level spec they hang off of.

## Goals

1. **Sauron observes only.** T-router actor; Sauron observer. T-router never polls external state. (Already enforced by commit `494a53a`.)
2. **Per-chain indexer services with a uniform external API.** Chain-side transport idioms (EVM RPC, Bitcoin ZMQ, Hyperliquid WS) stay encapsulated inside their respective indexer services; Sauron sees only query/prune/subscribe with typed events.
3. **Sauron is thin.** Pure venue-observer layer that consumes indexer events and emits domain-tagged hints. No transport code in Sauron after the migration.
4. **No domain leak across the indexer boundary.** Indexers know about chains and events; they do not know about Sauron's watches, candidates, or workflow states.
5. **Concurrent multi-source observation.** Within each chain transport, push (real-time) and periodic full-poll (reconciliation) run concurrently and feed a deduplicated event stream — not active/standby failover.
6. **Hyperliquid is a chain, not a venue.** Same indexer + receipt-watcher treatment as EVM and Bitcoin.

## Non-goals

- Becoming a general-purpose blockchain indexer for sale.
- Replacing the Postgres CDC plumbing Sauron uses internally for watch reconciliation.
- Building HyperEVM observation in this spec (when we touch HyperEVM contracts, it slots in as another EVM indexer instance).
- Real-time push from Sauron to T-router (already exists; out of scope).

## Architecture: two tiers, uniform per chain

```
+----------------------------------------------------------+
|  Tier 2: Sauron (thin)                                   |
|                                                          |
|  Venue observers (Velora, Across, CCTP, HL Bridge,       |
|  HyperUnit, HL Trade...) compose typed clients to        |
|  Tier 1 services. Emit domain-tagged hints to T-router.  |
+----------------------------------------------------------+
                         |  typed Rust clients
                         v
+----------------------------------------------------------+
|  Tier 1: per-chain transport services                    |
|                                                          |
|  Each chain has TWO services:                            |
|    - Indexer (FilterStream): broad ingest + query/prune/ |
|      subscribe. Owns chain-native push transport.        |
|    - Receipt watcher (ByIdLookup): wait for known ID's   |
|      result to land. Owns chain-native by-id lookup.     |
|                                                          |
|  Chains: Ethereum, Base, Arbitrum, Bitcoin, Hyperliquid  |
+----------------------------------------------------------+
                         |  chain-native protocols
                         v
                  (RPC / WS / ZMQ / REST)
```

## Components to build / refactor

### 1. EVM indexer (refactor existing `evm-token-indexer`)

**Status:** exists; refactor required.

**Refactor goals:**
- Strip Sauron-domain vocabulary from the API. Remove `PUT /watches`, `POST /candidates/materialize`, `GET /candidates/pending`, `POST /candidates/:id/{mark-submitted,release,discard}`. Remove `active_deposit_watch` and `deposit_candidate` tables.
- Restore as pure primitive:
  - `GET /transfers?to=<addr>&token=<addr>&from_block=<n>&min_amount=<bigint>&max_amount=<bigint>&limit=<n>&cursor=<opaque>` — paginated query with cursor.
  - `POST /prune` with body `{relevant_addresses: [Hex], before_block: u64}` — async pruning instruction.
  - `WS /subscribe` accepting filter `{token_addresses, recipient_addresses, min_amount?, max_amount?}` — pushes matched transfers as ingested.
- Keep the chain-wide ingest model (`erc20_transfer_raw`, indexed on `(chain_id, to_address, token_address, block_number)`).
- Per-chain instance (one process per EVM chain we observe).

**Out of scope for this refactor:** ingesting non-Transfer event types. Future work to add a generic `/logs?address=...&topic=...` query, but ERC-20 Transfer covers all current observation needs.

### 2. Bitcoin indexer (new)

**Status:** does not exist; build.

**Implementation decision (settled):** thin Rust service shim in front of electrs/Esplora. We already operate an electrs instance; building the shim is fast and avoids a from-scratch Rust indexer for v1. Can be replaced with a pure-Rust indexer later if needed without changing the external API.

**Shape:**
- Same API verbs as EVM indexer:
  - `GET /tx_outputs?address=<addr>&from_block=<n>&min_amount=<sats>&limit=<n>&cursor=<opaque>` — paginated query.
  - `WS /subscribe` accepting filter `{addresses, min_amount?}` — pushes matched outputs as ingested.
- **`POST /prune` deferred for v1.** Bitcoin doesn't have the same storage-growth pressure as EVM token indexers (Bitcoin volume is much lower; electrs's own retention is sufficient for our needs). Add when/if storage becomes an issue.
- Internal transport: ZMQ `pubrawblock` + `pubrawtx` from Bitcoin Core; electrs/Esplora HTTP for indexed-lookup backfill; Bitcoin Core RPC for verification.
- Reorg-rescan depth: 6 blocks.

### 3. Hyperliquid indexer (new — finalized)

**Status:** does not exist; build. API and ingestion semantics finalized from research in `docs/sauron/hyperliquid-api-reference.md` (read that for HL field shapes, rate limits, gotchas).

**Guiding principle:** in EVM, "transfers" capture all value flow — pure ERC-20 sends AND DeFi swap settlements both surface as `Transfer` events. The HL indexer exposes an analogous unified value-flow primitive: `/transfers` captures every event where a user's balance changed, regardless of mechanism.

#### Endpoints

```
GET  /transfers?user=<addr>&from_time=<ms>&limit=<n>&cursor=<opaque>
GET  /orders?user=<addr>&from_time=<ms>&limit=<n>&cursor=<opaque>
POST /prune {relevant_users: [Address], before_time_ms: i64}
WS   /subscribe  (frame: {action:"subscribe", filter:{users, kinds:[transfers|orders]}})
```

#### Ingested HL channels (all per-user)

| HL channel | Routed to | Notes |
|---|---|---|
| `userFills` | `/transfers` | One row per match; partial fills emit separately. TWAP slice fills (`hash="0x0..."`) routed to separate handling — out of scope v1. |
| `userNonFundingLedgerUpdates` | `/transfers` | All `delta.type` variants: deposit, withdraw, internalTransfer, accountClassTransfer (spot↔perp), spotTransfer, spotGenesis, liquidation, vault*, rewardsClaim, cStakingTransfer. |
| `userFundings` | `/transfers` | Hourly per perp position. Real USDC flows; must be tracked. |
| `orderUpdates` | `/orders` | Keyed by `oid`; emits `status` transitions including canonical `"filled"`. |
| `user` channel (a.k.a. userEvents) — `nonUserCancel` subset only | `/orders` | Protocol-side cancel reasons not surfaced by `orderUpdates`. Optional v1; defer if it complicates. |

Out of scope v1: `userTwapSliceFills`, `activeAssetData`, `activeAssetCtx`, `spotState`, vault-specific channels beyond what `userNonFundingLedgerUpdates` already provides.

#### Unified `/transfers` event schema

```rust
struct HlTransferEvent {
    user: Address,
    time_ms: i64,
    kind: HlTransferKind,
    asset: String,           // "BTC" (perp) | "BTC/USDC" or "@n" (spot) | "USDC" (ledger moves)
    market: HlMarket,        // Spot | Perp | NA (non-trade)
    amount_delta: String,    // signed decimal; positive = credit
    fee: Option<String>,
    fee_token: Option<String>,
    hash: HashHex,           // L1 hash; "0x0..." for TWAP slices
    metadata: HlTransferMetadata,
}

enum HlTransferKind {
    Fill { oid, tid, side, px, sz, crossed, dir, closed_pnl, start_position, builder_fee },
    Funding { szi, funding_rate },
    Deposit,                                                  // ledger.deposit
    Withdraw { nonce },                                       // ledger.withdraw
    InternalTransfer { destination },                         // ledger.internalTransfer
    SubAccountTransfer { destination },                       // ledger.subAccountTransfer
    AccountClassTransfer { to_perp },                         // ledger.accountClassTransfer
    SpotTransfer { destination, native_token_fee, nonce },    // ledger.spotTransfer
    SpotGenesis,
    Liquidation { liquidated_ntl_pos, account_value, leverage_type, liquidated_positions },
    Vault(VaultEventKind),
    RewardsClaim,
    CStakingTransfer { is_deposit },
}
```

#### Spot vs perp discrimination

Discriminator is the `coin` field on `userFills`. At indexer startup:

1. `POST /info {"type":"meta"}` → load perp universe.
2. `POST /info {"type":"spotMeta"}` → load spot universe (canonical names and `@n` index mappings).
3. Classify each fill: membership in perp universe → `Perp`; matches `<BASE>/<QUOTE>` or `@\d+` → `Spot`.

Refresh meta + spotMeta every ~10 min to pick up new listings. `feeToken` is a hint but not authoritative.

For ledger updates, `delta.type` is the discriminator (no meta lookup needed).

#### Internal transport

WebSocket pool against `wss://api.hyperliquid.xyz/ws`. Per-user subscription set: `userFills` + `userNonFundingLedgerUpdates` + `userFundings` + `orderUpdates` + (optional) `user`.

REST `/info` fallback when WS unavailable; reconnect + backfill via `/info` with `startTime` cursor. Pagination is timestamp-based — advance `startTime` to the last returned event's `time` until results stop.

#### Cross-chain Bridge2 correlation — NOT the indexer's job

The HL indexer does not correlate HL-side deposit/withdraw events with Arbitrum-side Bridge2 events. HL's `deposit`/`withdraw` deltas do not carry the Arbitrum tx hash; correlation lives in the HL Bridge venue observer in Sauron, which joins:

- HL-side ledger events from this indexer (with `nonce` on withdrawals)
- Bridge2 events from the Arbitrum EVM indexer (filtered to `0x2df1c51e09aecf9cacb7bc98cb1742757f163df7`)

Join key: `nonce` for withdrawals; `(user, amount, time-window)` for deposits (no shared id).

**Implication for the Arbitrum EVM indexer:** add Bridge2 to its watched-contract set — `RequestedWithdrawal`, `FinalizedWithdrawal`, `InvalidatedWithdrawal` events plus inbound USDC `Transfer` to the bridge address. Additive change to the EVM indexer's filter set, no schema change.

**v1 implementation note:** until the EVM token indexer grows a generic `/logs`
primitive, the HL Bridge observer uses the existing ERC-20 Transfer surface only.
Deposits join HL `deposit` ledger entries with Arbitrum USDC transfers into
Bridge2 by `(user, amount, 30-minute time window)`. Withdrawals emit
`HlWithdrawalAcknowledged` from the HL `withdraw` ledger entry, then
`HlWithdrawalSettled` when a matching Arbitrum USDC payout transfer from
Bridge2 to the recipient appears within the configured 30-minute window. The HL
nonce is carried in the acknowledgement/settlement evidence, but Bridge2
`InvalidatedWithdrawal` is not directly observed in v1; missing payouts time out
through the existing manual-intervention path. Task #5 will replace this with a
nonce-keyed Bridge2 log join once `/logs` exists.

#### WS subscription scaling — the load-bearing operational concern

HL caps **10 unique users in user-specific subscriptions per IP**. With one IP we can watch ≤10 HL accounts simultaneously (each with 4–5 subscriptions = 40–50 total subs per IP, well under the 1000 cap).

If active HL account cardinality exceeds 10, two options:

- **Option A — Egress IP pool.** Allocate N egress IPs (cloud NAT gateways, VPC endpoints, or SOCKS proxies). N IPs → 10×N unique users. Partition users across IPs.
- **Option B — Account consolidation.** Design executor model to use ≤10 unique HL accounts total. Cleaner architecturally if feasible.

**Decision needed before phase 5:** confirm actual cardinality of active HL accounts under target load.

#### Backfill ceiling

`userFillsByTime` reaches only the **10,000 most recent fills** per user via `/info`. For cold-starting a long-active account, anything older is unreachable from HL's public APIs.

Mitigation: live ingestion captures everything from the moment we start watching. New executor vaults start with zero fills, so cold-start ceiling has no v1 impact. Stale watches re-registered after long absence are an edge case — accept truncation horizon for v1.

#### Storage

Postgres, range partitioned by `user`. Two tables:

- `hl_transfers` — ordered by `(user, time_ms, kind_discriminator, idx_within_batch)`; secondary indexes on `(user, time_ms)` and `(oid)` where applicable.
- `hl_orders` — keyed by `(user, oid)`; latest `status` + `statusTimestamp`.

Dedup keys:
- Fills: `(user, tid)` canonical unique
- Withdraws: `(user, nonce)`
- Deposits: `(user, hash, usdc)`
- Other ledger: `(user, time, hash, type)` — `hash` is NOT unique across deltas of one L1 tx

#### Resilience (MultiSource per build-spec)

- **Source A (primary):** WS subscriptions per user, push.
- **Source B (reconciler):** periodic `/info` poll per user (~30 min cadence). Catches anything WS may have missed.
- Dedup by canonical keys above.

#### REST `/info` rate-limit headroom

1200 weight/min per IP. Per-user reconciler poll (4 endpoints @ weight 20+per-item) ≈ 80 weight per poll. At 30-min cadence × 10 users per IP → ~27 weight/min average. Comfortable headroom for catch-up bursts.

#### Action items before phase 5

1. **Confirm executor HL account cardinality** under target load → settles Option A vs B for IP pool.
2. **Sandbox-verify** undocumented items from `hyperliquid-api-reference.md` (max result sizes for ledger/funding, WS throttle behavior, end-to-end push latency).
3. **Add Bridge2 events** to the Arbitrum EVM indexer's filter set.

### 4. Per-chain receipt watchers (new across the board)

**Status:** does not exist as a dedicated component; built piecemeal across Sauron + T-router today.

**Shape:** one service per chain (or one consolidated multi-chain service — TBD), exposes:
- `POST /watch {chain, id}` — register interest in a tx hash / oid / withdrawal id.
- `WS /subscribe` — receive `{id, status: pending|confirmed, receipt: <chain-typed payload>}` events as IDs resolve.
- `DELETE /watch/{id}` — cancel a watch.

**Per-chain internal mechanism:**
- **EVM (Eth/Base/Arb):** subscribe to `newHeads` over WS; on each head, call `eth_getBlockByNumber(num, false)` to get tx hashes; intersect with pending set; for matches, call `eth_getTransactionReceipt(hash)` and decode logs. Gap-fill for missed heads. Polling fallback: `eth_blockNumber` + same loop.
- **Bitcoin:** ZMQ `pubrawblock` → iterate block txs → intersect with pending txid set → emit confirmation event with depth. Polling fallback: `getblockchaininfo` + `getblock`.
- **Hyperliquid:** `userFills` and `userNonFundingLedgerUpdates` subscriptions → intersect with pending oid / withdrawal id set. Polling fallback: `/info type=orderStatus`.

The receipt watcher and indexer COULD be combined into one service per chain (they share the chain RPC connection), but the operational profiles differ:
- Indexer: heavy stateful (Postgres, broad ingest), restarts slowly, deploys carefully.
- Receipt watcher: thin in-memory pending-id store, restarts in seconds.

**Recommendation:** separate processes per chain unless operational simplicity (one process per chain) wins out for a specific chain. Bitcoin and HL might naturally combine since their transports overlap.

### 5. Sauron retrofit (delete a lot)

**Status:** existing 5,051 LOC service; significant refactor.

**Refactor plan:**
- **Delete** `bin/sauron/src/discovery/bitcoin.rs` (1,909 LOC). Replace with thin typed client to Bitcoin indexer + Bitcoin receipt watcher.
- **Delete** `bin/sauron/src/discovery/evm_erc20.rs` (2,073 LOC). Replace with thin typed client to EVM indexer (per chain) + EVM receipt watcher.
- **Add** `bin/sauron/src/discovery/hyperliquid.rs` — thin typed client to HL indexer + HL receipt watcher; emits HL-venue-tagged hints.
- **Refactor** `bin/sauron/src/discovery/mod.rs` to be a venue-observer registry, not a chain-discovery coordinator.
- **Keep** `runtime.rs`, `watch.rs`, `provider_operations.rs`, `cdc.rs`, `router_client.rs`, `config.rs` — these are about Sauron's role (observation orchestration + hint emission to T-router), not about chain transport.
- Net Sauron LOC after refactor: target ~1,500 (down from 5,051).

## API contract (uniform across chains)

All indexers expose:

```
GET  /<events>?<chain-specific filter params>&from_<cursor_field>=<n>&limit=<n>&cursor=<opaque>
POST /prune {relevant_<id_kind>: [<id_type>], before_<cursor_field>: <n>}
WS   /subscribe   (frame: {action: "subscribe", filter: <chain-specific>}, replies: matched events as ingested)
WS   /subscribe   (frame: {action: "unsubscribe", subscription_id: ...})
```

All receipt watchers expose:

```
POST   /watch {chain: <id>, id: <chain-specific>}
DELETE /watch/{id}
WS     /subscribe (replies: {id, status, receipt}: as IDs resolve)
```

**What's uniform:** verbs, lifecycle, cursor pagination, error model, auth model, health endpoint, metrics shape.

**What's chain-specific:** event types, filter parameters, id types. These are typed per-chain in the Rust client crates; the wire format is JSON with chain-specific schemas.

## Trait surface (consumer-side, in Sauron)

```rust
trait FilterStream {
    type Filter;
    type Event;
    type Cursor;

    async fn query(&self, filter: Self::Filter, range: Range<Self::Cursor>)
        -> Result<Vec<Self::Event>, Error>;
    async fn subscribe(&self, filter: Self::Filter)
        -> Result<impl Stream<Item = Self::Event>, Error>;
    async fn prune(&self, ids: Vec<Self::Id>, before: Self::Cursor)
        -> Result<(), Error>;
}

trait ByIdLookup {
    type Id;
    type Receipt;
    async fn watch(&self, id: Self::Id) -> Result<impl Future<Output = Self::Receipt>, Error>;
}
```

Implementations:

| Chain | `FilterStream` impl | `ByIdLookup` impl |
|---|---|---|
| Ethereum | `EvmIndexerClient<chain=1>` (Filter = transfer params; Event = `EvmTransfer`) | `EvmReceiptWatcherClient<chain=1>` (Id = `TxHash`; Receipt = `(Receipt, Vec<Log>)`) |
| Base | `EvmIndexerClient<chain=8453>` | `EvmReceiptWatcherClient<chain=8453>` |
| Arbitrum | `EvmIndexerClient<chain=42161>` | `EvmReceiptWatcherClient<chain=42161>` |
| Bitcoin | `BitcoinIndexerClient` (Filter = address+amount; Event = `TxOutput`) | `BitcoinReceiptWatcherClient` (Id = `TxId`; Receipt = `(tx, confirmations)`) |
| Hyperliquid | `HlIndexerClient` (Filter = user+channels; Event = `Fill\|LedgerUpdate\|OrderUpdate`) | `HlReceiptWatcherClient` (Id = `Oid` or `WithdrawalId`; Receipt = chain-typed) |

Venue observers compose:

```rust
struct VeloraObserver<E: ByIdLookup<Id = TxHash, Receipt = EvmReceipt>> {
    evm_chains: HashMap<ChainId, E>,
}

struct AcrossObserver<O, D>
where O: ByIdLookup, D: FilterStream<Filter = LogFilter, Event = EvmLog>
{
    origin: HashMap<ChainId, O>,
    destination: HashMap<ChainId, D>,
}

struct HyperliquidBridgeObserver<Arb: ByIdLookup, Hl: FilterStream> {
    arbitrum: Arb,
    hyperliquid: Hl,
}
```

## MultiSource pattern (within each indexer / receipt watcher)

Push and periodic full-poll run concurrently; events deduplicated by chain-specific natural key.

```rust
struct MultiSource<E, K: Hash + Eq + Clone> {
    sources: Vec<Box<dyn EventSource<E>>>,
    key_of: fn(&E) -> K,
    seen: LruCache<K, ()>,  // sliding window, ~30 min
}
```

Per chain, the sources are typically:
- Source A: real-time push (WS subscription, ZMQ stream, HL user channels)
- Source B: periodic full-poll over watch set (hourly cadence by default)

Future:
- Source C: independent secondary RPC / third-party indexer (resilience)

The MultiSource is itself an `EventSource<E>` and composes. Consumers see one stream; the dedup ensures each unique event is delivered exactly once even when sources overlap.

## Cross-cutting changes (load-bearing)

### a) `execution_step_id` propagation through watches and hints

**Problem:** today, `ProviderOperationWatchEntry` (`bin/sauron/src/provider_operations.rs:58`) does not store `execution_step_id`. When a step is superseded and a new step takes its place, both old and new operations end up in the watch store sharing the same address; emitted hints are tagged with the operation ID, not the step ID, causing T-router's verifier to reject hints with "belongs to step A, not B." This is the root cause of the 127-order stall observed in the 2026-05-11 loadgen run.

**Fix:**
- Add `execution_step_id` column to the watch query (`provider_operations.rs:16-31`).
- Add `execution_step_id` field to `ProviderOperationWatchEntry`.
- Propagate `execution_step_id` through every emitted hint (Sauron-side `DetectorHint` carries it; T-router's verifier matches against the live step).
- When a step is superseded (status change observable via CDC), Sauron drops the watch for the old step and registers for the new.

**Scope:** Sauron-side change only; touches watch entry, query, and hint emission. T-router's verifier already uses `execution_step_id` as the routing key — this fix just feeds it the correct value.

### b) `ProviderOperationHintKind` enrichment

**Problem:** today the enum (`crates/router-core/src/models.rs:739-757`) has only `PossibleProgress`. Every hint is generic; verifiers cannot fast-path "definitively confirmed" vs "maybe progressed." Off-chain coordinators (Circle attestation, HyperUnit credit) emit hints indistinguishable from chain observations.

**Fix:** Add chain-event-typed and venue-event-typed variants. Initial set:

```rust
enum ProviderOperationHintKind {
    PossibleProgress,                // existing fallback / observer-loop poke

    // Chain-direct
    ChainTxConfirmed,                // generic: a known tx hash now has a receipt
    ErcTransferObserved,             // Transfer to/from a watched address
    BtcDepositObserved,              // mempool or confirmed BTC deposit

    // Venue-specific
    VeloraSwapSettled,
    AcrossOriginDeposited,
    AcrossDestinationFilled,
    CctpBurnObserved,
    CctpAttestationReady,
    CctpReceiveObserved,
    HlBridgeDepositObserved,         // Arbitrum-side
    HlBridgeDepositCredited,         // HL-side
    HlTradeFilled,
    HlTradeCanceled,
    HlWithdrawalAcknowledged,
    HlWithdrawalSettled,             // Arbitrum-side payout
    HyperUnitDepositCredited,
    HyperUnitWithdrawalSettled,
}
```

Each variant carries chain-typed evidence (block hash, log index, fill payload, etc.) sufficient for the verifier to reach a deterministic decision without re-querying.

## Migration plan (ordered, incremental, no big-bang)

The migration is tractable in 6 phases. Each phase ships independently and leaves the system functional.

### Phase 0: Foundations (must land first)

- Add `execution_step_id` to `ProviderOperationWatchEntry` and propagate through current Sauron hint emission. Eliminates the 127-order stall class. **Highest priority.**
- Define `ProviderOperationHintKind` v2 enum + serde compat. Existing emitters keep using `PossibleProgress`; new code paths use specialized variants.

### Phase 1: Refactor evm-token-indexer to pure primitive

- Add new endpoints (`GET /transfers`, `POST /prune`, `WS /subscribe`) alongside existing ones.
- Add `EvmIndexerClient` typed client crate.
- No Sauron changes yet.

### Phase 2: Build EVM receipt watcher service

- New service per EVM chain: newHeads sub + pending hash store + WS push API.
- `EvmReceiptWatcherClient` typed client.
- T-router gains the ability to "hand off" submitted tx hashes to the receipt watcher (via Sauron, per the actor/observer split).

### Phase 3: Sauron retrofit (EVM)

- Replace `bin/sauron/src/discovery/evm_erc20.rs` usages with `EvmIndexerClient` + `EvmReceiptWatcherClient`.
- Implement Velora, Across, CCTP venue observers using the new typed clients.
- Delete the old `evm_erc20.rs` discovery backend.
- Remove old endpoints from `evm-token-indexer`.

### Phase 4: Bitcoin indexer + Sauron retrofit (BTC)

- Build Bitcoin indexer service (electrs-shim or pure Rust — decide before starting).
- Build Bitcoin receipt watcher.
- Replace `bin/sauron/src/discovery/bitcoin.rs` with `BitcoinIndexerClient` + `BitcoinReceiptWatcherClient`.
- Implement HyperUnit deposit-leg via the BTC indexer client.
- Delete the old `bitcoin.rs` discovery backend.

### Phase 5: Hyperliquid indexer + receipt watcher + Sauron retrofit (HL)

API + ingestion semantics finalized (component §3 + `docs/sauron/hyperliquid-api-reference.md`).

- Build HL indexer service (WS pool + Postgres + REST/WS API). Requires resolving HL account cardinality (Option A: egress IP pool vs Option B: account consolidation — see §3).
- Add Bridge2 events to the Arbitrum EVM indexer's filter set (additive, can happen in phase 1 or 3).
- Build HL receipt watcher (`oid`-keyed via `orderUpdates`).
- Implement HL Bridge, HL Trade, HL Spot Trade venue observers in Sauron — bridge observer joins HL indexer (HL-side ledger) + Arbitrum EVM indexer (Bridge2 events).
- This phase eliminates the original loadgen-stall failure mode.

### Phase 6: MultiSource concurrent-poll within each indexer

- Add periodic full-poll source to each indexer (alongside push) with deduplication.
- Initially low-priority — push is enough for steady-state correctness; full-poll is a belt-and-suspenders reconciliation.
- Defer until phases 1-5 are stable.

## Decisions still open

1. ~~**Bitcoin indexer implementation:** thin Rust shim over electrs/Esplora vs pure Rust indexer.~~ **SETTLED:** thin shim over electrs for v1; revisit if/when storage or feature needs justify pure-Rust replacement.
2. ~~**Hyperliquid indexer API + ingestion semantics**~~ **SETTLED** via component §3 + `docs/sauron/hyperliquid-api-reference.md`. Remaining HL-related decisions: **HL account cardinality under load** (egress IP pool vs account consolidation) and **sandbox verification** of undocumented HL behaviors. Both are operational/empirical, not architectural — phase 5 can start with current spec and these items resolved as the build progresses.
3. **Indexer + receipt watcher separation:** separate processes per chain or merged? Default position: separate for EVM, possibly merged for BTC and HL where transports overlap. Revisit after phase 2 is built.
4. **Pruning cadence:** how often does Sauron send the "ever-mattered set" to each indexer? Default: hourly. Revisit after measuring storage growth. (BTC has no prune in v1, so this only applies to EVM and HL.)
5. **Receipt watcher retention:** how long do we hold an entry in the pending set before declaring it lost? Default: 1 hour with alert; configurable per chain. T-router's MIR backstop catches longer cases.

## Out-of-scope clarifications

- **HyperEVM:** when we touch HyperEVM contracts, treat as another EVM chain — slot in another `EvmIndexerClient` and `EvmReceiptWatcherClient` instance. No spec change required.
- **Solana / future chains:** same pattern (build chain-specific indexer + receipt watcher conforming to the uniform API).
- **Replacing third-party off-chain APIs:** Circle attestation API, HyperUnit REST, HL REST `/info` — these stay external. Sauron-level polling against them is acceptable per the actor/observer split (polling is fine inside Sauron; never in T-router's hotpath).
- **Building a managed indexer business:** out of scope; the architectural shape just happens to be one that could become one if we ever wanted.

## Success criteria

When this spec is fully built, the following are true:

1. Loadgen-fast 10k drains to 100% with zero false-positive refunds and zero MIR caused by missed hints. (Today: 98.7% drain with 127 stalls due to step-supersede + missing HL backend.)
2. Sauron LOC drops from 5,051 to ~1,500 (target — actual TBD after refactor).
3. Adding a new EVM chain requires zero Sauron code change — just stand up an indexer + receipt watcher instance.
4. Adding a new venue requires composing existing typed clients in a new `Observer` struct in Sauron — no transport plumbing.
5. Each chain transport service has independent metrics, health, and deploy lifecycle. An HL outage does not look like "Sauron is broken" in alerts.
6. The admin dashboard (or any other internal consumer) can query historical chain data via the same indexer APIs Sauron uses, without learning Sauron's domain model.
