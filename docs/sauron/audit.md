# Sauron Architectural Audit

Author: Claude (Opus 4.7) under c4syner orchestration
Date: 2026-05-11
Trigger: 10k loadgen-fast wedged at 98.7% complete with 127 orders awaiting hints that never arrived. T-router was healthy; Sauron was idle (CPU 0.02%, watch_count=0–2 at the stall). The architectural realignment (commit `494a53a`, removed router-side polling fallback) made Sauron's gaps visible for the first time.

## A. Top-Level Architecture

### Process model

Single-process Tokio service. Boots in `bin/sauron/src/main.rs` (49 lines) → `runtime.rs::run()`.

`runtime.rs` (1,398 LOC) launches **5 concurrent long-lived tasks** via `tokio::try_join!` at lines 113–119:

1. **CDC event task** — listens to PostgreSQL logical replication for `order_provider_operations`, `order_provider_addresses`, `deposit_vaults` mutations.
2. **Full reconcile loop** — periodic bulk refresh of watch set from replica DB, configurable interval (`SAURON_RECONCILE_INTERVAL_SECONDS`).
3. **Expiration prune loop** — removes stale watches; runs every 60s.
4. **Provider-operation observer loop** — polls `ProviderOperationWatchStore` every 5s, submits generic "possible progress" hints.
5. **Discovery backends** — per-blockchain block scanners that emit deposit-found hints.

### Persistence

- **Replica DB** (`router_replica_database_url`): read-only mirror of router-postgres. Sauron queries `order_provider_operations`, `order_provider_addresses`, `deposit_vaults`. 8 max conns (`runtime.rs:204`).
- **State DB** (`sauron_state_database_url`): dedicated; stores CDC replication cursors. 4 max conns (`runtime.rs:214`).
- **In-process** state: `WatchStore` and `ProviderOperationWatchStore` are `Arc<RwLock<HashMap>>`. Lost on restart; rebuilt via full reconcile.

## B. Watch Lifecycle (where the bug lives)

### Two watch types

- **Standard watches** (`WatchStore`, `watch.rs` 1,265 LOC) — funding vaults + unit deposits. Built from joined `order_provider_addresses` + `order_provider_operations`.
- **Provider-operation watches** (`ProviderOperationWatchStore`, `provider_operations.rs` 520 LOC) — in-flight bridge/swap/trade operations. Built from `order_provider_operations` only.

### Registration path

When T-router creates an execution step, it inserts an `order_execution_steps` row and a paired `order_provider_operations` row (with `execution_step_id` foreign key). The replica DB observes the commit immediately. Sauron picks it up two ways:

1. **CDC (preferred, real-time)** — `run_cdc_stream_once` (`runtime.rs:305-361`) decodes the logical message, queues a targeted refresh in `CdcRefreshPlan` (`runtime.rs:1048-1055`), then on the next `Commit` event calls `refresh_provider_operation` (line 1000-1006) to reload the single row and upsert into the store. Latency: <1s normally.
2. **Periodic full reconcile** — `run_full_reconcile_loop` (line 375-410) re-queries everything in `('submitted', 'waiting_external')` and revision-guarded replaces. Configurable interval. Exists as a CDC-gap backstop.

### THE BUG: step supersede is invisible to Sauron

When `QuoteRefreshWorkflow` supersedes a step (e.g., on `pre_execution_stale_provider_quote`):

1. Old step → terminal status; old `order_provider_operations.status` may stay `'submitted'`.
2. New step + new `order_provider_operations` row inserted, also `'submitted'`.
3. Sauron's full reconcile query (`provider_operations.rs:16-31`):
   ```sql
   WHERE operation_type IN (...) AND status IN ('submitted', 'waiting_external')
   ```
   has **no filter on `execution_step_id`**.
4. Both operations live in the watch store. Both reference the same address (e.g., HL bridge deposit address).
5. `ProviderOperationWatchEntry` (`provider_operations.rs:58`) **does not store `execution_step_id`**. The query joins through it but doesn't SELECT it.
6. When a backend observes an event matching the address, it emits a hint with `watch_id = operation_id`. T-router's verifier checks `step_id` and rejects: `provider_operation_hint.ignored ... reason="provider operation X belongs to step A, not B"`.
7. The hint for the *new* step never comes — Sauron has no way to disambiguate which operation the address-match belongs to, and even if it picked correctly, it would emit the wrong hint half the time.

This is the design defect the realignment exposed.

## C. Discovery backends — what actually exists

`runtime.rs:252-263` instantiates **only 4 backends**, all chain-level:

| Backend | File | LOC | What it watches |
|---|---|---|---|
| Bitcoin | `discovery/bitcoin.rs` | 1,909 | BTC deposits to watched addresses |
| Ethereum | `discovery/evm_erc20.rs` (chain=1) | shared | ERC-20 Transfers to watched addresses |
| Base | `discovery/evm_erc20.rs` (chain=8453) | shared | same |
| Arbitrum | `discovery/evm_erc20.rs` (chain=42161) | shared | same |

**No venue-specific backends.** Hyperliquid trades, CCTP attestations, Across fills, Velora swaps (the venue behind `universal_router_swap` step types), HyperUnit cross-chain settlement — none are observed by a dedicated backend. They depend solely on the generic `ProviderOperationObserverLoop`, which emits low-confidence "PossibleProgress" hints based on DB `updated_at` timestamps every 5–30s. That loop is a **poke**, not an observation.

### Bitcoin backend (`discovery/bitcoin.rs`)

- RPC client to `bitcoind` (`getblockheader`, `getblock`, `getrawtransaction`).
- Esplora HTTP client (`ELECTRUM_HTTP_SERVER_URL`) for indexed lookups.
- ZMQ raw transaction stream (`bitcoin_zmq_rawtx_endpoint`) for mempool visibility.
- Reorg depth: `BITCOIN_REORG_RESCAN_DEPTH = 6` (line 43).
- Indexed lookup with 30s timeout, jittered exponential backoff retries.
- Emits `DetectorHint` with `confirmation_state ∈ {Mempool, Confirmed}`.

### EVM backend (`discovery/evm_erc20.rs`)

- Alloy JSON-RPC HTTP client per chain.
- Optional token indexer gRPC (`*_TOKEN_INDEXER_URL`) for backfill (256-block window).
- `eth_getLogs` polling in 128-block windows (`EVM_MAX_LOG_SCAN_BLOCK_SPAN = 128`).
- Reorg depth: `EVM_REORG_RESCAN_DEPTH = 32` (uniform across chains — too conservative for L2s).
- Required confirmations: `EVM_REQUIRED_CONFIRMATION_BLOCKS = 1`.
- **No WebSocket subscriptions.** Pure HTTP polling.
- Matches `Transfer(address indexed from, address indexed to, uint256 value)` on watched recipients.

### Generic provider-operation observer loop (`runtime.rs:1168-1196`)

- Polls `ProviderOperationWatchStore` every 5s (`PROVIDER_OPERATION_OBSERVE_INTERVAL`).
- Per operation: computes fingerprint of state. If changed OR 30s elapsed (`PROVIDER_OPERATION_OBSERVATION_RESUBMIT_INTERVAL`), submits hint.
- Concurrency: 32 (`PROVIDER_OPERATION_OBSERVE_CONCURRENCY`).
- Submission timeout: 10s (`PROVIDER_OPERATION_HINT_SUBMIT_TIMEOUT`).
- Hint source: `"sauron_provider_operation_observation"`.
- This is a fallback heartbeat, not a real observation. Has no visibility into venue state.

## D. Hint kinds and emission

### Enum is impoverished

`ProviderOperationHintKind` (`crates/router-core/src/models.rs:739-757`):

```rust
pub enum ProviderOperationHintKind {
    PossibleProgress,
}
```

**One variant.** No `DepositConfirmed`, `BridgeAttested`, `SwapFilled`, `TradeFilled`, `WithdrawalSettled`. Every backend emits the same generic kind.

### Source labels

- `SAURON_DETECTOR_HINT_SOURCE = "sauron"` (`models.rs:948`) — emitted by Bitcoin and EVM discovery backends on actual detection.
- `PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE = "sauron_provider_operation_observation"` (`models.rs:946`) — emitted by the generic observer loop poking the DB.

## E. Weak spots ranked by severity

1. **No step-ID disambiguation in watches** — `ProviderOperationWatchEntry` has no `execution_step_id` field. Backends cannot tag hints with the right step. Cause of the 127-order stall.
2. **No Hyperliquid backend** — bridge deposits, trades, withdrawals are unwatched. Loadgen failures clustered here.
3. **No CCTP, Across, Velora backends** — same gap as HL. The first two need first-class venue observers; Velora is a special case where chain-level observation suffices but executor addresses must be in the watch set.
4. **EVM uses HTTP polling exclusively** — no WS subscriptions. Adds 5–30s latency per chain edge. Should have WebSocket `eth_subscribe('logs', ...)` as primary, HTTP for backfill/recovery.
5. **`ProviderOperationHintKind` has only one variant** — verifier loses context, can't fast-path "definitively confirmed" vs "maybe progressed."
6. **Reorg depth uniform across L1/L2** — Ethereum=32, Base=32, Arbitrum=32. L2 sequencer reorgs above 1 block are practically unheard of; cutting to 8–12 reduces backfill window and indexed-lookup pressure.
7. **Full reconcile is the only CDC-gap backstop** — if CDC stream stalls and the configured interval is long, gap visible only on next reconcile.
8. **Per-backend submission queue cap (10k)** — silent drop on overflow per `MAX_PENDING_DISCOVERY_SUBMISSIONS`. Should at minimum log + alert when nearing the cap.

## F. Surface-area inventory

| File | LOC | Purpose |
|---|---|---|
| `runtime.rs` | 1,398 | Main loop; CDC, reconcile, observer, backend orchestration |
| `watch.rs` | 1,265 | Standard watch store + reconciliation |
| `discovery/evm_erc20.rs` | 2,073 | All EVM chains: Ethereum, Base, Arbitrum |
| `discovery/bitcoin.rs` | 1,909 | Bitcoin scanning (RPC + Esplora + ZMQ) |
| `discovery/mod.rs` | 1,497 | Discovery trait + backend coordinator + hint submit logic |
| `provider_operations.rs` | 520 | Provider-operation watch store/repo |
| `cdc.rs` | 830 | PostgreSQL logical replication |
| `router_client.rs` | 276 | HTTP client to T-router |
| `config.rs` | 340 | Env/CLI parsing |
| `state_db.rs` | 30 | Schema migrations |
| `cursor.rs` | 91 | Checkpoint persistence |
| `error.rs` | 127 | Error types |
| Total | ~5,051 | |

## What this audit makes actionable

The next two doc sets — `chains/` and `venues/` — translate this audit into per-chain and per-venue efficient observation strategies. The cross-cutting fix (add `execution_step_id` to watches and hints) belongs in a follow-up PR; each chain/venue doc references it where relevant.
