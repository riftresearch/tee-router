# Unit (Hyperunit) — Efficient Observation Strategy

Hyperunit (`api.hyperunit.xyz`, testnet `api.hyperunit-testnet.xyz`) is the bridge that moves Bitcoin to and from Hyperliquid spot. We use it for `unit_deposit` (BTC → HL spot credit) and `unit_withdrawal` (HL spot → BTC payout).

## What we observe

Two operations, each with a **two-leg observation** (chain + venue):

| Operation | Leg 1 (chain) | Leg 2 (venue) | Why both |
|---|---|---|---|
| `unit_deposit` | BTC tx confirms to Hyperunit-issued deposit address | Hyperunit credits HL spot account with corresponding USDC/BTC | The chain confirmation tells us our money got there; the credit confirmation tells us the bridge actually picked it up and we can act |
| `unit_withdrawal` | HL spot withdrawal request acknowledged by Hyperunit | BTC tx lands at recipient address | Symmetric: Hyperunit ack tells us the request is in flight; on-chain confirms settlement |

Both legs need positive observation before T-router considers the step complete. Either leg's failure is a real failure.

## Native push mechanisms

### Bitcoin chain (already covered)

The deposit-leg of `unit_deposit` is a Bitcoin transaction to a Hyperunit-issued address. This is observed by the existing Bitcoin backend via ZMQ + Esplora (see `chains/bitcoin.md`). No new infrastructure needed — just ensure the Hyperunit-issued addresses are registered in `WatchStore` at the moment T-router materializes the step.

### Hyperunit REST API

Hyperunit exposes a REST API. Based on the [public documentation](https://docs.hyperunit.xyz/developers/api):

- **No public WebSocket / push channel** is documented.
- REST endpoints we use (canonical, per docs):
  - `GET /gen/{src_chain}/{dst_chain}/{asset}/{dst_addr}` — generate a deposit address. Returns `{address, signatures{node-id: base64-sig}, status}`.
  - `GET /operations/{address}` — look up the operations associated with a wallet/protocol address. Returns `{addresses[], operations[]}` where each `operations[]` entry has `operationId`, `state`, txids, amounts, timing fields.
  - `GET /v2/estimate-fees` — current per-chain fee schedule.

The polling here is *acceptable design*, not a workaround — there is no push surface. What we control is the cadence (5–15s per active operation) and that it stays inside Sauron, never in T-router's hotpath.

> **Endpoints that don't exist.** Earlier code referenced
> `GET /v2/operations/deposit/{id}` and `GET /v2/operations/withdrawal/{id}`.
> Those routes are not in the docs and probing the live API returns HTTP 200
> with empty bodies for any synthetic ID — they don't serve operation data.
> All lookups now go through `/operations/{address}`. The hyperunit-client
> methods that hit those routes have been removed.

## Canonical operation state machine

Per the [Hyperunit docs](https://docs.hyperunit.xyz/developers/api/operations), `state` (camelCase on the wire) progresses through these values:

**Deposit lifecycle:**
```
sourceTxDiscovered → waitForSrcTxFinalization → buildingDstTx
  → additionalChecks → signTx → broadcastTx → waitForDstTxFinalization
  → done
```

**Withdrawal lifecycle:**
```
sourceTxDiscovered → waitForSrcTxFinalization → readyForWithdrawQueue
  → queuedForWithdraw → done
```

**Terminal failure:** `failure` (for either operation type).

Earlier internal documentation invented states (`pending`, `confirmed`, `credited`, `failed`, `requested`, `processing`, `broadcast`) that the API never emits — those have been removed from code and docs. The Rust enum `UnitOperationState` in `crates/hyperunit-client/src/lib.rs` maps the real wire strings into typed variants and classifies them into `Pending / Active / Terminal{Success,Failure}` buckets.

## Guardian signature verification

Every `/gen/...` response carries a `signatures` object with one base64-encoded ECDSA P-256 signature per guardian node. Per the [guardian-signatures spec](https://docs.hyperunit.xyz/developers/api/generate-address/guardian-signatures):

- **Algorithm:** ECDSA over P-256, SHA-256 hash, signature encoded as base64 64-byte `R || S`.
- **Payload template (legacy, BTC/SOL):** `{nodeId}:{destinationAddress}-{destinationChain}-{asset}-{address}-{sourceChain}-deposit`
- **Payload template (new, ETH/EVM `coinType`):** `{nodeId}:user-{coinType}-{destinationChain}-{destinationAddress}-{address}`
- **Quorum:** at least **2 of 3** guardian signatures must verify.
- **Guardian public keys** (uncompressed, `04 || X || Y`):
  - **Mainnet:** `unit-node`, `hl-node`, `field-node` (canonical keys pinned in `hyperunit_client::guardian::MAINNET_GUARDIANS`).
  - **Testnet:** `node-1`, `hl-node-testnet`, `field-node` (in `TESTNET_GUARDIANS`).

`hyperunit_client::generate_address` verifies the guardian quorum before returning, so callers cannot silently accept a forged address from a compromised egress path. A bad upstream cannot substitute deposit addresses without forging at least 2 of 3 P-256 signatures over the canonical payload.

## Recommended efficient design

### Architecture

A new `bin/sauron/src/discovery/unit.rs` backend that:

1. Reads Hyperunit operation IDs from `ProviderOperationWatchStore` (filtered by `operation_type IN ('unit_deposit', 'unit_withdrawal')`).
2. For each active operation, runs a per-operation polling loop at 10s default cadence (configurable), tunable down to 5s for time-sensitive operations.
3. On state transition (e.g., `sourceTxDiscovered` → `broadcastTx` → `done`), emits `DetectorHint` with venue-specific kind.
4. On terminal state (`done`), emits and stops polling that operation.
5. On `failure`, emits a failure hint; T-router routes to refund.

### Deposit flow (`unit_deposit`)

1. T-router materializes the step → calls Hyperunit `GET /gen/...` to mint a deposit address. The hyperunit-client verifies guardian sigs before returning. T-router persists the address as a watched address (Bitcoin chain) AND a Hyperunit operation ID.
2. **Three concurrent observation paths:**
   - **Bitcoin mempool** (via ZMQ rawtx): user's BTC tx hits mempool → emit `DetectorHint { hint_kind: BridgeDepositOnChainObserved, confirmation_state: Mempool }`. T-router can already start anticipatory work.
   - **Bitcoin confirmed** (via ZMQ rawblock): tx lands in a block → emit `DetectorHint { hint_kind: BridgeDepositOnChainObserved, confirmation_state: Confirmed, block_height }`.
   - **Hyperunit credit** (via Sauron polling of `/operations/{address}`): operation `state` flips to `done` → emit `DetectorHint { hint_kind: BridgeDepositCredited, destinationTxHash }`.
3. T-router's verifier marks the step complete only when `BridgeDepositCredited` arrives. The on-chain hints are useful for telemetry and early UI confirmation but not for state-machine advancement.

### Withdrawal flow (`unit_withdrawal`)

1. T-router materializes the step → submits a withdrawal request on the HL side (action signed via hyperliquid-client). Hyperunit observes the HL withdrawal and creates an operation record visible at `/operations/{wallet_address}`.
2. **Two observation paths:**
   - **Hyperunit status** (Sauron polling of `/operations/{address}`): operation transitions through `sourceTxDiscovered` → `waitForSrcTxFinalization` → `readyForWithdrawQueue` → `queuedForWithdraw` → `done`. The `destinationTxHash` becomes available once Hyperunit broadcasts the payout. Emit `DetectorHint { hint_kind: WithdrawalAcknowledged, btc_txid }` once a txid appears.
   - **Bitcoin chain** (via ZMQ rawtx then rawblock): the BTC payout tx hits mempool then a block → emit `DetectorHint { hint_kind: WithdrawalSettled, confirmation_state }`.
3. T-router's verifier marks the step complete on `WithdrawalSettled` with `Confirmed` (configurable depth — for retail-size withdrawals 1–2 conf is fine).

### Polling cadence

- 5s while operation is in active state and < 5 minutes old.
- 10s after 5 minutes.
- 30s after 30 minutes.
- Alert + escalate if no terminal state after 2 hours (Hyperunit operations should not take this long).

This keeps the API call volume bounded even with thousands of active operations.

## Step-ID disambiguation

Hyperunit operation IDs are unique per request — strong disambiguator. The watch entry must carry both the Hyperunit operation ID and the corresponding `execution_step_id`; emitted hints reference the `execution_step_id` so T-router routes correctly.

For deposits specifically, the BTC address is single-use per operation — so address-based hint emission from the Bitcoin backend is also unambiguous, but should still be tagged with `execution_step_id` for symmetry.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| Hyperunit API unavailable | Sauron polling fails; mark backend degraded; alert. Bitcoin chain observations still work for the chain leg. T-router orders waiting on Hyperunit hints stall visibly. Resume on API recovery |
| Hyperunit credit takes much longer than expected | Sauron continues polling; T-router relies on configured timeouts. If exceeded, escalate to MIR |
| Hyperunit returns `failure` for a deposit (e.g., amount below minimum) | Emit `BridgeDepositFailed` hint; T-router triggers refund flow (BTC was sent and is held by Hyperunit; refund mechanism is Hyperunit's responsibility) |
| Hyperunit returns `failure` for a withdrawal | Emit `WithdrawalFailed`; HL credit was already debited at request time; T-router routes via the ExternalCustody refund path |
| BTC tx for deposit takes >1h to confirm (low fee?) | Keep watching; emit telemetry alert for slow confirmation |
| Egress / DNS attack substitutes a deposit address | Guardian signature verifier rejects: ≥2 of 3 P-256 sigs over the canonical payload must validate against the pinned mainnet/testnet pubkeys before `generate_address` returns. Bad upstreams cannot forge without compromising 2 guardian keys. |
| Address reuse (Hyperunit recycles addresses) | Should not happen per Hyperunit's design; if it does, our `execution_step_id` tagging on hints handles disambiguation |

## What's missing today

1. **No Hyperunit backend in Sauron.** All Hyperunit-side observation today flows through the generic `ProviderOperationObserverLoop` poking the DB. There's no API client polling Hyperunit's status endpoints.
2. **No `unit`-specific hint kinds** (`BridgeDepositCredited`, `WithdrawalAcknowledged`, `WithdrawalSettled` would all be added with the broader hint-kind enrichment).
3. **No coordination** between Bitcoin chain observation (which already works) and Hyperunit-side credit confirmation. Today, T-router conflates them and waits for a `PossibleProgress` hint that may or may not represent the right thing.

## Out of scope

- Hyperunit's internal settlement mechanics (we trust their state machine; we observe its outputs).
- Future Hyperunit WebSocket if/when it ships — we'd subscribe and demote polling to fallback.
- Other future cross-chain bridges (each gets its own venue doc).
