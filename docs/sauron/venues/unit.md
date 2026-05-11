# Unit (HyperUnit) — Efficient Observation Strategy

HyperUnit (api.hyperunit.xyz) is the bridge that moves Bitcoin to and from Hyperliquid spot. We use it for `unit_deposit` (BTC → HL spot credit) and `unit_withdrawal` (HL spot → BTC payout).

## What we observe

Two operations, each with a **two-leg observation** (chain + venue):

| Operation | Leg 1 (chain) | Leg 2 (venue) | Why both |
|---|---|---|---|
| `unit_deposit` | BTC tx confirms to HyperUnit-issued deposit address | HyperUnit credits HL spot account with corresponding USDC/BTC | The chain confirmation tells us our money got there; the credit confirmation tells us the bridge actually picked it up and we can act |
| `unit_withdrawal` | HL spot withdrawal request acknowledged by HyperUnit | BTC tx lands at recipient address | Symmetric: HyperUnit ack tells us the request is in flight; on-chain confirms settlement |

Both legs need positive observation before T-router considers the step complete. Either leg's failure is a real failure.

## Native push mechanisms

### Bitcoin chain (already covered)

The deposit-leg of `unit_deposit` is a Bitcoin transaction to a HyperUnit-issued address. This is observed by the existing Bitcoin backend via ZMQ + Esplora (see `chains/bitcoin.md`). No new infrastructure needed — just ensure the HyperUnit-issued addresses are registered in `WatchStore` at the moment T-router materializes the step.

### HyperUnit API

HyperUnit exposes a REST API. Based on public documentation and what we've integrated:

- **No public WebSocket / push channel** is documented. (If one exists, it's not yet announced; check periodically.)
- REST endpoints for: address generation, deposit/withdrawal status, fee quotes, supported assets.

This means the HyperUnit-side observation is **necessarily polling**, but at the Sauron level — never in T-router's hotpath.

## Pull mechanisms (Sauron-level)

| Endpoint | Returns | Use |
|---|---|---|
| `GET /v2/operations/deposit/{id}` (or address-keyed equivalent) | deposit status: `pending`, `confirmed`, `credited`, `failed`, with txids and HL credit hash | Poll while the deposit is in-flight to detect HL-side credit |
| `GET /v2/operations/withdrawal/{id}` | withdrawal status: `requested`, `processing`, `broadcast`, `confirmed`, `failed`, with the BTC payout txid once available | Poll while a withdrawal is in-flight |
| `GET /v2/status` (or similar) | service health | Liveness check; alert if HyperUnit reports degraded |

The polling here is *acceptable design*, not a workaround — there is no push surface. What we control is the cadence (5–15s per active operation) and that it stays inside Sauron.

## Recommended efficient design

### Architecture

A new `bin/sauron/src/discovery/unit.rs` backend that:

1. Reads HyperUnit operation IDs from `ProviderOperationWatchStore` (filtered by `operation_type IN ('unit_deposit', 'unit_withdrawal')`).
2. For each active operation, runs a per-operation polling loop at 10s default cadence (configurable), tunable down to 5s for time-sensitive operations.
3. On state transition (e.g., `pending` → `confirmed` → `credited`), emits `DetectorHint` with venue-specific kind.
4. On terminal state (`credited` for deposit, `confirmed` for withdrawal payout), emits and stops polling that operation.
5. On `failed`, emits a failure hint; T-router routes to refund.

### Deposit flow (`unit_deposit`)

1. T-router materializes the step → calls HyperUnit `POST /v2/deposits` to mint a deposit address. Persists the address as a watched address (Bitcoin chain) AND a HyperUnit operation ID.
2. **Three concurrent observation paths:**
   - **Bitcoin mempool** (via ZMQ rawtx): user's BTC tx hits mempool → emit `DetectorHint { hint_kind: BridgeDepositOnChainObserved, confirmation_state: Mempool }`. T-router can already start anticipatory work (e.g., pre-quote next leg).
   - **Bitcoin confirmed** (via ZMQ rawblock): tx lands in a block → emit `DetectorHint { hint_kind: BridgeDepositOnChainObserved, confirmation_state: Confirmed, block_height }`.
   - **HyperUnit credit** (via Sauron polling of `/v2/operations/deposit/{id}`): status flips to `credited`, response includes HL spot tx hash → emit `DetectorHint { hint_kind: BridgeDepositCredited, hl_credit_hash }`.
3. T-router's verifier marks the step complete only when `BridgeDepositCredited` arrives. The on-chain hints are useful for telemetry and can be used for early UI confirmation but not for state-machine advancement.

### Withdrawal flow (`unit_withdrawal`)

1. T-router materializes the step → calls HyperUnit `POST /v2/withdrawals` with destination BTC address.
2. **Two observation paths:**
   - **HyperUnit status** (Sauron polling of `/v2/operations/withdrawal/{id}`): status flips through `requested` → `processing` → `broadcast` → `confirmed`. On `broadcast`, response includes the BTC txid. Emit `DetectorHint { hint_kind: WithdrawalAcknowledged, btc_txid }`.
   - **Bitcoin chain** (via ZMQ rawtx then rawblock): the BTC payout tx hits mempool then a block → emit `DetectorHint { hint_kind: WithdrawalSettled, confirmation_state }`.
3. T-router's verifier marks the step complete on `WithdrawalSettled` with `Confirmed` (some configurable confirmation depth — for retail-size withdrawals 1–2 conf is fine).

### Polling cadence

- 5s while operation is in active state and < 5 minutes old.
- 10s after 5 minutes.
- 30s after 30 minutes.
- Alert + escalate if no terminal state after 2 hours (HyperUnit operations should not take this long).

This keeps the API call volume bounded even with thousands of active operations.

## Step-ID disambiguation

HyperUnit operation IDs are unique per request — strong disambiguator. The watch entry must carry both the HyperUnit operation ID and the corresponding `execution_step_id`; emitted hints reference the `execution_step_id` so T-router routes correctly.

For deposits specifically, the BTC address is single-use per operation — so address-based hint emission from the Bitcoin backend is also unambiguous, but should still be tagged with `execution_step_id` for symmetry.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| HyperUnit API unavailable | Sauron polling fails; mark backend degraded; alert. Bitcoin chain observations still work for the chain leg. T-router orders waiting on HyperUnit hints stall visibly. Resume on API recovery |
| HyperUnit credit takes much longer than expected | Sauron continues polling; T-router relies on configured timeouts. If exceeded, escalate to MIR |
| HyperUnit returns `failed` for a deposit (e.g., amount below minimum) | Emit `BridgeDepositFailed` hint; T-router triggers refund flow (BTC was sent and is held by HyperUnit; refund mechanism is HyperUnit's responsibility) |
| HyperUnit returns `failed` for a withdrawal | Emit `WithdrawalFailed`; HL credit was already debited at request time; T-router routes via PR6b1 ExternalCustody refund path |
| BTC tx for deposit takes >1h to confirm (low fee?) | Keep watching; emit telemetry alert for slow confirmation |
| Address reuse (HyperUnit recycles addresses) | Should not happen per HyperUnit's design; if it does, our `execution_step_id` tagging on hints handles disambiguation |

## What's missing today

1. **No HyperUnit backend in Sauron.** All HyperUnit-side observation today flows through the generic `ProviderOperationObserverLoop` poking the DB. There's no API client polling HyperUnit's status endpoints.
2. **No `unit`-specific hint kinds** (`BridgeDepositCredited`, `WithdrawalAcknowledged`, `WithdrawalSettled` would all be added with the broader hint-kind enrichment).
3. **No coordination** between Bitcoin chain observation (which already works) and HyperUnit-side credit confirmation. Today, T-router conflates them and waits for a `PossibleProgress` hint that may or may not represent the right thing.

## Out of scope

- HyperUnit's internal settlement mechanics (we trust their state machine; we observe its outputs).
- Future HyperUnit WebSocket if/when it ships — we'd subscribe and demote polling to fallback.
- Other future cross-chain bridges (each gets its own venue doc).
