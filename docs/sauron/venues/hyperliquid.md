# Hyperliquid — Efficient Observation Strategy

This is the venue the loadgen-fast 10k run exposed: **127 stalled orders all on `hyperliquid_bridge_deposit`** because Sauron has no Hyperliquid backend at all. The fix is a first-class HL observer; everything below is the design for it.

## What we observe

Hyperliquid produces several distinct kinds of state we need to track:

| Operation | What we need to know | Trigger |
|---|---|---|
| `hyperliquid_bridge_deposit` | USDC was deposited on Arbitrum AND credited to our HL account | Arbitrum log + HL account state change |
| `hyperliquid_bridge_withdrawal` | HL account withdrawal request was processed AND USDC landed on Arbitrum | HL withdraw tx + Arbitrum log |
| `hyperliquid_trade` (market) | Our perpetual order filled | HL `userFills` channel |
| `hyperliquid_limit_order` | Our limit order rested, partially filled, fully filled, or canceled | HL `orderUpdates` channel |
| `hyperliquid_spot_trade` (PR6b2 path) | Spot order filled (used in refund/exit paths) | HL `userFills` filtered by spot asset |

Two independent surfaces: the Arbitrum chain (for the bridge legs) and Hyperliquid's API (for the account/trading state).

## Native push mechanisms

### Hyperliquid WebSocket API

Endpoint: `wss://api.hyperliquid.xyz/ws` (mainnet) / `wss://api.hyperliquid-testnet.xyz/ws` (testnet).

Relevant subscriptions for our use case:

| Channel | Payload | Use |
|---|---|---|
| `userFills` | `{ user, fills: [{ coin, px, sz, side, time, hash, oid, tid, ... }] }` | Detect that a market or limit order filled |
| `orderUpdates` | `{ user, orders: [{ order: {...}, status, statusTimestamp }] }` | Detect order lifecycle (resting → filled / canceled / rejected) |
| `userEvents` | `{ user, events: [...] }` (fills, fundings, liquidations, twap fills, etc.) | Generic catch-all for unusual events |
| `webData2` | per-user account snapshot incl. balances, positions, open orders | Periodic snapshot — useful for reconciling local state |
| `userNonFundingLedgerUpdates` | deposits, withdrawals, transfers | Detect deposit credit + withdrawal settlement on the HL side |

Subscriptions are **per user address** — for our use case the address is the executor vault for that order's HL leg. Each WS connection can multiplex many subscriptions.

### Arbitrum-side push

The HL bridge contract on Arbitrum emits events when a deposit lands or a withdrawal settles. These are observable via the EVM backend's `eth_subscribe('logs', ...)` (see `chains/arbitrum.md`).

## Pull / fallback mechanisms (Sauron-level OK)

Hyperliquid REST `/info` endpoint accepts JSON-RPC-shaped requests:

| Request | Returns | Use |
|---|---|---|
| `{ "type": "userState", "user": "0x..." }` | balances, positions, open orders, withdrawable | Reconcile / catch-up |
| `{ "type": "orderStatus", "user": "0x...", "oid": ... }` | one order's current status | Direct lookup for a specific order |
| `{ "type": "userFills", "user": "0x...", "startTime": ms }` | fills since timestamp | Backfill on WS reconnect |
| `{ "type": "userNonFundingLedgerUpdates", "user": "0x...", "startTime": ms, "endTime": ms }` | deposits + withdrawals + transfers | Reconcile cross-chain credit |

Polling `/info` once per ~5–10s per active operation is *acceptable at the Sauron level* as a fallback when WS is down or for catch-up. **It must not be in T-router's hotpath** — the design for that is settled (router-side polling was ripped out in commit `494a53a`).

## Recommended efficient design

### Architecture

A new `bin/sauron/src/discovery/hyperliquid.rs` backend (similar shape to `bitcoin.rs` and `evm_erc20.rs`) that:

1. Maintains **one WebSocket connection** to `wss://api.hyperliquid.xyz/ws` (or a small pool if we hit subscription caps).
2. On receipt of a CDC notification of a new HL-bound watch (any operation in `hyperliquid_*`), opens the appropriate subscription:
   - For trades: `userFills` and `orderUpdates` for the executor address.
   - For bridge deposits: `userNonFundingLedgerUpdates` for the destination account address.
   - For withdrawals: `userNonFundingLedgerUpdates` (HL side) AND, on Arbitrum, ensure the EVM backend's log filter includes the HL bridge contract.
3. Collapses subscriptions: many operations on the same address share one subscription per channel.
4. On WS message → match against active operations → emit `DetectorHint` with a venue-specific hint kind.

### Deposit flow (`hyperliquid_bridge_deposit`)

1. T-router submits `bridge_deposit` activity → USDC `transfer()` on Arbitrum to the HL bridge address.
2. **Two observation paths** must both confirm before the verifier accepts the step complete:
   - **Arbitrum-side**: USDC `Transfer` from depositor (our vault) to HL bridge address, observed via Arbitrum WS log subscription. Emit `DetectorHint { hint_kind: BridgeDepositOnChainObserved }`.
   - **HL-side**: `userNonFundingLedgerUpdates` shows a `Deposit` ledger entry for the destination user with matching amount. Emit `DetectorHint { hint_kind: BridgeDepositCredited }`.
3. The verifier accepts the step complete when both hints land. Today it conflates the two; we should split.

### Trade flow (`hyperliquid_trade`)

1. T-router submits trade → POST `/exchange` to HL with order JSON.
2. HL acknowledges with `oid`. T-router persists `provider_ref = oid` on the operation.
3. Sauron's HL backend, having a `userFills` subscription on the executor address:
   - Receives `userFills` event with matching `oid` → emit `DetectorHint { hint_kind: TradeFilled, oid, fillSz, fillPx, hash }`.
   - For partial fills, emit progressive `TradeFilled` events; verifier sums until cumulative size reaches order size.
4. If `orderUpdates` shows status `canceled` or `rejected`, emit `DetectorHint { hint_kind: TradeCanceled, oid, reason }`.

### Withdrawal flow (`hyperliquid_bridge_withdrawal`)

Symmetric to deposit:
1. T-router submits withdrawal action on HL.
2. HL processes → `userNonFundingLedgerUpdates` shows `Withdraw` entry. Emit `DetectorHint { hint_kind: WithdrawalAcknowledged }`.
3. Some seconds/minutes later, USDC lands on Arbitrum. Observed via Arbitrum WS. Emit `DetectorHint { hint_kind: WithdrawalSettled }`.

### Catch-up path

On Sauron startup or WS reconnect:
1. For each watched HL operation, call `/info` with `userNonFundingLedgerUpdates` (for bridge ops) or `userFills` (for trade ops) bounded by `startTime = max(now − 1h, last_known_fill_time)`.
2. Emit `DetectorHint`s for any matching events found.
3. Resume WS subscription.

### Polling fallback (degraded mode)

If the WS connection has been down for > 30s and reconnect attempts fail:
1. Fall back to per-operation `/info` polling at 5s intervals.
2. Mark the backend as `degraded` in metrics; alert.
3. Continue polling until WS comes back, then upgrade and stop polling.

This is *Sauron-level* polling — it's still inside Sauron's process, never crosses the Sauron→T-router boundary.

## Step-ID disambiguation

Critical here because:
- One HL account address may have many simultaneous orders/operations.
- A `hyperliquid_trade` operation may have multiple fills (partials).
- A bridge deposit operation may be superseded across refresh cycles, so the same address has both an old and new operation.

Required: emit hints with `execution_step_id` from the watch entry. For trades specifically, the `oid` is the strongest disambiguator since HL assigns a unique `oid` per submission — match `oid` against `provider_ref`.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| HL WS connection drops | Reconnect with jittered backoff; backfill via `/info` |
| HL WS subscription quota exceeded (high active operation count) | Open additional WS connections; partition operations across them |
| HL API rate limit on `/info` | Per-address rate limiting in the Sauron backend; degraded-mode polling pulls back to 10s when limited |
| HL operation never gets a fill (canceled, rejected, expired) | `orderUpdates` provides terminal status; emit `TradeCanceled` hint; T-router routes to refund |
| HL bridge deposit appears on Arbitrum but never credits HL account | Both halves of the deposit must hint; if one hints and the other doesn't within a deadline, escalate to MIR (manual intervention) |
| HL maintenance / planned downtime | All HL-bound orders stall visibly; operator alert; orders resume on HL recovery |

## What's missing today

Everything. Hyperliquid has **zero** dedicated backend code in Sauron. The generic `ProviderOperationObserverLoop` is the only thing emitting hints for HL operations, and those hints are based on DB `updated_at` timestamps, not actual HL state. This is why the loadgen run stalled.

**Implementation order (most-bang-for-buck first):**

1. New `bin/sauron/src/discovery/hyperliquid.rs` backend with WS client and `userFills` + `userNonFundingLedgerUpdates` subscriptions.
2. New `ProviderOperationHintKind` variants: `TradeFilled`, `TradeCanceled`, `BridgeDepositCredited`, `WithdrawalAcknowledged`, `WithdrawalSettled`.
3. CDC integration: when an `hyperliquid_*` operation is registered, the HL backend opens the right subscription.
4. Catch-up on WS reconnect via `/info`.
5. Step-ID propagation through watches and hints (cross-cutting; see `audit.md` §B).
6. Add HL bridge contract address to Arbitrum WS log filter (cross-cutting; see `chains/arbitrum.md`).

## Out of scope

- HL vault management, trading strategy, or any decisions about *what* to trade (T-router and the planner own that).
- HL websocket post (write-side) — Sauron is read-only.
- Funding rate observation (we don't action on it).
- TWAP order observation (we don't use TWAP today).
