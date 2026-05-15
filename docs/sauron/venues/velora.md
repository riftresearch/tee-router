# Velora — Efficient Observation Strategy

Velora (the DEX aggregator formerly known as Paraswap, accessed at `api.velora.xyz`) is the venue behind every step we tag with the `universal_router_swap` transition kind. The transition-kind name is misleading — it's a generic "swap via aggregator router" shape, not Uniswap's UniversalRouter. The actual on-chain executor is Velora's own router (Augustus-style), and the off-chain quote/calldata source is Velora's REST API.

This is one of the *easier* venues to observe efficiently because everything observable is on-chain after Velora returns the calldata; T-router signs and submits the tx, then settlement is just an ERC-20 Transfer from Velora's executor contract to our recipient. No Velora-side push API is needed, and no Velora-side polling is needed once the tx is submitted.

## What we observe

For each `universal_router_swap` step:

- **Did the swap actually execute and deliver the expected output to our recipient?**
- **What was the realized output amount?** (We may have received more or less than `min_amount_out`; we need exact for downstream legs.)
- **Did the tx revert?** (Velora's router can revert mid-route; we need a definitive yes/no.)

The signal is: an ERC-20 `Transfer(from = velora_executor, to = our_recipient, value)` log in the same tx as our swap, OR the tx receipt's `status = 0` for a revert.

We do *not* re-observe Velora's quote API after submission — the quote is already validated against `priceRoute` invariants in `crates/router-core/src/services/action_providers.rs:5266-5318` (network, srcToken, destToken, amounts) before submission, and after submission only the chain matters.

## Native push mechanisms

All observation rides on the EVM chain backend (see `chains/{ethereum,base,arbitrum}.md`):

- `eth_subscribe('logs', { address: [...velora_executors_per_chain], topics: [Transfer.topic0] })` — push of every Transfer originating from Velora's executor on each chain.
- `eth_subscribe('newHeads')` — drives reorg detection.

There is **no Velora-specific WebSocket or push API**. Velora's REST API is request/response only: POST `/prices` for a quote (returns `priceRoute`), POST `/transactions` for swap calldata (returns `to`, `data`, `value`). We've already used both of these *before* the on-chain submission; nothing more to fetch from Velora once we have the tx hash.

## Pull / fallback mechanisms

| Mechanism | Use case |
|---|---|
| `eth_getTransactionReceipt(tx_hash)` | Definitive receipt + logs once we know our submission tx hash. Strongest single observation |
| `eth_getLogs` with the Transfer filter | Backfill on chain backend reconnect or cold start |
| Velora `POST /prices` (re-quote) | Used by the stale-quote refresh path (T-router side, not Sauron) — out of scope for observation |

The receipt-by-tx-hash path is the **single most reliable observation** for this venue: T-router knows the tx hash from its own submission, and the receipt is definitive. WS log subscription is the fast push-path that lets us emit a hint sub-second after the block lands; receipt verification is the closing check.

## Recommended efficient design

### Architecture

**No new Sauron backend needed.** Velora is fully covered by the EVM chain backend with two enhancements:

1. **Add Velora executor contract addresses (per chain) to the EVM watch set.** Velora's executor address comes back as the `to` field on the `/transactions` response (see `action_providers.rs:4429`); we know it per chain at config time.
2. **Emit chain hints with venue context**: when the EVM backend matches a `Transfer` where `from = a known velora executor` and `to = a watched recipient`, emit `DetectorHint { hint_kind: VeloraSwapSettled, tx_hash, log_index, amount_out, executor }`.

### Observation flow

1. T-router calls Velora `POST /prices` → receives `priceRoute`. Persists to step.
2. T-router calls Velora `POST /transactions` with `priceRoute` → receives `to` (Velora executor), `data` (calldata), `value` (msg.value if needed).
3. T-router signs + submits tx on the appropriate EVM chain. Persists `provider_ref = tx_hash`.
4. **Fast path (push):** EVM backend's WS log subscription receives `Transfer(from = velora_executor, to = our_recipient, value)` within sub-second of the block landing. Emit `DetectorHint { hint_kind: VeloraSwapSettled, ... }`.
5. **Definitive path (verifier-side, T-router):** verifier picks up the hint, calls `eth_getTransactionReceipt(tx_hash)` once to confirm: status, all logs, gas used. Source-of-truth check that closes out the step.
6. **Revert handling:** if no hint arrives within deadline (e.g., 60s), T-router's verifier proactively calls `eth_getTransactionReceipt(tx_hash)`. If `status = 0`, the swap reverted; emit failure path. If still pending, keep waiting.

### Why this works without a new Sauron backend

The chain backend already does Transfer matching. We just need:
- Velora executor addresses in the watch set per chain (small config + watch registration change).
- Venue-tagged hint kind so the verifier doesn't have to disambiguate "is this swap settlement vs an unrelated transfer to our recipient?".

This is a much smaller surface than HL or CCTP precisely because Velora has no off-chain state we need to keep observing.

### What about multi-hop routes and intermediate Transfers?

Velora's `priceRoute` can split a single user swap across multiple underlying DEXes (Uniswap, Curve, Balancer, etc.) and emit many intermediate `Transfer` events inside the executor before the final Transfer to our recipient. We don't care about those intermediates for observation — only the **final Transfer to `our_recipient`** matters. Filter the WS subscription on `topics[2] = our_recipient_topic` to drop intermediates at the RPC level.

## Step-ID disambiguation

`provider_ref = tx_hash` is the strongest disambiguator — one tx hash maps to exactly one step.

The watched recipient may be reused across orders (recipients are user-provided), so `(tx_hash, log_index)` is the unique fingerprint. Same cross-cutting requirement as `audit.md` §B: the watch entry must carry `execution_step_id` so the emitted hint references the right step even when two superseded steps share a recipient.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| Swap tx reverts mid-route | `eth_getTransactionReceipt` returns `status = 0`; verifier emits failure; T-router routes to refund |
| Output amount below `min_amount_out` (slippage exceeded) | Velora's executor enforces `min_amount_out` and reverts the whole tx if violated. Defensive check: verifier compares the realized amount in the Transfer log against expected min |
| `priceRoute` invalidated between quote and tx (Velora's quote went stale) | `POST /transactions` rejects with an error before submission; T-router triggers stale-quote refresh (PR5c). This is pre-submission; not an observation problem |
| Tx never gets mined (gas too low, mempool eviction) | Verifier observes "no receipt after deadline"; T-router resubmits with higher gas (existing retry policy) |
| Velora API unavailable for *quote* (pre-submission) | Quote-time failure; T-router routes to alternate venue if available, else refund. Not an observation problem |
| Velora executor contract upgrade (proxy redeployment) | Watched executor address pulled from config or from the most recent `/transactions` response; on upgrade we pick up the new address from the next quote naturally — but Sauron's watch set must be updated. Operational drill: when Velora changes executors, rotate the address in Sauron's config |
| Wrong recipient (planner bug) | Verifier rejects: receipt has logs but none to expected recipient. Refund. Should be a planner bug surfaced in CI |
| Reorg removes the tx | Chain reorg path retracts the hint; T-router re-evaluates (the tx may re-land in the new chain) |

## What's missing today

1. **Velora executor addresses are not explicitly in the EVM backend's contract address allow-list per chain.** The backend matches on recipient, not on `from`-side executor. Adding the Velora executor addresses lets us emit a venue-specific `VeloraSwapSettled` hint instead of a generic `Transfer observed` hint, and lets the verifier fast-path the disambiguation.
2. **No `VeloraSwapSettled` hint kind.** Same theme as other venues — needs the broader `ProviderOperationHintKind` enrichment from `audit.md` §E.
3. **No proactive `eth_getTransactionReceipt` poll** in T-router's verifier as a backup when hints don't arrive within deadline. (This belongs on T-router, not Sauron — but worth flagging here so the design is captured in one place.)
4. **No tracking of executor address changes** — Velora may rotate executor contracts. We should pull the executor address from the most recent `/transactions` response and reconcile against our watch set, alerting if the address has changed since last quote.

## Out of scope

- Other DEX aggregators (1inch, 0x, OpenOcean, Kyber). Each would be its own venue doc if/when added.
- Direct DEX pool observation (Uniswap V3 Swap events, Curve TokenExchange, etc.) — Velora's executor Transfer is the canonical signal for our use case.
- Velora's own backend infrastructure (we treat their API as a black box that returns quotes and calldata).
- Velora referral / partner reward observation (we set `partner = "rift-router"` per `VELORA_DEFAULT_PARTNER`, but rewards are out of band).
