# Across — Efficient Observation Strategy

Across is a cross-chain bridge protocol. We use it for `across_bridge` steps — usually moving USDC or ETH between EVM chains (Ethereum/Base/Arbitrum). Like Velora, it's fully on-chain, so observation is pure log-watching across two chains (origin + destination).

## What we observe

Each `across_bridge` step has two distinct on-chain phases:

| Phase | Chain | Event | Why we care |
|---|---|---|---|
| Origin deposit | Source chain | `V3FundsDeposited(depositId, ...)` from SpokePool | Confirms our deposit is locked in the bridge |
| Destination fill | Dest chain | `FilledV3Relay(depositId, ...)` from SpokePool | Confirms a relayer has filled the recipient — bridge complete |

Both events emit a `depositId` that uniquely correlates origin↔destination. That's our primary disambiguator.

## Native push mechanisms

All observation rides on the EVM chain backend (see `chains/{ethereum,base,arbitrum}.md`):

- `eth_subscribe('logs', { address: SpokePool_origin, topics: [V3FundsDeposited.topic0] })` on the source chain.
- `eth_subscribe('logs', { address: SpokePool_dest, topics: [FilledV3Relay.topic0] })` on the destination chain.

Two subscriptions per step (one per chain), but they're shared across all in-flight Across operations on each chain — so it's really two persistent log subscriptions per chain pair, total.

There is also an Across **REST API** (`across.to/api`) that returns deposit status and fill information, but it's redundant with the on-chain events. Use it only as a fallback during chain backend degradation.

## Pull / fallback mechanisms

| Mechanism | Use case |
|---|---|
| `eth_getLogs` for `V3FundsDeposited` and `FilledV3Relay` | Backfill on WS reconnect or cold start |
| `eth_getTransactionReceipt(deposit_tx_hash)` | Definitive origin confirmation when we know the tx hash from our own submission |
| Across REST API `/deposits/{depositId}` | Sauron-level fallback if chain backend is degraded for the destination chain |

## Recommended efficient design

### Architecture

**No new Sauron backend needed.** Across is fully covered by the EVM chain backend, with the same two enhancements as Velora:

1. **Add SpokePool contract addresses on each EVM chain** to the EVM backend's watch set.
2. **Emit chain hints with venue context**: when matching `V3FundsDeposited` or `FilledV3Relay`, tag the emitted `DetectorHint` with venue-specific kinds (`AcrossOriginDeposited`, `AcrossDestinationFilled`).

### Observation flow

1. T-router submits the deposit tx on origin chain via Across SpokePool. Persists `provider_ref = depositId` (extracted from the deposit call's return value or known deterministically before submission).
2. **Origin observation**: chain backend WS picks up `V3FundsDeposited(depositId=ours)` on origin chain → emit `DetectorHint { hint_kind: AcrossOriginDeposited, depositId, tx_hash }`.
3. **Destination observation**: chain backend WS picks up `FilledV3Relay(depositId=ours)` on destination chain → emit `DetectorHint { hint_kind: AcrossDestinationFilled, depositId, fill_tx_hash, output_amount }`.
4. T-router's verifier marks the step complete on `AcrossDestinationFilled`. The origin hint is useful for telemetry and for stale-quote refresh decisions, but settlement is the destination event.

### Why this works without a new backend

Both events are emitted by the SpokePool contracts; the chain backend already does log subscription with a topic + address filter. Adding the two SpokePool addresses (per chain) to the watch set and adding the venue-specific hint emission code is a small, contained change.

## Step-ID disambiguation

`depositId` is unique per deposit and is used by Across to correlate origin↔destination. Match `depositId` against `provider_ref` on the operation. For symmetry with other venues, the watch entry should still carry `execution_step_id` per the cross-cutting fix in `audit.md` §B.

## Failure modes and backstops

| Failure | Mitigation |
|---|---|
| No relayer fills on destination (rare; Across's economic model usually ensures fast fills) | After deadline (configurable, e.g., 60s), Sauron emits a `AcrossSlowFill` warning hint; T-router waits longer up to a configured cap, then escalates. Across's own `slowFill` mechanism eventually completes any deposit, but it may take hours |
| Origin deposit reverts | `eth_getTransactionReceipt(deposit_tx_hash)` returns status=0; verifier emits failure; T-router routes to refund (no funds were locked since the tx reverted) |
| Destination fill amount differs from expected (relayer fee changed) | The fill amount in `FilledV3Relay` is what we got. If less than `min_amount_out`, T-router treats as a partial-success and may need to top up downstream legs |
| Across SpokePool upgrade (new contract) | Watched contract addresses pulled from config; on upgrade we update config + reconcile. Standard pattern |
| Chain backend on destination chain degraded | Fall back to Across REST API at Sauron level for the destination fill query (poll `/deposits/{depositId}` until `status = filled`) |
| Reorg on destination removes the fill | Chain reorg path retracts hint; T-router waits for re-fill (Across relayers typically re-fill on reorg) |

## What's missing today

1. **SpokePool contract addresses are not explicitly in the EVM backend's watch set per chain.** Today the backend matches on Transfer-to-recipient, which catches the destination Transfer but misses the protocol-level `FilledV3Relay` event with its richer metadata (fill amount, depositId correlation, relayer address).
2. **No `Across*` hint kinds** (`AcrossOriginDeposited`, `AcrossDestinationFilled`, `AcrossSlowFill`, `AcrossOriginReverted`). All currently fall under generic `PossibleProgress`.
3. **No Across REST API fallback** for the case where the destination chain backend is degraded but Across-side state is queryable.
4. **No chain-pair correlation logic** — origin hint and destination hint should be correlated by `depositId` so the verifier can reconcile them as parts of one bridge operation. Today the verifier handles this, but Sauron could pre-correlate to reduce verifier work.

## Out of scope

- Across V2 (deprecated; we use V3 SpokePool).
- Across-managed assets (we only use it for USDC/ETH bridging).
- Across governance / token observation.
- Direct relayer integration (we don't run relayers).
