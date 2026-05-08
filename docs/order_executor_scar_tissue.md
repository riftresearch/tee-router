# `order_executor.rs` Scar Tissue

A catalogue of non-obvious behaviour, invariants, and edge-case branches mined
from `bin/router-server/src/services/order_executor.rs` (≈15.3k LOC). Intended
as a **test-case backlog** for the Temporal-based rewrite — every item below is
a property the new system must honour or explicitly retire with reasoning.

> Coverage status: the originally thin ranges called out in §16 have now been
> read through. One source-map caveat: the current `order_executor.rs` has moved
> refund finalisation to lines ~7937–8239, so that block was read in addition to
> the literal 3500–4858 range.

---

## 1. Magic constants

All declared at the top of the file. Each is load-bearing — copy them or
re-derive them, do not pick new round numbers.

| Constant | Value | Purpose |
| --- | --- | --- |
| `MAX_EXECUTION_ATTEMPTS` | `2` | Cap on attempt_index for an order before the next failure forces refund. Refund attempts themselves are exempt (see §5). |
| `REFUND_PATH_MAX_DEPTH` | `5` | Bound on hop-count when enumerating refund routes from a recoverable position to a refund destination. |
| `REFUND_TOP_K_PATHS` | `8` | After enumeration, the planner keeps only the top-K paths by quote `amount_out` before binding sources. |
| `REFUND_PROVIDER_TIMEOUT` | `10s` | Per-provider timeout for refund-side quoting (bridges, exchanges, units). |
| `REFRESH_PROVIDER_TIMEOUT` | `10s` | Same, for the stale-quote refresh path. |
| `REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW` | `1_000_000` | Gas reserve withheld from a Hyperliquid spot-send quote so the follow-up trade can pay. |
| `STALE_RUNNING_STEP_RECOVERY_AFTER` | `5 min` | A step stuck `Running` past this threshold without a checkpoint is treated as ambiguous (see §6). |
| `MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION` | `8` | Cap on how many `RefreshedExecution` attempts can chain within a single execution pass. |
| `REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS` | `12_500` (= 125 %) | Miner-fee reserve (in bps over expected) carved out of unit-deposit refreshes targeting Bitcoin. |
| `REFRESH_PROBE_MAX_AMOUNT_IN` | `U256::MAX` decimal | Sentinel used for ExactOut probe quotes — refreshed quote treats this as “unbounded input.” |

**Rewrite rule:** these are the workflow timer / retry-policy / budget knobs
of the new design. Do not bury them inside activity code.

---

## 2. Persistence boundaries (crash injection points)

`OrderExecutionCrashPoint` (line 347) names six points where the executor
defends against a crash mid-side-effect. Every Temporal activity boundary
must align with one of these (or explicitly justify why it doesn't):

1. **`AfterExecutionLegsPersisted`** – plan materialised, legs written, before any execution.
2. **`AfterStepMarkedFailed`** – step transitioned `Running → Failed`, before retry/refund decision.
3. **`AfterExecutionStepStatusPersisted`** – step transitioned to a terminal sub-status (`Completed`/`Waiting`).
4. **`AfterProviderReceiptPersisted`** – we have a provider receipt (e.g. Across deposit tx) recorded but step not yet settled.
5. **`AfterProviderOperationStatusPersisted`** – provider observation recorded, before the corresponding step completion is written.
6. **`AfterProviderStepSettlement`** – provider operation linked to a settled step row, before order/leg rollups update.

A single helper `maybe_inject_crash` (line 7511) gates all six. The reason
this matters: between any two of these points, **the next worker that picks
up the order has to figure out where we crashed and idempotently advance**.
If the rewrite collapses any pair into one activity, prove the crash window
is harmless.

---

## 3. The 14-phase worker pass

`process_worker_pass_with_limits_and_concurrency` (line 585) runs phases in a
fixed order. The order is meaningful — earlier passes drain ambiguity that
later passes assume away.

1. `reconcile_failed_orders` – clean up orders in `Failing*` states from prior crashes.
2. `process_provider_operation_hints` – ingest external "trust me, this happened" hints (CCTP attestations, etc.).
3. `process_terminal_provider_operation_recovery` – settle orphaned terminal provider ops onto their steps.
4. `process_stale_running_step_recovery` – §6 below.
5. `process_completed_orders_pending_finalization` – finalise orders whose last step is done.
6. `process_direct_refund_finalization_pass` – finalise refunds that completed.
7. `process_refunded_funding_vault_finalization_pass` – release vaults whose refunds are complete.
8. `process_refund_planning_pass` – materialise refund plans for orders that need them.
9. `process_manual_refund_order_alignment_pass` – align order status when a vault was manually refunded.
10. `process_manual_refund_vault_alignment_pass` – inverse alignment.
11. `process_planning_pass` – materialise execution plans for newly-funded orders.
12. *(execute)* `execute_ready_orders` – fan-out via `JoinSet`, capped at `execution_concurrency` (default 1; configurable per process).
13. `process_terminal_internal_custody_vaults` – mark internal custody vaults terminal once their ledger is reconciled.
14. `process_released_internal_custody_sweeps` – sweep released internal custody balances.

**Rewrite rule:** each phase becomes either (a) a periodic Temporal workflow
or (b) a signal-driven branch on the per-order workflow. Do **not** treat
them as 14 independent crons — phases 1, 2, 3, 4, 8 are recovery-side and
must run *before* execution in any given tick.

---

## 4. Order / vault / step state alignment

Statuses pair across two state machines that must converge:

| Order status | Compatible vault statuses (see `funding_vault_state_allows_order_execution`, line 6315) |
| --- | --- |
| `Funded` → `Executing` | `Funded` (CAS to `Executing`) |
| `RefundRequired` → `Refunding` | `Funded`, `RefundRequired` |
| `Executing` | `Executing` (idempotent re-entry) |
| `Refunding` | `Refunding` |
| `ManualInterventionRequired` / `RefundManualInterventionRequired` | matching vault flag |

Both transitions go through `mark_order_refund_manual_intervention` /
`transition_order_to_manual_intervention` which **dual-attempts** the
order-side and vault-side CAS, tolerating either side already being in the
target state. The rewrite must treat `(order_status, vault_status)` as a
single observation, not two — partial advance is real and observed.

`mark_order_refund_required` (line 1760) attempts **both**
`Executing → RefundRequired` *and* `Funded → RefundRequired` because the
same call site is reached from pre-execution refund triggers (vault funded,
plan failed) and mid-execution refund triggers.

---

## 5. Retry vs refund decision

Implemented around line 1391:

- **Refund-recovery attempts NEVER retry.** A failed step inside a
  `RefundRecovery` attempt goes straight to `RefundManualInterventionRequired`
  with reason `refund_attempt_failed` (line 1884). There is no refund of a
  refund.
- For ordinary attempts, retry until `attempt_index >= MAX_EXECUTION_ATTEMPTS`
  (= 2). On the 2nd failure, transition to `RefundRequired` with reason
  `refund_required_after_failed_attempts` (line 1776).
- Retry creates a new attempt with reason `retry_after_failed_attempt`
  (line 1551) and **supersedes** all prior steps from the failed step's
  `step_index` onward (`superseded_by_retry_attempt`, line 1582).

**Rewrite rule:** Temporal `RetryPolicy` covers the per-activity case but
not the cross-attempt supersede semantics. The rewrite needs an explicit
"retry attempt" concept that owns step-index supersession; this is not
something you get for free from `maximumAttempts`.

---

## 6. Stale running step → manual intervention

`process_stale_running_step_recovery` (line ~895) finds steps that have been
`Running` for ≥ `STALE_RUNNING_STEP_RECOVERY_AFTER` (5 min) **without a
provider checkpoint**.

The reason field tells the story:

- `durable_provider_operation_waiting_external_progress` (line 962) – step
  is waiting on a provider op that is itself still progressing externally.
  **No action**, just leave it.
- `ambiguous_external_side_effect_window` (line 993) – we may or may not
  have fired the provider call; we cannot tell. Path to manual intervention.
- `stale_running_step_without_checkpoint` (line 1021) – we never even got
  to the "about to fire" checkpoint. Same destination.

**Invariant:** if a step has been `Running` for > 5 min and we have no
provider receipt for it, **a human looks at it.** This is the single
strongest "do not auto-recover" rule in the file. Encode it as a Temporal
timer + side-effect query, not a generic retry.

---

## 7. Stale quote refresh

`refresh_stale_quote_before_step` (line ~5130) and
`compose_refreshed_market_order_quote` together implement mid-execution
quote refresh. Highlights:

- **Skipped for `LimitOrder`s and `RefundRecovery` attempts.** Limit orders
  have a fixed price; refunds can't safely re-quote.
- Fires only when the leg's `provider_quote_expires_at` is in the past
  **and** the leg is unstarted.
- ExactIn iterates forward from `start_transition_index`; ExactOut iterates
  backward, refining the required output.
- Special case: when a hyperliquid spot-send is followed by a hyperliquid
  trade, the send quote reserves
  `REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW` so the trade can pay.
- Bitcoin unit-deposit refreshes carve out a fee reserve at
  `REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS = 12_500` (125%).
- **Untenability outcomes** – the order goes to `RefundRequired`:
  - `refreshed_exact_in_output_below_min_amount_out` (line 5812)
  - `refreshed_exact_out_input_above_available_amount` (line 6131)
  - `stale_provider_quote_refresh_untenable` (line 5267)
- A successful refresh creates a new `RefreshedExecution` attempt
  (reason `stale_provider_quote_refresh`, line 5363) that **supersedes**
  prior steps from the failed step's index
  (`superseded_by_stale_provider_quote_refresh`, line 5382/5410).
- Bound: `MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION = 8` per execution
  pass — past this, give up.

**Rewrite rule:** a refresh is its own attempt, not a step-internal retry.
Same supersede semantics as §5.

---

## 8. Refund position discovery — "single recoverable position"

In `discover_refund_positions` / planning (line ~3170, see also constant at
3182):

- The refund planner enumerates positions across three handle types:
  `FundingVault`, `ExternalCustody`, `HyperliquidSpot`.
- **Invariant:** the recoverable set must be **exactly one** position. If
  it's `0` or `>1`, the order goes to `RefundManualInterventionRequired` with
  reason `refund_requires_single_recoverable_position` (line 3182).
- A second check at line 3196 catches the race where validation passed but
  the position disappeared before binding
  (`refund_recoverable_position_disappeared_after_validation`).

**Why:** multi-source refunds open a combinatorial space the operator
cannot reason about quickly. A human picks. The rewrite must preserve this
gate — it is not a candidate for "smarter automation."

---

## 9. Refund tree — per-transition rules

`materialize_refund_transition_plan` (line 8666) dispatches across 8
transition kinds. Each has its own builder:

- `refund_transition_across_bridge_step` (9480)
- `refund_transition_cctp_bridge_steps` (9584) – multi-step (burn + receive)
- `refund_transition_unit_deposit_step` (9693)
- `refund_transition_hyperliquid_bridge_deposit_step` (9760)
- `refund_transition_hyperliquid_bridge_withdrawal_step` (9820)
- `refund_transition_hyperliquid_trade_step` (9928) – multi-leg, supports
  `prefund_from_withdrawable`
- `refund_transition_universal_router_swap_step` (10066)
- `refund_transition_unit_withdrawal_step` (10183)

`refund_path_compatible_with_position` (line 8951) gates which positions can
seed which transitions:

- **`FundingVault` is NEVER a multi-hop refund source.** Direct refund only.
- **`HyperliquidSpot`** can only seed `HyperliquidTrade` *or*
  `UnitWithdrawal` as the first hop.
- **`ExternalCustody`** can do anything *except* `HyperliquidBridgeDeposit`
  unless its role is `DestinationExecution`.

`choose_better_refund_quote` picks the higher `amount_out`.

Bindings (line 8450, 8460):

- `RefundExternalSourceBinding`: `Explicit { vault_id, address, role }` |
  `DerivedDestinationExecution`.
- `RefundHyperliquidBinding`: `Explicit` | `DerivedSpot` |
  `DerivedDestinationExecution`.

A refund step that's a direct transfer uses `provider = "internal"` (the
custody-action executor), distinguishable from provider-driven refund legs.

---

## 10. Provider operation hint flow + verifier dispatch

External hints (line ~2423) are claimed under a lease (claim can be
reclaimed if the worker dies before processing). Verification dispatches
on the step type (line 2572):

- `UnitDeposit` step → `UnitDepositVerifier` (chain-evidence path, line 411)
- everything else → `ProviderStatusVerifier` (polling path, line 412)

Hint dispositions (`HintDisposition`, line 368):
`Accept | Reject | Defer`.

**Rewrite rule:** hints are external signals. In Temporal terms each hint
is a `signal` to the order workflow; the verifier dispatch lives in the
workflow, not the API handler.

---

## 11. Across on-chain log recovery

`recover_across_deposit_from_logs` (line 2127) scans `FundsDeposited` events
to recover a Across operation when the off-chain `intent_id` was lost.

Match criteria are **strict**: input/output token, amount, recipient, deadline,
all must match. A near-miss is treated as no-match (logged) rather than a
candidate, because mis-binding two intents is worse than waiting.

`RecoveredAcrossDeposit` (line 92) is the canonical struct produced by the
recovery path.

---

## 12. Hyperliquid trade timeout cancel

`hyperliquid_timeout_due` (line 11454). Hyperliquid trades are the **only**
provider step type with explicit time-bounded cancel logic; everything else
is "wait for terminal status." The trade cancel path lives at line ~2346.

**Why singular:** Hyperliquid orders can sit on the book indefinitely.
Across, CCTP, Unit, and the swap providers either complete on-chain or fail;
they don't have an "open forever" mode.

---

## 13. Step type dispatch (`execute_running_step`, line 6510)

Each variant of `OrderExecutionStepType` has a separate dispatch arm.
`Refund` and the provider variants share an outer envelope but call into
different provider traits:

- `WaitForDeposit` → not executable here; surfaces `InternalStepNotExecutable`.
- `Refund` → `custody_action_executor` direct transfer.
- `AcrossBridge`, `CctpBurn`, `CctpReceive`, `HyperliquidBridgeDeposit`,
  `HyperliquidBridgeWithdrawal` → `bridge` provider trait.
- `UnitDeposit`, `UnitWithdrawal` → `unit` provider trait.
- `HyperliquidTrade` → `exchange` provider trait.
- `UniversalRouterSwap` → bridge trait (treated as a bridge with no chain
  hop).

`CctpReceive` is special: its request is **hydrated** from the burn
receipt (`hydrate_cctp_receive_request`) before dispatch — the receive
side cannot be planned in advance because it depends on the attestation.

---

## 14. Race conditions / benign `NotFound`

The executor leans heavily on database CAS. A lost CAS is **always**
treated as benign — typically `RouterServerError::NotFound` mapped to
`StepExecutionOutcome::Skipped`. Sites:

- `Ready/Planned → Running` CAS lost (line 6368) – another worker won.
- `Running → Completed` CAS lost (line 6450) – provider observation
  recovery beat us to it.
- `Running → Waiting` CAS lost (line 6489) – same.
- `materialize_plan_for_order` `PendingFunding → Funded` CAS – benign
  `NotFound` tolerated.
- `mark_failed` losing its CAS – benign (someone else moved the step
  terminal).

**Rewrite rule:** Temporal serialises per-workflow execution, so most of
these races *go away* by construction. But the cross-workflow races (e.g.
provider-observation recovery touching the same order) remain. Build the
rewrite around: "the workflow owns the order's step state machine, and
external observers can only enqueue signals."

---

## 15. Failure snapshot & `failed_attempt_snapshot`

`failed_attempt_snapshot` (line 12361) captures the order + failed step
into a JSON blob persisted at attempt-failure time. Used by the alignment
passes (and operators) to reconstruct *why* an attempt died after later
attempts have superseded its rows. The rewrite must preserve this
forensic blob — the workflow-event log alone is not enough because step
rows get superseded.

---

## 16. Follow-up reads completed

The originally thin ranges have been read through. Findings below are additional
properties for the Temporal rewrite test backlog.

### 16.1 Refund balance reads, route selection, and materialisation

- Refund balance reads are chain-observation paths, not DB-only guesses.
  `deposit_vault_balance_raw` prefers a Bitcoin funding observation outpoint and
  asks whether that outpoint is spendable before falling back to address balance;
  EVM vaults read native or ERC-20 balance directly. `custody_vault_balance_raw`
  returns `"0"` when a custody vault has no asset, otherwise reads the chain.
- Direct automatic refund planning only happens when the recoverable position is
  an `ExternalCustody` position already in the order source asset. Funding-vault
  direct refund completion is handled by finalisation/reconciliation, not by
  building a transition path here.
- Refund path selection snapshots provider policy and filters providers before
  quoting. Quote failures for one candidate path/provider combination are logged
  and skipped; they do not abort all refund planning. The chosen refund quote is
  the highest `amount_out` among viable paths.
- Refund quoting is ExactIn with `min_amount_out = "1"` for bridge/exchange
  transitions. Across/CCTP/Hyperliquid bridge quotes disable partial fills.
  `expires_at` for the route is the minimum expiry across quoted legs.
- Hyperliquid bridge-deposit followed by Hyperliquid trade subtracts the
  spot-send gas reserve before quoting the trade. If the amount does not exceed
  the reserve, refund planning errors rather than producing a dust route.
- Universal-router refund quoting asks chain/provider metadata for decimals.
  Missing or invalid token decimals is a provider failure, not a silent default.
- Unit withdrawal refund paths must meet provider/asset minimums; BTC dust
  withdrawals are rejected before materialisation.
- `ensure_primary_execution_attempt` is idempotent under unique violation by
  re-reading the active attempt. Step idempotency keys include
  `(order_id, attempt_index, provider, step_index)`.
- Execution legs are inserted idempotently before steps are rebound to persisted
  leg IDs. `AfterExecutionLegsPersisted` fires between those two operations.
- Plan hydration creates or reuses per-order router-derived
  `DestinationExecution` and `HyperliquidSpot` vaults based on request role
  placeholders. Shared configured Hyperliquid accounts are not allowed in routed
  execution.
- A failed internal custody vault may be reactivated only while the order is
  `RefundRequired` or `Refunding`; reactivation records lifecycle metadata with
  the previous status/reason. Outside refund recovery, using a failed internal
  custody vault is an error.
- Persisted planned/ready steps are rehydrated in place; if another worker
  advances the step between read and write, the `NotFound` CAS loss is benign
  and the next pass re-reads instead of executing stale in-memory materialisation.
- Runtime custody validation enforces that materialised internal vault IDs,
  roles, visibility, control type, ownership, and request addresses match.
  Non-final hops that need an intermediate recipient must bind a
  `DestinationExecution` vault. `WaitForDeposit` and direct `Refund` steps are
  forbidden inside market-order execution routes.
- Provider execution policy is checked both route-wide and per provider before
  new execution. Blocked providers emit telemetry and fail the execution phase.

### 16.2 Provider completion, checkpoints, and observation recovery

- `CctpReceive` request hydration searches for the latest completed CCTP bridge
  operation matching `burn_transition_decl_id`. If the attested decoded amount is
  present it must equal the planned receive amount; `message` and `attestation`
  are mandatory, and the receive request records the burn operation ID and burn
  tx hash when available.
- Provider-only intents may not carry retention actions. They persist provider
  state and complete synchronously unless the provider supplied an operation
  state that still waits externally.
- Custody-backed intents capture balance observations before side effects and
  after receipts. When a completed step has a destination probe and
  `min_amount_out`, the observed destination credit delta must be at least the
  minimum and must not regress.
- `AfterProviderReceiptPersisted` is the checkpoint after custody receipt
  persistence. For multi-action provider intents, a checkpoint is written after
  each provider action; once all provider actions are submitted, provider
  `post_execute` data is folded into the checkpoint before it is persisted.
- Retention actions are executed as custody actions but are not counted as
  provider actions. Supported retention recipient role is only `paymaster_wallet`,
  and the retention settlement asset must match the step input asset.
- Hyperliquid retention is special: it may add a spot-send retention after the
  first provider action and, for a spot-to-perp prefund (`UsdClassTransfer` with
  `to_perp = false`), bump the first provider action amount by the total
  retention. Decimal precision must be consistent and overflow is an error.
- `persist_provider_state` upserts provider operations and provider addresses.
  Completed provider operations require a real provider ref; Across submitted
  operations intentionally do not persist the quote ID as a provider ref.
- Trusted provider-observation hints are accepted only from the dedicated
  provider-observation hint source. Current observations with a mismatched or
  missing provider ref are invalid; stale snapshots whose `provider_ref` does not
  match the current operation are ignored, not failed.
- Incomplete receipt-checkpoint observations are ignored when they only submitted
  a subset of expected provider actions. This prevents recovery from treating a
  partial multi-action checkpoint as terminal evidence.
- Provider observation updates wrap new observed state with hint metadata and
  preserve previous observed state. Tx hashes are recovered recursively from
  response, receipt, provider response, previous observed state, or provider
  observed state.
- Hyperliquid operation OID recovery prefers observed-state paths before falling
  back to parsing `provider_ref`. Hyperliquid timeout cancellation is only due
  for Hyperliquid trade operations observed as `WaitingExternal` with an expired
  `timeout_at_ms`.
- Provider status update settlement has two crash windows: after provider
  operation status persistence and after step settlement. Step completion/failure
  is idempotent if the step is already in the target terminal state.
- Hyperliquid bridge-withdrawal completion is normalized through a destination
  balance-readiness check. If the chain-observed destination balance has not
  credited at least `min_amount_out`, or regressed, the operation is demoted back
  to `WaitingExternal` and terminal response/error/tx data is cleared.

### 16.3 Order finalisation and custody lifecycle

- Order finalisation requires an active attempt with at least one execution step
  after deposit, every execution step completed, and every execution leg
  completed. A completed step alone is not sufficient.
- Finalisation transitions the funding vault first, then the order. A lost vault
  CAS is tolerated only when a re-read shows the vault is already in the target
  status. The active attempt is then marked `Completed`, tolerating a lost CAS.
- Normal execution finalises `Executing -> Completed` and
  `Executing` vault -> `Completed` vault. Refund recovery finalises
  `Refunding -> Refunded` and `Refunding` vault -> `Refunded` vault.
- Direct source-vault refunds are finalised without execution steps. If an order
  is `Refunding`, has a refunded funding vault, and has no active refund attempt,
  the executor creates a direct `RefundRecovery` attempt for finalisation.
- Refunded funding-vault reconciliation applies to unstarted orders in
  `PendingFunding`, `Funded`, `RefundRequired`, `Refunding`, and also `Expired`
  if funding was observed at or before `action_timeout_at`. It refuses orders
  that have execution steps after deposit.
- Terminal internal custody status is derived from order terminal status:
  `Completed` releases internal custody; `Refunded`,
  `ManualInterventionRequired`, `RefundManualInterventionRequired`, and
  `Expired` fail it. `RefundRequired`/`Refunding` are not terminal for custody.
- Internal custody finalisation writes lifecycle metadata and currently marks
  `lifecycle_balance_verified = false`; a later released-custody sweep attempts
  to sweep released balances and records sweep metadata. Sweep failures are
  logged and do not abort the pass.

### 16.4 Refresh helpers, balance probes, and snapshots

- Suffix quote refresh uses the latest completed prior leg from an eligible
  non-refund attempt at or before the active attempt index. It must match the
  previous transition and the stale leg input asset, be `Completed`, and have
  `actual_amount_out`. Only the first transition may fall back to the original
  quote `amount_in`.
- Refresh route construction carries a schema-v2 quote blob with the original
  quote ID/expiry, transition IDs, legs, and gas reimbursement plan. Missing gas
  reimbursement data becomes an explicit `"none"` plan.
- Refresh source and recipient addresses come from already materialised step
  request fields. Intermediate hops without materialised addresses fail. A
  Hyperliquid bridge-deposit refresh uses the materialised source account as the
  recipient, falling back to the funding vault only for the first transition.
- CCTP refresh materialises two quote legs: burn and receive. The receive leg
  uses child transition ID `<transition_id>:receive` and carries CCTP phase
  markers in raw quote data.
- Non-final refresh transitions use `min_amount_out = "1"`. ExactOut refresh
  uses the available amount as max input for the starting transition and the
  `REFRESH_PROBE_MAX_AMOUNT_IN` sentinel for earlier probe transitions.
- Bitcoin unit-deposit refresh can carry a miner-fee reserve quote. Paymaster
  gas retention is subtracted or added with strict underflow/overflow checks.
  Hyperliquid spot-send gas reserve helpers use the same strict arithmetic.
- Raw amount parsing accepts only unsigned decimal strings. Decimal-string to
  raw conversion rejects empty values, scientific notation, multiple decimal
  points, and fractional precision beyond the asset decimals.
- Hyperliquid refund balance is usable only when total balance is nonzero and
  hold balance is zero.
- `failed_attempt_snapshot` includes enough forensic custody context to survive
  later supersession: source kind, failed step ID/index/type, amount/minimum,
  source asset, source/hyperliquid/recipient/revert custody IDs, roles, and
  addresses.
- `StepBalanceObservation` tracks the source vault and, when present, an
  internal destination vault. Hyperliquid spot vault balances are not chain
  observable through this path; source/destination execution vaults are observed
  only when their chain matches the probed asset.
- Stored destination balance probes are parsed strictly. Malformed asset or
  amount fields are provider failures, not ignored. Response augmentation updates
  an existing matching destination probe or adds one if absent.

---

## 17. Quick summary for the rewrite

If you only remember five things from this doc:

1. **Crash points are activity boundaries.** Six of them. Don't merge.
2. **Refund recovery never retries.** One shot, then human.
3. **Stale running step = manual intervention.** 5 minutes, no auto-rescue.
4. **Single recoverable position invariant.** 0 or >1 = human.
5. **Quote refresh is its own attempt.** Same supersede semantics as retry.

Everything else is application of these five.
