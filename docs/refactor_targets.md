# Refactor Targets — Idiomatic, Performant, Readable

This is a living document of what to fix in the temporal-worker / router-temporal codebase to make it pleasant for humans to read and maintain. The audit was done 2026-05-10 after the brief PR1-10 work and the overnight loadgen loop landed.

The goal is not "no code smells." The goal is **respect for the next human who has to load this into their head.** Every refactor target below is justified by either (a) how a god-tier engineer would expect this to be shaped, or (b) a concrete bug-class the current shape allows.

---

## 1. Function and file size norms

Sourced from matklad ("Size Matters", 2025), TigerBeetle TIGER_STYLE.md, the Linux kernel coding style, Robert Martin's Clean Code, the Google C++/Go style guides, and the Rust API guidelines.

### Function length

| Source | Threshold | Rule or guideline |
|---|---|---|
| Uncle Bob (Clean Code) | ~20 lines, "hardly ever" larger | dogmatic |
| Linux kernel | one or two screenfuls (~24–48 lines) | guideline |
| Google C++ | ~40 lines is the threshold to "think about whether it can be broken up" | guideline |
| matklad (Aleksey Kladov) | ~60–70 lines (Schelling point: "sharp discontinuity between a function fitting on a screen and a slightly larger function where you can't immediately see the end of it") | guideline |
| **TigerBeetle TIGER_STYLE.md** | **70 lines, 100 columns, "without exception"** | **hard rule** |
| Rust API Guidelines / Google Go | no number — refactor by feel | none |

**Consensus:** a function should fit on one screen. Length scales **inversely with complexity and indentation depth**. A flat 200-line dispatcher is fine; a 50-line function nested 5-deep is not.

**Working number for this repo:** **soft 70, hard 100, with the indentation/complexity exception.** Any function over 100 lines requires either decomposition or an explicit comment justifying why it's flat (e.g. exhaustive enum dispatch).

### File length

No fixed rule. matklad's reframing is the most useful: stop optimizing raw lines, optimize the **interface-to-body ratio** ("minimize the cut") — small public surface, meaty private body.

**Working test for this repo:** if a file has more than ~20 public items or covers more than one bounded concern, split it. Concrete: `bin/temporal-worker/src/order_execution/activities.rs` at ~11k LOC and `workflows.rs` at ~2.3k LOC fail this test.

### Line length

Universal: **100 columns**, chosen so two copies of code fit side-by-side on a 16:9 display (lets you compare caller and callee).

---

## 2. Audit results — what we have today

Done 2026-05-10 against commit `07101ca` (post-cycle 2) plus the cycle 3 ERC20 retry-classifier fix (in flight at audit time).

### Strong (keep doing)

- Decision/outcome enums are NOT stringly-typed: `StepFailureDecision`, `OrderTerminalStatus`, `RefundTerminalStatus`, `OrderWorkflowPhase`, `RecoverablePositionKind`, `PersistenceBoundary`, `ProviderOperationHintDecision`. All `Copy + PartialEq + Eq` where appropriate.
- `StaleQuoteRefreshUntenableReason` is **exemplary**: `#[serde(tag = "reason", rename_all = "snake_case")]` with structured payload + `reason_str()` method that the compiler keeps in sync via exhaustive match. Use this as the template.
- Sum-typed outcomes (`SingleRefundPositionOutcome::{Position, Untenable}`, `RefundPlanOutcome::{Materialized, Untenable}`, `RefreshedQuoteAttemptOutcome::{Refreshed, Untenable}`) avoid the `Option<T> + reason: Option<String>` anti-pattern. Keep this shape.
- Pattern matching is exhaustive — only **5 `_ =>` catch-alls in 11k lines** of activities.rs (excellent).
- `Arc<dyn Trait>` usage is principled (provider trait families).
- Iterator usage is clean — no `for ... in` loops where `.iter().filter().count()` would work.
- `.expect()` discipline: ~35 of 42 are in `#[cfg(test)]`; production expects encode real local invariants.

### Weak — refactor targets (ordered by impact-per-effort)

#### T1 (low effort, high signal): Newtype the workflow IDs

`bin/temporal-worker/src/order_execution/types.rs:15-20`:

```rust
pub type WorkflowOrderId = Uuid;
pub type WorkflowAttemptId = Uuid;
pub type WorkflowStepId = Uuid;
pub type WorkflowVaultId = Uuid;
pub type WorkflowProviderOperationId = Uuid;
pub type WorkflowHintId = Uuid;
```

These are **type aliases**, not newtypes. The compiler will gladly let you swap `attempt_id` for `step_id` anywhere they appear together (`ExecuteStepInput`, `ClassifyStepFailureInput`, etc. — many sites).

**Refactor:** make each a `#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)] struct WorkflowOrderId(pub Uuid);` with a `From<Uuid>` and `as_uuid()` accessor. Catches a real class of bug for ~30 minutes of work + a global rename.

#### T2 (medium effort, dominant smell): Typed `OrderActivityError` enum

Today every error path in `activities.rs` is `.map_err(activity_error)` or `.map_err(activity_error_from_display)` where `activity_error(message: impl ToString)` wraps `io::Error::other`. Result: ~10k lines of stringly-typed errors. The Temporal SDK forces `ActivityError` at the boundary, but nothing forces the activity body to lose type info.

**Refactor:** introduce an `OrderActivityError` enum (Snafu) covering the failure modes (`DbQuery`, `ProviderQuote`, `ProviderExecute`, `MissingHydration { vault_role, step_id }`, `InvariantViolation { invariant }`, etc.). Convert at the SDK boundary in one place. Failure sites become `.map_err(OrderActivityError::ProviderQuote)` instead of opaque format strings. Half-day refactor; massively improves debuggability and grep-ability.

#### T3 (medium effort, real readability win): Polymorphic step dispatch

`bin/temporal-worker/src/order_execution/activities.rs:2423-2566`: 144-line `match step.step_type` over 11 variants that all do the same shape:

```rust
StepType::AcrossBridge => {
    let request: AcrossExecuteRequest = serde_json::from_value(step.request.clone())?;
    let provider = deps.action_providers.bridge("across").ok_or_else(...)?;
    provider.execute_bridge(...).await?
}
StepType::CctpBridge => { /* same shape, different provider */ }
// ... 9 more like this
```

**Refactor:** introduce a `StepDispatch` trait with one method, and have each step type produce a `Box<dyn StepDispatch>` from the registry. The big match collapses to `dispatch_for(step.step_type).execute(deps, step).await`. Or use the `enum_dispatch` macro if a trait feels heavy. Half to one day.

#### T4 (low-to-medium effort, type-modeling win): Stop leaking `serde_json::Value` in load-bearing payloads

`types.rs:471-473` — `RefreshedQuoteAttemptOutcome::Refreshed { failure_reason: Value, superseded_reason: Value, input_custody_snapshot: Value }`. These are structured concepts that get re-parsed downstream.

**Refactor:** introduce `FailureReason`, `SupersededReason`, `InputCustodySnapshot` typed structs. Audit blobs (`StepExecuted.response`, telemetry-only fields) can stay `Value`; load-bearing fields must not.

#### T5 (low effort, naming win): Booleans where enums belong

`LoadManualInterventionContextInput.refund_manual: bool` (`types.rs:155`), `AcknowledgeManualInterventionInput.refund_manual: bool` and `zombie_cleanup: bool` (`types.rs:300, 302`).

**Refactor:** `enum ManualInterventionScope { OrderAttempt, RefundAttempt }` and `enum AcknowledgeReason { OperatorTerminal, ZombieCleanup }`. Reads dramatically better at call sites: `acknowledge(..., ManualInterventionScope::OrderAttempt)` vs `acknowledge(..., false)`.

#### T6 (low effort, type-modeling win): Newtype raw amounts

`SingleRefundPosition.amount: String`, `FundingVaultFundedSignal.observed_amount_raw: String` (`types.rs:125, 375`).

**Refactor:** `struct RawAmount(U256)` (or `String` if we genuinely need arbitrary precision) with `try_from_str`, `as_u256`, `serialize as decimal string`. Domain invariant ("non-negative integer in raw units") moves from runtime checks to the type.

#### T7 (medium effort, real concern): Typestate for `OrderWorkflowPhase`

`types.rs:529-536` — `OrderWorkflowPhase` is a flat enum stored as a mutable field on the workflow state. Transitions (`WaitingForFunding → Executing → RefreshingQuote / Refunding / Finalizing`) are enforced only by code discipline in `workflows.rs`, not the type system.

**Refactor:** `enum OrderWorkflowState { WaitingForFunding(WaitingState), Executing(ExecutingState), RefreshingQuote(RefreshingState), Refunding(RefundingState), Finalizing(FinalizingState) }` where each variant carries phase-specific data. Compiler enforces "you cannot read execution-attempt context while in the WaitingForFunding phase." Heavier refactor; do after T1-T6 land.

### File-level refactor targets

#### F1 (the big one): Split `activities.rs` (~11k LOC)

Way over every god-tier threshold. Suggested decomposition by concern:

```
bin/temporal-worker/src/order_execution/activities/
├── mod.rs              # OrderActivities struct, registration, deps
├── persistence.rs      # All persist_* boundary activities (PR4-shaped)
├── execution.rs        # execute_step, settle_provider_step, classify_step_failure
├── refresh.rs          # check_pre_execution_stale_quote, compose_refreshed_quote_attempt, materialize_refreshed_attempt
├── refund.rs           # discover_single_refund_position, materialize_refund_plan + the per-handle helpers
├── refund_quote/       # quote helpers per source handle (external_custody, hyperliquid_spot)
├── refund_materialize/ # materialize helpers per source handle
├── hint.rs             # verify_*_hint, poll_provider_operation_hints
├── hydration.rs        # hydrate_destination_execution_steps + per-vault-role helpers
├── observation.rs      # recover_across_onchain_log
├── stale_running.rs    # classify_stale_running_step + manual intervention helpers
├── telemetry_helpers.rs # record_activity wrapper + metric emit helpers
└── types_internal.rs   # Activity-internal helper types (not the public types in types.rs)
```

Each file should land in the 1,000-2,500 LOC range — still big, but each has one bounded concern. The 329-line `hydrate_destination_execution_steps` should go further: split per vault role into `hydrate_destination_execution(steps, deps)`, `hydrate_source_deposit(steps, deps)`, `hydrate_hyperliquid_spot(steps, deps)`.

#### F2: Split `workflows.rs` (~2.3k LOC)

Mixes 4-5 workflow types in one file. Suggested:

```
bin/temporal-worker/src/order_execution/workflows/
├── mod.rs              # registration, shared types
├── order.rs            # OrderWorkflow
├── refund.rs           # RefundWorkflow
├── quote_refresh.rs    # QuoteRefreshWorkflow
├── provider_hint_poll.rs # ProviderHintPollWorkflow
└── shared.rs           # shared helpers (db_activity_options, execute_activity_options, etc.)
```

Workflows don't share much state; this split is easier than the activities one.

#### F3: The 329-line `hydrate_destination_execution_steps`

Already called out in F1. The hydrator does three independent vault-role concerns interleaved in one `for mut step in steps` loop with `set_json_value` mutating opaque `Value` JSON. Split per vault role; hide the JSON manipulation behind typed step builders.

#### F4: The 227-line `verify_unit_deposit_hint`

Sequential validation pipeline written as nested `if let Some(...) = ... else { return ... }`. Should be a `Result`-returning chain of `verify_chain → verify_amount → verify_recipient` helpers. Each helper ≤ 50 lines.

#### F5: Several 188-194 line refund quote/materialize functions

Per F1, these split naturally per source handle into `refund_quote/external_custody.rs`, `refund_materialize/hyperliquid_spot.rs`, etc. Each function inside should be ≤ 100 lines.

---

## 3. Smaller cleanups worth doing on the way

- 42 `#[allow(...)]` attributes in activities.rs — most are leftover `#[allow(dead_code)]` from earlier scaffolding. Prune the truly dead.
- Audit the ~7 production `.expect()` calls in activities.rs (the rest are tests). Each one should encode a real local invariant with a comment, or be replaced by `?` with a typed error.
- `ChainId::parse("hyperliquid").expect("valid hyperliquid chain id")` (`activities.rs:2217`) is a constant — make it a `const` or `OnceLock<ChainId>`.

---

## 4. Order in which to land the refactors

To minimize churn and keep diffs reviewable:

1. **T1 newtype IDs** — small, mechanical, catches bugs immediately.
2. **T5 boolean → enum** — same flavor, same scope, do it together.
3. **T6 newtype raw amounts** — same flavor.
4. **T2 typed `OrderActivityError`** — touches every file that uses activity errors but the diff is mostly mechanical `.map_err` rewrites.
5. **F1 split `activities.rs`** — biggest payoff for readability; do once T1/T2 are in to avoid double-rewrites.
6. **F3 + F4 + F5** — function decomposition, fall out naturally from F1.
7. **T3 polymorphic step dispatch** — once activities.rs is split, the `match step.step_type` lives in one file and is easier to refactor in isolation.
8. **F2 split `workflows.rs`** — independent of the activities work; can be done in parallel.
9. **T4 typed payloads** — once errors are typed (T2), this becomes the next-most-valuable typing improvement.
10. **T7 typestate `OrderWorkflowState`** — biggest semantic change; do last, on top of a clean codebase.

Each item should be its own commit, ideally its own PR. The order above keeps every step independently reviewable and revertable.

---

## 5. Why this matters

Code is read more than it's written. Every line over the screen-fit threshold costs a future reader cognitive load. Every primitive-typed ID is a bug waiting to happen. Every stringly-typed error is grep-archaeology when something breaks at 3am.

The current codebase passes the basic tests of being well-architected (Temporal split, persistence boundaries, sum-typed outcomes), but it grew organically through 35+ PRs in a multi-day session. The refactor work above is what turns "it ships and the tests pass" into "the next engineer can pick it up and understand it in an afternoon."

Respect the biological manna of whoever reads this next.
