# Order Executor Rewrite — Agent Brief

> Read [`order_executor_scar_tissue.md`](./order_executor_scar_tissue.md) **first**.
> This brief tells you *what to build*. The scar-tissue doc tells you *what
> the old system did that you must not break.* Do not start writing code
> until you have read both end-to-end.

## 0. TL;DR

Replace the lease-driven singleton worker (`OrderExecutionManager` +
`worker.rs`) with a **Temporal-driven** orchestrator. Postgres stays the
canonical store for orders, vaults, steps, attempts, and provider
operations. Temporal owns workflow state and per-order serialisation.
The system is **not yet deployed** — do a clean-slate rewrite, leave no
vestigial bits, do not preserve the old executor as a fallback.

## 1. System context

`tee-router` is a cross-chain market-order router. The user-visible flow
is:

1. A user gets a quote and is given a **funding vault** address.
2. The user deposits into the vault on the source chain.
3. The router executes a multi-step **plan** — a sequence of provider
   calls (bridges, swaps, exchange trades, unit deposits/withdrawals) —
   that moves value to the user's destination on the destination chain.
4. If execution fails irrecoverably, the router runs a **refund** plan
   that returns value to a user-controlled address. Refunds may
   themselves be multi-hop.

The components in this repo:

- `bin/router-server` — HTTP API + the executor that the rewrite targets.
  Currently runs both the API and the lease-elected worker; the rewrite
  splits these.
- `bin/sauron` — CDC / discovery service (chain event ingestion).
  **Out of scope** for the rewrite.
- `bin/router-loadgen` — load generator. **Out of scope.**
- `apps/admin-dashboard` — operator UI. **Out of scope** beyond
  ensuring its API contract still works.
- `crates/chains`, `crates/devnet`, `crates/hyperliquid-client`,
  `crates/hyperunit-client` — shared libs. **Reused as-is.**

### Provider taxonomy

The executor talks to four trait families (defined in
`bin/router-server/src/services/action_providers.rs`). The rewrite
**reuses these traits unchanged**:

- **`bridge`** — Across, CCTP burn, CCTP receive, Hyperliquid bridge
  deposit, Hyperliquid bridge withdrawal, Universal Router swap.
- **`unit`** — Hyperunit (Bitcoin) deposits and withdrawals.
- **`exchange`** — Hyperliquid spot/perp trades.
- **`custody_action_executor`** — internal direct transfers (used for
  refunds and gas top-ups).

## 2. The decision: Temporal

We picked **Temporal** after surveying DBOS, Restate, and a Postgres-as-bus
custom build. The constraints that drove the choice:

- **Multi-instance API today, multi-region follower stack tomorrow.**
  Temporal cluster handles cross-instance coordination natively.
- **Canonical state stays in Postgres.** Temporal's own state DB is
  acceptable as engine state — Postgres is where business truth lives.
- **HA Postgres future** (Patroni-class streaming replication). Temporal
  is indifferent to whether the *application's* Postgres is HA.
- **Mature observability** — built-in workflow event history, OTel
  exporters, and a UI that operators can use without learning our internal
  state machines.
- **Rust SDK caveat** — `temporal-sdk-core` is mature; the user-facing
  `temporal-sdk` Rust crate lags Go/Java/TS/Python. **Confirm with the
  user before assuming any specific Rust SDK API surface.** If the Rust
  SDK is too thin, an acceptable fallback is to write the worker in Go or
  TypeScript and have it call into Rust over a local HTTP/gRPC surface
  for shared crate logic — but **escalate before doing this.**

## 3. Target architecture

```
                    ┌────────────────────────────────────┐
                    │   Load balancer (auth, CORS, RL)   │
                    └────────────────┬───────────────────┘
                                     │
              ┌──────────────────────┼─────────────────────┐
              ▼                      ▼                     ▼
       ┌─────────────┐        ┌─────────────┐       ┌─────────────┐
       │ router-srv  │        │ router-srv  │  ...  │ router-srv  │
       │ (API only)  │        │ (API only)  │       │ (API only)  │
       └──────┬──────┘        └──────┬──────┘       └──────┬──────┘
              │                      │                     │
              └─────────┬────────────┴─────────────────────┘
                        ▼
              ┌──────────────────┐         ┌────────────────────┐
              │   Postgres       │◄────────┤  temporal-worker   │
              │ (canonical biz   │ activity│  (1 process; can   │
              │  state)          │  reads/ │  scale horizontally│
              │                  │  writes │  on the same task  │
              └──────────────────┘         │  queue if needed)  │
                                           └─────────┬──────────┘
                                                     ▼
                                           ┌────────────────────┐
                                           │  Temporal cluster  │
                                           │  (engine state in  │
                                           │   its own DB)      │
                                           └────────────────────┘
```

- **`router-server`** instances run **API only.** No worker pass, no lease
  table, no executor. They write to Postgres and start/signal workflows
  via the Temporal client.
- **`temporal-worker`** is a new binary (likely `bin/temporal-worker`)
  that hosts workflow + activity definitions and polls Temporal task
  queues. Single process is the deployment target; the design must remain
  correct if the operator runs N processes against the same task queue
  (Temporal serialises per workflow id).
- **Postgres** owns: `router_orders`, `deposit_vaults`,
  `order_execution_legs`, `order_execution_steps`,
  `order_execution_attempts`, `order_provider_operations`,
  `order_provider_operation_hints`, custody/vault tables. Schema is
  reused; cosmetic renames are fine but breaking changes need user
  approval.
- **Temporal cluster** runs Postgres or Cassandra under its own creds.
  *Not* the same Postgres as business state.
- **Load balancer** in front of `router-server` already handles auth,
  CORS, and rate limiting. **Do not add these at the app layer.**

## 4. Scope

### Dies entirely

- `bin/router-server/src/services/order_executor.rs` (15.3k lines).
- `bin/router-server/src/worker.rs` (lease loop, worker pass driver).
- The worker-lease tables and `worker_lease`-related migrations
  (lease/fence rows). **Confirm before dropping migrations** — schema
  history may need to stay clean rather than pruned.
- The 14-phase reconciliation loop as a coupled scheduler. Each phase
  becomes a workflow or workflow branch; some collapse into Temporal
  primitives (e.g. stale-running-step recovery is a workflow timer).
- `OrderExecutionCrashInjector` and the in-process crash-injection harness
  in its current shape — **the *concept* survives** as test seams, but
  the Temporal SDK's own failure-injection facilities replace the bespoke
  enum.

### Survives unchanged

- Provider trait surface (`bridge`, `unit`, `exchange`,
  `custody_action_executor`).
- `crates/chains`, `crates/devnet`, `crates/hyperliquid-client`,
  `crates/hyperunit-client`.
- Postgres schema for orders/vaults/steps/attempts/provider operations.
- The HTTP API surface exposed by `router-server`. Internal handlers can
  change, but the JSON contract operators and the admin dashboard rely on
  must not break without explicit user sign-off.
- Quoting (`market_order_planner.rs` and friends).
- Telemetry hooks (`telemetry.rs`) — extend, don't replace.

### Gets rebuilt

- The execution pipeline (planning → execution → finalisation) as
  Temporal workflows + activities.
- Refund planning and execution as a child workflow.
- Stale-quote refresh as a workflow-internal step (Temporal timers,
  not DB-driven cron).
- Provider operation hint ingestion (currently DB-claim-and-process)
  becomes a Temporal **signal** to the order workflow.

## 5. Workflow / activity decomposition

> This is a **starting shape**, not a contract. Refine as you read the
> codebase. Cite the scar-tissue doc when justifying deviations.

### Workflows

- **`OrderWorkflow(order_id)`** — one execution per order, keyed by
  `order_id` (the workflow id). Lifecycle:
  1. Wait for `FundingVaultFunded` signal (or activity-poll fallback).
  2. Materialise execution plan (activity) — corresponds to scar-tissue
     §1's `process_planning_pass`.
  3. For each step in step-index order: optionally refresh stale quote
     (sub-activity), execute step (activity), wait for terminal observation.
  4. On step failure: apply retry/refund decision (scar-tissue §5) — if
     retry, create new attempt and resume from the failed step's index;
     if refund, start `RefundWorkflow` as a child.
  5. On completion: finalise (activity) and exit.
  6. Manual-intervention transitions exit the workflow with a custom
     failure type that the operator surface can recognise.
- **`RefundWorkflow(order_id, recoverable_position)`** — child of
  `OrderWorkflow`. Single attempt — refund failures go straight to
  `RefundManualInterventionRequired` (scar-tissue §5, §6).
- **`OrderHousekeepingWorkflow`** — periodic, replaces the maintenance
  phases of the worker pass that don't naturally hang off an order
  workflow (e.g. terminal custody vault sweeps, released custody sweeps).
  Use a Temporal cron schedule, not an in-process timer.

### Activities (one side-effect each, idempotent)

Group by which scar-tissue crash point they straddle:

- **DB-only**: `materialise_plan`, `transition_order_status`,
  `transition_step_status`, `record_attempt`, `supersede_steps_from`,
  `record_failed_attempt_snapshot`, `align_vault_status`.
- **Provider-call**: `execute_across_bridge`, `execute_cctp_burn`,
  `execute_cctp_receive`, `execute_hl_bridge_deposit`,
  `execute_hl_bridge_withdrawal`, `execute_unit_deposit`,
  `execute_unit_withdrawal`, `execute_hl_trade`,
  `execute_universal_router_swap`, `execute_custody_transfer`.
- **Provider-observation**: `verify_unit_deposit_chain_evidence`,
  `verify_provider_status` (polling), `recover_across_from_logs`,
  `cancel_hl_trade_on_timeout`.
- **Quoting**: `quote_refresh`, `quote_refund`.

**Every activity must be idempotent at the DB level** even though
Temporal guarantees at-least-once semantics. The CAS patterns from
scar-tissue §14 (lost-CAS = benign skip) are still the right shape; do
not weaken them.

### Signals

- `provider_operation_hint` — external attestation (CCTP, Across, etc.).
  Replaces `process_provider_operation_hints` (worker pass phase 2).
- `funding_vault_funded` — fired by the API when a deposit is observed.
  Removes the polling pass.
- `manual_refund_trigger` — operator-initiated refund.
- `manual_intervention_release` — operator clearing a wedged order.

### Queries

The API reads order state from Postgres directly, not via Temporal
queries. Temporal queries are reserved for **operator-only** debug
endpoints (e.g. "show me the workflow's current cursor"). Do not build
the API on top of queries — it couples request latency to worker
availability.

## 6. The five must-honour invariants

From scar-tissue §17. Tests for these are **mandatory** before merge:

1. **Six persistence boundaries are six activities.** Don't merge them.
2. **Refund attempts never retry.** One shot, then human.
3. **Stale running step (≥ 5 min, no checkpoint) → manual intervention.**
   No auto-rescue.
4. **Refund requires exactly one recoverable position.** 0 or >1 = human.
5. **Quote refresh is its own attempt** with supersede semantics, not a
   step-internal retry.

If your design forces you to violate any of these, **stop and escalate**.
Do not silently relax an invariant.

## 7. Repo conventions (read before writing code)

Pulled from `CLAUDE.md`, the repo's existing patterns, and prior
agreements with the user:

- **No mocking of chains in tests.** Tests run against a real Anvil +
  Bitcoin regtest devnet (`etc/compose.devnet.yml`, `just devnet`).
  Reach for unit tests sparingly; integration tests against the devnet
  are the default.
- **Mock adapters speak the real API shape, byte-for-byte.** When a
  third-party API (Across, CCTP attestation, Hyperliquid, Hyperunit) must
  be mocked in integration tests, the mock returns the *real* JSON shape
  the live API returns. There is **one HTTP client** per provider; tests
  point it at the mock URL, prod points it at the real URL. Do not
  introduce a second "test-only" client.
- **No vibe-coded software.** If there is no obviously clean/idiomatic
  solution to a problem, **stop and ask the user.** Do not improvise.
- **Prioritise clean over low-churn.** This is a rewrite — large diffs
  are expected. Do not preserve old code structure to keep the diff
  small.
- **No vestigial bits.** Delete the old executor, the worker pass, the
  lease tables, the crash injector. No `// removed`, no `_unused`
  rebrand, no compatibility shims.
- **Errors use `snafu` with domain-specific variants.** No `anyhow` in
  library code. Match the existing error taxonomy in
  `bin/router-server/src/error.rs`.
- **DB access uses raw `sqlx`.** No ORM. Existing repo modules
  (`db/order_repo.rs`, `db/vault_repo.rs`) are the model.
- **No app-layer auth, CORS, or rate limiting.** The load balancer
  handles those. Do not add them in `router-server`.
- **Logging via `tracing`.** Telemetry events use `telemetry::record_*`
  helpers — extend the existing surface, don't fork it.
- **Comments only when the *why* is non-obvious.** Don't narrate code.
- **Don't create planning/decision/summary docs unless explicitly asked.**
  This brief and the scar-tissue doc are the only ones the user has
  authorised.

## 8. Open questions — escalate, do not decide alone

1. **Rust Temporal SDK readiness.** Confirm `temporal-sdk` (Rust) is
   sufficient for non-trivial workflows: signals, queries, child
   workflows, durable timers, retry policies, continue-as-new. If not,
   propose Go or TypeScript for the worker binary and **wait for
   approval.** As of `temporalio-macros` 0.4.0, the macro DSL leaks
   `anyhow` into consumer `Cargo.toml` files; treat this as a known SDK
   paper cut, not a violation of the no-`anyhow` rule.
2. **Temporal deployment target.** Self-hosted Temporal cluster
   (compose-managed locally, Kubernetes in prod) vs Temporal Cloud. The
   user has not committed.
3. **Workflow id collision strategy.** `OrderWorkflow` keyed on
   `order_id` is the obvious choice, but reuse-after-completion needs a
   policy (`AllowDuplicate`? `RejectDuplicate`? `TerminateIfRunning`?).
4. **Hint signal vs activity-driven hint poll.** Signals are cleaner but
   require the API process (or sauron) to know the workflow id. If
   hint-source services don't have that mapping, an activity that
   polls the hint table is acceptable. Decide explicitly.
5. **Migration of in-flight orders.** The system is not deployed, so
   "in-flight" means dev/staging only. Confirm with the user that we can
   throw away dev DB state during cutover rather than building a
   migration path.
6. **Database schema preservation.** Are operators allowed to drop and
   recreate the worker-lease tables and any executor-only columns, or
   must the migration history stay append-only? Default to append-only
   migrations unless the user says otherwise.
7. **Observability hand-off.** The current telemetry event names
   (`order.refund_manual_intervention_required`,
   `execution_step.completed`, etc.) are surfaced to dashboards. Confirm
   we may keep emitting the same names from Temporal activities, or if
   the dashboards expect a redesign.
8. **Crash injection in tests.** The bespoke `OrderExecutionCrashPoint`
   enum is going away, but the *property* (every persistence boundary
   tolerates a crash before and after) must still be tested. Propose a
   replacement (Temporal SDK fault-injection, activity-level test
   harnesses) and get sign-off before designing the test suite.

## 9. Definition of done

A pull request is mergeable when **all** of the following are true:

- [ ] Both this brief and the scar-tissue doc have been read end-to-end.
- [ ] Every numbered section in the scar-tissue doc maps to one of:
  (a) a Temporal workflow/activity that implements the property,
  (b) a test asserting the property, or
  (c) an explicit "retired because…" entry in the rewrite design notes.
- [ ] All five must-honour invariants (§6) have dedicated integration
  tests passing against the real devnet.
- [ ] `bin/router-server/src/services/order_executor.rs` is deleted.
- [ ] `bin/router-server/src/worker.rs` is deleted.
- [ ] Worker-lease tables are removed (or their removal scheduled — see
  §8.6).
- [ ] No code path imports the deleted executor (`grep` clean).
- [ ] The new `bin/temporal-worker` binary builds and runs against a
  local Temporal cluster brought up by `etc/compose.*`.
- [ ] `router-server` instances are stateless w.r.t. order execution —
  killing any one of them mid-request does not lose work.
- [ ] The 14 worker-pass phases each have a documented disposition
  (workflow / signal / cron / retired).
- [ ] All `cargo test`, integration tests, and admin-dashboard tests
  pass.
- [ ] `just fmt && just lint` are clean.
- [ ] No `TODO`, `FIXME`, or `// removed` comments left in new code.
- [ ] Open questions §8 are resolved (in this brief or in commit
  messages); none are silently decided.

## 10. Anti-goals

Things you might be tempted to do — **don't**:

- Don't write a "compatibility layer" that lets the old executor
  coexist with the new worker. There is no traffic to protect.
- Don't reintroduce the worker-lease pattern inside the Temporal worker.
  Temporal's task queue *is* the lease.
- Don't move auth/CORS/rate-limiting into the Rust app to "make it self-
  contained." The LB owns that, period.
- Don't merge two of the six crash points to "simplify." Read scar-tissue
  §2 again.
- Don't add a feature flag to toggle Temporal vs the old executor. Pick
  one (Temporal) and delete the other.
- Don't build a custom workflow event log alongside Temporal's. Temporal's
  history is the log.
- Don't optimise for hypothetical future providers. The current four
  trait families are the surface area.

## 11. Recommended first PRs

A suggested order of work — proposals, not commands:

1. **Compose-up Temporal locally** (`etc/compose.temporal.yml`) and
   document `just temporal-up`. Verify the Rust SDK can register a
   trivial workflow against it.
2. **Define workflow + activity skeletons** with no implementation, just
   types, signal surfaces, and child-workflow shape. PR for design
   review.
3. **Implement the happy-path `OrderWorkflow`** (Funded → Executing →
   completion) with a single-step plan. Devnet test.
4. **Add multi-step + retry semantics** (scar-tissue §5).
5. **Add stale-quote refresh** (scar-tissue §7).
6. **Add `RefundWorkflow`** (scar-tissue §8, §9).
7. **Add hints + provider observation recovery** (scar-tissue §10, §11).
8. **Add stale-running-step manual intervention** (scar-tissue §6).
9. **Wire `router-server` to start/signal workflows; delete the old
   executor + worker.**
10. **Operator UX & telemetry hand-off.**

Each PR should be small enough to review in one sitting. If it isn't,
split it.

---

If anything in this brief contradicts the scar-tissue doc, the
scar-tissue doc wins for *what the system did*, and this brief wins for
*what we're going to build instead*. If a contradiction implies a
behaviour change, escalate before implementing.
