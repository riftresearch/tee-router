# Money-Path Failure Map

**Status:** In progress — execution-core crash/double-execution safety **and R1 (refund of
in-flight money) RESOLVED — money-safe by design.** R2–R4 + R1-c open.
**Target property:** *No unrecoverable money.* Not "no failures" — failures are expected; the
guarantee we're auditing is that every failure leaves funds in a known place with a defined
recovery, and never double-spends or strands.

**Method (so you can grade it):** every claim below cites `file:line`. Each item is tagged
**CONFIRMED** (read the code), **PRELIMINARY** (grep-level signal, needs a deeper read), or
**OPEN** (located, not yet verified). Nothing here is a code change — this is a read-only map.
The plan is to resolve every OPEN, then *prove* the conclusions on the devnet by injecting
failures (kill a worker at each crash point, observe where the money lands).

Scope: the funds lifecycle — `temporal-worker/src/order_execution/` (workflow + activities) and
`router-core/.../custody_action_executor.rs`. ~5 files, not the whole tree.

---

## 1. The existing safety net (4 layers)

This system already invests heavily in failure-safety. The audit is **finding holes in this
net**, not building one. The four layers:

### Layer 1 — No double-execution (idempotency)
`activities/idempotent_step.rs`. Every step that fires a venue side effect funnels through
`execute_idempotent_step` (`idempotent_step.rs:85`):
- Deterministic key `(order_id, operation_type, step_index)` collapses retries onto one
  `order_provider_operations` row (`idempotent_step.rs:152`). Refund-recovery steps add
  `execution_attempt_id` so a fresh refund route doesn't inherit a dead op (`:156`).
- **Lookup-before-side-effect**: if a row exists, short-circuit *before* contacting the venue
  (`:96`) → retries never re-fire.
- **Persist-before-side-effect with a unique-key upsert** (`:200`): two racing workers both try
  to insert the `Planned` row; the unique constraint lets one win; the loser re-reads the
  winner's row and short-circuits (`:233`–`255`). **CONFIRMED** — this is the Postgres-native
  claim primitive; sound for the racing case.
- **One step type is intentionally NOT behind this guard**: `HyperliquidClearinghouseToSpot`
  (`execution.rs:820`) relies on the checkpoint (Layer 2) alone. → **OPEN R2.**

### Layer 2 — Crash-mid-fire disambiguation (checkpoint + stale watchdog)
The dual-write problem (the venue side effect and the DB update aren't atomic) is handled by a
**checkpoint written immediately before the side effect**:
- `run_step_dispatch` (`execution.rs:832`) sequence: (1) persist `running`, (2) master-switch
  check, (3) pre-exec stale-quote check, **(4) write `provider_side_effect_about_to_fire`
  checkpoint (`execution.rs:899`), (5) fire the side effect (`execution.rs:922`)**, (6)
  reconcile output balance, (7) persist receipt/status. **CONFIRMED: checkpoint precedes the
  side effect (4→5)** — so a crash before firing is conservatively classified.
- When a step is stuck too long, the `stale_watchdog` fires (`workflows/order.rs:987`) and calls
  `classify_stale_running_step_for_deps` (`execution.rs:1766`), which returns one of three
  decisions:
  | Decision | Meaning | Workflow action (`order.rs:1000`) | Money-safety |
  |---|---|---|---|
  | `DurableProviderOperationWaitingExternalProgress` | Submitted/WaitingExternal op **with** checkpoint → side effect landed | `continue` (keep waiting) | **SAFE** |
  | `AmbiguousExternalSideEffectWindow` | checkpoint present, no durable op → can't tell if money moved | `RefundRequired` | **hinges on Layer 3** |
  | `StaleRunningStepWithoutCheckpoint` | no checkpoint → side effect never started | `RefundRequired` | **SAFE** (money didn't move) |

### Layer 3 — Refund by position discovery (reconcile, don't assume)
`activities/refund.rs` + `refund_builders.rs` (~4,500 LOC). Refunds **read actual balances** and
only move what's really there, rather than blindly returning the source amount:
- `deposit_vault_balance_raw` (`refund.rs:79`, `:257`), `custody_vault_balance_raw` (`:300`),
  `inspect_hyperliquid_spot_balances` (`:609`), each gated by `raw_amount_is_positive`
  (`:84`, `:262`, `:305`). Annotated *"§8 refund position discovery, §16.1 balance reads."*
- **PRELIMINARY**: this is why `Ambiguous → Refund` is probably double-spend-safe for *at-rest*
  money — if the funds already moved forward, the source balance reads ~0, so nothing is
  double-refunded. The unverified case is **in-flight** money (mid-bridge, at neither end) — see
  **OPEN R1**.

### Layer 4 — Authoritative-balance reconciliation (trust the chain over the DB)
- `apply_authoritative_output_balance` (`execution.rs:923`) reconciles a step completion against
  the *actual* on-chain output balance after firing.
- `recover_failed_cctp_receive_completion` (`idempotent_step.rs:272`): a *failed* CCTP-receive op
  is reconciled against the destination custody-vault balance — if the funds already arrived
  (`observed >= expected`), the step is marked completed (`cctp_receive_already_claimed`) instead
  of failing. **CONFIRMED** — the money-safe pattern. → coverage question in **OPEN R4.**

---

## 2. Crash-recovery walkthrough (the core map)

For a single fund-moving step, where the money is at each crash point and what recovers it:

| Crash point | Checkpoint | Provider op | Stale classification | Action | Money outcome |
|---|---|---|---|---|---|
| Before step 4 (checkpoint) | none | none | `WithoutCheckpoint` | Refund | money at source → refund returns it · **SAFE** |
| Between 4 and 5 (checkpoint written, not fired) | yes | none | `Ambiguous` | Refund | money still at source → refund returns it · **SAFE (conservative)** |
| During 5 (side effect firing) | yes | maybe `Planned` | `Ambiguous` | Refund | **money may be in-flight** → CCTP auto-claimed at dest; otherwise → admin queue · **SAFE (R1 resolved)** |
| After 5, op `Submitted`+checkpoint | yes | `Submitted` | `Durable…Progress` | wait | money moved, awaiting confirmation · **SAFE** |
| Two workers race the same step | n/a | unique-key conflict | n/a | loser short-circuits | one fire only · **SAFE** |

The whole map reduces to **one unverified cell**: a crash *during* the side effect, where money
is in-flight, routed to refund — does position discovery find in-flight money?

---

## 3. Open risk areas (located, falsifiable — to resolve next)

- **R1 (highest value) — Ambiguous→Refund vs. in-flight money — ✅ RESOLVED, money-safe.**
  When an ambiguous step routes to refund, position discovery
  (`discover_single_refund_position_with_deps`, `refund.rs:215`) does **not** blind-refund:
  - **In-flight CCTP (the dominant bridge case) is auto-claimed at the destination.**
    `cctp_receive_claim_positions_from_steps_and_operations` (`refund.rs:477`) finds the
    *completed burn* (money already left source), reads the *attested burn amount*, and creates a
    `CctpReceiveClaim` position to finish the receive at the destination — and **skips if a claim
    is `already_in_flight`** (no double-claim). **SAFE.**
  - **Money it can't tenably account for → admin queue, never a blind refund.** Empty discovery →
    `refund_single_position_untenable` (`refund.rs:691`) → `finalize_refund_required` →
    `OrderTerminalStatus::RefundRequired` (`workflows/refund.rs:70,112,382`). This matches the
    documented invariant *"cannot determine safe continuation → refund_required."* **SAFE.**
  - **Multiple positions are drained iteratively, not dropped.** The refund workflow `loop`s over
    `discover_single_refund_position` (`workflows/refund.rs:57`), re-reading actual balances each
    pass, so a single-pass "dropped" position (`refund.rs:728`) is recovered on a later pass.
  - **Residual — R1-c (refinement, not a double-spend risk):** confirm the loop's
    `Refunded`-vs-`RefundRequired` exit (`workflows/refund.rs:263` vs `382`) can't mark `Refunded`
    while *expected* money remains unaccounted (premature success). And note: **non-CCTP in-flight
    legs** (Across, Unit withdrawal, HL clearinghouse↔spot mid-transfer) have **no claim model**,
    so they fall to Untenable→admin — *safe* (no loss), but **not auto-recovered**; whether to add
    claim models there is a product call, not a safety bug.
- **DOC-ROT — code cites scar `§6/§8/§16.1` that exist in no docs file.** `execution.rs:899`,
  `refund.rs:25,39` reference scar-tissue sections; `docs/order_executor_scar_tissue.md` is only
  18 lines of invariants with no such sections. The cited design rationale is missing from the
  repo (history/PRs only) — hurts auditability. Low-risk, worth restoring or de-referencing.
- **R2 — `HyperliquidClearinghouseToSpot` not behind the idempotent guard** (`execution.rs:820`).
  It relies on the checkpoint alone. *Next:* confirm a retry/crash can't double-fire the
  clearinghouse→spot transfer; verify the upstream checkpoint idempotency it claims.
- **R3 — stale-watchdog false-fire** (known issue: *"false-fires under congestion"*). Confirm a
  false-fire on a *healthy* step is benign: it re-classifies, finds the durable op, returns
  `Durable…Progress` → `continue` (no harm). Verify it can't push a healthy in-progress step to
  `Ambiguous → Refund`. *Next:* read the watchdog trigger/timer + `provider_operation_has_checkpoint`.
- **R4 — reconcile-by-observed-balance coverage.** Layer 4 exists for CCTP-receive and via
  `apply_authoritative_output_balance`. *Next:* enumerate every `OrderExecutionStepType`
  (`models.rs:1204`) and mark which have authoritative-balance reconciliation and which trust the
  DB — the gaps are where a misclassification could lose money.

---

## 4. Next steps

1. ~~Resolve **R1**~~ ✅ done — refund of in-flight money is money-safe (see §3).
   `docs/order_executor_scar_tissue.md` read (18 lines, invariants only; the cited scar §§ are
   missing — see DOC-ROT finding).
2. Resolve **R2–R4** + the **R1-c** refinement (refund loop exit logic).
3. Decide (product) whether non-CCTP in-flight legs warrant claim models or stay admin-recovered.
4. **Prove it on the devnet**: with the local full stack, inject a crash at each row of the §2
   table (kill `router-worker` mid-step) and at the racing case (two workers), and observe the
   money outcome end-to-end. Real devnet, no mocks.

## Verification ledger (spot-check any line)
- Idempotency guard: `activities/idempotent_step.rs:85,96,152,200,233`
- Checkpoint-before-side-effect: `activities/execution.rs:832,899,922`
- Stale classifier: `activities/execution.rs:1766,1800,1810,1820,1831`
- Workflow action on decision: `workflows/order.rs:987,1000,1010`
- Refund balance reads: `activities/refund.rs:25,79,254,300,609`
- Refund position discovery: `activities/refund.rs:215,355,477,684,691`
- In-flight CCTP claim model: `activities/refund.rs:477,497,501`
- Untenable → admin queue: `workflows/refund.rs:57,70,112,367,382`; `order.rs:1041`
- Authoritative reconciliation: `activities/execution.rs:923`; `idempotent_step.rs:272`

## Side findings (tracked, surfaced during the R1 devnet proof)

1. **Integration suite was silently red + ungated (process gap).** `integration-tests/tests/
   router_runtime_e2e.rs` (the only full-stack e2e suite) is `#[ignore = "integration: spawns
   devnet stack"]`, excluded from the fast `test` recipe and from routine CI — it was red across
   ≥2 merges undetected. **Action:** wire `just test-integration` into a gate so it can't rot.
2. **Recipient-agnostic regression — FIXED (`da96515`).** `719548f` removed recipient from the
   quote API (+`deny_unknown_fields`) but the e2e suite still sent it → `422`. Fixed by moving
   recipient quote→order; `router_api_worker_sauron_refunds_failed_mock_routes` now PASSES.
3. **Suspicious route selection — NEEDS CLASSIFICATION (deferred).** For Base.ETH→Ethereum.USDC
   the planner picks a 5-hop `across→unit_deposit→hyperliquid_trade→hyperliquid_bridge_withdrawal
   →cctp_bridge` route instead of the expected `across+velora` (matrix test failure). Likely
   triggered by `620d1f0 "score routes by effective usd cost"`. **Open:** cost model picking
   pathological multi-hop routes (real, money-relevant, affects every order) vs. mock synthetic
   rates making it legitimately cheapest (stale test). **Do NOT** update the matrix expectation
   until classified — that would bury a possible routing defect. Blocks gating the matrix test.
