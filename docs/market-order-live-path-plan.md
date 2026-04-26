# Market Order Live-Money Path — Working Plan

Working doc. Plan of attack for getting a first dust-sized real-money market-order
route running through `router-api` + `router-worker`. Updated as assumptions change.

## Audit snapshot

**Already built:**
- Per-chain `EvmPaymasterActor` with mpsc command queue + oneshot replies
  (`crates/chains/src/evm.rs`). One actor per chain (Ethereum, Base). Managed via
  `JoinSet` with restart-on-failure.
- Commands: `FundVaultAction`, `FundEvmTransaction`. Request struct carries
  target address, value, calldata, operation tag.
- Gas computation is dynamic: `action_value + action_gas × max_fee_per_gas_with_headroom + vault_gas_buffer_wei`.
- Wait-for-receipt polling (250ms / 300s timeout).
- `custody_action_executor` already calls `ensure_native_gas_for_transaction`
  before signing — correct pre-emptive shape.
- Concurrency-safe worker pass semantics (just landed).
- Custody vault roles generalized (dead variants removed).
- Polymorphic quote envelope (`RouterOrderQuote` sum type).

**Missing (ordered by criticality for first live dust run):**
1. Deposit vault address must exist **at quote time** so Across sees a real
   refund address in the quote. Today vaults are created at order-creation time.
2. Real Across adapter — replace router-contract mock shape with the actual
   Across API + SpokePool `depositV3` tx construction + submission via paymaster
   gas sponsorship. Partial fills disabled.
3. Real Unit adapter — fetch real Unit deposit address, sign/send source-vault
   funds to it, wire real status polling.
4. Real Hyperliquid adapter — wire the real SDK/API. Reference pattern exists in
   `bin/router-server/tests/live_market_order_e2e.rs`.
5. E2E live test driving through `router-api` + `router-worker` (not direct
   provider calls); balance invariants; spend guards.
6. **EIP-7702 batched same-chain funding** — optimization. After first green run.

## Ordered plan

### Phase 1 — Quote-time deposit address, order-time vault row  ✅ DONE
**Decision (supersedes an earlier row-at-quote-time sketch):** the deposit
address is deterministically derived from `quote_id` + master key and computed
at quote time for *internal* use only — Across needs it as the `depositor` /
refund slot in the bridge quote request. The DB row is not inserted and the
address is never exposed via any quote endpoint. As far as API consumers are
concerned, the deposit address appears on the `RouterOrderEnvelope`, not the
quote envelope. This sidesteps the "user funds a quoted address before POST
/orders and gets stuck" failure mode without needing a reaper.

Concrete shape:
- New `VaultManager::derive_deposit_address_for_quote(quote_id, deposit_asset)`:
  `salt = keccak256(domain_tag || master_key || quote_id_bytes)`, then
  `address = chain.derive_wallet(master_key, salt).address`. Pure function, no
  DB.
- `OrderManager::quote_market_order` generates `quote_id` upfront, computes the
  address, and feeds it to bridge provider quote requests as
  `depositor_address`. `BridgeQuoteRequest` gains a `depositor_address: String`
  field.
- `/api/v1/quotes` and `/api/v1/quotes/:id` responses are unchanged — the
  address is never exposed.
- `VaultManager::create_vault` (called from `POST /orders`) recomputes the same
  salt from the attached quote and derives the same address. Inserts the vault
  row. The existing `cancel_after` / refund pass handles funded-then-abandoned
  orders as today.
- Tests: quote → bridge provider sees correct depositor → accept → vault row
  has the same address; `/quotes` never returns the address.

**Landed as:**
- `bin/router-server/src/services/deposit_address.rs` — pure derivation
  (`derive_deposit_salt_for_quote`, `derive_deposit_address_for_quote`), domain
  tag `b"router-server-deposit-vault-v1"`.
- `BridgeQuoteRequest.depositor_address: String` added
  (`services/action_providers.rs`). Threaded through
  `OrderManager::quote_market_order` → `best_provider_quote` →
  `quote_across_to_unit`.
- `OrderManager` now carries `Arc<Settings>` for master-key access at quote time.
- `VaultManager::create_vault` looks up the attached order's quote
  (`get_market_order_quote`) and uses the deterministic salt when
  `order_id.is_some()`; falls back to random salt for legacy non-order vault
  paths in tests.
- Tests: `vault_address_matches_deterministic_derivation_from_quote` and
  `quote_envelope_never_exposes_deposit_address` in
  `bin/router-server/tests/vault_creation.rs`, plus four unit tests on the
  derivation function.

### Phase 2 — Real Across adapter
- **2.1 ✅** Real Across types + `AcrossClient` in
  `bin/router-server/src/services/across_client.rs` — camelCase query on
  `GET /swap/approval`, camelCase response (`swapTx`, `inputAmount`,
  `maxInputAmount`, `expectedOutputAmount`, `minOutputAmount`,
  `quoteExpiryTimestamp`, `id`, optional `approvalTxns`). Seven unit tests cover
  query serialization, real-shape fixture round-trip, and numeric parsing.
- **2.2 ✅** `AcrossProvider::quote_bridge` now calls `AcrossClient::swap_approval`
  (no SpokePool ABI — real API returns fully-formed `swapTx` calldata, so we just
  carry it forward in `provider_quote`). Mock `MockIntegratorServer` serves
  `GET /swap/approval` alongside the existing `POST /across/swap/approval` so
  both production and mock-backed tests speak the same shape.
  `order_manager::across_to_unit_quote` reads `maxInputAmount` camelCase.
- **2.3 (blocked on design decision — needs review)** Wire `AcrossProvider::execute_bridge`
  to return `ProviderExecutionIntent::CustodyAction { custody_vault_id, action:
  Call(Evm(EvmCall { to, value, data })) }` built from `quote.provider_quote.
  bridge_quote.swapTx`. The `CustodyActionExecutor::execute` path already
  supports paymaster-funded EVM calls via `ensure_native_gas_for_transaction`
  so no executor change is required.
  **Blocker**: Many integration tests depend on the mock's `POST /across/swap/approval`
  crediting `MockIntegratorLedger.across_fills` (see
  `bin/router-server/tests/vault_creation.rs:2686-2795` and
  `bin/sauron/tests/provider_operation_e2e.rs:545`). Real Across has no such
  endpoint — the bridge happens on-chain via `swapTx` submission. Options:
  (a) Drop the mock-only POST endpoint, rely on on-chain tx only — but no mock
  SpokePool contract exists on the Anvil devnet, and ledger-state assertions
  would need a different source of truth (tx_hash tracking? per-depositor
  balance snapshots?).
  (b) Keep the mock POST endpoint as a test-only "ack" surface that execute_bridge
  notifies after the on-chain call — preserves ledger assertions but means the
  production code has a mock-only side-effect.
  (c) Build a minimal mock SpokePool contract deployed to the test Anvil that
  emits a `Bridged` event on any call, and have the mock HTTP server index those
  events via `eth_getLogs` to keep ledger state. Clean but a meaningful amount of
  contract + indexer work.
  Recommend option (c) as the architecturally correct long-term path, but
  pausing for user input before committing.
- **2.4 ✅** Status polling uses real `GET /deposit/status` (camelCase
  `originChainId` + `depositId` query, camelCase response) via
  `AcrossClient::deposit_status`. `AcrossProvider::observe_bridge_operation`
  (`services/action_providers.rs:615`) is the production verifier; worker pass
  routes `ProviderOperationType::AcrossBridge` through it
  (`services/order_executor.rs:684`). Mock (`mock_integrators.rs`) serves the
  same `GET /deposit/status` shape — `ActionProviderRegistry::mock_http` and
  `::http` share the same `AcrossProvider::new` constructor, URL is the only
  difference. No `/across/operation/status` POST remains in production.
- Partial-fills disabled (`fillDeadline` tight, appropriate exclusivityDeadline).

### Phase 3 — Real Unit adapter
- Audit Unit's real API. Replace mock-shape client.
- Custody action executor: `execute_unit_deposit` — send source-vault funds to
  Unit's deposit address (native or ERC-20, chain-dependent). Paymaster gas.
- Real status polling via Unit's API.
- Unit refund semantics (BTC / Solana / EVM) need confirmation — flagged as
  unresolved in user Codex context. Defer resolution until mid-Phase 3 when the
  concrete API shape is in front of me.

### Phase 4 — Real Hyperliquid adapter
- Extract the real SDK/API call pattern from
  `bin/router-server/tests/live_market_order_e2e.rs`. Wire into the production
  `ExchangeProvider` implementation.
- Hyperliquid subaccount ↔ deposit vault is 1:1 (user decision). Subaccount
  provisioning either eager at vault creation or lazy at first trade — pick lazy
  unless there's a reason to eager-provision.

### Phase 5 — Live E2E test
- Migrate `live_market_order_e2e.rs` to drive the full stack:
  boot `initialize_components`, spawn `router-api`, spawn `router-worker`, call
  `POST /api/v1/quotes`, `POST /api/v1/orders`, fund the returned deposit vault
  with dust from a throwaway key, let the worker execute, assert final DB state +
  real on-chain balances.
- Spend guards:
  - `RIFT_LIVE_CONFIRM=1` env var required to run.
  - Hard max input: reject if amount > configured cap (e.g. 5 USDC equivalent).
  - Expected source address check: test aborts if the funded address doesn't
    match the throwaway key it expects.
  - Approval txs gated behind a separate flag (default off).
  - Balance invariants at start/end: deposit vault, paymaster wallet, recipient.

### Phase 6 — EIP-7702 batched funding
- After first green live run.
- `PaymasterActor` gains `FundMany { funds: Vec<FundItem> }` command that
  constructs a 7702 EOA-as-smart-account tx calling a batched `multicall`-style
  helper. Atomic same-chain funding of N vaults in one submission.
- Replaces N separate `FundVaultAction` calls when a plan's next steps touch the
  same chain.

## Open assumptions / flags
- Across real API shapes — will confirm against live docs during Phase 2.
- Unit refund semantics — will surface when I read Unit's docs in Phase 3.
- Hyperliquid subaccount provisioning cadence — pick lazy unless a constraint surfaces.
- Quote reaper TTL — start at `quote_expires_at + 10 min` grace.

## Starting point
Phase 1 first. Everything downstream depends on the deposit vault existing at
quote time so Across can see a real refund address.
