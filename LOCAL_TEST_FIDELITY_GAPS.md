# Local Test Fidelity Gaps

This tracks places where local tests currently diverge from production-shaped
behavior. These are not all test failures; most are places where the test suite
is deliberately injecting state or using a simplified mock. Before alpha, we
should decide which ones remain acceptable unit-test shortcuts and which ones
need production-shape end-to-end coverage.

## High Priority

### Across Destination Fulfillment Is Manually Credited

Status: resolved for production-shaped local happy paths.

The local Across mock now observes/indexes the source-chain deposit and only
reports the provider operation as filled after it has sent or minted the
expected output asset to the destination-chain recipient. In multi-chain mock
mode, missing destination-chain RPC configuration keeps the deposit pending
instead of returning a synthetic fill.

Representative locations:

- `bin/router-server/tests/vault_creation.rs`
- `bin/sauron/tests/provider_operation_e2e.rs`
- `crates/devnet/src/mock_integrators.rs`

Implemented shape:

- The mock Across relayer should observe the source deposit.
- It should send/mint the expected output asset to the destination execution
  vault on the destination chain.
- Happy-path E2E tests should remove manual
  `fund_destination_execution_vault_from_across` style helpers.
- Direct destination-vault funding remains only in explicitly named recovery
  fixtures that fabricate completed provider operation state in the database.

### Unit / HyperUnit Lifecycle Is Test-Driven

Status: resolved for production-shaped local happy paths. The shared
router-server and Sauron happy paths now use the mock Unit EVM/Bitcoin deposit
indexers for UnitDeposit completion, and the mock Hyperliquid `spotSend` hook
for UnitWithdrawal completion. Direct Unit lifecycle helpers remain for focused
fixture and failure-state tests.

The Unit mock exposes helpers that directly complete, fail, or seed operations.
Many tests use these helpers to move operations forward. Production Unit behavior
is external: Unit discovers source transfers, progresses lifecycle state, and
eventually reports completion/failure.

Representative locations:

- `crates/devnet/src/mock_integrators.rs`
- `bin/router-server/tests/vault_creation.rs`
- `bin/sauron/tests/provider_operation_e2e.rs`

Target shape:

- The Unit mock should observe actual source transfers where feasible. This is
  now true for UnitDeposit in the router-server and Sauron happy paths that use
  the shared mock-integrator harness.
- Operation state should progress from observed transfer state. UnitWithdrawal
  now advances from the actual mock Hyperliquid `spotSend` into the generated
  Unit protocol address.
- Direct `complete_unit_operation` calls should be reserved for focused unit
  tests, not production-shape happy-path E2E tests.

### Hyperliquid Spot Balances Are Manually Credited After Unit Deposits

Status: resolved for production-shaped local happy paths. UnitDeposit
completion now credits the mock Hyperliquid spot ledger from the observed source
transfer for ETH/BTC Unit ingress routes. Focused tests still use direct ledger
crediting for synthetic or failure-state setup.

Several tests directly credit mock Hyperliquid spot balances after Unit deposit
completion. This can mask bugs in deposited amount detection, fees, Unit credit
timing, or failed ingress handling.

Representative locations:

- `bin/router-server/tests/vault_creation.rs`
- `bin/sauron/tests/provider_operation_e2e.rs`
- `crates/devnet/src/mock_integrators.rs`

Target shape:

- Unit deposit completion should credit Hyperliquid automatically from the
  observed source transfer amount.
- The credited amount should account for any modeled provider fee.
- Happy-path E2E tests should not directly call Hyperliquid balance-credit
  helpers.

### Provider And Detector Hints Are Often Inserted Directly

Status: resolved for production-shaped local happy paths. Router-worker tests
still use direct hint insertion to isolate worker behavior, but the Sauron
runtime E2Es now assert that route-progress hints for the completed mock routes
come only from Sauron sources.

Many router-worker tests insert provider or detector hints directly. That is
valid for targeted worker validation tests, but those tests do not prove that
Sauron produced the hint from observed chain/provider state.

Representative production-shape coverage:

- `sauron_runtime_drives_router_worker_through_mock_base_eth_btc_progress`
- `sauron_runtime_drives_router_worker_through_mock_base_usdc_btc_progress`

Implemented shape:

- Keep direct hint insertion for focused worker tests.
- The Sauron runtime E2Es assert all provider-operation hints for the tested
  orders come from `sauron` or
  `sauron_provider_operation_observation`, and at least one is processed.
- Direct helper-sourced hints remain scoped to worker-fixture tests.

### Deposit Detection Is Sometimes Bypassed By Direct DB State Transitions

Status: resolved for production-shaped local happy paths. The Sauron runtime
E2Es now fund the source vault on-chain and assert a processed `sauron`
funding hint exists for the funding vault. Direct DB vault transitions remain
only in lower-level repository/service and worker-fixture tests.

Some tests fund a vault on-chain and then directly transition the DB row to
`Funded` or `Executing`. Bitcoin source-vault tests currently include this
pattern. That bypasses the production detector path.

Representative production-shape coverage:

- `sauron_runtime_drives_router_worker_through_mock_base_eth_btc_progress`
- `sauron_runtime_drives_router_worker_through_mock_base_usdc_btc_progress`

Implemented shape:

- Production-shape E2E tests should require Sauron/deposit detection to mark
  vaults funded.
- Direct DB transitions should be reserved for repository/service unit tests.

## Medium Priority

### Paymaster And Gas Funding Can Be Masked By Direct Native Balance Padding

Status: resolved for production-shaped local happy paths. The Sauron runtime
E2Es now start source vaults without native gas padding, fund only the quoted
input amount, and assert the Base paymaster spends native gas to top up the
source vault before the first EVM provider action.

Some tests give execution vaults native gas directly with Anvil balance mutation
or helper sends. That can hide failures in paymaster top-up calculation,
runtime reimbursement accounting, and per-leg gas funding.

Representative production-shape coverage:

- `sauron_runtime_drives_router_worker_through_mock_base_eth_btc_progress`
- `sauron_runtime_drives_router_worker_through_mock_base_usdc_btc_progress`

Implemented shape:

- Full-route tests should assert which vaults were topped up by the paymaster.
- E2E route tests should avoid direct native balance padding except when testing
  non-paymaster chains or explicit setup preconditions.

### Velora Mock Mints Output Instead Of Simulating Swap Semantics

Status: resolved for configured local devnet and shared test-harness routes.
The Velora mock now requires a per-network `MockVeloraSwap` contract address.
The mock quote endpoint returns that contract as the token-transfer proxy and
execution target, and `/transactions` returns calldata that spends the source
asset through native `msg.value` or ERC20 `transferFrom` before creating output
from the mock reserve/mint path. Missing swap-contract configuration now fails
instead of silently falling back to a mint-only transaction.

Representative production-shape coverage:

- `mock_velora_transaction_spends_input_and_mints_output_on_local_evm`
- `mock_velora_prices_sell_eth_to_usdc_uses_prices_and_decimals`
- `mock_velora_prices_buy_usdc_with_eth_uses_prices_and_decimals`
- `mock_velora_prices_sell_usdc_to_eth_uses_prices_and_decimals`
- `mock_velora_prices_unknown_erc20_with_deterministic_mock_price`

Remaining shape:

- Keep this mock focused on local calldata/executor integration.
- Keep live differential tests as the source of truth for Velora quote/execution
  semantics, price impact, production liquidity, and real revert causes.

### CCTP Mock Compresses Attestation And Finality

Status: improved. The local CCTP path still uses mock contracts for burn/mint,
but mock Iris can now return delayed pending attestations and explicit failed
attestation messages instead of always completing immediately. Router CCTP
observation now treats failed Iris statuses as terminal provider-operation
failures instead of leaving them pending forever.

Representative coverage:

- `mock_cctp_messages_honor_attestation_latency`
- `mock_cctp_messages_can_return_failed_attestation`
- `cctp_failed_iris_statuses_are_terminal_failures`

Remaining shape:

- Preserve the current burn/mint mock for deterministic local execution.
- Live differential tests remain the source of truth for Circle signature,
  attestation, and production finality semantics.

### Hyperliquid Exchange Mock Is A Small Synthetic Market

The Hyperliquid mock uses an in-memory balance/book model with limited asset
coverage and simplified IOC-style behavior. It does not model full liquidity,
rate limits, production fees, agent wallet edge cases, or all exchange failure
modes.

Target shape:

- Keep local tests focused on account, signing, balance, and state transition
  plumbing.
- Keep live differential tests for provider semantic coverage.

### Hyperliquid Bridge Mock Compresses Confirmation And Failure Behavior

Status: improved for router-local timing/failure coverage. The Hyperliquid
bridge mock observes local EVM deposits and credits the mock clearinghouse only
after configurable deposit latency, and can deterministically fail valid bridge
credits for retry/refund tests.

Remaining boundary:

- Live differential tests are still needed for production bridge confirmation
  depth and replay-protection semantics.

### Chainalysis / Address Screening Is Not Exercised Locally

Status: resolved for router API quote/order admission paths. The shared mock
integrator server now exposes a Chainalysis-compatible
`/api/risk/v2/entities/:address` endpoint, and the router API test harness can
configure per-address Low/Medium/High/Severe/Unknown risk or HTTP-provider
errors.

Representative production-shape coverage:

- `router_api_address_screening_covers_allow_block_and_provider_error`

Implemented shape:

- Quote creation screens recipient addresses before route planning.
- Order creation screens the quote recipient again and screens the refund
  address before creating the order/funding vault.
- Blocked screening responses map to `403 Forbidden`.
- Chainalysis/provider errors map to `500 Internal Server Error` and fail closed.

## Lower Priority

### Canonical Token Addresses Are Replaced With Local Mock Code

Some local tests install mock ERC-20 code at canonical token addresses with
`anvil_set_code` and unrestricted minting. This is reasonable for devnet setup,
but tests should not treat it as proof of mainnet token-specific behavior beyond
basic ERC-20 compatibility.

Target shape:

- Continue using this for devnet convenience.
- Avoid relying on unrestricted minting or nonstandard token behavior in
  production-shape assertions.

### Worker Passes Are Often Driven Directly

Status: resolved for production-shaped smoke coverage. The Sauron runtime E2Es
spawn a router API, the long-lived router worker loop, Sauron, a real test
Postgres database, and local devnet services together. Those smoke paths create
orders through the API, fund vaults on-chain, require Sauron-submitted hints,
and wait for the worker loop to drive orders to completion.

Representative coverage:

- `sauron_runtime_drives_router_worker_through_mock_base_eth_btc_progress`
- `sauron_runtime_drives_router_worker_through_mock_base_usdc_btc_progress`

Remaining shape:

- Keep direct worker-pass tests for deterministic state-machine coverage.
- Keep the runtime smoke paths focused and expensive, not the default shape for
  every edge-case assertion.
