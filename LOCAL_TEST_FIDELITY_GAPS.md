# Local Test Fidelity Gaps

This tracks places where local tests currently diverge from production-shaped
behavior. These are not all test failures; most are places where the test suite
is deliberately injecting state or using a simplified mock. Before alpha, we
should decide which ones remain acceptable unit-test shortcuts and which ones
need production-shape end-to-end coverage.

## High Priority

### Across Destination Fulfillment Is Manually Credited

The local Across mock observes/indexes the source-chain deposit and reports the
provider operation as filled, but it does not actually fulfill the destination
chain. Happy-path tests manually fund the destination execution vault after the
provider completion hint.

Representative locations:

- `bin/router-server/tests/vault_creation.rs`
- `bin/sauron/tests/provider_operation_e2e.rs`
- `crates/devnet/src/mock_integrators.rs`

Target shape:

- The mock Across relayer should observe the source deposit.
- It should send/mint the expected output asset to the destination execution
  vault on the destination chain.
- Happy-path E2E tests should remove manual
  `fund_destination_execution_vault_from_across` style helpers.

### Unit / HyperUnit Lifecycle Is Test-Driven

The Unit mock exposes helpers that directly complete, fail, or seed operations.
Many tests use these helpers to move operations forward. Production Unit behavior
is external: Unit discovers source transfers, progresses lifecycle state, and
eventually reports completion/failure.

Representative locations:

- `crates/devnet/src/mock_integrators.rs`
- `bin/router-server/tests/vault_creation.rs`
- `bin/sauron/tests/provider_operation_e2e.rs`

Target shape:

- The Unit mock should observe actual source transfers where feasible.
- Operation state should progress from observed transfer state.
- Direct `complete_unit_operation` calls should be reserved for focused unit
  tests, not production-shape happy-path E2E tests.

### Hyperliquid Spot Balances Are Manually Credited After Unit Deposits

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

Many router-worker tests insert provider or detector hints directly. That is
valid for targeted worker validation tests, but those tests do not prove that
Sauron produced the hint from observed chain/provider state.

Target shape:

- Keep direct hint insertion for focused worker tests.
- Add or preserve separate production-shape E2E tests where Sauron is the only
  component that emits hints.
- Name helpers/tests clearly so state-injection tests are not confused for full
  system tests.

### Deposit Detection Is Sometimes Bypassed By Direct DB State Transitions

Some tests fund a vault on-chain and then directly transition the DB row to
`Funded` or `Executing`. Bitcoin source-vault tests currently include this
pattern. That bypasses the production detector path.

Target shape:

- Production-shape E2E tests should require Sauron/deposit detection to mark
  vaults funded.
- Direct DB transitions should be reserved for repository/service unit tests.

## Medium Priority

### Paymaster And Gas Funding Can Be Masked By Direct Native Balance Padding

Some tests give execution vaults native gas directly with Anvil balance mutation
or helper sends. That can hide failures in paymaster top-up calculation,
runtime reimbursement accounting, and per-leg gas funding.

Target shape:

- Full-route tests should assert which vaults were topped up by the paymaster.
- E2E route tests should avoid direct native balance padding except when testing
  non-paymaster chains or explicit setup preconditions.

### Velora Mock Mints Output Instead Of Simulating Swap Semantics

The Velora mock validates calldata plumbing by returning executable calldata
that mints output tokens. It does not model liquidity, price impact, allowance
use, `transferFrom`, input debit, realistic revert causes, or partial execution
constraints.

Target shape:

- Keep this mock for calldata/executor integration.
- Add at least one stricter local route test that proves input token spend and
  output token receipt accounting.
- Keep live differential tests as the source of truth for Velora quote/execution
  semantics.

### CCTP Mock Compresses Attestation And Finality

The local CCTP path is closer to production than most mocks because it burns and
mints through local contracts. Remaining differences are that Circle attestation,
Iris pending/failure behavior, and finality delays are compressed or synthetic.

Target shape:

- Preserve the current burn/mint mock.
- Add pending, failure, and delayed-attestation modes if CCTP route reliability
  becomes a release gate.

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

The Hyperliquid bridge mock observes local EVM deposits and credits the mock
clearinghouse, but confirmations, latency, replay protection, and failure modes
are simplified.

Target shape:

- Add delayed and failed bridge-credit modes if bridge timing becomes material
  to retry/refund behavior.

### Chainalysis / Address Screening Is Not Exercised Locally

Local configs generally disable Chainalysis/address screening. That means local
integration tests do not prove that screening is enforced before quote/order
creation.

Target shape:

- Add a mock address-screening service.
- Cover allow, block, and provider-error behavior in router API tests.

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

Many tests call worker passes directly rather than running long-lived router API,
router worker, and Sauron processes. That is fine for deterministic unit and
integration tests, but it is not daemon-level E2E coverage.

Target shape:

- Keep direct worker-pass tests for small state-machine coverage.
- Ensure at least one production-shape smoke path runs the actual long-lived
  processes together.
