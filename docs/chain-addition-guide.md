# Chain Addition Guide

This is the exhaustive checklist for adding a chain to T Router. A chain is
fully integrated only when users can route swaps **from**, **through**, or **to**
that chain wherever providers support the relevant assets.

Adding a chain is broader than adding an asset row. The router must understand
addresses, key derivation, custody execution, deposit detection, finality,
provider chain identifiers, gas/paymaster behavior, route-cost pricing, mocks,
and operations.

## 1. Classify The Chain

Required decisions:

1. Router chain id:
   - EVM chains use `evm:<decimal_chain_id>`.
   - Non-EVM chains use lowercase names with letters, digits, and hyphens.

2. Backend chain type:
   - existing `ChainType`
   - or new `ChainType` variant

3. Execution family:
   - EVM-compatible
   - Bitcoin-like UTXO
   - Hyperliquid-like provider ledger
   - new chain family

4. Native asset:
   - symbol
   - decimals
   - minimum transferable unit
   - fee token

5. Token model:
   - native only
   - ERC-20-like contracts
   - UTXO assets
   - provider-ledger balances
   - memo/tag-based deposits

6. Finality model:
   - block confirmations
   - optimistic/finality tags
   - mempool support
   - reorg window
   - provider-specific settlement finality

7. Custody model:
   - address derivation
   - signing
   - transaction broadcast
   - refund/sweep support
   - paymaster support

8. Detection model:
   - RPC logs
   - token indexer
   - mempool/ZMQ
   - provider API
   - self-hosted indexer

Document these choices before implementation. They determine every later
touchpoint.

## 2. Primitive Chain Identity

Touchpoints:

- `crates/router-primitives/src/chain.rs`
- `bin/router-server/src/protocol.rs`
- `bin/sauron/src/watch.rs`
- any legacy `otc-models` usage that still compiles into the workspace

Required work:

1. Add a `ChainType` variant if the chain is not represented by an existing
   backend family.

2. Update `ChainType::to_db_string` and `ChainType::from_db_string`.

3. Add the public router chain id to `supported_chain_ids`.

4. Map the public chain id to the backend chain type in `backend_chain_for_id`.

5. Update Sauron watch parsing.
   - `chain_type_from_db`
   - native token handling
   - token-address normalization
   - tests for chain mapping

6. Add tests for:
   - parsing the public `ChainId`
   - backend mapping
   - unsupported chain rejection
   - asset id normalization

Completion criteria:

- The API can accept the chain id.
- Router services can map that chain id to a chain implementation.
- Sauron can bucket watches by the chain type.

## 3. Chain Operations Implementation

Touchpoints:

- `crates/chains/src/traits.rs`
- `crates/chains/src/registry.rs`
- `crates/chains/src/evm.rs`
- `crates/chains/src/bitcoin.rs`
- `crates/chains/src/hyperliquid.rs`
- new `crates/chains/src/<chain>.rs` for a new family

Required work:

1. If the chain is EVM-compatible, instantiate `EvmChain`.
   - RPC URL
   - reference token address
   - `ChainType`
   - key-derivation domain separator
   - minimum confirmations
   - estimated block time
   - optional gas sponsor

2. If the chain is not covered by an existing implementation, implement
   `ChainOperations`.
   - `create_wallet`
   - `derive_wallet`
   - `verify_user_deposit_candidate`
   - `get_tx_status`
   - `dump_to_address`
   - `validate_address`
   - `minimum_block_confirmations`
   - `estimated_block_time`
   - `get_block_height`
   - `get_best_hash`

3. If a typed accessor is useful, extend `ChainRegistry`.
   - `register_*`
   - `get_*`
   - tests

4. Verify deterministic key derivation.
   - unique domain separator per chain
   - stable address derivation from router master key + salt
   - no address collision across chains

5. Verify address validation.
   - user recipient address
   - refund address
   - provider deposit address
   - paymaster/sweep address

6. Verify transfer/refund behavior.
   - native transfer
   - token transfer if applicable
   - insufficient funds
   - fee handling
   - dust/minimum output behavior

Completion criteria:

- `ChainRegistry` can create, derive, validate, inspect, and sweep custody for
  the chain.
- `VaultManager` and `OrderManager` can validate user-facing addresses.

## 4. Router Configuration And Startup

Touchpoints:

- `bin/router-server/src/lib.rs`
- `bin/router-server/src/app.rs`
- deployment env files / compose files
- local test harness argument builders

Required work:

1. Add CLI/env args.
   - RPC URL
   - token indexer URL if used by Sauron
   - reference token address if EVM
   - paymaster private key if router-funded actions are possible
   - chain-specific network selector if needed
   - confirmation/finality knobs if not hard-coded

2. Instantiate the chain in `initialize_chain_registry`.

3. Add paymaster registry support if the chain can receive reimbursements or
   needs native gas top-ups.

4. Add startup validation.
   - URL format
   - key format
   - network mismatch checks
   - required settings present only when the chain is enabled

5. Update test `base_args` and other fixtures.

Completion criteria:

- Router startup either initializes the chain correctly or fails with a clear
  config error.
- Existing deployments can omit optional chain config without breaking
  unrelated chains.

## 5. Asset Registry And Provider Capability Rows

Touchpoints:

- `bin/router-server/src/services/asset_registry.rs`
- provider adapters in `action_providers.rs`
- provider-specific clients

Required work:

1. Add chain-local assets in `builtin_chain_assets`.
   - canonical asset
   - router chain id
   - `AssetId::Native` or `AssetId::reference(...)`
   - decimals

2. If the chain has anchor assets, update `is_anchor_canonical` only if the
   canonical asset should be part of the core graph.

3. Add provider capability rows in `builtin_provider_assets`.
   - Across chain id / token address
   - CCTP domain / token messenger token
   - Unit source/destination chain name
   - Velora network id
   - Hyperliquid bridge chain id
   - any other venue-specific chain identifier

4. Update provider-specific mapping helpers.
   - chain id conversion
   - domain conversion
   - symbol conversion
   - native token sentinel values

5. Add transition-path tests.
   - chain as source
   - chain as destination
   - chain as intermediate
   - arbitrary-asset edge route if using universal routers

Completion criteria:

- The route graph can include the chain wherever provider capabilities allow.
- The chain is not accidentally used for assets/providers that are not
  supported.

## 6. Custody Action Execution

Touchpoints:

- `bin/router-server/src/services/custody_action_executor.rs`
- `crates/chains/src/*`
- `bin/router-server/src/services/order_executor.rs`

Required work:

1. Add support for the chain in custody action dispatch.
   - transfer actions
   - chain calls
   - native vs token behavior
   - receipt/hash semantics

2. If the chain uses EVM calls, verify existing `ChainCall::Evm` is sufficient.

3. If the chain needs a new call family, add:
   - `ChainCall` variant
   - request struct
   - serialization
   - executor branch
   - receipt model

4. Add balance prechecks for source/intermediate custody.

5. Add release/sweep behavior for terminal internal custody.

6. Add refund support.
   - can refund native?
   - can refund token?
   - who pays chain fees?
   - what happens when refund fees exceed stranded balance?

Completion criteria:

- The worker can spend from router-derived vaults on the chain.
- Reverted/failed actions become retry/refund state transitions.
- Terminal internal custody can be released or explicitly left for manual
  operations.

## 7. Deposit Detection And Sauron

Touchpoints:

- `bin/sauron/src/config.rs`
- `bin/sauron/src/runtime.rs`
- `bin/sauron/src/watch.rs`
- `bin/sauron/src/discovery/mod.rs`
- `bin/sauron/src/discovery/evm_erc20.rs`
- `bin/sauron/src/discovery/bitcoin.rs`
- new `bin/sauron/src/discovery/<chain>.rs` if needed
- `bin/sauron/migrations-replica/*.sql` if watch SQL changes

Required work:

1. Add Sauron config.
   - RPC URL
   - indexer URL
   - mempool/ZMQ URL if needed
   - concurrency
   - scan interval
   - confirmations/reorg settings if configurable

2. Add or instantiate a discovery backend.
   - EVM chains can usually use `EvmErc20DiscoveryBackend`.
   - Bitcoin-like chains need block/mempool scanning.
   - Provider-ledger chains need a provider-backed detector.

3. Add backend construction in `build_backends`.

4. Update watch parsing and token normalization.
   - native token
   - token address/reference
   - invalid address handling
   - invalid token handling

5. Implement current cursor and backfill behavior.
   - initial cursor on clean startup
   - stored cursor load
   - reorg rescan window
   - indexed lookup for active watches
   - catch-up after outage

6. Implement scanning.
   - block range limits
   - log/event filters
   - transaction decoding
   - transfer index uniqueness
   - confirmation threshold
   - duplicate detection suppression

7. Submit detector hints through the existing router internal API.

8. Add Sauron tests.
   - watch SQL mapping
   - indexed lookup
   - block scan
   - restart/backfill
   - router hint submission

Completion criteria:

- Deposits into new-chain funding vaults are discovered without router-worker
  polling.
- Sauron can restart and emit missed hints inside the retention window.

## 8. Paymaster, Gas, And Reimbursement

Touchpoints:

- `crates/chains/src/evm.rs`
- `bin/router-server/src/services/gas_reimbursement.rs`
- `bin/router-server/src/services/pricing.rs`
- `crates/market-pricing`
- `bin/router-server/src/services/route_costs.rs`
- `paymaster-runtime-reimbursement-ledger.md`

Required work:

1. Decide whether the chain has paymaster-funded actions.
   - Native-input routes may not need paymaster reimbursement.
   - Token-input EVM routes usually do.

2. Add gas sponsor support if applicable.
   - paymaster private key
   - paymaster balance checks
   - vault top-up target/buffer
   - chain-specific gas estimation

3. Add gas price refresh.
   - EVM: RPC gas price/fee history as appropriate
   - non-EVM: explicit fee estimator or no paymaster support

4. Add native gas token price if gas reimbursement converts to USD.

5. Update gas reimbursement transition handling.
   - eligible debts
   - estimated gas units
   - native-input exceptions
   - settlement-site eligibility
   - retention action support

6. Add tests for:
   - gas debt generated
   - gas debt skipped
   - reimbursement settlement selected
   - runtime paymaster top-up

Completion criteria:

- The router never submits chain actions without enough gas.
- Paymaster debts are estimated and recoverable through the reimbursement
  ledger.

## 9. Route Costs, Pricing, And Minimums

Touchpoints:

- `crates/market-pricing`
- `bin/router-server/src/services/pricing.rs`
- `bin/router-server/src/services/route_costs.rs`
- `bin/router-server/src/services/route_minimums.rs`

Required work:

1. Add chain gas pricing to the market-pricing oracle if the chain is part of
   route ranking.

2. Add native asset price lookup if the chain's gas token is not ETH.

3. Add the chain to `PricingSnapshot::chain_gas_price_wei` or replace that API
   with a generalized map before adding many chains.

4. Add route-cost samples for providers that support the chain.

5. Add structural costs for transitions touching the chain.

6. Add route minimum handling for:
   - provider minimums
   - chain dust
   - bridge minimums
   - token decimals
   - final output asset units

Completion criteria:

- The planner can compare paths involving the chain without constant provider
  quote fanout.
- Route minimums prevent economically invalid quotes.

## 10. API, Validation, Screening, And UX Surface

Touchpoints:

- `bin/router-server/src/server.rs`
- `bin/router-server/src/services/order_manager.rs`
- `bin/router-server/src/services/vault_manager.rs`
- `bin/router-server/src/services/address_screening.rs`
- API docs

Required work:

1. Supported chains endpoint must include the new chain.

2. Quote and vault creation validation must accept the chain only when:
   - the chain is in `backend_chain_for_id`
   - the asset identity is valid for that chain
   - the recipient/refund address validates under `ChainOperations`

3. Address screening must map the chain to the vendor's expected chain/network
   identifier or explicitly disable with a documented policy.

4. Error messages should distinguish:
   - unsupported chain
   - unsupported asset on supported chain
   - invalid address
   - provider unsupported route

Completion criteria:

- User-facing API responses make the chain discoverable and validation failures
  understandable.

## 11. Database And Migrations

Touchpoints:

- `bin/router-server/migrations/*.sql`
- `bin/sauron/migrations-replica/*.sql`
- `bin/router-server/src/db/*`
- `bin/sauron/src/watch.rs`

Required work:

1. Add migrations only if schema constraints or new relational columns are
   needed.

2. Chain ids are stored as text in most current tables, so adding an EVM chain
   usually does not require DB changes.

3. New step types, provider operation types, custody control types, or custody
   roles do require constraint updates.

4. Sauron watch SQL changes require replica migration updates.

5. Verify fresh migration and restart compatibility.

Completion criteria:

- Fresh DB migration passes.
- Existing rows can be read after code changes.
- Sauron replica migrations do not version-collide with primary migrations.

## 12. Devnet And Local Mocks

Touchpoints:

- `crates/devnet/src/evm_devnet.rs`
- `crates/devnet/src/mock_integrators.rs`
- `crates/devnet/src/*_mock`
- `crates/devnet/tests/*`
- provider mock docs

Required work:

1. Add local chain simulation.
   - EVM: anvil chain or configured local RPC
   - non-EVM: lightweight mock/indexer where possible

2. Deploy or configure local tokens.

3. Add mock provider support for the chain.
   - provider chain id/domain
   - token address/symbol
   - settlement side effects
   - operation status progression

4. Add stable mock contract addresses if providers return calldata.

5. Add test harness helpers.
   - fund user/source vault
   - fund paymaster
   - mine/finalize blocks
   - query local balances

Completion criteria:

- Local integration tests can complete a route involving the new chain without
  real funds.

## 13. Provider Integrations

Touchpoints:

- `docs/venue-addition-guide.md`
- provider adapters and clients
- provider capability rows
- live differential tests

Required work:

1. For each provider expected to support the chain, update its mapping.
   - Across chain id/token
   - CCTP domain/contracts
   - Unit chain string
   - Velora network id
   - Hyperliquid bridge source chain
   - other provider-specific identifiers

2. Confirm provider min/max, fee, finality, and status semantics for the chain.

3. Add provider mock support.

4. Add live differential coverage.

Completion criteria:

- The chain is not merely registered; at least one provider can actually route
  to/from it.

## 14. Observability And Operations

Touchpoints:

- `bin/router-server/src/telemetry.rs`
- `bin/sauron/src/benchmarks.rs`
- deployment dashboards / runbooks
- logs and metrics labels

Required work:

1. Add chain labels where hard-coded.

2. Add Sauron backend metrics labels.

3. Add paymaster balance monitoring.

4. Add RPC error/retry metrics.

5. Add route-cost refresh visibility.

6. Add runbook entries.
   - RPC outage
   - stuck deposits
   - finality lag
   - paymaster depleted
   - manual refund

Completion criteria:

- Operators can tell whether the chain is healthy and why a route involving it
  is unavailable.

## 15. Security And Invariants

Required checks:

1. Router-derived keys only; no shared execution key for user orders.

2. Address validation is chain-specific.

3. Provider calldata cannot redirect funds.

4. Sauron hints are advisory; router-worker validates balances/provider state.

5. Reorg/finality assumptions are explicit.

6. Paymaster top-ups cannot be used as user withdrawals.

7. Refunds return to the explicit refund address, not an inferred address.

8. Internal custody is swept only after terminal release.

9. Unsupported tokens fail closed.

Completion criteria:

- The chain cannot introduce a path that bypasses custody, validation, refund,
  or paymaster accounting.

## 16. Test Matrix

Minimum test set:

1. `ChainId` parse/backend mapping tests.
2. `ChainType` db-string round-trip tests.
3. `ChainOperations` address/key derivation tests.
4. Deposit vault creation test.
5. Deposit detection test through Sauron.
6. Native deposit and token deposit tests when applicable.
7. Refund transfer test.
8. Paymaster top-up/reimbursement tests when applicable.
9. Asset registry path tests.
10. Route-cost ranking tests.
11. Route-minimum tests.
12. Quote test from the chain.
13. Quote test to the chain.
14. Quote test through the chain as an intermediate.
15. Planner materialization test.
16. Worker execution test with local mocks.
17. Worker restart/recovery test.
18. Failure-to-refund test.
19. Provider live differential tests for every provider enabled on the chain.
20. Full Sauron/router-worker E2E smoke.

Before merge, run at least:

- `cargo check --workspace --tests`
- `cargo test -p router-server --test vault_creation`
- affected `crates/chains` tests
- affected Sauron tests
- affected devnet/provider tests
- ignored live differential tests with explicit env gates

## Quick Touchpoint Index

Core identity:

- `crates/router-primitives/src/chain.rs`
- `bin/router-server/src/protocol.rs`
- `bin/sauron/src/watch.rs`

Chain implementation:

- `crates/chains/src/traits.rs`
- `crates/chains/src/registry.rs`
- `crates/chains/src/evm.rs`
- `crates/chains/src/bitcoin.rs`
- `crates/chains/src/hyperliquid.rs`

Router startup:

- `bin/router-server/src/lib.rs`
- `bin/router-server/src/app.rs`

Routing:

- `bin/router-server/src/services/asset_registry.rs`
- `bin/router-server/src/services/order_manager.rs`
- `bin/router-server/src/services/market_order_planner.rs`
- `bin/router-server/src/services/order_executor.rs`
- `bin/router-server/src/services/route_costs.rs`
- `bin/router-server/src/services/route_minimums.rs`

Custody/paymaster:

- `bin/router-server/src/services/custody_action_executor.rs`
- `bin/router-server/src/services/gas_reimbursement.rs`
- `bin/router-server/src/services/pricing.rs`
- `crates/market-pricing`

Detection:

- `bin/sauron/src/config.rs`
- `bin/sauron/src/runtime.rs`
- `bin/sauron/src/discovery/*`
- `bin/sauron/src/watch.rs`

Mocks/tests:

- `crates/devnet/src/*`
- `bin/router-server/tests/*`
- `bin/sauron/tests/*`
- provider/client crate tests
