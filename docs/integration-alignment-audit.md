# Current Integration Alignment Audit

This audit applies `docs/venue-addition-guide.md` and
`docs/chain-addition-guide.md` to the providers, venues, and chains currently
present in code.

Audit date: 2026-04-27.

## Summary

Current venue integrations are broadly aligned with the venue guide. The system
has explicit provider ids, venue taxonomy, asset support models, route graph
capabilities, provider adapters, route-cost handling, route-minimum handling,
planner/executor branches, mocks, and live differential coverage for the
current provider set.

Current chain integrations are aligned for Bitcoin, Ethereum, Base, and
Arbitrum as user-facing chains. Hyperliquid is aligned as an internal venue
ledger/custody chain, but it is not aligned as a user-facing source or
destination chain.

One concrete code mismatch was fixed during this audit: router telemetry now
labels Arbitrum and Hyperliquid instead of reporting them as `unsupported`.
Follow-up fixes now also keep Hyperliquid out of the public start/end chain
list and give Arbitrum its own local devnet instance instead of aliasing Base.

## Provider And Venue Matrix

| Provider | Venue guide status | Notes |
| --- | --- | --- |
| `across` | Aligned | Provider id, cross-chain taxonomy, runtime-enumerated support model, `AcrossBridge` transition, `BridgeProvider` adapter, startup config, provider policy id, DB operation/step types, quote composition, route costs, route minimums, planner/executor branches, provider-operation observation, refund materialization, mock API, mock execution coverage, and live Across tests are present. |
| `cctp` | Aligned | Provider id, cross-chain taxonomy, static USDC domain capabilities, `CctpBridge` transition, `BridgeProvider` adapter, startup config/contract overrides, provider policy id, DB operation and two-step burn/receive materialization, quote composition, route costs, route minimums, gas reimbursement, executor observation through Iris, mock contracts/mock Iris, mock tests, and live CCTP differential tests are present. |
| `unit` | Aligned | Provider id, cross-chain taxonomy, static asset support, `UnitDeposit`/`UnitWithdrawal`, `UnitProvider`, provider-address persistence, Sauron provider deposit watches, operation observation, planner/executor/refund support, mock `/gen` + `/operations`, recovery tests, and live HyperUnit differential tests are present. The mock's known semantic gap around automatic Hyperliquid crediting is documented in `provider-mock-parity.md`. |
| `hyperliquid_bridge` | Aligned, narrow by design | Provider id, cross-chain taxonomy, static Arbitrum USDC ingress capability, `HyperliquidBridgeDeposit`, `BridgeProvider`, route costs, route minimums, gas reimbursement, planner/executor support, Hyperliquid balance observation, mock Bridge2/indexer, mock tests, and live bridge tooling are present. The capability is intentionally narrow: Arbitrum USDC ingress. |
| `hyperliquid` | Aligned as a monochain venue | Provider id, monochain fixed-pair taxonomy, static spot assets, `HyperliquidTrade`, `ExchangeProvider`, startup network config, route costs, route minimums, planner/executor/refund support, timeout/cancel recovery, Hyperliquid custody calls, mock `/info` + `/exchange`, local integration tests, and live Hyperliquid differential tests are present. |
| `velora` | Aligned with explicit test opt-in | Provider id, monochain universal-router taxonomy, open-address quote support, runtime edge generation, `UniversalRouterSwap`, `ExchangeProvider`, startup config, route costs, route minimums, gas reimbursement, planner/executor/refund support, calldata custody action generation, mock `/prices` + `/transactions/:network`, local settlement tests, and live Velora tests are present. `ActionProviderRegistry::mock_http` does not include Velora by default; tests that exercise arbitrary-asset Velora routes opt it in explicitly. |

## Provider Checklist Coverage

These touchpoints were checked against the current provider set:

- `ProviderId`, `ProviderVenueKind`, `MonoChainVenueKind`, and
  `AssetSupportModel`
- `ProviderAssetCapability` rows in `builtin_provider_assets`
- `MarketOrderTransitionKind`, `RouteEdgeKind`, and custody role mapping
- `OrderExecutionStepType` and `ProviderOperationType`
- router DB baseline constraints for step and operation types
- `ActionProviderRegistry` wiring and provider startup config
- quote composition in `OrderManager`
- route-cost ranking and refresh in `RouteCostService`
- route minimums in `RouteMinimumService`
- paymaster gas reimbursement in `gas_reimbursement.rs`
- forward materialization in `MarketOrderRoutePlanner`
- execution, observation, recovery, and refund logic in `OrderExecutionManager`
- custody action support in `CustodyActionExecutor`
- Sauron provider-operation and provider-address watch behavior
- devnet/mock provider surfaces
- live differential test coverage
- provider mock parity documentation

Result: no provider is missing a major venue-guide integration category.

## Chain Matrix

| Chain | Chain guide status | Notes |
| --- | --- | --- |
| `bitcoin` / `ChainType::Bitcoin` | Aligned | Public chain id, backend mapping, `BitcoinChain`, wallet derivation, address validation, native-only asset validation, refund/sweep support, Sauron Bitcoin discovery, Bitcoin devnet, Unit/Hyperliquid routing support, tests, and live tooling are present. Bitcoin has no paymaster gas top-up path, which is correct for native-only Bitcoin deposits. |
| `evm:1` / `ChainType::Ethereum` | Aligned | Public chain id, backend mapping, `EvmChain`, startup RPC/paymaster config, asset rows, provider capability rows, Sauron EVM discovery, token indexer config, gas pricing, reimbursement, custody calls, mocks/tests, and live provider routes are present. |
| `evm:8453` / `ChainType::Base` | Aligned | Public chain id, backend mapping, `EvmChain`, startup RPC/paymaster config, asset rows, provider capability rows, Sauron EVM discovery, token indexer config, gas pricing, reimbursement, custody calls, Base devnet, mocks/tests, and live provider routes are present. |
| `evm:42161` / `ChainType::Arbitrum` | Aligned | Public chain id, backend mapping, `EvmChain`, startup RPC/paymaster config, asset rows, provider capability rows, Sauron EVM discovery, token indexer config, gas pricing, reimbursement, custody calls, dedicated Arbitrum devnet, mocks/tests, and CCTP/Hyperliquid-bridge routes are present. |
| `hyperliquid` / `ChainType::Hyperliquid` | Internal-only alignment | Chain identity, backend mapping, `HyperliquidChain`, router-derived address validation/derivation, internal custody use, Hyperliquid calls, asset-registry venue asset, and paymaster sweep registration are present. User-facing quote and vault validation intentionally reject Hyperliquid as a source/destination chain, and Sauron has no Hyperliquid discovery backend. |

## Chain Checklist Coverage

These touchpoints were checked against the current chain set:

- `ChainType::to_db_string` / `from_db_string`
- public `ChainId` parsing and `backend_chain_for_id`
- `supported_chain_ids`
- `ChainRegistry` registration
- `ChainOperations` implementations
- router startup config in `RouterServerArgs` and `initialize_chain_registry`
- paymaster registry setup
- chain-local assets and provider capability rows
- custody action dispatch and refund/sweep behavior
- Sauron watch parsing, backend construction, and discovery backends
- gas pricing and reimbursement snapshots
- route-cost and route-minimum handling
- API validation in `OrderManager` and `VaultManager`
- devnet and mocks
- telemetry labels

Result: Bitcoin, Ethereum, Base, and Arbitrum are aligned as user-facing
chains. Hyperliquid should be treated as a distinct internal chain category
unless we deliberately add user-facing Hyperliquid deposits/withdrawals.

## Gaps And Action Items

### 1. Hyperliquid Chain Visibility

Current state:

- `supported_chain_ids()` does not include `hyperliquid`.
- `backend_chain_for_id("hyperliquid")` maps to `ChainType::Hyperliquid`.
- `OrderManager` rejects Hyperliquid quote source/destination assets.
- `VaultManager` rejects Hyperliquid funding vaults.
- Sauron does not build a Hyperliquid discovery backend.

This now matches the intended alpha posture: Hyperliquid is an internal backend
chain used for venue custody and execution, not a public user start/end chain.

### 2. Arbitrum Local Devnet Fidelity

Current state:

- Arbitrum is a real configured chain in router and Sauron.
- Tests exercise Arbitrum route semantics.
- `RiftDevnet` now runs a dedicated Arbitrum Anvil instance with chain id
  `42161`, its own cache directory, RPC endpoint, paymaster key, token indexer,
  mock contracts, and runtime test wiring.

The Sauron mock Base USDC to BTC runtime route now exercises Arbitrum USDC on
the Arbitrum devnet before moving into the Hyperliquid bridge mock.

### 3. Make Velora Mock Inclusion Policy Explicit

Current state:

- The Velora mock endpoints exist.
- Velora live and local tests exist.
- `ActionProviderRegistry::mock_http` excludes Velora by default, while
  Velora-specific tests opt it in.

This is reasonable because Velora can generate arbitrary same-chain routes and
would materially change many default mock route choices. The policy should stay
explicit in tests: use the default mock registry for fixed core routes, and opt
Velora in for arbitrary-asset tests.

No code change required unless we want a second constructor such as
`mock_http_with_velora`.

## Fix Applied During Audit

Updated `bin/router-server/src/telemetry.rs` so `chain_label` recognizes:

- `evm:42161`
- `hyperliquid`

Before this change, Arbitrum vault telemetry would have been labeled
`unsupported`.

Follow-up fixes:

- Removed `hyperliquid` from public `supported_chain_ids()` while preserving
  internal `backend_chain_for_id("hyperliquid")`.
- Added a dedicated Arbitrum EVM devnet to `RiftDevnet`.
- Updated router and Sauron local runtime test wiring to use the Arbitrum
  devnet for Arbitrum RPC, paymaster, token indexing, mock USDC state, and
  Hyperliquid bridge mock execution.

## Alpha Readiness Read

Venue and chain integration alignment is strong enough for alpha from this
audit's perspective. Hyperliquid is no longer publicly exposed as a start/end
chain, and Arbitrum no longer depends on Base-devnet aliasing for local runtime
coverage.
