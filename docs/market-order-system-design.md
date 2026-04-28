# Market-Order System Design

This document describes the current router market-order architecture as it
exists in code today. It is intentionally practical: the goal is to make it
obvious what happens during quote, order creation, execution, retry, and
refund, and exactly what must change when we add a new asset or venue.

## Scope

This document covers:

- quote generation
- order creation
- transition-path planning
- execution and observation
- retry and refund routing
- extension points for new assets, venues, chains, and transition kinds

It does not cover:

- TEE deployment topology
- public API auth / rate limiting
- KYT / screening policy

## High-Level Flow

The system is split into five main layers:

1. `AssetRegistry`
   - static capability graph
   - canonical assets
   - provider capabilities
   - transition declarations and path search

2. `ActionProviderRegistry`
   - concrete provider adapters
   - quote / execute / observe implementations for Across, CCTP, HyperUnit,
     Hyperliquid Bridge, Hyperliquid spot, and Velora

3. `OrderManager`
   - validates quote requests
   - finds candidate transition paths
   - composes live quotes across those paths
   - persists the chosen quote
   - creates orders from quotes

4. `MarketOrderRoutePlanner`
   - takes the persisted quoted transition path
   - materializes `order_execution_steps`
   - enforces the intermediate-custody invariant before execution

5. `OrderExecutionManager`
   - worker-side state machine
   - executes steps
   - observes provider operations
   - retries failed execution attempts once
   - escalates to refund routing
   - plans and executes automatic refunds

## Current Venue Alignment

The exhaustive venue checklist lives in `docs/venue-addition-guide.md`. Every
venue should line up with that guide: provider id, venue taxonomy, asset support
model, transition kind, provider adapter, registry wiring, provider-policy id,
planner/executor behavior, Sauron behavior if async, local mock surface, mock
integration coverage, live differential coverage, observability, and operator
disable/drain behavior.

| Runtime id | Venue model | Asset support | Transition kind | Adapter trait | Current alignment |
| --- | --- | --- | --- | --- | --- |
| `across` | cross-chain bridge | runtime-enumerated allowlist | `AcrossBridge` | `BridgeProvider` | aligned: capability rows, HTTP registry wiring, policy id, mock `/swap/approval` + `/deposit/status`, mock execution tests, live Across transcript tests |
| `cctp` | cross-chain USDC burn/mint bridge | static declared USDC domains | `CctpBridge` | `BridgeProvider` | aligned: capability rows, HTTP registry wiring, policy id, mock TokenMessengerV2 + Iris `/v2/messages` + MessageTransmitterV2, mock execution tests, live CCTP differential tests |
| `unit` | cross-chain deposit/withdraw venue | static declared assets | `UnitDeposit`, `UnitWithdrawal` | `UnitProvider` | aligned: capability rows, HTTP registry wiring, policy id, mock `/gen` + `/operations`, async observation tests, live HyperUnit differential tests |
| `hyperliquid_bridge` | cross-chain bridge into Hyperliquid | static declared USDC ingress | `HyperliquidBridgeDeposit` | `BridgeProvider` | aligned: capability row, HTTP registry wiring, policy id, mock Bridge2 indexer plus Hyperliquid balance observation, planner/executor tests, live bridge differential tests |
| `hyperliquid` | mono-chain fixed-pair exchange | static declared spot assets | `HyperliquidTrade` | `ExchangeProvider` | aligned: capability rows, HTTP registry wiring, policy id, mock `/info` + `/exchange`, execution tests, live spot/order/cancel differential tests |
| `velora` | mono-chain universal router | open-address quote check | `UniversalRouterSwap` | `ExchangeProvider` | aligned: runtime transition generation, HTTP registry wiring, policy id, mock `/prices` + `/transactions/:network`, arbitrary-token quote and local-settlement tests, live Velora quote/swap differential tests |

Review rule: if a venue appears in `ProviderId`, it must have a row here, a row
in `docs/venue-addition-guide.md`, and a section in
`docs/provider-mock-parity.md`. If it moves funds through a real provider API,
it must also have an ignored live differential test that records the real
provider transcript before we trust the mock shape.

## Quote System

The quote path lives in `OrderManager`.

Current shape:

1. validate and normalize request
2. derive the source deposit address for the quote
3. optionally compute route minimums
4. enumerate transition paths from source asset to destination asset
5. drop non-executable paths
6. probe the ranked paths to discover expected input/output
7. derive `min_amount_out` or `max_amount_in` from user `slippage_bps`
8. re-quote the chosen provider path with the derived hard bound
9. persist the selected quote

Important detail:

- The persisted quote contains `path_id`, `transition_decl_ids`, and a
  transition-leg quote chain.
- The request stores the user-facing `slippage_bps`; the persisted quote and
  order store both that value and the derived hard amount bound.
- The worker does not rediscover the route template later.
- The quote is the source of truth for the forward execution path.

### Paymaster Gas Reimbursement

Rift still charges no platform fee. Venue fees remain pass-through. Paymaster
gas reimbursement is separate: it repays native gas that Rift advances so
router-controlled EVM vaults can submit required transactions.

The quote model is optimized cross-chain from the start:

1. expand the chosen transition path into EVM gas debts
2. ignore native-input EVM steps for paymaster reimbursement because the vault
   already holds the chain gas token
3. estimate each non-native EVM step's paymaster debt in native gas
4. normalize the debts into USD micro-units at quote time using the worker's
   latest pricing snapshot
5. enumerate eligible settlement sites where the order holds priced assets
   under Rift custody, including EVM tokens and Hyperliquid spot balances
6. choose the cheapest settlement site after collection gas and inventory
   mismatch penalty
7. retain the settlement asset before the provider action at that site
8. expose the retained amount in `provider_quote.gas_reimbursement`

For `Base USDC -> BTC`, the preferred route uses CCTP for the USDC chain hop
when both source and destination USDC domains are registered. It has three EVM
paymaster debts:

- Base USDC vault calls CCTP `depositForBurn`
- Arbitrum USDC vault calls CCTP `receiveMessage`
- Arbitrum USDC vault calls the Hyperliquid bridge

Route-cost refresh owns the pricing refresh loop. Each refresh asks configured
EVM RPCs for `eth_gasPrice` on Ethereum, Base, and Arbitrum, and asks Coinbase's
unauthenticated spot price API for ETH-USD, BTC-USD, and USDC-USD. If a refresh
fails, the worker keeps using the previous pricing snapshot instead of replacing
it with partial data.

Because the route reaches Hyperliquid USDC before the spot trade, the optimizer
should normally settle these debts by retaining Hyperliquid USDC before the
trade. The quote therefore leaves the CCTP and Hyperliquid bridge inputs
alone, reduces the Hyperliquid trade input by the retained USDC amount, and
carries a retention action on the Hyperliquid trade step. Base and Arbitrum
still receive paymaster gas on their own chains, but reimbursement is accounted
for by the Hyperliquid settlement.

Native-asset routes such as `Base ETH -> BTC` do not create paymaster
reimbursement lines. They may still use native balance to execute EVM calls,
but there is no paymaster debt to collect from the user.

Execution treats retention as a custody side effect of the selected step, not a
sweep. The route planner copies quoted retention actions into the relevant
step request. The worker executes those transfers to the configured paymaster
wallet adjacent to the provider custody action. On Hyperliquid, retention is a
spot send; when the trade first prefunds spot from withdrawable, the prefund
amount is increased so the retained amount is available before the order is
submitted. If a provider action is not custody-backed, a retention action on
that step is invalid.

The quote-time multiplier is a reserve, not a platform fee. The ledger schema
tracks estimated debt, settlement, and later actual receipt data so production
can reconcile over/under collection explicitly.

## Order System

The order-creation path also lives in `OrderManager`.

Current shape:

1. fetch the quote
2. reject expired or already-used quotes
3. require an explicit `refund_address`
4. create the order row in `quoted`
5. attach the quote to the order
6. later, once the funding vault is created and funded, execution planning can
   begin

Important invariants:

- `refund_address` is explicit and mandatory
- the quote can only be used once
- the order preserves the original source asset/chain and destination
  asset/chain for later retry/refund decisions

## Planning System

The route planner lives in `MarketOrderRoutePlanner`.

Current shape:

1. load the funded order, funding vault, and persisted quote
2. resolve `transition_decl_ids` back into concrete transition declarations
3. materialize execution steps from the quoted legs
4. derive intermediate custody roles mechanically from transition kinds
5. validate that every non-final step lands in Rift-controlled custody

Important invariant:

- reachability is now aligned with executability for the current transition
  kinds; there is no separate route-template whitelist after graph search

## Execution System

The worker-side state machine lives in `OrderExecutionManager`.

Current worker pass shape:

1. reconcile failed attempts that still need retry/refund decisions
2. process provider hints
3. recover terminal provider operations that were persisted before step
   settlement
4. finalize completed orders that survived a crash before finalization
5. finalize direct refunds
6. plan refund attempts waiting in `refund_required`
7. align vault state for manual refund cases
8. materialize forward execution plans for funded orders
9. execute ready orders
10. finalize terminal internal custody vault lifecycle
11. sweep released internal custody to paymasters when economic

Execution is generic at the step level:

- steps carry typed relational columns plus provider-specific JSON
- providers return execution intents
- the custody executor performs chain actions
- providers can derive observed state after submission
- Sauron submits hints for possible progress, and the worker validates those
  hints against provider or chain truth before moving the operation terminal

## Retry and Refund System

Retries and refunds are modeled as execution attempts on the same order.

Attempt kinds:

- `primary_execution`
- `retry_execution`
- `refund_recovery`

Current policy:

- forward execution gets one automatic retry
- if that still fails, the order becomes `refund_required`
- refund execution is non-recursive
- if a refund attempt fails once, the order becomes
  `refund_manual_intervention_required`

Automatic refund routing is now transition-path based:

1. discover the current stranded position
2. search transition paths from that position back to the original source
   asset/chain
3. compose quotes across those refund paths
4. materialize refund steps from the chosen path

Current conservative boundary:

- a single stranded position can be auto-refunded
- multi-position stranded balances still stop at manual intervention

## Current Extension Surface

The system is much cleaner than the original route-template model, but it is
not yet true one-touch extension. The actual touchpoints depend on what is
being added.

For full checklists, use:

- `docs/venue-addition-guide.md` for venues and transition kinds
- `docs/chain-addition-guide.md` for chains

### Case 1: Add a new chain-local asset that existing providers already support

Required changes:

1. `AssetRegistry::builtin_chain_assets`
   - add the canonical asset mapping for each chain-local representation

2. `AssetRegistry::builtin_provider_assets`
   - add provider capability rows for that asset on each supported provider

3. provider client mapping if needed
   - for example symbol/token resolution inside a provider adapter

4. tests
   - transition path coverage
   - quote coverage if the new asset participates in real routing

If the asset only extends existing transition kinds, no new planner or worker
match arms should be required.

### Case 2: Add a new venue that fits an existing transition kind

Examples:

- a new bridge provider
- a new exchange provider
- another Unit-like deposit/withdraw venue

Required changes:

1. add a provider identifier to `ProviderId`
2. classify the provider's `ProviderVenueKind` and `AssetSupportModel`
3. add provider capability rows in `builtin_provider_assets`
4. implement the correct provider trait in `action_providers.rs`
   - `BridgeProvider`
   - `ExchangeProvider`
   - `UnitProvider`
5. wire the provider into `ActionProviderRegistry` and router startup config
6. make sure provider-policy names match the runtime `id()`
7. update quote composition, route-cost ranking, route minimums, gas
   reimbursement, planner materialization, executor dispatch, observation, and
   refund support wherever the existing transition kind requires it
8. add a local mock service for the provider API surface the router consumes
   - the mock must live in devnet or the existing mock-integrator harness
   - it should return provider-shaped quote and transaction/operation responses
   - when practical, it should materialize local settlement state enough for
     worker tests to advance, while clearly documenting what it does not
     simulate
9. add mock coverage, recovery coverage, and live differential coverage

If the venue reuses an existing transition kind, the planner and refund
materializer can usually remain unchanged.

### Case 3: Add a brand-new transition kind

This is the expensive case.

Required changes:

1. `MarketOrderTransitionKind`
2. `AssetRegistry::market_order_transitions`
3. `required_roles_for_transition_kind`
4. path quoting in `OrderManager`
5. route-minimum computation in `RouteMinimumService`
6. route-cost ranking and refresh handling in `RouteCostService`
7. paymaster debt and retention handling in `gas_reimbursement.rs`
8. DB enum constraints and model db-string parsing if new step or operation
   values are needed
9. forward step materialization in `MarketOrderRoutePlanner`
10. runtime custody validation in planner/executor
11. execution dispatch in `OrderExecutionManager`
12. provider-operation observation and recovery
13. refund quote composition in `OrderExecutionManager`
14. refund step materialization in `OrderExecutionManager`
15. Sauron watch or provider-operation handling if the transition is async
16. local mock service, unit tests, integration tests, and live differential
    tests

This is the main place where the current design is still multi-touch.

### Case 4: Add a new chain

Use `docs/chain-addition-guide.md`. The short version is that a chain addition
must update primitive chain identity, router startup config, `ChainRegistry`,
custody actions, address validation, Sauron detection, asset registry rows,
provider capability rows, route-cost/gas pricing, paymaster behavior, mocks,
live differentials, and operator documentation. A chain is not integrated just
because its `ChainId` parses.

## What Is Clean Today

- transition-path search is shared by quote planning, route minimums, and
  refund routing
- the persisted quote is the source of truth for forward plan materialization
- retry history is explicit
- refund routing uses the same step engine as forward execution
- intermediate custody is enforced as a hard invariant
- routed Hyperliquid custody is per-order, not shared

## What Is Still Multi-Touch

- provider adapters are still split by trait family rather than a single
  transition-declaration interface
- quote composition logic for forward paths and refund paths is still partly
  duplicated
- introducing a brand-new transition kind still requires touching planner,
  executor, refund, and route-minimum logic in multiple places
- live differential harnesses are still provider-specific rather than generated
  from transition declarations

## Testing Checklist for a New Venue

Minimum expected coverage:

1. asset-registry transition test
2. quote-path test
3. planner materialization test
4. runtime custody validation test
5. local mock service for the provider endpoints the router calls
   - quote endpoint shape
   - transaction-build or operation-submission endpoint shape
   - observation/status endpoint shape for async venues
   - local settlement side effects when worker progression depends on them
6. mock execution integration test against that local mock
7. refund-path test
8. live differential test against the real provider API
   - read-only transcript when possible
   - real-funds lifecycle transcript when the venue submits transactions

If the venue moves real funds asynchronously, also add:

9. provider observation transcript test
10. crash/restart recovery test around its terminal provider operations

## Practical Guidance

If we want truly one-touch venue extension later, the next architectural step
would be to push more logic behind a provider-declared transition interface so
that:

- the graph comes from providers directly
- quoting is transition-driven instead of match-driven
- forward materialization and refund materialization share the same transition
  executor description

The current system is good enough to ship and extend safely, but it is still a
structured multi-touch design rather than the final ideal spec.
