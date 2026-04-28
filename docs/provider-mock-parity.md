# Provider Mock Parity Notes

This note scopes the devnet provider mocks to the external API surface the
router currently consumes. It is not a claim that the mocks are complete
provider emulators.

When adding a venue, use `docs/venue-addition-guide.md` first. The mock parity
section for that venue must be added before the mock is considered trustworthy:
it should name every provider endpoint/contract surface the router consumes,
the local settlement side effects the mock materializes, and the production
semantics it intentionally does not emulate.

## Across

Covered by the mock:

- `GET /swap/approval` returns executable approval/swap transactions.
- `GET /deposit/status` returns `pending` before an indexed deposit and
  `filled` after the mock SpokePool emits `FundsDeposited`.
- Native and ERC-20 inputs are distinguished enough for router execution tests.

Important semantic gaps:

- Production Across requires bearer auth and an `integratorId`; the mock can
  now enforce both when configured.
- Real `/swap/approval` supports `exactInput`, `minOutput`, and `exactOutput`.
  The mock now accepts all three for the router's direct deposit path.
- The real response includes rich `checks`, `steps`, `fees`, token metadata,
  simulation data, and dynamic quote fields. The mock returns only the subset
  consumed by router execution.
- Real `/deposit/status` can be queried by `depositTxnRef` or by
  `originChainId + depositId`. The client and mock now support both forms.
- Current Across docs use `depositTxnRef` / `fillTxnRef` /
  `depositRefundTxnRef`; the client and mock now use those names.
- Real status indexing has observable latency and more failure/refund/action
  states. The mock can now inject a deterministic latency and deterministic
  refund probability, but still does not emulate all action-level states.

## CCTP

Covered by the mock:

- The devnet deploys mock TokenMessengerV2 and MessageTransmitterV2 contracts at
  stable addresses so router provider policy can allowlist exact call targets.
- `depositForBurn` pulls the source USDC from the custody vault and emits a
  Circle-shaped burn event with the destination domain and mint recipient.
- The mock integrator indexes burn events and serves Iris-shaped
  `GET /v2/messages/:source_domain?transactionHash=...` responses.
- `receiveMessage` decodes the mock message and mints the configured
  destination USDC token to the burn's mint recipient. This gives local worker
  tests a real ERC-20 balance change without emulating Circle attestation
  cryptography.

Important semantic gaps:

- The mock does not verify Circle attestation signatures or finality proofs.
- Real CCTP V2 standard transfers have source-chain finality latency; the mock
  completes as soon as its burn indexer sees the event.
- The mock currently covers the USDC domains and chain IDs registered in the
  asset registry. Additional domains need explicit capability rows, mock token
  setup, and live differential coverage.
- Real Iris can expose pending, delayed, or failure states. The mock returns
  `complete` once indexed and otherwise returns `404`.

## HyperUnit

Covered by the mock:

- `GET /gen/:src_chain/:dst_chain/:asset/:dst_addr` returns a protocol address
  and guardian-signature-shaped JSON.
- `GET /operations/:address` returns address and operation arrays in the client
  shape.
- Tests can force operations to `done` or `failure`.
- Hyperliquid `spotSend` to a tracked withdrawal address advances the operation
  to `signTx`, mirroring the point where Unit observes the HL source transfer.

Important semantic gaps:

- Real protocol addresses are tied to the destination address and continue to
  accept future transfers above minimums. The mock derives deterministic
  addresses, which is convenient, but not proof of guardian-signature validity.
- Real Unit creates and advances operations from observed source-chain
  transfers and required confirmations. The mock seeds an operation at `/gen`
  time and needs test code to move it to terminal state.
- Real operation lifecycles expose intermediate states such as
  `waitForSrcTxFinalization`, `buildingDstTx`, `additionalChecks`, `signTx`,
  `broadcastTx`, `waitForDstTxFinalization`, `readyForWithdrawQueue`, and
  `queuedForWithdraw`. The mock mostly exercises `sourceTxDiscovered`,
  `signTx`, `done`, and `failure`.
- Real operations include realistic confirmations, fees, tx-hash formats,
  queue positions, broadcast timestamps, and sanctions/compliance failures.
  The mock uses synthetic values.
- The mock comment says completed deposits credit the Hyperliquid ledger, but
  the implementation does not currently do that automatically. Runtime tests
  seed the mock HL balance explicitly.
- The client only models the chains/assets the router uses today. Unit docs
  include additional chains and assets.

## Hyperliquid

Covered by the mock:

- `/info` supports `spotMeta`, `l2Book`, `orderStatus`,
  `spotClearinghouseState`, `openOrders`, and `userFills`.
- `/exchange` supports signed `order`, `cancel`, `spotSend`, and `withdraw3`
  envelopes.
- The mock validates L1 signatures under mainnet or testnet source-byte mode.
- IOC spot orders mutate balances and produce order/fill-shaped responses.

Important semantic gaps:

- Real spot metadata and asset IDs change by network and over time. The mock
  contains a tiny fixed universe: USDC, UBTC, UETH.
- Real `l2Book` is a live order book. The mock returns one synthetic bid and
  ask at a configured rate.
- Real Hyperliquid supports resting `Gtc`/`Alo` orders, holds, cancels, partial
  fills, maker/taker behavior, and fee accounting. The mock is IOC-only and has
  no resting book or hold model.
- Real precision, tick-size, lot-size, notional, rate-limit, and activation-gas
  rules are broader than the mock. The mock validates only the pieces needed by
  current router tests.
- Real `orderStatus`, `openOrders`, and `userFills` reflect HyperCore history.
  The mock synthesizes records from its in-memory fills.
- Real user queries must use the trading account/subaccount address, not an
  agent wallet. The mock recovers signatures and can use an optional
  `vaultAddress`, but it does not emulate full agent/subaccount behavior.

## Hyperliquid Bridge

Covered by the mock:

- Hyperliquid bridge deposits are modeled through the mock Bridge2 contract and
  the devnet bridge indexer.
- The indexer observes source-chain deposits and credits the mock Hyperliquid
  clearinghouse balance for the deposited user.
- The router bridge provider observes completion by reading Hyperliquid
  `clearinghouseState`, matching the production provider boundary.
- Hyperliquid `withdraw3` can schedule an EVM release through the same mock
  bridge harness for withdrawal-oriented live/mock coverage.

Important semantic gaps:

- The bridge mock shares the Hyperliquid mock server rather than exposing a
  separate public bridge API, because the router's bridge provider only needs
  the bridge address plus Hyperliquid balance observation.
- Real bridge confirmations, credit latency, replay/idempotency behavior, and
  operational failures are compressed into deterministic mock state changes.
- The current router bridge capability is intentionally narrow: Arbitrum USDC
  ingress into Hyperliquid. Additional bridge assets need explicit capability
  rows and live differential coverage.

## Velora

Covered by the mock:

- `GET /prices` returns a Velora-shaped `priceRoute` for same-chain universal
  router swaps.
- `POST /transactions/:network` returns Velora-shaped transaction fields
  (`to`, `data`, `value`) consumed by the router provider adapter.
- ERC-20 output swaps can materialize local settlement by returning calldata
  that calls the local mock token's unrestricted `mint(receiver, destAmount)`.
  This lets local worker/execution tests observe an output-token balance
  increase without running a full AMM.

Important semantic gaps:

- The mock is not a liquidity, pool-routing, or price-impact simulator.
- ERC-20 input balances are not debited and `transferFrom` behavior is not
  modeled by the Velora mock transaction.
- Native input value handling is only modeled to the extent the router needs
  transaction-shape coverage.
- Real Velora route quality, exchange selection, gas estimates, slippage, and
  calldata semantics must be validated through live differential tests.

## Live Differential Tests

Live-provider tests are intentionally ignored and gated:

- Set `LIVE_PROVIDER_TESTS=1`.
- Use `-- --ignored --nocapture` so the provider transcripts are printed.
- Spending tests require an additional explicit confirmation env var.

The tests are designed to record provider transcripts first, not to assert a
large number of brittle market-data values. The useful output is the exact API
shape and lifecycle sequence observed while using our clients against real
providers.
