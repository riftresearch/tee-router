# Hyperliquid, Unit, and Across Market-Order Plan

Research date: 2026-04-14.

## Status

This is a design note, not an implemented adapter. The current router already has
the right outer shape for this work:

- `router-api` and `router-worker` are separate binaries.
- Market-order quotes persist a generic `router_orders` row plus a
  `market_order_quotes` child row.
- Quote aggregation is provider-based and already runs providers concurrently.
- Funding vaults can be linked to generic orders atomically.
- Action intent now lives in `market_order_actions`, not in generic
  `router_orders` JSON.
- Private execution state now uses `custody_vaults` and generic
  `order_execution_steps` rows with provider-specific JSONB detail.
- The worker currently only processes refunds; market-order execution is still
  missing.
- The corrected primary custody flow is:
  `Rift deposit vault -> Across -> Rift destination execution vault -> Unit
  deposit address -> Hyperliquid subaccount -> Hyperliquid spot execution`.

## Primary Sources

- Hyperliquid API docs:
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets
  - https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/hyperevm/hypercore-less-than-greater-than-hyperevm-transfers
- Unit / HyperUnit docs:
  - https://docs.hyperunit.xyz/developers/api
  - https://docs.hyperunit.xyz/developers/api/generate-address
  - https://docs.hyperunit.xyz/developers/api/estimate-fees
  - https://docs.hyperunit.xyz/developers/api/operations
  - https://docs.hyperunit.xyz/developers/api/operations/deposit-lifecycle
  - https://docs.hyperunit.xyz/developers/api/operations/withdrawal-lifecycle
- Across docs:
  - https://docs.across.to/introduction/swap-api
  - https://docs.across.to/introduction/hypercore
  - https://docs.across.to/api-reference/swap/approval/get
  - https://docs.across.to/api-reference/swap/counterfactual/get
  - https://docs.across.to/introduction/embedded-actions/transfer-erc20

## Live Endpoint Snapshot

The following were checked directly on 2026-04-14.

Hyperliquid:

- `POST https://api.hyperliquid.xyz/info {"type":"spotMeta"}` returns spot
  token metadata and spot pairs.
- `POST https://api.hyperliquid.xyz/info {"type":"spotMetaAndAssetCtxs"}`
  returned 454 spot tokens and 291 spot pairs.
- Live spot mappings observed:
  - `USDC`: token index `0`
  - `HYPE`: token index `150`, spot pair `@107`
  - `UBTC`: token index `197`, spot pair `@142` against USDC
  - `UETH`: token index `221`, spot pair `@151` against USDC
- `POST /info {"type":"l2Book","coin":"@142"}` returned usable UBTC/USDC
  depth. Quote simulation should use book depth, not only `allMids`.

Unit:

- `GET https://api.hyperunit.xyz/v2/estimate-fees` returned fee estimates for
  `bitcoin`, `ethereum`, `ethereum-erc20`, `base`, `base-erc20`, `solana`,
  `spl`, `avalanche`, `monad`, `plasma`, and `zcash`.
- The fee endpoint looks network-cost based. Unit docs explicitly say Unit does
  not collect revenue from deposits or withdrawals; operation fees are source,
  destination, and sweep transaction costs.

Across:

- `GET https://app.across.to/api/swap/chains` includes:
  - HyperEVM chain ID `999`
  - HyperCore chain ID `1337`
- `GET https://app.across.to/api/available-routes` currently shows USDC/USDT
  routes into and out of HyperEVM `999`, not HyperCore `1337`.
- `GET /api/swap/approval` without an API key successfully quoted
  Arbitrum USDT -> HyperEVM USDC with `destinationChainId=999`. The response
  included `approvalTxns`, `swapTx`, `expectedOutputAmount`,
  `minOutputAmount`, `expectedFillTime`, `fees`, and `quoteExpiryTimestamp`.
- The same style request with `destinationChainId=1337` returned token-detail
  404s for the public endpoint. Across docs still describe the HyperCore path as
  `destinationChainId=1337` and say production requires an API key. Treat this as
  unresolved until verified with an Across API key and `/swap/tokens`.

## What Each System Should Do For Us

### Hyperliquid

Hyperliquid is the execution venue. We use it for price discovery, book-depth
simulation, signed spot orders, balance checks, and internal transfers.

Important semantics:

- Spot metadata is retrieved from `spotMeta`; live balances are retrieved from
  `spotClearinghouseState`.
- For spot order actions, the exchange `asset` is `10000 + spotMeta.universe`
  index. For example, live UBTC/USDC `@142` means order asset `10142`.
- Non-USDC spot pairs are often named `@{index}` even when the UI maps them to
  friendly names. We need our own token-index and token-id mapping.
- Market-order behavior should be implemented as aggressive IOC limit orders,
  not resting GTC orders.
- Price and size formatting must obey Hyperliquid tick/lot rules. Spot prices
  have precision constraints, and sizes must be rounded to `szDecimals`.
- API-wallet signing is subtle. Hyperliquid recommends using an SDK instead of
  hand-rolled signatures. If we implement signing in Rust, we should port the
  official Python SDK logic and add cross-language signature test vectors.
- API-wallet nonces are per signer. A single API wallet signing for multiple
  subaccounts shares one nonce set. The worker should serialize signed
  Hyperliquid actions through a signer actor or use distinct API wallets per
  execution partition.

### Unit / HyperUnit

Unit is the native-chain ingress/egress layer for assets such as BTC, ETH, SOL,
and other Unit-supported assets.

Important semantics:

- `GET /gen/:src_chain/:dst_chain/:asset/:dst_addr` generates a Unit protocol
  address tied to the destination address.
- For deposits, the protocol address lives on the source chain and credits a
  Hyperliquid destination address.
- For withdrawals, the protocol address lives on Hyperliquid and is assigned to
  a destination-chain user address.
- `GET /operations/:address` is the main lifecycle tracking API. It returns both
  generated protocol addresses and operations with source/destination hashes,
  fee amounts, state, and confirmations.
- Unit operation IDs are source-transaction based:
  - Bitcoin: `<txid>:<vout>`
  - Ethereum: `<tx-hash>:<trace-id>`
  - Hyperliquid: `<sender_address>:<nonce>`
  - Solana: `<signature>:<destination-address>`
- Deposits and withdrawals are asynchronous. The worker must poll operation
  state until `done` before considering an ingress/egress leg final.
- Unit docs have inconsistent minimums between pages. The generated-address
  minimum table is the better current reference, but implementation should fail
  closed when Unit rejects a transfer or returns an operation failure.

### Across

Across is the EVM-origin movement layer. In the primary design, it moves asset
`Z` from the user's Rift vault on chain `Y` to a Rift-controlled destination
execution vault on a Unit-supported chain `X`. The worker then sends from that
execution vault into the Unit deposit address. Stablecoins are the obvious first
candidates, but the actual allowlist must be the intersection of Across output
support and Unit deposit support.

Important semantics:

- `/swap/approval` returns the executable origin transaction and optional token
  approvals.
- Across supports `exactInput`, `minOutput`, and `exactOutput` trade types.
  This maps naturally to our API-level exact-input and exact-output model.
- `strictTradeType` defaults to true. Keep it true so Across does not silently
  downgrade exact-output behavior.
- Responses include `inputAmount`, `maxInputAmount`, `expectedOutputAmount`,
  `minOutputAmount`, fees, quote expiry, and expected fill time.
- Across HyperCore docs say `destinationChainId=1337` should automatically
  bridge into HyperCore, but the public route endpoint currently exposes
  HyperEVM `999` routes. This difference matters for custody and isolation.
- If Across lands funds on HyperEVM `999`, and not directly on HyperCore, we
  must perform the HyperEVM -> HyperCore transfer ourselves by transferring the
  EVM spot ERC20 to the token's HyperCore system address. That requires an EVM
  private key and HYPE gas on HyperEVM.

## Deposit-Vault-Scoped Hyperliquid Subaccounts

The intended accounting model is one Hyperliquid subaccount per Rift deposit
vault. Rift deposit vaults are one-time-use, so Hyperliquid subaccounts should
also be one-time-use within this product.

The corrected custody boundary is:

1. The user deposits into a Rift deposit vault on source chain `Y`.
2. The router worker controls that vault key.
3. The paymaster funds that vault for action gas if needed.
4. The worker signs an Across transaction from the vault.
5. Across delivers asset `Z` onto Unit-supported chain `X`.
6. The Across recipient is a Rift-controlled destination execution vault.
7. The paymaster funds the destination execution vault with gas.
8. The destination execution vault sends `Z` to the Unit-generated deposit
   address for the vault-scoped Hyperliquid subaccount.
9. Unit credits the Hyperliquid subaccount on HyperCore.
10. The worker trades from that subaccount by signing through the master/API
   wallet with `vaultAddress = subaccount`.

This means the Unit deposit address is not the initial user-facing vault. It is
an execution target inside the persisted route plan.

The main proof still needed is that Unit accepts Hyperliquid subaccount
addresses as deposit destinations. If it does, attribution is clean: each order
has one deposit vault, one Unit deposit address, and one Hyperliquid subaccount.

## Proposed Route Families

### Rift Vault -> Across -> Rift Execution Vault -> Unit -> Hyperliquid

This is the primary EVM-origin market-order route.

Example: user deposits USDC on Base into a Rift vault, the worker uses Across to
move USDC to Ethereum or Base Unit deposit infrastructure, and Unit credits the
vault-scoped Hyperliquid subaccount.

Flow:

1. Quote request arrives at `POST /api/v1/quotes/market-order`.
2. Provider chooses a Unit-supported source chain `X` and asset `Z`.
3. Provider creates or plans a Hyperliquid subaccount for the future deposit
   vault.
4. Provider generates a Unit deposit address for
   `{src_chain: X, dst_chain: hyperliquid, asset: Z, dst_addr: subaccount}`.
5. Provider calls Across `/swap/approval` from source chain `Y` to chain `X`,
   disables partial fills, and sets the Across refund address to a Rift-controlled
   refund/source vault.
6. Provider fetches Hyperliquid spot metadata/book depth and simulates the
   post-Unit credited amount through the spot order.
7. API persists the selected route in `market_order_quotes.provider_quote`.
8. Client creates the Rift deposit vault and deposits asset `Z` on chain `Y`.
9. Worker confirms vault funding.
10. Paymaster funds the vault for Across approval/swap gas if needed.
11. Worker signs and submits the Across approval/swap from the Rift vault.
12. Across delivers asset `Z` to the Rift destination execution vault on chain
    `X`.
13. Paymaster funds the destination execution vault with gas.
14. Worker sends `Z` from the destination execution vault to the Unit deposit
    address.
15. Worker polls Unit operations until the Hyperliquid subaccount is credited.
16. Worker places an IOC spot order on Hyperliquid with
    `vaultAddress = subaccount`.
17. Worker executes the chosen egress path.
18. Worker marks the order and vault completed only after destination delivery
    evidence.

This route keeps the Rift deposit vault as the first custody point and avoids
requiring users to interact with Across directly.

### Rift Vault -> Unit -> Hyperliquid

This route is for cases where the user's deposit vault is already on the same
chain and asset that Unit accepts. Across is unnecessary.

Example: user deposits ETH into an Ethereum Rift vault, and the worker sends ETH
directly to the Unit Ethereum deposit address for the vault-scoped Hyperliquid
subaccount.

Flow:

1. User funds the Rift vault.
2. Worker sends the funded asset directly from the vault to the Unit deposit
   address.
3. Unit credits the vault-scoped Hyperliquid subaccount.
4. Worker executes the Hyperliquid spot leg.

This is simpler than the Across route, but only applies when the source chain
and asset already line up with Unit's supported deposit surface.

### HyperEVM / HyperCore Across Routes

Across routes that land directly on HyperEVM `999` or HyperCore `1337` are not
required for the primary ingress design above. They may still matter later for
egress or for a direct EVM-stablecoin destination path.

Keep the earlier `1337` vs `999` finding as an integration risk, but it should
not block the primary plan of Across depositing into a Unit address on chain
`X`.

## Required Changes To Current Router

### Provider Injection

`initialize_components` currently constructs `OrderManager::new(...)` with no
market-order providers. Add a configured provider list and inject a
`HyperliquidUnitAcrossProvider`.

Likely config:

- `HYPERLIQUID_API_URL`, default `https://api.hyperliquid.xyz`
- `HYPERLIQUID_CHAIN`, `Mainnet` or `Testnet`
- `HYPERLIQUID_SIGNATURE_CHAIN_ID`
- `HYPERLIQUID_MASTER_ADDRESS`
- `HYPERLIQUID_API_WALLET_PRIVATE_KEY`
- `HYPERLIQUID_EXECUTION_ACCOUNT_MODE`, expected
  `deposit_vault_scoped_subaccount`
- `UNIT_API_URL`, default `https://api.hyperunit.xyz`
- `ACROSS_API_URL`, default `https://app.across.to/api`
- `ACROSS_API_KEY`
- `ACROSS_INTEGRATOR_ID`
- optional `HYPEREVM_RPC_URL`, default `https://rpc.hyperliquid.xyz/evm`, only
  needed for future HyperEVM egress or direct HyperEVM routes

### Asset Model

The public `DepositAsset` model can stay chain-agnostic, but the provider needs
an internal asset map:

- External chain/asset: `bitcoin:native`, `evm:1:<token>`, `evm:8453:<token>`
- Hyperliquid spot token: token name, token index, token ID, `weiDecimals`,
  `szDecimals`
- HyperEVM spot token: chain ID `999`, ERC20 address, system address, extra EVM
  decimals
- Unit asset symbol: `btc`, `eth`, `sol`, etc.
- Across token details: origin/destination chain ID, ERC20 address, decimals,
  symbol

Current router support is only `bitcoin`, `evm:1`, and `evm:8453`. That is fine
for a narrow v0. Quotes for Arbitrum or other EVM chains should remain disabled
until the router can create vaults, validate addresses, sign transactions, and
fund gas on those chains.

### Quote Payload

`market_order_quotes.provider_quote` should store a full normalized route
snapshot, not raw provider JSON alone.

Recommended shape:

```json
{
  "version": 1,
  "provider": "hyperliquid_unit_across",
  "route_family": "rift_vault_across_unit",
  "ingress": {
    "kind": "across_to_unit_deposit",
    "vault_source_chain": "evm:8453",
    "vault_source_asset": "token-z-on-chain-y",
    "unit_source_chain": "unit-supported-chain-x",
    "unit_asset": "asset_z",
    "unit_protocol_address": "0x...",
    "hyperliquid_destination_subaccount": "0x...",
    "across": {
      "origin_chain_id": 8453,
      "destination_chain_id": 1,
      "input_token": "token-z-on-chain-y",
      "output_token": "token-z-on-chain-x",
      "recipient": "0x..."
    }
  },
  "execution": {
    "venue": "hyperliquid_spot",
    "legs": [
      {
        "coin": "@142",
        "asset_id": 10142,
        "base_token": "UBTC",
        "quote_token": "USDC",
        "side": "sell",
        "tif": "Ioc",
        "expected_price": "74166.0"
      }
    ]
  },
  "egress": {
    "kind": "unit_withdrawal",
    "destination_chain": "bitcoin",
    "unit_asset": "btc",
    "recipient_address": "bc1..."
  },
  "constraints": {
    "amount_in": "1000000",
    "min_amount_out": "900000"
  },
  "expires_at": "2026-04-14T19:05:00Z"
}
```

Raw Across and Unit responses can be nested under `raw` fields, but execution
code should consume the normalized plan.

### Worker State Machine

The worker needs a market-order pass next to the refund pass:

1. Claim actionable funded orders with a per-row lease.
2. Move `router_orders.status` and `deposit_vaults.status` with compare-and-swap
   transitions.
3. Insert `order_execution_steps` for every planned internal or external step.
4. Execute one idempotent step at a time.
5. Persist step evidence before moving to the next step.
6. Reconcile balances/operations before retrying after a crash.
7. Mark `completed` only after destination delivery evidence.
8. Mark `refunding` when execution cannot satisfy the original user constraints
   before timeout.

Suggested execution substates:

- `awaiting_vault_funding`
- `ingress_submitted`
- `ingress_confirming`
- `venue_balance_confirmed`
- `spot_order_submitted`
- `spot_order_filled`
- `egress_submitted`
- `egress_confirming`
- `completed`
- `refund_required`

These should map onto `order_execution_steps.status`, plus provider-specific
detail in the step JSON fields. Generic order status should remain in
`router_orders`.

### Paymaster And Gas

The EVM paymaster actor already exists for ERC20 refund gas top-ups. It needs to
be generalized for action execution:

- Across approval transaction gas
- Across swap transaction gas
- HyperEVM ERC20 transfer-to-system-address gas
- HyperEVM outbound Across transaction gas

The current actor estimates gas for ERC20 refunds specifically. For market
orders it should accept a concrete EVM transaction plan and fund only the amount
needed to execute that plan plus the configured dust buffer.

### Hyperliquid Signer Actor

Add one long-running signer actor per API wallet. It should own nonce allocation
and signed exchange submissions. Do not scatter Hyperliquid signing across
worker subtasks.

Commands:

- `PlaceSpotIocOrder`
- `SendSpotAsset`
- `SendAsset`
- `QueryOrderStatus` can be read-only and does not need the signer

The actor should persist the nonce/action hash before submit or atomically with
the execution attempt row so retries can reconcile instead of double-submit.

## Exact-Input And Exact-Output

Exact-input:

- Across: use `tradeType=exactInput` for Across legs.
- Hyperliquid: simulate output by walking the order book, then place IOC with a
  limit price that preserves `min_amount_out`.
- Unit: subtract Unit network/sweep/destination estimates conservatively.

Exact-output:

- Across: use `tradeType=exactOutput` where the Across route supports it.
- Hyperliquid: solve the inverse book walk to compute max required input, then
  place IOC with a limit price that cannot exceed `max_amount_in`.
- Unit: include withdrawal/deposit fee estimates and require enough source
  amount to cover them.

If any leg cannot support exact-output safely, the provider should return
no-route. It must not silently convert exact-output into exact-input.

## Refund Semantics

Refunds are easy only before ingress.

Cases:

- Vault funded but no ingress side effect submitted: refund from the vault using
  existing refund machinery.
- Across approval submitted but swap not submitted: refund source asset from the
  vault after ensuring no spend happened.
- Unit or Across ingress submitted: funds may now be on Unit, HyperCore, or
  HyperEVM. A normal deposit-vault refund is no longer sufficient.
- Spot trade partially filled: refund requires unwinding or delivering the
  best valid asset to a recovery path.

V0 should avoid ambiguous post-ingress refunds by using small allowlisted route
families and strict pre-flight validation. Before production size, define a
post-ingress recovery policy:

- unwind to source asset if possible and send to recovery address, or
- send current asset to a user-provided recovery address on the asset's current
  chain, or
- manually intervene with an auditable stuck state.

## Recommended Implementation Phases

1. Add read-only clients:
   - Hyperliquid info client for spot metadata, books, balances, order status,
     and fills.
   - Unit client for address generation, fee estimates, and operations.
   - Across client for chains, routes, approval quotes, sources, and eventually
     tokens once the endpoint/API key works.
2. Add asset registry:
   - Hardcode a narrow allowlist first: BTC, ETH, USDC, USDT, HYPE, UBTC, UETH.
   - Include token IDs, spot pair indexes, decimals, HyperEVM ERC20 addresses,
     system addresses, and Unit symbols.
3. Add quote-only provider:
   - Start with Rift-vault -> Across -> Rift destination execution vault ->
     Unit routes where Across output exactly matches a Unit-supported deposit
     asset.
   - Also support direct Rift-vault -> Unit routes where no Across hop is
     needed.
   - Store normalized provider quote payloads.
   - Return no-route for unsupported exact-output paths.
4. Add worker claims on the existing execution tables:
   - `order_execution_steps` state.
   - `custody_vaults` role-labeled addresses.
   - Idempotency keys for each external transaction.
   - CAS order transitions.
5. Execute small-size ingress path:
   - Rift vault -> Across -> Rift destination execution vault -> Unit deposit
     address -> Hyperliquid subaccount.
   - Prove Unit credits the subaccount and records an operation we can
     reconcile.
   - Use testnet where possible, then tiny mainnet canary amounts.
6. Execute Hyperliquid IOC spot from the vault-scoped subaccount.
7. Add egress through Unit or another provider-specific path.
8. Keep direct Across `999` / `1337` Hyperliquid routes as later optional
   surfaces, not primary ingress.

## Production Risks And Required Proofs

- Across docs/API mismatch: verify `1337` with API key and `/swap/tokens`.
- Subaccount custody: prove Unit credits the vault-scoped subaccount and
  Hyperliquid lets the configured master/API wallet trade from it with
  `vaultAddress`.
- Unit destination support: prove `/gen/.../{subaccount}` accepts the
  vault-scoped Hyperliquid subaccount and credits it after Across delivery.
- Across recipient behavior: prove Across can deliver the exact output token to
  the Unit protocol address and that Unit recognizes the resulting transfer.
- Hyperliquid signing: build test vectors against the official Python SDK.
- Nonce safety: one signer actor per API wallet.
- Unit minimums: docs disagree across pages; fail closed and test live.
- Post-ingress refunds: define recovery before meaningful value.
- Balance reconciliation: every worker retry must start by querying current
  vault, HyperEVM, HyperCore, Unit, and Across state.
- Activation fees: new HyperCore accounts can require a one-time quote-token fee
  before sending CoreWriter actions. In live Unit BTC withdrawal testing, the
  first `sendAsset` to a fresh Unit HyperCore protocol address consumed `1 USDC`
  from the sender's spot wallet. Router quotes must reserve and expose this as
  a venue fee for Hyperliquid -> Unit withdrawal paths; it is separate from
  Unit's Bitcoin/network fee estimate and from Hyperliquid Bridge2's recurring
  `1 USDC` USDC withdrawal gas fee.
- HyperEVM gas: `999` routes can create HYPE gas requirements that are separate
  from source/destination token balances.
- Address validation: BTC, EVM, Hyperliquid/Core, HyperEVM, and Unit protocol
  addresses need provider-specific validation.

## Bottom Line

The architecture fits our current router if we treat Hyperliquid + Unit + Across
as one market-order provider with a normalized route plan. The primary ingress
path should be Rift deposit vault -> Across -> Rift destination execution vault
-> Unit deposit address -> vault-scoped Hyperliquid subaccount. This preserves
the deposit-vault custody model while adding a Rift-controlled checkpoint before
Unit. Direct Across routes into Unit, HyperEVM, or HyperCore are optional later
surfaces, not the foundation of this design.
