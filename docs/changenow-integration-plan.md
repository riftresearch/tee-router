# ChangeNOW Single-Hop Venue Integration Plan

This document outlines the research and integration plan for adding **ChangeNOW** as a single-hop quote venue in the `tee-router` stack.

---

## 1. ChangeNOW API v2 Specifications
ChangeNOW utilizes a REST API (v2) for quoting and currency discovery. All requests require authentication using an API key passed via the HTTP headers.

* **Base URL:** `https://api.changenow.io/v2`
* **Headers Required:**
  * `x-changenow-api-key: <API_KEY>`
  * `Content-Type: application/json`

### Key Endpoints to Implement

#### A. List of Active Currencies (`GET /v2/exchange/currencies`)
* **Endpoint:** `https://api.changenow.io/v2/exchange/currencies?active=true`
* **Response format:** Array of JSON objects:
  ```json
  [
    {
      "ticker": "btc",
      "name": "Bitcoin",
      "network": "btc",
      "tokenContract": null,
      "enabled": true
    },
    {
      "ticker": "usdt",
      "name": "Tether (ERC20)",
      "network": "eth",
      "tokenContract": "0xdac17f958d2ee523a2206206994597c13d831ec7",
      "enabled": true
    }
  ]
  ```

#### B. Rate Estimation (`GET /v2/exchange/estimated-amount`)
* **Endpoint:** `https://api.changenow.io/v2/exchange/estimated-amount`
* **Query Parameters:**
  * `fromCurrency`: Ticker of source token (e.g., `btc`)
  * `toCurrency`: Ticker of destination token (e.g., `eth`)
  * `fromNetwork`: Network of source token (e.g., `btc`)
  * `toNetwork`: Network of destination token (e.g., `eth`)
  * `fromAmount`: Input amount (as decimal string)
  * `flow`: `"standard"` (or `"fixed-rate"` if locked quotes are needed)
* **Response format:**
  ```json
  {
    "toAmount": 4.5432,
    "transactionSpeedForecast": "10-20"
  }
  ```

---

## 2. Asset Mapping Strategy
We will map ChangeNOW’s `network` identifiers to `tee-router` supported `ChainId` values:
* `btc` -> `bitcoin` (regtest/mainnet)
* `eth` -> `evm:1` (Ethereum)
* `arbitrum` -> `evm:42161` (Arbitrum)
* `base` -> `evm:8453` (Base)

### Automated Code Generation (`scripts/gen-single-hop-assets.py`)
We will add a `generate_changenow()` function in `scripts/gen-single-hop-assets.py`:
1. Query `https://api.changenow.io/v2/exchange/currencies?active=true` (which is public and does not require an API key for asset discovery).
2. Filter for supported networks (`btc`, `eth`, `base`, `arbitrum`).
3. Differentiate Native assets (where `tokenContract` is null/empty and ticker matches `eth`/`btc`) from ERC-20 references.
4. Dedup the mappings and write the resulting spec to `crates/router-core/src/services/single_hop_assets/changenow/generated.rs`.

---

## 3. Rust Backend Integration Steps

### Step 1: Register Provider Variant
* In `crates/router-core/src/services/asset_registry.rs`:
  * Add `Changenow` to the `ProviderId` enum.
  * Implement parsing (`"changenow" => Some(Self::Changenow)`) and string representation (`Self::Changenow => "changenow"`).
  * Declare `venue_kind` as `ProviderVenueKind::SingleHop`.
  * Declare `asset_support_model` as `AssetSupportModel::OpenAddressQuote`.
  * Return `true` in `is_single_hop_quote_provider`.
* In `crates/router-gateway-sdk/src/lib.rs`:
  * Add `Changenow` to `ProviderId` enum and parser.

### Step 2: Register Asset Spec Module
* Declare `changenow` module under `crates/router-core/src/services/single_hop_assets.rs`:
  ```rust
  pub mod changenow;
  pub use changenow::CHANGENOW_ASSET_MAP_SPEC;
  ```
* Create `crates/router-core/src/services/single_hop_assets/changenow/mod.rs` to expose `CHANGENOW_ASSET_MAP_SPEC` from the generated file.

### Step 3: Implement `ChangenowQuoteProvider`
* In `crates/router-core/src/services/single_hop_venues.rs`:
  * Define `ChangenowQuoteProvider` carrying its base URL, HTTP client, API key, and `VenueAssetMap`.
  * Implement the `SingleHopQuoteProvider` trait:
    * `id()` -> `ProviderId::Changenow`.
    * `quote_market_order()`: Map `source_asset` and `destination_asset` using the `VenueAssetMap`, build the `estimated-amount` query, attach the `x-changenow-api-key` header, send the GET request, and parse the output into a `SingleHopQuote`.
  * Implement a no-route detector `changenow_response_is_no_route` to gracefully catch and return `None` on errors like out-of-range bounds, inactive pairs, or bad requests.

### Step 4: Configure and Initialize on Startup
* In `bin/router-server/src/lib.rs`:
  * Add `--changenow-api-url` (defaults to `https://api.changenow.io`) and `--changenow-api-key` CLI/environment parameters to `RouterServerArgs`.
* In `bin/router-server/src/app.rs` (`initialize_single_hop_venues`):
  * Read `changenow_api_key` and `changenow_api_url`.
  * Instantiate `ChangenowQuoteProvider` and push it to the active `providers` list if configured.

---

## 4. Router Gateway Integration
* In `apps/router-gateway/src/routes/quote.ts` and related gateway schema files:
  * Register `'changenow'` as a valid value inside Hono Hono’s `ProviderIdSchema` so that client routing constraints can parse and validate it.

---

## 5. Execution Limits (Constraint Note)
* **Single-Hop Quotes are Quote-Only**: Like other single-hop quote providers (Garden, Chainflip, Mayan, Relay, and NearIntents), `OrderManager` will block execution requests at order-creation time (`create_market_order_from_orderable_quote`), returning `MarketOrderError::NoRoute` stating that execution is not implemented. Hence, only quoting and rate estimation pathways need to be integrated.
