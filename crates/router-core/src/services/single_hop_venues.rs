use crate::{
    protocol::DepositAsset,
    services::{
        asset_registry::ProviderId,
        http_body::{read_limited_response_text, response_body_error_preview},
        single_hop_assets::{
            VenueAsset, VenueAssetMap, CHAINFLIP_ASSET_MAP_SPEC, GARDEN_ASSET_MAP_SPEC,
            MAYAN_ASSET_MAP_SPEC, NEAR_INTENTS_ASSET_MAP_SPEC, RELAY_ASSET_MAP_SPEC,
        },
        upstream_proxy::UpstreamProxy,
    },
    telemetry,
};
use chrono::{DateTime, TimeZone, Utc};
use proxy_transport::apply_reqwest_proxy;
use reqwest::{RequestBuilder, StatusCode};
use serde_json::{json, Value};
use std::{future::Future, pin::Pin, sync::Arc, time::Instant};
use url::Url;

const SINGLE_HOP_QUOTE_RESPONSE_MAX_BYTES: usize = 256 * 1024;
const SINGLE_HOP_DEFAULT_QUOTE_TTL_SECONDS: i64 = 5 * 60;
const DEFAULT_SLIPPAGE_BPS: u64 = 100;
const MAYAN_SOLANA_PROGRAM: &str = "FC4eXxkyrMPTjiYUpp4EAnkmwMbQyZ6NDCh1kfLn6vsf";
const MAYAN_FORWARDER_ADDRESS: &str = "0x337685fdaB40D39bd02028545a4FfA7D287cC3E2";
const MAYAN_SDK_VERSION: &str = "13_1_0";

pub type SingleHopQuoteResult<T> = Result<T, String>;
pub type SingleHopQuoteFuture<'a, T> =
    Pin<Box<dyn Future<Output = SingleHopQuoteResult<T>> + Send + 'a>>;

#[derive(Debug, Clone)]
pub struct SingleHopQuoteRequest {
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub amount_in: String,
    pub source_depositor_address: String,
    pub destination_recipient_address: String,
    pub refund_address: String,
}

#[derive(Debug, Clone)]
pub struct SingleHopQuote {
    pub provider_id: ProviderId,
    pub amount_in: String,
    pub estimated_amount_out: String,
    pub min_amount_out: Option<String>,
    pub provider_quote: Value,
    pub expires_at: DateTime<Utc>,
}

pub trait SingleHopQuoteProvider: Send + Sync {
    fn id(&self) -> ProviderId;

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>>;
}

#[derive(Clone, Default)]
pub struct SingleHopVenueRegistry {
    providers: Vec<Arc<dyn SingleHopQuoteProvider>>,
}

impl SingleHopVenueRegistry {
    #[must_use]
    pub fn new(providers: Vec<Arc<dyn SingleHopQuoteProvider>>) -> Self {
        Self { providers }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    #[must_use]
    pub fn providers(&self) -> &[Arc<dyn SingleHopQuoteProvider>] {
        &self.providers
    }

    #[must_use]
    pub fn provider(&self, provider_id: ProviderId) -> Option<Arc<dyn SingleHopQuoteProvider>> {
        self.providers
            .iter()
            .find(|provider| provider.id() == provider_id)
            .cloned()
    }
}

#[derive(Clone)]
pub struct RelayQuoteProvider {
    base_url: String,
    api_key: Option<String>,
    http: reqwest::Client,
    asset_map: VenueAssetMap,
}

impl RelayQuoteProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        proxy_url: Option<&UpstreamProxy>,
    ) -> SingleHopQuoteResult<Self> {
        Ok(Self {
            base_url: normalize_base_url(base_url)?,
            api_key: normalize_optional_secret(api_key),
            http: rustls_http_client(proxy_url)?,
            asset_map: VenueAssetMap::new(RELAY_ASSET_MAP_SPEC)?,
        })
    }
}

impl SingleHopQuoteProvider for RelayQuoteProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Relay
    }

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>> {
        Box::pin(async move {
            let Some(origin) = self
                .asset_map
                .to_venue_asset(&request.source_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let Some(destination) = self
                .asset_map
                .to_venue_asset(&request.destination_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let origin_chain_id = parse_venue_chain_id(ProviderId::Relay, &origin)?;
            let destination_chain_id = parse_venue_chain_id(ProviderId::Relay, &destination)?;
            let body = json!({
                "user": request.source_depositor_address.clone(),
                "originChainId": origin_chain_id,
                "destinationChainId": destination_chain_id,
                "originCurrency": origin.asset,
                "destinationCurrency": destination.asset,
                "amount": request.amount_in.clone(),
                "tradeType": "EXACT_INPUT",
                "recipient": request.destination_recipient_address.clone(),
                "refundTo": request.refund_address.clone(),
            });
            let mut builder = self
                .http
                .post(format!("{}/quote/v2", self.base_url))
                .json(&body);
            if let Some(api_key) = self.api_key.as_ref() {
                builder = builder.header("x-api-key", api_key);
            }
            let Some(response) = send_json_request(
                builder,
                ProviderId::Relay.as_str(),
                "POST",
                "/quote/v2",
                relay_response_is_no_route,
            )
            .await?
            else {
                return Ok(None);
            };
            let amount_out = json_pointer_string(&response, "/details/currencyOut/amount")?;
            let min_amount_out =
                optional_json_pointer_string(&response, "/details/currencyOut/minimumAmount");
            let expires_at = relay_quote_expiry(&response).unwrap_or_else(default_quote_expiry);
            Ok(Some(SingleHopQuote {
                provider_id: ProviderId::Relay,
                amount_in: request.amount_in,
                estimated_amount_out: amount_out,
                min_amount_out,
                provider_quote: json!({
                    "request": body,
                    "response": response,
                }),
                expires_at,
            }))
        })
    }
}

#[derive(Clone)]
pub struct NearIntentsQuoteProvider {
    base_url: String,
    api_key: Option<String>,
    bearer_token: Option<String>,
    http: reqwest::Client,
    asset_map: VenueAssetMap,
}

impl NearIntentsQuoteProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        bearer_token: Option<String>,
        proxy_url: Option<&UpstreamProxy>,
    ) -> SingleHopQuoteResult<Self> {
        Ok(Self {
            base_url: normalize_base_url(base_url)?,
            api_key: normalize_optional_secret(api_key),
            bearer_token: normalize_optional_secret(bearer_token),
            http: rustls_http_client(proxy_url)?,
            asset_map: VenueAssetMap::new(NEAR_INTENTS_ASSET_MAP_SPEC)?,
        })
    }
}

impl SingleHopQuoteProvider for NearIntentsQuoteProvider {
    fn id(&self) -> ProviderId {
        ProviderId::NearIntents
    }

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>> {
        Box::pin(async move {
            let Some(origin) = self
                .asset_map
                .to_venue_asset(&request.source_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let Some(destination) = self
                .asset_map
                .to_venue_asset(&request.destination_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let deadline = Utc::now() + chrono::Duration::minutes(5);
            let body = json!({
                "dry": true,
                "swapType": "EXACT_INPUT",
                "slippageTolerance": DEFAULT_SLIPPAGE_BPS,
                "originAsset": origin.asset,
                "depositType": "ORIGIN_CHAIN",
                "destinationAsset": destination.asset,
                "amount": request.amount_in.clone(),
                "refundTo": request.refund_address.clone(),
                "refundType": "ORIGIN_CHAIN",
                "recipient": request.destination_recipient_address.clone(),
                "recipientType": "DESTINATION_CHAIN",
                "deadline": deadline.to_rfc3339(),
            });
            let mut builder = self
                .http
                .post(format!("{}/v0/quote", self.base_url))
                .json(&body);
            if let Some(api_key) = self.api_key.as_ref() {
                builder = builder.header("X-API-Key", api_key);
            }
            if let Some(token) = self.bearer_token.as_ref() {
                builder = builder.bearer_auth(token);
            }
            let Some(response) = send_json_request(
                builder,
                ProviderId::NearIntents.as_str(),
                "POST",
                "/v0/quote",
                near_intents_response_is_no_route,
            )
            .await?
            else {
                return Ok(None);
            };
            let amount_out = json_pointer_string(&response, "/quote/amountOut")?;
            let min_amount_out = optional_json_pointer_string(&response, "/quote/minAmountOut");
            let expires_at = parse_rfc3339_pointer(&response, "/quote/deadline")
                .or_else(|| parse_rfc3339_pointer(&response, "/quoteRequest/deadline"))
                .unwrap_or(deadline);
            Ok(Some(SingleHopQuote {
                provider_id: ProviderId::NearIntents,
                amount_in: request.amount_in,
                estimated_amount_out: amount_out,
                min_amount_out,
                provider_quote: json!({
                    "request": body,
                    "response": response,
                }),
                expires_at,
            }))
        })
    }
}

#[derive(Clone)]
pub struct MayanQuoteProvider {
    base_url: String,
    api_key: Option<String>,
    http: reqwest::Client,
    asset_map: VenueAssetMap,
}

impl MayanQuoteProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: Option<String>,
        proxy_url: Option<&UpstreamProxy>,
    ) -> SingleHopQuoteResult<Self> {
        Ok(Self {
            base_url: normalize_base_url(base_url)?,
            api_key: normalize_optional_secret(api_key),
            http: rustls_http_client(proxy_url)?,
            asset_map: VenueAssetMap::new(MAYAN_ASSET_MAP_SPEC)?,
        })
    }
}

impl SingleHopQuoteProvider for MayanQuoteProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Mayan
    }

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>> {
        Box::pin(async move {
            let Some(source) = self
                .asset_map
                .to_venue_asset(&request.source_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let Some(destination) = self
                .asset_map
                .to_venue_asset(&request.destination_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let query = vec![
                ("amountIn64", request.amount_in.clone()),
                ("fromToken", source.asset),
                ("fromChain", source.chain),
                ("toToken", destination.asset),
                ("toChain", destination.chain),
                ("slippageBps", DEFAULT_SLIPPAGE_BPS.to_string()),
                ("gasDrop", "0".to_string()),
                ("swift", "true".to_string()),
                ("mctp", "true".to_string()),
                ("fastMctp", "true".to_string()),
                ("wormhole", "true".to_string()),
                ("solanaProgram", MAYAN_SOLANA_PROGRAM.to_string()),
                ("forwarderAddress", MAYAN_FORWARDER_ADDRESS.to_string()),
                ("sdkVersion", MAYAN_SDK_VERSION.to_string()),
            ];
            // `query` is persisted into provider_quote (and surfaced via
            // includeQuoteCandidates); keep the apiKey out of it and append it
            // only to the wire request. Mayan authenticates via query param.
            let mut builder = self
                .http
                .get(format!("{}/quote", self.base_url))
                .query(&query);
            if let Some(api_key) = self.api_key.as_ref() {
                builder = builder.query(&[("apiKey", api_key.as_str())]);
            }
            let Some(response) = send_json_request(
                builder,
                ProviderId::Mayan.as_str(),
                "GET",
                "/quote",
                mayan_response_is_no_route,
            )
            .await?
            else {
                return Ok(None);
            };
            let quotes = response
                .get("quotes")
                .and_then(Value::as_array)
                .ok_or_else(|| "mayan quote response missing quotes array".to_string())?;
            let Some(selected) = quotes.iter().max_by_key(|quote| {
                quote
                    .get("expectedAmountOutBaseUnits")
                    .and_then(Value::as_str)
                    .and_then(parse_u128_string)
                    .unwrap_or(0)
            }) else {
                return Ok(None);
            };
            let amount_out = json_field_string(selected, "expectedAmountOutBaseUnits")?;
            let min_amount_out = optional_json_field_string(selected, "minReceivedBaseUnits")
                .or_else(|| optional_json_field_string(selected, "minAmountOutBaseUnits"));
            let expires_at = json_field_string(selected, "deadline64")
                .ok()
                .and_then(|value| parse_unix_seconds(&value))
                .unwrap_or_else(default_quote_expiry);
            Ok(Some(SingleHopQuote {
                provider_id: ProviderId::Mayan,
                amount_in: request.amount_in,
                estimated_amount_out: amount_out,
                min_amount_out,
                provider_quote: json!({
                    "request": query,
                    "selected_quote": selected,
                    "response": response,
                }),
                expires_at,
            }))
        })
    }
}

#[derive(Clone)]
pub struct ChainflipQuoteProvider {
    base_url: String,
    http: reqwest::Client,
    asset_map: VenueAssetMap,
}

impl ChainflipQuoteProvider {
    pub fn new(
        base_url: impl Into<String>,
        proxy_url: Option<&UpstreamProxy>,
    ) -> SingleHopQuoteResult<Self> {
        Ok(Self {
            base_url: normalize_base_url(base_url)?,
            http: rustls_http_client(proxy_url)?,
            asset_map: VenueAssetMap::new(CHAINFLIP_ASSET_MAP_SPEC)?,
        })
    }
}

impl SingleHopQuoteProvider for ChainflipQuoteProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Chainflip
    }

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>> {
        Box::pin(async move {
            let Some(source) = self
                .asset_map
                .to_venue_asset(&request.source_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let Some(destination) = self
                .asset_map
                .to_venue_asset(&request.destination_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let query = vec![
                ("amount", request.amount_in.clone()),
                ("srcChain", source.chain),
                ("srcAsset", source.asset),
                ("destChain", destination.chain),
                ("destAsset", destination.asset),
                ("isVaultSwap", "false".to_string()),
                ("isOnChain", "true".to_string()),
                ("dcaV2Enabled", "false".to_string()),
            ];
            let Some(response) = send_json_request(
                self.http
                    .get(format!("{}/v2/quote", self.base_url))
                    .query(&query),
                ProviderId::Chainflip.as_str(),
                "GET",
                "/v2/quote",
                chainflip_response_is_no_route,
            )
            .await?
            else {
                return Ok(None);
            };
            let quotes = response
                .as_array()
                .or_else(|| response.get("quotes").and_then(Value::as_array))
                .ok_or_else(|| "chainflip quote response missing quote array".to_string())?;
            let selected = quotes
                .iter()
                .find(|quote| quote.get("type").and_then(Value::as_str) == Some("REGULAR"))
                .or_else(|| {
                    quotes.iter().max_by_key(|quote| {
                        quote
                            .get("egressAmount")
                            .and_then(Value::as_str)
                            .and_then(parse_u128_string)
                            .unwrap_or(0)
                    })
                });
            let Some(selected) = selected else {
                return Ok(None);
            };
            let amount_out = json_field_string(selected, "egressAmount")?;
            Ok(Some(SingleHopQuote {
                provider_id: ProviderId::Chainflip,
                amount_in: request.amount_in,
                estimated_amount_out: amount_out,
                min_amount_out: None,
                provider_quote: json!({
                    "request": query,
                    "selected_quote": selected,
                    "response": response,
                }),
                expires_at: default_quote_expiry(),
            }))
        })
    }
}

#[derive(Clone)]
pub struct GardenQuoteProvider {
    base_url: String,
    api_key: String,
    http: reqwest::Client,
    asset_map: VenueAssetMap,
}

impl GardenQuoteProvider {
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        proxy_url: Option<&UpstreamProxy>,
    ) -> SingleHopQuoteResult<Self> {
        let api_key = api_key.into().trim().to_string();
        if api_key.is_empty() {
            return Err("garden api key must not be empty".to_string());
        }
        Ok(Self {
            base_url: normalize_base_url(base_url)?,
            api_key,
            http: rustls_http_client(proxy_url)?,
            asset_map: VenueAssetMap::new(GARDEN_ASSET_MAP_SPEC)?,
        })
    }
}

impl SingleHopQuoteProvider for GardenQuoteProvider {
    fn id(&self) -> ProviderId {
        ProviderId::Garden
    }

    fn quote_market_order<'a>(
        &'a self,
        request: SingleHopQuoteRequest,
    ) -> SingleHopQuoteFuture<'a, Option<SingleHopQuote>> {
        Box::pin(async move {
            let Some(from) = self
                .asset_map
                .to_venue_asset(&request.source_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let Some(to) = self
                .asset_map
                .to_venue_asset(&request.destination_asset)
                .into_asset()
            else {
                return Ok(None);
            };
            let from = garden_asset_id(&from);
            let to = garden_asset_id(&to);
            let query = vec![
                ("from", from),
                ("to", to),
                ("from_amount", request.amount_in.clone()),
                ("slippage", DEFAULT_SLIPPAGE_BPS.to_string()),
            ];
            let Some(response) = send_json_request(
                self.http
                    .get(format!("{}/quote", self.base_url))
                    .query(&query)
                    .header("garden-app-id", &self.api_key),
                ProviderId::Garden.as_str(),
                "GET",
                "/quote",
                garden_response_is_no_route,
            )
            .await?
            else {
                return Ok(None);
            };
            if response.get("status").and_then(Value::as_str) != Some("Ok") {
                return Ok(None);
            }
            let quotes = response
                .get("result")
                .and_then(Value::as_array)
                .ok_or_else(|| "garden quote response missing result array".to_string())?;
            let Some(selected) = quotes.iter().max_by_key(|quote| {
                quote
                    .pointer("/destination/amount")
                    .and_then(Value::as_str)
                    .and_then(parse_u128_string)
                    .unwrap_or(0)
            }) else {
                return Ok(None);
            };
            let amount_out = json_pointer_string(selected, "/destination/amount")?;
            Ok(Some(SingleHopQuote {
                provider_id: ProviderId::Garden,
                amount_in: request.amount_in,
                estimated_amount_out: amount_out,
                min_amount_out: None,
                provider_quote: json!({
                    "request": query,
                    "selected_quote": selected,
                    "response": response,
                }),
                expires_at: default_quote_expiry(),
            }))
        })
    }
}

async fn send_json_request(
    builder: RequestBuilder,
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    is_no_route: fn(StatusCode, &Value, &str) -> bool,
) -> SingleHopQuoteResult<Option<Value>> {
    let method_label = method;
    let started = Instant::now();
    let response = match builder.send().await {
        Ok(response) => response,
        Err(err) => {
            telemetry::record_trading_venue_transport_error(
                venue,
                method_label,
                endpoint,
                started.elapsed(),
            );
            return Err(format!(
                "{venue} quote request failed: {}",
                err.without_url()
            ));
        }
    };
    let status = response.status();
    telemetry::record_trading_venue_http_status(
        venue,
        method_label,
        endpoint,
        status.as_u16(),
        started.elapsed(),
    );
    let body = read_limited_response_text(response, SINGLE_HOP_QUOTE_RESPONSE_MAX_BYTES)
        .await
        .map_err(|err| format!("{venue} quote response body failed: {err}"))?;
    if body.truncated {
        return Err(format!(
            "{venue} quote response body exceeded {SINGLE_HOP_QUOTE_RESPONSE_MAX_BYTES} bytes"
        ));
    }
    let text = body.text;
    let parsed = serde_json::from_str::<Value>(&text).map_err(|err| {
        format!(
            "{venue} quote response was not JSON: {err}: {}",
            response_body_error_preview(&text)
        )
    })?;
    if !status.is_success() {
        if is_no_route(status, &parsed, &text) {
            return Ok(None);
        }
        return Err(format!(
            "{venue} quote request failed with {status}: {}",
            response_body_error_preview(&text)
        ));
    }
    if is_no_route(status, &parsed, &text) {
        return Ok(None);
    }
    Ok(Some(parsed))
}

fn normalize_base_url(base_url: impl Into<String>) -> Result<String, String> {
    let base_url = base_url.into().trim().trim_end_matches('/').to_string();
    let sanitized_base_url = sanitize_url_for_error(&base_url);
    let parsed = Url::parse(&base_url).map_err(|err| {
        format!("single-hop provider base URL {sanitized_base_url:?} is invalid: {err}")
    })?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(format!(
                "single-hop provider base URL {sanitized_base_url:?} must use http or https, got {scheme:?}"
            ));
        }
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(format!(
            "single-hop provider base URL {sanitized_base_url:?} must not include credentials"
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(format!(
            "single-hop provider base URL {sanitized_base_url:?} must not include a query string or fragment"
        ));
    }
    Ok(base_url)
}

fn rustls_http_client(proxy_url: Option<&UpstreamProxy>) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder().use_rustls_tls();
    let builder = apply_reqwest_proxy(builder, proxy_url)
        .map_err(|err| format!("failed to configure SOCKS5 proxy: {err}"))?;
    builder
        .build()
        .map_err(|err| format!("failed to construct reqwest client: {err}"))
}

fn normalize_optional_secret(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn sanitize_url_for_error(value: &str) -> String {
    if let Ok(mut parsed) = Url::parse(value.trim()) {
        if !parsed.username().is_empty() {
            let _ = parsed.set_username("redacted");
        }
        if parsed.password().is_some() {
            let _ = parsed.set_password(Some("redacted"));
        }
        if parsed.query().is_some() {
            parsed.set_query(Some("redacted"));
        }
        if parsed.fragment().is_some() {
            parsed.set_fragment(Some("redacted"));
        }
        parsed.to_string()
    } else {
        value.chars().take(128).collect()
    }
}

fn parse_venue_chain_id(provider_id: ProviderId, asset: &VenueAsset) -> SingleHopQuoteResult<u64> {
    asset.chain.parse::<u64>().map_err(|err| {
        format!(
            "{} asset map produced non-numeric chain id {}: {err}",
            provider_id.as_str(),
            asset.chain
        )
    })
}

fn garden_asset_id(asset: &VenueAsset) -> String {
    format!("{}:{}", asset.chain, asset.asset)
}

fn relay_response_is_no_route(status: StatusCode, value: &Value, _body: &str) -> bool {
    if status.is_success() {
        return false;
    }
    matches!(
        value.get("errorCode").and_then(Value::as_str),
        Some(
            "NO_QUOTES"
                | "NO_SWAP_ROUTES_FOUND"
                | "NO_INTERNAL_SWAP_ROUTES_FOUND"
                | "INSUFFICIENT_LIQUIDITY"
                | "UNSUPPORTED_ROUTE"
                | "UNSUPPORTED_CHAIN"
                | "UNSUPPORTED_CURRENCY"
                | "INVALID_INPUT_CURRENCY"
                | "INVALID_OUTPUT_CURRENCY"
                | "AMOUNT_TOO_LOW"
                | "CHAIN_DISABLED"
                | "ROUTE_TEMPORARILY_RESTRICTED"
                | "REQUEST_TIMED_OUT"
                | "RPC_HTTP_ERROR"
                | "SWAP_IMPACT_TOO_HIGH"
        )
    )
}

fn near_intents_response_is_no_route(status: StatusCode, value: &Value, _body: &str) -> bool {
    if status.is_success() {
        return false;
    }
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default();
    status == StatusCode::BAD_REQUEST
        && (message.contains("tokenIn is not valid")
            || message.contains("tokenOut is not valid")
            || message.contains("Failed to get quote"))
}

fn mayan_response_is_no_route(status: StatusCode, value: &Value, _body: &str) -> bool {
    if status.is_success() {
        return value
            .get("quotes")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty);
    }
    matches!(
        value.get("code").and_then(Value::as_str),
        Some("ROUTE_NOT_FOUND" | "TOKEN_NOT_FOUND" | "AMOUNT_TOO_SMALL" | "SAME_TOKEN")
    ) || status == StatusCode::NOT_ACCEPTABLE
}

fn chainflip_response_is_no_route(status: StatusCode, value: &Value, _body: &str) -> bool {
    if status.is_success() {
        return false;
    }
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    status == StatusCode::BAD_REQUEST
        && (message.contains("requested assets cannot be swapped")
            || message.contains("insufficient liquidity")
            || message.contains("below minimum swap amount")
            || message.contains("amount is lower")
            || message.contains("lower than minimum egress amount")
            || message.contains("invalid request"))
}

fn garden_response_is_no_route(_status: StatusCode, value: &Value, _body: &str) -> bool {
    value.get("status").and_then(Value::as_str) == Some("Error")
        || value
            .get("result")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn json_pointer_string(value: &Value, pointer: &str) -> SingleHopQuoteResult<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| format!("quote response missing string at {pointer}"))
}

fn optional_json_pointer_string(value: &Value, pointer: &str) -> Option<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn json_field_string(value: &Value, field: &str) -> SingleHopQuoteResult<String> {
    value
        .get(field)
        .and_then(|field_value| match field_value {
            Value::String(value) => Some(value.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .ok_or_else(|| format!("quote response missing string/number field {field}"))
}

fn optional_json_field_string(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(|field_value| match field_value {
        Value::String(value) => Some(value.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn relay_quote_expiry(value: &Value) -> Option<DateTime<Utc>> {
    value
        .pointer("/protocol/v2/orderData/output/deadline")
        .and_then(Value::as_u64)
        .and_then(|seconds| parse_unix_seconds(&seconds.to_string()))
}

fn parse_rfc3339_pointer(value: &Value, pointer: &str) -> Option<DateTime<Utc>> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn parse_unix_seconds(value: &str) -> Option<DateTime<Utc>> {
    let seconds = value.parse::<i64>().ok()?;
    Utc.timestamp_opt(seconds, 0).single()
}

fn default_quote_expiry() -> DateTime<Utc> {
    Utc::now() + chrono::Duration::seconds(SINGLE_HOP_DEFAULT_QUOTE_TTL_SECONDS)
}

fn parse_u128_string(value: &str) -> Option<u128> {
    value.parse::<u128>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{AssetId, ChainId};

    fn asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).expect("chain"),
            asset,
        }
    }

    #[test]
    fn maps_relay_router_assets() {
        let map = VenueAssetMap::new(RELAY_ASSET_MAP_SPEC).expect("relay map");
        let btc = map
            .to_venue_asset(&asset("bitcoin", AssetId::Native))
            .into_asset()
            .expect("btc");
        assert_eq!(btc.chain, "8253038");
        assert_eq!(btc.asset, "bc1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqmql8k8");

        let usdc = map
            .to_venue_asset(&asset(
                "evm:1",
                AssetId::reference("0xA0b86991c6218B36C1d19D4a2e9Eb0cE3606eB48"),
            ))
            .into_asset()
            .expect("ethereum usdc");
        assert_eq!(usdc.chain, "1");
        assert_eq!(usdc.asset, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    }

    #[test]
    fn maps_near_intents_router_assets() {
        let map = VenueAssetMap::new(NEAR_INTENTS_ASSET_MAP_SPEC).expect("near intents map");
        let btc = map
            .to_venue_asset(&asset("bitcoin", AssetId::Native))
            .into_asset()
            .expect("btc");
        assert_eq!(btc.chain, "btc");
        assert_eq!(btc.asset, "nep141:btc.omft.near");

        let base_usdc = map
            .to_venue_asset(&asset(
                "evm:8453",
                AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            ))
            .into_asset()
            .expect("base usdc");
        assert_eq!(base_usdc.chain, "base");
        assert_eq!(
            base_usdc.asset,
            "nep141:base-0x833589fcd6edb6e08f4c7c32d4f71b54bda02913.omft.near"
        );
    }

    #[test]
    fn maps_chainflip_router_assets() {
        let map = VenueAssetMap::new(CHAINFLIP_ASSET_MAP_SPEC).expect("chainflip map");
        let btc = map
            .to_venue_asset(&asset("bitcoin", AssetId::Native))
            .into_asset()
            .expect("btc");
        assert_eq!(btc.chain, "Bitcoin");
        assert_eq!(btc.asset, "BTC");

        let usdc = map
            .to_venue_asset(&asset(
                "evm:42161",
                AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
            ))
            .into_asset()
            .expect("arbitrum usdc");
        assert_eq!(usdc.chain, "Arbitrum");
        assert_eq!(usdc.asset, "USDC");
        assert!(map
            .to_venue_asset(&asset(
                "evm:8453",
                AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            ))
            .into_asset()
            .is_none());
    }

    #[test]
    fn maps_garden_router_assets() {
        let map = VenueAssetMap::new(GARDEN_ASSET_MAP_SPEC).expect("garden map");
        let usdc = map
            .to_venue_asset(&asset(
                "evm:1",
                AssetId::reference("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            ))
            .into_asset()
            .expect("ethereum usdc");
        assert_eq!(garden_asset_id(&usdc), "ethereum:usdc");

        let cbbtc = map
            .to_venue_asset(&asset(
                "evm:8453",
                AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
            ))
            .into_asset()
            .expect("base cbbtc");
        assert_eq!(garden_asset_id(&cbbtc), "base:cbbtc");
    }
    #[test]
    fn detects_single_hop_no_route_payloads() {
        assert!(relay_response_is_no_route(
            StatusCode::BAD_REQUEST,
            &json!({"errorCode":"NO_QUOTES"}),
            ""
        ));
        assert!(mayan_response_is_no_route(
            StatusCode::NOT_ACCEPTABLE,
            &json!({"code":"ROUTE_NOT_FOUND"}),
            ""
        ));
        assert!(garden_response_is_no_route(
            StatusCode::OK,
            &json!({"status":"Ok", "result": []}),
            ""
        ));
    }
}
