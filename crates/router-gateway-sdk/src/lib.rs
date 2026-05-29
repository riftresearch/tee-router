//! Typed async client for the Rift router gateway HTTP API.
//!
//! The gateway (`apps/router-gateway`) is the public entrypoint to the router
//! protocol. This crate wraps its three swap-relevant endpoints:
//!
//! - `POST /quote` — price a market order
//! - `POST /order/market` — create a market order (returns the deposit address)
//! - `GET  /order/{orderId}` — read an order's current status

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

/// Errors surfaced by [`GatewayClient`].
#[derive(Debug, Snafu)]
pub enum GatewayError {
    #[snafu(display("invalid gateway base URL: {source}"))]
    InvalidBaseUrl { source: url::ParseError },

    #[snafu(display("gateway request transport error: {source}"))]
    Transport { source: reqwest::Error },

    #[snafu(display("gateway returned HTTP {status} ({code}): {message}"))]
    Api {
        status: u16,
        code: String,
        message: String,
    },

    #[snafu(display("could not decode gateway response: {source}"))]
    Decode { source: reqwest::Error },
}

pub type Result<T> = std::result::Result<T, GatewayError>;

/// Whether amounts are expressed in human-readable units or raw base units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AmountFormat {
    Readable,
    Raw,
}

/// Provider identifiers accepted by route-selection constraints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderId {
    Across,
    Cctp,
    Unit,
    HyperliquidBridge,
    Hyperliquid,
    Velora,
}

/// Optional route-selection constraints for `POST /quote`.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteRouting {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_sequence: Option<Vec<ProviderId>>,
}

/// Request body for `POST /quote`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteRequest {
    /// Source asset identifier, e.g. `Ethereum.USDC`.
    pub from: String,
    /// Destination asset identifier, e.g. `Base.USDC`.
    pub to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_format: Option<AmountFormat>,
    /// Recipient address on the destination chain.
    pub to_address: String,
    /// Source amount, interpreted per `amount_format`.
    pub from_amount: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing: Option<QuoteRouting>,
}

/// A single fee line item attached to a quote or order.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Fee {
    pub kind: String,
    pub label: String,
    pub asset: String,
    pub amount: String,
    pub amount_raw: String,
}

/// Response body for `POST /quote`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QuoteResponse {
    pub quote_id: String,
    pub order_type: String,
    pub from: String,
    pub to: String,
    pub expiry: String,
    pub estimated_out: String,
    #[serde(default)]
    pub fees: Option<Vec<Fee>>,
    pub amount_format: AmountFormat,
}

/// Request body for `POST /order/market`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateOrderRequest {
    pub quote_id: String,
    /// Source-chain address the deposit is sent from.
    pub from_address: String,
    /// Recipient address on the destination chain. Must match the quote.
    pub to_address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_address: Option<String>,
    pub idempotency_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub amount_format: Option<AmountFormat>,
}

/// Response body for `POST /order/market` and `GET /order/{orderId}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderResponse {
    pub order_id: String,
    /// Deposit vault address — send `amount_to_send` of the source asset here.
    pub order_address: String,
    pub amount_to_send: String,
    pub quote_id: String,
    pub order_type: String,
    pub from: String,
    pub to: String,
    pub status: String,
    pub expiry: String,
    pub estimated_out: String,
    #[serde(default)]
    pub fees: Option<Vec<Fee>>,
    pub amount_format: AmountFormat,
}

/// Async client for the router gateway.
#[derive(Debug, Clone)]
pub struct GatewayClient {
    http: reqwest::Client,
    base_url: String,
}

impl GatewayClient {
    /// Construct a client against `base_url` (e.g. the public gateway URL).
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        url::Url::parse(&base_url).context(InvalidBaseUrlSnafu)?;
        Ok(Self {
            http: reqwest::Client::new(),
            base_url,
        })
    }

    /// Price a market order.
    pub async fn quote(&self, request: &QuoteRequest) -> Result<QuoteResponse> {
        self.post("/quote", request).await
    }

    /// Create a market order. The response carries the deposit address.
    pub async fn create_market_order(&self, request: &CreateOrderRequest) -> Result<OrderResponse> {
        self.post("/order/market", request).await
    }

    /// Read an order's current status.
    pub async fn get_order(&self, order_id: &str) -> Result<OrderResponse> {
        let response = self
            .http
            .get(format!("{}/order/{order_id}", self.base_url))
            .send()
            .await
            .context(TransportSnafu)?;
        Self::decode(response).await
    }

    async fn post<B, R>(&self, path: &str, body: &B) -> Result<R>
    where
        B: Serialize,
        R: for<'de> Deserialize<'de>,
    {
        let response = self
            .http
            .post(format!("{}{path}", self.base_url))
            .json(body)
            .send()
            .await
            .context(TransportSnafu)?;
        Self::decode(response).await
    }

    async fn decode<R>(response: reqwest::Response) -> Result<R>
    where
        R: for<'de> Deserialize<'de>,
    {
        let status = response.status();
        if status.is_success() {
            return response.json::<R>().await.context(DecodeSnafu);
        }

        let status = status.as_u16();
        // Gateway error bodies are `{ "error": { code, message, details? } }`.
        let body = response
            .json::<serde_json::Value>()
            .await
            .unwrap_or(serde_json::Value::Null);
        let error = body.get("error");
        let code = error
            .and_then(|e| e.get("code"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        let message = error
            .and_then(|e| e.get("message"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("gateway request failed")
            .to_string();
        Err(GatewayError::Api {
            status,
            code,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_request_serializes_to_gateway_field_names() {
        let request = QuoteRequest {
            from: "Ethereum.USDC".to_string(),
            to: "Base.USDC".to_string(),
            amount_format: Some(AmountFormat::Readable),
            to_address: "0x1111111111111111111111111111111111111111".to_string(),
            from_amount: "100".to_string(),
            routing: None,
        };
        let json = serde_json::to_value(&request).expect("serialize");
        assert_eq!(json["from"], "Ethereum.USDC");
        assert_eq!(
            json["toAddress"],
            "0x1111111111111111111111111111111111111111"
        );
        assert_eq!(json["amountFormat"], "readable");
        assert_eq!(json["fromAmount"], "100");
        assert!(json.get("routing").is_none());
    }

    #[test]
    fn quote_request_serializes_provider_sequence_routing() {
        let request = QuoteRequest {
            from: "Arbitrum.USDT".to_string(),
            to: "Base.USDC".to_string(),
            amount_format: Some(AmountFormat::Readable),
            to_address: "0x1111111111111111111111111111111111111111".to_string(),
            from_amount: "5".to_string(),
            routing: Some(QuoteRouting {
                provider_sequence: Some(vec![ProviderId::Velora, ProviderId::Cctp]),
            }),
        };
        let json = serde_json::to_value(&request).expect("serialize");
        assert_eq!(
            json["routing"]["providerSequence"],
            serde_json::json!(["velora", "cctp"])
        );
    }

    #[test]
    fn create_order_request_omits_refund_address_when_none() {
        let request = CreateOrderRequest {
            quote_id: "q".to_string(),
            from_address: "a".to_string(),
            to_address: "b".to_string(),
            refund_address: None,
            idempotency_key: "router-cli-0000000000000000".to_string(),
            amount_format: Some(AmountFormat::Raw),
        };
        let json = serde_json::to_value(&request).expect("serialize");
        assert!(json.get("refundAddress").is_none());
        assert_eq!(json["amountFormat"], "raw");
    }
}
