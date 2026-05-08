use alloy::primitives::{Address, U256};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::Snafu;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use url::Url;

use super::http_body::{read_limited_response_text, response_body_error_preview};
use crate::telemetry;

pub type AcrossResult<T> = Result<T, AcrossClientError>;
const ACROSS_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const ACROSS_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;

#[derive(Debug, Snafu)]
pub enum AcrossClientError {
    #[snafu(display("failed to join base URL {base_url} with path {path}: {source}"))]
    InvalidUrl {
        base_url: String,
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("invalid Across base URL {base_url}: {source}"))]
    InvalidBaseUrl {
        base_url: String,
        source: url::ParseError,
    },
    #[snafu(display("HTTP request to Across failed: {source}"))]
    Http { source: reqwest::Error },
    #[snafu(display("Across returned HTTP {status}: {body}"))]
    HttpStatus { status: u16, body: String },
    #[snafu(display("Across response body exceeded {max_bytes} bytes"))]
    ResponseBodyTooLarge { max_bytes: usize },
    #[snafu(display("Across returned an invalid JSON body: {source}; body={body}"))]
    InvalidBody {
        source: serde_json::Error,
        body: String,
    },
    #[snafu(display("Across numeric field {field} could not be parsed: {raw}"))]
    InvalidNumeric { field: &'static str, raw: String },
    #[snafu(display("Across HTTP client configuration failed: {message}"))]
    ClientConfiguration { message: String },
}

impl From<reqwest::Error> for AcrossClientError {
    fn from(source: reqwest::Error) -> Self {
        Self::Http {
            source: source.without_url(),
        }
    }
}

/// Query parameters for Across' `GET /swap/approval` endpoint.
///
/// Mirrors the public Across HTTP API. Any change here must also be reflected
/// in the mock server in `crates/devnet/src/mock_integrators.rs` so the same
/// deserialization path is exercised in tests.
#[derive(Debug, Clone)]
pub struct AcrossSwapApprovalRequest {
    pub trade_type: String,
    pub origin_chain_id: u64,
    pub destination_chain_id: u64,
    pub input_token: Address,
    pub output_token: Address,
    pub amount: U256,
    pub depositor: Address,
    pub recipient: String,
    pub refund_address: Address,
    pub refund_on_origin: bool,
    pub strict_trade_type: bool,
    pub integrator_id: String,
    pub slippage: Option<String>,
    pub extra_query: BTreeMap<String, String>,
}

impl AcrossSwapApprovalRequest {
    #[must_use]
    pub fn into_query(self) -> Vec<(String, String)> {
        let mut query = vec![
            ("tradeType".to_string(), self.trade_type),
            (
                "originChainId".to_string(),
                self.origin_chain_id.to_string(),
            ),
            (
                "destinationChainId".to_string(),
                self.destination_chain_id.to_string(),
            ),
            ("inputToken".to_string(), self.input_token.to_string()),
            ("outputToken".to_string(), self.output_token.to_string()),
            ("amount".to_string(), self.amount.to_string()),
            ("depositor".to_string(), self.depositor.to_string()),
            ("recipient".to_string(), self.recipient),
            ("refundAddress".to_string(), self.refund_address.to_string()),
            (
                "refundOnOrigin".to_string(),
                self.refund_on_origin.to_string(),
            ),
            (
                "strictTradeType".to_string(),
                self.strict_trade_type.to_string(),
            ),
            ("integratorId".to_string(), self.integrator_id),
        ];
        if let Some(slippage) = self.slippage {
            query.push(("slippage".to_string(), slippage));
        }
        query.extend(self.extra_query);
        query
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AcrossSwapApprovalResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_txns: Option<Vec<AcrossTransaction>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_tx: Option<AcrossTransaction>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_input_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_output_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_output_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote_expiry_timestamp: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AcrossTransaction {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chain_id: Option<u64>,
    pub to: String,
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gas: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_fee_per_gas: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_priority_fee_per_gas: Option<Value>,
}

/// Query parameters for Across' `GET /deposit/status` endpoint.
///
/// Mirrors the public Across HTTP API. Any change here must also be reflected
/// in the mock server in `crates/devnet/src/mock_integrators.rs` so the same
/// deserialization path is exercised in tests.
#[derive(Debug, Clone)]
pub struct AcrossDepositStatusRequest {
    pub origin_chain_id: u64,
    pub deposit_id: U256,
}

impl AcrossDepositStatusRequest {
    #[must_use]
    pub fn into_query(self) -> Vec<(String, String)> {
        vec![
            (
                "originChainId".to_string(),
                self.origin_chain_id.to_string(),
            ),
            ("depositId".to_string(), self.deposit_id.to_string()),
        ]
    }
}

/// Response body from Across' `GET /deposit/status` endpoint.
///
/// Fields are all optional because Across returns a sparse object that only
/// populates values once they are observed on-chain; only `status` is always
/// present. The `status` field is kept as a raw string so the router can evolve
/// independently of Across' status vocabulary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AcrossDepositStatusResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_chain_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_chain_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deposit_id: Option<String>,
    #[serde(
        default,
        rename = "depositTxnRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub deposit_tx_hash: Option<String>,
    #[serde(
        default,
        rename = "fillTxnRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub fill_tx: Option<String>,
    #[serde(
        default,
        rename = "depositRefundTxnRef",
        skip_serializing_if = "Option::is_none"
    )]
    pub deposit_refund_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fill_deadline: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Value>,
}

/// Parse a numeric Across field that may be emitted as either a JSON number or a
/// quoted decimal string.
pub fn parse_optional_u256(
    field: &'static str,
    value: &Option<Value>,
) -> AcrossResult<Option<U256>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => {
            let raw = number.to_string();
            parse_u256(field, &raw).map(Some)
        }
        Value::String(string) => {
            if string.is_empty() {
                Ok(None)
            } else {
                parse_u256(field, string).map(Some)
            }
        }
        other => Err(AcrossClientError::InvalidNumeric {
            field,
            raw: response_body_error_preview(&other.to_string()),
        }),
    }
}

fn parse_u256(field: &'static str, raw: &str) -> AcrossResult<U256> {
    let trimmed = raw.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        return U256::from_str_radix(hex, 16).map_err(|_| AcrossClientError::InvalidNumeric {
            field,
            raw: response_body_error_preview(raw),
        });
    }
    U256::from_str_radix(trimmed, 10).map_err(|_| AcrossClientError::InvalidNumeric {
        field,
        raw: response_body_error_preview(raw),
    })
}

#[derive(Clone)]
pub struct AcrossClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl AcrossClient {
    pub fn new(base_url: impl Into<String>, api_key: impl Into<String>) -> AcrossResult<Self> {
        let api_key = api_key.into().trim().to_string();
        if api_key.is_empty() {
            return Err(AcrossClientError::ClientConfiguration {
                message: "Across API key must not be empty".to_string(),
            });
        }
        let base_url = normalize_base_url(base_url)?;
        Ok(Self {
            http: rustls_http_client()?,
            base_url,
            api_key,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn swap_approval(
        &self,
        request: AcrossSwapApprovalRequest,
    ) -> AcrossResult<AcrossSwapApprovalResponse> {
        self.get_json("/swap/approval", request.into_query()).await
    }

    pub async fn deposit_status(
        &self,
        request: AcrossDepositStatusRequest,
    ) -> AcrossResult<AcrossDepositStatusResponse> {
        self.get_json("/deposit/status", request.into_query()).await
    }

    pub async fn deposit_status_by_tx_ref(
        &self,
        deposit_txn_ref: impl Into<String>,
    ) -> AcrossResult<AcrossDepositStatusResponse> {
        self.get_json(
            "/deposit/status",
            vec![("depositTxnRef".to_string(), deposit_txn_ref.into())],
        )
        .await
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: Vec<(String, String)>,
    ) -> AcrossResult<T> {
        let endpoint_label = across_endpoint_label(path);
        let endpoint = build_endpoint(&self.base_url, path)?;
        let builder = self
            .http
            .get(endpoint)
            .query(&query)
            .bearer_auth(&self.api_key);
        let started = Instant::now();
        let response = match builder.send().await {
            Ok(response) => response,
            Err(err) => {
                telemetry::record_venue_transport_error(
                    "across",
                    "GET",
                    endpoint_label,
                    started.elapsed(),
                );
                return Err(err.into());
            }
        };
        let status = response.status();
        let body = read_limited_response_text(response, ACROSS_MAX_RESPONSE_BODY_BYTES).await?;
        telemetry::record_venue_http_status(
            "across",
            "GET",
            endpoint_label,
            status.as_u16(),
            started.elapsed(),
        );
        if body.truncated {
            return Err(AcrossClientError::ResponseBodyTooLarge {
                max_bytes: ACROSS_MAX_RESPONSE_BODY_BYTES,
            });
        }
        let body = body.text;
        if !status.is_success() {
            return Err(AcrossClientError::HttpStatus {
                status: status.as_u16(),
                body: response_body_error_preview(&body),
            });
        }
        serde_json::from_str(&body).map_err(|source| AcrossClientError::InvalidBody {
            source,
            body: response_body_error_preview(&body),
        })
    }
}

fn across_endpoint_label(path: &str) -> &'static str {
    match path {
        "/swap/approval" => "/swap/approval",
        "/deposit/status" => "/deposit/status",
        _ => "unknown",
    }
}

fn rustls_http_client() -> AcrossResult<reqwest::Client> {
    reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(ACROSS_HTTP_TIMEOUT)
        .build()
        .map_err(|err| AcrossClientError::ClientConfiguration {
            message: err.to_string(),
        })
}

fn normalize_base_url(base_url: impl Into<String>) -> AcrossResult<String> {
    let base_url = base_url.into().trim().trim_end_matches('/').to_string();
    let sanitized_base_url = sanitize_url_for_error(&base_url);
    let parsed = Url::parse(&base_url).map_err(|source| AcrossClientError::InvalidBaseUrl {
        base_url: sanitized_base_url.clone(),
        source,
    })?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(AcrossClientError::ClientConfiguration {
                message: format!(
                    "Across base URL {sanitized_base_url:?} must use http or https, got {scheme:?}"
                ),
            });
        }
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AcrossClientError::ClientConfiguration {
            message: format!("Across base URL {sanitized_base_url:?} must not include credentials"),
        });
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(AcrossClientError::ClientConfiguration {
            message: format!(
                "Across base URL {sanitized_base_url:?} must not include a query string or fragment"
            ),
        });
    }
    Ok(base_url)
}

fn sanitize_url_for_error(value: &str) -> String {
    let Ok(parsed) = Url::parse(value.trim()) else {
        return "<invalid url>".to_string();
    };

    let host = parsed.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", parsed.scheme(), host);
    if let Some(port) = parsed.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if parsed.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if parsed.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if parsed.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

fn build_endpoint(base_url: &str, path: &str) -> AcrossResult<Url> {
    Url::parse(&format!("{base_url}{path}")).map_err(|source| AcrossClientError::InvalidUrl {
        base_url: sanitize_url_for_error(base_url),
        path: path.to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use serde_json::json;

    fn example_request() -> AcrossSwapApprovalRequest {
        AcrossSwapApprovalRequest {
            trade_type: "exactInput".to_string(),
            origin_chain_id: 1,
            destination_chain_id: 8453,
            input_token: address!("A0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
            output_token: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            amount: U256::from(1_000_000_u64),
            depositor: address!("1111111111111111111111111111111111111111"),
            recipient: "0x2222222222222222222222222222222222222222".to_string(),
            refund_address: address!("3333333333333333333333333333333333333333"),
            refund_on_origin: true,
            strict_trade_type: true,
            integrator_id: "rift-router".to_string(),
            slippage: Some("0.01".to_string()),
            extra_query: BTreeMap::from([("foo".to_string(), "bar".to_string())]),
        }
    }

    #[test]
    fn request_into_query_serializes_all_fields_in_camel_case() {
        let query = example_request().into_query();
        let map: BTreeMap<_, _> = query.into_iter().collect();

        assert_eq!(map["tradeType"], "exactInput");
        assert_eq!(map["originChainId"], "1");
        assert_eq!(map["destinationChainId"], "8453");
        assert_eq!(
            map["inputToken"].to_lowercase(),
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        );
        assert_eq!(
            map["outputToken"].to_lowercase(),
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
        assert_eq!(map["amount"], "1000000");
        assert_eq!(
            map["depositor"].to_lowercase(),
            "0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            map["recipient"],
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(
            map["refundAddress"].to_lowercase(),
            "0x3333333333333333333333333333333333333333"
        );
        assert_eq!(map["refundOnOrigin"], "true");
        assert_eq!(map["strictTradeType"], "true");
        assert_eq!(map["integratorId"], "rift-router");
        assert_eq!(map["slippage"], "0.01");
        assert_eq!(map["foo"], "bar");
    }

    #[test]
    fn request_into_query_omits_slippage_when_absent() {
        let mut request = example_request();
        request.slippage = None;
        let query = request.into_query();
        assert!(
            query.iter().all(|(key, _)| key != "slippage"),
            "slippage must be omitted when None"
        );
    }

    #[test]
    fn response_deserializes_real_shape_fixture() {
        // Sample payload derived from Across' public docs for /swap/approval.
        // Any drift from real Across means this fixture (and the mock server)
        // must be updated in lockstep.
        let body = json!({
            "swapTx": {
                "chainId": 1,
                "to": "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5",
                "data": "0xabcdef",
                "value": "1000000",
                "gas": 250000,
                "maxFeePerGas": "40000000000",
                "maxPriorityFeePerGas": "1500000000"
            },
            "approvalTxns": [
                {
                    "chainId": 1,
                    "to": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                    "data": "0x095ea7b3"
                }
            ],
            "inputAmount": "1000000",
            "maxInputAmount": "1010000",
            "expectedOutputAmount": "995000",
            "minOutputAmount": "990000",
            "quoteExpiryTimestamp": 1_700_000_000,
            "id": "quote-abc"
        });

        let response: AcrossSwapApprovalResponse = serde_json::from_value(body.clone()).unwrap();

        assert_eq!(response.input_amount.as_deref(), Some("1000000"));
        assert_eq!(response.max_input_amount.as_deref(), Some("1010000"));
        assert_eq!(response.expected_output_amount.as_deref(), Some("995000"));
        assert_eq!(response.min_output_amount.as_deref(), Some("990000"));
        assert_eq!(response.quote_expiry_timestamp, Some(1_700_000_000));
        assert_eq!(response.id.as_deref(), Some("quote-abc"));

        let swap_tx = response.swap_tx.as_ref().expect("swapTx present");
        assert_eq!(swap_tx.chain_id, Some(1));
        assert_eq!(swap_tx.to, "0x5c7BCd6E7De5423a257D81B442095A1a6ced35C5");
        assert_eq!(swap_tx.data, "0xabcdef");
        assert_eq!(
            parse_optional_u256("value", &swap_tx.value).unwrap(),
            Some(U256::from(1_000_000_u64))
        );
        assert_eq!(
            parse_optional_u256("gas", &swap_tx.gas).unwrap(),
            Some(U256::from(250_000_u64))
        );
        assert_eq!(
            parse_optional_u256("maxFeePerGas", &swap_tx.max_fee_per_gas).unwrap(),
            Some(U256::from(40_000_000_000_u128))
        );
        assert_eq!(
            parse_optional_u256("maxPriorityFeePerGas", &swap_tx.max_priority_fee_per_gas).unwrap(),
            Some(U256::from(1_500_000_000_u64))
        );

        let approvals = response.approval_txns.as_deref().unwrap();
        assert_eq!(approvals.len(), 1);
        assert_eq!(approvals[0].chain_id, Some(1));
        assert_eq!(
            approvals[0].to,
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        );
    }

    #[test]
    fn response_deserializes_minimal_shape() {
        let body = json!({});
        let response: AcrossSwapApprovalResponse = serde_json::from_value(body).unwrap();
        assert!(response.swap_tx.is_none());
        assert!(response.approval_txns.is_none());
        assert!(response.input_amount.is_none());
        assert!(response.id.is_none());
    }

    #[test]
    fn response_roundtrips_through_serde() {
        let original = AcrossSwapApprovalResponse {
            approval_txns: Some(vec![AcrossTransaction {
                chain_id: Some(8453),
                to: "0x09aea4b2242a2b47fc63a3fbcb1f0ab7c5e0a03a".to_string(),
                data: "0x01".to_string(),
                value: Some(Value::String("0".to_string())),
                gas: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
            }]),
            swap_tx: Some(AcrossTransaction {
                chain_id: Some(1),
                to: "0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5".to_string(),
                data: "0xdeadbeef".to_string(),
                value: Some(Value::String("42".to_string())),
                gas: Some(json!(200_000)),
                max_fee_per_gas: Some(Value::String("100000000000".to_string())),
                max_priority_fee_per_gas: Some(Value::String("2000000000".to_string())),
            }),
            input_amount: Some("100".to_string()),
            max_input_amount: Some("110".to_string()),
            expected_output_amount: Some("99".to_string()),
            min_output_amount: Some("95".to_string()),
            quote_expiry_timestamp: Some(1_700_000_123),
            id: Some("quote-xyz".to_string()),
        };
        let json = serde_json::to_value(&original).unwrap();
        // Verify the serialized object has camelCase keys.
        assert!(json.get("swapTx").is_some());
        assert!(json.get("approvalTxns").is_some());
        assert!(json.get("maxInputAmount").is_some());
        assert!(json.get("expectedOutputAmount").is_some());
        assert!(json.get("minOutputAmount").is_some());
        assert!(json.get("quoteExpiryTimestamp").is_some());

        let swap_tx = json.get("swapTx").unwrap();
        assert!(swap_tx.get("chainId").is_some());
        assert!(swap_tx.get("maxFeePerGas").is_some());

        let round_trip: AcrossSwapApprovalResponse = serde_json::from_value(json).unwrap();
        assert_eq!(original, round_trip);
    }

    #[test]
    fn parse_optional_u256_accepts_numbers_and_strings() {
        assert_eq!(
            parse_optional_u256("v", &Some(json!(1234))).unwrap(),
            Some(U256::from(1234_u64))
        );
        assert_eq!(
            parse_optional_u256("v", &Some(json!("1234"))).unwrap(),
            Some(U256::from(1234_u64))
        );
        assert_eq!(
            parse_optional_u256("v", &Some(json!("0x10"))).unwrap(),
            Some(U256::from(16_u64))
        );
        assert_eq!(parse_optional_u256("v", &Some(Value::Null)).unwrap(), None);
        assert_eq!(parse_optional_u256("v", &None).unwrap(), None);
        assert_eq!(parse_optional_u256("v", &Some(json!(""))).unwrap(), None);
    }

    #[test]
    fn deposit_status_request_serializes_query() {
        let request = AcrossDepositStatusRequest {
            origin_chain_id: 8453,
            deposit_id: U256::from(1337_u64),
        };
        let query: BTreeMap<_, _> = request.into_query().into_iter().collect();
        assert_eq!(query["originChainId"], "8453");
        assert_eq!(query["depositId"], "1337");
    }

    #[test]
    fn deposit_status_response_deserializes_filled_fixture() {
        // Sample payload derived from Across' public docs for /deposit/status.
        let body = json!({
            "status": "filled",
            "originChainId": 1,
            "destinationChainId": 10,
            "depositId": "42",
            "depositTxnRef": "0xaaaa",
            "fillTxnRef": "0xbbbb",
            "fillDeadline": 1_700_000_500,
            "pagination": { "cursor": null }
        });
        let response: AcrossDepositStatusResponse = serde_json::from_value(body).unwrap();
        assert_eq!(response.status, "filled");
        assert_eq!(response.origin_chain_id, Some(1));
        assert_eq!(response.destination_chain_id, Some(10));
        assert_eq!(response.deposit_id.as_deref(), Some("42"));
        assert_eq!(response.deposit_tx_hash.as_deref(), Some("0xaaaa"));
        assert_eq!(response.fill_tx.as_deref(), Some("0xbbbb"));
        assert!(response.deposit_refund_tx_hash.is_none());
        assert_eq!(response.fill_deadline, Some(1_700_000_500));
    }

    #[test]
    fn deposit_status_response_deserializes_minimal_pending_shape() {
        let body = json!({ "status": "pending" });
        let response: AcrossDepositStatusResponse = serde_json::from_value(body).unwrap();
        assert_eq!(response.status, "pending");
        assert!(response.origin_chain_id.is_none());
        assert!(response.fill_tx.is_none());
        assert!(response.deposit_refund_tx_hash.is_none());
    }

    #[test]
    fn parse_optional_u256_rejects_non_numeric() {
        let err = parse_optional_u256("value", &Some(json!("abc"))).unwrap_err();
        match err {
            AcrossClientError::InvalidNumeric { field, raw } => {
                assert_eq!(field, "value");
                assert_eq!(raw, "abc");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn client_rejects_unsupported_base_urls() {
        assert!(matches!(
            AcrossClient::new("file:///tmp/across", "key"),
            Err(AcrossClientError::ClientConfiguration { .. })
        ));

        assert!(matches!(
            AcrossClient::new("https://app.across.to?api_key=oops", "key"),
            Err(AcrossClientError::ClientConfiguration { .. })
        ));

        let credentialed =
            match AcrossClient::new("https://user:pass@app.across.to?token=secret", "key") {
                Ok(_) => panic!("credentialed URL must fail"),
                Err(error) => error,
            };
        let rendered = credentialed.to_string();
        assert!(!rendered.contains("user"));
        assert!(!rendered.contains("pass"));
        assert!(!rendered.contains("token"));
        assert!(!rendered.contains("secret"));
    }

    #[test]
    fn across_url_errors_redact_path_query_fragment_and_invalid_values() {
        let sanitized = sanitize_url_for_error(
            "https://user:pass@app.across.to/provider-path-secret?token=query-secret#fragment-secret",
        );

        assert_eq!(
            sanitized,
            "https://app.across.to/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!sanitized.contains("user"));
        assert!(!sanitized.contains("pass"));
        assert!(!sanitized.contains("provider-path-secret"));
        assert!(!sanitized.contains("query-secret"));
        assert!(!sanitized.contains("fragment-secret"));

        let invalid = sanitize_url_for_error("not a url with secret-token");
        assert_eq!(invalid, "<invalid url>");
        assert!(!invalid.contains("secret-token"));
    }
}
