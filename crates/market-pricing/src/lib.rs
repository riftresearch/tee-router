use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use snafu::Snafu;
use std::time::Duration;
use tokio::time::sleep;
use url::Url;

pub type MarketPricingResult<T> = Result<T, MarketPricingError>;

pub const USD_MICRO: u64 = 1_000_000;
const PRICING_RETRY_ATTEMPTS: usize = 4;
const PRICING_RETRY_INITIAL_DELAY: Duration = Duration::from_millis(250);
const MARKET_PRICING_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;
const MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS: usize = 4 * 1024;

#[derive(Debug, Snafu)]
pub enum MarketPricingError {
    #[snafu(display("invalid URL {url}: {source}"))]
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },
    #[snafu(display("invalid URL {url}: {reason}"))]
    InvalidUrlFormat { url: String, reason: String },
    #[snafu(display("failed to join base URL {base_url} with path {path}: {source}"))]
    UrlJoin {
        base_url: String,
        path: String,
        source: url::ParseError,
    },
    #[snafu(display("HTTP request failed: {source}"))]
    Http { source: reqwest::Error },
    #[snafu(display("HTTP {status} from {url}: {body}"))]
    HttpStatus {
        url: String,
        status: StatusCode,
        body: String,
    },
    #[snafu(display("response body from {url} exceeded {max_bytes} bytes"))]
    ResponseBodyTooLarge { url: String, max_bytes: usize },
    #[snafu(display("invalid JSON body from {url}: {source}; body={body}"))]
    InvalidJson {
        url: String,
        source: serde_json::Error,
        body: String,
    },
    #[snafu(display("coinbase price response for {product_id} was not USD"))]
    UnexpectedCurrency { product_id: String },
    #[snafu(display("invalid decimal USD value {raw:?}: {reason}"))]
    InvalidDecimal { raw: String, reason: String },
    #[snafu(display("non-positive market price for {price_source}: {value}"))]
    NonPositivePrice { price_source: String, value: u64 },
    #[snafu(display("invalid RPC response from {url}: {reason}; body={body}"))]
    InvalidRpc {
        url: String,
        reason: String,
        body: String,
    },
    #[snafu(display("RPC {url} returned error: {error}"))]
    RpcError { url: String, error: String },
}

impl MarketPricingError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::Http { source } => source.is_timeout() || source.is_connect(),
            Self::HttpStatus { status, .. } => {
                *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
            }
            Self::RpcError { error, .. } | Self::InvalidRpc { reason: error, .. } => {
                let error = error.to_ascii_lowercase();
                error.contains("temporary")
                    || error.contains("retry")
                    || error.contains("timeout")
                    || error.contains("timed out")
                    || error.contains("rate limit")
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketPricingSnapshot {
    pub source: String,
    pub captured_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub stable_usd_micro: u64,
    pub eth_usd_micro: u64,
    pub btc_usd_micro: u64,
    pub ethereum_gas_price_wei: u64,
    pub arbitrum_gas_price_wei: u64,
    pub base_gas_price_wei: u64,
}

#[derive(Debug, Clone)]
pub struct MarketPricingOracleConfig {
    pub coinbase_api_base_url: Url,
    pub ethereum_rpc_url: Url,
    pub arbitrum_rpc_url: Url,
    pub base_rpc_url: Url,
    pub timeout: Duration,
}

impl MarketPricingOracleConfig {
    pub fn new(
        coinbase_api_base_url: impl AsRef<str>,
        ethereum_rpc_url: impl AsRef<str>,
        arbitrum_rpc_url: impl AsRef<str>,
        base_rpc_url: impl AsRef<str>,
    ) -> MarketPricingResult<Self> {
        Ok(Self {
            coinbase_api_base_url: parse_http_url(coinbase_api_base_url.as_ref())?,
            ethereum_rpc_url: parse_http_url(ethereum_rpc_url.as_ref())?,
            arbitrum_rpc_url: parse_http_url(arbitrum_rpc_url.as_ref())?,
            base_rpc_url: parse_http_url(base_rpc_url.as_ref())?,
            timeout: Duration::from_secs(10),
        })
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

#[derive(Clone)]
pub struct MarketPricingOracle {
    config: MarketPricingOracleConfig,
    http: reqwest::Client,
}

impl MarketPricingOracle {
    pub fn new(config: MarketPricingOracleConfig) -> MarketPricingResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(config.timeout)
            .user_agent("tee-router-market-pricing/1.0")
            .build()
            .map_err(|source| MarketPricingError::Http { source })?;
        Ok(Self { config, http })
    }

    pub async fn snapshot(&self) -> MarketPricingResult<MarketPricingSnapshot> {
        let captured_at = Utc::now();
        let (
            eth_usd_micro,
            btc_usd_micro,
            stable_usd_micro,
            ethereum_gas_price_wei,
            arbitrum_gas_price_wei,
            base_gas_price_wei,
        ) = tokio::try_join!(
            self.coinbase_spot_usd_micro("ETH-USD"),
            self.coinbase_spot_usd_micro("BTC-USD"),
            self.coinbase_spot_usd_micro("USDC-USD"),
            self.evm_gas_price_wei(&self.config.ethereum_rpc_url),
            self.evm_gas_price_wei(&self.config.arbitrum_rpc_url),
            self.evm_gas_price_wei(&self.config.base_rpc_url),
        )?;

        Ok(MarketPricingSnapshot {
            source: "coinbase_spot_plus_rpc_eth_gas_price_v1".to_string(),
            captured_at,
            expires_at: None,
            stable_usd_micro,
            eth_usd_micro,
            btc_usd_micro,
            ethereum_gas_price_wei,
            arbitrum_gas_price_wei,
            base_gas_price_wei,
        })
    }

    async fn coinbase_spot_usd_micro(&self, product_id: &str) -> MarketPricingResult<u64> {
        let mut delay = PRICING_RETRY_INITIAL_DELAY;
        let mut attempt = 1;
        loop {
            match self.coinbase_spot_usd_micro_once(product_id).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < PRICING_RETRY_ATTEMPTS && error.is_retryable() => {
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn coinbase_spot_usd_micro_once(&self, product_id: &str) -> MarketPricingResult<u64> {
        let path = format!("/v2/prices/{product_id}/spot");
        let url = join_url(&self.config.coinbase_api_base_url, &path)?;
        let response: CoinbaseSpotPriceResponse = get_json(&self.http, url.clone()).await?;
        if response.data.currency != "USD" {
            return Err(MarketPricingError::UnexpectedCurrency {
                product_id: product_id.to_string(),
            });
        }
        let value = parse_usd_micro(&response.data.amount)?;
        ensure_positive_price(&format!("coinbase {product_id}"), value)?;
        Ok(value)
    }

    async fn evm_gas_price_wei(&self, rpc_url: &Url) -> MarketPricingResult<u64> {
        let mut delay = PRICING_RETRY_INITIAL_DELAY;
        let mut attempt = 1;
        loop {
            match self.evm_gas_price_wei_once(rpc_url).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < PRICING_RETRY_ATTEMPTS && error.is_retryable() => {
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn evm_gas_price_wei_once(&self, rpc_url: &Url) -> MarketPricingResult<u64> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_gasPrice",
            "params": [],
        });
        let response = self
            .http
            .post(rpc_url.clone())
            .json(&body)
            .send()
            .await
            .map_err(|source| MarketPricingError::Http {
                source: source.without_url(),
            })?;
        let sanitized_rpc_url = sanitized_parsed_url_for_error(rpc_url);
        let status = response.status();
        let body =
            read_limited_response_text(response, MARKET_PRICING_MAX_RESPONSE_BODY_BYTES, rpc_url)
                .await?;
        if !status.is_success() {
            return Err(MarketPricingError::HttpStatus {
                url: sanitized_rpc_url,
                status,
                body: error_text_preview(&body),
            });
        }
        let value: JsonRpcResponse =
            serde_json::from_str(&body).map_err(|source| MarketPricingError::InvalidJson {
                url: sanitized_rpc_url.clone(),
                source,
                body: error_text_preview(&body),
            })?;
        if let Some(error) = value.error {
            return Err(MarketPricingError::RpcError {
                url: sanitized_rpc_url,
                error: error_text_preview(&error.to_string()),
            });
        }
        let result = value
            .result
            .and_then(|value| value.as_str().map(str::to_string));
        let Some(result) = result else {
            return Err(MarketPricingError::InvalidRpc {
                url: sanitized_rpc_url.clone(),
                reason: "missing string result".to_string(),
                body: error_text_preview(&body),
            });
        };
        let value = parse_hex_u64(&result).map_err(|reason| MarketPricingError::InvalidRpc {
            url: sanitized_rpc_url.clone(),
            reason,
            body: error_text_preview(&body),
        })?;
        ensure_positive_price(&format!("rpc gas price {sanitized_rpc_url}"), value)?;
        Ok(value)
    }
}

#[derive(Debug, Deserialize)]
struct CoinbaseSpotPriceResponse {
    data: CoinbaseSpotPriceData,
}

#[derive(Debug, Deserialize)]
struct CoinbaseSpotPriceData {
    amount: String,
    currency: String,
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    result: Option<Value>,
    error: Option<Value>,
}

async fn get_json<T>(http: &reqwest::Client, url: Url) -> MarketPricingResult<T>
where
    T: for<'de> Deserialize<'de>,
{
    let response =
        http.get(url.clone())
            .send()
            .await
            .map_err(|source| MarketPricingError::Http {
                source: source.without_url(),
            })?;
    let sanitized_url = sanitized_parsed_url_for_error(&url);
    let status = response.status();
    let body =
        read_limited_response_text(response, MARKET_PRICING_MAX_RESPONSE_BODY_BYTES, &url).await?;
    if !status.is_success() {
        return Err(MarketPricingError::HttpStatus {
            url: sanitized_url,
            status,
            body: error_text_preview(&body),
        });
    }
    serde_json::from_str(&body).map_err(|source| MarketPricingError::InvalidJson {
        url: sanitized_url,
        source,
        body: error_text_preview(&body),
    })
}

async fn read_limited_response_text(
    mut response: reqwest::Response,
    max_bytes: usize,
    url: &Url,
) -> MarketPricingResult<String> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|source| MarketPricingError::Http {
            source: source.without_url(),
        })?
    {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(MarketPricingError::ResponseBodyTooLarge {
                url: sanitized_parsed_url_for_error(url),
                max_bytes,
            });
        }
    }

    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn error_text_preview(value: &str) -> String {
    let mut end = 0;
    for (count, (index, ch)) in value.char_indices().enumerate() {
        if count == MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS {
            return format!(
                "{}...<truncated {} chars>",
                &value[..end],
                value[index..].chars().count()
            );
        }
        end = index + ch.len_utf8();
    }
    value.to_string()
}

fn parse_http_url(raw: &str) -> MarketPricingResult<Url> {
    let url: Url = raw
        .parse()
        .map_err(|source| MarketPricingError::InvalidUrl {
            url: sanitize_url_for_error(raw),
            source,
        })?;
    match url.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(MarketPricingError::InvalidUrlFormat {
                url: sanitize_url_for_error(raw),
                reason: format!("expected http or https scheme, got {scheme:?}"),
            });
        }
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(MarketPricingError::InvalidUrlFormat {
            url: sanitized_parsed_url_for_error(&url),
            reason: "URL credentials are not allowed".to_string(),
        });
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err(MarketPricingError::InvalidUrlFormat {
            url: sanitized_parsed_url_for_error(&url),
            reason: "query strings and fragments are not allowed".to_string(),
        });
    }
    Ok(url)
}

fn sanitize_url_for_error(raw: &str) -> String {
    match raw.parse::<Url>() {
        Ok(url) => sanitized_parsed_url_for_error(&url),
        Err(_) => sanitize_unparsed_url_for_error(raw),
    }
}

fn sanitized_parsed_url_for_error(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    let mut sanitized = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        sanitized.push(':');
        sanitized.push_str(&port.to_string());
    }
    if url.path() != "/" {
        sanitized.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        sanitized.push_str("?<redacted-query>");
    }
    if url.fragment().is_some() {
        sanitized.push_str("#<redacted-fragment>");
    }
    sanitized
}

fn sanitize_unparsed_url_for_error(raw: &str) -> String {
    let _ = raw;
    "<invalid url>".to_string()
}

fn join_url(base: &Url, path: &str) -> MarketPricingResult<Url> {
    base.join(path)
        .map_err(|source| MarketPricingError::UrlJoin {
            base_url: sanitized_parsed_url_for_error(base),
            path: path.to_string(),
            source,
        })
}

fn parse_hex_u64(raw: &str) -> Result<u64, String> {
    let trimmed = raw
        .strip_prefix("0x")
        .or_else(|| raw.strip_prefix("0X"))
        .unwrap_or(raw);
    u64::from_str_radix(trimmed, 16).map_err(|err| format!("invalid hex u64 {raw:?}: {err}"))
}

fn ensure_positive_price(source: &str, value: u64) -> MarketPricingResult<()> {
    if value == 0 {
        return Err(MarketPricingError::NonPositivePrice {
            price_source: source.to_string(),
            value,
        });
    }
    Ok(())
}

pub fn parse_usd_micro(raw: &str) -> MarketPricingResult<u64> {
    let value = raw.trim();
    if value.is_empty() || value.starts_with('-') {
        return Err(invalid_decimal(raw, "expected non-negative decimal"));
    }
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() || !whole.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(invalid_decimal(raw, "invalid whole component"));
    }
    if !fractional.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(invalid_decimal(raw, "invalid fractional component"));
    }
    let whole = whole
        .parse::<u64>()
        .map_err(|err| invalid_decimal(raw, &err.to_string()))?;
    let mut padded = fractional.chars().take(6).collect::<String>();
    while padded.len() < 6 {
        padded.push('0');
    }
    let mut fractional_micro = padded
        .parse::<u64>()
        .map_err(|err| invalid_decimal(raw, &err.to_string()))?;
    if fractional.chars().skip(6).any(|ch| ch != '0') {
        fractional_micro = fractional_micro.saturating_add(1);
    }
    whole
        .checked_mul(USD_MICRO)
        .and_then(|whole_micro| whole_micro.checked_add(fractional_micro))
        .ok_or_else(|| invalid_decimal(raw, "USD value overflowed u64 micro-units"))
}

fn invalid_decimal(raw: &str, reason: &str) -> MarketPricingError {
    MarketPricingError::InvalidDecimal {
        raw: error_text_preview(raw),
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn parse_usd_micro_handles_fractional_values() {
        assert_eq!(parse_usd_micro("2313.755").unwrap(), 2_313_755_000);
        assert_eq!(parse_usd_micro("1").unwrap(), 1_000_000);
        assert_eq!(parse_usd_micro("0.999845").unwrap(), 999_845);
    }

    #[test]
    fn parse_usd_micro_rounds_tiny_dust_up() {
        assert_eq!(parse_usd_micro("0.0000001").unwrap(), 1);
    }

    #[test]
    fn parse_usd_micro_rounding_can_carry_to_next_unit() {
        assert_eq!(parse_usd_micro("0.9999991").unwrap(), USD_MICRO);
    }

    #[test]
    fn parse_usd_micro_rejects_overflow() {
        let error = parse_usd_micro("18446744073710").unwrap_err();
        assert!(matches!(error, MarketPricingError::InvalidDecimal { .. }));
        assert!(
            error.to_string().contains("overflowed u64"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parse_hex_u64_decodes_rpc_hex() {
        assert_eq!(parse_hex_u64("0x77359400").unwrap(), 2_000_000_000);
    }

    #[test]
    fn oracle_config_rejects_non_http_and_credentialed_urls() {
        let error = MarketPricingOracleConfig::new(
            "file:///tmp/coinbase.json",
            "https://eth.example.com",
            "https://arb.example.com",
            "https://base.example.com",
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("expected http or https"),
            "unexpected error: {error}"
        );

        let error = MarketPricingOracleConfig::new(
            "https://api.coinbase.com",
            "https://user:pass@eth.example.com",
            "https://arb.example.com",
            "https://base.example.com",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("URL credentials are not allowed"),
            "unexpected error: {error}"
        );
        let rendered = error.to_string();
        assert!(!rendered.contains("user"), "leaked username: {rendered}");
        assert!(!rendered.contains("pass"), "leaked password: {rendered}");
    }

    #[test]
    fn oracle_config_rejects_query_and_fragment_urls() {
        let error = MarketPricingOracleConfig::new(
            "https://api.coinbase.com?token=secret",
            "https://eth.example.com",
            "https://arb.example.com",
            "https://base.example.com",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("query strings and fragments are not allowed"),
            "unexpected error: {error}"
        );
        let rendered = error.to_string();
        assert!(!rendered.contains("token"), "leaked query key: {rendered}");
        assert!(
            !rendered.contains("secret"),
            "leaked query value: {rendered}"
        );
    }

    #[test]
    fn url_error_sanitization_redacts_unparsed_secret_material() {
        let sanitized = sanitize_unparsed_url_for_error("not a URL with secret-token");

        assert_eq!(sanitized, "<invalid url>");
        assert!(
            !sanitized.contains("secret-token"),
            "leaked invalid URL material: {sanitized}"
        );
    }

    #[test]
    fn url_error_sanitization_redacts_parsed_path_query_and_fragment() {
        let sanitized = sanitize_url_for_error(
            "https://user:pass@example.com/path-secret?token=query-secret#fragment-secret",
        );

        assert_eq!(
            sanitized,
            "https://example.com/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!sanitized.contains("user"), "leaked username: {sanitized}");
        assert!(!sanitized.contains("pass"), "leaked password: {sanitized}");
        assert!(
            !sanitized.contains("path-secret"),
            "leaked path material: {sanitized}"
        );
        assert!(
            !sanitized.contains("query-secret"),
            "leaked query value: {sanitized}"
        );
        assert!(
            !sanitized.contains("fragment-secret"),
            "leaked fragment value: {sanitized}"
        );
    }

    #[test]
    fn market_prices_must_be_positive() {
        let error = ensure_positive_price("coinbase ETH-USD", 0).unwrap_err();
        assert!(matches!(error, MarketPricingError::NonPositivePrice { .. }));
        ensure_positive_price("coinbase ETH-USD", 1).unwrap();
    }

    #[test]
    fn append_limited_body_chunk_rejects_chunks_past_the_limit_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn append_limited_body_chunk_accepts_chunks_at_the_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn error_text_preview_truncates_large_values() {
        let value = "a".repeat(MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS + 7);

        let preview = error_text_preview(&value);

        assert_eq!(
            preview.len(),
            MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS + "...<truncated 7 chars>".len()
        );
        assert!(preview.ends_with("...<truncated 7 chars>"));
    }

    #[test]
    fn error_text_preview_does_not_split_utf8_codepoints() {
        let value = format!(
            "{}{}",
            "a".repeat(MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS - 1),
            "é".repeat(3)
        );

        let preview = error_text_preview(&value);

        assert!(preview.starts_with(&format!(
            "{}é",
            "a".repeat(MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS - 1)
        )));
        assert!(preview.ends_with("...<truncated 2 chars>"));
    }

    #[test]
    fn invalid_decimal_error_truncates_untrusted_raw_value() {
        let raw = "9".repeat(MARKET_PRICING_MAX_ERROR_PREVIEW_CHARS + 1);

        let error = parse_usd_micro(&raw).unwrap_err();
        let rendered = error.to_string();

        assert!(rendered.contains("<truncated 1 chars>"));
        assert!(!rendered.contains(&raw));
    }

    #[tokio::test]
    #[ignore = "live Coinbase/RPC pricing test; run with LIVE_PROVIDER_TESTS=1"]
    async fn live_snapshot_fetches_coinbase_and_rpc_pricing() {
        if env::var("LIVE_PROVIDER_TESTS").as_deref() != Ok("1") {
            eprintln!("skipping live pricing test; set LIVE_PROVIDER_TESTS=1 to enable");
            return;
        }
        let ethereum_rpc_url = env::var("ETH_RPC_URL").expect("ETH_RPC_URL");
        let config = MarketPricingOracleConfig::new(
            "https://api.coinbase.com",
            ethereum_rpc_url,
            env::var("ARBITRUM_RPC_URL").expect("ARBITRUM_RPC_URL"),
            env::var("BASE_RPC_URL").expect("BASE_RPC_URL"),
        )
        .unwrap();
        let snapshot = MarketPricingOracle::new(config)
            .unwrap()
            .snapshot()
            .await
            .unwrap();
        eprintln!("LIVE_MARKET_PRICING_SNAPSHOT {snapshot:?}");
        assert!(snapshot.eth_usd_micro > 0);
        assert!(snapshot.btc_usd_micro > 0);
        assert!(snapshot.stable_usd_micro > 0);
        assert!(snapshot.ethereum_gas_price_wei > 0);
        assert!(snapshot.arbitrum_gas_price_wei > 0);
        assert!(snapshot.base_gas_price_wei > 0);
    }
}
