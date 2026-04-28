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

#[derive(Debug, Snafu)]
pub enum MarketPricingError {
    #[snafu(display("invalid URL {url}: {source}"))]
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },
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
            coinbase_api_base_url: parse_url(coinbase_api_base_url.as_ref())?,
            ethereum_rpc_url: parse_url(ethereum_rpc_url.as_ref())?,
            arbitrum_rpc_url: parse_url(arbitrum_rpc_url.as_ref())?,
            base_rpc_url: parse_url(base_rpc_url.as_ref())?,
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
        let eth_usd_micro = self.coinbase_spot_usd_micro("ETH-USD").await?;
        let btc_usd_micro = self.coinbase_spot_usd_micro("BTC-USD").await?;
        let stable_usd_micro = self.coinbase_spot_usd_micro("USDC-USD").await?;
        let ethereum_gas_price_wei = self
            .evm_gas_price_wei(&self.config.ethereum_rpc_url)
            .await?;
        let arbitrum_gas_price_wei = self
            .evm_gas_price_wei(&self.config.arbitrum_rpc_url)
            .await?;
        let base_gas_price_wei = self.evm_gas_price_wei(&self.config.base_rpc_url).await?;

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
        for attempt in 1..=PRICING_RETRY_ATTEMPTS {
            match self.coinbase_spot_usd_micro_once(product_id).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < PRICING_RETRY_ATTEMPTS && error.is_retryable() => {
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("pricing retry loop returns before exhausting attempts")
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
        parse_usd_micro(&response.data.amount)
    }

    async fn evm_gas_price_wei(&self, rpc_url: &Url) -> MarketPricingResult<u64> {
        let mut delay = PRICING_RETRY_INITIAL_DELAY;
        for attempt in 1..=PRICING_RETRY_ATTEMPTS {
            match self.evm_gas_price_wei_once(rpc_url).await {
                Ok(value) => return Ok(value),
                Err(error) if attempt < PRICING_RETRY_ATTEMPTS && error.is_retryable() => {
                    sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("pricing retry loop returns before exhausting attempts")
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
            .map_err(|source| MarketPricingError::Http { source })?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| MarketPricingError::Http { source })?;
        if !status.is_success() {
            return Err(MarketPricingError::HttpStatus {
                url: rpc_url.to_string(),
                status,
                body,
            });
        }
        let value: JsonRpcResponse =
            serde_json::from_str(&body).map_err(|source| MarketPricingError::InvalidJson {
                url: rpc_url.to_string(),
                source,
                body: body.clone(),
            })?;
        if let Some(error) = value.error {
            return Err(MarketPricingError::RpcError {
                url: rpc_url.to_string(),
                error: error.to_string(),
            });
        }
        let result = value
            .result
            .and_then(|value| value.as_str().map(str::to_string));
        let Some(result) = result else {
            return Err(MarketPricingError::InvalidRpc {
                url: rpc_url.to_string(),
                reason: "missing string result".to_string(),
                body,
            });
        };
        parse_hex_u64(&result).map_err(|reason| MarketPricingError::InvalidRpc {
            url: rpc_url.to_string(),
            reason,
            body,
        })
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
    let response = http
        .get(url.clone())
        .send()
        .await
        .map_err(|source| MarketPricingError::Http { source })?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|source| MarketPricingError::Http { source })?;
    if !status.is_success() {
        return Err(MarketPricingError::HttpStatus {
            url: url.to_string(),
            status,
            body,
        });
    }
    serde_json::from_str(&body).map_err(|source| MarketPricingError::InvalidJson {
        url: url.to_string(),
        source,
        body,
    })
}

fn parse_url(raw: &str) -> MarketPricingResult<Url> {
    raw.parse()
        .map_err(|source| MarketPricingError::InvalidUrl {
            url: raw.to_string(),
            source,
        })
}

fn join_url(base: &Url, path: &str) -> MarketPricingResult<Url> {
    base.join(path)
        .map_err(|source| MarketPricingError::UrlJoin {
            base_url: base.to_string(),
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
    Ok(whole
        .saturating_mul(USD_MICRO)
        .saturating_add(fractional_micro))
}

fn invalid_decimal(raw: &str, reason: &str) -> MarketPricingError {
    MarketPricingError::InvalidDecimal {
        raw: raw.to_string(),
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
    fn parse_hex_u64_decodes_rpc_hex() {
        assert_eq!(parse_hex_u64("0x77359400").unwrap(), 2_000_000_000);
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
