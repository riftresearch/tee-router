use reqwest::{
    header::{HeaderMap, HeaderValue, USER_AGENT},
    Client, StatusCode,
};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::time::Duration;
use tracing::{debug, instrument};
use url::Url;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("failed to build reqwest client: {source}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("request failed: {source}"))]
    Request { source: reqwest::Error },

    #[snafu(display("unexpected HTTP status {status}: {body}"))]
    HttpStatus { status: StatusCode, body: String },

    #[snafu(display("response body exceeded {max_bytes} bytes"))]
    ResponseBodyTooLarge { max_bytes: usize },

    #[snafu(display("failed to decode response body as JSON: {source}. Body: {body}"))]
    Decode {
        source: serde_json::Error,
        body: String,
    },

    #[snafu(display("invalid Chainalysis host URL: {source}"))]
    InvalidHost { source: url::ParseError },

    #[snafu(display("unsupported Chainalysis host URL: {reason}"))]
    UnsupportedHost { reason: String },

    #[snafu(display("invalid Chainalysis host URL path base"))]
    InvalidPathBase,

    #[snafu(display("Chainalysis token must not be empty"))]
    EmptyToken,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
const CHAINALYSIS_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const CHAINALYSIS_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;

#[derive(Clone)]
pub struct ChainalysisAddressScreener {
    host: Url,
    token: String,
    http: Client,
}

impl ChainalysisAddressScreener {
    pub fn new(host: impl Into<String>, token: impl Into<String>) -> Result<Self> {
        let host = normalize_host_url(&host.into())?;
        let token = token.into().trim().to_string();
        if token.is_empty() {
            return Err(Error::EmptyToken);
        }

        let mut headers = HeaderMap::new();
        headers.insert(
            USER_AGENT,
            HeaderValue::from_static("chainalysis-address-screener/0.1"),
        );

        let http = Client::builder()
            .default_headers(headers)
            .use_rustls_tls()
            .timeout(CHAINALYSIS_HTTP_TIMEOUT)
            .build()
            .context(BuildClientSnafu)?;

        Ok(Self { host, token, http })
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn get_address_risk(&self, address: &str) -> Result<AddressRiskResponse> {
        let url = self.address_risk_url(address)?;
        debug!(%url, "fetching entity");

        let resp = self
            .http
            .get(url)
            .header("Token", &self.token)
            .send()
            .await
            .context(RequestSnafu)?;

        let status = resp.status();
        let bytes = read_limited_response_body(resp, CHAINALYSIS_MAX_RESPONSE_BODY_BYTES).await?;

        if !status.is_success() {
            let body = String::from_utf8_lossy(&bytes).to_string();
            return Err(Error::HttpStatus { status, body });
        }

        serde_json::from_slice::<AddressRiskResponse>(&bytes).map_err(|source| Error::Decode {
            source,
            body: String::from_utf8_lossy(&bytes).to_string(),
        })
    }

    fn address_risk_url(&self, address: &str) -> Result<Url> {
        let mut url = self.host.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|()| Error::InvalidPathBase)?;
        segments.pop_if_empty();
        segments.extend(["api", "risk", "v2", "entities", address]);
        drop(segments);
        Ok(url)
    }
}

fn normalize_host_url(host: &str) -> Result<Url> {
    let mut parsed = Url::parse(host.trim()).map_err(|source| Error::InvalidHost { source })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(Error::UnsupportedHost {
            reason: "expected http or https scheme".to_string(),
        });
    }
    if parsed.host().is_none() {
        return Err(Error::UnsupportedHost {
            reason: "expected host".to_string(),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::UnsupportedHost {
            reason: "credentials are not allowed".to_string(),
        });
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Error::UnsupportedHost {
            reason: "query strings and fragments are not allowed".to_string(),
        });
    }
    if !parsed.path().ends_with('/') {
        let mut path = parsed.path().to_string();
        path.push('/');
        parsed.set_path(&path);
    }
    Ok(parsed)
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context(RequestSnafu)? {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(Error::ResponseBodyTooLarge { max_bytes });
        }
    }
    Ok(body)
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddressRiskResponse {
    pub address: String,
    pub risk: RiskLevel,
    pub cluster: Option<Cluster>,
    pub risk_reason: Option<String>,
    pub address_type: AddressType,
    pub address_identifications: Vec<serde_json::Value>,
    pub exposures: Vec<Exposure>,
    pub triggers: Vec<serde_json::Value>,
    pub status: Status,
    pub pool_metadata: Option<PoolMetadata>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Severe,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AddressType {
    PrivateWallet,
    LiquidityPool,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum Status {
    Complete,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Cluster {
    pub name: Option<String>,
    pub category: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Exposure {
    pub category: String,
    pub value: f64,
    pub exposure_type: ExposureType,
    pub direction: ExposureDirection,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExposureType {
    Direct,
    Indirect,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExposureDirection {
    BothDirections,
    Inbound,
    Outbound,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PoolMetadata {
    pub pool_name: Option<String>,
    pub protocol: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deserializes_sample() {
        let body = r#"{
            "address":"0x0038AC785dfB6C82b2c9A7B3B6854e08a10cb9f1",
            "risk":"Low",
            "cluster":{"name":null,"category":"unnamed service"},
            "riskReason":null,
            "addressType":"PRIVATE_WALLET",
            "addressIdentifications":[],
            "exposures":[{"category":"exchange","value":419806.06569,"exposureType":"direct","direction":"both_directions"}],
            "triggers":[],
            "status":"COMPLETE"
        }"#;

        let parsed: AddressRiskResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.risk, RiskLevel::Low);
        assert_eq!(parsed.address_type, AddressType::PrivateWallet);
        assert_eq!(parsed.status, Status::Complete);
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
    fn constructor_rejects_non_canonical_hosts_and_empty_tokens() {
        for host in [
            "ftp://api.chainalysis.com",
            "https://user:pass@api.chainalysis.com",
            "https://api.chainalysis.com?token=secret",
            "https://api.chainalysis.com#fragment",
        ] {
            let error = match ChainalysisAddressScreener::new(host, "token") {
                Ok(_) => panic!("host must fail"),
                Err(error) => error,
            };
            assert!(matches!(
                error,
                Error::InvalidHost { .. } | Error::UnsupportedHost { .. }
            ));
        }

        assert!(matches!(
            ChainalysisAddressScreener::new("https://api.chainalysis.com", " "),
            Err(Error::EmptyToken)
        ));
    }

    #[test]
    fn address_risk_url_preserves_host_prefix_and_escapes_address_path_segment() {
        let screener =
            ChainalysisAddressScreener::new("https://api.chainalysis.com/screening", "token")
                .expect("client");
        let url = screener
            .address_risk_url("0xabc/def?x=1#frag")
            .expect("risk url");

        assert_eq!(
            url.as_str(),
            "https://api.chainalysis.com/screening/api/risk/v2/entities/0xabc%2Fdef%3Fx=1%23frag"
        );
    }

    #[tokio::test]
    async fn test_get_address_risk() {
        let host = std::env::var("CHAINALYSIS_HOST").unwrap_or_default();
        let token = std::env::var("CHAINALYSIS_TOKEN").unwrap_or_default();
        if host.is_empty() || token.is_empty() {
            println!("CHAINALYSIS_HOST or CHAINALYSIS_TOKEN is not set, skipping test");
            return;
        }
        let screener = ChainalysisAddressScreener::new(host, token).unwrap();
        let entity = screener
            .get_address_risk("0x0038AC785dfB6C82b2c9A7B3B6854e08a10cb9f1")
            .await
            .unwrap();
        println!("{:#?}", entity);
    }
}
