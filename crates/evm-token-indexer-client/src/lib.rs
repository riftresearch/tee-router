use alloy::primitives::{Address, B256, U256};
use reqwest::{Client, RequestBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::time::{Duration, Instant};

const TOKEN_INDEXER_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const TOKEN_INDEXER_MAX_RESPONSE_BODY_BYTES: usize = 4 * 1024 * 1024;
const TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES: usize = 4 * 1024;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build HTTP client: {source:?}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("Failed to send request: {source:?} at {loc}"))]
    Request {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Failed to parse response: {source:?}"))]
    ParseResponse {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("HTTP {status}: {body}"))]
    HttpStatus { status: StatusCode, body: String },

    #[snafu(display("response body exceeded {max_bytes} bytes"))]
    ResponseBodyTooLarge { max_bytes: usize },

    #[snafu(display("failed to decode JSON response: {source}; body={body}"))]
    DecodeJson {
        source: serde_json::Error,
        body: String,
    },

    #[snafu(display("Invalid base URL: {source:?}"))]
    InvalidUrl {
        source: url::ParseError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Unsupported base URL: {reason}"))]
    UnsupportedBaseUrl { reason: String },

    #[snafu(display("Invalid base URL: {base_url} cannot be used as a path base"))]
    InvalidPathBase { base_url: String },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferEvent {
    pub id: String,
    pub amount: String, // BigInt serialized as string
    pub timestamp: u64,
    pub from: Address,
    pub to: Address,
    pub token_address: Option<Address>,
    pub transaction_hash: B256,
    pub block_number: String, // BigInt serialized as string
    pub block_hash: B256,
    pub log_index: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransfersResponse {
    pub transfers: Vec<TransferEvent>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIndexerWatch {
    pub watch_id: String,
    pub watch_target: String,
    pub token_address: Address,
    pub deposit_address: Address,
    pub min_amount: String,
    pub max_amount: String,
    pub required_amount: String,
    pub created_at: String,
    pub updated_at: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncWatchesRequest {
    pub watches: Vec<TokenIndexerWatch>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncWatchesResponse {
    pub synced: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositCandidate {
    pub id: String,
    pub watch_id: String,
    pub watch_target: String,
    pub chain_id: u64,
    pub token_address: Address,
    pub from_address: Address,
    pub deposit_address: Address,
    pub amount: String,
    pub required_amount: String,
    pub transaction_hash: B256,
    pub transfer_index: u64,
    pub block_number: String,
    pub block_hash: B256,
    pub block_timestamp: String,
    pub status: String,
    pub attempt_count: u64,
    pub last_error: Option<String>,
    pub created_at: String,
    pub delivered_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingCandidatesResponse {
    pub candidates: Vec<DepositCandidate>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseCandidateRequest {
    error: Option<String>,
}

pub struct TokenIndexerClient {
    client: Client,
    base_url: Url,
    api_key: Option<String>,
}

impl TokenIndexerClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::new_with_api_key(base_url, None)
    }

    pub fn new_with_api_key(base_url: impl AsRef<str>, api_key: Option<String>) -> Result<Self> {
        let client = Client::builder()
            .use_rustls_tls()
            .timeout(TOKEN_INDEXER_HTTP_TIMEOUT)
            .build()
            .context(BuildClientSnafu)?;

        let base_url = normalize_base_url(base_url.as_ref())?;

        tracing::info!(
            base_url = %redacted_url_for_debug(&base_url),
            "Creating TokenIndexerClient"
        );

        Ok(Self {
            client,
            base_url,
            api_key: api_key.and_then(|key| {
                let trimmed = key.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            }),
        })
    }

    pub async fn get_transfers_to(
        &self,
        address: Address,
        token: Option<Address>,
        min_amount: Option<U256>,
        limit: Option<u32>,
    ) -> Result<TransfersResponse> {
        let mut url = self
            .base_url
            .join(&format!("transfers/to/{address:?}"))
            .context(InvalidUrlSnafu)?;

        {
            let mut query_pairs = url.query_pairs_mut();

            if let Some(token) = token {
                query_pairs.append_pair("token", &format!("{token:?}"));
            }

            if let Some(amount) = min_amount {
                query_pairs.append_pair("amount", &amount.to_string());
            }

            if let Some(limit) = limit {
                query_pairs.append_pair("limit", &limit.to_string());
            }
        }

        let response = self
            .send_request(
                "GET",
                "/transfers/to/:address",
                self.authorize(self.client.get(url)),
            )
            .await
            .context(RequestSnafu)?;
        read_json_response(response).await
    }

    pub async fn sync_watches(
        &self,
        watches: Vec<TokenIndexerWatch>,
    ) -> Result<SyncWatchesResponse> {
        let url = self.base_url.join("watches").context(InvalidUrlSnafu)?;
        let response = self
            .send_request(
                "PUT",
                "/watches",
                self.authorize(self.client.put(url))
                    .json(&SyncWatchesRequest { watches }),
            )
            .await
            .context(RequestSnafu)?;
        read_json_response(response).await
    }

    pub async fn materialize_candidates(&self) -> Result<()> {
        let url = self
            .base_url
            .join("candidates/materialize")
            .context(InvalidUrlSnafu)?;
        self.send_request(
            "POST",
            "/candidates/materialize",
            self.authorize(self.client.post(url)),
        )
        .await
        .context(RequestSnafu)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn pending_candidates(&self, limit: Option<u32>) -> Result<Vec<DepositCandidate>> {
        let mut url = self
            .base_url
            .join("candidates/pending")
            .context(InvalidUrlSnafu)?;
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        let response = self
            .send_request(
                "GET",
                "/candidates/pending",
                self.authorize(self.client.get(url)),
            )
            .await
            .context(RequestSnafu)?;
        let response = read_json_response::<PendingCandidatesResponse>(response).await?;
        Ok(response.candidates)
    }

    pub async fn mark_candidate_submitted(&self, candidate_id: &str) -> Result<()> {
        let url = self.candidate_action_url(candidate_id, "mark-submitted")?;
        self.send_request(
            "POST",
            "/candidates/:candidate_id/mark-submitted",
            self.authorize(self.client.post(url)),
        )
        .await
        .context(RequestSnafu)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn release_candidate(&self, candidate_id: &str, error: Option<String>) -> Result<()> {
        let url = self.candidate_action_url(candidate_id, "release")?;
        self.send_request(
            "POST",
            "/candidates/:candidate_id/release",
            self.authorize(self.client.post(url))
                .json(&ReleaseCandidateRequest { error }),
        )
        .await
        .context(RequestSnafu)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn discard_candidate(&self, candidate_id: &str, error: Option<String>) -> Result<()> {
        let url = self.candidate_action_url(candidate_id, "discard")?;
        self.send_request(
            "POST",
            "/candidates/:candidate_id/discard",
            self.authorize(self.client.post(url))
                .json(&ReleaseCandidateRequest { error }),
        )
        .await
        .context(RequestSnafu)?
        .error_for_status()
        .map_err(reqwest::Error::without_url)
        .context(RequestSnafu)?;
        Ok(())
    }

    fn candidate_action_url(&self, candidate_id: &str, action: &str) -> Result<Url> {
        let mut url = self.base_url.join("candidates").context(InvalidUrlSnafu)?;
        url.path_segments_mut()
            .map_err(|()| Error::InvalidPathBase {
                base_url: redacted_url_for_debug(&self.base_url),
            })?
            .push(candidate_id)
            .push(action);
        Ok(url)
    }

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        }
    }

    async fn send_request(
        &self,
        method: &'static str,
        endpoint: &'static str,
        request: RequestBuilder,
    ) -> std::result::Result<reqwest::Response, reqwest::Error> {
        let started = Instant::now();
        match request.send().await.map_err(reqwest::Error::without_url) {
            Ok(response) => {
                record_upstream_request(
                    method,
                    endpoint,
                    status_class(response.status()),
                    started.elapsed(),
                );
                Ok(response)
            }
            Err(error) => {
                record_upstream_request(method, endpoint, "transport_error", started.elapsed());
                Err(error)
            }
        }
    }
}

fn record_upstream_request(
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    metrics::counter!(
        "tee_router_venue_requests_total",
        "venue" => "evm_token_indexer",
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    metrics::histogram!(
        "tee_router_venue_request_duration_seconds",
        "venue" => "evm_token_indexer",
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .record(duration.as_secs_f64());
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

fn normalize_base_url(base_url: &str) -> Result<Url> {
    let mut parsed = Url::parse(base_url.trim()).context(InvalidUrlSnafu)?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(Error::UnsupportedBaseUrl {
            reason: "expected http or https scheme".to_string(),
        });
    }
    if parsed.host().is_none() {
        return Err(Error::UnsupportedBaseUrl {
            reason: "expected host".to_string(),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::UnsupportedBaseUrl {
            reason: "credentials are not allowed".to_string(),
        });
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Error::UnsupportedBaseUrl {
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

fn redacted_url_for_debug(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if url.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if url.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

async fn read_json_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let bytes = read_limited_response_body(response, TOKEN_INDEXER_MAX_RESPONSE_BODY_BYTES).await?;
    if !status.is_success() {
        return Err(Error::HttpStatus {
            status,
            body: response_body_preview(&bytes),
        });
    }
    serde_json::from_slice(&bytes).map_err(|source| Error::DecodeJson {
        source,
        body: response_body_preview(&bytes),
    })
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(reqwest::Error::without_url)
        .context(ParseResponseSnafu)?
    {
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

fn response_body_preview(bytes: &[u8]) -> String {
    if bytes.len() <= TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES {
        return String::from_utf8_lossy(bytes).to_string();
    }

    format!(
        "{}...<truncated {} bytes>",
        String::from_utf8_lossy(&bytes[..TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES]),
        bytes.len() - TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn test_client_creation() {
        let client = TokenIndexerClient::new("http://localhost:3000");
        assert!(client.is_ok());
    }

    #[test]
    fn client_rejects_non_canonical_base_urls() {
        for base_url in [
            "ftp://localhost:3000",
            "http://user:pass@localhost:3000",
            "http://localhost:3000?token=secret",
            "http://localhost:3000#fragment",
        ] {
            let error = match TokenIndexerClient::new(base_url) {
                Ok(_) => panic!("base URL must fail"),
                Err(error) => error,
            };
            assert!(matches!(error, Error::UnsupportedBaseUrl { .. }));
        }
    }

    #[test]
    fn test_path_building() {
        let client = TokenIndexerClient::new("http://localhost:3000/erc20-indexer").unwrap();

        let url = client.base_url.join("candidates/pending").unwrap();
        assert_eq!(
            url.to_string(),
            "http://localhost:3000/erc20-indexer/candidates/pending"
        );
    }

    #[test]
    fn base_url_debug_redacts_path_material() {
        let url = normalize_base_url("http://localhost:3000/erc20-indexer-secret").unwrap();

        let redacted = redacted_url_for_debug(&url);

        assert_eq!(redacted, "http://localhost:3000/<redacted-path>");
        assert!(!redacted.contains("erc20-indexer-secret"));
    }

    #[test]
    fn candidate_action_path_escapes_candidate_id() {
        let client = TokenIndexerClient::new("http://localhost:3000/erc20-indexer/").unwrap();

        let url = client
            .candidate_action_url("watch:chain:tx:0", "mark-submitted")
            .unwrap();

        assert_eq!(
            url.to_string(),
            "http://localhost:3000/erc20-indexer/candidates/watch:chain:tx:0/mark-submitted"
        );
    }

    #[test]
    fn authorized_requests_include_bearer_token() {
        let client = TokenIndexerClient::new_with_api_key(
            "http://localhost:3000",
            Some("token-indexer-api-key-000000000000".to_string()),
        )
        .unwrap();
        let url = client.base_url.join("candidates/pending").unwrap();

        let request = client.authorize(client.client.get(url)).build().unwrap();

        assert_eq!(
            request
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer token-indexer-api-key-000000000000")
        );
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
    fn response_body_preview_truncates_large_error_bodies() {
        let body = vec![b'a'; TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES + 7];

        let preview = response_body_preview(&body);

        assert_eq!(
            preview.len(),
            TOKEN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES + "...<truncated 7 bytes>".len()
        );
        assert!(preview.ends_with("...<truncated 7 bytes>"));
    }

    #[tokio::test]
    async fn request_errors_do_not_render_base_url_path_material() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client =
            TokenIndexerClient::new(format!("http://127.0.0.1:{port}/secret-indexer-path/"))
                .unwrap();

        let error = client.pending_candidates(None).await.unwrap_err();
        let rendered = format!("{error:?}");

        assert!(!rendered.contains("secret-indexer-path"));
        assert!(!rendered.contains("candidates/pending"));
    }
}
