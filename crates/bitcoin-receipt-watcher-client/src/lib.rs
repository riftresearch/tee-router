use async_trait::async_trait;
use bitcoin::{BlockHash, Transaction, Txid};
use futures_util::StreamExt;
use reqwest::{Client, RequestBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::{
    str::FromStr,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::AUTHORIZATION, HeaderValue},
        Message,
    },
};

const BITCOIN_RECEIPT_WATCHER_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const BITCOIN_RECEIPT_WATCHER_MAX_RESPONSE_BODY_BYTES: usize = 8 * 1024 * 1024;
const BITCOIN_RECEIPT_WATCHER_MAX_ERROR_BODY_PREVIEW_BYTES: usize = 4 * 1024;

pub type Result<T> = std::result::Result<T, Error>;
pub type Confirmations = u64;
pub type BitcoinReceipt = (Transaction, Confirmations);

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

    #[snafu(display("Failed to parse response: {source:?} at {loc}"))]
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

    #[snafu(display("Invalid URL: {source:?}"))]
    InvalidUrl {
        source: url::ParseError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Unsupported URL: {reason}"))]
    UnsupportedUrl { reason: String },

    #[snafu(display("invalid transaction id {value:?}"))]
    InvalidTxid { value: String },

    #[snafu(display("Bitcoin receipt watcher websocket failed: {source}"))]
    WebSocket {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("Bitcoin receipt watcher websocket frame decode failed: {source}"))]
    WebSocketJson { source: serde_json::Error },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatchStatus {
    Pending,
    Confirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchRequest {
    pub chain: String,
    pub txid: Txid,
    pub requesting_operation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchResponse {
    pub chain: String,
    pub txid: Txid,
    pub requesting_operation_id: String,
    pub watched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchDeleteResponse {
    pub txid: Txid,
    pub watched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    pub tx: Transaction,
    pub block_hash: BlockHash,
    pub block_height: u64,
    pub confirmations: Confirmations,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchEvent {
    pub chain: String,
    pub txid: Txid,
    pub requesting_operation_id: String,
    pub status: WatchStatus,
    pub receipt: Option<Receipt>,
}

#[derive(Clone)]
pub struct BitcoinReceiptWatcherClient {
    http: Client,
    base_url: Url,
    subscribe_url: Url,
    chain: String,
    api_key: Option<String>,
}

impl BitcoinReceiptWatcherClient {
    pub fn new(base_url: impl AsRef<str>, chain: impl Into<String>) -> Result<Self> {
        Self::new_with_api_key(base_url, chain, None)
    }

    pub fn new_with_api_key(
        base_url: impl AsRef<str>,
        chain: impl Into<String>,
        api_key: Option<String>,
    ) -> Result<Self> {
        let http = Client::builder()
            .use_rustls_tls()
            .timeout(BITCOIN_RECEIPT_WATCHER_HTTP_TIMEOUT)
            .build()
            .context(BuildClientSnafu)?;
        let base_url = normalize_http_base_url(base_url.as_ref())?;
        let subscribe_url = subscribe_url_from_base(&base_url)?;
        let chain = chain.into();

        tracing::info!(
            base_url = %redacted_url_for_debug(&base_url),
            subscribe_url = %redacted_url_for_debug(&subscribe_url),
            chain,
            "Creating BitcoinReceiptWatcherClient"
        );

        Ok(Self {
            http,
            base_url,
            subscribe_url,
            chain,
            api_key: normalize_api_key(api_key),
        })
    }

    pub async fn watch(
        &self,
        txid: Txid,
        requesting_operation_id: impl Into<String>,
    ) -> Result<WatchResponse> {
        let url = self.base_url.join("watch").context(InvalidUrlSnafu)?;
        let response = self
            .send_request(
                "POST",
                "/watch",
                self.authorize(self.http.post(url)).json(&WatchRequest {
                    chain: self.chain.clone(),
                    txid,
                    requesting_operation_id: requesting_operation_id.into(),
                }),
            )
            .await
            .context(RequestSnafu)?;
        read_json_response(response).await
    }

    pub async fn cancel(&self, txid: Txid) -> Result<WatchDeleteResponse> {
        let mut url = self.base_url.join("watch").context(InvalidUrlSnafu)?;
        url.path_segments_mut()
            .map_err(|()| Error::UnsupportedUrl {
                reason: format!(
                    "base URL {} cannot be used as a path base",
                    redacted_url_for_debug(&self.base_url)
                ),
            })?
            .push(&txid.to_string());
        let response = self
            .send_request(
                "DELETE",
                "/watch/:txid",
                self.authorize(self.http.delete(url)),
            )
            .await
            .context(RequestSnafu)?;
        read_json_response(response).await
    }

    pub async fn subscribe(&self) -> Result<mpsc::Receiver<Result<WatchEvent>>> {
        let mut request = self
            .subscribe_url
            .as_str()
            .into_client_request()
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?;
        if let Some(api_key) = &self.api_key {
            let value = HeaderValue::from_str(&format!("Bearer {api_key}")).map_err(|source| {
                Error::WebSocket {
                    source: source.to_string(),
                }
            })?;
            request.headers_mut().insert(AUTHORIZATION, value);
        }

        let (socket, _) = connect_async(request)
            .await
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?;
        let (_, mut stream) = socket.split();

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                let event = match frame {
                    Ok(Message::Text(text)) => serde_json::from_str::<WatchEvent>(&text)
                        .map_err(|source| Error::WebSocketJson { source }),
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                };
                if tx.send(event).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
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

    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.api_key {
            Some(api_key) => request.bearer_auth(api_key),
            None => request,
        }
    }
}

#[async_trait]
pub trait ByIdLookup {
    type Id;
    type Receipt;

    async fn lookup_by_id(&self, id: Self::Id) -> Result<Option<Self::Receipt>>;
}

#[async_trait]
impl ByIdLookup for BitcoinReceiptWatcherClient {
    type Id = Txid;
    type Receipt = BitcoinReceipt;

    async fn lookup_by_id(&self, txid: Txid) -> Result<Option<Self::Receipt>> {
        let mut events = self.subscribe().await?;
        self.watch(txid, "by-id-lookup").await?;

        while let Some(event) = events.recv().await {
            let event = event?;
            if event.txid == txid && event.status == WatchStatus::Confirmed {
                if let Some(receipt) = event.receipt {
                    return Ok(Some((receipt.tx, receipt.confirmations)));
                }
            }
        }

        Ok(None)
    }
}

pub fn parse_txid(value: &str) -> Result<Txid> {
    Txid::from_str(value).map_err(|_| Error::InvalidTxid {
        value: value.to_string(),
    })
}

async fn read_json_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let bytes =
        read_limited_response_body(response, BITCOIN_RECEIPT_WATCHER_MAX_RESPONSE_BODY_BYTES)
            .await?;
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
    if bytes.len() <= BITCOIN_RECEIPT_WATCHER_MAX_ERROR_BODY_PREVIEW_BYTES {
        return String::from_utf8_lossy(bytes).to_string();
    }

    format!(
        "{}...<truncated {} bytes>",
        String::from_utf8_lossy(&bytes[..BITCOIN_RECEIPT_WATCHER_MAX_ERROR_BODY_PREVIEW_BYTES]),
        bytes.len() - BITCOIN_RECEIPT_WATCHER_MAX_ERROR_BODY_PREVIEW_BYTES
    )
}

fn normalize_http_base_url(base_url: &str) -> Result<Url> {
    let mut parsed = Url::parse(base_url.trim()).context(InvalidUrlSnafu)?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(Error::UnsupportedUrl {
            reason: "expected http or https scheme".to_string(),
        });
    }
    normalize_base_parts(&mut parsed)?;
    Ok(parsed)
}

fn subscribe_url_from_base(base: &Url) -> Result<Url> {
    let mut ws_base = base.clone();
    let scheme = match base.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => {
            return Err(Error::UnsupportedUrl {
                reason: format!("unsupported scheme {other}"),
            });
        }
    };
    ws_base
        .set_scheme(scheme)
        .map_err(|()| Error::UnsupportedUrl {
            reason: format!(
                "failed to set websocket scheme for {}",
                redacted_url_for_debug(base)
            ),
        })?;
    ws_base.join("subscribe").context(InvalidUrlSnafu)
}

fn normalize_base_parts(parsed: &mut Url) -> Result<()> {
    if parsed.host().is_none() {
        return Err(Error::UnsupportedUrl {
            reason: "expected host".to_string(),
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(Error::UnsupportedUrl {
            reason: "credentials are not allowed".to_string(),
        });
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(Error::UnsupportedUrl {
            reason: "query strings and fragments are not allowed".to_string(),
        });
    }
    if !parsed.path().ends_with('/') {
        let mut path = parsed.path().to_string();
        path.push('/');
        parsed.set_path(&path);
    }
    Ok(())
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
    redacted
}

fn normalize_api_key(api_key: Option<String>) -> Option<String> {
    api_key.and_then(|key| {
        let trimmed = key.trim().to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    })
}

fn record_upstream_request(
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    metrics::counter!(
        "tee_router_venue_requests_total",
        "venue" => "bitcoin_receipt_watcher",
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    metrics::histogram!(
        "tee_router_venue_request_duration_seconds",
        "venue" => "bitcoin_receipt_watcher",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn websocket_url_preserves_path_base() {
        let base = normalize_http_base_url("https://example.com/base/watcher").unwrap();
        let ws = subscribe_url_from_base(&base).unwrap();

        assert_eq!(ws.as_str(), "wss://example.com/base/watcher/subscribe");
    }

    #[test]
    fn parses_txid() {
        let txid =
            parse_txid("1111111111111111111111111111111111111111111111111111111111111111").unwrap();

        assert_eq!(
            txid.to_string(),
            "1111111111111111111111111111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn by_id_lookup_trait_uses_bitcoin_receipt_tuple() {
        fn assert_lookup<T: ByIdLookup<Id = Txid, Receipt = BitcoinReceipt>>() {}
        assert_lookup::<BitcoinReceiptWatcherClient>();
    }

    #[test]
    fn watch_event_decodes_pending_wire_message() {
        let event: WatchEvent = serde_json::from_str(
            r#"{
                "chain":"bitcoin",
                "txid":"2222222222222222222222222222222222222222222222222222222222222222",
                "requesting_operation_id":"operation-1",
                "status":"pending",
                "receipt":null
            }"#,
        )
        .unwrap();

        assert_eq!(event.chain, "bitcoin");
        assert_eq!(
            event.txid.to_string(),
            "2222222222222222222222222222222222222222222222222222222222222222"
        );
        assert_eq!(event.status, WatchStatus::Pending);
        assert!(event.receipt.is_none());
    }
}
