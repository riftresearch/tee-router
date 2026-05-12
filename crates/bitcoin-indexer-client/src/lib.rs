use async_trait::async_trait;
use bitcoin::{address::NetworkUnchecked, Address, BlockHash, Txid};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, RequestBuilder, StatusCode, Url};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::AUTHORIZATION, HeaderValue},
        Message,
    },
};

const BITCOIN_INDEXER_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const BITCOIN_INDEXER_MAX_RESPONSE_BODY_BYTES: usize = 4 * 1024 * 1024;
const BITCOIN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES: usize = 4 * 1024;

pub type Result<T> = std::result::Result<T, Error>;
pub type BitcoinAddress = Address<NetworkUnchecked>;
pub type BitcoinOutputFilter = (Vec<BitcoinAddress>, Option<u64>);

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

    #[snafu(display("Bitcoin indexer websocket failed: {source}"))]
    WebSocket {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("Bitcoin indexer websocket frame decode failed: {source}"))]
    WebSocketJson { source: serde_json::Error },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxOutput {
    pub txid: Txid,
    pub vout: u32,
    pub address: BitcoinAddress,
    pub amount_sats: u64,
    pub block_height: Option<u64>,
    pub block_hash: Option<BlockHash>,
    pub block_time: Option<u64>,
    pub confirmations: u64,
    #[serde(default)]
    pub removed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOutputPage {
    pub outputs: Vec<TxOutput>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOutputQuery {
    pub address: BitcoinAddress,
    pub from_block: Option<u64>,
    pub min_amount: Option<u64>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl TxOutputQuery {
    pub fn new(address: BitcoinAddress) -> Self {
        Self {
            address,
            from_block: None,
            min_amount: None,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    pub addresses: Vec<BitcoinAddress>,
    pub min_amount: Option<u64>,
}

impl From<BitcoinOutputFilter> for SubscribeFilter {
    fn from(filter: BitcoinOutputFilter) -> Self {
        let (addresses, min_amount) = filter;
        Self {
            addresses,
            min_amount,
        }
    }
}

#[derive(Clone)]
pub struct BitcoinIndexerClient {
    http: Client,
    base_url: Url,
    subscribe_url: Url,
    api_key: Option<String>,
}

impl BitcoinIndexerClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::new_with_api_key(base_url, None)
    }

    pub fn new_with_api_key(base_url: impl AsRef<str>, api_key: Option<String>) -> Result<Self> {
        let http = Client::builder()
            .use_rustls_tls()
            .timeout(BITCOIN_INDEXER_HTTP_TIMEOUT)
            .build()
            .context(BuildClientSnafu)?;
        let base_url = normalize_http_base_url(base_url.as_ref())?;
        let subscribe_url = subscribe_url_from_base(&base_url)?;

        tracing::info!(
            base_url = %redacted_url_for_debug(&base_url),
            subscribe_url = %redacted_url_for_debug(&subscribe_url),
            "Creating BitcoinIndexerClient"
        );

        Ok(Self {
            http,
            base_url,
            subscribe_url,
            api_key: normalize_api_key(api_key),
        })
    }

    pub async fn tx_outputs(&self, query: TxOutputQuery) -> Result<TxOutputPage> {
        let mut url = self.base_url.join("tx_outputs").context(InvalidUrlSnafu)?;
        append_tx_output_query(&mut url, &query);
        let response = self
            .send_request("GET", "/tx_outputs", self.authorize(self.http.get(url)))
            .await
            .context(RequestSnafu)?;
        read_tx_output_page_response(response).await
    }

    pub async fn subscribe(
        &self,
        filter: SubscribeFilter,
    ) -> Result<mpsc::Receiver<Result<TxOutput>>> {
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

        let (mut socket, _) = connect_async(request)
            .await
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?;
        socket
            .send(Message::Text(
                serde_json::to_string(&ClientMessage::Subscribe {
                    filter: WireSubscribeFilter::from(filter),
                })
                .map_err(|source| Error::WebSocketJson { source })?,
            ))
            .await
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?;

        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(frame) = socket.next().await {
                let event = match frame {
                    Ok(Message::Text(text)) => match decode_server_message(&text) {
                        Ok(Some(event)) => Ok(event),
                        Ok(None) => continue,
                        Err(error) => Err(error),
                    },
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

#[async_trait]
pub trait FilterStream {
    type Filter;
    type Event;

    async fn stream_filter(
        &self,
        filter: Self::Filter,
    ) -> Result<mpsc::Receiver<Result<Self::Event>>>;
}

#[async_trait]
impl FilterStream for BitcoinIndexerClient {
    type Filter = BitcoinOutputFilter;
    type Event = TxOutput;

    async fn stream_filter(
        &self,
        filter: Self::Filter,
    ) -> Result<mpsc::Receiver<Result<Self::Event>>> {
        self.subscribe(filter.into()).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTxOutputPage {
    outputs: Vec<TxOutput>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct WireSubscribeFilter {
    addresses: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_amount: Option<u64>,
}

impl From<SubscribeFilter> for WireSubscribeFilter {
    fn from(value: SubscribeFilter) -> Self {
        Self {
            addresses: value.addresses.iter().map(address_to_wire_string).collect(),
            min_amount: value.min_amount,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { filter: WireSubscribeFilter },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed {
        #[serde(rename = "subscription_id")]
        _subscription_id: Option<String>,
    },
    TxOutput {
        event: TxOutput,
    },
    Error {
        error: String,
    },
}

fn decode_server_message(text: &str) -> Result<Option<TxOutput>> {
    match serde_json::from_str::<ServerMessage>(text)
        .map_err(|source| Error::WebSocketJson { source })?
    {
        ServerMessage::TxOutput { event } => Ok(Some(event)),
        ServerMessage::Error { error } => Err(Error::WebSocket { source: error }),
        ServerMessage::Subscribed { .. } => Ok(None),
    }
}

fn append_tx_output_query(url: &mut Url, query: &TxOutputQuery) {
    let mut pairs = url.query_pairs_mut();
    pairs.append_pair("address", &address_to_wire_string(&query.address));
    if let Some(from_block) = query.from_block {
        pairs.append_pair("from_block", &from_block.to_string());
    }
    if let Some(min_amount) = query.min_amount {
        pairs.append_pair("min_amount", &min_amount.to_string());
    }
    if let Some(limit) = query.limit {
        pairs.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = &query.cursor {
        pairs.append_pair("cursor", cursor);
    }
}

fn address_to_wire_string(address: &BitcoinAddress) -> String {
    address.assume_checked_ref().to_string()
}

async fn read_tx_output_page_response(response: reqwest::Response) -> Result<TxOutputPage> {
    let page: WireTxOutputPage = read_json_response(response).await?;
    Ok(TxOutputPage {
        outputs: page.outputs,
        next_cursor: page.next_cursor,
        has_more: page.has_more,
    })
}

async fn read_json_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let bytes =
        read_limited_response_body(response, BITCOIN_INDEXER_MAX_RESPONSE_BODY_BYTES).await?;
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
    if bytes.len() <= BITCOIN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES {
        return String::from_utf8_lossy(bytes).to_string();
    }

    format!(
        "{}...<truncated {} bytes>",
        String::from_utf8_lossy(&bytes[..BITCOIN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES]),
        bytes.len() - BITCOIN_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES
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
        "venue" => "bitcoin_indexer",
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    metrics::histogram!(
        "tee_router_venue_request_duration_seconds",
        "venue" => "bitcoin_indexer",
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

    fn regtest_address() -> BitcoinAddress {
        "bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl"
            .parse()
            .unwrap()
    }

    #[test]
    fn websocket_url_preserves_path_base() {
        let base = normalize_http_base_url("https://example.com/base/indexer").unwrap();
        let ws = subscribe_url_from_base(&base).unwrap();

        assert_eq!(ws.as_str(), "wss://example.com/base/indexer/subscribe");
    }

    #[test]
    fn query_appends_bitcoin_filters() {
        let mut url = Url::parse("http://localhost/tx_outputs").unwrap();
        append_tx_output_query(
            &mut url,
            &TxOutputQuery {
                address: regtest_address(),
                from_block: Some(101),
                min_amount: Some(50_000),
                limit: Some(25),
                cursor: Some("cursor-1".to_string()),
            },
        );

        assert!(url
            .as_str()
            .contains("address=bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl"));
        assert!(url.as_str().contains("from_block=101"));
        assert!(url.as_str().contains("min_amount=50000"));
        assert!(url.as_str().contains("limit=25"));
        assert!(url.as_str().contains("cursor=cursor-1"));
    }

    #[test]
    fn filter_stream_trait_uses_bitcoin_output_types() {
        fn assert_stream<T: FilterStream<Filter = BitcoinOutputFilter, Event = TxOutput>>() {}
        assert_stream::<BitcoinIndexerClient>();
    }

    #[test]
    fn decodes_tx_output_wire_message() {
        let event = decode_server_message(
            r#"{
                "kind":"tx_output",
                "event":{
                    "txid":"1111111111111111111111111111111111111111111111111111111111111111",
                    "vout":0,
                    "address":"bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl",
                    "amount_sats":50000,
                    "block_height":102,
                    "block_hash":"2222222222222222222222222222222222222222222222222222222222222222",
                    "block_time":1710000000,
                    "confirmations":1,
                    "removed":false
                }
            }"#,
        )
        .unwrap()
        .unwrap();

        assert_eq!(event.vout, 0);
        assert_eq!(event.amount_sats, 50_000);
        assert_eq!(event.block_height, Some(102));
        assert!(!event.removed);
    }
}
