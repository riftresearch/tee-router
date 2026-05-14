use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
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

const EVM_INDEXER_HTTP_TIMEOUT: Duration = Duration::from_secs(10);
const EVM_INDEXER_MAX_RESPONSE_BODY_BYTES: usize = 4 * 1024 * 1024;
const EVM_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES: usize = 4 * 1024;

pub type Result<T> = std::result::Result<T, Error>;

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

    #[snafu(display("Invalid URL: {source:?}"))]
    InvalidUrl {
        source: url::ParseError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Unsupported URL: {reason}"))]
    UnsupportedUrl { reason: String },

    #[snafu(display("invalid decimal {field}: {value}"))]
    InvalidDecimal { field: &'static str, value: String },

    #[snafu(display("invalid integer {field}: {value}"))]
    InvalidInteger { field: &'static str, value: String },

    #[snafu(display("EVM indexer websocket failed: {source}"))]
    WebSocket {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("EVM indexer websocket frame decode failed: {source}"))]
    WebSocketJson { source: serde_json::Error },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmTransfer {
    pub id: String,
    pub chain_id: u64,
    pub token_address: Address,
    pub from_address: Address,
    pub to_address: Address,
    pub amount: U256,
    pub transaction_hash: B256,
    pub block_number: u64,
    pub block_hash: B256,
    pub log_index: u64,
    pub block_timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvmLog {
    pub id: String,
    pub chain_id: u64,
    pub block_number: u64,
    pub log_index: u64,
    pub tx_hash: B256,
    pub address: Address,
    pub topic0: B256,
    pub topic1: Option<B256>,
    pub topic2: Option<B256>,
    pub topic3: Option<B256>,
    pub data: String,
    pub block_timestamp: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferPage {
    pub transfers: Vec<EvmTransfer>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogPage {
    pub logs: Vec<EvmLog>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferQuery {
    pub to: Address,
    pub token: Option<Address>,
    pub from_block: Option<u64>,
    pub min_amount: Option<U256>,
    pub max_amount: Option<U256>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl TransferQuery {
    pub fn new(to: Address) -> Self {
        Self {
            to,
            token: None,
            from_block: None,
            min_amount: None,
            max_amount: None,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogQuery {
    pub address: Address,
    pub topic0: B256,
    pub topic1: Option<B256>,
    pub topic2: Option<B256>,
    pub topic3: Option<B256>,
    pub from_block: Option<u64>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

impl LogQuery {
    pub fn new(address: Address, topic0: B256) -> Self {
        Self {
            address,
            topic0,
            topic1: None,
            topic2: None,
            topic3: None,
            from_block: None,
            limit: None,
            cursor: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    pub token_addresses: Vec<Address>,
    pub recipient_addresses: Vec<Address>,
    pub min_amount: Option<U256>,
    pub max_amount: Option<U256>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSubscribeFilter {
    pub addresses: Vec<Address>,
    pub topic0: B256,
    pub topic1: Option<B256>,
    pub topic2: Option<B256>,
    pub topic3: Option<B256>,
}

pub type EvmTransferFilter = (Vec<Address>, Vec<Address>, Option<U256>, Option<U256>);

impl From<EvmTransferFilter> for SubscribeFilter {
    fn from(filter: EvmTransferFilter) -> Self {
        let (token_addresses, recipient_addresses, min_amount, max_amount) = filter;
        Self {
            token_addresses,
            recipient_addresses,
            min_amount,
            max_amount,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PruneRequest {
    pub relevant_addresses: Vec<Address>,
    pub before_block: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PruneResponse {
    pub accepted: bool,
    pub before_block: String,
    pub relevant_address_count: u64,
    pub reorg_safe_blocks: String,
}

#[derive(Clone)]
pub struct EvmIndexerClient {
    http: Client,
    base_url: Url,
    subscribe_url: Url,
    api_key: Option<String>,
}

impl EvmIndexerClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        Self::new_with_api_key(base_url, None)
    }

    pub fn new_with_api_key(base_url: impl AsRef<str>, api_key: Option<String>) -> Result<Self> {
        let http = Client::builder()
            .use_rustls_tls()
            .timeout(EVM_INDEXER_HTTP_TIMEOUT)
            .build()
            .context(BuildClientSnafu)?;
        let base_url = normalize_http_base_url(base_url.as_ref())?;
        let subscribe_url = subscribe_url_from_base(&base_url)?;

        tracing::info!(
            base_url = %redacted_url_for_debug(&base_url),
            subscribe_url = %redacted_url_for_debug(&subscribe_url),
            "Creating EvmIndexerClient"
        );

        Ok(Self {
            http,
            base_url,
            subscribe_url,
            api_key: api_key.and_then(|key| {
                let trimmed = key.trim().to_string();
                (!trimmed.is_empty()).then_some(trimmed)
            }),
        })
    }

    pub async fn transfers(&self, query: TransferQuery) -> Result<TransferPage> {
        let mut url = self.base_url.join("transfers").context(InvalidUrlSnafu)?;
        append_transfer_query(&mut url, &query);
        let response = self
            .send_request("GET", "/transfers", self.authorize(self.http.get(url)))
            .await
            .context(RequestSnafu)?;
        read_transfer_page_response(response).await
    }

    pub async fn logs(&self, query: LogQuery) -> Result<LogPage> {
        let mut url = self.base_url.join("logs").context(InvalidUrlSnafu)?;
        append_log_query(&mut url, &query);
        let response = self
            .send_request("GET", "/logs", self.authorize(self.http.get(url)))
            .await
            .context(RequestSnafu)?;
        read_log_page_response(response).await
    }

    pub async fn prune(&self, request: PruneRequest) -> Result<PruneResponse> {
        let url = self.base_url.join("prune").context(InvalidUrlSnafu)?;
        let response = self
            .send_request(
                "POST",
                "/prune",
                self.authorize(self.http.post(url))
                    .json(&WirePruneRequest::from(request)),
            )
            .await
            .context(RequestSnafu)?;
        read_json_response(response).await
    }

    pub async fn subscribe(
        &self,
        filter: SubscribeFilter,
    ) -> Result<mpsc::Receiver<Result<EvmTransfer>>> {
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
                    filter: WireClientSubscribeFilter::Transfer(WireSubscribeFilter::from(filter)),
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

    pub async fn subscribe_logs(
        &self,
        filter: LogSubscribeFilter,
    ) -> Result<mpsc::Receiver<Result<EvmLog>>> {
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
                    filter: WireClientSubscribeFilter::Log(WireLogSubscribeFilter::from(filter)),
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
                    Ok(Message::Text(text)) => match decode_log_server_message(&text) {
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
impl FilterStream for EvmIndexerClient {
    type Filter = EvmTransferFilter;
    type Event = EvmTransfer;

    async fn stream_filter(
        &self,
        filter: Self::Filter,
    ) -> Result<mpsc::Receiver<Result<Self::Event>>> {
        self.subscribe(filter.into()).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireTransferPage {
    transfers: Vec<WireEvmTransfer>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireLogPage {
    logs: Vec<WireEvmLog>,
    next_cursor: Option<String>,
    has_more: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEvmTransfer {
    id: String,
    chain_id: u64,
    token_address: Address,
    from_address: Address,
    to_address: Address,
    amount: String,
    transaction_hash: B256,
    block_number: String,
    block_hash: B256,
    log_index: u64,
    block_timestamp: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEvmLog {
    id: String,
    chain_id: u64,
    block_number: String,
    log_index: u64,
    tx_hash: B256,
    address: Address,
    topic0: B256,
    topic1: Option<B256>,
    topic2: Option<B256>,
    topic3: Option<B256>,
    data: String,
    block_timestamp: String,
}

impl TryFrom<WireEvmTransfer> for EvmTransfer {
    type Error = Error;

    fn try_from(value: WireEvmTransfer) -> Result<Self> {
        Ok(Self {
            id: value.id,
            chain_id: value.chain_id,
            token_address: value.token_address,
            from_address: value.from_address,
            to_address: value.to_address,
            amount: parse_u256_decimal(&value.amount, "amount")?,
            transaction_hash: value.transaction_hash,
            block_number: parse_u64_decimal(&value.block_number, "block_number")?,
            block_hash: value.block_hash,
            log_index: value.log_index,
            block_timestamp: parse_u64_decimal(&value.block_timestamp, "block_timestamp")?,
        })
    }
}

impl TryFrom<WireEvmLog> for EvmLog {
    type Error = Error;

    fn try_from(value: WireEvmLog) -> Result<Self> {
        Ok(Self {
            id: value.id,
            chain_id: value.chain_id,
            block_number: parse_u64_decimal(&value.block_number, "block_number")?,
            log_index: value.log_index,
            tx_hash: value.tx_hash,
            address: value.address,
            topic0: value.topic0,
            topic1: value.topic1,
            topic2: value.topic2,
            topic3: value.topic3,
            data: value.data,
            block_timestamp: parse_u64_decimal(&value.block_timestamp, "block_timestamp")?,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct WirePruneRequest {
    relevant_addresses: Vec<String>,
    before_block: u64,
}

impl From<PruneRequest> for WirePruneRequest {
    fn from(value: PruneRequest) -> Self {
        Self {
            relevant_addresses: value
                .relevant_addresses
                .into_iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            before_block: value.before_block,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct WireSubscribeFilter {
    token_addresses: Vec<String>,
    recipient_addresses: Vec<String>,
    min_amount: Option<String>,
    max_amount: Option<String>,
}

impl From<SubscribeFilter> for WireSubscribeFilter {
    fn from(value: SubscribeFilter) -> Self {
        Self {
            token_addresses: value
                .token_addresses
                .into_iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            recipient_addresses: value
                .recipient_addresses
                .into_iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            min_amount: value.min_amount.map(|amount| amount.to_string()),
            max_amount: value.max_amount.map(|amount| amount.to_string()),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct WireLogSubscribeFilter {
    addresses: Vec<String>,
    topic0: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic3: Option<String>,
}

impl From<LogSubscribeFilter> for WireLogSubscribeFilter {
    fn from(value: LogSubscribeFilter) -> Self {
        Self {
            addresses: value
                .addresses
                .into_iter()
                .map(|address| format!("{address:?}"))
                .collect(),
            topic0: format!("{:?}", value.topic0),
            topic1: value.topic1.map(|topic| format!("{topic:?}")),
            topic2: value.topic2.map(|topic| format!("{topic:?}")),
            topic3: value.topic3.map(|topic| format!("{topic:?}")),
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum WireClientSubscribeFilter {
    Transfer(WireSubscribeFilter),
    Log(WireLogSubscribeFilter),
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { filter: WireClientSubscribeFilter },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed {
        #[serde(rename = "subscription_id")]
        _subscription_id: Option<String>,
    },
    Unsubscribed {
        #[serde(rename = "subscription_id")]
        _subscription_id: Option<String>,
    },
    Transfer {
        event: WireEvmTransfer,
    },
    Log {
        event: WireEvmLog,
    },
    Error {
        error: String,
    },
}

fn decode_server_message(text: &str) -> Result<Option<EvmTransfer>> {
    match serde_json::from_str::<ServerMessage>(text)
        .map_err(|source| Error::WebSocketJson { source })?
    {
        ServerMessage::Transfer { event } => Ok(Some(event.try_into()?)),
        ServerMessage::Error { error } => Err(Error::WebSocket { source: error }),
        ServerMessage::Subscribed { .. }
        | ServerMessage::Unsubscribed { .. }
        | ServerMessage::Log { .. } => Ok(None),
    }
}

fn decode_log_server_message(text: &str) -> Result<Option<EvmLog>> {
    match serde_json::from_str::<ServerMessage>(text)
        .map_err(|source| Error::WebSocketJson { source })?
    {
        ServerMessage::Log { event } => Ok(Some(event.try_into()?)),
        ServerMessage::Error { error } => Err(Error::WebSocket { source: error }),
        ServerMessage::Subscribed { .. }
        | ServerMessage::Unsubscribed { .. }
        | ServerMessage::Transfer { .. } => Ok(None),
    }
}

fn append_transfer_query(url: &mut Url, query: &TransferQuery) {
    let mut pairs = url.query_pairs_mut();
    pairs.append_pair("to", &format!("{:?}", query.to));
    if let Some(token) = query.token {
        pairs.append_pair("token", &format!("{token:?}"));
    }
    if let Some(from_block) = query.from_block {
        pairs.append_pair("from_block", &from_block.to_string());
    }
    if let Some(min_amount) = query.min_amount {
        pairs.append_pair("min_amount", &min_amount.to_string());
    }
    if let Some(max_amount) = query.max_amount {
        pairs.append_pair("max_amount", &max_amount.to_string());
    }
    if let Some(limit) = query.limit {
        pairs.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = &query.cursor {
        pairs.append_pair("cursor", cursor);
    }
}

fn append_log_query(url: &mut Url, query: &LogQuery) {
    let mut pairs = url.query_pairs_mut();
    pairs.append_pair("address", &format!("{:?}", query.address));
    pairs.append_pair("topic0", &format!("{:?}", query.topic0));
    if let Some(topic1) = query.topic1 {
        pairs.append_pair("topic1", &format!("{topic1:?}"));
    }
    if let Some(topic2) = query.topic2 {
        pairs.append_pair("topic2", &format!("{topic2:?}"));
    }
    if let Some(topic3) = query.topic3 {
        pairs.append_pair("topic3", &format!("{topic3:?}"));
    }
    if let Some(from_block) = query.from_block {
        pairs.append_pair("from_block", &from_block.to_string());
    }
    if let Some(limit) = query.limit {
        pairs.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = &query.cursor {
        pairs.append_pair("cursor", cursor);
    }
}

async fn read_transfer_page_response(response: reqwest::Response) -> Result<TransferPage> {
    let wire = read_json_response::<WireTransferPage>(response).await?;
    let transfers = wire
        .transfers
        .into_iter()
        .map(EvmTransfer::try_from)
        .collect::<Result<Vec<_>>>()?;
    Ok(TransferPage {
        transfers,
        next_cursor: wire.next_cursor,
        has_more: wire.has_more,
    })
}

async fn read_log_page_response(response: reqwest::Response) -> Result<LogPage> {
    let wire = read_json_response::<WireLogPage>(response).await?;
    let logs = wire
        .logs
        .into_iter()
        .map(EvmLog::try_from)
        .collect::<Result<Vec<_>>>()?;
    Ok(LogPage {
        logs,
        next_cursor: wire.next_cursor,
        has_more: wire.has_more,
    })
}

async fn read_json_response<T>(response: reqwest::Response) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let status = response.status();
    let bytes = read_limited_response_body(response, EVM_INDEXER_MAX_RESPONSE_BODY_BYTES).await?;
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
    if bytes.len() <= EVM_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES {
        return String::from_utf8_lossy(bytes).to_string();
    }

    format!(
        "{}...<truncated {} bytes>",
        String::from_utf8_lossy(&bytes[..EVM_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES]),
        bytes.len() - EVM_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES
    )
}

fn parse_u256_decimal(value: &str, field: &'static str) -> Result<U256> {
    U256::from_str(value).map_err(|_| Error::InvalidDecimal {
        field,
        value: value.to_string(),
    })
}

fn parse_u64_decimal(value: &str, field: &'static str) -> Result<u64> {
    value.parse::<u64>().map_err(|_| Error::InvalidInteger {
        field,
        value: value.to_string(),
    })
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
        .map_err(|_| Error::UnsupportedUrl {
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

fn record_upstream_request(
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    observability::upstream::record_upstream_request(
        observability::upstream::UpstreamKind::SidecarService,
        "evm_indexer",
        method,
        endpoint,
        status_class,
        duration,
    );
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

    fn addr(byte: u8) -> Address {
        Address::repeat_byte(byte)
    }

    #[test]
    fn transfer_query_uses_primitive_endpoint_and_snake_case_parameters() {
        let client = EvmIndexerClient::new("http://localhost:4001/base-indexer").unwrap();
        let mut url = client.base_url.join("transfers").unwrap();
        append_transfer_query(
            &mut url,
            &TransferQuery {
                to: addr(0x11),
                token: Some(addr(0x22)),
                from_block: Some(123),
                min_amount: Some(U256::from(10_u64)),
                max_amount: Some(U256::from(20_u64)),
                limit: Some(50),
                cursor: Some("opaque".to_string()),
            },
        );

        assert_eq!(
            url.as_str(),
            "http://localhost:4001/base-indexer/transfers?to=0x1111111111111111111111111111111111111111&token=0x2222222222222222222222222222222222222222&from_block=123&min_amount=10&max_amount=20&limit=50&cursor=opaque"
        );
    }

    #[test]
    fn log_query_uses_primitive_endpoint_and_snake_case_parameters() {
        let client = EvmIndexerClient::new("http://localhost:4001/base-indexer").unwrap();
        let mut url = client.base_url.join("logs").unwrap();
        append_log_query(
            &mut url,
            &LogQuery {
                address: addr(0x11),
                topic0: B256::repeat_byte(0x22),
                topic1: Some(B256::repeat_byte(0x33)),
                topic2: None,
                topic3: Some(B256::repeat_byte(0x44)),
                from_block: Some(123),
                limit: Some(50),
                cursor: Some("opaque".to_string()),
            },
        );

        assert_eq!(
            url.as_str(),
            "http://localhost:4001/base-indexer/logs?address=0x1111111111111111111111111111111111111111&topic0=0x2222222222222222222222222222222222222222222222222222222222222222&topic1=0x3333333333333333333333333333333333333333333333333333333333333333&topic3=0x4444444444444444444444444444444444444444444444444444444444444444&from_block=123&limit=50&cursor=opaque"
        );
    }

    #[test]
    fn websocket_url_preserves_path_base() {
        let base = normalize_http_base_url("https://example.com/erc20-indexer").unwrap();
        let ws = subscribe_url_from_base(&base).unwrap();

        assert_eq!(ws.as_str(), "wss://example.com/erc20-indexer/subscribe");
    }

    #[test]
    fn filter_stream_trait_uses_tuple_filter() {
        fn assert_filter<T: FilterStream<Filter = EvmTransferFilter, Event = EvmTransfer>>() {}
        assert_filter::<EvmIndexerClient>();
    }

    #[test]
    fn wire_filter_serializes_as_snake_case_decimal_strings() {
        let wire = WireSubscribeFilter::from(SubscribeFilter {
            token_addresses: vec![addr(0xaa)],
            recipient_addresses: vec![addr(0xbb)],
            min_amount: Some(U256::from(7_u64)),
            max_amount: Some(U256::from(9_u64)),
        });

        let json = serde_json::to_string(&ClientMessage::Subscribe {
            filter: WireClientSubscribeFilter::Transfer(wire),
        })
        .unwrap();

        assert!(json.contains("\"action\":\"subscribe\""));
        assert!(json.contains("\"token_addresses\""));
        assert!(json.contains("\"recipient_addresses\""));
        assert!(json.contains("\"min_amount\":\"7\""));
        assert!(json.contains("\"max_amount\":\"9\""));
    }

    #[test]
    fn wire_log_filter_serializes_as_subscribe_action() {
        let wire = WireLogSubscribeFilter::from(LogSubscribeFilter {
            addresses: vec![addr(0xaa), addr(0xbb)],
            topic0: B256::repeat_byte(0x01),
            topic1: Some(B256::repeat_byte(0x02)),
            topic2: None,
            topic3: None,
        });

        let json = serde_json::to_string(&ClientMessage::Subscribe {
            filter: WireClientSubscribeFilter::Log(wire),
        })
        .unwrap();

        assert!(json.contains("\"action\":\"subscribe\""));
        assert!(json.contains("\"addresses\""));
        assert!(json.contains("\"topic0\""));
        assert!(json.contains("\"topic1\""));
        assert!(!json.contains("\"topic2\""));
    }

    #[test]
    fn wire_transfer_decodes_decimal_amount_and_block_strings() {
        let wire = WireEvmTransfer {
            id: "transfer-1".to_string(),
            chain_id: 8453,
            token_address: addr(0x01),
            from_address: addr(0x02),
            to_address: addr(0x03),
            amount: "12345678901234567890".to_string(),
            transaction_hash: B256::repeat_byte(0x04),
            block_number: "999".to_string(),
            block_hash: B256::repeat_byte(0x05),
            log_index: 6,
            block_timestamp: "1700000000".to_string(),
        };

        let transfer = EvmTransfer::try_from(wire).unwrap();

        assert_eq!(
            transfer.amount,
            U256::from_str("12345678901234567890").unwrap()
        );
        assert_eq!(transfer.block_number, 999);
        assert_eq!(transfer.block_timestamp, 1_700_000_000);
    }

    #[test]
    fn response_body_preview_truncates_large_error_bodies() {
        let body = vec![b'a'; EVM_INDEXER_MAX_ERROR_BODY_PREVIEW_BYTES + 3];

        let preview = response_body_preview(&body);

        assert!(preview.ends_with("...<truncated 3 bytes>"));
    }
}
