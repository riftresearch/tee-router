use alloy::primitives::Address;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
pub use hl_shim_indexer::ingest::{
    HlOrderEvent, HlOrderStatus, HlTransferEvent, HlTransferKind, StreamKind, SubscribeFilter,
};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("invalid HL shim URL {url}: {source}"))]
    InvalidUrl {
        url: String,
        source: url::ParseError,
    },

    #[snafu(display("unsupported HL shim URL scheme {scheme}; expected http or https"))]
    UnsupportedScheme { scheme: String },

    #[snafu(display("HL shim request failed"))]
    Request { source: reqwest::Error },

    #[snafu(display("HL shim response decode failed"))]
    Response { source: reqwest::Error },

    #[snafu(display("HL shim websocket failed: {source}"))]
    WebSocket {
        #[snafu(source(false))]
        source: String,
    },

    #[snafu(display("HL shim websocket frame decode failed: {source}"))]
    WebSocketJson { source: serde_json::Error },
}

#[derive(Clone)]
pub struct HlShimClient {
    http: reqwest::Client,
    base_url: Url,
}

impl HlShimClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let raw = base_url.as_ref().trim();
        let mut base_url = Url::parse(raw).context(InvalidUrlSnafu {
            url: raw.to_string(),
        })?;
        base_url.set_path("");
        base_url.set_query(None);
        base_url.set_fragment(None);
        Ok(Self {
            http: reqwest::Client::new(),
            base_url,
        })
    }

    pub async fn transfers(
        &self,
        user: Address,
        from_time: Option<i64>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<TransfersResponse> {
        let mut url = self.base_url.join("transfers").context(InvalidUrlSnafu {
            url: self.base_url.to_string(),
        })?;
        append_event_query(&mut url, user, from_time, limit, cursor);
        self.get_json(url).await
    }

    pub async fn orders(
        &self,
        user: Address,
        from_time: Option<i64>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<OrdersResponse> {
        let mut url = self.base_url.join("orders").context(InvalidUrlSnafu {
            url: self.base_url.to_string(),
        })?;
        append_event_query(&mut url, user, from_time, limit, cursor);
        self.get_json(url).await
    }

    pub async fn watch_order(&self, user: Address, oid: u64) -> Result<OrderWatchResponse> {
        let url = self
            .base_url
            .join("orders/watch")
            .context(InvalidUrlSnafu {
                url: self.base_url.to_string(),
            })?;
        self.http
            .post(url)
            .json(&OrderWatchRequest {
                user: format!("{user:?}"),
                oid,
            })
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?
            .json()
            .await
            .context(ResponseSnafu)
    }

    pub async fn unwatch_order(&self, oid: u64) -> Result<OrderWatchDeleteResponse> {
        let url = self
            .base_url
            .join(&format!("orders/watch/{oid}"))
            .context(InvalidUrlSnafu {
                url: self.base_url.to_string(),
            })?;
        self.http
            .delete(url)
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?
            .json()
            .await
            .context(ResponseSnafu)
    }

    pub async fn subscribe(
        &self,
        filter: SubscribeFilter,
    ) -> Result<mpsc::Receiver<Result<HlShimStreamEvent>>> {
        let ws_url = websocket_url(&self.base_url)?;
        let (mut socket, _) =
            connect_async(ws_url.as_str())
                .await
                .map_err(|source| Error::WebSocket {
                    source: source.to_string(),
                })?;
        socket
            .send(Message::Text(
                serde_json::to_string(&ClientMessage::Subscribe { filter })
                    .map_err(|source| Error::WebSocketJson { source })?,
            ))
            .await
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?;
        let (tx, rx) = mpsc::channel(256);
        tokio::spawn(async move {
            while let Some(frame) = socket.next().await {
                let result = match frame {
                    Ok(Message::Text(text)) => match decode_server_message(&text) {
                        Ok(Some(event)) => Ok(event),
                        Ok(None) => continue,
                        Err(error) => Err(error),
                    },
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => continue,
                };
                if tx.send(result).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: Url) -> Result<T> {
        self.http
            .get(url)
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?
            .json()
            .await
            .context(ResponseSnafu)
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
impl FilterStream for HlShimClient {
    type Filter = SubscribeFilter;
    type Event = HlShimStreamEvent;

    async fn stream_filter(
        &self,
        filter: Self::Filter,
    ) -> Result<mpsc::Receiver<Result<Self::Event>>> {
        self.subscribe(filter).await
    }
}

#[async_trait]
pub trait ByIdLookup {
    type Id;
    type Receipt;

    async fn lookup_by_id(&self, user: Address, id: Self::Id) -> Result<Option<Self::Receipt>>;
}

#[async_trait]
impl ByIdLookup for HlShimClient {
    type Id = u64;
    type Receipt = HlOrderEvent;

    async fn lookup_by_id(&self, user: Address, oid: u64) -> Result<Option<Self::Receipt>> {
        self.watch_order(user, oid).await?;
        let page = self.orders(user, Some(0), Some(2_000), None).await?;
        Ok(page.orders.into_iter().find(|order| order.oid == oid))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransfersResponse {
    pub events: Vec<HlTransferEvent>,
    pub next_cursor: String,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrdersResponse {
    pub orders: Vec<HlOrderEvent>,
    pub next_cursor: String,
    pub has_more: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWatchRequest {
    pub user: String,
    pub oid: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWatchResponse {
    pub user: String,
    pub oid: u64,
    pub watched: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWatchDeleteResponse {
    pub oid: u64,
    pub watched: bool,
}

#[derive(Debug, Clone)]
pub enum HlShimStreamEvent {
    Transfer(HlTransferEvent),
    Order(HlOrderEvent),
}

#[derive(Debug, Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum ClientMessage {
    Subscribe { filter: SubscribeFilter },
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ServerMessage {
    Subscribed {
        #[serde(rename = "subscription_id")]
        _subscription_id: uuid::Uuid,
    },
    Unsubscribed {
        #[serde(rename = "subscription_id")]
        _subscription_id: uuid::Uuid,
    },
    Transfer {
        event: HlTransferEvent,
    },
    Order {
        event: HlOrderEvent,
    },
    Error {
        error: String,
    },
}

fn decode_server_message(text: &str) -> Result<Option<HlShimStreamEvent>> {
    match serde_json::from_str::<ServerMessage>(text)
        .map_err(|source| Error::WebSocketJson { source })?
    {
        ServerMessage::Transfer { event } => Ok(Some(HlShimStreamEvent::Transfer(event))),
        ServerMessage::Order { event } => Ok(Some(HlShimStreamEvent::Order(event))),
        ServerMessage::Error { error } => Err(Error::WebSocket { source: error }),
        ServerMessage::Subscribed { .. } | ServerMessage::Unsubscribed { .. } => Ok(None),
    }
}

fn append_event_query(
    url: &mut Url,
    user: Address,
    from_time: Option<i64>,
    limit: Option<u32>,
    cursor: Option<&str>,
) {
    let mut pairs = url.query_pairs_mut();
    pairs.append_pair("user", &format!("{user:?}"));
    if let Some(from_time) = from_time {
        pairs.append_pair("from_time", &from_time.to_string());
    }
    if let Some(limit) = limit {
        pairs.append_pair("limit", &limit.to_string());
    }
    if let Some(cursor) = cursor {
        pairs.append_pair("cursor", cursor);
    }
}

fn websocket_url(base: &Url) -> Result<Url> {
    let mut url = base.clone();
    let scheme = match base.scheme() {
        "http" => "ws",
        "https" => "wss",
        other => {
            return Err(Error::UnsupportedScheme {
                scheme: other.to_string(),
            });
        }
    };
    url.set_scheme(scheme).map_err(|_| Error::WebSocket {
        source: format!("failed to set websocket scheme for {base}"),
    })?;
    url.set_path("subscribe");
    Ok(url)
}
