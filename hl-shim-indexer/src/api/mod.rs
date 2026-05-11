use std::sync::Arc;

use axum::{
    extract::{Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::{
    api::ws::handle_socket, ingest::QueryCursor, poller::Scheduler, storage::Storage, Error, PubSub,
};

pub mod ws;

#[derive(Clone)]
pub struct AppState {
    pub storage: Storage,
    pub scheduler: Scheduler,
    pub pubsub: PubSub,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/transfers", get(get_transfers))
        .route("/orders", get(get_orders))
        .route(
            "/orders/watch",
            get(get_order_watches).post(post_order_watch),
        )
        .route(
            "/orders/watch/:oid",
            axum::routing::delete(delete_order_watch),
        )
        .route("/prune", post(post_prune))
        .route("/subscribe", get(ws_subscribe))
        .with_state(Arc::new(state))
}

async fn healthz(State(state): State<Arc<AppState>>) -> ApiResult<Json<HealthResponse>> {
    state.storage.healthcheck().await?;
    Ok(Json(HealthResponse { ok: true }))
}

async fn get_transfers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<EventQuery>,
) -> ApiResult<Json<TransfersResponse>> {
    let user = parse_address(&query.user)?;
    let from_time = QueryCursor::decode(query.cursor.as_deref(), query.from_time.unwrap_or(0))?;
    let limit = normalized_limit(query.limit);
    let page = state
        .storage
        .transfers_page(user, from_time.0, i64::from(limit))
        .await?;
    Ok(Json(TransfersResponse {
        events: page.items,
        next_cursor: page.next_cursor.encode(),
        has_more: page.has_more,
    }))
}

async fn get_orders(
    State(state): State<Arc<AppState>>,
    Query(query): Query<EventQuery>,
) -> ApiResult<Json<OrdersResponse>> {
    let user = parse_address(&query.user)?;
    let from_time = QueryCursor::decode(query.cursor.as_deref(), query.from_time.unwrap_or(0))?;
    let limit = normalized_limit(query.limit);
    let page = state
        .storage
        .orders_page(user, from_time.0, i64::from(limit))
        .await?;
    Ok(Json(OrdersResponse {
        orders: page.items,
        next_cursor: page.next_cursor.encode(),
        has_more: page.has_more,
    }))
}

async fn post_prune(
    State(state): State<Arc<AppState>>,
    Json(request): Json<PruneRequest>,
) -> ApiResult<Json<PruneResponse>> {
    let users = request
        .relevant_users
        .iter()
        .map(|value| parse_address(value))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let storage = state.storage.clone();
    tokio::spawn(async move {
        if let Err(error) = storage.prune(&users, request.before_time_ms).await {
            tracing::warn!(?error, "HL shim prune job failed");
        }
    });
    Ok(Json(PruneResponse { accepted: true }))
}

async fn post_order_watch(
    State(state): State<Arc<AppState>>,
    Json(request): Json<OrderWatchRequest>,
) -> ApiResult<Json<OrderWatchResponse>> {
    let user = parse_address(&request.user)?;
    state
        .scheduler
        .register_pending_order(user, request.oid)
        .await;
    Ok(Json(OrderWatchResponse {
        user: format!("{user:?}"),
        oid: request.oid,
        watched: true,
    }))
}

async fn delete_order_watch(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(oid): axum::extract::Path<u64>,
) -> ApiResult<Json<OrderWatchDeleteResponse>> {
    state.scheduler.deregister_pending_order(oid).await;
    Ok(Json(OrderWatchDeleteResponse {
        oid,
        watched: false,
    }))
}

async fn get_order_watches(
    State(state): State<Arc<AppState>>,
) -> ApiResult<Json<OrderWatchesResponse>> {
    let mut watches = state
        .scheduler
        .pending_orders()
        .await
        .into_iter()
        .map(|(user, oid)| OrderWatchResponse {
            user: format!("{user:?}"),
            oid,
            watched: true,
        })
        .collect::<Vec<_>>();
    watches.sort_by(|left, right| left.user.cmp(&right.user).then(left.oid.cmp(&right.oid)));
    Ok(Json(OrderWatchesResponse { watches }))
}

async fn ws_subscribe(
    State(state): State<Arc<AppState>>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

#[derive(Debug, Deserialize)]
struct EventQuery {
    user: String,
    from_time: Option<i64>,
    limit: Option<u32>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PruneRequest {
    relevant_users: Vec<String>,
    before_time_ms: i64,
}

#[derive(Debug, Deserialize)]
struct OrderWatchRequest {
    user: String,
    oid: u64,
}

#[derive(Debug, Serialize)]
struct OrderWatchResponse {
    user: String,
    oid: u64,
    watched: bool,
}

#[derive(Debug, Serialize)]
struct OrderWatchDeleteResponse {
    oid: u64,
    watched: bool,
}

#[derive(Debug, Serialize)]
struct OrderWatchesResponse {
    watches: Vec<OrderWatchResponse>,
}

#[derive(Debug, Serialize)]
struct PruneResponse {
    accepted: bool,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct TransfersResponse {
    events: Vec<crate::ingest::HlTransferEvent>,
    next_cursor: String,
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct OrdersResponse {
    orders: Vec<crate::ingest::HlOrderEvent>,
    next_cursor: String,
    has_more: bool,
}

fn normalized_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(500).clamp(1, 2_000)
}

pub(crate) fn parse_address(value: &str) -> std::result::Result<alloy::primitives::Address, Error> {
    value.parse().map_err(|_| Error::InvalidAddress {
        value: value.to_string(),
    })
}

type ApiResult<T> = std::result::Result<T, ApiError>;

struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            Error::InvalidAddress { .. } | Error::InvalidCursor { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (
            status,
            Json(serde_json::json!({ "error": self.0.to_string() })),
        )
            .into_response()
    }
}

#[allow(dead_code)]
pub(crate) fn subscribe_filter_example() -> crate::ingest::SubscribeFilter {
    crate::ingest::SubscribeFilter {
        users: Vec::new(),
        kinds: Vec::new(),
    }
}
