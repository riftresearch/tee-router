use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use evm_receipt_watcher_client::{
    parse_tx_hash, WatchDeleteResponse, WatchEvent, WatchRequest, WatchResponse, WatchStatus,
};
use serde::Serialize;

use crate::{api::AppState, Error};

pub async fn healthz(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    let pending_count = state.pending.len().await;
    let max_pending = state.pending.max_pending().await;
    Json(HealthResponse {
        ok: true,
        chain: state.chain.clone(),
        pending_count,
        max_pending,
    })
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    match &state.metrics {
        Some(handle) => (
            StatusCode::OK,
            [("content-type", "text/plain; version=0.0.4")],
            handle.render(),
        )
            .into_response(),
        None => (
            StatusCode::SERVICE_UNAVAILABLE,
            "Prometheus recorder is not installed for this process",
        )
            .into_response(),
    }
}

pub async fn post_watch(
    State(state): State<Arc<AppState>>,
    Json(request): Json<WatchRequest>,
) -> ApiResult<Json<WatchResponse>> {
    if request.chain != state.chain {
        return Err(Error::ChainMismatch {
            expected: state.chain.clone(),
            actual: request.chain,
        }
        .into());
    }
    if request.requesting_operation_id.trim().is_empty() {
        return Err(Error::InvalidConfiguration {
            message: "requesting_operation_id must not be empty".to_string(),
        }
        .into());
    }

    let watch = state
        .pending
        .register(request.tx_hash, request.requesting_operation_id)
        .await?;
    state.pubsub.publish(WatchEvent {
        chain: watch.chain.clone(),
        tx_hash: watch.tx_hash,
        requesting_operation_id: watch.requesting_operation_id.clone(),
        status: WatchStatus::Pending,
        receipt: None,
        logs: Vec::new(),
    });

    Ok(Json(WatchResponse {
        chain: watch.chain,
        tx_hash: watch.tx_hash,
        requesting_operation_id: watch.requesting_operation_id,
        watched: true,
    }))
}

pub async fn delete_watch(
    State(state): State<Arc<AppState>>,
    Path(tx_hash): Path<String>,
) -> ApiResult<Json<WatchDeleteResponse>> {
    let tx_hash = parse_tx_hash(&tx_hash).map_err(|_| Error::InvalidTxHash { value: tx_hash })?;
    state.pending.cancel(&tx_hash).await;
    Ok(Json(WatchDeleteResponse {
        tx_hash,
        watched: false,
    }))
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub chain: String,
    pub pending_count: usize,
    pub max_pending: usize,
}

pub type ApiResult<T> = std::result::Result<T, ApiError>;

#[derive(Debug)]
pub struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::ChainMismatch { .. }
            | Error::InvalidConfiguration { .. }
            | Error::InvalidTxHash { .. } => StatusCode::BAD_REQUEST,
            Error::PendingCapacity { .. } => StatusCode::TOO_MANY_REQUESTS,
            Error::MissingBlock { .. }
            | Error::EvmRpc { .. }
            | Error::WebSocket { .. }
            | Error::Serialization { .. }
            | Error::Metrics { .. }
            | Error::HttpServer { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let body = Json(ErrorResponse {
            error: self.0.to_string(),
        });
        (status, body).into_response()
    }
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}
