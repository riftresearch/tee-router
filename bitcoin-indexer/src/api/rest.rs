use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bitcoin_indexer_client::TxOutput;
use serde::{Deserialize, Serialize};

use crate::{api::AppState, Error};

pub async fn healthz(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        last_block_height: state.indexer.last_block_height().await,
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

pub async fn tx_outputs(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TxOutputQueryParams>,
) -> ApiResult<Json<TxOutputPageResponse>> {
    let page = state
        .indexer
        .query_outputs(
            &query.address,
            query.from_block.unwrap_or(0),
            query.min_amount.unwrap_or(0),
            query.limit.map(usize::from),
            query.cursor.as_deref(),
        )
        .await?;

    Ok(Json(TxOutputPageResponse {
        outputs: page.outputs,
        next_cursor: page.next_cursor,
        has_more: page.has_more,
    }))
}

#[derive(Debug, Deserialize)]
pub struct TxOutputQueryParams {
    pub address: String,
    pub from_block: Option<u64>,
    pub min_amount: Option<u64>,
    pub limit: Option<u16>,
    pub cursor: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub last_block_height: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TxOutputPageResponse {
    pub outputs: Vec<TxOutput>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
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
            Error::InvalidConfiguration { .. }
            | Error::InvalidAddress { .. }
            | Error::InvalidCursor { .. } => StatusCode::BAD_REQUEST,
            Error::BitcoinRpcInit { .. }
            | Error::BitcoinRpc { .. }
            | Error::Esplora { .. }
            | Error::MissingTransaction { .. }
            | Error::Zmq { .. }
            | Error::ConsensusDecode { .. }
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
