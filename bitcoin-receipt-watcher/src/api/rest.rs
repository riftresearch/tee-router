use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bitcoin::{consensus::deserialize, hex::FromHex};
use bitcoin_receipt_watcher_client::{
    parse_txid, WatchDeleteResponse, WatchEvent, WatchRequest, WatchResponse, WatchStatus,
};
use bitcoincore_rpc_async::RpcApi;
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
        .register(request.txid, request.requesting_operation_id)
        .await?;
    state.pubsub.publish(WatchEvent {
        chain: watch.chain.clone(),
        txid: watch.txid,
        requesting_operation_id: watch.requesting_operation_id.clone(),
        status: WatchStatus::Pending,
        receipt: None,
    });

    if let Err(error) = immediate_lookup(&state, watch.txid).await {
        tracing::warn!(
            chain = state.chain,
            txid = %watch.txid,
            %error,
            "immediate Bitcoin receipt lookup failed after watch registration"
        );
    }

    Ok(Json(WatchResponse {
        chain: watch.chain,
        txid: watch.txid,
        requesting_operation_id: watch.requesting_operation_id,
        watched: true,
    }))
}

pub async fn delete_watch(
    State(state): State<Arc<AppState>>,
    Path(txid): Path<String>,
) -> ApiResult<Json<WatchDeleteResponse>> {
    let txid = parse_txid(&txid).map_err(|_| Error::InvalidTxid { value: txid })?;
    state.pending.cancel(&txid).await;
    Ok(Json(WatchDeleteResponse {
        txid,
        watched: false,
    }))
}

async fn immediate_lookup(state: &AppState, txid: bitcoin::Txid) -> Result<(), Error> {
    let tx_hex = match state.rpc.get_raw_transaction_hex(&txid, None).await {
        Ok(tx_hex) => tx_hex,
        Err(_) => return Ok(()),
    };
    let tx_bytes = Vec::<u8>::from_hex(&tx_hex).map_err(|source| Error::HexDecode {
        context: "getrawtransaction response",
        source: source.to_string(),
    })?;
    let tx = deserialize(&tx_bytes).map_err(|source| Error::ConsensusDecode {
        context: "getrawtransaction response",
        source,
    })?;
    let status = state
        .rpc
        .get_raw_transaction_verbose(&txid)
        .await
        .map_err(|source| Error::BitcoinRpc {
            method: "getrawtransaction",
            source,
        })?;
    let Some(block_hash) = status.block_hash else {
        return Ok(());
    };
    let block_hash =
        block_hash
            .parse()
            .map_err(
                |source: bitcoin::hashes::hex::HexToArrayError| Error::HexDecode {
                    context: "getrawtransaction block hash",
                    source: source.to_string(),
                },
            )?;
    let block = state
        .rpc
        .get_block_verbose_one(&block_hash)
        .await
        .map_err(|source| Error::BitcoinRpc {
            method: "getblock",
            source,
        })?;
    let tip = state
        .rpc
        .get_block_count()
        .await
        .map_err(|source| Error::BitcoinRpc {
            method: "getblockcount",
            source,
        })?;
    let block_height = nonnegative_i64_to_u64(block.height, "getblock.height")?;
    let confirmations = tip.saturating_sub(block_height).saturating_add(1);
    let Some(watch) = state.pending.take(&txid).await else {
        return Ok(());
    };
    state.pubsub.publish(WatchEvent {
        chain: watch.chain,
        txid: watch.txid,
        requesting_operation_id: watch.requesting_operation_id,
        status: WatchStatus::Confirmed,
        receipt: Some(bitcoin_receipt_watcher_client::Receipt {
            tx,
            block_hash,
            block_height,
            confirmations,
        }),
    });
    Ok(())
}

fn nonnegative_i64_to_u64(value: i64, field: &'static str) -> Result<u64, Error> {
    u64::try_from(value).map_err(|source| Error::InvalidConfiguration {
        message: format!("{field} was negative or too large for u64: {source}"),
    })
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
            | Error::InvalidTxid { .. } => StatusCode::BAD_REQUEST,
            Error::PendingCapacity { .. } => StatusCode::TOO_MANY_REQUESTS,
            Error::MissingBlock { .. }
            | Error::BitcoinRpcInit { .. }
            | Error::BitcoinRpc { .. }
            | Error::Zmq { .. }
            | Error::ConsensusDecode { .. }
            | Error::HexDecode { .. }
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
