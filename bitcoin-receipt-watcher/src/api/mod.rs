use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};
use bitcoincore_rpc_async::Client as BitcoinRpcClient;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::{PendingWatches, ReceiptPubSub};

pub mod rest;
pub mod ws;

#[derive(Clone)]
pub struct AppState {
    pub chain: String,
    pub pending: PendingWatches,
    pub pubsub: ReceiptPubSub,
    pub rpc: Arc<BitcoinRpcClient>,
    pub metrics: Option<PrometheusHandle>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(rest::healthz))
        .route("/metrics", get(rest::metrics))
        .route("/watch", post(rest::post_watch))
        .route("/watch/:txid", delete(rest::delete_watch))
        .route("/subscribe", get(ws::ws_subscribe))
        .with_state(Arc::new(state))
}
