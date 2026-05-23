use std::sync::Arc;

use axum::{routing::get, Router};
use bitcoin::Network;
use metrics_exporter_prometheus::PrometheusHandle;

use crate::{BitcoinIndexer, IndexerPubSub};

pub mod rest;
pub mod ws;

#[derive(Clone)]
pub struct AppState {
    pub indexer: BitcoinIndexer,
    pub pubsub: IndexerPubSub,
    pub metrics: Option<PrometheusHandle>,
    /// The Bitcoin network this indexer was configured against. Surfaced
    /// on `/healthz` so consumer clients can sanity-check that the
    /// indexer they're talking to matches their own expected network.
    pub network: Network,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(rest::healthz))
        .route("/metrics", get(rest::metrics))
        .route("/tx_outputs", get(rest::tx_outputs))
        .route("/subscribe", get(ws::ws_subscribe))
        .with_state(Arc::new(state))
}
