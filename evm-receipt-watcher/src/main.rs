use clap::Parser;
use evm_receipt_watcher::{
    build_router, AppState, Config, Error, PendingWatches, ReceiptPubSub, Watcher,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let config = Config::parse();
    config.validate()?;

    let metrics = PrometheusBuilder::new()
        .add_global_label("service", "evm-receipt-watcher")
        .add_global_label("chain", config.chain.clone())
        .install_recorder()
        .map_err(|source| Error::Metrics {
            message: source.to_string(),
        })?;

    let pending = PendingWatches::new(config.chain.clone(), config.max_pending);
    let pubsub = ReceiptPubSub::new(config.max_subscriber_lag);
    let watcher = Watcher::new(&config, pending.clone(), pubsub.clone()).await?;
    tokio::spawn(async move {
        watcher.run().await;
    });

    let app = build_router(AppState {
        chain: config.chain.clone(),
        pending,
        pubsub,
        metrics: Some(metrics),
    })
    .layer(TraceLayer::new_for_http());

    let listener = TcpListener::bind(config.bind)
        .await
        .context(evm_receipt_watcher::error::HttpServerSnafu)?;
    tracing::info!(bind = %config.bind, chain = config.chain, "EVM receipt watcher listening");
    axum::serve(listener, app)
        .await
        .context(evm_receipt_watcher::error::HttpServerSnafu)?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
