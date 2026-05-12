use bitcoin_indexer::{build_router, AppState, BitcoinIndexer, Config, Error, IndexerPubSub};
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use snafu::ResultExt;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();

    let config = Config::parse();
    config.validate()?;

    let metrics = PrometheusBuilder::new()
        .add_global_label("service", "bitcoin-indexer")
        .add_global_label("network", config.network.to_string())
        .install_recorder()
        .map_err(|source| Error::Metrics {
            message: source.to_string(),
        })?;

    let pubsub = IndexerPubSub::new(config.max_subscriber_lag);
    let indexer = BitcoinIndexer::new(&config, pubsub.clone()).await?;
    indexer.clone().run().await;

    let app = build_router(AppState {
        indexer,
        pubsub,
        metrics: Some(metrics),
    });

    let listener = TcpListener::bind(config.bind)
        .await
        .context(bitcoin_indexer::error::HttpServerSnafu)?;
    tracing::info!(
        bind = %config.bind,
        network = %config.network,
        "Bitcoin indexer listening"
    );
    axum::serve(listener, app)
        .await
        .context(bitcoin_indexer::error::HttpServerSnafu)?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
