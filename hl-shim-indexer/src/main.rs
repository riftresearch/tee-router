use clap::Parser;
use hl_shim_indexer::{
    build_router,
    config::Config,
    poller::{Poller, Scheduler},
    storage::Storage,
    AppState, PubSub,
};
use snafu::ResultExt;
use tokio::net::TcpListener;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    observability::init_prometheus_metrics_from_env("hl-shim-indexer")?;

    let config = Config::parse();
    config.validate()?;

    let storage = Storage::connect(&config.database_url, config.max_db_connections).await?;
    let pubsub = PubSub::new(config.max_subscriber_lag);
    let scheduler = Scheduler::new(config.cadences());
    let poller = Poller::new(
        scheduler.clone(),
        storage.clone(),
        pubsub.clone(),
        &config.info_url,
        config.weight_budget_per_min,
    )
    .await?;
    let worker_count = config.worker_count;
    tokio::spawn(async move {
        if let Err(error) = poller.run(worker_count).await {
            tracing::error!(?error, "HL shim poller stopped");
        }
    });

    let app = build_router(AppState {
        storage,
        scheduler,
        pubsub,
    });
    let listener = TcpListener::bind(config.bind)
        .await
        .context(hl_shim_indexer::error::HttpServerSnafu)?;
    tracing::info!(bind = %config.bind, "HL shim indexer listening");
    axum::serve(listener, app)
        .await
        .context(hl_shim_indexer::error::HttpServerSnafu)?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();
}
