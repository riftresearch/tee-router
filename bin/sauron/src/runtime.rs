use std::{sync::Arc, time::Duration};

use snafu::ResultExt;
use sqlx_postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    config::SauronArgs,
    cursor::CursorRepository,
    discovery::{
        bitcoin::BitcoinDiscoveryBackend, evm_erc20::EvmErc20DiscoveryBackend, run_backends,
        DiscoveryBackend, DiscoveryContext,
    },
    error::{
        ReplicaDatabaseConnectionSnafu, ReplicaListenSnafu, ReplicaListenerConnectionSnafu,
        ReplicaNotificationReceiveSnafu, Result,
    },
    provider_operations::{
        full_provider_operation_reconcile, ProviderOperationWatchRepository,
        ProviderOperationWatchStore,
    },
    replica_db::migrate_replica,
    router_client::RouterClient,
    watch::{full_reconcile, WatchChangeNotification, WatchRepository, WatchStore},
};

const PROVIDER_OPERATION_HINT_INTERVAL: Duration = Duration::from_secs(5);

pub async fn run(args: SauronArgs) -> Result<()> {
    let replica_pool = connect_replica_pool(&args).await?;
    migrate_replica(&replica_pool).await?;
    let repository = WatchRepository::new(replica_pool.clone());
    let provider_operation_repository = ProviderOperationWatchRepository::new(replica_pool.clone());
    let cursors = CursorRepository::new(replica_pool.clone());

    let mut listener = PgListener::connect(&args.router_replica_database_url)
        .await
        .context(ReplicaListenerConnectionSnafu)?;
    listener
        .listen(&args.router_replica_notification_channel)
        .await
        .context(ReplicaListenSnafu {
            channel: args.router_replica_notification_channel.clone(),
        })?;

    let router_client = RouterClient::new(&args)?;
    let store = WatchStore::default();
    let provider_operation_store = ProviderOperationWatchStore::default();
    let backends = build_backends(&args).await?;

    full_reconcile(&store, &repository).await?;
    full_provider_operation_reconcile(&provider_operation_store, &provider_operation_repository)
        .await?;

    let initial_watch_count = store.len().await;
    info!(
        replica_database = %args.router_replica_database_name,
        notification_channel = %args.router_replica_notification_channel,
        reconcile_interval_seconds = args.sauron_reconcile_interval_seconds,
        initial_watch_count,
        "Sauron startup completed"
    );

    let notification_task = run_notification_loop(
        listener,
        repository.clone(),
        store.clone(),
        provider_operation_repository.clone(),
        provider_operation_store.clone(),
        args.router_replica_notification_channel.clone(),
    );
    let reconcile_task = run_reconcile_loop(
        store.clone(),
        repository.clone(),
        provider_operation_store.clone(),
        provider_operation_repository.clone(),
        Duration::from_secs(args.sauron_reconcile_interval_seconds),
    );
    let expiration_prune_task = run_expiration_prune_loop(store.clone(), Duration::from_secs(60));
    let provider_operation_hint_task = run_provider_operation_hint_loop(
        provider_operation_store.clone(),
        router_client.clone(),
        PROVIDER_OPERATION_HINT_INTERVAL,
    );
    let discovery_task = run_backends(
        backends,
        DiscoveryContext {
            watches: store.clone(),
            cursors,
            router_client,
        },
    );

    tokio::try_join!(
        notification_task,
        reconcile_task,
        expiration_prune_task,
        provider_operation_hint_task,
        discovery_task
    )?;
    Ok(())
}

async fn connect_replica_pool(args: &SauronArgs) -> Result<PgPool> {
    PgPoolOptions::new()
        .max_connections(8)
        .connect(&args.router_replica_database_url)
        .await
        .context(ReplicaDatabaseConnectionSnafu)
}

async fn build_backends(args: &SauronArgs) -> Result<Vec<Arc<dyn DiscoveryBackend>>> {
    let bitcoin = BitcoinDiscoveryBackend::new(args).await?;
    let ethereum = EvmErc20DiscoveryBackend::new_ethereum(args).await?;
    let arbitrum = EvmErc20DiscoveryBackend::new_arbitrum(args).await?;
    let base = EvmErc20DiscoveryBackend::new_base(args).await?;

    Ok(vec![
        Arc::new(bitcoin),
        Arc::new(ethereum),
        Arc::new(arbitrum),
        Arc::new(base),
    ])
}

async fn run_notification_loop(
    mut listener: PgListener,
    repository: WatchRepository,
    store: WatchStore,
    provider_operation_repository: ProviderOperationWatchRepository,
    provider_operation_store: ProviderOperationWatchStore,
    channel: String,
) -> Result<()> {
    loop {
        let notification = listener
            .recv()
            .await
            .context(ReplicaNotificationReceiveSnafu)?;

        match WatchChangeNotification::parse(&notification) {
            Ok(change) => {
                match repository.load_watch(change.watch_id).await {
                    Ok(Some(watch)) => {
                        store.upsert(watch).await;
                        info!(watch_id = %change.watch_id, "Refreshed chain-deposit watch after Postgres notification");
                    }
                    Ok(None) => {
                        store.remove(change.watch_id).await;
                        info!(watch_id = %change.watch_id, "Removed chain-deposit watch after Postgres notification");
                    }
                    Err(error) => {
                        warn!(
                            watch_id = %change.watch_id,
                            %error,
                            "Targeted chain-deposit watch refresh failed; falling back to full reconcile"
                        );
                        full_reconcile(&store, &repository).await?;
                    }
                }

                match provider_operation_repository
                    .load_watch(change.watch_id)
                    .await
                {
                    Ok(Some(watch)) => {
                        provider_operation_store.upsert(watch).await;
                        info!(operation_id = %change.watch_id, "Refreshed provider-operation watch after Postgres notification");
                    }
                    Ok(None) => {
                        provider_operation_store.remove(change.watch_id).await;
                        info!(operation_id = %change.watch_id, "Removed provider-operation watch after Postgres notification");
                    }
                    Err(error) => {
                        warn!(
                            operation_id = %change.watch_id,
                            %error,
                            "Targeted provider-operation watch refresh failed; falling back to full reconcile"
                        );
                        full_provider_operation_reconcile(
                            &provider_operation_store,
                            &provider_operation_repository,
                        )
                        .await?;
                    }
                }
            }
            Err(error) => {
                warn!(
                    channel = %channel,
                    payload = notification.payload(),
                    %error,
                    "Replica notification payload was invalid; falling back to full reconcile"
                );
                full_reconcile(&store, &repository).await?;
                full_provider_operation_reconcile(
                    &provider_operation_store,
                    &provider_operation_repository,
                )
                .await?;
            }
        }
    }
}

async fn run_reconcile_loop(
    store: WatchStore,
    repository: WatchRepository,
    provider_operation_store: ProviderOperationWatchStore,
    provider_operation_repository: ProviderOperationWatchRepository,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        full_reconcile(&store, &repository).await?;
        full_provider_operation_reconcile(
            &provider_operation_store,
            &provider_operation_repository,
        )
        .await?;
    }
}

async fn run_provider_operation_hint_loop(
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        let operations = store.snapshot().await;
        for operation in operations {
            let request = router_server::api::ProviderOperationHintRequest {
                provider_operation_id: operation.operation_id,
                source: "sauron_provider_operation".to_string(),
                hint_kind: router_server::models::ProviderOperationHintKind::PossibleProgress,
                evidence: serde_json::json!({
                    "provider": operation.provider,
                    "operation_type": operation.operation_type.to_db_string(),
                    "provider_ref": operation.provider_ref,
                    "observed_at": chrono::Utc::now(),
                }),
                idempotency_key: None,
            };

            if let Err(error) = router_client.submit_provider_operation_hint(&request).await {
                warn!(
                    operation_id = %operation.operation_id,
                    provider = %operation.provider,
                    operation_type = %operation.operation_type.to_db_string(),
                    %error,
                    "Failed to submit provider-operation progress hint"
                );
            }
        }

        ticker.tick().await;
    }
}

async fn run_expiration_prune_loop(store: WatchStore, interval: Duration) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let pruned = store.prune_expired().await;
        if pruned > 0 {
            info!(pruned, "Pruned expired Sauron watches from memory");
        }
    }
}
