use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use snafu::ResultExt;
use sqlx_postgres::{PgListener, PgPool, PgPoolOptions};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    cdc::{batch_touches_watch_tables, validate_cdc_config, CdcConfig, RouterCdcRepository},
    config::{SauronArgs, SauronReplicaEventSource},
    cursor::CursorRepository,
    discovery::{
        bitcoin::BitcoinDiscoveryBackend, evm_erc20::EvmErc20DiscoveryBackend, run_backends,
        DiscoveryBackend, DiscoveryContext,
    },
    error::{
        InvalidConfigurationSnafu, ReplicaDatabaseConnectionSnafu, ReplicaListenSnafu,
        ReplicaListenerConnectionSnafu, ReplicaNotificationReceiveSnafu, Result,
        StateDatabaseConnectionSnafu,
    },
    provider_operations::{
        full_provider_operation_reconcile, ProviderOperationWatchRepository,
        ProviderOperationWatchStore,
    },
    replica_db::migrate_replica,
    router_client::RouterClient,
    state_db::migrate_state,
    watch::{full_reconcile, WatchChangeNotification, WatchRepository, WatchStore},
};

const PROVIDER_OPERATION_HINT_INTERVAL: Duration = Duration::from_secs(5);
type BoxedSauronTask = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub async fn run(args: SauronArgs) -> Result<()> {
    let replica_pool = connect_replica_pool(&args).await?;
    let state_pool = connect_state_pool(&args).await?;
    migrate_state(&state_pool).await?;
    if args.sauron_replica_event_source == SauronReplicaEventSource::Notify {
        migrate_replica(&replica_pool).await?;
    }

    let repository = WatchRepository::new(replica_pool.clone());
    let provider_operation_repository = ProviderOperationWatchRepository::new(replica_pool.clone());
    let cursors = CursorRepository::new(state_pool.clone());

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
        event_source = ?args.sauron_replica_event_source,
        state_database_separate = args.sauron_state_database_url.is_some(),
        reconcile_interval_seconds = args.sauron_reconcile_interval_seconds,
        initial_watch_count,
        "Sauron startup completed"
    );

    let event_task = build_event_task(
        &args,
        replica_pool.clone(),
        state_pool.clone(),
        repository.clone(),
        store.clone(),
        provider_operation_repository.clone(),
        provider_operation_store.clone(),
    )
    .await?;
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
        event_task,
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

async fn connect_state_pool(args: &SauronArgs) -> Result<PgPool> {
    let database_url = args
        .sauron_state_database_url
        .as_deref()
        .unwrap_or(&args.router_replica_database_url);

    PgPoolOptions::new()
        .max_connections(4)
        .connect(database_url)
        .await
        .context(StateDatabaseConnectionSnafu)
}

async fn build_event_task(
    args: &SauronArgs,
    replica_pool: PgPool,
    state_pool: PgPool,
    repository: WatchRepository,
    store: WatchStore,
    provider_operation_repository: ProviderOperationWatchRepository,
    provider_operation_store: ProviderOperationWatchStore,
) -> Result<BoxedSauronTask> {
    match args.sauron_replica_event_source {
        SauronReplicaEventSource::Notify => {
            let mut listener = PgListener::connect(&args.router_replica_database_url)
                .await
                .context(ReplicaListenerConnectionSnafu)?;
            listener
                .listen(&args.router_replica_notification_channel)
                .await
                .context(ReplicaListenSnafu {
                    channel: args.router_replica_notification_channel.clone(),
                })?;

            Ok(Box::pin(run_notification_loop(
                listener,
                repository,
                store,
                provider_operation_repository,
                provider_operation_store,
                args.router_replica_notification_channel.clone(),
            )))
        }
        SauronReplicaEventSource::Cdc => {
            if args.sauron_state_database_url.is_none() {
                return InvalidConfigurationSnafu {
                    message: "SAURON_REPLICA_EVENT_SOURCE=cdc requires SAURON_STATE_DATABASE_URL so cursor/checkpoint writes do not target the router standby"
                        .to_string(),
                }
                .fail();
            }
            let cdc_config = cdc_config_from_args(args)?;
            let cdc_repository = RouterCdcRepository::new(replica_pool, state_pool, cdc_config);
            Ok(Box::pin(run_cdc_loop(
                cdc_repository,
                repository,
                store,
                provider_operation_repository,
                provider_operation_store,
            )))
        }
        SauronReplicaEventSource::Reconcile => {
            warn!("Sauron replica event source is reconcile-only; watch updates wait for the reconcile interval");
            Ok(Box::pin(async {
                std::future::pending::<()>().await;
                Ok(())
            }))
        }
    }
}

fn cdc_config_from_args(args: &SauronArgs) -> Result<CdcConfig> {
    let batch_size = i32::try_from(args.sauron_cdc_batch_size).map_err(|_| {
        crate::error::Error::InvalidConfiguration {
            message: "SAURON_CDC_BATCH_SIZE exceeded i32 range".to_string(),
        }
    })?;
    let config = CdcConfig {
        slot_name: args.sauron_cdc_slot_name.clone(),
        plugin: args.sauron_cdc_plugin.clone(),
        batch_size,
        poll_interval: Duration::from_millis(args.sauron_cdc_poll_interval_ms),
    };
    validate_cdc_config(&config)?;
    Ok(config)
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

async fn run_cdc_loop(
    cdc_repository: RouterCdcRepository,
    repository: WatchRepository,
    store: WatchStore,
    provider_operation_repository: ProviderOperationWatchRepository,
    provider_operation_store: ProviderOperationWatchStore,
) -> Result<()> {
    cdc_repository.ensure_slot().await?;

    let mut ticker = tokio::time::interval(cdc_repository.poll_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        let changes = cdc_repository.fetch_changes().await?;
        if changes.is_empty() {
            continue;
        }

        if batch_touches_watch_tables(&changes) {
            full_reconcile(&store, &repository).await?;
            full_provider_operation_reconcile(
                &provider_operation_store,
                &provider_operation_repository,
            )
            .await?;
        }

        if let Some(last_change) = changes.last() {
            cdc_repository
                .save_checkpoint(last_change, changes.len())
                .await?;
        }
    }
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
