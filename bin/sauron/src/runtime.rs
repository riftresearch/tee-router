use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use pgwire_replication::ReplicationEvent;
use snafu::ResultExt;
use sqlx_postgres::{PgPool, PgPoolOptions};
use tokio::time::MissedTickBehavior;
use tracing::{info, warn};

use crate::{
    cdc::{
        cdc_event_seen, cdc_logical_message_seen, connect_stream, parse_router_cdc_message,
        recv_stream_event, validate_cdc_config, CdcConfig, RouterCdcMessage, RouterCdcRepository,
    },
    config::SauronArgs,
    cursor::CursorRepository,
    discovery::{
        bitcoin::BitcoinDiscoveryBackend, evm_erc20::EvmErc20DiscoveryBackend, run_backends,
        DiscoveryBackend, DiscoveryContext,
    },
    error::{
        InvalidConfigurationSnafu, ReplicaDatabaseConnectionSnafu, Result,
        StateDatabaseConnectionSnafu,
    },
    provider_operations::{
        full_provider_operation_reconcile, ProviderOperationWatchRepository,
        ProviderOperationWatchStore,
    },
    router_client::RouterClient,
    state_db::migrate_state,
    watch::{full_reconcile, WatchRepository, WatchStore},
};

const PROVIDER_OPERATION_HINT_INTERVAL: Duration = Duration::from_secs(5);
type BoxedSauronTask = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub async fn run(args: SauronArgs) -> Result<()> {
    let replica_pool = connect_replica_pool(&args).await?;
    let state_pool = connect_state_pool(&args).await?;
    migrate_state(&state_pool).await?;

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
        event_source = ?args.sauron_replica_event_source,
        state_database_separate = args.sauron_state_database_url.is_some(),
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

fn cdc_config_from_args(args: &SauronArgs) -> Result<CdcConfig> {
    let config = CdcConfig {
        database_url: args.router_replica_database_url.clone(),
        slot_name: args.sauron_cdc_slot_name.clone(),
        publication_name: args.router_cdc_publication_name.clone(),
        message_prefix: args.router_cdc_message_prefix.clone(),
        status_interval: Duration::from_millis(args.sauron_cdc_status_interval_ms),
        idle_wakeup_interval: Duration::from_millis(args.sauron_cdc_idle_wakeup_interval_ms),
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
    let replication_config = cdc_repository.replication_config()?;
    let slot_name = replication_config.slot.clone();
    let publication = replication_config.publication.clone();
    let mut client = connect_stream(replication_config).await?;
    info!(
        slot_name = %slot_name,
        publication = %publication,
        "Sauron CDC replication stream started"
    );

    loop {
        let Some(event) = recv_stream_event(&mut client).await? else {
            return InvalidConfigurationSnafu {
                message: "Sauron CDC replication stream ended unexpectedly".to_string(),
            }
            .fail();
        };

        cdc_event_seen();
        match event {
            ReplicationEvent::Message {
                prefix, content, ..
            } if prefix == cdc_repository.message_prefix() => {
                cdc_logical_message_seen();
                let message = parse_router_cdc_message(&content)?;
                refresh_from_cdc_message(
                    message,
                    &repository,
                    &store,
                    &provider_operation_repository,
                    &provider_operation_store,
                )
                .await?;
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                cdc_repository.save_checkpoint(end_lsn).await?;
                client.update_applied_lsn(end_lsn);
            }
            ReplicationEvent::StoppedAt { .. } => break,
            _ => {}
        }
    }
    Ok(())
}

async fn refresh_from_cdc_message(
    message: RouterCdcMessage,
    repository: &WatchRepository,
    store: &WatchStore,
    provider_operation_repository: &ProviderOperationWatchRepository,
    provider_operation_store: &ProviderOperationWatchStore,
) -> Result<()> {
    if let Some(watch_id) = message.watch_id {
        refresh_watch(watch_id, repository, store).await?;
    }

    if let Some(operation_id) = message.provider_operation_id {
        refresh_provider_operation(
            operation_id,
            provider_operation_repository,
            provider_operation_store,
        )
        .await?;
    }

    if message.watch_id.is_none() {
        if let Some(order_id) = message.order_id {
            let watches = repository.load_order_watches(order_id).await?;
            store.replace_order(order_id, watches).await;
            info!(
                order_id = %order_id,
                table = %message.table,
                op = %message.op,
                "Refreshed Sauron order watch set from CDC message"
            );
        }
    }

    Ok(())
}

async fn refresh_watch(
    watch_id: uuid::Uuid,
    repository: &WatchRepository,
    store: &WatchStore,
) -> Result<()> {
    match repository.load_watch(watch_id).await? {
        Some(watch) => {
            store.upsert(watch).await;
            info!(watch_id = %watch_id, "Refreshed Sauron chain-deposit watch from CDC message");
        }
        None => {
            store.remove(watch_id).await;
            info!(watch_id = %watch_id, "Removed Sauron chain-deposit watch from CDC message");
        }
    }
    Ok(())
}

async fn refresh_provider_operation(
    operation_id: uuid::Uuid,
    repository: &ProviderOperationWatchRepository,
    store: &ProviderOperationWatchStore,
) -> Result<()> {
    match repository.load_watch(operation_id).await? {
        Some(watch) => {
            store.upsert(watch).await;
            info!(operation_id = %operation_id, "Refreshed Sauron provider-operation watch from CDC message");
        }
        None => {
            store.remove(operation_id).await;
            info!(operation_id = %operation_id, "Removed Sauron provider-operation watch from CDC message");
        }
    }
    Ok(())
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
            let idempotency_key = Some(format!(
                "sauron-provider-operation-progress:{}",
                operation.operation_id
            ));
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
                idempotency_key,
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
