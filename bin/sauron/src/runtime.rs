use std::{
    collections::{HashMap, HashSet},
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use pgwire_replication::ReplicationEvent;
use router_server::{
    api::{
        ProviderOperationHintRequest, ProviderOperationObserveRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN,
    },
    models::{ProviderOperationHintKind, PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE},
    services::action_providers::ProviderOperationObservation,
};
use sha2::{Digest, Sha256};
use snafu::ResultExt;
use sqlx_postgres::{PgPool, PgPoolOptions};
use tokio::time::{sleep, timeout, MissedTickBehavior};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    cdc::{
        cdc_event_seen, cdc_logical_message_seen, connect_stream, parse_router_cdc_message,
        recv_stream_event, validate_cdc_config, CdcConfig, RouterCdcMessage, RouterCdcRepository,
    },
    config::{normalize_router_detector_api_key, SauronArgs, MIN_TOKEN_INDEXER_API_KEY_LEN},
    cursor::CursorRepository,
    discovery::{
        bitcoin::BitcoinDiscoveryBackend, evm_erc20::EvmErc20DiscoveryBackend, run_backends,
        DiscoveryBackend, DiscoveryContext,
    },
    error::{Error, ReplicaDatabaseConnectionSnafu, Result, StateDatabaseConnectionSnafu},
    provider_operations::{
        full_provider_operation_reconcile, ProviderOperationWatchEntry,
        ProviderOperationWatchRepository, ProviderOperationWatchStore,
        SharedProviderOperationWatchEntry,
    },
    router_client::{normalize_router_internal_base_url, RouterClient},
    state_db::migrate_state,
    watch::{full_reconcile, WatchRepository, WatchStore},
};

const PROVIDER_OPERATION_OBSERVE_INTERVAL: Duration = Duration::from_secs(5);
const PROVIDER_OPERATION_OBSERVE_CONCURRENCY: usize = 32;
const PROVIDER_OPERATION_OBSERVE_TIMEOUT: Duration = Duration::from_secs(10);
const PROVIDER_OPERATION_OBSERVATION_RESUBMIT_INTERVAL: Duration = Duration::from_secs(30);
const CDC_RECONNECT_INITIAL_DELAY: Duration = Duration::from_secs(1);
const CDC_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
const CDC_RECONNECT_RESET_AFTER: Duration = Duration::from_secs(60);
const MAX_CDC_PENDING_REFRESH_IDS: usize = 10_000;
type BoxedSauronTask = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

pub async fn run(args: SauronArgs) -> Result<()> {
    validate_runtime_config(&args)?;

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
        state_database_url_configured = true,
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
    let full_reconcile_task = run_full_reconcile_loop(
        repository.clone(),
        store.clone(),
        provider_operation_repository.clone(),
        provider_operation_store.clone(),
        Duration::from_secs(args.sauron_reconcile_interval_seconds),
    );
    let expiration_prune_task = run_expiration_prune_loop(store.clone(), Duration::from_secs(60));
    let provider_operation_observer_task = run_provider_operation_observer_loop(
        provider_operation_store.clone(),
        router_client.clone(),
        PROVIDER_OPERATION_OBSERVE_INTERVAL,
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
        full_reconcile_task,
        expiration_prune_task,
        provider_operation_observer_task,
        discovery_task
    )?;
    Ok(())
}

fn validate_runtime_config(args: &SauronArgs) -> Result<()> {
    validate_positive_seconds(
        args.sauron_reconcile_interval_seconds,
        "SAURON_RECONCILE_INTERVAL_SECONDS",
    )?;
    validate_positive_seconds(
        args.sauron_bitcoin_scan_interval_seconds,
        "SAURON_BITCOIN_SCAN_INTERVAL_SECONDS",
    )?;
    validate_positive_usize(
        args.sauron_bitcoin_indexed_lookup_concurrency,
        "SAURON_BITCOIN_INDEXED_LOOKUP_CONCURRENCY",
    )?;
    validate_positive_usize(
        args.sauron_evm_indexed_lookup_concurrency,
        "SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY",
    )?;
    validate_router_internal_base_url(&args.router_internal_base_url)?;
    validate_router_detector_api_key(&args.router_detector_api_key)?;
    validate_token_indexer_api_key(args)?;
    Ok(())
}

fn validate_router_internal_base_url(value: &str) -> Result<()> {
    normalize_router_internal_base_url(value)?;
    Ok(())
}

fn validate_router_detector_api_key(value: &str) -> Result<()> {
    if normalize_router_detector_api_key(value).is_none() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_DETECTOR_API_KEY must be at least 32 characters".to_string(),
        });
    }
    Ok(())
}

fn validate_token_indexer_api_key(args: &SauronArgs) -> Result<()> {
    let token_indexer_configured = args.ethereum_token_indexer_url.is_some()
        || args.base_token_indexer_url.is_some()
        || args.arbitrum_token_indexer_url.is_some();
    if !token_indexer_configured {
        return Ok(());
    }

    let Some(api_key) = args.token_indexer_api_key.as_deref().map(str::trim) else {
        return Err(Error::InvalidConfiguration {
            message:
                "TOKEN_INDEXER_API_KEY must be configured when token indexer URLs are configured"
                    .to_string(),
        });
    };
    if api_key.len() < MIN_TOKEN_INDEXER_API_KEY_LEN {
        return Err(Error::InvalidConfiguration {
            message: format!(
                "TOKEN_INDEXER_API_KEY must be at least {MIN_TOKEN_INDEXER_API_KEY_LEN} characters"
            ),
        });
    }
    Ok(())
}

fn validate_positive_seconds(value: u64, name: &str) -> Result<()> {
    if value == 0 {
        return Err(Error::InvalidConfiguration {
            message: format!("{name} must be positive"),
        });
    }
    Ok(())
}

fn validate_positive_usize(value: usize, name: &str) -> Result<()> {
    if value == 0 {
        return Err(Error::InvalidConfiguration {
            message: format!("{name} must be positive"),
        });
    }
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
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&args.sauron_state_database_url)
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
    let mut reconnect_delay = CDC_RECONNECT_INITIAL_DELAY;

    loop {
        let attempt_started_at = Instant::now();
        match run_cdc_stream_once(
            &cdc_repository,
            &repository,
            &store,
            &provider_operation_repository,
            &provider_operation_store,
        )
        .await
        {
            Ok(()) => warn!("Sauron CDC replication stream stopped; reconnecting"),
            Err(error) => {
                if cdc_error_is_fatal(&error) {
                    return Err(error);
                }
                warn!(
                    error = %error,
                    delay_ms = reconnect_delay.as_millis(),
                    "Sauron CDC replication stream failed; reconnecting"
                );
            }
        }

        sleep(reconnect_delay).await;
        reconnect_delay =
            next_cdc_reconnect_delay_after_attempt(reconnect_delay, attempt_started_at.elapsed());
    }
}

async fn run_cdc_stream_once(
    cdc_repository: &RouterCdcRepository,
    repository: &WatchRepository,
    store: &WatchStore,
    provider_operation_repository: &ProviderOperationWatchRepository,
    provider_operation_store: &ProviderOperationWatchStore,
) -> Result<()> {
    cdc_repository.ensure_slot().await?;
    reconcile_cdc_watch_sets(
        repository,
        store,
        provider_operation_repository,
        provider_operation_store,
    )
    .await?;
    let replication_config = cdc_repository.replication_config()?;
    let slot_name = replication_config.slot.clone();
    let publication = replication_config.publication.clone();
    let mut client = connect_stream(replication_config).await?;
    info!(
        slot_name = %slot_name,
        publication = %publication,
        "Sauron CDC replication stream started"
    );

    let mut pending_plan = CdcRefreshPlan::default();
    loop {
        let Some(event) = recv_stream_event(&mut client).await? else {
            return Ok(());
        };

        cdc_event_seen();
        match event {
            ReplicationEvent::Message {
                prefix, content, ..
            } if prefix == cdc_repository.message_prefix() => {
                cdc_logical_message_seen();
                merge_valid_router_cdc_message(&content, &mut pending_plan)?;
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                refresh_from_cdc_plan(
                    std::mem::take(&mut pending_plan),
                    repository,
                    store,
                    provider_operation_repository,
                    provider_operation_store,
                )
                .await?;
                cdc_repository.save_checkpoint(end_lsn).await?;
                client.update_applied_lsn(end_lsn);
            }
            ReplicationEvent::StoppedAt { .. } => break,
            _ => {}
        }
    }
    Ok(())
}

async fn reconcile_cdc_watch_sets(
    repository: &WatchRepository,
    store: &WatchStore,
    provider_operation_repository: &ProviderOperationWatchRepository,
    provider_operation_store: &ProviderOperationWatchStore,
) -> Result<()> {
    full_reconcile(store, repository).await?;
    full_provider_operation_reconcile(provider_operation_store, provider_operation_repository)
        .await?;
    Ok(())
}

async fn run_full_reconcile_loop(
    repository: WatchRepository,
    store: WatchStore,
    provider_operation_repository: ProviderOperationWatchRepository,
    provider_operation_store: ProviderOperationWatchStore,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        match reconcile_cdc_watch_sets(
            &repository,
            &store,
            &provider_operation_repository,
            &provider_operation_store,
        )
        .await
        {
            Ok(()) => {
                info!("Sauron periodic full reconcile completed");
            }
            Err(error) => {
                warn!(
                    %error,
                    "Sauron periodic full reconcile failed; keeping existing watch sets"
                );
            }
        }
    }
}

fn cdc_error_is_fatal(error: &Error) -> bool {
    matches!(error, Error::InvalidConfiguration { .. })
}

fn next_cdc_reconnect_delay(current: Duration) -> Duration {
    current.saturating_mul(2).min(CDC_RECONNECT_MAX_DELAY)
}

fn next_cdc_reconnect_delay_after_attempt(
    current: Duration,
    attempt_elapsed: Duration,
) -> Duration {
    if attempt_elapsed >= CDC_RECONNECT_RESET_AFTER {
        CDC_RECONNECT_INITIAL_DELAY
    } else {
        next_cdc_reconnect_delay(current)
    }
}

fn merge_valid_router_cdc_message(content: &[u8], pending_plan: &mut CdcRefreshPlan) -> Result<()> {
    if pending_plan.requires_full_reconcile {
        return Ok(());
    }

    match parse_router_cdc_message(content) {
        Ok(message) if supported_router_cdc_message(&message) => {
            pending_plan.merge_message(message);
            if pending_plan.pending_id_count() > MAX_CDC_PENDING_REFRESH_IDS {
                pending_plan.collapse_to_full_reconcile();
                warn!(
                    max_ids = MAX_CDC_PENDING_REFRESH_IDS,
                    "Sauron CDC commit exceeded targeted refresh cap; collapsing to full reconcile"
                );
            }
        }
        Ok(message) => {
            warn!(
                version = message.version,
                schema = %message.schema,
                table = %message.table,
                op = %message.op,
                "Ignoring unsupported Sauron CDC message"
            );
        }
        Err(error) => {
            pending_plan.collapse_to_full_reconcile();
            warn!(
                error = %error,
                "Malformed Sauron CDC message; requiring full reconcile for this commit"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use router_server::{
        models::ProviderOperationStatus, services::action_providers::ProviderOperationObservation,
    };

    #[test]
    fn cdc_reconnect_delay_backs_off_and_caps() {
        assert_eq!(
            next_cdc_reconnect_delay(CDC_RECONNECT_INITIAL_DELAY),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_cdc_reconnect_delay(Duration::from_secs(20)),
            CDC_RECONNECT_MAX_DELAY
        );
        assert_eq!(
            next_cdc_reconnect_delay(CDC_RECONNECT_MAX_DELAY),
            CDC_RECONNECT_MAX_DELAY
        );
    }

    #[test]
    fn cdc_reconnect_delay_resets_after_stable_stream_attempt() {
        assert_eq!(
            next_cdc_reconnect_delay_after_attempt(
                CDC_RECONNECT_MAX_DELAY,
                CDC_RECONNECT_RESET_AFTER
            ),
            CDC_RECONNECT_INITIAL_DELAY
        );
        assert_eq!(
            next_cdc_reconnect_delay_after_attempt(
                Duration::from_secs(4),
                CDC_RECONNECT_RESET_AFTER - Duration::from_millis(1)
            ),
            Duration::from_secs(8)
        );
    }

    #[test]
    fn cdc_reconnect_treats_only_invalid_configuration_as_fatal() {
        let invalid_configuration = Error::InvalidConfiguration {
            message: "bad slot".to_string(),
        };
        let transient_stream = Error::CdcStream {
            source: pgwire_replication::PgWireError::Io(std::sync::Arc::new(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "stream ended",
            ))),
        };

        assert!(cdc_error_is_fatal(&invalid_configuration));
        assert!(!cdc_error_is_fatal(&transient_stream));
    }

    #[test]
    fn runtime_positive_config_guards_reject_zero_values() {
        assert!(matches!(
            validate_positive_seconds(0, "SAURON_RECONCILE_INTERVAL_SECONDS"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_positive_usize(0, "SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY"),
            Err(Error::InvalidConfiguration { .. })
        ));
        validate_positive_seconds(1, "SAURON_RECONCILE_INTERVAL_SECONDS").unwrap();
        validate_positive_usize(1, "SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY").unwrap();
    }

    #[test]
    fn runtime_config_rejects_short_router_detector_api_key() {
        assert!(matches!(
            validate_router_detector_api_key("short-detector-key"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_detector_api_key("   "),
            Err(Error::InvalidConfiguration { .. })
        ));
        validate_router_detector_api_key("  test-detector-secret-000000000000  ").unwrap();
    }

    #[test]
    fn runtime_config_requires_token_indexer_api_key_when_urls_are_configured() {
        let mut args = test_sauron_args();
        args.ethereum_token_indexer_url = Some("http://token-indexer.internal".to_string());
        args.token_indexer_api_key = None;

        assert!(matches!(
            validate_token_indexer_api_key(&args),
            Err(Error::InvalidConfiguration { .. })
        ));

        args.token_indexer_api_key = Some("short".to_string());
        assert!(matches!(
            validate_token_indexer_api_key(&args),
            Err(Error::InvalidConfiguration { .. })
        ));

        args.token_indexer_api_key = Some("token-indexer-api-key-000000000000".to_string());
        validate_token_indexer_api_key(&args).unwrap();
    }

    #[test]
    fn runtime_config_rejects_invalid_router_internal_base_urls() {
        assert!(matches!(
            validate_router_internal_base_url("not a url"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_internal_base_url("file:///tmp/router.sock"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_internal_base_url("https://user:pass@router.internal"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_internal_base_url("https://router.internal?token=secret"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_internal_base_url("https://router.internal#fragment"),
            Err(Error::InvalidConfiguration { .. })
        ));
        assert!(matches!(
            validate_router_internal_base_url("https://router.internal/prefix"),
            Err(Error::InvalidConfiguration { .. })
        ));
        validate_router_internal_base_url("  http://router-api:4522  ").unwrap();
        validate_router_internal_base_url("http://router-api:4522///").unwrap();
        validate_router_internal_base_url("https://router.internal").unwrap();
    }

    fn test_sauron_args() -> SauronArgs {
        SauronArgs {
            log_level: "warn".to_string(),
            router_replica_database_url: "postgres://replica@localhost/router".to_string(),
            sauron_state_database_url: "postgres://state@localhost/sauron".to_string(),
            sauron_replica_event_source: crate::config::SauronReplicaEventSource::Cdc,
            router_replica_database_name: "router_db".to_string(),
            sauron_cdc_slot_name: "sauron_watch_cdc".to_string(),
            router_cdc_publication_name: "router_cdc_publication".to_string(),
            router_cdc_message_prefix: "rift.router.change".to_string(),
            sauron_cdc_status_interval_ms: 1000,
            sauron_cdc_idle_wakeup_interval_ms: 10_000,
            router_internal_base_url: "http://router-api:4522".to_string(),
            router_detector_api_key: "detector-api-key-0000000000000000".to_string(),
            electrum_http_server_url: "http://electrum:3000".to_string(),
            bitcoin_rpc_url: "http://bitcoin:8332".to_string(),
            bitcoin_rpc_auth: bitcoincore_rpc_async::Auth::None,
            bitcoin_zmq_rawtx_endpoint: "tcp://bitcoin:28332".to_string(),
            bitcoin_zmq_sequence_endpoint: "tcp://bitcoin:28333".to_string(),
            ethereum_mainnet_rpc_url: "http://ethereum:8545".to_string(),
            ethereum_token_indexer_url: None,
            base_rpc_url: "http://base:8545".to_string(),
            base_token_indexer_url: None,
            arbitrum_rpc_url: "http://arbitrum:8545".to_string(),
            arbitrum_token_indexer_url: None,
            token_indexer_api_key: None,
            sauron_reconcile_interval_seconds: 3600,
            sauron_bitcoin_scan_interval_seconds: 15,
            sauron_bitcoin_indexed_lookup_concurrency: 32,
            sauron_evm_indexed_lookup_concurrency: 8,
        }
    }

    #[test]
    fn cdc_refresh_plan_deduplicates_commit_batch() {
        let order_id = uuid::Uuid::now_v7();
        let watch_id = uuid::Uuid::now_v7();
        let provider_operation_id = uuid::Uuid::now_v7();
        let plan = cdc_refresh_plan([
            test_cdc_message(Some(order_id), None, None),
            test_cdc_message(Some(order_id), None, None),
            test_cdc_message(Some(order_id), Some(watch_id), None),
            test_provider_operation_cdc_message(order_id, provider_operation_id),
            test_provider_operation_cdc_message(order_id, provider_operation_id),
        ]);

        assert_eq!(plan.order_ids.len(), 1);
        assert!(plan.order_ids.contains_key(&order_id));
        assert_eq!(plan.watch_ids.len(), 1);
        assert!(plan.watch_ids.contains_key(&watch_id));
        assert_eq!(plan.provider_operation_ids.len(), 1);
        assert!(plan
            .provider_operation_ids
            .contains_key(&provider_operation_id));
    }

    #[test]
    fn cdc_stream_collapses_malformed_logical_messages_to_full_reconcile() {
        let mut pending = CdcRefreshPlan::default();
        merge_valid_router_cdc_message(b"{not-json", &mut pending)
            .expect("malformed messages should request reconcile");
        assert_eq!(pending.pending_id_count(), 0);
        assert!(pending.requires_full_reconcile);

        let order_id = uuid::Uuid::now_v7();
        let payload = format!(
            r#"{{
                "version": 1,
                "schema": "public",
                "table": "router_orders",
                "op": "UPDATE",
                "orderId": "{order_id}",
                "eventUpdatedAt": "2026-05-04T06:00:00Z"
            }}"#
        );
        merge_valid_router_cdc_message(payload.as_bytes(), &mut pending)
            .expect("valid message should be ignored after full reconcile was requested");

        assert_eq!(pending.pending_id_count(), 0);
        assert!(pending.order_ids.is_empty());

        let unsupported_payload = format!(
            r#"{{
                "version": 2,
                "schema": "public",
                "table": "router_orders",
                "op": "UPDATE",
                "orderId": "{order_id}"
            }}"#
        );
        merge_valid_router_cdc_message(unsupported_payload.as_bytes(), &mut pending)
            .expect("unsupported malformed message should keep full reconcile request");
        assert_eq!(pending.pending_id_count(), 0);
        assert!(pending.requires_full_reconcile);
    }

    #[test]
    fn cdc_stream_ignores_unassociated_quote_messages_without_full_reconcile() {
        let quote_id = uuid::Uuid::now_v7();
        let payload = format!(
            r#"{{
                "version": 1,
                "schema": "public",
                "table": "market_order_quotes",
                "op": "INSERT",
                "id": "{quote_id}",
                "orderId": null,
                "watchId": null,
                "providerOperationId": null,
                "eventUpdatedAt": "2026-05-04T06:00:00Z"
            }}"#
        );
        let mut pending = CdcRefreshPlan::default();

        merge_valid_router_cdc_message(payload.as_bytes(), &mut pending)
            .expect("unassociated quote messages should be valid no-ops");

        assert_eq!(pending.pending_id_count(), 0);
        assert!(!pending.requires_full_reconcile);
    }

    #[test]
    fn cdc_stream_collapses_oversized_pending_refresh_plans_to_full_reconcile() {
        let mut pending = CdcRefreshPlan::default();

        for _ in 0..MAX_CDC_PENDING_REFRESH_IDS {
            let order_id = uuid::Uuid::now_v7();
            let payload = format!(
                r#"{{
                    "version": 1,
                    "schema": "public",
                    "table": "router_orders",
                    "op": "UPDATE",
                    "orderId": "{order_id}",
                    "eventUpdatedAt": "2026-05-04T06:00:00Z"
                }}"#
            );
            merge_valid_router_cdc_message(payload.as_bytes(), &mut pending)
                .expect("message should fit below cap");
        }
        assert_eq!(pending.pending_id_count(), MAX_CDC_PENDING_REFRESH_IDS);
        assert!(!pending.requires_full_reconcile);

        let overflowing_order_id = uuid::Uuid::now_v7();
        let overflow_payload = format!(
            r#"{{
                "version": 1,
                "schema": "public",
                "table": "router_orders",
                "op": "UPDATE",
                "orderId": "{overflowing_order_id}",
                "eventUpdatedAt": "2026-05-04T06:00:00Z"
            }}"#
        );
        merge_valid_router_cdc_message(overflow_payload.as_bytes(), &mut pending)
            .expect("refresh id overflow should collapse to full reconcile");

        assert_eq!(pending.pending_id_count(), 0);
        assert!(pending.requires_full_reconcile);
    }

    #[test]
    fn provider_operation_observation_fingerprint_is_stable_and_status_sensitive() {
        let waiting = ProviderOperationObservation {
            status: ProviderOperationStatus::WaitingExternal,
            provider_ref: Some("provider-ref-1".to_string()),
            observed_state: serde_json::json!({ "state": "pending" }),
            response: None,
            tx_hash: None,
            error: None,
        };
        let completed = ProviderOperationObservation {
            status: ProviderOperationStatus::Completed,
            provider_ref: Some("provider-ref-1".to_string()),
            observed_state: serde_json::json!({ "state": "filled" }),
            response: Some(serde_json::json!({ "amount_out": "100" })),
            tx_hash: Some("0xabc".to_string()),
            error: None,
        };

        let first = provider_operation_observation_fingerprint(&waiting)
            .expect("fingerprint waiting observation");
        let second = provider_operation_observation_fingerprint(&waiting)
            .expect("fingerprint waiting observation again");
        let third = provider_operation_observation_fingerprint(&completed)
            .expect("fingerprint completed observation");

        assert_eq!(first, second);
        assert_ne!(first, third);
    }

    #[test]
    fn provider_operation_observation_hint_idempotency_key_is_bounded_token_text() {
        let operation_id = uuid::Uuid::now_v7();
        let fingerprint = "a".repeat(64);

        let key = provider_operation_observation_hint_idempotency_key(operation_id, &fingerprint);

        assert!(key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
        assert!(key.bytes().all(|byte| matches!(
            byte,
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b':' | b'-'
        )));
        assert_eq!(
            key,
            format!("sauron:op-observe:{operation_id}:{fingerprint}")
        );
    }

    #[test]
    fn provider_operation_observation_submission_is_throttled_not_suppressed_forever() {
        let now = Instant::now();
        let recent = SubmittedProviderOperationObservation {
            fingerprint: "same".to_string(),
            submitted_at: now - (PROVIDER_OPERATION_OBSERVATION_RESUBMIT_INTERVAL / 2),
        };
        let stale = SubmittedProviderOperationObservation {
            fingerprint: "same".to_string(),
            submitted_at: now - PROVIDER_OPERATION_OBSERVATION_RESUBMIT_INTERVAL,
        };

        assert!(should_submit_provider_operation_observation(
            None, "same", now
        ));
        assert!(!should_submit_provider_operation_observation(
            Some(&recent),
            "same",
            now
        ));
        assert!(should_submit_provider_operation_observation(
            Some(&recent),
            "changed",
            now
        ));
        assert!(should_submit_provider_operation_observation(
            Some(&stale),
            "same",
            now
        ));
    }

    #[test]
    fn cdc_refresh_plan_keeps_watch_refreshes_targeted() {
        let order_id = uuid::Uuid::now_v7();
        let watch_id = uuid::Uuid::now_v7();
        let plan = cdc_refresh_plan([
            test_cdc_message(Some(order_id), Some(watch_id), None),
            test_cdc_message(Some(order_id), Some(watch_id), None),
        ]);

        assert!(plan.order_ids.is_empty());
        assert_eq!(plan.watch_ids.len(), 1);
        assert!(plan.watch_ids.contains_key(&watch_id));
        assert!(plan.provider_operation_ids.is_empty());
    }

    #[test]
    fn cdc_refresh_plan_keeps_provider_operation_refreshes_out_of_watch_set() {
        let order_id = uuid::Uuid::now_v7();
        let erroneous_watch_id = uuid::Uuid::now_v7();
        let provider_operation_id = uuid::Uuid::now_v7();
        let mut operation_message = test_cdc_message(
            Some(order_id),
            Some(erroneous_watch_id),
            Some(provider_operation_id),
        );
        operation_message.table = "order_provider_operations".to_string();
        let mut address_message = test_cdc_message(
            Some(order_id),
            Some(erroneous_watch_id),
            Some(provider_operation_id),
        );
        address_message.table = "order_provider_addresses".to_string();

        let plan = cdc_refresh_plan([operation_message, address_message]);

        assert!(plan.order_ids.is_empty());
        assert!(plan.watch_ids.is_empty());
        assert_eq!(plan.provider_operation_ids.len(), 1);
        assert!(plan
            .provider_operation_ids
            .contains_key(&provider_operation_id));
    }

    #[test]
    fn cdc_refresh_plan_keeps_newest_source_timestamp() {
        let order_id = uuid::Uuid::now_v7();
        let watch_id = uuid::Uuid::now_v7();
        let older = "2026-05-04T06:00:00Z".parse().unwrap();
        let newer = "2026-05-04T06:00:01Z".parse().unwrap();
        let mut first = test_cdc_message(Some(order_id), Some(watch_id), None);
        first.event_updated_at = Some(newer);
        let mut second = test_cdc_message(Some(order_id), Some(watch_id), None);
        second.event_updated_at = Some(older);

        let plan = cdc_refresh_plan([first, second]);

        assert_eq!(plan.watch_ids.get(&watch_id), Some(&Some(newer)));
    }

    #[test]
    fn cdc_refresh_plan_ignores_unsupported_messages() {
        let order_id = uuid::Uuid::now_v7();
        let mut unsupported = test_cdc_message(Some(order_id), None, None);
        unsupported.version = 2;

        let plan = cdc_refresh_plan([unsupported]);

        assert!(plan.order_ids.is_empty());
        assert!(plan.watch_ids.is_empty());
        assert!(plan.provider_operation_ids.is_empty());
    }

    fn test_cdc_message(
        order_id: Option<uuid::Uuid>,
        watch_id: Option<uuid::Uuid>,
        provider_operation_id: Option<uuid::Uuid>,
    ) -> RouterCdcMessage {
        RouterCdcMessage {
            version: 1,
            schema: "public".to_string(),
            table: "router_orders".to_string(),
            op: "UPDATE".to_string(),
            id: order_id,
            order_id,
            order_updated_at: None,
            event_updated_at: None,
            watch_id,
            provider_operation_id,
        }
    }

    fn test_provider_operation_cdc_message(
        order_id: uuid::Uuid,
        provider_operation_id: uuid::Uuid,
    ) -> RouterCdcMessage {
        RouterCdcMessage {
            table: "order_provider_operations".to_string(),
            ..test_cdc_message(Some(order_id), None, Some(provider_operation_id))
        }
    }
}

async fn refresh_from_cdc_plan(
    plan: CdcRefreshPlan,
    repository: &WatchRepository,
    store: &WatchStore,
    provider_operation_repository: &ProviderOperationWatchRepository,
    provider_operation_store: &ProviderOperationWatchStore,
) -> Result<()> {
    if plan.requires_full_reconcile {
        reconcile_cdc_watch_sets(
            repository,
            store,
            provider_operation_repository,
            provider_operation_store,
        )
        .await?;
        info!("Refreshed full Sauron watch sets from CDC message batch");
        return Ok(());
    }

    for (watch_id, source_updated_at) in plan.watch_ids {
        refresh_watch(watch_id, source_updated_at, repository, store).await?;
    }

    for (operation_id, source_updated_at) in plan.provider_operation_ids {
        refresh_provider_operation(
            operation_id,
            source_updated_at,
            provider_operation_repository,
            provider_operation_store,
        )
        .await?;
    }

    for (order_id, source_updated_at) in plan.order_ids {
        let watches = repository.load_order_watches(order_id).await?;
        store
            .replace_order(order_id, watches, source_updated_at)
            .await;
        debug!(
            order_id = %order_id,
            "Refreshed Sauron order watch set from CDC message batch"
        );
    }

    Ok(())
}

#[derive(Debug, Default, PartialEq, Eq)]
struct CdcRefreshPlan {
    watch_ids: HashMap<uuid::Uuid, Option<DateTime<Utc>>>,
    provider_operation_ids: HashMap<uuid::Uuid, Option<DateTime<Utc>>>,
    order_ids: HashMap<uuid::Uuid, Option<DateTime<Utc>>>,
    requires_full_reconcile: bool,
}

impl CdcRefreshPlan {
    fn collapse_to_full_reconcile(&mut self) {
        self.watch_ids.clear();
        self.provider_operation_ids.clear();
        self.order_ids.clear();
        self.requires_full_reconcile = true;
    }

    fn merge_message(&mut self, message: RouterCdcMessage) {
        if self.requires_full_reconcile {
            return;
        }
        if !supported_router_cdc_message(&message) {
            return;
        }
        let source_updated_at = message.event_updated_at.or(message.order_updated_at);

        if provider_operation_cdc_table(&message.table) {
            if let Some(provider_operation_id) = message.provider_operation_id {
                merge_source_updated_at(
                    &mut self.provider_operation_ids,
                    provider_operation_id,
                    source_updated_at,
                );
            }
            return;
        }

        if let Some(watch_id) = message.watch_id {
            merge_source_updated_at(&mut self.watch_ids, watch_id, source_updated_at);
        } else if let Some(order_id) = message.order_id {
            merge_source_updated_at(&mut self.order_ids, order_id, source_updated_at);
        }
    }

    fn pending_id_count(&self) -> usize {
        self.watch_ids.len() + self.provider_operation_ids.len() + self.order_ids.len()
    }
}

fn provider_operation_cdc_table(table: &str) -> bool {
    matches!(
        table,
        "order_provider_operations" | "order_provider_addresses"
    )
}

fn merge_source_updated_at(
    values: &mut HashMap<uuid::Uuid, Option<DateTime<Utc>>>,
    id: uuid::Uuid,
    candidate: Option<DateTime<Utc>>,
) {
    values
        .entry(id)
        .and_modify(|existing| {
            if let Some(candidate) = candidate {
                if match existing.as_ref() {
                    Some(existing) => *existing < candidate,
                    None => true,
                } {
                    *existing = Some(candidate);
                }
            }
        })
        .or_insert(candidate);
}

fn supported_router_cdc_message(message: &RouterCdcMessage) -> bool {
    message.version == 1
        && message.schema == "public"
        && matches!(message.op.as_str(), "INSERT" | "UPDATE" | "DELETE")
        && matches!(
            message.table.as_str(),
            "router_orders"
                | "market_order_quotes"
                | "market_order_actions"
                | "limit_order_quotes"
                | "limit_order_actions"
                | "order_execution_legs"
                | "order_execution_steps"
                | "order_provider_operations"
                | "order_provider_addresses"
                | "deposit_vaults"
                | "custody_vaults"
        )
}

async fn refresh_watch(
    watch_id: uuid::Uuid,
    source_updated_at: Option<DateTime<Utc>>,
    repository: &WatchRepository,
    store: &WatchStore,
) -> Result<()> {
    match repository.load_watch(watch_id).await? {
        Some(watch) => {
            store.upsert(watch).await;
            debug!(watch_id = %watch_id, "Refreshed Sauron chain-deposit watch from CDC message");
        }
        None => {
            store.remove_with_source(watch_id, source_updated_at).await;
            debug!(watch_id = %watch_id, "Removed Sauron chain-deposit watch from CDC message");
        }
    }
    Ok(())
}

async fn refresh_provider_operation(
    operation_id: uuid::Uuid,
    source_updated_at: Option<DateTime<Utc>>,
    repository: &ProviderOperationWatchRepository,
    store: &ProviderOperationWatchStore,
) -> Result<()> {
    match repository.load_watch(operation_id).await? {
        Some(watch) => {
            store.upsert(watch).await;
            debug!(operation_id = %operation_id, "Refreshed Sauron provider-operation watch from CDC message");
        }
        None => {
            store
                .remove_with_source(operation_id, source_updated_at)
                .await;
            debug!(operation_id = %operation_id, "Removed Sauron provider-operation watch from CDC message");
        }
    }
    Ok(())
}
#[cfg(test)]
fn cdc_refresh_plan(messages: impl IntoIterator<Item = RouterCdcMessage>) -> CdcRefreshPlan {
    let mut plan = CdcRefreshPlan::default();

    for message in messages {
        plan.merge_message(message);
    }

    plan
}

async fn run_provider_operation_observer_loop(
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
    interval: Duration,
) -> Result<()> {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut last_observation_submissions: HashMap<
        uuid::Uuid,
        SubmittedProviderOperationObservation,
    > = HashMap::new();

    loop {
        let operations = store.snapshot().await;
        let active_operation_ids = operations
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<HashSet<_>>();
        last_observation_submissions
            .retain(|operation_id, _| active_operation_ids.contains(operation_id));

        observe_provider_operations(
            operations,
            router_client.clone(),
            &mut last_observation_submissions,
        )
        .await;

        ticker.tick().await;
    }
}

#[derive(Debug)]
struct ProviderOperationObserveOutcome {
    operation_id: uuid::Uuid,
    submitted_observation: Option<SubmittedProviderOperationObservation>,
}

#[derive(Debug, Clone)]
struct SubmittedProviderOperationObservation {
    fingerprint: String,
    submitted_at: Instant,
}

async fn observe_provider_operations(
    operations: Vec<SharedProviderOperationWatchEntry>,
    router_client: RouterClient,
    last_observation_submissions: &mut HashMap<uuid::Uuid, SubmittedProviderOperationObservation>,
) {
    let mut operations = operations.into_iter();
    let mut tasks = tokio::task::JoinSet::new();

    loop {
        while tasks.len() < PROVIDER_OPERATION_OBSERVE_CONCURRENCY {
            let Some(operation) = operations.next() else {
                break;
            };
            let router_client = router_client.clone();
            let last_submission = last_observation_submissions
                .get(&operation.operation_id)
                .cloned();
            tasks.spawn(async move {
                observe_provider_operation_once(router_client, operation, last_submission).await
            });
        }

        if tasks.is_empty() {
            break;
        }

        match tasks.join_next().await {
            Some(Ok(outcome)) => {
                if let Some(submission) = outcome.submitted_observation {
                    last_observation_submissions.insert(outcome.operation_id, submission);
                }
            }
            Some(Err(error)) => {
                warn!(
                    %error,
                    "Provider-operation observer task panicked or was cancelled"
                );
            }
            None => break,
        }
    }
}

async fn observe_provider_operation_once(
    router_client: RouterClient,
    operation: SharedProviderOperationWatchEntry,
    last_submission: Option<SubmittedProviderOperationObservation>,
) -> ProviderOperationObserveOutcome {
    let observe_request = ProviderOperationObserveRequest {
        hint_evidence: provider_operation_observe_evidence(operation.as_ref()),
    };
    let operation_id = operation.operation_id;
    let observation = match timeout(
        PROVIDER_OPERATION_OBSERVE_TIMEOUT,
        router_client.observe_provider_operation(operation_id, &observe_request),
    )
    .await
    {
        Ok(Ok(Some(observation))) => observation,
        Ok(Ok(None)) => {
            debug!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                "Provider-operation observe proxy returned no observation"
            );
            return ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            };
        }
        Ok(Err(error)) => {
            warn!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                %error,
                "Failed to observe provider operation through router proxy"
            );
            return ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            };
        }
        Err(_) => {
            warn!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                timeout_ms = PROVIDER_OPERATION_OBSERVE_TIMEOUT.as_millis(),
                "Timed out observing provider operation through router proxy"
            );
            return ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            };
        }
    };

    let fingerprint = match provider_operation_observation_fingerprint(&observation) {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            warn!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                %error,
                "Failed to fingerprint provider-operation observation"
            );
            return ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            };
        }
    };
    let now = Instant::now();
    if !should_submit_provider_operation_observation(last_submission.as_ref(), &fingerprint, now) {
        return ProviderOperationObserveOutcome {
            operation_id,
            submitted_observation: None,
        };
    }

    let request = ProviderOperationHintRequest {
        provider_operation_id: operation_id,
        source: PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE.to_string(),
        hint_kind: ProviderOperationHintKind::PossibleProgress,
        evidence: provider_operation_observation_hint_evidence(
            operation.as_ref(),
            &observation,
            &fingerprint,
        ),
        idempotency_key: Some(provider_operation_observation_hint_idempotency_key(
            operation_id,
            &fingerprint,
        )),
    };

    match timeout(
        PROVIDER_OPERATION_OBSERVE_TIMEOUT,
        router_client.submit_provider_operation_hint(&request),
    )
    .await
    {
        Ok(Ok(_)) => ProviderOperationObserveOutcome {
            operation_id,
            submitted_observation: Some(SubmittedProviderOperationObservation {
                fingerprint,
                submitted_at: Instant::now(),
            }),
        },
        Ok(Err(error)) => {
            warn!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                %error,
                "Failed to submit provider-operation observation hint"
            );
            ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            }
        }
        Err(_) => {
            warn!(
                operation_id = %operation_id,
                provider = %operation.provider,
                operation_type = %operation.operation_type.to_db_string(),
                timeout_ms = PROVIDER_OPERATION_OBSERVE_TIMEOUT.as_millis(),
                "Timed out submitting provider-operation observation hint"
            );
            ProviderOperationObserveOutcome {
                operation_id,
                submitted_observation: None,
            }
        }
    }
}

fn should_submit_provider_operation_observation(
    last_submission: Option<&SubmittedProviderOperationObservation>,
    fingerprint: &str,
    now: Instant,
) -> bool {
    let Some(last_submission) = last_submission else {
        return true;
    };
    last_submission.fingerprint != fingerprint
        || now.duration_since(last_submission.submitted_at)
            >= PROVIDER_OPERATION_OBSERVATION_RESUBMIT_INTERVAL
}

fn provider_operation_observe_evidence(
    operation: &ProviderOperationWatchEntry,
) -> serde_json::Value {
    serde_json::json!({
        "source": "sauron_provider_operation_observer",
        "provider": &operation.provider,
        "operation_type": operation.operation_type.to_db_string(),
        "provider_ref": &operation.provider_ref,
        "observed_at": chrono::Utc::now(),
    })
}

fn provider_operation_observation_hint_evidence(
    operation: &ProviderOperationWatchEntry,
    observation: &ProviderOperationObservation,
    fingerprint: &str,
) -> serde_json::Value {
    serde_json::json!({
        "source": "sauron_provider_operation_observer",
        "provider": &operation.provider,
        "operation_type": operation.operation_type.to_db_string(),
        "provider_ref": &operation.provider_ref,
        "observation_fingerprint": fingerprint,
        "provider_observation": observation,
        "observed_at": chrono::Utc::now(),
    })
}

fn provider_operation_observation_fingerprint(
    observation: &ProviderOperationObservation,
) -> std::result::Result<String, serde_json::Error> {
    let bytes = serde_json::to_vec(observation)?;
    let digest = Sha256::digest(bytes);
    Ok(alloy::hex::encode(digest))
}

fn provider_operation_observation_hint_idempotency_key(
    operation_id: Uuid,
    fingerprint: &str,
) -> String {
    let idempotency_key = format!("sauron:op-observe:{operation_id}:{fingerprint}");
    debug_assert!(idempotency_key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
    idempotency_key
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
