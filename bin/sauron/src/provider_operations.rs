use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use metrics::{gauge, histogram};
use router_core::models::{ProviderOperationStatus, ProviderOperationType};
use router_temporal::WorkflowStepId;
use serde_json::Value;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::{ReplicaDatabaseQuerySnafu, Result};

const FULL_PROVIDER_OPERATION_WATCH_QUERY: &str = r#"
SELECT
  opo.id::text AS operation_id,
  opo.execution_step_id::text AS execution_step_id,
  opo.provider,
  opo.operation_type,
  opo.provider_ref,
  opo.status,
  opo.request_json,
  opo.response_json,
  opo.observed_state_json,
  oes.request_json AS execution_step_request_json,
  opo.updated_at
FROM public.order_provider_operations opo
JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opo.operation_type IN ('across_bridge', 'cctp_bridge', 'cctp_receive', 'hyperliquid_bridge_deposit', 'hyperliquid_bridge_withdrawal', 'unit_deposit', 'unit_withdrawal', 'hyperliquid_trade', 'hyperliquid_limit_order', 'universal_router_swap')
  AND opo.status IN ('submitted', 'waiting_external')
  AND oes.status IN ('running', 'waiting')
ORDER BY opo.updated_at ASC, opo.id ASC
"#;

const TARGETED_PROVIDER_OPERATION_WATCH_QUERY: &str = r#"
SELECT
  opo.id::text AS operation_id,
  opo.execution_step_id::text AS execution_step_id,
  opo.provider,
  opo.operation_type,
  opo.provider_ref,
  opo.status,
  opo.request_json,
  opo.response_json,
  opo.observed_state_json,
  oes.request_json AS execution_step_request_json,
  opo.updated_at
FROM public.order_provider_operations opo
JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opo.id = $1::uuid
  AND opo.operation_type IN ('across_bridge', 'cctp_bridge', 'cctp_receive', 'hyperliquid_bridge_deposit', 'hyperliquid_bridge_withdrawal', 'unit_deposit', 'unit_withdrawal', 'hyperliquid_trade', 'hyperliquid_limit_order', 'universal_router_swap')
  AND opo.status IN ('submitted', 'waiting_external')
  AND oes.status IN ('running', 'waiting')
"#;

const SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC: &str = "sauron_provider_operation_watch_count";
const SAURON_PROVIDER_OPERATION_RECONCILE_DURATION_SECONDS: &str =
    "sauron_provider_operation_reconcile_duration_seconds";
const SAURON_PROVIDER_OPERATION_LOAD_ALL_DURATION_SECONDS: &str =
    "sauron_provider_operation_load_all_duration_seconds";
const SAURON_PROVIDER_OPERATION_LOAD_WATCH_DURATION_SECONDS: &str =
    "sauron_provider_operation_load_watch_duration_seconds";
#[derive(Debug, Clone)]
pub struct ProviderOperationWatchEntry {
    pub operation_id: Uuid,
    pub execution_step_id: WorkflowStepId,
    pub provider: String,
    pub operation_type: ProviderOperationType,
    pub provider_ref: Option<String>,
    pub status: ProviderOperationStatus,
    pub request: Value,
    pub response: Value,
    pub observed_state: Value,
    pub execution_step_request: Value,
    pub updated_at: DateTime<Utc>,
}

pub type SharedProviderOperationWatchEntry = Arc<ProviderOperationWatchEntry>;

#[derive(Debug, Default, Clone)]
pub struct ProviderOperationWatchStore {
    inner: Arc<RwLock<ProviderOperationWatchState>>,
}

#[derive(Debug, Default)]
struct ProviderOperationWatchState {
    revision: u64,
    entries: HashMap<Uuid, SharedProviderOperationWatchEntry>,
    removed_operation_versions: HashMap<Uuid, DateTime<Utc>>,
}

impl ProviderOperationWatchStore {
    pub async fn replace_all(&self, entries: Vec<ProviderOperationWatchEntry>) {
        let mut state = self.inner.write().await;
        replace_provider_operation_entries(&mut state, entries);
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.entries.len() as f64);
    }

    pub async fn replace_all_if_revision(
        &self,
        entries: Vec<ProviderOperationWatchEntry>,
        expected_revision: u64,
    ) -> bool {
        let mut state = self.inner.write().await;
        if state.revision != expected_revision {
            return false;
        }
        replace_provider_operation_entries(&mut state, entries);
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.entries.len() as f64);
        true
    }

    pub async fn revision(&self) -> u64 {
        self.inner.read().await.revision
    }

    pub async fn upsert(&self, entry: ProviderOperationWatchEntry) {
        let mut state = self.inner.write().await;
        upsert_provider_operation_entry_if_fresh(&mut state, entry);
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.entries.len() as f64);
    }

    pub async fn remove_with_source(
        &self,
        operation_id: Uuid,
        source_updated_at: Option<DateTime<Utc>>,
    ) {
        let mut state = self.inner.write().await;
        remove_provider_operation_entry_if_not_newer(
            &mut state,
            operation_id,
            source_updated_at.unwrap_or_else(utc::now),
        );
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.entries.len() as f64);
    }

    pub async fn remove(&self, operation_id: Uuid) {
        let mut state = self.inner.write().await;
        state.entries.remove(&operation_id);
        bump_revision(&mut state);
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.entries.len() as f64);
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.entries.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.entries.is_empty()
    }

    #[cfg(test)]
    async fn removed_operation_version_count(&self) -> usize {
        self.inner.read().await.removed_operation_versions.len()
    }

    pub async fn snapshot(&self) -> Vec<SharedProviderOperationWatchEntry> {
        self.inner.read().await.entries.values().cloned().collect()
    }
}

fn replace_provider_operation_entries(
    state: &mut ProviderOperationWatchState,
    entries: Vec<ProviderOperationWatchEntry>,
) {
    let previous_removed_operation_versions = std::mem::take(&mut state.removed_operation_versions);
    let mut retained_removed_operation_versions = HashMap::new();
    let entries = entries
        .into_iter()
        .filter_map(|entry| {
            if let Some(removed_at) = previous_removed_operation_versions.get(&entry.operation_id) {
                if *removed_at >= entry.updated_at {
                    retained_removed_operation_versions.insert(entry.operation_id, *removed_at);
                    return None;
                }
            }
            Some((entry.operation_id, Arc::new(entry)))
        })
        .collect::<HashMap<_, _>>();
    state.entries = entries;
    state.removed_operation_versions = retained_removed_operation_versions;
    bump_revision(state);
}

fn bump_revision(state: &mut ProviderOperationWatchState) {
    state.revision = state.revision.wrapping_add(1);
}

fn upsert_provider_operation_entry_if_fresh(
    state: &mut ProviderOperationWatchState,
    entry: ProviderOperationWatchEntry,
) {
    if let Some(existing) = state.entries.get(&entry.operation_id) {
        if existing.updated_at > entry.updated_at {
            return;
        }
    }
    if let Some(removed_at) = state.removed_operation_versions.get(&entry.operation_id) {
        if *removed_at >= entry.updated_at {
            return;
        }
    }

    state.removed_operation_versions.remove(&entry.operation_id);
    state.entries.insert(entry.operation_id, Arc::new(entry));
    bump_revision(state);
}

fn remove_provider_operation_entry_if_not_newer(
    state: &mut ProviderOperationWatchState,
    operation_id: Uuid,
    removed_at: DateTime<Utc>,
) {
    if let Some(existing) = state.entries.get(&operation_id) {
        if existing.updated_at > removed_at {
            return;
        }
    }

    state.entries.remove(&operation_id);
    state
        .removed_operation_versions
        .entry(operation_id)
        .and_modify(|existing| {
            if *existing < removed_at {
                *existing = removed_at;
            }
        })
        .or_insert(removed_at);
    bump_revision(state);
}

#[derive(Debug, Clone)]
pub struct ProviderOperationWatchRepository {
    pool: PgPool,
}

impl ProviderOperationWatchRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load_all(&self) -> Result<Vec<ProviderOperationWatchEntry>> {
        let started = std::time::Instant::now();
        let rows = sqlx_core::query::query(FULL_PROVIDER_OPERATION_WATCH_QUERY)
            .fetch_all(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_PROVIDER_OPERATION_LOAD_ALL_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        rows.into_iter().map(parse_provider_operation_row).collect()
    }

    pub async fn load_watch(
        &self,
        operation_id: Uuid,
    ) -> Result<Option<ProviderOperationWatchEntry>> {
        let started = std::time::Instant::now();
        let row = sqlx_core::query::query(TARGETED_PROVIDER_OPERATION_WATCH_QUERY)
            .bind(operation_id)
            .fetch_optional(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_PROVIDER_OPERATION_LOAD_WATCH_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        row.map(parse_provider_operation_row).transpose()
    }
}

pub async fn full_provider_operation_reconcile(
    store: &ProviderOperationWatchStore,
    repository: &ProviderOperationWatchRepository,
) -> Result<()> {
    let started = std::time::Instant::now();
    let expected_revision = store.revision().await;
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    let applied = store
        .replace_all_if_revision(watches, expected_revision)
        .await;
    histogram!(SAURON_PROVIDER_OPERATION_RECONCILE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    if applied {
        info!(
            watch_count,
            "Reconciled active Sauron provider-operation watch set from replica"
        );
    } else {
        warn!(
            watch_count,
            "Skipped stale Sauron provider-operation reconcile because CDC updated the watch set"
        );
    }
    Ok(())
}

fn parse_provider_operation_row(row: sqlx_postgres::PgRow) -> Result<ProviderOperationWatchEntry> {
    let operation_id_raw: String =
        row.try_get("operation_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: "missing provider operation id".to_string(),
            })?;
    let operation_id =
        Uuid::parse_str(&operation_id_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("provider operation id {operation_id_raw} was not a valid UUID"),
        })?;
    let execution_step_id_raw: String =
        row.try_get("execution_step_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing execution_step_id"),
            })?;
    let execution_step_id =
        Uuid::parse_str(&execution_step_id_raw).map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "provider operation {operation_id} execution_step_id {execution_step_id_raw} was not a valid UUID"
                ),
            }
        })?;
    let operation_type_raw: String =
        row.try_get("operation_type")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing operation_type"),
            })?;
    let operation_type =
        ProviderOperationType::from_db_string(&operation_type_raw).ok_or_else(|| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "provider operation {operation_id} has unsupported type {operation_type_raw}"
                ),
            }
        })?;
    let status_raw: String =
        row.try_get("status")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing status"),
            })?;
    let status = ProviderOperationStatus::from_db_string(&status_raw).ok_or_else(|| {
        crate::error::Error::InvalidWatchRow {
            message: format!(
                "provider operation {operation_id} has unsupported status {status_raw}"
            ),
        }
    })?;

    Ok(ProviderOperationWatchEntry {
        operation_id,
        execution_step_id: WorkflowStepId::from(execution_step_id),
        provider: row
            .try_get("provider")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing provider"),
            })?,
        operation_type,
        provider_ref: row.try_get("provider_ref").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing provider_ref"),
            }
        })?,
        status,
        request: row
            .try_get("request_json")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing request_json"),
            })?,
        response: row.try_get("response_json").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing response_json"),
            }
        })?,
        observed_state: row.try_get("observed_state_json").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing observed_state_json"),
            }
        })?,
        execution_step_request: row.try_get("execution_step_request_json").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "provider operation {operation_id} missing execution_step_request_json"
                ),
            }
        })?,
        updated_at: row.try_get("updated_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing updated_at"),
            }
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ProviderOperationWatchEntry, ProviderOperationWatchStore,
        FULL_PROVIDER_OPERATION_WATCH_QUERY, TARGETED_PROVIDER_OPERATION_WATCH_QUERY,
    };
    use chrono::Duration;
    use router_core::models::{ProviderOperationStatus, ProviderOperationType};
    use router_temporal::WorkflowStepId;
    use serde_json::json;
    use uuid::Uuid;

    fn watch(operation_id: Uuid) -> ProviderOperationWatchEntry {
        ProviderOperationWatchEntry {
            operation_id,
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "unit".to_string(),
            operation_type: ProviderOperationType::UnitWithdrawal,
            provider_ref: Some("unit-op-1".to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({}),
            execution_step_request: json!({}),
            updated_at: utc::now(),
        }
    }

    #[tokio::test]
    async fn provider_operation_watch_store_replaces_upserts_and_removes() {
        let store = ProviderOperationWatchStore::default();
        let first_id = Uuid::now_v7();
        let second_id = Uuid::now_v7();

        store.replace_all(vec![watch(first_id)]).await;
        assert_eq!(store.len().await, 1);

        store.upsert(watch(second_id)).await;
        assert_eq!(store.snapshot().await.len(), 2);

        store.remove(first_id).await;
        let remaining = store.snapshot().await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].operation_id, second_id);
    }

    #[test]
    fn provider_operation_watch_queries_select_step_and_filter_terminal_steps() {
        for query in [
            FULL_PROVIDER_OPERATION_WATCH_QUERY,
            TARGETED_PROVIDER_OPERATION_WATCH_QUERY,
        ] {
            assert!(query.contains("opo.execution_step_id::text AS execution_step_id"));
            assert!(query.contains("JOIN public.order_execution_steps oes"));
            assert!(query.contains("oes.status IN ('running', 'waiting')"));
        }
    }

    #[tokio::test]
    async fn provider_operation_watch_store_preserves_execution_step_id() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let entry = watch(operation_id);
        let step_id = entry.execution_step_id;

        store.replace_all(vec![entry]).await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].execution_step_id, step_id);
    }

    #[tokio::test]
    async fn upsert_ignores_older_provider_operation_versions() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let now = utc::now();
        let mut current = watch(operation_id);
        current.provider_ref = Some("current".to_string());
        current.updated_at = now;
        let mut stale = watch(operation_id);
        stale.provider_ref = Some("stale".to_string());
        stale.updated_at = now - Duration::seconds(1);

        store.upsert(current).await;
        store.upsert(stale).await;

        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].provider_ref.as_deref(), Some("current"));
    }

    #[tokio::test]
    async fn source_remove_does_not_delete_newer_provider_operation() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let now = utc::now();
        let mut current = watch(operation_id);
        current.updated_at = now + Duration::seconds(1);

        store.upsert(current).await;
        store.remove_with_source(operation_id, Some(now)).await;

        assert_eq!(store.len().await, 1);
    }

    #[tokio::test]
    async fn source_remove_blocks_stale_provider_operation_reinsertion() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let now = utc::now();
        let mut stale = watch(operation_id);
        stale.updated_at = now - Duration::seconds(1);

        store.remove_with_source(operation_id, Some(now)).await;
        store.upsert(stale).await;

        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn source_remove_blocks_stale_full_reconcile_reinsertion() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let now = utc::now();
        let mut stale = watch(operation_id);
        stale.updated_at = now - Duration::seconds(1);

        store.remove_with_source(operation_id, Some(now)).await;
        let snapshot_revision = store.revision().await;
        let applied = store
            .replace_all_if_revision(vec![stale], snapshot_revision)
            .await;

        assert!(applied);
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn full_reconcile_prunes_tombstones_for_absent_provider_operations() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let now = utc::now();

        store.remove_with_source(operation_id, Some(now)).await;
        assert_eq!(store.removed_operation_version_count().await, 1);

        let snapshot_revision = store.revision().await;
        let applied = store
            .replace_all_if_revision(Vec::new(), snapshot_revision)
            .await;

        assert!(applied);
        assert_eq!(store.removed_operation_version_count().await, 0);
    }

    #[tokio::test]
    async fn conditional_replace_all_skips_stale_reconcile_snapshots() {
        let store = ProviderOperationWatchStore::default();
        let first_id = Uuid::now_v7();
        let second_id = Uuid::now_v7();
        let snapshot_revision = store.revision().await;

        store.upsert(watch(first_id)).await;
        let applied = store
            .replace_all_if_revision(vec![watch(second_id)], snapshot_revision)
            .await;

        assert!(!applied);
        let snapshot = store.snapshot().await;
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].operation_id, first_id);
    }

    #[tokio::test]
    async fn remove_of_missing_operation_blocks_stale_reconcile_reinsertion() {
        let store = ProviderOperationWatchStore::default();
        let operation_id = Uuid::now_v7();
        let snapshot_revision = store.revision().await;

        store.remove(operation_id).await;
        let applied = store
            .replace_all_if_revision(vec![watch(operation_id)], snapshot_revision)
            .await;

        assert!(!applied);
        assert_eq!(store.len().await, 0);
    }
}
