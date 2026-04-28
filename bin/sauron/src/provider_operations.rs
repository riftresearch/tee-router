use std::{collections::HashMap, sync::Arc};

use chrono::{DateTime, Utc};
use metrics::{gauge, histogram};
use router_server::models::{ProviderOperationStatus, ProviderOperationType};
use serde_json::Value;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::error::{ReplicaDatabaseQuerySnafu, Result};

const FULL_PROVIDER_OPERATION_WATCH_QUERY: &str = r#"
SELECT
  id::text AS operation_id,
  provider,
  operation_type,
  provider_ref,
  status,
  request_json,
  response_json,
  observed_state_json,
  updated_at
FROM public.order_provider_operations
WHERE operation_type IN ('across_bridge', 'cctp_bridge', 'hyperliquid_bridge_deposit', 'unit_deposit', 'unit_withdrawal', 'hyperliquid_trade')
  AND status IN ('submitted', 'waiting_external')
ORDER BY updated_at ASC, id ASC
"#;

const TARGETED_PROVIDER_OPERATION_WATCH_QUERY: &str = r#"
SELECT
  id::text AS operation_id,
  provider,
  operation_type,
  provider_ref,
  status,
  request_json,
  response_json,
  observed_state_json,
  updated_at
FROM public.order_provider_operations
WHERE id = $1::uuid
  AND operation_type IN ('across_bridge', 'cctp_bridge', 'hyperliquid_bridge_deposit', 'unit_deposit', 'unit_withdrawal', 'hyperliquid_trade')
  AND status IN ('submitted', 'waiting_external')
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
    pub provider: String,
    pub operation_type: ProviderOperationType,
    pub provider_ref: Option<String>,
    pub status: ProviderOperationStatus,
    pub request: Value,
    pub response: Value,
    pub observed_state: Value,
    pub updated_at: DateTime<Utc>,
}

pub type SharedProviderOperationWatchEntry = Arc<ProviderOperationWatchEntry>;

#[derive(Debug, Default, Clone)]
pub struct ProviderOperationWatchStore {
    inner: Arc<RwLock<HashMap<Uuid, SharedProviderOperationWatchEntry>>>,
}

impl ProviderOperationWatchStore {
    pub async fn replace_all(&self, entries: Vec<ProviderOperationWatchEntry>) {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.operation_id, Arc::new(entry)))
            .collect::<HashMap<_, _>>();
        let mut state = self.inner.write().await;
        *state = entries;
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.len() as f64);
    }

    pub async fn upsert(&self, entry: ProviderOperationWatchEntry) {
        let mut state = self.inner.write().await;
        state.insert(entry.operation_id, Arc::new(entry));
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.len() as f64);
    }

    pub async fn remove(&self, operation_id: Uuid) {
        let mut state = self.inner.write().await;
        state.remove(&operation_id);
        gauge!(SAURON_PROVIDER_OPERATION_WATCH_COUNT_METRIC).set(state.len() as f64);
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.is_empty()
    }

    pub async fn snapshot(&self) -> Vec<SharedProviderOperationWatchEntry> {
        self.inner.read().await.values().cloned().collect()
    }
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
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    store.replace_all(watches).await;
    histogram!(SAURON_PROVIDER_OPERATION_RECONCILE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    info!(
        watch_count,
        "Reconciled active Sauron provider-operation watch set from replica"
    );
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
        updated_at: row.try_get("updated_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("provider operation {operation_id} missing updated_at"),
            }
        })?,
    })
}

#[cfg(test)]
mod tests {
    use super::{ProviderOperationWatchEntry, ProviderOperationWatchStore};
    use router_server::models::{ProviderOperationStatus, ProviderOperationType};
    use serde_json::json;
    use uuid::Uuid;

    fn watch(operation_id: Uuid) -> ProviderOperationWatchEntry {
        ProviderOperationWatchEntry {
            operation_id,
            provider: "unit".to_string(),
            operation_type: ProviderOperationType::UnitWithdrawal,
            provider_ref: Some("unit-op-1".to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({}),
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
}
