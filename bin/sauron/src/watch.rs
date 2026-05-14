use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use alloy::primitives::U256;
use bitcoin::Address as BitcoinAddress;
use chrono::{DateTime, Months, Utc};
use metrics::{counter, gauge, histogram};
use router_primitives::{normalize_evm_address, ChainType, TokenIdentifier};
use router_temporal::WorkflowStepId;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

use crate::error::{ReplicaDatabaseQuerySnafu, Result};

const FULL_WATCH_QUERY: &str = r#"
SELECT
  'provider_operation' AS watch_target,
  opo.id::text AS watch_id,
  opo.execution_step_id::text AS execution_step_id,
  opo.order_id::text AS order_id,
  opa.chain_id AS chain_id,
  opa.asset_id AS asset_id,
  opa.address AS address,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS required_amount,
  opa.created_at + INTERVAL '15 months' AS deposit_deadline,
  opa.created_at AS created_at,
  GREATEST(
    opa.updated_at,
    opo.updated_at,
    ro.updated_at,
    COALESCE(oes.updated_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.order_provider_addresses opa
JOIN public.order_provider_operations opo ON opo.id = opa.provider_operation_id
JOIN public.router_orders ro ON ro.id = opo.order_id
JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opa.role = 'unit_deposit'
  AND opo.operation_type = 'unit_deposit'
  AND opo.status IN ('submitted', 'waiting_external')
  AND oes.status IN ('running', 'waiting')
  AND opa.created_at >= $1
UNION ALL
SELECT
  'funding_vault' AS watch_target,
  dv.id::text AS watch_id,
  NULL::text AS execution_step_id,
  cv.order_id::text AS order_id,
  cv.chain_id AS chain_id,
  cv.asset_id AS asset_id,
  cv.address AS address,
  '1' AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(
    moq.amount_in,
    loq.input_amount,
    CASE
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_in'
        THEN dv.action->'payload'->>'amount_in'
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_out'
        THEN dv.action->'payload'->>'max_amount_in'
      WHEN dv.action->>'type' = 'limit_order'
        THEN dv.action->'payload'->>'input_amount'
      ELSE '1'
    END
  ) AS required_amount,
  dv.cancel_after AS deposit_deadline,
  dv.created_at AS created_at,
  GREATEST(
    dv.updated_at,
    cv.updated_at,
    COALESCE(moq.created_at, '-infinity'::timestamptz),
    COALESCE(loq.created_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.deposit_vaults dv
JOIN public.custody_vaults cv ON cv.id = dv.id
LEFT JOIN public.market_order_quotes moq ON moq.order_id = cv.order_id
LEFT JOIN public.limit_order_quotes loq ON loq.order_id = cv.order_id
WHERE dv.status = 'pending_funding'
  AND dv.created_at >= $1
ORDER BY updated_at ASC, watch_id ASC
"#;

const TARGETED_WATCH_QUERY: &str = r#"
SELECT
  'provider_operation' AS watch_target,
  opo.id::text AS watch_id,
  opo.execution_step_id::text AS execution_step_id,
  opo.order_id::text AS order_id,
  opa.chain_id AS chain_id,
  opa.asset_id AS asset_id,
  opa.address AS address,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS required_amount,
  opa.created_at + INTERVAL '15 months' AS deposit_deadline,
  opa.created_at AS created_at,
  GREATEST(
    opa.updated_at,
    opo.updated_at,
    ro.updated_at,
    COALESCE(oes.updated_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.order_provider_addresses opa
JOIN public.order_provider_operations opo ON opo.id = opa.provider_operation_id
JOIN public.router_orders ro ON ro.id = opo.order_id
JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opo.id = $1::uuid
  AND opa.role = 'unit_deposit'
  AND opo.operation_type = 'unit_deposit'
  AND opo.status IN ('submitted', 'waiting_external')
  AND oes.status IN ('running', 'waiting')
  AND opa.created_at >= $2
UNION ALL
SELECT
  'funding_vault' AS watch_target,
  dv.id::text AS watch_id,
  NULL::text AS execution_step_id,
  cv.order_id::text AS order_id,
  cv.chain_id AS chain_id,
  cv.asset_id AS asset_id,
  cv.address AS address,
  '1' AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(
    moq.amount_in,
    loq.input_amount,
    CASE
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_in'
        THEN dv.action->'payload'->>'amount_in'
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_out'
        THEN dv.action->'payload'->>'max_amount_in'
      WHEN dv.action->>'type' = 'limit_order'
        THEN dv.action->'payload'->>'input_amount'
      ELSE '1'
    END
  ) AS required_amount,
  dv.cancel_after AS deposit_deadline,
  dv.created_at AS created_at,
  GREATEST(
    dv.updated_at,
    cv.updated_at,
    COALESCE(moq.created_at, '-infinity'::timestamptz),
    COALESCE(loq.created_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.deposit_vaults dv
JOIN public.custody_vaults cv ON cv.id = dv.id
LEFT JOIN public.market_order_quotes moq ON moq.order_id = cv.order_id
LEFT JOIN public.limit_order_quotes loq ON loq.order_id = cv.order_id
WHERE dv.id = $1::uuid
  AND dv.status = 'pending_funding'
  AND dv.created_at >= $2
"#;

const ORDER_WATCH_QUERY: &str = r#"
SELECT
  'provider_operation' AS watch_target,
  opo.id::text AS watch_id,
  opo.execution_step_id::text AS execution_step_id,
  opo.order_id::text AS order_id,
  opa.chain_id AS chain_id,
  opa.asset_id AS asset_id,
  opa.address AS address,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS required_amount,
  opa.created_at + INTERVAL '15 months' AS deposit_deadline,
  opa.created_at AS created_at,
  GREATEST(
    opa.updated_at,
    opo.updated_at,
    ro.updated_at,
    COALESCE(oes.updated_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.order_provider_addresses opa
JOIN public.order_provider_operations opo ON opo.id = opa.provider_operation_id
JOIN public.router_orders ro ON ro.id = opo.order_id
JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opo.order_id = $1::uuid
  AND opa.role = 'unit_deposit'
  AND opo.operation_type = 'unit_deposit'
  AND opo.status IN ('submitted', 'waiting_external')
  AND oes.status IN ('running', 'waiting')
  AND opa.created_at >= $2
UNION ALL
SELECT
  'funding_vault' AS watch_target,
  dv.id::text AS watch_id,
  NULL::text AS execution_step_id,
  cv.order_id::text AS order_id,
  cv.chain_id AS chain_id,
  cv.asset_id AS asset_id,
  cv.address AS address,
  '1' AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(
    moq.amount_in,
    loq.input_amount,
    CASE
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_in'
        THEN dv.action->'payload'->>'amount_in'
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_out'
        THEN dv.action->'payload'->>'max_amount_in'
      WHEN dv.action->>'type' = 'limit_order'
        THEN dv.action->'payload'->>'input_amount'
      ELSE '1'
    END
  ) AS required_amount,
  dv.cancel_after AS deposit_deadline,
  dv.created_at AS created_at,
  GREATEST(
    dv.updated_at,
    cv.updated_at,
    COALESCE(moq.created_at, '-infinity'::timestamptz),
    COALESCE(loq.created_at, '-infinity'::timestamptz)
  ) AS updated_at
FROM public.deposit_vaults dv
JOIN public.custody_vaults cv ON cv.id = dv.id
LEFT JOIN public.market_order_quotes moq ON moq.order_id = cv.order_id
LEFT JOIN public.limit_order_quotes loq ON loq.order_id = cv.order_id
WHERE cv.order_id = $1::uuid
  AND dv.status = 'pending_funding'
  AND dv.created_at >= $2
"#;

const SAURON_WATCH_COUNT_METRIC: &str = "sauron_watch_count";
const SAURON_WATCH_FULL_RECONCILE_DURATION_SECONDS: &str =
    "sauron_watch_full_reconcile_duration_seconds";
const SAURON_WATCH_REPOSITORY_LOAD_ALL_DURATION_SECONDS: &str =
    "sauron_watch_repository_load_all_duration_seconds";
const SAURON_WATCH_REPOSITORY_LOAD_SWAP_DURATION_SECONDS: &str =
    "sauron_watch_repository_load_watch_duration_seconds";
const SAURON_WATCH_SNAPSHOT_DURATION_SECONDS: &str = "sauron_watch_snapshot_duration_seconds";
const SAURON_WATCH_EXPIRED_PRUNED_TOTAL: &str = "sauron_watch_expired_pruned_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WatchTarget {
    ProviderOperation,
    FundingVault,
}

impl WatchTarget {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProviderOperation => "provider_operation",
            Self::FundingVault => "funding_vault",
        }
    }

    fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "provider_operation" => Some(Self::ProviderOperation),
            "funding_vault" => Some(Self::FundingVault),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WatchEntry {
    pub watch_target: WatchTarget,
    pub watch_id: Uuid,
    pub execution_step_id: Option<WorkflowStepId>,
    pub order_id: Uuid,
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub address: String,
    pub min_amount: U256,
    pub max_amount: U256,
    pub required_amount: U256,
    pub deposit_deadline: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub type SharedWatchEntry = Arc<WatchEntry>;

#[derive(Debug, Default, Clone)]
pub struct WatchStore {
    inner: Arc<RwLock<WatchState>>,
}

#[derive(Debug, Default)]
struct WatchState {
    revision: u64,
    by_watch_id: HashMap<Uuid, SharedWatchEntry>,
    by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>>,
    by_order: HashMap<Uuid, HashMap<Uuid, SharedWatchEntry>>,
    removed_watch_versions: HashMap<Uuid, DateTime<Utc>>,
}

impl WatchStore {
    pub async fn replace_all(&self, entries: Vec<WatchEntry>) {
        let mut state = self.inner.write().await;
        replace_all_entries(&mut state, entries);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn replace_all_if_revision(
        &self,
        entries: Vec<WatchEntry>,
        expected_revision: u64,
    ) -> bool {
        let mut state = self.inner.write().await;
        if state.revision != expected_revision {
            return false;
        }
        replace_all_entries(&mut state, entries);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
        true
    }

    pub async fn revision(&self) -> u64 {
        self.inner.read().await.revision
    }

    pub async fn upsert(&self, entry: WatchEntry) {
        let mut state = self.inner.write().await;
        upsert_entry_if_fresh(&mut state, entry);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn replace_order(
        &self,
        order_id: Uuid,
        entries: Vec<WatchEntry>,
        source_updated_at: Option<DateTime<Utc>>,
    ) {
        let mut state = self.inner.write().await;
        let source_updated_at = source_updated_at.unwrap_or_else(utc::now);
        let existing_watch_ids = state
            .by_order
            .get(&order_id)
            .map(|entries| entries.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let replacement_watch_ids = entries
            .iter()
            .map(|entry| entry.watch_id)
            .collect::<std::collections::HashSet<_>>();

        for watch_id in existing_watch_ids {
            if !replacement_watch_ids.contains(&watch_id) {
                remove_entry_if_not_newer(&mut state, watch_id, source_updated_at);
            }
        }

        for entry in entries {
            upsert_entry_if_fresh(&mut state, entry);
        }
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn remove_with_source(
        &self,
        watch_id: Uuid,
        source_updated_at: Option<DateTime<Utc>>,
    ) {
        let mut state = self.inner.write().await;
        remove_entry_if_not_newer(
            &mut state,
            watch_id,
            source_updated_at.unwrap_or_else(utc::now),
        );
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn remove(&self, watch_id: Uuid) {
        let mut state = self.inner.write().await;
        remove_entry(&mut state, watch_id);
        bump_revision(&mut state);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.by_watch_id.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.by_watch_id.is_empty()
    }

    #[cfg(test)]
    async fn removed_watch_version_count(&self) -> usize {
        self.inner.read().await.removed_watch_versions.len()
    }

    pub async fn snapshot_for_chain(&self, chain: ChainType) -> Vec<SharedWatchEntry> {
        let started = Instant::now();
        let now = utc::now();
        let snapshot = self
            .inner
            .read()
            .await
            .by_chain
            .get(&chain)
            .into_iter()
            .flat_map(|entries| entries.values())
            .filter(|watch| is_watch_active(watch.as_ref(), now))
            .cloned()
            .collect::<Vec<_>>();
        histogram!(
            SAURON_WATCH_SNAPSHOT_DURATION_SECONDS,
            "chain" => chain.to_db_string().to_string(),
        )
        .record(started.elapsed().as_secs_f64());
        snapshot
    }

    pub async fn prune_expired(&self) -> usize {
        let now = utc::now();
        let mut state = self.inner.write().await;
        let expired_watch_ids = state
            .by_watch_id
            .iter()
            .filter_map(|(watch_id, watch)| {
                (!is_watch_active(watch.as_ref(), now)).then_some(*watch_id)
            })
            .collect::<Vec<_>>();

        for watch_id in &expired_watch_ids {
            remove_entry(&mut state, *watch_id);
        }

        if !expired_watch_ids.is_empty() {
            bump_revision(&mut state);
            counter!(SAURON_WATCH_EXPIRED_PRUNED_TOTAL).increment(expired_watch_ids.len() as u64);
        }
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
        expired_watch_ids.len()
    }
}

fn replace_all_entries(state: &mut WatchState, entries: Vec<WatchEntry>) {
    let mut by_watch_id = HashMap::with_capacity(entries.len());
    let mut by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>> = HashMap::new();
    let mut by_order: HashMap<Uuid, HashMap<Uuid, SharedWatchEntry>> = HashMap::new();
    let previous_removed_watch_versions = std::mem::take(&mut state.removed_watch_versions);
    let mut retained_removed_watch_versions = HashMap::new();
    for entry in entries {
        if let Some(removed_at) = previous_removed_watch_versions.get(&entry.watch_id) {
            if *removed_at >= entry.updated_at {
                retained_removed_watch_versions.insert(entry.watch_id, *removed_at);
                continue;
            }
        }
        let entry = Arc::new(entry);
        by_chain
            .entry(entry.source_chain)
            .or_default()
            .insert(entry.watch_id, entry.clone());
        by_order
            .entry(entry.order_id)
            .or_default()
            .insert(entry.watch_id, entry.clone());
        by_watch_id.insert(entry.watch_id, entry);
    }

    state.by_watch_id = by_watch_id;
    state.by_chain = by_chain;
    state.by_order = by_order;
    state.removed_watch_versions = retained_removed_watch_versions;
    bump_revision(state);
}

#[derive(Debug, Clone)]
pub struct WatchRepository {
    pool: PgPool,
}

impl WatchRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load_all(&self) -> Result<Vec<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff()?;
        let started = Instant::now();
        let rows = sqlx_core::query::query(FULL_WATCH_QUERY)
            .bind(cutoff_time)
            .fetch_all(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_WATCH_REPOSITORY_LOAD_ALL_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        rows.into_iter().map(parse_watch_row).collect()
    }

    pub async fn load_watch(&self, watch_id: Uuid) -> Result<Option<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff()?;
        let started = Instant::now();
        let row = sqlx_core::query::query(TARGETED_WATCH_QUERY)
            .bind(watch_id)
            .bind(cutoff_time)
            .fetch_optional(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;
        histogram!(SAURON_WATCH_REPOSITORY_LOAD_SWAP_DURATION_SECONDS)
            .record(started.elapsed().as_secs_f64());

        row.map(parse_watch_row).transpose()
    }

    pub async fn load_order_watches(&self, order_id: Uuid) -> Result<Vec<WatchEntry>> {
        let cutoff_time = deposit_vault_watch_cutoff()?;
        let rows = sqlx_core::query::query(ORDER_WATCH_QUERY)
            .bind(order_id)
            .bind(cutoff_time)
            .fetch_all(&self.pool)
            .await
            .context(ReplicaDatabaseQuerySnafu)?;

        rows.into_iter().map(parse_watch_row).collect()
    }
}

pub async fn full_reconcile(store: &WatchStore, repository: &WatchRepository) -> Result<()> {
    let started = Instant::now();
    let expected_revision = store.revision().await;
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    let applied = store
        .replace_all_if_revision(watches, expected_revision)
        .await;
    histogram!(SAURON_WATCH_FULL_RECONCILE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    if applied {
        info!(watch_count, "Reconciled full Sauron watch set from replica");
    } else {
        warn!(
            watch_count,
            "Skipped stale full Sauron watch reconcile because CDC updated the watch set"
        );
    }
    Ok(())
}

fn bump_revision(state: &mut WatchState) {
    state.revision = state.revision.wrapping_add(1);
}

fn upsert_entry_if_fresh(state: &mut WatchState, entry: WatchEntry) {
    if let Some(existing) = state.by_watch_id.get(&entry.watch_id) {
        if existing.updated_at > entry.updated_at {
            return;
        }
    }
    if let Some(removed_at) = state.removed_watch_versions.get(&entry.watch_id) {
        if *removed_at >= entry.updated_at {
            return;
        }
    }

    remove_entry(state, entry.watch_id);
    state.removed_watch_versions.remove(&entry.watch_id);

    let entry = Arc::new(entry);
    state
        .by_chain
        .entry(entry.source_chain)
        .or_default()
        .insert(entry.watch_id, entry.clone());
    state
        .by_order
        .entry(entry.order_id)
        .or_default()
        .insert(entry.watch_id, entry.clone());
    state.by_watch_id.insert(entry.watch_id, entry);
    bump_revision(state);
}

fn remove_entry_if_not_newer(state: &mut WatchState, watch_id: Uuid, removed_at: DateTime<Utc>) {
    if let Some(existing) = state.by_watch_id.get(&watch_id) {
        if existing.updated_at > removed_at {
            return;
        }
    }

    remove_entry(state, watch_id);
    state
        .removed_watch_versions
        .entry(watch_id)
        .and_modify(|existing| {
            if *existing < removed_at {
                *existing = removed_at;
            }
        })
        .or_insert(removed_at);
    bump_revision(state);
}

fn remove_entry(state: &mut WatchState, watch_id: Uuid) {
    let Some(previous) = state.by_watch_id.remove(&watch_id) else {
        return;
    };

    if let Some(chain_entries) = state.by_chain.get_mut(&previous.source_chain) {
        chain_entries.remove(&watch_id);
        if chain_entries.is_empty() {
            state.by_chain.remove(&previous.source_chain);
        }
    }

    if let Some(order_entries) = state.by_order.get_mut(&previous.order_id) {
        order_entries.remove(&watch_id);
        if order_entries.is_empty() {
            state.by_order.remove(&previous.order_id);
        }
    }
}

fn is_watch_active(watch: &WatchEntry, now: DateTime<Utc>) -> bool {
    watch.deposit_deadline > now
}

fn parse_watch_row(row: sqlx_postgres::PgRow) -> Result<WatchEntry> {
    let watch_id_raw =
        row.try_get::<String, _>("watch_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: "missing watch_id".to_string(),
            })?;
    let watch_id =
        Uuid::parse_str(&watch_id_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("watch_id {watch_id_raw} was not a valid UUID"),
        })?;
    let order_id_raw =
        row.try_get::<String, _>("order_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing order_id"),
            })?;
    let order_id =
        Uuid::parse_str(&order_id_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} order_id {order_id_raw} was not a valid UUID"),
        })?;
    let watch_target_raw: String =
        row.try_get("watch_target")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing watch_target"),
            })?;
    let watch_target = WatchTarget::from_db_string(&watch_target_raw).ok_or_else(|| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had unsupported watch_target {watch_target_raw}"),
        }
    })?;
    let execution_step_id_raw: Option<String> =
        row.try_get("execution_step_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing execution_step_id"),
            })?;
    let execution_step_id = match (watch_target, execution_step_id_raw) {
        (WatchTarget::ProviderOperation, Some(value)) => {
            let step_id = Uuid::parse_str(&value).map_err(|_| {
                crate::error::Error::InvalidWatchRow {
                    message: format!(
                        "provider-operation watch {watch_id} execution_step_id {value} was not a valid UUID"
                    ),
                }
            })?;
            Some(WorkflowStepId::from(step_id))
        }
        (WatchTarget::ProviderOperation, None) => {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!("provider-operation watch {watch_id} missing execution_step_id"),
            });
        }
        (WatchTarget::FundingVault, Some(value)) => {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "funding-vault watch {watch_id} unexpectedly had execution_step_id {value}"
                ),
            });
        }
        (WatchTarget::FundingVault, None) => None,
    };

    let chain_id_raw: String =
        row.try_get("chain_id")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing chain_id"),
            })?;
    let source_chain = chain_type_from_router_chain_id(&chain_id_raw).ok_or_else(|| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had unsupported chain_id {chain_id_raw}"),
        }
    })?;

    let asset_id = row.try_get::<Option<String>, _>("asset_id").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing asset_id"),
        }
    })?;
    let source_token =
        token_identifier_from_router_asset_id(watch_id, source_chain, asset_id.as_deref())?;

    let address_raw =
        row.try_get::<String, _>("address")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing address"),
            })?;
    let address = normalize_watch_address(watch_id, source_chain, &address_raw)?;

    let min_amount_raw = row.try_get::<String, _>("min_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing min_amount"),
        }
    })?;
    let min_amount = parse_positive_watch_amount(watch_id, "min_amount", &min_amount_raw)?;

    let max_amount_raw = row.try_get::<String, _>("max_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing max_amount"),
        }
    })?;
    let max_amount = parse_positive_watch_amount(watch_id, "max_amount", &max_amount_raw)?;
    let required_amount_raw = row.try_get::<String, _>("required_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing required_amount"),
        }
    })?;
    let required_amount =
        parse_positive_watch_amount(watch_id, "required_amount", &required_amount_raw)?;
    validate_watch_amount_bounds(watch_id, min_amount, max_amount, required_amount)?;

    Ok(WatchEntry {
        watch_target,
        watch_id,
        execution_step_id,
        order_id,
        source_chain,
        source_token: source_token.normalize(),
        address,
        min_amount,
        max_amount,
        required_amount,
        deposit_deadline: row.try_get("deposit_deadline").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing deposit_deadline"),
            }
        })?,
        created_at: row.try_get("created_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing created_at"),
            }
        })?,
        updated_at: row.try_get("updated_at").map_err(|_| {
            crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing updated_at"),
            }
        })?,
    })
}

fn parse_positive_watch_amount(watch_id: Uuid, field: &str, raw: &str) -> Result<U256> {
    if raw.is_empty() || !raw.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had invalid {field}"),
        });
    }
    let amount = U256::from_str(raw).map_err(|_| crate::error::Error::InvalidWatchRow {
        message: format!("watch {watch_id} had invalid {field}"),
    })?;
    if amount == U256::ZERO {
        return Err(crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had non-positive {field}"),
        });
    }
    Ok(amount)
}

fn validate_watch_amount_bounds(
    watch_id: Uuid,
    min_amount: U256,
    max_amount: U256,
    required_amount: U256,
) -> Result<()> {
    if min_amount > max_amount {
        return Err(crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had min_amount greater than max_amount"),
        });
    }
    if required_amount < min_amount || required_amount > max_amount {
        return Err(crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had required_amount outside min/max bounds"),
        });
    }
    Ok(())
}

fn normalize_watch_address(watch_id: Uuid, chain: ChainType, address: &str) -> Result<String> {
    match chain {
        ChainType::Bitcoin => BitcoinAddress::from_str(address)
            .map(|_| address.to_string())
            .map_err(|reason| crate::error::Error::InvalidWatchRow {
                message: format!(
                    "watch {watch_id} had invalid Bitcoin watch address {address}: {reason}"
                ),
            }),
        ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base | ChainType::Hyperliquid => {
            normalize_evm_address(address).map_err(|reason| crate::error::Error::InvalidWatchRow {
                message: format!(
                    "watch {watch_id} had invalid EVM watch address {address}: {reason}"
                ),
            })
        }
    }
}

fn chain_type_from_router_chain_id(chain_id: &str) -> Option<ChainType> {
    match chain_id {
        "bitcoin" => Some(ChainType::Bitcoin),
        "evm:1" | "ethereum" => Some(ChainType::Ethereum),
        "evm:42161" | "arbitrum" => Some(ChainType::Arbitrum),
        "evm:8453" | "base" => Some(ChainType::Base),
        _ => None,
    }
}

fn token_identifier_from_router_asset_id(
    watch_id: Uuid,
    chain: ChainType,
    asset_id: Option<&str>,
) -> Result<TokenIdentifier> {
    match asset_id {
        None | Some("native") => Ok(TokenIdentifier::Native),
        Some(asset_id) => match chain {
            ChainType::Bitcoin => Err(crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} had non-native Bitcoin asset_id {asset_id}"),
            }),
            ChainType::Ethereum
            | ChainType::Arbitrum
            | ChainType::Base
            | ChainType::Hyperliquid => normalize_evm_address(asset_id)
                .map(TokenIdentifier::Address)
                .map_err(|reason| crate::error::Error::InvalidWatchRow {
                    message: format!(
                        "watch {watch_id} had invalid EVM asset_id {asset_id}: {reason}"
                    ),
                }),
        },
    }
}

fn deposit_vault_watch_cutoff() -> Result<DateTime<Utc>> {
    utc::now()
        .checked_sub_months(Months::new(15))
        .ok_or_else(|| crate::error::Error::InvalidConfiguration {
            message: "15-month deposit vault watch cutoff was outside supported datetime range"
                .to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::{
        is_watch_active, normalize_watch_address, parse_positive_watch_amount,
        token_identifier_from_router_asset_id, validate_watch_amount_bounds, WatchEntry,
        WatchStore, WatchTarget, FULL_WATCH_QUERY, ORDER_WATCH_QUERY, TARGETED_WATCH_QUERY,
    };
    use alloy::primitives::U256;
    use chrono::Duration;
    use router_primitives::{ChainType, TokenIdentifier};
    use router_temporal::WorkflowStepId;
    use uuid::Uuid;

    fn watch_entry(
        watch_id: Uuid,
        source_chain: ChainType,
        address: &str,
        deposit_deadline: chrono::DateTime<chrono::Utc>,
    ) -> WatchEntry {
        WatchEntry {
            watch_target: WatchTarget::ProviderOperation,
            watch_id,
            execution_step_id: Some(WorkflowStepId::from(watch_id)),
            order_id: watch_id,
            source_chain,
            source_token: match source_chain {
                ChainType::Bitcoin => TokenIdentifier::Native,
                ChainType::Ethereum
                | ChainType::Arbitrum
                | ChainType::Base
                | ChainType::Hyperliquid => {
                    TokenIdentifier::address("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf")
                }
            },
            address: address.to_string(),
            min_amount: U256::from(1_u64),
            max_amount: U256::from(10_u64),
            required_amount: U256::from(10_u64),
            deposit_deadline,
            created_at: utc::now(),
            updated_at: utc::now(),
        }
    }

    fn funding_vault_watch_entry(
        watch_id: Uuid,
        source_chain: ChainType,
        address: &str,
        deposit_deadline: chrono::DateTime<chrono::Utc>,
    ) -> WatchEntry {
        WatchEntry {
            watch_target: WatchTarget::FundingVault,
            execution_step_id: None,
            ..watch_entry(watch_id, source_chain, address, deposit_deadline)
        }
    }

    #[test]
    fn provider_operation_deposit_watch_queries_select_step_and_filter_terminal_steps() {
        for query in [FULL_WATCH_QUERY, TARGETED_WATCH_QUERY, ORDER_WATCH_QUERY] {
            assert!(query.contains("opo.execution_step_id::text AS execution_step_id"));
            assert!(query.contains("JOIN public.order_execution_steps oes"));
            assert!(query.contains("oes.status IN ('running', 'waiting')"));
        }
    }

    #[test]
    fn provider_operation_deposit_watch_queries_use_provider_operation_lifetime_for_deadline() {
        let operation_expiry_column = ["opa", ".expires_at"].concat();
        for query in [FULL_WATCH_QUERY, TARGETED_WATCH_QUERY, ORDER_WATCH_QUERY] {
            assert!(query.contains("opa.created_at + INTERVAL '15 months' AS deposit_deadline"));
            assert!(!query.contains("ro.action_timeout_at"));
            assert!(!query.contains(&operation_expiry_column));
        }
    }

    #[test]
    fn funding_vault_watch_queries_use_cancel_after_for_deadline() {
        for query in [FULL_WATCH_QUERY, TARGETED_WATCH_QUERY, ORDER_WATCH_QUERY] {
            assert!(query.contains("dv.cancel_after AS deposit_deadline"));
        }
    }

    #[test]
    fn router_asset_tokens_are_normalized_for_evm_watches() {
        let token = token_identifier_from_router_asset_id(
            Uuid::now_v7(),
            ChainType::Base,
            Some("0xCbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"),
        )
        .unwrap();

        assert_eq!(
            token,
            TokenIdentifier::Address("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf".to_string())
        );
    }

    #[test]
    fn router_asset_tokens_reject_invalid_evm_watches() {
        let err = token_identifier_from_router_asset_id(
            Uuid::now_v7(),
            ChainType::Ethereum,
            Some("not-an-address"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("invalid EVM asset_id"));
    }

    #[test]
    fn watch_addresses_are_normalized_and_validated() {
        let watch_id = Uuid::now_v7();
        assert_eq!(
            normalize_watch_address(
                watch_id,
                ChainType::Base,
                "0xCbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
            )
            .unwrap(),
            "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"
        );

        let evm_err =
            normalize_watch_address(watch_id, ChainType::Ethereum, "not-an-address").unwrap_err();
        assert!(evm_err.to_string().contains("invalid EVM watch address"));

        let bitcoin_err =
            normalize_watch_address(watch_id, ChainType::Bitcoin, "not-a-bitcoin-address")
                .unwrap_err();
        assert!(bitcoin_err
            .to_string()
            .contains("invalid Bitcoin watch address"));
    }

    #[test]
    fn watch_amounts_must_be_positive_decimal_integers() {
        let watch_id = Uuid::now_v7();

        assert_eq!(
            parse_positive_watch_amount(watch_id, "required_amount", "00042").unwrap(),
            U256::from(42_u64)
        );
        for raw in ["", "0", "00", "1.0", "-1", "abc"] {
            assert!(
                parse_positive_watch_amount(watch_id, "required_amount", raw).is_err(),
                "{raw:?} must be rejected"
            );
        }
    }

    #[test]
    fn watch_amount_bounds_must_be_consistent() {
        let watch_id = Uuid::now_v7();

        validate_watch_amount_bounds(
            watch_id,
            U256::from(1_u64),
            U256::from(10_u64),
            U256::from(5_u64),
        )
        .expect("valid bounds");

        assert!(validate_watch_amount_bounds(
            watch_id,
            U256::from(11_u64),
            U256::from(10_u64),
            U256::from(11_u64),
        )
        .is_err());
        assert!(validate_watch_amount_bounds(
            watch_id,
            U256::from(2_u64),
            U256::from(10_u64),
            U256::from(1_u64),
        )
        .is_err());
        assert!(validate_watch_amount_bounds(
            watch_id,
            U256::from(1_u64),
            U256::from(10_u64),
            U256::from(11_u64),
        )
        .is_err());
    }

    #[tokio::test]
    async fn snapshot_for_chain_returns_only_active_watches_for_that_chain() {
        let store = WatchStore::default();
        let now = utc::now();
        store
            .replace_all(vec![
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "btc-address",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Ethereum,
                    "0x0000000000000000000000000000000000000001",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "expired-btc-address",
                    now - Duration::minutes(1),
                ),
            ])
            .await;

        let bitcoin_watches = store.snapshot_for_chain(ChainType::Bitcoin).await;
        let ethereum_watches = store.snapshot_for_chain(ChainType::Ethereum).await;

        assert_eq!(bitcoin_watches.len(), 1);
        assert_eq!(bitcoin_watches[0].address, "btc-address");
        assert_eq!(ethereum_watches.len(), 1);
        assert_eq!(
            ethereum_watches[0].address,
            "0x0000000000000000000000000000000000000001"
        );
    }

    #[tokio::test]
    async fn upsert_replaces_existing_entry_and_remove_deletes_it() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();

        store
            .replace_all(vec![watch_entry(
                watch_id,
                ChainType::Bitcoin,
                "btc-address",
                now + Duration::minutes(5),
            )])
            .await;

        store
            .upsert(watch_entry(
                watch_id,
                ChainType::Ethereum,
                "0x0000000000000000000000000000000000000002",
                now + Duration::minutes(10),
            ))
            .await;

        assert!(store
            .snapshot_for_chain(ChainType::Bitcoin)
            .await
            .is_empty());
        let ethereum_watches = store.snapshot_for_chain(ChainType::Ethereum).await;
        assert_eq!(ethereum_watches.len(), 1);
        assert_eq!(
            ethereum_watches[0].address,
            "0x0000000000000000000000000000000000000002"
        );

        store.remove(watch_id).await;
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn upsert_ignores_older_watch_versions() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let mut current = watch_entry(
            watch_id,
            ChainType::Ethereum,
            "0x0000000000000000000000000000000000000001",
            now + Duration::minutes(5),
        );
        current.updated_at = now;
        let mut stale = watch_entry(
            watch_id,
            ChainType::Ethereum,
            "0x0000000000000000000000000000000000000002",
            now + Duration::minutes(5),
        );
        stale.updated_at = now - Duration::seconds(1);

        store.upsert(current).await;
        store.upsert(stale).await;

        let ethereum_watches = store.snapshot_for_chain(ChainType::Ethereum).await;
        assert_eq!(ethereum_watches.len(), 1);
        assert_eq!(
            ethereum_watches[0].address,
            "0x0000000000000000000000000000000000000001"
        );
    }

    #[tokio::test]
    async fn source_remove_does_not_delete_newer_watch() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let mut current = watch_entry(
            watch_id,
            ChainType::Base,
            "0x0000000000000000000000000000000000000003",
            now + Duration::minutes(5),
        );
        current.updated_at = now + Duration::seconds(1);

        store.upsert(current).await;
        store.remove_with_source(watch_id, Some(now)).await;

        assert_eq!(store.len().await, 1);
        assert_eq!(store.snapshot_for_chain(ChainType::Base).await.len(), 1);
    }

    #[tokio::test]
    async fn source_remove_blocks_stale_order_reinsertion() {
        let store = WatchStore::default();
        let order_id = Uuid::now_v7();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let mut stale = watch_entry(
            watch_id,
            ChainType::Bitcoin,
            "stale-btc-address",
            now + Duration::minutes(5),
        );
        stale.order_id = order_id;
        stale.updated_at = now - Duration::seconds(1);

        store.remove_with_source(watch_id, Some(now)).await;
        store.replace_order(order_id, vec![stale], Some(now)).await;

        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn source_remove_blocks_stale_full_reconcile_reinsertion() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let mut stale = watch_entry(
            watch_id,
            ChainType::Bitcoin,
            "stale-btc-address",
            now + Duration::minutes(5),
        );
        stale.updated_at = now - Duration::seconds(1);

        store.remove_with_source(watch_id, Some(now)).await;
        let snapshot_revision = store.revision().await;
        let applied = store
            .replace_all_if_revision(vec![stale], snapshot_revision)
            .await;

        assert!(applied);
        assert_eq!(store.len().await, 0);
        assert_eq!(store.snapshot_for_chain(ChainType::Bitcoin).await.len(), 0);
    }

    #[tokio::test]
    async fn full_reconcile_prunes_tombstones_for_absent_watches() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();

        store.remove_with_source(watch_id, Some(now)).await;
        assert_eq!(store.removed_watch_version_count().await, 1);

        let snapshot_revision = store.revision().await;
        let applied = store
            .replace_all_if_revision(Vec::new(), snapshot_revision)
            .await;

        assert!(applied);
        assert_eq!(store.removed_watch_version_count().await, 0);
    }

    #[tokio::test]
    async fn replace_order_does_not_remove_newer_existing_watch() {
        let store = WatchStore::default();
        let order_id = Uuid::now_v7();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let mut current = watch_entry(
            watch_id,
            ChainType::Arbitrum,
            "0x0000000000000000000000000000000000000004",
            now + Duration::minutes(5),
        );
        current.order_id = order_id;
        current.updated_at = now + Duration::seconds(1);

        store.upsert(current).await;
        store.replace_order(order_id, Vec::new(), Some(now)).await;

        assert_eq!(store.len().await, 1);
        assert_eq!(store.snapshot_for_chain(ChainType::Arbitrum).await.len(), 1);
    }

    #[tokio::test]
    async fn conditional_replace_all_skips_stale_reconcile_snapshots() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let snapshot_revision = store.revision().await;

        store
            .upsert(watch_entry(
                Uuid::now_v7(),
                ChainType::Ethereum,
                "0x0000000000000000000000000000000000000004",
                now + Duration::minutes(5),
            ))
            .await;

        let applied = store
            .replace_all_if_revision(
                vec![watch_entry(
                    watch_id,
                    ChainType::Bitcoin,
                    "stale-btc-address",
                    now + Duration::minutes(5),
                )],
                snapshot_revision,
            )
            .await;

        assert!(!applied);
        assert_eq!(store.snapshot_for_chain(ChainType::Bitcoin).await.len(), 0);
        assert_eq!(store.snapshot_for_chain(ChainType::Ethereum).await.len(), 1);
    }

    #[tokio::test]
    async fn remove_of_missing_watch_blocks_stale_reconcile_reinsertion() {
        let store = WatchStore::default();
        let watch_id = Uuid::now_v7();
        let now = utc::now();
        let snapshot_revision = store.revision().await;

        store.remove(watch_id).await;
        let applied = store
            .replace_all_if_revision(
                vec![watch_entry(
                    watch_id,
                    ChainType::Bitcoin,
                    "removed-btc-address",
                    now + Duration::minutes(5),
                )],
                snapshot_revision,
            )
            .await;

        assert!(!applied);
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn prune_expired_removes_expired_watches_from_store() {
        let store = WatchStore::default();
        let now = utc::now();

        store
            .replace_all(vec![
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Bitcoin,
                    "btc-address",
                    now + Duration::minutes(5),
                ),
                watch_entry(
                    Uuid::now_v7(),
                    ChainType::Base,
                    "0x0000000000000000000000000000000000000003",
                    now - Duration::minutes(1),
                ),
            ])
            .await;

        let pruned = store.prune_expired().await;

        assert_eq!(pruned, 1);
        assert_eq!(store.len().await, 1);
        assert!(store.snapshot_for_chain(ChainType::Base).await.is_empty());
    }

    #[tokio::test]
    async fn prune_expired_keeps_provider_watch_when_operation_expired_but_order_alive() {
        let store = WatchStore::default();
        let now = utc::now();
        let operation_expires_at = now - Duration::minutes(1);
        let provider_operation_deadline = now + Duration::minutes(5);
        assert!(operation_expires_at < now);

        store
            .replace_all(vec![watch_entry(
                Uuid::now_v7(),
                ChainType::Base,
                "0x0000000000000000000000000000000000000004",
                provider_operation_deadline,
            )])
            .await;

        let pruned = store.prune_expired().await;

        assert_eq!(pruned, 0);
        assert_eq!(store.len().await, 1);
        assert_eq!(store.snapshot_for_chain(ChainType::Base).await.len(), 1);
    }

    #[test]
    fn provider_operation_watch_survives_action_timeout() {
        let now = utc::now();
        let action_timeout_at = now - Duration::minutes(1);
        let created_at = now - Duration::minutes(10);
        let provider_operation_deadline = created_at
            .checked_add_months(chrono::Months::new(15))
            .expect("provider-operation TTL should be representable");
        assert!(action_timeout_at < now);
        assert!(provider_operation_deadline > now);

        let mut watch = watch_entry(
            Uuid::now_v7(),
            ChainType::Base,
            "0x0000000000000000000000000000000000000005",
            provider_operation_deadline,
        );
        watch.created_at = created_at;

        assert!(is_watch_active(&watch, now));
    }

    #[tokio::test]
    async fn funding_vault_watch_respects_cancel_after() {
        let store = WatchStore::default();
        let now = utc::now();

        store
            .replace_all(vec![funding_vault_watch_entry(
                Uuid::now_v7(),
                ChainType::Base,
                "0x0000000000000000000000000000000000000006",
                now - Duration::minutes(1),
            )])
            .await;

        let pruned = store.prune_expired().await;

        assert_eq!(pruned, 1);
        assert_eq!(store.len().await, 0);
        assert!(store.snapshot_for_chain(ChainType::Base).await.is_empty());
    }

    #[tokio::test]
    async fn prune_expired_removes_provider_watch_after_provider_deadline() {
        let store = WatchStore::default();
        let now = utc::now();

        store
            .replace_all(vec![watch_entry(
                Uuid::now_v7(),
                ChainType::Base,
                "0x0000000000000000000000000000000000000007",
                now - Duration::minutes(1),
            )])
            .await;

        let pruned = store.prune_expired().await;

        assert_eq!(pruned, 1);
        assert_eq!(store.len().await, 0);
        assert!(store.snapshot_for_chain(ChainType::Base).await.is_empty());
    }
}
