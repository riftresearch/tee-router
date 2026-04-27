use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use alloy::primitives::U256;
use chrono::{DateTime, Months, Utc};
use metrics::{counter, gauge, histogram};
use router_primitives::{normalize_evm_address, ChainType, TokenIdentifier};
use serde::Deserialize;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::{PgNotification, PgPool};
use tokio::sync::RwLock;
use tracing::info;
use uuid::Uuid;

use crate::error::{NotificationPayloadSnafu, ReplicaDatabaseQuerySnafu, Result};

const FULL_WATCH_QUERY: &str = r#"
SELECT
  'provider_operation' AS watch_target,
  opo.id::text AS watch_id,
  opa.chain_id AS chain_id,
  opa.asset_id AS asset_id,
  opa.address AS address,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS required_amount,
  COALESCE(opa.expires_at, ro.action_timeout_at, opa.created_at + INTERVAL '15 months') AS deposit_deadline,
  opa.created_at AS created_at,
  GREATEST(opa.updated_at, opo.updated_at) AS updated_at
FROM public.order_provider_addresses opa
JOIN public.order_provider_operations opo ON opo.id = opa.provider_operation_id
JOIN public.router_orders ro ON ro.id = opo.order_id
LEFT JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opa.role = 'unit_deposit'
  AND opo.operation_type = 'unit_deposit'
  AND opo.status IN ('submitted', 'waiting_external')
  AND opa.created_at >= $1
UNION ALL
SELECT
  'funding_vault' AS watch_target,
  dv.id::text AS watch_id,
  cv.chain_id AS chain_id,
  cv.asset_id AS asset_id,
  cv.address AS address,
  '1' AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(
    moq.amount_in,
    CASE
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_in'
        THEN dv.action->'payload'->>'amount_in'
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_out'
        THEN dv.action->'payload'->>'max_amount_in'
      ELSE '1'
    END
  ) AS required_amount,
  dv.cancel_after AS deposit_deadline,
  dv.created_at AS created_at,
  GREATEST(dv.updated_at, cv.updated_at) AS updated_at
FROM public.deposit_vaults dv
JOIN public.custody_vaults cv ON cv.id = dv.id
LEFT JOIN public.market_order_quotes moq ON moq.order_id = cv.order_id
WHERE dv.status = 'pending_funding'
  AND dv.created_at >= $1
ORDER BY updated_at ASC, watch_id ASC
"#;

const TARGETED_WATCH_QUERY: &str = r#"
SELECT
  'provider_operation' AS watch_target,
  opo.id::text AS watch_id,
  opa.chain_id AS chain_id,
  opa.asset_id AS asset_id,
  opa.address AS address,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(opo.request_json->>'expected_amount', oes.amount_in, '1') AS required_amount,
  COALESCE(opa.expires_at, ro.action_timeout_at, opa.created_at + INTERVAL '15 months') AS deposit_deadline,
  opa.created_at AS created_at,
  GREATEST(opa.updated_at, opo.updated_at) AS updated_at
FROM public.order_provider_addresses opa
JOIN public.order_provider_operations opo ON opo.id = opa.provider_operation_id
JOIN public.router_orders ro ON ro.id = opo.order_id
LEFT JOIN public.order_execution_steps oes ON oes.id = opo.execution_step_id
WHERE opo.id = $1::uuid
  AND opa.role = 'unit_deposit'
  AND opo.operation_type = 'unit_deposit'
  AND opo.status IN ('submitted', 'waiting_external')
  AND opa.created_at >= $2
UNION ALL
SELECT
  'funding_vault' AS watch_target,
  dv.id::text AS watch_id,
  cv.chain_id AS chain_id,
  cv.asset_id AS asset_id,
  cv.address AS address,
  '1' AS min_amount,
  '115792089237316195423570985008687907853269984665640564039457584007913129639935' AS max_amount,
  COALESCE(
    moq.amount_in,
    CASE
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_in'
        THEN dv.action->'payload'->>'amount_in'
      WHEN dv.action->>'type' = 'market_order'
        AND dv.action->'payload'->>'kind' = 'exact_out'
        THEN dv.action->'payload'->>'max_amount_in'
      ELSE '1'
    END
  ) AS required_amount,
  dv.cancel_after AS deposit_deadline,
  dv.created_at AS created_at,
  GREATEST(dv.updated_at, cv.updated_at) AS updated_at
FROM public.deposit_vaults dv
JOIN public.custody_vaults cv ON cv.id = dv.id
LEFT JOIN public.market_order_quotes moq ON moq.order_id = cv.order_id
WHERE dv.id = $1::uuid
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
    by_watch_id: HashMap<Uuid, SharedWatchEntry>,
    by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>>,
}

impl WatchStore {
    pub async fn replace_all(&self, entries: Vec<WatchEntry>) {
        let mut state = self.inner.write().await;
        let mut by_watch_id = HashMap::with_capacity(entries.len());
        let mut by_chain: HashMap<ChainType, HashMap<Uuid, SharedWatchEntry>> = HashMap::new();

        for entry in entries {
            let entry = Arc::new(entry);
            by_chain
                .entry(entry.source_chain)
                .or_default()
                .insert(entry.watch_id, entry.clone());
            by_watch_id.insert(entry.watch_id, entry);
        }

        state.by_watch_id = by_watch_id;
        state.by_chain = by_chain;
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn upsert(&self, entry: WatchEntry) {
        let mut state = self.inner.write().await;
        remove_entry(&mut state, entry.watch_id);

        let entry = Arc::new(entry);
        state
            .by_chain
            .entry(entry.source_chain)
            .or_default()
            .insert(entry.watch_id, entry.clone());
        state.by_watch_id.insert(entry.watch_id, entry);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn remove(&self, watch_id: Uuid) {
        let mut state = self.inner.write().await;
        remove_entry(&mut state, watch_id);
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
    }

    pub async fn len(&self) -> usize {
        self.inner.read().await.by_watch_id.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.inner.read().await.by_watch_id.is_empty()
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
            counter!(SAURON_WATCH_EXPIRED_PRUNED_TOTAL).increment(expired_watch_ids.len() as u64);
        }
        gauge!(SAURON_WATCH_COUNT_METRIC).set(state.by_watch_id.len() as f64);
        expired_watch_ids.len()
    }
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
        let cutoff_time = deposit_vault_watch_cutoff();
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
        let cutoff_time = deposit_vault_watch_cutoff();
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WatchChangeNotification {
    pub watch_id: Uuid,
}

impl WatchChangeNotification {
    pub fn parse(notification: &PgNotification) -> Result<Self> {
        serde_json::from_str(notification.payload()).context(NotificationPayloadSnafu)
    }
}

pub async fn full_reconcile(store: &WatchStore, repository: &WatchRepository) -> Result<()> {
    let started = Instant::now();
    let watches = repository.load_all().await?;
    let watch_count = watches.len();
    store.replace_all(watches).await;
    histogram!(SAURON_WATCH_FULL_RECONCILE_DURATION_SECONDS)
        .record(started.elapsed().as_secs_f64());
    info!(watch_count, "Reconciled full Sauron watch set from replica");
    Ok(())
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

    let address = normalize_address(
        source_chain,
        &row.try_get::<String, _>("address")
            .map_err(|_| crate::error::Error::InvalidWatchRow {
                message: format!("watch {watch_id} missing address"),
            })?,
    );

    let min_amount_raw = row.try_get::<String, _>("min_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing min_amount"),
        }
    })?;
    let min_amount =
        U256::from_str(&min_amount_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had invalid min_amount"),
        })?;

    let max_amount_raw = row.try_get::<String, _>("max_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing max_amount"),
        }
    })?;
    let max_amount =
        U256::from_str(&max_amount_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had invalid max_amount"),
        })?;
    let required_amount_raw = row.try_get::<String, _>("required_amount").map_err(|_| {
        crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} missing required_amount"),
        }
    })?;
    let required_amount =
        U256::from_str(&required_amount_raw).map_err(|_| crate::error::Error::InvalidWatchRow {
            message: format!("watch {watch_id} had invalid required_amount"),
        })?;

    Ok(WatchEntry {
        watch_target,
        watch_id,
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

fn normalize_address(chain: ChainType, address: &str) -> String {
    match chain {
        ChainType::Bitcoin => address.to_string(),
        ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base | ChainType::Hyperliquid => {
            address.to_lowercase()
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

fn deposit_vault_watch_cutoff() -> DateTime<Utc> {
    utc::now()
        .checked_sub_months(Months::new(15))
        .expect("15-month deposit vault watch cutoff should stay within supported datetime range")
}

#[cfg(test)]
mod tests {
    use super::{token_identifier_from_router_asset_id, WatchEntry, WatchStore, WatchTarget};
    use alloy::primitives::U256;
    use chrono::Duration;
    use router_primitives::{ChainType, TokenIdentifier};
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
}
