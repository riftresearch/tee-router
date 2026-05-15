use crate::{
    error::{RouterCoreError, RouterCoreResult},
    protocol::{AssetId, ChainId, DepositAsset},
    services::route_costs::RouteCostSnapshot,
    telemetry,
};
use chrono::{DateTime, Utc};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;

const ROUTE_COST_SELECT_COLUMNS: &str = r#"
    transition_id,
    amount_bucket,
    provider,
    edge_kind,
    source_chain,
    source_asset,
    destination_chain,
    destination_asset,
    estimated_fee_bps,
    estimated_gas_usd_micros,
    estimated_latency_ms,
    sample_amount_usd_micros,
    quote_source,
    refreshed_at,
    expires_at
"#;

#[derive(Clone)]
pub struct RouteCostRepository {
    pool: PgPool,
}

impl RouteCostRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn upsert_many(&self, snapshots: &[RouteCostSnapshot]) -> RouterCoreResult<()> {
        let started = Instant::now();
        let mut tx = self.pool.begin().await?;
        for snapshot in snapshots {
            let result = sqlx_core::query::query(
                r#"
                INSERT INTO router_route_cost_snapshots (
                    transition_id,
                    amount_bucket,
                    provider,
                    edge_kind,
                    source_chain,
                    source_asset,
                    destination_chain,
                    destination_asset,
                    estimated_fee_bps,
                    estimated_gas_usd_micros,
                    estimated_latency_ms,
                    sample_amount_usd_micros,
                    quote_source,
                    refreshed_at,
                    expires_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                ON CONFLICT (transition_id, amount_bucket) DO UPDATE
                SET provider = EXCLUDED.provider,
                    edge_kind = EXCLUDED.edge_kind,
                    source_chain = EXCLUDED.source_chain,
                    source_asset = EXCLUDED.source_asset,
                    destination_chain = EXCLUDED.destination_chain,
                    destination_asset = EXCLUDED.destination_asset,
                    estimated_fee_bps = EXCLUDED.estimated_fee_bps,
                    estimated_gas_usd_micros = EXCLUDED.estimated_gas_usd_micros,
                    estimated_latency_ms = EXCLUDED.estimated_latency_ms,
                    sample_amount_usd_micros = EXCLUDED.sample_amount_usd_micros,
                    quote_source = EXCLUDED.quote_source,
                    refreshed_at = EXCLUDED.refreshed_at,
                    expires_at = EXCLUDED.expires_at
                WHERE router_route_cost_snapshots.refreshed_at <= EXCLUDED.refreshed_at
                "#,
            )
            .bind(&snapshot.transition_id)
            .bind(&snapshot.amount_bucket)
            .bind(&snapshot.provider)
            .bind(&snapshot.edge_kind)
            .bind(snapshot.source_asset.chain.as_str())
            .bind(snapshot.source_asset.asset.as_str())
            .bind(snapshot.destination_asset.chain.as_str())
            .bind(snapshot.destination_asset.asset.as_str())
            .bind(u64_to_i64(snapshot.estimated_fee_bps, "estimated_fee_bps")?)
            .bind(u64_to_i64(
                snapshot.estimated_gas_usd_micros,
                "estimated_gas_usd_micros",
            )?)
            .bind(u64_to_i64(
                snapshot.estimated_latency_ms,
                "estimated_latency_ms",
            )?)
            .bind(u64_to_i64(
                snapshot.sample_amount_usd_micros,
                "sample_amount_usd_micros",
            )?)
            .bind(&snapshot.quote_source)
            .bind(snapshot.refreshed_at)
            .bind(snapshot.expires_at)
            .execute(&mut *tx)
            .await;
            if let Err(err) = result {
                telemetry::record_db_query("route_cost.upsert_many", false, started.elapsed());
                return Err(err.into());
            }
        }
        tx.commit().await?;
        telemetry::record_db_query("route_cost.upsert_many", true, started.elapsed());
        Ok(())
    }

    pub async fn list_active(
        &self,
        amount_bucket: &str,
        now: DateTime<Utc>,
    ) -> RouterCoreResult<Vec<RouteCostSnapshot>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ROUTE_COST_SELECT_COLUMNS}
            FROM router_route_cost_snapshots
            WHERE amount_bucket = $1
              AND expires_at > $2
            ORDER BY transition_id ASC
            "#
        ))
        .bind(amount_bucket)
        .bind(now)
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query("route_cost.list_active", result.is_ok(), started.elapsed());
        let rows = result?;
        rows.iter().map(map_route_cost_snapshot).collect()
    }
}

fn map_route_cost_snapshot(row: &sqlx_postgres::PgRow) -> RouterCoreResult<RouteCostSnapshot> {
    Ok(RouteCostSnapshot {
        transition_id: row.get("transition_id"),
        amount_bucket: row.get("amount_bucket"),
        provider: row.get("provider"),
        edge_kind: row.get("edge_kind"),
        source_asset: DepositAsset {
            chain: parse_chain(row.get::<String, _>("source_chain"))?,
            asset: parse_asset(row.get::<String, _>("source_asset"))?,
        },
        destination_asset: DepositAsset {
            chain: parse_chain(row.get::<String, _>("destination_chain"))?,
            asset: parse_asset(row.get::<String, _>("destination_asset"))?,
        },
        estimated_fee_bps: i64_to_u64(row.get("estimated_fee_bps"), "estimated_fee_bps")?,
        estimated_gas_usd_micros: i64_to_u64(
            row.get("estimated_gas_usd_micros"),
            "estimated_gas_usd_micros",
        )?,
        estimated_latency_ms: i64_to_u64(row.get("estimated_latency_ms"), "estimated_latency_ms")?,
        sample_amount_usd_micros: i64_to_u64(
            row.get("sample_amount_usd_micros"),
            "sample_amount_usd_micros",
        )?,
        quote_source: row.get("quote_source"),
        refreshed_at: row.get("refreshed_at"),
        expires_at: row.get("expires_at"),
    })
}

fn parse_chain(raw: String) -> RouterCoreResult<ChainId> {
    ChainId::parse(&raw).map_err(|reason| RouterCoreError::InvalidData {
        message: format!("invalid route cost chain {raw}: {reason}"),
    })
}

fn parse_asset(raw: String) -> RouterCoreResult<AssetId> {
    AssetId::parse(&raw).map_err(|reason| RouterCoreError::InvalidData {
        message: format!("invalid route cost asset {raw}: {reason}"),
    })
}

fn u64_to_i64(value: u64, field: &'static str) -> RouterCoreResult<i64> {
    i64::try_from(value).map_err(|_| RouterCoreError::InvalidData {
        message: format!("{field} exceeds i64 range"),
    })
}

fn i64_to_u64(value: i64, field: &'static str) -> RouterCoreResult<u64> {
    u64::try_from(value).map_err(|_| RouterCoreError::InvalidData {
        message: format!("{field} is negative"),
    })
}
