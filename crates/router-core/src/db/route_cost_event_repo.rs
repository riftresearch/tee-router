use crate::{
    error::RouterCoreResult, services::route_costs::RouteCostSampleEvent, telemetry,
};
use chrono::Utc;
use sqlx_postgres::PgPool;
use std::time::Instant;

/// Activity-feed rows older than this are pruned on every write. Comfortably
/// larger than the refresh window (default 30 min) so the dashboard can always
/// render a full window of history, but small enough that the table stays tiny.
const RETENTION_SECONDS: i64 = 60 * 60;

#[derive(Clone)]
pub struct RouteCostEventRepository {
    pool: PgPool,
}

impl RouteCostEventRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert a batch of sample events and prune anything older than the
    /// retention horizon. Best-effort: callers should log and continue on
    /// error rather than failing the refresh cycle.
    pub async fn record_many(&self, events: &[RouteCostSampleEvent]) -> RouterCoreResult<()> {
        if events.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let mut tx = self.pool.begin().await?;
        for event in events {
            let result = sqlx_core::query::query(
                r#"
                INSERT INTO router_route_cost_sample_events (
                    sampled_at,
                    provider,
                    transition_id,
                    amount_bucket,
                    edge_kind,
                    source_chain,
                    source_asset,
                    destination_chain,
                    destination_asset,
                    sample_amount_usd_micros,
                    outcome,
                    estimated_fee_bps,
                    estimated_latency_ms,
                    reason,
                    failure_category
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                "#,
            )
            .bind(event.sampled_at)
            .bind(&event.provider)
            .bind(&event.transition_id)
            .bind(&event.amount_bucket)
            .bind(&event.edge_kind)
            .bind(event.source_asset.chain.as_str())
            .bind(event.source_asset.asset.as_str())
            .bind(event.destination_asset.chain.as_str())
            .bind(event.destination_asset.asset.as_str())
            .bind(sample_amount_to_i64(event.sample_amount_usd_micros))
            .bind(event.outcome.as_str())
            .bind(event.estimated_fee_bps.and_then(u64_to_i64))
            .bind(event.estimated_latency_ms.and_then(u64_to_i64))
            .bind(event.reason.as_deref())
            .bind(event.failure_category.map(|category| category.as_str()))
            .execute(&mut *tx)
            .await;
            if let Err(err) = result {
                telemetry::record_db_query(
                    "route_cost_event.record_many",
                    false,
                    started.elapsed(),
                );
                return Err(err.into());
            }
        }

        let prune_before = Utc::now() - chrono::Duration::seconds(RETENTION_SECONDS);
        let prune = sqlx_core::query::query(
            r#"
            DELETE FROM router_route_cost_sample_events
            WHERE sampled_at < $1
            "#,
        )
        .bind(prune_before)
        .execute(&mut *tx)
        .await;
        if let Err(err) = prune {
            telemetry::record_db_query("route_cost_event.record_many", false, started.elapsed());
            return Err(err.into());
        }

        tx.commit().await?;
        telemetry::record_db_query("route_cost_event.record_many", true, started.elapsed());
        Ok(())
    }
}

/// Sample amounts are always positive and well within `i64`, but clamp defensively
/// so an impossible value can never panic the writer.
fn sample_amount_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn u64_to_i64(value: u64) -> Option<i64> {
    i64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use crate::services::route_costs::RouteCostSampleOutcome;

    #[test]
    fn outcome_round_trips_to_db_text() {
        assert_eq!(RouteCostSampleOutcome::Succeeded.as_str(), "succeeded");
        assert_eq!(RouteCostSampleOutcome::Failed.as_str(), "failed");
        assert_eq!(RouteCostSampleOutcome::Skipped.as_str(), "skipped");
    }
}
