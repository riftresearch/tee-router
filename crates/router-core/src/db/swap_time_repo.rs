use crate::{
    error::{RouterCoreError, RouterCoreResult},
    models::{SwapTimeAverage, SwapTimeRouteKey},
    telemetry,
};
use sqlx_core::{query_builder::QueryBuilder, row::Row};
use sqlx_postgres::{PgPool, Postgres};
use std::time::Instant;

const SWAP_TIME_AVERAGE_SELECT_COLUMNS: &str = r#"
    provider,
    leg_type,
    transition_decl_id,
    input_chain_id,
    input_asset_id,
    output_chain_id,
    output_asset_id,
    sample_count,
    avg_duration_ms,
    min_duration_ms,
    max_duration_ms,
    last_sample_at
"#;

#[derive(Clone)]
pub struct SwapTimeRepository {
    pool: PgPool,
}

impl SwapTimeRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_averages_for_keys(
        &self,
        keys: &[SwapTimeRouteKey],
    ) -> RouterCoreResult<Vec<SwapTimeAverage>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let started = Instant::now();
        let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
            r#"
            SELECT
                "#,
        );
        builder.push(SWAP_TIME_AVERAGE_SELECT_COLUMNS);
        builder.push(
            r#"
            FROM public.router_swap_time_averages
            WHERE (
                provider,
                leg_type,
                transition_decl_id,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id
            ) IN (
            "#,
        );

        let mut first_key = true;
        for key in keys {
            if first_key {
                first_key = false;
            } else {
                builder.push(", ");
            }

            builder
                .push("(")
                .push_bind(&key.provider)
                .push(", ")
                .push_bind(&key.leg_type)
                .push(", ")
                .push_bind(&key.transition_decl_id)
                .push(", ")
                .push_bind(&key.input_chain_id)
                .push(", ")
                .push_bind(&key.input_asset_id)
                .push(", ")
                .push_bind(&key.output_chain_id)
                .push(", ")
                .push_bind(&key.output_asset_id)
                .push(")");
        }
        builder.push(")");

        let result = builder.build().fetch_all(&self.pool).await;
        telemetry::record_db_query(
            "swap_time.get_averages_for_keys",
            result.is_ok(),
            started.elapsed(),
        );
        let rows = result?;
        rows.iter().map(map_swap_time_average_row).collect()
    }
}

fn map_swap_time_average_row(row: &sqlx_postgres::PgRow) -> RouterCoreResult<SwapTimeAverage> {
    Ok(SwapTimeAverage {
        key: SwapTimeRouteKey {
            provider: row.get("provider"),
            leg_type: row.get("leg_type"),
            transition_decl_id: row.get("transition_decl_id"),
            input_chain_id: row.get("input_chain_id"),
            input_asset_id: row.get("input_asset_id"),
            output_chain_id: row.get("output_chain_id"),
            output_asset_id: row.get("output_asset_id"),
        },
        sample_count: i64_to_u64(row.get("sample_count"), "sample_count")?,
        avg_duration_ms: i64_to_u64(row.get("avg_duration_ms"), "avg_duration_ms")?,
        min_duration_ms: i64_to_u64(row.get("min_duration_ms"), "min_duration_ms")?,
        max_duration_ms: i64_to_u64(row.get("max_duration_ms"), "max_duration_ms")?,
        last_sample_at: row.get("last_sample_at"),
    })
}

fn i64_to_u64(value: i64, field: &'static str) -> RouterCoreResult<u64> {
    u64::try_from(value).map_err(|_| RouterCoreError::InvalidData {
        message: format!("{field} is negative"),
    })
}
