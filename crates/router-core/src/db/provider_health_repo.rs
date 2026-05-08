use crate::{
    error::{RouterCoreError, RouterCoreResult},
    models::{ProviderHealthCheck, ProviderHealthStatus},
    telemetry,
};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;

const PROVIDER_HEALTH_SELECT_COLUMNS: &str = r#"
    provider,
    status,
    checked_at,
    latency_ms,
    http_status,
    error,
    updated_by,
    created_at,
    updated_at
"#;

#[derive(Clone)]
pub struct ProviderHealthRepository {
    pool: PgPool,
}

impl ProviderHealthRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> RouterCoreResult<Vec<ProviderHealthCheck>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_HEALTH_SELECT_COLUMNS}
            FROM provider_health_checks
            ORDER BY provider ASC
            "#
        ))
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query("provider_health.list", result.is_ok(), started.elapsed());
        let rows = result?;
        rows.iter().map(map_provider_health_row).collect()
    }

    pub async fn upsert(
        &self,
        check: &ProviderHealthCheck,
    ) -> RouterCoreResult<ProviderHealthCheck> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH upserted AS (
                INSERT INTO provider_health_checks (
                    provider,
                    status,
                    checked_at,
                    latency_ms,
                    http_status,
                    error,
                    updated_by,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (provider) DO UPDATE
                SET status = EXCLUDED.status,
                    checked_at = EXCLUDED.checked_at,
                    latency_ms = EXCLUDED.latency_ms,
                    http_status = EXCLUDED.http_status,
                    error = EXCLUDED.error,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                WHERE provider_health_checks.checked_at <= EXCLUDED.checked_at
                RETURNING {PROVIDER_HEALTH_SELECT_COLUMNS}
            )
            SELECT {PROVIDER_HEALTH_SELECT_COLUMNS}
            FROM upserted
            UNION ALL
            SELECT {PROVIDER_HEALTH_SELECT_COLUMNS}
            FROM provider_health_checks
            WHERE provider = $1
              AND NOT EXISTS (SELECT 1 FROM upserted)
            "#
        ))
        .bind(&check.provider)
        .bind(check.status.to_db_string())
        .bind(check.checked_at)
        .bind(check.latency_ms)
        .bind(check.http_status)
        .bind(&check.error)
        .bind(&check.updated_by)
        .bind(check.updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("provider_health.upsert", result.is_ok(), started.elapsed());
        let row = result?;
        map_provider_health_row(&row)
    }
}

fn map_provider_health_row(row: &sqlx_postgres::PgRow) -> RouterCoreResult<ProviderHealthCheck> {
    let status = row.get::<String, _>("status");
    let status = ProviderHealthStatus::from_db_string(&status).ok_or_else(|| {
        RouterCoreError::InvalidData {
            message: format!("unsupported provider health status: {status}"),
        }
    })?;

    Ok(ProviderHealthCheck {
        provider: row.get("provider"),
        status,
        checked_at: row.get("checked_at"),
        latency_ms: row.get("latency_ms"),
        http_status: row.get("http_status"),
        error: row.get("error"),
        updated_by: row.get("updated_by"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}
