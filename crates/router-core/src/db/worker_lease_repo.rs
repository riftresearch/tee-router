use crate::{error::RouterCoreResult, telemetry};
use chrono::{DateTime, Utc};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLease {
    pub lease_name: String,
    pub owner_id: String,
    pub expires_at: DateTime<Utc>,
    pub fencing_token: i64,
}

#[derive(Clone)]
pub struct WorkerLeaseRepository {
    pool: PgPool,
}

impl WorkerLeaseRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn try_acquire(
        &self,
        lease_name: &str,
        owner_id: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> RouterCoreResult<Option<WorkerLease>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            INSERT INTO router_worker_leases (
                lease_name,
                owner_id,
                expires_at,
                fencing_token,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, 1, $4, $4)
            ON CONFLICT (lease_name) DO UPDATE
            SET
                owner_id = EXCLUDED.owner_id,
                expires_at = EXCLUDED.expires_at,
                fencing_token = router_worker_leases.fencing_token + 1,
                updated_at = EXCLUDED.updated_at
            WHERE router_worker_leases.expires_at <= $4
            RETURNING
                lease_name,
                owner_id,
                expires_at,
                fencing_token
            "#,
        )
        .bind(lease_name)
        .bind(owner_id)
        .bind(expires_at)
        .bind(now)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query(
            "worker_lease.try_acquire",
            result.is_ok(),
            started.elapsed(),
        );
        let row = result?;

        row.map(map_worker_lease).transpose()
    }

    pub async fn renew(
        &self,
        lease_name: &str,
        owner_id: &str,
        fencing_token: i64,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> RouterCoreResult<Option<WorkerLease>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(
            r#"
            UPDATE router_worker_leases
            SET
                expires_at = $5,
                updated_at = $4
            WHERE lease_name = $1
              AND owner_id = $2
              AND fencing_token = $3
              AND expires_at > $4
            RETURNING
                lease_name,
                owner_id,
                expires_at,
                fencing_token
            "#,
        )
        .bind(lease_name)
        .bind(owner_id)
        .bind(fencing_token)
        .bind(now)
        .bind(expires_at)
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query("worker_lease.renew", result.is_ok(), started.elapsed());
        let row = result?;

        row.map(map_worker_lease).transpose()
    }
}

fn map_worker_lease(row: sqlx_postgres::PgRow) -> RouterCoreResult<WorkerLease> {
    Ok(WorkerLease {
        lease_name: row.get("lease_name"),
        owner_id: row.get("owner_id"),
        expires_at: row.get("expires_at"),
        fencing_token: row.get("fencing_token"),
    })
}
