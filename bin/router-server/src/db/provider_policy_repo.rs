use crate::{
    error::{RouterServerError, RouterServerResult},
    models::{ProviderExecutionPolicyState, ProviderPolicy, ProviderQuotePolicyState},
    telemetry,
};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;

const PROVIDER_POLICY_SELECT_COLUMNS: &str = r#"
    provider,
    quote_state,
    execution_state,
    reason,
    updated_by,
    updated_at
"#;

#[derive(Clone)]
pub struct ProviderPolicyRepository {
    pool: PgPool,
}

impl ProviderPolicyRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> RouterServerResult<Vec<ProviderPolicy>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {PROVIDER_POLICY_SELECT_COLUMNS}
            FROM provider_policies
            ORDER BY provider ASC
            "#
        ))
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query("provider_policy.list", result.is_ok(), started.elapsed());
        let rows = result?;
        rows.iter().map(map_provider_policy_row).collect()
    }

    pub async fn upsert(&self, policy: &ProviderPolicy) -> RouterServerResult<ProviderPolicy> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH upserted AS (
                INSERT INTO provider_policies (
                    provider,
                    quote_state,
                    execution_state,
                    reason,
                    updated_by,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (provider) DO UPDATE
                SET quote_state = EXCLUDED.quote_state,
                    execution_state = EXCLUDED.execution_state,
                    reason = EXCLUDED.reason,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                WHERE provider_policies.updated_at <= EXCLUDED.updated_at
                RETURNING {PROVIDER_POLICY_SELECT_COLUMNS}
            )
            SELECT {PROVIDER_POLICY_SELECT_COLUMNS}
            FROM upserted
            UNION ALL
            SELECT {PROVIDER_POLICY_SELECT_COLUMNS}
            FROM provider_policies
            WHERE provider = $1
              AND NOT EXISTS (SELECT 1 FROM upserted)
            "#
        ))
        .bind(&policy.provider)
        .bind(policy.quote_state.to_db_string())
        .bind(policy.execution_state.to_db_string())
        .bind(&policy.reason)
        .bind(&policy.updated_by)
        .bind(policy.updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("provider_policy.upsert", result.is_ok(), started.elapsed());
        let row = result?;
        map_provider_policy_row(&row)
    }
}

fn map_provider_policy_row(row: &sqlx_postgres::PgRow) -> RouterServerResult<ProviderPolicy> {
    let quote_state = row.get::<String, _>("quote_state");
    let quote_state = ProviderQuotePolicyState::from_db_string(&quote_state).ok_or_else(|| {
        RouterServerError::InvalidData {
            message: format!("unsupported provider quote_state: {quote_state}"),
        }
    })?;
    let execution_state = row.get::<String, _>("execution_state");
    let execution_state = ProviderExecutionPolicyState::from_db_string(&execution_state)
        .ok_or_else(|| RouterServerError::InvalidData {
            message: format!("unsupported provider execution_state: {execution_state}"),
        })?;
    Ok(ProviderPolicy {
        provider: row.get("provider"),
        quote_state,
        execution_state,
        reason: row.get("reason"),
        updated_by: row.get("updated_by"),
        updated_at: row.get("updated_at"),
    })
}
