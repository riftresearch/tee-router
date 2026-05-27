use crate::{
    error::{RouterCoreError, RouterCoreResult},
    models::{RouterSwitch, RouterSwitchName},
    telemetry,
};
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use std::time::Instant;

const ROUTER_SWITCH_SELECT_COLUMNS: &str = r#"
    name,
    enabled,
    reason,
    updated_by,
    updated_at
"#;

#[derive(Clone)]
pub struct RouterSwitchRepository {
    pool: PgPool,
}

impl RouterSwitchRepository {
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list(&self) -> RouterCoreResult<Vec<RouterSwitch>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ROUTER_SWITCH_SELECT_COLUMNS}
            FROM router_switches
            ORDER BY name ASC
            "#
        ))
        .fetch_all(&self.pool)
        .await;
        telemetry::record_db_query("router_switch.list", result.is_ok(), started.elapsed());
        let rows = result?;
        rows.iter().map(map_router_switch_row).collect()
    }

    pub async fn get(&self, name: RouterSwitchName) -> RouterCoreResult<Option<RouterSwitch>> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            SELECT {ROUTER_SWITCH_SELECT_COLUMNS}
            FROM router_switches
            WHERE name = $1
            "#
        ))
        .bind(name.to_db_string())
        .fetch_optional(&self.pool)
        .await;
        telemetry::record_db_query("router_switch.get", result.is_ok(), started.elapsed());
        let Some(row) = result? else {
            return Ok(None);
        };
        map_router_switch_row(&row).map(Some)
    }

    pub async fn get_or_default(&self, name: RouterSwitchName) -> RouterCoreResult<RouterSwitch> {
        Ok(self
            .get(name)
            .await?
            .unwrap_or_else(|| RouterSwitch::disabled(name)))
    }

    pub async fn refund_only_enabled(&self) -> RouterCoreResult<bool> {
        Ok(self
            .get_or_default(RouterSwitchName::RefundOnlyMode)
            .await?
            .enabled)
    }

    pub async fn upsert(&self, switch: &RouterSwitch) -> RouterCoreResult<RouterSwitch> {
        let started = Instant::now();
        let result = sqlx_core::query::query(&format!(
            r#"
            WITH upserted AS (
                INSERT INTO router_switches (
                    name,
                    enabled,
                    reason,
                    updated_by,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (name) DO UPDATE
                SET enabled = EXCLUDED.enabled,
                    reason = EXCLUDED.reason,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                WHERE router_switches.updated_at <= EXCLUDED.updated_at
                RETURNING {ROUTER_SWITCH_SELECT_COLUMNS}
            )
            SELECT {ROUTER_SWITCH_SELECT_COLUMNS}
            FROM upserted
            UNION ALL
            SELECT {ROUTER_SWITCH_SELECT_COLUMNS}
            FROM router_switches
            WHERE name = $1
              AND NOT EXISTS (SELECT 1 FROM upserted)
            "#
        ))
        .bind(switch.name.to_db_string())
        .bind(switch.enabled)
        .bind(&switch.reason)
        .bind(&switch.updated_by)
        .bind(switch.updated_at)
        .fetch_one(&self.pool)
        .await;
        telemetry::record_db_query("router_switch.upsert", result.is_ok(), started.elapsed());
        let row = result?;
        map_router_switch_row(&row)
    }
}

fn map_router_switch_row(row: &sqlx_postgres::PgRow) -> RouterCoreResult<RouterSwitch> {
    let name = row.get::<String, _>("name");
    let name =
        RouterSwitchName::from_db_string(&name).ok_or_else(|| RouterCoreError::InvalidData {
            message: format!("unsupported router switch name: {name}"),
        })?;
    Ok(RouterSwitch {
        name,
        enabled: row.get("enabled"),
        reason: row.get("reason"),
        updated_by: row.get("updated_by"),
        updated_at: row.get("updated_at"),
    })
}
