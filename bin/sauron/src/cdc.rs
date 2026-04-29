use std::time::Duration;

use metrics::{counter, gauge};
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tracing::{info, warn};

use crate::error::{
    InvalidConfigurationSnafu, ReplicaDatabaseQuerySnafu, Result, StateDatabaseQuerySnafu,
};

const SAURON_CDC_CHANGES_TOTAL: &str = "sauron_cdc_changes_total";
const SAURON_CDC_RELEVANT_BATCHES_TOTAL: &str = "sauron_cdc_relevant_batches_total";
const SAURON_CDC_LAST_BATCH_SIZE: &str = "sauron_cdc_last_batch_size";

const WATCH_TABLES: &[&str] = &[
    "order_provider_operations",
    "order_provider_addresses",
    "deposit_vaults",
    "custody_vaults",
    "market_order_quotes",
    "order_execution_steps",
    "router_orders",
];

#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub slot_name: String,
    pub plugin: String,
    pub batch_size: i32,
    pub poll_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcChange {
    pub lsn: String,
    pub xid: Option<i64>,
    pub data: String,
}

#[derive(Debug, Clone)]
pub struct RouterCdcRepository {
    router_pool: PgPool,
    state_pool: PgPool,
    config: CdcConfig,
}

impl RouterCdcRepository {
    pub fn new(router_pool: PgPool, state_pool: PgPool, config: CdcConfig) -> Self {
        Self {
            router_pool,
            state_pool,
            config,
        }
    }

    pub async fn ensure_slot(&self) -> Result<()> {
        let existing = sqlx_core::query::query(
            r#"
            SELECT plugin
            FROM pg_catalog.pg_replication_slots
            WHERE slot_name = $1
            "#,
        )
        .bind(&self.config.slot_name)
        .fetch_optional(&self.router_pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        if let Some(row) = existing {
            let plugin: Option<String> =
                row.try_get("plugin").context(ReplicaDatabaseQuerySnafu)?;
            if plugin.as_deref() != Some(self.config.plugin.as_str()) {
                return InvalidConfigurationSnafu {
                    message: format!(
                        "CDC slot {} uses plugin {:?}, expected {}",
                        self.config.slot_name, plugin, self.config.plugin
                    ),
                }
                .fail();
            }

            info!(
                slot_name = %self.config.slot_name,
                plugin = %self.config.plugin,
                "Sauron CDC logical slot already exists"
            );
            return Ok(());
        }

        sqlx_core::query::query(
            r#"
            SELECT slot_name
            FROM pg_catalog.pg_create_logical_replication_slot($1::name, $2::name)
            "#,
        )
        .bind(&self.config.slot_name)
        .bind(&self.config.plugin)
        .fetch_one(&self.router_pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        info!(
            slot_name = %self.config.slot_name,
            plugin = %self.config.plugin,
            "Created Sauron CDC logical slot"
        );
        Ok(())
    }

    pub async fn fetch_changes(&self) -> Result<Vec<CdcChange>> {
        let rows = sqlx_core::query::query(
            r#"
            SELECT lsn::text AS lsn, xid::text AS xid, data
            FROM pg_catalog.pg_logical_slot_get_changes($1::name, NULL, $2)
            "#,
        )
        .bind(&self.config.slot_name)
        .bind(self.config.batch_size)
        .fetch_all(&self.router_pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        gauge!(SAURON_CDC_LAST_BATCH_SIZE).set(rows.len() as f64);
        if !rows.is_empty() {
            counter!(SAURON_CDC_CHANGES_TOTAL).increment(rows.len() as u64);
        }

        rows.into_iter()
            .map(|row| {
                let xid = row
                    .try_get::<String, _>("xid")
                    .context(ReplicaDatabaseQuerySnafu)?
                    .parse::<i64>()
                    .ok();
                Ok(CdcChange {
                    lsn: row.try_get("lsn").context(ReplicaDatabaseQuerySnafu)?,
                    xid,
                    data: row.try_get("data").context(ReplicaDatabaseQuerySnafu)?,
                })
            })
            .collect()
    }

    pub async fn save_checkpoint(&self, last_change: &CdcChange, batch_size: usize) -> Result<()> {
        let batch_size =
            i64::try_from(batch_size).map_err(|_| crate::error::Error::InvalidConfiguration {
                message: "CDC batch size exceeded i64 range".to_string(),
            })?;

        sqlx_core::query::query(
            r#"
            INSERT INTO public.sauron_cdc_checkpoints (
                consumer,
                slot_name,
                last_lsn,
                last_xid,
                last_batch_size,
                updated_at
            )
            VALUES ('sauron-watch-set', $1, $2, $3, $4, now())
            ON CONFLICT (consumer)
            DO UPDATE SET
                slot_name = EXCLUDED.slot_name,
                last_lsn = EXCLUDED.last_lsn,
                last_xid = EXCLUDED.last_xid,
                last_batch_size = EXCLUDED.last_batch_size,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&self.config.slot_name)
        .bind(&last_change.lsn)
        .bind(last_change.xid)
        .bind(batch_size)
        .execute(&self.state_pool)
        .await
        .context(StateDatabaseQuerySnafu)?;

        Ok(())
    }

    pub fn poll_interval(&self) -> Duration {
        self.config.poll_interval
    }
}

pub fn batch_touches_watch_tables(changes: &[CdcChange]) -> bool {
    let touched = changes.iter().any(|change| {
        let table = watch_table_from_test_decoding(&change.data);
        if let Some(table) = table {
            info!(table, "Sauron CDC observed router watch-table change");
            true
        } else {
            false
        }
    });
    if touched {
        counter!(SAURON_CDC_RELEVANT_BATCHES_TOTAL).increment(1);
    }
    touched
}

fn watch_table_from_test_decoding(data: &str) -> Option<&'static str> {
    WATCH_TABLES
        .iter()
        .copied()
        .find(|table| data.starts_with(&format!("table public.{table}:")))
}

pub fn validate_cdc_config(config: &CdcConfig) -> Result<()> {
    if config.slot_name.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_SLOT_NAME must not be empty".to_string(),
        }
        .fail();
    }
    if config.plugin.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_PLUGIN must not be empty".to_string(),
        }
        .fail();
    }
    if config.batch_size <= 0 {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_BATCH_SIZE must be positive".to_string(),
        }
        .fail();
    }
    if config.poll_interval.is_zero() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_POLL_INTERVAL_MS must be positive".to_string(),
        }
        .fail();
    }
    if config.plugin != "test_decoding" {
        warn!(
            plugin = %config.plugin,
            "Sauron CDC table filtering currently understands test_decoding output"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{batch_touches_watch_tables, CdcChange};

    #[test]
    fn detects_watch_table_changes_from_test_decoding_output() {
        let changes = vec![
            CdcChange {
                lsn: "0/16B6C50".to_string(),
                xid: Some(42),
                data: "BEGIN 42".to_string(),
            },
            CdcChange {
                lsn: "0/16B6D30".to_string(),
                xid: Some(42),
                data: "table public.deposit_vaults: UPDATE: id[uuid]:'00000000-0000-0000-0000-000000000001'"
                    .to_string(),
            },
        ];

        assert!(batch_touches_watch_tables(&changes));
    }

    #[test]
    fn ignores_unrelated_test_decoding_output() {
        let changes = vec![CdcChange {
            lsn: "0/16B6C50".to_string(),
            xid: Some(42),
            data: "table public.unrelated: INSERT: id[integer]:1".to_string(),
        }];

        assert!(!batch_touches_watch_tables(&changes));
    }
}
