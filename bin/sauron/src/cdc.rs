use std::time::Duration;

use metrics::{counter, gauge};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use serde::Deserialize;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tracing::info;
use url::Url;
use uuid::Uuid;

use crate::error::{
    CdcPayloadSnafu, CdcStreamSnafu, InvalidConfigurationSnafu, ReplicaDatabaseQuerySnafu, Result,
    StateDatabaseQuerySnafu,
};

pub const ROUTER_CDC_MESSAGE_PREFIX: &str = "rift.router.change";

const SAURON_CDC_EVENTS_TOTAL: &str = "sauron_cdc_events_total";
const SAURON_CDC_LOGICAL_MESSAGES_TOTAL: &str = "sauron_cdc_logical_messages_total";
const SAURON_CDC_LAST_COMMIT_LSN: &str = "sauron_cdc_last_commit_lsn";

#[derive(Debug, Clone)]
pub struct CdcConfig {
    pub database_url: String,
    pub slot_name: String,
    pub publication_name: String,
    pub message_prefix: String,
    pub status_interval: Duration,
    pub idle_wakeup_interval: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RouterCdcMessage {
    pub version: u16,
    pub schema: String,
    pub table: String,
    pub op: String,
    pub id: Option<Uuid>,
    pub order_id: Option<Uuid>,
    pub watch_id: Option<Uuid>,
    pub provider_operation_id: Option<Uuid>,
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
            if plugin.as_deref() != Some("pgoutput") {
                return InvalidConfigurationSnafu {
                    message: format!(
                        "CDC slot {} uses plugin {:?}, expected pgoutput",
                        self.config.slot_name, plugin
                    ),
                }
                .fail();
            }

            info!(
                slot_name = %self.config.slot_name,
                publication = %self.config.publication_name,
                "Sauron CDC logical replication slot already exists"
            );
            return Ok(());
        }

        sqlx_core::query::query(
            r#"
            SELECT slot_name
            FROM pg_catalog.pg_create_logical_replication_slot($1::name, 'pgoutput')
            "#,
        )
        .bind(&self.config.slot_name)
        .fetch_one(&self.router_pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        info!(
            slot_name = %self.config.slot_name,
            publication = %self.config.publication_name,
            "Created Sauron CDC logical replication slot"
        );
        Ok(())
    }

    pub fn replication_config(&self) -> Result<ReplicationConfig> {
        replication_config_from_database_url(
            &self.config.database_url,
            &self.config.slot_name,
            &self.config.publication_name,
            self.config.status_interval,
            self.config.idle_wakeup_interval,
        )
    }

    pub async fn save_checkpoint(&self, lsn: Lsn) -> Result<()> {
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
            VALUES ('sauron-watch-set', $1, $2, NULL, 0, now())
            ON CONFLICT (consumer)
            DO UPDATE SET
                slot_name = EXCLUDED.slot_name,
                last_lsn = EXCLUDED.last_lsn,
                last_xid = NULL,
                last_batch_size = 0,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(&self.config.slot_name)
        .bind(lsn.to_pg_string())
        .execute(&self.state_pool)
        .await
        .context(StateDatabaseQuerySnafu)?;

        gauge!(SAURON_CDC_LAST_COMMIT_LSN).set(lsn.as_u64() as f64);
        Ok(())
    }

    pub fn message_prefix(&self) -> &str {
        &self.config.message_prefix
    }
}

pub async fn connect_stream(config: ReplicationConfig) -> Result<ReplicationClient> {
    ReplicationClient::connect(config)
        .await
        .context(CdcStreamSnafu)
}

pub async fn recv_stream_event(client: &mut ReplicationClient) -> Result<Option<ReplicationEvent>> {
    client.recv().await.context(CdcStreamSnafu)
}

pub fn parse_router_cdc_message(content: &[u8]) -> Result<RouterCdcMessage> {
    serde_json::from_slice(content).context(CdcPayloadSnafu)
}

pub fn validate_cdc_config(config: &CdcConfig) -> Result<()> {
    if config.slot_name.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_SLOT_NAME must not be empty".to_string(),
        }
        .fail();
    }
    if config.publication_name.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "ROUTER_CDC_PUBLICATION_NAME must not be empty".to_string(),
        }
        .fail();
    }
    if config.message_prefix.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "ROUTER_CDC_MESSAGE_PREFIX must not be empty".to_string(),
        }
        .fail();
    }
    if config.status_interval.is_zero() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_STATUS_INTERVAL_MS must be positive".to_string(),
        }
        .fail();
    }
    if config.idle_wakeup_interval.is_zero() {
        return InvalidConfigurationSnafu {
            message: "SAURON_CDC_IDLE_WAKEUP_INTERVAL_MS must be positive".to_string(),
        }
        .fail();
    }
    Ok(())
}

pub fn cdc_event_seen() {
    counter!(SAURON_CDC_EVENTS_TOTAL).increment(1);
}

pub fn cdc_logical_message_seen() {
    counter!(SAURON_CDC_LOGICAL_MESSAGES_TOTAL).increment(1);
}

fn replication_config_from_database_url(
    database_url: &str,
    slot_name: &str,
    publication_name: &str,
    status_interval: Duration,
    idle_wakeup_interval: Duration,
) -> Result<ReplicationConfig> {
    let url =
        Url::parse(database_url).map_err(|error| crate::error::Error::InvalidConfiguration {
            message: format!("ROUTER_REPLICA_DATABASE_URL was not a valid URL: {error}"),
        })?;

    let host = url
        .host_str()
        .ok_or_else(|| crate::error::Error::InvalidConfiguration {
            message: "ROUTER_REPLICA_DATABASE_URL must include a host".to_string(),
        })?;
    let user = percent_decode(url.username())?;
    let password = percent_decode(url.password().unwrap_or_default())?;
    let database = url
        .path()
        .trim_start_matches('/')
        .split('/')
        .next()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| crate::error::Error::InvalidConfiguration {
            message: "ROUTER_REPLICA_DATABASE_URL must include a database name".to_string(),
        })?
        .to_string();

    let mut config =
        ReplicationConfig::new(host, user, password, database, slot_name, publication_name)
            .with_port(url.port().unwrap_or(5432))
            .with_tls(tls_config_from_url(&url))
            .with_status_interval(status_interval)
            .with_wakeup_interval(idle_wakeup_interval);

    config.start_lsn = Lsn::ZERO;
    Ok(config)
}

fn percent_decode(value: &str) -> Result<String> {
    percent_encoding::percent_decode_str(value)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|error| crate::error::Error::InvalidConfiguration {
            message: format!("database URL contained invalid percent encoding: {error}"),
        })
}

fn tls_config_from_url(url: &Url) -> TlsConfig {
    let sslmode = url
        .query_pairs()
        .find_map(|(key, value)| (key == "sslmode").then_some(value.into_owned()));

    match sslmode.as_deref() {
        Some("require") => TlsConfig::require(),
        Some("verify-ca") => TlsConfig::verify_ca(None),
        Some("verify-full") => TlsConfig::verify_full(None),
        _ => TlsConfig::disabled(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_router_cdc_message_payload() {
        let message = parse_router_cdc_message(
            br#"{
              "version": 1,
              "schema": "public",
              "table": "order_provider_operations",
              "op": "UPDATE",
              "id": "018f0000-0000-7000-8000-000000000001",
              "orderId": "018f0000-0000-7000-8000-000000000002",
              "watchId": "018f0000-0000-7000-8000-000000000001",
              "providerOperationId": "018f0000-0000-7000-8000-000000000001"
            }"#,
        )
        .expect("parse CDC message");

        assert_eq!(message.version, 1);
        assert_eq!(message.schema, "public");
        assert_eq!(message.table, "order_provider_operations");
        assert_eq!(message.op, "UPDATE");
        assert_eq!(message.watch_id, message.provider_operation_id);
    }

    #[test]
    fn parses_replication_database_url() {
        let config = replication_config_from_database_url(
            "postgres://replicator:p%40ss@db.local:6543/router_db?sslmode=require",
            "slot_a",
            "pub_a",
            Duration::from_secs(1),
            Duration::from_secs(10),
        )
        .expect("parse replication config");

        assert_eq!(config.host, "db.local");
        assert_eq!(config.port, 6543);
        assert_eq!(config.user, "replicator");
        assert_eq!(config.password, "p@ss");
        assert_eq!(config.database, "router_db");
        assert_eq!(config.slot, "slot_a");
        assert_eq!(config.publication, "pub_a");
    }
}
