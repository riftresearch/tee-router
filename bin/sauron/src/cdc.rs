use std::{fmt, time::Duration};

use metrics::{counter, gauge};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use serde::Deserialize;
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use crate::error::{
    CdcPayloadSnafu, CdcStreamSnafu, Error, InvalidConfigurationSnafu, ReplicaDatabaseQuerySnafu,
    Result, StateDatabaseQuerySnafu,
};

pub const ROUTER_CDC_MESSAGE_PREFIX: &str = "rift.router.change";
pub const ROUTER_CDC_PUBLICATION_NAME: &str = "router_cdc_publication";
pub const MAX_ROUTER_CDC_MESSAGE_BYTES: usize = 16 * 1024;

const SAURON_CDC_EVENTS_TOTAL: &str = "sauron_cdc_events_total";
const SAURON_CDC_LOGICAL_MESSAGES_TOTAL: &str = "sauron_cdc_logical_messages_total";
const SAURON_CDC_LAST_COMMIT_LSN: &str = "sauron_cdc_last_commit_lsn";
const MAX_POSTGRES_IDENTIFIER_BYTES: usize = 63;
const MAX_ROUTER_CDC_FIELD_BYTES: usize = 128;

#[derive(Clone)]
pub struct CdcConfig {
    pub database_url: String,
    pub slot_name: String,
    pub publication_name: String,
    pub message_prefix: String,
    pub status_interval: Duration,
    pub idle_wakeup_interval: Duration,
}

impl fmt::Debug for CdcConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdcConfig")
            .field("database_url", &"<redacted>")
            .field("slot_name", &self.slot_name)
            .field("publication_name", &self.publication_name)
            .field("message_prefix", &self.message_prefix)
            .field("status_interval", &self.status_interval)
            .field("idle_wakeup_interval", &self.idle_wakeup_interval)
            .finish()
    }
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
    pub order_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_updated_at: Option<chrono::DateTime<chrono::Utc>>,
    pub watch_id: Option<Uuid>,
    pub provider_operation_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct RouterCdcRepository {
    router_pool: PgPool,
    state_pool: PgPool,
    config: CdcConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CdcSlotHealth {
    active: Option<bool>,
    wal_status: Option<String>,
    invalidation_reason: Option<String>,
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
            SELECT plugin, database, current_database() AS current_database
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
            let slot_database: Option<String> =
                row.try_get("database").context(ReplicaDatabaseQuerySnafu)?;
            let current_database: String = row
                .try_get("current_database")
                .context(ReplicaDatabaseQuerySnafu)?;
            if plugin.as_deref() != Some("pgoutput") {
                return InvalidConfigurationSnafu {
                    message: format!(
                        "CDC slot {} uses plugin {:?}, expected pgoutput",
                        self.config.slot_name, plugin
                    ),
                }
                .fail();
            }
            if slot_database.as_deref() != Some(current_database.as_str()) {
                return InvalidConfigurationSnafu {
                    message: format!(
                        "CDC slot {} belongs to database {:?}, expected {}",
                        self.config.slot_name, slot_database, current_database
                    ),
                }
                .fail();
            }
            let health = fetch_cdc_slot_health(&self.router_pool, &self.config.slot_name).await?;
            if let Some(reason) = cdc_slot_invalid_reason(&health) {
                if health.active == Some(true) {
                    return InvalidConfigurationSnafu {
                        message: format!(
                            "CDC slot {} is invalid ({reason}) but active; stop the active consumer before recreating it",
                            self.config.slot_name
                        ),
                    }
                    .fail();
                }
                recreate_cdc_slot(&self.router_pool, &self.config.slot_name, &reason).await?;
                return Ok(());
            }

            info!(
                slot_name = %self.config.slot_name,
                publication = %self.config.publication_name,
                "Sauron CDC decoding slot already exists"
            );
            return Ok(());
        }

        create_cdc_slot(&self.router_pool, &self.config.slot_name).await?;
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

async fn create_cdc_slot(pool: &PgPool, slot_name: &str) -> Result<()> {
    sqlx_core::query::query(
        r#"
        SELECT slot_name
        FROM pg_catalog.pg_create_logical_replication_slot($1::name, 'pgoutput')
        "#,
    )
    .bind(slot_name)
    .fetch_one(pool)
    .await
    .context(ReplicaDatabaseQuerySnafu)?;

    info!(
        slot_name = %slot_name,
        "Created Sauron CDC decoding slot"
    );
    Ok(())
}

async fn recreate_cdc_slot(pool: &PgPool, slot_name: &str, reason: &str) -> Result<()> {
    warn!(
        slot_name = %slot_name,
        reason = %reason,
        "Sauron CDC decoding slot is invalid; recreating"
    );
    sqlx_core::query::query(
        r#"
        SELECT pg_catalog.pg_drop_replication_slot($1::name)
        "#,
    )
    .bind(slot_name)
    .fetch_one(pool)
    .await
    .context(ReplicaDatabaseQuerySnafu)?;
    create_cdc_slot(pool, slot_name).await
}

async fn fetch_cdc_slot_health(pool: &PgPool, slot_name: &str) -> Result<CdcSlotHealth> {
    let column_rows = sqlx_core::query::query(
        r#"
        SELECT attname
        FROM pg_catalog.pg_attribute
        WHERE attrelid = 'pg_catalog.pg_replication_slots'::regclass
          AND attnum > 0
          AND NOT attisdropped
          AND attname IN ('active', 'wal_status', 'invalidation_reason')
        "#,
    )
    .fetch_all(pool)
    .await
    .context(ReplicaDatabaseQuerySnafu)?;
    let columns = column_rows
        .iter()
        .map(|row| row.get::<String, _>("attname"))
        .collect::<std::collections::HashSet<_>>();

    let active_expr = if columns.contains("active") {
        "active"
    } else {
        "NULL::boolean AS active"
    };
    let wal_status_expr = if columns.contains("wal_status") {
        "wal_status"
    } else {
        "NULL::text AS wal_status"
    };
    let invalidation_reason_expr = if columns.contains("invalidation_reason") {
        "invalidation_reason"
    } else {
        "NULL::text AS invalidation_reason"
    };
    let query = format!(
        r#"
        SELECT {active_expr}, {wal_status_expr}, {invalidation_reason_expr}
        FROM pg_catalog.pg_replication_slots
        WHERE slot_name = $1
        "#
    );
    let row = sqlx_core::query::query(&query)
        .bind(slot_name)
        .fetch_optional(pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;
    let Some(row) = row else {
        return Ok(CdcSlotHealth {
            active: None,
            wal_status: None,
            invalidation_reason: None,
        });
    };

    Ok(CdcSlotHealth {
        active: row.try_get("active").context(ReplicaDatabaseQuerySnafu)?,
        wal_status: row
            .try_get("wal_status")
            .context(ReplicaDatabaseQuerySnafu)?,
        invalidation_reason: row
            .try_get("invalidation_reason")
            .context(ReplicaDatabaseQuerySnafu)?,
    })
}

fn cdc_slot_invalid_reason(slot: &CdcSlotHealth) -> Option<String> {
    if let Some(reason) = slot
        .invalidation_reason
        .as_deref()
        .map(str::trim)
        .filter(|reason| !reason.is_empty())
    {
        return Some(format!("invalidation_reason={reason}"));
    }
    if slot.wal_status.as_deref() == Some("lost") {
        return Some("wal_status=lost".to_string());
    }
    None
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
    if content.len() > MAX_ROUTER_CDC_MESSAGE_BYTES {
        return Err(Error::CdcPayloadTooLarge {
            max_bytes: MAX_ROUTER_CDC_MESSAGE_BYTES,
        });
    }

    let message: RouterCdcMessage = serde_json::from_slice(content).context(CdcPayloadSnafu)?;
    validate_router_cdc_message(&message)?;
    Ok(message)
}

pub fn validate_cdc_config(config: &CdcConfig) -> Result<()> {
    validate_postgres_identifier(&config.slot_name, "SAURON_CDC_SLOT_NAME")?;
    validate_postgres_identifier(&config.publication_name, "ROUTER_CDC_PUBLICATION_NAME")?;
    if config.publication_name != ROUTER_CDC_PUBLICATION_NAME {
        return InvalidConfigurationSnafu {
            message: format!(
                "ROUTER_CDC_PUBLICATION_NAME must be {ROUTER_CDC_PUBLICATION_NAME} because router migrations create that fixed publication"
            ),
        }
        .fail();
    }
    if config.message_prefix.trim().is_empty() {
        return InvalidConfigurationSnafu {
            message: "ROUTER_CDC_MESSAGE_PREFIX must not be empty".to_string(),
        }
        .fail();
    }
    if config.message_prefix != ROUTER_CDC_MESSAGE_PREFIX {
        return InvalidConfigurationSnafu {
            message: format!(
                "ROUTER_CDC_MESSAGE_PREFIX must be {ROUTER_CDC_MESSAGE_PREFIX} because router migrations emit that fixed logical-message prefix"
            ),
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

fn validate_postgres_identifier(value: &str, name: &str) -> Result<()> {
    let value = value.trim();
    if value.is_empty() {
        return InvalidConfigurationSnafu {
            message: format!("{name} must not be empty"),
        }
        .fail();
    }
    if value.len() > MAX_POSTGRES_IDENTIFIER_BYTES {
        return InvalidConfigurationSnafu {
            message: format!("{name} must be at most {MAX_POSTGRES_IDENTIFIER_BYTES} bytes"),
        }
        .fail();
    }
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return InvalidConfigurationSnafu {
            message: format!("{name} must not be empty"),
        }
        .fail();
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return InvalidConfigurationSnafu {
            message: format!("{name} must be an unquoted PostgreSQL identifier"),
        }
        .fail();
    }
    Ok(())
}

fn validate_router_cdc_message(message: &RouterCdcMessage) -> Result<()> {
    if message.version != 1 {
        return invalid_cdc_payload(format!(
            "unsupported router CDC payload version {}",
            message.version
        ));
    }
    if !bounded_router_cdc_field(&message.schema) {
        return invalid_cdc_payload("router CDC schema field was too large".to_string());
    }
    if !bounded_router_cdc_field(&message.table) {
        return invalid_cdc_payload("router CDC table field was too large".to_string());
    }
    if !bounded_router_cdc_field(&message.op) {
        return invalid_cdc_payload("router CDC op field was too large".to_string());
    }
    if message.schema != "public" {
        return invalid_cdc_payload(format!("unsupported router CDC schema {}", message.schema));
    }
    if !SUPPORTED_ROUTER_CDC_TABLES.contains(&message.table.as_str()) {
        return invalid_cdc_payload(format!("unsupported router CDC table {}", message.table));
    }
    if !SUPPORTED_ROUTER_CDC_OPS.contains(&message.op.as_str()) {
        return invalid_cdc_payload(format!("unsupported router CDC op {}", message.op));
    }
    if message.event_updated_at.is_none() {
        return invalid_cdc_payload("router CDC message missing eventUpdatedAt".to_string());
    }
    if message.order_id.is_none()
        && message.watch_id.is_none()
        && message.provider_operation_id.is_none()
        && !NULL_IDENTITY_IGNORED_TABLES.contains(&message.table.as_str())
    {
        return invalid_cdc_payload(
            "router CDC message did not include an order, watch, or provider-operation identity"
                .to_string(),
        );
    }
    Ok(())
}

fn bounded_router_cdc_field(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_ROUTER_CDC_FIELD_BYTES
}

fn invalid_cdc_payload<T>(message: String) -> Result<T> {
    Err(Error::InvalidCdcPayload { message })
}

const SUPPORTED_ROUTER_CDC_OPS: &[&str] = &["INSERT", "UPDATE", "DELETE"];
const SUPPORTED_ROUTER_CDC_TABLES: &[&str] = &[
    "router_orders",
    "market_order_quotes",
    "market_order_actions",
    "limit_order_quotes",
    "limit_order_actions",
    "order_execution_legs",
    "order_execution_steps",
    "order_provider_operations",
    "order_provider_addresses",
    "deposit_vaults",
    "custody_vaults",
];
const NULL_IDENTITY_IGNORED_TABLES: &[&str] = &[
    "market_order_quotes",
    "limit_order_quotes",
    "deposit_vaults",
    "custody_vaults",
];

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
    fn cdc_config_debug_redacts_database_url() {
        let config = CdcConfig {
            database_url: "postgres://replicator:db-secret@db.local/router_db".to_string(),
            slot_name: "sauron_watch_cdc".to_string(),
            publication_name: ROUTER_CDC_PUBLICATION_NAME.to_string(),
            message_prefix: ROUTER_CDC_MESSAGE_PREFIX.to_string(),
            status_interval: Duration::from_secs(1),
            idle_wakeup_interval: Duration::from_secs(10),
        };

        let rendered = format!("{config:?}");
        assert!(rendered.contains("database_url"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("sauron_watch_cdc"));
        assert!(!rendered.contains("replicator"));
        assert!(!rendered.contains("db-secret"));
    }

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
              "orderUpdatedAt": "2026-05-04T06:00:00Z",
              "eventUpdatedAt": "2026-05-04T06:00:01Z",
              "watchId": "018f0000-0000-7000-8000-000000000001",
              "providerOperationId": "018f0000-0000-7000-8000-000000000001"
            }"#,
        )
        .expect("parse CDC message");

        assert_eq!(message.version, 1);
        assert_eq!(message.schema, "public");
        assert_eq!(message.table, "order_provider_operations");
        assert_eq!(message.op, "UPDATE");
        assert_eq!(
            message
                .order_updated_at
                .map(|timestamp| timestamp.to_rfc3339()),
            Some("2026-05-04T06:00:00+00:00".to_string())
        );
        assert_eq!(
            message
                .event_updated_at
                .map(|timestamp| timestamp.to_rfc3339()),
            Some("2026-05-04T06:00:01+00:00".to_string())
        );
        assert_eq!(message.watch_id, message.provider_operation_id);
    }

    #[test]
    fn router_cdc_message_rejects_oversized_payloads() {
        let payload = vec![b' '; MAX_ROUTER_CDC_MESSAGE_BYTES + 1];
        let error = parse_router_cdc_message(&payload).expect_err("reject oversized payload");

        assert!(matches!(error, Error::CdcPayloadTooLarge { .. }));
    }

    #[test]
    fn router_cdc_message_rejects_unsupported_payload_shapes() {
        let unsupported_version = parse_router_cdc_message(
            br#"{
              "version": 2,
              "schema": "public",
              "table": "router_orders",
              "op": "UPDATE",
              "orderId": "018f0000-0000-7000-8000-000000000002"
            }"#,
        )
        .expect_err("reject unsupported version");
        assert!(matches!(
            unsupported_version,
            Error::InvalidCdcPayload { .. }
        ));

        let unsupported_table = parse_router_cdc_message(
            br#"{
              "version": 1,
              "schema": "public",
              "table": "unrelated_table",
              "op": "UPDATE",
              "orderId": "018f0000-0000-7000-8000-000000000002"
            }"#,
        )
        .expect_err("reject unsupported table");
        assert!(matches!(unsupported_table, Error::InvalidCdcPayload { .. }));

        let unsupported_op = parse_router_cdc_message(
            br#"{
              "version": 1,
              "schema": "public",
              "table": "router_orders",
              "op": "TRUNCATE",
              "orderId": "018f0000-0000-7000-8000-000000000002"
            }"#,
        )
        .expect_err("reject unsupported op");
        assert!(matches!(unsupported_op, Error::InvalidCdcPayload { .. }));

        let missing_identity = parse_router_cdc_message(
            br#"{
              "version": 1,
              "schema": "public",
              "table": "order_provider_operations",
              "op": "UPDATE",
              "id": "018f0000-0000-7000-8000-000000000001",
              "orderId": null,
              "watchId": null,
              "providerOperationId": null,
              "eventUpdatedAt": "2026-05-04T06:00:01Z"
            }"#,
        )
        .expect_err("reject supported message without a refresh identity");
        assert!(matches!(missing_identity, Error::InvalidCdcPayload { .. }));

        let unassociated_quote = parse_router_cdc_message(
            br#"{
              "version": 1,
              "schema": "public",
              "table": "market_order_quotes",
              "op": "INSERT",
              "id": "018f0000-0000-7000-8000-000000000001",
              "orderId": null,
              "watchId": null,
              "providerOperationId": null,
              "eventUpdatedAt": "2026-05-04T06:00:01Z"
            }"#,
        )
        .expect("accept unassociated quote message as ignorable");
        assert_eq!(unassociated_quote.order_id, None);
        assert_eq!(unassociated_quote.watch_id, None);
        assert_eq!(unassociated_quote.provider_operation_id, None);

        let missing_event_updated_at = parse_router_cdc_message(
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
        .expect_err("reject supported message without eventUpdatedAt");
        assert!(matches!(
            missing_event_updated_at,
            Error::InvalidCdcPayload { .. }
        ));
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

    #[test]
    fn cdc_config_rejects_invalid_postgres_identifiers() {
        let mut config = CdcConfig {
            database_url: "postgres://replicator:pass@db.local/router_db".to_string(),
            slot_name: "sauron_watch_cdc".to_string(),
            publication_name: "router_cdc_publication".to_string(),
            message_prefix: ROUTER_CDC_MESSAGE_PREFIX.to_string(),
            status_interval: Duration::from_secs(1),
            idle_wakeup_interval: Duration::from_secs(10),
        };

        validate_cdc_config(&config).unwrap();

        config.slot_name = "1bad".to_string();
        assert!(validate_cdc_config(&config).is_err());

        config.slot_name = "bad-name".to_string();
        assert!(validate_cdc_config(&config).is_err());

        config.slot_name = "a".repeat(MAX_POSTGRES_IDENTIFIER_BYTES + 1);
        assert!(validate_cdc_config(&config).is_err());
    }

    #[test]
    fn cdc_config_rejects_non_default_publication_names() {
        let mut config = CdcConfig {
            database_url: "postgres://replicator:pass@db.local/router_db".to_string(),
            slot_name: "sauron_watch_cdc".to_string(),
            publication_name: ROUTER_CDC_PUBLICATION_NAME.to_string(),
            message_prefix: ROUTER_CDC_MESSAGE_PREFIX.to_string(),
            status_interval: Duration::from_secs(1),
            idle_wakeup_interval: Duration::from_secs(10),
        };

        validate_cdc_config(&config).unwrap();

        config.publication_name = "router_cdc_publication_test".to_string();
        let error = validate_cdc_config(&config).expect_err("custom publication must fail");
        assert!(error
            .to_string()
            .contains("ROUTER_CDC_PUBLICATION_NAME must be router_cdc_publication"));
    }

    #[test]
    fn cdc_config_rejects_non_default_logical_message_prefixes() {
        let mut config = CdcConfig {
            database_url: "postgres://replicator:pass@db.local/router_db".to_string(),
            slot_name: "sauron_watch_cdc".to_string(),
            publication_name: "router_cdc_publication".to_string(),
            message_prefix: ROUTER_CDC_MESSAGE_PREFIX.to_string(),
            status_interval: Duration::from_secs(1),
            idle_wakeup_interval: Duration::from_secs(10),
        };

        validate_cdc_config(&config).unwrap();

        config.message_prefix = "custom.router.change".to_string();
        let error = validate_cdc_config(&config).expect_err("custom prefix must fail");
        assert!(error
            .to_string()
            .contains("ROUTER_CDC_MESSAGE_PREFIX must be rift.router.change"));

        config.message_prefix = " rift.router.change ".to_string();
        assert!(validate_cdc_config(&config).is_err());
    }

    #[test]
    fn cdc_slot_invalid_reason_detects_lost_or_invalidated_slots() {
        assert_eq!(
            cdc_slot_invalid_reason(&CdcSlotHealth {
                active: Some(false),
                wal_status: Some("reserved".to_string()),
                invalidation_reason: None,
            }),
            None
        );
        assert_eq!(
            cdc_slot_invalid_reason(&CdcSlotHealth {
                active: Some(false),
                wal_status: Some("lost".to_string()),
                invalidation_reason: None,
            })
            .as_deref(),
            Some("wal_status=lost")
        );
        assert_eq!(
            cdc_slot_invalid_reason(&CdcSlotHealth {
                active: Some(false),
                wal_status: Some("reserved".to_string()),
                invalidation_reason: Some("wal_removed".to_string()),
            })
            .as_deref(),
            Some("invalidation_reason=wal_removed")
        );
    }
}
