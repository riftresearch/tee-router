use std::path::PathBuf;

use alloy::primitives::Address;
use snafu::ResultExt;
use sqlx_core::{
    migrate::Migrator, query::query, query_scalar::query_scalar, row::Row, types::Json,
};
use sqlx_postgres::{PgPool, PgPoolOptions};

use crate::{
    error::{DatabaseMigrationSnafu, SerializationSnafu},
    ingest::{
        canonical_key,
        schema::{
            DecimalString, HlMarket, HlOrderEvent, HlOrderStatus, HlTransferEvent, HlTransferKind,
            QueryCursor,
        },
    },
    telemetry, Result,
};

#[derive(Debug, Clone)]
pub struct Storage {
    pool: PgPool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorEndpoint {
    Fills,
    Ledger,
    Funding,
    OrderStatus(u64),
}

impl CursorEndpoint {
    #[must_use]
    pub fn as_key(self) -> String {
        match self {
            Self::Fills => "fills".to_string(),
            Self::Ledger => "ledger".to_string(),
            Self::Funding => "funding".to_string(),
            Self::OrderStatus(oid) => format!("orderstatus:{oid}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Paged<T> {
    pub items: Vec<T>,
    pub next_cursor: QueryCursor,
    pub has_more: bool,
}

impl Storage {
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await?;
        let storage = Self { pool };
        storage.migrate().await?;
        Ok(storage)
    }

    #[must_use]
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        let migrator = Migrator::new(migrations_dir())
            .await
            .context(DatabaseMigrationSnafu)?;
        migrator
            .run(&self.pool)
            .await
            .context(DatabaseMigrationSnafu)
    }

    pub async fn cursor(&self, user: Address, endpoint: CursorEndpoint) -> Result<i64> {
        let row = query_scalar::<_, i64>(
            "SELECT cursor_ms FROM hl_cursors WHERE user_addr = $1 AND endpoint = $2",
        )
        .bind(address_bytes(user))
        .bind(endpoint.as_key())
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.unwrap_or(0))
    }

    pub async fn set_cursor(
        &self,
        user: Address,
        endpoint: CursorEndpoint,
        cursor_ms: i64,
    ) -> Result<()> {
        query(
            r#"
            INSERT INTO hl_cursors (user_addr, endpoint, cursor_ms)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_addr, endpoint)
            DO UPDATE SET cursor_ms = EXCLUDED.cursor_ms, updated_at = now()
            "#,
        )
        .bind(address_bytes(user))
        .bind(endpoint.as_key())
        .bind(cursor_ms)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn insert_transfer(&self, event: &HlTransferEvent) -> Result<bool> {
        let metadata = serde_json::to_value(&event.kind).context(SerializationSnafu {
            context: "transfer kind metadata",
        })?;
        let result = query(
            r#"
            INSERT INTO hl_transfers (
                user_addr, time_ms, kind, canonical_key, asset, market,
                amount_delta, fee, fee_token, hash, metadata, observed_at_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7::NUMERIC(78,18), $8::NUMERIC(78,18),
                $9, $10, $11, $12
            )
            ON CONFLICT (user_addr, canonical_key) DO NOTHING
            "#,
        )
        .bind(address_bytes(event.user))
        .bind(event.time_ms)
        .bind(event.kind_name())
        .bind(canonical_key(event))
        .bind(&event.asset)
        .bind(event.market.as_db_str())
        .bind(event.amount_delta.as_str())
        .bind(event.fee.as_ref().map(DecimalString::as_str))
        .bind(event.fee_token.as_deref())
        .bind(hash_bytes(&event.hash))
        .bind(Json(metadata))
        .bind(event.observed_at_ms)
        .execute(&self.pool)
        .await?;
        let inserted = result.rows_affected() == 1;
        telemetry::record_db_write(event.kind_name(), inserted);
        Ok(inserted)
    }

    pub async fn insert_order(&self, event: &HlOrderEvent) -> Result<bool> {
        let result = query(
            r#"
            INSERT INTO hl_orders (
                user_addr, oid, cloid, coin, side, limit_px, sz, orig_sz,
                status, status_timestamp_ms, observed_at_ms
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ON CONFLICT (user_addr, oid)
            DO UPDATE SET
                cloid = EXCLUDED.cloid,
                coin = EXCLUDED.coin,
                side = EXCLUDED.side,
                limit_px = EXCLUDED.limit_px,
                sz = EXCLUDED.sz,
                orig_sz = EXCLUDED.orig_sz,
                status = EXCLUDED.status,
                status_timestamp_ms = EXCLUDED.status_timestamp_ms,
                observed_at_ms = EXCLUDED.observed_at_ms
            WHERE hl_orders.status_timestamp_ms <= EXCLUDED.status_timestamp_ms
            "#,
        )
        .bind(address_bytes(event.user))
        .bind(i64::try_from(event.oid).unwrap_or(i64::MAX))
        .bind(event.cloid.as_deref())
        .bind(&event.coin)
        .bind(&event.side)
        .bind(&event.limit_px)
        .bind(&event.sz)
        .bind(&event.orig_sz)
        .bind(event.status.as_str())
        .bind(event.status_timestamp_ms)
        .bind(event.observed_at_ms)
        .execute(&self.pool)
        .await?;
        let inserted = result.rows_affected() > 0;
        telemetry::record_db_write("order", inserted);
        Ok(inserted)
    }

    pub async fn transfers_page(
        &self,
        user: Address,
        from_time: i64,
        limit: i64,
    ) -> Result<Paged<HlTransferEvent>> {
        let rows = query(
            r#"
            SELECT time_ms, kind, asset, market, amount_delta::TEXT, fee::TEXT,
                   fee_token, encode(hash, 'hex') AS hash_hex, metadata, observed_at_ms
            FROM hl_transfers
            WHERE user_addr = $1 AND time_ms > $2
            ORDER BY time_ms ASC, canonical_key ASC
            LIMIT $3
            "#,
        )
        .bind(address_bytes(user))
        .bind(from_time)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        let mut items = Vec::new();
        for row in rows
            .into_iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
        {
            let kind_json: Json<HlTransferKind> = row.try_get("metadata")?;
            let market = match row.try_get::<Option<String>, _>("market")?.as_deref() {
                Some("spot") => HlMarket::Spot,
                Some("perp") => HlMarket::Perp,
                _ => HlMarket::NotApplicable,
            };
            let fee = row
                .try_get::<Option<String>, _>("fee")?
                .map(DecimalString::new)
                .transpose()?;
            items.push(HlTransferEvent {
                user,
                time_ms: row.try_get("time_ms")?,
                kind: kind_json.0,
                asset: row.try_get("asset")?,
                market,
                amount_delta: DecimalString::new(row.try_get::<String, _>("amount_delta")?)?,
                fee,
                fee_token: row.try_get("fee_token")?,
                hash: format!("0x{}", row.try_get::<String, _>("hash_hex")?),
                observed_at_ms: row.try_get("observed_at_ms")?,
            });
        }
        let next_cursor = QueryCursor(items.last().map_or(from_time, |event| event.time_ms));
        Ok(Paged {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn orders_page(
        &self,
        user: Address,
        from_time: i64,
        limit: i64,
    ) -> Result<Paged<HlOrderEvent>> {
        let rows = query(
            r#"
            SELECT oid, cloid, coin, side, limit_px, sz, orig_sz, status,
                   status_timestamp_ms, observed_at_ms
            FROM hl_orders
            WHERE user_addr = $1 AND status_timestamp_ms > $2
            ORDER BY status_timestamp_ms ASC, oid ASC
            LIMIT $3
            "#,
        )
        .bind(address_bytes(user))
        .bind(from_time)
        .bind(limit + 1)
        .fetch_all(&self.pool)
        .await?;
        let has_more = i64::try_from(rows.len()).unwrap_or(i64::MAX) > limit;
        let mut items = Vec::new();
        for row in rows
            .into_iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
        {
            items.push(HlOrderEvent {
                user,
                oid: u64::try_from(row.try_get::<i64, _>("oid")?).unwrap_or(0),
                cloid: row.try_get("cloid")?,
                coin: row.try_get("coin")?,
                side: row.try_get("side")?,
                limit_px: row.try_get("limit_px")?,
                sz: row.try_get("sz")?,
                orig_sz: row.try_get("orig_sz")?,
                status: HlOrderStatus::from(row.try_get::<String, _>("status")?),
                status_timestamp_ms: row.try_get("status_timestamp_ms")?,
                observed_at_ms: row.try_get("observed_at_ms")?,
            });
        }
        let next_cursor = QueryCursor(
            items
                .last()
                .map_or(from_time, |event| event.status_timestamp_ms),
        );
        Ok(Paged {
            items,
            next_cursor,
            has_more,
        })
    }

    pub async fn prune(&self, relevant_users: &[Address], before_time_ms: i64) -> Result<()> {
        let users: Vec<Vec<u8>> = relevant_users.iter().copied().map(address_bytes).collect();
        query("DELETE FROM hl_transfers WHERE NOT (user_addr = ANY($1)) AND time_ms < $2")
            .bind(&users)
            .bind(before_time_ms)
            .execute(&self.pool)
            .await?;
        query("DELETE FROM hl_orders WHERE NOT (user_addr = ANY($1)) AND status_timestamp_ms < $2")
            .bind(&users)
            .bind(before_time_ms)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn healthcheck(&self) -> Result<()> {
        let _: i64 = query_scalar("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }
}

fn migrations_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("HL_SHIM_MIGRATIONS_DIR") {
        return path.into();
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

fn address_bytes(address: Address) -> Vec<u8> {
    address.as_slice().to_vec()
}

fn hash_bytes(hash: &str) -> Vec<u8> {
    let raw = hash.strip_prefix("0x").unwrap_or(hash);
    hex::decode(raw).unwrap_or_default()
}
