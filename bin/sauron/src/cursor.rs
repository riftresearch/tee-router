use chrono::{DateTime, Utc};
use snafu::ResultExt;
use sqlx_core::row::Row;
use sqlx_postgres::PgPool;

use crate::{
    discovery::BlockCursor,
    error::{ReplicaDatabaseQuerySnafu, Result},
};

#[derive(Debug, Clone)]
pub struct CursorRepository {
    pool: PgPool,
}

impl CursorRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn load(&self, backend: &str) -> Result<Option<BlockCursor>> {
        let row = sqlx_core::query::query(
            r#"
            SELECT height, hash
            FROM public.sauron_backend_cursors
            WHERE backend = $1
            "#,
        )
        .bind(backend)
        .fetch_optional(&self.pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        row.map(|row| {
            let height = row.try_get::<i64, _>("height").map_err(|_| {
                crate::error::Error::InvalidCursorRow {
                    message: format!("cursor for backend {backend} missing height"),
                }
            })?;
            let height =
                u64::try_from(height).map_err(|_| crate::error::Error::InvalidCursorRow {
                    message: format!("cursor for backend {backend} had negative height"),
                })?;
            let hash = row.try_get::<String, _>("hash").map_err(|_| {
                crate::error::Error::InvalidCursorRow {
                    message: format!("cursor for backend {backend} missing hash"),
                }
            })?;

            Ok(BlockCursor { height, hash })
        })
        .transpose()
    }

    pub async fn save(&self, backend: &str, cursor: &BlockCursor) -> Result<()> {
        let height =
            i64::try_from(cursor.height).map_err(|_| crate::error::Error::InvalidCursorRow {
                message: format!(
                    "cursor for backend {backend} height {} exceeded i64 range",
                    cursor.height
                ),
            })?;
        let updated_at: DateTime<Utc> = Utc::now();

        sqlx_core::query::query(
            r#"
            INSERT INTO public.sauron_backend_cursors (
                backend,
                height,
                hash,
                updated_at
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (backend)
            DO UPDATE SET
                height = EXCLUDED.height,
                hash = EXCLUDED.hash,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(backend)
        .bind(height)
        .bind(&cursor.hash)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .context(ReplicaDatabaseQuerySnafu)?;

        Ok(())
    }
}
