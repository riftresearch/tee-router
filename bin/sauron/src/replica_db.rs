use snafu::ResultExt;
use sqlx_core::migrate::Migrator;
use sqlx_postgres::PgPool;
use std::path::PathBuf;
use tracing::info;

use crate::error::{ReplicaMigrationSnafu, Result};

// These migrations target the logical subscriber / replica endpoint that Sauron
// reads from. Keep replica-only objects, such as NOTIFY trigger wiring, here.
fn replica_migrations_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("SAURON_REPLICA_MIGRATIONS_DIR") {
        return path.into();
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations-replica");
    if manifest_dir.is_dir() {
        return manifest_dir;
    }

    PathBuf::from("migrations-replica")
}

pub async fn migrate_replica(pool: &PgPool) -> Result<()> {
    info!("Running Sauron replica migrations...");
    let mut migrator = Migrator::new(replica_migrations_dir())
        .await
        .context(ReplicaMigrationSnafu)?;

    // The replica database also carries the primary ROUTER migrations in
    // `_sqlx_migrations`, so this migrator must ignore applied versions it does
    // not own.
    migrator.ignore_missing = true;
    migrator.run(pool).await.context(ReplicaMigrationSnafu)?;
    info!("Sauron replica migrations complete");
    Ok(())
}
