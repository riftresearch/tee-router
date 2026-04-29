use snafu::ResultExt;
use sqlx_core::migrate::Migrator;
use sqlx_postgres::PgPool;
use std::path::PathBuf;
use tracing::info;

use crate::error::{Result, StateMigrationSnafu};

fn state_migrations_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("SAURON_STATE_MIGRATIONS_DIR") {
        return path.into();
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations-state");
    if manifest_dir.is_dir() {
        return manifest_dir;
    }

    PathBuf::from("migrations-state")
}

pub async fn migrate_state(pool: &PgPool) -> Result<()> {
    info!("Running Sauron state migrations...");
    let mut migrator = Migrator::new(state_migrations_dir())
        .await
        .context(StateMigrationSnafu)?;

    // In legacy local/test deployments this can be the same database as the
    // router schema, whose `_sqlx_migrations` already contains router versions.
    migrator.ignore_missing = true;
    migrator.run(pool).await.context(StateMigrationSnafu)?;
    info!("Sauron state migrations complete");
    Ok(())
}
