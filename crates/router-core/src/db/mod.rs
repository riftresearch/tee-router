pub mod order_repo;
pub mod provider_health_repo;
pub mod provider_policy_repo;
pub mod route_cost_repo;
pub mod vault_repo;
pub mod worker_lease_repo;

pub use order_repo::{
    CompletedSingleStepOrder, OrderRepository, PersistStepCompletionRecord,
    SingleStepExecutionStartPlan, SingleStepExecutionStartRecord, StepCompletionRecord,
};
pub use provider_health_repo::ProviderHealthRepository;
pub use provider_policy_repo::ProviderPolicyRepository;
pub use route_cost_repo::RouteCostRepository;
pub use vault_repo::VaultRepository;
pub use worker_lease_repo::WorkerLeaseRepository;

use crate::error::RouterCoreResult;
use sqlx_core::migrate::Migrator;
use sqlx_postgres::{PgPool, PgPoolOptions};
use std::{path::PathBuf, time::Duration};
use tracing::info;

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

fn migrations_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("ROUTER_MIGRATIONS_DIR") {
        return path.into();
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let crate_dir = manifest_dir.join("migrations");
    if crate_dir.is_dir() {
        return crate_dir;
    }

    PathBuf::from("migrations")
}

impl Database {
    pub async fn connect(
        database_url: &str,
        max_db_connections: u32,
        min_db_connections: u32,
    ) -> RouterCoreResult<Self> {
        info!("Connecting router-server database...");

        let pool = PgPoolOptions::new()
            .max_connections(max_db_connections)
            .min_connections(min_db_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(600))
            .connect(database_url)
            .await?;

        Self::from_pool(pool).await
    }

    pub async fn from_pool(pool: PgPool) -> RouterCoreResult<Self> {
        info!("Running router-server migrations...");
        let migrator = Migrator::new(migrations_dir()).await?;
        migrator.run(&pool).await?;
        info!("Router-server database initialization complete");
        Ok(Self { pool })
    }

    #[must_use]
    pub fn vaults(&self) -> VaultRepository {
        VaultRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn orders(&self) -> OrderRepository {
        OrderRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn provider_policies(&self) -> ProviderPolicyRepository {
        ProviderPolicyRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn provider_health(&self) -> ProviderHealthRepository {
        ProviderHealthRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn route_costs(&self) -> RouteCostRepository {
        RouteCostRepository::new(self.pool.clone())
    }

    #[must_use]
    pub fn worker_leases(&self) -> WorkerLeaseRepository {
        WorkerLeaseRepository::new(self.pool.clone())
    }
}
