use std::{net::SocketAddr, time::Duration};

use clap::Parser;

use crate::{error::Result, Error};

#[derive(Debug, Clone, Parser)]
pub struct Config {
    #[arg(long, env = "HL_SHIM_DATABASE_URL")]
    pub database_url: String,

    #[arg(
        long,
        env = "HL_SHIM_INFO_URL",
        default_value = "https://api.hyperliquid.xyz"
    )]
    pub info_url: String,

    #[arg(long, env = "HL_SHIM_BIND", default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    // Under 10k load with thousands of HL identities polled across 4 endpoints each,
    // a 10-connection pool serializes inserts and surfaces as "slow statement"
    // warnings — the statements aren't slow, the pool is the queue. 64 covers
    // realistic burst concurrency without overwhelming Postgres.
    #[arg(long, env = "HL_SHIM_MAX_DB_CONNECTIONS", default_value_t = 64)]
    pub max_db_connections: u32,

    #[arg(long, env = "HL_SHIM_HOT_CADENCE_MS", default_value_t = 5_000)]
    pub hot_cadence_ms: u64,

    #[arg(long, env = "HL_SHIM_WARM_CADENCE_MS", default_value_t = 30_000)]
    pub warm_cadence_ms: u64,

    #[arg(long, env = "HL_SHIM_COLD_CADENCE_MS", default_value_t = 120_000)]
    pub cold_cadence_ms: u64,

    #[arg(long, env = "HL_SHIM_FUNDING_HOT_CADENCE_MS", default_value_t = 60_000)]
    pub funding_hot_cadence_ms: u64,

    #[arg(
        long,
        env = "HL_SHIM_FUNDING_WARM_CADENCE_MS",
        default_value_t = 300_000
    )]
    pub funding_warm_cadence_ms: u64,

    #[arg(
        long,
        env = "HL_SHIM_FUNDING_COLD_CADENCE_MS",
        default_value_t = 1_800_000
    )]
    pub funding_cold_cadence_ms: u64,

    #[arg(long, env = "HL_SHIM_ORDER_STATUS_CADENCE_MS", default_value_t = 2_000)]
    pub order_status_cadence_ms: u64,

    #[arg(long, env = "HL_SHIM_WEIGHT_BUDGET_PER_MIN", default_value_t = 1_100)]
    pub weight_budget_per_min: u32,

    #[arg(long, env = "HL_SHIM_WORKER_COUNT", default_value_t = 4)]
    pub worker_count: usize,

    #[arg(long, env = "HL_SHIM_MAX_SUBSCRIBER_LAG", default_value_t = 10_000)]
    pub max_subscriber_lag: usize,
}

#[derive(Debug, Clone)]
pub struct Cadences {
    pub hot: Duration,
    pub warm: Duration,
    pub cold: Duration,
    pub funding_hot: Duration,
    pub funding_warm: Duration,
    pub funding_cold: Duration,
    pub order_status: Duration,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.database_url.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "HL_SHIM_DATABASE_URL must be set".to_string(),
            });
        }
        if self.info_url.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "HL_SHIM_INFO_URL must be set".to_string(),
            });
        }
        if self.worker_count == 0 {
            return Err(Error::InvalidConfiguration {
                message: "HL_SHIM_WORKER_COUNT must be positive".to_string(),
            });
        }
        Ok(())
    }

    pub fn cadences(&self) -> Cadences {
        Cadences {
            hot: Duration::from_millis(self.hot_cadence_ms),
            warm: Duration::from_millis(self.warm_cadence_ms),
            cold: Duration::from_millis(self.cold_cadence_ms),
            funding_hot: Duration::from_millis(self.funding_hot_cadence_ms),
            funding_warm: Duration::from_millis(self.funding_warm_cadence_ms),
            funding_cold: Duration::from_millis(self.funding_cold_cadence_ms),
            order_status: Duration::from_millis(self.order_status_cadence_ms),
        }
    }
}
