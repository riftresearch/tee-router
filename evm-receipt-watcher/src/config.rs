use std::{net::SocketAddr, time::Duration};

use clap::Parser;
use url::Url;

use crate::{Error, Result};

#[derive(Debug, Clone, Parser)]
pub struct Config {
    #[arg(long, env = "EVM_RECEIPT_WATCHER_CHAIN")]
    pub chain: String,

    #[arg(long, env = "EVM_RECEIPT_WATCHER_HTTP_RPC_URL")]
    pub http_rpc_url: String,

    #[arg(long, env = "EVM_RECEIPT_WATCHER_WS_RPC_URL")]
    pub ws_rpc_url: Option<String>,

    #[arg(long, env = "EVM_RECEIPT_WATCHER_BIND", default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_MAX_PENDING",
        default_value_t = 100_000
    )]
    pub max_pending: usize,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_MAX_SUBSCRIBER_LAG",
        default_value_t = 10_000
    )]
    pub max_subscriber_lag: usize,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_POLL_INTERVAL_MS",
        default_value_t = 12_000
    )]
    pub poll_interval_ms: u64,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_WS_RECONNECT_DELAY_MS",
        default_value_t = 1_000
    )]
    pub ws_reconnect_delay_ms: u64,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_RECEIPT_RETRY_COUNT",
        default_value_t = 3
    )]
    pub receipt_retry_count: u32,

    #[arg(
        long,
        env = "EVM_RECEIPT_WATCHER_RECEIPT_RETRY_DELAY_MS",
        default_value_t = 200
    )]
    pub receipt_retry_delay_ms: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if self.chain.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "EVM_RECEIPT_WATCHER_CHAIN must be set".to_string(),
            });
        }
        validate_url(
            &self.http_rpc_url,
            &["http", "https"],
            "EVM_RECEIPT_WATCHER_HTTP_RPC_URL",
        )?;
        if let Some(ws_rpc_url) = &self.ws_rpc_url {
            validate_url(ws_rpc_url, &["ws", "wss"], "EVM_RECEIPT_WATCHER_WS_RPC_URL")?;
        }
        if self.max_pending == 0 {
            return Err(Error::InvalidConfiguration {
                message: "EVM_RECEIPT_WATCHER_MAX_PENDING must be positive".to_string(),
            });
        }
        if self.max_subscriber_lag == 0 {
            return Err(Error::InvalidConfiguration {
                message: "EVM_RECEIPT_WATCHER_MAX_SUBSCRIBER_LAG must be positive".to_string(),
            });
        }
        if self.poll_interval_ms == 0 {
            return Err(Error::InvalidConfiguration {
                message: "EVM_RECEIPT_WATCHER_POLL_INTERVAL_MS must be positive".to_string(),
            });
        }
        if self.ws_reconnect_delay_ms == 0 {
            return Err(Error::InvalidConfiguration {
                message: "EVM_RECEIPT_WATCHER_WS_RECONNECT_DELAY_MS must be positive".to_string(),
            });
        }
        Ok(())
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn ws_reconnect_delay(&self) -> Duration {
        Duration::from_millis(self.ws_reconnect_delay_ms)
    }

    pub fn receipt_retry_delay(&self) -> Duration {
        Duration::from_millis(self.receipt_retry_delay_ms)
    }
}

fn validate_url(value: &str, schemes: &[&str], env_name: &'static str) -> Result<()> {
    let parsed = Url::parse(value.trim()).map_err(|source| Error::InvalidConfiguration {
        message: format!("{env_name} must be a valid URL: {source}"),
    })?;
    if parsed.host().is_none() {
        return Err(Error::InvalidConfiguration {
            message: format!("{env_name} must include a host"),
        });
    }
    if !schemes.iter().any(|scheme| *scheme == parsed.scheme()) {
        return Err(Error::InvalidConfiguration {
            message: format!(
                "{env_name} must use one of these schemes: {}",
                schemes.join(", ")
            ),
        });
    }
    Ok(())
}
