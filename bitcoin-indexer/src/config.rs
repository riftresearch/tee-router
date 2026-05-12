use std::{net::SocketAddr, time::Duration};

use bitcoin::Network;
use bitcoincore_rpc_async::Auth;
use clap::Parser;
use url::Url;

use crate::{Error, Result};

#[derive(Debug, Clone, Parser)]
pub struct Config {
    #[arg(long, env = "BITCOIN_INDEXER_NETWORK", default_value = "bitcoin")]
    pub network: Network,

    #[arg(long, env = "BITCOIN_INDEXER_RPC_URL")]
    pub rpc_url: String,

    #[arg(long, env = "BITCOIN_INDEXER_RPC_AUTH")]
    pub rpc_auth: Option<String>,

    #[arg(long, env = "BITCOIN_INDEXER_ESPLORA_URL")]
    pub esplora_url: String,

    #[arg(long, env = "BITCOIN_INDEXER_ZMQ_RAWBLOCK_ENDPOINT")]
    pub zmq_rawblock_endpoint: String,

    #[arg(long, env = "BITCOIN_INDEXER_ZMQ_RAWTX_ENDPOINT")]
    pub zmq_rawtx_endpoint: String,

    #[arg(long, env = "BITCOIN_INDEXER_BIND", default_value = "0.0.0.0:8080")]
    pub bind: SocketAddr,

    #[arg(
        long,
        env = "BITCOIN_INDEXER_MAX_SUBSCRIBER_LAG",
        default_value_t = 10_000
    )]
    pub max_subscriber_lag: usize,

    #[arg(
        long,
        env = "BITCOIN_INDEXER_POLL_INTERVAL_MS",
        default_value_t = 12_000
    )]
    pub poll_interval_ms: u64,

    #[arg(
        long,
        env = "BITCOIN_INDEXER_ZMQ_RECONNECT_DELAY_MS",
        default_value_t = 1_000
    )]
    pub zmq_reconnect_delay_ms: u64,

    #[arg(long, env = "BITCOIN_INDEXER_REORG_RESCAN_DEPTH", default_value_t = 6)]
    pub reorg_rescan_depth: u64,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        validate_url(&self.rpc_url, &["http", "https"], "BITCOIN_INDEXER_RPC_URL")?;
        validate_url(
            &self.esplora_url,
            &["http", "https"],
            "BITCOIN_INDEXER_ESPLORA_URL",
        )?;
        validate_zmq_endpoint(
            &self.zmq_rawblock_endpoint,
            "BITCOIN_INDEXER_ZMQ_RAWBLOCK_ENDPOINT",
        )?;
        validate_zmq_endpoint(
            &self.zmq_rawtx_endpoint,
            "BITCOIN_INDEXER_ZMQ_RAWTX_ENDPOINT",
        )?;
        if self.max_subscriber_lag == 0 {
            return Err(Error::InvalidConfiguration {
                message: "BITCOIN_INDEXER_MAX_SUBSCRIBER_LAG must be positive".to_string(),
            });
        }
        if self.poll_interval_ms == 0 {
            return Err(Error::InvalidConfiguration {
                message: "BITCOIN_INDEXER_POLL_INTERVAL_MS must be positive".to_string(),
            });
        }
        if self.zmq_reconnect_delay_ms == 0 {
            return Err(Error::InvalidConfiguration {
                message: "BITCOIN_INDEXER_ZMQ_RECONNECT_DELAY_MS must be positive".to_string(),
            });
        }
        if self.reorg_rescan_depth == 0 {
            return Err(Error::InvalidConfiguration {
                message: "BITCOIN_INDEXER_REORG_RESCAN_DEPTH must be positive".to_string(),
            });
        }
        Ok(())
    }

    pub fn rpc_auth(&self) -> Result<Auth> {
        parse_rpc_auth(self.rpc_auth.as_deref())
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_millis(self.poll_interval_ms)
    }

    pub fn zmq_reconnect_delay(&self) -> Duration {
        Duration::from_millis(self.zmq_reconnect_delay_ms)
    }
}

fn parse_rpc_auth(value: Option<&str>) -> Result<Auth> {
    let Some(value) = value else {
        return Ok(Auth::None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(Auth::None);
    }
    if let Some(cookie_path) = trimmed.strip_prefix("cookie:") {
        return Ok(Auth::CookieFile(cookie_path.into()));
    }
    let Some((user, password)) = trimmed.split_once(':') else {
        return Err(Error::InvalidConfiguration {
            message: "BITCOIN_INDEXER_RPC_AUTH must be user:password or cookie:/path".to_string(),
        });
    };
    Ok(Auth::UserPass(user.to_string(), password.to_string()))
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

fn validate_zmq_endpoint(value: &str, env_name: &'static str) -> Result<()> {
    let trimmed = value.trim();
    if !trimmed.starts_with("tcp://") {
        return Err(Error::InvalidConfiguration {
            message: format!("{env_name} must use tcp://"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_userpass_rpc_auth() {
        assert!(matches!(
            parse_rpc_auth(Some("devnet:devnet")).unwrap(),
            Auth::UserPass(_, _)
        ));
    }

    #[test]
    fn parses_cookie_rpc_auth() {
        assert!(matches!(
            parse_rpc_auth(Some("cookie:/tmp/.cookie")).unwrap(),
            Auth::CookieFile(_)
        ));
    }
}
