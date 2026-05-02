use std::{fs, path::PathBuf};

use bitcoincore_rpc_async::Auth;
use clap::{Parser, ValueEnum};

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SauronReplicaEventSource {
    /// Consume a logical replication stream from the router replica.
    Cdc,
}

#[derive(Parser, Debug, Clone)]
#[command(name = "sauron")]
#[command(about = "Replica-backed provider-operation observer for tee-router")]
pub struct SauronArgs {
    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Replica database URL used for watch loading and notifications
    #[arg(long, env = "ROUTER_REPLICA_DATABASE_URL")]
    pub router_replica_database_url: String,

    /// Writable Sauron state database URL.
    ///
    /// This stores detector cursors and CDC checkpoints. If omitted, Sauron
    /// falls back to the router replica URL for local test deployments where
    /// the router database is writable.
    #[arg(long, env = "SAURON_STATE_DATABASE_URL")]
    pub sauron_state_database_url: Option<String>,

    /// Source used to learn about router replica changes.
    #[arg(
        long,
        env = "SAURON_REPLICA_EVENT_SOURCE",
        value_enum,
        default_value = "cdc"
    )]
    pub sauron_replica_event_source: SauronReplicaEventSource,

    /// Replica database name, used for logging and operational clarity
    #[arg(
        long,
        env = "ROUTER_REPLICA_DATABASE_NAME",
        default_value = "router_db"
    )]
    pub router_replica_database_name: String,

    /// Logical decoding slot used when SAURON_REPLICA_EVENT_SOURCE=cdc.
    #[arg(long, env = "SAURON_CDC_SLOT_NAME", default_value = "sauron_watch_cdc")]
    pub sauron_cdc_slot_name: String,

    /// Publication used by the pgoutput logical replication stream.
    #[arg(
        long,
        env = "ROUTER_CDC_PUBLICATION_NAME",
        default_value = "router_cdc_publication"
    )]
    pub router_cdc_publication_name: String,

    /// Message prefix emitted through pg_logical_emit_message by router DB triggers.
    #[arg(
        long,
        env = "ROUTER_CDC_MESSAGE_PREFIX",
        default_value = "rift.router.change"
    )]
    pub router_cdc_message_prefix: String,

    /// Interval for replication status updates to Postgres.
    #[arg(long, env = "SAURON_CDC_STATUS_INTERVAL_MS", default_value_t = 1000)]
    pub sauron_cdc_status_interval_ms: u64,

    /// Idle replication read wakeup interval, used only to send keepalive feedback.
    #[arg(
        long,
        env = "SAURON_CDC_IDLE_WAKEUP_INTERVAL_MS",
        default_value_t = 10_000
    )]
    pub sauron_cdc_idle_wakeup_interval_ms: u64,

    /// Base URL for the router server provider-operation hint route
    #[arg(long, env = "ROUTER_INTERNAL_BASE_URL")]
    pub router_internal_base_url: String,

    /// Trusted detector API bearer key
    #[arg(long, env = "ROUTER_DETECTOR_API_KEY")]
    pub router_detector_api_key: String,

    /// Electrum HTTP Server URL
    #[arg(long, env = "ELECTRUM_HTTP_SERVER_URL")]
    pub electrum_http_server_url: String,

    /// Direct Bitcoin Core RPC URL used for tip, block, and mempool reconciliation
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Bitcoin Core RPC authentication
    #[arg(long, env = "BITCOIN_RPC_AUTH", value_parser = parse_auth)]
    pub bitcoin_rpc_auth: Auth,

    /// Bitcoin Core ZMQ raw transaction endpoint
    #[arg(long, env = "BITCOIN_ZMQ_RAWTX_ENDPOINT")]
    pub bitcoin_zmq_rawtx_endpoint: String,

    /// Bitcoin Core ZMQ mempool sequence endpoint
    #[arg(long, env = "BITCOIN_ZMQ_SEQUENCE_ENDPOINT")]
    pub bitcoin_zmq_sequence_endpoint: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "ETH_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet Token Indexer URL
    #[arg(long, env = "ETHEREUM_TOKEN_INDEXER_URL")]
    pub ethereum_token_indexer_url: Option<String>,

    /// Legacy Ethereum token address kept for backwards-compatible deployment config.
    /// ERC-20 detection now follows the active watch set instead of this single token.
    #[arg(
        long,
        env = "ETHEREUM_ALLOWED_TOKEN",
        default_value = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
    )]
    pub ethereum_allowed_token: String,

    /// Base RPC URL
    #[arg(long, env = "BASE_RPC_URL")]
    pub base_rpc_url: String,

    /// Base Token Indexer URL
    #[arg(long, env = "BASE_TOKEN_INDEXER_URL")]
    pub base_token_indexer_url: Option<String>,

    /// Legacy Base token address kept for backwards-compatible deployment config.
    /// ERC-20 detection now follows the active watch set instead of this single token.
    #[arg(
        long,
        env = "BASE_ALLOWED_TOKEN",
        default_value = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"
    )]
    pub base_allowed_token: String,

    /// Arbitrum RPC URL
    #[arg(long, env = "ARBITRUM_RPC_URL")]
    pub arbitrum_rpc_url: String,

    /// Arbitrum Token Indexer URL
    #[arg(long, env = "ARBITRUM_TOKEN_INDEXER_URL")]
    pub arbitrum_token_indexer_url: Option<String>,

    /// Legacy Arbitrum token address kept for backwards-compatible deployment config.
    /// ERC-20 detection now follows the active watch set instead of this single token.
    #[arg(
        long,
        env = "ARBITRUM_ALLOWED_TOKEN",
        default_value = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
    )]
    pub arbitrum_allowed_token: String,

    /// Low-frequency full watch-set reconcile interval
    #[arg(
        long,
        env = "SAURON_RECONCILE_INTERVAL_SECONDS",
        default_value = "3600"
    )]
    pub sauron_reconcile_interval_seconds: u64,

    /// Temporary poll interval for the first Bitcoin detector implementation
    #[arg(
        long,
        env = "SAURON_BITCOIN_SCAN_INTERVAL_SECONDS",
        default_value = "15"
    )]
    pub sauron_bitcoin_scan_interval_seconds: u64,

    /// Maximum number of concurrent Bitcoin indexed lookups
    #[arg(
        long,
        env = "SAURON_BITCOIN_INDEXED_LOOKUP_CONCURRENCY",
        default_value = "32"
    )]
    pub sauron_bitcoin_indexed_lookup_concurrency: usize,

    /// Maximum number of concurrent EVM indexed lookups per backend
    #[arg(
        long,
        env = "SAURON_EVM_INDEXED_LOOKUP_CONCURRENCY",
        default_value = "8"
    )]
    pub sauron_evm_indexed_lookup_concurrency: usize,
}

fn parse_auth(s: &str) -> Result<Auth, String> {
    if s.eq_ignore_ascii_case("none") {
        Ok(Auth::None)
    } else if fs::exists(s).map_err(|error| error.to_string())? {
        Ok(Auth::CookieFile(PathBuf::from(s)))
    } else {
        let mut split = s.splitn(2, ':');
        let user = split.next().ok_or("Invalid auth string")?;
        let password = split.next().ok_or("Invalid auth string")?;
        Ok(Auth::UserPass(user.to_string(), password.to_string()))
    }
}
