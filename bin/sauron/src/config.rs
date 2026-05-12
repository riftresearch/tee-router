use clap::{Parser, ValueEnum};
use std::fmt;

use crate::cdc::{ROUTER_CDC_MESSAGE_PREFIX, ROUTER_CDC_PUBLICATION_NAME};

pub const MIN_ROUTER_DETECTOR_API_KEY_LEN: usize = 32;
pub const MIN_TOKEN_INDEXER_API_KEY_LEN: usize = 32;

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SauronReplicaEventSource {
    /// Consume the router CDC stream from the router replica.
    Cdc,
}

#[derive(Parser, Clone)]
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
    /// This stores detector cursors and CDC checkpoints. It must be writable and
    /// must not point at a physical router standby.
    #[arg(long, env = "SAURON_STATE_DATABASE_URL")]
    pub sauron_state_database_url: String,

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

    /// CDC decoding slot used when SAURON_REPLICA_EVENT_SOURCE=cdc.
    #[arg(long, env = "SAURON_CDC_SLOT_NAME", default_value = "sauron_watch_cdc")]
    pub sauron_cdc_slot_name: String,

    /// Publication used by the pgoutput CDC stream.
    #[arg(
        long,
        env = "ROUTER_CDC_PUBLICATION_NAME",
        default_value_t = ROUTER_CDC_PUBLICATION_NAME.to_string()
    )]
    pub router_cdc_publication_name: String,

    /// Message prefix emitted by router DB CDC triggers.
    #[arg(
        long,
        env = "ROUTER_CDC_MESSAGE_PREFIX",
        default_value_t = ROUTER_CDC_MESSAGE_PREFIX.to_string()
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

    /// Bitcoin indexer API URL
    #[arg(long, env = "BITCOIN_INDEXER_URL")]
    pub bitcoin_indexer_url: Option<String>,

    /// Bitcoin receipt watcher API URL
    #[arg(long, env = "BITCOIN_RECEIPT_WATCHER_URL")]
    pub bitcoin_receipt_watcher_url: Option<String>,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "ETH_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet Token Indexer URL
    #[arg(long, env = "ETHEREUM_TOKEN_INDEXER_URL")]
    pub ethereum_token_indexer_url: Option<String>,

    /// Ethereum Mainnet Receipt Watcher URL
    #[arg(long, env = "ETHEREUM_RECEIPT_WATCHER_URL")]
    pub ethereum_receipt_watcher_url: Option<String>,

    /// Base RPC URL
    #[arg(long, env = "BASE_RPC_URL")]
    pub base_rpc_url: String,

    /// Base Token Indexer URL
    #[arg(long, env = "BASE_TOKEN_INDEXER_URL")]
    pub base_token_indexer_url: Option<String>,

    /// Base Receipt Watcher URL
    #[arg(long, env = "BASE_RECEIPT_WATCHER_URL")]
    pub base_receipt_watcher_url: Option<String>,

    /// Arbitrum RPC URL
    #[arg(long, env = "ARBITRUM_RPC_URL")]
    pub arbitrum_rpc_url: String,

    /// Arbitrum Token Indexer URL
    #[arg(long, env = "ARBITRUM_TOKEN_INDEXER_URL")]
    pub arbitrum_token_indexer_url: Option<String>,

    /// Arbitrum Receipt Watcher URL
    #[arg(long, env = "ARBITRUM_RECEIPT_WATCHER_URL")]
    pub arbitrum_receipt_watcher_url: Option<String>,

    /// Hyperliquid shim indexer URL
    #[arg(long, env = "HL_SHIM_INDEXER_URL")]
    pub hl_shim_indexer_url: Option<String>,

    /// HyperUnit API base URL
    #[arg(long, env = "HYPERUNIT_API_URL")]
    pub hyperunit_api_url: Option<String>,

    /// Optional HyperUnit SOCKS5 proxy URL
    #[arg(long, env = "HYPERUNIT_PROXY_URL")]
    pub hyperunit_proxy_url: Option<String>,

    /// HL bridge Arbitrum/HL ledger correlation window, in seconds
    #[arg(
        long,
        env = "SAURON_HL_BRIDGE_MATCH_WINDOW_SECONDS",
        default_value = "1800"
    )]
    pub sauron_hl_bridge_match_window_seconds: i64,

    /// Bearer key shared by configured EVM token-indexer APIs
    #[arg(long, env = "TOKEN_INDEXER_API_KEY")]
    pub token_indexer_api_key: Option<String>,

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

impl fmt::Debug for SauronArgs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SauronArgs")
            .field("log_level", &self.log_level)
            .field("router_replica_database_url", &"<redacted>")
            .field("sauron_state_database_url", &"<redacted>")
            .field(
                "sauron_replica_event_source",
                &self.sauron_replica_event_source,
            )
            .field(
                "router_replica_database_name",
                &self.router_replica_database_name,
            )
            .field("sauron_cdc_slot_name", &self.sauron_cdc_slot_name)
            .field(
                "router_cdc_publication_name",
                &self.router_cdc_publication_name,
            )
            .field("router_cdc_message_prefix", &self.router_cdc_message_prefix)
            .field(
                "sauron_cdc_status_interval_ms",
                &self.sauron_cdc_status_interval_ms,
            )
            .field(
                "sauron_cdc_idle_wakeup_interval_ms",
                &self.sauron_cdc_idle_wakeup_interval_ms,
            )
            .field("router_internal_base_url", &"<redacted>")
            .field("router_detector_api_key", &"<redacted>")
            .field(
                "bitcoin_indexer_url",
                &self.bitcoin_indexer_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "bitcoin_receipt_watcher_url",
                &self
                    .bitcoin_receipt_watcher_url
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("ethereum_mainnet_rpc_url", &"<redacted>")
            .field(
                "ethereum_token_indexer_url",
                &self
                    .ethereum_token_indexer_url
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field(
                "ethereum_receipt_watcher_url",
                &self
                    .ethereum_receipt_watcher_url
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field("base_rpc_url", &"<redacted>")
            .field(
                "base_token_indexer_url",
                &self.base_token_indexer_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "base_receipt_watcher_url",
                &self.base_receipt_watcher_url.as_ref().map(|_| "<redacted>"),
            )
            .field("arbitrum_rpc_url", &"<redacted>")
            .field(
                "arbitrum_token_indexer_url",
                &self
                    .arbitrum_token_indexer_url
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field(
                "arbitrum_receipt_watcher_url",
                &self
                    .arbitrum_receipt_watcher_url
                    .as_ref()
                    .map(|_| "<redacted>"),
            )
            .field(
                "hl_shim_indexer_url",
                &self.hl_shim_indexer_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "hyperunit_api_url",
                &self.hyperunit_api_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "hyperunit_proxy_url",
                &self.hyperunit_proxy_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "sauron_hl_bridge_match_window_seconds",
                &self.sauron_hl_bridge_match_window_seconds,
            )
            .field(
                "token_indexer_api_key",
                &self.token_indexer_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "sauron_reconcile_interval_seconds",
                &self.sauron_reconcile_interval_seconds,
            )
            .field(
                "sauron_bitcoin_scan_interval_seconds",
                &self.sauron_bitcoin_scan_interval_seconds,
            )
            .field(
                "sauron_bitcoin_indexed_lookup_concurrency",
                &self.sauron_bitcoin_indexed_lookup_concurrency,
            )
            .field(
                "sauron_evm_indexed_lookup_concurrency",
                &self.sauron_evm_indexed_lookup_concurrency,
            )
            .finish()
    }
}

pub fn normalize_router_detector_api_key(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.len() < MIN_ROUTER_DETECTOR_API_KEY_LEN {
        return None;
    }
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sauron_args_debug_redacts_secret_configuration() {
        let args = SauronArgs {
            log_level: "debug".to_string(),
            router_replica_database_url: "postgres://replicator:db-secret@db/router".to_string(),
            sauron_state_database_url: "postgres://sauron:state-secret@db/sauron".to_string(),
            sauron_replica_event_source: SauronReplicaEventSource::Cdc,
            router_replica_database_name: "router_db".to_string(),
            sauron_cdc_slot_name: "sauron_slot".to_string(),
            router_cdc_publication_name: "router_publication".to_string(),
            router_cdc_message_prefix: "rift.router.change".to_string(),
            sauron_cdc_status_interval_ms: 1_000,
            sauron_cdc_idle_wakeup_interval_ms: 10_000,
            router_internal_base_url: "https://router.internal/api-key-path".to_string(),
            router_detector_api_key: "detector-secret-00000000000000000000".to_string(),
            bitcoin_indexer_url: Some("https://bitcoin-indexer.example/token-secret".to_string()),
            bitcoin_receipt_watcher_url: Some(
                "https://bitcoin-receipts.example/token-secret".to_string(),
            ),
            ethereum_mainnet_rpc_url: "https://eth.example/rpc-secret".to_string(),
            ethereum_token_indexer_url: Some(
                "https://eth-indexer.example/token-secret".to_string(),
            ),
            ethereum_receipt_watcher_url: Some(
                "https://eth-receipts.example/receipt-secret".to_string(),
            ),
            base_rpc_url: "https://base.example/rpc-secret".to_string(),
            base_token_indexer_url: Some("https://base-indexer.example/token-secret".to_string()),
            base_receipt_watcher_url: Some(
                "https://base-receipts.example/receipt-secret".to_string(),
            ),
            arbitrum_rpc_url: "https://arb.example/rpc-secret".to_string(),
            arbitrum_token_indexer_url: Some(
                "https://arb-indexer.example/token-secret".to_string(),
            ),
            arbitrum_receipt_watcher_url: Some(
                "https://arb-receipts.example/receipt-secret".to_string(),
            ),
            hl_shim_indexer_url: Some("https://hl-shim.example/token-secret".to_string()),
            hyperunit_api_url: Some("https://hyperunit.example/token-secret".to_string()),
            hyperunit_proxy_url: Some("socks5://hyperunit-proxy-secret:1080".to_string()),
            sauron_hl_bridge_match_window_seconds: 1_800,
            token_indexer_api_key: Some("token-indexer-api-key-secret".to_string()),
            sauron_reconcile_interval_seconds: 3600,
            sauron_bitcoin_scan_interval_seconds: 15,
            sauron_bitcoin_indexed_lookup_concurrency: 32,
            sauron_evm_indexed_lookup_concurrency: 8,
        };

        let rendered = format!("{args:?}");
        assert!(rendered.contains("SauronArgs"));
        assert!(rendered.contains("<redacted>"));
        for secret in [
            "db-secret",
            "state-secret",
            "detector-secret",
            "api-key-path",
            "token-secret",
            "receipt-secret",
            "token-indexer-api-key-secret",
            "rpc-secret",
            "bitcoin-user",
            "bitcoin-pass",
            "hyperunit-proxy-secret",
        ] {
            assert!(
                !rendered.contains(secret),
                "debug output leaked {secret}: {rendered}"
            );
        }
    }
}
