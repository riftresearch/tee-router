use std::{fs, net::IpAddr, path::PathBuf};

use bitcoincore_rpc_async::Auth;
use clap::Parser;
use snafu::{prelude::*, Whatever};

pub mod api;
pub mod app;
pub mod error;
pub mod query_api;
pub mod runtime;
pub mod server;
pub mod services;
pub mod telemetry;
pub mod worker;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to bind server"))]
    ServerBind { source: std::io::Error },

    #[snafu(display("Server failed to start"))]
    ServerStart { source: std::io::Error },

    #[snafu(display("Database initialization failed: {}", source))]
    DatabaseInit { source: error::RouterServerError },

    #[snafu(display("Generic error: {}", source))]
    Generic { source: Whatever },
}

impl From<Whatever> for Error {
    fn from(err: Whatever) -> Self {
        Error::Generic { source: err }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Parser, Clone)]
#[command(about = "Deposit-vault router server")]
pub struct RouterServerArgs {
    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    pub host: IpAddr,

    /// Port to bind to
    #[arg(short, long, default_value = "4522")]
    pub port: u16,

    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://router_user:router_password@localhost:5432/router_db"
    )]
    pub database_url: String,

    /// Database max connections
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "32")]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "4")]
    pub db_min_connections: u32,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// File path to the router master key hex file
    #[arg(long, env = "ROUTER_MASTER_KEY_PATH")]
    pub master_key_path: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "ETH_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum reference token address
    #[arg(
        long,
        env = "ETHEREUM_ALLOWED_TOKEN",
        default_value = "0x0000000000000000000000000000000000000000"
    )]
    pub ethereum_reference_token: String,

    /// Ethereum paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "ETHEREUM_PAYMASTER_PRIVATE_KEY")]
    pub ethereum_paymaster_private_key: Option<String>,

    /// Base RPC URL
    #[arg(long, env = "BASE_RPC_URL")]
    pub base_rpc_url: String,

    /// Base reference token address
    #[arg(
        long,
        env = "BASE_ALLOWED_TOKEN",
        default_value = "0x0000000000000000000000000000000000000000"
    )]
    pub base_reference_token: String,

    /// Base paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "BASE_PAYMASTER_PRIVATE_KEY")]
    pub base_paymaster_private_key: Option<String>,

    /// Arbitrum RPC URL
    #[arg(long, env = "ARBITRUM_RPC_URL")]
    pub arbitrum_rpc_url: String,

    /// Arbitrum reference token address
    #[arg(
        long,
        env = "ARBITRUM_ALLOWED_TOKEN",
        default_value = "0x0000000000000000000000000000000000000000"
    )]
    pub arbitrum_reference_token: String,

    /// Arbitrum paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "ARBITRUM_PAYMASTER_PRIVATE_KEY")]
    pub arbitrum_paymaster_private_key: Option<String>,

    /// Extra native balance to leave in EVM token vaults after action gas estimate, in wei
    #[arg(long, env = "EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI", default_value = "0")]
    pub evm_paymaster_vault_gas_buffer_wei: String,

    /// Deprecated alias for EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI
    #[arg(long, env = "EVM_PAYMASTER_VAULT_GAS_TARGET_WEI", hide = true)]
    pub evm_paymaster_vault_gas_target_wei: Option<String>,

    /// Bitcoin RPC URL
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Bitcoin RPC Auth
    #[arg(long, env = "BITCOIN_RPC_AUTH", default_value = "none", value_parser = parse_auth)]
    pub bitcoin_rpc_auth: Auth,

    /// Electrum HTTP Server URL
    #[arg(long, env = "ELECTRUM_HTTP_SERVER_URL")]
    pub untrusted_esplora_http_server_url: String,

    /// Bitcoin Network
    #[arg(long, env = "BITCOIN_NETWORK", default_value = "bitcoin")]
    pub bitcoin_network: bitcoin::Network,

    /// Bitcoin paymaster private key used as the recovery/sweep destination for
    /// released internal bitcoin custody
    #[arg(long, env = "BITCOIN_PAYMASTER_PRIVATE_KEY")]
    pub bitcoin_paymaster_private_key: Option<String>,

    /// CORS domain to allow (supports wildcards like "*.example.com")
    #[arg(long = "corsdomain", env = "CORS_DOMAIN")]
    pub cors_domain: Option<String>,

    /// Chainalysis Address Screening API base URL
    #[arg(long, env = "CHAINALYSIS_HOST")]
    pub chainalysis_host: Option<String>,

    /// Chainalysis Address Screening API token
    #[arg(long, env = "CHAINALYSIS_TOKEN")]
    pub chainalysis_token: Option<String>,

    /// Loki logging URL (if provided, logs will be shipped to Loki)
    #[arg(long, env = "LOKI_URL")]
    pub loki_url: Option<String>,

    /// Across API base URL
    #[arg(long, env = "ACROSS_API_URL")]
    pub across_api_url: Option<String>,

    /// Across API bearer token
    #[arg(long, env = "ACROSS_API_KEY")]
    pub across_api_key: Option<String>,

    /// Across integrator id sent on swap approval requests
    #[arg(long, env = "ACROSS_INTEGRATOR_ID")]
    pub across_integrator_id: Option<String>,

    /// Circle Iris CCTP API base URL
    #[arg(long, env = "CCTP_API_URL")]
    pub cctp_api_url: Option<String>,

    /// CCTP TokenMessengerV2 contract address override
    #[arg(long, env = "CCTP_TOKEN_MESSENGER_V2_ADDRESS")]
    pub cctp_token_messenger_v2_address: Option<String>,

    /// CCTP MessageTransmitterV2 contract address override
    #[arg(long, env = "CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS")]
    pub cctp_message_transmitter_v2_address: Option<String>,

    /// HyperUnit API base URL
    #[arg(long, env = "HYPERUNIT_API_URL")]
    pub hyperunit_api_url: Option<String>,

    /// Optional HyperUnit SOCKS5 proxy URL
    #[arg(long, env = "HYPERUNIT_PROXY_URL")]
    pub hyperunit_proxy_url: Option<String>,

    /// Hyperliquid API base URL
    #[arg(long, env = "HYPERLIQUID_API_URL")]
    pub hyperliquid_api_url: Option<String>,

    /// Velora/ParaSwap Market API base URL
    #[arg(long, env = "VELORA_API_URL")]
    pub velora_api_url: Option<String>,

    /// Partner string sent to Velora for route analytics
    #[arg(long, env = "VELORA_PARTNER")]
    pub velora_partner: Option<String>,

    /// Legacy shared Hyperliquid execution signer private key. Startup rejects
    /// this because router execution must use per-order derived identities.
    #[arg(long, env = "HYPERLIQUID_EXECUTION_PRIVATE_KEY")]
    pub hyperliquid_execution_private_key: Option<String>,

    /// Legacy shared Hyperliquid account/user address. Startup rejects this
    /// because router execution must use per-order derived identities.
    #[arg(long, env = "HYPERLIQUID_ACCOUNT_ADDRESS")]
    pub hyperliquid_account_address: Option<String>,

    /// Legacy shared Hyperliquid vault/subaccount address. Startup rejects
    /// this because router execution must use per-order derived identities.
    #[arg(long, env = "HYPERLIQUID_VAULT_ADDRESS")]
    pub hyperliquid_vault_address: Option<String>,

    /// Hyperliquid paymaster private key used as the recovery/sweep destination
    /// for released internal Hyperliquid custody
    #[arg(long, env = "HYPERLIQUID_PAYMASTER_PRIVATE_KEY")]
    pub hyperliquid_paymaster_private_key: Option<String>,

    /// Bearer API key accepted from Sauron on internal detector callbacks
    #[arg(long, env = "ROUTER_DETECTOR_API_KEY")]
    pub router_detector_api_key: Option<String>,

    /// Bearer API key accepted from the public router gateway for quote/order
    /// endpoints. Required when router-api binds a non-loopback host because
    /// create-order responses contain raw cancellation secrets.
    #[arg(long, env = "ROUTER_GATEWAY_API_KEY")]
    pub router_gateway_api_key: Option<String>,

    /// Bearer API key accepted on internal admin endpoints such as provider
    /// policy updates
    #[arg(long, env = "ROUTER_ADMIN_API_KEY")]
    pub router_admin_api_key: Option<String>,

    /// Hyperliquid network (mainnet|testnet) — selects the L1 signing
    /// "source" byte and the `hyperliquidChain` string stamped into
    /// user-type actions (Withdraw3, …).
    #[arg(
        long,
        env = "HYPERLIQUID_NETWORK",
        default_value = "mainnet",
        value_parser = parse_hyperliquid_network,
    )]
    pub hyperliquid_network: router_core::services::custody_action_executor::HyperliquidCallNetwork,

    /// Timeout for Hyperliquid resting orders before the router arms the
    /// exchange-side dead-man switch to cancel them, in milliseconds.
    #[arg(long, env = "HYPERLIQUID_ORDER_TIMEOUT_MS", default_value = "30000")]
    pub hyperliquid_order_timeout_ms: u64,

    /// Stable worker identity for the router-worker database lease
    #[arg(long, env = "ROUTER_WORKER_ID")]
    pub worker_id: Option<String>,

    /// Global worker lease name
    #[arg(
        long,
        env = "ROUTER_WORKER_LEASE_NAME",
        default_value = "global-router-worker"
    )]
    pub worker_lease_name: Option<String>,

    /// Worker leadership lease duration, in seconds
    #[arg(long, env = "ROUTER_WORKER_LEASE_SECONDS", default_value = "300")]
    pub worker_lease_seconds: u64,

    /// Worker leadership lease renewal interval, in seconds
    #[arg(long, env = "ROUTER_WORKER_LEASE_RENEW_SECONDS", default_value = "30")]
    pub worker_lease_renew_seconds: u64,

    /// Standby worker poll interval for trying to acquire leadership, in seconds
    #[arg(long, env = "ROUTER_WORKER_STANDBY_POLL_SECONDS", default_value = "5")]
    pub worker_standby_poll_seconds: u64,

    /// Vault work safety-sweep interval. LISTEN/NOTIFY drives normal funding and
    /// refund wakeups, but this periodic pass recovers missed wakeups and stale
    /// claimed funding hints.
    #[arg(long, env = "ROUTER_WORKER_REFUND_POLL_SECONDS", default_value = "60")]
    pub worker_refund_poll_seconds: u64,

    /// Order execution safety-sweep interval. LISTEN/NOTIFY drives normal
    /// low-latency wakeups, but this periodic pass recovers missed wakeups and
    /// retryable execution errors.
    #[arg(
        long,
        env = "ROUTER_WORKER_ORDER_EXECUTION_POLL_SECONDS",
        default_value = "5"
    )]
    pub worker_order_execution_poll_seconds: u64,

    /// Active router-worker route-cost refresh interval, in seconds
    #[arg(
        long,
        env = "ROUTER_WORKER_ROUTE_COST_REFRESH_SECONDS",
        default_value = "300"
    )]
    pub worker_route_cost_refresh_seconds: u64,

    /// Active router-worker provider health poll interval, in seconds
    #[arg(
        long,
        env = "ROUTER_WORKER_PROVIDER_HEALTH_POLL_SECONDS",
        default_value = "120"
    )]
    pub worker_provider_health_poll_seconds: u64,

    /// Timeout for individual provider health probes, in seconds
    #[arg(
        long,
        env = "ROUTER_PROVIDER_HEALTH_TIMEOUT_SECONDS",
        default_value = "10"
    )]
    pub provider_health_timeout_seconds: u64,

    /// Provider-operation observation hints to claim per router-worker hint pass
    #[arg(
        long,
        env = "ROUTER_WORKER_PROVIDER_OPERATION_HINT_PASS_LIMIT",
        default_value = "500"
    )]
    pub worker_provider_operation_hint_pass_limit: u32,

    /// Order maintenance rows to process per router-worker global pass
    #[arg(
        long,
        env = "ROUTER_WORKER_ORDER_MAINTENANCE_PASS_LIMIT",
        default_value = "100"
    )]
    pub worker_order_maintenance_pass_limit: u32,

    /// Orders to plan per router-worker global planning pass
    #[arg(
        long,
        env = "ROUTER_WORKER_ORDER_PLANNING_PASS_LIMIT",
        default_value = "100"
    )]
    pub worker_order_planning_pass_limit: u32,

    /// Ready orders to execute per router-worker global execution pass
    #[arg(
        long,
        env = "ROUTER_WORKER_ORDER_EXECUTION_PASS_LIMIT",
        default_value = "25"
    )]
    pub worker_order_execution_pass_limit: u32,

    /// Ready orders to execute concurrently within each router-worker execution pass
    #[arg(
        long,
        env = "ROUTER_WORKER_ORDER_EXECUTION_CONCURRENCY",
        default_value = "64"
    )]
    pub worker_order_execution_concurrency: u32,

    /// Vault funding hints to claim per router-worker vault pass
    #[arg(
        long,
        env = "ROUTER_WORKER_VAULT_FUNDING_HINT_PASS_LIMIT",
        default_value = "100"
    )]
    pub worker_vault_funding_hint_pass_limit: u32,

    /// Coinbase unauthenticated price API base URL used by route-cost pricing refresh
    #[arg(
        long,
        env = "COINBASE_PRICE_API_BASE_URL",
        default_value = "https://api.coinbase.com"
    )]
    pub coinbase_price_api_base_url: String,
}

impl RouterServerArgs {
    pub fn resolved_master_key_path(&self) -> std::result::Result<PathBuf, String> {
        let trimmed = self.master_key_path.trim();
        if trimmed.is_empty() {
            return Err("ROUTER_MASTER_KEY_PATH must not be empty".to_string());
        }
        Ok(PathBuf::from(trimmed))
    }
}

fn parse_hyperliquid_network(
    s: &str,
) -> std::result::Result<
    router_core::services::custody_action_executor::HyperliquidCallNetwork,
    String,
> {
    use router_core::services::custody_action_executor::HyperliquidCallNetwork;
    match s.to_ascii_lowercase().as_str() {
        "mainnet" => Ok(HyperliquidCallNetwork::Mainnet),
        "testnet" => Ok(HyperliquidCallNetwork::Testnet),
        other => Err(format!(
            "invalid hyperliquid network {other:?}: expected mainnet or testnet"
        )),
    }
}

fn parse_auth(s: &str) -> std::result::Result<Auth, String> {
    if s.eq_ignore_ascii_case("none") {
        Ok(Auth::None)
    } else if fs::exists(s).map_err(|e| e.to_string())? {
        Ok(Auth::CookieFile(PathBuf::from(s)))
    } else {
        let mut split = s.splitn(2, ':');
        let username = split.next().ok_or("Invalid auth string")?;
        let password = split.next().ok_or("Invalid auth string")?;
        Ok(Auth::UserPass(username.to_string(), password.to_string()))
    }
}
