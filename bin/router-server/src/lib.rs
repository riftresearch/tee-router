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

    /// CORS domain to allow (supports wildcards like "*.example.com")
    #[arg(long = "corsdomain", env = "CORS_DOMAIN")]
    pub cors_domain: Option<String>,

    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://router_user:router_password@localhost:5432/router_db"
    )]
    pub database_url: String,

    /// Database max connections
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "100")]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "4")]
    pub db_min_connections: u32,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    pub log_level: String,

    /// Enforce production upstream URL/proxy configuration
    #[arg(
        long,
        env = "PRODUCTION",
        default_value_t = true,
        action = clap::ArgAction::Set
    )]
    pub production: bool,

    /// URL for the ipv4-us-west-1 proxy profile.
    #[arg(long, env = "PROXY_PROFILE_IPV4_US_WEST_1_URL")]
    pub proxy_profile_ipv4_us_west_1_url: Option<String>,

    /// DNS mode for the ipv4-us-west-1 proxy profile.
    #[arg(
        long,
        env = "PROXY_PROFILE_IPV4_US_WEST_1_DNS_MODE",
        default_value = "system-default"
    )]
    pub proxy_profile_ipv4_us_west_1_dns_mode: String,

    /// URL for the ipv6-us-west-1 proxy profile.
    #[arg(long, env = "PROXY_PROFILE_IPV6_US_WEST_1_URL")]
    pub proxy_profile_ipv6_us_west_1_url: Option<String>,

    /// DNS mode for the ipv6-us-west-1 proxy profile.
    #[arg(
        long,
        env = "PROXY_PROFILE_IPV6_US_WEST_1_DNS_MODE",
        default_value = "local-ipv6-only"
    )]
    pub proxy_profile_ipv6_us_west_1_dns_mode: String,

    /// URL for the ipv4-eu proxy profile.
    #[arg(long, env = "PROXY_PROFILE_IPV4_EU_URL")]
    pub proxy_profile_ipv4_eu_url: Option<String>,

    /// DNS mode for the ipv4-eu proxy profile.
    #[arg(
        long,
        env = "PROXY_PROFILE_IPV4_EU_DNS_MODE",
        default_value = "system-default"
    )]
    pub proxy_profile_ipv4_eu_dns_mode: String,

    /// File path to the router master key hex file
    #[arg(long, env = "ROUTER_MASTER_KEY_PATH")]
    pub master_key_path: String,

    /// Ethereum Mainnet RPC URL
    #[arg(long, env = "ETH_RPC_URL")]
    pub ethereum_mainnet_rpc_url: String,

    /// Ethereum Mainnet RPC proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "ETH_RPC_PROXY_PROFILE")]
    pub ethereum_mainnet_rpc_proxy_profile: Option<String>,

    /// Flashbots Ethereum RPC URL used for FlashbotsIfEthereum broadcasts
    #[arg(long, env = "FLASHBOTS_RPC_URL")]
    pub flashbots_rpc_url: Option<String>,

    /// Ethereum paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "ETHEREUM_PAYMASTER_PRIVATE_KEY")]
    pub ethereum_paymaster_private_key: Option<String>,

    /// Base RPC URL
    #[arg(long, env = "BASE_RPC_URL")]
    pub base_rpc_url: String,

    /// Base RPC proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "BASE_RPC_PROXY_PROFILE")]
    pub base_rpc_proxy_profile: Option<String>,

    /// Base paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "BASE_PAYMASTER_PRIVATE_KEY")]
    pub base_paymaster_private_key: Option<String>,

    /// Arbitrum RPC URL
    #[arg(long, env = "ARBITRUM_RPC_URL")]
    pub arbitrum_rpc_url: String,

    /// Arbitrum RPC proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "ARBITRUM_RPC_PROXY_PROFILE")]
    pub arbitrum_rpc_proxy_profile: Option<String>,

    /// Arbitrum paymaster private key used to top up EVM token vault gas
    #[arg(long, env = "ARBITRUM_PAYMASTER_PRIVATE_KEY")]
    pub arbitrum_paymaster_private_key: Option<String>,

    /// Bitcoin RPC URL
    #[arg(long, env = "BITCOIN_RPC_URL")]
    pub bitcoin_rpc_url: String,

    /// Bitcoin RPC proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "BITCOIN_RPC_PROXY_PROFILE")]
    pub bitcoin_rpc_proxy_profile: Option<String>,

    /// Bitcoin RPC Auth
    #[arg(long, env = "BITCOIN_RPC_AUTH", default_value = "none", value_parser = parse_auth)]
    pub bitcoin_rpc_auth: Auth,

    /// Esplora HTTP Server URL
    #[arg(long, env = "ESPLORA_HTTP_SERVER_URL")]
    pub untrusted_esplora_http_server_url: String,

    /// Esplora proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "ESPLORA_PROXY_PROFILE")]
    pub esplora_proxy_profile: Option<String>,

    /// Bitcoin Network
    #[arg(long, env = "BITCOIN_NETWORK", default_value = "bitcoin")]
    pub bitcoin_network: bitcoin::Network,

    /// Chainalysis Address Screening API base URL
    #[arg(long, env = "CHAINALYSIS_HOST")]
    pub chainalysis_host: Option<String>,

    /// Chainalysis Address Screening API token
    #[arg(long, env = "CHAINALYSIS_TOKEN")]
    pub chainalysis_token: Option<String>,

    /// Chainalysis proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "CHAINALYSIS_PROXY_PROFILE")]
    pub chainalysis_proxy_profile: Option<String>,

    /// Loki logging URL (if provided, logs will be shipped to Loki)
    #[arg(long, env = "LOKI_URL")]
    pub loki_url: Option<String>,

    /// Across API base URL
    #[arg(long, env = "ACROSS_API_URL")]
    pub across_api_url: Option<String>,

    /// Across API bearer token
    #[arg(long, env = "ACROSS_API_KEY")]
    pub across_api_key: Option<String>,

    /// Across proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "ACROSS_PROXY_PROFILE")]
    pub across_proxy_profile: Option<String>,

    /// Across integrator id sent on swap approval requests
    #[arg(long, env = "ACROSS_INTEGRATOR_ID")]
    pub across_integrator_id: Option<String>,

    /// Circle Iris CCTP API base URL
    #[arg(long, env = "CCTP_API_URL")]
    pub cctp_api_url: Option<String>,

    /// CCTP proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "CCTP_PROXY_PROFILE")]
    pub cctp_proxy_profile: Option<String>,

    /// CCTP TokenMessengerV2 contract address override
    #[arg(long, env = "CCTP_TOKEN_MESSENGER_V2_ADDRESS")]
    pub cctp_token_messenger_v2_address: Option<String>,

    /// CCTP MessageTransmitterV2 contract address override
    #[arg(long, env = "CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS")]
    pub cctp_message_transmitter_v2_address: Option<String>,

    /// CCTP transfer mode (`standard` or `fast`)
    #[arg(long, env = "CCTP_TRANSFER_MODE")]
    pub cctp_transfer_mode: Option<String>,
    /// HyperUnit API base URL
    #[arg(long, env = "HYPERUNIT_API_URL")]
    pub hyperunit_api_url: Option<String>,

    /// HyperUnit proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "HYPERUNIT_PROXY_PROFILE")]
    pub hyperunit_proxy_profile: Option<String>,

    /// Hyperliquid API base URL
    #[arg(long, env = "HYPERLIQUID_API_URL")]
    pub hyperliquid_api_url: Option<String>,

    /// Hyperliquid proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "HYPERLIQUID_PROXY_PROFILE")]
    pub hyperliquid_proxy_profile: Option<String>,

    /// Velora/ParaSwap Market API base URL
    #[arg(long, env = "VELORA_API_URL")]
    pub velora_api_url: Option<String>,

    /// Velora proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "VELORA_PROXY_PROFILE")]
    pub velora_proxy_profile: Option<String>,

    /// KyberSwap Aggregator API base URL
    #[arg(long, env = "KYBERSWAP_API_URL")]
    pub kyberswap_api_url: Option<String>,

    /// KyberSwap proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "KYBERSWAP_PROXY_PROFILE")]
    pub kyberswap_proxy_profile: Option<String>,

    /// Partner string sent to Velora for route analytics
    #[arg(long, env = "VELORA_PARTNER")]
    pub velora_partner: Option<String>,

    /// Relay quote API base URL
    #[arg(long, env = "RELAY_API_URL")]
    pub relay_api_url: Option<String>,

    /// Optional Relay API key sent as x-api-key
    #[arg(long, env = "RELAY_API_KEY")]
    pub relay_api_key: Option<String>,

    /// Relay proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "RELAY_PROXY_PROFILE")]
    pub relay_proxy_profile: Option<String>,

    /// NEAR Intents 1Click API base URL
    #[arg(long, env = "NEAR_INTENTS_API_URL")]
    pub near_intents_api_url: Option<String>,

    /// Optional NEAR Intents API key sent as X-API-Key
    #[arg(long, env = "NEAR_INTENTS_API_KEY")]
    pub near_intents_api_key: Option<String>,

    /// Optional NEAR Intents bearer token
    #[arg(long, env = "NEAR_INTENTS_BEARER_TOKEN")]
    pub near_intents_bearer_token: Option<String>,

    /// NEAR Intents proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "NEAR_INTENTS_PROXY_PROFILE")]
    pub near_intents_proxy_profile: Option<String>,

    /// Mayan quote API base URL
    #[arg(long, env = "MAYAN_API_URL")]
    pub mayan_api_url: Option<String>,

    /// Optional Mayan API key
    #[arg(long, env = "MAYAN_API_KEY")]
    pub mayan_api_key: Option<String>,

    /// Mayan proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "MAYAN_PROXY_PROFILE")]
    pub mayan_proxy_profile: Option<String>,

    /// Chainflip swap API base URL
    #[arg(long, env = "CHAINFLIP_API_URL")]
    pub chainflip_api_url: Option<String>,

    /// Chainflip proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "CHAINFLIP_PROXY_PROFILE")]
    pub chainflip_proxy_profile: Option<String>,

    /// Garden quote API base URL
    #[arg(long, env = "GARDEN_API_URL")]
    pub garden_api_url: Option<String>,

    /// Garden API key sent as garden-app-id
    #[arg(long, env = "GARDEN_API_KEY")]
    pub garden_api_key: Option<String>,

    /// Garden proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "GARDEN_PROXY_PROFILE")]
    pub garden_proxy_profile: Option<String>,

    /// Hyperliquid paymaster private key used as the recovery/sweep destination
    /// for released internal Hyperliquid custody
    #[arg(long, env = "HYPERLIQUID_PAYMASTER_PRIVATE_KEY")]
    pub hyperliquid_paymaster_private_key: Option<String>,

    /// Temporal frontend URL used by router-worker to start order workflows
    #[arg(
        long,
        env = "TEMPORAL_ADDRESS",
        default_value = "http://127.0.0.1:7233"
    )]
    pub temporal_address: String,

    /// Temporal namespace used by router-worker for order workflows
    #[arg(long, env = "TEMPORAL_NAMESPACE", default_value = "default")]
    pub temporal_namespace: String,

    /// Temporal task queue consumed by temporal-worker for order execution
    #[arg(
        long,
        env = "ORDER_EXECUTION_TEMPORAL_TASK_QUEUE",
        default_value_t = router_temporal::DEFAULT_TASK_QUEUE.to_string()
    )]
    pub temporal_task_queue: String,

    /// Bearer API key accepted from Sauron on internal detector callbacks
    #[arg(long, env = "ROUTER_DETECTOR_API_KEY")]
    pub router_detector_api_key: Option<String>,

    /// Bearer API key accepted from the public router gateway for quote/order
    /// endpoints. Required when router-api binds a non-loopback host because
    /// these endpoints create custodial vaults and orders.
    #[arg(long, env = "ROUTER_GATEWAY_API_KEY")]
    pub router_gateway_api_key: Option<String>,

    /// Disable POST /api/v1/orders while keeping quote and read endpoints enabled
    #[arg(
        long,
        env = "ROUTER_ORDERS_DISABLED",
        default_value_t = false,
        action = clap::ArgAction::Set
    )]
    pub router_orders_disabled: bool,

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

    /// Global market-order quote deadline in milliseconds
    #[arg(
        long,
        env = "ROUTER_MARKET_ORDER_QUOTE_TIMEOUT_MS",
        default_value = "60000"
    )]
    pub router_market_order_quote_timeout_ms: u64,

    /// Per single-hop venue quote deadline in milliseconds
    #[arg(
        long,
        env = "ROUTER_SINGLE_HOP_QUOTE_TIMEOUT_MS",
        default_value = "5000"
    )]
    pub router_single_hop_quote_timeout_ms: u64,

    /// Stable worker identity for router-worker records
    #[arg(long, env = "ROUTER_WORKER_ID")]
    pub worker_id: Option<String>,

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

    /// Window (in seconds) over which a full sweep of the curated route-cost
    /// allowlist is spread. The worker paces small slices across this window so
    /// every provider's prices refresh continuously rather than in one burst.
    #[arg(
        long,
        env = "ROUTER_WORKER_ROUTE_COST_REFRESH_SECONDS",
        default_value = "1800"
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
    #[arg(long, env = "COINBASE_PRICE_API_BASE_URL")]
    pub coinbase_price_api_base_url: Option<String>,

    /// Coinbase proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "COINBASE_PROXY_PROFILE")]
    pub coinbase_proxy_profile: Option<String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn router_server_args_include_kyberswap_url_and_proxy_but_no_client_id() {
        let command = RouterServerArgs::command();
        let ids: Vec<String> = command
            .get_arguments()
            .map(|arg| arg.get_id().as_str().to_string())
            .collect();

        assert!(ids.iter().any(|id| id == "kyberswap_api_url"));
        assert!(ids.iter().any(|id| id == "kyberswap_proxy_profile"));
        assert!(!ids
            .iter()
            .any(|id| id.contains("kyberswap") && id.contains("client")));
    }
}
