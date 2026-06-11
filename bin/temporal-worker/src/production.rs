use std::{fs, path::PathBuf, str::FromStr, sync::Arc, time::Duration};

use bitcoincore_rpc_async::Auth;
use chains::{
    bitcoin::BitcoinChain,
    evm::{EvmChain, EvmGasSponsorConfig},
    hyperliquid::HyperliquidChain,
    ChainRegistry,
};
use clap::Args;
use market_pricing::{MarketPricingOracle, MarketPricingOracleConfig};
use router_core::{
    config::Settings,
    db::Database,
    services::{
        action_providers::{
            AcrossHttpProviderConfig, ActionProviderHttpOptions, ActionProviderRegistry,
            CctpHttpProviderConfig, CctpTransferMode, VeloraHttpProviderConfig,
        },
        custody_action_executor::{
            evm_address_from_private_key, CustodyActionExecutor, HyperliquidCallNetwork,
            HyperliquidRuntimeConfig, PaymasterRegistry,
        },
        upstream_proxy::{
            normalize_optional_string as normalize_proxy_string, ProxyProfileCatalog, ProxyTarget,
            ResolvedProxies,
        },
        PricingSnapshotProvider, RouteCostService,
    },
};
use router_primitives::ChainType;
use url::Url;

use crate::{
    order_execution::activities::OrderActivityDeps,
    runtime::{WorkerError, WorkerResult},
};

const CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG: &str = "https://iris-api.circle.com";

#[derive(Debug, Clone, Args)]
pub struct OrderWorkerRuntimeArgs {
    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://router_user:router_password@localhost:5432/router_db"
    )]
    pub database_url: String,

    /// Database max connections
    #[arg(
        long = "db-max-connections",
        env = "SAURON_TEMPORAL_DB_MAX_CONNECTIONS",
        default_value = "200"
    )]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "4")]
    pub db_min_connections: u32,

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

    /// Partner string sent to Velora for route analytics
    #[arg(long, env = "VELORA_PARTNER")]
    pub velora_partner: Option<String>,

    /// Hyperliquid paymaster private key used as the recovery/sweep destination for released custody
    #[arg(long, env = "HYPERLIQUID_PAYMASTER_PRIVATE_KEY")]
    pub hyperliquid_paymaster_private_key: Option<String>,

    /// Hyperliquid network (mainnet|testnet)
    #[arg(
        long,
        env = "HYPERLIQUID_NETWORK",
        default_value = "mainnet",
        value_parser = parse_hyperliquid_network,
    )]
    pub hyperliquid_network: HyperliquidCallNetwork,

    /// Timeout for Hyperliquid resting orders before exchange-side cancel, in milliseconds.
    #[arg(long, env = "HYPERLIQUID_ORDER_TIMEOUT_MS", default_value = "30000")]
    pub hyperliquid_order_timeout_ms: u64,

    /// Coinbase unauthenticated price API base URL used by USD valuation pricing refresh
    #[arg(long, env = "COINBASE_PRICE_API_BASE_URL")]
    pub coinbase_price_api_base_url: Option<String>,

    /// Coinbase proxy profile (`direct`, `ipv4-us-west-1`, `ipv6-us-west-1`, `ipv4-eu`).
    #[arg(long, env = "COINBASE_PROXY_PROFILE")]
    pub coinbase_proxy_profile: Option<String>,
}

impl OrderWorkerRuntimeArgs {
    pub async fn build_order_activities(&self) -> WorkerResult<OrderActivityDeps> {
        validate_upstream_config(self)?;
        let settings = Arc::new(load_settings(&self.master_key_path)?);
        let db = Database::connect(
            &self.database_url,
            self.db_max_connections,
            self.db_min_connections,
        )
        .await
        .map_err(|source| WorkerError::Configuration {
            message: format!("failed to connect to router database: {source}"),
        })?;
        let chain_registry = Arc::new(initialize_chain_registry(self).await?);
        let action_providers = Arc::new(initialize_action_providers(self)?);
        let pricing_provider: Arc<dyn PricingSnapshotProvider> = Arc::new(
            initialize_pricing_provider(self, db.clone(), action_providers.clone())?,
        );
        let custody_action_executor = Arc::new(
            CustodyActionExecutor::new(db.clone(), settings, chain_registry.clone())
                .with_hyperliquid_runtime(hyperliquid_runtime_config(self)?)
                .with_paymasters(paymaster_registry(self)?),
        );
        Ok(OrderActivityDeps::new(
            db,
            action_providers,
            custody_action_executor,
            chain_registry,
            pricing_provider,
        ))
    }
}

fn proxy_profile_catalog(args: &OrderWorkerRuntimeArgs) -> WorkerResult<ProxyProfileCatalog> {
    ProxyProfileCatalog::new(
        args.proxy_profile_ipv4_us_west_1_url.as_deref(),
        &args.proxy_profile_ipv4_us_west_1_dns_mode,
        args.proxy_profile_ipv6_us_west_1_url.as_deref(),
        &args.proxy_profile_ipv6_us_west_1_dns_mode,
        args.proxy_profile_ipv4_eu_url.as_deref(),
        &args.proxy_profile_ipv4_eu_dns_mode,
    )
    .map_err(|source| config_error(source.to_string()))
}

fn resolve_upstream_proxies(args: &OrderWorkerRuntimeArgs) -> WorkerResult<ResolvedProxies> {
    let catalog = proxy_profile_catalog(args)?;
    ResolvedProxies::resolve(
        &catalog,
        [
            (ProxyTarget::Across, args.across_proxy_profile.as_deref()),
            (ProxyTarget::Cctp, args.cctp_proxy_profile.as_deref()),
            (
                ProxyTarget::Hyperunit,
                args.hyperunit_proxy_profile.as_deref(),
            ),
            (
                ProxyTarget::Hyperliquid,
                args.hyperliquid_proxy_profile.as_deref(),
            ),
            (ProxyTarget::Velora, args.velora_proxy_profile.as_deref()),
            (
                ProxyTarget::Coinbase,
                args.coinbase_proxy_profile.as_deref(),
            ),
            (
                ProxyTarget::EthereumRpc,
                args.ethereum_mainnet_rpc_proxy_profile.as_deref(),
            ),
            (ProxyTarget::BaseRpc, args.base_rpc_proxy_profile.as_deref()),
            (
                ProxyTarget::ArbitrumRpc,
                args.arbitrum_rpc_proxy_profile.as_deref(),
            ),
            (
                ProxyTarget::BitcoinRpc,
                args.bitcoin_rpc_proxy_profile.as_deref(),
            ),
            (ProxyTarget::Esplora, args.esplora_proxy_profile.as_deref()),
        ],
    )
    .map_err(|source| config_error(source.to_string()))
}

fn validate_upstream_config(args: &OrderWorkerRuntimeArgs) -> WorkerResult<()> {
    let proxies = resolve_upstream_proxies(args)?;
    let mut errors = Vec::new();

    require_http_url(
        &mut errors,
        "ETH_RPC_URL",
        "Ethereum Mainnet RPC URL",
        Some(&args.ethereum_mainnet_rpc_url),
    );
    require_http_url(
        &mut errors,
        "BASE_RPC_URL",
        "Base RPC URL",
        Some(&args.base_rpc_url),
    );
    require_http_url(
        &mut errors,
        "ARBITRUM_RPC_URL",
        "Arbitrum RPC URL",
        Some(&args.arbitrum_rpc_url),
    );
    require_http_url(
        &mut errors,
        "BITCOIN_RPC_URL",
        "Bitcoin RPC URL",
        Some(&args.bitcoin_rpc_url),
    );
    require_http_url(
        &mut errors,
        "ESPLORA_HTTP_SERVER_URL",
        "Esplora HTTP Server URL",
        Some(&args.untrusted_esplora_http_server_url),
    );

    optional_http_url(
        &mut errors,
        "ACROSS_API_URL",
        "Across API URL",
        args.across_api_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "CCTP_API_URL",
        "CCTP API URL",
        args.cctp_api_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "HYPERUNIT_API_URL",
        "HyperUnit API URL",
        args.hyperunit_api_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "HYPERLIQUID_API_URL",
        "Hyperliquid API URL",
        args.hyperliquid_api_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "VELORA_API_URL",
        "Velora API URL",
        args.velora_api_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "COINBASE_PRICE_API_BASE_URL",
        "Coinbase price API base URL",
        args.coinbase_price_api_base_url.as_deref(),
    );
    optional_http_url(
        &mut errors,
        "FLASHBOTS_RPC_URL",
        "Flashbots RPC URL",
        args.flashbots_rpc_url.as_deref(),
    );

    if args.production {
        require_http_url(
            &mut errors,
            "ACROSS_API_URL",
            "Across API URL",
            args.across_api_url.as_deref(),
        );
        require_present(
            &mut errors,
            "ACROSS_API_KEY",
            "Across API key",
            args.across_api_key.as_deref(),
        );
        require_http_url(
            &mut errors,
            "CCTP_API_URL",
            "CCTP API URL",
            args.cctp_api_url.as_deref(),
        );
        require_http_url(
            &mut errors,
            "HYPERUNIT_API_URL",
            "HyperUnit API URL",
            args.hyperunit_api_url.as_deref(),
        );
        require_http_url(
            &mut errors,
            "HYPERLIQUID_API_URL",
            "Hyperliquid API URL",
            args.hyperliquid_api_url.as_deref(),
        );
        require_http_url(
            &mut errors,
            "VELORA_API_URL",
            "Velora API URL",
            args.velora_api_url.as_deref(),
        );
        require_http_url(
            &mut errors,
            "COINBASE_PRICE_API_BASE_URL",
            "Coinbase price API base URL",
            args.coinbase_price_api_base_url.as_deref(),
        );
        require_http_url(
            &mut errors,
            "FLASHBOTS_RPC_URL",
            "Flashbots RPC URL",
            args.flashbots_rpc_url.as_deref(),
        );

        proxies.require(
            &mut errors,
            [
                ProxyTarget::EthereumRpc,
                ProxyTarget::BaseRpc,
                ProxyTarget::ArbitrumRpc,
                ProxyTarget::BitcoinRpc,
                ProxyTarget::Esplora,
                ProxyTarget::Across,
                ProxyTarget::Cctp,
                ProxyTarget::Hyperunit,
                ProxyTarget::Hyperliquid,
                ProxyTarget::Velora,
                ProxyTarget::Coinbase,
            ],
        );
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(config_error(format!(
            "invalid upstream configuration:\n- {}",
            errors.join("\n- ")
        )))
    }
}

fn optional_http_url(errors: &mut Vec<String>, env_name: &str, name: &str, value: Option<&str>) {
    if let Err(error) = normalize_optional_url(value, name) {
        errors.push(format!("{env_name}: {error}"));
    }
}

fn require_http_url(errors: &mut Vec<String>, env_name: &str, name: &str, value: Option<&str>) {
    match normalize_optional_url(value, name) {
        Ok(Some(_)) => {}
        Ok(None) => errors.push(format!("{env_name} is required")),
        Err(error) => errors.push(format!("{env_name}: {error}")),
    }
}

fn require_present(errors: &mut Vec<String>, env_name: &str, name: &str, value: Option<&str>) {
    if normalize_proxy_string(value).is_none() {
        errors.push(format!("{env_name} is required for {name}"));
    }
}

fn initialize_pricing_provider(
    args: &OrderWorkerRuntimeArgs,
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
) -> WorkerResult<RouteCostService> {
    let Some(coinbase_price_api_base_url) = normalize_optional_url(
        args.coinbase_price_api_base_url.as_deref(),
        "Coinbase price API base URL",
    )?
    else {
        return Ok(RouteCostService::new(db, action_providers));
    };
    let proxies = resolve_upstream_proxies(args)?;
    let oracle_config = MarketPricingOracleConfig::new(
        &coinbase_price_api_base_url,
        &args.ethereum_mainnet_rpc_url,
        &args.arbitrum_rpc_url,
        &args.base_rpc_url,
        args.hyperliquid_api_url.as_deref(),
    )
    .map_err(|source| config_error(format!("invalid USD pricing oracle config: {source}")))?;
    let pricing_oracle = Arc::new(
        MarketPricingOracle::new_with_proxies(
            oracle_config,
            proxies.proxy_ref(ProxyTarget::Coinbase),
            proxies.proxy_ref(ProxyTarget::EthereumRpc),
            proxies.proxy_ref(ProxyTarget::ArbitrumRpc),
            proxies.proxy_ref(ProxyTarget::BaseRpc),
            proxies.proxy_ref(ProxyTarget::Hyperliquid),
        )
        .map_err(|source| {
            config_error(format!("failed to initialize USD pricing oracle: {source}"))
        })?,
    );
    Ok(RouteCostService::new(db, action_providers).with_pricing_oracle(pricing_oracle))
}

fn load_settings(master_key_path: &str) -> WorkerResult<Settings> {
    let path = PathBuf::from(master_key_path.trim());
    if path.as_os_str().is_empty() {
        return Err(config_error("ROUTER_MASTER_KEY_PATH must not be empty"));
    }
    Settings::load(&path).map_err(|source| {
        config_error(format!(
            "failed to load router master key from {}: {source}",
            path.display()
        ))
    })
}

async fn initialize_chain_registry(args: &OrderWorkerRuntimeArgs) -> WorkerResult<ChainRegistry> {
    let mut chain_registry = ChainRegistry::new();
    let proxies = resolve_upstream_proxies(args)?;
    let flashbots_rpc_url =
        normalize_optional_url(args.flashbots_rpc_url.as_deref(), "Flashbots RPC URL")?;

    let bitcoin_chain = BitcoinChain::new_with_proxies(
        &args.bitcoin_rpc_url,
        args.bitcoin_rpc_auth.clone(),
        proxies.proxy_ref(ProxyTarget::BitcoinRpc),
        &args.untrusted_esplora_http_server_url,
        proxies.proxy_ref(ProxyTarget::Esplora),
        args.bitcoin_network,
    )
    .map_err(|source| config_error(format!("failed to initialize bitcoin chain: {source}")))?;
    chain_registry.register_bitcoin(ChainType::Bitcoin, Arc::new(bitcoin_chain));

    let ethereum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy(
            &args.ethereum_mainnet_rpc_url,
            ChainType::Ethereum,
            b"router-ethereum-wallet",
            4,
            Duration::from_secs(12),
            gas_sponsor_config(args.ethereum_paymaster_private_key.as_ref()),
            proxies.proxy_ref(ProxyTarget::EthereumRpc),
            flashbots_rpc_url.as_deref(),
        )
        .await
        .map_err(|source| config_error(format!("failed to initialize ethereum chain: {source}")))?,
    );
    chain_registry.register_evm(ChainType::Ethereum, ethereum_chain);

    let base_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy(
            &args.base_rpc_url,
            ChainType::Base,
            b"router-base-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(args.base_paymaster_private_key.as_ref()),
            proxies.proxy_ref(ProxyTarget::BaseRpc),
            None,
        )
        .await
        .map_err(|source| config_error(format!("failed to initialize base chain: {source}")))?,
    );
    chain_registry.register_evm(ChainType::Base, base_chain);

    let arbitrum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy(
            &args.arbitrum_rpc_url,
            ChainType::Arbitrum,
            b"router-arbitrum-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(args.arbitrum_paymaster_private_key.as_ref()),
            proxies.proxy_ref(ProxyTarget::ArbitrumRpc),
            None,
        )
        .await
        .map_err(|source| config_error(format!("failed to initialize arbitrum chain: {source}")))?,
    );
    chain_registry.register_evm(ChainType::Arbitrum, arbitrum_chain);

    let hyperliquid_chain = Arc::new(HyperliquidChain::new(
        b"router-hyperliquid-wallet",
        1,
        Duration::from_secs(1),
    ));
    chain_registry.register(ChainType::Hyperliquid, hyperliquid_chain);

    Ok(chain_registry)
}

fn initialize_action_providers(
    args: &OrderWorkerRuntimeArgs,
) -> WorkerResult<ActionProviderRegistry> {
    let proxies = resolve_upstream_proxies(args)?;
    let across = match normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")? {
        Some(base_url) => Some(
            AcrossHttpProviderConfig::new(
                base_url,
                required_across_api_key(args.across_api_key.as_deref())?,
            )
            .with_integrator_id(args.across_integrator_id.clone())
            .with_proxy_url(proxies.proxy_owned(ProxyTarget::Across)),
        ),
        None => None,
    };
    let velora =
        normalize_optional_url(args.velora_api_url.as_deref(), "Velora API URL")?.map(|base_url| {
            VeloraHttpProviderConfig::new(base_url)
                .with_partner(normalize_optional_string(args.velora_partner.as_deref()))
                .with_proxy_url(proxies.proxy_owned(ProxyTarget::Velora))
        });
    let cctp_base_url = normalize_optional_url(args.cctp_api_url.as_deref(), "CCTP API URL")?
        .unwrap_or_else(|| CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG.to_string());
    let cctp_transfer_mode = cctp_transfer_mode_for_config(args.cctp_transfer_mode.as_deref())?;
    let cctp = Some(
        CctpHttpProviderConfig::new(cctp_base_url)
            .with_contract_addresses(
                normalize_optional_string(args.cctp_token_messenger_v2_address.as_deref()),
                normalize_optional_string(args.cctp_message_transmitter_v2_address.as_deref()),
            )
            .with_transfer_mode(cctp_transfer_mode)
            .with_proxy_url(proxies.proxy_owned(ProxyTarget::Cctp)),
    );

    ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across,
        cctp,
        hyperunit_base_url: normalize_optional_url(
            args.hyperunit_api_url.as_deref(),
            "HyperUnit API URL",
        )?,
        hyperunit_proxy_url: proxies.proxy_owned(ProxyTarget::Hyperunit),
        hyperliquid_base_url: normalize_optional_url(
            args.hyperliquid_api_url.as_deref(),
            "Hyperliquid API URL",
        )?,
        hyperliquid_proxy_url: proxies.proxy_owned(ProxyTarget::Hyperliquid),
        velora,
        hyperliquid_network: args.hyperliquid_network,
        hyperliquid_order_timeout_ms: args.hyperliquid_order_timeout_ms,
    })
    .map_err(|source| config_error(format!("failed to initialize action providers: {source}")))
}

fn hyperliquid_runtime_config(
    args: &OrderWorkerRuntimeArgs,
) -> WorkerResult<Option<HyperliquidRuntimeConfig>> {
    let proxies = resolve_upstream_proxies(args)?;
    Ok(
        normalize_optional_url(args.hyperliquid_api_url.as_deref(), "Hyperliquid API URL")?.map(
            |base_url| {
                let proxy_url = proxies
                    .proxy_ref(ProxyTarget::Hyperliquid)
                    .map(|proxy| proxy.url().clone());
                HyperliquidRuntimeConfig::new(base_url, args.hyperliquid_network)
                    .with_proxy_url(proxy_url)
            },
        ),
    )
}

fn paymaster_registry(args: &OrderWorkerRuntimeArgs) -> WorkerResult<PaymasterRegistry> {
    let mut registry = PaymasterRegistry::new();

    if let Some(private_key) =
        normalize_optional_string(args.ethereum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| config_error(source.to_string()))?;
        registry.register(ChainType::Ethereum, address);
    }
    if let Some(private_key) = normalize_optional_string(args.base_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| config_error(source.to_string()))?;
        registry.register(ChainType::Base, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.arbitrum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| config_error(source.to_string()))?;
        registry.register(ChainType::Arbitrum, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.hyperliquid_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| config_error(source.to_string()))?;
        registry.register(ChainType::Hyperliquid, address);
    }

    Ok(registry)
}

fn gas_sponsor_config(private_key: Option<&String>) -> Option<EvmGasSponsorConfig> {
    Some(EvmGasSponsorConfig {
        private_key: private_key?.clone(),
    })
}

fn required_across_api_key(value: Option<&str>) -> WorkerResult<String> {
    let Some(value) = value else {
        return Err(config_error(
            "ACROSS_API_KEY is required when ACROSS_API_URL is configured",
        ));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(config_error(
            "ACROSS_API_KEY must not be empty when ACROSS_API_URL is configured",
        ));
    }
    Ok(trimmed.to_string())
}

fn cctp_transfer_mode_for_config(value: Option<&str>) -> Result<CctpTransferMode, WorkerError> {
    let Some(value) = value else {
        return Ok(CctpTransferMode::Standard);
    };
    CctpTransferMode::from_str(value).map_err(config_error)
}
fn normalize_optional_url(value: Option<&str>, name: &str) -> WorkerResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed =
        Url::parse(trimmed).map_err(|source| config_error(format!("invalid {name}: {source}")))?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(config_error(format!(
            "invalid {name}: unsupported scheme {}",
            parsed.scheme()
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(config_error(format!(
            "invalid {name}: credentials are not allowed"
        )));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(config_error(format!(
            "invalid {name}: query strings and fragments are not allowed"
        )));
    }
    Ok(Some(parsed.as_str().trim_end_matches('/').to_string()))
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_auth(value: &str) -> Result<Auth, String> {
    if value.eq_ignore_ascii_case("none") {
        Ok(Auth::None)
    } else if fs::exists(value).map_err(|source| source.to_string())? {
        Ok(Auth::CookieFile(PathBuf::from(value)))
    } else {
        let mut split = value.splitn(2, ':');
        let username = split.next().ok_or("Invalid auth string")?;
        let password = split.next().ok_or("Invalid auth string")?;
        Ok(Auth::UserPass(username.to_string(), password.to_string()))
    }
}

fn parse_hyperliquid_network(value: &str) -> Result<HyperliquidCallNetwork, String> {
    match value.to_ascii_lowercase().as_str() {
        "mainnet" => Ok(HyperliquidCallNetwork::Mainnet),
        "testnet" => Ok(HyperliquidCallNetwork::Testnet),
        other => Err(format!(
            "invalid hyperliquid network {other:?}: expected mainnet or testnet"
        )),
    }
}

fn config_error(message: impl Into<String>) -> WorkerError {
    WorkerError::Configuration {
        message: message.into(),
    }
}
