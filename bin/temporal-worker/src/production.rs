use std::{fs, path::PathBuf, sync::Arc, time::Duration};

use alloy::primitives::U256;
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
            CctpHttpProviderConfig, VeloraHttpProviderConfig,
        },
        custody_action_executor::{
            bitcoin_address_from_private_key, evm_address_from_private_key, CustodyActionExecutor,
            HyperliquidCallNetwork, HyperliquidRuntimeConfig, PaymasterRegistry,
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
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "32")]
    pub db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "4")]
    pub db_min_connections: u32,

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

    /// Bitcoin paymaster private key used as the recovery/sweep destination for released custody
    #[arg(long, env = "BITCOIN_PAYMASTER_PRIVATE_KEY")]
    pub bitcoin_paymaster_private_key: Option<String>,

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

    /// Legacy shared Hyperliquid execution signer private key.
    #[arg(long, env = "HYPERLIQUID_EXECUTION_PRIVATE_KEY", hide = true)]
    pub hyperliquid_execution_private_key: Option<String>,

    /// Legacy shared Hyperliquid account/user address.
    #[arg(long, env = "HYPERLIQUID_ACCOUNT_ADDRESS", hide = true)]
    pub hyperliquid_account_address: Option<String>,

    /// Legacy shared Hyperliquid vault/subaccount address.
    #[arg(long, env = "HYPERLIQUID_VAULT_ADDRESS", hide = true)]
    pub hyperliquid_vault_address: Option<String>,

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
    #[arg(
        long,
        env = "COINBASE_PRICE_API_BASE_URL",
        default_value = "https://api.coinbase.com"
    )]
    pub coinbase_price_api_base_url: String,
}

impl OrderWorkerRuntimeArgs {
    pub async fn build_order_activities(&self) -> WorkerResult<OrderActivityDeps> {
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
        reject_shared_hyperliquid_execution_config(self)?;
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

fn initialize_pricing_provider(
    args: &OrderWorkerRuntimeArgs,
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
) -> WorkerResult<RouteCostService> {
    let oracle_config = MarketPricingOracleConfig::new(
        &args.coinbase_price_api_base_url,
        &args.ethereum_mainnet_rpc_url,
        &args.arbitrum_rpc_url,
        &args.base_rpc_url,
    )
    .map_err(|source| config_error(format!("invalid USD pricing oracle config: {source}")))?;
    let pricing_oracle = Arc::new(MarketPricingOracle::new(oracle_config).map_err(|source| {
        config_error(format!("failed to initialize USD pricing oracle: {source}"))
    })?);
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
    let evm_paymaster_vault_gas_buffer_wei = parse_u256_arg(
        args.evm_paymaster_vault_gas_target_wei
            .as_ref()
            .unwrap_or(&args.evm_paymaster_vault_gas_buffer_wei),
        "EVM paymaster vault gas buffer",
    )?;

    let bitcoin_chain = BitcoinChain::new(
        &args.bitcoin_rpc_url,
        args.bitcoin_rpc_auth.clone(),
        &args.untrusted_esplora_http_server_url,
        args.bitcoin_network,
    )
    .map_err(|source| config_error(format!("failed to initialize bitcoin chain: {source}")))?;
    chain_registry.register_bitcoin(ChainType::Bitcoin, Arc::new(bitcoin_chain));

    let ethereum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor(
            &args.ethereum_mainnet_rpc_url,
            &args.ethereum_reference_token,
            ChainType::Ethereum,
            b"router-ethereum-wallet",
            4,
            Duration::from_secs(12),
            gas_sponsor_config(
                args.ethereum_paymaster_private_key.as_ref(),
                evm_paymaster_vault_gas_buffer_wei,
            ),
        )
        .await
        .map_err(|source| config_error(format!("failed to initialize ethereum chain: {source}")))?,
    );
    chain_registry.register_evm(ChainType::Ethereum, ethereum_chain);

    let base_chain = Arc::new(
        EvmChain::new_with_gas_sponsor(
            &args.base_rpc_url,
            &args.base_reference_token,
            ChainType::Base,
            b"router-base-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(
                args.base_paymaster_private_key.as_ref(),
                evm_paymaster_vault_gas_buffer_wei,
            ),
        )
        .await
        .map_err(|source| config_error(format!("failed to initialize base chain: {source}")))?,
    );
    chain_registry.register_evm(ChainType::Base, base_chain);

    let arbitrum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor(
            &args.arbitrum_rpc_url,
            &args.arbitrum_reference_token,
            ChainType::Arbitrum,
            b"router-arbitrum-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(
                args.arbitrum_paymaster_private_key.as_ref(),
                evm_paymaster_vault_gas_buffer_wei,
            ),
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
    let across = match normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")? {
        Some(base_url) => Some(
            AcrossHttpProviderConfig::new(
                base_url,
                required_across_api_key(args.across_api_key.as_deref())?,
            )
            .with_integrator_id(args.across_integrator_id.clone()),
        ),
        None => None,
    };
    let velora =
        normalize_optional_url(args.velora_api_url.as_deref(), "Velora API URL")?.map(|base_url| {
            VeloraHttpProviderConfig::new(base_url)
                .with_partner(normalize_optional_string(args.velora_partner.as_deref()))
        });
    let cctp_base_url = normalize_optional_url(args.cctp_api_url.as_deref(), "CCTP API URL")?
        .unwrap_or_else(|| CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG.to_string());
    let cctp = Some(
        CctpHttpProviderConfig::new(cctp_base_url).with_contract_addresses(
            normalize_optional_string(args.cctp_token_messenger_v2_address.as_deref()),
            normalize_optional_string(args.cctp_message_transmitter_v2_address.as_deref()),
        ),
    );

    ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across,
        cctp,
        hyperunit_base_url: normalize_optional_url(
            args.hyperunit_api_url.as_deref(),
            "HyperUnit API URL",
        )?,
        hyperunit_proxy_url: normalize_optional_string(args.hyperunit_proxy_url.as_deref()),
        hyperliquid_base_url: normalize_optional_url(
            args.hyperliquid_api_url.as_deref(),
            "Hyperliquid API URL",
        )?,
        velora,
        hyperliquid_network: args.hyperliquid_network,
        hyperliquid_order_timeout_ms: args.hyperliquid_order_timeout_ms,
    })
    .map_err(|source| config_error(format!("failed to initialize action providers: {source}")))
}

fn hyperliquid_runtime_config(
    args: &OrderWorkerRuntimeArgs,
) -> WorkerResult<Option<HyperliquidRuntimeConfig>> {
    Ok(
        normalize_optional_url(args.hyperliquid_api_url.as_deref(), "Hyperliquid API URL")?
            .map(|base_url| HyperliquidRuntimeConfig::new(base_url, args.hyperliquid_network)),
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
        normalize_optional_string(args.bitcoin_paymaster_private_key.as_deref())
    {
        let address = bitcoin_address_from_private_key(&private_key, args.bitcoin_network)
            .map_err(|source| config_error(source.to_string()))?;
        registry.register(ChainType::Bitcoin, address);
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

fn gas_sponsor_config(
    private_key: Option<&String>,
    vault_gas_buffer_wei: U256,
) -> Option<EvmGasSponsorConfig> {
    Some(EvmGasSponsorConfig {
        private_key: private_key?.clone(),
        vault_gas_buffer_wei,
    })
}

fn reject_shared_hyperliquid_execution_config(args: &OrderWorkerRuntimeArgs) -> WorkerResult<()> {
    let configured_fields = [
        (
            "HYPERLIQUID_EXECUTION_PRIVATE_KEY",
            normalize_optional_string(args.hyperliquid_execution_private_key.as_deref()),
        ),
        (
            "HYPERLIQUID_ACCOUNT_ADDRESS",
            normalize_optional_string(args.hyperliquid_account_address.as_deref()),
        ),
        (
            "HYPERLIQUID_VAULT_ADDRESS",
            normalize_optional_string(args.hyperliquid_vault_address.as_deref()),
        ),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|_| name))
    .collect::<Vec<_>>();

    if configured_fields.is_empty() {
        return Ok(());
    }

    Err(config_error(format!(
        "shared Hyperliquid execution identities are no longer supported; remove {} and use per-order router-derived Hyperliquid custody",
        configured_fields.join(", ")
    )))
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

fn parse_u256_arg(value: &str, name: &str) -> WorkerResult<U256> {
    U256::from_str_radix(value, 10)
        .map_err(|source| config_error(format!("invalid {name}: {source}")))
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
