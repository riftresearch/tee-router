use crate::{
    error::RouterServerError,
    services::{
        AddressScreeningService, OrderManager, ProviderHealthPoller, ProviderHealthProbe,
        ProviderHealthService, ProviderPolicyService, RouterSwitchService, VaultManager,
    },
    Result, RouterServerArgs,
};
use chains::{
    bitcoin::BitcoinChain,
    evm::{EvmChain, EvmGasSponsorConfig},
    hyperliquid::HyperliquidChain,
    ChainRegistry,
};
use market_pricing::{MarketPricingOracle, MarketPricingOracleConfig};
use router_core::{
    config::Settings,
    db::Database,
    services::{
        action_providers::{
            AcrossHttpProviderConfig, ActionProviderHttpOptions, ActionProviderRegistry,
            CctpHttpProviderConfig, CctpTransferMode, VeloraHttpProviderConfig,
        },
        asset_registry::ProviderId,
        custody_action_executor::{evm_address_from_private_key, PaymasterRegistry},
        route_costs::RouteCostService,
        upstream_proxy::{
            effective_proxy, normalize_optional_string as normalize_proxy_string, ProxyUrl,
        },
    },
};
use router_primitives::ChainType;
use snafu::ResultExt;
use std::{str::FromStr, sync::Arc, time::Duration};
use tracing::warn;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaymasterMode {
    Enabled,
    Disabled,
}

#[derive(Clone)]
pub struct RouterComponents {
    pub db: Database,
    pub chain_registry: Arc<ChainRegistry>,
    pub vault_manager: Arc<VaultManager>,
    pub order_manager: Arc<OrderManager>,
    pub provider_policies: Arc<ProviderPolicyService>,
    pub router_switches: Arc<RouterSwitchService>,
    pub provider_health: Arc<ProviderHealthService>,
    pub provider_health_poller: Arc<ProviderHealthPoller>,
    pub route_costs: Arc<RouteCostService>,
    pub address_screener: Option<Arc<AddressScreeningService>>,
}

pub async fn initialize_components(
    args: &RouterServerArgs,
    worker_id: Option<String>,
    paymaster_mode: PaymasterMode,
) -> Result<RouterComponents> {
    validate_upstream_config(args)?;
    let action_providers = Arc::new(initialize_action_providers(args)?);
    initialize_components_with_action_providers(args, worker_id, paymaster_mode, action_providers)
        .await
}

pub async fn initialize_components_with_action_providers(
    args: &RouterServerArgs,
    worker_id: Option<String>,
    paymaster_mode: PaymasterMode,
    action_providers: Arc<ActionProviderRegistry>,
) -> Result<RouterComponents> {
    validate_upstream_config(args)?;
    let master_key_path =
        args.resolved_master_key_path()
            .map_err(|message| crate::Error::DatabaseInit {
                source: RouterServerError::InvalidData { message },
            })?;
    let settings =
        Arc::new(
            Settings::load(&master_key_path).map_err(|err| crate::Error::DatabaseInit {
                source: RouterServerError::InvalidData {
                    message: format!(
                        "failed to load router master key from {}: {err}",
                        master_key_path.display()
                    ),
                },
            })?,
        );

    let db = Database::connect(
        &args.database_url,
        args.db_max_connections,
        args.db_min_connections,
    )
    .await
    .map_err(RouterServerError::from)
    .context(crate::DatabaseInitSnafu)?;

    let chain_registry = Arc::new(initialize_chain_registry(args, paymaster_mode).await?);
    let provider_policies = Arc::new(ProviderPolicyService::new(db.clone()));
    let router_switches = Arc::new(
        RouterSwitchService::load(db.clone())
            .await
            .context(crate::DatabaseInitSnafu)?,
    );
    let provider_health = Arc::new(ProviderHealthService::new(db.clone()));
    let provider_health_poller = Arc::new(initialize_provider_health_poller(
        args,
        provider_health.clone(),
        worker_id.as_deref().unwrap_or("router-api"),
    )?);
    let route_costs = Arc::new(initialize_route_costs(
        args,
        db.clone(),
        action_providers.clone(),
    )?);
    let address_screener = initialize_address_screener(args)?;
    let order_manager = Arc::new(
        OrderManager::with_action_providers(
            db.clone(),
            settings.clone(),
            chain_registry.clone(),
            action_providers.clone(),
        )
        .with_route_costs(Some(route_costs.clone()))
        .with_provider_policies(Some(provider_policies.clone()))
        .with_provider_health(Some(provider_health.clone()))
        .with_paymasters(quote_paymaster_registry(args)?),
    );
    let vault_manager = Arc::new(match worker_id {
        Some(worker_id) => VaultManager::with_worker_id(
            db.clone(),
            settings.clone(),
            chain_registry.clone(),
            worker_id,
        ),
        None => VaultManager::new(db.clone(), settings.clone(), chain_registry.clone()),
    });

    Ok(RouterComponents {
        db,
        chain_registry,
        vault_manager,
        order_manager,
        provider_policies,
        router_switches,
        provider_health,
        provider_health_poller,
        route_costs,
        address_screener,
    })
}

#[derive(Debug, Clone, Default)]
struct ResolvedUpstreamProxies {
    across: Option<ProxyUrl>,
    cctp: Option<ProxyUrl>,
    hyperunit: Option<ProxyUrl>,
    hyperliquid: Option<ProxyUrl>,
    velora: Option<ProxyUrl>,
    chainalysis: Option<ProxyUrl>,
    coinbase: Option<ProxyUrl>,
    ethereum_rpc: Option<ProxyUrl>,
    base_rpc: Option<ProxyUrl>,
    arbitrum_rpc: Option<ProxyUrl>,

    bitcoin_rpc: Option<ProxyUrl>,
    esplora: Option<ProxyUrl>,
}

fn resolve_upstream_proxies(args: &RouterServerArgs) -> Result<ResolvedUpstreamProxies> {
    let upstream = args.upstream_proxy_url.as_deref();
    Ok(ResolvedUpstreamProxies {
        across: effective_proxy(
            args.across_proxy_url.as_deref(),
            "ACROSS_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        cctp: effective_proxy(args.cctp_proxy_url.as_deref(), "CCTP_PROXY_URL", upstream)
            .map_err(invalid_proxy_config)?,
        hyperunit: effective_proxy(
            args.hyperunit_proxy_url.as_deref(),
            "HYPERUNIT_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        hyperliquid: effective_proxy(
            args.hyperliquid_proxy_url.as_deref(),
            "HYPERLIQUID_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        velora: effective_proxy(
            args.velora_proxy_url.as_deref(),
            "VELORA_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        chainalysis: effective_proxy(
            args.chainalysis_proxy_url.as_deref(),
            "CHAINALYSIS_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        coinbase: effective_proxy(
            args.coinbase_proxy_url.as_deref(),
            "COINBASE_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        ethereum_rpc: effective_proxy(
            args.ethereum_mainnet_rpc_proxy_url.as_deref(),
            "ETH_RPC_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        base_rpc: effective_proxy(
            args.base_rpc_proxy_url.as_deref(),
            "BASE_RPC_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        arbitrum_rpc: effective_proxy(
            args.arbitrum_rpc_proxy_url.as_deref(),
            "ARBITRUM_RPC_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,

        bitcoin_rpc: effective_proxy(
            args.bitcoin_rpc_proxy_url.as_deref(),
            "BITCOIN_RPC_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
        esplora: effective_proxy(
            args.esplora_proxy_url.as_deref(),
            "ESPLORA_PROXY_URL",
            upstream,
        )
        .map_err(invalid_proxy_config)?,
    })
}

fn validate_upstream_config(args: &RouterServerArgs) -> Result<()> {
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
        "CHAINALYSIS_HOST",
        "Chainalysis host",
        args.chainalysis_host.as_deref(),
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
            "CHAINALYSIS_HOST",
            "Chainalysis host",
            args.chainalysis_host.as_deref(),
        );
        require_present(
            &mut errors,
            "CHAINALYSIS_TOKEN",
            "Chainalysis token",
            args.chainalysis_token.as_deref(),
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

        require_proxy(
            &mut errors,
            "ETH_RPC_URL",
            "ETH_RPC_PROXY_URL",
            &proxies.ethereum_rpc,
        );
        require_proxy(
            &mut errors,
            "BASE_RPC_URL",
            "BASE_RPC_PROXY_URL",
            &proxies.base_rpc,
        );
        require_proxy(
            &mut errors,
            "ARBITRUM_RPC_URL",
            "ARBITRUM_RPC_PROXY_URL",
            &proxies.arbitrum_rpc,
        );

        require_proxy(
            &mut errors,
            "BITCOIN_RPC_URL",
            "BITCOIN_RPC_PROXY_URL",
            &proxies.bitcoin_rpc,
        );
        require_proxy(
            &mut errors,
            "ESPLORA_HTTP_SERVER_URL",
            "ESPLORA_PROXY_URL",
            &proxies.esplora,
        );
        require_proxy(
            &mut errors,
            "ACROSS_API_URL",
            "ACROSS_PROXY_URL",
            &proxies.across,
        );
        require_proxy(&mut errors, "CCTP_API_URL", "CCTP_PROXY_URL", &proxies.cctp);
        require_proxy(
            &mut errors,
            "HYPERUNIT_API_URL",
            "HYPERUNIT_PROXY_URL",
            &proxies.hyperunit,
        );
        require_proxy(
            &mut errors,
            "HYPERLIQUID_API_URL",
            "HYPERLIQUID_PROXY_URL",
            &proxies.hyperliquid,
        );
        require_proxy(
            &mut errors,
            "VELORA_API_URL",
            "VELORA_PROXY_URL",
            &proxies.velora,
        );
        require_proxy(
            &mut errors,
            "CHAINALYSIS_HOST",
            "CHAINALYSIS_PROXY_URL",
            &proxies.chainalysis,
        );
        require_proxy(
            &mut errors,
            "COINBASE_PRICE_API_BASE_URL",
            "COINBASE_PROXY_URL",
            &proxies.coinbase,
        );
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(invalid_config(format!(
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

fn require_proxy(
    errors: &mut Vec<String>,
    upstream_env_name: &str,
    proxy_env_name: &str,
    proxy_url: &Option<ProxyUrl>,
) {
    if proxy_url.is_none() {
        errors.push(format!(
            "{proxy_env_name} or UPSTREAM_PROXY_URL is required for {upstream_env_name}"
        ));
    }
}

fn initialize_provider_health_poller(
    args: &RouterServerArgs,
    provider_health: Arc<ProviderHealthService>,
    updated_by: &str,
) -> Result<ProviderHealthPoller> {
    ProviderHealthPoller::new(
        provider_health,
        provider_health_probes(args)?,
        updated_by.to_string(),
        Duration::from_secs(args.provider_health_timeout_seconds),
    )
    .map_err(|source| crate::Error::DatabaseInit { source })
}

fn provider_health_probes(args: &RouterServerArgs) -> Result<Vec<ProviderHealthProbe>> {
    let mut probes = Vec::new();
    let proxies = resolve_upstream_proxies(args)?;

    if let Some(base_url) =
        normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")?
    {
        let probe = ProviderHealthProbe::get(
            ProviderId::Across.as_str(),
            synthetic_provider_status_url(&base_url),
        )
        .with_bearer_token(required_across_api_key(args.across_api_key.as_deref())?)
        .with_proxy_url(proxy_string(proxies.across.as_ref()));
        probes.push(probe);
    }

    if let Some(cctp_base_url) =
        normalize_optional_url(args.cctp_api_url.as_deref(), "CCTP API URL")?
    {
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::Cctp.as_str(),
                synthetic_provider_status_url(&cctp_base_url),
            )
            .with_proxy_url(proxy_string(proxies.cctp.as_ref())),
        );
    }

    if let Some(base_url) =
        normalize_optional_url(args.hyperunit_api_url.as_deref(), "HyperUnit API URL")?
    {
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::Unit.as_str(),
                synthetic_provider_status_url(&base_url),
            )
            .with_proxy_url(proxy_string(proxies.hyperunit.as_ref())),
        );
    }

    if let Some(base_url) =
        normalize_optional_url(args.hyperliquid_api_url.as_deref(), "Hyperliquid API URL")?
    {
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::Hyperliquid.as_str(),
                synthetic_provider_status_url(&base_url),
            )
            .with_proxy_url(proxy_string(proxies.hyperliquid.as_ref())),
        );
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::HyperliquidBridge.as_str(),
                synthetic_provider_status_url(&base_url),
            )
            .with_proxy_url(proxy_string(proxies.hyperliquid.as_ref())),
        );
    }

    if let Some(base_url) =
        normalize_optional_url(args.velora_api_url.as_deref(), "Velora API URL")?
    {
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::Velora.as_str(),
                synthetic_provider_status_url(&base_url),
            )
            .with_proxy_url(proxy_string(proxies.velora.as_ref())),
        );
    }

    Ok(probes)
}

fn synthetic_provider_status_url(base_url: &str) -> String {
    format!("{base_url}/status")
}

fn proxy_string(proxy_url: Option<&ProxyUrl>) -> Option<String> {
    proxy_url.map(|proxy_url| proxy_url.as_str().to_string())
}

fn initialize_route_costs(
    args: &RouterServerArgs,
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
) -> Result<RouteCostService> {
    let Some(coinbase_price_api_base_url) = normalize_optional_url(
        args.coinbase_price_api_base_url.as_deref(),
        "Coinbase price API base URL",
    )?
    else {
        warn!("COINBASE_PRICE_API_BASE_URL is not configured; live USD pricing oracle is disabled");
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
    .map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("invalid route-cost pricing oracle config: {err}"),
        },
    })?;
    let pricing_oracle = Arc::new(
        MarketPricingOracle::new_with_proxy_urls(
            oracle_config,
            proxies.coinbase.as_ref().map(ProxyUrl::as_str),
            proxies.ethereum_rpc.as_ref().map(ProxyUrl::as_str),
            proxies.arbitrum_rpc.as_ref().map(ProxyUrl::as_str),
            proxies.base_rpc.as_ref().map(ProxyUrl::as_str),
            proxies.hyperliquid.as_ref().map(ProxyUrl::as_str),
        )
        .map_err(|err| crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("failed to initialize route-cost pricing oracle: {err}"),
            },
        })?,
    );
    Ok(RouteCostService::new(db, action_providers).with_pricing_oracle(pricing_oracle))
}

async fn initialize_chain_registry(
    args: &RouterServerArgs,
    paymaster_mode: PaymasterMode,
) -> Result<ChainRegistry> {
    let mut chain_registry = ChainRegistry::new();
    let proxies = resolve_upstream_proxies(args)?;
    let flashbots_rpc_url =
        normalize_optional_url(args.flashbots_rpc_url.as_deref(), "Flashbots RPC URL")?;

    let bitcoin_chain = BitcoinChain::new_with_proxy_urls(
        &args.bitcoin_rpc_url,
        args.bitcoin_rpc_auth.clone(),
        proxies.bitcoin_rpc.as_ref().map(ProxyUrl::as_str),
        &args.untrusted_esplora_http_server_url,
        proxies.esplora.as_ref().map(ProxyUrl::as_str),
        args.bitcoin_network,
    )
    .map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("failed to initialize bitcoin chain: {err}"),
        },
    })?;
    chain_registry.register_bitcoin(ChainType::Bitcoin, Arc::new(bitcoin_chain));

    let ethereum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy_urls(
            &args.ethereum_mainnet_rpc_url,
            ChainType::Ethereum,
            b"router-ethereum-wallet",
            4,
            Duration::from_secs(12),
            gas_sponsor_config(args.ethereum_paymaster_private_key.as_ref(), paymaster_mode),
            proxies.ethereum_rpc.as_ref().map(ProxyUrl::as_str),
            flashbots_rpc_url.as_deref(),
        )
        .await
        .map_err(|err| crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("failed to initialize ethereum chain: {err}"),
            },
        })?,
    );
    chain_registry.register_evm(ChainType::Ethereum, ethereum_chain);

    let base_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy_urls(
            &args.base_rpc_url,
            ChainType::Base,
            b"router-base-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(args.base_paymaster_private_key.as_ref(), paymaster_mode),
            proxies.base_rpc.as_ref().map(ProxyUrl::as_str),
            None,
        )
        .await
        .map_err(|err| crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("failed to initialize base chain: {err}"),
            },
        })?,
    );
    chain_registry.register_evm(ChainType::Base, base_chain);

    let arbitrum_chain = Arc::new(
        EvmChain::new_with_gas_sponsor_and_proxy_urls(
            &args.arbitrum_rpc_url,
            ChainType::Arbitrum,
            b"router-arbitrum-wallet",
            2,
            Duration::from_secs(2),
            gas_sponsor_config(args.arbitrum_paymaster_private_key.as_ref(), paymaster_mode),
            proxies.arbitrum_rpc.as_ref().map(ProxyUrl::as_str),
            None,
        )
        .await
        .map_err(|err| crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("failed to initialize arbitrum chain: {err}"),
            },
        })?,
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

fn initialize_action_providers(args: &RouterServerArgs) -> Result<ActionProviderRegistry> {
    let proxies = resolve_upstream_proxies(args)?;
    let across_api_url = normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")?;
    let across = if let Some(base_url) = across_api_url {
        Some(
            AcrossHttpProviderConfig::new(
                base_url,
                required_across_api_key(args.across_api_key.as_deref())?,
            )
            .with_integrator_id(args.across_integrator_id.clone())
            .with_proxy_url(proxies.across.clone()),
        )
    } else {
        None
    };
    let velora =
        normalize_optional_url(args.velora_api_url.as_deref(), "Velora API URL")?.map(|base_url| {
            VeloraHttpProviderConfig::new(base_url)
                .with_partner(normalize_optional_string(args.velora_partner.as_deref()))
                .with_proxy_url(proxies.velora.clone())
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
            .with_proxy_url(proxies.cctp.clone()),
    );

    let registry = ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across,
        cctp,
        hyperunit_base_url: normalize_optional_url(
            args.hyperunit_api_url.as_deref(),
            "HyperUnit API URL",
        )?,
        hyperunit_proxy_url: proxies.hyperunit.clone(),
        hyperliquid_base_url: normalize_optional_url(
            args.hyperliquid_api_url.as_deref(),
            "Hyperliquid API URL",
        )?,
        hyperliquid_proxy_url: proxies.hyperliquid.clone(),
        velora,
        hyperliquid_network: args.hyperliquid_network,
        hyperliquid_order_timeout_ms: args.hyperliquid_order_timeout_ms,
    })
    .map_err(invalid_proxy_config)?;
    if registry.is_empty() {
        warn!(
            "No action providers are configured; market-order quotes and execution will return no route"
        );
    }
    Ok(registry)
}

const CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG: &str = "https://iris-api.circle.com";

fn required_across_api_key(value: Option<&str>) -> Result<String> {
    let Some(value) = value else {
        return Err(invalid_config(
            "ACROSS_API_KEY is required when ACROSS_API_URL is configured",
        ));
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(invalid_config(
            "ACROSS_API_KEY must not be empty when ACROSS_API_URL is configured",
        ));
    }
    Ok(trimmed.to_string())
}

fn cctp_transfer_mode_for_config(value: Option<&str>) -> Result<CctpTransferMode> {
    let Some(value) = value else {
        return Ok(CctpTransferMode::Standard);
    };
    CctpTransferMode::from_str(value).map_err(|message| invalid_config(message))
}

fn normalize_optional_url(value: Option<&str>, name: &str) -> Result<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let parsed = Url::parse(trimmed).map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("invalid {name}: {err}"),
        },
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("invalid {name}: unsupported scheme {}", parsed.scheme()),
            },
        });
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("invalid {name}: credentials are not allowed"),
            },
        });
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("invalid {name}: query strings and fragments are not allowed"),
            },
        });
    }
    Ok(Some(parsed.as_str().trim_end_matches('/').to_string()))
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
}

fn initialize_address_screener(
    args: &RouterServerArgs,
) -> Result<Option<Arc<AddressScreeningService>>> {
    let proxies = resolve_upstream_proxies(args)?;
    let host = normalize_optional_url(args.chainalysis_host.as_deref(), "Chainalysis host")?;
    let token = normalize_optional_string(args.chainalysis_token.as_deref());

    match (host, token) {
        (Some(host), Some(token)) => AddressScreeningService::new_with_proxy_url(
            host,
            token,
            proxies.chainalysis.as_ref().map(ProxyUrl::as_str),
        )
        .map(Arc::new)
        .map(Some)
        .map_err(|err| invalid_config(format!("failed to initialize Chainalysis: {err}"))),
        (None, None) => {
            warn!("Chainalysis address screening is not configured");
            Ok(None)
        }
        _ => Err(invalid_config(
            "CHAINALYSIS_HOST and CHAINALYSIS_TOKEN must be configured together",
        )),
    }
}

fn invalid_config(message: impl Into<String>) -> crate::Error {
    crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: message.into(),
        },
    }
}
fn quote_paymaster_registry(args: &RouterServerArgs) -> Result<PaymasterRegistry> {
    let mut registry = PaymasterRegistry::new();
    if let Some(private_key) =
        normalize_optional_string(args.ethereum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| invalid_config(source.to_string()))?;
        registry.register(ChainType::Ethereum, address);
    }
    if let Some(private_key) = normalize_optional_string(args.base_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| invalid_config(source.to_string()))?;
        registry.register(ChainType::Base, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.arbitrum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|source| invalid_config(source.to_string()))?;
        registry.register(ChainType::Arbitrum, address);
    }
    Ok(registry)
}

fn invalid_proxy_config(message: impl std::fmt::Display) -> crate::Error {
    invalid_config(message.to_string())
}

fn gas_sponsor_config(
    private_key: Option<&String>,
    paymaster_mode: PaymasterMode,
) -> Option<EvmGasSponsorConfig> {
    if paymaster_mode == PaymasterMode::Disabled {
        return None;
    }

    let private_key = private_key?;

    Some(EvmGasSponsorConfig {
        private_key: private_key.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoincore_rpc_async::Auth;
    use std::net::{IpAddr, Ipv4Addr};

    fn base_args() -> RouterServerArgs {
        RouterServerArgs {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 4522,
            database_url: "postgres://router_user:router_password@localhost:5432/router_db"
                .to_string(),
            db_max_connections: 32,
            db_min_connections: 4,
            log_level: "info".to_string(),
            production: false,
            upstream_proxy_url: None,
            master_key_path: "/tmp/router-server-master-key.hex".to_string(),
            ethereum_mainnet_rpc_url: "https://eth.example".to_string(),
            ethereum_mainnet_rpc_proxy_url: None,
            flashbots_rpc_url: None,
            ethereum_paymaster_private_key: None,
            base_rpc_url: "https://base.example".to_string(),
            base_rpc_proxy_url: None,
            base_paymaster_private_key: None,
            arbitrum_rpc_url: "https://arb.example".to_string(),
            arbitrum_rpc_proxy_url: None,
            arbitrum_paymaster_private_key: None,
            bitcoin_rpc_url: "http://btc.example".to_string(),
            bitcoin_rpc_proxy_url: None,
            bitcoin_rpc_auth: Auth::None,
            untrusted_esplora_http_server_url: "https://esplora.example".to_string(),
            esplora_proxy_url: None,
            bitcoin_network: bitcoin::Network::Bitcoin,
            cors_domain: None,
            chainalysis_host: None,
            chainalysis_token: None,
            chainalysis_proxy_url: None,
            loki_url: None,
            across_api_url: None,
            across_api_key: None,
            across_proxy_url: None,
            across_integrator_id: None,
            cctp_api_url: None,
            cctp_proxy_url: None,
            cctp_token_messenger_v2_address: None,
            cctp_message_transmitter_v2_address: None,
            cctp_transfer_mode: None,
            hyperunit_api_url: None,
            hyperunit_proxy_url: None,
            hyperliquid_api_url: None,
            hyperliquid_proxy_url: None,
            velora_api_url: None,
            velora_proxy_url: None,
            velora_partner: None,
            hyperliquid_paymaster_private_key: None,
            temporal_address: "http://127.0.0.1:7233".to_string(),
            temporal_namespace: "default".to_string(),
            temporal_task_queue: router_temporal::DEFAULT_TASK_QUEUE.to_string(),
            router_detector_api_key: None,
            router_gateway_api_key: None,
            router_admin_api_key: None,
            hyperliquid_network:
                router_core::services::custody_action_executor::HyperliquidCallNetwork::Mainnet,
            hyperliquid_order_timeout_ms: 30_000,
            worker_id: None,
            worker_refund_poll_seconds: 60,
            worker_order_execution_poll_seconds: 5,
            worker_route_cost_refresh_seconds: 300,
            worker_provider_health_poll_seconds: 120,
            provider_health_timeout_seconds: 10,
            worker_order_maintenance_pass_limit: 100,
            worker_order_planning_pass_limit: 100,
            worker_order_execution_pass_limit: 25,
            worker_order_execution_concurrency: 64,
            worker_vault_funding_hint_pass_limit: 100,
            coinbase_price_api_base_url: None,
            coinbase_proxy_url: None,
        }
    }

    fn configure_required_production_upstreams(args: &mut RouterServerArgs) {
        args.production = true;
        args.across_api_url = Some("https://across.example".to_string());
        args.across_api_key = Some("across-key".to_string());
        args.cctp_api_url = Some("https://iris.example".to_string());
        args.hyperunit_api_url = Some("https://unit.example".to_string());
        args.hyperliquid_api_url = Some("https://hyperliquid.example".to_string());
        args.velora_api_url = Some("https://velora.example".to_string());
        args.chainalysis_host = Some("https://chainalysis.example".to_string());
        args.chainalysis_token = Some("chainalysis-token".to_string());
        args.coinbase_price_api_base_url = Some("https://coinbase.example".to_string());
        args.flashbots_rpc_url = Some("https://flashbots.example".to_string());
    }

    #[test]
    fn required_across_api_key_rejects_missing_or_empty_values() {
        assert!(required_across_api_key(None).is_err());
        assert!(required_across_api_key(Some("   ")).is_err());
    }

    #[test]
    fn required_across_api_key_accepts_and_trims_real_values() {
        assert_eq!(
            required_across_api_key(Some("  test-across-key  ")).unwrap(),
            "test-across-key"
        );
    }

    #[test]
    fn provider_health_probes_use_synthetic_status_paths() {
        let mut args = base_args();
        args.across_api_url = Some("https://across.example".to_string());
        args.across_api_key = Some("across-key".to_string());
        args.cctp_api_url = Some("https://iris.example".to_string());
        args.hyperunit_api_url = Some("https://unit.example".to_string());
        args.hyperunit_proxy_url = Some("socks5://router:secret@proxy.example:1080".to_string());
        args.hyperliquid_api_url = Some("https://hyperliquid.example".to_string());
        args.velora_api_url = Some("https://velora.example".to_string());

        let probes = provider_health_probes(&args).unwrap();
        let urls: Vec<(&str, &str)> = probes
            .iter()
            .map(|probe| (probe.provider(), probe.url()))
            .collect();

        assert_eq!(
            urls,
            vec![
                ("across", "https://across.example/status"),
                ("cctp", "https://iris.example/status"),
                ("unit", "https://unit.example/status"),
                ("hyperliquid", "https://hyperliquid.example/status"),
                ("hyperliquid_bridge", "https://hyperliquid.example/status"),
                ("velora", "https://velora.example/status"),
            ]
        );
    }

    #[test]
    fn production_upstream_proxy_satisfies_all_required_upstreams() {
        let mut args = base_args();
        configure_required_production_upstreams(&mut args);
        args.upstream_proxy_url = Some("socks5://proxy.example:1080".to_string());

        validate_upstream_config(&args).unwrap();
    }

    #[test]
    fn production_rejects_missing_proxy_configuration() {
        let mut args = base_args();
        configure_required_production_upstreams(&mut args);

        let error = validate_upstream_config(&args).unwrap_err().to_string();

        assert!(error.contains("ETH_RPC_PROXY_URL or UPSTREAM_PROXY_URL"));
        assert!(error.contains("COINBASE_PROXY_URL or UPSTREAM_PROXY_URL"));
    }

    #[test]
    fn optional_url_config_rejects_non_canonical_urls() {
        for value in [
            "https://user:pass@provider.example",
            "https://provider.example?token=secret",
            "https://provider.example#fragment",
        ] {
            let error = match normalize_optional_url(Some(value), "Provider URL") {
                Ok(_) => panic!("non-canonical URL must fail"),
                Err(error) => error,
            };
            let message = error.to_string();
            assert!(!message.contains("user"));
            assert!(!message.contains("pass"));
            assert!(!message.contains("token"));
            assert!(!message.contains("secret"));
        }
    }
}
