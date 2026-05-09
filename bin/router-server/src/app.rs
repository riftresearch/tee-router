use crate::{
    error::RouterServerError,
    services::{
        AddressScreeningService, OrderExecutionManager, OrderManager, ProviderHealthPoller,
        ProviderHealthProbe, ProviderHealthService, ProviderPolicyService, RouteMinimumService,
        VaultManager,
    },
    Result, RouterServerArgs,
};
use alloy::primitives::U256;
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
            CctpHttpProviderConfig, VeloraHttpProviderConfig,
        },
        asset_registry::ProviderId,
        custody_action_executor::{
            bitcoin_address_from_private_key, evm_address_from_private_key, CustodyActionExecutor,
            HyperliquidRuntimeConfig, PaymasterRegistry,
        },
        route_costs::RouteCostService,
    },
};
use router_primitives::ChainType;
use snafu::ResultExt;
use std::{sync::Arc, time::Duration};
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
    pub order_execution_manager: Arc<OrderExecutionManager>,
    pub provider_policies: Arc<ProviderPolicyService>,
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

    reject_shared_hyperliquid_execution_config(args)?;
    let hyperliquid_runtime = hyperliquid_runtime_config(args)?;
    let chain_registry = Arc::new(initialize_chain_registry(args, paymaster_mode).await?);
    let paymasters = paymaster_registry(args)?;
    let provider_policies = Arc::new(ProviderPolicyService::new(db.clone()));
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
    let route_minimums = Arc::new(RouteMinimumService::new(action_providers.clone()));
    let address_screener = initialize_address_screener(args)?;
    let order_manager = Arc::new(
        OrderManager::with_action_providers(
            db.clone(),
            settings.clone(),
            chain_registry.clone(),
            action_providers.clone(),
        )
        .with_route_minimums(Some(route_minimums))
        .with_route_costs(Some(route_costs.clone()))
        .with_provider_policies(Some(provider_policies.clone()))
        .with_provider_health(Some(provider_health.clone())),
    );
    let custody_action_executor = Arc::new(
        CustodyActionExecutor::new(db.clone(), settings.clone(), chain_registry.clone())
            .with_hyperliquid_runtime(hyperliquid_runtime)
            .with_paymasters(paymasters),
    );
    let order_execution_manager = Arc::new(
        OrderExecutionManager::with_dependencies(
            db.clone(),
            action_providers,
            custody_action_executor,
            chain_registry.clone(),
        )
        .with_route_costs(Some(route_costs.clone()))
        .with_provider_policies(Some(provider_policies.clone())),
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
        order_execution_manager,
        provider_policies,
        provider_health,
        provider_health_poller,
        route_costs,
        address_screener,
    })
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

    if let Some(base_url) =
        normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")?
    {
        let probe = ProviderHealthProbe::get(
            ProviderId::Across.as_str(),
            synthetic_provider_status_url(&base_url),
        )
        .with_bearer_token(required_across_api_key(args.across_api_key.as_deref())?);
        probes.push(probe);
    }

    let cctp_base_url = normalize_optional_url(args.cctp_api_url.as_deref(), "CCTP API URL")?
        .unwrap_or_else(|| CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG.to_string());
    probes.push(ProviderHealthProbe::get(
        ProviderId::Cctp.as_str(),
        synthetic_provider_status_url(&cctp_base_url),
    ));

    if let Some(base_url) =
        normalize_optional_url(args.hyperunit_api_url.as_deref(), "HyperUnit API URL")?
    {
        probes.push(
            ProviderHealthProbe::get(
                ProviderId::Unit.as_str(),
                synthetic_provider_status_url(&base_url),
            )
            .with_proxy_url(normalize_optional_string(
                args.hyperunit_proxy_url.as_deref(),
            )),
        );
    }

    if let Some(base_url) =
        normalize_optional_url(args.hyperliquid_api_url.as_deref(), "Hyperliquid API URL")?
    {
        probes.push(ProviderHealthProbe::get(
            ProviderId::Hyperliquid.as_str(),
            synthetic_provider_status_url(&base_url),
        ));
        probes.push(ProviderHealthProbe::get(
            ProviderId::HyperliquidBridge.as_str(),
            synthetic_provider_status_url(&base_url),
        ));
    }

    if let Some(base_url) =
        normalize_optional_url(args.velora_api_url.as_deref(), "Velora API URL")?
    {
        probes.push(ProviderHealthProbe::get(
            ProviderId::Velora.as_str(),
            synthetic_provider_status_url(&base_url),
        ));
    }

    Ok(probes)
}

fn synthetic_provider_status_url(base_url: &str) -> String {
    format!("{base_url}/status")
}

fn initialize_route_costs(
    args: &RouterServerArgs,
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
) -> Result<RouteCostService> {
    let oracle_config = MarketPricingOracleConfig::new(
        &args.coinbase_price_api_base_url,
        &args.ethereum_mainnet_rpc_url,
        &args.arbitrum_rpc_url,
        &args.base_rpc_url,
    )
    .map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("invalid route-cost pricing oracle config: {err}"),
        },
    })?;
    let pricing_oracle = Arc::new(MarketPricingOracle::new(oracle_config).map_err(|err| {
        crate::Error::DatabaseInit {
            source: RouterServerError::InvalidData {
                message: format!("failed to initialize route-cost pricing oracle: {err}"),
            },
        }
    })?);
    Ok(RouteCostService::new(db, action_providers).with_pricing_oracle(pricing_oracle))
}

async fn initialize_chain_registry(
    args: &RouterServerArgs,
    paymaster_mode: PaymasterMode,
) -> Result<ChainRegistry> {
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
    .map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("failed to initialize bitcoin chain: {err}"),
        },
    })?;
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
                paymaster_mode,
            ),
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
                paymaster_mode,
            ),
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
                paymaster_mode,
            ),
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
    let across_api_url = normalize_optional_url(args.across_api_url.as_deref(), "Across API URL")?;
    let across = if let Some(base_url) = across_api_url {
        Some(
            AcrossHttpProviderConfig::new(
                base_url,
                required_across_api_key(args.across_api_key.as_deref())?,
            )
            .with_integrator_id(args.across_integrator_id.clone()),
        )
    } else {
        None
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
    let registry = ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
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
    .map_err(invalid_config)?;
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

fn reject_shared_hyperliquid_execution_config(args: &RouterServerArgs) -> Result<()> {
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

    Err(invalid_config(format!(
        "shared Hyperliquid execution identities are no longer supported; remove {} and use per-order router-derived Hyperliquid custody",
        configured_fields.join(", ")
    )))
}

fn hyperliquid_runtime_config(args: &RouterServerArgs) -> Result<Option<HyperliquidRuntimeConfig>> {
    Ok(
        normalize_optional_url(args.hyperliquid_api_url.as_deref(), "Hyperliquid API URL")?
            .map(|base_url| HyperliquidRuntimeConfig::new(base_url, args.hyperliquid_network)),
    )
}

fn initialize_address_screener(
    args: &RouterServerArgs,
) -> Result<Option<Arc<AddressScreeningService>>> {
    let host = normalize_optional_url(args.chainalysis_host.as_deref(), "Chainalysis host")?;
    let token = normalize_optional_string(args.chainalysis_token.as_deref());

    match (host, token) {
        (Some(host), Some(token)) => AddressScreeningService::new(host, token)
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

fn paymaster_registry(args: &RouterServerArgs) -> Result<PaymasterRegistry> {
    let mut registry = PaymasterRegistry::new();

    if let Some(private_key) =
        normalize_optional_string(args.ethereum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|err| invalid_config(err.to_string()))?;
        registry.register(ChainType::Ethereum, address);
    }
    if let Some(private_key) = normalize_optional_string(args.base_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|err| invalid_config(err.to_string()))?;
        registry.register(ChainType::Base, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.arbitrum_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|err| invalid_config(err.to_string()))?;
        registry.register(ChainType::Arbitrum, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.bitcoin_paymaster_private_key.as_deref())
    {
        let address = bitcoin_address_from_private_key(&private_key, args.bitcoin_network)
            .map_err(|err| invalid_config(err.to_string()))?;
        registry.register(ChainType::Bitcoin, address);
    }
    if let Some(private_key) =
        normalize_optional_string(args.hyperliquid_paymaster_private_key.as_deref())
    {
        let address = evm_address_from_private_key(&private_key)
            .map_err(|err| invalid_config(err.to_string()))?;
        registry.register(ChainType::Hyperliquid, address);
    }

    Ok(registry)
}

fn invalid_config(message: impl Into<String>) -> crate::Error {
    crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: message.into(),
        },
    }
}

fn parse_u256_arg(value: &str, name: &str) -> Result<U256> {
    U256::from_str_radix(value, 10).map_err(|err| crate::Error::DatabaseInit {
        source: RouterServerError::InvalidData {
            message: format!("invalid {name}: {err}"),
        },
    })
}

fn gas_sponsor_config(
    private_key: Option<&String>,
    vault_gas_buffer_wei: U256,
    paymaster_mode: PaymasterMode,
) -> Option<EvmGasSponsorConfig> {
    if paymaster_mode == PaymasterMode::Disabled {
        return None;
    }

    let private_key = private_key?;

    Some(EvmGasSponsorConfig {
        private_key: private_key.clone(),
        vault_gas_buffer_wei,
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
            master_key_path: "/tmp/router-server-master-key.hex".to_string(),
            ethereum_mainnet_rpc_url: "https://eth.example".to_string(),
            ethereum_reference_token: "0x0000000000000000000000000000000000000000".to_string(),
            ethereum_paymaster_private_key: None,
            base_rpc_url: "https://base.example".to_string(),
            base_reference_token: "0x0000000000000000000000000000000000000000".to_string(),
            base_paymaster_private_key: None,
            arbitrum_rpc_url: "https://arb.example".to_string(),
            arbitrum_reference_token: "0x0000000000000000000000000000000000000000".to_string(),
            arbitrum_paymaster_private_key: None,
            evm_paymaster_vault_gas_buffer_wei: "0".to_string(),
            evm_paymaster_vault_gas_target_wei: None,
            bitcoin_rpc_url: "http://btc.example".to_string(),
            bitcoin_rpc_auth: Auth::None,
            untrusted_esplora_http_server_url: "https://esplora.example".to_string(),
            bitcoin_network: bitcoin::Network::Bitcoin,
            bitcoin_paymaster_private_key: None,
            cors_domain: None,
            chainalysis_host: None,
            chainalysis_token: None,
            loki_url: None,
            across_api_url: None,
            across_api_key: None,
            across_integrator_id: None,
            cctp_api_url: None,
            cctp_token_messenger_v2_address: None,
            cctp_message_transmitter_v2_address: None,
            hyperunit_api_url: None,
            hyperunit_proxy_url: None,
            hyperliquid_api_url: None,
            velora_api_url: None,
            velora_partner: None,
            hyperliquid_execution_private_key: None,
            hyperliquid_account_address: None,
            hyperliquid_vault_address: None,
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
            worker_lease_name: Some("global-router-worker".to_string()),
            worker_lease_seconds: 300,
            worker_lease_renew_seconds: 30,
            worker_standby_poll_seconds: 5,
            worker_refund_poll_seconds: 60,
            worker_order_execution_poll_seconds: 5,
            worker_route_cost_refresh_seconds: 300,
            worker_provider_health_poll_seconds: 120,
            provider_health_timeout_seconds: 10,
            worker_provider_operation_hint_pass_limit: 500,
            worker_order_maintenance_pass_limit: 100,
            worker_order_planning_pass_limit: 100,
            worker_order_execution_pass_limit: 25,
            worker_order_execution_concurrency: 64,
            worker_vault_funding_hint_pass_limit: 100,
            coinbase_price_api_base_url: "https://api.coinbase.com".to_string(),
        }
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

    #[test]
    fn reject_shared_hyperliquid_execution_config_accepts_absent_settings() {
        let args = base_args();
        reject_shared_hyperliquid_execution_config(&args).unwrap();
    }

    #[test]
    fn reject_shared_hyperliquid_execution_config_rejects_legacy_settings() {
        let mut args = base_args();
        args.hyperliquid_execution_private_key =
            Some("0x59c6995e998f97a5a0044976f7ad0a7df4976fbe66f6cc18ff3c16f18a6b9e3f".to_string());
        args.hyperliquid_account_address =
            Some("0x1111111111111111111111111111111111111111".to_string());
        args.hyperliquid_vault_address =
            Some("0x2222222222222222222222222222222222222222".to_string());
        let err = reject_shared_hyperliquid_execution_config(&args).expect_err("must reject");
        let message = err.to_string();
        assert!(message.contains("shared Hyperliquid execution identities"));
        assert!(message.contains("HYPERLIQUID_EXECUTION_PRIVATE_KEY"));
        assert!(message.contains("HYPERLIQUID_ACCOUNT_ADDRESS"));
        assert!(message.contains("HYPERLIQUID_VAULT_ADDRESS"));
    }
}
