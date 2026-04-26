use std::{env, sync::Arc, time::Duration};

use bitcoincore_rpc_async::Auth;
use chains::{bitcoin::BitcoinChain, evm::EvmChain, hyperliquid::HyperliquidChain, ChainRegistry};
use router_primitives::ChainType;
use router_server::{
    api::MarketOrderQuoteRequest,
    config::Settings,
    db::Database,
    models::MarketOrderKind,
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            AcrossHttpProviderConfig, ActionProviderRegistry, ExchangeProvider,
            ExchangeQuoteRequest, HyperliquidProvider,
        },
        asset_registry::AssetRegistry,
        custody_action_executor::HyperliquidCallNetwork,
        order_manager::OrderManager,
    },
};
use serde_json::Value;
use tempfile::TempDir;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const RUN_FLAG: &str = "ROUTER_LIVE_RUNTIME_E2E";
const HYPERLIQUID_API_URL_ENV: &str = "HYPERLIQUID_API_URL";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_ACROSS_API_URL: &str = "https://app.across.to/api";
const DEFAULT_HYPERUNIT_API_URL: &str = "https://api.hyperunit.xyz";
const PEPE_ETHEREUM: &str = "0x6982508145454ce325ddbe47a25d4ec3d2311933";
const WBTC_ARBITRUM: &str = "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f";
const DEFAULT_BTC_RECIPIENT: &str = "bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn";
const DEFAULT_EVM_RECIPIENT: &str = "0x0000000000000000000000000000000000000001";
const DEFAULT_PEPE_AMOUNT_IN_RAW: &str = "1000000000000000000000000000";
const DEFAULT_BTC_AMOUNT_IN_SATS: &str = "1000000";
const DEFAULT_ARBITRUM_WBTC_AMOUNT_IN_RAW: &str = "1000000";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";

fn deposit_asset(chain: &str, asset: AssetId) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid static chain id"),
        asset,
    }
}

#[tokio::test]
#[ignore = "probes live Hyperliquid quote composition without spending funds"]
async fn live_hyperliquid_eth_btc_quote_probe() -> TestResult<()> {
    if env::var(RUN_FLAG).ok().as_deref() != Some("1") {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let provider = HyperliquidProvider::new(
        env::var(HYPERLIQUID_API_URL_ENV)
            .unwrap_or_else(|_| DEFAULT_HYPERLIQUID_API_URL.to_string()),
        HyperliquidCallNetwork::Mainnet,
        Arc::new(AssetRegistry::default()),
        30_000,
    );

    let quote = provider
        .quote_trade(ExchangeQuoteRequest {
            input_asset: deposit_asset("evm:1", AssetId::Native),
            output_asset: deposit_asset("bitcoin", AssetId::Native),
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "49718698578462480".to_string(),
                min_amount_out: "1".to_string(),
            },
            sender_address: None,
            recipient_address: "bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn".to_string(),
        })
        .await
        .map_err(|error| format!("hyperliquid quote failed: {error}"))?
        .ok_or("hyperliquid quote returned None")?;

    eprintln!(
        "live_hyperliquid_eth_btc_quote_probe amount_in={} amount_out={} provider_quote={}",
        quote.amount_in, quote.amount_out, quote.provider_quote
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_pepe_eth_to_bitcoin() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("evm:1", AssetId::reference(PEPE_ETHEREUM));
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = env_var_any(&["ROUTER_LIVE_PEPE_AMOUNT_IN_RAW"])
        .unwrap_or_else(|| DEFAULT_PEPE_AMOUNT_IN_RAW.to_string());
    let recipient_address = env_var_any(&[
        "ROUTER_LIVE_BTC_RECIPIENT_ADDRESS",
        "ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS",
    ])
    .unwrap_or_else(|| DEFAULT_BTC_RECIPIENT.to_string());

    let response = ctx
        .order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset.clone(),
            recipient_address,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
        })
        .await
        .map_err(|error| format!("router PEPE->BTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_transition_kinds(
        &quote.provider_quote,
        &["across_bridge", "hyperliquid_trade", "unit_withdrawal"],
    );

    eprintln!(
        "live_router_quote_probe_pepe_eth_to_bitcoin quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_bitcoin_to_pepe_eth() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("bitcoin", AssetId::Native);
    let destination_asset = deposit_asset("evm:1", AssetId::reference(PEPE_ETHEREUM));
    let amount_in = env_var_any(&["ROUTER_LIVE_BTC_AMOUNT_IN_SATS"])
        .unwrap_or_else(|| DEFAULT_BTC_AMOUNT_IN_SATS.to_string());
    let recipient_address = env_var_any(&["ROUTER_LIVE_EVM_RECIPIENT_ADDRESS"])
        .unwrap_or_else(|| DEFAULT_EVM_RECIPIENT.to_string());

    let response = ctx
        .order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset.clone(),
            recipient_address,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
        })
        .await
        .map_err(|error| format!("router BTC->PEPE live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_transition_kinds(
        &quote.provider_quote,
        &[
            "unit_deposit",
            "hyperliquid_trade",
            "unit_withdrawal",
            "across_bridge",
        ],
    );

    eprintln!(
        "live_router_quote_probe_bitcoin_to_pepe_eth quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_bitcoin_to_wbtc_arbitrum() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("bitcoin", AssetId::Native);
    let destination_asset = deposit_asset("evm:42161", AssetId::reference(WBTC_ARBITRUM));
    let amount_in = env_var_any(&["ROUTER_LIVE_BTC_TO_ARBITRUM_WBTC_AMOUNT_IN_SATS"])
        .unwrap_or_else(|| DEFAULT_BTC_AMOUNT_IN_SATS.to_string());
    let recipient_address = env_var_any(&["ROUTER_LIVE_EVM_RECIPIENT_ADDRESS"])
        .unwrap_or_else(|| DEFAULT_EVM_RECIPIENT.to_string());

    let response = ctx
        .order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset.clone(),
            recipient_address,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
        })
        .await
        .map_err(|error| format!("router BTC->Arbitrum WBTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_transition_kinds(
        &quote.provider_quote,
        &[
            "unit_deposit",
            "hyperliquid_trade",
            "unit_withdrawal",
            "across_bridge",
        ],
    );

    eprintln!(
        "live_router_quote_probe_bitcoin_to_wbtc_arbitrum quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_wbtc_arbitrum_to_bitcoin() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("evm:42161", AssetId::reference(WBTC_ARBITRUM));
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = env_var_any(&["ROUTER_LIVE_ARBITRUM_WBTC_AMOUNT_IN_RAW"])
        .unwrap_or_else(|| DEFAULT_ARBITRUM_WBTC_AMOUNT_IN_RAW.to_string());
    let recipient_address = env_var_any(&[
        "ROUTER_LIVE_BTC_RECIPIENT_ADDRESS",
        "ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS",
    ])
    .unwrap_or_else(|| DEFAULT_BTC_RECIPIENT.to_string());

    let response = ctx
        .order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset.clone(),
            recipient_address,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
        })
        .await
        .map_err(|error| format!("router Arbitrum WBTC->BTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_transition_kinds(
        &quote.provider_quote,
        &["across_bridge", "hyperliquid_trade", "unit_withdrawal"],
    );

    eprintln!(
        "live_router_quote_probe_wbtc_arbitrum_to_bitcoin quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

struct LiveRouterQuoteContext {
    order_manager: OrderManager,
    _config_dir: TempDir,
    _postgres: Option<ContainerAsync<GenericImage>>,
}

impl LiveRouterQuoteContext {
    async fn new() -> TestResult<Self> {
        let action_providers = Arc::new(live_action_provider_registry()?);
        let (db, postgres) = live_quote_database().await?;
        let config_dir = tempfile::tempdir()?;
        let settings = Arc::new(test_settings(config_dir.path())?);
        let chain_registry = Arc::new(live_quote_chain_registry().await?);
        let order_manager =
            OrderManager::with_action_providers(db, settings, chain_registry, action_providers);

        Ok(Self {
            order_manager,
            _config_dir: config_dir,
            _postgres: postgres,
        })
    }
}

fn live_probes_enabled() -> bool {
    env::var(RUN_FLAG).ok().as_deref() == Some("1")
}

fn live_action_provider_registry() -> TestResult<ActionProviderRegistry> {
    let across_api_key = env_var_any(&[
        "ROUTER_LIVE_ACROSS_API_KEY",
        "ACROSS_API_KEY",
        "ACROSS_LIVE_API_KEY",
    ])
    .ok_or("missing ROUTER_LIVE_ACROSS_API_KEY, ACROSS_API_KEY, or ACROSS_LIVE_API_KEY")?;
    let across_integrator_id = env_var_any(&[
        "ROUTER_LIVE_ACROSS_INTEGRATOR_ID",
        "ACROSS_INTEGRATOR_ID",
        "ACROSS_LIVE_INTEGRATOR_ID",
    ])
    .ok_or(
        "missing ROUTER_LIVE_ACROSS_INTEGRATOR_ID, ACROSS_INTEGRATOR_ID, or ACROSS_LIVE_INTEGRATOR_ID",
    )?;
    let across_api_url = env_var_any(&[
        "ROUTER_LIVE_ACROSS_API_URL",
        "ACROSS_API_URL",
        "ACROSS_LIVE_BASE_URL",
    ])
    .unwrap_or_else(|| DEFAULT_ACROSS_API_URL.to_string());
    let hyperunit_api_url = env_var_any(&["ROUTER_LIVE_UNIT_API_URL", "HYPERUNIT_API_URL"])
        .unwrap_or_else(|| DEFAULT_HYPERUNIT_API_URL.to_string());
    let hyperunit_proxy_url = env_var_any(&["ROUTER_LIVE_UNIT_PROXY_URL", "HYPERUNIT_PROXY_URL"]);
    let hyperliquid_api_url =
        env_var_any(&["ROUTER_LIVE_HYPERLIQUID_API_URL", HYPERLIQUID_API_URL_ENV])
            .unwrap_or_else(|| DEFAULT_HYPERLIQUID_API_URL.to_string());

    ActionProviderRegistry::http(
        Some(
            AcrossHttpProviderConfig::new(across_api_url, across_api_key)
                .with_integrator_id(Some(across_integrator_id)),
        ),
        Some(hyperunit_api_url),
        hyperunit_proxy_url,
        Some(hyperliquid_api_url),
        None,
        HyperliquidCallNetwork::Mainnet,
    )
    .map_err(|error| format!("build live action provider registry: {error}").into())
}

async fn live_quote_database() -> TestResult<(Database, Option<ContainerAsync<GenericImage>>)> {
    if let Ok(database_url) = env::var(ROUTER_TEST_DATABASE_URL_ENV) {
        let db = Database::connect(&database_url, 5, 1).await?;
        return Ok((db, None));
    }

    let image = GenericImage::new("postgres", "15-alpine")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE);
    let container = image.start().await?;
    let port = container.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
    );
    let db = Database::connect(&database_url, 5, 1).await?;
    Ok((db, Some(container)))
}

async fn live_quote_chain_registry() -> TestResult<ChainRegistry> {
    let mut registry = ChainRegistry::new();
    registry.register_bitcoin(
        ChainType::Bitcoin,
        Arc::new(BitcoinChain::new(
            "http://127.0.0.1:8332",
            Auth::None,
            "http://127.0.0.1:3002",
            bitcoin::Network::Bitcoin,
        )?),
    );
    registry.register_evm(
        ChainType::Ethereum,
        Arc::new(
            EvmChain::new(
                "http://127.0.0.1:8545",
                PEPE_ETHEREUM,
                ChainType::Ethereum,
                b"router-ethereum-wallet",
                4,
                Duration::from_secs(12),
            )
            .await?,
        ),
    );
    registry.register_evm(
        ChainType::Arbitrum,
        Arc::new(
            EvmChain::new(
                "http://127.0.0.1:8547",
                PEPE_ETHEREUM,
                ChainType::Arbitrum,
                b"router-arbitrum-wallet",
                2,
                Duration::from_secs(2),
            )
            .await?,
        ),
    );
    registry.register_evm(
        ChainType::Base,
        Arc::new(
            EvmChain::new(
                "http://127.0.0.1:8546",
                PEPE_ETHEREUM,
                ChainType::Base,
                b"router-base-wallet",
                2,
                Duration::from_secs(2),
            )
            .await?,
        ),
    );
    registry.register(
        ChainType::Hyperliquid,
        Arc::new(HyperliquidChain::new(
            b"router-hyperliquid-wallet",
            1,
            Duration::from_secs(1),
        )),
    );
    Ok(registry)
}

fn test_settings(dir: &std::path::Path) -> TestResult<Settings> {
    let path = dir.join("router-server-master-key.hex");
    std::fs::write(&path, alloy::hex::encode([0x42_u8; 64]))?;
    Settings::load(path).map_err(|error| format!("load live quote test settings: {error}").into())
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn assert_transition_kinds(provider_quote: &Value, expected_kinds: &[&str]) {
    let transitions = provider_quote["transitions"]
        .as_array()
        .expect("quote should serialize transition declarations");
    for expected in expected_kinds {
        assert!(
            transitions
                .iter()
                .any(|transition| transition["kind"] == *expected),
            "quote should contain transition kind {expected}; transitions={transitions:?}"
        );
    }
}
