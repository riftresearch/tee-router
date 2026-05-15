use std::{env, sync::Arc, time::Duration};

use bitcoincore_rpc_async::Auth;
use chains::{bitcoin::BitcoinChain, evm::EvmChain, hyperliquid::HyperliquidChain, ChainRegistry};
use router_core::{
    config::Settings,
    db::Database,
    models::{MarketOrderKind, MarketOrderQuote},
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            AcrossHttpProviderConfig, ActionProviderRegistry, BridgeProvider, BridgeQuote,
            BridgeQuoteRequest, CctpHttpProviderConfig, ExchangeProvider, ExchangeQuote,
            ExchangeQuoteRequest, HyperliquidProvider, VeloraHttpProviderConfig,
        },
        asset_registry::AssetRegistry,
        custody_action_executor::HyperliquidCallNetwork,
    },
};
use router_primitives::ChainType;
use router_server::{
    api::{MarketOrderQuoteKind, MarketOrderQuoteRequest},
    services::order_manager::OrderManager,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

mod support;
use support::{assert_provider_quote_leg_amounts_are_raw, assert_raw_amount_string};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

const RUN_FLAG: &str = "ROUTER_LIVE_RUNTIME_E2E";
const HYPERLIQUID_API_URL_ENV: &str = "HYPERLIQUID_API_URL";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_ACROSS_API_URL: &str = "https://app.across.to/api";
const DEFAULT_CCTP_API_URL: &str = "https://iris-api.circle.com";
const DEFAULT_HYPERUNIT_API_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_VELORA_API_URL: &str = "https://api.paraswap.io";
const ETHEREUM_USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const BASE_USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ARBITRUM_USDC: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const DEFAULT_BTC_RECIPIENT: &str = "bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn";
const DEFAULT_EVM_RECIPIENT: &str = "0x0000000000000000000000000000000000000001";
const DEFAULT_BTC_AMOUNT_IN_SATS: &str = "1000000";
const DEFAULT_ETH_AMOUNT_IN_WEI: &str = "50000000000000000";
const DEFAULT_USDC_AMOUNT_IN_RAW: &str = "60000000";
const DEFAULT_UBTC_PROBE_USDC_AMOUNT_IN_RAW: &str = "100000000";
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
async fn live_hyperliquid_ueth_ubtc_quote_probe() -> TestResult<()> {
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
    )?;

    let quote = provider
        .quote_trade(ExchangeQuoteRequest {
            input_asset: deposit_asset("hyperliquid", AssetId::reference("UETH")),
            output_asset: deposit_asset("hyperliquid", AssetId::reference("UBTC")),
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: DEFAULT_ETH_AMOUNT_IN_WEI.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: None,
            recipient_address: DEFAULT_EVM_RECIPIENT.to_string(),
        })
        .await
        .map_err(|error| format!("hyperliquid quote failed: {error}"))?
        .ok_or("hyperliquid quote returned None")?;
    assert_exchange_quote_amount_format(&quote);

    eprintln!(
        "live_hyperliquid_ueth_ubtc_quote_probe amount_in={} amount_out={} provider_quote={}",
        quote.amount_in, quote.amount_out, quote.provider_quote
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_base_usdc_to_bitcoin() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("evm:8453", AssetId::reference(BASE_USDC));
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = env_var_any(&["ROUTER_LIVE_BASE_USDC_TO_BTC_AMOUNT"])
        .unwrap_or_else(|| DEFAULT_USDC_AMOUNT_IN_RAW.to_string());
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
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.clone(),
                slippage_bps: Some(100),
            },
        })
        .await
        .map_err(|error| format!("router Base USDC->BTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_market_quote_amount_format(quote);
    assert_transition_kinds(
        &quote.provider_quote,
        &[
            "hyperliquid_bridge_deposit",
            "hyperliquid_trade",
            "unit_withdrawal",
        ],
    );
    assert_any_transition_kind(&quote.provider_quote, &["across_bridge", "cctp_bridge"]);

    eprintln!(
        "live_router_quote_probe_base_usdc_to_bitcoin quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live route alternatives into Hyperliquid UBTC without spending funds"]
async fn live_router_quote_probe_base_usdc_to_hyperliquid_ubtc_paths() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let amount_in = env_var_any(&["ROUTER_LIVE_BASE_USDC_TO_UBTC_AMOUNT"])
        .unwrap_or_else(|| DEFAULT_UBTC_PROBE_USDC_AMOUNT_IN_RAW.to_string());
    let recipient_address = env_var_any(&["ROUTER_LIVE_EVM_RECIPIENT_ADDRESS"])
        .unwrap_or_else(|| DEFAULT_EVM_RECIPIENT.to_string());
    let source_asset = deposit_asset("evm:8453", AssetId::reference(BASE_USDC));
    let destination_asset = deposit_asset("hyperliquid", AssetId::reference("UBTC"));

    let planner_report = match LiveRouterQuoteContext::new().await {
        Ok(ctx) => {
            let result = ctx
                .order_manager
                .quote_market_order(MarketOrderQuoteRequest {
                    from_asset: source_asset.clone(),
                    to_asset: destination_asset.clone(),
                    recipient_address: recipient_address.clone(),
                    order_kind: MarketOrderQuoteKind::ExactIn {
                        amount_in: amount_in.clone(),
                        slippage_bps: Some(100),
                    },
                })
                .await;
            match result {
                Ok(response) => {
                    let quote = response
                        .quote
                        .as_market_order()
                        .expect("market order quote");
                    assert_market_quote_amount_format(quote);
                    market_quote_summary("planner_base_usdc_to_hyperliquid_ubtc", quote)
                }
                Err(error) => json!({
                    "name": "planner_base_usdc_to_hyperliquid_ubtc",
                    "available": false,
                    "error": error.to_string(),
                }),
            }
        }
        Err(error) => json!({
            "name": "planner_base_usdc_to_hyperliquid_ubtc",
            "available": false,
            "error": error.to_string(),
        }),
    };

    let registry = live_action_provider_registry()?;
    let across = registry
        .bridge("across")
        .ok_or("Across bridge provider not configured")?;
    let cctp = registry
        .bridge("cctp")
        .ok_or("CCTP bridge provider not configured")?;
    let hyperliquid_bridge = registry
        .bridge("hyperliquid_bridge")
        .ok_or("Hyperliquid bridge provider not configured")?;
    let hyperliquid = registry
        .exchange("hyperliquid")
        .ok_or("Hyperliquid exchange provider not configured")?;
    let velora = registry
        .exchange("velora")
        .ok_or("Velora exchange provider not configured")?;

    let test1_across = quote_test1_base_usdc_to_ubtc_via_arbitrum_usdc(
        "test1_across_to_arbitrum_usdc",
        across.as_ref(),
        hyperliquid_bridge.as_ref(),
        hyperliquid.as_ref(),
        &recipient_address,
        &amount_in,
    )
    .await;
    let test1_cctp = quote_test1_base_usdc_to_ubtc_via_arbitrum_usdc(
        "test1_cctp_to_arbitrum_usdc",
        cctp.as_ref(),
        hyperliquid_bridge.as_ref(),
        hyperliquid.as_ref(),
        &recipient_address,
        &amount_in,
    )
    .await;
    let test1_best = best_route_by_final_amount(&test1_across, &test1_cctp);
    let test2 = quote_test2_velora_base_eth_to_unit_ueth_to_ubtc(
        velora.as_ref(),
        across.as_ref(),
        hyperliquid.as_ref(),
        &recipient_address,
        &amount_in,
    )
    .await;
    let test3 = quote_test3_across_base_usdc_to_eth_to_unit_ueth_to_ubtc(
        across.as_ref(),
        hyperliquid.as_ref(),
        &recipient_address,
        &amount_in,
    )
    .await;

    let report = json!({
        "input": {
            "source": asset_summary(&source_asset, Some(6)),
            "destination": asset_summary(&destination_asset, Some(8)),
            "amount_in_raw": amount_in,
            "amount_in": format_units_str(&amount_in, 6),
        },
        "planner": planner_report,
        "forced_paths": {
            "test1_best": test1_best,
            "test1_across": test1_across,
            "test1_cctp": test1_cctp,
            "test2": test2,
            "test3": test3,
        },
        "notes": [
            "Unit/HyperUnit ETH deposit is modeled as amount-preserving address generation; it has no quote endpoint in the current provider trait.",
            "All amounts are quote-only raw token amounts plus display values; this test does not submit approvals, bridge transactions, deposits, or trades."
        ],
    });

    eprintln!(
        "{}",
        serde_json::to_string_pretty(&report)
            .map_err(|error| format!("serialize route quote report: {error}"))?
    );

    Ok(())
}

#[tokio::test]
#[ignore = "probes live router quote composition without spending funds"]
async fn live_router_quote_probe_bitcoin_to_base_eth() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("bitcoin", AssetId::Native);
    let destination_asset = deposit_asset("evm:8453", AssetId::Native);
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
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.clone(),
                slippage_bps: Some(100),
            },
        })
        .await
        .map_err(|error| format!("router BTC->Base ETH live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_market_quote_amount_format(quote);
    assert_transition_kinds(
        &quote.provider_quote,
        &["unit_deposit", "hyperliquid_trade", "unit_withdrawal"],
    );

    eprintln!(
        "live_router_quote_probe_bitcoin_to_base_eth quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
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
async fn live_router_quote_probe_ethereum_eth_to_bitcoin() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("evm:1", AssetId::Native);
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = env_var_any(&["ROUTER_LIVE_ETH_AMOUNT_IN_WEI"])
        .unwrap_or_else(|| DEFAULT_ETH_AMOUNT_IN_WEI.to_string());
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
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.clone(),
                slippage_bps: Some(100),
            },
        })
        .await
        .map_err(|error| format!("router Ethereum ETH->BTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_market_quote_amount_format(quote);
    assert_transition_kinds(
        &quote.provider_quote,
        &["unit_deposit", "hyperliquid_trade", "unit_withdrawal"],
    );

    eprintln!(
        "live_router_quote_probe_ethereum_eth_to_bitcoin quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
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
async fn live_router_quote_probe_arbitrum_usdc_to_bitcoin() -> TestResult<()> {
    if !live_probes_enabled() {
        eprintln!("set {RUN_FLAG}=1 to run live quote probes");
        return Ok(());
    }

    let ctx = LiveRouterQuoteContext::new().await?;
    let source_asset = deposit_asset("evm:42161", AssetId::reference(ARBITRUM_USDC));
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = env_var_any(&["ROUTER_LIVE_ARBITRUM_USDC_TO_BTC_AMOUNT"])
        .unwrap_or_else(|| DEFAULT_USDC_AMOUNT_IN_RAW.to_string());
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
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.clone(),
                slippage_bps: Some(100),
            },
        })
        .await
        .map_err(|error| format!("router Arbitrum USDC->BTC live quote failed: {error}"))?;

    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert_eq!(quote.source_asset, source_asset);
    assert_eq!(quote.destination_asset, destination_asset);
    assert_eq!(quote.amount_in, amount_in);
    assert_market_quote_amount_format(quote);
    assert_transition_kinds(
        &quote.provider_quote,
        &[
            "hyperliquid_bridge_deposit",
            "hyperliquid_trade",
            "unit_withdrawal",
        ],
    );

    eprintln!(
        "live_router_quote_probe_arbitrum_usdc_to_bitcoin quote_id={} provider_id={} amount_in={} amount_out={} path_id={}",
        quote.id,
        quote.provider_id,
        quote.amount_in,
        quote.amount_out,
        quote.provider_quote["path_id"].as_str().unwrap_or("<missing>")
    );

    Ok(())
}

async fn quote_test1_base_usdc_to_ubtc_via_arbitrum_usdc(
    name: &str,
    usdc_bridge: &dyn BridgeProvider,
    hyperliquid_bridge: &dyn BridgeProvider,
    hyperliquid: &dyn ExchangeProvider,
    recipient_address: &str,
    amount_in: &str,
) -> Value {
    let source_asset = deposit_asset("evm:8453", AssetId::reference(BASE_USDC));
    let arbitrum_usdc = deposit_asset("evm:42161", AssetId::reference(ARBITRUM_USDC));
    let hyperliquid_usdc = deposit_asset("hyperliquid", AssetId::Native);
    let hyperliquid_ubtc = deposit_asset("hyperliquid", AssetId::reference("UBTC"));

    let result = async {
        let bridge_quote = quote_bridge_exact_in(
            usdc_bridge,
            source_asset.clone(),
            arbitrum_usdc.clone(),
            amount_in,
            recipient_address,
        )
        .await?;
        let hyperliquid_bridge_quote = quote_bridge_exact_in(
            hyperliquid_bridge,
            arbitrum_usdc.clone(),
            hyperliquid_usdc.clone(),
            &bridge_quote.amount_out,
            recipient_address,
        )
        .await?;
        let trade_quote = quote_trade_exact_in(
            hyperliquid,
            hyperliquid_usdc.clone(),
            hyperliquid_ubtc.clone(),
            &hyperliquid_bridge_quote.amount_out,
            None,
            None,
            recipient_address,
        )
        .await?;
        Ok::<_, String>(route_success(
            name,
            amount_in,
            &trade_quote.amount_out,
            vec![
                bridge_step_summary(
                    "base_usdc_to_arbitrum_usdc",
                    &bridge_quote,
                    &source_asset,
                    &arbitrum_usdc,
                    6,
                    6,
                ),
                bridge_step_summary(
                    "arbitrum_usdc_to_hyperliquid_usdc",
                    &hyperliquid_bridge_quote,
                    &arbitrum_usdc,
                    &hyperliquid_usdc,
                    6,
                    6,
                ),
                exchange_step_summary(
                    "hyperliquid_usdc_to_ubtc",
                    &trade_quote,
                    &hyperliquid_usdc,
                    &hyperliquid_ubtc,
                    6,
                    8,
                ),
            ],
        ))
    }
    .await;

    result.unwrap_or_else(|error| route_unavailable(name, error))
}

async fn quote_test2_velora_base_eth_to_unit_ueth_to_ubtc(
    velora: &dyn ExchangeProvider,
    across: &dyn BridgeProvider,
    hyperliquid: &dyn ExchangeProvider,
    recipient_address: &str,
    amount_in: &str,
) -> Value {
    let base_usdc = deposit_asset("evm:8453", AssetId::reference(BASE_USDC));
    let base_eth = deposit_asset("evm:8453", AssetId::Native);
    let ethereum_eth = deposit_asset("evm:1", AssetId::Native);
    let hyperliquid_ueth = deposit_asset("hyperliquid", AssetId::reference("UETH"));
    let hyperliquid_ubtc = deposit_asset("hyperliquid", AssetId::reference("UBTC"));

    let result = async {
        let velora_quote = quote_trade_exact_in(
            velora,
            base_usdc.clone(),
            base_eth.clone(),
            amount_in,
            Some(6),
            None,
            recipient_address,
        )
        .await?;
        let eth_bridge_quote = quote_bridge_exact_in(
            across,
            base_eth.clone(),
            ethereum_eth.clone(),
            &velora_quote.amount_out,
            recipient_address,
        )
        .await?;
        let unit_amount_out = eth_bridge_quote.amount_out.clone();
        let hyperliquid_quote = quote_trade_exact_in(
            hyperliquid,
            hyperliquid_ueth.clone(),
            hyperliquid_ubtc.clone(),
            &unit_amount_out,
            None,
            None,
            recipient_address,
        )
        .await?;
        Ok::<_, String>(route_success(
            "test2_velora_base_eth_to_unit_ueth_to_ubtc",
            amount_in,
            &hyperliquid_quote.amount_out,
            vec![
                exchange_step_summary(
                    "velora_base_usdc_to_base_eth",
                    &velora_quote,
                    &base_usdc,
                    &base_eth,
                    6,
                    18,
                ),
                bridge_step_summary(
                    "across_base_eth_to_ethereum_eth",
                    &eth_bridge_quote,
                    &base_eth,
                    &ethereum_eth,
                    18,
                    18,
                ),
                passthrough_step_summary(
                    "unit_ethereum_eth_to_hyperliquid_ueth",
                    "unit",
                    &ethereum_eth,
                    &hyperliquid_ueth,
                    &unit_amount_out,
                    18,
                    18,
                ),
                exchange_step_summary(
                    "hyperliquid_ueth_to_ubtc",
                    &hyperliquid_quote,
                    &hyperliquid_ueth,
                    &hyperliquid_ubtc,
                    18,
                    8,
                ),
            ],
        ))
    }
    .await;

    result.unwrap_or_else(|error| {
        route_unavailable("test2_velora_base_eth_to_unit_ueth_to_ubtc", error)
    })
}

async fn quote_test3_across_base_usdc_to_eth_to_unit_ueth_to_ubtc(
    across: &dyn BridgeProvider,
    hyperliquid: &dyn ExchangeProvider,
    recipient_address: &str,
    amount_in: &str,
) -> Value {
    let base_usdc = deposit_asset("evm:8453", AssetId::reference(BASE_USDC));
    let ethereum_eth = deposit_asset("evm:1", AssetId::Native);
    let hyperliquid_ueth = deposit_asset("hyperliquid", AssetId::reference("UETH"));
    let hyperliquid_usdc = deposit_asset("hyperliquid", AssetId::Native);
    let hyperliquid_ubtc = deposit_asset("hyperliquid", AssetId::reference("UBTC"));

    let result = async {
        let across_quote = quote_bridge_exact_in(
            across,
            base_usdc.clone(),
            ethereum_eth.clone(),
            amount_in,
            recipient_address,
        )
        .await?;
        let unit_amount_out = across_quote.amount_out.clone();
        let ueth_to_usdc = quote_trade_exact_in(
            hyperliquid,
            hyperliquid_ueth.clone(),
            hyperliquid_usdc.clone(),
            &unit_amount_out,
            None,
            None,
            recipient_address,
        )
        .await?;
        let usdc_to_ubtc = quote_trade_exact_in(
            hyperliquid,
            hyperliquid_usdc.clone(),
            hyperliquid_ubtc.clone(),
            &ueth_to_usdc.amount_out,
            None,
            None,
            recipient_address,
        )
        .await?;
        Ok::<_, String>(route_success(
            "test3_across_base_usdc_to_ethereum_eth_to_unit_ueth_to_ubtc",
            amount_in,
            &usdc_to_ubtc.amount_out,
            vec![
                bridge_step_summary(
                    "across_base_usdc_to_ethereum_eth",
                    &across_quote,
                    &base_usdc,
                    &ethereum_eth,
                    6,
                    18,
                ),
                passthrough_step_summary(
                    "unit_ethereum_eth_to_hyperliquid_ueth",
                    "unit",
                    &ethereum_eth,
                    &hyperliquid_ueth,
                    &unit_amount_out,
                    18,
                    18,
                ),
                exchange_step_summary(
                    "hyperliquid_ueth_to_usdc",
                    &ueth_to_usdc,
                    &hyperliquid_ueth,
                    &hyperliquid_usdc,
                    18,
                    6,
                ),
                exchange_step_summary(
                    "hyperliquid_usdc_to_ubtc",
                    &usdc_to_ubtc,
                    &hyperliquid_usdc,
                    &hyperliquid_ubtc,
                    6,
                    8,
                ),
            ],
        ))
    }
    .await;

    result.unwrap_or_else(|error| {
        route_unavailable(
            "test3_across_base_usdc_to_ethereum_eth_to_unit_ueth_to_ubtc",
            error,
        )
    })
}

async fn quote_bridge_exact_in(
    provider: &dyn BridgeProvider,
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    amount_in: &str,
    recipient_address: &str,
) -> Result<BridgeQuote, String> {
    provider
        .quote_bridge(BridgeQuoteRequest {
            source_asset,
            destination_asset,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: recipient_address.to_string(),
            depositor_address: recipient_address.to_string(),
            partial_fills_enabled: false,
        })
        .await?
        .ok_or_else(|| format!("{} returned no bridge quote", provider.id()))
}

async fn quote_trade_exact_in(
    provider: &dyn ExchangeProvider,
    input_asset: DepositAsset,
    output_asset: DepositAsset,
    amount_in: &str,
    input_decimals: Option<u8>,
    output_decimals: Option<u8>,
    recipient_address: &str,
) -> Result<ExchangeQuote, String> {
    provider
        .quote_trade(ExchangeQuoteRequest {
            input_asset,
            output_asset,
            input_decimals,
            output_decimals,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: Some(recipient_address.to_string()),
            recipient_address: recipient_address.to_string(),
        })
        .await?
        .ok_or_else(|| format!("{} returned no exchange quote", provider.id()))
}

fn route_success(name: &str, amount_in: &str, final_amount_out: &str, steps: Vec<Value>) -> Value {
    json!({
        "name": name,
        "available": true,
        "amount_in_raw": amount_in,
        "amount_in": format_units_str(amount_in, 6),
        "final_amount_out_raw": final_amount_out,
        "final_amount_out": format_units_str(final_amount_out, 8),
        "effective_usdc_per_ubtc": effective_usdc_per_btc(amount_in, final_amount_out),
        "steps": steps,
    })
}

fn route_unavailable(name: &str, error: String) -> Value {
    json!({
        "name": name,
        "available": false,
        "error": error,
    })
}

fn bridge_step_summary(
    name: &str,
    quote: &BridgeQuote,
    source: &DepositAsset,
    destination: &DepositAsset,
    input_decimals: usize,
    output_decimals: usize,
) -> Value {
    json!({
        "name": name,
        "kind": "bridge",
        "provider_id": quote.provider_id,
        "source": asset_summary(source, Some(input_decimals)),
        "destination": asset_summary(destination, Some(output_decimals)),
        "amount_in_raw": quote.amount_in,
        "amount_in": format_units_str(&quote.amount_in, input_decimals),
        "amount_out_raw": quote.amount_out,
        "amount_out": format_units_str(&quote.amount_out, output_decimals),
        "provider_quote_kind": quote.provider_quote.get("kind").and_then(Value::as_str),
        "expires_at": quote.expires_at.to_rfc3339(),
    })
}

fn exchange_step_summary(
    name: &str,
    quote: &ExchangeQuote,
    input: &DepositAsset,
    output: &DepositAsset,
    input_decimals: usize,
    output_decimals: usize,
) -> Value {
    json!({
        "name": name,
        "kind": "exchange",
        "provider_id": quote.provider_id,
        "input": asset_summary(input, Some(input_decimals)),
        "output": asset_summary(output, Some(output_decimals)),
        "amount_in_raw": quote.amount_in,
        "amount_in": format_units_str(&quote.amount_in, input_decimals),
        "amount_out_raw": quote.amount_out,
        "amount_out": format_units_str(&quote.amount_out, output_decimals),
        "provider_quote_kind": quote.provider_quote.get("kind").and_then(Value::as_str),
        "expires_at": quote.expires_at.to_rfc3339(),
    })
}

fn passthrough_step_summary(
    name: &str,
    provider_id: &str,
    input: &DepositAsset,
    output: &DepositAsset,
    amount: &str,
    input_decimals: usize,
    output_decimals: usize,
) -> Value {
    json!({
        "name": name,
        "kind": "unit_passthrough",
        "provider_id": provider_id,
        "input": asset_summary(input, Some(input_decimals)),
        "output": asset_summary(output, Some(output_decimals)),
        "amount_in_raw": amount,
        "amount_in": format_units_str(amount, input_decimals),
        "amount_out_raw": amount,
        "amount_out": format_units_str(amount, output_decimals),
    })
}

fn market_quote_summary(name: &str, quote: &MarketOrderQuote) -> Value {
    let output_decimals = asset_display_decimals(&quote.destination_asset).unwrap_or(0);
    json!({
        "name": name,
        "available": true,
        "quote_id": quote.id.to_string(),
        "provider_id": quote.provider_id,
        "source": asset_summary(&quote.source_asset, asset_display_decimals(&quote.source_asset)),
        "destination": asset_summary(&quote.destination_asset, Some(output_decimals)),
        "amount_in_raw": quote.amount_in,
        "amount_in": asset_display_decimals(&quote.source_asset)
            .map(|decimals| format_units_str(&quote.amount_in, decimals))
            .unwrap_or_else(|| quote.amount_in.clone()),
        "amount_out_raw": quote.amount_out,
        "amount_out": format_units_str(&quote.amount_out, output_decimals),
        "effective_usdc_per_ubtc": effective_usdc_per_btc(&quote.amount_in, &quote.amount_out),
        "path_id": quote.provider_quote.get("path_id").and_then(Value::as_str),
        "transitions": transition_summaries(&quote.provider_quote),
    })
}

fn transition_summaries(provider_quote: &Value) -> Value {
    let Some(transitions) = provider_quote.get("transitions").and_then(Value::as_array) else {
        return json!([]);
    };
    Value::Array(
        transitions
            .iter()
            .map(|transition| {
                json!({
                    "kind": transition.get("kind").and_then(Value::as_str),
                    "provider": transition.get("provider").and_then(Value::as_str),
                    "amount_in": transition.get("amount_in"),
                    "amount_out": transition.get("amount_out"),
                })
            })
            .collect(),
    )
}

fn best_route_by_final_amount(first: &Value, second: &Value) -> Value {
    let first_amount = final_amount_out_raw(first);
    let second_amount = final_amount_out_raw(second);
    match (first_amount, second_amount) {
        (Some(first_amount), Some(second_amount)) if first_amount >= second_amount => json!({
            "selected": first.get("name").and_then(Value::as_str),
            "reason": "highest_final_ubtc_output",
            "route": first,
        }),
        (Some(_), Some(_)) => json!({
            "selected": second.get("name").and_then(Value::as_str),
            "reason": "highest_final_ubtc_output",
            "route": second,
        }),
        (Some(_), None) => json!({
            "selected": first.get("name").and_then(Value::as_str),
            "reason": "only_available_route",
            "route": first,
        }),
        (None, Some(_)) => json!({
            "selected": second.get("name").and_then(Value::as_str),
            "reason": "only_available_route",
            "route": second,
        }),
        (None, None) => json!({
            "selected": Value::Null,
            "reason": "no_available_route",
        }),
    }
}

fn final_amount_out_raw(route: &Value) -> Option<u128> {
    if route.get("available").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    route
        .get("final_amount_out_raw")
        .and_then(Value::as_str)
        .and_then(|raw| raw.parse::<u128>().ok())
}

fn asset_summary(asset: &DepositAsset, decimals: Option<usize>) -> Value {
    json!({
        "chain": asset.chain.as_str(),
        "asset": asset.asset.as_str(),
        "decimals": decimals,
    })
}

fn asset_display_decimals(asset: &DepositAsset) -> Option<usize> {
    match (asset.chain.as_str(), &asset.asset) {
        ("bitcoin", AssetId::Native) => Some(8),
        ("hyperliquid", AssetId::Native) => Some(6),
        ("hyperliquid", AssetId::Reference(symbol)) if symbol == "UBTC" => Some(8),
        ("hyperliquid", AssetId::Reference(symbol)) if symbol == "UETH" => Some(18),
        (_, AssetId::Native) if asset.chain.evm_chain_id().is_some() => Some(18),
        (_, AssetId::Reference(address)) if address.eq_ignore_ascii_case(BASE_USDC) => Some(6),
        (_, AssetId::Reference(address)) if address.eq_ignore_ascii_case(ARBITRUM_USDC) => Some(6),
        (_, AssetId::Reference(address)) if address.eq_ignore_ascii_case(ETHEREUM_USDC) => Some(6),
        _ => None,
    }
}

fn effective_usdc_per_btc(amount_in_usdc_raw: &str, amount_out_ubtc_raw: &str) -> Option<String> {
    let amount_in = amount_in_usdc_raw.parse::<f64>().ok()? / 1_000_000.0;
    let amount_out = amount_out_ubtc_raw.parse::<f64>().ok()? / 100_000_000.0;
    if amount_out <= 0.0 {
        return None;
    }
    Some(format!("{:.2}", amount_in / amount_out))
}

fn format_units_str(raw: &str, decimals: usize) -> String {
    if decimals == 0 {
        return raw.to_string();
    }
    let raw = raw.trim();
    if raw.is_empty() || !raw.chars().all(|ch| ch.is_ascii_digit()) {
        return raw.to_string();
    }
    let padded = if raw.len() <= decimals {
        format!("{raw:0>width$}", width = decimals + 1)
    } else {
        raw.to_string()
    };
    let split_at = padded.len() - decimals;
    let (whole, fraction) = padded.split_at(split_at);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
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
    let cctp_api_url = env_var_any(&["ROUTER_LIVE_CCTP_API_URL", "CCTP_API_URL"])
        .unwrap_or_else(|| DEFAULT_CCTP_API_URL.to_string());
    let velora_api_url = env_var_any(&[
        "ROUTER_LIVE_VELORA_API_URL",
        "VELORA_LIVE_BASE_URL",
        "VELORA_API_URL",
    ])
    .unwrap_or_else(|| DEFAULT_VELORA_API_URL.to_string());
    let velora_partner = env_var_any(&["ROUTER_LIVE_VELORA_PARTNER", "VELORA_LIVE_PARTNER"]);

    ActionProviderRegistry::http(
        Some(
            AcrossHttpProviderConfig::new(across_api_url, across_api_key)
                .with_integrator_id(Some(across_integrator_id)),
        ),
        Some(CctpHttpProviderConfig::new(cctp_api_url)),
        Some(hyperunit_api_url),
        hyperunit_proxy_url,
        Some(hyperliquid_api_url),
        Some(VeloraHttpProviderConfig::new(velora_api_url).with_partner(velora_partner)),
        HyperliquidCallNetwork::Mainnet,
    )
    .map_err(|error| format!("build live action provider registry: {error}").into())
}

async fn live_quote_database() -> TestResult<(Database, Option<ContainerAsync<GenericImage>>)> {
    if let Ok(database_url) = env::var(ROUTER_TEST_DATABASE_URL_ENV) {
        let db = Database::connect(&database_url, 5, 1).await?;
        return Ok((db, None));
    }

    let image = GenericImage::new("postgres", "18-alpine")
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
                ETHEREUM_USDC,
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
                ARBITRUM_USDC,
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
                BASE_USDC,
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

fn assert_any_transition_kind(provider_quote: &Value, expected_kinds: &[&str]) {
    let transitions = provider_quote["transitions"]
        .as_array()
        .expect("quote should serialize transition declarations");
    assert!(
        expected_kinds.iter().any(|expected| {
            transitions
                .iter()
                .any(|transition| transition["kind"] == *expected)
        }),
        "quote should contain one of {expected_kinds:?}; transitions={transitions:?}"
    );
}

fn assert_market_quote_amount_format(quote: &MarketOrderQuote) {
    assert_raw_amount_string("market quote amount_in", &quote.amount_in);
    assert_raw_amount_string("market quote amount_out", &quote.amount_out);
    if let Some(min_amount_out) = quote.min_amount_out.as_deref() {
        assert_raw_amount_string("market quote min_amount_out", min_amount_out);
    }
    if let Some(max_amount_in) = quote.max_amount_in.as_deref() {
        assert_raw_amount_string("market quote max_amount_in", max_amount_in);
    }
    assert_provider_quote_leg_amounts_are_raw(&quote.provider_quote);
}

fn assert_exchange_quote_amount_format(quote: &ExchangeQuote) {
    assert_raw_amount_string("exchange quote amount_in", &quote.amount_in);
    assert_raw_amount_string("exchange quote amount_out", &quote.amount_out);
    if let Some(min_amount_out) = quote.min_amount_out.as_deref() {
        assert_raw_amount_string("exchange quote min_amount_out", min_amount_out);
    }
    if let Some(max_amount_in) = quote.max_amount_in.as_deref() {
        assert_raw_amount_string("exchange quote max_amount_in", max_amount_in);
    }
    if quote.provider_quote.get("legs").is_some() {
        assert_provider_quote_leg_amounts_are_raw(&quote.provider_quote);
    }
}
