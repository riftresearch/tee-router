use alloy::{
    primitives::Address, providers::ProviderBuilder, signers::local::PrivateKeySigner, sol,
};
use router_server::{
    models::MarketOrderKind,
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            AcrossHttpProviderConfig, ActionProviderRegistry, BridgeProvider, BridgeQuoteRequest,
            ExchangeProvider, ExchangeQuoteRequest,
        },
        custody_action_executor::HyperliquidCallNetwork,
    },
};
use serde_json::json;
use std::{env, error::Error, str::FromStr};

const BASE_USDC: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ARBITRUM_USDC: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const UNIT_BTC_MINIMUM_SATS: u64 = 30_000;
const HYPERLIQUID_BRIDGE_MINIMUM_USDC: u64 = 5_000_000;
const MAX_SEARCH_USDC_RAW: u64 = 1_000_000_000;
const DEFAULT_BTC_RECIPIENT: &str = "bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn";

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
    }
}

#[derive(Debug, Clone)]
struct ProbeResult {
    amount_in_raw: u64,
    across_amount_out_raw: Option<u64>,
    btc_amount_out_sats: Option<u64>,
    viable: bool,
    reason: String,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let source_private_key =
        env_var_any(&["ROUTER_LIVE_SOURCE_PRIVATE_KEY", "LIVE_TEST_PRIVATE_KEY"])
            .ok_or("missing ROUTER_LIVE_SOURCE_PRIVATE_KEY or LIVE_TEST_PRIVATE_KEY")?;
    let across_api_key = env_var_required("ACROSS_API_KEY")?;
    let across_integrator_id = env_var_required("ACROSS_INTEGRATOR_ID")?;
    let across_api_url =
        env::var("ACROSS_API_URL").unwrap_or_else(|_| "https://app.across.to/api".to_string());
    let hyperunit_api_url =
        env::var("HYPERUNIT_API_URL").unwrap_or_else(|_| "https://api.hyperunit.xyz".to_string());
    let hyperunit_proxy_url = env::var("HYPERUNIT_PROXY_URL").ok();
    let hyperliquid_api_url = env::var("HYPERLIQUID_API_URL")
        .unwrap_or_else(|_| "https://api.hyperliquid.xyz".to_string());
    let base_rpc_url = env_var_required("BASE_RPC_URL")?;
    let recipient_address = env_var_any(&[
        "ROUTER_LIVE_BTC_RECIPIENT_ADDRESS",
        "ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS",
    ])
    .unwrap_or_else(|| DEFAULT_BTC_RECIPIENT.to_string());

    let signer = source_private_key.parse::<PrivateKeySigner>()?;
    let source_address = signer.address();

    let registry = ActionProviderRegistry::http(
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
    .map_err(|err| format!("build live provider registry: {err}"))?;

    let across = registry
        .bridge("across")
        .ok_or("Across bridge provider not configured")?;
    let hyperliquid_bridge = registry
        .bridge("hyperliquid_bridge")
        .ok_or("Hyperliquid bridge provider not configured")?;
    let hyperliquid = registry
        .exchange("hyperliquid")
        .ok_or("Hyperliquid exchange provider not configured")?;

    let base_usdc_balance_raw = read_base_usdc_balance(&base_rpc_url, &source_private_key).await?;

    let min_probe = find_minimum_viable_input(
        across.as_ref(),
        hyperliquid_bridge.as_ref(),
        hyperliquid.as_ref(),
        source_address,
        &recipient_address,
    )
    .await?;

    let fits_wallet = min_probe.amount_in_raw <= base_usdc_balance_raw;
    let suggested_amount_raw = min_probe.amount_in_raw;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "source_address": format!("{source_address:#x}"),
            "base_usdc_wallet_balance_raw": base_usdc_balance_raw.to_string(),
            "base_usdc_wallet_balance": format_usdc(base_usdc_balance_raw),
            "hyperliquid_bridge_minimum_usdc_raw": HYPERLIQUID_BRIDGE_MINIMUM_USDC.to_string(),
            "hyperliquid_bridge_minimum_usdc": format_usdc(HYPERLIQUID_BRIDGE_MINIMUM_USDC),
            "unit_btc_minimum_sats": UNIT_BTC_MINIMUM_SATS.to_string(),
            "unit_btc_minimum_btc": format_btc(UNIT_BTC_MINIMUM_SATS),
            "minimum_viable_input_raw": suggested_amount_raw.to_string(),
            "minimum_viable_input_usdc": format_usdc(suggested_amount_raw),
            "minimum_route_across_output_raw": min_probe.across_amount_out_raw.map(|v| v.to_string()),
            "minimum_route_across_output_usdc": min_probe.across_amount_out_raw.map(format_usdc),
            "minimum_route_btc_output_sats": min_probe.btc_amount_out_sats.map(|v| v.to_string()),
            "minimum_route_btc_output": min_probe.btc_amount_out_sats.map(format_btc),
            "minimum_probe_reason": min_probe.reason,
            "fits_current_wallet": fits_wallet,
            "shortfall_raw": (!fits_wallet)
                .then_some(suggested_amount_raw.saturating_sub(base_usdc_balance_raw))
                .map(|v| v.to_string()),
            "shortfall_usdc": (!fits_wallet)
                .then_some(suggested_amount_raw.saturating_sub(base_usdc_balance_raw))
                .map(format_usdc),
        }))?
    );

    Ok(())
}

async fn find_minimum_viable_input(
    across: &dyn BridgeProvider,
    hyperliquid_bridge: &dyn BridgeProvider,
    hyperliquid: &dyn ExchangeProvider,
    source_address: Address,
    recipient_address: &str,
) -> CliResult<ProbeResult> {
    let mut lower = HYPERLIQUID_BRIDGE_MINIMUM_USDC;
    let mut lower_probe = probe_route(
        across,
        hyperliquid_bridge,
        hyperliquid,
        source_address,
        recipient_address,
        lower,
    )
    .await?;

    if lower_probe.viable {
        return Ok(lower_probe);
    }

    let mut upper = lower;
    let upper_probe = loop {
        upper = upper.saturating_mul(2);
        if upper > MAX_SEARCH_USDC_RAW {
            return Err(format!(
                "no viable Base USDC -> BTC route found up to {} USDC; last_probe={lower_probe:?}",
                format_usdc(MAX_SEARCH_USDC_RAW)
            )
            .into());
        }
        let probe = probe_route(
            across,
            hyperliquid_bridge,
            hyperliquid,
            source_address,
            recipient_address,
            upper,
        )
        .await?;
        if probe.viable {
            break probe;
        }
        lower = upper;
        lower_probe = probe;
    };

    let mut best = upper_probe.clone();
    let mut lo = lower.saturating_add(1);
    let mut hi = upper;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let probe = probe_route(
            across,
            hyperliquid_bridge,
            hyperliquid,
            source_address,
            recipient_address,
            mid,
        )
        .await?;
        if probe.viable {
            best = probe;
            if mid == 0 {
                break;
            }
            hi = mid.saturating_sub(1);
        } else {
            lo = mid.saturating_add(1);
        }
    }

    Ok(best)
}

async fn probe_route(
    across: &dyn BridgeProvider,
    hyperliquid_bridge: &dyn BridgeProvider,
    hyperliquid: &dyn ExchangeProvider,
    source_address: Address,
    recipient_address: &str,
    amount_in_raw: u64,
) -> CliResult<ProbeResult> {
    let source_asset = deposit_asset("evm:8453", AssetId::Reference(BASE_USDC.to_string()));
    let unit_ingress_asset =
        deposit_asset("evm:42161", AssetId::Reference(ARBITRUM_USDC.to_string()));
    let exchange_input_asset = deposit_asset("hyperliquid", AssetId::Native);
    let destination_asset = deposit_asset("bitcoin", AssetId::Native);
    let amount_in = amount_in_raw.to_string();

    let Some(across_quote) = across
        .quote_bridge(BridgeQuoteRequest {
            source_asset: source_asset.clone(),
            destination_asset: unit_ingress_asset.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
            recipient_address: format!("{source_address:#x}"),
            depositor_address: format!("{source_address:#x}"),
            partial_fills_enabled: false,
        })
        .await
        .map_err(|err| format!("Across quote failed for {amount_in_raw}: {err}"))?
    else {
        return Ok(ProbeResult {
            amount_in_raw,
            across_amount_out_raw: None,
            btc_amount_out_sats: None,
            viable: false,
            reason: "across_quote_unavailable".to_string(),
        });
    };

    let across_amount_out_raw = across_quote.amount_out.parse::<u64>().map_err(|err| {
        format!(
            "Across amount_out {} was not a u64 for probe {amount_in_raw}: {err}",
            across_quote.amount_out
        )
    })?;

    let Some(bridge_quote) = hyperliquid_bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: unit_ingress_asset,
            destination_asset: exchange_input_asset.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: across_quote.amount_out.clone(),
                min_amount_out: across_quote.amount_out.clone(),
            },
            recipient_address: format!("{source_address:#x}"),
            depositor_address: format!("{source_address:#x}"),
            partial_fills_enabled: false,
        })
        .await
        .map_err(|err| format!("Hyperliquid bridge quote failed for {amount_in_raw}: {err}"))?
    else {
        return Ok(ProbeResult {
            amount_in_raw,
            across_amount_out_raw: Some(across_amount_out_raw),
            btc_amount_out_sats: None,
            viable: false,
            reason: "hyperliquid_bridge_minimum_not_met".to_string(),
        });
    };

    let Some(exchange_quote) = hyperliquid
        .quote_trade(ExchangeQuoteRequest {
            input_asset: exchange_input_asset,
            output_asset: destination_asset,
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: bridge_quote.amount_out.clone(),
                min_amount_out: "1".to_string(),
            },
            sender_address: None,
            recipient_address: recipient_address.to_string(),
        })
        .await
        .map_err(|err| format!("Hyperliquid quote failed for {amount_in_raw}: {err}"))?
    else {
        return Ok(ProbeResult {
            amount_in_raw,
            across_amount_out_raw: Some(across_amount_out_raw),
            btc_amount_out_sats: None,
            viable: false,
            reason: "hyperliquid_exchange_quote_unavailable".to_string(),
        });
    };

    let btc_amount_out_sats = exchange_quote.amount_out.parse::<u64>().map_err(|err| {
        format!(
            "Hyperliquid amount_out {} was not a u64 for probe {amount_in_raw}: {err}",
            exchange_quote.amount_out
        )
    })?;
    let viable = btc_amount_out_sats >= UNIT_BTC_MINIMUM_SATS;
    let reason = if viable {
        "viable".to_string()
    } else {
        "unit_btc_minimum_not_met".to_string()
    };

    Ok(ProbeResult {
        amount_in_raw,
        across_amount_out_raw: Some(across_amount_out_raw),
        btc_amount_out_sats: Some(btc_amount_out_sats),
        viable,
        reason,
    })
}

async fn read_base_usdc_balance(rpc_url: &str, private_key: &str) -> CliResult<u64> {
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let owner = signer.address();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse()?);
    let balance = IERC20::new(Address::from_str(BASE_USDC)?, provider)
        .balanceOf(owner)
        .call()
        .await?;
    Ok(balance.to::<u64>())
}

fn deposit_asset(chain: &str, asset: AssetId) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid static chain"),
        asset,
    }
}

fn env_var_required(key: &str) -> CliResult<String> {
    env::var(key)
        .map(|value| value.trim().to_string())
        .map_err(|_| format!("missing required env var {key}").into())
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn format_usdc(raw: u64) -> String {
    format_units(raw, 6)
}

fn format_btc(raw: u64) -> String {
    format_units(raw, 8)
}

fn format_units(raw: u64, decimals: usize) -> String {
    if decimals == 0 {
        return raw.to_string();
    }
    let raw_str = raw.to_string();
    let padded = if raw_str.len() <= decimals {
        format!("{:0>width$}", raw_str, width = decimals + 1)
    } else {
        raw_str
    };
    let split_at = padded.len() - decimals;
    let (whole, frac) = padded.split_at(split_at);
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{frac}")
    }
}
