use std::{env, error::Error};

use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use dotenvy::dotenv;
use hyperliquid_client::{client::Network, HyperliquidClient, Limit, Order, OrderRequest, Tif};
use serde_json::{json, Value};

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const HYPERLIQUID_PRIVATE_KEY: &str = "HYPERLIQUID_LIVE_PRIVATE_KEY";

#[derive(Parser)]
#[command(about = "Sell a live Hyperliquid spot balance using a marketable IOC limit order")]
struct Args {
    #[arg(
        long,
        env = "HYPERLIQUID_LIVE_BASE_URL",
        default_value = "https://api.hyperliquid.xyz"
    )]
    hyperliquid_base_url: String,

    #[arg(long, env = "HYPERLIQUID_LIVE_NETWORK", default_value = "mainnet")]
    hyperliquid_network: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long, default_value = "UBTC/USDC")]
    pair: String,

    #[arg(
        long,
        help = "Base asset amount in Hyperliquid natural units, e.g. 0.00075"
    )]
    sz: String,

    #[arg(long, default_value = "0.99")]
    limit_multiplier: f64,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    let private_key = args
        .private_key
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .or_else(|| env::var(HYPERLIQUID_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing private key: pass --private-key or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let wallet = private_key.parse::<PrivateKeySigner>()?;
    let user = wallet.address();
    let network = match args.hyperliquid_network.to_ascii_lowercase().as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => return Err(format!("unsupported Hyperliquid network {other:?}").into()),
    };
    let mut client = HyperliquidClient::new(&args.hyperliquid_base_url, wallet, None, network)?;
    let meta = client.refresh_spot_meta().await?;
    let asset = client.asset_index(&args.pair)?;
    let base = meta
        .base_token_for(&args.pair)
        .ok_or_else(|| format!("pair {} missing base metadata", args.pair))?;
    let base_decimals = base.sz_decimals as usize;
    let sz = floor_decimal(&args.sz, base_decimals)?;
    if sz == "0" {
        return Err(format!(
            "sell size {} floors to zero at {} decimals",
            args.sz, base_decimals
        )
        .into());
    }

    let book = client.l2_book(&args.pair).await?;
    let best_bid_level = book
        .best_bid()
        .ok_or_else(|| format!("pair {} has no bid levels", args.pair))?;
    let best_bid = best_bid_level.px.parse::<f64>()?;
    let price_decimals = visible_decimal_places(&best_bid_level.px);
    let limit_px = format_decimal_floor(best_bid * args.limit_multiplier, price_decimals);
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let response = client
        .place_orders(
            vec![OrderRequest {
                asset,
                is_buy: false,
                limit_px: limit_px.clone(),
                sz: sz.clone(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: Tif::Ioc.as_wire().to_string(),
                }),
                cloid: None,
            }],
            "na",
        )
        .await?;
    if let Some(error) = hyperliquid_response_error(&response) {
        return Err(format!("Hyperliquid returned order error: {error}").into());
    }
    let after_spot = client.spot_clearinghouse_state(user).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "user": format!("{user:#x}"),
            "pair": args.pair,
            "asset": asset,
            "is_buy": false,
            "limit_px": limit_px,
            "sz": sz,
            "before_spot": before_spot,
            "exchange_response": response,
            "after_spot": after_spot,
        }))?
    );

    Ok(())
}

fn hyperliquid_response_error(response: &Value) -> Option<String> {
    if response
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("err"))
    {
        return Some(
            response
                .get("response")
                .map(Value::to_string)
                .unwrap_or_else(|| response.to_string()),
        );
    }
    response
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .and_then(|statuses| {
            statuses
                .iter()
                .find_map(|status| status.get("error").map(Value::to_string))
        })
}

fn floor_decimal(raw: &str, decimals: usize) -> CliResult<String> {
    let mut parts = raw.trim().split('.');
    let whole = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !frac.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!("invalid decimal amount {raw:?}").into());
    }
    let frac = if frac.len() > decimals {
        &frac[..decimals]
    } else {
        frac
    };
    let formatted = if decimals == 0 || frac.is_empty() {
        whole.trim_start_matches('0').to_string()
    } else {
        format!(
            "{}.{}",
            whole.trim_start_matches('0'),
            frac.trim_end_matches('0')
        )
    };
    Ok(if formatted.is_empty() || formatted == "." {
        "0".to_string()
    } else if formatted.starts_with('.') {
        format!("0{formatted}")
    } else {
        formatted
    })
}

fn visible_decimal_places(raw: &str) -> usize {
    raw.split('.')
        .nth(1)
        .map(|frac| frac.trim_end_matches('0').len())
        .unwrap_or(0)
}

fn format_decimal_floor(value: f64, decimals: usize) -> String {
    let factor = 10_f64.powi(decimals as i32);
    let floored = (value * factor).floor() / factor;
    trim_decimal_string(&format!("{floored:.*}", decimals))
}

fn trim_decimal_string(raw: &str) -> String {
    let trimmed = raw.trim_end_matches('0').trim_end_matches('.');
    if trimmed.is_empty() {
        "0".to_string()
    } else {
        trimmed.to_string()
    }
}
