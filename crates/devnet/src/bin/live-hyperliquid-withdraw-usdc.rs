use std::{env, error::Error, future::Future, str::FromStr, time::Duration};

use alloy::{
    primitives::{address, Address, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use clap::Parser;
use dotenvy::dotenv;
use hyperliquid_client::{client::Network, HyperliquidClient};
use serde_json::{json, Value};
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const HYPERLIQUID_PRIVATE_KEY: &str = "HYPERLIQUID_LIVE_PRIVATE_KEY";
const DEFAULT_ARBITRUM_USDC: Address = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC: f64 = 1.0;
const RPC_RETRY_ATTEMPTS: usize = 6;
const BALANCE_POLL_ATTEMPTS: usize = 120;
const BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(5);

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

#[derive(Debug, Parser)]
#[command(about = "Withdraw live Hyperliquid USDC to an arbitrary Arbitrum address")]
struct Args {
    #[arg(
        long,
        env = "HYPERLIQUID_LIVE_BASE_URL",
        default_value = "https://api.hyperliquid.xyz"
    )]
    hyperliquid_base_url: String,

    #[arg(long, env = "HYPERLIQUID_LIVE_NETWORK", default_value = "mainnet")]
    hyperliquid_network: String,

    #[arg(long, env = "ARBITRUM_RPC_URL")]
    arbitrum_rpc_url: String,

    #[arg(long, default_value_t = DEFAULT_ARBITRUM_USDC)]
    arbitrum_usdc: Address,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long)]
    destination: Address,

    #[arg(
        long,
        help = "Total USDC debited on Hyperliquid, before the 1 USDC bridge fee"
    )]
    amount: String,
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
    let client = HyperliquidClient::new(&args.hyperliquid_base_url, wallet, None, network)?;
    let provider = ProviderBuilder::new().connect_http(args.arbitrum_rpc_url.parse::<Url>()?);

    let amount = args.amount.parse::<f64>()?;
    if amount <= HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC {
        return Err(format!(
            "withdraw amount must exceed the {HYPERLIQUID_BRIDGE_WITHDRAW_FEE_USDC} USDC bridge fee"
        )
        .into());
    }
    let amount_wire = format_decimal_ceil(amount, 6);
    let amount_raw = usdc_decimal_to_raw(&amount_wire)?;
    let expected_credit_raw = amount_raw - U256::from(1_000_000u64);

    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_destination_balance =
        read_erc20_balance(&provider, args.arbitrum_usdc, args.destination).await?;

    let funding = ensure_withdrawable_usdc_balance(&client, user, amount).await?;
    let funded_clearinghouse = client.clearinghouse_state(user).await?;
    let funded_withdrawable = parse_decimal_balance(&funded_clearinghouse.withdrawable);
    let response = client
        .withdraw_to_bridge(
            format!("{:#x}", args.destination),
            amount_wire.clone(),
            current_time_ms(),
        )
        .await?;
    let after_clearinghouse =
        poll_withdrawable_balance_at_most(&client, user, funded_withdrawable - amount).await?;
    let after_spot = client.spot_clearinghouse_state(user).await?;
    let after_destination_balance = poll_erc20_balance(
        &provider,
        args.arbitrum_usdc,
        args.destination,
        before_destination_balance + expected_credit_raw,
    )
    .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "user": format!("{user:#x}"),
            "destination": format!("{:#x}", args.destination),
            "amount": amount_wire,
            "expected_destination_credit_raw": expected_credit_raw.to_string(),
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
            "funding": funding,
            "funded_clearinghouse": funded_clearinghouse,
            "exchange_response": response,
            "after_spot": after_spot,
            "after_clearinghouse": after_clearinghouse,
            "before_destination_balance_raw": before_destination_balance.to_string(),
            "after_destination_balance_raw": after_destination_balance.to_string(),
            "destination_delta_raw": (after_destination_balance - before_destination_balance).to_string(),
        }))?
    );

    Ok(())
}

async fn ensure_withdrawable_usdc_balance(
    client: &HyperliquidClient,
    user: Address,
    minimum_expected_total: f64,
) -> CliResult<Value> {
    let before_spot = client.spot_clearinghouse_state(user).await?;
    let before_spot_total = parse_spot_balance(&before_spot, "USDC");
    let before_clearinghouse = client.clearinghouse_state(user).await?;
    let before_withdrawable = parse_decimal_balance(&before_clearinghouse.withdrawable);
    if before_withdrawable + 1e-9 >= minimum_expected_total {
        return Ok(json!({
            "transfer_needed": false,
            "before_spot": before_spot,
            "before_clearinghouse": before_clearinghouse,
        }));
    }

    let needed = minimum_expected_total - before_withdrawable;
    if before_spot_total + 1e-9 < needed {
        return Err(format!(
            "insufficient Hyperliquid USDC: need {needed}, have spot {before_spot_total} and withdrawable {before_withdrawable}"
        )
        .into());
    }

    let amount = format_decimal_ceil(needed, 6);
    let response = client
        .usd_class_transfer(amount.clone(), true, current_time_ms())
        .await?;
    let after_clearinghouse =
        poll_withdrawable_balance(client, user, minimum_expected_total).await?;
    let after_spot = client.spot_clearinghouse_state(user).await?;

    Ok(json!({
        "transfer_needed": true,
        "transfer_amount": amount,
        "before_spot": before_spot,
        "before_clearinghouse": before_clearinghouse,
        "exchange_response": response,
        "after_spot": after_spot,
        "after_clearinghouse": after_clearinghouse,
    }))
}

async fn poll_withdrawable_balance(
    client: &HyperliquidClient,
    user: Address,
    minimum_expected_total: f64,
) -> CliResult<Value> {
    let mut last = None;
    for _ in 0..BALANCE_POLL_ATTEMPTS {
        let state = client.clearinghouse_state(user).await?;
        let withdrawable = parse_decimal_balance(&state.withdrawable);
        let value = serde_json::to_value(&state)?;
        last = Some(value);
        if withdrawable + 1e-9 >= minimum_expected_total {
            return Ok(last.expect("set above"));
        }
        tokio::time::sleep(BALANCE_POLL_INTERVAL).await;
    }
    Err(format!("timed out waiting for withdrawable balance; last={last:#?}").into())
}

async fn poll_withdrawable_balance_at_most(
    client: &HyperliquidClient,
    user: Address,
    maximum_expected_total: f64,
) -> CliResult<Value> {
    let mut last = None;
    for _ in 0..BALANCE_POLL_ATTEMPTS {
        let state = client.clearinghouse_state(user).await?;
        let withdrawable = parse_decimal_balance(&state.withdrawable);
        let value = serde_json::to_value(&state)?;
        last = Some(value);
        if withdrawable <= maximum_expected_total + 1e-9 {
            return Ok(last.expect("set above"));
        }
        tokio::time::sleep(BALANCE_POLL_INTERVAL).await;
    }
    Err(format!("timed out waiting for withdrawable balance to fall; last={last:#?}").into())
}

async fn read_erc20_balance<P>(provider: &P, token: Address, owner: Address) -> CliResult<U256>
where
    P: Provider + Clone,
{
    retry_rpc("erc20 balanceOf", || async {
        IERC20::new(token, provider.clone())
            .balanceOf(owner)
            .call()
            .await
            .map_err(box_error)
    })
    .await
}

async fn poll_erc20_balance<P>(
    provider: &P,
    token: Address,
    owner: Address,
    minimum: U256,
) -> CliResult<U256>
where
    P: Provider + Clone,
{
    let mut last = U256::ZERO;
    for _ in 0..BALANCE_POLL_ATTEMPTS {
        last = read_erc20_balance(provider, token, owner).await?;
        if last >= minimum {
            return Ok(last);
        }
        tokio::time::sleep(BALANCE_POLL_INTERVAL).await;
    }
    Err(format!("timed out waiting for Arbitrum USDC balance; last={last}").into())
}

fn parse_spot_balance(state: &hyperliquid_client::info::SpotClearinghouseState, coin: &str) -> f64 {
    parse_decimal_balance(state.balance_of(coin))
}

fn parse_decimal_balance(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or(0.0)
}

fn format_decimal_ceil(value: f64, decimals: usize) -> String {
    let factor = 10_f64.powi(decimals as i32);
    let ceiled = (value * factor).ceil() / factor;
    trim_decimal_string(&format!("{ceiled:.*}", decimals))
}

fn trim_decimal_string(raw: &str) -> String {
    raw.trim_end_matches('0').trim_end_matches('.').to_string()
}

fn usdc_decimal_to_raw(raw: &str) -> CliResult<U256> {
    let mut parts = raw.split('.');
    let whole = parts.next().unwrap_or("0");
    let frac = parts.next().unwrap_or("");
    if parts.next().is_some() || whole.is_empty() || whole.starts_with('-') {
        return Err(format!("invalid USDC decimal amount {raw:?}").into());
    }
    let frac = format!("{frac:0<6}");
    let frac = frac
        .get(..6)
        .ok_or_else(|| format!("invalid USDC fraction {raw:?}"))?;
    Ok(U256::from_str(whole)? * U256::from(1_000_000u64) + U256::from_str(frac)?)
}

fn current_time_ms() -> u64 {
    chrono::Utc::now().timestamp_millis().max(0) as u64
}

async fn retry_rpc<T, Fut, F>(label: &str, mut op: F) -> CliResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = CliResult<T>>,
{
    let mut last = None;
    for attempt in 1..=RPC_RETRY_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                last = Some(err.to_string());
                if attempt == RPC_RETRY_ATTEMPTS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
            }
        }
    }
    Err(format!("{label} failed after {RPC_RETRY_ATTEMPTS} attempts: {last:?}").into())
}

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}
