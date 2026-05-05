use std::{env, error::Error, future::Future, str::FromStr, time::Duration};

use alloy::{
    primitives::{address, Address, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use clap::Parser;
use dotenvy::dotenv;
use serde_json::json;
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const BASE_USDC: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bDA02913");
const BASE_CBBTC: Address = address!("cbb7c0000ab88b473b1f5afd9ef808440eed33bf");
const BASE_WETH: Address = address!("4200000000000000000000000000000000000006");

const RPC_RETRY_ATTEMPTS: usize = 6;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

#[derive(Parser)]
#[command(about = "Read ETH, USDC, cbBTC, and WETH balances for a Base wallet")]
struct Args {
    #[arg(long, env = "BASE_RPC_URL")]
    rpc_url: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long)]
    address: Option<String>,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    let owner = resolve_owner(args.private_key.as_deref(), args.address.as_deref())?;
    let rpc_url: Url = args.rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    let chain_id = retry_rpc("get_chain_id", || async {
        provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    let native_balance = retry_rpc("get_balance", || async {
        provider.get_balance(owner).await.map_err(box_error)
    })
    .await?;
    let usdc_balance = read_erc20_balance(&provider, BASE_USDC, owner).await?;
    let cbbtc_balance = read_erc20_balance(&provider, BASE_CBBTC, owner).await?;
    let weth_balance = read_erc20_balance(&provider, BASE_WETH, owner).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "chain_id": chain_id,
            "address": format!("{owner:#x}"),
            "balances": {
                "ETH": {
                    "raw": native_balance.to_string(),
                    "formatted": format_units(native_balance, 18),
                },
                "USDC": {
                    "token": format!("{BASE_USDC:#x}"),
                    "raw": usdc_balance.to_string(),
                    "formatted": format_units(usdc_balance, 6),
                },
                "cbBTC": {
                    "token": format!("{BASE_CBBTC:#x}"),
                    "raw": cbbtc_balance.to_string(),
                    "formatted": format_units(cbbtc_balance, 8),
                },
                "WETH": {
                    "token": format!("{BASE_WETH:#x}"),
                    "raw": weth_balance.to_string(),
                    "formatted": format_units(weth_balance, 18),
                }
            }
        }))?
    );

    Ok(())
}

fn resolve_owner(private_key: Option<&str>, address: Option<&str>) -> CliResult<Address> {
    if let Some(address) = address {
        return Ok(Address::from_str(address)?);
    }
    let private_key = private_key
        .map(str::to_string)
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing wallet selector: pass --address, --private-key, or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let signer = private_key.parse::<PrivateKeySigner>()?;
    Ok(signer.address())
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

fn format_units(value: U256, decimals: usize) -> String {
    let raw = value.to_string();
    if decimals == 0 {
        return raw;
    }

    if raw.len() <= decimals {
        let mut formatted = format!("0.{}{}", "0".repeat(decimals - raw.len()), raw);
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
        return formatted;
    }

    let split = raw.len() - decimals;
    let mut formatted = format!("{}.{}", &raw[..split], &raw[split..]);
    while formatted.ends_with('0') {
        formatted.pop();
    }
    if formatted.ends_with('.') {
        formatted.pop();
    }
    formatted
}

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}

async fn retry_rpc<T, Fut, Op>(label: &str, mut op: Op) -> CliResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = CliResult<T>>,
{
    let mut delay = Duration::from_millis(500);
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < RPC_RETRY_ATTEMPTS && is_retryable_rpc_error(err.as_ref()) => {
                eprintln!(
                    "retrying RPC {label} after attempt {attempt}/{RPC_RETRY_ATTEMPTS}: {err}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn is_retryable_rpc_error(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        let text = err.to_string().to_ascii_lowercase();
        if text.contains("429")
            || text.contains("rate limit")
            || text.contains("timeout")
            || text.contains("temporar")
            || text.contains("connection reset")
            || text.contains("connection refused")
            || text.contains("broken pipe")
            || text.contains("eof")
            || text.contains("unavailable")
            || text.contains("overloaded")
            || text.contains("internal error")
            || text.contains("header not found")
        {
            return true;
        }
        current = err.source();
    }
    false
}
