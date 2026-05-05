use std::{error::Error, future::Future, str::FromStr, time::Duration};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, Bytes, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
    sol_types::SolCall,
};
use clap::Parser;
use serde_json::json;
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function allowance(address owner, address spender) external view returns (uint256);
        function approve(address spender, uint256 amount) external returns (bool);
    }
}

#[derive(Parser)]
#[command(about = "Read or set ERC20 allowance for the signer account")]
struct Args {
    #[arg(long, env = "ERC20_ALLOWANCE_RPC_URL")]
    rpc_url: String,

    #[arg(long, env = "ERC20_ALLOWANCE_PRIVATE_KEY")]
    private_key: String,

    #[arg(long, env = "ERC20_ALLOWANCE_TOKEN")]
    token: String,

    #[arg(long, env = "ERC20_ALLOWANCE_SPENDER")]
    spender: String,

    #[arg(long, env = "ERC20_ALLOWANCE_AMOUNT", default_value = "0")]
    amount: String,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let args = Args::parse();
    let signer = args.private_key.parse::<PrivateKeySigner>()?;
    let owner = signer.address();
    let wallet = EthereumWallet::new(signer);
    let rpc_url: Url = args.rpc_url.parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url.clone());
    let wallet_provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);

    let token = Address::from_str(&args.token)?;
    let spender = Address::from_str(&args.spender)?;
    let amount = U256::from_str_radix(&args.amount, 10)?;
    let chain_id = retry_rpc("get_chain_id", || async {
        provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    let allowance_before = read_allowance(&provider, token, owner, spender).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "before",
            "chain_id": chain_id,
            "owner": format!("{owner:#x}"),
            "token": format!("{token:#x}"),
            "spender": format!("{spender:#x}"),
            "allowance": allowance_before.to_string(),
            "target_allowance": amount.to_string(),
        }))?
    );

    if allowance_before == amount {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "phase": "noop",
                "message": "allowance already matches target",
            }))?
        );
        return Ok(());
    }

    let nonce = retry_rpc("get_transaction_count", || async {
        provider
            .get_transaction_count(owner)
            .await
            .map_err(box_error)
    })
    .await?;
    let approve_data = Bytes::from(IERC20::approveCall { spender, amount }.abi_encode());
    let estimate_request = TransactionRequest::default()
        .with_from(owner)
        .with_to(token)
        .with_input(approve_data.clone());
    let gas_limit = retry_rpc("estimate_gas", || async {
        provider
            .estimate_gas(estimate_request.clone())
            .await
            .map_err(box_error)
    })
    .await?;
    let fee_estimate = retry_rpc("estimate_eip1559_fees", || async {
        provider.estimate_eip1559_fees().await.map_err(box_error)
    })
    .await?;
    let transaction_request = TransactionRequest::default()
        .with_from(owner)
        .with_to(token)
        .with_input(approve_data)
        .with_chain_id(chain_id)
        .with_nonce(nonce)
        .with_gas_limit(gas_limit)
        .with_max_fee_per_gas(with_fee_headroom(fee_estimate.max_fee_per_gas))
        .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);
    let pending = retry_rpc("send approve transaction", || async {
        wallet_provider
            .send_transaction(transaction_request.clone())
            .await
            .map_err(box_error)
    })
    .await?;
    let tx_hash = *pending.tx_hash();
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "submitted",
            "tx_hash": tx_hash.to_string(),
        }))?
    );

    wait_for_successful_receipt(&wallet_provider, tx_hash, "erc20 approval").await?;
    let allowance_after = read_allowance(&provider, token, owner, spender).await?;
    if allowance_after != amount {
        return Err(format!(
            "allowance post-check failed: expected {}, got {}",
            amount, allowance_after
        )
        .into());
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "after",
            "allowance": allowance_after.to_string(),
        }))?
    );

    Ok(())
}

fn with_fee_headroom(max_fee_per_gas: u128) -> u128 {
    max_fee_per_gas.saturating_mul(11).div_ceil(10)
}

async fn read_allowance<P>(
    provider: &P,
    token: Address,
    owner: Address,
    spender: Address,
) -> CliResult<U256>
where
    P: Provider + Clone,
{
    retry_rpc("erc20 allowance", || async {
        IERC20::new(token, provider.clone())
            .allowance(owner, spender)
            .call()
            .await
            .map_err(box_error)
    })
    .await
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

async fn wait_for_successful_receipt<P>(provider: &P, tx_hash: B256, label: &str) -> CliResult<()>
where
    P: Provider,
{
    for _ in 0..RECEIPT_POLL_ATTEMPTS {
        let receipt = retry_rpc(&format!("{label} receipt"), || async {
            provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(box_error)
        })
        .await?;

        if let Some(receipt) = receipt {
            if !receipt.status() {
                return Err(format!("{label} transaction {tx_hash} reverted").into());
            }
            return Ok(());
        }

        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }

    Err(format!("{label} transaction {tx_hash} was not mined before timeout").into())
}

fn is_retryable_rpc_error(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(err) = current {
        let message = err.to_string().to_ascii_lowercase();
        if message.contains("429")
            || message.contains("rate limit")
            || message.contains("rate-limited")
            || message.contains("too many requests")
            || message.contains("timeout")
            || message.contains("timed out")
            || message.contains("temporarily unavailable")
            || message.contains("connection reset")
            || message.contains("connection closed")
            || message.contains("connection refused")
            || message.contains("502")
            || message.contains("503")
            || message.contains("504")
        {
            return true;
        }
        current = err.source();
    }
    false
}
