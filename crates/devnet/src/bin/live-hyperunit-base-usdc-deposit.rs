use std::{
    env,
    error::Error,
    future::Future,
    time::{Duration, Instant},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{address, Address, Bytes, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
    sol_types::SolCall,
};
use clap::Parser;
use dotenvy::dotenv;
use hyperliquid_client::{client::Network, HyperliquidClient};
use hyperunit_client::{
    UnitGenerateAddressResponse, UnitOperation, UnitOperationState, UnitOperationsResponse,
};
use reqwest::Proxy;
use serde_json::{json, Value};
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const DEFAULT_HYPERUNIT_API_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const BASE_USDC: Address = address!("833589fCD6eDb6E08f4c7C32D4f71b54bDA02913");

const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

#[derive(Debug, Parser)]
#[command(
    about = "Send Base USDC to a live HyperUnit deposit address and verify the Hyperliquid credit"
)]
struct Args {
    #[arg(long, env = "BASE_RPC_URL")]
    base_rpc_url: String,

    #[arg(long, env = "HYPERUNIT_LIVE_BASE_URL", default_value = DEFAULT_HYPERUNIT_API_URL)]
    hyperunit_base_url: String,

    #[arg(long, env = "HYPERUNIT_LIVE_PROXY_URL")]
    hyperunit_proxy_url: Option<String>,

    #[arg(long, env = "HYPERLIQUID_LIVE_BASE_URL", default_value = DEFAULT_HYPERLIQUID_API_URL)]
    hyperliquid_base_url: String,

    #[arg(long, env = "HYPERLIQUID_LIVE_NETWORK", default_value = "mainnet")]
    hyperliquid_network: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long, default_value = "1.0")]
    amount_usdc: String,

    #[arg(long, default_value_t = 15)]
    poll_interval_secs: u64,

    #[arg(long, default_value_t = 1800)]
    timeout_secs: u64,

    #[arg(long, help = "Actually submit the Base USDC transfer")]
    execute: bool,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    let private_key = args
        .private_key
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing live private key: pass --private-key or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let amount_raw = parse_usdc_amount(&args.amount_usdc)?;
    let poll_interval = Duration::from_secs(args.poll_interval_secs);
    let timeout = Duration::from_secs(args.timeout_secs);

    let base_rpc_url: Url = args.base_rpc_url.parse()?;
    let read_provider = ProviderBuilder::new().connect_http(base_rpc_url.clone());
    let write_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer.clone()))
        .connect_http(base_rpc_url);
    let unit_http = build_hyperunit_http(args.hyperunit_proxy_url.clone())?;
    let unit_base_url = args.hyperunit_base_url.trim_end_matches('/').to_string();
    let hl_network = parse_hyperliquid_network(&args.hyperliquid_network)?;
    let hl_client = HyperliquidClient::new(&args.hyperliquid_base_url, signer, None, hl_network)?;

    let chain_id = retry_rpc("get_chain_id", || async {
        read_provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    let base_usdc_before = read_erc20_balance(&read_provider, BASE_USDC, user).await?;
    if base_usdc_before < amount_raw {
        return Err(format!(
            "insufficient Base USDC: need {}, have {}",
            amount_raw, base_usdc_before
        )
        .into());
    }

    let spot_before = hl_client.spot_clearinghouse_state(user).await?;
    let clearing_before = hl_client.clearinghouse_state(user).await?;
    let spot_usdc_before = parse_hl_balance(&spot_before, "USDC");
    let withdrawable_before = parse_decimal_balance(&clearing_before.withdrawable);

    let generate = hyperunit_generate_address(
        &unit_http,
        &unit_base_url,
        "base",
        "hyperliquid",
        "usdc",
        &format!("{user:#x}"),
    )
    .await?;
    let status = generate.status.as_deref().unwrap_or("OK");
    if !status.eq_ignore_ascii_case("ok") {
        return Err(format!(
            "HyperUnit /gen returned non-OK status for base/usdc -> hyperliquid: {status}"
        )
        .into());
    }

    let seen_fingerprints = hyperunit_seen_fingerprints(
        &unit_http,
        &unit_base_url,
        &format!("{user:#x}"),
        &generate.address,
    )
    .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "prepared",
            "base_chain_id": chain_id,
            "user": format!("{user:#x}"),
            "amount_usdc": args.amount_usdc,
            "amount_raw": amount_raw.to_string(),
            "base_usdc_before": {
                "raw": base_usdc_before.to_string(),
                "formatted": format_units(base_usdc_before, 6),
            },
            "hyperliquid_before": {
                "spot_usdc": spot_usdc_before,
                "withdrawable_usdc": withdrawable_before,
                "spot_state": spot_before,
                "clearinghouse_state": clearing_before,
            },
            "hyperunit_generate_address": generate,
            "execute": args.execute,
        }))?
    );

    if !args.execute {
        return Ok(());
    }

    let tx_hash = send_usdc_transfer(
        &read_provider,
        &write_provider,
        user,
        generate.address.parse()?,
        amount_raw,
    )
    .await?;
    let base_usdc_after_send = read_erc20_balance(&read_provider, BASE_USDC, user).await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "submitted",
            "tx_hash": format!("{tx_hash:#x}"),
            "base_usdc_after_send": {
                "raw": base_usdc_after_send.to_string(),
                "formatted": format_units(base_usdc_after_send, 6),
            }
        }))?
    );

    let operation = wait_for_unit_done_operation(
        &unit_http,
        &unit_base_url,
        &format!("{user:#x}"),
        &generate.address,
        &seen_fingerprints,
        poll_interval,
        timeout,
    )
    .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "unit_done",
            "operation": operation,
        }))?
    );

    let hyperliquid_credit = wait_for_hyperliquid_credit(
        &hl_client,
        user,
        spot_usdc_before,
        withdrawable_before,
        poll_interval,
        timeout,
    )
    .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "hyperliquid_credit",
            "credit": hyperliquid_credit,
        }))?
    );

    Ok(())
}

fn parse_hyperliquid_network(raw: &str) -> CliResult<Network> {
    match raw.to_ascii_lowercase().as_str() {
        "mainnet" => Ok(Network::Mainnet),
        "testnet" => Ok(Network::Testnet),
        other => Err(format!("unsupported HYPERLIQUID_LIVE_NETWORK={other}").into()),
    }
}

fn build_hyperunit_http(proxy_url: Option<String>) -> CliResult<reqwest::Client> {
    let mut builder = reqwest::Client::builder().use_rustls_tls();
    if let Some(proxy_url) = proxy_url
        .map(|raw| raw.trim().to_string())
        .filter(|raw| !raw.is_empty())
    {
        let parsed = Url::parse(&proxy_url)?;
        if parsed.scheme() != "socks5" {
            return Err(format!(
                "unsupported HyperUnit proxy scheme {}; expected socks5",
                parsed.scheme()
            )
            .into());
        }
        builder = builder.proxy(Proxy::all(&proxy_url)?);
    }
    Ok(builder.build()?)
}

async fn hyperunit_generate_address(
    http: &reqwest::Client,
    base_url: &str,
    src_chain: &str,
    dst_chain: &str,
    asset: &str,
    dst_addr: &str,
) -> CliResult<UnitGenerateAddressResponse> {
    let url = format!(
        "{base_url}/gen/{}/{}/{}/{}",
        clean_path_segment(src_chain),
        clean_path_segment(dst_chain),
        clean_path_segment(asset),
        clean_path_segment(dst_addr)
    );
    get_json(http, &url).await
}

async fn hyperunit_operations(
    http: &reqwest::Client,
    base_url: &str,
    address: &str,
) -> CliResult<UnitOperationsResponse> {
    let url = format!("{base_url}/operations/{}", clean_path_segment(address));
    get_json(http, &url).await
}

async fn hyperunit_seen_fingerprints(
    http: &reqwest::Client,
    base_url: &str,
    query_address: &str,
    protocol_address: &str,
) -> CliResult<std::collections::BTreeSet<String>> {
    let response = hyperunit_operations(http, base_url, query_address).await?;
    Ok(response
        .operations
        .iter()
        .filter(|operation| operation.matches_protocol_address(protocol_address))
        .flat_map(UnitOperation::fingerprints)
        .collect())
}

async fn get_json<T: serde::de::DeserializeOwned>(
    http: &reqwest::Client,
    url: &str,
) -> CliResult<T> {
    let response = http.get(url).send().await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("HTTP {status} for {url}: {body}").into());
    }
    serde_json::from_str(&body)
        .map_err(|err| format!("invalid JSON from {url}: {err}; body={body}").into())
}

async fn send_usdc_transfer<PRead, PWrite>(
    read_provider: &PRead,
    write_provider: &PWrite,
    from: Address,
    to: Address,
    amount: U256,
) -> CliResult<B256>
where
    PRead: Provider,
    PWrite: Provider,
{
    let nonce = retry_rpc("get_transaction_count", || async {
        read_provider
            .get_transaction_count(from)
            .await
            .map_err(box_error)
    })
    .await?;
    let transfer_data = Bytes::from(IERC20::transferCall { to, amount }.abi_encode());
    let estimate_request = TransactionRequest::default()
        .with_from(from)
        .with_to(BASE_USDC)
        .with_input(transfer_data.clone());
    let gas_limit = retry_rpc("estimate_gas", || async {
        read_provider
            .estimate_gas(estimate_request.clone())
            .await
            .map_err(box_error)
    })
    .await?;
    let fee_estimate = retry_rpc("estimate_eip1559_fees", || async {
        read_provider
            .estimate_eip1559_fees()
            .await
            .map_err(box_error)
    })
    .await?;
    let transaction_request = TransactionRequest::default()
        .with_from(from)
        .with_to(BASE_USDC)
        .with_input(transfer_data)
        .with_nonce(nonce)
        .with_gas_limit(gas_limit)
        .with_max_fee_per_gas(with_fee_headroom(fee_estimate.max_fee_per_gas))
        .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);
    let pending = write_provider.send_transaction(transaction_request).await?;
    let tx_hash = *pending.tx_hash();
    wait_for_successful_receipt(write_provider, tx_hash, "Base USDC transfer").await?;
    Ok(tx_hash)
}

async fn wait_for_unit_done_operation(
    http: &reqwest::Client,
    base_url: &str,
    query_address: &str,
    protocol_address: &str,
    seen_fingerprints: &std::collections::BTreeSet<String>,
    poll_interval: Duration,
    timeout: Duration,
) -> CliResult<UnitOperation> {
    let start = Instant::now();
    let mut last_match = None;
    let mut last_state = None::<String>;
    loop {
        let response = hyperunit_operations(http, base_url, query_address).await?;
        let matching = response
            .operations
            .iter()
            .filter(|operation| operation.matches_protocol_address(protocol_address))
            .filter(|operation| !operation.has_seen_fingerprint(seen_fingerprints))
            .cloned()
            .collect::<Vec<_>>();

        if let Some(operation) = matching.last().cloned() {
            let state = operation
                .state
                .clone()
                .unwrap_or_else(|| "unknown".to_string());
            if last_state.as_deref() != Some(state.as_str()) {
                eprintln!("HyperUnit operation state -> {state}");
                last_state = Some(state);
            }
            match operation.classified_state() {
                UnitOperationState::Done => return Ok(operation),
                UnitOperationState::Failure => {
                    return Err(format!(
                        "HyperUnit operation failed: {}",
                        serde_json::to_string_pretty(&operation)?
                    )
                    .into())
                }
                _ => {
                    last_match = Some(operation);
                }
            }
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "timed out waiting for HyperUnit deposit completion; last={:#?}",
                last_match
            )
            .into());
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn wait_for_hyperliquid_credit(
    client: &HyperliquidClient,
    user: Address,
    spot_usdc_before: f64,
    withdrawable_before: f64,
    poll_interval: Duration,
    timeout: Duration,
) -> CliResult<Value> {
    let start = Instant::now();
    loop {
        let spot = client.spot_clearinghouse_state(user).await?;
        let clearing = client.clearinghouse_state(user).await?;
        let spot_usdc = parse_hl_balance(&spot, "USDC");
        let withdrawable = parse_decimal_balance(&clearing.withdrawable);
        let sample = json!({
            "spot_usdc_before": spot_usdc_before,
            "spot_usdc_after": spot_usdc,
            "withdrawable_before": withdrawable_before,
            "withdrawable_after": withdrawable,
            "spot_state": spot,
            "clearinghouse_state": clearing,
        });
        if spot_usdc > spot_usdc_before + 1e-9 {
            return Ok(json!({
                "credited_ledger": "spot",
                "sample": sample,
            }));
        }
        if withdrawable > withdrawable_before + 1e-9 {
            return Ok(json!({
                "credited_ledger": "withdrawable",
                "sample": sample,
            }));
        }
        if start.elapsed() >= timeout {
            return Err(
                format!("timed out waiting for Hyperliquid credit; last={sample:#?}").into(),
            );
        }
        tokio::time::sleep(poll_interval).await;
    }
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

fn parse_hl_balance(state: &hyperliquid_client::info::SpotClearinghouseState, coin: &str) -> f64 {
    parse_decimal_balance(state.balance_of(coin))
}

fn parse_decimal_balance(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or(0.0)
}

fn parse_usdc_amount(raw: &str) -> CliResult<U256> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err("USDC amount cannot be empty".into());
    }
    let mut parts = raw.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err(format!("invalid USDC amount {raw}: too many decimal points").into());
    }
    if !whole.chars().all(|c| c.is_ascii_digit()) || !frac.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("invalid USDC amount {raw}: expected decimal digits").into());
    }
    if frac.len() > 6 {
        return Err(format!("invalid USDC amount {raw}: more than 6 decimals").into());
    }

    let whole = if whole.is_empty() { "0" } else { whole };
    let scaled = format!("{whole}{:0<6}", frac);
    Ok(U256::from_str_radix(&scaled, 10)?)
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

fn clean_path_segment(raw: &str) -> String {
    raw.chars()
        .filter(|c| !c.is_whitespace() && *c != '/' && *c != '?' && *c != '#')
        .collect()
}

fn with_fee_headroom(max_fee_per_gas: u128) -> u128 {
    max_fee_per_gas.saturating_mul(11).div_ceil(10)
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
    for attempt in 1..=RPC_RETRY_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < RPC_RETRY_ATTEMPTS && is_retryable_rpc_error(err.as_ref()) => {
                eprintln!(
                    "retrying RPC {label} after attempt {attempt}/{RPC_RETRY_ATTEMPTS}: {err}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!("retry loop always returns before exhausting attempts")
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
            if receipt.status() {
                return Ok(());
            }
            return Err(format!("{label} transaction reverted: {tx_hash:#x}").into());
        }
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
    Err(format!("timed out waiting for {label} receipt: {tx_hash:#x}").into())
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
