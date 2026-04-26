use std::{
    collections::BTreeSet,
    env,
    error::Error,
    future::Future,
    str::FromStr,
    time::{Duration, Instant},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use clap::Parser;
use dotenvy::dotenv;
use hyperunit_client::{
    HyperUnitClient, UnitAsset, UnitChain, UnitGenerateAddressRequest, UnitOperation,
    UnitOperationState, UnitOperationsRequest,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const ETH_RPC_URL: &str = "ETH_RPC_URL";
const EVM_RPC_URL: &str = "EVM_RPC_URL";
const HYPERUNIT_API_URL: &str = "HYPERUNIT_API_URL";
const HYPERUNIT_PROXY_URL: &str = "HYPERUNIT_PROXY_URL";
const HYPERLIQUID_API_URL: &str = "HYPERLIQUID_API_URL";
const DEFAULT_HYPERUNIT_API_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_DEPOSIT_AMOUNT_WEI: &str = "10000000000000000";

const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Parser)]
#[command(
    about = "Probe whether a deterministic fresh Hyperliquid address can be initialized implicitly by a live HyperUnit ETH deposit"
)]
struct Args {
    #[arg(long, env = ETH_RPC_URL)]
    eth_rpc_url: Option<String>,

    #[arg(long, env = EVM_RPC_URL)]
    evm_rpc_url: Option<String>,

    #[arg(long, env = HYPERUNIT_API_URL, default_value = DEFAULT_HYPERUNIT_API_URL)]
    hyperunit_api_url: String,

    #[arg(long, env = HYPERUNIT_PROXY_URL)]
    hyperunit_proxy_url: Option<String>,

    #[arg(long, env = HYPERLIQUID_API_URL, default_value = DEFAULT_HYPERLIQUID_API_URL)]
    hyperliquid_api_url: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long, default_value = DEFAULT_DEPOSIT_AMOUNT_WEI)]
    amount_wei: String,

    #[arg(long, default_value_t = 15)]
    poll_interval_secs: u64,

    #[arg(long, default_value_t = 1800)]
    timeout_secs: u64,

    #[arg(long, help = "Actually submit the Ethereum transfer into HyperUnit")]
    execute: bool,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();

    let source_private_key = args
        .private_key
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing live private key: pass --private-key or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let source_signer = source_private_key.parse::<PrivateKeySigner>()?;
    let source_address = source_signer.address();
    let probe_signer = derive_probe_destination(&source_private_key)?;
    let probe_address = probe_signer.address();
    let probe_address_str = format!("{probe_address:#x}");
    let amount = U256::from_str_radix(&args.amount_wei, 10)?;
    let poll_interval = Duration::from_secs(args.poll_interval_secs);
    let timeout = Duration::from_secs(args.timeout_secs);

    let eth_rpc_url: Url = args
        .eth_rpc_url
        .or(args.evm_rpc_url)
        .ok_or_else(|| format!("missing {ETH_RPC_URL} or {EVM_RPC_URL}"))?
        .parse()?;
    let read_provider = ProviderBuilder::new().connect_http(eth_rpc_url.clone());
    let write_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(source_signer))
        .connect_http(eth_rpc_url.clone());

    let chain_id = retry_rpc("ethereum chain id", || async {
        read_provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    if chain_id != 1 {
        return Err(format!("selected Ethereum RPC is on chain {chain_id}, expected 1").into());
    }

    let source_balance = retry_rpc("ethereum balance", || async {
        read_provider
            .get_balance(source_address)
            .await
            .map_err(box_error)
    })
    .await?;
    if source_balance <= amount {
        return Err(format!(
            "insufficient ETH: source balance {} wei is not greater than probe amount {} wei",
            source_balance, amount
        )
        .into());
    }

    let unit = live_unit_client(&args.hyperunit_api_url, args.hyperunit_proxy_url.clone())?;
    let hl_http = reqwest::Client::builder().use_rustls_tls().build()?;

    let user_role_before = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"userRole","user": probe_address_str}),
    )
    .await?;
    let spot_before = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"spotClearinghouseState","user": probe_address_str}),
    )
    .await
    .unwrap_or_else(|err| json!({ "error": err.to_string() }));
    let clearing_before = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"clearinghouseState","user": probe_address_str}),
    )
    .await
    .unwrap_or_else(|err| json!({ "error": err.to_string() }));

    let generated = unit
        .generate_address(UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: probe_address_str.clone(),
        })
        .await?;
    let operations_before_by_protocol = unit
        .operations(UnitOperationsRequest {
            address: generated.address.clone(),
        })
        .await?;
    let operations_before_by_destination = unit
        .operations(UnitOperationsRequest {
            address: probe_address_str.clone(),
        })
        .await?;
    let seen_operation_fingerprints = observed_unit_operation_fingerprints(
        &operations_before_by_protocol.operations,
        &operations_before_by_destination.operations,
        &generated.address,
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "prepared",
            "ethereum_rpc_url": eth_rpc_url.as_str(),
            "source_address": format!("{source_address:#x}"),
            "probe_destination_address": probe_address_str,
            "amount_wei": amount.to_string(),
            "source_balance_before_wei": source_balance.to_string(),
            "hyperliquid_user_role_before": user_role_before,
            "hyperliquid_spot_before": spot_before,
            "hyperliquid_clearinghouse_before": clearing_before,
            "hyperunit_generate_address": generated,
            "execute": args.execute,
            "note": "probe destination is deterministically derived from the source private key with the tag rift:hyperunit:fresh-hl-dst:v1"
        }))?
    );

    if !args.execute {
        return Ok(());
    }

    let tx_hash = send_native(
        &read_provider,
        &write_provider,
        source_address,
        Address::from_str(&generated.address)?,
        amount,
    )
    .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "submitted",
            "tx_hash": format!("{tx_hash:#x}"),
        }))?
    );

    let operation = wait_for_unit_terminal_operation(
        &unit,
        &probe_address_str,
        &generated.address,
        &seen_operation_fingerprints,
        poll_interval,
        timeout,
    )
    .await?;

    let user_role_after = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"userRole","user": probe_address_str}),
    )
    .await?;
    let spot_after = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"spotClearinghouseState","user": probe_address_str}),
    )
    .await
    .unwrap_or_else(|err| json!({ "error": err.to_string() }));
    let clearing_after = hyperliquid_info(
        &hl_http,
        &args.hyperliquid_api_url,
        json!({"type":"clearinghouseState","user": probe_address_str}),
    )
    .await
    .unwrap_or_else(|err| json!({ "error": err.to_string() }));

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "phase": "finished",
            "terminal_unit_operation": operation,
            "hyperliquid_user_role_after": user_role_after,
            "hyperliquid_spot_after": spot_after,
            "hyperliquid_clearinghouse_after": clearing_after,
        }))?
    );

    Ok(())
}

fn derive_probe_destination(source_private_key: &str) -> CliResult<PrivateKeySigner> {
    for counter in 0u32..1024 {
        let mut hasher = Sha256::new();
        hasher.update(source_private_key.trim().as_bytes());
        hasher.update(b":rift:hyperunit:fresh-hl-dst:v1:");
        hasher.update(counter.to_le_bytes());
        let digest = hasher.finalize();
        let candidate = B256::from_slice(&digest);
        if candidate.is_zero() {
            continue;
        }
        if let Ok(signer) = PrivateKeySigner::from_bytes(&candidate) {
            return Ok(signer);
        }
    }
    Err("failed to derive a valid probe private key".into())
}

fn live_unit_client(base_url: &str, proxy_url: Option<String>) -> CliResult<HyperUnitClient> {
    HyperUnitClient::new_with_proxy_url(base_url, proxy_url).map_err(|err| err.into())
}

async fn hyperliquid_info(
    http: &reqwest::Client,
    base_url: &str,
    request: Value,
) -> CliResult<Value> {
    let base = base_url.trim_end_matches('/');
    let response = http
        .post(format!("{base}/info"))
        .json(&request)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("hyperliquid /info returned HTTP {status}: {body}").into());
    }
    serde_json::from_str(&body)
        .map_err(|err| format!("invalid hyperliquid /info JSON: {err}; body={body}").into())
}

async fn wait_for_unit_terminal_operation(
    unit: &HyperUnitClient,
    destination_address: &str,
    protocol_address: &str,
    seen_operation_fingerprints: &[String],
    poll_interval: Duration,
    timeout: Duration,
) -> CliResult<UnitOperation> {
    let start = Instant::now();
    let mut last_operations = Vec::<UnitOperation>::new();
    let seen_operation_fingerprints: BTreeSet<String> =
        seen_operation_fingerprints.iter().cloned().collect();

    while start.elapsed() < timeout {
        let by_protocol = unit
            .operations(UnitOperationsRequest {
                address: protocol_address.to_string(),
            })
            .await?;
        let by_destination = unit
            .operations(UnitOperationsRequest {
                address: destination_address.to_string(),
            })
            .await?;
        last_operations = by_protocol
            .operations
            .into_iter()
            .chain(by_destination.operations.into_iter())
            .filter(|operation| {
                operation.matches_protocol_address(protocol_address)
                    && !operation.has_seen_fingerprint(&seen_operation_fingerprints)
            })
            .collect();

        if let Some(operation) = last_operations.iter().find(|operation| {
            matches!(
                operation.classified_state(),
                UnitOperationState::Done | UnitOperationState::Failure
            )
        }) {
            return Ok(operation.clone());
        }

        tokio::time::sleep(poll_interval).await;
    }

    Err(format!(
        "timed out waiting for HyperUnit terminal operation; last_operations={:#?}",
        last_operations
    )
    .into())
}

fn observed_unit_operation_fingerprints(
    protocol_operations: &[UnitOperation],
    destination_operations: &[UnitOperation],
    protocol_address: &str,
) -> Vec<String> {
    protocol_operations
        .iter()
        .chain(destination_operations.iter())
        .filter(|operation| operation.matches_protocol_address(protocol_address))
        .flat_map(UnitOperation::fingerprints)
        .collect()
}

async fn send_native<P>(
    read_provider: &impl Provider,
    write_provider: &P,
    from: Address,
    destination: Address,
    amount: U256,
) -> CliResult<B256>
where
    P: Provider,
{
    let tx = TransactionRequest::default()
        .with_from(from)
        .with_to(destination)
        .with_value(amount);
    let pending = retry_rpc("eth send native", || async {
        write_provider
            .send_transaction(tx.clone())
            .await
            .map_err(box_error)
    })
    .await?;
    let tx_hash = *pending.tx_hash();
    wait_for_successful_receipt(read_provider, tx_hash, "ethereum source transfer").await?;
    Ok(tx_hash)
}

async fn wait_for_successful_receipt<P>(provider: &P, tx_hash: B256, label: &str) -> CliResult<()>
where
    P: Provider,
{
    for _ in 0..RECEIPT_POLL_ATTEMPTS {
        let receipt = retry_rpc("eth get transaction receipt", || async {
            provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(box_error)
        })
        .await?;
        if let Some(receipt) = receipt {
            let status = receipt.status();
            if status {
                return Ok(());
            }
            return Err(format!("{label} reverted: {receipt:?}").into());
        }
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
    Err(format!("timed out waiting for {label} receipt {tx_hash:#x}").into())
}

async fn retry_rpc<F, Fut, T>(label: &'static str, mut operation: F) -> CliResult<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = CliResult<T>>,
{
    let mut last_error = None;
    for attempt in 1..=RPC_RETRY_ATTEMPTS {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) => {
                last_error = Some(err.to_string());
                if attempt == RPC_RETRY_ATTEMPTS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
            }
        }
    }
    Err(format!(
        "{label} failed after {RPC_RETRY_ATTEMPTS} attempts: {}",
        last_error.unwrap_or_else(|| "unknown RPC error".to_string())
    )
    .into())
}

fn box_error<E>(error: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(error)
}
