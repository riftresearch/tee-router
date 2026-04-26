use std::{
    collections::BTreeSet,
    env,
    error::Error,
    fs,
    future::Future,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use hyperliquid_client::{
    client::Network as HyperliquidNetwork, HyperliquidExchangeClient, HyperliquidInfoClient,
    SpotClearinghouseState,
};
use hyperunit_client::{
    HyperUnitClient, UnitAsset, UnitChain, UnitGenerateAddressRequest, UnitOperation,
    UnitOperationState, UnitOperationsRequest, UnitOperationsResponse,
};
use serde_json::{json, Value};
use url::Url;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const HYPERUNIT_BASE_URL: &str = "HYPERUNIT_LIVE_BASE_URL";
const HYPERUNIT_PROXY_URL: &str = "HYPERUNIT_LIVE_PROXY_URL";
const HYPERUNIT_PRIVATE_KEY: &str = "HYPERUNIT_LIVE_PRIVATE_KEY";
const HYPERUNIT_ETHEREUM_RPC_URL: &str = "HYPERUNIT_LIVE_ETHEREUM_RPC_URL";
const HYPERUNIT_DEPOSIT_AMOUNT_WEI: &str = "HYPERUNIT_LIVE_ETH_DEPOSIT_AMOUNT_WEI";
const HYPERUNIT_WITHDRAW_AMOUNT_ETH: &str = "HYPERUNIT_LIVE_ETH_WITHDRAW_AMOUNT";
const HYPERUNIT_WITHDRAW_FUNDING_AMOUNT_WEI: &str =
    "HYPERUNIT_LIVE_ETH_WITHDRAW_FUNDING_AMOUNT_WEI";
const HYPERUNIT_SPEND_CONFIRMATION: &str = "HYPERUNIT_LIVE_I_UNDERSTAND_THIS_SENDS_REAL_FUNDS";
const HYPERLIQUID_BASE_URL: &str = "HYPERLIQUID_LIVE_BASE_URL";
const HYPERLIQUID_NETWORK: &str = "HYPERLIQUID_LIVE_NETWORK";
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";

const DEFAULT_HYPERUNIT_BASE_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_HYPERLIQUID_BASE_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_HYPERUNIT_DEPOSIT_AMOUNT_WEI: &str = "10000000000000000";
const DEFAULT_HYPERUNIT_WITHDRAW_AMOUNT_ETH: &str = "0.008";
const DEFAULT_HYPERUNIT_WITHDRAW_FUNDING_AMOUNT_WEI: &str = "12000000000000000";
const DEFAULT_UNIT_POLL_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_UNIT_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const DEFAULT_SPOT_BALANCE_POLL_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_SPOT_BALANCE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[tokio::test]
#[ignore = "Hits live HyperUnit through the configured proxy without spending funds"]
async fn live_hyperunit_generate_address_smoke() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let private_key = required_private_key()?;
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let unit = live_unit_client()?;

    let generated = unit
        .generate_address(UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: format!("{user:#x}"),
        })
        .await?;
    let operations = unit
        .operations(UnitOperationsRequest {
            address: generated.address.clone(),
        })
        .await?;

    emit_transcript(
        "hyperunit.generate_address_smoke",
        json!({
            "unit_base_url": unit.base_url(),
            "user": format!("{user:#x}"),
            "generate_response": generated,
            "operations_by_protocol_address": operations,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by depositing Ethereum ETH into Hyperliquid through live HyperUnit"]
async fn live_hyperunit_eth_deposit_to_hyperliquid_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERUNIT_SPEND_CONFIRMATION,
        "YES",
        "refusing to submit a live HyperUnit ETH deposit",
    )?;

    let private_key = required_private_key()?;
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let user_str = format!("{user:#x}");
    let unit = live_unit_client()?;
    let mut hl_info = live_hyperliquid_info_client()?;
    let hl_exchange = live_hyperliquid_exchange_client(signer.clone())?;
    let ethereum_rpc_url = select_ethereum_rpc_url().await?;
    let eth_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(ethereum_rpc_url.clone());
    let chain_id = retry_rpc("ethereum chain id", || async {
        eth_provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    if chain_id != 1 {
        return Err(format!("selected Ethereum RPC is on chain {chain_id}, expected 1").into());
    }

    hl_info.refresh_spot_meta().await?;
    let before_spot = hl_info.spot_clearinghouse_state(user).await?;
    let before_ueth = parse_spot_balance(&before_spot, "UETH");

    let generate_request = UnitGenerateAddressRequest {
        src_chain: UnitChain::Ethereum,
        dst_chain: UnitChain::Hyperliquid,
        asset: UnitAsset::Eth,
        dst_addr: user_str.clone(),
    };
    let generated = unit.generate_address(generate_request.clone()).await?;
    let operations_before_by_protocol = unit
        .operations(UnitOperationsRequest {
            address: generated.address.clone(),
        })
        .await?;
    let operations_before_by_destination = unit
        .operations(UnitOperationsRequest {
            address: user_str.clone(),
        })
        .await?;
    let seen_operation_fingerprints = observed_unit_operation_fingerprints(
        &operations_before_by_protocol,
        &operations_before_by_destination,
        &generated.address,
    );

    let amount_wei = env::var(HYPERUNIT_DEPOSIT_AMOUNT_WEI)
        .unwrap_or_else(|_| DEFAULT_HYPERUNIT_DEPOSIT_AMOUNT_WEI.to_string());
    let amount = U256::from_str_radix(&amount_wei, 10)?;
    let source_tx_hash = send_native(
        &eth_provider,
        Address::from_str(&generated.address)?,
        amount,
    )
    .await?;

    let (terminal_operation, lifecycle_samples) = wait_for_terminal_unit_operation(
        &unit,
        &user_str,
        &generated.address,
        "HyperUnit ETH deposit",
        &seen_operation_fingerprints,
    )
    .await?;

    if !unit_tx_hash_matches_submitted(
        terminal_operation.source_tx_hash.as_deref(),
        &source_tx_hash,
    ) {
        return Err(format!(
            "HyperUnit source tx mismatch: sent {}, observed {:?}",
            source_tx_hash, terminal_operation.source_tx_hash
        )
        .into());
    }

    let (after_spot, spot_balance_samples) =
        poll_spot_balance_increase(&hl_info, user, "UETH", before_ueth).await?;

    emit_transcript(
        "hyperunit.eth_deposit_to_hyperliquid",
        json!({
            "unit_base_url": unit.base_url(),
            "hyperliquid_base_url": hl_exchange.http().base_url(),
            "ethereum_rpc_url": ethereum_rpc_url.as_str(),
            "user": user_str,
            "generate_request": {
                "src_chain": generate_request.src_chain.as_wire_str(),
                "dst_chain": generate_request.dst_chain.as_wire_str(),
                "asset": generate_request.asset.as_wire_str(),
                "dst_addr": generate_request.dst_addr,
            },
            "generate_response": generated,
            "operations_before_source_tx": {
                "by_protocol_address": operations_before_by_protocol,
                "by_destination_address": operations_before_by_destination,
            },
            "source_transfer": {
                "amount_wei": amount_wei,
                "tx_hash": source_tx_hash,
            },
            "hyperliquid_spot_before": before_spot,
            "unit_lifecycle_samples": lifecycle_samples,
            "terminal_operation": terminal_operation,
            "hyperliquid_spot_balance_samples": spot_balance_samples,
            "hyperliquid_spot_after": after_spot,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by withdrawing Hyperliquid UETH back to Ethereum through live HyperUnit"]
async fn live_hyperunit_eth_withdraw_from_hyperliquid_to_ethereum_transcript_spends_funds(
) -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        HYPERUNIT_SPEND_CONFIRMATION,
        "YES",
        "refusing to submit a live HyperUnit ETH withdrawal",
    )?;

    let private_key = required_private_key()?;
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let user_str = format!("{user:#x}");
    let unit = live_unit_client()?;
    let mut hl_info = live_hyperliquid_info_client()?;
    let hl_exchange = live_hyperliquid_exchange_client(signer.clone())?;
    let ethereum_rpc_url = select_ethereum_rpc_url().await?;
    let eth_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(ethereum_rpc_url.clone());
    let chain_id = retry_rpc("ethereum chain id", || async {
        eth_provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    if chain_id != 1 {
        return Err(format!("selected Ethereum RPC is on chain {chain_id}, expected 1").into());
    }

    let spot_meta = hl_info.refresh_spot_meta().await?;
    let ueth_token_wire = hl_info.spot_token_wire("UETH")?;
    let withdraw_amount = env::var(HYPERUNIT_WITHDRAW_AMOUNT_ETH)
        .unwrap_or_else(|_| DEFAULT_HYPERUNIT_WITHDRAW_AMOUNT_ETH.to_string());
    let withdraw_amount_f64 = withdraw_amount.parse::<f64>()?;

    let funding = ensure_hyperliquid_ueth_balance(
        &unit,
        &hl_info,
        &eth_provider,
        user,
        &user_str,
        withdraw_amount_f64,
    )
    .await?;

    let before_spot = hl_info.spot_clearinghouse_state(user).await?;
    let before_ueth = parse_spot_balance(&before_spot, "UETH");
    if before_ueth + 1e-9 < withdraw_amount_f64 {
        return Err(format!(
            "insufficient Hyperliquid UETH balance after funding: have {before_ueth}, need {withdraw_amount_f64}"
        )
        .into());
    }

    let generate_request = UnitGenerateAddressRequest {
        src_chain: UnitChain::Hyperliquid,
        dst_chain: UnitChain::Ethereum,
        asset: UnitAsset::Eth,
        dst_addr: user_str.clone(),
    };
    let generated = unit.generate_address(generate_request.clone()).await?;
    let operations_before_by_protocol = unit
        .operations(UnitOperationsRequest {
            address: generated.address.clone(),
        })
        .await?;
    let operations_before_by_destination = unit
        .operations(UnitOperationsRequest {
            address: user_str.clone(),
        })
        .await?;
    let seen_operation_fingerprints = observed_unit_operation_fingerprints(
        &operations_before_by_protocol,
        &operations_before_by_destination,
        &generated.address,
    );

    let before_destination_receipt = operations_before_by_destination
        .operations
        .iter()
        .filter(|op| op.matches_protocol_address(&generated.address))
        .filter_map(|op| op.destination_tx_hash.clone())
        .collect::<Vec<_>>();

    let exchange_response = hl_exchange
        .spot_send(
            generated.address.clone(),
            ueth_token_wire.clone(),
            withdraw_amount.clone(),
            current_time_ms(),
        )
        .await?;

    let (terminal_operation, lifecycle_samples) = wait_for_terminal_unit_operation(
        &unit,
        &user_str,
        &generated.address,
        "HyperUnit ETH withdrawal",
        &seen_operation_fingerprints,
    )
    .await?;

    let (after_spot, spot_balance_samples) =
        poll_spot_balance_decrease(&hl_info, user, "UETH", before_ueth - withdraw_amount_f64)
            .await?;

    let destination_tx_hash = terminal_operation
        .destination_tx_hash
        .clone()
        .ok_or("HyperUnit withdrawal terminal operation is missing destination_tx_hash")?;
    wait_for_successful_receipt(
        &eth_provider,
        B256::from_str(&destination_tx_hash)?,
        "HyperUnit withdrawal destination transfer",
    )
    .await?;

    emit_transcript(
        "hyperunit.eth_withdraw_from_hyperliquid_to_ethereum",
        json!({
            "unit_base_url": unit.base_url(),
            "hyperliquid_base_url": hl_exchange.http().base_url(),
            "hyperliquid_network": format!("{:?}", hl_exchange.network()),
            "ethereum_rpc_url": ethereum_rpc_url.as_str(),
            "user": user_str,
            "funding": funding,
            "spot_meta_token_wire": {
                "symbol": "UETH",
                "wire": ueth_token_wire,
                "tokens_sample": spot_meta.tokens.iter().take(5).collect::<Vec<_>>(),
            },
            "generate_request": {
                "src_chain": generate_request.src_chain.as_wire_str(),
                "dst_chain": generate_request.dst_chain.as_wire_str(),
                "asset": generate_request.asset.as_wire_str(),
                "dst_addr": generate_request.dst_addr,
            },
            "generate_response": generated,
            "operations_before_spot_send": {
                "by_protocol_address": operations_before_by_protocol,
                "by_destination_address": operations_before_by_destination,
                "matching_destination_tx_hashes": before_destination_receipt,
            },
            "hyperliquid_spot_before": before_spot,
            "hyperliquid_exchange_response": exchange_response,
            "unit_lifecycle_samples": lifecycle_samples,
            "terminal_operation": terminal_operation,
            "hyperliquid_spot_balance_samples": spot_balance_samples,
            "hyperliquid_spot_after": after_spot,
            "ethereum_destination_receipt_tx_hash": destination_tx_hash,
        }),
    );

    Ok(())
}

async fn ensure_hyperliquid_ueth_balance<P>(
    unit: &HyperUnitClient,
    hl_info: &HyperliquidInfoClient,
    eth_provider: &P,
    user: Address,
    user_str: &str,
    minimum_ueth: f64,
) -> TestResult<Value>
where
    P: Provider,
{
    let before_spot = hl_info.spot_clearinghouse_state(user).await?;
    let before_ueth = parse_spot_balance(&before_spot, "UETH");
    if before_ueth + 1e-9 >= minimum_ueth {
        return Ok(json!({
            "kind": "already_funded",
            "hyperliquid_spot_before": before_spot,
        }));
    }

    let funding_amount_wei = env::var(HYPERUNIT_WITHDRAW_FUNDING_AMOUNT_WEI)
        .unwrap_or_else(|_| DEFAULT_HYPERUNIT_WITHDRAW_FUNDING_AMOUNT_WEI.to_string());
    let funding_amount = U256::from_str_radix(&funding_amount_wei, 10)?;
    let generate_request = UnitGenerateAddressRequest {
        src_chain: UnitChain::Ethereum,
        dst_chain: UnitChain::Hyperliquid,
        asset: UnitAsset::Eth,
        dst_addr: user_str.to_string(),
    };
    let generated = unit.generate_address(generate_request.clone()).await?;
    let source_tx_hash = send_native(
        eth_provider,
        Address::from_str(&generated.address)?,
        funding_amount,
    )
    .await?;
    let (terminal_operation, lifecycle_samples) = wait_for_terminal_unit_operation(
        unit,
        user_str,
        &generated.address,
        "HyperUnit ETH funding deposit",
        &BTreeSet::new(),
    )
    .await?;
    let (after_spot, spot_balance_samples) =
        poll_spot_balance_increase(hl_info, user, "UETH", before_ueth).await?;

    Ok(json!({
        "kind": "unit_funding_deposit",
        "generate_request": {
            "src_chain": generate_request.src_chain.as_wire_str(),
            "dst_chain": generate_request.dst_chain.as_wire_str(),
            "asset": generate_request.asset.as_wire_str(),
            "dst_addr": generate_request.dst_addr,
        },
        "generate_response": generated,
        "source_transfer": {
            "amount_wei": funding_amount_wei,
            "tx_hash": source_tx_hash,
        },
        "terminal_operation": terminal_operation,
        "unit_lifecycle_samples": lifecycle_samples,
        "hyperliquid_spot_before": before_spot,
        "hyperliquid_spot_balance_samples": spot_balance_samples,
        "hyperliquid_spot_after": after_spot,
    }))
}

async fn wait_for_terminal_unit_operation(
    client: &HyperUnitClient,
    destination_query: &str,
    protocol_query: &str,
    label: &str,
    seen_operations: &BTreeSet<String>,
) -> TestResult<(UnitOperation, Vec<Value>)> {
    let deadline = Instant::now() + DEFAULT_UNIT_TIMEOUT;
    let mut samples = Vec::new();
    loop {
        let protocol_response = client
            .operations(UnitOperationsRequest {
                address: protocol_query.to_string(),
            })
            .await?;
        let destination_response = client
            .operations(UnitOperationsRequest {
                address: destination_query.to_string(),
            })
            .await?;
        let protocol_matches = matching_operations(&protocol_response, protocol_query);
        let destination_matches = matching_operations(&destination_response, protocol_query);
        let matched = protocol_matches
            .iter()
            .find(|operation| !operation.has_seen_fingerprint(seen_operations))
            .cloned()
            .or_else(|| {
                destination_matches
                    .iter()
                    .find(|operation| !operation.has_seen_fingerprint(seen_operations))
                    .cloned()
            });

        samples.push(json!({
            "sample_index": samples.len(),
            "query": {
                "protocol_address": protocol_query,
                "destination_address": destination_query,
            },
            "operations_by_protocol_address": protocol_response,
            "operations_by_destination_address": destination_response,
            "matching_operations_by_protocol_address": protocol_matches,
            "matching_operations_by_destination_address": destination_matches,
        }));

        if let Some(operation) = matched {
            match operation.classified_state() {
                UnitOperationState::Done => return Ok((operation, samples)),
                UnitOperationState::Failure => {
                    return Err(format!("{label} failed: {operation:?}").into())
                }
                _ => {}
            }
        }

        if Instant::now() >= deadline {
            return Err(format!(
                "{label} timed out waiting for a terminal Unit operation on protocol address {protocol_query}"
            )
            .into());
        }
        tokio::time::sleep(DEFAULT_UNIT_POLL_INTERVAL).await;
    }
}

async fn poll_spot_balance_increase(
    client: &HyperliquidInfoClient,
    user: Address,
    coin: &str,
    before_balance: f64,
) -> TestResult<(SpotClearinghouseState, Vec<Value>)> {
    let deadline = Instant::now() + DEFAULT_SPOT_BALANCE_TIMEOUT;
    let mut samples = Vec::new();
    loop {
        let state = client.spot_clearinghouse_state(user).await?;
        let total = parse_spot_balance(&state, coin);
        samples.push(serde_json::to_value(&state)?);
        if total > before_balance + 1e-9 {
            return Ok((state, samples));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for Hyperliquid {coin} balance to increase above {before_balance}"
            )
            .into());
        }
        tokio::time::sleep(DEFAULT_SPOT_BALANCE_POLL_INTERVAL).await;
    }
}

async fn poll_spot_balance_decrease(
    client: &HyperliquidInfoClient,
    user: Address,
    coin: &str,
    target_max_balance: f64,
) -> TestResult<(SpotClearinghouseState, Vec<Value>)> {
    let deadline = Instant::now() + DEFAULT_SPOT_BALANCE_TIMEOUT;
    let mut samples = Vec::new();
    loop {
        let state = client.spot_clearinghouse_state(user).await?;
        let total = parse_spot_balance(&state, coin);
        samples.push(serde_json::to_value(&state)?);
        if total <= target_max_balance + 1e-9 {
            return Ok((state, samples));
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "timed out waiting for Hyperliquid {coin} balance to fall to at most {target_max_balance}"
            )
            .into());
        }
        tokio::time::sleep(DEFAULT_SPOT_BALANCE_POLL_INTERVAL).await;
    }
}

async fn send_native<P>(provider: &P, to: Address, amount: U256) -> TestResult<String>
where
    P: Provider,
{
    let tx = TransactionRequest::default().with_to(to).with_value(amount);
    let pending = provider.send_transaction(tx).await?;
    let tx_hash = *pending.tx_hash();
    eprintln!("submitted HyperUnit source transfer {tx_hash}");
    wait_for_successful_receipt(provider, tx_hash, "HyperUnit source transfer").await?;
    Ok(format!("{tx_hash:#x}"))
}

async fn retry_rpc<T, Fut, Op>(label: &str, mut op: Op) -> TestResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = TestResult<T>>,
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

async fn wait_for_successful_receipt<P>(provider: &P, tx_hash: B256, label: &str) -> TestResult<()>
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

async fn select_ethereum_rpc_url() -> TestResult<Url> {
    let mut candidates = Vec::new();
    if let Ok(value) = env::var(HYPERUNIT_ETHEREUM_RPC_URL) {
        let value = value.trim();
        if !value.is_empty() {
            candidates.push(value.to_string());
        }
    }
    if let Ok(value) = env::var("ETHEREUM_RPC_URL") {
        let value = value.trim();
        if !value.is_empty() {
            candidates.push(value.to_string());
        }
    }
    candidates.push("https://ethereum-rpc.publicnode.com".to_string());
    candidates.push("https://cloudflare-eth.com".to_string());

    let mut seen = BTreeSet::new();
    let mut errors = Vec::new();
    for candidate in candidates {
        if !seen.insert(candidate.clone()) {
            continue;
        }
        let Ok(url) = candidate.parse::<Url>() else {
            errors.push(format!("{candidate}: invalid URL"));
            continue;
        };
        let provider = ProviderBuilder::new().connect_http(url.clone());
        match provider.get_chain_id().await {
            Ok(1) => return Ok(url),
            Ok(chain_id) => errors.push(format!("{candidate}: unexpected chain id {chain_id}")),
            Err(err) => errors.push(format!("{candidate}: {err}")),
        }
    }

    Err(format!(
        "failed to find a usable Ethereum mainnet RPC; tried: {}",
        errors.join("; ")
    )
    .into())
}

fn matching_operations(
    response: &UnitOperationsResponse,
    protocol_address: &str,
) -> Vec<UnitOperation> {
    response
        .operations
        .iter()
        .filter(|operation| operation.matches_protocol_address(protocol_address))
        .cloned()
        .collect()
}

fn observed_unit_operation_fingerprints(
    protocol_response: &UnitOperationsResponse,
    destination_response: &UnitOperationsResponse,
    protocol_address: &str,
) -> BTreeSet<String> {
    protocol_response
        .operations
        .iter()
        .chain(destination_response.operations.iter())
        .filter(|operation| operation.matches_protocol_address(protocol_address))
        .flat_map(UnitOperation::fingerprints)
        .collect()
}

fn parse_spot_balance(state: &SpotClearinghouseState, coin: &str) -> f64 {
    state.balance_of(coin).parse::<f64>().unwrap_or_default()
}

fn unit_tx_hash_matches_submitted(observed: Option<&str>, submitted: &str) -> bool {
    let Some(observed) = observed.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };

    observed == submitted
        || observed
            .strip_prefix(submitted)
            .is_some_and(|suffix| suffix.starts_with(':'))
}

fn live_unit_client() -> TestResult<HyperUnitClient> {
    HyperUnitClient::new_with_proxy_url(
        env::var(HYPERUNIT_BASE_URL).unwrap_or_else(|_| DEFAULT_HYPERUNIT_BASE_URL.to_string()),
        env::var(HYPERUNIT_PROXY_URL).ok(),
    )
    .map_err(Into::into)
}

fn live_hyperliquid_info_client() -> TestResult<HyperliquidInfoClient> {
    let base_url =
        env::var(HYPERLIQUID_BASE_URL).unwrap_or_else(|_| DEFAULT_HYPERLIQUID_BASE_URL.to_string());
    Ok(HyperliquidInfoClient::new(&base_url)?)
}

fn live_hyperliquid_exchange_client(
    wallet: PrivateKeySigner,
) -> TestResult<HyperliquidExchangeClient> {
    let base_url =
        env::var(HYPERLIQUID_BASE_URL).unwrap_or_else(|_| DEFAULT_HYPERLIQUID_BASE_URL.to_string());
    Ok(HyperliquidExchangeClient::new(
        &base_url,
        wallet,
        None,
        live_hyperliquid_network()?,
    )?)
}

fn live_hyperliquid_network() -> TestResult<HyperliquidNetwork> {
    match env::var(HYPERLIQUID_NETWORK)
        .unwrap_or_else(|_| "mainnet".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "mainnet" => Ok(HyperliquidNetwork::Mainnet),
        "testnet" => Ok(HyperliquidNetwork::Testnet),
        other => Err(format!("unsupported {HYPERLIQUID_NETWORK}={other}").into()),
    }
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before unix epoch")
        .as_millis() as u64
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

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider test; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn private_key_env() -> Result<String, env::VarError> {
    env::var(LIVE_TEST_PRIVATE_KEY).or_else(|_| env::var(HYPERUNIT_PRIVATE_KEY))
}

fn required_private_key() -> TestResult<String> {
    private_key_env().map_err(|_| {
        format!(
            "missing required env var {} or {}",
            LIVE_TEST_PRIVATE_KEY, HYPERUNIT_PRIVATE_KEY
        )
        .into()
    })
}

fn required_env(key: &str) -> TestResult<String> {
    env::var(key).map_err(|_| format!("missing required env var {key}").into())
}

fn require_confirmation(key: &str, expected: &str, context: &str) -> TestResult<()> {
    let actual = required_env(key)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}; expected {key}={expected}, got {actual:?}").into())
    }
}

fn emit_transcript(label: &str, value: Value) {
    println!(
        "LIVE_PROVIDER_TRANSCRIPT {label}\n{}",
        serde_json::to_string_pretty(&value).expect("transcript serializes")
    );
    if let Err(error) = write_transcript_artifact(label, &value) {
        eprintln!("failed to write live provider transcript artifact: {error}");
    }
}

fn write_transcript_artifact(label: &str, value: &Value) -> TestResult<()> {
    let created_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before unix epoch: {error}"))?
        .as_millis();
    let dir = env::var(LIVE_RECOVERY_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/live-recovery")
        })
        .join("provider-transcripts");
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!(
        "{}-{}.json",
        created_at_ms,
        sanitize_artifact_label(label)
    ));
    let payload = json!({
        "schema_version": 1,
        "kind": "live_provider_transcript",
        "provider": "hyperunit",
        "label": label,
        "created_at_ms": created_at_ms,
        "private_key_env_candidates": [LIVE_TEST_PRIVATE_KEY, HYPERUNIT_PRIVATE_KEY],
        "transcript": value,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)?;
    eprintln!(
        "live provider transcript artifact written to {}",
        path.display()
    );
    Ok(())
}

fn sanitize_artifact_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
}
