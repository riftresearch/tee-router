use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, Bytes, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use router_server::services::across_client::{
    parse_optional_u256, AcrossClient, AcrossClientError, AcrossSwapApprovalRequest,
    AcrossSwapApprovalResponse, AcrossTransaction,
};
use serde_json::{json, Value};

mod support;
use support::wait_for_successful_receipt;

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const ACROSS_BASE_URL: &str = "ACROSS_LIVE_BASE_URL";
const ACROSS_API_KEY: &str = "ACROSS_LIVE_API_KEY";
const ACROSS_INTEGRATOR_ID: &str = "ACROSS_LIVE_INTEGRATOR_ID";
const ACROSS_ORIGIN_RPC_URL: &str = "ACROSS_LIVE_ORIGIN_RPC_URL";
const ACROSS_PRIVATE_KEY: &str = "ACROSS_LIVE_PRIVATE_KEY";
const ACROSS_TRADE_TYPE: &str = "ACROSS_LIVE_TRADE_TYPE";
const ACROSS_ORIGIN_CHAIN_ID: &str = "ACROSS_LIVE_ORIGIN_CHAIN_ID";
const ACROSS_DESTINATION_CHAIN_ID: &str = "ACROSS_LIVE_DESTINATION_CHAIN_ID";
const ACROSS_INPUT_TOKEN: &str = "ACROSS_LIVE_INPUT_TOKEN";
const ACROSS_OUTPUT_TOKEN: &str = "ACROSS_LIVE_OUTPUT_TOKEN";
const ACROSS_AMOUNT: &str = "ACROSS_LIVE_AMOUNT";
const ACROSS_RECIPIENT: &str = "ACROSS_LIVE_RECIPIENT";
const ACROSS_REFUND_ADDRESS: &str = "ACROSS_LIVE_REFUND_ADDRESS";
const ACROSS_SLIPPAGE: &str = "ACROSS_LIVE_SLIPPAGE";
const ACROSS_SPEND_CONFIRMATION: &str = "ACROSS_LIVE_I_UNDERSTAND_THIS_BRIDGES_REAL_FUNDS";
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";

#[tokio::test]
#[ignore = "live Across API observer test; run with LIVE_PROVIDER_TESTS=1"]
async fn live_across_swap_approval_transcript() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let client = live_client()?;
    let request = live_swap_request()?;
    let response = client.swap_approval(request.clone()).await?;
    let raw_response = fetch_raw_swap_approval(&client, &request).await?;

    emit_transcript(
        "across.swap_approval",
        json!({
            "base_url": client.base_url(),
            "request": request_summary(&request),
            "typed_response": response,
            "raw_response": raw_response,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by submitting Across approval/swap transactions"]
async fn live_across_swap_lifecycle_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        ACROSS_SPEND_CONFIRMATION,
        "YES",
        "refusing to submit live Across transactions",
    )?;

    let client = live_client()?;
    let request = live_swap_request()?;
    let response = client.swap_approval(request.clone()).await?;
    let tx_hashes = execute_across_transactions(&response, request.origin_chain_id).await?;
    let swap_tx_hash = tx_hashes
        .last()
        .ok_or("no Across swap transaction hash was submitted")?
        .clone();
    let status_samples = poll_deposit_status_by_tx_ref(&client, &swap_tx_hash).await?;

    emit_transcript(
        "across.swap.lifecycle",
        json!({
            "base_url": client.base_url(),
            "request": request_summary(&request),
            "swap_approval": response,
            "submitted_tx_hashes": tx_hashes,
            "deposit_txn_ref": swap_tx_hash,
            "deposit_status_samples": status_samples,
        }),
    );

    Ok(())
}

fn live_client() -> TestResult<AcrossClient> {
    Ok(AcrossClient::new(
        env::var(ACROSS_BASE_URL).unwrap_or_else(|_| "https://app.across.to/api".into()),
        required_env(ACROSS_API_KEY)?,
    )?)
}

fn live_swap_request() -> TestResult<AcrossSwapApprovalRequest> {
    let depositor = required_private_key()?
        .parse::<PrivateKeySigner>()?
        .address();
    let recipient = env::var(ACROSS_RECIPIENT).unwrap_or_else(|_| format!("{depositor:?}"));
    let refund_address = env::var(ACROSS_REFUND_ADDRESS)
        .ok()
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or(depositor);

    Ok(AcrossSwapApprovalRequest {
        trade_type: env::var(ACROSS_TRADE_TYPE).unwrap_or_else(|_| "exactInput".into()),
        origin_chain_id: required_env(ACROSS_ORIGIN_CHAIN_ID)?.parse()?,
        destination_chain_id: required_env(ACROSS_DESTINATION_CHAIN_ID)?.parse()?,
        input_token: Address::from_str(&required_env(ACROSS_INPUT_TOKEN)?)?,
        output_token: Address::from_str(&required_env(ACROSS_OUTPUT_TOKEN)?)?,
        amount: U256::from_str_radix(&required_env(ACROSS_AMOUNT)?, 10)?,
        depositor,
        recipient,
        refund_address,
        refund_on_origin: true,
        strict_trade_type: true,
        integrator_id: required_env(ACROSS_INTEGRATOR_ID)?,
        slippage: env::var(ACROSS_SLIPPAGE).ok(),
        extra_query: BTreeMap::new(),
    })
}

async fn execute_across_transactions(
    response: &AcrossSwapApprovalResponse,
    expected_chain_id: u64,
) -> TestResult<Vec<String>> {
    let signer = required_private_key()?.parse::<PrivateKeySigner>()?;
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(required_env(ACROSS_ORIGIN_RPC_URL)?.parse()?);

    let mut tx_hashes = Vec::new();
    for tx in response.approval_txns.as_deref().unwrap_or_default() {
        tx_hashes.push(send_provider_transaction(&provider, tx, expected_chain_id).await?);
    }
    let swap_tx = response
        .swap_tx
        .as_ref()
        .ok_or("Across swap approval did not include swapTx")?;
    tx_hashes.push(send_provider_transaction(&provider, swap_tx, expected_chain_id).await?);
    Ok(tx_hashes)
}

async fn send_provider_transaction<P>(
    provider: &P,
    tx: &AcrossTransaction,
    expected_chain_id: u64,
) -> TestResult<String>
where
    P: Provider,
{
    if tx.chain_id != Some(expected_chain_id) {
        return Err(format!(
            "Across tx chainId {:?} did not match expected {expected_chain_id}",
            tx.chain_id
        )
        .into());
    }

    let to = Address::from_str(&tx.to)?;
    let data = Bytes::from_str(&tx.data)?;
    let mut request = TransactionRequest::default()
        .with_to(to)
        .with_input(data)
        .with_value(parse_optional_u256("value", &tx.value)?.unwrap_or(U256::ZERO));
    if let Some(gas_limit) = parse_optional_u64("gas", &tx.gas)?.filter(|gas_limit| *gas_limit > 0)
    {
        request = request.with_gas_limit(gas_limit);
    }
    if let Some(max_fee_per_gas) = parse_optional_u128("maxFeePerGas", &tx.max_fee_per_gas)?
        .filter(|max_fee_per_gas| *max_fee_per_gas > 0)
    {
        request = request.with_max_fee_per_gas(max_fee_per_gas);
    }
    if let Some(max_priority_fee_per_gas) =
        parse_optional_u128("maxPriorityFeePerGas", &tx.max_priority_fee_per_gas)?
            .filter(|max_priority_fee_per_gas| *max_priority_fee_per_gas > 0)
    {
        request = request.with_max_priority_fee_per_gas(max_priority_fee_per_gas);
    }

    // Do not blindly retry transaction submission: if the RPC returns a
    // transport error after accepting the tx, resubmission can create duplicate
    // side effects. Persist the hash as soon as the provider gives us one and
    // retry the receipt polling instead.
    let pending = provider.send_transaction(request).await?;
    let tx_hash = *pending.tx_hash();
    eprintln!("submitted Across transaction {tx_hash}");
    wait_for_successful_receipt(provider, tx_hash, "Across").await?;
    Ok(tx_hash.to_string())
}

async fn poll_deposit_status_by_tx_ref(
    client: &AcrossClient,
    deposit_txn_ref: &str,
) -> TestResult<Vec<Value>> {
    let mut samples = Vec::new();
    for _ in 0..120 {
        match client.deposit_status_by_tx_ref(deposit_txn_ref).await {
            Ok(status) => {
                let terminal = status.status == "filled" || status.status == "expired";
                samples.push(serde_json::to_value(&status)?);
                if terminal {
                    return Ok(samples);
                }
            }
            Err(AcrossClientError::HttpStatus { status: 404, body })
                if is_across_deposit_not_found(&body) =>
            {
                samples.push(json!({
                    "status": "not_found",
                    "error": "DepositNotFoundException",
                    "body": parse_json_or_string(&body),
                }));
            }
            Err(err) => return Err(Box::new(err)),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Err(format!("timed out waiting for Across deposit tx {deposit_txn_ref} to fill").into())
}

fn is_across_deposit_not_found(body: &str) -> bool {
    body.contains("DepositNotFoundException")
}

fn parse_json_or_string(body: &str) -> Value {
    serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_string()))
}

fn parse_optional_u64(field: &'static str, value: &Option<Value>) -> TestResult<Option<u64>> {
    Ok(parse_optional_u256(field, value)?.map(|value| value.to::<u64>()))
}

fn parse_optional_u128(field: &'static str, value: &Option<Value>) -> TestResult<Option<u128>> {
    Ok(parse_optional_u256(field, value)?.map(|value| value.to::<u128>()))
}

fn request_summary(request: &AcrossSwapApprovalRequest) -> Value {
    json!({
        "trade_type": request.trade_type,
        "origin_chain_id": request.origin_chain_id,
        "destination_chain_id": request.destination_chain_id,
        "input_token": format!("{:?}", request.input_token),
        "output_token": format!("{:?}", request.output_token),
        "amount": request.amount.to_string(),
        "depositor": format!("{:?}", request.depositor),
        "recipient": request.recipient,
        "refund_address": format!("{:?}", request.refund_address),
        "refund_on_origin": request.refund_on_origin,
        "strict_trade_type": request.strict_trade_type,
        "integrator_id": request.integrator_id,
        "slippage": request.slippage,
    })
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider test; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn required_env(key: &str) -> TestResult<String> {
    env::var(key).map_err(|_| format!("missing required env var {key}").into())
}

fn required_private_key() -> TestResult<String> {
    env::var(LIVE_TEST_PRIVATE_KEY)
        .or_else(|_| env::var(ACROSS_PRIVATE_KEY))
        .map_err(|_| {
            format!(
                "missing required env var {} or {}",
                LIVE_TEST_PRIVATE_KEY, ACROSS_PRIVATE_KEY
            )
            .into()
        })
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
        "provider": "across",
        "label": label,
        "created_at_ms": created_at_ms,
        "private_key_env_candidates": [LIVE_TEST_PRIVATE_KEY, ACROSS_PRIVATE_KEY],
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

async fn fetch_raw_swap_approval(
    client: &AcrossClient,
    request: &AcrossSwapApprovalRequest,
) -> TestResult<Value> {
    let mut query = vec![
        ("tradeType", request.trade_type.clone()),
        ("originChainId", request.origin_chain_id.to_string()),
        (
            "destinationChainId",
            request.destination_chain_id.to_string(),
        ),
        ("inputToken", request.input_token.to_string()),
        ("outputToken", request.output_token.to_string()),
        ("amount", request.amount.to_string()),
        ("depositor", request.depositor.to_string()),
        ("recipient", request.recipient.clone()),
        ("refundAddress", request.refund_address.to_string()),
        (
            "refundOnOrigin",
            if request.refund_on_origin {
                "true".to_string()
            } else {
                "false".to_string()
            },
        ),
        (
            "strictTradeType",
            if request.strict_trade_type {
                "true".to_string()
            } else {
                "false".to_string()
            },
        ),
        ("integratorId", request.integrator_id.clone()),
    ];
    if let Some(slippage) = &request.slippage {
        query.push(("slippage", slippage.clone()));
    }
    for (key, value) in &request.extra_query {
        query.push((key.as_str(), value.clone()));
    }

    let response = reqwest::Client::new()
        .get(format!("{}/swap/approval", client.base_url()))
        .bearer_auth(required_env(ACROSS_API_KEY)?)
        .query(&query)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("Across raw /swap/approval failed with {status}: {body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}
