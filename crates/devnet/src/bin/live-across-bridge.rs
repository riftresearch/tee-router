use std::{env, error::Error, future::Future, str::FromStr, time::Duration};

use alloy::{
    dyn_abi::DynSolValue,
    hex,
    network::{EthereumWallet, TransactionBuilder},
    primitives::{keccak256, Address, Bytes, B256, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use clap::Parser;
use dotenvy::dotenv;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const ACROSS_PRIVATE_KEY: &str = "ACROSS_LIVE_PRIVATE_KEY";
const DEFAULT_MULTICALL_HANDLER: &str = "0x0F7Ae28dE1C8532170AD4ee566B5801485c13a0E";

const RPC_RETRY_ATTEMPTS: usize = 6;
const RECEIPT_POLL_ATTEMPTS: usize = 120;
const RECEIPT_POLL_INTERVAL: Duration = Duration::from_secs(2);
const LIVE_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_HTTP_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;

#[derive(Parser)]
#[command(about = "Submit a live Across bridge transaction and wait for fill")]
struct Args {
    #[arg(
        long,
        env = "ACROSS_LIVE_BASE_URL",
        default_value = "https://app.across.to/api"
    )]
    base_url: String,

    #[arg(long, env = "ACROSS_LIVE_API_KEY")]
    api_key: String,

    #[arg(long, env = "ACROSS_LIVE_INTEGRATOR_ID")]
    integrator_id: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long)]
    origin_rpc_url: String,

    #[arg(long)]
    origin_chain_id: u64,

    #[arg(long)]
    destination_chain_id: u64,

    #[arg(long)]
    input_token: String,

    #[arg(long)]
    output_token: String,

    #[arg(long)]
    amount: String,

    #[arg(long, default_value = "exactInput")]
    trade_type: String,

    #[arg(long)]
    recipient: Option<String>,

    #[arg(long)]
    refund_address: Option<String>,

    #[arg(long)]
    slippage: Option<String>,

    #[arg(
        long,
        help = "Send native ETH on the destination chain to this recipient via Across embedded actions"
    )]
    native_output_recipient: Option<String>,

    #[arg(
        long,
        help = "Unwrap destination WETH inside Across MulticallHandler and forward native ETH to this recipient"
    )]
    unwrap_weth_recipient: Option<String>,

    #[arg(long, default_value = DEFAULT_MULTICALL_HANDLER, help = "Destination-chain Across MulticallHandler used for embedded actions")]
    multicall_handler: String,
}

#[derive(Debug, Clone)]
struct AcrossSwapApprovalRequest {
    trade_type: String,
    origin_chain_id: u64,
    destination_chain_id: u64,
    input_token: Address,
    output_token: Address,
    amount: U256,
    depositor: Address,
    recipient: String,
    refund_address: Address,
    integrator_id: String,
    slippage: Option<String>,
}

impl AcrossSwapApprovalRequest {
    fn query_pairs(&self, message: Option<&str>) -> Vec<(String, String)> {
        let mut query = vec![
            ("tradeType".to_string(), self.trade_type.clone()),
            (
                "originChainId".to_string(),
                self.origin_chain_id.to_string(),
            ),
            (
                "destinationChainId".to_string(),
                self.destination_chain_id.to_string(),
            ),
            ("inputToken".to_string(), self.input_token.to_string()),
            ("outputToken".to_string(), self.output_token.to_string()),
            ("amount".to_string(), self.amount.to_string()),
            ("depositor".to_string(), self.depositor.to_string()),
            ("recipient".to_string(), self.recipient.clone()),
            ("refundAddress".to_string(), self.refund_address.to_string()),
            ("refundOnOrigin".to_string(), true.to_string()),
            ("strictTradeType".to_string(), true.to_string()),
            ("integratorId".to_string(), self.integrator_id.clone()),
        ];
        if let Some(slippage) = &self.slippage {
            query.push(("slippage".to_string(), slippage.clone()));
        }
        if let Some(message) = message {
            query.push(("message".to_string(), message.to_string()));
        }
        query
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcrossSwapApprovalResponse {
    #[serde(default)]
    approval_txns: Option<Vec<AcrossTransaction>>,
    #[serde(default)]
    swap_tx: Option<AcrossTransaction>,
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    expected_output_amount: Option<String>,
    #[serde(default)]
    min_output_amount: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcrossTransaction {
    #[serde(default)]
    chain_id: Option<u64>,
    to: String,
    data: String,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    gas: Option<Value>,
    #[serde(default)]
    max_fee_per_gas: Option<Value>,
    #[serde(default)]
    max_priority_fee_per_gas: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AcrossDepositStatusResponse {
    status: String,
    #[serde(default)]
    deposit_id: Option<String>,
    #[serde(default, rename = "fillTxnRef")]
    fill_tx: Option<String>,
    #[serde(default)]
    pagination: Option<Value>,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    if args.native_output_recipient.is_some() && args.unwrap_weth_recipient.is_some() {
        return Err(
            "use at most one of --native-output-recipient and --unwrap-weth-recipient".into(),
        );
    }

    let private_key = match args.private_key {
        Some(private_key) => private_key,
        None => read_private_key_from_env()?,
    };
    let signer = private_key.parse::<PrivateKeySigner>()?;
    let depositor = signer.address();
    let native_output_recipient = args
        .native_output_recipient
        .as_deref()
        .map(Address::from_str)
        .transpose()?;
    let unwrap_weth_recipient = args
        .unwrap_weth_recipient
        .as_deref()
        .map(Address::from_str)
        .transpose()?;
    let recipient = if native_output_recipient.is_some() || unwrap_weth_recipient.is_some() {
        args.multicall_handler.clone()
    } else {
        args.recipient.unwrap_or_else(|| format!("{depositor:#x}"))
    };
    let refund_address = args
        .refund_address
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or(depositor);
    let request = AcrossSwapApprovalRequest {
        trade_type: args.trade_type,
        origin_chain_id: args.origin_chain_id,
        destination_chain_id: args.destination_chain_id,
        input_token: Address::from_str(&args.input_token)?,
        output_token: Address::from_str(&args.output_token)?,
        amount: U256::from_str_radix(&args.amount, 10)?,
        depositor,
        recipient,
        refund_address,
        integrator_id: args.integrator_id,
        slippage: args.slippage,
    };

    let embedded_actions = native_output_recipient.map(|target| {
        json!({
            "actions": [
                {
                    "target": format!("{:#x}", request.output_token),
                    "functionSignature": "function withdraw(uint256)",
                    "args": [
                        {
                            "value": "0",
                            "populateDynamically": true,
                            "balanceSourceToken": format!("{:#x}", request.output_token),
                        }
                    ],
                    "value": "0",
                    "isNativeTransfer": false,
                },
                {
                    "target": format!("{target:#x}"),
                    "functionSignature": "",
                    "args": [],
                    "value": "0",
                    "isNativeTransfer": true,
                    "populateCallValueDynamically": true,
                }
            ]
        })
    });

    let wrap_choice_message = if let Some(target) = unwrap_weth_recipient {
        Some(fetch_wrap_choice_quote(&args.base_url, &args.api_key, request.clone(), target).await?)
    } else {
        None
    };

    let approval = fetch_swap_approval(
        &args.base_url,
        &args.api_key,
        request.clone(),
        embedded_actions.clone(),
        wrap_choice_message.clone(),
    )
    .await?;
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(args.origin_rpc_url.parse::<Url>()?);

    let mut submitted = Vec::new();
    for tx in approval.approval_txns.as_deref().unwrap_or_default() {
        submitted.push(send_provider_transaction(&provider, tx, request.origin_chain_id).await?);
    }
    let swap_tx = approval
        .swap_tx
        .as_ref()
        .ok_or("Across response did not include swapTx")?;
    let swap_tx_hash =
        send_provider_transaction(&provider, swap_tx, request.origin_chain_id).await?;
    submitted.push(swap_tx_hash.clone());

    let status_samples =
        poll_deposit_status_by_tx_ref(&args.base_url, &args.api_key, &swap_tx_hash).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "request": {
                "origin_chain_id": request.origin_chain_id,
                "destination_chain_id": request.destination_chain_id,
                "input_token": format!("{:#x}", request.input_token),
                "output_token": format!("{:#x}", request.output_token),
                "amount": request.amount.to_string(),
                "depositor": format!("{:#x}", request.depositor),
                "recipient": request.recipient,
                "refund_address": format!("{:#x}", request.refund_address),
            },
            "embedded_actions": embedded_actions,
            "message": wrap_choice_message,
            "quote": approval,
            "submitted_tx_hashes": submitted,
            "deposit_txn_ref": swap_tx_hash,
            "status_samples": status_samples,
        }))?
    );

    Ok(())
}

async fn fetch_swap_approval(
    base_url: &str,
    api_key: &str,
    request: AcrossSwapApprovalRequest,
    actions: Option<Value>,
    message: Option<String>,
) -> CliResult<AcrossSwapApprovalResponse> {
    let endpoint = build_endpoint(base_url, "/swap/approval")?;
    let client = live_http_client()?;
    let response = if let Some(actions) = actions {
        client
            .post(endpoint)
            .query(&request.query_pairs(message.as_deref()))
            .bearer_auth(api_key)
            .json(&actions)
            .send()
            .await?
    } else {
        client
            .get(endpoint)
            .query(&request.query_pairs(message.as_deref()))
            .bearer_auth(api_key)
            .send()
            .await?
    };
    parse_json_response(response).await
}

async fn fetch_wrap_choice_quote(
    base_url: &str,
    api_key: &str,
    request: AcrossSwapApprovalRequest,
    unwrap_recipient: Address,
) -> CliResult<String> {
    let mut message = build_wrap_choice_message(
        unwrap_recipient,
        request.output_token,
        request.amount,
        false,
    )?;
    for _ in 0..3 {
        let quote = fetch_swap_approval(
            base_url,
            api_key,
            request.clone(),
            None,
            Some(message.clone()),
        )
        .await?;
        let quoted_output = wrap_choice_output_amount(&quote)?;
        let next_message = build_wrap_choice_message(
            unwrap_recipient,
            request.output_token,
            quoted_output,
            false,
        )?;
        if next_message == message {
            return Ok(message);
        }
        message = next_message;
    }
    Ok(message)
}

async fn fetch_deposit_status_by_tx_ref(
    base_url: &str,
    api_key: &str,
    deposit_txn_ref: &str,
) -> CliResult<AcrossDepositStatusResponse> {
    let endpoint = build_endpoint(base_url, "/deposit/status")?;
    let response = live_http_client()?
        .get(endpoint)
        .query(&[("depositTxnRef", deposit_txn_ref)])
        .bearer_auth(api_key)
        .send()
        .await?;
    parse_json_response(response).await
}

fn live_http_client() -> CliResult<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(LIVE_HTTP_TIMEOUT)
        .build()?)
}

async fn parse_json_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> CliResult<T> {
    let status = response.status();
    let body = read_limited_response_text(response, LIVE_HTTP_MAX_RESPONSE_BODY_BYTES).await?;
    if !status.is_success() {
        return Err(format!("HTTP {}: {}", status.as_u16(), body).into());
    }
    Ok(serde_json::from_str(&body)?)
}

async fn read_limited_response_text(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> CliResult<String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(format!("response body exceeded {max_bytes} bytes").into());
        }
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

async fn send_provider_transaction<P>(
    provider: &P,
    tx: &AcrossTransaction,
    expected_chain_id: u64,
) -> CliResult<String>
where
    P: Provider,
{
    if tx.chain_id != Some(expected_chain_id) {
        return Err(format!(
            "Across tx chainId {:?} did not match expected {}",
            tx.chain_id, expected_chain_id
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
    if let Some(max_fee_per_gas) =
        parse_optional_u128("maxFeePerGas", &tx.max_fee_per_gas)?.filter(|value| *value > 0)
    {
        request = request.with_max_fee_per_gas(max_fee_per_gas);
    }
    if let Some(max_priority_fee_per_gas) =
        parse_optional_u128("maxPriorityFeePerGas", &tx.max_priority_fee_per_gas)?
            .filter(|value| *value > 0)
    {
        request = request.with_max_priority_fee_per_gas(max_priority_fee_per_gas);
    }

    let pending = provider.send_transaction(request).await?;
    let tx_hash = *pending.tx_hash();
    wait_for_successful_receipt(provider, tx_hash, "Across").await?;
    Ok(tx_hash.to_string())
}

async fn poll_deposit_status_by_tx_ref(
    base_url: &str,
    api_key: &str,
    deposit_txn_ref: &str,
) -> CliResult<Vec<Value>> {
    let mut samples = Vec::new();
    for _ in 0..120 {
        match fetch_deposit_status_by_tx_ref(base_url, api_key, deposit_txn_ref).await {
            Ok(status) => {
                let terminal = status.status == "filled" || status.status == "expired";
                samples.push(serde_json::to_value(&status)?);
                if terminal {
                    return Ok(samples);
                }
            }
            Err(err) if err.to_string().contains("DepositNotFoundException") => {
                samples.push(json!({
                    "status": "not_found",
                    "error": "DepositNotFoundException",
                }));
            }
            Err(err) => return Err(err),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    Err(format!("timed out waiting for Across deposit tx {deposit_txn_ref} to fill").into())
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
                    "retrying RPC {} after attempt {}/{}: {}",
                    label, attempt, RPC_RETRY_ATTEMPTS, err
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
                return Err(format!("{label} transaction {} reverted", tx_hash).into());
            }
            return Ok(());
        }
        tokio::time::sleep(RECEIPT_POLL_INTERVAL).await;
    }
    Err(format!(
        "{label} transaction {} was not mined before timeout",
        tx_hash
    )
    .into())
}

fn build_endpoint(base_url: &str, path: &str) -> CliResult<Url> {
    Ok(Url::parse(&format!(
        "{}{path}",
        base_url.trim_end_matches('/')
    ))?)
}

fn read_private_key_from_env() -> CliResult<String> {
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

fn parse_optional_u64(field: &'static str, value: &Option<Value>) -> CliResult<Option<u64>> {
    Ok(parse_optional_u256(field, value)?.map(|value| value.to::<u64>()))
}

fn parse_optional_u128(field: &'static str, value: &Option<Value>) -> CliResult<Option<u128>> {
    Ok(parse_optional_u256(field, value)?.map(|value| value.to::<u128>()))
}

fn parse_optional_u256(field: &'static str, value: &Option<Value>) -> CliResult<Option<U256>> {
    let Some(value) = value else {
        return Ok(None);
    };
    match value {
        Value::Null => Ok(None),
        Value::Number(number) => parse_u256(field, &number.to_string()).map(Some),
        Value::String(string) if string.is_empty() => Ok(None),
        Value::String(string) => parse_u256(field, string).map(Some),
        other => Err(format!("invalid numeric field {field}: {other}").into()),
    }
}

fn parse_u256(field: &'static str, raw: &str) -> CliResult<U256> {
    let trimmed = raw.trim();
    if let Some(hex) = trimmed
        .strip_prefix("0x")
        .or_else(|| trimmed.strip_prefix("0X"))
    {
        return U256::from_str_radix(hex, 16)
            .map_err(|_| format!("invalid hex numeric field {field}: {raw}").into());
    }
    U256::from_str_radix(trimmed, 10)
        .map_err(|_| format!("invalid decimal numeric field {field}: {raw}").into())
}

fn wrap_choice_output_amount(quote: &AcrossSwapApprovalResponse) -> CliResult<U256> {
    if let Some(min_output_amount) = quote.min_output_amount.as_deref() {
        return parse_u256("minOutputAmount", min_output_amount);
    }
    if let Some(expected_output_amount) = quote.expected_output_amount.as_deref() {
        return parse_u256("expectedOutputAmount", expected_output_amount);
    }
    Err("Across quote missing minOutputAmount/expectedOutputAmount for wrap-choice message".into())
}

fn build_wrap_choice_message(
    user_address: Address,
    output_token: Address,
    output_amount: U256,
    send_weth: bool,
) -> CliResult<String> {
    let calls = if send_weth {
        vec![DynSolValue::Tuple(vec![
            DynSolValue::Address(output_token),
            DynSolValue::Bytes(encode_erc20_transfer_calldata(user_address, output_amount)),
            DynSolValue::Uint(U256::ZERO, 256),
        ])]
    } else {
        vec![
            DynSolValue::Tuple(vec![
                DynSolValue::Address(output_token),
                DynSolValue::Bytes(encode_weth_withdraw_calldata(output_amount)),
                DynSolValue::Uint(U256::ZERO, 256),
            ]),
            DynSolValue::Tuple(vec![
                DynSolValue::Address(user_address),
                DynSolValue::Bytes(Vec::new()),
                DynSolValue::Uint(output_amount, 256),
            ]),
        ]
    };
    let instructions = DynSolValue::Tuple(vec![
        DynSolValue::Array(calls),
        DynSolValue::Address(user_address),
    ]);
    Ok(format!("0x{}", hex::encode(instructions.abi_encode())))
}

fn encode_weth_withdraw_calldata(amount: U256) -> Vec<u8> {
    let mut calldata = keccak256("withdraw(uint256)".as_bytes())[..4].to_vec();
    calldata.extend(DynSolValue::Uint(amount, 256).abi_encode());
    calldata
}

fn encode_erc20_transfer_calldata(recipient: Address, amount: U256) -> Vec<u8> {
    let mut calldata = keccak256("transfer(address,uint256)")[..4].to_vec();
    calldata.extend(
        DynSolValue::Tuple(vec![
            DynSolValue::Address(recipient),
            DynSolValue::Uint(amount, 256),
        ])
        .abi_encode_params(),
    );
    calldata
}

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
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
