use std::{
    env,
    error::Error,
    fs,
    path::PathBuf,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{address, Address, Bytes, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
};
use router_server::{
    models::MarketOrderKind,
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{ExchangeProvider, ExchangeQuote, ExchangeQuoteRequest, VeloraProvider},
        ChainCall, CustodyAction, ProviderExecutionIntent,
    },
};
use serde_json::{json, Value};
use url::Url;
use uuid::Uuid;

mod support;
use support::{box_error, retry_rpc, wait_for_successful_receipt};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const VELORA_BASE_URL: &str = "VELORA_LIVE_BASE_URL";
const VELORA_API_URL: &str = "VELORA_API_URL";
const VELORA_PARTNER: &str = "VELORA_LIVE_PARTNER";
const VELORA_BASE_RPC_URL: &str = "VELORA_LIVE_BASE_RPC_URL";
const VELORA_PRIVATE_KEY: &str = "VELORA_LIVE_PRIVATE_KEY";
const VELORA_AMOUNT_IN_WEI: &str = "VELORA_LIVE_BASE_ETH_AMOUNT_IN_WEI";
const VELORA_SLIPPAGE_BPS: &str = "VELORA_LIVE_SLIPPAGE_BPS";
const VELORA_SPEND_CONFIRMATION: &str = "VELORA_LIVE_I_UNDERSTAND_THIS_SWAPS_REAL_FUNDS";
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";

const DEFAULT_VELORA_BASE_URL: &str = "https://api.paraswap.io";
const DEFAULT_BASE_USDC: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
const DEFAULT_AMOUNT_IN_WEI: &str = "10000000000000";
const DEFAULT_SLIPPAGE_BPS: u64 = 300;
const BASE_CHAIN_ID: u64 = 8453;
const NATIVE_TOKEN_FOR_VELORA: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

fn base_eth_asset() -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse("evm:8453").expect("static Base chain id"),
        asset: AssetId::Native,
    }
}

fn base_usdc_asset() -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse("evm:8453").expect("static Base chain id"),
        asset: AssetId::reference(format!("{DEFAULT_BASE_USDC:#x}")),
    }
}

#[tokio::test]
#[ignore = "live Velora API observer test; run with LIVE_PROVIDER_TESTS=1"]
async fn live_velora_base_eth_usdc_quote_transcript() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let wallet = optional_private_key()?
        .map(|raw| raw.parse::<PrivateKeySigner>())
        .transpose()?
        .unwrap_or_else(PrivateKeySigner::random);
    let user = wallet.address();
    let amount_in = configured_amount_in()?;
    let provider = live_velora_provider();
    let quote = quote_base_eth_to_usdc(&provider, user, amount_in, "1".to_string()).await?;
    let raw_price_route = fetch_raw_price_route(user, amount_in).await?;

    emit_transcript(
        "velora.base_eth_usdc_quote",
        json!({
            "base_url": live_velora_base_url(),
            "network": BASE_CHAIN_ID,
            "user": format!("{user:#x}"),
            "request": {
                "src_token": NATIVE_TOKEN_FOR_VELORA,
                "dest_token": format!("{DEFAULT_BASE_USDC:#x}"),
                "amount_in_wei": amount_in.to_string(),
                "side": "SELL",
                "version": "6.2",
            },
            "typed_quote": summarize_quote(&quote),
            "raw_price_route": raw_price_route,
        }),
    );

    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS by swapping a tiny amount of Base ETH through live Velora"]
async fn live_velora_base_eth_to_usdc_swap_transcript_spends_funds() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }
    require_confirmation(
        VELORA_SPEND_CONFIRMATION,
        "YES",
        "refusing to submit a live Velora swap",
    )?;

    let signer = required_private_key()?.parse::<PrivateKeySigner>()?;
    let user = signer.address();
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(live_base_rpc_url()?);
    let chain_id = retry_rpc("Base chain id", || async {
        provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    if chain_id != BASE_CHAIN_ID {
        return Err(format!("configured Base RPC is on chain {chain_id}, expected 8453").into());
    }

    let amount_in = configured_amount_in()?;
    let slippage_bps = configured_slippage_bps()?;
    let before_eth = retry_rpc("Base ETH balance before Velora swap", || async {
        provider.get_balance(user).await.map_err(box_error)
    })
    .await?;
    if before_eth <= amount_in {
        return Err(format!(
            "insufficient Base ETH for Velora live swap: balance={}, amount_in={}",
            before_eth, amount_in
        )
        .into());
    }
    let before_usdc = base_usdc_balance(&provider, user).await?;

    let velora = live_velora_provider();
    let loose_quote = quote_base_eth_to_usdc(&velora, user, amount_in, "1".to_string()).await?;
    let min_amount_out = apply_slippage_bps(&loose_quote.amount_out, slippage_bps)?;
    let quote = quote_base_eth_to_usdc(&velora, user, amount_in, min_amount_out).await?;
    let step_request = velora_step_request(user, &quote);
    let execution = velora
        .execute_trade(&step_request)
        .await
        .map_err(|error| format!("Velora execute_trade failed: {error}"))?;
    let actions = execution_actions(execution)?;

    let mut tx_hashes = Vec::new();
    for action in actions {
        tx_hashes.push(execute_evm_action(&provider, &action).await?);
    }

    let after_usdc = base_usdc_balance(&provider, user).await?;
    if after_usdc <= before_usdc {
        return Err(format!(
            "Velora swap did not increase USDC balance: before={}, after={}",
            before_usdc, after_usdc
        )
        .into());
    }
    let after_eth = retry_rpc("Base ETH balance after Velora swap", || async {
        provider.get_balance(user).await.map_err(box_error)
    })
    .await?;

    emit_transcript(
        "velora.base_eth_to_usdc_swap.lifecycle",
        json!({
            "base_url": live_velora_base_url(),
            "base_rpc_url": live_base_rpc_url()?.as_str(),
            "network": BASE_CHAIN_ID,
            "user": format!("{user:#x}"),
            "request": {
                "amount_in_wei": amount_in.to_string(),
                "slippage_bps": slippage_bps,
            },
            "loose_quote": summarize_quote(&loose_quote),
            "execution_quote": summarize_quote(&quote),
            "submitted_tx_hashes": tx_hashes,
            "balances": {
                "eth_before_wei": before_eth.to_string(),
                "eth_after_wei": after_eth.to_string(),
                "usdc_before_raw": before_usdc.to_string(),
                "usdc_after_raw": after_usdc.to_string(),
                "usdc_delta_raw": after_usdc.saturating_sub(before_usdc).to_string(),
            },
        }),
    );

    Ok(())
}

fn live_velora_provider() -> VeloraProvider {
    VeloraProvider::new(live_velora_base_url(), env_var_any(&[VELORA_PARTNER]))
}

async fn quote_base_eth_to_usdc(
    provider: &VeloraProvider,
    user: Address,
    amount_in: U256,
    min_amount_out: String,
) -> TestResult<ExchangeQuote> {
    provider
        .quote_trade(ExchangeQuoteRequest {
            input_asset: base_eth_asset(),
            output_asset: base_usdc_asset(),
            input_decimals: Some(18),
            output_decimals: Some(6),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out,
            },
            sender_address: Some(format!("{user:#x}")),
            recipient_address: format!("{user:#x}"),
        })
        .await
        .map_err(|error| format!("Velora quote failed: {error}"))?
        .ok_or_else(|| "Velora quote returned None".into())
}

async fn fetch_raw_price_route(user: Address, amount_in: U256) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .get(format!("{}/prices", live_velora_base_url()))
        .query(&[
            ("srcToken", NATIVE_TOKEN_FOR_VELORA.to_string()),
            ("destToken", format!("{DEFAULT_BASE_USDC:#x}")),
            ("srcDecimals", "18".to_string()),
            ("destDecimals", "6".to_string()),
            ("amount", amount_in.to_string()),
            ("side", "SELL".to_string()),
            ("network", BASE_CHAIN_ID.to_string()),
            ("version", "6.2".to_string()),
            ("ignoreBadUsdPrice", "true".to_string()),
            ("excludeRFQ", "true".to_string()),
            ("userAddress", format!("{user:#x}")),
            ("receiver", format!("{user:#x}")),
        ])
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("Velora raw /prices failed with {status}: {body}").into());
    }
    let body: Value = serde_json::from_str(&body)?;
    Ok(body
        .get("priceRoute")
        .cloned()
        .ok_or("Velora raw /prices response missing priceRoute")?)
}

fn velora_step_request(user: Address, quote: &ExchangeQuote) -> Value {
    let raw = &quote.provider_quote;
    json!({
        "order_id": Uuid::now_v7(),
        "quote_id": Uuid::now_v7(),
        "order_kind": raw["order_kind"].as_str().unwrap_or("exact_in"),
        "amount_in": quote.amount_in,
        "amount_out": quote.amount_out,
        "min_amount_out": quote.min_amount_out,
        "max_amount_in": quote.max_amount_in,
        "input_asset": raw["input_asset"],
        "output_asset": raw["output_asset"],
        "input_decimals": raw["src_decimals"],
        "output_decimals": raw["dest_decimals"],
        "price_route": raw["price_route"],
        "slippage_bps": raw["slippage_bps"],
        "source_custody_vault_id": Uuid::now_v7(),
        "source_custody_vault_address": format!("{user:#x}"),
        "recipient_address": format!("{user:#x}"),
        "recipient_custody_vault_id": null,
    })
}

fn execution_actions(execution: ProviderExecutionIntent) -> TestResult<Vec<CustodyAction>> {
    match execution {
        ProviderExecutionIntent::CustodyAction { action, .. } => Ok(vec![action]),
        ProviderExecutionIntent::CustodyActions { actions, .. } => Ok(actions),
        ProviderExecutionIntent::ProviderOnly { .. } => {
            Err("Velora execution unexpectedly returned provider-only intent".into())
        }
    }
}

async fn execute_evm_action<P>(provider: &P, action: &CustodyAction) -> TestResult<String>
where
    P: Provider,
{
    let CustodyAction::Call(ChainCall::Evm(call)) = action else {
        return Err(format!("Velora live test only supports EVM calls, got {action:?}").into());
    };
    let request = TransactionRequest::default()
        .with_to(Address::from_str(&call.to_address)?)
        .with_value(U256::from_str_radix(&call.value, 10)?)
        .with_input(Bytes::from_str(&call.calldata)?);
    let pending = provider.send_transaction(request).await?;
    let tx_hash = *pending.tx_hash();
    eprintln!("submitted Velora transaction {tx_hash}");
    wait_for_successful_receipt(provider, tx_hash, "Velora").await?;
    Ok(tx_hash.to_string())
}

async fn base_usdc_balance<P>(provider: &P, account: Address) -> TestResult<U256>
where
    P: Provider,
{
    let token = IERC20::new(DEFAULT_BASE_USDC, provider);
    retry_rpc("Base USDC balance", || async {
        token.balanceOf(account).call().await.map_err(box_error)
    })
    .await
}

fn apply_slippage_bps(amount_out: &str, slippage_bps: u64) -> TestResult<String> {
    if slippage_bps >= 10_000 {
        return Err(format!("{} must be below 10000", VELORA_SLIPPAGE_BPS).into());
    }
    let amount = U256::from_str_radix(amount_out, 10)?;
    let numerator = U256::from(10_000_u64.saturating_sub(slippage_bps));
    Ok((amount.saturating_mul(numerator) / U256::from(10_000_u64)).to_string())
}

fn summarize_quote(quote: &ExchangeQuote) -> Value {
    json!({
        "provider_id": quote.provider_id,
        "amount_in": quote.amount_in,
        "amount_out": quote.amount_out,
        "min_amount_out": quote.min_amount_out,
        "max_amount_in": quote.max_amount_in,
        "expires_at": quote.expires_at.to_rfc3339(),
        "provider_quote": quote.provider_quote,
    })
}

fn configured_amount_in() -> TestResult<U256> {
    U256::from_str_radix(
        &env::var(VELORA_AMOUNT_IN_WEI).unwrap_or_else(|_| DEFAULT_AMOUNT_IN_WEI.to_string()),
        10,
    )
    .map_err(|error| format!("invalid {VELORA_AMOUNT_IN_WEI}: {error}").into())
}

fn configured_slippage_bps() -> TestResult<u64> {
    env::var(VELORA_SLIPPAGE_BPS)
        .ok()
        .map(|raw| raw.parse::<u64>())
        .transpose()
        .map_err(|error| format!("invalid {VELORA_SLIPPAGE_BPS}: {error}").into())
        .and_then(|value| {
            let value = value.unwrap_or(DEFAULT_SLIPPAGE_BPS);
            if value < 10_000 {
                Ok(value)
            } else {
                Err(format!("{VELORA_SLIPPAGE_BPS} must be below 10000").into())
            }
        })
}

fn live_base_rpc_url() -> TestResult<Url> {
    env_var_any(&[
        VELORA_BASE_RPC_URL,
        "ROUTER_LIVE_BASE_RPC_URL",
        "BASE_RPC_URL",
    ])
    .ok_or_else(|| {
        format!(
            "missing required env var {}, ROUTER_LIVE_BASE_RPC_URL, or BASE_RPC_URL",
            VELORA_BASE_RPC_URL
        )
        .into()
    })
    .and_then(|raw| raw.parse::<Url>().map_err(|error| error.into()))
}

fn live_velora_base_url() -> String {
    env_var_any(&[VELORA_BASE_URL, VELORA_API_URL])
        .unwrap_or_else(|| DEFAULT_VELORA_BASE_URL.to_string())
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider test; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn optional_private_key() -> TestResult<Option<String>> {
    let value = env_var_any(&[
        VELORA_PRIVATE_KEY,
        "ROUTER_LIVE_BASE_PRIVATE_KEY",
        "BASE_LIVE_PRIVATE_KEY",
        "ROUTER_LIVE_EVM_PRIVATE_KEY",
        LIVE_TEST_PRIVATE_KEY,
    ]);
    if let Some(raw) = value.as_deref() {
        assert_hex_private_key(raw)?;
    }
    Ok(value)
}

fn required_private_key() -> TestResult<String> {
    optional_private_key()?.ok_or_else(|| {
        format!(
            "missing required env var {}, ROUTER_LIVE_BASE_PRIVATE_KEY, BASE_LIVE_PRIVATE_KEY, ROUTER_LIVE_EVM_PRIVATE_KEY, or {}",
            VELORA_PRIVATE_KEY, LIVE_TEST_PRIVATE_KEY
        )
        .into()
    })
}

fn require_confirmation(key: &str, expected: &str, context: &str) -> TestResult<()> {
    let actual = env::var(key).map_err(|_| format!("missing required env var {key}"))?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{context}; expected {key}={expected}, got {actual:?}").into())
    }
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn assert_hex_private_key(value: &str) -> TestResult<()> {
    let trimmed = value.strip_prefix("0x").unwrap_or(value);
    if trimmed.len() != 64 || !trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("live EVM private key must be a 32-byte hex string".into());
    }
    Ok(())
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
        "provider": "velora",
        "label": label,
        "created_at_ms": created_at_ms,
        "private_key_env_candidates": [
            VELORA_PRIVATE_KEY,
            "ROUTER_LIVE_BASE_PRIVATE_KEY",
            "BASE_LIVE_PRIVATE_KEY",
            "ROUTER_LIVE_EVM_PRIVATE_KEY",
            LIVE_TEST_PRIVATE_KEY
        ],
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
