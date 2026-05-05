use alloy::{
    hex,
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, Bytes, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use router_server::protocol::{AssetId, ChainId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    fmt, fs,
    path::PathBuf,
    process::Stdio,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{io::AsyncWriteExt, process::Command, time::Instant};
use url::Url;

mod support;
use support::{box_error, retry_rpc, wait_for_successful_receipt};

const RUN_FLAG: &str = "ROUTER_LIVE_E2E";
const CONFIRM_SPEND_ENV: &str = "ROUTER_LIVE_CONFIRM_SPEND";
const CONFIRM_SPEND_VALUE: &str = "I_UNDERSTAND_THIS_SPENDS_REAL_FUNDS";
const DEFAULT_ACROSS_API_URL: &str = "https://app.across.to/api";
const DEFAULT_UNIT_API_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_POLL_INTERVAL_SECS: u64 = 15;
const DEFAULT_TIMEOUT_SECS: u64 = 60 * 60;
const MIN_QUOTE_EXPIRY_BUFFER_SECS: i64 = 30;
const LIVE_RECOVERY_DIR: &str = "ROUTER_LIVE_RECOVERY_DIR";

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::test]
#[ignore = "requires funded wallets, real RPCs, live provider credentials, and explicit spend confirmation"]
async fn live_market_order_e2e_environment_contract() -> TestResult<()> {
    if env::var(RUN_FLAG).ok().as_deref() != Some("1") {
        eprintln!(
            "set {RUN_FLAG}=1 plus the ROUTER_LIVE_* variables to validate a live market-order run"
        );
        return Ok(());
    }

    let config = LiveRouteConfig::from_env()?;
    config.validate_static_contract()?;
    eprintln!(
        "live E2E env contract is valid for {} -> Unit({}/{}) -> Hyperliquid -> Unit({}/{})",
        config.across_input_token,
        config.unit_deposit_src_chain,
        config.unit_deposit_asset,
        config.unit_withdraw_dst_chain,
        config.unit_withdraw_asset
    );
    Ok(())
}

#[tokio::test]
#[ignore = "SPENDS REAL FUNDS through Across, Unit, and Hyperliquid when explicitly enabled"]
async fn live_across_unit_hyperliquid_real_money_route() -> TestResult<()> {
    if env::var(RUN_FLAG).ok().as_deref() != Some("1") {
        eprintln!(
            "skipping live real-money route; set {RUN_FLAG}=1 and {CONFIRM_SPEND_ENV}={CONFIRM_SPEND_VALUE}"
        );
        return Ok(());
    }

    let config = LiveRouteConfig::from_env()?;
    config.validate_static_contract()?;
    config.validate_spend_confirmation()?;

    let source_signer = PrivateKeySigner::from_str(&config.evm_private_key)?;
    let source_address = source_signer.address();
    if let Some(expected) = config.expected_evm_address {
        assert_eq!(
            source_address, expected,
            "ROUTER_LIVE_EXPECTED_EVM_ADDRESS does not match ROUTER_LIVE_EVM_PRIVATE_KEY"
        );
    }
    assert_eq!(
        config.refund_address, source_address,
        "live E2E currently requires refund address to be the source wallet"
    );

    let source_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(source_signer))
        .connect_http(config.source_rpc_url.clone());
    let chain_id = retry_rpc("source get_chain_id", || async {
        source_provider.get_chain_id().await.map_err(box_error)
    })
    .await?;
    assert_eq!(
        chain_id, config.across_origin_chain_id,
        "ROUTER_LIVE_SOURCE_RPC_URL is connected to chain {chain_id}, expected {}",
        config.across_origin_chain_id
    );

    let unit = UnitClient::new(config.unit_api_url.clone());
    let across = AcrossClient::new(config.across_api_url.clone(), config.across_api_key.clone());

    let hyperliquid_unit_destination = config.hyperliquid_unit_destination_address();
    let unit_deposit = unit
        .generate_address(
            &config.unit_deposit_src_chain,
            "hyperliquid",
            &config.unit_deposit_asset,
            &hyperliquid_unit_destination.to_string(),
        )
        .await?;
    assert_eq!(
        unit_deposit.status.as_deref().unwrap_or("OK"),
        "OK",
        "Unit generate-address returned non-OK status"
    );
    eprintln!(
        "Unit deposit address for Hyperliquid {}: {}",
        hyperliquid_unit_destination, unit_deposit.address
    );
    emit_live_route_recovery_artifact(
        "unit_deposit_address_generated",
        &config,
        json!({
            "source_address": source_address.to_string(),
            "hyperliquid_unit_destination": hyperliquid_unit_destination.to_string(),
            "unit_deposit": &unit_deposit,
        }),
    );
    let seen_deposit_operations = unit
        .operation_fingerprints(
            &hyperliquid_unit_destination.to_string(),
            &unit_deposit.address,
        )
        .await?;

    let quote = across
        .swap_approval(AcrossSwapApprovalRequest {
            trade_type: config.across_trade_type.clone(),
            origin_chain_id: config.across_origin_chain_id,
            destination_chain_id: config.across_destination_chain_id,
            input_token: config.across_input_token,
            output_token: config.across_output_token,
            amount: config.amount_in,
            depositor: source_address,
            recipient: unit_deposit.address.clone(),
            refund_address: config.refund_address,
            refund_on_origin: config.across_refund_on_origin,
            strict_trade_type: true,
            integrator_id: config.across_integrator_id.clone(),
            slippage: config.across_slippage.clone(),
            extra_query: config.across_extra_query.clone(),
        })
        .await?;

    quote.assert_executable(config.across_origin_chain_id)?;
    quote.assert_not_expired()?;
    quote.assert_max_input_amount(config.max_amount_in)?;
    quote.assert_minimum_output(config.min_across_output_amount)?;
    quote.assert_approval_policy(config.allow_approval_txns)?;
    eprintln!(
        "Across quote {} expects output {} with min {}",
        quote.id.as_deref().unwrap_or("<no-id>"),
        quote
            .expected_output_amount
            .as_deref()
            .unwrap_or("<unknown>"),
        quote.min_output_amount.as_deref().unwrap_or("<unknown>")
    );
    emit_live_route_recovery_artifact(
        "across_quote_ready",
        &config,
        json!({
            "source_address": source_address.to_string(),
            "unit_deposit_address": &unit_deposit.address,
            "across_quote": &quote,
        }),
    );

    let across_tx_hashes =
        execute_across_quote(&source_provider, &quote, config.across_origin_chain_id).await?;
    assert!(
        !across_tx_hashes.is_empty(),
        "Across quote did not produce any executable transactions"
    );
    eprintln!("submitted Across txs: {across_tx_hashes:?}");
    emit_live_route_recovery_artifact(
        "across_transactions_submitted",
        &config,
        json!({
            "source_address": source_address.to_string(),
            "unit_deposit_address": &unit_deposit.address,
            "across_tx_hashes": &across_tx_hashes,
        }),
    );

    let deposit_operation = unit
        .wait_for_done_operation(
            &hyperliquid_unit_destination.to_string(),
            &unit_deposit.address,
            "Unit deposit into Hyperliquid",
            config.poll_interval,
            config.timeout,
            &seen_deposit_operations,
        )
        .await?;
    eprintln!(
        "Unit deposit done: operation_id={:?} source_tx={:?} destination_tx={:?}",
        deposit_operation.operation_id,
        deposit_operation.source_tx_hash,
        deposit_operation.destination_tx_hash
    );
    emit_live_route_recovery_artifact(
        "unit_deposit_completed",
        &config,
        json!({
            "source_address": source_address.to_string(),
            "unit_deposit_address": &unit_deposit.address,
            "deposit_operation": &deposit_operation,
        }),
    );

    let unit_withdraw = unit
        .generate_address(
            "hyperliquid",
            &config.unit_withdraw_dst_chain,
            &config.unit_withdraw_asset,
            &config.withdraw_recipient_address,
        )
        .await?;
    assert_eq!(
        unit_withdraw.status.as_deref().unwrap_or("OK"),
        "OK",
        "Unit generate-address returned non-OK status for withdrawal"
    );
    eprintln!(
        "Unit withdrawal protocol address for {}: {}",
        config.withdraw_recipient_address, unit_withdraw.address
    );
    emit_live_route_recovery_artifact(
        "unit_withdraw_address_generated",
        &config,
        json!({
            "withdraw_recipient_address": &config.withdraw_recipient_address,
            "unit_withdraw": &unit_withdraw,
        }),
    );
    let seen_withdraw_operations = unit
        .operation_fingerprints(&config.withdraw_recipient_address, &unit_withdraw.address)
        .await?;

    let hyperliquid_result = run_hyperliquid_helper(&config, &unit_withdraw.address).await?;
    eprintln!("Hyperliquid helper result: {hyperliquid_result}");
    emit_live_route_recovery_artifact(
        "hyperliquid_helper_completed",
        &config,
        json!({
            "unit_withdraw_address": &unit_withdraw.address,
            "hyperliquid_result": &hyperliquid_result,
        }),
    );

    let withdraw_operation = unit
        .wait_for_done_operation(
            &config.withdraw_recipient_address,
            &unit_withdraw.address,
            "Unit withdrawal from Hyperliquid",
            config.poll_interval,
            config.timeout,
            &seen_withdraw_operations,
        )
        .await?;
    eprintln!(
        "Unit withdrawal done: operation_id={:?} source_tx={:?} destination_tx={:?}",
        withdraw_operation.operation_id,
        withdraw_operation.source_tx_hash,
        withdraw_operation.destination_tx_hash
    );
    emit_live_route_recovery_artifact(
        "unit_withdraw_completed",
        &config,
        json!({
            "unit_withdraw_address": &unit_withdraw.address,
            "withdraw_operation": &withdraw_operation,
        }),
    );

    Ok(())
}

#[derive(Clone)]
struct LiveRouteConfig {
    source_rpc_url: Url,
    evm_private_key: String,
    expected_evm_address: Option<Address>,
    across_api_url: Url,
    across_api_key: String,
    across_integrator_id: String,
    across_trade_type: String,
    across_origin_chain_id: u64,
    across_destination_chain_id: u64,
    across_input_token: Address,
    across_output_token: Address,
    across_refund_on_origin: bool,
    across_slippage: Option<String>,
    across_extra_query: BTreeMap<String, String>,
    allow_approval_txns: bool,
    unit_api_url: Url,
    unit_deposit_src_chain: String,
    unit_deposit_asset: String,
    unit_withdraw_dst_chain: String,
    unit_withdraw_asset: String,
    hyperliquid_api_url: Url,
    hyperliquid_python: String,
    hyperliquid_private_key: String,
    hyperliquid_destination_address: Address,
    hyperliquid_account_address: Option<Address>,
    hyperliquid_vault_address: Option<Address>,
    hyperliquid_trade_plan: Value,
    hyperliquid_withdraw_token_symbol: String,
    hyperliquid_withdraw_amount: String,
    withdraw_recipient_address: String,
    refund_address: Address,
    amount_in: U256,
    max_amount_in: U256,
    min_across_output_amount: U256,
    poll_interval: Duration,
    timeout: Duration,
}

impl fmt::Debug for LiveRouteConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveRouteConfig")
            .field(
                "source_rpc_url",
                &redacted_url_for_debug(&self.source_rpc_url),
            )
            .field("evm_private_key", &"<redacted>")
            .field("expected_evm_address", &self.expected_evm_address)
            .field(
                "across_api_url",
                &redacted_url_for_debug(&self.across_api_url),
            )
            .field("across_api_key", &"<redacted>")
            .field("across_integrator_id", &self.across_integrator_id)
            .field("across_trade_type", &self.across_trade_type)
            .field("across_origin_chain_id", &self.across_origin_chain_id)
            .field(
                "across_destination_chain_id",
                &self.across_destination_chain_id,
            )
            .field("across_input_token", &self.across_input_token)
            .field("across_output_token", &self.across_output_token)
            .field("across_refund_on_origin", &self.across_refund_on_origin)
            .field("across_slippage", &self.across_slippage)
            .field("across_extra_query_count", &self.across_extra_query.len())
            .field("allow_approval_txns", &self.allow_approval_txns)
            .field("unit_api_url", &redacted_url_for_debug(&self.unit_api_url))
            .field("unit_deposit_src_chain", &self.unit_deposit_src_chain)
            .field("unit_deposit_asset", &self.unit_deposit_asset)
            .field("unit_withdraw_dst_chain", &self.unit_withdraw_dst_chain)
            .field("unit_withdraw_asset", &self.unit_withdraw_asset)
            .field(
                "hyperliquid_api_url",
                &redacted_url_for_debug(&self.hyperliquid_api_url),
            )
            .field("hyperliquid_python", &self.hyperliquid_python)
            .field("hyperliquid_private_key", &"<redacted>")
            .field(
                "hyperliquid_destination_address",
                &self.hyperliquid_destination_address,
            )
            .field(
                "hyperliquid_account_address",
                &self.hyperliquid_account_address,
            )
            .field("hyperliquid_vault_address", &self.hyperliquid_vault_address)
            .field("hyperliquid_trade_plan", &self.hyperliquid_trade_plan)
            .field(
                "hyperliquid_withdraw_token_symbol",
                &self.hyperliquid_withdraw_token_symbol,
            )
            .field(
                "hyperliquid_withdraw_amount",
                &self.hyperliquid_withdraw_amount,
            )
            .field(
                "withdraw_recipient_address",
                &self.withdraw_recipient_address,
            )
            .field("refund_address", &self.refund_address)
            .field("amount_in", &self.amount_in)
            .field("max_amount_in", &self.max_amount_in)
            .field("min_across_output_amount", &self.min_across_output_amount)
            .field("poll_interval", &self.poll_interval)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl LiveRouteConfig {
    fn from_env() -> TestResult<Self> {
        let source_rpc_url = read_url_env("ROUTER_LIVE_SOURCE_RPC_URL")?;
        let evm_private_key = read_required_env("ROUTER_LIVE_EVM_PRIVATE_KEY")?;
        assert_hex_private_key(&evm_private_key)?;
        let expected_evm_address = read_optional_address_env("ROUTER_LIVE_EXPECTED_EVM_ADDRESS")?;
        let across_api_url =
            read_url_env_with_default("ROUTER_LIVE_ACROSS_API_URL", DEFAULT_ACROSS_API_URL)?;
        let across_api_key = read_required_env("ROUTER_LIVE_ACROSS_API_KEY")?;
        let across_integrator_id = read_required_env("ROUTER_LIVE_ACROSS_INTEGRATOR_ID")?;
        let across_trade_type =
            read_env_with_default("ROUTER_LIVE_ACROSS_TRADE_TYPE", "exactInput");
        let across_origin_chain_id = read_u64_env("ROUTER_LIVE_ACROSS_ORIGIN_CHAIN_ID")?;
        let across_destination_chain_id = read_u64_env("ROUTER_LIVE_ACROSS_DESTINATION_CHAIN_ID")?;
        let across_input_token = read_address_env("ROUTER_LIVE_ACROSS_INPUT_TOKEN")?;
        let across_output_token = read_address_env("ROUTER_LIVE_ACROSS_OUTPUT_TOKEN")?;
        let across_refund_on_origin =
            read_bool_env_with_default("ROUTER_LIVE_ACROSS_REFUND_ON_ORIGIN", true)?;
        let across_slippage = read_optional_env("ROUTER_LIVE_ACROSS_SLIPPAGE")
            .or_else(|| read_optional_env("ROUTER_LIVE_ACROSS_SLIPPAGE_TOLERANCE"));
        let across_extra_query = read_json_query_map_env("ROUTER_LIVE_ACROSS_EXTRA_QUERY_JSON")?;
        let allow_approval_txns =
            read_bool_env_with_default("ROUTER_LIVE_ALLOW_APPROVAL_TXNS", false)?;
        let unit_api_url =
            read_url_env_with_default("ROUTER_LIVE_UNIT_API_URL", DEFAULT_UNIT_API_URL)?;
        let unit_deposit_src_chain = read_required_env("ROUTER_LIVE_UNIT_DEPOSIT_SRC_CHAIN")?;
        let unit_deposit_asset = read_required_env("ROUTER_LIVE_UNIT_DEPOSIT_ASSET")?;
        let unit_withdraw_dst_chain = read_required_env("ROUTER_LIVE_UNIT_WITHDRAW_DST_CHAIN")?;
        let unit_withdraw_asset = read_required_env("ROUTER_LIVE_UNIT_WITHDRAW_ASSET")?;
        let hyperliquid_api_url = read_url_env_with_default(
            "ROUTER_LIVE_HYPERLIQUID_API_URL",
            DEFAULT_HYPERLIQUID_API_URL,
        )?;
        let hyperliquid_python = read_env_with_default("ROUTER_LIVE_HYPERLIQUID_PYTHON", "python3");
        let hyperliquid_private_key = read_required_env("ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY")?;
        assert_hex_private_key(&hyperliquid_private_key)?;
        let hyperliquid_destination_address =
            read_address_env("ROUTER_LIVE_HYPERLIQUID_DESTINATION_ADDRESS")?;
        let hyperliquid_account_address =
            read_optional_address_env("ROUTER_LIVE_HYPERLIQUID_ACCOUNT_ADDRESS")?;
        let hyperliquid_vault_address =
            read_optional_address_env("ROUTER_LIVE_HYPERLIQUID_VAULT_ADDRESS")?;
        let hyperliquid_trade_plan = read_json_env("ROUTER_LIVE_HYPERLIQUID_TRADE_PLAN_JSON")?;
        let hyperliquid_withdraw_token_symbol =
            read_required_env("ROUTER_LIVE_HYPERLIQUID_WITHDRAW_TOKEN_SYMBOL")?;
        let hyperliquid_withdraw_amount =
            read_required_env("ROUTER_LIVE_HYPERLIQUID_WITHDRAW_AMOUNT")?;
        let withdraw_recipient_address =
            read_required_env("ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS")?;
        let refund_address = read_address_env("ROUTER_LIVE_REFUND_ADDRESS")?;
        let amount_in = read_u256_env("ROUTER_LIVE_AMOUNT_IN")?;
        let max_amount_in = read_u256_env("ROUTER_LIVE_MAX_AMOUNT_IN")?;
        let min_across_output_amount = read_u256_env("ROUTER_LIVE_MIN_ACROSS_OUTPUT_AMOUNT")?;
        let poll_interval = Duration::from_secs(read_u64_env_with_default(
            "ROUTER_LIVE_POLL_INTERVAL_SECS",
            DEFAULT_POLL_INTERVAL_SECS,
        )?);
        let timeout = Duration::from_secs(read_u64_env_with_default(
            "ROUTER_LIVE_TIMEOUT_SECS",
            DEFAULT_TIMEOUT_SECS,
        )?);

        Ok(Self {
            source_rpc_url,
            evm_private_key,
            expected_evm_address,
            across_api_url,
            across_api_key,
            across_integrator_id,
            across_trade_type,
            across_origin_chain_id,
            across_destination_chain_id,
            across_input_token,
            across_output_token,
            across_refund_on_origin,
            across_slippage,
            across_extra_query,
            allow_approval_txns,
            unit_api_url,
            unit_deposit_src_chain,
            unit_deposit_asset,
            unit_withdraw_dst_chain,
            unit_withdraw_asset,
            hyperliquid_api_url,
            hyperliquid_python,
            hyperliquid_private_key,
            hyperliquid_destination_address,
            hyperliquid_account_address,
            hyperliquid_vault_address,
            hyperliquid_trade_plan,
            hyperliquid_withdraw_token_symbol,
            hyperliquid_withdraw_amount,
            withdraw_recipient_address,
            refund_address,
            amount_in,
            max_amount_in,
            min_across_output_amount,
            poll_interval,
            timeout,
        })
    }

    fn validate_static_contract(&self) -> TestResult<()> {
        assert!(
            matches!(
                self.across_trade_type.as_str(),
                "exactInput" | "exactOutput" | "minOutput"
            ),
            "ROUTER_LIVE_ACROSS_TRADE_TYPE must be exactInput, exactOutput, or minOutput"
        );
        if let Some(slippage) = &self.across_slippage {
            assert_across_slippage(slippage)?;
        }
        assert!(
            self.amount_in <= self.max_amount_in,
            "ROUTER_LIVE_AMOUNT_IN exceeds ROUTER_LIVE_MAX_AMOUNT_IN"
        );
        assert!(
            self.poll_interval > Duration::ZERO,
            "ROUTER_LIVE_POLL_INTERVAL_SECS must be nonzero"
        );
        assert!(
            self.timeout >= self.poll_interval,
            "ROUTER_LIVE_TIMEOUT_SECS must be at least ROUTER_LIVE_POLL_INTERVAL_SECS"
        );
        assert!(
            self.hyperliquid_trade_plan
                .as_array()
                .is_some_and(|items| !items.is_empty()),
            "ROUTER_LIVE_HYPERLIQUID_TRADE_PLAN_JSON must be a non-empty JSON array"
        );
        assert_decimal_amount(
            "ROUTER_LIVE_HYPERLIQUID_WITHDRAW_AMOUNT",
            &self.hyperliquid_withdraw_amount,
        )?;

        if let Ok(chain_id) = env::var("ROUTER_LIVE_SOURCE_CHAIN_ID") {
            ChainId::parse(&chain_id)
                .map_err(|err| format!("ROUTER_LIVE_SOURCE_CHAIN_ID is invalid: {err}"))?;
        }
        if let Ok(asset_id) = env::var("ROUTER_LIVE_SOURCE_ASSET") {
            AssetId::parse(&asset_id)
                .map_err(|err| format!("ROUTER_LIVE_SOURCE_ASSET is invalid: {err}"))?;
        }
        if let Ok(chain_id) = env::var("ROUTER_LIVE_DESTINATION_CHAIN_ID") {
            ChainId::parse(&chain_id)
                .map_err(|err| format!("ROUTER_LIVE_DESTINATION_CHAIN_ID is invalid: {err}"))?;
        }
        if let Ok(asset_id) = env::var("ROUTER_LIVE_DESTINATION_ASSET") {
            AssetId::parse(&asset_id)
                .map_err(|err| format!("ROUTER_LIVE_DESTINATION_ASSET is invalid: {err}"))?;
        }

        Ok(())
    }

    fn validate_spend_confirmation(&self) -> TestResult<()> {
        let confirmation = read_required_env(CONFIRM_SPEND_ENV)?;
        assert_eq!(
            confirmation, CONFIRM_SPEND_VALUE,
            "{CONFIRM_SPEND_ENV} must equal {CONFIRM_SPEND_VALUE}"
        );
        assert!(
            self.amount_in <= self.max_amount_in,
            "refusing to spend: ROUTER_LIVE_AMOUNT_IN exceeds ROUTER_LIVE_MAX_AMOUNT_IN"
        );
        Ok(())
    }

    fn hyperliquid_unit_destination_address(&self) -> Address {
        self.hyperliquid_vault_address
            .unwrap_or(self.hyperliquid_destination_address)
    }
}

fn redacted_url_for_debug(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if url.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    redacted
}

#[test]
fn live_route_config_debug_redacts_live_credentials() {
    let evm_private_key = format!("0x{}", "11".repeat(32));
    let hyperliquid_private_key = format!("0x{}", "22".repeat(32));
    let mut across_extra_query = BTreeMap::new();
    across_extra_query.insert("apiToken".to_string(), "extra-secret".to_string());

    let config = LiveRouteConfig {
        source_rpc_url: Url::parse(
            "https://rpc-user:rpc-pass@rpc.example/private-token?api_key=query-secret",
        )
        .expect("source rpc url"),
        evm_private_key: evm_private_key.clone(),
        expected_evm_address: None,
        across_api_url: Url::parse("https://across.example/api").expect("across api url"),
        across_api_key: "across-secret".to_string(),
        across_integrator_id: "integrator".to_string(),
        across_trade_type: "exactInput".to_string(),
        across_origin_chain_id: 1,
        across_destination_chain_id: 8453,
        across_input_token: Address::ZERO,
        across_output_token: Address::ZERO,
        across_refund_on_origin: true,
        across_slippage: None,
        across_extra_query,
        allow_approval_txns: false,
        unit_api_url: Url::parse("https://unit.example").expect("unit api url"),
        unit_deposit_src_chain: "base".to_string(),
        unit_deposit_asset: "usdc".to_string(),
        unit_withdraw_dst_chain: "base".to_string(),
        unit_withdraw_asset: "usdc".to_string(),
        hyperliquid_api_url: Url::parse("https://hyper.example/private?token=hyper-url-secret")
            .expect("hyperliquid api url"),
        hyperliquid_python: "python3".to_string(),
        hyperliquid_private_key: hyperliquid_private_key.clone(),
        hyperliquid_destination_address: Address::ZERO,
        hyperliquid_account_address: None,
        hyperliquid_vault_address: None,
        hyperliquid_trade_plan: json!({"asset": "ETH"}),
        hyperliquid_withdraw_token_symbol: "USDC".to_string(),
        hyperliquid_withdraw_amount: "1".to_string(),
        withdraw_recipient_address: "recipient".to_string(),
        refund_address: Address::ZERO,
        amount_in: U256::from(1),
        max_amount_in: U256::from(1),
        min_across_output_amount: U256::from(1),
        poll_interval: Duration::from_secs(1),
        timeout: Duration::from_secs(1),
    };

    let debug = format!("{config:?}");

    assert!(debug.contains("evm_private_key: \"<redacted>\""));
    assert!(debug.contains("hyperliquid_private_key: \"<redacted>\""));
    assert!(debug.contains("across_api_key: \"<redacted>\""));
    assert!(!debug.contains(&evm_private_key));
    assert!(!debug.contains(&hyperliquid_private_key));
    assert!(!debug.contains("across-secret"));
    assert!(!debug.contains("rpc-user"));
    assert!(!debug.contains("rpc-pass"));
    assert!(!debug.contains("private-token"));
    assert!(!debug.contains("query-secret"));
    assert!(!debug.contains("hyper-url-secret"));
    assert!(!debug.contains("extra-secret"));
}

fn emit_live_route_recovery_artifact(label: &str, config: &LiveRouteConfig, value: Value) {
    if let Err(error) = write_live_route_recovery_artifact(label, config, &value) {
        eprintln!("failed to write live market-order recovery artifact: {error}");
    }
}

fn write_live_route_recovery_artifact(
    label: &str,
    config: &LiveRouteConfig,
    value: &Value,
) -> TestResult<()> {
    let created_at_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("system clock before unix epoch: {error}"))?
        .as_millis();
    let dir = env::var(LIVE_RECOVERY_DIR)
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/live-recovery")
        })
        .join("legacy-live-market-order");
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!(
        "{}-{}.json",
        created_at_ms,
        sanitize_artifact_label(label)
    ));
    let payload = json!({
        "schema_version": 1,
        "kind": "legacy_live_market_order_recovery_artifact",
        "label": label,
        "created_at_ms": created_at_ms,
        "key_material_envs": {
            "source_evm_private_key": "ROUTER_LIVE_EVM_PRIVATE_KEY",
            "hyperliquid_private_key": "ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY",
        },
        "config": {
            "source_rpc_url": config.source_rpc_url.as_str(),
            "expected_evm_address": config.expected_evm_address.map(|address| address.to_string()),
            "across_api_url": config.across_api_url.as_str(),
            "across_origin_chain_id": config.across_origin_chain_id,
            "across_destination_chain_id": config.across_destination_chain_id,
            "across_input_token": config.across_input_token.to_string(),
            "across_output_token": config.across_output_token.to_string(),
            "unit_api_url": config.unit_api_url.as_str(),
            "unit_deposit_src_chain": &config.unit_deposit_src_chain,
            "unit_deposit_asset": &config.unit_deposit_asset,
            "unit_withdraw_dst_chain": &config.unit_withdraw_dst_chain,
            "unit_withdraw_asset": &config.unit_withdraw_asset,
            "hyperliquid_api_url": config.hyperliquid_api_url.as_str(),
            "hyperliquid_destination_address": config.hyperliquid_destination_address.to_string(),
            "hyperliquid_account_address": config.hyperliquid_account_address.map(|address| address.to_string()),
            "hyperliquid_vault_address": config.hyperliquid_vault_address.map(|address| address.to_string()),
            "withdraw_recipient_address": &config.withdraw_recipient_address,
            "refund_address": config.refund_address.to_string(),
            "amount_in": config.amount_in.to_string(),
            "max_amount_in": config.max_amount_in.to_string(),
            "min_across_output_amount": config.min_across_output_amount.to_string(),
        },
        "event": value,
    });
    fs::write(&path, serde_json::to_vec_pretty(&payload)?)?;
    eprintln!(
        "live market-order recovery artifact written to {}",
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

struct AcrossClient {
    http: reqwest::Client,
    base_url: Url,
    api_key: String,
}

impl AcrossClient {
    fn new(base_url: Url, api_key: String) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
            api_key,
        }
    }

    async fn swap_approval(
        &self,
        request: AcrossSwapApprovalRequest,
    ) -> TestResult<AcrossSwapApprovalResponse> {
        let endpoint = self.endpoint("/swap/approval")?;
        let mut query = vec![
            ("tradeType".to_string(), request.trade_type),
            (
                "originChainId".to_string(),
                request.origin_chain_id.to_string(),
            ),
            (
                "destinationChainId".to_string(),
                request.destination_chain_id.to_string(),
            ),
            ("inputToken".to_string(), request.input_token.to_string()),
            ("outputToken".to_string(), request.output_token.to_string()),
            ("amount".to_string(), request.amount.to_string()),
            ("depositor".to_string(), request.depositor.to_string()),
            ("recipient".to_string(), request.recipient),
            (
                "refundAddress".to_string(),
                request.refund_address.to_string(),
            ),
            (
                "refundOnOrigin".to_string(),
                request.refund_on_origin.to_string(),
            ),
            (
                "strictTradeType".to_string(),
                request.strict_trade_type.to_string(),
            ),
            ("integratorId".to_string(), request.integrator_id),
        ];
        if let Some(slippage) = request.slippage {
            query.push(("slippage".to_string(), slippage));
        }
        query.extend(request.extra_query);

        let builder = self
            .http
            .get(endpoint)
            .query(&query)
            .bearer_auth(&self.api_key);

        let response = builder.send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("Across /swap/approval HTTP {status}: {body}").into());
        }

        serde_json::from_str(&body).map_err(|err| {
            format!("Across /swap/approval returned invalid JSON: {err}; body={body}").into()
        })
    }

    fn endpoint(&self, path: &str) -> TestResult<Url> {
        let base = self.base_url.as_str().trim_end_matches('/');
        Ok(Url::parse(&format!("{base}{path}"))?)
    }
}

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
    refund_on_origin: bool,
    strict_trade_type: bool,
    integrator_id: String,
    slippage: Option<String>,
    extra_query: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct AcrossSwapApprovalResponse {
    approval_txns: Option<Vec<AcrossTransaction>>,
    swap_tx: Option<AcrossTransaction>,
    input_amount: Option<String>,
    max_input_amount: Option<String>,
    expected_output_amount: Option<String>,
    min_output_amount: Option<String>,
    quote_expiry_timestamp: Option<i64>,
    id: Option<String>,
}

impl AcrossSwapApprovalResponse {
    fn assert_executable(&self, origin_chain_id: u64) -> TestResult<()> {
        let swap_tx = self
            .swap_tx
            .as_ref()
            .ok_or("Across response did not include swapTx")?;
        assert_eq!(
            swap_tx
                .chain_id
                .ok_or("Across swapTx did not include chainId")?,
            origin_chain_id,
            "Across swapTx chainId does not match origin chain"
        );
        assert!(
            !swap_tx.to.trim().is_empty(),
            "Across swapTx missing target address"
        );
        assert!(
            !swap_tx.data.trim().is_empty(),
            "Across swapTx missing calldata"
        );
        Ok(())
    }

    fn assert_not_expired(&self) -> TestResult<()> {
        let Some(expiry) = self.quote_expiry_timestamp else {
            return Ok(());
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| format!("system clock is before UNIX epoch: {err}"))?
            .as_secs() as i64;
        assert!(
            expiry > now + MIN_QUOTE_EXPIRY_BUFFER_SECS,
            "Across quote expires too soon: expiry={expiry}, now={now}, required buffer={MIN_QUOTE_EXPIRY_BUFFER_SECS}s"
        );
        Ok(())
    }

    fn assert_minimum_output(&self, required_min_output: U256) -> TestResult<()> {
        let min_output = self
            .min_output_amount
            .as_deref()
            .or(self.expected_output_amount.as_deref())
            .ok_or("Across response did not include minOutputAmount or expectedOutputAmount")?;
        let min_output = parse_u256_decimal("Across min output", min_output)?;
        assert!(
            min_output >= required_min_output,
            "Across quote min output {min_output} is below ROUTER_LIVE_MIN_ACROSS_OUTPUT_AMOUNT {required_min_output}"
        );
        Ok(())
    }

    fn assert_max_input_amount(&self, allowed_max_input: U256) -> TestResult<()> {
        let max_input = self
            .max_input_amount
            .as_deref()
            .or(self.input_amount.as_deref())
            .ok_or("Across response did not include maxInputAmount or inputAmount")?;
        let max_input = parse_u256_decimal("Across max input", max_input)?;
        assert!(
            max_input <= allowed_max_input,
            "Across quote max input {max_input} exceeds ROUTER_LIVE_MAX_AMOUNT_IN {allowed_max_input}"
        );
        Ok(())
    }

    fn assert_approval_policy(&self, allow_approval_txns: bool) -> TestResult<()> {
        let approval_count = self
            .approval_txns
            .as_deref()
            .map_or(0, <[AcrossTransaction]>::len);
        assert!(
            allow_approval_txns || approval_count == 0,
            "Across returned {approval_count} approval transaction(s). Set ROUTER_LIVE_ALLOW_APPROVAL_TXNS=true only when using a throwaway wallet or after manually reviewing the allowance."
        );
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct AcrossTransaction {
    chain_id: Option<u64>,
    to: String,
    data: String,
    value: Option<Value>,
    gas: Option<Value>,
    max_fee_per_gas: Option<Value>,
    max_priority_fee_per_gas: Option<Value>,
}

async fn execute_across_quote<P>(
    provider: &P,
    quote: &AcrossSwapApprovalResponse,
    expected_chain_id: u64,
) -> TestResult<Vec<String>>
where
    P: Provider,
{
    let mut tx_hashes = Vec::new();
    for tx in quote.approval_txns.as_deref().unwrap_or_default() {
        tx_hashes.push(send_provider_transaction(provider, tx, expected_chain_id).await?);
    }
    let swap_tx = quote
        .swap_tx
        .as_ref()
        .ok_or("Across response did not include swapTx")?;
    tx_hashes.push(send_provider_transaction(provider, swap_tx, expected_chain_id).await?);
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
    assert_eq!(
        tx.chain_id
            .ok_or("Across transaction did not include chainId")?,
        expected_chain_id,
        "Across transaction chainId does not match origin chain"
    );
    let to = Address::from_str(&tx.to)?;
    let data = hex_to_bytes(&tx.data)?;
    assert!(!data.is_empty(), "Across transaction missing calldata");
    let mut request = TransactionRequest::default()
        .with_to(to)
        .with_input(data)
        .with_value(parse_optional_u256(&tx.value)?.unwrap_or(U256::ZERO));
    if let Some(gas_limit) = parse_optional_u64(&tx.gas)?.filter(|gas_limit| *gas_limit > 0) {
        request = request.with_gas_limit(gas_limit);
    }
    if let Some(max_fee_per_gas) =
        parse_optional_u128(&tx.max_fee_per_gas)?.filter(|max_fee_per_gas| *max_fee_per_gas > 0)
    {
        request = request.with_max_fee_per_gas(max_fee_per_gas);
    }
    if let Some(max_priority_fee_per_gas) = parse_optional_u128(&tx.max_priority_fee_per_gas)?
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
    eprintln!("submitted live E2E transaction {tx_hash}");
    wait_for_successful_receipt(provider, tx_hash, "live E2E").await?;
    Ok(tx_hash.to_string())
}

#[derive(Clone)]
struct UnitClient {
    http: reqwest::Client,
    base_url: Url,
}

impl UnitClient {
    fn new(base_url: Url) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url,
        }
    }

    async fn generate_address(
        &self,
        src_chain: &str,
        dst_chain: &str,
        asset: &str,
        dst_addr: &str,
    ) -> TestResult<UnitGenerateAddressResponse> {
        let url = self.endpoint(&format!(
            "/gen/{}/{}/{}/{}",
            clean_path_segment(src_chain),
            clean_path_segment(dst_chain),
            clean_path_segment(asset),
            clean_path_segment(dst_addr)
        ))?;
        let response = self.http.get(url).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("Unit generate-address HTTP {status}: {body}").into());
        }
        serde_json::from_str(&body).map_err(|err| {
            format!("Unit generate-address returned invalid JSON: {err}; body={body}").into()
        })
    }

    async fn operations(&self, address: &str) -> TestResult<UnitOperationsResponse> {
        let url = self.endpoint(&format!("/operations/{}", clean_path_segment(address)))?;
        let response = self.http.get(url).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(format!("Unit operations HTTP {status}: {body}").into());
        }
        serde_json::from_str(&body).map_err(|err| {
            format!("Unit operations returned invalid JSON: {err}; body={body}").into()
        })
    }

    async fn operation_fingerprints(
        &self,
        query_address: &str,
        protocol_address: &str,
    ) -> TestResult<BTreeSet<String>> {
        let operations = self.operations(query_address).await?;
        Ok(operations
            .operations
            .iter()
            .filter(|operation| operation.matches_protocol_address(protocol_address))
            .flat_map(UnitOperation::fingerprints)
            .collect())
    }

    async fn wait_for_done_operation(
        &self,
        query_address: &str,
        protocol_address: &str,
        label: &str,
        poll_interval: Duration,
        timeout: Duration,
        seen_operations: &BTreeSet<String>,
    ) -> TestResult<UnitOperation> {
        let deadline = Instant::now() + timeout;
        loop {
            let operations = self.operations(query_address).await?;
            if let Some(operation) = operations.operations.iter().find(|operation| {
                operation.matches_protocol_address(protocol_address)
                    && !operation.has_seen_fingerprint(seen_operations)
            }) {
                eprintln!(
                    "{label}: Unit operation state={} source_tx={:?} destination_tx={:?}",
                    operation.state.as_deref().unwrap_or("<missing>"),
                    operation.source_tx_hash,
                    operation.destination_tx_hash
                );
                match operation.state.as_deref() {
                    Some("done") => return Ok(operation.clone()),
                    Some("failure") => {
                        return Err(format!("{label}: Unit operation failed: {operation:?}").into())
                    }
                    _ => {}
                }
            } else {
                eprintln!(
                    "{label}: no new Unit operation yet for protocol address {protocol_address}"
                );
            }

            if Instant::now() >= deadline {
                return Err(format!(
                    "{label}: timed out waiting for Unit operation on protocol address {protocol_address}"
                )
                .into());
            }
            tokio::time::sleep(poll_interval).await;
        }
    }

    fn endpoint(&self, path: &str) -> TestResult<Url> {
        let base = self.base_url.as_str().trim_end_matches('/');
        Ok(Url::parse(&format!("{base}{path}"))?)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[allow(dead_code)]
struct UnitGenerateAddressResponse {
    address: String,
    status: Option<String>,
    #[serde(default)]
    signatures: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(dead_code)]
struct UnitOperationsResponse {
    #[serde(default)]
    addresses: Vec<Value>,
    #[serde(default)]
    operations: Vec<UnitOperation>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct UnitOperation {
    op_created_at: Option<String>,
    operation_id: Option<String>,
    protocol_address: Option<String>,
    source_address: Option<String>,
    destination_address: Option<String>,
    source_chain: Option<String>,
    destination_chain: Option<String>,
    source_amount: Option<String>,
    destination_fee_amount: Option<String>,
    sweep_fee_amount: Option<String>,
    state: Option<String>,
    source_tx_hash: Option<String>,
    destination_tx_hash: Option<String>,
    asset: Option<String>,
}

impl UnitOperation {
    fn matches_protocol_address(&self, protocol_address: &str) -> bool {
        self.protocol_address
            .as_deref()
            .is_some_and(|candidate| candidate.eq_ignore_ascii_case(protocol_address))
    }

    fn fingerprints(&self) -> Vec<String> {
        let mut fingerprints = Vec::new();
        push_fingerprint(
            &mut fingerprints,
            "operationId",
            self.operation_id.as_deref(),
        );
        push_fingerprint(
            &mut fingerprints,
            "sourceTxHash",
            self.source_tx_hash.as_deref(),
        );
        push_fingerprint(
            &mut fingerprints,
            "destinationTxHash",
            self.destination_tx_hash.as_deref(),
        );
        push_fingerprint(
            &mut fingerprints,
            "opCreatedAt",
            self.op_created_at.as_deref(),
        );
        fingerprints
    }

    fn has_seen_fingerprint(&self, seen_operations: &BTreeSet<String>) -> bool {
        self.fingerprints()
            .iter()
            .any(|fingerprint| seen_operations.contains(fingerprint))
    }
}

fn push_fingerprint(fingerprints: &mut Vec<String>, field: &str, value: Option<&str>) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    fingerprints.push(format!("{field}:{value}"));
}

async fn run_hyperliquid_helper(
    config: &LiveRouteConfig,
    unit_withdraw_address: &str,
) -> TestResult<Value> {
    let tempdir = tempfile::tempdir()?;
    let helper_path = tempdir.path().join("hyperliquid_live_helper.py");
    std::fs::write(&helper_path, HYPERLIQUID_HELPER_PY)?;

    let payload = json!({
        "base_url": config.hyperliquid_api_url.as_str().trim_end_matches('/'),
        "private_key": config.hyperliquid_private_key,
        "account_address": config.hyperliquid_account_address.map(|address| address.to_string()),
        "vault_address": config.hyperliquid_vault_address.map(|address| address.to_string()),
        "balance_user": config.hyperliquid_vault_address.unwrap_or(config.hyperliquid_destination_address).to_string(),
        "trade_plan": config.hyperliquid_trade_plan,
        "withdraw": {
            "destination": unit_withdraw_address,
            "token_symbol": config.hyperliquid_withdraw_token_symbol,
            "amount": config.hyperliquid_withdraw_amount
        }
    });

    let mut child = Command::new(&config.hyperliquid_python)
        .arg(&helper_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or("failed to open Hyperliquid helper stdin")?;
        stdin
            .write_all(serde_json::to_string(&payload)?.as_bytes())
            .await?;
    }
    let output = child.wait_with_output().await?;
    if !output.status.success() {
        return Err(format!(
            "Hyperliquid helper failed with status {}:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    serde_json::from_slice(&output.stdout).map_err(|err| {
        format!(
            "Hyperliquid helper returned invalid JSON: {err}; stdout={}; stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into()
    })
}

fn read_required_env(key: &'static str) -> TestResult<String> {
    env::var(key)
        .map(|value| value.trim().to_string())
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing required env var {key}").into())
}

fn read_optional_env(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn read_env_with_default(key: &'static str, default: &'static str) -> String {
    read_optional_env(key).unwrap_or_else(|| default.to_string())
}

fn read_url_env(key: &'static str) -> TestResult<Url> {
    let value = read_required_env(key)?;
    assert_url(key, &value)
}

fn read_url_env_with_default(key: &'static str, default: &'static str) -> TestResult<Url> {
    let value = read_env_with_default(key, default);
    assert_url(key, &value)
}

fn read_u64_env(key: &'static str) -> TestResult<u64> {
    let value = read_required_env(key)?;
    value
        .parse::<u64>()
        .map_err(|err| format!("{key} must be a u64: {err}").into())
}

fn read_u64_env_with_default(key: &'static str, default: u64) -> TestResult<u64> {
    match read_optional_env(key) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|err| format!("{key} must be a u64: {err}").into()),
        None => Ok(default),
    }
}

fn read_bool_env_with_default(key: &'static str, default: bool) -> TestResult<bool> {
    match read_optional_env(key) {
        Some(value) => match value.as_str() {
            "1" | "true" | "TRUE" | "True" => Ok(true),
            "0" | "false" | "FALSE" | "False" => Ok(false),
            _ => Err(format!("{key} must be true/false or 1/0").into()),
        },
        None => Ok(default),
    }
}

fn read_address_env(key: &'static str) -> TestResult<Address> {
    let value = read_required_env(key)?;
    Address::from_str(&value).map_err(|err| format!("{key} must be an EVM address: {err}").into())
}

fn read_optional_address_env(key: &'static str) -> TestResult<Option<Address>> {
    match read_optional_env(key) {
        Some(value) => Address::from_str(&value)
            .map(Some)
            .map_err(|err| format!("{key} must be an EVM address: {err}").into()),
        None => Ok(None),
    }
}

fn read_u256_env(key: &'static str) -> TestResult<U256> {
    let value = read_required_env(key)?;
    parse_u256_decimal(key, &value)
}

fn read_json_env(key: &'static str) -> TestResult<Value> {
    let value = read_required_env(key)?;
    serde_json::from_str(&value).map_err(|err| format!("{key} must be valid JSON: {err}").into())
}

fn read_json_query_map_env(key: &'static str) -> TestResult<BTreeMap<String, String>> {
    let Some(value) = read_optional_env(key) else {
        return Ok(BTreeMap::new());
    };
    let json: Value =
        serde_json::from_str(&value).map_err(|err| format!("{key} must be valid JSON: {err}"))?;
    let object = json
        .as_object()
        .ok_or_else(|| format!("{key} must be a JSON object"))?;
    let mut map = BTreeMap::new();
    for (query_key, value) in object {
        map.insert(query_key.clone(), json_query_value_to_string(value)?);
    }
    Ok(map)
}

fn json_query_value_to_string(value: &Value) -> TestResult<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => Err("query parameter values cannot be null".into()),
        Value::Array(_) | Value::Object(_) => Err("query parameter values must be scalar".into()),
    }
}

fn assert_url(key: &str, value: &str) -> TestResult<Url> {
    let parsed = Url::parse(value).map_err(|err| format!("{key} must be a URL: {err}"))?;
    assert!(
        matches!(parsed.scheme(), "http" | "https"),
        "{key} must use http or https"
    );
    Ok(parsed)
}

fn assert_hex_private_key(value: &str) -> TestResult<()> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    assert_eq!(hex.len(), 64, "private keys must be 32-byte hex strings");
    assert!(
        hex.chars().all(|ch| ch.is_ascii_hexdigit()),
        "private keys must be hex"
    );
    Ok(())
}

fn assert_decimal_amount(key: &str, value: &str) -> TestResult<()> {
    let mut dot_count = 0;
    let normalized: String = value
        .chars()
        .filter(|ch| {
            if *ch == '.' {
                dot_count += 1;
                false
            } else {
                true
            }
        })
        .collect();
    assert!(dot_count <= 1, "{key} must be a decimal amount");
    assert!(
        !normalized.is_empty() && normalized.chars().all(|ch| ch.is_ascii_digit()),
        "{key} must be a positive decimal amount"
    );
    assert!(
        normalized.chars().any(|ch| ch != '0'),
        "{key} must be nonzero"
    );
    Ok(())
}

fn assert_across_slippage(value: &str) -> TestResult<()> {
    if value == "auto" {
        return Ok(());
    }
    let slippage = value
        .parse::<f64>()
        .map_err(|err| format!("ROUTER_LIVE_ACROSS_SLIPPAGE must be auto or a number: {err}"))?;
    assert!(
        (0.0..=1.0).contains(&slippage),
        "ROUTER_LIVE_ACROSS_SLIPPAGE must be between 0 and 1"
    );
    Ok(())
}

fn parse_u256_decimal(key: &str, value: &str) -> TestResult<U256> {
    assert!(
        value.chars().all(|ch| ch.is_ascii_digit()),
        "{key} must be an integer amount in base units"
    );
    assert_ne!(value, "0", "{key} must be nonzero");
    U256::from_str_radix(value, 10).map_err(|err| format!("{key} is not a U256: {err}").into())
}

fn parse_optional_u256(value: &Option<Value>) -> TestResult<Option<U256>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => json_value_to_u256(value).map(Some),
    }
}

fn parse_optional_u64(value: &Option<Value>) -> TestResult<Option<u64>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => json_value_to_u64(value).map(Some),
    }
}

fn parse_optional_u128(value: &Option<Value>) -> TestResult<Option<u128>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(value) => json_value_to_u128(value).map(Some),
    }
}

fn json_value_to_u256(value: &Value) -> TestResult<U256> {
    let string = json_numberish_to_string(value)?;
    if let Some(hex) = string.strip_prefix("0x") {
        U256::from_str_radix(hex, 16)
            .map_err(|err| format!("invalid hex U256 {string}: {err}").into())
    } else {
        U256::from_str_radix(&string, 10)
            .map_err(|err| format!("invalid decimal U256 {string}: {err}").into())
    }
}

fn json_value_to_u64(value: &Value) -> TestResult<u64> {
    let string = json_numberish_to_string(value)?;
    if let Some(hex) = string.strip_prefix("0x") {
        u64::from_str_radix(hex, 16)
            .map_err(|err| format!("invalid hex u64 {string}: {err}").into())
    } else {
        string
            .parse::<u64>()
            .map_err(|err| format!("invalid decimal u64 {string}: {err}").into())
    }
}

fn json_value_to_u128(value: &Value) -> TestResult<u128> {
    let string = json_numberish_to_string(value)?;
    if let Some(hex) = string.strip_prefix("0x") {
        u128::from_str_radix(hex, 16)
            .map_err(|err| format!("invalid hex u128 {string}: {err}").into())
    } else {
        string
            .parse::<u128>()
            .map_err(|err| format!("invalid decimal u128 {string}: {err}").into())
    }
}

fn json_numberish_to_string(value: &Value) -> TestResult<String> {
    match value {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        Value::Null => Err("expected numeric value, got null".into()),
        _ => Err(format!("expected scalar numeric value, got {value}").into()),
    }
}

fn hex_to_bytes(value: &str) -> TestResult<Bytes> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    Ok(Bytes::from(hex::decode(hex)?))
}

fn clean_path_segment(segment: &str) -> String {
    segment.trim().trim_matches('/').to_string()
}

impl fmt::Display for AcrossTransaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "to={} chain={:?}", self.to, self.chain_id)
    }
}

const HYPERLIQUID_HELPER_PY: &str = r#"
import json
import sys
import time

try:
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
except Exception as exc:
    print(
        "missing Hyperliquid Python SDK. Install it with: python3 -m pip install hyperliquid-python-sdk. "
        f"Import error: {exc}",
        file=sys.stderr,
    )
    sys.exit(78)


def token_wire(info, symbol):
    wanted = symbol.upper()
    meta = info.spot_meta()
    for token in meta.get("tokens", []):
        if token.get("name", "").upper() == wanted:
            return f"{token['name']}:{token['tokenId']}"
    raise RuntimeError(f"token {symbol!r} was not found in Hyperliquid spot metadata")


def spot_balances(info, user):
    state = info.spot_user_state(user)
    balances = {}
    for balance in state.get("balances", []):
        token = balance.get("coin") or balance.get("token")
        if token:
            balances[token] = balance
    return balances


def as_float(value):
    if isinstance(value, (int, float)):
        return float(value)
    return float(str(value))


payload = json.loads(sys.stdin.read())
base_url = payload["base_url"].rstrip("/")
wallet = Account.from_key(payload["private_key"])
account_address = payload.get("account_address")
vault_address = payload.get("vault_address")
balance_user = payload.get("balance_user") or vault_address or account_address or wallet.address

info = Info(base_url=base_url, skip_ws=True)
exchange = Exchange(
    wallet=wallet,
    base_url=base_url,
    account_address=account_address,
    vault_address=vault_address,
)

results = {
    "wallet_address": wallet.address,
    "balance_user": balance_user,
    "before": spot_balances(info, balance_user),
    "actions": [],
}

for action in payload["trade_plan"]:
    kind = action.get("kind", "market_open")
    if kind != "market_open":
        raise RuntimeError(f"unsupported Hyperliquid trade action kind {kind!r}")
    result = exchange.market_open(
        name=action["coin"],
        is_buy=bool(action["is_buy"]),
        sz=as_float(action["size"]),
        slippage=as_float(action.get("slippage", 0.05)),
    )
    results["actions"].append({"request": action, "response": result})
    if result.get("status") != "ok":
        raise RuntimeError(f"Hyperliquid market_open failed: {result}")
    wait_secs = as_float(action.get("wait_secs", 3))
    if wait_secs > 0:
        time.sleep(wait_secs)

withdraw = payload["withdraw"]
withdraw_token = token_wire(info, withdraw["token_symbol"])
if vault_address:
    withdraw_result = exchange.send_asset(
        destination=withdraw["destination"],
        source_dex="spot",
        destination_dex="spot",
        token=withdraw_token,
        amount=as_float(withdraw["amount"]),
    )
else:
    withdraw_result = exchange.spot_transfer(
        amount=as_float(withdraw["amount"]),
        destination=withdraw["destination"],
        token=withdraw_token,
    )
results["withdraw"] = {
    "request": {
        "destination": withdraw["destination"],
        "token": withdraw_token,
        "amount": withdraw["amount"],
        "from_subaccount": vault_address,
    },
    "response": withdraw_result,
}
if withdraw_result.get("status") != "ok":
    raise RuntimeError(f"Hyperliquid spot_transfer failed: {withdraw_result}")

time.sleep(3)
results["after"] = spot_balances(info, balance_user)
print(json.dumps(results, default=str))
"#;
