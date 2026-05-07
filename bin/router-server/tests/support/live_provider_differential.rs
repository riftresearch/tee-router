#![allow(dead_code)]

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::{
    primitives::{address, Address, U256},
    signers::local::PrivateKeySigner,
};
use devnet::mock_integrators::{MockIntegratorConfig, MockIntegratorServer};
use hyperunit_client::{
    HyperUnitClient, UnitAsset, UnitChain, UnitGenerateAddressRequest, UnitGenerateAddressResponse,
    UnitOperation, UnitOperationsRequest, UnitOperationsResponse,
};
use router_server::{
    models::MarketOrderKind,
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        across_client::{AcrossClient, AcrossSwapApprovalRequest, AcrossSwapApprovalResponse},
        action_providers::{
            BridgeProvider, BridgeQuote, BridgeQuoteRequest, CctpProvider, ExchangeProvider,
            ExchangeQuote, ExchangeQuoteRequest, HyperliquidProvider, VeloraProvider,
        },
        asset_registry::AssetRegistry,
        custody_action_executor::HyperliquidCallNetwork,
    },
};
use serde::Serialize;
use serde_json::{json, Value};
use url::Url;

use super::assert_raw_amount_string;

pub type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_PROVIDER_TESTS: &str = "LIVE_PROVIDER_TESTS";
const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";

const ACROSS_BASE_URL: &str = "ACROSS_LIVE_BASE_URL";
const ACROSS_API_KEY: &str = "ACROSS_LIVE_API_KEY";
const ACROSS_INTEGRATOR_ID: &str = "ACROSS_LIVE_INTEGRATOR_ID";
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

const CCTP_API_URL: &str = "CCTP_API_URL";
const VELORA_BASE_URL: &str = "VELORA_LIVE_BASE_URL";
const VELORA_API_URL: &str = "VELORA_API_URL";
const VELORA_PARTNER: &str = "VELORA_LIVE_PARTNER";
const VELORA_AMOUNT_IN_WEI: &str = "VELORA_LIVE_BASE_ETH_AMOUNT_IN_WEI";
const HYPERLIQUID_API_URL_ENV: &str = "HYPERLIQUID_API_URL";
const HYPERUNIT_BASE_URL: &str = "HYPERUNIT_LIVE_BASE_URL";
const HYPERUNIT_FALLBACK_BASE_URL: &str = "HYPERUNIT_API_URL";
const HYPERUNIT_PROXY_URL: &str = "HYPERUNIT_LIVE_PROXY_URL";
const HYPERUNIT_FALLBACK_PROXY_URL: &str = "HYPERUNIT_PROXY_URL";

const DEFAULT_ACROSS_BASE_URL: &str = "https://app.across.to/api";
const DEFAULT_CCTP_API_URL: &str = "https://iris-api.circle.com";
const DEFAULT_VELORA_BASE_URL: &str = "https://api.paraswap.io";
const DEFAULT_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_HYPERUNIT_BASE_URL: &str = "https://api.hyperunit.xyz";

const DEFAULT_ACROSS_AMOUNT_RAW: &str = "10000000";
const DEFAULT_VELORA_AMOUNT_IN_WEI: &str = "10000000000000";
const DEFAULT_HYPERLIQUID_UETH_AMOUNT_IN_WEI: &str = "50000000000000000";
const DEFAULT_MOCK_SPOKE_POOL: &str = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
const DEFAULT_EVM_ADDRESS: &str = "0x3333333333333333333333333333333333333333";
const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

const BASE_CHAIN_ID: u64 = 8453;
const ARBITRUM_CHAIN_ID: u64 = 42161;
const BASE_DOMAIN: u32 = 6;
const ARBITRUM_DOMAIN: u32 = 3;
const BASE_USDC: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
const ARBITRUM_USDC: Address = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
const TOKEN_MESSENGER_V2: Address = address!("28b5a0e9c621a5badaa536219b3a228c8168cf5d");
const MESSAGE_TRANSMITTER_V2: Address = address!("81d40f21f12a8f0e3252bccb954d722d4c464b64");
const NATIVE_TOKEN_FOR_VELORA: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

pub async fn live_vs_mock_across_swap_approval_contract() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let api_key = required_env_any(&[ACROSS_API_KEY, "ACROSS_API_KEY"])?;
    let integrator_id = env_var_any(&[ACROSS_INTEGRATOR_ID, "ACROSS_INTEGRATOR_ID"])
        .unwrap_or_else(|| "0x010e".to_string());
    let request = configured_across_request(&integrator_id)?;

    let live_client = AcrossClient::new(
        env_var_any(&[ACROSS_BASE_URL]).unwrap_or_else(|| DEFAULT_ACROSS_BASE_URL.to_string()),
        api_key.clone(),
    )?;
    let live_response = live_client.swap_approval(request.clone()).await?;
    let live_raw = fetch_raw_across_swap_approval(&live_client, &request).await?;
    let live_contract =
        assert_across_swap_approval_contract("live Across", &request, &live_response, &live_raw)?;

    let mock = MockIntegratorServer::spawn_with_config(
        MockIntegratorConfig::default()
            .with_across_spoke_pool_address(DEFAULT_MOCK_SPOKE_POOL)
            .with_across_auth(api_key.clone(), integrator_id.clone()),
    )
    .await?;
    let mock_client = AcrossClient::new(mock.base_url(), api_key)?;
    let mock_response = mock_client.swap_approval(request.clone()).await?;
    let mock_raw = fetch_raw_across_swap_approval(&mock_client, &request).await?;
    let mock_contract =
        assert_across_swap_approval_contract("mock Across", &request, &mock_response, &mock_raw)?;

    assert_eq!(
        live_contract, mock_contract,
        "Across mock contract drifted from live API contract"
    );
    write_differential_artifact(
        "across",
        json!({
            "live_contract": live_contract,
            "mock_contract": mock_contract,
            "live_raw": live_raw,
            "mock_raw": mock_raw,
        }),
    );

    Ok(())
}

pub async fn live_vs_mock_cctp_quote_contract() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let amount = U256::from(100_000_u64);
    let request = cctp_quote_request(amount);
    let live_provider = cctp_provider(
        env_var_any(&[CCTP_API_URL]).unwrap_or_else(|| DEFAULT_CCTP_API_URL.to_string()),
    )?;
    let live_quote = live_provider
        .quote_bridge(request.clone())
        .await?
        .ok_or("live CCTP quote returned None")?;
    let live_contract = assert_cctp_quote_contract("live CCTP", &live_quote, &request)?;
    let live_iris_contract = assert_iris_zero_hash_contract(
        "live Iris",
        &env_var_any(&[CCTP_API_URL]).unwrap_or_else(|| DEFAULT_CCTP_API_URL.to_string()),
    )
    .await?;

    let mock = MockIntegratorServer::spawn().await?;
    let mock_provider = cctp_provider(mock.base_url())?;
    let mock_quote = mock_provider
        .quote_bridge(request.clone())
        .await?
        .ok_or("mock CCTP quote returned None")?;
    let mock_contract = assert_cctp_quote_contract("mock CCTP", &mock_quote, &request)?;
    let mock_iris_contract = assert_iris_zero_hash_contract("mock Iris", mock.base_url()).await?;

    assert_eq!(
        live_contract, mock_contract,
        "CCTP mock quote contract drifted from live provider contract"
    );
    assert_eq!(
        live_iris_contract, mock_iris_contract,
        "CCTP mock Iris not-found contract drifted from live Iris"
    );
    write_differential_artifact(
        "cctp",
        json!({
            "live_contract": live_contract,
            "mock_contract": mock_contract,
            "live_iris_contract": live_iris_contract,
            "mock_iris_contract": mock_iris_contract,
        }),
    );

    Ok(())
}

pub async fn live_vs_mock_velora_quote_contract() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let user = configured_evm_address()?;
    let amount_in = U256::from_str_radix(
        &env_var_any(&[VELORA_AMOUNT_IN_WEI])
            .unwrap_or_else(|| DEFAULT_VELORA_AMOUNT_IN_WEI.to_string()),
        10,
    )?;
    let request = ExchangeQuoteRequest {
        input_asset: deposit_asset("evm:8453", AssetId::Native),
        output_asset: deposit_asset("evm:8453", AssetId::reference(format!("{BASE_USDC:#x}"))),
        input_decimals: Some(18),
        output_decimals: Some(6),
        order_kind: MarketOrderKind::ExactIn {
            amount_in: amount_in.to_string(),
            min_amount_out: Some("1".to_string()),
        },
        sender_address: Some(format!("{user:#x}")),
        recipient_address: format!("{user:#x}"),
    };

    let live_base_url = live_velora_base_url();
    let live_provider = VeloraProvider::new(live_base_url.clone(), env_var_any(&[VELORA_PARTNER]))?;
    let live_quote = live_provider
        .quote_trade(request.clone())
        .await?
        .ok_or("live Velora quote returned None")?;
    let live_price_route = fetch_velora_price_route(&live_base_url, user, amount_in).await?;
    let live_contract =
        assert_velora_quote_contract("live Velora", &live_quote, &live_price_route, &request)?;

    let mock = MockIntegratorServer::spawn_with_config(
        MockIntegratorConfig::default()
            .with_velora_swap_contract_address(BASE_CHAIN_ID, DEFAULT_EVM_ADDRESS),
    )
    .await?;
    let mock_provider = VeloraProvider::new(mock.base_url(), None)?;
    let mock_quote = mock_provider
        .quote_trade(request.clone())
        .await?
        .ok_or("mock Velora quote returned None")?;
    let mock_price_route = fetch_velora_price_route(mock.base_url(), user, amount_in).await?;
    let mock_contract =
        assert_velora_quote_contract("mock Velora", &mock_quote, &mock_price_route, &request)?;

    assert_eq!(
        live_contract, mock_contract,
        "Velora mock quote contract drifted from live provider contract"
    );
    write_differential_artifact(
        "velora",
        json!({
            "live_contract": live_contract,
            "mock_contract": mock_contract,
            "live_price_route": live_price_route,
            "mock_price_route": mock_price_route,
        }),
    );

    Ok(())
}

pub async fn live_vs_mock_hyperliquid_quote_contract() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let amount_in = env_var_any(&["ROUTER_LIVE_ETH_AMOUNT_IN_WEI"])
        .unwrap_or_else(|| DEFAULT_HYPERLIQUID_UETH_AMOUNT_IN_WEI.to_string());
    let request = ExchangeQuoteRequest {
        input_asset: deposit_asset("hyperliquid", AssetId::reference("UETH")),
        output_asset: deposit_asset("hyperliquid", AssetId::reference("UBTC")),
        input_decimals: None,
        output_decimals: None,
        order_kind: MarketOrderKind::ExactIn {
            amount_in: amount_in.clone(),
            min_amount_out: Some("1".to_string()),
        },
        sender_address: None,
        recipient_address: DEFAULT_EVM_ADDRESS.to_string(),
    };

    let live_provider = HyperliquidProvider::new(
        env_var_any(&[HYPERLIQUID_API_URL_ENV])
            .unwrap_or_else(|| DEFAULT_HYPERLIQUID_API_URL.to_string()),
        HyperliquidCallNetwork::Mainnet,
        Arc::new(AssetRegistry::default()),
        30_000,
    )?;
    let live_quote = live_provider
        .quote_trade(request.clone())
        .await?
        .ok_or("live Hyperliquid quote returned None")?;
    let live_contract =
        assert_hyperliquid_quote_contract("live Hyperliquid", &live_quote, &request)?;

    let mock = MockIntegratorServer::spawn_with_config(
        MockIntegratorConfig::default().with_mainnet_hyperliquid(true),
    )
    .await?;
    let mock_provider = HyperliquidProvider::new(
        mock.base_url(),
        HyperliquidCallNetwork::Mainnet,
        Arc::new(AssetRegistry::default()),
        30_000,
    )?;
    let mock_quote = mock_provider
        .quote_trade(request.clone())
        .await?
        .ok_or("mock Hyperliquid quote returned None")?;
    let mock_contract =
        assert_hyperliquid_quote_contract("mock Hyperliquid", &mock_quote, &request)?;

    assert_eq!(
        live_contract, mock_contract,
        "Hyperliquid mock quote contract drifted from live provider contract"
    );
    write_differential_artifact(
        "hyperliquid",
        json!({
            "live_contract": live_contract,
            "mock_contract": mock_contract,
        }),
    );

    Ok(())
}

pub async fn live_vs_mock_hyperunit_generate_address_contract() -> TestResult<()> {
    if !live_enabled() {
        return Ok(());
    }

    let user = configured_evm_address()?;
    let request = UnitGenerateAddressRequest {
        src_chain: UnitChain::Ethereum,
        dst_chain: UnitChain::Hyperliquid,
        asset: UnitAsset::Eth,
        dst_addr: format!("{user:#x}"),
    };

    let live_client = HyperUnitClient::new_with_proxy_url(
        env_var_any(&[HYPERUNIT_BASE_URL, HYPERUNIT_FALLBACK_BASE_URL])
            .unwrap_or_else(|| DEFAULT_HYPERUNIT_BASE_URL.to_string()),
        env_var_any(&[HYPERUNIT_PROXY_URL, HYPERUNIT_FALLBACK_PROXY_URL]),
    )?;
    let live_generated = live_client.generate_address(request.clone()).await?;
    let live_operations = live_client
        .operations(UnitOperationsRequest {
            address: live_generated.address.clone(),
        })
        .await?;
    let live_generate_contract =
        assert_hyperunit_generate_contract("live HyperUnit", &live_generated, &request)?;
    let live_operations_contract =
        assert_hyperunit_operations_contract("live HyperUnit", &live_operations)?;

    let mock = MockIntegratorServer::spawn().await?;
    let mock_client = HyperUnitClient::new(mock.base_url())?;
    let mock_generated = mock_client.generate_address(request.clone()).await?;
    let mock_operations = mock_client
        .operations(UnitOperationsRequest {
            address: mock_generated.address.clone(),
        })
        .await?;
    let mock_generate_contract =
        assert_hyperunit_generate_contract("mock HyperUnit", &mock_generated, &request)?;
    let mock_operations_contract =
        assert_hyperunit_operations_contract("mock HyperUnit", &mock_operations)?;

    assert_eq!(
        live_generate_contract, mock_generate_contract,
        "HyperUnit mock /gen contract drifted from live provider contract"
    );
    assert_eq!(
        live_operations_contract.schema_contract, mock_operations_contract.schema_contract,
        "HyperUnit mock /operations schema contract drifted from live provider contract"
    );
    write_differential_artifact(
        "unit",
        json!({
            "live_generate_contract": live_generate_contract,
            "mock_generate_contract": mock_generate_contract,
            "live_operations_contract": live_operations_contract,
            "mock_operations_contract": mock_operations_contract,
            "live_generated_address": live_generated.address,
            "mock_generated_address": mock_generated.address,
        }),
    );

    Ok(())
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct AcrossApprovalContract {
    trade_type: String,
    origin_chain_id: u64,
    destination_chain_id: u64,
    amount_fields: Vec<&'static str>,
    has_swap_tx: bool,
    swap_tx_chain_id: u64,
    has_quote_expiry: bool,
    raw_amount_type: String,
    raw_has_checks: bool,
    raw_has_steps: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct BridgeContract {
    provider_id: String,
    kind: String,
    amount_in_passthrough: bool,
    amount_out_passthrough: bool,
    source_domain: u64,
    destination_domain: u64,
    max_fee: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct IrisNotFoundContract {
    source_domain: u32,
    zero_hash_not_found: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct ExchangeContract {
    provider_id: String,
    kind: String,
    order_kind: String,
    amount_in_matches_request: bool,
    min_amount_out_present: bool,
    max_amount_in_present: bool,
    leg_count: usize,
    first_input_asset: String,
    last_output_asset: String,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct VeloraContract {
    exchange: ExchangeContract,
    network: u64,
    src_token: String,
    dest_token: String,
    src_decimals: u64,
    dest_decimals: u64,
    price_route_amount_fields: Vec<&'static str>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct HyperUnitGenerateContract {
    src_chain: String,
    dst_chain: String,
    asset: String,
    destination_address_kind: String,
    generated_address_kind: String,
    status_ok_or_missing: bool,
    signatures_is_object: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct HyperUnitOperationsSchemaContract {
    addresses_is_array: bool,
    operations_is_array: bool,
    operation_amount_fields_are_decimal_compatible: bool,
    operation_tx_hashes_are_strings: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
struct HyperUnitOperationsContract {
    schema_contract: HyperUnitOperationsSchemaContract,
    operation_count: usize,
}

fn assert_across_swap_approval_contract(
    label: &str,
    request: &AcrossSwapApprovalRequest,
    response: &AcrossSwapApprovalResponse,
    raw: &Value,
) -> TestResult<AcrossApprovalContract> {
    let mut amount_fields = Vec::new();
    for (field, typed_value) in [
        ("inputAmount", response.input_amount.as_deref()),
        ("maxInputAmount", response.max_input_amount.as_deref()),
        (
            "expectedOutputAmount",
            response.expected_output_amount.as_deref(),
        ),
        ("minOutputAmount", response.min_output_amount.as_deref()),
    ] {
        let Some(value) = typed_value else {
            continue;
        };
        amount_fields.push(field);
        assert_raw_amount_string(&format!("{label} typed {field}"), value);
        if field == "inputAmount" && request.trade_type == "exactInput" {
            assert_eq!(
                value,
                request.amount.to_string(),
                "{label} typed inputAmount must equal exactInput request amount"
            );
        }
    }
    assert!(
        !amount_fields.is_empty(),
        "{label} typed response must include amount fields"
    );
    let swap_tx = response
        .swap_tx
        .as_ref()
        .ok_or_else(|| format!("{label} typed response missing swapTx"))?;
    let swap_tx_chain_id = swap_tx
        .chain_id
        .ok_or_else(|| format!("{label} swapTx missing chainId"))?;
    assert_eq!(
        swap_tx_chain_id, request.origin_chain_id,
        "{label} swapTx chainId must match origin chain"
    );
    assert_hex_string(&format!("{label} swapTx.data"), &swap_tx.data);
    assert_address_string(&format!("{label} swapTx.to"), &swap_tx.to);
    if let Some(expiry) = response.quote_expiry_timestamp {
        assert_future_timestamp(&format!("{label} typed quoteExpiryTimestamp"), expiry)?;
    }

    let raw_amount_type = required_string(raw, "amountType", label)?.to_string();
    assert_eq!(
        raw_amount_type, request.trade_type,
        "{label} raw amountType must match request tradeType"
    );
    for field in [
        "inputAmount",
        "maxInputAmount",
        "expectedOutputAmount",
        "minOutputAmount",
    ] {
        if let Some(value) = raw.get(field).and_then(Value::as_str) {
            assert_raw_amount_string(&format!("{label} raw {field}"), value);
        }
    }
    assert_eq!(
        required_string(raw, "inputAmount", label)?,
        request.amount.to_string(),
        "{label} raw inputAmount must equal exactInput request amount"
    );
    assert_future_timestamp(
        &format!("{label} raw quoteExpiryTimestamp"),
        raw.get("quoteExpiryTimestamp")
            .and_then(Value::as_i64)
            .ok_or_else(|| format!("{label} raw response missing quoteExpiryTimestamp"))?,
    )?;
    assert_eq!(
        raw.pointer("/inputToken/chainId")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("{label} raw inputToken.chainId missing"))?,
        request.origin_chain_id,
        "{label} raw input token chain mismatch"
    );
    assert_eq!(
        raw.pointer("/outputToken/chainId")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("{label} raw outputToken.chainId missing"))?,
        request.destination_chain_id,
        "{label} raw output token chain mismatch"
    );
    assert_eq!(
        required_string_at(raw, "/inputToken/address", label)?.to_ascii_lowercase(),
        format!("{:#x}", request.input_token).to_ascii_lowercase(),
        "{label} raw input token address mismatch"
    );
    assert_eq!(
        required_string_at(raw, "/outputToken/address", label)?.to_ascii_lowercase(),
        format!("{:#x}", request.output_token).to_ascii_lowercase(),
        "{label} raw output token address mismatch"
    );
    assert_eq!(
        raw.pointer("/swapTx/chainId")
            .and_then(Value::as_u64)
            .ok_or_else(|| format!("{label} raw swapTx.chainId missing"))?,
        request.origin_chain_id,
        "{label} raw swapTx.chainId mismatch"
    );
    assert_hex_string(
        &format!("{label} raw swapTx.data"),
        required_string_at(raw, "/swapTx/data", label)?,
    );
    assert_address_string(
        &format!("{label} raw swapTx.to"),
        required_string_at(raw, "/swapTx/to", label)?,
    );

    Ok(AcrossApprovalContract {
        trade_type: request.trade_type.clone(),
        origin_chain_id: request.origin_chain_id,
        destination_chain_id: request.destination_chain_id,
        amount_fields,
        has_swap_tx: response.swap_tx.is_some(),
        swap_tx_chain_id,
        has_quote_expiry: response.quote_expiry_timestamp.is_some(),
        raw_amount_type,
        raw_has_checks: raw.get("checks").is_some_and(Value::is_object),
        raw_has_steps: raw.get("steps").is_some_and(Value::is_object),
    })
}

fn assert_cctp_quote_contract(
    label: &str,
    quote: &BridgeQuote,
    request: &BridgeQuoteRequest,
) -> TestResult<BridgeContract> {
    assert_eq!(quote.provider_id, "cctp", "{label} provider_id");
    let request_amount = match &request.order_kind {
        MarketOrderKind::ExactIn { amount_in, .. } => amount_in.as_str(),
        MarketOrderKind::ExactOut { .. } => {
            return Err(format!("{label} CCTP differential expects exact-in request").into())
        }
    };
    assert_eq!(quote.amount_in, request_amount, "{label} amount_in");
    assert_eq!(quote.amount_out, request_amount, "{label} amount_out");
    assert_raw_amount_string(&format!("{label} amount_in"), &quote.amount_in);
    assert_raw_amount_string(&format!("{label} amount_out"), &quote.amount_out);
    assert_eq!(
        quote.provider_quote["source_domain"],
        json!(BASE_DOMAIN),
        "{label} source domain"
    );
    assert_eq!(
        quote.provider_quote["destination_domain"],
        json!(ARBITRUM_DOMAIN),
        "{label} destination domain"
    );
    assert_eq!(
        quote.provider_quote["burn_token"],
        json!(format!("{BASE_USDC:#x}")),
        "{label} burn token"
    );
    assert_eq!(
        quote.provider_quote["destination_token"],
        json!(format!("{ARBITRUM_USDC:#x}")),
        "{label} destination token"
    );
    Ok(BridgeContract {
        provider_id: quote.provider_id.clone(),
        kind: required_string(&quote.provider_quote, "kind", label)?.to_string(),
        amount_in_passthrough: quote.amount_in == request_amount,
        amount_out_passthrough: quote.amount_out == request_amount,
        source_domain: quote.provider_quote["source_domain"]
            .as_u64()
            .ok_or_else(|| format!("{label} source_domain must be integer"))?,
        destination_domain: quote.provider_quote["destination_domain"]
            .as_u64()
            .ok_or_else(|| format!("{label} destination_domain must be integer"))?,
        max_fee: required_string(&quote.provider_quote, "max_fee", label)?.to_string(),
    })
}

async fn assert_iris_zero_hash_contract(
    label: &str,
    base_url: &str,
) -> TestResult<IrisNotFoundContract> {
    let mut url = Url::parse(base_url)?;
    url.set_path(&format!("/v2/messages/{BASE_DOMAIN}"));
    url.query_pairs_mut()
        .append_pair("transactionHash", ZERO_HASH);
    let response = reqwest::Client::new().get(url).send().await?;
    assert_eq!(
        response.status(),
        reqwest::StatusCode::NOT_FOUND,
        "{label} zero-hash probe should return 404/not found"
    );
    Ok(IrisNotFoundContract {
        source_domain: BASE_DOMAIN,
        zero_hash_not_found: true,
    })
}

fn assert_velora_quote_contract(
    label: &str,
    quote: &ExchangeQuote,
    price_route: &Value,
    request: &ExchangeQuoteRequest,
) -> TestResult<VeloraContract> {
    let exchange = assert_exchange_quote_contract(label, quote, request)?;
    assert_eq!(exchange.provider_id, "velora", "{label} provider_id");
    assert_eq!(exchange.kind, "universal_router_swap", "{label} kind");
    assert_eq!(
        quote.provider_quote["network"],
        json!(BASE_CHAIN_ID),
        "{label} provider_quote.network"
    );
    assert_eq!(
        quote.provider_quote["src_token"],
        json!(NATIVE_TOKEN_FOR_VELORA),
        "{label} provider_quote.src_token"
    );
    assert_eq!(
        quote.provider_quote["dest_token"],
        json!(format!("{BASE_USDC:#x}")),
        "{label} provider_quote.dest_token"
    );
    assert_raw_amount_string(
        &format!("{label} priceRoute.srcAmount"),
        required_string(price_route, "srcAmount", label)?,
    );
    assert_raw_amount_string(
        &format!("{label} priceRoute.destAmount"),
        required_string(price_route, "destAmount", label)?,
    );
    assert_eq!(
        price_route["network"],
        json!(BASE_CHAIN_ID),
        "{label} raw priceRoute.network"
    );
    assert_eq!(
        required_string(price_route, "side", label)?,
        "SELL",
        "{label} raw priceRoute.side"
    );
    Ok(VeloraContract {
        exchange,
        network: price_route["network"]
            .as_u64()
            .ok_or_else(|| format!("{label} priceRoute.network must be integer"))?,
        src_token: required_string(price_route, "srcToken", label)?.to_ascii_lowercase(),
        dest_token: required_string(price_route, "destToken", label)?.to_ascii_lowercase(),
        src_decimals: price_route["srcDecimals"]
            .as_u64()
            .ok_or_else(|| format!("{label} priceRoute.srcDecimals must be integer"))?,
        dest_decimals: price_route["destDecimals"]
            .as_u64()
            .ok_or_else(|| format!("{label} priceRoute.destDecimals must be integer"))?,
        price_route_amount_fields: vec!["srcAmount", "destAmount"],
    })
}

fn assert_hyperliquid_quote_contract(
    label: &str,
    quote: &ExchangeQuote,
    request: &ExchangeQuoteRequest,
) -> TestResult<ExchangeContract> {
    let contract = assert_exchange_quote_contract(label, quote, request)?;
    assert_eq!(contract.provider_id, "hyperliquid", "{label} provider_id");
    assert_eq!(contract.kind, "spot_cross_token", "{label} kind");
    assert_eq!(contract.leg_count, 2, "{label} two-leg UETH/UBTC route");
    assert_eq!(
        contract.first_input_asset, "hyperliquid:UETH",
        "{label} first input asset"
    );
    assert_eq!(
        contract.last_output_asset, "hyperliquid:UBTC",
        "{label} last output asset"
    );
    Ok(contract)
}

fn assert_hyperunit_generate_contract(
    label: &str,
    response: &UnitGenerateAddressResponse,
    request: &UnitGenerateAddressRequest,
) -> TestResult<HyperUnitGenerateContract> {
    assert!(
        !response.address.trim().is_empty(),
        "{label} generated address must not be empty"
    );
    let status_ok_or_missing = response
        .status
        .as_deref()
        .map(|status| status.eq_ignore_ascii_case("ok"))
        .unwrap_or(true);
    assert!(
        status_ok_or_missing,
        "{label} generated status must be OK or absent: {:?}",
        response.status
    );
    let destination_address_kind = unit_address_kind(&request.dst_chain, &request.dst_addr)?;
    let generated_address_kind = unit_address_kind(&request.src_chain, &response.address)?;
    Ok(HyperUnitGenerateContract {
        src_chain: request.src_chain.as_wire_str().to_string(),
        dst_chain: request.dst_chain.as_wire_str().to_string(),
        asset: request.asset.as_wire_str().to_string(),
        destination_address_kind,
        generated_address_kind,
        status_ok_or_missing,
        signatures_is_object: response.signatures.is_object(),
    })
}

fn assert_hyperunit_operations_contract(
    label: &str,
    response: &UnitOperationsResponse,
) -> TestResult<HyperUnitOperationsContract> {
    for (index, operation) in response.operations.iter().enumerate() {
        assert_hyperunit_operation_contract(&format!("{label} operations[{index}]"), operation)?;
    }
    Ok(HyperUnitOperationsContract {
        schema_contract: HyperUnitOperationsSchemaContract {
            addresses_is_array: true,
            operations_is_array: true,
            operation_amount_fields_are_decimal_compatible: true,
            operation_tx_hashes_are_strings: true,
        },
        operation_count: response.operations.len(),
    })
}

fn assert_hyperunit_operation_contract(label: &str, operation: &UnitOperation) -> TestResult<()> {
    for (field, value) in [
        ("sourceAmount", operation.source_amount.as_deref()),
        (
            "destinationFeeAmount",
            operation.destination_fee_amount.as_deref(),
        ),
        ("sweepFeeAmount", operation.sweep_fee_amount.as_deref()),
    ] {
        if let Some(value) = value {
            assert_decimal_compatible_amount_string(&format!("{label} {field}"), value);
        }
    }
    for (field, value) in [
        ("sourceTxHash", operation.source_tx_hash.as_deref()),
        (
            "destinationTxHash",
            operation.destination_tx_hash.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            assert!(
                !value.trim().is_empty(),
                "{label} {field} must not be empty"
            );
        }
    }
    Ok(())
}

fn assert_exchange_quote_contract(
    label: &str,
    quote: &ExchangeQuote,
    request: &ExchangeQuoteRequest,
) -> TestResult<ExchangeContract> {
    let requested_amount_in = match &request.order_kind {
        MarketOrderKind::ExactIn { amount_in, .. } => amount_in.as_str(),
        MarketOrderKind::ExactOut { .. } => {
            return Err(format!("{label} differential expects exact-in request").into())
        }
    };
    assert_raw_amount_string(&format!("{label} amount_in"), &quote.amount_in);
    assert_nonzero_raw_amount(&format!("{label} amount_out"), &quote.amount_out)?;
    if let Some(min_amount_out) = quote.min_amount_out.as_deref() {
        assert_raw_amount_string(&format!("{label} min_amount_out"), min_amount_out);
    }
    if let Some(max_amount_in) = quote.max_amount_in.as_deref() {
        assert_raw_amount_string(&format!("{label} max_amount_in"), max_amount_in);
    }
    assert_eq!(
        quote.amount_in, requested_amount_in,
        "{label} amount_in must equal request amount"
    );
    let legs = quote
        .provider_quote
        .get("legs")
        .and_then(Value::as_array)
        .map(|legs| legs.as_slice())
        .unwrap_or(&[]);
    for (index, leg) in legs.iter().enumerate() {
        for field in ["amount_in", "amount_out"] {
            assert_raw_amount_string(
                &format!("{label} provider_quote.legs[{index}].{field}"),
                required_string(leg, field, label)?,
            );
        }
    }
    Ok(ExchangeContract {
        provider_id: quote.provider_id.clone(),
        kind: required_string(&quote.provider_quote, "kind", label)?.to_string(),
        order_kind: required_string(&quote.provider_quote, "order_kind", label)
            .unwrap_or("exact_in")
            .to_string(),
        amount_in_matches_request: quote.amount_in == requested_amount_in,
        min_amount_out_present: quote.min_amount_out.is_some(),
        max_amount_in_present: quote.max_amount_in.is_some(),
        leg_count: legs.len(),
        first_input_asset: legs
            .first()
            .map(|leg| leg_asset_label(leg, "input_asset"))
            .transpose()?
            .unwrap_or_else(|| provider_quote_asset_label(&quote.provider_quote, "input_asset")),
        last_output_asset: legs
            .last()
            .map(|leg| leg_asset_label(leg, "output_asset"))
            .transpose()?
            .unwrap_or_else(|| provider_quote_asset_label(&quote.provider_quote, "output_asset")),
    })
}

fn cctp_provider(base_url: impl Into<String>) -> Result<CctpProvider, String> {
    CctpProvider::new(
        base_url.into(),
        format!("{TOKEN_MESSENGER_V2:#x}"),
        format!("{MESSAGE_TRANSMITTER_V2:#x}"),
        Arc::new(AssetRegistry::default()),
    )
}

fn cctp_quote_request(amount: U256) -> BridgeQuoteRequest {
    BridgeQuoteRequest {
        source_asset: deposit_asset("evm:8453", AssetId::reference(format!("{BASE_USDC:#x}"))),
        destination_asset: deposit_asset(
            "evm:42161",
            AssetId::reference(format!("{ARBITRUM_USDC:#x}")),
        ),
        order_kind: MarketOrderKind::ExactIn {
            amount_in: amount.to_string(),
            min_amount_out: Some("1".to_string()),
        },
        recipient_address: DEFAULT_EVM_ADDRESS.to_string(),
        depositor_address: "0x2222222222222222222222222222222222222222".to_string(),
        partial_fills_enabled: false,
    }
}

fn configured_across_request(integrator_id: &str) -> TestResult<AcrossSwapApprovalRequest> {
    let depositor = configured_evm_address()?;
    let recipient = env_var_any(&[ACROSS_RECIPIENT]).unwrap_or_else(|| format!("{depositor:#x}"));
    let refund_address = env_var_any(&[ACROSS_REFUND_ADDRESS])
        .map(|raw| Address::from_str(&raw))
        .transpose()?
        .unwrap_or(depositor);
    Ok(AcrossSwapApprovalRequest {
        trade_type: env_var_any(&[ACROSS_TRADE_TYPE]).unwrap_or_else(|| "exactInput".to_string()),
        origin_chain_id: env_var_any(&[ACROSS_ORIGIN_CHAIN_ID])
            .unwrap_or_else(|| BASE_CHAIN_ID.to_string())
            .parse()?,
        destination_chain_id: env_var_any(&[ACROSS_DESTINATION_CHAIN_ID])
            .unwrap_or_else(|| ARBITRUM_CHAIN_ID.to_string())
            .parse()?,
        input_token: Address::from_str(
            &env_var_any(&[ACROSS_INPUT_TOKEN]).unwrap_or_else(|| format!("{BASE_USDC:#x}")),
        )?,
        output_token: Address::from_str(
            &env_var_any(&[ACROSS_OUTPUT_TOKEN]).unwrap_or_else(|| format!("{ARBITRUM_USDC:#x}")),
        )?,
        amount: U256::from_str_radix(
            &env_var_any(&[ACROSS_AMOUNT]).unwrap_or_else(|| DEFAULT_ACROSS_AMOUNT_RAW.to_string()),
            10,
        )?,
        depositor,
        recipient,
        refund_address,
        refund_on_origin: true,
        strict_trade_type: true,
        integrator_id: integrator_id.to_string(),
        slippage: env_var_any(&[ACROSS_SLIPPAGE]),
        extra_query: BTreeMap::new(),
    })
}

async fn fetch_raw_across_swap_approval(
    client: &AcrossClient,
    request: &AcrossSwapApprovalRequest,
) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .get(format!("{}/swap/approval", client.base_url()))
        .query(&request.clone().into_query())
        .bearer_auth(env_var_any(&[ACROSS_API_KEY, "ACROSS_API_KEY"]).unwrap_or_default())
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(format!("Across raw swap approval failed with {status}: {body}").into());
    }
    Ok(serde_json::from_str(&body)?)
}

async fn fetch_velora_price_route(
    base_url: &str,
    user: Address,
    amount_in: U256,
) -> TestResult<Value> {
    let response = reqwest::Client::new()
        .get(format!("{}/prices", base_url.trim_end_matches('/')))
        .query(&[
            ("srcToken", NATIVE_TOKEN_FOR_VELORA.to_string()),
            ("destToken", format!("{BASE_USDC:#x}")),
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

fn deposit_asset(chain: &str, asset: AssetId) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid static chain id"),
        asset,
    }
}

fn configured_evm_address() -> TestResult<Address> {
    if let Some(private_key) = env_var_any(&[LIVE_TEST_PRIVATE_KEY, ACROSS_PRIVATE_KEY]) {
        return Ok(private_key.parse::<PrivateKeySigner>()?.address());
    }
    Ok(Address::from_str(DEFAULT_EVM_ADDRESS)?)
}

fn unit_address_kind(chain: &UnitChain, address: &str) -> TestResult<String> {
    match chain {
        UnitChain::Ethereum | UnitChain::Base | UnitChain::Hyperliquid => {
            assert_address_string("Unit EVM/Hyperliquid address", address);
            Ok("evm_address".to_string())
        }
        UnitChain::Bitcoin => {
            assert!(
                !address.trim().is_empty(),
                "Unit Bitcoin address must not be empty"
            );
            Ok("bitcoin_address".to_string())
        }
        _ => {
            assert!(
                !address.trim().is_empty(),
                "Unit address for unknown chain must not be empty"
            );
            Ok("unknown_chain_address".to_string())
        }
    }
}

fn live_velora_base_url() -> String {
    env_var_any(&[VELORA_BASE_URL, VELORA_API_URL])
        .unwrap_or_else(|| DEFAULT_VELORA_BASE_URL.to_string())
}

fn live_enabled() -> bool {
    if env::var(LIVE_PROVIDER_TESTS).as_deref() == Ok("1") {
        true
    } else {
        eprintln!("skipping live provider differential; set {LIVE_PROVIDER_TESTS}=1 to enable");
        false
    }
}

fn write_differential_artifact(provider: &str, payload: Value) {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let dir = env_var_any(&["ROUTER_LIVE_RECOVERY_DIR"])
        .map(PathBuf::from)
        .unwrap_or_else(|| env::temp_dir().join("router-live-provider-tests"))
        .join("provider-differentials");
    let path = dir.join(format!("{timestamp}-{provider}-differential.json"));
    let document = json!({
        "kind": "live_provider_differential",
        "provider": provider,
        "generated_at_unix": timestamp,
        "payload": payload,
    });

    if let Err(error) = fs::create_dir_all(&dir).and_then(|_| {
        fs::write(
            &path,
            serde_json::to_vec_pretty(&document).unwrap_or_else(|_| b"{}".to_vec()),
        )
    }) {
        eprintln!("failed to write live provider differential artifact: {error}");
    } else {
        eprintln!(
            "live provider differential artifact written to {}",
            path.display()
        );
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

fn required_env_any(keys: &[&str]) -> TestResult<String> {
    env_var_any(keys)
        .ok_or_else(|| format!("missing required env var {}", keys.join(" or ")).into())
}

fn assert_nonzero_raw_amount(label: &str, value: &str) -> TestResult<()> {
    assert_raw_amount_string(label, value);
    let parsed = U256::from_str_radix(value, 10)?;
    assert!(!parsed.is_zero(), "{label} must be non-zero");
    Ok(())
}

fn assert_decimal_compatible_amount_string(label: &str, value: &str) {
    assert!(!value.is_empty(), "{label} must not be empty");
    assert_eq!(
        value.trim(),
        value,
        "{label} must not contain surrounding whitespace: {value:?}"
    );
    let dot_count = value.bytes().filter(|byte| *byte == b'.').count();
    assert!(
        dot_count <= 1,
        "{label} must contain at most one decimal point"
    );
    assert!(
        value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte == b'.'),
        "{label} must be a decimal-compatible numeric string, got {value:?}"
    );
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or_default();
    let fractional = parts.next();
    assert!(
        !whole.is_empty() && whole.bytes().all(|byte| byte.is_ascii_digit()),
        "{label} must include whole-number digits, got {value:?}"
    );
    if let Some(fractional) = fractional {
        assert!(
            !fractional.is_empty() && fractional.bytes().all(|byte| byte.is_ascii_digit()),
            "{label} must include fractional digits after '.', got {value:?}"
        );
    }
}

fn assert_future_timestamp(label: &str, timestamp_secs: i64) -> TestResult<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
    assert!(
        timestamp_secs > now,
        "{label} must be in the future: timestamp={timestamp_secs}, now={now}"
    );
    Ok(())
}

fn assert_hex_string(label: &str, value: &str) {
    let raw = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value);
    assert!(
        !raw.is_empty()
            && raw.len().is_multiple_of(2)
            && raw.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{label} must be non-empty hex data, got {value:?}"
    );
}

fn assert_address_string(label: &str, value: &str) {
    Address::from_str(value).unwrap_or_else(|err| panic!("{label} must be an address: {err}"));
}

fn required_string<'a>(value: &'a Value, field: &str, label: &str) -> TestResult<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} missing string field {field}").into())
}

fn required_string_at<'a>(value: &'a Value, pointer: &str, label: &str) -> TestResult<&'a str> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("{label} missing string field at {pointer}").into())
}

fn provider_quote_asset_label(value: &Value, field: &str) -> String {
    value
        .get(field)
        .map(|asset| {
            format!(
                "{}:{}",
                asset
                    .get("chain_id")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing>"),
                asset
                    .get("asset")
                    .and_then(Value::as_str)
                    .unwrap_or("<missing>")
            )
        })
        .unwrap_or_else(|| "<missing>:<missing>".to_string())
}

fn leg_asset_label(leg: &Value, field: &str) -> TestResult<String> {
    let asset = leg
        .get(field)
        .ok_or_else(|| format!("quote leg missing {field}"))?;
    Ok(format!(
        "{}:{}",
        required_string(asset, "chain_id", field)?,
        required_string(asset, "asset", field)?
    ))
}
