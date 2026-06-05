use alloy::{
    hex,
    primitives::{Address, FixedBytes, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    sol_types::{SolCall, SolEvent},
};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, str::FromStr, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::across_spoke_pool_mock::MockSpokePool::{depositCall, FundsDeposited};
use crate::mock_integrators::{
    address_to_bytes32, bytes32_to_evm_address, deterministic_bps, mock_credit_native_on_anvil,
    mock_evm_indexer_initial_last_scanned, mock_mint_erc20_on_anvil, parse_amount, AcrossDepositKey,
    MockIntegratorConfig, MockIntegratorState, MockService, IERC20,
};

#[derive(Debug, Clone)]
pub struct MockAcrossChainConfig {
    pub spoke_pool_address: String,
    pub evm_rpc_url: String,
}


#[derive(Debug, Clone)]
pub(crate) struct ResolvedMockAcrossChainConfig {
    pub(crate) spoke_pool_address: String,
    pub(crate) evm_rpc_url: Option<String>,
}


#[derive(Debug, Clone)]
pub struct MockAcrossDepositRecord {
    pub origin_chain_id: u64,
    pub destination_chain_id: U256,
    pub deposit_id: U256,
    pub depositor: FixedBytes<32>,
    pub recipient: FixedBytes<32>,
    pub input_token: FixedBytes<32>,
    pub output_token: FixedBytes<32>,
    pub input_amount: U256,
    pub output_amount: U256,
    pub fill_deadline: u32,
    pub deposit_tx_hash: String,
    pub block_number: u64,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MockAcrossRealSwapApprovalQuery {
    trade_type: String,
    origin_chain_id: u64,
    #[allow(dead_code)]
    destination_chain_id: u64,
    #[allow(dead_code)]
    input_token: String,
    #[allow(dead_code)]
    output_token: String,
    amount: String,
    #[allow(dead_code)]
    depositor: String,
    #[allow(dead_code)]
    recipient: String,
    #[allow(dead_code)]
    refund_address: Option<String>,
    #[allow(dead_code)]
    refund_on_origin: Option<bool>,
    #[allow(dead_code)]
    strict_trade_type: Option<bool>,
    #[allow(dead_code)]
    integrator_id: Option<String>,
    #[allow(dead_code)]
    slippage: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossRealTransaction {
    chain_id: u64,
    to: String,
    data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_priority_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ecosystem: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    simulation_success: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossRealSwapApprovalResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_txns: Option<Vec<MockAcrossRealTransaction>>,
    swap_tx: MockAcrossRealTransaction,
    input_amount: String,
    max_input_amount: String,
    expected_output_amount: String,
    min_output_amount: String,
    quote_expiry_timestamp: i64,
    id: String,
    amount_type: String,
    cross_swap_type: String,
    expected_fill_time: u64,
    fees: Value,
    checks: Value,
    steps: Value,
    input_token: Value,
    output_token: Value,
    refund_token: Value,
}

pub(crate) async fn mock_across_real_swap_approval(
    State(state): State<Arc<MockIntegratorState>>,
    headers: HeaderMap,
    Query(query): Query<MockAcrossRealSwapApprovalQuery>,
) -> impl IntoResponse {
    if let Err(response) =
        validate_across_request(&state, &headers, query.integrator_id.as_deref(), true)
    {
        return *response;
    }
    if let Some(error) = state.next_across_swap_approval_error.lock().await.take() {
        return mock_across_error_response(
            StatusCode::BAD_GATEWAY,
            "bad_gateway",
            "mock_across_upstream_error",
            error,
            None,
        );
    }
    let amount = match parse_amount("amount", &query.amount) {
        Ok(amount) => amount,
        Err(error) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_amount",
                error,
                Some("amount"),
            );
        }
    };
    match query.trade_type.as_str() {
        "exactInput" | "minOutput" | "exactOutput" => {}
        other => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "unsupported_trade_type",
                format!("unsupported tradeType: {other}"),
                Some("tradeType"),
            );
        }
    }
    let Some(origin_config) = state.across_chain_config(query.origin_chain_id) else {
        return mock_across_error_response(
            StatusCode::FAILED_DEPENDENCY,
            "configuration_error",
            "missing_spoke_pool",
            format!(
                "mock Across: origin chain {} is not configured",
                query.origin_chain_id
            ),
            None,
        );
    };
    let spoke_pool_address = match Address::from_str(&origin_config.spoke_pool_address) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "configuration_error",
                "invalid_spoke_pool",
                format!(
                    "mock Across: invalid spoke_pool_address for origin chain {}: {err}",
                    query.origin_chain_id
                ),
                None,
            );
        }
    };
    let input_token_addr = match Address::from_str(&query.input_token) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_input_token",
                format!("inputToken is not a valid address: {err}"),
                Some("inputToken"),
            );
        }
    };
    let output_token_addr = match Address::from_str(&query.output_token) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_output_token",
                format!("outputToken is not a valid address: {err}"),
                Some("outputToken"),
            );
        }
    };
    let depositor_addr = match Address::from_str(&query.depositor) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_depositor",
                format!("depositor is not a valid address: {err}"),
                Some("depositor"),
            );
        }
    };
    let recipient_addr = match Address::from_str(&query.recipient) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_recipient",
                format!("recipient is not a valid address: {err}"),
                Some("recipient"),
            );
        }
    };

    let quote_amounts = match mock_across_quote_amounts(&state, &query, amount) {
        Ok(amounts) => amounts,
        Err(error) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "unsupported_trade_type",
                error,
                Some("tradeType"),
            );
        }
    };
    let input_amount_u256 = U256::from(quote_amounts.input_amount);
    let output_amount_u256 = U256::from(quote_amounts.output_amount);
    let now_secs = Utc::now().timestamp();
    let quote_timestamp = u32::try_from(now_secs.max(0)).unwrap_or(u32::MAX);
    let fill_deadline = u32::try_from((now_secs + 300).max(0)).unwrap_or(u32::MAX);

    let deposit_call = depositCall {
        depositor: address_to_bytes32(depositor_addr),
        recipient: address_to_bytes32(recipient_addr),
        inputToken: address_to_bytes32(input_token_addr),
        outputToken: address_to_bytes32(output_token_addr),
        inputAmount: input_amount_u256,
        outputAmount: output_amount_u256,
        destinationChainId: U256::from(query.destination_chain_id),
        exclusiveRelayer: FixedBytes::<32>::ZERO,
        quoteTimestamp: quote_timestamp,
        fillDeadline: fill_deadline,
        exclusivityDeadline: 0,
        message: alloy::primitives::Bytes::new(),
    };
    let deposit_calldata = format!("0x{}", hex::encode(deposit_call.abi_encode()));

    let allowance = match (
        input_token_addr == Address::ZERO,
        origin_config.evm_rpc_url.as_deref(),
    ) {
        (true, _) | (false, None) => None,
        (false, Some(rpc_url)) => {
            match mock_across_allowance(
                rpc_url,
                input_token_addr,
                depositor_addr,
                spoke_pool_address,
                query.origin_chain_id,
            )
            .await
            {
                Ok(allowance) => Some(allowance),
                Err(error) => {
                    return mock_across_error_response(
                        StatusCode::BAD_GATEWAY,
                        "bad_gateway",
                        "mock_across_rpc_error",
                        error,
                        Some("inputToken"),
                    );
                }
            }
        }
    };
    let needs_approval =
        input_token_addr != Address::ZERO && allowance.unwrap_or_default() < input_amount_u256;
    let balance = match origin_config.evm_rpc_url.as_deref() {
        Some(rpc_url) => match mock_across_balance(
            rpc_url,
            input_token_addr,
            depositor_addr,
            query.origin_chain_id,
        )
        .await
        {
            Ok(balance) => balance,
            Err(error) => {
                return mock_across_error_response(
                    StatusCode::BAD_GATEWAY,
                    "bad_gateway",
                    "mock_across_rpc_error",
                    error,
                    Some("inputToken"),
                );
            }
        },
        None => input_amount_u256,
    };

    let (approval_txns, swap_value) = if input_token_addr == Address::ZERO {
        (None, Some(input_amount_u256.to_string()))
    } else if needs_approval {
        let approve_call = IERC20::approveCall {
            spender: spoke_pool_address,
            amount: U256::MAX,
        };
        let approve_calldata = format!("0x{}", hex::encode(approve_call.abi_encode()));
        let approval_tx = MockAcrossRealTransaction {
            chain_id: query.origin_chain_id,
            to: format!("{input_token_addr:#x}"),
            data: approve_calldata,
            value: None,
            gas: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            ecosystem: None,
            simulation_success: None,
        };
        (Some(vec![approval_tx]), None)
    } else {
        (None, None)
    };

    let approval_required = approval_txns.is_some();
    let swap_tx = MockAcrossRealTransaction {
        chain_id: query.origin_chain_id,
        to: format!("{spoke_pool_address:#x}"),
        data: deposit_calldata,
        value: swap_value,
        gas: Some(if approval_required { "0" } else { "84804" }.to_string()),
        max_fee_per_gas: (!approval_required).then(|| "13500000".to_string()),
        max_priority_fee_per_gas: (!approval_required).then(|| "1000000".to_string()),
        ecosystem: Some("evm".to_string()),
        simulation_success: Some(true),
    };

    let quote_expiry = (Utc::now() + ChronoDuration::seconds(60)).timestamp();
    let input_token_meta = mock_across_token_metadata(input_token_addr, query.origin_chain_id);
    let output_token_meta =
        mock_across_token_metadata(output_token_addr, query.destination_chain_id);
    let refund_token_meta = input_token_meta.clone();
    let fee_amount = match quote_amounts
        .input_amount
        .checked_sub(quote_amounts.expected_output_amount)
    {
        Some(fee_amount) => fee_amount,
        None => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_quote_amounts",
                "mock Across quote output exceeds input".to_string(),
                Some("amount"),
            );
        }
    };
    let response = MockAcrossRealSwapApprovalResponse {
        approval_txns,
        swap_tx,
        input_amount: quote_amounts.input_amount.to_string(),
        max_input_amount: quote_amounts.max_input_amount.to_string(),
        expected_output_amount: quote_amounts.expected_output_amount.to_string(),
        min_output_amount: quote_amounts.min_output_amount.to_string(),
        quote_expiry_timestamp: quote_expiry,
        id: format!("mock-across-{}", Uuid::now_v7()),
        amount_type: query.trade_type.clone(),
        cross_swap_type: mock_across_cross_swap_type(input_token_addr, output_token_addr),
        expected_fill_time: 2,
        fees: mock_across_fee_breakdown(
            quote_amounts.input_amount,
            fee_amount,
            &input_token_meta,
            &output_token_meta,
            query.origin_chain_id,
        ),
        checks: mock_across_checks(
            input_token_addr,
            allowance,
            balance,
            input_amount_u256,
            spoke_pool_address,
        ),
        steps: mock_across_steps(
            quote_amounts.input_amount,
            quote_amounts.expected_output_amount,
            fee_amount,
            &input_token_meta,
            &output_token_meta,
        ),
        input_token: input_token_meta,
        output_token: output_token_meta,
        refund_token: refund_token_meta,
    };
    Json(response).into_response()
}

async fn mock_across_allowance(
    rpc_url: &str,
    token: Address,
    owner: Address,
    spender: Address,
    origin_chain_id: u64,
) -> Result<U256, String> {
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            return Err(format!(
                "failed to connect origin chain {origin_chain_id} RPC for allowance check at {rpc_url}: {err}"
            ));
        }
    };
    let token = IERC20::new(token, provider);
    match token.allowance(owner, spender).call().await {
        Ok(allowance) => Ok(allowance),
        Err(err) => Err(format!(
            "allowance check failed on origin chain {origin_chain_id} RPC {rpc_url} for token {:#x}, owner {owner:#x}, spender {spender:#x}: {err}",
            token.address()
        )),
    }
}

async fn mock_across_balance(
    rpc_url: &str,
    token: Address,
    owner: Address,
    origin_chain_id: u64,
) -> Result<U256, String> {
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            return Err(format!(
                "failed to connect origin chain {origin_chain_id} RPC for balance check at {rpc_url}: {err}"
            ));
        }
    };

    if token == Address::ZERO {
        match provider.get_balance(owner).await {
            Ok(balance) => Ok(balance),
            Err(err) => Err(format!(
                "native balance check failed on origin chain {origin_chain_id} RPC {rpc_url} for owner {owner:#x}: {err}"
            )),
        }
    } else {
        let token = IERC20::new(token, provider);
        match token.balanceOf(owner).call().await {
            Ok(balance) => Ok(balance),
            Err(err) => Err(format!(
                "token balance check failed on origin chain {origin_chain_id} RPC {rpc_url} for token {:#x}, owner {owner:#x}: {err}",
                token.address()
            )),
        }
    }
}

fn mock_across_cross_swap_type(input_token: Address, output_token: Address) -> String {
    match (input_token == Address::ZERO, output_token == Address::ZERO) {
        (true, true) => "nativeToNative",
        (true, false) => "nativeToBridgeable",
        (false, true) => "bridgeableToNative",
        (false, false) => "bridgeableToBridgeable",
    }
    .to_string()
}

fn mock_across_checks(
    input_token: Address,
    allowance: Option<U256>,
    balance: U256,
    required_amount: U256,
    spender: Address,
) -> Value {
    let mut checks = serde_json::Map::new();
    if input_token != Address::ZERO {
        checks.insert(
            "allowance".to_string(),
            json!({
                "actual": allowance.unwrap_or_default().to_string(),
                "expected": required_amount.to_string(),
                "spender": format!("{spender:#x}"),
                "token": format!("{input_token:#x}"),
            }),
        );
    }
    checks.insert(
        "balance".to_string(),
        json!({
            "actual": balance.to_string(),
            "expected": required_amount.to_string(),
            "token": format!("{input_token:#x}"),
        }),
    );
    Value::Object(checks)
}

fn mock_across_steps(
    input_amount: u128,
    output_amount: u128,
    fee_amount: u128,
    input_token: &Value,
    output_token: &Value,
) -> Value {
    let fee_components = mock_across_fee_components(fee_amount);
    let mut steps = serde_json::Map::new();
    steps.insert(
        "bridge".to_string(),
        json!({
            "provider": "across",
            "inputAmount": input_amount.to_string(),
            "outputAmount": output_amount.to_string(),
            "tokenIn": input_token,
            "tokenOut": output_token,
            "fees": {
                "amount": fee_amount.to_string(),
                "pct": mock_across_fee_pct_string(input_amount, fee_amount),
                "token": input_token,
                "details": {
                    "destinationGas": {
                        "amount": fee_components.destination_gas.to_string(),
                        "pct": mock_across_fee_pct_string(input_amount, fee_components.destination_gas),
                        "token": input_token,
                    },
                    "lp": {
                        "amount": fee_components.lp.to_string(),
                        "pct": mock_across_fee_pct_string(input_amount, fee_components.lp),
                        "token": input_token,
                    },
                    "relayerCapital": {
                        "amount": fee_components.relayer_capital.to_string(),
                        "pct": mock_across_fee_pct_string(input_amount, fee_components.relayer_capital),
                        "token": input_token,
                    },
                    "type": "across",
                },
            },
        }),
    );
    Value::Object(steps)
}

fn mock_across_fee_breakdown(
    input_amount: u128,
    fee_amount: u128,
    input_token: &Value,
    output_token: &Value,
    origin_chain_id: u64,
) -> Value {
    let fee_pct = mock_across_fee_pct_string(input_amount, fee_amount);
    let origin_gas_token = mock_across_token_metadata(Address::ZERO, origin_chain_id);
    let fee_components = mock_across_fee_components(fee_amount);
    json!({
        "originGas": {
            "amount": "0",
            "amountUsd": "0.0",
            "token": origin_gas_token,
        },
        "total": {
            "amount": fee_amount.to_string(),
            "amountUsd": "0.0",
            "pct": fee_pct,
            "token": input_token,
            "details": {
                "app": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": output_token,
                },
                "bridge": {
                    "amount": fee_amount.to_string(),
                    "amountUsd": "0.0",
                    "pct": fee_pct,
                    "token": input_token,
                    "details": {
                        "destinationGas": {
                            "amount": fee_components.destination_gas.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.destination_gas),
                            "token": input_token,
                        },
                        "lp": {
                            "amount": fee_components.lp.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.lp),
                            "token": input_token,
                        },
                        "relayerCapital": {
                            "amount": fee_components.relayer_capital.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.relayer_capital),
                            "token": input_token,
                        },
                        "type": "across",
                    },
                },
                "swapImpact": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": input_token,
                },
                "type": "total-breakdown",
            },
        },
        "totalMax": {
            "amount": fee_amount.to_string(),
            "amountUsd": "0.0",
            "pct": fee_pct,
            "token": input_token,
            "details": {
                "app": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": output_token,
                },
                "bridge": {
                    "amount": fee_amount.to_string(),
                    "amountUsd": "0.0",
                    "pct": fee_pct,
                    "token": input_token,
                    "details": {
                        "destinationGas": {
                            "amount": fee_components.destination_gas.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.destination_gas),
                            "token": input_token,
                        },
                        "lp": {
                            "amount": fee_components.lp.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.lp),
                            "token": input_token,
                        },
                        "relayerCapital": {
                            "amount": fee_components.relayer_capital.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, fee_components.relayer_capital),
                            "token": input_token,
                        },
                        "type": "across",
                    },
                },
                "maxSwapImpact": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": input_token,
                },
                "type": "max-total-breakdown",
            },
        },
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MockAcrossFeeComponents {
    destination_gas: u128,
    lp: u128,
    relayer_capital: u128,
}

fn mock_across_fee_components(fee_amount: u128) -> MockAcrossFeeComponents {
    let lp = fee_amount.min(40);
    let remaining_after_lp = fee_amount - lp;
    let relayer_capital = remaining_after_lp.min(100);
    let destination_gas = remaining_after_lp - relayer_capital;
    MockAcrossFeeComponents {
        destination_gas,
        lp,
        relayer_capital,
    }
}

fn mock_across_fee_pct_string(input_amount: u128, fee_amount: u128) -> String {
    if input_amount == 0 {
        return "0".to_string();
    }
    ((U256::from(fee_amount) * U256::from(1_000_000_000_000_000_000_u128))
        / U256::from(input_amount))
    .to_string()
}

fn mock_across_token_metadata(address: Address, chain_id: u64) -> Value {
    match format!("{address:#x}").as_str() {
        "0x0000000000000000000000000000000000000000" => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 18,
            "symbol": "ETH",
        }),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        | "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        | "0xaf88d065e77c8cc2239327c5edb3a432268e5831" => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 6,
            "name": "USD Coin",
            "symbol": "USDC",
        }),
        _ => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 18,
            "name": "Mock Token",
            "symbol": "MOCK",
        }),
    }
}

#[derive(Debug)]
struct MockAcrossQuoteAmounts {
    input_amount: u128,
    max_input_amount: u128,
    expected_output_amount: u128,
    min_output_amount: u128,
    output_amount: u128,
}

fn mock_across_quote_amounts(
    state: &MockIntegratorState,
    query: &MockAcrossRealSwapApprovalQuery,
    amount: u128,
) -> Result<MockAcrossQuoteAmounts, String> {
    let fee_bps = mock_across_quote_fee_bps(state, query);
    let amounts = match query.trade_type.as_str() {
        "exactInput" => {
            let output = apply_mock_across_discount(amount, fee_bps)?;
            MockAcrossQuoteAmounts {
                input_amount: amount,
                max_input_amount: amount,
                expected_output_amount: output,
                min_output_amount: output,
                output_amount: output,
            }
        }
        "minOutput" | "exactOutput" => {
            let input = gross_up_mock_across_input(amount, fee_bps)?;
            MockAcrossQuoteAmounts {
                input_amount: input,
                max_input_amount: input,
                expected_output_amount: amount,
                min_output_amount: amount,
                output_amount: amount,
            }
        }
        other => return Err(format!("unsupported tradeType: {other}")),
    };
    Ok(amounts)
}

fn mock_across_quote_fee_bps(
    state: &MockIntegratorState,
    query: &MockAcrossRealSwapApprovalQuery,
) -> u16 {
    let base = state.across_quote_fee_bps;
    let jitter = deterministic_bps(
        &format!(
            "across-quote:{}:{}:{}:{}:{}:{}",
            query.trade_type,
            query.origin_chain_id,
            query.destination_chain_id,
            query.input_token,
            query.output_token,
            query.amount
        ),
        state.across_quote_jitter_bps,
    );
    base.saturating_add(jitter).min(9_999)
}

fn apply_mock_across_discount(amount: u128, fee_bps: u16) -> Result<u128, String> {
    let keep_bps = 10_000_u128
        .checked_sub(u128::from(fee_bps))
        .ok_or_else(|| "mock Across fee bps exceed 100%".to_string())?;
    amount
        .checked_mul(keep_bps)
        .map(|value| value / 10_000_u128)
        .ok_or_else(|| "mock Across exact-input quote amount overflow".to_string())
}

fn gross_up_mock_across_input(amount: u128, fee_bps: u16) -> Result<u128, String> {
    let keep_bps = 10_000_u128
        .checked_sub(u128::from(fee_bps))
        .ok_or_else(|| "mock Across fee bps exceed 100%".to_string())?
        .max(1);
    amount
        .checked_mul(10_000_u128)
        .map(|value| value.div_ceil(keep_bps))
        .ok_or_else(|| "mock Across exact-output quote amount overflow".to_string())
}

fn mock_across_should_refund(
    state: &MockIntegratorState,
    record: &MockAcrossDepositRecord,
) -> bool {
    deterministic_bps(
        &format!(
            "across-status:{}:{}:{}",
            record.origin_chain_id, record.deposit_id, record.deposit_tx_hash
        ),
        9_999,
    ) < state.across_refund_probability_bps
}

fn mock_across_destination_chain_id(destination_chain_id: U256) -> Result<u64, String> {
    u64::try_from(destination_chain_id)
        .map_err(|err| format!("invalid destination chain id: {err}"))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MockAcrossDepositStatusQuery {
    origin_chain_id: Option<u64>,
    deposit_id: Option<String>,
    deposit_txn_ref: Option<String>,
}

/// Shape mirrors Across' real `GET /deposit/status` response (camelCase). The
/// mock serves `"filled"` once the FundsDeposited event has been observed
/// on-chain. Queries by `depositTxnRef` can temporarily return the same
/// `DepositNotFoundException` 404 that real Across returns before indexing
/// catches up.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossDepositStatusResponse {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin_chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_refund_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_deadline: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<Value>,
}

pub(crate) async fn mock_across_deposit_status(
    State(state): State<Arc<MockIntegratorState>>,
    headers: HeaderMap,
    Query(query): Query<MockAcrossDepositStatusQuery>,
) -> impl IntoResponse {
    if let Err(response) = validate_across_request(&state, &headers, None, false) {
        return *response;
    }
    let record = {
        let deposits = state.across_deposits.lock().await;
        if let (Some(origin_chain_id), Some(deposit_id)) =
            (query.origin_chain_id, query.deposit_id.as_deref())
        {
            deposits
                .get(&(origin_chain_id, deposit_id.to_string()))
                .cloned()
        } else if let Some(deposit_txn_ref) = query.deposit_txn_ref.as_deref() {
            deposits
                .values()
                .find(|record| record.deposit_tx_hash.eq_ignore_ascii_case(deposit_txn_ref))
                .cloned()
        } else {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "missing_deposit_lookup",
                "deposit/status requires depositTxnRef or originChainId + depositId",
                None,
            );
        }
    };
    let Some(record) = record else {
        if query.deposit_txn_ref.is_some() {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": "DepositNotFoundException",
                    "message": "Deposit not found given the provided constraints",
                })),
            )
                .into_response();
        }
        return Json(MockAcrossDepositStatusResponse {
            status: "pending".to_string(),
            origin_chain_id: query.origin_chain_id,
            destination_chain_id: None,
            deposit_id: query.deposit_id,
            deposit_txn_ref: query.deposit_txn_ref,
            fill_txn_ref: None,
            deposit_refund_txn_ref: None,
            fill_deadline: None,
            pagination: Some(mock_across_pagination()),
        })
        .into_response();
    };
    let destination_chain_id = match mock_across_destination_chain_id(record.destination_chain_id) {
        Ok(destination_chain_id) => destination_chain_id,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_deposit_record",
                "invalid_destination_chain_id",
                err,
                None,
            );
        }
    };
    if Utc::now()
        .signed_duration_since(record.indexed_at)
        .to_std()
        .unwrap_or_default()
        < state.across_status_latency
    {
        return Json(MockAcrossDepositStatusResponse {
            status: "pending".to_string(),
            origin_chain_id: Some(record.origin_chain_id),
            destination_chain_id: Some(destination_chain_id),
            deposit_id: Some(record.deposit_id.to_string()),
            deposit_txn_ref: Some(record.deposit_tx_hash.clone()),
            fill_txn_ref: None,
            deposit_refund_txn_ref: None,
            fill_deadline: None,
            pagination: Some(mock_across_pagination()),
        })
        .into_response();
    }

    let should_refund = mock_across_should_refund(&state, &record);
    let (status, fill_txn_ref, deposit_refund_txn_ref) = if should_refund {
        (
            "refunded".to_string(),
            None,
            Some(format!("0x{}", "de".repeat(32))),
        )
    } else {
        let key = (record.origin_chain_id, record.deposit_id.to_string());
        match ensure_mock_across_destination_credit(&state, key, &record).await {
            Ok(fill_txn_ref) => ("filled".to_string(), Some(fill_txn_ref), None),
            Err(err) => {
                tracing::warn!(
                    %err,
                    origin_chain_id = record.origin_chain_id,
                    destination_chain_id = %record.destination_chain_id,
                    deposit_id = %record.deposit_id,
                    "mock Across destination credit failed; keeping deposit pending"
                );
                ("pending".to_string(), None, None)
            }
        }
    };
    Json(MockAcrossDepositStatusResponse {
        status,
        origin_chain_id: Some(record.origin_chain_id),
        destination_chain_id: Some(destination_chain_id),
        deposit_id: Some(record.deposit_id.to_string()),
        deposit_txn_ref: Some(record.deposit_tx_hash.clone()),
        fill_txn_ref,
        deposit_refund_txn_ref,
        fill_deadline: None,
        pagination: Some(mock_across_pagination()),
    })
    .into_response()
}

async fn ensure_mock_across_destination_credit(
    state: &Arc<MockIntegratorState>,
    key: AcrossDepositKey,
    record: &MockAcrossDepositRecord,
) -> Result<String, String> {
    const PENDING_CREDIT: &str = "__pending__";

    {
        let mut credits = state.across_destination_credit_tx_hashes.lock().await;
        match credits.get(&key).map(String::as_str) {
            Some(PENDING_CREDIT) => {
                return Err("destination credit is still being applied".to_string());
            }
            Some(tx_hash) => return Ok(tx_hash.to_string()),
            None => {
                credits.insert(key.clone(), PENDING_CREDIT.to_string());
            }
        }
    }

    let credit_result = mock_across_credit_destination(state, record).await;
    let mut credits = state.across_destination_credit_tx_hashes.lock().await;
    match credit_result {
        Ok(tx_hash) => {
            credits.insert(key, tx_hash.clone());
            Ok(tx_hash)
        }
        Err(err) => {
            credits.remove(&key);
            Err(err)
        }
    }
}

async fn mock_across_credit_destination(
    state: &MockIntegratorState,
    record: &MockAcrossDepositRecord,
) -> Result<String, String> {
    if record.output_amount.is_zero() {
        return Ok(mock_across_pending_fill_tx_ref(record));
    }
    let destination_chain_id = mock_across_destination_chain_id(record.destination_chain_id)?;
    if state.across_chain_rpc_url(destination_chain_id).is_none() {
        return Err(format!(
            "mock Across destination chain {destination_chain_id} has no RPC configured"
        ));
    }
    let recipient = bytes32_to_evm_address(record.recipient);
    let output_token = bytes32_to_evm_address(record.output_token);
    if output_token == Address::ZERO {
        mock_credit_native_on_anvil(
            state,
            destination_chain_id,
            MockService::Across,
            recipient,
            record.output_amount,
        )
        .await?;
        return Ok(mock_across_pending_fill_tx_ref(record));
    }
    mock_mint_erc20_on_anvil(
        state,
        destination_chain_id,
        MockService::Across,
        output_token,
        recipient,
        record.output_amount,
    )
    .await
}

fn mock_across_pending_fill_tx_ref(record: &MockAcrossDepositRecord) -> String {
    let digest = Sha256::digest(format!(
        "mock-across-fill:{}:{}:{}",
        record.origin_chain_id, record.destination_chain_id, record.deposit_id
    ));
    format!("0x{}", hex::encode(digest))
}

fn mock_across_pagination() -> Value {
    json!({
        "currentIndex": 0,
        "maxIndex": 0,
    })
}

pub(crate) async fn maybe_spawn_across_deposit_indexer(
    config: &MockIntegratorConfig,
    state: Arc<MockIntegratorState>,
) -> eyre::Result<(Vec<tokio::sync::oneshot::Sender<()>>, Vec<JoinHandle<()>>)> {
    let mut chain_configs = config.across_chains.values().cloned().collect::<Vec<_>>();
    if let (Some(spoke_pool_address), Some(evm_rpc_url)) = (
        config.across_spoke_pool_address.clone(),
        config.across_evm_rpc_url.clone(),
    ) {
        chain_configs.push(MockAcrossChainConfig {
            spoke_pool_address,
            evm_rpc_url,
        });
    }
    if chain_configs.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut shutdowns = Vec::with_capacity(chain_configs.len());
    let mut handles = Vec::with_capacity(chain_configs.len());
    let mut seen_chain_ids = BTreeSet::new();
    for chain_config in chain_configs {
        let spoke_pool_address: Address =
            chain_config.spoke_pool_address.parse().map_err(|err| {
                eyre::eyre!(
                    "mock across indexer: invalid spoke pool address {}: {err}",
                    chain_config.spoke_pool_address
                )
            })?;
        let provider: DynProvider = ProviderBuilder::new()
            .connect(&chain_config.evm_rpc_url)
            .await?
            .erased();
        let chain_id = provider.get_chain_id().await?;
        if !seen_chain_ids.insert(chain_id) {
            continue;
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(run_across_deposit_indexer(
            provider,
            spoke_pool_address,
            chain_id,
            state.clone(),
            shutdown_rx,
        ));
        shutdowns.push(shutdown_tx);
        handles.push(handle);
    }
    Ok((shutdowns, handles))
}

async fn run_across_deposit_indexer(
    provider: DynProvider,
    spoke_pool_address: Address,
    chain_id: u64,
    state: Arc<MockIntegratorState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = FundsDeposited::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = mock_evm_indexer_initial_last_scanned();
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock across indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(spoke_pool_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock across indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match FundsDeposited::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock across indexer: decode FundsDeposited failed");
                    continue;
                }
            };
            let event = decoded.data;
            let Some(deposit_tx_hash) = log.transaction_hash.map(|h| format!("{h:#x}")) else {
                tracing::warn!("mock across indexer: FundsDeposited log missing transaction hash");
                continue;
            };
            let Some(block_number) = log.block_number else {
                tracing::warn!("mock across indexer: FundsDeposited log missing block number");
                continue;
            };
            let record = MockAcrossDepositRecord {
                origin_chain_id: chain_id,
                destination_chain_id: event.destinationChainId,
                deposit_id: event.depositId,
                depositor: event.depositor,
                recipient: event.recipient,
                input_token: event.inputToken,
                output_token: event.outputToken,
                input_amount: event.inputAmount,
                output_amount: event.outputAmount,
                fill_deadline: event.fillDeadline,
                deposit_tx_hash,
                block_number,
                indexed_at: Utc::now(),
            };
            let key = (chain_id, record.deposit_id.to_string());
            state.across_deposits.lock().await.insert(key, record);
        }
        last_scanned = head;
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossErrorResponse {
    #[serde(rename = "type")]
    error_type: String,
    code: String,
    status: u16,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    param: Option<String>,
}

fn validate_across_request(
    state: &MockIntegratorState,
    headers: &HeaderMap,
    integrator_id: Option<&str>,
    require_integrator_id: bool,
) -> Result<(), Box<axum::response::Response>> {
    if let Some(expected_api_key) = state.across_api_key.as_deref() {
        let expected = format!("Bearer {expected_api_key}");
        let actual = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if actual != Some(expected.as_str()) {
            return Err(Box::new(mock_across_error_response(
                StatusCode::UNAUTHORIZED,
                "auth_error",
                "missing_or_invalid_auth",
                "Across API key is required",
                None,
            )));
        }
    }

    if require_integrator_id {
        if let Some(expected_integrator_id) = state.across_integrator_id.as_deref() {
            if integrator_id != Some(expected_integrator_id) {
                return Err(Box::new(mock_across_error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "validation_error",
                    "invalid_integrator_id",
                    "integratorId is required",
                    Some("integratorId"),
                )));
            }
        }
    }

    Ok(())
}

fn mock_across_error_response(
    status: StatusCode,
    error_type: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    param: Option<&'static str>,
) -> axum::response::Response {
    (
        status,
        Json(MockAcrossErrorResponse {
            error_type: error_type.into(),
            code: code.into(),
            status: status.as_u16(),
            message: message.into(),
            param: param.map(str::to_string),
        }),
    )
        .into_response()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_integrators::{MockIntegratorConfig, MockIntegratorServer};
    use blockchain_utils::create_websocket_wallet_provider;
    use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;

    #[test]
    fn mock_across_quote_math_rejects_overflow() {
        assert_eq!(apply_mock_across_discount(1_000, 100).unwrap(), 990);
        assert_eq!(gross_up_mock_across_input(990, 100).unwrap(), 1_000);

        assert!(apply_mock_across_discount(u128::MAX, 100).is_err());
        assert!(gross_up_mock_across_input(u128::MAX, 100).is_err());

        let pct = mock_across_fee_pct_string(1, u128::MAX);
        assert_eq!(
            pct,
            (U256::from(u128::MAX) * U256::from(1_000_000_000_000_000_000_u128)).to_string()
        );
    }

    #[test]
    fn mock_across_fee_components_sum_to_total_without_saturation() {
        for fee_amount in [0, 1, 39, 40, 41, 100, 140, 141, 1_000, u128::MAX] {
            let components = mock_across_fee_components(fee_amount);
            assert_eq!(
                components.destination_gas + components.lp + components.relayer_capital,
                fee_amount
            );
        }

        assert_eq!(
            mock_across_fee_components(39),
            MockAcrossFeeComponents {
                destination_gas: 0,
                lp: 39,
                relayer_capital: 0
            }
        );
        assert_eq!(
            mock_across_fee_components(141),
            MockAcrossFeeComponents {
                destination_gas: 1,
                lp: 40,
                relayer_capital: 100
            }
        );
    }

    #[test]
    fn mock_across_destination_chain_id_rejects_unrepresentable_values() {
        assert_eq!(
            mock_across_destination_chain_id(U256::from(u64::MAX)).unwrap(),
            u64::MAX
        );
        assert!(
            mock_across_destination_chain_id(U256::from(u64::MAX) + U256::from(1_u64)).is_err()
        );
    }

    /// The mock `GET /swap/approval` returns a `swapTx` whose calldata must
    /// decode back to a real `deposit(...)` call with the exact args derived
    /// from the query — proving the mock is semantically equivalent to real
    /// Across and that production code submitting the returned calldata
    /// on-chain will invoke `MockSpokePool.deposit(...)` with the same args.
    #[tokio::test]
    async fn mock_swap_approval_returns_decodable_deposit_calldata_for_erc20() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let input_token = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
        let output_token = "0x2222222222222222222222222222222222222222";
        let depositor = "0x3333333333333333333333333333333333333333";
        let recipient = "0x4444444444444444444444444444444444444444";
        let amount = 1_000_000u128;
        let origin_chain_id = 1u64;
        let destination_chain_id = 42161u64;

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_spoke_pool_address(spoke_pool),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId={}&destinationChainId={}&inputToken={}&outputToken={}&amount={}&depositor={}&recipient={}",
            server.across_url(),
            origin_chain_id,
            destination_chain_id,
            input_token,
            output_token,
            amount,
            depositor,
            recipient
        );
        let resp = reqwest::get(&url).await.expect("http get");
        let status = resp.status();
        let text = resp.text().await.expect("body");
        assert!(status.is_success(), "status={status} body={text}");
        let body: Value = serde_json::from_str(&text).expect("json body");

        let swap_tx_to = body["swapTx"]["to"].as_str().expect("swapTx.to");
        assert_eq!(swap_tx_to.to_lowercase(), spoke_pool.to_lowercase());
        assert!(body.get("swapTx").is_some());
        assert!(body["swapTx"].get("value").is_none() || body["swapTx"]["value"].is_null());

        let data_hex = body["swapTx"]["data"].as_str().expect("swapTx.data");
        let calldata = hex::decode(data_hex.trim_start_matches("0x")).expect("hex decode");
        let decoded = depositCall::abi_decode(&calldata).expect("decode deposit call");
        assert_eq!(
            decoded.depositor,
            address_to_bytes32(Address::from_str(depositor).unwrap())
        );
        assert_eq!(
            decoded.recipient,
            address_to_bytes32(Address::from_str(recipient).unwrap())
        );
        assert_eq!(
            decoded.inputToken,
            address_to_bytes32(Address::from_str(input_token).unwrap())
        );
        assert_eq!(
            decoded.outputToken,
            address_to_bytes32(Address::from_str(output_token).unwrap())
        );
        assert_eq!(decoded.inputAmount, U256::from(amount));
        assert_eq!(decoded.outputAmount, U256::from(amount));
        assert_eq!(decoded.destinationChainId, U256::from(destination_chain_id));

        let approval_txns = body["approvalTxns"].as_array().expect("approvalTxns");
        assert_eq!(approval_txns.len(), 1);
        let approval_to = approval_txns[0]["to"].as_str().unwrap();
        assert_eq!(approval_to.to_lowercase(), input_token.to_lowercase());
        assert!(approval_txns[0]["gas"].is_null());
        let approval_data = approval_txns[0]["data"].as_str().unwrap();
        let approval_bytes =
            hex::decode(approval_data.trim_start_matches("0x")).expect("approval hex decode");
        let approve = IERC20::approveCall::abi_decode(&approval_bytes).expect("decode approve");
        assert_eq!(approve.spender, Address::from_str(spoke_pool).unwrap());
        assert_eq!(approve.amount, U256::MAX);
        assert_eq!(body["amountType"].as_str(), Some("exactInput"));
        assert_eq!(
            body["crossSwapType"].as_str(),
            Some("bridgeableToBridgeable")
        );
        assert_eq!(body["expectedFillTime"].as_u64(), Some(2));
        assert_eq!(
            body["inputToken"]["address"].as_str(),
            Some(input_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["outputToken"]["address"].as_str(),
            Some(output_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["refundToken"]["address"].as_str(),
            Some(input_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["checks"]["allowance"]["expected"].as_str(),
            Some("1000000")
        );
        assert_eq!(
            body["checks"]["allowance"]["spender"].as_str(),
            Some(spoke_pool.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["checks"]["balance"]["expected"].as_str(),
            Some("1000000")
        );
        assert!(body["steps"].get("approve").is_none());
        assert_eq!(body["steps"]["bridge"]["provider"].as_str(), Some("across"));
        assert_eq!(
            body["steps"]["bridge"]["inputAmount"].as_str(),
            Some("1000000")
        );
        assert_eq!(body["swapTx"]["gas"].as_str(), Some("0"));
        assert!(body["swapTx"].get("maxFeePerGas").is_none());
        assert!(body["swapTx"].get("maxPriorityFeePerGas").is_none());
        assert_eq!(body["swapTx"]["ecosystem"].as_str(), Some("evm"));
        assert_eq!(body["swapTx"]["simulationSuccess"].as_bool(), Some(true));
        assert_eq!(body["fees"]["total"]["amount"].as_str(), Some("0"));
    }

    #[tokio::test]
    async fn mock_swap_approval_skips_approval_for_native_input() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let native_zero = "0x0000000000000000000000000000000000000000";
        let amount = 42u128;
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_spoke_pool_address(spoke_pool),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken={native_zero}&outputToken={native_zero}&amount={amount}&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.across_url()
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(
            body["swapTx"]["value"].as_str(),
            Some(amount.to_string()).as_deref()
        );
        assert!(
            body.get("approvalTxns").is_none() || body["approvalTxns"].is_null(),
            "native input must not include approvalTxns, got: {body}"
        );
    }

    #[tokio::test]
    async fn mock_swap_approval_can_require_bearer_auth_and_integrator_id() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(spoke_pool)
                .with_across_auth("test-key", "rift-test"),
        )
        .await
        .expect("spawn mock integrator");
        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken=0x0000000000000000000000000000000000000000&outputToken=0x0000000000000000000000000000000000000000&amount=42&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.across_url()
        );
        let client = reqwest::Client::new();

        let unauth = client.get(&url).send().await.expect("unauth request");
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
        let unauth_body: Value = unauth.json().await.expect("unauth json");
        assert_eq!(unauth_body["code"], "missing_or_invalid_auth");

        let missing_integrator = client
            .get(&url)
            .bearer_auth("test-key")
            .send()
            .await
            .expect("missing integrator request");
        assert_eq!(
            missing_integrator.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let missing_integrator_body: Value =
            missing_integrator.json().await.expect("integrator json");
        assert_eq!(missing_integrator_body["param"], "integratorId");

        let ok: Value = client
            .get(format!("{url}&integratorId=rift-test"))
            .bearer_auth("test-key")
            .send()
            .await
            .expect("authenticated request")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json");
        assert_eq!(ok["inputAmount"].as_str(), Some("42"));
    }

    #[tokio::test]
    async fn mock_deposit_status_requires_auth_but_not_integrator_id() {
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_auth("test-key", "rift-test"),
        )
        .await
        .expect("spawn mock integrator");
        let url = format!(
            "{}/deposit/status?originChainId=1&depositId=0",
            server.across_url()
        );
        let client = reqwest::Client::new();

        let unauth = client.get(&url).send().await.expect("unauth request");
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);

        let ok: Value = client
            .get(&url)
            .bearer_auth("test-key")
            .send()
            .await
            .expect("authenticated request")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json");
        assert_eq!(ok["status"].as_str(), Some("pending"));
    }

    #[tokio::test]
    async fn mock_swap_approval_supports_min_output_with_deterministic_fee() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let native_zero = "0x0000000000000000000000000000000000000000";
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(spoke_pool)
                .with_across_quote_fee_bps(100),
        )
        .await
        .expect("spawn mock integrator");
        let url = format!(
            "{}/swap/approval?tradeType=minOutput&originChainId=1&destinationChainId=42161&inputToken={native_zero}&outputToken={native_zero}&amount=990&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.across_url()
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["inputAmount"].as_str(), Some("1000"));
        assert_eq!(body["maxInputAmount"].as_str(), Some("1000"));
        assert_eq!(body["expectedOutputAmount"].as_str(), Some("990"));
        assert_eq!(body["minOutputAmount"].as_str(), Some("990"));
        assert_eq!(body["swapTx"]["value"].as_str(), Some("1000"));

        let data_hex = body["swapTx"]["data"].as_str().expect("swapTx.data");
        let calldata = hex::decode(data_hex.trim_start_matches("0x")).expect("hex decode");
        let decoded = depositCall::abi_decode(&calldata).expect("decode deposit call");
        assert_eq!(decoded.inputAmount, U256::from(1000_u64));
        assert_eq!(decoded.outputAmount, U256::from(990_u64));
    }

    #[tokio::test]
    async fn mock_swap_approval_skips_approval_when_allowance_already_exists() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new()
            .prague()
            .block_time(1)
            .try_spawn()
            .expect("anvil spawn");
        let ws_url = anvil.ws_endpoint_url().to_string();
        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&ws_url, private_key)
            .await
            .expect("ws wallet provider");
        let provider: DynProvider = provider.erased();

        let token = GenericEIP3009ERC20Instance::deploy(provider.clone())
            .await
            .expect("deploy token");
        let token_address = *token.address();
        let spoke_pool =
            Address::from_str("0xACE055C0C055D0C035E47055D05E7055055BACE0").expect("spoke pool");
        let depositor = anvil.addresses()[0];
        let recipient = anvil.addresses()[1];
        let amount = U256::from(1_000_000_u64);

        token
            .mint(depositor, amount)
            .send()
            .await
            .expect("mint send")
            .get_receipt()
            .await
            .expect("mint receipt");

        token
            .approve(spoke_pool, amount)
            .send()
            .await
            .expect("approve send")
            .get_receipt()
            .await
            .expect("approve receipt");

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{spoke_pool:#x}"))
                .with_across_evm_rpc_url(ws_url),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken={:#x}&outputToken={:#x}&amount={}&depositor={:#x}&recipient={:#x}",
            server.across_url(),
            token_address,
            Address::ZERO,
            amount,
            depositor,
            recipient,
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json body");

        assert!(
            body.get("approvalTxns").is_none() || body["approvalTxns"].is_null(),
            "existing allowance should skip approvalTxns, got: {body}"
        );
        assert_eq!(
            body["checks"]["allowance"]["actual"].as_str(),
            Some(amount.to_string()).as_deref()
        );
        assert!(body["steps"].get("approve").is_none());
    }

    #[tokio::test]
    async fn mock_swap_approval_uses_origin_chain_rpc_for_erc20_checks() {
        use alloy::node_bindings::Anvil;

        let fallback_anvil = Anvil::new()
            .chain_id(31_337)
            .block_time(1)
            .try_spawn()
            .expect("fallback anvil spawn");
        let origin_anvil = Anvil::new()
            .chain_id(84_530)
            .block_time(1)
            .try_spawn()
            .expect("origin anvil spawn");

        let origin_ws_url = origin_anvil.ws_endpoint_url().to_string();
        let private_key: [u8; 32] = origin_anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&origin_ws_url, private_key)
            .await
            .expect("origin ws wallet provider");
        let provider: DynProvider = provider.erased();

        let token = GenericEIP3009ERC20Instance::deploy(provider.clone())
            .await
            .expect("deploy token");
        let token_address = *token.address();
        let depositor = origin_anvil.addresses()[0];
        let recipient = origin_anvil.addresses()[1];
        let origin_spoke_pool = Address::from_str("0xACE055C0C055D0C035E47055D05E7055055BACE0")
            .expect("origin spoke pool");
        let fallback_spoke_pool = Address::from_str("0x00000000000000000000000000000000000000aC")
            .expect("fallback spoke pool");
        let amount = U256::from(1_000_000_u64);

        token
            .mint(depositor, amount)
            .send()
            .await
            .expect("mint send")
            .get_receipt()
            .await
            .expect("mint receipt");
        token
            .approve(origin_spoke_pool, amount)
            .send()
            .await
            .expect("approve send")
            .get_receipt()
            .await
            .expect("approve receipt");

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{fallback_spoke_pool:#x}"))
                .with_across_evm_rpc_url(fallback_anvil.ws_endpoint_url().to_string())
                .with_across_chain(
                    origin_anvil.chain_id(),
                    format!("{origin_spoke_pool:#x}"),
                    origin_ws_url,
                ),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId={}&destinationChainId=42161&inputToken={:#x}&outputToken={:#x}&amount={}&depositor={:#x}&recipient={:#x}",
            server.across_url(),
            origin_anvil.chain_id(),
            token_address,
            Address::ZERO,
            amount,
            depositor,
            recipient,
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json body");

        assert_eq!(
            body["swapTx"]["to"].as_str(),
            Some(format!("{origin_spoke_pool:#x}").as_str())
        );
        assert!(
            body.get("approvalTxns").is_none() || body["approvalTxns"].is_null(),
            "origin-chain allowance should skip approvalTxns, got: {body}"
        );
        assert_eq!(
            body["checks"]["allowance"]["actual"].as_str(),
            Some(amount.to_string()).as_deref()
        );
        assert_eq!(
            body["checks"]["allowance"]["spender"].as_str(),
            Some(format!("{origin_spoke_pool:#x}").as_str())
        );
    }

    #[tokio::test]
    async fn mock_swap_approval_fails_when_origin_rpc_cannot_read_erc20_checks() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().block_time(1).try_spawn().expect("anvil spawn");
        let spoke_pool =
            Address::from_str("0xACE055C0C055D0C035E47055D05E7055055BACE0").expect("spoke pool");
        let depositor = anvil.addresses()[0];
        let recipient = anvil.addresses()[1];
        let non_contract_token =
            Address::from_str("0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf").expect("token address");

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{spoke_pool:#x}"))
                .with_across_evm_rpc_url(anvil.ws_endpoint_url().to_string()),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId={}&destinationChainId=42161&inputToken={non_contract_token:#x}&outputToken={:#x}&amount=1000000&depositor={depositor:#x}&recipient={recipient:#x}",
            server.across_url(),
            anvil.chain_id(),
            Address::ZERO,
        );
        let response = reqwest::get(&url).await.expect("http get");
        let status = response.status();
        let body: Value = response.json().await.expect("json body");

        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body["code"].as_str(), Some("mock_across_rpc_error"));
        assert_eq!(body["param"].as_str(), Some("inputToken"));
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|message| message.contains("allowance check failed")),
            "unexpected body: {body}"
        );
    }

    /// Indexer-backed `GET /deposit/status` must report `"filled"` once the
    /// MockSpokePool emits `FundsDeposited`, surfacing the decoded tx hash and
    /// deposit fields in the same shape the real Across `/deposit/status`
    /// endpoint returns. Production code polls this endpoint identically for
    /// mock and real Across — this test proves the mock completes the loop.
    #[tokio::test]
    async fn deposit_status_indexer_reports_filled_after_deposit() {
        use crate::across_spoke_pool_mock::MockSpokePool;
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new()
            .prague()
            .block_time(1)
            .try_spawn()
            .expect("anvil spawn");
        let ws_url = anvil.ws_endpoint_url().to_string();
        let chain_id = anvil.chain_id();
        let depositor = anvil.addresses()[0];
        let recipient_addr = anvil.addresses()[1];
        let destination_chain_id = 42161_u64;

        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&ws_url, private_key)
            .await
            .expect("ws wallet provider");
        let provider: DynProvider = provider.erased();

        let spoke_pool = MockSpokePool::deploy(provider).await.expect("deploy");
        let spoke_pool_address = *spoke_pool.address();

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{spoke_pool_address:#x}"))
                .with_across_evm_rpc_url(ws_url)
                .with_mock_service_evm_chain(destination_chain_id, anvil.endpoint()),
        )
        .await
        .expect("spawn mock integrator");

        let amount = U256::from(1_000_u64);
        let fill_deadline: u32 = u32::MAX;
        let receipt = spoke_pool
            .deposit(
                address_to_bytes32(depositor),
                address_to_bytes32(recipient_addr),
                FixedBytes::<32>::ZERO,
                FixedBytes::<32>::ZERO,
                amount,
                amount,
                U256::from(destination_chain_id),
                FixedBytes::<32>::ZERO,
                0,
                fill_deadline,
                0,
                alloy::primitives::Bytes::new(),
            )
            .value(amount)
            .send()
            .await
            .expect("send deposit")
            .get_receipt()
            .await
            .expect("receipt");
        let expected_deposit_tx = format!("{:#x}", receipt.transaction_hash).to_lowercase();

        let status_url = format!(
            "{}/deposit/status?originChainId={}&depositId=0",
            server.across_url(),
            chain_id
        );
        let mut filled_body: Option<Value> = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let body: Value = reqwest::get(&status_url)
                .await
                .expect("http")
                .json()
                .await
                .expect("json body");
            if body["status"] == "filled" {
                filled_body = Some(body);
                break;
            }
        }
        let body = filled_body.expect("deposit status never transitioned to filled");
        assert_eq!(body["status"], "filled");
        assert_eq!(body["originChainId"].as_u64(), Some(chain_id));
        assert_eq!(body["depositId"].as_str(), Some("0"));
        assert_eq!(
            body["destinationChainId"].as_u64(),
            Some(destination_chain_id)
        );
        assert_eq!(
            body["depositTxnRef"].as_str().unwrap().to_lowercase(),
            expected_deposit_tx
        );
        assert!(body["fillTxnRef"].as_str().unwrap().starts_with("0x"));
        assert!(body.get("fillDeadline").is_none() || body["fillDeadline"].is_null());
        assert_eq!(body["pagination"]["currentIndex"].as_u64(), Some(0));
        assert_eq!(body["pagination"]["maxIndex"].as_u64(), Some(0));

        let status_url_by_tx = format!(
            "{}/deposit/status?depositTxnRef={}",
            server.across_url(),
            expected_deposit_tx
        );
        let body_by_tx: Value = reqwest::get(&status_url_by_tx)
            .await
            .expect("http")
            .json()
            .await
            .expect("json body");
        assert_eq!(body_by_tx["status"], "filled");
        assert_eq!(body_by_tx["depositId"].as_str(), Some("0"));
        assert_eq!(
            body_by_tx["depositTxnRef"]
                .as_str()
                .expect("depositTxnRef")
                .to_lowercase(),
            expected_deposit_tx
        );
        assert_eq!(body_by_tx["pagination"]["currentIndex"].as_u64(), Some(0));
        assert_eq!(body_by_tx["pagination"]["maxIndex"].as_u64(), Some(0));
    }

    #[tokio::test]
    async fn deposit_status_mints_erc20_output_before_reporting_filled() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().prague().try_spawn().expect("anvil spawn");
        let ws_url = anvil.ws_endpoint_url().to_string();
        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&ws_url, private_key)
            .await
            .expect("ws wallet provider");
        let provider: DynProvider = provider.erased();
        let token = GenericEIP3009ERC20Instance::deploy(provider.clone())
            .await
            .expect("deploy token");
        let token_address = *token.address();
        let recipient = anvil.addresses()[1];
        let amount = U256::from(12_345_u64);
        let destination_chain_id = 42161_u64;
        let spoke_pool = Address::repeat_byte(0x44);

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_chain(
                    destination_chain_id,
                    format!("{spoke_pool:#x}"),
                    anvil.endpoint(),
                )
                .with_mock_service_evm_chain(destination_chain_id, anvil.endpoint()),
        )
        .await
        .expect("spawn mock integrator");

        let record = MockAcrossDepositRecord {
            origin_chain_id: 1,
            destination_chain_id: U256::from(destination_chain_id),
            deposit_id: U256::from(7_u64),
            depositor: FixedBytes::<32>::ZERO,
            recipient: address_to_bytes32(recipient),
            input_token: FixedBytes::<32>::ZERO,
            output_token: address_to_bytes32(token_address),
            input_amount: amount,
            output_amount: amount,
            fill_deadline: u32::MAX,
            deposit_tx_hash: format!("0x{}", "ab".repeat(32)),
            block_number: 1,
            indexed_at: Utc::now(),
        };
        server
            .state
            .across_deposits
            .lock()
            .await
            .insert((1, "7".to_string()), record);

        let status_url = format!(
            "{}/deposit/status?originChainId=1&depositId=7",
            server.across_url()
        );
        let body: Value = reqwest::get(&status_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json body");
        assert_eq!(body["status"], "filled");

        let balance = token
            .balanceOf(recipient)
            .call()
            .await
            .expect("recipient balance");
        assert_eq!(balance, amount);
    }

    #[tokio::test]
    async fn deposit_status_stays_pending_when_destination_chain_is_not_configured() {
        use alloy::node_bindings::Anvil;

        let origin_anvil = Anvil::new().try_spawn().expect("origin anvil spawn");
        let origin_chain_id = origin_anvil.chain_id();
        let missing_destination_chain_id = 42161_u64;
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_chain(
                origin_chain_id,
                format!("{:#x}", Address::repeat_byte(0x44)),
                origin_anvil.endpoint(),
            ),
        )
        .await
        .expect("spawn mock integrator");

        let record = MockAcrossDepositRecord {
            origin_chain_id,
            destination_chain_id: U256::from(missing_destination_chain_id),
            deposit_id: U256::from(9_u64),
            depositor: FixedBytes::<32>::ZERO,
            recipient: address_to_bytes32(origin_anvil.addresses()[1]),
            input_token: FixedBytes::<32>::ZERO,
            output_token: FixedBytes::<32>::ZERO,
            input_amount: U256::from(1_000_u64),
            output_amount: U256::from(990_u64),
            fill_deadline: u32::MAX,
            deposit_tx_hash: format!("0x{}", "cd".repeat(32)),
            block_number: 1,
            indexed_at: Utc::now(),
        };
        server
            .state
            .across_deposits
            .lock()
            .await
            .insert((origin_chain_id, "9".to_string()), record);

        let status_url = format!(
            "{}/deposit/status?originChainId={origin_chain_id}&depositId=9",
            server.across_url()
        );
        let body: Value = reqwest::get(&status_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["status"], "pending");
        assert_eq!(body["originChainId"].as_u64(), Some(origin_chain_id));
        assert_eq!(
            body["destinationChainId"].as_u64(),
            Some(missing_destination_chain_id)
        );
        assert!(body.get("fillTxnRef").is_none() || body["fillTxnRef"].is_null());
    }

    #[tokio::test]
    async fn deposit_status_by_tx_ref_returns_not_found_before_indexing() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let response = reqwest::get(format!(
            "{}/deposit/status?depositTxnRef=0x{}",
            server.across_url(),
            "ab".repeat(32)
        ))
        .await
        .expect("http");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: Value = response.json().await.expect("json body");
        assert_eq!(body["error"].as_str(), Some("DepositNotFoundException"));
        assert_eq!(
            body["message"].as_str(),
            Some("Deposit not found given the provided constraints")
        );
    }

    #[tokio::test]
    async fn deposit_status_can_delay_and_refund_deterministically() {
        let delayed = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_status_latency(Duration::from_secs(3600)),
        )
        .await
        .expect("spawn delayed mock integrator");
        insert_mock_across_record(
            &delayed,
            7,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .await;
        let delayed_url = format!(
            "{}/deposit/status?originChainId=1&depositId=7",
            delayed.across_url()
        );
        let delayed_body: Value = reqwest::get(&delayed_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(delayed_body["status"], "pending");
        assert_eq!(
            delayed_body["depositTxnRef"].as_str(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

        let refunded = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_refund_probability_bps(10_000),
        )
        .await
        .expect("spawn refunded mock integrator");
        insert_mock_across_record(
            &refunded,
            8,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .await;
        let refunded_url = format!(
            "{}/deposit/status?depositTxnRef=0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            refunded.across_url()
        );
        let refunded_body: Value = reqwest::get(&refunded_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(refunded_body["status"], "refunded");
        assert!(refunded_body["depositRefundTxnRef"]
            .as_str()
            .expect("refund tx")
            .starts_with("0x"));
        assert!(refunded_body.get("fillTxnRef").is_none());
    }

    async fn insert_mock_across_record(
        server: &MockIntegratorServer,
        deposit_id: u64,
        deposit_tx_hash: &str,
    ) {
        let record = MockAcrossDepositRecord {
            origin_chain_id: 1,
            destination_chain_id: U256::from(42161_u64),
            deposit_id: U256::from(deposit_id),
            depositor: FixedBytes::<32>::ZERO,
            recipient: FixedBytes::<32>::ZERO,
            input_token: FixedBytes::<32>::ZERO,
            output_token: FixedBytes::<32>::ZERO,
            input_amount: U256::from(1000_u64),
            output_amount: U256::from(990_u64),
            fill_deadline: u32::MAX,
            deposit_tx_hash: deposit_tx_hash.to_string(),
            block_number: 1,
            indexed_at: Utc::now(),
        };
        server
            .state
            .across_deposits
            .lock()
            .await
            .insert((1, deposit_id.to_string()), record);
    }
}
