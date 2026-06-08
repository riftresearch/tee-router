use alloy::{
    hex,
    primitives::{Address, Bytes, FixedBytes, U256},
    sol_types::SolCall,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr, sync::Arc};
use tokio::sync::Mutex;

pub(crate) mod contract;

use crate::mock_integrators::velora::contract::MockVeloraSwap;
use crate::mock_integrators::parse_amount;

/// Per-venue state for the Velora mock: the simulated USD price book, the
/// per-network swap-contract addresses, the two injectable
/// transaction-failure counters, and the flat quote-fee haircut.
pub(crate) struct VeloraMockState {
    pub(crate) usd_prices: Mutex<BTreeMap<String, u128>>,
    pub(crate) swap_contract_addresses: BTreeMap<u64, String>,
    pub(crate) transaction_failures_remaining: Mutex<usize>,
    pub(crate) transaction_stale_quote_failures_remaining: Mutex<usize>,
    /// Flat fee applied to quote amounts, in basis points: haircuts the `SELL`
    /// output and grosses up the `BUY` input. Zero means a 1:1 price conversion.
    pub(crate) quote_fee_bps: u16,
}

/// Builds the Velora mock router. Mounted under `/velora`; receives its own
/// [`VeloraMockState`] substate at nest time.
pub(crate) fn router() -> Router<Arc<VeloraMockState>> {
    Router::new()
        .route("/prices", get(mock_velora_prices))
        .route("/transactions/:network", post(mock_velora_transaction))
}

const VELORA_NATIVE_TOKEN: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const USD_MICRO: u128 = 1_000_000;
const DEFAULT_ETH_USD_MICRO: u128 = 3_000 * USD_MICRO;
const DEFAULT_BTC_USD_MICRO: u128 = 100_000 * USD_MICRO;
const DEFAULT_USD_STABLE_USD_MICRO: u128 = USD_MICRO;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MockVeloraPricesQuery {
    src_token: String,
    dest_token: String,
    src_decimals: u8,
    dest_decimals: u8,
    amount: String,
    side: String,
    network: u64,
    #[allow(dead_code)]
    version: Option<String>,
    #[allow(dead_code)]
    ignore_bad_usd_price: Option<bool>,
    #[allow(dead_code)]
    exclude_rfq: Option<bool>,
    user_address: Option<String>,
    receiver: Option<String>,
    #[allow(dead_code)]
    partner: Option<String>,
}

pub(crate) async fn mock_velora_prices(
    State(state): State<Arc<VeloraMockState>>,
    Query(query): Query<MockVeloraPricesQuery>,
) -> impl IntoResponse {
    // Mirror real Velora's pair constraint: if `receiver` is set, `userAddress`
    // must also be set. Production hit a 400 here because the router pushed
    // `receiver` without `userAddress` at quote time and the mock silently
    // accepted it. The mock must reject the same way real Velora does.
    let receiver_present = query
        .receiver
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());
    let user_address_present = query
        .user_address
        .as_deref()
        .is_some_and(|s| !s.trim().is_empty());
    if receiver_present && !user_address_present {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "If receiver is defined userAddress should also be defined",
            })),
        )
            .into_response();
    }
    let amount = match parse_amount("amount", &query.amount) {
        Ok(amount) => amount,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_amount",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let (src_amount, dest_amount) = match mock_velora_quote_amounts(&state, &query, amount).await {
        Ok(amounts) => amounts,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "unsupported_pair",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let Some(swap_contract) = state.swap_contract_addresses.get(&query.network) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "unsupported_network",
                "message": format!(
                    "missing mock Velora swap contract for network {}",
                    query.network
                ),
            })),
        )
            .into_response();
    };

    Json(json!({
        "priceRoute": {
            "network": query.network,
            "srcToken": query.src_token,
            "destToken": query.dest_token,
            "srcDecimals": query.src_decimals,
            "destDecimals": query.dest_decimals,
            "srcAmount": src_amount.to_string(),
            "destAmount": dest_amount.to_string(),
            "side": query.side,
            "tokenTransferProxy": swap_contract,
            "contractAddress": swap_contract,
            "bestRoute": [],
        }
    }))
    .into_response()
}

async fn mock_velora_quote_amounts(
    state: &VeloraMockState,
    query: &MockVeloraPricesQuery,
    amount: u128,
) -> Result<(u128, u128), String> {
    let src_symbol = mock_velora_token_symbol(&query.src_token);
    let dest_symbol = mock_velora_token_symbol(&query.dest_token);
    let prices = state.usd_prices.lock().await;
    let src_price = *prices
        .get(src_symbol.as_str())
        .ok_or_else(|| format!("missing USD price for {src_symbol}"))?;
    let dest_price = *prices
        .get(dest_symbol.as_str())
        .ok_or_else(|| format!("missing USD price for {dest_symbol}"))?;

    let fee_bps = state.quote_fee_bps;
    match query.side.as_str() {
        "SELL" => {
            let gross_output = convert_raw_amount_floor(
                amount,
                src_price,
                query.src_decimals,
                dest_price,
                query.dest_decimals,
            )?;
            // Haircut the output so the swap shows a realistic value loss
            // instead of a perfect price conversion.
            let net_output = apply_mock_velora_discount(gross_output, fee_bps)?;
            Ok((amount, net_output))
        }
        "BUY" => {
            let gross_input = convert_raw_amount_ceil(
                amount,
                dest_price,
                query.dest_decimals,
                src_price,
                query.src_decimals,
            )?;
            // Gross up the input the caller must supply to net `amount` out.
            let net_input = gross_up_mock_velora_input(gross_input, fee_bps)?;
            Ok((net_input, amount))
        }
        other => Err(format!("unsupported side: {other}")),
    }
}

/// Haircut `amount` by `fee_bps` (exact-input / `SELL`): the caller keeps
/// `(10_000 - fee_bps) / 10_000` of the converted output.
fn apply_mock_velora_discount(amount: u128, fee_bps: u16) -> Result<u128, String> {
    let keep_bps = 10_000_u128
        .checked_sub(u128::from(fee_bps))
        .ok_or_else(|| "mock Velora fee bps exceed 100%".to_string())?;
    amount
        .checked_mul(keep_bps)
        .map(|value| value / 10_000_u128)
        .ok_or_else(|| "mock Velora exact-input quote amount overflow".to_string())
}

/// Gross up `amount` by `fee_bps` (exact-output / `BUY`): the caller must
/// supply `amount * 10_000 / (10_000 - fee_bps)` to net `amount` out.
fn gross_up_mock_velora_input(amount: u128, fee_bps: u16) -> Result<u128, String> {
    let keep_bps = 10_000_u128
        .checked_sub(u128::from(fee_bps))
        .ok_or_else(|| "mock Velora fee bps exceed 100%".to_string())?
        .max(1);
    amount
        .checked_mul(10_000_u128)
        .map(|value| value.div_ceil(keep_bps))
        .ok_or_else(|| "mock Velora exact-output quote amount overflow".to_string())
}

pub(crate) fn default_velora_usd_prices() -> BTreeMap<String, u128> {
    BTreeMap::from([
        ("ETH".to_string(), DEFAULT_ETH_USD_MICRO),
        ("BTC".to_string(), DEFAULT_BTC_USD_MICRO),
        ("MOCK".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
        ("USDC".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
        ("USDT".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
    ])
}

fn mock_velora_token_symbol(token: &str) -> String {
    let token = token.to_ascii_lowercase();
    match token.as_str() {
        VELORA_NATIVE_TOKEN => "ETH",
        // WETH
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        | "0x4200000000000000000000000000000000000006"
        | "0x82af49447d8a07e3bd95bd0d56f35241523fbab1" => "ETH",
        // USDC
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        | "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        | "0xaf88d065e77c8cc2239327c5edb3a432268e5831" => "USDC",
        // USDT
        "0xdac17f958d2ee523a2206206994597c13d831ec7"
        | "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"
        | "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9" => "USDT",
        // WBTC / cbBTC
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
        | "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf" => "BTC",
        _ => "MOCK",
    }
    .to_string()
}

fn convert_raw_amount_floor(
    amount: u128,
    src_price_usd_micro: u128,
    src_decimals: u8,
    dest_price_usd_micro: u128,
    dest_decimals: u8,
) -> Result<u128, String> {
    let numerator = checked_mul3(
        amount,
        src_price_usd_micro,
        pow10_u128(dest_decimals)?,
        "velora quote numerator overflow",
    )?;
    let denominator = checked_mul(
        dest_price_usd_micro,
        pow10_u128(src_decimals)?,
        "velora quote denominator overflow",
    )?;
    if denominator == 0 {
        return Err("velora quote denominator is zero".to_string());
    }
    Ok(numerator / denominator)
}

fn convert_raw_amount_ceil(
    amount: u128,
    src_price_usd_micro: u128,
    src_decimals: u8,
    dest_price_usd_micro: u128,
    dest_decimals: u8,
) -> Result<u128, String> {
    let numerator = checked_mul3(
        amount,
        src_price_usd_micro,
        pow10_u128(dest_decimals)?,
        "velora quote numerator overflow",
    )?;
    let denominator = checked_mul(
        dest_price_usd_micro,
        pow10_u128(src_decimals)?,
        "velora quote denominator overflow",
    )?;
    if denominator == 0 {
        return Err("velora quote denominator is zero".to_string());
    }
    Ok(numerator / denominator + u128::from(numerator % denominator != 0))
}

fn checked_mul(a: u128, b: u128, message: &str) -> Result<u128, String> {
    a.checked_mul(b).ok_or_else(|| message.to_string())
}

fn checked_mul3(a: u128, b: u128, c: u128, message: &str) -> Result<u128, String> {
    checked_mul(checked_mul(a, b, message)?, c, message)
}

fn pow10_u128(decimals: u8) -> Result<u128, String> {
    10u128
        .checked_pow(u32::from(decimals))
        .ok_or_else(|| format!("unsupported decimal scale {decimals}"))
}

pub(crate) async fn mock_velora_transaction(
    State(state): State<Arc<VeloraMockState>>,
    Path(network): Path<u64>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut stale_quote_failures_remaining = state
        .transaction_stale_quote_failures_remaining
        .lock()
        .await;
    if *stale_quote_failures_remaining > 0 {
        *stale_quote_failures_remaining -= 1;
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "stale_quote",
                "message": "price route expired",
            })),
        )
            .into_response();
    }
    drop(stale_quote_failures_remaining);

    let mut failures_remaining = state.transaction_failures_remaining.lock().await;
    if *failures_remaining > 0 {
        *failures_remaining -= 1;
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "service_unavailable",
                "message": "temporary Velora transaction failure",
            })),
        )
            .into_response();
    }
    drop(failures_remaining);

    let src_token = match mock_velora_required_string(&body, "srcToken") {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_request",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let dest_token = match mock_velora_required_string(&body, "destToken") {
        Ok(value) => value,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_request",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let receiver = match body
        .get("receiver")
        .and_then(Value::as_str)
        .or_else(|| body.get("userAddress").and_then(Value::as_str))
    {
        Some(value) => value,
        None => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_request",
                    "message": "missing receiver or userAddress",
                })),
            )
                .into_response();
        }
    };
    let src_amount = match mock_velora_price_route_amount(&body, "srcAmount") {
        Ok(amount) => amount,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_price_route",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let dest_amount = match mock_velora_price_route_amount(&body, "destAmount") {
        Ok(amount) => amount,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_price_route",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let Some(swap_contract) = state.swap_contract_addresses.get(&network) else {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "unsupported_network",
                "message": format!("missing mock Velora swap contract for network {network}"),
            })),
        )
            .into_response();
    };
    let src_token = match mock_velora_evm_token_address(src_token) {
        Ok(address) => address,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_src_token",
                    "message": error.to_string(),
                })),
            )
                .into_response();
        }
    };
    let dest_token = match mock_velora_evm_token_address(dest_token) {
        Ok(address) => address,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_dest_token",
                    "message": error.to_string(),
                })),
            )
                .into_response();
        }
    };
    let receiver = match Address::from_str(receiver) {
        Ok(address) => address,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_receiver",
                    "message": error.to_string(),
                })),
            )
                .into_response();
        }
    };
    let value = if src_token == Address::ZERO {
        src_amount.to_string()
    } else {
        "0".to_string()
    };
    // Encode real AugustusV6 `swapExactAmountIn` calldata. `executor`,
    // `partnerAndFee`, `permit`, `executorData` are zeroed/empty — the mock
    // ignores them but the signature must match V6 byte-for-byte so consumer
    // code that decodes the calldata sees the same layout it would on
    // mainnet/L2.
    let call = MockVeloraSwap::swapExactAmountInCall {
        _0: Address::ZERO, // executor
        swapData: MockVeloraSwap::SwapData {
            srcToken: src_token,
            destToken: dest_token,
            fromAmount: U256::from(src_amount),
            toAmount: U256::from(dest_amount),
            quotedAmount: U256::from(dest_amount),
            metadata: FixedBytes::<32>::ZERO,
            beneficiary: receiver,
        },
        _2: U256::ZERO,   // partnerAndFee
        _3: Bytes::new(), // permit
        _4: Bytes::new(), // executorData
    };
    Json(json!({
        "network": network,
        "to": swap_contract,
        "data": format!("0x{}", hex::encode(call.abi_encode())),
        "value": value,
        "request": body,
    }))
    .into_response()
}

fn mock_velora_required_string<'a>(body: &'a Value, name: &str) -> Result<&'a str, String> {
    body.get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing {name}"))
}

fn mock_velora_evm_token_address(token: &str) -> Result<Address, <Address as FromStr>::Err> {
    if token.eq_ignore_ascii_case(VELORA_NATIVE_TOKEN) {
        Ok(Address::ZERO)
    } else {
        Address::from_str(token)
    }
}

fn mock_velora_price_route_amount(body: &Value, name: &str) -> Result<u128, String> {
    let price_route = body
        .get("priceRoute")
        .ok_or_else(|| "missing priceRoute".to_string())?;
    let value = price_route
        .get(name)
        .ok_or_else(|| format!("missing priceRoute.{name}"))?;
    mock_velora_u128(value)
}

fn mock_velora_u128(value: &Value) -> Result<u128, String> {
    match value {
        Value::String(raw) => parse_amount("velora amount", raw),
        Value::Number(number) => number
            .as_u64()
            .map(u128::from)
            .ok_or_else(|| format!("velora amount is not a u64: {number}")),
        other => Err(format!("velora amount is not a string/number: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_integrators::{MockIntegratorConfig, MockIntegratorServer};

    async fn spawn_mock_velora_server() -> MockIntegratorServer {
        MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_velora_swap_contract_address(1, "0x0000000000000000000000000000000000000002"),
        )
        .await
        .expect("spawn mock integrator")
    }

    #[tokio::test]
    async fn mock_velora_prices_sell_eth_to_usdc_uses_prices_and_decimals() {
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken={}&destToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&srcDecimals=18&destDecimals=6&amount=1000000000000000000&side=SELL&network=1",
            server.velora_url(),
            VELORA_NATIVE_TOKEN
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["priceRoute"]["srcAmount"], "1000000000000000000");
        assert_eq!(body["priceRoute"]["destAmount"], "3000000000");
        assert_eq!(body["priceRoute"]["side"], "SELL");
    }

    #[tokio::test]
    async fn mock_velora_prices_buy_usdc_with_eth_uses_prices_and_decimals() {
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken={}&destToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&srcDecimals=18&destDecimals=6&amount=3000000000&side=BUY&network=1",
            server.velora_url(),
            VELORA_NATIVE_TOKEN
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["priceRoute"]["srcAmount"], "1000000000000000000");
        assert_eq!(body["priceRoute"]["destAmount"], "3000000000");
        assert_eq!(body["priceRoute"]["side"], "BUY");
    }

    #[tokio::test]
    async fn mock_velora_prices_sell_usdc_to_eth_uses_prices_and_decimals() {
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&destToken={}&srcDecimals=6&destDecimals=18&amount=60000000&side=SELL&network=1",
            server.velora_url(),
            VELORA_NATIVE_TOKEN
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["priceRoute"]["srcAmount"], "60000000");
        assert_eq!(body["priceRoute"]["destAmount"], "20000000000000000");
    }

    #[tokio::test]
    async fn mock_velora_prices_unknown_erc20_with_deterministic_mock_price() {
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken=0x3333333333333333333333333333333333333333&destToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&srcDecimals=18&destDecimals=6&amount=1000000000000000000&side=SELL&network=1",
            server.velora_url()
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["priceRoute"]["srcAmount"], "1000000000000000000");
        assert_eq!(body["priceRoute"]["destAmount"], "1000000");
    }

    #[tokio::test]
    async fn mock_velora_prices_rejects_receiver_without_user_address() {
        // Mirror real Velora: if `receiver` is set, `userAddress` must be set
        // too. The router used to pass receiver alone at quote time and the
        // mock silently succeeded — this guards against the regression.
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken={}&destToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&srcDecimals=18&destDecimals=6&amount=1000000000000000000&side=SELL&network=1&receiver=0x1111111111111111111111111111111111111111",
            server.velora_url(),
            VELORA_NATIVE_TOKEN,
        );
        let response = reqwest::get(&url).await.expect("http get");
        assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
        let body: Value = response.json().await.expect("json body");
        assert!(
            body["error"]
                .as_str()
                .unwrap_or_default()
                .contains("userAddress"),
            "expected error mentioning userAddress, got {body}"
        );
    }

    #[tokio::test]
    async fn mock_velora_prices_accepts_receiver_with_user_address() {
        // Both fields set → request succeeds (production "execution time" or
        // post-fallback quote shape).
        let server = spawn_mock_velora_server().await;
        let url = format!(
            "{}/prices?srcToken={}&destToken=0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48&srcDecimals=18&destDecimals=6&amount=1000000000000000000&side=SELL&network=1&receiver=0x1111111111111111111111111111111111111111&userAddress=0x1111111111111111111111111111111111111111",
            server.velora_url(),
            VELORA_NATIVE_TOKEN,
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");
        assert_eq!(body["priceRoute"]["srcAmount"], "1000000000000000000");
    }

    #[tokio::test]
    async fn mock_velora_transaction_rejects_missing_required_fields() {
        let server = spawn_mock_velora_server().await;
        let base_body = json!({
            "srcToken": VELORA_NATIVE_TOKEN,
            "destToken": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "receiver": "0x1111111111111111111111111111111111111111",
            "priceRoute": {
                "srcAmount": "1000000000000000000",
                "destAmount": "3000000000"
            }
        });
        let cases = [
            ("srcToken", "missing srcToken"),
            ("destToken", "missing destToken"),
            ("receiver", "missing receiver or userAddress"),
        ];

        for (field, expected_message) in cases {
            let mut body = base_body.clone();
            body.as_object_mut().expect("object body").remove(field);
            let response = reqwest::Client::new()
                .post(format!("{}/transactions/1", server.velora_url()))
                .json(&body)
                .send()
                .await
                .expect("http");

            assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
            let error: Value = response.json().await.expect("error body");
            assert_eq!(error["error"], "invalid_request");
            assert_eq!(error["message"], expected_message);
        }
    }
}
