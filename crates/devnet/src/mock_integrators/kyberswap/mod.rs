use alloy::{
    hex,
    primitives::{address, Address, Bytes, U256},
    sol_types::SolCall,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, str::FromStr, sync::Arc};
use tokio::sync::Mutex;

use crate::mock_integrators::parse_amount;
use contract::MockKyberSwapRouter;

pub(crate) mod contract;

/// KyberSwap's native-asset sentinel address (`0xEee…EEeE`), the 20-byte form of
/// [`KYBERSWAP_NATIVE_TOKEN`]. Matches `MockKyberSwapRouter`'s `NATIVE` constant.
const KYBERSWAP_NATIVE_ADDRESS: Address = address!("EeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE");

pub(crate) struct KyberswapMockState {
    pub(crate) usd_prices: Mutex<BTreeMap<String, u128>>,
    pub(crate) swap_contract_addresses: BTreeMap<u64, String>,
    pub(crate) transaction_failures_remaining: Mutex<usize>,
    pub(crate) transaction_stale_quote_failures_remaining: Mutex<usize>,
    pub(crate) quote_fee_bps: u16,
}

pub(crate) fn router() -> Router<Arc<KyberswapMockState>> {
    Router::new()
        .route("/:chain/api/v1/routes", get(mock_kyberswap_routes))
        .route(
            "/:chain/api/v1/route/build",
            post(mock_kyberswap_route_build),
        )
}

const KYBERSWAP_NATIVE_TOKEN: &str = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
const USD_MICRO: u128 = 1_000_000;
const DEFAULT_ETH_USD_MICRO: u128 = 3_000 * USD_MICRO;
const DEFAULT_BTC_USD_MICRO: u128 = 100_000 * USD_MICRO;
const DEFAULT_USD_STABLE_USD_MICRO: u128 = USD_MICRO;

pub(crate) fn default_kyberswap_usd_prices() -> BTreeMap<String, u128> {
    BTreeMap::from([
        ("ETH".to_string(), DEFAULT_ETH_USD_MICRO),
        ("BTC".to_string(), DEFAULT_BTC_USD_MICRO),
        ("MOCK".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
        ("USDC".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
        ("USDT".to_string(), DEFAULT_USD_STABLE_USD_MICRO),
    ])
}

async fn mock_kyberswap_routes(
    State(state): State<Arc<KyberswapMockState>>,
    Path(chain): Path<String>,
    Query(query): Query<BTreeMap<String, String>>,
) -> impl IntoResponse {
    let Some(network) = kyberswap_network_for_slug(&chain) else {
        return kyberswap_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            4221,
            format!("unsupported KyberSwap chain: {chain}"),
        );
    };
    let token_in = match required_query(&query, "tokenIn") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let token_out = match required_query(&query, "tokenOut") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let amount_in = match required_query(&query, "amountIn").and_then(|value| {
        parse_amount("amountIn", value)
            .map_err(|error| kyberswap_error(StatusCode::BAD_REQUEST, 4001, error))
    }) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(router_address) = state.swap_contract_addresses.get(&network) else {
        return kyberswap_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            4221,
            format!("missing mock KyberSwap swap contract for network {network}"),
        );
    };
    let amount_out = match mock_kyberswap_quote_amount(&state, token_in, token_out, amount_in).await
    {
        Ok(value) => value,
        Err(error) => return kyberswap_error(StatusCode::UNPROCESSABLE_ENTITY, 4010, error),
    };
    let amount_in_usd = mock_kyberswap_amount_usd(&state, token_in, amount_in).await;
    let amount_out_usd = mock_kyberswap_amount_usd(&state, token_out, amount_out).await;
    let timestamp = chrono::Utc::now().timestamp();

    Json(json!({
        "code": 0,
        "message": "successfully",
        "data": {
            "routeSummary": {
                "tokenIn": token_in,
                "amountIn": amount_in.to_string(),
                "amountInUsd": amount_in_usd,
                "tokenOut": token_out,
                "amountOut": amount_out.to_string(),
                "amountOutUsd": amount_out_usd,
                "gas": "360000",
                "gasPrice": "1",
                "gasUsd": "0.01",
                "l1FeeUsd": "0",
                "route": [],
                "routeID": "mock-route",
                "checksum": "mock",
                "timestamp": timestamp.to_string(),
            },
            "routerAddress": router_address,
        },
        "requestId": "mock",
    }))
    .into_response()
}

async fn mock_kyberswap_route_build(
    State(state): State<Arc<KyberswapMockState>>,
    Path(chain): Path<String>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let Some(network) = kyberswap_network_for_slug(&chain) else {
        return kyberswap_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            4221,
            format!("unsupported KyberSwap chain: {chain}"),
        );
    };

    let mut stale_quote_failures_remaining = state
        .transaction_stale_quote_failures_remaining
        .lock()
        .await;
    if *stale_quote_failures_remaining > 0 {
        *stale_quote_failures_remaining -= 1;
        return kyberswap_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            4008,
            "route summary expired",
        );
    }
    drop(stale_quote_failures_remaining);

    let mut failures_remaining = state.transaction_failures_remaining.lock().await;
    if *failures_remaining > 0 {
        *failures_remaining -= 1;
        return kyberswap_error(
            StatusCode::SERVICE_UNAVAILABLE,
            5000,
            "temporary KyberSwap route build failure",
        );
    }
    drop(failures_remaining);

    let route_summary = match body.get("routeSummary") {
        Some(Value::Object(_)) => body.get("routeSummary").expect("checked above"),
        _ => return kyberswap_error(StatusCode::BAD_REQUEST, 4001, "missing routeSummary"),
    };
    let sender = match required_body_string(&body, "sender") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let recipient = match required_body_string(&body, "recipient") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let token_in = match required_body_string(route_summary, "tokenIn") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let token_out = match required_body_string(route_summary, "tokenOut") {
        Ok(value) => value,
        Err(response) => return response,
    };
    let amount_in = match required_body_string(route_summary, "amountIn").and_then(|value| {
        parse_amount("routeSummary.amountIn", value)
            .map_err(|error| kyberswap_error(StatusCode::BAD_REQUEST, 4001, error))
    }) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let amount_out = match required_body_string(route_summary, "amountOut").and_then(|value| {
        parse_amount("routeSummary.amountOut", value)
            .map_err(|error| kyberswap_error(StatusCode::BAD_REQUEST, 4001, error))
    }) {
        Ok(value) => value,
        Err(response) => return response,
    };
    let Some(router_address) = state.swap_contract_addresses.get(&network) else {
        return kyberswap_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            4221,
            format!("missing mock KyberSwap swap contract for network {network}"),
        );
    };
    let src_token = match kyberswap_evm_token_address(token_in) {
        Ok(address) => address,
        Err(error) => return kyberswap_error(StatusCode::BAD_REQUEST, 4011, error.to_string()),
    };
    let dest_token = match kyberswap_evm_token_address(token_out) {
        Ok(address) => address,
        Err(error) => return kyberswap_error(StatusCode::BAD_REQUEST, 4011, error.to_string()),
    };
    let _sender = match Address::from_str(sender) {
        Ok(address) => address,
        Err(error) => return kyberswap_error(StatusCode::BAD_REQUEST, 4001, error.to_string()),
    };
    let recipient = match Address::from_str(recipient) {
        Ok(address) => address,
        Err(error) => return kyberswap_error(StatusCode::BAD_REQUEST, 4001, error.to_string()),
    };
    let transaction_value = if src_token == KYBERSWAP_NATIVE_ADDRESS {
        amount_in.to_string()
    } else {
        "0".to_string()
    };
    // Encode against the real `MetaAggregationRouterV2.swap` ABI. The mock
    // router fills exactly at `minReturnAmount`, so set it to the quoted output.
    let call = MockKyberSwapRouter::swapCall {
        execution: MockKyberSwapRouter::SwapExecutionParams {
            callTarget: Address::ZERO,
            approveTarget: Address::ZERO,
            targetData: Bytes::new(),
            desc: MockKyberSwapRouter::SwapDescriptionV2 {
                srcToken: src_token,
                dstToken: dest_token,
                srcReceivers: Vec::new(),
                srcAmounts: Vec::new(),
                feeReceivers: Vec::new(),
                feeAmounts: Vec::new(),
                dstReceiver: recipient,
                amount: U256::from(amount_in),
                minReturnAmount: U256::from(amount_out),
                flags: U256::ZERO,
                permit: Bytes::new(),
            },
            clientData: Bytes::new(),
        },
    };

    Json(json!({
        "code": 0,
        "message": "successfully",
        "data": {
            "amountIn": amount_in.to_string(),
            "amountOut": amount_out.to_string(),
            "gas": "360000",
            "gasUsd": "0.01",
            "data": format!("0x{}", hex::encode(call.abi_encode())),
            "routerAddress": router_address,
            "transactionValue": transaction_value,
        },
        "requestId": "mock",
    }))
    .into_response()
}

async fn mock_kyberswap_quote_amount(
    state: &KyberswapMockState,
    token_in: &str,
    token_out: &str,
    amount_in: u128,
) -> Result<u128, String> {
    let src_symbol = kyberswap_token_symbol(token_in);
    let dest_symbol = kyberswap_token_symbol(token_out);
    let prices = state.usd_prices.lock().await;
    let src_price = *prices
        .get(src_symbol.as_str())
        .ok_or_else(|| format!("missing USD price for {src_symbol}"))?;
    let dest_price = *prices
        .get(dest_symbol.as_str())
        .ok_or_else(|| format!("missing USD price for {dest_symbol}"))?;
    let gross_output = convert_raw_amount_floor(
        amount_in,
        src_price,
        kyberswap_token_decimals(token_in),
        dest_price,
        kyberswap_token_decimals(token_out),
    )?;
    apply_discount(gross_output, state.quote_fee_bps)
}

async fn mock_kyberswap_amount_usd(
    state: &KyberswapMockState,
    token: &str,
    amount: u128,
) -> String {
    let symbol = kyberswap_token_symbol(token);
    let prices = state.usd_prices.lock().await;
    let price = prices.get(symbol.as_str()).copied().unwrap_or(USD_MICRO);
    let scale = pow10_u128(kyberswap_token_decimals(token)).unwrap_or(1);
    format_decimal_micro(amount.saturating_mul(price) / scale)
}

fn required_query<'a>(
    query: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, axum::response::Response> {
    query
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| kyberswap_error(StatusCode::BAD_REQUEST, 4001, format!("missing {name}")))
}

fn required_body_string<'a>(
    body: &'a Value,
    name: &str,
) -> Result<&'a str, axum::response::Response> {
    body.get(name)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| kyberswap_error(StatusCode::BAD_REQUEST, 4001, format!("missing {name}")))
}

fn kyberswap_error(
    status: StatusCode,
    code: i64,
    message: impl Into<String>,
) -> axum::response::Response {
    (
        status,
        Json(json!({
            "code": code,
            "message": message.into(),
            "data": null,
            "requestId": "mock",
        })),
    )
        .into_response()
}

fn kyberswap_network_for_slug(chain: &str) -> Option<u64> {
    match chain {
        "ethereum" => Some(1),
        "optimism" => Some(10),
        "bsc" => Some(56),
        "unichain" => Some(130),
        "polygon" => Some(137),
        "monad" => Some(143),
        "sonic" => Some(146),
        "hyperevm" => Some(999),
        "ronin" => Some(2020),
        "megaeth" => Some(4326),
        "mantle" => Some(5000),
        "base" => Some(8453),
        "plasma" => Some(9745),
        "arbitrum" => Some(42161),
        "etherlink" => Some(42793),
        "avalanche" => Some(43114),
        "linea" => Some(59144),
        "berachain" => Some(80094),
        _ => None,
    }
}

fn kyberswap_evm_token_address(token: &str) -> Result<Address, <Address as FromStr>::Err> {
    if token.eq_ignore_ascii_case(KYBERSWAP_NATIVE_TOKEN) {
        Ok(KYBERSWAP_NATIVE_ADDRESS)
    } else {
        Address::from_str(token)
    }
}

fn kyberswap_token_symbol(token: &str) -> String {
    let token = token.to_ascii_lowercase();
    match token.as_str() {
        KYBERSWAP_NATIVE_TOKEN => "ETH",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        | "0x4200000000000000000000000000000000000006"
        | "0x82af49447d8a07e3bd95bd0d56f35241523fbab1" => "ETH",
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        | "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        | "0xaf88d065e77c8cc2239327c5edb3a432268e5831" => "USDC",
        "0xdac17f958d2ee523a2206206994597c13d831ec7"
        | "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"
        | "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9" => "USDT",
        "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
        | "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf" => "BTC",
        _ => "MOCK",
    }
    .to_string()
}

fn kyberswap_token_decimals(token: &str) -> u8 {
    match kyberswap_token_symbol(token).as_str() {
        "USDC" | "USDT" => 6,
        "BTC" => 8,
        _ => 18,
    }
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
        "kyberswap quote numerator overflow",
    )?;
    let denominator = checked_mul(
        dest_price_usd_micro,
        pow10_u128(src_decimals)?,
        "kyberswap quote denominator overflow",
    )?;
    if denominator == 0 {
        return Err("kyberswap quote denominator is zero".to_string());
    }
    Ok(numerator / denominator)
}

fn apply_discount(amount: u128, fee_bps: u16) -> Result<u128, String> {
    let keep_bps = 10_000_u128
        .checked_sub(u128::from(fee_bps))
        .ok_or_else(|| "mock KyberSwap fee bps exceed 100%".to_string())?;
    amount
        .checked_mul(keep_bps)
        .map(|value| value / 10_000_u128)
        .ok_or_else(|| "mock KyberSwap exact-input quote amount overflow".to_string())
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

fn format_decimal_micro(value_micro: u128) -> String {
    let whole = value_micro / USD_MICRO;
    let fraction = value_micro % USD_MICRO;
    if fraction == 0 {
        whole.to_string()
    } else {
        let mut raw = format!("{whole}.{fraction:06}");
        while raw.ends_with('0') {
            raw.pop();
        }
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_integrators::{MockIntegratorConfig, MockIntegratorServer};

    const ETH_USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const MOCK_ROUTER: &str = "0x0000000000000000000000000000000000000002";
    const SENDER: &str = "0x1111111111111111111111111111111111111111";
    const RECIPIENT: &str = "0x2222222222222222222222222222222222222222";

    async fn spawn_mock_kyberswap_server_with(
        config: MockIntegratorConfig,
    ) -> MockIntegratorServer {
        MockIntegratorServer::spawn_with_config(
            config.with_kyberswap_swap_contract_address(1, MOCK_ROUTER),
        )
        .await
        .expect("spawn mock integrator")
    }

    async fn spawn_mock_kyberswap_server() -> MockIntegratorServer {
        spawn_mock_kyberswap_server_with(MockIntegratorConfig::default()).await
    }

    fn routes_url(
        server: &MockIntegratorServer,
        token_in: &str,
        token_out: &str,
        amount_in: &str,
    ) -> String {
        format!(
            "{}/ethereum/api/v1/routes?tokenIn={token_in}&tokenOut={token_out}&amountIn={amount_in}&gasInclude=true",
            server.kyberswap_url(),
        )
    }

    async fn get_json(url: &str) -> (reqwest::StatusCode, Value) {
        let response = reqwest::get(url).await.expect("http get");
        let status = response.status();
        let body = response.json().await.expect("json body");
        (status, body)
    }

    #[tokio::test]
    async fn routes_sell_eth_to_usdc_uses_prices_and_decimals() {
        let server = spawn_mock_kyberswap_server().await;
        let url = routes_url(
            &server,
            KYBERSWAP_NATIVE_TOKEN,
            ETH_USDC,
            "1000000000000000000",
        );
        let (status, body) = get_json(&url).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(body["code"], 0);
        let summary = &body["data"]["routeSummary"];
        // 1 ETH @ $3000 -> 3000 USDC (6 decimals).
        assert_eq!(summary["amountIn"], "1000000000000000000");
        assert_eq!(summary["amountOut"], "3000000000");
        assert_eq!(summary["amountInUsd"], "3000");
        assert_eq!(summary["amountOutUsd"], "3000");
        assert_eq!(body["data"]["routerAddress"], MOCK_ROUTER);
    }

    #[tokio::test]
    async fn routes_sell_usdc_to_eth_uses_prices_and_decimals() {
        let server = spawn_mock_kyberswap_server().await;
        let url = routes_url(&server, ETH_USDC, KYBERSWAP_NATIVE_TOKEN, "60000000");
        let (status, body) = get_json(&url).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        // 60 USDC @ $1 / ETH @ $3000 -> 0.02 ETH (18 decimals).
        assert_eq!(
            body["data"]["routeSummary"]["amountOut"],
            "20000000000000000"
        );
    }

    #[tokio::test]
    async fn routes_unknown_erc20_uses_deterministic_mock_price() {
        let server = spawn_mock_kyberswap_server().await;
        let url = routes_url(
            &server,
            "0x3333333333333333333333333333333333333333",
            ETH_USDC,
            "1000000000000000000",
        );
        let (status, body) = get_json(&url).await;
        assert_eq!(status, reqwest::StatusCode::OK);
        // Unknown 18-decimal token defaults to $1 -> 1 USDC.
        assert_eq!(body["data"]["routeSummary"]["amountOut"], "1000000");
    }

    #[tokio::test]
    async fn routes_apply_quote_fee_bps_haircut() {
        let server = spawn_mock_kyberswap_server_with(
            MockIntegratorConfig::default().with_kyberswap_quote_fee_bps(100),
        )
        .await;
        let url = routes_url(
            &server,
            KYBERSWAP_NATIVE_TOKEN,
            ETH_USDC,
            "1000000000000000000",
        );
        let (_, body) = get_json(&url).await;
        // 3000 USDC haircut by 100 bps (1%) -> 2970 USDC.
        assert_eq!(body["data"]["routeSummary"]["amountOut"], "2970000000");
    }

    #[tokio::test]
    async fn routes_reject_unsupported_chain() {
        let server = spawn_mock_kyberswap_server().await;
        let url = format!(
            "{}/notachain/api/v1/routes?tokenIn={KYBERSWAP_NATIVE_TOKEN}&tokenOut={ETH_USDC}&amountIn=1",
            server.kyberswap_url(),
        );
        let (status, body) = get_json(&url).await;
        assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], 4221);
    }

    #[tokio::test]
    async fn routes_reject_missing_amount_in() {
        let server = spawn_mock_kyberswap_server().await;
        let url = format!(
            "{}/ethereum/api/v1/routes?tokenIn={KYBERSWAP_NATIVE_TOKEN}&tokenOut={ETH_USDC}",
            server.kyberswap_url(),
        );
        let (status, body) = get_json(&url).await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], 4001);
    }

    async fn post_build(
        server: &MockIntegratorServer,
        body: Value,
    ) -> (reqwest::StatusCode, Value) {
        let response = reqwest::Client::new()
            .post(format!(
                "{}/ethereum/api/v1/route/build",
                server.kyberswap_url()
            ))
            .json(&body)
            .send()
            .await
            .expect("http post");
        let status = response.status();
        let json = response.json().await.expect("json body");
        (status, json)
    }

    fn route_summary(token_in: &str, token_out: &str, amount_in: &str, amount_out: &str) -> Value {
        json!({
            "tokenIn": token_in,
            "tokenOut": token_out,
            "amountIn": amount_in,
            "amountOut": amount_out,
        })
    }

    #[tokio::test]
    async fn route_build_encodes_swap_calldata_and_zero_value_for_erc20_input() {
        let server = spawn_mock_kyberswap_server().await;
        let (status, body) = post_build(
            &server,
            json!({
                "routeSummary": route_summary(ETH_USDC, KYBERSWAP_NATIVE_TOKEN, "60000000", "20000000000000000"),
                "sender": SENDER,
                "recipient": RECIPIENT,
            }),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK);
        assert_eq!(body["code"], 0);
        assert_eq!(body["data"]["routerAddress"], MOCK_ROUTER);
        // ERC20 input -> no native value attached.
        assert_eq!(body["data"]["transactionValue"], "0");
        let data = body["data"]["data"].as_str().expect("calldata string");
        assert!(data.starts_with("0x") && data.len() > 2, "calldata: {data}");
        // Decodes against the real MetaAggregationRouterV2 `swap` ABI.
        let bytes = Bytes::from_str(data).expect("hex calldata");
        let decoded = MockKyberSwapRouter::swapCall::abi_decode(&bytes).expect("decode swap call");
        assert_eq!(
            decoded.execution.desc.dstReceiver,
            Address::from_str(RECIPIENT).unwrap()
        );
        assert_eq!(decoded.execution.desc.amount, U256::from(60000000u64));
        assert_eq!(
            decoded.execution.desc.minReturnAmount,
            U256::from(20000000000000000u64)
        );
    }

    #[tokio::test]
    async fn route_build_attaches_native_value_for_native_input() {
        let server = spawn_mock_kyberswap_server().await;
        let (status, body) = post_build(
            &server,
            json!({
                "routeSummary": route_summary(KYBERSWAP_NATIVE_TOKEN, ETH_USDC, "1000000000000000000", "3000000000"),
                "sender": SENDER,
                "recipient": RECIPIENT,
            }),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::OK);
        // Native input -> transactionValue equals amountIn.
        assert_eq!(body["data"]["transactionValue"], "1000000000000000000");
    }

    #[tokio::test]
    async fn route_build_rejects_missing_route_summary_and_recipient() {
        let server = spawn_mock_kyberswap_server().await;
        let (status, body) = post_build(&server, json!({ "sender": SENDER })).await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], 4001);

        let (status, _) = post_build(
            &server,
            json!({
                "routeSummary": route_summary(ETH_USDC, KYBERSWAP_NATIVE_TOKEN, "1", "1"),
                "sender": SENDER,
            }),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn route_build_transaction_failure_injection_returns_503() {
        let server = spawn_mock_kyberswap_server().await;
        server.set_kyberswap_transaction_fail_next_n(1).await;
        let request = json!({
            "routeSummary": route_summary(ETH_USDC, KYBERSWAP_NATIVE_TOKEN, "60000000", "20000000000000000"),
            "sender": SENDER,
            "recipient": RECIPIENT,
        });
        let (status, body) = post_build(&server, request.clone()).await;
        assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["code"], 5000);
        // The next attempt succeeds (only one failure was injected).
        let (status, _) = post_build(&server, request).await;
        assert_eq!(status, reqwest::StatusCode::OK);
    }

    #[tokio::test]
    async fn route_build_stale_quote_injection_returns_4008() {
        let server = spawn_mock_kyberswap_server().await;
        server
            .set_kyberswap_transaction_stale_quote_fail_next_n(1)
            .await;
        let (status, body) = post_build(
            &server,
            json!({
                "routeSummary": route_summary(ETH_USDC, KYBERSWAP_NATIVE_TOKEN, "60000000", "20000000000000000"),
                "sender": SENDER,
                "recipient": RECIPIENT,
            }),
        )
        .await;
        assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["code"], 4008);
    }
}
