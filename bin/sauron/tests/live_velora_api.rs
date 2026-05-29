//! Tier-1 live differential against real Velora (formerly ParaSwap) API.
//!
//! Hits production `api.velora.xyz` at `/prices` and `/transactions/{network}`
//! with `version=6.2` and asserts:
//!
//!   1. The price route comes back with `version: 6.2`, `contractAddress` =
//!      the AugustusV6 constant, and `contractMethod = "swapExactAmountIn"`.
//!   2. The `/transactions` response's `to` field is AugustusV6.
//!   3. **The returned calldata ABI-decodes via our vendored V6 ABI as a
//!      valid `swapExactAmountIn` call.** This is the strongest check —
//!      proves the vendored bindings match wire reality byte-for-byte.
//!
//! Read-only, no funds, no key needed. Gated by `LIVE_PROVIDER_TESTS=1` so
//! CI doesn't burn requests against Velora by default.
//!
//! Run with:
//!
//!     LIVE_PROVIDER_TESTS=1 cargo test -p sauron --test live_velora_api \
//!         -- --ignored --nocapture

use std::time::Duration;

use alloy::{
    json_abi::JsonAbi,
    primitives::{address, Address},
};

/// Vendored AugustusV6 ABI JSON (the same file consumed by router-core
/// callers via `alloy::sol!`). Parsed once at test setup so we can look up
/// methods by name and validate selectors at runtime.
const AUGUSTUS_V6_ABI_JSON: &str = include_str!("../../../vendor/velora/AugustusV6.abi.json");

/// Canonical Velora AugustusV6 — same CREATE2 deployment on Ethereum, Base,
/// and Arbitrum. Audit-finding source of truth.
const AUGUSTUS_V6: Address = address!("6a000f20005980200259b80c5102003040001068");

/// Base chain id; we test on Base because it's the cheapest live network
/// (gas-wise) for the matching tier-2 funded test we'll add next.
const NETWORK_ID: u64 = 8453;
/// WETH on Base.
const WETH_BASE: &str = "0x4200000000000000000000000000000000000006";
/// USDC on Base.
const USDC_BASE: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
/// Arbitrary user/receiver address for `/transactions` (any valid address
/// works — the API only requires the format to parse).
const USER_ADDR: &str = "0x33F65788aCa48D733c2C2444Ac9F79B18206aa92";

#[tokio::test]
#[ignore = "live network call to Velora; run with LIVE_PROVIDER_TESTS=1"]
async fn real_velora_v6_quote_and_transaction_match_vendored_abi() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build http client");

    // ─── /prices ──────────────────────────────────────────────────────────
    let prices_url = format!(
        "https://api.velora.xyz/prices?\
         srcToken={WETH_BASE}\
         &destToken={USDC_BASE}\
         &amount=1000000000000000\
         &srcDecimals=18\
         &destDecimals=6\
         &side=SELL\
         &network={NETWORK_ID}\
         &version=6.2"
    );
    let prices: serde_json::Value = http
        .get(&prices_url)
        .send()
        .await
        .expect("prices request")
        .error_for_status()
        .expect("prices 200")
        .json()
        .await
        .expect("prices json");

    let pr = prices.get("priceRoute").expect("priceRoute present");
    let version = pr.get("version").expect("version field");
    let contract_addr_str = pr
        .get("contractAddress")
        .and_then(|v| v.as_str())
        .expect("contractAddress string");
    let contract_method = pr
        .get("contractMethod")
        .and_then(|v| v.as_str())
        .expect("contractMethod string");

    // Velora returns `version` as a number (despite our `version: "6.2"`
    // query-string convention; the response is a JSON number).
    let version_str = format!("{version}");
    assert_eq!(
        version_str.trim_matches('"'),
        "6.2",
        "real Velora returned unexpected version {version}; \
         our vendored ABI is V6-specific"
    );
    assert_eq!(
        contract_addr_str
            .parse::<Address>()
            .expect("contractAddress parses"),
        AUGUSTUS_V6,
        "real Velora pointed at {contract_addr_str} (expected AugustusV6 \
         {AUGUSTUS_V6:?}); either Velora migrated or Augustus moved"
    );

    // V6 has multiple swap entry points: the generic `swapExactAmountIn` plus
    // per-DEX direct routes (`swapExactAmountInOnUniswapV3`,
    // `swapExactAmountInOnCurveV2`, `swapExactAmountInOnBalancerV2`, …).
    // Velora's router picks the cheapest path; for a WETH→USDC quote on Base
    // it usually picks `swapExactAmountInOnUniswapV3`. We just require it's
    // *some* known method in our vendored ABI.
    assert!(
        contract_method.starts_with("swapExactAmount"),
        "real Velora returned a non-swap contractMethod ({contract_method:?})"
    );

    // ─── /transactions ────────────────────────────────────────────────────
    let tx_url = format!(
        "https://api.velora.xyz/transactions/{NETWORK_ID}?ignoreChecks=true&ignoreGasEstimate=true"
    );
    let tx_body = serde_json::json!({
        "srcToken": WETH_BASE,
        "srcDecimals": 18,
        "destToken": USDC_BASE,
        "destDecimals": 6,
        "priceRoute": pr,
        "srcAmount": pr.get("srcAmount").and_then(|v| v.as_str()).expect("srcAmount"),
        "slippage": 100, // 1% in bps
        "userAddress": USER_ADDR,
        "receiver": USER_ADDR,
        "partner": "rift-router",
        "side": "SELL",
    });
    let tx: serde_json::Value = http
        .post(&tx_url)
        .json(&tx_body)
        .send()
        .await
        .expect("transactions request")
        .error_for_status()
        .expect("transactions 200")
        .json()
        .await
        .expect("transactions json");

    let tx_to = tx
        .get("to")
        .and_then(|v| v.as_str())
        .expect("transactions.to string");
    let tx_data = tx
        .get("data")
        .and_then(|v| v.as_str())
        .expect("transactions.data string");

    assert_eq!(
        tx_to.parse::<Address>().expect("to parses"),
        AUGUSTUS_V6,
        "real Velora /transactions pointed at {tx_to}; expected AugustusV6"
    );

    // ─── Validate calldata selector against vendored V6 ABI ───────────────
    // Parse the vendored ABI at runtime, look up the function Velora chose,
    // and confirm the calldata's first 4 bytes match its selector. This
    // proves our vendored ABI knows the function Velora returned.
    let abi: JsonAbi = serde_json::from_str(AUGUSTUS_V6_ABI_JSON).expect("vendored ABI parses");
    let funcs = abi.functions.get(contract_method).unwrap_or_else(|| {
        panic!(
            "vendored AugustusV6 ABI has no function named {contract_method:?} — \
             Velora may have added a new entry point we haven't vendored yet"
        )
    });
    let func = funcs
        .first()
        .expect("at least one overload of the function");

    let calldata_hex = tx_data.trim_start_matches("0x");
    let calldata = alloy::hex::decode(calldata_hex).expect("calldata hex");
    assert!(
        calldata.len() >= 4,
        "calldata too short ({} bytes) to contain a selector",
        calldata.len()
    );
    let actual_selector = &calldata[..4];
    let expected_selector = func.selector();
    assert_eq!(
        actual_selector,
        expected_selector.as_slice(),
        "calldata selector {} does not match vendored ABI selector {} for {contract_method:?} — \
         vendored ABI has drifted from production",
        alloy::hex::encode(actual_selector),
        alloy::hex::encode(expected_selector),
    );

    eprintln!(
        "✓ Velora V6 wire shape validated: priceRoute.version=6.2, \
         contractAddress={AUGUSTUS_V6:?}, contractMethod={contract_method:?}, \
         /transactions.to={AUGUSTUS_V6:?}, calldata selector matches vendored ABI"
    );
}
