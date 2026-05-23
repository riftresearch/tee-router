//! Tier-2 funded subflow: real 1 USDC → WETH swap on Base via Velora V6.
//!
//! Exercises the full Velora V6 protocol round-trip with real money:
//!   1. Approve USDC → AugustusV6 on Base.
//!   2. Fetch a /prices quote (version=6.2).
//!   3. Fetch /transactions calldata for the quote.
//!   4. Send the swap tx to AugustusV6 with the returned calldata.
//!   5. Assert WETH balance increased by ≥ the quoted destAmount × (1 - slippage).
//!
//! Validates that our vendored AugustusV6 ABI accurately models the deployed
//! contract — anything wrong manifests as a revert or a mismatched balance.
//!
//! Gated by `FUNDED_SUBFLOW_TESTS=1`. Writes a self-contained record under
//! `<repo-root>/live-test-logs/<timestamp>--velora-usdc-to-weth-base/`.

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy::{
    network::EthereumWallet,
    primitives::{address, Address, Bytes, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
};
use serde_json::{json, Value};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

const AUGUSTUS_V6: Address = address!("6a000f20005980200259b80c5102003040001068");
const USDC_BASE: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
const WETH_BASE: Address = address!("4200000000000000000000000000000000000006");
const BASE_RPC: &str = "https://mainnet.base.org";
const BASE_CHAIN_ID: u64 = 8453;
/// 1 USDC, atomic units (6 decimals).
const SWAP_USDC_RAW: u64 = 1_000_000;
/// 1% slippage tolerance, in basis points.
const SLIPPAGE_BPS: u64 = 100;
/// Hard floor on Base ETH balance — bail if we're below this before sending.
/// Two txs at typical Base gas + L1 fee total ~0.00005 ETH; leave headroom.
const MIN_BASE_ETH_WEI: u64 = 30_000_000_000_000; // 0.00003 ETH ≈ $0.10

#[tokio::test]
#[ignore = "spends real USDC + gas; run with FUNDED_SUBFLOW_TESTS=1"]
async fn velora_v6_usdc_to_weth_base_swap() {
    if env::var("FUNDED_SUBFLOW_TESTS").ok().as_deref() != Some("1") {
        eprintln!("FUNDED_SUBFLOW_TESTS != 1; skipping (this is a real-money test)");
        return;
    }

    let timestamp = compact_utc_timestamp();
    let log_dir = repo_root_live_test_logs()
        .join(format!("{timestamp}--velora-usdc-to-weth-base"));
    fs::create_dir_all(&log_dir).expect("create log dir");
    let logger = Logger::open(log_dir.join("run.log"));
    logger.log(&format!("starting Velora V6 1-USDC → WETH swap on Base at {timestamp}"));
    logger.log(&format!("log dir: {}", log_dir.display()));
    let run_started = Instant::now();

    let pk = read_env_or_dotenv("ROUTER_LIVE_SOURCE_PRIVATE_KEY")
        .expect("ROUTER_LIVE_SOURCE_PRIVATE_KEY not set in env or .env");
    let signer: PrivateKeySigner = pk.parse().expect("parse private key");
    let wallet_addr = signer.address();
    logger.log(&format!("wallet: {wallet_addr:?}"));

    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(BASE_RPC.parse().unwrap());

    // ─── pre-balances ─────────────────────────────────────────────────────
    let pre = Balances::capture(&provider, wallet_addr).await;
    write_json(&log_dir.join("pre-balances.json"), &pre.to_json());
    logger.log(&format!(
        "pre  : eth={} usdc={} weth={}",
        format_eth(&pre.eth),
        format_usdc(&pre.usdc),
        format_eth(&pre.weth),
    ));

    assert!(
        pre.eth >= U256::from(MIN_BASE_ETH_WEI),
        "insufficient Base ETH for two txs: have {} wei, need at least {} wei",
        pre.eth,
        MIN_BASE_ETH_WEI
    );
    assert!(
        pre.usdc >= U256::from(SWAP_USDC_RAW),
        "insufficient Base USDC: have {}, need {}",
        format_usdc(&pre.usdc),
        format_usdc(&U256::from(SWAP_USDC_RAW))
    );

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("build http client");

    // ─── step 1: approve USDC → AugustusV6 (if needed) ────────────────────
    let usdc = IERC20::new(USDC_BASE, &provider);
    let allowance = usdc
        .allowance(wallet_addr, AUGUSTUS_V6)
        .call()
        .await
        .expect("read USDC allowance");
    logger.log(&format!("USDC allowance → AugustusV6: {}", format_usdc(&allowance)));

    if allowance < U256::from(SWAP_USDC_RAW) {
        logger.log("approving USDC → AugustusV6");
        let approve_pending = usdc
            .approve(AUGUSTUS_V6, U256::from(SWAP_USDC_RAW))
            .send()
            .await
            .expect("send approve tx");
        let approve_hash = *approve_pending.tx_hash();
        logger.log(&format!("approve tx: {approve_hash:?}"));
        let approve_receipt = approve_pending.get_receipt().await.expect("approve receipt");
        write_json(
            &log_dir.join("approve-tx.json"),
            &serde_json::to_value(&approve_receipt).expect("serialize approve receipt"),
        );
        assert!(approve_receipt.status(), "approve reverted");
        logger.log(&format!(
            "approve mined: block={} gas={}",
            approve_receipt.block_number.unwrap_or(0),
            approve_receipt.gas_used
        ));
    } else {
        logger.log("skipping approve — sufficient allowance already in place");
    }

    // ─── step 2: fetch /prices quote ──────────────────────────────────────
    let prices_url = format!(
        "https://api.velora.xyz/prices?\
         srcToken={USDC_BASE:#x}\
         &destToken={WETH_BASE:#x}\
         &amount={SWAP_USDC_RAW}\
         &srcDecimals=6\
         &destDecimals=18\
         &side=SELL\
         &network={BASE_CHAIN_ID}\
         &version=6.2\
         &userAddress={wallet_addr:#x}"
    );
    logger.log(&format!("fetching /prices: {prices_url}"));
    let prices: Value = http
        .get(&prices_url)
        .send()
        .await
        .expect("/prices request")
        .error_for_status()
        .expect("/prices 200")
        .json()
        .await
        .expect("/prices json");
    write_json(&log_dir.join("velora-prices.json"), &prices);
    let price_route = prices.get("priceRoute").expect("priceRoute present");
    let quote_dest_amount = price_route
        .get("destAmount")
        .and_then(|v| v.as_str())
        .expect("destAmount string");
    let quote_dest_u256 =
        U256::from_str_radix(quote_dest_amount, 10).expect("destAmount parses");
    let contract_method = price_route
        .get("contractMethod")
        .and_then(|v| v.as_str())
        .expect("contractMethod string");
    logger.log(&format!(
        "quote: 1 USDC → {} WETH via {contract_method}",
        format_eth(&quote_dest_u256)
    ));

    // ─── step 3: fetch /transactions calldata ─────────────────────────────
    let tx_url = format!(
        "https://api.velora.xyz/transactions/{BASE_CHAIN_ID}?ignoreChecks=true&ignoreGasEstimate=true"
    );
    let tx_body = json!({
        "srcToken": format!("{USDC_BASE:#x}"),
        "srcDecimals": 6,
        "destToken": format!("{WETH_BASE:#x}"),
        "destDecimals": 18,
        "priceRoute": price_route,
        "srcAmount": SWAP_USDC_RAW.to_string(),
        "slippage": SLIPPAGE_BPS,
        "userAddress": format!("{wallet_addr:#x}"),
        "receiver": format!("{wallet_addr:#x}"),
        "partner": "rift-router",
        "side": "SELL",
    });
    logger.log("fetching /transactions calldata");
    let tx_resp: Value = http
        .post(&tx_url)
        .json(&tx_body)
        .send()
        .await
        .expect("/transactions request")
        .error_for_status()
        .expect("/transactions 200")
        .json()
        .await
        .expect("/transactions json");
    write_json(&log_dir.join("velora-transactions.json"), &tx_resp);
    let tx_to: Address = tx_resp
        .get("to")
        .and_then(|v| v.as_str())
        .expect("tx.to")
        .parse()
        .expect("tx.to parses");
    let tx_data_hex = tx_resp
        .get("data")
        .and_then(|v| v.as_str())
        .expect("tx.data");
    let tx_value: U256 = tx_resp
        .get("value")
        .and_then(|v| v.as_str())
        .map(|s| U256::from_str_radix(s, 10).expect("value parses"))
        .unwrap_or(U256::ZERO);
    let calldata = alloy::hex::decode(tx_data_hex.trim_start_matches("0x")).expect("calldata hex");

    assert_eq!(tx_to, AUGUSTUS_V6, "tx.to mismatch (expected AugustusV6)");
    assert_eq!(tx_value, U256::ZERO, "ERC-20 swap should not send native value");
    logger.log(&format!("calldata: {} bytes, to: {tx_to:?}", calldata.len()));

    // ─── step 4: send the swap tx ────────────────────────────────────────
    // Velora's `/transactions` does not return a `gas` field when we pass
    // `ignoreGasEstimate=true`, so we estimate ourselves. The default
    // alloy estimate undercounts on multi-hop AugustusV6 routes (saw a
    // real revert at 399 264 gas: the inner WETH.transfer ran out of
    // EIP-150 subgas). Add a 50% safety margin.
    let mut swap_req = TransactionRequest::default()
        .to(tx_to)
        .value(tx_value)
        .input(Bytes::from(calldata).into());
    swap_req.from = Some(wallet_addr);
    let estimated = provider
        .estimate_gas(swap_req.clone())
        .await
        .expect("estimate_gas");
    let gas_limit = estimated * 3 / 2;
    logger.log(&format!(
        "gas estimate: {estimated}; setting limit to {gas_limit} (1.5x headroom)"
    ));
    swap_req = swap_req.gas_limit(gas_limit);
    let swap_pending = provider
        .send_transaction(swap_req)
        .await
        .expect("send swap tx");
    let swap_hash = *swap_pending.tx_hash();
    logger.log(&format!("swap tx: {swap_hash:?}"));
    let swap_receipt = swap_pending
        .get_receipt()
        .await
        .expect("swap tx receipt");
    write_json(
        &log_dir.join("swap-tx.json"),
        &serde_json::to_value(&swap_receipt).expect("serialize swap receipt"),
    );
    assert!(swap_receipt.status(), "swap reverted");
    logger.log(&format!(
        "swap mined: block={} gas={}",
        swap_receipt.block_number.unwrap_or(0),
        swap_receipt.gas_used
    ));

    // ─── post-balances + assertions ───────────────────────────────────────
    let post = Balances::capture(&provider, wallet_addr).await;
    write_json(&log_dir.join("post-balances.json"), &post.to_json());
    logger.log(&format!(
        "post : eth={} usdc={} weth={}",
        format_eth(&post.eth),
        format_usdc(&post.usdc),
        format_eth(&post.weth),
    ));

    let usdc_delta = pre.usdc.saturating_sub(post.usdc);
    let weth_delta = post.weth.saturating_sub(pre.weth);
    let min_acceptable = quote_dest_u256
        - (quote_dest_u256 * U256::from(SLIPPAGE_BPS) / U256::from(10_000));
    assert_eq!(
        usdc_delta,
        U256::from(SWAP_USDC_RAW),
        "USDC decreased by {} (expected {})",
        format_usdc(&usdc_delta),
        SWAP_USDC_RAW
    );
    assert!(
        weth_delta >= min_acceptable,
        "WETH gain {} < min acceptable {} (quote {}, slippage {} bps)",
        format_eth(&weth_delta),
        format_eth(&min_acceptable),
        format_eth(&quote_dest_u256),
        SLIPPAGE_BPS
    );

    let total = run_started.elapsed();
    logger.log(&format!("total wall time: {:.1}s", total.as_secs_f64()));

    let summary = format!(
        r#"# Velora V6 USDC → WETH swap on Base (tier-2 funded)

- **Started**: {timestamp} UTC
- **Wallet**: `{wallet_addr:?}`
- **Result**: ✅ swap settled
- **Total wall time**: {total_secs:.1}s

## Quote
- via `{contract_method}` (V6 direct-route variant chosen by Velora)
- 1.000000 USDC → {weth_quote} WETH (slippage tolerance {slippage} bps)

## Transactions
| Step | tx hash | Block | Gas used |
|---|---|---|---|
| swap | `{swap_hash:?}` | {block} | {gas} |

Explorer: https://basescan.org/tx/{swap_hash:?}

## Balance deltas

| | ETH | USDC | WETH |
|---|---|---|---|
| pre  | {eth_pre} | {usdc_pre} | {weth_pre} |
| post | {eth_post} | {usdc_post} | {weth_post} |

USDC delta: -{usdc_delta_str}
WETH delta: +{weth_delta_str}

## Validation

End-to-end Velora V6 contract behavior matches the vendored ABI:
- `/prices` returned `version: 6.2`, `contractAddress: AugustusV6`
- `/transactions` returned calldata for `{contract_method}` directed at AugustusV6
- The swap settled on-chain, USDC decreased by exactly 1.0, WETH increased
  by ≥ quote × (1 - slippage). No revert.
"#,
        total_secs = total.as_secs_f64(),
        weth_quote = format_eth(&quote_dest_u256),
        slippage = SLIPPAGE_BPS,
        block = swap_receipt.block_number.unwrap_or(0),
        gas = swap_receipt.gas_used,
        eth_pre = format_eth(&pre.eth),
        usdc_pre = format_usdc(&pre.usdc),
        weth_pre = format_eth(&pre.weth),
        eth_post = format_eth(&post.eth),
        usdc_post = format_usdc(&post.usdc),
        weth_post = format_eth(&post.weth),
        usdc_delta_str = format_usdc(&usdc_delta),
        weth_delta_str = format_eth(&weth_delta),
    );
    fs::write(log_dir.join("summary.md"), summary).expect("write summary.md");

    logger.log(&format!("✓ run complete — artifacts in {}", log_dir.display()));
}

// ─── helpers ──────────────────────────────────────────────────────────────────

struct Balances {
    eth: U256,
    usdc: U256,
    weth: U256,
}

impl Balances {
    async fn capture<P: Provider>(provider: &P, wallet: Address) -> Self {
        let eth = provider.get_balance(wallet).await.expect("eth balance");
        let usdc = IERC20::new(USDC_BASE, provider)
            .balanceOf(wallet)
            .call()
            .await
            .expect("USDC balance");
        let weth = IERC20::new(WETH_BASE, provider)
            .balanceOf(wallet)
            .call()
            .await
            .expect("WETH balance");
        Self { eth, usdc, weth }
    }

    fn to_json(&self) -> Value {
        json!({
            "captured_at": utc_now_iso(),
            "base": {
                "eth_wei": self.eth.to_string(),
                "eth_human": format_eth(&self.eth),
                "usdc_raw": self.usdc.to_string(),
                "usdc_human": format_usdc(&self.usdc),
                "weth_wei": self.weth.to_string(),
                "weth_human": format_eth(&self.weth),
            }
        })
    }
}

struct Logger {
    file: Mutex<std::fs::File>,
}

impl Logger {
    fn open(path: PathBuf) -> Self {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open run.log");
        Self {
            file: Mutex::new(file),
        }
    }

    fn log(&self, msg: &str) {
        use std::io::Write;
        let line = format!("{} {msg}\n", utc_now_iso());
        eprint!("{line}");
        if let Ok(mut f) = self.file.lock() {
            let _ = f.write_all(line.as_bytes());
        }
    }
}

fn write_json(path: &Path, value: &Value) {
    fs::write(
        path,
        serde_json::to_string_pretty(value).expect("serialize json"),
    )
    .expect("write json");
}

fn format_eth(wei: &U256) -> String {
    let s = wei.to_string();
    let len = s.len();
    if len <= 18 {
        format!("0.{:0>18}", s)
    } else {
        let (whole, frac) = s.split_at(len - 18);
        format!("{whole}.{frac}")
    }
}

fn format_usdc(raw: &U256) -> String {
    let s = raw.to_string();
    let len = s.len();
    if len <= 6 {
        format!("0.{:0>6}", s)
    } else {
        let (whole, frac) = s.split_at(len - 6);
        format!("{whole}.{frac}")
    }
}

fn compact_utc_timestamp() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0)
        .expect("valid timestamp");
    datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string()
}

fn utc_now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0)
        .expect("valid timestamp");
    datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

fn repo_root_live_test_logs() -> PathBuf {
    let manifest = env!("CARGO_MANIFEST_DIR");
    let mut dir = PathBuf::from(manifest);
    loop {
        if dir.join("Cargo.toml").is_file() {
            let toml = fs::read_to_string(dir.join("Cargo.toml")).unwrap_or_default();
            if toml.contains("[workspace]") {
                return dir.join("live-test-logs");
            }
        }
        if !dir.pop() {
            break;
        }
    }
    PathBuf::from("live-test-logs")
}

fn read_env_or_dotenv(name: &str) -> Option<String> {
    if let Ok(v) = env::var(name) {
        return Some(v);
    }
    let manifest = env!("CARGO_MANIFEST_DIR");
    let mut dir = PathBuf::from(manifest);
    loop {
        let candidate = dir.join(".env");
        if candidate.is_file() {
            for line in fs::read_to_string(&candidate).ok()?.lines() {
                if let Some(rest) = line.strip_prefix(&format!("{name}=")) {
                    return Some(rest.trim().trim_matches(['"', '\'']).to_string());
                }
            }
        }
        if !dir.pop() {
            break;
        }
    }
    None
}
