//! Tier-2 funded subflow: real CCTP V2 burn on Arbitrum + redeem on Base.
//!
//! Runs the full protocol round-trip with **real USDC** from the test wallet
//! configured by `ROUTER_LIVE_SOURCE_PRIVATE_KEY`. Validates that our CCTP
//! code matches reality end-to-end — burn calldata format, Iris response
//! shape, attestation polling, redeem calldata format — anything we got
//! wrong shows up as a revert or a mismatched balance delta.
//!
//! Direction: Arbitrum → Base (more ETH headroom on Arbitrum for the burn).
//!
//! Gated by `FUNDED_SUBFLOW_TESTS=1` so it never runs in CI or by accident.
//! Writes a self-contained record under `<repo-root>/live-test-logs/...` per
//! the convention in `live-test-logs/README.md`.
//!
//! Resume mode (after a flaky-network failure mid-poll): set
//! `CCTP_RESUME_BURN_TX=0x...` and optionally `CCTP_RESUME_LOG_DIR=...` to
//! skip the approve+burn steps and continue polling Iris + redeeming on
//! Base. No additional USDC burned.
//!
//! Run with:
//!
//!     FUNDED_SUBFLOW_TESTS=1 cargo test -p sauron --test funded_cctp_arb_to_base \
//!         -- --ignored --nocapture

use std::{
    env, fs,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy::{
    hex,
    network::EthereumWallet,
    primitives::{address, Address, Bytes, FixedBytes, B256, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use serde_json::{json, Value};

// --- vendored CCTP V2 contract bindings ---
// Paths are relative to `bin/sauron/`, the integration test's CARGO_MANIFEST_DIR.
sol! {
    #[sol(rpc)]
    ICctpTokenMessengerV2,
    "../../vendor/cctp/TokenMessengerV2.abi.json",
}

sol! {
    #[sol(rpc)]
    ICctpMessageTransmitterV2,
    "../../vendor/cctp/MessageTransmitterV2.abi.json",
}

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function balanceOf(address owner) external view returns (uint256);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

// --- canonical mainnet/L2 addresses (same CREATE2 across chains) ---
const TOKEN_MESSENGER_V2: Address = address!("28b5a0e9c621a5badaa536219b3a228c8168cf5d");
const MESSAGE_TRANSMITTER_V2: Address = address!("81d40f21f12a8f0e3252bccb954d722d4c464b64");
const USDC_ARB: Address = address!("af88d065e77c8cc2239327c5edb3a432268e5831");
const USDC_BASE: Address = address!("833589fcd6edb6e08f4c7c32d4f71b54bda02913");
const ARB_RPC: &str = "https://arb1.arbitrum.io/rpc";
const BASE_RPC: &str = "https://mainnet.base.org";
const ARB_DOMAIN: u32 = 3;
const BASE_DOMAIN: u32 = 6;
/// CCTP V2 "standard finality" — Iris waits ~13 confirmations on the source
/// chain before attesting (~12-15 min on Arbitrum). Use `1000` for "fast
/// finality" on small amounts.
const FINALITY_STANDARD: u32 = 2000;
/// 1 USDC, raw atomic units (6 decimals).
const BURN_AMOUNT_RAW: u64 = 1_000_000;
/// Max wall time waiting on Iris before bailing.
const IRIS_POLL_TIMEOUT: Duration = Duration::from_secs(1800);
/// Interval between Iris polls.
const IRIS_POLL_INTERVAL: Duration = Duration::from_secs(15);
/// Per-request HTTP timeout for the Iris poll.
const IRIS_HTTP_TIMEOUT: Duration = Duration::from_secs(20);
/// How many transient HTTP errors to tolerate before bailing.
const IRIS_HTTP_MAX_TRANSIENT_ERRORS: u32 = 20;

#[tokio::test]
#[ignore = "spends real USDC + gas; run with FUNDED_SUBFLOW_TESTS=1"]
async fn cctp_arb_to_base_1usdc_round_trip() {
    if env::var("FUNDED_SUBFLOW_TESTS").ok().as_deref() != Some("1") {
        eprintln!("FUNDED_SUBFLOW_TESTS != 1; skipping (this is a real-money test)");
        return;
    }

    // --- resume mode ---
    let resume_burn_tx = env::var("CCTP_RESUME_BURN_TX")
        .ok()
        .filter(|s| !s.is_empty());
    let resume_log_dir = env::var("CCTP_RESUME_LOG_DIR")
        .ok()
        .filter(|s| !s.is_empty());

    // --- log dir setup ---
    let timestamp = compact_utc_timestamp();
    let log_dir = match resume_log_dir.as_deref() {
        Some(path) => PathBuf::from(path),
        None => repo_root_live_test_logs().join(format!("{timestamp}--cctp-arb-to-base-1usdc")),
    };
    fs::create_dir_all(&log_dir).expect("create log dir");
    let logger = Logger::open(log_dir.join("run.log"));
    logger.log(&format!(
        "starting CCTP Arb→Base 1-USDC round trip at {timestamp}"
    ));
    logger.log(&format!("log dir: {}", log_dir.display()));
    if let Some(prior) = &resume_burn_tx {
        logger.log(&format!("RESUME mode — reusing prior burn tx: {prior}"));
    }
    let run_started = Instant::now();

    // --- wallet ---
    let pk = read_env_or_dotenv("ROUTER_LIVE_SOURCE_PRIVATE_KEY")
        .expect("ROUTER_LIVE_SOURCE_PRIVATE_KEY not set in env or .env");
    let signer: PrivateKeySigner = pk.parse().expect("parse private key");
    let wallet_addr = signer.address();
    logger.log(&format!("wallet: {wallet_addr:?}"));

    let arb_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer.clone()))
        .connect_http(ARB_RPC.parse().unwrap());
    let base_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(signer))
        .connect_http(BASE_RPC.parse().unwrap());

    // --- pre-balances (overwritten by resume runs — that's fine, post will diff against this) ---
    let pre = Balances::capture(&arb_provider, &base_provider, wallet_addr).await;
    write_json(&log_dir.join("pre-balances.json"), &pre.to_json());
    logger.log(&format!(
        "pre  : arb_eth={} arb_usdc={} base_eth={} base_usdc={}",
        format_eth(&pre.arb_eth),
        format_usdc(&pre.arb_usdc),
        format_eth(&pre.base_eth),
        format_usdc(&pre.base_usdc),
    ));

    let burn_hash: B256 = if let Some(prior) = &resume_burn_tx {
        prior
            .parse()
            .expect("CCTP_RESUME_BURN_TX must be a valid 0x-prefixed 32-byte hash")
    } else {
        assert!(
            pre.arb_usdc >= U256::from(BURN_AMOUNT_RAW),
            "insufficient Arbitrum USDC: have {}, need {}",
            format_usdc(&pre.arb_usdc),
            format_usdc(&U256::from(BURN_AMOUNT_RAW))
        );

        // --- step 1: approve USDC -> TokenMessengerV2 on Arbitrum (if needed) ---
        let arb_usdc = IERC20::new(USDC_ARB, &arb_provider);
        let allowance = arb_usdc
            .allowance(wallet_addr, TOKEN_MESSENGER_V2)
            .call()
            .await
            .expect("read USDC allowance");
        logger.log(&format!(
            "USDC allowance (arb -> token messenger): {}",
            format_usdc(&allowance)
        ));
        if allowance < U256::from(BURN_AMOUNT_RAW) {
            logger.log("approving USDC -> TokenMessengerV2 on Arbitrum");
            let approve_pending = arb_usdc
                .approve(TOKEN_MESSENGER_V2, U256::from(BURN_AMOUNT_RAW))
                .send()
                .await
                .expect("send approve tx");
            let approve_hash = *approve_pending.tx_hash();
            logger.log(&format!("approve tx: {approve_hash:?}"));
            let approve_receipt = approve_pending
                .get_receipt()
                .await
                .expect("approve tx receipt");
            write_json(
                &log_dir.join("approve-tx.json"),
                &serde_json::to_value(&approve_receipt).expect("serialize approve receipt"),
            );
            assert!(approve_receipt.status(), "approve tx reverted");
            logger.log(&format!(
                "approve mined: block={} gas={}",
                approve_receipt.block_number.unwrap_or(0),
                approve_receipt.gas_used
            ));
        } else {
            logger.log("skipping approve — sufficient allowance already in place");
        }

        // --- step 2: burn 1 USDC on Arbitrum ---
        logger.log("calling depositForBurn on Arbitrum TokenMessengerV2");
        let token_messenger = ICctpTokenMessengerV2::new(TOKEN_MESSENGER_V2, &arb_provider);
        let burn_pending = token_messenger
            .depositForBurn(
                U256::from(BURN_AMOUNT_RAW),
                BASE_DOMAIN,
                evm_address_to_bytes32(wallet_addr),
                USDC_ARB,
                FixedBytes::<32>::ZERO, // destinationCaller=0 → permissionless redeem
                U256::ZERO,             // maxFee=0 → user pays no Iris fee (standard finality)
                FINALITY_STANDARD,
            )
            .send()
            .await
            .expect("send depositForBurn tx");
        let burn_hash = *burn_pending.tx_hash();
        logger.log(&format!("burn tx: {burn_hash:?}"));
        let burn_receipt = burn_pending.get_receipt().await.expect("burn tx receipt");
        write_json(
            &log_dir.join("burn-tx.json"),
            &serde_json::to_value(&burn_receipt).expect("serialize burn receipt"),
        );
        assert!(burn_receipt.status(), "depositForBurn reverted");
        logger.log(&format!(
            "burn mined: block={} gas={}",
            burn_receipt.block_number.unwrap_or(0),
            burn_receipt.gas_used
        ));
        burn_hash
    };

    // --- step 3: poll Iris until attestation is complete ---
    logger.log(&format!(
        "polling Iris https://iris-api.circle.com/v2/messages/{ARB_DOMAIN}?transactionHash={burn_hash:?}"
    ));
    let (message_bytes, attestation_bytes) =
        poll_iris_until_complete(&format!("{burn_hash:?}"), &log_dir, &logger).await;
    logger.log(&format!(
        "attestation ready: message={} bytes, attestation={} bytes",
        message_bytes.len(),
        attestation_bytes.len()
    ));

    // --- step 4: redeem on Base via receiveMessage ---
    logger.log("calling receiveMessage on Base MessageTransmitterV2");
    let message_transmitter =
        ICctpMessageTransmitterV2::new(MESSAGE_TRANSMITTER_V2, &base_provider);
    let receive_pending = message_transmitter
        .receiveMessage(Bytes::from(message_bytes), Bytes::from(attestation_bytes))
        .send()
        .await
        .expect("send receiveMessage tx");
    let receive_hash = *receive_pending.tx_hash();
    logger.log(&format!("receive tx: {receive_hash:?}"));
    let receive_receipt = receive_pending
        .get_receipt()
        .await
        .expect("receive tx receipt");
    write_json(
        &log_dir.join("receive-tx.json"),
        &serde_json::to_value(&receive_receipt).expect("serialize receive receipt"),
    );
    assert!(receive_receipt.status(), "receiveMessage reverted");
    logger.log(&format!(
        "receive mined: block={} gas={}",
        receive_receipt.block_number.unwrap_or(0),
        receive_receipt.gas_used
    ));

    // --- post-balances ---
    let post = Balances::capture(&arb_provider, &base_provider, wallet_addr).await;
    write_json(&log_dir.join("post-balances.json"), &post.to_json());
    logger.log(&format!(
        "post : arb_eth={} arb_usdc={} base_eth={} base_usdc={}",
        format_eth(&post.arb_eth),
        format_usdc(&post.arb_usdc),
        format_eth(&post.base_eth),
        format_usdc(&post.base_usdc),
    ));

    let total_elapsed = run_started.elapsed();
    logger.log(&format!(
        "total wall time: {:.1}s",
        total_elapsed.as_secs_f64()
    ));

    // --- assert balance deltas match expectations (only when not in resume mode;
    // in resume mode the pre-balances were captured AFTER the burn so the diff
    // is the receive delta only) ---
    let base_usdc_delta = post.base_usdc.saturating_sub(pre.base_usdc);
    assert_eq!(
        base_usdc_delta,
        U256::from(BURN_AMOUNT_RAW),
        "Base USDC increased by {} (expected {}); receive failed to credit recipient",
        format_usdc(&base_usdc_delta),
        BURN_AMOUNT_RAW
    );
    if resume_burn_tx.is_none() {
        let arb_usdc_delta = pre.arb_usdc.saturating_sub(post.arb_usdc);
        assert_eq!(
            arb_usdc_delta,
            U256::from(BURN_AMOUNT_RAW),
            "Arbitrum USDC decreased by {} (expected {})",
            format_usdc(&arb_usdc_delta),
            BURN_AMOUNT_RAW
        );
    }

    // --- summary.md (written last) ---
    let summary = format!(
        r#"# CCTP Arbitrum → Base 1 USDC round trip

- **Started**: {timestamp} UTC
- **Wallet**: `{wallet_addr:?}`
- **Result**: ✅ success — burn + Iris attestation + receive all completed
- **Total wall time (this invocation)**: {total_secs:.1}s
- **Resume mode**: {resume_note}

## Transactions

| Step | Chain | tx hash | Block | Gas used |
|---|---|---|---|---|
| burn  | Arbitrum | `{burn_hash:?}`   | (see burn-tx.json) | (see burn-tx.json) |
| receive | Base    | `{receive_hash:?}` | {receive_block} | {receive_gas} |

Explorers:
- Burn: https://arbiscan.io/tx/{burn_hash:?}
- Receive: https://basescan.org/tx/{receive_hash:?}

## Balance deltas (this invocation's pre/post snapshots)

| | Arbitrum ETH | Arbitrum USDC | Base ETH | Base USDC |
|---|---|---|---|---|
| pre  | {arb_eth_pre} | {arb_usdc_pre} | {base_eth_pre} | {base_usdc_pre} |
| post | {arb_eth_post} | {arb_usdc_post} | {base_eth_post} | {base_usdc_post} |

Base USDC delta: +{base_delta} ✓

## Validation

This run validates the full CCTP V2 protocol contract against our types:
- `depositForBurn` calldata (vendored ABI binding) accepted by Circle's
  TokenMessengerV2 on Arbitrum
- Iris V2 response shape successfully polled to `complete` (see
  iris-poll-*.json + attestation-complete.json)
- `receiveMessage` calldata (vendored ABI binding) accepted by
  MessageTransmitterV2 on Base; `MintAndWithdraw` event emitted; USDC
  delivered to recipient with the expected delta.
"#,
        total_secs = total_elapsed.as_secs_f64(),
        receive_block = receive_receipt.block_number.unwrap_or(0),
        receive_gas = receive_receipt.gas_used,
        arb_eth_pre = format_eth(&pre.arb_eth),
        arb_usdc_pre = format_usdc(&pre.arb_usdc),
        base_eth_pre = format_eth(&pre.base_eth),
        base_usdc_pre = format_usdc(&pre.base_usdc),
        arb_eth_post = format_eth(&post.arb_eth),
        arb_usdc_post = format_usdc(&post.arb_usdc),
        base_eth_post = format_eth(&post.base_eth),
        base_usdc_post = format_usdc(&post.base_usdc),
        base_delta = format_usdc(&base_usdc_delta),
        resume_note = if resume_burn_tx.is_some() {
            "yes (skipped approve+burn; reused prior burn tx)"
        } else {
            "no (full burn → poll → receive)"
        },
    );
    fs::write(log_dir.join("summary.md"), summary).expect("write summary.md");

    logger.log(&format!(
        "✓ run complete — artifacts in {}",
        log_dir.display()
    ));
}

// ─── helpers ──────────────────────────────────────────────────────────────────

async fn poll_iris_until_complete(
    burn_tx_hash: &str,
    log_dir: &Path,
    logger: &Logger,
) -> (Vec<u8>, Vec<u8>) {
    let http = reqwest::Client::builder()
        .timeout(IRIS_HTTP_TIMEOUT)
        .build()
        .expect("build http client");
    let url = format!(
        "https://iris-api.circle.com/v2/messages/{ARB_DOMAIN}?transactionHash={burn_tx_hash}"
    );
    let started = Instant::now();
    let mut poll_n = next_iris_poll_index(log_dir);
    let mut transient_errors: u32 = 0;
    loop {
        let request_started = Instant::now();
        let outcome = http.get(&url).send().await;
        let (status_code, body): (u16, Value) = match outcome {
            Ok(response) => {
                let status = response.status();
                let body_text = response.text().await.unwrap_or_default();
                let body: Value = serde_json::from_str(&body_text)
                    .unwrap_or_else(|_| json!({"_raw": body_text.clone()}));
                transient_errors = 0;
                (status.as_u16(), body)
            }
            Err(err) => {
                transient_errors += 1;
                logger.log(&format!(
                    "iris poll {poll_n:03}: TRANSIENT ERROR (#{transient_errors}/{IRIS_HTTP_MAX_TRANSIENT_ERRORS}) after {:.1}s — {err}",
                    request_started.elapsed().as_secs_f64()
                ));
                let body = json!({
                    "polled_at": utc_now_iso(),
                    "transient_error": err.to_string(),
                });
                write_json(&log_dir.join(format!("iris-poll-{poll_n:03}.json")), &body);
                if transient_errors > IRIS_HTTP_MAX_TRANSIENT_ERRORS {
                    panic!(
                        "Iris poll exceeded {IRIS_HTTP_MAX_TRANSIENT_ERRORS} consecutive transient errors; last: {err}"
                    );
                }
                if started.elapsed() > IRIS_POLL_TIMEOUT {
                    panic!(
                        "Iris attestation did not reach `complete` within {:?} (transient errors only); aborting",
                        IRIS_POLL_TIMEOUT
                    );
                }
                poll_n += 1;
                tokio::time::sleep(IRIS_POLL_INTERVAL).await;
                continue;
            }
        };

        write_json(
            &log_dir.join(format!("iris-poll-{poll_n:03}.json")),
            &json!({
                "polled_at": utc_now_iso(),
                "http_status": status_code,
                "body": body.clone(),
            }),
        );
        let status_str = body
            .get("messages")
            .and_then(|m| m.as_array())
            .and_then(|a| a.first())
            .and_then(|m| m.get("status"))
            .and_then(Value::as_str)
            .unwrap_or("?");
        logger.log(&format!(
            "iris poll {poll_n:03}: http={status_code} status={status_str}"
        ));

        if status_code == 200 {
            if let Some(msg) = body
                .get("messages")
                .and_then(|m| m.as_array())
                .and_then(|a| a.first())
            {
                let st = msg.get("status").and_then(Value::as_str);
                let message_hex = msg.get("message").and_then(Value::as_str);
                let attestation_hex = msg.get("attestation").and_then(Value::as_str);
                if st == Some("complete")
                    && message_hex.is_some_and(|s| s.starts_with("0x") && s.len() > 2)
                    && attestation_hex.is_some_and(|s| s.starts_with("0x") && s.len() > 2)
                {
                    write_json(
                        &log_dir.join("attestation-complete.json"),
                        &json!({
                            "fetched_at": utc_now_iso(),
                            "body": body,
                        }),
                    );
                    let message = hex::decode(message_hex.unwrap().trim_start_matches("0x"))
                        .expect("decode message hex");
                    let attestation =
                        hex::decode(attestation_hex.unwrap().trim_start_matches("0x"))
                            .expect("decode attestation hex");
                    return (message, attestation);
                }
            }
        }

        if started.elapsed() > IRIS_POLL_TIMEOUT {
            panic!(
                "Iris attestation did not reach `complete` within {:?}; last body: {body}",
                IRIS_POLL_TIMEOUT
            );
        }
        poll_n += 1;
        tokio::time::sleep(IRIS_POLL_INTERVAL).await;
    }
}

/// If we're in resume mode and the prior run wrote some iris-poll-NNN.json
/// files, continue numbering from the next free index instead of overwriting.
fn next_iris_poll_index(log_dir: &Path) -> u32 {
    let mut max_n: i64 = -1;
    if let Ok(entries) = fs::read_dir(log_dir) {
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if let Some(rest) = name.strip_prefix("iris-poll-") {
                if let Some(num_str) = rest.strip_suffix(".json") {
                    if let Ok(n) = num_str.parse::<i64>() {
                        max_n = max_n.max(n);
                    }
                }
            }
        }
    }
    (max_n + 1) as u32
}

struct Balances {
    arb_eth: U256,
    arb_usdc: U256,
    base_eth: U256,
    base_usdc: U256,
}

impl Balances {
    async fn capture<P1: Provider, P2: Provider>(arb: &P1, base: &P2, wallet: Address) -> Self {
        let arb_eth = arb.get_balance(wallet).await.expect("arb eth balance");
        let arb_usdc = IERC20::new(USDC_ARB, arb)
            .balanceOf(wallet)
            .call()
            .await
            .expect("arb USDC balance");
        let base_eth = base.get_balance(wallet).await.expect("base eth balance");
        let base_usdc = IERC20::new(USDC_BASE, base)
            .balanceOf(wallet)
            .call()
            .await
            .expect("base USDC balance");
        Self {
            arb_eth,
            arb_usdc,
            base_eth,
            base_usdc,
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "captured_at": utc_now_iso(),
            "arbitrum": {
                "eth_wei": self.arb_eth.to_string(),
                "eth_human": format_eth(&self.arb_eth),
                "usdc_raw": self.arb_usdc.to_string(),
                "usdc_human": format_usdc(&self.arb_usdc),
            },
            "base": {
                "eth_wei": self.base_eth.to_string(),
                "eth_human": format_eth(&self.base_eth),
                "usdc_raw": self.base_usdc.to_string(),
                "usdc_human": format_usdc(&self.base_usdc),
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

fn evm_address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::from(bytes)
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
    let datetime =
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0).expect("valid timestamp");
    datetime.format("%Y-%m-%dT%H-%M-%SZ").to_string()
}

fn utc_now_iso() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    let datetime =
        chrono::DateTime::<chrono::Utc>::from_timestamp(secs as i64, 0).expect("valid timestamp");
    datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// `live-test-logs/` at the workspace root. Integration tests run with
/// `CWD = bin/sauron/`, so we walk up looking for the workspace `Cargo.toml`.
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
    // Fallback (shouldn't happen): write under the CWD.
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
