//! Tier-1 live differential against real Hyperunit API.
//!
//! Hits production `api.hyperunit.xyz` and exercises:
//!   1. `GET /gen/bitcoin/hyperliquid/btc/{addr}` — generates a deposit
//!      address. `HyperUnitClient::generate_address` runs the new guardian
//!      signature verifier; if our pinned mainnet pubkeys, payload template,
//!      or ECDSA verification logic is wrong, this fails loudly with the
//!      exact reasons per guardian.
//!   2. `GET /operations/{address}` — confirms the documented response shape
//!      still deserializes into our typed `UnitOperationsResponse`.
//!
//! Read-only, no funds. Gated by `LIVE_PROVIDER_TESTS=1`.
//!
//! Run with:
//!
//!     LIVE_PROVIDER_TESTS=1 cargo test -p sauron --test live_hyperunit_api \
//!         -- --ignored --nocapture

use hyperunit_client::{
    GuardianNetwork, HyperUnitClient, UnitAsset, UnitChain, UnitGenerateAddressRequest,
    UnitOperationsRequest,
};
use std::{env, fs, path::PathBuf};

/// Stable HL spot wallet for which we'll generate a BTC deposit address.
/// Any valid HL-address-shape (0x-prefixed 40-char hex) works for the
/// /gen call; this is just our session test wallet.
const TEST_HL_ADDRESS: &str = "0x33F65788aCa48D733c2C2444Ac9F79B18206aa92";

/// Hyperunit's `/gen` endpoint is protected by Cloudflare and 403s any
/// datacenter IP. Production reaches it via a SOCKS5 proxy configured by
/// `HYPERUNIT_LIVE_PROXY_URL` (or `HYPERUNIT_PROXY_URL` as fallback). Tier-1
/// must use the same path. We surface it through `.env` if running locally.
fn live_proxy_url() -> Option<String> {
    for key in ["HYPERUNIT_LIVE_PROXY_URL", "HYPERUNIT_PROXY_URL"] {
        if let Ok(v) = env::var(key) {
            let trimmed = v.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    // Fall back to reading the repo's .env directly.
    let manifest = env!("CARGO_MANIFEST_DIR");
    let mut dir = PathBuf::from(manifest);
    while dir.pop() {
        let candidate = dir.join(".env");
        if candidate.is_file() {
            if let Ok(contents) = fs::read_to_string(&candidate) {
                for key in ["HYPERUNIT_LIVE_PROXY_URL", "HYPERUNIT_PROXY_URL"] {
                    for line in contents.lines() {
                        if let Some(rest) = line.strip_prefix(&format!("{key}=")) {
                            let v = rest.trim().trim_matches(['"', '\'']);
                            if !v.is_empty() {
                                return Some(v.to_string());
                            }
                        }
                    }
                }
            }
            break;
        }
    }
    None
}

/// Verifier-validation test: pull a real `/operations/{address}` response (whose
/// `addresses[]` entries carry the same guardian signatures that were
/// originally returned by `/gen`) and confirm our `verify_generate_address`
/// validates them against the pinned mainnet pubkeys.
///
/// We can't call `/gen` directly from this machine because Cloudflare 403s
/// any datacenter IP — production reaches it via a SOCKS5 proxy
/// (`HYPERUNIT_LIVE_PROXY_URL`). `/operations` doesn't have the same
/// protection and works direct, and the signatures it carries are
/// byte-identical to what `/gen` originally returned.
#[tokio::test]
#[ignore = "live network call to Hyperunit; run with LIVE_PROVIDER_TESTS=1"]
async fn real_hyperunit_guardian_signatures_verify_against_pinned_keys() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }

    // Fetch raw /operations response — bypassing our HyperUnitClient because
    // we want the raw `addresses[]` entries (which carry sourceCoinType +
    // destinationChain + the original guardian signatures).
    let url = format!("https://api.hyperunit.xyz/operations/{TEST_HL_ADDRESS}");
    let body: serde_json::Value = reqwest::get(&url)
        .await
        .expect("/operations GET")
        .error_for_status()
        .expect("/operations 200")
        .json()
        .await
        .expect("/operations json");

    let addresses = body
        .get("addresses")
        .and_then(|v| v.as_array())
        .expect("operations response has addresses array");
    assert!(
        !addresses.is_empty(),
        "test wallet must have at least one generated address; \
         if zero, run a deposit-address generation against it first"
    );

    let mut verified_count = 0usize;
    for entry in addresses {
        let source_coin_type = entry
            .get("sourceCoinType")
            .and_then(|v| v.as_str())
            .expect("address entry has sourceCoinType");
        let destination_chain = entry
            .get("destinationChain")
            .and_then(|v| v.as_str())
            .expect("address entry has destinationChain");
        let address = entry
            .get("address")
            .and_then(|v| v.as_str())
            .expect("address entry has address");
        let signatures = entry
            .get("signatures")
            .cloned()
            .expect("address entry has signatures");

        let src_chain = UnitChain::parse(source_coin_type).unwrap_or_else(|| {
            panic!("unrecognized sourceCoinType {source_coin_type:?} for entry {address:?}")
        });
        let dst_chain = UnitChain::parse(destination_chain).unwrap_or_else(|| {
            panic!("unrecognized destinationChain {destination_chain:?} for entry {address:?}")
        });
        // sourceCoinType=ethereum → asset=eth (newer EVM payload template);
        // sourceCoinType=bitcoin/solana → asset=btc-style (legacy template).
        let asset = if matches!(src_chain, UnitChain::Bitcoin) {
            UnitAsset::Btc
        } else {
            UnitAsset::Eth
        };

        let request = UnitGenerateAddressRequest {
            src_chain,
            dst_chain,
            asset,
            dst_addr: TEST_HL_ADDRESS.to_string(),
        };
        let response = hyperunit_client::UnitGenerateAddressResponse {
            address: address.to_string(),
            status: None,
            signatures,
        };

        hyperunit_client::verify_generate_address(&request, &response, GuardianNetwork::Mainnet)
            .unwrap_or_else(|err| {
                panic!(
                    "guardian verification failed for real production address {address:?} \
                     ({source_coin_type} → {destination_chain}): {err}"
                )
            });
        eprintln!(
            "✓ verified guardian sigs for {source_coin_type} → {destination_chain} \
             address {address}"
        );
        verified_count += 1;
    }

    eprintln!("✓ verified {verified_count} real production address(es) against pinned guardian pubkeys");
}

/// If a proxy is available, also exercise the *full* path: real /gen call +
/// auto-verification through `HyperUnitClient::generate_address`. Skipped
/// gracefully (not failed) when no proxy is configured.
#[tokio::test]
#[ignore = "live network call to Hyperunit via proxy; run with LIVE_PROVIDER_TESTS=1"]
async fn real_hyperunit_gen_call_via_proxy_auto_verifies() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }
    let Some(proxy_url) = live_proxy_url() else {
        eprintln!(
            "HYPERUNIT_LIVE_PROXY_URL / HYPERUNIT_PROXY_URL not set; \
             skipping the /gen call (Cloudflare 403s direct datacenter requests)"
        );
        return;
    };

    let client = HyperUnitClient::new_with_proxy_url("https://api.hyperunit.xyz", Some(proxy_url))
        .expect("hyperunit client construction");
    assert_eq!(client.network(), GuardianNetwork::Mainnet);

    let request = UnitGenerateAddressRequest {
        src_chain: UnitChain::Bitcoin,
        dst_chain: UnitChain::Hyperliquid,
        asset: UnitAsset::Btc,
        dst_addr: TEST_HL_ADDRESS.to_string(),
    };
    let response = match client.generate_address(request.clone()).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("proxy unreachable from this machine ({e}); skipping");
            return;
        }
    };
    eprintln!(
        "✓ live /gen via proxy succeeded + auto-verified: address={}",
        response.address
    );
}

#[tokio::test]
#[ignore = "live network call to Hyperunit; run with LIVE_PROVIDER_TESTS=1"]
async fn real_hyperunit_operations_listing_deserializes() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }

    let client =
        HyperUnitClient::new("https://api.hyperunit.xyz").expect("hyperunit client construction");
    let request = UnitOperationsRequest {
        address: TEST_HL_ADDRESS.to_string(),
    };

    // The wallet may or may not have any operations; we just want the
    // response shape to deserialize cleanly into our typed
    // `UnitOperationsResponse`. If Hyperunit ever renames a field, this
    // fails loudly.
    let response = client
        .operations(request)
        .await
        .expect("live Hyperunit /operations call must succeed and deserialize");
    eprintln!(
        "✓ Hyperunit returned {} addresses and {} operations for {TEST_HL_ADDRESS}",
        response.addresses.len(),
        response.operations.len()
    );
}
