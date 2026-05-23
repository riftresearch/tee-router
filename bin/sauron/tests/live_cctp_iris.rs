//! Tier-1 live differential against real Circle Iris V2.
//!
//! Hits the production endpoint at `https://iris-api.circle.com` with a known
//! completed burn transaction hash and asserts our `CctpIrisClient` returns
//! `Ready`. This validates that the on-the-wire response shape we model in
//! `cctp_iris::CctpMessagesResponse` / `CctpMessageEntry` /
//! `CctpDecodedMessage` still matches reality — without it the equivalent
//! check in production silently no-ops.
//!
//! Gated by `LIVE_PROVIDER_TESTS=1` so CI doesn't burn requests against Iris
//! by default. Run locally with:
//!
//!     LIVE_PROVIDER_TESTS=1 cargo test -p sauron --test live_cctp_iris -- --ignored --nocapture
//!
//! Companion to the fixture-replay test in `cctp_iris::tests` (which uses a
//! cached real response) — this one re-validates against the live endpoint.

use sauron::cctp_iris::{CctpAttestationStatus, CctpIrisClient};

/// CCTP source-domain for Arbitrum One (where the test burn originated).
const ARBITRUM_SOURCE_DOMAIN: u32 = 3;

/// Real burn tx hash from the 2026-05-22 stuck-order refund — minted 40 USDC
/// from Arbitrum domain 3 to Base domain 6. Iris has had this attestation
/// `complete` since 2026-05-22; it should remain queryable indefinitely.
const KNOWN_COMPLETE_BURN_TX_HASH: &str =
    "0x2d77d9667cc326816cd0ddc736a2222829e2087c7073362f16e0043141e43c96";

#[tokio::test]
#[ignore = "live network call to Circle Iris; run with LIVE_PROVIDER_TESTS=1"]
async fn real_iris_returns_ready_for_known_complete_burn() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }

    let client = CctpIrisClient::new("https://iris-api.circle.com").expect("client construction");
    let status = client
        .attestation_status(ARBITRUM_SOURCE_DOMAIN, KNOWN_COMPLETE_BURN_TX_HASH)
        .await
        .expect("live Iris call must succeed");

    assert_eq!(
        status,
        CctpAttestationStatus::Ready,
        "Iris reported {status:?} for a burn that was confirmed complete on 2026-05-22; \
         either our response-shape model has drifted, or Circle changed something"
    );
}

#[tokio::test]
#[ignore = "live network call to Circle Iris; run with LIVE_PROVIDER_TESTS=1"]
async fn real_iris_returns_pending_for_unknown_burn() {
    if std::env::var("LIVE_PROVIDER_TESTS").ok().as_deref() != Some("1") {
        eprintln!("LIVE_PROVIDER_TESTS != 1; skipping");
        return;
    }

    // 32-byte hash that's structurally valid but never burned — Iris should
    // return 404 which our client maps to `Pending`.
    let bogus = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let client = CctpIrisClient::new("https://iris-api.circle.com").expect("client construction");
    let status = client
        .attestation_status(ARBITRUM_SOURCE_DOMAIN, bogus)
        .await
        .expect("live Iris call must succeed (404 is treated as Pending)");

    assert_eq!(
        status,
        CctpAttestationStatus::Pending,
        "Iris reported {status:?} for an unknown burn tx; client should map 404 to Pending"
    );
}
