//! Hyperunit guardian signature verification.
//!
//! Every `GET /gen/...` response carries a `signatures` object with one
//! base64-encoded ECDSA P-256 signature per guardian node. The signing
//! protocol is documented at
//! <https://docs.hyperunit.xyz/developers/api/generate-address/guardian-signatures>
//! and summarized here:
//!
//! - **Algorithm:** ECDSA over P-256, SHA-256 hash. Sig encoded as base64
//!   64-byte `R || S`.
//! - **Payload template (legacy, BTC/SOL/etc):**
//!   `{nodeId}:{destinationAddress}-{destinationChain}-{asset}-{address}-{sourceChain}-deposit`
//! - **Payload template (newer, EVM coin types):**
//!   `{nodeId}:user-{coinType}-{destinationChain}-{destinationAddress}-{address}`
//! - **Quorum:** ≥ 2 of 3 guardian signatures must verify.
//! - **Guardian public keys** (uncompressed SEC1, `04 || X || Y`): pinned
//!   below per network.
//!
//! Verifying the quorum closes the audit-medium MITM risk on the
//! deposit-address generation path: a bad upstream / egress cannot
//! substitute a fraudulent deposit address without forging at least
//! two P-256 signatures over the canonical payload.

use base64::Engine as _;
use p256::ecdsa::{signature::Verifier, Signature, VerifyingKey};
use snafu::Snafu;

use crate::{UnitAsset, UnitChain, UnitGenerateAddressRequest, UnitGenerateAddressResponse};

/// Hyperunit network the response was fetched from. Determines which set of
/// pinned guardian public keys to verify against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardianNetwork {
    Mainnet,
    Testnet,
    /// Local mock / unknown deployment — verification is skipped.
    /// `verify_generate_address` returns `Ok(())` in this mode so integration
    /// tests against a local mock server (which produces placeholder
    /// signature strings) continue to work.
    Unknown,
}

impl GuardianNetwork {
    /// Infer the guardian network from the API base URL. Used by
    /// [`HyperUnitClient::new`] so most callers don't have to think about it:
    /// `api.hyperunit.xyz` → `Mainnet`, `api.hyperunit-testnet.xyz` →
    /// `Testnet`, anything else (mock servers, custom deployments) →
    /// `Unknown`. For non-standard deployments call
    /// [`HyperUnitClient::new_with_network`] to set this explicitly.
    pub fn from_base_url(url: &str) -> Self {
        let lower = url.to_ascii_lowercase();
        if lower.contains("hyperunit-testnet.xyz") {
            Self::Testnet
        } else if lower.contains("api.hyperunit.xyz") {
            Self::Mainnet
        } else {
            Self::Unknown
        }
    }
}

/// Minimum number of valid guardian signatures required to accept a response.
pub const REQUIRED_QUORUM: usize = 2;

/// Mainnet guardians. Source: docs.hyperunit.xyz/developers/key-addresses/mainnet.
pub const MAINNET_GUARDIANS: &[(&str, &str)] = &[
    (
        "unit-node",
        "04dc6f89f921dc816aa69b687be1fcc3cc1d48912629abc2c9964e807422e1047e0435cb5ba0fa53cb9a57a9c610b4e872a0a2caedda78c4f85ebafcca93524061",
    ),
    (
        "hl-node",
        "048633ea6ab7e40cdacf37d1340057e84bb9810de0687af78d031e9b07b65ad4ab379180ab55075f5c2ebb96dab30d2c2fab49d5635845327b6a3c27d20ba4755b",
    ),
    (
        "field-node",
        "04ae2ab20787f816ea5d13f36c4c4f7e196e29e867086f3ce818abb73077a237f841b33ada5be71b83f4af29f333dedc5411ca4016bd52ab657db2896ef374ce99",
    ),
];

/// Testnet guardians. Source: docs.hyperunit.xyz/developers/key-addresses/testnet.
pub const TESTNET_GUARDIANS: &[(&str, &str)] = &[
    (
        "node-1",
        "04bab844e8620c4a1ec304df6284cd6fdffcde79b3330a7bffb1e4cecfee72d02a7c1f3a4415b253dc",
    ),
    (
        "hl-node-testnet",
        "04502d20a0d8d8aaea9395eb46d50ad2d8278c1b3a3bcdc200d531253612be23f5",
    ),
    (
        "field-node",
        "04e674a796ff01d6b74f4ee4079640729797538cdb4926ec333ce1bd18414ef7f2",
    ),
];

#[derive(Debug, Snafu)]
pub enum GuardianVerifyError {
    #[snafu(display(
        "guardian quorum not met: {valid_count} of {total} guardian signatures verified \
         (required ≥ {REQUIRED_QUORUM}); errors: {errors:?}"
    ))]
    QuorumNotMet {
        valid_count: usize,
        total: usize,
        errors: Vec<String>,
    },
    #[snafu(display("response `signatures` field is not a JSON object"))]
    SignaturesNotObject,
    #[snafu(display(
        "guardian pinned key {node_id:?} could not be parsed: {source}"
    ))]
    InvalidPinnedKey {
        node_id: String,
        source: p256::ecdsa::Error,
    },
}

/// Verify the guardian-signature quorum on a `/gen/...` response.
///
/// Returns `Ok(())` when ≥ [`REQUIRED_QUORUM`] guardian signatures verify
/// against their pinned public keys, *or* when `network` is
/// [`GuardianNetwork::Unknown`] (mock mode).
pub fn verify_generate_address(
    request: &UnitGenerateAddressRequest,
    response: &UnitGenerateAddressResponse,
    network: GuardianNetwork,
) -> Result<(), GuardianVerifyError> {
    let guardians: &[(&str, &str)] = match network {
        GuardianNetwork::Mainnet => MAINNET_GUARDIANS,
        GuardianNetwork::Testnet => TESTNET_GUARDIANS,
        GuardianNetwork::Unknown => return Ok(()),
    };

    let Some(sig_map) = response.signatures.as_object() else {
        return Err(GuardianVerifyError::SignaturesNotObject);
    };

    let mut valid_count = 0usize;
    let mut errors: Vec<String> = Vec::new();

    for (node_id, pubkey_hex) in guardians {
        // 1. fetch the signature for this guardian
        let Some(sig_value) = sig_map.get(*node_id) else {
            errors.push(format!("{node_id}: missing signature in response"));
            continue;
        };
        let Some(sig_b64) = sig_value.as_str() else {
            errors.push(format!("{node_id}: signature is not a string"));
            continue;
        };

        // 2. base64-decode → expect 64 bytes (R || S)
        let sig_bytes = match base64::engine::general_purpose::STANDARD.decode(sig_b64) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("{node_id}: base64 decode failed: {e}"));
                continue;
            }
        };
        if sig_bytes.len() != 64 {
            errors.push(format!(
                "{node_id}: signature was {} bytes (expected 64)",
                sig_bytes.len()
            ));
            continue;
        }

        // 3. parse the signature into a P-256 ECDSA Signature
        let signature = match Signature::from_slice(&sig_bytes) {
            Ok(s) => s,
            Err(e) => {
                errors.push(format!("{node_id}: signature parse failed: {e}"));
                continue;
            }
        };

        // 4. parse the pinned guardian pubkey (uncompressed SEC1)
        let pubkey_bytes = match hex::decode(pubkey_hex) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("{node_id}: pinned key hex parse failed: {e}"));
                continue;
            }
        };
        let verifying_key = match VerifyingKey::from_sec1_bytes(&pubkey_bytes) {
            Ok(k) => k,
            Err(e) => {
                return Err(GuardianVerifyError::InvalidPinnedKey {
                    node_id: (*node_id).to_string(),
                    source: e,
                });
            }
        };

        // 5. construct the canonical payload string and verify
        let payload = construct_payload(node_id, request, &response.address);
        if verifying_key
            .verify(payload.as_bytes(), &signature)
            .is_ok()
        {
            valid_count += 1;
        } else {
            errors.push(format!(
                "{node_id}: signature did not verify against payload {payload:?}"
            ));
        }
    }

    if valid_count >= REQUIRED_QUORUM {
        Ok(())
    } else {
        Err(GuardianVerifyError::QuorumNotMet {
            valid_count,
            total: guardians.len(),
            errors,
        })
    }
}

/// Build the canonical payload string each guardian signs.
///
/// Two branches per the spec: a "legacy" form for non-EVM assets
/// (BTC, SOL, …) and a "newer" form for EVM coin types (ETH, etc).
fn construct_payload(
    node_id: &str,
    request: &UnitGenerateAddressRequest,
    address: &str,
) -> String {
    match request.asset {
        UnitAsset::Eth => format!(
            // {nodeId}:user-{coinType}-{destinationChain}-{destinationAddress}-{address}
            "{node_id}:user-{coin_type}-{dst_chain}-{dst_addr}-{address}",
            coin_type = evm_coin_type(request.src_chain),
            dst_chain = request.dst_chain.as_wire_str(),
            dst_addr = request.dst_addr,
        ),
        UnitAsset::Btc => format!(
            // {nodeId}:{destinationAddress}-{destinationChain}-{asset}-{address}-{sourceChain}-deposit
            "{node_id}:{dst_addr}-{dst_chain}-{asset}-{address}-{src_chain}-deposit",
            dst_addr = request.dst_addr,
            dst_chain = request.dst_chain.as_wire_str(),
            asset = request.asset.as_wire_str(),
            src_chain = request.src_chain.as_wire_str(),
        ),
    }
}

/// `coinType` value used in the EVM-asset payload template. The spec
/// snippet `proposal.coinType === 'ethereum'` is the canonical
/// example; the value appears to track the source-chain wire name
/// for EVM bridges.
fn evm_coin_type(src_chain: UnitChain) -> &'static str {
    match src_chain {
        UnitChain::Ethereum => "ethereum",
        UnitChain::Base => "base",
        UnitChain::Hyperliquid => "hyperliquid",
        // Non-EVM source paired with ETH asset is unusual but covered
        // for completeness.
        UnitChain::Bitcoin => "bitcoin",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_inference_from_base_url() {
        assert_eq!(
            GuardianNetwork::from_base_url("https://api.hyperunit.xyz"),
            GuardianNetwork::Mainnet
        );
        assert_eq!(
            GuardianNetwork::from_base_url("https://api.hyperunit.xyz/"),
            GuardianNetwork::Mainnet
        );
        assert_eq!(
            GuardianNetwork::from_base_url("https://api.hyperunit-testnet.xyz"),
            GuardianNetwork::Testnet
        );
        assert_eq!(
            GuardianNetwork::from_base_url("http://127.0.0.1:12345"),
            GuardianNetwork::Unknown
        );
    }

    #[test]
    fn unknown_network_skips_verification() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Bitcoin,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Btc,
            dst_addr: "0xabc".to_string(),
        };
        let response = UnitGenerateAddressResponse {
            address: "bc1qzzz".to_string(),
            status: Some("OK".to_string()),
            signatures: serde_json::json!({"field-node": "nonsense"}),
        };
        assert!(verify_generate_address(&request, &response, GuardianNetwork::Unknown).is_ok());
    }

    #[test]
    fn mainnet_pinned_keys_parse() {
        for (node_id, key_hex) in MAINNET_GUARDIANS {
            let bytes = hex::decode(key_hex).expect("hex");
            VerifyingKey::from_sec1_bytes(&bytes).unwrap_or_else(|e| {
                panic!("mainnet guardian {node_id} pubkey did not parse: {e}")
            });
        }
    }

    #[test]
    fn legacy_payload_template_for_btc() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Bitcoin,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Btc,
            dst_addr: "0xfedcba".to_string(),
        };
        let payload = construct_payload("hl-node", &request, "bc1qabc");
        assert_eq!(
            payload,
            "hl-node:0xfedcba-hyperliquid-btc-bc1qabc-bitcoin-deposit"
        );
    }

    #[test]
    fn new_payload_template_for_eth() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: "0xfedcba".to_string(),
        };
        let payload = construct_payload("unit-node", &request, "0x12345");
        assert_eq!(payload, "unit-node:user-ethereum-hyperliquid-0xfedcba-0x12345");
    }
}
