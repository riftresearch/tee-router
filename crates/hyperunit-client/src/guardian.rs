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
use sha3::{Digest, Keccak256};
use snafu::Snafu;

use crate::{UnitAsset, UnitGenerateAddressRequest, UnitGenerateAddressResponse};

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
    #[snafu(display("guardian pinned key {node_id:?} could not be parsed: {source}"))]
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

        // 5. construct candidate payload strings and verify against any.
        //
        // Hyperunit's reference verifier (see
        // github.com/lifinance/contracts/script/demoScripts/demoUnit.ts)
        // tries the "new" EVM template first when the proposal's coinType
        // is "ethereum", and tries the "legacy" template otherwise — with
        // a fallback to the other template if the first doesn't verify.
        //
        // Empirically, every /gen and /operations.addresses entry observed
        // against api.hyperunit.xyz uses the NEW template with `coinType`
        // hardcoded to "ethereum" — even for BTC withdrawal addresses,
        // /operations reports `sourceCoinType: "ethereum"`. So we always
        // try that first; we still fall back to the legacy template so a
        // future Hyperunit deployment that re-introduces per-coin-type
        // signing doesn't break.
        let candidates = candidate_payloads(node_id, request, &response.address);
        let mut verified = false;
        for payload in &candidates {
            if verifying_key.verify(payload.as_bytes(), &signature).is_ok() {
                verified = true;
                break;
            }
        }
        if verified {
            valid_count += 1;
        } else {
            errors.push(format!(
                "{node_id}: signature did not verify against any candidate; tried: {candidates:?}"
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

/// Build the candidate payload strings each guardian might have signed,
/// in order of likelihood.
///
/// Hyperunit has two documented payload templates:
///   - **New (EVM):** `{nodeId}:user-{coinType}-{destinationChain}-{destinationAddress}-{address}`
///   - **Legacy:** `{nodeId}:{destinationAddress}-{destinationChain}-{asset}-{address}-{sourceChain}-deposit`
///
/// The trigger documented in the reference verifier is
/// `proposal.coinType === 'ethereum'` → new; otherwise legacy with new
/// as fallback. **Empirically, every /gen + /operations response
/// observed against api.hyperunit.xyz today reports
/// `sourceCoinType: "ethereum"` regardless of asset** — including BTC
/// withdrawal addresses — so the new template with that hardcoded
/// coinType is always the one that verifies in practice. We still
/// emit the legacy candidate as a fallback for forward-compat with
/// possible future Hyperunit deployments.
fn candidate_payloads(
    node_id: &str,
    request: &UnitGenerateAddressRequest,
    address: &str,
) -> Vec<String> {
    let dst_chain = request.dst_chain.as_wire_str();
    let src_chain = request.src_chain.as_wire_str();
    let asset = request.asset.as_wire_str();
    let mut candidates = Vec::new();

    for dst_addr in destination_address_payload_variants(&request.dst_addr) {
        // 1. NEW EVM template with coinType = "ethereum" (the only one
        //    observed against the live API as of 2026-05-23). Always tried
        //    first because /operations responses confirm coinType is
        //    hardcoded ethereum for this Hyperunit deployment.
        candidates.push(format!(
            "{node_id}:user-ethereum-{dst_chain}-{dst_addr}-{address}"
        ));
        // 2. NEW EVM template with coinType derived from the asset, for a
        //    future Hyperunit deployment that respects per-asset coinType.
        candidates.push(format!(
            "{node_id}:user-{coin_type}-{dst_chain}-{dst_addr}-{address}",
            coin_type = match request.asset {
                UnitAsset::Eth => "ethereum",
                UnitAsset::Btc => "bitcoin",
            },
        ));
        // 3. LEGACY template, matching the lifinance reference verifier's
        //    second branch.
        candidates.push(format!(
            "{node_id}:{dst_addr}-{dst_chain}-{asset}-{address}-{src_chain}-deposit"
        ));
    }

    candidates.dedup();
    candidates
}

fn destination_address_payload_variants(dst_addr: &str) -> Vec<String> {
    let mut variants = vec![dst_addr.to_string()];
    if let Some(checksum) = evm_checksum_address(dst_addr) {
        if checksum != dst_addr {
            variants.push(checksum);
        }
    }
    variants
}

fn evm_checksum_address(raw: &str) -> Option<String> {
    let hex = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X"))?;
    if hex.len() != 40 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }

    let lower = hex.to_ascii_lowercase();
    let hash = Keccak256::digest(lower.as_bytes());
    let mut checksum = String::with_capacity(42);
    checksum.push_str("0x");

    for (index, ch) in lower.chars().enumerate() {
        let hash_byte = hash[index / 2];
        let nibble = if index % 2 == 0 {
            hash_byte >> 4
        } else {
            hash_byte & 0x0f
        };
        if ch.is_ascii_alphabetic() && nibble >= 8 {
            checksum.push(ch.to_ascii_uppercase());
        } else {
            checksum.push(ch);
        }
    }

    Some(checksum)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::UnitChain;

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
            VerifyingKey::from_sec1_bytes(&bytes)
                .unwrap_or_else(|e| panic!("mainnet guardian {node_id} pubkey did not parse: {e}"));
        }
    }

    #[test]
    fn candidate_payloads_for_btc_withdrawal() {
        // HL → BTC withdrawal — same shape as the 2026-05-23 funded test
        // that initially revealed Hyperunit hardcodes coinType=ethereum.
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Hyperliquid,
            dst_chain: UnitChain::Bitcoin,
            asset: UnitAsset::Btc,
            dst_addr: "bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn".to_string(),
        };
        let candidates = candidate_payloads(
            "hl-node",
            &request,
            "0xa0756aF705a4C587511a33912AeFbd249b49e2Cc",
        );
        // First candidate must be the one that actually verifies against
        // production responses.
        assert_eq!(
            candidates[0],
            "hl-node:user-ethereum-bitcoin-bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn-0xa0756aF705a4C587511a33912AeFbd249b49e2Cc"
        );
        // Second candidate is the alternate coinType (forward-compat).
        assert!(candidates[1].contains("user-bitcoin-bitcoin-"));
        // Third candidate is the legacy template.
        assert!(candidates[2].ends_with("-hyperliquid-deposit"));
    }

    #[test]
    fn candidate_payloads_for_eth_deposit() {
        // ETH → HL deposit, matching the production /operations entry the
        // tier-1 test exercises.
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: "0xfedcba".to_string(),
        };
        let candidates = candidate_payloads("unit-node", &request, "0x12345");
        assert_eq!(
            candidates[0],
            "unit-node:user-ethereum-hyperliquid-0xfedcba-0x12345"
        );
    }

    #[test]
    fn candidate_payloads_for_eth_withdrawal_include_checksum_destination() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Hyperliquid,
            dst_chain: UnitChain::Ethereum,
            asset: UnitAsset::Eth,
            dst_addr: "0x9d9e065c15d8da95f031eba51c28ca2caddb661e".to_string(),
        };
        let candidates = candidate_payloads(
            "unit-node",
            &request,
            "0xDE5933e0eAad474e16dd14fB8E97C237adB311A7",
        );
        assert_eq!(
            candidates[0],
            "unit-node:user-ethereum-ethereum-0x9d9e065c15d8da95f031eba51c28ca2caddb661e-0xDE5933e0eAad474e16dd14fB8E97C237adB311A7"
        );
        assert!(
            candidates.iter().any(|payload| payload
                == "unit-node:user-ethereum-ethereum-0x9D9E065C15d8Da95f031ebA51c28CA2CAddb661E-0xDE5933e0eAad474e16dd14fB8E97C237adB311A7")
        );
    }

    #[test]
    fn mainnet_verifies_eth_withdrawal_checksum_destination_payload() {
        let request = UnitGenerateAddressRequest {
            src_chain: UnitChain::Hyperliquid,
            dst_chain: UnitChain::Ethereum,
            asset: UnitAsset::Eth,
            dst_addr: "0x9d9e065c15d8da95f031eba51c28ca2caddb661e".to_string(),
        };
        let response = UnitGenerateAddressResponse {
            address: "0xDE5933e0eAad474e16dd14fB8E97C237adB311A7".to_string(),
            status: Some("OK".to_string()),
            signatures: serde_json::json!({
                "field-node": "vSoUKRXTG/4/Q9ueFs6ERYsQ34nXadDZRIxEUgYTDyVp9bTmT4gOnqfo0DNUdteiI7y/v8Jbtvg8kjSa1/sJOQ==",
                "hl-node": "Wa6YbamNDTPy23IQMysQDurS/t8I2lDF/RNEr9cDVBGWUZ/pcidigw0bruy/UA7ORBO5YkOsTe6b+FkNoJS5KA==",
                "unit-node": "KGWRn0SmOT+Vt6j/dZ9FBm/3UkXakpghOCtWhcEN7XGXrxkfUjEZxHCP07N0VwyApodTZEN5htHQu1NympNWVA==",
            }),
        };
        assert!(verify_generate_address(&request, &response, GuardianNetwork::Mainnet).is_ok());
    }
}
