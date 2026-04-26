//! EIP-712 signing for Hyperliquid.
//!
//! Two flavours:
//!   * `sign_l1_action` — L1 trading actions (Order, Cancel, VaultTransfer,
//!     ...). The caller hashes the action (see [`crate::actions::Actions::hash`])
//!     into a `connection_id`, which is then wrapped in a fixed `Agent`
//!     EIP-712 struct and signed.
//!   * `sign_typed_data` — user actions (Withdraw3, UsdSend, SpotSend, ...).
//!     Each action declares its own EIP-712 domain + struct hash via the
//!     [`Eip712`] trait; the signature is derived directly.

use alloy::{
    dyn_abi::Eip712Domain,
    primitives::{Address, B256},
    signers::{local::PrivateKeySigner, Signature, SignerSync},
    sol,
    sol_types::{eip712_domain, SolStruct},
};

use crate::{eip712::Eip712, error::Error};

pub(crate) mod l1 {
    use super::{eip712_domain, sol, Address, Eip712, Eip712Domain, SolStruct, B256};

    sol! {
        #[derive(Debug)]
        struct Agent {
            string source;
            bytes32 connectionId;
        }
    }

    impl Eip712 for Agent {
        fn domain(&self) -> Eip712Domain {
            eip712_domain! {
                name: "Exchange",
                version: "1",
                chain_id: 1337,
                verifying_contract: Address::ZERO,
            }
        }

        fn struct_hash(&self) -> B256 {
            self.eip712_hash_struct()
        }
    }
}

/// Sign an L1 trading action whose contents have already been hashed into
/// `connection_id`. Mainnet signs under source "a"; testnet under "b".
pub fn sign_l1_action(
    wallet: &PrivateKeySigner,
    connection_id: B256,
    is_mainnet: bool,
) -> Result<Signature, Error> {
    let source = if is_mainnet { "a" } else { "b" }.to_string();
    let payload = l1::Agent {
        source,
        connectionId: connection_id,
    };
    sign_typed_data(&payload, wallet)
}

/// Sign any typed payload that knows its own EIP-712 domain + struct hash.
pub fn sign_typed_data<T: Eip712>(
    payload: &T,
    wallet: &PrivateKeySigner,
) -> Result<Signature, Error> {
    wallet
        .sign_hash_sync(&payload.eip712_signing_hash())
        .map_err(|e| Error::Signature {
            message: e.to_string(),
        })
}

/// Recover the EVM address that signed an L1 action hashed to
/// `connection_id`. Inverse of [`sign_l1_action`] — the `is_mainnet` flag
/// must match what the signer used (source = "a" mainnet, "b" testnet).
pub fn recover_l1_signer(
    connection_id: B256,
    is_mainnet: bool,
    signature: &Signature,
) -> Result<Address, Error> {
    let source = if is_mainnet { "a" } else { "b" }.to_string();
    let payload = l1::Agent {
        source,
        connectionId: connection_id,
    };
    recover_typed_signer(&payload, signature)
}

/// Recover the EVM address that signed a typed-data payload. Inverse of
/// [`sign_typed_data`].
pub fn recover_typed_signer<T: Eip712>(
    payload: &T,
    signature: &Signature,
) -> Result<Address, Error> {
    signature
        .recover_address_from_prehash(&payload.eip712_signing_hash())
        .map_err(|e| Error::Signature {
            message: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    //! Golden-vector tests ported byte-for-byte from hyperliquid-rust-sdk's
    //! `signature/create_signature.rs`. Any drift in our signing pipeline
    //! (EIP-712 encoding, domain construction, signature serialization) will
    //! surface here as a hex mismatch against the upstream-known sigs.

    use std::str::FromStr;

    use alloy::{
        primitives::{keccak256, Address, B256},
        signers::local::PrivateKeySigner,
        sol_types::{eip712_domain, SolValue},
    };
    use serde::{Deserialize, Serialize};

    use super::*;

    fn get_wallet() -> PrivateKeySigner {
        "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
            .parse::<PrivateKeySigner>()
            .expect("known-good test key")
    }

    fn sig_to_hex(sig: &Signature) -> String {
        // Hyperliquid serializes v as 27 + parity; alloy's `Signature::to_string`
        // already yields that form (`0x{r}{s}{v:02x}`), so the hex below
        // matches the SDK-produced fixtures verbatim.
        sig.to_string()
    }

    #[test]
    fn sign_l1_action_matches_sdk_vectors() {
        let wallet = get_wallet();
        let connection_id =
            B256::from_str("0xde6c4037798a4434ca03cd05f00e3b803126221375cd1e7eaaaf041768be06eb")
                .expect("fixed hex");

        let mainnet = sign_l1_action(&wallet, connection_id, true).expect("sign mainnet");
        assert_eq!(
            sig_to_hex(&mainnet),
            "0xfa8a41f6a3fa728206df80801a83bcbfbab08649cd34d9c0bfba7c7b2f99340f53a00226604567b98a1492803190d65a201d6805e5831b7044f17fd530aec7841c",
        );

        let testnet = sign_l1_action(&wallet, connection_id, false).expect("sign testnet");
        assert_eq!(
            sig_to_hex(&testnet),
            "0x1713c0fc661b792a50e8ffdd59b637b1ed172d9a3aa4d801d9d88646710fb74b33959f4d075a7ccbec9f2374a6da21ffa4448d58d0413a0d335775f680a881431c",
        );
    }

    // Minimal local re-declarations of UsdSend/Withdraw3 pinned to SDK field
    // layouts, used only to exercise the typed-data signing path with
    // upstream-known vectors.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    struct UsdSend {
        pub signature_chain_id: u64,
        pub hyperliquid_chain: String,
        pub destination: String,
        pub amount: String,
        pub time: u64,
    }

    impl Eip712 for UsdSend {
        fn domain(&self) -> Eip712Domain {
            eip712_domain! {
                name: "HyperliquidSignTransaction",
                version: "1",
                chain_id: self.signature_chain_id,
                verifying_contract: Address::ZERO,
            }
        }

        fn struct_hash(&self) -> B256 {
            let items = (
                keccak256(
                    "HyperliquidTransaction:UsdSend(string hyperliquidChain,string destination,string amount,uint64 time)",
                ),
                keccak256(&self.hyperliquid_chain),
                keccak256(&self.destination),
                keccak256(&self.amount),
                self.time,
            );
            keccak256(items.abi_encode())
        }
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    struct Withdraw3 {
        pub signature_chain_id: u64,
        pub hyperliquid_chain: String,
        pub destination: String,
        pub amount: String,
        pub time: u64,
    }

    impl Eip712 for Withdraw3 {
        fn domain(&self) -> Eip712Domain {
            eip712_domain! {
                name: "HyperliquidSignTransaction",
                version: "1",
                chain_id: self.signature_chain_id,
                verifying_contract: Address::ZERO,
            }
        }

        fn struct_hash(&self) -> B256 {
            let items = (
                keccak256(
                    "HyperliquidTransaction:Withdraw(string hyperliquidChain,string destination,string amount,uint64 time)",
                ),
                keccak256(&self.hyperliquid_chain),
                keccak256(&self.destination),
                keccak256(&self.amount),
                self.time,
            );
            keccak256(items.abi_encode())
        }
    }

    #[test]
    fn sign_usd_send_matches_sdk_vector() {
        let wallet = get_wallet();
        let payload = UsdSend {
            signature_chain_id: 421_614,
            hyperliquid_chain: "Testnet".to_string(),
            destination: "0x0D1d9635D0640821d15e323ac8AdADfA9c111414".to_string(),
            amount: "1".to_string(),
            time: 1_690_393_044_548,
        };
        let sig = sign_typed_data(&payload, &wallet).expect("sign");
        assert_eq!(
            sig_to_hex(&sig),
            "0x214d507bbdaebba52fa60928f904a8b2df73673e3baba6133d66fe846c7ef70451e82453a6d8db124e7ed6e60fa00d4b7c46e4d96cb2bd61fd81b6e8953cc9d21b",
        );
    }

    #[test]
    fn sign_withdraw3_matches_sdk_vector() {
        let wallet = get_wallet();
        let payload = Withdraw3 {
            signature_chain_id: 421_614,
            hyperliquid_chain: "Testnet".to_string(),
            destination: "0x0D1d9635D0640821d15e323ac8AdADfA9c111414".to_string(),
            amount: "1".to_string(),
            time: 1_690_393_044_548,
        };
        let sig = sign_typed_data(&payload, &wallet).expect("sign");
        assert_eq!(
            sig_to_hex(&sig),
            "0xb3172e33d2262dac2b4cb135ce3c167fda55dafa6c62213564ab728b9f9ba76b769a938e9f6d603dae7154c83bf5a4c3ebab81779dc2db25463a3ed663c82ae41c",
        );
    }

    // Minimal SpotSend re-declaration mirroring hyperliquid-rust-sdk's struct
    // field order and type string, for signing-pipeline validation.
    #[derive(Serialize, Deserialize, Debug, Clone)]
    #[serde(rename_all = "camelCase")]
    struct SpotSend {
        pub signature_chain_id: u64,
        pub hyperliquid_chain: String,
        pub destination: String,
        pub token: String,
        pub amount: String,
        pub time: u64,
    }

    impl Eip712 for SpotSend {
        fn domain(&self) -> Eip712Domain {
            eip712_domain! {
                name: "HyperliquidSignTransaction",
                version: "1",
                chain_id: self.signature_chain_id,
                verifying_contract: Address::ZERO,
            }
        }

        fn struct_hash(&self) -> B256 {
            let items = (
                keccak256(
                    "HyperliquidTransaction:SpotSend(string hyperliquidChain,string destination,string token,string amount,uint64 time)",
                ),
                keccak256(&self.hyperliquid_chain),
                keccak256(&self.destination),
                keccak256(&self.token),
                keccak256(&self.amount),
                self.time,
            );
            keccak256(items.abi_encode())
        }
    }

    // SpotSend is not in hyperliquid-rust-sdk's `create_signature.rs` test
    // fixtures. Pinning this signature locally guards against drift in our
    // own SpotSend struct_hash / type string; the `sign_typed_data` pipeline
    // itself is already SDK-validated by the UsdSend + Withdraw3 vectors
    // above, so if this vector ever breaks, the diff will be in
    // SpotSend-specific code.
    #[test]
    fn sign_spot_send_matches_pinned_local_vector() {
        let wallet = get_wallet();
        let payload = SpotSend {
            signature_chain_id: 421_614,
            hyperliquid_chain: "Testnet".to_string(),
            destination: "0x0D1d9635D0640821d15e323ac8AdADfA9c111414".to_string(),
            token: "UBTC:0x1111111111111111111111111111111111111111".to_string(),
            amount: "1".to_string(),
            time: 1_690_393_044_548,
        };
        let sig = sign_typed_data(&payload, &wallet).expect("sign");
        // Captured from our implementation after SDK-structure audit.
        assert_eq!(
            sig_to_hex(&sig),
            "0xf05348a32b904747d7056926795b427eae7d3a72a6dfa4c266ea4ed0ec1b45ba3875d83e98a1904a16aba63979a49a72701296039420898867bf5fe016fa3d301c",
        );
    }
}
