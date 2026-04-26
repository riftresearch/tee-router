use crate::traits::UserDepositCandidateStatus;
use crate::{key_derivation, ChainOperations, Result};
use alloy::hex;
use alloy::primitives::{Address, U256};
use alloy::signers::local::PrivateKeySigner;
use async_trait::async_trait;
use router_primitives::{ChainType, Currency, TokenIdentifier, TxStatus, Wallet};
use std::{str::FromStr, time::Duration};
use tracing::{debug, info};

/// Hyperliquid "chain" — a custody layer where assets persist under an
/// EVM-compatible address. Trading, withdrawal, and intra-HL transfer operations
/// are EIP-712-signed REST API calls, not on-chain transactions; they live in
/// the `HyperliquidProvider` action provider, not on this type.
///
/// User-initiated deposits never land directly on HL: funds reach HL only via a
/// provider (HyperUnit for BTC/ETH, Arbitrum Bridge2 for USDC).
/// `verify_user_deposit_candidate` therefore always returns `TxNotFound`.
pub struct HyperliquidChain {
    chain_type: ChainType,
    wallet_seed_tag: Vec<u8>,
    min_confirmations: u32,
    est_block_time: Duration,
}

impl HyperliquidChain {
    #[must_use]
    pub fn new(wallet_seed_tag: &[u8], min_confirmations: u32, est_block_time: Duration) -> Self {
        Self {
            chain_type: ChainType::Hyperliquid,
            wallet_seed_tag: wallet_seed_tag.to_vec(),
            min_confirmations,
            est_block_time,
        }
    }
}

#[async_trait]
impl ChainOperations for HyperliquidChain {
    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])> {
        let mut salt = [0u8; 32];
        getrandom::getrandom(&mut salt).map_err(|_| crate::Error::Serialization {
            message: "Failed to generate random salt".to_string(),
        })?;

        let signer = PrivateKeySigner::random();
        let address = signer.address();
        let private_key = hex::encode(signer.to_bytes());

        info!(
            "Created new {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        let wallet = Wallet::new(format!("{address:?}"), format!("0x{private_key}"));
        Ok((wallet, salt))
    }

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet> {
        let private_key_bytes =
            key_derivation::derive_private_key(master_key, salt, &self.wallet_seed_tag)?;

        let signer = PrivateKeySigner::from_bytes(&private_key_bytes.into()).map_err(|_| {
            crate::Error::Serialization {
                message: "Failed to create signer from derived key".to_string(),
            }
        })?;

        let address = format!("{:?}", signer.address());
        let private_key = format!("0x{}", hex::encode(private_key_bytes));

        debug!(
            "Derived {} wallet: {}",
            self.chain_type.to_db_string(),
            address
        );

        Ok(Wallet::new(address, private_key))
    }

    async fn verify_user_deposit_candidate(
        &self,
        _recipient_address: &str,
        _currency: &Currency,
        _tx_hash: &str,
        _transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus> {
        Ok(UserDepositCandidateStatus::TxNotFound)
    }

    async fn get_tx_status(&self, _tx_hash: &str) -> Result<TxStatus> {
        unimplemented!("HyperliquidChain::get_tx_status — wired in phase 4.3")
    }

    async fn dump_to_address(
        &self,
        _token: &TokenIdentifier,
        _private_key: &str,
        _recipient_address: &str,
        _fee: U256,
    ) -> Result<String> {
        unimplemented!("HyperliquidChain::dump_to_address — HL sweeps go through the provider")
    }

    fn validate_address(&self, address: &str) -> bool {
        Address::from_str(address).is_ok()
    }

    fn minimum_block_confirmations(&self) -> u32 {
        self.min_confirmations
    }

    fn estimated_block_time(&self) -> Duration {
        self.est_block_time
    }

    async fn get_block_height(&self) -> Result<u64> {
        unimplemented!("HyperliquidChain::get_block_height — wired in phase 4.3")
    }

    async fn get_best_hash(&self) -> Result<String> {
        unimplemented!("HyperliquidChain::get_best_hash — wired in phase 4.3")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SEED_TAG: &[u8] = b"hyperliquid-wallet";

    fn make_chain() -> HyperliquidChain {
        HyperliquidChain::new(TEST_SEED_TAG, 1, Duration::from_secs(1))
    }

    #[test]
    fn derive_wallet_is_deterministic_for_same_inputs() {
        let chain = make_chain();
        let master_key = [0x42u8; 32];
        let salt = [0x11u8; 32];
        let wallet_1 = chain.derive_wallet(&master_key, &salt).expect("derive");
        let wallet_2 = chain.derive_wallet(&master_key, &salt).expect("derive");
        assert_eq!(wallet_1.address, wallet_2.address);
        assert_eq!(wallet_1.private_key(), wallet_2.private_key());
    }

    #[test]
    fn derive_wallet_differs_for_different_salts() {
        let chain = make_chain();
        let master_key = [0x42u8; 32];
        let salt_a = [0x11u8; 32];
        let salt_b = [0x22u8; 32];
        let wallet_a = chain.derive_wallet(&master_key, &salt_a).expect("derive");
        let wallet_b = chain.derive_wallet(&master_key, &salt_b).expect("derive");
        assert_ne!(wallet_a.address, wallet_b.address);
    }

    #[test]
    fn derive_wallet_produces_evm_shaped_address() {
        let chain = make_chain();
        let master_key = [0x42u8; 32];
        let salt = [0x11u8; 32];
        let wallet = chain.derive_wallet(&master_key, &salt).expect("derive");
        assert!(wallet.address.starts_with("0x"));
        assert_eq!(wallet.address.len(), 42);
        assert!(wallet.address[2..].chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn derive_wallet_differs_between_seed_tags() {
        let chain_hl = HyperliquidChain::new(b"hyperliquid-wallet", 1, Duration::from_secs(1));
        let chain_eth = HyperliquidChain::new(b"ethereum-wallet", 1, Duration::from_secs(1));
        let master_key = [0x42u8; 32];
        let salt = [0x11u8; 32];
        let wallet_hl = chain_hl.derive_wallet(&master_key, &salt).expect("derive");
        let wallet_eth = chain_eth.derive_wallet(&master_key, &salt).expect("derive");
        assert_ne!(wallet_hl.private_key(), wallet_eth.private_key());
        assert_ne!(wallet_hl.address, wallet_eth.address);
    }

    #[test]
    fn validate_address_accepts_evm_formats() {
        let chain = make_chain();
        assert!(chain.validate_address("0x742d35cc6634C0532925a3b844Bc9e7595f0bEb7"));
        assert!(chain.validate_address("0x742d35cc6634c0532925a3b844bc9e7595f0beb7"));
    }

    #[test]
    fn validate_address_rejects_non_evm() {
        let chain = make_chain();
        assert!(!chain.validate_address("bc1qw508d6qejxtdg4y5r3zarvary0c5xw7kv8f3t4"));
        assert!(!chain.validate_address("hello"));
        assert!(!chain.validate_address(""));
        assert!(!chain.validate_address("0x"));
    }

    #[tokio::test]
    async fn verify_user_deposit_candidate_always_returns_tx_not_found() {
        let chain = make_chain();
        let currency = Currency {
            chain: ChainType::Hyperliquid,
            token: TokenIdentifier::Native,
            decimals: 8,
        };
        let status = chain
            .verify_user_deposit_candidate(
                "0x742d35cc6634c0532925a3b844bc9e7595f0beb7",
                &currency,
                "0xdeadbeef",
                0,
            )
            .await
            .expect("call");
        assert!(matches!(status, UserDepositCandidateStatus::TxNotFound));
    }

    #[test]
    fn config_getters_return_what_was_set() {
        let chain = HyperliquidChain::new(TEST_SEED_TAG, 3, Duration::from_millis(500));
        assert_eq!(chain.minimum_block_confirmations(), 3);
        assert_eq!(chain.estimated_block_time(), Duration::from_millis(500));
    }
}
