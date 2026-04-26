use crate::Result;
use alloy::primitives::U256;
use async_trait::async_trait;
use router_primitives::{Currency, TokenIdentifier, TxStatus, Wallet};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifiedUserDeposit {
    pub amount: U256,
    pub confirmations: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UserDepositCandidateStatus {
    TxNotFound,
    TransferNotFound,
    Verified(VerifiedUserDeposit),
}

#[async_trait]
pub trait ChainOperations: Send + Sync {
    #[must_use]
    fn esplora_client(&self) -> Option<&esplora_client::AsyncClient> {
        None
    }

    fn create_wallet(&self) -> Result<(Wallet, [u8; 32])>;

    fn derive_wallet(&self, master_key: &[u8], salt: &[u8; 32]) -> Result<Wallet>;

    async fn verify_user_deposit_candidate(
        &self,
        recipient_address: &str,
        currency: &Currency,
        tx_hash: &str,
        transfer_index: u64,
    ) -> Result<UserDepositCandidateStatus>;

    async fn get_tx_status(&self, tx_hash: &str) -> Result<TxStatus>;

    async fn dump_to_address(
        &self,
        token: &TokenIdentifier,
        private_key: &str,
        recipient_address: &str,
        fee: U256,
    ) -> Result<String>;

    fn validate_address(&self, address: &str) -> bool;

    fn minimum_block_confirmations(&self) -> u32;

    fn estimated_block_time(&self) -> Duration;

    async fn get_block_height(&self) -> Result<u64>;

    async fn get_best_hash(&self) -> Result<String>;
}
