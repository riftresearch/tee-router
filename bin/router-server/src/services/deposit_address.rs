use crate::protocol::{backend_chain_for_id, ChainId};
use alloy::primitives::keccak256;
use chains::ChainRegistry;
use snafu::Snafu;
use uuid::Uuid;

const DEPOSIT_VAULT_SALT_DOMAIN: &[u8] = b"router-server-deposit-vault-v1";

#[derive(Debug, Snafu)]
pub enum DepositAddressError {
    #[snafu(display("Chain not supported: {}", chain))]
    ChainNotSupported { chain: ChainId },

    #[snafu(display("Failed to derive wallet: {}", source))]
    WalletDerivation { source: chains::Error },
}

pub type DepositAddressResult<T> = Result<T, DepositAddressError>;

#[must_use]
pub fn derive_deposit_salt_for_quote(master_key: &[u8], quote_id: Uuid) -> [u8; 32] {
    let mut buf = Vec::with_capacity(DEPOSIT_VAULT_SALT_DOMAIN.len() + master_key.len() + 16);
    buf.extend_from_slice(DEPOSIT_VAULT_SALT_DOMAIN);
    buf.extend_from_slice(master_key);
    buf.extend_from_slice(quote_id.as_bytes());
    keccak256(&buf).into()
}

pub fn derive_deposit_address_for_quote(
    chain_registry: &ChainRegistry,
    master_key: &[u8],
    quote_id: Uuid,
    chain_id: &ChainId,
) -> DepositAddressResult<(String, [u8; 32])> {
    let backend_chain =
        backend_chain_for_id(chain_id).ok_or(DepositAddressError::ChainNotSupported {
            chain: chain_id.clone(),
        })?;
    let chain =
        chain_registry
            .get(&backend_chain)
            .ok_or(DepositAddressError::ChainNotSupported {
                chain: chain_id.clone(),
            })?;
    let salt = derive_deposit_salt_for_quote(master_key, quote_id);
    let wallet = chain
        .derive_wallet(master_key, &salt)
        .map_err(|source| DepositAddressError::WalletDerivation { source })?;
    Ok((wallet.address.clone(), salt))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn salt_is_deterministic_for_same_inputs() {
        let master_key = [0x11u8; 64];
        let quote_id = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        let a = derive_deposit_salt_for_quote(&master_key, quote_id);
        let b = derive_deposit_salt_for_quote(&master_key, quote_id);
        assert_eq!(a, b);
    }

    #[test]
    fn salt_differs_for_different_quote_ids() {
        let master_key = [0x11u8; 64];
        let q1 = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        let q2 = Uuid::parse_str("019557a1-0000-7000-8000-000000000002").unwrap();
        assert_ne!(
            derive_deposit_salt_for_quote(&master_key, q1),
            derive_deposit_salt_for_quote(&master_key, q2)
        );
    }

    #[test]
    fn salt_differs_for_different_master_keys() {
        let q = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        assert_ne!(
            derive_deposit_salt_for_quote(&[0x11u8; 64], q),
            derive_deposit_salt_for_quote(&[0x22u8; 64], q)
        );
    }

    #[test]
    fn salt_uses_domain_separation() {
        let master_key = [0x11u8; 64];
        let quote_id = Uuid::parse_str("019557a1-0000-7000-8000-000000000001").unwrap();
        let with_domain = derive_deposit_salt_for_quote(&master_key, quote_id);
        let without_domain: [u8; 32] =
            keccak256([master_key.as_slice(), quote_id.as_bytes()].concat()).into();
        assert_ne!(with_domain, without_domain);
    }
}
