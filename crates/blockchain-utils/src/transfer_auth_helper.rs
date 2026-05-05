use std::{
    collections::HashMap,
    sync::{LazyLock, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use alloy::{
    dyn_abi::DynSolValue,
    primitives::{keccak256, Address, FixedBytes, U256},
    providers::DynProvider,
    signers::{local::PrivateKeySigner, SignerSync},
};
use bitcoin::key::rand::RngCore;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::EIP7702Delegator::Execution;
use router_primitives::{ChainType, Lot, TokenIdentifier};
use snafu::{location, Location, ResultExt, Snafu};
use tracing::warn;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Eip3009MetadataKey {
    chain: ChainType,
    token_address: Address,
}

#[derive(Debug, Clone)]
struct Eip3009Metadata {
    domain_separator: FixedBytes<32>,
    transfer_with_authorization_typehash: FixedBytes<32>,
}

static EIP3009_METADATA_CACHE: LazyLock<RwLock<HashMap<Eip3009MetadataKey, Eip3009Metadata>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
const TRANSFER_AUTHORIZATION_VALIDITY: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Snafu)]
pub enum TransferAuthorizationError {
    UnsupportedToken {
        token: TokenIdentifier,
        #[snafu(implicit)]
        loc: Location,
    },
    #[snafu(display("Invalid transfer authorization token address {address} at {loc}"))]
    InvalidTokenAddress {
        address: String,
        #[snafu(implicit)]
        loc: Location,
    },
    #[snafu(display("Transfer authorization contract call {call} failed at {loc}: {message}"))]
    ContractCallFailed {
        call: &'static str,
        message: String,
        #[snafu(implicit)]
        loc: Location,
    },
    SignatureFailed {
        source: alloy::signers::Error,
        #[snafu(implicit)]
        loc: Location,
    },
    #[snafu(display("System clock was before the Unix epoch at {loc}: {source}"))]
    ClockFailed {
        source: std::time::SystemTimeError,
        #[snafu(implicit)]
        loc: Location,
    },
    #[snafu(display("Transfer authorization expiry overflowed at {loc}"))]
    ValidBeforeOverflow {
        #[snafu(implicit)]
        loc: Location,
    },
}

pub async fn create_transfer_with_authorization_execution(
    lot: &Lot,
    lot_signer: &PrivateKeySigner,
    provider: &DynProvider,
    recipient: &Address,
) -> Result<Execution, TransferAuthorizationError> {
    let token_address = match &lot.currency.token {
        TokenIdentifier::Address(address) => address.parse::<Address>().map_err(|_| {
            TransferAuthorizationError::InvalidTokenAddress {
                address: address.clone(),
                loc: location!(),
            }
        })?,
        _ => {
            return UnsupportedTokenSnafu {
                token: lot.currency.token.clone(),
            }
            .fail()
        }
    };
    let eip_3009_token_contract = GenericEIP3009ERC20Instance::new(token_address, provider);

    let metadata_key = Eip3009MetadataKey {
        chain: lot.currency.chain,
        token_address,
    };
    let metadata = match cached_eip3009_metadata(&metadata_key) {
        Some(metadata) => metadata,
        None => {
            let metadata = Eip3009Metadata {
                domain_separator: eip_3009_token_contract
                    .DOMAIN_SEPARATOR()
                    .call()
                    .await
                    .map_err(|e| TransferAuthorizationError::ContractCallFailed {
                        call: "DOMAIN_SEPARATOR",
                        message: e.to_string(),
                        loc: location!(),
                    })?,
                transfer_with_authorization_typehash: eip_3009_token_contract
                    .TRANSFER_WITH_AUTHORIZATION_TYPEHASH()
                    .call()
                    .await
                    .map_err(|e| TransferAuthorizationError::ContractCallFailed {
                        call: "TRANSFER_WITH_AUTHORIZATION_TYPEHASH",
                        message: e.to_string(),
                        loc: location!(),
                    })?,
            };
            store_eip3009_metadata(metadata_key, metadata.clone());
            metadata
        }
    };

    let from = lot_signer.address();
    let to = *recipient;
    let value = lot.amount;
    let valid_after = U256::ZERO;
    let valid_before = transfer_authorization_valid_before(SystemTime::now())?;
    let mut nonce: [u8; 32] = [0; 32];
    bitcoin::key::rand::thread_rng().fill_bytes(&mut nonce);
    let nonce = nonce.into();

    /*
     * @notice Receive a transfer with a signed authorization from the payer
     * @dev This has an additional check to ensure that the payee's address
     * matches the caller of this function to prevent front-running attacks.
     * @param from          Payer's address (Authorizer)
     * @param to            Payee's address
     * @param value         Amount to be transferred
     * @param validAfter    The time after which this is valid (unix time)
     * @param validBefore   The time before which this is valid (unix time)
     * @param nonce         Unique nonce
     * @param v             v of the signature
     * @param r             r of the signature
     * @param s             s of the signature
    function receiveWithAuthorization(
        address from,
        address to,
        uint256 value,
        uint256 validAfter,
        uint256 validBefore,
        bytes32 nonce,
        uint8 v,
        bytes32 r,
        bytes32 s
    )
    */
    let message_value = DynSolValue::Tuple(vec![
        DynSolValue::FixedBytes(metadata.transfer_with_authorization_typehash, 32), // transfer_with_authorization_typehash
        DynSolValue::Address(from),                                                 // from
        DynSolValue::Address(to),                                                   // to
        DynSolValue::Uint(value, 256),                                              // value
        DynSolValue::Uint(valid_after, 256),                                        // validAfter
        DynSolValue::Uint(valid_before, 256),                                       // validBefore
        DynSolValue::FixedBytes(nonce, 32),
    ]);

    let encoded_message = message_value.abi_encode();
    let message_hash = keccak256(&encoded_message);
    let eip712_hash = keccak256(
        [
            &[0x19, 0x01],
            &metadata.domain_separator[..],
            &message_hash[..],
        ]
        .concat(),
    );
    let signature = lot_signer
        .sign_hash_sync(&eip712_hash)
        .context(SignatureFailedSnafu)?;

    let calldata = eip_3009_token_contract
        .transferWithAuthorization(
            from,
            to,
            value,
            valid_after,
            valid_before,
            nonce,
            27 + (signature.v() as u8), // Note: the Solidity ECRecover library expects v to be 27 or 28
            signature.r().into(),
            signature.s().into(),
        )
        .calldata()
        .clone();

    Ok(Execution {
        target: token_address,
        value: U256::ZERO,
        callData: calldata.clone(),
    })
}

fn cached_eip3009_metadata(key: &Eip3009MetadataKey) -> Option<Eip3009Metadata> {
    match EIP3009_METADATA_CACHE.read() {
        Ok(cache) => cache.get(key).cloned(),
        Err(error) => {
            warn!(error = %error, "EIP-3009 metadata cache read lock was poisoned");
            None
        }
    }
}

fn store_eip3009_metadata(key: Eip3009MetadataKey, metadata: Eip3009Metadata) {
    match EIP3009_METADATA_CACHE.write() {
        Ok(mut cache) => {
            cache.insert(key, metadata);
        }
        Err(error) => {
            warn!(error = %error, "EIP-3009 metadata cache write lock was poisoned");
        }
    }
}

fn transfer_authorization_valid_before(
    now: SystemTime,
) -> Result<U256, TransferAuthorizationError> {
    let unix_now = now
        .duration_since(UNIX_EPOCH)
        .context(ClockFailedSnafu)?
        .as_secs();
    transfer_authorization_valid_before_from_unix_secs(unix_now)
}

fn transfer_authorization_valid_before_from_unix_secs(
    unix_now: u64,
) -> Result<U256, TransferAuthorizationError> {
    let valid_before = unix_now
        .checked_add(TRANSFER_AUTHORIZATION_VALIDITY.as_secs())
        .ok_or(TransferAuthorizationError::ValidBeforeOverflow { loc: location!() })?;
    Ok(U256::from(valid_before))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::B256;

    #[test]
    fn eip3009_metadata_cache_is_scoped_by_chain_and_token() {
        let token_address = Address::repeat_byte(0x11);
        let ethereum_key = Eip3009MetadataKey {
            chain: ChainType::Ethereum,
            token_address,
        };
        let base_key = Eip3009MetadataKey {
            chain: ChainType::Base,
            token_address,
        };
        let metadata = Eip3009Metadata {
            domain_separator: B256::repeat_byte(0x22),
            transfer_with_authorization_typehash: B256::repeat_byte(0x33),
        };

        store_eip3009_metadata(ethereum_key, metadata.clone());

        assert_eq!(
            cached_eip3009_metadata(&ethereum_key)
                .expect("metadata should be cached")
                .domain_separator,
            metadata.domain_separator
        );
        assert!(cached_eip3009_metadata(&base_key).is_none());
    }

    #[test]
    fn transfer_authorization_expiry_is_bounded_from_current_time() {
        let valid_before =
            transfer_authorization_valid_before(UNIX_EPOCH + Duration::from_secs(100))
                .expect("valid system time");

        assert_eq!(
            valid_before,
            U256::from(100 + TRANSFER_AUTHORIZATION_VALIDITY.as_secs())
        );
    }

    #[test]
    fn transfer_authorization_expiry_rejects_overflow() {
        let overflow_unix_secs = u64::MAX - TRANSFER_AUTHORIZATION_VALIDITY.as_secs() + 1;

        let error = transfer_authorization_valid_before_from_unix_secs(overflow_unix_secs)
            .expect_err("expiry overflow should fail");

        assert!(matches!(
            error,
            TransferAuthorizationError::ValidBeforeOverflow { .. }
        ));
    }
}
