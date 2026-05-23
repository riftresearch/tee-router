use crate::batched_tx::{max_fee_per_gas_with_headroom, retry_evm_rpc, wait_for_evm_receipt};
use crate::error::{PaymasterError, Result};
use alloy::hex;
use alloy::network::{TransactionBuilder, TransactionBuilder7702};
use alloy::primitives::{Address, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::{Authorization, TransactionRequest};
use alloy::signers::{local::PrivateKeySigner, SignerSync};
use eip7702_delegator_contract::EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS;
use std::str::FromStr;
use tracing::{debug, info, warn};

/// EIP-7702 designator prefix. A delegated EOA's code is exactly 23 bytes:
/// `0xef 0x01 0x00 || delegated_contract_address (20 bytes)`.
const EIP7702_DESIGNATOR_PREFIX: [u8; 3] = [0xef, 0x01, 0x00];
const EIP7702_DESIGNATOR_LEN: usize = 23;

pub async fn ensure_eip7702_delegation(
    provider: &DynProvider,
    sponsor_signer: &PrivateKeySigner,
    metric_label: &'static str,
) -> Result<()> {
    ensure_eip7702_delegation_with_wallet(provider, provider, sponsor_signer, metric_label).await
}

pub(crate) async fn ensure_eip7702_delegation_with_wallet(
    provider: &DynProvider,
    wallet_provider: &DynProvider,
    sponsor_signer: &PrivateKeySigner,
    metric_label: &'static str,
) -> Result<()> {
    let sponsor_address = sponsor_signer.address();
    let delegator_contract_address = Address::from_str(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS)
        .map_err(|_| PaymasterError::Serialization {
            message: "Invalid EIP-7702 delegator address".to_string(),
        })?;
    let code = retry_evm_rpc(metric_label, "get_code_at", || async {
        provider.get_code_at(sponsor_address).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;

    let mut delegation_pattern = hex!("ef0100").to_vec();
    delegation_pattern.extend_from_slice(delegator_contract_address.as_slice());
    if code.starts_with(&delegation_pattern) {
        debug!(
            chain = metric_label,
            sponsor = %sponsor_address,
            "EVM paymaster sponsor already delegated to EIP-7702 delegator"
        );
        return Ok(());
    }
    // The sponsor's code already contains a non-empty EIP-7702 designator,
    // but it points to a *different* delegator. Re-delegating overwrites it.
    // Log loudly so operators can audit the rotation rather than have it
    // silently happen on a paymaster cold-start.
    if code.len() == EIP7702_DESIGNATOR_LEN && code.starts_with(&EIP7702_DESIGNATOR_PREFIX) {
        let existing = &code[EIP7702_DESIGNATOR_PREFIX.len()..];
        warn!(
            chain = metric_label,
            sponsor = %sponsor_address,
            existing_delegator = %hex::encode(existing),
            new_delegator = %delegator_contract_address,
            "sponsor is currently delegated to a different EIP-7702 contract; re-delegating to ours"
        );
    }

    let nonce = retry_evm_rpc(metric_label, "get_transaction_count", || async {
        provider.get_transaction_count(sponsor_address).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let chain_id = retry_evm_rpc(metric_label, "get_chain_id", || async {
        provider.get_chain_id().await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;

    let authorization = Authorization {
        chain_id: U256::from(chain_id),
        address: delegator_contract_address,
        nonce: nonce
            .checked_add(1)
            .ok_or(PaymasterError::NumericOverflow {
                context: "EIP-7702 authorization nonce",
            })?,
    };
    let signature = sponsor_signer
        .sign_hash_sync(&authorization.signature_hash())
        .map_err(|error| PaymasterError::Actor {
            message: format!("failed to sign EIP-7702 authorization: {error}"),
        })?;
    let signed_authorization = authorization.into_signed(signature);
    let base_tx = TransactionRequest::default()
        .with_from(sponsor_address)
        .with_to(sponsor_address)
        .with_authorization_list(vec![signed_authorization]);

    // Set explicit gas + fee fields. The previous code submitted the
    // delegation tx without setting gas_limit / max_fee_per_gas /
    // max_priority_fee_per_gas, relying on the provider filler. On
    // non-Pectra-aware RPCs the auto-fill undercounts the
    // PER_AUTH_BASE_COST (12,500) and PER_EMPTY_ACCOUNT_COST (25,000)
    // surcharges and the tx underprices. Mirrors the steady-state path
    // in `batched_tx.rs::send_batched_tx`.
    let gas_limit = retry_evm_rpc(metric_label, "estimate_gas", || async {
        wallet_provider.estimate_gas(base_tx.clone()).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let fee_estimate = retry_evm_rpc(metric_label, "estimate_eip1559_fees", || async {
        wallet_provider.estimate_eip1559_fees().await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
    let tx = base_tx
        .with_gas_limit(gas_limit)
        .with_max_fee_per_gas(max_fee_per_gas)
        .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

    let pending_tx = wallet_provider
        .send_transaction(tx)
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
    let tx_hash = *pending_tx.tx_hash();
    wait_for_evm_receipt(wallet_provider, metric_label, tx_hash).await?;

    info!(
        chain = metric_label,
        sponsor = %sponsor_address,
        tx_hash = %tx_hash,
        "EVM paymaster sponsor delegated to EIP-7702 delegator"
    );
    Ok(())
}

/// Revoke an EIP-7702 delegation by signing an authorization for the zero
/// address. After the tx mines, the sponsor's code goes back to empty
/// (it's no longer a delegated EOA). Use this to cleanly rotate sponsors
/// or to wind down a paymaster.
///
/// No-op if the sponsor is already undelegated.
#[allow(dead_code)] // operator-facing API; no in-crate caller.
pub async fn revoke_eip7702_delegation(
    provider: &DynProvider,
    sponsor_signer: &PrivateKeySigner,
    metric_label: &'static str,
) -> Result<()> {
    revoke_eip7702_delegation_with_wallet(provider, provider, sponsor_signer, metric_label).await
}

#[allow(dead_code)]
pub(crate) async fn revoke_eip7702_delegation_with_wallet(
    provider: &DynProvider,
    wallet_provider: &DynProvider,
    sponsor_signer: &PrivateKeySigner,
    metric_label: &'static str,
) -> Result<()> {
    let sponsor_address = sponsor_signer.address();
    let code = retry_evm_rpc(metric_label, "get_code_at", || async {
        provider.get_code_at(sponsor_address).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    if code.is_empty() {
        debug!(
            chain = metric_label,
            sponsor = %sponsor_address,
            "EVM paymaster sponsor is not delegated; nothing to revoke"
        );
        return Ok(());
    }

    let nonce = retry_evm_rpc(metric_label, "get_transaction_count", || async {
        provider.get_transaction_count(sponsor_address).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let chain_id = retry_evm_rpc(metric_label, "get_chain_id", || async {
        provider.get_chain_id().await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;

    // Revocation = authorization for the zero address. After the tx mines,
    // EXTCODE on the sponsor returns 0 bytes again.
    let authorization = Authorization {
        chain_id: U256::from(chain_id),
        address: Address::ZERO,
        nonce: nonce
            .checked_add(1)
            .ok_or(PaymasterError::NumericOverflow {
                context: "EIP-7702 revoke authorization nonce",
            })?,
    };
    let signature = sponsor_signer
        .sign_hash_sync(&authorization.signature_hash())
        .map_err(|error| PaymasterError::Actor {
            message: format!("failed to sign EIP-7702 revoke authorization: {error}"),
        })?;
    let signed_authorization = authorization.into_signed(signature);
    let base_tx = TransactionRequest::default()
        .with_from(sponsor_address)
        .with_to(sponsor_address)
        .with_authorization_list(vec![signed_authorization]);

    let gas_limit = retry_evm_rpc(metric_label, "estimate_gas", || async {
        wallet_provider.estimate_gas(base_tx.clone()).await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let fee_estimate = retry_evm_rpc(metric_label, "estimate_eip1559_fees", || async {
        wallet_provider.estimate_eip1559_fees().await
    })
    .await
    .map_err(|source| PaymasterError::EVMRpcError {
        source,
        loc: snafu::location!(),
    })?;
    let max_fee_per_gas = max_fee_per_gas_with_headroom(fee_estimate.max_fee_per_gas)?;
    let tx = base_tx
        .with_gas_limit(gas_limit)
        .with_max_fee_per_gas(max_fee_per_gas)
        .with_max_priority_fee_per_gas(fee_estimate.max_priority_fee_per_gas);

    let pending_tx = wallet_provider
        .send_transaction(tx)
        .await
        .map_err(|source| PaymasterError::EVMRpcError {
            source,
            loc: snafu::location!(),
        })?;
    let tx_hash = *pending_tx.tx_hash();
    wait_for_evm_receipt(wallet_provider, metric_label, tx_hash).await?;

    info!(
        chain = metric_label,
        sponsor = %sponsor_address,
        tx_hash = %tx_hash,
        "EVM paymaster sponsor EIP-7702 delegation revoked"
    );
    Ok(())
}
