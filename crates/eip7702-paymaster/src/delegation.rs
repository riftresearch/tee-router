use crate::batched_tx::{retry_evm_rpc, wait_for_evm_receipt};
use crate::error::{PaymasterError, Result};
use alloy::hex;
use alloy::network::{TransactionBuilder, TransactionBuilder7702};
use alloy::primitives::{Address, U256};
use alloy::providers::{DynProvider, Provider};
use alloy::rpc::types::{Authorization, TransactionRequest};
use alloy::signers::{local::PrivateKeySigner, SignerSync};
use eip7702_delegator_contract::EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS;
use std::str::FromStr;
use tracing::{debug, info};

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
    let tx = TransactionRequest::default()
        .with_from(sponsor_address)
        .with_to(sponsor_address)
        .with_authorization_list(vec![signed_authorization]);

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
