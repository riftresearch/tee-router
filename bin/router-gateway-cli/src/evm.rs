//! EVM source-chain deposit: sign and broadcast native or ERC-20 transfers
//! to the order's deposit address, via alloy.

use alloy::network::{EthereumWallet, TransactionBuilder};
use alloy::primitives::{Address, Bytes, U256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::SolCall;
use eyre::{eyre, Result, WrapErr};

use crate::assets::EvmToken;

sol! {
    function transfer(address to, uint256 amount) external returns (bool);
}

/// Parse an EVM private key (hex, with or without `0x`) into a signer.
fn signer(private_key: &str) -> Result<PrivateKeySigner> {
    private_key
        .trim()
        .parse::<PrivateKeySigner>()
        .map_err(|err| eyre!("invalid EVM private key: {err}"))
}

/// The 0x-prefixed address an EVM private key controls.
pub fn address(private_key: &str) -> Result<String> {
    Ok(signer(private_key)?.address().to_string())
}

/// Sign and broadcast the deposit transaction; returns its hash.
///
/// Broadcast only — does not wait for inclusion.
pub async fn send_deposit(
    rpc_url: &str,
    private_key: &str,
    token: EvmToken,
    expected_chain_id: u64,
    order_address: &str,
    amount_raw: &str,
) -> Result<String> {
    let signer = signer(private_key)?;
    let from = signer.address();
    let url: reqwest::Url = rpc_url
        .parse()
        .wrap_err_with(|| format!("invalid --rpc-url `{rpc_url}`"))?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(url);

    let chain_id = provider
        .get_chain_id()
        .await
        .wrap_err("could not read the chain id from the EVM RPC")?;
    if chain_id != expected_chain_id {
        return Err(eyre!(
            "RPC chain id {chain_id} does not match the source asset's chain {expected_chain_id} \
             — pass an --rpc-url for the correct chain"
        ));
    }

    let order: Address = order_address.parse().wrap_err_with(|| {
        format!("order deposit address `{order_address}` is not a valid EVM address")
    })?;
    let amount = U256::from_str_radix(amount_raw, 10)
        .map_err(|err| eyre!("invalid raw amount `{amount_raw}`: {err}"))?;

    let request = match token {
        EvmToken::Native => TransactionRequest::default()
            .with_from(from)
            .with_to(order)
            .with_value(amount),
        EvmToken::Erc20 { address } => {
            let contract: Address = address
                .parse()
                .wrap_err("built-in ERC-20 contract address is invalid")?;
            let calldata = transferCall { to: order, amount }.abi_encode();
            TransactionRequest::default()
                .with_from(from)
                .with_to(contract)
                .with_input(Bytes::from(calldata))
        }
    };

    let pending = provider
        .send_transaction(request)
        .await
        .wrap_err("failed to broadcast the EVM deposit transaction")?;
    Ok(pending.tx_hash().to_string())
}
