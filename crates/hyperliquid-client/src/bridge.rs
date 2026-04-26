use alloy::{
    network::TransactionBuilder,
    primitives::{address, Address, Bytes, U256},
    rpc::types::TransactionRequest,
    sol,
    sol_types::SolCall,
};

use crate::{client::Network, Error, Result};

sol! {
    interface IERC20BridgeToken {
        function transfer(address recipient, uint256 amount) external returns (bool);
    }
}

/// Hyperliquid's Arbitrum Bridge2 contract addresses from the official docs.
pub const MAINNET_BRIDGE2_ADDRESS: Address = address!("2df1c51e09aecf9cacb7bc98cb1742757f163df7");
pub const TESTNET_BRIDGE2_ADDRESS: Address = address!("08cfc1b6b2dcf36a1480b99353a354aa8ac56f89");

/// Hyperliquid documents a minimum native Arbitrum USDC deposit of 5 USDC.
pub const MINIMUM_BRIDGE_DEPOSIT_USDC: u64 = 5_000_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BridgeDepositTx {
    pub from: Address,
    pub usdc_token: Address,
    pub bridge: Address,
    pub amount: U256,
    pub tx: TransactionRequest,
}

pub fn bridge_address(network: Network) -> Address {
    match network {
        Network::Mainnet => MAINNET_BRIDGE2_ADDRESS,
        Network::Testnet => TESTNET_BRIDGE2_ADDRESS,
    }
}

pub fn minimum_bridge_deposit() -> U256 {
    U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC)
}

pub fn build_bridge_deposit_tx(
    network: Network,
    from: Address,
    usdc_token: Address,
    amount: U256,
) -> Result<BridgeDepositTx> {
    build_bridge_deposit_tx_to(from, usdc_token, bridge_address(network), amount)
}

pub fn build_bridge_deposit_tx_to(
    from: Address,
    usdc_token: Address,
    bridge: Address,
    amount: U256,
) -> Result<BridgeDepositTx> {
    let minimum = minimum_bridge_deposit();
    if amount < minimum {
        return Err(Error::InvalidBridgeDepositAmount {
            amount: amount.to_string(),
            minimum: minimum.to_string(),
        });
    }

    let calldata: Bytes = IERC20BridgeToken::transferCall {
        recipient: bridge,
        amount,
    }
    .abi_encode()
    .into();

    let tx = TransactionRequest::default()
        .with_from(from)
        .with_to(usdc_token)
        .with_input(calldata);

    Ok(BridgeDepositTx {
        from,
        usdc_token,
        bridge,
        amount,
        tx,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bridge_addresses_match_docs() {
        assert_eq!(
            bridge_address(Network::Mainnet),
            address!("2df1c51e09aecf9cacb7bc98cb1742757f163df7")
        );
        assert_eq!(
            bridge_address(Network::Testnet),
            address!("08cfc1b6b2dcf36a1480b99353a354aa8ac56f89")
        );
    }

    #[test]
    fn build_bridge_deposit_rejects_subminimum_amounts() {
        let err = build_bridge_deposit_tx(
            Network::Mainnet,
            address!("1111111111111111111111111111111111111111"),
            address!("2222222222222222222222222222222222222222"),
            U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC - 1),
        )
        .unwrap_err();

        assert!(matches!(err, Error::InvalidBridgeDepositAmount { .. }));
    }

    #[test]
    fn build_bridge_deposit_targets_usdc_transfer_to_bridge() {
        let from = address!("1111111111111111111111111111111111111111");
        let usdc = address!("2222222222222222222222222222222222222222");
        let amount = U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC);

        let deposit =
            build_bridge_deposit_tx(Network::Testnet, from, usdc, amount).expect("deposit tx");

        assert_eq!(deposit.from, from);
        assert_eq!(deposit.usdc_token, usdc);
        assert_eq!(
            deposit.bridge,
            address!("08cfc1b6b2dcf36a1480b99353a354aa8ac56f89")
        );
        assert_eq!(deposit.amount, amount);
        assert_eq!(deposit.tx.from, Some(from));
        assert_eq!(deposit.tx.to, Some(usdc.into()));

        let input = deposit.tx.input.input().expect("input bytes");
        let decoded = IERC20BridgeToken::transferCall::abi_decode(input).expect("decode transfer");
        assert_eq!(decoded.recipient, deposit.bridge);
        assert_eq!(decoded.amount, amount);
    }

    #[test]
    fn build_bridge_deposit_can_target_override_bridge_address() {
        let from = address!("1111111111111111111111111111111111111111");
        let usdc = address!("2222222222222222222222222222222222222222");
        let bridge = address!("3333333333333333333333333333333333333333");
        let amount = U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC);

        let deposit =
            build_bridge_deposit_tx_to(from, usdc, bridge, amount).expect("deposit override");

        assert_eq!(deposit.bridge, bridge);
        let input = deposit.tx.input.input().expect("input bytes");
        let decoded = IERC20BridgeToken::transferCall::abi_decode(input).expect("decode transfer");
        assert_eq!(decoded.recipient, bridge);
        assert_eq!(decoded.amount, amount);
    }
}
