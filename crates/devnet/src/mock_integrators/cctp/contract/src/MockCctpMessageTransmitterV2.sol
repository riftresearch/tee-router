// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IMockCctpTokenMessengerV2 {
    function handleReceiveFinalizedMessage(
        address mintToken,
        address mintRecipient,
        uint256 amount
    ) external;
}

/// Mock of Circle's CCTP v2 `MessageTransmitterV2`.
///
/// `receiveMessage` mirrors the real CCTP v2 topology: it does not mint or
/// emit a mint event itself. It dispatches the decoded message into the local
/// token messenger, which mints the bridged USDC and emits the real
/// `MintAndWithdraw` event from the token messenger address. This keeps the
/// observed event ABI byte-for-byte identical to mainnet CCTP v2.
contract MockCctpMessageTransmitterV2 {
    /// Deterministic devnet address of the mock token messenger. Kept in sync
    /// with `MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS` in `evm_devnet.rs`.
    address internal constant LOCAL_TOKEN_MESSENGER =
        0xccCCCcCCcCCcCcccccCccCccCCCCcCCCccCc0001;

    function receiveMessage(
        bytes calldata message,
        bytes calldata attestation
    ) external returns (bool) {
        require(attestation.length > 0, "missing attestation");
        (address token, address recipient, uint256 amount) = abi.decode(
            message,
            (address, address, uint256)
        );
        IMockCctpTokenMessengerV2(LOCAL_TOKEN_MESSENGER).handleReceiveFinalizedMessage(
            token,
            recipient,
            amount
        );
        return true;
    }
}
