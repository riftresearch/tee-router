// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

/// Mock of Circle's CCTP v2 `TokenMessengerV2`.
///
/// Event shapes mirror the real CCTP v2 contracts byte-for-byte so production
/// observers (Sauron's EVM receipt observer) are exercised against the same
/// ABI they see on mainnet. See `circlefin/evm-cctp-contracts` `src/v2`.
contract MockCctpTokenMessengerV2 {
    /// Deterministic devnet address of the mock message transmitter. Kept in
    /// sync with `MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS` in `evm_devnet.rs`.
    address internal constant LOCAL_MESSAGE_TRANSMITTER =
        0xcCCcCCCcccCcCCCcCccCCcCcCCcCCcccCccC0002;

    /// Real CCTP v2 `DepositForBurn`, defined on `TokenMessengerV2` and emitted
    /// by `_depositForBurn`. topic0 =
    /// keccak256("DepositForBurn(address,uint256,address,bytes32,uint32,bytes32,bytes32,uint256,uint32,bytes)")
    event DepositForBurn(
        address indexed burnToken,
        uint256 amount,
        address indexed depositor,
        bytes32 mintRecipient,
        uint32 destinationDomain,
        bytes32 destinationTokenMessenger,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 indexed minFinalityThreshold,
        bytes hookData
    );

    /// Real CCTP v2 `MintAndWithdraw`, defined on `BaseTokenMessenger` and
    /// emitted by `_mintAndWithdraw` when the local minter mints bridged USDC.
    /// topic0 = keccak256("MintAndWithdraw(address,uint256,address,uint256)")
    ///        = 0x50c55e915134d457debfa58eb6f4342956f8b0616d51a89a3659360178e1ab63
    event MintAndWithdraw(
        address indexed mintRecipient,
        uint256 amount,
        address indexed mintToken,
        uint256 feeCollected
    );

    function depositForBurn(
        uint256 amount,
        uint32 destinationDomain,
        bytes32 mintRecipient,
        address burnToken,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 minFinalityThreshold
    ) external {
        require(
            IERC20TransferFrom(burnToken).transferFrom(msg.sender, address(this), amount),
            "transferFrom failed"
        );
        emit DepositForBurn(
            burnToken,
            amount,
            msg.sender,
            mintRecipient,
            destinationDomain,
            bytes32(0),
            destinationCaller,
            maxFee,
            minFinalityThreshold,
            bytes("")
        );
    }

    /// Mints the bridged token and emits the real `MintAndWithdraw` event.
    ///
    /// In real CCTP v2 the message transmitter dispatches a received message
    /// into the local token messenger, which mints via `_mintAndWithdraw` and
    /// emits `MintAndWithdraw` from the token messenger address. The mock
    /// keeps that topology so the event's emitter is the token messenger.
    function handleReceiveFinalizedMessage(
        address mintToken,
        address mintRecipient,
        uint256 amount
    ) external {
        require(msg.sender == LOCAL_MESSAGE_TRANSMITTER, "unauthorized caller");
        IMintableERC20(mintToken).mint(mintRecipient, amount);
        emit MintAndWithdraw(mintRecipient, amount, mintToken, 0);
    }
}
