// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

contract MockCctpTokenMessengerV2 {
    uint64 public nextNonce;

    event DepositForBurn(
        uint64 indexed nonce,
        address indexed burnToken,
        address indexed depositor,
        uint256 amount,
        uint32 destinationDomain,
        bytes32 mintRecipient,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 minFinalityThreshold
    );

    function depositForBurn(
        uint256 amount,
        uint32 destinationDomain,
        bytes32 mintRecipient,
        address burnToken,
        bytes32 destinationCaller,
        uint256 maxFee,
        uint32 minFinalityThreshold
    ) external returns (uint64 nonce) {
        require(
            IERC20TransferFrom(burnToken).transferFrom(msg.sender, address(this), amount),
            "transferFrom failed"
        );
        nonce = nextNonce++;
        emit DepositForBurn(
            nonce,
            burnToken,
            msg.sender,
            amount,
            destinationDomain,
            mintRecipient,
            destinationCaller,
            maxFee,
            minFinalityThreshold
        );
    }
}
