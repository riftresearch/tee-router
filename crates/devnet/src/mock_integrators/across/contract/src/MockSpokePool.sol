// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

/// Mock of Across Protocol's SpokePool contract.
///
/// Event signature matches the real `FundsDeposited` event on
/// across-protocol/contracts SpokePool.sol byte-for-byte (indexed
/// markers, field order, types).
///
/// The `deposit` function is `payable` and pulls ERC-20 tokens via
/// `transferFrom` — matching real SpokePool behavior. This way the
/// mock exercises the same approval + native-ETH-value paths that
/// production deposits go through, so a missing ERC-20 approve or a
/// mismatched native-value reverts in devnet the same way it would
/// on mainnet.
contract MockSpokePool {
    uint256 public nextDepositId;

    event FundsDeposited(
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 indexed destinationChainId,
        uint256 indexed depositId,
        uint32 quoteTimestamp,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes32 indexed depositor,
        bytes32 recipient,
        bytes32 exclusiveRelayer,
        bytes message
    );

    function deposit(
        bytes32 depositor,
        bytes32 recipient,
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 destinationChainId,
        bytes32 exclusiveRelayer,
        uint32 quoteTimestamp,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes calldata message
    ) external payable {
        // Native ETH deposit: inputToken=bytes32(0), msg.value must equal inputAmount.
        // ERC-20 deposit: msg.value must be 0; the pool pulls the token via transferFrom.
        if (inputToken == bytes32(0)) {
            require(msg.value == inputAmount, "native input mismatch");
        } else {
            require(msg.value == 0, "unexpected native value");
            // bytes32 left-pads the 20-byte EVM address.
            address tokenAddr = address(uint160(uint256(inputToken)));
            require(
                IERC20TransferFrom(tokenAddr).transferFrom(msg.sender, address(this), inputAmount),
                "transferFrom failed"
            );
        }

        uint256 depositId = nextDepositId++;
        emit FundsDeposited(
            inputToken,
            outputToken,
            inputAmount,
            outputAmount,
            destinationChainId,
            depositId,
            quoteTimestamp,
            fillDeadline,
            exclusivityDeadline,
            depositor,
            recipient,
            exclusiveRelayer,
            message
        );
    }
}
