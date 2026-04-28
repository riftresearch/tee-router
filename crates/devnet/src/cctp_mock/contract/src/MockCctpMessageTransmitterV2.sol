// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

contract MockCctpMessageTransmitterV2 {
    event MessageReceived(address indexed token, address indexed recipient, uint256 amount);

    function receiveMessage(
        bytes calldata message,
        bytes calldata attestation
    ) external returns (bool) {
        require(attestation.length > 0, "missing attestation");
        (address token, address recipient, uint256 amount) = abi.decode(
            message,
            (address, address, uint256)
        );
        IMintableERC20(token).mint(recipient, amount);
        emit MessageReceived(token, recipient, amount);
        return true;
    }
}
