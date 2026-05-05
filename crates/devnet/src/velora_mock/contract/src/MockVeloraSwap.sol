// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

contract MockVeloraSwap {
    event Swap(
        address indexed sender,
        address indexed recipient,
        address srcToken,
        address destToken,
        uint256 srcAmount,
        uint256 destAmount
    );

    receive() external payable {}

    function swap(
        address srcToken,
        address destToken,
        address recipient,
        uint256 srcAmount,
        uint256 destAmount
    ) external payable {
        if (srcToken == address(0)) {
            require(msg.value == srcAmount, "native input mismatch");
        } else {
            require(msg.value == 0, "unexpected native value");
            require(
                IERC20TransferFrom(srcToken).transferFrom(msg.sender, address(this), srcAmount),
                "transferFrom failed"
            );
        }

        if (destToken == address(0)) {
            (bool sent, ) = recipient.call{value: destAmount}("");
            require(sent, "native output failed");
        } else {
            IMintableERC20(destToken).mint(recipient, destAmount);
        }

        emit Swap(msg.sender, recipient, srcToken, destToken, srcAmount, destAmount);
    }
}
