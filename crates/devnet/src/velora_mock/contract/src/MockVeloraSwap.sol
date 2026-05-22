// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

/// Mock of Velora's (formerly ParaSwap) Augustus swap router.
///
/// The settlement event mirrors the real Velora `SwappedV3` event byte-for-byte
/// so production observers (Sauron's EVM receipt observer) are exercised
/// against the same ABI they see on mainnet.
/// topic0 = keccak256("SwappedV3(bytes16,address,uint256,address,address,address,address,uint256,uint256,uint256)")
///        = 0xe00361d207b252a464323eb23d45d42583e391f2031acdd2e9fa36efddd43cb0
contract MockVeloraSwap {
    event SwappedV3(
        bytes16 uuid,
        address partner,
        uint256 feePercent,
        address initiator,
        address indexed beneficiary,
        address indexed srcToken,
        address indexed destToken,
        uint256 srcAmount,
        uint256 receivedAmount,
        uint256 expectedAmount
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

        // `receivedAmount` and `expectedAmount` are equal in the mock: the
        // swap fills exactly at the quoted output with no slippage.
        emit SwappedV3(
            bytes16(0),
            address(0),
            0,
            msg.sender,
            recipient,
            srcToken,
            destToken,
            srcAmount,
            destAmount,
            destAmount
        );
    }
}
