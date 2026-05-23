// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

/// Mock of Velora's AugustusV6 swap router (the V6 diamond at
/// `0x6a000f20005980200259b80c5102003040001068` on Ethereum, Base, Arbitrum).
///
/// **Shape matches the real V6 contract exactly:**
/// - `swapExactAmountIn` takes the same struct layout as on-chain.
/// - **No swap events are emitted.** Real `AugustusV6` only emits
///   `DiamondCut` and `OwnershipTransferred` — no `SwappedV3`,
///   `SwappedDirect`, `Bought`, etc. The only on-chain evidence of a
///   settled swap is the ERC-20 `Transfer` events from the involved
///   token contracts themselves.
///
/// The legacy V5 `SwappedV3(...)` event we previously emitted was a
/// fabrication — pinned by the V5 Augustus router but **not** by V6.
/// Our production code routes everything through `version=6.2` and
/// therefore never sees that event on mainnet/L2s.
contract MockVeloraSwap {
    /// V6 swap data tuple — bytes-identical to the real `AugustusV6`
    /// `swapExactAmountIn` 2nd parameter.
    struct SwapData {
        address srcToken;
        address destToken;
        uint256 fromAmount;
        uint256 toAmount;
        uint256 quotedAmount;
        bytes32 metadata;
        address payable beneficiary;
    }

    receive() external payable {}

    /// Mirror of `AugustusV6.swapExactAmountIn`. Pulls `swapData.fromAmount`
    /// of `swapData.srcToken` from `msg.sender` (or native ETH from `msg.value`)
    /// and credits `swapData.toAmount` of `swapData.destToken` to
    /// `swapData.beneficiary`. Returns `(receivedAmount, 0, 0)` — paraswap
    /// share and partner share are always zero in the mock.
    ///
    /// `executor`, `partnerAndFee`, `permit`, and `executorData` are accepted
    /// to match the real call signature but ignored by the mock body.
    function swapExactAmountIn(
        address /* executor */,
        SwapData calldata swapData,
        uint256 /* partnerAndFee */,
        bytes calldata /* permit */,
        bytes calldata /* executorData */
    ) external payable returns (uint256 receivedAmount, uint256 paraswapShare, uint256 partnerShare) {
        if (swapData.srcToken == address(0)) {
            require(msg.value == swapData.fromAmount, "native input mismatch");
        } else {
            require(msg.value == 0, "unexpected native value");
            require(
                IERC20TransferFrom(swapData.srcToken).transferFrom(
                    msg.sender, address(this), swapData.fromAmount
                ),
                "transferFrom failed"
            );
        }

        if (swapData.destToken == address(0)) {
            (bool sent, ) = swapData.beneficiary.call{value: swapData.toAmount}("");
            require(sent, "native output failed");
        } else {
            IMintableERC20(swapData.destToken).mint(swapData.beneficiary, swapData.toAmount);
        }

        // Match the real V6 return shape. In the mock the swap always fills
        // at the quoted output with no slippage, so receivedAmount == toAmount.
        return (swapData.toAmount, 0, 0);
    }
}
