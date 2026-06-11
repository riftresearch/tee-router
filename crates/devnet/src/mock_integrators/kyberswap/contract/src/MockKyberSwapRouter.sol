// SPDX-License-Identifier: MIT
pragma solidity ^0.8.23;

interface IERC20TransferFrom {
    function transferFrom(address owner, address recipient, uint256 amount) external returns (bool);
}

interface IMintableERC20 {
    function mint(address recipient, uint256 amount) external;
}

/// Mock of KyberSwap's `MetaAggregationRouterV2` aggregation router (the
/// production router the aggregator API's `route/build` endpoint targets, e.g.
/// `0x6131B5fae19EA4f9D964eAc0408E4408b66337b5` on most chains).
///
/// **Shape matches the real router's `swap` ABI exactly** — the
/// `SwapExecutionParams` / `SwapDescriptionV2` tuples are byte-identical to the
/// on-chain interface so the `data` blob produced by the mock aggregator API
/// decodes against the real ABI. The body is a *simulated* fill, not the real
/// executor-driven pool routing:
///
/// - pulls `desc.amount` of `desc.srcToken` from `msg.sender` (or native ETH
///   from `msg.value` when `srcToken` is the KyberSwap native sentinel),
/// - credits `desc.minReturnAmount` of `desc.dstToken` to `desc.dstReceiver`
///   (mints the mock ERC-20, or sends native ETH),
/// - returns `(returnAmount, gasUsed)`.
///
/// Like the real router, **no swap-specific event is emitted** — the only
/// on-chain settlement evidence is the ERC-20 `Transfer(_, dstReceiver, amount)`
/// log from the output token, which is what the settlement detector verifies.
contract MockKyberSwapRouter {
    /// KyberSwap's native-asset sentinel (`ETH`/native) used in the aggregator
    /// API and router calldata.
    address internal constant NATIVE = 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE;

    /// `SwapDescriptionV2` — bytes-identical to the real
    /// `MetaAggregationRouterV2` tuple.
    struct SwapDescriptionV2 {
        address srcToken;
        address dstToken;
        address[] srcReceivers;
        uint256[] srcAmounts;
        address[] feeReceivers;
        uint256[] feeAmounts;
        address dstReceiver;
        uint256 amount;
        uint256 minReturnAmount;
        uint256 flags;
        bytes permit;
    }

    /// `SwapExecutionParams` — bytes-identical to the real
    /// `MetaAggregationRouterV2` tuple.
    struct SwapExecutionParams {
        address callTarget;
        address approveTarget;
        bytes targetData;
        SwapDescriptionV2 desc;
        bytes clientData;
    }

    receive() external payable {}

    /// Mirror of `MetaAggregationRouterV2.swap`. `callTarget`, `approveTarget`,
    /// `targetData`, and `clientData` are accepted to match the real call
    /// signature but ignored by the mock body.
    function swap(SwapExecutionParams calldata execution)
        external
        payable
        returns (uint256 returnAmount, uint256 gasUsed)
    {
        SwapDescriptionV2 calldata desc = execution.desc;

        if (desc.srcToken == NATIVE) {
            require(msg.value == desc.amount, "native input mismatch");
        } else {
            require(msg.value == 0, "unexpected native value");
            require(
                IERC20TransferFrom(desc.srcToken).transferFrom(
                    msg.sender, address(this), desc.amount
                ),
                "transferFrom failed"
            );
        }

        if (desc.dstToken == NATIVE) {
            (bool sent, ) = payable(desc.dstReceiver).call{value: desc.minReturnAmount}("");
            require(sent, "native output failed");
        } else {
            IMintableERC20(desc.dstToken).mint(desc.dstReceiver, desc.minReturnAmount);
        }

        // The mock fills exactly at the requested minimum return (no slippage,
        // no fees), so returnAmount == desc.minReturnAmount.
        return (desc.minReturnAmount, 0);
    }
}
