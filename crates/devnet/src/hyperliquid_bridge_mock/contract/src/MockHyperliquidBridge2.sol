// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

interface IERC20PermitMinimal {
    function permit(
        address owner,
        address spender,
        uint256 value,
        uint256 deadline,
        uint8 v,
        bytes32 r,
        bytes32 s
    ) external;

    function transfer(address recipient, uint256 amount) external returns (bool);
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
}

/// @notice Minimal Bridge2-compatible mock for devnet/testing.
/// Real Hyperliquid deposits are usually plain ERC20 transfers to the bridge
/// address, so this contract mostly serves as the receiving address plus the
/// `batchedDepositWithPermit` ABI. The mock server watches ERC20 `Transfer`
/// logs into this contract to credit Hyperliquid balances. Permit-based
/// deposits also emit `Deposit` directly from the bridge.
contract MockHyperliquidBridge2 {
    IERC20PermitMinimal public immutable usdcToken;

    struct Signature {
        uint8 v;
        uint256 r;
        uint256 s;
    }

    struct DepositWithPermit {
        address user;
        uint64 usd;
        uint64 deadline;
        Signature signature;
    }

    event Deposit(address indexed user, uint64 usd);
    event FailedPermitDeposit(address user, uint64 usd, uint32 errorCode);
    event Withdrawal(address indexed user, uint64 usd);

    constructor(address usdcAddress) {
        usdcToken = IERC20PermitMinimal(usdcAddress);
    }

    function batchedDepositWithPermit(DepositWithPermit[] calldata deposits) external {
        for (uint256 i = 0; i < deposits.length; i++) {
            DepositWithPermit calldata deposit = deposits[i];
            try usdcToken.permit(
                deposit.user,
                address(this),
                deposit.usd,
                deposit.deadline,
                deposit.signature.v,
                bytes32(deposit.signature.r),
                bytes32(deposit.signature.s)
            ) {
                bool transferred = usdcToken.transferFrom(deposit.user, address(this), deposit.usd);
                if (transferred) {
                    emit Deposit(deposit.user, deposit.usd);
                } else {
                    emit FailedPermitDeposit(deposit.user, deposit.usd, 1);
                }
            } catch {
                emit FailedPermitDeposit(deposit.user, deposit.usd, 0);
            }
        }
    }

    /// @notice Mock-only withdrawal release path. Real Hyperliquid Bridge2
    /// payouts are validator-driven, but devnet needs an on-chain effect the
    /// mock server can trigger after a `withdraw3` request is accepted.
    function release(address user, uint64 usd) external {
        require(usdcToken.transfer(user, usd), "transfer failed");
        emit Withdrawal(user, usd);
    }
}
