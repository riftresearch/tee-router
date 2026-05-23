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
///
/// Event and struct shapes mirror Hyperliquid's real `Bridge2.sol` (at
/// `0x2df1c51e09aecf9cacb7bc98cb1742757f163df7` mainnet) byte-for-byte so
/// observers exercise the same ABI they see on mainnet:
///   - `Signature { uint256 r; uint256 s; uint8 v; }`  (r, s, v field order)
///   - `FailedPermitDeposit(address user, uint64 usd, uint32 errorCode)`
///   - `RequestedWithdrawal(address indexed user, address destination,
///       uint64 usd, uint64 nonce, bytes32 message, uint64 requestedTime)`
///   - `FinalizedWithdrawal(address indexed user, address destination,
///       uint64 usd, uint64 nonce, bytes32 message)`
///
/// Note: real `Bridge2` declares `Deposit(address indexed user, uint64 usd)`
/// but **never emits it** — successful deposits are silent at the Bridge2
/// layer; observers credit balances from the USDC `Transfer` log into the
/// bridge address. The mock previously emitted `Deposit` on permit success
/// (a fabrication); now it matches reality and emits nothing on success.
contract MockHyperliquidBridge2 {
    IERC20PermitMinimal public immutable usdcToken;

    /// Field order matches `Bridge2`'s `Signature.sol` exactly: (r, s, v).
    /// The previous mock had (v, r, s) which would deserialize wrong against
    /// any code that consumed the real ABI.
    struct Signature {
        uint256 r;
        uint256 s;
        uint8 v;
    }

    struct DepositWithPermit {
        address user;
        uint64 usd;
        uint64 deadline;
        Signature signature;
    }

    event FailedPermitDeposit(address user, uint64 usd, uint32 errorCode);
    event RequestedWithdrawal(
        address indexed user,
        address destination,
        uint64 usd,
        uint64 nonce,
        bytes32 message,
        uint64 requestedTime
    );
    event FinalizedWithdrawal(
        address indexed user,
        address destination,
        uint64 usd,
        uint64 nonce,
        bytes32 message
    );

    /// Mock-only counter to give each withdrawal a unique synthetic nonce.
    uint64 private nextWithdrawalNonce;

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
                if (!transferred) {
                    emit FailedPermitDeposit(deposit.user, deposit.usd, 1);
                }
                // Real Bridge2 emits nothing on success — observers credit
                // balances from the USDC Transfer log. Stay silent here too.
            } catch {
                emit FailedPermitDeposit(deposit.user, deposit.usd, 0);
            }
        }
    }

    /// Mock-only withdrawal payout path. Real Hyperliquid Bridge2 payouts
    /// are validator-driven: `batchedRequestWithdrawals` emits
    /// `RequestedWithdrawal`, then after a dispute period validators call
    /// `finalizeWithdrawals` which emits `FinalizedWithdrawal`. The devnet
    /// mock compresses both steps into one call so the mock server has a
    /// single trigger for the on-chain effect, but emits BOTH real events
    /// so observers see the same event sequence they'd see on mainnet.
    function release(address user, uint64 usd) external {
        require(usdcToken.transfer(user, usd), "transfer failed");
        uint64 nonce = ++nextWithdrawalNonce;
        bytes32 message = keccak256(abi.encodePacked(user, usd, nonce));
        emit RequestedWithdrawal(user, user, usd, nonce, message, uint64(block.timestamp));
        emit FinalizedWithdrawal(user, user, usd, nonce, message);
    }
}
