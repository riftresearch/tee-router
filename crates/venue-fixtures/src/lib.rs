//! Captured real-world responses, events, and transactions from the venues
//! we integrate with (CCTP, Velora, Hyperliquid, Hyperunit, Across, Bitcoin).
//!
//! Each fixture is a byte-for-byte payload pulled from a production endpoint
//! and pinned here. Tests deserialize them via the consumer crate's own types
//! and assert the expected fields — this is the "tier 1" leg of the
//! differential-test harness (no network, no funds, runs every commit).
//!
//! When a venue rotates an API shape or renames a contract event upstream,
//! a fixture-replay test will fail at PR time instead of silently no-op'ing
//! in production. The companion nightly drift-detection job can refresh these
//! fixtures from their original sources.
//!
//! Adding a new fixture: drop the captured JSON/bytes file under
//! `fixtures/<venue>/` and expose it here via `include_str!`.

/// Circle CCTP V2 fixtures.
pub mod cctp {
    /// Real Iris V2 `/v2/messages/{sourceDomain}?transactionHash=...` response
    /// captured from production on 2026-05-22 for the 40-USDC refund burn
    /// (source domain 3 = Arbitrum, dest domain 6 = Base, burn tx
    /// `0x2d77d9667cc326816cd0ddc736a2222829e2087c7073362f16e0043141e43c96`).
    ///
    /// Pins the on-the-wire shape including the `decodedMessage` nesting
    /// (the bug that PR #3 fixed) and the V2-only `delayReason` field.
    pub const IRIS_V2_COMPLETE_REAL: &str =
        include_str!("../fixtures/cctp/iris_v2_complete_real.json");
}
