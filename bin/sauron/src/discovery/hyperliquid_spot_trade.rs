use crate::provider_operations::ProviderOperationWatchEntry;

/// Spot and perp HL trade operation rows share the same provider operation
/// shape: both persist an oid in `observed_state`, and the shim's fill feed
/// carries the coin/market detail. Classification is therefore performed from
/// the fill payload in `hyperliquid_trade`; this module exists as the explicit
/// venue boundary for future spot-only routing rules.
#[must_use]
pub fn is_spot_trade_candidate(operation: &ProviderOperationWatchEntry) -> bool {
    operation
        .request
        .get("coin")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|coin| coin.starts_with('U') || coin.contains('/'))
}
