use std::time::Duration;

use metrics::{counter, gauge, histogram};

pub fn record_pending_count(chain: &str, count: usize) {
    gauge!("evm_receipt_watcher_pending_watches", "chain" => chain.to_string()).set(count as f64);
}

pub fn record_watch_registered(chain: &str, outcome: &'static str) {
    counter!(
        "evm_receipt_watcher_watch_registrations_total",
        "chain" => chain.to_string(),
        "outcome" => outcome,
    )
    .increment(1);
}

pub fn record_watch_cancelled(chain: &str, found: bool) {
    counter!(
        "evm_receipt_watcher_watch_cancellations_total",
        "chain" => chain.to_string(),
        "found" => found.to_string(),
    )
    .increment(1);
}

pub fn record_block_scanned(
    chain: &str,
    tx_count: usize,
    matched_count: usize,
    duration: Duration,
) {
    counter!(
        "evm_receipt_watcher_blocks_scanned_total",
        "chain" => chain.to_string(),
    )
    .increment(1);
    histogram!(
        "evm_receipt_watcher_block_scan_duration_seconds",
        "chain" => chain.to_string(),
    )
    .record(duration.as_secs_f64());
    histogram!(
        "evm_receipt_watcher_block_transactions",
        "chain" => chain.to_string(),
    )
    .record(tx_count as f64);
    histogram!(
        "evm_receipt_watcher_block_matches",
        "chain" => chain.to_string(),
    )
    .record(matched_count as f64);
}

pub fn record_receipt(chain: &str, status: &'static str) {
    counter!(
        "evm_receipt_watcher_receipts_total",
        "chain" => chain.to_string(),
        "status" => status,
    )
    .increment(1);
}

pub fn record_rpc(chain: &str, method: &'static str, status: &'static str, duration: Duration) {
    counter!(
        "evm_receipt_watcher_rpc_requests_total",
        "chain" => chain.to_string(),
        "method" => method,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "evm_receipt_watcher_rpc_request_duration_seconds",
        "chain" => chain.to_string(),
        "method" => method,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

pub fn record_mode(chain: &str, degraded: bool) {
    gauge!("evm_receipt_watcher_degraded", "chain" => chain.to_string()).set(if degraded {
        1.0
    } else {
        0.0
    });
}

pub fn record_active_subscriptions(count: usize) {
    gauge!("evm_receipt_watcher_active_subscriptions").set(count as f64);
}
