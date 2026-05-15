use alloy::primitives::U256;
use metrics::{counter, gauge, histogram};

const WEI_PER_GWEI: f64 = 1_000_000_000.0;

pub(crate) fn record_paymaster_gas_estimate(
    metric_label: &'static str,
    operation: &'static str,
    gas_units: u64,
) {
    histogram!(
        "tee_router_paymaster_estimated_gas_units",
        "chain" => metric_label,
        "operation" => operation,
    )
    .record(gas_units as f64);
}

pub(crate) fn record_paymaster_balance_gwei(
    metric_label: &'static str,
    wallet_kind: &'static str,
    balance: U256,
) {
    gauge!(
        "tee_router_paymaster_balance_gwei",
        "chain" => metric_label,
        "wallet_kind" => wallet_kind,
    )
    .set(u256_to_gwei_f64(balance));
}

pub(crate) fn record_paymaster_funding_skipped(metric_label: &'static str, reason: &'static str) {
    counter!(
        "tee_router_paymaster_funding_skipped_total",
        "chain" => metric_label,
        "reason" => reason,
    )
    .increment(1);
}

pub(crate) fn record_paymaster_funding_failed(
    metric_label: &'static str,
    failure_kind: &'static str,
) {
    counter!(
        "tee_router_paymaster_funding_failed_total",
        "chain" => metric_label,
        "failure_kind" => failure_kind,
    )
    .increment(1);
}

pub(crate) fn record_paymaster_funding_sent(metric_label: &'static str, top_up_amount: U256) {
    counter!(
        "tee_router_paymaster_funding_tx_sent_total",
        "chain" => metric_label,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_funding_amount_gwei",
        "chain" => metric_label,
    )
    .record(u256_to_gwei_f64(top_up_amount));
}

pub(crate) fn record_paymaster_batch_sent(metric_label: &'static str, execution_count: usize) {
    counter!(
        "tee_router_paymaster_batch_sent_total",
        "chain" => metric_label,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_batch_size",
        "chain" => metric_label,
        "stage" => "sent",
    )
    .record(execution_count as f64);
}

pub(crate) fn record_paymaster_batch_executed(
    metric_label: &'static str,
    execution_count: usize,
    success: bool,
) {
    let status = success_status(success);
    counter!(
        "tee_router_paymaster_batch_executed_total",
        "chain" => metric_label,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_paymaster_batch_size",
        "chain" => metric_label,
        "stage" => "executed",
        "status" => status,
    )
    .record(execution_count as f64);
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}

fn u256_to_gwei_f64(value: U256) -> f64 {
    value.to_string().parse::<f64>().unwrap_or(f64::MAX) / WEI_PER_GWEI
}
