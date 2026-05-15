use metrics::{counter, gauge};

pub fn record_hl_request(endpoint: &'static str, status: &'static str, weight: u32) {
    counter!(
        "hl_shim_hl_request_total",
        "endpoint" => endpoint,
        "status" => status,
    )
    .increment(1);
    counter!("hl_shim_hl_weight_used_total", "endpoint" => endpoint).increment(u64::from(weight));
}

pub fn record_db_write(kind: &'static str, inserted: bool) {
    let status = if inserted { "inserted" } else { "duplicate" };
    counter!("hl_shim_db_writes_total", "kind" => kind, "status" => status).increment(1);
}

pub fn record_active_subscriptions(count: usize) {
    gauge!("hl_shim_active_subscriptions").set(count as f64);
}

pub fn record_user_bucket(bucket: &'static str, count: usize) {
    gauge!("hl_shim_user_bucket", "bucket" => bucket).set(count as f64);
}

pub fn record_unsupported_ledger_delta(type_name: &str) {
    counter!(
        "hl_shim_unsupported_ledger_delta_total",
        "type_name" => type_name.to_string(),
    )
    .increment(1);
}
