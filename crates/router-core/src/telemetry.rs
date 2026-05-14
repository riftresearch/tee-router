use metrics::{counter, histogram};
use std::time::Duration;

use observability::upstream::{
    record_upstream_http_status, record_upstream_transport_error, UpstreamKind,
};

pub fn record_db_query(operation: &'static str, success: bool, duration: Duration) {
    let status = success_status(success);
    counter!(
        "tee_router_db_queries_total",
        "operation" => operation,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_db_query_duration_seconds",
        "operation" => operation,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

pub fn record_trading_venue_http_status(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status: u16,
    duration: Duration,
) {
    record_upstream_http_status(
        UpstreamKind::TradingVenue,
        venue,
        method,
        endpoint,
        status,
        duration,
    );
}

pub fn record_trading_venue_transport_error(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    duration: Duration,
) {
    record_upstream_transport_error(
        UpstreamKind::TradingVenue,
        venue,
        method,
        endpoint,
        duration,
    );
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}
