use metrics::{counter, histogram};
use std::time::Duration;

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

pub fn record_venue_http_status(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status: u16,
    duration: Duration,
) {
    record_venue_request(venue, method, endpoint, status_class(status), duration);
}

pub fn record_venue_transport_error(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    duration: Duration,
) {
    record_venue_request(venue, method, endpoint, "transport_error", duration);
}

fn record_venue_request(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    counter!(
        "tee_router_venue_requests_total",
        "venue" => venue,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    histogram!(
        "tee_router_venue_request_duration_seconds",
        "venue" => venue,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .record(duration.as_secs_f64());
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}
