use std::time::Duration;

use metrics::{counter, histogram};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamKind {
    TradingVenue,
    SidecarService,
}

impl UpstreamKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TradingVenue => "trading_venue",
            Self::SidecarService => "sidecar_service",
        }
    }
}

pub fn record_upstream_http_status(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status: u16,
    duration: Duration,
) {
    record_upstream_request(kind, service, method, endpoint, status_class(status), duration);
}

pub fn record_upstream_transport_error(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    duration: Duration,
) {
    record_upstream_request(kind, service, method, endpoint, "transport_error", duration);
}

pub fn record_upstream_request(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    counter!(
        "tee_router_upstream_requests_total",
        "kind" => kind.as_str(),
        "service" => service,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    histogram!(
        "tee_router_upstream_request_duration_seconds",
        "kind" => kind.as_str(),
        "service" => service,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .record(duration.as_secs_f64());
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
