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
    emit(
        kind,
        service,
        method,
        endpoint,
        status_class(status),
        &status.to_string(),
        duration,
    );
}

pub fn record_upstream_transport_error(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    duration: Duration,
) {
    emit(
        kind,
        service,
        method,
        endpoint,
        TRANSPORT_ERROR,
        TRANSPORT_ERROR,
        duration,
    );
}

/// Legacy class-only entry point (sidecar clients that already mapped their
/// status to a class). The exact-code label mirrors the class so every series
/// still carries a `status_code`; prefer [`record_upstream_http_status`] when
/// the raw code is available so `status_code` is the exact HTTP status.
pub fn record_upstream_request(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    emit(
        kind,
        service,
        method,
        endpoint,
        status_class,
        status_class,
        duration,
    );
}

const TRANSPORT_ERROR: &str = "transport_error";

/// Single emission point. `status_code` (exact HTTP code, or `transport_error`)
/// rides the request counter only; the latency histogram stays at `status_class`
/// granularity to keep its per-bucket cardinality bounded.
fn emit(
    kind: UpstreamKind,
    service: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    status_code: &str,
    duration: Duration,
) {
    counter!(
        "tee_router_upstream_requests_total",
        "kind" => kind.as_str(),
        "service" => service,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
        "status_code" => status_code.to_owned(),
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

/// HTTP status → class bucket (`2xx`, `4xx`, …). Public so the rare emitter that
/// must build the metric inline (dynamic `service`/`method` labels) reuses this
/// single mapping instead of copying it.
#[must_use]
pub fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}
