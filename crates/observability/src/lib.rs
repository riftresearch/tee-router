use std::{
    env,
    net::{IpAddr, Ipv6Addr, SocketAddr},
};

use metrics_exporter_prometheus::PrometheusBuilder;
use opentelemetry::{
    global,
    trace::{
        SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState, TracerProvider as _,
    },
    Context, KeyValue,
};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    logs::{SdkLogger, SdkLoggerProvider},
    trace::{SdkTracer, SdkTracerProvider},
    Resource,
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::registry::LookupSpan;

const METRICS_BIND_ADDR_ENV: &str = "METRICS_BIND_ADDR";
const DEPLOYMENT_ENVIRONMENT_ENV: &str = "DEPLOYMENT_ENVIRONMENT";
const SERVICE_NAMESPACE_ENV: &str = "SERVICE_NAMESPACE";
const SERVICE_VERSION_ENV: &str = "SERVICE_VERSION";
const RAILWAY_PRIVATE_DOMAIN_ENV: &str = "RAILWAY_PRIVATE_DOMAIN";
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_TRACES_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
const OTLP_LOGS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowTraceContext {
    pub trace_id: String,
    pub parent_span_id: String,
}

impl WorkflowTraceContext {
    pub fn new(trace_id: impl Into<String>, parent_span_id: impl Into<String>) -> Option<Self> {
        let trace_id = trace_id.into();
        let parent_span_id = parent_span_id.into();
        if parse_trace_ids(&trace_id, &parent_span_id).is_some() {
            Some(Self {
                trace_id,
                parent_span_id,
            })
        } else {
            None
        }
    }
}

pub fn current_workflow_trace_context() -> Option<WorkflowTraceContext> {
    let context = tracing::Span::current().context();
    let span_context = context.span().span_context().clone();
    if !span_context.is_valid() {
        return None;
    }

    Some(WorkflowTraceContext {
        trace_id: span_context.trace_id().to_string(),
        parent_span_id: span_context.span_id().to_string(),
    })
}

pub fn set_workflow_parent(span: &tracing::Span, context: &WorkflowTraceContext) -> bool {
    let Some((trace_id, parent_span_id)) =
        parse_trace_ids(&context.trace_id, &context.parent_span_id)
    else {
        return false;
    };
    let span_context = SpanContext::new(
        trace_id,
        parent_span_id,
        TraceFlags::SAMPLED,
        true,
        TraceState::NONE,
    );
    span.set_parent(Context::new().with_remote_span_context(span_context))
        .is_ok()
}

fn parse_trace_ids(trace_id: &str, span_id: &str) -> Option<(TraceId, SpanId)> {
    if trace_id.len() != 32 || span_id.len() != 16 {
        return None;
    }
    if !is_lower_hex(trace_id) || !is_lower_hex(span_id) {
        return None;
    }
    let trace_id = TraceId::from_hex(trace_id).ok()?;
    let span_id = SpanId::from_hex(span_id).ok()?;
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }
    Some((trace_id, span_id))
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

pub struct OtlpTelemetry {
    tracer_provider: Option<SdkTracerProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl OtlpTelemetry {
    pub fn disabled() -> Self {
        Self {
            tracer_provider: None,
            logger_provider: None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracer_provider.is_some() || self.logger_provider.is_some()
    }

    pub fn trace_layer<S>(
        &self,
        service_name: &'static str,
    ) -> Option<OpenTelemetryLayer<S, SdkTracer>>
    where
        S: tracing::Subscriber + for<'span> LookupSpan<'span>,
    {
        self.tracer_provider.as_ref().map(|provider| {
            let tracer = provider.tracer(service_name);
            tracing_opentelemetry::layer().with_tracer(tracer)
        })
    }

    pub fn log_layer(&self) -> Option<OpenTelemetryTracingBridge<SdkLoggerProvider, SdkLogger>> {
        self.logger_provider
            .as_ref()
            .map(OpenTelemetryTracingBridge::new)
    }

    pub fn shutdown(self) {
        if let Some(provider) = self.tracer_provider {
            if let Err(err) = provider.shutdown() {
                eprintln!("failed to shutdown OpenTelemetry tracer provider: {err}");
            }
        }

        if let Some(provider) = self.logger_provider {
            if let Err(err) = provider.shutdown() {
                eprintln!("failed to shutdown OpenTelemetry logger provider: {err}");
            }
        }
    }
}

pub fn init_prometheus_metrics_from_env(service_name: &'static str) -> Result<(), String> {
    let raw_addr = match env::var(METRICS_BIND_ADDR_ENV) {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(env::VarError::NotPresent) => {
            tracing::info!(
                service = service_name,
                "{} not set; Prometheus metrics export disabled",
                METRICS_BIND_ADDR_ENV
            );
            return Ok(());
        }
        Err(err) => {
            return Err(format!(
                "failed to read {METRICS_BIND_ADDR_ENV} for {service_name}: {err}"
            ));
        }
    };

    let configured_addr = raw_addr.parse::<SocketAddr>().map_err(|err| {
        format!("failed to parse {METRICS_BIND_ADDR_ENV}={raw_addr:?} for {service_name}: {err}")
    })?;
    let bind_addr = normalize_metrics_bind_addr(configured_addr);

    let mut builder = PrometheusBuilder::new()
        .with_http_listener(bind_addr)
        .add_global_label("service", service_name)
        .add_global_label(
            "service_namespace",
            env::var(SERVICE_NAMESPACE_ENV).unwrap_or_else(|_| "tee-router".to_string()),
        );

    if let Ok(environment) = env::var(DEPLOYMENT_ENVIRONMENT_ENV) {
        if !environment.trim().is_empty() {
            builder = builder.add_global_label("deployment_environment", environment);
        }
    }

    if let Ok(version) = env::var(SERVICE_VERSION_ENV) {
        if !version.trim().is_empty() {
            builder = builder.add_global_label("service_version", version);
        }
    }

    builder
        .install()
        .map_err(|err| format!("failed to install Prometheus metrics exporter: {err}"))?;

    tracing::info!(
        service = service_name,
        %bind_addr,
        "Prometheus metrics exporter enabled"
    );
    Ok(())
}

fn normalize_metrics_bind_addr(addr: SocketAddr) -> SocketAddr {
    normalize_metrics_bind_addr_for_runtime(addr, is_railway_runtime())
}

fn normalize_metrics_bind_addr_for_runtime(addr: SocketAddr, railway_runtime: bool) -> SocketAddr {
    if railway_runtime && addr.ip() == IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED) {
        return SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), addr.port());
    }

    addr
}

fn is_railway_runtime() -> bool {
    matches!(env::var(RAILWAY_PRIVATE_DOMAIN_ENV), Ok(value) if !value.trim().is_empty())
}

pub fn init_otlp_from_env(service_name: &'static str) -> Result<OtlpTelemetry, String> {
    let traces_endpoint = resolve_signal_endpoint(OTLP_TRACES_ENDPOINT_ENV, "v1/traces")?;
    let logs_endpoint = resolve_signal_endpoint(OTLP_LOGS_ENDPOINT_ENV, "v1/logs")?;

    if traces_endpoint.is_none() && logs_endpoint.is_none() {
        return Ok(OtlpTelemetry::disabled());
    }

    let resource = otlp_resource(service_name);

    let tracer_provider = if let Some(endpoint) = traces_endpoint {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .build()
            .map_err(|err| format!("failed to build OTLP trace exporter: {err}"))?;

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.clone())
            .build();
        global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    let logger_provider = if let Some(endpoint) = logs_endpoint {
        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_endpoint(endpoint)
            .with_protocol(Protocol::HttpBinary)
            .build()
            .map_err(|err| format!("failed to build OTLP log exporter: {err}"))?;

        Some(
            SdkLoggerProvider::builder()
                .with_batch_exporter(exporter)
                .with_resource(resource)
                .build(),
        )
    } else {
        None
    };

    Ok(OtlpTelemetry {
        tracer_provider,
        logger_provider,
    })
}

fn resolve_signal_endpoint(signal_env: &str, suffix: &str) -> Result<Option<String>, String> {
    if let Some(endpoint) = nonempty_env(signal_env)? {
        return Ok(Some(endpoint));
    }

    let Some(base_endpoint) = nonempty_env(OTLP_ENDPOINT_ENV)? else {
        return Ok(None);
    };

    Ok(Some(format!(
        "{}/{}",
        base_endpoint.trim_end_matches('/'),
        suffix
    )))
}

fn nonempty_env(name: &str) -> Result<Option<String>, String> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(format!("failed to read {name}: {err}")),
    }
}

fn otlp_resource(service_name: &'static str) -> Resource {
    let mut attributes = vec![
        KeyValue::new("service.namespace", service_namespace()),
        KeyValue::new("deployment.environment", deployment_environment()),
    ];

    if let Some(version) = service_version() {
        attributes.push(KeyValue::new("service.version", version));
    }

    Resource::builder_empty()
        .with_service_name(service_name)
        .with_attributes(attributes)
        .build()
}

fn deployment_environment() -> String {
    env::var(DEPLOYMENT_ENVIRONMENT_ENV).unwrap_or_else(|_| "alpha".to_string())
}

fn service_namespace() -> String {
    env::var(SERVICE_NAMESPACE_ENV).unwrap_or_else(|_| "tee-router".to_string())
}

fn service_version() -> Option<String> {
    match env::var(SERVICE_VERSION_ENV) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_configured_ipv4_wildcard_outside_railway() {
        let addr = SocketAddr::from(([0, 0, 0, 0], 9102));

        assert_eq!(normalize_metrics_bind_addr_for_runtime(addr, false), addr);
    }

    #[test]
    fn maps_ipv4_wildcard_to_ipv6_wildcard_on_railway() {
        let addr = SocketAddr::from(([0, 0, 0, 0], 9102));

        assert_eq!(
            normalize_metrics_bind_addr_for_runtime(addr, true),
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9102)
        );
    }

    #[test]
    fn keeps_explicit_ipv4_addr_on_railway() {
        let addr = SocketAddr::from(([127, 0, 0, 1], 9102));

        assert_eq!(normalize_metrics_bind_addr_for_runtime(addr, true), addr);
    }

    #[test]
    fn validates_workflow_trace_context_hex_ids() {
        assert!(
            WorkflowTraceContext::new("11111111111111111111111111111111", "2222222222222222")
                .is_some()
        );
        assert!(
            WorkflowTraceContext::new("00000000000000000000000000000000", "2222222222222222")
                .is_none()
        );
        assert!(
            WorkflowTraceContext::new("11111111111111111111111111111111", "0000000000000000")
                .is_none()
        );
    }
}
