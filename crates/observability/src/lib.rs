use std::{
    env,
    net::{IpAddr, Ipv6Addr, SocketAddr},
};

use metrics_exporter_prometheus::PrometheusBuilder;
use opentelemetry::KeyValue;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{
    logs::{SdkLogger, SdkLoggerProvider},
    Resource,
};

const METRICS_BIND_ADDR_ENV: &str = "METRICS_BIND_ADDR";
const DEPLOYMENT_ENVIRONMENT_ENV: &str = "DEPLOYMENT_ENVIRONMENT";
const SERVICE_NAMESPACE_ENV: &str = "SERVICE_NAMESPACE";
const SERVICE_VERSION_ENV: &str = "SERVICE_VERSION";
const RAILWAY_PRIVATE_DOMAIN_ENV: &str = "RAILWAY_PRIVATE_DOMAIN";
const OTLP_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
const OTLP_LOGS_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT";

pub struct OtlpTelemetry {
    logger_provider: Option<SdkLoggerProvider>,
}

impl OtlpTelemetry {
    pub fn disabled() -> Self {
        Self {
            logger_provider: None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.logger_provider.is_some()
    }

    pub fn log_layer(&self) -> Option<OpenTelemetryTracingBridge<SdkLoggerProvider, SdkLogger>> {
        self.logger_provider
            .as_ref()
            .map(OpenTelemetryTracingBridge::new)
    }

    pub fn shutdown(self) {
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
    let logs_endpoint = resolve_signal_endpoint(OTLP_LOGS_ENDPOINT_ENV, "v1/logs")?;

    if logs_endpoint.is_none() {
        return Ok(OtlpTelemetry::disabled());
    }

    let resource = otlp_resource(service_name);

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

    Ok(OtlpTelemetry { logger_provider })
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
}
