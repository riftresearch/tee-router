use blockchain_utils::shutdown_signal;
use clap::Parser;
use sauron::{run, Result, SauronArgs};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

#[tokio::main]
async fn main() -> Result<()> {
    let args = SauronArgs::parse();

    let fmt_env_filter = EnvFilter::new(&args.log_level);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(fmt_env_filter);
    let otlp_telemetry = observability::init_otlp_from_env("sauron")
        .map_err(|message| sauron::Error::Observability { message })?;
    let otlp_trace_layer = otlp_telemetry
        .trace_layer("sauron")
        .map(|layer| layer.with_filter(EnvFilter::new(&args.log_level)));
    let otlp_log_layer = otlp_telemetry
        .log_layer()
        .map(|layer| layer.with_filter(EnvFilter::new(&args.log_level)));

    tracing_subscriber::registry()
        .with(otlp_trace_layer)
        .with(otlp_log_layer)
        .with(fmt_layer)
        .init();

    observability::init_prometheus_metrics_from_env("sauron")
        .map_err(|message| sauron::Error::Observability { message })?;

    if otlp_telemetry.is_enabled() {
        tracing::info!("OpenTelemetry OTLP logs/traces enabled");
    } else {
        tracing::info!("OpenTelemetry OTLP logs/traces not configured");
    }

    let result = tokio::select! {
        result = run(args) => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping sauron");
            Ok(())
        }
    };

    otlp_telemetry.shutdown();
    result
}
