mod alloy_ext;
mod bitcoin_wallet;
mod transfer_auth_helper;
pub use alloy_ext::*;
pub use bitcoin_wallet::*;
use tracing::error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
pub use transfer_auth_helper::*;

pub fn handle_background_thread_result<T, E>(
    result: Option<Result<Result<T, E>, tokio::task::JoinError>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    match result {
        Some(Ok(thread_result)) => match thread_result {
            Ok(_) => Err("Background thread completed unexpectedly".into()),
            Err(e) => Err(format!("Background thread returned error: {e}").into()),
        },
        Some(Err(e)) => Err(format!("Join set failed: {e}").into()),
        None => Err("Join set ended with no result".into()),
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum InitLoggerError {
    #[snafu(display("Failed to initialize logger: {}", source))]
    LoggerFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub fn init_logger<L>(log_level: &str, console_layer: Option<L>) -> Result<(), InitLoggerError>
where
    L: Layer<tracing_subscriber::Registry> + Send + Sync + 'static,
{
    tracing_subscriber::registry()
        .with(console_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_line_number(true)
                .with_filter(EnvFilter::new(log_level)),
        )
        .init();

    Ok(())
}

/// Awaits the first shutdown signal (SIGTERM or SIGINT) and then returns.
pub async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        match (
            signal(SignalKind::terminate()),
            signal(SignalKind::interrupt()),
        ) {
            (Ok(mut sigterm), Ok(mut sigint)) => {
                tokio::select! {
                    _ = sigterm.recv() => {},
                    _ = sigint.recv() => {},
                }
            }
            (Ok(mut sigterm), Err(err)) => {
                error!(error = %err, "failed to install SIGINT handler; waiting for SIGTERM only");
                let _ = sigterm.recv().await;
            }
            (Err(err), Ok(mut sigint)) => {
                error!(error = %err, "failed to install SIGTERM handler; waiting for SIGINT only");
                let _ = sigint.recv().await;
            }
            (Err(term_err), Err(int_err)) => {
                error!(
                    sigterm_error = %term_err,
                    sigint_error = %int_err,
                    "failed to install unix signal handlers; falling back to Ctrl+C"
                );
                if let Err(err) = tokio::signal::ctrl_c().await {
                    error!(error = %err, "failed to wait for Ctrl+C shutdown signal");
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!(error = %err, "failed to wait for Ctrl+C shutdown signal");
        }
    }
}
