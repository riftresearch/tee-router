use crate::{Error, Result, RouterServerArgs};
use blockchain_utils::shutdown_signal;
use snafu::{FromString, Whatever};
use std::future::Future;
use tokio::task::JoinSet;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

pub type BackgroundTaskResult = std::result::Result<(), String>;

pub fn init_tracing(
    args: &RouterServerArgs,
    service_name: &'static str,
) -> JoinSet<BackgroundTaskResult> {
    let mut background_tasks = JoinSet::new();

    let fmt_env_filter = EnvFilter::new(&args.log_level);
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(fmt_env_filter);

    let loki_task = if let Some(loki_url) = &args.loki_url {
        match url::Url::parse(loki_url) {
            Ok(url) => {
                let loki_env_filter = EnvFilter::new(&args.log_level);
                match tracing_loki::builder()
                    .label("service", service_name)
                    .and_then(|builder| builder.build_url(url))
                {
                    Ok((loki_layer, task)) => {
                        let filtered_loki_layer = loki_layer.with_filter(loki_env_filter);
                        tracing_subscriber::registry()
                            .with(fmt_layer)
                            .with(filtered_loki_layer)
                            .init();

                        background_tasks.spawn(async move {
                            task.await;
                            Ok::<(), String>(())
                        });
                        tracing::info!("Loki logging enabled, shipping logs to {}", loki_url);
                        true
                    }
                    Err(err) => {
                        eprintln!(
                            "Failed to initialize Loki layer: {}, continuing without Loki",
                            err
                        );
                        tracing_subscriber::registry().with(fmt_layer).init();
                        false
                    }
                }
            }
            Err(err) => {
                eprintln!("Invalid LOKI_URL: {}, continuing without Loki", err);
                tracing_subscriber::registry().with(fmt_layer).init();
                false
            }
        }
    } else {
        tracing_subscriber::registry().with(fmt_layer).init();
        false
    };

    if !loki_task {
        tracing::info!("Loki logging not configured (set LOKI_URL to enable)");
    }

    background_tasks
}

pub async fn run_until_shutdown<F>(
    service_name: &'static str,
    service: F,
    mut background_tasks: JoinSet<BackgroundTaskResult>,
) -> Result<()>
where
    F: Future<Output = Result<()>>,
{
    tokio::pin!(service);

    tokio::select! {
        result = &mut service => result,
        _ = shutdown_signal() => {
            tracing::info!("Shutdown signal received; stopping {}", service_name);
            Ok(())
        }
        background_result = background_tasks.join_next(), if !background_tasks.is_empty() => {
            match background_result {
                Some(Ok(Ok(()))) => Err(Error::Generic {
                    source: Whatever::without_source(
                        "A background task exited unexpectedly".to_string(),
                    ),
                }),
                Some(Ok(Err(err))) => Err(Error::Generic {
                    source: Whatever::without_source(err),
                }),
                Some(Err(err)) => Err(Error::Generic {
                    source: Whatever::without_source(format!(
                        "A background task panicked or was cancelled: {err}"
                    )),
                }),
                None => Err(Error::Generic {
                    source: Whatever::without_source(
                        "The background task set terminated unexpectedly".to_string(),
                    ),
                }),
            }
        }
    }
}
