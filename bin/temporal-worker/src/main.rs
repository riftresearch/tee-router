use clap::{Parser, Subcommand};
use temporal_worker::{
    order_execution,
    order_execution::WorkerTuningConfig,
    production::OrderWorkerRuntimeArgs,
    runtime::{TemporalConnection, WorkerResult},
    spike,
};
use tracing_subscriber::EnvFilter;

const DEFAULT_TEMPORAL_ADDRESS: &str = "http://127.0.0.1:7233";
const DEFAULT_NAMESPACE: &str = "default";

#[derive(Debug, Parser)]
#[command(about = "Temporal worker for the order execution rewrite")]
struct Cli {
    #[arg(long, env = "TEMPORAL_ADDRESS", default_value = DEFAULT_TEMPORAL_ADDRESS)]
    temporal_address: String,

    #[arg(long, env = "TEMPORAL_NAMESPACE", default_value = DEFAULT_NAMESPACE)]
    namespace: String,

    #[command(subcommand)]
    command: Command,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Subcommand)]
enum Command {
    /// Run the production order-execution worker.
    Worker {
        #[arg(long, env = "TEMPORAL_TASK_QUEUE", default_value = order_execution::DEFAULT_TASK_QUEUE)]
        task_queue: String,

        #[arg(
            long,
            env = "SAURON_QUOTE_REFRESH_MAX_ATTEMPTS",
            default_value_t = order_execution::activities::DEFAULT_QUOTE_REFRESH_MAX_ATTEMPTS
        )]
        quote_refresh_max_attempts: usize,

        #[arg(
            long,
            env = "SAURON_TEMPORAL_TARGET_MEM_USAGE",
            default_value = "0.8",
            value_parser = parse_resource_target
        )]
        sauron_temporal_target_mem_usage: f64,

        #[arg(
            long,
            env = "SAURON_TEMPORAL_TARGET_CPU_USAGE",
            default_value = "0.9",
            value_parser = parse_resource_target
        )]
        sauron_temporal_target_cpu_usage: f64,

        #[arg(
            long,
            env = "SAURON_TEMPORAL_MAX_CACHED_WORKFLOWS",
            default_value_t = order_execution::DEFAULT_TEMPORAL_MAX_CACHED_WORKFLOWS
        )]
        sauron_temporal_max_cached_workflows: usize,

        #[command(flatten)]
        runtime: OrderWorkerRuntimeArgs,
    },
    /// Run the PR1 Rust SDK capability spike.
    Spike {
        #[arg(long, env = "TEMPORAL_TASK_QUEUE", default_value = spike::DEFAULT_TASK_QUEUE)]
        task_queue: String,

        #[arg(long, default_value_t = 45)]
        timeout_seconds: u64,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> WorkerResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    observability::init_prometheus_metrics_from_env("temporal-worker")
        .map_err(|message| temporal_worker::runtime::WorkerError::Configuration { message })?;
    temporal_worker::telemetry::record_process_started();

    let cli = Cli::parse();
    let connection = TemporalConnection {
        temporal_address: cli.temporal_address,
        namespace: cli.namespace,
    };

    match cli.command {
        Command::Worker {
            task_queue,
            quote_refresh_max_attempts,
            sauron_temporal_target_mem_usage,
            sauron_temporal_target_cpu_usage,
            sauron_temporal_max_cached_workflows,
            runtime,
        } => {
            let activities = order_execution::activities::OrderActivities::new(
                runtime
                    .build_order_activities()
                    .await?
                    .with_quote_refresh_max_attempts(quote_refresh_max_attempts),
            );
            order_execution::run_worker_with_activities_and_tuning(
                &connection,
                &task_queue,
                activities,
                WorkerTuningConfig {
                    target_mem_usage: sauron_temporal_target_mem_usage,
                    target_cpu_usage: sauron_temporal_target_cpu_usage,
                    max_cached_workflows: sauron_temporal_max_cached_workflows,
                },
            )
            .await?;
        }
        Command::Spike {
            task_queue,
            timeout_seconds,
        } => {
            spike::run(connection, task_queue, timeout_seconds).await?;
        }
    }

    Ok(())
}

fn parse_resource_target(value: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|source| format!("invalid resource target {value:?}: {source}"))?;
    order_execution::validate_resource_target("resource target", parsed)
}

#[cfg(test)]
mod tests {
    use std::{
        env,
        ffi::OsString,
        sync::{Mutex, OnceLock},
    };

    use clap::{error::ErrorKind, Parser};

    use super::*;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvGuard {
        saved: Vec<(&'static str, Option<OsString>)>,
    }

    impl EnvGuard {
        fn set(vars: &[(&'static str, Option<&'static str>)]) -> Self {
            let saved = vars
                .iter()
                .map(|(key, _)| (*key, env::var_os(key)))
                .collect::<Vec<_>>();
            for (key, value) in vars {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
            Self { saved }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (key, value) in self.saved.drain(..) {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    fn temporal_env_vars() -> [(&'static str, Option<&'static str>); 4] {
        [
            ("SAURON_TEMPORAL_TARGET_MEM_USAGE", None),
            ("SAURON_TEMPORAL_TARGET_CPU_USAGE", None),
            ("SAURON_TEMPORAL_MAX_CACHED_WORKFLOWS", None),
            ("SAURON_TEMPORAL_DB_MAX_CONNECTIONS", None),
        ]
    }

    fn worker_args() -> Vec<&'static str> {
        vec![
            "temporal-worker",
            "worker",
            "--master-key-path",
            "/tmp/router-master-key.hex",
            "--ethereum-mainnet-rpc-url",
            "http://ethereum.example",
            "--base-rpc-url",
            "http://base.example",
            "--arbitrum-rpc-url",
            "http://arbitrum.example",
            "--bitcoin-rpc-url",
            "http://bitcoin.example",
            "--untrusted-esplora-http-server-url",
            "http://electrum.example",
        ]
    }

    #[test]
    fn worker_cli_reads_temporal_env_tuning_values() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = EnvGuard::set(&[
            ("SAURON_TEMPORAL_TARGET_MEM_USAGE", Some("0.7")),
            ("SAURON_TEMPORAL_TARGET_CPU_USAGE", Some("0.6")),
            ("SAURON_TEMPORAL_MAX_CACHED_WORKFLOWS", Some("3000")),
            ("SAURON_TEMPORAL_DB_MAX_CONNECTIONS", Some("222")),
        ]);

        let cli = Cli::try_parse_from(worker_args()).expect("worker CLI parses");
        match cli.command {
            Command::Worker {
                sauron_temporal_target_mem_usage,
                sauron_temporal_target_cpu_usage,
                sauron_temporal_max_cached_workflows,
                runtime,
                ..
            } => {
                assert_eq!(sauron_temporal_target_mem_usage, 0.7);
                assert_eq!(sauron_temporal_target_cpu_usage, 0.6);
                assert_eq!(sauron_temporal_max_cached_workflows, 3000);
                assert_eq!(runtime.db_max_connections, 222);
            }
            Command::Spike { .. } => panic!("expected worker command"),
        }
    }

    #[test]
    fn worker_cli_rejects_target_mem_usage_above_one() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = EnvGuard::set(&temporal_env_vars());
        let mut args = worker_args();
        args.extend(["--sauron-temporal-target-mem-usage", "2.0"]);

        let err = Cli::try_parse_from(args).expect_err("target above 1.0 must be rejected");
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
    }

    #[test]
    fn worker_cli_rejects_negative_target_cpu_usage() {
        let _lock = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap();
        let _env = EnvGuard::set(&temporal_env_vars());
        let mut args = worker_args();
        args.push("--sauron-temporal-target-cpu-usage=-0.1");

        let err = Cli::try_parse_from(args).expect_err("negative target must be rejected");
        assert_eq!(err.kind(), ErrorKind::ValueValidation);
    }
}
