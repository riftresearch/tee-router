use clap::{Parser, Subcommand};
use temporal_worker::{
    order_execution,
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

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the production order-execution worker.
    Worker {
        #[arg(long, env = "TEMPORAL_TASK_QUEUE", default_value = order_execution::DEFAULT_TASK_QUEUE)]
        task_queue: String,

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

    let cli = Cli::parse();
    let connection = TemporalConnection {
        temporal_address: cli.temporal_address,
        namespace: cli.namespace,
    };

    match cli.command {
        Command::Worker {
            task_queue,
            runtime,
        } => {
            let activities = order_execution::activities::OrderActivities::new(
                runtime.build_order_activities().await?,
            );
            order_execution::run_worker_with_activities(&connection, &task_queue, activities)
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
