mod order_execution;
mod runtime;
mod spike;

use clap::{Parser, Subcommand};
use runtime::{TemporalConnection, WorkerResult};
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
    /// Run the production order-execution worker skeleton.
    Worker {
        #[arg(long, env = "TEMPORAL_TASK_QUEUE", default_value = order_execution::DEFAULT_TASK_QUEUE)]
        task_queue: String,
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
        Command::Worker { task_queue } => {
            order_execution::run_worker(&connection, &task_queue).await?;
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
