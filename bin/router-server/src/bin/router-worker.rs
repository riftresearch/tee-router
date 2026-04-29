use clap::Parser;
use router_server::{
    runtime::{init_tracing, run_until_shutdown},
    worker::run_worker,
    Result, RouterServerArgs,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = RouterServerArgs::parse();
    let (background_tasks, otlp_telemetry) = init_tracing(&args, "router-worker")?;
    run_until_shutdown(
        "router-worker",
        run_worker(args),
        background_tasks,
        otlp_telemetry,
    )
    .await
}
