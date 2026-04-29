use clap::Parser;
use router_server::{
    runtime::{init_tracing, run_until_shutdown},
    server::run_api,
    Result, RouterServerArgs,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = RouterServerArgs::parse();
    let (background_tasks, otlp_telemetry) = init_tracing(&args, "router-api")?;
    run_until_shutdown(
        "router-api",
        run_api(args),
        background_tasks,
        otlp_telemetry,
    )
    .await
}
