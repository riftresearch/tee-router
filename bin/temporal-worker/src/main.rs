use std::{error::Error as StdError, fmt::Display, str::FromStr, time::Duration};

use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use snafu::Snafu;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::{
    protos::temporal::api::{
        common::v1::RetryPolicy,
        enums::v1::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy},
    },
    telemetry::TelemetryOptions,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, SyncWorkflowContext, Worker,
    WorkerOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEFAULT_TEMPORAL_ADDRESS: &str = "http://127.0.0.1:7233";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_TASK_QUEUE: &str = "tee-router-order-execution-spike";

type SpikeResult<T> = std::result::Result<T, SpikeError>;
type BoxError = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Snafu)]
enum SpikeError {
    #[snafu(display("invalid Temporal address '{address}'"))]
    InvalidTemporalAddress {
        address: String,
        source: url::ParseError,
    },

    #[snafu(display("failed to {action}"))]
    Temporal {
        action: &'static str,
        source: BoxError,
    },

    #[snafu(display("worker task failed"))]
    WorkerTask { source: tokio::task::JoinError },

    #[snafu(display("Temporal spike timed out after {seconds}s"))]
    SpikeTimedOut { seconds: u64 },

    #[snafu(display("duplicate workflow ID unexpectedly started: {workflow_id}"))]
    DuplicateWorkflowStarted { workflow_id: String },

    #[snafu(display("Temporal spike assertion failed: {reason}"))]
    Assertion { reason: String },
}

#[derive(Debug, Parser)]
#[command(about = "Temporal Rust SDK spike worker for the order execution rewrite")]
struct Cli {
    #[arg(long, env = "TEMPORAL_ADDRESS", default_value = DEFAULT_TEMPORAL_ADDRESS)]
    temporal_address: String,

    #[arg(long, env = "TEMPORAL_NAMESPACE", default_value = DEFAULT_NAMESPACE)]
    namespace: String,

    #[arg(long, env = "TEMPORAL_TASK_QUEUE", default_value = DEFAULT_TASK_QUEUE)]
    task_queue: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a worker that polls the spike task queue.
    Worker,
    /// Run an in-process worker and execute the SDK capability checks.
    Spike {
        #[arg(long, default_value_t = 45)]
        timeout_seconds: u64,
    },
}

#[derive(Debug, Clone)]
struct TemporalConnection {
    temporal_address: String,
    namespace: String,
}

impl TemporalConnection {
    fn from_cli(cli: &Cli) -> Self {
        Self {
            temporal_address: cli.temporal_address.clone(),
            namespace: cli.namespace.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderExecutionSpikeInput {
    child_workflow_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OrderExecutionSpikeOutput {
    signal_markers: Vec<String>,
    retry_attempt: u32,
    child_result: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContinueAsNewSpikeInput {
    current_iteration: u32,
    max_iterations: u32,
}

#[workflow]
#[derive(Default)]
struct OrderExecutionSpikeWorkflow {
    signal_markers: Vec<String>,
}

#[workflow_methods]
impl OrderExecutionSpikeWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: OrderExecutionSpikeInput,
    ) -> WorkflowResult<OrderExecutionSpikeOutput> {
        ctx.wait_condition(|state| !state.signal_markers.is_empty())
            .await;
        ctx.timer(Duration::from_millis(100)).await;

        let retry_attempt = ctx
            .start_activity(
                OrderExecutionSpikeActivities::retry_once,
                "submit-provider-order".to_owned(),
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5))
                    .retry_policy(RetryPolicy {
                        maximum_attempts: 2,
                        ..Default::default()
                    })
                    .build(),
            )
            .await?;

        let child = ctx
            .child_workflow(
                OrderExecutionSpikeChildWorkflow::run,
                "custody-success".to_owned(),
                ChildWorkflowOptions {
                    workflow_id: input.child_workflow_id,
                    ..Default::default()
                },
            )
            .await?;
        let child_result = child.result().await?;
        let signal_markers = ctx.state(|state| state.signal_markers.clone());

        Ok(OrderExecutionSpikeOutput {
            signal_markers,
            retry_attempt,
            child_result,
        })
    }

    #[signal]
    fn funding_vault_funded(&mut self, _ctx: &mut SyncWorkflowContext<Self>, marker: String) {
        self.signal_markers.push(marker);
    }

    #[query]
    fn signal_count(&self, _ctx: &WorkflowContextView) -> usize {
        self.signal_markers.len()
    }
}

#[workflow]
#[derive(Default)]
struct OrderExecutionSpikeChildWorkflow;

#[workflow_methods]
impl OrderExecutionSpikeChildWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, marker: String) -> WorkflowResult<String> {
        Ok(format!("child:{marker}"))
    }
}

#[workflow]
#[derive(Default)]
struct ContinueAsNewSpikeWorkflow;

#[workflow_methods]
impl ContinueAsNewSpikeWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: ContinueAsNewSpikeInput,
    ) -> WorkflowResult<String> {
        ctx.timer(Duration::from_millis(100)).await;

        if input.current_iteration < input.max_iterations {
            ctx.continue_as_new(
                &ContinueAsNewSpikeInput {
                    current_iteration: input.current_iteration + 1,
                    max_iterations: input.max_iterations,
                },
                ContinueAsNewOptions::default(),
            )?;
        }

        Ok(format!("continued:{}", input.current_iteration))
    }
}

struct OrderExecutionSpikeActivities;

#[activities]
impl OrderExecutionSpikeActivities {
    #[activity]
    async fn retry_once(
        ctx: ActivityContext,
        _label: String,
    ) -> std::result::Result<u32, ActivityError> {
        if ctx.info().attempt == 1 {
            return Err(std::io::Error::other("intentional retryable spike failure").into());
        }

        Ok(ctx.info().attempt)
    }
}

struct BuiltWorker {
    _runtime: CoreRuntime,
    worker: Worker,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> SpikeResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let connection = TemporalConnection::from_cli(&cli);

    match cli.command {
        Command::Worker => {
            let mut worker = build_worker(&connection, &cli.task_queue).await?;
            worker
                .worker
                .run()
                .await
                .map_err(|source| SpikeError::Temporal {
                    action: "run Temporal worker",
                    source: boxed(source),
                })?;
        }
        Command::Spike { timeout_seconds } => {
            run_in_process_spike(connection, cli.task_queue, timeout_seconds).await?;
        }
    }

    Ok(())
}

async fn run_in_process_spike(
    connection: TemporalConnection,
    task_queue: String,
    timeout_seconds: u64,
) -> SpikeResult<()> {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async move {
            let mut worker = build_worker(&connection, &task_queue).await?;
            let shutdown_worker = worker.worker.shutdown_handle();
            let worker_task = tokio::task::spawn_local(async move { worker.worker.run().await });

            sleep(Duration::from_millis(500)).await;

            let client = connect_client(&connection).await?;
            let spike_result = timeout(
                Duration::from_secs(timeout_seconds),
                run_client_spike(&client, &task_queue),
            )
            .await;

            shutdown_worker();

            let worker_result = timeout(Duration::from_secs(10), worker_task)
                .await
                .map_err(|_| SpikeError::Temporal {
                    action: "shut down Temporal worker",
                    source: boxed(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "worker did not shut down within 10s",
                    )),
                })?
                .map_err(|source| SpikeError::WorkerTask { source })?;

            worker_result.map_err(|source| SpikeError::Temporal {
                action: "run Temporal worker",
                source: boxed(source),
            })?;

            spike_result.map_err(|_| SpikeError::SpikeTimedOut {
                seconds: timeout_seconds,
            })??;

            Ok(())
        })
        .await
}

async fn build_worker(
    connection: &TemporalConnection,
    task_queue: &str,
) -> SpikeResult<BuiltWorker> {
    let runtime_options = RuntimeOptions::builder()
        .telemetry_options(TelemetryOptions::builder().build())
        .build()
        .map_err(|source| SpikeError::Temporal {
            action: "build Temporal runtime options",
            source: boxed(source),
        })?;
    let runtime =
        CoreRuntime::new_assume_tokio(runtime_options).map_err(|source| SpikeError::Temporal {
            action: "create Temporal runtime",
            source: boxed(source),
        })?;
    let client = connect_client(connection).await?;
    let worker_options = WorkerOptions::new(task_queue)
        .register_workflow::<OrderExecutionSpikeWorkflow>()
        .register_workflow::<OrderExecutionSpikeChildWorkflow>()
        .register_workflow::<ContinueAsNewSpikeWorkflow>()
        .register_activities(OrderExecutionSpikeActivities)
        .build();
    let worker =
        Worker::new(&runtime, client, worker_options).map_err(|source| SpikeError::Temporal {
            action: "create Temporal worker",
            source: boxed(source),
        })?;

    Ok(BuiltWorker {
        _runtime: runtime,
        worker,
    })
}

async fn connect_client(connection: &TemporalConnection) -> SpikeResult<Client> {
    let target = Url::from_str(&connection.temporal_address).map_err(|source| {
        SpikeError::InvalidTemporalAddress {
            address: connection.temporal_address.clone(),
            source,
        }
    })?;
    let connection_options = ConnectionOptions::new(target).build();
    let temporal_connection = Connection::connect(connection_options)
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "connect to Temporal",
            source: boxed(source),
        })?;

    Client::new(
        temporal_connection,
        ClientOptions::new(connection.namespace.clone()).build(),
    )
    .map_err(|source| SpikeError::Temporal {
        action: "create Temporal client",
        source: boxed(source),
    })
}

async fn run_client_spike(client: &Client, task_queue: &str) -> SpikeResult<()> {
    let order_id = Uuid::now_v7();
    let primary_workflow_id = format!("order:{order_id}:execution");
    let child_workflow_id = format!("order:{order_id}:success-child");
    let continue_workflow_id = format!("order:{order_id}:continue-as-new");

    let primary_options = workflow_start_options(task_queue, &primary_workflow_id);
    let primary_handle = client
        .start_workflow(
            OrderExecutionSpikeWorkflow::run,
            OrderExecutionSpikeInput {
                child_workflow_id: child_workflow_id.clone(),
            },
            primary_options,
        )
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "start primary spike workflow",
            source: boxed(source),
        })?;
    info!(
        workflow_id = primary_workflow_id,
        run_id = ?primary_handle.run_id(),
        "started primary spike workflow"
    );

    match client
        .start_workflow(
            OrderExecutionSpikeWorkflow::run,
            OrderExecutionSpikeInput {
                child_workflow_id: child_workflow_id.clone(),
            },
            workflow_start_options(task_queue, &primary_workflow_id),
        )
        .await
    {
        Ok(_) => {
            return Err(SpikeError::DuplicateWorkflowStarted {
                workflow_id: primary_workflow_id,
            });
        }
        Err(source) => {
            info!(
                workflow_id = primary_workflow_id,
                error = %source,
                "duplicate running workflow ID was rejected"
            );
        }
    }

    primary_handle
        .signal(
            OrderExecutionSpikeWorkflow::funding_vault_funded,
            "funding-vault-funded".to_owned(),
            WorkflowSignalOptions::default(),
        )
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "signal primary spike workflow",
            source: boxed(source),
        })?;

    let query_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match primary_handle
            .query(
                OrderExecutionSpikeWorkflow::signal_count,
                (),
                WorkflowQueryOptions::default(),
            )
            .await
        {
            Ok(count) if count > 0 => {
                info!(count, "workflow query observed signal state");
                break;
            }
            Ok(count) if Instant::now() < query_deadline => {
                debug!(count, "workflow query has not observed signal yet");
            }
            Ok(count) => {
                return Err(SpikeError::Assertion {
                    reason: format!(
                        "query did not observe signal before deadline; last count={count}"
                    ),
                });
            }
            Err(source) if Instant::now() < query_deadline => {
                debug!(error = %source, "workflow query failed before retry deadline");
            }
            Err(source) => {
                return Err(SpikeError::Temporal {
                    action: "query primary spike workflow",
                    source: boxed(source),
                });
            }
        }

        sleep(Duration::from_millis(100)).await;
    }

    let output: OrderExecutionSpikeOutput = primary_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "get primary spike workflow result",
            source: boxed(source),
        })?;
    assert_primary_output(&output)?;
    info!(?output, "primary spike workflow completed");

    let continue_handle = client
        .start_workflow(
            ContinueAsNewSpikeWorkflow::run,
            ContinueAsNewSpikeInput {
                current_iteration: 0,
                max_iterations: 1,
            },
            workflow_start_options(task_queue, &continue_workflow_id),
        )
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "start continue-as-new spike workflow",
            source: boxed(source),
        })?;
    let continue_result: String = continue_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .map_err(|source| SpikeError::Temporal {
            action: "get continue-as-new spike workflow result",
            source: boxed(source),
        })?;
    if continue_result != "continued:1" {
        return Err(SpikeError::Assertion {
            reason: format!(
                "expected continue-as-new result 'continued:1', got '{continue_result}'"
            ),
        });
    }
    info!(
        result = continue_result,
        "continue-as-new spike workflow completed"
    );

    Ok(())
}

fn workflow_start_options(task_queue: &str, workflow_id: &str) -> WorkflowStartOptions {
    WorkflowStartOptions::new(task_queue.to_owned(), workflow_id.to_owned())
        .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
        .id_conflict_policy(WorkflowIdConflictPolicy::Fail)
        .build()
}

fn assert_primary_output(output: &OrderExecutionSpikeOutput) -> SpikeResult<()> {
    if output.signal_markers != ["funding-vault-funded"] {
        return Err(SpikeError::Assertion {
            reason: format!("unexpected signal markers: {:?}", output.signal_markers),
        });
    }
    if output.retry_attempt != 2 {
        return Err(SpikeError::Assertion {
            reason: format!("expected retry attempt 2, got {}", output.retry_attempt),
        });
    }
    if output.child_result != "child:custody-success" {
        return Err(SpikeError::Assertion {
            reason: format!("unexpected child workflow result: {}", output.child_result),
        });
    }

    Ok(())
}

fn boxed(source: impl Display) -> BoxError {
    Box::new(std::io::Error::other(source.to_string()))
}
