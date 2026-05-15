use std::time::Duration;

use serde::{Deserialize, Serialize};
use temporalio_client::{
    Client, WorkflowGetResultOptions, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::protos::temporal::api::{
    common::v1::RetryPolicy,
    enums::v1::{WorkflowIdConflictPolicy, WorkflowIdReusePolicy},
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    activities::{ActivityContext, ActivityError},
    ActivityOptions, ChildWorkflowOptions, ContinueAsNewOptions, SyncWorkflowContext, Worker,
    WorkerOptions, WorkflowContext, WorkflowContextView, WorkflowResult,
};
use temporalio_sdk_core::CoreRuntime;
use tokio::time::{sleep, timeout, Instant};
use tracing::{debug, info};
use uuid::Uuid;

use crate::runtime::{
    boxed, connect_client, new_core_runtime, TemporalConnection, WorkerError, WorkerResult,
};

pub const DEFAULT_TASK_QUEUE: &str = "tee-router-order-execution-spike";

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

struct BuiltSpikeWorker {
    _runtime: CoreRuntime,
    worker: Worker,
}

pub async fn run(
    connection: TemporalConnection,
    task_queue: String,
    timeout_seconds: u64,
) -> WorkerResult<()> {
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
                .map_err(|_| WorkerError::Temporal {
                    action: "shut down Temporal worker",
                    source: boxed(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "worker did not shut down within 10s",
                    )),
                })?
                .map_err(|source| WorkerError::WorkerTask { source })?;

            worker_result.map_err(|source| WorkerError::Temporal {
                action: "run Temporal worker",
                source: boxed(source),
            })?;

            spike_result.map_err(|_| WorkerError::SpikeTimedOut {
                seconds: timeout_seconds,
            })??;

            Ok(())
        })
        .await
}

async fn build_worker(
    connection: &TemporalConnection,
    task_queue: &str,
) -> WorkerResult<BuiltSpikeWorker> {
    let runtime = new_core_runtime()?;
    let client = connect_client(connection).await?;
    let worker_options = WorkerOptions::new(task_queue)
        .register_workflow::<OrderExecutionSpikeWorkflow>()
        .register_workflow::<OrderExecutionSpikeChildWorkflow>()
        .register_workflow::<ContinueAsNewSpikeWorkflow>()
        .register_activities(OrderExecutionSpikeActivities)
        .build();
    let worker =
        Worker::new(&runtime, client, worker_options).map_err(|source| WorkerError::Temporal {
            action: "create Temporal worker",
            source: boxed(source),
        })?;

    Ok(BuiltSpikeWorker {
        _runtime: runtime,
        worker,
    })
}

async fn run_client_spike(client: &Client, task_queue: &str) -> WorkerResult<()> {
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
        .map_err(|source| WorkerError::Temporal {
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
            return Err(WorkerError::DuplicateWorkflowStarted {
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
        .map_err(|source| WorkerError::Temporal {
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
                return Err(WorkerError::Assertion {
                    reason: format!(
                        "query did not observe signal before deadline; last count={count}"
                    ),
                });
            }
            Err(source) if Instant::now() < query_deadline => {
                debug!(error = %source, "workflow query failed before retry deadline");
            }
            Err(source) => {
                return Err(WorkerError::Temporal {
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
        .map_err(|source| WorkerError::Temporal {
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
        .map_err(|source| WorkerError::Temporal {
            action: "start continue-as-new spike workflow",
            source: boxed(source),
        })?;
    let continue_result: String = continue_handle
        .get_result(WorkflowGetResultOptions::default())
        .await
        .map_err(|source| WorkerError::Temporal {
            action: "get continue-as-new spike workflow result",
            source: boxed(source),
        })?;
    if continue_result != "continued:1" {
        return Err(WorkerError::Assertion {
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

fn assert_primary_output(output: &OrderExecutionSpikeOutput) -> WorkerResult<()> {
    if output.signal_markers != ["funding-vault-funded"] {
        return Err(WorkerError::Assertion {
            reason: format!("unexpected signal markers: {:?}", output.signal_markers),
        });
    }
    if output.retry_attempt != 2 {
        return Err(WorkerError::Assertion {
            reason: format!("expected retry attempt 2, got {}", output.retry_attempt),
        });
    }
    if output.child_result != "child:custody-success" {
        return Err(WorkerError::Assertion {
            reason: format!("unexpected child workflow result: {}", output.child_result),
        });
    }

    Ok(())
}
