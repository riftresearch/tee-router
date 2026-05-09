pub mod activities;
pub mod types;
pub mod workflows;

use temporalio_client::WorkflowStartOptions;
use temporalio_common::protos::temporal::api::enums::v1::{
    WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;
use uuid::Uuid;

use crate::runtime::{
    boxed, connect_client, new_core_runtime, TemporalConnection, WorkerError, WorkerResult,
};

use activities::{
    OrderActivities, ProviderObservationActivities, QuoteRefreshActivities, RefundActivities,
    StepDispatchActivities,
};
use workflows::{
    OrderWorkflow, ProviderHintPollWorkflow, QuoteRefreshWorkflow, RefundWorkflow,
    StaleRunningStepWatchdogWorkflow,
};

pub const DEFAULT_TASK_QUEUE: &str = "tee-router-order-execution";

pub async fn run_worker(connection: &TemporalConnection, task_queue: &str) -> WorkerResult<()> {
    let mut built = build_worker(connection, task_queue, OrderActivities::default()).await?;
    built
        .worker
        .run()
        .await
        .map_err(|source| WorkerError::Temporal {
            action: "run order-execution Temporal worker",
            source: boxed(source),
        })
}

pub struct BuiltOrderWorker {
    pub runtime: CoreRuntime,
    pub worker: Worker,
}

pub async fn build_worker(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
) -> WorkerResult<BuiltOrderWorker> {
    let runtime = new_core_runtime()?;
    let client = connect_client(connection).await?;
    let quote_refresh_activities = QuoteRefreshActivities::from_order_activities(&order_activities);
    let refund_activities = RefundActivities::from_order_activities(&order_activities);
    let provider_observation_activities =
        ProviderObservationActivities::from_order_activities(&order_activities);
    let worker_options = WorkerOptions::new(task_queue)
        .register_workflow::<OrderWorkflow>()
        .register_workflow::<RefundWorkflow>()
        .register_workflow::<QuoteRefreshWorkflow>()
        .register_workflow::<StaleRunningStepWatchdogWorkflow>()
        .register_workflow::<ProviderHintPollWorkflow>()
        .register_activities(order_activities)
        .register_activities(refund_activities)
        .register_activities(quote_refresh_activities)
        .register_activities(provider_observation_activities)
        .register_activities(StepDispatchActivities)
        .build();
    let worker =
        Worker::new(&runtime, client, worker_options).map_err(|source| WorkerError::Temporal {
            action: "create order-execution Temporal worker",
            source: boxed(source),
        })?;

    Ok(BuiltOrderWorker { runtime, worker })
}

#[must_use]
pub fn order_workflow_id(order_id: Uuid) -> String {
    format!("order:{order_id}:execution")
}

#[must_use]
pub fn refund_workflow_id(order_id: Uuid, parent_attempt_id: Uuid) -> String {
    format!("order:{order_id}:refund:{parent_attempt_id}")
}

#[must_use]
pub fn workflow_start_options(task_queue: &str, workflow_id: &str) -> WorkflowStartOptions {
    WorkflowStartOptions::new(task_queue.to_owned(), workflow_id.to_owned())
        .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)
        .id_conflict_policy(WorkflowIdConflictPolicy::Fail)
        .build()
}
