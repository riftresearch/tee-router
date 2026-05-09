pub mod activities;
pub mod types;
pub mod workflows;

pub use router_temporal::{
    order_workflow_id, provider_hint_poll_workflow_id, quote_refresh_workflow_id,
    refund_workflow_id, workflow_start_options, DEFAULT_TASK_QUEUE,
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::CoreRuntime;

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

pub async fn run_worker_with_activities(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
) -> WorkerResult<()> {
    let mut built = build_worker(connection, task_queue, order_activities).await?;
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
