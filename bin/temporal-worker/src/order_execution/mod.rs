pub mod activities;
pub mod types;
pub mod workflows;

use temporalio_sdk::{Worker, WorkerOptions};

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
    let runtime = new_core_runtime()?;
    let client = connect_client(connection).await?;
    let worker_options = WorkerOptions::new(task_queue)
        .register_workflow::<OrderWorkflow>()
        .register_workflow::<RefundWorkflow>()
        .register_workflow::<QuoteRefreshWorkflow>()
        .register_workflow::<StaleRunningStepWatchdogWorkflow>()
        .register_workflow::<ProviderHintPollWorkflow>()
        .register_activities(OrderActivities)
        .register_activities(RefundActivities)
        .register_activities(QuoteRefreshActivities)
        .register_activities(ProviderObservationActivities)
        .register_activities(StepDispatchActivities)
        .build();
    let mut worker =
        Worker::new(&runtime, client, worker_options).map_err(|source| WorkerError::Temporal {
            action: "create order-execution Temporal worker",
            source: boxed(source),
        })?;

    worker.run().await.map_err(|source| WorkerError::Temporal {
        action: "run order-execution Temporal worker",
        source: boxed(source),
    })
}
