use std::sync::Arc;

pub mod activities;
pub mod error;
pub mod types;
pub mod workflows;

pub use router_temporal::{
    order_workflow_id, quote_refresh_workflow_id, refund_workflow_id, DEFAULT_TASK_QUEUE,
};
use temporalio_client::WorkflowStartOptions;
use temporalio_common::protos::temporal::api::enums::v1::{
    WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
};
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, ResourceBasedTuner, WorkerTuner};

use crate::runtime::{
    boxed, connect_client, new_core_runtime, TemporalConnection, WorkerError, WorkerResult,
};

use activities::{
    OrderActivities, ProviderObservationActivities, QuoteRefreshActivities, RefundActivities,
};
use workflows::{OrderWorkflow, QuoteRefreshWorkflow, RefundWorkflow};

pub const DEFAULT_TEMPORAL_TARGET_MEM_USAGE: f64 = 0.8;
pub const DEFAULT_TEMPORAL_TARGET_CPU_USAGE: f64 = 0.9;
pub const DEFAULT_TEMPORAL_MAX_CACHED_WORKFLOWS: usize = 2000;

#[derive(Debug, Clone, Copy)]
pub struct WorkerTuningConfig {
    pub target_mem_usage: f64,
    pub target_cpu_usage: f64,
    pub max_cached_workflows: usize,
}

impl WorkerTuningConfig {
    #[must_use]
    pub fn resource_based_tuner(&self) -> Arc<dyn WorkerTuner + Send + Sync> {
        Arc::new(ResourceBasedTuner::new(
            self.target_mem_usage,
            self.target_cpu_usage,
        ))
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_resource_target("SAURON_TEMPORAL_TARGET_MEM_USAGE", self.target_mem_usage)?;
        validate_resource_target("SAURON_TEMPORAL_TARGET_CPU_USAGE", self.target_cpu_usage)?;
        Ok(())
    }
}

impl Default for WorkerTuningConfig {
    fn default() -> Self {
        Self {
            target_mem_usage: DEFAULT_TEMPORAL_TARGET_MEM_USAGE,
            target_cpu_usage: DEFAULT_TEMPORAL_TARGET_CPU_USAGE,
            max_cached_workflows: DEFAULT_TEMPORAL_MAX_CACHED_WORKFLOWS,
        }
    }
}

pub fn validate_resource_target(name: &str, value: f64) -> Result<f64, String> {
    if value.is_finite() && value > 0.0 && value <= 1.0 {
        Ok(value)
    } else {
        Err(format!(
            "{name} must be greater than 0.0 and less than or equal to 1.0"
        ))
    }
}

pub async fn run_worker_with_activities(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
) -> WorkerResult<()> {
    run_worker_with_activities_and_tuning(
        connection,
        task_queue,
        order_activities,
        WorkerTuningConfig::default(),
    )
    .await
}

pub async fn run_worker_with_activities_and_tuning(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
    tuning: WorkerTuningConfig,
) -> WorkerResult<()> {
    let mut built =
        build_worker_with_tuning(connection, task_queue, order_activities, tuning).await?;
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
    build_worker_with_tuning(
        connection,
        task_queue,
        order_activities,
        WorkerTuningConfig::default(),
    )
    .await
}

pub async fn build_worker_with_tuning(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
    tuning: WorkerTuningConfig,
) -> WorkerResult<BuiltOrderWorker> {
    tuning
        .validate()
        .map_err(|message| WorkerError::Configuration { message })?;
    let runtime = new_core_runtime()?;
    let client = connect_client(connection).await?;
    let quote_refresh_activities = QuoteRefreshActivities::from_order_activities(&order_activities);
    let refund_activities = RefundActivities::from_order_activities(&order_activities);
    let provider_observation_activities =
        ProviderObservationActivities::from_order_activities(&order_activities);
    let worker_options = WorkerOptions::new(task_queue)
        .tuner(tuning.resource_based_tuner())
        .max_cached_workflows(tuning.max_cached_workflows)
        .register_workflow::<OrderWorkflow>()
        .register_workflow::<RefundWorkflow>()
        .register_workflow::<QuoteRefreshWorkflow>()
        .register_activities(order_activities)
        .register_activities(refund_activities)
        .register_activities(quote_refresh_activities)
        .register_activities(provider_observation_activities)
        .build();
    let worker =
        Worker::new(&runtime, client, worker_options).map_err(|source| WorkerError::Temporal {
            action: "create order-execution Temporal worker",
            source: boxed(source),
        })?;

    Ok(BuiltOrderWorker { runtime, worker })
}

#[must_use]
pub fn workflow_start_options(task_queue: &str, workflow_id: &str) -> WorkflowStartOptions {
    WorkflowStartOptions::new(task_queue.to_owned(), workflow_id.to_owned())
        .id_reuse_policy(WorkflowIdReusePolicy::AllowDuplicateFailedOnly)
        .id_conflict_policy(WorkflowIdConflictPolicy::Fail)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_based_tuner_constructs_as_worker_tuner() {
        let tuning = WorkerTuningConfig::default();
        tuning.validate().expect("default tuning config is valid");

        let tuner: Arc<dyn WorkerTuner + Send + Sync> = tuning.resource_based_tuner();
        let _ = tuner.workflow_task_slot_supplier();
        let _ = tuner.activity_task_slot_supplier();
        let _ = tuner.local_activity_slot_supplier();
    }
}
