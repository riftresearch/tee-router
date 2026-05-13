use std::{sync::Arc, time::Duration};

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
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, ResourceBasedTuner, ResourceSlotOptions, WorkerTuner,
};

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
pub const DEFAULT_TEMPORAL_ACTIVITY_TASK_POLLER_MAX: usize = 128;
pub const DEFAULT_TEMPORAL_WORKFLOW_TASK_POLLER_MAX: usize = 32;
pub const DEFAULT_TEMPORAL_ACTIVITY_SLOT_MIN: usize = 64;
pub const DEFAULT_TEMPORAL_ACTIVITY_SLOT_MAX: usize = 2000;
pub const DEFAULT_TEMPORAL_WORKFLOW_SLOT_MIN: usize = 16;
pub const DEFAULT_TEMPORAL_WORKFLOW_SLOT_MAX: usize = 2000;

#[derive(Debug, Clone, Copy)]
pub struct OrderWorkerTuning {
    pub target_mem_usage: f64,
    pub target_cpu_usage: f64,
    pub max_cached_workflows: usize,
    pub activity_task_poller_max: usize,
    pub workflow_task_poller_max: usize,
    pub activity_slot_min: usize,
    pub activity_slot_max: usize,
    pub workflow_slot_min: usize,
    pub workflow_slot_max: usize,
}

pub type WorkerTuningConfig = OrderWorkerTuning;

impl OrderWorkerTuning {
    #[must_use]
    pub fn resource_based_tuner(&self) -> Arc<dyn WorkerTuner + Send + Sync> {
        let mut tuner = ResourceBasedTuner::new(self.target_mem_usage, self.target_cpu_usage);
        tuner.with_activity_slots_options(ResourceSlotOptions::new(
            self.activity_slot_min,
            self.activity_slot_max,
            Duration::from_millis(20),
        ));
        tuner.with_workflow_slots_options(ResourceSlotOptions::new(
            self.workflow_slot_min,
            self.workflow_slot_max,
            Duration::from_millis(0),
        ));
        Arc::new(tuner)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_resource_target("SAURON_TEMPORAL_TARGET_MEM_USAGE", self.target_mem_usage)?;
        validate_resource_target("SAURON_TEMPORAL_TARGET_CPU_USAGE", self.target_cpu_usage)?;
        validate_poller_max(
            "ROUTER_TEMPORAL_ACTIVITY_POLLERS",
            self.activity_task_poller_max,
        )?;
        validate_poller_max(
            "ROUTER_TEMPORAL_WORKFLOW_POLLERS",
            self.workflow_task_poller_max,
        )?;
        validate_slot_config(
            "ROUTER_TEMPORAL_ACTIVITY_SLOT_MIN",
            self.activity_slot_min,
            "ROUTER_TEMPORAL_ACTIVITY_SLOT_MAX",
            self.activity_slot_max,
        )?;
        validate_slot_config(
            "ROUTER_TEMPORAL_WORKFLOW_SLOT_MIN",
            self.workflow_slot_min,
            "ROUTER_TEMPORAL_WORKFLOW_SLOT_MAX",
            self.workflow_slot_max,
        )?;
        Ok(())
    }
}

impl Default for OrderWorkerTuning {
    fn default() -> Self {
        Self {
            target_mem_usage: DEFAULT_TEMPORAL_TARGET_MEM_USAGE,
            target_cpu_usage: DEFAULT_TEMPORAL_TARGET_CPU_USAGE,
            max_cached_workflows: DEFAULT_TEMPORAL_MAX_CACHED_WORKFLOWS,
            activity_task_poller_max: DEFAULT_TEMPORAL_ACTIVITY_TASK_POLLER_MAX,
            workflow_task_poller_max: DEFAULT_TEMPORAL_WORKFLOW_TASK_POLLER_MAX,
            activity_slot_min: DEFAULT_TEMPORAL_ACTIVITY_SLOT_MIN,
            activity_slot_max: DEFAULT_TEMPORAL_ACTIVITY_SLOT_MAX,
            workflow_slot_min: DEFAULT_TEMPORAL_WORKFLOW_SLOT_MIN,
            workflow_slot_max: DEFAULT_TEMPORAL_WORKFLOW_SLOT_MAX,
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

fn validate_poller_max(name: &str, value: usize) -> Result<usize, String> {
    if value >= 2 {
        Ok(value)
    } else {
        Err(format!("{name} must be at least 2"))
    }
}

fn validate_slot_config(
    min_name: &str,
    min_value: usize,
    max_name: &str,
    max_value: usize,
) -> Result<(), String> {
    if min_value < 2 {
        return Err(format!("{min_name} must be at least 2"));
    }
    if min_value > max_value {
        return Err(format!(
            "{min_name} must be less than or equal to {max_name}"
        ));
    }
    Ok(())
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
        OrderWorkerTuning::default(),
    )
    .await
}

pub async fn run_worker_with_activities_and_tuning(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
    tuning: OrderWorkerTuning,
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
        OrderWorkerTuning::default(),
    )
    .await
}

pub async fn build_worker_with_tuning(
    connection: &TemporalConnection,
    task_queue: &str,
    order_activities: OrderActivities,
    tuning: OrderWorkerTuning,
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
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(
            tuning.activity_task_poller_max,
        ))
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(
            tuning.workflow_task_poller_max,
        ))
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
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    #[test]
    fn resource_based_tuner_constructs_as_worker_tuner() {
        let tuning = OrderWorkerTuning::default();
        tuning.validate().expect("default tuning config is valid");

        let tuner: Arc<dyn WorkerTuner + Send + Sync> = tuning.resource_based_tuner();
        let _ = tuner.workflow_task_slot_supplier();
        let _ = tuner.activity_task_slot_supplier();
        let _ = tuner.local_activity_slot_supplier();
    }

    #[test]
    fn tuning_validates_positive_poller_counts() {
        let mut tuning = OrderWorkerTuning::default();
        tuning.activity_task_poller_max = 0;
        let err = tuning
            .validate()
            .expect_err("zero activity pollers must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_ACTIVITY_POLLERS"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.activity_task_poller_max = 1;
        let err = tuning
            .validate()
            .expect_err("one activity poller must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_ACTIVITY_POLLERS"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.workflow_task_poller_max = 0;
        let err = tuning
            .validate()
            .expect_err("zero workflow pollers must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_WORKFLOW_POLLERS"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.workflow_task_poller_max = 1;
        let err = tuning
            .validate()
            .expect_err("one workflow poller must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_WORKFLOW_POLLERS"));
    }

    #[test]
    fn tuning_defaults_have_sane_pollers() {
        const EXPECTED_TASK_QUEUE_PARTITIONS: usize = 16;

        let tuning = OrderWorkerTuning::default();

        assert!(tuning.activity_task_poller_max >= EXPECTED_TASK_QUEUE_PARTITIONS);
        assert_eq!(
            tuning.activity_task_poller_max,
            DEFAULT_TEMPORAL_ACTIVITY_TASK_POLLER_MAX
        );
        assert_eq!(
            tuning.workflow_task_poller_max,
            DEFAULT_TEMPORAL_WORKFLOW_TASK_POLLER_MAX
        );
        assert!(tuning.workflow_task_poller_max >= 2);
    }

    #[test]
    fn tuning_validates_slot_config() {
        let mut tuning = OrderWorkerTuning::default();
        tuning.activity_slot_min = 0;
        let err = tuning
            .validate()
            .expect_err("zero activity slot minimum must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_ACTIVITY_SLOT_MIN"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.activity_slot_min = tuning.activity_slot_max + 1;
        let err = tuning
            .validate()
            .expect_err("activity slot minimum above maximum must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_ACTIVITY_SLOT_MIN"));
        assert!(err.contains("ROUTER_TEMPORAL_ACTIVITY_SLOT_MAX"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.workflow_slot_min = 0;
        let err = tuning
            .validate()
            .expect_err("zero workflow slot minimum must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_WORKFLOW_SLOT_MIN"));

        let mut tuning = OrderWorkerTuning::default();
        tuning.workflow_slot_min = tuning.workflow_slot_max + 1;
        let err = tuning
            .validate()
            .expect_err("workflow slot minimum above maximum must be rejected");
        assert!(err.contains("ROUTER_TEMPORAL_WORKFLOW_SLOT_MIN"));
        assert!(err.contains("ROUTER_TEMPORAL_WORKFLOW_SLOT_MAX"));
    }

    #[test]
    fn tuning_defaults_have_sane_slots() {
        let tuning = OrderWorkerTuning::default();

        assert!(tuning.activity_slot_min >= 64);
        assert_eq!(tuning.activity_slot_min, DEFAULT_TEMPORAL_ACTIVITY_SLOT_MIN);
        assert_eq!(tuning.activity_slot_max, DEFAULT_TEMPORAL_ACTIVITY_SLOT_MAX);
        assert!(tuning.activity_slot_max >= tuning.activity_slot_min);
        assert!(tuning.workflow_slot_min >= 16);
        assert_eq!(tuning.workflow_slot_min, DEFAULT_TEMPORAL_WORKFLOW_SLOT_MIN);
        assert_eq!(tuning.workflow_slot_max, DEFAULT_TEMPORAL_WORKFLOW_SLOT_MAX);
        assert!(tuning.workflow_slot_max >= tuning.workflow_slot_min);
    }
}
