use std::time::{Duration, SystemTime};

use router_core::models::RouterOrderStatus;
use serde_json::{json, Value};
use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};
use uuid::Uuid;

use super::activities::{
    OrderActivities, ProviderObservationActivities, QuoteRefreshActivities, RefundActivities,
};
use super::refund_workflow_id;
use super::types::{
    AcknowledgeManualInterventionInput, AcknowledgeReason, AcknowledgeUnrecoverableSignal,
    CheckPreExecutionStaleQuoteInput, ClassifyStaleRunningStepInput, ClassifyStepFailureInput,
    ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput, ExecuteStepInput,
    FinalizeOrderOrRefundInput, FinalizedOrder, FundingVaultFundedSignal,
    LoadManualInterventionContextInput, LoadOrderExecutionStateInput, ManualInterventionScope,
    ManualReleaseSignal, ManualResolutionSignalKind, ManualTriggerRefundSignal,
    MarkOrderCompletedInput, MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
    MaterializeRefundPlanInput, MaterializeRetryAttemptInput, MaterializedExecutionAttempt,
    OrderTerminalStatus, OrderWorkflowDebugCursor, OrderWorkflowInput, OrderWorkflowOutput,
    OrderWorkflowPhase, PersistProviderOperationStatusInput, PersistProviderReceiptInput,
    PersistStepFailedInput, PersistStepReadyToFireInput, PrepareManualInterventionRefundInput,
    PrepareManualInterventionRetryInput, ProviderOperationHintDecision,
    ProviderOperationHintSignal, QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutcome,
    QuoteRefreshWorkflowOutput, RefreshedQuoteAttemptOutcome, RefundPlanOutcome,
    RefundTerminalStatus, RefundTrigger, RefundWorkflowInput, RefundWorkflowOutput,
    ReleaseRefundManualInterventionInput, SettleProviderStepInput, SingleRefundPositionOutcome,
    StaleRunningStepClassified, StaleRunningStepDecision, StepExecuted, StepExecutionOutcome,
    StepFailureDecision, VerifyProviderOperationHintInput, WorkflowAttemptId, WorkflowOrderId,
    WorkflowStepId, WriteFailedAttemptSnapshotInput,
};
use crate::telemetry;

const PROVIDER_HINT_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const STALE_RUNNING_STEP_RECOVERY_AFTER: Duration = Duration::from_secs(5 * 60);
const EXECUTE_STEP_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const QUOTE_REFRESH_WORKFLOW_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
const MANUAL_INTERVENTION_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const ORDER_WORKFLOW_TYPE: &str = "OrderWorkflow";
const REFUND_WORKFLOW_TYPE: &str = "RefundWorkflow";

mod order;
mod quote_refresh;
mod refund;
mod shared;

pub use self::order::OrderWorkflow;
pub use self::quote_refresh::QuoteRefreshWorkflow;
pub use self::refund::RefundWorkflow;
