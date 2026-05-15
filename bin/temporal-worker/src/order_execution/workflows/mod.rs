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

use super::activities::{OrderActivities, QuoteRefreshActivities, RefundActivities};
use super::refund_workflow_id;
use super::types::{
    AcknowledgeManualInterventionInput, AcknowledgeReason, AcknowledgeUnrecoverableSignal,
    ClassifyStaleRunningStepInput, ClassifyStepFailureInput, ComposeRefreshedQuoteAttemptInput,
    DiscoverSingleRefundPositionInput, DispatchOutcome, DispatchStepInput,
    FinalizeOrderOrRefundInput, FinalizedOrder, FundingVaultFundedSignal,
    LoadManualInterventionContextInput, LoadOrderExecutionStateInput, ManualInterventionScope,
    ManualReleaseSignal, ManualResolutionSignalKind, ManualTriggerRefundSignal,
    MarkOrderCompletedInput, MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
    MaterializeRefundPlanInput, MaterializeRetryAttemptInput, MaterializedExecutionAttempt,
    OrderTerminalStatus, OrderWorkflowDebugCursor, OrderWorkflowInput, OrderWorkflowOutput,
    OrderWorkflowPhase, PersistStepFailedInput, PrepareManualInterventionRefundInput,
    PrepareManualInterventionRetryInput, ProviderOperationHintDecision,
    ProviderOperationHintSignal, QuoteRefreshWorkflowInput,
    QuoteRefreshWorkflowOutcome, QuoteRefreshWorkflowOutput, RefreshedQuoteAttemptOutcome,
    RefundPlanOutcome, RefundTerminalStatus, RefundTrigger, RefundWorkflowInput,
    RefundWorkflowOutput, ReleaseRefundManualInterventionInput, SingleRefundPositionOutcome,
    StaleRunningStepClassified, StaleRunningStepDecision, StepDispatched, StepFailureDecision,
    VerifyProviderOperationHintInput, WorkflowAttemptId, WorkflowExecutionStep, WorkflowOrderId,
    WorkflowStepId, WriteFailedAttemptSnapshotInput,
};
use crate::telemetry;

const PROVIDER_HINT_WAIT_TIMEOUT: Duration = Duration::from_secs(2 * 60 * 60);
// Activity tasks can sit in Temporal matching for tens of minutes during 10k+ order
// bursts. Keep this timer long enough that queue dispatch lag does not look like a
// stuck step. Honest "activity actually hung" cases will still surface, just with a
// longer detection window.
const STALE_RUNNING_STEP_RECOVERY_AFTER: Duration = Duration::from_secs(2 * 60 * 60);
// Activity start-to-close timeout. Under 10k burst load, refund-side activities
// (HL trade reversal + bridge withdrawal + sweep) can chain enough work to exceed
// 10 min. 60 min keeps honest hang detection while absorbing burst-load lag.
const EXECUTE_STEP_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(60 * 60);
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
