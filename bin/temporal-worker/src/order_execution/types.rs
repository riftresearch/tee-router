use router_core::{
    models::{OrderExecutionLeg, OrderExecutionStep, RouterOrder},
    services::ProviderExecutionState,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub type WorkflowOrderId = Uuid;
pub type WorkflowAttemptId = Uuid;
pub type WorkflowStepId = Uuid;
pub type WorkflowVaultId = Uuid;
pub type WorkflowProviderOperationId = Uuid;
pub type WorkflowHintId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWorkflowInput {
    pub order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWorkflowOutput {
    pub order_id: WorkflowOrderId,
    pub terminal_status: OrderTerminalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionPlan {
    pub path_id: String,
    pub transition_decl_ids: Vec<String>,
    pub legs: Vec<OrderExecutionLeg>,
    pub steps: Vec<OrderExecutionStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecuteStepInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepExecuted {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub response: Value,
    pub tx_hash: Option<String>,
    pub provider_state: ProviderExecutionState,
    pub outcome: StepExecutionOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepExecutionOutcome {
    Completed,
    Waiting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkOrderCompletedInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderCompleted {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundWorkflowInput {
    pub order_id: WorkflowOrderId,
    pub parent_attempt_id: Option<WorkflowAttemptId>,
    pub trigger: RefundTrigger,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundWorkflowOutput {
    pub order_id: WorkflowOrderId,
    pub terminal_status: RefundTerminalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteRefreshWorkflowInput {
    pub order_id: WorkflowOrderId,
    pub stale_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuoteRefreshWorkflowOutput {
    pub outcome: QuoteRefreshWorkflowOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuoteRefreshWorkflowOutcome {
    Refreshed {
        attempt_id: WorkflowAttemptId,
        steps: Vec<WorkflowExecutionStep>,
    },
    Untenable {
        reason: StaleQuoteRefreshUntenableReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleRunningStepWatchdogInput {
    pub order_id: WorkflowOrderId,
    pub step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleRunningStepWatchdogOutput {
    pub manual_intervention_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHintPollWorkflowInput {
    pub order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHintPollWorkflowOutput {
    pub hints_claimed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingVaultFundedSignal {
    pub order_id: WorkflowOrderId,
    pub vault_id: WorkflowVaultId,
    pub observed_amount_raw: String,
    pub source_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintSignal {
    pub order_id: WorkflowOrderId,
    pub hint_id: WorkflowHintId,
    pub provider_operation_id: Option<WorkflowProviderOperationId>,
    pub provider: ProviderKind,
    pub hint_kind: ProviderHintKind,
    pub provider_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualRefundTriggerSignal {
    pub order_id: WorkflowOrderId,
    pub operator_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualInterventionReleaseSignal {
    pub order_id: WorkflowOrderId,
    pub operator_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderWorkflowDebugCursor {
    pub order_id: WorkflowOrderId,
    pub phase: OrderWorkflowPhase,
    pub active_attempt_id: Option<WorkflowAttemptId>,
    pub active_step_id: Option<WorkflowStepId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadOrderExecutionStateInput {
    pub order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderExecutionState {
    pub order_id: WorkflowOrderId,
    pub phase: OrderWorkflowPhase,
    pub active_attempt_id: Option<WorkflowAttemptId>,
    pub active_step_id: Option<WorkflowStepId>,
    pub order: RouterOrder,
    pub plan: ExecutionPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeExecutionAttemptInput {
    pub order_id: WorkflowOrderId,
    pub plan: ExecutionPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedExecutionAttempt {
    pub attempt_id: WorkflowAttemptId,
    pub steps: Vec<WorkflowExecutionStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeRetryAttemptInput {
    pub order_id: WorkflowOrderId,
    pub failed_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowExecutionStep {
    pub step_id: WorkflowStepId,
    pub step_index: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistStepReadyToFireInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistStepFailedInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub failure_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistStepTerminalStatusInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub terminal_status: StepTerminalSubStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistProviderReceiptInput {
    pub execution: StepExecuted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistProviderOperationStatusInput {
    pub execution: StepExecuted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettleProviderStepInput {
    pub execution: StepExecuted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundaryPersisted {
    pub boundary: PersistenceBoundary,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifyStepFailureInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteFailedAttemptSnapshotInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedAttemptSnapshotWritten {
    pub attempt_id: WorkflowAttemptId,
    pub attempt_index: i32,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizeOrderOrRefundInput {
    pub order_id: WorkflowOrderId,
    pub terminal_status: OrderTerminalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedOrder {
    pub order_id: WorkflowOrderId,
    pub terminal_status: OrderTerminalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyProviderOperationHintInput {
    pub signal: ProviderOperationHintSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintVerified {
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollProviderOperationHintsInput {
    pub order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintsPolled {
    pub hints_claimed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverAcrossOnchainLogInput {
    pub order_id: WorkflowOrderId,
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcrossOnchainLogRecovered {
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelTimedOutHyperliquidTradeInput {
    pub order_id: WorkflowOrderId,
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidTradeCancelRecorded {
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverSingleRefundPositionInput {
    pub order_id: WorkflowOrderId,
    pub failed_attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleRefundPosition {
    pub position_kind: RecoverablePositionKind,
    pub owning_step_id: Option<WorkflowStepId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeRefundPlanInput {
    pub order_id: WorkflowOrderId,
    pub position: SingleRefundPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundPlanShape {
    pub refund_attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposeRefreshedQuoteAttemptInput {
    pub order_id: WorkflowOrderId,
    pub stale_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshedQuoteAttemptShape {
    pub outcome: RefreshedQuoteAttemptOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefreshedQuoteAttemptOutcome {
    Refreshed {
        order_id: WorkflowOrderId,
        stale_attempt_id: WorkflowAttemptId,
        failed_step_id: WorkflowStepId,
        plan: ExecutionPlan,
        failure_reason: Value,
        superseded_reason: Value,
        input_custody_snapshot: Value,
    },
    Untenable {
        order_id: WorkflowOrderId,
        stale_attempt_id: WorkflowAttemptId,
        failed_step_id: WorkflowStepId,
        reason: StaleQuoteRefreshUntenableReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeRefreshedAttemptInput {
    pub order_id: WorkflowOrderId,
    pub refreshed_attempt: RefreshedQuoteAttemptShape,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshedAttemptMaterialized {
    pub attempt_id: WorkflowAttemptId,
    pub steps: Vec<WorkflowExecutionStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum StaleQuoteRefreshUntenableReason {
    RefreshedExactInOutputBelowMinAmountOut {
        amount_out: String,
        min_amount_out: String,
    },
    RefreshedExactOutInputAboveAvailableAmount {
        amount_in: String,
        available_amount: String,
    },
    StaleProviderQuoteRefreshUntenable {
        message: String,
    },
}

impl StaleQuoteRefreshUntenableReason {
    #[must_use]
    pub fn reason_str(&self) -> &'static str {
        match self {
            Self::RefreshedExactInOutputBelowMinAmountOut { .. } => {
                "refreshed_exact_in_output_below_min_amount_out"
            }
            Self::RefreshedExactOutInputAboveAvailableAmount { .. } => {
                "refreshed_exact_out_input_above_available_amount"
            }
            Self::StaleProviderQuoteRefreshUntenable { .. } => {
                "stale_provider_quote_refresh_untenable"
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DispatchStepProviderActionInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub step_kind: ProviderStepKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderActionDispatchShape {
    pub provider_operation_id: WorkflowProviderOperationId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderWorkflowPhase {
    WaitingForFunding,
    Executing,
    RefreshingQuote,
    Refunding,
    WaitingForManualIntervention,
    Finalizing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderTerminalStatus {
    Completed,
    RefundRequired,
    Refunded,
    ManualInterventionRequired,
    RefundManualInterventionRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefundTerminalStatus {
    Refunded,
    RefundManualInterventionRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefundTrigger {
    FailedAttempt,
    ManualRefund,
    VaultAlreadyRefunded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepTerminalSubStatus {
    Completed,
    Waiting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderOperationStatus {
    Planned,
    Submitted,
    WaitingExternal,
    Completed,
    Failed,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistenceBoundary {
    AfterExecutionLegsPersisted,
    AfterStepMarkedFailed,
    AfterExecutionStepStatusPersisted,
    AfterProviderReceiptPersisted,
    AfterProviderOperationStatusPersisted,
    AfterProviderStepSettlement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepFailureDecision {
    RetryNewAttempt,
    RefreshQuote,
    StartRefund,
    ManualIntervention,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderKind {
    Bridge,
    Unit,
    Exchange,
    CustodyActionExecutor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderHintKind {
    CctpAttestation,
    AcrossFill,
    UnitDeposit,
    ProviderObservation,
    HyperliquidTrade,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoverablePositionKind {
    FundingVault,
    ExternalCustody,
    InternalCustody,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderStepKind {
    Refund,
    AcrossBridge,
    CctpBridge,
    HyperliquidBridgeDeposit,
    HyperliquidBridgeWithdrawal,
    UnitDeposit,
    UnitWithdrawal,
    HyperliquidTrade,
    UniversalRouterSwap,
}
