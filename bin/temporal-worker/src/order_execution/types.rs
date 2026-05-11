use router_core::{
    models::{OrderExecutionLeg, OrderExecutionStep, RouterOrder},
    protocol::DepositAsset,
    services::{asset_registry::CanonicalAsset, ProviderExecutionState},
};
pub use router_temporal::{
    AcknowledgeUnrecoverableSignal, HlBridgeDepositCreditedEvidence,
    HlBridgeDepositObservedEvidence, HlTradeCanceledEvidence, HlTradeFilledEvidence,
    HlWithdrawalAcknowledgedEvidence, HlWithdrawalSettledEvidence, ManualReleaseSignal,
    ManualTriggerRefundSignal, OrderWorkflowInput, ProviderHintKind, ProviderKind,
    ProviderOperationHintEvidence, ProviderOperationHintSignal, UnitDepositHintEvidence,
    WorkflowAttemptId, WorkflowHintId, WorkflowOrderId, WorkflowProviderOperationId,
    WorkflowStepId, WorkflowVaultId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{error::Error, fmt};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RawAmount(String);

impl RawAmount {
    pub fn new(value: impl Into<String>) -> Result<Self, RawAmountError> {
        let value = value.into();
        if value.is_empty() {
            return Err(RawAmountError::Empty);
        }
        if !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(RawAmountError::NonDecimalDigit);
        }
        if value.len() > 1 && value.starts_with('0') {
            return Err(RawAmountError::LeadingZero);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RawAmount {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for RawAmount {
    type Error = RawAmountError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl TryFrom<&str> for RawAmount {
    type Error = RawAmountError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for RawAmount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawAmountError {
    Empty,
    NonDecimalDigit,
    LeadingZero,
}

impl fmt::Display for RawAmountError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("raw amount is empty"),
            Self::NonDecimalDigit => {
                formatter.write_str("raw amount must contain only ASCII decimal digits")
            }
            Self::LeadingZero => formatter.write_str("raw amount must not contain leading zeroes"),
        }
    }
}

impl Error for RawAmountError {}

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
    #[serde(default)]
    pub funded_to_workflow_start_seconds: Option<f64>,
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
    #[serde(default)]
    pub funded_to_workflow_start_seconds: Option<f64>,
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
pub struct FundingVaultFundedSignal {
    pub order_id: WorkflowOrderId,
    pub vault_id: WorkflowVaultId,
    pub observed_amount_raw: RawAmount,
    pub source_ref: Option<String>,
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
    #[serde(default)]
    pub funded_to_workflow_start_seconds: Option<f64>,
    pub order: RouterOrder,
    pub plan: ExecutionPlan,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadManualInterventionContextInput {
    pub order_id: WorkflowOrderId,
    pub scope: ManualInterventionScope,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManualInterventionWorkflowContext {
    pub attempt_id: Option<WorkflowAttemptId>,
    pub step_id: Option<WorkflowStepId>,
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
pub struct ClassifyStaleRunningStepInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StaleRunningStepClassified {
    pub step_id: WorkflowStepId,
    pub decision: StaleRunningStepDecision,
    pub reason: Value,
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
    pub attempt_id: Option<WorkflowAttemptId>,
    pub step_id: Option<WorkflowStepId>,
    pub terminal_status: OrderTerminalStatus,
    pub reason: Option<Value>,
    #[serde(default)]
    pub funded_to_workflow_start_seconds: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareManualInterventionRetryInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub signal: ManualReleaseSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareManualInterventionRefundInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
    pub signal: ManualTriggerRefundSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReleaseRefundManualInterventionInput {
    pub order_id: WorkflowOrderId,
    pub refund_attempt_id: Option<WorkflowAttemptId>,
    pub step_id: Option<WorkflowStepId>,
    pub signal_kind: ManualResolutionSignalKind,
    pub reason: String,
    pub operator_id: Option<String>,
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgeManualInterventionInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: Option<WorkflowAttemptId>,
    pub step_id: Option<WorkflowStepId>,
    pub scope: ManualInterventionScope,
    pub signal: AcknowledgeUnrecoverableSignal,
    pub reason: AcknowledgeReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManualInterventionScope {
    OrderAttempt,
    RefundAttempt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcknowledgeReason {
    OperatorTerminal,
    ZombieCleanup,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManualResolutionSignalKind {
    Release,
    TriggerRefund,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizedOrder {
    pub order_id: WorkflowOrderId,
    pub terminal_status: OrderTerminalStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyProviderOperationHintInput {
    pub order_id: WorkflowOrderId,
    pub step_id: WorkflowStepId,
    pub signal: ProviderOperationHintSignal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintVerified {
    pub provider_operation_id: Option<WorkflowProviderOperationId>,
    pub decision: ProviderOperationHintDecision,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderOperationHintDecision {
    Accept,
    Reject,
    Defer,
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
pub struct DiscoverSingleRefundPositionInput {
    pub order_id: WorkflowOrderId,
    pub failed_attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleRefundPosition {
    pub position_kind: RecoverablePositionKind,
    pub owning_step_id: Option<WorkflowStepId>,
    pub funding_vault_id: Option<WorkflowVaultId>,
    pub custody_vault_id: Option<WorkflowVaultId>,
    pub asset: DepositAsset,
    pub amount: RawAmount,
    pub hyperliquid_coin: Option<String>,
    pub hyperliquid_canonical: Option<CanonicalAsset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SingleRefundPositionDiscovery {
    pub outcome: SingleRefundPositionOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SingleRefundPositionOutcome {
    Position(SingleRefundPosition),
    Untenable { reason: RefundUntenableReason },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum RefundUntenableReason {
    RefundRequiresSingleRecoverablePosition {
        position_count: usize,
        recoverable_position_count: usize,
    },
    RefundRecoverablePositionDisappearedAfterValidation,
}

impl RefundUntenableReason {
    #[must_use]
    pub fn reason_str(&self) -> &'static str {
        match self {
            Self::RefundRequiresSingleRecoverablePosition { .. } => {
                "refund_requires_single_recoverable_position"
            }
            Self::RefundRecoverablePositionDisappearedAfterValidation => {
                "refund_recoverable_position_disappeared_after_validation"
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializeRefundPlanInput {
    pub order_id: WorkflowOrderId,
    pub failed_attempt_id: WorkflowAttemptId,
    pub position: SingleRefundPosition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefundPlanShape {
    pub outcome: RefundPlanOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefundPlanOutcome {
    Materialized {
        refund_attempt_id: WorkflowAttemptId,
        steps: Vec<WorkflowExecutionStep>,
    },
    Untenable {
        reason: RefundUntenableReason,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComposeRefreshedQuoteAttemptInput {
    pub order_id: WorkflowOrderId,
    pub stale_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckPreExecutionStaleQuoteInput {
    pub order_id: WorkflowOrderId,
    pub attempt_id: WorkflowAttemptId,
    pub step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreExecutionStaleQuoteCheck {
    pub should_refresh: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshedQuoteAttemptShape {
    pub outcome: RefreshedQuoteAttemptOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshedQuoteFailureReason {
    pub reason: String,
    pub trace: String,
    pub stale_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
    pub superseded_attempt_id: WorkflowAttemptId,
    pub superseded_attempt_index: i32,
    pub stale_step_id: WorkflowStepId,
    pub stale_execution_leg_id: Uuid,
    pub provider_quote_expires_at: String,
    pub refreshed_quote_id: Uuid,
}

impl RefreshedQuoteFailureReason {
    #[must_use]
    pub fn stale_provider_quote_refresh(
        stale_attempt_id: Uuid,
        superseded_attempt_index: i32,
        failed_step_id: Uuid,
        stale_execution_leg_id: Uuid,
        provider_quote_expires_at: chrono::DateTime<chrono::Utc>,
        refreshed_quote_id: Uuid,
    ) -> Self {
        Self {
            reason: "stale_provider_quote_refresh".to_string(),
            trace: "quote_refresh_workflow".to_string(),
            stale_attempt_id: stale_attempt_id.into(),
            failed_step_id: failed_step_id.into(),
            superseded_attempt_id: stale_attempt_id.into(),
            superseded_attempt_index,
            stale_step_id: failed_step_id.into(),
            stale_execution_leg_id,
            provider_quote_expires_at: provider_quote_expires_at.to_rfc3339(),
            refreshed_quote_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshedQuoteSupersededReason {
    pub reason: String,
    pub stale_attempt_id: WorkflowAttemptId,
    pub failed_step_id: WorkflowStepId,
    pub stale_step_id: WorkflowStepId,
    pub stale_execution_leg_id: Uuid,
    pub provider_quote_expires_at: String,
    pub refreshed_quote_id: Uuid,
}

impl RefreshedQuoteSupersededReason {
    #[must_use]
    pub fn stale_provider_quote_refresh(
        stale_attempt_id: Uuid,
        failed_step_id: Uuid,
        stale_execution_leg_id: Uuid,
        provider_quote_expires_at: chrono::DateTime<chrono::Utc>,
        refreshed_quote_id: Uuid,
    ) -> Self {
        Self {
            reason: "superseded_by_stale_provider_quote_refresh".to_string(),
            stale_attempt_id: stale_attempt_id.into(),
            failed_step_id: failed_step_id.into(),
            stale_step_id: failed_step_id.into(),
            stale_execution_leg_id,
            provider_quote_expires_at: provider_quote_expires_at.to_rfc3339(),
            refreshed_quote_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputCustodySourceKind {
    FundingVault,
    ExternalCustody,
    HyperliquidCustody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputCustodySnapshotAsset {
    pub chain: String,
    pub asset: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputCustodySnapshot {
    pub source_kind: InputCustodySourceKind,
    pub funding_vault_id: Option<WorkflowVaultId>,
    pub failed_step_id: WorkflowStepId,
    pub failed_step_index: i32,
    pub failed_step_type: String,
    pub amount_in: Option<String>,
    pub min_amount_out: Option<String>,
    pub source_asset: InputCustodySnapshotAsset,
    pub source_custody_vault_id: Option<Value>,
    pub source_custody_vault_role: Option<Value>,
    pub source_custody_vault_address: Option<Value>,
    pub hyperliquid_custody_vault_id: Option<Value>,
    pub hyperliquid_custody_vault_role: Option<Value>,
    pub hyperliquid_custody_vault_address: Option<Value>,
    pub recipient_custody_vault_id: Option<Value>,
    pub recipient_custody_vault_role: Option<Value>,
    pub recipient: Option<Value>,
    pub revert_custody_vault_id: Option<Value>,
    pub revert_custody_vault_role: Option<Value>,
    pub revert_custody_vault_address: Option<Value>,
}

impl InputCustodySnapshot {
    #[must_use]
    pub fn from_failed_step(order: &RouterOrder, failed_step: &OrderExecutionStep) -> Self {
        let source_kind = if failed_step.request.get("source_custody_vault_id").is_some() {
            InputCustodySourceKind::ExternalCustody
        } else if failed_step
            .request
            .get("hyperliquid_custody_vault_id")
            .is_some()
        {
            InputCustodySourceKind::HyperliquidCustody
        } else {
            InputCustodySourceKind::FundingVault
        };
        let source_asset = failed_step
            .input_asset
            .clone()
            .or_else(|| failed_step.output_asset.clone())
            .unwrap_or_else(|| order.source_asset.clone());

        Self {
            source_kind,
            funding_vault_id: order.funding_vault_id.map(Into::into),
            failed_step_id: failed_step.id.into(),
            failed_step_index: failed_step.step_index,
            failed_step_type: failed_step.step_type.to_db_string().to_string(),
            amount_in: failed_step.amount_in.clone(),
            min_amount_out: failed_step.min_amount_out.clone(),
            source_asset: InputCustodySnapshotAsset {
                chain: source_asset.chain.as_str().to_string(),
                asset: source_asset.asset.as_str().to_string(),
            },
            source_custody_vault_id: failed_step.request.get("source_custody_vault_id").cloned(),
            source_custody_vault_role: failed_step
                .request
                .get("source_custody_vault_role")
                .cloned(),
            source_custody_vault_address: failed_step
                .request
                .get("source_custody_vault_address")
                .cloned(),
            hyperliquid_custody_vault_id: failed_step
                .request
                .get("hyperliquid_custody_vault_id")
                .cloned(),
            hyperliquid_custody_vault_role: failed_step
                .request
                .get("hyperliquid_custody_vault_role")
                .cloned(),
            hyperliquid_custody_vault_address: failed_step
                .request
                .get("hyperliquid_custody_vault_address")
                .cloned(),
            recipient_custody_vault_id: failed_step
                .request
                .get("recipient_custody_vault_id")
                .cloned(),
            recipient_custody_vault_role: failed_step
                .request
                .get("recipient_custody_vault_role")
                .cloned(),
            recipient: failed_step.request.get("recipient").cloned(),
            revert_custody_vault_id: failed_step.request.get("revert_custody_vault_id").cloned(),
            revert_custody_vault_role: failed_step
                .request
                .get("revert_custody_vault_role")
                .cloned(),
            revert_custody_vault_address: failed_step
                .details
                .get("revert_custody_vault_address")
                .cloned(),
        }
    }
}

impl From<RefreshedQuoteFailureReason> for Value {
    fn from(value: RefreshedQuoteFailureReason) -> Self {
        serde_json::to_value(value).expect("refreshed quote failure reason serializes to JSON")
    }
}

impl From<RefreshedQuoteSupersededReason> for Value {
    fn from(value: RefreshedQuoteSupersededReason) -> Self {
        serde_json::to_value(value).expect("refreshed quote superseded reason serializes to JSON")
    }
}

impl From<InputCustodySnapshot> for Value {
    fn from(value: InputCustodySnapshot) -> Self {
        serde_json::to_value(value).expect("input custody snapshot serializes to JSON")
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefreshedQuoteAttemptOutcome {
    Refreshed {
        order_id: WorkflowOrderId,
        stale_attempt_id: WorkflowAttemptId,
        failed_step_id: WorkflowStepId,
        plan: ExecutionPlan,
        failure_reason: RefreshedQuoteFailureReason,
        superseded_reason: RefreshedQuoteSupersededReason,
        input_custody_snapshot: InputCustodySnapshot,
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
pub enum StaleRunningStepDecision {
    DurableProviderOperationWaitingExternalProgress,
    AmbiguousExternalSideEffectWindow,
    StaleRunningStepWithoutCheckpoint,
}

impl StaleRunningStepDecision {
    #[must_use]
    pub fn reason_str(self) -> &'static str {
        match self {
            Self::DurableProviderOperationWaitingExternalProgress => {
                "durable_provider_operation_waiting_external_progress"
            }
            Self::AmbiguousExternalSideEffectWindow => "ambiguous_external_side_effect_window",
            Self::StaleRunningStepWithoutCheckpoint => "stale_running_step_without_checkpoint",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecoverablePositionKind {
    FundingVault,
    ExternalCustody,
    HyperliquidSpot,
}

#[cfg(test)]
mod tests {
    use super::{
        InputCustodySnapshot, RawAmount, RawAmountError, RefreshedQuoteFailureReason,
        RefreshedQuoteSupersededReason,
    };
    use serde_json::json;

    #[test]
    fn raw_amount_accepts_positive_decimal_string() {
        let amount = RawAmount::new("123456789012345678901234567890").unwrap();
        assert_eq!(amount.as_str(), "123456789012345678901234567890");
        assert_eq!(amount.to_string(), "123456789012345678901234567890");
    }

    #[test]
    fn raw_amount_accepts_zero() {
        let amount = RawAmount::new("0").unwrap();
        assert_eq!(amount.as_str(), "0");
    }

    #[test]
    fn raw_amount_rejects_empty_string() {
        assert_eq!(RawAmount::new("").unwrap_err(), RawAmountError::Empty);
    }

    #[test]
    fn raw_amount_rejects_non_decimal_digits() {
        assert_eq!(
            RawAmount::new("12_34").unwrap_err(),
            RawAmountError::NonDecimalDigit
        );
        assert_eq!(
            RawAmount::new("１２").unwrap_err(),
            RawAmountError::NonDecimalDigit
        );
    }

    #[test]
    fn raw_amount_rejects_leading_zeroes() {
        assert_eq!(
            RawAmount::new("01").unwrap_err(),
            RawAmountError::LeadingZero
        );
        assert_eq!(
            RawAmount::new("00").unwrap_err(),
            RawAmountError::LeadingZero
        );
    }

    #[test]
    fn raw_amount_serde_uses_decimal_string_wire_format() {
        let amount = RawAmount::new("42").unwrap();
        let encoded = serde_json::to_string(&amount).unwrap();
        assert_eq!(encoded, "\"42\"");
        let decoded: RawAmount = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, amount);
        assert!(serde_json::from_str::<RawAmount>("\"0042\"").is_err());
    }

    #[test]
    fn refreshed_quote_failure_reason_preserves_json_wire_shape() {
        let value = json!({
            "reason": "stale_provider_quote_refresh",
            "trace": "quote_refresh_workflow",
            "stale_attempt_id": "018f13e1-0000-7000-8000-000000000001",
            "failed_step_id": "018f13e1-0000-7000-8000-000000000002",
            "superseded_attempt_id": "018f13e1-0000-7000-8000-000000000001",
            "superseded_attempt_index": 1,
            "stale_step_id": "018f13e1-0000-7000-8000-000000000002",
            "stale_execution_leg_id": "018f13e1-0000-7000-8000-000000000003",
            "provider_quote_expires_at": "2026-05-10T12:00:00+00:00",
            "refreshed_quote_id": "018f13e1-0000-7000-8000-000000000004",
        });

        let decoded: RefreshedQuoteFailureReason = serde_json::from_value(value.clone()).unwrap();

        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }

    #[test]
    fn refreshed_quote_superseded_reason_preserves_json_wire_shape() {
        let value = json!({
            "reason": "superseded_by_stale_provider_quote_refresh",
            "stale_attempt_id": "018f13e1-0000-7000-8000-000000000001",
            "failed_step_id": "018f13e1-0000-7000-8000-000000000002",
            "stale_step_id": "018f13e1-0000-7000-8000-000000000002",
            "stale_execution_leg_id": "018f13e1-0000-7000-8000-000000000003",
            "provider_quote_expires_at": "2026-05-10T12:00:00+00:00",
            "refreshed_quote_id": "018f13e1-0000-7000-8000-000000000004",
        });

        let decoded: RefreshedQuoteSupersededReason =
            serde_json::from_value(value.clone()).unwrap();

        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }

    #[test]
    fn input_custody_snapshot_preserves_json_wire_shape() {
        let value = json!({
            "source_kind": "external_custody",
            "funding_vault_id": "018f13e1-0000-7000-8000-000000000001",
            "failed_step_id": "018f13e1-0000-7000-8000-000000000002",
            "failed_step_index": 1,
            "failed_step_type": "across_bridge",
            "amount_in": "1000000",
            "min_amount_out": "990000",
            "source_asset": {
                "chain": "base",
                "asset": "usdc"
            },
            "source_custody_vault_id": "018f13e1-0000-7000-8000-000000000003",
            "source_custody_vault_role": "source_deposit",
            "source_custody_vault_address": "0x0000000000000000000000000000000000000001",
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_role": null,
            "hyperliquid_custody_vault_address": null,
            "recipient_custody_vault_id": "018f13e1-0000-7000-8000-000000000004",
            "recipient_custody_vault_role": "destination_execution",
            "recipient": "0x0000000000000000000000000000000000000002",
            "revert_custody_vault_id": null,
            "revert_custody_vault_role": null,
            "revert_custody_vault_address": null,
        });

        let decoded: InputCustodySnapshot = serde_json::from_value(value.clone()).unwrap();

        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    }
}
