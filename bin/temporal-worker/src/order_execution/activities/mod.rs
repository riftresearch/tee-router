// Activity function constants are registered before the later rewrite slices invoke them.
use std::{
    future::Future,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::{
    primitives::{Address, FixedBytes, U256},
    rpc::types::Filter,
    sol,
    sol_types::SolEvent,
};
use chains::{ChainRegistry, UserDepositCandidateStatus};
use chrono::Utc;
use router_core::{
    db::{
        Database, ExecutionAttemptPlan, ExecutionStepLatencyRecord,
        ExternalCustodyRefundAttemptPlan, FundingVaultRefundAttemptPlan,
        HyperliquidSpotRefundAttemptPlan, PersistStepCompletionRecord,
        RefreshedExecutionAttemptPlan,
    },
    error::{RouterCoreError, RouterCoreResult},
    models::{
        CustodyVault, CustodyVaultRole, CustodyVaultVisibility, DepositVault, MarketOrderKind,
        MarketOrderKindType, MarketOrderQuote, OrderExecutionAttempt, OrderExecutionAttemptKind,
        OrderExecutionAttemptStatus, OrderExecutionLeg, OrderExecutionStep,
        OrderExecutionStepStatus, OrderExecutionStepType, OrderProviderAddress,
        OrderProviderOperation, ProviderAddressRole, ProviderOperationStatus,
        ProviderOperationType, RouterOrder, RouterOrderQuote,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            unit_withdrawal_minimum_raw, BridgeExecutionRequest, BridgeQuote, BridgeQuoteRequest,
            ExchangeExecutionRequest, ExchangeQuote, ExchangeQuoteRequest,
            ProviderExecutionStatePatch, ProviderOperationObservation,
            ProviderOperationObservationRequest, UnitDepositStepRequest, UnitWithdrawalStepRequest,
        },
        asset_registry::{
            CanonicalAsset, MarketOrderNode, MarketOrderTransitionKind, ProviderId, TransitionDecl,
            TransitionPath,
        },
        custody_action_executor::{CustodyAction, CustodyActionError, CustodyActionRequest},
        market_order_planner::MarketOrderPlanRemainingStart,
        quote_legs::{
            execution_step_type_for_transition_kind, QuoteLeg, QuoteLegAsset, QuoteLegSpec,
        },
        ActionProviderRegistry, CustodyActionExecutor, CustodyActionReceipt,
        MarketOrderRoutePlanner, ProviderExecutionIntent, ProviderExecutionState,
        ProviderOperationIntent,
    },
};
use router_primitives::{ChainType, Currency, TokenIdentifier};
use serde_json::{json, Value};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use uuid::Uuid;

use crate::telemetry;

use super::{
    error::OrderActivityError,
    types::{
        AcknowledgeManualInterventionInput, AcknowledgeReason, AcrossOnchainLogRecovered,
        BoundaryPersisted, CheckPreExecutionStaleQuoteInput, ClassifyStaleRunningStepInput,
        ClassifyStepFailureInput, ComposeRefreshedQuoteAttemptInput,
        DiscoverSingleRefundPositionInput, ExecuteStepInput, ExecutionPlan,
        FailedAttemptSnapshotWritten, FinalizeOrderOrRefundInput, FinalizedOrder,
        InputCustodySnapshot, LoadManualInterventionContextInput, LoadOrderExecutionStateInput,
        ManualInterventionScope, ManualInterventionWorkflowContext, ManualResolutionSignalKind,
        MarkOrderCompletedInput, MaterializeExecutionAttemptInput,
        MaterializeRefreshedAttemptInput, MaterializeRefundPlanInput, MaterializeRetryAttemptInput,
        MaterializedExecutionAttempt, OrderCompleted, OrderExecutionState, OrderTerminalStatus,
        OrderWorkflowPhase, PersistProviderOperationStatusInput, PersistProviderReceiptInput,
        PersistStepFailedInput, PersistStepReadyToFireInput, PersistenceBoundary,
        PreExecutionStaleQuoteCheck, PrepareManualInterventionRefundInput,
        PrepareManualInterventionRetryInput, ProviderHintKind, ProviderOperationHintDecision,
        ProviderOperationHintEvidence, ProviderOperationHintSignal, ProviderOperationHintVerified,
        RawAmount, RecoverAcrossOnchainLogInput, RecoverablePositionKind,
        RefreshedAttemptMaterialized, RefreshedQuoteAttemptOutcome, RefreshedQuoteAttemptShape,
        RefreshedQuoteFailureReason, RefreshedQuoteSupersededReason, RefundPlanOutcome,
        RefundPlanShape, RefundUntenableReason, ReleaseRefundManualInterventionInput,
        SettleProviderStepInput, SingleRefundPosition, SingleRefundPositionDiscovery,
        SingleRefundPositionOutcome, StaleQuoteRefreshUntenableReason, StaleRunningStepClassified,
        StaleRunningStepDecision, StepExecuted, StepExecutionOutcome, StepFailureDecision,
        UnitDepositHintEvidence, VerifyProviderOperationHintInput, WorkflowExecutionStep,
        WriteFailedAttemptSnapshotInput,
    },
};

const MAX_EXECUTION_ATTEMPTS: i32 = 2;
const MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION: usize = 8;
const REFUND_PATH_MAX_DEPTH: usize = 5;
const REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;
const REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 =
    REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW;
const REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS: u64 = 12_500;
const REFRESH_PROBE_MAX_AMOUNT_IN: &str = "340282366920938463463374607431768211455";

async fn record_activity<T, F>(activity_name: &'static str, activity: F) -> Result<T, ActivityError>
where
    F: Future<Output = Result<T, OrderActivityError>>,
{
    let started = Instant::now();
    let result = activity.await.map_err(ActivityError::from);
    telemetry::record_activity(activity_name, result.is_ok(), started.elapsed());
    result
}

async fn record_step_waiting_external_latency_metrics(deps: &OrderActivityDeps, step_id: Uuid) {
    match deps
        .db
        .orders()
        .get_execution_step_latency_record(step_id)
        .await
    {
        Ok(record) => record_waiting_external_latency_record(&record),
        Err(error) => {
            tracing::warn!(
                step_id = %step_id,
                error = %error,
                "failed to record waiting-external latency metrics"
            );
        }
    }
}

async fn record_step_terminal_latency_metrics(deps: &OrderActivityDeps, step_id: Uuid) {
    match deps
        .db
        .orders()
        .get_execution_step_latency_record(step_id)
        .await
    {
        Ok(record) => record_terminal_latency_record(&record),
        Err(error) => {
            tracing::warn!(
                step_id = %step_id,
                error = %error,
                "failed to record terminal step latency metrics"
            );
        }
    }
}

async fn record_order_executor_wait_total_metric(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    outcome: &'static str,
    funded_to_workflow_start_seconds: Option<f64>,
) {
    match deps
        .db
        .orders()
        .get_order_executor_wait_total(order_id)
        .await
    {
        Ok(total) => telemetry::record_order_executor_wait_total(
            outcome,
            total.total_seconds() + funded_to_workflow_start_seconds.unwrap_or(0.0),
        ),
        Err(error) => {
            tracing::warn!(
                order_id = %order_id,
                outcome,
                error = %error,
                "failed to record order executor wait total metric"
            );
        }
    }
}

fn record_waiting_external_latency_record(record: &ExecutionStepLatencyRecord) {
    let step_type = record.step_type.to_db_string();
    if let Some(started_at) = record.started_at {
        if let Some(duration) = nonnegative_duration(record.created_at, started_at) {
            telemetry::record_step_queue_latency(step_type, duration);
        }
        if let Some(waiting_external_at) = record.waiting_external_at {
            if let Some(duration) = nonnegative_duration(started_at, waiting_external_at) {
                telemetry::record_step_dispatch_latency(step_type, duration);
            }
        }
    }
}

fn record_terminal_latency_record(record: &ExecutionStepLatencyRecord) {
    let step_type = record.step_type.to_db_string();
    if record.waiting_external_at.is_none() {
        if let Some(started_at) = record.started_at {
            if let Some(duration) = nonnegative_duration(record.created_at, started_at) {
                telemetry::record_step_queue_latency(step_type, duration);
            }
        }
        return;
    }

    let Some(waiting_external_at) = record.waiting_external_at else {
        return;
    };
    let Some(hint_arrived_at) = record.hint_arrived_at else {
        return;
    };
    let Some(completed_at) = record.completed_at else {
        return;
    };
    if let Some(duration) = nonnegative_duration(waiting_external_at, hint_arrived_at) {
        telemetry::record_step_external_wait(step_type, duration);
    }
    if let Some(duration) = nonnegative_duration(hint_arrived_at, completed_at) {
        telemetry::record_step_verification_latency(
            provider_operation_hint_kind_label(record.provider_operation_type),
            duration,
        );
    }
}

fn nonnegative_duration(
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
) -> Option<Duration> {
    end.signed_duration_since(start).to_std().ok()
}

fn nonnegative_duration_seconds(
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
) -> Option<f64> {
    nonnegative_duration(start, end).map(|duration| duration.as_secs_f64())
}

fn provider_operation_hint_kind_label(
    operation_type: Option<ProviderOperationType>,
) -> &'static str {
    match operation_type {
        Some(ProviderOperationType::AcrossBridge) => "across_fill",
        Some(ProviderOperationType::CctpBridge) => "cctp_attestation",
        Some(ProviderOperationType::CctpReceive) => "cctp_receive_observed",
        Some(ProviderOperationType::UnitDeposit) => "unit_deposit",
        Some(
            ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder,
        ) => "hyperliquid_trade",
        Some(ProviderOperationType::UniversalRouterSwap) => "velora_swap_settled",
        Some(
            ProviderOperationType::UnitWithdrawal
            | ProviderOperationType::HyperliquidBridgeDeposit
            | ProviderOperationType::HyperliquidBridgeWithdrawal,
        ) => "provider_observation",
        None => "unknown",
    }
}

// Canonical Across V3 `FundsDeposited` event used for scar §11 lost-intent
// recovery. Keep this byte-for-byte aligned with router-core's Across provider.
sol! {
    event FundsDeposited(
        bytes32 inputToken,
        bytes32 outputToken,
        uint256 inputAmount,
        uint256 outputAmount,
        uint256 indexed destinationChainId,
        uint256 indexed depositId,
        uint32 quoteTimestamp,
        uint32 fillDeadline,
        uint32 exclusivityDeadline,
        bytes32 indexed depositor,
        bytes32 recipient,
        bytes32 exclusiveRelayer,
        bytes message
    );
}

#[derive(Clone)]
pub struct OrderActivityDeps {
    pub db: Database,
    pub action_providers: Arc<ActionProviderRegistry>,
    pub custody_action_executor: Arc<CustodyActionExecutor>,
    pub chain_registry: Arc<ChainRegistry>,
    pub planner: MarketOrderRoutePlanner,
}

impl OrderActivityDeps {
    #[must_use]
    pub fn new(
        db: Database,
        action_providers: Arc<ActionProviderRegistry>,
        custody_action_executor: Arc<CustodyActionExecutor>,
        chain_registry: Arc<ChainRegistry>,
    ) -> Self {
        let planner = MarketOrderRoutePlanner::new(action_providers.asset_registry());
        Self {
            db,
            action_providers,
            custody_action_executor,
            chain_registry,
            planner,
        }
    }
}

#[derive(Clone, Default)]
pub struct OrderActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl OrderActivities {
    #[must_use]
    pub fn new(deps: OrderActivityDeps) -> Self {
        Self {
            deps: Some(Arc::new(deps)),
        }
    }

    #[must_use]
    pub(crate) fn shared_deps(&self) -> Option<Arc<OrderActivityDeps>> {
        self.deps.clone()
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, OrderActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| OrderActivityError::missing_configuration("order activities"))
    }
}

mod execution;
mod hint;
mod hydration;
mod refresh;
mod refund;
mod refund_builders;

#[cfg(test)]
mod tests;

pub use self::hint::ProviderObservationActivities;
pub use self::refresh::QuoteRefreshActivities;
pub use self::refund::RefundActivities;

use self::execution::*;
use self::hydration::*;
use self::refresh::*;
use self::refund::*;
use self::refund_builders::*;
