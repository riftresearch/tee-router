// Activity function constants are registered before the later rewrite slices invoke them.
use std::{future::Future, str::FromStr, sync::Arc, time::Instant};

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
        Database, ExecutionAttemptPlan, ExternalCustodyRefundAttemptPlan,
        FundingVaultRefundAttemptPlan, HyperliquidSpotRefundAttemptPlan,
        PersistStepCompletionRecord, RefreshedExecutionAttemptPlan,
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
        LoadManualInterventionContextInput, LoadOrderExecutionStateInput, ManualInterventionScope,
        ManualInterventionWorkflowContext, ManualResolutionSignalKind, MarkOrderCompletedInput,
        MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
        MaterializeRefundPlanInput, MaterializeRetryAttemptInput, MaterializedExecutionAttempt,
        OrderCompleted, OrderExecutionState, OrderTerminalStatus, OrderWorkflowPhase,
        PersistProviderOperationStatusInput, PersistProviderReceiptInput, PersistStepFailedInput,
        PersistStepReadyToFireInput, PersistenceBoundary, PollProviderOperationHintsInput,
        PreExecutionStaleQuoteCheck, PrepareManualInterventionRefundInput,
        PrepareManualInterventionRetryInput, ProviderHintKind, ProviderKind,
        ProviderOperationHintDecision, ProviderOperationHintEvidence, ProviderOperationHintSignal,
        ProviderOperationHintVerified, ProviderOperationHintsPolled, RawAmount,
        RecoverAcrossOnchainLogInput, RecoverablePositionKind, RefreshedAttemptMaterialized,
        RefreshedQuoteAttemptOutcome, RefreshedQuoteAttemptShape, RefundPlanOutcome,
        RefundPlanShape, RefundUntenableReason, ReleaseRefundManualInterventionInput,
        SettleProviderStepInput, SingleRefundPosition, SingleRefundPositionDiscovery,
        SingleRefundPositionOutcome, StaleQuoteRefreshUntenableReason, StaleRunningStepClassified,
        StaleRunningStepDecision, StepExecuted, StepExecutionOutcome, StepFailureDecision,
        VerifyProviderOperationHintInput, WorkflowExecutionStep, WriteFailedAttemptSnapshotInput,
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
