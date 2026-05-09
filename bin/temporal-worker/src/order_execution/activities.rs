#![allow(dead_code)]

// Activity function constants are registered before the later rewrite slices invoke them.
use std::{str::FromStr, sync::Arc};

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

use super::types::{
    AcrossOnchainLogRecovered, BoundaryPersisted, CancelTimedOutHyperliquidTradeInput,
    CheckPreExecutionStaleQuoteInput, ClassifyStaleRunningStepInput, ClassifyStepFailureInput,
    ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput,
    DispatchStepProviderActionInput, ExecuteStepInput, ExecutionPlan, FailedAttemptSnapshotWritten,
    FinalizeOrderOrRefundInput, FinalizedOrder, HyperliquidTradeCancelRecorded,
    LoadOrderExecutionStateInput, MarkOrderCompletedInput, MaterializeExecutionAttemptInput,
    MaterializeRefreshedAttemptInput, MaterializeRefundPlanInput, MaterializeRetryAttemptInput,
    MaterializedExecutionAttempt, OrderCompleted, OrderExecutionState, OrderTerminalStatus,
    OrderWorkflowPhase, PersistProviderOperationStatusInput, PersistProviderReceiptInput,
    PersistStepFailedInput, PersistStepReadyToFireInput, PersistStepTerminalStatusInput,
    PersistenceBoundary, PollProviderOperationHintsInput, PreExecutionStaleQuoteCheck,
    ProviderActionDispatchShape, ProviderHintKind, ProviderKind, ProviderOperationHintDecision,
    ProviderOperationHintEvidence, ProviderOperationHintSignal, ProviderOperationHintVerified,
    ProviderOperationHintsPolled, RecoverAcrossOnchainLogInput, RecoverablePositionKind,
    RefreshedAttemptMaterialized, RefreshedQuoteAttemptOutcome, RefreshedQuoteAttemptShape,
    RefundPlanOutcome, RefundPlanShape, RefundUntenableReason, SettleProviderStepInput,
    SingleRefundPosition, SingleRefundPositionDiscovery, SingleRefundPositionOutcome,
    StaleQuoteRefreshUntenableReason, StaleRunningStepClassified, StaleRunningStepDecision,
    StepExecuted, StepExecutionOutcome, StepFailureDecision, VerifyProviderOperationHintInput,
    WorkflowExecutionStep, WriteFailedAttemptSnapshotInput,
};

const MAX_EXECUTION_ATTEMPTS: i32 = 2;
const MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION: usize = 8;
const REFUND_PATH_MAX_DEPTH: usize = 5;
const REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;
const REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 =
    REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW;
const REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS: u64 = 12_500;
const REFRESH_PROBE_MAX_AMOUNT_IN: &str = "340282366920938463463374607431768211455";

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

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, ActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| activity_error("order activities are not configured"))
    }
}

#[activities]
impl OrderActivities {
    /// Scar tissue: §13 step type dispatch provider trait family mapping.
    #[activity]
    pub async fn execute_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ExecuteStepInput,
    ) -> Result<StepExecuted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .get_execution_step(input.step_id)
            .await
            .map_err(activity_error_from_display)?;
        if step.order_id != input.order_id || step.execution_attempt_id != Some(input.attempt_id) {
            return Err(activity_error(format!(
                "step {} does not belong to order {} attempt {}",
                step.id, input.order_id, input.attempt_id
            )));
        }
        if step.status != OrderExecutionStepStatus::Running {
            return Err(activity_error(format!(
                "step {} must be running before provider execution, got {}",
                step.id,
                step.status.to_db_string()
            )));
        }
        let checkpoint = json!({
            "kind": "provider_side_effect_about_to_fire",
            "reason": "about_to_fire_provider_side_effect",
            "step_id": step.id,
            "attempt_id": input.attempt_id,
            "recorded_at": Utc::now().to_rfc3339(),
            "scar_tissue": "§6"
        });
        let step = deps
            .db
            .orders()
            .record_execution_step_provider_side_effect_checkpoint(
                input.order_id,
                input.attempt_id,
                input.step_id,
                checkpoint,
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        let completion = execute_running_step(&deps, &step).await?;
        Ok(StepExecuted {
            order_id: input.order_id,
            attempt_id: input.attempt_id,
            step_id: input.step_id,
            response: completion.response,
            tx_hash: completion.tx_hash,
            provider_state: completion.provider_state,
            outcome: completion.outcome,
        })
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn mark_order_completed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MarkOrderCompletedInput,
    ) -> Result<OrderCompleted, ActivityError> {
        let deps = self.deps()?;
        let completed = deps
            .db
            .orders()
            .mark_execution_order_completed(input.order_id, input.attempt_id, Utc::now())
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %completed.order.id,
            attempt_id = %completed.attempt.id,
            event_name = "order.completed",
            "order.completed"
        );
        Ok(OrderCompleted {
            order_id: completed.order.id,
            attempt_id: completed.attempt.id,
        })
    }

    /// Scar tissue: §3 phase pass and §4 order/vault/step state alignment.
    #[activity]
    pub async fn load_order_execution_state(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadOrderExecutionStateInput,
    ) -> Result<OrderExecutionState, ActivityError> {
        let deps = self.deps()?;
        let order = deps
            .db
            .orders()
            .get(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let funding_vault_id = order.funding_vault_id.ok_or_else(|| {
            activity_error(format!(
                "order {} cannot execute without a funding vault",
                order.id
            ))
        })?;
        let source_vault = deps
            .db
            .vaults()
            .get(funding_vault_id)
            .await
            .map_err(activity_error_from_display)?;
        let order = if order.status == router_core::models::RouterOrderStatus::PendingFunding {
            let funded = deps
                .db
                .orders()
                .mark_order_funded_from_funded_vault(order.id, Utc::now())
                .await
                .map_err(activity_error_from_display)?;
            tracing::info!(
                order_id = %funded.id,
                event_name = "order.funded",
                "order.funded"
            );
            funded
        } else {
            order
        };
        let quote = deps
            .db
            .orders()
            .get_router_order_quote(order.id)
            .await
            .map_err(activity_error_from_display)?;
        let planned_at = Utc::now();
        let route = match quote {
            RouterOrderQuote::MarketOrder(quote) => deps
                .planner
                .plan(&order, &source_vault, &quote, planned_at)
                .map_err(activity_error_from_display)?,
            RouterOrderQuote::LimitOrder(quote) => deps
                .planner
                .plan_limit_order(&order, &source_vault, &quote, planned_at)
                .map_err(activity_error_from_display)?,
        };
        if route.legs.is_empty() || route.steps.is_empty() {
            return Err(activity_error(format!(
                "order {} execution plan has {} legs and {} steps",
                order.id,
                route.legs.len(),
                route.steps.len()
            )));
        }

        tracing::info!(
            order_id = %order.id,
            path_id = %route.path_id,
            leg_count = route.legs.len(),
            step_count = route.steps.len(),
            event_name = "order.execution_plan_started",
            "order.execution_plan_started"
        );
        Ok(OrderExecutionState {
            order_id: order.id,
            phase: OrderWorkflowPhase::WaitingForFunding,
            active_attempt_id: None,
            active_step_id: None,
            order,
            plan: ExecutionPlan {
                path_id: route.path_id,
                transition_decl_ids: route.transition_decl_ids,
                legs: route.legs,
                steps: route.steps,
            },
        })
    }

    /// Scar tissue: §2.1 `AfterExecutionLegsPersisted`, §3 phase 2, and §4 state alignment.
    #[activity]
    pub async fn materialize_execution_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeExecutionAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        let deps = self.deps()?;
        let mut plan = input.plan;
        plan.steps = hydrate_destination_execution_steps(&deps, input.order_id, plan.steps).await?;
        let record = deps
            .db
            .orders()
            .materialize_primary_execution_attempt(
                input.order_id,
                ExecutionAttemptPlan {
                    legs: plan.legs,
                    steps: plan.steps,
                },
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %record.order.id,
            attempt_id = %record.attempt.id,
            leg_count = record.legs.len(),
            step_count = record.steps.len(),
            event_name = "order.execution_plan_materialized",
            "order.execution_plan_materialized"
        );
        tracing::info!(
            order_id = %record.order.id,
            event_name = "order.executing",
            "order.executing"
        );
        Ok(MaterializedExecutionAttempt {
            attempt_id: record.attempt.id,
            steps: record
                .steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id,
                    step_index: step.step_index,
                })
                .collect(),
        })
    }

    /// Scar tissue: §5 retry attempt creation and suffix supersession.
    #[activity]
    pub async fn materialize_retry_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRetryAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        let deps = self.deps()?;
        let record = deps
            .db
            .orders()
            .create_retry_execution_attempt_from_failed_step(
                input.order_id,
                input.failed_attempt_id,
                input.failed_step_id,
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %record.order.id,
            attempt_id = %record.attempt.id,
            failed_attempt_id = %input.failed_attempt_id,
            failed_step_id = %input.failed_step_id,
            event_name = "order.execution_retry_materialized",
            "order.execution_retry_materialized"
        );
        Ok(MaterializedExecutionAttempt {
            attempt_id: record.attempt.id,
            steps: record
                .steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id,
                    step_index: step.step_index,
                })
                .collect(),
        })
    }

    /// Scar tissue: §2.1 `AfterExecutionLegsPersisted`.
    #[activity]
    pub async fn persist_step_ready_to_fire(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistStepReadyToFireInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .persist_execution_step_ready_to_fire(
                input.order_id,
                input.attempt_id,
                input.step_id,
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %input.order_id,
            attempt_id = %input.attempt_id,
            step_id = %step.id,
            step_index = step.step_index,
            event_name = "execution_step.started",
            "execution_step.started"
        );
        Ok(BoundaryPersisted {
            boundary: PersistenceBoundary::AfterExecutionLegsPersisted,
        })
    }

    /// Scar tissue: §2 boundary 2, `AfterStepMarkedFailed`.
    #[activity]
    pub async fn persist_step_failed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistStepFailedInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .persist_execution_step_failed(
                input.order_id,
                input.attempt_id,
                input.step_id,
                json!({ "error": input.failure_reason }),
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %input.order_id,
            attempt_id = %input.attempt_id,
            step_id = %step.id,
            event_name = "execution_step.failed",
            "execution_step.failed"
        );
        Ok(BoundaryPersisted {
            boundary: PersistenceBoundary::AfterStepMarkedFailed,
        })
    }

    /// Scar tissue: §2 boundary 3, `AfterExecutionStepStatusPersisted`.
    #[activity]
    pub async fn persist_step_terminal_status(
        _ctx: ActivityContext,
        _input: PersistStepTerminalStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR4b, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR4b: persist boundary 3")
    }

    /// Scar tissue: §2 boundary 4, `AfterProviderReceiptPersisted`.
    #[activity]
    pub async fn persist_provider_receipt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistProviderReceiptInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .get_execution_step(input.execution.step_id)
            .await
            .map_err(activity_error_from_display)?;
        let (operation, mut addresses) = provider_state_records(
            &step,
            &input.execution.provider_state,
            &input.execution.response,
        )
        .map_err(activity_error_from_display)?;
        if let Some(mut operation) = operation {
            operation.status = receipt_boundary_status(operation.status);
            let provider_operation_id = deps
                .db
                .orders()
                .upsert_provider_operation(&operation)
                .await
                .map_err(activity_error_from_display)?;
            for address in &mut addresses {
                address.provider_operation_id = Some(provider_operation_id);
                deps.db
                    .orders()
                    .upsert_provider_address(address)
                    .await
                    .map_err(activity_error_from_display)?;
            }
            tracing::info!(
                order_id = %input.execution.order_id,
                provider_operation_id = %provider_operation_id,
                event_name = "provider_operation.persisted",
                "provider_operation.persisted"
            );
        }
        Ok(BoundaryPersisted {
            boundary: PersistenceBoundary::AfterProviderReceiptPersisted,
        })
    }

    /// Scar tissue: §2 boundary 5, `AfterProviderOperationStatusPersisted`.
    #[activity]
    pub async fn persist_provider_operation_status(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistProviderOperationStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .get_execution_step(input.execution.step_id)
            .await
            .map_err(activity_error_from_display)?;
        let (operation, _) = provider_state_records(
            &step,
            &input.execution.provider_state,
            &input.execution.response,
        )
        .map_err(activity_error_from_display)?;
        if let Some(operation) = operation {
            let provider_operation_id = deps
                .db
                .orders()
                .upsert_provider_operation(&operation)
                .await
                .map_err(activity_error_from_display)?;
            let _ = deps
                .db
                .orders()
                .update_provider_operation_status(
                    provider_operation_id,
                    operation.status,
                    operation.provider_ref,
                    operation.observed_state,
                    Some(operation.response),
                    Utc::now(),
                )
                .await
                .map_err(activity_error_from_display)?;
        }
        Ok(BoundaryPersisted {
            boundary: PersistenceBoundary::AfterProviderOperationStatusPersisted,
        })
    }

    /// Scar tissue: §2 boundary 6, `AfterProviderStepSettlement`.
    #[activity]
    pub async fn settle_provider_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: SettleProviderStepInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        let deps = self.deps()?;
        if input.execution.outcome == StepExecutionOutcome::Waiting {
            settle_waiting_provider_step(&deps, &input.execution).await?;
            return Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterProviderStepSettlement,
            });
        }
        let record = deps
            .db
            .orders()
            .persist_execution_step_completion(PersistStepCompletionRecord {
                step_id: input.execution.step_id,
                operation: None,
                addresses: Vec::new(),
                response: input.execution.response,
                tx_hash: input.execution.tx_hash,
                usd_valuation: json!({}),
                completed_at: Utc::now(),
            })
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %input.execution.order_id,
            attempt_id = %input.execution.attempt_id,
            step_id = %record.step.id,
            event_name = "execution_step.completed",
            "execution_step.completed"
        );
        Ok(BoundaryPersisted {
            boundary: PersistenceBoundary::AfterProviderStepSettlement,
        })
    }

    /// Scar tissue: §5 retry/refund decision and §7 stale quote branch.
    #[activity]
    pub async fn classify_step_failure(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ClassifyStepFailureInput,
    ) -> Result<StepFailureDecision, ActivityError> {
        let deps = self.deps()?;
        let attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        if attempt.order_id != input.order_id {
            return Err(activity_error(format!(
                "attempt {} does not belong to order {}",
                attempt.id, input.order_id
            )));
        }
        let order_quote = deps
            .db
            .orders()
            .get_router_order_quote(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let failed_step = deps
            .db
            .orders()
            .get_execution_step(input.failed_step_id)
            .await
            .map_err(activity_error_from_display)?;
        if failed_step.order_id != input.order_id
            || failed_step.execution_attempt_id != Some(input.attempt_id)
        {
            return Err(activity_error(format!(
                "step {} does not belong to order {} attempt {}",
                failed_step.id, input.order_id, input.attempt_id
            )));
        }
        let refreshed_attempt_count = deps
            .db
            .orders()
            .get_execution_attempts(input.order_id)
            .await
            .map_err(activity_error_from_display)?
            .into_iter()
            .filter(|attempt| attempt.attempt_kind == OrderExecutionAttemptKind::RefreshedExecution)
            .count();
        let stale_quote_refresh_allowed = if matches!(order_quote, RouterOrderQuote::MarketOrder(_))
            // Scar §7: RefundRecovery attempts never stale-refresh. Funds may be
            // mid-flight during a refund, so stale refund quotes route to refund
            // manual intervention instead of being re-quoted.
            && attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery
            && refreshed_attempt_count < MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION
        {
            let legs = deps
                .db
                .orders()
                .get_execution_legs_for_attempt(input.attempt_id)
                .await
                .map_err(activity_error_from_display)?;
            failed_step
                .execution_leg_id
                .and_then(|leg_id| legs.into_iter().find(|leg| leg.id == leg_id))
                .and_then(|leg| leg.provider_quote_expires_at)
                .is_some_and(|expires_at| expires_at < Utc::now())
        } else {
            false
        };
        let decision = if stale_quote_refresh_allowed {
            StepFailureDecision::RefreshQuote
        } else if attempt.attempt_index < MAX_EXECUTION_ATTEMPTS {
            StepFailureDecision::RetryNewAttempt
        } else {
            StepFailureDecision::StartRefund
        };
        tracing::info!(
            order_id = %input.order_id,
            attempt_id = %input.attempt_id,
            failed_step_id = %input.failed_step_id,
            attempt_index = attempt.attempt_index,
            refreshed_attempt_count,
            decision = ?decision,
            event_name = "execution_step.failure_classified",
            "execution_step.failure_classified"
        );
        Ok(decision)
    }

    /// Scar tissue: §7 stale quote refresh. This mirrors the legacy
    /// refresh-before-step boundary: an expired, unstarted market-order leg
    /// refreshes before any external side effect is fired.
    #[activity]
    pub async fn check_pre_execution_stale_quote(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: CheckPreExecutionStaleQuoteInput,
    ) -> Result<PreExecutionStaleQuoteCheck, ActivityError> {
        let deps = self.deps()?;
        let attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        if attempt.order_id != input.order_id {
            return Err(activity_error(format!(
                "attempt {} does not belong to order {}",
                attempt.id, input.order_id
            )));
        }
        if attempt.status != OrderExecutionAttemptStatus::Active
            || attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery
        {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        }
        let order_quote = deps
            .db
            .orders()
            .get_router_order_quote(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        if !matches!(order_quote, RouterOrderQuote::MarketOrder(_)) {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        }
        let refreshed_attempt_count = deps
            .db
            .orders()
            .get_execution_attempts(input.order_id)
            .await
            .map_err(activity_error_from_display)?
            .into_iter()
            .filter(|attempt| attempt.attempt_kind == OrderExecutionAttemptKind::RefreshedExecution)
            .count();
        if refreshed_attempt_count >= MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        }
        let step = deps
            .db
            .orders()
            .get_execution_step(input.step_id)
            .await
            .map_err(activity_error_from_display)?;
        if step.order_id != input.order_id || step.execution_attempt_id != Some(input.attempt_id) {
            return Err(activity_error(format!(
                "step {} does not belong to order {} attempt {}",
                step.id, input.order_id, input.attempt_id
            )));
        }
        let has_provider_operation = deps
            .db
            .orders()
            .get_provider_operations(input.order_id)
            .await
            .map_err(activity_error_from_display)?
            .into_iter()
            .any(|operation| operation.execution_step_id == Some(step.id));
        let provider_side_effect_started = step
            .details
            .get("provider_side_effect_checkpoint")
            .is_some()
            || step.tx_hash.is_some()
            || step.provider_ref.is_some()
            || has_provider_operation;
        if step.status != OrderExecutionStepStatus::Running || provider_side_effect_started {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        }
        let Some(leg_id) = step.execution_leg_id else {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        };
        let legs = deps
            .db
            .orders()
            .get_execution_legs_for_attempt(input.attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        let Some(leg) = legs.into_iter().find(|leg| leg.id == leg_id) else {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        };
        let Some(provider_quote_expires_at) = leg.provider_quote_expires_at else {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        };
        if provider_quote_expires_at >= Utc::now() {
            return Ok(PreExecutionStaleQuoteCheck {
                should_refresh: false,
                reason: None,
            });
        }
        let reason = json!({
            "reason": "pre_execution_stale_provider_quote",
            "step_id": step.id,
            "execution_leg_id": leg.id,
            "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
            "refreshed_attempt_count": refreshed_attempt_count,
        });
        tracing::info!(
            order_id = %input.order_id,
            attempt_id = %input.attempt_id,
            step_id = %step.id,
            execution_leg_id = %leg.id,
            provider_quote_expires_at = %provider_quote_expires_at,
            event_name = "execution_step.pre_execution_stale_quote_detected",
            "execution_step.pre_execution_stale_quote_detected"
        );
        Ok(PreExecutionStaleQuoteCheck {
            should_refresh: true,
            reason: Some(reason),
        })
    }

    /// Scar tissue: §6 stale running step manual intervention.
    #[activity]
    pub async fn classify_stale_running_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ClassifyStaleRunningStepInput,
    ) -> Result<StaleRunningStepClassified, ActivityError> {
        let deps = self.deps()?;
        classify_stale_running_step_for_deps(&deps, input).await
    }

    /// Scar tissue: §15 failure snapshot.
    #[activity]
    pub async fn write_failed_attempt_snapshot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: WriteFailedAttemptSnapshotInput,
    ) -> Result<FailedAttemptSnapshotWritten, ActivityError> {
        let deps = self.deps()?;
        let order = deps
            .db
            .orders()
            .get(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let failed_step = deps
            .db
            .orders()
            .get_execution_step(input.failed_step_id)
            .await
            .map_err(activity_error_from_display)?;
        if failed_step.order_id != input.order_id
            || failed_step.execution_attempt_id != Some(input.attempt_id)
        {
            return Err(activity_error(format!(
                "step {} does not belong to order {} attempt {}",
                failed_step.id, input.order_id, input.attempt_id
            )));
        }
        let trigger_provider_operation_id = deps
            .db
            .orders()
            .get_provider_operations(input.order_id)
            .await
            .map_err(activity_error_from_display)?
            .into_iter()
            .filter(|operation| operation.execution_step_id == Some(failed_step.id))
            .map(|operation| (operation.created_at, operation.id))
            .max_by_key(|(created_at, _)| *created_at)
            .map(|(_, id)| id);
        let snapshot = failed_attempt_snapshot(&order, &failed_step);
        let failure_reason = json!({
            "reason": "execution_step_failed",
            "step_id": failed_step.id,
            "step_type": failed_step.step_type.to_db_string(),
            "step_error": &failed_step.error,
        });
        let attempt = match deps
            .db
            .orders()
            .mark_execution_attempt_failed(
                input.attempt_id,
                Some(failed_step.id),
                trigger_provider_operation_id,
                failure_reason,
                snapshot,
                Utc::now(),
            )
            .await
        {
            Ok(attempt) => attempt,
            Err(RouterCoreError::NotFound) => {
                let attempt = deps
                    .db
                    .orders()
                    .get_execution_attempt(input.attempt_id)
                    .await
                    .map_err(activity_error_from_display)?;
                if attempt.status != OrderExecutionAttemptStatus::Failed {
                    return Err(activity_error(format!(
                        "attempt {} was not active or failed while writing failure snapshot",
                        input.attempt_id
                    )));
                }
                attempt
            }
            Err(source) => return Err(activity_error_from_display(source)),
        };
        Ok(FailedAttemptSnapshotWritten {
            attempt_id: attempt.id,
            attempt_index: attempt.attempt_index,
            failed_step_id: failed_step.id,
        })
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn finalize_order_or_refund(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: FinalizeOrderOrRefundInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        let deps = self.deps()?;
        match input.terminal_status {
            OrderTerminalStatus::RefundRequired => {
                let order = deps
                    .db
                    .orders()
                    .mark_order_refund_required(input.order_id, Utc::now())
                    .await
                    .map_err(activity_error_from_display)?;
                tracing::info!(
                    order_id = %order.id,
                    event_name = "order.refund_required",
                    "order.refund_required"
                );
                Ok(FinalizedOrder {
                    order_id: order.id,
                    terminal_status: OrderTerminalStatus::RefundRequired,
                })
            }
            OrderTerminalStatus::Refunded => {
                let attempt_id = input.attempt_id.ok_or_else(|| {
                    activity_error("finalize refunded order requires a refund attempt id")
                })?;
                let completed = deps
                    .db
                    .orders()
                    .mark_order_refunded(input.order_id, attempt_id, Utc::now())
                    .await
                    .map_err(activity_error_from_display)?;
                tracing::info!(
                    order_id = %completed.order.id,
                    attempt_id = %completed.attempt.id,
                    event_name = "order.refunded",
                    "order.refunded"
                );
                Ok(FinalizedOrder {
                    order_id: completed.order.id,
                    terminal_status: OrderTerminalStatus::Refunded,
                })
            }
            OrderTerminalStatus::RefundManualInterventionRequired => {
                let order = deps
                    .db
                    .orders()
                    .mark_order_refund_manual_intervention_required(input.order_id, Utc::now())
                    .await
                    .map_err(activity_error_from_display)?;
                tracing::info!(
                    order_id = %order.id,
                    event_name = "order.refund_manual_intervention_required",
                    "order.refund_manual_intervention_required"
                );
                Ok(FinalizedOrder {
                    order_id: order.id,
                    terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                })
            }
            OrderTerminalStatus::ManualInterventionRequired => {
                let attempt_id = input.attempt_id.ok_or_else(|| {
                    activity_error("finalize manual intervention requires an execution attempt id")
                })?;
                let step_id = input.step_id.ok_or_else(|| {
                    activity_error("finalize manual intervention requires an execution step id")
                })?;
                let order = finalize_execution_manual_intervention(
                    &deps,
                    input.order_id,
                    attempt_id,
                    step_id,
                    input.reason.unwrap_or_else(|| {
                        json!({
                            "reason": "execution_manual_intervention_required",
                            "step_id": step_id,
                        })
                    }),
                )
                .await?;
                tracing::info!(
                    order_id = %order.id,
                    attempt_id = %attempt_id,
                    step_id = %step_id,
                    event_name = "order.execution_manual_intervention_required",
                    "order.execution_manual_intervention_required"
                );
                Ok(FinalizedOrder {
                    order_id: order.id,
                    terminal_status: OrderTerminalStatus::ManualInterventionRequired,
                })
            }
            terminal_status => Err(activity_error(format!(
                "finalize_order_or_refund does not handle {terminal_status:?} in PR6a"
            ))),
        }
    }
}

async fn classify_stale_running_step_for_deps(
    deps: &OrderActivityDeps,
    input: ClassifyStaleRunningStepInput,
) -> Result<StaleRunningStepClassified, ActivityError> {
    let step = deps
        .db
        .orders()
        .get_execution_step(input.step_id)
        .await
        .map_err(activity_error_from_display)?;
    if step.order_id != input.order_id || step.execution_attempt_id != Some(input.attempt_id) {
        return Err(activity_error(format!(
            "step {} does not belong to order {} attempt {}",
            step.id, input.order_id, input.attempt_id
        )));
    }

    let step_operations: Vec<_> = deps
        .db
        .orders()
        .get_provider_operations(input.order_id)
        .await
        .map_err(activity_error_from_display)?
        .into_iter()
        .filter(|operation| operation.execution_step_id == Some(step.id))
        .collect();

    let durable_operation = step_operations
        .iter()
        .filter(|operation| {
            matches!(
                operation.status,
                ProviderOperationStatus::Submitted | ProviderOperationStatus::WaitingExternal
            ) && provider_operation_has_checkpoint(operation)
        })
        .max_by_key(|operation| (operation.updated_at, operation.created_at, operation.id));

    let (decision, mut reason) = if let Some(operation) = durable_operation {
        (
            StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress,
            json!({
                "kind": "stale_running_step_recovery",
                "reason": StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress.reason_str(),
                "provider_operation_id": operation.id,
                "provider_operation_status": operation.status.to_db_string(),
            }),
        )
    } else if let Some(checkpoint) = step.details.get("provider_side_effect_checkpoint") {
        (
            StaleRunningStepDecision::AmbiguousExternalSideEffectWindow,
            json!({
                "kind": "stale_running_step_recovery",
                "reason": StaleRunningStepDecision::AmbiguousExternalSideEffectWindow.reason_str(),
                "checkpoint": checkpoint,
                "started_at": step.started_at,
                "updated_at": step.updated_at,
            }),
        )
    } else {
        (
            StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint,
            json!({
                "kind": "stale_running_step_recovery",
                "reason": StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint.reason_str(),
                "started_at": step.started_at,
                "updated_at": step.updated_at,
            }),
        )
    };

    if let Some(object) = reason.as_object_mut() {
        object.insert("step_id".to_string(), json!(step.id));
        object.insert("attempt_id".to_string(), json!(input.attempt_id));
    }

    let recorded = deps
        .db
        .orders()
        .record_stale_running_step_classification(
            input.order_id,
            input.attempt_id,
            input.step_id,
            reason.clone(),
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;
    tracing::info!(
        order_id = %input.order_id,
        attempt_id = %input.attempt_id,
        step_id = %recorded.id,
        decision = ?decision,
        event_name = "execution_step.stale_running_classified",
        "execution_step.stale_running_classified"
    );

    Ok(StaleRunningStepClassified {
        step_id: input.step_id,
        decision,
        reason,
    })
}

async fn finalize_execution_manual_intervention(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
    reason: Value,
) -> Result<RouterOrder, ActivityError> {
    let now = Utc::now();
    let step = deps
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .map_err(activity_error_from_display)?;
    if step.order_id != order_id || step.execution_attempt_id != Some(attempt_id) {
        return Err(activity_error(format!(
            "step {} does not belong to order {} attempt {}",
            step.id, order_id, attempt_id
        )));
    }

    let step_error = json!({
        "error": "execution step requires manual intervention",
        "reason": reason
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("execution_manual_intervention_required"),
        "manual_intervention": reason.clone(),
    });
    let failed_step = match step.status {
        OrderExecutionStepStatus::Running => deps
            .db
            .orders()
            .fail_execution_step(step.id, step_error, now)
            .await
            .map_err(activity_error_from_display)?,
        OrderExecutionStepStatus::Waiting => deps
            .db
            .orders()
            .fail_observed_execution_step(step.id, step_error, now)
            .await
            .map_err(activity_error_from_display)?,
        OrderExecutionStepStatus::Failed => step,
        status => {
            return Err(activity_error(format!(
                "step {} cannot be finalized as manual intervention from status {}",
                step.id,
                status.to_db_string()
            )));
        }
    };

    let order = deps
        .db
        .orders()
        .get(order_id)
        .await
        .map_err(activity_error_from_display)?;
    let failure_reason = json!({
        "reason": reason
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("execution_manual_intervention_required"),
        "step_id": failed_step.id,
        "step_type": failed_step.step_type.to_db_string(),
        "step_error": &failed_step.error,
        "manual_intervention": reason,
    });
    match deps
        .db
        .orders()
        .mark_execution_attempt_manual_intervention_required(
            attempt_id,
            failure_reason,
            failed_attempt_snapshot(&order, &failed_step),
            now,
        )
        .await
    {
        Ok(_) => {}
        Err(RouterCoreError::NotFound) => {
            let attempt = deps
                .db
                .orders()
                .get_execution_attempt(attempt_id)
                .await
                .map_err(activity_error_from_display)?;
            if attempt.status != OrderExecutionAttemptStatus::ManualInterventionRequired {
                return Err(activity_error(format!(
                    "attempt {} was not eligible for manual intervention finalization",
                    attempt_id
                )));
            }
        }
        Err(source) => return Err(activity_error_from_display(source)),
    }

    deps.db
        .orders()
        .mark_order_manual_intervention_required(order_id, now)
        .await
        .map_err(activity_error_from_display)
}

async fn settle_waiting_provider_step(
    deps: &OrderActivityDeps,
    execution: &StepExecuted,
) -> Result<(), ActivityError> {
    let operations = deps
        .db
        .orders()
        .get_provider_operations(execution.order_id)
        .await
        .map_err(activity_error_from_display)?;
    let operation = operations
        .iter()
        .filter(|operation| operation.execution_step_id == Some(execution.step_id))
        .max_by_key(|operation| (operation.updated_at, operation.created_at, operation.id));

    match operation.map(|operation| operation.status) {
        Some(ProviderOperationStatus::Completed) => {
            let operation = operation.expect("checked completed operation above");
            complete_observed_provider_step(deps, execution, operation).await?;
            tracing::info!(
                order_id = %execution.order_id,
                attempt_id = %execution.attempt_id,
                step_id = %execution.step_id,
                provider_operation_id = %operation.id,
                event_name = "execution_step.completed",
                "execution_step.completed"
            );
        }
        Some(ProviderOperationStatus::Failed | ProviderOperationStatus::Expired) => {
            let operation = operation.expect("checked failed operation above");
            let step = deps
                .db
                .orders()
                .fail_observed_execution_step(
                    execution.step_id,
                    json!({
                        "kind": "provider_status_update",
                        "provider": &operation.provider,
                        "operation_id": operation.id,
                        "provider_ref": &operation.provider_ref,
                        "status": operation.status.to_db_string(),
                        "observed_state": &operation.observed_state,
                    }),
                    Utc::now(),
                )
                .await;
            match step {
                Ok(_) => {}
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(execution.step_id)
                        .await
                        .map_err(activity_error_from_display)?;
                    if current.status != OrderExecutionStepStatus::Failed {
                        return Err(activity_error(format!(
                            "failed provider step {} is in unexpected status {}",
                            execution.step_id,
                            current.status.to_db_string()
                        )));
                    }
                }
                Err(source) => return Err(activity_error_from_display(source)),
            }
        }
        Some(
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal,
        )
        | None => {
            let waiting = deps
                .db
                .orders()
                .wait_execution_step(
                    execution.step_id,
                    execution.response.clone(),
                    execution.tx_hash.clone(),
                    Utc::now(),
                )
                .await;
            match waiting {
                Ok(_) => {
                    tracing::info!(
                        order_id = %execution.order_id,
                        attempt_id = %execution.attempt_id,
                        step_id = %execution.step_id,
                        event_name = "execution_step.waiting_external",
                        "execution_step.waiting_external"
                    );
                }
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(execution.step_id)
                        .await
                        .map_err(activity_error_from_display)?;
                    if !matches!(
                        current.status,
                        OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Completed
                    ) {
                        return Err(activity_error(format!(
                            "waiting provider step {} is in unexpected status {}",
                            execution.step_id,
                            current.status.to_db_string()
                        )));
                    }
                }
                Err(source) => return Err(activity_error_from_display(source)),
            }
        }
    }

    Ok(())
}

async fn complete_observed_provider_step(
    deps: &OrderActivityDeps,
    execution: &StepExecuted,
    operation: &OrderProviderOperation,
) -> Result<OrderExecutionStep, ActivityError> {
    let completed = deps
        .db
        .orders()
        .complete_observed_execution_step(
            execution.step_id,
            provider_operation_step_response(operation),
            provider_operation_tx_hash(operation),
            json!({}),
            Utc::now(),
        )
        .await;
    match completed {
        Ok(step) => Ok(step),
        Err(RouterCoreError::NotFound) => {
            let current = deps
                .db
                .orders()
                .get_execution_step(execution.step_id)
                .await
                .map_err(activity_error_from_display)?;
            if current.status == OrderExecutionStepStatus::Completed {
                Ok(current)
            } else {
                Err(activity_error(format!(
                    "completed provider operation {} could not settle step {} in status {}",
                    operation.id,
                    execution.step_id,
                    current.status.to_db_string()
                )))
            }
        }
        Err(source) => Err(activity_error_from_display(source)),
    }
}

fn provider_operation_step_response(operation: &OrderProviderOperation) -> Value {
    if !is_empty_json_object(&operation.response) {
        return operation.response.clone();
    }
    json!({
        "kind": "provider_status_update",
        "provider": &operation.provider,
        "operation_id": operation.id,
        "provider_ref": &operation.provider_ref,
        "observed_state": &operation.observed_state,
    })
}

fn provider_operation_tx_hash(operation: &OrderProviderOperation) -> Option<String> {
    operation
        .observed_state
        .get("tx_hash")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn is_empty_json_object(value: &Value) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}

struct StepCompletion {
    response: Value,
    tx_hash: Option<String>,
    provider_state: ProviderExecutionState,
    outcome: StepExecutionOutcome,
}

enum PostExecuteProvider {
    Bridge(Arc<dyn router_core::services::action_providers::BridgeProvider>),
    Exchange(Arc<dyn router_core::services::action_providers::ExchangeProvider>),
    Unit(Arc<dyn router_core::services::action_providers::UnitProvider>),
}

impl PostExecuteProvider {
    async fn post_execute(
        &self,
        provider_context: &Value,
        receipts: &[CustodyActionReceipt],
    ) -> Result<ProviderExecutionStatePatch, String> {
        match self {
            Self::Bridge(provider) => provider.post_execute(provider_context, receipts).await,
            Self::Exchange(provider) => provider.post_execute(provider_context, receipts).await,
            Self::Unit(provider) => provider.post_execute(provider_context, receipts).await,
        }
    }
}

async fn hydrate_destination_execution_steps(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    steps: Vec<OrderExecutionStep>,
) -> Result<Vec<OrderExecutionStep>, ActivityError> {
    let mut destination_execution_vault: Option<CustodyVault> = None;
    let mut hyperliquid_spot_vault: Option<CustodyVault> = None;
    let mut hydrated = Vec::with_capacity(steps.len());

    for mut step in steps {
        if json_string_equals(
            &step.request,
            "recipient_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step.output_asset.clone().ok_or_else(|| {
                activity_error(format!(
                    "destination execution recipient requires output_asset for step {}",
                    step.id
                ))
            })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            if step.request.get("recipient").is_some() {
                set_json_value(&mut step.request, "recipient", json!(vault.address));
            }
            if step.request.get("recipient_address").is_some() {
                set_json_value(&mut step.request, "recipient_address", json!(vault.address));
            }
            set_json_value(
                &mut step.request,
                "recipient_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "destination_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "destination_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "source_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step.input_asset.clone().ok_or_else(|| {
                activity_error(format!(
                    "destination execution source requires input_asset for step {}",
                    step.id
                ))
            })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "source_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "source_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "source_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "source_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "depositor_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step.input_asset.clone().ok_or_else(|| {
                activity_error(format!(
                    "destination execution depositor requires input_asset for step {}",
                    step.id
                ))
            })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "depositor_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(&mut step.request, "depositor_address", json!(vault.address));
            set_json_value(
                &mut step.details,
                "depositor_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "depositor_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "refund_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let asset = step.input_asset.clone().ok_or_else(|| {
                activity_error(format!(
                    "destination execution refund requires input_asset for step {}",
                    step.id
                ))
            })?;
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "refund_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(&mut step.request, "refund_address", json!(vault.address));
            set_json_value(
                &mut step.details,
                "refund_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "refund_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "hyperliquid_custody_vault_role",
            CustodyVaultRole::HyperliquidSpot.to_db_string(),
        ) {
            let vault =
                ensure_hyperliquid_spot_vault(deps, order_id, &mut hyperliquid_spot_vault).await?;
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
        }

        if json_string_equals(
            &step.request,
            "hyperliquid_custody_vault_role",
            CustodyVaultRole::DestinationExecution.to_db_string(),
        ) {
            let chain_id = step
                .request
                .get("hyperliquid_custody_vault_chain_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    activity_error(format!(
                        "destination execution Hyperliquid custody requires hyperliquid_custody_vault_chain_id for step {}",
                        step.id
                    ))
                })?;
            let asset_id = step
                .request
                .get("hyperliquid_custody_vault_asset_id")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    activity_error(format!(
                        "destination execution Hyperliquid custody requires hyperliquid_custody_vault_asset_id for step {}",
                        step.id
                    ))
                })?;
            let asset = DepositAsset {
                chain: ChainId::parse(chain_id).map_err(|source| {
                    activity_error(format!(
                        "step {} hyperliquid_custody_vault_chain_id is invalid: {source}",
                        step.id
                    ))
                })?,
                asset: AssetId::parse(asset_id).map_err(|source| {
                    activity_error(format!(
                        "step {} hyperliquid_custody_vault_asset_id is invalid: {source}",
                        step.id
                    ))
                })?,
            };
            let vault = ensure_destination_execution_vault(
                deps,
                order_id,
                &asset,
                &mut destination_execution_vault,
            )
            .await?;
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.request,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_id",
                json!(vault.id),
            );
            set_json_value(
                &mut step.details,
                "hyperliquid_custody_vault_address",
                json!(vault.address),
            );
        }

        hydrated.push(step);
    }

    Ok(hydrated)
}

async fn ensure_hyperliquid_spot_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    cache: &mut Option<CustodyVault>,
) -> Result<CustodyVault, ActivityError> {
    if let Some(vault) = cache.as_ref() {
        return Ok(vault.clone());
    }

    if let Some(vault) = find_hyperliquid_spot_vault(deps, order_id).await? {
        *cache = Some(vault.clone());
        return Ok(vault);
    }

    let created = deps
        .custody_action_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::HyperliquidSpot,
            CustodyVaultVisibility::Internal,
            ChainId::parse("hyperliquid").map_err(activity_error)?,
            None,
            json!({ "source": "order_workflow_plan_hydration" }),
        )
        .await;
    let vault = match created {
        Ok(vault) => vault,
        Err(CustodyActionError::Database { source }) if is_unique_violation(&source) => {
            find_hyperliquid_spot_vault(deps, order_id)
                .await?
                .ok_or_else(|| {
                    activity_error(format!(
                        "HyperliquidSpot custody vault unique violation for order {order_id} but re-read returned none"
                    ))
                })?
        }
        Err(source) => return Err(activity_error_from_display(source)),
    };
    *cache = Some(vault.clone());
    Ok(vault)
}

async fn find_hyperliquid_spot_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
) -> Result<Option<CustodyVault>, ActivityError> {
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .map_err(activity_error_from_display)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.role == CustodyVaultRole::HyperliquidSpot
            && vault.chain == ChainId::parse("hyperliquid").expect("valid hyperliquid chain id")
    }))
}

async fn ensure_destination_execution_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
    cache: &mut Option<CustodyVault>,
) -> Result<CustodyVault, ActivityError> {
    if let Some(vault) = cache.as_ref().filter(|vault| {
        vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
            && vault.role == CustodyVaultRole::DestinationExecution
    }) {
        return Ok(vault.clone());
    }

    if let Some(vault) = find_destination_execution_vault(deps, order_id, asset).await? {
        *cache = Some(vault.clone());
        return Ok(vault);
    }

    let created = deps
        .custody_action_executor
        .create_router_derived_vault(
            order_id,
            CustodyVaultRole::DestinationExecution,
            CustodyVaultVisibility::Internal,
            asset.chain.clone(),
            Some(asset.asset.clone()),
            json!({ "source": "order_workflow_plan_hydration" }),
        )
        .await;
    let vault = match created {
        Ok(vault) => vault,
        Err(CustodyActionError::Database { source }) if is_unique_violation(&source) => find_destination_execution_vault(deps, order_id, asset)
            .await?
            .ok_or_else(|| {
                activity_error(format!(
                    "custody vault unique violation for order {order_id} on {} {} but re-read returned none",
                    asset.chain.as_str(),
                    asset.asset.as_str()
                ))
            })?,
        Err(source) => return Err(activity_error_from_display(source)),
    };
    *cache = Some(vault.clone());
    Ok(vault)
}

async fn find_destination_execution_vault(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
) -> Result<Option<CustodyVault>, ActivityError> {
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(order_id)
        .await
        .map_err(activity_error_from_display)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.role == CustodyVaultRole::DestinationExecution
            && vault.chain == asset.chain
            && vault.asset.as_ref() == Some(&asset.asset)
    }))
}

fn json_string_equals(value: &Value, key: &'static str, expected: &'static str) -> bool {
    value.get(key).and_then(Value::as_str) == Some(expected)
}

fn provider_operation_has_checkpoint(operation: &OrderProviderOperation) -> bool {
    operation.provider_ref.is_some()
        || json_object_non_empty(&operation.response)
        || json_object_non_empty(&operation.observed_state)
}

fn json_object_non_empty(value: &Value) -> bool {
    value.as_object().is_some_and(|object| !object.is_empty())
}

fn is_unique_violation(err: &RouterCoreError) -> bool {
    matches!(
        err,
        RouterCoreError::DatabaseQuery { source }
            if source
                .as_database_error()
                .and_then(|db_err| db_err.code())
                .as_deref()
                == Some("23505")
    )
}

fn request_string_field(
    step: &OrderExecutionStep,
    field: &'static str,
) -> Result<String, ActivityError> {
    step.request
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| {
            activity_error(format!(
                "step {} refund request is missing string field {field}",
                step.id
            ))
        })
}

fn request_uuid_field(
    step: &OrderExecutionStep,
    field: &'static str,
) -> Result<Uuid, ActivityError> {
    let value = request_string_field(step, field)?;
    Uuid::parse_str(&value).map_err(|source| {
        activity_error(format!(
            "step {} refund request field {field} is not a uuid: {source}",
            step.id
        ))
    })
}

async fn hydrate_cctp_receive_request(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<Value, ActivityError> {
    let burn_transition_decl_id = step
        .request
        .get("burn_transition_decl_id")
        .and_then(Value::as_str)
        .ok_or_else(|| activity_error("CCTP receive step missing burn_transition_decl_id"))?;
    let operations = deps
        .db
        .orders()
        .get_provider_operations(step.order_id)
        .await
        .map_err(activity_error_from_display)?;
    let operation = operations.into_iter().rev().find(|operation| {
        operation.operation_type == ProviderOperationType::CctpBridge
            && operation.status == ProviderOperationStatus::Completed
            && operation
                .request
                .get("transition_decl_id")
                .and_then(Value::as_str)
                == Some(burn_transition_decl_id)
    });
    let Some(operation) = operation else {
        return Err(activity_error(format!(
            "CCTP receive step could not find completed burn operation for transition {burn_transition_decl_id}"
        )));
    };
    let cctp_state = operation
        .observed_state
        .get("provider_observed_state")
        .unwrap_or(&operation.observed_state);
    if let Some(decoded_amount) = cctp_state
        .get("decoded_message_body")
        .and_then(|body| body.get("amount"))
        .and_then(Value::as_str)
    {
        let planned_amount = step
            .request
            .get("amount")
            .and_then(Value::as_str)
            .ok_or_else(|| activity_error("CCTP receive step missing amount"))?;
        if decoded_amount != planned_amount {
            return Err(activity_error(format!(
                "CCTP receive amount {planned_amount} does not match attested burn amount {decoded_amount}"
            )));
        }
    }
    let message = cctp_state
        .get("message")
        .and_then(Value::as_str)
        .ok_or_else(|| activity_error("completed CCTP burn operation missing attested message"))?;
    let attestation = cctp_state
        .get("attestation")
        .and_then(Value::as_str)
        .ok_or_else(|| activity_error("completed CCTP burn operation missing attestation"))?;
    let mut request = step.request.clone();
    set_json_value(&mut request, "message", json!(message));
    set_json_value(&mut request, "attestation", json!(attestation));
    set_json_value(
        &mut request,
        "burn_provider_operation_id",
        json!(operation.id),
    );
    if let Some(tx_hash) = operation.provider_ref {
        set_json_value(&mut request, "burn_tx_hash", json!(tx_hash));
    }
    Ok(request)
}

async fn execute_running_step(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<StepCompletion, ActivityError> {
    let intent = match step.step_type {
        OrderExecutionStepType::WaitForDeposit => {
            return Err(activity_error(format!(
                "{} is not executable in PR4a OrderWorkflow",
                step.step_type.to_db_string()
            )));
        }
        OrderExecutionStepType::Refund => {
            let custody_vault_id = request_uuid_field(step, "source_custody_vault_id")?;
            let recipient_address = request_string_field(step, "recipient_address")?;
            let amount = request_string_field(step, "amount")?;
            let receipt = deps
                .custody_action_executor
                .execute(CustodyActionRequest {
                    custody_vault_id,
                    action: CustodyAction::Transfer {
                        to_address: recipient_address.clone(),
                        amount: amount.clone(),
                        bitcoin_fee_budget_sats: None,
                    },
                })
                .await
                .map_err(activity_error_from_display)?;
            return Ok(StepCompletion {
                response: json!({
                    "kind": "refund_transfer",
                    "recipient_address": recipient_address,
                    "amount": amount,
                    "tx_hash": &receipt.tx_hash,
                }),
                tx_hash: Some(receipt.tx_hash),
                provider_state: ProviderExecutionState::default(),
                outcome: StepExecutionOutcome::Completed,
            });
        }
        OrderExecutionStepType::AcrossBridge => {
            let request =
                BridgeExecutionRequest::across_from_value(&step.request).map_err(activity_error)?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::CctpBurn => {
            let request = BridgeExecutionRequest::cctp_burn_from_value(&step.request)
                .map_err(activity_error)?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::CctpReceive => {
            let request_json = hydrate_cctp_receive_request(deps, step).await?;
            let request = BridgeExecutionRequest::cctp_receive_from_value(&request_json)
                .map_err(activity_error)?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::HyperliquidBridgeDeposit => {
            let request =
                BridgeExecutionRequest::hyperliquid_bridge_deposit_from_value(&step.request)
                    .map_err(activity_error)?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::HyperliquidBridgeWithdrawal => {
            let request =
                BridgeExecutionRequest::hyperliquid_bridge_withdrawal_from_value(&step.request)
                    .map_err(activity_error)?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::UnitDeposit => {
            let request =
                UnitDepositStepRequest::from_value(&step.request).map_err(activity_error)?;
            deps.action_providers
                .unit(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_deposit(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::UnitWithdrawal => {
            let request =
                UnitWithdrawalStepRequest::from_value(&step.request).map_err(activity_error)?;
            deps.action_providers
                .unit(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_withdrawal(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::HyperliquidTrade => {
            let request = ExchangeExecutionRequest::hyperliquid_trade_from_value(&step.request)
                .map_err(activity_error)?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::HyperliquidLimitOrder => {
            let request =
                ExchangeExecutionRequest::hyperliquid_limit_order_from_value(&step.request)
                    .map_err(activity_error)?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(activity_error)?
        }
        OrderExecutionStepType::UniversalRouterSwap => {
            let request = ExchangeExecutionRequest::universal_router_swap_from_value(&step.request)
                .map_err(activity_error)?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(activity_error)?
        }
    };

    prepare_provider_completion(deps, step, intent).await
}

async fn prepare_provider_completion(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    intent: ProviderExecutionIntent,
) -> Result<StepCompletion, ActivityError> {
    match intent {
        ProviderExecutionIntent::ProviderOnly { response, state } => {
            let outcome = provider_only_outcome(step, state.operation.as_ref())
                .map_err(activity_error_from_display)?;
            Ok(StepCompletion {
                response: json!({
                    "kind": "provider_only",
                    "provider": &step.provider,
                    "response": response,
                }),
                tx_hash: None,
                provider_state: state,
                outcome,
            })
        }
        ProviderExecutionIntent::CustodyAction {
            custody_vault_id,
            action,
            provider_context,
            state,
        } => {
            let action_for_response = action.clone();
            let receipt = deps
                .custody_action_executor
                .execute(CustodyActionRequest {
                    custody_vault_id,
                    action,
                })
                .await
                .map_err(activity_error_from_display)?;
            let outcome = provider_operation_outcome(step, state.operation.as_ref())
                .map_err(activity_error_from_display)?;
            let observed_state = state
                .operation
                .as_ref()
                .and_then(|operation| operation.observed_state.clone());
            Ok(StepCompletion {
                response: json!({
                    "kind": "custody_action",
                    "provider": &step.provider,
                    "step_id": step.id,
                    "custody_vault_id": receipt.custody_vault_id,
                    "action": action_for_response,
                    "provider_context": provider_context,
                    "observed_state": observed_state,
                    "tx_hash": &receipt.tx_hash,
                }),
                tx_hash: Some(receipt.tx_hash),
                provider_state: state,
                outcome,
            })
        }
        ProviderExecutionIntent::CustodyActions {
            custody_vault_id,
            actions,
            provider_context,
            mut state,
        } => {
            let post_execute_provider = deps
                .action_providers
                .bridge(&step.provider)
                .map(PostExecuteProvider::Bridge)
                .or_else(|| {
                    deps.action_providers
                        .exchange(&step.provider)
                        .map(PostExecuteProvider::Exchange)
                })
                .or_else(|| {
                    deps.action_providers
                        .unit(&step.provider)
                        .map(PostExecuteProvider::Unit)
                })
                .ok_or_else(|| {
                    activity_error(format!(
                        "custody_actions intent for {} requires a configured provider",
                        step.provider
                    ))
                })?;
            let actions_for_response = actions.clone();
            let mut receipts = Vec::with_capacity(actions.len());
            for action in actions {
                let receipt = deps
                    .custody_action_executor
                    .execute(CustodyActionRequest {
                        custody_vault_id,
                        action,
                    })
                    .await
                    .map_err(activity_error_from_display)?;
                receipts.push(receipt);
            }
            let patch = post_execute_provider
                .post_execute(&provider_context, &receipts)
                .await
                .map_err(activity_error)?;
            apply_post_execute_patch(&mut state, patch, &step.provider)
                .map_err(activity_error_from_display)?;
            let tx_hashes: Vec<String> = receipts
                .iter()
                .map(|receipt| receipt.tx_hash.clone())
                .collect();
            let primary_tx_hash = receipts.last().map(|receipt| receipt.tx_hash.clone());
            let outcome = provider_operation_outcome(step, state.operation.as_ref())
                .map_err(activity_error_from_display)?;
            let observed_state = state
                .operation
                .as_ref()
                .and_then(|operation| operation.observed_state.clone());
            Ok(StepCompletion {
                response: json!({
                    "kind": "custody_actions",
                    "provider": &step.provider,
                    "step_id": step.id,
                    "custody_vault_id": custody_vault_id,
                    "actions": actions_for_response,
                    "provider_context": provider_context,
                    "observed_state": observed_state,
                    "tx_hashes": tx_hashes,
                }),
                tx_hash: primary_tx_hash,
                provider_state: state,
                outcome,
            })
        }
    }
}

fn provider_state_records(
    step: &OrderExecutionStep,
    state: &ProviderExecutionState,
    step_response: &Value,
) -> RouterCoreResult<(Option<OrderProviderOperation>, Vec<OrderProviderAddress>)> {
    let now = Utc::now();
    let operation = state
        .operation
        .as_ref()
        .map(|operation| {
            Ok::<OrderProviderOperation, RouterCoreError>(OrderProviderOperation {
                id: Uuid::now_v7(),
                order_id: step.order_id,
                execution_attempt_id: step.execution_attempt_id,
                execution_step_id: Some(step.id),
                provider: step.provider.clone(),
                operation_type: operation.operation_type,
                provider_ref: provider_operation_ref_for_persist(step, operation)?,
                status: operation.status,
                request: operation
                    .request
                    .clone()
                    .unwrap_or_else(|| step.request.clone()),
                response: operation
                    .response
                    .clone()
                    .unwrap_or_else(|| step_response.clone()),
                observed_state: operation
                    .observed_state
                    .clone()
                    .unwrap_or_else(|| json!({})),
                created_at: now,
                updated_at: now,
            })
        })
        .transpose()?;
    let addresses = state
        .addresses
        .iter()
        .map(|address| OrderProviderAddress {
            id: Uuid::now_v7(),
            order_id: step.order_id,
            execution_step_id: Some(step.id),
            provider_operation_id: None,
            provider: step.provider.clone(),
            role: address.role,
            chain: address.chain.clone(),
            asset: address.asset.clone(),
            address: address.address.clone(),
            memo: address.memo.clone(),
            expires_at: address.expires_at,
            metadata: address.metadata.clone().unwrap_or_else(|| json!({})),
            created_at: now,
            updated_at: now,
        })
        .collect();
    Ok((operation, addresses))
}

fn provider_operation_outcome(
    step: &OrderExecutionStep,
    operation: Option<&ProviderOperationIntent>,
) -> RouterCoreResult<StepExecutionOutcome> {
    match operation.map(|operation| operation.status) {
        None if step.step_type == OrderExecutionStepType::CctpReceive => {
            Ok(StepExecutionOutcome::Completed)
        }
        None => Err(RouterCoreError::InvalidData {
            message: format!(
                "{} provider intent is missing operation state",
                step.step_type.to_db_string()
            ),
        }),
        Some(ProviderOperationStatus::Completed) => Ok(StepExecutionOutcome::Completed),
        Some(
            ProviderOperationStatus::Planned
            | ProviderOperationStatus::Submitted
            | ProviderOperationStatus::WaitingExternal,
        ) => Ok(StepExecutionOutcome::Waiting),
        Some(ProviderOperationStatus::Failed | ProviderOperationStatus::Expired) => {
            Err(RouterCoreError::InvalidData {
                message: "provider returned a terminal failure operation state".to_string(),
            })
        }
    }
}

fn receipt_boundary_status(status: ProviderOperationStatus) -> ProviderOperationStatus {
    match status {
        ProviderOperationStatus::Completed
        | ProviderOperationStatus::Failed
        | ProviderOperationStatus::Expired => ProviderOperationStatus::Submitted,
        status => status,
    }
}

fn provider_only_outcome(
    step: &OrderExecutionStep,
    operation: Option<&ProviderOperationIntent>,
) -> RouterCoreResult<StepExecutionOutcome> {
    match operation {
        Some(operation) => provider_operation_outcome(step, Some(operation)),
        None => Ok(StepExecutionOutcome::Completed),
    }
}

fn provider_operation_ref_for_persist(
    step: &OrderExecutionStep,
    operation: &ProviderOperationIntent,
) -> RouterCoreResult<Option<String>> {
    let provider_ref = operation.provider_ref.clone().or_else(|| {
        if step.step_type == OrderExecutionStepType::AcrossBridge {
            None
        } else {
            step.provider_ref.clone()
        }
    });
    if operation.status == ProviderOperationStatus::Completed && provider_ref.is_none() {
        return Err(RouterCoreError::InvalidData {
            message: format!(
                "{} completed provider operation is missing provider_ref",
                step.step_type.to_db_string()
            ),
        });
    }
    Ok(provider_ref)
}

fn apply_post_execute_patch(
    state: &mut ProviderExecutionState,
    patch: ProviderExecutionStatePatch,
    provider: &str,
) -> RouterCoreResult<()> {
    let Some(operation) = state.operation.as_mut() else {
        return Err(RouterCoreError::InvalidData {
            message: format!(
                "custody_actions intent for {provider} must include a provider operation"
            ),
        });
    };
    if let Some(provider_ref) = patch.provider_ref {
        operation.provider_ref = Some(provider_ref);
    }
    if let Some(observed_state) = patch.observed_state {
        operation.observed_state = Some(observed_state);
    }
    if let Some(response) = patch.response {
        operation.response = Some(response);
    }
    if let Some(status) = patch.status {
        operation.status = status;
    }
    Ok(())
}

fn provider_not_configured(step: &OrderExecutionStep) -> ActivityError {
    activity_error(format!("{} provider is not configured", step.provider))
}

fn failed_attempt_snapshot(order: &RouterOrder, failed_step: &OrderExecutionStep) -> Value {
    let source_kind = if failed_step.request.get("source_custody_vault_id").is_some() {
        "external_custody"
    } else if failed_step
        .request
        .get("hyperliquid_custody_vault_id")
        .is_some()
    {
        "hyperliquid_custody"
    } else {
        "funding_vault"
    };
    let source_asset = failed_step
        .input_asset
        .clone()
        .or_else(|| failed_step.output_asset.clone())
        .map(|asset| {
            json!({
                "chain": asset.chain.as_str(),
                "asset": asset.asset.as_str(),
            })
        })
        .unwrap_or_else(|| {
            json!({
                "chain": order.source_asset.chain.as_str(),
                "asset": order.source_asset.asset.as_str(),
            })
        });

    json!({
        "source_kind": source_kind,
        "funding_vault_id": order.funding_vault_id,
        "failed_step_id": failed_step.id,
        "failed_step_index": failed_step.step_index,
        "failed_step_type": failed_step.step_type.to_db_string(),
        "amount_in": failed_step.amount_in,
        "min_amount_out": failed_step.min_amount_out,
        "source_asset": source_asset,
        "source_custody_vault_id": failed_step.request.get("source_custody_vault_id").cloned(),
        "source_custody_vault_role": failed_step.request.get("source_custody_vault_role").cloned(),
        "source_custody_vault_address": failed_step
            .request
            .get("source_custody_vault_address")
            .cloned(),
        "hyperliquid_custody_vault_id": failed_step
            .request
            .get("hyperliquid_custody_vault_id")
            .cloned(),
        "hyperliquid_custody_vault_role": failed_step
            .request
            .get("hyperliquid_custody_vault_role")
            .cloned(),
        "hyperliquid_custody_vault_address": failed_step
            .request
            .get("hyperliquid_custody_vault_address")
            .cloned(),
        "recipient_custody_vault_id": failed_step
            .request
            .get("recipient_custody_vault_id")
            .cloned(),
        "recipient_custody_vault_role": failed_step
            .request
            .get("recipient_custody_vault_role")
            .cloned(),
        "recipient": failed_step.request.get("recipient").cloned(),
        "revert_custody_vault_id": failed_step.request.get("revert_custody_vault_id").cloned(),
        "revert_custody_vault_role": failed_step.request.get("revert_custody_vault_role").cloned(),
        "revert_custody_vault_address": failed_step
            .details
            .get("revert_custody_vault_address")
            .cloned(),
    })
}

fn refreshed_market_order_quote_exact_in(
    order: &RouterOrder,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    legs: Vec<QuoteLeg>,
    amount_in: String,
    amount_out: String,
    expires_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        source_asset: original_quote.source_asset.clone(),
        destination_asset: original_quote.destination_asset.clone(),
        recipient_address: original_quote.recipient_address.clone(),
        provider_id: original_quote.provider_id.clone(),
        order_kind: original_quote.order_kind,
        amount_in,
        amount_out,
        min_amount_out: original_quote.min_amount_out.clone(),
        max_amount_in: None,
        slippage_bps: original_quote.slippage_bps,
        provider_quote: json!({
            "schema_version": 2,
            "planner": "transition_decl_v1",
            "path_id": original_quote
                .provider_quote
                .get("path_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| transitions
                    .iter()
                    .map(|transition| transition.id.as_str())
                    .collect::<Vec<_>>()
                    .join(">")),
            "transition_decl_ids": transitions
                .iter()
                .map(|transition| transition.id.clone())
                .collect::<Vec<_>>(),
            "transitions": transitions,
            "legs": legs,
            "gas_reimbursement": original_quote
                .provider_quote
                .get("gas_reimbursement")
                .cloned()
                .unwrap_or_else(|| json!({ "retention_actions": [] })),
            "refresh": {
                "schema_version": 1,
                "source_quote_id": original_quote.id,
                "source_quote_expires_at": original_quote.expires_at.to_rfc3339(),
            },
        }),
        usd_valuation: json!({}),
        expires_at,
        created_at,
    }
}

fn refreshed_market_order_quote_exact_out(
    order: &RouterOrder,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    legs: Vec<QuoteLeg>,
    amount_in: String,
    max_amount_in: String,
    expires_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        source_asset: original_quote.source_asset.clone(),
        destination_asset: original_quote.destination_asset.clone(),
        recipient_address: original_quote.recipient_address.clone(),
        provider_id: original_quote.provider_id.clone(),
        order_kind: original_quote.order_kind,
        amount_in,
        amount_out: original_quote.amount_out.clone(),
        min_amount_out: None,
        max_amount_in: Some(max_amount_in),
        slippage_bps: original_quote.slippage_bps,
        provider_quote: json!({
            "schema_version": 2,
            "planner": "transition_decl_v1",
            "path_id": original_quote
                .provider_quote
                .get("path_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| transitions
                    .iter()
                    .map(|transition| transition.id.as_str())
                    .collect::<Vec<_>>()
                    .join(">")),
            "transition_decl_ids": transitions
                .iter()
                .map(|transition| transition.id.clone())
                .collect::<Vec<_>>(),
            "transitions": transitions,
            "legs": legs,
            "gas_reimbursement": original_quote
                .provider_quote
                .get("gas_reimbursement")
                .cloned()
                .unwrap_or_else(|| json!({ "retention_actions": [] })),
            "refresh": {
                "schema_version": 1,
                "source_quote_id": original_quote.id,
                "source_quote_expires_at": original_quote.expires_at.to_rfc3339(),
            },
        }),
        usd_valuation: json!({}),
        expires_at,
        created_at,
    }
}

fn refresh_bridge_quote_transition_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Result<Vec<QuoteLeg>, ActivityError> {
    if transition.kind == MarketOrderTransitionKind::CctpBridge {
        return Ok(refresh_cctp_quote_transition_legs(transition, quote));
    }
    Ok(vec![QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: &quote.amount_in,
        amount_out: &quote.amount_out,
        expires_at: quote.expires_at,
        raw: quote.provider_quote.clone(),
    })])
}

fn refresh_exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, ActivityError> {
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    if kind == "spot_cross_token" {
        return refresh_spot_cross_token_quote_transition_legs(
            transition_decl_id,
            transition_kind,
            provider,
            quote,
        );
    }
    if kind != "universal_router_swap" {
        return Err(activity_error(format!(
            "unsupported refreshed exchange quote kind {kind:?}"
        )));
    }
    let input_asset = quote
        .provider_quote
        .get("input_asset")
        .ok_or_else(|| activity_error("universal router quote missing input_asset"))
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "input_asset").map_err(activity_error)
        })?;
    let output_asset = quote
        .provider_quote
        .get("output_asset")
        .ok_or_else(|| activity_error("universal router quote missing output_asset"))
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "output_asset").map_err(activity_error)
        })?;
    Ok(vec![QuoteLeg {
        transition_decl_id: transition_decl_id.to_string(),
        transition_parent_decl_id: transition_decl_id.to_string(),
        transition_kind,
        execution_step_type: execution_step_type_for_transition_kind(transition_kind),
        provider,
        input_asset,
        output_asset,
        amount_in: quote.amount_in.clone(),
        amount_out: quote.amount_out.clone(),
        expires_at: quote.expires_at,
        raw: quote.provider_quote.clone(),
    }])
}

fn refresh_cctp_quote_transition_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Vec<QuoteLeg> {
    let burn = QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: &quote.amount_in,
        amount_out: &quote.amount_out,
        expires_at: quote.expires_at,
        raw: refresh_cctp_quote_phase_raw(&quote.provider_quote, OrderExecutionStepType::CctpBurn),
    })
    .with_execution_step_type(OrderExecutionStepType::CctpBurn);

    let receive = QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.output.asset,
        output_asset: &transition.output.asset,
        amount_in: &quote.amount_out,
        amount_out: &quote.amount_out,
        expires_at: quote.expires_at,
        raw: refresh_cctp_quote_phase_raw(
            &quote.provider_quote,
            OrderExecutionStepType::CctpReceive,
        ),
    })
    .with_child_transition_id(format!("{}:receive", transition.id))
    .with_execution_step_type(OrderExecutionStepType::CctpReceive);

    vec![burn, receive]
}

fn refresh_cctp_quote_phase_raw(
    provider_quote: &Value,
    execution_step_type: OrderExecutionStepType,
) -> Value {
    let mut raw = provider_quote.clone();
    set_json_value(&mut raw, "bridge_kind", json!("cctp_bridge"));
    set_json_value(&mut raw, "kind", json!(execution_step_type.to_db_string()));
    set_json_value(
        &mut raw,
        "execution_step_type",
        json!(execution_step_type.to_db_string()),
    );
    raw
}

fn refresh_spot_cross_token_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, ActivityError> {
    let provider_name = provider.as_str();
    let legs = quote
        .provider_quote
        .get("legs")
        .and_then(Value::as_array)
        .ok_or_else(|| activity_error("exchange quote missing spot_cross_token legs"))?;
    legs.iter()
        .enumerate()
        .map(|(index, leg)| {
            let input_asset = leg
                .get("input_asset")
                .ok_or_else(|| activity_error("hyperliquid quote leg missing input_asset"))
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "input_asset").map_err(activity_error)
                })?;
            let output_asset = leg
                .get("output_asset")
                .ok_or_else(|| activity_error("hyperliquid quote leg missing output_asset"))
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "output_asset").map_err(activity_error)
                })?;
            Ok(QuoteLeg {
                transition_decl_id: format!("{transition_decl_id}:leg:{index}"),
                transition_parent_decl_id: transition_decl_id.to_string(),
                transition_kind,
                execution_step_type: execution_step_type_for_transition_kind(transition_kind),
                provider,
                input_asset,
                output_asset,
                amount_in: refresh_required_quote_leg_amount(provider_name, leg, "amount_in")?,
                amount_out: refresh_required_quote_leg_amount(provider_name, leg, "amount_out")?,
                expires_at: quote.expires_at,
                raw: leg.clone(),
            })
        })
        .collect()
}

fn refresh_required_quote_leg_amount(
    provider: &str,
    leg: &Value,
    field: &'static str,
) -> Result<String, ActivityError> {
    let amount = leg
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| activity_error(format!("{provider} quote leg missing {field}")))?;
    if amount.is_empty() {
        return Err(activity_error(format!(
            "{provider} quote leg has empty {field}"
        )));
    }
    Ok(amount.to_string())
}

fn refresh_unit_deposit_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
    raw: Value,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw,
    })
    .with_execution_step_type(OrderExecutionStepType::UnitDeposit)
}

fn refresh_unit_withdrawal_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    recipient_address: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: refresh_unit_withdrawal_quote_raw(recipient_address),
    })
    .with_execution_step_type(OrderExecutionStepType::UnitWithdrawal)
}

fn refresh_unit_withdrawal_quote_raw(recipient_address: &str) -> Value {
    json!({
        "recipient_address": recipient_address,
        "hyperliquid_core_activation_fee": refresh_hyperliquid_core_activation_fee_quote_json(),
    })
}

fn refresh_hyperliquid_core_activation_fee_quote_json() -> Value {
    json!({
        "kind": "first_transfer_to_new_hypercore_destination",
        "quote_asset": "USDC",
        "amount_raw": REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW.to_string(),
        "amount_decimal": "1",
        "source": "hyperliquid_activation_gas_fee",
    })
}

fn refresh_unit_deposit_fee_reserve(
    quote: &MarketOrderQuote,
    transition: &TransitionDecl,
) -> Result<Option<U256>, ActivityError> {
    if transition.kind != MarketOrderTransitionKind::UnitDeposit {
        return Ok(None);
    }
    let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
        return Ok(None);
    };
    for leg in legs {
        let parsed: QuoteLeg = serde_json::from_value(leg.clone())
            .map_err(|source| activity_error(format!("stored quote leg is invalid: {source}")))?;
        if parsed.parent_transition_id() != transition.id {
            continue;
        }
        let Some(amount) = parsed
            .raw
            .get("source_fee_reserve")
            .and_then(|value| value.get("amount"))
            .and_then(Value::as_str)
        else {
            return Ok(None);
        };
        return refresh_parse_u256_amount("source_fee_reserve.amount", amount).map(Some);
    }
    Ok(None)
}

fn refresh_bitcoin_fee_reserve_quote(fee_reserve: U256) -> Value {
    json!({
        "source_fee_reserve": {
            "kind": "bitcoin_miner_fee",
            "chain_id": "bitcoin",
            "asset": AssetId::Native.as_str(),
            "amount": fee_reserve.to_string(),
            "reserve_bps": REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS,
        }
    })
}

fn refresh_parse_u256_amount(field: &'static str, value: &str) -> Result<U256, ActivityError> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(activity_error(format!(
            "{field} must be a raw unsigned integer: {value:?}"
        )));
    }
    U256::from_str_radix(value, 10)
        .map_err(|source| activity_error(format!("{field} is not a valid raw amount: {source}")))
}

fn refresh_subtract_amount(
    field: &'static str,
    gross_amount: &str,
    subtract: U256,
    transition_id: &str,
    label: &'static str,
) -> Result<String, ActivityError> {
    let gross = refresh_parse_u256_amount(field, gross_amount)?;
    if subtract == U256::ZERO {
        return Ok(gross_amount.to_string());
    }
    if gross <= subtract {
        return Err(activity_error(format!(
            "{field} must exceed {label} {subtract} for transition {transition_id}"
        )));
    }
    gross
        .checked_sub(subtract)
        .ok_or_else(|| {
            activity_error(format!(
                "{field} minus {label} underflowed for transition {transition_id}"
            ))
        })
        .map(|amount| amount.to_string())
}

fn refresh_add_amount(
    field: &'static str,
    amount: &str,
    add: U256,
    label: &'static str,
) -> Result<String, ActivityError> {
    let amount = refresh_parse_u256_amount(field, amount)?;
    amount
        .checked_add(add)
        .ok_or_else(|| activity_error(format!("{field} plus {label} overflowed")))
        .map(|amount| amount.to_string())
}

fn refresh_reserve_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> Result<String, ActivityError> {
    refresh_subtract_amount(
        field,
        value,
        U256::from(REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW),
        "hyperliquid_trade",
        "Hyperliquid spot token transfer gas reserve",
    )
}

fn refresh_add_hyperliquid_spot_send_quote_gas_reserve(
    field: &'static str,
    value: &str,
) -> Result<String, ActivityError> {
    refresh_add_amount(
        field,
        value,
        U256::from(REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW),
        "Hyperliquid spot token transfer gas reserve",
    )
}

fn refresh_transition_min_amount_out(
    transitions: &[TransitionDecl],
    index: usize,
    final_min_amount_out: &Option<String>,
) -> Option<String> {
    if index + 1 == transitions.len() {
        final_min_amount_out.clone()
    } else {
        Some("1".to_string())
    }
}

fn refresh_exact_out_max_input(
    start_transition_index: usize,
    index: usize,
    available_amount: &str,
) -> Option<String> {
    if index == start_transition_index {
        Some(available_amount.to_string())
    } else {
        Some(REFRESH_PROBE_MAX_AMOUNT_IN.to_string())
    }
}

fn flatten_refresh_transition_legs(legs_per_transition: Vec<Vec<QuoteLeg>>) -> Vec<QuoteLeg> {
    let mut flattened = Vec::new();
    for mut transition_legs in legs_per_transition {
        flattened.append(&mut transition_legs);
    }
    flattened
}

fn refresh_remaining_exact_in_amount(
    legs: &[OrderExecutionLeg],
    attempts: &[OrderExecutionAttempt],
    stale_attempt: &OrderExecutionAttempt,
    stale_leg: &OrderExecutionLeg,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    start_transition_index: usize,
) -> Result<String, String> {
    let Some(previous_transition_index) = start_transition_index.checked_sub(1) else {
        return Ok(original_quote.amount_in.clone());
    };
    let previous_transition = transitions.get(previous_transition_index).ok_or_else(|| {
        format!(
            "refresh previous transition index {previous_transition_index} is outside transition path of length {}",
            transitions.len()
        )
    })?;
    let previous = legs
        .iter()
        .filter(|leg| {
            let Some(attempt_id) = leg.execution_attempt_id else {
                return false;
            };
            let Some(attempt) = attempts.iter().find(|attempt| attempt.id == attempt_id) else {
                return false;
            };
            // Keep refund-recovery attempts out of stale-quote refresh accounting;
            // scar §7 treats refund re-quotes as unsafe once funds may be mid-flight.
            attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery
                && attempt.attempt_index <= stale_attempt.attempt_index
                && leg.transition_decl_id.as_deref() == Some(previous_transition.id.as_str())
                && leg.output_asset == stale_leg.input_asset
        })
        .max_by(|left, right| {
            let left_attempt = attempts
                .iter()
                .find(|attempt| Some(attempt.id) == left.execution_attempt_id);
            let right_attempt = attempts
                .iter()
                .find(|attempt| Some(attempt.id) == right.execution_attempt_id);
            left_attempt
                .map(|attempt| attempt.attempt_index)
                .cmp(&right_attempt.map(|attempt| attempt.attempt_index))
                .then_with(|| left.completed_at.cmp(&right.completed_at))
                .then_with(|| left.updated_at.cmp(&right.updated_at))
                .then_with(|| left.created_at.cmp(&right.created_at))
        })
        .ok_or_else(|| {
            format!(
                "cannot refresh leg {} because transition {} has no completed prior leg for {} {}",
                stale_leg.id,
                previous_transition.id,
                stale_leg.input_asset.chain,
                stale_leg.input_asset.asset
            )
        })?;
    if previous.status != OrderExecutionStepStatus::Completed {
        return Err(format!(
            "cannot refresh leg {} because previous leg {} is {}",
            stale_leg.id,
            previous.id,
            previous.status.to_db_string()
        ));
    }
    previous.actual_amount_out.clone().ok_or_else(|| {
        format!(
            "cannot refresh leg {} because previous completed leg {} has no actual_amount_out",
            stale_leg.id, previous.id
        )
    })
}

fn refresh_step_request_string(
    steps: &[OrderExecutionStep],
    transition: &TransitionDecl,
    keys: &[&'static str],
) -> Option<String> {
    steps
        .iter()
        .filter(|step| refresh_step_matches_transition(step, transition))
        .find_map(|step| {
            keys.iter().find_map(|key| {
                step.request
                    .get(*key)
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
            })
        })
}

fn refresh_step_matches_transition(step: &OrderExecutionStep, transition: &TransitionDecl) -> bool {
    let Some(step_transition_id) = step.transition_decl_id.as_deref() else {
        return false;
    };
    step_transition_id == transition.id
        || step_transition_id
            .strip_prefix(transition.id.as_str())
            .is_some_and(|suffix| suffix.starts_with(':'))
}

fn refresh_source_address(
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, ActivityError> {
    let transition = &transitions[index];
    if let Some(value) = refresh_step_request_string(
        steps,
        transition,
        &[
            "depositor_address",
            "source_custody_vault_address",
            "hyperliquid_custody_vault_address",
        ],
    ) {
        return Ok(value);
    }
    if index == 0 {
        return Ok(source_vault.deposit_vault_address.clone());
    }
    Err(activity_error(format!(
        "cannot refresh transition {} because no source address is materialized",
        transition.id
    )))
}

fn refresh_bridge_recipient_address(
    order: &RouterOrder,
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, ActivityError> {
    let transition = &transitions[index];
    if transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit {
        return refresh_hyperliquid_bridge_deposit_account_address(
            source_vault,
            steps,
            transition,
            index,
        );
    }
    refresh_recipient_address(order, steps, transitions, index)
}

fn refresh_recipient_address(
    order: &RouterOrder,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, ActivityError> {
    let transition = &transitions[index];
    if let Some(value) =
        refresh_step_request_string(steps, transition, &["recipient", "recipient_address"])
    {
        return Ok(value);
    }
    if index + 1 == transitions.len() {
        return Ok(order.recipient_address.clone());
    }
    Err(activity_error(format!(
        "cannot refresh transition {} because no recipient address is materialized",
        transition.id
    )))
}

fn refresh_hyperliquid_bridge_deposit_account_address(
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transition: &TransitionDecl,
    index: usize,
) -> Result<String, ActivityError> {
    if let Some(value) = refresh_step_request_string(
        steps,
        transition,
        &[
            "source_custody_vault_address",
            "hyperliquid_custody_vault_address",
            "depositor_address",
        ],
    ) {
        return Ok(value);
    }
    if index == 0 {
        return Ok(source_vault.deposit_vault_address.clone());
    }
    Err(activity_error(format!(
        "cannot refresh transition {} because no Hyperliquid account/source address is materialized",
        transition.id
    )))
}

fn stale_quote_refresh_untenable(
    order_id: Uuid,
    stale_attempt_id: Uuid,
    failed_step_id: Uuid,
    message: impl ToString,
) -> RefreshedQuoteAttemptShape {
    RefreshedQuoteAttemptShape {
        outcome: RefreshedQuoteAttemptOutcome::Untenable {
            order_id,
            stale_attempt_id,
            failed_step_id,
            reason: StaleQuoteRefreshUntenableReason::StaleProviderQuoteRefreshUntenable {
                message: message.to_string(),
            },
        },
    }
}

fn parse_refresh_amount(field: &'static str, value: &str) -> Result<u128, ActivityError> {
    value.parse::<u128>().map_err(|source| {
        activity_error(format!(
            "refreshed quote {field} must be an unsigned integer: {source}"
        ))
    })
}

fn set_json_value(target: &mut Value, key: &'static str, value: Value) {
    if !target.is_object() {
        *target = json!({});
    }
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
}

fn activity_error(message: impl ToString) -> ActivityError {
    std::io::Error::other(message.to_string()).into()
}

fn activity_error_from_display(source: impl std::fmt::Display) -> ActivityError {
    activity_error(source)
}

#[derive(Clone, Default)]
pub struct RefundActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl RefundActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, ActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| activity_error("refund activities are not configured"))
    }
}

#[activities]
impl RefundActivities {
    /// Scar tissue: §8 refund position discovery and §16.1 balance reads.
    #[activity]
    pub async fn discover_single_refund_position(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DiscoverSingleRefundPositionInput,
    ) -> Result<SingleRefundPositionDiscovery, ActivityError> {
        let deps = self.deps()?;
        let failed_attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.failed_attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        if failed_attempt.order_id != input.order_id {
            return Err(activity_error(format!(
                "refund failed attempt {} does not belong to order {}",
                failed_attempt.id, input.order_id
            )));
        }
        let order = deps
            .db
            .orders()
            .get(input.order_id)
            .await
            .map_err(activity_error_from_display)?;

        let mut positions = Vec::new();
        if let Some(funding_vault_id) = order.funding_vault_id {
            let vault = match deps.db.vaults().get(funding_vault_id).await {
                Ok(vault) => Some(vault),
                Err(RouterCoreError::NotFound) => None,
                Err(source) => return Err(activity_error_from_display(source)),
            };
            if let Some(vault) = vault {
                let amount = deps
                    .custody_action_executor
                    .deposit_vault_balance_raw(&vault)
                    .await
                    .map_err(activity_error_from_display)?;
                if raw_amount_is_positive(&amount, "funding vault refund balance")? {
                    positions.push(SingleRefundPosition {
                        position_kind: RecoverablePositionKind::FundingVault,
                        owning_step_id: None,
                        funding_vault_id: Some(funding_vault_id),
                        custody_vault_id: None,
                        asset: vault.deposit_asset,
                        amount,
                        hyperliquid_coin: None,
                        hyperliquid_canonical: None,
                    });
                }
            }
        }

        let custody_vaults = deps
            .db
            .orders()
            .get_custody_vaults(order.id)
            .await
            .map_err(activity_error_from_display)?;
        for vault in custody_vaults {
            match vault.role {
                CustodyVaultRole::SourceDeposit => {}
                CustodyVaultRole::DestinationExecution => {
                    let Some(asset_id) = vault.asset.clone() else {
                        continue;
                    };
                    let amount = deps
                        .custody_action_executor
                        .custody_vault_balance_raw(&vault)
                        .await
                        .map_err(activity_error_from_display)?;
                    if raw_amount_is_positive(&amount, "external custody refund balance")? {
                        positions.push(SingleRefundPosition {
                            position_kind: RecoverablePositionKind::ExternalCustody,
                            owning_step_id: None,
                            funding_vault_id: None,
                            custody_vault_id: Some(vault.id),
                            asset: DepositAsset {
                                chain: vault.chain,
                                asset: asset_id,
                            },
                            amount,
                            hyperliquid_coin: None,
                            hyperliquid_canonical: None,
                        });
                    }
                }
                CustodyVaultRole::HyperliquidSpot => {
                    let balances = deps
                        .custody_action_executor
                        .inspect_hyperliquid_spot_balances(&vault)
                        .await
                        .map_err(activity_error_from_display)?;
                    for balance in balances {
                        let registry = deps.action_providers.asset_registry();
                        let Some((canonical, asset)) = registry
                            .hyperliquid_coin_asset(&balance.coin, Some(&order.source_asset.chain))
                        else {
                            continue;
                        };
                        let decimals = registry
                            .chain_asset(&asset)
                            .ok_or_else(|| {
                                activity_error(format!(
                                    "missing registered decimals for Hyperliquid refund asset {} {}",
                                    asset.chain, asset.asset
                                ))
                            })?
                            .decimals;
                        let Some(amount) = hyperliquid_refund_balance_amount_raw(
                            &balance.total,
                            &balance.hold,
                            decimals,
                        )?
                        else {
                            continue;
                        };
                        positions.push(SingleRefundPosition {
                            position_kind: RecoverablePositionKind::HyperliquidSpot,
                            owning_step_id: None,
                            funding_vault_id: None,
                            custody_vault_id: Some(vault.id),
                            asset,
                            amount,
                            hyperliquid_coin: Some(balance.coin),
                            hyperliquid_canonical: Some(canonical),
                        });
                    }
                }
            }
        }

        if positions.len() == 1 {
            return Ok(SingleRefundPositionDiscovery {
                outcome: SingleRefundPositionOutcome::Position(positions.remove(0)),
            });
        }
        Ok(refund_single_position_untenable(
            positions.len(),
            positions.len(),
        ))
    }

    /// Scar tissue: §9 refund tree and §16.1 refund materialisation.
    #[activity]
    pub async fn materialize_refund_plan(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRefundPlanInput,
    ) -> Result<RefundPlanShape, ActivityError> {
        let deps = self.deps()?;
        if input.position.position_kind == RecoverablePositionKind::ExternalCustody {
            return materialize_external_custody_refund_plan(&deps, input).await;
        }
        if input.position.position_kind == RecoverablePositionKind::HyperliquidSpot {
            return materialize_hyperliquid_spot_refund_plan(&deps, input).await;
        }
        let order = deps
            .db
            .orders()
            .get(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let Some(funding_vault_id) = order.funding_vault_id else {
            return Ok(refund_plan_untenable(
                RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
            ));
        };
        let vault = match deps.db.vaults().get(funding_vault_id).await {
            Ok(vault) => vault,
            Err(RouterCoreError::NotFound) => {
                return Ok(refund_plan_untenable(
                    RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
                ));
            }
            Err(source) => return Err(activity_error_from_display(source)),
        };
        let amount = deps
            .custody_action_executor
            .deposit_vault_balance_raw(&vault)
            .await
            .map_err(activity_error_from_display)?;
        if !raw_amount_is_positive(&amount, "funding vault refund balance")? {
            return Ok(refund_plan_untenable(
                RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
            ));
        }
        let record = deps
            .db
            .orders()
            .create_refund_attempt_from_funding_vault(
                input.order_id,
                input.failed_attempt_id,
                FundingVaultRefundAttemptPlan {
                    funding_vault_id,
                    amount,
                    failure_reason: json!({
                        "reason": "primary_execution_attempts_exhausted",
                        "trace": "refund_workflow",
                        "failed_attempt_id": input.failed_attempt_id,
                    }),
                    input_custody_snapshot: json!({
                        "schema_version": 1,
                        "source_kind": "funding_vault",
                        "funding_vault_id": funding_vault_id,
                    }),
                },
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %record.order.id,
            attempt_id = %record.attempt.id,
            step_count = record.steps.len(),
            event_name = "order.refund_plan_materialized",
            "order.refund_plan_materialized"
        );
        Ok(RefundPlanShape {
            outcome: RefundPlanOutcome::Materialized {
                refund_attempt_id: record.attempt.id,
                steps: record
                    .steps
                    .into_iter()
                    .map(|step| WorkflowExecutionStep {
                        step_id: step.id,
                        step_index: step.step_index,
                    })
                    .collect(),
            },
        })
    }
}

fn refund_single_position_untenable(
    position_count: usize,
    recoverable_position_count: usize,
) -> SingleRefundPositionDiscovery {
    SingleRefundPositionDiscovery {
        outcome: SingleRefundPositionOutcome::Untenable {
            reason: RefundUntenableReason::RefundRequiresSingleRecoverablePosition {
                position_count,
                recoverable_position_count,
            },
        },
    }
}

fn refund_plan_untenable(reason: RefundUntenableReason) -> RefundPlanShape {
    RefundPlanShape {
        outcome: RefundPlanOutcome::Untenable { reason },
    }
}

fn raw_amount_is_positive(value: &str, label: &'static str) -> Result<bool, ActivityError> {
    let trimmed = value.trim();
    if trimmed.is_empty() || !trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(activity_error(format!(
            "{label} is not a raw unsigned integer: {value:?}"
        )));
    }
    Ok(trimmed.as_bytes().iter().any(|digit| *digit != b'0'))
}

async fn materialize_external_custody_refund_plan(
    deps: &OrderActivityDeps,
    input: MaterializeRefundPlanInput,
) -> Result<RefundPlanShape, ActivityError> {
    let order = deps
        .db
        .orders()
        .get(input.order_id)
        .await
        .map_err(activity_error_from_display)?;
    let custody_vault_id = input.position.custody_vault_id.ok_or_else(|| {
        activity_error("external-custody refund position is missing custody_vault_id")
    })?;
    let vault = match deps.db.orders().get_custody_vault(custody_vault_id).await {
        Ok(vault) => vault,
        Err(RouterCoreError::NotFound) => {
            return Ok(refund_plan_untenable(
                RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
            ));
        }
        Err(source) => return Err(activity_error_from_display(source)),
    };
    if vault.order_id != Some(input.order_id) {
        return Err(activity_error(format!(
            "external-custody vault {} does not belong to order {}",
            vault.id, input.order_id
        )));
    }
    let Some(asset_id) = vault.asset.clone() else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };
    let asset = DepositAsset {
        chain: vault.chain.clone(),
        asset: asset_id,
    };
    let amount = deps
        .custody_action_executor
        .custody_vault_balance_raw(&vault)
        .await
        .map_err(activity_error_from_display)?;
    if !raw_amount_is_positive(&amount, "external custody refund balance")? {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    }

    let now = Utc::now();
    let maybe_plan = if asset == order.source_asset {
        Some(external_custody_direct_refund_steps(
            &order, &vault, &asset, &amount, now,
        ))
    } else {
        external_custody_refund_back_steps(deps, &order, &vault, &asset, &amount, now).await?
    };
    let Some((legs, mut steps)) = maybe_plan else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };
    steps = hydrate_destination_execution_steps(deps, input.order_id, steps).await?;
    let record = deps
        .db
        .orders()
        .create_refund_attempt_from_external_custody(
            input.order_id,
            input.failed_attempt_id,
            ExternalCustodyRefundAttemptPlan {
                source_custody_vault_id: vault.id,
                legs,
                steps,
                failure_reason: json!({
                    "reason": "primary_execution_attempts_exhausted",
                    "trace": "refund_workflow",
                    "failed_attempt_id": input.failed_attempt_id,
                }),
                input_custody_snapshot: json!({
                    "schema_version": 1,
                    "source_kind": "external_custody",
                    "custody_vault_id": vault.id,
                    "custody_vault_role": vault.role.to_db_string(),
                    "source_asset": {
                        "chain": asset.chain.as_str(),
                        "asset": asset.asset.as_str(),
                    },
                    "amount": amount,
                }),
            },
            now,
        )
        .await
        .map_err(activity_error_from_display)?;
    tracing::info!(
        order_id = %record.order.id,
        attempt_id = %record.attempt.id,
        step_count = record.steps.len(),
        event_name = "order.refund_plan_materialized",
        "order.refund_plan_materialized"
    );
    Ok(RefundPlanShape {
        outcome: RefundPlanOutcome::Materialized {
            refund_attempt_id: record.attempt.id,
            steps: record
                .steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id,
                    step_index: step.step_index,
                })
                .collect(),
        },
    })
}

async fn materialize_hyperliquid_spot_refund_plan(
    deps: &OrderActivityDeps,
    input: MaterializeRefundPlanInput,
) -> Result<RefundPlanShape, ActivityError> {
    let order = deps
        .db
        .orders()
        .get(input.order_id)
        .await
        .map_err(activity_error_from_display)?;
    let custody_vault_id = input.position.custody_vault_id.ok_or_else(|| {
        activity_error("HyperliquidSpot refund position is missing custody_vault_id")
    })?;
    let vault = match deps.db.orders().get_custody_vault(custody_vault_id).await {
        Ok(vault) => vault,
        Err(RouterCoreError::NotFound) => {
            return Ok(refund_plan_untenable(
                RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
            ));
        }
        Err(source) => return Err(activity_error_from_display(source)),
    };
    if vault.order_id != Some(input.order_id) {
        return Err(activity_error(format!(
            "HyperliquidSpot vault {} does not belong to order {}",
            vault.id, input.order_id
        )));
    }
    if vault.role != CustodyVaultRole::HyperliquidSpot {
        return Err(activity_error(format!(
            "custody vault {} is {}, not hyperliquid_spot",
            vault.id,
            vault.role.to_db_string()
        )));
    }

    let coin = input
        .position
        .hyperliquid_coin
        .clone()
        .ok_or_else(|| activity_error("HyperliquidSpot refund position is missing coin"))?;
    let canonical = input.position.hyperliquid_canonical.ok_or_else(|| {
        activity_error("HyperliquidSpot refund position is missing canonical asset")
    })?;
    let (asset, amount) =
        match current_hyperliquid_spot_refund_balance(deps, &order, &vault, &coin).await? {
            Some((current_canonical, asset, amount)) if current_canonical == canonical => {
                (asset, amount)
            }
            Some((current_canonical, _, _)) => {
                return Err(activity_error(format!(
                    "HyperliquidSpot refund coin {coin} canonical changed from {} to {}",
                    canonical.as_str(),
                    current_canonical.as_str()
                )));
            }
            None => {
                return Ok(refund_plan_untenable(
                    RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
                ));
            }
        };

    let now = Utc::now();
    let Some((legs, steps)) =
        hyperliquid_spot_refund_back_steps(deps, &order, &vault, canonical, &asset, &amount, now)
            .await?
    else {
        return Ok(refund_plan_untenable(
            RefundUntenableReason::RefundRecoverablePositionDisappearedAfterValidation,
        ));
    };
    let record = deps
        .db
        .orders()
        .create_refund_attempt_from_hyperliquid_spot(
            input.order_id,
            input.failed_attempt_id,
            HyperliquidSpotRefundAttemptPlan {
                source_custody_vault_id: vault.id,
                legs,
                steps,
                failure_reason: json!({
                    "reason": "primary_execution_attempts_exhausted",
                    "trace": "refund_workflow",
                    "failed_attempt_id": input.failed_attempt_id,
                }),
                input_custody_snapshot: json!({
                    "schema_version": 1,
                    "source_kind": "hyperliquid_spot",
                    "custody_vault_id": vault.id,
                    "source_asset": {
                        "chain": asset.chain.as_str(),
                        "asset": asset.asset.as_str(),
                    },
                    "hyperliquid_coin": coin,
                    "hyperliquid_canonical": canonical.as_str(),
                    "amount": amount,
                }),
            },
            now,
        )
        .await
        .map_err(activity_error_from_display)?;
    tracing::info!(
        order_id = %record.order.id,
        attempt_id = %record.attempt.id,
        step_count = record.steps.len(),
        event_name = "order.refund_plan_materialized",
        "order.refund_plan_materialized"
    );
    Ok(RefundPlanShape {
        outcome: RefundPlanOutcome::Materialized {
            refund_attempt_id: record.attempt.id,
            steps: record
                .steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id,
                    step_index: step.step_index,
                })
                .collect(),
        },
    })
}

async fn current_hyperliquid_spot_refund_balance(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    coin: &str,
) -> Result<Option<(CanonicalAsset, DepositAsset, String)>, ActivityError> {
    let balances = deps
        .custody_action_executor
        .inspect_hyperliquid_spot_balances(vault)
        .await
        .map_err(activity_error_from_display)?;
    let Some(balance) = balances.into_iter().find(|balance| balance.coin == coin) else {
        return Ok(None);
    };
    let registry = deps.action_providers.asset_registry();
    let Some((canonical, asset)) =
        registry.hyperliquid_coin_asset(&balance.coin, Some(&order.source_asset.chain))
    else {
        return Ok(None);
    };
    let decimals = registry
        .chain_asset(&asset)
        .ok_or_else(|| {
            activity_error(format!(
                "missing registered decimals for Hyperliquid refund asset {} {}",
                asset.chain, asset.asset
            ))
        })?
        .decimals;
    let Some(amount) =
        hyperliquid_refund_balance_amount_raw(&balance.total, &balance.hold, decimals)?
    else {
        return Ok(None);
    };
    Ok(Some((canonical, asset, amount)))
}

async fn hyperliquid_spot_refund_back_steps(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    canonical: CanonicalAsset,
    asset: &DepositAsset,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Option<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>)>, ActivityError> {
    let Some(quoted_path) =
        best_hyperliquid_spot_refund_quote(deps, order, canonical, amount).await?
    else {
        tracing::warn!(
            order_id = %order.id,
            source_asset_chain = %asset.chain,
            source_asset = %asset.asset,
            amount,
            event_name = "order.refund_plan_unavailable",
            "no PR6b7a HyperliquidSpot refund path is available"
        );
        return Ok(None);
    };

    materialize_hyperliquid_spot_refund_path(order, vault, &quoted_path, planned_at).map(Some)
}

async fn best_hyperliquid_spot_refund_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    canonical: CanonicalAsset,
    amount: &str,
) -> Result<Option<RefundQuotedPath>, ActivityError> {
    let start = MarketOrderNode::Venue {
        provider: ProviderId::Hyperliquid,
        canonical,
    };
    let goal = MarketOrderNode::External(order.source_asset.clone());
    let mut paths = deps
        .action_providers
        .asset_registry()
        .select_transition_paths_between(start, goal, REFUND_PATH_MAX_DEPTH)
        .into_iter()
        .filter(|path| {
            refund_path_compatible_with_position(
                RecoverablePositionKind::HyperliquidSpot,
                None,
                path,
            )
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        left.transitions
            .len()
            .cmp(&right.transitions.len())
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut best = None;
    for path in paths {
        let path_id = path.id.clone();
        match quote_hyperliquid_spot_refund_path(deps, order, amount, path).await {
            Ok(Some(quoted)) => {
                best = choose_better_refund_quote(quoted, best)?;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    order_id = %order.id,
                    path_id,
                    error = ?err,
                    "HyperliquidSpot refund transition-path quote failed"
                );
            }
        }
    }
    Ok(best)
}

async fn quote_hyperliquid_spot_refund_path(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    amount: &str,
    path: TransitionPath,
) -> Result<Option<RefundQuotedPath>, ActivityError> {
    let mut cursor_amount = amount.to_string();
    let mut legs = Vec::new();

    for transition in &path.transitions {
        match transition.kind {
            MarketOrderTransitionKind::HyperliquidTrade => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "exchange provider {} is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals: None,
                        output_decimals: None,
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: None,
                        recipient_address: order.refund_address.clone(),
                    })
                    .await
                    .map_err(activity_error_from_display)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_exchange_quote_transition_legs(
                    &transition.id,
                    transition.kind,
                    transition.provider,
                    &quote,
                )?);
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let unit = deps
                    .action_providers
                    .unit(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} unit provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                if !unit.supports_withdrawal(&transition.output.asset)
                    || !refund_unit_withdrawal_amount_meets_minimum(transition, &cursor_amount)?
                {
                    return Ok(None);
                }
                legs.push(refund_unit_withdrawal_quote_leg(
                    order,
                    transition,
                    &cursor_amount,
                    Utc::now() + chrono::Duration::minutes(10),
                ));
            }
            MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge => {
                let bridge = deps
                    .action_providers
                    .bridge(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} bridge provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = bridge
                    .quote_bridge(BridgeQuoteRequest {
                        source_asset: transition.input.asset.clone(),
                        destination_asset: transition.output.asset.clone(),
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        recipient_address: order.refund_address.clone(),
                        depositor_address: order.refund_address.clone(),
                        partial_fills_enabled: false,
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_bridge_quote_legs(transition, &quote)?);
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let bridge = deps
                    .action_providers
                    .bridge(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} bridge provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = bridge
                    .quote_bridge(BridgeQuoteRequest {
                        source_asset: transition.input.asset.clone(),
                        destination_asset: transition.output.asset.clone(),
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        recipient_address: order.refund_address.clone(),
                        depositor_address: order.refund_address.clone(),
                        partial_fills_enabled: false,
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_bridge_quote_legs(transition, &quote)?);
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} exchange provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals: refund_asset_decimals(deps, &transition.input.asset),
                        output_decimals: refund_asset_decimals(deps, &transition.output.asset),
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: None,
                        recipient_address: order.refund_address.clone(),
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_exchange_quote_transition_legs(
                    &transition.id,
                    transition.kind,
                    transition.provider,
                    &quote,
                )?);
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let unit = deps
                    .action_providers
                    .unit(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} unit provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                if !unit.supports_deposit(&transition.input.asset) {
                    return Ok(None);
                }
                legs.push(refund_unit_deposit_quote_leg(
                    transition,
                    &cursor_amount,
                    Utc::now() + chrono::Duration::minutes(10),
                ));
            }
        }
    }

    Ok(Some(RefundQuotedPath {
        path,
        amount_out: cursor_amount,
        legs,
    }))
}

fn refund_path_compatible_with_position(
    position_kind: RecoverablePositionKind,
    external_vault_role: Option<CustodyVaultRole>,
    path: &TransitionPath,
) -> bool {
    let Some(first) = path.transitions.first() else {
        return false;
    };
    match position_kind {
        RecoverablePositionKind::FundingVault => false,
        RecoverablePositionKind::ExternalCustody => match first.kind {
            MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge => true,
            MarketOrderTransitionKind::UniversalRouterSwap => {
                external_custody_universal_router_refund_is_supported_first_hop(path)
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                external_vault_role == Some(CustodyVaultRole::DestinationExecution)
            }
            MarketOrderTransitionKind::UnitDeposit => true,
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => false,
            _ => false,
        },
        RecoverablePositionKind::HyperliquidSpot => matches!(
            first.kind,
            MarketOrderTransitionKind::HyperliquidTrade | MarketOrderTransitionKind::UnitWithdrawal
        ),
    }
}

fn external_custody_universal_router_refund_is_supported_first_hop(path: &TransitionPath) -> bool {
    let Some(transition) = path.transitions.first() else {
        return false;
    };
    matches!(transition.from, MarketOrderNode::External(_))
        && matches!(transition.to, MarketOrderNode::External(_))
        && transition.input.asset.chain == transition.output.asset.chain
        && transition.input.asset.asset != transition.output.asset.asset
        && transition.input.asset.chain.evm_chain_id().is_some()
}

fn materialize_hyperliquid_spot_refund_path(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>), ActivityError> {
    let mut execution_legs = Vec::new();
    let mut steps = Vec::new();
    let mut step_index = 0_i32;
    let mut leg_index = 0_i32;

    for (transition_index, transition) in quoted_path.path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == quoted_path.path.transitions.len();
        let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
        if transition_legs.is_empty() {
            return Err(activity_error(format!(
                "quoted HyperliquidSpot refund path is missing legs for transition {}",
                transition.id
            )));
        }
        let transition_steps = match transition.kind {
            MarketOrderTransitionKind::HyperliquidTrade => {
                let custody = RefundHyperliquidBinding::Explicit {
                    vault_id: vault.id,
                    address: vault.address.clone(),
                    role: CustodyVaultRole::HyperliquidSpot,
                    asset: None,
                };
                let refund_quote_id = Uuid::now_v7();
                let leg_count = transition_legs.len();
                let mut trade_steps = Vec::with_capacity(leg_count);
                for (local_leg_index, leg) in transition_legs.iter().enumerate() {
                    trade_steps.push(refund_transition_hyperliquid_trade_step(
                        RefundHyperliquidTradeStepSpec {
                            order,
                            transition,
                            leg,
                            custody: &custody,
                            prefund_from_withdrawable: false,
                            refund_quote_id,
                            leg_index: local_leg_index,
                            leg_count,
                            step_index: step_index
                                .checked_add(i32::try_from(local_leg_index).map_err(|err| {
                                    activity_error(format!(
                                        "refund HyperliquidTrade step_index overflow: {err}"
                                    ))
                                })?)
                                .ok_or_else(|| {
                                    activity_error("refund HyperliquidTrade step_index overflow")
                                })?,
                            planned_at,
                        },
                    )?);
                }
                trade_steps
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UnitWithdrawal,
                )?;
                let custody = RefundHyperliquidBinding::Explicit {
                    vault_id: vault.id,
                    address: vault.address.clone(),
                    role: CustodyVaultRole::HyperliquidSpot,
                    asset: None,
                };
                vec![refund_transition_unit_withdrawal_step(
                    order, transition, &leg, &custody, is_final, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::HyperliquidBridgeDeposit,
                )?;
                vec![refund_transition_hyperliquid_bridge_deposit_step(
                    order,
                    RefundExternalSourceBinding::DerivedDestinationExecution,
                    transition,
                    &leg,
                    step_index,
                    planned_at,
                )?]
            }
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::HyperliquidBridgeWithdrawal,
                )?;
                let custody = RefundHyperliquidBinding::Explicit {
                    vault_id: vault.id,
                    address: vault.address.clone(),
                    role: CustodyVaultRole::HyperliquidSpot,
                    asset: None,
                };
                vec![refund_transition_hyperliquid_bridge_withdrawal_step(
                    order, transition, &leg, &custody, is_final, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::AcrossBridge => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::AcrossBridge,
                )?;
                vec![refund_transition_across_bridge_step(
                    order,
                    RefundExternalSourceBinding::DerivedDestinationExecution,
                    transition,
                    &leg,
                    is_final,
                    step_index,
                    planned_at,
                )?]
            }
            MarketOrderTransitionKind::CctpBridge => {
                let burn_leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::CctpBurn,
                )?;
                let receive_leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::CctpReceive,
                )?;
                refund_transition_cctp_bridge_steps(
                    order,
                    RefundExternalSourceBinding::DerivedDestinationExecution,
                    transition,
                    &burn_leg,
                    &receive_leg,
                    is_final,
                    step_index,
                    planned_at,
                )?
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UniversalRouterSwap,
                )?;
                vec![refund_transition_universal_router_swap_step(
                    order,
                    RefundExternalSourceBinding::DerivedDestinationExecution,
                    transition,
                    &leg,
                    is_final,
                    step_index,
                    planned_at,
                )?]
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UnitDeposit,
                )?;
                vec![refund_transition_unit_deposit_step(
                    order,
                    RefundExternalSourceBinding::DerivedDestinationExecution,
                    transition,
                    &leg,
                    step_index,
                    planned_at,
                )?]
            }
        };
        append_refund_transition_plan(
            order,
            transition,
            transition_legs,
            transition_steps,
            leg_index,
            planned_at,
            &mut execution_legs,
            &mut steps,
        )?;
        leg_index = leg_index
            .checked_add(1)
            .ok_or_else(|| activity_error("refund leg_index overflow"))?;
        step_index = i32::try_from(steps.len())
            .map_err(|err| activity_error(format!("refund step_index overflow: {err}")))?;
    }

    Ok((execution_legs, steps))
}

fn refund_unit_deposit_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: json!({}),
    })
}

fn refund_unit_withdrawal_quote_leg(
    order: &RouterOrder,
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: json!({
            "recipient_address": order.refund_address,
        }),
    })
}

fn refund_unit_withdrawal_amount_meets_minimum(
    transition: &TransitionDecl,
    amount: &str,
) -> Result<bool, ActivityError> {
    let minimum_amount = unit_withdrawal_minimum_raw(&transition.output.asset).to_string();
    refund_amount_gte(amount, &minimum_amount)
}

fn refund_hyperliquid_unit_withdrawal_leg(
    order: &RouterOrder,
    transition: &TransitionDecl,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    refund_unit_withdrawal_quote_leg(
        order,
        transition,
        amount,
        planned_at + chrono::Duration::minutes(10),
    )
}

#[derive(Debug, Clone)]
enum RefundExternalSourceBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
    },
    DerivedDestinationExecution,
}

fn refund_external_source_binding(
    vault: &CustodyVault,
    transition_index: usize,
) -> RefundExternalSourceBinding {
    if transition_index == 0 {
        RefundExternalSourceBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: vault.role,
        }
    } else {
        RefundExternalSourceBinding::DerivedDestinationExecution
    }
}

fn external_refund_source_binding(
    vault: &CustodyVault,
    transition_index: usize,
) -> RefundExternalSourceBinding {
    refund_external_source_binding(vault, transition_index)
}

fn hyperliquid_spot_unit_withdrawal_refund_steps(
    order: &RouterOrder,
    vault: &CustodyVault,
    transition: &TransitionDecl,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>), ActivityError> {
    let leg = refund_hyperliquid_unit_withdrawal_leg(order, transition, amount, planned_at);
    let custody = RefundHyperliquidBinding::Explicit {
        vault_id: vault.id,
        address: vault.address.clone(),
        role: CustodyVaultRole::HyperliquidSpot,
        asset: None,
    };
    let mut step = refund_transition_unit_withdrawal_step(
        order, transition, &leg, &custody, true, 0, planned_at,
    )?;
    let leg = refund_execution_leg_from_quote_legs(order, transition, &[leg], 0, planned_at)?;
    step.execution_leg_id = Some(leg.id);
    Ok((vec![leg], vec![step]))
}

async fn hyperliquid_spot_trade_refund_steps(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    transition: &TransitionDecl,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Option<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>)>, ActivityError> {
    let exchange = deps
        .action_providers
        .exchange(transition.provider.as_str())
        .ok_or_else(|| {
            activity_error(format!(
                "exchange provider {} is not configured",
                transition.provider.as_str()
            ))
        })?;
    let quote = exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: transition.input.asset.clone(),
            output_asset: transition.output.asset.clone(),
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: None,
            recipient_address: order.refund_address.clone(),
        })
        .await
        .map_err(activity_error_from_display)?;
    let Some(quote) = quote else {
        return Ok(None);
    };
    let quote_legs = refund_exchange_quote_transition_legs(
        &transition.id,
        transition.kind,
        transition.provider,
        &quote,
    )?;
    if quote_legs.is_empty() {
        return Ok(None);
    }
    let custody = RefundHyperliquidBinding::Explicit {
        vault_id: vault.id,
        address: vault.address.clone(),
        role: CustodyVaultRole::HyperliquidSpot,
        asset: None,
    };
    let refund_quote_id = Uuid::now_v7();
    let leg_count = quote_legs.len();
    let mut steps = Vec::with_capacity(leg_count);
    for (leg_index, leg) in quote_legs.iter().enumerate() {
        steps.push(refund_transition_hyperliquid_trade_step(
            RefundHyperliquidTradeStepSpec {
                order,
                transition,
                leg,
                custody: &custody,
                prefund_from_withdrawable: false,
                refund_quote_id,
                leg_index,
                leg_count,
                step_index: i32::try_from(leg_index).map_err(|err| {
                    activity_error(format!(
                        "refund HyperliquidTrade step_index overflow: {err}"
                    ))
                })?,
                planned_at,
            },
        )?);
    }
    let leg = refund_execution_leg_from_quote_legs(order, transition, &quote_legs, 0, planned_at)?;
    let leg_id = leg.id;
    for step in &mut steps {
        step.execution_leg_id = Some(leg_id);
    }
    Ok(Some((vec![leg], steps)))
}

fn external_custody_direct_refund_steps(
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> (Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>) {
    let leg = OrderExecutionLeg {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: None,
        leg_index: 0,
        leg_type: OrderExecutionStepType::Refund.to_db_string().to_string(),
        provider: "internal".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: asset.clone(),
        output_asset: asset.clone(),
        amount_in: amount.to_string(),
        expected_amount_out: amount.to_string(),
        min_amount_out: None,
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        provider_quote_expires_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_direct_transfer",
            "quote_leg_count": 1,
            "action_step_types": [OrderExecutionStepType::Refund.to_db_string()],
        }),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    let step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        execution_leg_id: Some(leg.id),
        transition_decl_id: None,
        step_index: 0,
        step_type: OrderExecutionStepType::Refund,
        provider: "internal".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: Some(asset.clone()),
        output_asset: Some(asset.clone()),
        amount_in: Some(amount.to_string()),
        min_amount_out: None,
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("order:{}:external-refund:0", order.id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_direct_transfer",
            "source_custody_vault_id": vault.id,
            "source_custody_vault_role": vault.role.to_db_string(),
            "recipient_address": &order.refund_address,
        }),
        request: json!({
            "order_id": order.id,
            "source_custody_vault_id": vault.id,
            "recipient_address": &order.refund_address,
            "amount": amount,
        }),
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    };
    (vec![leg], vec![step])
}

#[derive(Clone)]
struct RefundQuotedPath {
    path: TransitionPath,
    amount_out: String,
    legs: Vec<QuoteLeg>,
}

async fn external_custody_refund_back_steps(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Option<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>)>, ActivityError> {
    let Some(quoted_path) =
        best_external_custody_refund_quote(deps, order, vault, asset, amount).await?
    else {
        return Ok(None);
    };
    materialize_external_custody_refund_path(order, vault, &quoted_path, planned_at).map(Some)
}

fn materialize_external_custody_refund_path(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>), ActivityError> {
    let mut execution_legs = Vec::new();
    let mut steps = Vec::new();
    let mut step_index = 0_i32;
    let mut leg_index = 0_i32;

    for (transition_index, transition) in quoted_path.path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == quoted_path.path.transitions.len();
        let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
        if transition_legs.is_empty() {
            return Err(activity_error(format!(
                "quoted ExternalCustody refund path is missing legs for transition {}",
                transition.id
            )));
        }
        let source = external_refund_source_binding(vault, transition_index);
        let transition_steps = match transition.kind {
            MarketOrderTransitionKind::AcrossBridge => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::AcrossBridge,
                )?;
                vec![refund_transition_across_bridge_step(
                    order, source, transition, &leg, is_final, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::CctpBridge => {
                let burn_leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::CctpBurn,
                )?;
                let receive_leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::CctpReceive,
                )?;
                refund_transition_cctp_bridge_steps(
                    order,
                    source,
                    transition,
                    &burn_leg,
                    &receive_leg,
                    is_final,
                    step_index,
                    planned_at,
                )?
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UniversalRouterSwap,
                )?;
                vec![refund_transition_universal_router_swap_step(
                    order, source, transition, &leg, is_final, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::HyperliquidBridgeDeposit,
                )?;
                vec![refund_transition_hyperliquid_bridge_deposit_step(
                    order, source, transition, &leg, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::HyperliquidBridgeWithdrawal,
                )?;
                let custody = external_refund_hyperliquid_binding(
                    vault,
                    &quoted_path.path.transitions,
                    transition_index,
                )?;
                vec![refund_transition_hyperliquid_bridge_withdrawal_step(
                    order, transition, &leg, &custody, is_final, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UnitDeposit,
                )?;
                vec![refund_transition_unit_deposit_step(
                    order, source, transition, &leg, step_index, planned_at,
                )?]
            }
            MarketOrderTransitionKind::HyperliquidTrade => {
                let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
                if transition_legs.is_empty() {
                    return Err(activity_error(format!(
                        "quoted refund path is missing HyperliquidTrade legs for transition {}",
                        transition.id
                    )));
                }
                let custody = external_refund_hyperliquid_binding(
                    vault,
                    &quoted_path.path.transitions,
                    transition_index,
                )?;
                let prefund_first_trade = refund_trade_prefund_from_withdrawable(
                    &quoted_path.path.transitions,
                    transition_index,
                );
                let refund_quote_id = Uuid::now_v7();
                let leg_count = transition_legs.len();
                let mut trade_steps = Vec::with_capacity(leg_count);
                for (local_leg_index, leg) in transition_legs.iter().enumerate() {
                    trade_steps.push(refund_transition_hyperliquid_trade_step(
                        RefundHyperliquidTradeStepSpec {
                            order,
                            transition,
                            leg,
                            custody: &custody,
                            prefund_from_withdrawable: prefund_first_trade && local_leg_index == 0,
                            refund_quote_id,
                            leg_index: local_leg_index,
                            leg_count,
                            step_index: step_index
                                .checked_add(i32::try_from(local_leg_index).map_err(|err| {
                                    activity_error(format!(
                                        "refund HyperliquidTrade step_index overflow: {err}"
                                    ))
                                })?)
                                .ok_or_else(|| {
                                    activity_error("refund HyperliquidTrade step_index overflow")
                                })?,
                            planned_at,
                        },
                    )?);
                }
                trade_steps
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let leg = refund_take_one_leg(
                    &quoted_path.legs,
                    transition,
                    OrderExecutionStepType::UnitWithdrawal,
                )?;
                let custody = external_refund_hyperliquid_binding(
                    vault,
                    &quoted_path.path.transitions,
                    transition_index,
                )?;
                vec![refund_transition_unit_withdrawal_step(
                    order, transition, &leg, &custody, is_final, step_index, planned_at,
                )?]
            }
        };
        append_refund_transition_plan(
            order,
            transition,
            transition_legs,
            transition_steps,
            leg_index,
            planned_at,
            &mut execution_legs,
            &mut steps,
        )?;
        leg_index = leg_index
            .checked_add(1)
            .ok_or_else(|| activity_error("refund leg_index overflow"))?;
        step_index = i32::try_from(steps.len())
            .map_err(|err| activity_error(format!("refund step_index overflow: {err}")))?;
    }

    Ok((execution_legs, steps))
}

async fn best_external_custody_refund_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
) -> Result<Option<RefundQuotedPath>, ActivityError> {
    let mut paths = deps
        .action_providers
        .asset_registry()
        .select_transition_paths_between(
            MarketOrderNode::External(asset.clone()),
            MarketOrderNode::External(order.source_asset.clone()),
            REFUND_PATH_MAX_DEPTH,
        )
        .into_iter()
        .filter(|path| {
            refund_path_compatible_with_position(
                RecoverablePositionKind::ExternalCustody,
                Some(vault.role),
                path,
            )
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        left.transitions
            .len()
            .cmp(&right.transitions.len())
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut best = None;
    for path in paths {
        let path_id = path.id.clone();
        match quote_external_custody_refund_path(deps, order, vault, amount, path).await {
            Ok(Some(quoted)) => {
                best = choose_better_refund_quote(quoted, best)?;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    order_id = %order.id,
                    path_id,
                    error = ?err,
                    "refund transition-path quote failed"
                );
            }
        }
    }
    Ok(best)
}

async fn quote_external_custody_refund_path(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    amount: &str,
    path: TransitionPath,
) -> Result<Option<RefundQuotedPath>, ActivityError> {
    let mut cursor_amount = amount.to_string();
    let mut legs = Vec::new();

    for (transition_index, transition) in path.transitions.iter().enumerate() {
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let bridge = deps
                    .action_providers
                    .bridge(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} bridge provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = bridge
                    .quote_bridge(BridgeQuoteRequest {
                        source_asset: transition.input.asset.clone(),
                        destination_asset: transition.output.asset.clone(),
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        recipient_address: order.refund_address.clone(),
                        depositor_address: vault.address.clone(),
                        partial_fills_enabled: false,
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_bridge_quote_legs(transition, &quote)?);
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} exchange provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals: refund_asset_decimals(deps, &transition.input.asset),
                        output_decimals: refund_asset_decimals(deps, &transition.output.asset),
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: cursor_amount.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: Some(vault.address.clone()),
                        recipient_address: order.refund_address.clone(),
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_exchange_quote_transition_legs(
                    &transition.id,
                    transition.kind,
                    transition.provider,
                    &quote,
                )?);
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let unit = deps
                    .action_providers
                    .unit(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} unit provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                if !unit.supports_deposit(&transition.input.asset) {
                    return Ok(None);
                }
                legs.push(refund_unit_deposit_quote_leg(
                    transition,
                    &cursor_amount,
                    Utc::now() + chrono::Duration::minutes(10),
                ));
            }
            MarketOrderTransitionKind::HyperliquidTrade => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} exchange provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                let mut quote_amount_in = cursor_amount.clone();
                if transition_index > 0
                    && path.transitions[transition_index - 1].kind
                        == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                {
                    quote_amount_in = reserve_refund_hyperliquid_spot_send_quote_gas(
                        "refund.hyperliquid_trade.amount_in",
                        &quote_amount_in,
                    )?;
                }
                let quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals: None,
                        output_decimals: None,
                        order_kind: MarketOrderKind::ExactIn {
                            amount_in: quote_amount_in,
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: None,
                        recipient_address: order.refund_address.clone(),
                    })
                    .await
                    .map_err(activity_error)?;
                let Some(quote) = quote else {
                    return Ok(None);
                };
                cursor_amount = quote.amount_out.clone();
                legs.extend(refund_exchange_quote_transition_legs(
                    &transition.id,
                    transition.kind,
                    transition.provider,
                    &quote,
                )?);
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let unit = deps
                    .action_providers
                    .unit(transition.provider.as_str())
                    .ok_or_else(|| {
                        activity_error(format!(
                            "{} unit provider is not configured",
                            transition.provider.as_str()
                        ))
                    })?;
                if !unit.supports_withdrawal(&transition.output.asset)
                    || !refund_unit_withdrawal_amount_meets_minimum(transition, &cursor_amount)?
                {
                    return Ok(None);
                }
                legs.push(refund_unit_withdrawal_quote_leg(
                    order,
                    transition,
                    &cursor_amount,
                    Utc::now() + chrono::Duration::minutes(10),
                ));
            }
        }
    }

    Ok(Some(RefundQuotedPath {
        path,
        amount_out: cursor_amount,
        legs,
    }))
}

fn refund_asset_decimals(deps: &OrderActivityDeps, asset: &DepositAsset) -> Option<u8> {
    deps.action_providers
        .asset_registry()
        .chain_asset(asset)
        .map(|entry| entry.decimals)
        .or_else(|| (asset.chain.evm_chain_id().is_some() && asset.asset.is_native()).then_some(18))
}

fn choose_better_refund_quote(
    candidate: RefundQuotedPath,
    current: Option<RefundQuotedPath>,
) -> Result<Option<RefundQuotedPath>, ActivityError> {
    let Some(current) = current else {
        validate_refund_amount("refund.amount_out", &candidate.amount_out)?;
        return Ok(Some(candidate));
    };
    if refund_amount_gt(&candidate.amount_out, &current.amount_out)? {
        Ok(Some(candidate))
    } else {
        Ok(Some(current))
    }
}

fn refund_amount_gt(candidate: &str, current: &str) -> Result<bool, ActivityError> {
    let candidate = normalize_refund_amount("refund.amount_out", candidate)?;
    let current = normalize_refund_amount("refund.amount_out", current)?;
    Ok(candidate.len() > current.len() || candidate.len() == current.len() && candidate > current)
}

fn refund_amount_gte(value: &str, minimum: &str) -> Result<bool, ActivityError> {
    let value = normalize_refund_amount("refund.amount", value)?;
    let minimum = normalize_refund_amount("refund.minimum_amount", minimum)?;
    Ok(value.len() > minimum.len() || value.len() == minimum.len() && value >= minimum)
}

fn validate_refund_amount(field: &'static str, value: &str) -> Result<(), ActivityError> {
    normalize_refund_amount(field, value).map(|_| ())
}

fn normalize_refund_amount<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, ActivityError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(activity_error(format!(
            "invalid amount for {field}: expected unsigned decimal integer"
        )));
    }
    let normalized = value.trim_start_matches('0');
    Ok(if normalized.is_empty() {
        "0"
    } else {
        normalized
    })
}

fn refund_bridge_quote_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Result<Vec<QuoteLeg>, ActivityError> {
    match transition.kind {
        MarketOrderTransitionKind::AcrossBridge => Ok(vec![QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &transition.input.asset,
            output_asset: &transition.output.asset,
            amount_in: &quote.amount_in,
            amount_out: &quote.amount_out,
            expires_at: quote.expires_at,
            raw: quote.provider_quote.clone(),
        })]),
        MarketOrderTransitionKind::CctpBridge => {
            let burn = QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.input.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_in,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: cctp_refund_quote_leg_raw(
                    &quote.provider_quote,
                    OrderExecutionStepType::CctpBurn,
                ),
            });
            let receive = QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.output.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_out,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: cctp_refund_quote_leg_raw(
                    &quote.provider_quote,
                    OrderExecutionStepType::CctpReceive,
                ),
            })
            .with_child_transition_id(format!("{}:receive", transition.id))
            .with_execution_step_type(OrderExecutionStepType::CctpReceive);
            Ok(vec![burn, receive])
        }
        MarketOrderTransitionKind::HyperliquidBridgeDeposit
        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            Ok(vec![QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.input.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_in,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: quote.provider_quote.clone(),
            })
            .with_execution_step_type(
                execution_step_type_for_transition_kind(transition.kind),
            )])
        }
        other => Err(activity_error(format!(
            "transition kind {} does not produce bridge quote legs",
            other.as_str()
        ))),
    }
}

fn reserve_refund_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> Result<String, ActivityError> {
    let amount = normalize_refund_amount(field, value)?;
    let reserve = REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW.to_string();
    if !refund_amount_gt(amount, &reserve)? {
        return Err(activity_error(format!(
            "amount must exceed Hyperliquid spot token transfer gas reserve {reserve}"
        )));
    }
    subtract_refund_decimal_amount(amount, &reserve)
}

fn subtract_refund_decimal_amount(value: &str, subtrahend: &str) -> Result<String, ActivityError> {
    let mut digits = normalize_refund_amount("refund.amount", value)?
        .as_bytes()
        .to_vec();
    let subtrahend = normalize_refund_amount("refund.subtrahend", subtrahend)?.as_bytes();
    let mut borrow = 0_i16;

    for offset in 0..digits.len() {
        let digit_index = digits.len() - 1 - offset;
        let lhs = i16::from(digits[digit_index] - b'0') - borrow;
        let rhs = subtrahend
            .len()
            .checked_sub(1 + offset)
            .map(|index| i16::from(subtrahend[index] - b'0'))
            .unwrap_or(0);
        let (next_digit, next_borrow) = if lhs < rhs {
            (lhs + 10 - rhs, 1)
        } else {
            (lhs - rhs, 0)
        };
        digits[digit_index] = b'0'
            + u8::try_from(next_digit).map_err(|err| {
                activity_error(format!("refund amount subtraction digit overflow: {err}"))
            })?;
        borrow = next_borrow;
    }

    if borrow != 0 {
        return Err(activity_error(
            "Hyperliquid spot token transfer gas reserve exceeded amount",
        ));
    }
    let raw = String::from_utf8(digits)
        .map_err(|err| activity_error(format!("refund amount subtraction utf8: {err}")))?;
    let normalized = raw.trim_start_matches('0');
    Ok(if normalized.is_empty() {
        "0".to_string()
    } else {
        normalized.to_string()
    })
}

fn cctp_refund_quote_leg_raw(provider_quote: &Value, step_type: OrderExecutionStepType) -> Value {
    let mut raw = provider_quote.clone();
    set_json_value(&mut raw, "bridge_kind", json!("cctp_bridge"));
    set_json_value(
        &mut raw,
        "execution_step_type",
        json!(step_type.to_db_string()),
    );
    set_json_value(
        &mut raw,
        "kind",
        json!(match step_type {
            OrderExecutionStepType::CctpBurn => "cctp_burn",
            OrderExecutionStepType::CctpReceive => "cctp_receive",
            _ => "cctp_bridge",
        }),
    );
    raw
}

fn refund_take_one_leg(
    legs: &[QuoteLeg],
    transition: &TransitionDecl,
    step_type: OrderExecutionStepType,
) -> Result<QuoteLeg, ActivityError> {
    let mut matches = legs
        .iter()
        .filter(|leg| {
            leg.parent_transition_id() == transition.id
                && leg.transition_kind == transition.kind
                && leg.execution_step_type == step_type
        })
        .cloned()
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(activity_error(format!(
            "refund transition {} ({}) expected exactly one {:?} leg, found {}",
            transition.id,
            transition.kind.as_str(),
            step_type,
            matches.len()
        )));
    }
    Ok(matches.remove(0))
}

fn refund_legs_for_transition(legs: &[QuoteLeg], transition: &TransitionDecl) -> Vec<QuoteLeg> {
    legs.iter()
        .filter(|leg| {
            leg.parent_transition_id() == transition.id && leg.transition_kind == transition.kind
        })
        .cloned()
        .collect()
}

fn append_refund_transition_plan(
    order: &RouterOrder,
    transition: &TransitionDecl,
    quote_legs: Vec<QuoteLeg>,
    mut transition_steps: Vec<OrderExecutionStep>,
    leg_index: i32,
    planned_at: chrono::DateTime<Utc>,
    execution_legs: &mut Vec<OrderExecutionLeg>,
    steps: &mut Vec<OrderExecutionStep>,
) -> Result<(), ActivityError> {
    let leg = refund_execution_leg_from_quote_legs(
        order,
        transition,
        &quote_legs,
        leg_index,
        planned_at,
    )?;
    let leg_id = leg.id;
    for step in &mut transition_steps {
        step.execution_leg_id = Some(leg_id);
    }
    execution_legs.push(leg);
    steps.extend(transition_steps);
    Ok(())
}

fn refund_transition_across_bridge_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (depositor_id, depositor_address, depositor_role, refund_id, refund_address, refund_role) =
        match source {
            RefundExternalSourceBinding::Explicit {
                vault_id,
                address,
                role,
            } => (
                json!(vault_id),
                json!(address.clone()),
                json!(role.to_db_string()),
                json!(vault_id),
                json!(address),
                json!(role.to_db_string()),
            ),
            RefundExternalSourceBinding::DerivedDestinationExecution => (
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            ),
        };
    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund", order.id)),
        request: json!({
            "order_id": order.id,
            "origin_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "refund_address": refund_address,
            "refund_custody_vault_id": refund_id,
            "refund_custody_vault_role": refund_role,
            "partial_fills_enabled": false,
            "depositor_address": depositor_address,
            "depositor_custody_vault_id": depositor_id,
            "depositor_custody_vault_role": depositor_role,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_across_bridge",
            "transition_kind": transition.kind.as_str(),
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "depositor_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "depositor_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "recipient_address": if is_final { json!(&order.refund_address) } else { Value::Null },
        }),
        planned_at,
    }))
}

fn refund_transition_cctp_bridge_steps(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    burn_leg: &QuoteLeg,
    receive_leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, ActivityError> {
    let provider = refund_leg_provider(burn_leg, transition)?
        .as_str()
        .to_string();
    let receive_provider = refund_leg_provider(receive_leg, transition)?;
    if receive_provider.as_str() != provider {
        return Err(activity_error(format!(
            "CCTP receive leg provider {} does not match burn leg provider {} for transition {}",
            receive_provider.as_str(),
            provider,
            transition.id
        )));
    }
    let amount_in = burn_leg.amount_in.clone();
    let amount_out = receive_leg.amount_out.clone();
    let max_fee = burn_leg
        .raw
        .get("max_fee")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            activity_error(format!(
                "CCTP burn leg {} missing raw.max_fee",
                burn_leg.transition_decl_id
            ))
        })?
        .to_string();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };
    let burn = refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(burn_leg.transition_decl_id.clone()),
        step_index,
        step_type: OrderExecutionStepType::CctpBurn,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund:cctp", order.id)),
        request: json!({
            "order_id": order.id,
            "transition_decl_id": transition.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "source_custody_vault_id": source_id,
            "source_custody_vault_address": source_address,
            "source_custody_vault_role": source_role,
            "max_fee": max_fee,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_cctp_bridge",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "receive_step_index": step_index + 1,
        }),
        planned_at,
    });
    let receive = refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(receive_leg.transition_decl_id.clone()),
        step_index: step_index + 1,
        step_type: OrderExecutionStepType::CctpReceive,
        provider,
        input_asset: Some(transition.output.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_out.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund:cctp", order.id)),
        request: json!({
            "order_id": order.id,
            "burn_transition_decl_id": transition.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "source_custody_vault_id": null,
            "source_custody_vault_address": null,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "message": "",
            "attestation": "",
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_cctp_bridge",
            "burn_step_index": step_index,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "source_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    });
    Ok(vec![burn, receive])
}

fn refund_transition_universal_router_swap_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let swap_leg = &leg.raw;
    let order_kind = refund_required_str(swap_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let input_asset = leg.input_deposit_asset().map_err(activity_error)?;
    let output_asset = leg.output_deposit_asset().map_err(activity_error)?;
    let input_decimals = refund_required_u8(swap_leg, "src_decimals")?;
    let output_decimals = refund_required_u8(swap_leg, "dest_decimals")?;
    let price_route = swap_leg
        .get("price_route")
        .cloned()
        .ok_or_else(|| activity_error("universal router refund swap leg missing price_route"))?;
    let min_amount_out = swap_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = swap_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let slippage_bps = swap_leg.get("slippage_bps").and_then(Value::as_u64);
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UniversalRouterSwap,
        provider,
        input_asset: Some(input_asset.clone()),
        output_asset: Some(output_asset.clone()),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: Some(format!("order:{}:external-refund:swap", order.id)),
        request: json!({
            "order_id": order.id,
            "quote_id": Uuid::now_v7(),
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": input_asset.chain.as_str(),
                "asset": input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": output_asset.chain.as_str(),
                "asset": output_asset.asset.as_str(),
            },
            "input_decimals": input_decimals,
            "output_decimals": output_decimals,
            "price_route": price_route,
            "slippage_bps": slippage_bps,
            "source_custody_vault_id": source_id,
            "source_custody_vault_address": source_address,
            "source_custody_vault_role": source_role,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_universal_router_swap",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "recipient_address": if is_final { json!(&order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
        }),
        planned_at,
    }))
}

fn refund_transition_unit_deposit_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role, revert_id, revert_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(vault_id),
            json!(role.to_db_string()),
        ),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UnitDeposit,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: None,
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "src_chain_id": transition.input.asset.chain.as_str(),
            "dst_chain_id": "hyperliquid",
            "asset_id": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
            "revert_custody_vault_id": revert_id,
            "revert_custody_vault_role": revert_role,
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_address": null,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    }))
}

fn refund_transition_hyperliquid_bridge_deposit_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeDeposit,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
        }),
        planned_at,
    }))
}

fn refund_transition_hyperliquid_bridge_withdrawal_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    custody: &RefundHyperliquidBinding,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let amount_out = leg.amount_out.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);
    let transfer_from_spot = hyperliquid_binding_transfers_from_spot(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeWithdrawal,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: Some(amount_out),
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "transfer_from_spot": transfer_from_spot,
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

#[derive(Debug, Clone)]
enum RefundHyperliquidBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
        asset: Option<DepositAsset>,
    },
    DerivedSpot,
    DerivedDestinationExecution {
        asset: DepositAsset,
    },
}

fn external_refund_hyperliquid_binding(
    vault: &CustodyVault,
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> Result<RefundHyperliquidBinding, ActivityError> {
    let prior_transitions = transitions.get(..transition_index).ok_or_else(|| {
        activity_error(format!(
            "refund transition index {transition_index} is out of bounds"
        ))
    })?;

    if transitions.first().map(|transition| transition.kind)
        == Some(MarketOrderTransitionKind::HyperliquidBridgeDeposit)
        && prior_transitions
            .iter()
            .all(|transition| transition.kind != MarketOrderTransitionKind::AcrossBridge)
    {
        let asset_id = vault.asset.clone().ok_or_else(|| {
            activity_error(format!(
                "external custody vault {} is missing asset for Hyperliquid binding",
                vault.id
            ))
        })?;
        return Ok(RefundHyperliquidBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: vault.role,
            asset: Some(DepositAsset {
                chain: vault.chain.clone(),
                asset: asset_id,
            }),
        });
    }

    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
    {
        return Ok(RefundHyperliquidBinding::DerivedSpot);
    }

    let Some(bridge_transition) = prior_transitions
        .iter()
        .find(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
    else {
        return Err(activity_error(format!(
            "refund transition {} has no preceding Hyperliquid custody source",
            transitions[transition_index].id
        )));
    };

    Ok(RefundHyperliquidBinding::DerivedDestinationExecution {
        asset: bridge_transition.input.asset.clone(),
    })
}

fn refund_trade_prefund_from_withdrawable(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> bool {
    let prior_transitions = &transitions[..transition_index];
    prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
        && prior_transitions
            .iter()
            .filter(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidTrade)
            .count()
            == 0
}

fn hyperliquid_binding_request_parts(
    custody: &RefundHyperliquidBinding,
) -> (Value, Value, Value, Value, Value) {
    match custody {
        RefundHyperliquidBinding::Explicit {
            vault_id,
            address,
            role,
            asset,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(asset.as_ref().map(|asset| asset.chain.as_str())),
            json!(asset.as_ref().map(|asset| asset.asset.as_str())),
        ),
        RefundHyperliquidBinding::DerivedSpot => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
            Value::Null,
            Value::Null,
        ),
        RefundHyperliquidBinding::DerivedDestinationExecution { asset } => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            json!(asset.chain.as_str()),
            json!(asset.asset.as_str()),
        ),
    }
}

fn hyperliquid_binding_role(custody: &RefundHyperliquidBinding) -> Value {
    match custody {
        RefundHyperliquidBinding::Explicit { role, .. } => json!(role.to_db_string()),
        RefundHyperliquidBinding::DerivedSpot => {
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string())
        }
        RefundHyperliquidBinding::DerivedDestinationExecution { .. } => {
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        }
    }
}

fn hyperliquid_binding_status(custody: &RefundHyperliquidBinding) -> Value {
    match custody {
        RefundHyperliquidBinding::Explicit { .. } => json!("bound"),
        RefundHyperliquidBinding::DerivedSpot
        | RefundHyperliquidBinding::DerivedDestinationExecution { .. } => {
            json!("pending_derivation")
        }
    }
}

fn hyperliquid_binding_transfers_from_spot(custody: &RefundHyperliquidBinding) -> bool {
    match custody {
        RefundHyperliquidBinding::Explicit { role, .. } => {
            *role == CustodyVaultRole::HyperliquidSpot
        }
        RefundHyperliquidBinding::DerivedSpot => true,
        RefundHyperliquidBinding::DerivedDestinationExecution { .. } => false,
    }
}

struct RefundHyperliquidTradeStepSpec<'a> {
    order: &'a RouterOrder,
    transition: &'a TransitionDecl,
    leg: &'a QuoteLeg,
    custody: &'a RefundHyperliquidBinding,
    prefund_from_withdrawable: bool,
    refund_quote_id: Uuid,
    leg_index: usize,
    leg_count: usize,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
}

struct RefundPlannedStepSpec {
    order_id: Uuid,
    transition_decl_id: Option<String>,
    step_index: i32,
    step_type: OrderExecutionStepType,
    provider: String,
    input_asset: Option<DepositAsset>,
    output_asset: Option<DepositAsset>,
    amount_in: Option<String>,
    min_amount_out: Option<String>,
    provider_ref: Option<String>,
    request: Value,
    details: Value,
    planned_at: chrono::DateTime<Utc>,
}

fn refund_transition_hyperliquid_trade_step(
    spec: RefundHyperliquidTradeStepSpec<'_>,
) -> Result<OrderExecutionStep, ActivityError> {
    let RefundHyperliquidTradeStepSpec {
        order,
        transition,
        leg,
        custody,
        prefund_from_withdrawable,
        refund_quote_id,
        leg_index,
        leg_count,
        step_index,
        planned_at,
    } = spec;
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let trade_leg = &leg.raw;
    let order_kind = refund_required_str(trade_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let min_amount_out = trade_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = trade_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let input_asset = leg.input_deposit_asset().map_err(activity_error)?;
    let output_asset = leg.output_deposit_asset().map_err(activity_error)?;
    let request_input_asset = input_asset.clone();
    let request_output_asset = output_asset.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidTrade,
        provider,
        input_asset: Some(input_asset),
        output_asset: Some(output_asset),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: Some(format!("refund-quote-{refund_quote_id}")),
        request: json!({
            "order_id": order.id,
            "quote_id": refund_quote_id,
            "leg_index": leg_index,
            "leg_count": leg_count,
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": request_input_asset.chain.as_str(),
                "asset": request_input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": request_output_asset.chain.as_str(),
                "asset": request_output_asset.asset.as_str(),
            },
            "prefund_from_withdrawable": prefund_from_withdrawable,
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "leg_index": leg_index,
            "leg_count": leg_count,
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

fn refund_transition_unit_withdrawal_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    custody: &RefundHyperliquidBinding,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, ActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let amount_out = leg.amount_out.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UnitWithdrawal,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in),
        min_amount_out: Some("0".to_string()),
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "input_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "dst_chain_id": transition.output.asset.chain.as_str(),
            "asset_id": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "min_amount_out": "0",
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

fn refund_planned_step(spec: RefundPlannedStepSpec) -> OrderExecutionStep {
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: spec.order_id,
        execution_attempt_id: None,
        execution_leg_id: None,
        transition_decl_id: spec.transition_decl_id,
        step_index: spec.step_index,
        step_type: spec.step_type,
        provider: spec.provider,
        status: OrderExecutionStepStatus::Planned,
        input_asset: spec.input_asset,
        output_asset: spec.output_asset,
        amount_in: spec.amount_in,
        min_amount_out: spec.min_amount_out,
        tx_hash: None,
        provider_ref: spec.provider_ref,
        idempotency_key: None,
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: spec.details,
        request: spec.request,
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: spec.planned_at,
        updated_at: spec.planned_at,
    }
}

fn refund_execution_leg_from_quote_legs(
    order: &RouterOrder,
    transition: &TransitionDecl,
    quote_legs: &[QuoteLeg],
    leg_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionLeg, ActivityError> {
    let first = quote_legs.first().ok_or_else(|| {
        activity_error(format!(
            "refund transition {} ({}) has no quoted legs to materialize",
            transition.id,
            transition.kind.as_str()
        ))
    })?;
    let last = quote_legs.last().ok_or_else(|| {
        activity_error(format!(
            "refund transition {} ({}) has no quoted legs to materialize",
            transition.id,
            transition.kind.as_str()
        ))
    })?;
    let input_asset = first.input_deposit_asset().map_err(activity_error)?;
    let output_asset = last.output_deposit_asset().map_err(activity_error)?;
    let provider = quote_legs
        .iter()
        .map(|leg| leg.provider.as_str())
        .reduce(|first, next| if first == next { first } else { "multi" })
        .unwrap_or("unknown")
        .to_string();
    let min_amount_out = last
        .raw
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    Ok(OrderExecutionLeg {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: Some(transition.id.clone()),
        leg_index,
        leg_type: transition.kind.as_str().to_string(),
        provider,
        status: OrderExecutionStepStatus::Planned,
        input_asset,
        output_asset,
        amount_in: first.amount_in.clone(),
        expected_amount_out: last.amount_out.clone(),
        min_amount_out,
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        provider_quote_expires_at: quote_legs.iter().map(|leg| leg.expires_at).min(),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "transition_kind": transition.kind.as_str(),
            "quote_leg_count": quote_legs.len(),
            "quote_leg_transition_decl_ids": quote_legs
                .iter()
                .map(|leg| leg.transition_decl_id.clone())
                .collect::<Vec<_>>(),
            "action_step_types": quote_legs
                .iter()
                .map(|leg| leg.execution_step_type.to_db_string())
                .collect::<Vec<_>>(),
        }),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    })
}

fn refund_exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, ActivityError> {
    let provider_name = provider.as_str();
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    match kind {
        "spot_no_op" => Ok(vec![]),
        "universal_router_swap" => {
            let input_asset = quote
                .provider_quote
                .get("input_asset")
                .ok_or_else(|| activity_error("refund universal router quote missing input_asset"))
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "input_asset").map_err(activity_error)
                })?;
            let output_asset = quote
                .provider_quote
                .get("output_asset")
                .ok_or_else(|| activity_error("refund universal router quote missing output_asset"))
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "output_asset").map_err(activity_error)
                })?;
            Ok(vec![QuoteLeg {
                transition_decl_id: transition_decl_id.to_string(),
                transition_parent_decl_id: transition_decl_id.to_string(),
                transition_kind,
                execution_step_type: execution_step_type_for_transition_kind(transition_kind),
                provider,
                input_asset,
                output_asset,
                amount_in: quote.amount_in.clone(),
                amount_out: quote.amount_out.clone(),
                expires_at: quote.expires_at,
                raw: quote.provider_quote.clone(),
            }])
        }
        "spot_cross_token" => {
            let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
                return Err(activity_error(format!(
                    "refund exchange quote from {provider_name} missing spot_cross_token legs"
                )));
            };
            legs.iter()
                .enumerate()
                .map(|(index, leg)| {
                    let input_asset = leg
                        .get("input_asset")
                        .ok_or_else(|| {
                            activity_error("refund hyperliquid quote leg missing input_asset")
                        })
                        .and_then(|value| {
                            QuoteLegAsset::from_value(value, "input_asset").map_err(activity_error)
                        })?;
                    let output_asset = leg
                        .get("output_asset")
                        .ok_or_else(|| {
                            activity_error("refund hyperliquid quote leg missing output_asset")
                        })
                        .and_then(|value| {
                            QuoteLegAsset::from_value(value, "output_asset").map_err(activity_error)
                        })?;
                    Ok(QuoteLeg {
                        transition_decl_id: format!("{transition_decl_id}:leg:{index}"),
                        transition_parent_decl_id: transition_decl_id.to_string(),
                        transition_kind,
                        execution_step_type: execution_step_type_for_transition_kind(
                            transition_kind,
                        ),
                        provider,
                        input_asset,
                        output_asset,
                        amount_in: required_refund_quote_leg_amount(leg, "amount_in")?,
                        amount_out: required_refund_quote_leg_amount(leg, "amount_out")?,
                        expires_at: quote.expires_at,
                        raw: leg.clone(),
                    })
                })
                .collect()
        }
        other => Err(activity_error(format!(
            "unsupported refund exchange quote kind in transition path: {other:?}"
        ))),
    }
}

fn required_refund_quote_leg_amount(
    leg: &Value,
    field: &'static str,
) -> Result<String, ActivityError> {
    let Some(amount) = leg.get(field).and_then(Value::as_str) else {
        return Err(activity_error(format!(
            "refund hyperliquid quote leg missing {field}"
        )));
    };
    if amount.is_empty() {
        return Err(activity_error(format!(
            "refund hyperliquid quote leg has empty {field}"
        )));
    }
    Ok(amount.to_string())
}

fn refund_leg_provider(
    leg: &QuoteLeg,
    transition: &TransitionDecl,
) -> Result<ProviderId, ActivityError> {
    if leg.provider != transition.provider {
        return Err(activity_error(format!(
            "refund quote leg provider {} does not match transition provider {}",
            leg.provider.as_str(),
            transition.provider.as_str()
        )));
    }
    Ok(leg.provider)
}

fn refund_required_str<'a>(value: &'a Value, key: &'static str) -> Result<&'a str, ActivityError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| activity_error(format!("refund quote leg missing string field {key}")))
}

fn refund_required_u8(value: &Value, key: &'static str) -> Result<u8, ActivityError> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| activity_error(format!("refund quote leg missing numeric field {key}")))?;
    u8::try_from(raw).map_err(|source| {
        activity_error(format!(
            "refund quote leg field {key} does not fit u8: {source}"
        ))
    })
}

fn hyperliquid_refund_balance_amount_raw(
    total: &str,
    hold: &str,
    decimals: u8,
) -> Result<Option<String>, ActivityError> {
    let total_raw = decimal_string_to_raw_digits(total, decimals).map_err(|message| {
        activity_error(format!(
            "invalid Hyperliquid refund total balance: {message}"
        ))
    })?;
    let hold_raw = decimal_string_to_raw_digits(hold, decimals).map_err(|message| {
        activity_error(format!(
            "invalid Hyperliquid refund hold balance: {message}"
        ))
    })?;
    if total_raw == "0" || hold_raw != "0" {
        return Ok(None);
    }
    Ok(Some(total_raw))
}

fn decimal_string_to_raw_digits(value: &str, decimals: u8) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !frac.chars().all(|ch| ch.is_ascii_digit())
        || frac.len() > usize::from(decimals)
    {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let combined = format!("{whole}{:0<width$}", frac, width = usize::from(decimals));
    let digits = combined.trim_start_matches('0');
    Ok(if digits.is_empty() { "0" } else { digits }.to_string())
}

// Refund-side stale quote refresh is intentionally not implemented. Scar §7
// preserves the legacy rule that RefundRecovery attempts never stale-refresh:
// partial-execution funds may be mid-flight during a refund, so re-quoting is
// unsafe. A stale refund quote routes to RefundManualInterventionRequired via
// the existing classify_step_failure path, matching
// bin/router-server/src/services/order_executor.rs.

#[derive(Clone, Default)]
pub struct QuoteRefreshActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl QuoteRefreshActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, ActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| activity_error("quote refresh activities are not configured"))
    }
}

#[activities]
impl QuoteRefreshActivities {
    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    ///
    /// ExactIn refresh walks the stale suffix forward; ExactOut refresh walks it
    /// backward from the requested terminal output.
    #[activity]
    pub async fn compose_refreshed_quote_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ComposeRefreshedQuoteAttemptInput,
    ) -> Result<RefreshedQuoteAttemptShape, ActivityError> {
        let deps = self.deps()?;
        let order = deps
            .db
            .orders()
            .get(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let original_quote = match deps
            .db
            .orders()
            .get_router_order_quote(input.order_id)
            .await
            .map_err(activity_error_from_display)?
        {
            RouterOrderQuote::MarketOrder(quote) => quote,
            RouterOrderQuote::LimitOrder(_) => {
                return Err(activity_error(format!(
                    "order {} is a limit order and cannot refresh stale market quotes",
                    input.order_id
                )));
            }
        };
        let stale_attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.stale_attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        if stale_attempt.order_id != input.order_id {
            return Err(activity_error(format!(
                "attempt {} does not belong to order {}",
                stale_attempt.id, input.order_id
            )));
        }
        let failed_step = deps
            .db
            .orders()
            .get_execution_step(input.failed_step_id)
            .await
            .map_err(activity_error_from_display)?;
        if failed_step.order_id != input.order_id
            || failed_step.execution_attempt_id != Some(input.stale_attempt_id)
        {
            return Err(activity_error(format!(
                "step {} does not belong to order {} attempt {}",
                failed_step.id, input.order_id, input.stale_attempt_id
            )));
        }
        let stale_legs = deps
            .db
            .orders()
            .get_execution_legs_for_attempt(input.stale_attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        let stale_leg = failed_step
            .execution_leg_id
            .and_then(|leg_id| stale_legs.iter().find(|leg| leg.id == leg_id).cloned())
            .ok_or_else(|| {
                activity_error(format!(
                    "failed step {} has no materialized stale execution leg",
                    failed_step.id
                ))
            })?;
        let Some(provider_quote_expires_at) = stale_leg.provider_quote_expires_at else {
            return Err(activity_error(format!(
                "failed step {} execution leg {} has no provider quote expiry",
                failed_step.id, stale_leg.id
            )));
        };
        if provider_quote_expires_at >= Utc::now() {
            return Err(activity_error(format!(
                "failed step {} execution leg {} provider quote has not expired",
                failed_step.id, stale_leg.id
            )));
        }
        let source_vault_id = order.funding_vault_id.ok_or_else(|| {
            activity_error(format!(
                "order {} cannot refresh a quote without a funding vault",
                order.id
            ))
        })?;
        let source_vault = deps
            .db
            .vaults()
            .get(source_vault_id)
            .await
            .map_err(activity_error_from_display)?;
        let transitions = deps
            .planner
            .quoted_transition_path(&order, &original_quote)
            .map_err(activity_error_from_display)?;
        let Some(stale_transition_id) = stale_leg.transition_decl_id.as_deref() else {
            return Err(activity_error(format!(
                "failed stale execution leg {} has no transition declaration",
                stale_leg.id
            )));
        };
        let start_transition_index = transitions
            .iter()
            .position(|transition| transition.id == stale_transition_id)
            .ok_or_else(|| {
                activity_error(format!(
                    "stale execution leg {} transition {} is not in the quoted path",
                    stale_leg.id, stale_transition_id
                ))
            })?;
        let attempts = deps
            .db
            .orders()
            .get_execution_attempts(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let execution_history = deps
            .db
            .orders()
            .get_execution_legs(input.order_id)
            .await
            .map_err(activity_error_from_display)?;
        let stale_attempt_steps = deps
            .db
            .orders()
            .get_execution_steps_for_attempt(input.stale_attempt_id)
            .await
            .map_err(activity_error_from_display)?;
        let available_amount = match refresh_remaining_exact_in_amount(
            &execution_history,
            &attempts,
            &stale_attempt,
            &stale_leg,
            &original_quote,
            &transitions,
            start_transition_index,
        ) {
            Ok(amount) => amount,
            Err(err) => {
                return Ok(stale_quote_refresh_untenable(
                    input.order_id,
                    input.stale_attempt_id,
                    input.failed_step_id,
                    err,
                ));
            }
        };
        let now = Utc::now();
        let refreshed_quote = match original_quote.order_kind {
            MarketOrderKindType::ExactIn => {
                let mut cursor_amount = available_amount.clone();
                let mut expires_at = now + chrono::Duration::minutes(10);
                let mut refreshed_legs = Vec::new();
                for index in start_transition_index..transitions.len() {
                    let transition = &transitions[index];
                    match transition.kind {
                        MarketOrderTransitionKind::UniversalRouterSwap => {
                            let template_step = stale_attempt_steps
                        .iter()
                        .find(|step| refresh_step_matches_transition(step, transition))
                        .ok_or_else(|| {
                            activity_error(format!(
                                "cannot refresh transition {} because no template step is materialized",
                                transition.id
                            ))
                        })?;
                            let step_request =
                                match ExchangeExecutionRequest::universal_router_swap_from_value(
                                    &template_step.request,
                                )
                                .map_err(activity_error_from_display)?
                                {
                                    ExchangeExecutionRequest::UniversalRouterSwap(request) => {
                                        request
                                    }
                                    _ => {
                                        return Err(activity_error(format!(
                                            "step {} is not a universal-router swap request",
                                            template_step.id
                                        )));
                                    }
                                };
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: Some(step_request.input_decimals),
                                    output_decimals: Some(step_request.output_decimals),
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: cursor_amount.clone(),
                                        min_amount_out: refresh_transition_min_amount_out(
                                            &transitions,
                                            index,
                                            &original_quote.min_amount_out,
                                        ),
                                    },
                                    sender_address: Some(refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?),
                                    recipient_address: refresh_recipient_address(
                                        &order,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} returned no refreshed quote",
                                        exchange.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_exchange_quote_transition_legs(
                                transition.id.as_str(),
                                transition.kind,
                                transition.provider,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::AcrossBridge
                        | MarketOrderTransitionKind::CctpBridge
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                            let bridge = deps
                                .action_providers
                                .bridge(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "bridge provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let refreshed_quote = bridge
                                .quote_bridge(BridgeQuoteRequest {
                                    source_asset: transition.input.asset.clone(),
                                    destination_asset: transition.output.asset.clone(),
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: cursor_amount.clone(),
                                        min_amount_out: Some("1".to_string()),
                                    },
                                    recipient_address: refresh_bridge_recipient_address(
                                        &order,
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    depositor_address: refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    partial_fills_enabled: false,
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "bridge provider {} returned no refreshed quote",
                                        bridge.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_bridge_quote_transition_legs(
                                transition,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "unit provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let (unit_amount_in, provider_quote) = if let Some(fee_reserve) =
                                refresh_unit_deposit_fee_reserve(&original_quote, transition)?
                            {
                                (
                                    refresh_subtract_amount(
                                        "amount_in",
                                        &cursor_amount,
                                        fee_reserve,
                                        &transition.id,
                                        "bitcoin miner fee reserve",
                                    )?,
                                    refresh_bitcoin_fee_reserve_quote(fee_reserve),
                                )
                            } else {
                                (cursor_amount.clone(), json!({}))
                            };
                            if !unit.supports_deposit(&transition.input.asset) {
                                return Err(activity_error(format!(
                                    "unit provider {} does not support refreshed deposit",
                                    unit.id()
                                )));
                            }
                            cursor_amount = unit_amount_in.clone();
                            refreshed_legs.push(refresh_unit_deposit_quote_leg(
                                transition,
                                &unit_amount_in,
                                expires_at,
                                provider_quote,
                            ));
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let mut quote_amount_in = cursor_amount.clone();
                            if index > 0
                                && transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                quote_amount_in = refresh_reserve_hyperliquid_spot_send_quote_gas(
                                    "hyperliquid_trade.amount_in",
                                    &quote_amount_in,
                                )?;
                            }
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: quote_amount_in,
                                        min_amount_out: refresh_transition_min_amount_out(
                                            &transitions,
                                            index,
                                            &original_quote.min_amount_out,
                                        ),
                                    },
                                    sender_address: None,
                                    recipient_address: order.recipient_address.clone(),
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} returned no refreshed quote",
                                        exchange.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_exchange_quote_transition_legs(
                                transition.id.as_str(),
                                transition.kind,
                                transition.provider,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::UnitWithdrawal => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "unit provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            if !unit.supports_withdrawal(&transition.output.asset) {
                                return Err(activity_error(format!(
                                    "unit provider {} does not support refreshed withdrawal",
                                    unit.id()
                                )));
                            }
                            let recipient_address = refresh_recipient_address(
                                &order,
                                &stale_attempt_steps,
                                &transitions,
                                index,
                            )?;
                            refreshed_legs.push(refresh_unit_withdrawal_quote_leg(
                                transition,
                                &cursor_amount,
                                &recipient_address,
                                expires_at,
                            ));
                        }
                    }
                }
                if let Some(min_amount_out) = original_quote.min_amount_out.as_deref() {
                    let quoted_amount_out = parse_refresh_amount("amount_out", &cursor_amount)?;
                    let min_amount_out_raw =
                        parse_refresh_amount("min_amount_out", min_amount_out)?;
                    if quoted_amount_out < min_amount_out_raw {
                        return Ok(RefreshedQuoteAttemptShape {
                            outcome: RefreshedQuoteAttemptOutcome::Untenable {
                                order_id: input.order_id,
                                stale_attempt_id: input.stale_attempt_id,
                                failed_step_id: input.failed_step_id,
                                reason: StaleQuoteRefreshUntenableReason::RefreshedExactInOutputBelowMinAmountOut {
                                    amount_out: cursor_amount,
                                    min_amount_out: min_amount_out.to_string(),
                                },
                            },
                        });
                    }
                }
                refreshed_market_order_quote_exact_in(
                    &order,
                    &original_quote,
                    &transitions,
                    refreshed_legs,
                    available_amount,
                    cursor_amount,
                    expires_at,
                    now,
                )
            }
            MarketOrderKindType::ExactOut => {
                let mut required_output = original_quote.amount_out.clone();
                let mut expires_at = now + chrono::Duration::minutes(10);
                let mut refreshed_legs_per_transition: Vec<Vec<QuoteLeg>> =
                    (0..transitions.len()).map(|_| Vec::new()).collect();
                for index in (start_transition_index..transitions.len()).rev() {
                    let transition = &transitions[index];
                    match transition.kind {
                        MarketOrderTransitionKind::UnitWithdrawal => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "unit provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            if !unit.supports_withdrawal(&transition.output.asset) {
                                return Err(activity_error(format!(
                                    "unit provider {} does not support refreshed withdrawal",
                                    unit.id()
                                )));
                            }
                            let recipient_address = refresh_recipient_address(
                                &order,
                                &stale_attempt_steps,
                                &transitions,
                                index,
                            )?;
                            refreshed_legs_per_transition[index].push(
                                refresh_unit_withdrawal_quote_leg(
                                    transition,
                                    &required_output,
                                    &recipient_address,
                                    expires_at,
                                ),
                            );
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
                                    },
                                    sender_address: None,
                                    recipient_address: order.recipient_address.clone(),
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} returned no refreshed quote",
                                        exchange.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            let mut next_required = refreshed_quote.amount_in.clone();
                            if index > 0
                                && transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                next_required =
                                    refresh_add_hyperliquid_spot_send_quote_gas_reserve(
                                        "hyperliquid_trade.amount_in",
                                        &next_required,
                                    )?;
                            }
                            required_output = next_required;
                            refreshed_legs_per_transition[index] =
                                refresh_exchange_quote_transition_legs(
                                    transition.id.as_str(),
                                    transition.kind,
                                    transition.provider,
                                    &refreshed_quote,
                                )?;
                        }
                        MarketOrderTransitionKind::UniversalRouterSwap => {
                            let template_step = stale_attempt_steps
                                .iter()
                                .find(|step| refresh_step_matches_transition(step, transition))
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "cannot refresh transition {} because no template step is materialized",
                                        transition.id
                                    ))
                                })?;
                            let step_request =
                                match ExchangeExecutionRequest::universal_router_swap_from_value(
                                    &template_step.request,
                                )
                                .map_err(activity_error_from_display)?
                                {
                                    ExchangeExecutionRequest::UniversalRouterSwap(request) => {
                                        request
                                    }
                                    _ => {
                                        return Err(activity_error(format!(
                                            "step {} is not a universal-router swap request",
                                            template_step.id
                                        )));
                                    }
                                };
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: Some(step_request.input_decimals),
                                    output_decimals: Some(step_request.output_decimals),
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
                                    },
                                    sender_address: Some(refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?),
                                    recipient_address: refresh_recipient_address(
                                        &order,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "exchange provider {} returned no refreshed quote",
                                        exchange.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            required_output = refreshed_quote.amount_in.clone();
                            refreshed_legs_per_transition[index] =
                                refresh_exchange_quote_transition_legs(
                                    transition.id.as_str(),
                                    transition.kind,
                                    transition.provider,
                                    &refreshed_quote,
                                )?;
                        }
                        MarketOrderTransitionKind::AcrossBridge
                        | MarketOrderTransitionKind::CctpBridge
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                            let bridge = deps
                                .action_providers
                                .bridge(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "bridge provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            let refreshed_quote = bridge
                                .quote_bridge(BridgeQuoteRequest {
                                    source_asset: transition.input.asset.clone(),
                                    destination_asset: transition.output.asset.clone(),
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
                                    },
                                    recipient_address: refresh_bridge_recipient_address(
                                        &order,
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    depositor_address: refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    partial_fills_enabled: false,
                                })
                                .await
                                .map_err(activity_error_from_display)?
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "bridge provider {} returned no refreshed quote",
                                        bridge.id()
                                    ))
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            required_output = refreshed_quote.amount_in.clone();
                            refreshed_legs_per_transition[index] =
                                refresh_bridge_quote_transition_legs(transition, &refreshed_quote)?;
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    activity_error(format!(
                                        "unit provider {} is not configured",
                                        transition.provider.as_str()
                                    ))
                                })?;
                            if !unit.supports_deposit(&transition.input.asset) {
                                return Err(activity_error(format!(
                                    "unit provider {} does not support refreshed deposit",
                                    unit.id()
                                )));
                            }
                            let (unit_amount_in, upstream_required, provider_quote) =
                                if let Some(fee_reserve) =
                                    refresh_unit_deposit_fee_reserve(&original_quote, transition)?
                                {
                                    (
                                        required_output.clone(),
                                        refresh_add_amount(
                                            "amount_in",
                                            &required_output,
                                            fee_reserve,
                                            "bitcoin miner fee reserve",
                                        )?,
                                        refresh_bitcoin_fee_reserve_quote(fee_reserve),
                                    )
                                } else {
                                    (required_output.clone(), required_output.clone(), json!({}))
                                };
                            refreshed_legs_per_transition[index].push(
                                refresh_unit_deposit_quote_leg(
                                    transition,
                                    &unit_amount_in,
                                    expires_at,
                                    provider_quote,
                                ),
                            );
                            required_output = upstream_required;
                        }
                    }
                }
                let required_input = parse_refresh_amount("amount_in", &required_output)?;
                let available = parse_refresh_amount("available_amount", &available_amount)?;
                if required_input > available {
                    return Ok(RefreshedQuoteAttemptShape {
                        outcome: RefreshedQuoteAttemptOutcome::Untenable {
                            order_id: input.order_id,
                            stale_attempt_id: input.stale_attempt_id,
                            failed_step_id: input.failed_step_id,
                            reason: StaleQuoteRefreshUntenableReason::RefreshedExactOutInputAboveAvailableAmount {
                                amount_in: required_output,
                                available_amount,
                            },
                        },
                    });
                }
                refreshed_market_order_quote_exact_out(
                    &order,
                    &original_quote,
                    &transitions,
                    flatten_refresh_transition_legs(refreshed_legs_per_transition),
                    required_output,
                    available_amount,
                    expires_at,
                    now,
                )
            }
        };
        let mut plan = deps
            .planner
            .plan_remaining(
                &order,
                &source_vault,
                &refreshed_quote,
                MarketOrderPlanRemainingStart {
                    transition_decl_id: &transitions[start_transition_index].id,
                    step_index: failed_step.step_index,
                    leg_index: stale_leg.leg_index,
                    planned_at: now,
                },
            )
            .map_err(activity_error_from_display)?;
        for leg in &mut plan.legs {
            set_json_value(
                &mut leg.details,
                "refreshed_from_attempt_id",
                json!(stale_attempt.id),
            );
            set_json_value(
                &mut leg.details,
                "refreshed_from_execution_leg_id",
                json!(stale_leg.id),
            );
            set_json_value(
                &mut leg.details,
                "refreshed_quote_id",
                json!(refreshed_quote.id),
            );
        }
        for step in &mut plan.steps {
            set_json_value(
                &mut step.details,
                "refreshed_from_attempt_id",
                json!(stale_attempt.id),
            );
            set_json_value(
                &mut step.details,
                "refreshed_from_step_id",
                json!(failed_step.id),
            );
            set_json_value(
                &mut step.details,
                "refreshed_quote_id",
                json!(refreshed_quote.id),
            );
        }

        Ok(RefreshedQuoteAttemptShape {
            outcome: RefreshedQuoteAttemptOutcome::Refreshed {
                order_id: input.order_id,
                stale_attempt_id: input.stale_attempt_id,
                failed_step_id: input.failed_step_id,
                plan: ExecutionPlan {
                    path_id: plan.path_id,
                    transition_decl_ids: plan.transition_decl_ids,
                    legs: plan.legs,
                    steps: plan.steps,
                },
                failure_reason: json!({
                    "reason": "stale_provider_quote_refresh",
                    "trace": "quote_refresh_workflow",
                    "stale_attempt_id": stale_attempt.id,
                    "failed_step_id": failed_step.id,
                    "superseded_attempt_id": stale_attempt.id,
                    "superseded_attempt_index": stale_attempt.attempt_index,
                    "stale_step_id": failed_step.id,
                    "stale_execution_leg_id": stale_leg.id,
                    "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
                    "refreshed_quote_id": refreshed_quote.id,
                }),
                superseded_reason: json!({
                    "reason": "superseded_by_stale_provider_quote_refresh",
                    "stale_attempt_id": stale_attempt.id,
                    "failed_step_id": failed_step.id,
                    "stale_step_id": failed_step.id,
                    "stale_execution_leg_id": stale_leg.id,
                    "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
                    "refreshed_quote_id": refreshed_quote.id,
                }),
                input_custody_snapshot: failed_attempt_snapshot(&order, &failed_step),
            },
        })
    }

    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn materialize_refreshed_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRefreshedAttemptInput,
    ) -> Result<RefreshedAttemptMaterialized, ActivityError> {
        let deps = self.deps()?;
        let RefreshedQuoteAttemptOutcome::Refreshed {
            order_id,
            stale_attempt_id,
            failed_step_id,
            mut plan,
            failure_reason,
            superseded_reason,
            input_custody_snapshot,
        } = input.refreshed_attempt.outcome
        else {
            return Err(activity_error(
                "untenable stale quote refresh must not be materialized",
            ));
        };
        if order_id != input.order_id {
            return Err(activity_error(format!(
                "refreshed attempt order {} does not match materialize input order {}",
                order_id, input.order_id
            )));
        }
        plan.steps = hydrate_destination_execution_steps(&deps, input.order_id, plan.steps).await?;
        let record = deps
            .db
            .orders()
            .create_refreshed_execution_attempt_from_failed_step(
                order_id,
                stale_attempt_id,
                failed_step_id,
                RefreshedExecutionAttemptPlan {
                    legs: plan.legs,
                    steps: plan.steps,
                    failure_reason,
                    superseded_reason,
                    input_custody_snapshot,
                },
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %record.order.id,
            attempt_id = %record.attempt.id,
            stale_attempt_id = %stale_attempt_id,
            failed_step_id = %failed_step_id,
            event_name = "order.execution_quote_refreshed",
            "order.execution_quote_refreshed"
        );
        Ok(RefreshedAttemptMaterialized {
            attempt_id: record.attempt.id,
            steps: record
                .steps
                .into_iter()
                .map(|step| WorkflowExecutionStep {
                    step_id: step.id,
                    step_index: step.step_index,
                })
                .collect(),
        })
    }
}

#[derive(Clone, Default)]
pub struct ProviderObservationActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl ProviderObservationActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, ActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| activity_error("provider observation activities are not configured"))
    }
}

#[activities]
impl ProviderObservationActivities {
    /// Scar tissue: §10 provider operation hint flow and verifier dispatch.
    #[activity]
    pub async fn verify_provider_operation_hint(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: VerifyProviderOperationHintInput,
    ) -> Result<ProviderOperationHintVerified, ActivityError> {
        match input.signal.hint_kind {
            ProviderHintKind::AcrossFill => {
                let deps = self.deps()?;
                verify_across_fill_hint(&deps, input).await
            }
            ProviderHintKind::CctpAttestation => {
                let deps = self.deps()?;
                verify_cctp_attestation_hint(&deps, input).await
            }
            ProviderHintKind::UnitDeposit => {
                let deps = self.deps()?;
                verify_unit_deposit_hint(&deps, input).await
            }
            ProviderHintKind::ProviderObservation => {
                let deps = self.deps()?;
                verify_provider_observation_hint(&deps, input).await
            }
            ProviderHintKind::HyperliquidTrade => {
                let deps = self.deps()?;
                verify_hyperliquid_trade_hint(&deps, input).await
            }
        }
    }

    /// Scar tissue: §10 provider hint recovery. This is the polling half of the user-approved
    /// signal-first plus fallback shape for brief §8.4.
    #[activity]
    pub async fn poll_provider_operation_hints(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PollProviderOperationHintsInput,
    ) -> Result<ProviderOperationHintsPolled, ActivityError> {
        let deps = self.deps()?;
        poll_provider_operation_hint_for_step(&deps, input).await
    }

    /// Scar tissue: §11 Across on-chain log recovery.
    #[activity]
    pub async fn recover_across_onchain_log(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RecoverAcrossOnchainLogInput,
    ) -> Result<AcrossOnchainLogRecovered, ActivityError> {
        let deps = self.deps()?;
        let operation = deps
            .db
            .orders()
            .get_provider_operation(input.provider_operation_id)
            .await
            .map_err(activity_error_from_display)?;
        if operation.order_id != input.order_id {
            return Err(activity_error(format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            )));
        }

        let recovered = recover_across_operation_from_checkpoint(&deps, operation).await?;
        if recovered
            .as_ref()
            .and_then(|operation| operation.provider_ref.as_deref())
            .is_some_and(is_decimal_u256)
        {
            Ok(AcrossOnchainLogRecovered {
                provider_operation_id: input.provider_operation_id,
            })
        } else {
            Err(activity_error(format!(
                "Across provider operation {} has no recoverable deposit log yet",
                input.provider_operation_id
            )))
        }
    }

    /// Scar tissue: §12 Hyperliquid trade timeout cancel.
    #[activity]
    pub async fn cancel_timed_out_hyperliquid_trade(
        _ctx: ActivityContext,
        _input: CancelTimedOutHyperliquidTradeInput,
    ) -> Result<HyperliquidTradeCancelRecorded, ActivityError> {
        todo!("PR4: cancel timed-out Hyperliquid trade")
    }
}

async fn poll_provider_operation_hint_for_step(
    deps: &OrderActivityDeps,
    input: PollProviderOperationHintsInput,
) -> Result<ProviderOperationHintsPolled, ActivityError> {
    let step = deps
        .db
        .orders()
        .get_execution_step(input.step_id)
        .await
        .map_err(activity_error_from_display)?;
    if step.order_id != input.order_id {
        return Ok(provider_hints_polled(provider_hint_rejected(
            None,
            format!(
                "step {} belongs to order {}, not {}",
                step.id, step.order_id, input.order_id
            ),
        )));
    }

    let operations = deps
        .db
        .orders()
        .get_provider_operations(input.order_id)
        .await
        .map_err(activity_error_from_display)?;
    let Some(operation) = operations
        .into_iter()
        .filter(|operation| operation.execution_step_id == Some(input.step_id))
        .max_by_key(|operation| (operation.updated_at, operation.created_at, operation.id))
    else {
        return Ok(provider_hints_polled(provider_hint_deferred(
            None,
            format!(
                "provider hint poll found no provider operation for step {}",
                input.step_id
            ),
        )));
    };

    let Some((provider, hint_kind)) = polling_hint_shape_for_operation(&operation) else {
        return Ok(provider_hints_polled(provider_hint_deferred(
            Some(operation.id),
            format!(
                "provider hint polling for {} is deferred to a later PR7b verifier",
                operation.operation_type.to_db_string()
            ),
        )));
    };

    let signal = ProviderOperationHintSignal {
        order_id: input.order_id,
        // Deterministic synthetic hint id for activity-driven polling; this is
        // not persisted as a user hint row.
        hint_id: operation.id,
        provider_operation_id: Some(operation.id),
        provider,
        hint_kind,
        provider_ref: operation.provider_ref.clone(),
        evidence: None,
    };
    let verified = match hint_kind {
        ProviderHintKind::AcrossFill => {
            verify_across_fill_hint(
                deps,
                VerifyProviderOperationHintInput {
                    order_id: input.order_id,
                    step_id: input.step_id,
                    signal,
                },
            )
            .await?
        }
        ProviderHintKind::ProviderObservation => {
            verify_provider_observation_hint(
                deps,
                VerifyProviderOperationHintInput {
                    order_id: input.order_id,
                    step_id: input.step_id,
                    signal,
                },
            )
            .await?
        }
        ProviderHintKind::UnitDeposit => {
            verify_unit_deposit_hint(
                deps,
                VerifyProviderOperationHintInput {
                    order_id: input.order_id,
                    step_id: input.step_id,
                    signal,
                },
            )
            .await?
        }
        ProviderHintKind::CctpAttestation => {
            verify_cctp_attestation_hint(
                deps,
                VerifyProviderOperationHintInput {
                    order_id: input.order_id,
                    step_id: input.step_id,
                    signal,
                },
            )
            .await?
        }
        ProviderHintKind::HyperliquidTrade => {
            verify_hyperliquid_trade_hint(
                deps,
                VerifyProviderOperationHintInput {
                    order_id: input.order_id,
                    step_id: input.step_id,
                    signal,
                },
            )
            .await?
        }
    };
    Ok(provider_hints_polled(verified))
}

fn polling_hint_shape_for_operation(
    operation: &OrderProviderOperation,
) -> Option<(ProviderKind, ProviderHintKind)> {
    match operation.operation_type {
        ProviderOperationType::AcrossBridge => {
            Some((ProviderKind::Bridge, ProviderHintKind::AcrossFill))
        }
        ProviderOperationType::UnitDeposit => {
            Some((ProviderKind::Unit, ProviderHintKind::UnitDeposit))
        }
        ProviderOperationType::CctpBridge => {
            Some((ProviderKind::Bridge, ProviderHintKind::CctpAttestation))
        }
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder => {
            Some((ProviderKind::Exchange, ProviderHintKind::HyperliquidTrade))
        }
        // These operation families do not have typed hint kinds, but the generic
        // provider observation verifier can still poll their real provider APIs.
        ProviderOperationType::UnitWithdrawal
        | ProviderOperationType::HyperliquidBridgeDeposit
        | ProviderOperationType::HyperliquidBridgeWithdrawal
        | ProviderOperationType::UniversalRouterSwap => Some((
            provider_kind_for_operation(&operation.operation_type),
            ProviderHintKind::ProviderObservation,
        )),
    }
}

fn provider_kind_for_operation(operation_type: &ProviderOperationType) -> ProviderKind {
    match operation_type {
        ProviderOperationType::AcrossBridge
        | ProviderOperationType::CctpBridge
        | ProviderOperationType::HyperliquidBridgeDeposit
        | ProviderOperationType::HyperliquidBridgeWithdrawal => ProviderKind::Bridge,
        ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal => {
            ProviderKind::Unit
        }
        ProviderOperationType::HyperliquidTrade
        | ProviderOperationType::HyperliquidLimitOrder
        | ProviderOperationType::UniversalRouterSwap => ProviderKind::Exchange,
    }
}

fn provider_hints_polled(verified: ProviderOperationHintVerified) -> ProviderOperationHintsPolled {
    ProviderOperationHintsPolled {
        provider_operation_id: verified.provider_operation_id,
        decision: verified.decision,
        reason: verified.reason,
    }
}

#[derive(Debug, Clone)]
struct RecoveredAcrossDeposit {
    deposit_id: String,
    deposit_tx_hash: String,
}

async fn recover_across_operation_from_checkpoint(
    deps: &OrderActivityDeps,
    operation: OrderProviderOperation,
) -> Result<Option<OrderProviderOperation>, ActivityError> {
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return Ok(Some(operation));
    }
    if operation
        .provider_ref
        .as_deref()
        .is_some_and(is_decimal_u256)
    {
        return Ok(Some(operation));
    }
    if operation.response.get("kind").and_then(Value::as_str) != Some("provider_receipt_checkpoint")
    {
        return Ok(None);
    }

    let Some(step_id) = operation.execution_step_id else {
        return Ok(None);
    };
    let step = deps
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .map_err(activity_error_from_display)?;
    let Some(recovered) = recover_across_deposit_from_origin_logs(deps, &operation, &step).await?
    else {
        return Ok(None);
    };

    let mut observed_state = json!({
        "source": "temporal_across_deposit_log_recovery",
        "deposit_id": &recovered.deposit_id,
        "deposit_tx_hash": &recovered.deposit_tx_hash,
        "router_recovery": {
            "kind": "across_deposit_log_recovery",
            "deposit_tx_hash": &recovered.deposit_tx_hash,
            "source_provider_ref": &operation.provider_ref,
        },
    });
    if !is_empty_json_object(&operation.observed_state) {
        observed_state["previous_observed_state"] = operation.observed_state.clone();
    }

    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            operation.status,
            Some(recovered.deposit_id),
            observed_state,
            None,
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;
    tracing::info!(
        order_id = %updated.order_id,
        provider_operation_id = %updated.id,
        step_id = ?updated.execution_step_id,
        event_name = "provider_operation.across_onchain_log_recovered",
        "provider_operation.across_onchain_log_recovered"
    );
    Ok(Some(updated))
}

async fn recover_across_deposit_from_origin_logs(
    deps: &OrderActivityDeps,
    operation: &OrderProviderOperation,
    step: &OrderExecutionStep,
) -> Result<Option<RecoveredAcrossDeposit>, ActivityError> {
    let provider_response = operation
        .response
        .get("provider_response")
        .unwrap_or(&operation.response);
    let swap_tx = provider_response.get("swapTx").ok_or_else(|| {
        activity_error(format!(
            "Across checkpoint recovery missing provider_response.swapTx for operation {}",
            operation.id
        ))
    })?;
    let spoke_pool_address = json_str_field(swap_tx, "to").ok_or_else(|| {
        activity_error(format!(
            "Across checkpoint recovery missing swapTx.to for operation {}",
            operation.id
        ))
    })?;
    let spoke_pool_address = Address::from_str(spoke_pool_address).map_err(|err| {
        activity_error(format!(
            "Across checkpoint recovery invalid swapTx.to for operation {}: {err}",
            operation.id
        ))
    })?;

    let origin_chain_id =
        json_u64_field(&operation.request, "origin_chain_id").ok_or_else(|| {
            activity_error(format!(
                "Across checkpoint recovery missing origin_chain_id for operation {}",
                operation.id
            ))
        })?;
    let destination_chain_id = json_u64_field(&operation.request, "destination_chain_id")
        .ok_or_else(|| {
            activity_error(format!(
                "Across checkpoint recovery missing destination_chain_id for operation {}",
                operation.id
            ))
        })?;
    let origin_chain = ChainId::parse(&format!("evm:{origin_chain_id}")).map_err(|err| {
        activity_error(format!(
            "Across checkpoint recovery invalid origin chain for operation {}: {err}",
            operation.id
        ))
    })?;
    let backend_chain = backend_chain_for_id(&origin_chain).ok_or_else(|| {
        activity_error(format!(
            "Across checkpoint recovery unsupported origin chain {origin_chain}"
        ))
    })?;
    let evm_chain = deps.chain_registry.get_evm(&backend_chain).ok_or_else(|| {
        activity_error(format!(
            "Across checkpoint recovery origin chain {origin_chain} is not configured"
        ))
    })?;

    let BridgeExecutionRequest::Across(step_request) =
        BridgeExecutionRequest::across_from_value(&step.request).map_err(|err| {
            activity_error(format!(
                "Across checkpoint recovery could not decode step request for operation {}: {err}",
                operation.id
            ))
        })?
    else {
        return Err(activity_error(format!(
            "Across checkpoint recovery step {} is not an Across request",
            step.id
        )));
    };
    let input_token =
        recovery_step_asset_address(&step_request.input_asset, "input_asset", operation)?;
    let output_token =
        recovery_step_asset_address(&step_request.output_asset, "output_asset", operation)?;
    let depositor = recovery_step_address(
        &step_request.depositor_address,
        "depositor_address",
        operation,
    )?;
    let recipient = recovery_step_address(&step_request.recipient, "recipient", operation)?;
    let amount = U256::from_str_radix(&step_request.amount, 10).map_err(|err| {
        activity_error(format!(
            "Across checkpoint recovery invalid request.amount for operation {}: {err}",
            operation.id
        ))
    })?;
    let expected_output = json_u256_string_field(provider_response, "expectedOutputAmount")
        .or_else(|| json_u256_string_field(provider_response, "minOutputAmount"));

    let Some(checkpoint_tx_hash) = persisted_provider_operation_tx_hash(operation) else {
        return Ok(None);
    };
    let Some(checkpoint_receipt) = evm_chain
        .transaction_receipt(&checkpoint_tx_hash)
        .await
        .map_err(activity_error_from_display)?
    else {
        return Ok(None);
    };
    let Some(from_block) = checkpoint_receipt.block_number else {
        return Ok(None);
    };

    let logs = evm_chain
        .logs(
            &Filter::new()
                .address(spoke_pool_address)
                .event_signature(FundsDeposited::SIGNATURE_HASH)
                .from_block(from_block),
        )
        .await
        .map_err(activity_error_from_display)?;

    let input_token = evm_address_to_bytes32(input_token);
    let output_token = evm_address_to_bytes32(output_token);
    let depositor = evm_address_to_bytes32(depositor);
    let recipient = evm_address_to_bytes32(recipient);
    let destination_chain_id = U256::from(destination_chain_id);

    for log in logs {
        let Ok(decoded) = FundsDeposited::decode_log(&log.inner) else {
            continue;
        };
        let event = decoded.data;
        if event.destinationChainId != destination_chain_id
            || event.inputToken != input_token
            || event.outputToken != output_token
            || event.inputAmount != amount
            || expected_output
                .as_ref()
                .is_some_and(|expected| event.outputAmount != *expected)
            || event.depositor != depositor
            || event.recipient != recipient
        {
            continue;
        }
        let Some(deposit_tx_hash) = log.transaction_hash.map(|hash| format!("{hash:#x}")) else {
            continue;
        };
        return Ok(Some(RecoveredAcrossDeposit {
            deposit_id: event.depositId.to_string(),
            deposit_tx_hash,
        }));
    }

    Ok(None)
}

async fn verify_across_fill_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, ActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "AcrossFill hint missing provider_operation_id",
        ));
    };
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(activity_error_from_display)?;

    if operation.order_id != input.order_id {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id, operation.execution_step_id, input.step_id
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "AcrossFill hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    let Some(operation) = recover_across_operation_from_checkpoint(deps, operation).await? else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Across provider operation has no deposit-id provider_ref and no recoverable deposit log yet",
        ));
    };

    let provider = deps
        .action_providers
        .bridge(&operation.provider)
        .ok_or_else(|| {
            activity_error(format!(
                "bridge provider {} is not configured",
                operation.provider
            ))
        })?;
    let observation = provider
        .observe_bridge_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: provider_hint_signal_evidence(&input.signal),
        })
        .await
        .map_err(activity_error)?;

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Across provider returned no observation",
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    match observation.status {
        ProviderOperationStatus::Completed => {
            let provider_ref = observation
                .provider_ref
                .clone()
                .or_else(|| operation.provider_ref.clone());
            let observed_state = provider_hint_observed_state(&operation, &input, &observation);
            let (updated, _) = deps
                .db
                .orders()
                .update_provider_operation_status(
                    operation.id,
                    ProviderOperationStatus::Completed,
                    provider_ref,
                    observed_state,
                    observation.response.clone(),
                    Utc::now(),
                )
                .await
                .map_err(activity_error_from_display)?;
            tracing::info!(
                order_id = %updated.order_id,
                provider_operation_id = %updated.id,
                step_id = ?updated.execution_step_id,
                event_name = "provider_operation.completed",
                "provider_operation.completed"
            );
            Ok(ProviderOperationHintVerified {
                provider_operation_id: Some(updated.id),
                decision: ProviderOperationHintDecision::Accept,
                reason: None,
            })
        }
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(provider_operation_id),
                format!(
                    "Across fill observation returned {}",
                    observation.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "Across fill observation returned {}",
                observation.status.to_db_string()
            ),
        )),
    }
}

async fn verify_cctp_attestation_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, ActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "CctpAttestation hint missing provider_operation_id",
        ));
    };
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(activity_error_from_display)?;

    if operation.order_id != input.order_id {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id, operation.execution_step_id, input.step_id
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::CctpBridge {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "CctpAttestation hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }

    let provider = deps
        .action_providers
        .bridge(&operation.provider)
        .ok_or_else(|| {
            activity_error(format!(
                "bridge provider {} is not configured",
                operation.provider
            ))
        })?;
    let observation = provider
        .observe_bridge_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: provider_hint_signal_evidence(&input.signal),
        })
        .await
        .map_err(activity_error)?;

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "CCTP provider returned no attestation observation",
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    let provider_ref = observation
        .provider_ref
        .clone()
        .or_else(|| operation.provider_ref.clone())
        .or_else(|| observation.tx_hash.clone());
    let observed_state = provider_hint_observed_state(&operation, &input, &observation);
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            provider_ref,
            observed_state,
            observation.response.clone(),
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "CCTP attestation observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "CCTP attestation observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

async fn verify_hyperliquid_trade_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, ActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "HyperliquidTrade hint missing provider_operation_id",
        ));
    };
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(activity_error_from_display)?;

    if operation.order_id != input.order_id {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id, operation.execution_step_id, input.step_id
            ),
        ));
    }
    if !matches!(
        operation.operation_type,
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder
    ) {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "HyperliquidTrade hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }

    let provider = deps
        .action_providers
        .exchange(&operation.provider)
        .ok_or_else(|| {
            activity_error(format!(
                "exchange provider {} is not configured",
                operation.provider
            ))
        })?;
    let observation = provider
        .observe_trade_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: provider_hint_signal_evidence(&input.signal),
        })
        .await
        .map_err(activity_error)?;

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            "Hyperliquid provider returned no trade observation",
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    let provider_ref = observation
        .provider_ref
        .clone()
        .or_else(|| operation.provider_ref.clone())
        .or_else(|| observation.tx_hash.clone());
    let observed_state = provider_hint_observed_state(&operation, &input, &observation);
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            provider_ref,
            observed_state,
            observation.response.clone(),
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "Hyperliquid trade observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "Hyperliquid trade observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

async fn verify_unit_deposit_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, ActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "UnitDeposit hint missing provider_operation_id",
        ));
    };
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(activity_error_from_display)?;

    if operation.order_id != input.order_id {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id, operation.execution_step_id, input.step_id
            ),
        ));
    }
    if operation.operation_type != ProviderOperationType::UnitDeposit {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "UnitDeposit hint cannot verify {} operation",
                operation.operation_type.to_db_string()
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }

    let Some(evidence) = input.signal.evidence.as_ref() else {
        return verify_provider_observation_hint(deps, input).await;
    };
    if evidence.tx_hash.trim().is_empty() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            "UnitDeposit evidence tx_hash is empty",
        ));
    }
    if evidence.address.trim().is_empty() {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            "UnitDeposit evidence address is empty",
        ));
    }

    let addresses = deps
        .db
        .orders()
        .get_provider_addresses_by_operation(operation.id)
        .await
        .map_err(activity_error_from_display)?;
    let Some(provider_address) = addresses.iter().find(|address| {
        address.role == ProviderAddressRole::UnitDeposit
            && address.address.eq_ignore_ascii_case(&evidence.address)
    }) else {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "evidence address {} does not match a UnitDeposit provider address",
                evidence.address
            ),
        ));
    };
    let verified_amount =
        match verify_unit_deposit_candidate(deps, provider_address, evidence).await {
            Ok(amount) => amount,
            Err(reason) => return Ok(provider_hint_rejected(Some(provider_operation_id), reason)),
        };
    let step = deps
        .db
        .orders()
        .get_execution_step(input.step_id)
        .await
        .map_err(activity_error_from_display)?;
    let expected_amount =
        match expected_provider_operation_amount(&operation, Some(&step), input.signal.hint_id) {
            Ok(amount) => amount,
            Err(reason) => return Ok(provider_hint_rejected(Some(provider_operation_id), reason)),
        };
    if verified_amount < expected_amount {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!("observed amount {verified_amount} is below expected amount {expected_amount}"),
        ));
    }

    let provider = deps
        .action_providers
        .unit(&operation.provider)
        .ok_or_else(|| {
            activity_error(format!(
                "unit provider {} is not configured",
                operation.provider
            ))
        })?;
    let provider_observation = provider
        .observe_unit_operation(ProviderOperationObservationRequest {
            operation_id: operation.id,
            operation_type: operation.operation_type,
            provider_ref: operation.provider_ref.clone(),
            request: operation.request.clone(),
            response: operation.response.clone(),
            observed_state: operation.observed_state.clone(),
            hint_evidence: unit_deposit_evidence_json(evidence),
        })
        .await
        .map_err(activity_error)?;
    let (status, provider_observed_state, provider_tx_hash, provider_error, provider_response) =
        if let Some(observation) = provider_observation {
            (
                observation.status,
                json_object_or_wrapped(observation.observed_state),
                observation.tx_hash,
                observation.error,
                observation.response,
            )
        } else {
            (
                ProviderOperationStatus::WaitingExternal,
                json!({}),
                None,
                None,
                None,
            )
        };
    let tx_hash = provider_tx_hash.unwrap_or_else(|| evidence.tx_hash.clone());
    let mut observed_state = json!({
        "source": "temporal_provider_operation_hint_validation",
        "hint_id": input.signal.hint_id,
        "hint_kind": input.signal.hint_kind,
        "evidence": unit_deposit_evidence_json(evidence),
        "validated_provider_address_id": provider_address.id,
        "expected_amount": expected_amount.to_string(),
        "observed_amount": verified_amount.to_string(),
        "transfer_index": evidence.transfer_index,
        "chain_verified": true,
        "tx_hash": &tx_hash,
        "provider_observed_state": provider_observed_state,
    });
    if let Some(error) = provider_error {
        observed_state["provider_error"] = error;
    }
    let response = provider_response.or_else(|| {
        Some(json!({
            "kind": "provider_operation_hint_validation",
            "provider": &operation.provider,
            "operation_id": operation.id,
            "hint_id": input.signal.hint_id,
            "tx_hash": &tx_hash,
            "amount": verified_amount.to_string(),
            "chain_verified": true,
        }))
    });
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            status,
            operation.provider_ref.clone(),
            observed_state,
            response,
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "UnitDeposit observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "UnitDeposit observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

async fn verify_provider_observation_hint(
    deps: &OrderActivityDeps,
    input: VerifyProviderOperationHintInput,
) -> Result<ProviderOperationHintVerified, ActivityError> {
    let Some(provider_operation_id) = input.signal.provider_operation_id else {
        return Ok(provider_hint_deferred(
            None,
            "ProviderObservation hint missing provider_operation_id",
        ));
    };
    let operation = deps
        .db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map_err(activity_error_from_display)?;

    if operation.order_id != input.order_id {
        return Ok(provider_hint_rejected(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to order {}, not {}",
                operation.id, operation.order_id, input.order_id
            ),
        ));
    }
    if operation.execution_step_id != Some(input.step_id) {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider operation {} belongs to step {:?}, not {}",
                operation.id, operation.execution_step_id, input.step_id
            ),
        ));
    }
    if matches!(
        operation.status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    ) {
        return Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(operation.id),
            decision: if operation.status == ProviderOperationStatus::Completed {
                ProviderOperationHintDecision::Accept
            } else {
                ProviderOperationHintDecision::Reject
            },
            reason: Some(format!(
                "provider operation already terminal: {}",
                operation.status.to_db_string()
            )),
        });
    }
    let operation = if operation.operation_type == ProviderOperationType::AcrossBridge {
        let Some(operation) = recover_across_operation_from_checkpoint(deps, operation).await?
        else {
            return Ok(provider_hint_deferred(
                Some(provider_operation_id),
                "Across provider operation has no deposit-id provider_ref and no recoverable deposit log yet",
            ));
        };
        operation
    } else {
        operation
    };

    let request = ProviderOperationObservationRequest {
        operation_id: operation.id,
        operation_type: operation.operation_type,
        provider_ref: operation.provider_ref.clone(),
        request: operation.request.clone(),
        response: operation.response.clone(),
        observed_state: operation.observed_state.clone(),
        hint_evidence: provider_hint_signal_evidence(&input.signal),
    };
    let observation = match operation.operation_type {
        ProviderOperationType::AcrossBridge
        | ProviderOperationType::CctpBridge
        | ProviderOperationType::HyperliquidBridgeDeposit
        | ProviderOperationType::HyperliquidBridgeWithdrawal => {
            let provider = deps
                .action_providers
                .bridge(&operation.provider)
                .ok_or_else(|| {
                    activity_error(format!(
                        "bridge provider {} is not configured",
                        operation.provider
                    ))
                })?;
            provider
                .observe_bridge_operation(request)
                .await
                .map_err(activity_error)?
        }
        ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal => {
            let provider = deps
                .action_providers
                .unit(&operation.provider)
                .ok_or_else(|| {
                    activity_error(format!(
                        "unit provider {} is not configured",
                        operation.provider
                    ))
                })?;
            provider
                .observe_unit_operation(request)
                .await
                .map_err(activity_error)?
        }
        ProviderOperationType::HyperliquidTrade
        | ProviderOperationType::HyperliquidLimitOrder
        | ProviderOperationType::UniversalRouterSwap => {
            let provider = deps
                .action_providers
                .exchange(&operation.provider)
                .ok_or_else(|| {
                    activity_error(format!(
                        "exchange provider {} is not configured",
                        operation.provider
                    ))
                })?;
            provider
                .observe_trade_operation(request)
                .await
                .map_err(activity_error)?
        }
    };

    let Some(observation) = observation else {
        return Ok(provider_hint_deferred(
            Some(provider_operation_id),
            format!(
                "provider {} returned no observation for {}",
                operation.provider,
                operation.operation_type.to_db_string()
            ),
        ));
    };
    if let Some(reason) = provider_observation_ref_reject_reason(&operation, &observation) {
        return Ok(provider_hint_rejected(Some(provider_operation_id), reason));
    }

    let provider_ref = observation
        .provider_ref
        .clone()
        .or_else(|| operation.provider_ref.clone())
        .or_else(|| observation.tx_hash.clone());
    let observed_state = provider_hint_observed_state(&operation, &input, &observation);
    let (updated, _) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            observation.status,
            provider_ref,
            observed_state,
            observation.response.clone(),
            Utc::now(),
        )
        .await
        .map_err(activity_error_from_display)?;

    match updated.status {
        ProviderOperationStatus::Completed => Ok(ProviderOperationHintVerified {
            provider_operation_id: Some(updated.id),
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        }),
        ProviderOperationStatus::Failed | ProviderOperationStatus::Expired => {
            Ok(provider_hint_rejected(
                Some(updated.id),
                format!(
                    "provider observation returned {}",
                    updated.status.to_db_string()
                ),
            ))
        }
        ProviderOperationStatus::Planned
        | ProviderOperationStatus::Submitted
        | ProviderOperationStatus::WaitingExternal => Ok(provider_hint_deferred(
            Some(updated.id),
            format!(
                "provider observation returned {}",
                updated.status.to_db_string()
            ),
        )),
    }
}

fn provider_hint_deferred(
    provider_operation_id: Option<Uuid>,
    reason: impl Into<String>,
) -> ProviderOperationHintVerified {
    ProviderOperationHintVerified {
        provider_operation_id,
        decision: ProviderOperationHintDecision::Defer,
        reason: Some(reason.into()),
    }
}

fn provider_hint_rejected(
    provider_operation_id: Option<Uuid>,
    reason: impl Into<String>,
) -> ProviderOperationHintVerified {
    ProviderOperationHintVerified {
        provider_operation_id,
        decision: ProviderOperationHintDecision::Reject,
        reason: Some(reason.into()),
    }
}

fn provider_observation_ref_reject_reason(
    operation: &OrderProviderOperation,
    observation: &ProviderOperationObservation,
) -> Option<String> {
    let expected = operation.provider_ref.as_deref()?;
    match observation.provider_ref.as_deref() {
        Some(observed) if observed == expected => None,
        Some(observed) => Some(format!(
            "provider observation ref {observed} does not match operation ref {expected}"
        )),
        None => Some(format!(
            "provider observation for {} did not include expected operation ref {expected}",
            operation.operation_type.to_db_string()
        )),
    }
}

fn provider_hint_observed_state(
    operation: &OrderProviderOperation,
    input: &VerifyProviderOperationHintInput,
    observation: &ProviderOperationObservation,
) -> Value {
    let mut observed_state = json!({
        "source": "temporal_provider_operation_hint_signal",
        "hint_id": input.signal.hint_id,
        "hint_kind": input.signal.hint_kind,
        "provider_observed_state": &observation.observed_state,
    });
    if let Some(tx_hash) = &observation.tx_hash {
        observed_state["tx_hash"] = json!(tx_hash);
    }
    if let Some(error) = &observation.error {
        observed_state["provider_error"] = error.clone();
    }
    if let Some(recovery) = across_log_recovery_marker(operation) {
        observed_state["router_recovery"] = recovery;
    }
    if !is_empty_json_object(&operation.observed_state) {
        observed_state["previous_observed_state"] = operation.observed_state.clone();
    }
    observed_state
}

fn across_log_recovery_marker(operation: &OrderProviderOperation) -> Option<Value> {
    if operation.operation_type != ProviderOperationType::AcrossBridge {
        return None;
    }
    if let Some(recovery) = find_across_log_recovery_marker(&operation.observed_state) {
        return Some(recovery);
    }
    if let Some(deposit_tx_hash) =
        recovered_across_deposit_tx_hash_from_value(&operation.observed_state)
    {
        return Some(json!({
            "kind": "across_deposit_log_recovery",
            "deposit_tx_hash": deposit_tx_hash,
            "source_provider_ref": &operation.provider_ref,
        }));
    }
    if operation.response.get("kind").and_then(Value::as_str) != Some("provider_receipt_checkpoint")
    {
        return None;
    }
    let deposit_tx_hash = provider_operation_tx_hash_from_value(&operation.response)
        .or_else(|| provider_operation_tx_hash_from_value(&operation.observed_state));
    Some(json!({
        "kind": "across_deposit_log_recovery",
        "deposit_tx_hash": deposit_tx_hash,
        "source_provider_ref": &operation.provider_ref,
    }))
}

fn find_across_log_recovery_marker(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    if object
        .get("kind")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "across_deposit_log_recovery")
    {
        return Some(value.clone());
    }
    object.values().find_map(find_across_log_recovery_marker)
}

fn recovered_across_deposit_tx_hash_from_value(value: &Value) -> Option<String> {
    let object = value.as_object()?;
    let has_recovery_shape = object.contains_key("deposit_id")
        || object
            .get("source")
            .and_then(Value::as_str)
            .is_some_and(|source| source == "temporal_across_deposit_log_recovery");
    if has_recovery_shape {
        if let Some(deposit_tx_hash) = object.get("deposit_tx_hash").and_then(Value::as_str) {
            return Some(deposit_tx_hash.to_string());
        }
    }
    object
        .get("previous_observed_state")
        .and_then(recovered_across_deposit_tx_hash_from_value)
}

fn provider_hint_signal_evidence(signal: &ProviderOperationHintSignal) -> Value {
    json!({
        "source": "temporal_provider_operation_hint_signal",
        "hint_id": signal.hint_id,
        "provider": signal.provider,
        "hint_kind": signal.hint_kind,
        "provider_ref": &signal.provider_ref,
        "evidence": &signal.evidence,
    })
}

fn unit_deposit_evidence_json(evidence: &ProviderOperationHintEvidence) -> Value {
    let mut value = json!({
        "tx_hash": &evidence.tx_hash,
        "address": &evidence.address,
        "transfer_index": evidence.transfer_index,
    });
    if let Some(amount) = &evidence.amount {
        value["amount"] = json!(amount);
    }
    value
}

async fn verify_unit_deposit_candidate(
    deps: &OrderActivityDeps,
    provider_address: &OrderProviderAddress,
    evidence: &ProviderOperationHintEvidence,
) -> Result<U256, String> {
    let backend_chain = backend_chain_for_id(&provider_address.chain).ok_or_else(|| {
        format!(
            "provider address chain {} is not supported by the temporal worker",
            provider_address.chain
        )
    })?;
    let chain = deps.chain_registry.get(&backend_chain).ok_or_else(|| {
        format!(
            "temporal worker has no chain implementation for {}",
            backend_chain.to_db_string()
        )
    })?;
    let asset = provider_address
        .asset
        .as_ref()
        .ok_or_else(|| "provider address does not declare an asset".to_string())?;
    let currency = Currency {
        chain: backend_chain,
        token: token_identifier(asset),
        decimals: currency_decimals(&backend_chain, asset),
    };

    match chain
        .verify_user_deposit_candidate(
            &provider_address.address,
            &currency,
            &evidence.tx_hash,
            evidence.transfer_index,
        )
        .await
    {
        Ok(UserDepositCandidateStatus::Verified(deposit)) => Ok(deposit.amount),
        Ok(UserDepositCandidateStatus::TxNotFound) => Err(format!(
            "candidate transaction {} was not found",
            evidence.tx_hash
        )),
        Ok(UserDepositCandidateStatus::TransferNotFound) => Err(format!(
            "candidate transaction {} does not pay provider address {} at transfer index {}",
            evidence.tx_hash, provider_address.address, evidence.transfer_index
        )),
        Err(err) => Err(format!(
            "failed to verify candidate deposit on chain: {err}"
        )),
    }
}

fn expected_provider_operation_amount(
    operation: &OrderProviderOperation,
    step: Option<&OrderExecutionStep>,
    hint_id: Uuid,
) -> Result<U256, String> {
    if let Some(expected) = operation
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
    {
        return U256::from_str(expected)
            .map_err(|err| format!("hint {hint_id}: operation expected_amount is invalid: {err}"));
    }
    if let Some(amount_in) = step.and_then(|step| step.amount_in.as_deref()) {
        return U256::from_str(amount_in)
            .map_err(|err| format!("hint {hint_id}: step amount_in is invalid: {err}"));
    }

    Ok(U256::from(1_u64))
}

fn token_identifier(asset: &AssetId) -> TokenIdentifier {
    match asset {
        AssetId::Native => TokenIdentifier::Native,
        AssetId::Reference(value) => TokenIdentifier::address(value.clone()),
    }
}

fn currency_decimals(chain: &ChainType, asset: &AssetId) -> u8 {
    match (chain, asset) {
        (ChainType::Bitcoin, AssetId::Native) => 8,
        (_, AssetId::Native) => 18,
        (_, AssetId::Reference(_)) => 8,
    }
}

fn is_decimal_u256(value: &str) -> bool {
    !value.is_empty() && U256::from_str_radix(value, 10).is_ok()
}

fn json_str_field<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn json_u64_field(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64).or_else(|| {
        value
            .get(key)
            .and_then(Value::as_str)
            .and_then(|raw| raw.parse::<u64>().ok())
    })
}

fn json_u256_string_field(value: &Value, key: &str) -> Option<U256> {
    value
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| U256::from_str_radix(raw, 10).ok())
}

fn recovery_step_asset_address(
    raw: &str,
    key: &'static str,
    operation: &OrderProviderOperation,
) -> Result<Address, ActivityError> {
    let asset = AssetId::parse(raw).map_err(|err| {
        activity_error(format!(
            "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
            operation.id
        ))
    })?;
    match asset {
        AssetId::Native => Ok(Address::ZERO),
        AssetId::Reference(address) => Address::from_str(&address).map_err(|err| {
            activity_error(format!(
                "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
                operation.id
            ))
        }),
    }
}

fn recovery_step_address(
    raw: &str,
    key: &'static str,
    operation: &OrderProviderOperation,
) -> Result<Address, ActivityError> {
    Address::from_str(raw).map_err(|err| {
        activity_error(format!(
            "Across checkpoint recovery invalid request.{key} for operation {}: {err}",
            operation.id
        ))
    })
}

fn evm_address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0_u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::from(bytes)
}

fn persisted_provider_operation_tx_hash(operation: &OrderProviderOperation) -> Option<String> {
    provider_operation_tx_hash_from_value(&operation.response)
        .or_else(|| provider_operation_tx_hash_from_value(&operation.observed_state))
}

fn provider_operation_tx_hash_from_value(value: &Value) -> Option<String> {
    value
        .get("tx_hash")
        .or_else(|| value.get("latest_tx_hash"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            value
                .get("tx_hashes")
                .and_then(Value::as_array)
                .and_then(|hashes| hashes.last())
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .or_else(|| {
            value
                .get("receipt")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("response")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("provider_response")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("previous_observed_state")
                .and_then(provider_operation_tx_hash_from_value)
        })
        .or_else(|| {
            value
                .get("provider_observed_state")
                .and_then(provider_operation_tx_hash_from_value)
        })
}

fn json_object_or_wrapped(value: Value) -> Value {
    if value.is_object() {
        value
    } else {
        json!({ "value": value })
    }
}

pub struct StepDispatchActivities;

#[activities]
impl StepDispatchActivities {
    /// Scar tissue: §13 step type dispatch provider trait family mapping.
    #[activity]
    pub async fn dispatch_step_provider_action(
        _ctx: ActivityContext,
        _input: DispatchStepProviderActionInput,
    ) -> Result<ProviderActionDispatchShape, ActivityError> {
        todo!("PR7: dispatch provider action for the active step")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chains::ChainRegistry;
    use router_core::{
        config::Settings,
        models::{
            CustodyVaultControlType, CustodyVaultStatus, MarketOrderAction, RouterOrderAction,
            RouterOrderStatus, RouterOrderType,
        },
        services::{
            asset_registry::{AssetSlot, RequiredCustodyRole},
            custody_action_executor::HyperliquidCallNetwork,
            ActionProviderHttpOptions,
        },
    };
    use sqlx_postgres::PgPool;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };

    const POSTGRES_PORT: u16 = 5432;
    const POSTGRES_USER: &str = "postgres";
    const POSTGRES_PASSWORD: &str = "password";
    const POSTGRES_DATABASE: &str = "postgres";

    struct TestDatabase {
        _container: ContainerAsync<GenericImage>,
        db: Database,
        pool: PgPool,
    }

    struct SeededRunningStep {
        order_id: Uuid,
        attempt_id: Uuid,
        step_id: Uuid,
    }

    #[tokio::test]
    async fn classify_stale_running_step_records_all_decisions() {
        let test_db = test_database().await;
        let deps = test_deps(test_db.db.clone());

        let durable = seed_running_step(&test_db.pool, true, true).await;
        let classified = classify_stale_running_step_for_deps(
            &deps,
            ClassifyStaleRunningStepInput {
                order_id: durable.order_id,
                attempt_id: durable.attempt_id,
                step_id: durable.step_id,
            },
        )
        .await
        .expect("classify durable progress");
        assert_eq!(
            classified.decision,
            StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress
        );
        assert_step_classification(
            &test_db.db,
            durable.step_id,
            "durable_provider_operation_waiting_external_progress",
        )
        .await;

        let ambiguous = seed_running_step(&test_db.pool, true, false).await;
        let classified = classify_stale_running_step_for_deps(
            &deps,
            ClassifyStaleRunningStepInput {
                order_id: ambiguous.order_id,
                attempt_id: ambiguous.attempt_id,
                step_id: ambiguous.step_id,
            },
        )
        .await
        .expect("classify ambiguous external window");
        assert_eq!(
            classified.decision,
            StaleRunningStepDecision::AmbiguousExternalSideEffectWindow
        );
        assert_step_classification(
            &test_db.db,
            ambiguous.step_id,
            "ambiguous_external_side_effect_window",
        )
        .await;

        let missing_checkpoint = seed_running_step(&test_db.pool, false, false).await;
        let classified = classify_stale_running_step_for_deps(
            &deps,
            ClassifyStaleRunningStepInput {
                order_id: missing_checkpoint.order_id,
                attempt_id: missing_checkpoint.attempt_id,
                step_id: missing_checkpoint.step_id,
            },
        )
        .await
        .expect("classify missing checkpoint");
        assert_eq!(
            classified.decision,
            StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint
        );
        assert_step_classification(
            &test_db.db,
            missing_checkpoint.step_id,
            "stale_running_step_without_checkpoint",
        )
        .await;
    }

    #[tokio::test]
    async fn hyperliquid_bridge_quotes_build_refund_quote_leg_shapes() {
        let registry = ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
            across: None,
            cctp: None,
            hyperunit_base_url: None,
            hyperunit_proxy_url: None,
            hyperliquid_base_url: Some("http://127.0.0.1:1".to_string()),
            velora: None,
            hyperliquid_network: HyperliquidCallNetwork::Testnet,
            hyperliquid_order_timeout_ms: 30_000,
        })
        .expect("hyperliquid bridge provider registry");
        let bridge = registry
            .bridge(ProviderId::HyperliquidBridge.as_str())
            .expect("hyperliquid bridge provider");
        let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let hl_usdc = test_asset("hyperliquid", "native");
        let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
        let withdrawal =
            hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());

        let deposit_quote = bridge
            .quote_bridge(BridgeQuoteRequest {
                source_asset: external_usdc.clone(),
                destination_asset: hl_usdc.clone(),
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "150000000".to_string(),
                    min_amount_out: Some("1".to_string()),
                },
                recipient_address: test_address(1),
                depositor_address: test_address(2),
                partial_fills_enabled: false,
            })
            .await
            .expect("quote HL deposit")
            .expect("HL deposit quote");
        let deposit_legs =
            refund_bridge_quote_legs(&deposit, &deposit_quote).expect("deposit quote legs");
        assert_eq!(deposit_legs.len(), 1);
        assert_eq!(
            deposit_legs[0].execution_step_type,
            OrderExecutionStepType::HyperliquidBridgeDeposit
        );
        assert_eq!(deposit_legs[0].amount_in, "150000000");
        assert_eq!(deposit_legs[0].amount_out, "150000000");

        let withdrawal_quote = bridge
            .quote_bridge(BridgeQuoteRequest {
                source_asset: hl_usdc,
                destination_asset: external_usdc,
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "150000000".to_string(),
                    min_amount_out: Some("1".to_string()),
                },
                recipient_address: test_address(1),
                depositor_address: test_address(2),
                partial_fills_enabled: false,
            })
            .await
            .expect("quote HL withdrawal")
            .expect("HL withdrawal quote");
        let withdrawal_legs = refund_bridge_quote_legs(&withdrawal, &withdrawal_quote)
            .expect("withdrawal quote legs");
        assert_eq!(withdrawal_legs.len(), 1);
        assert_eq!(
            withdrawal_legs[0].execution_step_type,
            OrderExecutionStepType::HyperliquidBridgeWithdrawal
        );
        assert_eq!(withdrawal_legs[0].amount_in, "150000000");
        assert_eq!(withdrawal_legs[0].amount_out, "149000000");
    }

    #[test]
    fn external_custody_hyperliquid_bridge_path_materializes_steps() {
        let planned_at = Utc::now();
        let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let hl_usdc = test_asset("hyperliquid", "native");
        let order = test_order(external_usdc.clone(), planned_at);
        let vault = test_custody_vault(
            &order,
            CustodyVaultRole::DestinationExecution,
            &external_usdc,
        );
        let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
        let withdrawal =
            hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
        let quoted_path = RefundQuotedPath {
            path: TransitionPath {
                id: "hl-deposit>hl-withdrawal".to_string(),
                transitions: vec![deposit.clone(), withdrawal.clone()],
            },
            amount_out: "149000000".to_string(),
            legs: vec![
                quote_leg_for_transition(&deposit, "150000000", "150000000", planned_at),
                quote_leg_for_transition(&withdrawal, "150000000", "149000000", planned_at),
            ],
        };

        let (legs, steps) =
            materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
                .expect("materialize HL bridge refund path");

        assert_eq!(legs.len(), 2);
        assert_eq!(steps.len(), 2);
        assert_eq!(
            steps[0].step_type,
            OrderExecutionStepType::HyperliquidBridgeDeposit
        );
        assert_eq!(steps[0].provider, ProviderId::HyperliquidBridge.as_str());
        assert_eq!(steps[0].input_asset, Some(external_usdc.clone()));
        assert_eq!(steps[0].output_asset, Some(hl_usdc.clone()));
        assert_eq!(steps[0].amount_in.as_deref(), Some("150000000"));
        assert_eq!(
            steps[0].request.get("source_custody_vault_id"),
            Some(&json!(vault.id))
        );
        assert_eq!(
            steps[0]
                .request
                .get("source_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );

        assert_eq!(
            steps[1].step_type,
            OrderExecutionStepType::HyperliquidBridgeWithdrawal
        );
        assert_eq!(steps[1].provider, ProviderId::HyperliquidBridge.as_str());
        assert_eq!(steps[1].input_asset, Some(hl_usdc));
        assert_eq!(steps[1].output_asset, Some(external_usdc.clone()));
        assert_eq!(steps[1].amount_in.as_deref(), Some("150000000"));
        assert_eq!(steps[1].min_amount_out.as_deref(), Some("149000000"));
        assert_eq!(
            steps[1].request.get("hyperliquid_custody_vault_id"),
            Some(&json!(vault.id))
        );
        assert_eq!(
            steps[1]
                .request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            steps[1]
                .request
                .get("hyperliquid_custody_vault_chain_id")
                .and_then(Value::as_str),
            Some("evm:42161")
        );
        assert_eq!(
            steps[1]
                .request
                .get("hyperliquid_custody_vault_asset_id")
                .and_then(Value::as_str),
            Some("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
        );
        assert_eq!(
            steps[1]
                .request
                .get("transfer_from_spot")
                .and_then(Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn external_custody_hyperliquid_bridge_role_gate_requires_destination_execution() {
        let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let hl_usdc = test_asset("hyperliquid", "native");
        let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
        let withdrawal =
            hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
        let path = TransitionPath {
            id: "hl-deposit>hl-withdrawal".to_string(),
            transitions: vec![deposit, withdrawal.clone()],
        };

        assert!(refund_path_compatible_with_position(
            RecoverablePositionKind::ExternalCustody,
            Some(CustodyVaultRole::DestinationExecution),
            &path,
        ));
        assert!(!refund_path_compatible_with_position(
            RecoverablePositionKind::ExternalCustody,
            Some(CustodyVaultRole::SourceDeposit),
            &path,
        ));

        let first_hop_withdrawal = TransitionPath {
            id: "hl-withdrawal".to_string(),
            transitions: vec![withdrawal],
        };
        assert!(!refund_path_compatible_with_position(
            RecoverablePositionKind::ExternalCustody,
            Some(CustodyVaultRole::DestinationExecution),
            &first_hop_withdrawal,
        ));
    }

    #[test]
    fn hyperliquid_bridge_withdrawal_marks_derived_destination_for_hydration() {
        let planned_at = Utc::now();
        let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let hl_usdc = test_asset("hyperliquid", "native");
        let order = test_order(external_usdc.clone(), planned_at);
        let transition =
            hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
        let leg = quote_leg_for_transition(&transition, "150000000", "149000000", planned_at);
        let custody = RefundHyperliquidBinding::DerivedDestinationExecution {
            asset: external_usdc,
        };

        let step = refund_transition_hyperliquid_bridge_withdrawal_step(
            &order,
            &transition,
            &leg,
            &custody,
            false,
            3,
            planned_at,
        )
        .expect("build derived HL withdrawal step");

        assert_eq!(
            step.request.get("hyperliquid_custody_vault_id"),
            Some(&Value::Null)
        );
        assert_eq!(
            step.request.get("hyperliquid_custody_vault_address"),
            Some(&Value::Null)
        );
        assert_eq!(
            step.request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            step.details
                .get("hyperliquid_custody_vault_status")
                .and_then(Value::as_str),
            Some("pending_derivation")
        );
        assert_eq!(
            step.request
                .get("recipient_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );
    }

    #[test]
    fn unit_refund_quote_legs_are_passthrough_shapes() {
        let planned_at = Utc::now();
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let order = test_order(btc.clone(), planned_at);
        let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
        let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

        let deposit_leg = refund_unit_deposit_quote_leg(&deposit, "30000", planned_at);
        assert_eq!(
            deposit_leg.execution_step_type,
            OrderExecutionStepType::UnitDeposit
        );
        assert_eq!(deposit_leg.amount_in, "30000");
        assert_eq!(deposit_leg.amount_out, "30000");
        assert_eq!(deposit_leg.raw, json!({}));

        let withdrawal_leg =
            refund_unit_withdrawal_quote_leg(&order, &withdrawal, "30000", planned_at);
        assert_eq!(
            withdrawal_leg.execution_step_type,
            OrderExecutionStepType::UnitWithdrawal
        );
        assert_eq!(withdrawal_leg.amount_in, "30000");
        assert_eq!(withdrawal_leg.amount_out, "30000");
        assert_eq!(
            withdrawal_leg
                .raw
                .get("recipient_address")
                .and_then(Value::as_str),
            Some(order.refund_address.as_str())
        );
    }

    #[test]
    fn external_custody_unit_path_materializes_deposit_and_withdrawal_steps() {
        let planned_at = Utc::now();
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let order = test_order(btc.clone(), planned_at);
        let vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &btc);
        let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
        let withdrawal =
            unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
        let quoted_path = RefundQuotedPath {
            path: TransitionPath {
                id: "unit-deposit>unit-withdrawal".to_string(),
                transitions: vec![deposit.clone(), withdrawal.clone()],
            },
            amount_out: "30000".to_string(),
            legs: vec![
                quote_leg_for_transition(&deposit, "30000", "30000", planned_at),
                quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at),
            ],
        };

        let (legs, steps) =
            materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
                .expect("materialize Unit refund path");

        assert_eq!(legs.len(), 2);
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].step_type, OrderExecutionStepType::UnitDeposit);
        assert_eq!(steps[0].provider, ProviderId::Unit.as_str());
        assert_eq!(steps[0].input_asset, Some(btc.clone()));
        assert_eq!(steps[0].output_asset, None);
        assert_eq!(steps[0].amount_in.as_deref(), Some("30000"));
        assert_eq!(
            steps[0].request.get("source_custody_vault_id"),
            Some(&json!(vault.id))
        );
        assert_eq!(
            steps[0].request.get("revert_custody_vault_id"),
            Some(&json!(vault.id))
        );
        assert_eq!(
            steps[0]
                .request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );

        assert_eq!(steps[1].step_type, OrderExecutionStepType::UnitWithdrawal);
        assert_eq!(steps[1].provider, ProviderId::Unit.as_str());
        assert_eq!(steps[1].input_asset, Some(hl_btc));
        assert_eq!(steps[1].output_asset, Some(btc));
        assert_eq!(steps[1].amount_in.as_deref(), Some("30000"));
        assert_eq!(steps[1].min_amount_out.as_deref(), Some("0"));
        assert_eq!(
            steps[1].request.get("hyperliquid_custody_vault_id"),
            Some(&Value::Null)
        );
        assert_eq!(
            steps[1]
                .request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );
        assert_eq!(
            steps[1]
                .details
                .get("hyperliquid_custody_vault_status")
                .and_then(Value::as_str),
            Some("pending_derivation")
        );
        assert_eq!(
            steps[1]
                .request
                .get("recipient_address")
                .and_then(Value::as_str),
            Some(order.refund_address.as_str())
        );
    }

    #[test]
    fn unit_withdrawal_builder_accepts_all_hyperliquid_binding_flavors() {
        let planned_at = Utc::now();
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let order = test_order(btc.clone(), planned_at);
        let vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_btc);
        let withdrawal =
            unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
        let leg = quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at);

        let explicit = refund_transition_unit_withdrawal_step(
            &order,
            &withdrawal,
            &leg,
            &RefundHyperliquidBinding::Explicit {
                vault_id: vault.id,
                address: vault.address.clone(),
                role: CustodyVaultRole::HyperliquidSpot,
                asset: Some(hl_btc),
            },
            true,
            0,
            planned_at,
        )
        .expect("explicit UnitWithdrawal");
        assert_eq!(
            explicit.request.get("hyperliquid_custody_vault_id"),
            Some(&json!(vault.id))
        );
        assert_eq!(
            explicit
                .details
                .get("hyperliquid_custody_vault_status")
                .and_then(Value::as_str),
            Some("bound")
        );

        let derived_spot = refund_transition_unit_withdrawal_step(
            &order,
            &withdrawal,
            &leg,
            &RefundHyperliquidBinding::DerivedSpot,
            false,
            1,
            planned_at,
        )
        .expect("derived spot UnitWithdrawal");
        assert_eq!(
            derived_spot.request.get("hyperliquid_custody_vault_id"),
            Some(&Value::Null)
        );
        assert_eq!(
            derived_spot
                .request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
        );
        assert_eq!(
            derived_spot
                .request
                .get("recipient_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );

        let derived_destination = refund_transition_unit_withdrawal_step(
            &order,
            &withdrawal,
            &leg,
            &RefundHyperliquidBinding::DerivedDestinationExecution { asset: btc },
            false,
            2,
            planned_at,
        )
        .expect("derived destination UnitWithdrawal");
        assert_eq!(
            derived_destination
                .request
                .get("hyperliquid_custody_vault_role")
                .and_then(Value::as_str),
            Some(CustodyVaultRole::DestinationExecution.to_db_string())
        );
        assert_eq!(
            derived_destination
                .request
                .get("hyperliquid_custody_vault_chain_id")
                .and_then(Value::as_str),
            Some("bitcoin")
        );
        assert_eq!(
            derived_destination
                .request
                .get("hyperliquid_custody_vault_asset_id")
                .and_then(Value::as_str),
            Some("native")
        );
    }

    #[test]
    fn unit_refund_compat_gate_accepts_external_deposit_and_hyperliquid_withdrawal() {
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
        let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

        assert!(refund_path_compatible_with_position(
            RecoverablePositionKind::ExternalCustody,
            Some(CustodyVaultRole::SourceDeposit),
            &TransitionPath {
                id: "unit-deposit>unit-withdrawal".to_string(),
                transitions: vec![deposit, withdrawal.clone()],
            },
        ));
        assert!(refund_path_compatible_with_position(
            RecoverablePositionKind::HyperliquidSpot,
            None,
            &TransitionPath {
                id: "unit-withdrawal".to_string(),
                transitions: vec![withdrawal.clone()],
            },
        ));
        assert!(!refund_path_compatible_with_position(
            RecoverablePositionKind::ExternalCustody,
            Some(CustodyVaultRole::SourceDeposit),
            &TransitionPath {
                id: "unit-withdrawal".to_string(),
                transitions: vec![withdrawal],
            },
        ));
    }

    #[test]
    fn refresh_cctp_quote_legs_materialize_burn_and_receive() {
        let expires_at = Utc::now();
        let base_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
        let arbitrum_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let transition = cctp_transition(base_usdc.clone(), arbitrum_usdc.clone());
        let quote = BridgeQuote {
            provider_id: "cctp".to_string(),
            amount_in: "1000000".to_string(),
            amount_out: "999000".to_string(),
            provider_quote: json!({ "message_hash": "0xcctp" }),
            expires_at,
        };

        let legs = refresh_cctp_quote_transition_legs(&transition, &quote);

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].transition_decl_id, "cctp");
        assert_eq!(legs[0].parent_transition_id(), "cctp");
        assert_eq!(
            legs[0].execution_step_type,
            OrderExecutionStepType::CctpBurn
        );
        assert_eq!(
            legs[0].input_asset,
            QuoteLegAsset::from_deposit_asset(&base_usdc)
        );
        assert_eq!(
            legs[0].output_asset,
            QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
        );
        assert_eq!(legs[0].amount_in, "1000000");
        assert_eq!(legs[0].amount_out, "999000");
        assert_eq!(
            legs[0]
                .raw
                .get("execution_step_type")
                .and_then(Value::as_str),
            Some(OrderExecutionStepType::CctpBurn.to_db_string())
        );

        assert_eq!(legs[1].transition_decl_id, "cctp:receive");
        assert_eq!(legs[1].parent_transition_id(), "cctp");
        assert_eq!(
            legs[1].execution_step_type,
            OrderExecutionStepType::CctpReceive
        );
        assert_eq!(
            legs[1].input_asset,
            QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
        );
        assert_eq!(
            legs[1].output_asset,
            QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
        );
        assert_eq!(legs[1].amount_in, "999000");
        assert_eq!(legs[1].amount_out, "999000");
        assert_eq!(
            legs[1].raw.get("kind").and_then(Value::as_str),
            Some(OrderExecutionStepType::CctpReceive.to_db_string())
        );
        assert_eq!(
            legs[1].raw.get("bridge_kind").and_then(Value::as_str),
            Some("cctp_bridge")
        );
    }

    #[test]
    fn refresh_spot_cross_token_quote_legs_parse_hyperliquid_legs() {
        let expires_at = Utc::now();
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let hl_usdc = test_asset("hyperliquid", "native");
        let hl_eth = test_asset("hyperliquid", "UETH");
        let quote = ExchangeQuote {
            provider_id: "hyperliquid".to_string(),
            amount_in: "30000".to_string(),
            amount_out: "40000000000000000".to_string(),
            min_amount_out: Some("1".to_string()),
            max_amount_in: None,
            provider_quote: json!({
                "kind": "spot_cross_token",
                "legs": [
                    {
                        "input_asset": QuoteLegAsset::from_deposit_asset(&hl_btc),
                        "output_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                        "amount_in": "30000",
                        "amount_out": "20000000"
                    },
                    {
                        "input_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                        "output_asset": QuoteLegAsset::from_deposit_asset(&hl_eth),
                        "amount_in": "20000000",
                        "amount_out": "40000000000000000"
                    }
                ]
            }),
            expires_at,
        };

        let legs = refresh_spot_cross_token_quote_transition_legs(
            "hl-trade",
            MarketOrderTransitionKind::HyperliquidTrade,
            ProviderId::Hyperliquid,
            &quote,
        )
        .expect("parse spot_cross_token legs");

        assert_eq!(legs.len(), 2);
        assert_eq!(legs[0].transition_decl_id, "hl-trade:leg:0");
        assert_eq!(legs[0].parent_transition_id(), "hl-trade");
        assert_eq!(
            legs[0].execution_step_type,
            OrderExecutionStepType::HyperliquidTrade
        );
        assert_eq!(
            legs[0].input_asset,
            QuoteLegAsset::from_deposit_asset(&hl_btc)
        );
        assert_eq!(
            legs[0].output_asset,
            QuoteLegAsset::from_deposit_asset(&hl_usdc)
        );
        assert_eq!(legs[0].amount_in, "30000");
        assert_eq!(legs[0].amount_out, "20000000");
        assert_eq!(legs[1].transition_decl_id, "hl-trade:leg:1");
        assert_eq!(legs[1].parent_transition_id(), "hl-trade");
        assert_eq!(
            legs[1].input_asset,
            QuoteLegAsset::from_deposit_asset(&hl_usdc)
        );
        assert_eq!(
            legs[1].output_asset,
            QuoteLegAsset::from_deposit_asset(&hl_eth)
        );
        assert_eq!(legs[1].amount_in, "20000000");
        assert_eq!(legs[1].amount_out, "40000000000000000");
    }

    #[test]
    fn refresh_unit_quote_legs_are_passthrough_shapes() {
        let expires_at = Utc::now();
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
        let withdrawal =
            unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
        let reserve_quote = refresh_bitcoin_fee_reserve_quote(U256::from(2500_u64));

        let deposit_leg =
            refresh_unit_deposit_quote_leg(&deposit, "30000", expires_at, reserve_quote.clone());
        assert_eq!(
            deposit_leg.execution_step_type,
            OrderExecutionStepType::UnitDeposit
        );
        assert_eq!(
            deposit_leg.input_asset,
            QuoteLegAsset::from_deposit_asset(&btc)
        );
        assert_eq!(
            deposit_leg.output_asset,
            QuoteLegAsset::from_deposit_asset(&hl_btc)
        );
        assert_eq!(deposit_leg.amount_in, "30000");
        assert_eq!(deposit_leg.amount_out, "30000");
        assert_eq!(deposit_leg.raw, reserve_quote);
        assert_eq!(
            deposit_leg
                .raw
                .pointer("/source_fee_reserve/reserve_bps")
                .and_then(Value::as_u64),
            Some(REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS)
        );

        let withdrawal_leg =
            refresh_unit_withdrawal_quote_leg(&withdrawal, "30000", "0xrecipient", expires_at);
        assert_eq!(
            withdrawal_leg.execution_step_type,
            OrderExecutionStepType::UnitWithdrawal
        );
        assert_eq!(
            withdrawal_leg.input_asset,
            QuoteLegAsset::from_deposit_asset(&hl_btc)
        );
        assert_eq!(
            withdrawal_leg.output_asset,
            QuoteLegAsset::from_deposit_asset(&btc)
        );
        assert_eq!(withdrawal_leg.amount_in, "30000");
        assert_eq!(withdrawal_leg.amount_out, "30000");
        assert_eq!(
            withdrawal_leg
                .raw
                .get("recipient_address")
                .and_then(Value::as_str),
            Some("0xrecipient")
        );
        assert_eq!(
            withdrawal_leg
                .raw
                .pointer("/hyperliquid_core_activation_fee/amount_raw")
                .and_then(Value::as_str),
            Some(
                REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW
                    .to_string()
                    .as_str()
            )
        );
    }

    #[test]
    fn refresh_unit_deposit_fee_reserve_reads_original_quote_leg() {
        let now = Utc::now();
        let btc = test_asset("bitcoin", "native");
        let hl_btc = test_asset("hyperliquid", "UBTC");
        let transition = unit_deposit_transition(btc.clone(), hl_btc, CanonicalAsset::Btc);
        let fee_reserve = U256::from(42_000_u64);
        let leg = refresh_unit_deposit_quote_leg(
            &transition,
            "100000",
            now,
            refresh_bitcoin_fee_reserve_quote(fee_reserve),
        );
        let quote = test_market_order_quote(
            btc,
            transition.output.asset.clone(),
            vec![leg],
            now + chrono::Duration::minutes(5),
            now,
        );

        assert_eq!(
            refresh_unit_deposit_fee_reserve(&quote, &transition).expect("read fee reserve"),
            Some(fee_reserve)
        );

        let mut no_reserve_quote = quote.clone();
        no_reserve_quote.provider_quote = json!({ "legs": [] });
        assert_eq!(
            refresh_unit_deposit_fee_reserve(&no_reserve_quote, &transition)
                .expect("missing fee reserve is allowed"),
            None
        );
    }

    #[test]
    fn refresh_hyperliquid_spot_send_reserve_math_matches_legacy() {
        assert_eq!(
            refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000001")
                .expect("subtract reserve"),
            "1"
        );
        assert_eq!(
            refresh_add_hyperliquid_spot_send_quote_gas_reserve("amount_in", "1")
                .expect("add reserve"),
            "1000001"
        );
        assert!(
            refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000000").is_err(),
            "amount must strictly exceed the reserve"
        );
        assert!(
            refresh_add_hyperliquid_spot_send_quote_gas_reserve(
                "amount_in",
                &U256::MAX.to_string(),
            )
            .is_err(),
            "addition must reject overflow"
        );
    }

    async fn test_database() -> TestDatabase {
        let image = GenericImage::new("postgres", "18-alpine")
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_USER", POSTGRES_USER)
            .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_wal_senders=10",
                "-c",
                "max_replication_slots=10",
            ]);

        let container = image.start().await.expect("start Postgres testcontainer");
        let port = container
            .get_host_port_ipv4(POSTGRES_PORT.tcp())
            .await
            .expect("read Postgres testcontainer port");
        let database_url = format!(
            "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
        );
        let db = Database::connect(&database_url, 4, 1)
            .await
            .expect("connect migrated test database");
        let pool = PgPool::connect(&database_url)
            .await
            .expect("connect raw test pool");

        TestDatabase {
            _container: container,
            db,
            pool,
        }
    }

    fn test_deps(db: Database) -> OrderActivityDeps {
        let settings = Arc::new(test_settings());
        let chain_registry = Arc::new(ChainRegistry::new());
        let action_providers = Arc::new(
            ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
                across: None,
                cctp: None,
                hyperunit_base_url: None,
                hyperunit_proxy_url: None,
                hyperliquid_base_url: None,
                velora: None,
                hyperliquid_network: HyperliquidCallNetwork::Testnet,
                hyperliquid_order_timeout_ms: 30_000,
            })
            .expect("empty action provider registry"),
        );
        let custody_executor = Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            chain_registry.clone(),
        ));
        OrderActivityDeps::new(db, action_providers, custody_executor, chain_registry)
    }

    fn test_settings() -> Settings {
        let dir = tempfile::tempdir().expect("settings tempdir");
        let path = dir.path().join("router-master-key.hex");
        std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
        Settings::load(&path).expect("load test settings")
    }

    fn test_asset(chain: &str, asset: &str) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).expect("valid test chain"),
            asset: AssetId::parse(asset).expect("valid test asset"),
        }
    }

    fn test_address(byte: u8) -> String {
        format!("0x{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}")
    }

    fn test_order(source_asset: DepositAsset, now: chrono::DateTime<Utc>) -> RouterOrder {
        RouterOrder {
            id: Uuid::now_v7(),
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Refunding,
            funding_vault_id: None,
            source_asset: source_asset.clone(),
            destination_asset: source_asset,
            recipient_address: test_address(9),
            refund_address: test_address(8),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "150000000".to_string(),
                    min_amount_out: Some("1".to_string()),
                },
                slippage_bps: Some(100),
            }),
            action_timeout_at: now + chrono::Duration::hours(1),
            idempotency_key: None,
            workflow_trace_id: Uuid::now_v7().simple().to_string(),
            workflow_parent_span_id: "0000000000000000".to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    fn test_custody_vault(
        order: &RouterOrder,
        role: CustodyVaultRole,
        asset: &DepositAsset,
    ) -> CustodyVault {
        CustodyVault {
            id: Uuid::now_v7(),
            order_id: Some(order.id),
            role,
            visibility: CustodyVaultVisibility::Internal,
            chain: asset.chain.clone(),
            asset: Some(asset.asset.clone()),
            address: test_address(7),
            control_type: CustodyVaultControlType::RouterDerivedKey,
            derivation_salt: None,
            signer_ref: None,
            status: CustodyVaultStatus::Active,
            metadata: json!({}),
            created_at: order.created_at,
            updated_at: order.created_at,
        }
    }

    fn hyperliquid_bridge_deposit_transition(
        input: DepositAsset,
        output: DepositAsset,
    ) -> TransitionDecl {
        TransitionDecl {
            id: "hl-deposit".to_string(),
            kind: MarketOrderTransitionKind::HyperliquidBridgeDeposit,
            provider: ProviderId::HyperliquidBridge,
            input: AssetSlot {
                asset: input.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            output: AssetSlot {
                asset: output.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: MarketOrderNode::External(input),
            to: MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical: CanonicalAsset::Usdc,
            },
        }
    }

    fn hyperliquid_bridge_withdrawal_transition(
        input: DepositAsset,
        output: DepositAsset,
    ) -> TransitionDecl {
        TransitionDecl {
            id: "hl-withdrawal".to_string(),
            kind: MarketOrderTransitionKind::HyperliquidBridgeWithdrawal,
            provider: ProviderId::HyperliquidBridge,
            input: AssetSlot {
                asset: input.clone(),
                required_custody_role: RequiredCustodyRole::HyperliquidSpot,
            },
            output: AssetSlot {
                asset: output.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical: CanonicalAsset::Usdc,
            },
            to: MarketOrderNode::External(output),
        }
    }

    fn unit_deposit_transition(
        input: DepositAsset,
        output: DepositAsset,
        canonical: CanonicalAsset,
    ) -> TransitionDecl {
        TransitionDecl {
            id: "unit-deposit".to_string(),
            kind: MarketOrderTransitionKind::UnitDeposit,
            provider: ProviderId::Unit,
            input: AssetSlot {
                asset: input.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            output: AssetSlot {
                asset: output,
                required_custody_role: RequiredCustodyRole::HyperliquidSpot,
            },
            from: MarketOrderNode::External(input),
            to: MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical,
            },
        }
    }

    fn unit_withdrawal_transition(
        input: DepositAsset,
        output: DepositAsset,
        canonical: CanonicalAsset,
    ) -> TransitionDecl {
        TransitionDecl {
            id: "unit-withdrawal".to_string(),
            kind: MarketOrderTransitionKind::UnitWithdrawal,
            provider: ProviderId::Unit,
            input: AssetSlot {
                asset: input,
                required_custody_role: RequiredCustodyRole::HyperliquidSpot,
            },
            output: AssetSlot {
                asset: output.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical,
            },
            to: MarketOrderNode::External(output),
        }
    }

    fn cctp_transition(input: DepositAsset, output: DepositAsset) -> TransitionDecl {
        TransitionDecl {
            id: "cctp".to_string(),
            kind: MarketOrderTransitionKind::CctpBridge,
            provider: ProviderId::Cctp,
            input: AssetSlot {
                asset: input.clone(),
                required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
            },
            output: AssetSlot {
                asset: output.clone(),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: MarketOrderNode::External(input),
            to: MarketOrderNode::External(output),
        }
    }

    fn quote_leg_for_transition(
        transition: &TransitionDecl,
        amount_in: &str,
        amount_out: &str,
        expires_at: chrono::DateTime<Utc>,
    ) -> QuoteLeg {
        QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &transition.input.asset,
            output_asset: &transition.output.asset,
            amount_in,
            amount_out,
            expires_at,
            raw: json!({
                "kind": match transition.kind {
                    MarketOrderTransitionKind::HyperliquidBridgeDeposit => "hyperliquid_native_bridge",
                    MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
                    _ => transition.kind.as_str(),
                },
            }),
        })
        .with_execution_step_type(execution_step_type_for_transition_kind(
            transition.kind,
        ))
    }

    fn test_market_order_quote(
        source_asset: DepositAsset,
        destination_asset: DepositAsset,
        legs: Vec<QuoteLeg>,
        expires_at: chrono::DateTime<Utc>,
        created_at: chrono::DateTime<Utc>,
    ) -> MarketOrderQuote {
        MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: None,
            source_asset,
            destination_asset,
            recipient_address: test_address(1),
            provider_id: "path:test".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "100000".to_string(),
            amount_out: "100000".to_string(),
            min_amount_out: Some("1".to_string()),
            max_amount_in: None,
            slippage_bps: Some(100),
            provider_quote: json!({ "legs": legs }),
            usd_valuation: json!({}),
            expires_at,
            created_at,
        }
    }

    async fn seed_running_step(
        pool: &PgPool,
        with_checkpoint: bool,
        with_durable_provider_operation: bool,
    ) -> SeededRunningStep {
        let order_id = Uuid::now_v7();
        let attempt_id = Uuid::now_v7();
        let leg_id = Uuid::now_v7();
        let step_id = Uuid::now_v7();
        let now = Utc::now();
        let started_at = now - chrono::Duration::minutes(10);
        let trace_id = order_id.simple().to_string();
        let parent_span_id = trace_id[..16].to_string();
        let details = if with_checkpoint {
            json!({
                "provider_side_effect_checkpoint": {
                    "kind": "provider_side_effect_about_to_fire",
                    "reason": "about_to_fire_provider_side_effect",
                    "recorded_at": started_at.to_rfc3339(),
                    "scar_tissue": "§6"
                }
            })
        } else {
            json!({})
        };

        sqlx_core::query::query(
            r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                action_timeout_at,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, 'market_order', 'executing', 'evm:8453', 'native',
                'evm:8453', 'native', '0x0000000000000000000000000000000000000001',
                '0x0000000000000000000000000000000000000002',
                $2, $3, $4, $5, $5
            )
            "#,
        )
        .bind(order_id)
        .bind(now + chrono::Duration::hours(1))
        .bind(trace_id)
        .bind(parent_span_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert router order");

        sqlx_core::query::query(
            r#"
            INSERT INTO market_order_actions (
                order_id,
                order_kind,
                amount_in,
                min_amount_out,
                amount_out,
                max_amount_in,
                created_at,
                updated_at,
                slippage_bps
            )
            VALUES ($1, 'exact_in', '100', '1', NULL, NULL, $2, $2, 100)
            "#,
        )
        .bind(order_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert market order action");

        sqlx_core::query::query(
            r#"
            INSERT INTO order_execution_attempts (
                id,
                order_id,
                attempt_index,
                attempt_kind,
                status,
                failure_reason_json,
                input_custody_snapshot_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, 1, 'primary_execution', 'active', '{}'::jsonb, '{}'::jsonb, $3, $3)
            "#,
        )
        .bind(attempt_id)
        .bind(order_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert execution attempt");

        sqlx_core::query::query(
            r#"
            INSERT INTO order_execution_legs (
                id,
                order_id,
                execution_attempt_id,
                transition_decl_id,
                leg_index,
                leg_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                expected_amount_out,
                min_amount_out,
                started_at,
                details_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, 'test-transition', 0, 'swap', 'velora',
                'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', '100', '1', $4, '{}'::jsonb, '{}'::jsonb, $5, $5
            )
            "#,
        )
        .bind(leg_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(started_at)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert execution leg");

        sqlx_core::query::query(
            r#"
            INSERT INTO order_execution_steps (
                id,
                order_id,
                execution_attempt_id,
                execution_leg_id,
                transition_decl_id,
                step_index,
                step_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                min_amount_out,
                idempotency_key,
                attempt_count,
                started_at,
                details_json,
                request_json,
                response_json,
                error_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, 'test-transition', 0, 'universal_router_swap',
                'velora', 'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', '1', $5, 1, $6, $7, '{}'::jsonb, '{}'::jsonb,
                '{}'::jsonb, '{}'::jsonb, $8, $8
            )
            "#,
        )
        .bind(step_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(leg_id)
        .bind(format!("order:{order_id}:execution:0"))
        .bind(started_at)
        .bind(details)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert execution step");

        if with_durable_provider_operation {
            sqlx_core::query::query(
                r#"
                INSERT INTO order_provider_operations (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_step_id,
                    provider,
                    operation_type,
                    provider_ref,
                    status,
                    request_json,
                    response_json,
                    observed_state_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'velora', 'universal_router_swap',
                    'provider-ref', 'waiting_external', '{}'::jsonb,
                    '{"receipt":"recorded"}'::jsonb, '{}'::jsonb, $5, $5
                )
                "#,
            )
            .bind(Uuid::now_v7())
            .bind(order_id)
            .bind(attempt_id)
            .bind(step_id)
            .bind(now)
            .execute(pool)
            .await
            .expect("insert durable provider operation");
        }

        SeededRunningStep {
            order_id,
            attempt_id,
            step_id,
        }
    }

    async fn assert_step_classification(db: &Database, step_id: Uuid, expected_reason: &str) {
        let step = db
            .orders()
            .get_execution_step(step_id)
            .await
            .expect("load classified step");
        assert_eq!(
            step.details
                .get("stale_running_step_classification")
                .and_then(|classification| classification.get("reason"))
                .and_then(Value::as_str),
            Some(expected_reason)
        );
    }
}
