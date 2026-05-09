#![allow(dead_code)]

// Activity function constants are registered before the later rewrite slices invoke them.
use std::sync::Arc;

use chrono::Utc;
use router_core::{
    db::{
        Database, ExecutionAttemptPlan, PersistStepCompletionRecord, RefreshedExecutionAttemptPlan,
    },
    error::{RouterCoreError, RouterCoreResult},
    models::{
        MarketOrderKind, MarketOrderKindType, MarketOrderQuote, OrderExecutionAttemptKind,
        OrderExecutionAttemptStatus, OrderExecutionStep, OrderExecutionStepStatus,
        OrderExecutionStepType, OrderProviderAddress, OrderProviderOperation,
        ProviderOperationStatus, RouterOrder, RouterOrderQuote,
    },
    services::{
        action_providers::{
            BridgeExecutionRequest, ExchangeExecutionRequest, ExchangeQuote, ExchangeQuoteRequest,
            ProviderExecutionStatePatch, UnitDepositStepRequest, UnitWithdrawalStepRequest,
        },
        asset_registry::{MarketOrderTransitionKind, ProviderId, TransitionDecl},
        custody_action_executor::CustodyActionRequest,
        market_order_planner::MarketOrderPlanRemainingStart,
        quote_legs::{execution_step_type_for_transition_kind, QuoteLeg, QuoteLegAsset},
        ActionProviderRegistry, CustodyActionExecutor, CustodyActionReceipt,
        MarketOrderRoutePlanner, ProviderExecutionIntent, ProviderExecutionState,
        ProviderOperationIntent,
    },
};
use serde_json::{json, Value};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use uuid::Uuid;

use super::types::{
    AcrossOnchainLogRecovered, BoundaryPersisted, CancelTimedOutHyperliquidTradeInput,
    ClassifyStepFailureInput, ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput,
    DispatchStepProviderActionInput, ExecuteStepInput, ExecutionPlan, FailedAttemptSnapshotWritten,
    FinalizeOrderOrRefundInput, FinalizedOrder, HyperliquidTradeCancelRecorded,
    LoadOrderExecutionStateInput, MarkOrderCompletedInput, MaterializeExecutionAttemptInput,
    MaterializeRefreshedAttemptInput, MaterializeRefundPlanInput, MaterializeRetryAttemptInput,
    MaterializedExecutionAttempt, OrderCompleted, OrderExecutionState, OrderTerminalStatus,
    OrderWorkflowPhase, PersistProviderOperationStatusInput, PersistProviderReceiptInput,
    PersistStepFailedInput, PersistStepReadyToFireInput, PersistStepTerminalStatusInput,
    PersistenceBoundary, PollProviderOperationHintsInput, ProviderActionDispatchShape,
    ProviderOperationHintVerified, ProviderOperationHintsPolled, RecoverAcrossOnchainLogInput,
    RefreshedAttemptMaterialized, RefreshedQuoteAttemptOutcome, RefreshedQuoteAttemptShape,
    RefundPlanShape, SettleProviderStepInput, SingleRefundPosition,
    StaleQuoteRefreshUntenableReason, StepExecuted, StepExecutionOutcome, StepFailureDecision,
    VerifyProviderOperationHintInput, WorkflowExecutionStep, WriteFailedAttemptSnapshotInput,
};

const MAX_EXECUTION_ATTEMPTS: i32 = 2;
const MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION: usize = 8;

#[derive(Clone)]
pub struct OrderActivityDeps {
    pub db: Database,
    pub action_providers: Arc<ActionProviderRegistry>,
    pub custody_action_executor: Arc<CustodyActionExecutor>,
    pub planner: MarketOrderRoutePlanner,
}

impl OrderActivityDeps {
    #[must_use]
    pub fn new(
        db: Database,
        action_providers: Arc<ActionProviderRegistry>,
        custody_action_executor: Arc<CustodyActionExecutor>,
    ) -> Self {
        let planner = MarketOrderRoutePlanner::new(action_providers.asset_registry());
        Self {
            db,
            action_providers,
            custody_action_executor,
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
        let record = deps
            .db
            .orders()
            .materialize_primary_execution_attempt(
                input.order_id,
                ExecutionAttemptPlan {
                    legs: input.plan.legs,
                    steps: input.plan.steps,
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
        if input.execution.outcome == StepExecutionOutcome::Waiting {
            return Err(activity_error(format!(
                "step {} is waiting for provider observation; PR7 adds the observation/recovery path",
                input.execution.step_id
            )));
        }
        let deps = self.deps()?;
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
            terminal_status => Err(activity_error(format!(
                "finalize_order_or_refund does not handle {terminal_status:?} in PR4b"
            ))),
        }
    }
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

async fn execute_running_step(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<StepCompletion, ActivityError> {
    let intent = match step.step_type {
        OrderExecutionStepType::WaitForDeposit | OrderExecutionStepType::Refund => {
            return Err(activity_error(format!(
                "{} is not executable in PR4a OrderWorkflow",
                step.step_type.to_db_string()
            )));
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
            let request = BridgeExecutionRequest::cctp_receive_from_value(&step.request)
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

fn refreshed_market_order_quote_single(
    order: &RouterOrder,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    legs: Vec<QuoteLeg>,
    exchange_quote: &ExchangeQuote,
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
        amount_in: exchange_quote.amount_in.clone(),
        amount_out: exchange_quote.amount_out.clone(),
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
        expires_at: exchange_quote.expires_at,
        created_at,
    }
}

fn refresh_exchange_quote_transition_legs_single(
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
    if kind != "universal_router_swap" {
        return Err(activity_error(format!(
            "unsupported single-leg refreshed exchange quote kind {kind:?}"
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

pub struct RefundActivities;

#[activities]
impl RefundActivities {
    /// Scar tissue: §8 refund position discovery and §16.1 balance reads.
    #[activity]
    pub async fn discover_single_refund_position(
        _ctx: ActivityContext,
        _input: DiscoverSingleRefundPositionInput,
    ) -> Result<SingleRefundPosition, ActivityError> {
        // TODO(PR4, brief §6 invariant 4; scar §8): require exactly one recoverable position.
        todo!("PR4: discover single recoverable refund position")
    }

    /// Scar tissue: §9 refund tree and §16.1 refund materialisation.
    #[activity]
    pub async fn materialize_refund_plan(
        _ctx: ActivityContext,
        _input: MaterializeRefundPlanInput,
    ) -> Result<RefundPlanShape, ActivityError> {
        // TODO(PR4, brief §6 invariant 2; scar §5): refund attempts never get retry branches.
        todo!("PR4: materialize refund plan")
    }
}

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
    /// PR5a supports single-leg ExactIn market orders only. PR5b owns the deferred branches:
    /// multi-leg ExactIn-forward iteration, ExactOut-backward iteration, Bitcoin unit-deposit fee
    /// reserve, and Hyperliquid spot-send gas reserve.
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
            .and_then(|leg_id| stale_legs.into_iter().find(|leg| leg.id == leg_id))
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
        if original_quote.order_kind != MarketOrderKindType::ExactIn {
            todo!("PR5b: multi-leg refresh");
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
        if transitions.len() != 1 {
            todo!("PR5b: multi-leg refresh");
        }
        let transition = transitions
            .first()
            .expect("single transition checked above");
        if transition.kind != MarketOrderTransitionKind::UniversalRouterSwap {
            todo!("PR5b: multi-leg refresh");
        }
        let step_request =
            match ExchangeExecutionRequest::universal_router_swap_from_value(&failed_step.request)
                .map_err(activity_error_from_display)?
            {
                ExchangeExecutionRequest::UniversalRouterSwap(request) => request,
                _ => {
                    return Err(activity_error(format!(
                        "step {} is not a universal-router swap request",
                        failed_step.id
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
                    amount_in: original_quote.amount_in.clone(),
                    min_amount_out: original_quote.min_amount_out.clone(),
                },
                sender_address: Some(
                    step_request
                        .source_custody_vault_address
                        .clone()
                        .unwrap_or_else(|| source_vault.deposit_vault_address.clone()),
                ),
                recipient_address: if step_request.recipient_address.is_empty() {
                    order.recipient_address.clone()
                } else {
                    step_request.recipient_address.clone()
                },
            })
            .await
            .map_err(activity_error_from_display)?
            .ok_or_else(|| {
                activity_error(format!(
                    "exchange provider {} returned no refreshed quote",
                    exchange.id()
                ))
            })?;
        if let Some(min_amount_out) = original_quote.min_amount_out.as_deref() {
            let quoted_amount_out =
                parse_refresh_amount("amount_out", &refreshed_quote.amount_out)?;
            let min_amount_out_raw = parse_refresh_amount("min_amount_out", min_amount_out)?;
            if quoted_amount_out < min_amount_out_raw {
                return Ok(RefreshedQuoteAttemptShape {
                    outcome: RefreshedQuoteAttemptOutcome::Untenable {
                        order_id: input.order_id,
                        stale_attempt_id: input.stale_attempt_id,
                        failed_step_id: input.failed_step_id,
                        reason: StaleQuoteRefreshUntenableReason::RefreshedExactInOutputBelowMinAmountOut {
                            amount_out: refreshed_quote.amount_out,
                            min_amount_out: min_amount_out.to_string(),
                        },
                    },
                });
            }
        }
        let refreshed_legs = refresh_exchange_quote_transition_legs_single(
            transition.id.as_str(),
            transition.kind,
            transition.provider,
            &refreshed_quote,
        )?;
        let refreshed_quote = refreshed_market_order_quote_single(
            &order,
            &original_quote,
            &transitions,
            refreshed_legs,
            &refreshed_quote,
            Utc::now(),
        );
        let now = Utc::now();
        let mut plan = deps
            .planner
            .plan_remaining(
                &order,
                &source_vault,
                &refreshed_quote,
                MarketOrderPlanRemainingStart {
                    transition_decl_id: &transition.id,
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
            plan,
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

pub struct ProviderObservationActivities;

#[activities]
impl ProviderObservationActivities {
    /// Scar tissue: §10 provider operation hint flow and verifier dispatch.
    #[activity]
    pub async fn verify_provider_operation_hint(
        _ctx: ActivityContext,
        _input: VerifyProviderOperationHintInput,
    ) -> Result<ProviderOperationHintVerified, ActivityError> {
        todo!("PR7: verify provider operation hint against provider API")
    }

    /// Scar tissue: §10 provider hint recovery. This is the polling half of the user-approved
    /// signal-first plus fallback shape for brief §8.4.
    #[activity]
    pub async fn poll_provider_operation_hints(
        _ctx: ActivityContext,
        _input: PollProviderOperationHintsInput,
    ) -> Result<ProviderOperationHintsPolled, ActivityError> {
        todo!("PR7: poll provider hints as a fallback to signals")
    }

    /// Scar tissue: §11 Across on-chain log recovery.
    #[activity]
    pub async fn recover_across_onchain_log(
        _ctx: ActivityContext,
        _input: RecoverAcrossOnchainLogInput,
    ) -> Result<AcrossOnchainLogRecovered, ActivityError> {
        todo!("PR4: recover Across fill from on-chain logs")
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
