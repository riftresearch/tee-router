#![allow(dead_code)]

// Activity function constants are intentionally registered before PR3 workflow logic invokes them.
use std::sync::Arc;

use chrono::Utc;
use router_core::{
    db::{Database, PersistStepCompletionRecord, SingleStepExecutionStartPlan},
    error::{RouterCoreError, RouterCoreResult},
    models::{
        OrderExecutionStep, OrderExecutionStepType, OrderProviderAddress, OrderProviderOperation,
        ProviderOperationStatus, RouterOrderQuote,
    },
    services::{
        action_providers::{
            BridgeExecutionRequest, ExchangeExecutionRequest, ProviderExecutionStatePatch,
            UnitDepositStepRequest, UnitWithdrawalStepRequest,
        },
        custody_action_executor::CustodyActionRequest,
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
    DispatchStepProviderActionInput, ExecuteStepInput, ExecutionStartPersisted,
    FailedAttemptSnapshotWritten, FinalizeOrderOrRefundInput, FinalizedOrder,
    HyperliquidTradeCancelRecorded, LoadOrderExecutionStateInput, LoadOrderInput, LoadedOrder,
    MarkOrderCompletedInput, MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
    MaterializeRefundPlanInput, MaterializedExecutionAttempt, OrderCompleted, OrderExecutionState,
    PersistExecutionStartInput, PersistProviderOperationStatusInput, PersistProviderReceiptInput,
    PersistStepCompletionInput, PersistStepFailedInput, PersistStepReadyToFireInput,
    PersistStepTerminalStatusInput, PollProviderOperationHintsInput, ProviderActionDispatchShape,
    ProviderOperationHintVerified, ProviderOperationHintsPolled, RecoverAcrossOnchainLogInput,
    RefreshedAttemptMaterialized, RefreshedQuoteAttemptShape, RefundPlanShape,
    SettleProviderStepInput, SingleRefundPosition, SingleStepExecutionPlan,
    StepCompletionPersisted, StepExecuted, StepExecutionOutcome, StepFailureDecision,
    VerifyProviderOperationHintInput, WriteFailedAttemptSnapshotInput,
};

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

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, ActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| activity_error("order activities are not configured"))
    }
}

#[activities]
impl OrderActivities {
    /// Scar tissue: §3 phase pass and §4 order/vault/step state alignment.
    #[activity]
    pub async fn load_order(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadOrderInput,
    ) -> Result<LoadedOrder, ActivityError> {
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
        if route.legs.len() != 1 || route.steps.len() != 1 {
            return Err(activity_error(format!(
                "PR3 OrderWorkflow requires exactly one leg and one step, got {} legs and {} steps",
                route.legs.len(),
                route.steps.len()
            )));
        }

        tracing::info!(
            order_id = %order.id,
            event_name = "order.execution_plan_started",
            "order.execution_plan_started"
        );
        let leg = route.legs.into_iter().next().expect("len checked");
        let step = route.steps.into_iter().next().expect("len checked");
        Ok(LoadedOrder {
            order,
            plan: SingleStepExecutionPlan {
                path_id: route.path_id,
                transition_decl_ids: route.transition_decl_ids,
                leg,
                step,
            },
        })
    }

    /// Scar tissue: §2.1 `AfterExecutionLegsPersisted`, §3 phase 2, and §4 state alignment.
    /// This happy-path composite composes brief §5 `materialise_plan` + `record_attempt` +
    /// `transition_order_status(Funded→Executing)` for the single-step case.
    #[activity]
    pub async fn persist_execution_start(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistExecutionStartInput,
    ) -> Result<ExecutionStartPersisted, ActivityError> {
        let deps = self.deps()?;
        let record = deps
            .db
            .orders()
            .persist_single_step_execution_start(
                input.order_id,
                SingleStepExecutionStartPlan {
                    leg: input.plan.leg,
                    step: input.plan.step,
                },
                Utc::now(),
            )
            .await
            .map_err(activity_error_from_display)?;
        tracing::info!(
            order_id = %record.order.id,
            attempt_id = %record.attempt.id,
            step_id = %record.step.id,
            leg_id = %record.leg.id,
            event_name = "order.execution_plan_materialized",
            "order.execution_plan_materialized"
        );
        tracing::info!(
            order_id = %record.order.id,
            event_name = "order.executing",
            "order.executing"
        );
        tracing::info!(
            order_id = %record.order.id,
            step_id = %record.step.id,
            event_name = "execution_step.started",
            "execution_step.started"
        );
        Ok(ExecutionStartPersisted {
            order_id: record.order.id,
            attempt_id: record.attempt.id,
            step_id: record.step.id,
        })
    }

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

    /// Scar tissue: §2.3 `AfterExecutionStepStatusPersisted`. For the PR3 single-step happy path
    /// this absorbs §2.4 and §2.6 because provider receipt, provider terminal state, and step
    /// completion commit in one Postgres transaction.
    #[activity]
    pub async fn persist_step_completion(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistStepCompletionInput,
    ) -> Result<StepCompletionPersisted, ActivityError> {
        let deps = self.deps()?;
        let step = deps
            .db
            .orders()
            .get_execution_step(input.execution.step_id)
            .await
            .map_err(activity_error_from_display)?;
        let (operation, addresses) = provider_state_records(
            &step,
            &input.execution.provider_state,
            &input.execution.response,
        )
        .map_err(activity_error_from_display)?;
        let record = deps
            .db
            .orders()
            .persist_single_step_completion(PersistStepCompletionRecord {
                step_id: input.execution.step_id,
                operation,
                addresses,
                response: input.execution.response,
                tx_hash: input.execution.tx_hash,
                usd_valuation: json!({}),
                completed_at: Utc::now(),
            })
            .await
            .map_err(activity_error_from_display)?;
        if let Some(provider_operation_id) = record.provider_operation_id {
            tracing::info!(
                order_id = %input.execution.order_id,
                provider_operation_id = %provider_operation_id,
                event_name = "provider_operation.persisted",
                "provider_operation.persisted"
            );
        }
        tracing::info!(
            order_id = %input.execution.order_id,
            step_id = %record.step.id,
            event_name = "execution_step.completed",
            "execution_step.completed"
        );
        Ok(StepCompletionPersisted {
            order_id: input.execution.order_id,
            attempt_id: input.execution.attempt_id,
            step_id: record.step.id,
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
            .mark_single_step_order_completed(input.order_id, input.attempt_id, Utc::now())
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
        _ctx: ActivityContext,
        _input: LoadOrderExecutionStateInput,
    ) -> Result<OrderExecutionState, ActivityError> {
        todo!("PR3: load canonical order execution state from Postgres")
    }

    /// Scar tissue: §3 phase 2 and §4 state alignment.
    #[activity]
    pub async fn materialize_execution_attempt(
        _ctx: ActivityContext,
        _input: MaterializeExecutionAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        todo!("PR3: materialize the execution attempt")
    }

    /// Scar tissue: §2 boundary 1, `AfterStepMarkedReadyToFire`.
    #[activity]
    pub async fn persist_step_ready_to_fire(
        _ctx: ActivityContext,
        _input: PersistStepReadyToFireInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 1")
    }

    /// Scar tissue: §2 boundary 2, `AfterStepMarkedFailed`.
    #[activity]
    pub async fn persist_step_failed(
        _ctx: ActivityContext,
        _input: PersistStepFailedInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 2")
    }

    /// Scar tissue: §2 boundary 3, `AfterExecutionStepStatusPersisted`.
    #[activity]
    pub async fn persist_step_terminal_status(
        _ctx: ActivityContext,
        _input: PersistStepTerminalStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 3")
    }

    /// Scar tissue: §2 boundary 4, `AfterProviderReceiptPersisted`.
    #[activity]
    pub async fn persist_provider_receipt(
        _ctx: ActivityContext,
        _input: PersistProviderReceiptInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 4")
    }

    /// Scar tissue: §2 boundary 5, `AfterProviderOperationStatusPersisted`.
    #[activity]
    pub async fn persist_provider_operation_status(
        _ctx: ActivityContext,
        _input: PersistProviderOperationStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 5")
    }

    /// Scar tissue: §2 boundary 6, `AfterProviderStepSettlement`.
    #[activity]
    pub async fn settle_provider_step(
        _ctx: ActivityContext,
        _input: SettleProviderStepInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 6")
    }

    /// Scar tissue: §5 retry/refund decision and §7 stale quote branch.
    #[activity]
    pub async fn classify_step_failure(
        _ctx: ActivityContext,
        _input: ClassifyStepFailureInput,
    ) -> Result<StepFailureDecision, ActivityError> {
        todo!("PR4: classify failed step recovery path")
    }

    /// Scar tissue: §15 failure snapshot.
    #[activity]
    pub async fn write_failed_attempt_snapshot(
        _ctx: ActivityContext,
        _input: WriteFailedAttemptSnapshotInput,
    ) -> Result<FailedAttemptSnapshotWritten, ActivityError> {
        todo!("PR4: write failed attempt snapshot")
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn finalize_order_or_refund(
        _ctx: ActivityContext,
        _input: FinalizeOrderOrRefundInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        todo!("PR5: finalize order/refund terminal state")
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
                "{} is not executable in PR3 OrderWorkflow",
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

pub struct QuoteRefreshActivities;

#[activities]
impl QuoteRefreshActivities {
    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn compose_refreshed_quote_attempt(
        _ctx: ActivityContext,
        _input: ComposeRefreshedQuoteAttemptInput,
    ) -> Result<RefreshedQuoteAttemptShape, ActivityError> {
        // TODO(PR4, brief §6 invariant 5; scar §7): stale quote refresh is its own attempt.
        todo!("PR4: compose refreshed quote attempt")
    }

    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn materialize_refreshed_attempt(
        _ctx: ActivityContext,
        _input: MaterializeRefreshedAttemptInput,
    ) -> Result<RefreshedAttemptMaterialized, ActivityError> {
        // TODO(PR4, brief §6 invariant 5; scar §7): persist refreshed attempt separately.
        todo!("PR4: materialize refreshed quote attempt")
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
        todo!("PR3: verify provider operation hint against provider API")
    }

    /// Scar tissue: §10 provider hint recovery. This is the polling half of the user-approved
    /// signal-first plus fallback shape for brief §8.4.
    #[activity]
    pub async fn poll_provider_operation_hints(
        _ctx: ActivityContext,
        _input: PollProviderOperationHintsInput,
    ) -> Result<ProviderOperationHintsPolled, ActivityError> {
        todo!("PR3: poll provider hints as a fallback to signals")
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
        todo!("PR3: dispatch provider action for the active step")
    }
}
