use futures_util::future::{BoxFuture, FutureExt};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StaleQuoteRefreshBudgetDecision {
    Refresh,
    RefundRequired,
}

pub(super) fn stale_quote_refresh_budget_decision(
    refreshed_attempt_count: usize,
    max_refresh_attempts: usize,
) -> StaleQuoteRefreshBudgetDecision {
    if refreshed_attempt_count < max_refresh_attempts {
        StaleQuoteRefreshBudgetDecision::Refresh
    } else {
        StaleQuoteRefreshBudgetDecision::RefundRequired
    }
}

pub(super) fn stale_quote_step_failure_decision(
    refreshed_attempt_count: usize,
    max_refresh_attempts: usize,
) -> StepFailureDecision {
    match stale_quote_refresh_budget_decision(refreshed_attempt_count, max_refresh_attempts) {
        StaleQuoteRefreshBudgetDecision::Refresh => StepFailureDecision::RefreshQuote,
        StaleQuoteRefreshBudgetDecision::RefundRequired => StepFailureDecision::StartRefund,
    }
}

#[activities]
impl OrderActivities {
    // ─── per-step venue-specific dispatch activities ───────────────────────
    //
    // Each `dispatch_<step_type>_step` activity collapses what used to be six
    // separate Temporal round-trips (persist ready-to-fire → check stale quote
    // → execute → persist receipt → persist op status → settle) into a single
    // body. The thin wrappers exist so Temporal UI shows the venue-specific
    // activity name (e.g. `OrderActivities::dispatch_cctp_burn_step`); the
    // shared body lives in [`run_step_dispatch`]. The `Refund` step type goes
    // through `dispatch_refund_step` because the refund workflow walks steps
    // exactly the same way the order workflow does. `WaitForDeposit` has no
    // dispatch_* — it is not provider-executable.

    #[activity]
    pub async fn dispatch_across_bridge_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_across_bridge_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::AcrossBridge).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_cctp_burn_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_cctp_burn_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::CctpBurn).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_cctp_receive_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_cctp_receive_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::CctpReceive).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_hyperliquid_bridge_deposit_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_hyperliquid_bridge_deposit_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(
                &deps,
                input,
                OrderExecutionStepType::HyperliquidBridgeDeposit,
            )
            .await
        })
        .await
    }
    #[activity]
    pub async fn dispatch_hypercore_bridge_deposit_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_hypercore_bridge_deposit_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::HypercoreBridgeDeposit).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_hyperliquid_bridge_withdrawal_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_hyperliquid_bridge_withdrawal_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(
                &deps,
                input,
                OrderExecutionStepType::HyperliquidBridgeWithdrawal,
            )
            .await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_hyperliquid_clearinghouse_to_spot_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity(
            "dispatch_hyperliquid_clearinghouse_to_spot_step",
            async move {
                let deps = self.deps()?;
                run_step_dispatch(
                    &deps,
                    input,
                    OrderExecutionStepType::HyperliquidClearinghouseToSpot,
                )
                .await
            },
        )
        .await
    }

    #[activity]
    pub async fn dispatch_hyperliquid_limit_order_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_hyperliquid_limit_order_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::HyperliquidLimitOrder).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_hyperliquid_trade_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_hyperliquid_trade_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::HyperliquidTrade).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_unit_deposit_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_unit_deposit_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::UnitDeposit).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_unit_withdrawal_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_unit_withdrawal_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::UnitWithdrawal).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_universal_router_swap_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_universal_router_swap_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::UniversalRouterSwap).await
        })
        .await
    }

    #[activity]
    pub async fn dispatch_refund_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: DispatchStepInput,
    ) -> Result<StepDispatched, ActivityError> {
        record_activity("dispatch_refund_step", async move {
            let deps = self.deps()?;
            run_step_dispatch(&deps, input, OrderExecutionStepType::Refund).await
        })
        .await
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn mark_order_completed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MarkOrderCompletedInput,
    ) -> Result<OrderCompleted, ActivityError> {
        record_activity("mark_order_completed", async move {
            let deps = self.deps()?;
            let completed = deps
                .db
                .orders()
                .mark_execution_order_completed(
                    input.order_id.inner(),
                    input.attempt_id.inner(),
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %completed.order.id,
                attempt_id = %completed.attempt.id,
                event_name = "order.completed",
                "order.completed"
            );
            record_order_executor_wait_total_metric(
                &deps,
                completed.order.id,
                "completed",
                input.funded_to_workflow_start_seconds,
            )
            .await;
            Ok(OrderCompleted {
                order_id: completed.order.id.into(),
                attempt_id: completed.attempt.id.into(),
            })
        })
        .await
    }

    /// Scar tissue: §3 phase pass and §4 order/vault/step state alignment.
    #[activity]
    pub async fn load_order_execution_state(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadOrderExecutionStateInput,
    ) -> Result<OrderExecutionState, ActivityError> {
        record_activity("load_order_execution_state", async move {
            let deps = self.deps()?;
            let first_activity_at = Utc::now();
            let order = deps
                .db
                .orders()
                .get(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let funded_to_workflow_start_seconds =
                if order.status == router_core::models::RouterOrderStatus::Funded {
                    nonnegative_duration_seconds(order.updated_at, first_activity_at)
                } else {
                    None
                };
            if let Some(seconds) = funded_to_workflow_start_seconds {
                telemetry::record_funded_to_workflow_start(Duration::from_secs_f64(seconds));
            }
            let funding_vault_id = order.funding_vault_id.ok_or_else(|| {
                OrderActivityError::invariant(
                    "order_has_funding_vault",
                    format!("order {} cannot execute without a funding vault", order.id),
                )
            })?;
            let source_vault = deps
                .db
                .vaults()
                .get(funding_vault_id)
                .await
                .map_err(OrderActivityError::db_query)?;
            let order = if order.status == router_core::models::RouterOrderStatus::PendingFunding {
                let funded = deps
                    .db
                    .orders()
                    .mark_order_funded_from_funded_vault(order.id, Utc::now())
                    .await
                    .map_err(OrderActivityError::db_query)?;
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
                .map_err(OrderActivityError::db_query)?;
            let planned_at = Utc::now();
            let route = match quote {
                RouterOrderQuote::MarketOrder(quote) => deps
                    .planner
                    .plan(&order, &source_vault, &quote, planned_at)
                    .map_err(|source| provider_quote_error("market_order_route_planner", source))?,
                RouterOrderQuote::LimitOrder(quote) => deps
                    .planner
                    .plan_limit_order(&order, &source_vault, &quote, planned_at)
                    .map_err(|source| provider_quote_error("market_order_route_planner", source))?,
            };
            if route.legs.is_empty() || route.steps.is_empty() {
                return Err(OrderActivityError::invariant(
                    "execution_plan_non_empty",
                    format!(
                        "order {} execution plan has {} legs and {} steps",
                        order.id,
                        route.legs.len(),
                        route.steps.len()
                    ),
                ));
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
                order_id: order.id.into(),
                phase: OrderWorkflowPhase::WaitingForFunding,
                active_attempt_id: None,
                active_step_id: None,
                funded_to_workflow_start_seconds,
                order,
                plan: ExecutionPlan {
                    path_id: route.path_id,
                    transition_decl_ids: route.transition_decl_ids,
                    legs: route.legs,
                    steps: route.steps,
                },
            })
        })
        .await
    }

    /// Scar tissue: §2.1 `AfterExecutionLegsPersisted`, §3 phase 2, and §4 state alignment.
    #[activity]
    pub async fn materialize_execution_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeExecutionAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        record_activity("materialize_execution_attempt", async move {
            let deps = self.deps()?;
            let mut plan = input.plan;
            plan.steps =
                hydrate_destination_execution_steps(&deps, input.order_id.inner(), plan.steps)
                    .await?;
            apply_execution_leg_usd_valuations(&deps, &mut plan.legs).await;
            let record = deps
                .db
                .orders()
                .materialize_primary_execution_attempt(
                    input.order_id.inner(),
                    ExecutionAttemptPlan {
                        legs: plan.legs,
                        steps: plan.steps,
                    },
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
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
                attempt_id: record.attempt.id.into(),
                steps: record
                    .steps
                    .into_iter()
                    .map(|step| WorkflowExecutionStep {
                        step_id: step.id.into(),
                        step_index: step.step_index,
                        step_type: step.step_type,
                    })
                    .collect(),
            })
        })
        .await
    }

    /// Scar tissue: §5 retry attempt creation and suffix supersession.
    #[activity]
    pub async fn materialize_retry_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRetryAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        record_activity("materialize_retry_attempt", async move {
            let deps = self.deps()?;
            let record = deps
                .db
                .orders()
                .create_retry_execution_attempt_from_failed_step(
                    input.order_id.inner(),
                    input.failed_attempt_id.inner(),
                    input.failed_step_id.inner(),
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %record.order.id,
                attempt_id = %record.attempt.id,
                failed_attempt_id = %input.failed_attempt_id.inner(),
                failed_step_id = %input.failed_step_id.inner(),
                event_name = "order.execution_retry_materialized",
                "order.execution_retry_materialized"
            );
            Ok(MaterializedExecutionAttempt {
                attempt_id: record.attempt.id.into(),
                steps: record
                    .steps
                    .into_iter()
                    .map(|step| WorkflowExecutionStep {
                        step_id: step.id.into(),
                        step_index: step.step_index,
                        step_type: step.step_type,
                    })
                    .collect(),
            })
        })
        .await
    }

    /// Scar tissue: §2 boundary 2, `AfterStepMarkedFailed`.
    #[activity]
    pub async fn persist_step_failed(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistStepFailedInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        record_activity("persist_step_failed", async move {
            let deps = self.deps()?;
            let step = deps
                .db
                .orders()
                .persist_execution_step_failed(
                    input.order_id.inner(),
                    input.attempt_id.inner(),
                    input.step_id.inner(),
                    json!({ "error": input.failure_reason }),
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %input.order_id.inner(),
                attempt_id = %input.attempt_id.inner(),
                step_id = %step.id,
                event_name = "execution_step.failed",
                "execution_step.failed"
            );
            record_step_terminal_latency_metrics(&deps, step.id).await;
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterStepMarkedFailed,
            })
        })
        .await
    }

    /// Scar tissue: §5 retry/refund decision and §7 stale quote branch.
    #[activity]
    pub async fn classify_step_failure(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ClassifyStepFailureInput,
    ) -> Result<StepFailureDecision, ActivityError> {
        record_activity("classify_step_failure", async move {
        let deps = self.deps()?;
        let attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.attempt_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        if attempt.order_id != input.order_id.inner() {
            return Err(OrderActivityError::invariant(
                "attempt_belongs_to_order",
                format!(
                    "attempt {} does not belong to order {}",
                    attempt.id,
                    input.order_id.inner()
                ),
            ));
        }
        let order_quote = deps
            .db
            .orders()
            .get_router_order_quote(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let failed_step = deps
            .db
            .orders()
            .get_execution_step(input.failed_step_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        if failed_step.order_id != input.order_id.inner()
            || failed_step.execution_attempt_id != Some(input.attempt_id.inner())
        {
            return Err(OrderActivityError::invariant(
                "step_belongs_to_order_attempt",
                format!(
                    "step {} does not belong to order {} attempt {}",
                    failed_step.id,
                    input.order_id.inner(),
                    input.attempt_id.inner()
                ),
            ));
        }
        let refreshed_attempt_count = deps
            .db
            .orders()
            .get_execution_attempts(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?
            .into_iter()
            .filter(|attempt| stale_quote_refresh_attempt(attempt))
            .count();
        let stale_quote_refresh_eligible = matches!(&order_quote, RouterOrderQuote::MarketOrder(_))
            // Scar §7: RefundRecovery attempts never stale-refresh. Funds may be
            // mid-flight during a refund, so stale refund quotes route to
            // refund-required instead of being re-quoted.
            && attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery;
        let stale_quote_expired = if stale_quote_refresh_eligible {
            let legs = deps
                .db
                .orders()
                .get_execution_legs_for_attempt(input.attempt_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            failed_step
                .execution_leg_id
                .and_then(|leg_id| legs.into_iter().find(|leg| leg.id == leg_id))
                .and_then(|leg| leg.provider_quote_expires_at)
                .is_some_and(|expires_at| expires_at < Utc::now())
        } else {
            false
        };
        let refresh_budget_decision = stale_quote_refresh_budget_decision(
            refreshed_attempt_count,
            deps.quote_refresh_max_attempts,
        );
        if stale_quote_expired
            && refresh_budget_decision == StaleQuoteRefreshBudgetDecision::RefundRequired
        {
            telemetry::record_stale_quote_refresh_cap_hit();
        }
        let decision = if stale_quote_expired {
            let decision = stale_quote_step_failure_decision(
                refreshed_attempt_count,
                deps.quote_refresh_max_attempts,
            );
            if decision == StepFailureDecision::RefreshQuote {
                telemetry::record_stale_quote_refresh("post_failure_detected");
            }
            decision
        } else if attempt.attempt_index < MAX_EXECUTION_ATTEMPTS {
            StepFailureDecision::RetryNewAttempt
        } else {
            StepFailureDecision::StartRefund
        };
        tracing::info!(
            order_id = %input.order_id.inner(),
            attempt_id = %input.attempt_id.inner(),
            failed_step_id = %input.failed_step_id.inner(),
            attempt_index = attempt.attempt_index,
            refreshed_attempt_count,
            quote_refresh_max_attempts = deps.quote_refresh_max_attempts,
            decision = ?decision,
            event_name = "execution_step.failure_classified",
            "execution_step.failure_classified"
        );
        Ok(decision)
        })
        .await
    }

    /// Scar tissue: §6 stale running step refund classification.
    #[activity]
    pub async fn classify_stale_running_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ClassifyStaleRunningStepInput,
    ) -> Result<StaleRunningStepClassified, ActivityError> {
        record_activity("classify_stale_running_step", async move {
            let deps = self.deps()?;
            classify_stale_running_step_for_deps(&deps, input).await
        })
        .await
    }

    /// Scar tissue: §15 failure snapshot.
    #[activity]
    pub async fn write_failed_attempt_snapshot(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: WriteFailedAttemptSnapshotInput,
    ) -> Result<FailedAttemptSnapshotWritten, ActivityError> {
        record_activity("write_failed_attempt_snapshot", async move {
            let deps = self.deps()?;
            let order = deps
                .db
                .orders()
                .get(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let failed_step = deps
                .db
                .orders()
                .get_execution_step(input.failed_step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if failed_step.order_id != input.order_id.inner()
                || failed_step.execution_attempt_id != Some(input.attempt_id.inner())
            {
                return Err(invariant_error(
                    "step_belongs_to_order_attempt",
                    format!(
                        "step {} does not belong to order {} attempt {}",
                        failed_step.id,
                        input.order_id.inner(),
                        input.attempt_id.inner()
                    ),
                ));
            }
            let trigger_provider_operation_id = deps
                .db
                .orders()
                .get_provider_operations(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?
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
                    input.attempt_id.inner(),
                    Some(failed_step.id),
                    trigger_provider_operation_id,
                    failure_reason,
                    snapshot.into(),
                    Utc::now(),
                )
                .await
            {
                Ok(attempt) => attempt,
                Err(RouterCoreError::NotFound) => {
                    let attempt = deps
                        .db
                        .orders()
                        .get_execution_attempt(input.attempt_id.inner())
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    if attempt.status != OrderExecutionAttemptStatus::Failed {
                        return Err(invariant_error(
                            "failed_attempt_snapshot_idempotent_state",
                            format!(
                                "attempt {} was not active or failed while writing failure snapshot",
                                input.attempt_id.inner()
                            ),
                        ));
                    }
                    attempt
                }
                Err(source) => return Err(OrderActivityError::db_query(source)),
            };
            Ok(FailedAttemptSnapshotWritten {
                attempt_id: attempt.id.into(),
                attempt_index: attempt.attempt_index,
                failed_step_id: failed_step.id.into(),
            })
        })
        .await
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn finalize_order_or_refund(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: FinalizeOrderOrRefundInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        record_activity("finalize_order_or_refund", async move {
            let deps = self.deps()?;
            match input.terminal_status {
                OrderTerminalStatus::RefundRequired => {
                    let order = deps
                        .db
                        .orders()
                        .mark_order_refund_required(input.order_id.inner(), Utc::now())
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    tracing::info!(
                        order_id = %order.id,
                        event_name = "order.refund_required",
                        "order.refund_required"
                    );
                    record_order_executor_wait_total_metric(
                        &deps,
                        order.id,
                        "refund_required",
                        input.funded_to_workflow_start_seconds,
                    )
                    .await;
                    Ok(FinalizedOrder {
                        order_id: order.id.into(),
                        terminal_status: OrderTerminalStatus::RefundRequired,
                    })
                }
                OrderTerminalStatus::Refunded => {
                    let attempt_id = input.attempt_id.map(|id| id.inner()).ok_or_else(|| {
                        invariant_error(
                            "finalize_refunded_order_requires_refund_attempt",
                            "finalize refunded order requires a refund attempt id",
                        )
                    })?;
                    let completed = deps
                        .db
                        .orders()
                        .mark_order_refunded(input.order_id.inner(), attempt_id, Utc::now())
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    tracing::info!(
                        order_id = %completed.order.id,
                        attempt_id = %completed.attempt.id,
                        event_name = "order.refunded",
                        "order.refunded"
                    );
                    record_order_executor_wait_total_metric(
                        &deps,
                        completed.order.id,
                        "refunded",
                        input.funded_to_workflow_start_seconds,
                    )
                    .await;
                    Ok(FinalizedOrder {
                        order_id: completed.order.id.into(),
                        terminal_status: OrderTerminalStatus::Refunded,
                    })
                }
                terminal_status => Err(invariant_error(
                    "unsupported_order_terminal_status",
                    format!("finalize_order_or_refund does not handle {terminal_status:?}"),
                )),
            }
        })
        .await
    }
}

/// Shared body for every venue-specific `dispatch_<step_type>_step` activity.
///
/// Collapses the historic six-activity boundary chain into a single body:
///
/// 1. Validate the step belongs to `(order_id, attempt_id)` and is `Running`.
/// 2. Persist `step ready to fire` (latency/event boundary).
/// 3. Pre-execution stale-quote check — short-circuits with
///    `DispatchOutcome::StaleQuoteDetected` if the leg's provider quote
///    expired before any side effect fired.
/// 4. Write the side-effect about-to-fire checkpoint (so the stale-running
///    classifier can disambiguate a worker crash mid-side-effect).
/// 5. Run the step through `execute_running_step`, which routes through
///    [`super::idempotent_step::execute_idempotent_step`] for every step type
///    that produces a `ProviderExecutionIntent` — that is the Tier 1 retry
///    safety guarantee. (`HyperliquidClearinghouseToSpot` is intentionally
///    not behind that guard; idempotency for it is upstream via the
///    side-effect checkpoint.)
/// 6. Persist the provider receipt (operation row + addresses).
/// 7. Update the provider operation status to its post-execution status.
/// 8. Settle the step into `waiting_external` (if `Waiting`) or `completed`
///    (if synchronous).
///
/// `expected_step_type` is used as a defence-in-depth check that the workflow
/// dispatched to the correct venue-specific activity for the persisted step's
/// type — a workflow code drift would surface as a clean invariant error here
/// rather than silent execution under the wrong dispatcher.
pub(super) async fn run_step_dispatch(
    deps: &OrderActivityDeps,
    input: DispatchStepInput,
    expected_step_type: OrderExecutionStepType,
) -> Result<StepDispatched, OrderActivityError> {
    // 1. Persist `running` and validate the step.
    let step = deps
        .db
        .orders()
        .persist_execution_step_ready_to_fire(
            input.order_id.inner(),
            input.attempt_id.inner(),
            input.step_id.inner(),
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    if step.step_type != expected_step_type {
        return Err(invariant_error(
            "dispatch_activity_matches_step_type",
            format!(
                "step {} has type {} but was dispatched as {}",
                step.id,
                step.step_type.to_db_string(),
                expected_step_type.to_db_string(),
            ),
        ));
    }
    tracing::info!(
        order_id = %input.order_id.inner(),
        attempt_id = %input.attempt_id.inner(),
        step_id = %step.id,
        step_index = step.step_index,
        event_name = "execution_step.started",
        "execution_step.started"
    );

    // 2. Master switches and venue execution policy. Short-circuit before
    // stale-quote refresh so refund-only mode cannot create fresh normal
    // execution paths.
    if let Some(blocked) = pre_execution_switch_check(deps, &input, &step).await? {
        return Ok(StepDispatched {
            order_id: input.order_id,
            attempt_id: input.attempt_id,
            step_id: input.step_id,
            outcome: DispatchOutcome::StaleQuoteDetected {
                should_refund: true,
                reason: Some(blocked),
            },
        });
    }

    let step = refresh_refund_step_request_if_needed(deps, step).await?;

    // 3. Pre-execution stale-quote check. Short-circuit before side effects.
    if let Some(stale) = pre_execution_stale_quote_check(deps, &input, &step).await? {
        return Ok(StepDispatched {
            order_id: input.order_id,
            attempt_id: input.attempt_id,
            step_id: input.step_id,
            outcome: DispatchOutcome::StaleQuoteDetected {
                should_refund: stale.should_refund,
                reason: stale.reason,
            },
        });
    }

    // 4. Side-effect checkpoint (scar §6).
    let checkpoint = json!({
        "kind": "provider_side_effect_about_to_fire",
        "reason": "about_to_fire_provider_side_effect",
        "step_id": step.id,
        "attempt_id": input.attempt_id.inner(),
        "recorded_at": Utc::now().to_rfc3339(),
        "scar_tissue": "§6"
    });
    let step = deps
        .db
        .orders()
        .record_execution_step_provider_side_effect_checkpoint(
            input.order_id.inner(),
            input.attempt_id.inner(),
            input.step_id.inner(),
            checkpoint,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;

    // 4. Execute through the idempotent guard.
    let completion = execute_running_step(deps, &step).await?;
    let completion = apply_authoritative_output_balance(deps, &step, completion).await?;

    // 5. Persist provider receipt + status + settle. The earlier
    // `persist_provider_receipt` and `persist_provider_operation_status`
    // boundaries each upserted the operation; here we collapse into a single
    // upsert (with the status set to the post-execution status directly) plus
    // the post-receipt status update that downstream readers expect.
    persist_step_provider_records(deps, &step, &completion).await?;

    // 6. Settle into waiting_external or completed.
    let outcome = settle_dispatched_step(deps, &input, &step, completion).await?;

    Ok(StepDispatched {
        order_id: input.order_id,
        attempt_id: input.attempt_id,
        step_id: input.step_id,
        outcome,
    })
}

/// Collapsed equivalent of the legacy `persist_provider_receipt` +
/// `persist_provider_operation_status` boundaries: one upsert at the
/// post-execution status plus the post-receipt status update that historical
/// downstream readers expect (it carries the response payload).
async fn persist_step_provider_records(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    completion: &StepCompletion,
) -> Result<(), OrderActivityError> {
    let (operation, mut addresses) =
        provider_state_records(step, &completion.provider_state, &completion.response)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
    let Some(operation) = operation else {
        return Ok(());
    };
    let provider_operation_id = deps
        .db
        .orders()
        .upsert_provider_operation(&operation)
        .await
        .map_err(OrderActivityError::db_query)?;
    for address in &mut addresses {
        address.provider_operation_id = Some(provider_operation_id);
        deps.db
            .orders()
            .upsert_provider_address(address)
            .await
            .map_err(OrderActivityError::db_query)?;
    }
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
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %step.order_id,
        provider_operation_id = %provider_operation_id,
        event_name = "provider_operation.persisted",
        "provider_operation.persisted"
    );
    Ok(())
}

/// Drive the step into its terminal-for-this-leg-of-the-workflow status.
/// `Waiting` returns `DispatchOutcome::Waiting` (the workflow signal-waits for
/// a hint); `Completed` returns `DispatchOutcome::Completed`.
async fn settle_dispatched_step(
    deps: &OrderActivityDeps,
    input: &DispatchStepInput,
    step: &OrderExecutionStep,
    completion: StepCompletion,
) -> Result<DispatchOutcome, OrderActivityError> {
    match completion.outcome {
        StepExecutionOutcome::Waiting => {
            settle_waiting_step_after_execute(
                deps,
                input.order_id.inner(),
                input.attempt_id.inner(),
                input.step_id.inner(),
                completion.response,
                completion.tx_hash,
            )
            .await?;
            Ok(DispatchOutcome::Waiting)
        }
        StepExecutionOutcome::Completed => {
            let usd_valuation =
                execution_step_usd_valuation_for_response(deps, step, &completion.response).await;
            let record = deps
                .db
                .orders()
                .persist_execution_step_completion(PersistStepCompletionRecord {
                    step_id: input.step_id.inner(),
                    operation: None,
                    addresses: Vec::new(),
                    response: completion.response,
                    tx_hash: completion.tx_hash,
                    usd_valuation,
                    completed_at: Utc::now(),
                })
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %input.order_id.inner(),
                attempt_id = %input.attempt_id.inner(),
                step_id = %record.step.id,
                event_name = "execution_step.completed",
                "execution_step.completed"
            );
            record_step_terminal_latency_metrics(deps, record.step.id).await;
            propagate_refund_completed_step_amount_out(deps, &record.step).await?;
            Ok(DispatchOutcome::Completed)
        }
    }
}

/// `Waiting` outcome settle path. Mirrors the historic `settle_provider_step`
/// behaviour but skips the `StepExecuted`-shaped argument shuffle (we already
/// have the response/tx_hash inline from the dispatch body). Also reused by
/// the `verify_<hint_kind>_hint` activities on Accept — the hint update has
/// already touched the provider operation, so we just need to drive the step
/// itself into `completed`/`failed` (or back to `waiting` for non-terminal
/// hint kinds like `HlBridgeDepositObserved`).
pub(super) async fn settle_waiting_step_after_execute(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
    response: Value,
    tx_hash: Option<String>,
) -> Result<(), OrderActivityError> {
    let operations = deps
        .db
        .orders()
        .get_provider_operations(order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let operation = operations
        .iter()
        .filter(|operation| operation.execution_step_id == Some(step_id))
        .max_by_key(|operation| (operation.updated_at, operation.created_at, operation.id));

    match operation.map(|operation| operation.status) {
        Some(ProviderOperationStatus::Completed) => {
            let operation = operation.expect("checked completed operation above");
            let step = deps
                .db
                .orders()
                .get_execution_step(step_id)
                .await
                .map_err(OrderActivityError::db_query)?;
            let mut response = provider_operation_step_response(operation);
            apply_authoritative_output_observation_to_response(deps, &step, &mut response).await?;
            let usd_valuation =
                execution_step_usd_valuation_for_response(deps, &step, &response).await;
            let completed = deps
                .db
                .orders()
                .complete_observed_execution_step(
                    step_id,
                    response,
                    provider_operation_tx_hash(operation),
                    usd_valuation,
                    Utc::now(),
                )
                .await;
            match completed {
                Ok(step) => {
                    record_step_terminal_latency_metrics(deps, step.id).await;
                    propagate_refund_completed_step_amount_out(deps, &step).await?;
                    tracing::info!(
                        order_id = %order_id,
                        attempt_id = %attempt_id,
                        step_id = %step_id,
                        provider_operation_id = %operation.id,
                        event_name = "execution_step.completed",
                        "execution_step.completed"
                    );
                }
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(step_id)
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    if current.status != OrderExecutionStepStatus::Completed {
                        return Err(invariant_error(
                            "completed_provider_operation_settlement_status",
                            format!(
                                "completed provider operation {} could not settle step {} in status {}",
                                operation.id,
                                step_id,
                                current.status.to_db_string()
                            ),
                        ));
                    }
                }
                Err(source) => return Err(OrderActivityError::db_query(source)),
            }
        }
        Some(ProviderOperationStatus::Failed | ProviderOperationStatus::Expired) => {
            let operation = operation.expect("checked failed operation above");
            let step = deps
                .db
                .orders()
                .fail_observed_execution_step(
                    step_id,
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
                Ok(step) => {
                    record_step_terminal_latency_metrics(deps, step.id).await;
                }
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(step_id)
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    if current.status != OrderExecutionStepStatus::Failed {
                        return Err(invariant_error(
                            "failed_provider_step_status",
                            format!(
                                "failed provider step {} is in unexpected status {}",
                                step_id,
                                current.status.to_db_string()
                            ),
                        ));
                    }
                }
                Err(source) => return Err(OrderActivityError::db_query(source)),
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
                .wait_execution_step(step_id, response, tx_hash, Utc::now())
                .await;
            match waiting {
                Ok(step) => {
                    tracing::info!(
                        order_id = %order_id,
                        attempt_id = %attempt_id,
                        step_id = %step_id,
                        event_name = "execution_step.waiting_external",
                        "execution_step.waiting_external"
                    );
                    record_step_waiting_external_latency_metrics(deps, step.id).await;
                }
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(step_id)
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    if !matches!(
                        current.status,
                        OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Completed
                    ) {
                        return Err(invariant_error(
                            "waiting_provider_step_status",
                            format!(
                                "waiting provider step {} is in unexpected status {}",
                                step_id,
                                current.status.to_db_string()
                            ),
                        ));
                    }
                }
                Err(source) => return Err(OrderActivityError::db_query(source)),
            }
        }
    }
    Ok(())
}

async fn propagate_refund_completed_step_amount_out(
    deps: &OrderActivityDeps,
    completed_step: &OrderExecutionStep,
) -> Result<(), OrderActivityError> {
    if !refund_recovery_step(completed_step) {
        return Ok(());
    }
    let Some(attempt_id) = completed_step.execution_attempt_id else {
        return Ok(());
    };
    let amount_out = match completed_step_amount_out(completed_step) {
        Some(amount_out) => amount_out,
        None => return Ok(()),
    };
    let steps = deps
        .db
        .orders()
        .get_execution_steps_for_attempt(attempt_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let Some(next_step) = steps
        .iter()
        .filter(|step| step.step_index > completed_step.step_index)
        .min_by_key(|step| (step.step_index, step.created_at, step.id))
    else {
        return Ok(());
    };
    if !matches!(
        next_step.status,
        OrderExecutionStepStatus::Planned | OrderExecutionStepStatus::Ready
    ) {
        return Ok(());
    }
    if let (Some(output_asset), Some(input_asset)) =
        (&completed_step.output_asset, &next_step.input_asset)
    {
        if output_asset != input_asset {
            return Ok(());
        }
    }

    let request = patch_refund_step_request_amount(next_step, &amount_out);
    let updated = deps
        .db
        .orders()
        .update_execution_step_amount_and_request(
            next_step.id,
            &amount_out,
            None,
            request,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %completed_step.order_id,
        attempt_id = %attempt_id,
        completed_step_id = %completed_step.id,
        next_step_id = %updated.id,
        amount_out = %amount_out,
        event_name = "refund_step.amount_propagated",
        "refund_step.amount_propagated"
    );
    Ok(())
}

pub(super) fn completed_step_amount_out(step: &OrderExecutionStep) -> Option<String> {
    let response = &step.response;
    if let Some(amount_out) = balance_observation_amount_out(response) {
        return Some(amount_out.to_string());
    }
    if let Some(amount_out) = claim_observation_amount_out(response) {
        return Some(amount_out.to_string());
    }
    None
}

fn balance_observation_amount_out(response: &Value) -> Option<&str> {
    for pointer in [
        "/balance_observation/output/spendable_balance",
        "/balance_observation/output/balance",
    ] {
        if let Some(amount) = response.pointer(pointer).and_then(decimal_string) {
            return Some(amount);
        }
    }
    response
        .pointer("/balance_observation/probes")
        .and_then(Value::as_array)
        .and_then(|probes| {
            probes.iter().find_map(|probe| {
                let role = probe.get("role").and_then(Value::as_str)?;
                if role != "destination" {
                    return None;
                }
                probe
                    .get("spendable_balance")
                    .or_else(|| probe.get("balance"))
                    .and_then(decimal_string)
            })
        })
}

fn claim_observation_amount_out(response: &Value) -> Option<&str> {
    response
        .pointer("/claim_observation/amount")
        .and_then(decimal_string)
}

pub(super) fn decimal_string(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|raw| !raw.is_empty() && raw.chars().all(|ch| ch.is_ascii_digit()))
}

async fn refresh_refund_step_request_if_needed(
    deps: &OrderActivityDeps,
    step: OrderExecutionStep,
) -> Result<OrderExecutionStep, OrderActivityError> {
    if !refund_recovery_step(&step) {
        return Ok(step);
    }
    let Some(amount_in) = step.amount_in.clone() else {
        return Ok(step);
    };
    if refund_step_request_amount(&step).as_deref() == Some(amount_in.as_str()) {
        return Ok(step);
    }
    let (request, estimated_amount_out) =
        refreshed_refund_step_request(deps, &step, &amount_in).await?;
    let updated = deps
        .db
        .orders()
        .update_execution_step_amount_and_request(
            step.id,
            &amount_in,
            estimated_amount_out.as_deref(),
            request,
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %updated.order_id,
        step_id = %updated.id,
        amount_in = %amount_in,
        step_type = %updated.step_type.to_db_string(),
        event_name = "refund_step.request_refreshed",
        "refund_step.request_refreshed"
    );
    Ok(updated)
}

async fn refreshed_refund_step_request(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    amount_in: &str,
) -> Result<(Value, Option<String>), OrderActivityError> {
    match step.step_type {
        OrderExecutionStepType::HyperliquidTrade | OrderExecutionStepType::UniversalRouterSwap => {
            refresh_refund_exchange_step_request(deps, step, amount_in).await
        }
        _ => Ok((patch_refund_step_request_amount(step, amount_in), None)),
    }
}

async fn refresh_refund_exchange_step_request(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    amount_in: &str,
) -> Result<(Value, Option<String>), OrderActivityError> {
    let input_asset = step.input_asset.clone().ok_or_else(|| {
        invariant_error(
            "refund_exchange_step_input_asset",
            format!("refund exchange step {} has no input asset", step.id),
        )
    })?;
    let output_asset = step.output_asset.clone().ok_or_else(|| {
        invariant_error(
            "refund_exchange_step_output_asset",
            format!("refund exchange step {} has no output asset", step.id),
        )
    })?;
    let exchange = deps
        .action_providers
        .exchange(&step.provider)
        .ok_or_else(|| provider_not_configured(step))?;
    let quote = exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: input_asset.clone(),
            output_asset: output_asset.clone(),
            input_decimals: refund_exchange_step_decimals(
                deps,
                step,
                "input_decimals",
                &input_asset,
            ),
            output_decimals: refund_exchange_step_decimals(
                deps,
                step,
                "output_decimals",
                &output_asset,
            ),
            order_kind: ProviderOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: step
                .request
                .get("source_custody_vault_address")
                .and_then(Value::as_str)
                .map(str::to_string),
            recipient_address: step
                .request
                .get("recipient_address")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
        })
        .await
        .map_err(|source| provider_quote_error(&step.provider, source))?
        .ok_or_else(|| {
            provider_quote_error(
                &step.provider,
                format!("refund step {} has no refreshed quote", step.id),
            )
        })?;

    let mut request = patch_refund_step_request_amount(step, amount_in);
    set_request_string_field(&mut request, "amount_out", quote.amount_out.as_str());
    set_request_optional_string_field(
        &mut request,
        "min_amount_out",
        quote.min_amount_out.as_deref(),
    );
    set_request_optional_string_field(
        &mut request,
        "max_amount_in",
        quote.max_amount_in.as_deref(),
    );
    if step.step_type == OrderExecutionStepType::UniversalRouterSwap {
        set_request_value_field(&mut request, "price_route", quote.provider_quote);
    }
    set_request_value_field(&mut request, "quote_id", json!(Uuid::now_v7()));
    Ok((request, Some(quote.amount_out)))
}

fn refund_exchange_step_decimals(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    field: &'static str,
    asset: &DepositAsset,
) -> Option<u8> {
    if step.step_type == OrderExecutionStepType::HyperliquidTrade {
        return None;
    }
    step.request
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| u8::try_from(value).ok())
        .or_else(|| refund_asset_decimals(deps, asset))
}

fn refund_step_request_amount(step: &OrderExecutionStep) -> Option<String> {
    let field = refund_step_request_amount_field(step.step_type);
    step.request
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn patch_refund_step_request_amount(step: &OrderExecutionStep, amount: &str) -> Value {
    let mut request = step.request.clone();
    set_request_string_field(
        &mut request,
        refund_step_request_amount_field(step.step_type),
        amount,
    );
    request
}

fn refund_step_request_amount_field(step_type: OrderExecutionStepType) -> &'static str {
    match step_type {
        OrderExecutionStepType::HyperliquidTrade
        | OrderExecutionStepType::HyperliquidLimitOrder
        | OrderExecutionStepType::UniversalRouterSwap => "amount_in",
        _ => "amount",
    }
}

fn set_request_string_field(request: &mut Value, field: &'static str, value: &str) {
    set_request_value_field(request, field, json!(value));
}

fn set_request_optional_string_field(
    request: &mut Value,
    field: &'static str,
    value: Option<&str>,
) {
    set_request_value_field(
        request,
        field,
        value.map_or(Value::Null, |value| json!(value)),
    );
}

fn set_request_value_field(request: &mut Value, field: &'static str, value: Value) {
    if let Some(object) = request.as_object_mut() {
        object.insert(field.to_string(), value);
    }
}

fn refund_recovery_step(step: &OrderExecutionStep) -> bool {
    step.details.get("refund_kind").is_some()
        || step
            .provider_ref
            .as_deref()
            .is_some_and(|provider_ref| provider_ref.starts_with("refund-quote-"))
}

/// Internal return type for [`pre_execution_stale_quote_check`]. `Some` means
/// the leg's quote expired; the caller should short-circuit dispatch with
/// [`DispatchOutcome::StaleQuoteDetected`].
struct PreExecutionStaleQuote {
    should_refund: bool,
    reason: Option<Value>,
}

async fn pre_execution_switch_check(
    deps: &OrderActivityDeps,
    input: &DispatchStepInput,
    step: &OrderExecutionStep,
) -> Result<Option<Value>, OrderActivityError> {
    let attempt = deps
        .db
        .orders()
        .get_execution_attempt(input.attempt_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if attempt.order_id != input.order_id.inner() {
        return Err(invariant_error(
            "attempt_belongs_to_order",
            format!(
                "attempt {} does not belong to order {}",
                attempt.id,
                input.order_id.inner()
            ),
        ));
    }

    let is_refund_attempt = attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery;
    if !is_refund_attempt
        && deps
            .db
            .router_switches()
            .get_or_default(RouterSwitchName::RefundOnlyMode)
            .await
            .map_err(OrderActivityError::db_query)?
            .enabled
    {
        return Ok(Some(json!({
            "reason": "refund_only_mode_enabled",
            "step_id": step.id,
            "attempt_id": attempt.id,
            "provider": step.provider,
        })));
    }

    if step.provider == "internal" {
        return Ok(None);
    }
    let policy = deps
        .db
        .provider_policies()
        .get(&step.provider)
        .await
        .map_err(OrderActivityError::db_query)?
        .unwrap_or_else(|| ProviderPolicy::enabled(&step.provider));
    if policy.execution_state != ProviderExecutionPolicyState::Disabled {
        return Ok(None);
    }

    let reason = json!({
        "reason": "provider_execution_disabled",
        "step_id": step.id,
        "attempt_id": attempt.id,
        "provider": step.provider,
        "policy_reason": policy.reason,
        "policy_updated_by": policy.updated_by,
        "policy_updated_at": policy.updated_at,
    });
    if is_refund_attempt {
        return Err(provider_execute_error(&step.provider, reason.to_string()));
    }
    Ok(Some(reason))
}

/// Pre-execution stale-quote check absorbed from the standalone
/// `check_pre_execution_stale_quote` activity. Returns `None` if the dispatch
/// should proceed; `Some` if the leg's provider quote expired before any side
/// effect fired and the workflow should refresh the quote (or refund, if the
/// per-attempt refresh budget is exhausted).
async fn pre_execution_stale_quote_check(
    deps: &OrderActivityDeps,
    input: &DispatchStepInput,
    step: &OrderExecutionStep,
) -> Result<Option<PreExecutionStaleQuote>, OrderActivityError> {
    let attempt = deps
        .db
        .orders()
        .get_execution_attempt(input.attempt_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if attempt.order_id != input.order_id.inner() {
        return Err(invariant_error(
            "attempt_belongs_to_order",
            format!(
                "attempt {} does not belong to order {}",
                attempt.id,
                input.order_id.inner()
            ),
        ));
    }
    if attempt.status != OrderExecutionAttemptStatus::Active
        || attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery
    {
        return Ok(None);
    }
    let order_quote = match deps
        .db
        .orders()
        .get_router_order_quote(input.order_id.inner())
        .await
    {
        Ok(quote) => quote,
        // No persisted quote → there is nothing for the stale-quote refresh
        // path to refresh; let dispatch proceed. Mirrors how the legacy
        // `check_pre_execution_stale_quote` activity behaved when callers
        // pre-validated the order had a quote — under the consolidated
        // dispatch the call sites can no longer guarantee that, so we treat
        // a missing quote as "not stale" and let `execute_running_step`
        // surface any real misconfiguration.
        Err(RouterCoreError::NotFound) => return Ok(None),
        Err(source) => return Err(OrderActivityError::db_query(source)),
    };
    if !matches!(order_quote, RouterOrderQuote::MarketOrder(_)) {
        return Ok(None);
    }
    let provider_operations = deps
        .db
        .orders()
        .get_provider_operations(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if step.status != OrderExecutionStepStatus::Running
        || step_provider_side_effect_started(step, &provider_operations)
    {
        return Ok(None);
    }
    let attempt_steps = deps
        .db
        .orders()
        .get_execution_steps_for_attempt(input.attempt_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if leg_already_crossed_provider_boundary(step, &attempt_steps, &provider_operations) {
        return Ok(None);
    }
    let Some(leg_id) = step.execution_leg_id else {
        return Ok(None);
    };
    let legs = deps
        .db
        .orders()
        .get_execution_legs_for_attempt(input.attempt_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    let Some(leg) = legs.into_iter().find(|leg| leg.id == leg_id) else {
        return Ok(None);
    };
    let Some(provider_quote_expires_at) = leg.provider_quote_expires_at else {
        return Ok(None);
    };
    if provider_quote_expires_at >= Utc::now() {
        return Ok(None);
    }
    let refreshed_attempt_count = deps
        .db
        .orders()
        .get_execution_attempts(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?
        .into_iter()
        .filter(|attempt| stale_quote_refresh_attempt(attempt))
        .count();
    let reason = json!({
        "reason": "pre_execution_stale_provider_quote",
        "step_id": step.id,
        "execution_leg_id": leg.id,
        "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
        "refreshed_attempt_count": refreshed_attempt_count,
        "quote_refresh_max_attempts": deps.quote_refresh_max_attempts,
    });
    tracing::info!(
        order_id = %input.order_id.inner(),
        attempt_id = %input.attempt_id.inner(),
        step_id = %step.id,
        execution_leg_id = %leg.id,
        provider_quote_expires_at = %provider_quote_expires_at,
        event_name = "execution_step.pre_execution_stale_quote_detected",
        "execution_step.pre_execution_stale_quote_detected"
    );
    Ok(Some(
        match stale_quote_refresh_budget_decision(
            refreshed_attempt_count,
            deps.quote_refresh_max_attempts,
        ) {
            StaleQuoteRefreshBudgetDecision::Refresh => {
                telemetry::record_stale_quote_refresh("pre_execution_detected");
                PreExecutionStaleQuote {
                    should_refund: false,
                    reason: Some(reason),
                }
            }
            StaleQuoteRefreshBudgetDecision::RefundRequired => {
                telemetry::record_stale_quote_refresh_cap_hit();
                PreExecutionStaleQuote {
                    should_refund: true,
                    reason: Some(reason),
                }
            }
        },
    ))
}

fn stale_quote_refresh_attempt(attempt: &OrderExecutionAttempt) -> bool {
    attempt.attempt_kind == OrderExecutionAttemptKind::RefreshedExecution
        && attempt.failure_reason.get("reason").and_then(Value::as_str)
            == Some("stale_provider_quote_refresh")
}

pub(super) async fn classify_stale_running_step_for_deps(
    deps: &OrderActivityDeps,
    input: ClassifyStaleRunningStepInput,
) -> Result<StaleRunningStepClassified, OrderActivityError> {
    let step = deps
        .db
        .orders()
        .get_execution_step(input.step_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    if step.order_id != input.order_id.inner()
        || step.execution_attempt_id != Some(input.attempt_id.inner())
    {
        return Err(invariant_error(
            "step_belongs_to_order_attempt",
            format!(
                "step {} does not belong to order {} attempt {}",
                step.id,
                input.order_id.inner(),
                input.attempt_id.inner()
            ),
        ));
    }

    let step_operations: Vec<_> = deps
        .db
        .orders()
        .get_provider_operations(input.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?
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
        object.insert("attempt_id".to_string(), json!(input.attempt_id.inner()));
    }

    let recorded = deps
        .db
        .orders()
        .record_stale_running_step_classification(
            input.order_id.inner(),
            input.attempt_id.inner(),
            input.step_id.inner(),
            reason.clone(),
            Utc::now(),
        )
        .await
        .map_err(OrderActivityError::db_query)?;
    tracing::info!(
        order_id = %input.order_id.inner(),
        attempt_id = %input.attempt_id.inner(),
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

pub(super) fn step_provider_side_effect_started(
    step: &OrderExecutionStep,
    provider_operations: &[OrderProviderOperation],
) -> bool {
    step.details
        .get("provider_side_effect_checkpoint")
        .is_some()
        || step.tx_hash.is_some()
        || step.provider_ref.is_some()
        || provider_operations
            .iter()
            .any(|operation| operation.execution_step_id == Some(step.id))
}

pub(super) fn leg_already_crossed_provider_boundary(
    current_step: &OrderExecutionStep,
    attempt_steps: &[OrderExecutionStep],
    provider_operations: &[OrderProviderOperation],
) -> bool {
    let Some(leg_id) = current_step.execution_leg_id else {
        return false;
    };

    attempt_steps.iter().any(|step| {
        step.id != current_step.id
            && step.execution_leg_id == Some(leg_id)
            && (matches!(
                step.status,
                OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Completed
            ) || step_provider_side_effect_started(step, provider_operations))
    })
}

pub(super) fn provider_operation_step_response(operation: &OrderProviderOperation) -> Value {
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

pub(super) fn provider_operation_tx_hash(operation: &OrderProviderOperation) -> Option<String> {
    operation
        .observed_state
        .get("tx_hash")
        .and_then(Value::as_str)
        .map(str::to_owned)
}

pub(super) fn is_empty_json_object(value: &Value) -> bool {
    value.as_object().is_some_and(serde_json::Map::is_empty)
}

pub(super) struct StepCompletion {
    pub(super) response: Value,
    pub(super) tx_hash: Option<String>,
    pub(super) provider_state: ProviderExecutionState,
    pub(super) outcome: StepExecutionOutcome,
}

pub(super) enum StepDispatchResult {
    ProviderIntent(ProviderExecutionIntent),
    Complete(StepCompletion),
}

pub(super) type StepDispatchFuture<'a> =
    BoxFuture<'a, Result<StepDispatchResult, OrderActivityError>>;
type StepDispatchHandler =
    for<'a> fn(&'a OrderActivityDeps, &'a OrderExecutionStep) -> StepDispatchFuture<'a>;

struct StepDispatch {
    execute: StepDispatchHandler,
}

impl StepDispatch {
    fn execute<'a>(
        &self,
        deps: &'a OrderActivityDeps,
        step: &'a OrderExecutionStep,
    ) -> StepDispatchFuture<'a> {
        (self.execute)(deps, step)
    }
}

const WAIT_FOR_DEPOSIT_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_wait_for_deposit_step,
};
const REFUND_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_refund_step,
};
const ACROSS_BRIDGE_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_across_bridge_step,
};
const CCTP_BURN_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_cctp_burn_step,
};
const CCTP_RECEIVE_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_cctp_receive_step,
};
const HYPERLIQUID_BRIDGE_DEPOSIT_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hyperliquid_bridge_deposit_step,
};
const HYPERCORE_BRIDGE_DEPOSIT_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hypercore_bridge_deposit_step,
};

const HYPERLIQUID_BRIDGE_WITHDRAWAL_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hyperliquid_bridge_withdrawal_step,
};
const UNIT_DEPOSIT_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_unit_deposit_step,
};
const UNIT_WITHDRAWAL_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_unit_withdrawal_step,
};
const HYPERLIQUID_TRADE_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hyperliquid_trade_step,
};
const HYPERLIQUID_LIMIT_ORDER_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hyperliquid_limit_order_step,
};
const HYPERLIQUID_CLEARINGHOUSE_TO_SPOT_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_hyperliquid_clearinghouse_to_spot_step,
};
const UNIVERSAL_ROUTER_SWAP_DISPATCH: StepDispatch = StepDispatch {
    execute: execute_universal_router_swap_step,
};

fn step_dispatch(step_type: OrderExecutionStepType) -> &'static StepDispatch {
    match step_type {
        OrderExecutionStepType::WaitForDeposit => &WAIT_FOR_DEPOSIT_DISPATCH,
        OrderExecutionStepType::Refund => &REFUND_DISPATCH,
        OrderExecutionStepType::AcrossBridge => &ACROSS_BRIDGE_DISPATCH,
        OrderExecutionStepType::CctpBurn => &CCTP_BURN_DISPATCH,
        OrderExecutionStepType::CctpReceive => &CCTP_RECEIVE_DISPATCH,
        OrderExecutionStepType::HyperliquidBridgeDeposit => &HYPERLIQUID_BRIDGE_DEPOSIT_DISPATCH,
        OrderExecutionStepType::HypercoreBridgeDeposit => &HYPERCORE_BRIDGE_DEPOSIT_DISPATCH,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal => {
            &HYPERLIQUID_BRIDGE_WITHDRAWAL_DISPATCH
        }
        OrderExecutionStepType::UnitDeposit => &UNIT_DEPOSIT_DISPATCH,
        OrderExecutionStepType::UnitWithdrawal => &UNIT_WITHDRAWAL_DISPATCH,
        OrderExecutionStepType::HyperliquidTrade => &HYPERLIQUID_TRADE_DISPATCH,
        OrderExecutionStepType::HyperliquidLimitOrder => &HYPERLIQUID_LIMIT_ORDER_DISPATCH,
        OrderExecutionStepType::HyperliquidClearinghouseToSpot => {
            &HYPERLIQUID_CLEARINGHOUSE_TO_SPOT_DISPATCH
        }
        OrderExecutionStepType::UniversalRouterSwap => &UNIVERSAL_ROUTER_SWAP_DISPATCH,
    }
}

pub(super) enum PostExecuteProvider {
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

pub(super) async fn execute_running_step(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<StepCompletion, OrderActivityError> {
    match step_dispatch(step.step_type).execute(deps, step).await? {
        StepDispatchResult::ProviderIntent(intent) => {
            // `execute_idempotent_step` has already persisted the operation row
            // (status `Planned`) before this point — the side effect fires
            // inside `prepare_provider_completion`. Capture the persisted row's
            // `(operation_type, idempotency_key)` *before* `intent` is consumed
            // so that, if the fire fails cleanly, we can mark the row `Failed`.
            // A retry then surfaces that failure to the workflow instead of
            // re-reading a `Planned` row and silently waiting forever.
            let persisted_operation =
                intent_state(&intent)
                    .operation
                    .as_ref()
                    .and_then(|operation| {
                        operation
                            .idempotency_key
                            .clone()
                            .map(|idempotency_key| (operation.operation_type, idempotency_key))
                    });
            match prepare_provider_completion(deps, step, intent).await {
                Ok(completion) => Ok(completion),
                Err(error) => {
                    if let Some((operation_type, idempotency_key)) = persisted_operation {
                        fail_planned_provider_operation(
                            deps,
                            step,
                            operation_type,
                            &idempotency_key,
                        )
                        .await;
                    }
                    Err(error)
                }
            }
        }
        StepDispatchResult::Complete(completion) => Ok(completion),
    }
}

/// Best-effort: after a provider side effect failed to fire, mark the
/// pre-side-effect operation row `Planned → Failed` so a retry surfaces the
/// failure rather than re-reading the `Planned` row as a `Waiting` outcome.
///
/// Strictly best-effort — any error looking up or updating the row is logged
/// and swallowed, because the caller still propagates the original side-effect
/// error. The update is gated on the row still being `Planned`: `Submitted` /
/// `WaitingExternal` rows mean the side effect did make durable progress and
/// must be left for the stale-running-step mechanism to adjudicate.
async fn fail_planned_provider_operation(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    operation_type: ProviderOperationType,
    idempotency_key: &str,
) {
    let lookup = deps
        .db
        .orders()
        .get_provider_operation_by_idempotency_key(&step.provider, operation_type, idempotency_key)
        .await;
    let operation = match lookup {
        Ok(Some(operation)) => operation,
        Ok(None) => return,
        Err(source) => {
            tracing::warn!(
                order_id = %step.order_id,
                step_id = %step.id,
                provider = %step.provider,
                operation_type = %operation_type.to_db_string(),
                idempotency_key,
                error = %source,
                "failed to look up provider operation while marking a failed side effect"
            );
            return;
        }
    };
    if operation.status != ProviderOperationStatus::Planned {
        return;
    }
    if let Err(source) = deps
        .db
        .orders()
        .update_provider_operation_status(
            operation.id,
            ProviderOperationStatus::Failed,
            operation.provider_ref.clone(),
            operation.observed_state.clone(),
            Some(operation.response.clone()),
            Utc::now(),
        )
        .await
    {
        tracing::warn!(
            order_id = %step.order_id,
            step_id = %step.id,
            provider_operation_id = %operation.id,
            error = %source,
            "failed to mark provider operation Failed after a failed side effect"
        );
        return;
    }
    tracing::warn!(
        order_id = %step.order_id,
        step_id = %step.id,
        provider_operation_id = %operation.id,
        event_name = "provider_operation.marked_failed_after_side_effect_error",
        "provider_operation.marked_failed_after_side_effect_error"
    );
}

fn execute_wait_for_deposit_step<'a>(
    _deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move {
        Err(invariant_error(
            "wait_for_deposit_not_provider_executable",
            format!(
                "{} is not provider executable",
                step.step_type.to_db_string()
            ),
        ))
    }
    .boxed()
}

fn execute_refund_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move {
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
            .map_err(|source| custody_action_error("execute refund transfer", source))?;
        Ok(StepDispatchResult::Complete(StepCompletion {
            response: json!({
                "kind": "refund_transfer",
                "recipient_address": recipient_address,
                "amount": amount,
                "tx_hash": &receipt.tx_hash,
            }),
            tx_hash: Some(receipt.tx_hash),
            provider_state: ProviderExecutionState::default(),
            outcome: StepExecutionOutcome::Completed,
        }))
    }
    .boxed()
}

fn execute_across_bridge_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_bridge_step(
        deps,
        step,
        ProviderOperationType::AcrossBridge,
        BridgeExecutionRequest::across_from_value,
    )
}

fn execute_cctp_burn_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_bridge_step(
        deps,
        step,
        ProviderOperationType::CctpBridge,
        BridgeExecutionRequest::cctp_burn_from_value,
    )
}

/// CCTP receive is unique among bridge step types because it has to fetch the
/// burn attestation (`hydrate_cctp_receive_request`) before it can build the
/// provider intent. We deliberately funnel through the idempotency guard via
/// a custom executor whose `build_intent` runs the hydration lazily — that way
/// the attestation fetch is skipped on retries that already have a persisted
/// `CctpReceive` operation row.
fn execute_cctp_receive_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move { execute_idempotent_step(deps, step, CctpReceiveExecutor).await }.boxed()
}

fn execute_hyperliquid_bridge_deposit_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_bridge_step(
        deps,
        step,
        ProviderOperationType::HyperliquidBridgeDeposit,
        BridgeExecutionRequest::hyperliquid_bridge_deposit_from_value,
    )
}
fn execute_hypercore_bridge_deposit_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_bridge_step(
        deps,
        step,
        ProviderOperationType::HypercoreBridgeDeposit,
        BridgeExecutionRequest::hypercore_bridge_deposit_from_value,
    )
}

fn execute_hyperliquid_bridge_withdrawal_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_bridge_step(
        deps,
        step,
        ProviderOperationType::HyperliquidBridgeWithdrawal,
        BridgeExecutionRequest::hyperliquid_bridge_withdrawal_from_value,
    )
}

/// Special-cased: clearinghouse-to-spot does not flow through the
/// `IdempotentStepExecutor` abstraction because it has no provider trait family
/// (no Bridge / Exchange / Unit handles `usdClassTransfer`) and therefore no
/// `ProviderOperationType` to key an `order_provider_operations` row on. The
/// step returns a `Complete(StepCompletion)` directly rather than producing a
/// `ProviderExecutionIntent`, so there's nothing for the generic guard to
/// short-circuit on. Idempotency at this layer is upstream:
/// `record_execution_step_provider_side_effect_checkpoint` writes a checkpoint
/// before the activity fires, and the workflow's stale-running-step classifier
/// uses that checkpoint to avoid re-firing on retry.
fn execute_hyperliquid_clearinghouse_to_spot_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move {
        let custody_vault_id = request_uuid_field(step, "hyperliquid_custody_vault_id")?;
        let amount = request_string_field(step, "amount")?;
        let receipt = deps
            .custody_action_executor
            .execute_hyperliquid_usd_class_transfer(custody_vault_id, &amount, false)
            .await
            .map_err(|source| {
                custody_action_error("execute hyperliquid clearinghouse to spot transfer", source)
            })?;
        Ok(StepDispatchResult::Complete(StepCompletion {
            response: json!({
                "kind": "hyperliquid_clearinghouse_to_spot",
                "amount": amount,
                "tx_hash": &receipt.tx_hash,
                "hyperliquid_custody_vault_id": custody_vault_id,
                "provider_response": receipt.response,
            }),
            tx_hash: Some(receipt.tx_hash),
            provider_state: ProviderExecutionState::default(),
            outcome: StepExecutionOutcome::Completed,
        }))
    }
    .boxed()
}

pub(super) fn execute_unit_deposit_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move { execute_idempotent_step(deps, step, UnitDepositExecutor).await }.boxed()
}

fn execute_unit_withdrawal_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    async move { execute_idempotent_step(deps, step, UnitWithdrawalExecutor).await }.boxed()
}

struct UnitDepositExecutor;

impl IdempotentStepExecutor for UnitDepositExecutor {
    fn operation_type(&self) -> ProviderOperationType {
        ProviderOperationType::UnitDeposit
    }

    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        let mut request = UnitDepositStepRequest::from_value(&step.request)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
        request.idempotency_key = Some(idempotency_key.to_string());
        deps.action_providers
            .unit(&step.provider)
            .ok_or_else(|| provider_not_configured(step))?
            .execute_deposit(&request)
            .await
            .map_err(|source| provider_execute_error(&step.provider, source))
    }
}

struct UnitWithdrawalExecutor;

impl IdempotentStepExecutor for UnitWithdrawalExecutor {
    fn operation_type(&self) -> ProviderOperationType {
        ProviderOperationType::UnitWithdrawal
    }

    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        _idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        // Unit withdrawal threads idempotency on the resulting provider operation
        // row only — the provider request itself doesn't carry the key today.
        let request = UnitWithdrawalStepRequest::from_value(&step.request)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
        deps.action_providers
            .unit(&step.provider)
            .ok_or_else(|| provider_not_configured(step))?
            .execute_withdrawal(&request)
            .await
            .map_err(|source| provider_execute_error(&step.provider, source))
    }
}

struct BridgeExecutor {
    operation_type: ProviderOperationType,
    decode: fn(&Value) -> Result<BridgeExecutionRequest, String>,
}

impl IdempotentStepExecutor for BridgeExecutor {
    fn operation_type(&self) -> ProviderOperationType {
        self.operation_type
    }

    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        _idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        let request = (self.decode)(&step.request)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
        deps.action_providers
            .bridge(&step.provider)
            .ok_or_else(|| provider_not_configured(step))?
            .execute_bridge(&request)
            .await
            .map_err(|source| provider_execute_error(&step.provider, source))
    }
}

struct ExchangeExecutor {
    operation_type: ProviderOperationType,
    decode: fn(&Value) -> Result<ExchangeExecutionRequest, String>,
}

impl IdempotentStepExecutor for ExchangeExecutor {
    fn operation_type(&self) -> ProviderOperationType {
        self.operation_type
    }

    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        _idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        let request = (self.decode)(&step.request)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
        deps.action_providers
            .exchange(&step.provider)
            .ok_or_else(|| provider_not_configured(step))?
            .execute_trade(&request)
            .await
            .map_err(|source| provider_execute_error(&step.provider, source))
    }
}

/// CCTP receive needs the burn attestation from `hydrate_cctp_receive_request`
/// to build a `BridgeExecutionRequest::CctpReceive`. We do the attestation
/// fetch inside `build_intent` so it only happens on the path where we will
/// actually fire the side effect — retries with a persisted operation row
/// short-circuit before we ever touch the attestation source.
struct CctpReceiveExecutor;

impl IdempotentStepExecutor for CctpReceiveExecutor {
    fn operation_type(&self) -> ProviderOperationType {
        ProviderOperationType::CctpReceive
    }

    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        _idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        let request_json = hydrate_cctp_receive_request(deps, step).await?;
        let request = BridgeExecutionRequest::cctp_receive_from_value(&request_json)
            .map_err(|source| provider_execute_error(&step.provider, source))?;
        deps.action_providers
            .bridge(&step.provider)
            .ok_or_else(|| provider_not_configured(step))?
            .execute_bridge(&request)
            .await
            .map_err(|source| provider_execute_error(&step.provider, source))
    }
}

fn execute_idempotent_bridge_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
    operation_type: ProviderOperationType,
    decode: fn(&Value) -> Result<BridgeExecutionRequest, String>,
) -> StepDispatchFuture<'a> {
    async move {
        execute_idempotent_step(
            deps,
            step,
            BridgeExecutor {
                operation_type,
                decode,
            },
        )
        .await
    }
    .boxed()
}

fn execute_idempotent_exchange_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
    operation_type: ProviderOperationType,
    decode: fn(&Value) -> Result<ExchangeExecutionRequest, String>,
) -> StepDispatchFuture<'a> {
    async move {
        execute_idempotent_step(
            deps,
            step,
            ExchangeExecutor {
                operation_type,
                decode,
            },
        )
        .await
    }
    .boxed()
}

/// Stamp the idempotency key onto the intent's `ProviderOperationIntent` so
/// that when `provider_state_records` materialises the row it carries the key
/// — which feeds both the unique index and the lookup that short-circuits the
/// next retry. Used by [`super::idempotent_step::execute_idempotent_step`] for
/// every step type, after the executor's `build_intent` returns.
pub(super) fn set_provider_intent_operation_idempotency_key(
    intent: &mut ProviderExecutionIntent,
    idempotency_key: String,
) {
    let operation = match intent {
        ProviderExecutionIntent::ProviderOnly { state, .. }
        | ProviderExecutionIntent::CustodyAction { state, .. }
        | ProviderExecutionIntent::CustodyActions { state, .. } => state.operation.as_mut(),
    };
    if let Some(operation) = operation {
        operation.idempotency_key = Some(idempotency_key);
    }
}

fn execute_hyperliquid_trade_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_exchange_step(
        deps,
        step,
        ProviderOperationType::HyperliquidTrade,
        ExchangeExecutionRequest::hyperliquid_trade_from_value,
    )
}

fn execute_hyperliquid_limit_order_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_exchange_step(
        deps,
        step,
        ProviderOperationType::HyperliquidLimitOrder,
        ExchangeExecutionRequest::hyperliquid_limit_order_from_value,
    )
}

fn execute_universal_router_swap_step<'a>(
    deps: &'a OrderActivityDeps,
    step: &'a OrderExecutionStep,
) -> StepDispatchFuture<'a> {
    execute_idempotent_exchange_step(
        deps,
        step,
        ProviderOperationType::UniversalRouterSwap,
        ExchangeExecutionRequest::universal_router_swap_from_value,
    )
}

pub(super) async fn prepare_provider_completion(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    intent: ProviderExecutionIntent,
) -> Result<StepCompletion, OrderActivityError> {
    match intent {
        ProviderExecutionIntent::ProviderOnly { response, state } => {
            let outcome = provider_only_outcome(step, state.operation.as_ref())
                .map_err(|source| provider_execute_error(&step.provider, source))?;
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
            mut state,
        } => {
            let action_for_response = action.clone();
            let receipt_result = deps
                .custody_action_executor
                .execute(CustodyActionRequest {
                    custody_vault_id,
                    action,
                })
                .await;
            let receipt = match receipt_result {
                Ok(receipt) => receipt,
                Err(source) => {
                    let source = source.to_string();
                    if let Some(completion) = cctp_receive_already_claimed_completion(
                        deps,
                        step,
                        custody_vault_id,
                        &action_for_response,
                        &provider_context,
                        state,
                        &source,
                    )
                    .await?
                    {
                        return Ok(completion);
                    }
                    return Err(custody_action_error(
                        "execute provider custody action",
                        source,
                    ));
                }
            };
            if let Some(operation) = state.operation.as_mut() {
                operation
                    .provider_ref
                    .get_or_insert_with(|| receipt.tx_hash.clone());
                let submitted_state = json!({
                    "kind": "custody_action_submitted",
                    "tx_hash": receipt.tx_hash.clone(),
                    "custody_vault_id": receipt.custody_vault_id,
                });
                operation.observed_state.get_or_insert(submitted_state);
            }
            let outcome = provider_operation_outcome(step, state.operation.as_ref())
                .map_err(|source| provider_execute_error(&step.provider, source))?;
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
                    provider_execute_error(
                        &step.provider,
                        "custody_actions intent requires a configured provider",
                    )
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
                    .map_err(|source| {
                        custody_action_error("execute provider custody action batch", source)
                    })?;
                receipts.push(receipt);
            }
            let patch = post_execute_provider
                .post_execute(&provider_context, &receipts)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            apply_post_execute_patch(&mut state, patch, &step.provider)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            let tx_hashes: Vec<String> = receipts
                .iter()
                .map(|receipt| receipt.tx_hash.clone())
                .collect();
            let primary_tx_hash = receipts.last().map(|receipt| receipt.tx_hash.clone());
            let outcome = provider_operation_outcome(step, state.operation.as_ref())
                .map_err(|source| provider_execute_error(&step.provider, source))?;
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

async fn cctp_receive_already_claimed_completion(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    custody_vault_id: Uuid,
    action_for_response: &CustodyAction,
    provider_context: &Value,
    state: ProviderExecutionState,
    source: &str,
) -> Result<Option<StepCompletion>, OrderActivityError> {
    if step.step_type != OrderExecutionStepType::CctpReceive {
        return Ok(None);
    }
    let Some(output_asset) = step.output_asset.as_ref() else {
        return Ok(None);
    };
    let Some(balance) = authoritative_external_output_balance(deps, step, output_asset).await?
    else {
        return Ok(None);
    };
    let Some(expected_amount) = cctp_receive_expected_amount(step, &state) else {
        return Ok(None);
    };
    let observed = parse_raw_amount_for_provider(step, &balance.amount)?;
    let expected = parse_raw_amount_for_provider(step, expected_amount)?;
    if observed < expected {
        return Ok(None);
    }
    Ok(Some(cctp_receive_existing_balance_completion(
        step,
        custody_vault_id,
        action_for_response,
        provider_context,
        state,
        &balance,
        source,
    )))
}

fn cctp_receive_expected_amount<'a>(
    step: &'a OrderExecutionStep,
    state: &'a ProviderExecutionState,
) -> Option<&'a str> {
    state
        .operation
        .as_ref()
        .and_then(|operation| operation.request.as_ref())
        .and_then(|request| request.get("amount"))
        .and_then(decimal_string)
        .or_else(|| step.request.get("amount").and_then(decimal_string))
}

fn parse_raw_amount_for_provider(
    step: &OrderExecutionStep,
    value: &str,
) -> Result<U256, OrderActivityError> {
    U256::from_str_radix(value, 10).map_err(|source| {
        provider_execute_error(
            &step.provider,
            format!("invalid raw amount {value:?}: {source}"),
        )
    })
}

pub(super) fn cctp_receive_existing_balance_completion(
    step: &OrderExecutionStep,
    custody_vault_id: Uuid,
    action_for_response: &CustodyAction,
    provider_context: &Value,
    mut state: ProviderExecutionState,
    balance: &AuthoritativeOutputBalance,
    source: &str,
) -> StepCompletion {
    let provider_ref = format!("cctp_receive_already_claimed:{}", step.id);
    let observed_state = json!({
        "kind": "cctp_receive_already_claimed",
        "reason": "destination_custody_vault_already_funded",
        "custody_vault_id": custody_vault_id,
        "chain_id": balance.chain_id,
        "asset_id": balance.asset_id,
        "balance": balance.amount,
        "address": balance.address,
        "prior_error": source,
    });
    if let Some(operation) = state.operation.as_mut() {
        operation.status = ProviderOperationStatus::Completed;
        operation.provider_ref = Some(provider_ref);
        operation.observed_state = Some(observed_state.clone());
        operation.response = Some(json!({
            "kind": "cctp_receive_already_claimed",
            "amount_out": balance.amount,
        }));
    }
    StepCompletion {
        response: json!({
            "kind": "cctp_receive_already_claimed",
            "provider": &step.provider,
            "step_id": step.id,
            "custody_vault_id": custody_vault_id,
            "action": action_for_response,
            "provider_context": provider_context,
            "observed_state": observed_state,
        }),
        tx_hash: None,
        provider_state: state,
        outcome: StepExecutionOutcome::Completed,
    }
}

#[derive(Debug, Clone)]
pub(super) struct AuthoritativeOutputBalance {
    pub(super) amount: String,
    pub(super) location: &'static str,
    pub(super) custody_vault_id: Option<Uuid>,
    pub(super) address: Option<String>,
    pub(super) chain_id: String,
    pub(super) asset_id: String,
    pub(super) details: Value,
}

pub(super) async fn apply_authoritative_output_balance(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    mut completion: StepCompletion,
) -> Result<StepCompletion, OrderActivityError> {
    if completion.outcome != StepExecutionOutcome::Completed {
        return Ok(completion);
    }
    apply_authoritative_output_observation_to_response(deps, step, &mut completion.response)
        .await?;
    Ok(completion)
}

async fn apply_authoritative_output_observation_to_response(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    response: &mut Value,
) -> Result<(), OrderActivityError> {
    if let Some(balance) = authoritative_output_balance(deps, step).await? {
        if balance.amount == "0" {
            return Err(provider_execute_error(
                &step.provider,
                "authoritative output balance resolved to zero",
            ));
        }
        apply_authoritative_output_balance_to_response(response, &balance);
        return Ok(());
    }

    if let Some(claim_amount) = authoritative_claim_amount(step, response).map(str::to_owned) {
        apply_authoritative_claim_observation_to_response(step, response, &claim_amount);
    }
    strip_provider_reported_output_amounts(response);
    Ok(())
}

fn authoritative_claim_amount<'a>(
    step: &OrderExecutionStep,
    response: &'a Value,
) -> Option<&'a str> {
    if step.step_type != OrderExecutionStepType::CctpBurn {
        return None;
    }
    cctp_claim_amount_from_response(response)
}

fn cctp_claim_amount_from_response(response: &Value) -> Option<&str> {
    for pointer in [
        "/claim_observation/amount",
        "/observed_state/provider_observed_state/decoded_message_body/amount",
        "/observed_state/decoded_message_body/amount",
        "/messages/0/decodedMessage/decodedMessageBody/amount",
    ] {
        if let Some(amount) = response.pointer(pointer).and_then(decimal_string) {
            return Some(amount);
        }
    }
    None
}

fn apply_authoritative_claim_observation_to_response(
    step: &OrderExecutionStep,
    response: &mut Value,
    amount: &str,
) {
    if !response.is_object() {
        *response = json!({});
    }
    let output_asset = step.output_asset.as_ref();
    let object = response.as_object_mut().expect("response is object");
    object.insert(
        "claim_observation".to_string(),
        json!({
            "amount_out_source": "cctp_attested_message",
            "claim_kind": "cctp_receive_claim",
            "amount": amount,
            "chain_id": output_asset.map(|asset| asset.chain.to_string()),
            "asset_id": output_asset.map(|asset| asset.asset.to_string()),
        }),
    );
}

fn strip_provider_reported_output_amounts(response: &mut Value) {
    let Some(object) = response.as_object_mut() else {
        return;
    };
    for key in [
        "amount_out",
        "amountOut",
        "output_amount",
        "outputAmount",
        "expectedOutputAmount",
        "minOutputAmount",
    ] {
        if let Some(value) = object.remove(key) {
            let reported = object
                .entry("provider_reported_output_amounts".to_string())
                .or_insert_with(|| json!({}));
            if !reported.is_object() {
                *reported = json!({});
            }
            reported
                .as_object_mut()
                .expect("provider_reported_output_amounts is object")
                .entry(key.to_string())
                .or_insert(value);
        }
    }
}

async fn authoritative_output_balance(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
) -> Result<Option<AuthoritativeOutputBalance>, OrderActivityError> {
    if step.step_type == OrderExecutionStepType::CctpBurn {
        return Ok(None);
    }
    let Some(output_asset) = step.output_asset.as_ref() else {
        return Ok(None);
    };
    if output_asset.chain.as_str() == "hyperliquid" {
        return authoritative_hyperliquid_output_balance(deps, step, output_asset).await;
    }
    authoritative_external_output_balance(deps, step, output_asset).await
}

async fn authoritative_hyperliquid_output_balance(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    output_asset: &DepositAsset,
) -> Result<Option<AuthoritativeOutputBalance>, OrderActivityError> {
    if step.step_type == OrderExecutionStepType::HyperliquidBridgeDeposit
        && output_asset.asset == AssetId::Native
    {
        return authoritative_hyperliquid_clearinghouse_output_balance(deps, step, output_asset)
            .await;
    }
    let Some(vault) = hyperliquid_output_custody_vault(deps, step, output_asset).await? else {
        return Ok(None);
    };
    let registry = deps.action_providers.asset_registry();
    let Some(provider_asset) = registry.provider_asset(
        ProviderId::Hyperliquid,
        output_asset,
        ProviderAssetCapability::ExchangeOutput,
    ) else {
        return Ok(None);
    };
    let balances = deps
        .custody_action_executor
        .inspect_hyperliquid_spot_balances(&vault)
        .await
        .map_err(|source| custody_action_error("read Hyperliquid output balance", source))?;
    let Some(balance) = balances
        .into_iter()
        .find(|balance| balance.coin == provider_asset.provider_asset)
    else {
        return Ok(None);
    };
    let total_raw =
        decimal_amount_to_raw_u256(&balance.total, provider_asset.decimals).map_err(|reason| {
            provider_execute_error(
                &step.provider,
                format!(
                    "invalid Hyperliquid {} total balance: {reason}",
                    provider_asset.provider_asset
                ),
            )
        })?;
    let hold_raw =
        decimal_amount_to_raw_u256(&balance.hold, provider_asset.decimals).map_err(|reason| {
            provider_execute_error(
                &step.provider,
                format!(
                    "invalid Hyperliquid {} hold balance: {reason}",
                    provider_asset.provider_asset
                ),
            )
        })?;
    if hold_raw > total_raw {
        return Err(provider_execute_error(
            &step.provider,
            format!(
                "Hyperliquid {} hold balance exceeds total balance",
                provider_asset.provider_asset
            ),
        ));
    }
    let spendable_raw = total_raw - hold_raw;
    Ok(Some(AuthoritativeOutputBalance {
        amount: spendable_raw.to_string(),
        location: "hyperliquid_spot",
        custody_vault_id: Some(vault.id),
        address: Some(vault.address),
        chain_id: output_asset.chain.to_string(),
        asset_id: output_asset.asset.to_string(),
        details: json!({
            "coin": provider_asset.provider_asset,
            "total": balance.total,
            "hold": balance.hold,
            "decimals": provider_asset.decimals,
        }),
    }))
}

async fn authoritative_hyperliquid_clearinghouse_output_balance(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    output_asset: &DepositAsset,
) -> Result<Option<AuthoritativeOutputBalance>, OrderActivityError> {
    let Some(vault_id) = optional_request_uuid_field(step, "source_custody_vault_id")? else {
        return Ok(None);
    };
    let vault = deps
        .db
        .orders()
        .get_custody_vault(vault_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    let amount = deps
        .custody_action_executor
        .inspect_hyperliquid_clearinghouse_balance(&vault)
        .await
        .map_err(|source| {
            custody_action_error("read Hyperliquid clearinghouse output balance", source)
        })?;
    Ok(Some(AuthoritativeOutputBalance {
        amount: amount.to_string(),
        location: "hyperliquid_clearinghouse",
        custody_vault_id: Some(vault.id),
        address: Some(vault.address),
        chain_id: output_asset.chain.to_string(),
        asset_id: output_asset.asset.to_string(),
        details: json!({
            "coin": "USDC",
            "decimals": 6,
        }),
    }))
}

async fn hyperliquid_output_custody_vault(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    output_asset: &DepositAsset,
) -> Result<Option<CustodyVault>, OrderActivityError> {
    if let Some(vault_id) = optional_request_uuid_field(step, "hyperliquid_custody_vault_id")? {
        return deps
            .db
            .orders()
            .get_custody_vault(vault_id)
            .await
            .map(Some)
            .map_err(OrderActivityError::db_query);
    }
    let Some(address) = step
        .request
        .get("hyperliquid_custody_vault_address")
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    let vaults = deps
        .db
        .orders()
        .get_custody_vaults(step.order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    Ok(vaults.into_iter().find(|vault| {
        vault.address.eq_ignore_ascii_case(address)
            && vault.chain == output_asset.chain
            && vault.asset.as_ref() == Some(&output_asset.asset)
    }))
}

async fn authoritative_external_output_balance(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    output_asset: &DepositAsset,
) -> Result<Option<AuthoritativeOutputBalance>, OrderActivityError> {
    let Some(vault_id) = output_custody_vault_id(step)? else {
        return Ok(None);
    };
    let vault = deps
        .db
        .orders()
        .get_custody_vault(vault_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    if vault.chain != output_asset.chain || vault.asset.as_ref() != Some(&output_asset.asset) {
        return Ok(None);
    }
    let amount = deps
        .custody_action_executor
        .custody_vault_balance_raw(&vault)
        .await
        .map_err(|source| custody_action_error("read external output balance", source))?;
    Ok(Some(AuthoritativeOutputBalance {
        amount,
        location: "custody_vault",
        custody_vault_id: Some(vault.id),
        address: Some(vault.address),
        chain_id: output_asset.chain.to_string(),
        asset_id: output_asset.asset.to_string(),
        details: json!({}),
    }))
}

fn output_custody_vault_id(step: &OrderExecutionStep) -> Result<Option<Uuid>, OrderActivityError> {
    if let Some(vault_id) = optional_request_uuid_field(step, "recipient_custody_vault_id")? {
        return Ok(Some(vault_id));
    }
    if step.step_type == OrderExecutionStepType::CctpReceive {
        return optional_request_uuid_field(step, "source_custody_vault_id");
    }
    Ok(None)
}

fn optional_request_uuid_field(
    step: &OrderExecutionStep,
    field: &'static str,
) -> Result<Option<Uuid>, OrderActivityError> {
    let Some(value) = step.request.get(field) else {
        return Ok(None);
    };
    if value.is_null() {
        return Ok(None);
    }
    let Some(raw) = value.as_str() else {
        return Err(provider_execute_error(
            &step.provider,
            format!("step request field {field} must be a uuid string"),
        ));
    };
    Uuid::parse_str(raw).map(Some).map_err(|source| {
        provider_execute_error(
            &step.provider,
            format!("step request field {field} is not a uuid: {source}"),
        )
    })
}

fn decimal_amount_to_raw_u256(value: &str, decimals: u8) -> Result<U256, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("decimal amount must not be empty".to_string());
    }
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !frac.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let decimals = usize::from(decimals);
    let frac = if frac.len() > decimals {
        &frac[..decimals]
    } else {
        frac
    };
    let combined = format!("{whole}{:0<width$}", frac, width = decimals);
    let normalized = combined.trim_start_matches('0');
    let digits = if normalized.is_empty() {
        "0"
    } else {
        normalized
    };
    U256::from_str_radix(digits, 10).map_err(|source| source.to_string())
}

fn apply_authoritative_output_balance_to_response(
    response: &mut Value,
    balance: &AuthoritativeOutputBalance,
) {
    if !response.is_object() {
        *response = json!({});
    }
    strip_provider_reported_output_amounts(response);
    let object = response.as_object_mut().expect("response is object");
    object.insert("amount_out".to_string(), json!(balance.amount));
    object.insert(
        "balance_observation".to_string(),
        json!({
            "amount_out_source": "isolated_account_balance",
            "output": {
                "role": "destination",
                "location": balance.location,
                "chain_id": balance.chain_id,
                "asset_id": balance.asset_id,
                "custody_vault_id": balance.custody_vault_id,
                "address": balance.address,
                "balance": balance.amount,
                "spendable_balance": balance.amount,
                "details": balance.details,
            },
            "probes": [{
                "role": "destination",
                "location": balance.location,
                "chain_id": balance.chain_id,
                "asset_id": balance.asset_id,
                "custody_vault_id": balance.custody_vault_id,
                "address": balance.address,
                "balance": balance.amount,
                "spendable_balance": balance.amount,
                "credit_delta": balance.amount,
                "details": balance.details,
            }],
        }),
    );
}

pub(super) fn provider_state_records(
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
                idempotency_key: operation.idempotency_key.clone(),
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

pub(super) fn provider_operation_outcome(
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

pub(super) fn provider_only_outcome(
    step: &OrderExecutionStep,
    operation: Option<&ProviderOperationIntent>,
) -> RouterCoreResult<StepExecutionOutcome> {
    match operation {
        Some(operation) => provider_operation_outcome(step, Some(operation)),
        None => Ok(StepExecutionOutcome::Completed),
    }
}

pub(super) fn provider_operation_ref_for_persist(
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

pub(super) fn apply_post_execute_patch(
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

pub(super) fn provider_not_configured(step: &OrderExecutionStep) -> OrderActivityError {
    provider_execute_not_configured(&step.provider)
}

pub(super) fn provider_quote_error(
    provider: impl AsRef<str>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::provider_quote(provider.as_ref(), source)
}

pub(super) fn provider_execute_error(
    provider: impl AsRef<str>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::provider_execute(provider.as_ref(), source)
}

pub(super) fn provider_observe_error(
    provider: impl AsRef<str>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::provider_observe(provider.as_ref(), source)
}

pub(super) fn provider_quote_not_configured(provider: impl AsRef<str>) -> OrderActivityError {
    provider_quote_error(provider, "provider is not configured")
}

pub(super) fn provider_execute_not_configured(provider: impl AsRef<str>) -> OrderActivityError {
    provider_execute_error(provider, "provider is not configured")
}

pub(super) fn provider_observe_not_configured(provider: impl AsRef<str>) -> OrderActivityError {
    provider_observe_error(provider, "provider is not configured")
}

pub(super) fn invariant_error(
    invariant: &'static str,
    detail: impl Into<String>,
) -> OrderActivityError {
    OrderActivityError::invariant(invariant, detail)
}

pub(super) fn refund_discovery_error(context: impl Into<String>) -> OrderActivityError {
    OrderActivityError::refund_discovery(context)
}

pub(super) fn refund_materialization_error(context: impl Into<String>) -> OrderActivityError {
    OrderActivityError::refund_materialization(context)
}

pub(super) fn refresh_materialization_error(context: impl Into<String>) -> OrderActivityError {
    OrderActivityError::refresh_materialization(context)
}

pub(super) fn refresh_untenable_error(context: impl Into<String>) -> OrderActivityError {
    OrderActivityError::refresh_untenable(context)
}

pub(super) fn lost_intent_recovery_error(
    context: impl Into<String>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::lost_intent_recovery(context, source)
}

pub(super) fn custody_action_error(
    context: impl Into<String>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::custody_action(context, source)
}

pub(super) fn amount_parse_error(
    context: impl Into<String>,
    source: impl ToString,
) -> OrderActivityError {
    OrderActivityError::amount_parse(context, source)
}

pub(super) fn failed_attempt_snapshot(
    order: &RouterOrder,
    failed_step: &OrderExecutionStep,
) -> InputCustodySnapshot {
    InputCustodySnapshot::from_failed_step(order, failed_step)
}
