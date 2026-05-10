use super::*;

#[activities]
impl OrderActivities {
    /// Scar tissue: §13 step type dispatch provider trait family mapping.
    #[activity]
    pub async fn execute_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ExecuteStepInput,
    ) -> Result<StepExecuted, ActivityError> {
        record_activity("execute_step", async move {
            let deps = self.deps()?;
            let step = deps
                .db
                .orders()
                .get_execution_step(input.step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if step.order_id != input.order_id.inner()
                || step.execution_attempt_id != Some(input.attempt_id.inner())
            {
                return Err(OrderActivityError::invariant(
                    "step_belongs_to_order_attempt",
                    format!(
                        "step {} does not belong to order {} attempt {}",
                        step.id,
                        input.order_id.inner(),
                        input.attempt_id.inner()
                    ),
                ));
            }
            if step.status != OrderExecutionStepStatus::Running {
                return Err(OrderActivityError::invariant(
                    "step_running_before_provider_execution",
                    format!(
                        "step {} must be running before provider execution, got {}",
                        step.id,
                        step.status.to_db_string()
                    ),
                ));
            }
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
            let order = deps
                .db
                .orders()
                .get(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
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

    #[activity]
    pub async fn load_manual_intervention_context(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: LoadManualInterventionContextInput,
    ) -> Result<ManualInterventionWorkflowContext, ActivityError> {
        record_activity("load_manual_intervention_context", async move {
            let deps = self.deps()?;
            let expected_attempt_kind = match input.scope {
                ManualInterventionScope::OrderAttempt => {
                    OrderExecutionAttemptKind::PrimaryExecution
                }
                ManualInterventionScope::RefundAttempt => OrderExecutionAttemptKind::RefundRecovery,
            };
            let attempt = deps
                .db
                .orders()
                .get_execution_attempts(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?
                .into_iter()
                .filter(|attempt| attempt.attempt_kind == expected_attempt_kind)
                .max_by_key(|attempt| (attempt.attempt_index, attempt.updated_at));
            let Some(attempt) = attempt else {
                return Ok(ManualInterventionWorkflowContext {
                    attempt_id: None,
                    step_id: None,
                });
            };
            let step = deps
                .db
                .orders()
                .get_execution_steps_for_attempt(attempt.id)
                .await
                .map_err(OrderActivityError::db_query)?
                .into_iter()
                .filter(|step| step.status != OrderExecutionStepStatus::Superseded)
                .max_by_key(|step| {
                    (
                        manual_context_step_rank(step.status),
                        step.updated_at,
                        step.step_index,
                    )
                });
            Ok(ManualInterventionWorkflowContext {
                attempt_id: Some(attempt.id.into()),
                step_id: step.map(|step| step.id.into()),
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
                    })
                    .collect(),
            })
        })
        .await
    }

    /// Scar tissue: §2.1 `AfterExecutionLegsPersisted`.
    #[activity]
    pub async fn persist_step_ready_to_fire(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistStepReadyToFireInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        record_activity("persist_step_ready_to_fire", async move {
            let deps = self.deps()?;
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
            tracing::info!(
                order_id = %input.order_id.inner(),
                attempt_id = %input.attempt_id.inner(),
                step_id = %step.id,
                step_index = step.step_index,
                event_name = "execution_step.started",
                "execution_step.started"
            );
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterExecutionLegsPersisted,
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
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterStepMarkedFailed,
            })
        })
        .await
    }

    /// Scar tissue: §2 boundary 4, `AfterProviderReceiptPersisted`.
    #[activity]
    pub async fn persist_provider_receipt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistProviderReceiptInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        record_activity("persist_provider_receipt", async move {
            let deps = self.deps()?;
            let step = deps
                .db
                .orders()
                .get_execution_step(input.execution.step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let (operation, mut addresses) = provider_state_records(
                &step,
                &input.execution.provider_state,
                &input.execution.response,
            )
            .map_err(|source| provider_execute_error(&step.provider, source))?;
            if let Some(mut operation) = operation {
                operation.status = receipt_boundary_status(operation.status);
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
                tracing::info!(
                    order_id = %input.execution.order_id.inner(),
                    provider_operation_id = %provider_operation_id,
                    event_name = "provider_operation.persisted",
                    "provider_operation.persisted"
                );
            }
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterProviderReceiptPersisted,
            })
        })
        .await
    }

    /// Scar tissue: §2 boundary 5, `AfterProviderOperationStatusPersisted`.
    #[activity]
    pub async fn persist_provider_operation_status(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PersistProviderOperationStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        record_activity("persist_provider_operation_status", async move {
            let deps = self.deps()?;
            let step = deps
                .db
                .orders()
                .get_execution_step(input.execution.step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let (operation, _) = provider_state_records(
                &step,
                &input.execution.provider_state,
                &input.execution.response,
            )
            .map_err(|source| provider_execute_error(&step.provider, source))?;
            if let Some(operation) = operation {
                let provider_operation_id = deps
                    .db
                    .orders()
                    .upsert_provider_operation(&operation)
                    .await
                    .map_err(OrderActivityError::db_query)?;
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
            }
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterProviderOperationStatusPersisted,
            })
        })
        .await
    }

    /// Scar tissue: §2 boundary 6, `AfterProviderStepSettlement`.
    #[activity]
    pub async fn settle_provider_step(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: SettleProviderStepInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        record_activity("settle_provider_step", async move {
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
                    step_id: input.execution.step_id.inner(),
                    operation: None,
                    addresses: Vec::new(),
                    response: input.execution.response,
                    tx_hash: input.execution.tx_hash,
                    usd_valuation: json!({}),
                    completed_at: Utc::now(),
                })
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %input.execution.order_id.inner(),
                attempt_id = %input.execution.attempt_id.inner(),
                step_id = %record.step.id,
                event_name = "execution_step.completed",
                "execution_step.completed"
            );
            Ok(BoundaryPersisted {
                boundary: PersistenceBoundary::AfterProviderStepSettlement,
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
        if refreshed_attempt_count >= MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION
            && matches!(order_quote, RouterOrderQuote::MarketOrder(_))
            && attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery
        {
            telemetry::record_stale_quote_refresh_cap_hit();
        }
        let decision = if stale_quote_refresh_allowed {
            telemetry::record_stale_quote_refresh("post_failure_detected");
            StepFailureDecision::RefreshQuote
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
            decision = ?decision,
            event_name = "execution_step.failure_classified",
            "execution_step.failure_classified"
        );
        Ok(decision)
        })
        .await
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
        record_activity("check_pre_execution_stale_quote", async move {
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
                .get_router_order_quote(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if !matches!(order_quote, RouterOrderQuote::MarketOrder(_)) {
                return Ok(PreExecutionStaleQuoteCheck {
                    should_refresh: false,
                    reason: None,
                });
            }
            let refreshed_attempt_count = deps
                .db
                .orders()
                .get_execution_attempts(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?
                .into_iter()
                .filter(|attempt| {
                    attempt.attempt_kind == OrderExecutionAttemptKind::RefreshedExecution
                })
                .count();
            if refreshed_attempt_count >= MAX_STALE_QUOTE_REFRESHES_PER_ORDER_EXECUTION {
                telemetry::record_stale_quote_refresh_cap_hit();
                return Ok(PreExecutionStaleQuoteCheck {
                    should_refresh: false,
                    reason: None,
                });
            }
            let step = deps
                .db
                .orders()
                .get_execution_step(input.step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if step.order_id != input.order_id.inner()
                || step.execution_attempt_id != Some(input.attempt_id.inner())
            {
                return Err(OrderActivityError::invariant(
                    "step_belongs_to_order_attempt",
                    format!(
                        "step {} does not belong to order {} attempt {}",
                        step.id,
                        input.order_id.inner(),
                        input.attempt_id.inner()
                    ),
                ));
            }
            let provider_operations = deps
                .db
                .orders()
                .get_provider_operations(input.order_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            let provider_side_effect_started =
                step_provider_side_effect_started(&step, &provider_operations);
            if step.status != OrderExecutionStepStatus::Running || provider_side_effect_started {
                return Ok(PreExecutionStaleQuoteCheck {
                    should_refresh: false,
                    reason: None,
                });
            }
            let attempt_steps = deps
                .db
                .orders()
                .get_execution_steps_for_attempt(input.attempt_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if leg_already_crossed_provider_boundary(&step, &attempt_steps, &provider_operations) {
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
                .get_execution_legs_for_attempt(input.attempt_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
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
                order_id = %input.order_id.inner(),
                attempt_id = %input.attempt_id.inner(),
                step_id = %step.id,
                execution_leg_id = %leg.id,
                provider_quote_expires_at = %provider_quote_expires_at,
                event_name = "execution_step.pre_execution_stale_quote_detected",
                "execution_step.pre_execution_stale_quote_detected"
            );
            telemetry::record_stale_quote_refresh("pre_execution_detected");
            Ok(PreExecutionStaleQuoteCheck {
                should_refresh: true,
                reason: Some(reason),
            })
        })
        .await
    }

    /// Scar tissue: §6 stale running step manual intervention.
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
                    Ok(FinalizedOrder {
                        order_id: completed.order.id.into(),
                        terminal_status: OrderTerminalStatus::Refunded,
                    })
                }
                OrderTerminalStatus::RefundManualInterventionRequired => {
                    let order = deps
                        .db
                        .orders()
                        .mark_order_refund_manual_intervention_required(
                            input.order_id.inner(),
                            Utc::now(),
                        )
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    tracing::info!(
                        order_id = %order.id,
                        event_name = "order.refund_manual_intervention_required",
                        "order.refund_manual_intervention_required"
                    );
                    Ok(FinalizedOrder {
                        order_id: order.id.into(),
                        terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                    })
                }
                OrderTerminalStatus::ManualInterventionRequired => {
                    let attempt_id = input.attempt_id.map(|id| id.inner()).ok_or_else(|| {
                        invariant_error(
                            "finalize_manual_intervention_requires_attempt",
                            "finalize manual intervention requires an execution attempt id",
                        )
                    })?;
                    let step_id = input.step_id.map(|id| id.inner()).ok_or_else(|| {
                        invariant_error(
                            "finalize_manual_intervention_requires_step",
                            "finalize manual intervention requires an execution step id",
                        )
                    })?;
                    let order = finalize_execution_manual_intervention(
                        &deps,
                        input.order_id.inner(),
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
                        order_id: order.id.into(),
                        terminal_status: OrderTerminalStatus::ManualInterventionRequired,
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

    /// Operator-driven release from the root ManualInterventionRequired wait-state.
    #[activity]
    pub async fn prepare_manual_intervention_retry(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PrepareManualInterventionRetryInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        record_activity("prepare_manual_intervention_retry", async move {
            let deps = self.deps()?;
            let resolution = json!({
                "kind": "manual_intervention_resolution",
                "action": "release",
                "reason": input.signal.reason,
                "operator_id": input.signal.operator_id,
                "requested_at": input.signal.requested_at,
            });
            let order = deps
                .db
                .orders()
                .prepare_manual_intervention_retry(
                    input.order_id.inner(),
                    input.attempt_id.inner(),
                    input.step_id.inner(),
                    resolution,
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %order.id,
                attempt_id = %input.attempt_id.inner(),
                step_id = %input.step_id.inner(),
                event_name = "order.manual_intervention_released",
                "order.manual_intervention_released"
            );
            Ok(FinalizedOrder {
                order_id: order.id.into(),
                terminal_status: OrderTerminalStatus::ManualInterventionRequired,
            })
        })
        .await
    }

    /// Operator-driven refund trigger from the root ManualInterventionRequired wait-state.
    #[activity]
    pub async fn prepare_manual_intervention_refund(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: PrepareManualInterventionRefundInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        record_activity("prepare_manual_intervention_refund", async move {
            let deps = self.deps()?;
            let resolution = json!({
                "kind": "manual_intervention_resolution",
                "action": "trigger_refund",
                "reason": input.signal.reason,
                "operator_id": input.signal.operator_id,
                "requested_at": input.signal.requested_at,
                "refund_kind_hint": input.signal.refund_kind_hint,
            });
            let order = deps
                .db
                .orders()
                .prepare_manual_intervention_refund(
                    input.order_id.inner(),
                    input.attempt_id.inner(),
                    input.step_id.inner(),
                    resolution,
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %order.id,
                attempt_id = %input.attempt_id.inner(),
                step_id = %input.step_id.inner(),
                event_name = "order.manual_intervention_refund_triggered",
                "order.manual_intervention_refund_triggered"
            );
            Ok(FinalizedOrder {
                order_id: order.id.into(),
                terminal_status: OrderTerminalStatus::RefundRequired,
            })
        })
        .await
    }

    /// Operator-driven release from RefundManualInterventionRequired.
    #[activity]
    pub async fn release_refund_manual_intervention(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ReleaseRefundManualInterventionInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        record_activity("release_refund_manual_intervention", async move {
            let deps = self.deps()?;
            let resolution = json!({
                "kind": "manual_intervention_resolution",
                "action": match input.signal_kind {
                    ManualResolutionSignalKind::Release => "release",
                    ManualResolutionSignalKind::TriggerRefund => "trigger_refund",
                },
                "reason": input.reason,
                "operator_id": input.operator_id,
                "requested_at": input.requested_at,
            });
            let order = deps
                .db
                .orders()
                .release_refund_manual_intervention(
                    input.order_id.inner(),
                    input.refund_attempt_id.map(|id| id.inner()),
                    input.step_id.map(|id| id.inner()),
                    resolution,
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %order.id,
                refund_attempt_id = ?input.refund_attempt_id,
                step_id = ?input.step_id,
                event_name = "order.refund_manual_intervention_released",
                "order.refund_manual_intervention_released"
            );
            Ok(FinalizedOrder {
                order_id: order.id.into(),
                terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
            })
        })
        .await
    }

    /// Acknowledges manual intervention as a true terminal state.
    #[activity]
    pub async fn acknowledge_manual_intervention_terminal(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: AcknowledgeManualInterventionInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        record_activity("acknowledge_manual_intervention_terminal", async move {
            let deps = self.deps()?;
            let refund_manual = matches!(input.scope, ManualInterventionScope::RefundAttempt);
            let zombie_cleanup = matches!(input.reason, AcknowledgeReason::ZombieCleanup);
            let terminal_status = match input.scope {
                ManualInterventionScope::OrderAttempt => {
                    OrderTerminalStatus::ManualInterventionRequired
                }
                ManualInterventionScope::RefundAttempt => {
                    OrderTerminalStatus::RefundManualInterventionRequired
                }
            };
            let resolution = json!({
                "kind": "manual_intervention_terminal_ack",
                "action": if zombie_cleanup {
                    "zombie_cleanup"
                } else {
                    "acknowledge_unrecoverable"
                },
                "reason": input.signal.reason,
                "operator_id": input.signal.operator_id,
                "requested_at": input.signal.requested_at,
            });
            let order = deps
                .db
                .orders()
                .acknowledge_manual_intervention_terminal(
                    input.order_id.inner(),
                    input.attempt_id.map(|id| id.inner()),
                    input.step_id.map(|id| id.inner()),
                    refund_manual,
                    resolution,
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %order.id,
                terminal_status = ?terminal_status,
                zombie_cleanup,
                event_name = "order.manual_intervention_terminal_acknowledged",
                "order.manual_intervention_terminal_acknowledged"
            );
            Ok(FinalizedOrder {
                order_id: order.id.into(),
                terminal_status,
            })
        })
        .await
    }
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

pub(super) async fn finalize_execution_manual_intervention(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
    reason: Value,
) -> Result<RouterOrder, OrderActivityError> {
    let now = Utc::now();
    let step = deps
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    if step.order_id != order_id || step.execution_attempt_id != Some(attempt_id) {
        return Err(invariant_error(
            "step_belongs_to_order_attempt",
            format!(
                "step {} does not belong to order {} attempt {}",
                step.id, order_id, attempt_id
            ),
        ));
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
            .map_err(OrderActivityError::db_query)?,
        OrderExecutionStepStatus::Waiting => deps
            .db
            .orders()
            .fail_observed_execution_step(step.id, step_error, now)
            .await
            .map_err(OrderActivityError::db_query)?,
        OrderExecutionStepStatus::Failed => step,
        status => {
            return Err(OrderActivityError::invalid_terminal_state(
                status.to_db_string(),
                "running, waiting, or failed",
            ));
        }
    };

    let order = deps
        .db
        .orders()
        .get(order_id)
        .await
        .map_err(OrderActivityError::db_query)?;
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
                .map_err(OrderActivityError::db_query)?;
            if attempt.status != OrderExecutionAttemptStatus::ManualInterventionRequired {
                return Err(invariant_error(
                    "attempt_eligible_for_manual_intervention_finalization",
                    format!(
                        "attempt {} was not eligible for manual intervention finalization",
                        attempt_id
                    ),
                ));
            }
        }
        Err(source) => return Err(OrderActivityError::db_query(source)),
    }

    deps.db
        .orders()
        .mark_order_manual_intervention_required(order_id, now)
        .await
        .map_err(OrderActivityError::db_query)
}

pub(super) async fn settle_waiting_provider_step(
    deps: &OrderActivityDeps,
    execution: &StepExecuted,
) -> Result<(), OrderActivityError> {
    let operations = deps
        .db
        .orders()
        .get_provider_operations(execution.order_id.inner())
        .await
        .map_err(OrderActivityError::db_query)?;
    let operation = operations
        .iter()
        .filter(|operation| operation.execution_step_id == Some(execution.step_id.inner()))
        .max_by_key(|operation| (operation.updated_at, operation.created_at, operation.id));

    match operation.map(|operation| operation.status) {
        Some(ProviderOperationStatus::Completed) => {
            let operation = operation.expect("checked completed operation above");
            complete_observed_provider_step(deps, execution, operation).await?;
            tracing::info!(
                order_id = %execution.order_id.inner(),
                attempt_id = %execution.attempt_id.inner(),
                step_id = %execution.step_id.inner(),
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
                    execution.step_id.inner(),
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
                        .get_execution_step(execution.step_id.inner())
                        .await
                        .map_err(OrderActivityError::db_query)?;
                    if current.status != OrderExecutionStepStatus::Failed {
                        return Err(invariant_error(
                            "failed_provider_step_status",
                            format!(
                                "failed provider step {} is in unexpected status {}",
                                execution.step_id.inner(),
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
                .wait_execution_step(
                    execution.step_id.inner(),
                    execution.response.clone(),
                    execution.tx_hash.clone(),
                    Utc::now(),
                )
                .await;
            match waiting {
                Ok(_) => {
                    tracing::info!(
                        order_id = %execution.order_id.inner(),
                        attempt_id = %execution.attempt_id.inner(),
                        step_id = %execution.step_id.inner(),
                        event_name = "execution_step.waiting_external",
                        "execution_step.waiting_external"
                    );
                }
                Err(RouterCoreError::NotFound) => {
                    let current = deps
                        .db
                        .orders()
                        .get_execution_step(execution.step_id.inner())
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
                                execution.step_id.inner(),
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

pub(super) async fn complete_observed_provider_step(
    deps: &OrderActivityDeps,
    execution: &StepExecuted,
    operation: &OrderProviderOperation,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let completed = deps
        .db
        .orders()
        .complete_observed_execution_step(
            execution.step_id.inner(),
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
                .get_execution_step(execution.step_id.inner())
                .await
                .map_err(OrderActivityError::db_query)?;
            if current.status == OrderExecutionStepStatus::Completed {
                Ok(current)
            } else {
                Err(invariant_error(
                    "completed_provider_operation_settlement_status",
                    format!(
                        "completed provider operation {} could not settle step {} in status {}",
                        operation.id,
                        execution.step_id.inner(),
                        current.status.to_db_string()
                    ),
                ))
            }
        }
        Err(source) => Err(OrderActivityError::db_query(source)),
    }
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
    response: Value,
    tx_hash: Option<String>,
    provider_state: ProviderExecutionState,
    outcome: StepExecutionOutcome,
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
    let intent = match step.step_type {
        OrderExecutionStepType::WaitForDeposit => {
            return Err(invariant_error(
                "wait_for_deposit_not_provider_executable",
                format!(
                    "{} is not provider executable",
                    step.step_type.to_db_string()
                ),
            ));
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
                .map_err(|source| custody_action_error("execute refund transfer", source))?;
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
            let request = BridgeExecutionRequest::across_from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::CctpBurn => {
            let request = BridgeExecutionRequest::cctp_burn_from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::CctpReceive => {
            let request_json = hydrate_cctp_receive_request(deps, step).await?;
            let request = BridgeExecutionRequest::cctp_receive_from_value(&request_json)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::HyperliquidBridgeDeposit => {
            let request =
                BridgeExecutionRequest::hyperliquid_bridge_deposit_from_value(&step.request)
                    .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::HyperliquidBridgeWithdrawal => {
            let request =
                BridgeExecutionRequest::hyperliquid_bridge_withdrawal_from_value(&step.request)
                    .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .bridge(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_bridge(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::UnitDeposit => {
            let request = UnitDepositStepRequest::from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .unit(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_deposit(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::UnitWithdrawal => {
            let request = UnitWithdrawalStepRequest::from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .unit(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_withdrawal(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::HyperliquidTrade => {
            let request = ExchangeExecutionRequest::hyperliquid_trade_from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::HyperliquidLimitOrder => {
            let request =
                ExchangeExecutionRequest::hyperliquid_limit_order_from_value(&step.request)
                    .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
        OrderExecutionStepType::UniversalRouterSwap => {
            let request = ExchangeExecutionRequest::universal_router_swap_from_value(&step.request)
                .map_err(|source| provider_execute_error(&step.provider, source))?;
            deps.action_providers
                .exchange(&step.provider)
                .ok_or_else(|| provider_not_configured(step))?
                .execute_trade(&request)
                .await
                .map_err(|source| provider_execute_error(&step.provider, source))?
        }
    };

    prepare_provider_completion(deps, step, intent).await
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
                .map_err(|source| {
                    custody_action_error("execute provider custody action", source)
                })?;
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

pub(super) fn receipt_boundary_status(status: ProviderOperationStatus) -> ProviderOperationStatus {
    match status {
        ProviderOperationStatus::Completed
        | ProviderOperationStatus::Failed
        | ProviderOperationStatus::Expired => ProviderOperationStatus::Submitted,
        status => status,
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
) -> Value {
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
