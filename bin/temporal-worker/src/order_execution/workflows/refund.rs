use super::shared::*;
use super::*;

/// Refund child workflow.
///
/// Scar tissue: §5 retry/refund decision, §8 position discovery, and §9 refund tree.
#[workflow]
#[derive(Default)]
pub struct RefundWorkflow {
    order_id: Option<WorkflowOrderId>,
    provider_operation_hints: Vec<ProviderOperationHintSignal>,
}

#[workflow_methods]
impl RefundWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: RefundWorkflowInput,
    ) -> WorkflowResult<RefundWorkflowOutput> {
        ctx.state_mut(|state| {
            state.order_id = Some(input.order_id);
        });
        let workflow_started_at = workflow_start_time(ctx, REFUND_WORKFLOW_TYPE);
        let db_activity_options = db_activity_options();
        let refund_execute_activity_options = refund_execute_activity_options();

        // Resolve the failed attempt id that drives `discover_single_refund_position`.
        // A child refund (`FailedAttempt`) inherits it from its parent
        // `OrderWorkflow`; a standalone admin refund (`Manual`) has no parent,
        // so it resolves the order's most recent execution attempt via an
        // activity. `VaultAlreadyRefunded` is not an executable trigger.
        let failed_attempt_id = match input.trigger {
            RefundTrigger::FailedAttempt => input.parent_attempt_id.ok_or_else(|| {
                anyhow::anyhow!("failed-attempt RefundWorkflow requires parent_attempt_id")
            })?,
            RefundTrigger::Manual => {
                ctx.start_activity(
                    RefundActivities::resolve_latest_execution_attempt,
                    ResolveLatestExecutionAttemptInput {
                        order_id: input.order_id,
                    },
                    db_activity_options.clone(),
                )
                .await?
                .attempt_id
            }
            RefundTrigger::VaultAlreadyRefunded => {
                return Err(anyhow::anyhow!(
                    "RefundWorkflow does not handle the VaultAlreadyRefunded trigger"
                )
                .into());
            }
        };

        loop {
            let discovery = ctx
                .start_activity(
                    RefundActivities::discover_single_refund_position,
                    DiscoverSingleRefundPositionInput {
                        order_id: input.order_id,
                        failed_attempt_id,
                    },
                    db_activity_options.clone(),
                )
                .await?;
            let position = match discovery.outcome {
                SingleRefundPositionOutcome::Position(position) => position,
                SingleRefundPositionOutcome::Untenable { reason } => {
                    let finalized = finalize_refund_required(
                        ctx,
                        input.order_id,
                        None,
                        None,
                        Some(json!(reason)),
                        db_activity_options.clone(),
                    )
                    .await?;
                    return refund_workflow_output(
                        ctx,
                        workflow_started_at,
                        input.order_id,
                        refund_terminal_status(finalized.terminal_status),
                    );
                }
            };

            let plan = ctx
                .start_activity(
                    RefundActivities::materialize_refund_plan,
                    MaterializeRefundPlanInput {
                        order_id: input.order_id,
                        failed_attempt_id,
                        position,
                    },
                    db_activity_options.clone(),
                )
                .await?;
            let (refund_attempt_id, steps) = match plan.outcome {
                RefundPlanOutcome::Materialized {
                    refund_attempt_id,
                    steps,
                } => (refund_attempt_id, steps),
                RefundPlanOutcome::Untenable { reason } => {
                    let finalized = finalize_refund_required(
                        ctx,
                        input.order_id,
                        None,
                        None,
                        Some(json!(reason)),
                        db_activity_options.clone(),
                    )
                    .await?;
                    return refund_workflow_output(
                        ctx,
                        workflow_started_at,
                        input.order_id,
                        refund_terminal_status(finalized.terminal_status),
                    );
                }
            };

            for step in steps {
                let dispatch_input = DispatchStepInput {
                    order_id: input.order_id,
                    attempt_id: refund_attempt_id,
                    step_id: step.step_id,
                };
                let dispatched = match crate::dispatch_step_activity!(
                    ctx,
                    step.step_type,
                    dispatch_input,
                    refund_execute_activity_options.clone()
                )
                .await
                {
                    Ok(dispatched) => dispatched,
                    Err(source) => {
                        let failure_reason = source.to_string();
                        drop(source);
                        let _failed = ctx
                            .start_activity(
                                OrderActivities::persist_step_failed,
                                PersistStepFailedInput {
                                    order_id: input.order_id,
                                    attempt_id: refund_attempt_id,
                                    step_id: step.step_id,
                                    failure_reason,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        let _snapshot = ctx
                            .start_activity(
                                OrderActivities::write_failed_attempt_snapshot,
                                WriteFailedAttemptSnapshotInput {
                                    order_id: input.order_id,
                                    attempt_id: refund_attempt_id,
                                    failed_step_id: step.step_id,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        let finalized = finalize_refund_required(
                            ctx,
                            input.order_id,
                            Some(refund_attempt_id),
                            Some(step.step_id),
                            Some(json!({
                                "reason": "refund_step_activity_failed",
                                "step_id": step.step_id,
                            })),
                            db_activity_options.clone(),
                        )
                        .await?;
                        return refund_workflow_output(
                            ctx,
                            workflow_started_at,
                            input.order_id,
                            refund_terminal_status(finalized.terminal_status),
                        );
                    }
                };
                match dispatched.outcome {
                    DispatchOutcome::Completed => {}
                    DispatchOutcome::Waiting => {
                        match wait_for_refund_provider_completion_hint(
                            ctx,
                            input.order_id,
                            refund_attempt_id,
                            step.step_id,
                            db_activity_options.clone(),
                        )
                        .await?
                        {
                            RefundProviderCompletionWait::Completed => {}
                            RefundProviderCompletionWait::RefundRequired { reason } => {
                                let finalized = finalize_refund_required(
                                    ctx,
                                    input.order_id,
                                    Some(refund_attempt_id),
                                    Some(step.step_id),
                                    Some(reason),
                                    db_activity_options.clone(),
                                )
                                .await?;
                                return refund_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    refund_terminal_status(finalized.terminal_status),
                                );
                            }
                        }
                    }
                    DispatchOutcome::StaleQuoteDetected { reason, .. } => {
                        let finalized = finalize_refund_required(
                            ctx,
                            input.order_id,
                            Some(refund_attempt_id),
                            Some(step.step_id),
                            reason.or_else(|| {
                                Some(json!({
                                    "reason": "refund_step_stale_quote_detected",
                                    "step_id": step.step_id,
                                }))
                            }),
                            db_activity_options.clone(),
                        )
                        .await?;
                        return refund_workflow_output(
                            ctx,
                            workflow_started_at,
                            input.order_id,
                            refund_terminal_status(finalized.terminal_status),
                        );
                    }
                }
            }

            let _finalized = ctx
                .start_activity(
                    OrderActivities::finalize_order_or_refund,
                    FinalizeOrderOrRefundInput {
                        order_id: input.order_id,
                        attempt_id: Some(refund_attempt_id),
                        step_id: None,
                        terminal_status: OrderTerminalStatus::Refunded,
                        reason: None,
                        funded_to_workflow_start_seconds: input.funded_to_workflow_start_seconds,
                    },
                    db_activity_options,
                )
                .await?;
            return refund_workflow_output(
                ctx,
                workflow_started_at,
                input.order_id,
                RefundTerminalStatus::Refunded,
            );
        }
    }

    /// Scar tissue: §10 provider operation hint flow for refund transition steps.
    #[signal]
    pub fn provider_operation_hint(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ProviderOperationHintSignal,
    ) {
        maybe_record_signal(ctx, REFUND_WORKFLOW_TYPE, "provider_operation_hint");
        if self
            .order_id
            .is_none_or(|order_id| order_id == signal.order_id)
        {
            self.provider_operation_hints.push(signal);
        }
    }
}

impl RefundWorkflow {
    fn has_provider_operation_hint(
        &self,
        order_id: WorkflowOrderId,
        step_id: WorkflowStepId,
    ) -> bool {
        self.provider_operation_hints
            .iter()
            .any(|signal| provider_operation_hint_targets_step(signal, order_id, step_id))
    }

    fn pop_provider_operation_hint(
        &mut self,
        order_id: WorkflowOrderId,
        step_id: WorkflowStepId,
    ) -> Option<ProviderOperationHintSignal> {
        let index = self
            .provider_operation_hints
            .iter()
            .position(|signal| provider_operation_hint_targets_step(signal, order_id, step_id))?;
        Some(self.provider_operation_hints.remove(index))
    }
}

enum RefundProviderCompletionWait {
    Completed,
    RefundRequired { reason: Value },
}

async fn record_refund_provider_hint_timeout(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    reason: Value,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<()> {
    tracing::warn!(
        order_id = %order_id,
        attempt_id = %attempt_id,
        step_id = %step_id,
        reason = %reason,
        event_name = "provider_operation_hint.refund_required",
        "provider_operation_hint.refund_required"
    );
    let _failed = ctx
        .start_activity(
            OrderActivities::persist_step_failed,
            PersistStepFailedInput {
                order_id,
                attempt_id,
                step_id,
                failure_reason: reason.to_string(),
            },
            db_activity_options.clone(),
        )
        .await?;
    let _snapshot = ctx
        .start_activity(
            OrderActivities::write_failed_attempt_snapshot,
            WriteFailedAttemptSnapshotInput {
                order_id,
                attempt_id,
                failed_step_id: step_id,
            },
            db_activity_options.clone(),
        )
        .await?;
    Ok(())
}

async fn finalize_refund_required(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: WorkflowOrderId,
    refund_attempt_id: Option<WorkflowAttemptId>,
    step_id: Option<WorkflowStepId>,
    reason: Option<Value>,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    Ok(ctx
        .start_activity(
            OrderActivities::finalize_order_or_refund,
            FinalizeOrderOrRefundInput {
                order_id,
                attempt_id: refund_attempt_id,
                step_id,
                terminal_status: OrderTerminalStatus::RefundRequired,
                reason,
                funded_to_workflow_start_seconds: None,
            },
            db_activity_options,
        )
        .await?)
}

async fn wait_for_refund_provider_completion_hint(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<RefundProviderCompletionWait> {
    let wait_started_at = provider_hint_wait_started(ctx);

    // Single-timer restructure: see the detailed rationale on the analogous
    // `wait_for_provider_completion_hint` in order.rs. The old shape armed a
    // fresh 2h timer per deferred hint (2-4 per step) that the SDK never
    // dropped/cancelled. We now arm one timer, run the deferred-hint loop
    // inside the work arm of a single `select!`, and explicitly `.cancel()`
    // the timer on Accept (zero leak on the happy path). All work-loop calls
    // take `&self` on the SDK context, so the shared borrow does not collide
    // with the timer future's held `&ctx` borrow.
    // The timer + select + Accept handling are scoped to an inner block so the
    // `ctx.timer()` future's held `&ctx` borrow is released before the
    // `&mut ctx` timeout escalation runs. See order.rs for the full rationale.
    let wait_outcome = {
        let hint_timeout = ctx.timer(PROVIDER_HINT_WAIT_TIMEOUT);
        futures_util::pin_mut!(hint_timeout);
        let ctx_ref: &WorkflowContext<RefundWorkflow> = ctx;
        let hint_work = async {
            loop {
                ctx_ref
                    .wait_condition(move |state: &RefundWorkflow| {
                        state.has_provider_operation_hint(order_id, step_id)
                    })
                    .await;
                let signal = ctx_ref
                    .state_mut(|state| state.pop_provider_operation_hint(order_id, step_id))
                    .expect("refund provider operation hint condition was satisfied");
                let hint_kind = signal.hint_kind;
                let verify_input = VerifyProviderOperationHintInput {
                    order_id,
                    attempt_id,
                    step_id,
                    signal,
                };
                let verified = crate::verify_hint_activity!(
                    ctx_ref,
                    hint_kind,
                    verify_input,
                    db_activity_options.clone()
                )
                .await?;

                match verified.decision {
                    ProviderOperationHintDecision::Accept => {
                        return WorkflowResult::<()>::Ok(());
                    }
                    ProviderOperationHintDecision::Reject
                    | ProviderOperationHintDecision::Defer => {
                        tracing::info!(
                            order_id = %order_id,
                            step_id = %step_id,
                            hint_kind = ?hint_kind,
                            provider_operation_id = ?verified.provider_operation_id,
                            decision = ?verified.decision,
                            reason = ?verified.reason,
                            event_name = "provider_operation_hint.ignored",
                            "provider_operation_hint.ignored"
                        );
                        // SAME timer: no new timer armed on a deferred hint.
                    }
                }
            }
        };
        use futures_util::FutureExt;
        let hint_work = hint_work.fuse();
        futures_util::pin_mut!(hint_work);
        let outcome = temporalio_sdk::workflows::select! {
            _ = hint_timeout.as_mut() => RefundHintWaitOutcome::TimedOut,
            res = hint_work => RefundHintWaitOutcome::Accepted(res),
        };
        match outcome {
            RefundHintWaitOutcome::Accepted(res) => {
                res?;
                hint_timeout.cancel();
                record_provider_hint_wait(
                    ctx,
                    REFUND_WORKFLOW_TYPE,
                    wait_started_at,
                    "signal_accept",
                );
                RefundHintWaitOutcome::Accepted(Ok(()))
            }
            RefundHintWaitOutcome::TimedOut => RefundHintWaitOutcome::TimedOut,
        }
    };

    match wait_outcome {
        RefundHintWaitOutcome::Accepted(_) => Ok(RefundProviderCompletionWait::Completed),
        RefundHintWaitOutcome::TimedOut => {
            let reason = json!({
                "reason": "provider_operation_hint_wait_timeout",
                "step_id": step_id,
            });
            record_refund_provider_hint_timeout(
                ctx,
                order_id,
                attempt_id,
                step_id,
                reason.clone(),
                db_activity_options.clone(),
            )
            .await?;
            record_provider_hint_wait(
                ctx,
                REFUND_WORKFLOW_TYPE,
                wait_started_at,
                "timeout_refund_required",
            );
            Ok(RefundProviderCompletionWait::RefundRequired { reason })
        }
    }
}

enum RefundHintWaitOutcome {
    Accepted(WorkflowResult<()>),
    TimedOut,
}
