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
    manual_releases: Vec<ManualReleaseSignal>,
    manual_refund_triggers: Vec<ManualTriggerRefundSignal>,
    acknowledge_unrecoverables: Vec<AcknowledgeUnrecoverableSignal>,
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
        if input.trigger != RefundTrigger::FailedAttempt {
            return Err(anyhow::anyhow!(
                "PR6a RefundWorkflow only handles failed-attempt triggers"
            )
            .into());
        }
        let failed_attempt_id = input.parent_attempt_id.ok_or_else(|| {
            anyhow::anyhow!("failed-attempt RefundWorkflow requires parent_attempt_id")
        })?;
        let db_activity_options = db_activity_options();
        let refund_execute_activity_options = refund_execute_activity_options();

        'refund: loop {
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
                SingleRefundPositionOutcome::Untenable { .. } => {
                    match wait_for_refund_manual_intervention_resolution(
                        ctx,
                        input.order_id,
                        None,
                        None,
                        db_activity_options.clone(),
                    )
                    .await?
                    {
                        RefundManualInterventionResolution::Continue => continue 'refund,
                        RefundManualInterventionResolution::Terminal(finalized) => {
                            return refund_workflow_output(
                                ctx,
                                workflow_started_at,
                                input.order_id,
                                refund_terminal_status(finalized.terminal_status),
                            );
                        }
                    }
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
                RefundPlanOutcome::Untenable { .. } => {
                    match wait_for_refund_manual_intervention_resolution(
                        ctx,
                        input.order_id,
                        None,
                        None,
                        db_activity_options.clone(),
                    )
                    .await?
                    {
                        RefundManualInterventionResolution::Continue => continue 'refund,
                        RefundManualInterventionResolution::Terminal(finalized) => {
                            return refund_workflow_output(
                                ctx,
                                workflow_started_at,
                                input.order_id,
                                refund_terminal_status(finalized.terminal_status),
                            );
                        }
                    }
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
                        match wait_for_refund_manual_intervention_resolution(
                            ctx,
                            input.order_id,
                            Some(refund_attempt_id),
                            Some(step.step_id),
                            db_activity_options.clone(),
                        )
                        .await?
                        {
                            RefundManualInterventionResolution::Continue => continue 'refund,
                            RefundManualInterventionResolution::Terminal(finalized) => {
                                return refund_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    refund_terminal_status(finalized.terminal_status),
                                );
                            }
                        }
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
                            RefundProviderCompletionWait::RefundManualInterventionRequired {
                                resolution,
                            } => match resolution {
                                RefundManualInterventionResolution::Continue => continue 'refund,
                                RefundManualInterventionResolution::Terminal(finalized) => {
                                    return refund_workflow_output(
                                        ctx,
                                        workflow_started_at,
                                        input.order_id,
                                        refund_terminal_status(finalized.terminal_status),
                                    );
                                }
                            },
                        }
                    }
                    DispatchOutcome::StaleQuoteDetected { .. } => {
                        return Err(anyhow::anyhow!(
                            "RefundWorkflow does not handle stale-quote refresh: step {} produced StaleQuoteDetected",
                            step.step_id
                        )
                        .into());
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

    #[signal]
    pub fn manual_refund_trigger(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ManualTriggerRefundSignal,
    ) {
        maybe_record_signal(ctx, REFUND_WORKFLOW_TYPE, "manual_trigger_refund");
        self.manual_refund_triggers.push(signal);
    }

    #[signal]
    pub fn manual_intervention_release(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ManualReleaseSignal,
    ) {
        maybe_record_signal(ctx, REFUND_WORKFLOW_TYPE, "manual_release");
        self.manual_releases.push(signal);
    }

    #[signal]
    pub fn acknowledge_unrecoverable(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: AcknowledgeUnrecoverableSignal,
    ) {
        maybe_record_signal(ctx, REFUND_WORKFLOW_TYPE, "acknowledge_unrecoverable");
        self.acknowledge_unrecoverables.push(signal);
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

    fn has_manual_resolution(&self) -> bool {
        !self.manual_releases.is_empty()
            || !self.manual_refund_triggers.is_empty()
            || !self.acknowledge_unrecoverables.is_empty()
    }

    fn pop_manual_resolution(&mut self) -> Option<ManualInterventionSignal> {
        if !self.manual_releases.is_empty() {
            return Some(ManualInterventionSignal::Release(
                self.manual_releases.remove(0),
            ));
        }
        if !self.manual_refund_triggers.is_empty() {
            return Some(ManualInterventionSignal::TriggerRefund(
                self.manual_refund_triggers.remove(0),
            ));
        }
        if !self.acknowledge_unrecoverables.is_empty() {
            return Some(ManualInterventionSignal::AcknowledgeUnrecoverable(
                self.acknowledge_unrecoverables.remove(0),
            ));
        }
        None
    }
}

enum RefundProviderCompletionWait {
    Completed,
    RefundManualInterventionRequired {
        resolution: RefundManualInterventionResolution,
    },
}

enum RefundManualInterventionResolution {
    Continue,
    Terminal(FinalizedOrder),
}

async fn finalize_refund_provider_hint_manual_intervention(
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
        event_name = "provider_operation_hint.refund_manual_intervention_required",
        "provider_operation_hint.refund_manual_intervention_required"
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
    let _finalized = ctx
        .start_activity(
            OrderActivities::finalize_order_or_refund,
            FinalizeOrderOrRefundInput {
                order_id,
                attempt_id: Some(attempt_id),
                step_id: Some(step_id),
                terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                reason: Some(reason),
                funded_to_workflow_start_seconds: None,
            },
            db_activity_options,
        )
        .await?;
    Ok(())
}

#[allow(clippy::never_loop)]
async fn wait_for_refund_manual_intervention_resolution(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: WorkflowOrderId,
    refund_attempt_id: Option<WorkflowAttemptId>,
    step_id: Option<WorkflowStepId>,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<RefundManualInterventionResolution> {
    let wait_started_at = manual_wait_started(ctx, REFUND_WORKFLOW_TYPE);
    let _paused = ctx
        .start_activity(
            OrderActivities::finalize_order_or_refund,
            FinalizeOrderOrRefundInput {
                order_id,
                attempt_id: refund_attempt_id,
                step_id,
                terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                reason: None,
                funded_to_workflow_start_seconds: None,
            },
            db_activity_options.clone(),
        )
        .await?;

    loop {
        temporalio_sdk::workflows::select! {
            _ = ctx.wait_condition(|state: &RefundWorkflow| state.has_manual_resolution()) => {
                let signal = ctx
                    .state_mut(|state| state.pop_manual_resolution())
                    .expect("refund manual-intervention condition was satisfied");
                match signal {
                    ManualInterventionSignal::Release(signal) => {
                        let _ = ctx
                            .start_activity(
                                OrderActivities::release_refund_manual_intervention,
                                ReleaseRefundManualInterventionInput {
                                    order_id,
                                    refund_attempt_id,
                                    step_id,
                                    signal_kind: ManualResolutionSignalKind::Release,
                                    reason: signal.reason,
                                    operator_id: signal.operator_id,
                                    requested_at: signal.requested_at,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        record_manual_wait_completed(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "release",
                        );
                        return Ok(RefundManualInterventionResolution::Continue);
                    }
                    ManualInterventionSignal::TriggerRefund(signal) => {
                        let _ = ctx
                            .start_activity(
                                OrderActivities::release_refund_manual_intervention,
                                ReleaseRefundManualInterventionInput {
                                    order_id,
                                    refund_attempt_id,
                                    step_id,
                                    signal_kind: ManualResolutionSignalKind::TriggerRefund,
                                    reason: signal.reason,
                                    operator_id: signal.operator_id,
                                    requested_at: signal.requested_at,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        record_manual_wait_completed(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "trigger_refund",
                        );
                        return Ok(RefundManualInterventionResolution::Continue);
                    }
                    ManualInterventionSignal::AcknowledgeUnrecoverable(signal) => {
                        let finalized = acknowledge_refund_manual_intervention_terminal(
                            ctx,
                            order_id,
                            refund_attempt_id,
                            step_id,
                            signal,
                            AcknowledgeReason::OperatorTerminal,
                            db_activity_options.clone(),
                        )
                        .await?;
                        record_manual_wait_completed(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "acknowledge_unrecoverable",
                        );
                        return Ok(RefundManualInterventionResolution::Terminal(finalized));
                    }
                }
            }
            _ = ctx.timer(MANUAL_INTERVENTION_WAIT_TIMEOUT) => {
                let finalized = acknowledge_refund_manual_intervention_terminal(
                    ctx,
                    order_id,
                    refund_attempt_id,
                    step_id,
                    zombie_cleanup_signal(),
                    AcknowledgeReason::ZombieCleanup,
                    db_activity_options.clone(),
                )
                .await?;
                record_manual_wait_completed(
                    ctx,
                    REFUND_WORKFLOW_TYPE,
                    wait_started_at,
                    "zombie_cleanup",
                );
                return Ok(RefundManualInterventionResolution::Terminal(finalized));
            }
        }
    }
}

async fn acknowledge_refund_manual_intervention_terminal(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: Option<WorkflowAttemptId>,
    step_id: Option<WorkflowStepId>,
    signal: AcknowledgeUnrecoverableSignal,
    reason: AcknowledgeReason,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    Ok(ctx
        .start_activity(
            OrderActivities::acknowledge_manual_intervention_terminal,
            AcknowledgeManualInterventionInput {
                order_id,
                attempt_id,
                step_id,
                scope: ManualInterventionScope::RefundAttempt,
                signal,
                reason,
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
            finalize_refund_provider_hint_manual_intervention(
                ctx,
                order_id,
                attempt_id,
                step_id,
                json!({
                    "reason": "provider_operation_hint_wait_timeout",
                    "step_id": step_id,
                }),
                db_activity_options.clone(),
            )
            .await?;
            let resolution = wait_for_refund_manual_intervention_resolution(
                ctx,
                order_id,
                Some(attempt_id),
                Some(step_id),
                db_activity_options.clone(),
            )
            .await?;
            record_provider_hint_wait(
                ctx,
                REFUND_WORKFLOW_TYPE,
                wait_started_at,
                "timeout_manual_intervention",
            );
            Ok(RefundProviderCompletionWait::RefundManualInterventionRequired { resolution })
        }
    }
}

enum RefundHintWaitOutcome {
    Accepted(WorkflowResult<()>),
    TimedOut,
}

