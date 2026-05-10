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
                let _ready = ctx
                    .start_activity(
                        OrderActivities::persist_step_ready_to_fire,
                        PersistStepReadyToFireInput {
                            order_id: input.order_id,
                            attempt_id: refund_attempt_id,
                            step_id: step.step_id,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                let executed = match ctx
                    .start_activity(
                        OrderActivities::execute_step,
                        ExecuteStepInput {
                            order_id: input.order_id,
                            attempt_id: refund_attempt_id,
                            step_id: step.step_id,
                        },
                        refund_execute_activity_options.clone(),
                    )
                    .await
                {
                    Ok(executed) => executed,
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
                let _receipt = ctx
                    .start_activity(
                        OrderActivities::persist_provider_receipt,
                        PersistProviderReceiptInput {
                            execution: executed.clone(),
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                let _provider_status = ctx
                    .start_activity(
                        OrderActivities::persist_provider_operation_status,
                        PersistProviderOperationStatusInput {
                            execution: executed.clone(),
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                let _settled = ctx
                    .start_activity(
                        OrderActivities::settle_provider_step,
                        SettleProviderStepInput {
                            execution: executed.clone(),
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                if executed.outcome == StepExecutionOutcome::Waiting {
                    match wait_for_refund_provider_completion_hint(
                        ctx,
                        input.order_id,
                        step.step_id,
                        executed,
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
            .map_or(true, |order_id| order_id == signal.order_id)
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
    fn has_provider_operation_hint(&self, order_id: WorkflowOrderId) -> bool {
        self.provider_operation_hints
            .iter()
            .any(|signal| signal.order_id == order_id)
    }

    fn pop_provider_operation_hint(
        &mut self,
        order_id: WorkflowOrderId,
    ) -> Option<ProviderOperationHintSignal> {
        let index = self
            .provider_operation_hints
            .iter()
            .position(|signal| signal.order_id == order_id)?;
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

/// Quote-refresh child workflow.

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

async fn settle_refund_provider_completion(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    execution: StepExecuted,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<()> {
    let _settled = ctx
        .start_activity(
            OrderActivities::settle_provider_step,
            SettleProviderStepInput { execution },
            db_activity_options,
        )
        .await?;
    Ok(())
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
    step_id: WorkflowStepId,
    execution: StepExecuted,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<RefundProviderCompletionWait> {
    let wait_started_at = provider_hint_wait_started(ctx);
    let poll_child = ctx
        .child_workflow(
            ProviderHintPollWorkflow::run,
            ProviderHintPollWorkflowInput { order_id, step_id },
            provider_hint_poll_child_options(order_id, step_id),
        )
        .await?;
    let poll_result = poll_child.result();
    futures_util::pin_mut!(poll_result);

    loop {
        temporalio_sdk::workflows::select! {
            _ = ctx.wait_condition(move |state: &RefundWorkflow| state.has_provider_operation_hint(order_id)) => {
                let signal = ctx
                    .state_mut(|state| state.pop_provider_operation_hint(order_id))
                    .expect("refund provider operation hint condition was satisfied");
                let verified = ctx
                    .start_activity(
                        ProviderObservationActivities::verify_provider_operation_hint,
                        VerifyProviderOperationHintInput {
                            order_id,
                            step_id,
                            signal,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;

                match verified.decision {
                    ProviderOperationHintDecision::Accept => {
                        let _ = ctx
                            .external_workflow(provider_hint_poll_workflow_id(order_id, step_id), None)
                            .cancel(Some("provider operation hint accepted by signal".to_string()))
                            .await;
                        poll_result.cancel();
                        settle_refund_provider_completion(ctx, execution.clone(), db_activity_options.clone()).await?;
                        record_provider_hint_wait(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "signal_accept",
                        );
                        return Ok(RefundProviderCompletionWait::Completed);
                    }
                    ProviderOperationHintDecision::Reject | ProviderOperationHintDecision::Defer => {
                        tracing::info!(
                            order_id = %order_id,
                            step_id = %step_id,
                            provider_operation_id = ?verified.provider_operation_id,
                            decision = ?verified.decision,
                            reason = ?verified.reason,
                            event_name = "provider_operation_hint.ignored",
                            "provider_operation_hint.ignored"
                        );
                    }
                }
            }
            polled = poll_result => {
                let polled = polled?;
                match polled.decision {
                    ProviderOperationHintDecision::Accept => {
                        settle_refund_provider_completion(ctx, execution.clone(), db_activity_options.clone()).await?;
                        record_provider_hint_wait(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "poll_accept",
                        );
                        return Ok(RefundProviderCompletionWait::Completed);
                    }
                    ProviderOperationHintDecision::Reject | ProviderOperationHintDecision::Defer => {
                        finalize_refund_provider_hint_manual_intervention(
                            ctx,
                            order_id,
                            execution.attempt_id,
                            step_id,
                            json!({
                                "reason": "provider_operation_hint_poll_unresolved",
                                "step_id": step_id,
                                "provider_operation_id": polled.provider_operation_id,
                                "decision": format!("{:?}", polled.decision),
                                "poll_reason": polled.reason,
                            }),
                            db_activity_options.clone(),
                        )
                        .await?;
                        let resolution = wait_for_refund_manual_intervention_resolution(
                            ctx,
                            order_id,
                            Some(execution.attempt_id),
                            Some(step_id),
                            db_activity_options.clone(),
                        )
                        .await?;
                        record_provider_hint_wait(
                            ctx,
                            REFUND_WORKFLOW_TYPE,
                            wait_started_at,
                            "poll_unresolved_manual_intervention",
                        );
                        return Ok(RefundProviderCompletionWait::RefundManualInterventionRequired { resolution });
                    }
                }
            }
            _ = ctx.timer(PROVIDER_HINT_WAIT_TIMEOUT) => {
                let _ = ctx
                    .external_workflow(provider_hint_poll_workflow_id(order_id, step_id), None)
                    .cancel(Some("provider hint wait timed out".to_string()))
                    .await;
                poll_result.cancel();
                finalize_refund_provider_hint_manual_intervention(
                    ctx,
                    order_id,
                    execution.attempt_id,
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
                    Some(execution.attempt_id),
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
                return Ok(RefundProviderCompletionWait::RefundManualInterventionRequired { resolution });
            }
        }
    }
}
