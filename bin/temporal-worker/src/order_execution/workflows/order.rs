use super::shared::*;
use super::*;

#[workflow]
pub struct OrderWorkflow {
    order_id: Option<WorkflowOrderId>,
    phase: OrderWorkflowPhase,
    active_attempt_id: Option<WorkflowAttemptId>,
    active_step_id: Option<WorkflowStepId>,
    provider_operation_hints: Vec<ProviderOperationHintSignal>,
    manual_releases: Vec<ManualReleaseSignal>,
    manual_refund_triggers: Vec<ManualTriggerRefundSignal>,
    acknowledge_unrecoverables: Vec<AcknowledgeUnrecoverableSignal>,
}

impl Default for OrderWorkflow {
    fn default() -> Self {
        Self {
            order_id: None,
            phase: OrderWorkflowPhase::WaitingForFunding,
            active_attempt_id: None,
            active_step_id: None,
            provider_operation_hints: Vec::new(),
            manual_releases: Vec::new(),
            manual_refund_triggers: Vec::new(),
            acknowledge_unrecoverables: Vec::new(),
        }
    }
}

#[workflow_methods]
impl OrderWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: OrderWorkflowInput,
    ) -> WorkflowResult<OrderWorkflowOutput> {
        ctx.state_mut(|state| {
            state.order_id = Some(input.order_id);
            state.phase = OrderWorkflowPhase::WaitingForFunding;
        });
        let workflow_started_at = workflow_start_time(ctx, ORDER_WORKFLOW_TYPE);
        let db_activity_options = db_activity_options();
        let execute_activity_options = execute_activity_options();

        let execution_state = ctx
            .start_activity(
                OrderActivities::load_order_execution_state,
                LoadOrderExecutionStateInput {
                    order_id: input.order_id,
                },
                db_activity_options.clone(),
            )
            .await?;

        let mut resumed_manual_attempt = None;
        if execution_state.order.status == RouterOrderStatus::ManualInterventionRequired {
            let context = ctx
                .start_activity(
                    OrderActivities::load_manual_intervention_context,
                    LoadManualInterventionContextInput {
                        order_id: input.order_id,
                        scope: ManualInterventionScope::OrderAttempt,
                    },
                    db_activity_options.clone(),
                )
                .await?;
            let attempt_id = context.attempt_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "manual-intervention order {} has no execution attempt context",
                    input.order_id
                )
            })?;
            let step_id = context.step_id.ok_or_else(|| {
                anyhow::anyhow!(
                    "manual-intervention order {} has no execution step context",
                    input.order_id
                )
            })?;
            match wait_for_manual_intervention_resolution(
                ctx,
                input.order_id,
                attempt_id,
                step_id,
                json_reason("workflow_started_in_manual_intervention", step_id),
                db_activity_options.clone(),
            )
            .await?
            {
                ManualInterventionResolution::Release => {
                    let execution_attempt = ctx
                        .start_activity(
                            OrderActivities::materialize_retry_attempt,
                            MaterializeRetryAttemptInput {
                                order_id: input.order_id,
                                failed_attempt_id: attempt_id,
                                failed_step_id: step_id,
                            },
                            db_activity_options.clone(),
                        )
                        .await?;
                    ctx.state_mut(|state| {
                        state.phase = OrderWorkflowPhase::Executing;
                        state.active_attempt_id = Some(execution_attempt.attempt_id);
                        state.active_step_id = None;
                    });
                    resumed_manual_attempt = Some(execution_attempt);
                }
                ManualInterventionResolution::TriggerRefund => {
                    ctx.state_mut(|state| {
                        state.phase = OrderWorkflowPhase::Refunding;
                        state.active_attempt_id = Some(attempt_id);
                        state.active_step_id = None;
                    });
                    let terminal_status = run_refund_child(ctx, input.order_id, attempt_id).await?;
                    return order_workflow_output(
                        ctx,
                        workflow_started_at,
                        input.order_id,
                        terminal_status,
                    );
                }
                ManualInterventionResolution::Terminal(finalized) => {
                    return order_workflow_output(
                        ctx,
                        workflow_started_at,
                        input.order_id,
                        finalized.terminal_status,
                    );
                }
            }
        }

        let mut execution_attempt = if let Some(execution_attempt) = resumed_manual_attempt {
            execution_attempt
        } else {
            ctx.start_activity(
                OrderActivities::materialize_execution_attempt,
                MaterializeExecutionAttemptInput {
                    order_id: input.order_id,
                    plan: execution_state.plan,
                },
                db_activity_options.clone(),
            )
            .await?
        };
        ctx.state_mut(|state| {
            state.phase = OrderWorkflowPhase::Executing;
            state.active_attempt_id = Some(execution_attempt.attempt_id);
        });

        'attempts: loop {
            for step in execution_attempt.steps.clone() {
                ctx.state_mut(|state| {
                    state.active_step_id = Some(step.step_id);
                });
                let _ready = ctx
                    .start_activity(
                        OrderActivities::persist_step_ready_to_fire,
                        PersistStepReadyToFireInput {
                            order_id: input.order_id,
                            attempt_id: execution_attempt.attempt_id,
                            step_id: step.step_id,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                let stale_quote_check = ctx
                    .start_activity(
                        OrderActivities::check_pre_execution_stale_quote,
                        CheckPreExecutionStaleQuoteInput {
                            order_id: input.order_id,
                            attempt_id: execution_attempt.attempt_id,
                            step_id: step.step_id,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                if stale_quote_check.should_refresh {
                    let reason = stale_quote_check.reason.unwrap_or_else(|| {
                        json_reason("pre_execution_stale_provider_quote", step.step_id)
                    });
                    let _failed = ctx
                        .start_activity(
                            OrderActivities::persist_step_failed,
                            PersistStepFailedInput {
                                order_id: input.order_id,
                                attempt_id: execution_attempt.attempt_id,
                                step_id: step.step_id,
                                failure_reason: reason.to_string(),
                            },
                            db_activity_options.clone(),
                        )
                        .await?;
                    let _snapshot = ctx
                        .start_activity(
                            OrderActivities::write_failed_attempt_snapshot,
                            WriteFailedAttemptSnapshotInput {
                                order_id: input.order_id,
                                attempt_id: execution_attempt.attempt_id,
                                failed_step_id: step.step_id,
                            },
                            db_activity_options.clone(),
                        )
                        .await?;
                    ctx.state_mut(|state| {
                        state.phase = OrderWorkflowPhase::RefreshingQuote;
                        state.active_step_id = Some(step.step_id);
                    });
                    let child = ctx
                        .child_workflow(
                            QuoteRefreshWorkflow::run,
                            QuoteRefreshWorkflowInput {
                                order_id: input.order_id,
                                stale_attempt_id: execution_attempt.attempt_id,
                                failed_step_id: step.step_id,
                            },
                            quote_refresh_child_options(input.order_id, step.step_id),
                        )
                        .await?;
                    let refreshed = child.result().await?;
                    match refreshed.outcome {
                        QuoteRefreshWorkflowOutcome::Refreshed { attempt_id, steps } => {
                            execution_attempt = MaterializedExecutionAttempt { attempt_id, steps };
                            ctx.state_mut(|state| {
                                state.phase = OrderWorkflowPhase::Executing;
                                state.active_attempt_id = Some(execution_attempt.attempt_id);
                                state.active_step_id = None;
                            });
                            continue 'attempts;
                        }
                        QuoteRefreshWorkflowOutcome::Untenable { .. } => {
                            ctx.state_mut(|state| {
                                state.phase = OrderWorkflowPhase::Finalizing;
                                state.active_step_id = None;
                            });
                            let finalized = ctx
                                .start_activity(
                                    OrderActivities::finalize_order_or_refund,
                                    FinalizeOrderOrRefundInput {
                                        order_id: input.order_id,
                                        attempt_id: None,
                                        step_id: None,
                                        terminal_status: OrderTerminalStatus::RefundRequired,
                                        reason: None,
                                    },
                                    db_activity_options.clone(),
                                )
                                .await?;
                            return order_workflow_output(
                                ctx,
                                workflow_started_at,
                                input.order_id,
                                finalized.terminal_status,
                            );
                        }
                    }
                }
                let executed = match execute_step_with_stale_running_timer(
                    ctx,
                    input.order_id,
                    execution_attempt.attempt_id,
                    step.step_id,
                    execute_activity_options.clone(),
                    db_activity_options.clone(),
                )
                .await?
                {
                    StepExecutionProgress::Executed(executed) => executed,
                    StepExecutionProgress::ActivityFailed { failure_reason } => {
                        let _failed = ctx
                            .start_activity(
                                OrderActivities::persist_step_failed,
                                PersistStepFailedInput {
                                    order_id: input.order_id,
                                    attempt_id: execution_attempt.attempt_id,
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
                                    attempt_id: execution_attempt.attempt_id,
                                    failed_step_id: step.step_id,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        let decision = ctx
                            .start_activity(
                                OrderActivities::classify_step_failure,
                                ClassifyStepFailureInput {
                                    order_id: input.order_id,
                                    attempt_id: execution_attempt.attempt_id,
                                    failed_step_id: step.step_id,
                                },
                                db_activity_options.clone(),
                            )
                            .await?;

                        match decision {
                            StepFailureDecision::RetryNewAttempt => {
                                execution_attempt = ctx
                                    .start_activity(
                                        OrderActivities::materialize_retry_attempt,
                                        MaterializeRetryAttemptInput {
                                            order_id: input.order_id,
                                            failed_attempt_id: execution_attempt.attempt_id,
                                            failed_step_id: step.step_id,
                                        },
                                        db_activity_options.clone(),
                                    )
                                    .await?;
                                ctx.state_mut(|state| {
                                    state.phase = OrderWorkflowPhase::Executing;
                                    state.active_attempt_id = Some(execution_attempt.attempt_id);
                                    state.active_step_id = None;
                                });
                                continue 'attempts;
                            }
                            StepFailureDecision::StartRefund => {
                                ctx.state_mut(|state| {
                                    state.phase = OrderWorkflowPhase::Refunding;
                                    state.active_step_id = None;
                                });
                                let terminal_status = run_refund_child(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                )
                                .await?;
                                return order_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    terminal_status,
                                );
                            }
                            StepFailureDecision::RefreshQuote => {
                                ctx.state_mut(|state| {
                                    state.phase = OrderWorkflowPhase::RefreshingQuote;
                                    state.active_step_id = Some(step.step_id);
                                });
                                let child = ctx
                                    .child_workflow(
                                        QuoteRefreshWorkflow::run,
                                        QuoteRefreshWorkflowInput {
                                            order_id: input.order_id,
                                            stale_attempt_id: execution_attempt.attempt_id,
                                            failed_step_id: step.step_id,
                                        },
                                        quote_refresh_child_options(input.order_id, step.step_id),
                                    )
                                    .await?;
                                let refreshed = child.result().await?;
                                match refreshed.outcome {
                                    QuoteRefreshWorkflowOutcome::Refreshed {
                                        attempt_id,
                                        steps,
                                    } => {
                                        execution_attempt =
                                            MaterializedExecutionAttempt { attempt_id, steps };
                                        ctx.state_mut(|state| {
                                            state.phase = OrderWorkflowPhase::Executing;
                                            state.active_attempt_id =
                                                Some(execution_attempt.attempt_id);
                                            state.active_step_id = None;
                                        });
                                        continue 'attempts;
                                    }
                                    QuoteRefreshWorkflowOutcome::Untenable { .. } => {
                                        ctx.state_mut(|state| {
                                            state.phase = OrderWorkflowPhase::Finalizing;
                                            state.active_step_id = None;
                                        });
                                        let finalized = ctx
                                            .start_activity(
                                                OrderActivities::finalize_order_or_refund,
                                                FinalizeOrderOrRefundInput {
                                                    order_id: input.order_id,
                                                    attempt_id: None,
                                                    step_id: None,
                                                    terminal_status:
                                                        OrderTerminalStatus::RefundRequired,
                                                    reason: None,
                                                },
                                                db_activity_options,
                                            )
                                            .await?;
                                        return order_workflow_output(
                                            ctx,
                                            workflow_started_at,
                                            input.order_id,
                                            finalized.terminal_status,
                                        );
                                    }
                                }
                            }
                            StepFailureDecision::ManualIntervention => {
                                match wait_for_manual_intervention_resolution(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                    step.step_id,
                                    json_reason(
                                        "execution_step_failure_manual_intervention",
                                        step.step_id,
                                    ),
                                    db_activity_options.clone(),
                                )
                                .await?
                                {
                                    ManualInterventionResolution::Release => {
                                        execution_attempt = ctx
                                            .start_activity(
                                                OrderActivities::materialize_retry_attempt,
                                                MaterializeRetryAttemptInput {
                                                    order_id: input.order_id,
                                                    failed_attempt_id: execution_attempt.attempt_id,
                                                    failed_step_id: step.step_id,
                                                },
                                                db_activity_options.clone(),
                                            )
                                            .await?;
                                        ctx.state_mut(|state| {
                                            state.phase = OrderWorkflowPhase::Executing;
                                            state.active_attempt_id =
                                                Some(execution_attempt.attempt_id);
                                            state.active_step_id = None;
                                        });
                                        continue 'attempts;
                                    }
                                    ManualInterventionResolution::TriggerRefund => {
                                        ctx.state_mut(|state| {
                                            state.phase = OrderWorkflowPhase::Refunding;
                                            state.active_step_id = None;
                                        });
                                        let terminal_status = run_refund_child(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        )
                                        .await?;
                                        return order_workflow_output(
                                            ctx,
                                            workflow_started_at,
                                            input.order_id,
                                            terminal_status,
                                        );
                                    }
                                    ManualInterventionResolution::Terminal(finalized) => {
                                        return order_workflow_output(
                                            ctx,
                                            workflow_started_at,
                                            input.order_id,
                                            finalized.terminal_status,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    StepExecutionProgress::ManualInterventionRequired { classified } => {
                        match wait_for_manual_intervention_resolution(
                            ctx,
                            input.order_id,
                            execution_attempt.attempt_id,
                            step.step_id,
                            classified.reason,
                            db_activity_options.clone(),
                        )
                        .await?
                        {
                            ManualInterventionResolution::Release => {
                                execution_attempt = ctx
                                    .start_activity(
                                        OrderActivities::materialize_retry_attempt,
                                        MaterializeRetryAttemptInput {
                                            order_id: input.order_id,
                                            failed_attempt_id: execution_attempt.attempt_id,
                                            failed_step_id: step.step_id,
                                        },
                                        db_activity_options.clone(),
                                    )
                                    .await?;
                                ctx.state_mut(|state| {
                                    state.phase = OrderWorkflowPhase::Executing;
                                    state.active_attempt_id = Some(execution_attempt.attempt_id);
                                    state.active_step_id = None;
                                });
                                continue 'attempts;
                            }
                            ManualInterventionResolution::TriggerRefund => {
                                ctx.state_mut(|state| {
                                    state.phase = OrderWorkflowPhase::Refunding;
                                    state.active_step_id = None;
                                });
                                let terminal_status = run_refund_child(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                )
                                .await?;
                                return order_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    terminal_status,
                                );
                            }
                            ManualInterventionResolution::Terminal(finalized) => {
                                return order_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    finalized.terminal_status,
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
                    match wait_for_provider_completion_hint(
                        ctx,
                        input.order_id,
                        step.step_id,
                        executed,
                        db_activity_options.clone(),
                    )
                    .await?
                    {
                        ProviderCompletionWait::Completed => {}
                        ProviderCompletionWait::ManualInterventionRequired { resolution } => {
                            match resolution {
                                ManualInterventionResolution::Release => {
                                    execution_attempt = ctx
                                        .start_activity(
                                            OrderActivities::materialize_retry_attempt,
                                            MaterializeRetryAttemptInput {
                                                order_id: input.order_id,
                                                failed_attempt_id: execution_attempt.attempt_id,
                                                failed_step_id: step.step_id,
                                            },
                                            db_activity_options.clone(),
                                        )
                                        .await?;
                                    ctx.state_mut(|state| {
                                        state.phase = OrderWorkflowPhase::Executing;
                                        state.active_attempt_id =
                                            Some(execution_attempt.attempt_id);
                                        state.active_step_id = None;
                                    });
                                    continue 'attempts;
                                }
                                ManualInterventionResolution::TriggerRefund => {
                                    ctx.state_mut(|state| {
                                        state.phase = OrderWorkflowPhase::Refunding;
                                        state.active_step_id = None;
                                    });
                                    let terminal_status = run_refund_child(
                                        ctx,
                                        input.order_id,
                                        execution_attempt.attempt_id,
                                    )
                                    .await?;
                                    return order_workflow_output(
                                        ctx,
                                        workflow_started_at,
                                        input.order_id,
                                        terminal_status,
                                    );
                                }
                                ManualInterventionResolution::Terminal(finalized) => {
                                    return order_workflow_output(
                                        ctx,
                                        workflow_started_at,
                                        input.order_id,
                                        finalized.terminal_status,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            break;
        }

        ctx.state_mut(|state| {
            state.phase = OrderWorkflowPhase::Finalizing;
            state.active_step_id = None;
        });
        let _completed = ctx
            .start_activity(
                OrderActivities::mark_order_completed,
                MarkOrderCompletedInput {
                    order_id: input.order_id,
                    attempt_id: execution_attempt.attempt_id,
                },
                db_activity_options,
            )
            .await?;

        // Child-workflow shape: RefundWorkflow, QuoteRefreshWorkflow,
        // ProviderHintPollWorkflow.
        order_workflow_output(
            ctx,
            workflow_started_at,
            input.order_id,
            OrderTerminalStatus::Completed,
        )
    }

    /// Scar tissue: §4 order/vault/step state alignment.
    #[signal]
    pub fn funding_vault_funded(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        _signal: FundingVaultFundedSignal,
    ) {
    }

    /// Scar tissue: §10 provider operation hint flow and verifier dispatch.
    #[signal]
    pub fn provider_operation_hint(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ProviderOperationHintSignal,
    ) {
        maybe_record_signal(ctx, ORDER_WORKFLOW_TYPE, "provider_operation_hint");
        if self
            .order_id
            .map_or(true, |order_id| order_id == signal.order_id)
        {
            self.provider_operation_hints.push(signal);
        }
    }

    /// Scar tissue: §3 phases 8-10 and §8 refund position discovery.
    #[signal]
    pub fn manual_refund_trigger(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ManualTriggerRefundSignal,
    ) {
        maybe_record_signal(ctx, ORDER_WORKFLOW_TYPE, "manual_trigger_refund");
        self.manual_refund_triggers.push(signal);
    }

    /// Scar tissue: §6 manual-intervention state. Exception: the release transport is a new
    /// Temporal signal surface, but it targets the manual-intervention state §6 requires.
    #[signal]
    pub fn manual_intervention_release(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: ManualReleaseSignal,
    ) {
        maybe_record_signal(ctx, ORDER_WORKFLOW_TYPE, "manual_release");
        self.manual_releases.push(signal);
    }

    /// Scar tissue: §6 manual-intervention state. This is the only operator signal that
    /// intentionally closes a manually paused workflow without further execution.
    #[signal]
    pub fn acknowledge_unrecoverable(
        &mut self,
        ctx: &mut SyncWorkflowContext<Self>,
        signal: AcknowledgeUnrecoverableSignal,
    ) {
        maybe_record_signal(ctx, ORDER_WORKFLOW_TYPE, "acknowledge_unrecoverable");
        self.acknowledge_unrecoverables.push(signal);
    }

    /// Scar tissue: §3 phase-pass visibility. Exception: this operator-only Temporal query is a
    /// new debug surface, replacing lease-worker pass introspection without changing business
    /// state.
    #[query]
    pub fn debug_cursor(&self, _ctx: &WorkflowContextView) -> OrderWorkflowDebugCursor {
        OrderWorkflowDebugCursor {
            order_id: self
                .order_id
                .unwrap_or_else(|| WorkflowOrderId::from(Uuid::nil())),
            phase: self.phase,
            active_attempt_id: self.active_attempt_id,
            active_step_id: self.active_step_id,
        }
    }
}

impl OrderWorkflow {
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

enum StepExecutionProgress {
    Executed(StepExecuted),
    ActivityFailed {
        failure_reason: String,
    },
    ManualInterventionRequired {
        classified: StaleRunningStepClassified,
    },
}

enum ProviderCompletionWait {
    Completed,
    ManualInterventionRequired {
        resolution: ManualInterventionResolution,
    },
}

enum ManualInterventionResolution {
    Release,
    TriggerRefund,
    Terminal(FinalizedOrder),
}

async fn execute_step_with_stale_running_timer(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    execute_activity_options: ActivityOptions,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<StepExecutionProgress> {
    let execute_future = ctx.start_activity(
        OrderActivities::execute_step,
        ExecuteStepInput {
            order_id,
            attempt_id,
            step_id,
        },
        execute_activity_options,
    );
    futures_util::pin_mut!(execute_future);

    loop {
        temporalio_sdk::workflows::select! {
            result = execute_future => {
                return Ok(match result {
                    Ok(executed) => StepExecutionProgress::Executed(executed),
                    Err(source) => StepExecutionProgress::ActivityFailed {
                        failure_reason: source.to_string(),
                    },
                });
            }
            _ = ctx.timer(STALE_RUNNING_STEP_RECOVERY_AFTER) => {
                let classified = ctx
                    .start_activity(
                        OrderActivities::classify_stale_running_step,
                        ClassifyStaleRunningStepInput {
                            order_id,
                            attempt_id,
                            step_id,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                match classified.decision {
                    StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress => {
                        tracing::info!(
                            order_id = %order_id,
                            attempt_id = %attempt_id,
                            step_id = %step_id,
                            event_name = "execution_step.stale_running_durable_progress",
                            "execution_step.stale_running_durable_progress"
                        );
                        continue;
                    }
                    StaleRunningStepDecision::AmbiguousExternalSideEffectWindow
                    | StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint => {
                        return Ok(StepExecutionProgress::ManualInterventionRequired {
                            classified,
                        });
                    }
                }
            }
        }
    }
}

async fn run_refund_child(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    parent_attempt_id: WorkflowAttemptId,
) -> WorkflowResult<OrderTerminalStatus> {
    let child = ctx
        .child_workflow(
            RefundWorkflow::run,
            RefundWorkflowInput {
                order_id,
                parent_attempt_id: Some(parent_attempt_id),
                trigger: RefundTrigger::FailedAttempt,
            },
            refund_child_options(order_id, parent_attempt_id),
        )
        .await?;
    let refunded = child.result().await?;
    Ok(match refunded.terminal_status {
        RefundTerminalStatus::Refunded => OrderTerminalStatus::Refunded,
        RefundTerminalStatus::RefundManualInterventionRequired => {
            OrderTerminalStatus::RefundManualInterventionRequired
        }
    })
}

async fn finalize_manual_intervention(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    reason: Value,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    Ok(ctx
        .start_activity(
            OrderActivities::finalize_order_or_refund,
            FinalizeOrderOrRefundInput {
                order_id,
                attempt_id: Some(attempt_id),
                step_id: Some(step_id),
                terminal_status: OrderTerminalStatus::ManualInterventionRequired,
                reason: Some(reason),
            },
            db_activity_options,
        )
        .await?)
}

async fn wait_for_manual_intervention_resolution(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    reason: Value,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<ManualInterventionResolution> {
    ctx.state_mut(|state| {
        state.phase = OrderWorkflowPhase::WaitingForManualIntervention;
        state.active_attempt_id = Some(attempt_id);
        state.active_step_id = Some(step_id);
    });
    let wait_started_at = manual_wait_started(ctx, ORDER_WORKFLOW_TYPE);
    let _paused = finalize_manual_intervention(
        ctx,
        order_id,
        attempt_id,
        step_id,
        reason,
        db_activity_options.clone(),
    )
    .await?;

    loop {
        temporalio_sdk::workflows::select! {
            _ = ctx.wait_condition(|state: &OrderWorkflow| state.has_manual_resolution()) => {
                let signal = ctx
                    .state_mut(|state| state.pop_manual_resolution())
                    .expect("manual-intervention condition was satisfied");
                match signal {
                    ManualInterventionSignal::Release(signal) => {
                        let _ = ctx
                            .start_activity(
                                OrderActivities::prepare_manual_intervention_retry,
                                PrepareManualInterventionRetryInput {
                                    order_id,
                                    attempt_id,
                                    step_id,
                                    signal: signal.clone(),
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        record_manual_wait_completed(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "release",
                        );
                        return Ok(ManualInterventionResolution::Release);
                    }
                    ManualInterventionSignal::TriggerRefund(signal) => {
                        let _ = ctx
                            .start_activity(
                                OrderActivities::prepare_manual_intervention_refund,
                                PrepareManualInterventionRefundInput {
                                    order_id,
                                    attempt_id,
                                    step_id,
                                    signal: signal.clone(),
                                },
                                db_activity_options.clone(),
                            )
                            .await?;
                        record_manual_wait_completed(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "trigger_refund",
                        );
                        return Ok(ManualInterventionResolution::TriggerRefund);
                    }
                    ManualInterventionSignal::AcknowledgeUnrecoverable(signal) => {
                        let finalized = acknowledge_manual_intervention_terminal(
                            ctx,
                            order_id,
                            Some(attempt_id),
                            Some(step_id),
                            ManualInterventionScope::OrderAttempt,
                            signal,
                            AcknowledgeReason::OperatorTerminal,
                            db_activity_options.clone(),
                        )
                        .await?;
                        record_manual_wait_completed(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "acknowledge_unrecoverable",
                        );
                        return Ok(ManualInterventionResolution::Terminal(finalized));
                    }
                }
            }
            _ = ctx.timer(MANUAL_INTERVENTION_WAIT_TIMEOUT) => {
                let finalized = acknowledge_manual_intervention_terminal(
                    ctx,
                    order_id,
                    Some(attempt_id),
                    Some(step_id),
                    ManualInterventionScope::OrderAttempt,
                    zombie_cleanup_signal(),
                    AcknowledgeReason::ZombieCleanup,
                    db_activity_options.clone(),
                )
                .await?;
                record_manual_wait_completed(
                    ctx,
                    ORDER_WORKFLOW_TYPE,
                    wait_started_at,
                    "zombie_cleanup",
                );
                return Ok(ManualInterventionResolution::Terminal(finalized));
            }
        }
    }
}

async fn acknowledge_manual_intervention_terminal(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: Option<WorkflowAttemptId>,
    step_id: Option<WorkflowStepId>,
    scope: ManualInterventionScope,
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
                scope,
                signal,
                reason,
            },
            db_activity_options,
        )
        .await?)
}

async fn settle_provider_completion(
    ctx: &mut WorkflowContext<OrderWorkflow>,
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

async fn wait_for_provider_completion_hint(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    step_id: WorkflowStepId,
    execution: StepExecuted,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<ProviderCompletionWait> {
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
            _ = ctx.wait_condition(move |state: &OrderWorkflow| state.has_provider_operation_hint(order_id)) => {
                let signal = ctx
                    .state_mut(|state| state.pop_provider_operation_hint(order_id))
                    .expect("provider operation hint condition was satisfied");
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
                        settle_provider_completion(ctx, execution.clone(), db_activity_options.clone()).await?;
                        record_provider_hint_wait(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "signal_accept",
                        );
                        return Ok(ProviderCompletionWait::Completed);
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
                        settle_provider_completion(ctx, execution.clone(), db_activity_options.clone()).await?;
                        record_provider_hint_wait(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "poll_accept",
                        );
                        return Ok(ProviderCompletionWait::Completed);
                    }
                    ProviderOperationHintDecision::Reject | ProviderOperationHintDecision::Defer => {
                        let reason = json!({
                            "reason": "provider_operation_hint_poll_unresolved",
                            "step_id": step_id,
                            "provider_operation_id": polled.provider_operation_id,
                            "decision": format!("{:?}", polled.decision),
                            "poll_reason": polled.reason,
                        });
                        let resolution = wait_for_manual_intervention_resolution(
                            ctx,
                            order_id,
                            execution.attempt_id,
                            step_id,
                            reason,
                            db_activity_options.clone(),
                        )
                        .await?;
                        record_provider_hint_wait(
                            ctx,
                            ORDER_WORKFLOW_TYPE,
                            wait_started_at,
                            "poll_unresolved_manual_intervention",
                        );
                        return Ok(ProviderCompletionWait::ManualInterventionRequired { resolution });
                    }
                }
            }
            _ = ctx.timer(PROVIDER_HINT_WAIT_TIMEOUT) => {
                let _ = ctx
                    .external_workflow(provider_hint_poll_workflow_id(order_id, step_id), None)
                    .cancel(Some("provider hint wait timed out".to_string()))
                    .await;
                poll_result.cancel();
                let resolution = wait_for_manual_intervention_resolution(
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
                record_provider_hint_wait(
                    ctx,
                    ORDER_WORKFLOW_TYPE,
                    wait_started_at,
                    "timeout_manual_intervention",
                );
                return Ok(ProviderCompletionWait::ManualInterventionRequired { resolution });
            }
        }
    }
}
