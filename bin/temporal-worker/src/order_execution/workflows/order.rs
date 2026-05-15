use super::shared::*;
use super::*;

#[workflow]
pub struct OrderWorkflow {
    state: OrderWorkflowState,
    provider_operation_hints: Vec<ProviderOperationHintSignal>,
    manual_releases: Vec<ManualReleaseSignal>,
    manual_refund_triggers: Vec<ManualTriggerRefundSignal>,
    acknowledge_unrecoverables: Vec<AcknowledgeUnrecoverableSignal>,
}

#[derive(Debug, Clone, Copy)]
enum OrderWorkflowState {
    NotStarted,
    WaitingForFunding(WaitingForFundingState),
    Executing(ExecutingState),
    RefreshingQuote(RefreshingQuoteState),
    Refunding(RefundingState),
    WaitingForManualIntervention(WaitingForManualInterventionState),
    Finalizing(FinalizingState),
}

#[derive(Debug, Clone, Copy)]
struct WaitingForFundingState {
    order_id: WorkflowOrderId,
}

#[derive(Debug, Clone, Copy)]
struct ExecutingState {
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    active_step_id: Option<WorkflowStepId>,
}

#[derive(Debug, Clone, Copy)]
struct RefreshingQuoteState {
    order_id: WorkflowOrderId,
    stale_attempt_id: WorkflowAttemptId,
    failed_step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Copy)]
struct RefundingState {
    order_id: WorkflowOrderId,
    parent_attempt_id: WorkflowAttemptId,
}

#[derive(Debug, Clone, Copy)]
struct WaitingForManualInterventionState {
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
}

#[derive(Debug, Clone, Copy)]
struct FinalizingState {
    order_id: WorkflowOrderId,
    terminal_status: Option<OrderTerminalStatus>,
}

enum QuoteRefreshContinuation {
    ResumeExecution(MaterializedExecutionAttempt),
    RefundRequired,
}

fn quote_refresh_continuation(outcome: QuoteRefreshWorkflowOutcome) -> QuoteRefreshContinuation {
    match outcome {
        QuoteRefreshWorkflowOutcome::Refreshed { attempt_id, steps } => {
            QuoteRefreshContinuation::ResumeExecution(MaterializedExecutionAttempt {
                attempt_id,
                steps,
            })
        }
        QuoteRefreshWorkflowOutcome::Untenable { .. } => QuoteRefreshContinuation::RefundRequired,
    }
}

impl OrderWorkflowState {
    fn waiting_for_funding(order_id: WorkflowOrderId) -> Self {
        Self::WaitingForFunding(WaitingForFundingState { order_id })
    }

    fn executing(order_id: WorkflowOrderId, attempt_id: WorkflowAttemptId) -> Self {
        Self::Executing(ExecutingState {
            order_id,
            attempt_id,
            active_step_id: None,
        })
    }

    fn refreshing_quote(
        order_id: WorkflowOrderId,
        stale_attempt_id: WorkflowAttemptId,
        failed_step_id: WorkflowStepId,
    ) -> Self {
        Self::RefreshingQuote(RefreshingQuoteState {
            order_id,
            stale_attempt_id,
            failed_step_id,
        })
    }

    fn refunding(order_id: WorkflowOrderId, parent_attempt_id: WorkflowAttemptId) -> Self {
        Self::Refunding(RefundingState {
            order_id,
            parent_attempt_id,
        })
    }

    fn waiting_for_manual_intervention(
        order_id: WorkflowOrderId,
        attempt_id: WorkflowAttemptId,
        step_id: WorkflowStepId,
    ) -> Self {
        Self::WaitingForManualIntervention(WaitingForManualInterventionState {
            order_id,
            attempt_id,
            step_id,
        })
    }

    fn finalizing(order_id: WorkflowOrderId, terminal_status: Option<OrderTerminalStatus>) -> Self {
        Self::Finalizing(FinalizingState {
            order_id,
            terminal_status,
        })
    }

    fn phase(&self) -> OrderWorkflowPhase {
        match self {
            Self::NotStarted | Self::WaitingForFunding(_) => OrderWorkflowPhase::WaitingForFunding,
            Self::Executing(_) => OrderWorkflowPhase::Executing,
            Self::RefreshingQuote(_) => OrderWorkflowPhase::RefreshingQuote,
            Self::Refunding(_) => OrderWorkflowPhase::Refunding,
            Self::WaitingForManualIntervention(_) => {
                OrderWorkflowPhase::WaitingForManualIntervention
            }
            Self::Finalizing(state) => {
                let _terminal_status = state.terminal_status;
                OrderWorkflowPhase::Finalizing
            }
        }
    }

    fn order_id(&self) -> Option<WorkflowOrderId> {
        match self {
            Self::NotStarted => None,
            Self::WaitingForFunding(state) => Some(state.order_id),
            Self::Executing(state) => Some(state.order_id),
            Self::RefreshingQuote(state) => Some(state.order_id),
            Self::Refunding(state) => Some(state.order_id),
            Self::WaitingForManualIntervention(state) => Some(state.order_id),
            Self::Finalizing(state) => Some(state.order_id),
        }
    }

    fn active_attempt_id(&self) -> Option<WorkflowAttemptId> {
        match self {
            Self::Executing(state) => Some(state.attempt_id),
            Self::RefreshingQuote(state) => Some(state.stale_attempt_id),
            Self::Refunding(state) => Some(state.parent_attempt_id),
            Self::WaitingForManualIntervention(state) => Some(state.attempt_id),
            Self::NotStarted | Self::WaitingForFunding(_) | Self::Finalizing(_) => None,
        }
    }

    fn active_step_id(&self) -> Option<WorkflowStepId> {
        match self {
            Self::Executing(state) => state.active_step_id,
            Self::RefreshingQuote(state) => Some(state.failed_step_id),
            Self::WaitingForManualIntervention(state) => Some(state.step_id),
            Self::NotStarted
            | Self::WaitingForFunding(_)
            | Self::Refunding(_)
            | Self::Finalizing(_) => None,
        }
    }

    fn set_active_step_id(&mut self, step_id: WorkflowStepId) {
        let Self::Executing(state) = self else {
            panic!("active execution step can only be set while executing");
        };
        state.active_step_id = Some(step_id);
    }
}

impl Default for OrderWorkflow {
    fn default() -> Self {
        Self {
            state: OrderWorkflowState::NotStarted,
            provider_operation_hints: Vec::new(),
            manual_releases: Vec::new(),
            manual_refund_triggers: Vec::new(),
            acknowledge_unrecoverables: Vec::new(),
        }
    }
}

fn set_order_workflow_executing(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
) {
    ctx.state_mut(|workflow| {
        workflow.state = OrderWorkflowState::executing(order_id, attempt_id);
    });
}

fn set_order_workflow_active_step(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    step_id: WorkflowStepId,
) {
    ctx.state_mut(|workflow| {
        workflow.state.set_active_step_id(step_id);
    });
}

fn set_order_workflow_refreshing_quote(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    stale_attempt_id: WorkflowAttemptId,
    failed_step_id: WorkflowStepId,
) {
    ctx.state_mut(|workflow| {
        workflow.state =
            OrderWorkflowState::refreshing_quote(order_id, stale_attempt_id, failed_step_id);
    });
}

fn set_order_workflow_refunding(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    parent_attempt_id: WorkflowAttemptId,
) {
    ctx.state_mut(|workflow| {
        workflow.state = OrderWorkflowState::refunding(order_id, parent_attempt_id);
    });
}

fn set_order_workflow_finalizing(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    terminal_status: Option<OrderTerminalStatus>,
) {
    ctx.state_mut(|workflow| {
        workflow.state = OrderWorkflowState::finalizing(order_id, terminal_status);
    });
}

#[workflow_methods]
impl OrderWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: OrderWorkflowInput,
    ) -> WorkflowResult<OrderWorkflowOutput> {
        ctx.state_mut(|state| {
            state.state = OrderWorkflowState::waiting_for_funding(input.order_id);
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
        let funded_to_workflow_start_seconds = execution_state.funded_to_workflow_start_seconds;

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
                funded_to_workflow_start_seconds,
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
                    set_order_workflow_executing(ctx, input.order_id, execution_attempt.attempt_id);
                    resumed_manual_attempt = Some(execution_attempt);
                }
                ManualInterventionResolution::TriggerRefund => {
                    set_order_workflow_refunding(ctx, input.order_id, attempt_id);
                    let terminal_status = run_refund_child(
                        ctx,
                        input.order_id,
                        attempt_id,
                        funded_to_workflow_start_seconds,
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
        set_order_workflow_executing(ctx, input.order_id, execution_attempt.attempt_id);

        'attempts: loop {
            for step in execution_attempt.steps.clone() {
                set_order_workflow_active_step(ctx, step.step_id);
                let dispatched = match dispatch_step_with_stale_running_timer(
                    ctx,
                    input.order_id,
                    execution_attempt.attempt_id,
                    &step,
                    execute_activity_options.clone(),
                    db_activity_options.clone(),
                )
                .await?
                {
                    StepExecutionProgress::Executed(dispatched) => dispatched,
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
                                set_order_workflow_executing(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                continue 'attempts;
                            }
                            StepFailureDecision::StartRefund => {
                                set_order_workflow_refunding(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                let terminal_status = run_refund_child(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                    funded_to_workflow_start_seconds,
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
                                set_order_workflow_refreshing_quote(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                    step.step_id,
                                );
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
                                match quote_refresh_continuation(refreshed.outcome) {
                                    QuoteRefreshContinuation::ResumeExecution(
                                        refreshed_attempt,
                                    ) => {
                                        execution_attempt = refreshed_attempt;
                                        set_order_workflow_executing(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        continue 'attempts;
                                    }
                                    QuoteRefreshContinuation::RefundRequired => {
                                        set_order_workflow_refunding(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        let terminal_status = run_refund_child(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                            funded_to_workflow_start_seconds,
                                        )
                                        .await?;
                                        return order_workflow_output(
                                            ctx,
                                            workflow_started_at,
                                            input.order_id,
                                            terminal_status,
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
                                    funded_to_workflow_start_seconds,
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
                                        set_order_workflow_executing(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        continue 'attempts;
                                    }
                                    ManualInterventionResolution::TriggerRefund => {
                                        set_order_workflow_refunding(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        let terminal_status = run_refund_child(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                            funded_to_workflow_start_seconds,
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
                            funded_to_workflow_start_seconds,
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
                                set_order_workflow_executing(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                continue 'attempts;
                            }
                            ManualInterventionResolution::TriggerRefund => {
                                set_order_workflow_refunding(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                let terminal_status = run_refund_child(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                    funded_to_workflow_start_seconds,
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
                match dispatched.outcome {
                    DispatchOutcome::Completed => {}
                    DispatchOutcome::Waiting => {
                        match wait_for_provider_completion_hint(
                            ctx,
                            input.order_id,
                            execution_attempt.attempt_id,
                            step.step_id,
                            db_activity_options.clone(),
                            funded_to_workflow_start_seconds,
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
                                        set_order_workflow_executing(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        continue 'attempts;
                                    }
                                    ManualInterventionResolution::TriggerRefund => {
                                        set_order_workflow_refunding(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        );
                                        let terminal_status = run_refund_child(
                                            ctx,
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                            funded_to_workflow_start_seconds,
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
                    DispatchOutcome::StaleQuoteDetected {
                        should_refund,
                        reason,
                    } => {
                        let reason = reason.unwrap_or_else(|| {
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
                        if should_refund {
                            set_order_workflow_refunding(
                                ctx,
                                input.order_id,
                                execution_attempt.attempt_id,
                            );
                            let terminal_status = run_refund_child(
                                ctx,
                                input.order_id,
                                execution_attempt.attempt_id,
                                funded_to_workflow_start_seconds,
                            )
                            .await?;
                            return order_workflow_output(
                                ctx,
                                workflow_started_at,
                                input.order_id,
                                terminal_status,
                            );
                        }
                        set_order_workflow_refreshing_quote(
                            ctx,
                            input.order_id,
                            execution_attempt.attempt_id,
                            step.step_id,
                        );
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
                        match quote_refresh_continuation(refreshed.outcome) {
                            QuoteRefreshContinuation::ResumeExecution(refreshed_attempt) => {
                                execution_attempt = refreshed_attempt;
                                set_order_workflow_executing(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                continue 'attempts;
                            }
                            QuoteRefreshContinuation::RefundRequired => {
                                set_order_workflow_refunding(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                );
                                let terminal_status = run_refund_child(
                                    ctx,
                                    input.order_id,
                                    execution_attempt.attempt_id,
                                    funded_to_workflow_start_seconds,
                                )
                                .await?;
                                return order_workflow_output(
                                    ctx,
                                    workflow_started_at,
                                    input.order_id,
                                    terminal_status,
                                );
                            }
                        }
                    }
                }
            }
            break;
        }

        set_order_workflow_finalizing(ctx, input.order_id, Some(OrderTerminalStatus::Completed));
        let _completed = ctx
            .start_activity(
                OrderActivities::mark_order_completed,
                MarkOrderCompletedInput {
                    order_id: input.order_id,
                    attempt_id: execution_attempt.attempt_id,
                    funded_to_workflow_start_seconds,
                },
                db_activity_options,
            )
            .await?;

        // Child-workflow shape: RefundWorkflow and QuoteRefreshWorkflow.
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
            .state
            .order_id()
            .is_none_or(|order_id| order_id == signal.order_id)
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
                .state
                .order_id()
                .unwrap_or_else(|| WorkflowOrderId::from(Uuid::nil())),
            phase: self.state.phase(),
            active_attempt_id: self.state.active_attempt_id(),
            active_step_id: self.state.active_step_id(),
        }
    }
}

impl OrderWorkflow {
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

#[allow(clippy::large_enum_variant)]
enum StepExecutionProgress {
    Executed(StepDispatched),
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

/// Fires the venue-specific `dispatch_<step_type>_step` activity for the given
/// step, racing it against a stale-running timer that handles the case where a
/// dispatch activity hangs past `STALE_RUNNING_STEP_RECOVERY_AFTER`. The
/// venue-specific activity name is what surfaces in Temporal UI.
/// Static-dispatch from `step_type` to the matching `dispatch_<step_type>_step`
/// activity. Used by both `OrderWorkflow` and `RefundWorkflow` per-step loops.
///
/// We use a macro (not a helper that returns a single boxed future) because:
///
/// 1. `WorkflowContext::start_activity` returns a different opaque
///    `impl CancellableFuture` type per activity definition; the match arms
///    must each box to a single uniform type before unifying.
/// 2. Each boxed future borrows `&mut ctx`. Lifting the match into a helper
///    would extend that borrow past the `select!` arm that needs `&ctx` for
///    the stale-running timer; macro expansion at the call site keeps the
///    borrow scoped to the local future binding.
///
/// The macro therefore boxes each arm and returns a `LocalBoxFuture`. We use
/// `LocalBoxFuture` (not `BoxFuture`) because Temporal's workflow context is
/// `!Send` (it carries an `Rc`).
#[macro_export]
macro_rules! dispatch_step_activity {
    ($ctx:expr, $step_type:expr, $input:expr, $options:expr) => {{
        use $crate::order_execution::activities::OrderActivities;
        use $crate::order_execution::types::OrderExecutionStepType;
        use ::futures_util::FutureExt;
        let __dispatch_future: ::futures_util::future::LocalBoxFuture<
            '_,
            ::std::result::Result<
                $crate::order_execution::types::StepDispatched,
                ::temporalio_sdk::ActivityExecutionError,
            >,
        > = match $step_type {
            OrderExecutionStepType::AcrossBridge => $ctx
                .start_activity(OrderActivities::dispatch_across_bridge_step, $input, $options)
                .boxed_local(),
            OrderExecutionStepType::CctpBurn => $ctx
                .start_activity(OrderActivities::dispatch_cctp_burn_step, $input, $options)
                .boxed_local(),
            OrderExecutionStepType::CctpReceive => $ctx
                .start_activity(OrderActivities::dispatch_cctp_receive_step, $input, $options)
                .boxed_local(),
            OrderExecutionStepType::HyperliquidBridgeDeposit => $ctx
                .start_activity(
                    OrderActivities::dispatch_hyperliquid_bridge_deposit_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::HyperliquidBridgeWithdrawal => $ctx
                .start_activity(
                    OrderActivities::dispatch_hyperliquid_bridge_withdrawal_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::HyperliquidClearinghouseToSpot => $ctx
                .start_activity(
                    OrderActivities::dispatch_hyperliquid_clearinghouse_to_spot_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::HyperliquidLimitOrder => $ctx
                .start_activity(
                    OrderActivities::dispatch_hyperliquid_limit_order_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::HyperliquidTrade => $ctx
                .start_activity(
                    OrderActivities::dispatch_hyperliquid_trade_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::UnitDeposit => $ctx
                .start_activity(OrderActivities::dispatch_unit_deposit_step, $input, $options)
                .boxed_local(),
            OrderExecutionStepType::UnitWithdrawal => $ctx
                .start_activity(
                    OrderActivities::dispatch_unit_withdrawal_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::UniversalRouterSwap => $ctx
                .start_activity(
                    OrderActivities::dispatch_universal_router_swap_step,
                    $input,
                    $options,
                )
                .boxed_local(),
            OrderExecutionStepType::Refund => $ctx
                .start_activity(OrderActivities::dispatch_refund_step, $input, $options)
                .boxed_local(),
            // wait_for_deposit is not provider-executable; the planner never
            // produces it in an executable plan. Reuse the refund arm as a
            // sentinel — if a WaitForDeposit step ever reaches dispatch the
            // activity-side invariant check (`run_step_dispatch`'s
            // `dispatch_activity_matches_step_type`) will surface the bug.
            OrderExecutionStepType::WaitForDeposit => $ctx
                .start_activity(OrderActivities::dispatch_refund_step, $input, $options)
                .boxed_local(),
        };
        __dispatch_future
    }};
}

/// Same as [`dispatch_step_activity!`] but for the verify-hint side.
#[macro_export]
macro_rules! verify_hint_activity {
    ($ctx:expr, $hint_kind:expr, $input:expr, $options:expr) => {{
        use $crate::order_execution::activities::ProviderObservationActivities;
        use $crate::order_execution::types::ProviderHintKind;
        use ::futures_util::FutureExt;
        let __verify_future: ::futures_util::future::LocalBoxFuture<
            '_,
            ::std::result::Result<
                $crate::order_execution::types::ProviderOperationHintVerified,
                ::temporalio_sdk::ActivityExecutionError,
            >,
        > = match $hint_kind {
            ProviderHintKind::AcrossFill | ProviderHintKind::AcrossDestinationFilled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_across_fill_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::CctpAttestation => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_cctp_attestation_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::CctpReceiveObserved => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_cctp_receive_observed_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::BtcDepositObserved => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_btc_deposit_observed_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::UnitDeposit => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_unit_deposit_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HyperUnitDepositCredited => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hyperunit_deposit_credited_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HyperUnitWithdrawalAcknowledged => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hyperunit_withdrawal_acknowledged_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HyperUnitWithdrawalSettled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hyperunit_withdrawal_settled_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::ProviderObservation => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_provider_observation_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::VeloraSwapSettled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_velora_swap_settled_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HyperliquidTrade => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hyperliquid_trade_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlTradeFilled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_trade_filled_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlTradeCanceled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_trade_canceled_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlBridgeDepositObserved => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_bridge_deposit_observed_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlBridgeDepositCredited => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_bridge_deposit_credited_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlWithdrawalAcknowledged => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_withdrawal_acknowledged_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
            ProviderHintKind::HlWithdrawalSettled => $ctx
                .start_activity(
                    ProviderObservationActivities::verify_hl_withdrawal_settled_hint,
                    $input,
                    $options,
                )
                .boxed_local(),
        };
        __verify_future
    }};
}

async fn dispatch_step_with_stale_running_timer(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step: &WorkflowExecutionStep,
    execute_activity_options: ActivityOptions,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<StepExecutionProgress> {
    let dispatch_input = DispatchStepInput {
        order_id,
        attempt_id,
        step_id: step.step_id,
    };
    use futures_util::FutureExt;
    let dispatch_future = crate::dispatch_step_activity!(
        ctx,
        step.step_type,
        dispatch_input,
        execute_activity_options
    )
    .fuse();
    futures_util::pin_mut!(dispatch_future);

    loop {
        temporalio_sdk::workflows::select! {
            result = dispatch_future => {
                return Ok(match result {
                    Ok(dispatched) => StepExecutionProgress::Executed(dispatched),
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
                            step_id: step.step_id,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
                match classified.decision {
                    StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress => {
                        tracing::info!(
                            order_id = %order_id,
                            attempt_id = %attempt_id,
                            step_id = %step.step_id,
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
    funded_to_workflow_start_seconds: Option<f64>,
) -> WorkflowResult<OrderTerminalStatus> {
    let child = ctx
        .child_workflow(
            RefundWorkflow::run,
            RefundWorkflowInput {
                order_id,
                parent_attempt_id: Some(parent_attempt_id),
                trigger: RefundTrigger::FailedAttempt,
                funded_to_workflow_start_seconds,
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
    funded_to_workflow_start_seconds: Option<f64>,
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
                funded_to_workflow_start_seconds,
            },
            db_activity_options,
        )
        .await?)
}

#[allow(clippy::never_loop)]
async fn wait_for_manual_intervention_resolution(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    reason: Value,
    db_activity_options: ActivityOptions,
    funded_to_workflow_start_seconds: Option<f64>,
) -> WorkflowResult<ManualInterventionResolution> {
    ctx.state_mut(|workflow| {
        workflow.state =
            OrderWorkflowState::waiting_for_manual_intervention(order_id, attempt_id, step_id);
    });
    let wait_started_at = manual_wait_started(ctx, ORDER_WORKFLOW_TYPE);
    let _paused = finalize_manual_intervention(
        ctx,
        order_id,
        attempt_id,
        step_id,
        reason,
        db_activity_options.clone(),
        funded_to_workflow_start_seconds,
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

#[allow(clippy::too_many_arguments)]
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

async fn wait_for_provider_completion_hint(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: WorkflowOrderId,
    attempt_id: WorkflowAttemptId,
    step_id: WorkflowStepId,
    db_activity_options: ActivityOptions,
    funded_to_workflow_start_seconds: Option<f64>,
) -> WorkflowResult<ProviderCompletionWait> {
    let wait_started_at = provider_hint_wait_started(ctx);

    loop {
        temporalio_sdk::workflows::select! {
            _ = ctx.wait_condition(move |state: &OrderWorkflow| state.has_provider_operation_hint(order_id, step_id)) => {
                let signal = ctx
                    .state_mut(|state| state.pop_provider_operation_hint(order_id, step_id))
                    .expect("provider operation hint condition was satisfied");
                let hint_kind = signal.hint_kind;
                let verify_input = VerifyProviderOperationHintInput {
                    order_id,
                    attempt_id,
                    step_id,
                    signal,
                };
                let verified = crate::verify_hint_activity!(
                    ctx,
                    hint_kind,
                    verify_input,
                    db_activity_options.clone()
                )
                .await?;

                match verified.decision {
                    ProviderOperationHintDecision::Accept => {
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
                            hint_kind = ?hint_kind,
                            provider_operation_id = ?verified.provider_operation_id,
                            decision = ?verified.decision,
                            reason = ?verified.reason,
                            event_name = "provider_operation_hint.ignored",
                            "provider_operation_hint.ignored"
                        );
                    }
                }
            }
            _ = ctx.timer(PROVIDER_HINT_WAIT_TIMEOUT) => {
                let resolution = wait_for_manual_intervention_resolution(
                    ctx,
                    order_id,
                    attempt_id,
                    step_id,
                    json!({
                        "reason": "provider_operation_hint_wait_timeout",
                        "step_id": step_id,
                    }),
                    db_activity_options.clone(),
                    funded_to_workflow_start_seconds,
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::order_execution::types::WorkflowExecutionStep;

    fn workflow_order_id(value: u128) -> WorkflowOrderId {
        WorkflowOrderId::from(Uuid::from_u128(value))
    }

    fn workflow_attempt_id(value: u128) -> WorkflowAttemptId {
        WorkflowAttemptId::from(Uuid::from_u128(value))
    }

    fn workflow_step_id(value: u128) -> WorkflowStepId {
        WorkflowStepId::from(Uuid::from_u128(value))
    }

    #[test]
    fn stale_running_step_recovery_timer_absorbs_dispatch_lag() {
        assert!(super::super::STALE_RUNNING_STEP_RECOVERY_AFTER >= Duration::from_secs(15 * 60));
    }

    #[test]
    fn waiting_for_funding_state_has_no_execution_cursor() {
        let state = OrderWorkflowState::waiting_for_funding(workflow_order_id(1));

        assert_eq!(state.phase(), OrderWorkflowPhase::WaitingForFunding);
        assert_eq!(state.active_attempt_id(), None);
        assert_eq!(state.active_step_id(), None);

        // WaitingForFundingState has no attempt_id or step_id fields; code that needs those
        // values must first match or transition into an execution-bearing state.
    }

    #[test]
    fn executing_state_owns_execution_cursor() {
        let attempt_id = workflow_attempt_id(2);
        let step_id = workflow_step_id(3);
        let mut state = OrderWorkflowState::executing(workflow_order_id(1), attempt_id);

        state.set_active_step_id(step_id);

        assert_eq!(state.phase(), OrderWorkflowPhase::Executing);
        assert_eq!(state.active_attempt_id(), Some(attempt_id));
        assert_eq!(state.active_step_id(), Some(step_id));
    }

    #[test]
    fn refreshed_quote_outcome_resumes_execution() {
        let attempt_id = workflow_attempt_id(2);
        let step_id = workflow_step_id(3);
        let continuation = quote_refresh_continuation(QuoteRefreshWorkflowOutcome::Refreshed {
            attempt_id,
            steps: vec![WorkflowExecutionStep {
                step_id,
                step_index: 0,
                step_type: router_core::models::OrderExecutionStepType::AcrossBridge,
            }],
        });

        match continuation {
            QuoteRefreshContinuation::ResumeExecution(attempt) => {
                assert_eq!(attempt.attempt_id, attempt_id);
                assert_eq!(attempt.steps.len(), 1);
                assert_eq!(attempt.steps[0].step_id, step_id);
            }
            QuoteRefreshContinuation::RefundRequired => {
                panic!("refreshed quote continuation should resume execution")
            }
        }
    }

    #[test]
    fn refund_required_workflow_paths_start_refund_child() {
        let source = workflow_implementation_source();
        assert!(
            !source.contains("OrderTerminalStatus::RefundRequired"),
            "OrderWorkflow must not finalize or output RefundRequired directly"
        );

        // The consolidated dispatch surfaces a stale quote as
        // `DispatchOutcome::StaleQuoteDetected { should_refund, .. }`. The
        // refund branch is now keyed off `if should_refund {` inside that arm.
        let pre_execution_branch = branch_window(source, "if should_refund {", 0, 24);
        assert_refund_child_branch("pre-execution stale quote refund", &pre_execution_branch);

        let quote_refresh_branches =
            branch_windows(source, "QuoteRefreshContinuation::RefundRequired => {", 24);
        assert_eq!(
            quote_refresh_branches.len(),
            2,
            "expected both quote-refresh RefundRequired workflow branches"
        );
        for (index, branch) in quote_refresh_branches.iter().enumerate() {
            assert_refund_child_branch(
                &format!("quote-refresh refund branch {}", index + 1),
                branch,
            );
        }
    }

    fn workflow_implementation_source() -> &'static str {
        include_str!("order.rs")
            .split_once("#[cfg(test)]")
            .map(|(source, _tests)| source)
            .expect("order workflow source should contain cfg(test) module")
    }

    fn branch_windows(source: &str, needle: &str, line_count: usize) -> Vec<String> {
        let lines = source.lines().collect::<Vec<_>>();
        lines
            .iter()
            .enumerate()
            .filter(|(_, line)| line.contains(needle))
            .map(|(index, _)| lines[index..usize::min(index + line_count, lines.len())].join("\n"))
            .collect()
    }

    fn branch_window(
        source: &str,
        needle: &str,
        occurrence_index: usize,
        line_count: usize,
    ) -> String {
        branch_windows(source, needle, line_count)
            .into_iter()
            .nth(occurrence_index)
            .unwrap_or_else(|| {
                panic!("missing occurrence {occurrence_index} for workflow branch {needle}")
            })
    }

    fn assert_refund_child_branch(label: &str, branch: &str) {
        assert!(
            branch.contains("set_order_workflow_refunding("),
            "{label} should set the workflow state to refunding"
        );
        assert!(
            branch.contains("run_refund_child("),
            "{label} should invoke the refund child workflow"
        );
        assert!(
            branch.contains("execution_attempt.attempt_id"),
            "{label} should chain the refund to the failed execution attempt"
        );
        assert!(
            !branch.contains("set_order_workflow_finalizing("),
            "{label} must not finalize directly"
        );
        assert!(
            !branch.contains("finalize_order_or_refund"),
            "{label} must not mark RefundRequired without attempting refund"
        );
    }
}
