use std::time::Duration;

use serde_json::{json, Value};
use temporalio_common::protos::{
    coresdk::child_workflow::ChildWorkflowCancellationType,
    temporal::api::{common::v1::RetryPolicy, enums::v1::ParentClosePolicy},
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};
use uuid::Uuid;

use super::activities::{
    OrderActivities, ProviderObservationActivities, QuoteRefreshActivities, RefundActivities,
};
use super::refund_workflow_id;
use super::types::{
    CheckPreExecutionStaleQuoteInput, ClassifyStaleRunningStepInput, ClassifyStepFailureInput,
    ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput, ExecuteStepInput,
    FinalizeOrderOrRefundInput, FinalizedOrder, FundingVaultFundedSignal,
    LoadOrderExecutionStateInput, ManualInterventionReleaseSignal, ManualRefundTriggerSignal,
    MarkOrderCompletedInput, MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
    MaterializeRefundPlanInput, MaterializeRetryAttemptInput, OrderTerminalStatus,
    OrderWorkflowDebugCursor, OrderWorkflowInput, OrderWorkflowOutput, OrderWorkflowPhase,
    PersistProviderOperationStatusInput, PersistProviderReceiptInput, PersistStepFailedInput,
    PersistStepReadyToFireInput, PollProviderOperationHintsInput, ProviderHintPollWorkflowInput,
    ProviderHintPollWorkflowOutput, ProviderOperationHintDecision, ProviderOperationHintSignal,
    QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutcome, QuoteRefreshWorkflowOutput,
    RefreshedQuoteAttemptOutcome, RefundPlanOutcome, RefundTerminalStatus, RefundTrigger,
    RefundWorkflowInput, RefundWorkflowOutput, SettleProviderStepInput,
    SingleRefundPositionOutcome, StaleRunningStepClassified, StaleRunningStepDecision,
    StaleRunningStepWatchdogInput, StaleRunningStepWatchdogOutput, StepExecuted,
    StepExecutionOutcome, StepFailureDecision, VerifyProviderOperationHintInput,
    WriteFailedAttemptSnapshotInput,
};

const PROVIDER_HINT_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const PROVIDER_HINT_POLL_INTERVAL: Duration = Duration::from_secs(30);
const STALE_RUNNING_STEP_RECOVERY_AFTER: Duration = Duration::from_secs(5 * 60);
const EXECUTE_STEP_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Root order execution workflow.
///
/// Scar tissue: §3 phase pass, §4 state alignment, and §14 benign CAS races.
#[workflow]
pub struct OrderWorkflow {
    order_id: Option<Uuid>,
    phase: OrderWorkflowPhase,
    active_attempt_id: Option<Uuid>,
    active_step_id: Option<Uuid>,
    provider_operation_hints: Vec<ProviderOperationHintSignal>,
}

impl Default for OrderWorkflow {
    fn default() -> Self {
        Self {
            order_id: None,
            phase: OrderWorkflowPhase::WaitingForFunding,
            active_attempt_id: None,
            active_step_id: None,
            provider_operation_hints: Vec::new(),
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

        let mut execution_attempt = ctx
            .start_activity(
                OrderActivities::materialize_execution_attempt,
                MaterializeExecutionAttemptInput {
                    order_id: input.order_id,
                    plan: execution_state.plan,
                },
                db_activity_options.clone(),
            )
            .await?;
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
                            execution_attempt =
                                super::types::MaterializedExecutionAttempt { attempt_id, steps };
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
                            return Ok(OrderWorkflowOutput {
                                order_id: input.order_id,
                                terminal_status: finalized.terminal_status,
                            });
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
                                let child = ctx
                                    .child_workflow(
                                        RefundWorkflow::run,
                                        RefundWorkflowInput {
                                            order_id: input.order_id,
                                            parent_attempt_id: Some(execution_attempt.attempt_id),
                                            trigger: RefundTrigger::FailedAttempt,
                                        },
                                        refund_child_options(
                                            input.order_id,
                                            execution_attempt.attempt_id,
                                        ),
                                    )
                                    .await?;
                                let refunded = child.result().await?;
                                let terminal_status = match refunded.terminal_status {
                                    RefundTerminalStatus::Refunded => OrderTerminalStatus::Refunded,
                                    RefundTerminalStatus::RefundManualInterventionRequired => {
                                        OrderTerminalStatus::RefundManualInterventionRequired
                                    }
                                };
                                return Ok(OrderWorkflowOutput {
                                    order_id: input.order_id,
                                    terminal_status,
                                });
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
                                            super::types::MaterializedExecutionAttempt {
                                                attempt_id,
                                                steps,
                                            };
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
                                        return Ok(OrderWorkflowOutput {
                                            order_id: input.order_id,
                                            terminal_status: finalized.terminal_status,
                                        });
                                    }
                                }
                            }
                            StepFailureDecision::ManualIntervention => {
                                let finalized = finalize_manual_intervention(
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
                                .await?;
                                return Ok(OrderWorkflowOutput {
                                    order_id: input.order_id,
                                    terminal_status: finalized.terminal_status,
                                });
                            }
                        }
                    }
                    StepExecutionProgress::ManualInterventionRequired { classified } => {
                        ctx.state_mut(|state| {
                            state.phase = OrderWorkflowPhase::WaitingForManualIntervention;
                            state.active_step_id = Some(step.step_id);
                        });
                        let finalized = finalize_manual_intervention(
                            ctx,
                            input.order_id,
                            execution_attempt.attempt_id,
                            step.step_id,
                            classified.reason,
                            db_activity_options.clone(),
                        )
                        .await?;
                        return Ok(OrderWorkflowOutput {
                            order_id: input.order_id,
                            terminal_status: finalized.terminal_status,
                        });
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
                        ProviderCompletionWait::ManualInterventionRequired { finalized } => {
                            return Ok(OrderWorkflowOutput {
                                order_id: input.order_id,
                                terminal_status: finalized.terminal_status,
                            });
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
        // StaleRunningStepWatchdogWorkflow, and ProviderHintPollWorkflow.
        Ok(OrderWorkflowOutput {
            order_id: input.order_id,
            terminal_status: OrderTerminalStatus::Completed,
        })
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
        _ctx: &mut SyncWorkflowContext<Self>,
        signal: ProviderOperationHintSignal,
    ) {
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
        _ctx: &mut SyncWorkflowContext<Self>,
        _signal: ManualRefundTriggerSignal,
    ) {
    }

    /// Scar tissue: §6 manual-intervention state. Exception: the release transport is a new
    /// Temporal signal surface, but it targets the manual-intervention state §6 requires.
    #[signal]
    pub fn manual_intervention_release(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        _signal: ManualInterventionReleaseSignal,
    ) {
    }

    /// Scar tissue: §3 phase-pass visibility. Exception: this operator-only Temporal query is a
    /// new debug surface, replacing lease-worker pass introspection without changing business
    /// state.
    #[query]
    pub fn debug_cursor(&self, _ctx: &WorkflowContextView) -> OrderWorkflowDebugCursor {
        OrderWorkflowDebugCursor {
            order_id: self.order_id.unwrap_or_else(Uuid::nil),
            phase: self.phase,
            active_attempt_id: self.active_attempt_id,
            active_step_id: self.active_step_id,
        }
    }
}

impl OrderWorkflow {
    fn has_provider_operation_hint(&self, order_id: Uuid) -> bool {
        self.provider_operation_hints
            .iter()
            .any(|signal| signal.order_id == order_id)
    }

    fn pop_provider_operation_hint(
        &mut self,
        order_id: Uuid,
    ) -> Option<ProviderOperationHintSignal> {
        let index = self
            .provider_operation_hints
            .iter()
            .position(|signal| signal.order_id == order_id)?;
        Some(self.provider_operation_hints.remove(index))
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
    ManualInterventionRequired { finalized: FinalizedOrder },
}

enum RefundProviderCompletionWait {
    Completed,
    RefundManualInterventionRequired,
}

async fn execute_step_with_stale_running_timer(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
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

async fn finalize_manual_intervention(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
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

async fn finalize_provider_hint_manual_intervention(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
    reason: Value,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    tracing::warn!(
        order_id = %order_id,
        attempt_id = %attempt_id,
        step_id = %step_id,
        reason = %reason,
        event_name = "provider_operation_hint.manual_intervention_required",
        "provider_operation_hint.manual_intervention_required"
    );
    finalize_manual_intervention(
        ctx,
        order_id,
        attempt_id,
        step_id,
        reason,
        db_activity_options,
    )
    .await
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
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
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
            },
            db_activity_options,
        )
        .await?;
    Ok(())
}

fn json_reason(reason: &'static str, step_id: Uuid) -> Value {
    json!({
        "reason": reason,
        "step_id": step_id,
    })
}

async fn wait_for_provider_completion_hint(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    step_id: Uuid,
    execution: StepExecuted,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<ProviderCompletionWait> {
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
                        return Ok(ProviderCompletionWait::Completed);
                    }
                    ProviderOperationHintDecision::Reject | ProviderOperationHintDecision::Defer => {
                        let finalized = finalize_provider_hint_manual_intervention(
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
                        return Ok(ProviderCompletionWait::ManualInterventionRequired { finalized });
                    }
                }
            }
            _ = ctx.timer(PROVIDER_HINT_WAIT_TIMEOUT) => {
                let _ = ctx
                    .external_workflow(provider_hint_poll_workflow_id(order_id, step_id), None)
                    .cancel(Some("provider hint wait timed out".to_string()))
                    .await;
                poll_result.cancel();
                let finalized = finalize_provider_hint_manual_intervention(
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
                return Ok(ProviderCompletionWait::ManualInterventionRequired { finalized });
            }
        }
    }
}

async fn wait_for_refund_provider_completion_hint(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: Uuid,
    step_id: Uuid,
    execution: StepExecuted,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<RefundProviderCompletionWait> {
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
                        return Ok(RefundProviderCompletionWait::RefundManualInterventionRequired);
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
                return Ok(RefundProviderCompletionWait::RefundManualInterventionRequired);
            }
        }
    }
}

fn db_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(RetryPolicy {
            maximum_attempts: 5,
            ..Default::default()
        })
        .build()
}

fn execute_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(EXECUTE_STEP_START_TO_CLOSE_TIMEOUT)
        .retry_policy(RetryPolicy {
            maximum_attempts: 3,
            ..Default::default()
        })
        .build()
}

fn refund_execute_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(120))
        .retry_policy(RetryPolicy {
            maximum_attempts: 1,
            ..Default::default()
        })
        .build()
}

fn refund_child_options(order_id: Uuid, parent_attempt_id: Uuid) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: refund_workflow_id(order_id, parent_attempt_id),
        run_timeout: Some(PROVIDER_HINT_WAIT_TIMEOUT + Duration::from_secs(15 * 60)),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

fn quote_refresh_child_options(order_id: Uuid, failed_step_id: Uuid) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: format!("order:{order_id}:quote-refresh:{failed_step_id}"),
        run_timeout: Some(Duration::from_secs(120)),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

fn provider_hint_poll_child_options(order_id: Uuid, step_id: Uuid) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: provider_hint_poll_workflow_id(order_id, step_id),
        cancel_type: ChildWorkflowCancellationType::Abandon,
        parent_close_policy: ParentClosePolicy::Terminate,
        run_timeout: Some(PROVIDER_HINT_WAIT_TIMEOUT + Duration::from_secs(60)),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

fn provider_hint_poll_workflow_id(order_id: Uuid, step_id: Uuid) -> String {
    format!("order:{order_id}:provider-hint-poll:{step_id}")
}

/// Refund child workflow.
///
/// Scar tissue: §5 retry/refund decision, §8 position discovery, and §9 refund tree.
#[workflow]
#[derive(Default)]
pub struct RefundWorkflow {
    order_id: Option<Uuid>,
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
                let _finalized = ctx
                    .start_activity(
                        OrderActivities::finalize_order_or_refund,
                        FinalizeOrderOrRefundInput {
                            order_id: input.order_id,
                            attempt_id: None,
                            step_id: None,
                            terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                            reason: None,
                        },
                        db_activity_options,
                    )
                    .await?;
                return Ok(RefundWorkflowOutput {
                    order_id: input.order_id,
                    terminal_status: RefundTerminalStatus::RefundManualInterventionRequired,
                });
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
                let _finalized = ctx
                    .start_activity(
                        OrderActivities::finalize_order_or_refund,
                        FinalizeOrderOrRefundInput {
                            order_id: input.order_id,
                            attempt_id: None,
                            step_id: None,
                            terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
                            reason: None,
                        },
                        db_activity_options,
                    )
                    .await?;
                return Ok(RefundWorkflowOutput {
                    order_id: input.order_id,
                    terminal_status: RefundTerminalStatus::RefundManualInterventionRequired,
                });
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
                    let _finalized = ctx
                        .start_activity(
                            OrderActivities::finalize_order_or_refund,
                            FinalizeOrderOrRefundInput {
                                order_id: input.order_id,
                                attempt_id: None,
                                step_id: None,
                                terminal_status:
                                    OrderTerminalStatus::RefundManualInterventionRequired,
                                reason: None,
                            },
                            db_activity_options,
                        )
                        .await?;
                    return Ok(RefundWorkflowOutput {
                        order_id: input.order_id,
                        terminal_status: RefundTerminalStatus::RefundManualInterventionRequired,
                    });
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
                    RefundProviderCompletionWait::RefundManualInterventionRequired => {
                        return Ok(RefundWorkflowOutput {
                            order_id: input.order_id,
                            terminal_status: RefundTerminalStatus::RefundManualInterventionRequired,
                        });
                    }
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
                },
                db_activity_options,
            )
            .await?;
        Ok(RefundWorkflowOutput {
            order_id: input.order_id,
            terminal_status: RefundTerminalStatus::Refunded,
        })
    }

    /// Scar tissue: §10 provider operation hint flow for refund transition steps.
    #[signal]
    pub fn provider_operation_hint(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        signal: ProviderOperationHintSignal,
    ) {
        if self
            .order_id
            .map_or(true, |order_id| order_id == signal.order_id)
        {
            self.provider_operation_hints.push(signal);
        }
    }
}

impl RefundWorkflow {
    fn has_provider_operation_hint(&self, order_id: Uuid) -> bool {
        self.provider_operation_hints
            .iter()
            .any(|signal| signal.order_id == order_id)
    }

    fn pop_provider_operation_hint(
        &mut self,
        order_id: Uuid,
    ) -> Option<ProviderOperationHintSignal> {
        let index = self
            .provider_operation_hints
            .iter()
            .position(|signal| signal.order_id == order_id)?;
        Some(self.provider_operation_hints.remove(index))
    }
}

/// Quote-refresh child workflow.
///
/// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
#[workflow]
#[derive(Default)]
pub struct QuoteRefreshWorkflow;

#[workflow_methods]
impl QuoteRefreshWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: QuoteRefreshWorkflowInput,
    ) -> WorkflowResult<QuoteRefreshWorkflowOutput> {
        let db_activity_options = db_activity_options();
        let refreshed_attempt = ctx
            .start_activity(
                QuoteRefreshActivities::compose_refreshed_quote_attempt,
                ComposeRefreshedQuoteAttemptInput {
                    order_id: input.order_id,
                    stale_attempt_id: input.stale_attempt_id,
                    failed_step_id: input.failed_step_id,
                },
                db_activity_options.clone(),
            )
            .await?;
        if let RefreshedQuoteAttemptOutcome::Untenable { reason, .. } = &refreshed_attempt.outcome {
            return Ok(QuoteRefreshWorkflowOutput {
                outcome: QuoteRefreshWorkflowOutcome::Untenable {
                    reason: reason.clone(),
                },
            });
        }
        let materialized = ctx
            .start_activity(
                QuoteRefreshActivities::materialize_refreshed_attempt,
                MaterializeRefreshedAttemptInput {
                    order_id: input.order_id,
                    refreshed_attempt,
                },
                db_activity_options,
            )
            .await?;
        Ok(QuoteRefreshWorkflowOutput {
            outcome: QuoteRefreshWorkflowOutcome::Refreshed {
                attempt_id: materialized.attempt_id,
                steps: materialized.steps,
            },
        })
    }
}

/// Stale-running-step watchdog child workflow.
///
/// Scar tissue: §6 stale running step manual intervention.
#[workflow]
#[derive(Default)]
pub struct StaleRunningStepWatchdogWorkflow;

#[workflow_methods]
impl StaleRunningStepWatchdogWorkflow {
    #[run]
    pub async fn run(
        _ctx: &mut WorkflowContext<Self>,
        _input: StaleRunningStepWatchdogInput,
    ) -> WorkflowResult<StaleRunningStepWatchdogOutput> {
        // TODO(PR4, brief §6 invariant 3; scar §6): five minutes without checkpoint requires
        // manual intervention.
        todo!("PR4: monitor stale running step checkpoint")
    }
}

/// Provider-hint fallback polling child workflow.
///
/// Scar tissue: §10 provider operation hint flow. This workflow is the polling half of the
/// signal-first plus fallback shape for brief §8.4.
#[workflow]
#[derive(Default)]
pub struct ProviderHintPollWorkflow;

#[workflow_methods]
impl ProviderHintPollWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: ProviderHintPollWorkflowInput,
    ) -> WorkflowResult<ProviderHintPollWorkflowOutput> {
        let db_activity_options = db_activity_options();
        let mut elapsed = Duration::ZERO;

        loop {
            let polled = ctx
                .start_activity(
                    ProviderObservationActivities::poll_provider_operation_hints,
                    PollProviderOperationHintsInput {
                        order_id: input.order_id,
                        step_id: input.step_id,
                    },
                    db_activity_options.clone(),
                )
                .await?;
            match polled.decision {
                ProviderOperationHintDecision::Accept | ProviderOperationHintDecision::Reject => {
                    return Ok(ProviderHintPollWorkflowOutput {
                        provider_operation_id: polled.provider_operation_id,
                        decision: polled.decision,
                        reason: polled.reason,
                    });
                }
                ProviderOperationHintDecision::Defer => {
                    if elapsed >= PROVIDER_HINT_WAIT_TIMEOUT {
                        return Ok(ProviderHintPollWorkflowOutput {
                            provider_operation_id: polled.provider_operation_id,
                            decision: ProviderOperationHintDecision::Defer,
                            reason: Some(
                                polled.reason.unwrap_or_else(|| {
                                    "provider hint polling timed out".to_string()
                                }),
                            ),
                        });
                    }
                    ctx.timer(PROVIDER_HINT_POLL_INTERVAL).await;
                    elapsed = elapsed.saturating_add(PROVIDER_HINT_POLL_INTERVAL);
                }
            }
        }
    }
}
