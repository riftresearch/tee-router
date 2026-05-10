use std::time::{Duration, SystemTime};

use router_core::models::RouterOrderStatus;
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
    AcknowledgeManualInterventionInput, AcknowledgeUnrecoverableSignal,
    CheckPreExecutionStaleQuoteInput, ClassifyStaleRunningStepInput, ClassifyStepFailureInput,
    ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput, ExecuteStepInput,
    FinalizeOrderOrRefundInput, FinalizedOrder, FundingVaultFundedSignal,
    LoadManualInterventionContextInput, LoadOrderExecutionStateInput, ManualReleaseSignal,
    ManualResolutionSignalKind, ManualTriggerRefundSignal, MarkOrderCompletedInput,
    MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput, MaterializeRefundPlanInput,
    MaterializeRetryAttemptInput, OrderTerminalStatus, OrderWorkflowDebugCursor,
    OrderWorkflowInput, OrderWorkflowOutput, OrderWorkflowPhase,
    PersistProviderOperationStatusInput, PersistProviderReceiptInput, PersistStepFailedInput,
    PersistStepReadyToFireInput, PollProviderOperationHintsInput,
    PrepareManualInterventionRefundInput, PrepareManualInterventionRetryInput,
    ProviderHintPollWorkflowInput, ProviderHintPollWorkflowOutput, ProviderOperationHintDecision,
    ProviderOperationHintSignal, QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutcome,
    QuoteRefreshWorkflowOutput, RefreshedQuoteAttemptOutcome, RefundPlanOutcome,
    RefundTerminalStatus, RefundTrigger, RefundWorkflowInput, RefundWorkflowOutput,
    ReleaseRefundManualInterventionInput, SettleProviderStepInput, SingleRefundPositionOutcome,
    StaleRunningStepClassified, StaleRunningStepDecision, StepExecuted, StepExecutionOutcome,
    StepFailureDecision, VerifyProviderOperationHintInput, WriteFailedAttemptSnapshotInput,
};
use crate::telemetry;

const PROVIDER_HINT_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const PROVIDER_HINT_POLL_INTERVAL: Duration = Duration::from_secs(30);
const STALE_RUNNING_STEP_RECOVERY_AFTER: Duration = Duration::from_secs(5 * 60);
const EXECUTE_STEP_START_TO_CLOSE_TIMEOUT: Duration = Duration::from_secs(10 * 60);
const MANUAL_INTERVENTION_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const ORDER_WORKFLOW_TYPE: &str = "OrderWorkflow";
const REFUND_WORKFLOW_TYPE: &str = "RefundWorkflow";

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
                        refund_manual: false,
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

enum RefundProviderCompletionWait {
    Completed,
    RefundManualInterventionRequired {
        resolution: RefundManualInterventionResolution,
    },
}

enum ManualInterventionSignal {
    Release(ManualReleaseSignal),
    TriggerRefund(ManualTriggerRefundSignal),
    AcknowledgeUnrecoverable(AcknowledgeUnrecoverableSignal),
}

enum ManualInterventionResolution {
    Release,
    TriggerRefund,
    Terminal(FinalizedOrder),
}

enum RefundManualInterventionResolution {
    Continue,
    Terminal(FinalizedOrder),
}

fn workflow_start_time<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
) -> Option<SystemTime> {
    if !ctx.is_replaying() {
        telemetry::record_workflow_started(workflow_type);
    }
    ctx.workflow_time()
}

fn workflow_duration<W>(
    ctx: &WorkflowContext<W>,
    started_at: Option<SystemTime>,
) -> Option<Duration> {
    let started_at = started_at?;
    ctx.workflow_time()?.duration_since(started_at).ok()
}

fn order_workflow_output(
    ctx: &WorkflowContext<OrderWorkflow>,
    started_at: Option<SystemTime>,
    order_id: Uuid,
    terminal_status: OrderTerminalStatus,
) -> WorkflowResult<OrderWorkflowOutput> {
    if !ctx.is_replaying() {
        telemetry::record_workflow_terminal(
            ORDER_WORKFLOW_TYPE,
            order_terminal_status_label(terminal_status),
            workflow_duration(ctx, started_at),
        );
    }
    Ok(OrderWorkflowOutput {
        order_id,
        terminal_status,
    })
}

fn refund_workflow_output(
    ctx: &WorkflowContext<RefundWorkflow>,
    started_at: Option<SystemTime>,
    order_id: Uuid,
    terminal_status: RefundTerminalStatus,
) -> WorkflowResult<RefundWorkflowOutput> {
    if !ctx.is_replaying() {
        telemetry::record_workflow_terminal(
            REFUND_WORKFLOW_TYPE,
            refund_terminal_status_label(terminal_status),
            workflow_duration(ctx, started_at),
        );
    }
    Ok(RefundWorkflowOutput {
        order_id,
        terminal_status,
    })
}

fn order_terminal_status_label(status: OrderTerminalStatus) -> &'static str {
    match status {
        OrderTerminalStatus::Completed => "completed",
        OrderTerminalStatus::RefundRequired => "refund_required",
        OrderTerminalStatus::Refunded => "refunded",
        OrderTerminalStatus::ManualInterventionRequired => "manual_intervention_required",
        OrderTerminalStatus::RefundManualInterventionRequired => {
            "refund_manual_intervention_required"
        }
    }
}

fn refund_terminal_status_label(status: RefundTerminalStatus) -> &'static str {
    match status {
        RefundTerminalStatus::Refunded => "refunded",
        RefundTerminalStatus::RefundManualInterventionRequired => {
            "refund_manual_intervention_required"
        }
    }
}

fn maybe_record_signal<W>(
    ctx: &SyncWorkflowContext<W>,
    workflow_type: &'static str,
    signal_name: &'static str,
) {
    if !ctx.is_replaying() {
        telemetry::record_signal(workflow_type, signal_name);
    }
}

fn manual_wait_started<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
) -> Option<SystemTime> {
    if !ctx.is_replaying() {
        telemetry::record_manual_intervention_wait_started(workflow_type);
    }
    ctx.workflow_time()
}

fn record_manual_wait_completed<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
    started_at: Option<SystemTime>,
    resolution: &'static str,
) {
    if !ctx.is_replaying() {
        telemetry::record_manual_intervention_wait_completed(
            workflow_type,
            resolution,
            workflow_duration(ctx, started_at),
        );
    }
}

fn provider_hint_wait_started<W>(ctx: &WorkflowContext<W>) -> Option<SystemTime> {
    ctx.workflow_time()
}

fn record_provider_hint_wait<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
    started_at: Option<SystemTime>,
    outcome: &'static str,
) {
    if !ctx.is_replaying() {
        telemetry::record_provider_hint_wait(
            workflow_type,
            outcome,
            workflow_duration(ctx, started_at),
        );
    }
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

async fn run_refund_child(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    parent_attempt_id: Uuid,
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

async fn wait_for_manual_intervention_resolution(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
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
                            false,
                            signal,
                            false,
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
                    false,
                    zombie_cleanup_signal(),
                    true,
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
    order_id: Uuid,
    attempt_id: Option<Uuid>,
    step_id: Option<Uuid>,
    refund_manual: bool,
    signal: AcknowledgeUnrecoverableSignal,
    zombie_cleanup: bool,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    Ok(ctx
        .start_activity(
            OrderActivities::acknowledge_manual_intervention_terminal,
            AcknowledgeManualInterventionInput {
                order_id,
                attempt_id,
                step_id,
                refund_manual,
                signal,
                zombie_cleanup,
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

async fn wait_for_refund_manual_intervention_resolution(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: Uuid,
    refund_attempt_id: Option<Uuid>,
    step_id: Option<Uuid>,
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
                            false,
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
                    true,
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
    order_id: Uuid,
    attempt_id: Option<Uuid>,
    step_id: Option<Uuid>,
    signal: AcknowledgeUnrecoverableSignal,
    zombie_cleanup: bool,
    db_activity_options: ActivityOptions,
) -> WorkflowResult<FinalizedOrder> {
    Ok(ctx
        .start_activity(
            OrderActivities::acknowledge_manual_intervention_terminal,
            AcknowledgeManualInterventionInput {
                order_id,
                attempt_id,
                step_id,
                refund_manual: true,
                signal,
                zombie_cleanup,
            },
            db_activity_options,
        )
        .await?)
}

fn json_reason(reason: &'static str, step_id: Uuid) -> Value {
    json!({
        "reason": reason,
        "step_id": step_id,
    })
}

fn zombie_cleanup_signal() -> AcknowledgeUnrecoverableSignal {
    AcknowledgeUnrecoverableSignal {
        reason: "zombie_cleanup_after_manual_intervention_timeout".to_string(),
        operator_id: Some("temporal-worker".to_string()),
        requested_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }
}

fn refund_terminal_status(status: OrderTerminalStatus) -> RefundTerminalStatus {
    match status {
        OrderTerminalStatus::Refunded => RefundTerminalStatus::Refunded,
        OrderTerminalStatus::RefundManualInterventionRequired
        | OrderTerminalStatus::ManualInterventionRequired
        | OrderTerminalStatus::RefundRequired
        | OrderTerminalStatus::Completed => RefundTerminalStatus::RefundManualInterventionRequired,
    }
}

async fn wait_for_provider_completion_hint(
    ctx: &mut WorkflowContext<OrderWorkflow>,
    order_id: Uuid,
    step_id: Uuid,
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

async fn wait_for_refund_provider_completion_hint(
    ctx: &mut WorkflowContext<RefundWorkflow>,
    order_id: Uuid,
    step_id: Uuid,
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
        run_timeout: Some(MANUAL_INTERVENTION_WAIT_TIMEOUT + Duration::from_secs(60 * 60)),
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
