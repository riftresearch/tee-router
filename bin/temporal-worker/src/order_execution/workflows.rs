use std::time::Duration;

use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext,
    WorkflowContextView, WorkflowResult,
};
use uuid::Uuid;

use super::activities::{OrderActivities, QuoteRefreshActivities, RefundActivities};
use super::types::{
    ClassifyStepFailureInput, ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput,
    ExecuteStepInput, FinalizeOrderOrRefundInput, FundingVaultFundedSignal,
    LoadOrderExecutionStateInput, ManualInterventionReleaseSignal, ManualRefundTriggerSignal,
    MarkOrderCompletedInput, MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput,
    MaterializeRefundPlanInput, MaterializeRetryAttemptInput, OrderTerminalStatus,
    OrderWorkflowDebugCursor, OrderWorkflowInput, OrderWorkflowOutput, OrderWorkflowPhase,
    PersistProviderOperationStatusInput, PersistProviderReceiptInput, PersistStepFailedInput,
    PersistStepReadyToFireInput, ProviderHintPollWorkflowInput, ProviderHintPollWorkflowOutput,
    ProviderOperationHintSignal, QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutcome,
    QuoteRefreshWorkflowOutput, RefreshedQuoteAttemptOutcome, RefundPlanOutcome,
    RefundTerminalStatus, RefundTrigger, RefundWorkflowInput, RefundWorkflowOutput,
    SettleProviderStepInput, SingleRefundPositionOutcome, StaleRunningStepWatchdogInput,
    StaleRunningStepWatchdogOutput, StepFailureDecision, WriteFailedAttemptSnapshotInput,
};

/// Root order execution workflow.
///
/// Scar tissue: §3 phase pass, §4 state alignment, and §14 benign CAS races.
#[workflow]
pub struct OrderWorkflow {
    order_id: Option<Uuid>,
    phase: OrderWorkflowPhase,
    active_attempt_id: Option<Uuid>,
    active_step_id: Option<Uuid>,
}

impl Default for OrderWorkflow {
    fn default() -> Self {
        Self {
            order_id: None,
            phase: OrderWorkflowPhase::WaitingForFunding,
            active_attempt_id: None,
            active_step_id: None,
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

        // TODO(PR8, brief §6 invariant 3; scar §6): start the stale-running-step watchdog child
        // before provider execution and surface manual intervention after five minutes without a
        // checkpoint.
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
                let executed = match ctx
                    .start_activity(
                        OrderActivities::execute_step,
                        ExecuteStepInput {
                            order_id: input.order_id,
                            attempt_id: execution_attempt.attempt_id,
                            step_id: step.step_id,
                        },
                        execute_activity_options.clone(),
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
                                                    terminal_status:
                                                        OrderTerminalStatus::RefundRequired,
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
                                return Err(anyhow::anyhow!(
                                    "PR4b does not implement ManualIntervention for failed step {}",
                                    step.step_id
                                )
                                .into());
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
                            execution: executed,
                        },
                        db_activity_options.clone(),
                    )
                    .await?;
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
        _signal: ProviderOperationHintSignal,
    ) {
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

fn db_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(RetryPolicy {
            maximum_attempts: 5,
            ..Default::default()
        })
        .build()
}

fn execute_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(120))
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
        workflow_id: format!("order:{order_id}:refund:{parent_attempt_id}"),
        run_timeout: Some(Duration::from_secs(240)),
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

/// Refund child workflow.
///
/// Scar tissue: §5 retry/refund decision, §8 position discovery, and §9 refund tree.
#[workflow]
#[derive(Default)]
pub struct RefundWorkflow;

#[workflow_methods]
impl RefundWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: RefundWorkflowInput,
    ) -> WorkflowResult<RefundWorkflowOutput> {
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
                            terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
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
                            terminal_status: OrderTerminalStatus::RefundManualInterventionRequired,
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
                                terminal_status:
                                    OrderTerminalStatus::RefundManualInterventionRequired,
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
                        execution: executed,
                    },
                    db_activity_options.clone(),
                )
                .await?;
        }

        let _finalized = ctx
            .start_activity(
                OrderActivities::finalize_order_or_refund,
                FinalizeOrderOrRefundInput {
                    order_id: input.order_id,
                    attempt_id: Some(refund_attempt_id),
                    terminal_status: OrderTerminalStatus::Refunded,
                },
                db_activity_options,
            )
            .await?;
        Ok(RefundWorkflowOutput {
            order_id: input.order_id,
            terminal_status: RefundTerminalStatus::Refunded,
        })
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
        _ctx: &mut WorkflowContext<Self>,
        _input: ProviderHintPollWorkflowInput,
    ) -> WorkflowResult<ProviderHintPollWorkflowOutput> {
        todo!("PR7: poll provider hints as signal fallback")
    }
}
