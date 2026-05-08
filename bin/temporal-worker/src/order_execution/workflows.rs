use std::time::Duration;

use temporalio_common::protos::temporal::api::common::v1::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult,
};
use uuid::Uuid;

use super::activities::OrderActivities;
use super::types::{
    ExecuteStepInput, FundingVaultFundedSignal, LoadOrderExecutionStateInput,
    ManualInterventionReleaseSignal, ManualRefundTriggerSignal, MarkOrderCompletedInput,
    MaterializeExecutionAttemptInput, OrderTerminalStatus, OrderWorkflowDebugCursor,
    OrderWorkflowInput, OrderWorkflowOutput, OrderWorkflowPhase,
    PersistProviderOperationStatusInput, PersistProviderReceiptInput, PersistStepReadyToFireInput,
    ProviderHintPollWorkflowInput, ProviderHintPollWorkflowOutput, ProviderOperationHintSignal,
    QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutput, RefundWorkflowInput,
    RefundWorkflowOutput, SettleProviderStepInput, StaleRunningStepWatchdogInput,
    StaleRunningStepWatchdogOutput,
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
        let execution_attempt = ctx
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

        for step in execution_attempt.steps {
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
            let executed = ctx
                .start_activity(
                    OrderActivities::execute_step,
                    ExecuteStepInput {
                        order_id: input.order_id,
                        attempt_id: execution_attempt.attempt_id,
                        step_id: step.step_id,
                    },
                    execute_activity_options.clone(),
                )
                .await?;
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

        // TODO(PR4b, brief §6 invariant 2; scar §5): route refund failures to manual
        // intervention, never to retry.
        // TODO(PR6, brief §6 invariant 4; scar §8): start refund only after single-position
        // discovery succeeds.
        // TODO(PR5, brief §6 invariant 5; scar §7): start quote refresh as a child workflow that
        // creates its own attempt.
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
        _ctx: &mut WorkflowContext<Self>,
        _input: RefundWorkflowInput,
    ) -> WorkflowResult<RefundWorkflowOutput> {
        // TODO(PR4, brief §6 invariant 2; scar §5): refund attempts never retry.
        // TODO(PR4, brief §6 invariant 4; scar §8): require exactly one recoverable position.
        todo!("PR4: orchestrate refund child workflow")
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
        _ctx: &mut WorkflowContext<Self>,
        _input: QuoteRefreshWorkflowInput,
    ) -> WorkflowResult<QuoteRefreshWorkflowOutput> {
        // TODO(PR4, brief §6 invariant 5; scar §7): quote refresh creates its own attempt.
        todo!("PR4: orchestrate quote refresh child workflow")
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
