use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

use super::types::{
    FundingVaultFundedSignal, ManualInterventionReleaseSignal, ManualRefundTriggerSignal,
    OrderWorkflowDebugCursor, OrderWorkflowInput, OrderWorkflowOutput,
    ProviderHintPollWorkflowInput, ProviderHintPollWorkflowOutput, ProviderOperationHintSignal,
    QuoteRefreshWorkflowInput, QuoteRefreshWorkflowOutput, RefundWorkflowInput,
    RefundWorkflowOutput, StaleRunningStepWatchdogInput, StaleRunningStepWatchdogOutput,
};

/// Root order execution workflow.
///
/// Scar tissue: §3 phase pass, §4 state alignment, and §14 benign CAS races.
#[workflow]
#[derive(Default)]
pub struct OrderWorkflow;

#[workflow_methods]
impl OrderWorkflow {
    #[run]
    pub async fn run(
        _ctx: &mut WorkflowContext<Self>,
        _input: OrderWorkflowInput,
    ) -> WorkflowResult<OrderWorkflowOutput> {
        // TODO(PR3, brief §6 invariant 1; scar §2): call the six persistence boundaries as six
        // distinct activities.
        // TODO(PR4, brief §6 invariant 2; scar §5): route refund failures to manual
        // intervention, never to retry.
        // TODO(PR4, brief §6 invariant 3; scar §6): start the stale-running-step watchdog child
        // and surface manual intervention after five minutes without a checkpoint.
        // TODO(PR4, brief §6 invariant 4; scar §8): start refund only after single-position
        // discovery succeeds.
        // TODO(PR4, brief §6 invariant 5; scar §7): start quote refresh as a child workflow that
        // creates its own attempt.
        // Child-workflow shape: RefundWorkflow, QuoteRefreshWorkflow,
        // StaleRunningStepWatchdogWorkflow, and ProviderHintPollWorkflow.
        todo!("PR3: orchestrate order execution workflow")
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
        todo!("PR3: expose workflow debug cursor")
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
        todo!("PR3: poll provider hints as signal fallback")
    }
}
