#![allow(dead_code)]

// Activity function constants are intentionally registered before PR3 workflow logic invokes them.
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};

use super::types::{
    AcrossOnchainLogRecovered, BoundaryPersisted, CancelTimedOutHyperliquidTradeInput,
    ClassifyStepFailureInput, ComposeRefreshedQuoteAttemptInput, DiscoverSingleRefundPositionInput,
    DispatchStepProviderActionInput, FailedAttemptSnapshotWritten, FinalizeOrderOrRefundInput,
    FinalizedOrder, HyperliquidTradeCancelRecorded, LoadOrderExecutionStateInput,
    MaterializeExecutionAttemptInput, MaterializeRefreshedAttemptInput, MaterializeRefundPlanInput,
    MaterializedExecutionAttempt, OrderExecutionState, PersistProviderOperationStatusInput,
    PersistProviderReceiptInput, PersistStepFailedInput, PersistStepReadyToFireInput,
    PersistStepTerminalStatusInput, PollProviderOperationHintsInput, ProviderActionDispatchShape,
    ProviderOperationHintVerified, ProviderOperationHintsPolled, RecoverAcrossOnchainLogInput,
    RefreshedAttemptMaterialized, RefreshedQuoteAttemptShape, RefundPlanShape,
    SettleProviderStepInput, SingleRefundPosition, StepFailureDecision,
    VerifyProviderOperationHintInput, WriteFailedAttemptSnapshotInput,
};

pub struct OrderActivities;

#[activities]
impl OrderActivities {
    /// Scar tissue: §3 phase pass and §4 order/vault/step state alignment.
    #[activity]
    pub async fn load_order_execution_state(
        _ctx: ActivityContext,
        _input: LoadOrderExecutionStateInput,
    ) -> Result<OrderExecutionState, ActivityError> {
        todo!("PR3: load canonical order execution state from Postgres")
    }

    /// Scar tissue: §3 phase 2 and §4 state alignment.
    #[activity]
    pub async fn materialize_execution_attempt(
        _ctx: ActivityContext,
        _input: MaterializeExecutionAttemptInput,
    ) -> Result<MaterializedExecutionAttempt, ActivityError> {
        todo!("PR3: materialize the execution attempt")
    }

    /// Scar tissue: §2 boundary 1, `AfterStepMarkedReadyToFire`.
    #[activity]
    pub async fn persist_step_ready_to_fire(
        _ctx: ActivityContext,
        _input: PersistStepReadyToFireInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 1")
    }

    /// Scar tissue: §2 boundary 2, `AfterStepMarkedFailed`.
    #[activity]
    pub async fn persist_step_failed(
        _ctx: ActivityContext,
        _input: PersistStepFailedInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 2")
    }

    /// Scar tissue: §2 boundary 3, `AfterExecutionStepStatusPersisted`.
    #[activity]
    pub async fn persist_step_terminal_status(
        _ctx: ActivityContext,
        _input: PersistStepTerminalStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 3")
    }

    /// Scar tissue: §2 boundary 4, `AfterProviderReceiptPersisted`.
    #[activity]
    pub async fn persist_provider_receipt(
        _ctx: ActivityContext,
        _input: PersistProviderReceiptInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 4")
    }

    /// Scar tissue: §2 boundary 5, `AfterProviderOperationStatusPersisted`.
    #[activity]
    pub async fn persist_provider_operation_status(
        _ctx: ActivityContext,
        _input: PersistProviderOperationStatusInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 5")
    }

    /// Scar tissue: §2 boundary 6, `AfterProviderStepSettlement`.
    #[activity]
    pub async fn settle_provider_step(
        _ctx: ActivityContext,
        _input: SettleProviderStepInput,
    ) -> Result<BoundaryPersisted, ActivityError> {
        // TODO(PR3, brief §6 invariant 1; scar §2): keep this as its own activity call.
        todo!("PR3: persist boundary 6")
    }

    /// Scar tissue: §5 retry/refund decision and §7 stale quote branch.
    #[activity]
    pub async fn classify_step_failure(
        _ctx: ActivityContext,
        _input: ClassifyStepFailureInput,
    ) -> Result<StepFailureDecision, ActivityError> {
        todo!("PR4: classify failed step recovery path")
    }

    /// Scar tissue: §15 failure snapshot.
    #[activity]
    pub async fn write_failed_attempt_snapshot(
        _ctx: ActivityContext,
        _input: WriteFailedAttemptSnapshotInput,
    ) -> Result<FailedAttemptSnapshotWritten, ActivityError> {
        todo!("PR4: write failed attempt snapshot")
    }

    /// Scar tissue: §16.3 order finalisation and custody lifecycle.
    #[activity]
    pub async fn finalize_order_or_refund(
        _ctx: ActivityContext,
        _input: FinalizeOrderOrRefundInput,
    ) -> Result<FinalizedOrder, ActivityError> {
        todo!("PR5: finalize order/refund terminal state")
    }
}

pub struct RefundActivities;

#[activities]
impl RefundActivities {
    /// Scar tissue: §8 refund position discovery and §16.1 balance reads.
    #[activity]
    pub async fn discover_single_refund_position(
        _ctx: ActivityContext,
        _input: DiscoverSingleRefundPositionInput,
    ) -> Result<SingleRefundPosition, ActivityError> {
        // TODO(PR4, brief §6 invariant 4; scar §8): require exactly one recoverable position.
        todo!("PR4: discover single recoverable refund position")
    }

    /// Scar tissue: §9 refund tree and §16.1 refund materialisation.
    #[activity]
    pub async fn materialize_refund_plan(
        _ctx: ActivityContext,
        _input: MaterializeRefundPlanInput,
    ) -> Result<RefundPlanShape, ActivityError> {
        // TODO(PR4, brief §6 invariant 2; scar §5): refund attempts never get retry branches.
        todo!("PR4: materialize refund plan")
    }
}

pub struct QuoteRefreshActivities;

#[activities]
impl QuoteRefreshActivities {
    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn compose_refreshed_quote_attempt(
        _ctx: ActivityContext,
        _input: ComposeRefreshedQuoteAttemptInput,
    ) -> Result<RefreshedQuoteAttemptShape, ActivityError> {
        // TODO(PR4, brief §6 invariant 5; scar §7): stale quote refresh is its own attempt.
        todo!("PR4: compose refreshed quote attempt")
    }

    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn materialize_refreshed_attempt(
        _ctx: ActivityContext,
        _input: MaterializeRefreshedAttemptInput,
    ) -> Result<RefreshedAttemptMaterialized, ActivityError> {
        // TODO(PR4, brief §6 invariant 5; scar §7): persist refreshed attempt separately.
        todo!("PR4: materialize refreshed quote attempt")
    }
}

pub struct ProviderObservationActivities;

#[activities]
impl ProviderObservationActivities {
    /// Scar tissue: §10 provider operation hint flow and verifier dispatch.
    #[activity]
    pub async fn verify_provider_operation_hint(
        _ctx: ActivityContext,
        _input: VerifyProviderOperationHintInput,
    ) -> Result<ProviderOperationHintVerified, ActivityError> {
        todo!("PR3: verify provider operation hint against provider API")
    }

    /// Scar tissue: §10 provider hint recovery. This is the polling half of the user-approved
    /// signal-first plus fallback shape for brief §8.4.
    #[activity]
    pub async fn poll_provider_operation_hints(
        _ctx: ActivityContext,
        _input: PollProviderOperationHintsInput,
    ) -> Result<ProviderOperationHintsPolled, ActivityError> {
        todo!("PR3: poll provider hints as a fallback to signals")
    }

    /// Scar tissue: §11 Across on-chain log recovery.
    #[activity]
    pub async fn recover_across_onchain_log(
        _ctx: ActivityContext,
        _input: RecoverAcrossOnchainLogInput,
    ) -> Result<AcrossOnchainLogRecovered, ActivityError> {
        todo!("PR4: recover Across fill from on-chain logs")
    }

    /// Scar tissue: §12 Hyperliquid trade timeout cancel.
    #[activity]
    pub async fn cancel_timed_out_hyperliquid_trade(
        _ctx: ActivityContext,
        _input: CancelTimedOutHyperliquidTradeInput,
    ) -> Result<HyperliquidTradeCancelRecorded, ActivityError> {
        todo!("PR4: cancel timed-out Hyperliquid trade")
    }
}

pub struct StepDispatchActivities;

#[activities]
impl StepDispatchActivities {
    /// Scar tissue: §13 step type dispatch provider trait family mapping.
    #[activity]
    pub async fn dispatch_step_provider_action(
        _ctx: ActivityContext,
        _input: DispatchStepProviderActionInput,
    ) -> Result<ProviderActionDispatchShape, ActivityError> {
        todo!("PR3: dispatch provider action for the active step")
    }
}
