use super::*;

pub(super) enum ManualInterventionSignal {
    Release(ManualReleaseSignal),
    TriggerRefund(ManualTriggerRefundSignal),
    AcknowledgeUnrecoverable(AcknowledgeUnrecoverableSignal),
}

pub(super) fn workflow_start_time<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
) -> Option<SystemTime> {
    if !ctx.is_replaying() {
        telemetry::record_workflow_started(workflow_type);
    }
    ctx.workflow_time()
}

pub(super) fn workflow_duration<W>(
    ctx: &WorkflowContext<W>,
    started_at: Option<SystemTime>,
) -> Option<Duration> {
    let started_at = started_at?;
    ctx.workflow_time()?.duration_since(started_at).ok()
}

pub(super) fn order_workflow_output(
    ctx: &WorkflowContext<OrderWorkflow>,
    started_at: Option<SystemTime>,
    order_id: WorkflowOrderId,
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

pub(super) fn refund_workflow_output(
    ctx: &WorkflowContext<RefundWorkflow>,
    started_at: Option<SystemTime>,
    order_id: WorkflowOrderId,
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

pub(super) fn order_terminal_status_label(status: OrderTerminalStatus) -> &'static str {
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

pub(super) fn refund_terminal_status_label(status: RefundTerminalStatus) -> &'static str {
    match status {
        RefundTerminalStatus::Refunded => "refunded",
        RefundTerminalStatus::RefundManualInterventionRequired => {
            "refund_manual_intervention_required"
        }
    }
}

pub(super) fn maybe_record_signal<W>(
    ctx: &SyncWorkflowContext<W>,
    workflow_type: &'static str,
    signal_name: &'static str,
) {
    if !ctx.is_replaying() {
        telemetry::record_signal(workflow_type, signal_name);
    }
}

pub(super) fn manual_wait_started<W>(
    ctx: &WorkflowContext<W>,
    workflow_type: &'static str,
) -> Option<SystemTime> {
    if !ctx.is_replaying() {
        telemetry::record_manual_intervention_wait_started(workflow_type);
    }
    ctx.workflow_time()
}

pub(super) fn record_manual_wait_completed<W>(
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

pub(super) fn provider_hint_wait_started<W>(ctx: &WorkflowContext<W>) -> Option<SystemTime> {
    ctx.workflow_time()
}

pub(super) fn record_provider_hint_wait<W>(
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

pub(super) fn json_reason(reason: &'static str, step_id: WorkflowStepId) -> Value {
    json!({
        "reason": reason,
        "step_id": step_id,
    })
}

pub(super) fn zombie_cleanup_signal() -> AcknowledgeUnrecoverableSignal {
    AcknowledgeUnrecoverableSignal {
        reason: "zombie_cleanup_after_manual_intervention_timeout".to_string(),
        operator_id: Some("temporal-worker".to_string()),
        requested_at: chrono::DateTime::<chrono::Utc>::UNIX_EPOCH,
    }
}

pub(super) fn refund_terminal_status(status: OrderTerminalStatus) -> RefundTerminalStatus {
    match status {
        OrderTerminalStatus::Refunded => RefundTerminalStatus::Refunded,
        OrderTerminalStatus::RefundManualInterventionRequired
        | OrderTerminalStatus::ManualInterventionRequired
        | OrderTerminalStatus::RefundRequired
        | OrderTerminalStatus::Completed => RefundTerminalStatus::RefundManualInterventionRequired,
    }
}

pub(super) fn db_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(RetryPolicy {
            maximum_attempts: 5,
            ..Default::default()
        })
        .build()
}

pub(super) fn execute_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(EXECUTE_STEP_START_TO_CLOSE_TIMEOUT)
        .retry_policy(RetryPolicy {
            maximum_attempts: 3,
            ..Default::default()
        })
        .build()
}

pub(super) fn refund_execute_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(120))
        .retry_policy(RetryPolicy {
            maximum_attempts: 1,
            ..Default::default()
        })
        .build()
}

pub(super) fn refund_child_options(
    order_id: WorkflowOrderId,
    parent_attempt_id: WorkflowAttemptId,
) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: refund_workflow_id(order_id, parent_attempt_id),
        run_timeout: Some(MANUAL_INTERVENTION_WAIT_TIMEOUT + Duration::from_secs(60 * 60)),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

pub(super) fn quote_refresh_child_options(
    order_id: WorkflowOrderId,
    failed_step_id: WorkflowStepId,
) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: format!("order:{order_id}:quote-refresh:{failed_step_id}"),
        // Quote refresh must tolerate activity queue backlog during load spikes. Short run timeouts
        // let load-induced refresh delays fail the parent workflow while leaving Postgres executing.
        run_timeout: Some(QUOTE_REFRESH_WORKFLOW_TIMEOUT),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

pub(super) fn provider_hint_poll_child_options(
    order_id: WorkflowOrderId,
    step_id: WorkflowStepId,
) -> ChildWorkflowOptions {
    ChildWorkflowOptions {
        workflow_id: provider_hint_poll_workflow_id(order_id, step_id),
        cancel_type: ChildWorkflowCancellationType::Abandon,
        parent_close_policy: ParentClosePolicy::Terminate,
        run_timeout: Some(PROVIDER_HINT_WAIT_TIMEOUT + Duration::from_secs(60)),
        task_timeout: Some(Duration::from_secs(30)),
        ..Default::default()
    }
}

pub(super) fn provider_hint_poll_workflow_id(
    order_id: WorkflowOrderId,
    step_id: WorkflowStepId,
) -> String {
    format!("order:{order_id}:provider-hint-poll:{step_id}")
}
