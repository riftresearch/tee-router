use std::time::Duration;

use metrics::{counter, histogram};

pub fn record_process_started() {
    counter!("tee_router_temporal_worker_process_started_total").increment(1);
}

pub fn record_workflow_started(workflow_type: &'static str) {
    counter!(
        "tee_router_temporal_worker_workflow_started_total",
        "workflow_type" => workflow_type,
    )
    .increment(1);
}

pub fn record_workflow_terminal(
    workflow_type: &'static str,
    terminal_status: &'static str,
    duration: Option<Duration>,
) {
    counter!(
        "tee_router_temporal_worker_workflow_terminal_total",
        "workflow_type" => workflow_type,
        "terminal_status" => terminal_status,
    )
    .increment(1);
    if let Some(duration) = duration {
        histogram!(
            "tee_router_temporal_worker_workflow_duration_seconds",
            "workflow_type" => workflow_type,
            "terminal_status" => terminal_status,
        )
        .record(duration.as_secs_f64());
    }
}

pub fn record_activity(activity_name: &'static str, success: bool, duration: Duration) {
    let status = success_status(success);
    counter!(
        "tee_router_temporal_worker_activity_invocations_total",
        "activity_name" => activity_name,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_temporal_worker_activity_duration_seconds",
        "activity_name" => activity_name,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

pub fn record_signal(workflow_type: &'static str, signal_name: &'static str) {
    counter!(
        "tee_router_temporal_worker_signals_total",
        "workflow_type" => workflow_type,
        "signal_name" => signal_name,
    )
    .increment(1);
}

pub fn record_manual_intervention_wait_started(workflow_type: &'static str) {
    counter!(
        "tee_router_temporal_worker_manual_intervention_wait_started_total",
        "workflow_type" => workflow_type,
    )
    .increment(1);
}

pub fn record_manual_intervention_wait_completed(
    workflow_type: &'static str,
    resolution: &'static str,
    duration: Option<Duration>,
) {
    counter!(
        "tee_router_temporal_worker_manual_intervention_wait_completed_total",
        "workflow_type" => workflow_type,
        "resolution" => resolution,
    )
    .increment(1);
    if let Some(duration) = duration {
        histogram!(
            "tee_router_temporal_worker_manual_intervention_wait_duration_seconds",
            "workflow_type" => workflow_type,
            "resolution" => resolution,
        )
        .record(duration.as_secs_f64());
    }
}

pub fn record_provider_hint_wait(
    workflow_type: &'static str,
    outcome: &'static str,
    duration: Option<Duration>,
) {
    counter!(
        "tee_router_temporal_worker_provider_hint_wait_completed_total",
        "workflow_type" => workflow_type,
        "outcome" => outcome,
    )
    .increment(1);
    if let Some(duration) = duration {
        histogram!(
            "tee_router_temporal_worker_provider_hint_wait_duration_seconds",
            "workflow_type" => workflow_type,
            "outcome" => outcome,
        )
        .record(duration.as_secs_f64());
    }
}

pub fn record_refund_attempt_materialized(position_kind: &'static str, transition_kind: &str) {
    counter!(
        "tee_router_temporal_worker_refund_attempt_materialized_total",
        "position_kind" => position_kind,
        "transition_kind" => transition_kind.to_string(),
    )
    .increment(1);
}

pub fn record_refund_position_untenable(reason: &'static str) {
    counter!(
        "tee_router_temporal_worker_refund_position_untenable_total",
        "reason" => reason,
    )
    .increment(1);
}

pub fn record_stale_quote_refresh(outcome: &'static str) {
    counter!(
        "tee_router_temporal_worker_stale_quote_refresh_total",
        "outcome" => outcome,
    )
    .increment(1);
}

pub fn record_stale_quote_refresh_cap_hit() {
    counter!("tee_router_temporal_worker_stale_quote_refresh_cap_hit_total").increment(1);
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}
