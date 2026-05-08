use crate::services::vault_manager::VaultError;
use metrics::{counter, gauge, histogram};
use router_core::{
    models::{
        DepositVault, DepositVaultStatus, OrderExecutionStep, OrderProviderOperation,
        OrderProviderOperationHint, RouterOrder, RouterOrderStatus,
    },
    protocol::{AssetId, ChainId, DepositAsset},
};
use std::time::Duration;
use tracing::info;

const IN_PROGRESS_ORDER_STATUSES: [RouterOrderStatus; 5] = [
    RouterOrderStatus::PendingFunding,
    RouterOrderStatus::Funded,
    RouterOrderStatus::Executing,
    RouterOrderStatus::RefundRequired,
    RouterOrderStatus::Refunding,
];

pub fn record_http_response(route: &str, method: &str, status: u16, duration: Duration) {
    let status_class = status_class(status);
    counter!(
        "tee_router_http_requests_total",
        "route" => route.to_string(),
        "method" => method.to_string(),
        "status_class" => status_class,
    )
    .increment(1);
    histogram!(
        "tee_router_http_request_duration_seconds",
        "route" => route.to_string(),
        "method" => method.to_string(),
        "status_class" => status_class,
    )
    .record(duration.as_secs_f64());
}

pub fn record_db_query(operation: &'static str, success: bool, duration: Duration) {
    let status = success_status(success);
    counter!(
        "tee_router_db_queries_total",
        "operation" => operation,
        "status" => status,
    )
    .increment(1);
    histogram!(
        "tee_router_db_query_duration_seconds",
        "operation" => operation,
        "status" => status,
    )
    .record(duration.as_secs_f64());
}

pub fn record_venue_http_status(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status: u16,
    duration: Duration,
) {
    record_venue_request(venue, method, endpoint, status_class(status), duration);
}

pub fn record_venue_transport_error(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    duration: Duration,
) {
    record_venue_request(venue, method, endpoint, "transport_error", duration);
}

fn record_venue_request(
    venue: &'static str,
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    counter!(
        "tee_router_venue_requests_total",
        "venue" => venue,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .increment(1);
    histogram!(
        "tee_router_venue_request_duration_seconds",
        "venue" => venue,
        "method" => method,
        "endpoint" => endpoint,
        "status_class" => status_class,
    )
    .record(duration.as_secs_f64());
}

pub fn record_order_workflow_event(order: &RouterOrder, event: &'static str) {
    let span = order_workflow_span(order, event);
    span.in_scope(|| {
        info!(
            order_id = %order.id,
            order_status = order.status.to_db_string(),
            workflow_trace_id = %order.workflow_trace_id,
            workflow_event = event,
            "Router order workflow event",
        );
    });
}

pub fn record_execution_step_workflow_event(
    order: &RouterOrder,
    step: &OrderExecutionStep,
    event: &'static str,
) {
    let span = order_workflow_span(order, event);
    span.in_scope(|| {
        info!(
            order_id = %order.id,
            step_id = %step.id,
            execution_attempt_id = %step.execution_attempt_id.map(|id| id.to_string()).unwrap_or_default(),
            step_index = step.step_index,
            step_type = step.step_type.to_db_string(),
            step_status = step.status.to_db_string(),
            provider = %step.provider,
            workflow_trace_id = %order.workflow_trace_id,
            workflow_event = event,
            "Router order execution-step workflow event",
        );
    });
}

pub fn record_provider_operation_workflow_event(
    order: &RouterOrder,
    operation: &OrderProviderOperation,
    event: &'static str,
) {
    let span = order_workflow_span(order, event);
    span.in_scope(|| {
        info!(
            order_id = %order.id,
            provider_operation_id = %operation.id,
            execution_attempt_id = %operation.execution_attempt_id.map(|id| id.to_string()).unwrap_or_default(),
            execution_step_id = %operation.execution_step_id.map(|id| id.to_string()).unwrap_or_default(),
            provider = %operation.provider,
            provider_operation_type = operation.operation_type.to_db_string(),
            provider_operation_status = operation.status.to_db_string(),
            provider_ref = operation.provider_ref.as_deref().unwrap_or_default(),
            workflow_trace_id = %order.workflow_trace_id,
            workflow_event = event,
            "Router provider-operation workflow event",
        );
    });
}

pub fn record_provider_operation_hint_workflow_event(
    order: &RouterOrder,
    hint: &OrderProviderOperationHint,
    operation: &OrderProviderOperation,
    event: &'static str,
) {
    let span = order_workflow_span(order, event);
    span.in_scope(|| {
        info!(
            order_id = %order.id,
            provider_operation_id = %operation.id,
            provider_operation_hint_id = %hint.id,
            provider = %operation.provider,
            provider_operation_type = operation.operation_type.to_db_string(),
            hint_source = %hint.source,
            hint_status = hint.status.to_db_string(),
            workflow_trace_id = %order.workflow_trace_id,
            workflow_event = event,
            "Router provider-operation hint workflow event",
        );
    });
}

fn order_workflow_span(order: &RouterOrder, event: &'static str) -> tracing::Span {
    let span = tracing::info_span!(
        "router.order.workflow",
        order_id = %order.id,
        order_status = order.status.to_db_string(),
        workflow_trace_id = %order.workflow_trace_id,
        workflow_event = event,
    );
    let context = observability::WorkflowTraceContext {
        trace_id: order.workflow_trace_id.clone(),
        parent_span_id: order.workflow_parent_span_id.clone(),
    };
    let _ = observability::set_workflow_parent(&span, &context);
    span
}

pub fn record_vault_created(vault: &DepositVault) {
    counter!(
        "tee_router_vault_created_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
    )
    .increment(1);
}

pub fn record_vault_create_failed(request: &DepositAsset, error: &VaultError) {
    counter!(
        "tee_router_vault_create_failed_total",
        "chain" => chain_label(&request.chain),
        "asset_kind" => asset_kind(&request.asset),
        "error_kind" => vault_error_kind(error),
    )
    .increment(1);
}

pub fn record_vault_cancel_requested(vault: &DepositVault) {
    counter!(
        "tee_router_vault_cancel_requested_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "status" => vault.status.to_db_string(),
    )
    .increment(1);
}

pub fn record_vault_cancel_failed(error: &VaultError) {
    counter!(
        "tee_router_vault_cancel_failed_total",
        "error_kind" => vault_error_kind(error),
    )
    .increment(1);
}

pub fn record_vault_transition(
    vault: &DepositVault,
    from_status: DepositVaultStatus,
    to_status: DepositVaultStatus,
) {
    counter!(
        "tee_router_vault_transition_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "from_status" => from_status.to_db_string(),
        "to_status" => to_status.to_db_string(),
    )
    .increment(1);
}

pub fn record_refund_pass(stage: &'static str, scanned: usize, duration: Duration) {
    counter!(
        "tee_router_refund_pass_total",
        "stage" => stage,
    )
    .increment(1);
    histogram!(
        "tee_router_refund_pass_duration_seconds",
        "stage" => stage,
    )
    .record(duration.as_secs_f64());
    gauge!(
        "tee_router_refund_pass_scanned",
        "stage" => stage,
    )
    .set(scanned as f64);
}

pub fn record_refund_attempt(vault: &DepositVault) {
    counter!(
        "tee_router_vault_refund_attempt_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "status" => vault.status.to_db_string(),
    )
    .increment(1);
}

pub fn record_refund_success(vault: &DepositVault, duration: Duration) {
    counter!(
        "tee_router_vault_refund_success_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
    )
    .increment(1);
    histogram!(
        "tee_router_vault_refund_duration_seconds",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "status" => "success",
    )
    .record(duration.as_secs_f64());
}

pub fn record_refund_failure(vault: &DepositVault, failure_kind: &'static str, duration: Duration) {
    counter!(
        "tee_router_vault_refund_failure_total",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "failure_kind" => failure_kind,
    )
    .increment(1);
    histogram!(
        "tee_router_vault_refund_duration_seconds",
        "chain" => vault.deposit_asset.chain.as_str().to_string(),
        "asset_kind" => asset_kind(&vault.deposit_asset.asset),
        "status" => "failure",
    )
    .record(duration.as_secs_f64());
}

pub fn record_worker_tick(worker: &'static str, duration: Duration) {
    counter!(
        "tee_router_worker_tick_total",
        "worker" => worker,
    )
    .increment(1);
    histogram!(
        "tee_router_worker_tick_duration_seconds",
        "worker" => worker,
    )
    .record(duration.as_secs_f64());
}

pub fn record_worker_order_pass_summary(
    reconciled_failed_orders: usize,
    processed_provider_hints: usize,
    maintenance_tasks: usize,
    planned_orders: usize,
    executed_orders: usize,
) {
    record_worker_order_pass_stage("reconciled_failed_orders", reconciled_failed_orders);
    record_worker_order_pass_stage("processed_provider_hints", processed_provider_hints);
    record_worker_order_pass_stage("maintenance_tasks", maintenance_tasks);
    record_worker_order_pass_stage("planned_orders", planned_orders);
    record_worker_order_pass_stage("executed_orders", executed_orders);
}

fn record_worker_order_pass_stage(stage: &'static str, count: usize) {
    gauge!(
        "tee_router_worker_order_pass_last_rows",
        "stage" => stage,
    )
    .set(count as f64);

    if count > 0 {
        counter!(
            "tee_router_worker_order_pass_rows_total",
            "stage" => stage,
        )
        .increment(count as u64);
    }
}

pub fn record_order_in_progress_queue_depth(status_counts: &[(RouterOrderStatus, u64)]) {
    let mut total = 0_u64;
    for status in IN_PROGRESS_ORDER_STATUSES {
        let count = status_counts
            .iter()
            .find_map(|(count_status, count)| (*count_status == status).then_some(*count))
            .unwrap_or(0);
        total = total.saturating_add(count);
        gauge!(
            "tee_router_order_in_progress_queue_depth",
            "status" => status.to_db_string(),
        )
        .set(count as f64);
    }

    gauge!(
        "tee_router_order_in_progress_queue_depth",
        "status" => "all",
    )
    .set(total as f64);
}

pub fn record_worker_provider_operation_hint_pass(processed_hints: usize) {
    gauge!("tee_router_worker_provider_operation_hint_pass_last_rows").set(processed_hints as f64);

    if processed_hints > 0 {
        counter!("tee_router_worker_provider_operation_hint_pass_rows_total")
            .increment(processed_hints as u64);
    }
}

pub fn record_worker_lease_event(event: &'static str, status: &'static str) {
    counter!(
        "tee_router_worker_lease_events_total",
        "event" => event,
        "status" => status,
    )
    .increment(1);
}

pub fn record_worker_active(active: bool) {
    gauge!("tee_router_worker_active").set(if active { 1.0 } else { 0.0 });
}

pub fn record_market_order_quote_requested(source_asset: &DepositAsset) {
    counter!(
        "tee_router_market_order_quote_requested_total",
        "source_chain" => chain_label(&source_asset.chain),
        "source_asset_kind" => asset_kind(&source_asset.asset),
    )
    .increment(1);
}

pub fn record_market_order_quote_success(source_asset: &DepositAsset, duration: Duration) {
    counter!(
        "tee_router_market_order_quote_success_total",
        "source_chain" => chain_label(&source_asset.chain),
        "source_asset_kind" => asset_kind(&source_asset.asset),
    )
    .increment(1);
    histogram!(
        "tee_router_market_order_quote_duration_seconds",
        "source_chain" => chain_label(&source_asset.chain),
        "source_asset_kind" => asset_kind(&source_asset.asset),
        "status" => "success",
    )
    .record(duration.as_secs_f64());
}

pub fn record_market_order_quote_no_route(
    source_asset: &DepositAsset,
    reason: &'static str,
    duration: Duration,
) {
    counter!(
        "tee_router_market_order_quote_no_route_total",
        "source_chain" => chain_label(&source_asset.chain),
        "source_asset_kind" => asset_kind(&source_asset.asset),
        "reason" => reason,
    )
    .increment(1);
    histogram!(
        "tee_router_market_order_quote_duration_seconds",
        "source_chain" => chain_label(&source_asset.chain),
        "source_asset_kind" => asset_kind(&source_asset.asset),
        "status" => "no_route",
    )
    .record(duration.as_secs_f64());
}

pub fn record_provider_quote_blocked(provider: &str, reason: &str) {
    counter!(
        "tee_router_provider_quote_blocked_total",
        "provider" => provider.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

pub fn record_provider_execution_blocked(provider: &str, state: &str, reason: &str) {
    counter!(
        "tee_router_provider_execution_blocked_total",
        "provider" => provider.to_string(),
        "state" => state.to_string(),
        "reason" => reason.to_string(),
    )
    .increment(1);
}

pub fn record_background_task_exit(task_name: &str, exit_kind: &'static str) {
    counter!(
        "tee_router_background_task_exit_total",
        "task" => task_name.to_string(),
        "exit_kind" => exit_kind,
    )
    .increment(1);
}

fn success_status(success: bool) -> &'static str {
    if success {
        "success"
    } else {
        "error"
    }
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

fn asset_kind(asset: &AssetId) -> &'static str {
    match asset {
        AssetId::Native => "native",
        AssetId::Reference(_) => "reference",
    }
}

fn chain_label(chain: &ChainId) -> &'static str {
    match chain.as_str() {
        "bitcoin" => "bitcoin",
        "evm:1" => "evm:1",
        "evm:42161" => "evm:42161",
        "evm:8453" => "evm:8453",
        "hyperliquid" => "hyperliquid",
        _ => "unsupported",
    }
}

fn vault_error_kind(error: &VaultError) -> &'static str {
    match error {
        VaultError::ChainNotSupported { .. } => "chain_not_supported",
        VaultError::InvalidAssetId { .. } => "invalid_asset_id",
        VaultError::InvalidRecoveryAddress { .. } => "invalid_recovery_address",
        VaultError::InvalidMetadata { .. } => "invalid_metadata",
        VaultError::InvalidCancellationCommitment { .. } => "invalid_cancellation_commitment",
        VaultError::InvalidCancellationSecret => "invalid_cancellation_secret",
        VaultError::RefundNotAllowed { .. } => "refund_not_allowed",
        VaultError::InvalidOrderBinding { .. } => "invalid_order_binding",
        VaultError::InvalidFundingAmount { .. } => "invalid_funding_amount",
        VaultError::InvalidFundingHint { .. } => "invalid_funding_hint",
        VaultError::FundingCheck { .. } => "funding_check",
        VaultError::FundingHintNotReady { .. } => "funding_hint_not_ready",
        VaultError::Random { .. } => "random",
        VaultError::WalletDerivation { .. } => "wallet_derivation",
        VaultError::DepositAddress { .. } => "deposit_address",
        VaultError::Database { .. } => "database",
    }
}
