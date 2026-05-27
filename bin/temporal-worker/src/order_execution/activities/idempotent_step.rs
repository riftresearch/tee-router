//! Per-step provider operation idempotency guard.
//!
//! Every step that produces a [`ProviderExecutionIntent`] funnels through
//! [`execute_idempotent_step`], which:
//!
//! 1. Derives a deterministic idempotency key from `(order_id, step_type, step_index)`.
//!    Those three fields uniquely identify the logical work the step represents
//!    across normal retries (a retry-attempt step shares the same
//!    `(order_id, step_index)` as the original step it replaces), so the key
//!    collapses retries onto the same provider operation row. Refund recovery
//!    steps additionally include `execution_attempt_id`, because a later manual
//!    refund may intentionally materialize a new route at the same step index
//!    after a previous refund route failed.
//! 2. Looks up `order_provider_operations` by `(provider, operation_type, key)` —
//!    if found, short-circuits with [`existing_operation_completion`] *before*
//!    contacting the venue, so retries never re-fire side effects.
//! 3. Otherwise asks the per-step-type [`IdempotentStepExecutor`] to build the
//!    intent (this may call the provider trait), stamps the key onto the intent's
//!    operation, and persists a **pre-side-effect** provider-operation row with
//!    status `Planned` *before* the side effect fires (which happens later in
//!    `prepare_provider_completion`). After a successful side effect, the later
//!    persist path upserts the real post-fire status (`Submitted` /
//!    `WaitingExternal` / `Completed`). On the unique-key conflict path (a
//!    racing worker won) we re-read the persisted row and short-circuit just like
//!    (2).
//!
//! The historical per-step `*_existing_operation_completion` and
//! `persist_*_intent_before_side_effect` helpers (originally specialised for
//! `unit_deposit` / `unit_withdrawal`) are now exactly one generic each.
//! Per-step-type code shrinks to a single `IdempotentStepExecutor` impl whose
//! body is dominated by the venue-specific intent build.

use alloy::primitives::U256;
use router_core::{
    models::{
        OrderExecutionStep, OrderProviderOperation, ProviderOperationStatus, ProviderOperationType,
    },
    services::{ProviderExecutionIntent, ProviderExecutionState, ProviderOperationIntent},
};
use serde_json::json;

use super::{
    execution::{
        provider_execute_error, provider_operation_tx_hash, provider_state_records,
        set_provider_intent_operation_idempotency_key, AuthoritativeOutputBalance, StepCompletion,
        StepDispatchResult,
    },
    OrderActivityDeps, StepExecutionOutcome,
};
use crate::order_execution::error::OrderActivityError;

/// Per-step-type plug that builds a provider intent for an idempotent step.
///
/// Implementations carry no state beyond what they need to call the provider —
/// the framework owns the lookup / persist / short-circuit dance.
pub(super) trait IdempotentStepExecutor {
    /// The persisted operation type. This drives both the lookup index and the
    /// pre-side-effect persist record. Implemented as a method (not a const) so
    /// a single executor struct can carry the operation type as state — the
    /// bridge / exchange families share their executor implementation across
    /// several step types and would otherwise need one struct per variant just
    /// to vary a constant.
    fn operation_type(&self) -> ProviderOperationType;

    /// Build the provider execution intent for this step. Called only when the
    /// idempotency lookup found no prior operation, so this is the path that
    /// is allowed to perform expensive work (attestation fetches, planner calls,
    /// external HTTP, etc.).
    ///
    /// `idempotency_key` is supplied for executors that thread it onto the
    /// outbound provider request (e.g. unit deposit's HyperUnit `/gen` request).
    /// The framework also stamps the key onto the intent's operation row after
    /// this returns, so executors don't have to.
    async fn build_intent(
        self,
        deps: &OrderActivityDeps,
        step: &OrderExecutionStep,
        idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError>;
}

/// Run an idempotent step end-to-end: look up by key, build intent if needed,
/// persist the operation row before the side effect, return either a short-circuit
/// completion or the intent for the executor to fire.
pub(super) async fn execute_idempotent_step<E>(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    executor: E,
) -> Result<StepDispatchResult, OrderActivityError>
where
    E: IdempotentStepExecutor,
{
    let operation_type = executor.operation_type();
    let idempotency_key = step_idempotency_key(step, operation_type);

    if let Some(operation) =
        lookup_existing_operation(deps, step, operation_type, &idempotency_key).await?
    {
        // A `Failed` row means a prior attempt persisted the operation but its
        // side effect never landed (see `fail_planned_provider_operation` in
        // `execution.rs`). Re-reading it through `existing_operation_completion`
        // would map it to a `Waiting` outcome and the workflow would wait
        // forever for an external event that will never arrive. Surface it as a
        // non-retryable failure so `classify_step_failure` routes to a refund.
        // `Planned` / `Submitted` / `WaitingExternal` are genuinely ambiguous
        // (a worker may have crashed mid-broadcast leaving a real in-flight tx)
        // and stay on the existing short-circuit path — that case is owned by
        // the separate stale-running-step mechanism.
        if operation.status == ProviderOperationStatus::Failed {
            if let Some(completion) = recover_failed_cctp_receive_completion(
                deps,
                step,
                &operation,
                idempotency_key.clone(),
            )
            .await?
            {
                return Ok(StepDispatchResult::Complete(completion));
            }
            return Err(OrderActivityError::provider_operation_lost(format!(
                "provider operation {} for step {} did not land on a prior attempt",
                operation.id, step.id
            )));
        }
        return Ok(existing_operation_completion(
            step,
            operation,
            idempotency_key,
        ));
    }

    let mut intent = executor.build_intent(deps, step, &idempotency_key).await?;
    set_provider_intent_operation_idempotency_key(&mut intent, idempotency_key.clone());

    if let Some(operation) =
        persist_intent_before_side_effect(deps, step, &intent, operation_type).await?
    {
        return Ok(existing_operation_completion(
            step,
            operation,
            idempotency_key,
        ));
    }

    Ok(StepDispatchResult::ProviderIntent(intent))
}

/// `(order, operation_type, step_index)` uniquely identifies normal retry work
/// because retry attempts re-create the step with the same `step_index`.
/// Refund recovery steps add the attempt id so a later recovery route does not
/// inherit a stale provider operation from a failed refund route.
pub(super) fn step_idempotency_key(
    step: &OrderExecutionStep,
    operation_type: ProviderOperationType,
) -> String {
    if refund_recovery_step(step) {
        if let Some(attempt_id) = step.execution_attempt_id {
            return format!(
                "order:{}:attempt:{}:{}:step:{}",
                step.order_id,
                attempt_id,
                operation_type.to_db_string(),
                step.step_index,
            );
        }
    }
    format!(
        "order:{}:{}:step:{}",
        step.order_id,
        operation_type.to_db_string(),
        step.step_index,
    )
}

fn refund_recovery_step(step: &OrderExecutionStep) -> bool {
    step.details.get("refund_kind").is_some()
        || step
            .provider_ref
            .as_deref()
            .is_some_and(|provider_ref| provider_ref.starts_with("refund-quote-"))
}

async fn lookup_existing_operation(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    operation_type: ProviderOperationType,
    idempotency_key: &str,
) -> Result<Option<OrderProviderOperation>, OrderActivityError> {
    deps.db
        .orders()
        .get_provider_operation_by_idempotency_key(&step.provider, operation_type, idempotency_key)
        .await
        .map_err(OrderActivityError::db_query)
}

/// Persist the provider operation row before the side effect fires. Returns
/// `Some(existing)` when the unique-key conflict path was taken (a racing
/// worker had already inserted the row); the caller should short-circuit on
/// that prior row instead of firing a duplicate side effect.
async fn persist_intent_before_side_effect(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    intent: &ProviderExecutionIntent,
    operation_type: ProviderOperationType,
) -> Result<Option<OrderProviderOperation>, OrderActivityError> {
    let state = pre_side_effect_state(intent_state(intent));
    let Some(operation_intent) = state.operation.as_ref() else {
        return Ok(None);
    };
    if operation_intent.operation_type != operation_type {
        return Ok(None);
    }

    let response_kind = format!(
        "{}_provider_operation_pre_side_effect",
        operation_type.to_db_string()
    );
    let (operation, mut addresses) =
        provider_state_records(step, &state, &json!({ "kind": response_kind }))
            .map_err(|source| provider_execute_error(&step.provider, source))?;
    let Some(operation) = operation else {
        return Ok(None);
    };

    let candidate_operation_id = operation.id;
    let provider_operation_id = deps
        .db
        .orders()
        .upsert_provider_operation(&operation)
        .await
        .map_err(OrderActivityError::db_query)?;

    if provider_operation_id == candidate_operation_id {
        // Inserted: link any addresses that came with the intent (only unit
        // deposit currently emits provider addresses) and proceed to fire.
        for address in &mut addresses {
            address.provider_operation_id = Some(provider_operation_id);
            deps.db
                .orders()
                .upsert_provider_address(address)
                .await
                .map_err(OrderActivityError::db_query)?;
        }
        return Ok(None);
    }

    // Conflict: a racing worker won the insert. Re-read its row so we can
    // short-circuit on the same provider operation it persisted.
    deps.db
        .orders()
        .get_provider_operation(provider_operation_id)
        .await
        .map(Some)
        .map_err(OrderActivityError::db_query)
}

pub(super) fn intent_state(intent: &ProviderExecutionIntent) -> &ProviderExecutionState {
    match intent {
        ProviderExecutionIntent::ProviderOnly { state, .. }
        | ProviderExecutionIntent::CustodyAction { state, .. }
        | ProviderExecutionIntent::CustodyActions { state, .. } => state,
    }
}
fn pre_side_effect_state(state: &ProviderExecutionState) -> ProviderExecutionState {
    let mut state = state.clone();
    if let Some(operation) = state.operation.as_mut() {
        operation.status = ProviderOperationStatus::Planned;
    }
    state
}

async fn recover_failed_cctp_receive_completion(
    deps: &OrderActivityDeps,
    step: &OrderExecutionStep,
    operation: &OrderProviderOperation,
    idempotency_key: String,
) -> Result<Option<StepCompletion>, OrderActivityError> {
    if step.step_type != router_core::models::OrderExecutionStepType::CctpReceive {
        return Ok(None);
    }
    let Some(output_asset) = step.output_asset.as_ref() else {
        return Ok(None);
    };
    let expected_amount = operation
        .request
        .get("amount")
        .and_then(decimal_string)
        .or_else(|| step.request.get("amount").and_then(decimal_string))
        .ok_or_else(|| {
            provider_execute_error(
                &step.provider,
                "cctp receive amount is missing on failed operation",
            )
        })?;
    let Some(source_custody_vault_id) = step
        .request
        .get("source_custody_vault_id")
        .and_then(|value| value.as_str())
    else {
        return Ok(None);
    };
    let source_custody_vault_id =
        uuid::Uuid::parse_str(source_custody_vault_id).map_err(|source| {
            provider_execute_error(
                &step.provider,
                format!("invalid cctp receive source_custody_vault_id: {source}"),
            )
        })?;
    let vault = deps
        .db
        .orders()
        .get_custody_vault(source_custody_vault_id)
        .await
        .map_err(OrderActivityError::db_query)?;
    if vault.chain != output_asset.chain || vault.asset.as_ref() != Some(&output_asset.asset) {
        return Ok(None);
    }
    let balance_amount = deps
        .custody_action_executor
        .custody_vault_balance_raw(&vault)
        .await
        .map_err(|source| {
            OrderActivityError::custody_action("read cctp receive destination balance", source)
        })?;
    let observed = U256::from_str_radix(&balance_amount, 10).map_err(|source| {
        provider_execute_error(
            &step.provider,
            format!("invalid destination balance {balance_amount:?}: {source}"),
        )
    })?;
    let expected = U256::from_str_radix(expected_amount, 10).map_err(|source| {
        provider_execute_error(
            &step.provider,
            format!("invalid expected cctp receive amount {expected_amount:?}: {source}"),
        )
    })?;
    if observed < expected {
        return Ok(None);
    }
    let balance = AuthoritativeOutputBalance {
        amount: balance_amount,
        location: "custody_vault",
        custody_vault_id: Some(vault.id),
        address: Some(vault.address.clone()),
        chain_id: output_asset.chain.to_string(),
        asset_id: output_asset.asset.to_string(),
        details: json!({}),
    };
    let observed_state = json!({
        "kind": "cctp_receive_already_claimed",
        "reason": "destination_custody_vault_already_funded",
        "recovered_from_failed_operation_id": operation.id,
        "chain_id": balance.chain_id,
        "asset_id": balance.asset_id,
        "balance": balance.amount,
        "address": balance.address,
    });
    let mut provider_state = provider_state_from_operation(operation, Some(idempotency_key));
    if let Some(existing_operation) = provider_state.operation.as_mut() {
        existing_operation.status = ProviderOperationStatus::Completed;
        existing_operation.provider_ref = Some(format!("cctp_receive_already_claimed:{}", step.id));
        existing_operation.observed_state = Some(observed_state.clone());
        existing_operation.response = Some(json!({
            "kind": "cctp_receive_already_claimed",
            "amount_out": balance.amount,
        }));
    }
    Ok(Some(StepCompletion {
        response: json!({
            "kind": "cctp_receive_already_claimed",
            "provider": &step.provider,
            "step_id": step.id,
            "custody_vault_id": vault.id,
            "observed_state": observed_state,
        }),
        tx_hash: None,
        provider_state,
        outcome: StepExecutionOutcome::Completed,
    }))
}

fn decimal_string(value: &serde_json::Value) -> Option<&str> {
    value
        .as_str()
        .filter(|raw| !raw.is_empty() && raw.chars().all(|ch| ch.is_ascii_digit()))
}

fn provider_state_from_operation(
    operation: &OrderProviderOperation,
    fallback_idempotency_key: Option<String>,
) -> ProviderExecutionState {
    ProviderExecutionState {
        operation: Some(ProviderOperationIntent {
            operation_type: operation.operation_type,
            status: operation.status,
            provider_ref: operation.provider_ref.clone(),
            idempotency_key: operation
                .idempotency_key
                .clone()
                .or(fallback_idempotency_key),
            request: Some(operation.request.clone()),
            response: Some(operation.response.clone()),
            observed_state: Some(operation.observed_state.clone()),
        }),
        addresses: Vec::new(),
    }
}

/// Build the short-circuit `StepCompletion` for an existing provider operation
/// row. Identical shape across all step types — the only thing that varies is
/// the operation type tag in the response JSON, which we derive from the row.
fn existing_operation_completion(
    step: &OrderExecutionStep,
    operation: OrderProviderOperation,
    idempotency_key: String,
) -> StepDispatchResult {
    let outcome = if operation.status == ProviderOperationStatus::Completed {
        StepExecutionOutcome::Completed
    } else {
        StepExecutionOutcome::Waiting
    };
    let tx_hash = provider_operation_tx_hash(&operation);
    let state = provider_state_from_operation(&operation, Some(idempotency_key.clone()));
    let response_kind = format!(
        "{}_idempotency_reuse",
        operation.operation_type.to_db_string()
    );
    StepDispatchResult::Complete(StepCompletion {
        response: json!({
            "kind": response_kind,
            "provider": &step.provider,
            "step_id": step.id,
            "provider_operation_id": operation.id,
            "provider_ref": &operation.provider_ref,
            "idempotency_key": idempotency_key,
            "status": operation.status.to_db_string(),
        }),
        tx_hash,
        provider_state: state,
        outcome,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use router_core::models::ProviderOperationType;
    use serde_json::json;

    #[test]
    fn pre_side_effect_state_forces_planned_status() {
        let state = ProviderExecutionState {
            operation: Some(ProviderOperationIntent {
                operation_type: ProviderOperationType::CctpReceive,
                status: ProviderOperationStatus::Submitted,
                provider_ref: Some("ref".to_string()),
                idempotency_key: Some("key".to_string()),
                request: Some(json!({"k":"v"})),
                response: Some(json!({"kind":"submitted"})),
                observed_state: Some(json!({"seen":true})),
            }),
            addresses: Vec::new(),
        };

        let persisted = pre_side_effect_state(&state);

        assert_eq!(
            persisted
                .operation
                .as_ref()
                .map(|operation| operation.status),
            Some(ProviderOperationStatus::Planned)
        );
        assert_eq!(
            persisted
                .operation
                .as_ref()
                .and_then(|operation| operation.provider_ref.as_deref()),
            Some("ref")
        );
        assert_eq!(
            state.operation.as_ref().map(|operation| operation.status),
            Some(ProviderOperationStatus::Submitted)
        );
    }
}
