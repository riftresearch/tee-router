//! Per-step provider operation idempotency guard.
//!
//! Every step that produces a [`ProviderExecutionIntent`] funnels through
//! [`execute_idempotent_step`], which:
//!
//! 1. Derives a deterministic idempotency key from `(order_id, step_type, step_index)`.
//!    Those three fields uniquely identify the logical work the step represents
//!    across retries (a retry-attempt step shares the same `(order_id, step_index)`
//!    as the original step it supersedes), so the key collapses retries onto the
//!    same provider operation row.
//! 2. Looks up `order_provider_operations` by `(provider, operation_type, key)` —
//!    if found, short-circuits with [`existing_operation_completion`] *before*
//!    contacting the venue, so retries never re-fire side effects.
//! 3. Otherwise asks the per-step-type [`IdempotentStepExecutor`] to build the
//!    intent (this may call the provider trait), stamps the key onto the intent's
//!    operation, and persists the operation row *before* the side effect fires
//!    (which happens later in `prepare_provider_completion`). On the unique-key
//!    conflict path (a racing worker won) we re-read the persisted row and
//!    short-circuit just like (2).
//!
//! The historical per-step `*_existing_operation_completion` and
//! `persist_*_intent_before_side_effect` helpers (originally specialised for
//! `unit_deposit` / `unit_withdrawal`) are now exactly one generic each.
//! Per-step-type code shrinks to a single `IdempotentStepExecutor` impl whose
//! body is dominated by the venue-specific intent build.

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
        set_provider_intent_operation_idempotency_key, StepCompletion, StepDispatchResult,
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

    if let Some(operation) = lookup_existing_operation(deps, step, operation_type, &idempotency_key).await? {
        return Ok(existing_operation_completion(step, operation, idempotency_key));
    }

    let mut intent = executor.build_intent(deps, step, &idempotency_key).await?;
    set_provider_intent_operation_idempotency_key(&mut intent, idempotency_key.clone());

    if let Some(operation) =
        persist_intent_before_side_effect(deps, step, &intent, operation_type).await?
    {
        return Ok(existing_operation_completion(step, operation, idempotency_key));
    }

    Ok(StepDispatchResult::ProviderIntent(intent))
}

/// `(order, step_type, step_index)` uniquely identifies a step across retries
/// because retry attempts re-create the step with the same `step_index`. We
/// include the step_type tag for human readability and as a defence against
/// operator confusion (e.g. comparing keys across step types in a SQL prompt).
fn step_idempotency_key(step: &OrderExecutionStep, operation_type: ProviderOperationType) -> String {
    format!(
        "order:{}:{}:step:{}",
        step.order_id,
        operation_type.to_db_string(),
        step.step_index,
    )
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
    let state = intent_state(intent);
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
        provider_state_records(step, state, &json!({ "kind": response_kind }))
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

fn intent_state(intent: &ProviderExecutionIntent) -> &ProviderExecutionState {
    match intent {
        ProviderExecutionIntent::ProviderOnly { state, .. }
        | ProviderExecutionIntent::CustodyAction { state, .. }
        | ProviderExecutionIntent::CustodyActions { state, .. } => state,
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
    let state = ProviderExecutionState {
        operation: Some(ProviderOperationIntent {
            operation_type: operation.operation_type,
            status: operation.status,
            provider_ref: operation.provider_ref.clone(),
            idempotency_key: operation
                .idempotency_key
                .clone()
                .or_else(|| Some(idempotency_key.clone())),
            request: Some(operation.request.clone()),
            response: Some(operation.response.clone()),
            observed_state: Some(operation.observed_state.clone()),
        }),
        addresses: Vec::new(),
    };
    let response_kind = format!("{}_idempotency_reuse", operation.operation_type.to_db_string());
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

