//! Typed venue observer for CCTP burn attestations.
//!
//! ## Why this exists
//!
//! A CCTP burn step persists a `CctpBridge` provider operation that then waits
//! ~13-19 minutes for Circle to publish a burn attestation. Before this
//! observer existed, that operation fell into Sauron's *generic*
//! provider-operation observer, which re-submits a `PossibleProgress` hint on a
//! fixed ~35s interval regardless of whether anything changed — producing
//! hundreds of evidence-free "nudges" while nothing is actually ready.
//!
//! This observer instead polls Circle's Iris attestation API directly and only
//! submits a hint once the attestation is genuinely ready (or terminally
//! failed). While the attestation is still pending it emits nothing, so the
//! blind nudge stream is eliminated.
//!
//! ## Hint shape
//!
//! The submitted hint uses [`ProviderOperationHintKind::PossibleProgress`].
//! `router-server` maps a `PossibleProgress` hint on a `CctpBridge` operation
//! to the workflow's `CctpAttestation` hint kind (`provider_hint_shape_for_*`
//! in `router-server`'s `server.rs`), which dispatches the
//! `verify_cctp_attestation_hint` activity. That activity re-checks Iris
//! itself, so the hint is emitted **evidence-free** — there is no
//! `CctpAttestation`-typed evidence variant in `router-temporal`, and the only
//! purpose of the hint is to wake the workflow once Iris is ready.

use std::{collections::HashSet, sync::Arc, time::Duration};

use router_core::models::{
    ProviderOperationHintKind, ProviderOperationType, PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE,
};
use router_server::api::{ProviderOperationHintRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN};
use serde_json::Value;
use tokio::{
    sync::Mutex,
    task::JoinSet,
    time::{timeout, MissedTickBehavior},
};
use tracing::{debug, warn};

use crate::{
    cctp_iris::{CctpAttestationStatus, CctpIrisClient},
    error::Result,
    provider_operations::{ProviderOperationWatchStore, SharedProviderOperationWatchEntry},
    router_client::RouterClient,
};

/// Poll cadence for the CCTP attestation observer.
///
/// A CCTP attestation from an L2 burn takes ~13-19 minutes, so this is a slow
/// background poll rather than a hot loop — 45s keeps Iris request volume tiny
/// while still reacting promptly once an attestation lands.
const CCTP_ATTESTATION_OBSERVER_INTERVAL: Duration = Duration::from_secs(45);
/// Bound on concurrent in-flight Iris requests per cycle.
const CCTP_ATTESTATION_OBSERVER_CONCURRENCY: usize = 16;
/// Per-operation timeout for a single Iris poll + hint submit.
const CCTP_ATTESTATION_OBSERVE_TIMEOUT: Duration = Duration::from_secs(30);

/// Runs the CCTP burn attestation observer loop until cancelled.
///
/// Each cycle snapshots the active provider-operation watch set, filters to
/// in-flight `CctpBridge` operations, polls Iris for each, and submits a single
/// de-duplicated hint per operation once the attestation is ready or failed.
pub async fn run_cctp_attestation_observer_loop(
    iris_client: CctpIrisClient,
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
) -> Result<()> {
    let iris_client = Arc::new(iris_client);
    let router_client = Arc::new(router_client);
    // `submitted` is keyed by operation id: a ready/failed attestation is a
    // terminal verdict, so one hint per operation is enough. Entries are pruned
    // when an operation leaves the active watch set.
    let submitted = Arc::new(Mutex::new(HashSet::<uuid::Uuid>::new()));
    let mut ticker = tokio::time::interval(CCTP_ATTESTATION_OBSERVER_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        run_cctp_attestation_observer_cycle(
            &store,
            Arc::clone(&iris_client),
            Arc::clone(&router_client),
            Arc::clone(&submitted),
        )
        .await;

        ticker.tick().await;
    }
}

async fn run_cctp_attestation_observer_cycle(
    store: &ProviderOperationWatchStore,
    iris_client: Arc<CctpIrisClient>,
    router_client: Arc<RouterClient>,
    submitted: Arc<Mutex<HashSet<uuid::Uuid>>>,
) {
    let operations = store.snapshot().await;
    let active = operations
        .iter()
        .map(|operation| operation.operation_id)
        .collect::<HashSet<_>>();
    {
        let mut submitted = submitted.lock().await;
        submitted.retain(|operation_id| active.contains(operation_id));
    }

    let due = operations
        .into_iter()
        .filter(|operation| operation.operation_type == ProviderOperationType::CctpBridge)
        .collect::<Vec<_>>();
    let mut operations = due.into_iter();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < CCTP_ATTESTATION_OBSERVER_CONCURRENCY {
            let Some(operation) = operations.next() else {
                break;
            };
            // Skip operations whose attestation hint was already submitted.
            if submitted.lock().await.contains(&operation.operation_id) {
                continue;
            }
            let iris_client = Arc::clone(&iris_client);
            let router_client = Arc::clone(&router_client);
            let submitted = Arc::clone(&submitted);
            tasks.spawn(async move {
                observe_cctp_attestation_once(&iris_client, &router_client, &submitted, &operation)
                    .await;
            });
        }

        if tasks.is_empty() {
            break;
        }
        match tasks.join_next().await {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                warn!(%error, "CCTP attestation observer task failed");
            }
            None => break,
        }
    }
}

async fn observe_cctp_attestation_once(
    iris_client: &CctpIrisClient,
    router_client: &RouterClient,
    submitted: &Mutex<HashSet<uuid::Uuid>>,
    operation: &SharedProviderOperationWatchEntry,
) {
    let outcome = timeout(
        CCTP_ATTESTATION_OBSERVE_TIMEOUT,
        cctp_attestation_hint_request(iris_client, operation),
    )
    .await;
    let request = match outcome {
        Ok(Ok(Some(request))) => request,
        Ok(Ok(None)) => return,
        Ok(Err(error)) => {
            warn!(
                operation_id = %operation.operation_id,
                %error,
                "failed to poll Iris for CCTP burn attestation"
            );
            return;
        }
        Err(_) => {
            warn!(
                operation_id = %operation.operation_id,
                timeout_ms = CCTP_ATTESTATION_OBSERVE_TIMEOUT.as_millis(),
                "timed out polling Iris for CCTP burn attestation"
            );
            return;
        }
    };

    // De-dupe: claim the operation before submitting so a slow submit racing
    // the next cycle cannot double-fire. Release the claim on submit failure
    // so a transient router error is retried next cycle.
    {
        let mut submitted = submitted.lock().await;
        if !submitted.insert(operation.operation_id) {
            return;
        }
    }
    if let Err(error) = router_client.submit_provider_operation_hint(&request).await {
        submitted.lock().await.remove(&operation.operation_id);
        warn!(
            operation_id = %operation.operation_id,
            %error,
            "failed to submit CCTP attestation provider-operation hint"
        );
    }
}

/// Polls Iris for one `CctpBridge` operation and builds a hint request iff the
/// attestation is ready or terminally failed.
///
/// Returns `Ok(None)` while the attestation is still pending (the whole point
/// of this observer: no hint until something actually changed).
async fn cctp_attestation_hint_request(
    iris_client: &CctpIrisClient,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Option<ProviderOperationHintRequest>> {
    let Some(burn_tx_hash) = cctp_burn_tx_hash(operation) else {
        debug!(
            operation_id = %operation.operation_id,
            "CCTP burn operation has no burn tx hash yet; skipping Iris poll"
        );
        return Ok(None);
    };
    let Some(source_domain) = cctp_source_domain(operation) else {
        debug!(
            operation_id = %operation.operation_id,
            "CCTP burn operation request is missing source_domain; skipping Iris poll"
        );
        return Ok(None);
    };

    match iris_client
        .attestation_status(source_domain, burn_tx_hash)
        .await?
    {
        CctpAttestationStatus::Pending => Ok(None),
        CctpAttestationStatus::Ready => {
            debug!(
                operation_id = %operation.operation_id,
                burn_tx_hash,
                "CCTP burn attestation is ready; submitting hint"
            );
            Ok(Some(cctp_attestation_hint(operation)))
        }
        CctpAttestationStatus::Failed => {
            debug!(
                operation_id = %operation.operation_id,
                burn_tx_hash,
                "CCTP burn attestation terminally failed; submitting hint"
            );
            Ok(Some(cctp_attestation_hint(operation)))
        }
    }
}

/// The burn transaction hash is persisted as the operation's `provider_ref`
/// (set by `CctpProvider::post_execute`); `observed_state.burn_tx_hash` is a
/// fallback, mirroring `CctpProvider::observe_bridge_operation`.
fn cctp_burn_tx_hash(operation: &SharedProviderOperationWatchEntry) -> Option<&str> {
    operation
        .provider_ref
        .as_deref()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            operation
                .observed_state
                .get("burn_tx_hash")
                .and_then(Value::as_str)
        })
}

/// The CCTP source domain is persisted on the operation's `request` JSON by
/// `CctpProvider::execute_burn`; matches `cctp_source_domain_from_request`.
fn cctp_source_domain(operation: &SharedProviderOperationWatchEntry) -> Option<u32> {
    operation
        .request
        .get("source_domain")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

/// Builds the evidence-free `PossibleProgress` hint that wakes the workflow's
/// `verify_cctp_attestation_hint` activity. See the module docs for why the
/// hint kind is `PossibleProgress` and why no evidence is attached.
fn cctp_attestation_hint(
    operation: &SharedProviderOperationWatchEntry,
) -> ProviderOperationHintRequest {
    ProviderOperationHintRequest {
        provider_operation_id: operation.operation_id,
        execution_step_id: operation.execution_step_id,
        source: PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE.to_string(),
        hint_kind: ProviderOperationHintKind::PossibleProgress,
        evidence: Value::Object(serde_json::Map::new()),
        idempotency_key: Some(cctp_attestation_idempotency_key(operation.operation_id)),
    }
}

/// One hint per operation, so the idempotency key is just the operation id.
fn cctp_attestation_idempotency_key(operation_id: uuid::Uuid) -> String {
    let key = format!("sauron:cctp-attestation:{operation_id}");
    debug_assert!(key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_operations::ProviderOperationWatchEntry;
    use router_core::models::ProviderOperationStatus;
    use router_temporal::WorkflowStepId;
    use serde_json::json;
    use std::sync::Arc;
    use uuid::Uuid;

    fn cctp_operation(
        provider_ref: Option<&str>,
        request: Value,
        observed_state: Value,
    ) -> SharedProviderOperationWatchEntry {
        Arc::new(ProviderOperationWatchEntry {
            operation_id: Uuid::now_v7(),
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "cctp".to_string(),
            operation_type: ProviderOperationType::CctpBridge,
            provider_ref: provider_ref.map(str::to_string),
            status: ProviderOperationStatus::WaitingExternal,
            request,
            response: json!({}),
            observed_state,
            execution_step_request: json!({}),
            updated_at: utc::now(),
        })
    }

    #[test]
    fn burn_tx_hash_prefers_provider_ref() {
        let operation = cctp_operation(
            Some("0xfromref"),
            json!({}),
            json!({"burn_tx_hash": "0xfromstate"}),
        );
        assert_eq!(cctp_burn_tx_hash(&operation), Some("0xfromref"));
    }

    #[test]
    fn burn_tx_hash_falls_back_to_observed_state() {
        let operation = cctp_operation(None, json!({}), json!({"burn_tx_hash": "0xfromstate"}));
        assert_eq!(cctp_burn_tx_hash(&operation), Some("0xfromstate"));
    }

    #[test]
    fn burn_tx_hash_absent_when_unpersisted() {
        let operation = cctp_operation(None, json!({}), json!({}));
        assert_eq!(cctp_burn_tx_hash(&operation), None);
        let blank = cctp_operation(Some(""), json!({}), json!({}));
        assert_eq!(cctp_burn_tx_hash(&blank), None);
    }

    #[test]
    fn source_domain_decoded_from_request() {
        let operation = cctp_operation(Some("0xburn"), json!({"source_domain": 6}), json!({}));
        assert_eq!(cctp_source_domain(&operation), Some(6));
        let missing = cctp_operation(Some("0xburn"), json!({}), json!({}));
        assert_eq!(cctp_source_domain(&missing), None);
    }

    #[test]
    fn hint_is_evidence_free_possible_progress_with_bounded_key() {
        let operation = cctp_operation(Some("0xburn"), json!({"source_domain": 6}), json!({}));
        let hint = cctp_attestation_hint(&operation);
        assert_eq!(hint.hint_kind, ProviderOperationHintKind::PossibleProgress);
        assert_eq!(hint.provider_operation_id, operation.operation_id);
        assert_eq!(hint.execution_step_id, operation.execution_step_id);
        assert_eq!(hint.evidence, json!({}));
        assert_eq!(
            hint.source,
            PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE.to_string()
        );
        let key = hint.idempotency_key.expect("hint carries idempotency key");
        assert!(key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
        assert!(key.contains(&operation.operation_id.to_string()));
    }
}
