pub mod bitcoin;
pub mod evm_erc20;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use metrics::{gauge, histogram};
use reqwest::StatusCode;
use router_primitives::{ChainType, TokenIdentifier};
use router_server::{
    api::{DetectorHintEnvelope, DetectorHintRequest, DetectorHintTarget},
    models::ProviderOperationHintKind,
};
use serde_json::json;
use snafu::ResultExt;
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    cursor::CursorRepository,
    error::{DiscoveryTaskJoinSnafu, Error, Result},
    router_client::RouterClient,
    watch::{SharedWatchEntry, WatchEntry, WatchStore, WatchTarget},
};

const SAURON_INDEXED_LOOKUP_DURATION_SECONDS: &str = "sauron_indexed_lookup_duration_seconds";
const SAURON_BLOCK_SCAN_DURATION_SECONDS: &str = "sauron_block_scan_duration_seconds";
const SAURON_INDEXED_LOOKUP_QUEUE_DEPTH: &str = "sauron_indexed_lookup_queue_depth";
const SAURON_INDEXED_LOOKUP_INFLIGHT: &str = "sauron_indexed_lookup_inflight";
const SAURON_BLOCK_SCAN_DETECTIONS: &str = "sauron_block_scan_detections";
const SUBMISSION_RETRY_BASE_DELAY: Duration = Duration::from_secs(5);
const SUBMISSION_RETRY_MAX_DELAY: Duration = Duration::from_secs(5 * 60);
const SUBMISSION_RETRY_JITTER_MAX_MILLIS: u64 = 1_000;

#[derive(Clone)]
pub struct DiscoveryContext {
    pub watches: WatchStore,
    pub cursors: CursorRepository,
    pub router_client: RouterClient,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectedDeposit {
    pub watch_target: WatchTarget,
    pub watch_id: Uuid,
    pub source_chain: ChainType,
    pub source_token: TokenIdentifier,
    pub address: String,
    pub tx_hash: String,
    pub transfer_index: u64,
    pub amount: U256,
    pub observed_at: DateTime<Utc>,
    pub indexer_candidate_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockCursor {
    pub height: u64,
    pub hash: String,
}

#[derive(Debug, Clone)]
pub struct BlockScan {
    pub new_cursor: BlockCursor,
    pub detections: Vec<DetectedDeposit>,
}

#[derive(Debug, Clone)]
struct PendingSubmission {
    detected: DetectedDeposit,
    attempts: u32,
    next_attempt_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SubmissionOutcome {
    Submitted,
    RetryableFailure(String),
    TerminalFailure,
    AlreadyTracked,
}

#[async_trait]
pub trait DiscoveryBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn chain(&self) -> ChainType;
    fn poll_interval(&self) -> Duration;
    fn indexed_lookup_concurrency(&self) -> usize;

    async fn sync_watches(&self, _watches: &[SharedWatchEntry]) -> Result<()> {
        Ok(())
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>>;

    async fn current_cursor(&self) -> Result<BlockCursor>;

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan>;

    async fn mark_detection_submitted(&self, _detected: &DetectedDeposit) -> Result<()> {
        Ok(())
    }

    async fn release_detection(&self, _detected: &DetectedDeposit, _error: &str) -> Result<()> {
        Ok(())
    }
}

pub async fn run_backends(
    backends: Vec<Arc<dyn DiscoveryBackend>>,
    context: DiscoveryContext,
) -> Result<()> {
    let mut join_set = JoinSet::new();

    for backend in backends {
        let backend_name = backend.name();
        let context = context.clone();
        join_set.spawn(async move {
            info!(backend = backend_name, "Starting Sauron discovery backend");
            run_backend_loop(backend, context).await
        });
    }

    while let Some(join_result) = join_set.join_next().await {
        let backend_result = join_result.context(DiscoveryTaskJoinSnafu)?;
        backend_result?;
    }

    Ok(())
}

async fn run_backend_loop(
    backend: Arc<dyn DiscoveryBackend>,
    context: DiscoveryContext,
) -> Result<()> {
    let mut indexed_lookup_tasks = JoinSet::new();
    let mut indexed_lookup_backfill = IndexedLookupBackfillState::default();
    let mut reported_candidates = HashMap::new();
    let mut pending_submissions = HashMap::new();
    let mut cursor = initial_cursor(backend.as_ref(), &context).await?;

    let mut ticker = tokio::time::interval(backend.poll_interval());
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let backend_watches = context.watches.snapshot_for_chain(backend.chain()).await;
        if let Err(error) = backend.sync_watches(&backend_watches).await {
            warn!(
                backend = backend.name(),
                %error,
                "Failed to sync backend-local watch state; keeping previous snapshot"
            );
        }
        let backend_watch_map = backend_watches
            .iter()
            .map(|watch| (watch.watch_id, watch.clone()))
            .collect::<HashMap<_, _>>();
        let current_watch_versions: HashMap<Uuid, DateTime<Utc>> = backend_watch_map
            .iter()
            .map(|(watch_id, watch)| (*watch_id, watch.updated_at))
            .collect();
        let active_watch_ids = current_watch_versions
            .keys()
            .copied()
            .collect::<HashSet<_>>();

        reported_candidates.retain(|watch_id, _| active_watch_ids.contains(watch_id));
        pending_submissions.retain(|watch_id, _| active_watch_ids.contains(watch_id));
        indexed_lookup_backfill.retain_active(&active_watch_ids);

        drain_indexed_lookup_tasks(
            &mut indexed_lookup_tasks,
            &current_watch_versions,
            &mut indexed_lookup_backfill,
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await?;

        retry_pending_submissions(
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await;

        indexed_lookup_backfill.sync_snapshot(&backend_watch_map);
        indexed_lookup_backfill
            .requeue_pending_submissions(&backend_watch_map, pending_submissions.keys().copied());
        spawn_indexed_lookup_tasks(
            backend.clone(),
            &backend_watch_map,
            &mut indexed_lookup_backfill,
            &mut indexed_lookup_tasks,
        );
        gauge!(
            SAURON_INDEXED_LOOKUP_QUEUE_DEPTH,
            "backend" => backend.name().to_string(),
        )
        .set(indexed_lookup_backfill.queue_len() as f64);
        gauge!(
            SAURON_INDEXED_LOOKUP_INFLIGHT,
            "backend" => backend.name().to_string(),
        )
        .set(indexed_lookup_backfill.inflight_len() as f64);

        if backend_watches.is_empty() {
            cursor = backend.current_cursor().await?;
            context.cursors.save(backend.name(), &cursor).await?;
            continue;
        }

        let scan_started = Instant::now();
        match backend.scan_new_blocks(&cursor, &backend_watches).await {
            Ok(scan) => {
                histogram!(
                    SAURON_BLOCK_SCAN_DURATION_SECONDS,
                    "backend" => backend.name().to_string(),
                )
                .record(scan_started.elapsed().as_secs_f64());
                cursor = scan.new_cursor;
                context.cursors.save(backend.name(), &cursor).await?;
                gauge!(
                    SAURON_BLOCK_SCAN_DETECTIONS,
                    "backend" => backend.name().to_string(),
                )
                .set(scan.detections.len() as f64);
                for detected in scan.detections {
                    let outcome = report_detected_deposit(
                        &context,
                        backend.name(),
                        &detected,
                        &mut pending_submissions,
                        &mut reported_candidates,
                    )
                    .await;
                    handle_submission_outcome(backend.as_ref(), &detected, outcome).await;
                }
            }
            Err(error) => {
                warn!(
                    backend = backend.name(),
                    error = %error,
                    "Block scan failed; keeping existing cursor"
                );
                histogram!(
                    SAURON_BLOCK_SCAN_DURATION_SECONDS,
                    "backend" => backend.name().to_string(),
                )
                .record(scan_started.elapsed().as_secs_f64());
            }
        }

        drain_indexed_lookup_tasks(
            &mut indexed_lookup_tasks,
            &current_watch_versions,
            &mut indexed_lookup_backfill,
            &context,
            backend.name(),
            &mut pending_submissions,
            &mut reported_candidates,
        )
        .await?;
    }
}

async fn initial_cursor(
    backend: &dyn DiscoveryBackend,
    context: &DiscoveryContext,
) -> Result<BlockCursor> {
    match context.cursors.load(backend.name()).await? {
        Some(cursor) => {
            info!(
                backend = backend.name(),
                height = cursor.height,
                hash = %cursor.hash,
                "Loaded persisted Sauron discovery cursor"
            );
            Ok(cursor)
        }
        None => {
            let cursor = backend.current_cursor().await?;
            context.cursors.save(backend.name(), &cursor).await?;
            info!(
                backend = backend.name(),
                height = cursor.height,
                hash = %cursor.hash,
                "Initialized Sauron discovery cursor at current chain tip"
            );
            Ok(cursor)
        }
    }
}

#[derive(Debug, Default)]
struct IndexedLookupBackfillState {
    completed_versions: HashMap<Uuid, DateTime<Utc>>,
    queued_versions: HashMap<Uuid, DateTime<Utc>>,
    inflight_versions: HashMap<Uuid, DateTime<Utc>>,
    queue: VecDeque<(Uuid, DateTime<Utc>)>,
}

impl IndexedLookupBackfillState {
    fn retain_active(&mut self, active_watch_ids: &HashSet<Uuid>) {
        self.completed_versions
            .retain(|watch_id, _| active_watch_ids.contains(watch_id));
        self.queued_versions
            .retain(|watch_id, _| active_watch_ids.contains(watch_id));
        self.inflight_versions
            .retain(|watch_id, _| active_watch_ids.contains(watch_id));
        self.queue
            .retain(|(watch_id, _)| active_watch_ids.contains(watch_id));
    }

    fn sync_snapshot(&mut self, watches: &HashMap<Uuid, SharedWatchEntry>) {
        for (watch_id, watch) in watches {
            let version = watch.updated_at;

            if self.completed_versions.get(watch_id) == Some(&version)
                || self.queued_versions.get(watch_id) == Some(&version)
                || self.inflight_versions.get(watch_id) == Some(&version)
            {
                continue;
            }

            self.queue.push_back((*watch_id, version));
            self.queued_versions.insert(*watch_id, version);
        }
    }

    fn requeue_pending_submissions<I>(
        &mut self,
        watches: &HashMap<Uuid, SharedWatchEntry>,
        pending_watch_ids: I,
    ) where
        I: IntoIterator<Item = Uuid>,
    {
        for watch_id in pending_watch_ids {
            let Some(watch) = watches.get(&watch_id) else {
                continue;
            };
            let version = watch.updated_at;

            if self.queued_versions.get(&watch_id) == Some(&version)
                || self.inflight_versions.get(&watch_id) == Some(&version)
            {
                continue;
            }

            self.queue.push_back((watch_id, version));
            self.queued_versions.insert(watch_id, version);
        }
    }

    fn take_ready(
        &mut self,
        watches: &HashMap<Uuid, SharedWatchEntry>,
        count: usize,
    ) -> Vec<(SharedWatchEntry, DateTime<Utc>)> {
        let mut ready = Vec::with_capacity(count);

        while ready.len() < count {
            let Some((watch_id, version)) = self.queue.pop_front() else {
                break;
            };
            self.queued_versions.remove(&watch_id);

            let Some(watch) = watches.get(&watch_id) else {
                continue;
            };
            if watch.updated_at != version {
                continue;
            }

            self.inflight_versions.insert(watch_id, version);
            ready.push((watch.clone(), version));
        }

        ready
    }

    fn finish(
        &mut self,
        watch_id: Uuid,
        version: DateTime<Utc>,
        current_watch_versions: &HashMap<Uuid, DateTime<Utc>>,
    ) -> bool {
        self.inflight_versions.remove(&watch_id);

        if current_watch_versions.get(&watch_id) == Some(&version) {
            self.completed_versions.insert(watch_id, version);
            return true;
        }

        false
    }

    fn queue_len(&self) -> usize {
        self.queue.len()
    }

    fn inflight_len(&self) -> usize {
        self.inflight_versions.len()
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexedLookupBackfillBenchmarkStats {
    pub queued: usize,
    pub inflight: usize,
    pub ready: usize,
}

#[doc(hidden)]
pub struct IndexedLookupBackfillBenchmarkScenario {
    backend_watch_map: HashMap<Uuid, SharedWatchEntry>,
    current_watch_versions: HashMap<Uuid, DateTime<Utc>>,
    active_watch_ids: HashSet<Uuid>,
    backfill_state: IndexedLookupBackfillState,
    ready: usize,
}

#[doc(hidden)]
impl IndexedLookupBackfillBenchmarkScenario {
    pub fn stats(&self) -> IndexedLookupBackfillBenchmarkStats {
        IndexedLookupBackfillBenchmarkStats {
            queued: self.backfill_state.queue_len(),
            inflight: self.backfill_state.inflight_len(),
            ready: self.ready,
        }
    }

    pub fn watch_count(&self) -> usize {
        self.backend_watch_map.len()
    }

    pub fn current_watch_versions_count(&self) -> usize {
        self.current_watch_versions.len()
    }

    pub fn active_watch_ids_count(&self) -> usize {
        self.active_watch_ids.len()
    }
}

#[doc(hidden)]
pub fn benchmark_seed_indexed_lookup_backfill(watches: &[SharedWatchEntry]) -> usize {
    benchmark_prepare_indexed_lookup_backfill(watches)
        .stats()
        .queued
}

#[doc(hidden)]
pub fn benchmark_schedule_initial_indexed_lookups(
    watches: &[SharedWatchEntry],
    concurrency: usize,
) -> IndexedLookupBackfillBenchmarkStats {
    benchmark_prepare_initial_indexed_lookups(watches, concurrency).stats()
}

#[doc(hidden)]
pub fn benchmark_prepare_indexed_lookup_backfill(
    watches: &[SharedWatchEntry],
) -> IndexedLookupBackfillBenchmarkScenario {
    let backend_watch_map = watches
        .iter()
        .map(|watch| (watch.watch_id, watch.clone()))
        .collect::<HashMap<_, _>>();
    let current_watch_versions = backend_watch_map
        .iter()
        .map(|(watch_id, watch)| (*watch_id, watch.updated_at))
        .collect::<HashMap<_, _>>();
    let active_watch_ids = current_watch_versions
        .keys()
        .copied()
        .collect::<HashSet<_>>();
    let mut backfill_state = IndexedLookupBackfillState::default();
    backfill_state.sync_snapshot(&backend_watch_map);

    IndexedLookupBackfillBenchmarkScenario {
        backend_watch_map,
        current_watch_versions,
        active_watch_ids,
        backfill_state,
        ready: 0,
    }
}

#[doc(hidden)]
pub fn benchmark_prepare_initial_indexed_lookups(
    watches: &[SharedWatchEntry],
    concurrency: usize,
) -> IndexedLookupBackfillBenchmarkScenario {
    let mut scenario = benchmark_prepare_indexed_lookup_backfill(watches);
    scenario.ready = scenario
        .backfill_state
        .take_ready(&scenario.backend_watch_map, concurrency)
        .len();
    scenario
}

fn spawn_indexed_lookup_tasks(
    backend: Arc<dyn DiscoveryBackend>,
    watches: &HashMap<Uuid, SharedWatchEntry>,
    backfill_state: &mut IndexedLookupBackfillState,
    tasks: &mut JoinSet<(Uuid, DateTime<Utc>, Result<Option<DetectedDeposit>>)>,
) {
    let available_slots = backend
        .indexed_lookup_concurrency()
        .saturating_sub(backfill_state.inflight_len());
    let ready = backfill_state.take_ready(watches, available_slots);

    for (watch, version) in ready {
        let backend = backend.clone();
        tasks.spawn(async move {
            let watch_id = watch.watch_id;
            let started = Instant::now();
            let result = backend.indexed_lookup(watch.as_ref()).await;
            histogram!(
                SAURON_INDEXED_LOOKUP_DURATION_SECONDS,
                "backend" => backend.name().to_string(),
            )
            .record(started.elapsed().as_secs_f64());
            (watch_id, version, result)
        });
    }
}

async fn drain_indexed_lookup_tasks(
    tasks: &mut JoinSet<(Uuid, DateTime<Utc>, Result<Option<DetectedDeposit>>)>,
    current_watch_versions: &HashMap<Uuid, DateTime<Utc>>,
    backfill_state: &mut IndexedLookupBackfillState,
    context: &DiscoveryContext,
    backend_name: &str,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) -> Result<()> {
    while let Some(join_result) = tasks.try_join_next() {
        let (watch_id, version, lookup_result) = join_result.context(DiscoveryTaskJoinSnafu)?;

        if !backfill_state.finish(watch_id, version, current_watch_versions) {
            continue;
        }

        match lookup_result {
            Ok(Some(detected)) => {
                let outcome = report_detected_deposit(
                    context,
                    backend_name,
                    &detected,
                    pending_submissions,
                    reported_candidates,
                )
                .await;
                if detected.indexer_candidate_id.is_some() {
                    warn!(
                        backend = backend_name,
                        candidate_id = ?detected.indexer_candidate_id,
                        outcome = ?outcome,
                        "Indexed lookup returned a durable indexer candidate but cannot acknowledge it from this path"
                    );
                }
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    backend = backend_name,
                    watch_id = %watch_id,
                    %error,
                    "Indexed lookup failed for active watch"
                );
            }
        }
    }

    Ok(())
}

async fn report_detected_deposit(
    context: &DiscoveryContext,
    backend_name: &str,
    detected: &DetectedDeposit,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) -> SubmissionOutcome {
    if reported_candidates
        .get(&detected.watch_id)
        .is_some_and(|existing| existing == detected)
    {
        return SubmissionOutcome::AlreadyTracked;
    }

    if pending_submissions
        .get(&detected.watch_id)
        .is_some_and(|pending| pending.detected == *detected)
    {
        return SubmissionOutcome::AlreadyTracked;
    }

    submit_detected_deposit(
        context,
        backend_name,
        detected,
        pending_submissions,
        reported_candidates,
    )
    .await
}

async fn retry_pending_submissions(
    context: &DiscoveryContext,
    backend_name: &str,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) {
    let now = Instant::now();
    let pending = pending_submissions
        .values()
        .filter(|submission| submission.next_attempt_at <= now)
        .map(|submission| submission.detected.clone())
        .collect::<Vec<_>>();

    for detected in pending {
        submit_detected_deposit(
            context,
            backend_name,
            &detected,
            pending_submissions,
            reported_candidates,
        )
        .await;
    }
}

async fn submit_detected_deposit(
    context: &DiscoveryContext,
    backend_name: &str,
    detected: &DetectedDeposit,
    pending_submissions: &mut HashMap<Uuid, PendingSubmission>,
    reported_candidates: &mut HashMap<Uuid, DetectedDeposit>,
) -> SubmissionOutcome {
    let evidence = json!({
        "source": "sauron",
        "backend": backend_name,
        "watch_target": detected.watch_target.as_str(),
        "chain": detected.source_chain.to_db_string(),
        "token": detected.source_token.clone(),
        "address": detected.address,
        "tx_hash": detected.tx_hash,
        "transfer_index": detected.transfer_index,
        "amount": detected.amount.to_string(),
        "observed_at": detected.observed_at,
    });
    let idempotency_key = Some(format!(
        "sauron:{backend_name}:{}:{}:{}:{}",
        detected.watch_target.as_str(),
        detected.watch_id,
        detected.tx_hash,
        detected.transfer_index
    ));

    let target = match detected.watch_target {
        WatchTarget::ProviderOperation => DetectorHintTarget::ProviderOperation {
            id: detected.watch_id,
        },
        WatchTarget::FundingVault => DetectorHintTarget::FundingVault {
            id: detected.watch_id,
        },
    };
    let submit_result = context
        .router_client
        .submit_detector_hint(&DetectorHintRequest {
            target,
            source: "sauron".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence,
            idempotency_key,
        })
        .await
        .map(detector_hint_id);

    match submit_result {
        Ok(response) => {
            info!(
                backend = backend_name,
                watch_id = %detected.watch_id,
                watch_target = detected.watch_target.as_str(),
                hint_id = %response,
                tx_hash = %detected.tx_hash,
                transfer_index = detected.transfer_index,
                "Discovery backend submitted funding/progress hint"
            );
            pending_submissions.remove(&detected.watch_id);
            reported_candidates.insert(detected.watch_id, detected.clone());
            SubmissionOutcome::Submitted
        }
        Err(error) => {
            if should_retry_submission(&error) {
                if detected.indexer_candidate_id.is_some() {
                    let message = error.to_string();
                    warn!(
                        backend = backend_name,
                        watch_id = %detected.watch_id,
                        tx_hash = %detected.tx_hash,
                        %error,
                        "Discovery backend submit was retryable; durable candidate will be released"
                    );
                    return SubmissionOutcome::RetryableFailure(message);
                }
                let now = Instant::now();
                let attempts = pending_submissions
                    .get(&detected.watch_id)
                    .map_or(1, |pending| pending.attempts.saturating_add(1));
                let retry_delay = submission_retry_delay(&detected, attempts);
                pending_submissions.insert(
                    detected.watch_id,
                    PendingSubmission {
                        detected: detected.clone(),
                        attempts,
                        next_attempt_at: now + retry_delay,
                    },
                );
                warn!(
                    backend = backend_name,
                    watch_id = %detected.watch_id,
                    tx_hash = %detected.tx_hash,
                    attempts,
                    retry_in_ms = retry_delay.as_millis(),
                    %error,
                    "Discovery backend submit was retryable; keeping candidate queued for retry"
                );
                SubmissionOutcome::RetryableFailure(error.to_string())
            } else {
                pending_submissions.remove(&detected.watch_id);
                warn!(
                    backend = backend_name,
                    watch_id = %detected.watch_id,
                    tx_hash = %detected.tx_hash,
                    %error,
                    "Discovery backend failed to submit provider-operation hint"
                );
                SubmissionOutcome::TerminalFailure
            }
        }
    }
}

async fn handle_submission_outcome(
    backend: &dyn DiscoveryBackend,
    detected: &DetectedDeposit,
    outcome: SubmissionOutcome,
) {
    if detected.indexer_candidate_id.is_none() {
        return;
    }

    let result = match &outcome {
        SubmissionOutcome::Submitted
        | SubmissionOutcome::TerminalFailure
        | SubmissionOutcome::AlreadyTracked => backend.mark_detection_submitted(detected).await,
        SubmissionOutcome::RetryableFailure(error) => {
            backend.release_detection(detected, error).await
        }
    };

    if let Err(error) = result {
        warn!(
            backend = backend.name(),
            candidate_id = ?detected.indexer_candidate_id,
            %error,
            "Failed to update durable indexer candidate after hint submission"
        );
    }
}

fn detector_hint_id(envelope: DetectorHintEnvelope) -> Uuid {
    match envelope {
        DetectorHintEnvelope::ProviderOperation { hint } => hint.id,
        DetectorHintEnvelope::FundingVault { hint } => hint.id,
    }
}

fn should_retry_submission(error: &Error) -> bool {
    match error {
        Error::RouterRequest { .. } => true,
        Error::RouterRejected { status, .. } => {
            status.is_server_error()
                || matches!(
                    *status,
                    StatusCode::REQUEST_TIMEOUT
                        | StatusCode::TOO_MANY_REQUESTS
                        | StatusCode::BAD_GATEWAY
                        | StatusCode::SERVICE_UNAVAILABLE
                        | StatusCode::GATEWAY_TIMEOUT
                )
        }
        _ => false,
    }
}

fn submission_retry_delay(detected: &DetectedDeposit, attempts: u32) -> Duration {
    let exponent = attempts.saturating_sub(1).min(6);
    let multiplier = 1_u64 << exponent;
    let base_delay_secs = SUBMISSION_RETRY_BASE_DELAY
        .as_secs()
        .saturating_mul(multiplier)
        .min(SUBMISSION_RETRY_MAX_DELAY.as_secs());
    let jitter_millis =
        submission_retry_jitter_millis(detected, attempts).min(SUBMISSION_RETRY_JITTER_MAX_MILLIS);

    Duration::from_secs(base_delay_secs) + Duration::from_millis(jitter_millis)
}

fn submission_retry_jitter_millis(detected: &DetectedDeposit, attempts: u32) -> u64 {
    let mut hash = detected.watch_id.as_u128() as u64 ^ detected.transfer_index;
    hash ^= (detected.watch_id.as_u128() >> 64) as u64;
    hash ^= u64::from(attempts);

    for byte in detected.tx_hash.as_bytes() {
        hash = hash
            .wrapping_mul(1099511628211)
            .wrapping_add(u64::from(*byte));
    }

    hash % (SUBMISSION_RETRY_JITTER_MAX_MILLIS.saturating_add(1))
}

#[cfg(test)]
mod tests {
    use super::{
        should_retry_submission, submission_retry_delay, DetectedDeposit,
        IndexedLookupBackfillState, SUBMISSION_RETRY_BASE_DELAY, SUBMISSION_RETRY_MAX_DELAY,
    };
    use crate::error::Error;
    use crate::watch::{WatchEntry, WatchTarget};
    use alloy::primitives::U256;
    use chrono::{Duration, Utc};
    use reqwest::StatusCode;
    use router_primitives::{ChainType, TokenIdentifier};
    use std::{collections::HashMap, sync::Arc};
    use uuid::Uuid;

    #[test]
    fn retries_transient_http_rejection() {
        let error = Error::RouterRejected {
            status: StatusCode::BAD_GATEWAY,
            body: r#"{"error":{"code":502,"message":"temporary upstream failure"}}"#.to_string(),
        };

        assert!(should_retry_submission(&error));
    }

    #[test]
    fn does_not_retry_permanent_conflict_rejection() {
        let error = Error::RouterRejected {
            status: StatusCode::CONFLICT,
            body: r#"{"error":{"code":"transfer_not_found_in_tx","message":"candidate transfer was not found in the transaction"}}"#.to_string(),
        };

        assert!(!should_retry_submission(&error));
    }

    #[test]
    fn retries_service_unavailable_rejection() {
        let error = Error::RouterRejected {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: r#"{"error":{"code":"transient_failure","message":"service unavailable"}}"#
                .to_string(),
        };

        assert!(should_retry_submission(&error));
    }

    fn detected_deposit() -> DetectedDeposit {
        DetectedDeposit {
            watch_target: WatchTarget::ProviderOperation,
            watch_id: Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: "btc-address".to_string(),
            tx_hash: "deadbeef".to_string(),
            transfer_index: 0,
            amount: U256::from(1_u64),
            observed_at: Utc::now(),
            indexer_candidate_id: None,
        }
    }

    #[test]
    fn submission_retry_delay_grows_with_attempts_and_is_capped() {
        let detected = detected_deposit();
        let first = submission_retry_delay(&detected, 1);
        let second = submission_retry_delay(&detected, 2);
        let capped = submission_retry_delay(&detected, 32);

        assert!(first >= SUBMISSION_RETRY_BASE_DELAY);
        assert!(second > first);
        assert!(capped <= SUBMISSION_RETRY_MAX_DELAY + Duration::seconds(1).to_std().unwrap());
    }

    fn watch_entry(watch_id: Uuid, updated_at: chrono::DateTime<Utc>) -> Arc<WatchEntry> {
        Arc::new(WatchEntry {
            watch_target: WatchTarget::ProviderOperation,
            watch_id,
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: "btc-address".to_string(),
            min_amount: U256::from(1_u64),
            max_amount: U256::from(10_u64),
            required_amount: U256::from(10_u64),
            deposit_deadline: Utc::now() + Duration::minutes(5),
            created_at: Utc::now(),
            updated_at,
        })
    }

    #[test]
    fn backfill_state_enqueues_new_and_updated_watches_once() {
        let watch_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let mut state = IndexedLookupBackfillState::default();
        let mut watches = HashMap::new();
        watches.insert(watch_id, watch_entry(watch_id, updated_at));

        state.sync_snapshot(&watches);
        state.sync_snapshot(&watches);
        assert_eq!(state.queue_len(), 1);

        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);
        assert!(state.finish(
            watch_id,
            updated_at,
            &HashMap::from([(watch_id, updated_at)]),
        ));

        let newer_updated_at = updated_at + Duration::seconds(1);
        watches.insert(watch_id, watch_entry(watch_id, newer_updated_at));
        state.sync_snapshot(&watches);
        assert_eq!(state.queue_len(), 1);
    }

    #[test]
    fn backfill_state_discards_stale_lookup_results() {
        let watch_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let newer_updated_at = updated_at + Duration::seconds(1);
        let mut state = IndexedLookupBackfillState::default();
        let watches = HashMap::from([(watch_id, watch_entry(watch_id, updated_at))]);

        state.sync_snapshot(&watches);
        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);

        assert!(!state.finish(
            watch_id,
            updated_at,
            &HashMap::from([(watch_id, newer_updated_at)]),
        ));
    }

    #[test]
    fn backfill_state_requeues_pending_submission_for_same_version() {
        let watch_id = Uuid::now_v7();
        let updated_at = Utc::now();
        let mut state = IndexedLookupBackfillState::default();
        let watches = HashMap::from([(watch_id, watch_entry(watch_id, updated_at))]);

        state.sync_snapshot(&watches);
        let ready = state.take_ready(&watches, 1);
        assert_eq!(ready.len(), 1);
        assert!(state.finish(
            watch_id,
            updated_at,
            &HashMap::from([(watch_id, updated_at)]),
        ));

        state.requeue_pending_submissions(&watches, [watch_id]);
        assert_eq!(state.queue_len(), 1);
    }
}
