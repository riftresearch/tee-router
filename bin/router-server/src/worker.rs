use crate::{
    app::{initialize_components, PaymasterMode},
    runtime::BackgroundTaskResult,
    services::{
        vault_manager::FundingHintPassSummary, ProviderHealthPollSummary, ProviderHealthPoller,
        VaultManager,
    },
    telemetry, Error, Result, RouterServerArgs,
};
use chrono::{DateTime, Utc};
use router_core::{
    db::Database,
    error::RouterCoreError,
    services::route_costs::{RouteCostRefreshSummary, RouteCostService},
};
use router_temporal::{OrderWorkflowClient, RouterTemporalError, TemporalConnection};
use snafu::{FromString, Whatever};
use sqlx_core::error::Error as SqlxError;
use sqlx_postgres::{PgListener, PgNotification};
use std::{
    collections::{HashSet, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    task::{JoinError, JoinSet},
    time::{interval, sleep, MissedTickBehavior},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

const WORKFLOW_START_QUEUE_MAX_IDS: usize = 10_000;
const VAULT_FUNDING_HINT_CHANNEL: &str = "router_vault_funding_hints";
const VAULT_REFUND_WAKEUP_CHANNEL: &str = "router_vault_refund_wakeups";
const QUOTE_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);
const WORKER_LISTENER_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_WORKER_IDENTIFIER_LEN: usize = 128;

#[derive(Clone)]
pub struct RouterWorkerConfig {
    pub database_url: String,
    pub worker_id: String,
    pub vault_work_poll_interval: Duration,
    pub order_execution_poll_interval: Duration,
    pub route_cost_refresh_interval: Duration,
    pub provider_health_poll_interval: Duration,
    pub order_maintenance_pass_limit: i64,
    pub order_planning_pass_limit: i64,
    pub order_execution_pass_limit: i64,
    pub order_execution_concurrency: usize,
    pub vault_funding_hint_pass_limit: i64,
}

impl fmt::Debug for RouterWorkerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterWorkerConfig")
            .field("database_url", &"<redacted>")
            .field("worker_id", &self.worker_id)
            .field("vault_work_poll_interval", &self.vault_work_poll_interval)
            .field(
                "order_execution_poll_interval",
                &self.order_execution_poll_interval,
            )
            .field(
                "route_cost_refresh_interval",
                &self.route_cost_refresh_interval,
            )
            .field(
                "provider_health_poll_interval",
                &self.provider_health_poll_interval,
            )
            .field(
                "order_maintenance_pass_limit",
                &self.order_maintenance_pass_limit,
            )
            .field("order_planning_pass_limit", &self.order_planning_pass_limit)
            .field(
                "order_execution_pass_limit",
                &self.order_execution_pass_limit,
            )
            .field(
                "order_execution_concurrency",
                &self.order_execution_concurrency,
            )
            .field(
                "vault_funding_hint_pass_limit",
                &self.vault_funding_hint_pass_limit,
            )
            .finish()
    }
}

impl RouterWorkerConfig {
    pub fn from_args(args: &RouterServerArgs) -> Result<Self> {
        ensure_positive_seconds(
            "worker refund/vault poll seconds",
            args.worker_refund_poll_seconds,
        )?;
        ensure_positive_seconds(
            "worker route-cost refresh seconds",
            args.worker_route_cost_refresh_seconds,
        )?;
        ensure_positive_seconds(
            "worker order execution poll seconds",
            args.worker_order_execution_poll_seconds,
        )?;
        ensure_positive_seconds(
            "worker provider-health poll seconds",
            args.worker_provider_health_poll_seconds,
        )?;
        ensure_positive_seconds(
            "provider health timeout seconds",
            args.provider_health_timeout_seconds,
        )?;
        ensure_positive_count(
            "worker order maintenance pass limit",
            args.worker_order_maintenance_pass_limit,
        )?;
        ensure_positive_count(
            "worker order planning pass limit",
            args.worker_order_planning_pass_limit,
        )?;
        ensure_positive_count(
            "worker order execution pass limit",
            args.worker_order_execution_pass_limit,
        )?;
        ensure_positive_count(
            "worker order execution concurrency",
            args.worker_order_execution_concurrency,
        )?;
        ensure_positive_count(
            "worker vault funding hint pass limit",
            args.worker_vault_funding_hint_pass_limit,
        )?;
        Ok(Self {
            database_url: args.database_url.clone(),
            worker_id: match args.worker_id.as_deref() {
                Some(worker_id) => normalize_worker_identifier("worker id", worker_id)?,
                None => format!("router-worker-{}", Uuid::now_v7()),
            },
            vault_work_poll_interval: Duration::from_secs(args.worker_refund_poll_seconds),
            order_execution_poll_interval: Duration::from_secs(
                args.worker_order_execution_poll_seconds,
            ),
            route_cost_refresh_interval: Duration::from_secs(
                args.worker_route_cost_refresh_seconds,
            ),
            provider_health_poll_interval: Duration::from_secs(
                args.worker_provider_health_poll_seconds,
            ),
            order_maintenance_pass_limit: i64::from(args.worker_order_maintenance_pass_limit),
            order_planning_pass_limit: i64::from(args.worker_order_planning_pass_limit),
            order_execution_pass_limit: i64::from(args.worker_order_execution_pass_limit),
            order_execution_concurrency: args.worker_order_execution_concurrency as usize,
            vault_funding_hint_pass_limit: i64::from(args.worker_vault_funding_hint_pass_limit),
        })
    }
}

#[derive(Debug)]
enum WorkflowStartBatch {
    Global,
    Orders(Vec<Uuid>),
}

#[derive(Debug, Default)]
struct WorkflowStartQueue {
    global: bool,
    queued_order_ids: HashSet<Uuid>,
    order_ids: VecDeque<Uuid>,
}

impl WorkflowStartQueue {
    fn enqueue_global(&mut self) {
        self.global = true;
    }

    fn enqueue_order(&mut self, order_id: Uuid) {
        if self.queued_order_ids.contains(&order_id) {
            return;
        }
        if self.queued_order_ids.len() >= WORKFLOW_START_QUEUE_MAX_IDS {
            self.queued_order_ids.clear();
            self.order_ids.clear();
            self.enqueue_global();
            return;
        }
        self.queued_order_ids.insert(order_id);
        self.order_ids.push_back(order_id);
    }

    fn enqueue_orders(&mut self, order_ids: impl IntoIterator<Item = Uuid>) {
        for order_id in order_ids {
            self.enqueue_order(order_id);
        }
    }

    fn take_batch(&mut self, batch_limit: usize) -> Option<WorkflowStartBatch> {
        let batch_limit = batch_limit.max(1);
        if self.global {
            self.global = false;
            self.queued_order_ids.clear();
            self.order_ids.clear();
            return Some(WorkflowStartBatch::Global);
        }

        let mut order_ids = Vec::with_capacity(batch_limit);
        while order_ids.len() < batch_limit {
            let Some(order_id) = self.order_ids.pop_front() else {
                break;
            };
            self.queued_order_ids.remove(&order_id);
            order_ids.push(order_id);
        }

        if order_ids.is_empty() {
            None
        } else {
            Some(WorkflowStartBatch::Orders(order_ids))
        }
    }
}

#[derive(Debug, Clone, Default)]
struct VaultWorkOutcome {
    funding_hints: FundingHintPassSummary,
}

#[derive(Debug, Clone, Default)]
struct WorkflowStartSummary {
    considered_orders: usize,
    marked_funded_orders: usize,
    started_workflows: usize,
    already_started_workflows: usize,
    skipped_orders: usize,
}

impl WorkflowStartSummary {
    fn has_activity(&self) -> bool {
        self.marked_funded_orders > 0
            || self.started_workflows > 0
            || self.already_started_workflows > 0
    }
}

type VaultTaskResult = std::result::Result<VaultWorkOutcome, String>;
type WorkflowStartTaskResult = std::result::Result<WorkflowStartSummary, String>;
type RouteCostTaskResult = std::result::Result<RouteCostRefreshSummary, String>;
type ProviderHealthTaskResult = std::result::Result<ProviderHealthPollSummary, String>;
type QuoteCleanupTaskResult = std::result::Result<u64, String>;

pub async fn run_worker(args: RouterServerArgs) -> Result<()> {
    let config = RouterWorkerConfig::from_args(&args)?;
    info!(
        worker_id = %config.worker_id,
        "Starting router-worker"
    );
    let components = initialize_components(
        &args,
        Some(config.worker_id.clone()),
        PaymasterMode::Enabled,
    )
    .await?;
    let order_workflow_client = Arc::new(
        OrderWorkflowClient::connect(
            &TemporalConnection {
                temporal_address: args.temporal_address.clone(),
                namespace: args.temporal_namespace.clone(),
            },
            args.temporal_task_queue.clone(),
        )
        .await
        .map_err(|source| Error::Generic {
            source: Whatever::without_source(format!(
                "failed to initialize Temporal order workflow client: {source}"
            )),
        })?,
    );

    run_worker_loop(
        components.db,
        components.vault_manager,
        order_workflow_client,
        components.route_costs,
        components.provider_health_poller,
        config,
    )
    .await
    .map_err(|message| Error::Generic {
        source: Whatever::without_source(message),
    })
}

pub async fn run_worker_loop(
    db: Database,
    vault_manager: Arc<VaultManager>,
    order_workflow_client: Arc<OrderWorkflowClient>,
    route_costs: Arc<RouteCostService>,
    provider_health_poller: Arc<ProviderHealthPoller>,
    config: RouterWorkerConfig,
) -> BackgroundTaskResult {
    let mut vault_work_poll_interval = interval(config.vault_work_poll_interval);
    vault_work_poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut workflow_start_poll_interval = interval(config.order_execution_poll_interval);
    workflow_start_poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut route_cost_refresh_interval = interval(config.route_cost_refresh_interval);
    route_cost_refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut provider_health_poll_interval = interval(config.provider_health_poll_interval);
    provider_health_poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut quote_cleanup_interval = interval(QUOTE_CLEANUP_INTERVAL);
    quote_cleanup_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut worker_listener = Some(connect_worker_listener(&config.database_url).await?);
    let mut vault_tasks: JoinSet<VaultTaskResult> = JoinSet::new();
    let mut workflow_start_tasks: JoinSet<WorkflowStartTaskResult> = JoinSet::new();
    let mut route_cost_tasks: JoinSet<RouteCostTaskResult> = JoinSet::new();
    let mut provider_health_tasks: JoinSet<ProviderHealthTaskResult> = JoinSet::new();
    let mut quote_cleanup_tasks: JoinSet<QuoteCleanupTaskResult> = JoinSet::new();
    let mut workflow_start_queue = WorkflowStartQueue::default();
    let mut pending_vault_wakeup = true;
    let mut pending_route_cost_refresh = true;
    let mut pending_provider_health_poll = true;
    let mut pending_quote_cleanup = true;

    info!(worker_id = %config.worker_id, "Router worker started");
    workflow_start_queue.enqueue_global();
    spawn_workflow_start_if_idle(
        &mut workflow_start_tasks,
        db.clone(),
        order_workflow_client.clone(),
        &mut workflow_start_queue,
        config.order_execution_pass_limit,
    );
    spawn_vault_work_if_idle(
        &mut vault_tasks,
        vault_manager.clone(),
        &mut pending_vault_wakeup,
        config.vault_funding_hint_pass_limit,
    );
    spawn_route_cost_refresh_if_idle(
        &mut route_cost_tasks,
        route_costs.clone(),
        &mut pending_route_cost_refresh,
    );
    spawn_provider_health_poll_if_idle(
        &mut provider_health_tasks,
        provider_health_poller.clone(),
        &mut pending_provider_health_poll,
    );
    spawn_quote_cleanup_if_idle(
        &mut quote_cleanup_tasks,
        db.clone(),
        &mut pending_quote_cleanup,
    );
    let mut next_refund_due_at = refresh_next_refund_due_at(&db).await?;

    loop {
        tokio::select! {
            _ = sleep(refund_due_delay(next_refund_due_at.as_ref())), if next_refund_due_at.is_some() && vault_tasks.is_empty() => {
                next_refund_due_at = None;
                pending_vault_wakeup = true;
                spawn_vault_work_if_idle(
                    &mut vault_tasks,
                    vault_manager.clone(),
                    &mut pending_vault_wakeup,
                    config.vault_funding_hint_pass_limit,
                );
            }
            _ = vault_work_poll_interval.tick(), if vault_tasks.is_empty() => {
                pending_vault_wakeup = true;
                spawn_vault_work_if_idle(
                    &mut vault_tasks,
                    vault_manager.clone(),
                    &mut pending_vault_wakeup,
                    config.vault_funding_hint_pass_limit,
                );
            }
            _ = route_cost_refresh_interval.tick(), if route_cost_tasks.is_empty() => {
                pending_route_cost_refresh = true;
                spawn_route_cost_refresh_if_idle(
                    &mut route_cost_tasks,
                    route_costs.clone(),
                    &mut pending_route_cost_refresh,
                );
            }
            _ = workflow_start_poll_interval.tick(), if workflow_start_tasks.is_empty() => {
                workflow_start_queue.enqueue_global();
                spawn_workflow_start_if_idle(
                    &mut workflow_start_tasks,
                    db.clone(),
                    order_workflow_client.clone(),
                    &mut workflow_start_queue,
                    config.order_execution_pass_limit,
                );
            }
            _ = provider_health_poll_interval.tick(), if provider_health_tasks.is_empty() => {
                pending_provider_health_poll = true;
                spawn_provider_health_poll_if_idle(
                    &mut provider_health_tasks,
                    provider_health_poller.clone(),
                    &mut pending_provider_health_poll,
                );
            }
            _ = quote_cleanup_interval.tick(), if quote_cleanup_tasks.is_empty() => {
                pending_quote_cleanup = true;
                spawn_quote_cleanup_if_idle(
                    &mut quote_cleanup_tasks,
                    db.clone(),
                    &mut pending_quote_cleanup,
                );
            }
            hint = recv_worker_notification(&mut worker_listener), if worker_listener.is_some() => {
                match hint {
                    Some(Ok(notification)) => {
                        debug!(
                            channel = notification.channel(),
                            payload = notification.payload(),
                            "Router worker received hint wakeup"
                        );
                        match notification.channel() {
                            VAULT_FUNDING_HINT_CHANNEL | VAULT_REFUND_WAKEUP_CHANNEL => {
                                pending_vault_wakeup = true;
                                spawn_vault_work_if_idle(
                                    &mut vault_tasks,
                                    vault_manager.clone(),
                                    &mut pending_vault_wakeup,
                                    config.vault_funding_hint_pass_limit,
                                );
                            }
                            _ => {}
                        }
                    }
                    Some(Err(err)) => {
                        warn!(
                            %err,
                            "Router worker hint listener failed; poll loops remain active while reconnecting"
                        );
                        worker_listener = None;
                    }
                    None => {}
                }
            }
            _ = sleep(WORKER_LISTENER_RECONNECT_DELAY), if worker_listener.is_none() => {
                match connect_worker_listener(&config.database_url).await {
                    Ok(listener) => {
                        info!("Router worker hint listener reconnected");
                        worker_listener = Some(listener);
                    }
                    Err(err) => warn!(
                        %err,
                        "Router worker hint listener reconnect failed; retrying"
                    ),
                }
            }
            vault_result = vault_tasks.join_next(), if !vault_tasks.is_empty() => {
                let outcome = handle_vault_task_result(vault_result)?;
                workflow_start_queue.enqueue_orders(outcome.funding_hints.funded_order_ids);
                spawn_workflow_start_if_idle(
                    &mut workflow_start_tasks,
                    db.clone(),
                    order_workflow_client.clone(),
                    &mut workflow_start_queue,
                    config.order_execution_pass_limit,
                );
                next_refund_due_at = refresh_next_refund_due_at(&db).await?;
                spawn_vault_work_if_idle(
                    &mut vault_tasks,
                    vault_manager.clone(),
                    &mut pending_vault_wakeup,
                    config.vault_funding_hint_pass_limit,
                );
            }
            workflow_start_result = workflow_start_tasks.join_next(), if !workflow_start_tasks.is_empty() => {
                let summary = handle_workflow_start_task_result(workflow_start_result)?;
                if summary.has_activity() {
                    info!(
                        considered_orders = summary.considered_orders,
                        marked_funded_orders = summary.marked_funded_orders,
                        started_workflows = summary.started_workflows,
                        already_started_workflows = summary.already_started_workflows,
                        skipped_orders = summary.skipped_orders,
                        "Router worker started order workflows"
                    );
                }
                spawn_workflow_start_if_idle(
                    &mut workflow_start_tasks,
                    db.clone(),
                    order_workflow_client.clone(),
                    &mut workflow_start_queue,
                    config.order_execution_pass_limit,
                );
            }
            route_cost_result = route_cost_tasks.join_next(), if !route_cost_tasks.is_empty() => {
                let summary = handle_route_cost_task_result(route_cost_result)?;
                info!(
                    candidate_edges = summary.candidate_edges,
                    snapshots_upserted = summary.snapshots_upserted,
                    provider_quotes_attempted = summary.provider_quotes_attempted,
                    provider_quotes_succeeded = summary.provider_quotes_succeeded,
                    provider_quotes_failed = summary.provider_quotes_failed,
                    refreshed_at = %summary.refreshed_at,
                    "Router worker refreshed route costs"
                );
                spawn_route_cost_refresh_if_idle(
                    &mut route_cost_tasks,
                    route_costs.clone(),
                    &mut pending_route_cost_refresh,
                );
            }
            provider_health_result = provider_health_tasks.join_next(), if !provider_health_tasks.is_empty() => {
                let summary = handle_provider_health_task_result(provider_health_result)?;
                info!(
                    checked = summary.checked,
                    healthy = summary.healthy,
                    down = summary.down,
                    "Router worker refreshed provider health"
                );
                spawn_provider_health_poll_if_idle(
                    &mut provider_health_tasks,
                    provider_health_poller.clone(),
                    &mut pending_provider_health_poll,
                );
            }
            quote_cleanup_result = quote_cleanup_tasks.join_next(), if !quote_cleanup_tasks.is_empty() => {
                let deleted_quotes = handle_quote_cleanup_task_result(quote_cleanup_result)?;
                if deleted_quotes > 0 {
                    info!(
                        deleted_quotes,
                        "Router worker pruned expired unassociated quotes"
                    );
                }
                spawn_quote_cleanup_if_idle(
                    &mut quote_cleanup_tasks,
                    db.clone(),
                    &mut pending_quote_cleanup,
                );
            }
        }
    }
}

async fn connect_worker_listener(database_url: &str) -> Result<PgListener, String> {
    let mut listener = PgListener::connect(database_url)
        .await
        .map_err(|err| format!("failed to connect worker listener: {err}"))?;
    listener
        .listen(VAULT_FUNDING_HINT_CHANNEL)
        .await
        .map_err(|err| format!("failed to listen for vault funding hints: {err}"))?;
    listener
        .listen(VAULT_REFUND_WAKEUP_CHANNEL)
        .await
        .map_err(|err| format!("failed to listen for vault refund wakeups: {err}"))?;
    Ok(listener)
}

async fn recv_worker_notification(
    listener: &mut Option<PgListener>,
) -> Option<std::result::Result<PgNotification, SqlxError>> {
    match listener {
        Some(listener) => Some(listener.recv().await),
        None => None,
    }
}

fn spawn_vault_work_if_idle(
    vault_tasks: &mut JoinSet<VaultTaskResult>,
    vault_manager: Arc<VaultManager>,
    pending_vault_wakeup: &mut bool,
    funding_hint_pass_limit: i64,
) {
    if vault_tasks.is_empty() && *pending_vault_wakeup {
        *pending_vault_wakeup = false;
        vault_tasks.spawn(run_vault_work_pass(vault_manager, funding_hint_pass_limit));
    }
}

fn spawn_workflow_start_if_idle(
    workflow_start_tasks: &mut JoinSet<WorkflowStartTaskResult>,
    db: Database,
    order_workflow_client: Arc<OrderWorkflowClient>,
    workflow_start_queue: &mut WorkflowStartQueue,
    batch_limit: i64,
) {
    if workflow_start_tasks.is_empty() {
        let batch_limit = usize::try_from(batch_limit)
            .unwrap_or(WORKFLOW_START_QUEUE_MAX_IDS)
            .min(WORKFLOW_START_QUEUE_MAX_IDS);
        if let Some(batch) = workflow_start_queue.take_batch(batch_limit) {
            workflow_start_tasks.spawn(run_workflow_start_batch(
                db,
                order_workflow_client,
                batch,
                batch_limit,
            ));
        }
    }
}

fn spawn_route_cost_refresh_if_idle(
    route_cost_tasks: &mut JoinSet<RouteCostTaskResult>,
    route_costs: Arc<RouteCostService>,
    pending_route_cost_refresh: &mut bool,
) {
    if route_cost_tasks.is_empty() && *pending_route_cost_refresh {
        *pending_route_cost_refresh = false;
        route_cost_tasks.spawn(run_route_cost_refresh(route_costs));
    }
}

fn spawn_provider_health_poll_if_idle(
    provider_health_tasks: &mut JoinSet<ProviderHealthTaskResult>,
    provider_health_poller: Arc<ProviderHealthPoller>,
    pending_provider_health_poll: &mut bool,
) {
    if provider_health_tasks.is_empty() && *pending_provider_health_poll {
        *pending_provider_health_poll = false;
        provider_health_tasks.spawn(run_provider_health_poll(provider_health_poller));
    }
}

fn spawn_quote_cleanup_if_idle(
    quote_cleanup_tasks: &mut JoinSet<QuoteCleanupTaskResult>,
    db: Database,
    pending_quote_cleanup: &mut bool,
) {
    if quote_cleanup_tasks.is_empty() && *pending_quote_cleanup {
        *pending_quote_cleanup = false;
        quote_cleanup_tasks.spawn(run_quote_cleanup(db));
    }
}

async fn run_vault_work_pass(
    vault_manager: Arc<VaultManager>,
    funding_hint_pass_limit: i64,
) -> VaultTaskResult {
    let started = Instant::now();
    let mut funding_hints = vault_manager
        .process_funding_hints_detailed(funding_hint_pass_limit)
        .await
        .map_err(|err| err.to_string())?;
    if funding_hints.processed > 0 {
        info!(
            processed_funding_hints = funding_hints.processed,
            funded_orders = funding_hints.funded_order_ids.len(),
            "Router worker processed vault funding hints"
        );
    }
    let balance_reconciled_order_ids = vault_manager
        .reconcile_pending_funding_balances(funding_hint_pass_limit)
        .await
        .map_err(|err| err.to_string())?;
    if !balance_reconciled_order_ids.is_empty() {
        info!(
            funded_orders = balance_reconciled_order_ids.len(),
            "Router worker reconciled vault funding balances"
        );
        funding_hints
            .funded_order_ids
            .extend(balance_reconciled_order_ids);
    }
    let refunds = vault_manager.process_refund_pass().await;
    if refunds.timeout_claimed > 0 || refunds.retry_claimed > 0 {
        info!(
            timeout_refunds = refunds.timeout_claimed,
            retry_refunds = refunds.retry_claimed,
            refunded_orders = refunds.refunded_order_ids.len(),
            "Router worker processed vault refunds"
        );
    }
    telemetry::record_worker_tick("vault_worker", started.elapsed());
    Ok(VaultWorkOutcome { funding_hints })
}

async fn run_workflow_start_batch(
    db: Database,
    order_workflow_client: Arc<OrderWorkflowClient>,
    batch: WorkflowStartBatch,
    batch_limit: usize,
) -> WorkflowStartTaskResult {
    let started = Instant::now();
    let order_ids = match batch {
        WorkflowStartBatch::Global => db
            .orders()
            .get_market_orders_needing_execution_plan(
                i64::try_from(batch_limit).unwrap_or(i64::MAX),
            )
            .await
            .map_err(|err| err.to_string())?
            .into_iter()
            .map(|order| order.id)
            .collect::<Vec<_>>(),
        WorkflowStartBatch::Orders(order_ids) => order_ids,
    };

    let mut summary = WorkflowStartSummary {
        considered_orders: order_ids.len(),
        ..WorkflowStartSummary::default()
    };
    for order_id in order_ids {
        match mark_order_funded_and_start_workflow(&db, &order_workflow_client, order_id).await {
            Ok(StartOrderWorkflowOutcome::Started) => {
                summary.marked_funded_orders += 1;
                summary.started_workflows += 1;
            }
            Ok(StartOrderWorkflowOutcome::AlreadyStarted) => {
                summary.marked_funded_orders += 1;
                summary.already_started_workflows += 1;
            }
            Ok(StartOrderWorkflowOutcome::Skipped) => {
                summary.skipped_orders += 1;
            }
            Err(error) => return Err(error),
        }
    }
    let status_counts = db
        .orders()
        .count_operator_order_statuses()
        .await
        .map_err(|err| err.to_string())?;
    let status_counts = status_counts
        .iter()
        .map(|count| (count.status, count.order_count))
        .collect::<Vec<_>>();
    telemetry::record_operator_order_status_depth(&status_counts);
    telemetry::record_worker_tick("order_workflow_start", started.elapsed());
    Ok(summary)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOrderWorkflowOutcome {
    Started,
    AlreadyStarted,
    Skipped,
}

async fn mark_order_funded_and_start_workflow(
    db: &Database,
    order_workflow_client: &OrderWorkflowClient,
    order_id: Uuid,
) -> std::result::Result<StartOrderWorkflowOutcome, String> {
    match db
        .orders()
        .mark_order_funded_from_funded_vault(order_id, Utc::now())
        .await
    {
        Ok(_) => {}
        Err(RouterCoreError::NotFound) => {
            let order = db
                .orders()
                .get(order_id)
                .await
                .map_err(|err| err.to_string())?;
            if order.status != router_core::models::RouterOrderStatus::Funded {
                debug!(
                    order_id = %order_id,
                    status = %order.status.to_db_string(),
                    "Router worker skipped OrderWorkflow start for order not ready to execute"
                );
                return Ok(StartOrderWorkflowOutcome::Skipped);
            }
        }
        Err(err) => return Err(err.to_string()),
    }

    match order_workflow_client.start_order_workflow(order_id).await {
        Ok(_) => Ok(StartOrderWorkflowOutcome::Started),
        Err(RouterTemporalError::WorkflowAlreadyStarted { .. }) => {
            Ok(StartOrderWorkflowOutcome::AlreadyStarted)
        }
        Err(err) => Err(err.to_string()),
    }
}

async fn run_route_cost_refresh(route_costs: Arc<RouteCostService>) -> RouteCostTaskResult {
    let started = Instant::now();
    let summary = route_costs
        .refresh_anchor_costs()
        .await
        .map_err(|err| err.to_string())?;
    telemetry::record_worker_tick("route_cost_refresh", started.elapsed());
    Ok(summary)
}

async fn run_provider_health_poll(
    provider_health_poller: Arc<ProviderHealthPoller>,
) -> ProviderHealthTaskResult {
    let started = Instant::now();
    let summary = provider_health_poller
        .poll_once()
        .await
        .map_err(|err| err.to_string())?;
    telemetry::record_worker_tick("provider_health", started.elapsed());
    Ok(summary)
}

async fn run_quote_cleanup(db: Database) -> QuoteCleanupTaskResult {
    let started = Instant::now();
    let deleted = db
        .orders()
        .delete_expired_unassociated_router_order_quotes(Utc::now())
        .await
        .map_err(|err| err.to_string())?;
    telemetry::record_worker_tick("quote_cleanup", started.elapsed());
    Ok(deleted)
}

fn handle_vault_task_result(
    result: Option<std::result::Result<VaultTaskResult, JoinError>>,
) -> std::result::Result<VaultWorkOutcome, String> {
    match result {
        Some(Ok(Ok(outcome))) => Ok(outcome),
        Some(Ok(Err(error))) => Err(format!("vault worker pass failed: {error}")),
        Some(Err(error)) => Err(format!(
            "vault worker pass panicked or was cancelled: {error}"
        )),
        None => Err("vault worker task set terminated unexpectedly".to_string()),
    }
}

fn handle_workflow_start_task_result(
    result: Option<std::result::Result<WorkflowStartTaskResult, JoinError>>,
) -> std::result::Result<WorkflowStartSummary, String> {
    match result {
        Some(Ok(Ok(summary))) => Ok(summary),
        Some(Ok(Err(error))) => Err(format!("order workflow start pass failed: {error}")),
        Some(Err(error)) => Err(format!(
            "order workflow start pass panicked or was cancelled: {error}"
        )),
        None => Err("order workflow start task set terminated unexpectedly".to_string()),
    }
}

fn handle_route_cost_task_result(
    result: Option<std::result::Result<RouteCostTaskResult, JoinError>>,
) -> std::result::Result<RouteCostRefreshSummary, String> {
    match result {
        Some(Ok(Ok(summary))) => Ok(summary),
        Some(Ok(Err(error))) => Err(format!("route cost refresh failed: {error}")),
        Some(Err(error)) => Err(format!(
            "route cost refresh panicked or was cancelled: {error}"
        )),
        None => Err("route cost refresh task set terminated unexpectedly".to_string()),
    }
}

fn handle_provider_health_task_result(
    result: Option<std::result::Result<ProviderHealthTaskResult, JoinError>>,
) -> std::result::Result<ProviderHealthPollSummary, String> {
    match result {
        Some(Ok(Ok(summary))) => Ok(summary),
        Some(Ok(Err(error))) => Err(format!("provider health poll failed: {error}")),
        Some(Err(error)) => Err(format!(
            "provider health poll panicked or was cancelled: {error}"
        )),
        None => Err("provider health task set terminated unexpectedly".to_string()),
    }
}

fn handle_quote_cleanup_task_result(
    result: Option<std::result::Result<QuoteCleanupTaskResult, JoinError>>,
) -> std::result::Result<u64, String> {
    match result {
        Some(Ok(Ok(deleted))) => Ok(deleted),
        Some(Ok(Err(error))) => Err(format!("quote cleanup failed: {error}")),
        Some(Err(error)) => Err(format!("quote cleanup panicked or was cancelled: {error}")),
        None => Err("quote cleanup task set terminated unexpectedly".to_string()),
    }
}

async fn refresh_next_refund_due_at(
    db: &Database,
) -> std::result::Result<Option<DateTime<Utc>>, String> {
    db.vaults()
        .next_refund_due_at(Utc::now())
        .await
        .map_err(|err| format!("failed to schedule next refund wakeup: {err}"))
}

fn refund_due_delay(next_refund_due_at: Option<&DateTime<Utc>>) -> Duration {
    let Some(next_refund_due_at) = next_refund_due_at else {
        return Duration::from_secs(3600);
    };
    let now = Utc::now();
    if *next_refund_due_at <= now {
        return Duration::ZERO;
    }
    (*next_refund_due_at - now)
        .to_std()
        .unwrap_or(Duration::ZERO)
}

fn ensure_positive_seconds(name: &str, seconds: u64) -> Result<()> {
    if seconds == 0 {
        return Err(invalid_worker_config(&format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

fn ensure_positive_count(name: &str, count: u32) -> Result<()> {
    if count == 0 {
        return Err(invalid_worker_config(&format!(
            "{name} must be greater than zero"
        )));
    }
    Ok(())
}

fn normalize_worker_identifier(name: &str, value: &str) -> Result<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(invalid_worker_config(&format!("{name} must not be empty")));
    }
    if value.len() > MAX_WORKER_IDENTIFIER_LEN {
        return Err(invalid_worker_config(&format!(
            "{name} must be at most {MAX_WORKER_IDENTIFIER_LEN} bytes"
        )));
    }
    if !is_protocol_token(value) {
        return Err(invalid_worker_config(&format!(
            "{name} may only contain letters, numbers, '.', '_', ':', and '-'"
        )));
    }
    Ok(value.to_string())
}

fn is_protocol_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b':' | b'-'
        )
    })
}

fn invalid_worker_config(message: &str) -> Error {
    Error::Generic {
        source: Whatever::without_source(message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_start_queue_dedupes_order_ids_without_global_work() {
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let mut queue = WorkflowStartQueue::default();

        queue.enqueue_order(first);
        queue.enqueue_order(first);
        queue.enqueue_order(second);

        match queue.take_batch(25) {
            Some(WorkflowStartBatch::Orders(order_ids)) => {
                assert_eq!(order_ids, vec![first, second]);
            }
            other => panic!("expected deduped order batch, got {other:?}"),
        }
        assert!(queue.take_batch(25).is_none());
    }

    #[test]
    fn workflow_start_queue_global_work_supersedes_targeted_ids() {
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let mut queue = WorkflowStartQueue::default();

        queue.enqueue_order(first);
        queue.enqueue_order(second);
        queue.enqueue_global();

        assert!(matches!(
            queue.take_batch(25),
            Some(WorkflowStartBatch::Global)
        ));
        assert!(queue.take_batch(25).is_none());
    }

    #[test]
    fn worker_identifiers_are_trimmed_bounded_and_token_shaped() {
        assert_eq!(
            normalize_worker_identifier("worker id", "  worker:api_1.test  ").unwrap(),
            "worker:api_1.test"
        );

        assert!(normalize_worker_identifier("worker id", "  ").is_err());
        assert!(normalize_worker_identifier("worker id", "worker/id").is_err());
        assert!(normalize_worker_identifier("worker id", &"a".repeat(129)).is_err());
    }

    #[test]
    fn worker_config_debug_redacts_database_credentials() {
        let config = RouterWorkerConfig {
            database_url: "postgres://router_user:secret-password@db.example/router_db".to_string(),
            worker_id: "worker-1".to_string(),
            vault_work_poll_interval: Duration::from_secs(60),
            order_execution_poll_interval: Duration::from_secs(5),
            route_cost_refresh_interval: Duration::from_secs(300),
            provider_health_poll_interval: Duration::from_secs(120),
            order_maintenance_pass_limit: 100,
            order_planning_pass_limit: 100,
            order_execution_pass_limit: 25,
            order_execution_concurrency: 64,
            vault_funding_hint_pass_limit: 100,
        };

        let rendered = format!("{config:?}");
        assert!(rendered.contains("database_url"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains("secret-password"));
        assert!(!rendered.contains("router_user"));
    }

    #[test]
    fn workflow_start_queue_prioritizes_global_work_over_targeted_batches() {
        let mut queue = WorkflowStartQueue::default();
        let batch_limit = 4;
        let order_ids = (0..(batch_limit + 1))
            .map(|_| Uuid::now_v7())
            .collect::<Vec<_>>();

        queue.enqueue_global();
        queue.enqueue_orders(order_ids.clone());

        assert!(matches!(
            queue.take_batch(batch_limit),
            Some(WorkflowStartBatch::Global)
        ));
        assert!(queue.take_batch(batch_limit).is_none());
    }

    #[test]
    fn workflow_start_queue_collapses_to_global_when_targeted_ids_are_unbounded() {
        let mut queue = WorkflowStartQueue::default();

        for _ in 0..WORKFLOW_START_QUEUE_MAX_IDS {
            queue.enqueue_order(Uuid::now_v7());
        }
        queue.enqueue_order(Uuid::now_v7());

        assert!(matches!(
            queue.take_batch(25),
            Some(WorkflowStartBatch::Global)
        ));
        assert!(queue.take_batch(25).is_none());
    }

    #[test]
    fn refund_due_delay_is_zero_for_due_or_overdue_work() {
        assert_eq!(refund_due_delay(Some(&Utc::now())), Duration::ZERO);
        assert_eq!(
            refund_due_delay(Some(&(Utc::now() - chrono::Duration::seconds(1)))),
            Duration::ZERO
        );
    }
}
