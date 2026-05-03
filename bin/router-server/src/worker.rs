use crate::{
    app::{initialize_components, PaymasterMode},
    db::{worker_lease_repo::WorkerLease, Database},
    runtime::BackgroundTaskResult,
    services::{
        order_executor::OrderWorkerPassSummary,
        vault_manager::{FundingHintPassSummary, RefundPassSummary},
        OrderExecutionManager, ProviderHealthPollSummary, ProviderHealthPoller,
        RouteCostRefreshSummary, RouteCostService, VaultManager,
    },
    telemetry, Error, Result, RouterServerArgs,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use snafu::{FromString, Whatever};
use sqlx_postgres::PgListener;
use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    task::{JoinError, JoinSet},
    time::{interval, sleep, MissedTickBehavior},
};
use tracing::{debug, info, warn};
use uuid::Uuid;

const DEFAULT_WORKER_LEASE_NAME: &str = "global-router-worker";
const ORDER_EXECUTION_PASS_LIMIT: i64 = 25;
const ORDER_WAKE_BATCH_LIMIT: usize = 25;
const VAULT_FUNDING_HINT_PASS_LIMIT: i64 = 25;
const PROVIDER_OPERATION_HINT_CHANNEL: &str = "router_provider_operation_hints";
const VAULT_FUNDING_HINT_CHANNEL: &str = "router_vault_funding_hints";
const VAULT_REFUND_WAKEUP_CHANNEL: &str = "router_vault_refund_wakeups";
const QUOTE_CLEANUP_INTERVAL: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone)]
pub struct RouterWorkerConfig {
    pub database_url: String,
    pub worker_id: String,
    pub lease_name: String,
    pub lease_duration: ChronoDuration,
    pub lease_renew_interval: Duration,
    pub standby_poll_interval: Duration,
    pub route_cost_refresh_interval: Duration,
    pub provider_health_poll_interval: Duration,
}

impl RouterWorkerConfig {
    pub fn from_args(args: &RouterServerArgs) -> Result<Self> {
        ensure_positive_seconds("worker lease seconds", args.worker_lease_seconds)?;
        ensure_positive_seconds(
            "worker lease renew seconds",
            args.worker_lease_renew_seconds,
        )?;
        ensure_positive_seconds(
            "worker standby poll seconds",
            args.worker_standby_poll_seconds,
        )?;
        ensure_positive_seconds(
            "worker route-cost refresh seconds",
            args.worker_route_cost_refresh_seconds,
        )?;
        ensure_positive_seconds(
            "worker provider-health poll seconds",
            args.worker_provider_health_poll_seconds,
        )?;
        ensure_positive_seconds(
            "provider health timeout seconds",
            args.provider_health_timeout_seconds,
        )?;
        if args.worker_lease_renew_seconds >= args.worker_lease_seconds {
            return Err(invalid_worker_config(
                "worker lease renew seconds must be less than worker lease seconds",
            ));
        }

        let lease_duration = ChronoDuration::from_std(Duration::from_secs(
            args.worker_lease_seconds,
        ))
        .map_err(|err| invalid_worker_config(&format!("invalid worker lease duration: {err}")))?;

        Ok(Self {
            database_url: args.database_url.clone(),
            worker_id: args
                .worker_id
                .clone()
                .unwrap_or_else(|| format!("router-worker-{}", Uuid::now_v7())),
            lease_name: args
                .worker_lease_name
                .clone()
                .unwrap_or_else(|| DEFAULT_WORKER_LEASE_NAME.to_string()),
            lease_duration,
            lease_renew_interval: Duration::from_secs(args.worker_lease_renew_seconds),
            standby_poll_interval: Duration::from_secs(args.worker_standby_poll_seconds),
            route_cost_refresh_interval: Duration::from_secs(
                args.worker_route_cost_refresh_seconds,
            ),
            provider_health_poll_interval: Duration::from_secs(
                args.worker_provider_health_poll_seconds,
            ),
        })
    }
}

#[derive(Debug)]
enum OrderWorkBatch {
    Global,
    Orders(Vec<Uuid>),
}

#[derive(Debug, Default)]
struct OrderWakeQueue {
    global: bool,
    queued_order_ids: HashSet<Uuid>,
    order_ids: VecDeque<Uuid>,
}

impl OrderWakeQueue {
    fn enqueue_global(&mut self) {
        self.global = true;
    }

    fn enqueue_order(&mut self, order_id: Uuid) {
        if self.queued_order_ids.insert(order_id) {
            self.order_ids.push_back(order_id);
        }
    }

    fn enqueue_orders(&mut self, order_ids: impl IntoIterator<Item = Uuid>) {
        for order_id in order_ids {
            self.enqueue_order(order_id);
        }
    }

    fn take_batch(&mut self) -> Option<OrderWorkBatch> {
        if self.global {
            self.global = false;
            return Some(OrderWorkBatch::Global);
        }

        let mut order_ids = Vec::with_capacity(ORDER_WAKE_BATCH_LIMIT);
        while order_ids.len() < ORDER_WAKE_BATCH_LIMIT {
            let Some(order_id) = self.order_ids.pop_front() else {
                break;
            };
            self.queued_order_ids.remove(&order_id);
            order_ids.push(order_id);
        }

        if order_ids.is_empty() {
            None
        } else {
            Some(OrderWorkBatch::Orders(order_ids))
        }
    }
}

#[derive(Debug, Clone, Default)]
struct VaultWorkOutcome {
    funding_hints: FundingHintPassSummary,
    refunds: RefundPassSummary,
}

type VaultTaskResult = std::result::Result<VaultWorkOutcome, String>;
type OrderTaskResult = std::result::Result<OrderWorkerPassSummary, String>;
type RouteCostTaskResult = std::result::Result<RouteCostRefreshSummary, String>;
type ProviderHealthTaskResult = std::result::Result<ProviderHealthPollSummary, String>;
type QuoteCleanupTaskResult = std::result::Result<u64, String>;

pub async fn run_worker(args: RouterServerArgs) -> Result<()> {
    let config = RouterWorkerConfig::from_args(&args)?;
    info!(
        worker_id = %config.worker_id,
        lease_name = %config.lease_name,
        "Starting router-worker"
    );
    let components = initialize_components(
        &args,
        Some(config.worker_id.clone()),
        PaymasterMode::Enabled,
    )
    .await?;

    run_worker_loop(
        components.db,
        components.vault_manager,
        components.order_execution_manager,
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
    order_execution_manager: Arc<OrderExecutionManager>,
    route_costs: Arc<RouteCostService>,
    provider_health_poller: Arc<ProviderHealthPoller>,
    config: RouterWorkerConfig,
) -> BackgroundTaskResult {
    let lease_repo = db.worker_leases();
    let mut active_lease: Option<WorkerLease> = None;
    let mut lease_renew_interval = interval(config.lease_renew_interval);
    lease_renew_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut route_cost_refresh_interval = interval(config.route_cost_refresh_interval);
    route_cost_refresh_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut provider_health_poll_interval = interval(config.provider_health_poll_interval);
    provider_health_poll_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut quote_cleanup_interval = interval(QUOTE_CLEANUP_INTERVAL);
    quote_cleanup_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut worker_listener = PgListener::connect(&config.database_url)
        .await
        .map_err(|err| format!("failed to connect worker listener: {err}"))?;
    worker_listener
        .listen(PROVIDER_OPERATION_HINT_CHANNEL)
        .await
        .map_err(|err| format!("failed to listen for provider operation hints: {err}"))?;
    worker_listener
        .listen(VAULT_FUNDING_HINT_CHANNEL)
        .await
        .map_err(|err| format!("failed to listen for vault funding hints: {err}"))?;
    worker_listener
        .listen(VAULT_REFUND_WAKEUP_CHANNEL)
        .await
        .map_err(|err| format!("failed to listen for vault refund wakeups: {err}"))?;
    let mut vault_tasks: JoinSet<VaultTaskResult> = JoinSet::new();
    let mut order_execution_tasks: JoinSet<OrderTaskResult> = JoinSet::new();
    let mut route_cost_tasks: JoinSet<RouteCostTaskResult> = JoinSet::new();
    let mut provider_health_tasks: JoinSet<ProviderHealthTaskResult> = JoinSet::new();
    let mut quote_cleanup_tasks: JoinSet<QuoteCleanupTaskResult> = JoinSet::new();
    let mut order_wake_queue = OrderWakeQueue::default();
    let mut pending_vault_wakeup = false;
    let mut pending_route_cost_refresh = false;
    let mut pending_provider_health_poll = false;
    let mut pending_quote_cleanup = false;
    let mut next_refund_due_at: Option<DateTime<Utc>> = None;

    loop {
        if active_lease.is_none() {
            match try_acquire_worker_lease(&db, &config).await {
                Ok(Some(lease)) => {
                    info!(
                        worker_id = %config.worker_id,
                        lease_name = %lease.lease_name,
                        fencing_token = lease.fencing_token,
                        expires_at = %lease.expires_at,
                        "Router worker became active"
                    );
                    telemetry::record_worker_active(true);
                    active_lease = Some(lease);
                    lease_renew_interval.reset();
                    route_cost_refresh_interval.reset();
                    provider_health_poll_interval.reset();
                    quote_cleanup_interval.reset();
                    order_wake_queue.enqueue_global();
                    spawn_order_execution_work_if_idle(
                        &mut order_execution_tasks,
                        order_execution_manager.clone(),
                        &mut order_wake_queue,
                    );
                    pending_vault_wakeup = true;
                    spawn_vault_work_if_idle(
                        &mut vault_tasks,
                        vault_manager.clone(),
                        &mut pending_vault_wakeup,
                    );
                    pending_route_cost_refresh = true;
                    spawn_route_cost_refresh_if_idle(
                        &mut route_cost_tasks,
                        route_costs.clone(),
                        &mut pending_route_cost_refresh,
                    );
                    pending_provider_health_poll = true;
                    spawn_provider_health_poll_if_idle(
                        &mut provider_health_tasks,
                        provider_health_poller.clone(),
                        &mut pending_provider_health_poll,
                    );
                    pending_quote_cleanup = true;
                    spawn_quote_cleanup_if_idle(
                        &mut quote_cleanup_tasks,
                        db.clone(),
                        &mut pending_quote_cleanup,
                    );
                    next_refund_due_at = refresh_next_refund_due_at(&db).await?;
                }
                Ok(None) => {
                    telemetry::record_worker_active(false);
                    debug!(
                        worker_id = %config.worker_id,
                        lease_name = %config.lease_name,
                        "Router worker is standby"
                    );
                    sleep(config.standby_poll_interval).await;
                }
                Err(err) => {
                    telemetry::record_worker_lease_event("acquire", "error");
                    return Err(format!("failed to acquire worker lease: {err}"));
                }
            }
            continue;
        }

        tokio::select! {
            _ = lease_renew_interval.tick() => {
                match renew_worker_lease(&lease_repo, &config, active_lease.as_ref().expect("active lease")).await {
                    Ok(Some(lease)) => {
                        active_lease = Some(lease);
                    }
                    Ok(None) => {
                        warn!(
                            worker_id = %config.worker_id,
                            lease_name = %config.lease_name,
                            "Router worker lost leadership lease"
                        );
                        telemetry::record_worker_active(false);
                        active_lease = None;
                        abort_vault_tasks(&mut vault_tasks).await;
                        abort_order_execution_tasks(&mut order_execution_tasks).await;
                        abort_route_cost_tasks(&mut route_cost_tasks).await;
                        abort_provider_health_tasks(&mut provider_health_tasks).await;
                        abort_quote_cleanup_tasks(&mut quote_cleanup_tasks).await;
                        order_wake_queue = OrderWakeQueue::default();
                        pending_vault_wakeup = false;
                        pending_route_cost_refresh = false;
                        pending_provider_health_poll = false;
                        pending_quote_cleanup = false;
                        next_refund_due_at = None;
                    }
                    Err(err) => {
                        telemetry::record_worker_lease_event("renew", "error");
                        return Err(format!("failed to renew worker lease: {err}"));
                    }
                }
            }
            _ = sleep(refund_due_delay(next_refund_due_at.as_ref())), if next_refund_due_at.is_some() && vault_tasks.is_empty() => {
                next_refund_due_at = None;
                pending_vault_wakeup = true;
                spawn_vault_work_if_idle(
                    &mut vault_tasks,
                    vault_manager.clone(),
                    &mut pending_vault_wakeup,
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
            hint = worker_listener.recv() => {
                match hint {
                    Ok(notification) => {
                        debug!(
                            channel = notification.channel(),
                            payload = notification.payload(),
                            "Router worker received hint wakeup"
                        );
                        match notification.channel() {
                            PROVIDER_OPERATION_HINT_CHANNEL => {
                                order_wake_queue.enqueue_global();
                                spawn_order_execution_work_if_idle(
                                    &mut order_execution_tasks,
                                    order_execution_manager.clone(),
                                    &mut order_wake_queue,
                                );
                            }
                            VAULT_FUNDING_HINT_CHANNEL | VAULT_REFUND_WAKEUP_CHANNEL => {
                                pending_vault_wakeup = true;
                                spawn_vault_work_if_idle(
                                    &mut vault_tasks,
                                    vault_manager.clone(),
                                    &mut pending_vault_wakeup,
                                );
                            }
                            _ => {}
                        }
                    }
                    Err(err) => {
                        return Err(format!("worker hint listener failed: {err}"));
                    }
                }
            }
            vault_result = vault_tasks.join_next(), if !vault_tasks.is_empty() => {
                let outcome = handle_vault_task_result(vault_result)?;
                order_wake_queue.enqueue_orders(outcome.funding_hints.funded_order_ids);
                order_wake_queue.enqueue_orders(outcome.refunds.refunded_order_ids);
                spawn_order_execution_work_if_idle(
                    &mut order_execution_tasks,
                    order_execution_manager.clone(),
                    &mut order_wake_queue,
                );
                next_refund_due_at = refresh_next_refund_due_at(&db).await?;
                spawn_vault_work_if_idle(
                    &mut vault_tasks,
                    vault_manager.clone(),
                    &mut pending_vault_wakeup,
                );
            }
            order_execution_result = order_execution_tasks.join_next(), if !order_execution_tasks.is_empty() => {
                let summary = handle_order_execution_task_result(order_execution_result)?;
                if summary.has_activity() {
                    order_wake_queue.enqueue_global();
                }
                spawn_order_execution_work_if_idle(
                    &mut order_execution_tasks,
                    order_execution_manager.clone(),
                    &mut order_wake_queue,
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

async fn try_acquire_worker_lease(
    db: &Database,
    config: &RouterWorkerConfig,
) -> crate::error::RouterServerResult<Option<WorkerLease>> {
    let now = Utc::now();
    let lease = db
        .worker_leases()
        .try_acquire(
            &config.lease_name,
            &config.worker_id,
            now,
            now + config.lease_duration,
        )
        .await?;
    telemetry::record_worker_lease_event(
        "acquire",
        if lease.is_some() {
            "acquired"
        } else {
            "standby"
        },
    );
    Ok(lease)
}

async fn renew_worker_lease(
    lease_repo: &crate::db::WorkerLeaseRepository,
    config: &RouterWorkerConfig,
    lease: &WorkerLease,
) -> crate::error::RouterServerResult<Option<WorkerLease>> {
    let now = Utc::now();
    let lease = lease_repo
        .renew(
            &config.lease_name,
            &config.worker_id,
            lease.fencing_token,
            now,
            now + config.lease_duration,
        )
        .await?;
    telemetry::record_worker_lease_event("renew", if lease.is_some() { "renewed" } else { "lost" });
    Ok(lease)
}

fn spawn_vault_work_if_idle(
    vault_tasks: &mut JoinSet<VaultTaskResult>,
    vault_manager: Arc<VaultManager>,
    pending_vault_wakeup: &mut bool,
) {
    if vault_tasks.is_empty() && *pending_vault_wakeup {
        *pending_vault_wakeup = false;
        vault_tasks.spawn(run_vault_work_pass(vault_manager));
    }
}

fn spawn_order_execution_work_if_idle(
    order_execution_tasks: &mut JoinSet<OrderTaskResult>,
    order_execution_manager: Arc<OrderExecutionManager>,
    order_wake_queue: &mut OrderWakeQueue,
) {
    if order_execution_tasks.is_empty() {
        if let Some(batch) = order_wake_queue.take_batch() {
            order_execution_tasks.spawn(run_order_execution_work_batch(
                order_execution_manager,
                batch,
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

async fn run_vault_work_pass(vault_manager: Arc<VaultManager>) -> VaultTaskResult {
    let started = Instant::now();
    let funding_hints = vault_manager
        .process_funding_hints_detailed(VAULT_FUNDING_HINT_PASS_LIMIT)
        .await
        .map_err(|err| err.to_string())?;
    if funding_hints.processed > 0 {
        info!(
            processed_funding_hints = funding_hints.processed,
            funded_orders = funding_hints.funded_order_ids.len(),
            "Router worker processed vault funding hints"
        );
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
    Ok(VaultWorkOutcome {
        funding_hints,
        refunds,
    })
}

async fn run_order_execution_work_batch(
    order_execution_manager: Arc<OrderExecutionManager>,
    batch: OrderWorkBatch,
) -> OrderTaskResult {
    let started = Instant::now();
    let summary = match batch {
        OrderWorkBatch::Global => {
            order_execution_manager
                .process_worker_pass(ORDER_EXECUTION_PASS_LIMIT)
                .await
        }
        OrderWorkBatch::Orders(order_ids) => {
            order_execution_manager.process_order_ids(&order_ids).await
        }
    }
    .map_err(|err| err.to_string())?;
    if summary.has_activity() {
        info!(
            reconciled_failed_orders = summary.reconciled_failed_orders,
            processed_provider_hints = summary.processed_provider_hints,
            maintenance_tasks = summary.maintenance_tasks,
            planned_orders = summary.planned_orders,
            executed_orders = summary.executed_orders,
            "Router worker processed orders"
        );
    }
    telemetry::record_worker_tick("order_executor", started.elapsed());
    Ok(summary)
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

fn handle_order_execution_task_result(
    result: Option<std::result::Result<OrderTaskResult, JoinError>>,
) -> std::result::Result<OrderWorkerPassSummary, String> {
    match result {
        Some(Ok(Ok(summary))) => Ok(summary),
        Some(Ok(Err(error))) => Err(format!("order executor pass failed: {error}")),
        Some(Err(error)) => Err(format!(
            "order executor pass panicked or was cancelled: {error}"
        )),
        None => Err("order executor task set terminated unexpectedly".to_string()),
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

async fn abort_vault_tasks(vault_tasks: &mut JoinSet<VaultTaskResult>) {
    vault_tasks.abort_all();
    while vault_tasks.join_next().await.is_some() {}
}

async fn abort_order_execution_tasks(order_execution_tasks: &mut JoinSet<OrderTaskResult>) {
    order_execution_tasks.abort_all();
    while order_execution_tasks.join_next().await.is_some() {}
}

async fn abort_route_cost_tasks(route_cost_tasks: &mut JoinSet<RouteCostTaskResult>) {
    route_cost_tasks.abort_all();
    while route_cost_tasks.join_next().await.is_some() {}
}

async fn abort_provider_health_tasks(
    provider_health_tasks: &mut JoinSet<ProviderHealthTaskResult>,
) {
    provider_health_tasks.abort_all();
    while provider_health_tasks.join_next().await.is_some() {}
}

async fn abort_quote_cleanup_tasks(quote_cleanup_tasks: &mut JoinSet<QuoteCleanupTaskResult>) {
    quote_cleanup_tasks.abort_all();
    while quote_cleanup_tasks.join_next().await.is_some() {}
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

fn invalid_worker_config(message: &str) -> Error {
    Error::Generic {
        source: Whatever::without_source(message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_wake_queue_dedupes_order_ids_and_prioritizes_global_work() {
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let mut queue = OrderWakeQueue::default();

        queue.enqueue_order(first);
        queue.enqueue_order(first);
        queue.enqueue_order(second);
        queue.enqueue_global();

        assert!(matches!(queue.take_batch(), Some(OrderWorkBatch::Global)));
        match queue.take_batch() {
            Some(OrderWorkBatch::Orders(order_ids)) => {
                assert_eq!(order_ids, vec![first, second]);
            }
            other => panic!("expected deduped order batch, got {other:?}"),
        }
        assert!(queue.take_batch().is_none());
    }

    #[test]
    fn refund_due_delay_is_zero_for_due_or_overdue_work() {
        assert_eq!(refund_due_delay(Some(&Utc::now())), Duration::ZERO);
        assert_eq!(
            refund_due_delay(Some(&(Utc::now() - ChronoDuration::seconds(1)))),
            Duration::ZERO
        );
    }
}
