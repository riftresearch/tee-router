use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::Address;
use chrono::Utc;
use hyperliquid_client::{HyperliquidInfoClient, OrderStatusResponse};
use snafu::ResultExt;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, sleep_until};
use tracing::{debug, warn};

use crate::{
    config::Cadences,
    error::HyperliquidRequestSnafu,
    ingest::{
        decode_fills, decode_funding, decode_ledger_update,
        schema::{HlOrderEvent, HlOrderStatus},
        PubSub, StreamEvent,
    },
    metadata::MetadataSnapshot,
    storage::{CursorEndpoint, Storage},
    telemetry, Result,
};

const CURSOR_SAFETY_WINDOW_MS: i64 = 5_000;
const REBUCKET_INTERVAL: Duration = Duration::from_secs(10);
const METADATA_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const RECENT_ACTIVITY_WINDOW_MS: i64 = 5 * 60 * 1000;
const RATE_LIMIT_ENDPOINT: &str = "userRateLimit";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bucket {
    Hot,
    Warm,
    Cold,
}

impl Bucket {
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Hot => "hot",
            Self::Warm => "warm",
            Self::Cold => "cold",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PollEndpoint {
    Fills,
    Ledger,
    Funding,
    OrderStatus(u64),
}

impl PollEndpoint {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Fills => "fills",
            Self::Ledger => "ledger",
            Self::Funding => "funding",
            Self::OrderStatus(_) => "orderStatus",
        }
    }

    #[must_use]
    pub fn weight(self) -> u32 {
        match self {
            Self::OrderStatus(_) => 2,
            _ => 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledPoll {
    pub user: Address,
    pub endpoint: PollEndpoint,
    pub deadline: Instant,
}

impl Ord for ScheduledPoll {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| self.user.cmp(&other.user))
    }
}

impl PartialOrd for ScheduledPoll {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<SchedulerInner>>,
    cadences: Cadences,
}

#[derive(Debug)]
struct SchedulerInner {
    users: HashMap<Address, UserState>,
    pending_orders: HashMap<u64, Address>,
    heap: BinaryHeap<ScheduledPoll>,
    slow_until: Option<Instant>,
}

#[derive(Debug, Clone)]
struct UserState {
    bucket: Bucket,
    active_subscribers: usize,
    last_activity_ms: i64,
}

impl Scheduler {
    #[must_use]
    pub fn new(cadences: Cadences) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SchedulerInner {
                users: HashMap::new(),
                pending_orders: HashMap::new(),
                heap: BinaryHeap::new(),
                slow_until: None,
            })),
            cadences,
        }
    }

    pub async fn register_user(&self, user: Address) {
        let mut inner = self.inner.lock().await;
        let inserted = inner.users.insert(
            user,
            UserState {
                bucket: Bucket::Warm,
                active_subscribers: 0,
                last_activity_ms: now_ms(),
            },
        );
        if inserted.is_none() {
            let now = Instant::now();
            inner.heap.push(ScheduledPoll {
                user,
                endpoint: PollEndpoint::Fills,
                deadline: now,
            });
            inner.heap.push(ScheduledPoll {
                user,
                endpoint: PollEndpoint::Ledger,
                deadline: now,
            });
            inner.heap.push(ScheduledPoll {
                user,
                endpoint: PollEndpoint::Funding,
                deadline: now,
            });
        }
    }

    pub async fn add_subscriber(&self, user: Address) {
        self.register_user(user).await;
        let mut inner = self.inner.lock().await;
        if let Some(state) = inner.users.get_mut(&user) {
            state.active_subscribers = state.active_subscribers.saturating_add(1);
            state.bucket = Bucket::Hot;
        }
    }

    pub async fn remove_subscriber(&self, user: Address) {
        let mut inner = self.inner.lock().await;
        if let Some(state) = inner.users.get_mut(&user) {
            state.active_subscribers = state.active_subscribers.saturating_sub(1);
        }
    }

    pub async fn mark_activity(&self, user: Address) {
        let mut inner = self.inner.lock().await;
        if let Some(state) = inner.users.get_mut(&user) {
            state.last_activity_ms = now_ms();
        }
    }

    pub async fn register_pending_order(&self, user: Address, oid: u64) {
        self.register_user(user).await;
        let mut inner = self.inner.lock().await;
        inner.pending_orders.insert(oid, user);
        inner.heap.push(ScheduledPoll {
            user,
            endpoint: PollEndpoint::OrderStatus(oid),
            deadline: Instant::now(),
        });
    }

    pub async fn deregister_pending_order(&self, oid: u64) {
        self.inner.lock().await.pending_orders.remove(&oid);
    }

    pub async fn pending_orders(&self) -> Vec<(Address, u64)> {
        self.inner
            .lock()
            .await
            .pending_orders
            .iter()
            .map(|(oid, user)| (*user, *oid))
            .collect()
    }

    pub async fn next_due(&self) -> ScheduledPoll {
        loop {
            let maybe_due = {
                let mut inner = self.inner.lock().await;
                inner.heap.pop()
            };
            if let Some(next) = maybe_due {
                if !self.poll_is_still_registered(next).await {
                    continue;
                }
                let now = Instant::now();
                if next.deadline > now {
                    sleep_until(next.deadline.into()).await;
                }
                return next;
            }
            sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn reschedule(&self, poll: ScheduledPoll) {
        let mut inner = self.inner.lock().await;
        if let PollEndpoint::OrderStatus(oid) = poll.endpoint {
            if inner.pending_orders.get(&oid) != Some(&poll.user) {
                return;
            }
        }
        let Some(state) = inner.users.get(&poll.user) else {
            return;
        };
        let slow = inner.slow_until.is_some_and(|until| until > Instant::now());
        let delay = self.delay_for(state.bucket, poll.endpoint, slow);
        inner.heap.push(ScheduledPoll {
            deadline: Instant::now() + delay,
            ..poll
        });
    }

    async fn poll_is_still_registered(&self, poll: ScheduledPoll) -> bool {
        if let PollEndpoint::OrderStatus(oid) = poll.endpoint {
            return self.inner.lock().await.pending_orders.get(&oid) == Some(&poll.user);
        }
        true
    }

    pub async fn rebucket_once(&self) {
        let mut inner = self.inner.lock().await;
        let mut counts = [0usize; 3];
        let now = now_ms();
        for state in inner.users.values_mut() {
            state.bucket = if state.active_subscribers > 0 {
                Bucket::Hot
            } else if now.saturating_sub(state.last_activity_ms) < RECENT_ACTIVITY_WINDOW_MS {
                Bucket::Warm
            } else {
                Bucket::Cold
            };
            match state.bucket {
                Bucket::Hot => counts[0] += 1,
                Bucket::Warm => counts[1] += 1,
                Bucket::Cold => counts[2] += 1,
            }
        }
        telemetry::record_user_bucket(Bucket::Hot.label(), counts[0]);
        telemetry::record_user_bucket(Bucket::Warm.label(), counts[1]);
        telemetry::record_user_bucket(Bucket::Cold.label(), counts[2]);
    }

    pub async fn users(&self) -> Vec<Address> {
        self.inner.lock().await.users.keys().copied().collect()
    }

    pub async fn slow_for(&self, duration: Duration) {
        self.inner.lock().await.slow_until = Some(Instant::now() + duration);
    }

    fn delay_for(&self, bucket: Bucket, endpoint: PollEndpoint, slow: bool) -> Duration {
        let base = match endpoint {
            PollEndpoint::OrderStatus(_) => self.cadences.order_status,
            PollEndpoint::Funding => match bucket {
                Bucket::Hot => self.cadences.funding_hot,
                Bucket::Warm => self.cadences.funding_warm,
                Bucket::Cold => self.cadences.funding_cold,
            },
            PollEndpoint::Fills | PollEndpoint::Ledger => match bucket {
                Bucket::Hot => self.cadences.hot,
                Bucket::Warm => self.cadences.warm,
                Bucket::Cold => self.cadences.cold,
            },
        };
        if slow {
            base.saturating_mul(2)
        } else {
            base
        }
    }
}

#[derive(Debug)]
pub struct WeightBudget {
    max_per_minute: u32,
    recent: Mutex<VecDeque<(Instant, u32)>>,
}

impl WeightBudget {
    #[must_use]
    pub fn new(max_per_minute: u32) -> Self {
        Self {
            max_per_minute,
            recent: Mutex::new(VecDeque::new()),
        }
    }

    pub async fn acquire(&self, weight: u32) {
        loop {
            let mut recent = self.recent.lock().await;
            prune_old(&mut recent);
            let used: u32 = recent.iter().map(|(_, weight)| *weight).sum();
            if used.saturating_add(weight) <= self.max_per_minute {
                recent.push_back((Instant::now(), weight));
                return;
            }
            drop(recent);
            sleep(Duration::from_secs(1)).await;
        }
    }

    pub async fn used_last_minute(&self) -> u32 {
        let mut recent = self.recent.lock().await;
        prune_old(&mut recent);
        recent.iter().map(|(_, weight)| *weight).sum()
    }
}

fn prune_old(recent: &mut VecDeque<(Instant, u32)>) {
    let cutoff = Instant::now() - Duration::from_secs(60);
    while recent.front().is_some_and(|(instant, _)| *instant < cutoff) {
        recent.pop_front();
    }
}

#[derive(Clone)]
pub struct Poller {
    scheduler: Scheduler,
    storage: Storage,
    pubsub: PubSub,
    hl: Arc<Mutex<HyperliquidInfoClient>>,
    metadata: Arc<RwLock<MetadataSnapshot>>,
    weight_budget: Arc<WeightBudget>,
}

impl Poller {
    pub async fn new(
        scheduler: Scheduler,
        storage: Storage,
        pubsub: PubSub,
        info_url: &str,
        weight_budget_per_min: u32,
    ) -> Result<Self> {
        let hl = HyperliquidInfoClient::new(info_url).map_err(|source| {
            crate::Error::HyperliquidRequest {
                endpoint: "client_init",
                source,
            }
        })?;
        let poller = Self {
            scheduler,
            storage,
            pubsub,
            hl: Arc::new(Mutex::new(hl)),
            metadata: Arc::new(RwLock::new(MetadataSnapshot::default())),
            weight_budget: Arc::new(WeightBudget::new(weight_budget_per_min)),
        };
        poller.refresh_metadata().await?;
        Ok(poller)
    }

    #[must_use]
    pub fn scheduler(&self) -> Scheduler {
        self.scheduler.clone()
    }

    pub async fn used_weight_last_minute(&self) -> u32 {
        self.weight_budget.used_last_minute().await
    }

    pub async fn run(self, worker_count: usize) -> Result<()> {
        let mut handles = Vec::new();
        for _ in 0..worker_count {
            let worker = self.clone();
            handles.push(tokio::spawn(async move { worker.run_worker().await }));
        }
        let rebucket = self.scheduler.clone();
        handles.push(tokio::spawn(async move {
            loop {
                sleep(REBUCKET_INTERVAL).await;
                rebucket.rebucket_once().await;
            }
            #[allow(unreachable_code)]
            Ok(())
        }));
        let rate_limit = self.clone();
        handles.push(tokio::spawn(async move {
            rate_limit.run_rate_limit_loop().await
        }));
        let metadata = self.clone();
        handles.push(tokio::spawn(async move {
            loop {
                sleep(METADATA_REFRESH_INTERVAL).await;
                metadata.refresh_metadata().await?;
            }
            #[allow(unreachable_code)]
            Ok(())
        }));
        for handle in handles {
            handle
                .await
                .map_err(|err| crate::Error::InvalidConfiguration {
                    message: format!("poller task join failed: {err}"),
                })??;
        }
        Ok(())
    }

    async fn run_rate_limit_loop(self) -> Result<()> {
        loop {
            sleep(Duration::from_secs(60)).await;
            let Some(user) = self.scheduler.users().await.into_iter().next() else {
                continue;
            };
            let used = self.check_rate_limit(user).await?;
            if used > u64::from(self.weight_budget.max_per_minute.saturating_sub(100)) {
                self.scheduler.slow_for(Duration::from_secs(60)).await;
                warn!(
                    user = ?user,
                    used,
                    budget = self.weight_budget.max_per_minute,
                    "Hyperliquid rate-limit headroom low; slowing poll cadences for one minute"
                );
            }
        }
    }

    async fn run_worker(self) -> Result<()> {
        loop {
            let poll = self.scheduler.next_due().await;
            if let Err(error) = self.poll_once(poll).await {
                warn!(?error, endpoint = poll.endpoint.name(), user = ?poll.user, "HL shim poll failed");
            }
            self.scheduler.reschedule(poll).await;
        }
    }

    pub async fn poll_once(&self, poll: ScheduledPoll) -> Result<()> {
        self.weight_budget.acquire(poll.endpoint.weight()).await;
        match poll.endpoint {
            PollEndpoint::Fills => self.poll_fills(poll.user).await,
            PollEndpoint::Ledger => self.poll_ledger(poll.user).await,
            PollEndpoint::Funding => self.poll_funding(poll.user).await,
            PollEndpoint::OrderStatus(oid) => self.poll_order_status(poll.user, oid).await,
        }
    }

    async fn refresh_metadata(&self) -> Result<()> {
        self.weight_budget.acquire(40).await;
        let hl = self.hl.lock().await;
        let perp = hl
            .fetch_perp_meta()
            .await
            .context(HyperliquidRequestSnafu { endpoint: "meta" })?;
        let spot = hl
            .fetch_spot_meta()
            .await
            .context(HyperliquidRequestSnafu {
                endpoint: "spotMeta",
            })?;
        *self.metadata.write().await = MetadataSnapshot::new(&perp, &spot);
        Ok(())
    }

    async fn poll_fills(&self, user: Address) -> Result<()> {
        let cursor = self.storage.cursor(user, CursorEndpoint::Fills).await?;
        let start = cursor.saturating_sub(CURSOR_SAFETY_WINDOW_MS).max(0) as u64;
        let fills = {
            let hl = self.hl.lock().await;
            hl.user_fills_by_time(user, start, None, false)
                .await
                .context(HyperliquidRequestSnafu {
                    endpoint: "userFillsByTime",
                })?
        };
        telemetry::record_hl_request("userFillsByTime", "ok", PollEndpoint::Fills.weight());
        let max_time = fills
            .iter()
            .filter_map(|fill| i64::try_from(fill.time).ok())
            .max()
            .unwrap_or(cursor);
        let metadata = self.metadata.read().await.clone();
        for event in decode_fills(user, fills, &metadata)? {
            if self.storage.insert_transfer(&event).await? {
                self.scheduler.mark_activity(user).await;
                self.pubsub.publish(StreamEvent::Transfer { event });
            }
        }
        self.storage
            .set_cursor(user, CursorEndpoint::Fills, max_time)
            .await
    }

    async fn poll_ledger(&self, user: Address) -> Result<()> {
        let cursor = self.storage.cursor(user, CursorEndpoint::Ledger).await?;
        let start = cursor.saturating_sub(CURSOR_SAFETY_WINDOW_MS).max(0) as u64;
        let updates = {
            let hl = self.hl.lock().await;
            hl.user_non_funding_ledger_updates(user, start, None)
                .await
                .context(HyperliquidRequestSnafu {
                    endpoint: "userNonFundingLedgerUpdates",
                })?
        };
        telemetry::record_hl_request(
            "userNonFundingLedgerUpdates",
            "ok",
            PollEndpoint::Ledger.weight(),
        );
        let max_time = updates
            .iter()
            .filter_map(|update| i64::try_from(update.time).ok())
            .max()
            .unwrap_or(cursor);
        for update in updates {
            if let Some(event) = decode_ledger_update(user, update)? {
                if self.storage.insert_transfer(&event).await? {
                    self.scheduler.mark_activity(user).await;
                    self.pubsub.publish(StreamEvent::Transfer { event });
                }
            }
        }
        self.storage
            .set_cursor(user, CursorEndpoint::Ledger, max_time)
            .await
    }

    async fn poll_funding(&self, user: Address) -> Result<()> {
        let cursor = self.storage.cursor(user, CursorEndpoint::Funding).await?;
        let start = cursor.saturating_sub(CURSOR_SAFETY_WINDOW_MS).max(0) as u64;
        let fundings = {
            let hl = self.hl.lock().await;
            hl.user_funding(user, start, None)
                .await
                .context(HyperliquidRequestSnafu {
                    endpoint: "userFunding",
                })?
        };
        telemetry::record_hl_request("userFunding", "ok", PollEndpoint::Funding.weight());
        let max_time = fundings
            .iter()
            .filter_map(|funding| i64::try_from(funding.time).ok())
            .max()
            .unwrap_or(cursor);
        for event in decode_funding(user, fundings)? {
            if self.storage.insert_transfer(&event).await? {
                self.scheduler.mark_activity(user).await;
                self.pubsub.publish(StreamEvent::Transfer { event });
            }
        }
        self.storage
            .set_cursor(user, CursorEndpoint::Funding, max_time)
            .await
    }

    async fn poll_order_status(&self, user: Address, oid: u64) -> Result<()> {
        let response = {
            let hl = self.hl.lock().await;
            hl.order_status(user, oid)
                .await
                .context(HyperliquidRequestSnafu {
                    endpoint: "orderStatus",
                })?
        };
        telemetry::record_hl_request("orderStatus", "ok", PollEndpoint::OrderStatus(oid).weight());
        if let Some(event) = order_event_from_status(user, response) {
            if self.storage.insert_order(&event).await? {
                self.scheduler.mark_activity(user).await;
                self.pubsub.publish(StreamEvent::Order { event });
            }
        }
        Ok(())
    }

    pub async fn check_rate_limit(&self, user: Address) -> Result<u64> {
        self.weight_budget.acquire(2).await;
        let rate_limit = {
            let hl = self.hl.lock().await;
            hl.user_rate_limit(user)
                .await
                .context(HyperliquidRequestSnafu {
                    endpoint: RATE_LIMIT_ENDPOINT,
                })?
        };
        telemetry::record_hl_request(RATE_LIMIT_ENDPOINT, "ok", 2);
        debug!(
            user = ?user,
            n_requests_used = rate_limit.n_requests_used,
            n_requests_cap = rate_limit.n_requests_cap,
            "checked Hyperliquid user rate limit"
        );
        Ok(rate_limit.n_requests_used)
    }
}

fn order_event_from_status(user: Address, response: OrderStatusResponse) -> Option<HlOrderEvent> {
    let envelope = response.order?;
    Some(HlOrderEvent {
        user,
        oid: envelope.order.oid,
        cloid: envelope.order.cloid,
        coin: envelope.order.coin,
        side: envelope.order.side,
        limit_px: envelope.order.limit_px,
        sz: envelope.order.sz,
        orig_sz: envelope.order.orig_sz,
        status: HlOrderStatus::from(envelope.status),
        status_timestamp_ms: i64::try_from(envelope.status_timestamp).unwrap_or(i64::MAX),
        observed_at_ms: now_ms(),
    })
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cadences() -> Cadences {
        Cadences {
            hot: Duration::from_millis(5),
            warm: Duration::from_millis(30),
            cold: Duration::from_millis(120),
            funding_hot: Duration::from_millis(60),
            funding_warm: Duration::from_millis(300),
            funding_cold: Duration::from_millis(1_800),
            order_status: Duration::from_millis(2),
        }
    }

    #[tokio::test]
    async fn scheduler_returns_heap_ordered_polls_and_hot_subscriber_moves_cadence_up() {
        let scheduler = Scheduler::new(cadences());
        let user = Address::repeat_byte(7);
        scheduler.add_subscriber(user).await;

        let first = scheduler.next_due().await;
        assert_eq!(first.user, user);
        scheduler.reschedule(first).await;
        scheduler.rebucket_once().await;
        let inner = scheduler.inner.lock().await;
        assert_eq!(inner.users.get(&user).expect("user").bucket, Bucket::Hot);
    }

    #[tokio::test]
    async fn weight_budget_limits_requests_per_minute() {
        let budget = WeightBudget::new(25);
        budget.acquire(20).await;
        assert_eq!(budget.used_last_minute().await, 20);
    }
}
