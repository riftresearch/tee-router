use crate::{
    db::Database,
    error::{RouterCoreError, RouterCoreResult},
    models::ProviderOrderKind,
    protocol::DepositAsset,
    services::{
        action_providers::{
            ActionProviderRegistry, BridgeQuoteRequest, ExchangeQuoteRequest, UnitFeeDirection,
        },
        asset_registry::{
            AssetRegistry, ChainAsset, MarketOrderTransitionKind, TransitionDecl, TransitionPath,
        },
        pricing::{checked_pow10, PricingSnapshotProvider, STATIC_BOOTSTRAP_PRICING_SOURCE},
        pricing::{PricingSnapshot, BPS_DENOMINATOR, USD_MICRO},
    },
};
use alloy::primitives::{U256, U512};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::stream::{FuturesUnordered, StreamExt};
use market_pricing::MarketPricingOracle;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time::timeout;
use tracing::{debug, warn};

const DEFAULT_REFRESH_TTL: Duration = Duration::from_secs(600);
/// How long a sampled route-cost snapshot stays valid for ranking. This is
/// intentionally decoupled from [`DEFAULT_REFRESH_TTL`] (which only gates
/// pricing-oracle freshness): the paced refresher spreads one full sweep over
/// the ~30 min `window`, so a snapshot must stay "active" until its next
/// refresh or the ranker would treat freshly-curated legs as uncached for most
/// of every window. We set this to the refresh window plus margin so the last
/// sampled value for each `(transition, tier)` is always considered valid.
const DEFAULT_ROUTE_COST_SNAPSHOT_TTL: Duration = Duration::from_secs(2400);
/// Tier label corresponding to [`DEFAULT_SAMPLE_AMOUNT_USD_MICROS`].
/// Used by tests and by callers that intentionally pin to a single anchor
/// (V1 limit orders, temporal boundary requote, offline trace harness).
pub const DEFAULT_AMOUNT_BUCKET: &str = "usd_1000";
/// Anchor explicitly used by callers that do not invest in amount-aware
/// ranking: V1 limit orders (audit S1.10), the temporal boundary requote
/// fallback (audit S1.8), and the offline `asset_registry` trace harness.
/// Production market `/quote` flows through `select_route_cost_tier` and
/// never uses this constant - it derives the request size from
/// `request.amount_in` via `raw_amount_to_usd_micros`.
pub const DEFAULT_SAMPLE_AMOUNT_USD_MICROS: u64 = 1_000 * USD_MICRO;
const ROUTE_COST_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
const DUMMY_EVM_DEPOSITOR: &str = "0x1111111111111111111111111111111111111111";
const DUMMY_EVM_RECIPIENT: &str = "0x2222222222222222222222222222222222222222";

/// Maximum number of concurrent provider sampling calls during
/// `refresh_anchor_costs`. `tiers x edges` is ~360 work items; the live
/// bridge calls (~50/cycle) are the only network-bound ones. 16 keeps the
/// cycle under ~30s on a fresh cache while leaving headroom for provider rate
/// limits.
const REFRESH_FANOUT_PERMITS: usize = 16;

/// Maximum number of times the retry queue will re-attempt a failed cell before
/// giving up and letting the normal paced sweep cover it on the next window.
const RETRY_MAX_ATTEMPTS: u32 = 5;
/// Upper bound on how many due retries are folded into a single refresh tick,
/// so retries cannot burst past the one-by-one pacing or the fanout cap.
const MAX_RETRIES_PER_TICK: usize = 8;
/// Backoff base/cap for ordinary failures: `base * 2^(attempts-1)`, clamped.
const RETRY_BASE_DELAY: Duration = Duration::from_secs(20);
const RETRY_MAX_DELAY: Duration = Duration::from_secs(300);
/// Rate-limited failures back off harder so retries do not amplify throttling.
const RETRY_RATE_LIMITED_BASE_DELAY: Duration = Duration::from_secs(60);
const RETRY_RATE_LIMITED_MAX_DELAY: Duration = Duration::from_secs(600);

/// Identifies a single cacheable cell: `(transition_id, tier_label)`.
type RetryKey = (String, String);

/// Cost cache tier. Each tier is one row of `router_route_cost_snapshots`
/// per transition. Quote-time ranking picks the smallest tier whose
/// `sample_usd_micros` is at least the request size (round up to be
/// slippage-conservative).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteCostTier {
    pub label: &'static str,
    pub sample_usd_micros: u64,
}

/// Cost-cache tier ladder. Aligned with the discussed `100 / 500 / 1k / 10k /
/// 25k / 50k / 75k / 100k / 200k / 500k / 1m` set, with extra `5m / 10m`
/// rungs to cover HL-only routes. The `usd_500` rung exists because round-up
/// tier selection otherwise priced every $101-999 request off the $1k sample,
/// overstating fixed-cost legs by up to 10x in bps terms. A request larger
/// than the top tier clamps to the `usd_10000000` bucket; if that bucket has
/// no cached row the leg is simply treated as uncached (no fabricated
/// estimate).
pub const ROUTE_COST_TIERS: &[RouteCostTier] = &[
    RouteCostTier {
        label: "usd_100",
        sample_usd_micros: 100 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_500",
        sample_usd_micros: 500 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_1000",
        sample_usd_micros: 1_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_10000",
        sample_usd_micros: 10_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_25000",
        sample_usd_micros: 25_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_50000",
        sample_usd_micros: 50_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_75000",
        sample_usd_micros: 75_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_100000",
        sample_usd_micros: 100_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_200000",
        sample_usd_micros: 200_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_500000",
        sample_usd_micros: 500_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_1000000",
        sample_usd_micros: 1_000_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_5000000",
        sample_usd_micros: 5_000_000 * USD_MICRO,
    },
    RouteCostTier {
        label: "usd_10000000",
        sample_usd_micros: 10_000_000 * USD_MICRO,
    },
];

/// Look up the cache tier we should rank against for a request of
/// `request_usd_micros`. Rounds up so we err on the side of conservative
/// slippage modeling; clamps to the top tier if the request exceeds the
/// ladder.
#[must_use]
pub fn select_route_cost_tier(request_usd_micros: u64) -> &'static RouteCostTier {
    ROUTE_COST_TIERS
        .iter()
        .find(|tier| tier.sample_usd_micros >= request_usd_micros)
        .unwrap_or_else(|| {
            ROUTE_COST_TIERS
                .last()
                .expect("non-empty route cost tier table")
        })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteCostSnapshot {
    pub transition_id: String,
    pub amount_bucket: String,
    pub provider: String,
    pub edge_kind: String,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub estimated_fee_bps: u64,
    pub estimated_fee_usd_micros: u64,
    pub estimated_gas_usd_micros: u64,
    pub estimated_latency_ms: u64,
    pub sample_amount_usd_micros: u64,
    pub quote_source: String,
    pub refreshed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl RouteCostSnapshot {
    #[must_use]
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }

    #[must_use]
    pub fn effective_cost_usd_micros(&self) -> u64 {
        capped_add_u64(self.estimated_fee_usd_micros, self.estimated_gas_usd_micros)
    }

    #[must_use]
    pub fn effective_cost_bps(&self) -> u64 {
        usd_cost_bps(
            self.effective_cost_usd_micros(),
            self.sample_amount_usd_micros,
        )
    }
}

/// Outcome of a single provider sample, as recorded in the activity feed.
/// Mirrors the attempt arms of [`LiveCostSnapshotOutcome`] minus
/// `NotAttempted` (which never reaches a provider, so it is not an "API hit").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteCostSampleOutcome {
    Succeeded,
    Failed,
    /// The provider was reached but reported a benign, expected condition that
    /// is not a failure - e.g. the orderbook is too shallow to absorb the
    /// sampled tier notional. Recorded for visibility, shown neutrally in the
    /// feed, excluded from the failures panel, and not retried.
    Skipped,
}

impl RouteCostSampleOutcome {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RouteCostSampleOutcome::Succeeded => "succeeded",
            RouteCostSampleOutcome::Failed => "failed",
            RouteCostSampleOutcome::Skipped => "skipped",
        }
    }
}

/// Coarse classification of a failed provider sample, derived heuristically
/// from the provider error string. Drives both the dashboard failures log and
/// the retry backoff (rate-limited failures back off harder so we do not
/// amplify provider throttling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCategory {
    /// HTTP 429 / "rate limit" / "too many requests".
    RateLimited,
    /// Our own provider timeout, or an upstream "timed out".
    Timeout,
    /// Provider answered but has no route for this pair/size.
    NoRoute,
    /// Upstream 5xx (server-side failure).
    UpstreamServer,
    /// Other upstream 4xx (client-side rejection that is not a rate limit).
    UpstreamClient,
    /// Anything we could not classify.
    Other,
}

impl FailureCategory {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            FailureCategory::RateLimited => "rate_limited",
            FailureCategory::Timeout => "timeout",
            FailureCategory::NoRoute => "no_route",
            FailureCategory::UpstreamServer => "upstream_server",
            FailureCategory::UpstreamClient => "upstream_client",
            FailureCategory::Other => "other",
        }
    }
}

/// Best-effort classification of a provider sampling failure from its error
/// string. Heuristic by necessity: provider errors are surfaced as free-text
/// `String`s, so we match on substrings. Order matters - rate limiting is
/// checked before the generic 4xx bucket.
#[must_use]
pub fn classify_failure(reason: &str) -> FailureCategory {
    let lower = reason.to_ascii_lowercase();
    let has = |needle: &str| lower.contains(needle);

    if has("429") || has("rate limit") || has("rate-limit") || has("too many requests") {
        FailureCategory::RateLimited
    } else if has("timed out") || has("timeout") {
        FailureCategory::Timeout
    } else if has("no route") || has("no available route") || has("returned no route") {
        FailureCategory::NoRoute
    } else if has("500") || has("502") || has("503") || has("504") || has("server error") {
        FailureCategory::UpstreamServer
    } else if has("400") || has("401") || has("403") || has("404") || has("422") {
        FailureCategory::UpstreamClient
    } else {
        FailureCategory::Other
    }
}

/// Detect benign "the sampled amount is simply too large for this venue at this
/// tier" conditions, as opposed to genuine provider failures. Covers:
/// - Hyperliquid spot orderbook-depth exhaustion (`could not absorb` /
///   `could not produce` / `empty book side`).
/// - Across `AMOUNT_TOO_HIGH` (HTTP 400; the notional exceeds the route cap).
/// These are recorded as a neutral `skipped` sample rather than a failure, and
/// are not retried (retrying the same oversized tier cannot succeed).
#[must_use]
pub fn is_benign_unsatisfiable_amount(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("could not absorb")
        || lower.contains("could not produce")
        || lower.contains("empty book side")
        || lower.contains("amount_too_high")
        || lower.contains("amount too high")
}

/// One provider sample attempt made by the paced refresher. Written to the
/// `router_route_cost_sample_events` activity feed (best-effort), never read on
/// any quote-time path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteCostSampleEvent {
    pub sampled_at: DateTime<Utc>,
    pub provider: String,
    pub transition_id: String,
    pub amount_bucket: String,
    pub edge_kind: String,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub sample_amount_usd_micros: u64,
    pub outcome: RouteCostSampleOutcome,
    /// Present only when `outcome == Succeeded`.
    pub estimated_fee_bps: Option<u64>,
    /// Present only when `outcome == Succeeded`.
    pub estimated_latency_ms: Option<u64>,
    /// Present when `outcome == Failed` or `outcome == Skipped`.
    pub reason: Option<String>,
    /// Coarse failure classification. Present only when `outcome == Failed`.
    pub failure_category: Option<FailureCategory>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutePathCostScore {
    pub missing_edges: usize,
    pub total_effective_cost_usd_micros: u64,
    pub total_latency_ms: u64,
}

/// A `TransitionPath` ranked at a specific request size, with the per-path
/// score the quote pipeline uses to pick its fixed top-N live-quote fanout.
#[derive(Debug, Clone, PartialEq)]
pub struct RankedTransitionPath {
    pub path: TransitionPath,
    pub score: RoutePathCostScore,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteCostRefreshSummary {
    pub candidate_edges: usize,
    pub snapshots_upserted: usize,
    pub provider_quotes_attempted: usize,
    pub provider_quotes_succeeded: usize,
    pub provider_quotes_failed: usize,
    pub tiers_attempted: usize,
    pub pricing_source: String,
    pub refreshed_at: DateTime<Utc>,
}

/// Position within one provider's current paced refresh cycle. We keep one of
/// these per provider (see [`RouteCostService::refresh_cursors`]) so every
/// venue is its own independent metronome: a provider trickles a single
/// request every `window / provider_cells` regardless of how many cells the
/// other providers have. Shared across all clones of a [`RouteCostService`].
#[derive(Debug, Default, Clone)]
struct ProviderCursor {
    /// When the in-progress cycle began. `None` means the next paced pass
    /// starts a fresh cycle (and re-anchors the deadline clock to "now").
    cycle_started_at: Option<DateTime<Utc>>,
    /// Index of the next work unit to sample in this provider's plan.
    next_unit: usize,
}

/// A failed cell awaiting re-sampling. Held in [`RouteCostService::retry_queue`]
/// keyed by [`RetryKey`]. In-memory only: a worker restart simply falls back to
/// the normal paced sweep, which is the backstop for any dropped retry.
#[derive(Debug, Clone)]
struct RetryEntry {
    transition: TransitionDecl,
    tier: &'static RouteCostTier,
    attempts: u32,
    due_at: DateTime<Utc>,
}

/// Aggregate counters returned by sampling a set of curated work units.
#[derive(Debug, Default)]
struct UnitSampleTotals {
    snapshots_upserted: usize,
    provider_quotes_attempted: usize,
    provider_quotes_succeeded: usize,
    provider_quotes_failed: usize,
}

#[derive(Clone)]
pub struct RouteCostService {
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
    asset_registry: Arc<AssetRegistry>,
    /// Freshness window for the pricing-oracle snapshot only.
    ttl: Duration,
    /// Validity window for sampled route-cost rows (`expires_at`). Decoupled
    /// from `ttl` so snapshots survive the full paced refresh window.
    snapshot_ttl: Duration,
    pricing: Arc<RwLock<PricingSnapshot>>,
    pricing_refresh: Arc<Mutex<()>>,
    pricing_oracle: Option<Arc<MarketPricingOracle>>,
    /// One paced cursor per provider, keyed by provider id. Each provider
    /// advances independently so venues never bunch against each other.
    refresh_cursors: Arc<Mutex<BTreeMap<String, ProviderCursor>>>,
    /// Failed cells awaiting an out-of-band retry, keyed by `(transition, tier)`.
    /// Folded into each refresh tick alongside the paced chunk (see
    /// [`Self::refresh_due_costs`]) so a failure is re-attempted within seconds.
    retry_queue: Arc<Mutex<HashMap<RetryKey, RetryEntry>>>,
}

impl RouteCostService {
    #[must_use]
    pub fn new(db: Database, action_providers: Arc<ActionProviderRegistry>) -> Self {
        let asset_registry = action_providers.asset_registry();
        Self {
            db,
            action_providers,
            asset_registry,
            ttl: DEFAULT_REFRESH_TTL,
            snapshot_ttl: DEFAULT_ROUTE_COST_SNAPSHOT_TTL,
            pricing: Arc::new(RwLock::new(PricingSnapshot::static_bootstrap(Utc::now()))),
            pricing_refresh: Arc::new(Mutex::new(())),
            pricing_oracle: None,
            refresh_cursors: Arc::new(Mutex::new(BTreeMap::new())),
            retry_queue: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Total number of curated work units (`curated_edges x tiers`). Used by
    /// the worker to size its pacing tick so each provider advances at most one
    /// unit per tick.
    #[must_use]
    pub fn curated_unit_count(&self) -> usize {
        self.asset_registry
            .curated_cacheable_transitions()
            .len()
            .saturating_mul(ROUTE_COST_TIERS.len())
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Override the validity window for sampled route-cost rows. Used by tests
    /// that need short-lived snapshots; production relies on the default which
    /// outlives the paced refresh window.
    #[must_use]
    pub fn with_snapshot_ttl(mut self, snapshot_ttl: Duration) -> Self {
        self.snapshot_ttl = snapshot_ttl;
        self
    }

    #[must_use]
    pub fn with_pricing(mut self, pricing: PricingSnapshot) -> Self {
        self.pricing = Arc::new(RwLock::new(pricing));
        self
    }

    #[must_use]
    pub fn with_pricing_oracle(mut self, pricing_oracle: Arc<MarketPricingOracle>) -> Self {
        self.pricing_oracle = Some(pricing_oracle);
        self
    }

    pub async fn current_pricing_snapshot(&self) -> PricingSnapshot {
        self.pricing.read().await.clone()
    }

    pub async fn current_or_refresh_pricing_snapshot(&self) -> PricingSnapshot {
        let now = Utc::now();
        let current = self.current_pricing_snapshot().await;
        if pricing_snapshot_is_fresh(&current, now, self.ttl) {
            current
        } else {
            let _guard = self.pricing_refresh.lock().await;
            let now = Utc::now();
            let current = self.current_pricing_snapshot().await;
            if pricing_snapshot_is_fresh(&current, now, self.ttl) {
                return current;
            }
            self.refresh_pricing_snapshot().await
        }
    }

    pub async fn current_or_refresh_live_pricing_snapshot(&self) -> Option<PricingSnapshot> {
        let pricing = self.current_or_refresh_pricing_snapshot().await;
        pricing_snapshot_is_fresh(&pricing, Utc::now(), self.ttl).then_some(pricing)
    }
    pub async fn force_refresh_pricing_snapshot(&self) -> PricingSnapshot {
        let _guard = self.pricing_refresh.lock().await;
        self.refresh_pricing_snapshot().await
    }

    /// Sample and persist the **entire** curated allowlist in one pass. Used
    /// by tests and any caller that wants a complete, synchronous refresh. The
    /// production worker uses [`Self::refresh_due_costs`] instead, which spreads
    /// the same work evenly across a window.
    pub async fn refresh_anchor_costs(&self) -> RouterCoreResult<RouteCostRefreshSummary> {
        let now = Utc::now();
        let expires_at = self.expiry_from(now);
        let pricing = self.refresh_pricing_snapshot().await;
        require_live_pricing_for_route_cost_refresh(&pricing, Utc::now(), self.ttl)?;
        // Only the explicit curated allowlist is ever cached. The routing
        // graph (`transition_declarations`) is intentionally broader; this
        // narrows what gets a measured cost row so nothing outside the
        // curated Across/CCTP/Unit/universal-router set is persisted.
        let transitions = self.asset_registry.curated_cacheable_transitions();
        let plan = interleaved_refresh_plan(&transitions);

        let totals = self
            .sample_and_store_units(plan, now, expires_at, &pricing)
            .await?;

        Ok(RouteCostRefreshSummary {
            candidate_edges: transitions.len(),
            snapshots_upserted: totals.snapshots_upserted,
            provider_quotes_attempted: totals.provider_quotes_attempted,
            provider_quotes_succeeded: totals.provider_quotes_succeeded,
            provider_quotes_failed: totals.provider_quotes_failed,
            tiers_attempted: ROUTE_COST_TIERS.len(),
            pricing_source: pricing.source.clone(),
            refreshed_at: now,
        })
    }

    /// Sample only the slice of curated work that has become "due" since the
    /// last paced pass, so that one full sweep of every `(transition, tier)`
    /// unit completes once per `window`. Work units are interleaved across
    /// providers, so any slice touches every venue proportionally and prices
    /// stay continuously fresh instead of being burst-refreshed.
    ///
    /// This is deadline-paced: each call computes how many units *should* be
    /// finished by now (`total * elapsed / window`) and samples up to that
    /// index. A frequent caller therefore trickles a few units per tick; a
    /// slow caller catches up in larger slices. When the cursor reaches the
    /// end the next call re-anchors a fresh cycle.
    pub async fn refresh_due_costs(
        &self,
        window: Duration,
    ) -> RouterCoreResult<RouteCostRefreshSummary> {
        let now = Utc::now();
        let expires_at = self.expiry_from(now);
        // Use the cached snapshot when it is still fresh; only hit the oracle
        // when the TTL has lapsed. Forcing a refresh on every paced tick would
        // hammer the pricing upstream far more than necessary.
        let pricing = self.current_or_refresh_pricing_snapshot().await;
        require_live_pricing_for_route_cost_refresh(&pricing, Utc::now(), self.ttl)?;

        let transitions = self.asset_registry.curated_cacheable_transitions();
        let candidate_edges = transitions.len();
        let plan = interleaved_refresh_plan(&transitions);
        let chunk = self.claim_due_units(&plan, now, window).await;
        // Fold in any failed cells whose backoff has elapsed so they are
        // re-sampled within seconds instead of waiting the next window sweep.
        let retries = self.claim_due_retries(now, MAX_RETRIES_PER_TICK).await;
        let units = merge_paced_and_retries(chunk, retries);

        let totals = if units.is_empty() {
            UnitSampleTotals::default()
        } else {
            self.sample_and_store_units(units, now, expires_at, &pricing)
                .await?
        };

        Ok(RouteCostRefreshSummary {
            candidate_edges,
            snapshots_upserted: totals.snapshots_upserted,
            provider_quotes_attempted: totals.provider_quotes_attempted,
            provider_quotes_succeeded: totals.provider_quotes_succeeded,
            provider_quotes_failed: totals.provider_quotes_failed,
            tiers_attempted: ROUTE_COST_TIERS.len(),
            pricing_source: pricing.source.clone(),
            refreshed_at: now,
        })
    }

    fn expiry_from(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        now + chrono::Duration::from_std(self.snapshot_ttl)
            .unwrap_or_else(|_| chrono::Duration::seconds(2400))
    }

    /// Advance every provider's paced cursor independently and return the work
    /// units that are due this tick. Each provider is its own metronome: it
    /// trickles a single request every `window / provider_cells`, paced on its
    /// own cycle clock, so venues with different cell counts never bunch
    /// against each other. The aggregate returned chunk therefore holds at most
    /// one unit per provider on a tick sized so each provider advances < 1 unit
    /// per tick (see `route_cost_pacing_tick`).
    async fn claim_due_units(
        &self,
        plan: &[(TransitionDecl, &'static RouteCostTier)],
        now: DateTime<Utc>,
        window: Duration,
    ) -> Vec<(TransitionDecl, &'static RouteCostTier)> {
        let mut cursors = self.refresh_cursors.lock().await;
        claim_due_units_paced(&mut cursors, plan, now, window)
    }

    /// Live-sample every `(transition, tier)` unit concurrently (bounded by
    /// [`REFRESH_FANOUT_PERMITS`]) and upsert the successful rows. A sample that
    /// fails or cannot be attempted writes **no row** - the cell stays
    /// legitimately absent rather than being seeded with a fabricated estimate.
    async fn sample_and_store_units(
        &self,
        units: Vec<(TransitionDecl, &'static RouteCostTier)>,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        pricing: &PricingSnapshot,
    ) -> RouterCoreResult<UnitSampleTotals> {
        if units.is_empty() {
            return Ok(UnitSampleTotals::default());
        }

        let semaphore = Arc::new(Semaphore::new(REFRESH_FANOUT_PERMITS));
        let mut tasks = FuturesUnordered::new();
        let pricing_for_tasks = Arc::new(pricing.clone());
        for (transition, tier) in units {
            let permit = Arc::clone(&semaphore);
            let pricing_inner = Arc::clone(&pricing_for_tasks);
            tasks.push(async move {
                let _permit = permit
                    .acquire_owned()
                    .await
                    .expect("route-cost refresh semaphore is never closed");
                let call_started = std::time::Instant::now();
                let outcome = self
                    .live_cost_snapshot(&transition, now, expires_at, tier, &pricing_inner)
                    .await;
                let elapsed_ms =
                    u64::try_from(call_started.elapsed().as_millis()).unwrap_or(u64::MAX);
                (transition, tier, outcome, elapsed_ms)
            });
        }

        let mut snapshots = Vec::new();
        let mut events = Vec::new();
        let mut totals = UnitSampleTotals::default();
        // Cells that no longer need retrying (succeeded, or could not be
        // attempted at all) and cells that failed and should be (re)scheduled.
        let mut resolved_keys: Vec<RetryKey> = Vec::new();
        let mut failed_units: Vec<(TransitionDecl, &'static RouteCostTier, FailureCategory)> =
            Vec::new();
        while let Some((transition, tier, outcome, elapsed_ms)) = tasks.next().await {
            match outcome {
                LiveCostSnapshotOutcome::NotAttempted => {
                    // Nothing reached a provider; stop retrying this cell so we
                    // do not spin on something we cannot sample.
                    resolved_keys.push(retry_key(&transition, tier));
                }
                LiveCostSnapshotOutcome::Succeeded { snapshot } => {
                    totals.provider_quotes_attempted += 1;
                    totals.provider_quotes_succeeded += 1;
                    events.push(sample_event(
                        &transition,
                        tier,
                        RouteCostSampleOutcome::Succeeded,
                        Some(snapshot.estimated_fee_bps),
                        Some(elapsed_ms),
                        None,
                        None,
                    ));
                    // A fresh success clears any pending retry for this cell.
                    resolved_keys.push(retry_key(&transition, tier));
                    snapshots.push(*snapshot);
                }
                LiveCostSnapshotOutcome::Skipped(reason) => {
                    // Provider reached, but reported a benign expected
                    // condition (e.g. orderbook too shallow for this tier).
                    // Record it neutrally and do not retry - the next paced
                    // sweep re-samples it anyway, and retrying will not help.
                    totals.provider_quotes_attempted += 1;
                    events.push(sample_event(
                        &transition,
                        tier,
                        RouteCostSampleOutcome::Skipped,
                        None,
                        Some(elapsed_ms),
                        Some(reason),
                        None,
                    ));
                    resolved_keys.push(retry_key(&transition, tier));
                }
                LiveCostSnapshotOutcome::Failed(reason) => {
                    totals.provider_quotes_attempted += 1;
                    totals.provider_quotes_failed += 1;
                    let category = classify_failure(&reason);
                    debug!(
                        transition_id = %transition.id,
                        provider = transition.provider.as_str(),
                        tier = tier.label,
                        category = category.as_str(),
                        reason = %reason,
                        "route-cost provider sampling failed; leaving cell uncached"
                    );
                    events.push(sample_event(
                        &transition,
                        tier,
                        RouteCostSampleOutcome::Failed,
                        None,
                        Some(elapsed_ms),
                        Some(reason),
                        Some(category),
                    ));
                    failed_units.push((transition, tier, category));
                }
            }
        }
        self.db.route_costs().upsert_many(&snapshots).await?;
        totals.snapshots_upserted = snapshots.len();
        // Update the in-memory retry queue: clear resolved cells and
        // (re)schedule the failures with category-aware backoff.
        self.reschedule_failures(resolved_keys, failed_units).await;
        // The activity feed is best-effort: a write failure here must never
        // fail the refresh cycle (the worker treats refresh errors as fatal to
        // the tick). Log and continue.
        if let Err(err) = self.db.route_cost_events().record_many(&events).await {
            warn!(error = %err, "route-cost sample-event feed write failed; continuing");
        }
        Ok(totals)
    }

    /// Apply the outcome of a sampling pass to the retry queue. Resolved cells
    /// (succeeded or not-attempted) are dropped; failed cells have their
    /// attempt count incremented and a fresh `due_at` computed from
    /// [`retry_delay`], or are abandoned once they exceed
    /// [`RETRY_MAX_ATTEMPTS`] (the next paced sweep is the backstop).
    async fn reschedule_failures(
        &self,
        resolved: Vec<RetryKey>,
        failed: Vec<(TransitionDecl, &'static RouteCostTier, FailureCategory)>,
    ) {
        if resolved.is_empty() && failed.is_empty() {
            return;
        }
        let now = Utc::now();
        let mut queue = self.retry_queue.lock().await;
        for key in resolved {
            queue.remove(&key);
        }
        for (transition, tier, category) in failed {
            let key = retry_key(&transition, tier);
            let attempts = queue.get(&key).map_or(0, |entry| entry.attempts) + 1;
            if attempts > RETRY_MAX_ATTEMPTS {
                queue.remove(&key);
                continue;
            }
            let due_at = now
                + chrono::Duration::from_std(retry_delay(attempts, category))
                    .unwrap_or_else(|_| chrono::Duration::seconds(60));
            queue.insert(
                key,
                RetryEntry {
                    transition,
                    tier,
                    attempts,
                    due_at,
                },
            );
        }
    }

    /// Return up to `limit` retry cells whose `due_at` has passed, oldest-due
    /// first. Entries are left in the queue; [`Self::reschedule_failures`]
    /// removes or reschedules them after sampling completes.
    async fn claim_due_retries(
        &self,
        now: DateTime<Utc>,
        limit: usize,
    ) -> Vec<(TransitionDecl, &'static RouteCostTier)> {
        if limit == 0 {
            return Vec::new();
        }
        let queue = self.retry_queue.lock().await;
        let mut due: Vec<&RetryEntry> =
            queue.values().filter(|entry| entry.due_at <= now).collect();
        due.sort_by_key(|entry| entry.due_at);
        due.into_iter()
            .take(limit)
            .map(|entry| (entry.transition.clone(), entry.tier))
            .collect()
    }

    /// Rank `paths` against the request size in USD micros. The returned
    /// `Vec<RankedTransitionPath>` is aligned with `paths` after sorting and
    /// carries the per-path score so callers can inspect costs without
    /// re-scoring.
    pub async fn rank_transition_paths_for_request(
        &self,
        paths: &mut [TransitionPath],
        request_usd_micros: u64,
    ) -> RouterCoreResult<Vec<RankedTransitionPath>> {
        self.rank_transition_paths_for_request_with_overrides(
            paths,
            request_usd_micros,
            &HashMap::new(),
        )
        .await
    }

    /// Active (non-expired) cached snapshots for the tier implied by
    /// `request_usd_micros`, keyed by transition id. Exposed so the route
    /// explainer can tell which legs already have a fresh cached cost and only
    /// live-sample the ones that do not.
    pub async fn active_snapshots_for_request(
        &self,
        request_usd_micros: u64,
    ) -> RouterCoreResult<HashMap<String, RouteCostSnapshot>> {
        let tier = select_route_cost_tier(request_usd_micros);
        let snapshots = self
            .db
            .route_costs()
            .list_active(tier.label, Utc::now())
            .await?;
        Ok(snapshots
            .into_iter()
            .map(|snapshot| (snapshot.transition_id.clone(), snapshot))
            .collect())
    }

    /// Same as [`rank_transition_paths_for_request`] but lets the caller inject
    /// live-sampled snapshots that take precedence over the cached rows for
    /// their transition ids. This is how route explainers fold uncached
    /// universal-router wrap legs (Velora or KyberSwap) into the score when live
    /// quoting is enabled, so a leg that was previously a `missing_edge` with
    /// zero cost contributes its real value-loss bps and the cheapest total
    /// (e.g. the best ERC20 entry/exit anchor) sorts to the top.
    pub async fn rank_transition_paths_for_request_with_overrides(
        &self,
        paths: &mut [TransitionPath],
        request_usd_micros: u64,
        overrides: &HashMap<String, RouteCostSnapshot>,
    ) -> RouterCoreResult<Vec<RankedTransitionPath>> {
        let tier = select_route_cost_tier(request_usd_micros);
        let snapshots = self
            .db
            .route_costs()
            .list_active(tier.label, Utc::now())
            .await?;
        let mut by_transition_id = snapshots
            .into_iter()
            .map(|snapshot| (snapshot.transition_id.clone(), snapshot))
            .collect::<HashMap<_, _>>();
        for (transition_id, snapshot) in overrides {
            by_transition_id.insert(transition_id.clone(), snapshot.clone());
        }

        let pricing = self.current_or_refresh_pricing_snapshot().await;
        rank_paths_in_place(paths, &by_transition_id, &pricing, request_usd_micros);
        let ranked = paths
            .iter()
            .map(|path| RankedTransitionPath {
                path: path.clone(),
                score: amount_aware_path_score(
                    path,
                    &by_transition_id,
                    &pricing,
                    request_usd_micros,
                ),
            })
            .collect();
        Ok(ranked)
    }

    /// Live-sample the cost of a single transition at the tier implied by
    /// `request_usd_micros`, returning the snapshot on success. Used by the
    /// route explainer to price uncached legs (notably runtime Velora wrap
    /// legs for arbitrary ERC20s) on demand. Benign skips, provider failures,
    /// and unsupported legs return `None` so the caller leaves the edge as a
    /// `missing_edge` rather than fabricating a cost.
    pub async fn sample_transition_cost(
        &self,
        transition: &TransitionDecl,
        request_usd_micros: u64,
    ) -> Option<RouteCostSnapshot> {
        let tier = select_route_cost_tier(request_usd_micros);
        let pricing = self.current_or_refresh_pricing_snapshot().await;
        let now = Utc::now();
        let expires_at = now + chrono::Duration::minutes(5);
        match self
            .live_cost_snapshot(transition, now, expires_at, tier, &pricing)
            .await
        {
            LiveCostSnapshotOutcome::Succeeded { snapshot } => Some(*snapshot),
            _ => None,
        }
    }

    async fn refresh_pricing_snapshot(&self) -> PricingSnapshot {
        let Some(pricing_oracle) = self.pricing_oracle.as_ref() else {
            return self.current_pricing_snapshot().await;
        };
        match pricing_oracle.snapshot().await {
            Ok(snapshot) => {
                let pricing = PricingSnapshot::from_market(snapshot);
                *self.pricing.write().await = pricing.clone();
                pricing
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "market pricing refresh failed; using previous pricing snapshot"
                );
                self.current_pricing_snapshot().await
            }
        }
    }

    async fn live_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        tier: &RouteCostTier,
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                self.live_bridge_cost_snapshot(transition, refreshed_at, expires_at, tier, pricing)
                    .await
            }
            MarketOrderTransitionKind::UnitDeposit | MarketOrderTransitionKind::UnitWithdrawal => {
                self.live_unit_cost_snapshot(transition, refreshed_at, expires_at, tier, pricing)
                    .await
            }
            MarketOrderTransitionKind::UniversalRouterSwap
            | MarketOrderTransitionKind::HyperliquidTrade => {
                self.live_exchange_cost_snapshot(
                    transition,
                    refreshed_at,
                    expires_at,
                    tier,
                    pricing,
                )
                .await
            }
        }
    }

    async fn live_bridge_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        tier: &RouteCostTier,
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        let Some(bridge) = self.action_providers.bridge(transition.provider.as_str()) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(input_asset) = self.asset_registry.chain_asset(&transition.input.asset) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(output_asset) = self.asset_registry.chain_asset(&transition.output.asset) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(amount_in) = sample_amount_for_chain_asset_at_tier(input_asset, pricing, tier)
        else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };

        let request = BridgeQuoteRequest {
            source_asset: transition.input.asset.clone(),
            destination_asset: transition.output.asset.clone(),
            order_kind: ProviderOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: DUMMY_EVM_RECIPIENT.to_string(),
            depositor_address: DUMMY_EVM_DEPOSITOR.to_string(),
            partial_fills_enabled: false,
        };
        let quote = match timeout(ROUTE_COST_PROVIDER_TIMEOUT, bridge.quote_bridge(request)).await {
            Ok(Ok(Some(quote))) => quote,
            Ok(Ok(None)) => {
                return LiveCostSnapshotOutcome::Failed("provider returned no route".to_string())
            }
            Ok(Err(err)) => {
                // "Amount too large for this route" (e.g. Across
                // `AMOUNT_TOO_HIGH`) is a benign, expected condition at large
                // notionals - record it as a neutral skip, not a failure, and
                // do not retry the same oversized tier.
                return if is_benign_unsatisfiable_amount(&err) {
                    LiveCostSnapshotOutcome::Skipped(err)
                } else {
                    LiveCostSnapshotOutcome::Failed(err)
                };
            }
            Err(_) => return LiveCostSnapshotOutcome::Failed("provider timed out".to_string()),
        };
        let amount_out = match U256::from_str(&quote.amount_out) {
            Ok(amount_out) => amount_out,
            Err(err) => {
                return LiveCostSnapshotOutcome::Failed(format!(
                    "provider amount_out was not numeric: {err}"
                ))
            }
        };
        measured_value_loss_snapshot(
            transition,
            refreshed_at,
            expires_at,
            tier,
            pricing,
            amount_in,
            input_asset,
            amount_out,
            output_asset,
            &format!("provider_quote:{}", quote.provider_id),
        )
    }

    /// Live-sample an exchange-quoted leg: a curated same-chain Velora swap
    /// (`USDC <-> USDT`) or a Hyperliquid spot trade (`USDC <-> UBTC/UETH`).
    /// The measured cost is the value loss between the input amount and the
    /// exchange `amount_out`, captured as bps + USD micros exactly like a
    /// bridge. The exchange provider is resolved by the transition's provider
    /// id (`velora` / `hyperliquid_spot`).
    async fn live_exchange_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        tier: &RouteCostTier,
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        let Some(exchange) = self.action_providers.exchange(transition.provider.as_str()) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(input_asset) = self.asset_registry.chain_asset(&transition.input.asset) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(output_asset) = self.asset_registry.chain_asset(&transition.output.asset) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(amount_in) = sample_amount_for_chain_asset_at_tier(input_asset, pricing, tier)
        else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };

        let request = ExchangeQuoteRequest {
            input_asset: transition.input.asset.clone(),
            output_asset: transition.output.asset.clone(),
            input_decimals: Some(input_asset.decimals),
            output_decimals: Some(output_asset.decimals),
            order_kind: ProviderOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: Some(DUMMY_EVM_DEPOSITOR.to_string()),
            recipient_address: DUMMY_EVM_RECIPIENT.to_string(),
        };
        let quote = match timeout(ROUTE_COST_PROVIDER_TIMEOUT, exchange.quote_trade(request)).await
        {
            Ok(Ok(Some(quote))) => quote,
            Ok(Ok(None)) => {
                // A swap venue with no route at this size means it cannot fill
                // the sampled notional (insufficient liquidity) - benign, not a
                // failure, and not retryable at the same tier.
                return LiveCostSnapshotOutcome::Skipped(
                    "no route at this size (insufficient liquidity)".to_string(),
                );
            }
            Ok(Err(err)) => {
                // "Amount too large for this venue/tier" (orderbook too shallow,
                // amount-too-high) is a benign, expected condition at large
                // notionals - record it as a neutral skip rather than a failure.
                return if is_benign_unsatisfiable_amount(&err) {
                    LiveCostSnapshotOutcome::Skipped(err)
                } else {
                    LiveCostSnapshotOutcome::Failed(err)
                };
            }
            Err(_) => return LiveCostSnapshotOutcome::Failed("provider timed out".to_string()),
        };
        let amount_out = match U256::from_str(&quote.amount_out) {
            Ok(amount_out) => amount_out,
            Err(err) => {
                return LiveCostSnapshotOutcome::Failed(format!(
                    "provider amount_out was not numeric: {err}"
                ))
            }
        };
        measured_value_loss_snapshot(
            transition,
            refreshed_at,
            expires_at,
            tier,
            pricing,
            amount_in,
            input_asset,
            amount_out,
            output_asset,
            &format!("provider_quote:{}", quote.provider_id),
        )
    }

    /// Live-sample a curated Unit deposit/withdrawal. Unit charges only a
    /// network fee in the external asset's native units (sats / wei); the
    /// fee applies to the external-chain side of the transfer, so the bps is
    /// `fee_native * 10_000 / sample_amount_native` and the USD micros is the
    /// fee converted at the current pricing snapshot.
    async fn live_unit_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        tier: &RouteCostTier,
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        let Some(unit) = self.action_providers.unit(transition.provider.as_str()) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        // The fee is denominated in the external (non-hyperliquid) asset. For
        // a deposit that is the input side; for a withdrawal the output side.
        let (external_ref, direction) = match transition.kind {
            MarketOrderTransitionKind::UnitDeposit => {
                (&transition.input.asset, UnitFeeDirection::Deposit)
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                (&transition.output.asset, UnitFeeDirection::Withdrawal)
            }
            _ => return LiveCostSnapshotOutcome::NotAttempted,
        };
        let Some(external_asset) = self.asset_registry.chain_asset(external_ref) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(sample_amount) =
            sample_amount_for_chain_asset_at_tier(external_asset, pricing, tier)
        else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };

        let fee_native = match timeout(
            ROUTE_COST_PROVIDER_TIMEOUT,
            unit.estimate_unit_fee(external_ref, direction),
        )
        .await
        {
            Ok(Ok(Some(fee))) => fee,
            Ok(Ok(None)) => {
                return LiveCostSnapshotOutcome::Failed(
                    "unit provider returned no fee estimate".to_string(),
                )
            }
            Ok(Err(err)) => return LiveCostSnapshotOutcome::Failed(err),
            Err(_) => return LiveCostSnapshotOutcome::Failed("provider timed out".to_string()),
        };
        // Both the fee and the sample amount are in the same native units, so
        // the value loss is exactly the fee against the sampled notional.
        let Some(amount_out) = sample_amount.checked_sub(fee_native) else {
            // Fee exceeds the sampled notional - the leg is effectively 100%
            // loss at this tier; clamp by reporting a zero output.
            return measured_value_loss_snapshot(
                transition,
                refreshed_at,
                expires_at,
                tier,
                pricing,
                sample_amount,
                external_asset,
                U256::ZERO,
                external_asset,
                "provider_quote:unit",
            );
        };
        measured_value_loss_snapshot(
            transition,
            refreshed_at,
            expires_at,
            tier,
            pricing,
            sample_amount,
            external_asset,
            amount_out,
            external_asset,
            "provider_quote:unit",
        )
    }
}

/// Build a `RouteCostSnapshot` from a measured input/output pair. All of the
/// measured cost (relayer fee, value loss, network fee) is captured in
/// `estimated_fee_bps` / `estimated_fee_usd_micros`; gas and latency are left
/// at zero because the curated cache stores only the real value-loss bps the
/// provider quoted, not modeled side costs.
#[allow(clippy::too_many_arguments)]
fn measured_value_loss_snapshot(
    transition: &TransitionDecl,
    refreshed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    tier: &RouteCostTier,
    pricing: &PricingSnapshot,
    amount_in: U256,
    input_asset: &ChainAsset,
    amount_out: U256,
    output_asset: &ChainAsset,
    quote_source: &str,
) -> LiveCostSnapshotOutcome {
    let Some(fee_bps) =
        quote_value_loss_bps(amount_in, input_asset, amount_out, output_asset, pricing)
    else {
        return LiveCostSnapshotOutcome::Failed(
            "could not convert provider value loss to bps".to_string(),
        );
    };
    let Some(fee_usd_micros) =
        quote_value_loss_usd_micros(amount_in, input_asset, amount_out, output_asset, pricing)
    else {
        return LiveCostSnapshotOutcome::Failed(
            "could not convert provider value loss to usd micros".to_string(),
        );
    };
    LiveCostSnapshotOutcome::Succeeded {
        snapshot: Box::new(RouteCostSnapshot {
            transition_id: transition.id.clone(),
            amount_bucket: tier.label.to_string(),
            provider: transition.provider.as_str().to_string(),
            edge_kind: transition.route_edge_kind().as_str().to_string(),
            source_asset: transition.input.asset.clone(),
            destination_asset: transition.output.asset.clone(),
            estimated_fee_bps: fee_bps,
            estimated_fee_usd_micros: fee_usd_micros,
            estimated_gas_usd_micros: 0,
            estimated_latency_ms: 0,
            sample_amount_usd_micros: tier.sample_usd_micros,
            quote_source: quote_source.to_string(),
            refreshed_at,
            expires_at,
        }),
    }
}

/// Build an activity-feed event for a single sampled `(transition, tier)`
/// unit. `sampled_at` is captured here (per completion) so the feed reflects
/// the true wall-clock spacing of each provider call across the window.
fn sample_event(
    transition: &TransitionDecl,
    tier: &RouteCostTier,
    outcome: RouteCostSampleOutcome,
    estimated_fee_bps: Option<u64>,
    estimated_latency_ms: Option<u64>,
    reason: Option<String>,
    failure_category: Option<FailureCategory>,
) -> RouteCostSampleEvent {
    RouteCostSampleEvent {
        sampled_at: Utc::now(),
        provider: transition.provider.as_str().to_string(),
        transition_id: transition.id.clone(),
        amount_bucket: tier.label.to_string(),
        edge_kind: transition.route_edge_kind().as_str().to_string(),
        source_asset: transition.input.asset.clone(),
        destination_asset: transition.output.asset.clone(),
        sample_amount_usd_micros: tier.sample_usd_micros,
        outcome,
        estimated_fee_bps,
        estimated_latency_ms,
        reason,
        failure_category,
    }
}

/// Stable key for a single cacheable cell, used by the retry queue and to
/// dedupe paced work against due retries within a tick.
fn retry_key(transition: &TransitionDecl, tier: &RouteCostTier) -> RetryKey {
    (transition.id.clone(), tier.label.to_string())
}

/// Category-aware exponential backoff: `base * 2^(attempts-1)`, clamped to the
/// per-category cap. Rate-limited failures use a longer base/cap so retries do
/// not amplify provider throttling. `attempts` is 1-based (first retry == 1).
fn retry_delay(attempts: u32, category: FailureCategory) -> Duration {
    let (base, cap) = match category {
        FailureCategory::RateLimited => {
            (RETRY_RATE_LIMITED_BASE_DELAY, RETRY_RATE_LIMITED_MAX_DELAY)
        }
        _ => (RETRY_BASE_DELAY, RETRY_MAX_DELAY),
    };
    // Clamp the shift so the multiplier cannot overflow; the cap bounds it anyway.
    let shift = attempts.saturating_sub(1).min(16);
    base.saturating_mul(1u32 << shift).min(cap)
}

/// Merge the paced work chunk with due retries, deduping by [`RetryKey`] so a
/// cell already in the paced slice is not sampled twice in one tick.
fn merge_paced_and_retries(
    paced: Vec<(TransitionDecl, &'static RouteCostTier)>,
    retries: Vec<(TransitionDecl, &'static RouteCostTier)>,
) -> Vec<(TransitionDecl, &'static RouteCostTier)> {
    let mut seen: std::collections::HashSet<RetryKey> =
        paced.iter().map(|(t, tier)| retry_key(t, tier)).collect();
    let mut units = paced;
    for (transition, tier) in retries {
        if seen.insert(retry_key(&transition, tier)) {
            units.push((transition, tier));
        }
    }
    units
}

#[async_trait]
impl PricingSnapshotProvider for RouteCostService {
    async fn usd_pricing_snapshot(&self) -> Option<PricingSnapshot> {
        self.current_or_refresh_live_pricing_snapshot().await
    }
}

enum LiveCostSnapshotOutcome {
    NotAttempted,
    Succeeded {
        snapshot: Box<RouteCostSnapshot>,
    },
    Failed(String),
    /// Provider reached but reported a benign expected condition (e.g. the
    /// orderbook is too shallow for this tier). Recorded as a neutral
    /// `skipped` sample, not a failure, and not retried.
    Skipped(String),
}

fn pricing_snapshot_is_fresh(
    snapshot: &PricingSnapshot,
    now: DateTime<Utc>,
    ttl: Duration,
) -> bool {
    snapshot.source != STATIC_BOOTSTRAP_PRICING_SOURCE
        && snapshot.is_fresh(now)
        && now
            .signed_duration_since(snapshot.captured_at)
            .to_std()
            .is_ok_and(|age| age < ttl)
}

/// Number of work units that *should* be finished by now to keep a full sweep
/// of `total` units on pace to complete in exactly `window`. Linear in elapsed
/// time and clamped to `total`, so a paced caller advances its cursor up to
/// this index each tick.
fn paced_target_index(total: usize, elapsed: Duration, window: Duration) -> usize {
    if total == 0 {
        return 0;
    }
    if elapsed >= window {
        return total;
    }
    let window_ms = window.as_millis().max(1);
    usize::try_from(total as u128 * elapsed.as_millis() / window_ms)
        .unwrap_or(total)
        .min(total)
}

/// Advance every provider's paced cursor independently and return the work
/// units that are due this tick. Each provider is its own metronome: the plan
/// is bucketed by provider (preserving each provider's relative order) and each
/// bucket is deadline-paced on its own cycle clock, so a venue with many cells
/// and a venue with few cells never bunch against each other. On a tick sized
/// to `window / total_units` (see `route_cost_pacing_tick`) every provider
/// advances at most one unit per call, giving true one-by-one pacing.
fn claim_due_units_paced(
    cursors: &mut BTreeMap<String, ProviderCursor>,
    plan: &[(TransitionDecl, &'static RouteCostTier)],
    now: DateTime<Utc>,
    window: Duration,
) -> Vec<(TransitionDecl, &'static RouteCostTier)> {
    // Bucket the plan by provider, preserving each provider's relative order
    // (the interleaved plan keeps per-provider insertion order).
    let mut by_provider: BTreeMap<String, Vec<(TransitionDecl, &'static RouteCostTier)>> =
        BTreeMap::new();
    for unit in plan {
        by_provider
            .entry(unit.0.provider.as_str().to_string())
            .or_default()
            .push(unit.clone());
    }

    // Drop cursors for providers no longer in the plan so the map cannot grow
    // unbounded across allowlist changes.
    cursors.retain(|provider, _| by_provider.contains_key(provider));

    let mut chunk = Vec::new();
    for (provider, units) in &by_provider {
        let total = units.len();
        if total == 0 {
            cursors.remove(provider);
            continue;
        }
        let cursor = cursors.entry(provider.clone()).or_default();

        // Anchor (or re-anchor) this provider's cycle clock. A fresh cycle
        // starts whenever the previous one finished (`next_unit >= total`) or
        // was never begun.
        let cycle_started_at = match cursor.cycle_started_at {
            Some(started_at) if cursor.next_unit < total => started_at,
            _ => {
                cursor.next_unit = 0;
                cursor.cycle_started_at = Some(now);
                now
            }
        };

        let elapsed = (now - cycle_started_at).to_std().unwrap_or_default();
        let target = paced_target_index(total, elapsed, window);

        let start_idx = cursor.next_unit.min(total);
        let end_idx = target.min(total).max(start_idx);
        chunk.extend_from_slice(&units[start_idx..end_idx]);
        cursor.next_unit = end_idx;
        if cursor.next_unit >= total {
            // Cycle complete; the next paced pass re-anchors at its own `now`.
            *cursor = ProviderCursor::default();
        }
    }
    chunk
}

/// Expand the curated transitions into a flat list of `(transition, tier)`
/// work units ordered so that providers are interleaved round-robin. A
/// contiguous slice of the returned plan therefore touches every venue
/// proportionally, which keeps each provider's prices refreshing evenly across
/// the paced window rather than in per-provider bursts.
///
/// Within a provider the units are ordered **tier-outer, route-inner** (one
/// tier across every route before advancing to the next tier). Combined with
/// one-by-one pacing, consecutive samples for a venue therefore step through
/// *distinct routes* rather than firing all twelve tiers of a single route in a
/// row - so the activity feed visibly cycles through every cell instead of
/// looking like a batch of the same route repeated.
fn interleaved_refresh_plan(
    transitions: &[TransitionDecl],
) -> Vec<(TransitionDecl, &'static RouteCostTier)> {
    // Bucket by provider, ordering each provider's units tier-outer/route-inner
    // so consecutive units for a venue are different routes.
    let mut by_provider: BTreeMap<String, VecDeque<(TransitionDecl, &'static RouteCostTier)>> =
        BTreeMap::new();
    for tier in ROUTE_COST_TIERS {
        for transition in transitions {
            by_provider
                .entry(transition.provider.as_str().to_string())
                .or_default()
                .push_back((transition.clone(), tier));
        }
    }

    let mut queues: Vec<VecDeque<(TransitionDecl, &'static RouteCostTier)>> =
        by_provider.into_values().collect();
    let total: usize = queues.iter().map(VecDeque::len).sum();
    let mut plan = Vec::with_capacity(total);
    let mut progressed = true;
    while progressed {
        progressed = false;
        for queue in &mut queues {
            if let Some(unit) = queue.pop_front() {
                plan.push(unit);
                progressed = true;
            }
        }
    }
    plan
}

fn require_live_pricing_for_route_cost_refresh(
    snapshot: &PricingSnapshot,
    now: DateTime<Utc>,
    ttl: Duration,
) -> RouterCoreResult<()> {
    if pricing_snapshot_is_fresh(snapshot, now, ttl) {
        return Ok(());
    }

    Err(RouterCoreError::NotReady {
        message: format!(
            "live market pricing is unavailable or stale; refusing to refresh route costs from {}",
            snapshot.source
        ),
    })
}

/// Structural-only ranking (no DB lookup). Used by the temporal boundary
/// requote fallback and the offline trace harness. Callers must pass the
/// request size in USD micros explicitly - there is no longer an implicit
/// "$1k default mode". For market `/quote` callers, prefer
/// [`RouteCostService::rank_transition_paths_for_request`] which also
/// consults the tier-aware cache.
pub fn rank_transition_paths_structurally(paths: &mut [TransitionPath], request_usd_micros: u64) {
    let pricing = PricingSnapshot::static_bootstrap(Utc::now());
    let snapshots: HashMap<String, RouteCostSnapshot> = HashMap::new();
    paths.sort_by(|left, right| {
        path_sort_key(
            left,
            amount_aware_path_score(left, &snapshots, &pricing, request_usd_micros),
            request_usd_micros,
        )
        .cmp(&path_sort_key(
            right,
            amount_aware_path_score(right, &snapshots, &pricing, request_usd_micros),
            request_usd_micros,
        ))
    });
}

#[must_use]
pub fn path_score(
    path: &TransitionPath,
    snapshots: &HashMap<String, RouteCostSnapshot>,
) -> RoutePathCostScore {
    let mut missing_edges = 0_usize;
    let mut total_effective_cost_usd_micros = 0_u64;
    let mut total_latency_ms = 0_u64;

    for transition in &path.transitions {
        if let Some(snapshot) = snapshots.get(&transition.id) {
            total_effective_cost_usd_micros = capped_add_u64(
                total_effective_cost_usd_micros,
                snapshot.effective_cost_usd_micros(),
            );
            total_latency_ms = capped_add_u64(total_latency_ms, snapshot.estimated_latency_ms);
        } else {
            missing_edges = missing_edges.saturating_add(1);
            total_effective_cost_usd_micros =
                capped_add_u64(total_effective_cost_usd_micros, u64::MAX / 4);
            total_latency_ms = capped_add_u64(total_latency_ms, u64::MAX / 4);
        }
    }

    RoutePathCostScore {
        missing_edges,
        total_effective_cost_usd_micros,
        total_latency_ms,
    }
}

/// Re-derives a per-path score using the request amount instead of the
/// fixed `usd_1000` anchor. Per leg:
///
/// - if a cached snapshot exists for the current tier, take its
///   `estimated_fee_usd_micros` (computed by the refresher at that tier),
///   floored by the request-size re-derivation from `estimated_fee_bps` so a
///   stale lower-tier row cannot under-price a much larger request;
/// - otherwise (Velora runtime edges, uncached legs, or a transient refresh
///   miss for this tier) the leg contributes **zero fabricated cost** here and
///   bumps `missing_edges`. We no longer guess a structural estimate. Ranking
///   ([`path_sort_key`]) then adds a notional-scaled penalty per unknown leg so
///   a fully-priced cheaper route always wins, and the live-quote fanout fills
///   those legs with real values when live quoting is enabled.
///
/// Gas costs stay absolute (USD micros / leg). Latency stays absolute (ms).
/// `pricing` is retained for signature stability with structural callers but
/// is unused now that no estimate is fabricated for uncached legs.
#[must_use]
pub fn amount_aware_path_score(
    path: &TransitionPath,
    snapshots: &HashMap<String, RouteCostSnapshot>,
    pricing: &PricingSnapshot,
    request_usd_micros: u64,
) -> RoutePathCostScore {
    let _ = pricing;
    let mut missing_edges = 0_usize;
    let mut total_effective_cost_usd_micros = 0_u64;
    let mut total_latency_ms = 0_u64;
    for transition in &path.transitions {
        match snapshots.get(&transition.id) {
            Some(snapshot) => {
                total_effective_cost_usd_micros = capped_add_u64(
                    total_effective_cost_usd_micros,
                    effective_leg_cost_usd_micros(snapshot, request_usd_micros),
                );
                total_latency_ms = capped_add_u64(total_latency_ms, snapshot.estimated_latency_ms);
            }
            None => {
                missing_edges = missing_edges.saturating_add(1);
            }
        }
    }
    RoutePathCostScore {
        missing_edges,
        total_effective_cost_usd_micros,
        total_latency_ms,
    }
}

/// Effective per-leg cost in USD micros at `request_usd_micros`: the leg fee
/// (cached USD micros floored by the request-size re-derivation from
/// `estimated_fee_bps`, so a stale lower-tier row cannot under-price a larger
/// request) plus absolute gas. This is the exact per-leg quantity
/// [`amount_aware_path_score`] sums, exposed so the route explainer can render
/// per-leg and total bps that match the ranking metric.
#[must_use]
pub fn effective_leg_cost_usd_micros(snapshot: &RouteCostSnapshot, request_usd_micros: u64) -> u64 {
    let bps_derived = fee_usd_micros_from_bps(snapshot.estimated_fee_bps, request_usd_micros);
    let fee = snapshot.estimated_fee_usd_micros.max(bps_derived);
    capped_add_u64(fee, snapshot.estimated_gas_usd_micros)
}

/// Sort `paths` in place by the amount-aware score, with deterministic
/// `path.id` tie-breaking (audit S2.2).
fn rank_paths_in_place(
    paths: &mut [TransitionPath],
    snapshots: &HashMap<String, RouteCostSnapshot>,
    pricing: &PricingSnapshot,
    request_usd_micros: u64,
) {
    paths.sort_by(|left, right| {
        let left_score = amount_aware_path_score(left, snapshots, pricing, request_usd_micros);
        let right_score = amount_aware_path_score(right, snapshots, pricing, request_usd_micros);
        path_sort_key(left, left_score, request_usd_micros).cmp(&path_sort_key(
            right,
            right_score,
            request_usd_micros,
        ))
    });
}

/// Per-unknown-leg penalty as a fraction of the request notional. A leg with no
/// fresh snapshot (and no live override) has genuinely unknown cost, so we must
/// not let it count as free or an unpriced path would always "win". We add this
/// penalty per unknown leg when ranking so a fully-priced cheaper path beats a
/// partially-unknown one, while two paths with the *same* unknown count still
/// compare on their real known cost. `10` == 10% of notional (~1000 bps) per
/// unknown leg, which dwarfs any realistic single-leg fee.
const UNKNOWN_LEG_PENALTY_DIVISOR: u64 = 10;

/// Floor for the unknown-leg penalty so it stays meaningful even for tiny or
/// zero request notionals (e.g. structural callers).
const UNKNOWN_LEG_PENALTY_FLOOR_USD_MICROS: u64 = 100 * USD_MICRO;

#[must_use]
fn unknown_leg_penalty_usd_micros(request_usd_micros: u64) -> u64 {
    (request_usd_micros / UNKNOWN_LEG_PENALTY_DIVISOR).max(UNKNOWN_LEG_PENALTY_FLOOR_USD_MICROS)
}

/// Sort key for least-cost ranking. The primary key is the path's total cost in
/// USD micros (monotonic with total bps at a fixed notional) plus a penalty for
/// any unknown leg, so the cheapest *real* route sorts first. Latency is a soft
/// secondary factor (only breaks exact cost ties), then hop count, then the
/// deterministic `path.id`. `missing_edges` is no longer a leading key; it is
/// folded into the cost via the penalty and otherwise kept only for display.
fn path_sort_key(
    path: &TransitionPath,
    score: RoutePathCostScore,
    request_usd_micros: u64,
) -> (u64, u64, usize, &str) {
    let penalty = unknown_leg_penalty_usd_micros(request_usd_micros)
        .saturating_mul(score.missing_edges as u64);
    let sort_cost = capped_add_u64(score.total_effective_cost_usd_micros, penalty);
    (
        sort_cost,
        score.total_latency_ms,
        path.transitions.len(),
        path.id.as_str(),
    )
}

/// Cache-free path score (no DB lookup). Used by the temporal boundary
/// requote fallback, the V1 limit-order fallback, and the offline trace
/// harness. With structural seeds removed this scores every leg as uncached
/// (zero fabricated cost, `missing_edges` per leg), so the ordering it
/// produces relies purely on hop count and `path.id` tie-breaks; the live
/// quote fanout downstream is what actually prices the routes. Callers must
/// pass the request size in USD micros explicitly.
#[must_use]
pub fn structural_path_score(
    path: &TransitionPath,
    pricing: &PricingSnapshot,
    request_usd_micros: u64,
) -> RoutePathCostScore {
    amount_aware_path_score(path, &HashMap::new(), pricing, request_usd_micros)
}

/// A live quote outcome that survived `validate_provider_quote`. Carries
/// the fields `select_best_quote` needs to rank it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveQuoteOutcome {
    pub quote_index: usize,
    pub path_id: String,
    pub hop_count: usize,
    pub estimated_amount_out: U256,
}

/// Strict-drop routes that still have an unpriced leg, with a cold-cache
/// safety fallback. Given the ranked candidate set (after Velora legs were
/// live-priced and folded into the cached hop costs), a route with
/// `missing_edges > 0` has a leg whose cost is genuinely unknown - a
/// broken/unavailable provider - so it must not be ranked or counted as free
/// and is dropped.
///
/// If that drop empties the set even though candidates existed (e.g. a
/// momentarily cold cache where every route has an unpriced curated hop), the
/// original input is returned unchanged so the downstream live-quote stage can
/// still try to price a route end-to-end rather than failing outright.
#[must_use]
pub fn retain_viable_ranked(ranked: Vec<RankedTransitionPath>) -> Vec<RankedTransitionPath> {
    let viable: Vec<RankedTransitionPath> = ranked
        .iter()
        .filter(|ranked_path| ranked_path.score.missing_edges == 0)
        .cloned()
        .collect();
    if viable.is_empty() {
        ranked
    } else {
        viable
    }
}

/// Pick the best-output live quote, deterministically tie-breaking by
/// `(amount_out desc, hops asc, path_id asc)`.
#[must_use]
pub fn select_best_quote(outcomes: &[LiveQuoteOutcome]) -> Option<usize> {
    outcomes
        .iter()
        .max_by(|left, right| {
            left.estimated_amount_out
                .cmp(&right.estimated_amount_out)
                .then(right.hop_count.cmp(&left.hop_count))
                .then(right.path_id.cmp(&left.path_id))
        })
        .map(|outcome| outcome.quote_index)
}

#[cfg(test)]
fn sample_amount_for_asset(
    asset: &DepositAsset,
    registry: &AssetRegistry,
    pricing: &PricingSnapshot,
) -> Option<U256> {
    let chain_asset = registry.chain_asset(asset)?;
    sample_amount_for_chain_asset_at_usd_micros(
        chain_asset,
        pricing,
        DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
    )
}

fn sample_amount_for_chain_asset_at_tier(
    chain_asset: &ChainAsset,
    pricing: &PricingSnapshot,
    tier: &RouteCostTier,
) -> Option<U256> {
    sample_amount_for_chain_asset_at_usd_micros(chain_asset, pricing, tier.sample_usd_micros)
}

fn sample_amount_for_chain_asset_at_usd_micros(
    chain_asset: &ChainAsset,
    pricing: &PricingSnapshot,
    sample_usd_micros: u64,
) -> Option<U256> {
    pricing
        .sample_amount_raw(
            sample_usd_micros,
            chain_asset.canonical,
            chain_asset.decimals,
        )
        .map(|amount| amount.max(U256::from(1_u64)))
}

fn quote_value_loss_bps(
    amount_in: U256,
    input_asset: &ChainAsset,
    amount_out: U256,
    output_asset: &ChainAsset,
    pricing: &PricingSnapshot,
) -> Option<u64> {
    let input_usd_micro = raw_amount_usd_micros(amount_in, input_asset, pricing)?;
    let output_usd_micro = raw_amount_usd_micros(amount_out, output_asset, pricing)?;
    loss_bps(input_usd_micro, output_usd_micro)
}

fn quote_value_loss_usd_micros(
    amount_in: U256,
    input_asset: &ChainAsset,
    amount_out: U256,
    output_asset: &ChainAsset,
    pricing: &PricingSnapshot,
) -> Option<u64> {
    let input_usd_micro = raw_amount_usd_micros(amount_in, input_asset, pricing)?;
    let output_usd_micro = raw_amount_usd_micros(amount_out, output_asset, pricing)?;
    loss_usd_micros(input_usd_micro, output_usd_micro)
}

fn raw_amount_usd_micros(
    raw_amount: U256,
    chain_asset: &ChainAsset,
    pricing: &PricingSnapshot,
) -> Option<U256> {
    let asset_usd_micro = U256::from(pricing.canonical_asset_usd_micro(chain_asset.canonical)?);
    Some(raw_amount.checked_mul(asset_usd_micro)? / checked_pow10(chain_asset.decimals)?)
}

/// Public helper for converting a raw token amount into USD micros via the
/// current pricing snapshot. Returns `None` when pricing for the canonical
/// asset is unknown (e.g. HYPE without a live oracle row) or when the
/// product overflows `U256`. Order manager calls this once per /quote to
/// drive amount-aware ranking.
#[must_use]
pub fn raw_amount_to_usd_micros(
    raw_amount: U256,
    chain_asset: &ChainAsset,
    pricing: &PricingSnapshot,
) -> Option<u64> {
    let value_u256 = raw_amount_usd_micros(raw_amount, chain_asset, pricing)?;
    u256_to_u64_saturating(value_u256)
}

fn loss_usd_micros(value_in: U256, value_out: U256) -> Option<u64> {
    if value_out >= value_in {
        return Some(0);
    }
    let loss = value_in.checked_sub(value_out)?;
    u256_to_u64_saturating(loss)
}

fn loss_bps(value_in: U256, value_out: U256) -> Option<u64> {
    if value_in.is_zero() {
        return None;
    }
    if value_out >= value_in {
        return Some(0);
    }
    let loss = value_in.checked_sub(value_out)?;
    div_ceil_u512_to_u64(
        U512::from(loss) * U512::from(BPS_DENOMINATOR),
        U512::from(value_in),
    )
    .map(|bps| bps.min(BPS_DENOMINATOR))
}

fn fee_usd_micros_from_bps(fee_bps: u64, sample_amount_usd_micros: u64) -> u64 {
    div_ceil_u512_to_u64(
        U512::from(sample_amount_usd_micros) * U512::from(fee_bps),
        U512::from(BPS_DENOMINATOR),
    )
    .unwrap_or(u64::MAX)
}

fn usd_cost_bps(cost_usd_micros: u64, sample_amount_usd_micros: u64) -> u64 {
    div_ceil_u512_to_u64(
        U512::from(cost_usd_micros) * U512::from(BPS_DENOMINATOR),
        U512::from(sample_amount_usd_micros),
    )
    .unwrap_or(u64::MAX)
}

fn div_ceil_u512_to_u64(numerator: U512, denominator: U512) -> Option<u64> {
    if denominator.is_zero() {
        return None;
    }
    if numerator.is_zero() {
        return Some(0);
    }
    let value = (numerator - U512::from(1_u64)) / denominator + U512::from(1_u64);
    if value > U512::from(u64::MAX) {
        return Some(u64::MAX);
    }
    value.to_string().parse::<u64>().ok()
}

fn u256_to_u64_saturating(value: U256) -> Option<u64> {
    if value > U256::from(u64::MAX) {
        return Some(u64::MAX);
    }
    value.to_string().parse::<u64>().ok()
}

fn capped_add_u64(left: u64, right: u64) -> u64 {
    left.saturating_add(right)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{AssetId, ChainId},
        services::asset_registry::{AssetSlot, ProviderId, RequiredCustodyRole},
    };

    fn asset(chain: &str) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset: AssetId::Native,
        }
    }

    fn transition(id: &str, kind: MarketOrderTransitionKind) -> TransitionDecl {
        let provider = match kind {
            MarketOrderTransitionKind::HyperliquidTrade => ProviderId::HyperliquidSpot,
            MarketOrderTransitionKind::UniversalRouterSwap => ProviderId::Kyberswap,
            MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                ProviderId::HyperliquidBridge
            }
            MarketOrderTransitionKind::UnitDeposit | MarketOrderTransitionKind::UnitWithdrawal => {
                ProviderId::Unit
            }
            MarketOrderTransitionKind::AcrossBridge => ProviderId::Across,
            MarketOrderTransitionKind::CctpBridge => ProviderId::Cctp,
        };
        transition_with_provider(id, kind, provider)
    }

    fn transition_with_provider(
        id: &str,
        kind: MarketOrderTransitionKind,
        provider: ProviderId,
    ) -> TransitionDecl {
        TransitionDecl {
            id: id.to_string(),
            kind,
            provider,
            input: AssetSlot {
                asset: asset("evm:1"),
                required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
            },
            output: AssetSlot {
                asset: asset("evm:8453"),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: crate::services::asset_registry::MarketOrderNode::External(asset("evm:1")),
            to: crate::services::asset_registry::MarketOrderNode::External(asset("evm:8453")),
        }
    }

    fn path(id: &str, transitions: Vec<TransitionDecl>) -> TransitionPath {
        TransitionPath {
            id: id.to_string(),
            transitions,
        }
    }

    #[test]
    fn classify_failure_buckets_common_provider_errors() {
        assert_eq!(
            classify_failure("HTTP 429 Too Many Requests"),
            FailureCategory::RateLimited
        );
        assert_eq!(
            classify_failure("upstream rate limit exceeded"),
            FailureCategory::RateLimited
        );
        assert_eq!(
            classify_failure("provider timed out"),
            FailureCategory::Timeout
        );
        assert_eq!(
            classify_failure("request timeout after 10s"),
            FailureCategory::Timeout
        );
        assert_eq!(
            classify_failure("provider returned no route"),
            FailureCategory::NoRoute
        );
        assert_eq!(
            classify_failure("502 Bad Gateway"),
            FailureCategory::UpstreamServer
        );
        assert_eq!(
            classify_failure("400 Bad Request: invalid token"),
            FailureCategory::UpstreamClient
        );
        assert_eq!(
            classify_failure("provider amount_out was not numeric: x"),
            FailureCategory::Other
        );
    }

    #[test]
    fn benign_unsatisfiable_amount_is_detected() {
        assert!(is_benign_unsatisfiable_amount(
            "hyperliquid quote: book side could not absorb remaining input 904951000"
        ));
        assert!(is_benign_unsatisfiable_amount(
            "hyperliquid quote: book side could not produce remaining output 12"
        ));
        assert!(is_benign_unsatisfiable_amount(
            "hyperliquid quote: empty book side"
        ));
        assert!(is_benign_unsatisfiable_amount(
            "Across returned HTTP 400: {\"type\":\"AcrossApiError\",\"code\":\"AMOUNT_TOO_HIGH\",\"status\":400}"
        ));
        assert!(!is_benign_unsatisfiable_amount("429 Too Many Requests"));
        assert!(!is_benign_unsatisfiable_amount("provider timed out"));
        assert!(!is_benign_unsatisfiable_amount(
            "400 Bad Request: invalid token"
        ));
    }

    #[test]
    fn retry_delay_grows_exponentially_and_clamps() {
        assert_eq!(
            retry_delay(1, FailureCategory::Other),
            Duration::from_secs(20)
        );
        assert_eq!(
            retry_delay(2, FailureCategory::Other),
            Duration::from_secs(40)
        );
        assert_eq!(
            retry_delay(3, FailureCategory::Other),
            Duration::from_secs(80)
        );
        // Eventually clamps to the per-category cap.
        assert_eq!(retry_delay(10, FailureCategory::Other), RETRY_MAX_DELAY);
        // Monotonic non-decreasing across attempts.
        let mut prev = Duration::ZERO;
        for attempts in 1..=8 {
            let delay = retry_delay(attempts, FailureCategory::Other);
            assert!(
                delay >= prev,
                "delay should not shrink at attempt {attempts}"
            );
            prev = delay;
        }
    }

    #[test]
    fn retry_delay_rate_limited_backs_off_harder() {
        assert_eq!(
            retry_delay(1, FailureCategory::RateLimited),
            Duration::from_secs(60)
        );
        assert_eq!(
            retry_delay(20, FailureCategory::RateLimited),
            RETRY_RATE_LIMITED_MAX_DELAY
        );
        for attempts in 1..=6 {
            assert!(
                retry_delay(attempts, FailureCategory::RateLimited)
                    >= retry_delay(attempts, FailureCategory::Other),
                "rate-limited backoff must be >= ordinary backoff at attempt {attempts}"
            );
        }
    }

    #[test]
    fn merge_paced_and_retries_dedupes_by_cell() {
        let tier = &ROUTE_COST_TIERS[0];
        let t1 = transition("across:eth->base", MarketOrderTransitionKind::AcrossBridge);
        let t2 = transition("cctp:eth->base", MarketOrderTransitionKind::CctpBridge);
        let paced = vec![(t1.clone(), tier)];
        // t1 duplicates the paced cell; t2 is new.
        let retries = vec![(t1.clone(), tier), (t2.clone(), tier)];
        let merged = merge_paced_and_retries(paced, retries);
        assert_eq!(merged.len(), 2);
        let keys: Vec<RetryKey> = merged.iter().map(|(t, ti)| retry_key(t, ti)).collect();
        assert!(keys.contains(&retry_key(&t1, tier)));
        assert!(keys.contains(&retry_key(&t2, tier)));
    }

    #[test]
    fn merge_paced_and_retries_distinguishes_tiers() {
        let t1 = transition("across:eth->base", MarketOrderTransitionKind::AcrossBridge);
        let tier_a = &ROUTE_COST_TIERS[0];
        let tier_b = &ROUTE_COST_TIERS[1];
        let paced = vec![(t1.clone(), tier_a)];
        // Same transition at a different tier is a distinct cell.
        let retries = vec![(t1.clone(), tier_b)];
        let merged = merge_paced_and_retries(paced, retries);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn paced_target_index_is_linear_and_clamped() {
        let window = Duration::from_secs(1800);
        // Nothing due at t=0.
        assert_eq!(paced_target_index(360, Duration::ZERO, window), 0);
        // Halfway through the window -> half the units.
        assert_eq!(
            paced_target_index(360, Duration::from_secs(900), window),
            180
        );
        // At/after the deadline -> the whole sweep, never more.
        assert_eq!(paced_target_index(360, window, window), 360);
        assert_eq!(
            paced_target_index(360, Duration::from_secs(5000), window),
            360
        );
        // Empty plan is always satisfied.
        assert_eq!(paced_target_index(0, Duration::from_secs(900), window), 0);
    }

    #[test]
    fn interleaved_refresh_plan_round_robins_providers_and_covers_every_unit() {
        let transitions = vec![
            transition("across-a", MarketOrderTransitionKind::AcrossBridge),
            transition("across-b", MarketOrderTransitionKind::AcrossBridge),
            transition("cctp-a", MarketOrderTransitionKind::CctpBridge),
            transition(
                "kyberswap-b",
                MarketOrderTransitionKind::UniversalRouterSwap,
            ),
            transition_with_provider(
                "kyberswap-a",
                MarketOrderTransitionKind::UniversalRouterSwap,
                ProviderId::Kyberswap,
            ),
        ];

        let plan = interleaved_refresh_plan(&transitions);

        // Every (transition, tier) unit appears exactly once.
        assert_eq!(plan.len(), transitions.len() * ROUTE_COST_TIERS.len());
        for transition in &transitions {
            let count = plan
                .iter()
                .filter(|(decl, _)| decl.id == transition.id)
                .count();
            assert_eq!(count, ROUTE_COST_TIERS.len(), "{} coverage", transition.id);
        }

        // The first units rotate across the three providers rather than
        // draining one venue before the next.
        let lead_providers: Vec<&str> = plan
            .iter()
            .take(3)
            .map(|(decl, _)| decl.provider.as_str())
            .collect();
        let distinct: std::collections::BTreeSet<&str> = lead_providers.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            3,
            "providers should interleave: {lead_providers:?}"
        );
    }

    #[test]
    fn interleaved_refresh_plan_cycles_distinct_routes_within_a_provider() {
        // A provider with several routes must step through every distinct route
        // before repeating one, so paced one-by-one sampling visibly cycles the
        // cells instead of firing all tiers of a single route back-to-back.
        let transitions = vec![
            transition("across-a", MarketOrderTransitionKind::AcrossBridge),
            transition("across-b", MarketOrderTransitionKind::AcrossBridge),
            transition("across-c", MarketOrderTransitionKind::AcrossBridge),
        ];
        let plan = interleaved_refresh_plan(&transitions);
        let across: Vec<&str> = plan
            .iter()
            .filter(|(decl, _)| decl.provider == ProviderId::Across)
            .map(|(decl, _)| decl.id.as_str())
            .collect();

        // The first three consecutive Across units are the three distinct
        // routes (tier-outer ordering), not the same route three times.
        let lead: std::collections::BTreeSet<&str> = across.iter().take(3).copied().collect();
        assert_eq!(
            lead.len(),
            3,
            "first units should be distinct routes: {across:?}"
        );
        // No two adjacent Across units share a route id.
        for pair in across.windows(2) {
            assert_ne!(
                pair[0], pair[1],
                "adjacent units repeat a route: {across:?}"
            );
        }
    }

    #[test]
    fn claim_due_units_paces_each_provider_independently() {
        // Two providers with different cell counts: Across has more curated
        // edges than CCTP, so it should fire more often over the same window.
        let transitions = vec![
            transition("across-a", MarketOrderTransitionKind::AcrossBridge),
            transition("across-b", MarketOrderTransitionKind::AcrossBridge),
            transition("across-c", MarketOrderTransitionKind::AcrossBridge),
            transition("cctp-a", MarketOrderTransitionKind::CctpBridge),
        ];
        let plan = interleaved_refresh_plan(&transitions);
        let window = Duration::from_secs(1800);
        let start = Utc::now();
        let mut cursors: BTreeMap<String, ProviderCursor> = BTreeMap::new();

        let across_total = 3 * ROUTE_COST_TIERS.len();
        let cctp_total = ROUTE_COST_TIERS.len();
        let total = across_total + cctp_total;

        // Walk the whole window at the production tick (window / total) and
        // count how many units each provider claimed. One extra tick past the
        // window lets each provider's deadline-paced cycle complete even when
        // the tick does not divide the window exactly (the real worker keeps
        // ticking past the window and the cursor re-anchors).
        let tick = window / u32::try_from(total).unwrap();
        let mut across = 0usize;
        let mut cctp = 0usize;
        let mut max_per_provider_per_tick = 0usize;
        let mut elapsed = Duration::ZERO;
        while elapsed <= window + tick {
            let now = start + chrono::Duration::from_std(elapsed).unwrap();
            let chunk = claim_due_units_paced(&mut cursors, &plan, now, window);
            let mut across_this_tick = 0usize;
            let mut cctp_this_tick = 0usize;
            for (decl, _) in &chunk {
                match decl.provider {
                    ProviderId::Across => across_this_tick += 1,
                    ProviderId::Cctp => cctp_this_tick += 1,
                    other => panic!("unexpected provider {other:?}"),
                }
            }
            // Each provider advances at most one unit per tick at this rate.
            max_per_provider_per_tick = max_per_provider_per_tick
                .max(across_this_tick)
                .max(cctp_this_tick);
            across += across_this_tick;
            cctp += cctp_this_tick;
            elapsed += tick;
        }

        assert!(
            max_per_provider_per_tick <= 1,
            "no provider should bunch within a tick; saw {max_per_provider_per_tick}"
        );
        // Across has 3x the cells, so it fires ~3x as often as CCTP.
        assert!(
            across >= cctp * 2,
            "across ({across}) should trickle far more often than cctp ({cctp})"
        );
        // Both providers complete roughly one full sweep across the window.
        assert!(across >= across_total, "across covered one sweep: {across}");
        assert!(cctp >= cctp_total, "cctp covered one sweep: {cctp}");
    }

    #[test]
    fn route_cost_pacing_tick_keeps_each_provider_below_one_unit_per_tick() {
        // With tick = window / total, every provider's per-tick advance
        // (provider_cells * tick / window = provider_cells / total) is < 1.
        let window = Duration::from_secs(1800);
        let total = 408usize;
        let tick = window / u32::try_from(total).unwrap();
        // The busiest provider (216 cells) advances 216/408 < 1 unit per tick.
        let busiest = 216usize;
        assert_eq!(
            paced_target_index(busiest, tick, window),
            0,
            "busiest provider must not advance more than one unit per tick"
        );
    }

    #[test]
    fn effective_cost_adds_fee_and_gas_bps() {
        let snapshot = RouteCostSnapshot {
            transition_id: "edge".to_string(),
            amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
            provider: "test".to_string(),
            edge_kind: "fixed_pair_swap".to_string(),
            source_asset: asset("evm:1"),
            destination_asset: asset("evm:8453"),
            estimated_fee_bps: 5,
            estimated_fee_usd_micros: 500_000,
            estimated_gas_usd_micros: 1_000_000,
            estimated_latency_ms: 1,
            sample_amount_usd_micros: 1_000_000_000,
            quote_source: "test".to_string(),
            refreshed_at: Utc::now(),
            expires_at: Utc::now(),
        };

        assert_eq!(snapshot.effective_cost_usd_micros(), 1_500_000);
        assert_eq!(snapshot.effective_cost_bps(), 15);
    }

    #[test]
    fn effective_cost_caps_overflowing_gas_bps_explicitly() {
        let snapshot = RouteCostSnapshot {
            transition_id: "edge".to_string(),
            amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
            provider: "test".to_string(),
            edge_kind: "fixed_pair_swap".to_string(),
            source_asset: asset("evm:1"),
            destination_asset: asset("evm:8453"),
            estimated_fee_bps: 5,
            estimated_fee_usd_micros: 1,
            estimated_gas_usd_micros: u64::MAX,
            estimated_latency_ms: 1,
            sample_amount_usd_micros: 1,
            quote_source: "test".to_string(),
            refreshed_at: Utc::now(),
            expires_at: Utc::now(),
        };

        assert_eq!(snapshot.effective_cost_bps(), u64::MAX);
    }

    #[test]
    fn path_score_prefers_known_lower_cost_edges() {
        let expensive = transition("expensive", MarketOrderTransitionKind::AcrossBridge);
        let cheap_a = transition("cheap_a", MarketOrderTransitionKind::HyperliquidTrade);
        let cheap_b = transition("cheap_b", MarketOrderTransitionKind::HyperliquidTrade);
        let now = Utc::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            expensive.id.clone(),
            RouteCostSnapshot {
                transition_id: expensive.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "across".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: expensive.input.asset.clone(),
                destination_asset: expensive.output.asset.clone(),
                estimated_fee_bps: 20,
                estimated_fee_usd_micros: 2_000_000,
                estimated_gas_usd_micros: 0,
                estimated_latency_ms: 1,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        for transition in [&cheap_a, &cheap_b] {
            snapshots.insert(
                transition.id.clone(),
                RouteCostSnapshot {
                    transition_id: transition.id.clone(),
                    amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                    provider: "hyperliquid".to_string(),
                    edge_kind: "fixed_pair_swap".to_string(),
                    source_asset: transition.input.asset.clone(),
                    destination_asset: transition.output.asset.clone(),
                    estimated_fee_bps: 4,
                    estimated_fee_usd_micros: 400_000,
                    estimated_gas_usd_micros: 0,
                    estimated_latency_ms: 1,
                    sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                    quote_source: "test".to_string(),
                    refreshed_at: now,
                    expires_at: now,
                },
            );
        }

        let one_hop = path("one_hop", vec![expensive]);
        let two_hop = path("two_hop", vec![cheap_a, cheap_b]);

        assert!(
            path_score(&two_hop, &snapshots).total_effective_cost_usd_micros
                < path_score(&one_hop, &snapshots).total_effective_cost_usd_micros
        );
    }
    #[test]
    fn path_score_prefers_hyperevm_route_when_micro_usd_costs_are_lower() {
        let cctp_arb = transition("cctp_to_arb", MarketOrderTransitionKind::CctpBridge);
        let hl_bridge = transition(
            "arb_hl_bridge",
            MarketOrderTransitionKind::HyperliquidBridgeDeposit,
        );
        let cctp_hyperevm = transition("cctp_to_hyperevm", MarketOrderTransitionKind::CctpBridge);
        let hypercore_bridge = transition(
            "hyperevm_hypercore_bridge",
            MarketOrderTransitionKind::HyperliquidBridgeDeposit,
        );
        let trade = transition("trade", MarketOrderTransitionKind::HyperliquidTrade);
        let withdraw = transition("withdraw", MarketOrderTransitionKind::UnitWithdrawal);
        let now = Utc::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            cctp_arb.id.clone(),
            RouteCostSnapshot {
                transition_id: cctp_arb.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "cctp".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: cctp_arb.input.asset.clone(),
                destination_asset: cctp_arb.output.asset.clone(),
                estimated_fee_bps: 0,
                estimated_fee_usd_micros: 0,
                estimated_gas_usd_micros: 16_663,
                estimated_latency_ms: 60_000,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        snapshots.insert(
            hl_bridge.id.clone(),
            RouteCostSnapshot {
                transition_id: hl_bridge.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "hyperliquid_bridge".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: hl_bridge.input.asset.clone(),
                destination_asset: hl_bridge.output.asset.clone(),
                estimated_fee_bps: 0,
                estimated_fee_usd_micros: 0,
                estimated_gas_usd_micros: 0,
                estimated_latency_ms: 30_000,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        snapshots.insert(
            cctp_hyperevm.id.clone(),
            RouteCostSnapshot {
                transition_id: cctp_hyperevm.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "cctp".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: cctp_hyperevm.input.asset.clone(),
                destination_asset: cctp_hyperevm.output.asset.clone(),
                estimated_fee_bps: 0,
                estimated_fee_usd_micros: 0,
                estimated_gas_usd_micros: 8_264,
                estimated_latency_ms: 60_000,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        snapshots.insert(
            hypercore_bridge.id.clone(),
            RouteCostSnapshot {
                transition_id: hypercore_bridge.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "hypercore_bridge".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: hypercore_bridge.input.asset.clone(),
                destination_asset: hypercore_bridge.output.asset.clone(),
                estimated_fee_bps: 0,
                estimated_fee_usd_micros: 0,
                estimated_gas_usd_micros: 0,
                estimated_latency_ms: 30_000,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        for transition in [&trade, &withdraw] {
            snapshots.insert(
                transition.id.clone(),
                RouteCostSnapshot {
                    transition_id: transition.id.clone(),
                    amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                    provider: transition.provider.as_str().to_string(),
                    edge_kind: transition.route_edge_kind().as_str().to_string(),
                    source_asset: transition.input.asset.clone(),
                    destination_asset: transition.output.asset.clone(),
                    estimated_fee_bps: if transition.kind
                        == MarketOrderTransitionKind::HyperliquidTrade
                    {
                        4
                    } else {
                        0
                    },
                    estimated_fee_usd_micros: if transition.kind
                        == MarketOrderTransitionKind::HyperliquidTrade
                    {
                        400_000
                    } else {
                        0
                    },
                    estimated_gas_usd_micros: 0,
                    estimated_latency_ms: if transition.kind
                        == MarketOrderTransitionKind::HyperliquidTrade
                    {
                        1_500
                    } else {
                        60_000
                    },
                    sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                    quote_source: "test".to_string(),
                    refreshed_at: now,
                    expires_at: now,
                },
            );
        }

        let arb_path = path(
            "arb",
            vec![cctp_arb, hl_bridge, trade.clone(), withdraw.clone()],
        );
        let hyperevm_path = path(
            "hyperevm",
            vec![cctp_hyperevm, hypercore_bridge, trade, withdraw],
        );

        assert!(
            path_score(&hyperevm_path, &snapshots).total_effective_cost_usd_micros
                < path_score(&arb_path, &snapshots).total_effective_cost_usd_micros
        );
    }

    #[test]
    fn sample_amount_uses_asset_decimals_and_reference_prices() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let usdt = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
        };
        // BTC (8 decimals, priced off the BTC reference) exercises the
        // non-stable decimals path now that cbBTC has been removed.
        let btc = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };

        assert_eq!(
            sample_amount_for_asset(&usdt, &registry, &pricing),
            Some(U256::from(1_000_000_000_u64))
        );
        assert_eq!(
            sample_amount_for_asset(&btc, &registry, &pricing),
            Some(U256::from(1_000_000_u64))
        );
    }

    #[test]
    fn pricing_freshness_rejects_static_stale_and_expired_snapshots() {
        let now = Utc::now();
        let ttl = Duration::from_secs(600);
        let mut market = PricingSnapshot::static_bootstrap(now);
        market.source = "test_market_pricing".to_string();

        assert!(pricing_snapshot_is_fresh(&market, now, ttl));
        assert!(!pricing_snapshot_is_fresh(
            &PricingSnapshot::static_bootstrap(now),
            now,
            ttl
        ));

        let mut stale = market.clone();
        stale.captured_at = now - chrono::Duration::seconds(601);
        assert!(!pricing_snapshot_is_fresh(&stale, now, ttl));

        let mut expired = market;
        expired.expires_at = Some(now - chrono::Duration::seconds(1));
        assert!(!pricing_snapshot_is_fresh(&expired, now, ttl));
    }

    #[test]
    fn route_cost_refresh_requires_live_fresh_pricing() {
        let now = Utc::now();
        let ttl = Duration::from_secs(600);
        let mut live = PricingSnapshot::static_bootstrap(now);
        live.source = "test_market_pricing".to_string();

        require_live_pricing_for_route_cost_refresh(&live, now, ttl).unwrap();

        let static_pricing = PricingSnapshot::static_bootstrap(now);
        let error =
            require_live_pricing_for_route_cost_refresh(&static_pricing, now, ttl).unwrap_err();
        assert!(matches!(error, RouterCoreError::NotReady { .. }));
        assert!(
            error
                .to_string()
                .contains("refusing to refresh route costs"),
            "unexpected error: {error}"
        );

        let mut stale = live;
        stale.captured_at = now - chrono::Duration::seconds(601);
        assert!(require_live_pricing_for_route_cost_refresh(&stale, now, ttl).is_err());
    }

    fn usdc(chain: &str, address: &str) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset: AssetId::reference(address),
        }
    }

    #[test]
    fn quote_value_loss_bps_normalizes_decimals_and_prices() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let eth = registry.chain_asset(&asset("evm:1")).unwrap();
        let usdc = registry
            .chain_asset(&usdc("evm:1", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"))
            .unwrap();

        assert_eq!(
            quote_value_loss_bps(
                U256::from(1_000_000_000_000_000_000_u128),
                eth,
                U256::from(2_970_000_000_u64),
                usdc,
                &pricing,
            ),
            Some(100)
        );
    }

    #[test]
    fn quote_value_loss_bps_rejects_overflowing_value_conversion() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let btc = registry
            .chain_asset(&DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            })
            .unwrap();

        assert_eq!(
            quote_value_loss_bps(U256::MAX, btc, U256::from(1_u64), btc, &pricing),
            None
        );
    }

    #[test]
    fn raw_amount_usd_micros_rejects_unrepresentable_decimals() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let mut eth = registry.chain_asset(&asset("evm:1")).unwrap().clone();
        eth.decimals = u8::MAX;

        assert_eq!(
            raw_amount_usd_micros(U256::from(1_u64), &eth, &pricing),
            None
        );
    }

    #[test]
    fn loss_bps_handles_values_that_overflow_u256_intermediate_products() {
        let value_in = U256::MAX;
        let value_out = value_in - value_in / U256::from(2_u64);

        assert_eq!(loss_bps(value_in, value_out), Some(5_000));
    }

    #[test]
    fn loss_bps_rounds_loss_up() {
        assert_eq!(
            loss_bps(U256::from(1_000_000_u64), U256::from(999_001_u64)),
            Some(10)
        );
        assert_eq!(
            loss_bps(U256::from(1_000_000_u64), U256::from(1_000_001_u64)),
            Some(0)
        );
    }

    // ----------------------------------------------------------------------
    // S1.2 / S2.2 / S2.3 - amount-aware ranking, size penalty, tier table,
    // select_best_quote, refresher fanout. Each test block
    // is grouped by surface.
    // ----------------------------------------------------------------------

    fn make_snapshot(
        transition_id: &str,
        fee_bps: u64,
        fee_usd_micros: u64,
        gas_usd_micros: u64,
        latency_ms: u64,
        sample_usd_micros: u64,
    ) -> RouteCostSnapshot {
        let now = Utc::now();
        RouteCostSnapshot {
            transition_id: transition_id.to_string(),
            amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
            provider: "test".to_string(),
            edge_kind: "fixed_pair_swap".to_string(),
            source_asset: asset("evm:1"),
            destination_asset: asset("evm:8453"),
            estimated_fee_bps: fee_bps,
            estimated_fee_usd_micros: fee_usd_micros,
            estimated_gas_usd_micros: gas_usd_micros,
            estimated_latency_ms: latency_ms,
            sample_amount_usd_micros: sample_usd_micros,
            quote_source: "test".to_string(),
            refreshed_at: now,
            expires_at: now,
        }
    }

    // -- tier table --------------------------------------------------------

    #[test]
    fn select_route_cost_tier_rounds_up_to_smallest_tier_that_covers_request() {
        assert_eq!(select_route_cost_tier(50 * USD_MICRO).label, "usd_100");
        assert_eq!(select_route_cost_tier(100 * USD_MICRO).label, "usd_100");
        assert_eq!(select_route_cost_tier(101 * USD_MICRO).label, "usd_500");
        assert_eq!(select_route_cost_tier(500 * USD_MICRO).label, "usd_500");
        assert_eq!(select_route_cost_tier(501 * USD_MICRO).label, "usd_1000");
        assert_eq!(select_route_cost_tier(1_500 * USD_MICRO).label, "usd_10000");
        assert_eq!(
            select_route_cost_tier(50_000 * USD_MICRO).label,
            "usd_50000"
        );
        assert_eq!(
            select_route_cost_tier(60_000 * USD_MICRO).label,
            "usd_75000"
        );
        assert_eq!(select_route_cost_tier(0).label, "usd_100");
    }

    #[test]
    fn select_route_cost_tier_clamps_above_table_top() {
        let last = ROUTE_COST_TIERS
            .last()
            .expect("non-empty route cost tier table");
        assert_eq!(
            select_route_cost_tier(last.sample_usd_micros.saturating_mul(10)).label,
            last.label
        );
    }

    #[test]
    fn route_cost_tier_table_is_strictly_monotone_in_size_and_unique_in_label() {
        let mut prev = 0_u64;
        let mut seen_labels = std::collections::HashSet::new();
        for tier in ROUTE_COST_TIERS {
            assert!(
                tier.sample_usd_micros > prev,
                "tier table not strictly increasing"
            );
            assert!(
                seen_labels.insert(tier.label),
                "tier label {} duplicated",
                tier.label
            );
            prev = tier.sample_usd_micros;
        }
    }

    // -- amount-aware scoring ---------------------------------------------

    #[test]
    fn amount_aware_score_uses_request_size_for_bps_legs() {
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let edge = transition(
            "kyberswap_leg",
            MarketOrderTransitionKind::UniversalRouterSwap,
        );
        let p = path("p", vec![edge.clone()]);
        let mut snapshots = HashMap::new();
        snapshots.insert(
            edge.id.clone(),
            make_snapshot(
                &edge.id,
                25,
                2_500_000,
                0,
                1_000,
                DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            ),
        );

        let at_1k = amount_aware_path_score(&p, &snapshots, &pricing, 1_000 * USD_MICRO);
        let at_1m = amount_aware_path_score(&p, &snapshots, &pricing, 1_000_000 * USD_MICRO);
        assert!(
            at_1m.total_effective_cost_usd_micros > at_1k.total_effective_cost_usd_micros * 100,
            "cost at $1m must scale ~1000x of $1k for a pure-bps leg (got $1k={}, $1m={})",
            at_1k.total_effective_cost_usd_micros,
            at_1m.total_effective_cost_usd_micros
        );
    }

    #[test]
    fn amount_aware_score_floors_cached_usd_micros_at_request_re_derivation() {
        // A snapshot whose cached USD micros was computed at $1k but whose
        // bps says "25 bps of $1m = $2,500". At a $1m request the amount-
        // aware scorer must take the larger of the two.
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let edge = transition(
            "cached_kyberswap",
            MarketOrderTransitionKind::UniversalRouterSwap,
        );
        let p = path("p", vec![edge.clone()]);
        let stale_cached_usd_micros = 2_500_000_u64; // 25 bps of $1k
        let mut snapshots = HashMap::new();
        snapshots.insert(
            edge.id.clone(),
            make_snapshot(
                &edge.id,
                25,
                stale_cached_usd_micros,
                0,
                1_000,
                DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            ),
        );
        let at_1m = amount_aware_path_score(&p, &snapshots, &pricing, 1_000_000 * USD_MICRO);
        assert!(
            at_1m.total_effective_cost_usd_micros > stale_cached_usd_micros * 100,
            "expected re-derived fee to dominate stale cached usd_micros"
        );
    }

    #[test]
    fn amount_aware_score_marks_uncached_edges_as_missing() {
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let cached = transition("cached", MarketOrderTransitionKind::CctpBridge);
        let uncached = transition("uncached", MarketOrderTransitionKind::HyperliquidTrade);
        let p = path("p", vec![cached.clone(), uncached.clone()]);
        let mut snapshots = HashMap::new();
        snapshots.insert(
            cached.id.clone(),
            make_snapshot(
                &cached.id,
                0,
                0,
                10_000_000,
                60_000,
                DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            ),
        );
        let score = amount_aware_path_score(&p, &snapshots, &pricing, 1_000 * USD_MICRO);
        assert_eq!(score.missing_edges, 1);
    }

    #[test]
    fn amount_aware_score_sorts_path_id_as_final_tiebreaker() {
        // Two paths with identical scores; only `path.id` differs.
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let edge = transition("edge", MarketOrderTransitionKind::CctpBridge);
        let mut paths = vec![
            path("b_path", vec![edge.clone()]),
            path("a_path", vec![edge.clone()]),
        ];
        rank_paths_in_place(&mut paths, &HashMap::new(), &pricing, 1_000 * USD_MICRO);
        assert_eq!(
            paths[0].id, "a_path",
            "path.id is the deterministic tiebreaker"
        );
    }

    // -- ranked-path test helper --------------------------------------------

    fn ranked_at(score_usd_micros: u64, hop_count: usize, path_id: &str) -> RankedTransitionPath {
        let mut transitions = Vec::with_capacity(hop_count.max(1));
        for i in 0..hop_count.max(1) {
            transitions.push(transition(
                &format!("e{i}_{path_id}"),
                MarketOrderTransitionKind::CctpBridge,
            ));
        }
        RankedTransitionPath {
            path: path(path_id, transitions),
            score: RoutePathCostScore {
                missing_edges: 0,
                total_effective_cost_usd_micros: score_usd_micros,
                total_latency_ms: 0,
            },
        }
    }

    // -- select_best_quote -------------------------------------------------

    fn outcome(
        quote_index: usize,
        amount_out: u64,
        hop_count: usize,
        path_id: &str,
    ) -> LiveQuoteOutcome {
        LiveQuoteOutcome {
            quote_index,
            path_id: path_id.to_string(),
            hop_count,
            estimated_amount_out: U256::from(amount_out),
        }
    }

    #[test]
    fn select_best_quote_picks_highest_amount_out() {
        let outcomes = vec![
            outcome(0, 100, 1, "a"),
            outcome(1, 250, 1, "b"),
            outcome(2, 200, 1, "c"),
        ];
        assert_eq!(select_best_quote(&outcomes), Some(1));
    }

    #[test]
    fn select_best_quote_breaks_ties_by_hops_then_path_id() {
        let outcomes = vec![
            outcome(0, 200, 2, "b"),
            outcome(1, 200, 1, "c"),
            outcome(2, 200, 1, "a"),
        ];
        // hops=1 wins over hops=2; then path_id "a" beats "c".
        assert_eq!(select_best_quote(&outcomes), Some(2));
    }

    #[test]
    fn select_best_quote_returns_none_on_empty() {
        assert_eq!(select_best_quote(&[]), None);
    }

    // -- effective_leg_cost_usd_micros ------------------------------------

    #[test]
    fn effective_leg_cost_uses_bps_floor_when_cached_usd_is_stale_low() {
        // Cached USD fee was sampled at a tiny notional (so it is ~$0), but the
        // request is $1000. The bps re-derivation must floor the cost so a stale
        // low-tier row cannot under-price a large request: 20 bps of $1000 = $2.
        let request = 1_000 * USD_MICRO;
        let snapshot = make_snapshot("leg", 20, 1, 0, 0, 1);
        assert_eq!(
            effective_leg_cost_usd_micros(&snapshot, request),
            2 * USD_MICRO
        );
    }

    #[test]
    fn effective_leg_cost_keeps_cached_usd_over_bps_floor_and_adds_gas() {
        // bps floor = 5 bps of $1000 = $0.50; cached fee $0.90 wins; plus $0.10 gas.
        let request = 1_000 * USD_MICRO;
        let snapshot = make_snapshot("leg", 5, 900_000, 100_000, 0, request);
        assert_eq!(effective_leg_cost_usd_micros(&snapshot, request), 1_000_000);
    }

    #[test]
    fn effective_leg_cost_zero_notional_is_safe() {
        let snapshot = make_snapshot("leg", 20, 0, 0, 0, 0);
        assert_eq!(effective_leg_cost_usd_micros(&snapshot, 0), 0);
    }

    // -- path_sort_key -----------------------------------------------------

    fn score_of(missing: usize, cost: u64, latency: u64) -> RoutePathCostScore {
        RoutePathCostScore {
            missing_edges: missing,
            total_effective_cost_usd_micros: cost,
            total_latency_ms: latency,
        }
    }

    #[test]
    fn path_sort_key_orders_by_total_cost_before_latency() {
        let req = 1_000 * USD_MICRO;
        let cheap = path(
            "z",
            vec![transition("e", MarketOrderTransitionKind::CctpBridge)],
        );
        let pricey = path(
            "a",
            vec![transition("f", MarketOrderTransitionKind::CctpBridge)],
        );
        // Cheaper total cost wins even with much higher latency.
        assert!(
            path_sort_key(&cheap, score_of(0, 100, 9_999), req)
                < path_sort_key(&pricey, score_of(0, 200, 0), req)
        );
    }

    #[test]
    fn path_sort_key_penalizes_unknown_legs_so_fully_priced_route_wins() {
        let req = 1_000 * USD_MICRO;
        // A fully-priced route at $5 vs a partially-unknown route whose known
        // cost is only $1: the unknown-leg penalty (10% of $1000 = $100) must
        // make the fully-priced route sort first.
        let priced = path(
            "priced",
            vec![transition("p", MarketOrderTransitionKind::CctpBridge)],
        );
        let missing = path(
            "missing",
            vec![transition("m", MarketOrderTransitionKind::CctpBridge)],
        );
        assert!(
            path_sort_key(&priced, score_of(0, 5 * USD_MICRO, 0), req)
                < path_sort_key(&missing, score_of(1, 1 * USD_MICRO, 0), req)
        );
    }

    #[test]
    fn path_sort_key_equal_missing_counts_compare_on_real_cost() {
        let req = 1_000 * USD_MICRO;
        let cheaper = path(
            "a",
            vec![transition("x", MarketOrderTransitionKind::CctpBridge)],
        );
        let pricier = path(
            "b",
            vec![transition("y", MarketOrderTransitionKind::CctpBridge)],
        );
        assert!(
            path_sort_key(&cheaper, score_of(1, 1_000, 0), req)
                < path_sort_key(&pricier, score_of(1, 2_000, 0), req)
        );
    }

    // -- amount_aware_path_score ------------------------------------------

    #[test]
    fn amount_aware_score_marks_uncached_leg_missing_with_zero_cost() {
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let p = path(
            "p",
            vec![transition("only", MarketOrderTransitionKind::CctpBridge)],
        );
        let score = amount_aware_path_score(&p, &HashMap::new(), &pricing, 1_000 * USD_MICRO);
        assert_eq!(score.missing_edges, 1);
        assert_eq!(score.total_effective_cost_usd_micros, 0);
    }

    #[test]
    fn amount_aware_score_prefers_cctp_over_across_when_one_bps_cheaper() {
        // The PEPE case: identical routes apart from the bridge leg; CCTP at
        // 1 bps must beat Across at 2 bps.
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let req = 1_000 * USD_MICRO;
        let cctp = transition("cctp", MarketOrderTransitionKind::CctpBridge);
        let across = transition("across", MarketOrderTransitionKind::AcrossBridge);
        let mut snapshots = HashMap::new();
        snapshots.insert(cctp.id.clone(), make_snapshot(&cctp.id, 1, 0, 0, 0, req));
        snapshots.insert(
            across.id.clone(),
            make_snapshot(&across.id, 2, 0, 0, 0, req),
        );
        let cctp_score = amount_aware_path_score(&path("c", vec![cctp]), &snapshots, &pricing, req);
        let across_score =
            amount_aware_path_score(&path("a", vec![across]), &snapshots, &pricing, req);
        assert!(
            cctp_score.total_effective_cost_usd_micros
                < across_score.total_effective_cost_usd_micros
        );
    }

    #[test]
    fn kyberswap_override_fold_reorders_anchor_choice_toward_cheaper_total() {
        // PEPE -> USDC via KyberSwap (57 bps) + CCTP (1 bps) = 58 bps, vs
        // PEPE -> ETH via KyberSwap (39 bps) + a 13 bps hop = 52 bps. Once the
        // live KyberSwap legs are folded in as snapshots, the ETH anchor wins.
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let req = 1_000 * USD_MICRO;
        let v_usdc = transition("v_usdc", MarketOrderTransitionKind::UniversalRouterSwap);
        let cctp = transition("cctp", MarketOrderTransitionKind::CctpBridge);
        let v_eth = transition("v_eth", MarketOrderTransitionKind::UniversalRouterSwap);
        let unit = transition("unit", MarketOrderTransitionKind::UnitDeposit);
        let mut snapshots = HashMap::new();
        snapshots.insert(
            v_usdc.id.clone(),
            make_snapshot(&v_usdc.id, 57, 0, 0, 0, req),
        );
        snapshots.insert(cctp.id.clone(), make_snapshot(&cctp.id, 1, 0, 0, 0, req));
        snapshots.insert(v_eth.id.clone(), make_snapshot(&v_eth.id, 39, 0, 0, 0, req));
        snapshots.insert(unit.id.clone(), make_snapshot(&unit.id, 13, 0, 0, 0, req));
        let usdc_path =
            amount_aware_path_score(&path("usdc", vec![v_usdc, cctp]), &snapshots, &pricing, req);
        let eth_path =
            amount_aware_path_score(&path("eth", vec![v_eth, unit]), &snapshots, &pricing, req);
        assert!(
            eth_path.total_effective_cost_usd_micros < usdc_path.total_effective_cost_usd_micros,
            "cheaper KyberSwap->ETH total (52 bps) must beat KyberSwap->USDC (58 bps)"
        );
    }

    #[test]
    fn ranker_total_equals_sum_of_displayed_leg_costs() {
        // The dashboard renders each leg's bps from effective_leg_cost_usd_micros
        // and the path total from the ranker score; they must be the same data.
        // The score is exactly the sum of the per-leg effective costs.
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let req = 1_000 * USD_MICRO;
        let a = transition("a", MarketOrderTransitionKind::CctpBridge);
        let b = transition("b", MarketOrderTransitionKind::HyperliquidTrade);
        let mut snapshots = HashMap::new();
        snapshots.insert(a.id.clone(), make_snapshot(&a.id, 3, 0, 25_000, 0, req));
        snapshots.insert(b.id.clone(), make_snapshot(&b.id, 9, 0, 0, 0, req));
        let p = path("p", vec![a.clone(), b.clone()]);
        let score = amount_aware_path_score(&p, &snapshots, &pricing, req);
        let leg_sum = effective_leg_cost_usd_micros(snapshots.get(&a.id).unwrap(), req)
            + effective_leg_cost_usd_micros(snapshots.get(&b.id).unwrap(), req);
        assert_eq!(score.total_effective_cost_usd_micros, leg_sum);
    }

    // -- retain_viable_ranked ---------------------------------------------

    #[test]
    fn retain_viable_ranked_drops_missing_when_viable_peer_exists() {
        let mut broken = ranked_at(1, 1, "broken");
        broken.score.missing_edges = 1;
        let viable = ranked_at(10_000, 1, "viable");
        let kept = retain_viable_ranked(vec![broken, viable]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].path.id, "viable");
    }

    #[test]
    fn retain_viable_ranked_keeps_all_when_none_viable_cold_cache() {
        let mut a = ranked_at(1, 1, "a");
        a.score.missing_edges = 1;
        let mut b = ranked_at(2, 1, "b");
        b.score.missing_edges = 2;
        let kept = retain_viable_ranked(vec![a, b]);
        assert_eq!(
            kept.len(),
            2,
            "cold-cache fallback keeps the set when every route has a missing leg"
        );
    }

    // -- unit fee schedule parsing ----------------------------------------

    #[test]
    fn hyperunit_native_fee_reads_bare_and_prefixed_fields() {
        use crate::services::action_providers::{hyperunit_native_fee, UnitFeeDirection};
        let bare = serde_json::json!({
            "ethereum": { "depositFee": 348377194481578_u64, "withdrawalFee": 50000650000_u64 }
        });
        assert_eq!(
            hyperunit_native_fee(&bare, "ethereum", UnitFeeDirection::Deposit),
            Some(U256::from(348377194481578_u64))
        );
        assert_eq!(
            hyperunit_native_fee(&bare, "ethereum", UnitFeeDirection::Withdrawal),
            Some(U256::from(50000650000_u64))
        );

        let prefixed = serde_json::json!({
            "bitcoin": { "bitcoin-depositFee": 2065, "bitcoin-withdrawalFee": 715 }
        });
        assert_eq!(
            hyperunit_native_fee(&prefixed, "bitcoin", UnitFeeDirection::Withdrawal),
            Some(U256::from(715_u64))
        );
        assert_eq!(
            hyperunit_native_fee(&prefixed, "solana", UnitFeeDirection::Deposit),
            None
        );
    }

    #[test]
    fn hyperunit_native_fee_truncates_float_encoded_values() {
        use crate::services::action_providers::{hyperunit_native_fee, UnitFeeDirection};
        let float_encoded = serde_json::json!({
            "plasma": { "depositFee": 4.624188358606018e+18_f64, "withdrawalFee": 500000 }
        });
        assert_eq!(
            hyperunit_native_fee(&float_encoded, "plasma", UnitFeeDirection::Withdrawal),
            Some(U256::from(500000_u64))
        );
        let deposit =
            hyperunit_native_fee(&float_encoded, "plasma", UnitFeeDirection::Deposit).unwrap();
        assert!(deposit > U256::from(4_000_000_000_000_000_000_u64));
    }
}
