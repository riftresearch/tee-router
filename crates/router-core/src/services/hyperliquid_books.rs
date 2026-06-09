//! Per-pair TTL cache of multi-resolution Hyperliquid `l2Book` snapshots.
//!
//! Takes the HL book fetch out of the quote hot path: each pair's books are
//! fetched lazily on first use as one parallel batch across the whole
//! [`L2BookResolution::LADDER_FINE_TO_COARSE`] ladder, then served from
//! memory for [`HL_BOOK_CACHE_TTL`]. Batching keeps the tiers mutually
//! consistent — the waterfall stitch reasons across tiers, and skewed
//! snapshots would put the seams in the wrong place (design doc §5.1).
//!
//! Concurrency model follows the house conventions: router-core spawns no
//! background tasks, so refresh is pull-based — the unlucky caller that finds
//! a stale entry runs the batch fetch while concurrent callers for the same
//! pair await a per-pair single-flight guard with a double-checked freshness
//! re-read (the [`RouteCostService::current_or_refresh_pricing_snapshot`]
//! pattern). Distinct pairs never serialize against each other.
//!
//! Correctness framing: this cache affects quote accuracy only, never
//! settlement safety — execution still places a slippage-bounded order
//! against the live book. See `docs/hyperliquid-l2-book-cache-design.md`.
//!
//! [`RouteCostService::current_or_refresh_pricing_snapshot`]: crate::services::route_costs::RouteCostService::current_or_refresh_pricing_snapshot

use crate::services::action_providers::ProviderResult;
use async_trait::async_trait;
use futures_util::future::join_all;
use hyperliquid_client::{HttpClient as HyperliquidHttpClient, L2BookResolution, L2BookSnapshot};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};
use tokio::time::Instant;
use tracing::warn;

/// How long a fetched snapshot set is served before the next caller refreshes
/// it. A load/freshness knob, not a safety parameter: quotes tolerate a
/// ~10s-old *pessimistic* book well inside the existing risk envelope
/// (design doc §5.4), and 6 tiers per pair per 10s stays trivially inside
/// HL's `/info` weight budget (§3.5).
pub(crate) const HL_BOOK_CACHE_TTL: Duration = Duration::from_secs(10);

/// How long a failed refresh is memoized. Waiters queued on the flight guard
/// (and new arrivals) share the cached error instead of serially re-running
/// ~10s failing batches against a downed venue — during an outage at most one
/// upstream batch runs per cooldown window per pair, matching the pre-cache
/// behavior where concurrent quotes all failed together at one HTTP timeout.
const HL_BOOK_FAILURE_COOLDOWN: Duration = Duration::from_secs(2);

/// Per-tier fetch budget for the coarser resolutions. A coarse tier that
/// fails *slowly* (e.g. a blackholed connection riding the full 10s client
/// timeout) would otherwise drag every refresh to the timeout wall; the fine
/// `SigFigs5` head is exempt — it is required for pricing and gets the full
/// client timeout.
const HL_COARSE_TIER_FETCH_BUDGET: Duration = Duration::from_secs(3);

/// One book snapshot tagged with the resolution it was fetched at.
#[derive(Debug)]
pub(crate) struct TieredBook {
    pub resolution: L2BookResolution,
    pub book: L2BookSnapshot,
}

/// An immutable, mutually consistent snapshot set for one pair — all tiers
/// fetched in a single parallel batch. Readers walk a frozen `Arc<PairBooks>`
/// with no locks held; refreshes publish a whole new value.
#[derive(Debug)]
pub(crate) struct PairBooks {
    /// Batch *completion* time on the tokio clock (so paused-clock tests
    /// work): every published entry gets a full TTL of useful life even when
    /// the batch itself was slow.
    pub fetched_at: Instant,
    /// Tiers ordered fine → coarse. `tiers[0]` is **always**
    /// [`L2BookResolution::SigFigs5`]: a failed coarser tier is dropped from
    /// the batch, but a failed `SigFigs5` fetch fails the whole refresh.
    pub tiers: Vec<TieredBook>,
}

/// Source of individual book snapshots. Abstracted so the cache mechanics are
/// unit-testable without a venue, and so a future WS-fed local book can slot
/// in behind the same read API (design doc §8).
#[async_trait]
pub(crate) trait L2BookSource: Send + Sync {
    async fn fetch(
        &self,
        coin: &str,
        resolution: L2BookResolution,
    ) -> Result<L2BookSnapshot, String>;
}

/// Production source: POSTs the resolution's `l2Book` request to HL `/info`.
pub(crate) struct HttpL2BookSource {
    http: HyperliquidHttpClient,
}

impl HttpL2BookSource {
    pub(crate) fn new(http: HyperliquidHttpClient) -> Self {
        Self { http }
    }
}

#[async_trait]
impl L2BookSource for HttpL2BookSource {
    async fn fetch(
        &self,
        coin: &str,
        resolution: L2BookResolution,
    ) -> Result<L2BookSnapshot, String> {
        self.http
            .post_json("/info", &resolution.request_body(coin))
            .await
            .map_err(|err| match &err {
                // HL rejects invalid resolution params with HTTP 500 and a
                // literal `null` body — indistinguishable from an outage at
                // the status-code level (design doc §3.2). The closed
                // L2BookResolution enum makes this unreachable from our
                // requests, so seeing it means the HL API contract changed:
                // surface it as a config error, not venue weather.
                hyperliquid_client::Error::HttpStatus { status: 500, body }
                    if body.trim() == "null" =>
                {
                    format!(
                        "hyperliquid l2Book {coin} at {resolution:?}: HL rejected the resolution \
                         params (HTTP 500 `null`) — non-retryable; the L2BookResolution ladder \
                         no longer matches the HL API contract"
                    )
                }
                _ => format!("fetch hyperliquid l2Book {coin} at {resolution:?}: {err}"),
            })
    }
}

/// A memoized refresh failure: the error served to flight-guard waiters and
/// new arrivals until the cooldown lapses.
#[derive(Debug)]
struct FailedFetch {
    at: Instant,
    error: String,
}

/// Lazy, per-pair, single-flight-deduped cache of [`PairBooks`].
#[derive(Clone)]
pub(crate) struct HlBookCache {
    source: Arc<dyn L2BookSource>,
    /// Published snapshots, keyed by pair coin (e.g. `"UBTC/USDC"`).
    entries: Arc<RwLock<HashMap<String, Arc<PairBooks>>>>,
    /// Per-pair single-flight guards. The outer map lock is only held long
    /// enough to clone a guard out — never across an await of the guard
    /// itself — so a slow refresh of one pair cannot stall another.
    flights: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
    /// Memoized refresh failures, served for [`HL_BOOK_FAILURE_COOLDOWN`] so
    /// an HL outage degrades to fast shared errors instead of every caller
    /// serially re-running a failing batch behind the flight guard.
    failures: Arc<RwLock<HashMap<String, FailedFetch>>>,
    ttl: Duration,
}

impl HlBookCache {
    pub(crate) fn new(source: Arc<dyn L2BookSource>, ttl: Duration) -> Self {
        Self {
            source,
            entries: Arc::new(RwLock::new(HashMap::new())),
            flights: Arc::new(Mutex::new(HashMap::new())),
            failures: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// Return a fresh snapshot set for `coin`, batch-fetching the full
    /// resolution ladder if the cached entry is missing or older than the
    /// TTL. Concurrent callers on a cold/stale pair are deduped to exactly
    /// one upstream batch via the per-pair flight guard; a failed batch is
    /// memoized for [`HL_BOOK_FAILURE_COOLDOWN`] and served to waiters.
    pub(crate) async fn pair_books(&self, coin: &str) -> ProviderResult<Arc<PairBooks>> {
        // Fast path: serve a fresh published snapshot from a brief read lock.
        if let Some(fresh) = self.fresh_entry(coin).await {
            return Ok(fresh);
        }
        if let Some(error) = self.recent_failure(coin).await {
            return Err(error);
        }
        let flight = {
            let mut flights = self.flights.lock().await;
            Arc::clone(flights.entry(coin.to_string()).or_default())
        };
        let _guard = flight.lock().await;
        // Double-checked re-read: the previous flight holder refreshed —
        // or failed — while this caller was waiting on the guard.
        if let Some(fresh) = self.fresh_entry(coin).await {
            return Ok(fresh);
        }
        if let Some(error) = self.recent_failure(coin).await {
            return Err(error);
        }
        match self.fetch_all_tiers(coin).await {
            Ok(books) => {
                let books = Arc::new(books);
                self.failures.write().await.remove(coin);
                self.entries
                    .write()
                    .await
                    .insert(coin.to_string(), Arc::clone(&books));
                Ok(books)
            }
            Err(error) => {
                self.failures.write().await.insert(
                    coin.to_string(),
                    FailedFetch {
                        at: Instant::now(),
                        error: error.clone(),
                    },
                );
                Err(error)
            }
        }
    }

    /// The published entry for `coin`, if it exists and is younger than the
    /// TTL.
    async fn fresh_entry(&self, coin: &str) -> Option<Arc<PairBooks>> {
        self.entries
            .read()
            .await
            .get(coin)
            .filter(|books| books.fetched_at.elapsed() < self.ttl)
            .cloned()
    }

    /// The memoized refresh error for `coin`, if one happened within the
    /// cooldown.
    async fn recent_failure(&self, coin: &str) -> Option<String> {
        self.failures
            .read()
            .await
            .get(coin)
            .filter(|failure| failure.at.elapsed() < HL_BOOK_FAILURE_COOLDOWN)
            .map(|failure| failure.error.clone())
    }

    /// Fetch every ladder resolution for `coin` concurrently. A failed
    /// `SigFigs5` fetch fails the whole batch — the fine head is required
    /// for pricing. Failed coarser tiers are dropped (and budgeted to
    /// [`HL_COARSE_TIER_FETCH_BUDGET`] so a slow failure cannot drag the
    /// batch to the client timeout): per design doc §5.1 the stitch then
    /// degrades to less visible depth, never wrong prices.
    async fn fetch_all_tiers(&self, coin: &str) -> ProviderResult<PairBooks> {
        let fetches = L2BookResolution::LADDER_FINE_TO_COARSE.map(|resolution| async move {
            let result = if resolution == L2BookResolution::SigFigs5 {
                self.source.fetch(coin, resolution).await
            } else {
                match tokio::time::timeout(
                    HL_COARSE_TIER_FETCH_BUDGET,
                    self.source.fetch(coin, resolution),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(format!(
                        "coarse l2Book tier {resolution:?} exceeded its \
                         {HL_COARSE_TIER_FETCH_BUDGET:?} fetch budget"
                    )),
                }
            };
            (resolution, result)
        });
        let mut tiers = Vec::with_capacity(L2BookResolution::LADDER_FINE_TO_COARSE.len());
        for (resolution, result) in join_all(fetches).await {
            match result {
                Ok(book) => tiers.push(TieredBook { resolution, book }),
                Err(err) if resolution == L2BookResolution::SigFigs5 => return Err(err),
                Err(err) => {
                    warn!(coin, ?resolution, %err, "dropping failed coarse l2Book tier");
                }
            }
        }
        // Stamped at batch *completion*: a slow batch must still publish an
        // entry with a full TTL of useful life, or a persistently slow tier
        // would publish born-stale entries and silently zero the hit rate.
        Ok(PairBooks {
            fetched_at: Instant::now(),
            tiers,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Position of `resolution` in the fine→coarse ladder, used as the
    /// counter/override index ([`L2BookResolution`] does not impl `Hash`).
    fn ladder_index(resolution: L2BookResolution) -> usize {
        L2BookResolution::LADDER_FINE_TO_COARSE
            .iter()
            .position(|candidate| *candidate == resolution)
            .expect("resolution is in the ladder")
    }

    fn empty_snapshot(coin: &str) -> L2BookSnapshot {
        L2BookSnapshot {
            coin: coin.to_string(),
            levels: vec![Vec::new(), Vec::new()],
            time: 0,
        }
    }

    /// Pure-logic concurrency test double for the cache mechanics (call
    /// counting, artificial latency, per-resolution failure injection).
    /// Venue-shape parity is covered by the devnet node suite, not here.
    struct CountingSource {
        calls: [AtomicUsize; 6],
        delay: Duration,
        /// Per-resolution delay overrides (else `delay`), by ladder position.
        delays: [Option<Duration>; 6],
        /// Per-resolution error overrides, indexed by ladder position.
        /// Mutable so tests can clear an injected outage mid-test.
        errors: std::sync::Mutex<[Option<String>; 6]>,
    }

    impl CountingSource {
        fn new() -> Self {
            Self {
                calls: std::array::from_fn(|_| AtomicUsize::new(0)),
                delay: Duration::ZERO,
                delays: std::array::from_fn(|_| None),
                errors: std::sync::Mutex::new(std::array::from_fn(|_| None)),
            }
        }

        fn with_delay(delay: Duration) -> Self {
            Self {
                delay,
                ..Self::new()
            }
        }

        fn with_error(resolution: L2BookResolution, message: &str) -> Self {
            let source = Self::new();
            source.set_error(resolution, Some(message));
            source
        }

        fn with_tier_delay(resolution: L2BookResolution, delay: Duration) -> Self {
            let mut source = Self::new();
            source.delays[ladder_index(resolution)] = Some(delay);
            source
        }

        fn set_error(&self, resolution: L2BookResolution, message: Option<&str>) {
            self.errors.lock().expect("errors lock")[ladder_index(resolution)] =
                message.map(str::to_string);
        }

        fn calls_for(&self, resolution: L2BookResolution) -> usize {
            self.calls[ladder_index(resolution)].load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl L2BookSource for CountingSource {
        async fn fetch(
            &self,
            coin: &str,
            resolution: L2BookResolution,
        ) -> Result<L2BookSnapshot, String> {
            self.calls[ladder_index(resolution)].fetch_add(1, Ordering::SeqCst);
            let delay = self.delays[ladder_index(resolution)].unwrap_or(self.delay);
            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }
            let error = self.errors.lock().expect("errors lock")[ladder_index(resolution)].clone();
            match error {
                Some(message) => Err(message),
                None => Ok(empty_snapshot(coin)),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn single_flight_dedupes_concurrent_fetches() {
        let source = Arc::new(CountingSource::with_delay(Duration::from_millis(200)));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        let books = join_all((0..8).map(|_| cache.pair_books("UBTC/USDC")))
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("all callers succeed");

        // Exactly one upstream batch ran: one fetch per resolution.
        for resolution in L2BookResolution::LADDER_FINE_TO_COARSE {
            assert_eq!(source.calls_for(resolution), 1, "{resolution:?}");
        }
        // Every caller got the same published snapshot allocation.
        for other in &books[1..] {
            assert!(Arc::ptr_eq(&books[0], other));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn ttl_expiry_refetches() {
        let source = Arc::new(CountingSource::new());
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        cache.pair_books("UBTC/USDC").await.expect("cold fetch");
        // Within the TTL the cached snapshot is served — no new batch.
        tokio::time::advance(HL_BOOK_CACHE_TTL / 2).await;
        cache.pair_books("UBTC/USDC").await.expect("warm fetch");
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 1);

        // Past the TTL the next caller refreshes: a second batch.
        tokio::time::advance(HL_BOOK_CACHE_TTL).await;
        cache.pair_books("UBTC/USDC").await.expect("stale fetch");
        for resolution in L2BookResolution::LADDER_FINE_TO_COARSE {
            assert_eq!(source.calls_for(resolution), 2, "{resolution:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn distinct_pairs_do_not_serialize() {
        let delay = Duration::from_secs(1);
        let source = Arc::new(CountingSource::with_delay(delay));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        let started = Instant::now();
        let (ubtc, ueth) =
            tokio::join!(cache.pair_books("UBTC/USDC"), cache.pair_books("UETH/USDC"));
        ubtc.expect("UBTC fetch");
        ueth.expect("UETH fetch");

        // Both pairs' delayed batches ran concurrently on the paused clock:
        // total elapsed is one delay, not two. Cross-pair serialization
        // (a shared flight guard) would make this 2× the delay.
        assert_eq!(started.elapsed(), delay);
    }

    #[tokio::test]
    async fn coarse_tier_failure_degrades() {
        let source = Arc::new(CountingSource::with_error(
            L2BookResolution::SigFigs3,
            "sigfigs3 down",
        ));
        let cache = HlBookCache::new(source as Arc<dyn L2BookSource>, HL_BOOK_CACHE_TTL);

        let books = cache.pair_books("UBTC/USDC").await.expect("degraded batch");
        assert_eq!(books.tiers.len(), 5);
        assert_eq!(books.tiers[0].resolution, L2BookResolution::SigFigs5);
        assert!(books
            .tiers
            .iter()
            .all(|tier| tier.resolution != L2BookResolution::SigFigs3));
    }

    #[tokio::test]
    async fn fine_tier_failure_errors() {
        let source = Arc::new(CountingSource::with_error(
            L2BookResolution::SigFigs5,
            "sigfigs5 down",
        ));
        let cache = HlBookCache::new(source as Arc<dyn L2BookSource>, HL_BOOK_CACHE_TTL);

        let err = cache
            .pair_books("UBTC/USDC")
            .await
            .expect_err("fine head required");
        assert!(err.contains("sigfigs5 down"), "{err}");
    }

    #[tokio::test(start_paused = true)]
    async fn failure_is_memoized_for_waiters_instead_of_serial_refetches() {
        // An outage where fetches fail *slowly* (1s here, the 10s client
        // timeout in production). Without failure memoization the 8 queued
        // callers would serially re-run the failing batch behind the flight
        // guard — 8s of stacked latency; with it, the first flight's error is
        // shared and exactly one upstream batch runs.
        let source = Arc::new(CountingSource::with_delay(Duration::from_secs(1)));
        source.set_error(L2BookResolution::SigFigs5, Some("hl unreachable"));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        let started = Instant::now();
        let results = join_all((0..8).map(|_| cache.pair_books("UBTC/USDC"))).await;
        for result in &results {
            let err = result.as_ref().expect_err("outage propagates");
            assert!(err.contains("hl unreachable"), "{err}");
        }
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 1);
        // All callers resolved together at the first batch's failure, not
        // serially at 1s, 2s, ..., 8s.
        assert_eq!(started.elapsed(), Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn failure_cooldown_expires_then_success_clears_the_marker() {
        let source = Arc::new(CountingSource::with_error(
            L2BookResolution::SigFigs5,
            "hl unreachable",
        ));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        cache
            .pair_books("UBTC/USDC")
            .await
            .expect_err("initial outage");
        // Within the cooldown the memoized error is served without a fetch.
        tokio::time::advance(Duration::from_millis(500)).await;
        cache
            .pair_books("UBTC/USDC")
            .await
            .expect_err("memoized outage");
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 1);

        // Past the cooldown, the venue has recovered: the retry succeeds and
        // clears the failure marker, so the next read is a plain cache hit.
        tokio::time::advance(Duration::from_secs(3)).await;
        source.set_error(L2BookResolution::SigFigs5, None);
        cache.pair_books("UBTC/USDC").await.expect("recovered");
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 2);
        cache.pair_books("UBTC/USDC").await.expect("cache hit");
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn fetched_at_reflects_batch_completion_not_start() {
        // A 5s-slow batch (every tier delayed) must still publish an entry
        // with a full TTL of useful life: 8s after publish is a cache hit.
        // Stamping at batch *start* would make the entry born half-stale.
        let source = Arc::new(CountingSource::with_delay(Duration::from_secs(5)));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        cache.pair_books("UBTC/USDC").await.expect("slow cold fetch");
        tokio::time::advance(Duration::from_secs(8)).await;
        cache.pair_books("UBTC/USDC").await.expect("warm fetch");
        assert_eq!(source.calls_for(L2BookResolution::SigFigs5), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn slow_coarse_tier_is_budgeted_and_dropped() {
        // A blackholed coarse tier rides the client timeout (long past the
        // per-tier budget); the batch must complete at the budget with the
        // tier dropped instead of dragging every refresh to the timeout wall.
        let source = Arc::new(CountingSource::with_tier_delay(
            L2BookResolution::SigFigs2,
            Duration::from_secs(60),
        ));
        let cache = HlBookCache::new(
            Arc::clone(&source) as Arc<dyn L2BookSource>,
            HL_BOOK_CACHE_TTL,
        );

        let started = Instant::now();
        let books = cache.pair_books("UBTC/USDC").await.expect("degraded batch");
        assert_eq!(started.elapsed(), HL_COARSE_TIER_FETCH_BUDGET);
        assert_eq!(books.tiers.len(), 5);
        assert!(books
            .tiers
            .iter()
            .all(|tier| tier.resolution != L2BookResolution::SigFigs2));
    }
}
