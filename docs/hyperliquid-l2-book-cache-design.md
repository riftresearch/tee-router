# Hyperliquid L2 book cache + multi-resolution quote stitching

> Canonical description of how HL spot quoting sources and walks the order
> book: a cached, multi-resolution `l2Book` snapshot set per pair, and a
> "waterfall stitch" walk that prices large orders against coarser book
> views. §3 records empirically verified facts about the HL API that the
> code relies on but cannot express — re-verify those if HL changes
> behavior. Keep this updated as the system changes.
>
> All API behavior in §3 was verified live against `api.hyperliquid.xyz` on
> 2026-06-09 using the UBTC/USDC spot pair (`@142`).
>
> Last updated: 2026-06-09 (as-built).

---

## 1. TL;DR

HL's `l2Book` returns at most **20 levels per side**, full stop — no
parameter or endpoint returns more. At full price precision those 20 levels
span a razor-thin window (±0.06% of mid on UBTC/USDC, ≈ $213k of asks when
measured), so before this system any order larger than that failed quoting
with "book side could not absorb remaining input", even though ~65× more
liquidity is visible at coarser `nSigFigs` resolutions.

Two cooperating pieces fix that:

1. **Cache** (`crates/router-core/src/services/hyperliquid_books.rs`): a
   lazy, per-pair, in-memory cache of book snapshots fetched at every
   `nSigFigs` resolution in one parallel batch. On-demand with a 10s TTL,
   per-pair single-flight deduped, failure-memoized. Quotes landing within
   the TTL never touch HL at all.
2. **Stitch** (`stitch_book_levels` / `simulate_leg` in
   `action_providers.rs`): quote-time walk that consumes the finest view
   first, then continues into coarser views strictly above the finer view's
   coverage ceiling. HL rounds coarse bucket labels *away from the spread*
   (asks up, bids down — §3.4), so coarse-tail pricing is intrinsically
   pessimistic for the taker: the stitched quote can never over-promise
   relative to the snapshot.

Correctness framing: the cached book affects **quote accuracy only, never
settlement safety**. Execution still places a real slippage-bounded limit
order against the live book (`hl_leg_descriptor` pins per-leg bounds to the
simulated amounts). TTL is a load/freshness knob, not a safety parameter.

Validated live (live-local stack, 2026-06-09): a 100 BTC → Ethereum.USDC
quote priced ~$6M through the stitched book in one 6-tier batch; back-to-back
quotes hit the cache with zero HL requests; the maximum quotable size was
~782 BTC ≈ $42M — the actual visible-liquidity boundary of the book
(~150× the old fine-tier wall), failing beyond it with the classified
`INSUFFICIENT_LIQUIDITY` error.

---

## 2. Why this exists

Pre-cache behavior: every HL spot quote fetched a fresh full-precision
`l2Book` per leg (two for two-leg quotes) and walked it all-or-nothing.
Full precision is the *narrowest* possible price window — the worst
configuration for large orders — and every quote paid an HL round trip for
data that was ~identical to 5 seconds earlier. Large swaps failed even when
the book held the liquidity slightly beyond the visible window.

---

## 3. Verified HL API facts (do not re-litigate; re-verify if HL changes)

### 3.1 The 20-level cap is absolute

`l2Book` (REST and WS subscription alike) returns at most 20 *populated*
aggregated levels per side. Coarsening never yields a 21st level; at
`nSigFigs:2` the response was pinned at exactly 20 buckets per side and
still truncated (asks ended at +31% of mid). **There is no way to see the
entire book via the public API**; full L4 depth requires running an HL node.

### 3.2 The complete resolution ladder is exactly six settings

Probed exhaustively; everything else is rejected.

| Setting | Bucket step at ~$61k | Notes |
|---|---|---|
| `nSigFigs: null` | $1 | identical bytes to `nSigFigs:5` (HL prices are ≤5 sig figs anyway) — never sent |
| `nSigFigs: 5` | $1 | finest |
| `nSigFigs: 5, mantissa: 2` | $2 | |
| `nSigFigs: 5, mantissa: 5` | $5 | |
| `nSigFigs: 4` | $10 | |
| `nSigFigs: 3` | $100 | |
| `nSigFigs: 2` | $1,000 | coarsest |

Rejected (all **HTTP 500, body `null`**): `nSigFigs` 1 and 6; `mantissa`
1, 3, 4, 10; `mantissa` combined with `nSigFigs` 2/3/4; `mantissa` without
`nSigFigs`. The 1-2-5 step series only exists at the finest tier.

This is why `L2BookResolution` (`crates/hyperliquid-client/src/l2book.rs`)
is a closed 6-variant enum: invalid params are indistinguishable from a real
HL outage at the status-code level (500/`null`), so a typo'd passthrough
integer would masquerade as an outage and trigger pointless retries. The
HTTP source maps a 500/`null` on a param-bearing request to a non-retryable
config error ("the ladder no longer matches the HL API contract").

### 3.3 Measured depth per tier (UBTC/USDC, mid ≈ $61,669, 2026-06-09)

| Setting | Ask depth | Ask span | Bid depth | Bid span |
|---|---:|---|---:|---|
| `5` | 3.46 UBTC (~$213k) | +0.003%..+0.06% | 4.98 UBTC | −0.06% |
| `5, m2` | 4.76 | +0.08% | 4.85 | |
| `5, m5` | 12.53 | +0.2% | 9.91 | |
| `4` | 16.63 | +0.34% | 12.52 | |
| `3` | 47.85 | +3.1% | 63.60 | −3.2% |
| `2` | 224.94 (~$13.9M) | +31% | 797.14 | −32% |

Point-in-time numbers; the *structure* (each tier ~2–10× the previous) is
the durable fact. The pre-cache "book not deep enough" cliff for buys was
the 3.46-UBTC ask depth in row 1.

### 3.4 Bucket labels round away from the spread (the linchpin)

An ask bucket labeled `B` with step `S` contains orders priced in `(B−S, B]`;
a bid bucket labeled `B` contains `[B, B+S)`. The label is the
**taker-worst-case** price of its contents: walking coarse asks
overestimates cost; walking coarse bids underestimates proceeds. Coarse
views are therefore intrinsically conservative — pricing the coarse tail
needs no invented policy.

> **VERIFIED 2026-06-09 with concurrent fetches** (6 rounds, all tiers
> fetched simultaneously per round). Decisive observations that nearest-
> rounding cannot explain: bid 61,759 → `2`-label 61,000 (nearest would be
> 62,000 — floored); ask 61,751 → `5m5`-label 61,755 and `4`-label 61,760
> (nearest would be 61,750 in both — ceiled). Asks ceil, bids floor, in
> every round. Coarse labels are safe to price at directly. The golden
> vectors in `l2book.rs` pin exactly these observations.

### 3.5 Rate-limit envelope

HL's `/info` weight budget is ~1200/min per IP, shared with everything else
we do (order status polling, spotMeta, clearinghouse queries). Fetching all
6 tiers per pair per 10s tick = 36 req/min/pair. Lazy fetching (only pairs
actively being quoted) keeps this trivially affordable; a poll-everything
loop would not be.

---

## 4. Quote algorithm: the waterfall stitch

### 4.1 Why stitching is not concatenation

The tiers are overlapping re-aggregations of the *same* orders: a coarse
view's first bucket already contains everything the fine view showed. The
stitch must consume the fine view, then resume in the coarser view **only
above the fine view's coverage ceiling**, discarding the one bucket that
straddles the boundary (its size is part-already-counted and cannot be
split).

### 4.2 Coverage invariant

A view's 20 levels are the 20 *best populated* buckets, so a view with last
label `L` represents **all** resting orders priced at-or-better-than `L`.
After fully processing view `k`, `ceiling = last label of view k` is a
complete-knowledge frontier.

### 4.3 Algorithm (buy / walking asks; sells are the mirror with floors)

```text
stitch_walk_exact_in(views fine→coarse, usdc_budget):
    remaining  = usdc_budget
    base_total = 0
    ceiling    = 0

    for V in views:                       # [5, 5m2, 5m5, 4, 3, 2]
        S = bucket_step(V)
        # ask bucket labeled B covers (B−S, B]; keep only buckets entirely
        # above already-covered range; this drops the straddler by design
        usable = [lvl in V.asks where lvl.px − S >= ceiling]

        walk usable ascending with the existing level math
            (max_base_for_usdc_budget, base_to_usdc_raw_ceil, ...)
            consuming `remaining`, accumulating `base_total`
        if remaining == 0: break
        ceiling = max(ceiling, V.asks.last.px)   # from the UNFILTERED side

    if remaining > 0:
        fail "book side could not absorb remaining input"
    return floor_to_quantum(base_total, base_quantum)
```

Properties:

- **Small orders are unchanged**: they fill inside the finest view and the
  result is bit-identical to the pre-stitch walk (pinned by the legacy
  `simulate_hl_*` unit tests, which pass with unchanged expectations).
- **Per-seam loss**: discarding the straddling bucket hides at most one
  bucket of depth per seam (≤5 seams). Conservative in the safe direction.
- **Degenerate tiers are free**: if every tier returns the same levels
  (e.g. a one-level synthetic book), the boundary filter removes all
  coarser duplicates — no double-counting, identical to single-tier.
- **Exact-out** is the same waterfall walking until the target output is
  reached instead of until the budget is exhausted.
- **Two-leg** (`quote_two_leg`) needs no special logic: each leg walks its
  own stitched book, chained through USDC.
- **Slippage**: `hl_leg_descriptor` pins per-leg `min_amount_out` /
  `max_amount_in` to simulated amounts. The stitched simulation is
  pessimistic, so the real fill should beat it; bounds remain safe.
- **Quantization, U512 fixed-point math, and ceil-cost/floor-proceeds
  rounding are the pre-existing per-level walk math, untouched** — the
  stitch only changes *which levels* are walked, not how a level is priced.
  Quantum floor/ceil applies once at whole-walk boundaries, never per tier.

### 4.4 Max-pessimism guard (future knob, not built)

A fill that lands mostly in $100/$1000 buckets is safe but possibly so
pessimistic it is uncompetitive. If live data ever shows that hurting
flow, add a configurable cap: when more than X% of the input fills beyond
tier `4`, reject with the insufficient-liquidity error instead of quoting
a terrible price. Shipped without the guard (quote everything).

---

## 5. Cache design

### 5.1 Shape

```text
HlBookCache (crates/router-core/src/services/hyperliquid_books.rs)
├─ entries:  Arc<RwLock<HashMap<String, Arc<PairBooks>>>>   (per-instance)
├─ flights:  Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>    (per-pair single-flight guards)
└─ failures: Arc<RwLock<HashMap<String, FailedFetch>>>      (memoized refresh errors)

PairBooks {                          # one immutable snapshot set
    fetched_at: tokio::time::Instant,  # batch COMPLETION time
    tiers: Vec<TieredBook>,            # fine→coarse; tiers[0] always SigFigs5
}
```

(`dashmap`/`arc-swap`/`moka` deliberately not used: the repo's cache idiom
is `tokio::sync` RwLock/Mutex — see `provider_health.rs` and
`route_costs.rs` — and none of those crates are direct deps of
router-core.)

- **Immutable snapshots**: readers clone an `Arc<PairBooks>` out of a brief
  read lock and walk a frozen, consistent snapshot set; the refresher
  publishes a whole new `PairBooks`. No locks held during walks, no torn
  reads.
- **All tiers fetched in one parallel batch** per refresh so the views are
  mutually consistent (the stitch reasons across tiers; skewed snapshots
  would put the seams in the wrong place). Batch failure policy: a failed
  `SigFigs5` fails the refresh (the fine head is required for pricing);
  failed coarser tiers are dropped — the stitch degrades to less visible
  depth, never to wrong prices.

### 5.2 Refresh policy (resolves the popular-vs-cold-route tension)

Spawn-based stale-while-revalidate was considered and rejected: router-core
spawns no background tasks by convention (all refresh is pull-based, driven
by the worker binary's `JoinSet` loop — see `bin/router-server/src/worker.rs`).
v1 uses the repo's established double-checked single-flight pattern
(`RouteCostService::current_or_refresh_pricing_snapshot` is the template).
Cost: the one unlucky caller per pair per TTL window blocks on the batch
fetch (~300ms) instead of being served stale — still strictly better than
pre-cache, where *every* quote blocked on a fresh fetch.

1. **Lazy on-demand + TTL (10s, `HL_BOOK_CACHE_TTL`)**: nothing is fetched
   for a pair until a quote asks for it. Cold routes cost zero.
2. **Single-flight per pair**: concurrent quotes hitting a cold/stale key
   take a per-pair `Mutex` guard with a double-checked freshness re-read;
   exactly one batch fetch runs, the rest await it. Distinct pairs never
   serialize against each other.
3. **Failure memoization** (`HL_BOOK_FAILURE_COOLDOWN`, 2s): a failed batch
   records the error; flight-guard waiters and new arrivals get the
   memoized error instantly, so at most one upstream batch runs per
   cooldown window per pair during an outage. Success clears the marker.
   (Without this, queued waiters serially re-ran failing ~10s batches —
   strictly worse than pre-cache, where concurrent callers failed together
   at one HTTP timeout. Found by adversarial review.)
4. **`fetched_at` is batch *completion* time**, so every published entry
   has a full TTL of useful life regardless of batch wall time. (Stamping
   at batch start with TTL == client timeout let one persistently-slow
   tier publish born-stale entries and silently zero the hit rate.)
5. **Per-coarse-tier fetch budget** (`HL_COARSE_TIER_FETCH_BUDGET`, 3s):
   a coarse tier that fails slowly (blackholed connection riding the 10s
   client timeout) is cut off and dropped at the budget instead of
   dragging every refresh to the timeout wall. The fine `SigFigs5` head is
   exempt — it is required for pricing and keeps the full client timeout.

### 5.3 Multi-instance

This is read-only market data: two router instances independently caching
the same book is redundant fetching, not a correctness problem. **Do not**
coordinate via Redis/Postgres (that machinery is for execution-side
claim safety, a different problem). Per-instance memory is the right
answer unless instance count × pairs ever threatens the §3.5 budget.

### 5.4 Staleness bound

Worst case served: TTL + refresh latency ≈ 11s-old book. Quotes already
tolerate a 10-minute expiry against a live book, and execution re-validates
with slippage bounds, so an 11s-old *pessimistic* quote is well inside the
existing risk envelope.

---

## 6. Where it lives

- `crates/hyperliquid-client/src/l2book.rs` — `L2BookResolution` closed
  enum, label arithmetic (`round_up/down_to_resolution`, `prev/next_label`)
  with golden vectors from the live probes; `l2_book_at` on the clients.
- `crates/router-core/src/services/hyperliquid_books.rs` — `HlBookCache`,
  `PairBooks`, `TieredBook`, the `L2BookSource` abstraction (HTTP today,
  WS-feedable later).
- `crates/router-core/src/services/action_providers.rs` —
  `stitch_book_levels` + `simulate_leg` (the waterfall), per-level walk
  math unchanged from the pre-stitch implementation.
- `crates/devnet/src/hyperliquid_core.rs` + `hyperliquid_devnet/mod.rs` —
  the mock node honors the full ladder byte-for-byte (including 500/`null`
  rejects for invalid combos) and supports `set_book_levels` for
  multi-level book fixtures, so the stitch is testable on devnet.

## 7. Future work

- **Max-pessimism guard** (§4.4) — only if live data shows deep-tail
  pricing losing flow.
- **Worker-driven keep-warm**: expose a `refresh_due()` and let the worker
  binary tick it (route-costs pattern) so hot pairs never block at all.
  The 10s × ~300ms blocking budget doesn't justify it yet.
- **WebSocket local book**: the idiomatic exchange-connectivity upgrade —
  a WS-maintained local book (snapshot + deltas) behind the same
  `L2BookSource` seam. HL's WS `l2Book` feed is shaped identically
  (20 levels, takes `nSigFigs` per subscription) so it does **not** unlock
  more depth — only fresher data at lower request cost. Everything in
  §4–§5 is source-agnostic.
