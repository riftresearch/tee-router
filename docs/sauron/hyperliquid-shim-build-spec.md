# Hyperliquid Shim Indexer — Build Spec (v1)

This is the concrete near-term build for the Hyperliquid data layer. It implements the uniform indexer API defined in `build-spec.md` and consumed by Sauron's HL venue observers. Upstream: REST `/info` polling only. Downstream: REST query + WS push subscription. Designed to be swapped for the self-hosted-node indexer (`hyperliquid-endgame-indexer-plan.md`) without API change.

## Goals

1. Expose the uniform indexer API to Sauron — `GET /transfers`, `GET /orders`, `POST /prune`, `WS /subscribe` — using HL event types.
2. Source upstream data exclusively from HL's public REST `/info` endpoint. No WebSocket to HL in v1.
3. Push events to active WS subscribers as soon as they're ingested. WS push is internal (indexer → Sauron); not a passthrough of HL WS.
4. Adapt upstream polling cadence based on subscriber demand (drive budget toward users that actually matter).
5. Stay under HL's 1200-weight-per-minute REST budget per IP.
6. Ship in ~4-5 days. ~1,000 LOC of Rust.

## Non-goals

- Hyperliquid WebSocket consumption (deferred to never — endgame uses node disk-stream).
- Cross-chain Bridge2 correlation (lives in Sauron's HL Bridge venue observer, joining this indexer with the Arbitrum EVM indexer).
- TWAP slice handling (`userTwapSliceFills`) — defer until TWAP execution is supported.
- Vault, subaccount, liquidation, and similar ledger variants are gracefully
  skipped via the typed `Unsupported` decoder fallthrough. Re-add typed support
  when route coverage demands it.
- Backfill beyond HL's 10,000-fill-per-user cap (operational acceptance per `hyperliquid-api-reference.md`).

## API contract (downstream — Sauron-facing)

### `GET /transfers`

```
GET /transfers
  ?user=<addr>
  &from_time=<ms epoch>     # required for streaming forward; optional for "recent"
  &limit=<n>                # default 500, max 2000
  &cursor=<opaque>          # optional, for pagination

Response:
{
  "events": [HlTransferEvent...],
  "next_cursor": <opaque>,
  "has_more": bool
}
```

Returns the unified value-flow feed: fills + non-funding ledger updates + funding payments for the user, ordered by `time_ms` ascending. `HlTransferEvent` shape per `build-spec.md` §3.

### `GET /orders`

```
GET /orders
  ?user=<addr>
  &from_time=<ms epoch>
  &limit=<n>
  &cursor=<opaque>

Response:
{
  "orders": [HlOrderEvent...],
  "next_cursor": <opaque>,
  "has_more": bool
}
```

Returns order-status transitions for the user, sourced from `orderStatus` polling. `HlOrderEvent` schema:

```rust
struct HlOrderEvent {
    user: Address,
    oid: u64,
    cloid: Option<String>,
    coin: String,
    side: char,        // 'A' | 'B'
    limit_px: String,
    sz: String,
    orig_sz: String,
    status: HlOrderStatus,     // Open | Filled | Canceled | Triggered | Rejected | MarginCanceled | ...
    status_timestamp_ms: i64,
    observed_at_ms: i64,
}
```

### `POST /orders/watch`

```
POST /orders/watch
{
  "user": "0x...",
  "oid": 123456
}
```

Registers a pending oid for hot-cadence `orderStatus` polling. The endpoint is
idempotent and exists for Sauron's by-id lookup access pattern: Sauron knows the
oid it is waiting on, but the shim needs an explicit registration to spend the
upstream `orderStatus` polling budget. Delivery still happens through
`GET /orders` replay and `WS /subscribe` order events.

```
DELETE /orders/watch/<oid>
GET /orders/watch
```

`DELETE` deregisters an oid idempotently. `GET` is a debug endpoint that lists
currently watched oids.

### `POST /prune`

```
POST /prune
{
  "relevant_users": ["0x..."],
  "before_time_ms": <i64>
}
```

Async pruning instruction. Indexer drops `hl_transfers` and `hl_orders` rows for users NOT in `relevant_users` AND with `time_ms < before_time_ms`. Returns immediately; pruning runs in background.

### `WS /subscribe`

```
Client → Server:
{
  "action": "subscribe",
  "filter": {
    "users": ["0x..."],
    "kinds": ["transfers", "orders"]   // subset; defaults to both
  }
}

Server → Client (one per matched event):
{
  "kind": "transfer" | "order",
  "event": <HlTransferEvent | HlOrderEvent>
}

Client → Server:
{
  "action": "unsubscribe",
  "subscription_id": "..."
}
```

Subscriptions are session-scoped (closed on WS disconnect). Each event matching the active filter is pushed exactly once. Backpressure: if the subscriber lags by > N events, the slow subscriber is disconnected (Sauron handles reconnect + replay via REST query).

## API contract (upstream — HL-facing)

The shim only calls these HL endpoints. No WS.

| HL endpoint | Use | Weight | Cadence (steady-state) |
|---|---|---|---|
| `POST /info {type:"userFills",user,aggregateByTime:false}` | recent fills (2000 cap, no time params) | 20 + 1/20 items | unused steady; used on watch registration for warmup |
| `POST /info {type:"userFillsByTime",user,startTime,endTime?}` | fills since cursor | 20 + 1/20 items | adaptive per-user (5s hot / 30s warm / 120s cold) |
| `POST /info {type:"userNonFundingLedgerUpdates",user,startTime,endTime?}` | ledger updates since cursor | 20 + 1/20 items | same adaptive cadence as fills |
| `POST /info {type:"userFunding",user,startTime,endTime?}` | funding payments since cursor | 20 + 1/20 items | once per hour per user (funding accrues hourly) |
| `POST /info {type:"orderStatus",user,oid}` | pending oid status | 2 | 2s per pending oid (cheap; fast detection) |
| `POST /info {type:"meta"}` | perp universe metadata | 20 | once on boot + every 10 min |
| `POST /info {type:"spotMeta"}` | spot universe metadata | 20 | once on boot + every 10 min |
| `POST /info {type:"userRateLimit",user}` | live REST budget check | 2 | every 60s for any one watched user |

### Adaptive cadence

Each watched user is in one of these cadence buckets, dynamically assigned:

| Bucket | Trigger | Fills/Ledger cadence | Funding cadence |
|---|---|---|---|
| Hot | Has ≥1 active WS subscriber OR has pending oid receipt-watch | **5s** | 60s (still hourly events; 60s cap on latency) |
| Warm | Watch registered but no active subscriber, last activity < 5min ago | 30s | 5min |
| Cold | Watch registered, last activity > 5min ago | 120s | 30min |

Reassignment runs every 10s: walk active subscribers, walk recent-activity timestamps, reassign buckets.

### Weight budget projection

Per user per minute:

- Hot: 2 endpoints × 12 polls × 20 weight = **480 weight/min/user**
- Warm: 2 endpoints × 2 polls × 20 weight = **80 weight/min/user**
- Cold: 2 endpoints × 0.5 polls × 20 weight = **20 weight/min/user**

Plus orderStatus polling for pending oids: 2 weight/oid × 30 polls/min = **60 weight/min/oid**.

At 1200/min IP cap, comfortable concurrent capacities:
- **2 hot users + 4 warm users + 10 cold users + 5 pending oids**: 960 + 320 + 200 + 300 = … wait, let me reconsider:
  - 2 × 480 = 960
  - 4 × 80 = 320 → over budget already

Realistic: at any moment, expect ~1-3 hot users (orders actively executing on HL), ~5-10 warm, the rest cold. Plus a few pending oids. Tune cadences down if we approach the cap; the `userRateLimit` poll gives live headroom.

If we exceed the per-IP cap regularly, escalate to multi-IP egress (Phase 5b in `build-spec.md`).

## Implementation outline

### Module layout

```
hl-shim-indexer/
  src/
    main.rs              # binary entry, axum app, config
    api/
      mod.rs
      rest.rs            # GET /transfers, /orders, POST /prune
      ws.rs              # WS /subscribe handler
    poller/
      mod.rs             # adaptive cadence scheduler
      hl_client.rs       # POST /info wrapper, weight accounting
      cursor.rs          # per-user per-endpoint cursor management
    ingest/
      mod.rs
      schema.rs          # HlTransferEvent + HlOrderEvent + HlTransferKind
      decode.rs          # HL native shapes → unified events
      dedup.rs           # canonical-key dedup before insert
      pubsub.rs          # tokio::broadcast for downstream WS push
    storage/
      mod.rs
      schema.sql         # migrations
      transfers.rs       # hl_transfers table CRUD
      orders.rs          # hl_orders table CRUD
      prune.rs           # async prune job
    metadata/
      meta.rs            # perp + spot universe loader
      classifier.rs      # coin → Spot|Perp|NA discriminator
    config.rs
    error.rs
  Cargo.toml
  migrations/
    20260512_000_initial.sql
```

Target LOC: ~1,000-1,100 total.

### Polling loop (core)

```rust
async fn poll_loop(scheduler: Arc<Scheduler>, hl: HlClient, pubsub: PubSub, db: Db) {
    loop {
        let next = scheduler.next_due().await;  // returns (user, endpoint, deadline)
        sleep_until(next.deadline).await;
        match next.endpoint {
            Endpoint::Fills => fetch_fills(&hl, &db, &pubsub, next.user).await,
            Endpoint::Ledger => fetch_ledger(&hl, &db, &pubsub, next.user).await,
            Endpoint::Funding => fetch_funding(&hl, &db, &pubsub, next.user).await,
            Endpoint::OrderStatus(oid) => fetch_order_status(&hl, &db, &pubsub, next.user, oid).await,
        }
        scheduler.reschedule(next).await;
    }
}
```

Concurrent: spawn N worker tasks reading from the scheduler. N tuned by available weight budget (start with 4).

### Cursor management

Per user per endpoint: `last_seen_time_ms` stored in Postgres. On each poll:

```rust
let cursor = db.get_cursor(user, endpoint).await.unwrap_or(0);
let safety_window_ms = 5_000;
let start = (cursor - safety_window_ms).max(0);

let response = hl.post_info(Request {
    r#type: endpoint.as_str(),
    user,
    start_time: start,
    end_time: None,
}).await?;

let events = decode_response(response);
let new_events = dedup_and_filter(events, cursor, db).await;

for event in &new_events {
    db.insert_event(event).await?;
    pubsub.broadcast(event.clone());
}

let new_cursor = new_events.iter().map(|e| e.time_ms).max().unwrap_or(cursor);
db.set_cursor(user, endpoint, new_cursor).await?;
```

Safety window prevents missing events at clock boundaries. Dedup against Postgres unique constraints on canonical keys.

### Postgres schema

```sql
CREATE TABLE hl_transfers (
    user_addr        BYTEA NOT NULL,
    time_ms          BIGINT NOT NULL,
    kind             TEXT NOT NULL,            -- discriminator: 'fill' | 'funding' | 'deposit' | 'withdraw' | ...
    canonical_key    TEXT NOT NULL,            -- (user, tid) for fills; (user, nonce) for withdraws; etc.
    asset            TEXT NOT NULL,
    market           TEXT,                     -- 'spot' | 'perp' | NULL
    amount_delta     NUMERIC(78, 18) NOT NULL,
    fee              NUMERIC(78, 18),
    fee_token        TEXT,
    hash             BYTEA NOT NULL,
    metadata         JSONB NOT NULL,           -- kind-specific fields
    observed_at_ms   BIGINT NOT NULL,
    UNIQUE (canonical_key)
) PARTITION BY HASH (user_addr);

CREATE INDEX idx_hl_transfers_user_time ON hl_transfers (user_addr, time_ms);
CREATE INDEX idx_hl_transfers_oid ON hl_transfers ((metadata->>'oid')) WHERE kind = 'fill';

-- 16 partitions for hash distribution
CREATE TABLE hl_transfers_0 PARTITION OF hl_transfers FOR VALUES WITH (modulus 16, remainder 0);
-- ... through 15

CREATE TABLE hl_orders (
    user_addr             BYTEA NOT NULL,
    oid                   BIGINT NOT NULL,
    cloid                 TEXT,
    coin                  TEXT NOT NULL,
    side                  CHAR(1) NOT NULL,
    limit_px              TEXT NOT NULL,
    sz                    TEXT NOT NULL,
    orig_sz               TEXT NOT NULL,
    status                TEXT NOT NULL,
    status_timestamp_ms   BIGINT NOT NULL,
    observed_at_ms        BIGINT NOT NULL,
    PRIMARY KEY (user_addr, oid)
);

CREATE INDEX idx_hl_orders_user_time ON hl_orders (user_addr, status_timestamp_ms);

CREATE TABLE hl_cursors (
    user_addr  BYTEA NOT NULL,
    endpoint   TEXT NOT NULL,            -- 'fills' | 'ledger' | 'funding' | 'orderstatus:<oid>'
    cursor_ms  BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_addr, endpoint)
);
```

### Subscription-driven cadence integration

```rust
struct Scheduler {
    users: DashMap<Address, UserState>,
    subscribers: DashMap<SubscriptionId, ActiveSubscription>,
}

struct UserState {
    bucket: Bucket,                       // Hot | Warm | Cold
    last_activity_ms: AtomicI64,
    next_due: AtomicI64,
    pending_oids: DashSet<u64>,
}

impl Scheduler {
    fn on_subscribe(&self, sub_id: SubscriptionId, filter: Filter) {
        for user in &filter.users {
            self.users.entry(*user)
                .or_insert_with(default_user_state)
                .bucket
                .store(Bucket::Hot);
        }
        self.subscribers.insert(sub_id, ActiveSubscription { filter });
    }

    fn on_unsubscribe(&self, sub_id: SubscriptionId) {
        let sub = self.subscribers.remove(&sub_id);
        // recompute buckets for any users in the removed filter — if no other subscribers, demote
        ...
    }

    async fn rebucket_loop(&self) {
        loop {
            sleep(Duration::from_secs(10)).await;
            for user in self.users.iter() {
                let has_subscriber = self.subscribers.iter()
                    .any(|s| s.value().filter.users.contains(user.key()));
                let recent = (now_ms() - user.last_activity_ms.load(Ordering::Relaxed)) < 5 * 60 * 1000;
                user.bucket.store(match (has_subscriber, recent) {
                    (true, _) => Bucket::Hot,
                    (false, true) => Bucket::Warm,
                    (false, false) => Bucket::Cold,
                });
            }
        }
    }
}
```

### Dedup canonical keys (per `hyperliquid-api-reference.md`)

```rust
fn canonical_key(event: &HlTransferEvent) -> String {
    match &event.kind {
        Fill { tid, .. } => format!("fill:{:?}:{}", event.user, tid),
        Funding { .. } => format!("funding:{:?}:{}:{}", event.user, event.asset, event.time_ms),
        Deposit => format!("deposit:{:?}:{}:{}", event.user, hex(&event.hash), event.amount_delta),
        Withdraw { nonce } => format!("withdraw:{:?}:{}", event.user, nonce),
        SpotTransfer { nonce, .. } => format!("spotxfer:{:?}:{}", event.user, nonce),
        _ => format!("misc:{:?}:{}:{}:{:?}", event.user, event.time_ms, hex(&event.hash), event.kind),
    }
}
```

Use `INSERT ... ON CONFLICT (canonical_key) DO NOTHING` for idempotent ingestion.

**Correction from the original spec:** Hyperliquid emits sizes, fees, funding,
and ledger amounts as decimal strings (`"0.001"`, `"-0.0123"`, etc.). The shim
API preserves these as decimal strings, and Postgres stores them as
`NUMERIC(78, 18)`. We intentionally do not force scaled integers because doing
so requires per-asset decimal lookup and introduces avoidable precision and
correlation bugs.

## Configuration

```toml
[hl]
info_url = "https://api.hyperliquid.xyz/info"
testnet_info_url = "https://api.hyperliquid-testnet.xyz/info"
chain = "mainnet"   # | "testnet"

[poller]
hot_cadence_ms = 5_000
warm_cadence_ms = 30_000
cold_cadence_ms = 120_000
funding_cadence_hot_ms = 60_000
funding_cadence_warm_ms = 300_000
funding_cadence_cold_ms = 1_800_000
order_status_cadence_ms = 2_000
weight_budget_per_min = 1100   # keep 100 weight headroom
worker_count = 4

[server]
bind = "0.0.0.0:8080"
max_subscriber_lag = 10_000

[postgres]
url = "postgresql://..."
max_connections = 10
```

## Operational considerations

- **Healthcheck:** `GET /healthz` returns 200 if poll loop is making progress (last successful poll within 60s) and Postgres is reachable.
- **Metrics:** `hl_shim_hl_request_total{endpoint,status}`, `hl_shim_hl_weight_used`, `hl_shim_active_subscriptions`, `hl_shim_user_bucket{bucket}`, `hl_shim_db_writes_total{kind}`, `hl_shim_event_emit_lag_seconds`.
- **Logs:** structured JSON; per-poll log includes user, endpoint, events_fetched, weight_used.
- **Restart safety:** all state in Postgres. Restart loses in-memory subscriber set; subscribers reconnect via Sauron's retry logic.
- **Rate-limit response handling:** on HL 429 or `userRateLimit` near cap, halve all bucket cadences for 1 minute (back off), log warning, alert if persistent.

## Out of scope for v1

- Multi-IP egress pool (Phase 5b).
- WS connection to HL (Phase 5c / endgame).
- TWAP slice fills via `userTwapSliceFills`.
- Typed ingestion of vault, subaccount, liquidation, rewards, staking, and
  other ledger variants beyond the v1 executor routes. The decoder falls back
  to `Unsupported`, emits `hl_shim_unsupported_ledger_delta_total{type_name}`,
  logs a warning, and skips the row until route coverage demands typed support.
- `nonUserCancel` capture via the `user` channel — defer (we can backfill cancellation reasons via `orderStatus` polling for now).
- Spot balance snapshot reconciliation via `spotState`.
- mark price / oracle / funding context via `activeAssetData` / `activeAssetCtx`.

## Definition of done

1. Crate `hl-shim-indexer` builds and runs locally with Postgres.
2. Smoke test against HL testnet: register a watch for a known testnet account; observe `userFills` + `userNonFundingLedgerUpdates` ingested; query via `GET /transfers` and verify ordering + pagination; subscribe via WS and verify push.
3. Stress test: 20 hot users + 5 pending oids for 10 minutes against testnet without exceeding weight budget.
4. Integration test from a stubbed Sauron consumer: subscribe to events for 5 users; ingest test data; confirm consumer receives expected push events.
5. Health endpoint returns 200; metrics emit with expected cardinalities.
6. Docs in this file kept up to date with what actually shipped (esp. endpoint cadences if tuned).

## Migration path to endgame

Per `hyperliquid-endgame-indexer-plan.md`:

- **v1 (this spec):** REST poller against api.hyperliquid.xyz.
- **v1.5 (if active HL accounts > ~10-15 hot at once):** add multi-IP egress pool.
- **v2 (endgame):** swap the poller for a disk-stream reader against a self-hosted non-validator node. **API contract unchanged; Sauron does not notice.**

The Postgres schema is shared across all three. Cursors, dedup keys, and event shapes are identical. The only thing that changes is what populates the tables.
