# DevOps Deployment Plan

This document captures the alpha deployment shape across Phala Cloud and
Railway. It names every service that runs in production and describes how the
peripheral services connect to the security-critical router stack.

## Target Runtime Split

### Phala Cloud (TEE)

Phala runs the security-critical router execution stack. Everything in
`etc/compose.phala.yml` runs here:

- `pg-secret-generator` (one-shot, writes the app DB password to a persistent
  volume)
- `router-master-key-init` (one-shot, generates or validates the 64-byte
  router master key)
- `postgres` — primary Postgres, scram-sha-256, `wal_level=logical` so CDC
  consumers on the physical standby can decode router events
- `postgres-replication-gateway` (socat sidecar exposing `:5432` for the
  Railway physical standby)
- `temporal-postgres` (tuned for the split topology: `max_connections=1000`,
  ~800 peak from 4 roles × `SQL_MAX_CONNS=200`)
- `temporal-schema` (one-shot)
- `temporal` — Temporal server **frontend** role, gRPC `:7233`, metrics
  on `:9090` (this is the endpoint clients/workers connect to)
- `temporal-history` — Temporal **history** role (health `:7234`)
- `temporal-matching` — Temporal **matching** role (health `:7235`)
- `temporal-internal-worker` — Temporal's **internal worker** role
  (health `:7239`); distinct from the app's `temporal-worker`
- `temporal-create-namespace` (one-shot)
- `router-api` — `ghcr.io/riftresearch/tee-router`, port `4522`, metrics on
  `:9100`
- `router-worker` — `ghcr.io/riftresearch/tee-router`, metrics on `:9101`
- `temporal-worker` — `ghcr.io/riftresearch/tee-router-temporal-worker`,
  metrics on `:9103`

`etc/compose.phala.yml` is self-contained: all configuration is inlined via
Compose `configs:` blocks, all images are pulled from public registries, and
there are no bind mounts of repo files. Phala can deploy it without a repo
checkout.

### Railway

Railway runs every observer, indexer, watcher, and operator-facing service:

- `router-replica-stunnel-v3` — client-side TLS termination for the Phala DB
  endpoint
- `router-physical-standby-v3` — physical streaming standby of the Phala
  primary
- `router-replica-setup` — one-shot bootstrap helper for the standby
- `sauron-state-db-v3` — Sauron-local detector cursor + CDC checkpoint store
- `sauron-worker-v3` — Sauron observer
- `evm-token-indexer-ethereum-v3`, `evm-token-indexer-base-v3`,
  `evm-token-indexer-arbitrum-v3` — Ponder indexers (one per chain)
- `evm-receipt-watcher-ethereum-v3`, `evm-receipt-watcher-base-v3`,
  `evm-receipt-watcher-arbitrum-v3` — Rust `evm-receipt-watcher` services
  (newHeads subscription + pending tx hash store)
- `bitcoin-indexer-v3` — Rust `bitcoin-indexer` (ZMQ push + RPC poll
  MultiSource with dedup)
- `bitcoin-receipt-watcher-v3` — Rust counterpart for Bitcoin tx receipts
- `hl-shim-indexer-v3` + `hl-shim-db-v3` — Rust Hyperliquid shim
  (REST-upstream / uniform-API-downstream)
- `router-gateway-v3` — public-facing load balancer in front of the Phala
  router-api (handles auth, CORS, rate-limiting)
- `admin-dashboard-v3` + `admin-dashboard-auth-db-v3` +
  `admin-dashboard-analytics-db-v3` — operator dashboard and its supporting
  DBs
- `explorer-v3` — public explorer UI
- `alloy-v3` — Grafana Alloy: scrapes `/metrics`, receives OTLP logs,
  `remote_write`s to VictoriaMetrics, forwards logs to Loki
- `victoriametrics-v3` — sole metrics TSDB
- `loki-v3` — log store
- `grafana-v3` — observability UI (VictoriaMetrics + Loki datasources)
- `hyperunit-socks5-proxy` — existing service, exposed for the Phala
  router-worker
- `sauron-bitcoin-rathole-broker` — existing service, reused for the v3
  Sauron Bitcoin transport

The Railway dashboard may visually group these services under a `rift v3`
canvas group, but the group metadata is not required by runtime config and is
not currently visible through the Railway CLI.

Do not reuse or modify existing Railway services for v3 unless explicitly
called out here. The current exceptions are `hyperunit-socks5-proxy` and
`sauron-bitcoin-rathole-broker`.

## Service Topology Overview

| Service | Host | Image source | Role |
|---|---|---|---|
| postgres (primary) | Phala | `postgres:18-alpine` | Router business state; CDC source |
| postgres-replication-gateway | Phala | `alpine:3.22.1` + socat | Exposes :5432 for the Railway standby |
| temporal-postgres | Phala | `postgres:16-alpine` | Temporal workflow history (max_connections=1000) |
| temporal | Phala | `temporalio/server:1.31.0` | Temporal **frontend** role (gRPC :7233) |
| temporal-history | Phala | `temporalio/server:1.31.0` | Temporal **history** role |
| temporal-matching | Phala | `temporalio/server:1.31.0` | Temporal **matching** role |
| temporal-internal-worker | Phala | `temporalio/server:1.31.0` | Temporal **internal worker** role |
| temporal-schema, temporal-create-namespace | Phala | `temporalio/admin-tools` | One-shot setup |
| router-master-key-init, pg-secret-generator | Phala | `alpine:3.22.1` | One-shot key/secret material |
| router-api | Phala | `ghcr.io/riftresearch/tee-router` | Public API (behind router-gateway) |
| router-worker | Phala | `ghcr.io/riftresearch/tee-router` | Order execution worker |
| temporal-worker | Phala | `ghcr.io/riftresearch/tee-router-temporal-worker` | Temporal activity worker |
| router-replica-stunnel-v3 | Railway | `railway/router-replica-stunnel/` | TLS termination for Phala DB |
| router-physical-standby-v3 | Railway | `railway/router-physical-standby/` | Read-only standby |
| router-replica-setup | Railway | `railway/router-replica-setup/` | One-shot standby bootstrap |
| sauron-state-db-v3 | Railway | Managed Postgres | Sauron-local cursors/checkpoints |
| sauron-worker-v3 | Railway | `ghcr.io/riftresearch/sauron` (via bin/sauron) | Observer (T-router acts, Sauron observes) |
| evm-token-indexer-{eth,base,arb}-v3 | Railway | `evm-token-indexer/` (Ponder/TS) | Token transfer indexer per chain |
| evm-receipt-watcher-{eth,base,arb}-v3 | Railway | `evm-receipt-watcher/` (Rust) | newHeads + pending tx receipt confirmation |
| bitcoin-indexer-v3 | Railway | `bitcoin-indexer/` (Rust) | Bitcoin block/tx indexer, MultiSource dedup |
| bitcoin-receipt-watcher-v3 | Railway | `bitcoin-receipt-watcher/` (Rust) | Bitcoin tx receipt confirmation |
| hl-shim-indexer-v3 | Railway | `hl-shim-indexer/` (Rust) | Hyperliquid REST→uniform API shim |
| hl-shim-db-v3 | Railway | Managed Postgres | hl-shim-indexer state |
| router-gateway-v3 | Railway | `railway/router-gateway/` (Bun) | Public LB: auth/CORS/rate-limiting |
| admin-dashboard-v3 | Railway | `railway/admin-dashboard/` (Bun) | Operator dashboard |
| admin-dashboard-auth-db-v3, admin-dashboard-analytics-db-v3 | Railway | Managed Postgres | Dashboard auth + analytics buckets |
| explorer-v3 | Railway | `railway/explorer/` (Bun) | Public explorer UI |
| alloy-v3 | Railway | `grafana/alloy` + `railway/alloy/` | Scrape /metrics + OTLP logs ingest (to build) |
| victoriametrics-v3 | Railway | `victoriametrics/victoria-metrics` | Metrics TSDB (to build) |
| loki-v3 | Railway | `grafana/loki` | Log store (to build) |
| grafana-v3 | Railway | `grafana/grafana` | Observability UI (to build) |
| hyperunit-socks5-proxy | Railway | existing | SOCKS5 egress for HyperUnit calls |
| sauron-bitcoin-rathole-broker | Railway | existing | Bitcoin RPC/ZMQ transport for Sauron |

## Router API And Worker

The router API and worker live in the singular Phala compose file. Expected
service shape:

```yaml
router-api:
  image: ghcr.io/riftresearch/tee-router:<semver-or-sha-tag>
  command: ["router-api", "--host", "0.0.0.0", "--port", "4522"]

router-worker:
  image: ghcr.io/riftresearch/tee-router:<semver-or-sha-tag>
  command: ["router-worker"]

temporal-worker:
  image: ghcr.io/riftresearch/tee-router-temporal-worker:<semver-or-sha-tag>
  command: ["worker"]
```

The router services and temporal-worker use the same primary router Postgres
instance inside the Phala compose network. Temporal Server uses a separate
Temporal Postgres instance so workflow history is isolated from router business
state.

`router-api` is exposed through a direct Phala public endpoint and consumed by
`router-gateway-v3` on Railway, which terminates public traffic. Sauron also
receives the public Phala URL as `ROUTER_INTERNAL_BASE_URL` and posts hints
over the internet (protected by `ROUTER_DETECTOR_API_KEY`).

`router-worker` and `temporal-worker` do not expose public endpoints.

Phala public endpoints are generated from literal Docker Compose `ports:`
entries. Keep the externally reachable router API mapping literal:

```yaml
ports:
  - "4522:4522"
```

Do not rely on environment-variable interpolation for Phala port declarations;
the dashboard/API may fail to materialize public endpoints from interpolated
port values.

## Temporal Performance Topology

The Phala Temporal server is **split into four single-responsibility role
containers** (frontend / history / matching / internal-worker) rather than a
single all-in-one server. All four share the `*temporal-server-base` anchor in
`etc/compose.phala.yml` and differ only by `SERVICES=` and health port. This
mirrors `etc/compose.local-full.yml` and is the topology the perf/loadgen work
was validated against.

Key server config (inline in the compose `configs:` / base anchor):

- `NUM_HISTORY_SHARDS=512` — set on every role; must be identical across roles
  and immutable for the life of the `temporal-postgres` data. Changing it
  requires a fresh Temporal DB.
- `SQL_MAX_CONNS=200` / `SQL_MAX_IDLE_CONNS=200` per role → ~800 peak
  connections; `temporal-postgres` runs `max_connections=1000` accordingly.
- Expanded `temporal-dynamic-config`: raised `frontend/matching/history` `rps`
  and `persistenceMaxQPS` ceilings, `matching.numTaskqueue{Read,Write}Partitions=16`,
  and `history.hostLevelCacheMaxSize=256000` (the post-1.27 replacement for the
  old `history.cacheMaxSize`) + `history.eventsCacheMaxSizeBytes=1GiB`.

The app's Rust `temporal-worker` is tuned via env (exported in its compose
command): `ROUTER_TEMPORAL_ACTIVITY_POLLERS=128`,
`ROUTER_TEMPORAL_WORKFLOW_POLLERS=32`, `ROUTER_TEMPORAL_ACTIVITY_SLOT_MIN=64`,
`ROUTER_TEMPORAL_ACTIVITY_SLOT_MAX=2000`,
`ROUTER_TEMPORAL_WORKFLOW_SLOT_MIN=16`, `ROUTER_TEMPORAL_WORKFLOW_SLOT_MAX=2000`,
`ROUTER_PAYMASTER_BATCH_MAX_SIZE=64`. These are baked into the prod compose; to
re-tune, change the `export` lines in the `temporal-worker` service.

> Sizing note: `compose.local-full.yml` additionally sets
> `deploy.resources.limits.cpus` per Temporal role (≈32 cores total) for the
> loadgen host. Those limits are **intentionally not** carried into
> `compose.phala.yml` — they are loadgen-box sizing, are ignored by plain
> `docker compose up`, and must be sized to the actual Phala TEE host before
> relying on them.

Source-of-truth Postgres (`postgres`) also received the durability-safe half of
the `a391849` tuning (`shared_buffers=1GB`, `effective_cache_size=4GB`,
`maintenance_work_mem=256MB`); `synchronous_commit` stays `on`. The
reconstructible-DB `synchronous_commit=off` profile is deliberately **not**
applied to the router primary.

## Router Gateway

`router-gateway-v3` (Railway, `railway/router-gateway/`, Bun runtime) is the
public-facing load balancer in front of `router-api`. It handles
authentication, CORS, and rate-limiting. **Do not add any of these at the app
layer** — the gateway is the single boundary.

Shape:

- Bun service from `apps/router-gateway/`
- Forwards to `ROUTER_INTERNAL_BASE_URL` (the public Phala router-api
  endpoint) using a service-level API key
- Manages public refund-authorization claim flow
- Public traffic terminates here; everything past this is internal-only

The gateway is the only thing public traffic should hit. Sauron's hints take a
separate path (direct to router-api with `ROUTER_DETECTOR_API_KEY`) because
they bypass user-facing concerns.

## Container Image Strategy

Build public GHCR Docker images and push them from GitHub Actions.

Image targets:

```text
ghcr.io/riftresearch/tee-router
ghcr.io/riftresearch/tee-router-temporal-worker
```

The GHCR packages must be public so Phala can pull immutable deploy tags
without registry credentials.

The router image contains:

- `/usr/local/bin/router-api`
- `/usr/local/bin/router-worker`
- router-server migrations
- CA certificates
- entrypoint/helper scripts

The temporal-worker image contains:

- `/usr/local/bin/temporal-worker`
- CA certificates

Every pushed image gets a `sha-<git-sha>` tag. Semver release tags come from
git tags of the form `vX.Y.Z`; the workflow also emits `X.Y.Z`. The `vX.Y.Z`
git tag must match the workspace version in `Cargo.toml`.

Phala can deploy either a semver tag for human readability or a SHA tag for
the exact source revision. For alpha, prefer pinning a semver release tag
after the matching SHA has been smoke tested.

## Phala Upgrade Flow

There is deliberately **no CI/CD for Phala**. The GHCR image workflow still
builds + pushes `tee-router{,-temporal-worker}` on every commit/tag, so the
image you want already exists in GHCR. Upgrading the TEE is then a
one-command `just` action against the existing CVM — Phala restarts the
CVM, pulls the pinned images, and starts with the new config.

One-time setup:

- `just phala-login` (device flow; `phala login --manual` for an API token)
- Create `.secrets/phala.env` (gitignored) containing every required
  compose variable — the `${VAR:?}` secret set plus `OBS_INGEST_TOKEN` and
  `ALLOY_V3_OTLP_URL` (see the secret-bootstrap table in the Deployment
  Runbook).

Normal upgrade (one command):

```sh
just phala-deploy 0.2.0      # pins both GHCR images to :0.2.0, redeploys CVM
```

Config-only redeploy (no image bump — e.g. compose/secret edit):

```sh
just phala-upgrade
```

Notes:

- `phala-deploy` rewrites the two `ghcr.io/riftresearch/tee-router*` tags in
  `etc/compose.phala.yml` in place and validates the compose before
  deploying. **Commit that tag bump** so the deployed revision is recorded
  in git (the pinned tag in `compose.phala.yml` is the source of truth for
  what's running).
- CVM is referenced by name (`phala_cvm`, default `tee-router-demo`);
  override per-invocation: `just phala-deploy 0.2.0 phala_cvm=other-cvm`.
- The `phala cvms upgrade` env-file flag is marked `VERIFY` in the justfile
  — confirm against `phala cvms upgrade --help` on the installed CLI before
  the first real run (CLI surface shifts between versions).
- Rollback = `just phala-deploy <previous-tag>`.

## Primary Postgres In Phala

The router primary database lives inside the Phala compose file.

`etc/compose.phala.yml` already implements the pattern:

- primary Postgres runs in the compose stack
- app DB password is generated once into a persistent volume
- Postgres is configured for physical streaming replication to Railway
- `wal_level = logical` is retained so CDC consumers can decode router events
  from the physical standby
- physical replication user is created during init
- physical replication slots are allowed for the Railway standby
- `pg_hba.conf` restricts `router_app` to the internal network and
  `replicator` to the replication network only
- the primary Postgres port is exposed through `postgres-replication-gateway`
  (a socat sidecar) so the public DB gateway endpoint never points at the
  Postgres container directly

Required primary config (already in the inline `configs:` block):

```conf
listen_addresses = '*'
password_encryption = 'scram-sha-256'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
```

Names in use:

- database: `router_db`
- app user: `router_app`
- replication user: `replicator`

The Phala stack uses separate fixed private subnets so Postgres can
distinguish internal router API/worker traffic from public replication
traffic:

```yaml
networks:
  router-network:
    name: router-v3-private-network
    ipam:
      config:
        - subnet: 172.30.0.0/16
  router-replication-network:
    ipam:
      config:
        - subnet: 172.31.0.0/16
```

## Railway Read Replica

Sauron and the admin dashboard do not connect directly to the Phala primary.
A Railway physical standby streams WAL from the Phala primary and is the
read endpoint for everything observer-side.

`etc/compose.physical-replica.yml` contains the model:

1. `router-replica-stunnel-v3` connects to the Phala DB SNI over `:443`.
2. `router-physical-standby-v3` runs on Railway from
   `railway/router-physical-standby/`.
3. The standby initializes itself with `pg_basebackup` (helper:
   `router-replica-setup`).
4. The standby uses a physical replication slot on the Phala primary.
5. Sauron and the admin dashboard run heavy reads against the standby.
6. Sauron consumes a CDC decoding slot on the standby for watch-set events.
7. Admin dashboard consumes a separate CDC slot for order analytics.

Sauron connects to the Railway physical standby via
`ROUTER_REPLICA_DATABASE_URL`.

The physical standby is read-only. Sauron-local tables live in
`sauron-state-db-v3`, wired through `SAURON_STATE_DATABASE_URL`. This state
DB stores detector cursors and CDC checkpoints only; router-owned schema and
data derive from the primary through the physical standby.

Phala exposes the primary Postgres TCP port through
`postgres-replication-gateway` and Phala's TLS-terminating endpoint performs
TLS termination before forwarding plaintext TCP to Postgres. Railway's
`router-replica-stunnel-v3` performs client-side TLS-to-TCP conversion before
the physical standby connects.

Helper images for this path live in the repo:

- `railway/router-replica-stunnel/` — client-side stunnel sidecar
- `railway/router-physical-standby/` — physical standby bootstrap and Postgres
  process
- `railway/router-replica-setup/` — one-shot bootstrap helper

Deploy these from the repo root so their Dockerfile `COPY` paths resolve
against the repository layout.

## Router Master Key

Router-derived vault addresses depend on a stable 64-byte router master key.

The app accepts a file path via `ROUTER_MASTER_KEY_PATH`, not a raw
environment variable. The Phala compose stack includes a one-shot
`router-master-key-init` service that prepares this file before `router-api`
and `router-worker` start.

Shared persistent volume layout:

```text
router-master-key-init -> /run/router-secrets
router-api             -> /run/router-secrets
router-worker          -> /run/router-secrets
temporal-worker        -> /run/router-secrets
```

Behavior:

1. Check for `/run/router-secrets/router-master-key.hex`.
2. If the file exists, validate that it is exactly 128 hex chars.
3. If the file does not exist, create it with `openssl rand -hex 64`.
4. Use `umask 077`.
5. Write to a temporary file, then atomically rename.
6. Never print the key.
7. Fail hard on malformed existing key material.

If the persistent volume is lost, derived vault addresses change. Treat this
volume as critical key material.

## Sauron

Sauron runs on Railway as `sauron-worker-v3`.

Required inputs:

- `ROUTER_REPLICA_DATABASE_URL` pointing at the Railway physical standby
- `SAURON_STATE_DATABASE_URL` pointing at `sauron-state-db-v3`
- `SAURON_REPLICA_EVENT_SOURCE=cdc`
- `SAURON_CDC_SLOT_NAME=sauron_watch_cdc`
- `ROUTER_CDC_PUBLICATION_NAME=router_cdc_publication`
- `ROUTER_CDC_MESSAGE_PREFIX=rift.router.change`
- `ROUTER_REPLICA_DATABASE_NAME`
- `ROUTER_INTERNAL_BASE_URL` pointing at the public Phala router API URL
- `ROUTER_DETECTOR_API_KEY`
- EVM RPC URLs
- EVM token indexer URLs (per-chain `*_TOKEN_INDEXER_URL` + shared API key)
- EVM receipt watcher URLs (per chain)
- Bitcoin indexer URL
- Bitcoin receipt watcher URL
- HL shim indexer URL
- Bitcoin RPC/ZMQ/Electrum config (still consumed for some direct lookups)

Sauron posts non-authoritative hints to the router API. Router-worker still
validates provider and chain state before executing state transitions.

Sauron consumes router CDC events from the physical standby through the
`pgoutput` plugin with emitted CDC messages enabled. Router migrations install
the `pg_logical_emit_message` triggers and `router_cdc_publication`; no
replica-local trigger migrations are used. The publication contains no tables
— all CDC data flows via the logical-message channel.

## Admin Dashboard

`admin-dashboard-v3` (Railway, `railway/admin-dashboard/`, Bun runtime)
provides the operator-facing dashboard.

Connections:

- Reads router state from `router-physical-standby-v3` via
  `ADMIN_DASHBOARD_REPLICA_DATABASE_URL`
- Consumes router CDC events from the standby via a dedicated logical
  replication slot `admin_dashboard_orders_cdc` (publication
  `router_cdc_publication`, message prefix `rift.router.change`)
- Owns two separate Railway Postgres instances:
  - `admin-dashboard-auth-db-v3` — better-auth user store
    (`ADMIN_DASHBOARD_AUTH_DATABASE_URL`)
  - `admin-dashboard-analytics-db-v3` — volume buckets, order status counts,
    backfill state (`ADMIN_DASHBOARD_ANALYTICS_DATABASE_URL`)

The dashboard's analytics tables (`admin_volume_buckets`,
`admin_volume_order_contributions`, `admin_order_status_counts`) are populated
incrementally from CDC events, with a one-shot snapshot backfill at startup
or on schema migration.

Authentication uses better-auth. The dashboard exposes its own session login
flow; do not put it behind a separate auth gateway. Production requires
`ADMIN_DASHBOARD_PRODUCTION=true` and a real `BETTER_AUTH_URL` /
`BETTER_AUTH_SECRET`.

The dashboard uses `ROUTER_ADMIN_API_KEY` to call protected router endpoints
(e.g. operator-only admin actions).

## EVM Token Indexers

All Ponder EVM token indexers stay on Railway. Create brand-new v3 services;
do not reuse existing cbBTC-specific Ponder services.

Run one indexer per EVM chain: Ethereum, Base, Arbitrum.

Each indexer needs:

- its own Railway service with `-v3` suffix
- `PONDER_CHAIN_ID`
- `PONDER_RPC_URL_HTTP`
- `PONDER_WS_URL_HTTP`
- chain-specific `DATABASE_SCHEMA` / `PONDER_SCHEMA`
- `PONDER_CONTRACT_START_BLOCK`
- `PONDER_PORT=4001`
- `PORT=4001`
- raw transfer retention config
- `EVM_TOKEN_INDEXER_API_KEY`

Railway injects `PORT`; Ponder honors that value for its listener, so set
both `PONDER_PORT` and `PORT` to `4001` to keep the private-network URL
stable.

Each indexer also runs a periodic full-poll job alongside Ponder ingest. Both
sources feed a shared MultiSource dedup path (LRU `recentSeen` cache + a
`dedupKey` derived from transfer identity). Push-only ingest is no longer
sufficient on its own.

The v3 start block for each chain corresponds to yesterday at midnight UTC
relative to the deployment planning date `2026-04-27T00:00:00Z`:

- Ethereum: `24967646` (`timestamp=1777247999`)
- Base: `45229327` (`timestamp=1777248001`)
- Arbitrum: `456704761` (`timestamp=1777248000`)

RPC URLs, including credentialed WebSocket URLs, must be configured as
Railway service variables and must not be committed to the repo.

Sauron uses Railway-reachable indexer URLs:

- `ETHEREUM_TOKEN_INDEXER_URL`
- `BASE_TOKEN_INDEXER_URL`
- `ARBITRUM_TOKEN_INDEXER_URL`
- `TOKEN_INDEXER_API_KEY`, matching each indexer's
  `EVM_TOKEN_INDEXER_API_KEY`

Because Sauron is also on Railway, these can use Railway private networking
where possible.

## EVM Receipt Watchers

`evm-receipt-watcher-{ethereum,base,arbitrum}-v3` (Railway, Rust, source
`evm-receipt-watcher/`) confirm transaction receipts on each EVM chain.

Each instance:

- Subscribes to `newHeads` on the chain's WebSocket endpoint
- Tracks a pending tx-hash store
- Emits a `ReceiptObserved` signal back to Sauron when a tracked tx confirms

Required per-instance env (TBD — verify against the binary's config):

- `EVM_CHAIN_ID`
- `EVM_RPC_URL` (HTTP)
- `EVM_WS_URL` (WebSocket for newHeads)
- HTTP listener port
- Optional API key for outgoing Sauron callbacks

Sauron consumes these via per-chain `*_RECEIPT_WATCHER_URL` env vars.

## Bitcoin Indexer

`bitcoin-indexer-v3` (Railway, Rust, source `bitcoin-indexer/`) is the
Bitcoin counterpart to the EVM token indexers, but covers blocks/txs rather
than ERC-20 transfers.

Sources (concurrent via MultiSource + LRU dedup):

- **Push**: `bitcoind` ZMQ `rawblock`/`rawtx` streams
- **Poll**: periodic `getblockchaininfo` + `getblock` fallback

Both feed a shared dedup path; neither is active/standby. A push-only design
was insufficient because ZMQ drops are silent.

Required env (TBD — verify against the binary):

- `BITCOIN_RPC_URL`
- `BITCOIN_RPC_AUTH`
- `BITCOIN_ZMQ_RAWBLOCK_ENDPOINT`
- `BITCOIN_ZMQ_RAWTX_ENDPOINT`
- HTTP listener port
- Indexer state DB URL (if used)

The existing `sauron-bitcoin-rathole-broker` is reused here for the RPC/ZMQ
transport.

## Bitcoin Receipt Watcher

`bitcoin-receipt-watcher-v3` (Railway, Rust, source `bitcoin-receipt-watcher/`)
mirrors the EVM receipt watcher pattern for Bitcoin. Tracks pending txids,
confirms via ZMQ + RPC, and signals back to Sauron.

Required env follows the same shape as the Bitcoin indexer.

## HL Shim Indexer

`hl-shim-indexer-v3` (Railway, Rust, source `hl-shim-indexer/`) plus its
dedicated `hl-shim-db-v3` Postgres provides a uniform-API shim in front of
Hyperliquid's REST endpoints. Upstream is HTTP polling against Hyperliquid;
downstream is the same WebSocket + HTTP API the rest of the stack consumes.

The shim isolates Sauron from Hyperliquid REST quirks (rate limits, response
shapes, cursor management). Endpoints:

- `/transfers` (historical)
- `/prune` (state hygiene)
- `/subscribe` (WebSocket stream)

Required env:

- `HL_SHIM_DATABASE_URL` (points at `hl-shim-db-v3`)
- `HYPERLIQUID_API_URL`
- HTTP listener port (default `4002`, metrics on `:9104`)
- Hyperliquid rate-limit and pagination config
- API key for downstream consumers (`HL_SHIM_API_KEY`)

Sauron consumes this via `HL_SHIM_INDEXER_URL` + `HL_SHIM_API_KEY`.

This is pull-only by upstream design; no MultiSource concurrent push+poll
applies.

## Explorer

`explorer-v3` (Railway, `railway/explorer/`, Bun runtime) is the public
explorer UI. It is read-only and consumes router state through whatever
public surface is appropriate (TBD: confirm whether it talks directly to
router-api, to router-gateway, or to the standby).

## HyperUnit SOCKS5 Proxy

The existing HyperUnit SOCKS5 proxy remains on Railway, exposed for the
Phala router-worker.

Requirements:

- Railway TCP proxy reachable from Phala
- username/password auth
- long random password
- no unauthenticated open-proxy behavior
- no `ALLOWED_IPS` restriction
- no destination FQDN allowlisting; SOCKS5 username/password auth is the
  proxy access boundary
- generate a fresh random proxy password during deployment and store it only
  as Railway/Phala secret material

Router-worker uses:

```text
HYPERUNIT_PROXY_URL=socks5://<user>:<password>@<public-proxy-host>:<port>
```

The router expects `socks5://`, not `socks5h://`.

## Bitcoin RPC/ZMQ Transport

Bitcoin RPC/ZMQ is needed for both:

- The router-worker (when Bitcoin is a source/destination asset)
- The `bitcoin-indexer-v3` and `bitcoin-receipt-watcher-v3` services
- Sauron for any direct Bitcoin lookups not covered by the indexer

The existing Railway `sauron-bitcoin-rathole-broker` service is reused.
Railway private networking gives:

```env
BITCOIN_RPC_URL=http://sauron-bitcoin-rathole-broker.railway.internal:40031
BITCOIN_ZMQ_RAWTX_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40032
BITCOIN_ZMQ_SEQUENCE_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40033
```

Phala-side router-worker connects through the public broker endpoint with the
same RPC auth.

## Public Router API Security

Public traffic terminates at `router-gateway-v3` on Railway. The gateway
handles authentication, CORS, and rate-limiting. The Phala `router-api`
itself only enforces internal API keys for the non-public surface:

- Sauron hint endpoints use `ROUTER_DETECTOR_API_KEY`.
- Admin/provider policy endpoints use `ROUTER_ADMIN_API_KEY`.

Do not add a `ROUTER_PUBLIC_API_KEY` or any auth/CORS/rate-limit logic to the
app layer. The gateway is the single boundary for those concerns.

## Provider And Chain Environment

Router API and worker need the same core config:

- `DATABASE_URL`
- `ROUTER_MASTER_KEY_PATH`
- `ETH_RPC_URL`, `BASE_RPC_URL`, `ARBITRUM_RPC_URL`
- `BITCOIN_RPC_URL`, `BITCOIN_RPC_AUTH`
- `ELECTRUM_HTTP_SERVER_URL`
- `ACROSS_API_URL`, `ACROSS_API_KEY`, `ACROSS_INTEGRATOR_ID`
- `CCTP_API_URL` if overriding Circle Iris default,
  `CCTP_TOKEN_MESSENGER_V2_ADDRESS`, `CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS`
- `HYPERUNIT_API_URL`, `HYPERUNIT_PROXY_URL`
- `HYPERLIQUID_API_URL`, `HYPERLIQUID_NETWORK`,
  `HYPERLIQUID_ORDER_TIMEOUT_MS`
- `VELORA_API_URL`, `VELORA_PARTNER`
- per-chain paymaster private keys (Bitcoin, Ethereum, Base, Arbitrum,
  Hyperliquid)
- `CHAINALYSIS_HOST`, `CHAINALYSIS_TOKEN`
- `ROUTER_DETECTOR_API_KEY`, `ROUTER_ADMIN_API_KEY`,
  `ROUTER_GATEWAY_API_KEY`
- `COINBASE_PRICE_API_BASE_URL`

## Observability

Spans and distributed tracing are not used. The previous Tempo / OTel trace
pipeline was removed in commit `3b09b8b` to avoid the CPU cost of
per-request span allocation. Only logs and metrics are produced.

The production observability design is Alloy + VictoriaMetrics + Loki +
Grafana on Railway (see below).

### Logs

Each Rust service emits structured logs via `tracing::info!/warn!/error!`.
`opentelemetry-appender-tracing` bridges these into the OTLP logs export
path. The export is disabled unless `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is set
by the deployment. There is no trace exporter (removed in `3b09b8b`).

- **Local**: services emit OTLP logs to `alloy`, which forwards to Loki;
  Grafana queries Loki.
- **Production (design)**: services emit OTLP logs to `alloy-v3` on Railway,
  which forwards to `loki-v3`; `grafana-v3` queries it.
- **Production (actual status)**: **not wired.** `etc/compose.phala.yml` sets
  no `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT`, so prod log export is currently
  disabled. It becomes live only once the Railway observability stack exists
  and the endpoint is injected into the Phala/Railway services.

### Metrics

Each service exposes a Prometheus-format `/metrics` endpoint on a dedicated
port:

| Service | Port |
|---|---|
| router-api | 9100 |
| router-worker | 9101 |
| sauron | 9102 |
| temporal-worker | 9103 |
| hl-shim-indexer | 9104 |
| temporal (server) | 9090 |

Receipt watchers and Bitcoin indexers expose metrics too; their port
assignments are TBD until the Railway service definitions are written.

**Local** (`etc/compose.local-observability.yml`):

```sh
docker compose \
  -f etc/compose.local-full.yml \
  -f etc/compose.local-observability.yml \
  up -d
```

The local stack runs:

- `alloy` (Grafana's OTel-collector-compatible agent) — scrapes
  `/metrics` endpoints and receives OTLP logs
- `victoriametrics` — Prometheus-compatible TSDB (the single metrics store)
- `loki` — log store
- `grafana` — UI

The names `prometheus.scrape` and `prometheus.remote_write` in the Alloy
config (`etc/alloy.local.alloy`) are Alloy component names referring to the
Prometheus protocols. There is **no separate Prometheus server**.
VictoriaMetrics is the sole TSDB and serves Grafana queries through its
Prometheus-compatible read API.

**Production metrics destination (DECIDED)**: the local topology is promoted
to Railway services — Alloy scrapes every service's `/metrics` and
`remote_write`s to VictoriaMetrics; Grafana reads VictoriaMetrics (metrics)
and Loki (logs). This is no longer TBD.

**Production (actual status)**: **not built.** `railway/` has no
alloy/victoriametrics/grafana service, there is no prod Alloy config (only
`etc/alloy.local.alloy`, hardcoded to compose-internal hostnames), and
`compose.phala.yml` only *exposes* `/metrics` ports — nothing scrapes them.
Prod metrics and logs are effectively dark until the stack below is built.

### Production Observability Stack (to build)

Target Railway services (mirror the local stack, `-v3` suffix):

| Service | Source | Role |
|---|---|---|
| `alloy-v3` | `grafana/alloy` + new `railway/alloy/config.alloy` | **Sole public, authenticated observability ingress.** Scrapes Railway services over `*.railway.internal`; accepts authenticated `remote_write` + OTLP from the Phala-side agent; fans out to VictoriaMetrics + Loki over Railway private net |
| `victoriametrics-v3` | `victoriametrics/victoria-metrics` | Sole metrics TSDB (`:8428`), Railway-private only |
| `loki-v3` | `grafana/loki` | Log store (`:3100`), Railway-private only |
| `grafana-v3` | `grafana/grafana` | UI; datasources = VictoriaMetrics + Loki |

#### Decided transport: Phala → Railway

The TEE is **not** on Railway's private network. The decided model
(push-out, never scrape-in):

- **No `/metrics` port on the Phala stack is ever exposed publicly.** Zero
  inbound to the TEE.
- A small **`alloy` sidecar runs inside `etc/compose.phala.yml`**. It scrapes
  the Phala core services over the internal compose network and makes a
  **single authenticated TLS egress** to `alloy-v3` (`remote_write` metrics +
  forward OTLP logs).
- `alloy-v3` is the **only public, authenticated** observability endpoint
  (bearer token for alpha; token stored as Phala secret material; mTLS is a
  later hardening). `victoriametrics-v3` / `loki-v3` are Railway-private and
  never publicly reachable.
- Railway-side services (sauron, indexers, watchers, hl-shim) stay on the
  Railway private network and are scraped by `alloy-v3` directly — unchanged.
- **Follow-up (non-blocking):** add a label/line scrub allowlist in the
  Phala-side Alloy so telemetry leaving the TEE boundary cannot carry
  sensitive labels or log content. Tracked, not required for first cutover.

Implementation tasks:

1. Author `railway/alloy/config.alloy` (the `alloy-v3` config) from
   `etc/alloy.local.alloy`: `*.railway.internal` scrape targets for every
   Railway metrics endpoint — sauron `:9102`, hl-shim-indexer `:9104`, the
   receipt-watcher / bitcoin-indexer ports (assign and record them —
   currently TBD); an authenticated `remote_write` + OTLP receiver for the
   Phala-side agent; fan-out to `victoriametrics-v3` / `loki-v3`.
2. Add an `alloy` sidecar service to `etc/compose.phala.yml` with an inline
   `configs:` Alloy config that scrapes the Phala core services over the
   compose network (router-api `:9100`, router-worker `:9101`,
   temporal-worker `:9103`, the four Temporal roles `:9090`) and
   `remote_write`s + forwards OTLP outward to `alloy-v3` using a bearer
   token from Phala secret material. Egress only — no published ports.
3. Inject `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` (and base
   `OTEL_EXPORTER_OTLP_ENDPOINT`): Phala services → the in-TEE `alloy`
   sidecar; Railway Rust services → `alloy-v3` over the private network.
4. Persistent volumes for `victoriametrics-v3` and `loki-v3`; set retention.
5. `grafana-v3`: provision VictoriaMetrics + Loki datasources and import the
   existing dashboards under `etc/grafana/`.
6. Confirm the Railway IPv6 listener behavior (below) for every Railway
   service scraped by `alloy-v3` via `*.railway.internal`.
7. Follow-up: implement the Phala-side Alloy label/line scrub allowlist.

### IPv6 listener note

Railway private networking can require IPv6 wildcard listeners. The Rust
observability helper keeps the operator-facing `METRICS_BIND_ADDR=0.0.0.0:9102`
setting but binds it as `[::]:9102` when Railway runtime metadata is present,
so private scrapers can reach the endpoint through `*.railway.internal`.

## Railway Build Plan (GitHub-connected, auto-redeploy)

### Model

- **One Railway project**, one `production` environment. (Add `staging`
  later by environment-duplicate; out of scope for alpha.)
- Every app service is **GitHub-sourced** from `riftresearch/tee-router` on
  a fixed deploy branch. A push to that branch auto-deploys **only the
  services whose `watchPatterns` match the changed paths**.
- This is a **shared monorepo** (one Rust workspace + repo-root
  Dockerfiles). Do **not** use Railway `rootDirectory` isolation — it hides
  the workspace and breaks Rust builds. Use the **Dockerfile builder** with
  full repo context and a per-service `dockerfilePath`.
- **No official Railway Terraform provider exists.** Reproducible topology =
  (a) a per-service `railway.json` checked into the repo + (b) one
  idempotent bootstrap script (`railway/bootstrap.sh`, Railway CLI/GraphQL
  `serviceCreate` + config patch). Those two together are the IaC.

### Per-service `railway.json`

Each service gets a `railway.json` at a stable path in the repo (e.g.
`railway/<service>/railway.json`) declaring at minimum:

```json
{
  "build": {
    "builder": "DOCKERFILE",
    "dockerfilePath": "<path to that service's Dockerfile>",
    "watchPatterns": ["<the service's source subtree>", "<shared deps>"]
  },
  "deploy": {
    "healthcheckPath": "<if HTTP>",
    "restartPolicyType": "ON_FAILURE",
    "numReplicas": 1
  }
}
```

`watchPatterns` is **mandatory** — without it every push rebuilds all
services. Scope each to its own subtree **plus** shared Rust crates it
depends on (a `crates/**` change must redeploy every Rust service that
links it; accept that fan-out, it is correct).

### Service source / build matrix

All Dockerfiles below **exist and are verified**. Each repo-sourced service
has a committed `railway/<svc>/railway.json` (builder/dockerfilePath/
watchPatterns/restart); `railway/bootstrap.sh` reconciles the topology into
the existing `tee-router` Railway project and applies the equivalent config
via CLI patches.

| Service | Source | Builder / Dockerfile | railway.json |
|---|---|---|---|
| router-physical-standby-v3 | repo | `railway/router-physical-standby/Dockerfile` | `railway/router-physical-standby/railway.json` |
| router-replica-stunnel-v3 | repo | `railway/router-replica-stunnel/Dockerfile` | `railway/router-replica-stunnel/railway.json` |
| sauron-worker-v3 | repo | `etc/Dockerfile.sauron` | `railway/sauron/railway.json` |
| sauron-state-db-v3 | **managed Postgres** | n/a | n/a |
| evm-token-indexer-{eth,base,arb}-v3 | repo | `evm-token-indexer/Dockerfile.index` | `railway/evm-token-indexer/railway.json` |
| evm-receipt-watcher-{eth,base,arb}-v3 | repo | `etc/Dockerfile.evm-receipt-watcher` | `railway/evm-receipt-watcher/railway.json` |
| bitcoin-indexer-v3 | repo | `etc/Dockerfile.bitcoin-indexer` | `railway/bitcoin-indexer/railway.json` |
| bitcoin-receipt-watcher-v3 | repo | `etc/Dockerfile.bitcoin-receipt-watcher` | `railway/bitcoin-receipt-watcher/railway.json` |
| hl-shim-indexer-v3 | repo | `etc/Dockerfile.hl-shim-indexer` | `railway/hl-shim-indexer/railway.json` |
| hl-shim-db-v3 | **managed Postgres** | n/a | n/a |
| router-gateway-v3 | repo | `railway/router-gateway/Dockerfile` | `railway/router-gateway/railway.json` |
| admin-dashboard-v3 | repo | `railway/admin-dashboard/Dockerfile` | `railway/admin-dashboard/railway.json` |
| admin-dashboard-auth-db-v3 | **managed Postgres** | n/a | n/a |
| admin-dashboard-analytics-db-v3 | **managed Postgres** | n/a | n/a |
| explorer-v3 | repo | `railway/explorer/Dockerfile` | `railway/explorer/railway.json` |
| alloy-v3 | repo | `railway/alloy/Dockerfile` (+ `etc/alloy.railway.alloy`) | `railway/alloy/railway.json` |
| loki-v3 | repo | `railway/loki/Dockerfile` (+ `etc/loki.railway.yml`) | `railway/loki/railway.json` |
| grafana-v3 | repo | `railway/grafana/Dockerfile` (+ `etc/grafana/**`, datasource provisioning) | `railway/grafana/railway.json` |
| victoriametrics-v3 | image `victoriametrics/victoria-metrics` | image + start args (set by bootstrap) | n/a |

> Gap: `router-replica-setup` (one-shot standby bootstrap helper) has no
> `railway/router-replica-setup/` Dockerfile in the repo. Standby bring-up
> currently relies on `railway/router-physical-standby/entrypoint.sh`.
> Confirm whether a separate setup service is still needed; if so it must be
> authored. (Not created here — flagged rather than invented.)

### Wiring (no hardcoded URLs)

- Managed Postgres → consumers via reference variables, e.g.
  `SAURON_STATE_DATABASE_URL=${{sauron-state-db-v3.DATABASE_URL}}`.
- Service→service via private DNS:
  `HL_SHIM_INDEXER_URL=http://${{hl-shim-indexer-v3.RAILWAY_PRIVATE_DOMAIN}}:${{hl-shim-indexer-v3.PORT}}`.
- Cross-cutting secrets/keys (`ROUTER_DETECTOR_API_KEY`,
  `TOKEN_INDEXER_API_KEY`, observability bearer token) as **project shared
  variables**, referenced as `${{shared.NAME}}` so one value fans out.
- Every Railway Rust service must bind its listener on `[::]` (IPv6
  wildcard) or private DNS + healthchecks fail — same rule the observability
  helper already applies to metrics.

### Gotchas

- `watchPatterns` omitted ⇒ all services rebuild on every commit.
- `rootDirectory` + shared workspace ⇒ broken Rust build (use Dockerfile
  builder, full context).
- Phala TEE is **not** on Railway's network — its only Railway links are the
  DB replication gateway and the decided alloy push-out. Do not attempt
  Railway private DNS from Phala.
- Config-as-code is **per service**; topology creation/teardown is the
  bootstrap script's job, not a single manifest.

## Deployment Runbook

### Pre-deploy blockers (must be true before "deploy everything")

Done (in repo):

- ✅ Observability stack built: `etc/alloy.railway.alloy` (alloy-v3),
  `etc/alloy.phala.alloy` + in-TEE `alloy` sidecar in `compose.phala.yml`,
  `etc/loki.railway.yml`, `railway/{alloy,loki,grafana}/Dockerfile`, Grafana
  datasource/dashboard provisioning.
- ✅ All service Dockerfiles exist and are verified.
- ✅ `railway/<svc>/railway.json` committed for every repo-sourced service.
- ✅ `railway/bootstrap.sh` written (idempotent; targets the existing
  `tee-router` project, never creates one), `bash -n` clean.

Still required before a real run:

1. Receipt-watcher / bitcoin-indexer **metrics ports confirmed** (the
   alloy-v3 scrape config assumes `:8080`; verify each binary's
   `METRICS_BIND_ADDR` and that it binds `[::]`).
2. EVM token-indexer **RPC/WS URLs provisioned as Railway vars**.
3. Secret bootstrap complete (next subsection), incl. `OBS_INGEST_TOKEN`
   and `ALLOY_V3_OTLP_URL` for the Phala↔alloy-v3 channel.
4. `bootstrap.sh` lines marked `# VERIFY:` checked against the installed
   Railway CLI; GitHub App connected to the Railway account.
5. `router-replica-setup` gap resolved (see matrix note).

### Secret bootstrap (do first, once)

| Secret | Generated how | Lives where |
|---|---|---|
| Paymaster keys ×5 (BTC/ETH/BASE/ARB/HL) | operator-generated, funded | Phala secret store |
| `POSTGRES_REPLICA_PASSWORD` | generate once | Phala secret store **and** mirrored to Railway stunnel/standby (must match) |
| `TEMPORAL_POSTGRES_PASSWORD` | generate once | Phala secret store |
| `CHAINALYSIS_TOKEN`, `ACROSS_API_KEY` | from provider | Phala secret store |
| `ROUTER_DETECTOR_API_KEY`, `ROUTER_GATEWAY_API_KEY`, `ROUTER_ADMIN_API_KEY` | generate | Phala secret + Railway shared var (consumers: sauron, gateway, dashboard) |
| Observability bearer token | generate | Phala secret + Railway shared var |
| Router master key, app DB password | **generated in-TEE** by one-shot init | Phala persistent volumes (never injected) |
| RPC/Electrum/HyperUnit-proxy endpoints | from providers | Phala env / Railway vars (proxy URL is credential-bearing) |

The replica password and TLS material are the only **cross-platform**
secrets — generate once, set identically on both sides.

### Ordered bring-up (health-gated)

1. **Phala**: deploy `compose.phala.yml`. Gate: `pg-secret-generator` +
   `router-master-key-init` complete → `postgres` healthy → `temporal-*`
   healthy → `router-api` `/status` 200, `router-worker` /
   `temporal-worker` polling.
2. **Phala public endpoints**: confirm literal `4522:4522` mapping resolves;
   confirm replication gateway `:5432` reachable.
3. **Railway data path**: `router-replica-stunnel-v3` →
   `router-physical-standby-v3` (run `router-replica-setup`). Gate:
   standby replay caught up + CDC slots present.
4. **Railway managed DBs**: `sauron-state-db-v3`, `hl-shim-db-v3`,
   `admin-dashboard-{auth,analytics}-db-v3`.
5. **Railway indexers/watchers**: token-indexers, receipt-watchers,
   bitcoin-indexer/receipt-watcher, hl-shim-indexer. Gate: each healthy and
   reachable on private DNS.
6. **Railway sauron-worker-v3**: wire all URLs (matrix below). Gate: CDC
   slot/checkpoint healthy, watch sync, hint POST to Phala `router-api` 200.
7. **Railway public**: `router-gateway-v3`, `admin-dashboard-v3`,
   `explorer-v3`.
8. **Observability**: `victoriametrics-v3`, `loki-v3`, `alloy-v3`,
   `grafana-v3`; add in-TEE alloy sidecar; inject `OTEL_EXPORTER_OTLP_*`.
   Gate: metrics in VM, logs in Loki, dashboards render.
9. **Validate**: run the smoke-test list (below). Then a dust-sized live
   route. Then the 10k loadgen-fast replay.

### Inter-service wiring order

Set wiring **after** the producer service exists (its
`RAILWAY_PRIVATE_DOMAIN`/`DATABASE_URL` only resolve post-create):

- standby ready → set `ROUTER_REPLICA_DATABASE_URL` on sauron + dashboard
- state/shim DBs ready → set `SAURON_STATE_DATABASE_URL`,
  `HL_SHIM_DATABASE_URL`
- indexers/watchers ready → set per-chain
  `*_TOKEN_INDEXER_URL` / `*_RECEIPT_WATCHER_URL` / bitcoin / hl-shim URLs
  on sauron
- Phala `router-api` public URL → `ROUTER_INTERNAL_BASE_URL` on sauron
- alloy-v3 endpoint + bearer → `OTEL_EXPORTER_OTLP_*` on every service

### Rollback

Per service: `railway down --service <svc>` reverts to the prior
deployment (does not delete). Phala: redeploy the previous pinned image tag
(semver/SHA). The router-master-key and primary-DB volumes are **never**
torn down on rollback — treat as protected state.

Completed:

- [x] Router Dockerfile that builds `router-api` and `router-worker`.
- [x] Temporal-worker Dockerfile that builds `temporal-worker`.
- [x] GitHub Actions image build/push workflow for public
      `ghcr.io/riftresearch/tee-router` and
      `ghcr.io/riftresearch/tee-router-temporal-worker`.
- [x] Phala compose file `etc/compose.phala.yml` (self-contained, inline
      configs, public-registry images only).
- [x] Primary Postgres config/init scripts inlined into the Phala compose
      file.
- [x] Sauron 9-task architectural refactor + MultiSource dedup landed
      (commits up through `5031581`).
- [x] Tempo / span tracing removed; logs-only OTLP path retained
      (commit `3b09b8b`).
- [x] `usd_valuation_json` populated on legs/steps so admin-dashboard volume
      charts can backfill (commit `2e75fbd`).

Still to do:

- [ ] Deploy Railway `router-physical-standby-v3`,
      `router-replica-stunnel-v3`, and `sauron-state-db-v3`.
- [ ] Verify physical standby replay and CDC decoding on the standby.
- [ ] Configure `sauron-worker-v3` against the physical standby and state DB.
- [ ] Deploy the three Ponder `evm-token-indexer-*-v3` services.
- [ ] Deploy the three `evm-receipt-watcher-*-v3` services.
- [ ] Deploy `bitcoin-indexer-v3` and `bitcoin-receipt-watcher-v3`.
- [ ] Deploy `hl-shim-indexer-v3` + `hl-shim-db-v3`.
- [ ] Deploy `router-gateway-v3` (public LB).
- [ ] Deploy `admin-dashboard-v3` + auth-db + analytics-db.
- [ ] Deploy `explorer-v3`.
- [ ] Build the production observability stack (see "Production
      Observability Stack (to build)"): `alloy-v3`, `victoriametrics-v3`,
      `loki-v3`, `grafana-v3`; author `railway/alloy/config.alloy`; add the
      in-TEE `alloy` sidecar to `compose.phala.yml` (decided push-out
      transport, bearer auth); inject `OTEL_EXPORTER_OTLP_*` into all
      services; provision Grafana datasources + dashboards.
- [ ] Publicly expose and lock down the existing HyperUnit SOCKS5 proxy
      (no `ALLOWED_IPS`).
- [ ] Reuse the existing Railway `sauron-bitcoin-rathole-broker` for v3
      Bitcoin transport.
- [ ] Smoke test:
  - Phala router API `/status`
  - primary DB migration
  - Temporal namespace bootstrap
  - temporal-worker starts and polls the order-execution task queue
  - router-worker starts funded-order workflows
  - Railway physical standby replay health
  - Sauron CDC slot/checkpoint health
  - Sauron startup and watch sync
  - all indexers reachable from Sauron
  - all receipt watchers reachable from Sauron
  - HL shim indexer reachable from Sauron
  - router-gateway reachable from public internet
  - admin-dashboard CDC backfill + live ingest
  - Sauron hint submission to Phala router API
  - log shipping via alloy-v3 → loki-v3; metrics scrape → victoriametrics-v3;
    grafana-v3 dashboards render
- [ ] Run a dust-sized live route only after the smoke checks pass.
- [ ] Replay the 10k loadgen-fast scenario that originally surfaced the
      stall-class bug; verify Sauron architectural fixes hold at scale.
