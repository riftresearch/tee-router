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
- `temporal-postgres`
- `temporal-schema` (one-shot)
- `temporal` — Temporal server, metrics on `:9090`
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
- `betterstack-otel-collector-v3` — OTLP log collector that forwards to
  Better Stack
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
| temporal-postgres | Phala | `postgres:16-alpine` | Temporal workflow history |
| temporal | Phala | `temporalio/server:1.31.0` | Workflow engine |
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
| betterstack-otel-collector-v3 | Railway | `railway/betterstack-otel-collector/` | OTLP logs → Better Stack |
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

### Logs

Each Rust service emits structured logs via `tracing::info!/warn!/error!`.
`opentelemetry-appender-tracing` bridges these into the OTLP logs export
path. Destinations:

- **Local**: services emit OTLP logs to `alloy`, which forwards to Loki.
  Grafana queries Loki for the log viewer.
- **Production**: services emit OTLP logs to `betterstack-otel-collector-v3`
  on Railway, which forwards to Better Stack (formerly Logtail).

The OTLP log export is disabled unless `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is
set by the deployment. There is no trace exporter.

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

**Production metrics destination**: TBD. The current options:

1. Run VictoriaMetrics on Railway and have Sauron / indexers / receipt
   watchers ship metrics to it via Alloy or a remote_write client.
2. Use Better Stack metrics ingestion (some plans support it).
3. Defer metrics in prod until needed.

This needs a decision before the alpha cutover.

### IPv6 listener note

Railway private networking can require IPv6 wildcard listeners. The Rust
observability helper keeps the operator-facing `METRICS_BIND_ADDR=0.0.0.0:9102`
setting but binds it as `[::]:9102` when Railway runtime metadata is present,
so private scrapers can reach the endpoint through `*.railway.internal`.

## Implementation Checklist

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
- [ ] Deploy `betterstack-otel-collector-v3` and point all services'
      `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` at it.
- [ ] Decide and implement the production metrics destination.
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
  - log shipping via betterstack-otel-collector
- [ ] Run a dust-sized live route only after the smoke checks pass.
- [ ] Replay the 10k loadgen-fast scenario that originally surfaced the
      stall-class bug; verify Sauron architectural fixes hold at scale.
