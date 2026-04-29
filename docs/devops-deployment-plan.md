# DevOps Deployment Plan

This document captures the intended alpha deployment shape before implementing
the deployment files and scripts.

## Target Runtime Split

### Phala Cloud

Phala runs the security-critical router execution stack:

- `router-postgres-primary`
- `router-master-key-init`
- `router-api`
- `router-worker`

`router-api` and `router-worker` are separate binaries from the same
`router-server` package. They should be deployed as separate compose services,
using the same Docker image with different commands.

### Railway

Railway runs observer and supporting infrastructure:

- `router-replica-stunnel-v3`
- `router-physical-standby-v3`
- `sauron-state-db-v3`
- `sauron-worker-v3`
- `evm-token-indexer-ethereum-v3`
- `evm-token-indexer-base-v3`
- `evm-token-indexer-arbitrum-v3`
- existing `hyperunit-socks5-proxy`, configured for public authenticated TCP
  access
- Bitcoin RPC/ZMQ transport if Bitcoin source deposits are enabled

The Railway dashboard may visually group these services under a `rift v3`
canvas group, but the group metadata is not required by runtime config and is
not currently visible through the Railway CLI.

Do not reuse or modify existing Railway services for v3 unless explicitly
called out here. The current exception is the existing `hyperunit-socks5-proxy`,
which should be made publicly reachable for the Phala router-worker.

## Router API And Worker

The router API and worker should live in the singular Phala compose file.

Expected service shape:

```yaml
router-api:
  image: ghcr.io/riftresearch/tee-router:<semver-or-sha-tag>
  command: ["router-api", "--host", "0.0.0.0", "--port", "4522"]

router-worker:
  image: ghcr.io/riftresearch/tee-router:<semver-or-sha-tag>
  command: ["router-worker"]
```

Both services use the same primary Postgres instance inside the Phala compose
network.

`router-api` is exposed through a direct Phala public endpoint. Sauron receives
that public URL as `ROUTER_INTERNAL_BASE_URL` and posts detector/provider hints
to the router API over the internet. Internal hint endpoints must be protected
with `ROUTER_DETECTOR_API_KEY`.

`router-worker` should not expose a public endpoint.

Phala public endpoints are generated from literal Docker Compose `ports:`
entries. Keep the externally reachable router API mapping literal:

```yaml
ports:
  - "4522:4522"
```

Do not rely on environment-variable interpolation for Phala port declarations;
the dashboard/API may fail to materialize public endpoints from interpolated
port values.

## Container Image Strategy

Build a public GHCR Docker image and push it from GitHub Actions.

Image target:

```text
ghcr.io/riftresearch/tee-router
```

The GHCR package should be public so Phala can pull immutable deploy tags
without registry credentials.

The router image should contain:

- `/usr/local/bin/router-api`
- `/usr/local/bin/router-worker`
- router-server migrations
- CA certificates
- any small entrypoint/helper scripts needed for deployment

Every pushed image should get a `sha-<git-sha>` tag. Semver release tags should
be produced from git tags of the form `vX.Y.Z`; the workflow should also emit
`X.Y.Z`. The `vX.Y.Z` git tag must match the workspace version in `Cargo.toml`.

Phala can deploy either a semver tag for human readability or a SHA tag for the
exact source revision. For alpha, prefer pinning a semver release tag after the
matching SHA has been smoke tested.

## Primary Postgres In Phala

The router primary database lives inside the Phala compose file.

The old `etc/compose.phala.yml` already contains the pattern we want to adapt:

- primary Postgres runs in the compose stack
- app DB password is generated once into a persistent volume
- Postgres is configured with `wal_level = logical`
- replication user is created during init
- publication is created for all router tables while the old logical subscriber
  exists
- physical replication slots are allowed for the Railway standby
- `pg_hba.conf` allows the replication user to connect through the controlled
  replication path
- the primary Postgres port is exposed as a literal Compose port so Phala can
  create the DB gateway endpoint for Railway replica setup
- `pg_hba.conf` must not expose the router application DB user over the public
  DB gateway; route public DB traffic through a dedicated replication proxy
  network and keep that network limited to the read-only logical replication
  user

Required primary config:

```conf
listen_addresses = '*'
password_encryption = 'scram-sha-256'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
max_logical_replication_workers = 4
max_sync_workers_per_subscription = 2
hot_standby = on
```

The old names should be replaced with router-specific names where practical:

- database: `router_db`
- app user: `router_app`
- replication user: `replicator`
- publication: `router_all_tables`
- subscription: `router_subscription`

The Phala compose stack should use separate fixed private subnets so Postgres
can distinguish internal router API/worker traffic from public gateway traffic:

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

The public `5432` endpoint should not be published from the Postgres container
itself. Publish it from a small TCP forwarding sidecar attached only to
`router-replication-network`, then restrict `router_app` to
`router-network` and `replicator` to `router-replication-network` in
`pg_hba.conf`.

## Railway Read Replica

Sauron should not connect directly to the Phala primary database. Instead,
Railway should run a read replica that subscribes to the Phala primary via
logical replication.

The old `etc/compose.replica.yml` already contains the basic model:

1. `router-replica-stunnel-v3` connects to the Phala DB SNI over `:443`.
2. `router-physical-standby-v3` runs on Railway from
   `railway/router-physical-standby/`.
3. The standby initializes itself with `pg_basebackup`.
4. The standby uses a physical replication slot on the Phala primary.
5. Sauron and the admin dashboard run heavy reads against the standby.
6. Sauron consumes a logical decoding slot on the standby for watch-set events.

Sauron connects to the Railway physical standby via
`ROUTER_REPLICA_DATABASE_URL`.

The physical standby is read-only. Sauron-local tables must live in
`sauron-state-db-v3`, wired through `SAURON_STATE_DATABASE_URL`. This state DB
stores detector cursors and CDC checkpoints only; router-owned schema and data
derive from the primary through the physical standby.

The new Phala DB endpoint shape is expected to match the old `PHALA_DB_SNI`
model, but the exact new SNI value will not be known until the Phala stack is
created.

The `stunnel-v3` service lives on Railway, not inside the Phala compose stack.
Phala exposes the primary Postgres TCP port and the gateway performs TLS
termination before forwarding plaintext TCP to Postgres. Railway's `stunnel-v3`
performs the client-side TLS-to-TCP conversion before `replica-setup` connects.

The repo contains Railway-specific helper images for this path:

- `railway/router-replica-stunnel/` runs the client-side stunnel sidecar.
- `railway/router-physical-standby/` runs the physical standby bootstrap and
  Postgres process.

`railway/router-replica-setup/` is the legacy logical-replica setup image and
should only be kept while the old subscriber is still being retired.

Deploy these from the repo root so their Dockerfile `COPY` paths resolve
against the repository layout.

## Router Master Key

Router-derived vault addresses depend on a stable 64-byte router master key.

The app currently accepts a file path via `ROUTER_MASTER_KEY_PATH`, not a raw
environment variable. The Phala compose stack should include a one-shot
`router-master-key-init` service that prepares this file before `router-api`
and `router-worker` start.

The init service should use a shared persistent volume mounted by all router
services:

```text
router-master-key-init -> /run/router-secrets
router-api             -> /run/router-secrets
router-worker          -> /run/router-secrets
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

- `ROUTER_REPLICA_DATABASE_URL` pointing at the Railway replica
- `SAURON_STATE_DATABASE_URL` pointing at `sauron-state-db-v3`
- `SAURON_REPLICA_EVENT_SOURCE=cdc`
- `SAURON_CDC_SLOT_NAME=sauron_watch_cdc`
- `SAURON_CDC_PLUGIN=test_decoding`
- `ROUTER_REPLICA_DATABASE_NAME`
- `ROUTER_INTERNAL_BASE_URL` pointing at the public Phala router API URL
- `ROUTER_DETECTOR_API_KEY`
- EVM RPC URLs
- EVM token indexer URLs
- Bitcoin RPC/ZMQ/Esplora config if Bitcoin source deposits are enabled

Sauron posts non-authoritative hints to the router API. Router-worker still
validates provider and chain state before executing state transitions.

Local tests may still use `SAURON_REPLICA_EVENT_SOURCE=notify` against a
writable logical replica. Production physical-standby deployments should use
`cdc`; do not run replica-local `LISTEN/NOTIFY` trigger migrations against the
physical standby.

## EVM Token Indexers

All Ponder EVM token indexers stay on Railway. Create brand-new v3 services;
do not reuse existing cbBTC-specific Ponder services.

Run one indexer per EVM chain:

- Ethereum
- Base
- Arbitrum

Each indexer should have:

- its own Railway service with `-v3` suffix
- `PONDER_CHAIN_ID`
- `PONDER_RPC_URL_HTTP`
- `PONDER_WS_URL_HTTP`
- chain-specific `DATABASE_SCHEMA` / `PONDER_SCHEMA`
- `PONDER_CONTRACT_START_BLOCK`
- `PONDER_PORT=4001`
- `PORT=4001`
- raw transfer retention config

Railway injects `PORT`; Ponder honors that value for its listener, so set both
`PONDER_PORT` and `PORT` to `4001` to keep the private-network URL stable.

The v3 start block for each chain should correspond to yesterday at midnight
UTC relative to the deployment planning date: `2026-04-27T00:00:00Z`.

Use:

- Ethereum: `24967646` (`timestamp=1777247999`)
- Base: `45229327` (`timestamp=1777248001`)
- Arbitrum: `456704761` (`timestamp=1777248000`)

RPC URLs, including credentialed WebSocket URLs, must be configured as Railway
service variables and must not be committed to the repo.

Sauron should use Railway-reachable URLs:

- `ETHEREUM_TOKEN_INDEXER_URL`
- `BASE_TOKEN_INDEXER_URL`
- `ARBITRUM_TOKEN_INDEXER_URL`

Because Sauron is also on Railway, these can use Railway private networking
where possible.

## HyperUnit SOCKS5 Proxy

The existing HyperUnit SOCKS5 proxy remains on Railway, but it must be publicly
reachable by the Phala router-worker.

Requirements:

- Railway TCP proxy reachable from Phala
- username/password auth
- long random password
- destination allowlist restricted to `api.hyperunit.xyz`
- no unauthenticated open-proxy behavior
- no `ALLOWED_IPS` restriction for now
- generate a fresh random proxy password during deployment and store it only as
  Railway/Phala secret material

Router-worker should use:

```text
HYPERUNIT_PROXY_URL=socks5://<user>:<password>@<public-proxy-host>:<port>
```

The router currently expects `socks5://`, not `socks5h://`.

## Bitcoin RPC/ZMQ

Bitcoin RPC/ZMQ is still needed if Bitcoin is supported as a source asset.

For routes where Bitcoin is only the destination, Sauron does not need to
detect a Bitcoin source deposit. For `BTC -> ...` routes, Sauron must detect
deposits into Bitcoin vaults.

Current Sauron config requires:

- `BITCOIN_RPC_URL`
- `BITCOIN_RPC_AUTH`
- `BITCOIN_ZMQ_RAWTX_ENDPOINT`
- `BITCOIN_ZMQ_SEQUENCE_ENDPOINT`
- `ELECTRUM_HTTP_SERVER_URL`

Reuse the existing Railway `sauron-bitcoin-rathole-broker` for v3 Sauron rather
than creating a new Bitcoin transport service. The new `sauron-worker-v3` can
use Railway private networking:

```env
BITCOIN_RPC_URL=http://sauron-bitcoin-rathole-broker.railway.internal:40031
BITCOIN_ZMQ_RAWTX_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40032
BITCOIN_ZMQ_SEQUENCE_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40033
```

Without a code change, Sauron expects real Bitcoin config at startup.

## Public Router API Security

The public Phala router API exposes quote/order endpoints. Internal endpoints
must be protected:

- Sauron hint endpoints use `ROUTER_DETECTOR_API_KEY`.
- Admin/provider policy endpoints use `ROUTER_ADMIN_API_KEY`.

Do not add `ROUTER_PUBLIC_API_KEY`. Public quote/order endpoints remain public
for this alpha deployment unless an external edge layer is added later.

## Provider And Chain Environment

Router API and worker need the same core config:

- `DATABASE_URL`
- `ROUTER_MASTER_KEY_PATH`
- `ETH_RPC_URL`
- `BASE_RPC_URL`
- `ARBITRUM_RPC_URL`
- `BITCOIN_RPC_URL`
- `BITCOIN_RPC_AUTH`
- `ELECTRUM_HTTP_SERVER_URL`
- `ACROSS_API_URL`
- `ACROSS_API_KEY`
- `ACROSS_INTEGRATOR_ID`
- `CCTP_API_URL` if overriding Circle Iris default
- `HYPERUNIT_API_URL`
- `HYPERUNIT_PROXY_URL`
- `HYPERLIQUID_API_URL`
- `HYPERLIQUID_NETWORK`
- `VELORA_API_URL`
- `VELORA_PARTNER`
- per-chain paymaster private keys
- `CHAINALYSIS_HOST`
- `CHAINALYSIS_TOKEN`
- `ROUTER_DETECTOR_API_KEY`
- `ROUTER_ADMIN_API_KEY`
- `COINBASE_PRICE_API_BASE_URL`

## Observability

Better Stack is the alpha observability backend. The source is:

- source name: `tee-router-alpha`
- source ID: `2403167`
- platform: `open_telemetry`
- table: `tee_router_alpha`
- ingest endpoint: `https://s2403167.us-east-9.betterstackdata.com`

Do not commit the Better Stack source token. It must be supplied as
`BETTERSTACK_SOURCE_TOKEN` in deployment-local secret storage such as
`.env.phala.prod` or Railway service variables.

The Phala stack runs an OpenTelemetry Collector sidecar. The collector receives
OTLP on private ports `4317`/`4318`, scrapes router-specific private metrics
from:

- `router-api:9100`
- `router-worker:9101`

and forwards metrics, logs, and traces to Better Stack. The current Rust
application path exports tee-router custom metrics through the existing
`metrics` crate via `metrics-exporter-prometheus`; the Prometheus endpoint is
private and is only an internal receiver for the OpenTelemetry Collector. This
does not deploy or depend on a Prometheus server.

`router-api` and `router-worker` also emit tracing events/spans to the Phala
collector when `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` is set.
The shared Rust observability helper derives `/v1/traces` and `/v1/logs` from
that base endpoint.

Sauron should get the same pattern on Railway:

- set `METRICS_BIND_ADDR=0.0.0.0:9102` on `sauron-worker-v3`
- set `OTEL_EXPORTER_OTLP_ENDPOINT=http://betterstack-otel-collector-v3.railway.internal:4318`
  on `sauron-worker-v3`
- run a Railway collector service that scrapes Sauron over Railway private
  networking and exports to the same Better Stack source
- deploy the collector from `railway/betterstack-otel-collector/Dockerfile`

Railway private networking can require IPv6 wildcard listeners. The Rust
observability helper keeps the operator-facing `METRICS_BIND_ADDR=0.0.0.0:9102`
setting but binds it as `[::]:9102` when Railway runtime metadata is present, so
the private collector can reach the endpoint through `*.railway.internal`.

## Implementation Checklist

1. Add router Dockerfile that builds `router-api` and `router-worker`.
2. Add GitHub Actions image build/push workflow for public
   `ghcr.io/riftresearch/tee-router`.
3. Add Phala compose file for:
   - router primary Postgres
   - DB secret generator
   - router master key init
   - router API
   - router worker
   Use Phala-provided environment variables with `${VAR:?}` guards rather than
   hard-coded env values.
   Exposed Phala service ports must be literal values, and existing CVMs must
   be updated with Phala's `update_ports` API behavior when port mappings
   change.
4. Adapt primary Postgres config/init scripts from old `compose.phala.yml`.
5. Deploy Railway `router-physical-standby-v3` and `sauron-state-db-v3`.
6. Verify physical standby replay and logical decoding on the standby.
7. Configure `sauron-worker-v3` on Railway against the physical standby and
   state DB.
8. Configure new Ponder token indexers on Railway with `-v3` service names.
9. Publicly expose and lock down the existing HyperUnit SOCKS5 proxy, without
   `ALLOWED_IPS`.
10. Reuse the existing Railway Bitcoin RPC/ZMQ rathole broker for v3 Sauron.
11. Smoke test:
    - Phala router API `/status`
    - primary DB migration
    - router-worker lease acquisition
    - Railway physical standby replay health
    - Sauron CDC slot/checkpoint health
    - Sauron startup and watch sync
    - Ponder indexer health
    - Sauron hint submission to Phala router API
12. Run a dust-sized live route only after the smoke checks pass.
