# Physical Replica + Standby CDC Plan

## Goal

Use a physical Postgres standby on Railway so the replica derives schema and
data directly from the Phala primary WAL stream. Use standby-side CDC decoding
for event delivery to Sauron and the admin dashboard.

## Target Architecture

```text
Phala router Postgres primary
        |
        | physical streaming replication
        v
Railway physical standby Postgres
        |
        | CDC decoding slot on standby
        v
Sauron / admin-dashboard CDC consumers
```

## Why This Model

- Physical replication naturally follows primary DDL, data, migration metadata,
  indexes, constraints, and extension state.
- The replica no longer needs router schema migrations or schema-diff repair.
- Removed subscriber drift cannot wedge replication when primary schema changes.
- Heavy reads stay on Railway instead of the Phala primary.
- CDC still gives Sauron and the admin dashboard an event source without polling
  the router API or worker.

## Constraints

- Physical replication requires the same Postgres major version on primary and
  standby. The router stack should standardize on Postgres 18 before switching.
- A physical standby is read-only. Replica-local triggers, `LISTEN/NOTIFY`,
  Sauron cursor tables, and admin local tables cannot live on the standby.
- CDC decoding on a standby requires Postgres 16+ and careful WAL/slot lag
  monitoring.
- CDC consumers must track confirmed LSNs durably and be idempotent across
  restarts.

## Service Shape

- `router-postgres-primary`
  - Runs in Phala.
  - Postgres 18.
  - `wal_level = logical` so the physical standby can support CDC decoding.
  - Exposes only the controlled replication path.

- `router-physical-standby-v3`
  - Runs in Railway.
  - Postgres 18.
  - Initialized from the Phala primary with `pg_basebackup`.
  - Uses a physical replication slot on the primary.
  - Read-only query source for Sauron and admin dashboard heavy reads.

- `sauron-state-db-v3`
  - Runs in Railway.
  - Writable state DB for Sauron cursors, CDC LSN checkpoints, and local runtime
    state.

- `sauron-worker-v3`
  - Reads active watch/order state from the physical standby.
  - Consumes a CDC stream from the physical standby.
  - Writes only to `sauron-state-db-v3` for local state.
  - Sends validated hints to router API.

- `admin-dashboard-api-v3`
  - Reads summaries and detailed flows from the physical standby.
  - Uses CDC-derived events or a local fanout queue for SSE.

## Migration Path

1. Standardize all local and deployed router Postgres images on Postgres 18.
2. Prove local primary/physical-standby/standby-CDC behavior.
3. Deploy a new Railway Postgres 18 physical standby.
4. Add a writable `sauron-state-db-v3`.
5. Add CDC mode to Sauron with durable LSN checkpointing.
6. Move Sauron reads to the physical standby and local state writes to
   `sauron-state-db-v3`.
7. Build the admin dashboard backend against the physical standby.
8. Keep Sauron and admin dashboard reads on the physical standby.

## Implemented Repo Pieces

- `railway/router-physical-standby/`
  - Postgres 18 image that waits for the Phala primary, creates/uses a physical
    replication slot, initializes with `pg_basebackup`, and starts as a hot
    standby with CDC decoding enabled.
- `etc/compose.physical-replica.yml`
  - Local/Railway-shaped compose file for `router-replica-stunnel-v3`,
    `router-physical-standby-v3`, and `sauron-state-db-v3`.
- `bin/sauron/migrations-state/`
  - Sauron-owned writable state schema for detector cursors and CDC
    checkpoints.
- `bin/sauron/src/state_db.rs`
  - Migrator for the writable Sauron state DB.
- `bin/sauron/src/cdc.rs`
  - CDC consumer support using a standby-side `pgoutput` slot.
- `bin/sauron/src/runtime.rs`
  - Split router read pool from Sauron state pool.
  - Uses CDC for physical-standby production.

## Production Sauron Env

```text
ROUTER_REPLICA_DATABASE_URL=postgres://...@router-physical-standby-v3.railway.internal:5432/router_db
SAURON_STATE_DATABASE_URL=postgres://...@sauron-state-db-v3.railway.internal:5432/sauron_state
SAURON_REPLICA_EVENT_SOURCE=cdc
SAURON_CDC_SLOT_NAME=sauron_watch_cdc
SAURON_CDC_STATUS_INTERVAL_MS=1000
SAURON_CDC_IDLE_WAKEUP_INTERVAL_MS=10000
```

In `notify` mode, Sauron still runs the legacy replica migrations that install
replica-local `LISTEN/NOTIFY` triggers. In `cdc` mode, those migrations are not
run; the router replica can be read-only and all Sauron writes go to the state
DB.

## Local Smoke Result

Verified with Docker:

1. Started a Postgres 18 primary with CDC-capable WAL.
2. Started `tee-router-physical-standby:local`.
3. Confirmed the standby reported `pg_is_in_recovery() = true`.
4. Created a `pgoutput` CDC slot on the standby.
5. Inserted on the primary and consumed the decoded change from the standby.

## Operational Checks

- Primary and standby Postgres major versions match.
- Physical slot exists and retained WAL is bounded.
- Standby replay lag is below alert threshold.
- Standby CDC decoding slot exists and CDC lag is below alert threshold.
- Sauron CDC consumer checkpoint advances.
- Admin dashboard read queries never connect to the Phala primary.
