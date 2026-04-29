Legacy writable logical-replica migrations for `sauron`.

Use this directory for logical-subscriber / replica objects that do not belong in
the primary router schema migration stream, such as:

- `pg_notify(...)` trigger functions
- replica-side row triggers enabled for logical replication apply
- replica-local views or helper objects used only by Sauron

Do not run these migrations against a read-only physical standby. Production
physical-standby deployments should use `SAURON_REPLICA_EVENT_SOURCE=cdc` and
`bin/sauron/migrations-state` instead.

The Rust migrator for this directory lives in `src/replica_db.rs`.
