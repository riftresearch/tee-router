# Sauron State Migrations

These migrations target Sauron's writable local state database.

Do not put router read-model tables, replica triggers, or `LISTEN/NOTIFY`
objects here. In the physical-standby deployment this database is separate from
the read-only router standby and stores only Sauron-owned runtime state such as
detector cursors and CDC checkpoints.
