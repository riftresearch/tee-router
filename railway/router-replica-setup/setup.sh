#!/usr/bin/env bash
set -euo pipefail

echo "=== Router v3 Logical Replica Setup ==="

PRIMARY_HOST="${PRIMARY_DB_HOST:-router-replica-stunnel-v3.railway.internal}"
PRIMARY_PORT="${PRIMARY_DB_PORT:-5432}"
PRIMARY_DB="${PRIMARY_DB_NAME:-router_db}"
PRIMARY_USER="${PRIMARY_DB_USER:-replicator}"
PRIMARY_PASS="${PRIMARY_DB_PASSWORD:?PRIMARY_DB_PASSWORD must be set}"
PRIMARY_SSLMODE="${PRIMARY_DB_SSLMODE:-disable}"
PRIMARY_PUBLICATION="${PRIMARY_PUBLICATION:-router_all_tables}"

REPLICA_HOST="${REPLICA_DB_HOST:-router-replica-db-v3.railway.internal}"
REPLICA_PORT="${REPLICA_DB_PORT:-5432}"
REPLICA_DB="${REPLICA_DB_NAME:-router_db}"
REPLICA_USER="${REPLICA_DB_USER:-postgres}"
REPLICA_PASS="${REPLICA_DB_PASSWORD:?REPLICA_DB_PASSWORD must be set}"
REPLICA_SSLMODE="${REPLICA_DB_SSLMODE:-prefer}"
REPLICA_SUBSCRIPTION="${REPLICA_SUBSCRIPTION:-router_subscription}"

PRIMARY_CONN="host=${PRIMARY_HOST} port=${PRIMARY_PORT} dbname=${PRIMARY_DB} user=${PRIMARY_USER} sslmode=${PRIMARY_SSLMODE}"
REPLICA_ADMIN_CONN="host=${REPLICA_HOST} port=${REPLICA_PORT} dbname=postgres user=${REPLICA_USER} sslmode=${REPLICA_SSLMODE}"

echo "Waiting for Phala primary router database..."
until PGPASSWORD="${PRIMARY_PASS}" psql "${PRIMARY_CONN}" -c '\q' 2>/dev/null; do
  echo "primary unavailable; retrying in 2s"
  sleep 2
done
echo "Primary is reachable"

echo "Waiting for Railway replica database..."
until PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_ADMIN_CONN}" -c '\q' 2>/dev/null; do
  echo "replica unavailable; retrying in 2s"
  sleep 2
done
echo "Replica is reachable"

echo "Ensuring replica database exists"
PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_ADMIN_CONN}" <<-SQL
  SELECT 'CREATE DATABASE ${REPLICA_DB}'
  WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${REPLICA_DB}')\gexec
SQL

REPLICA_DB_CONN="host=${REPLICA_HOST} port=${REPLICA_PORT} dbname=${REPLICA_DB} user=${REPLICA_USER} sslmode=${REPLICA_SSLMODE}"

echo "Checking primary publication"
PUB_EXISTS="$(PGPASSWORD="${PRIMARY_PASS}" psql "${PRIMARY_CONN}" -tAc "SELECT COUNT(*) FROM pg_publication WHERE pubname = '${PRIMARY_PUBLICATION}';")"
if [ "${PUB_EXISTS}" -ne "1" ]; then
  echo "ERROR: primary publication '${PRIMARY_PUBLICATION}' was not found" >&2
  exit 1
fi

echo "Copying or synchronizing primary schema"
TABLES_EXIST="$(PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_DB_CONN}" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name IN ('router_orders', 'deposit_vaults', 'market_order_quotes');")"

if [ "${TABLES_EXIST}" -eq "3" ]; then
  echo "Core router tables already exist; skipping schema copy"
else
  echo "Copying full primary schema"
  PGPASSWORD="${PRIMARY_PASS}" pg_dump "${PRIMARY_CONN}" --schema-only --no-owner --no-privileges | \
    PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_DB_CONN}"
fi

echo "Creating logical subscription if missing"
SUB_EXISTS="$(PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_DB_CONN}" -tAc "SELECT COUNT(*) FROM pg_subscription WHERE subname = '${REPLICA_SUBSCRIPTION}';")"

if [ "${SUB_EXISTS}" -eq "0" ]; then
  SLOT_EXISTS="$(PGPASSWORD="${PRIMARY_PASS}" psql "${PRIMARY_CONN}" -tAc "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '${REPLICA_SUBSCRIPTION}';")"
  if [ "${SLOT_EXISTS}" -eq "1" ]; then
    echo "Dropping orphaned primary replication slot '${REPLICA_SUBSCRIPTION}'"
    PGPASSWORD="${PRIMARY_PASS}" psql "${PRIMARY_CONN}" -c "SELECT pg_drop_replication_slot('${REPLICA_SUBSCRIPTION}');"
  fi

  PRIMARY_PASS_SQL="${PRIMARY_PASS//\'/\'\'}"
  CONNECTION_STRING="host=${PRIMARY_HOST} port=${PRIMARY_PORT} dbname=${PRIMARY_DB} user=${PRIMARY_USER} password=${PRIMARY_PASS_SQL} sslmode=${PRIMARY_SSLMODE}"
  PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_DB_CONN}" <<-SQL
    CREATE SUBSCRIPTION ${REPLICA_SUBSCRIPTION}
      CONNECTION '${CONNECTION_STRING}'
      PUBLICATION ${PRIMARY_PUBLICATION}
      WITH (copy_data = true, create_slot = true);
SQL
  echo "Subscription '${REPLICA_SUBSCRIPTION}' created"
else
  echo "Subscription '${REPLICA_SUBSCRIPTION}' already exists"
fi

echo "=== Replica Status ==="
PGPASSWORD="${REPLICA_PASS}" psql "${REPLICA_DB_CONN}" -c "SELECT subname, subenabled, subslotname FROM pg_subscription WHERE subname = '${REPLICA_SUBSCRIPTION}';"
echo "Router v3 logical replica setup complete"
