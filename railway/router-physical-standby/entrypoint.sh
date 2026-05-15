#!/usr/bin/env bash
set -euo pipefail

if [ "$(id -u)" = "0" ]; then
  mkdir -p "${PGDATA:-/var/lib/postgresql/data}"
  chown -R postgres:postgres /var/lib/postgresql
  exec gosu postgres "$BASH_SOURCE" "$@"
fi

: "${PRIMARY_DB_PASSWORD:?PRIMARY_DB_PASSWORD must be set}"

PRIMARY_DB_HOST="${PRIMARY_DB_HOST:-router-replica-stunnel-v3.railway.internal}"
PRIMARY_DB_PORT="${PRIMARY_DB_PORT:-5432}"
PRIMARY_DB_NAME="${PRIMARY_DB_NAME:-router_db}"
PRIMARY_DB_USER="${PRIMARY_DB_USER:-replicator}"
PRIMARY_DB_SSLMODE="${PRIMARY_DB_SSLMODE:-disable}"
PRIMARY_REPLICATION_SLOT="${PRIMARY_REPLICATION_SLOT:-router_physical_standby_v3}"
STANDBY_APPLICATION_NAME="${STANDBY_APPLICATION_NAME:-router-physical-standby-v3}"
STANDBY_MAX_CONNECTIONS="${STANDBY_MAX_CONNECTIONS:-256}"
PGDATA="${PGDATA:-/var/lib/postgresql/18/docker}"

if [[ ! "${PRIMARY_REPLICATION_SLOT}" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
  echo "PRIMARY_REPLICATION_SLOT must be a valid Postgres identifier" >&2
  exit 1
fi

PRIMARY_CONNINFO="host=${PRIMARY_DB_HOST} port=${PRIMARY_DB_PORT} dbname=${PRIMARY_DB_NAME} user=${PRIMARY_DB_USER} password=${PRIMARY_DB_PASSWORD} application_name=${STANDBY_APPLICATION_NAME} sslmode=${PRIMARY_DB_SSLMODE}"

write_standby_config() {
  local config="${PGDATA}/postgresql.auto.conf"
  local tmp="${config}.tmp"
  touch "${config}"
  grep -vE '^(hot_standby|hot_standby_feedback|wal_level|max_connections|max_wal_senders|max_replication_slots) =' "${config}" >"${tmp}" || true
  cat "${tmp}" >"${config}"
  rm -f "${tmp}"
  cat >>"${config}" <<-CONF
	hot_standby = on
	hot_standby_feedback = on
	wal_level = logical
	max_connections = ${STANDBY_MAX_CONNECTIONS}
	max_wal_senders = 10
	max_replication_slots = 10
	CONF
}

write_standby_hba() {
  local hba="${PGDATA}/pg_hba.conf"
  if ! grep -q "router-physical-standby-v3 railway private readers" "${hba}"; then
    cat >>"${hba}" <<-HBA

	# router-physical-standby-v3 railway private readers
	host all all all scram-sha-256
	host replication all all scram-sha-256
	HBA
  fi
}

echo "=== Router v3 Physical Standby ==="
echo "primary=${PRIMARY_DB_HOST}:${PRIMARY_DB_PORT}/${PRIMARY_DB_NAME}"
echo "slot=${PRIMARY_REPLICATION_SLOT}"

export PGPASSWORD="${PRIMARY_DB_PASSWORD}"
until psql "${PRIMARY_CONNINFO}" -c '\q' 2>/dev/null; do
  echo "primary unavailable; retrying in 2s"
  sleep 2
done
echo "Primary is reachable"

if [ ! -s "${PGDATA}/PG_VERSION" ]; then
  echo "Initializing standby data directory with pg_basebackup"
  rm -rf "${PGDATA:?}"/*

  SLOT_EXISTS="$(psql "${PRIMARY_CONNINFO}" -tAc "SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name = '${PRIMARY_REPLICATION_SLOT}';")"
  if [ "${SLOT_EXISTS}" -eq "0" ]; then
    psql "${PRIMARY_CONNINFO}" -c "SELECT pg_create_physical_replication_slot('${PRIMARY_REPLICATION_SLOT}');"
    echo "Created physical replication slot '${PRIMARY_REPLICATION_SLOT}'"
  else
    echo "Physical replication slot '${PRIMARY_REPLICATION_SLOT}' already exists"
  fi

  pg_basebackup \
    --pgdata="${PGDATA}" \
    --format=plain \
    --checkpoint=fast \
    --wal-method=stream \
    --write-recovery-conf \
    --slot="${PRIMARY_REPLICATION_SLOT}" \
    --dbname="${PRIMARY_CONNINFO}"

  chmod 700 "${PGDATA}"
  echo "Standby basebackup complete"
else
  echo "Existing standby data directory found"
fi

write_standby_config
write_standby_hba

exec docker-entrypoint.sh "$@"
