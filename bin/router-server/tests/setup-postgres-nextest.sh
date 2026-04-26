#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${NEXTEST_ENV:-}" ]]; then
    echo "NEXTEST_ENV must be set by cargo-nextest" >&2
    exit 1
fi

if [[ -n "${ROUTER_TEST_DATABASE_URL:-}" ]]; then
    echo "ROUTER_TEST_DATABASE_URL=${ROUTER_TEST_DATABASE_URL}" >>"${NEXTEST_ENV}"
    exit 0
fi

run_id="${NEXTEST_RUN_ID:-manual-$$}"
container_name="tee-router-nextest-postgres-${run_id//[^a-zA-Z0-9_.-]/-}"
image="${ROUTER_TEST_POSTGRES_IMAGE:-postgres:15-alpine}"
user="postgres"
password="password"
database="postgres"

docker rm -f "${container_name}" >/dev/null 2>&1 || true

docker run \
    --detach \
    --rm \
    --name "${container_name}" \
    --label "org.rift.tee-router.nextest-run=${run_id}" \
    --env "POSTGRES_USER=${user}" \
    --env "POSTGRES_PASSWORD=${password}" \
    --env "POSTGRES_DB=${database}" \
    --publish "127.0.0.1::5432" \
    "${image}" >/dev/null

nextest_pid="${PPID}"
(
    while kill -0 "${nextest_pid}" 2>/dev/null; do
        sleep 1
    done

    docker rm -f "${container_name}" >/dev/null 2>&1 || true
) </dev/null >/dev/null 2>&1 &

port=""
for _ in {1..120}; do
    port="$(docker port "${container_name}" 5432/tcp 2>/dev/null | awk -F: 'NR == 1 { print $NF }')"
    if [[ -n "${port}" ]] && docker exec "${container_name}" pg_isready -h 127.0.0.1 -U "${user}" -d "${database}" >/dev/null 2>&1; then
        break
    fi
    sleep 0.5
done

if [[ -z "${port}" ]] || ! docker exec "${container_name}" pg_isready -h 127.0.0.1 -U "${user}" -d "${database}" >/dev/null 2>&1; then
    docker logs "${container_name}" >&2 || true
    docker rm -f "${container_name}" >/dev/null 2>&1 || true
    echo "Postgres test container did not become ready" >&2
    exit 1
fi

{
    echo "ROUTER_TEST_DATABASE_URL=postgres://${user}:${password}@127.0.0.1:${port}/${database}"
    echo "ROUTER_TEST_SHARED_POSTGRES_CONTAINER=${container_name}"
} >>"${NEXTEST_ENV}"
