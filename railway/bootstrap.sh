#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# railway/bootstrap.sh
#
# Idempotent bootstrap of the ENTIRE Railway-side topology for tee-router v3
# into the EXISTING `tee-router` Railway project. Services are added/
# reconciled; the project itself is never created and existing unrelated
# services are left untouched.
#
# This script + the per-service railway/<svc>/railway.json files + the
# Dockerfiles are the reproducible "infra as code". Railway has no
# whole-topology manifest and no official Terraform provider, so topology
# creation lives here.
#
# Safe to re-run: every step checks for existence before creating and
# applies config idempotently.
#
# Requirements: railway CLI >= 4.59, authenticated (`railway login`), and
# the GitHub repo riftresearch/tee-router connected to the Railway account
# (GitHub App installed) so repo-sourced services can be created.
#
# Lines marked `# VERIFY:` use a CLI surface that can shift between CLI
# versions — confirm against `railway <cmd> --help` before a real run.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO="riftresearch/tee-router"
BRANCH="${DEPLOY_BRANCH:-main}"
PROJECT_NAME="${RAILWAY_PROJECT_NAME:-tee-router}"
ENVIRONMENT="${RAILWAY_ENVIRONMENT:-production}"
ENV_DIR="${RAILWAY_ENV_DIR:-railway/env}"

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap:warn]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[bootstrap:err]\033[0m %s\n' "$*" >&2; exit 1; }
retry() {
  local attempt=1 max=4
  until "$@"; do
    if [ "$attempt" -ge "$max" ]; then
      return 1
    fi
    warn "command failed; retrying ($attempt/$max)"
    sleep $((attempt * 3))
    attempt=$((attempt + 1))
  done
}

command -v railway >/dev/null || die "railway CLI not found"
command -v python3 >/dev/null || die "python3 not found"
command -v curl >/dev/null || die "curl not found"
command -v timeout >/dev/null || die "timeout not found"
railway whoami >/dev/null 2>&1 || die "not authenticated; run: railway login"

# --- Project -----------------------------------------------------------------
# Target the EXISTING project/environment. Never create it; fail loudly if
# absent so we can't accidentally scatter services into the wrong project.
log "linking existing project ${PROJECT_NAME}, environment ${ENVIRONMENT}"
railway link --project "${PROJECT_NAME}" --environment "${ENVIRONMENT}" \
  || die "project '${PROJECT_NAME}' / environment '${ENVIRONMENT}' not found or not linkable. This script only deploys into the existing project; it will not create one."
PROJECT_ID="$(railway status --json | python3 -c 'import sys,json; data=json.load(sys.stdin); print(data.get("id") or data.get("project",{}).get("id",""))')"
[ -n "${PROJECT_ID}" ] || die "could not resolve project id for ${PROJECT_NAME}"
log "project id ${PROJECT_ID}, environment ${ENVIRONMENT}"

SERVICE_NAMES=""
refresh_service_names() {
  SERVICE_NAMES="$(railway service list --environment "${ENVIRONMENT}" --json \
    | python3 -c 'import json,sys; print("\n".join(s.get("name","") for s in json.load(sys.stdin)))')"
}
svc_exists() { printf '%s\n' "${SERVICE_NAMES}" | grep -Fxq "$1"; }
remember_svc() { SERVICE_NAMES="$(printf '%s\n%s\n' "${SERVICE_NAMES}" "$1" | sed '/^$/d')"; }
list_service_ids() {
  railway service list --environment "${ENVIRONMENT}" --json \
    | python3 -c 'import json,sys; print("\n".join(s.get("id","") for s in json.load(sys.stdin) if s.get("id")))'
}
service_id_by_name() {  # <service-name>
  railway service list --environment "${ENVIRONMENT}" --json \
    | SERVICE_NAME="$1" python3 -c 'import json, os, sys
wanted = os.environ["SERVICE_NAME"]
for service in json.load(sys.stdin):
    if service.get("name") == wanted:
        print(service.get("id", ""))
        raise SystemExit
'
}
environment_id_by_name() {  # <environment-name>
  railway environment list --json \
    | ENVIRONMENT_NAME="$1" python3 -c 'import json, os, sys
wanted = os.environ["ENVIRONMENT_NAME"]
for env in (json.load(sys.stdin).get("environments") or []):
    if env.get("name") == wanted:
        print(env.get("id", ""))
        raise SystemExit
'
}

refresh_service_names

# Apply a railway.json-equivalent config patch (authoritative; the in-repo
# railway/<svc>/railway.json is the portable record of the same settings).
apply_service_cfg() {  # <service> <service-config-json>
  local svc="$1" config="$2" service_id patch attempt max
  service_id="$(service_id_by_name "$svc")"
  [ -n "$service_id" ] || die "could not resolve Railway service id for $svc"
  patch="$(SERVICE_ID="$service_id" SERVICE_CONFIG="$config" python3 - <<'PY'
import json
import os

print(json.dumps({
    "services": {
        os.environ["SERVICE_ID"]: json.loads(os.environ["SERVICE_CONFIG"]),
    },
}))
PY
)"

  attempt=1
  max=4
  until printf '%s\n' "$patch" | timeout 75s railway environment edit --environment "$ENVIRONMENT" --json >/dev/null; do
    if [ "$attempt" -ge "$max" ]; then
      return 1
    fi
    warn "service config patch for $svc failed; retrying ($attempt/$max)"
    sleep $((attempt * 3))
    attempt=$((attempt + 1))
  done
}

apply_repo_cfg() {  # <service> <dockerfilePath> <watchPatterns-json>
  local svc="$1" dockerfile="$2" watch="$3" config
  config="$(REPO="$REPO" BRANCH="$BRANCH" DOCKERFILE="$dockerfile" WATCH="$watch" python3 - <<'PY'
import json
import os

print(json.dumps({
    "source": {
        "repo": os.environ["REPO"],
        "branch": os.environ["BRANCH"],
    },
    "build": {
        "builder": "DOCKERFILE",
        "dockerfilePath": os.environ["DOCKERFILE"],
        "watchPatterns": json.loads(os.environ["WATCH"]),
    },
    "deploy": {
        "restartPolicyType": "ON_FAILURE",
    },
}))
PY
)"
  apply_service_cfg "$svc" "$config"
}

set_var() { retry timeout 75s railway variable set "$2" --service "$1" --environment "$ENVIRONMENT" >/dev/null; }

railway_token() {
  python3 - <<'PY'
import json
import os
from pathlib import Path

path = Path.home() / ".railway" / "config.json"
if not path.exists():
    raise SystemExit
data = json.loads(path.read_text())
user = data.get("user") or {}
print(user.get("token") or user.get("accessToken") or "")
PY
}

railway_api() {  # <query> <variables-json>
  local query="$1" variables="$2" token payload
  token="$(railway_token)"
  [ -n "$token" ] || die "could not read Railway API token from ~/.railway/config.json"
  payload="$(QUERY="$query" VARIABLES="$variables" python3 - <<'PY'
import json
import os

print(json.dumps({
    "query": os.environ["QUERY"],
    "variables": json.loads(os.environ["VARIABLES"]),
}))
PY
)"
  curl -fsS https://backboard.railway.com/graphql/v2 \
    -H "Authorization: Bearer ${token}" \
    -H "Content-Type: application/json" \
    -d "$payload"
}

pin_service_region() {  # <service> <region-id>
  local svc="$1" region="$2" service_id environment_id query_vars config_response multi_region variables response
  service_id="$(service_id_by_name "$svc")"
  environment_id="$(environment_id_by_name "$ENVIRONMENT")"
  [ -n "$service_id" ] || die "could not resolve Railway service id for $svc"
  [ -n "$environment_id" ] || die "could not resolve Railway environment id for $ENVIRONMENT"

  query_vars="$(ENVIRONMENT_ID="$environment_id" python3 - <<'PY'
import json
import os

print(json.dumps({"id": os.environ["ENVIRONMENT_ID"]}))
PY
)"
  config_response="$(railway_api \
    'query getEnvironmentConfig($id: String!) { environment(id: $id) { config(decryptVariables: false) } }' \
    "$query_vars")"
  multi_region="$(SERVICE_ID="$service_id" REGION="$region" RESPONSE="$config_response" python3 - <<'PY'
import json
import os

data = json.loads(os.environ["RESPONSE"])
config = data.get("data", {}).get("environment", {}).get("config") or {}
existing = (((config.get("services") or {}).get(os.environ["SERVICE_ID"]) or {}).get("deploy") or {}).get("multiRegionConfig") or {}
next_config = {key: None for key in existing}
next_config[os.environ["REGION"]] = {"numReplicas": 1}
print(json.dumps(next_config))
PY
)"
  variables="$(ENVIRONMENT_ID="$environment_id" SERVICE_ID="$service_id" MULTI_REGION="$multi_region" python3 - <<'PY'
import json
import os

print(json.dumps({
    "environmentId": os.environ["ENVIRONMENT_ID"],
    "serviceId": os.environ["SERVICE_ID"],
    "multiRegionConfig": json.loads(os.environ["MULTI_REGION"]),
}))
PY
)"
  response="$(railway_api \
    'mutation pinRegion($environmentId: String!, $serviceId: String!, $multiRegionConfig: JSON!) { serviceInstanceUpdate(environmentId: $environmentId, serviceId: $serviceId, input: { multiRegionConfig: $multiRegionConfig }) }' \
    "$variables")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE"])
if data.get("errors"):
    print(data["errors"], file=sys.stderr)
    raise SystemExit(1)
if data.get("data", {}).get("serviceInstanceUpdate") is not True:
    print("Railway serviceInstanceUpdate returned false", file=sys.stderr)
    raise SystemExit(1)
PY
}

rename_service() {  # <service-id> <name>
  local service_id="$1" name="$2" variables response
  variables="$(SERVICE_ID="$service_id" SERVICE_NAME="$name" python3 - <<'PY'
import json
import os

print(json.dumps({
    "id": os.environ["SERVICE_ID"],
    "input": {"name": os.environ["SERVICE_NAME"]},
}))
PY
)"
  response="$(railway_api \
    'mutation updateService($id: String!, $input: ServiceUpdateInput!) { serviceUpdate(id: $id, input: $input) { id name } }' \
    "$variables")"
  RESPONSE="$response" python3 - <<'PY'
import json
import os
import sys

data = json.loads(os.environ["RESPONSE"])
if data.get("errors"):
    print(data["errors"], file=sys.stderr)
    raise SystemExit(1)
updated = data.get("data", {}).get("serviceUpdate") or {}
if not updated.get("id"):
    print("Railway serviceUpdate returned no service", file=sys.stderr)
    raise SystemExit(1)
PY
}

create_managed_postgres() {  # <desired-service-name>
  local db="$1" before add_json new_id
  if svc_exists "$db"; then
    log "managed postgres $db exists"
    return 0
  fi

  log "creating managed postgres $db"
  before="$(list_service_ids)"
  add_json="$(railway add --database postgres --json)"
  new_id="$(printf '%s\n' "$add_json" | python3 -c 'import json,sys
try:
    data=json.load(sys.stdin)
except Exception:
    data={}
print(data.get("id") or data.get("service",{}).get("id",""))' || true)"
  if [ -z "$new_id" ]; then
    new_id="$(BEFORE="$before" railway service list --environment "${ENVIRONMENT}" --json \
      | python3 -c 'import json, os, sys
before=set(os.environ["BEFORE"].splitlines())
created=[s.get("id","") for s in json.load(sys.stdin) if s.get("id") and s.get("id") not in before]
print(created[0] if len(created) == 1 else "")')"
  fi
  [ -n "$new_id" ] || die "created postgres for $db but could not identify its service id"
  rename_service "$new_id" "$db"
  remember_svc "$db"
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

is_placeholder_value() {
  local value="$1"
  [ -z "$value" ] || [[ "$value" == CHANGEME* ]] || [[ "$value" == *TODO* ]] || \
    [[ "$value" == *REPLACE* ]] || [[ "$value" == *replace-with* ]] || [[ "$value" == *your-* ]]
}

iter_env_file() {  # <file> <callback-name> [callback-args...]
  local file="$1" callback="$2"
  shift 2
  [ -f "$file" ] || return 1

  local raw line key value
  while IFS= read -r raw || [ -n "$raw" ]; do
    raw="${raw%$'\r'}"
    line="$(trim "$raw")"
    [ -z "$line" ] && continue
    [[ "$line" == \#* ]] && continue
    [[ "$line" != *=* ]] && { warn "skipping malformed env line in $file: ${line%%=*}"; continue; }

    key="$(trim "${line%%=*}")"
    value="$(trim "${line#*=}")"
    key="${key#export }"
    key="$(trim "$key")"
    if [[ "$value" == \"*\" && "$value" == *\" ]]; then
      value="${value:1:${#value}-2}"
    elif [[ "$value" == \'*\' && "$value" == *\' ]]; then
      value="${value:1:${#value}-2}"
    else
      value="$(trim "${value%%[[:space:]]#*}")"
    fi
    "$callback" "$key" "$value" "$@"
  done <"$file"
}

apply_service_env_pair() {  # <key> <value> <service> <file>
  local key="$1" value="$2" svc="$3" file="$4"
  if is_placeholder_value "$value"; then
    warn "$file leaves $key unset/placeholder — not applying to $svc"
    return 0
  fi
  set_var "$svc" "${key}=${value}"
}

apply_service_env_file() {  # <service> <env-file>
  local svc="$1" file="$2"
  if [ ! -f "$file" ]; then
    if [ -f "${file}.example" ]; then
      warn "service env file missing for $svc: $file — applying non-placeholder defaults from ${file}.example"
      file="${file}.example"
    else
      warn "service env file missing for $svc: $file"
      return 0
    fi
  fi
  iter_env_file "$file" apply_service_env_pair "$svc" "$file"
}

apply_shared_env_pair() {  # <key> <value> <file>
  local key="$1" value="$2" file="$3" patch
  if is_placeholder_value "$value"; then
    warn "$file leaves shared $key unset/placeholder — not applying"
    return 0
  fi
  patch="$(KEY="$key" VALUE="$value" python3 - <<'PY'
import json
import os
print(json.dumps({"sharedVariables": {os.environ["KEY"]: {"value": os.environ["VALUE"]}}}))
PY
)"
  local attempt=1 max=4
  until printf '%s\n' "$patch" | timeout 75s railway environment edit --environment "$ENVIRONMENT" --json >/dev/null; do
    if [ "$attempt" -ge "$max" ]; then
      return 1
    fi
    warn "shared env patch for $key failed; retrying ($attempt/$max)"
    sleep $((attempt * 3))
    attempt=$((attempt + 1))
  done
}

apply_shared_env_file() {
  local file="${ENV_DIR}/_shared.env"
  if [ ! -f "$file" ]; then
    warn "shared env file missing: $file"
    return 0
  fi
  iter_env_file "$file" apply_shared_env_pair "$file"
}

apply_derived_shared_vars() {
  local file="${ENV_DIR}/_shared.env" password encoded
  if [ ! -f "$file" ]; then
    return 0
  fi
  if ! password="$(read_env_value "$file" POSTGRES_REPLICA_PASSWORD)"; then
    return 0
  fi
  if is_placeholder_value "$password"; then
    return 0
  fi
  encoded="$(printf '%s' "$password" | python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=""))')"
  apply_shared_env_pair POSTGRES_REPLICA_PASSWORD_URLENCODED "$encoded" "derived:${file}"
}

set_shared_ref() {  # <service> <service-var> <shared-var>
  set_var "$1" "$2=\${{shared.$3}}"
}

READ_ENV_FOUND=0
READ_ENV_VALUE=""
capture_env_pair() {  # <key> <value> <wanted-key>
  if [ "$1" = "$3" ]; then
    READ_ENV_FOUND=1
    READ_ENV_VALUE="$2"
  fi
}
read_env_value() {  # <file> <key>
  READ_ENV_FOUND=0
  READ_ENV_VALUE=""
  iter_env_file "$1" capture_env_pair "$2" || true
  [ "$READ_ENV_FOUND" -eq 1 ] || return 1
  printf '%s' "$READ_ENV_VALUE"
}
set_public_url_from_domain_if_unset() {  # <service> <var> <env-file> <domain>
  local svc="$1" key="$2" file="$3" domain="$4" existing="" url=""
  [ -n "$domain" ] || return 0
  existing="$(read_env_value "$file" "$key" || true)"
  if is_placeholder_value "$existing"; then
    case "$domain" in
      http://*|https://*) url="$domain" ;;
      *) url="https://${domain}" ;;
    esac
    set_var "$svc" "${key}=${url}"
  fi
}

# --- Repo-sourced services ---------------------------------------------------
# name | dockerfilePath | watchPatterns(JSON)
REPO_SERVICES=(
  "router-physical-standby-v3|railway/router-physical-standby/Dockerfile|[\"railway/router-physical-standby/**\"]"
  "router-replica-stunnel-v3|railway/router-replica-stunnel/Dockerfile|[\"railway/router-replica-stunnel/**\"]"
  "sauron-worker-v3|etc/Dockerfile.sauron|[\"bin/sauron/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.sauron\"]"
  "evm-receipt-watcher-ethereum-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.evm-receipt-watcher\"]"
  "evm-receipt-watcher-base-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.evm-receipt-watcher\"]"
  "evm-receipt-watcher-arbitrum-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.evm-receipt-watcher\"]"
  "bitcoin-indexer-v3|etc/Dockerfile.bitcoin-indexer|[\"bitcoin-indexer/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.bitcoin-indexer\"]"
  "bitcoin-receipt-watcher-v3|etc/Dockerfile.bitcoin-receipt-watcher|[\"bitcoin-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.bitcoin-receipt-watcher\"]"
  "hl-shim-indexer-v3|etc/Dockerfile.hl-shim-indexer|[\"hl-shim-indexer/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.hl-shim-indexer\"]"
  "evm-token-indexer-ethereum-v3|evm-token-indexer/Dockerfile.index|[\"evm-token-indexer/**\"]"
  "evm-token-indexer-base-v3|evm-token-indexer/Dockerfile.index|[\"evm-token-indexer/**\"]"
  "evm-token-indexer-arbitrum-v3|evm-token-indexer/Dockerfile.index|[\"evm-token-indexer/**\"]"
  "router-gateway-v3|railway/router-gateway/Dockerfile|[\"apps/router-gateway/**\",\"railway/router-gateway/**\"]"
  "admin-dashboard-v3|railway/admin-dashboard/Dockerfile|[\"apps/admin-dashboard/**\",\"railway/admin-dashboard/**\"]"
  "explorer-v3|railway/explorer/Dockerfile|[\"railway/explorer/**\"]"
  "alloy-v3|railway/alloy/Dockerfile|[\"etc/alloy.railway.alloy\",\"railway/alloy/**\"]"
  "loki-v3|railway/loki/Dockerfile|[\"etc/loki.railway.yml\",\"railway/loki/**\"]"
  "grafana-v3|railway/grafana/Dockerfile|[\"etc/grafana/**\",\"railway/grafana/**\"]"
  "sauron-bitcoin-rathole-broker-v3|etc/Dockerfile.rathole-broker|[\"etc/Dockerfile.rathole-broker\",\"deploy/railway/sauron/**\"]"
)

for row in "${REPO_SERVICES[@]}"; do
  IFS='|' read -r name dockerfile watch <<<"$row"
  if svc_exists "$name"; then
    log "service $name exists — reconciling config"
  else
    log "creating repo service $name"
    railway add --service "$name"                 # VERIFY: --service create flag
    remember_svc "$name"
  fi
  apply_repo_cfg "$name" "$dockerfile" "$watch"
done

# --- Managed Postgres --------------------------------------------------------
for db in sauron-state-db-v3 hl-shim-db-v3 router-gateway-db-v3 \
          evm-token-indexer-ethereum-db-v3 evm-token-indexer-base-db-v3 \
          evm-token-indexer-arbitrum-db-v3 admin-dashboard-auth-db-v3 \
          admin-dashboard-analytics-db-v3; do
  create_managed_postgres "$db"
done

# --- Image-only service ------------------------------------------------------
if ! svc_exists victoriametrics-v3; then
  log "creating victoriametrics-v3 (image)"
  railway add --service victoriametrics-v3 --image victoriametrics/victoria-metrics:latest  # VERIFY
  remember_svc victoriametrics-v3
fi
apply_service_cfg victoriametrics-v3 \
  '{"deploy":{"startCommand":"/victoria-metrics-prod -storageDataPath=/victoria-metrics-data -retentionPeriod=30d -httpListenAddr=[::]:8428"}}' || true

# --- public-domain target ports ----------------------------------------------
# Railway's public proxy chooses one EXPOSE port. For services where that pick
# diverges from the actual app listener, pin PORT explicitly so the domain
# routes to the correct port instead of returning 502.
#   alloy-v3: EXPOSE 4318 12345; proxy needs 4318 (OTLP) not 12345 (admin)
#   grafana-v3: EXPOSE 3000 but Railway sometimes mis-detects on Grafana image
set_var alloy-v3 "PORT=4318"
set_var grafana-v3 "PORT=3000"

# --- SOCKS5 proxies -----------------------------------------------------------
# General fallback egress proxy for router upstreams. Image: serjs/go-socks5-proxy,
# auth required, port 1080, normal Railway region.
if ! svc_exists upstream-socks5-proxy-v3; then
  log "creating upstream-socks5-proxy-v3 (image)"
  railway add --service upstream-socks5-proxy-v3 --image serjs/go-socks5-proxy:latest  # VERIFY
  remember_svc upstream-socks5-proxy-v3
fi
# PROXY_USER/PROXY_PASSWORD from railway/env/upstream-socks5-proxy.env;
# REQUIRE_AUTH enforces username/password as the access boundary. Phala consumes
# this service's public TCP proxy as UPSTREAM_PROXY_URL.
set_var upstream-socks5-proxy-v3 "REQUIRE_AUTH=true"
set_var upstream-socks5-proxy-v3 "PROXY_PORT=1080"

# Dedicated HyperUnit egress proxy. Keep this Europe-pinned and do not use it as
# the general UPSTREAM_PROXY_URL fallback.
if ! svc_exists hyperunit-socks5-proxy-v3; then
  log "creating hyperunit-socks5-proxy-v3 (image, Europe)"
  railway add --service hyperunit-socks5-proxy-v3 --image serjs/go-socks5-proxy:latest  # VERIFY
  remember_svc hyperunit-socks5-proxy-v3
fi
# PROXY_USER/PROXY_PASSWORD from railway/env/hyperunit-socks5-proxy.env;
# REQUIRE_AUTH enforces username/password as the access boundary. Phala and
# sauron-worker-v3 consume this service as HYPERUNIT_PROXY_URL.
set_var hyperunit-socks5-proxy-v3 "REQUIRE_AUTH=true"
set_var hyperunit-socks5-proxy-v3 "PROXY_PORT=1080"
# Pin to Railway Europe (Netherlands / EU-West).  VERIFY: region id current.
pin_service_region hyperunit-socks5-proxy-v3 "europe-west4-drams3a" || \
  warn "set hyperunit-socks5-proxy-v3 region to Europe manually"

# --- Env files ---------------------------------------------------------------
# Apply local gitignored env files before reference wiring. Placeholder values
# are skipped with warnings so deploy-time values can be filled later.
log "applying shared variables from ${ENV_DIR}/_shared.env"
apply_shared_env_file
apply_derived_shared_vars

SERVICE_ENV_FILES=(
  "sauron-worker-v3|sauron.env"
  "hl-shim-indexer-v3|hl-shim-indexer.env"
  "evm-receipt-watcher-ethereum-v3|evm-receipt-watcher-ethereum.env"
  "evm-receipt-watcher-base-v3|evm-receipt-watcher-base.env"
  "evm-receipt-watcher-arbitrum-v3|evm-receipt-watcher-arbitrum.env"
  "bitcoin-indexer-v3|bitcoin-indexer.env"
  "bitcoin-receipt-watcher-v3|bitcoin-receipt-watcher.env"
  "evm-token-indexer-ethereum-v3|evm-token-indexer-ethereum.env"
  "evm-token-indexer-base-v3|evm-token-indexer-base.env"
  "evm-token-indexer-arbitrum-v3|evm-token-indexer-arbitrum.env"
  "router-gateway-v3|router-gateway.env"
  "admin-dashboard-v3|admin-dashboard.env"
  "explorer-v3|explorer.env"
  "router-physical-standby-v3|router-physical-standby.env"
  "router-replica-stunnel-v3|router-replica-stunnel.env"
  "upstream-socks5-proxy-v3|upstream-socks5-proxy.env"
  "hyperunit-socks5-proxy-v3|hyperunit-socks5-proxy.env"
  "sauron-bitcoin-rathole-broker-v3|sauron-bitcoin-rathole-broker.env"
)
for row in "${SERVICE_ENV_FILES[@]}"; do
  IFS='|' read -r svc file <<<"$row"
  log "applying service env for $svc"
  apply_service_env_file "$svc" "${ENV_DIR}/${file}"
done

# --- Reference-variable wiring ----------------------------------------------
# Set ONLY after producers exist (their RAILWAY_PRIVATE_DOMAIN/DATABASE_URL
# resolve post-create). Re-run this script after first pass to complete wiring.
log "wiring reference variables"
for c in ethereum base arbitrum; do
  set_var "evm-token-indexer-${c}-v3" "DATABASE_URL=\${{evm-token-indexer-${c}-db-v3.DATABASE_URL}}"
  set_var "evm-token-indexer-${c}-v3" "EVM_TOKEN_INDEXER_API_KEY=\${{shared.TOKEN_INDEXER_API_KEY}}"
done
set_shared_ref evm-token-indexer-ethereum-v3 PONDER_RPC_URL_HTTP ETH_RPC_URL
set_shared_ref evm-token-indexer-ethereum-v3 PONDER_WS_URL_HTTP ETH_WS_RPC_URL
set_shared_ref evm-token-indexer-base-v3 PONDER_RPC_URL_HTTP BASE_RPC_URL
set_shared_ref evm-token-indexer-base-v3 PONDER_WS_URL_HTTP BASE_WS_RPC_URL
set_shared_ref evm-token-indexer-arbitrum-v3 PONDER_RPC_URL_HTTP ARBITRUM_RPC_URL
set_shared_ref evm-token-indexer-arbitrum-v3 PONDER_WS_URL_HTTP ARBITRUM_WS_RPC_URL

set_shared_ref evm-receipt-watcher-ethereum-v3 EVM_RECEIPT_WATCHER_HTTP_RPC_URL ETH_RPC_URL
set_shared_ref evm-receipt-watcher-ethereum-v3 EVM_RECEIPT_WATCHER_WS_RPC_URL ETH_WS_RPC_URL
set_shared_ref evm-receipt-watcher-base-v3 EVM_RECEIPT_WATCHER_HTTP_RPC_URL BASE_RPC_URL
set_shared_ref evm-receipt-watcher-base-v3 EVM_RECEIPT_WATCHER_WS_RPC_URL BASE_WS_RPC_URL
set_shared_ref evm-receipt-watcher-arbitrum-v3 EVM_RECEIPT_WATCHER_HTTP_RPC_URL ARBITRUM_RPC_URL
set_shared_ref evm-receipt-watcher-arbitrum-v3 EVM_RECEIPT_WATCHER_WS_RPC_URL ARBITRUM_WS_RPC_URL

set_shared_ref bitcoin-indexer-v3 BITCOIN_INDEXER_RPC_URL BITCOIN_RPC_URL
set_shared_ref bitcoin-indexer-v3 BITCOIN_INDEXER_RPC_AUTH BITCOIN_RPC_AUTH
set_shared_ref bitcoin-indexer-v3 BITCOIN_INDEXER_ESPLORA_URL ELECTRUM_HTTP_SERVER_URL
set_shared_ref bitcoin-indexer-v3 BITCOIN_INDEXER_ZMQ_RAWBLOCK_ENDPOINT BITCOIN_ZMQ_RAWBLOCK_ENDPOINT
set_shared_ref bitcoin-indexer-v3 BITCOIN_INDEXER_ZMQ_RAWTX_ENDPOINT BITCOIN_ZMQ_RAWTX_ENDPOINT
set_shared_ref bitcoin-receipt-watcher-v3 BITCOIN_RECEIPT_WATCHER_RPC_URL BITCOIN_RPC_URL
set_shared_ref bitcoin-receipt-watcher-v3 BITCOIN_RECEIPT_WATCHER_RPC_AUTH BITCOIN_RPC_AUTH
set_shared_ref bitcoin-receipt-watcher-v3 BITCOIN_RECEIPT_WATCHER_ZMQ_RAWBLOCK_ENDPOINT BITCOIN_ZMQ_RAWBLOCK_ENDPOINT

set_shared_ref hl-shim-indexer-v3 HL_SHIM_API_KEY HL_SHIM_API_KEY
set_shared_ref alloy-v3 OBS_INGEST_TOKEN OBS_INGEST_TOKEN
set_shared_ref router-gateway-v3 ROUTER_GATEWAY_API_KEY ROUTER_GATEWAY_API_KEY
set_shared_ref router-gateway-v3 ROUTER_INTERNAL_BASE_URL ROUTER_INTERNAL_BASE_URL
set_shared_ref admin-dashboard-v3 ROUTER_ADMIN_API_KEY ROUTER_ADMIN_API_KEY
set_shared_ref admin-dashboard-v3 ROUTER_CDC_PUBLICATION_NAME ROUTER_CDC_PUBLICATION_NAME
set_shared_ref admin-dashboard-v3 ROUTER_CDC_MESSAGE_PREFIX ROUTER_CDC_MESSAGE_PREFIX
set_shared_ref router-physical-standby-v3 PRIMARY_DB_PASSWORD POSTGRES_REPLICA_PASSWORD

set_var router-replica-stunnel-v3 "PORT=5432"
set_var router-physical-standby-v3 "PORT=5432"
set_var router-physical-standby-v3 "DATABASE_URL=postgres://replicator:\${{shared.POSTGRES_REPLICA_PASSWORD_URLENCODED}}@router-physical-standby-v3.railway.internal:5432/router_db?sslmode=disable"
set_var sauron-worker-v3      "SAURON_STATE_DATABASE_URL=\${{sauron-state-db-v3.DATABASE_URL}}"
set_var sauron-worker-v3      "ROUTER_REPLICA_DATABASE_URL=\${{router-physical-standby-v3.DATABASE_URL}}"
set_shared_ref sauron-worker-v3 ETH_RPC_URL ETH_RPC_URL
set_shared_ref sauron-worker-v3 BASE_RPC_URL BASE_RPC_URL
set_shared_ref sauron-worker-v3 ARBITRUM_RPC_URL ARBITRUM_RPC_URL
set_shared_ref sauron-worker-v3 BITCOIN_RPC_URL BITCOIN_RPC_URL
set_shared_ref sauron-worker-v3 BITCOIN_RPC_AUTH BITCOIN_RPC_AUTH
set_shared_ref sauron-worker-v3 BITCOIN_ZMQ_RAWBLOCK_ENDPOINT BITCOIN_ZMQ_RAWBLOCK_ENDPOINT
set_shared_ref sauron-worker-v3 BITCOIN_ZMQ_RAWTX_ENDPOINT BITCOIN_ZMQ_RAWTX_ENDPOINT
set_shared_ref sauron-worker-v3 BITCOIN_ZMQ_SEQUENCE_ENDPOINT BITCOIN_ZMQ_SEQUENCE_ENDPOINT
set_shared_ref sauron-worker-v3 ELECTRUM_HTTP_SERVER_URL ELECTRUM_HTTP_SERVER_URL
set_shared_ref sauron-worker-v3 ROUTER_DETECTOR_API_KEY ROUTER_DETECTOR_API_KEY
set_shared_ref sauron-worker-v3 TOKEN_INDEXER_API_KEY TOKEN_INDEXER_API_KEY
set_shared_ref sauron-worker-v3 ROUTER_CDC_PUBLICATION_NAME ROUTER_CDC_PUBLICATION_NAME
set_shared_ref sauron-worker-v3 ROUTER_CDC_MESSAGE_PREFIX ROUTER_CDC_MESSAGE_PREFIX
set_shared_ref sauron-worker-v3 ROUTER_INTERNAL_BASE_URL ROUTER_INTERNAL_BASE_URL
set_shared_ref sauron-worker-v3 HYPERUNIT_API_URL HYPERUNIT_API_URL
set_var sauron-worker-v3      "HYPERUNIT_PROXY_URL=socks5://\${{hyperunit-socks5-proxy-v3.PROXY_USER}}:\${{hyperunit-socks5-proxy-v3.PROXY_PASSWORD}}@\${{hyperunit-socks5-proxy-v3.RAILWAY_PRIVATE_DOMAIN}}:1080"
set_var hl-shim-indexer-v3    "HL_SHIM_DATABASE_URL=\${{hl-shim-db-v3.DATABASE_URL}}"
set_var router-gateway-v3     "ROUTER_GATEWAY_DATABASE_URL=\${{router-gateway-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_AUTH_DATABASE_URL=\${{admin-dashboard-auth-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_ANALYTICS_DATABASE_URL=\${{admin-dashboard-analytics-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_REPLICA_DATABASE_URL=\${{router-physical-standby-v3.DATABASE_URL}}"
set_var sauron-worker-v3      "HL_SHIM_INDEXER_URL=http://\${{hl-shim-indexer-v3.RAILWAY_PRIVATE_DOMAIN}}:8080"
set_var sauron-worker-v3      "BITCOIN_INDEXER_URL=http://\${{bitcoin-indexer-v3.RAILWAY_PRIVATE_DOMAIN}}:8080"
set_var sauron-worker-v3      "BITCOIN_RECEIPT_WATCHER_URL=http://\${{bitcoin-receipt-watcher-v3.RAILWAY_PRIVATE_DOMAIN}}:8080"
for c in ethereum base arbitrum; do
  set_var sauron-worker-v3 "$(printf '%s_RECEIPT_WATCHER_URL=http://${{evm-receipt-watcher-%s-v3.RAILWAY_PRIVATE_DOMAIN}}:8080' "$(echo "$c" | tr a-z A-Z)" "$c")"
  set_var sauron-worker-v3 "$(printf '%s_TOKEN_INDEXER_URL=http://${{evm-token-indexer-%s-v3.RAILWAY_PRIVATE_DOMAIN}}:4001' "$(echo "$c" | tr a-z A-Z)" "$c")"
done

# Public domains. alloy-v3 is included: the Phala in-TEE sidecar reaches it
# from OUTSIDE Railway's private network, so it needs a public, bearer-
# authenticated endpoint (the others are user-facing UIs/APIs).
# sauron-bitcoin-rathole-broker-v3 also gets a public domain: the rathole
# CLIENT on the isolated bitcoin host dials the broker's websocket control
# plane over the public domain. The data ports (40031/2/3/4) stay private.
PUBLIC_DOMAINS=(
  "router-gateway-v3|4000"
  "admin-dashboard-v3|3000"
  "explorer-v3|3000"
  "grafana-v3|3000"
  "alloy-v3|4318"
  "sauron-bitcoin-rathole-broker-v3|8080"
)
set_var sauron-bitcoin-rathole-broker-v3 "PORT=8080"
extract_domain_from_json() {
  python3 -c 'import sys,json
try:
  data=json.load(sys.stdin)
  def walk(value):
    if isinstance(value, dict):
      if value.get("domain"):
        print(value["domain"])
        raise SystemExit
      for child in value.values():
        walk(child)
    elif isinstance(value, list):
      for child in value:
        walk(child)
    elif isinstance(value, str) and value.startswith(("http://", "https://")):
      print(value)
      raise SystemExit
  walk(data)
except SystemExit:
  pass
except Exception:
  pass'
}
ALLOY_DOMAIN=""
ROUTER_GATEWAY_DOMAIN=""
ADMIN_DASHBOARD_DOMAIN=""
for row in "${PUBLIC_DOMAINS[@]}"; do
  IFS='|' read -r pub port <<<"$row"
  domain_json="$(timeout 45s railway domain --service "$pub" --environment "$ENVIRONMENT" --port "$port" --json 2>/dev/null || true)"
  if [ -z "$domain_json" ]; then
    warn "set a domain for $pub manually on port $port if needed"
  else
    resolved_domain="$(printf '%s\n' "$domain_json" | extract_domain_from_json)"
    case "$pub" in
      alloy-v3) ALLOY_DOMAIN="$resolved_domain" ;;
      router-gateway-v3) ROUTER_GATEWAY_DOMAIN="$resolved_domain" ;;
      admin-dashboard-v3) ADMIN_DASHBOARD_DOMAIN="$resolved_domain" ;;
    esac
  fi
done

set_public_url_from_domain_if_unset router-gateway-v3 ROUTER_GATEWAY_PUBLIC_BASE_URL "${ENV_DIR}/router-gateway.env" "$ROUTER_GATEWAY_DOMAIN"
set_public_url_from_domain_if_unset admin-dashboard-v3 ADMIN_DASHBOARD_WEB_ORIGIN "${ENV_DIR}/admin-dashboard.env" "$ADMIN_DASHBOARD_DOMAIN"
set_public_url_from_domain_if_unset admin-dashboard-v3 BETTER_AUTH_URL "${ENV_DIR}/admin-dashboard.env" "$ADMIN_DASHBOARD_DOMAIN"

# Emit alloy-v3's resolved public URL — this is the value of ALLOY_V3_OTLP_URL
# that must go into .env.phala.prod BEFORE Phala is brought up (Railway-first).
if [ -z "${ALLOY_DOMAIN:-}" ]; then
  ALLOY_DOMAIN="$(timeout 45s railway domain --service alloy-v3 --environment "$ENVIRONMENT" --port 4318 --json 2>/dev/null \
    | extract_domain_from_json || true)"
fi
if [ -n "${ALLOY_DOMAIN:-}" ]; then
  log "alloy-v3 public endpoint: https://${ALLOY_DOMAIN}"
  log ">>> Set this in .env.phala.prod, then deploy Phala:"
  log ">>>   ALLOY_V3_OTLP_URL=https://${ALLOY_DOMAIN}"
else
  warn "could not resolve alloy-v3 domain automatically — read it from the Railway"
  warn "dashboard and set ALLOY_V3_OTLP_URL=https://<domain> in .env.phala.prod"
fi

log "bootstrap pass complete (Railway-first deploy order)."
log "1) Re-run this script once more so reference variables resolve against"
log "   now-created producers."
log "2) Put ALLOY_V3_OTLP_URL (above) + the matching OBS_INGEST_TOKEN into"
log "   .env.phala.prod, then bring up Phala (just phala-deploy <tag>)."
log "3) AFTER Phala is up, set ROUTER_INTERNAL_BASE_URL to the Phala"
log "   router-api URL, rerun this script, then complete gateway/dashboard/"
log "   replica/sauron verification per the Deployment Runbook."
