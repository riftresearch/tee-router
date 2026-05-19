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

log()  { printf '\033[1;34m[bootstrap]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[bootstrap:warn]\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m[bootstrap:err]\033[0m %s\n' "$*" >&2; exit 1; }

command -v railway >/dev/null || die "railway CLI not found"
railway whoami >/dev/null 2>&1 || die "not authenticated; run: railway login"

# --- Project -----------------------------------------------------------------
# Target the EXISTING project. Never create it; fail loudly if absent so we
# can't accidentally scatter services into the wrong/new project.
if ! railway status --json 2>/dev/null | grep -q "\"name\": *\"${PROJECT_NAME}\""; then
  log "linking existing project ${PROJECT_NAME}"
  railway link --project "${PROJECT_NAME}" \
    || die "project '${PROJECT_NAME}' not found / not linkable. This script only deploys into the existing project; it will not create one."
else
  log "project ${PROJECT_NAME} already linked"
fi
PROJECT_ID="$(railway status --json | python3 -c 'import sys,json;print(json.load(sys.stdin)["project"]["id"])')"
[ -n "${PROJECT_ID}" ] || die "could not resolve project id for ${PROJECT_NAME}"
log "project id ${PROJECT_ID}, environment ${ENVIRONMENT}"

svc_exists() { railway service status --all --json 2>/dev/null | grep -q "\"name\": *\"$1\""; }

# Apply a railway.json-equivalent config patch (authoritative; the in-repo
# railway/<svc>/railway.json is the portable record of the same settings).
apply_build_cfg() {  # <service> <dockerfilePath> <watchPatterns-json>
  local svc="$1" dockerfile="$2" watch="$3"
  railway environment edit --service-config "$svc" build.builder DOCKERFILE
  railway environment edit --service-config "$svc" build.dockerfilePath "$dockerfile"
  railway environment edit --service-config "$svc" build.watchPatterns "$watch"
  railway environment edit --service-config "$svc" deploy.restartPolicyType ON_FAILURE
}

set_src_repo() {  # <service>
  railway environment edit --service-config "$1" source.repo "$REPO"
  railway environment edit --service-config "$1" source.branch "$BRANCH"
}

set_var() { railway variable set "$2" --service "$1" --environment "$ENVIRONMENT" >/dev/null; }

# --- Repo-sourced services ---------------------------------------------------
# name | dockerfilePath | watchPatterns(JSON)
REPO_SERVICES=(
  "router-physical-standby-v3|railway/router-physical-standby/Dockerfile|[\"railway/router-physical-standby/**\"]"
  "router-replica-stunnel-v3|railway/router-replica-stunnel/Dockerfile|[\"railway/router-replica-stunnel/**\"]"
  "sauron-worker-v3|etc/Dockerfile.sauron|[\"bin/sauron/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\",\"etc/Dockerfile.sauron\"]"
  "evm-receipt-watcher-ethereum-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
  "evm-receipt-watcher-base-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
  "evm-receipt-watcher-arbitrum-v3|etc/Dockerfile.evm-receipt-watcher|[\"evm-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
  "bitcoin-indexer-v3|etc/Dockerfile.bitcoin-indexer|[\"bitcoin-indexer/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
  "bitcoin-receipt-watcher-v3|etc/Dockerfile.bitcoin-receipt-watcher|[\"bitcoin-receipt-watcher/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
  "hl-shim-indexer-v3|etc/Dockerfile.hl-shim-indexer|[\"hl-shim-indexer/**\",\"crates/**\",\"Cargo.toml\",\"Cargo.lock\"]"
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
  fi
  set_src_repo "$name"
  apply_build_cfg "$name" "$dockerfile" "$watch"
done

# --- Managed Postgres --------------------------------------------------------
for db in sauron-state-db-v3 hl-shim-db-v3 admin-dashboard-auth-db-v3 admin-dashboard-analytics-db-v3; do
  if svc_exists "$db"; then
    log "managed postgres $db exists"
  else
    log "creating managed postgres $db"
    railway add --database postgres --service "$db"   # VERIFY: db create flags
  fi
done

# --- Image-only service ------------------------------------------------------
if ! svc_exists victoriametrics-v3; then
  log "creating victoriametrics-v3 (image)"
  railway add --service victoriametrics-v3 --image victoriametrics/victoria-metrics:latest  # VERIFY
fi
railway environment edit --service-config victoriametrics-v3 deploy.startCommand \
  '-storageDataPath=/victoria-metrics-data -retentionPeriod=30d -httpListenAddr=[::]:8428' || true

# --- HyperUnit SOCKS5 proxy (managed, Europe) --------------------------------
# Dedicated egress proxy for HyperUnit only (see deploy/railway/hyperunit-socks5
# for the original helper this supersedes). Image: serjs/go-socks5-proxy,
# auth required, port 1080, pinned to Railway Europe (EU-West Metal / AMS).
# NOTE: a GENERAL/fallback upstream proxy (services without a dedicated one)
# is the feat/upstream-proxy work — NOT in main; add its managed entry when
# that branch merges.
if ! svc_exists hyperunit-socks5-proxy-v3; then
  log "creating hyperunit-socks5-proxy-v3 (image, Europe)"
  railway add --service hyperunit-socks5-proxy-v3 --image serjs/go-socks5-proxy:latest  # VERIFY
fi
# PROXY_USER/PROXY_PASSWORD from railway/env/hyperunit-socks5-proxy.env;
# REQUIRE_AUTH enforces username/password as the access boundary.
set_var hyperunit-socks5-proxy-v3 "REQUIRE_AUTH=true"
set_var hyperunit-socks5-proxy-v3 "PROXY_PORT=1080"
# Pin to Railway Europe (Netherlands / EU-West).  VERIFY: region id current.
railway environment edit --service-config hyperunit-socks5-proxy-v3 \
  deploy.multiRegionConfig '{"europe-west4-drams3a":{"numReplicas":1}}' || \
  warn "set hyperunit-socks5-proxy-v3 region to Europe manually"

# --- Shared variables (fan out to many services) -----------------------------
# These are read by multiple services via ${{shared.NAME}}. Values must be
# provided in the environment running this script (never hardcode secrets).
for shared in ROUTER_DETECTOR_API_KEY ROUTER_ADMIN_API_KEY ROUTER_GATEWAY_API_KEY \
              TOKEN_INDEXER_API_KEY HL_SHIM_API_KEY OBS_INGEST_TOKEN; do
  val="${!shared:-}"
  [ -n "$val" ] || { warn "shared var $shared not set in env — skipping (set before real run)"; continue; }
  railway environment edit --json <<JSON || true
{"sharedVariables":{"${shared}":{"value":"${val}"}}}
JSON
done

# --- Reference-variable wiring ----------------------------------------------
# Set ONLY after producers exist (their RAILWAY_PRIVATE_DOMAIN/DATABASE_URL
# resolve post-create). Re-run this script after first pass to complete wiring.
log "wiring reference variables"
set_var sauron-worker-v3      "SAURON_STATE_DATABASE_URL=\${{sauron-state-db-v3.DATABASE_URL}}"
set_var sauron-worker-v3      "ROUTER_REPLICA_DATABASE_URL=\${{router-physical-standby-v3.DATABASE_URL}}"
set_var hl-shim-indexer-v3    "HL_SHIM_DATABASE_URL=\${{hl-shim-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_AUTH_DATABASE_URL=\${{admin-dashboard-auth-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_ANALYTICS_DATABASE_URL=\${{admin-dashboard-analytics-db-v3.DATABASE_URL}}"
set_var admin-dashboard-v3    "ADMIN_DASHBOARD_REPLICA_DATABASE_URL=\${{router-physical-standby-v3.DATABASE_URL}}"
set_var sauron-worker-v3      "HL_SHIM_INDEXER_URL=http://\${{hl-shim-indexer-v3.RAILWAY_PRIVATE_DOMAIN}}:9104"
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
# plane over the public domain. The 3 feed ports (40031/2/3) stay private.
for pub in router-gateway-v3 admin-dashboard-v3 explorer-v3 grafana-v3 alloy-v3 sauron-bitcoin-rathole-broker-v3; do
  railway domain --service "$pub" >/dev/null 2>&1 || warn "set a domain for $pub manually if needed"
done

# Emit alloy-v3's resolved public URL — this is the value of ALLOY_V3_OTLP_URL
# that must go into .env.phala.prod BEFORE Phala is brought up (Railway-first).
ALLOY_DOMAIN="$(railway domain --service alloy-v3 --json 2>/dev/null \
  | python3 -c 'import sys,json
try:
  d=json.load(sys.stdin)
  def f(o):
    if isinstance(o,dict):
      if o.get("domain"): print(o["domain"]); raise SystemExit
      for v in o.values(): f(v)
    elif isinstance(o,list):
      for v in o: f(v)
  f(d)
except SystemExit: pass
except Exception: pass' || true)"
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
log "3) AFTER Phala primary is up, complete the replica/sauron tail (stunnel"
log "   -> standby -> sauron wiring) per the Deployment Runbook — those Railway"
log "   services depend on the Phala primary and cannot precede it."
