#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Create an internal HyperUnit-only SOCKS5 proxy service on Railway.

Environment:
  PROXY_USER                 Required. SOCKS5 username.
  PROXY_PASSWORD             Required. SOCKS5 password.
  SERVICE_NAME               Optional. Defaults to hyperunit-socks5.
  SOURCE_MODE                Optional. "image" (default) or "repo".
  PROXY_PORT                 Optional. Defaults to 1080.
  RAILWAY_REGION_FLAG        Optional. Defaults to europe-west4.
  ALLOWED_DEST_FQDN          Optional. Defaults to ^api\.hyperunit\.xyz$
  REQUIRE_AUTH               Optional. Defaults to true.
  ALLOWED_IPS                Optional. Comma-separated client IP allowlist.

  RAILWAY_PROJECT            Optional. Railway project name or id to link.
  RAILWAY_ENVIRONMENT        Optional. Railway environment to link.
  RAILWAY_WORKSPACE          Optional. Railway workspace/team to link.

Examples:
  PROXY_USER=router PROXY_PASSWORD=secret \
    ./deploy/railway/hyperunit-socks5/create-service.sh

  RAILWAY_PROJECT="rift v2" RAILWAY_ENVIRONMENT=production \
  PROXY_USER=router PROXY_PASSWORD=secret \
    ./deploy/railway/hyperunit-socks5/create-service.sh
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

: "${PROXY_USER:?set PROXY_USER}"
: "${PROXY_PASSWORD:?set PROXY_PASSWORD}"

SERVICE_NAME="${SERVICE_NAME:-hyperunit-socks5}"
SOURCE_MODE="${SOURCE_MODE:-image}"
PROXY_PORT="${PROXY_PORT:-1080}"
RAILWAY_REGION_FLAG="${RAILWAY_REGION_FLAG:-europe-west4}"
ALLOWED_DEST_FQDN="${ALLOWED_DEST_FQDN:-^api\\.hyperunit\\.xyz$}"
REQUIRE_AUTH="${REQUIRE_AUTH:-true}"

railway_config_file() {
  case "${RAILWAY_ENV:-production}" in
    production|"")
      printf '%s\n' "$HOME/.railway/config.json"
      ;;
    staging)
      printf '%s\n' "$HOME/.railway/config-staging.json"
      ;;
    dev|develop)
      printf '%s\n' "$HOME/.railway/config-dev.json"
      ;;
    *)
      printf '%s\n' "$HOME/.railway/config.json"
      ;;
  esac
}

railway_backboard_url() {
  case "${RAILWAY_ENV:-production}" in
    production|"")
      printf '%s\n' "https://backboard.railway.com/graphql/v2"
      ;;
    staging)
      printf '%s\n' "https://backboard.railway-staging.com/graphql/v2"
      ;;
    dev|develop)
      printf '%s\n' "https://backboard.railway-develop.com/graphql/v2"
      ;;
    *)
      printf '%s\n' "https://backboard.railway.com/graphql/v2"
      ;;
  esac
}

railway_graphql() {
  local payload="$1"
  local config_file token

  config_file="$(railway_config_file)"
  token="$(jq -r '.user.accessToken // .user.token // empty' "$config_file")"
  if [[ -z "$token" ]]; then
    echo "Unable to read Railway auth token from $config_file" >&2
    exit 1
  fi

  curl -fsSL "$(railway_backboard_url)" \
    -H "Authorization: Bearer $token" \
    -H 'x-source: CLI 4.35.0' \
    -H 'User-Agent: CLI 4.35.0' \
    -H 'Content-Type: application/json' \
    --data "$payload"
}

pin_service_to_region() {
  local config_file project_id environment_id service_query_payload env_query_payload
  local service_id existing_multi_region multi_region_payload update_payload updated

  config_file="$(railway_config_file)"
  project_id="$(jq -r --arg path "$PWD" '.projects[$path].project // empty' "$config_file")"
  environment_id="$(jq -r --arg path "$PWD" '.projects[$path].environment // empty' "$config_file")"

  if [[ -z "$project_id" || -z "$environment_id" ]]; then
    echo "Unable to read linked Railway project/environment from $config_file" >&2
    exit 1
  fi

  service_query_payload="$(jq -nc \
    --arg id "$project_id" \
    '{"query":"query($id:String!){ project(id:$id){ services{ edges{ node{ id name } } } } }","variables":{"id":$id}}')"
  service_id="$(
    railway_graphql "$service_query_payload" |
      jq -r --arg name "$SERVICE_NAME" '.data.project.services.edges[] | select(.node.name == $name) | .node.id' |
      head -n 1
  )"

  if [[ -z "$service_id" ]]; then
    echo "Unable to resolve Railway service id for $SERVICE_NAME" >&2
    exit 1
  fi

  env_query_payload="$(jq -nc \
    --arg id "$environment_id" \
    '{"query":"query($id:String!){ environment(id:$id){ config(decryptVariables:false) } }","variables":{"id":$id}}')"
  existing_multi_region="$(
    railway_graphql "$env_query_payload" |
      jq -c --arg service_id "$service_id" \
        '.data.environment.config.services[$service_id].deploy.multiRegionConfig // {}'
  )"

  multi_region_payload="$(
    jq -nc \
      --arg target "$RAILWAY_REGION_FLAG" \
      --argjson existing "$existing_multi_region" '
        reduce ($existing | keys_unsorted[]) as $key ({}; .[$key] = null)
        | .[$target] = {"numReplicas": 1}
      '
  )"

  update_payload="$(jq -nc \
    --arg environment_id "$environment_id" \
    --arg service_id "$service_id" \
    --argjson multi_region "$multi_region_payload" \
    '{"query":"mutation($environmentId:String!,$serviceId:String!,$multiRegionConfig:JSON!){ serviceInstanceUpdate(environmentId:$environmentId, serviceId:$serviceId, input:{ multiRegionConfig:$multiRegionConfig }) }","variables":{"environmentId":$environment_id,"serviceId":$service_id,"multiRegionConfig":$multi_region}}')"
  updated="$(railway_graphql "$update_payload" | jq -r '.data.serviceInstanceUpdate')"

  if [[ "$updated" != "true" ]]; then
    echo "Railway region pin failed for $SERVICE_NAME" >&2
    exit 1
  fi
}

if ! railway whoami >/dev/null 2>&1; then
  echo "Railway CLI is not authenticated. Run 'railway login' first." >&2
  exit 1
fi

if [[ -n "${RAILWAY_PROJECT:-}" ]]; then
  link_args=(project link --project "$RAILWAY_PROJECT")
  if [[ -n "${RAILWAY_ENVIRONMENT:-}" ]]; then
    link_args+=(--environment "$RAILWAY_ENVIRONMENT")
  fi
  if [[ -n "${RAILWAY_WORKSPACE:-}" ]]; then
    link_args+=(--workspace "$RAILWAY_WORKSPACE")
  fi
  railway "${link_args[@]}"
fi

add_args=(add --service "$SERVICE_NAME")
case "$SOURCE_MODE" in
  image)
    add_args+=(--image "serjs/go-socks5-proxy")
    ;;
  repo)
    add_args+=(--repo "https://github.com/serjs/socks5-server")
    ;;
  *)
    echo "Unsupported SOURCE_MODE: $SOURCE_MODE (expected image or repo)" >&2
    exit 1
    ;;
esac

railway "${add_args[@]}"

var_args=(
  variable set
  --service "$SERVICE_NAME"
  "REQUIRE_AUTH=$REQUIRE_AUTH"
  "PROXY_USER=$PROXY_USER"
  "PROXY_PASSWORD=$PROXY_PASSWORD"
  "PROXY_PORT=$PROXY_PORT"
  "ALLOWED_DEST_FQDN=$ALLOWED_DEST_FQDN"
)

if [[ -n "${ALLOWED_IPS:-}" ]]; then
  var_args+=("ALLOWED_IPS=$ALLOWED_IPS")
fi

railway "${var_args[@]}"

pin_service_to_region

cat <<EOF
Created Railway service '$SERVICE_NAME'.

Recommended router configuration:
  HYPERUNIT_PROXY_URL=socks5://${PROXY_USER}:${PROXY_PASSWORD}@${SERVICE_NAME}.railway.internal:${PROXY_PORT}

Notes:
  - This is an internal SOCKS5 proxy URL. Do not use socks5h.
  - The service was pinned to Railway region flag: ${RAILWAY_REGION_FLAG}
  - The proxy is locked to destinations matching: ${ALLOWED_DEST_FQDN}
  - Add a public Railway domain only if you explicitly need external ingress.
EOF
