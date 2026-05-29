#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Trigger and monitor a live-local manual refund for a stuck order.

This loads .env and .env.live-local, marks an executing/refund_required order as
refund_required in the local router Postgres, starts the admin refund endpoint,
and polls the order until it reaches refunded or the timeout expires.

Usage:
  scripts/live-local-rescue-order.sh ORDER_ID [options]

Options:
  --router-api-url URL   router-api URL (default: http://localhost:4522)
  --pg-url URL           Postgres URL (default: postgresql://router_app:router_app@127.0.0.1:55432/router_db)
  --poll-seconds N       max poll duration (default: 300)
  --poll-interval N      poll interval seconds (default: 5)
  --no-mark              do not update router_orders.status before calling refund endpoint
  -h, --help             show this help
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

ORDER_ID="$1"
shift
ROUTER_API_URL="http://localhost:4522"
PG_URL="postgresql://router_app:router_app@127.0.0.1:55432/router_db"
POLL_SECONDS=300
POLL_INTERVAL=5
MARK_REFUND_REQUIRED=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --router-api-url)
      [[ $# -ge 2 ]] || { echo "--router-api-url requires a URL" >&2; exit 2; }
      ROUTER_API_URL="$2"
      shift 2
      ;;
    --pg-url)
      [[ $# -ge 2 ]] || { echo "--pg-url requires a URL" >&2; exit 2; }
      PG_URL="$2"
      shift 2
      ;;
    --poll-seconds)
      [[ $# -ge 2 ]] || { echo "--poll-seconds requires a value" >&2; exit 2; }
      POLL_SECONDS="$2"
      shift 2
      ;;
    --poll-interval)
      [[ $# -ge 2 ]] || { echo "--poll-interval requires a value" >&2; exit 2; }
      POLL_INTERVAL="$2"
      shift 2
      ;;
    --no-mark)
      MARK_REFUND_REQUIRED=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ ! "$ORDER_ID" =~ ^[0-9a-fA-F-]{36}$ ]]; then
  echo "ORDER_ID must be a UUID, got: $ORDER_ID" >&2
  exit 2
fi
if [[ ! "$POLL_SECONDS" =~ ^[0-9]+$ || ! "$POLL_INTERVAL" =~ ^[0-9]+$ || "$POLL_INTERVAL" -eq 0 ]]; then
  echo "poll values must be positive integers" >&2
  exit 2
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
if [[ -f .env.live-local ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env.live-local
  set +a
fi
: "${ROUTER_ADMIN_API_KEY:?ROUTER_ADMIN_API_KEY is required in .env/.env.live-local}"

psql_order_status() {
  psql "$PG_URL" -v ON_ERROR_STOP=1 -P pager=off -Atc \
    "select status from router_orders where id = '$ORDER_ID';"
}

if [[ "$MARK_REFUND_REQUIRED" -eq 1 ]]; then
  psql "$PG_URL" -v ON_ERROR_STOP=1 -P pager=off -c \
    "update router_orders set status = 'refund_required', updated_at = now() where id = '$ORDER_ID' and status in ('executing', 'refund_required') returning id, status, updated_at;"
fi

python3 - "$ROUTER_API_URL" "$ORDER_ID" <<'PY'
import os
import sys
import urllib.error
import urllib.request

base_url, order_id = sys.argv[1:]
key = os.environ["ROUTER_ADMIN_API_KEY"]
request = urllib.request.Request(
    base_url.rstrip("/") + f"/internal/v1/orders/{order_id}/refund",
    data=b"",
    method="POST",
    headers={"Authorization": f"Bearer {key}"},
)
try:
    with urllib.request.urlopen(request, timeout=30) as response:
        print(response.status)
        print(response.read().decode())
except urllib.error.HTTPError as exc:
    print(exc.code)
    print(exc.read().decode())
    raise
PY

elapsed=0
while [[ "$elapsed" -le "$POLL_SECONDS" ]]; do
  status="$(psql_order_status)"
  printf 'order %s status=%s elapsed=%ss\n' "$ORDER_ID" "$status" "$elapsed"
  case "$status" in
    refunded)
      psql "$PG_URL" -v ON_ERROR_STOP=1 -P pager=off -c \
        "select a.attempt_index, a.attempt_kind, a.status, s.step_index, s.step_type, s.status as step_status, s.tx_hash from order_execution_attempts a left join order_execution_steps s on s.execution_attempt_id = a.id where a.order_id = '$ORDER_ID' order by a.attempt_index, s.step_index;"
      exit 0
      ;;
    completed)
      echo "order completed, not refunded" >&2
      exit 1
      ;;
  esac
  sleep "$POLL_INTERVAL"
  elapsed=$((elapsed + POLL_INTERVAL))
done

echo "timed out waiting for refund" >&2
exit 1
