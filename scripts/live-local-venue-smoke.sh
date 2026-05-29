#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Run the live-local fast venue smoke suite with router-cli.

This spends real funds from LIVE_TEST_PRIVATE_KEY. It loads .env and
.env.live-local from the current repo worktree and writes artifacts under
live-test-logs/.

Default suite covers the fast non-Bitcoin venues:
  across, cctp, velora, hyperliquid_bridge, hyperliquid, unit

Usage:
  scripts/live-local-venue-smoke.sh [options]

Options:
  --mode sequential|parallel  sequential chains outputs; parallel starts selected swaps at once (default: sequential)
  --sequential                same as --mode sequential
  --parallel                  same as --mode parallel
  --only SELECTOR             run one swap by 1-based index, zero-padded index, or name prefix; repeatable
  --list                      list available swaps and exit without spending funds
  --gateway-url URL           router gateway URL (default: http://localhost:3001)
  --recipient ADDRESS         destination recipient (default: live local EVM wallet)
  --router-cli PATH           router-cli path (default: ./target/release/router-cli)
  --no-build                  do not run cargo build --release --bin router-cli first
  -h, --help                  show this help

Sequential default plan:
  1. Base.USDC        -> Arbitrum.USDC      amount 21    providers across
  2. Base.USDC        -> Arbitrum.USDC      amount 10    providers cctp
  3. Base.USDC        -> Base.USDT          amount 5     providers velora
  4. Arbitrum.USDC    -> Hyperliquid.USDC   amount 25    providers hyperliquid_bridge,hyperliquid
  5. Hyperliquid.USDC -> Arbitrum.USDC      amount 5     providers hyperliquid_bridge
  6. Hyperliquid.USDC -> Hyperliquid.UETH   amount 18    providers hyperliquid,hyperliquid
  7. Hyperliquid.UETH -> Ethereum.ETH       amount 0.008 providers unit

Parallel mode uses the same swaps but does not rely on prior outputs; it
requires all selected source balances to already exist before launch.
Use scripts/live-local-venue-quote-matrix.sh first for a no-spend route check.
USAGE
}

MODE="sequential"
GATEWAY_URL="http://localhost:3001"
RECIPIENT="0x33F65788aCa48D733c2C2444Ac9F79B18206aa92"
ROUTER_CLI="./target/release/router-cli"
BUILD_CLI=1
LIST_ONLY=0
SELECTORS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      [[ $# -ge 2 ]] || { echo "--mode requires sequential or parallel" >&2; exit 2; }
      MODE="$2"
      shift 2
      ;;
    --sequential)
      MODE="sequential"
      shift
      ;;
    --parallel)
      MODE="parallel"
      shift
      ;;
    --only)
      [[ $# -ge 2 ]] || { echo "--only requires a selector" >&2; exit 2; }
      SELECTORS+=("$2")
      shift 2
      ;;
    --list)
      LIST_ONLY=1
      shift
      ;;
    --gateway-url)
      [[ $# -ge 2 ]] || { echo "--gateway-url requires a URL" >&2; exit 2; }
      GATEWAY_URL="$2"
      shift 2
      ;;
    --recipient)
      [[ $# -ge 2 ]] || { echo "--recipient requires an address" >&2; exit 2; }
      RECIPIENT="$2"
      shift 2
      ;;
    --router-cli)
      [[ $# -ge 2 ]] || { echo "--router-cli requires a path" >&2; exit 2; }
      ROUTER_CLI="$2"
      shift 2
      ;;
    --no-build)
      BUILD_CLI=0
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

case "$MODE" in
  sequential|parallel) ;;
  *) echo "--mode must be sequential or parallel, got: $MODE" >&2; exit 2 ;;
esac

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

: "${BASE_RPC_URL:?BASE_RPC_URL is required in .env/.env.live-local}"
: "${ARBITRUM_RPC_URL:?ARBITRUM_RPC_URL is required in .env/.env.live-local}"

NAMES=()
RPC_URLS=()
FROMS=()
TOS=()
AMOUNTS=()
PROVIDER_SEQUENCES=()

add_swap() {
  NAMES+=("$1")
  RPC_URLS+=("$2")
  FROMS+=("$3")
  TOS+=("$4")
  AMOUNTS+=("$5")
  PROVIDER_SEQUENCES+=("$6")
}

add_swap \
  "01-across-base-usdc-to-arbitrum-usdc" \
  "$BASE_RPC_URL" \
  "Base.USDC" \
  "Arbitrum.USDC" \
  "21" \
  "across"

add_swap \
  "02-cctp-base-usdc-to-arbitrum-usdc" \
  "$BASE_RPC_URL" \
  "Base.USDC" \
  "Arbitrum.USDC" \
  "10" \
  "cctp"

add_swap \
  "03-velora-base-usdc-to-base-usdt" \
  "$BASE_RPC_URL" \
  "Base.USDC" \
  "Base.USDT" \
  "5" \
  "velora"

add_swap \
  "04-hlbridge-deposit-arbitrum-usdc-to-hl-usdc" \
  "$ARBITRUM_RPC_URL" \
  "Arbitrum.USDC" \
  "Hyperliquid.USDC" \
  "25" \
  "hyperliquid_bridge,hyperliquid"

add_swap \
  "05-hlbridge-withdraw-hl-usdc-to-arbitrum-usdc" \
  "$BASE_RPC_URL" \
  "Hyperliquid.USDC" \
  "Arbitrum.USDC" \
  "5" \
  "hyperliquid_bridge"

add_swap \
  "06-hyperliquid-hl-usdc-to-hl-ueth" \
  "$BASE_RPC_URL" \
  "Hyperliquid.USDC" \
  "Hyperliquid.UETH" \
  "18" \
  "hyperliquid,hyperliquid"

add_swap \
  "07-unit-hl-ueth-to-ethereum-eth" \
  "$BASE_RPC_URL" \
  "Hyperliquid.UETH" \
  "Ethereum.ETH" \
  "0.008" \
  "unit"

selector_matches_swap() {
  local selector="$1"
  local i="$2"
  local ordinal=$((i + 1))
  local padded
  padded="$(printf '%02d' "$ordinal")"
  [[ "$selector" == "$ordinal" || "$selector" == "$padded" || "$selector" == "${NAMES[$i]}" || "${NAMES[$i]}" == "$selector"* ]]
}

selected_indices() {
  local selected=()
  local i selector matched candidate already
  if [[ "${#SELECTORS[@]}" -eq 0 ]]; then
    for i in "${!NAMES[@]}"; do
      selected+=("$i")
    done
  else
    for selector in "${SELECTORS[@]}"; do
      matched=0
      for i in "${!NAMES[@]}"; do
        if selector_matches_swap "$selector" "$i"; then
          already=0
          for candidate in "${selected[@]}"; do
            [[ "$candidate" == "$i" ]] && already=1
          done
          [[ "$already" -eq 1 ]] || selected+=("$i")
          matched=1
        fi
      done
      [[ "$matched" -eq 1 ]] || { echo "--only selector did not match any swap: $selector" >&2; exit 2; }
    done
  fi
  printf '%s\n' "${selected[@]}"
}

mapfile -t SELECTED_INDICES < <(selected_indices)

print_swap_list() {
  local i
  for i in "${!NAMES[@]}"; do
    printf '%d\t%s\t%s\t%s\t%s\t%s\n' \
      "$((i + 1))" "${NAMES[$i]}" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}"
  done
}

if [[ "$LIST_ONLY" -eq 1 ]]; then
  print_swap_list
  exit 0
fi

: "${LIVE_TEST_PRIVATE_KEY:?LIVE_TEST_PRIVATE_KEY is required in .env}"
if [[ "${TEE_ROUTER_LIVE_LOCAL_ACK:-}" != "I_UNDERSTAND_THIS_USES_REAL_FUNDS" ]]; then
  echo "TEE_ROUTER_LIVE_LOCAL_ACK must be I_UNDERSTAND_THIS_USES_REAL_FUNDS" >&2
  exit 2
fi
export ROUTER_CLI_PRIVATE_KEY="$LIVE_TEST_PRIVATE_KEY"

if [[ "$BUILD_CLI" -eq 1 ]]; then
  cargo build --release --bin router-cli
fi

if [[ ! -x "$ROUTER_CLI" ]]; then
  echo "router-cli is not executable: $ROUTER_CLI" >&2
  exit 2
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)--fast-venue-smoke-${MODE}"
RUN_DIR="live-test-logs/${RUN_ID}"
mkdir -p "$RUN_DIR"
summary_file="$RUN_DIR/summary.tsv"
printf 'name\tfrom\tto\tamount\tprovider_sequence\torder_id\tfinal_status\tswap_log\tstatus_log\n' > "$summary_file"

print_plan() {
  echo "Run directory: $RUN_DIR"
  echo "Mode: $MODE"
  echo "Gateway: $GATEWAY_URL"
  echo "Recipient: $RECIPIENT"
  if [[ "${#SELECTORS[@]}" -gt 0 ]]; then
    printf 'Only selectors:'
    printf ' %s' "${SELECTORS[@]}"
    printf '\n'
  fi
  if [[ "$MODE" == "parallel" ]]; then
    echo "Parallel mode requires all selected source balances to exist before launch."
  fi
  echo "Plan:"
  local i
  for i in "${SELECTED_INDICES[@]}"; do
    printf '  %s: %s -> %s amount=%s providers=%s\n' \
      "${NAMES[$i]}" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}"
  done
}

parse_order_id() {
  local log_file="$1"
  local line
  while IFS= read -r line; do
    if [[ "$line" =~ order[[:space:]]id:[[:space:]]*([0-9a-fA-F-]{36}) ]]; then
      printf '%s\n' "${BASH_REMATCH[1]}"
      return 0
    fi
  done < "$log_file"
  return 1
}

parse_last_status() {
  local log_file="$1"
  local status=""
  local line
  while IFS= read -r line; do
    if [[ "$line" =~ status:[[:space:]]*([a-zA-Z_]+) ]]; then
      status="${BASH_REMATCH[1]}"
    fi
  done < "$log_file"
  [[ -n "$status" ]] || return 1
  printf '%s\n' "$status"
}

run_swap() {
  local i="$1"
  local name="${NAMES[$i]}"
  local swap_log="$RUN_DIR/${name}.swap.log"
  echo "Starting $name"
  "$ROUTER_CLI" swap \
    --gateway-url "$GATEWAY_URL" \
    --rpc-url "${RPC_URLS[$i]}" \
    --from "${FROMS[$i]}" \
    --to "${TOS[$i]}" \
    --from-amount "${AMOUNTS[$i]}" \
    --to-address "$RECIPIENT" \
    --provider-sequence "${PROVIDER_SEQUENCES[$i]}" \
    -y \
    > "$swap_log" 2>&1
  local order_id
  order_id="$(parse_order_id "$swap_log")" || {
    echo "Failed to parse order id for $name; see $swap_log" >&2
    return 1
  }
  printf '%s\n' "$order_id" > "$RUN_DIR/${name}.order-id"
  echo "Created $name order $order_id"
}

watch_swap() {
  local i="$1"
  local name="${NAMES[$i]}"
  local order_file="$RUN_DIR/${name}.order-id"
  local status_log="$RUN_DIR/${name}.status.log"
  [[ -f "$order_file" ]] || { echo "missing order id for $name" >&2; return 1; }
  local order_id
  order_id="$(<"$order_file")"
  "$ROUTER_CLI" status "$order_id" \
    --gateway-url "$GATEWAY_URL" \
    --watch \
    > "$status_log" 2>&1
  local final_status
  final_status="$(parse_last_status "$status_log")" || final_status="unknown"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}" \
    "$order_id" "$final_status" "$RUN_DIR/${name}.swap.log" "$status_log" \
    >> "$summary_file"
  echo "Final $name order $order_id status=$final_status"
  [[ "$final_status" == "completed" ]]
}

wait_all() {
  local failed=0
  local pid
  for pid in "$@"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  return "$failed"
}

print_plan | tee "$RUN_DIR/plan.txt"

if [[ "$MODE" == "sequential" ]]; then
  for i in "${SELECTED_INDICES[@]}"; do
    run_swap "$i"
    watch_swap "$i"
  done
else
  pids=()
  for i in "${SELECTED_INDICES[@]}"; do
    run_swap "$i" &
    pids+=("$!")
  done
  wait_all "${pids[@]}"

  pids=()
  for i in "${SELECTED_INDICES[@]}"; do
    watch_swap "$i" &
    pids+=("$!")
  done
  wait_all "${pids[@]}"
fi

echo "Summary: $summary_file"
