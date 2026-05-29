#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'USAGE'
Dry-run the live-local fast venue route matrix without creating orders or moving funds.

It loads .env and .env.live-local for defaults, POSTs /quote to the live-local
gateway, and writes quote responses under live-test-logs/.

Usage:
  scripts/live-local-venue-quote-matrix.sh [options]

Options:
  --gateway-url URL    router gateway URL (default: http://localhost:3001)
  --recipient ADDRESS  destination recipient used for route validation
  --only SELECTOR      quote one swap by 1-based index, zero-padded index, or name prefix; repeatable
  --fail-fast          stop on first quote failure
  --list               list quote cases and exit
  -h, --help           show this help
USAGE
}

GATEWAY_URL="http://localhost:3001"
RECIPIENT="0x33F65788aCa48D733c2C2444Ac9F79B18206aa92"
FAIL_FAST=0
LIST_ONLY=0
SELECTORS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
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
    --only)
      [[ $# -ge 2 ]] || { echo "--only requires a selector" >&2; exit 2; }
      SELECTORS+=("$2")
      shift 2
      ;;
    --fail-fast)
      FAIL_FAST=1
      shift
      ;;
    --list)
      LIST_ONLY=1
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

NAMES=()
FROMS=()
TOS=()
AMOUNTS=()
PROVIDER_SEQUENCES=()

add_quote() {
  NAMES+=("$1")
  FROMS+=("$2")
  TOS+=("$3")
  AMOUNTS+=("$4")
  PROVIDER_SEQUENCES+=("$5")
}

add_quote \
  "01-across-base-usdc-to-arbitrum-usdc" \
  "Base.USDC" \
  "Arbitrum.USDC" \
  "21" \
  "across"

add_quote \
  "02-cctp-base-usdc-to-arbitrum-usdc" \
  "Base.USDC" \
  "Arbitrum.USDC" \
  "10" \
  "cctp"

add_quote \
  "03-velora-base-usdc-to-base-usdt" \
  "Base.USDC" \
  "Base.USDT" \
  "5" \
  "velora"

add_quote \
  "04-hlbridge-deposit-arbitrum-usdc-to-hl-usdc" \
  "Arbitrum.USDC" \
  "Hyperliquid.USDC" \
  "25" \
  "hyperliquid_bridge,hyperliquid"

add_quote \
  "05-hlbridge-withdraw-hl-usdc-to-arbitrum-usdc" \
  "Hyperliquid.USDC" \
  "Arbitrum.USDC" \
  "5" \
  "hyperliquid_bridge"

add_quote \
  "06-hyperliquid-hl-usdc-to-hl-ueth" \
  "Hyperliquid.USDC" \
  "Hyperliquid.UETH" \
  "18" \
  "hyperliquid,hyperliquid"

add_quote \
  "07-unit-hl-ueth-to-ethereum-eth" \
  "Hyperliquid.UETH" \
  "Ethereum.ETH" \
  "0.008" \
  "unit"

selector_matches_quote() {
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
        if selector_matches_quote "$selector" "$i"; then
          already=0
          for candidate in "${selected[@]}"; do
            [[ "$candidate" == "$i" ]] && already=1
          done
          [[ "$already" -eq 1 ]] || selected+=("$i")
          matched=1
        fi
      done
      [[ "$matched" -eq 1 ]] || { echo "--only selector did not match any quote: $selector" >&2; exit 2; }
    done
  fi
  printf '%s\n' "${selected[@]}"
}

mapfile -t SELECTED_INDICES < <(selected_indices)

print_quote_list() {
  local i
  for i in "${!NAMES[@]}"; do
    printf '%d\t%s\t%s\t%s\t%s\t%s\n' \
      "$((i + 1))" "${NAMES[$i]}" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}"
  done
}

if [[ "$LIST_ONLY" -eq 1 ]]; then
  print_quote_list
  exit 0
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)--fast-venue-quote-matrix"
RUN_DIR="live-test-logs/${RUN_ID}"
mkdir -p "$RUN_DIR"
summary_file="$RUN_DIR/summary.tsv"
printf 'name\tfrom\tto\tamount\tprovider_sequence\tstatus\tquote_id\testimated_out\tresponse_file\terror\n' > "$summary_file"

quote_one() {
  local i="$1"
  local name="${NAMES[$i]}"
  local response_file="$RUN_DIR/${name}.quote.json"
  local error_file="$RUN_DIR/${name}.quote.error"
  local providers_json
  providers_json="$(python3 - "${PROVIDER_SEQUENCES[$i]}" <<'PY'
import json, sys
print(json.dumps([part.strip() for part in sys.argv[1].split(',') if part.strip()]))
PY
)"
  python3 - "$GATEWAY_URL" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "$RECIPIENT" "$providers_json" "$response_file" "$error_file" <<'PY'
import json
import sys
import urllib.error
import urllib.request

base_url, source, dest, amount, recipient, providers_raw, response_path, error_path = sys.argv[1:]
providers = json.loads(providers_raw)
body = {
    "from": source,
    "to": dest,
    "fromAmount": amount,
    "amountFormat": "readable",
    "toAddress": recipient,
    "routing": {"providerSequence": providers},
}
url = base_url.rstrip("/") + "/quote"
request = urllib.request.Request(
    url,
    data=json.dumps(body).encode(),
    method="POST",
    headers={"content-type": "application/json"},
)
try:
    with urllib.request.urlopen(request, timeout=90) as response:
        raw = response.read().decode()
        data = json.loads(raw)
    with open(response_path, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, sort_keys=True)
        handle.write("\n")
    quote_id = data.get("quoteId") or data.get("quote_id") or ""
    estimated_out = data.get("estimatedOut") or data.get("estimated_out") or ""
    print("ok\t{}\t{}\t".format(quote_id, estimated_out))
except urllib.error.HTTPError as exc:
    raw = exc.read().decode(errors="replace")
    with open(error_path, "w", encoding="utf-8") as handle:
        handle.write(raw)
    print("error\t\t\tHTTP {}: {}".format(exc.code, raw.replace("\n", " ")[:500]))
    sys.exit(1)
except Exception as exc:  # noqa: BLE001 - shell boundary reports concise failure
    with open(error_path, "w", encoding="utf-8") as handle:
        handle.write(str(exc))
    print("error\t\t\t{}".format(str(exc).replace("\n", " ")[:500]))
    sys.exit(1)
PY
}

echo "Run directory: $RUN_DIR"
echo "Gateway: $GATEWAY_URL"
echo "Recipient: $RECIPIENT"
echo "Plan:"
for i in "${SELECTED_INDICES[@]}"; do
  printf '  %s: %s -> %s amount=%s providers=%s\n' \
    "${NAMES[$i]}" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}"
done

failed=0
for i in "${SELECTED_INDICES[@]}"; do
  name="${NAMES[$i]}"
  echo "Quoting $name"
  if result="$(quote_one "$i")"; then
    IFS=$'\t' read -r status quote_id estimated_out error_text <<< "$result"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}" \
      "$status" "$quote_id" "$estimated_out" "$RUN_DIR/${name}.quote.json" "$error_text" \
      >> "$summary_file"
    echo "  ok quote_id=$quote_id estimated_out=$estimated_out"
  else
    IFS=$'\t' read -r status quote_id estimated_out error_text <<< "$result"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$name" "${FROMS[$i]}" "${TOS[$i]}" "${AMOUNTS[$i]}" "${PROVIDER_SEQUENCES[$i]}" \
      "error" "" "" "$RUN_DIR/${name}.quote.error" "$error_text" \
      >> "$summary_file"
    echo "  error $error_text" >&2
    failed=1
    [[ "$FAIL_FAST" -eq 0 ]] || break
  fi
done

echo "Summary: $summary_file"
exit "$failed"
