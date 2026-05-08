local_full_compose := "docker compose --env-file .env.admin -p tee-router-local-full-test -f etc/compose.local-full.yml -f etc/compose.local-observability.yml"
temporal_compose := "docker compose -p tee-router-temporal -f etc/compose.temporal.yml"

# Cache the local devnet
cache-devnet:
    cargo run --bin devnet -- cache
    @echo "Devnet cached"

# Pass through docker compose commands for the local full devnet stack.
# Examples: just devnet up, just devnet up -d --build, just devnet up-d, just devnet down, just devnet ps
devnet +args:
    #!/usr/bin/env bash
    set -euo pipefail
    has_compose_up_services() {
      local skip_next=0
      for arg in "$@"; do
        if [[ "$skip_next" -eq 1 ]]; then
          skip_next=0
          continue
        fi
        case "$arg" in
          --abort-on-container-exit|--always-recreate-deps|--attach-dependencies|--build|--detach|-d|--force-recreate|--menu|--no-attach|--no-build|--no-color|--no-deps|--no-log-prefix|--no-recreate|--no-start|--pull|--quiet-pull|--remove-orphans|--renew-anon-volumes|-V|--timestamps|--wait)
            ;;
          --attach|--exit-code-from|--scale|--timeout|-t|--wait-timeout)
            skip_next=1
            ;;
          -*)
            ;;
          *)
            return 0
            ;;
        esac
      done
      return 1
    }
    args=( {{args}} )
    if [[ "${#args[@]}" -eq 0 ]]; then
      args=(ps)
    fi
    if [[ "${args[0]}" == "up-d" ]]; then
      args=(up -d "${args[@]:1}")
    fi
    if [[ "${args[0]}" == "up" ]] && ! has_compose_up_services "${args[@]:1}"; then
      {{local_full_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
    fi
    {{local_full_compose}} "${args[@]}"

# Short alias for local full docker compose commands.
# Examples: just dc up -d, just dc down -v, just dc ps
dc +args:
    #!/usr/bin/env bash
    set -euo pipefail
    has_compose_up_services() {
      local skip_next=0
      for arg in "$@"; do
        if [[ "$skip_next" -eq 1 ]]; then
          skip_next=0
          continue
        fi
        case "$arg" in
          --abort-on-container-exit|--always-recreate-deps|--attach-dependencies|--build|--detach|-d|--force-recreate|--menu|--no-attach|--no-build|--no-color|--no-deps|--no-log-prefix|--no-recreate|--no-start|--pull|--quiet-pull|--remove-orphans|--renew-anon-volumes|-V|--timestamps|--wait)
            ;;
          --attach|--exit-code-from|--scale|--timeout|-t|--wait-timeout)
            skip_next=1
            ;;
          -*)
            ;;
          *)
            return 0
            ;;
        esac
      done
      return 1
    }
    args=( {{args}} )
    if [[ "${#args[@]}" -eq 0 ]]; then
      args=(ps)
    fi
    if [[ "${args[0]}" == "up-d" ]]; then
      args=(up -d "${args[@]:1}")
    fi
    if [[ "${args[0]}" == "up" ]] && ! has_compose_up_services "${args[@]:1}"; then
      {{local_full_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
    fi
    {{local_full_compose}} "${args[@]}"

# Pass through docker compose commands for the local Temporal stack.
# Examples: just temporal up -d, just temporal down -v, just temporal ps
temporal *args:
    #!/usr/bin/env bash
    set -euo pipefail
    args=( {{args}} )
    if [[ "${#args[@]}" -eq 0 ]]; then
      args=(ps)
    fi
    if [[ "${args[0]}" == "up-d" ]]; then
      args=(up -d "${args[@]:1}")
    fi
    {{temporal_compose}} "${args[@]}"

# Start the local Temporal stack.
temporal-up:
    {{temporal_compose}} up -d

# Stop the local Temporal stack. Pass -v to remove Temporal state.
temporal-down *args:
    {{temporal_compose}} down {{args}}

# Run the Rust SDK spike against the local Temporal stack.
temporal-spike *args:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${PROTOC:-}" && -x "$PWD/target/tools/protoc/bin/protoc" ]]; then
      export PROTOC="$PWD/target/tools/protoc/bin/protoc"
    fi
    if [[ -z "${PROTOC:-}" ]] && ! command -v protoc >/dev/null 2>&1; then
      echo "temporal-worker requires protoc; install protobuf-compiler or set PROTOC=/path/to/protoc" >&2
      exit 1
    fi
    args=( {{args}} )
    cargo run -p temporal-worker -- spike "${args[@]}"

# Run random router loadgen inside the local full compose stack
compose-router-loadgen count='100' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    {{local_full_compose}} \
      --profile tools \
      run --build --rm router-loadgen create-and-fund \
      --random \
      --order-type {{order_type}} \
      --random-min-raw-amount {{min_raw_amount}} \
      --random-max-raw-amount {{max_raw_amount}} \
      --amount-format raw \
      --to-address 0x1111111111111111111111111111111111111111 \
      --count {{count}} \
      --concurrency {{concurrency}} \
      --rps {{rps}} \
      --evm-rpc evm:1=http://devnet:50101 \
      --evm-rpc evm:8453=http://devnet:50102 \
      --evm-rpc evm:42161=http://devnet:50103 \
      --devnet-manifest-url http://devnet:50108/manifest.json \
      --bitcoin-rpc-url http://devnet:50100/wallet/alice \
      --bitcoin-rpc-auth devnet:devnet

# Rebuild only the router-loadgen tool image used by compose-router-loadgen
compose-router-loadgen-build:
    {{local_full_compose}} --profile tools build router-loadgen

# Run random router loadgen from the host cargo binary.
router-loadgen count='100' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

# Run the high-volume random router loadgen profile from the host cargo binary.
router-loadgen-fast count='10000' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

# Run the slow random router loadgen profile from the host cargo binary.
router-loadgen-slow count='600' concurrency='64' rps='0.1666666667' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

_router-loadgen-host count concurrency rps min_raw_amount max_raw_amount order_type:
    cargo run --release -p router-loadgen -- create-and-fund \
      --gateway-url http://localhost:3001 \
      --random \
      --order-type {{order_type}} \
      --random-min-raw-amount {{min_raw_amount}} \
      --random-max-raw-amount {{max_raw_amount}} \
      --amount-format raw \
      --to-address 0x1111111111111111111111111111111111111111 \
      --count {{count}} \
      --concurrency {{concurrency}} \
      --rps {{rps}} \
      --evm-rpc evm:1=http://localhost:50101 \
      --evm-rpc evm:8453=http://localhost:50102 \
      --evm-rpc evm:42161=http://localhost:50103 \
      --devnet-manifest-url http://localhost:50108/manifest.json \
      --bitcoin-rpc-url http://localhost:50100/wallet/alice \
      --bitcoin-rpc-auth devnet:devnet

# Run random router loadgen from the host cargo binary
router-loadgen-limit count='100' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='limit':
    cargo run --release -p router-loadgen -- create-and-fund \
      --gateway-url http://localhost:3001 \
      --random \
      --order-type {{order_type}} \
      --random-min-raw-amount {{min_raw_amount}} \
      --random-max-raw-amount {{max_raw_amount}} \
      --amount-format raw \
      --to-address 0x1111111111111111111111111111111111111111 \
      --count {{count}} \
      --concurrency {{concurrency}} \
      --rps {{rps}} \
      --evm-rpc evm:1=http://localhost:50101 \
      --evm-rpc evm:8453=http://localhost:50102 \
      --evm-rpc evm:42161=http://localhost:50103 \
      --devnet-manifest-url http://localhost:50108/manifest.json \
      --bitcoin-rpc-url http://localhost:50100/wallet/alice \
      --bitcoin-rpc-auth devnet:devnet

# Inspect test-wallet balances across local EVM chains, Hyperliquid, and Bitcoin.
# Examples:
#   just wallet-balance --address 0x1111111111111111111111111111111111111111
#   just wallet-balance --address bcrt1... --skip-evm --skip-hyperliquid
wallet-balance +args:
    cargo run -p devnet --bin wallet-balance -- {{args}}
