local_full_compose := "docker compose --env-file .env.admin -p tee-router-local-full-test -f etc/compose.local-full.yml"

# Cache the local devnet
cache-devnet:
    cargo run --bin devnet -- cache
    @echo "Devnet cached"

# Pass through docker compose commands for the local full devnet stack.
# Examples: just devnet up, just devnet up -d --build, just devnet up-d, just devnet down, just devnet ps
devnet +args:
    #!/usr/bin/env bash
    set -euo pipefail
    args=( {{args}} )
    if [[ "${#args[@]}" -eq 0 ]]; then
      args=(ps)
    fi
    if [[ "${args[0]}" == "up-d" ]]; then
      args=(up -d "${args[@]:1}")
    fi
    if [[ "${args[0]}" == "up" ]]; then
      {{local_full_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
      {{local_full_compose}} rm -f router-replica-setup >/dev/null 2>&1 || true
    fi
    {{local_full_compose}} "${args[@]}"

# Short alias for local full docker compose commands.
# Examples: just dc up -d, just dc down -v, just dc ps
dc +args:
    #!/usr/bin/env bash
    set -euo pipefail
    args=( {{args}} )
    if [[ "${#args[@]}" -eq 0 ]]; then
      args=(ps)
    fi
    if [[ "${args[0]}" == "up-d" ]]; then
      args=(up -d "${args[@]:1}")
    fi
    if [[ "${args[0]}" == "up" ]]; then
      {{local_full_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
      {{local_full_compose}} rm -f router-replica-setup >/dev/null 2>&1 || true
    fi
    {{local_full_compose}} "${args[@]}"

# Run random router loadgen inside the local full compose stack
compose-router-loadgen count='100' concurrency='8' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
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

# Run random router loadgen from the host cargo binary
router-loadgen count='100' concurrency='8' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
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
