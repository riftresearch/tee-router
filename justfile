local_devnet_compose := "docker compose --env-file .env.admin -p tee-router-local-full-test -f etc/compose.local-infra.yml -f etc/compose.local-devnet.yml -f etc/compose.local-observability.yml -f etc/compose.local-devnet-ports.yml"
live_local_compose := "docker compose --env-file .env.live-local -p tee-router-live-local -f etc/compose.local-infra.yml -f etc/compose.local-observability.yml -f etc/compose.live-local.yml"
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
      {{local_devnet_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
    fi
    {{local_devnet_compose}} "${args[@]}"

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
      {{local_devnet_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
    fi
    {{local_devnet_compose}} "${args[@]}"

# Pass through docker compose commands for the live-local real-network stack.
# Requires .env.live-local; start from etc/env.live-local.example.
# Examples: just live-local up -d, just live-local up-d, just live-local down -v, just live-local ps
live-local +args:
    #!/usr/bin/env bash
    set -euo pipefail
    test -f .env.live-local || { echo "missing .env.live-local (copy etc/env.live-local.example and fill real-network values)" >&2; exit 1; }
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
      {{live_local_compose}} stop admin-dashboard sauron >/dev/null 2>&1 || true
    fi
    {{live_local_compose}} "${args[@]}"

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
    {{local_devnet_compose}} \
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
    {{local_devnet_compose}} --profile tools build router-loadgen

# Run random router loadgen from the host cargo binary.
router-loadgen count='100' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

# Run the high-volume random router loadgen profile from the host cargo binary.
router-loadgen-slow count='10000' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

router-loadgen-one count='1' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='market':
    just _router-loadgen-host {{count}} {{concurrency}} {{rps}} {{min_raw_amount}} {{max_raw_amount}} {{order_type}}

_router-loadgen-host count concurrency rps min_raw_amount max_raw_amount order_type:
    cargo run --release -p router-loadgen -- create-and-fund \
      --gateway-url http://localhost:13001 \
      --random \
      --order-type {{order_type}} \
      --random-min-raw-amount {{min_raw_amount}} \
      --random-max-raw-amount {{max_raw_amount}} \
      --amount-format raw \
      --to-address 0x1111111111111111111111111111111111111111 \
      --count {{count}} \
      --concurrency {{concurrency}} \
      --rps {{rps}} \
      --evm-rpc evm:1=http://localhost:56101 \
      --evm-rpc evm:8453=http://localhost:56102 \
      --evm-rpc evm:42161=http://localhost:56103 \
      --devnet-manifest-url http://localhost:56108/manifest.json \
      --bitcoin-rpc-url http://localhost:56100/wallet/alice \
      --bitcoin-rpc-auth devnet:devnet

# Run random router loadgen from the host cargo binary
router-loadgen-limit count='100' concurrency='64' rps='5' min_raw_amount='100000000' max_raw_amount='250000000' order_type='limit':
    cargo run --release -p router-loadgen -- create-and-fund \
      --gateway-url http://localhost:13001 \
      --random \
      --order-type {{order_type}} \
      --random-min-raw-amount {{min_raw_amount}} \
      --random-max-raw-amount {{max_raw_amount}} \
      --amount-format raw \
      --to-address 0x1111111111111111111111111111111111111111 \
      --count {{count}} \
      --concurrency {{concurrency}} \
      --rps {{rps}} \
      --evm-rpc evm:1=http://localhost:56101 \
      --evm-rpc evm:8453=http://localhost:56102 \
      --evm-rpc evm:42161=http://localhost:56103 \
      --devnet-manifest-url http://localhost:56108/manifest.json \
      --bitcoin-rpc-url http://localhost:56100/wallet/alice \
      --bitcoin-rpc-auth devnet:devnet


test-stack:
  just dc down -v
  just dc up -d --build
  just router-loadgen 1


# Inspect test-wallet balances across local EVM chains, Hyperliquid, and Bitcoin.
# Examples:
#   just wallet-balance --address 0x1111111111111111111111111111111111111111
#   just wallet-balance --address bcrt1... --skip-evm --skip-hyperliquid
wallet-balance +args:
    cargo run -p devnet --bin wallet-balance -- {{args}}

# --- Test recipes -----------------------------------------------------------
# Fast iteration: unit + lib + lightweight integration tests. Devnet-spawning
# tests are tagged `#[ignore = "integration: ..."]` and excluded here.
# Target runtime: under 30 seconds.
test:
    cargo nextest run --workspace

# Slow integration tests only (devnet-spawning, multi-process).
# Use this when validating cross-cutting changes that touch Sauron + T-router
# + provider observers end-to-end.
test-integration:
    cargo nextest run --workspace --run-ignored=ignored-only

# Full gate: every test including integration. Run before commit on changes
# that touch order workflow / Sauron observation / hint verification.
test-all:
    cargo nextest run --workspace --run-ignored=all

# --- Phala (TEE) deploy -----------------------------------------------------
# No CI/CD for Phala by design. Images are built+pushed to GHCR by the image
# workflow on a `vX.Y.Z` tag; these recipes pin etc/compose.phala.yml to a tag
# and (re)deploy it to a Phala CVM via the unified `phala deploy` command
# (the old `phala cvms create/upgrade` subcommands are deprecated).
#
# One-time: `just phala-login`, and create .secrets/phala.env (gitignored)
# holding every required compose var (the ${VAR:?} set + OBS_INGEST_TOKEN +
# ALLOY_V3_OTLP_URL — see docs/devops-deployment-plan.md secret table).
#
# CVM is referenced by name/id. No active deployment (the previous CVM was
# deleted 2026-06-02), so there is no default — pass phala_cvm explicitly:
#   just phala-create 0.2.26 phala_cvm=<new-name>   # first time (creates the CVM)
#   just phala-deploy  0.2.27 phala_cvm=<name|id>    # later (updates existing)
phala_cvm           := ""
phala_env           := ".secrets/phala.env"
phala_compose       := "etc/compose.phala.yml"
# Machine size — applied by `phala-create` only (updates keep the CVM's existing
# size). tdx.xlarge (8 vCPU / 16 GB) is the floor that fits the stack's two
# Postgres (shared_buffers=1GB each) + the 4-role Temporal split; size up for
# sustained load (tdx.2xlarge 16/32, tdx.4xlarge 32/64). See the sizing note in
# docs/devops-deployment-plan.md. Override per-deploy, e.g.
# `phala_instance_type=tdx.2xlarge phala_disk_size=80G`.
phala_instance_type := "tdx.xlarge"
phala_disk_size     := "60G"

# One-time auth to Phala Cloud (device flow; `phala login --manual` for an
# API token instead).
phala-login:
    phala login

# Create a NEW CVM from compose.phala.yml pinned to TAG, sized by
# phala_instance_type / phala_disk_size. Use once (no CVM exists); afterwards
# use phala-deploy to update it. Example:
#   just phala-create 0.2.26 phala_cvm=tee-router-prod1
phala-create TAG:
    #!/usr/bin/env bash
    set -euo pipefail
    test -n "{{phala_cvm}}" || { echo "pass phala_cvm=<new-name>"; exit 1; }
    test -f "{{phala_env}}" || { echo "missing {{phala_env}} (gitignored secret env)"; exit 1; }
    sed -i -E 's#(ghcr\.io/riftresearch/tee-router(-temporal-worker|-temporal-ui)?:)[^"[:space:]]+#\1{{TAG}}#g' "{{phala_compose}}"
    echo "pinned ghcr images to {{TAG}}:"
    grep -n 'ghcr.io/riftresearch/tee-router' "{{phala_compose}}"
    docker compose -f "{{phala_compose}}" config --no-interpolate >/dev/null
    phala deploy --name "{{phala_cvm}}" -c "{{phala_compose}}" -e "{{phala_env}}" \
      --instance-type "{{phala_instance_type}}" --disk-size "{{phala_disk_size}}"
    echo "Created {{phala_cvm}} ({{phala_instance_type}}, {{phala_disk_size}}) at {{TAG}}."
    echo "Capture app-id + endpoints: phala cvms get {{phala_cvm}} -j"

# Push an upgrade to an EXISTING CVM: pin both GHCR images to TAG, then
# redeploy. Phala restarts the CVM, pulls the new images, starts with new
# config. Example: just phala-deploy 0.2.27 phala_cvm=<name|id>
phala-deploy TAG:
    #!/usr/bin/env bash
    set -euo pipefail
    test -n "{{phala_cvm}}" || { echo "pass phala_cvm=<name|id>"; exit 1; }
    test -f "{{phala_env}}" || { echo "missing {{phala_env}} (gitignored secret env)"; exit 1; }
    sed -i -E 's#(ghcr\.io/riftresearch/tee-router(-temporal-worker|-temporal-ui)?:)[^"[:space:]]+#\1{{TAG}}#g' "{{phala_compose}}"
    echo "pinned ghcr images to {{TAG}}:"
    grep -n 'ghcr.io/riftresearch/tee-router' "{{phala_compose}}"
    docker compose -f "{{phala_compose}}" config --no-interpolate >/dev/null
    phala deploy --cvm-id "{{phala_cvm}}" -c "{{phala_compose}}" -e "{{phala_env}}"
    echo "Deployed {{TAG}} to Phala CVM {{phala_cvm}}. Commit the compose tag bump to record the deployed revision."

# Redeploy the CURRENT compose.phala.yml as-is (config-only change; no image
# tag bump). Use after editing compose/secrets without changing the release.
phala-upgrade:
    #!/usr/bin/env bash
    set -euo pipefail
    test -n "{{phala_cvm}}" || { echo "pass phala_cvm=<name|id>"; exit 1; }
    test -f "{{phala_env}}" || { echo "missing {{phala_env}}"; exit 1; }
    docker compose -f "{{phala_compose}}" config --no-interpolate >/dev/null
    phala deploy --cvm-id "{{phala_cvm}}" -c "{{phala_compose}}" -e "{{phala_env}}"
