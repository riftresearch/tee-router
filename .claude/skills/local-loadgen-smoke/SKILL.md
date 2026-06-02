---
name: local-loadgen-smoke
description: >
  Run the local full-stack loadgen smoke test for tee-router end to end: reset the
  local docker-compose stack (wiping volumes), bring it up, wait for the gateway,
  run router-loadgen, then VERIFY every order settles to `completed`. Use to validate
  local changes end-to-end, or to reproduce/debug order-flow on the local devnet.
allowed-tools: Bash, Read
---

# Local full-stack loadgen smoke test

Fresh local stack → generate random orders → confirm they all settle. The single
gotcha this skill exists to prevent: **the loadgen's own success line only means
create+fund — it does NOT mean the orders settled.** Always verify completion (step 4).

## Prerequisites

- Run from **`.worktrees/main/`** (the full `justfile`; the repo root has a stripped one).
- Docker running. The `dc` recipe is a docker-compose passthrough for the local
  full-stack compose (project `tee-router-local-full-test`).

## Workflow

### 1. Reset + bring up the stack

```bash
just dc down -v        # stop stack AND wipe volumes (clean devnet + DBs)
just dc up -d          # bring it back up (detached)
just dc ps             # sanity: containers Up/healthy
```

⚠️ **`dc up -d` reuses the existing locally-built images — it does NOT rebuild.**
To validate local **code** changes in-container, build first:

```bash
just dc up -d --build  # rebuild router-api / router-worker / gateway / etc. from source
```

### 2. Wait for the gateway to be healthy

The loadgen entrypoint is the gateway at `http://localhost:13001`. After `up -d` the
containers are "starting" — the devnet (anvil + bitcoin regtest + contract deploy +
manifest) and the gateway take time. Poll before loadgen:

```bash
until [ "$(curl -s -o /dev/null -w '%{http_code}' -m 3 http://localhost:13001/health)" = 200 ]; do sleep 3; done
curl -s http://localhost:13001/providers | python3 -m json.tool   # expect status:"ok", all 6 venues "reachable"
```

### 3. Run the loadgen

```bash
just router-loadgen 2>&1 | tee /tmp/loadgen.out      # 100 random market orders, rps 5 (defaults)
```

It builds `router-loadgen` (release) on first run, then quotes → creates → funds N
random orders (mixed routes across Base/Ethereum/Arbitrum USDC·ETH and Bitcoin
regtest `bcrt1q…`). Success line: `router-loadgen completed N/N orders in …`.

Profiles / overrides:

```bash
just router-loadgen-one     # 1 order (quick check)
just router-loadgen-slow    # 10,000 orders (high-volume profile)
just router-loadgen-limit   # limit orders instead of market
just router-loadgen count=500 concurrency=64 rps=10 order_type=market
```

### 4. VERIFY settlement (the step people skip)

`router-loadgen completed N/N` only confirms **create+fund**. Settlement happens
asynchronously in the router-worker / Temporal afterward (seconds–minutes on the
local devnet). Confirm every order reached `completed` by polling the gateway:

```bash
grep -oE '"orderId":"[0-9a-f-]+"' /tmp/loadgen.out | sed -E 's/.*:"//;s/"//' | sort -u > /tmp/oids.txt
while read -r oid; do
  curl -sm 8 "http://localhost:13001/order/$oid" \
    | python3 -c "import json,sys;print(json.load(sys.stdin).get('status','ERR'))"
done < /tmp/oids.txt | sort | uniq -c
# target: "<N> completed". Re-run this tally until all settle; anything stuck in
# executing/pending_funding/refunded/failed is a real finding worth investigating.
```

A clean run is **all N `completed`, zero failures**.

## Reference

- **Ports** (host): gateway `13001`, router-api `14522` (metrics `19100`), temporal-ui
  `18080`, grafana `13002`, admin-dashboard `13000`, devnet RPCs `56100`(btc)
  `56101`(eth) `56102`(base) `56103`(arb) `56108`(manifest), router-postgres `56432`.
- **Debug a stuck order**: open `http://localhost:18080` (Temporal UI), workflow
  `order:<orderId>:execution`; or read worker logs (`just dc logs router-worker`).
- For deploy/venue-API/infra details see the **`tee-router-ops`** skill.
