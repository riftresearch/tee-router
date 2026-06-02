---
name: tee-router-ops
description: >
  Operate the tee-router stack: deploy releases, inspect live orders, debug stuck
  workflows, query venue APIs (Hyperliquid, Hyperunit, CCTP, Velora, Across, Bitcoin),
  and reach Phala / Railway / Temporal infrastructure. Use whenever the task involves
  this codebase's production systems, live tests, deploys, or stuck-order debugging.
allowed-tools: Bash, Read, Write, Edit, WebFetch
---

# tee-router operations

The single reference for production infra, CLIs, URLs, and external API patterns.
Skim this once at the start of any session involving deploys, live tests, or debugging.

## Where you actually work

- Main development happens in **`.worktrees/main/`**, NOT the repo root.
- The repo root `/home/alpinevm/Development/rift/tee-router/` has a stripped-down
  `justfile` (only `cache-devnet`). If `just --list` shows almost nothing, you're
  in the wrong directory — `cd .worktrees/main` or use absolute paths.

## Topology in one paragraph

> **⚠️ No active deployment.** The previous Phala CVM was deleted on 2026-06-02;
> its concrete app id, dashboard, and per-port endpoints have been
> removed from this doc. The deploy tooling below (`etc/compose.phala.yml`,
> `just phala-deploy`) still works for standing up a fresh CVM — after which you
> re-derive the live coordinates via `phala cvms` (see the Phala section).

The TEE is designed to run on a **single Phala CVM** that runs the whole
`etc/compose.phala.yml` stack: router-api, router-worker, temporal-worker,
temporal cluster + temporal-ui, postgres, postgres-replication-gateway, alloy
sidecars. **Railway** runs the public Bun gateway (`router-gateway-v3`) that
fronts the Phala router-api, plus `loki-v3` log aggregation, plus a separate
`hyperunit-socks5-proxy-v3` service. The Bun gateway is the only public surface.

```
client → router-gateway-v3 (Railway/Bun) → router-api (Phala) → temporal-worker (Phala)
                                                                  ↓
                                                       CCTP / Velora / Across / HL / Hyperunit
```

## Phala

**CLI**: `phala` (already authenticated in this env)

**No CVM is currently deployed** (the previous one was deleted 2026-06-02; `phala
cvms list` returns nothing). The steps below stand up a new CVM and recover its
coordinates. Throughout, `<cvm>` is the name you deploy under.

After deploying, capture the CVM's name / app id and derive its public per-port
endpoints:

```bash
phala cvms list                                   # find the CVM name / app id
phala cvms get <cvm> -j | python3 -c 'import json,sys;[print(e["app"]) for e in json.load(sys.stdin).get("endpoints",[])]'
```

The stack exposes **4522** router-api, **5432** postgres-replication-gateway, and
**8080** temporal-ui; the public URL pattern is
`https://<app-id>-<port>.<phala-gateway-base>` (the gateway base, e.g.
`dstack-pha-prodN.phala.network`, depends on the node the CVM lands on).

**Secrets file** (gitignored): `.secrets/phala.env` — every `${VAR:?}` referenced
by `etc/compose.phala.yml` must be set here. Notably:
- `HYPERUNIT_PROXY_URL=socks5://<user>:<pass>@<railway-tcp-proxy-host>:<port>`
- `TEMPORAL_UI_PW_HASH` (bcrypt hash; plaintext is what the operator typed)

**Deploy commands** (run from `.worktrees/main/`, pass your CVM name):

```bash
just phala-deploy 0.2.X phala_cvm=<cvm>    # pin etc/compose.phala.yml to :0.2.X, redeploy CVM
just phala-upgrade phala_cvm=<cvm>         # config-only redeploy (no version bump)
phala cvms get <cvm>                       # status (text)
phala cvms get <cvm> -j                    # status (JSON, includes endpoints)
```

**Don't use `:main` image tags** in compose.phala.yml — always pinned versions.

## Release flow

1. Bump workspace version in `Cargo.toml` (single source: `[workspace.package].version`)
2. `cargo update --workspace --offline` to refresh Cargo.lock
3. Update `etc/compose.phala.yml`: pin `ghcr.io/riftresearch/tee-router:<ver>` and
   `ghcr.io/riftresearch/tee-router-temporal-worker:<ver>` (3 image refs)
4. Commit on `main`, push
5. Tag `v0.2.X` and push the tag — this triggers `.github/workflows/router-image.yml`
   on the **self-hosted runner `tee-router-builder`**
6. Wait for build (`gh run list --workflow=router-image.yml --limit 3`)
7. Verify pushed: `docker manifest inspect ghcr.io/riftresearch/tee-router:0.2.X`
8. `just phala-deploy 0.2.X phala_cvm=<cvm>`
9. Poll until `phala cvms get <cvm>` shows `Status: running`
10. Probe `https://router-gateway-v3-production.up.railway.app/providers` — expect
    `status: ok` with all 6 venues `reachable`

## Railway

**CLI**: `railway` (authenticated; user is Cliff Syner)

**Project**: `tee-router` (id `6cc9f652-7aa7-48a7-b696-cd2b22012bcb`)
**Environment**: `production` (id `064ab24b-cf75-4822-a5a5-bc75372bb128`)
**Workspace**: Atlas

Services in `tee-router/production`:

| Service | Source | Public URL |
|---|---|---|
| `router-gateway-v3` | repo `apps/router-gateway/` (Bun, `railway/router-gateway/Dockerfile`) | `https://router-gateway-v3-production.up.railway.app` |
| `loki-v3` | repo `railway/loki/Dockerfile` | internal |

**Key**: `router-gateway-v3` only auto-deploys when files matching its
`watchPatterns` change: `apps/router-gateway/**` or `railway/router-gateway/**`.
A workspace version bump alone WILL NOT trigger a Railway redeploy — that's
correct, the gateway is decoupled from the Rust release cadence.

The **Hyperunit SOCKS5 proxy** is on a different Railway service in a separate
project — TCP proxies expose it at hosts like `kodama.proxy.rlwy.net:25441`. To
create / rotate the TCP proxy:

```bash
# Use the Skill tool with 'use-railway' for the GraphQL `tcpProxyCreate`
# mutation (it's hidden from default schema, needs includeDeprecated: true).
```

When the SOCKS5 endpoint rotates, update `HYPERUNIT_PROXY_URL` in
`.secrets/phala.env` and `just phala-upgrade`.

## Temporal

**UI**: `<temporal-ui>` — port 8080 on the deployed CVM; derive the URL as in the
Phala section. HTTP basic auth, password set by operator; hash stored in
`.secrets/phala.env` as `TEMPORAL_UI_PW_HASH`.

**Namespace**: `default`

**Workflow ID conventions** (from `crates/router-temporal/src/lib.rs`):

| Pattern | Description |
|---|---|
| `order:<orderId>:execution` | Main order workflow |
| `order:<orderId>:refund:<attemptId>` | Auto-triggered refund |
| `order:<orderId>:refund:manual` | Operator-triggered standalone refund (idempotent) |
| `order:<orderId>:quote-refresh:<failedStepId>` | Quote refresh after a step failure |

**Direct workflow link**:
`<temporal-ui>/namespaces/default/workflows/order:<orderId>:execution`

**Direct workflow history API** (use early when triaging a failing order):
`<temporal-ui>/api/v1/namespaces/default/workflows/order%3A<orderId>%3Aexecution/history?execution.runId=<runId>`

## Public gateway API surface

Base: `https://router-gateway-v3-production.up.railway.app`

| Route | Purpose |
|---|---|
| `GET /health` | Gateway liveness |
| `GET /providers` | Backend's per-venue reachability check (`across`, `cctp`, `hyperliquid`, `hyperliquid_bridge`, `unit`, `velora`) |
| `GET /openapi.json` | Full OpenAPI 3.1 schema |
| `POST /quote` | Get a quote (fields: `from`, `to`, `toAddress`, `fromAmount`, optional `amountFormat`) |
| `POST /order/market` | Create a market order (requires `quoteId`, `fromAddress`, `toAddress`, `idempotencyKey`) |
| `GET /order/{orderId}` | Read order status |

**Asset strings**: `<Chain>.<Token>` e.g. `Base.USDC`, `Bitcoin.BTC`, `Hyperliquid.USDC`.

**Amount format**: `readable` (default; e.g. `"40"`) or `raw` (e.g. `"40000000"`).

**Minimum order size** as of 0.2.14: ~**40 USDC** for routes through Hyperunit
(driven by Hyperunit's 30,000-sat BTC withdrawal floor + 1 USDC HL activation fee).
Quotes below this return 400 with `upstream router API returned 400`.

`api.rift.trade` is **NOT** this codebase — it serves a legacy `Rift Swap Router
API v1.0.0`. Do not confuse the two.

## End-to-end live test (CLI)

The `router-gateway-cli` binary runs the full flow: quote → create order → broadcast
source-chain deposit → optionally watch status.

```bash
cargo build --release --bin router-gateway-cli

set -a && . ./.env && set +a   # loads LIVE_TEST_PRIVATE_KEY etc.

./target/release/router-gateway-cli swap \
  --gateway-url https://router-gateway-v3-production.up.railway.app \
  --rpc-url "$BASE_RPC_URL" \
  --private-key "$LIVE_TEST_PRIVATE_KEY" \
  --from Base.USDC --to Bitcoin.BTC \
  --from-amount 40 --to-address "$ROUTER_LIVE_BTC_RECIPIENT_ADDRESS" \
  -y

./target/release/router-gateway-cli status <orderId> \
  --gateway-url https://router-gateway-v3-production.up.railway.app --watch
```

**Funded-test artifact convention** (always): save full self-contained record
under `live-test-logs/<UTC-timestamp>--<scenario>/`. Per `feedback_funded_test_logs`
memory: summary, full run log, pre/post balances, every on-chain receipt, every
API response. Committed to the repo.

## Test wallet (single funded wallet shared across live tests)

- **EVM address**: `0x33F65788aCa48D733c2C2444Ac9F79B18206aa92`
- **Private key**: `$LIVE_TEST_PRIVATE_KEY` (in `.env`, gitignored)
- **BTC destination**: `$ROUTER_LIVE_BTC_RECIPIENT_ADDRESS` (`bc1qk4m6mpxulnlufegdh3w40kayhx9m722am38apn`)
- Has balances on Base (USDC + ETH for gas), HL spot, occasionally Arb
- Each live test is **money-spending** — confirm with the user before executing

## External APIs

### Hyperliquid

Base: `https://api.hyperliquid.xyz/info` (POST JSON, no auth)

```bash
# Spot account state
curl -sm 10 -X POST https://api.hyperliquid.xyz/info \
  -H 'content-type: application/json' \
  -d '{"type":"spotClearinghouseState","user":"0x..."}'

# Perp account state (HL bridge2 deposits land here first!)
curl -sm 10 -X POST https://api.hyperliquid.xyz/info \
  -H 'content-type: application/json' \
  -d '{"type":"clearinghouseState","user":"0x..."}'

# Recent fills
curl -sm 10 -X POST https://api.hyperliquid.xyz/info \
  -H 'content-type: application/json' \
  -d '{"type":"userFills","user":"0x..."}'

# Deposit/withdraw/transfer ledger
curl -sm 10 -X POST https://api.hyperliquid.xyz/info \
  -H 'content-type: application/json' \
  -d '{"type":"userNonFundingLedgerUpdates","user":"0x...","startTime":<unix-ms>}'

# Open orders
curl -sm 10 -X POST https://api.hyperliquid.xyz/info \
  -H 'content-type: application/json' \
  -d '{"type":"openOrders","user":"0x..."}'
```

**Critical quirk**: HL bridge2 USDC deposits credit the sender's **perp**
account, NOT spot. To trade spot UBTC/UETH/etc., must do `usdClassTransfer`
(perp → spot) first. A "stuck" order that has USDC in perp but not spot is
likely wedged at this step.

**Spot internal IDs**:
- `@142` = UBTC/USDC
- `@151` = UETH/USDC

For market-order signing / `l2Book` queries on these pairs, use `@142`/`@151`
**not** the human names (which return null).

### Hyperunit

Base: `https://api.hyperunit.xyz` — **must use SOCKS5 proxy** (mainnet API is
geofenced). Proxy URL is in `.env` as `HYPERUNIT_PROXY_URL`.

```bash
# Operations for a wallet (deposits + withdrawals)
curl -sm 15 --proxy "$HYPERUNIT_PROXY_URL" \
  https://api.hyperunit.xyz/operations/<wallet>

# Health
curl -sm 15 --proxy "$HYPERUNIT_PROXY_URL" \
  -i https://api.hyperunit.xyz/healthz
```

**Critical facts** (from the deep audit):
- Minimum BTC withdrawal: **30,000 sats** (orders below this end `state: failure`)
- Withdrawal fee: ~715 sats
- Hyperunit hardcodes `coinType=ethereum` in guardian proposals, even for BTC
- Protocol HL address (receives UBTC withdrawals): `0xa0756aF705a4C587511a33912AeFbd249b49e2Cc`
- Stuck funds at the protocol address require manual support contact
  (app.hyperunit.xyz/support or @unitxyz / t.me/hyperunit)
- Operation ID format: `<wallet>:<unix-ms>`
- `state` values: `created`, `signed`, `done`, `failure`

### CCTP (Circle Iris)

Base: `https://iris-api.circle.com`

```bash
# Lookup by burn-tx hash (source domain in the path)
curl -sm 15 "https://iris-api.circle.com/v2/messages/<sourceDomain>?transactionHash=<burn-tx>"

# Lookup by depositor address (sometimes empty even if attestation exists; tx-hash is more reliable)
curl -sm 15 "https://iris-api.circle.com/v2/messages/<sourceDomain>?depositor=0x..."
```

**CCTP domain IDs**:
| Domain | Chain |
|---|---|
| 0 | Ethereum |
| 1 | Avalanche |
| 2 | Optimism |
| 3 | Arbitrum |
| 6 | Base |
| 7 | Polygon |

**Critical fact** (from the deep audit, fixed in 0.2.14):
- Iris V2 response nests `decodedMessageBody` inside `decodedMessage`, **not** at
  the top level. The pre-0.2.14 code read it from the wrong place.
- `minFinalityThreshold` values: `1000` = Fast transfer (~10-20s, pays a fee),
  `2000` = Standard transfer (~13-19 min on Base, free). Router-core currently
  uses `2000` (standard).
  Docs: https://developers.circle.com/cctp/cctp-finality-and-fees

### CCTP V2 burn discovery on EVM (depositor → burn tx)

The orderAddress's CCTP burn is a separate tx after the user's USDC transfer
in. Find it via `eth_getLogs` on TokenMessengerV2 (same address on Base + Arb:
`0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d`):

```bash
PADDED=0x000000000000000000000000<orderAddress-without-0x>
curl -sm 20 -X POST "$BASE_RPC_URL" -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"eth_getLogs",
  "params":[{"fromBlock":"0x<hex-block>","toBlock":"latest",
             "address":"0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d",
             "topics":[null,null,"'"$PADDED"'",null]}]
}'
```

The DepositForBurn V2 event has depositor in `topics[2]` (NOT topics[1] —
topics[1] is `burnToken`, topics[3] is `minFinalityThreshold` as uint32).

### Velora (formerly Paraswap)

Base: `https://api.paraswap.io` (no auth for prices, requires API key for
production). V6 AugustusV6 diamond at `0x6a000f20005980200259b80c5102003040001068`
(same on Eth/Base/Arb).

**Critical fact**: AugustusV6 emits **NO swap event** (V5 SwappedV3 does not
exist in V6). Decoders must gate by `tx.to == AUGUSTUS_V6` and decode method-set
from the vendored ABI at runtime, not by event topic.

### Across

SpokePool emits `FundsDeposited` and `FilledRelay` (not the old `V3FundsDeposited`
/ `FilledV3Relay` — that's V2 terminology, fixed in audit). Router does NOT use
the MulticallHandler.

### Bitcoin

Bitcoin core RPC + Esplora; bitcoin-indexer service exposes its own REST + WebSocket.

**Critical facts** (from audit):
- Compare deposits by `script_pubkey`, NOT by stringified address
- Treat RPC errors by JSON-RPC error code, not message
- `bitcoin-indexer /healthz` exposes a `network: "mainnet"|"testnet"|"regtest"|"signet"` field
- `bitcoin-indexer-client::assert_network_matches(expected)` returns
  `Error::NetworkMismatch` on mismatch

## Important addresses (mainnet)

| Address | What |
|---|---|
| `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` | Base USDC |
| `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` | Arbitrum USDC |
| `0x28b5a0e9C621a5BadaA536219b3a228C8168cf5d` | CCTP V2 TokenMessenger (Base & Arb) |
| `0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7` | Hyperliquid bridge2 (Arbitrum) |
| `0xa0756aF705a4C587511a33912AeFbd249b49e2Cc` | Hyperunit protocol address (HL spot) |
| `0x6a000f20005980200259b80c5102003040001068` | Velora AugustusV6 (all EVM chains) |

## EVM RPCs

| Chain | Env var | Notes |
|---|---|---|
| Ethereum | `$ETH_RPC_URL` | in `.env` |
| Base | `$BASE_RPC_URL` | in `.env` |
| Arbitrum | (none set) | use `https://arb1.arbitrum.io/rpc` directly |

**Etherscan v2** (`$ETHERSCAN_API_KEY` in `.env`): unified endpoint
`https://api.etherscan.io/v2/api?chainid=<id>&...`. **Free plan does NOT support
Base / Arb / most L2s** — for transaction lists on those chains, fall back to
raw RPC + `eth_getLogs`.

## Useful debug recipes

### Is the deployed backend healthy?

```bash
curl -sm 10 https://router-gateway-v3-production.up.railway.app/health
curl -sm 10 https://router-gateway-v3-production.up.railway.app/providers
```

### Where is a stuck order?

For order id `<oid>`:

```bash
# First triage: full Temporal history JSON (<temporal-ui> = port 8080 on the CVM,
# derived as in the Phala section). Requires the workflow runId.
curl -sm 30 "<temporal-ui>/api/v1/namespaces/default/workflows/order%3A<oid>%3Aexecution/history?execution.runId=<runId>" | python3 -m json.tool

# Gateway view (just umbrella state)
curl -sm 10 "https://router-gateway-v3-production.up.railway.app/order/<oid>" | python3 -m json.tool

# Temporal workflow URL:
echo "<temporal-ui>/namespaces/default/workflows/order:<oid>:execution"
```

If gateway shows `executing` for 30+ min, walk on-chain:

1. **Base side**: find the orderAddress from the order JSON, check
   `cast nonce <orderAddr> --rpc-url $BASE_RPC_URL` and
   `cast call <USDC> 'balanceOf(address)(uint256)' <orderAddr>` — both should be
   `nonce>=2`, `USDC=0` once CCTP burn has fired.
2. **CCTP burn tx**: `eth_getLogs` on TokenMessengerV2 with the orderAddress as
   `topics[2]`. The log's `topics[3]` is `minFinalityThreshold` (`0x07d0`=2000).
3. **Iris attestation**: `curl iris-api.circle.com/v2/messages/<sourceDomain>?transactionHash=<burn-tx>` —
   look for `status: "complete"`.
4. **Arb side**: find the mint-recipient (`decodedMessage.recipient`); check its
   HL **perp** account (`clearinghouseState`) AND HL **spot** account
   (`spotClearinghouseState`). USDC stuck in perp means workflow has not yet
   done `usdClassTransfer`.
5. **HL → user transfer**: check test wallet's `userNonFundingLedgerUpdates` for a
   `send` from the mint-recipient → user.
6. **Spot trade**: check test wallet's `userFills` for a `@142` buy.
7. **Hyperunit**: `curl --proxy $HYPERUNIT_PROXY_URL .../operations/<test-wallet>` —
   look for an op with `opCreatedAt` after the order's creation.
8. **BTC broadcast**: the Hyperunit operation's `destinationTxHash`.

## Gotchas to memorize

- **Worktree confusion**: always check `pwd` if something looks off. The main
  worktree has the full `justfile`; the repo root has a stripped one.
- **`.secrets/phala.env`** is gitignored. If a deploy fails with "missing
  required env var", it's almost always a missing secret here.
- **`api.rift.trade`** is a legacy product, not this codebase. Use
  `router-gateway-v3-production.up.railway.app` for current code.
- **HL bridge deposits land in perp, not spot.** Always check both.
- **CCTP V2 attestation can take 13-19 min** on standard finality threshold.
  Not a bug; it's `minFinalityThreshold=2000`.
- **Hyperunit minimum withdrawal is 30,000 sats.** Smaller withdrawals fail.
- **Router-gateway-v3 only redeploys on `apps/router-gateway/**` or
  `railway/router-gateway/**` changes.** A version bump alone does nothing on
  Railway.
- **Self-hosted runner** (`tee-router-builder`) builds the Phala images on tag
  push. If a tag push doesn't trigger a build, check the runner is online.
- **Etherscan free plan doesn't cover Base/Arb** — use raw RPC.
- **Hyperunit must go through SOCKS5** — direct API calls from datacenter IPs
  often get blocked.
