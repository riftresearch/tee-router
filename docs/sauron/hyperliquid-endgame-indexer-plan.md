# Hyperliquid Indexer — Endgame Plan (Self-Hosted Non-Validator Node)

Captures the architecture and empirical findings for the long-term Hyperliquid data layer. Not the near-term build — see `build-spec.md` for the phased rollout. This document is the destination, not the next step.

## Empirical findings from 2026-05-11 probes

### Test 1: testnet (`wss://api.hyperliquid-testnet.xyz/ws`)

Single connection, sequential subscribes for 15 unique synthetic addresses (`0x...01` through `0x...0F`). Then a second connection on same IP, 15 more unique addresses (`0x...10` through `0x...1E`).

**Result:** all 30 unique users across 2 connections accepted on testnet. No rejections. **Testnet does not enforce the 10-user cap.**

### Test 2: mainnet, sequential (`wss://api.hyperliquid.xyz/ws`)

Single connection, 12 unique synthetic addresses. Close conn1, open conn2, try 1 more.

**Result:**
- conn1 users 1-10: all OK
- conn1 user 11: `{"channel":"error","data":"Cannot track more than 10 total users."}`
- conn1 user 12: same error
- conn1 closed
- conn2 user 16: OK (quota released after conn1 close)

Confirms the cap is real on mainnet. Hard error, not silent drop. Quota releases when the connection holding it closes.

### Test 3: mainnet, concurrent (the disambiguator)

conn1 and conn2 opened simultaneously. conn1 holds 10 users. Try user 11 on conn1 → error (expected). Then try user 11 through 20 on conn2 (same IP, conn1 still open).

**Result:**
- conn1 users 1-10: OK
- conn1 user 11: error
- **conn2 users 11-20 (every single one): error** — `"Cannot track more than 10 total users."`

**Definitive conclusion: the 10-user limit is per-IP, NOT per-connection.** Opening more connections does not add user-slot budget. The wording "10 unique users across user-specific websocket subscriptions" means the union of unique users across ALL your user-specific subscriptions on the IP.

### Implication for the public-API path

To monitor N unique HL accounts simultaneously via api.hyperliquid.xyz, we need ⌈N/10⌉ distinct egress IPs. For hundreds of accounts, that's 30-100 IPs minimum. Operationally heavy.

## The endgame: self-hosted non-validator node

A regular (non-Foundation-tier) non-validator HL node is **fully permissionless**. The 10k HYPE + Maker-Tier-1+ + 98% uptime requirements are for the *Foundation Non-Validating Node Program* — a separate privileged-peering tier. We do not need that program.

### Hardware

Per the Hyperliquid node docs (Manuel Prhyme's article and HL's own GitHub):

- 16 vCPU
- 64 GB RAM
- 500 GB SSD (NVMe preferred — high I/O)
- Ubuntu 24.04 only
- Tokyo region recommended for latency (matters for HFT; less for our indexing case)

Estimated cloud cost: ~$300-500/mo on AWS/GCP for a comparable instance + storage. Cheap relative to vendor tiers ($1,999-$2,500/mo for Hydromancer/HyperPC).

### Setup

```sh
# Chain selection
echo '{"chain": "Mainnet"}' > ~/visor.json

# Download visor (manages node lifecycle, verifies hl-node binary)
curl https://binaries.hyperliquid.xyz/Mainnet/hl-visor > ~/hl-visor
chmod a+x ~/hl-visor
# (verify gpg signature against published pub_key.asc)

# Run with data-writing flags enabled
~/hl-visor run-non-validator \
  --write-fills \
  --write-order-statuses \
  --write-trades \
  --write-raw-book-diffs \
  --write-misc-events \
  --serve-info \
  --disable-output-file-buffering
```

### Ports

- 4001, 4002: gossip (public, must be open inbound for peer discovery)
- 4003-4010: validator-only ranges (we don't use these as a non-validator)
- 3001: local HTTP `/info` server when `--serve-info` is enabled (loopback only)

### Data written to `~/hl/data`

| Path | Content |
|---|---|
| `replica_cmds/{start_time}/{date}/{height}` | Raw transaction blocks (all txs on chain at this height) |
| `periodic_abci_states/{date}/{height}.rmp` | State snapshots (messagepack) |
| Fills (with `--write-fills`) | Stream of every fill across the chain |
| Order statuses (with `--write-order-statuses`) | Every order transition (resting → filled / canceled / etc.) |
| Trades (with `--write-trades`) | Trade events |
| Raw book diffs (with `--write-raw-book-diffs`) | Orderbook deltas |
| Misc events (with `--write-misc-events`) | Liquidations, fundings, etc. |

Volume: ~100 GB/day total. Plan archiving / pruning.

### Local `/info` server

`--serve-info` exposes the standard HL `/info` API at `http://localhost:3001/info`. **Exact endpoint parity** with `api.hyperliquid.xyz/info` is what we need to verify — community indexer products (Hydromancer, HyperPC) build their own user-scoped APIs on top of the disk data, suggesting the local server doesn't expose user-historical queries (`userFills`, `userFillsByTime`, `userNonFundingLedgerUpdates`, `userFunding`). State-dependent queries (`clearinghouseState`, `openOrders`, `spotClearinghouseState`) likely DO work locally.

**Sandbox-verify when we stand up the node:** call each /info endpoint we need against the local server, document which work and which don't. This determines how much of the indexer we have to build vs. consume.

### Data path

```
HL gossip network
       |
       v
Non-validator node (hl-visor run-non-validator)
       |
       +-- writes to ~/hl/data (disk stream)
       |          |
       |          v
       |    Our HL indexer service (tails files, parses, ingests to Postgres)
       |          |
       +-- serves /info on localhost:3001
       |          |
       |    Our HL indexer service (proxies + augments local /info where it works)
       |          |
       v          v
     Uniform indexer API (per build-spec):
       GET  /transfers
       GET  /orders
       POST /prune
       WS   /subscribe
       |
       v
   Sauron HL venue observers
```

### Why this is the right endgame

1. **No api.hyperliquid.xyz dependency in the critical path.** The 10-user cap, 1200-weight `/info` budget, and all other public-API rate limits vanish.
2. **No vendor in the critical path.** Aligns with the TEE-router sovereignty narrative.
3. **Permissionless.** No staking, no approval, no commercial agreement.
4. **Drop-in API.** Our uniform indexer API is the contract; the implementation can swap from "shim over public API" to "own-node indexer" without touching Sauron.
5. **Cost competitive.** ~$300-500/mo infrastructure vs $1,999-$2,500/mo for vendor.
6. **Unlimited user cardinality.** A self-hosted node sees every fill on the chain; user-cardinality is bounded only by our indexer's storage budget.

### Engineering scope

The indexer (the Rust service that consumes node disk output and serves the uniform API):

- Tail `~/hl/data/{replica_cmds,fills,order_statuses,trades,raw_book_diffs,misc_events}` directory trees.
- Parse each format (messagepack for state snapshots; documented formats for the rest).
- Normalize into our unified `HlTransferEvent` and order-status schemas (per `build-spec.md` §3).
- Ingest into Postgres with per-user partitioning.
- Serve `/transfers`, `/orders`, `/prune`, `WS /subscribe` per uniform API.
- Optional: proxy local `/info` for queries it natively supports.

Estimated: 1500-2500 LOC of Rust. ~2-3 weeks focused build + 1-2 weeks of testnet validation and edge-case hardening.

Ops setup (separate work):

- Provision the cloud instance (Ubuntu 24.04, 16 vCPU, 64 GB RAM, 500 GB SSD)
- Configure inbound 4001/4002 for gossip
- Configure seed peers via `~/override_gossip_config.json` if needed (community-run peers in JP/KR/EU/SG)
- Set up `/hl/data` archiving (rotate to S3 or similar before disk fills)
- Health checks: liveness on local /info port, lag against chain head
- Crash log monitoring: `visor_child_stderr`, `node_logs/{consensus,status}`

### Open questions to verify when standing up the node

1. Which /info endpoints does the local `--serve-info` server support? Specifically: does it serve `userFills`, `userFundings`, `userNonFundingLedgerUpdates`, `historicalOrders`? If yes, we can lean on it for parts of the indexer; if no, we build entirely from disk.
2. Format spec for the on-disk fill/order/trade streams. Need to confirm with HL or read the node binary's writer code.
3. Reorg semantics: HyperBFT has fast finality; how does the on-disk data reflect this? Are reorgs ever observed at the disk layer, or only confirmed blocks written?
4. Cold-start replay: if the node restarts, does it backfill the gossip catch-up? Does the indexer need to replay from any cursor?

### When to actually build this

Per `build-spec.md`, this is **phase 5 endgame**, not the near-term path. The near-term build is a shim over the HL public API (see "near-term plan" sibling doc when written). The endgame replaces the shim with own-node indexer once the operational lift is justified by either:

- Active HL account cardinality exceeds the threshold where multi-IP shim management becomes painful (~30+ accounts).
- Strategic priority shift toward sovereignty/cost-control.
- Vendor (if we ever used one) becomes a real risk surface.

The shim and endgame share the **same external API**, so the swap is invisible to Sauron and Sauron's venue observers — pure implementation change.

## References

- HL docs: https://hyperliquid.gitbook.io/hyperliquid-docs
- HL node GitHub: https://github.com/hyperliquid-dex/node
- Rate limits doc: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- Foundation Non-Validating Node program: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/nodes/foundation-non-validating-node
- Probe scripts: `/tmp/hl_ratelimit_probe.py`, `/tmp/hl_concurrent_probe.py` (not committed)
- Probe logs: `/tmp/hl_probe_result.log`, `/tmp/hl_mainnet_result.log`, `/tmp/hl_concurrent_result.log`
- HL API field reference: `docs/sauron/hyperliquid-api-reference.md`
