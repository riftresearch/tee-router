---
name: live-local-quote-matrix
description: >
  Bring up a fresh live-local stack (real venues, real RPCs) and run a read-only
  quote matrix against the gateway: many token/chain combinations with amount
  ladders from dust to past-liquidity-boundary. No orders are created and no
  funds move. Use to validate quoting changes against real venue books before a
  release, or to probe route/venue coverage and size limits.
allowed-tools: Bash, Read, Write
---

# Live-local quote matrix

Fresh live-local stack → read-only `/quote` sweeps across route families ×
amount ladders → interpret every non-200 → tear down. **Quotes are free and
spend nothing**; the stack talks to real venue APIs (Hyperliquid, Hyperunit,
Across, CCTP, Velora, Garden, ChangeNow, …) and real chain RPCs, so results
reflect live books and live liquidity.

## Prerequisites

- Run from **`.worktrees/main/`** (the full `justfile`; the repo root has a
  stripped one). Docker running.
- **`.env.live-local`** exists (copy `etc/env.live-local.example`, fill real
  RPCs + venue keys). `:?`-guarded vars fail `up` loudly; venue keys that are
  `:-`-optional (e.g. `CHANGENOW_API_KEY`) just bench that venue if missing —
  real keys live in `.secrets/*.env` (gitignored).
- The matrix runner lives next to this skill: `quote_matrix.py`.

## 1. Fresh stack up (build from source)

```bash
just live-local down -v        # wipe any previous live-local state
just live-local up -d --build  # rebuild router-api/worker/gateway from the working tree
until curl -sm 3 http://localhost:3001/health >/dev/null; do sleep 3; done
```

⚠️ Without `--build`, compose reuses stale images — local code changes won't
be what you're testing.

## 2. Run the matrix

```bash
RUN_DIR="live-test-logs/$(date -u +%Y%m%dT%H%M%SZ)--quote-matrix"
python3 .claude/skills/live-local-quote-matrix/quote_matrix.py "$RUN_DIR"
```

The script POSTs `/quote` (`{"from","to","fromAmount","amountFormat":"readable"}`
— there is **no `toAddress`** in the current schema) for every (route, amount)
case, prints a live table, and writes `results.jsonl` + summary counts to
`$RUN_DIR`. Edit its `CASES` list to add pairs; design ladders to span:

- **dust** (expect a viable-floor rejection on BTC-bound routes),
- **small/medium/large** (expect 200s; sanity-check the effective price),
- **huge** (expect `INSUFFICIENT_LIQUIDITY` on order-book-bound routes,
  e.g. anything with a Hyperliquid spot leg; stable↔stable via CCTP keeps
  quoting into the tens of millions).

Keep `max_workers` ≤ 2 and the per-quote pacing — **Velora's unauthenticated
price API 429s easily**, and multi-hop paths that need a Velora leg fail while
it's limited.

Single ad-hoc quote (e.g. to retry an anomaly), with per-venue debug:

```bash
curl -s -X POST http://localhost:3001/quote -H 'content-type: application/json' \
  -d '{"from":"Ethereum.WBTC","to":"Bitcoin.BTC","fromAmount":"0.05",
       "amountFormat":"readable","includeQuoteCandidates":true}'
```

(`quoteCandidates` — per-venue `success`/`no_route`/`error` — only appears on
2xx responses.)

## 3. Interpret every non-200 (the actual work)

| Response | Meaning | Action |
|---|---|---|
| `422 INSUFFICIENT_LIQUIDITY` | venue-agnostic classification: best route died of liquidity exhaustion | Correct behavior at huge sizes — record the boundary |
| `502 UPSTREAM_ERROR` + "below the viable floor 30000" | dust: output under Hyperunit's 30k-sat withdrawal floor | Correct behavior |
| `422 UPSTREAM_ERROR` + Velora "429 Too Many Requests" | venue weather, not a routing bug | Pace harder / wait; only a finding if it persists at low rate |
| `502` "upstream router API request failed" | gateway→router-api transport blip | Retry once; a finding only if reproducible |
| Anything else | **a real finding** | `docker logs tee-router-live-local-router-api-1` and grep the per-path `WARN`s — the public message is one representative error; the full per-path story is only in the logs |

A silent failure mode to watch: a route family returning *nothing* (no venues
attempted, no candidate rows, no log lines) usually means an upstream guard is
filtering assets before venues are consulted — compare against production
(`https://router-gateway-v3-production.up.railway.app/quote`, read-only) to
tell a regression from a venue outage.

## 4. Record + tear down

Keep the run record in `live-test-logs/<ts>--<scenario>/` (results.jsonl, the
run log, a summary.md of findings) — read-only quote runs follow the same
record discipline as funded tests. Then:

```bash
just live-local down -v
```

Don't leave the stack running: it polls real venue APIs in the background
(route-cost refresh) and burns rate limits while idle.

## Reference

- **Ports** (host): gateway `3001`, admin-dashboard `3000`, grafana `3002`,
  router-api metrics `9100` (`tee_router_upstream_requests_total{service="hyperliquid",...}`
  is how you confirm cache behavior / venue call counts).
- Asset strings are `<Chain>.<Token>` (`Bitcoin.BTC`, `Ethereum.WBTC`,
  `Hyperliquid.UBTC`, …); `amountFormat: "readable"` takes human-decimal
  amounts.
- `scripts/live-local-venue-quote-matrix.sh` predates the current `/quote`
  schema (sends `toAddress`) — prefer this skill's `quote_matrix.py`.
- For deploy/venue-API/debug details see the **`tee-router-ops`** skill; for
  the order-flow smoke (devnet, funded, settlement-verified) see
  **`local-loadgen-smoke`**.
