# Hyperliquid API Reference — Indexer Design Source

Research compiled 2026-05-11 for the Hyperliquid indexer build-out. Authoritative HL docs: https://hyperliquid.gitbook.io/hyperliquid-docs.

## userFills payload (WS + REST)

WS channel `userFills` and REST `POST /info {"type":"userFills","user":"0x..."}` return the same `WsFill` shape:

```
{
  coin: string,              // "BTC" (perp) | "BTC/USDC" (spot canonical) | "@<n>" (spot indexed)
  px: string,
  sz: string,
  side: "A" | "B",           // A=ask/sell, B=bid/buy
  time: number,              // ms
  startPosition: string,
  dir: string,               // "Open Long" | "Close Short" | "Buy" | "Sell" | ...
  closedPnl: string,
  hash: string,              // L1 hash; "0x000..." for TWAP slices
  oid: number,
  crossed: bool,             // true = taker, false = maker
  fee: string,
  feeToken: string,          // unit of `fee` — USDC for perp, base or quote for spot
  tid: number,
  liquidation?: {...},
  builderFee?: string
}
```

**Notes:**
- Only ONE `coin` per fill; counter-asset is inferred from pair name (spot) or constant (perp = USDC).
- Each match = one fill record (partials emit separately). Aggregation is client-side.
- `aggregateByTime: bool` option exists for display aggregation only — do not use for state-machine logic.
- TWAP slice fills have `hash="0x000..."` and `tid` lives under `userTwapSliceFills` instead.

## userNonFundingLedgerUpdates payload

```
{
  time: number,
  hash: string,            // L1 hash — NOT unique across deltas
  delta: { type: string, ...kind-specific fields }
}
```

Documented `delta.type` variants:

| type | fields |
|---|---|
| `deposit` | `usdc` |
| `withdraw` | `usdc`, `nonce`, `fee` |
| `internalTransfer` | `usdc`, `user`, `destination`, `fee` |
| `subAccountTransfer` | `usdc`, `user`, `destination` |
| `accountClassTransfer` | `usdc`, `toPerp` (spot↔perp) |
| `spotTransfer` | `token`, `amount`, `usdcValue`, `user`, `destination`, `fee`, `nativeTokenFee`, `nonce` |
| `spotGenesis` | `token`, `amount` |
| `liquidation` | `liquidatedNtlPos`, `accountValue`, `leverageType`, `liquidatedPositions` |
| `vaultCreate` / `vaultDeposit` / `vaultDistribution` / `vaultWithdraw` / `vaultLeaderCommission` | vault-specific |
| `rewardsClaim` | rewards delta |
| `cStakingTransfer` | `token`, `amount`, `isDeposit` |

### Critical finding: NO Arbitrum tx hashes on HL-side

- `deposit` delta does NOT contain the source Arbitrum tx hash.
- `withdraw` delta does NOT contain the destination Arbitrum tx hash.

Cross-chain correlation must be done externally by indexing the **Bridge2** contract on Arbitrum (`0x2df1c51e09aecf9cacb7bc98cb1742757f163df7`) and joining by `(user, usdc, nonce)` for withdrawals or `(user, usdc, time-window)` for deposits.

This is a venue-observer concern, NOT an HL-indexer concern. The HL indexer emits HL-side events with `nonce` (where applicable); the HL Bridge venue observer in Sauron joins with the Arbitrum EVM indexer's Bridge2 events.

### Dedup key

`hash` is NOT unique across deltas (multiple deltas can share a single L1 hash, e.g., liquidation + fee). Use compound key:

- `withdraw`: `(user, nonce)`
- `deposit`: `(user, hash, usdc)`
- Others: `(time, hash, type, nonce-or-stable-fields)`

## userFundings payload

```
{
  time: number,
  coin: string,
  usdc: string,        // signed; negative = paid
  szi: string,         // signed position size at funding time
  fundingRate: string
}
```

- One record per (position, funding interval), where funding interval = 1 hour.
- Real USDC flows; must be tracked as value flow.
- Separated from `userNonFundingLedgerUpdates` because volume would otherwise dominate.

## orderUpdates payload

```
{
  order: { oid, coin, side, limitPx, sz, origSz, timestamp, cloid },
  status: string,      // "open" | "filled" | "canceled" | "triggered" | "rejected" | "marginCanceled" | ...
  statusTimestamp: number
}
```

- Keyed by `oid`. Canonical terminal "fully filled" state: `status == "filled"`.
- Use this — NOT userFills summation — to detect order completion.

## Rate limits

### WebSocket per IP

| Limit | Value |
|---|---|
| WS connections per IP | **10** |
| New connections/min per IP | 30 |
| Total subscriptions per IP | 1000 |
| **Unique users across user-specific subscriptions per IP** | **10** ← critical |
| Inbound messages/min per connection | 2000 |
| Simultaneous in-flight `post` messages | 100 |

### REST `/info` per IP

- Aggregate budget: **1200 weight per minute** per IP
- `userFills`, `userFillsByTime`, `userFunding`, `userNonFundingLedgerUpdates`: weight 20 + 1 per 20 items returned
- `l2Book`, `clearinghouseState`, `orderStatus`, `spotClearinghouseState`: weight 2
- `userRole`: weight 60
- `userRateLimit` returns live usage for an address

### Exchange (write) per address

- Initial budget: 10,000 requests + 1 per 1 USDC volume + 1 every 10s
- Cancels capped at `min(limit + 100000, limit × 2)`

## Backfill semantics

| Endpoint | Cap |
|---|---|
| `userFills` (no time params) | 2000 most recent fills, no pagination |
| `userFillsByTime` | 2000 per response, **10,000 most recent fills total reachable** |
| `userFunding` | undocumented; treat as ~2000 per response, no documented lookback cap — confirm in sandbox |
| `userNonFundingLedgerUpdates` | same — undocumented |

Pagination is timestamp-based: advance `startTime` to the last returned `time`.

**Implication for cold-start:** an account with > 10,000 lifetime fills cannot be fully backfilled from `/info`. Either prune-and-accept-truncation, or supplement from on-L1 explorer.

## Spot vs perp discrimination

By `coin` field on `userFills`:
- Perp: bare symbol (`"BTC"`, `"ETH"`, `"HYPE"`)
- Spot canonical: `"<BASE>/<QUOTE>"` (`"PURR/USDC"`)
- Spot indexed: `"@<n>"` (e.g., `"@42"`)

Indexer must:
1. Load `meta` (perp universe) at startup via `POST /info {"type":"meta"}`
2. Load `spotMeta` at startup via `POST /info {"type":"spotMeta"}`
3. Classify by membership

`feeToken` is a hint (perp = USDC always; spot can vary) but not authoritative.

For ledger updates: discriminator is `delta.type`. Spot-side: `spotTransfer`, `spotGenesis`, `cStakingTransfer`. Perp-side: most others. `accountClassTransfer` toggles via `toPerp` boolean.

## Other WS channels worth knowing

| Channel | Purpose | Indexer relevance |
|---|---|---|
| `user` (a.k.a. userEvents) | union of `{fills | funding | liquidation | nonUserCancel}` | Subscribe for `nonUserCancel` (protocol-side cancel reasons not in `orderUpdates`) |
| `webData3` | aggregate user state for UI | Account liveness only, not primary indexing |
| `webData2` | deprecated for fills/ledger | Skip |
| `userTwapSliceFills`, `userTwapHistory` | TWAP slice fills | Only if/when we support TWAP execution |
| `activeAssetData`, `activeAssetCtx` | per-asset mark/oracle/funding | Only if we expose mark price / unrealized PnL |
| `spotState` | spot balance snapshots | Useful for reconciliation against summed `spotTransfer` |
| `notification` | human-facing strings | Low value |
| `allDexsClearinghouseState`, `allDexsAssetCtxs`, `outcomeMetaUpdates` | builder DEXes, prediction markets | Out of scope today |

**Footgun:** the user-events channel name on the wire is `"user"`, not `"userEvents"`.

## Bridge2 (Arbitrum-side, not HL API)

Contract: `0x2df1c51e09aecf9cacb7bc98cb1742757f163df7` on Arbitrum.

Source code: https://github.com/hyperliquid-dex/contracts/blob/master/Bridge2.sol

Relevant events:
- `RequestedWithdrawal` (HL → user requests Arb-side payout)
- `FinalizedWithdrawal` (Arb-side payout finalized)
- `InvalidatedWithdrawal` (withdrawal rejected by Arb side)
- Deposit-side: standard ERC-20 `Transfer` from user to bridge address (no custom event)

The HL Bridge venue observer in Sauron joins:
- HL-side: `userNonFundingLedgerUpdates` with `type=deposit|withdraw` from HL indexer
- Arb-side: Bridge2 events + USDC `Transfer` events from the Arbitrum EVM indexer

Join key for withdrawals: `nonce` (HL `withdraw.nonce` ↔ Bridge2 `FinalizedWithdrawal` nonce).
Join key for deposits: `(user, amount, time-window)` since no shared identifier exists.

## Items unanswerable from public docs (sandbox-verify before relying)

1. Exact max result size and lookback for `userFunding` / `userNonFundingLedgerUpdates` via `/info`.
2. WS disconnect-on-throttle behavior (which limit violations disconnect vs throttle vs drop).
3. End-to-end WS push latency from book match to client.
4. Whether `hash` field can ever be unique across deltas of the same L1 transaction.

## Authoritative source URLs

- WebSocket Subscriptions: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Info Endpoint: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint
- Info Endpoint / Perpetuals: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
- Info Endpoint / Spot: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
- Rate Limits: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits
- Bridge2: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/bridge2
- Bridge2.sol: https://github.com/hyperliquid-dex/contracts/blob/master/Bridge2.sol
- userNonFundingLedgerUpdates (Chainstack mirror): https://docs.chainstack.com/reference/hyperliquid-info-user-non-funding-ledger-updates
