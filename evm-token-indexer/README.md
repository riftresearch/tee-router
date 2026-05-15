# EVM Token Indexer

Ponder-based chain-wide ERC-20 transfer indexer. The primitive API exposes raw
ERC-20 transfers without Sauron watch or candidate vocabulary; the legacy
Sauron endpoints remain available until the Sauron retrofit is complete.

The indexer is chain-agnostic at runtime:

- `PONDER_CHAIN_ID` selects the EVM chain.
- `PONDER_RPC_URL_HTTP` and `PONDER_WS_URL_HTTP` select the node endpoints.
- `PONDER_CONTRACT_START_BLOCK` selects the first block to index.
- `DATABASE_URL` selects the Postgres database.
- `DATABASE_SCHEMA` or `PONDER_SCHEMA` selects the Ponder schema used by the write API.
- `EVM_TOKEN_INDEXER_API_KEY` is the bearer key required by all API routes.
  Local-only unauthenticated runs must set
  `EVM_TOKEN_INDEXER_ALLOW_UNAUTHENTICATED=true` explicitly. This escape hatch
  is rejected when `NODE_ENV=production` or `RAILWAY_ENVIRONMENT=production`.

The app indexes addressless ERC-20 `Transfer` logs into `erc20_transfer_raw`.
Legacy routes also mirror Sauron's active deposit watches and materialize
durable deposit candidates for Sauron to submit as router hints.

## API

All API routes require `Authorization: Bearer $EVM_TOKEN_INDEXER_API_KEY`.

### Primitive API

- `GET /transfers?to=0x...&token=0x...&from_block=0&min_amount=1&max_amount=100&limit=100&cursor=...`
  returns ascending raw ERC-20 transfers for a recipient. `to` is required;
  `token`, block, amount, limit, and cursor filters are optional. The response
  shape is `{ transfers, nextCursor, hasMore }`; cursors are opaque base64url
  encodings of the last `(block_number, log_index)` tuple.
- `POST /prune` with body
  `{ "relevant_addresses": ["0x..."], "before_block": 12345 }` accepts an
  asynchronous raw-transfer prune job. `before_block` must be at least the
  configured reorg horizon behind the indexed head
  (`EVM_TOKEN_INDEXER_REORG_SAFE_BLOCKS`, default `1000`).
- `WS /subscribe` accepts a first frame
  `{ "action": "subscribe", "filter": { "token_addresses": ["0x..."], "recipient_addresses": ["0x..."], "min_amount": "1", "max_amount": "100" } }`
  and pushes matching frames as `{ "kind": "transfer", "event": ... }`.
  Slow subscribers are disconnected once their pending socket buffer exceeds
  `EVM_TOKEN_INDEXER_WS_MAX_BUFFERED_BYTES` (default `1 MiB`).

### Legacy Sauron API

- `PUT /watches` replaces the active ERC-20 watch mirror for this chain and
  materializes matching candidates.
- `POST /candidates/materialize` backfills candidates by joining active watches
  with raw transfers.
- `GET /candidates/pending?limit=250` returns durable pending candidates.
- `POST /candidates/:id/mark-submitted` marks a candidate delivered after the
  router accepts the hint.
- `POST /candidates/:id/release` records a retryable submission failure.
- `POST /candidates/:id/discard` marks a malformed candidate discarded so it
  cannot permanently block the pending candidate page.
- `POST /maintenance/prune-raw` deletes raw transfer rows older than the
  configured retention window. Candidates are retained separately.
- `GET /transfers/to/:address?token=0x...&amount=...&limit=50` is a diagnostic
  latest-transfer lookup over the raw table.
