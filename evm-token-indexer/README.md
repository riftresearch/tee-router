# EVM Token Indexer

Ponder-based chain-wide ERC-20 transfer indexer for Sauron deposit detection.

The indexer is chain-agnostic at runtime:

- `PONDER_CHAIN_ID` selects the EVM chain.
- `PONDER_RPC_URL_HTTP` and `PONDER_WS_URL_HTTP` select the node endpoints.
- `PONDER_CONTRACT_START_BLOCK` selects the first block to index.
- `DATABASE_URL` selects the Postgres database.
- `DATABASE_SCHEMA` or `PONDER_SCHEMA` selects the Ponder schema used by the write API.

The app indexes addressless ERC-20 `Transfer` logs. It stores raw rolling transfer
rows, mirrors Sauron's active deposit watches, and materializes durable deposit
candidates for Sauron to submit as router hints.

## API

- `PUT /watches` replaces the active ERC-20 watch mirror for this chain and
  materializes matching candidates.
- `POST /candidates/materialize` backfills candidates by joining active watches
  with raw transfers.
- `GET /candidates/pending?limit=250` returns durable pending candidates.
- `POST /candidates/:id/mark-submitted` marks a candidate delivered after the
  router accepts the hint.
- `POST /candidates/:id/release` records a retryable submission failure.
- `POST /maintenance/prune-raw` deletes raw transfer rows older than the
  configured retention window. Candidates are retained separately.
- `GET /transfers/to/:address?token=0x...&amount=...` is a diagnostic transfer
  lookup over the raw table.
