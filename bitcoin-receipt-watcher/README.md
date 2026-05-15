# Bitcoin Receipt Watcher

Thin service for waiting on known Bitcoin transaction IDs. It subscribes to
Bitcoin Core `pubrawblock`, intersects each block's transactions with a bounded
pending set, and emits confirmation events over websocket. A polling loop over
`getblockchaininfo` + `getblock` runs as fallback and gap-fill.

```sh
BITCOIN_RECEIPT_WATCHER_CHAIN=bitcoin \
BITCOIN_RECEIPT_WATCHER_RPC_URL=http://localhost:50100 \
BITCOIN_RECEIPT_WATCHER_RPC_AUTH=devnet:devnet \
BITCOIN_RECEIPT_WATCHER_ZMQ_RAWBLOCK_ENDPOINT=tcp://localhost:50118 \
cargo run -p bitcoin-receipt-watcher
```

## API

`POST /watch`

```json
{
  "chain": "bitcoin",
  "txid": "1111111111111111111111111111111111111111111111111111111111111111",
  "requesting_operation_id": "operation-123"
}
```

Registers a transaction ID in the pending map and emits a `pending` websocket
event. New distinct IDs are rejected with HTTP 429 when the configured cap is
reached.

`DELETE /watch/{txid}`

Cancels a pending watch if present.

`WS /subscribe`

Receives lifecycle events:

```json
{
  "chain": "bitcoin",
  "txid": "1111111111111111111111111111111111111111111111111111111111111111",
  "requesting_operation_id": "operation-123",
  "status": "confirmed",
  "receipt": {
    "tx": {},
    "block_hash": "...",
    "block_height": 102,
    "confirmations": 1
  }
}
```

`GET /healthz` returns `{ "ok": true, "chain": "...", "pending_count": n, "max_pending": n }`.
`GET /metrics` returns Prometheus metrics when the process installed a recorder.

## Configuration

- `BITCOIN_RECEIPT_WATCHER_CHAIN`: chain label accepted by `/watch`
- `BITCOIN_RECEIPT_WATCHER_RPC_URL`: Bitcoin Core HTTP RPC URL
- `BITCOIN_RECEIPT_WATCHER_RPC_AUTH`: `user:password` or `cookie:/path/to/.cookie`
- `BITCOIN_RECEIPT_WATCHER_ZMQ_RAWBLOCK_ENDPOINT`: Bitcoin Core `pubrawblock`
- `BITCOIN_RECEIPT_WATCHER_MAX_PENDING`: pending map cap, default `100000`
- `BITCOIN_RECEIPT_WATCHER_POLL_INTERVAL_MS`: fallback polling interval
