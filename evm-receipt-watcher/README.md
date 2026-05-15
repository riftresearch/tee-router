# EVM Receipt Watcher

Thin per-chain service for waiting on known EVM transaction hashes. It keeps a
single `eth_subscribe("newHeads")` websocket subscription when available,
intersects each block's transaction hashes with an in-memory pending set, fetches
receipts only for matches, and emits confirmed receipt events over websocket.

Run one process per chain:

```sh
EVM_RECEIPT_WATCHER_CHAIN=base \
EVM_RECEIPT_WATCHER_HTTP_RPC_URL=http://localhost:50102 \
EVM_RECEIPT_WATCHER_WS_RPC_URL=ws://localhost:50102 \
cargo run -p evm-receipt-watcher
```

## API

`POST /watch`

```json
{
  "chain": "base",
  "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
  "requesting_operation_id": "operation-123"
}
```

Registers a transaction hash in the bounded pending map. The service emits a
`pending` websocket event after registration and rejects new distinct hashes with
HTTP 429 when the configured cap is reached.

`DELETE /watch/{tx_hash}`

Cancels a pending watch if present.

`WS /subscribe`

Receives watch lifecycle events:

```json
{
  "chain": "base",
  "tx_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
  "requesting_operation_id": "operation-123",
  "status": "confirmed",
  "receipt": {},
  "logs": []
}
```

`GET /healthz`

Returns `{ "ok": true, "chain": "...", "pending_count": n, "max_pending": n }`.

`GET /metrics`

Returns Prometheus metrics for RPC calls, block scans, pending set size,
confirmed receipts, degraded polling mode, and websocket subscriber count.

## Configuration

- `EVM_RECEIPT_WATCHER_CHAIN`: chain label accepted by `/watch`
- `EVM_RECEIPT_WATCHER_HTTP_RPC_URL`: HTTP RPC endpoint
- `EVM_RECEIPT_WATCHER_WS_RPC_URL`: websocket RPC endpoint; if unset or down,
  the watcher polls `eth_blockNumber`
- `EVM_RECEIPT_WATCHER_BIND`: API bind address, default `0.0.0.0:8080`
- `EVM_RECEIPT_WATCHER_MAX_PENDING`: pending map cap, default `100000`
- `EVM_RECEIPT_WATCHER_POLL_INTERVAL_MS`: fallback polling interval
