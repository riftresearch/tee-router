# Sauron Railway Deployment

Sauron is the router-side provider-operation observer. It watches the router
replica database for external provider addresses, observes chain activity, and
submits provider-operation hints back to the router API.

Current V0 scope:

- Watches `order_provider_addresses` for `unit_deposit` provider operations.
- Watches pending funding vault addresses in `deposit_vaults`.
- Uses Postgres `LISTEN/NOTIFY` on `sauron_watch_set_changed`, plus periodic
  full reconciliation.
- Persists per-backend scan cursors in `sauron_backend_cursors`.
- Observes Bitcoin native deposits through Bitcoin Core/Esplora.
- Observes EVM ERC-20 deposits by deriving active token contracts from the
  router watch set and scanning those contracts with RPC logs. The optional
  token indexer is still used for indexed lookup/backfill when it can validate
  the watched token from transaction receipts.
- Observes EVM native deposits through full-block transaction scans while
  Sauron is running.
- Posts non-authoritative wakeup hints to `POST /api/v1/hints`.

Required router environment:

- `ROUTER_REPLICA_DATABASE_URL`
- `ROUTER_REPLICA_DATABASE_NAME`
- `ROUTER_REPLICA_NOTIFICATION_CHANNEL`
- `ROUTER_INTERNAL_BASE_URL`
- `ROUTER_DETECTOR_API_KEY`

Required chain observer environment:

- `ELECTRUM_HTTP_SERVER_URL`
- `BITCOIN_RPC_URL`
- `BITCOIN_RPC_AUTH`
- `BITCOIN_ZMQ_RAWTX_ENDPOINT`
- `BITCOIN_ZMQ_SEQUENCE_ENDPOINT`
- `ETH_RPC_URL`
- `ETHEREUM_TOKEN_INDEXER_URL`
- `ETHEREUM_ALLOWED_TOKEN` legacy compatibility value; not the ERC-20 watch limit
- `BASE_RPC_URL`
- `BASE_TOKEN_INDEXER_URL`
- `BASE_ALLOWED_TOKEN` legacy compatibility value; not the ERC-20 watch limit
- `ARBITRUM_RPC_URL`
- `ARBITRUM_TOKEN_INDEXER_URL`
- `ARBITRUM_ALLOWED_TOKEN` legacy compatibility value; not the ERC-20 watch limit
