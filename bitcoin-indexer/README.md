# Bitcoin Indexer

Thin Bitcoin output indexer shim over Bitcoin Core ZMQ and electrs/Esplora.
It exposes address output queries and websocket output subscriptions for
Sauron-style chain observers.

```sh
BITCOIN_INDEXER_NETWORK=regtest \
BITCOIN_INDEXER_RPC_URL=http://localhost:50100 \
BITCOIN_INDEXER_RPC_AUTH=devnet:devnet \
BITCOIN_INDEXER_ESPLORA_URL=http://localhost:50110 \
BITCOIN_INDEXER_ZMQ_RAWBLOCK_ENDPOINT=tcp://localhost:50118 \
BITCOIN_INDEXER_ZMQ_RAWTX_ENDPOINT=tcp://localhost:50111 \
cargo run -p bitcoin-indexer
```

## API

`GET /tx_outputs?address=<addr>&from_block=<n>&min_amount=<sats>&limit=<n>&cursor=<opaque>`

Returns:

```json
{
  "outputs": [
    {
      "txid": "...",
      "vout": 0,
      "address": "bcrt1...",
      "amount_sats": 50000,
      "block_height": 102,
      "block_hash": "...",
      "block_time": 1710000000,
      "confirmations": 1,
      "removed": false
    }
  ],
  "nextCursor": null,
  "hasMore": false
}
```

`WS /subscribe`

First client frame:

```json
{"action":"subscribe","filter":{"addresses":["bcrt1..."],"min_amount":50000}}
```

Server frames:

```json
{"kind":"subscribed","subscription_id":null}
{"kind":"tx_output","event":{"txid":"...","vout":0,"address":"bcrt1...","amount_sats":50000,"block_height":null,"block_hash":null,"block_time":null,"confirmations":0,"removed":false}}
```

Reorg retractions are sent as the same `tx_output` event with `removed: true`.

`GET /healthz` returns service liveness and the last processed block height.
`GET /metrics` returns Prometheus metrics when the process installed a recorder.

## Configuration

- `BITCOIN_INDEXER_NETWORK`: `bitcoin`, `testnet`, `signet`, or `regtest`
- `BITCOIN_INDEXER_RPC_URL`: Bitcoin Core HTTP RPC URL
- `BITCOIN_INDEXER_RPC_AUTH`: `user:password` or `cookie:/path/to/.cookie`
- `BITCOIN_INDEXER_ESPLORA_URL`: electrs/Esplora HTTP URL
- `BITCOIN_INDEXER_ZMQ_RAWBLOCK_ENDPOINT`: Bitcoin Core `pubrawblock`
- `BITCOIN_INDEXER_ZMQ_RAWTX_ENDPOINT`: Bitcoin Core `pubrawtx`
- `BITCOIN_INDEXER_REORG_RESCAN_DEPTH`: default `6`
