# Router v3 Railway Services

This directory tracks the Railway-side pieces for the router v3 alpha
deployment. Do not mutate existing Railway services for v3 unless explicitly
called out here.

## Service Names

Create new services with `-v3` suffix:

- `router-postgres-replica-v3`
- `router-replica-setup-v3`
- `sauron-worker-v3`
- `evm-token-indexer-ethereum-v3`
- `evm-token-indexer-base-v3`
- `evm-token-indexer-arbitrum-v3`

Exception:

- Reuse the existing `sauron-bitcoin-rathole-broker`.
- Reconfigure the existing `hyperunit-socks5-proxy` to expose an authenticated
  Railway TCP proxy reachable by Phala.

## Sauron v3

`sauron-worker-v3` should build from this repository using:

```env
RAILWAY_DOCKERFILE_PATH=etc/Dockerfile.sauron
```

It connects to:

- Railway replica Postgres through `ROUTER_REPLICA_DATABASE_URL`
- public Phala router API through `ROUTER_INTERNAL_BASE_URL`
- existing Bitcoin rathole broker over Railway private networking
- new v3 Ponder indexers over Railway private networking

Bitcoin transport:

```env
BITCOIN_RPC_URL=http://sauron-bitcoin-rathole-broker.railway.internal:40031
BITCOIN_ZMQ_RAWTX_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40032
BITCOIN_ZMQ_SEQUENCE_ENDPOINT=tcp://sauron-bitcoin-rathole-broker.railway.internal:40033
```

Do not commit credentialed `BITCOIN_RPC_AUTH` values.

## Railway Replica

The replica should follow the legacy `etc/compose.replica.yml` model:

1. connect to Phala primary Postgres through `PHALA_DB_SNI`
2. run a Railway Postgres service
3. copy schema from the Phala primary
4. create a logical subscription to `router_all_tables`

The exact new `PHALA_DB_SNI` value is not known until the Phala stack exists.

## Ponder v3 Indexers

Create three new Ponder services, one per chain.

Use the `evm-token-indexer` directory as the Railway service root directory and
use:

```env
RAILWAY_DOCKERFILE_PATH=Dockerfile.index
```

The credentialed QuickNode HTTP/WS URLs must be configured as Railway variables
only. Do not commit them.

Each service needs:

```env
DATABASE_URL=<ponder postgres url>
DATABASE_SCHEMA=<chain-specific schema>
PONDER_SCHEMA=<same as DATABASE_SCHEMA>
PONDER_CHAIN_ID=<chain id>
PONDER_RPC_URL_HTTP=<chain HTTP RPC>
PONDER_WS_URL_HTTP=<chain WS RPC>
PONDER_CONTRACT_START_BLOCK=<chain start block>
PONDER_DISABLE_CACHE=false
PONDER_LOG_LEVEL=info
PONDER_PORT=4001
EVM_TOKEN_INDEXER_RAW_RETENTION_SECONDS=2592000
```

Suggested schemas:

| Chain | Schema | Chain ID | Start block |
| --- | --- | ---: | ---: |
| Ethereum | `ethereum_v3` | `1` | `24967646` |
| Base | `base_v3` | `8453` | `45229327` |
| Arbitrum | `arbitrum_v3` | `42161` | `456704761` |

These start blocks correspond to `2026-04-27T00:00:00Z` within one second on
each chain.

## HyperUnit SOCKS5 Proxy

The existing `hyperunit-socks5-proxy` service should be exposed with Railway TCP
proxy, not a normal HTTP domain.

Keep:

```env
REQUIRE_AUTH=true
ALLOWED_DEST_FQDN=^api\.hyperunit\.xyz$
```

Set a fresh random:

```env
PROXY_USER=router-v3
PROXY_PASSWORD=<generated secret>
```

Do not configure `ALLOWED_IPS` for now.

Phala `router-worker` should use:

```env
HYPERUNIT_PROXY_URL=socks5://router-v3:<generated secret>@<RAILWAY_TCP_PROXY_DOMAIN>:<RAILWAY_TCP_PROXY_PORT>
```
