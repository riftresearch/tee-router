# router-loadgen

Rust CLI for exercising the public router gateway quote/order flow.

The tool intentionally talks to the gateway instead of the internal router API.
Environment selection is just configuration: swap gateway URLs, chain RPC URLs,
and funding private keys to move between local devnet and another environment.

Example against the local compose stack:

```sh
cargo run -p router-loadgen -- create-and-fund \
  --gateway-url http://localhost:3001 \
  --from Base.USDC \
  --to Ethereum.USDC \
  --from-amount 10000000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111 \
  --evm-rpc evm:8453=http://localhost:50102 \
  --devnet-manifest-url http://localhost:50108/manifest.json
```

For the local deterministic devnet, prefer the manifest-backed loadgen key pool
instead of the Anvil/paymaster key:

```sh
cargo run -p router-loadgen -- create-and-fund \
  --gateway-url http://localhost:3001 \
  --from Base.USDC \
  --to Ethereum.USDC \
  --from-amount 100000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111 \
  --evm-rpc evm:8453=http://localhost:50102 \
  --devnet-manifest-url http://localhost:50108/manifest.json
```

Random mode chooses a random executable route template covering EVM USDC,
Bitcoin BTC, and EVM ETH exits for BTC-source routes. It also randomizes
exact-in/exact-out where the local router currently supports exact-out, plus a
raw amount in the configured bounds. When Bitcoin is selected, the devnet
manifest supplies the demo Bitcoin address unless `--bitcoin-address` is set:

```sh
cargo run -p router-loadgen -- create-and-fund \
  --gateway-url http://localhost:3001 \
  --random \
  --random-min-raw-amount 100000 \
  --random-max-raw-amount 10000000 \
  --amount-format raw \
  --to-address 0x1111111111111111111111111111111111111111 \
  --evm-rpc evm:1=http://localhost:50101 \
  --evm-rpc evm:8453=http://localhost:50102 \
  --evm-rpc evm:42161=http://localhost:50103 \
  --devnet-manifest-url http://localhost:50108/manifest.json \
  --bitcoin-rpc-url http://localhost:50100/wallet/alice \
  --bitcoin-rpc-auth devnet:devnet
```

Use `--funding-mode skip` to leave orders in `pending_funding`, `--funding-mode
half` to intentionally underfund, and `--count`, `--concurrency`, and `--rps`
for load generation.

Bitcoin source orders can be funded from a Bitcoin Core wallet RPC:

```sh
--bitcoin-rpc-url http://localhost:50100/wallet/alice \
--bitcoin-rpc-auth devnet:devnet
```
