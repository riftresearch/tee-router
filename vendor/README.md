# `vendor/` — canonical venue ABIs

This directory holds verified-contract **ABI JSON** files for the on-chain
venues we integrate with (CCTP, Velora, Hyperliquid Bridge2, Across, …),
pulled directly from Etherscan v2 verified contracts.

## Why

Hand-rolled `sol! { event Foo(...) }` blocks in observer/sender code have
hallucinated event shapes more than once (see PR #3 / the May 2026 venue
audit). Pinning the canonical ABI JSON here — and consuming it via
`alloy::sol!(macro_path = "...")` instead of hand-writing types —
eliminates that failure mode at the type-system level.

## Policy

- **ABI JSON only.** No vendored Solidity sources, no git submodules of
  contract repos. JSON is small, diffable, and is exactly what the EVM
  emits.
- **Raw ABI array files** so `alloy::sol!(Name, "vendor/<venue>/Foo.abi.json")`
  consumes them directly without any preprocessing.
- **Single source-of-truth: Etherscan v2.** The deployed verified contract
  is what executes on-chain; the vendored ABI must match that or our
  decoders will diverge from reality.
- **Proxy auto-resolution.** Most major venues deploy behind a
  TransparentUpgradeableProxy. `scripts/vendor-abi.py` resolves
  `Proxy=1` addresses to the implementation and vendors the impl's ABI,
  which is the one with the real events and functions.
- **Provenance.** Each venue directory contains a `PROVENANCE.md` table
  recording `chain id`, `queried address`, `impl address`, `is proxy`,
  and `fetched (UTC)` per ABI file. The vendor script updates this row
  on every refresh.

## Refreshing an ABI

```bash
# Requires ETHERSCAN_API_KEY in .env or the env.
./scripts/vendor-abi.py \
    --chain 1 \
    --address 0x28b5a0e9c621a5badaa536219b3a228c8168cf5d \
    --out vendor/cctp/TokenMessengerV2.abi.json
```

The single Etherscan v2 key works across all supported chains (1=Ethereum,
8453=Base, 42161=Arbitrum, 10=Optimism, …) — pass `--chain` accordingly.

## Layout

```
vendor/
├── cctp/
│   ├── TokenMessengerV2.abi.json        # CCTP V2 burn-chain contract
│   ├── MessageTransmitterV2.abi.json    # CCTP V2 receive-chain contract
│   └── PROVENANCE.md                    # per-file: chain, address, impl, fetched_at
└── …                                    # one directory per venue, added as venues are audited
```

## Companion

Captured real responses (JSON payloads from Iris, REST API responses,
etc.) live in [`crates/venue-fixtures/`](../crates/venue-fixtures/). The
two together form the "tier 1" differential-test inputs — ABIs pin
on-chain shapes; fixtures pin off-chain API shapes.
