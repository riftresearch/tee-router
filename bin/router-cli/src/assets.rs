//! Built-in table of source assets the CLI can sign and broadcast deposits for.
//!
//! Only the *source* side needs an entry — the CLI signs the deposit on the
//! source chain. The destination asset is passed through to the gateway as an
//! opaque identifier. Values mirror the gateway's own asset table
//! (`apps/router-gateway/src/assets.ts`).

/// Chain family of a source asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceChain {
    /// EVM chain identified by its numeric chain id.
    Evm { chain_id: u64 },
    /// Bitcoin mainnet.
    Bitcoin,
}

/// Token kind for an EVM source asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvmToken {
    /// The chain's native coin (ETH).
    Native,
    /// An ERC-20 token at the given lowercase hex contract address.
    Erc20 { address: &'static str },
}

/// A source asset the CLI knows how to deposit.
#[derive(Debug, Clone, Copy)]
pub struct SourceAsset {
    /// Canonical identifier sent to the gateway, e.g. `Ethereum.USDC`.
    pub identifier: &'static str,
    pub chain: SourceChain,
    /// Token kind. Unused for Bitcoin (always treated as native BTC).
    pub evm_token: EvmToken,
}

const SOURCE_ASSETS: &[SourceAsset] = &[
    SourceAsset {
        identifier: "Bitcoin.BTC",
        chain: SourceChain::Bitcoin,
        evm_token: EvmToken::Native,
    },
    SourceAsset {
        identifier: "Ethereum.ETH",
        chain: SourceChain::Evm { chain_id: 1 },
        evm_token: EvmToken::Native,
    },
    SourceAsset {
        identifier: "Ethereum.USDC",
        chain: SourceChain::Evm { chain_id: 1 },
        evm_token: EvmToken::Erc20 {
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
        },
    },
    SourceAsset {
        identifier: "Ethereum.USDT",
        chain: SourceChain::Evm { chain_id: 1 },
        evm_token: EvmToken::Erc20 {
            address: "0xdac17f958d2ee523a2206206994597c13d831ec7",
        },
    },
    SourceAsset {
        identifier: "Ethereum.CBBTC",
        chain: SourceChain::Evm { chain_id: 1 },
        evm_token: EvmToken::Erc20 {
            address: "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        },
    },
    SourceAsset {
        identifier: "Arbitrum.USDC",
        chain: SourceChain::Evm { chain_id: 42161 },
        evm_token: EvmToken::Erc20 {
            address: "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
        },
    },
    SourceAsset {
        identifier: "Arbitrum.USDT",
        chain: SourceChain::Evm { chain_id: 42161 },
        evm_token: EvmToken::Erc20 {
            address: "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
        },
    },
    SourceAsset {
        identifier: "Arbitrum.CBBTC",
        chain: SourceChain::Evm { chain_id: 42161 },
        evm_token: EvmToken::Erc20 {
            address: "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        },
    },
    SourceAsset {
        identifier: "Base.ETH",
        chain: SourceChain::Evm { chain_id: 8453 },
        evm_token: EvmToken::Native,
    },
    SourceAsset {
        identifier: "Base.USDC",
        chain: SourceChain::Evm { chain_id: 8453 },
        evm_token: EvmToken::Erc20 {
            address: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
        },
    },
    SourceAsset {
        identifier: "Base.USDT",
        chain: SourceChain::Evm { chain_id: 8453 },
        evm_token: EvmToken::Erc20 {
            address: "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
        },
    },
    SourceAsset {
        identifier: "Base.CBBTC",
        chain: SourceChain::Evm { chain_id: 8453 },
        evm_token: EvmToken::Erc20 {
            address: "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
        },
    },
];

/// Resolve a `--from` identifier (case-insensitive) to a known source asset.
pub fn resolve_source(identifier: &str) -> eyre::Result<SourceAsset> {
    let wanted = identifier.trim().to_ascii_lowercase();
    SOURCE_ASSETS
        .iter()
        .copied()
        .find(|asset| asset.identifier.to_ascii_lowercase() == wanted)
        .ok_or_else(|| {
            let supported = SOURCE_ASSETS
                .iter()
                .map(|asset| asset.identifier)
                .collect::<Vec<_>>()
                .join(", ");
            eyre::eyre!("unsupported source asset `{identifier}`. supported: {supported}")
        })
}
