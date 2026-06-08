pub mod chainflip;
pub mod garden;
pub mod mayan;
pub mod near_intents;
pub mod relay;

pub use chainflip::CHAINFLIP_ASSET_MAP_SPEC;
pub use garden::GARDEN_ASSET_MAP_SPEC;
pub use mayan::MAYAN_ASSET_MAP_SPEC;
pub use near_intents::NEAR_INTENTS_ASSET_MAP_SPEC;
pub use relay::RELAY_ASSET_MAP_SPEC;

use crate::{
    protocol::{AssetId, ChainId, DepositAsset},
    services::asset_registry::ProviderId,
};
use router_primitives::normalize_evm_address;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueAssetPolicy {
    ListedOnly,
    ListedPlusOpenContracts(ContractPassthrough),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractPassthrough {
    pub standard: ContractStandard,
    pub encoding: ContractEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractStandard {
    Erc20,
    Spl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractEncoding {
    LowercaseHex,
    Raw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouterAssetKind {
    Native,
    Reference(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueAssetEntry {
    pub router_asset: RouterAssetKind,
    pub venue_asset: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueChainAssetsSpec {
    pub router_chain: &'static str,
    pub venue_chain: &'static str,
    pub asset_policy: VenueAssetPolicy,
    pub assets: &'static [VenueAssetEntry],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VenueAssetMapSpec {
    pub provider: ProviderId,
    pub chains: &'static [VenueChainAssetsSpec],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenueAsset {
    pub chain: String,
    pub asset: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VenueAssetLookup {
    Listed(VenueAsset),
    OpenContract(VenueAsset),
    UnsupportedChain,
    UnsupportedAsset,
}

impl VenueAssetLookup {
    #[must_use]
    pub fn into_asset(self) -> Option<VenueAsset> {
        match self {
            Self::Listed(asset) | Self::OpenContract(asset) => Some(asset),
            Self::UnsupportedChain | Self::UnsupportedAsset => None,
        }
    }

    #[must_use]
    pub fn as_asset(&self) -> Option<&VenueAsset> {
        match self {
            Self::Listed(asset) | Self::OpenContract(asset) => Some(asset),
            Self::UnsupportedChain | Self::UnsupportedAsset => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VenueAssetMap {
    provider: ProviderId,
    by_router_chain: HashMap<String, ChainIndex>,
    by_venue_chain: HashMap<String, ChainIndex>,
}

#[derive(Debug, Clone)]
struct ChainIndex {
    router_chain: ChainId,
    venue_chain: String,
    asset_policy: VenueAssetPolicy,
    router_to_venue: HashMap<RouterAssetKey, String>,
    venue_to_router: HashMap<String, RouterAssetKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum RouterAssetKey {
    Native,
    Reference(String),
}

impl VenueAssetMap {
    pub fn new(spec: VenueAssetMapSpec) -> Result<Self, String> {
        let mut by_router_chain = HashMap::with_capacity(spec.chains.len());
        let mut by_venue_chain = HashMap::with_capacity(spec.chains.len());

        for chain_spec in spec.chains {
            let chain_index = ChainIndex::new(*chain_spec)?;
            let router_chain_key = chain_index.router_chain.as_str().to_string();
            let venue_chain_key = chain_index.venue_chain.clone();

            if by_router_chain.contains_key(&router_chain_key) {
                return Err(format!(
                    "duplicate router chain {} in {} asset map",
                    router_chain_key,
                    spec.provider.as_str()
                ));
            }
            if by_venue_chain.contains_key(&venue_chain_key) {
                return Err(format!(
                    "duplicate venue chain {} in {} asset map",
                    venue_chain_key,
                    spec.provider.as_str()
                ));
            }

            by_router_chain.insert(router_chain_key, chain_index.clone());
            by_venue_chain.insert(venue_chain_key, chain_index);
        }

        Ok(Self {
            provider: spec.provider,
            by_router_chain,
            by_venue_chain,
        })
    }

    #[must_use]
    pub fn provider(&self) -> ProviderId {
        self.provider
    }

    #[must_use]
    pub fn to_venue_asset(&self, asset: &DepositAsset) -> VenueAssetLookup {
        let Some(chain) = self.by_router_chain.get(asset.chain.as_str()) else {
            return VenueAssetLookup::UnsupportedChain;
        };
        let Some(router_key) = router_asset_key(&asset.chain, &asset.asset) else {
            return VenueAssetLookup::UnsupportedAsset;
        };
        if let Some(venue_asset) = chain.router_to_venue.get(&router_key) {
            return VenueAssetLookup::Listed(VenueAsset {
                chain: chain.venue_chain.clone(),
                asset: venue_asset.clone(),
            });
        }
        if let Some(encoded) = encode_open_contract(chain.asset_policy, &asset.chain, &asset.asset)
        {
            return VenueAssetLookup::OpenContract(VenueAsset {
                chain: chain.venue_chain.clone(),
                asset: encoded,
            });
        }
        VenueAssetLookup::UnsupportedAsset
    }

    #[must_use]
    pub fn to_router_asset(&self, asset: &VenueAsset) -> Option<DepositAsset> {
        let chain = self.by_venue_chain.get(&asset.chain)?;
        if let Some(router_key) = chain.venue_to_router.get(&asset.asset) {
            return Some(DepositAsset {
                chain: chain.router_chain.clone(),
                asset: router_key.to_asset_id(),
            });
        }
        decode_open_contract(chain.asset_policy, &chain.router_chain, &asset.asset).map(|asset| {
            DepositAsset {
                chain: chain.router_chain.clone(),
                asset,
            }
        })
    }
}

impl ChainIndex {
    fn new(spec: VenueChainAssetsSpec) -> Result<Self, String> {
        if spec.venue_chain.trim().is_empty() {
            return Err("venue chain cannot be empty".to_string());
        }
        let router_chain = ChainId::parse(spec.router_chain)?;
        let mut router_to_venue = HashMap::with_capacity(spec.assets.len());
        let mut venue_to_router = HashMap::with_capacity(spec.assets.len());

        for entry in spec.assets {
            let router_key = static_router_asset_key(&router_chain, entry.router_asset)?;
            let venue_asset = entry.venue_asset.trim();
            if venue_asset.is_empty() {
                return Err(format!(
                    "empty venue asset in {} -> {} asset map",
                    router_chain, spec.venue_chain
                ));
            }
            if router_to_venue
                .insert(router_key.clone(), venue_asset.to_string())
                .is_some()
            {
                return Err(format!(
                    "duplicate router asset {:?} in {} asset map",
                    router_key, router_chain
                ));
            }
            if venue_to_router
                .insert(venue_asset.to_string(), router_key)
                .is_some()
            {
                return Err(format!(
                    "duplicate venue asset {} in {} asset map",
                    venue_asset, spec.venue_chain
                ));
            }
        }

        Ok(Self {
            router_chain,
            venue_chain: spec.venue_chain.trim().to_string(),
            asset_policy: spec.asset_policy,
            router_to_venue,
            venue_to_router,
        })
    }
}

impl RouterAssetKey {
    fn to_asset_id(&self) -> AssetId {
        match self {
            Self::Native => AssetId::Native,
            Self::Reference(value) => AssetId::Reference(value.clone()),
        }
    }
}

fn static_router_asset_key(
    chain: &ChainId,
    asset: RouterAssetKind,
) -> Result<RouterAssetKey, String> {
    match asset {
        RouterAssetKind::Native => Ok(RouterAssetKey::Native),
        RouterAssetKind::Reference(value) => router_reference_key(chain, value),
    }
}

fn router_asset_key(chain: &ChainId, asset: &AssetId) -> Option<RouterAssetKey> {
    match asset {
        AssetId::Native => Some(RouterAssetKey::Native),
        AssetId::Reference(value) => router_reference_key(chain, value).ok(),
    }
}

fn router_reference_key(chain: &ChainId, value: &str) -> Result<RouterAssetKey, String> {
    if chain.evm_chain_id().is_some() {
        normalize_evm_address(value).map(RouterAssetKey::Reference)
    } else {
        Ok(RouterAssetKey::Reference(value.to_string()))
    }
}

fn encode_open_contract(
    policy: VenueAssetPolicy,
    router_chain: &ChainId,
    router_asset: &AssetId,
) -> Option<String> {
    let VenueAssetPolicy::ListedPlusOpenContracts(passthrough) = policy else {
        return None;
    };
    let AssetId::Reference(value) = router_asset else {
        return None;
    };
    match passthrough.standard {
        ContractStandard::Erc20 => {
            router_chain.evm_chain_id()?;
            let address = normalize_evm_address(value).ok()?;
            match passthrough.encoding {
                ContractEncoding::LowercaseHex => Some(address),
                ContractEncoding::Raw => Some(value.to_string()),
            }
        }
        ContractStandard::Spl => match passthrough.encoding {
            ContractEncoding::Raw => Some(value.to_string()),
            ContractEncoding::LowercaseHex => None,
        },
    }
}

fn decode_open_contract(
    policy: VenueAssetPolicy,
    router_chain: &ChainId,
    venue_asset: &str,
) -> Option<AssetId> {
    let VenueAssetPolicy::ListedPlusOpenContracts(passthrough) = policy else {
        return None;
    };
    match passthrough.standard {
        ContractStandard::Erc20 => {
            router_chain.evm_chain_id()?;
            normalize_evm_address(venue_asset)
                .ok()
                .map(AssetId::Reference)
        }
        ContractStandard::Spl => match passthrough.encoding {
            ContractEncoding::Raw => Some(AssetId::Reference(venue_asset.to_string())),
            ContractEncoding::LowercaseHex => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NATIVE_ZERO: &str = "0x0000000000000000000000000000000000000000";
    const RELAY_CHAIN: VenueChainAssetsSpec = VenueChainAssetsSpec {
        router_chain: "evm:8453",
        venue_chain: "8453",
        asset_policy: VenueAssetPolicy::ListedPlusOpenContracts(ContractPassthrough {
            standard: ContractStandard::Erc20,
            encoding: ContractEncoding::LowercaseHex,
        }),
        assets: &[VenueAssetEntry {
            router_asset: RouterAssetKind::Native,
            venue_asset: NATIVE_ZERO,
        }],
    };

    fn map() -> VenueAssetMap {
        VenueAssetMap::new(VenueAssetMapSpec {
            provider: ProviderId::Relay,
            chains: &[RELAY_CHAIN],
        })
        .expect("asset map")
    }

    #[test]
    fn maps_listed_assets_before_open_contracts() {
        let asset = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::Native,
        };
        assert_eq!(
            map().to_venue_asset(&asset),
            VenueAssetLookup::Listed(VenueAsset {
                chain: "8453".to_string(),
                asset: NATIVE_ZERO.to_string(),
            })
        );
    }

    #[test]
    fn maps_open_contracts_on_supported_chains() {
        let asset = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::Reference("0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48".to_string()),
        };
        assert_eq!(
            map().to_venue_asset(&asset),
            VenueAssetLookup::OpenContract(VenueAsset {
                chain: "8453".to_string(),
                asset: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string(),
            })
        );
    }

    #[test]
    fn reverse_maps_listed_and_open_contracts() {
        let listed = map()
            .to_router_asset(&VenueAsset {
                chain: "8453".to_string(),
                asset: NATIVE_ZERO.to_string(),
            })
            .expect("native");
        assert_eq!(listed.chain.as_str(), "evm:8453");
        assert_eq!(listed.asset, AssetId::Native);

        let open = map()
            .to_router_asset(&VenueAsset {
                chain: "8453".to_string(),
                asset: "0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48".to_string(),
            })
            .expect("erc20");
        assert_eq!(open.chain.as_str(), "evm:8453");
        assert_eq!(
            open.asset,
            AssetId::Reference("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string())
        );
    }

    #[test]
    fn rejects_duplicate_chain_specs() {
        let err = VenueAssetMap::new(VenueAssetMapSpec {
            provider: ProviderId::Relay,
            chains: &[RELAY_CHAIN, RELAY_CHAIN],
        })
        .expect_err("duplicate chain");
        assert!(err.contains("duplicate router chain"));
    }
}
