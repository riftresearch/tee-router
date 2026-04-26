use router_primitives::ChainType;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};

const EVM_CHAIN_PREFIX: &str = "evm:";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChainId(String);

impl ChainId {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, String> {
        let raw = value.as_ref().trim();
        if raw.is_empty() {
            return Err("chain id cannot be empty".to_string());
        }

        if let Some(chain_id) = raw.strip_prefix(EVM_CHAIN_PREFIX) {
            if chain_id.is_empty() {
                return Err("evm chain id must include a numeric chain id".to_string());
            }
            if !chain_id.chars().all(|ch| ch.is_ascii_digit()) {
                return Err("evm chain id must use decimal digits".to_string());
            }

            let chain_id = chain_id
                .parse::<u64>()
                .map_err(|err| format!("invalid evm chain id: {err}"))?;
            if chain_id == 0 {
                return Err("evm chain id must be greater than zero".to_string());
            }

            return Ok(Self(format!("{EVM_CHAIN_PREFIX}{chain_id}")));
        }

        if raw != raw.to_ascii_lowercase() {
            return Err("non-evm chain ids must be lowercase".to_string());
        }
        if !raw
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
        {
            return Err(
                "non-evm chain ids may only contain lowercase letters, digits, and hyphens"
                    .to_string(),
            );
        }

        Ok(Self(raw.to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn evm_chain_id(&self) -> Option<u64> {
        self.0
            .strip_prefix(EVM_CHAIN_PREFIX)
            .and_then(|value| value.parse::<u64>().ok())
    }
}

impl fmt::Display for ChainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ChainId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ChainId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ChainId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AssetId {
    Native,
    Reference(String),
}

impl AssetId {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, String> {
        let raw = value.as_ref().trim();
        if raw.is_empty() {
            return Err("asset id cannot be empty".to_string());
        }
        if raw.eq_ignore_ascii_case("native") {
            return Ok(Self::Native);
        }
        if raw.chars().any(char::is_whitespace) {
            return Err("asset id cannot contain whitespace".to_string());
        }

        Ok(Self::Reference(raw.to_string()))
    }

    #[must_use]
    pub fn reference(value: impl Into<String>) -> Self {
        Self::Reference(value.into())
    }

    #[must_use]
    pub fn is_native(&self) -> bool {
        matches!(self, Self::Native)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Native => "native",
            Self::Reference(value) => value,
        }
    }
}

impl fmt::Display for AssetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for AssetId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AssetId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DepositAsset {
    pub chain: ChainId,
    pub asset: AssetId,
}

#[must_use]
pub fn supported_chain_ids() -> Vec<ChainId> {
    vec![
        ChainId::parse("bitcoin").expect("valid router chain id"),
        ChainId::parse("evm:1").expect("valid router chain id"),
        ChainId::parse("evm:42161").expect("valid router chain id"),
        ChainId::parse("evm:8453").expect("valid router chain id"),
        ChainId::parse("hyperliquid").expect("valid router chain id"),
    ]
}

#[must_use]
pub fn backend_chain_for_id(chain_id: &ChainId) -> Option<ChainType> {
    match chain_id.as_str() {
        "bitcoin" => Some(ChainType::Bitcoin),
        "evm:1" => Some(ChainType::Ethereum),
        "evm:42161" => Some(ChainType::Arbitrum),
        "evm:8453" => Some(ChainType::Base),
        "hyperliquid" => Some(ChainType::Hyperliquid),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{backend_chain_for_id, supported_chain_ids, AssetId, ChainId};
    use router_primitives::ChainType;

    #[test]
    fn parses_evm_chain_ids() {
        let chain_id = ChainId::parse("evm:8453").expect("valid evm chain id");
        assert_eq!(chain_id.as_str(), "evm:8453");
        assert_eq!(chain_id.evm_chain_id(), Some(8453));
    }

    #[test]
    fn rejects_invalid_non_evm_chain_ids() {
        assert!(ChainId::parse("Bitcoin").is_err());
        assert!(ChainId::parse("bitcoin/mainnet").is_err());
    }

    #[test]
    fn normalizes_native_asset_ids() {
        let asset = AssetId::parse("NATIVE").expect("valid native asset");
        assert!(asset.is_native());
        assert_eq!(asset.as_str(), "native");
    }

    #[test]
    fn maps_supported_router_chain_ids_to_backends() {
        let supported = supported_chain_ids();
        assert_eq!(supported.len(), 5);
        assert_eq!(
            backend_chain_for_id(&ChainId::parse("bitcoin").unwrap()),
            Some(ChainType::Bitcoin)
        );
        assert_eq!(
            backend_chain_for_id(&ChainId::parse("evm:1").unwrap()),
            Some(ChainType::Ethereum)
        );
        assert_eq!(
            backend_chain_for_id(&ChainId::parse("evm:42161").unwrap()),
            Some(ChainType::Arbitrum)
        );
        assert_eq!(
            backend_chain_for_id(&ChainId::parse("evm:8453").unwrap()),
            Some(ChainType::Base)
        );
        assert_eq!(
            backend_chain_for_id(&ChainId::parse("hyperliquid").unwrap()),
            Some(ChainType::Hyperliquid)
        );
    }
}
