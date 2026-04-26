use crate::{serde_utils::u256_decimal, ChainType};
use alloy::primitives::U256;
use serde::de::Deserializer;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Clone, PartialEq, Eq, Hash)]
#[serde(tag = "type", content = "data")]
pub enum TokenIdentifier {
    Native,
    Address(String),
}

impl TokenIdentifier {
    #[must_use]
    pub fn address(addr: impl Into<String>) -> Self {
        Self::Address(addr.into().to_lowercase())
    }

    #[must_use]
    pub fn normalize(&self) -> Self {
        match self {
            Self::Native => Self::Native,
            Self::Address(addr) => Self::address(addr),
        }
    }
}

impl<'de> Deserialize<'de> for TokenIdentifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", content = "data")]
        enum RawTokenIdentifier {
            Native,
            Address(String),
        }

        match RawTokenIdentifier::deserialize(deserializer)? {
            RawTokenIdentifier::Native => Ok(TokenIdentifier::Native),
            RawTokenIdentifier::Address(addr) => Ok(TokenIdentifier::address(addr)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Currency {
    pub chain: ChainType,
    pub token: TokenIdentifier,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lot {
    pub currency: Currency,
    #[serde(with = "u256_decimal")]
    pub amount: U256,
}
