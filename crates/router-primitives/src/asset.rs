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

pub fn normalize_evm_address(value: impl AsRef<str>) -> Result<String, String> {
    let raw = value.as_ref().trim();
    let Some(stripped) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) else {
        return Err("expected a 0x-prefixed EVM address".to_string());
    };
    if stripped.len() != 40 {
        return Err("expected a 20-byte EVM address".to_string());
    }
    if !stripped.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err("expected a hexadecimal EVM address".to_string());
    }
    Ok(format!("0x{}", stripped.to_ascii_lowercase()))
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

#[cfg(test)]
mod tests {
    use super::normalize_evm_address;

    #[test]
    fn normalize_evm_address_lowercases_0x_addresses() {
        let normalized =
            normalize_evm_address("0xA0B86991C6218B36C1D19D4A2E9EB0CE3606EB48").unwrap();
        assert_eq!(normalized, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
    }

    #[test]
    fn normalize_evm_address_rejects_invalid_values() {
        assert!(normalize_evm_address("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").is_err());
        assert!(normalize_evm_address("0xnot-an-address").is_err());
    }
}
