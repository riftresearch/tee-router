use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChainType {
    Bitcoin,
    Ethereum,
    Arbitrum,
    Base,
    Hyperliquid,
}

impl ChainType {
    #[must_use]
    pub fn to_db_string(&self) -> &'static str {
        match self {
            ChainType::Bitcoin => "bitcoin",
            ChainType::Ethereum => "ethereum",
            ChainType::Arbitrum => "arbitrum",
            ChainType::Base => "base",
            ChainType::Hyperliquid => "hyperliquid",
        }
    }

    #[must_use]
    pub fn from_db_string(s: &str) -> Option<ChainType> {
        match s {
            "bitcoin" => Some(ChainType::Bitcoin),
            "ethereum" => Some(ChainType::Ethereum),
            "arbitrum" => Some(ChainType::Arbitrum),
            "base" => Some(ChainType::Base),
            "hyperliquid" => Some(ChainType::Hyperliquid),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTxStatus {
    pub current_height: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfirmedTxStatus {
    pub confirmations: u64,
    pub current_height: u64,
    pub inclusion_height: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxStatus {
    NotFound,
    Pending(PendingTxStatus),
    Confirmed(ConfirmedTxStatus),
}
