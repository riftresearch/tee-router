use std::{fmt, str::FromStr};

use alloy::primitives::Address;
use serde::{Deserialize, Serialize};

use crate::{error::Result, Error};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DecimalString(String);

impl DecimalString {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        validate_decimal(&value)?;
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DecimalString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for DecimalString {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::new(value)
    }
}

fn validate_decimal(value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::InvalidDecimal {
            value: value.to_string(),
            reason: "empty",
        });
    }
    let rest = value.strip_prefix('-').unwrap_or(value);
    if rest.is_empty() {
        return Err(Error::InvalidDecimal {
            value: value.to_string(),
            reason: "missing digits",
        });
    }
    let mut parts = rest.split('.');
    let whole = parts.next().unwrap_or_default();
    let fractional = parts.next();
    if parts.next().is_some() || whole.is_empty() {
        return Err(Error::InvalidDecimal {
            value: value.to_string(),
            reason: "invalid decimal shape",
        });
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidDecimal {
            value: value.to_string(),
            reason: "non-digit whole part",
        });
    }
    if let Some(fractional) = fractional {
        if fractional.is_empty() || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(Error::InvalidDecimal {
                value: value.to_string(),
                reason: "invalid fractional part",
            });
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HlMarket {
    Spot,
    Perp,
    NotApplicable,
}

impl HlMarket {
    #[must_use]
    pub fn as_db_str(self) -> Option<&'static str> {
        match self {
            Self::Spot => Some("spot"),
            Self::Perp => Some("perp"),
            Self::NotApplicable => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HlTransferEvent {
    pub user: Address,
    pub time_ms: i64,
    pub kind: HlTransferKind,
    pub asset: String,
    pub market: HlMarket,
    pub amount_delta: DecimalString,
    pub fee: Option<DecimalString>,
    pub fee_token: Option<String>,
    pub hash: String,
    pub observed_at_ms: i64,
}

impl HlTransferEvent {
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        self.kind.kind_name()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HlTransferKind {
    Fill {
        oid: u64,
        tid: u64,
        side: String,
        px: String,
        sz: String,
        crossed: bool,
        dir: String,
        closed_pnl: String,
        start_position: String,
        builder_fee: Option<String>,
    },
    Funding {
        szi: String,
        funding_rate: String,
    },
    Deposit,
    Withdraw {
        nonce: u64,
    },
    InternalTransfer {
        destination: String,
    },
    AccountClassTransfer {
        to_perp: bool,
    },
    SpotTransfer {
        destination: String,
        native_token_fee: String,
        nonce: u64,
        usdc_value: String,
    },
}

impl HlTransferKind {
    #[must_use]
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Fill { .. } => "fill",
            Self::Funding { .. } => "funding",
            Self::Deposit => "deposit",
            Self::Withdraw { .. } => "withdraw",
            Self::InternalTransfer { .. } => "internal_transfer",
            Self::AccountClassTransfer { .. } => "account_class_transfer",
            Self::SpotTransfer { .. } => "spot_transfer",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HlOrderEvent {
    pub user: Address,
    pub oid: u64,
    pub cloid: Option<String>,
    pub coin: String,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
    pub orig_sz: String,
    pub status: HlOrderStatus,
    pub status_timestamp_ms: i64,
    pub observed_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HlOrderStatus {
    Open,
    Filled,
    Canceled,
    Triggered,
    Rejected,
    MarginCanceled,
    Unknown(String),
}

impl HlOrderStatus {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Open => "open",
            Self::Filled => "filled",
            Self::Canceled => "canceled",
            Self::Triggered => "triggered",
            Self::Rejected => "rejected",
            Self::MarginCanceled => "marginCanceled",
            Self::Unknown(value) => value.as_str(),
        }
    }
}

impl From<String> for HlOrderStatus {
    fn from(value: String) -> Self {
        match value.as_str() {
            "open" | "order" => Self::Open,
            "filled" => Self::Filled,
            "canceled" | "cancelled" => Self::Canceled,
            "triggered" => Self::Triggered,
            "rejected" => Self::Rejected,
            "marginCanceled" => Self::MarginCanceled,
            _ => Self::Unknown(value),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Transfers,
    Orders,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeFilter {
    pub users: Vec<Address>,
    #[serde(default = "default_stream_kinds")]
    pub kinds: Vec<StreamKind>,
}

impl SubscribeFilter {
    #[must_use]
    pub fn matches_transfer(&self, event: &HlTransferEvent) -> bool {
        self.users.contains(&event.user) && self.kinds.contains(&StreamKind::Transfers)
    }

    #[must_use]
    pub fn matches_order(&self, event: &HlOrderEvent) -> bool {
        self.users.contains(&event.user) && self.kinds.contains(&StreamKind::Orders)
    }
}

fn default_stream_kinds() -> Vec<StreamKind> {
    vec![StreamKind::Transfers, StreamKind::Orders]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryCursor(pub i64);

impl QueryCursor {
    #[must_use]
    pub fn encode(self) -> String {
        self.0.to_string()
    }

    pub fn decode(value: Option<&str>, fallback: i64) -> Result<Self> {
        match value {
            Some(value) => Ok(Self(value.parse().map_err(|source| {
                Error::InvalidCursor {
                    value: value.to_string(),
                    source,
                }
            })?)),
            None => Ok(Self(fallback)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_string_validates_decimal_shape() {
        assert!(DecimalString::new("0").is_ok());
        assert!(DecimalString::new("-0.0123").is_ok());
        assert!(DecimalString::new("10.000000000000000001").is_ok());
        assert!(DecimalString::new("").is_err());
        assert!(DecimalString::new("-").is_err());
        assert!(DecimalString::new("1e6").is_err());
        assert!(DecimalString::new("1.").is_err());
    }
}
