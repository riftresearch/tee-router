//! Read-only queries against HL's `/info` endpoint. Response shapes mirror
//! the SDK byte-for-byte so mock adapters can speak the real wire format.

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// L2 orderbook snapshot. `levels[0]` is bids (descending px), `levels[1]` is
/// asks (ascending px). Each level carries `(n, px, sz)` as strings — HL
/// doesn't round-trip floats natively.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct L2BookSnapshot {
    pub coin: String,
    pub levels: Vec<Vec<L2Level>>,
    pub time: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct L2Level {
    pub n: u64,
    pub px: String,
    pub sz: String,
}

impl L2BookSnapshot {
    /// Best bid / best ask as (px, sz) strings, if the book has any levels.
    #[must_use]
    pub fn best_bid(&self) -> Option<&L2Level> {
        self.levels.first().and_then(|side| side.first())
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<&L2Level> {
        self.levels.get(1).and_then(|side| side.first())
    }

    /// Bid side (index 0) — descending px.
    #[must_use]
    pub fn bids(&self) -> &[L2Level] {
        self.levels.first().map_or(&[], |side| side.as_slice())
    }

    /// Ask side (index 1) — ascending px.
    #[must_use]
    pub fn asks(&self) -> &[L2Level] {
        self.levels.get(1).map_or(&[], |side| side.as_slice())
    }
}

/// Response to `/info { type: "orderStatus", user, oid }`. `status` is the
/// textual lifecycle state ("order" when resting, otherwise a terminal state);
/// `order` is populated when HL still has the order in its state.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OrderStatusResponse {
    pub status: String,
    #[serde(default)]
    pub order: Option<OrderInfoEnvelope>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfoEnvelope {
    pub order: BasicOrderInfo,
    pub status: String,
    pub status_timestamp: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BasicOrderInfo {
    pub coin: String,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    #[serde(default)]
    pub trigger_condition: String,
    #[serde(default)]
    pub is_trigger: bool,
    #[serde(default)]
    pub trigger_px: String,
    #[serde(default)]
    pub is_position_tpsl: bool,
    #[serde(default)]
    pub reduce_only: bool,
    #[serde(default)]
    pub order_type: String,
    #[serde(default)]
    pub orig_sz: String,
    #[serde(default)]
    pub tif: Option<String>,
    #[serde(default)]
    pub cloid: Option<String>,
}

/// Response to `/info { type: "spotClearinghouseState", user }`. `balances`
/// lists one entry per spot token the account holds; `total` is the string
/// decimal balance, `hold` is the portion locked by resting orders.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct SpotClearinghouseState {
    #[serde(default)]
    pub balances: Vec<SpotBalance>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpotBalance {
    pub coin: String,
    pub token: u64,
    pub hold: String,
    pub total: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_ntl: Option<String>,
}

impl SpotClearinghouseState {
    /// Return the `total` balance string for `coin`, or `"0"` when absent.
    #[must_use]
    pub fn balance_of(&self, coin: &str) -> &str {
        self.balances
            .iter()
            .find(|b| b.coin == coin)
            .map_or("0", |b| b.total.as_str())
    }
}

/// Response to `/info { type: "clearinghouseState", user }`. Bridge deposits
/// show up here as withdrawable USDC collateral before any spot transfers.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClearinghouseState {
    #[serde(default)]
    pub margin_summary: MarginSummary,
    #[serde(default)]
    pub cross_margin_summary: MarginSummary,
    #[serde(default)]
    pub cross_maintenance_margin_used: String,
    #[serde(default)]
    pub withdrawable: String,
    #[serde(default)]
    pub asset_positions: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MarginSummary {
    #[serde(default)]
    pub account_value: String,
    #[serde(default)]
    pub total_ntl_pos: String,
    #[serde(default)]
    pub total_raw_usd: String,
    #[serde(default)]
    pub total_margin_used: String,
}

/// Response to `/info { type: "openOrders", user }` — a flat array of resting
/// orders. Coin is returned as the HL wire coin ("@140") or base/quote form.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenOrder {
    pub coin: String,
    pub side: String,
    pub limit_px: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: u64,
    #[serde(default)]
    pub orig_sz: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

/// Response to `/info { type: "userFills", user }` — one entry per trade the
/// account executed, newest first. `dir` is `"Buy"` / `"Sell"`; `crossed`
/// distinguishes taker vs. maker fills.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFill {
    pub coin: String,
    pub px: String,
    pub sz: String,
    pub side: String,
    pub time: u64,
    #[serde(default)]
    pub start_position: String,
    #[serde(default)]
    pub dir: String,
    #[serde(default)]
    pub closed_pnl: String,
    #[serde(default)]
    pub hash: String,
    pub oid: u64,
    #[serde(default)]
    pub crossed: bool,
    #[serde(default)]
    pub fee: String,
    pub tid: u64,
    #[serde(default)]
    pub fee_token: String,
}

/// Response to `/info { type: "userNonFundingLedgerUpdates", user,
/// startTime, endTime? }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UserNonFundingLedgerUpdate {
    pub time: u64,
    pub hash: String,
    pub delta: UserNonFundingLedgerDelta,
}

/// Non-funding ledger delta shapes documented by Hyperliquid. Variants not
/// needed by the router/shim v1 routes decode to [`Self::Unsupported`] so new
/// Hyperliquid ledger variants do not crash indexer ingestion.
#[derive(Debug, Clone)]
pub enum UserNonFundingLedgerDelta {
    Deposit {
        usdc: String,
    },
    Withdraw {
        usdc: String,
        nonce: u64,
        fee: String,
    },
    InternalTransfer {
        usdc: String,
        user: String,
        destination: String,
        fee: String,
    },
    AccountClassTransfer {
        usdc: String,
        to_perp: bool,
    },
    SpotTransfer {
        token: String,
        amount: String,
        usdc_value: String,
        user: String,
        destination: String,
        fee: String,
        native_token_fee: String,
        nonce: u64,
    },
    Unsupported {
        type_name: String,
        raw: Value,
    },
}

impl<'de> Deserialize<'de> for UserNonFundingLedgerDelta {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        let type_name = raw
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("ledger delta missing string `type` field"))?;
        match type_name {
            "deposit" => {
                #[derive(Deserialize)]
                struct Delta {
                    usdc: String,
                }
                let delta = serde_json::from_value::<Delta>(raw).map_err(D::Error::custom)?;
                Ok(Self::Deposit { usdc: delta.usdc })
            }
            "withdraw" => {
                #[derive(Deserialize)]
                struct Delta {
                    usdc: String,
                    nonce: u64,
                    fee: String,
                }
                let delta = serde_json::from_value::<Delta>(raw).map_err(D::Error::custom)?;
                Ok(Self::Withdraw {
                    usdc: delta.usdc,
                    nonce: delta.nonce,
                    fee: delta.fee,
                })
            }
            "internalTransfer" => {
                #[derive(Deserialize)]
                struct Delta {
                    usdc: String,
                    user: String,
                    destination: String,
                    fee: String,
                }
                let delta = serde_json::from_value::<Delta>(raw).map_err(D::Error::custom)?;
                Ok(Self::InternalTransfer {
                    usdc: delta.usdc,
                    user: delta.user,
                    destination: delta.destination,
                    fee: delta.fee,
                })
            }
            "accountClassTransfer" => {
                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Delta {
                    usdc: String,
                    to_perp: bool,
                }
                let delta = serde_json::from_value::<Delta>(raw).map_err(D::Error::custom)?;
                Ok(Self::AccountClassTransfer {
                    usdc: delta.usdc,
                    to_perp: delta.to_perp,
                })
            }
            "spotTransfer" => {
                #[derive(Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct Delta {
                    token: String,
                    amount: String,
                    usdc_value: String,
                    user: String,
                    destination: String,
                    fee: String,
                    native_token_fee: String,
                    nonce: u64,
                }
                let delta = serde_json::from_value::<Delta>(raw).map_err(D::Error::custom)?;
                Ok(Self::SpotTransfer {
                    token: delta.token,
                    amount: delta.amount,
                    usdc_value: delta.usdc_value,
                    user: delta.user,
                    destination: delta.destination,
                    fee: delta.fee,
                    native_token_fee: delta.native_token_fee,
                    nonce: delta.nonce,
                })
            }
            other => Ok(Self::Unsupported {
                type_name: other.to_string(),
                raw,
            }),
        }
    }
}

impl Serialize for UserNonFundingLedgerDelta {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Deposit { usdc } => {
                #[derive(Serialize)]
                struct Delta<'a> {
                    r#type: &'static str,
                    usdc: &'a str,
                }
                Delta {
                    r#type: "deposit",
                    usdc,
                }
                .serialize(serializer)
            }
            Self::Withdraw { usdc, nonce, fee } => {
                #[derive(Serialize)]
                struct Delta<'a> {
                    r#type: &'static str,
                    usdc: &'a str,
                    nonce: u64,
                    fee: &'a str,
                }
                Delta {
                    r#type: "withdraw",
                    usdc,
                    nonce: *nonce,
                    fee,
                }
                .serialize(serializer)
            }
            Self::InternalTransfer {
                usdc,
                user,
                destination,
                fee,
            } => {
                #[derive(Serialize)]
                struct Delta<'a> {
                    r#type: &'static str,
                    usdc: &'a str,
                    user: &'a str,
                    destination: &'a str,
                    fee: &'a str,
                }
                Delta {
                    r#type: "internalTransfer",
                    usdc,
                    user,
                    destination,
                    fee,
                }
                .serialize(serializer)
            }
            Self::AccountClassTransfer { usdc, to_perp } => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Delta<'a> {
                    r#type: &'static str,
                    usdc: &'a str,
                    to_perp: bool,
                }
                Delta {
                    r#type: "accountClassTransfer",
                    usdc,
                    to_perp: *to_perp,
                }
                .serialize(serializer)
            }
            Self::SpotTransfer {
                token,
                amount,
                usdc_value,
                user,
                destination,
                fee,
                native_token_fee,
                nonce,
            } => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Delta<'a> {
                    r#type: &'static str,
                    token: &'a str,
                    amount: &'a str,
                    usdc_value: &'a str,
                    user: &'a str,
                    destination: &'a str,
                    fee: &'a str,
                    native_token_fee: &'a str,
                    nonce: u64,
                }
                Delta {
                    r#type: "spotTransfer",
                    token,
                    amount,
                    usdc_value,
                    user,
                    destination,
                    fee,
                    native_token_fee,
                    nonce: *nonce,
                }
                .serialize(serializer)
            }
            Self::Unsupported { raw, .. } => raw.serialize(serializer),
        }
    }
}

/// Response item for `/info { type: "userFunding", user, startTime,
/// endTime? }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserFunding {
    pub time: u64,
    pub coin: String,
    pub usdc: String,
    pub szi: String,
    pub funding_rate: String,
}

/// Response to `/info { type: "meta" }` for the perp universe.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct PerpMeta {
    pub universe: Vec<PerpAssetMeta>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PerpAssetMeta {
    pub name: String,
    pub sz_decimals: u8,
    pub max_leverage: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub only_isolated: Option<bool>,
}

/// Response to `/info { type: "userRateLimit", user }`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UserRateLimit {
    pub cum_vlm: String,
    pub n_requests_used: u64,
    pub n_requests_cap: u64,
    pub n_requests_surplus: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_ledger_delta_captures_type_and_raw_shape() {
        let delta: UserNonFundingLedgerDelta = serde_json::from_value(serde_json::json!({
            "type": "vaultDeposit",
            "vault": "0x1111111111111111111111111111111111111111",
            "usdc": "1.23"
        }))
        .expect("unsupported delta should decode");
        let UserNonFundingLedgerDelta::Unsupported { type_name, raw } = delta else {
            panic!("expected unsupported delta");
        };
        assert_eq!(type_name, "vaultDeposit");
        assert_eq!(raw["usdc"], "1.23");
    }
}
