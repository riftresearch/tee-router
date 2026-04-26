//! Read-only queries against HL's `/info` endpoint. Response shapes mirror
//! the SDK byte-for-byte so mock adapters can speak the real wire format.

use serde::{Deserialize, Serialize};
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
