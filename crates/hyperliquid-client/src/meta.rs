//! `/info { type: "spotMeta" }` response shapes. Spot trading on HL is the
//! only trading path this crate supports — perps are intentionally absent.
//!
//! HL's wire format for a spot order uses `asset = 10000 + pair_index`; the
//! [`SpotMeta::coin_to_asset_map`] helper builds that lookup so callers can
//! resolve either a pair name (e.g. "PURR/USDC") or the canonical HL short
//! name (e.g. "@0") to the right wire index.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Offset applied to a spot pair's universe index to yield the wire asset id
/// used inside `OrderRequest::asset`.
pub const SPOT_ASSET_INDEX_OFFSET: u32 = 10_000;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SpotMeta {
    pub universe: Vec<SpotAssetMeta>,
    pub tokens: Vec<TokenInfo>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SpotAssetMeta {
    /// `[base_token_idx, quote_token_idx]` into [`SpotMeta::tokens`].
    pub tokens: [usize; 2],
    /// HL's canonical short name for the pair — e.g. `"@0"` or `"PURR/USDC"`.
    pub name: String,
    /// 0-based pair index; `10000 + index` is the wire asset id.
    pub index: usize,
    pub is_canonical: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TokenInfo {
    pub name: String,
    pub sz_decimals: u8,
    pub wei_decimals: u8,
    pub index: usize,
    #[serde(default)]
    pub token_id: Option<String>,
    #[serde(default)]
    pub is_canonical: bool,
}

impl SpotMeta {
    /// Build a lookup from both the short pair name (`"@0"`) and the
    /// "{base}/{quote}" form (`"UBTC/USDC"`) onto the wire asset id.
    pub fn coin_to_asset_map(&self) -> Result<HashMap<String, u32>, String> {
        let index_to_name: HashMap<usize, &str> = self
            .tokens
            .iter()
            .map(|info| (info.index, info.name.as_str()))
            .collect();

        let mut map = HashMap::with_capacity(self.universe.len() * 2);
        for pair in &self.universe {
            let spot_ind = spot_wire_asset_index(pair.index).ok_or_else(|| {
                format!(
                    "spot pair {} index {} does not fit Hyperliquid wire asset id",
                    pair.name, pair.index
                )
            })?;
            map.insert(pair.name.clone(), spot_ind);

            let Some(base_name) = index_to_name.get(&pair.tokens[0]) else {
                continue;
            };
            let Some(quote_name) = index_to_name.get(&pair.tokens[1]) else {
                continue;
            };
            map.insert(format!("{base_name}/{quote_name}"), spot_ind);
        }
        Ok(map)
    }

    /// Look up the base token's [`TokenInfo`] for a pair by its canonical
    /// short name or "{base}/{quote}" form. Callers use the base token's
    /// `sz_decimals` when encoding order sizes on the wire.
    #[must_use]
    pub fn base_token_for(&self, coin: &str) -> Option<&TokenInfo> {
        let pair = self.pair_for(coin)?;
        self.tokens.iter().find(|t| t.index == pair.tokens[0])
    }

    /// Look up the quote token's [`TokenInfo`] for a pair.
    #[must_use]
    pub fn quote_token_for(&self, coin: &str) -> Option<&TokenInfo> {
        let pair = self.pair_for(coin)?;
        self.tokens.iter().find(|t| t.index == pair.tokens[1])
    }

    /// Build the `tokenName:tokenId` wire form Hyperliquid expects for
    /// `spotSend` actions.
    #[must_use]
    pub fn spot_token_wire(&self, symbol: &str) -> Option<String> {
        let token = self
            .tokens
            .iter()
            .find(|token| token.name.eq_ignore_ascii_case(symbol))?;
        let token_id = token.token_id.as_deref()?;
        Some(format!("{}:{token_id}", token.name))
    }

    fn pair_for(&self, coin: &str) -> Option<&SpotAssetMeta> {
        if let Some(hit) = self.universe.iter().find(|p| p.name == coin) {
            return Some(hit);
        }
        let (base_name, quote_name) = coin.split_once('/')?;
        let base = self.tokens.iter().find(|t| t.name == base_name)?;
        let quote = self.tokens.iter().find(|t| t.name == quote_name)?;
        self.universe
            .iter()
            .find(|p| p.tokens[0] == base.index && p.tokens[1] == quote.index)
    }
}

pub fn spot_wire_asset_index(pair_index: usize) -> Option<u32> {
    let pair_index = u32::try_from(pair_index).ok()?;
    SPOT_ASSET_INDEX_OFFSET.checked_add(pair_index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_meta() -> SpotMeta {
        SpotMeta {
            tokens: vec![
                TokenInfo {
                    name: "USDC".to_string(),
                    sz_decimals: 8,
                    wei_decimals: 8,
                    index: 0,
                    token_id: Some("0x6d1e7cde53ba9467b783cb7c530ce054".to_string()),
                    is_canonical: true,
                },
                TokenInfo {
                    name: "UBTC".to_string(),
                    sz_decimals: 5,
                    wei_decimals: 8,
                    index: 1,
                    token_id: Some("0x11111111111111111111111111111111".to_string()),
                    is_canonical: true,
                },
                TokenInfo {
                    name: "UETH".to_string(),
                    sz_decimals: 4,
                    wei_decimals: 18,
                    index: 2,
                    token_id: Some("0x22222222222222222222222222222222".to_string()),
                    is_canonical: true,
                },
            ],
            universe: vec![
                SpotAssetMeta {
                    tokens: [1, 0],
                    name: "@140".to_string(),
                    index: 140,
                    is_canonical: true,
                },
                SpotAssetMeta {
                    tokens: [2, 0],
                    name: "@141".to_string(),
                    index: 141,
                    is_canonical: true,
                },
            ],
        }
    }

    #[test]
    fn coin_to_asset_indexes_both_name_forms() {
        let map = sample_meta()
            .coin_to_asset_map()
            .expect("sample spotMeta should be valid");
        assert_eq!(map.get("@140"), Some(&10_140));
        assert_eq!(map.get("UBTC/USDC"), Some(&10_140));
        assert_eq!(map.get("@141"), Some(&10_141));
        assert_eq!(map.get("UETH/USDC"), Some(&10_141));
    }

    #[test]
    fn spot_wire_asset_index_rejects_overflowing_pair_indexes() {
        assert_eq!(spot_wire_asset_index(140), Some(10_140));
        assert_eq!(spot_wire_asset_index(usize::MAX), None);
        assert_eq!(
            spot_wire_asset_index(usize::try_from(u32::MAX).unwrap()),
            None
        );
    }

    #[test]
    fn base_and_quote_lookup_resolves_by_either_name_form() {
        let meta = sample_meta();
        assert_eq!(meta.base_token_for("UBTC/USDC").unwrap().name, "UBTC");
        assert_eq!(meta.quote_token_for("UBTC/USDC").unwrap().name, "USDC");
        assert_eq!(meta.base_token_for("@141").unwrap().name, "UETH");
        assert_eq!(meta.quote_token_for("@141").unwrap().name, "USDC");
    }

    #[test]
    fn spot_token_wire_uses_token_id() {
        let meta = sample_meta();
        assert_eq!(
            meta.spot_token_wire("UETH").as_deref(),
            Some("UETH:0x22222222222222222222222222222222")
        );
        assert_eq!(
            meta.spot_token_wire("ueth").as_deref(),
            Some("UETH:0x22222222222222222222222222222222")
        );
        assert!(meta.spot_token_wire("UNKNOWN").is_none());
    }

    #[test]
    fn lookup_returns_none_for_unknown_pair() {
        let meta = sample_meta();
        assert!(meta.base_token_for("UNKNOWN/USDC").is_none());
        assert!(meta.pair_for("NOPE").is_none());
    }
}
