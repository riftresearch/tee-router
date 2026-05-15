use std::collections::{HashMap, HashSet};

use hyperliquid_client::{PerpMeta, SpotMeta};

use crate::ingest::schema::HlMarket;

#[derive(Debug, Clone, Default)]
pub struct MetadataSnapshot {
    perps: HashSet<String>,
    spot_pairs: HashSet<String>,
    indexed_spots: HashMap<String, String>,
}

impl MetadataSnapshot {
    #[must_use]
    pub fn new(perp: &PerpMeta, spot: &SpotMeta) -> Self {
        let perps = perp
            .universe
            .iter()
            .map(|asset| asset.name.clone())
            .collect();
        let mut spot_pairs = HashSet::new();
        let mut indexed_spots = HashMap::new();
        let token_names: HashMap<usize, &str> = spot
            .tokens
            .iter()
            .map(|token| (token.index, token.name.as_str()))
            .collect();
        for pair in &spot.universe {
            spot_pairs.insert(pair.name.clone());
            indexed_spots.insert(pair.name.clone(), pair.name.clone());
            if let (Some(base), Some(quote)) = (
                token_names.get(&pair.tokens[0]),
                token_names.get(&pair.tokens[1]),
            ) {
                let canonical = format!("{base}/{quote}");
                spot_pairs.insert(canonical.clone());
                indexed_spots.insert(pair.name.clone(), canonical);
            }
        }
        Self {
            perps,
            spot_pairs,
            indexed_spots,
        }
    }

    #[must_use]
    pub fn classify_coin(&self, coin: &str) -> HlMarket {
        if self.perps.contains(coin) {
            return HlMarket::Perp;
        }
        if self.spot_pairs.contains(coin) || self.indexed_spots.contains_key(coin) {
            return HlMarket::Spot;
        }
        if coin.contains('/') || coin.starts_with('@') {
            return HlMarket::Spot;
        }
        HlMarket::Perp
    }
}
