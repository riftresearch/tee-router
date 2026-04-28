use crate::{
    protocol::{backend_chain_for_id, ChainId},
    services::asset_registry::CanonicalAsset,
};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use router_primitives::ChainType;

pub const USD_MICRO: u64 = 1_000_000;
pub const BPS_DENOMINATOR: u64 = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PricingSnapshot {
    pub source: String,
    pub captured_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub stable_usd_micro: u64,
    pub eth_usd_micro: u64,
    pub btc_usd_micro: u64,
    pub ethereum_gas_price_wei: u64,
    pub arbitrum_gas_price_wei: u64,
    pub base_gas_price_wei: u64,
}

impl PricingSnapshot {
    #[must_use]
    pub fn static_bootstrap(captured_at: DateTime<Utc>) -> Self {
        Self {
            source: "static_bootstrap_pricing_v1".to_string(),
            captured_at,
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        }
    }

    #[must_use]
    pub fn from_market(snapshot: market_pricing::MarketPricingSnapshot) -> Self {
        Self {
            source: snapshot.source,
            captured_at: snapshot.captured_at,
            expires_at: snapshot.expires_at,
            stable_usd_micro: snapshot.stable_usd_micro,
            eth_usd_micro: snapshot.eth_usd_micro,
            btc_usd_micro: snapshot.btc_usd_micro,
            ethereum_gas_price_wei: snapshot.ethereum_gas_price_wei,
            arbitrum_gas_price_wei: snapshot.arbitrum_gas_price_wei,
            base_gas_price_wei: snapshot.base_gas_price_wei,
        }
    }

    #[must_use]
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_none_or(|expires_at| expires_at > now)
    }

    #[must_use]
    pub fn canonical_asset_usd_micro(&self, canonical: CanonicalAsset) -> Option<u64> {
        match canonical {
            CanonicalAsset::Usdc | CanonicalAsset::Usdt => Some(self.stable_usd_micro),
            CanonicalAsset::Eth => Some(self.eth_usd_micro),
            CanonicalAsset::Btc | CanonicalAsset::Cbbtc => Some(self.btc_usd_micro),
            CanonicalAsset::Hype => None,
        }
    }

    #[must_use]
    pub fn chain_gas_price_wei(&self, chain: &ChainId) -> U256 {
        match backend_chain_for_id(chain) {
            Some(ChainType::Ethereum) => U256::from(self.ethereum_gas_price_wei),
            Some(ChainType::Arbitrum) => U256::from(self.arbitrum_gas_price_wei),
            Some(ChainType::Base) => U256::from(self.base_gas_price_wei),
            _ => U256::ZERO,
        }
    }

    #[must_use]
    pub fn wei_to_usd_micro(&self, wei: U256) -> U256 {
        div_ceil(
            wei.saturating_mul(U256::from(self.eth_usd_micro)),
            pow10(18),
        )
    }

    #[must_use]
    pub fn usd_micro_to_asset_raw(
        &self,
        usd_micro: U256,
        canonical: CanonicalAsset,
        decimals: u8,
    ) -> Option<U256> {
        let asset_usd_micro = U256::from(self.canonical_asset_usd_micro(canonical)?);
        Some(div_ceil(
            usd_micro.saturating_mul(pow10(decimals)),
            asset_usd_micro,
        ))
    }

    #[must_use]
    pub fn sample_amount_raw(
        &self,
        sample_amount_usd_micros: u64,
        canonical: CanonicalAsset,
        decimals: u8,
    ) -> Option<U256> {
        let asset_usd_micro = U256::from(self.canonical_asset_usd_micro(canonical)?);
        Some(U256::from(sample_amount_usd_micros).saturating_mul(pow10(decimals)) / asset_usd_micro)
    }
}

#[must_use]
pub fn apply_bps_multiplier(amount: U256, multiplier_bps: u64) -> U256 {
    div_ceil(
        amount.saturating_mul(U256::from(multiplier_bps)),
        U256::from(BPS_DENOMINATOR),
    )
}

#[must_use]
pub fn pow10(decimals: u8) -> U256 {
    let mut value = U256::from(1_u64);
    for _ in 0..decimals {
        value = value.saturating_mul(U256::from(10_u64));
    }
    value
}

#[must_use]
pub fn div_ceil(numerator: U256, denominator: U256) -> U256 {
    if numerator == U256::ZERO {
        return U256::ZERO;
    }
    numerator.saturating_add(denominator.saturating_sub(U256::from(1_u64))) / denominator
}

#[must_use]
pub fn div_ceil_u64(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return u64::MAX;
    }
    numerator
        .saturating_add(denominator.saturating_sub(1))
        .saturating_div(denominator)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_snapshot_prices_core_assets() {
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        assert_eq!(
            pricing.canonical_asset_usd_micro(CanonicalAsset::Usdc),
            Some(USD_MICRO)
        );
        assert_eq!(
            pricing.canonical_asset_usd_micro(CanonicalAsset::Eth),
            Some(3_000 * USD_MICRO)
        );
        assert_eq!(
            pricing.canonical_asset_usd_micro(CanonicalAsset::Btc),
            Some(100_000 * USD_MICRO)
        );
    }
}
