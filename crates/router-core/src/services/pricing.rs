use crate::{
    protocol::{backend_chain_for_id, ChainId},
    services::asset_registry::CanonicalAsset,
};
use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use router_primitives::ChainType;

pub const USD_MICRO: u64 = 1_000_000;
pub const BPS_DENOMINATOR: u64 = 10_000;
pub const STATIC_BOOTSTRAP_PRICING_SOURCE: &str = "static_bootstrap_pricing_v1";

#[async_trait]
pub trait PricingSnapshotProvider: Send + Sync {
    async fn usd_pricing_snapshot(&self) -> Option<PricingSnapshot>;
}

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
            source: STATIC_BOOTSTRAP_PRICING_SOURCE.to_string(),
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
    pub fn checked_wei_to_usd_micro(&self, wei: U256) -> Option<U256> {
        div_ceil_checked(
            wei.checked_mul(U256::from(self.eth_usd_micro))?,
            checked_pow10(18)?,
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
        div_ceil_checked(
            usd_micro.checked_mul(checked_pow10(decimals)?)?,
            asset_usd_micro,
        )
    }

    #[must_use]
    pub fn sample_amount_raw(
        &self,
        sample_amount_usd_micros: u64,
        canonical: CanonicalAsset,
        decimals: u8,
    ) -> Option<U256> {
        let asset_usd_micro = U256::from(self.canonical_asset_usd_micro(canonical)?);
        Some(
            U256::from(sample_amount_usd_micros).checked_mul(checked_pow10(decimals)?)?
                / asset_usd_micro,
        )
    }
}

#[must_use]
pub fn apply_bps_multiplier(amount: U256, multiplier_bps: u64) -> Option<U256> {
    div_ceil_checked(
        amount.checked_mul(U256::from(multiplier_bps))?,
        U256::from(BPS_DENOMINATOR),
    )
}

#[must_use]
pub fn checked_pow10(decimals: u8) -> Option<U256> {
    let mut value = U256::from(1_u64);
    for _ in 0..decimals {
        value = value.checked_mul(U256::from(10_u64))?;
    }
    Some(value)
}

#[must_use]
pub fn div_ceil_checked(numerator: U256, denominator: U256) -> Option<U256> {
    if denominator == U256::ZERO {
        return None;
    }
    if numerator == U256::ZERO {
        return Some(U256::ZERO);
    }
    Some((numerator - U256::from(1_u64)) / denominator + U256::from(1_u64))
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

    #[test]
    fn pricing_raw_conversions_reject_overflow() {
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());

        assert_eq!(
            pricing.usd_micro_to_asset_raw(U256::MAX, CanonicalAsset::Btc, 18),
            None
        );
        assert_eq!(
            pricing.sample_amount_raw(u64::MAX, CanonicalAsset::Btc, u8::MAX),
            None
        );
        assert_eq!(
            pricing.usd_micro_to_asset_raw(U256::from(1_u64), CanonicalAsset::Btc, u8::MAX),
            None
        );
        assert_eq!(pricing.checked_wei_to_usd_micro(U256::MAX), None);
        assert_eq!(apply_bps_multiplier(U256::MAX, 2), None);
    }
}
