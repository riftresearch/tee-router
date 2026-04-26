use crate::{
    db::Database,
    error::RouterServerResult,
    models::MarketOrderKind,
    protocol::DepositAsset,
    services::{
        action_providers::{ActionProviderRegistry, BridgeQuoteRequest},
        asset_registry::{
            AssetRegistry, MarketOrderTransitionKind, TransitionDecl, TransitionPath,
        },
    },
};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::time::timeout;
use tracing::debug;

const DEFAULT_REFRESH_TTL: Duration = Duration::from_secs(600);
const DEFAULT_AMOUNT_BUCKET: &str = "usd_1000";
const DEFAULT_SAMPLE_AMOUNT_USD_MICROS: u64 = 1_000_000_000;
const BPS_DENOMINATOR: u64 = 10_000;
const ROUTE_COST_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
const ETH_USD_MICRO: u64 = 3_000_000_000;
const BTC_USD_MICRO: u64 = 100_000_000_000;
const DUMMY_EVM_DEPOSITOR: &str = "0x1111111111111111111111111111111111111111";
const DUMMY_EVM_RECIPIENT: &str = "0x2222222222222222222222222222222222222222";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteCostSnapshot {
    pub transition_id: String,
    pub amount_bucket: String,
    pub provider: String,
    pub edge_kind: String,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub estimated_fee_bps: u64,
    pub estimated_gas_usd_micros: u64,
    pub estimated_latency_ms: u64,
    pub sample_amount_usd_micros: u64,
    pub quote_source: String,
    pub refreshed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl RouteCostSnapshot {
    #[must_use]
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }

    #[must_use]
    pub fn effective_cost_bps(&self) -> u64 {
        self.estimated_fee_bps.saturating_add(div_ceil_u64(
            self.estimated_gas_usd_micros
                .saturating_mul(BPS_DENOMINATOR),
            self.sample_amount_usd_micros,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutePathCostScore {
    pub missing_edges: usize,
    pub total_effective_cost_bps: u64,
    pub total_latency_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteCostRefreshSummary {
    pub candidate_edges: usize,
    pub snapshots_upserted: usize,
    pub provider_quotes_attempted: usize,
    pub provider_quotes_succeeded: usize,
    pub provider_quotes_failed: usize,
    pub refreshed_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct RouteCostService {
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
    asset_registry: Arc<AssetRegistry>,
    ttl: Duration,
}

impl RouteCostService {
    #[must_use]
    pub fn new(db: Database, action_providers: Arc<ActionProviderRegistry>) -> Self {
        let asset_registry = action_providers.asset_registry();
        Self {
            db,
            action_providers,
            asset_registry,
            ttl: DEFAULT_REFRESH_TTL,
        }
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub async fn refresh_anchor_costs(&self) -> RouterServerResult<RouteCostRefreshSummary> {
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(self.ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(600));
        let transitions = self.asset_registry.transition_declarations();
        let mut snapshots = Vec::with_capacity(transitions.len());
        let mut provider_quotes_attempted = 0_usize;
        let mut provider_quotes_succeeded = 0_usize;
        let mut provider_quotes_failed = 0_usize;

        for transition in &transitions {
            let live_snapshot = match self.live_cost_snapshot(transition, now, expires_at).await {
                LiveCostSnapshotOutcome::NotAttempted => None,
                LiveCostSnapshotOutcome::Succeeded(snapshot) => {
                    provider_quotes_attempted += 1;
                    provider_quotes_succeeded += 1;
                    Some(snapshot)
                }
                LiveCostSnapshotOutcome::Failed(reason) => {
                    provider_quotes_attempted += 1;
                    provider_quotes_failed += 1;
                    debug!(
                        transition_id = %transition.id,
                        provider = transition.provider.as_str(),
                        reason = %reason,
                        "route-cost provider sampling failed; using structural seed"
                    );
                    None
                }
            };
            snapshots.push(
                live_snapshot
                    .unwrap_or_else(|| structural_cost_snapshot(transition, now, expires_at)),
            );
        }
        self.db.route_costs().upsert_many(&snapshots).await?;

        Ok(RouteCostRefreshSummary {
            candidate_edges: transitions.len(),
            snapshots_upserted: snapshots.len(),
            provider_quotes_attempted,
            provider_quotes_succeeded,
            provider_quotes_failed,
            refreshed_at: now,
        })
    }

    pub async fn rank_transition_paths(
        &self,
        paths: &mut [TransitionPath],
    ) -> RouterServerResult<()> {
        let snapshots = self
            .db
            .route_costs()
            .list_active(DEFAULT_AMOUNT_BUCKET, Utc::now())
            .await?;
        let by_transition_id = snapshots
            .into_iter()
            .map(|snapshot| (snapshot.transition_id.clone(), snapshot))
            .collect::<HashMap<_, _>>();

        paths.sort_by(|left, right| {
            let left_score = path_score(left, &by_transition_id);
            let right_score = path_score(right, &by_transition_id);
            (
                left_score.missing_edges,
                left_score.total_effective_cost_bps,
                left_score.total_latency_ms,
                left.transitions.len(),
            )
                .cmp(&(
                    right_score.missing_edges,
                    right_score.total_effective_cost_bps,
                    right_score.total_latency_ms,
                    right.transitions.len(),
                ))
        });
        Ok(())
    }

    async fn live_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> LiveCostSnapshotOutcome {
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                self.live_bridge_cost_snapshot(transition, refreshed_at, expires_at)
                    .await
            }
            MarketOrderTransitionKind::UnitDeposit
            | MarketOrderTransitionKind::UnitWithdrawal
            | MarketOrderTransitionKind::HyperliquidTrade
            | MarketOrderTransitionKind::UniversalRouterSwap => {
                LiveCostSnapshotOutcome::NotAttempted
            }
        }
    }

    async fn live_bridge_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> LiveCostSnapshotOutcome {
        let Some(bridge) = self.action_providers.bridge(transition.provider.as_str()) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(amount_in) =
            sample_amount_for_asset(&transition.input.asset, self.asset_registry.as_ref())
        else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };

        let request = BridgeQuoteRequest {
            source_asset: transition.input.asset.clone(),
            destination_asset: transition.output.asset.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.to_string(),
                min_amount_out: "1".to_string(),
            },
            recipient_address: DUMMY_EVM_RECIPIENT.to_string(),
            depositor_address: DUMMY_EVM_DEPOSITOR.to_string(),
            partial_fills_enabled: false,
        };
        let quote = match timeout(ROUTE_COST_PROVIDER_TIMEOUT, bridge.quote_bridge(request)).await {
            Ok(Ok(Some(quote))) => quote,
            Ok(Ok(None)) => {
                return LiveCostSnapshotOutcome::Failed("provider returned no route".to_string())
            }
            Ok(Err(err)) => return LiveCostSnapshotOutcome::Failed(err),
            Err(_) => return LiveCostSnapshotOutcome::Failed("provider timed out".to_string()),
        };
        let amount_out = match U256::from_str(&quote.amount_out) {
            Ok(amount_out) => amount_out,
            Err(err) => {
                return LiveCostSnapshotOutcome::Failed(format!(
                    "provider amount_out was not numeric: {err}"
                ))
            }
        };
        let estimate = structural_cost_estimate(transition.kind);
        let estimated_fee_bps = match quote_loss_bps(amount_in, amount_out) {
            Some(fee_bps) => fee_bps,
            None => {
                return LiveCostSnapshotOutcome::Failed(
                    "could not convert provider loss to bps".to_string(),
                )
            }
        };

        LiveCostSnapshotOutcome::Succeeded(RouteCostSnapshot {
            transition_id: transition.id.clone(),
            amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
            provider: transition.provider.as_str().to_string(),
            edge_kind: transition.route_edge_kind().as_str().to_string(),
            source_asset: transition.input.asset.clone(),
            destination_asset: transition.output.asset.clone(),
            estimated_fee_bps,
            estimated_gas_usd_micros: estimate.estimated_gas_usd_micros,
            estimated_latency_ms: estimate.estimated_latency_ms,
            sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            quote_source: format!("provider_quote:{}", quote.provider_id),
            refreshed_at,
            expires_at,
        })
    }
}

enum LiveCostSnapshotOutcome {
    NotAttempted,
    Succeeded(RouteCostSnapshot),
    Failed(String),
}

#[must_use]
pub fn path_score(
    path: &TransitionPath,
    snapshots: &HashMap<String, RouteCostSnapshot>,
) -> RoutePathCostScore {
    let mut missing_edges = 0_usize;
    let mut total_effective_cost_bps = 0_u64;
    let mut total_latency_ms = 0_u64;

    for transition in &path.transitions {
        if let Some(snapshot) = snapshots.get(&transition.id) {
            total_effective_cost_bps =
                total_effective_cost_bps.saturating_add(snapshot.effective_cost_bps());
            total_latency_ms = total_latency_ms.saturating_add(snapshot.estimated_latency_ms);
        } else {
            missing_edges = missing_edges.saturating_add(1);
            total_effective_cost_bps = total_effective_cost_bps.saturating_add(u64::MAX / 4);
            total_latency_ms = total_latency_ms.saturating_add(u64::MAX / 4);
        }
    }

    RoutePathCostScore {
        missing_edges,
        total_effective_cost_bps,
        total_latency_ms,
    }
}

fn structural_cost_snapshot(
    transition: &TransitionDecl,
    refreshed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
) -> RouteCostSnapshot {
    let estimate = structural_cost_estimate(transition.kind);
    RouteCostSnapshot {
        transition_id: transition.id.clone(),
        amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
        provider: transition.provider.as_str().to_string(),
        edge_kind: transition.route_edge_kind().as_str().to_string(),
        source_asset: transition.input.asset.clone(),
        destination_asset: transition.output.asset.clone(),
        estimated_fee_bps: estimate.estimated_fee_bps,
        estimated_gas_usd_micros: estimate.estimated_gas_usd_micros,
        estimated_latency_ms: estimate.estimated_latency_ms,
        sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
        quote_source: estimate.quote_source.to_string(),
        refreshed_at,
        expires_at,
    }
}

#[derive(Debug, Clone, Copy)]
struct StructuralCostEstimate {
    estimated_fee_bps: u64,
    estimated_gas_usd_micros: u64,
    estimated_latency_ms: u64,
    quote_source: &'static str,
}

fn structural_cost_estimate(kind: MarketOrderTransitionKind) -> StructuralCostEstimate {
    match kind {
        MarketOrderTransitionKind::AcrossBridge => StructuralCostEstimate {
            estimated_fee_bps: 6,
            estimated_gas_usd_micros: 0,
            estimated_latency_ms: 120_000,
            quote_source: "static_across_anchor_seed",
        },
        MarketOrderTransitionKind::UnitDeposit | MarketOrderTransitionKind::UnitWithdrawal => {
            StructuralCostEstimate {
                estimated_fee_bps: 0,
                estimated_gas_usd_micros: 0,
                estimated_latency_ms: 60_000,
                quote_source: "static_unit_anchor_seed",
            }
        }
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => StructuralCostEstimate {
            estimated_fee_bps: 0,
            estimated_gas_usd_micros: 0,
            estimated_latency_ms: 30_000,
            quote_source: "static_hyperliquid_bridge_seed",
        },
        MarketOrderTransitionKind::HyperliquidTrade => StructuralCostEstimate {
            estimated_fee_bps: 4,
            estimated_gas_usd_micros: 0,
            estimated_latency_ms: 1_500,
            quote_source: "static_hyperliquid_spot_seed",
        },
        MarketOrderTransitionKind::UniversalRouterSwap => StructuralCostEstimate {
            estimated_fee_bps: 25,
            estimated_gas_usd_micros: 750_000,
            estimated_latency_ms: 12_000,
            quote_source: "static_velora_universal_router_seed",
        },
    }
}

fn sample_amount_for_asset(asset: &DepositAsset, registry: &AssetRegistry) -> Option<U256> {
    let chain_asset = registry.chain_asset(asset)?;
    let amount_usd_micro = U256::from(DEFAULT_SAMPLE_AMOUNT_USD_MICROS);
    let asset_usd_micro = match chain_asset.canonical {
        crate::services::asset_registry::CanonicalAsset::Usdc
        | crate::services::asset_registry::CanonicalAsset::Usdt => U256::from(1_000_000_u64),
        crate::services::asset_registry::CanonicalAsset::Eth => U256::from(ETH_USD_MICRO),
        crate::services::asset_registry::CanonicalAsset::Btc
        | crate::services::asset_registry::CanonicalAsset::Cbbtc => U256::from(BTC_USD_MICRO),
        crate::services::asset_registry::CanonicalAsset::Hype => return None,
    };
    Some(
        (amount_usd_micro.saturating_mul(pow10(chain_asset.decimals)) / asset_usd_micro)
            .max(U256::from(1_u64)),
    )
}

fn quote_loss_bps(amount_in: U256, amount_out: U256) -> Option<u64> {
    if amount_in.is_zero() {
        return None;
    }
    if amount_out >= amount_in {
        return Some(0);
    }
    let loss = amount_in.saturating_sub(amount_out);
    let bps = loss
        .saturating_mul(U256::from(BPS_DENOMINATOR))
        .saturating_add(amount_in.saturating_sub(U256::from(1_u64)))
        / amount_in;
    u64::try_from(bps).ok()
}

fn pow10(decimals: u8) -> U256 {
    let mut value = U256::from(1_u64);
    for _ in 0..decimals {
        value = value.saturating_mul(U256::from(10_u64));
    }
    value
}

fn div_ceil_u64(numerator: u64, denominator: u64) -> u64 {
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
    use crate::{
        protocol::{AssetId, ChainId},
        services::asset_registry::{
            AssetSlot, ProviderId, RequiredCustodyRole, TransitionMetadata,
        },
    };

    fn asset(chain: &str) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset: AssetId::Native,
        }
    }

    fn transition(id: &str, kind: MarketOrderTransitionKind) -> TransitionDecl {
        TransitionDecl {
            id: id.to_string(),
            kind,
            provider: match kind {
                MarketOrderTransitionKind::HyperliquidTrade => ProviderId::Hyperliquid,
                MarketOrderTransitionKind::UniversalRouterSwap => ProviderId::Velora,
                MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                    ProviderId::HyperliquidBridge
                }
                MarketOrderTransitionKind::UnitDeposit
                | MarketOrderTransitionKind::UnitWithdrawal => ProviderId::Unit,
                MarketOrderTransitionKind::AcrossBridge => ProviderId::Across,
            },
            input: AssetSlot {
                asset: asset("evm:1"),
                required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
            },
            output: AssetSlot {
                asset: asset("evm:8453"),
                required_custody_role: RequiredCustodyRole::IntermediateExecution,
            },
            from: crate::services::asset_registry::MarketOrderNode::External(asset("evm:1")),
            to: crate::services::asset_registry::MarketOrderNode::External(asset("evm:8453")),
        }
    }

    fn path(id: &str, transitions: Vec<TransitionDecl>) -> TransitionPath {
        TransitionPath {
            id: id.to_string(),
            transitions,
            metadata: TransitionMetadata {
                reliability_prior: 1.0,
                p50_latency_ms: 0,
                p95_latency_ms: 0,
            },
        }
    }

    #[test]
    fn effective_cost_adds_fee_and_gas_bps() {
        let snapshot = RouteCostSnapshot {
            transition_id: "edge".to_string(),
            amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
            provider: "test".to_string(),
            edge_kind: "fixed_pair_swap".to_string(),
            source_asset: asset("evm:1"),
            destination_asset: asset("evm:8453"),
            estimated_fee_bps: 5,
            estimated_gas_usd_micros: 1_000_000,
            estimated_latency_ms: 1,
            sample_amount_usd_micros: 1_000_000_000,
            quote_source: "test".to_string(),
            refreshed_at: Utc::now(),
            expires_at: Utc::now(),
        };

        assert_eq!(snapshot.effective_cost_bps(), 15);
    }

    #[test]
    fn path_score_prefers_known_lower_cost_edges() {
        let expensive = transition("expensive", MarketOrderTransitionKind::AcrossBridge);
        let cheap_a = transition("cheap_a", MarketOrderTransitionKind::HyperliquidTrade);
        let cheap_b = transition("cheap_b", MarketOrderTransitionKind::HyperliquidTrade);
        let now = Utc::now();
        let mut snapshots = HashMap::new();
        snapshots.insert(
            expensive.id.clone(),
            RouteCostSnapshot {
                transition_id: expensive.id.clone(),
                amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                provider: "across".to_string(),
                edge_kind: "cross_chain_transfer".to_string(),
                source_asset: expensive.input.asset.clone(),
                destination_asset: expensive.output.asset.clone(),
                estimated_fee_bps: 20,
                estimated_gas_usd_micros: 0,
                estimated_latency_ms: 1,
                sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                quote_source: "test".to_string(),
                refreshed_at: now,
                expires_at: now,
            },
        );
        for transition in [&cheap_a, &cheap_b] {
            snapshots.insert(
                transition.id.clone(),
                RouteCostSnapshot {
                    transition_id: transition.id.clone(),
                    amount_bucket: DEFAULT_AMOUNT_BUCKET.to_string(),
                    provider: "hyperliquid".to_string(),
                    edge_kind: "fixed_pair_swap".to_string(),
                    source_asset: transition.input.asset.clone(),
                    destination_asset: transition.output.asset.clone(),
                    estimated_fee_bps: 4,
                    estimated_gas_usd_micros: 0,
                    estimated_latency_ms: 1,
                    sample_amount_usd_micros: DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
                    quote_source: "test".to_string(),
                    refreshed_at: now,
                    expires_at: now,
                },
            );
        }

        let one_hop = path("one_hop", vec![expensive]);
        let two_hop = path("two_hop", vec![cheap_a, cheap_b]);

        assert!(
            path_score(&two_hop, &snapshots).total_effective_cost_bps
                < path_score(&one_hop, &snapshots).total_effective_cost_bps
        );
    }

    #[test]
    fn sample_amount_uses_asset_decimals_and_reference_prices() {
        let registry = AssetRegistry::default();
        let usdt = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
        };
        let cbbtc = DepositAsset {
            chain: ChainId::parse("evm:42161").unwrap(),
            asset: AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
        };

        assert_eq!(
            sample_amount_for_asset(&usdt, &registry),
            Some(U256::from(1_000_000_000_u64))
        );
        assert_eq!(
            sample_amount_for_asset(&cbbtc, &registry),
            Some(U256::from(1_000_000_u64))
        );
    }

    #[test]
    fn quote_loss_bps_rounds_loss_up() {
        assert_eq!(
            quote_loss_bps(U256::from(1_000_000_u64), U256::from(999_001_u64)),
            Some(10)
        );
        assert_eq!(
            quote_loss_bps(U256::from(1_000_000_u64), U256::from(1_000_001_u64)),
            Some(0)
        );
    }
}
