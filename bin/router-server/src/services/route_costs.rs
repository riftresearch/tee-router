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
        pricing::{div_ceil_u64, PricingSnapshot, BPS_DENOMINATOR},
    },
};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use market_pricing::MarketPricingOracle;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tokio::time::timeout;
use tracing::{debug, warn};

const DEFAULT_REFRESH_TTL: Duration = Duration::from_secs(600);
const DEFAULT_AMOUNT_BUCKET: &str = "usd_1000";
const DEFAULT_SAMPLE_AMOUNT_USD_MICROS: u64 = 1_000_000_000;
const ROUTE_COST_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
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
    pub pricing_source: String,
    pub refreshed_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct RouteCostService {
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
    asset_registry: Arc<AssetRegistry>,
    ttl: Duration,
    pricing: Arc<RwLock<PricingSnapshot>>,
    pricing_oracle: Option<Arc<MarketPricingOracle>>,
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
            pricing: Arc::new(RwLock::new(PricingSnapshot::static_bootstrap(Utc::now()))),
            pricing_oracle: None,
        }
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    #[must_use]
    pub fn with_pricing(mut self, pricing: PricingSnapshot) -> Self {
        self.pricing = Arc::new(RwLock::new(pricing));
        self
    }

    #[must_use]
    pub fn with_pricing_oracle(mut self, pricing_oracle: Arc<MarketPricingOracle>) -> Self {
        self.pricing_oracle = Some(pricing_oracle);
        self
    }

    pub async fn current_pricing_snapshot(&self) -> PricingSnapshot {
        self.pricing.read().await.clone()
    }

    pub async fn refresh_anchor_costs(&self) -> RouterServerResult<RouteCostRefreshSummary> {
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(self.ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(600));
        let pricing = self.refresh_pricing_snapshot().await;
        let transitions = self.asset_registry.transition_declarations();
        let mut snapshots = Vec::with_capacity(transitions.len());
        let mut provider_quotes_attempted = 0_usize;
        let mut provider_quotes_succeeded = 0_usize;
        let mut provider_quotes_failed = 0_usize;

        for transition in &transitions {
            let live_snapshot = match self
                .live_cost_snapshot(transition, now, expires_at, &pricing)
                .await
            {
                LiveCostSnapshotOutcome::NotAttempted => None,
                LiveCostSnapshotOutcome::Succeeded(snapshot) => {
                    provider_quotes_attempted += 1;
                    provider_quotes_succeeded += 1;
                    Some(*snapshot)
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
            snapshots.push(live_snapshot.unwrap_or_else(|| {
                structural_cost_snapshot(transition, now, expires_at, &pricing)
            }));
        }
        self.db.route_costs().upsert_many(&snapshots).await?;

        Ok(RouteCostRefreshSummary {
            candidate_edges: transitions.len(),
            snapshots_upserted: snapshots.len(),
            provider_quotes_attempted,
            provider_quotes_succeeded,
            provider_quotes_failed,
            pricing_source: pricing.source.clone(),
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

        let pricing = self.current_pricing_snapshot().await;
        paths.sort_by(|left, right| {
            compare_path_scores(
                path_score_with_structural_fallback(left, &by_transition_id, &pricing),
                left.transitions.len(),
                path_score_with_structural_fallback(right, &by_transition_id, &pricing),
                right.transitions.len(),
            )
        });
        Ok(())
    }

    async fn refresh_pricing_snapshot(&self) -> PricingSnapshot {
        let Some(pricing_oracle) = self.pricing_oracle.as_ref() else {
            return self.current_pricing_snapshot().await;
        };
        match pricing_oracle.snapshot().await {
            Ok(snapshot) => {
                let pricing = PricingSnapshot::from_market(snapshot);
                *self.pricing.write().await = pricing.clone();
                pricing
            }
            Err(error) => {
                warn!(
                    error = %error,
                    "market pricing refresh failed; using previous pricing snapshot"
                );
                self.current_pricing_snapshot().await
            }
        }
    }

    async fn live_cost_snapshot(
        &self,
        transition: &TransitionDecl,
        refreshed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                self.live_bridge_cost_snapshot(transition, refreshed_at, expires_at, pricing)
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
        pricing: &PricingSnapshot,
    ) -> LiveCostSnapshotOutcome {
        let Some(bridge) = self.action_providers.bridge(transition.provider.as_str()) else {
            return LiveCostSnapshotOutcome::NotAttempted;
        };
        let Some(amount_in) = sample_amount_for_asset(
            &transition.input.asset,
            self.asset_registry.as_ref(),
            pricing,
        ) else {
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
        let estimate = structural_cost_estimate(transition, pricing);
        let quoted_fee_bps = match quote_loss_bps(amount_in, amount_out) {
            Some(fee_bps) => fee_bps,
            None => {
                return LiveCostSnapshotOutcome::Failed(
                    "could not convert provider loss to bps".to_string(),
                )
            }
        };
        let estimated_fee_bps = quoted_fee_bps.max(estimate.estimated_fee_bps);

        LiveCostSnapshotOutcome::Succeeded(Box::new(RouteCostSnapshot {
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
        }))
    }
}

enum LiveCostSnapshotOutcome {
    NotAttempted,
    Succeeded(Box<RouteCostSnapshot>),
    Failed(String),
}

pub fn rank_transition_paths_structurally(paths: &mut [TransitionPath]) {
    let pricing = PricingSnapshot::static_bootstrap(Utc::now());
    paths.sort_by(|left, right| {
        compare_path_scores(
            structural_path_score(left, &pricing),
            left.transitions.len(),
            structural_path_score(right, &pricing),
            right.transitions.len(),
        )
    });
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

fn path_score_with_structural_fallback(
    path: &TransitionPath,
    snapshots: &HashMap<String, RouteCostSnapshot>,
    pricing: &PricingSnapshot,
) -> RoutePathCostScore {
    let mut total_effective_cost_bps = 0_u64;
    let mut total_latency_ms = 0_u64;

    for transition in &path.transitions {
        if let Some(snapshot) = snapshots.get(&transition.id) {
            total_effective_cost_bps =
                total_effective_cost_bps.saturating_add(snapshot.effective_cost_bps());
            total_latency_ms = total_latency_ms.saturating_add(snapshot.estimated_latency_ms);
        } else {
            let estimate = structural_cost_estimate(transition, pricing);
            let gas_bps = div_ceil_u64(
                estimate
                    .estimated_gas_usd_micros
                    .saturating_mul(BPS_DENOMINATOR),
                DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            );
            total_effective_cost_bps = total_effective_cost_bps
                .saturating_add(estimate.estimated_fee_bps.saturating_add(gas_bps));
            total_latency_ms = total_latency_ms.saturating_add(estimate.estimated_latency_ms);
        }
    }

    RoutePathCostScore {
        missing_edges: 0,
        total_effective_cost_bps,
        total_latency_ms,
    }
}

fn structural_path_score(path: &TransitionPath, pricing: &PricingSnapshot) -> RoutePathCostScore {
    path_score_with_structural_fallback(path, &HashMap::new(), pricing)
}

fn compare_path_scores(
    left_score: RoutePathCostScore,
    left_len: usize,
    right_score: RoutePathCostScore,
    right_len: usize,
) -> std::cmp::Ordering {
    (
        left_score.missing_edges,
        left_score.total_effective_cost_bps,
        left_score.total_latency_ms,
        left_len,
    )
        .cmp(&(
            right_score.missing_edges,
            right_score.total_effective_cost_bps,
            right_score.total_latency_ms,
            right_len,
        ))
}

fn structural_cost_snapshot(
    transition: &TransitionDecl,
    refreshed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    pricing: &PricingSnapshot,
) -> RouteCostSnapshot {
    let estimate = structural_cost_estimate(transition, pricing);
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

fn structural_cost_estimate(
    transition: &TransitionDecl,
    pricing: &PricingSnapshot,
) -> StructuralCostEstimate {
    match transition.kind {
        MarketOrderTransitionKind::AcrossBridge => StructuralCostEstimate {
            estimated_fee_bps: 6,
            estimated_gas_usd_micros: structural_gas_usd_micros(
                pricing,
                &transition.input.asset.chain,
                450_000,
            ),
            estimated_latency_ms: 120_000,
            quote_source: "static_across_anchor_seed",
        },
        MarketOrderTransitionKind::CctpBridge => StructuralCostEstimate {
            estimated_fee_bps: 0,
            estimated_gas_usd_micros: structural_gas_usd_micros(
                pricing,
                &transition.input.asset.chain,
                300_000,
            )
            .saturating_add(structural_gas_usd_micros(
                pricing,
                &transition.output.asset.chain,
                300_000,
            )),
            estimated_latency_ms: 60_000,
            quote_source: "static_cctp_standard_seed",
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
            estimated_gas_usd_micros: structural_gas_usd_micros(
                pricing,
                &transition.input.asset.chain,
                360_000,
            ),
            estimated_latency_ms: 12_000,
            quote_source: "static_velora_universal_router_seed",
        },
    }
}

fn sample_amount_for_asset(
    asset: &DepositAsset,
    registry: &AssetRegistry,
    pricing: &PricingSnapshot,
) -> Option<U256> {
    let chain_asset = registry.chain_asset(asset)?;
    pricing
        .sample_amount_raw(
            DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
            chain_asset.canonical,
            chain_asset.decimals,
        )
        .map(|amount| amount.max(U256::from(1_u64)))
}

fn structural_gas_usd_micros(
    pricing: &PricingSnapshot,
    chain: &crate::protocol::ChainId,
    gas_units: u64,
) -> u64 {
    pricing
        .wei_to_usd_micro(U256::from(gas_units).saturating_mul(pricing.chain_gas_price_wei(chain)))
        .try_into()
        .unwrap_or(u64::MAX)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::{AssetId, ChainId},
        services::asset_registry::{AssetSlot, ProviderId, RequiredCustodyRole},
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
                MarketOrderTransitionKind::CctpBridge => ProviderId::Cctp,
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
    fn structural_route_ranking_prefers_cctp_for_base_usdc_to_bitcoin() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let base_usdc = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        };
        let btc = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };
        let paths = registry.select_transition_paths(&base_usdc, &btc, 5);
        let cctp_path = paths
            .iter()
            .find(|path| {
                path.transitions
                    .iter()
                    .any(|transition| transition.kind == MarketOrderTransitionKind::CctpBridge)
            })
            .expect("CCTP route should exist");
        let across_path = paths
            .iter()
            .find(|path| {
                path.transitions
                    .iter()
                    .any(|transition| transition.kind == MarketOrderTransitionKind::AcrossBridge)
            })
            .expect("Across route should exist");

        assert!(
            structural_path_score(cctp_path, &pricing).total_effective_cost_bps
                < structural_path_score(across_path, &pricing).total_effective_cost_bps
        );
    }

    #[test]
    fn structural_usdc_bridge_costs_choose_cctp_for_l2_l2_and_across_for_eth_destination() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let base_usdc = usdc("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
        let arbitrum_usdc = usdc("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
        let ethereum_usdc = usdc("evm:1", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");

        assert!(
            direct_bridge_cost(
                &registry,
                &pricing,
                &base_usdc,
                &arbitrum_usdc,
                MarketOrderTransitionKind::CctpBridge
            ) < direct_bridge_cost(
                &registry,
                &pricing,
                &base_usdc,
                &arbitrum_usdc,
                MarketOrderTransitionKind::AcrossBridge
            )
        );
        assert!(
            direct_bridge_cost(
                &registry,
                &pricing,
                &arbitrum_usdc,
                &ethereum_usdc,
                MarketOrderTransitionKind::AcrossBridge
            ) < direct_bridge_cost(
                &registry,
                &pricing,
                &arbitrum_usdc,
                &ethereum_usdc,
                MarketOrderTransitionKind::CctpBridge
            )
        );
        assert!(
            direct_bridge_cost(
                &registry,
                &pricing,
                &base_usdc,
                &ethereum_usdc,
                MarketOrderTransitionKind::AcrossBridge
            ) < direct_bridge_cost(
                &registry,
                &pricing,
                &base_usdc,
                &ethereum_usdc,
                MarketOrderTransitionKind::CctpBridge
            )
        );
    }

    #[test]
    fn sample_amount_uses_asset_decimals_and_reference_prices() {
        let registry = AssetRegistry::default();
        let pricing = PricingSnapshot::static_bootstrap(Utc::now());
        let usdt = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
        };
        let cbbtc = DepositAsset {
            chain: ChainId::parse("evm:42161").unwrap(),
            asset: AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
        };

        assert_eq!(
            sample_amount_for_asset(&usdt, &registry, &pricing),
            Some(U256::from(1_000_000_000_u64))
        );
        assert_eq!(
            sample_amount_for_asset(&cbbtc, &registry, &pricing),
            Some(U256::from(1_000_000_u64))
        );
    }

    fn usdc(chain: &str, address: &str) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset: AssetId::reference(address),
        }
    }

    fn direct_bridge_cost(
        registry: &AssetRegistry,
        pricing: &PricingSnapshot,
        source: &DepositAsset,
        destination: &DepositAsset,
        kind: MarketOrderTransitionKind,
    ) -> u64 {
        let path = registry
            .select_transition_paths(source, destination, 1)
            .into_iter()
            .find(|path| path.transitions.len() == 1 && path.transitions[0].kind == kind)
            .unwrap_or_else(|| panic!("missing direct {kind:?} path"));
        structural_path_score(&path, pricing).total_effective_cost_bps
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
