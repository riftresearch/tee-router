use crate::{
    protocol::{ChainId, DepositAsset},
    services::{
        asset_registry::{
            AssetRegistry, CanonicalAsset, MarketOrderTransitionKind, ProviderAssetCapability,
            ProviderId, TransitionPath,
        },
        pricing::{apply_bps_multiplier, PricingSnapshot, BPS_DENOMINATOR},
    },
};
use alloy::primitives::U256;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const QUOTE_SAFETY_MULTIPLIER_BPS: u64 = 12_500;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasReimbursementPlan {
    pub schema_version: u32,
    pub policy: String,
    pub quote_safety_multiplier_bps: u64,
    pub debts: Vec<GasReimbursementDebt>,
    pub retention_actions: Vec<GasRetentionAction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasReimbursementDebt {
    pub id: String,
    pub transition_decl_id: String,
    pub transition_kind: String,
    pub spend_chain_id: String,
    pub payment_model: GasPaymentModel,
    pub estimated_native_gas_wei: String,
    pub estimated_usd_micro: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GasPaymentModel {
    PaymasterAdvanced,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GasRetentionAction {
    pub id: String,
    pub transition_decl_id: String,
    pub settlement_chain_id: String,
    pub settlement_asset_id: String,
    pub settlement_decimals: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement_provider_asset: Option<String>,
    pub amount: String,
    pub estimated_usd_micro: String,
    pub recipient_role: String,
    pub timing: String,
    pub debt_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GasReimbursementError {
    PricingUnavailable,
    UnsupportedSettlementAsset { asset: DepositAsset },
    NoSettlementSite { debt_ids: Vec<String> },
    InvalidPlanAmount { action_id: String, amount: String },
    NumericOverflow { context: &'static str },
}

impl std::fmt::Display for GasReimbursementError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PricingUnavailable => {
                write!(f, "live paymaster gas reimbursement pricing is unavailable")
            }
            Self::UnsupportedSettlementAsset { asset } => write!(
                f,
                "unsupported paymaster gas settlement asset {} {}",
                asset.chain, asset.asset
            ),
            Self::NoSettlementSite { debt_ids } => write!(
                f,
                "no eligible paymaster gas settlement site for debts [{}]",
                debt_ids.join(", ")
            ),
            Self::InvalidPlanAmount { action_id, amount } => write!(
                f,
                "paymaster gas retention action {action_id} has invalid amount {amount}"
            ),
            Self::NumericOverflow { context } => {
                write!(
                    f,
                    "paymaster gas reimbursement numeric overflow in {context}"
                )
            }
        }
    }
}

impl std::error::Error for GasReimbursementError {}

pub fn optimized_paymaster_reimbursement_plan(
    registry: &AssetRegistry,
    path: &TransitionPath,
) -> Result<GasReimbursementPlan, GasReimbursementError> {
    let pricing = PricingSnapshot::static_bootstrap(Utc::now());
    optimized_paymaster_reimbursement_plan_with_pricing(registry, path, &pricing)
}

pub fn optimized_paymaster_reimbursement_plan_with_pricing(
    registry: &AssetRegistry,
    path: &TransitionPath,
    pricing: &PricingSnapshot,
) -> Result<GasReimbursementPlan, GasReimbursementError> {
    let debts = paymaster_debts(path, pricing)?;
    if debts.is_empty() {
        return Ok(GasReimbursementPlan {
            schema_version: 1,
            policy: "optimized_cross_chain_paymaster_v1".to_string(),
            quote_safety_multiplier_bps: QUOTE_SAFETY_MULTIPLIER_BPS,
            debts,
            retention_actions: vec![],
        });
    }

    let candidates = settlement_candidates(registry, path, pricing)?;
    let mut best: Option<(SettlementCandidate, U256)> = None;
    for candidate in candidates {
        let score = candidate.score_for(&debts)?;
        if best
            .as_ref()
            .map(|(_, best_score)| score < *best_score)
            .unwrap_or(true)
        {
            best = Some((candidate, score));
        }
    }
    let Some((best, _score)) = best else {
        return Err(GasReimbursementError::NoSettlementSite {
            debt_ids: debts.iter().map(|debt| debt.id.clone()).collect(),
        });
    };
    let total_usd_micro = debts.iter().try_fold(U256::ZERO, |acc, debt| {
        acc.checked_add(debt.usd_micro()?)
            .ok_or(GasReimbursementError::NumericOverflow {
                context: "total paymaster debt usd sum",
            })
    })?;
    let retained_usd_micro = apply_bps_multiplier(total_usd_micro, QUOTE_SAFETY_MULTIPLIER_BPS)
        .ok_or(GasReimbursementError::NumericOverflow {
            context: "paymaster debt safety multiplier",
        })?;
    let amount = usd_micro_to_asset_raw(retained_usd_micro, &best.asset, registry, pricing)?;

    Ok(GasReimbursementPlan {
        schema_version: 1,
        policy: "optimized_cross_chain_paymaster_v1".to_string(),
        quote_safety_multiplier_bps: QUOTE_SAFETY_MULTIPLIER_BPS,
        retention_actions: vec![GasRetentionAction {
            id: "paymaster-retention:0".to_string(),
            transition_decl_id: best.transition_decl_id,
            settlement_chain_id: best.asset.chain.as_str().to_string(),
            settlement_asset_id: best.asset.asset.as_str().to_string(),
            settlement_decimals: best.decimals,
            settlement_provider_asset: best.provider_asset,
            amount: amount.to_string(),
            estimated_usd_micro: retained_usd_micro.to_string(),
            recipient_role: "paymaster_wallet".to_string(),
            timing: "before_provider_action".to_string(),
            debt_ids: debts.iter().map(|debt| debt.id.clone()).collect(),
        }],
        debts,
    })
}

pub fn try_transition_retention_amount(
    plan: &GasReimbursementPlan,
    transition_id: &str,
) -> Result<U256, GasReimbursementError> {
    plan.retention_actions
        .iter()
        .filter(|action| action.transition_decl_id == transition_id)
        .try_fold(U256::ZERO, |acc, action| {
            let amount = parse_positive_retention_amount(&action.id, &action.amount)?;
            acc.checked_add(amount)
                .ok_or(GasReimbursementError::NumericOverflow {
                    context: "transition retention amount sum",
                })
        })
}

pub fn parse_positive_retention_amount(
    action_id: &str,
    amount: &str,
) -> Result<U256, GasReimbursementError> {
    if amount.is_empty() || !amount.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(GasReimbursementError::InvalidPlanAmount {
            action_id: action_id.to_string(),
            amount: amount.to_string(),
        });
    }
    let parsed =
        U256::from_str_radix(amount, 10).map_err(|_| GasReimbursementError::InvalidPlanAmount {
            action_id: action_id.to_string(),
            amount: amount.to_string(),
        })?;
    if parsed == U256::ZERO {
        return Err(GasReimbursementError::InvalidPlanAmount {
            action_id: action_id.to_string(),
            amount: amount.to_string(),
        });
    }
    Ok(parsed)
}

#[derive(Debug, Clone)]
struct SettlementCandidate {
    transition_decl_id: String,
    asset: DepositAsset,
    decimals: u8,
    provider_asset: Option<String>,
    collection_cost_usd_micro: U256,
}

impl SettlementCandidate {
    fn score_for(&self, debts: &[GasReimbursementDebt]) -> Result<U256, GasReimbursementError> {
        let inventory_penalty = debts.iter().try_fold(U256::ZERO, |acc, debt| {
            if debt.spend_chain_id == self.asset.chain.as_str() {
                Ok(acc)
            } else {
                let penalty = debt.usd_micro()?.checked_mul(U256::from(25_u64)).ok_or(
                    GasReimbursementError::NumericOverflow {
                        context: "cross-chain settlement inventory penalty",
                    },
                )? / U256::from(BPS_DENOMINATOR);
                acc.checked_add(penalty)
                    .ok_or(GasReimbursementError::NumericOverflow {
                        context: "cross-chain settlement inventory penalty sum",
                    })
            }
        })?;
        self.collection_cost_usd_micro
            .checked_add(inventory_penalty)
            .ok_or(GasReimbursementError::NumericOverflow {
                context: "settlement candidate score",
            })
    }
}

impl GasReimbursementDebt {
    fn usd_micro(&self) -> Result<U256, GasReimbursementError> {
        U256::from_str_radix(&self.estimated_usd_micro, 10).map_err(|_| {
            GasReimbursementError::NumericOverflow {
                context: "paymaster debt usd amount parse",
            }
        })
    }
}

fn paymaster_debts(
    path: &TransitionPath,
    pricing: &PricingSnapshot,
) -> Result<Vec<GasReimbursementDebt>, GasReimbursementError> {
    let mut debts = Vec::new();
    for (index, transition) in path.transitions.iter().enumerate() {
        if transition.kind == MarketOrderTransitionKind::CctpBridge {
            if is_evm_chain(&transition.input.asset.chain)
                && !transition.input.asset.asset.is_native()
            {
                debts.push(paymaster_debt(
                    format!("paymaster-gas:{index}:source"),
                    transition.id.clone(),
                    transition.kind,
                    &transition.input.asset.chain,
                    pricing,
                )?);
            }
            if is_evm_chain(&transition.output.asset.chain) {
                debts.push(paymaster_debt(
                    format!("paymaster-gas:{index}:destination"),
                    transition.id.clone(),
                    transition.kind,
                    &transition.output.asset.chain,
                    pricing,
                )?);
            }
            continue;
        }
        if !transition_requires_evm_sender_gas(transition.kind)
            || !is_evm_chain(&transition.input.asset.chain)
            || transition.input.asset.asset.is_native()
        {
            continue;
        }
        debts.push(paymaster_debt(
            format!("paymaster-gas:{index}"),
            transition.id.clone(),
            transition.kind,
            &transition.input.asset.chain,
            pricing,
        )?);
    }
    Ok(debts)
}

fn paymaster_debt(
    id: String,
    transition_decl_id: String,
    transition_kind: MarketOrderTransitionKind,
    spend_chain: &ChainId,
    pricing: &PricingSnapshot,
) -> Result<GasReimbursementDebt, GasReimbursementError> {
    let estimated_native_gas_wei =
        estimate_paymaster_native_cost_wei(spend_chain, transition_kind, pricing)?;
    let estimated_usd_micro = pricing
        .checked_wei_to_usd_micro(estimated_native_gas_wei)
        .ok_or(GasReimbursementError::NumericOverflow {
            context: "paymaster native gas wei to usd",
        })?;
    Ok(GasReimbursementDebt {
        id,
        transition_decl_id,
        transition_kind: transition_kind.as_str().to_string(),
        spend_chain_id: spend_chain.as_str().to_string(),
        payment_model: GasPaymentModel::PaymasterAdvanced,
        estimated_native_gas_wei: estimated_native_gas_wei.to_string(),
        estimated_usd_micro: estimated_usd_micro.to_string(),
    })
}

fn settlement_candidates(
    registry: &AssetRegistry,
    path: &TransitionPath,
    pricing: &PricingSnapshot,
) -> Result<Vec<SettlementCandidate>, GasReimbursementError> {
    let mut by_site = BTreeMap::<(String, String, String), SettlementCandidate>::new();
    for transition in &path.transitions {
        let asset = &transition.input.asset;
        let Some(chain_asset) = registry.chain_asset(asset) else {
            continue;
        };
        let (eligible, collection_cost_usd_micro, provider_asset) =
            if asset.chain.as_str() == "hyperliquid" {
                let provider_asset = registry
                    .provider_asset(
                        ProviderId::Hyperliquid,
                        asset,
                        ProviderAssetCapability::ExchangeInput,
                    )
                    .map(|entry| entry.provider_asset.clone());
                (
                    provider_asset.is_some()
                        && is_supported_settlement_canonical(chain_asset.canonical),
                    U256::ZERO,
                    provider_asset,
                )
            } else if is_evm_chain(&asset.chain)
                && !asset.asset.is_native()
                && is_supported_settlement_canonical(chain_asset.canonical)
            {
                (
                    true,
                    pricing
                        .checked_wei_to_usd_micro(estimate_erc20_collection_cost_wei(
                            &asset.chain,
                            pricing,
                        )?)
                        .ok_or(GasReimbursementError::NumericOverflow {
                            context: "erc20 settlement collection gas wei to usd",
                        })?,
                    None,
                )
            } else {
                (false, U256::ZERO, None)
            };
        if !eligible {
            continue;
        }
        by_site
            .entry((
                asset.chain.as_str().to_string(),
                asset.asset.as_str().to_string(),
                transition.id.clone(),
            ))
            .or_insert_with(|| SettlementCandidate {
                transition_decl_id: transition.id.clone(),
                asset: asset.clone(),
                decimals: chain_asset.decimals,
                provider_asset,
                collection_cost_usd_micro,
            });
    }
    Ok(by_site.into_values().collect())
}

fn transition_requires_evm_sender_gas(kind: MarketOrderTransitionKind) -> bool {
    matches!(
        kind,
        MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::UnitDeposit
            | MarketOrderTransitionKind::UniversalRouterSwap
    )
}

fn estimate_paymaster_native_cost_wei(
    chain: &ChainId,
    kind: MarketOrderTransitionKind,
    pricing: &PricingSnapshot,
) -> Result<U256, GasReimbursementError> {
    let action_gas_units = match kind {
        MarketOrderTransitionKind::AcrossBridge => 450_000_u64,
        MarketOrderTransitionKind::CctpBridge => 300_000_u64,
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => 180_000_u64,
        MarketOrderTransitionKind::UnitDeposit => 140_000_u64,
        MarketOrderTransitionKind::UniversalRouterSwap => 360_000_u64,
        MarketOrderTransitionKind::HyperliquidBridgeWithdrawal
        | MarketOrderTransitionKind::HyperliquidTrade
        | MarketOrderTransitionKind::UnitWithdrawal => 0_u64,
    };
    let top_up_gas_units = 21_000_u64;
    U256::from(action_gas_units + top_up_gas_units)
        .checked_mul(pricing.chain_gas_price_wei(chain))
        .ok_or(GasReimbursementError::NumericOverflow {
            context: "paymaster native gas estimate",
        })
}

fn estimate_erc20_collection_cost_wei(
    chain: &ChainId,
    pricing: &PricingSnapshot,
) -> Result<U256, GasReimbursementError> {
    U256::from(65_000_u64)
        .checked_mul(pricing.chain_gas_price_wei(chain))
        .ok_or(GasReimbursementError::NumericOverflow {
            context: "erc20 settlement collection gas estimate",
        })
}

fn is_supported_settlement_canonical(canonical: CanonicalAsset) -> bool {
    matches!(
        canonical,
        CanonicalAsset::Usdc | CanonicalAsset::Usdt | CanonicalAsset::Eth
    )
}

fn usd_micro_to_asset_raw(
    usd_micro: U256,
    asset: &DepositAsset,
    registry: &AssetRegistry,
    pricing: &PricingSnapshot,
) -> Result<U256, GasReimbursementError> {
    let chain_asset = registry.chain_asset(asset).ok_or_else(|| {
        GasReimbursementError::UnsupportedSettlementAsset {
            asset: asset.clone(),
        }
    })?;
    match chain_asset.canonical {
        CanonicalAsset::Usdc | CanonicalAsset::Usdt | CanonicalAsset::Eth => pricing
            .usd_micro_to_asset_raw(usd_micro, chain_asset.canonical, chain_asset.decimals)
            .ok_or_else(|| GasReimbursementError::UnsupportedSettlementAsset {
                asset: asset.clone(),
            }),
        _ => Err(GasReimbursementError::UnsupportedSettlementAsset {
            asset: asset.clone(),
        }),
    }
}

fn is_evm_chain(chain: &ChainId) -> bool {
    chain.evm_chain_id().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::AssetId,
        services::asset_registry::{
            AssetSlot, MarketOrderNode, RequiredCustodyRole, TransitionDecl,
        },
    };

    fn asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset,
        }
    }

    #[test]
    fn evm_usdc_to_btc_settles_all_paymaster_debt_on_hyperliquid_usdc() {
        let registry = AssetRegistry::default();
        let btc = asset("bitcoin", AssetId::Native);
        for source in [
            asset(
                "evm:1",
                AssetId::reference("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            ),
            asset(
                "evm:8453",
                AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            ),
            asset(
                "evm:42161",
                AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
            ),
        ] {
            let path = registry
                .select_transition_paths(&source, &btc, 5)
                .into_iter()
                .find(|path| {
                    path.transitions.iter().any(|transition| {
                        transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                    })
                })
                .expect("evm usdc to btc path");

            let plan = optimized_paymaster_reimbursement_plan(&registry, &path).unwrap();

            assert!(!plan.debts.is_empty());
            assert_eq!(plan.retention_actions.len(), 1);
            let action = &plan.retention_actions[0];
            assert_eq!(action.settlement_chain_id, "hyperliquid");
            assert_eq!(action.settlement_asset_id, "native");
            assert_eq!(action.settlement_provider_asset.as_deref(), Some("USDC"));
            assert_eq!(action.debt_ids.len(), plan.debts.len());
            assert!(U256::from_str_radix(&action.amount, 10).unwrap() > U256::ZERO);
        }
    }

    #[test]
    fn base_usdc_to_arbitrum_usdc_has_cctp_source_and_destination_reimbursement() {
        let registry = AssetRegistry::default();
        let base_usdc = asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );
        let arb_usdc = asset(
            "evm:42161",
            AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
        );
        let path = registry
            .select_transition_paths(&base_usdc, &arb_usdc, 2)
            .into_iter()
            .next()
            .expect("base to arbitrum usdc path");

        let plan = optimized_paymaster_reimbursement_plan(&registry, &path).unwrap();

        assert_eq!(plan.debts.len(), 2);
        assert_eq!(plan.retention_actions.len(), 1);
        assert_eq!(plan.retention_actions[0].settlement_chain_id, "evm:8453");
    }

    #[test]
    fn native_eth_route_has_no_paymaster_reimbursement() {
        let registry = AssetRegistry::default();
        let base_eth = asset("evm:8453", AssetId::Native);
        let btc = asset("bitcoin", AssetId::Native);
        let path = registry
            .select_transition_paths(&base_eth, &btc, 5)
            .into_iter()
            .next()
            .expect("base eth to btc path");

        let plan = optimized_paymaster_reimbursement_plan(&registry, &path).unwrap();

        assert!(plan.debts.is_empty());
        assert!(plan.retention_actions.is_empty());
    }

    #[test]
    fn unsupported_erc20_assets_are_not_gas_settlement_candidates() {
        let registry = AssetRegistry::default();
        let base_cbbtc = asset(
            "evm:8453",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
        );
        let base_usdc = asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );
        let arbitrum_usdc = asset(
            "evm:42161",
            AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
        );
        let path = TransitionPath {
            id: "test-path".to_string(),
            transitions: vec![
                transition(
                    "test-swap",
                    MarketOrderTransitionKind::UniversalRouterSwap,
                    ProviderId::Velora,
                    base_cbbtc,
                    base_usdc.clone(),
                ),
                transition(
                    "test-bridge",
                    MarketOrderTransitionKind::CctpBridge,
                    ProviderId::Cctp,
                    base_usdc,
                    arbitrum_usdc,
                ),
            ],
        };

        let plan = optimized_paymaster_reimbursement_plan(&registry, &path).unwrap();

        assert_eq!(plan.retention_actions.len(), 1);
        assert_eq!(plan.retention_actions[0].settlement_chain_id, "evm:8453");
        assert_eq!(
            plan.retention_actions[0].settlement_asset_id,
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        );
    }

    #[test]
    fn transition_retention_amount_rejects_invalid_plan_amounts() {
        for amount in ["", "not-a-number", "0", "00"] {
            let plan = GasReimbursementPlan {
                schema_version: 1,
                policy: "test".to_string(),
                quote_safety_multiplier_bps: 10_000,
                debts: vec![],
                retention_actions: vec![GasRetentionAction {
                    id: "retention-1".to_string(),
                    transition_decl_id: "transition-1".to_string(),
                    settlement_chain_id: "evm:1".to_string(),
                    settlement_asset_id: "native".to_string(),
                    settlement_decimals: 18,
                    settlement_provider_asset: None,
                    amount: amount.to_string(),
                    estimated_usd_micro: "1".to_string(),
                    recipient_role: "paymaster_wallet".to_string(),
                    timing: "before_provider_action".to_string(),
                    debt_ids: vec![],
                }],
            };

            assert!(
                matches!(
                    try_transition_retention_amount(&plan, "transition-1"),
                    Err(GasReimbursementError::InvalidPlanAmount { .. })
                ),
                "amount {amount:?} must be rejected"
            );
        }
    }

    #[test]
    fn transition_retention_amount_rejects_overflowing_sums() {
        let action = |id: &str, amount: U256| GasRetentionAction {
            id: id.to_string(),
            transition_decl_id: "transition-1".to_string(),
            settlement_chain_id: "evm:1".to_string(),
            settlement_asset_id: "native".to_string(),
            settlement_decimals: 18,
            settlement_provider_asset: None,
            amount: amount.to_string(),
            estimated_usd_micro: "1".to_string(),
            recipient_role: "paymaster_wallet".to_string(),
            timing: "before_provider_action".to_string(),
            debt_ids: vec![],
        };
        let plan = GasReimbursementPlan {
            schema_version: 1,
            policy: "test".to_string(),
            quote_safety_multiplier_bps: 10_000,
            debts: vec![],
            retention_actions: vec![
                action("retention-1", U256::MAX),
                action("retention-2", U256::from(1_u64)),
            ],
        };

        assert!(matches!(
            try_transition_retention_amount(&plan, "transition-1"),
            Err(GasReimbursementError::NumericOverflow { .. })
        ));
    }

    #[test]
    fn settlement_candidate_score_rejects_inventory_penalty_overflow() {
        let candidate = SettlementCandidate {
            transition_decl_id: "transition-1".to_string(),
            asset: asset(
                "evm:8453",
                AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            ),
            decimals: 6,
            provider_asset: None,
            collection_cost_usd_micro: U256::ZERO,
        };
        let debt = GasReimbursementDebt {
            id: "paymaster-gas:0".to_string(),
            transition_decl_id: "transition-0".to_string(),
            transition_kind: "universal_router_swap".to_string(),
            spend_chain_id: "evm:1".to_string(),
            payment_model: GasPaymentModel::PaymasterAdvanced,
            estimated_native_gas_wei: "1".to_string(),
            estimated_usd_micro: U256::MAX.to_string(),
        };

        assert!(matches!(
            candidate.score_for(&[debt]),
            Err(GasReimbursementError::NumericOverflow { .. })
        ));
    }

    fn transition(
        id: &str,
        kind: MarketOrderTransitionKind,
        provider: ProviderId,
        input: DepositAsset,
        output: DepositAsset,
    ) -> TransitionDecl {
        TransitionDecl {
            id: id.to_string(),
            kind,
            provider,
            input: slot(input.clone()),
            output: slot(output.clone()),
            from: MarketOrderNode::External(input),
            to: MarketOrderNode::External(output),
        }
    }

    fn slot(asset: DepositAsset) -> AssetSlot {
        AssetSlot {
            asset,
            required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
        }
    }
}
