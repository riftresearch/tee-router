use crate::{
    models::{
        CustodyVaultRole, DepositVault, LimitOrderQuote, MarketOrderKindType, MarketOrderQuote,
        OrderExecutionLeg, OrderExecutionStep, OrderExecutionStepStatus, OrderExecutionStepType,
        RouterOrder, RouterOrderAction,
    },
    protocol::DepositAsset,
    services::asset_registry::{
        AssetRegistry, MarketOrderTransitionKind, ProviderId, RequiredCustodyRole, TransitionDecl,
    },
    services::quote_legs::QuoteLeg,
};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use snafu::Snafu;
use std::{collections::HashMap, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum MarketOrderRoutePlanError {
    #[snafu(display("market order route planning requires a market-order action"))]
    NonMarketOrderAction,

    #[snafu(display("limit order route planning requires a limit-order action"))]
    NonLimitOrderAction,

    #[snafu(display("quote {} does not belong to order {}", quote_order_id, order_id))]
    QuoteOrderMismatch {
        quote_order_id: Uuid,
        order_id: Uuid,
    },

    #[snafu(display("vault {} is not attached to order {}", vault_id, order_id))]
    VaultOrderMismatch { vault_id: Uuid, order_id: Uuid },

    #[snafu(display("order {} is not funded by vault {}", order_id, vault_id))]
    FundingVaultMismatch { order_id: Uuid, vault_id: Uuid },

    #[snafu(display("vault source asset does not match order source asset"))]
    SourceAssetMismatch,

    #[snafu(display("unsupported market-order route: {}", reason))]
    UnsupportedRoute { reason: String },

    #[snafu(display(
        "execution step {} ({}) violates the intermediate custody invariant: {}",
        step_index,
        step_type,
        reason
    ))]
    IntermediateCustodyInvariant {
        step_index: i32,
        step_type: &'static str,
        reason: String,
    },

    #[snafu(display(
        "execution step {} ({}) violates the materialization invariant: {}",
        step_index,
        step_type,
        reason
    ))]
    StepMaterializationInvariant {
        step_index: i32,
        step_type: &'static str,
        reason: String,
    },
}

pub type MarketOrderRoutePlanResult<T> = Result<T, MarketOrderRoutePlanError>;

#[derive(Debug, Clone)]
pub struct MarketOrderRoutePlan {
    pub path_id: String,
    pub transition_decl_ids: Vec<String>,
    pub legs: Vec<OrderExecutionLeg>,
    pub steps: Vec<OrderExecutionStep>,
}

#[derive(Debug, Clone)]
pub struct MarketOrderRoutePlanner {
    asset_registry: Arc<AssetRegistry>,
}

#[derive(Debug, Clone)]
struct QuotedTransitionPath {
    path_id: String,
    transitions: Vec<TransitionDecl>,
}

impl Default for MarketOrderRoutePlanner {
    fn default() -> Self {
        Self::new(Arc::new(AssetRegistry::default()))
    }
}

impl MarketOrderRoutePlanner {
    #[must_use]
    pub fn new(asset_registry: Arc<AssetRegistry>) -> Self {
        Self { asset_registry }
    }

    pub fn plan(
        &self,
        order: &RouterOrder,
        source_vault: &DepositVault,
        quote: &MarketOrderQuote,
        planned_at: DateTime<Utc>,
    ) -> MarketOrderRoutePlanResult<MarketOrderRoutePlan> {
        self.validate_inputs(order, source_vault, quote)?;

        let path = self.resolve_quoted_transition_path(order, quote)?;
        let materialized =
            materialize_transition_steps(order, source_vault, quote, &path, planned_at)?;
        validate_leg_materialization(&materialized.legs)?;
        validate_step_materialization(&materialized.steps)?;
        validate_intermediate_custody_plan(&materialized.steps)?;

        Ok(MarketOrderRoutePlan {
            path_id: path.path_id,
            transition_decl_ids: path
                .transitions
                .iter()
                .map(|transition| transition.id.clone())
                .collect(),
            legs: materialized.legs,
            steps: materialized.steps,
        })
    }

    pub fn plan_limit_order(
        &self,
        order: &RouterOrder,
        source_vault: &DepositVault,
        quote: &LimitOrderQuote,
        planned_at: DateTime<Utc>,
    ) -> MarketOrderRoutePlanResult<MarketOrderRoutePlan> {
        self.validate_limit_order_inputs(order, source_vault, quote)?;
        let materialization_quote = MarketOrderQuote {
            id: quote.id,
            order_id: quote.order_id,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address: quote.recipient_address.clone(),
            provider_id: quote.provider_id.clone(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: quote.input_amount.clone(),
            amount_out: quote.output_amount.clone(),
            min_amount_out: Some(quote.output_amount.clone()),
            max_amount_in: None,
            slippage_bps: 0,
            provider_quote: quote.provider_quote.clone(),
            usd_valuation: json!({}),
            expires_at: quote.expires_at,
            created_at: quote.created_at,
        };
        let path = self.resolve_quoted_transition_path(order, &materialization_quote)?;
        let materialized = materialize_transition_steps(
            order,
            source_vault,
            &materialization_quote,
            &path,
            planned_at,
        )?;
        validate_leg_materialization(&materialized.legs)?;
        validate_step_materialization(&materialized.steps)?;
        validate_intermediate_custody_plan(&materialized.steps)?;

        Ok(MarketOrderRoutePlan {
            path_id: path.path_id,
            transition_decl_ids: path
                .transitions
                .iter()
                .map(|transition| transition.id.clone())
                .collect(),
            legs: materialized.legs,
            steps: materialized.steps,
        })
    }

    fn validate_inputs(
        &self,
        order: &RouterOrder,
        source_vault: &DepositVault,
        quote: &MarketOrderQuote,
    ) -> MarketOrderRoutePlanResult<()> {
        if !matches!(order.action, RouterOrderAction::MarketOrder(_)) {
            return Err(MarketOrderRoutePlanError::NonMarketOrderAction);
        }
        if quote.order_id != Some(order.id) {
            return Err(MarketOrderRoutePlanError::QuoteOrderMismatch {
                quote_order_id: quote.order_id.unwrap_or_default(),
                order_id: order.id,
            });
        }
        if source_vault.order_id != Some(order.id) {
            return Err(MarketOrderRoutePlanError::VaultOrderMismatch {
                vault_id: source_vault.id,
                order_id: order.id,
            });
        }
        if order.funding_vault_id != Some(source_vault.id) {
            return Err(MarketOrderRoutePlanError::FundingVaultMismatch {
                order_id: order.id,
                vault_id: source_vault.id,
            });
        }
        if source_vault.deposit_asset != order.source_asset {
            return Err(MarketOrderRoutePlanError::SourceAssetMismatch);
        }

        Ok(())
    }

    fn validate_limit_order_inputs(
        &self,
        order: &RouterOrder,
        source_vault: &DepositVault,
        quote: &LimitOrderQuote,
    ) -> MarketOrderRoutePlanResult<()> {
        if !matches!(order.action, RouterOrderAction::LimitOrder(_)) {
            return Err(MarketOrderRoutePlanError::NonLimitOrderAction);
        }
        if quote.order_id != Some(order.id) {
            return Err(MarketOrderRoutePlanError::QuoteOrderMismatch {
                quote_order_id: quote.order_id.unwrap_or_default(),
                order_id: order.id,
            });
        }
        if source_vault.order_id != Some(order.id) {
            return Err(MarketOrderRoutePlanError::VaultOrderMismatch {
                vault_id: source_vault.id,
                order_id: order.id,
            });
        }
        if order.funding_vault_id != Some(source_vault.id) {
            return Err(MarketOrderRoutePlanError::FundingVaultMismatch {
                order_id: order.id,
                vault_id: source_vault.id,
            });
        }
        if source_vault.deposit_asset != order.source_asset {
            return Err(MarketOrderRoutePlanError::SourceAssetMismatch);
        }

        Ok(())
    }

    fn resolve_quoted_transition_path(
        &self,
        order: &RouterOrder,
        quote: &MarketOrderQuote,
    ) -> MarketOrderRoutePlanResult<QuotedTransitionPath> {
        let transition_ids: Vec<String> = quote
            .provider_quote
            .get("transition_decl_ids")
            .and_then(Value::as_array)
            .ok_or_else(|| MarketOrderRoutePlanError::UnsupportedRoute {
                reason: "market order quote is missing provider_quote.transition_decl_ids"
                    .to_string(),
            })?
            .iter()
            .filter_map(|value| value.as_str().map(ToString::to_string))
            .collect();
        if transition_ids.is_empty() {
            return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                reason: "market order quote contains no transition declarations".to_string(),
            });
        }

        let transitions_by_id: HashMap<_, _> = self
            .asset_registry
            .transition_declarations()
            .into_iter()
            .map(|transition| (transition.id.clone(), transition))
            .collect();
        let mut transitions_by_id = transitions_by_id;
        if let Some(serialized_transitions) = quote
            .provider_quote
            .get("transitions")
            .and_then(Value::as_array)
        {
            for value in serialized_transitions {
                let transition: TransitionDecl = serde_json::from_value(value.clone()).map_err(
                    |err| MarketOrderRoutePlanError::UnsupportedRoute {
                        reason: format!(
                            "market order quote contains invalid provider_quote.transitions entry: {err}"
                        ),
                    },
                )?;
                transitions_by_id.insert(transition.id.clone(), transition);
            }
        }
        let transitions: Vec<TransitionDecl> = transition_ids
            .iter()
            .map(|id| {
                transitions_by_id.get(id).cloned().ok_or_else(|| {
                    MarketOrderRoutePlanError::UnsupportedRoute {
                        reason: format!("quoted transition declaration {id:?} is not registered"),
                    }
                })
            })
            .collect::<Result<_, _>>()?;

        let first =
            transitions
                .first()
                .ok_or_else(|| MarketOrderRoutePlanError::UnsupportedRoute {
                    reason: "quoted transition path is empty".to_string(),
                })?;
        let last =
            transitions
                .last()
                .ok_or_else(|| MarketOrderRoutePlanError::UnsupportedRoute {
                    reason: "quoted transition path is empty".to_string(),
                })?;
        if first.input.asset != order.source_asset {
            return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                reason: format!(
                    "quoted transition path starts at {} {} instead of order source {} {}",
                    first.input.asset.chain,
                    first.input.asset.asset,
                    order.source_asset.chain,
                    order.source_asset.asset
                ),
            });
        }
        if last.output.asset != order.destination_asset {
            return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                reason: format!(
                    "quoted transition path ends at {} {} instead of order destination {} {}",
                    last.output.asset.chain,
                    last.output.asset.asset,
                    order.destination_asset.chain,
                    order.destination_asset.asset
                ),
            });
        }

        let path_id = quote
            .provider_quote
            .get("path_id")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .unwrap_or_else(|| transition_ids.join("|"));
        Ok(QuotedTransitionPath {
            path_id,
            transitions,
        })
    }
}

fn materialize_transition_steps(
    order: &RouterOrder,
    source_vault: &DepositVault,
    quote: &MarketOrderQuote,
    path: &QuotedTransitionPath,
    planned_at: DateTime<Utc>,
) -> MarketOrderRoutePlanResult<MaterializedRoutePlan> {
    let mut legs = QuoteLegIndex::from_quote(quote)?;
    let mut execution_legs = Vec::new();
    let mut steps = Vec::new();
    let mut step_index = 1;
    let mut leg_index = 0;

    for (transition_index, transition) in path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == path.transitions.len();
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let transition_steps = vec![across_bridge_step(AcrossBridgeStepSpec {
                    order,
                    source_vault,
                    quote,
                    transition,
                    leg: &leg,
                    source_role: input_custody_role_for_transition(
                        &path.transitions,
                        transition_index,
                    ),
                    is_final,
                    step_index,
                    planned_at,
                })?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
            MarketOrderTransitionKind::CctpBridge => {
                let mut cctp_legs = legs.take_all(&transition.id);
                let burn_leg = take_execution_leg(
                    &mut cctp_legs,
                    &transition.id,
                    transition.kind,
                    OrderExecutionStepType::CctpBurn,
                )?;
                let receive_leg = take_execution_leg(
                    &mut cctp_legs,
                    &transition.id,
                    transition.kind,
                    OrderExecutionStepType::CctpReceive,
                )?;
                if !cctp_legs.is_empty() {
                    return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                        reason: format!(
                            "quoted transition {} ({}) has {} unexpected CCTP legs",
                            transition.id,
                            transition.kind.as_str(),
                            cctp_legs.len()
                        ),
                    });
                }
                let cctp_steps = cctp_bridge_steps(CctpBridgeStepSpec {
                    order,
                    source_vault,
                    transition,
                    burn_leg: &burn_leg,
                    receive_leg: &receive_leg,
                    source_role: input_custody_role_for_transition(
                        &path.transitions,
                        transition_index,
                    ),
                    is_final,
                    step_index,
                    planned_at,
                })?;
                let cctp_step_count = cctp_steps.len();
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[burn_leg, receive_leg],
                        transition_steps: cctp_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += i32::try_from(cctp_step_count).unwrap_or(0);
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let transition_steps = vec![unit_deposit_step(
                    order,
                    source_vault,
                    transition,
                    &leg,
                    input_custody_role_for_transition(&path.transitions, transition_index),
                    step_index,
                    planned_at,
                )?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
            MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let transition_steps = vec![hyperliquid_bridge_deposit_step(
                    order,
                    source_vault,
                    transition,
                    &leg,
                    input_custody_role_for_transition(&path.transitions, transition_index),
                    step_index,
                    planned_at,
                )?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let custody =
                    hyperliquid_custody_for_withdrawal(&path.transitions, transition_index)?;
                let transition_steps = vec![hyperliquid_bridge_withdrawal_step(
                    HyperliquidBridgeWithdrawalStepSpec {
                        order,
                        quote,
                        transition,
                        leg: &leg,
                        hyperliquid_custody: custody,
                        is_final,
                        step_index,
                        planned_at,
                    },
                )?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
            MarketOrderTransitionKind::HyperliquidTrade => {
                let transition_legs = legs.take_all(&transition.id);
                if transition_legs.is_empty() {
                    return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                        reason: format!(
                            "quoted path is missing hyperliquid trade legs for transition {}",
                            transition.id
                        ),
                    });
                }
                let custody = hyperliquid_custody_for_transition(
                    &path.transitions,
                    transition_index,
                    transition_legs.len(),
                )?;
                let mut transition_steps = Vec::with_capacity(transition_legs.len());
                for (leg_index, leg) in transition_legs.iter().enumerate() {
                    let hyperliquid_custody = HyperliquidCustodySpec {
                        role: custody.role,
                        asset: custody.asset.clone(),
                        prefund_from_withdrawable: custody.prefund_first_trade && leg_index == 0,
                    };
                    let materialized_step = if leg.raw.get("kind").and_then(Value::as_str)
                        == Some("hyperliquid_limit_order")
                    {
                        if transition_legs.len() != 1 {
                            return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                                reason: "limit-order transition must materialize exactly one Hyperliquid leg"
                                    .to_string(),
                            });
                        }
                        hyperliquid_limit_order_step(HyperliquidLimitOrderStepSpec {
                            order,
                            quote,
                            exchange_provider: leg_provider(leg, transition)?,
                            hyperliquid_custody,
                            leg,
                            transition_decl_id: Some(transition.id.clone()),
                            step_index,
                            planned_at,
                        })?
                    } else {
                        hyperliquid_trade_step(HyperliquidTradeStepSpec {
                            order,
                            quote,
                            exchange_provider: leg_provider(leg, transition)?,
                            hyperliquid_custody,
                            leg_index,
                            leg_count: transition_legs.len(),
                            leg,
                            transition_decl_id: Some(transition.id.clone()),
                            step_index,
                            planned_at,
                        })?
                    };
                    transition_steps.push(materialized_step);
                    step_index += 1;
                }
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &transition_legs,
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let transition_steps =
                    vec![universal_router_swap_step(UniversalRouterSwapStepSpec {
                        order,
                        source_vault,
                        quote,
                        transition,
                        leg: &leg,
                        source_role: input_custody_role_for_transition(
                            &path.transitions,
                            transition_index,
                        ),
                        is_final,
                        step_index,
                        planned_at,
                    })?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let leg = legs.take_one(&transition.id, transition.kind)?;
                let custody =
                    hyperliquid_custody_for_withdrawal(&path.transitions, transition_index)?;
                let transition_steps = vec![unit_withdrawal_step(UnitWithdrawalStepSpec {
                    order,
                    quote,
                    leg: &leg,
                    unit_provider: leg_provider(&leg, transition)?,
                    hyperliquid_custody: custody,
                    transition_decl_id: Some(transition.id.clone()),
                    is_final,
                    step_index,
                    planned_at,
                })?];
                push_execution_leg(
                    &mut execution_legs,
                    &mut steps,
                    ExecutionLegMaterializationSpec {
                        order,
                        quote,
                        transition,
                        quote_legs: &[leg],
                        transition_steps,
                        leg_index,
                        planned_at,
                    },
                )?;
                leg_index += 1;
                step_index += 1;
            }
        }
    }

    attach_quote_gas_plan_to_steps(&mut steps, quote);
    Ok(MaterializedRoutePlan {
        legs: execution_legs,
        steps,
    })
}

struct MaterializedRoutePlan {
    legs: Vec<OrderExecutionLeg>,
    steps: Vec<OrderExecutionStep>,
}

struct ExecutionLegMaterializationSpec<'a> {
    order: &'a RouterOrder,
    quote: &'a MarketOrderQuote,
    transition: &'a TransitionDecl,
    quote_legs: &'a [QuoteLeg],
    transition_steps: Vec<OrderExecutionStep>,
    leg_index: i32,
    planned_at: DateTime<Utc>,
}

fn push_execution_leg(
    execution_legs: &mut Vec<OrderExecutionLeg>,
    steps: &mut Vec<OrderExecutionStep>,
    spec: ExecutionLegMaterializationSpec<'_>,
) -> MarketOrderRoutePlanResult<()> {
    let leg = execution_leg_from_quote_legs(
        spec.order,
        spec.quote,
        spec.transition,
        spec.quote_legs,
        spec.leg_index,
        spec.planned_at,
    )?;
    let leg_id = leg.id;
    execution_legs.push(leg);
    steps.extend(spec.transition_steps.into_iter().map(|mut step| {
        step.execution_leg_id = Some(leg_id);
        step
    }));
    Ok(())
}

fn execution_leg_from_quote_legs(
    order: &RouterOrder,
    quote: &MarketOrderQuote,
    transition: &TransitionDecl,
    quote_legs: &[QuoteLeg],
    leg_index: i32,
    planned_at: DateTime<Utc>,
) -> MarketOrderRoutePlanResult<OrderExecutionLeg> {
    let first = quote_legs
        .first()
        .ok_or_else(|| MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "transition {} ({}) has no quoted legs to materialize",
                transition.id,
                transition.kind.as_str()
            ),
        })?;
    let last = quote_legs.last().expect("non-empty quote legs");
    let input_asset = first.input_deposit_asset().map_err(|reason| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "transition {} ({}) has invalid input asset: {reason}",
                transition.id,
                transition.kind.as_str()
            ),
        }
    })?;
    let output_asset = last.output_deposit_asset().map_err(|reason| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "transition {} ({}) has invalid output asset: {reason}",
                transition.id,
                transition.kind.as_str()
            ),
        }
    })?;
    let provider = quote_legs
        .iter()
        .map(|leg| leg.provider.as_str())
        .reduce(|first, next| if first == next { first } else { "multi" })
        .unwrap_or("unknown")
        .to_string();
    let min_amount_out = last
        .raw
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            if output_asset == order.destination_asset {
                match quote.order_kind {
                    MarketOrderKindType::ExactIn => quote.min_amount_out.clone(),
                    MarketOrderKindType::ExactOut => Some(quote.amount_out.clone()),
                }
            } else {
                None
            }
        });

    Ok(OrderExecutionLeg {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: None,
        transition_decl_id: Some(transition.id.clone()),
        leg_index,
        leg_type: transition.kind.as_str().to_string(),
        provider,
        status: OrderExecutionStepStatus::Planned,
        input_asset,
        output_asset,
        amount_in: first.amount_in.clone(),
        expected_amount_out: last.amount_out.clone(),
        min_amount_out,
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "quote_leg_count": quote_legs.len(),
            "quote_leg_transition_decl_ids": quote_legs
                .iter()
                .map(|leg| leg.transition_decl_id.clone())
                .collect::<Vec<_>>(),
            "action_step_types": quote_legs
                .iter()
                .map(|leg| leg.execution_step_type.to_db_string())
                .collect::<Vec<_>>(),
        }),
        usd_valuation: json!({}),
        created_at: planned_at,
        updated_at: planned_at,
    })
}

struct UnitWithdrawalStepSpec<'a> {
    order: &'a RouterOrder,
    quote: &'a MarketOrderQuote,
    leg: &'a QuoteLeg,
    unit_provider: ProviderId,
    hyperliquid_custody: HyperliquidCustodySpec,
    transition_decl_id: Option<String>,
    is_final: bool,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn unit_withdrawal_step(
    spec: UnitWithdrawalStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let UnitWithdrawalStepSpec {
        order,
        quote,
        leg,
        unit_provider,
        hyperliquid_custody,
        transition_decl_id,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let output_asset = leg.output_deposit_asset().map_err(|reason| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "unit withdrawal leg {} has invalid output asset: {reason}",
                leg.transition_decl_id
            ),
        }
    })?;
    let input_asset = leg.input_deposit_asset().map_err(|reason| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "unit withdrawal leg {} has invalid input asset: {reason}",
                leg.transition_decl_id
            ),
        }
    })?;
    let min_amount_out = if output_asset == order.destination_asset {
        match quote.order_kind {
            MarketOrderKindType::ExactIn => quote.min_amount_out.clone(),
            MarketOrderKindType::ExactOut => Some(quote.amount_out.clone()),
        }
    } else {
        Some(leg.amount_out.clone())
    };
    let recipient_address = leg
        .raw
        .get("recipient_address")
        .and_then(Value::as_str)
        .unwrap_or(&order.recipient_address)
        .to_string();
    let amount_in = leg.amount_in.clone();
    let amount_out = leg.amount_out.clone();
    let provider = unit_provider.as_str();
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };

    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id,
        step_index,
        step_type: OrderExecutionStepType::UnitWithdrawal,
        provider: provider.to_string(),
        input_asset: Some(input_asset.clone()),
        output_asset: Some(output_asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, provider, step_index),
        // The HyperUnitProvider will resolve `dst_chain_id`/`asset_id` to the
        // HyperUnit wire chain/asset (bitcoin/ethereum, btc/eth) and call
        // `GET /gen` to obtain the protocol_address. The hydrator fills in
        // `hyperliquid_custody_vault_id`/`hyperliquid_custody_vault_address`
        // from the router's HL spot vault — that vault is what signs the
        // spotSend custody action to the protocol_address.
        request: json!({
            "order_id": order.id,
            "input_chain_id": input_asset.chain.as_str(),
            "input_asset": input_asset.asset.as_str(),
            "dst_chain_id": output_asset.chain.as_str(),
            "asset_id": output_asset.asset.as_str(),
            "amount": amount_out,
            "recipient_address": recipient_address,
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": recipient_role,
            "min_amount_out": min_amount_out,
            "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_address": null,
            "hyperliquid_custody_vault_chain_id": hyperliquid_custody
                .asset
                .as_ref()
                .map(|asset| asset.chain.as_str()),
            "hyperliquid_custody_vault_asset_id": hyperliquid_custody
                .asset
                .as_ref()
                .map(|asset| asset.asset.as_str()),
        }),
        details: json!({
            "schema_version": 1,
            "recipient_address": &order.recipient_address,
            "destination_asset": {
                "chain": order.destination_asset.chain.as_str(),
                "asset": order.destination_asset.asset.as_str()
            },
            "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
            "hyperliquid_custody_vault_status": "pending_derivation",
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
        }),
        planned_at,
    }))
}

struct StepSpec {
    order_id: Uuid,
    transition_decl_id: Option<String>,
    step_index: i32,
    step_type: OrderExecutionStepType,
    provider: String,
    input_asset: Option<DepositAsset>,
    output_asset: Option<DepositAsset>,
    amount_in: Option<String>,
    min_amount_out: Option<String>,
    provider_ref: Option<String>,
    idempotency_key: Option<String>,
    request: serde_json::Value,
    details: serde_json::Value,
    planned_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct HyperliquidCustodySpec {
    role: CustodyVaultRole,
    asset: Option<DepositAsset>,
    prefund_from_withdrawable: bool,
}

#[derive(Debug, Clone)]
struct QuoteLegIndex {
    by_transition_id: HashMap<String, Vec<QuoteLeg>>,
}

impl QuoteLegIndex {
    fn from_quote(quote: &MarketOrderQuote) -> MarketOrderRoutePlanResult<Self> {
        let mut by_transition_id: HashMap<String, Vec<QuoteLeg>> = HashMap::new();
        for leg in quote
            .provider_quote
            .get("legs")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let leg: QuoteLeg = serde_json::from_value(leg.clone()).map_err(|err| {
                MarketOrderRoutePlanError::UnsupportedRoute {
                    reason: format!("quoted leg is invalid for typed route materialization: {err}"),
                }
            })?;
            let transition_id = leg.parent_transition_id().to_string();
            by_transition_id.entry(transition_id).or_default().push(leg);
        }
        Ok(Self { by_transition_id })
    }

    fn take_one(
        &mut self,
        transition_id: &str,
        kind: MarketOrderTransitionKind,
    ) -> MarketOrderRoutePlanResult<QuoteLeg> {
        let mut legs = self.take_all(transition_id);
        if legs.len() != 1 {
            return Err(MarketOrderRoutePlanError::UnsupportedRoute {
                reason: format!(
                    "quoted transition {transition_id} ({}) expected exactly 1 leg, got {}",
                    kind.as_str(),
                    legs.len()
                ),
            });
        }
        Ok(legs.remove(0))
    }

    fn take_all(&mut self, transition_id: &str) -> Vec<QuoteLeg> {
        self.by_transition_id
            .remove(transition_id)
            .unwrap_or_default()
    }
}

impl HyperliquidCustodySpec {
    fn spot_vault() -> Self {
        Self {
            role: CustodyVaultRole::HyperliquidSpot,
            asset: None,
            prefund_from_withdrawable: false,
        }
    }
}

fn take_execution_leg(
    legs: &mut Vec<QuoteLeg>,
    transition_id: &str,
    kind: MarketOrderTransitionKind,
    execution_step_type: OrderExecutionStepType,
) -> MarketOrderRoutePlanResult<QuoteLeg> {
    let Some(index) = legs
        .iter()
        .position(|leg| leg.execution_step_type == execution_step_type)
    else {
        return Err(MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "quoted transition {transition_id} ({}) is missing {} leg",
                kind.as_str(),
                execution_step_type.to_db_string()
            ),
        });
    };
    Ok(legs.remove(index))
}

fn step(spec: StepSpec) -> OrderExecutionStep {
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: spec.order_id,
        execution_attempt_id: None,
        execution_leg_id: None,
        transition_decl_id: spec.transition_decl_id,
        step_index: spec.step_index,
        step_type: spec.step_type,
        provider: spec.provider,
        status: OrderExecutionStepStatus::Planned,
        input_asset: spec.input_asset,
        output_asset: spec.output_asset,
        amount_in: spec.amount_in,
        min_amount_out: spec.min_amount_out,
        tx_hash: None,
        provider_ref: spec.provider_ref,
        idempotency_key: spec.idempotency_key,
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: spec.details,
        request: spec.request,
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: spec.planned_at,
        updated_at: spec.planned_at,
    }
}

fn idempotency_key(order_id: Uuid, provider: &str, step_index: i32) -> Option<String> {
    Some(format!("order:{order_id}:{provider}:{step_index}"))
}

fn attach_quote_gas_plan_to_steps(steps: &mut [OrderExecutionStep], quote: &MarketOrderQuote) {
    let Some(retention_actions) = quote
        .provider_quote
        .get("gas_reimbursement")
        .and_then(|value| value.get("retention_actions"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for step in steps {
        let Some(transition_decl_id) = step.transition_decl_id.as_deref() else {
            continue;
        };
        let step_actions: Vec<Value> = retention_actions
            .iter()
            .filter(|action| {
                action.get("transition_decl_id").and_then(Value::as_str) == Some(transition_decl_id)
            })
            .cloned()
            .collect();
        if step_actions.is_empty() {
            continue;
        }
        set_object_field(
            &mut step.request,
            "retention_actions",
            Value::Array(step_actions.clone()),
        );
        set_object_field(
            &mut step.details,
            "retention_actions",
            Value::Array(step_actions),
        );
    }
}

fn set_object_field(object: &mut Value, key: &'static str, value: Value) {
    if let Some(map) = object.as_object_mut() {
        map.insert(key.to_string(), value);
    }
}

struct UnitDepositRequestSpec<'a> {
    order_id: Uuid,
    unit_ingress_asset: &'a DepositAsset,
    unit_output_asset: &'a DepositAsset,
    expected_amount: String,
    source_custody_vault_id: Option<Uuid>,
    source_custody_vault_role: Option<String>,
    revert_custody_vault_id: Option<Uuid>,
    revert_custody_vault_role: Option<String>,
}

/// The HyperUnitProvider resolves `src_chain_id`/`asset_id` to the HyperUnit
/// wire chain/asset (bitcoin/ethereum, btc/eth) and calls `GET /gen` with the
/// HL spot vault address as `dst_addr` to obtain the protocol_address. The
/// hydrator fills in `hyperliquid_custody_vault_id`/`hyperliquid_custody_vault_address`
/// from the router's HL spot vault — funds land there as UBTC/UETH once the
/// source chain send to the protocol_address is finalized.
fn unit_deposit_request(spec: UnitDepositRequestSpec<'_>) -> serde_json::Value {
    json!({
        "order_id": spec.order_id,
        "src_chain_id": spec.unit_ingress_asset.chain.as_str(),
        "dst_chain_id": "hyperliquid",
        "asset_id": spec.unit_ingress_asset.asset.as_str(),
        "output_chain_id": spec.unit_output_asset.chain.as_str(),
        "output_asset": spec.unit_output_asset.asset.as_str(),
        "amount": spec.expected_amount,
        "source_custody_vault_id": spec.source_custody_vault_id,
        "source_custody_vault_role": spec.source_custody_vault_role,
        "revert_custody_vault_id": spec.revert_custody_vault_id,
        "revert_custody_vault_role": spec.revert_custody_vault_role,
        "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
        "hyperliquid_custody_vault_id": null,
        "hyperliquid_custody_vault_address": null
    })
}

struct HyperliquidTradeStepSpec<'a> {
    order: &'a RouterOrder,
    quote: &'a MarketOrderQuote,
    exchange_provider: ProviderId,
    hyperliquid_custody: HyperliquidCustodySpec,
    leg_index: usize,
    leg_count: usize,
    leg: &'a QuoteLeg,
    transition_decl_id: Option<String>,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn hyperliquid_trade_step(
    spec: HyperliquidTradeStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let HyperliquidTradeStepSpec {
        order,
        quote,
        exchange_provider,
        hyperliquid_custody,
        leg_index,
        leg_count,
        leg,
        transition_decl_id,
        step_index,
        planned_at,
    } = spec;
    let provider = exchange_provider.as_str();
    let trade_leg = &leg.raw;
    let order_kind = required_str(trade_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let min_amount_out = trade_leg
        .get("min_amount_out")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let max_amount_in = trade_leg
        .get("max_amount_in")
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    let input_asset = leg
        .input_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;

    let request = json!({
        "order_id": order.id,
        "quote_id": quote.id,
        "leg_index": leg_index,
        "leg_count": leg_count,
        "order_kind": order_kind,
        "amount_in": amount_in,
        "amount_out": amount_out,
        "min_amount_out": min_amount_out,
        "max_amount_in": max_amount_in,
        "input_asset": {
            "chain_id": input_asset.chain.as_str(),
            "asset": input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": output_asset.chain.as_str(),
            "asset": output_asset.asset.as_str(),
        },
        "prefund_from_withdrawable": hyperliquid_custody.prefund_from_withdrawable,
        "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
        "hyperliquid_custody_vault_id": null,
        "hyperliquid_custody_vault_address": null,
        "hyperliquid_custody_vault_chain_id": hyperliquid_custody
            .asset
            .as_ref()
            .map(|asset| asset.chain.as_str()),
        "hyperliquid_custody_vault_asset_id": hyperliquid_custody
            .asset
            .as_ref()
            .map(|asset| asset.asset.as_str()),
    });
    let details = json!({
        "schema_version": 1,
        "leg_index": leg_index,
        "leg_count": leg_count,
        "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
        "hyperliquid_custody_vault_status": "pending_derivation",
    });

    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id,
        step_index,
        step_type: OrderExecutionStepType::HyperliquidTrade,
        provider: provider.to_string(),
        input_asset: Some(input_asset),
        output_asset: Some(output_asset),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, provider, step_index),
        request,
        details,
        planned_at,
    }))
}

struct HyperliquidLimitOrderStepSpec<'a> {
    order: &'a RouterOrder,
    quote: &'a MarketOrderQuote,
    exchange_provider: ProviderId,
    hyperliquid_custody: HyperliquidCustodySpec,
    leg: &'a QuoteLeg,
    transition_decl_id: Option<String>,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn hyperliquid_limit_order_step(
    spec: HyperliquidLimitOrderStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let HyperliquidLimitOrderStepSpec {
        order,
        quote,
        exchange_provider,
        hyperliquid_custody,
        leg,
        transition_decl_id,
        step_index,
        planned_at,
    } = spec;
    let provider = exchange_provider.as_str();
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let input_asset = leg
        .input_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;
    let residual_policy = leg
        .raw
        .get("residual_policy")
        .and_then(Value::as_str)
        .unwrap_or("refund");

    let request = json!({
        "order_id": order.id,
        "quote_id": quote.id,
        "amount_in": amount_in,
        "amount_out": amount_out,
        "residual_policy": residual_policy,
        "input_asset": {
            "chain_id": input_asset.chain.as_str(),
            "asset": input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": output_asset.chain.as_str(),
            "asset": output_asset.asset.as_str(),
        },
        "prefund_from_withdrawable": hyperliquid_custody.prefund_from_withdrawable,
        "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
        "hyperliquid_custody_vault_id": null,
        "hyperliquid_custody_vault_address": null,
        "hyperliquid_custody_vault_chain_id": hyperliquid_custody
            .asset
            .as_ref()
            .map(|asset| asset.chain.as_str()),
        "hyperliquid_custody_vault_asset_id": hyperliquid_custody
            .asset
            .as_ref()
            .map(|asset| asset.asset.as_str()),
    });
    let details = json!({
        "schema_version": 1,
        "hyperliquid_custody_vault_role": hyperliquid_custody.role.to_db_string(),
        "hyperliquid_custody_vault_status": "pending_derivation",
        "residual_policy": residual_policy,
    });

    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id,
        step_index,
        step_type: OrderExecutionStepType::HyperliquidLimitOrder,
        provider: provider.to_string(),
        input_asset: Some(input_asset),
        output_asset: Some(output_asset),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: Some(amount_out.to_string()),
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, provider, step_index),
        request,
        details,
        planned_at,
    }))
}

struct UniversalRouterSwapStepSpec<'a> {
    order: &'a RouterOrder,
    source_vault: &'a DepositVault,
    quote: &'a MarketOrderQuote,
    transition: &'a TransitionDecl,
    leg: &'a QuoteLeg,
    source_role: Option<CustodyVaultRole>,
    is_final: bool,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn universal_router_swap_step(
    spec: UniversalRouterSwapStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let UniversalRouterSwapStepSpec {
        order,
        source_vault,
        quote,
        transition,
        leg,
        source_role,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let provider_id = leg_provider(leg, transition)?;
    let provider = provider_id.as_str().to_string();
    let swap_leg = &leg.raw;
    let order_kind = required_str(swap_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let input_asset = leg
        .input_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(|reason| MarketOrderRoutePlanError::UnsupportedRoute { reason })?;
    let input_decimals = required_u8(swap_leg, "src_decimals")?;
    let output_decimals = required_u8(swap_leg, "dest_decimals")?;
    let price_route = swap_leg.get("price_route").cloned().ok_or_else(|| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: "universal router swap leg missing price_route".to_string(),
        }
    })?;
    let min_amount_out = swap_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = swap_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let slippage_bps = swap_leg.get("slippage_bps").and_then(Value::as_u64);
    let source_role_string = source_role.map(|role| role.to_db_string().to_string());
    let (source_custody_vault_id, source_custody_vault_address) = if source_role.is_none() {
        (
            json!(source_vault.id),
            json!(source_vault.deposit_vault_address.clone()),
        )
    } else {
        (Value::Null, Value::Null)
    };
    let recipient_address = if is_final {
        json!(order.recipient_address)
    } else {
        Value::Null
    };
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };

    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UniversalRouterSwap,
        provider: provider.clone(),
        input_asset: Some(input_asset.clone()),
        output_asset: Some(output_asset.clone()),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: json!({
            "order_id": order.id,
            "quote_id": quote.id,
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": input_asset.chain.as_str(),
                "asset": input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": output_asset.chain.as_str(),
                "asset": output_asset.asset.as_str(),
            },
            "input_decimals": input_decimals,
            "output_decimals": output_decimals,
            "price_route": price_route,
            "slippage_bps": slippage_bps,
            "source_custody_vault_id": source_custody_vault_id,
            "source_custody_vault_address": source_custody_vault_address,
            "source_custody_vault_role": source_role_string,
            "recipient_address": recipient_address,
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": recipient_role,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "source_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "source_custody_vault_status": if source_role.is_some() { json!("pending_derivation") } else { json!("bound") },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
        }),
        planned_at,
    }))
}

fn required_u8(obj: &Value, key: &'static str) -> MarketOrderRoutePlanResult<u8> {
    let value = obj.get(key).and_then(Value::as_u64).ok_or_else(|| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!("universal router swap leg missing numeric field {key}"),
        }
    })?;
    u8::try_from(value).map_err(|err| MarketOrderRoutePlanError::UnsupportedRoute {
        reason: format!("universal router swap leg field {key} does not fit u8: {err}"),
    })
}

fn required_str<'a>(obj: &'a Value, key: &'static str) -> MarketOrderRoutePlanResult<&'a str> {
    obj.get(key).and_then(Value::as_str).ok_or_else(|| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!("hyperliquid leg missing string field {key}"),
        }
    })
}

fn leg_provider(
    leg: &QuoteLeg,
    transition: &TransitionDecl,
) -> MarketOrderRoutePlanResult<ProviderId> {
    if leg.provider != transition.provider {
        return Err(MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "quote leg provider {} does not match transition provider {}",
                leg.provider.as_str(),
                transition.provider.as_str()
            ),
        });
    }
    Ok(leg.provider)
}

fn input_custody_role_for_transition(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> Option<CustodyVaultRole> {
    if transition_index == 0 {
        return None;
    }
    let previous_role = transitions[transition_index - 1]
        .output
        .required_custody_role;
    if previous_role == RequiredCustodyRole::DestinationPayout {
        return Some(CustodyVaultRole::DestinationExecution);
    }
    required_role_to_custody(previous_role)
}

fn required_role_to_custody(role: RequiredCustodyRole) -> Option<CustodyVaultRole> {
    match role {
        RequiredCustodyRole::SourceOrIntermediate => None,
        RequiredCustodyRole::IntermediateExecution => Some(CustodyVaultRole::DestinationExecution),
        RequiredCustodyRole::HyperliquidSpot => Some(CustodyVaultRole::HyperliquidSpot),
        RequiredCustodyRole::DestinationPayout => None,
    }
}

#[derive(Debug, Clone)]
struct DerivedHyperliquidCustody {
    role: CustodyVaultRole,
    asset: Option<DepositAsset>,
    prefund_first_trade: bool,
}

fn hyperliquid_custody_for_transition(
    transitions: &[TransitionDecl],
    transition_index: usize,
    _leg_count: usize,
) -> MarketOrderRoutePlanResult<DerivedHyperliquidCustody> {
    let prior_transitions = &transitions[..transition_index];
    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
    {
        return Ok(DerivedHyperliquidCustody {
            role: CustodyVaultRole::HyperliquidSpot,
            asset: None,
            prefund_first_trade: false,
        });
    }
    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
    {
        let prior_trade_count = prior_transitions
            .iter()
            .filter(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidTrade)
            .count();
        let custody = hyperliquid_custody_for_prior_bridge(transitions, transition_index)?;
        return Ok(DerivedHyperliquidCustody {
            role: custody.role,
            asset: custody.asset,
            prefund_first_trade: prior_trade_count == 0,
        });
    }
    Err(MarketOrderRoutePlanError::UnsupportedRoute {
        reason: format!(
            "hyperliquid trade transition {} has no preceding custody ingress",
            transitions[transition_index].id
        ),
    })
}

fn hyperliquid_custody_for_withdrawal(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> MarketOrderRoutePlanResult<HyperliquidCustodySpec> {
    let prior_transitions = &transitions[..transition_index];
    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
    {
        return Ok(HyperliquidCustodySpec::spot_vault());
    }
    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
    {
        return hyperliquid_custody_for_prior_bridge(transitions, transition_index);
    }
    Err(MarketOrderRoutePlanError::UnsupportedRoute {
        reason: format!(
            "unit withdrawal transition {} has no preceding Hyperliquid custody source",
            transitions[transition_index].id
        ),
    })
}

fn hyperliquid_custody_for_prior_bridge(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> MarketOrderRoutePlanResult<HyperliquidCustodySpec> {
    let bridge_index = transitions[..transition_index]
        .iter()
        .rposition(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit
        })
        .ok_or_else(|| MarketOrderRoutePlanError::UnsupportedRoute {
            reason: "hyperliquid custody requested without a prior bridge deposit".to_string(),
        })?;
    let bridge = &transitions[bridge_index];
    let signer_asset = bridge.input.asset.clone();
    let role = input_custody_role_for_transition(transitions, bridge_index)
        .unwrap_or(CustodyVaultRole::SourceDeposit);
    match role {
        CustodyVaultRole::SourceDeposit | CustodyVaultRole::DestinationExecution => {
            Ok(HyperliquidCustodySpec {
                role,
                asset: Some(signer_asset),
                prefund_from_withdrawable: false,
            })
        }
        CustodyVaultRole::HyperliquidSpot => Err(MarketOrderRoutePlanError::UnsupportedRoute {
            reason: "hyperliquid bridge deposit cannot be signed from a Hyperliquid spot vault"
                .to_string(),
        }),
    }
}

struct AcrossBridgeStepSpec<'a> {
    order: &'a RouterOrder,
    source_vault: &'a DepositVault,
    quote: &'a MarketOrderQuote,
    transition: &'a TransitionDecl,
    leg: &'a QuoteLeg,
    source_role: Option<CustodyVaultRole>,
    is_final: bool,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn across_bridge_step(
    spec: AcrossBridgeStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let AcrossBridgeStepSpec {
        order,
        source_vault,
        quote,
        transition,
        leg,
        source_role,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let provider_id = leg_provider(leg, transition)?;
    let provider = provider_id.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let recipient = if is_final {
        json!(order.recipient_address)
    } else {
        Value::Null
    };
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };
    let source_role_string = source_role.map(|role| role.to_db_string().to_string());
    let (depositor_custody_vault_id, depositor_address, refund_custody_vault_id, refund_address) =
        if source_role.is_none() {
            (
                json!(source_vault.id),
                json!(source_vault.deposit_vault_address.clone()),
                json!(source_vault.id),
                json!(source_vault.deposit_vault_address.clone()),
            )
        } else {
            (Value::Null, Value::Null, Value::Null, Value::Null)
        };
    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("quote-{}", quote.id)),
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: json!({
            "order_id": order.id,
            "origin_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient": recipient,
            "recipient_custody_vault_role": recipient_role,
            "refund_address": refund_address,
            "refund_custody_vault_id": refund_custody_vault_id,
            "refund_custody_vault_role": source_role_string,
            "partial_fills_enabled": false,
            "depositor_address": depositor_address,
            "depositor_custody_vault_id": depositor_custody_vault_id,
            "depositor_custody_vault_role": source_role_string
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "partial_fills_enabled": false,
            "depositor_custody_vault_id": if source_role.is_none() { json!(source_vault.id) } else { Value::Null },
            "depositor_custody_vault_address": if source_role.is_none() { json!(source_vault.deposit_vault_address.clone()) } else { Value::Null },
            "depositor_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "depositor_custody_vault_status": if source_role.is_some() { json!("pending_derivation") } else { json!("bound") },
            "refund_custody_vault_id": if source_role.is_none() { json!(source_vault.id) } else { Value::Null },
            "refund_custody_vault_address": if source_role.is_none() { json!(source_vault.deposit_vault_address.clone()) } else { Value::Null },
            "refund_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "refund_custody_vault_status": if source_role.is_some() { json!("pending_derivation") } else { json!("bound") },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") }
        }),
        planned_at,
    }))
}

struct CctpBridgeStepSpec<'a> {
    order: &'a RouterOrder,
    source_vault: &'a DepositVault,
    transition: &'a TransitionDecl,
    burn_leg: &'a QuoteLeg,
    receive_leg: &'a QuoteLeg,
    source_role: Option<CustodyVaultRole>,
    is_final: bool,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn cctp_bridge_steps(
    spec: CctpBridgeStepSpec<'_>,
) -> MarketOrderRoutePlanResult<Vec<OrderExecutionStep>> {
    let CctpBridgeStepSpec {
        order,
        source_vault,
        transition,
        burn_leg,
        receive_leg,
        source_role,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let provider_id = leg_provider(burn_leg, transition)?;
    let receive_provider_id = leg_provider(receive_leg, transition)?;
    if receive_provider_id != provider_id {
        return Err(MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "CCTP receive leg provider {} does not match burn leg provider {} for transition {}",
                receive_provider_id.as_str(),
                provider_id.as_str(),
                transition.id
            ),
        });
    }
    let provider = provider_id.as_str().to_string();
    let amount_in = burn_leg.amount_in.clone();
    let amount_out = receive_leg.amount_out.clone();
    let max_fee = burn_leg
        .raw
        .get("max_fee")
        .and_then(Value::as_str)
        .unwrap_or("0")
        .to_string();
    let source_role_string = source_role.map(|role| role.to_db_string().to_string());
    let (source_custody_vault_id, source_custody_vault_address) = if source_role.is_none() {
        (
            json!(source_vault.id),
            json!(source_vault.deposit_vault_address.clone()),
        )
    } else {
        (Value::Null, Value::Null)
    };
    let recipient_address = if is_final {
        json!(order.recipient_address)
    } else {
        Value::Null
    };
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };

    let burn = step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(burn_leg.transition_decl_id.clone()),
        step_index,
        step_type: OrderExecutionStepType::CctpBurn,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: json!({
            "order_id": order.id,
            "transition_decl_id": transition.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": recipient_address,
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": recipient_role,
            "source_custody_vault_id": source_custody_vault_id,
            "source_custody_vault_address": source_custody_vault_address,
            "source_custody_vault_role": source_role_string,
            "max_fee": max_fee,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "source_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "source_custody_vault_status": if source_role.is_some() { json!("pending_derivation") } else { json!("bound") },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "receive_step_index": step_index + 1,
        }),
        planned_at,
    });

    let receive = step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(receive_leg.transition_decl_id.clone()),
        step_index: step_index + 1,
        step_type: OrderExecutionStepType::CctpReceive,
        provider,
        input_asset: Some(transition.output.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_out.clone()),
        min_amount_out: None,
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, provider_id.as_str(), step_index + 1),
        request: json!({
            "order_id": order.id,
            "burn_transition_decl_id": transition.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "source_custody_vault_id": null,
            "source_custody_vault_address": null,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "recipient_address": if is_final { json!(order.recipient_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "message": "",
            "attestation": "",
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "burn_step_index": step_index,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "source_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    });

    Ok(vec![burn, receive])
}

fn unit_deposit_step(
    order: &RouterOrder,
    source_vault: &DepositVault,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    source_role: Option<CustodyVaultRole>,
    step_index: i32,
    planned_at: DateTime<Utc>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let provider_id = leg_provider(leg, transition)?;
    let provider = provider_id.as_str().to_string();
    let expected_amount = leg.amount_in.clone();
    let output_asset = leg.output_deposit_asset().map_err(|reason| {
        MarketOrderRoutePlanError::UnsupportedRoute {
            reason: format!(
                "unit deposit leg {} has invalid output asset: {reason}",
                leg.transition_decl_id
            ),
        }
    })?;
    let source_role_string = source_role.map(|role| role.to_db_string().to_string());
    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UnitDeposit,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(output_asset.clone()),
        amount_in: Some(expected_amount.clone()),
        min_amount_out: None,
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: unit_deposit_request(UnitDepositRequestSpec {
            order_id: order.id,
            unit_ingress_asset: &transition.input.asset,
            unit_output_asset: &output_asset,
            expected_amount,
            source_custody_vault_id: if source_role.is_none() {
                Some(source_vault.id)
            } else {
                None
            },
            source_custody_vault_role: source_role_string.clone(),
            revert_custody_vault_id: if source_role.is_none() {
                Some(source_vault.id)
            } else {
                None
            },
            revert_custody_vault_role: source_role_string.clone(),
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "source_custody_vault_id": if source_role.is_none() { Some(source_vault.id) } else { None },
            "source_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "unit_deposit_provider_address_role": "unit_deposit",
            "unit_deposit_address_status": "pending_provider_generation",
            "hyperliquid_destination_provider_address_role": "hyperliquid_destination"
        }),
        planned_at,
    }))
}

fn hyperliquid_bridge_deposit_step(
    order: &RouterOrder,
    source_vault: &DepositVault,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    source_role: Option<CustodyVaultRole>,
    step_index: i32,
    planned_at: DateTime<Utc>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let provider_id = leg_provider(leg, transition)?;
    let provider = provider_id.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_role_string = source_role.map(|role| role.to_db_string().to_string());
    let (source_custody_vault_id, source_custody_vault_address) = if source_role.is_none() {
        (
            json!(source_vault.id),
            json!(source_vault.deposit_vault_address.clone()),
        )
    } else {
        (Value::Null, Value::Null)
    };
    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeDeposit,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: json!({
            "order_id": order.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_custody_vault_id,
            "source_custody_vault_role": source_role_string,
            "source_custody_vault_address": source_custody_vault_address
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "source_custody_vault_role": source_role.map(CustodyVaultRole::to_db_string),
            "source_custody_vault_status": if source_role.is_some() { json!("pending_derivation") } else { json!("bound") }
        }),
        planned_at,
    }))
}

struct HyperliquidBridgeWithdrawalStepSpec<'a> {
    order: &'a RouterOrder,
    quote: &'a MarketOrderQuote,
    transition: &'a TransitionDecl,
    leg: &'a QuoteLeg,
    hyperliquid_custody: HyperliquidCustodySpec,
    is_final: bool,
    step_index: i32,
    planned_at: DateTime<Utc>,
}

fn hyperliquid_bridge_withdrawal_step(
    spec: HyperliquidBridgeWithdrawalStepSpec<'_>,
) -> MarketOrderRoutePlanResult<OrderExecutionStep> {
    let HyperliquidBridgeWithdrawalStepSpec {
        order,
        quote,
        transition,
        leg,
        hyperliquid_custody,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let provider_id = leg_provider(leg, transition)?;
    let provider = provider_id.as_str().to_string();
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let recipient_address = if is_final {
        json!(order.recipient_address)
    } else {
        Value::Null
    };
    let recipient_role = if is_final {
        Value::Null
    } else {
        json!(CustodyVaultRole::DestinationExecution.to_db_string())
    };
    let custody_role = hyperliquid_custody.role;
    let custody_asset = hyperliquid_custody.asset.as_ref();

    Ok(step(StepSpec {
        order_id: order.id,
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeWithdrawal,
        provider: provider.clone(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: Some(amount_out.to_string()),
        provider_ref: Some(format!("quote-{}", quote.id)),
        idempotency_key: idempotency_key(order.id, &provider, step_index),
        request: json!({
            "order_id": order.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": recipient_address,
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": recipient_role,
            "transfer_from_spot": custody_role == CustodyVaultRole::HyperliquidSpot,
            "hyperliquid_custody_vault_role": custody_role.to_db_string(),
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_address": null,
            "hyperliquid_custody_vault_chain_id": custody_asset.map(|asset| asset.chain.as_str()),
            "hyperliquid_custody_vault_asset_id": custody_asset.map(|asset| asset.asset.as_str()),
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": custody_role.to_db_string(),
            "hyperliquid_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    }))
}

fn validate_step_materialization(steps: &[OrderExecutionStep]) -> MarketOrderRoutePlanResult<()> {
    for step in steps {
        let mut missing = Vec::new();
        if step.input_asset.is_none() {
            missing.push("input_asset");
        }
        if step.output_asset.is_none() {
            missing.push("output_asset");
        }
        if step.amount_in.is_none() {
            missing.push("amount_in");
        }
        if !missing.is_empty() {
            return Err(MarketOrderRoutePlanError::StepMaterializationInvariant {
                step_index: step.step_index,
                step_type: step.step_type.to_db_string(),
                reason: format!("missing {}", missing.join(", ")),
            });
        }
    }
    Ok(())
}

fn validate_leg_materialization(legs: &[OrderExecutionLeg]) -> MarketOrderRoutePlanResult<()> {
    for leg in legs {
        if leg.amount_in.is_empty() || leg.expected_amount_out.is_empty() {
            return Err(MarketOrderRoutePlanError::StepMaterializationInvariant {
                step_index: leg.leg_index,
                step_type: "execution_leg",
                reason: "missing amount_in or expected_amount_out".to_string(),
            });
        }
    }
    Ok(())
}

fn validate_intermediate_custody_plan(
    steps: &[OrderExecutionStep],
) -> MarketOrderRoutePlanResult<()> {
    for (index, step) in steps.iter().enumerate() {
        let is_final = index + 1 == steps.len();
        if is_final {
            continue;
        }
        match step.step_type {
            OrderExecutionStepType::AcrossBridge => require_request_role(
                step,
                "recipient_custody_vault_role",
                &[CustodyVaultRole::DestinationExecution],
            )
            .and_then(|_| {
                maybe_require_request_role(
                    step,
                    "depositor_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )
            })
            .and_then(|_| {
                maybe_require_request_role(
                    step,
                    "refund_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )
            })?,
            OrderExecutionStepType::CctpBurn => {
                maybe_require_request_role(
                    step,
                    "source_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )?;
                if step
                    .request
                    .get("recipient_custody_vault_role")
                    .is_some_and(|value| !value.is_null())
                {
                    require_request_role(
                        step,
                        "recipient_custody_vault_role",
                        &[CustodyVaultRole::DestinationExecution],
                    )?;
                } else if step
                    .request
                    .get("recipient_address")
                    .and_then(Value::as_str)
                    .is_none_or(str::is_empty)
                {
                    return Err(MarketOrderRoutePlanError::IntermediateCustodyInvariant {
                        step_index: step.step_index,
                        step_type: step.step_type.to_db_string(),
                        reason:
                            "cctp burn requires a destination execution recipient or final recipient address"
                                .to_string(),
                    });
                }
            }
            OrderExecutionStepType::CctpReceive => require_request_role(
                step,
                "source_custody_vault_role",
                &[CustodyVaultRole::DestinationExecution],
            )?,
            OrderExecutionStepType::UnitDeposit => require_request_role(
                step,
                "hyperliquid_custody_vault_role",
                &[CustodyVaultRole::HyperliquidSpot],
            )?,
            OrderExecutionStepType::HyperliquidBridgeDeposit => maybe_require_request_role(
                step,
                "source_custody_vault_role",
                &[CustodyVaultRole::DestinationExecution],
            )?,
            OrderExecutionStepType::HyperliquidBridgeWithdrawal => {
                require_request_role(
                    step,
                    "hyperliquid_custody_vault_role",
                    &[
                        CustodyVaultRole::DestinationExecution,
                        CustodyVaultRole::HyperliquidSpot,
                    ],
                )?;
                require_request_role(
                    step,
                    "recipient_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )?;
            }
            OrderExecutionStepType::HyperliquidTrade
            | OrderExecutionStepType::HyperliquidLimitOrder => require_request_role(
                step,
                "hyperliquid_custody_vault_role",
                &[
                    CustodyVaultRole::SourceDeposit,
                    CustodyVaultRole::DestinationExecution,
                    CustodyVaultRole::HyperliquidSpot,
                ],
            )?,
            OrderExecutionStepType::UniversalRouterSwap => {
                maybe_require_request_role(
                    step,
                    "source_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )?;
                require_request_role(
                    step,
                    "recipient_custody_vault_role",
                    &[CustodyVaultRole::DestinationExecution],
                )?;
            }
            OrderExecutionStepType::UnitWithdrawal => require_request_role(
                step,
                "recipient_custody_vault_role",
                &[CustodyVaultRole::DestinationExecution],
            )?,
            OrderExecutionStepType::WaitForDeposit | OrderExecutionStepType::Refund => {
                return Err(MarketOrderRoutePlanError::IntermediateCustodyInvariant {
                    step_index: step.step_index,
                    step_type: step.step_type.to_db_string(),
                    reason: "internal-only steps must not appear in market-order route plans"
                        .to_string(),
                });
            }
        }
    }
    Ok(())
}

fn require_request_role(
    step: &OrderExecutionStep,
    field: &'static str,
    allowed_roles: &[CustodyVaultRole],
) -> MarketOrderRoutePlanResult<()> {
    let Some(role) = step.request.get(field).and_then(Value::as_str) else {
        return Err(MarketOrderRoutePlanError::IntermediateCustodyInvariant {
            step_index: step.step_index,
            step_type: step.step_type.to_db_string(),
            reason: format!("request is missing {field}"),
        });
    };
    let Some(parsed) = CustodyVaultRole::from_db_string(role) else {
        return Err(MarketOrderRoutePlanError::IntermediateCustodyInvariant {
            step_index: step.step_index,
            step_type: step.step_type.to_db_string(),
            reason: format!("request field {field} contains unknown custody role {role:?}"),
        });
    };
    if allowed_roles.contains(&parsed) {
        return Ok(());
    }
    Err(MarketOrderRoutePlanError::IntermediateCustodyInvariant {
        step_index: step.step_index,
        step_type: step.step_type.to_db_string(),
        reason: format!(
            "request field {field} must be one of [{}], got {}",
            allowed_roles
                .iter()
                .map(|role| role.to_db_string())
                .collect::<Vec<_>>()
                .join(", "),
            parsed.to_db_string()
        ),
    })
}

fn maybe_require_request_role(
    step: &OrderExecutionStep,
    field: &'static str,
    allowed_roles: &[CustodyVaultRole],
) -> MarketOrderRoutePlanResult<()> {
    if step
        .request
        .get(field)
        .is_none_or(serde_json::Value::is_null)
    {
        return Ok(());
    }
    require_request_role(step, field, allowed_roles)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        DepositVaultStatus, MarketOrderAction, MarketOrderKind, RouterOrderStatus, RouterOrderType,
        VaultAction,
    };
    use crate::protocol::{AssetId, ChainId};

    fn planned_step(
        step_index: i32,
        step_type: OrderExecutionStepType,
        request: Value,
    ) -> OrderExecutionStep {
        OrderExecutionStep {
            id: Uuid::now_v7(),
            order_id: Uuid::now_v7(),
            execution_attempt_id: None,
            execution_leg_id: None,
            transition_decl_id: None,
            step_index,
            step_type,
            provider: "test".to_string(),
            status: OrderExecutionStepStatus::Planned,
            input_asset: None,
            output_asset: None,
            amount_in: None,
            min_amount_out: None,
            tx_hash: None,
            provider_ref: None,
            idempotency_key: None,
            attempt_count: 0,
            next_attempt_at: None,
            started_at: None,
            completed_at: None,
            details: json!({}),
            request,
            response: json!({}),
            error: json!({}),
            usd_valuation: json!({}),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset,
        }
    }

    fn assert_steps_are_materialized(steps: &[OrderExecutionStep]) {
        validate_step_materialization(steps).expect("steps should be materialized");
        for step in steps {
            assert!(
                step.input_asset.is_some(),
                "step {} ({}) missing input_asset",
                step.step_index,
                step.step_type.to_db_string()
            );
            assert!(
                step.output_asset.is_some(),
                "step {} ({}) missing output_asset",
                step.step_index,
                step.step_type.to_db_string()
            );
            assert!(
                step.amount_in.is_some(),
                "step {} ({}) missing amount_in",
                step.step_index,
                step.step_type.to_db_string()
            );
        }
    }

    #[test]
    fn materialization_validator_rejects_provider_step_missing_assets() {
        let mut step = planned_step(
            1,
            OrderExecutionStepType::UnitWithdrawal,
            json!({
                "recipient_custody_vault_role": "destination_execution"
            }),
        );
        step.amount_in = Some("1000".to_string());

        let err = validate_step_materialization(&[step]).expect_err("missing assets must reject");
        assert!(matches!(
            err,
            MarketOrderRoutePlanError::StepMaterializationInvariant { step_index: 1, .. }
        ));
    }

    #[test]
    fn intermediate_custody_validator_rejects_nonfinal_unit_withdrawal() {
        let steps = vec![
            planned_step(
                1,
                OrderExecutionStepType::UnitWithdrawal,
                json!({
                    "hyperliquid_custody_vault_role": "hyperliquid_spot"
                }),
            ),
            planned_step(2, OrderExecutionStepType::HyperliquidTrade, json!({})),
        ];

        let err = validate_intermediate_custody_plan(&steps).expect_err("must reject");
        assert!(matches!(
            err,
            MarketOrderRoutePlanError::IntermediateCustodyInvariant { step_index: 1, .. }
        ));
    }

    #[test]
    fn planner_resolves_serialized_runtime_velora_transitions_and_final_recipient() {
        let registry = Arc::new(AssetRegistry::default());
        let source_asset = asset(
            "evm:8453",
            AssetId::reference("0x3333333333333333333333333333333333333333"),
        );
        let destination_asset = asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );
        let path = registry
            .select_transition_paths(&source_asset, &destination_asset, 3)
            .into_iter()
            .find(|path| {
                path.transitions.len() == 2
                    && path.transitions.iter().all(|transition| {
                        transition.kind == MarketOrderTransitionKind::UniversalRouterSwap
                    })
            })
            .expect("runtime Velora path");
        let transition_ids = path
            .transitions
            .iter()
            .map(|transition| transition.id.clone())
            .collect::<Vec<_>>();
        let legs = path
            .transitions
            .iter()
            .map(|transition| {
                json!({
                    "transition_decl_id": transition.id,
                    "transition_parent_decl_id": transition.id,
                    "transition_kind": transition.kind.as_str(),
                    "execution_step_type": "universal_router_swap",
                    "provider": transition.provider.as_str(),
                    "input_asset": {
                        "chain_id": transition.input.asset.chain.as_str(),
                        "asset": transition.input.asset.asset.as_str(),
                    },
                    "output_asset": {
                        "chain_id": transition.output.asset.chain.as_str(),
                        "asset": transition.output.asset.asset.as_str(),
                    },
                    "amount_in": "1000000000000000000",
                    "amount_out": "999000000000000000",
                    "expires_at": (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339(),
                    "raw": {
                        "kind": "universal_router_swap",
                        "order_kind": "exact_in",
                        "amount_in": "1000000000000000000",
                        "amount_out": "999000000000000000",
                        "min_amount_out": "1",
                        "src_decimals": 18,
                        "dest_decimals": 18,
                        "slippage_bps": 100,
                        "price_route": {
                            "srcAmount": "1000000000000000000",
                            "destAmount": "999000000000000000",
                            "tokenTransferProxy": "0x9999999999999999999999999999999999999999"
                        },
                        "input_asset": {
                            "chain_id": transition.input.asset.chain.as_str(),
                            "asset": transition.input.asset.asset.as_str(),
                        },
                        "output_asset": {
                            "chain_id": transition.output.asset.chain.as_str(),
                            "asset": transition.output.asset.asset.as_str(),
                        }
                    },
                })
            })
            .collect::<Vec<_>>();

        let now = Utc::now();
        let order_id = Uuid::now_v7();
        let source_vault_id = Uuid::now_v7();
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Funded,
            funding_vault_id: Some(source_vault_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: "0x5555555555555555555555555555555555555555".to_string(),
            refund_address: "0x6666666666666666666666666666666666666666".to_string(),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "1000000000000000000".to_string(),
                    min_amount_out: "1".to_string(),
                },
                slippage_bps: 100,
            }),
            action_timeout_at: now + chrono::Duration::minutes(10),
            idempotency_key: None,
            workflow_trace_id: order_id.simple().to_string(),
            workflow_parent_span_id: "1111111111111111".to_string(),
            created_at: now,
            updated_at: now,
        };
        let source_vault = DepositVault {
            id: source_vault_id,
            order_id: Some(order_id),
            deposit_asset: source_asset.clone(),
            action: VaultAction::Null,
            metadata: json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "0x7777777777777777777777777777777777777777".to_string(),
            recovery_address: "0x8888888888888888888888888888888888888888".to_string(),
            cancellation_commitment: "0x".to_string(),
            cancel_after: now + chrono::Duration::minutes(10),
            status: DepositVaultStatus::Funded,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            created_at: now,
            updated_at: now,
        };
        let quote = MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: order.recipient_address.clone(),
            provider_id: "path:runtime-velora".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "1000000000000000000".to_string(),
            amount_out: "999000000000000000".to_string(),
            min_amount_out: Some("1".to_string()),
            max_amount_in: None,
            slippage_bps: 100,
            provider_quote: json!({
                "path_id": path.id,
                "transition_decl_ids": transition_ids,
                "transitions": path.transitions,
                "legs": legs,
                "gas_reimbursement": {
                    "retention_actions": []
                }
            }),
            usd_valuation: json!({}),
            expires_at: now + chrono::Duration::minutes(5),
            created_at: now,
        };

        let plan = MarketOrderRoutePlanner::new(registry)
            .plan(&order, &source_vault, &quote, now)
            .expect("runtime Velora path should plan");

        assert_steps_are_materialized(&plan.steps);
        assert_eq!(plan.steps.len(), 2);
        assert_eq!(plan.transition_decl_ids, transition_ids);
        assert_eq!(
            plan.steps
                .iter()
                .map(|step| step.step_type)
                .collect::<Vec<_>>(),
            vec![
                OrderExecutionStepType::UniversalRouterSwap,
                OrderExecutionStepType::UniversalRouterSwap
            ]
        );
        assert_eq!(
            plan.steps[0].request["source_custody_vault_id"],
            json!(source_vault_id)
        );
        assert_eq!(
            plan.steps[0].request["recipient_custody_vault_role"],
            json!("destination_execution")
        );
        assert!(plan.steps[0].request["recipient_address"].is_null());
        assert_eq!(
            plan.steps[1].request["source_custody_vault_role"],
            json!("destination_execution")
        );
        assert_eq!(
            plan.steps[1].request["recipient_address"],
            json!(order.recipient_address)
        );
        assert!(plan.steps[1].request["recipient_custody_vault_role"].is_null());
        assert_eq!(plan.steps[1].output_asset, Some(destination_asset));
    }

    #[test]
    fn planner_materializes_universal_router_swap_with_bound_source_and_final_recipient() {
        let registry = Arc::new(AssetRegistry::default());
        let source_asset = asset(
            "evm:8453",
            AssetId::reference("0x3333333333333333333333333333333333333333"),
        );
        let destination_asset = asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );
        let path = registry
            .select_transition_paths(&source_asset, &destination_asset, 1)
            .into_iter()
            .find(|path| {
                path.transitions.len() == 1
                    && path.transitions[0].kind == MarketOrderTransitionKind::UniversalRouterSwap
            })
            .expect("direct Velora runtime path");
        let transition = &path.transitions[0];
        let transition_ids = vec![transition.id.clone()];
        let now = Utc::now();
        let order_id = Uuid::now_v7();
        let source_vault_id = Uuid::now_v7();
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Funded,
            funding_vault_id: Some(source_vault_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: "0x5555555555555555555555555555555555555555".to_string(),
            refund_address: "0x6666666666666666666666666666666666666666".to_string(),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "1000000000000000000".to_string(),
                    min_amount_out: "900000000000000000".to_string(),
                },
                slippage_bps: 100,
            }),
            action_timeout_at: now + chrono::Duration::minutes(10),
            idempotency_key: None,
            workflow_trace_id: order_id.simple().to_string(),
            workflow_parent_span_id: "1111111111111111".to_string(),
            created_at: now,
            updated_at: now,
        };
        let source_vault = DepositVault {
            id: source_vault_id,
            order_id: Some(order_id),
            deposit_asset: source_asset.clone(),
            action: VaultAction::Null,
            metadata: json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "0x7777777777777777777777777777777777777777".to_string(),
            recovery_address: "0x8888888888888888888888888888888888888888".to_string(),
            cancellation_commitment: "0x".to_string(),
            cancel_after: now + chrono::Duration::minutes(10),
            status: DepositVaultStatus::Funded,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            created_at: now,
            updated_at: now,
        };
        let leg = json!({
            "transition_decl_id": transition.id,
            "transition_parent_decl_id": transition.id,
            "transition_kind": transition.kind.as_str(),
            "execution_step_type": "universal_router_swap",
            "provider": "velora",
            "input_asset": {
                "chain_id": source_asset.chain.as_str(),
                "asset": source_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": destination_asset.chain.as_str(),
                "asset": destination_asset.asset.as_str(),
            },
            "amount_in": "1000000000000000000",
            "amount_out": "950000000000000000",
            "expires_at": (now + chrono::Duration::minutes(5)).to_rfc3339(),
            "raw": {
                "kind": "universal_router_swap",
                "order_kind": "exact_in",
                "amount_in": "1000000000000000000",
                "amount_out": "950000000000000000",
                "min_amount_out": "900000000000000000",
                "src_decimals": 18,
                "dest_decimals": 18,
                "slippage_bps": 526,
                "price_route": {
                    "srcAmount": "1000000000000000000",
                    "destAmount": "950000000000000000",
                    "tokenTransferProxy": "0x9999999999999999999999999999999999999999"
                },
                "input_asset": {
                    "chain_id": source_asset.chain.as_str(),
                    "asset": source_asset.asset.as_str(),
                },
                "output_asset": {
                    "chain_id": destination_asset.chain.as_str(),
                    "asset": destination_asset.asset.as_str(),
                }
            },
        });
        let quote = MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: order.recipient_address.clone(),
            provider_id: "path:velora".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "1000000000000000000".to_string(),
            amount_out: "950000000000000000".to_string(),
            min_amount_out: Some("900000000000000000".to_string()),
            max_amount_in: None,
            slippage_bps: 526,
            provider_quote: json!({
                "path_id": path.id,
                "transition_decl_ids": transition_ids,
                "transitions": path.transitions,
                "legs": [leg],
                "gas_reimbursement": {
                    "retention_actions": []
                }
            }),
            usd_valuation: json!({}),
            expires_at: now + chrono::Duration::minutes(5),
            created_at: now,
        };

        let plan = MarketOrderRoutePlanner::new(registry)
            .plan(&order, &source_vault, &quote, now)
            .expect("Velora path should plan");

        assert_steps_are_materialized(&plan.steps);
        assert_eq!(plan.steps.len(), 1);
        let step = &plan.steps[0];
        assert_eq!(step.step_type, OrderExecutionStepType::UniversalRouterSwap);
        assert_eq!(step.provider, "velora");
        assert_eq!(
            step.request["source_custody_vault_id"],
            json!(source_vault_id)
        );
        assert_eq!(
            step.request["source_custody_vault_address"],
            json!(source_vault.deposit_vault_address)
        );
        assert!(step.request["source_custody_vault_role"].is_null());
        assert_eq!(
            step.request["recipient_address"],
            json!(order.recipient_address)
        );
        assert!(step.request["recipient_custody_vault_role"].is_null());
        assert_eq!(step.request["input_decimals"], json!(18));
        assert_eq!(step.request["output_decimals"], json!(18));
        assert_eq!(
            step.request["price_route"]["tokenTransferProxy"],
            json!("0x9999999999999999999999999999999999999999")
        );
    }

    #[test]
    fn planner_materializes_cctp_bridge_as_burn_then_receive_steps() {
        let registry = Arc::new(AssetRegistry::default());
        let source_asset = asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );
        let destination_asset = asset(
            "evm:42161",
            AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
        );
        let path = registry
            .select_transition_paths(&source_asset, &destination_asset, 2)
            .into_iter()
            .find(|path| {
                path.transitions.len() == 1
                    && path.transitions[0].kind == MarketOrderTransitionKind::CctpBridge
            })
            .expect("direct CCTP path");
        let transition = &path.transitions[0];
        let now = Utc::now();
        let order_id = Uuid::now_v7();
        let source_vault_id = Uuid::now_v7();
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Funded,
            funding_vault_id: Some(source_vault_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: "0x5555555555555555555555555555555555555555".to_string(),
            refund_address: "0x6666666666666666666666666666666666666666".to_string(),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "1000000".to_string(),
                    min_amount_out: "990000".to_string(),
                },
                slippage_bps: 100,
            }),
            action_timeout_at: now + chrono::Duration::minutes(10),
            idempotency_key: None,
            workflow_trace_id: order_id.simple().to_string(),
            workflow_parent_span_id: "1111111111111111".to_string(),
            created_at: now,
            updated_at: now,
        };
        let source_vault = DepositVault {
            id: source_vault_id,
            order_id: Some(order_id),
            deposit_asset: source_asset.clone(),
            action: VaultAction::Null,
            metadata: json!({}),
            deposit_vault_salt: [7; 32],
            deposit_vault_address: "0x7777777777777777777777777777777777777777".to_string(),
            recovery_address: "0x8888888888888888888888888888888888888888".to_string(),
            cancellation_commitment: "0x".to_string(),
            cancel_after: now + chrono::Duration::minutes(10),
            status: DepositVaultStatus::Funded,
            refund_requested_at: None,
            refunded_at: None,
            refund_tx_hash: None,
            last_refund_error: None,
            created_at: now,
            updated_at: now,
        };
        let transition_ids = vec![transition.id.clone()];
        let burn_leg = json!({
            "transition_decl_id": transition.id,
            "transition_parent_decl_id": transition.id,
            "transition_kind": transition.kind.as_str(),
            "execution_step_type": "cctp_burn",
            "provider": "cctp",
            "input_asset": {
                "chain_id": source_asset.chain.as_str(),
                "asset": source_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": destination_asset.chain.as_str(),
                "asset": destination_asset.asset.as_str(),
            },
            "amount_in": "1000000",
            "amount_out": "1000000",
            "expires_at": (now + chrono::Duration::minutes(5)).to_rfc3339(),
            "raw": {
                "kind": "cctp_burn",
                "bridge_kind": "cctp_bridge",
                "execution_step_type": "cctp_burn",
                "source_domain": 6,
                "destination_domain": 3,
                "max_fee": "0",
            },
        });
        let receive_leg = json!({
            "transition_decl_id": format!("{}:receive", transition.id),
            "transition_parent_decl_id": transition.id,
            "transition_kind": transition.kind.as_str(),
            "execution_step_type": "cctp_receive",
            "provider": "cctp",
            "input_asset": {
                "chain_id": destination_asset.chain.as_str(),
                "asset": destination_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": destination_asset.chain.as_str(),
                "asset": destination_asset.asset.as_str(),
            },
            "amount_in": "1000000",
            "amount_out": "1000000",
            "expires_at": (now + chrono::Duration::minutes(5)).to_rfc3339(),
            "raw": {
                "kind": "cctp_receive",
                "bridge_kind": "cctp_bridge",
                "execution_step_type": "cctp_receive",
                "source_domain": 6,
                "destination_domain": 3,
                "max_fee": "0",
            },
        });
        let quote = MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            recipient_address: order.recipient_address.clone(),
            provider_id: "path:cctp".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "1000000".to_string(),
            amount_out: "1000000".to_string(),
            min_amount_out: Some("990000".to_string()),
            max_amount_in: None,
            slippage_bps: 100,
            provider_quote: json!({
                "path_id": path.id,
                "transition_decl_ids": transition_ids,
                "transitions": path.transitions,
                "legs": [burn_leg, receive_leg],
                "gas_reimbursement": {
                    "retention_actions": []
                }
            }),
            usd_valuation: json!({}),
            expires_at: now + chrono::Duration::minutes(5),
            created_at: now,
        };

        let plan = MarketOrderRoutePlanner::new(registry)
            .plan(&order, &source_vault, &quote, now)
            .expect("CCTP path should plan");

        assert_steps_are_materialized(&plan.steps);
        let quoted_legs = quote.provider_quote["legs"].as_array().unwrap();
        assert_eq!(quoted_legs.len(), 2);
        assert_eq!(quoted_legs[0]["execution_step_type"], json!("cctp_burn"));
        assert_eq!(quoted_legs[1]["execution_step_type"], json!("cctp_receive"));
        assert_eq!(quoted_legs[0]["transition_decl_id"], json!(transition.id));
        assert_eq!(
            quoted_legs[1]["transition_decl_id"],
            json!(format!("{}:receive", transition.id))
        );

        assert_eq!(plan.legs.len(), 1);
        assert_eq!(
            plan.legs[0].leg_type,
            MarketOrderTransitionKind::CctpBridge.as_str()
        );
        assert_eq!(plan.legs[0].input_asset, source_asset);
        assert_eq!(plan.legs[0].output_asset, destination_asset);
        assert_eq!(plan.legs[0].amount_in, "1000000");
        assert_eq!(plan.legs[0].expected_amount_out, "1000000");
        assert_eq!(plan.steps[0].execution_leg_id, Some(plan.legs[0].id));
        assert_eq!(plan.steps[1].execution_leg_id, Some(plan.legs[0].id));

        assert_eq!(plan.steps.len(), 2);
        assert_eq!(
            plan.steps
                .iter()
                .map(|step| step.step_type)
                .collect::<Vec<_>>(),
            vec![
                OrderExecutionStepType::CctpBurn,
                OrderExecutionStepType::CctpReceive
            ]
        );
        assert_eq!(
            plan.steps[0].transition_decl_id,
            Some(transition.id.clone())
        );
        assert_eq!(
            plan.steps[1].transition_decl_id,
            Some(format!("{}:receive", transition.id))
        );
        assert_eq!(
            plan.steps[0].request["source_custody_vault_id"],
            json!(source_vault_id)
        );
        assert_eq!(
            plan.steps[0].request["recipient_address"],
            json!(order.recipient_address)
        );
        assert_eq!(
            plan.steps[1].request["burn_transition_decl_id"],
            json!(transition.id)
        );
        assert_eq!(
            plan.steps[1].request["source_custody_vault_role"],
            json!("destination_execution")
        );
        assert_eq!(plan.steps[1].request["message"], json!(""));
        assert_eq!(plan.steps[1].output_asset, Some(destination_asset));
    }

    #[test]
    fn intermediate_custody_validator_rejects_across_bridge_without_destination_execution() {
        let steps = vec![
            planned_step(
                1,
                OrderExecutionStepType::AcrossBridge,
                json!({
                    "recipient_custody_vault_role": "hyperliquid_spot"
                }),
            ),
            planned_step(2, OrderExecutionStepType::UnitWithdrawal, json!({})),
        ];

        let err = validate_intermediate_custody_plan(&steps).expect_err("must reject");
        assert!(matches!(
            err,
            MarketOrderRoutePlanError::IntermediateCustodyInvariant { step_index: 1, .. }
        ));
    }
}
