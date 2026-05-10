use super::*;

#[derive(Clone)]
pub(super) struct RefundQuotedPath {
    pub(super) path: TransitionPath,
    pub(super) amount_out: String,
    pub(super) legs: Vec<QuoteLeg>,
}

pub(super) struct RefundTransitionQuote {
    pub(super) amount_out: String,
    pub(super) legs: Vec<QuoteLeg>,
}

pub(super) async fn external_custody_refund_back_steps(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Option<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>)>, OrderActivityError> {
    let Some(quoted_path) =
        best_external_custody_refund_quote(deps, order, vault, asset, amount).await?
    else {
        return Ok(None);
    };
    materialize_external_custody_refund_path(order, vault, &quoted_path, planned_at).map(Some)
}

pub(super) fn materialize_external_custody_refund_path(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    planned_at: chrono::DateTime<Utc>,
) -> Result<(Vec<OrderExecutionLeg>, Vec<OrderExecutionStep>), OrderActivityError> {
    let mut execution_legs = Vec::new();
    let mut steps = Vec::new();
    let mut step_index = 0_i32;
    let mut leg_index = 0_i32;

    for (transition_index, transition) in quoted_path.path.transitions.iter().enumerate() {
        let is_final = transition_index + 1 == quoted_path.path.transitions.len();
        let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
        if transition_legs.is_empty() {
            return Err(refund_materialization_error(format!(
                "quoted ExternalCustody refund path is missing legs for transition {}",
                transition.id
            )));
        }
        let transition_steps = materialize_external_custody_refund_transition(
            order,
            vault,
            quoted_path,
            transition,
            transition_index,
            step_index,
            is_final,
            planned_at,
        )?;
        append_refund_transition_plan(
            order,
            transition,
            transition_legs,
            transition_steps,
            leg_index,
            planned_at,
            &mut execution_legs,
            &mut steps,
        )?;
        leg_index = leg_index
            .checked_add(1)
            .ok_or_else(|| refund_materialization_error("refund leg_index overflow"))?;
        step_index = i32::try_from(steps.len()).map_err(|err| {
            refund_materialization_error(format!("refund step_index overflow: {err}"))
        })?;
    }

    Ok((execution_legs, steps))
}

fn materialize_external_custody_refund_transition(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    transition_index: usize,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let source = external_refund_source_binding(vault, transition_index);
    match transition.kind {
        MarketOrderTransitionKind::AcrossBridge => {
            let leg = external_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::AcrossBridge,
            )?;
            Ok(vec![refund_transition_across_bridge_step(
                order, source, transition, &leg, is_final, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::CctpBridge => {
            materialize_external_custody_cctp_refund_transition(
                order,
                source,
                quoted_path,
                transition,
                step_index,
                is_final,
                planned_at,
            )
        }
        MarketOrderTransitionKind::UniversalRouterSwap => {
            let leg = external_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::UniversalRouterSwap,
            )?;
            Ok(vec![refund_transition_universal_router_swap_step(
                order, source, transition, &leg, is_final, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
            let leg = external_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::HyperliquidBridgeDeposit,
            )?;
            Ok(vec![refund_transition_hyperliquid_bridge_deposit_step(
                order, source, transition, &leg, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            let leg = external_refund_leg(
                quoted_path,
                transition,
                OrderExecutionStepType::HyperliquidBridgeWithdrawal,
            )?;
            let custody = external_refund_hyperliquid_binding_for_transition(
                vault,
                quoted_path,
                transition_index,
            )?;
            Ok(vec![refund_transition_hyperliquid_bridge_withdrawal_step(
                order, transition, &leg, &custody, is_final, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::UnitDeposit => {
            let leg =
                external_refund_leg(quoted_path, transition, OrderExecutionStepType::UnitDeposit)?;
            Ok(vec![refund_transition_unit_deposit_step(
                order, source, transition, &leg, step_index, planned_at,
            )?])
        }
        MarketOrderTransitionKind::HyperliquidTrade => materialize_external_custody_trade_steps(
            order,
            vault,
            quoted_path,
            transition,
            transition_index,
            step_index,
            planned_at,
        ),
        MarketOrderTransitionKind::UnitWithdrawal => materialize_external_custody_unit_withdrawal(
            order,
            vault,
            quoted_path,
            transition,
            transition_index,
            step_index,
            is_final,
            planned_at,
        ),
    }
}

fn external_refund_leg(
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_type: OrderExecutionStepType,
) -> Result<QuoteLeg, OrderActivityError> {
    refund_take_one_leg(&quoted_path.legs, transition, step_type)
}

fn external_refund_hyperliquid_binding_for_transition(
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition_index: usize,
) -> Result<RefundHyperliquidBinding, OrderActivityError> {
    external_refund_hyperliquid_binding(vault, &quoted_path.path.transitions, transition_index)
}

fn materialize_external_custody_unit_withdrawal(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    transition_index: usize,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let leg = external_refund_leg(
        quoted_path,
        transition,
        OrderExecutionStepType::UnitWithdrawal,
    )?;
    let custody =
        external_refund_hyperliquid_binding_for_transition(vault, quoted_path, transition_index)?;
    Ok(vec![refund_transition_unit_withdrawal_step(
        order, transition, &leg, &custody, is_final, step_index, planned_at,
    )?])
}

fn materialize_external_custody_cctp_refund_transition(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    step_index: i32,
    is_final: bool,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let burn_leg = refund_take_one_leg(
        &quoted_path.legs,
        transition,
        OrderExecutionStepType::CctpBurn,
    )?;
    let receive_leg = refund_take_one_leg(
        &quoted_path.legs,
        transition,
        OrderExecutionStepType::CctpReceive,
    )?;
    refund_transition_cctp_bridge_steps(
        order,
        source,
        transition,
        &burn_leg,
        &receive_leg,
        is_final,
        step_index,
        planned_at,
    )
}

fn materialize_external_custody_trade_steps(
    order: &RouterOrder,
    vault: &CustodyVault,
    quoted_path: &RefundQuotedPath,
    transition: &TransitionDecl,
    transition_index: usize,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let transition_legs = refund_legs_for_transition(&quoted_path.legs, transition);
    if transition_legs.is_empty() {
        return Err(refund_materialization_error(format!(
            "quoted refund path is missing HyperliquidTrade legs for transition {}",
            transition.id
        )));
    }
    let custody = external_refund_hyperliquid_binding(
        vault,
        &quoted_path.path.transitions,
        transition_index,
    )?;
    let prefund_first_trade =
        refund_trade_prefund_from_withdrawable(&quoted_path.path.transitions, transition_index);
    materialize_refund_hyperliquid_trade_steps(
        order,
        transition,
        transition_legs,
        &custody,
        prefund_first_trade,
        step_index,
        planned_at,
    )
}

pub(super) fn materialize_refund_hyperliquid_trade_steps(
    order: &RouterOrder,
    transition: &TransitionDecl,
    transition_legs: Vec<QuoteLeg>,
    custody: &RefundHyperliquidBinding,
    prefund_first_trade: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let refund_quote_id = Uuid::now_v7();
    let leg_count = transition_legs.len();
    let mut trade_steps = Vec::with_capacity(leg_count);
    for (local_leg_index, leg) in transition_legs.iter().enumerate() {
        trade_steps.push(refund_transition_hyperliquid_trade_step(
            RefundHyperliquidTradeStepSpec {
                order,
                transition,
                leg,
                custody,
                prefund_from_withdrawable: prefund_first_trade && local_leg_index == 0,
                refund_quote_id,
                leg_index: local_leg_index,
                leg_count,
                step_index: step_index
                    .checked_add(i32::try_from(local_leg_index).map_err(|err| {
                        refund_materialization_error(format!(
                            "refund HyperliquidTrade step_index overflow: {err}"
                        ))
                    })?)
                    .ok_or_else(|| {
                        refund_materialization_error("refund HyperliquidTrade step_index overflow")
                    })?,
                planned_at,
            },
        )?);
    }
    Ok(trade_steps)
}

pub(super) async fn best_external_custody_refund_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    asset: &DepositAsset,
    amount: &str,
) -> Result<Option<RefundQuotedPath>, OrderActivityError> {
    let mut paths = deps
        .action_providers
        .asset_registry()
        .select_transition_paths_between(
            MarketOrderNode::External(asset.clone()),
            MarketOrderNode::External(order.source_asset.clone()),
            REFUND_PATH_MAX_DEPTH,
        )
        .into_iter()
        .filter(|path| {
            refund_path_compatible_with_position(
                RecoverablePositionKind::ExternalCustody,
                Some(vault.role),
                path,
            )
        })
        .collect::<Vec<_>>();
    paths.sort_by(|left, right| {
        left.transitions
            .len()
            .cmp(&right.transitions.len())
            .then_with(|| left.id.cmp(&right.id))
    });

    let mut best = None;
    for path in paths {
        let path_id = path.id.clone();
        match quote_external_custody_refund_path(deps, order, vault, amount, path).await {
            Ok(Some(quoted)) => {
                best = choose_better_refund_quote(quoted, best)?;
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(
                    order_id = %order.id,
                    path_id,
                    error = ?err,
                    "refund transition-path quote failed"
                );
            }
        }
    }
    Ok(best)
}

pub(super) async fn quote_external_custody_refund_path(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    amount: &str,
    path: TransitionPath,
) -> Result<Option<RefundQuotedPath>, OrderActivityError> {
    let mut cursor_amount = amount.to_string();
    let mut legs = Vec::new();

    for transition_index in 0..path.transitions.len() {
        let Some(quote) = quote_external_custody_refund_transition(
            deps,
            order,
            vault,
            &path,
            transition_index,
            &cursor_amount,
        )
        .await?
        else {
            return Ok(None);
        };
        cursor_amount = quote.amount_out;
        legs.extend(quote.legs);
    }

    Ok(Some(RefundQuotedPath {
        path,
        amount_out: cursor_amount,
        legs,
    }))
}

async fn quote_external_custody_refund_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    vault: &CustodyVault,
    path: &TransitionPath,
    transition_index: usize,
    cursor_amount: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let transition = &path.transitions[transition_index];
    match transition.kind {
        MarketOrderTransitionKind::AcrossBridge
        | MarketOrderTransitionKind::CctpBridge
        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            refund_bridge_quote_transition(deps, order, transition, cursor_amount, &vault.address)
                .await
        }
        MarketOrderTransitionKind::UniversalRouterSwap => {
            refund_universal_router_quote_transition(
                deps,
                order,
                transition,
                cursor_amount,
                Some(vault.address.clone()),
            )
            .await
        }
        MarketOrderTransitionKind::UnitDeposit => {
            refund_unit_deposit_quote_transition(deps, transition, cursor_amount)
        }
        MarketOrderTransitionKind::HyperliquidTrade => {
            let amount_in = external_custody_hyperliquid_trade_quote_amount(
                path,
                transition_index,
                cursor_amount,
            )?;
            refund_hyperliquid_trade_quote_transition(deps, order, transition, &amount_in).await
        }
        MarketOrderTransitionKind::UnitWithdrawal => {
            refund_unit_withdrawal_quote_transition(deps, order, transition, cursor_amount)
        }
    }
}

fn external_custody_hyperliquid_trade_quote_amount(
    path: &TransitionPath,
    transition_index: usize,
    cursor_amount: &str,
) -> Result<String, OrderActivityError> {
    if transition_index > 0
        && path.transitions[transition_index - 1].kind
            == MarketOrderTransitionKind::HyperliquidBridgeDeposit
    {
        reserve_refund_hyperliquid_spot_send_quote_gas(
            "refund.hyperliquid_trade.amount_in",
            cursor_amount,
        )
    } else {
        Ok(cursor_amount.to_string())
    }
}

pub(super) async fn refund_bridge_quote_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    transition: &TransitionDecl,
    cursor_amount: &str,
    depositor_address: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let bridge = deps
        .action_providers
        .bridge(transition.provider.as_str())
        .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
    let Some(quote) = bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: transition.input.asset.clone(),
            destination_asset: transition.output.asset.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: cursor_amount.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: order.refund_address.clone(),
            depositor_address: depositor_address.to_string(),
            partial_fills_enabled: false,
        })
        .await
        .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
    else {
        return Ok(None);
    };
    Ok(Some(RefundTransitionQuote {
        amount_out: quote.amount_out.clone(),
        legs: refund_bridge_quote_legs(transition, &quote)?,
    }))
}

pub(super) async fn refund_universal_router_quote_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    transition: &TransitionDecl,
    cursor_amount: &str,
    sender_address: Option<String>,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let exchange = deps
        .action_providers
        .exchange(transition.provider.as_str())
        .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
    let Some(quote) = exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: transition.input.asset.clone(),
            output_asset: transition.output.asset.clone(),
            input_decimals: refund_asset_decimals(deps, &transition.input.asset),
            output_decimals: refund_asset_decimals(deps, &transition.output.asset),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: cursor_amount.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address,
            recipient_address: order.refund_address.clone(),
        })
        .await
        .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
    else {
        return Ok(None);
    };
    Ok(Some(RefundTransitionQuote {
        amount_out: quote.amount_out.clone(),
        legs: refund_exchange_quote_transition_legs(
            &transition.id,
            transition.kind,
            transition.provider,
            &quote,
        )?,
    }))
}

pub(super) async fn refund_hyperliquid_trade_quote_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    transition: &TransitionDecl,
    cursor_amount: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let exchange = deps
        .action_providers
        .exchange(transition.provider.as_str())
        .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
    let Some(quote) = exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: transition.input.asset.clone(),
            output_asset: transition.output.asset.clone(),
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: cursor_amount.to_string(),
                min_amount_out: Some("1".to_string()),
            },
            sender_address: None,
            recipient_address: order.refund_address.clone(),
        })
        .await
        .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
    else {
        return Ok(None);
    };
    Ok(Some(RefundTransitionQuote {
        amount_out: quote.amount_out.clone(),
        legs: refund_exchange_quote_transition_legs(
            &transition.id,
            transition.kind,
            transition.provider,
            &quote,
        )?,
    }))
}

pub(super) fn refund_unit_deposit_quote_transition(
    deps: &OrderActivityDeps,
    transition: &TransitionDecl,
    cursor_amount: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let unit = deps
        .action_providers
        .unit(transition.provider.as_str())
        .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
    if !unit.supports_deposit(&transition.input.asset) {
        return Ok(None);
    }
    let leg = refund_unit_deposit_quote_leg(
        transition,
        cursor_amount,
        Utc::now() + chrono::Duration::minutes(10),
    );
    Ok(Some(RefundTransitionQuote {
        amount_out: leg.amount_out.clone(),
        legs: vec![leg],
    }))
}

pub(super) fn refund_unit_withdrawal_quote_transition(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    transition: &TransitionDecl,
    cursor_amount: &str,
) -> Result<Option<RefundTransitionQuote>, OrderActivityError> {
    let unit = deps
        .action_providers
        .unit(transition.provider.as_str())
        .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
    if !unit.supports_withdrawal(&transition.output.asset)
        || !refund_unit_withdrawal_amount_meets_minimum(transition, cursor_amount)?
    {
        return Ok(None);
    }
    let leg = refund_unit_withdrawal_quote_leg(
        order,
        transition,
        cursor_amount,
        Utc::now() + chrono::Duration::minutes(10),
    );
    Ok(Some(RefundTransitionQuote {
        amount_out: leg.amount_out.clone(),
        legs: vec![leg],
    }))
}

pub(super) fn refund_asset_decimals(deps: &OrderActivityDeps, asset: &DepositAsset) -> Option<u8> {
    deps.action_providers
        .asset_registry()
        .chain_asset(asset)
        .map(|entry| entry.decimals)
        .or_else(|| (asset.chain.evm_chain_id().is_some() && asset.asset.is_native()).then_some(18))
}

pub(super) fn choose_better_refund_quote(
    candidate: RefundQuotedPath,
    current: Option<RefundQuotedPath>,
) -> Result<Option<RefundQuotedPath>, OrderActivityError> {
    let Some(current) = current else {
        validate_refund_amount("refund.amount_out", &candidate.amount_out)?;
        return Ok(Some(candidate));
    };
    if refund_amount_gt(&candidate.amount_out, &current.amount_out)? {
        Ok(Some(candidate))
    } else {
        Ok(Some(current))
    }
}

pub(super) fn refund_amount_gt(candidate: &str, current: &str) -> Result<bool, OrderActivityError> {
    let candidate = normalize_refund_amount("refund.amount_out", candidate)?;
    let current = normalize_refund_amount("refund.amount_out", current)?;
    Ok(candidate.len() > current.len() || candidate.len() == current.len() && candidate > current)
}

pub(super) fn refund_amount_gte(value: &str, minimum: &str) -> Result<bool, OrderActivityError> {
    let value = normalize_refund_amount("refund.amount", value)?;
    let minimum = normalize_refund_amount("refund.minimum_amount", minimum)?;
    Ok(value.len() > minimum.len() || value.len() == minimum.len() && value >= minimum)
}

pub(super) fn validate_refund_amount(
    field: &'static str,
    value: &str,
) -> Result<(), OrderActivityError> {
    normalize_refund_amount(field, value).map(|_| ())
}

pub(super) fn normalize_refund_amount<'a>(
    field: &'static str,
    value: &'a str,
) -> Result<&'a str, OrderActivityError> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(amount_parse_error(
            field,
            "invalid amount for {field}: expected unsigned decimal integer",
        ));
    }
    let normalized = value.trim_start_matches('0');
    Ok(if normalized.is_empty() {
        "0"
    } else {
        normalized
    })
}

pub(super) fn refund_bridge_quote_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    match transition.kind {
        MarketOrderTransitionKind::AcrossBridge => Ok(vec![QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &transition.input.asset,
            output_asset: &transition.output.asset,
            amount_in: &quote.amount_in,
            amount_out: &quote.amount_out,
            expires_at: quote.expires_at,
            raw: quote.provider_quote.clone(),
        })]),
        MarketOrderTransitionKind::CctpBridge => {
            let burn = QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.input.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_in,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: cctp_refund_quote_leg_raw(
                    &quote.provider_quote,
                    OrderExecutionStepType::CctpBurn,
                ),
            });
            let receive = QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.output.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_out,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: cctp_refund_quote_leg_raw(
                    &quote.provider_quote,
                    OrderExecutionStepType::CctpReceive,
                ),
            })
            .with_child_transition_id(format!("{}:receive", transition.id))
            .with_execution_step_type(OrderExecutionStepType::CctpReceive);
            Ok(vec![burn, receive])
        }
        MarketOrderTransitionKind::HyperliquidBridgeDeposit
        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
            Ok(vec![QuoteLeg::new(QuoteLegSpec {
                transition_decl_id: &transition.id,
                transition_kind: transition.kind,
                provider: transition.provider,
                input_asset: &transition.input.asset,
                output_asset: &transition.output.asset,
                amount_in: &quote.amount_in,
                amount_out: &quote.amount_out,
                expires_at: quote.expires_at,
                raw: quote.provider_quote.clone(),
            })
            .with_execution_step_type(
                execution_step_type_for_transition_kind(transition.kind),
            )])
        }
        other => Err(refund_materialization_error(format!(
            "transition kind {} does not produce bridge quote legs",
            other.as_str()
        ))),
    }
}

pub(super) fn reserve_refund_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> Result<String, OrderActivityError> {
    let amount = normalize_refund_amount(field, value)?;
    let reserve = REFUND_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW.to_string();
    if !refund_amount_gt(amount, &reserve)? {
        return Err(refund_materialization_error(format!(
            "amount must exceed Hyperliquid spot token transfer gas reserve {reserve}"
        )));
    }
    subtract_refund_decimal_amount(amount, &reserve)
}

pub(super) fn subtract_refund_decimal_amount(
    value: &str,
    subtrahend: &str,
) -> Result<String, OrderActivityError> {
    let mut digits = normalize_refund_amount("refund.amount", value)?
        .as_bytes()
        .to_vec();
    let subtrahend = normalize_refund_amount("refund.subtrahend", subtrahend)?.as_bytes();
    let mut borrow = 0_i16;

    for offset in 0..digits.len() {
        let digit_index = digits.len() - 1 - offset;
        let lhs = i16::from(digits[digit_index] - b'0') - borrow;
        let rhs = subtrahend
            .len()
            .checked_sub(1 + offset)
            .map(|index| i16::from(subtrahend[index] - b'0'))
            .unwrap_or(0);
        let (next_digit, next_borrow) = if lhs < rhs {
            (lhs + 10 - rhs, 1)
        } else {
            (lhs - rhs, 0)
        };
        digits[digit_index] = b'0'
            + u8::try_from(next_digit)
                .map_err(|err| amount_parse_error("refund amount subtraction digit", err))?;
        borrow = next_borrow;
    }

    if borrow != 0 {
        return Err(refund_materialization_error(
            "Hyperliquid spot token transfer gas reserve exceeded amount",
        ));
    }
    let raw = String::from_utf8(digits)
        .map_err(|err| amount_parse_error("refund amount subtraction utf8", err))?;
    let normalized = raw.trim_start_matches('0');
    Ok(if normalized.is_empty() {
        "0".to_string()
    } else {
        normalized.to_string()
    })
}

pub(super) fn cctp_refund_quote_leg_raw(
    provider_quote: &Value,
    step_type: OrderExecutionStepType,
) -> Value {
    let mut raw = provider_quote.clone();
    set_json_value(&mut raw, "bridge_kind", json!("cctp_bridge"));
    set_json_value(
        &mut raw,
        "execution_step_type",
        json!(step_type.to_db_string()),
    );
    set_json_value(
        &mut raw,
        "kind",
        json!(match step_type {
            OrderExecutionStepType::CctpBurn => "cctp_burn",
            OrderExecutionStepType::CctpReceive => "cctp_receive",
            _ => "cctp_bridge",
        }),
    );
    raw
}

pub(super) fn refund_take_one_leg(
    legs: &[QuoteLeg],
    transition: &TransitionDecl,
    step_type: OrderExecutionStepType,
) -> Result<QuoteLeg, OrderActivityError> {
    let mut matches = legs
        .iter()
        .filter(|leg| {
            leg.parent_transition_id() == transition.id
                && leg.transition_kind == transition.kind
                && leg.execution_step_type == step_type
        })
        .cloned()
        .collect::<Vec<_>>();
    if matches.len() != 1 {
        return Err(refund_materialization_error(format!(
            "refund transition {} ({}) expected exactly one {:?} leg, found {}",
            transition.id,
            transition.kind.as_str(),
            step_type,
            matches.len()
        )));
    }
    Ok(matches.remove(0))
}

pub(super) fn refund_legs_for_transition(
    legs: &[QuoteLeg],
    transition: &TransitionDecl,
) -> Vec<QuoteLeg> {
    legs.iter()
        .filter(|leg| {
            leg.parent_transition_id() == transition.id && leg.transition_kind == transition.kind
        })
        .cloned()
        .collect()
}

pub(super) fn append_refund_transition_plan(
    order: &RouterOrder,
    transition: &TransitionDecl,
    quote_legs: Vec<QuoteLeg>,
    mut transition_steps: Vec<OrderExecutionStep>,
    leg_index: i32,
    planned_at: chrono::DateTime<Utc>,
    execution_legs: &mut Vec<OrderExecutionLeg>,
    steps: &mut Vec<OrderExecutionStep>,
) -> Result<(), OrderActivityError> {
    let leg = refund_execution_leg_from_quote_legs(
        order,
        transition,
        &quote_legs,
        leg_index,
        planned_at,
    )?;
    let leg_id = leg.id;
    for step in &mut transition_steps {
        step.execution_leg_id = Some(leg_id);
    }
    execution_legs.push(leg);
    steps.extend(transition_steps);
    Ok(())
}

pub(super) fn refund_transition_across_bridge_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (depositor_id, depositor_address, depositor_role, refund_id, refund_address, refund_role) =
        match source {
            RefundExternalSourceBinding::Explicit {
                vault_id,
                address,
                role,
            } => (
                json!(vault_id),
                json!(address.clone()),
                json!(role.to_db_string()),
                json!(vault_id),
                json!(address),
                json!(role.to_db_string()),
            ),
            RefundExternalSourceBinding::DerivedDestinationExecution => (
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
                Value::Null,
                Value::Null,
                json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            ),
        };
    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund", order.id)),
        request: json!({
            "order_id": order.id,
            "origin_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "refund_address": refund_address,
            "refund_custody_vault_id": refund_id,
            "refund_custody_vault_role": refund_role,
            "partial_fills_enabled": false,
            "depositor_address": depositor_address,
            "depositor_custody_vault_id": depositor_id,
            "depositor_custody_vault_role": depositor_role,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "external_custody_across_bridge",
            "transition_kind": transition.kind.as_str(),
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "depositor_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "depositor_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "recipient_address": if is_final { json!(&order.refund_address) } else { Value::Null },
        }),
        planned_at,
    }))
}

pub(super) fn refund_transition_cctp_bridge_steps(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    burn_leg: &QuoteLeg,
    receive_leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<Vec<OrderExecutionStep>, OrderActivityError> {
    let provider = cctp_refund_provider(transition, burn_leg, receive_leg)?;
    let amount_in = burn_leg.amount_in.clone();
    let amount_out = receive_leg.amount_out.clone();
    let max_fee = cctp_refund_max_fee(burn_leg)?;
    let source_for_details = source.clone();
    let burn = refund_transition_cctp_burn_step(
        CctpBurnRefundStepSpec {
            order,
            transition,
            burn_leg,
            provider: &provider,
            amount_in,
            max_fee,
            is_final,
            step_index,
            planned_at,
        },
        source,
        &source_for_details,
    );
    let receive = refund_transition_cctp_receive_step(
        order,
        transition,
        receive_leg,
        provider,
        amount_out,
        is_final,
        step_index,
        planned_at,
    );
    Ok(vec![burn, receive])
}

fn cctp_refund_provider(
    transition: &TransitionDecl,
    burn_leg: &QuoteLeg,
    receive_leg: &QuoteLeg,
) -> Result<String, OrderActivityError> {
    let provider = refund_leg_provider(burn_leg, transition)?
        .as_str()
        .to_string();
    let receive_provider = refund_leg_provider(receive_leg, transition)?;
    if receive_provider.as_str() != provider {
        return Err(refund_materialization_error(format!(
            "CCTP receive leg provider {} does not match burn leg provider {} for transition {}",
            receive_provider.as_str(),
            provider,
            transition.id
        )));
    }
    Ok(provider)
}

fn cctp_refund_max_fee(burn_leg: &QuoteLeg) -> Result<String, OrderActivityError> {
    burn_leg
        .raw
        .get("max_fee")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            refund_materialization_error(format!(
                "CCTP burn leg {} missing raw.max_fee",
                burn_leg.transition_decl_id
            ))
        })
        .map(ToString::to_string)
}

struct CctpBurnRefundStepSpec<'a> {
    order: &'a RouterOrder,
    transition: &'a TransitionDecl,
    burn_leg: &'a QuoteLeg,
    provider: &'a str,
    amount_in: String,
    max_fee: String,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
}

fn refund_transition_cctp_burn_step(
    spec: CctpBurnRefundStepSpec<'_>,
    source: RefundExternalSourceBinding,
    source_for_details: &RefundExternalSourceBinding,
) -> OrderExecutionStep {
    let CctpBurnRefundStepSpec {
        order,
        transition,
        burn_leg,
        provider,
        amount_in,
        max_fee,
        is_final,
        step_index,
        planned_at,
    } = spec;
    let (source_id, source_address, source_role) = refund_external_source_request_parts(source);
    let (source_detail_role, source_detail_status) =
        refund_external_source_detail_parts(source_for_details);
    refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(burn_leg.transition_decl_id.clone()),
        step_index,
        step_type: OrderExecutionStepType::CctpBurn,
        provider: provider.to_string(),
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund:cctp", order.id)),
        request: json!({
            "order_id": order.id,
            "transition_decl_id": transition.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "source_custody_vault_id": source_id,
            "source_custody_vault_address": source_address,
            "source_custody_vault_role": source_role,
            "max_fee": max_fee,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_cctp_bridge",
            "source_custody_vault_role": source_detail_role,
            "source_custody_vault_status": source_detail_status,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "receive_step_index": step_index + 1,
        }),
        planned_at,
    })
}

fn refund_transition_cctp_receive_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    receive_leg: &QuoteLeg,
    provider: String,
    amount_out: String,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> OrderExecutionStep {
    refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(receive_leg.transition_decl_id.clone()),
        step_index: step_index + 1,
        step_type: OrderExecutionStepType::CctpReceive,
        provider,
        input_asset: Some(transition.output.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_out.clone()),
        min_amount_out: None,
        provider_ref: Some(format!("order:{}:external-refund:cctp", order.id)),
        request: json!({
            "order_id": order.id,
            "burn_transition_decl_id": transition.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "source_custody_vault_id": null,
            "source_custody_vault_address": null,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "message": "",
            "attestation": "",
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_cctp_bridge",
            "burn_step_index": step_index,
            "source_custody_vault_role": CustodyVaultRole::DestinationExecution.to_db_string(),
            "source_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    })
}

fn refund_external_source_request_parts(
    source: RefundExternalSourceBinding,
) -> (Value, Value, Value) {
    match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    }
}

fn refund_external_source_detail_parts(source: &RefundExternalSourceBinding) -> (Value, Value) {
    match source {
        RefundExternalSourceBinding::Explicit { role, .. } => {
            (json!(role.to_db_string()), json!("bound"))
        }
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            json!("pending_derivation"),
        ),
    }
}

pub(super) fn refund_transition_universal_router_swap_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let swap_leg = &leg.raw;
    let order_kind = refund_required_str(swap_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let input_asset = leg
        .input_deposit_asset()
        .map_err(refund_materialization_error)?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(refund_materialization_error)?;
    let input_decimals = refund_required_u8(swap_leg, "src_decimals")?;
    let output_decimals = refund_required_u8(swap_leg, "dest_decimals")?;
    let price_route = swap_leg.get("price_route").cloned().ok_or_else(|| {
        refund_materialization_error("universal router refund swap leg missing price_route")
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
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = refund_external_source_request_parts(source);
    let (source_detail_role, source_detail_status) =
        refund_external_source_detail_parts(&source_for_details);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UniversalRouterSwap,
        provider,
        input_asset: Some(input_asset.clone()),
        output_asset: Some(output_asset.clone()),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: Some(format!("order:{}:external-refund:swap", order.id)),
        request: json!({
            "order_id": order.id,
            "quote_id": Uuid::now_v7(),
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
            "source_custody_vault_id": source_id,
            "source_custody_vault_address": source_address,
            "source_custody_vault_role": source_role,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_id": null,
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "external_custody_universal_router_swap",
            "source_custody_vault_role": source_detail_role,
            "source_custody_vault_status": source_detail_status,
            "recipient_address": if is_final { json!(&order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
        }),
        planned_at,
    }))
}

pub(super) fn refund_transition_unit_deposit_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role, revert_id, revert_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(vault_id),
            json!(role.to_db_string()),
        ),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UnitDeposit,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: None,
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "src_chain_id": transition.input.asset.chain.as_str(),
            "dst_chain_id": "hyperliquid",
            "asset_id": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
            "revert_custody_vault_id": revert_id,
            "revert_custody_vault_role": revert_role,
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_id": null,
            "hyperliquid_custody_vault_address": null,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
            "hyperliquid_custody_vault_role": CustodyVaultRole::HyperliquidSpot.to_db_string(),
            "hyperliquid_custody_vault_status": "pending_derivation",
        }),
        planned_at,
    }))
}

pub(super) fn refund_transition_hyperliquid_bridge_deposit_step(
    order: &RouterOrder,
    source: RefundExternalSourceBinding,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let source_for_details = source.clone();
    let (source_id, source_address, source_role) = match source {
        RefundExternalSourceBinding::Explicit {
            vault_id,
            address,
            role,
        } => (json!(vault_id), json!(address), json!(role.to_db_string())),
        RefundExternalSourceBinding::DerivedDestinationExecution => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
        ),
    };

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeDeposit,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: None,
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "source_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "amount": amount_in,
            "source_custody_vault_id": source_id,
            "source_custody_vault_role": source_role,
            "source_custody_vault_address": source_address,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "source_custody_vault_role": match &source_for_details {
                RefundExternalSourceBinding::Explicit { role, .. } => json!(role.to_db_string()),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            },
            "source_custody_vault_status": match &source_for_details {
                RefundExternalSourceBinding::Explicit { .. } => json!("bound"),
                RefundExternalSourceBinding::DerivedDestinationExecution => json!("pending_derivation"),
            },
        }),
        planned_at,
    }))
}

pub(super) fn refund_transition_hyperliquid_bridge_withdrawal_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    custody: &RefundHyperliquidBinding,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let amount_out = leg.amount_out.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);
    let transfer_from_spot = hyperliquid_binding_transfers_from_spot(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidBridgeWithdrawal,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in.clone()),
        min_amount_out: Some(amount_out),
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "destination_chain_id": transition.output.asset.chain.as_str(),
            "output_asset": transition.output.asset.asset.as_str(),
            "amount": amount_in,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "transfer_from_spot": transfer_from_spot,
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "transition_kind": transition.kind.as_str(),
            "refund_kind": "transition_path",
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

#[derive(Debug, Clone)]
pub(super) enum RefundHyperliquidBinding {
    Explicit {
        vault_id: Uuid,
        address: String,
        role: CustodyVaultRole,
        asset: Option<DepositAsset>,
    },
    DerivedSpot,
    DerivedDestinationExecution {
        asset: DepositAsset,
    },
}

pub(super) fn external_refund_hyperliquid_binding(
    vault: &CustodyVault,
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> Result<RefundHyperliquidBinding, OrderActivityError> {
    let prior_transitions = transitions.get(..transition_index).ok_or_else(|| {
        refund_materialization_error(format!(
            "refund transition index {transition_index} is out of bounds"
        ))
    })?;

    if transitions.first().map(|transition| transition.kind)
        == Some(MarketOrderTransitionKind::HyperliquidBridgeDeposit)
        && prior_transitions
            .iter()
            .all(|transition| transition.kind != MarketOrderTransitionKind::AcrossBridge)
    {
        let asset_id = vault.asset.clone().ok_or_else(|| {
            refund_materialization_error(format!(
                "external custody vault {} is missing asset for Hyperliquid binding",
                vault.id
            ))
        })?;
        return Ok(RefundHyperliquidBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: vault.role,
            asset: Some(DepositAsset {
                chain: vault.chain.clone(),
                asset: asset_id,
            }),
        });
    }

    if prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
    {
        return Ok(RefundHyperliquidBinding::DerivedSpot);
    }

    let Some(bridge_transition) = prior_transitions
        .iter()
        .find(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
    else {
        return Err(refund_materialization_error(format!(
            "refund transition {} has no preceding Hyperliquid custody source",
            transitions[transition_index].id
        )));
    };

    Ok(RefundHyperliquidBinding::DerivedDestinationExecution {
        asset: bridge_transition.input.asset.clone(),
    })
}

pub(super) fn refund_trade_prefund_from_withdrawable(
    transitions: &[TransitionDecl],
    transition_index: usize,
) -> bool {
    let prior_transitions = &transitions[..transition_index];
    prior_transitions
        .iter()
        .any(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit)
        && prior_transitions
            .iter()
            .filter(|transition| transition.kind == MarketOrderTransitionKind::HyperliquidTrade)
            .count()
            == 0
}

pub(super) fn hyperliquid_binding_request_parts(
    custody: &RefundHyperliquidBinding,
) -> (Value, Value, Value, Value, Value) {
    match custody {
        RefundHyperliquidBinding::Explicit {
            vault_id,
            address,
            role,
            asset,
        } => (
            json!(vault_id),
            json!(address),
            json!(role.to_db_string()),
            json!(asset.as_ref().map(|asset| asset.chain.as_str())),
            json!(asset.as_ref().map(|asset| asset.asset.as_str())),
        ),
        RefundHyperliquidBinding::DerivedSpot => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string()),
            Value::Null,
            Value::Null,
        ),
        RefundHyperliquidBinding::DerivedDestinationExecution { asset } => (
            Value::Null,
            Value::Null,
            json!(CustodyVaultRole::DestinationExecution.to_db_string()),
            json!(asset.chain.as_str()),
            json!(asset.asset.as_str()),
        ),
    }
}

pub(super) fn hyperliquid_binding_role(custody: &RefundHyperliquidBinding) -> Value {
    match custody {
        RefundHyperliquidBinding::Explicit { role, .. } => json!(role.to_db_string()),
        RefundHyperliquidBinding::DerivedSpot => {
            json!(CustodyVaultRole::HyperliquidSpot.to_db_string())
        }
        RefundHyperliquidBinding::DerivedDestinationExecution { .. } => {
            json!(CustodyVaultRole::DestinationExecution.to_db_string())
        }
    }
}

pub(super) fn hyperliquid_binding_status(custody: &RefundHyperliquidBinding) -> Value {
    match custody {
        RefundHyperliquidBinding::Explicit { .. } => json!("bound"),
        RefundHyperliquidBinding::DerivedSpot
        | RefundHyperliquidBinding::DerivedDestinationExecution { .. } => {
            json!("pending_derivation")
        }
    }
}

pub(super) fn hyperliquid_binding_transfers_from_spot(custody: &RefundHyperliquidBinding) -> bool {
    match custody {
        RefundHyperliquidBinding::Explicit { role, .. } => {
            *role == CustodyVaultRole::HyperliquidSpot
        }
        RefundHyperliquidBinding::DerivedSpot => true,
        RefundHyperliquidBinding::DerivedDestinationExecution { .. } => false,
    }
}

pub(super) struct RefundHyperliquidTradeStepSpec<'a> {
    pub(super) order: &'a RouterOrder,
    pub(super) transition: &'a TransitionDecl,
    pub(super) leg: &'a QuoteLeg,
    pub(super) custody: &'a RefundHyperliquidBinding,
    pub(super) prefund_from_withdrawable: bool,
    pub(super) refund_quote_id: Uuid,
    pub(super) leg_index: usize,
    pub(super) leg_count: usize,
    pub(super) step_index: i32,
    pub(super) planned_at: chrono::DateTime<Utc>,
}

pub(super) struct RefundPlannedStepSpec {
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
    request: Value,
    details: Value,
    planned_at: chrono::DateTime<Utc>,
}

pub(super) fn refund_transition_hyperliquid_trade_step(
    spec: RefundHyperliquidTradeStepSpec<'_>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let RefundHyperliquidTradeStepSpec {
        order,
        transition,
        leg,
        custody,
        prefund_from_withdrawable,
        refund_quote_id,
        leg_index,
        leg_count,
        step_index,
        planned_at,
    } = spec;
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let trade_leg = &leg.raw;
    let order_kind = refund_required_str(trade_leg, "order_kind")?;
    let amount_in = leg.amount_in.as_str();
    let amount_out = leg.amount_out.as_str();
    let min_amount_out = trade_leg
        .get("min_amount_out")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let max_amount_in = trade_leg
        .get("max_amount_in")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let input_asset = leg
        .input_deposit_asset()
        .map_err(refund_materialization_error)?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(refund_materialization_error)?;
    let request_input_asset = input_asset.clone();
    let request_output_asset = output_asset.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::HyperliquidTrade,
        provider,
        input_asset: Some(input_asset),
        output_asset: Some(output_asset),
        amount_in: Some(amount_in.to_string()),
        min_amount_out: min_amount_out.clone(),
        provider_ref: Some(format!("refund-quote-{refund_quote_id}")),
        request: json!({
            "order_id": order.id,
            "quote_id": refund_quote_id,
            "leg_index": leg_index,
            "leg_count": leg_count,
            "order_kind": order_kind,
            "amount_in": amount_in,
            "amount_out": amount_out,
            "min_amount_out": min_amount_out,
            "max_amount_in": max_amount_in,
            "input_asset": {
                "chain_id": request_input_asset.chain.as_str(),
                "asset": request_input_asset.asset.as_str(),
            },
            "output_asset": {
                "chain_id": request_output_asset.chain.as_str(),
                "asset": request_output_asset.asset.as_str(),
            },
            "prefund_from_withdrawable": prefund_from_withdrawable,
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "leg_index": leg_index,
            "leg_count": leg_count,
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

pub(super) fn refund_transition_unit_withdrawal_step(
    order: &RouterOrder,
    transition: &TransitionDecl,
    leg: &QuoteLeg,
    custody: &RefundHyperliquidBinding,
    is_final: bool,
    step_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionStep, OrderActivityError> {
    let provider = refund_leg_provider(leg, transition)?.as_str().to_string();
    let amount_in = leg.amount_in.clone();
    let amount_out = leg.amount_out.clone();
    let (vault_id, vault_address, vault_role, vault_chain_id, vault_asset_id) =
        hyperliquid_binding_request_parts(custody);

    Ok(refund_planned_step(RefundPlannedStepSpec {
        order_id: order.id.into(),
        transition_decl_id: Some(transition.id.clone()),
        step_index,
        step_type: OrderExecutionStepType::UnitWithdrawal,
        provider,
        input_asset: Some(transition.input.asset.clone()),
        output_asset: Some(transition.output.asset.clone()),
        amount_in: Some(amount_in),
        min_amount_out: Some("0".to_string()),
        provider_ref: None,
        request: json!({
            "order_id": order.id,
            "input_chain_id": transition.input.asset.chain.as_str(),
            "input_asset": transition.input.asset.asset.as_str(),
            "dst_chain_id": transition.output.asset.chain.as_str(),
            "asset_id": transition.output.asset.asset.as_str(),
            "amount": amount_out,
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_id": null,
            "min_amount_out": "0",
            "hyperliquid_custody_vault_role": vault_role,
            "hyperliquid_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": vault_address,
            "hyperliquid_custody_vault_chain_id": vault_chain_id,
            "hyperliquid_custody_vault_asset_id": vault_asset_id,
        }),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
            "recipient_address": if is_final { json!(order.refund_address) } else { Value::Null },
            "recipient_custody_vault_role": if is_final { Value::Null } else { json!(CustodyVaultRole::DestinationExecution.to_db_string()) },
            "recipient_custody_vault_status": if is_final { Value::Null } else { json!("pending_derivation") },
            "hyperliquid_custody_vault_role": hyperliquid_binding_role(custody),
            "hyperliquid_custody_vault_status": hyperliquid_binding_status(custody),
        }),
        planned_at,
    }))
}

pub(super) fn refund_planned_step(spec: RefundPlannedStepSpec) -> OrderExecutionStep {
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
        idempotency_key: None,
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

pub(super) fn refund_execution_leg_from_quote_legs(
    order: &RouterOrder,
    transition: &TransitionDecl,
    quote_legs: &[QuoteLeg],
    leg_index: i32,
    planned_at: chrono::DateTime<Utc>,
) -> Result<OrderExecutionLeg, OrderActivityError> {
    let first = quote_legs.first().ok_or_else(|| {
        refund_materialization_error(format!(
            "refund transition {} ({}) has no quoted legs to materialize",
            transition.id,
            transition.kind.as_str()
        ))
    })?;
    let last = quote_legs.last().ok_or_else(|| {
        refund_materialization_error(format!(
            "refund transition {} ({}) has no quoted legs to materialize",
            transition.id,
            transition.kind.as_str()
        ))
    })?;
    let input_asset = first
        .input_deposit_asset()
        .map_err(refund_materialization_error)?;
    let output_asset = last
        .output_deposit_asset()
        .map_err(refund_materialization_error)?;
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
        .map(ToString::to_string);

    Ok(OrderExecutionLeg {
        id: Uuid::now_v7(),
        order_id: order.id.into(),
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
        provider_quote_expires_at: quote_legs.iter().map(|leg| leg.expires_at).min(),
        details: json!({
            "schema_version": 1,
            "refund_kind": "transition_path",
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

pub(super) fn refund_exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    let provider_name = provider.as_str();
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    match kind {
        "spot_no_op" => Ok(vec![]),
        "universal_router_swap" => {
            refund_universal_router_quote_leg(transition_decl_id, transition_kind, provider, quote)
                .map(|leg| vec![leg])
        }
        "spot_cross_token" => refund_spot_cross_token_quote_legs(
            transition_decl_id,
            transition_kind,
            provider,
            provider_name,
            quote,
        ),
        other => Err(refund_materialization_error(format!(
            "unsupported refund exchange quote kind in transition path: {other:?}"
        ))),
    }
}

fn refund_universal_router_quote_leg(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<QuoteLeg, OrderActivityError> {
    let input_asset = required_refund_quote_asset(
        &quote.provider_quote,
        "input_asset",
        "refund universal router quote missing input_asset",
    )?;
    let output_asset = required_refund_quote_asset(
        &quote.provider_quote,
        "output_asset",
        "refund universal router quote missing output_asset",
    )?;
    Ok(QuoteLeg {
        transition_decl_id: transition_decl_id.to_string(),
        transition_parent_decl_id: transition_decl_id.to_string(),
        transition_kind,
        execution_step_type: execution_step_type_for_transition_kind(transition_kind),
        provider,
        input_asset,
        output_asset,
        amount_in: quote.amount_in.clone(),
        amount_out: quote.amount_out.clone(),
        expires_at: quote.expires_at,
        raw: quote.provider_quote.clone(),
    })
}

fn refund_spot_cross_token_quote_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    provider_name: &str,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
        return Err(refund_materialization_error(format!(
            "refund exchange quote from {provider_name} missing spot_cross_token legs"
        )));
    };
    legs.iter()
        .enumerate()
        .map(|(index, leg)| {
            let input_asset = required_refund_quote_asset(
                leg,
                "input_asset",
                "refund hyperliquid quote leg missing input_asset",
            )?;
            let output_asset = required_refund_quote_asset(
                leg,
                "output_asset",
                "refund hyperliquid quote leg missing output_asset",
            )?;
            Ok(QuoteLeg {
                transition_decl_id: format!("{transition_decl_id}:leg:{index}"),
                transition_parent_decl_id: transition_decl_id.to_string(),
                transition_kind,
                execution_step_type: execution_step_type_for_transition_kind(transition_kind),
                provider,
                input_asset,
                output_asset,
                amount_in: required_refund_quote_leg_amount(leg, "amount_in")?,
                amount_out: required_refund_quote_leg_amount(leg, "amount_out")?,
                expires_at: quote.expires_at,
                raw: leg.clone(),
            })
        })
        .collect()
}

fn required_refund_quote_asset(
    value: &Value,
    field: &'static str,
    missing_message: &'static str,
) -> Result<QuoteLegAsset, OrderActivityError> {
    value
        .get(field)
        .ok_or_else(|| refund_materialization_error(missing_message))
        .and_then(|value| {
            QuoteLegAsset::from_value(value, field).map_err(refund_materialization_error)
        })
}

pub(super) fn required_refund_quote_leg_amount(
    leg: &Value,
    field: &'static str,
) -> Result<String, OrderActivityError> {
    let Some(amount) = leg.get(field).and_then(Value::as_str) else {
        return Err(refund_materialization_error(format!(
            "refund hyperliquid quote leg missing {field}"
        )));
    };
    if amount.is_empty() {
        return Err(refund_materialization_error(format!(
            "refund hyperliquid quote leg has empty {field}"
        )));
    }
    Ok(amount.to_string())
}

pub(super) fn refund_leg_provider(
    leg: &QuoteLeg,
    transition: &TransitionDecl,
) -> Result<ProviderId, OrderActivityError> {
    if leg.provider != transition.provider {
        return Err(refund_materialization_error(format!(
            "refund quote leg provider {} does not match transition provider {}",
            leg.provider.as_str(),
            transition.provider.as_str()
        )));
    }
    Ok(leg.provider)
}

pub(super) fn refund_required_str<'a>(
    value: &'a Value,
    key: &'static str,
) -> Result<&'a str, OrderActivityError> {
    value.get(key).and_then(Value::as_str).ok_or_else(|| {
        refund_materialization_error(format!("refund quote leg missing string field {key}"))
    })
}

pub(super) fn refund_required_u8(
    value: &Value,
    key: &'static str,
) -> Result<u8, OrderActivityError> {
    let raw = value.get(key).and_then(Value::as_u64).ok_or_else(|| {
        refund_materialization_error(format!("refund quote leg missing numeric field {key}"))
    })?;
    u8::try_from(raw).map_err(|source| {
        refund_materialization_error(format!(
            "refund quote leg field {key} does not fit u8: {source}"
        ))
    })
}

pub(super) fn hyperliquid_refund_balance_amount_raw(
    total: &str,
    hold: &str,
    decimals: u8,
) -> Result<Option<String>, OrderActivityError> {
    let total_raw = decimal_string_to_raw_digits(total, decimals).map_err(|message| {
        amount_parse_error(
            "Hyperliquid refund total balance",
            format!("invalid Hyperliquid refund total balance: {message}"),
        )
    })?;
    let hold_raw = decimal_string_to_raw_digits(hold, decimals).map_err(|message| {
        amount_parse_error(
            "Hyperliquid refund hold balance",
            format!("invalid Hyperliquid refund hold balance: {message}"),
        )
    })?;
    if total_raw == "0" || hold_raw != "0" {
        return Ok(None);
    }
    Ok(Some(total_raw))
}

pub(super) fn decimal_string_to_raw_digits(value: &str, decimals: u8) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let mut parts = trimmed.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || !frac.chars().all(|ch| ch.is_ascii_digit())
        || frac.len() > usize::from(decimals)
    {
        return Err(format!("invalid decimal amount {value:?}"));
    }
    let combined = format!("{whole}{:0<width$}", frac, width = usize::from(decimals));
    let digits = combined.trim_start_matches('0');
    Ok(if digits.is_empty() { "0" } else { digits }.to_string())
}

// Refund-side stale quote refresh is intentionally not implemented. Scar §7
// preserves the legacy rule that RefundRecovery attempts never stale-refresh:
// partial-execution funds may be mid-flight during a refund, so re-quoting is
// unsafe. A stale refund quote routes to RefundManualInterventionRequired via
// the existing classify_step_failure path, matching
// bin/router-server/src/services/order_executor.rs.
