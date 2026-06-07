#![allow(clippy::too_many_arguments)]

use super::*;
use router_core::services::action_providers::UnitProvider;
use router_core::services::asset_registry::{AssetRegistry, RequiredCustodyRole};
use router_core::services::gas_reimbursement::{
    optimized_paymaster_reimbursement_plan, optimized_paymaster_reimbursement_plan_with_pricing,
    try_transition_retention_amount, GasReimbursementError, GasReimbursementPlan,
};
use router_core::services::market_order_planner::MarketOrderInitialHyperliquidCustody;
use super::super::types::{LegBoundaryRequoteAttemptOutcome, LegBoundaryRequoteAttemptShape};

pub(super) fn refreshed_market_order_quote_exact_in(
    order: &RouterOrder,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    legs: Vec<QuoteLeg>,
    amount_in: String,
    amount_out: String,
    expires_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        source_asset: original_quote.source_asset.clone(),
        destination_asset: original_quote.destination_asset.clone(),
        recipient_address: original_quote.recipient_address.clone(),
        provider_id: original_quote.provider_id.clone(),
        amount_in,
        estimated_amount_out: amount_out,
        provider_quote: json!({
            "schema_version": 2,
            "planner": "transition_decl_v1",
            "path_id": original_quote
                .provider_quote
                .get("path_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| transitions
                    .iter()
                    .map(|transition| transition.id.as_str())
                    .collect::<Vec<_>>()
                    .join(">")),
            "transition_decl_ids": transitions
                .iter()
                .map(|transition| transition.id.clone())
                .collect::<Vec<_>>(),
            "transitions": transitions,
            "legs": legs,
            "gas_reimbursement": original_quote
                .provider_quote
                .get("gas_reimbursement")
                .cloned()
                .unwrap_or_else(|| json!({ "retention_actions": [] })),
            "refresh": {
                "schema_version": 1,
                "source_quote_id": original_quote.id,
                "source_quote_expires_at": original_quote.expires_at.to_rfc3339(),
            },
        }),
        usd_valuation: json!({}),
        expected_swap_time_ms: original_quote.expected_swap_time_ms,
        expires_at,
        created_at,
    }
}

pub(super) fn refresh_bridge_quote_transition_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    if transition.kind == MarketOrderTransitionKind::CctpBridge {
        return Ok(refresh_cctp_quote_transition_legs(transition, quote));
    }
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
    })])
}

pub(super) fn refresh_exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    if kind == "spot_cross_token" {
        return refresh_spot_cross_token_quote_transition_legs(
            transition_decl_id,
            transition_kind,
            provider,
            quote,
        );
    }
    if kind == "spot_transfer" {
        return refresh_spot_transfer_quote_transition_legs(
            transition_decl_id,
            transition_kind,
            provider,
            quote,
        );
    }
    if kind != "universal_router_swap" {
        return Err(refresh_materialization_error(format!(
            "unsupported refreshed exchange quote kind {kind:?}"
        )));
    }
    let input_asset = quote
        .provider_quote
        .get("input_asset")
        .ok_or_else(|| refresh_materialization_error("universal router quote missing input_asset"))
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "input_asset").map_err(refresh_materialization_error)
        })?;
    let output_asset = quote
        .provider_quote
        .get("output_asset")
        .ok_or_else(|| refresh_materialization_error("universal router quote missing output_asset"))
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "output_asset").map_err(refresh_materialization_error)
        })?;
    Ok(vec![QuoteLeg {
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
    }])
}

pub(super) fn refresh_cctp_quote_transition_legs(
    transition: &TransitionDecl,
    quote: &BridgeQuote,
) -> Vec<QuoteLeg> {
    let burn = QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: &quote.amount_in,
        amount_out: &quote.amount_out,
        expires_at: quote.expires_at,
        raw: refresh_cctp_quote_phase_raw(&quote.provider_quote, OrderExecutionStepType::CctpBurn),
    })
    .with_execution_step_type(OrderExecutionStepType::CctpBurn);

    let receive = QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.output.asset,
        output_asset: &transition.output.asset,
        amount_in: &quote.amount_out,
        amount_out: &quote.amount_out,
        expires_at: quote.expires_at,
        raw: refresh_cctp_quote_phase_raw(
            &quote.provider_quote,
            OrderExecutionStepType::CctpReceive,
        ),
    })
    .with_child_transition_id(format!("{}:receive", transition.id))
    .with_execution_step_type(OrderExecutionStepType::CctpReceive);

    vec![burn, receive]
}

pub(super) fn refresh_cctp_quote_phase_raw(
    provider_quote: &Value,
    execution_step_type: OrderExecutionStepType,
) -> Value {
    let mut raw = provider_quote.clone();
    set_json_value(&mut raw, "bridge_kind", json!("cctp_bridge"));
    set_json_value(&mut raw, "kind", json!(execution_step_type.to_db_string()));
    set_json_value(
        &mut raw,
        "execution_step_type",
        json!(execution_step_type.to_db_string()),
    );
    raw
}
pub(super) fn refresh_spot_transfer_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    let leg = quote.provider_quote.get("leg").ok_or_else(|| {
        refresh_materialization_error("hyperliquid spot_transfer quote missing leg")
    })?;
    let input_asset = leg
        .get("input_asset")
        .ok_or_else(|| {
            refresh_materialization_error("hyperliquid spot_transfer leg missing input_asset")
        })
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "input_asset").map_err(refresh_materialization_error)
        })?;
    let output_asset = leg
        .get("output_asset")
        .ok_or_else(|| {
            refresh_materialization_error("hyperliquid spot_transfer leg missing output_asset")
        })
        .and_then(|value| {
            QuoteLegAsset::from_value(value, "output_asset").map_err(refresh_materialization_error)
        })?;

    Ok(vec![QuoteLeg {
        transition_decl_id: transition_decl_id.to_string(),
        transition_parent_decl_id: transition_decl_id.to_string(),
        transition_kind,
        execution_step_type: execution_step_type_for_transition_kind(transition_kind),
        provider,
        input_asset,
        output_asset,
        amount_in: refresh_required_quote_leg_amount(provider.as_str(), leg, "amount_in")?,
        amount_out: refresh_required_quote_leg_amount(provider.as_str(), leg, "amount_out")?,
        expires_at: quote.expires_at,
        raw: leg.clone(),
    }])
}

pub(super) fn refresh_spot_cross_token_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> Result<Vec<QuoteLeg>, OrderActivityError> {
    let provider_name = provider.as_str();
    let legs = quote
        .provider_quote
        .get("legs")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            refresh_materialization_error("exchange quote missing spot_cross_token legs")
        })?;
    legs.iter()
        .enumerate()
        .map(|(index, leg)| {
            let input_asset = leg
                .get("input_asset")
                .ok_or_else(|| {
                    refresh_materialization_error("hyperliquid quote leg missing input_asset")
                })
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "input_asset")
                        .map_err(refresh_materialization_error)
                })?;
            let output_asset = leg
                .get("output_asset")
                .ok_or_else(|| {
                    refresh_materialization_error("hyperliquid quote leg missing output_asset")
                })
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "output_asset")
                        .map_err(refresh_materialization_error)
                })?;
            Ok(QuoteLeg {
                transition_decl_id: format!("{transition_decl_id}:leg:{index}"),
                transition_parent_decl_id: transition_decl_id.to_string(),
                transition_kind,
                execution_step_type: execution_step_type_for_transition_kind(transition_kind),
                provider,
                input_asset,
                output_asset,
                amount_in: refresh_required_quote_leg_amount(provider_name, leg, "amount_in")?,
                amount_out: refresh_required_quote_leg_amount(provider_name, leg, "amount_out")?,
                expires_at: quote.expires_at,
                raw: leg.clone(),
            })
        })
        .collect()
}

pub(super) fn refresh_required_quote_leg_amount(
    provider: &str,
    leg: &Value,
    field: &'static str,
) -> Result<String, OrderActivityError> {
    let amount = leg.get(field).and_then(Value::as_str).ok_or_else(|| {
        refresh_materialization_error(format!("{provider} quote leg missing {field}"))
    })?;
    if amount.is_empty() {
        return Err(refresh_materialization_error(format!(
            "{provider} quote leg has empty {field}"
        )));
    }
    Ok(amount.to_string())
}

pub(super) fn refresh_unit_deposit_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    expires_at: chrono::DateTime<Utc>,
    raw: Value,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw,
    })
    .with_execution_step_type(OrderExecutionStepType::UnitDeposit)
}

pub(super) fn refresh_unit_withdrawal_quote_leg(
    transition: &TransitionDecl,
    amount: &str,
    recipient_address: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: amount,
        amount_out: amount,
        expires_at,
        raw: refresh_unit_withdrawal_quote_raw(recipient_address),
    })
    .with_execution_step_type(OrderExecutionStepType::UnitWithdrawal)
}

pub(super) fn refresh_unit_withdrawal_quote_raw(recipient_address: &str) -> Value {
    json!({
        "recipient_address": recipient_address,
        "hyperliquid_core_activation_fee": refresh_hyperliquid_core_activation_fee_quote_json(),
    })
}

pub(super) fn refresh_hyperliquid_core_activation_fee_quote_json() -> Value {
    json!({
        "kind": "first_transfer_to_new_hypercore_destination",
        "quote_asset": "USDC",
        "amount_raw": REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW.to_string(),
        "amount_decimal": "1",
        "source": "hyperliquid_activation_gas_fee",
    })
}

pub(super) fn refresh_unit_deposit_fee_reserve(
    quote: &MarketOrderQuote,
    transition: &TransitionDecl,
) -> Result<Option<U256>, OrderActivityError> {
    if transition.kind != MarketOrderTransitionKind::UnitDeposit {
        return Ok(None);
    }
    let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
        return Ok(None);
    };
    for leg in legs {
        let parsed: QuoteLeg = serde_json::from_value(leg.clone()).map_err(|source| {
            OrderActivityError::serialization("reading stored quote leg", source)
        })?;
        if parsed.parent_transition_id() != transition.id {
            continue;
        }
        let Some(amount) = parsed
            .raw
            .get("source_fee_reserve")
            .and_then(|value| value.get("amount"))
            .and_then(Value::as_str)
        else {
            return Ok(None);
        };
        return refresh_parse_u256_amount("source_fee_reserve.amount", amount).map(Some);
    }
    Ok(None)
}

pub(super) fn refresh_bitcoin_fee_reserve_quote(fee_reserve: U256) -> Value {
    json!({
        "source_fee_reserve": {
            "kind": "bitcoin_miner_fee",
            "chain_id": "bitcoin",
            "asset": AssetId::Native.as_str(),
            "amount": fee_reserve.to_string(),
            "reserve_bps": REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS,
        }
    })
}

pub(super) fn refresh_parse_u256_amount(
    field: &'static str,
    value: &str,
) -> Result<U256, OrderActivityError> {
    if value.is_empty() || !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(amount_parse_error(
            field,
            format!("{field} must be a raw unsigned integer: {value:?}"),
        ));
    }
    U256::from_str_radix(value, 10).map_err(|source| amount_parse_error(field, source))
}

pub(super) fn refresh_subtract_amount(
    field: &'static str,
    gross_amount: &str,
    subtract: U256,
    transition_id: &str,
    label: &'static str,
) -> Result<String, OrderActivityError> {
    let gross = refresh_parse_u256_amount(field, gross_amount)?;
    if subtract == U256::ZERO {
        return Ok(gross_amount.to_string());
    }
    if gross <= subtract {
        return Err(refresh_untenable_error(format!(
            "{field} must exceed {label} {subtract} for transition {transition_id}"
        )));
    }
    gross
        .checked_sub(subtract)
        .ok_or_else(|| {
            refresh_materialization_error(format!(
                "{field} minus {label} underflowed for transition {transition_id}"
            ))
        })
        .map(|amount| amount.to_string())
}

pub(super) fn refresh_reserve_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> Result<String, OrderActivityError> {
    refresh_subtract_amount(
        field,
        value,
        U256::from(REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW),
        "hyperliquid_trade",
        "Hyperliquid spot token transfer gas reserve",
    )
}

pub(super) fn refresh_remaining_exact_in_amount(
    legs: &[OrderExecutionLeg],
    attempts: &[OrderExecutionAttempt],
    stale_attempt: &OrderExecutionAttempt,
    stale_leg: &OrderExecutionLeg,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    start_transition_index: usize,
) -> Result<String, String> {
    let Some(previous_transition_index) = start_transition_index.checked_sub(1) else {
        return Ok(original_quote.amount_in.clone());
    };
    let previous_transition = transitions.get(previous_transition_index).ok_or_else(|| {
        format!(
            "refresh previous transition index {previous_transition_index} is outside transition path of length {}",
            transitions.len()
        )
    })?;
    let previous = legs
        .iter()
        .filter(|leg| {
            let Some(attempt_id) = leg.execution_attempt_id else {
                return false;
            };
            let Some(attempt) = attempts.iter().find(|attempt| attempt.id == attempt_id) else {
                return false;
            };
            // Keep refund-recovery attempts out of stale-quote refresh accounting;
            // scar §7 treats refund re-quotes as unsafe once funds may be mid-flight.
            attempt.attempt_kind != OrderExecutionAttemptKind::RefundRecovery
                && attempt.attempt_index <= stale_attempt.attempt_index
                && leg.transition_decl_id.as_deref() == Some(previous_transition.id.as_str())
                && leg.output_asset == stale_leg.input_asset
        })
        .max_by(|left, right| {
            let left_attempt = attempts
                .iter()
                .find(|attempt| Some(attempt.id) == left.execution_attempt_id);
            let right_attempt = attempts
                .iter()
                .find(|attempt| Some(attempt.id) == right.execution_attempt_id);
            left_attempt
                .map(|attempt| attempt.attempt_index)
                .cmp(&right_attempt.map(|attempt| attempt.attempt_index))
                .then_with(|| left.completed_at.cmp(&right.completed_at))
                .then_with(|| left.updated_at.cmp(&right.updated_at))
                .then_with(|| left.created_at.cmp(&right.created_at))
        })
        .ok_or_else(|| {
            format!(
                "cannot refresh leg {} because transition {} has no completed prior leg for {} {}",
                stale_leg.id,
                previous_transition.id,
                stale_leg.input_asset.chain,
                stale_leg.input_asset.asset
            )
        })?;
    if previous.status != OrderExecutionStepStatus::Completed {
        return Err(format!(
            "cannot refresh leg {} because previous leg {} is {}",
            stale_leg.id,
            previous.id,
            previous.status.to_db_string()
        ));
    }
    previous.actual_amount_out.clone().ok_or_else(|| {
        format!(
            "cannot refresh leg {} because previous completed leg {} has no actual_amount_out",
            stale_leg.id, previous.id
        )
    })
}

pub(super) fn refresh_step_request_string(
    steps: &[OrderExecutionStep],
    transition: &TransitionDecl,
    keys: &[&'static str],
) -> Option<String> {
    steps
        .iter()
        .filter(|step| refresh_step_matches_transition(step, transition))
        .find_map(|step| {
            keys.iter().find_map(|key| {
                step.request
                    .get(*key)
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(ToString::to_string)
            })
        })
}

pub(super) fn refresh_step_matches_transition(
    step: &OrderExecutionStep,
    transition: &TransitionDecl,
) -> bool {
    let Some(step_transition_id) = step.transition_decl_id.as_deref() else {
        return false;
    };
    step_transition_id == transition.id
        || step_transition_id
            .strip_prefix(transition.id.as_str())
            .is_some_and(|suffix| suffix.starts_with(':'))
}

pub(super) fn refresh_source_address(
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, OrderActivityError> {
    let transition = &transitions[index];
    if let Some(value) = refresh_step_request_string(
        steps,
        transition,
        &[
            "depositor_address",
            "source_custody_vault_address",
            "hyperliquid_custody_vault_address",
        ],
    ) {
        return Ok(value);
    }
    if index == 0 {
        return Ok(source_vault.deposit_vault_address.clone());
    }
    Err(refresh_materialization_error(format!(
        "cannot refresh transition {} because no source address is materialized",
        transition.id
    )))
}

pub(super) fn refresh_bridge_recipient_address(
    order: &RouterOrder,
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, OrderActivityError> {
    let transition = &transitions[index];
    if transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit {
        return refresh_hyperliquid_bridge_deposit_account_address(
            source_vault,
            steps,
            transition,
            index,
        );
    }
    refresh_recipient_address(order, steps, transitions, index)
}

pub(super) fn refresh_recipient_address(
    order: &RouterOrder,
    steps: &[OrderExecutionStep],
    transitions: &[TransitionDecl],
    index: usize,
) -> Result<String, OrderActivityError> {
    let transition = &transitions[index];
    if let Some(value) =
        refresh_step_request_string(steps, transition, &["recipient", "recipient_address"])
    {
        return Ok(value);
    }
    if index + 1 == transitions.len() {
        return Ok(order.recipient_address.clone());
    }
    Err(refresh_materialization_error(format!(
        "cannot refresh transition {} because no recipient address is materialized",
        transition.id
    )))
}

pub(super) fn refresh_hyperliquid_bridge_deposit_account_address(
    source_vault: &DepositVault,
    steps: &[OrderExecutionStep],
    transition: &TransitionDecl,
    index: usize,
) -> Result<String, OrderActivityError> {
    if let Some(value) = refresh_step_request_string(
        steps,
        transition,
        &[
            "source_custody_vault_address",
            "hyperliquid_custody_vault_address",
            "depositor_address",
        ],
    ) {
        return Ok(value);
    }
    if index == 0 {
        return Ok(source_vault.deposit_vault_address.clone());
    }
    Err(refresh_materialization_error(format!(
        "cannot refresh transition {} because no Hyperliquid account/source address is materialized",
        transition.id
    )))
}

pub(super) fn stale_quote_refresh_untenable(
    order_id: Uuid,
    stale_attempt_id: Uuid,
    failed_step_id: Uuid,
    message: impl ToString,
) -> RefreshedQuoteAttemptShape {
    RefreshedQuoteAttemptShape {
        outcome: RefreshedQuoteAttemptOutcome::Untenable {
            order_id: order_id.into(),
            stale_attempt_id: stale_attempt_id.into(),
            failed_step_id: failed_step_id.into(),
            reason: StaleQuoteRefreshUntenableReason::StaleProviderQuoteRefreshUntenable {
                message: message.to_string(),
            },
        },
    }
}

pub(super) fn set_json_value(target: &mut Value, key: &'static str, value: Value) {
    if !target.is_object() {
        *target = json!({});
    }
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
}

#[derive(Debug, Clone)]
struct BoundaryComposedMarketOrderQuote {
    provider_id: String,
    amount_in: String,
    estimated_amount_out: String,
    provider_quote: Value,
    expires_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct BoundaryRequoteContext {
    source_asset: DepositAsset,
    amount_in: String,
    source_address: Option<String>,
    initial_source_role: Option<CustodyVaultRole>,
    initial_hyperliquid_custody: Option<MarketOrderInitialHyperliquidCustody>,
}

const BOUNDARY_REQUOTE_MAX_PATH_DEPTH: usize = 5;
const BOUNDARY_REQUOTE_TOP_K_PATHS: usize = 8;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS: usize = 1;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS: usize = 2;

fn leg_boundary_requote_untenable(
    order_id: Uuid,
    source_attempt_id: Uuid,
    completed_step_id: Uuid,
    message: impl ToString,
) -> LegBoundaryRequoteAttemptShape {
    LegBoundaryRequoteAttemptShape {
        outcome: LegBoundaryRequoteAttemptOutcome::Untenable {
            order_id: order_id.into(),
            source_attempt_id: source_attempt_id.into(),
            completed_step_id: completed_step_id.into(),
            reason: StaleQuoteRefreshUntenableReason::StaleProviderQuoteRefreshUntenable {
                message: message.to_string(),
            },
        },
    }
}

fn leg_boundary_requote_not_needed(
    order_id: Uuid,
    source_attempt_id: Uuid,
    completed_step_id: Uuid,
    reason: Value,
) -> LegBoundaryRequoteAttemptShape {
    LegBoundaryRequoteAttemptShape {
        outcome: LegBoundaryRequoteAttemptOutcome::NotNeeded {
            order_id: order_id.into(),
            source_attempt_id: source_attempt_id.into(),
            completed_step_id: completed_step_id.into(),
            reason,
        },
    }
}

fn boundary_requoted_market_order_quote_exact_in(
    order: &RouterOrder,
    source_asset: DepositAsset,
    _path: &TransitionPath,
    quote: BoundaryComposedMarketOrderQuote,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        source_asset,
        destination_asset: order.destination_asset.clone(),
        recipient_address: Some(order.recipient_address.clone()),
        provider_id: quote.provider_id,
        amount_in: quote.amount_in,
        estimated_amount_out: quote.estimated_amount_out,
        provider_quote: quote.provider_quote,
        usd_valuation: json!({}),
        expected_swap_time_ms: None,
        expires_at: quote.expires_at,
        created_at,
    }
}

fn boundary_transition_path_quote_blob(
    path: &TransitionPath,
    legs: &[QuoteLeg],
    gas_reimbursement_plan: &GasReimbursementPlan,
) -> Value {
    json!({
        "schema_version": 2,
        "planner": "transition_decl_v1",
        "path_id": path.id,
        "transition_decl_ids": path
            .transitions
            .iter()
            .map(|transition| transition.id.clone())
            .collect::<Vec<_>>(),
        "transitions": path.transitions,
        "legs": legs,
        "gas_reimbursement": gas_reimbursement_plan,
        "refresh": {
            "schema_version": 1,
            "reason": "completed_leg_boundary_requote",
        },
    })
}

fn boundary_flatten_transition_legs(legs_per_transition: Vec<Vec<QuoteLeg>>) -> Vec<QuoteLeg> {
    let mut flattened = Vec::new();
    for mut transition_legs in legs_per_transition {
        flattened.append(&mut transition_legs);
    }
    flattened
}

fn boundary_composed_provider_id(
    path: &TransitionPath,
    unit_provider: Option<&str>,
    exchange_providers: &[&str],
) -> String {
    let mut providers = Vec::new();
    for transition in &path.transitions {
        providers.push(format!(
            "{}:{}",
            transition.kind.as_str(),
            transition.provider.as_str()
        ));
    }
    if let Some(unit_provider) = unit_provider {
        providers.push(format!("unit:{unit_provider}"));
    }
    for exchange_provider in exchange_providers {
        providers.push(format!("exchange:{exchange_provider}"));
    }
    format!("path:{}|{}", path.id, providers.join("|"))
}

fn boundary_transition_exchange_provider_ids(path: &TransitionPath) -> Vec<&str> {
    let mut providers = Vec::new();
    for transition in &path.transitions {
        if !matches!(
            transition.kind,
            MarketOrderTransitionKind::HyperliquidTrade
                | MarketOrderTransitionKind::UniversalRouterSwap
        ) {
            continue;
        }
        let provider = transition.provider.as_str();
        if !providers.contains(&provider) {
            providers.push(provider);
        }
    }
    providers
}

fn boundary_is_executable_transition_path(registry: &AssetRegistry, path: &TransitionPath) -> bool {
    if path.transitions.is_empty() {
        return false;
    }
    matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::UnitWithdrawal)
    ) || matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::UniversalRouterSwap)
    ) || matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::HyperliquidBridgeWithdrawal)
    ) || matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge)
    ) && boundary_path_contains_runtime_asset(registry, path)
}

fn boundary_path_contains_runtime_asset(registry: &AssetRegistry, path: &TransitionPath) -> bool {
    path.transitions.iter().any(|transition| {
        registry.chain_asset(&transition.input.asset).is_none()
            || registry.chain_asset(&transition.output.asset).is_none()
    })
}

fn boundary_path_has_configured_provider_set(
    action_providers: &ActionProviderRegistry,
    path: &TransitionPath,
) -> bool {
    path.transitions
        .iter()
        .all(|transition| match transition.kind {
            MarketOrderTransitionKind::UnitDeposit => action_providers
                .units()
                .iter()
                .any(|unit| unit.supports_deposit(&transition.input.asset)),
            MarketOrderTransitionKind::UnitWithdrawal => action_providers
                .units()
                .iter()
                .any(|unit| unit.supports_withdrawal(&transition.output.asset)),
            MarketOrderTransitionKind::HyperliquidTrade
            | MarketOrderTransitionKind::UniversalRouterSwap => action_providers
                .exchange(transition.provider.as_str())
                .is_some(),
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => action_providers
                .bridge(transition.provider.as_str())
                .is_some(),
        })
}

fn boundary_unit_path_compatible(unit: &dyn UnitProvider, path: &TransitionPath) -> bool {
    path.transitions
        .iter()
        .all(|transition| match transition.kind {
            MarketOrderTransitionKind::UnitDeposit => {
                unit.supports_deposit(&transition.input.asset)
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                unit.supports_withdrawal(&transition.output.asset)
            }
            _ => true,
        })
}

fn boundary_prefer_same_chain_evm_paths(
    source_asset: &DepositAsset,
    destination_asset: &DepositAsset,
    paths: &mut Vec<TransitionPath>,
) {
    if source_asset.chain != destination_asset.chain
        || !source_asset.chain.as_str().starts_with("evm:")
    {
        return;
    }
    let chain = &source_asset.chain;
    let same_chain_paths = paths
        .iter()
        .filter(|path| {
            path.transitions.iter().all(|transition| {
                transition.input.asset.chain == *chain && transition.output.asset.chain == *chain
            })
        })
        .cloned()
        .collect::<Vec<_>>();
    if !same_chain_paths.is_empty() {
        *paths = same_chain_paths;
    }
}

fn boundary_is_better_quote(
    candidate: &BoundaryComposedMarketOrderQuote,
    current: &Option<BoundaryComposedMarketOrderQuote>,
) -> bool {
    let Some(current) = current else {
        return true;
    };
    let primary_cmp =
        refresh_parse_u256_amount("estimated_amount_out", &candidate.estimated_amount_out)
            .ok()
            .cmp(
                &refresh_parse_u256_amount("estimated_amount_out", &current.estimated_amount_out)
                    .ok(),
            );
    if primary_cmp.is_gt() {
        return true;
    }
    if primary_cmp.is_lt() {
        return false;
    }
    boundary_quote_hop_count(candidate) < boundary_quote_hop_count(current)
}

fn boundary_quote_hop_count(quote: &BoundaryComposedMarketOrderQuote) -> usize {
    quote
        .provider_quote
        .get("transition_decl_ids")
        .and_then(Value::as_array)
        .map_or(usize::MAX, Vec::len)
}

fn boundary_gas_reimbursement_plan_or_path_ineligible(
    result: Result<GasReimbursementPlan, GasReimbursementError>,
) -> Result<Option<GasReimbursementPlan>, OrderActivityError> {
    match result {
        Ok(plan) => Ok(Some(plan)),
        Err(
            GasReimbursementError::NoSettlementSite { .. }
            | GasReimbursementError::UnsupportedSettlementAsset { .. },
        ) => Ok(None),
        Err(
            err @ (GasReimbursementError::PricingUnavailable
            | GasReimbursementError::InvalidPlanAmount { .. }
            | GasReimbursementError::NumericOverflow { .. }),
        ) => Err(refresh_materialization_error(err.to_string())),
    }
}

fn boundary_apply_transition_retention(
    plan: &GasReimbursementPlan,
    transition_id: &str,
    field: &'static str,
    gross_amount: &str,
) -> Result<String, OrderActivityError> {
    let retention = try_transition_retention_amount(plan, transition_id)
        .map_err(|source| refresh_materialization_error(source.to_string()))?;
    refresh_subtract_amount(
        field,
        gross_amount,
        retention,
        transition_id,
        "paymaster gas retention",
    )
}

async fn boundary_destination_address(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    asset: &DepositAsset,
) -> Result<String, OrderActivityError> {
    let mut cache = None;
    ensure_destination_execution_vault(deps, order_id, asset, &mut cache)
        .await
        .map(|vault| vault.address)
}

async fn boundary_hyperliquid_spot_address(
    deps: &OrderActivityDeps,
    order_id: Uuid,
) -> Result<String, OrderActivityError> {
    let mut cache = None;
    ensure_hyperliquid_spot_vault(deps, order_id, &mut cache)
        .await
        .map(|vault| vault.address)
}

async fn boundary_source_address_for_role(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    role: Option<CustodyVaultRole>,
    asset: &DepositAsset,
    fallback: Option<&str>,
) -> Result<String, OrderActivityError> {
    match role {
        Some(CustodyVaultRole::DestinationExecution) => {
            boundary_destination_address(deps, order_id, asset).await
        }
        Some(CustodyVaultRole::HyperliquidSpot) => {
            boundary_hyperliquid_spot_address(deps, order_id).await
        }
        Some(CustodyVaultRole::SourceDeposit) | None => {
            fallback.map(ToString::to_string).ok_or_else(|| {
                refresh_materialization_error("boundary requote source address is unavailable")
            })
        }
    }
}

async fn boundary_recipient_address(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    path: &TransitionPath,
    index: usize,
    transition: &TransitionDecl,
) -> Result<String, OrderActivityError> {
    if index + 1 == path.transitions.len() {
        return Ok(order.recipient_address.clone());
    }
    match transition.output.required_custody_role {
        RequiredCustodyRole::HyperliquidSpot => {
            boundary_hyperliquid_spot_address(deps, order.id).await
        }
        RequiredCustodyRole::IntermediateExecution
        | RequiredCustodyRole::SourceOrIntermediate
        | RequiredCustodyRole::DestinationPayout => {
            boundary_destination_address(deps, order.id, &transition.output.asset).await
        }
    }
}

fn boundary_input_role_for_transition(
    path: &TransitionPath,
    index: usize,
    initial_source_role: Option<CustodyVaultRole>,
) -> Option<CustodyVaultRole> {
    if index == 0 {
        return initial_source_role;
    }
    match path.transitions[index - 1].output.required_custody_role {
        RequiredCustodyRole::HyperliquidSpot => Some(CustodyVaultRole::HyperliquidSpot),
        RequiredCustodyRole::IntermediateExecution | RequiredCustodyRole::DestinationPayout => {
            Some(CustodyVaultRole::DestinationExecution)
        }
        RequiredCustodyRole::SourceOrIntermediate => None,
    }
}

async fn boundary_depositor_address(
    deps: &OrderActivityDeps,
    order_id: Uuid,
    path: &TransitionPath,
    index: usize,
    initial_source_role: Option<CustodyVaultRole>,
    initial_source_address: Option<&str>,
) -> Result<String, OrderActivityError> {
    let role = boundary_input_role_for_transition(path, index, initial_source_role);
    boundary_source_address_for_role(
        deps,
        order_id,
        role,
        &path.transitions[index].input.asset,
        if index == 0 {
            initial_source_address
        } else {
            None
        },
    )
    .await
}

async fn boundary_bitcoin_unit_deposit_fee_reserve(
    deps: &OrderActivityDeps,
    transition: &TransitionDecl,
) -> Result<Option<U256>, OrderActivityError> {
    if transition.kind != MarketOrderTransitionKind::UnitDeposit
        || transition.input.asset.chain.as_str() != "bitcoin"
    {
        return Ok(None);
    }
    let bitcoin_chain = deps
        .chain_registry
        .get_bitcoin(&ChainType::Bitcoin)
        .ok_or_else(|| {
            refresh_materialization_error(
                "bitcoin chain is required to estimate Unit deposit miner fee",
            )
        })?;
    let estimated_fee = bitcoin_chain
        .estimate_p2wpkh_transfer_fee_sats(
            BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS,
            BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS,
        )
        .await
        .map_err(|err| {
            refresh_materialization_error(format!(
                "failed to estimate bitcoin Unit deposit miner fee: {err}"
            ))
        })?;
    let reserved_fee = estimated_fee
        .checked_mul(REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS)
        .and_then(|value| value.checked_add(9_999))
        .map(|value| value / 10_000)
        .ok_or_else(|| refresh_materialization_error("bitcoin fee reserve overflowed"))?;
    Ok(Some(U256::from(reserved_fee)))
}

async fn boundary_quote_transition_path(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    context: &BoundaryRequoteContext,
    path: &TransitionPath,
) -> Result<Option<BoundaryComposedMarketOrderQuote>, OrderActivityError> {
    let requires_unit = path.transitions.iter().any(|transition| {
        matches!(
            transition.kind,
            MarketOrderTransitionKind::UnitDeposit | MarketOrderTransitionKind::UnitWithdrawal
        )
    });
    let unit_candidates: Vec<Option<Arc<dyn UnitProvider>>> = if requires_unit {
        deps.action_providers
            .units()
            .iter()
            .filter(|unit| boundary_unit_path_compatible(unit.as_ref(), path))
            .cloned()
            .map(Some)
            .collect()
    } else {
        vec![None]
    };
    if unit_candidates.is_empty() {
        return Ok(None);
    }

    let mut best_for_path: Option<BoundaryComposedMarketOrderQuote> = None;
    let mut last_error: Option<OrderActivityError> = None;
    for unit in &unit_candidates {
        match boundary_compose_transition_path_quote(deps, order, context, path, unit.as_deref())
            .await
        {
            Ok(Some(quote)) => {
                if boundary_is_better_quote(&quote, &best_for_path) {
                    best_for_path = Some(quote);
                }
            }
            Ok(None) => {}
            Err(err) => {
                tracing::warn!(path_id = %path.id, error = %err, "boundary requote path quote failed");
                last_error = Some(err);
            }
        }
    }

    match (best_for_path, last_error) {
        (Some(quote), _) => Ok(Some(quote)),
        (None, Some(err)) => Err(err),
        (None, None) => Ok(None),
    }
}

async fn boundary_compose_transition_path_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    context: &BoundaryRequoteContext,
    path: &TransitionPath,
    unit: Option<&dyn UnitProvider>,
) -> Result<Option<BoundaryComposedMarketOrderQuote>, OrderActivityError> {
    let gas_plan_result = if let Some(pricing) = deps.usd_pricing_snapshot().await {
        optimized_paymaster_reimbursement_plan_with_pricing(
            deps.action_providers.asset_registry().as_ref(),
            path,
            &pricing,
        )
    } else {
        optimized_paymaster_reimbursement_plan(
            deps.action_providers.asset_registry().as_ref(),
            path,
        )
    };
    let Some(gas_reimbursement_plan) =
        boundary_gas_reimbursement_plan_or_path_ineligible(gas_plan_result)?
    else {
        return Ok(None);
    };

    let mut expires_at = Utc::now() + chrono::Duration::minutes(10);
    let mut legs_per_transition: Vec<Vec<QuoteLeg>> = vec![Vec::new(); path.transitions.len()];
    let mut cursor_amount = context.amount_in.clone();

    for (index, transition) in path.transitions.iter().enumerate() {
        let provider_amount_in = boundary_apply_transition_retention(
            &gas_reimbursement_plan,
            &transition.id,
            "amount_in",
            &cursor_amount,
        )?;
        match transition.kind {
            MarketOrderTransitionKind::AcrossBridge
            | MarketOrderTransitionKind::CctpBridge
            | MarketOrderTransitionKind::HyperliquidBridgeDeposit
            | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                let bridge = deps
                    .action_providers
                    .bridge(transition.provider.as_str())
                    .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
                let bridge_quote = bridge
                    .quote_bridge(BridgeQuoteRequest {
                        source_asset: transition.input.asset.clone(),
                        destination_asset: transition.output.asset.clone(),
                        order_kind: ProviderOrderKind::ExactIn {
                            amount_in: provider_amount_in.clone(),
                            min_amount_out: Some("1".to_string()),
                        },
                        recipient_address: boundary_recipient_address(
                            deps, order, path, index, transition,
                        )
                        .await?,
                        depositor_address: boundary_depositor_address(
                            deps,
                            order.id,
                            path,
                            index,
                            context.initial_source_role,
                            context.source_address.as_deref(),
                        )
                        .await?,
                        partial_fills_enabled: false,
                    })
                    .await
                    .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
                    .ok_or_else(|| {
                        provider_quote_error(bridge.id(), "provider returned no refreshed quote")
                    })?;
                expires_at = expires_at.min(bridge_quote.expires_at);
                cursor_amount = bridge_quote.amount_out.clone();
                legs_per_transition[index] =
                    refresh_bridge_quote_transition_legs(transition, &bridge_quote)?;
            }
            MarketOrderTransitionKind::UnitDeposit => {
                let _unit = unit.ok_or_else(|| {
                    refresh_materialization_error("unit provider is required for unit_deposit")
                })?;
                let (unit_amount_in, provider_quote) = if let Some(fee_reserve) =
                    boundary_bitcoin_unit_deposit_fee_reserve(deps, transition).await?
                {
                    (
                        refresh_subtract_amount(
                            "amount_in",
                            &provider_amount_in,
                            fee_reserve,
                            &transition.id,
                            "bitcoin miner fee reserve",
                        )?,
                        refresh_bitcoin_fee_reserve_quote(fee_reserve),
                    )
                } else {
                    (provider_amount_in.clone(), json!({}))
                };
                cursor_amount = unit_amount_in.clone();
                legs_per_transition[index].push(refresh_unit_deposit_quote_leg(
                    transition,
                    &unit_amount_in,
                    expires_at,
                    provider_quote,
                ));
            }
            MarketOrderTransitionKind::HyperliquidTrade => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
                let mut quote_amount_in = provider_amount_in.clone();
                let prefund_from_boundary = index == 0
                    && context
                        .initial_hyperliquid_custody
                        .as_ref()
                        .is_some_and(|custody| custody.prefund_first_trade);
                if prefund_from_boundary
                    || (index > 0
                        && matches!(
                            path.transitions[index - 1].kind,
                            MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        ))
                {
                    quote_amount_in = refresh_reserve_hyperliquid_spot_send_quote_gas(
                        "hyperliquid_trade.amount_in",
                        &quote_amount_in,
                    )?;
                }
                let exchange_quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals: None,
                        output_decimals: None,
                        order_kind: ProviderOrderKind::ExactIn {
                            amount_in: quote_amount_in,
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: None,
                        recipient_address: order.recipient_address.clone(),
                    })
                    .await
                    .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
                    .ok_or_else(|| {
                        provider_quote_error(exchange.id(), "provider returned no refreshed quote")
                    })?;
                expires_at = expires_at.min(exchange_quote.expires_at);
                cursor_amount = exchange_quote.amount_out.clone();
                legs_per_transition[index] = refresh_exchange_quote_transition_legs(
                    transition.id.as_str(),
                    transition.kind,
                    transition.provider,
                    &exchange_quote,
                )?;
            }
            MarketOrderTransitionKind::UniversalRouterSwap => {
                let exchange = deps
                    .action_providers
                    .exchange(transition.provider.as_str())
                    .ok_or_else(|| provider_quote_not_configured(transition.provider.as_str()))?;
                let input_decimals = deps
                    .action_providers
                    .asset_registry()
                    .chain_asset(&transition.input.asset)
                    .map(|asset| asset.decimals);
                let output_decimals = deps
                    .action_providers
                    .asset_registry()
                    .chain_asset(&transition.output.asset)
                    .map(|asset| asset.decimals);
                let exchange_quote = exchange
                    .quote_trade(ExchangeQuoteRequest {
                        input_asset: transition.input.asset.clone(),
                        output_asset: transition.output.asset.clone(),
                        input_decimals,
                        output_decimals,
                        order_kind: ProviderOrderKind::ExactIn {
                            amount_in: provider_amount_in,
                            min_amount_out: Some("1".to_string()),
                        },
                        sender_address: Some(
                            boundary_depositor_address(
                                deps,
                                order.id,
                                path,
                                index,
                                context.initial_source_role,
                                context.source_address.as_deref(),
                            )
                            .await?,
                        ),
                        recipient_address: boundary_recipient_address(
                            deps, order, path, index, transition,
                        )
                        .await?,
                    })
                    .await
                    .map_err(|source| provider_quote_error(transition.provider.as_str(), source))?
                    .ok_or_else(|| {
                        provider_quote_error(exchange.id(), "provider returned no refreshed quote")
                    })?;
                expires_at = expires_at.min(exchange_quote.expires_at);
                cursor_amount = exchange_quote.amount_out.clone();
                legs_per_transition[index] = refresh_exchange_quote_transition_legs(
                    transition.id.as_str(),
                    transition.kind,
                    transition.provider,
                    &exchange_quote,
                )?;
            }
            MarketOrderTransitionKind::UnitWithdrawal => {
                let unit = unit.ok_or_else(|| {
                    refresh_materialization_error("unit provider is required for unit_withdrawal")
                })?;
                if !unit.supports_withdrawal(&transition.output.asset) {
                    return Ok(None);
                }
                let recipient_address =
                    boundary_recipient_address(deps, order, path, index, transition).await?;
                legs_per_transition[index].push(refresh_unit_withdrawal_quote_leg(
                    transition,
                    &provider_amount_in,
                    &recipient_address,
                    expires_at,
                ));
                cursor_amount = provider_amount_in;
            }
        }
    }

    let legs = boundary_flatten_transition_legs(legs_per_transition);
    let provider_id = boundary_composed_provider_id(
        path,
        unit.map(|provider| provider.id()),
        &boundary_transition_exchange_provider_ids(path),
    );
    Ok(Some(BoundaryComposedMarketOrderQuote {
        provider_id,
        amount_in: context.amount_in.clone(),
        estimated_amount_out: cursor_amount,
        provider_quote: boundary_transition_path_quote_blob(path, &legs, &gas_reimbursement_plan),
        expires_at,
    }))
}

async fn boundary_best_provider_quote(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    start_node: MarketOrderNode,
    context: &BoundaryRequoteContext,
) -> Result<Option<(TransitionPath, BoundaryComposedMarketOrderQuote)>, OrderActivityError> {
    let registry = deps.action_providers.asset_registry();
    let mut paths = registry.select_transition_paths_between(
        start_node,
        MarketOrderNode::External(order.destination_asset.clone()),
        BOUNDARY_REQUOTE_MAX_PATH_DEPTH,
    );
    paths.retain(|path| boundary_is_executable_transition_path(registry.as_ref(), path));
    paths.retain(|path| {
        boundary_path_has_configured_provider_set(deps.action_providers.as_ref(), path)
    });
    boundary_prefer_same_chain_evm_paths(
        &context.source_asset,
        &order.destination_asset,
        &mut paths,
    );
    if paths.is_empty() {
        return Ok(None);
    }
    // Boundary requote pins to a fixed structural anchor (audit S1.8 is
    // not yet addressed); when that lands this should derive size from the
    // order's actual amount and call the tier-aware ranker.
    router_core::services::route_costs::rank_transition_paths_structurally(
        &mut paths,
        router_core::services::route_costs::DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
    );
    paths.truncate(BOUNDARY_REQUOTE_TOP_K_PATHS);

    let mut best: Option<(TransitionPath, BoundaryComposedMarketOrderQuote)> = None;
    let mut last_error = None;
    for path in paths {
        match boundary_quote_transition_path(deps, order, context, &path).await {
            Ok(Some(quote)) => {
                let replace_best = match &best {
                    Some((_, current)) => boundary_is_better_quote(&quote, &Some(current.clone())),
                    None => true,
                };
                if replace_best {
                    best = Some((path, quote));
                }
            }
            Ok(None) => {}
            Err(err) => last_error = Some(err),
        }
    }
    if best.is_some() {
        return Ok(best);
    }
    if let Some(err) = last_error {
        return Err(err);
    }
    Ok(None)
}

fn boundary_start_node(
    registry: &AssetRegistry,
    asset: &DepositAsset,
) -> Result<MarketOrderNode, OrderActivityError> {
    if asset.chain.as_str() == "hyperliquid" {
        let canonical = registry.canonical_for(asset).ok_or_else(|| {
            refresh_materialization_error(format!(
                "completed leg output {} {} is not a registered Hyperliquid asset",
                asset.chain, asset.asset
            ))
        })?;
        return Ok(MarketOrderNode::Venue {
            provider: ProviderId::HyperliquidSpot,
            canonical,
        });
    }
    Ok(MarketOrderNode::External(asset.clone()))
}

fn boundary_step_request_string(
    steps: &[OrderExecutionStep],
    keys: &[&'static str],
) -> Option<String> {
    steps.iter().rev().find_map(|step| {
        keys.iter().find_map(|key| {
            step.request
                .get(*key)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .map(ToString::to_string)
        })
    })
}

fn boundary_step_request_custody_role(
    steps: &[OrderExecutionStep],
    key: &'static str,
) -> Option<CustodyVaultRole> {
    steps
        .iter()
        .rev()
        .find_map(|step| step.request.get(key).and_then(Value::as_str))
        .and_then(CustodyVaultRole::from_db_string)
}

fn boundary_step_request_asset(
    steps: &[OrderExecutionStep],
    chain_key: &'static str,
    asset_key: &'static str,
) -> Option<DepositAsset> {
    steps.iter().rev().find_map(|step| {
        let chain = step.request.get(chain_key).and_then(Value::as_str)?;
        let asset = step.request.get(asset_key).and_then(Value::as_str)?;
        Some(DepositAsset {
            chain: ChainId::parse(chain).ok()?,
            asset: AssetId::parse(asset).ok()?,
        })
    })
}

fn boundary_step_hyperliquid_custody(
    steps: &[OrderExecutionStep],
) -> (CustodyVaultRole, Option<DepositAsset>, Option<String>) {
    let role = boundary_step_request_custody_role(steps, "hyperliquid_custody_vault_role")
        .unwrap_or(CustodyVaultRole::HyperliquidSpot);
    let asset = boundary_step_request_asset(
        steps,
        "hyperliquid_custody_vault_chain_id",
        "hyperliquid_custody_vault_asset_id",
    );
    let source_address =
        boundary_step_request_string(steps, &["hyperliquid_custody_vault_address"]);
    (role, asset, source_address)
}

async fn boundary_hyperliquid_current_location(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    source_vault: &DepositVault,
    completed_leg: &OrderExecutionLeg,
    completed_leg_steps: &[OrderExecutionStep],
) -> Result<BoundaryRequoteContext, OrderActivityError> {
    let source_asset = completed_leg.output_asset.clone();
    let amount_in = completed_leg.actual_amount_out.clone().ok_or_else(|| {
        refresh_materialization_error(format!(
            "completed leg {} has no actual_amount_out for boundary requote",
            completed_leg.id
        ))
    })?;

    let (role, asset, prefund_first_trade, source_address) = match completed_leg.leg_type.as_str() {
        "hyperliquid_bridge_deposit" => {
            let role = boundary_step_request_custody_role(
                completed_leg_steps,
                "source_custody_vault_role",
            )
            .unwrap_or(CustodyVaultRole::SourceDeposit);
            let asset =
                boundary_step_request_asset(completed_leg_steps, "source_chain_id", "input_asset")
                    .or_else(|| Some(completed_leg.input_asset.clone()));
            let source_address = boundary_step_request_string(
                completed_leg_steps,
                &["source_custody_vault_address", "depositor_address"],
            );
            (role, asset, true, source_address)
        }
        "hypercore_bridge_deposit" => {
            let role = boundary_step_request_custody_role(
                completed_leg_steps,
                "source_custody_vault_role",
            )
            .unwrap_or(CustodyVaultRole::SourceDeposit);
            let asset =
                boundary_step_request_asset(completed_leg_steps, "source_chain_id", "input_asset")
                    .or_else(|| Some(completed_leg.input_asset.clone()));
            let source_address = boundary_step_request_string(
                completed_leg_steps,
                &["source_custody_vault_address", "depositor_address"],
            );
            (role, asset, false, source_address)
        }
        "unit_deposit" | "hyperliquid_trade" => {
            let (role, asset, source_address) =
                boundary_step_hyperliquid_custody(completed_leg_steps);
            (role, asset, false, source_address)
        }
        _ => {
            let role = boundary_step_request_custody_role(
                completed_leg_steps,
                "hyperliquid_custody_vault_role",
            )
            .unwrap_or(CustodyVaultRole::HyperliquidSpot);
            let asset = boundary_step_request_asset(
                completed_leg_steps,
                "hyperliquid_custody_vault_chain_id",
                "hyperliquid_custody_vault_asset_id",
            );
            let source_address = boundary_step_request_string(
                completed_leg_steps,
                &["hyperliquid_custody_vault_address"],
            );
            (role, asset, false, source_address)
        }
    };

    let source_address = match (source_address, role, asset.as_ref()) {
        (Some(address), _, _) => Some(address),
        (None, CustodyVaultRole::SourceDeposit, _) => {
            Some(source_vault.deposit_vault_address.clone())
        }
        (None, CustodyVaultRole::DestinationExecution, Some(asset)) => {
            Some(boundary_destination_address(deps, order.id, asset).await?)
        }
        (None, CustodyVaultRole::HyperliquidSpot, _) => {
            Some(boundary_hyperliquid_spot_address(deps, order.id).await?)
        }
        (None, CustodyVaultRole::DestinationExecution, None) => None,
    };

    Ok(BoundaryRequoteContext {
        source_asset,
        amount_in,
        source_address,
        initial_source_role: if role == CustodyVaultRole::HyperliquidSpot {
            Some(CustodyVaultRole::HyperliquidSpot)
        } else {
            None
        },
        initial_hyperliquid_custody: Some(MarketOrderInitialHyperliquidCustody {
            role,
            asset,
            prefund_first_trade,
        }),
    })
}

async fn boundary_external_current_location(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    completed_leg: &OrderExecutionLeg,
) -> Result<BoundaryRequoteContext, OrderActivityError> {
    let amount_in = completed_leg.actual_amount_out.clone().ok_or_else(|| {
        refresh_materialization_error(format!(
            "completed leg {} has no actual_amount_out for boundary requote",
            completed_leg.id
        ))
    })?;
    Ok(BoundaryRequoteContext {
        source_asset: completed_leg.output_asset.clone(),
        amount_in,
        source_address: Some(
            boundary_destination_address(deps, order.id, &completed_leg.output_asset).await?,
        ),
        initial_source_role: Some(CustodyVaultRole::DestinationExecution),
        initial_hyperliquid_custody: None,
    })
}

async fn boundary_requote_context(
    deps: &OrderActivityDeps,
    order: &RouterOrder,
    source_vault: &DepositVault,
    completed_leg: &OrderExecutionLeg,
    completed_leg_steps: &[OrderExecutionStep],
) -> Result<(MarketOrderNode, BoundaryRequoteContext), OrderActivityError> {
    let start_node = boundary_start_node(
        deps.action_providers.asset_registry().as_ref(),
        &completed_leg.output_asset,
    )?;
    let context = if completed_leg.output_asset.chain.as_str() == "hyperliquid" {
        boundary_hyperliquid_current_location(
            deps,
            order,
            source_vault,
            completed_leg,
            completed_leg_steps,
        )
        .await?
    } else {
        boundary_external_current_location(deps, order, completed_leg).await?
    };
    Ok((start_node, context))
}

#[derive(Clone, Default)]
pub struct QuoteRefreshActivities {
    deps: Option<Arc<OrderActivityDeps>>,
}

impl QuoteRefreshActivities {
    #[must_use]
    pub(crate) fn from_order_activities(order_activities: &OrderActivities) -> Self {
        Self {
            deps: order_activities.shared_deps(),
        }
    }

    fn deps(&self) -> Result<Arc<OrderActivityDeps>, OrderActivityError> {
        self.deps
            .clone()
            .ok_or_else(|| OrderActivityError::missing_configuration("quote refresh activities"))
    }
}

#[activities]
impl QuoteRefreshActivities {
    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    ///
    /// ExactIn refresh walks the stale suffix forward; ExactOut refresh walks it
    /// backward from the requested terminal output.
    #[activity]
    pub async fn compose_refreshed_quote_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ComposeRefreshedQuoteAttemptInput,
    ) -> Result<RefreshedQuoteAttemptShape, ActivityError> {
        record_activity("compose_refreshed_quote_attempt", async move {
        let deps = self.deps()?;
        let order = deps
            .db
            .orders()
            .get(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let original_quote = match deps
            .db
            .orders()
            .get_router_order_quote(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?
        {
            RouterOrderQuote::MarketOrder(quote) => quote,
            RouterOrderQuote::LimitOrder(_) => {
                return Err(refresh_materialization_error(format!(
                    "order {} is a limit order and cannot refresh stale market quotes",
                    input.order_id.inner()
                )));
            }
        };
        let stale_attempt = deps
            .db
            .orders()
            .get_execution_attempt(input.stale_attempt_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        if stale_attempt.order_id != input.order_id.inner() {
            return Err(invariant_error(
                "refresh_attempt_order",
                format!(
                "attempt {} does not belong to order {}",
                stale_attempt.id, input.order_id.inner()
                ),
            ));
        }
        let failed_step = deps
            .db
            .orders()
            .get_execution_step(input.failed_step_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        if failed_step.order_id != input.order_id.inner()
            || failed_step.execution_attempt_id != Some(input.stale_attempt_id.inner())
        {
            return Err(invariant_error(
                "refresh_failed_step_owner",
                format!(
                "step {} does not belong to order {} attempt {}",
                failed_step.id, input.order_id.inner(), input.stale_attempt_id.inner()
                ),
            ));
        }
        let stale_legs = deps
            .db
            .orders()
            .get_execution_legs_for_attempt(input.stale_attempt_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let stale_leg = failed_step
            .execution_leg_id
            .and_then(|leg_id| stale_legs.iter().find(|leg| leg.id == leg_id).cloned())
            .ok_or_else(|| {
                refresh_materialization_error(format!(
                    "failed step {} has no materialized stale execution leg",
                    failed_step.id
                ))
            })?;
        let Some(provider_quote_expires_at) = stale_leg.provider_quote_expires_at else {
            return Err(refresh_materialization_error(format!(
                "failed step {} execution leg {} has no provider quote expiry",
                failed_step.id, stale_leg.id
            )));
        };
        if provider_quote_expires_at >= Utc::now() {
            return Err(refresh_untenable_error(format!(
                "failed step {} execution leg {} provider quote has not expired",
                failed_step.id, stale_leg.id
            )));
        }
        let source_vault_id = order.funding_vault_id.ok_or_else(|| {
            refresh_materialization_error(format!(
                "order {} cannot refresh a quote without a funding vault",
                order.id
            ))
        })?;
        let source_vault = deps
            .db
            .vaults()
            .get(source_vault_id)
            .await
            .map_err(OrderActivityError::db_query)?;
        let transitions = deps
            .planner
            .quoted_transition_path(&order, &original_quote)
            .map_err(|source| refresh_materialization_error(source.to_string()))?;
        let Some(stale_transition_id) = stale_leg.transition_decl_id.as_deref() else {
            return Err(refresh_materialization_error(format!(
                "failed stale execution leg {} has no transition declaration",
                stale_leg.id
            )));
        };
        let start_transition_index = transitions
            .iter()
            .position(|transition| transition.id == stale_transition_id)
            .ok_or_else(|| {
                refresh_materialization_error(format!(
                    "stale execution leg {} transition {} is not in the quoted path",
                    stale_leg.id, stale_transition_id
                ))
            })?;
        let attempts = deps
            .db
            .orders()
            .get_execution_attempts(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let execution_history = deps
            .db
            .orders()
            .get_execution_legs(input.order_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let stale_attempt_steps = deps
            .db
            .orders()
            .get_execution_steps_for_attempt(input.stale_attempt_id.inner())
            .await
            .map_err(OrderActivityError::db_query)?;
        let available_amount = match refresh_remaining_exact_in_amount(
            &execution_history,
            &attempts,
            &stale_attempt,
            &stale_leg,
            &original_quote,
            &transitions,
            start_transition_index,
        ) {
            Ok(amount) => amount,
            Err(err) => {
                return Ok(stale_quote_refresh_untenable(
                    input.order_id.inner(),
                    input.stale_attempt_id.inner(),
                    input.failed_step_id.inner(),
                    err,
                ));
            }
        };
        let now = Utc::now();
        let refreshed_quote = {
                let mut cursor_amount = available_amount.clone();
                let mut expires_at = now + chrono::Duration::minutes(10);
                let mut refreshed_legs = Vec::new();
                for index in start_transition_index..transitions.len() {
                    let transition = &transitions[index];
                    match transition.kind {
                        MarketOrderTransitionKind::UniversalRouterSwap => {
                            let template_step = stale_attempt_steps
                        .iter()
                        .find(|step| refresh_step_matches_transition(step, transition))
                        .ok_or_else(|| {
                            refresh_materialization_error(format!(
                                "cannot refresh transition {} because no template step is materialized",
                                transition.id
                            ))
                        })?;
                            let step_request =
                                match ExchangeExecutionRequest::universal_router_swap_from_value(
                                    &template_step.request,
                                )
                                .map_err(refresh_materialization_error)?
                                {
                                    ExchangeExecutionRequest::UniversalRouterSwap(request) => {
                                        request
                                    }
                                    _ => {
                                        return Err(refresh_materialization_error(format!(
                                            "step {} is not a universal-router swap request",
                                            template_step.id
                                        )));
                                    }
                                };
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: Some(step_request.input_decimals),
                                    output_decimals: Some(step_request.output_decimals),
                                    order_kind: ProviderOrderKind::ExactIn {
                                        amount_in: cursor_amount.clone(),
                                        min_amount_out: Some("1".to_string()),
                                    },
                                    sender_address: Some(refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?),
                                    recipient_address: refresh_recipient_address(
                                        &order,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                })
                                .await
                                .map_err(|source| {
                                    provider_quote_error(transition.provider.as_str(), source)
                                })?
                                .ok_or_else(|| {
                                    provider_quote_error(
                                        exchange.id(),
                                        "provider returned no refreshed quote",
                                    )
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_exchange_quote_transition_legs(
                                transition.id.as_str(),
                                transition.kind,
                                transition.provider,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::AcrossBridge
                        | MarketOrderTransitionKind::CctpBridge
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                            let bridge = deps
                                .action_providers
                                .bridge(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            let refreshed_quote = bridge
                                .quote_bridge(BridgeQuoteRequest {
                                    source_asset: transition.input.asset.clone(),
                                    destination_asset: transition.output.asset.clone(),
                                    order_kind: ProviderOrderKind::ExactIn {
                                        amount_in: cursor_amount.clone(),
                                        min_amount_out: Some("1".to_string()),
                                    },
                                    recipient_address: refresh_bridge_recipient_address(
                                        &order,
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    depositor_address: refresh_source_address(
                                        &source_vault,
                                        &stale_attempt_steps,
                                        &transitions,
                                        index,
                                    )?,
                                    partial_fills_enabled: false,
                                })
                                .await
                                .map_err(|source| {
                                    provider_quote_error(transition.provider.as_str(), source)
                                })?
                                .ok_or_else(|| {
                                    provider_quote_error(
                                        bridge.id(),
                                        "provider returned no refreshed quote",
                                    )
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_bridge_quote_transition_legs(
                                transition,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            let (unit_amount_in, provider_quote) = if let Some(fee_reserve) =
                                refresh_unit_deposit_fee_reserve(&original_quote, transition)?
                            {
                                (
                                    refresh_subtract_amount(
                                        "amount_in",
                                        &cursor_amount,
                                        fee_reserve,
                                        &transition.id,
                                        "bitcoin miner fee reserve",
                                    )?,
                                    refresh_bitcoin_fee_reserve_quote(fee_reserve),
                                )
                            } else {
                                (cursor_amount.clone(), json!({}))
                            };
                            if !unit.supports_deposit(&transition.input.asset) {
                                return Err(provider_quote_error(
                                    unit.id(),
                                    "provider does not support refreshed deposit",
                                ));
                            }
                            cursor_amount = unit_amount_in.clone();
                            refreshed_legs.push(refresh_unit_deposit_quote_leg(
                                transition,
                                &unit_amount_in,
                                expires_at,
                                provider_quote,
                            ));
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let exchange = deps
                                .action_providers
                                .exchange(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            let mut quote_amount_in = cursor_amount.clone();
                            if index > 0
                                && transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                quote_amount_in = refresh_reserve_hyperliquid_spot_send_quote_gas(
                                    "hyperliquid_trade.amount_in",
                                    &quote_amount_in,
                                )?;
                            }
                            let refreshed_quote = exchange
                                .quote_trade(ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: ProviderOrderKind::ExactIn {
                                        amount_in: quote_amount_in,
                                        min_amount_out: Some("1".to_string()),
                                    },
                                    sender_address: None,
                                    recipient_address: order.recipient_address.clone(),
                                })
                                .await
                                .map_err(|source| {
                                    provider_quote_error(transition.provider.as_str(), source)
                                })?
                                .ok_or_else(|| {
                                    provider_quote_error(
                                        exchange.id(),
                                        "provider returned no refreshed quote",
                                    )
                                })?;
                            expires_at = expires_at.min(refreshed_quote.expires_at);
                            cursor_amount = refreshed_quote.amount_out.clone();
                            refreshed_legs.extend(refresh_exchange_quote_transition_legs(
                                transition.id.as_str(),
                                transition.kind,
                                transition.provider,
                                &refreshed_quote,
                            )?);
                        }
                        MarketOrderTransitionKind::UnitWithdrawal => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            if !unit.supports_withdrawal(&transition.output.asset) {
                                return Err(provider_quote_error(
                                    unit.id(),
                                    "provider does not support refreshed withdrawal",
                                ));
                            }
                            let recipient_address = refresh_recipient_address(
                                &order,
                                &stale_attempt_steps,
                                &transitions,
                                index,
                            )?;
                            refreshed_legs.push(refresh_unit_withdrawal_quote_leg(
                                transition,
                                &cursor_amount,
                                &recipient_address,
                                expires_at,
                            ));
                        }
                    }
                }
                refreshed_market_order_quote_exact_in(
                    &order,
                    &original_quote,
                    &transitions,
                    refreshed_legs,
                    available_amount,
                    cursor_amount,
                    expires_at,
                    now,
                )
        };
        let mut plan = deps
            .planner
            .plan_remaining(
                &order,
                &source_vault,
                &refreshed_quote,
                MarketOrderPlanRemainingStart {
                    transition_decl_id: &transitions[start_transition_index].id,
                    step_index: failed_step.step_index,
                    leg_index: stale_leg.leg_index,
                    planned_at: now,
                },
            )
            .map_err(|source| refresh_materialization_error(source.to_string()))?;
        for leg in &mut plan.legs {
            set_json_value(
                &mut leg.details,
                "refreshed_from_attempt_id",
                json!(stale_attempt.id),
            );
            set_json_value(
                &mut leg.details,
                "refreshed_from_execution_leg_id",
                json!(stale_leg.id),
            );
            set_json_value(
                &mut leg.details,
                "refreshed_quote_id",
                json!(refreshed_quote.id),
            );
        }
        for step in &mut plan.steps {
            set_json_value(
                &mut step.details,
                "refreshed_from_attempt_id",
                json!(stale_attempt.id),
            );
            set_json_value(
                &mut step.details,
                "refreshed_from_step_id",
                json!(failed_step.id),
            );
            set_json_value(
                &mut step.details,
                "refreshed_quote_id",
                json!(refreshed_quote.id),
            );
        }

        let refreshed_shape = RefreshedQuoteAttemptShape {
            outcome: RefreshedQuoteAttemptOutcome::Refreshed {
                order_id: input.order_id,
                stale_attempt_id: input.stale_attempt_id,
                failed_step_id: input.failed_step_id,
                plan: ExecutionPlan {
                    path_id: plan.path_id,
                    transition_decl_ids: plan.transition_decl_ids,
                    legs: plan.legs,
                    steps: plan.steps,
                },
                failure_reason: RefreshedQuoteFailureReason::stale_provider_quote_refresh(
                    stale_attempt.id,
                    stale_attempt.attempt_index,
                    failed_step.id,
                    stale_leg.id,
                    provider_quote_expires_at,
                    refreshed_quote.id,
                ),
                cancelled_reason: RefreshedQuoteCancelledReason::stale_provider_quote_refresh(
                    stale_attempt.id,
                    failed_step.id,
                    stale_leg.id,
                    provider_quote_expires_at,
                    refreshed_quote.id,
                ),
                input_custody_snapshot: failed_attempt_snapshot(&order, &failed_step),
            },
        };
        telemetry::record_stale_quote_refresh("attempt_composed");
        Ok(refreshed_shape)
        })
        .await
    }

    /// Scar tissue: §7 stale quote refresh and §16.4 refresh helpers.
    #[activity]
    pub async fn materialize_refreshed_attempt(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: MaterializeRefreshedAttemptInput,
    ) -> Result<RefreshedAttemptMaterialized, ActivityError> {
        record_activity("materialize_refreshed_attempt", async move {
            let deps = self.deps()?;
            let RefreshedQuoteAttemptOutcome::Refreshed {
                order_id,
                stale_attempt_id,
                failed_step_id,
                mut plan,
                failure_reason,
                cancelled_reason,
                input_custody_snapshot,
            } = input.refreshed_attempt.outcome
            else {
                return Err(refresh_materialization_error(
                    "untenable stale quote refresh must not be materialized",
                ));
            };
            if order_id != input.order_id.inner() {
                return Err(invariant_error(
                    "refreshed_attempt_order",
                    format!(
                        "refreshed attempt order {} does not match materialize input order {}",
                        order_id,
                        input.order_id.inner()
                    ),
                ));
            }
            plan.steps =
                hydrate_destination_execution_steps(&deps, input.order_id.inner(), plan.steps)
                    .await?;
            apply_execution_leg_usd_valuations(&deps, &mut plan.legs).await;
            let record = deps
                .db
                .orders()
                .create_refreshed_execution_attempt_from_failed_step(
                    order_id.inner(),
                    stale_attempt_id.inner(),
                    failed_step_id.inner(),
                    RefreshedExecutionAttemptPlan {
                        legs: plan.legs,
                        steps: plan.steps,
                        failure_reason: failure_reason.into(),
                        cancelled_reason: cancelled_reason.into(),
                        input_custody_snapshot: input_custody_snapshot.into(),
                    },
                    Utc::now(),
                )
                .await
                .map_err(OrderActivityError::db_query)?;
            tracing::info!(
                order_id = %record.order.id,
                attempt_id = %record.attempt.id,
                stale_attempt_id = %stale_attempt_id,
                failed_step_id = %failed_step_id,
                event_name = "order.execution_quote_refreshed",
                "order.execution_quote_refreshed"
            );
            telemetry::record_stale_quote_refresh("attempt_materialized");
            Ok(RefreshedAttemptMaterialized {
                attempt_id: record.attempt.id.into(),
                steps: record
                    .steps
                    .into_iter()
                    .map(|step| WorkflowExecutionStep {
                        step_id: step.id.into(),
                        step_index: step.step_index,
                        step_type: step.step_type,
                    })
                    .collect(),
            })
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refreshed_spot_transfer_quote_materializes_transfer_leg() {
        let expires_at = Utc::now() + chrono::Duration::minutes(10);
        let quote = ExchangeQuote {
            provider_id: ProviderId::HyperliquidSpot.as_str().to_string(),
            amount_in: "134770".to_string(),
            amount_out: "134770".to_string(),
            min_amount_out: Some("1".to_string()),
            max_amount_in: None,
            expires_at,
            provider_quote: json!({
                "kind": "spot_transfer",
                "token": "UBTC",
                "recipient_address": "0x33f65788aca48d733c2c2444ac9f79b18206aa92",
                "leg": {
                    "kind": "spot_transfer",
                    "order_kind": "exact_in",
                    "input_asset": { "chain_id": "hyperliquid", "asset": "UBTC" },
                    "output_asset": { "chain_id": "hyperliquid", "asset": "UBTC" },
                    "amount_in": "134770",
                    "amount_out": "134770",
                    "recipient_address": "0x33f65788aca48d733c2c2444ac9f79b18206aa92"
                }
            }),
        };

        let legs = refresh_exchange_quote_transition_legs(
            "hyperliquid_trade:hyperliquid:hyperliquid:UBTC->hyperliquid:UBTC",
            MarketOrderTransitionKind::HyperliquidTrade,
            ProviderId::HyperliquidSpot,
            &quote,
        )
        .expect("spot transfer quote should materialize");

        assert_eq!(legs.len(), 1);
        let leg = &legs[0];
        assert_eq!(
            leg.transition_decl_id,
            "hyperliquid_trade:hyperliquid:hyperliquid:UBTC->hyperliquid:UBTC"
        );
        assert_eq!(
            leg.transition_parent_decl_id,
            "hyperliquid_trade:hyperliquid:hyperliquid:UBTC->hyperliquid:UBTC"
        );
        assert_eq!(
            leg.transition_kind,
            MarketOrderTransitionKind::HyperliquidTrade
        );
        assert_eq!(
            leg.execution_step_type,
            OrderExecutionStepType::HyperliquidTrade
        );
        assert_eq!(leg.provider, ProviderId::HyperliquidSpot);
        assert_eq!(leg.input_asset.chain_id.as_str(), "hyperliquid");
        assert_eq!(leg.input_asset.asset.as_str(), "UBTC");
        assert_eq!(leg.output_asset.chain_id.as_str(), "hyperliquid");
        assert_eq!(leg.output_asset.asset.as_str(), "UBTC");
        assert_eq!(leg.amount_in, "134770");
        assert_eq!(leg.amount_out, "134770");
        assert_eq!(leg.expires_at, expires_at);
        assert_eq!(leg.raw["kind"], json!("spot_transfer"));
        assert_eq!(
            leg.raw["recipient_address"],
            json!("0x33f65788aca48d733c2c2444ac9f79b18206aa92")
        );
    }
}
