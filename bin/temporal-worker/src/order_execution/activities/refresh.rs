use super::*;

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
        order_kind: original_quote.order_kind,
        amount_in,
        amount_out,
        min_amount_out: original_quote.min_amount_out.clone(),
        max_amount_in: None,
        slippage_bps: original_quote.slippage_bps,
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
        expires_at,
        created_at,
    }
}

pub(super) fn refreshed_market_order_quote_exact_out(
    order: &RouterOrder,
    original_quote: &MarketOrderQuote,
    transitions: &[TransitionDecl],
    legs: Vec<QuoteLeg>,
    amount_in: String,
    max_amount_in: String,
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
        order_kind: original_quote.order_kind,
        amount_in,
        amount_out: original_quote.amount_out.clone(),
        min_amount_out: None,
        max_amount_in: Some(max_amount_in),
        slippage_bps: original_quote.slippage_bps,
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

pub(super) fn refresh_add_amount(
    field: &'static str,
    amount: &str,
    add: U256,
    label: &'static str,
) -> Result<String, OrderActivityError> {
    let amount = refresh_parse_u256_amount(field, amount)?;
    amount
        .checked_add(add)
        .ok_or_else(|| refresh_materialization_error(format!("{field} plus {label} overflowed")))
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

pub(super) fn refresh_add_hyperliquid_spot_send_quote_gas_reserve(
    field: &'static str,
    value: &str,
) -> Result<String, OrderActivityError> {
    refresh_add_amount(
        field,
        value,
        U256::from(REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW),
        "Hyperliquid spot token transfer gas reserve",
    )
}

pub(super) fn refresh_transition_min_amount_out(
    transitions: &[TransitionDecl],
    index: usize,
    final_min_amount_out: &Option<String>,
) -> Option<String> {
    if index + 1 == transitions.len() {
        final_min_amount_out.clone()
    } else {
        Some("1".to_string())
    }
}

pub(super) fn refresh_exact_out_max_input(
    start_transition_index: usize,
    index: usize,
    available_amount: &str,
) -> Option<String> {
    if index == start_transition_index {
        Some(available_amount.to_string())
    } else {
        Some(REFRESH_PROBE_MAX_AMOUNT_IN.to_string())
    }
}

pub(super) fn flatten_refresh_transition_legs(
    legs_per_transition: Vec<Vec<QuoteLeg>>,
) -> Vec<QuoteLeg> {
    let mut flattened = Vec::new();
    for mut transition_legs in legs_per_transition {
        flattened.append(&mut transition_legs);
    }
    flattened
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

pub(super) fn parse_refresh_amount(
    field: &'static str,
    value: &str,
) -> Result<u128, OrderActivityError> {
    value
        .parse::<u128>()
        .map_err(|source| amount_parse_error(format!("refreshed quote {field}"), source))
}

pub(super) fn set_json_value(target: &mut Value, key: &'static str, value: Value) {
    if !target.is_object() {
        *target = json!({});
    }
    if let Some(object) = target.as_object_mut() {
        object.insert(key.to_string(), value);
    }
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
        let refreshed_quote = match original_quote.order_kind {
            MarketOrderKindType::ExactIn => {
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
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: cursor_amount.clone(),
                                        min_amount_out: refresh_transition_min_amount_out(
                                            &transitions,
                                            index,
                                            &original_quote.min_amount_out,
                                        ),
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
                                    order_kind: MarketOrderKind::ExactIn {
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
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: quote_amount_in,
                                        min_amount_out: refresh_transition_min_amount_out(
                                            &transitions,
                                            index,
                                            &original_quote.min_amount_out,
                                        ),
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
                if let Some(min_amount_out) = original_quote.min_amount_out.as_deref() {
                    let quoted_amount_out = parse_refresh_amount("amount_out", &cursor_amount)?;
                    let min_amount_out_raw =
                        parse_refresh_amount("min_amount_out", min_amount_out)?;
                    if quoted_amount_out < min_amount_out_raw {
                        return Ok(RefreshedQuoteAttemptShape {
                            outcome: RefreshedQuoteAttemptOutcome::Untenable {
                                order_id: input.order_id,
                                stale_attempt_id: input.stale_attempt_id,
                                failed_step_id: input.failed_step_id,
                                reason: StaleQuoteRefreshUntenableReason::RefreshedExactInOutputBelowMinAmountOut {
                                    amount_out: cursor_amount,
                                    min_amount_out: min_amount_out.to_string(),
                                },
                            },
                        });
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
            }
            MarketOrderKindType::ExactOut => {
                let mut required_output = original_quote.amount_out.clone();
                let mut expires_at = now + chrono::Duration::minutes(10);
                let mut refreshed_legs_per_transition: Vec<Vec<QuoteLeg>> =
                    (0..transitions.len()).map(|_| Vec::new()).collect();
                for index in (start_transition_index..transitions.len()).rev() {
                    let transition = &transitions[index];
                    match transition.kind {
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
                            refreshed_legs_per_transition[index].push(
                                refresh_unit_withdrawal_quote_leg(
                                    transition,
                                    &required_output,
                                    &recipient_address,
                                    expires_at,
                                ),
                            );
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
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
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
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
                            let mut next_required = refreshed_quote.amount_in.clone();
                            if index > 0
                                && transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                next_required =
                                    refresh_add_hyperliquid_spot_send_quote_gas_reserve(
                                        "hyperliquid_trade.amount_in",
                                        &next_required,
                                    )?;
                            }
                            required_output = next_required;
                            refreshed_legs_per_transition[index] =
                                refresh_exchange_quote_transition_legs(
                                    transition.id.as_str(),
                                    transition.kind,
                                    transition.provider,
                                    &refreshed_quote,
                                )?;
                        }
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
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
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
                            required_output = refreshed_quote.amount_in.clone();
                            refreshed_legs_per_transition[index] =
                                refresh_exchange_quote_transition_legs(
                                    transition.id.as_str(),
                                    transition.kind,
                                    transition.provider,
                                    &refreshed_quote,
                                )?;
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
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: refresh_exact_out_max_input(
                                            start_transition_index,
                                            index,
                                            &available_amount,
                                        ),
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
                            required_output = refreshed_quote.amount_in.clone();
                            refreshed_legs_per_transition[index] =
                                refresh_bridge_quote_transition_legs(transition, &refreshed_quote)?;
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            let unit = deps
                                .action_providers
                                .unit(transition.provider.as_str())
                                .ok_or_else(|| {
                                    provider_quote_not_configured(transition.provider.as_str())
                                })?;
                            if !unit.supports_deposit(&transition.input.asset) {
                                return Err(provider_quote_error(
                                    unit.id(),
                                    "provider does not support refreshed deposit",
                                ));
                            }
                            let (unit_amount_in, upstream_required, provider_quote) =
                                if let Some(fee_reserve) =
                                    refresh_unit_deposit_fee_reserve(&original_quote, transition)?
                                {
                                    (
                                        required_output.clone(),
                                        refresh_add_amount(
                                            "amount_in",
                                            &required_output,
                                            fee_reserve,
                                            "bitcoin miner fee reserve",
                                        )?,
                                        refresh_bitcoin_fee_reserve_quote(fee_reserve),
                                    )
                                } else {
                                    (required_output.clone(), required_output.clone(), json!({}))
                                };
                            refreshed_legs_per_transition[index].push(
                                refresh_unit_deposit_quote_leg(
                                    transition,
                                    &unit_amount_in,
                                    expires_at,
                                    provider_quote,
                                ),
                            );
                            required_output = upstream_required;
                        }
                    }
                }
                let required_input = parse_refresh_amount("amount_in", &required_output)?;
                let available = parse_refresh_amount("available_amount", &available_amount)?;
                if required_input > available {
                    return Ok(RefreshedQuoteAttemptShape {
                        outcome: RefreshedQuoteAttemptOutcome::Untenable {
                            order_id: input.order_id,
                            stale_attempt_id: input.stale_attempt_id,
                            failed_step_id: input.failed_step_id,
                            reason: StaleQuoteRefreshUntenableReason::RefreshedExactOutInputAboveAvailableAmount {
                                amount_in: required_output,
                                available_amount,
                            },
                        },
                    });
                }
                refreshed_market_order_quote_exact_out(
                    &order,
                    &original_quote,
                    &transitions,
                    flatten_refresh_transition_legs(refreshed_legs_per_transition),
                    required_output,
                    available_amount,
                    expires_at,
                    now,
                )
            }
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
                failure_reason: json!({
                    "reason": "stale_provider_quote_refresh",
                    "trace": "quote_refresh_workflow",
                    "stale_attempt_id": stale_attempt.id,
                    "failed_step_id": failed_step.id,
                    "superseded_attempt_id": stale_attempt.id,
                    "superseded_attempt_index": stale_attempt.attempt_index,
                    "stale_step_id": failed_step.id,
                    "stale_execution_leg_id": stale_leg.id,
                    "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
                    "refreshed_quote_id": refreshed_quote.id,
                }),
                superseded_reason: json!({
                    "reason": "superseded_by_stale_provider_quote_refresh",
                    "stale_attempt_id": stale_attempt.id,
                    "failed_step_id": failed_step.id,
                    "stale_step_id": failed_step.id,
                    "stale_execution_leg_id": stale_leg.id,
                    "provider_quote_expires_at": provider_quote_expires_at.to_rfc3339(),
                    "refreshed_quote_id": refreshed_quote.id,
                }),
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
                superseded_reason,
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
                        failure_reason,
                        superseded_reason,
                        input_custody_snapshot,
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
                    })
                    .collect(),
            })
        })
        .await
    }
}
