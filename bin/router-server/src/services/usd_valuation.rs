use crate::{
    models::{MarketOrderQuote, OrderExecutionLeg, OrderExecutionStep},
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        asset_registry::{AssetRegistry, CanonicalAsset},
        pricing::{pow10, PricingSnapshot, STATIC_BOOTSTRAP_PRICING_SOURCE},
        quote_legs::QuoteLeg,
    },
};
use alloy::primitives::U256;
use serde_json::{json, Map, Value};
use std::str::FromStr;

const USD_VALUATION_SCHEMA_VERSION: u32 = 1;

#[must_use]
pub fn empty_usd_valuation() -> Value {
    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "amounts": {},
        "legs": [],
    })
}

#[must_use]
pub fn quote_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: Option<&PricingSnapshot>,
    quote: &MarketOrderQuote,
) -> Value {
    let Some(pricing) = usable_pricing(pricing) else {
        return empty_usd_valuation();
    };

    let mut amounts = Map::new();
    insert_amount(
        &mut amounts,
        "input",
        asset_registry,
        pricing,
        &quote.source_asset,
        &quote.amount_in,
    );
    insert_amount(
        &mut amounts,
        "output",
        asset_registry,
        pricing,
        &quote.destination_asset,
        &quote.amount_out,
    );
    if let Some(min_amount_out) = &quote.min_amount_out {
        insert_amount(
            &mut amounts,
            "minOutput",
            asset_registry,
            pricing,
            &quote.destination_asset,
            min_amount_out,
        );
    }
    if let Some(max_amount_in) = &quote.max_amount_in {
        insert_amount(
            &mut amounts,
            "maxInput",
            asset_registry,
            pricing,
            &quote.source_asset,
            max_amount_in,
        );
    }

    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "pricing": pricing_metadata(pricing),
        "amounts": amounts,
        "legs": quote_leg_usd_valuations(asset_registry, pricing, &quote.provider_quote),
    })
}

#[must_use]
pub fn execution_step_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: Option<&PricingSnapshot>,
    step: &OrderExecutionStep,
    response: Option<&Value>,
) -> Value {
    let Some(pricing) = usable_pricing(pricing) else {
        return empty_usd_valuation();
    };

    let mut amounts = Map::new();
    if let (Some(asset), Some(amount)) = (&step.input_asset, &step.amount_in) {
        insert_amount(
            &mut amounts,
            "plannedInput",
            asset_registry,
            pricing,
            asset,
            amount,
        );
    }
    if let (Some(asset), Some(amount)) = (&step.output_asset, &step.min_amount_out) {
        insert_amount(
            &mut amounts,
            "plannedMinOutput",
            asset_registry,
            pricing,
            asset,
            amount,
        );
    }
    if let Some(response) = response {
        insert_observed_amounts(&mut amounts, asset_registry, pricing, response);
    }

    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "pricing": pricing_metadata(pricing),
        "amounts": amounts,
        "legs": [],
    })
}

#[must_use]
pub fn execution_leg_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: Option<&PricingSnapshot>,
    leg: &OrderExecutionLeg,
) -> Value {
    let Some(pricing) = usable_pricing(pricing) else {
        return empty_usd_valuation();
    };

    let mut amounts = Map::new();
    insert_amount(
        &mut amounts,
        "plannedInput",
        asset_registry,
        pricing,
        &leg.input_asset,
        &leg.amount_in,
    );
    insert_amount(
        &mut amounts,
        "plannedOutput",
        asset_registry,
        pricing,
        &leg.output_asset,
        &leg.expected_amount_out,
    );
    insert_amount(
        &mut amounts,
        "plannedMinOutput",
        asset_registry,
        pricing,
        &leg.output_asset,
        leg.min_amount_out
            .as_deref()
            .unwrap_or(&leg.expected_amount_out),
    );
    if let Some(actual_amount_in) = &leg.actual_amount_in {
        insert_amount(
            &mut amounts,
            "actualInput",
            asset_registry,
            pricing,
            &leg.input_asset,
            actual_amount_in,
        );
    }
    if let Some(actual_amount_out) = &leg.actual_amount_out {
        insert_amount(
            &mut amounts,
            "actualOutput",
            asset_registry,
            pricing,
            &leg.output_asset,
            actual_amount_out,
        );
    }

    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "pricing": pricing_metadata(pricing),
        "amounts": amounts,
        "legs": [],
    })
}

#[must_use]
pub fn pricing_has_live_usd_values(pricing: &PricingSnapshot) -> bool {
    pricing.source != STATIC_BOOTSTRAP_PRICING_SOURCE
}

fn usable_pricing(pricing: Option<&PricingSnapshot>) -> Option<&PricingSnapshot> {
    pricing.filter(|snapshot| pricing_has_live_usd_values(snapshot))
}

fn quote_leg_usd_valuations(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    provider_quote: &Value,
) -> Vec<Value> {
    let Some(legs) = provider_quote.get("legs").and_then(Value::as_array) else {
        return vec![];
    };

    legs.iter()
        .enumerate()
        .filter_map(|(index, leg)| quote_leg_usd_valuation(asset_registry, pricing, index, leg))
        .collect()
}

fn quote_leg_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    index: usize,
    leg: &Value,
) -> Option<Value> {
    let leg = serde_json::from_value::<QuoteLeg>(leg.clone()).ok()?;
    let input_asset = leg.input_deposit_asset().ok()?;
    let output_asset = leg.output_deposit_asset().ok()?;
    let mut amounts = Map::new();
    insert_amount(
        &mut amounts,
        "input",
        asset_registry,
        pricing,
        &input_asset,
        &leg.amount_in,
    );
    insert_amount(
        &mut amounts,
        "output",
        asset_registry,
        pricing,
        &output_asset,
        &leg.amount_out,
    );
    Some(json!({
        "index": index,
        "transitionDeclId": leg.transition_decl_id,
        "amounts": amounts,
    }))
}

fn insert_observed_amounts(
    amounts: &mut Map<String, Value>,
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    response: &Value,
) {
    let Some(probes) = response
        .get("balance_observation")
        .and_then(|value| value.get("probes"))
        .and_then(Value::as_array)
    else {
        return;
    };

    for probe in probes {
        let Some(role) = probe.get("role").and_then(Value::as_str) else {
            continue;
        };
        let Some(asset) = probe_asset(probe) else {
            continue;
        };
        match role {
            "source" => {
                if let Some(amount) = probe
                    .get("debit_delta")
                    .and_then(Value::as_str)
                    .filter(|amount| !is_zero_raw_amount(amount))
                {
                    insert_amount(
                        amounts,
                        "actualInput",
                        asset_registry,
                        pricing,
                        &asset,
                        amount,
                    );
                }
            }
            "destination" => {
                if let Some(amount) = probe.get("credit_delta").and_then(Value::as_str) {
                    insert_amount(
                        amounts,
                        "actualOutput",
                        asset_registry,
                        pricing,
                        &asset,
                        amount,
                    );
                }
            }
            _ => {}
        }
    }
}

fn is_zero_raw_amount(amount: &str) -> bool {
    amount.chars().all(|character| character == '0')
}

fn probe_asset(probe: &Value) -> Option<DepositAsset> {
    let asset = probe.get("asset")?;
    let chain = asset.get("chain").and_then(Value::as_str)?;
    let asset_id = asset.get("asset").and_then(Value::as_str)?;
    DepositAsset {
        chain: ChainId::parse(chain).ok()?,
        asset: AssetId::parse(asset_id).ok()?,
    }
    .normalized_asset_identity()
    .ok()
}

fn insert_amount(
    amounts: &mut Map<String, Value>,
    key: &str,
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    asset: &DepositAsset,
    raw_amount: &str,
) {
    if let Some(valuation) = amount_valuation(asset_registry, pricing, asset, raw_amount) {
        amounts.insert(key.to_string(), valuation);
    }
}

fn amount_valuation(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    asset: &DepositAsset,
    raw_amount: &str,
) -> Option<Value> {
    let chain_asset = asset_registry.chain_asset(asset)?;
    let unit_usd_micro = canonical_unit_usd_micro(pricing, chain_asset.canonical)?;
    let raw = U256::from_str(raw_amount).ok()?;
    let amount_usd_micro =
        raw.saturating_mul(U256::from(unit_usd_micro)) / pow10(chain_asset.decimals);

    Some(json!({
        "raw": raw_amount,
        "asset": {
            "chainId": asset.chain.as_str(),
            "assetId": asset.asset.as_str(),
        },
        "canonical": chain_asset.canonical.as_str(),
        "decimals": chain_asset.decimals,
        "unitUsdMicro": unit_usd_micro.to_string(),
        "amountUsdMicro": amount_usd_micro.to_string(),
    }))
}

fn canonical_unit_usd_micro(pricing: &PricingSnapshot, canonical: CanonicalAsset) -> Option<u64> {
    match canonical {
        CanonicalAsset::Usdc | CanonicalAsset::Usdt => Some(pricing.stable_usd_micro),
        CanonicalAsset::Eth => Some(pricing.eth_usd_micro),
        CanonicalAsset::Btc => Some(pricing.btc_usd_micro),
        CanonicalAsset::Cbbtc | CanonicalAsset::Hype => None,
    }
}

fn pricing_metadata(pricing: &PricingSnapshot) -> Value {
    json!({
        "source": &pricing.source,
        "capturedAt": &pricing.captured_at,
        "expiresAt": pricing.expires_at.as_ref(),
    })
}
