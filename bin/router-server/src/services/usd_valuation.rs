use crate::{
    models::{LimitOrderQuote, MarketOrderQuote, OrderExecutionLeg, OrderExecutionStep},
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        asset_registry::{AssetRegistry, CanonicalAsset, ChainAsset},
        pricing::{checked_pow10, PricingSnapshot, STATIC_BOOTSTRAP_PRICING_SOURCE},
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
    let mut errors = Vec::new();
    insert_amount(
        &mut amounts,
        &mut errors,
        "input",
        asset_registry,
        pricing,
        &quote.source_asset,
        &quote.amount_in,
    );
    insert_amount(
        &mut amounts,
        &mut errors,
        "output",
        asset_registry,
        pricing,
        &quote.destination_asset,
        &quote.amount_out,
    );
    if let Some(min_amount_out) = &quote.min_amount_out {
        insert_amount(
            &mut amounts,
            &mut errors,
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
            &mut errors,
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
        "legs": quote_leg_usd_valuations(asset_registry, pricing, &quote.provider_quote, &mut errors),
        "errors": errors,
    })
}

#[must_use]
pub fn limit_quote_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: Option<&PricingSnapshot>,
    quote: &LimitOrderQuote,
) -> Value {
    let Some(pricing) = usable_pricing(pricing) else {
        return empty_usd_valuation();
    };

    let mut amounts = Map::new();
    let mut errors = Vec::new();
    insert_amount(
        &mut amounts,
        &mut errors,
        "input",
        asset_registry,
        pricing,
        &quote.source_asset,
        &quote.input_amount,
    );
    insert_amount(
        &mut amounts,
        &mut errors,
        "output",
        asset_registry,
        pricing,
        &quote.destination_asset,
        &quote.output_amount,
    );

    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "pricing": pricing_metadata(pricing),
        "amounts": amounts,
        "legs": quote_leg_usd_valuations(asset_registry, pricing, &quote.provider_quote, &mut errors),
        "errors": errors,
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
    let mut errors = Vec::new();
    if let (Some(asset), Some(amount)) = (&step.input_asset, &step.amount_in) {
        insert_amount(
            &mut amounts,
            &mut errors,
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
            &mut errors,
            "plannedMinOutput",
            asset_registry,
            pricing,
            asset,
            amount,
        );
    }
    if let Some(response) = response {
        insert_observed_amounts(&mut amounts, &mut errors, asset_registry, pricing, response);
    }

    json!({
        "schemaVersion": USD_VALUATION_SCHEMA_VERSION,
        "pricing": pricing_metadata(pricing),
        "amounts": amounts,
        "legs": [],
        "errors": errors,
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
    let mut errors = Vec::new();
    insert_amount(
        &mut amounts,
        &mut errors,
        "plannedInput",
        asset_registry,
        pricing,
        &leg.input_asset,
        &leg.amount_in,
    );
    insert_amount(
        &mut amounts,
        &mut errors,
        "plannedOutput",
        asset_registry,
        pricing,
        &leg.output_asset,
        &leg.expected_amount_out,
    );
    insert_amount(
        &mut amounts,
        &mut errors,
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
            &mut errors,
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
            &mut errors,
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
        "errors": errors,
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
    errors: &mut Vec<String>,
) -> Vec<Value> {
    let Some(legs) = provider_quote.get("legs").and_then(Value::as_array) else {
        return vec![];
    };

    legs.iter()
        .enumerate()
        .filter_map(|(index, leg)| {
            match quote_leg_usd_valuation(asset_registry, pricing, index, leg) {
                Ok(valuation) => valuation,
                Err(error) => {
                    errors.push(error);
                    None
                }
            }
        })
        .collect()
}

fn quote_leg_usd_valuation(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    index: usize,
    leg: &Value,
) -> Result<Option<Value>, String> {
    let leg = serde_json::from_value::<QuoteLeg>(leg.clone())
        .map_err(|err| format!("quote leg {index} is malformed: {err}"))?;
    let input_asset = leg
        .input_deposit_asset()
        .map_err(|err| format!("quote leg {index} input asset is invalid: {err}"))?;
    let output_asset = leg
        .output_deposit_asset()
        .map_err(|err| format!("quote leg {index} output asset is invalid: {err}"))?;
    let mut amounts = Map::new();
    let mut amount_errors = Vec::new();
    insert_amount(
        &mut amounts,
        &mut amount_errors,
        "input",
        asset_registry,
        pricing,
        &input_asset,
        &leg.amount_in,
    );
    insert_amount(
        &mut amounts,
        &mut amount_errors,
        "output",
        asset_registry,
        pricing,
        &output_asset,
        &leg.amount_out,
    );
    Ok(Some(json!({
        "index": index,
        "transitionDeclId": leg.transition_decl_id,
        "amounts": amounts,
        "errors": amount_errors,
    })))
}

fn insert_observed_amounts(
    amounts: &mut Map<String, Value>,
    errors: &mut Vec<String>,
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    response: &Value,
) {
    let Some(balance_observation) = response.get("balance_observation") else {
        return;
    };
    let Some(probes_value) = balance_observation.get("probes") else {
        return;
    };
    let Some(probes) = probes_value.as_array() else {
        errors.push("balance_observation.probes must be an array".to_string());
        return;
    };

    for (index, probe) in probes.iter().enumerate() {
        let Some(role) = probe.get("role").and_then(Value::as_str) else {
            continue;
        };
        let asset = match probe_asset(probe) {
            Ok(Some(asset)) => asset,
            Ok(None) => continue,
            Err(error) => {
                errors.push(format!("balance observation probe {index}: {error}"));
                continue;
            }
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
                        errors,
                        "actualInput",
                        asset_registry,
                        pricing,
                        &asset,
                        amount,
                    );
                } else if probe.get("debit_delta").is_some() {
                    errors.push(format!(
                        "balance observation probe {index}: debit_delta must be a non-zero decimal string"
                    ));
                }
            }
            "destination" => {
                if let Some(amount) = probe.get("credit_delta").and_then(Value::as_str) {
                    insert_amount(
                        amounts,
                        errors,
                        "actualOutput",
                        asset_registry,
                        pricing,
                        &asset,
                        amount,
                    );
                } else if probe.get("credit_delta").is_some() {
                    errors.push(format!(
                        "balance observation probe {index}: credit_delta must be a decimal string"
                    ));
                }
            }
            _ => {}
        }
    }
}

fn is_zero_raw_amount(amount: &str) -> bool {
    amount.chars().all(|character| character == '0')
}

fn probe_asset(probe: &Value) -> Result<Option<DepositAsset>, String> {
    let Some(asset) = probe.get("asset") else {
        return Ok(None);
    };
    let chain = asset
        .get("chain")
        .and_then(Value::as_str)
        .ok_or_else(|| "asset.chain must be a string".to_string())?;
    let asset_id = asset
        .get("asset")
        .and_then(Value::as_str)
        .ok_or_else(|| "asset.asset must be a string".to_string())?;
    DepositAsset {
        chain: ChainId::parse(chain)
            .map_err(|err| format!("invalid asset.chain {chain:?}: {err}"))?,
        asset: AssetId::parse(asset_id)
            .map_err(|err| format!("invalid asset.asset {asset_id:?}: {err}"))?,
    }
    .normalized_asset_identity()
    .map(Some)
}

fn insert_amount(
    amounts: &mut Map<String, Value>,
    errors: &mut Vec<String>,
    key: &str,
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    asset: &DepositAsset,
    raw_amount: &str,
) {
    match amount_valuation_result(asset_registry, pricing, asset, raw_amount) {
        Ok(Some(valuation)) => {
            amounts.insert(key.to_string(), valuation);
        }
        Ok(None) => {}
        Err(error) => errors.push(format!("{key}: {error}")),
    }
}

#[cfg(test)]
fn amount_valuation(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    asset: &DepositAsset,
    raw_amount: &str,
) -> Option<Value> {
    amount_valuation_result(asset_registry, pricing, asset, raw_amount)
        .ok()
        .flatten()
}

fn amount_valuation_result(
    asset_registry: &AssetRegistry,
    pricing: &PricingSnapshot,
    asset: &DepositAsset,
    raw_amount: &str,
) -> Result<Option<Value>, String> {
    let Some(chain_asset) = asset_registry.chain_asset(asset) else {
        return Ok(None);
    };
    let raw = U256::from_str(raw_amount)
        .map_err(|err| format!("raw amount {raw_amount:?} is not a valid U256: {err}"))?;
    let Some(unit_usd_micro) = canonical_unit_usd_micro(pricing, chain_asset.canonical) else {
        return Ok(None);
    };
    let amount_usd_micro = raw_amount_usd_micro(pricing, chain_asset, raw).ok_or_else(|| {
        format!(
            "USD valuation overflowed for {} {} amount {raw_amount:?}",
            asset.chain.as_str(),
            asset.asset.as_str()
        )
    })?;

    Ok(Some(json!({
        "raw": raw_amount,
        "asset": {
            "chainId": asset.chain.as_str(),
            "assetId": asset.asset.as_str(),
        },
        "canonical": chain_asset.canonical.as_str(),
        "decimals": chain_asset.decimals,
        "unitUsdMicro": unit_usd_micro.to_string(),
        "amountUsdMicro": amount_usd_micro.to_string(),
    })))
}

fn raw_amount_usd_micro(
    pricing: &PricingSnapshot,
    chain_asset: &ChainAsset,
    raw: U256,
) -> Option<U256> {
    let unit_usd_micro = canonical_unit_usd_micro(pricing, chain_asset.canonical)?;
    Some(raw.checked_mul(U256::from(unit_usd_micro))? / checked_pow10(chain_asset.decimals)?)
}

fn canonical_unit_usd_micro(pricing: &PricingSnapshot, canonical: CanonicalAsset) -> Option<u64> {
    pricing.canonical_asset_usd_micro(canonical)
}

fn pricing_metadata(pricing: &PricingSnapshot) -> Value {
    json!({
        "source": &pricing.source,
        "capturedAt": &pricing.captured_at,
        "expiresAt": pricing.expires_at.as_ref(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        models::{
            LimitOrderResidualPolicy, MarketOrderKindType, OrderExecutionStep,
            OrderExecutionStepStatus, OrderExecutionStepType,
        },
        services::pricing::{PricingSnapshot, USD_MICRO},
    };
    use chrono::Utc;
    use uuid::Uuid;

    fn test_pricing() -> PricingSnapshot {
        PricingSnapshot {
            source: "test_live_pricing".to_string(),
            captured_at: Utc::now(),
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        }
    }

    #[test]
    fn limit_quote_usd_valuation_prices_top_level_amounts() {
        let pricing = PricingSnapshot {
            source: "test_live_pricing".to_string(),
            captured_at: Utc::now(),
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        };
        let quote = LimitOrderQuote {
            id: Uuid::now_v7(),
            order_id: None,
            source_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::parse("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913").unwrap(),
            },
            destination_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: "bcrt1qrecipient".to_string(),
            provider_id: "hyperliquid".to_string(),
            input_amount: "100000000".to_string(),
            output_amount: "100000".to_string(),
            residual_policy: LimitOrderResidualPolicy::Refund,
            provider_quote: json!({ "legs": [] }),
            usd_valuation: empty_usd_valuation(),
            expires_at: Utc::now(),
            created_at: Utc::now(),
        };

        let valuation =
            limit_quote_usd_valuation(&AssetRegistry::default(), Some(&pricing), &quote);

        assert_eq!(valuation["pricing"]["source"], "test_live_pricing");
        assert_eq!(valuation["amounts"]["input"]["canonical"], "usdc");
        assert_eq!(valuation["amounts"]["input"]["amountUsdMicro"], "100000000");
        assert_eq!(valuation["amounts"]["output"]["canonical"], "btc");
        assert_eq!(
            valuation["amounts"]["output"]["amountUsdMicro"],
            "100000000"
        );
    }

    #[test]
    fn quote_usd_valuation_records_malformed_quote_legs() {
        let pricing = test_pricing();
        let quote = MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: None,
            source_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::parse("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913").unwrap(),
            },
            destination_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: "bcrt1qrecipient".to_string(),
            provider_id: "hyperliquid".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "100000000".to_string(),
            amount_out: "100000".to_string(),
            min_amount_out: Some("99000".to_string()),
            max_amount_in: None,
            slippage_bps: Some(100),
            provider_quote: json!({ "legs": [{ "not": "a quote leg" }] }),
            usd_valuation: empty_usd_valuation(),
            expires_at: Utc::now(),
            created_at: Utc::now(),
        };

        let valuation = quote_usd_valuation(&AssetRegistry::default(), Some(&pricing), &quote);

        assert_eq!(valuation["legs"].as_array().unwrap().len(), 0);
        assert!(
            valuation["errors"][0]
                .as_str()
                .is_some_and(|error| error.contains("quote leg 0 is malformed")),
            "{valuation}"
        );
    }

    #[test]
    fn execution_step_usd_valuation_records_malformed_balance_probes() {
        let pricing = test_pricing();
        let now = Utc::now();
        let step = OrderExecutionStep {
            id: Uuid::now_v7(),
            order_id: Uuid::now_v7(),
            execution_attempt_id: None,
            execution_leg_id: None,
            transition_decl_id: None,
            step_index: 0,
            step_type: OrderExecutionStepType::UniversalRouterSwap,
            provider: "velora".to_string(),
            status: OrderExecutionStepStatus::Completed,
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
            request: json!({}),
            response: json!({}),
            error: json!({}),
            usd_valuation: empty_usd_valuation(),
            created_at: now,
            updated_at: now,
        };
        let response = json!({
            "balance_observation": {
                "probes": [{
                    "role": "destination",
                    "asset": {
                        "chain": "EVM:8453",
                        "asset": "native",
                    },
                    "credit_delta": "100",
                }],
            },
        });

        let valuation = execution_step_usd_valuation(
            &AssetRegistry::default(),
            Some(&pricing),
            &step,
            Some(&response),
        );

        assert!(
            valuation["errors"][0]
                .as_str()
                .is_some_and(|error| error.contains("invalid asset.chain")),
            "{valuation}"
        );
    }

    #[test]
    fn usd_valuation_prices_wrapped_bitcoin_with_btc_reference_price() {
        let pricing = PricingSnapshot {
            source: "test_live_pricing".to_string(),
            captured_at: Utc::now(),
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        };
        let asset = DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::parse("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf").unwrap(),
        };

        let valuation = amount_valuation(&AssetRegistry::default(), &pricing, &asset, "100000000")
            .expect("CBBTC should be priced");

        assert_eq!(valuation["canonical"], "cbbtc");
        assert_eq!(valuation["amountUsdMicro"], "100000000000");
    }

    #[test]
    fn usd_valuation_omits_amounts_that_overflow_value_math() {
        let pricing = PricingSnapshot {
            source: "test_live_pricing".to_string(),
            captured_at: Utc::now(),
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        };
        let asset = DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        };

        assert!(amount_valuation(
            &AssetRegistry::default(),
            &pricing,
            &asset,
            &U256::MAX.to_string()
        )
        .is_none());
    }

    #[test]
    fn usd_valuation_omits_amounts_with_unrepresentable_decimals() {
        let pricing = PricingSnapshot {
            source: "test_live_pricing".to_string(),
            captured_at: Utc::now(),
            expires_at: None,
            stable_usd_micro: USD_MICRO,
            eth_usd_micro: 3_000 * USD_MICRO,
            btc_usd_micro: 100_000 * USD_MICRO,
            ethereum_gas_price_wei: 25_000_000_000,
            arbitrum_gas_price_wei: 100_000_000,
            base_gas_price_wei: 20_000_000,
        };
        let registry = AssetRegistry::default();
        let mut asset = registry
            .chain_asset(&DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            })
            .unwrap()
            .clone();
        asset.decimals = u8::MAX;

        assert_eq!(
            raw_amount_usd_micro(&pricing, &asset, U256::from(1_u64)),
            None
        );
    }
}
