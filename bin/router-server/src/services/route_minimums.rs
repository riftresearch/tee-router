use crate::{
    models::MarketOrderKind,
    protocol::{AssetId, DepositAsset},
    services::{
        action_providers::{
            ActionProviderRegistry, BridgeProvider, BridgeQuoteRequest, ExchangeProvider,
            ExchangeQuoteRequest, UnitProvider,
        },
        asset_registry::{MarketOrderTransitionKind, TransitionPath},
    },
};
use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use snafu::Snafu;
use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};
use tokio::sync::{Mutex, RwLock};

const DEFAULT_TTL: Duration = Duration::from_secs(60);
const OPERATIONAL_MULTIPLIER: u64 = 2;
const UNIT_BTC_MINIMUM_SATS: u64 = 30_000;
const UNIT_ETH_MINIMUM_WEI: u128 = 7_000_000_000_000_000;
const HYPERLIQUID_BRIDGE_MINIMUM_USDC: u64 = 5_000_000;
const HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;
const MAX_PATH_DEPTH: usize = 5;
const TOP_K_PATHS: usize = 8;

#[derive(Debug, Snafu)]
pub enum RouteMinimumError {
    #[snafu(display("unsupported route minimum: {}", reason))]
    Unsupported { reason: String },

    #[snafu(display(
        "provider {} failed while computing route minimum: {}",
        provider,
        message
    ))]
    Provider { provider: String, message: String },

    #[snafu(display("route minimum amount field {} was invalid: {}", field, raw))]
    InvalidAmount { field: &'static str, raw: String },
}

pub type RouteMinimumResult<T> = Result<T, RouteMinimumError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RouteMinimumKey {
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
}

impl RouteMinimumKey {
    #[must_use]
    pub fn new(source_asset: &DepositAsset, destination_asset: &DepositAsset) -> Self {
        Self {
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteMinimumSnapshot {
    pub key: RouteMinimumKey,
    pub path: String,
    pub hard_min_input: String,
    pub operational_min_input: String,
    pub output_floor: String,
    pub observed: Value,
    pub computed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl RouteMinimumSnapshot {
    #[must_use]
    pub fn hard_min_input_u256(&self) -> Option<U256> {
        U256::from_str(&self.hard_min_input).ok()
    }

    #[must_use]
    pub fn operational_min_input_u256(&self) -> Option<U256> {
        U256::from_str(&self.operational_min_input).ok()
    }

    #[must_use]
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        self.expires_at > now
    }
}

#[derive(Clone)]
pub struct RouteMinimumService {
    action_providers: Arc<ActionProviderRegistry>,
    cache: Arc<RwLock<HashMap<RouteMinimumKey, RouteMinimumSnapshot>>>,
    refresh_lock: Arc<Mutex<()>>,
    ttl: Duration,
    operational_multiplier: u64,
}

impl RouteMinimumService {
    #[must_use]
    pub fn new(action_providers: Arc<ActionProviderRegistry>) -> Self {
        Self {
            action_providers,
            cache: Arc::new(RwLock::new(HashMap::new())),
            refresh_lock: Arc::new(Mutex::new(())),
            ttl: DEFAULT_TTL,
            operational_multiplier: OPERATIONAL_MULTIPLIER,
        }
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    #[must_use]
    pub fn with_operational_multiplier(mut self, multiplier: u64) -> Self {
        self.operational_multiplier = multiplier.max(1);
        self
    }

    pub async fn floor_for_route(
        &self,
        source_asset: &DepositAsset,
        destination_asset: &DepositAsset,
        recipient_address: &str,
        depositor_address: &str,
    ) -> RouteMinimumResult<RouteMinimumSnapshot> {
        let key = RouteMinimumKey::new(source_asset, destination_asset);
        let now = Utc::now();
        if let Some(snapshot) = self.cache.read().await.get(&key).cloned() {
            if snapshot.is_fresh(now) {
                return Ok(snapshot);
            }
        }

        let _guard = self.refresh_lock.lock().await;
        let now = Utc::now();
        if let Some(snapshot) = self.cache.read().await.get(&key).cloned() {
            if snapshot.is_fresh(now) {
                return Ok(snapshot);
            }
        }

        let snapshot = self
            .compute_floor(
                source_asset,
                destination_asset,
                recipient_address,
                depositor_address,
            )
            .await?;
        self.cache
            .write()
            .await
            .insert(key.clone(), snapshot.clone());
        Ok(snapshot)
    }

    pub async fn cached_floor(
        &self,
        source_asset: &DepositAsset,
        destination_asset: &DepositAsset,
    ) -> Option<RouteMinimumSnapshot> {
        let key = RouteMinimumKey::new(source_asset, destination_asset);
        self.cache.read().await.get(&key).cloned()
    }

    async fn compute_floor(
        &self,
        source_asset: &DepositAsset,
        destination_asset: &DepositAsset,
        recipient_address: &str,
        depositor_address: &str,
    ) -> RouteMinimumResult<RouteMinimumSnapshot> {
        let output_floor = route_output_floor(destination_asset)?;
        let mut paths = self
            .action_providers
            .asset_registry()
            .select_transition_paths(source_asset, destination_asset, MAX_PATH_DEPTH);
        paths.retain(is_executable_transition_path);
        paths.sort_by_key(|path| path.transitions.len());
        paths.truncate(TOP_K_PATHS);
        if paths.is_empty() {
            return Err(RouteMinimumError::Unsupported {
                reason: format!(
                    "no executable transition path from {} {} to {} {}",
                    source_asset.chain,
                    source_asset.asset,
                    destination_asset.chain,
                    destination_asset.asset
                ),
            });
        }

        let mut best: Option<ComputedRouteMinimum> = None;
        let mut last_error: Option<RouteMinimumError> = None;
        for path in paths {
            match self
                .compute_floor_for_transition_path(
                    &path,
                    &output_floor,
                    recipient_address,
                    depositor_address,
                )
                .await
            {
                Ok(Some(candidate)) => {
                    if best
                        .as_ref()
                        .map(|current| candidate.hard_min_input < current.hard_min_input)
                        .unwrap_or(true)
                    {
                        best = Some(candidate);
                    }
                }
                Ok(None) => {}
                Err(err) => last_error = Some(err),
            }
        }
        let best = match (best, last_error) {
            (Some(best), _) => best,
            (None, Some(err)) => return Err(err),
            (None, None) => {
                return Err(RouteMinimumError::Unsupported {
                    reason: format!(
                        "no transition path could compute a route minimum from {} {} to {} {}",
                        source_asset.chain,
                        source_asset.asset,
                        destination_asset.chain,
                        destination_asset.asset
                    ),
                })
            }
        };

        let operational_min_input = best
            .hard_min_input
            .saturating_mul(U256::from(self.operational_multiplier));
        let now = Utc::now();
        let expires_at = now
            + chrono::Duration::from_std(self.ttl)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
        Ok(RouteMinimumSnapshot {
            key: RouteMinimumKey::new(source_asset, destination_asset),
            path: best.path_id,
            hard_min_input: best.hard_min_input.to_string(),
            operational_min_input: operational_min_input.to_string(),
            output_floor: output_floor.to_string(),
            observed: best.observed,
            computed_at: now,
            expires_at,
        })
    }

    async fn compute_floor_for_transition_path(
        &self,
        path: &TransitionPath,
        output_floor: &U256,
        recipient_address: &str,
        depositor_address: &str,
    ) -> RouteMinimumResult<Option<ComputedRouteMinimum>> {
        let requires_unit = path.transitions.iter().any(|transition| {
            matches!(
                transition.kind,
                MarketOrderTransitionKind::UnitDeposit | MarketOrderTransitionKind::UnitWithdrawal
            )
        });
        let requires_exchange = path.transitions.iter().any(|transition| {
            matches!(
                transition.kind,
                MarketOrderTransitionKind::HyperliquidTrade
                    | MarketOrderTransitionKind::UniversalRouterSwap
            )
        });

        let unit_candidates: Vec<Option<Arc<dyn UnitProvider>>> = if requires_unit {
            self.action_providers
                .units()
                .iter()
                .filter(|unit| unit_path_compatible(unit.as_ref(), path))
                .cloned()
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        if unit_candidates.is_empty() {
            return Ok(None);
        }

        let exchange_candidates: Vec<Option<Arc<dyn ExchangeProvider>>> = if requires_exchange {
            self.action_providers
                .exchanges()
                .iter()
                .filter(|exchange| exchange_path_compatible(exchange.id(), path))
                .cloned()
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        if exchange_candidates.is_empty() {
            return Ok(None);
        }

        let mut best: Option<ComputedRouteMinimum> = None;
        let mut last_error: Option<RouteMinimumError> = None;
        for unit in &unit_candidates {
            for exchange in &exchange_candidates {
                match self
                    .compose_transition_path_floor(
                        path,
                        output_floor,
                        recipient_address,
                        depositor_address,
                        unit.as_deref(),
                        exchange.as_deref(),
                    )
                    .await
                {
                    Ok(candidate) => {
                        if best
                            .as_ref()
                            .map(|current| candidate.hard_min_input < current.hard_min_input)
                            .unwrap_or(true)
                        {
                            best = Some(candidate);
                        }
                    }
                    Err(err) => last_error = Some(err),
                }
            }
        }

        match (best, last_error) {
            (Some(best), _) => Ok(Some(best)),
            (None, Some(err)) => Err(err),
            (None, None) => Ok(None),
        }
    }

    async fn compose_transition_path_floor(
        &self,
        path: &TransitionPath,
        output_floor: &U256,
        recipient_address: &str,
        depositor_address: &str,
        unit: Option<&dyn UnitProvider>,
        exchange: Option<&dyn ExchangeProvider>,
    ) -> RouteMinimumResult<ComputedRouteMinimum> {
        let mut required_output = output_floor.to_string();
        let mut observed_legs = Vec::with_capacity(path.transitions.len());

        for index in (0..path.transitions.len()).rev() {
            let transition = &path.transitions[index];
            match transition.kind {
                MarketOrderTransitionKind::UnitWithdrawal => {
                    let unit = unit.ok_or_else(|| RouteMinimumError::Unsupported {
                        reason: "unit provider is required for unit withdrawal".to_string(),
                    })?;
                    observed_legs.push(transition_floor_leg_json(TransitionFloorLegSpec {
                        transition_decl_id: &transition.id,
                        transition_kind: transition.kind,
                        provider: unit.id(),
                        input_asset: &transition.input.asset,
                        output_asset: &transition.output.asset,
                        amount_in: &required_output,
                        amount_out: &required_output,
                        raw: json!({
                            "recipient_address": recipient_address,
                        }),
                    }));
                }
                MarketOrderTransitionKind::HyperliquidTrade => {
                    let exchange = exchange.ok_or_else(|| RouteMinimumError::Unsupported {
                        reason: "exchange provider is required for hyperliquid trade".to_string(),
                    })?;
                    let exchange_quote = quote_exchange_exact_out(
                        exchange,
                        &transition.input.asset,
                        &transition.output.asset,
                        &parse_u256("required_output", &required_output)?,
                        recipient_address,
                    )
                    .await?;
                    observed_legs.push(transition_floor_leg_json(TransitionFloorLegSpec {
                        transition_decl_id: &transition.id,
                        transition_kind: transition.kind,
                        provider: exchange.id(),
                        input_asset: &transition.input.asset,
                        output_asset: &transition.output.asset,
                        amount_in: &exchange_quote.amount_in,
                        amount_out: &exchange_quote.amount_out,
                        raw: exchange_quote.provider_quote,
                    }));
                    let mut next_required =
                        parse_u256("exchange_quote.amount_in", &exchange_quote.amount_in)?;
                    if index > 0
                        && path.transitions[index - 1].kind
                            == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                    {
                        next_required = next_required.saturating_add(U256::from(
                            HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW,
                        ));
                    }
                    required_output = next_required.to_string();
                }
                MarketOrderTransitionKind::UniversalRouterSwap => {
                    return Err(RouteMinimumError::Unsupported {
                        reason:
                            "universal router route minimums require live token decimals and are computed at quote time"
                                .to_string(),
                    });
                }
                MarketOrderTransitionKind::AcrossBridge
                | MarketOrderTransitionKind::CctpBridge
                | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                    let bridge = self.bridge(transition.provider.as_str())?;
                    let mut required_out = parse_u256("required_output", &required_output)?;
                    if transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit {
                        required_out =
                            required_out.max(U256::from(HYPERLIQUID_BRIDGE_MINIMUM_USDC));
                    }
                    let bridge_quote = quote_bridge_exact_out(
                        bridge.as_ref(),
                        &transition.input.asset,
                        &transition.output.asset,
                        &required_out,
                        depositor_address,
                        depositor_address,
                    )
                    .await?;
                    observed_legs.push(transition_floor_leg_json(TransitionFloorLegSpec {
                        transition_decl_id: &transition.id,
                        transition_kind: transition.kind,
                        provider: bridge.id(),
                        input_asset: &transition.input.asset,
                        output_asset: &transition.output.asset,
                        amount_in: &bridge_quote.amount_in,
                        amount_out: &bridge_quote.amount_out,
                        raw: bridge_quote.provider_quote,
                    }));
                    required_output = bridge_quote.amount_in;
                }
                MarketOrderTransitionKind::UnitDeposit => {
                    let unit = unit.ok_or_else(|| RouteMinimumError::Unsupported {
                        reason: "unit provider is required for unit deposit".to_string(),
                    })?;
                    let required_in = parse_u256("required_output", &required_output)?
                        .max(unit_minimum_for_asset(&transition.input.asset));
                    required_output = required_in.to_string();
                    observed_legs.push(transition_floor_leg_json(TransitionFloorLegSpec {
                        transition_decl_id: &transition.id,
                        transition_kind: transition.kind,
                        provider: unit.id(),
                        input_asset: &transition.input.asset,
                        output_asset: &transition.output.asset,
                        amount_in: &required_output,
                        amount_out: &required_output,
                        raw: json!({}),
                    }));
                }
            }
        }

        observed_legs.reverse();
        Ok(ComputedRouteMinimum {
            path_id: path.id.clone(),
            hard_min_input: parse_u256("hard_min_input", &required_output)?,
            observed: json!({
                "path_id": path.id,
                "transition_decl_ids": path.transitions.iter().map(|transition| transition.id.clone()).collect::<Vec<_>>(),
                "legs": observed_legs,
            }),
        })
    }

    fn bridge(&self, id: &str) -> RouteMinimumResult<Arc<dyn BridgeProvider>> {
        self.action_providers
            .bridge(id)
            .ok_or_else(|| RouteMinimumError::Unsupported {
                reason: format!("bridge provider {id:?} is not configured"),
            })
    }
}

#[derive(Debug, Clone)]
struct ComputedRouteMinimum {
    path_id: String,
    hard_min_input: U256,
    observed: Value,
}

async fn quote_exchange_exact_out(
    exchange: &dyn ExchangeProvider,
    input_asset: &DepositAsset,
    output_asset: &DepositAsset,
    amount_out: &U256,
    recipient_address: &str,
) -> RouteMinimumResult<crate::services::action_providers::ExchangeQuote> {
    exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: input_asset.clone(),
            output_asset: output_asset.clone(),
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactOut {
                amount_out: amount_out.to_string(),
                max_amount_in: exact_out_cap_for_asset(input_asset).to_string(),
            },
            sender_address: None,
            recipient_address: recipient_address.to_string(),
        })
        .await
        .map_err(|message| RouteMinimumError::Provider {
            provider: exchange.id().to_string(),
            message,
        })?
        .ok_or_else(|| RouteMinimumError::Unsupported {
            reason: format!(
                "exchange provider {} cannot quote exact-out {} {} -> {} {}",
                exchange.id(),
                input_asset.chain,
                input_asset.asset,
                output_asset.chain,
                output_asset.asset
            ),
        })
}

async fn quote_bridge_exact_out(
    bridge: &dyn BridgeProvider,
    source_asset: &DepositAsset,
    destination_asset: &DepositAsset,
    amount_out: &U256,
    recipient_address: &str,
    depositor_address: &str,
) -> RouteMinimumResult<crate::services::action_providers::BridgeQuote> {
    bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            order_kind: MarketOrderKind::ExactOut {
                amount_out: amount_out.to_string(),
                max_amount_in: exact_out_cap_for_asset(source_asset).to_string(),
            },
            recipient_address: recipient_address.to_string(),
            depositor_address: depositor_address.to_string(),
            partial_fills_enabled: false,
        })
        .await
        .map_err(|message| RouteMinimumError::Provider {
            provider: bridge.id().to_string(),
            message,
        })?
        .ok_or_else(|| RouteMinimumError::Unsupported {
            reason: format!(
                "bridge provider {} cannot quote exact-out {} {} -> {} {}",
                bridge.id(),
                source_asset.chain,
                source_asset.asset,
                destination_asset.chain,
                destination_asset.asset
            ),
        })
}

fn route_output_floor(destination_asset: &DepositAsset) -> RouteMinimumResult<U256> {
    if destination_asset.chain.as_str() == "bitcoin" && destination_asset.asset.is_native() {
        return Ok(U256::from(UNIT_BTC_MINIMUM_SATS));
    }
    Err(RouteMinimumError::Unsupported {
        reason: format!(
            "no route output floor for {} {}",
            destination_asset.chain, destination_asset.asset
        ),
    })
}

fn is_executable_transition_path(path: &TransitionPath) -> bool {
    if path.transitions.is_empty() {
        return false;
    }
    matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::UnitWithdrawal)
    ) || matches!(
        path.transitions.last().map(|transition| transition.kind),
        Some(MarketOrderTransitionKind::UniversalRouterSwap)
    )
}

fn unit_path_compatible(unit: &dyn UnitProvider, path: &TransitionPath) -> bool {
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

fn exchange_path_compatible(exchange_id: &str, path: &TransitionPath) -> bool {
    path.transitions
        .iter()
        .all(|transition| match transition.kind {
            MarketOrderTransitionKind::HyperliquidTrade
            | MarketOrderTransitionKind::UniversalRouterSwap => {
                transition.provider.as_str() == exchange_id
            }
            _ => true,
        })
}

struct TransitionFloorLegSpec<'a> {
    transition_decl_id: &'a str,
    transition_kind: MarketOrderTransitionKind,
    provider: &'a str,
    input_asset: &'a DepositAsset,
    output_asset: &'a DepositAsset,
    amount_in: &'a str,
    amount_out: &'a str,
    raw: Value,
}

fn transition_floor_leg_json(spec: TransitionFloorLegSpec<'_>) -> Value {
    json!({
        "transition_decl_id": spec.transition_decl_id,
        "transition_kind": spec.transition_kind.as_str(),
        "provider": spec.provider,
        "input_asset": {
            "chain_id": spec.input_asset.chain.as_str(),
            "asset": spec.input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": spec.output_asset.chain.as_str(),
            "asset": spec.output_asset.asset.as_str(),
        },
        "amount_in": spec.amount_in,
        "amount_out": spec.amount_out,
        "raw": spec.raw,
    })
}

fn unit_minimum_for_asset(asset: &DepositAsset) -> U256 {
    match (asset.chain.as_str(), &asset.asset) {
        ("bitcoin", AssetId::Native) => U256::from(UNIT_BTC_MINIMUM_SATS),
        ("evm:1", AssetId::Native) => U256::from(UNIT_ETH_MINIMUM_WEI),
        _ => U256::ZERO,
    }
}

fn exact_out_cap_for_asset(asset: &DepositAsset) -> &'static str {
    match (asset.chain.as_str(), &asset.asset) {
        ("bitcoin", AssetId::Native) => "10000000000000",
        ("evm:1" | "evm:8453", AssetId::Native) => "1000000000000000000000",
        ("hyperliquid", AssetId::Native) => "1000000000000000",
        ("hyperliquid", AssetId::Reference(symbol)) if symbol == "UETH" => "1000000000000000000000",
        ("hyperliquid", AssetId::Reference(symbol)) if symbol == "UBTC" => "10000000000000",
        (_, AssetId::Reference(_)) => "1000000000000000",
        _ => "1000000000000000",
    }
}

fn parse_u256(field: &'static str, raw: &str) -> RouteMinimumResult<U256> {
    U256::from_str(raw).map_err(|_| RouteMinimumError::InvalidAmount {
        field,
        raw: raw.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::ChainId;

    fn asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset,
        }
    }

    #[test]
    fn exact_out_cap_uses_hyperliquid_venue_asset_decimals() {
        assert_eq!(
            exact_out_cap_for_asset(&asset("hyperliquid", AssetId::reference("UETH"))),
            "1000000000000000000000"
        );
        assert_eq!(
            exact_out_cap_for_asset(&asset("hyperliquid", AssetId::reference("UBTC"))),
            "10000000000000"
        );
    }
}
