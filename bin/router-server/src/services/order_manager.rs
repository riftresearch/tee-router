use crate::{
    api::{CreateOrderRequest, MarketOrderQuoteKind, MarketOrderQuoteRequest},
    config::Settings,
    db::Database,
    error::RouterServerError,
    models::{
        MarketOrderAction, MarketOrderKind, MarketOrderKindType, MarketOrderQuote, RouterOrder,
        RouterOrderAction, RouterOrderQuoteEnvelope, RouterOrderStatus, RouterOrderType,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            ActionProviderRegistry, BridgeProvider, BridgeQuote, BridgeQuoteRequest,
            ExchangeProvider, ExchangeQuote, ExchangeQuoteRequest, UnitProvider,
        },
        asset_registry::{
            AssetRegistry, MarketOrderTransitionKind, ProviderId, TransitionDecl, TransitionPath,
        },
        deposit_address::{derive_deposit_address_for_quote, DepositAddressError},
        gas_reimbursement::{
            optimized_paymaster_reimbursement_plan,
            optimized_paymaster_reimbursement_plan_with_pricing, transition_retention_amount,
            GasReimbursementError, GasReimbursementPlan,
        },
        provider_policy::ProviderPolicyService,
        quote_legs::{QuoteLeg, QuoteLegAsset, QuoteLegSpec},
        route_costs::{rank_transition_paths_structurally, RouteCostService},
        route_minimums::{RouteMinimumError, RouteMinimumService},
    },
    telemetry,
};
use alloy::primitives::U256;
use chains::ChainRegistry;
use chrono::{Duration as ChronoDuration, Utc};
use router_primitives::ChainType;
use serde_json::{json, Value};
use snafu::Snafu;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::warn;
use uuid::Uuid;

const MARKET_ORDER_ACTION_TIMEOUT: ChronoDuration = ChronoDuration::minutes(10);
const MARKET_ORDER_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS: usize = 2;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS: usize = 2;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS: u64 = 12_500;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_FALLBACK_SATS: u64 = 25_000;

#[derive(Debug, Snafu)]
pub enum MarketOrderError {
    #[snafu(display("Chain not supported: {}", chain))]
    ChainNotSupported { chain: ChainId },

    #[snafu(display("Invalid asset id {} for {}: {}", asset, chain, reason))]
    InvalidAssetId {
        asset: String,
        chain: ChainId,
        reason: String,
    },

    #[snafu(display("Invalid recipient address {} for {}", address, chain))]
    InvalidRecipientAddress { address: String, chain: ChainId },

    #[snafu(display("Invalid refund address {} for {}", address, chain))]
    InvalidRefundAddress { address: String, chain: ChainId },

    #[snafu(display("Invalid amount {}: {}", field, reason))]
    InvalidAmount { field: &'static str, reason: String },

    #[snafu(display("Invalid idempotency key: {}", reason))]
    InvalidIdempotencyKey { reason: String },

    #[snafu(display("No market-order route found: {}", reason))]
    NoRoute { reason: String },

    #[snafu(display(
        "Input amount {} is below operational route minimum {} (hard minimum {}) for {} {} to {} {}",
        amount_in,
        operational_min_input,
        hard_min_input,
        source_chain,
        source_asset,
        destination_chain,
        destination_asset
    ))]
    InputBelowRouteMinimum {
        amount_in: String,
        operational_min_input: String,
        hard_min_input: String,
        source_chain: Box<ChainId>,
        source_asset: Box<AssetId>,
        destination_chain: Box<ChainId>,
        destination_asset: Box<AssetId>,
    },

    #[snafu(display("Route minimum check failed: {}", reason))]
    RouteMinimum { reason: String },

    #[snafu(display("Gas reimbursement planning failed: {}", source))]
    GasReimbursement { source: Box<GasReimbursementError> },

    #[snafu(display("Quote is expired"))]
    QuoteExpired,

    #[snafu(display("Quote has already been used to create an order"))]
    QuoteAlreadyOrdered,

    #[snafu(display("Deposit address derivation failed: {}", source))]
    DepositAddress { source: Box<DepositAddressError> },

    #[snafu(display("Database error: {}", source))]
    Database { source: Box<RouterServerError> },
}

pub type MarketOrderResult<T> = Result<T, MarketOrderError>;

impl MarketOrderError {
    fn gas_reimbursement(source: GasReimbursementError) -> Self {
        Self::GasReimbursement {
            source: Box::new(source),
        }
    }

    fn deposit_address(source: DepositAddressError) -> Self {
        Self::DepositAddress {
            source: Box::new(source),
        }
    }

    fn database(source: RouterServerError) -> Self {
        Self::Database {
            source: Box::new(source),
        }
    }
}

#[derive(Debug, Clone)]
struct ComposedMarketOrderQuote {
    pub provider_id: String,
    pub amount_in: String,
    pub amount_out: String,
    pub min_amount_out: Option<String>,
    pub max_amount_in: Option<String>,
    pub provider_quote: Value,
    pub expires_at: chrono::DateTime<Utc>,
}

struct ComposeTransitionPathQuoteRequest<'a> {
    request: &'a NormalizedMarketOrderQuoteRequest,
    order_kind: &'a MarketOrderKind,
    quote_id: Uuid,
    source_depositor_address: &'a str,
    path: &'a TransitionPath,
    provider_policy_snapshot: Option<&'a crate::services::ProviderPolicySnapshot>,
    unit: Option<&'a dyn UnitProvider>,
    exchange: Option<&'a dyn ExchangeProvider>,
}

pub struct OrderManager {
    db: Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    asset_registry: Arc<AssetRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    route_minimums: Option<Arc<RouteMinimumService>>,
    route_costs: Option<Arc<RouteCostService>>,
    provider_policies: Option<Arc<ProviderPolicyService>>,
}

impl OrderManager {
    #[must_use]
    pub fn new(db: Database, settings: Arc<Settings>, chain_registry: Arc<ChainRegistry>) -> Self {
        Self::with_action_providers(
            db,
            settings,
            chain_registry,
            Arc::new(ActionProviderRegistry::default()),
        )
    }

    #[must_use]
    pub fn with_action_providers(
        db: Database,
        settings: Arc<Settings>,
        chain_registry: Arc<ChainRegistry>,
        action_providers: Arc<ActionProviderRegistry>,
    ) -> Self {
        let asset_registry = action_providers.asset_registry();
        Self {
            db,
            settings,
            chain_registry,
            asset_registry,
            action_providers,
            route_minimums: None,
            route_costs: None,
            provider_policies: None,
        }
    }

    #[must_use]
    pub fn with_route_minimums(mut self, route_minimums: Option<Arc<RouteMinimumService>>) -> Self {
        self.route_minimums = route_minimums;
        self
    }

    #[must_use]
    pub fn with_route_costs(mut self, route_costs: Option<Arc<RouteCostService>>) -> Self {
        self.route_costs = route_costs;
        self
    }

    #[must_use]
    pub fn with_provider_policies(
        mut self,
        provider_policies: Option<Arc<ProviderPolicyService>>,
    ) -> Self {
        self.provider_policies = provider_policies;
        self
    }

    pub async fn quote_market_order(
        &self,
        request: MarketOrderQuoteRequest,
    ) -> MarketOrderResult<RouterOrderQuoteEnvelope> {
        let started = Instant::now();
        let normalized_request = self.validate_and_normalize_quote_request(request)?;
        telemetry::record_market_order_quote_requested(&normalized_request.source_asset);

        let quote_id = Uuid::now_v7();
        let (depositor_address, _) = derive_deposit_address_for_quote(
            self.chain_registry.as_ref(),
            &self.settings.master_key_bytes(),
            quote_id,
            &normalized_request.source_asset.chain,
        )
        .map_err(MarketOrderError::deposit_address)?;

        self.validate_route_minimum(&normalized_request, &depositor_address)
            .await?;

        let provider_quote = match self
            .best_provider_quote(&normalized_request, quote_id, &depositor_address)
            .await
        {
            Ok(provider_quote) => provider_quote,
            Err(err @ MarketOrderError::NoRoute { .. }) => {
                telemetry::record_market_order_quote_no_route(
                    &normalized_request.source_asset,
                    "providers_returned_no_route",
                    started.elapsed(),
                );
                return Err(err);
            }
            Err(err) => return Err(err),
        };
        let now = Utc::now();
        let quote = MarketOrderQuote {
            id: quote_id,
            order_id: None,
            source_asset: normalized_request.source_asset,
            destination_asset: normalized_request.destination_asset,
            recipient_address: normalized_request.recipient_address,
            provider_id: provider_quote.provider_id,
            order_kind: normalized_request.order_kind.kind_type(),
            amount_in: provider_quote.amount_in,
            amount_out: provider_quote.amount_out,
            min_amount_out: provider_quote.min_amount_out,
            max_amount_in: provider_quote.max_amount_in,
            slippage_bps: normalized_request.order_kind.slippage_bps(),
            provider_quote: provider_quote.provider_quote,
            expires_at: provider_quote.expires_at,
            created_at: now,
        };

        self.db
            .orders()
            .create_market_order_quote(&quote)
            .await
            .map_err(MarketOrderError::database)?;

        telemetry::record_market_order_quote_success(&quote.source_asset, started.elapsed());
        Ok(RouterOrderQuoteEnvelope {
            quote: quote.into(),
        })
    }

    pub async fn get_quote(&self, quote_id: Uuid) -> MarketOrderResult<MarketOrderQuote> {
        self.db
            .orders()
            .get_market_order_quote_by_id(quote_id)
            .await
            .map_err(MarketOrderError::database)
    }

    pub async fn get_order(&self, order_id: Uuid) -> MarketOrderResult<RouterOrder> {
        self.db
            .orders()
            .get(order_id)
            .await
            .map_err(MarketOrderError::database)
    }

    pub async fn get_quote_for_order(&self, order_id: Uuid) -> MarketOrderResult<MarketOrderQuote> {
        self.db
            .orders()
            .get_market_order_quote(order_id)
            .await
            .map_err(MarketOrderError::database)
    }

    pub async fn mark_order_refunding(
        &self,
        order: &RouterOrder,
    ) -> MarketOrderResult<RouterOrder> {
        match order.status {
            RouterOrderStatus::Refunding
            | RouterOrderStatus::Refunded
            | RouterOrderStatus::RefundManualInterventionRequired => Ok(order.clone()),
            RouterOrderStatus::PendingFunding
            | RouterOrderStatus::Funded
            | RouterOrderStatus::RefundRequired => self
                .db
                .orders()
                .transition_status(
                    order.id,
                    order.status,
                    RouterOrderStatus::Refunding,
                    Utc::now(),
                )
                .await
                .map_err(MarketOrderError::database),
            _ => Ok(order.clone()),
        }
    }

    pub async fn create_order_from_quote(
        &self,
        request: CreateOrderRequest,
    ) -> MarketOrderResult<(RouterOrder, MarketOrderQuote)> {
        let quote = self.get_quote(request.quote_id).await?;
        if quote.order_id.is_some() {
            return self.resume_unvaulted_order_from_quote(request, quote).await;
        }
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::QuoteExpired);
        }

        let now = Utc::now();
        let refund_address = self.validate_and_normalize_refund_address(
            &quote.source_asset.chain,
            &request.refund_address,
        )?;
        let order = RouterOrder {
            id: Uuid::now_v7(),
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Quoted,
            funding_vault_id: None,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address: quote.recipient_address.clone(),
            refund_address,
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: market_order_kind_from_quote(&quote)?,
                slippage_bps: quote.slippage_bps,
            }),
            action_timeout_at: now + MARKET_ORDER_ACTION_TIMEOUT,
            idempotency_key: normalize_idempotency_key(request.idempotency_key)?,
            created_at: now,
            updated_at: now,
        };

        let quote = self
            .db
            .orders()
            .create_market_order_from_quote(&order, quote.id)
            .await
            .map_err(MarketOrderError::database)?;

        Ok((order, quote))
    }

    async fn resume_unvaulted_order_from_quote(
        &self,
        request: CreateOrderRequest,
        quote: MarketOrderQuote,
    ) -> MarketOrderResult<(RouterOrder, MarketOrderQuote)> {
        let order_id = quote
            .order_id
            .ok_or(MarketOrderError::QuoteAlreadyOrdered)?;
        let order = self
            .db
            .orders()
            .get(order_id)
            .await
            .map_err(MarketOrderError::database)?;
        let requested_idempotency_key = normalize_idempotency_key(request.idempotency_key)?;
        let requested_refund_address = self.validate_and_normalize_refund_address(
            &order.source_asset.chain,
            &request.refund_address,
        )?;
        if order.status == RouterOrderStatus::Quoted
            && order.funding_vault_id.is_none()
            && order.idempotency_key == requested_idempotency_key
            && order.refund_address == requested_refund_address
        {
            return Ok((order, quote));
        }

        Err(MarketOrderError::QuoteAlreadyOrdered)
    }

    pub async fn release_quote_after_vault_creation_failure(
        &self,
        order_id: Uuid,
        quote_id: Uuid,
    ) -> MarketOrderResult<()> {
        self.db
            .orders()
            .release_quote_and_delete_quoted_order(order_id, quote_id)
            .await
            .map_err(MarketOrderError::database)
    }

    async fn best_provider_quote(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        source_depositor_address: &str,
    ) -> MarketOrderResult<ComposedMarketOrderQuote> {
        const MAX_PATH_DEPTH: usize = 5;
        const TOP_K_PATHS: usize = 8;

        let mut paths = self.asset_registry.select_transition_paths(
            &request.source_asset,
            &request.destination_asset,
            MAX_PATH_DEPTH,
        );
        paths.retain(|path| is_executable_transition_path(self.asset_registry.as_ref(), path));
        if paths.is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: format!(
                    "no executable transition path from {} {} to {} {}",
                    request.source_asset.chain,
                    request.source_asset.asset,
                    request.destination_asset.chain,
                    request.destination_asset.asset
                ),
            });
        }
        if let Some(route_costs) = self.route_costs.as_ref() {
            if let Err(err) = route_costs.rank_transition_paths(&mut paths).await {
                warn!(
                    error = %err,
                    "route-cost ranking failed; falling back to structural route ordering"
                );
                rank_transition_paths_structurally(&mut paths);
            }
        } else {
            rank_transition_paths_structurally(&mut paths);
        }
        paths.truncate(TOP_K_PATHS);

        let provider_policy_snapshot = if let Some(service) = self.provider_policies.as_ref() {
            Some(
                service
                    .snapshot()
                    .await
                    .map_err(MarketOrderError::database)?,
            )
        } else {
            None
        };

        let mut last_error: Option<MarketOrderError> = None;
        for path in paths {
            // Path ranking is the route-selection decision. Provider quotes
            // then prove executability for that ranked path; they do not
            // re-run global path search by maximizing raw output.
            let candidate = self
                .quote_transition_path(
                    request,
                    quote_id,
                    source_depositor_address,
                    &path,
                    provider_policy_snapshot.as_ref(),
                )
                .await;
            let quote = match candidate {
                Ok(Some(quote)) => quote,
                Ok(None) => continue,
                Err(err) => {
                    last_error = Some(err);
                    continue;
                }
            };
            if let Err(err) = self.validate_provider_quote(request, &quote) {
                warn!(error = %err, "Ignoring invalid transition-path quote");
                last_error = Some(err);
                continue;
            }
            return Ok(quote);
        }

        match last_error {
            Some(err) => Err(err),
            None => Err(MarketOrderError::NoRoute {
                reason: "no executable transition path satisfied the requested constraints"
                    .to_string(),
            }),
        }
    }

    async fn validate_route_minimum(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        depositor_address: &str,
    ) -> MarketOrderResult<()> {
        let Some(route_minimums) = &self.route_minimums else {
            return Ok(());
        };
        let MarketOrderQuoteKind::ExactIn { amount_in, .. } = &request.order_kind else {
            return Ok(());
        };
        let amount_in_u256 = parse_amount("amount_in", amount_in)?;
        let snapshot = match tokio::time::timeout(
            MARKET_ORDER_PROVIDER_TIMEOUT,
            route_minimums.floor_for_route(
                &request.source_asset,
                &request.destination_asset,
                &request.recipient_address,
                depositor_address,
            ),
        )
        .await
        {
            Ok(Ok(snapshot)) => snapshot,
            Ok(Err(RouteMinimumError::Unsupported { .. })) => return Ok(()),
            Ok(Err(err)) => {
                return Err(MarketOrderError::RouteMinimum {
                    reason: err.to_string(),
                });
            }
            Err(_) => {
                return Err(MarketOrderError::RouteMinimum {
                    reason: "route minimum check timed out".to_string(),
                });
            }
        };
        let operational_min_input = snapshot.operational_min_input_u256().ok_or_else(|| {
            MarketOrderError::RouteMinimum {
                reason: format!(
                    "cached operational minimum was invalid: {}",
                    snapshot.operational_min_input
                ),
            }
        })?;

        if amount_in_u256 < operational_min_input {
            return Err(MarketOrderError::InputBelowRouteMinimum {
                amount_in: amount_in.clone(),
                operational_min_input: snapshot.operational_min_input,
                hard_min_input: snapshot.hard_min_input,
                source_chain: Box::new(request.source_asset.chain.clone()),
                source_asset: Box::new(request.source_asset.asset.clone()),
                destination_chain: Box::new(request.destination_asset.chain.clone()),
                destination_asset: Box::new(request.destination_asset.asset.clone()),
            });
        }

        Ok(())
    }

    async fn quote_transition_path(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        source_depositor_address: &str,
        path: &TransitionPath,
        provider_policy_snapshot: Option<&crate::services::ProviderPolicySnapshot>,
    ) -> MarketOrderResult<Option<ComposedMarketOrderQuote>> {
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
                .filter(|unit| {
                    provider_allowed_for_new_routes(provider_policy_snapshot, unit.id())
                        && unit_path_compatible(unit.as_ref(), path)
                })
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
                .filter(|exchange| {
                    provider_allowed_for_new_routes(provider_policy_snapshot, exchange.id())
                        && exchange_path_compatible(exchange.id(), path)
                })
                .cloned()
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        if exchange_candidates.is_empty() {
            return Ok(None);
        }

        let mut best_for_path: Option<ComposedMarketOrderQuote> = None;
        let mut last_error: Option<MarketOrderError> = None;
        for unit in &unit_candidates {
            for exchange in &exchange_candidates {
                let probe_order_kind = request.order_kind.probe_order_kind();
                let probe_quote = self
                    .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                        request,
                        order_kind: &probe_order_kind,
                        quote_id,
                        source_depositor_address,
                        path,
                        provider_policy_snapshot,
                        unit: unit.as_deref(),
                        exchange: exchange.as_deref(),
                    })
                    .await;
                let probe_quote = match probe_quote {
                    Ok(Some(quote)) => quote,
                    Ok(None) => continue,
                    Err(err) => {
                        warn!(path_id = %path.id, error = %err, "transition-path probe quote failed");
                        last_error = Some(err);
                        continue;
                    }
                };

                let bounded_order_kind = match request.order_kind.bounded_order_kind(&probe_quote) {
                    Ok(order_kind) => order_kind,
                    Err(err) => {
                        last_error = Some(err);
                        continue;
                    }
                };
                let quote = self
                    .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                        request,
                        order_kind: &bounded_order_kind,
                        quote_id,
                        source_depositor_address,
                        path,
                        provider_policy_snapshot,
                        unit: unit.as_deref(),
                        exchange: exchange.as_deref(),
                    })
                    .await;
                let quote = match quote {
                    Ok(Some(quote)) => quote,
                    Ok(None) => continue,
                    Err(err) => {
                        warn!(path_id = %path.id, error = %err, "transition-path bounded quote failed");
                        last_error = Some(err);
                        continue;
                    }
                };
                best_for_path = choose_better_quote(request, quote, best_for_path);
            }
        }

        match (best_for_path, last_error) {
            (Some(quote), _) => Ok(Some(quote)),
            (None, Some(err)) => Err(err),
            (None, None) => Ok(None),
        }
    }

    async fn compose_transition_path_quote(
        &self,
        spec: ComposeTransitionPathQuoteRequest<'_>,
    ) -> MarketOrderResult<Option<ComposedMarketOrderQuote>> {
        let ComposeTransitionPathQuoteRequest {
            request,
            order_kind,
            quote_id,
            source_depositor_address,
            path,
            provider_policy_snapshot,
            unit,
            exchange,
        } = spec;
        let mut expires_at = Utc::now() + ChronoDuration::minutes(10);
        let mut legs_per_transition: Vec<Vec<QuoteLeg>> = vec![Vec::new(); path.transitions.len()];
        let gas_reimbursement_plan = if let Some(route_costs) = self.route_costs.as_ref() {
            let pricing = route_costs.current_pricing_snapshot().await;
            optimized_paymaster_reimbursement_plan_with_pricing(
                self.asset_registry.as_ref(),
                path,
                &pricing,
            )
        } else {
            optimized_paymaster_reimbursement_plan(self.asset_registry.as_ref(), path)
        }
        .map_err(MarketOrderError::gas_reimbursement)?;

        match order_kind {
            MarketOrderKind::ExactIn {
                amount_in,
                min_amount_out,
            } => {
                let mut cursor_amount = amount_in.clone();
                for (index, transition) in path.transitions.iter().enumerate() {
                    let provider_amount_in = apply_transition_retention(
                        &gas_reimbursement_plan,
                        &transition.id,
                        "amount_in",
                        &cursor_amount,
                    )?;
                    match transition.kind {
                        MarketOrderTransitionKind::AcrossBridge
                        | MarketOrderTransitionKind::CctpBridge
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                            let bridge_id = transition.provider.as_str();
                            if !provider_allowed_for_new_routes(provider_policy_snapshot, bridge_id)
                            {
                                return Ok(None);
                            }
                            let bridge =
                                self.action_providers.bridge(bridge_id).ok_or_else(|| {
                                    MarketOrderError::NoRoute {
                                        reason: format!(
                                            "bridge provider {bridge_id} is not configured"
                                        ),
                                    }
                                })?;
                            let bridge_quote = quote_bridge(
                                bridge.as_ref(),
                                BridgeQuoteRequest {
                                    source_asset: transition.input.asset.clone(),
                                    destination_asset: transition.output.asset.clone(),
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: provider_amount_in.clone(),
                                        min_amount_out: "1".to_string(),
                                    },
                                    recipient_address: self.bridge_quote_recipient_address(
                                        request, quote_id, path, index, transition,
                                    )?,
                                    depositor_address: self.bridge_quote_depositor_address(
                                        request,
                                        quote_id,
                                        source_depositor_address,
                                        transition,
                                    )?,
                                    partial_fills_enabled: false,
                                },
                            )
                            .await?;
                            let Some(bridge_quote) = bridge_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(bridge_quote.expires_at);
                            cursor_amount = bridge_quote.amount_out.clone();
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &bridge_quote.amount_in,
                                amount_out: &bridge_quote.amount_out,
                                expires_at: bridge_quote.expires_at,
                                raw: bridge_quote.provider_quote,
                            }));
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            unit.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "unit provider is required for unit_deposit".to_string(),
                            })?;
                            let (unit_amount_in, provider_quote) = if let Some(fee_reserve) =
                                self.bitcoin_unit_deposit_fee_reserve(transition).await?
                            {
                                (
                                    subtract_bitcoin_fee_reserve(
                                        "amount_in",
                                        &provider_amount_in,
                                        fee_reserve,
                                        &transition.id,
                                    )?,
                                    bitcoin_fee_reserve_quote(fee_reserve),
                                )
                            } else {
                                (provider_amount_in.clone(), json!({}))
                            };
                            cursor_amount = unit_amount_in.clone();
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &unit_amount_in,
                                amount_out: &unit_amount_in,
                                expires_at,
                                raw: provider_quote,
                            }));
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let exchange = exchange.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "exchange provider is required for hyperliquid_trade"
                                    .to_string(),
                            })?;
                            let mut quote_amount_in = provider_amount_in.clone();
                            if index > 0
                                && path.transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                quote_amount_in = reserve_hyperliquid_spot_send_quote_gas(
                                    "hyperliquid_trade.amount_in",
                                    &quote_amount_in,
                                )?;
                            }
                            let exchange_quote = quote_exchange(
                                exchange,
                                ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: quote_amount_in,
                                        min_amount_out: transition_min_amount_out(
                                            path,
                                            index,
                                            min_amount_out,
                                        ),
                                    },
                                    sender_address: None,
                                    recipient_address: request.recipient_address.clone(),
                                },
                            )
                            .await?;
                            let Some(exchange_quote) = exchange_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(exchange_quote.expires_at);
                            cursor_amount = exchange_quote.amount_out.clone();
                            legs_per_transition[index] = exchange_quote_transition_legs(
                                &transition.id,
                                transition.kind,
                                transition.provider,
                                &exchange_quote,
                            )?;
                        }
                        MarketOrderTransitionKind::UniversalRouterSwap => {
                            let exchange = exchange.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "exchange provider is required for universal_router_swap"
                                    .to_string(),
                            })?;
                            let input_decimals = self
                                .exchange_asset_decimals(&transition.input.asset)
                                .await?;
                            let output_decimals = self
                                .exchange_asset_decimals(&transition.output.asset)
                                .await?;
                            let exchange_quote = quote_exchange(
                                exchange,
                                ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals,
                                    output_decimals,
                                    order_kind: MarketOrderKind::ExactIn {
                                        amount_in: provider_amount_in,
                                        min_amount_out: transition_min_amount_out(
                                            path,
                                            index,
                                            min_amount_out,
                                        ),
                                    },
                                    sender_address: Some(self.exchange_quote_sender_address(
                                        request,
                                        quote_id,
                                        source_depositor_address,
                                        transition,
                                    )?),
                                    recipient_address: self.exchange_quote_recipient_address(
                                        request, quote_id, path, index, transition,
                                    )?,
                                },
                            )
                            .await?;
                            let Some(exchange_quote) = exchange_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(exchange_quote.expires_at);
                            cursor_amount = exchange_quote.amount_out.clone();
                            legs_per_transition[index] = exchange_quote_transition_legs(
                                &transition.id,
                                transition.kind,
                                transition.provider,
                                &exchange_quote,
                            )?;
                        }
                        MarketOrderTransitionKind::UnitWithdrawal => {
                            unit.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "unit provider is required for unit_withdrawal".to_string(),
                            })?;
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &provider_amount_in,
                                amount_out: &provider_amount_in,
                                expires_at,
                                raw: json!({
                                    "recipient_address": request.recipient_address,
                                }),
                            }));
                        }
                    }
                }

                let legs = flatten_transition_legs(legs_per_transition);
                let provider_id = composed_provider_id(
                    path,
                    unit.map(|provider| provider.id()),
                    exchange.map(|provider| provider.id()),
                );
                Ok(Some(ComposedMarketOrderQuote {
                    provider_id,
                    amount_in: amount_in.clone(),
                    amount_out: cursor_amount,
                    min_amount_out: Some(min_amount_out.clone()),
                    max_amount_in: None,
                    provider_quote: transition_path_quote_blob(
                        path,
                        &legs,
                        &gas_reimbursement_plan,
                    ),
                    expires_at,
                }))
            }
            MarketOrderKind::ExactOut {
                amount_out,
                max_amount_in,
            } => {
                let mut required_output = amount_out.clone();
                for index in (0..path.transitions.len()).rev() {
                    let transition = &path.transitions[index];
                    match transition.kind {
                        MarketOrderTransitionKind::UnitWithdrawal => {
                            unit.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "unit provider is required for unit_withdrawal".to_string(),
                            })?;
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &required_output,
                                amount_out: &required_output,
                                expires_at,
                                raw: json!({
                                    "recipient_address": request.recipient_address,
                                }),
                            }));
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let exchange = exchange.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "exchange provider is required for hyperliquid_trade"
                                    .to_string(),
                            })?;
                            let max_in_cap = if index == 0 {
                                max_amount_in.clone()
                            } else {
                                practical_max_input_for_asset(&transition.input.asset)
                            };
                            let exchange_quote = quote_exchange(
                                exchange,
                                ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals: None,
                                    output_decimals: None,
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_in_cap,
                                    },
                                    sender_address: None,
                                    recipient_address: request.recipient_address.clone(),
                                },
                            )
                            .await?;
                            let Some(exchange_quote) = exchange_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(exchange_quote.expires_at);
                            let mut next_required = exchange_quote.amount_in.clone();
                            if index > 0
                                && path.transitions[index - 1].kind
                                    == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                            {
                                next_required = add_hyperliquid_spot_send_quote_gas_reserve(
                                    "hyperliquid_trade.amount_in",
                                    &next_required,
                                )?;
                            }
                            required_output = next_required;
                            legs_per_transition[index] = exchange_quote_transition_legs(
                                &transition.id,
                                transition.kind,
                                transition.provider,
                                &exchange_quote,
                            )?;
                        }
                        MarketOrderTransitionKind::UniversalRouterSwap => {
                            let exchange = exchange.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "exchange provider is required for universal_router_swap"
                                    .to_string(),
                            })?;
                            let max_in_cap = if index == 0 {
                                max_amount_in.clone()
                            } else {
                                practical_max_input_for_asset(&transition.input.asset)
                            };
                            let input_decimals = self
                                .exchange_asset_decimals(&transition.input.asset)
                                .await?;
                            let output_decimals = self
                                .exchange_asset_decimals(&transition.output.asset)
                                .await?;
                            let exchange_quote = quote_exchange(
                                exchange,
                                ExchangeQuoteRequest {
                                    input_asset: transition.input.asset.clone(),
                                    output_asset: transition.output.asset.clone(),
                                    input_decimals,
                                    output_decimals,
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_in_cap,
                                    },
                                    sender_address: Some(self.exchange_quote_sender_address(
                                        request,
                                        quote_id,
                                        source_depositor_address,
                                        transition,
                                    )?),
                                    recipient_address: self.exchange_quote_recipient_address(
                                        request, quote_id, path, index, transition,
                                    )?,
                                },
                            )
                            .await?;
                            let Some(exchange_quote) = exchange_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(exchange_quote.expires_at);
                            required_output = exchange_quote.amount_in.clone();
                            legs_per_transition[index] = exchange_quote_transition_legs(
                                &transition.id,
                                transition.kind,
                                transition.provider,
                                &exchange_quote,
                            )?;
                        }
                        MarketOrderTransitionKind::AcrossBridge
                        | MarketOrderTransitionKind::CctpBridge
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
                            let bridge_id = transition.provider.as_str();
                            if !provider_allowed_for_new_routes(provider_policy_snapshot, bridge_id)
                            {
                                return Ok(None);
                            }
                            let bridge =
                                self.action_providers.bridge(bridge_id).ok_or_else(|| {
                                    MarketOrderError::NoRoute {
                                        reason: format!(
                                            "bridge provider {bridge_id} is not configured"
                                        ),
                                    }
                                })?;
                            let max_input = if index == 0 {
                                max_amount_in.clone()
                            } else {
                                practical_max_input_for_asset(&transition.input.asset)
                            };
                            let bridge_quote = quote_bridge(
                                bridge.as_ref(),
                                BridgeQuoteRequest {
                                    source_asset: transition.input.asset.clone(),
                                    destination_asset: transition.output.asset.clone(),
                                    order_kind: MarketOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_input,
                                    },
                                    recipient_address: self.bridge_quote_recipient_address(
                                        request, quote_id, path, index, transition,
                                    )?,
                                    depositor_address: self.bridge_quote_depositor_address(
                                        request,
                                        quote_id,
                                        source_depositor_address,
                                        transition,
                                    )?,
                                    partial_fills_enabled: false,
                                },
                            )
                            .await?;
                            let Some(bridge_quote) = bridge_quote else {
                                return Ok(None);
                            };
                            expires_at = expires_at.min(bridge_quote.expires_at);
                            required_output = bridge_quote.amount_in.clone();
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &bridge_quote.amount_in,
                                amount_out: &bridge_quote.amount_out,
                                expires_at: bridge_quote.expires_at,
                                raw: bridge_quote.provider_quote,
                            }));
                        }
                        MarketOrderTransitionKind::UnitDeposit => {
                            unit.ok_or_else(|| MarketOrderError::NoRoute {
                                reason: "unit provider is required for unit_deposit".to_string(),
                            })?;
                            let (unit_amount_in, upstream_required, provider_quote) =
                                if let Some(fee_reserve) =
                                    self.bitcoin_unit_deposit_fee_reserve(transition).await?
                                {
                                    (
                                        required_output.clone(),
                                        add_bitcoin_fee_reserve(&required_output, fee_reserve)?,
                                        bitcoin_fee_reserve_quote(fee_reserve),
                                    )
                                } else {
                                    (required_output.clone(), required_output.clone(), json!({}))
                                };
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &unit_amount_in,
                                amount_out: &unit_amount_in,
                                expires_at,
                                raw: provider_quote,
                            }));
                            required_output = upstream_required;
                        }
                    }
                    required_output = add_transition_retention(
                        &gas_reimbursement_plan,
                        &transition.id,
                        &required_output,
                    )?;
                }

                let legs = flatten_transition_legs(legs_per_transition);
                let provider_id = composed_provider_id(
                    path,
                    unit.map(|provider| provider.id()),
                    exchange.map(|provider| provider.id()),
                );
                Ok(Some(ComposedMarketOrderQuote {
                    provider_id,
                    amount_in: required_output,
                    amount_out: amount_out.clone(),
                    min_amount_out: None,
                    max_amount_in: Some(max_amount_in.clone()),
                    provider_quote: transition_path_quote_blob(
                        path,
                        &legs,
                        &gas_reimbursement_plan,
                    ),
                    expires_at,
                }))
            }
        }
    }

    fn bridge_quote_depositor_address(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        source_depositor_address: &str,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        if transition.input.asset.chain == request.source_asset.chain {
            Ok(source_depositor_address.to_string())
        } else {
            self.quote_address_for_chain(quote_id, &transition.input.asset.chain)
        }
    }

    fn bridge_quote_recipient_address(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        path: &TransitionPath,
        index: usize,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        if matches!(
            transition.kind,
            MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge
        ) && index + 1 == path.transitions.len()
        {
            Ok(request.recipient_address.clone())
        } else {
            self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
        }
    }

    fn exchange_quote_sender_address(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        source_depositor_address: &str,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        if transition.input.asset.chain == request.source_asset.chain
            && transition.input.asset.asset == request.source_asset.asset
        {
            Ok(source_depositor_address.to_string())
        } else {
            self.quote_address_for_chain(quote_id, &transition.input.asset.chain)
        }
    }

    fn exchange_quote_recipient_address(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote_id: Uuid,
        path: &TransitionPath,
        index: usize,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        if index + 1 == path.transitions.len() {
            Ok(request.recipient_address.clone())
        } else {
            self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
        }
    }

    async fn exchange_asset_decimals(&self, asset: &DepositAsset) -> MarketOrderResult<Option<u8>> {
        if let Some(chain_asset) = self.asset_registry.chain_asset(asset) {
            return Ok(Some(chain_asset.decimals));
        }
        match &asset.asset {
            AssetId::Native => Ok(Some(18)),
            AssetId::Reference(token_address) => {
                let backend_chain = backend_chain_for_id(&asset.chain).ok_or(
                    MarketOrderError::ChainNotSupported {
                        chain: asset.chain.clone(),
                    },
                )?;
                if !matches!(
                    backend_chain,
                    ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base
                ) {
                    return Ok(None);
                }
                let evm_chain = self.chain_registry.get_evm(&backend_chain).ok_or_else(|| {
                    MarketOrderError::NoRoute {
                        reason: format!(
                            "no EVM chain implementation is configured for {}",
                            asset.chain
                        ),
                    }
                })?;
                evm_chain
                    .erc20_decimals(token_address)
                    .await
                    .map(Some)
                    .map_err(|err| MarketOrderError::NoRoute {
                        reason: format!(
                            "failed to read token decimals for {} {}: {err}",
                            asset.chain, asset.asset
                        ),
                    })
            }
        }
    }

    fn quote_address_for_chain(
        &self,
        quote_id: Uuid,
        chain_id: &ChainId,
    ) -> MarketOrderResult<String> {
        derive_deposit_address_for_quote(
            self.chain_registry.as_ref(),
            &self.settings.master_key_bytes(),
            quote_id,
            chain_id,
        )
        .map(|(address, _)| address)
        .map_err(MarketOrderError::deposit_address)
    }

    fn validate_and_normalize_quote_request(
        &self,
        request: MarketOrderQuoteRequest,
    ) -> MarketOrderResult<NormalizedMarketOrderQuoteRequest> {
        validate_market_order_quote_kind(&request.order_kind)?;
        let source_asset = self.validate_and_normalize_asset(&request.from_asset)?;
        let destination_asset = self.validate_and_normalize_asset(&request.to_asset)?;
        let recipient_address = self.validate_and_normalize_recipient_address(
            &destination_asset.chain,
            &request.recipient_address,
        )?;
        Ok(NormalizedMarketOrderQuoteRequest {
            source_asset,
            destination_asset,
            recipient_address,
            order_kind: request.order_kind,
        })
    }

    fn validate_and_normalize_asset(
        &self,
        asset: &DepositAsset,
    ) -> MarketOrderResult<DepositAsset> {
        let backend_chain =
            backend_chain_for_id(&asset.chain).ok_or(MarketOrderError::ChainNotSupported {
                chain: asset.chain.clone(),
            })?;
        match backend_chain {
            ChainType::Bitcoin => match &asset.asset {
                AssetId::Native => Ok(asset.clone()),
                AssetId::Reference(asset_id) => Err(MarketOrderError::InvalidAssetId {
                    asset: asset_id.clone(),
                    chain: asset.chain.clone(),
                    reason: "bitcoin only supports the native asset".to_string(),
                }),
            },
            ChainType::Ethereum | ChainType::Arbitrum | ChainType::Base => match &asset.asset {
                AssetId::Native => Ok(asset.clone()),
                AssetId::Reference(asset_id) => {
                    asset.normalized_asset_identity().map_err(|reason| {
                        MarketOrderError::InvalidAssetId {
                            asset: asset_id.clone(),
                            chain: asset.chain.clone(),
                            reason,
                        }
                    })
                }
            },
            ChainType::Hyperliquid => Err(MarketOrderError::ChainNotSupported {
                chain: asset.chain.clone(),
            }),
        }
    }

    fn validate_and_normalize_recipient_address(
        &self,
        chain_id: &ChainId,
        address: &str,
    ) -> MarketOrderResult<String> {
        let backend_chain =
            backend_chain_for_id(chain_id).ok_or(MarketOrderError::ChainNotSupported {
                chain: chain_id.clone(),
            })?;
        let chain =
            self.chain_registry
                .get(&backend_chain)
                .ok_or(MarketOrderError::ChainNotSupported {
                    chain: chain_id.clone(),
                })?;
        let normalized = if chain_id.evm_chain_id().is_some() {
            address.to_lowercase()
        } else {
            address.to_string()
        };
        if chain.validate_address(&normalized) {
            Ok(normalized)
        } else {
            Err(MarketOrderError::InvalidRecipientAddress {
                address: address.to_string(),
                chain: chain_id.clone(),
            })
        }
    }

    fn validate_and_normalize_refund_address(
        &self,
        chain_id: &ChainId,
        address: &str,
    ) -> MarketOrderResult<String> {
        let backend_chain =
            backend_chain_for_id(chain_id).ok_or(MarketOrderError::ChainNotSupported {
                chain: chain_id.clone(),
            })?;
        let chain =
            self.chain_registry
                .get(&backend_chain)
                .ok_or(MarketOrderError::ChainNotSupported {
                    chain: chain_id.clone(),
                })?;
        let normalized = if chain_id.evm_chain_id().is_some() {
            address.to_lowercase()
        } else {
            address.to_string()
        };
        if chain.validate_address(&normalized) {
            Ok(normalized)
        } else {
            Err(MarketOrderError::InvalidRefundAddress {
                address: address.to_string(),
                chain: chain_id.clone(),
            })
        }
    }

    fn validate_provider_quote(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
        quote: &ComposedMarketOrderQuote,
    ) -> MarketOrderResult<()> {
        if quote.provider_id.trim().is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: "provider returned an empty provider id".to_string(),
            });
        }
        validate_positive_amount("amount_in", &quote.amount_in)?;
        validate_positive_amount("amount_out", &quote.amount_out)?;
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::NoRoute {
                reason: "provider returned an expired quote".to_string(),
            });
        }

        match &request.order_kind {
            MarketOrderQuoteKind::ExactIn { amount_in, .. } => {
                if quote.amount_in != *amount_in {
                    return Err(MarketOrderError::NoRoute {
                        reason: "provider exact-in quote changed amount_in".to_string(),
                    });
                }
                let quoted_amount_out = parse_amount("amount_out", &quote.amount_out)?;
                let requested_min_out = parse_amount(
                    "min_amount_out",
                    quote
                        .min_amount_out
                        .as_deref()
                        .ok_or(MarketOrderError::InvalidAmount {
                            field: "min_amount_out",
                            reason: "exact-in quote is missing min_amount_out".to_string(),
                        })?,
                )?;
                if quoted_amount_out < requested_min_out {
                    return Err(MarketOrderError::NoRoute {
                        reason: "provider exact-in quote is below min_amount_out".to_string(),
                    });
                }
            }
            MarketOrderQuoteKind::ExactOut { amount_out, .. } => {
                if quote.amount_out != *amount_out {
                    return Err(MarketOrderError::NoRoute {
                        reason: "provider exact-out quote changed amount_out".to_string(),
                    });
                }
                let quoted_amount_in = parse_amount("amount_in", &quote.amount_in)?;
                let requested_max_in = parse_amount(
                    "max_amount_in",
                    quote
                        .max_amount_in
                        .as_deref()
                        .ok_or(MarketOrderError::InvalidAmount {
                            field: "max_amount_in",
                            reason: "exact-out quote is missing max_amount_in".to_string(),
                        })?,
                )?;
                if quoted_amount_in > requested_max_in {
                    return Err(MarketOrderError::NoRoute {
                        reason: "provider exact-out quote is above max_amount_in".to_string(),
                    });
                }
            }
        }

        Ok(())
    }

    async fn bitcoin_unit_deposit_fee_reserve(
        &self,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<Option<U256>> {
        if transition.kind != MarketOrderTransitionKind::UnitDeposit
            || transition.input.asset.chain.as_str() != "bitcoin"
            || !matches!(transition.input.asset.asset, AssetId::Native)
        {
            return Ok(None);
        }

        let bitcoin_chain = self
            .chain_registry
            .get_bitcoin(&ChainType::Bitcoin)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: "bitcoin chain is required to estimate Unit deposit miner fee".to_string(),
            })?;
        let estimated_fee = match bitcoin_chain
            .estimate_p2wpkh_transfer_fee_sats(
                BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS,
                BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS,
            )
            .await
        {
            Ok(estimated_fee) => estimated_fee,
            Err(err) => {
                warn!(
                    error = %err,
                    fallback_sats = BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_FALLBACK_SATS,
                    "falling back to static bitcoin Unit deposit miner fee reserve"
                );
                BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_FALLBACK_SATS
            }
        };
        Ok(Some(U256::from(apply_bps_u64(
            estimated_fee,
            BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS,
        ))))
    }
}

fn provider_allowed_for_new_routes(
    snapshot: Option<&crate::services::ProviderPolicySnapshot>,
    provider: &str,
) -> bool {
    let Some(snapshot) = snapshot else {
        return true;
    };
    let policy = snapshot.policy(provider);
    if policy.allows_new_routes() {
        return true;
    }
    let reason = if policy.reason.is_empty() {
        "provider_policy".to_string()
    } else {
        policy.reason.clone()
    };
    telemetry::record_provider_quote_blocked(provider, &reason);
    false
}

fn is_executable_transition_path(registry: &AssetRegistry, path: &TransitionPath) -> bool {
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
        Some(MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge)
    ) && path_contains_runtime_asset(registry, path)
}

fn path_contains_runtime_asset(registry: &AssetRegistry, path: &TransitionPath) -> bool {
    path.transitions.iter().any(|transition| {
        registry.chain_asset(&transition.input.asset).is_none()
            || registry.chain_asset(&transition.output.asset).is_none()
    })
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

fn transition_leg(spec: QuoteLegSpec<'_>) -> QuoteLeg {
    QuoteLeg::new(spec)
}

fn exchange_quote_transition_legs(
    transition_decl_id: &str,
    transition_kind: MarketOrderTransitionKind,
    provider: ProviderId,
    quote: &ExchangeQuote,
) -> MarketOrderResult<Vec<QuoteLeg>> {
    let kind = quote
        .provider_quote
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("");
    match kind {
        "spot_no_op" => Ok(vec![]),
        "universal_router_swap" => {
            let input_asset = quote
                .provider_quote
                .get("input_asset")
                .ok_or_else(|| MarketOrderError::NoRoute {
                    reason: "universal router quote missing input_asset".to_string(),
                })
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "input_asset")
                        .map_err(|reason| MarketOrderError::NoRoute { reason })
                })?;
            let output_asset = quote
                .provider_quote
                .get("output_asset")
                .ok_or_else(|| MarketOrderError::NoRoute {
                    reason: "universal router quote missing output_asset".to_string(),
                })
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "output_asset")
                        .map_err(|reason| MarketOrderError::NoRoute { reason })
                })?;
            Ok(vec![QuoteLeg {
                transition_decl_id: transition_decl_id.to_string(),
                transition_parent_decl_id: transition_decl_id.to_string(),
                transition_kind,
                provider,
                input_asset,
                output_asset,
                amount_in: quote.amount_in.clone(),
                amount_out: quote.amount_out.clone(),
                expires_at: quote.expires_at,
                raw: quote.provider_quote.clone(),
            }])
        }
        "spot_cross_token" => {
            let Some(legs) = quote.provider_quote.get("legs").and_then(Value::as_array) else {
                return Err(MarketOrderError::NoRoute {
                    reason: "exchange quote missing spot_cross_token legs".to_string(),
                });
            };
            let mapped = legs
                .iter()
                .enumerate()
                .map(|(index, leg)| {
                    let input_asset = leg
                        .get("input_asset")
                        .ok_or_else(|| MarketOrderError::NoRoute {
                            reason: "hyperliquid cross-token leg missing input_asset".to_string(),
                        })
                        .and_then(|value| {
                            QuoteLegAsset::from_value(value, "input_asset")
                                .map_err(|reason| MarketOrderError::NoRoute { reason })
                        })?;
                    let output_asset = leg
                        .get("output_asset")
                        .ok_or_else(|| MarketOrderError::NoRoute {
                            reason: "hyperliquid cross-token leg missing output_asset".to_string(),
                        })
                        .and_then(|value| {
                            QuoteLegAsset::from_value(value, "output_asset")
                                .map_err(|reason| MarketOrderError::NoRoute { reason })
                        })?;
                    Ok(QuoteLeg {
                        transition_decl_id: format!("{transition_decl_id}:leg:{index}"),
                        transition_parent_decl_id: transition_decl_id.to_string(),
                        transition_kind,
                        provider,
                        input_asset,
                        output_asset,
                        amount_in: leg
                            .get("amount_in")
                            .and_then(Value::as_str)
                            .unwrap_or(&quote.amount_in)
                            .to_string(),
                        amount_out: leg
                            .get("amount_out")
                            .and_then(Value::as_str)
                            .unwrap_or(&quote.amount_out)
                            .to_string(),
                        expires_at: quote.expires_at,
                        raw: leg.clone(),
                    })
                })
                .collect::<MarketOrderResult<Vec<_>>>()?;
            Ok(mapped)
        }
        other => Err(MarketOrderError::NoRoute {
            reason: format!("unsupported exchange quote kind in transition path: {other:?}"),
        }),
    }
}

fn flatten_transition_legs(legs_per_transition: Vec<Vec<QuoteLeg>>) -> Vec<QuoteLeg> {
    let mut flattened = Vec::new();
    for mut transition_legs in legs_per_transition {
        flattened.append(&mut transition_legs);
    }
    flattened
}

fn composed_provider_id(
    path: &TransitionPath,
    unit_provider: Option<&str>,
    exchange_provider: Option<&str>,
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
    if let Some(exchange_provider) = exchange_provider {
        providers.push(format!("exchange:{exchange_provider}"));
    }
    format!("path:{}|{}", path.id, providers.join("|"))
}

fn transition_path_quote_blob(
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
    })
}

fn bitcoin_fee_reserve_quote(fee_reserve: U256) -> Value {
    json!({
        "source_fee_reserve": {
            "kind": "bitcoin_miner_fee",
            "chain_id": "bitcoin",
            "asset": AssetId::Native.as_str(),
            "amount": fee_reserve.to_string(),
            "reserve_bps": BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS,
        }
    })
}

fn subtract_bitcoin_fee_reserve(
    field: &'static str,
    gross_amount: &str,
    fee_reserve: U256,
    transition_id: &str,
) -> MarketOrderResult<String> {
    let gross = parse_amount(field, gross_amount)?;
    if fee_reserve == U256::ZERO {
        return Ok(gross_amount.to_string());
    }
    if gross <= fee_reserve {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount must exceed bitcoin miner fee reserve {} for transition {}",
                fee_reserve, transition_id
            ),
        });
    }
    Ok(gross.saturating_sub(fee_reserve).to_string())
}

fn add_bitcoin_fee_reserve(amount: &str, fee_reserve: U256) -> MarketOrderResult<String> {
    let amount = parse_amount("amount_in", amount)?;
    Ok(amount.saturating_add(fee_reserve).to_string())
}

fn apply_bps_u64(value: u64, bps: u64) -> u64 {
    value.saturating_mul(bps).div_ceil(10_000)
}

fn apply_transition_retention(
    plan: &GasReimbursementPlan,
    transition_id: &str,
    field: &'static str,
    gross_amount: &str,
) -> MarketOrderResult<String> {
    let gross = parse_amount(field, gross_amount)?;
    let retention = transition_retention_amount(plan, transition_id);
    if retention == U256::ZERO {
        return Ok(gross_amount.to_string());
    }
    if gross <= retention {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount must exceed paymaster gas retention {} for transition {}",
                retention, transition_id
            ),
        });
    }
    Ok(gross.saturating_sub(retention).to_string())
}

fn add_transition_retention(
    plan: &GasReimbursementPlan,
    transition_id: &str,
    amount: &str,
) -> MarketOrderResult<String> {
    let amount = parse_amount("amount_in", amount)?;
    Ok(amount
        .saturating_add(transition_retention_amount(plan, transition_id))
        .to_string())
}

fn practical_max_input_for_asset(_asset: &DepositAsset) -> String {
    U256::MAX.to_string()
}

fn transition_min_amount_out(
    path: &TransitionPath,
    index: usize,
    final_min_amount_out: &str,
) -> String {
    if index + 1 == path.transitions.len() {
        final_min_amount_out.to_string()
    } else {
        "1".to_string()
    }
}

async fn quote_bridge(
    bridge: &dyn BridgeProvider,
    request: BridgeQuoteRequest,
) -> MarketOrderResult<Option<BridgeQuote>> {
    tokio::time::timeout(MARKET_ORDER_PROVIDER_TIMEOUT, bridge.quote_bridge(request))
        .await
        .map_err(|_| MarketOrderError::NoRoute {
            reason: format!("bridge provider {} timed out", bridge.id()),
        })?
        .map_err(|err| MarketOrderError::NoRoute {
            reason: format!("bridge provider {} failed: {err}", bridge.id()),
        })
}

async fn quote_exchange(
    exchange: &dyn ExchangeProvider,
    request: ExchangeQuoteRequest,
) -> MarketOrderResult<Option<ExchangeQuote>> {
    tokio::time::timeout(MARKET_ORDER_PROVIDER_TIMEOUT, exchange.quote_trade(request))
        .await
        .map_err(|_| MarketOrderError::NoRoute {
            reason: format!("exchange provider {} timed out", exchange.id()),
        })?
        .map_err(|err| MarketOrderError::NoRoute {
            reason: format!("exchange provider {} failed: {err}", exchange.id()),
        })
}

fn reserve_hyperliquid_spot_send_quote_gas(
    field: &'static str,
    value: &str,
) -> MarketOrderResult<String> {
    let amount = parse_amount(field, value)?;
    let reserve = U256::from(HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW);
    if amount <= reserve {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount must exceed Hyperliquid spot token transfer gas reserve {reserve}"
            ),
        });
    }
    Ok(amount.saturating_sub(reserve).to_string())
}

fn add_hyperliquid_spot_send_quote_gas_reserve(
    field: &'static str,
    value: &str,
) -> MarketOrderResult<String> {
    let amount = parse_amount(field, value)?;
    Ok(amount
        .saturating_add(U256::from(HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW))
        .to_string())
}

fn choose_better_quote(
    request: &NormalizedMarketOrderQuoteRequest,
    candidate: ComposedMarketOrderQuote,
    current: Option<ComposedMarketOrderQuote>,
) -> Option<ComposedMarketOrderQuote> {
    if is_better_quote(request.order_kind.kind_type(), &candidate, &current) {
        Some(candidate)
    } else {
        current
    }
}

#[derive(Debug, Clone)]
struct NormalizedMarketOrderQuoteRequest {
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    recipient_address: String,
    order_kind: MarketOrderQuoteKind,
}

impl MarketOrderQuoteKind {
    #[must_use]
    fn kind_type(&self) -> MarketOrderKindType {
        match self {
            Self::ExactIn { .. } => MarketOrderKindType::ExactIn,
            Self::ExactOut { .. } => MarketOrderKindType::ExactOut,
        }
    }

    #[must_use]
    fn slippage_bps(&self) -> u64 {
        match self {
            Self::ExactIn { slippage_bps, .. } | Self::ExactOut { slippage_bps, .. } => {
                *slippage_bps
            }
        }
    }

    fn probe_order_kind(&self) -> MarketOrderKind {
        match self {
            Self::ExactIn { amount_in, .. } => MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
            Self::ExactOut { amount_out, .. } => MarketOrderKind::ExactOut {
                amount_out: amount_out.clone(),
                max_amount_in: U256::MAX.to_string(),
            },
        }
    }

    fn bounded_order_kind(
        &self,
        probe_quote: &ComposedMarketOrderQuote,
    ) -> MarketOrderResult<MarketOrderKind> {
        match self {
            Self::ExactIn {
                amount_in,
                slippage_bps,
            } => Ok(MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: apply_slippage_floor(
                    "amount_out",
                    &probe_quote.amount_out,
                    *slippage_bps,
                )?,
            }),
            Self::ExactOut {
                amount_out,
                slippage_bps,
            } => Ok(MarketOrderKind::ExactOut {
                amount_out: amount_out.clone(),
                max_amount_in: apply_slippage_ceiling(
                    "amount_in",
                    &probe_quote.amount_in,
                    *slippage_bps,
                )?,
            }),
        }
    }
}

fn market_order_kind_from_quote(quote: &MarketOrderQuote) -> MarketOrderResult<MarketOrderKind> {
    match quote.order_kind {
        MarketOrderKindType::ExactIn => Ok(MarketOrderKind::ExactIn {
            amount_in: quote.amount_in.clone(),
            min_amount_out: quote.min_amount_out.clone().ok_or(
                MarketOrderError::InvalidAmount {
                    field: "min_amount_out",
                    reason: "quote is missing min_amount_out".to_string(),
                },
            )?,
        }),
        MarketOrderKindType::ExactOut => Ok(MarketOrderKind::ExactOut {
            amount_out: quote.amount_out.clone(),
            max_amount_in: quote
                .max_amount_in
                .clone()
                .ok_or(MarketOrderError::InvalidAmount {
                    field: "max_amount_in",
                    reason: "quote is missing max_amount_in".to_string(),
                })?,
        }),
    }
}

fn validate_market_order_quote_kind(order_kind: &MarketOrderQuoteKind) -> MarketOrderResult<()> {
    match order_kind {
        MarketOrderQuoteKind::ExactIn {
            amount_in,
            slippage_bps,
        } => {
            validate_positive_amount("amount_in", amount_in)?;
            validate_slippage_bps(*slippage_bps)?;
        }
        MarketOrderQuoteKind::ExactOut {
            amount_out,
            slippage_bps,
        } => {
            validate_positive_amount("amount_out", amount_out)?;
            validate_slippage_bps(*slippage_bps)?;
        }
    }

    Ok(())
}

fn validate_slippage_bps(slippage_bps: u64) -> MarketOrderResult<()> {
    if slippage_bps > 10_000 {
        return Err(MarketOrderError::InvalidAmount {
            field: "slippage_bps",
            reason: "slippage_bps must be between 0 and 10000".to_string(),
        });
    }
    Ok(())
}

fn apply_slippage_floor(
    field: &'static str,
    expected_amount: &str,
    slippage_bps: u64,
) -> MarketOrderResult<String> {
    validate_slippage_bps(slippage_bps)?;
    let expected = parse_amount(field, expected_amount)?;
    let multiplier = U256::from(10_000_u64.saturating_sub(slippage_bps));
    let bounded = expected.saturating_mul(multiplier) / U256::from(10_000_u64);
    Ok(if bounded.is_zero() && !expected.is_zero() {
        U256::from(1_u64)
    } else {
        bounded
    }
    .to_string())
}

fn apply_slippage_ceiling(
    field: &'static str,
    expected_amount: &str,
    slippage_bps: u64,
) -> MarketOrderResult<String> {
    validate_slippage_bps(slippage_bps)?;
    let expected = parse_amount(field, expected_amount)?;
    let numerator = expected.saturating_mul(U256::from(10_000_u64.saturating_add(slippage_bps)));
    Ok(div_ceil_u256(numerator, U256::from(10_000_u64)).to_string())
}

fn div_ceil_u256(numerator: U256, denominator: U256) -> U256 {
    if numerator.is_zero() {
        return U256::ZERO;
    }
    numerator.saturating_add(denominator.saturating_sub(U256::from(1_u64))) / denominator
}

fn validate_positive_amount(field: &'static str, value: &str) -> MarketOrderResult<()> {
    let amount = parse_amount(field, value)?;
    if amount == U256::ZERO {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: "amount must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn parse_amount(field: &'static str, value: &str) -> MarketOrderResult<U256> {
    if value.is_empty() {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: "amount cannot be empty".to_string(),
        });
    }
    if !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: "amount must be a base-unit decimal integer".to_string(),
        });
    }
    U256::from_str_radix(value, 10).map_err(|err| MarketOrderError::InvalidAmount {
        field,
        reason: err.to_string(),
    })
}

fn normalize_idempotency_key(value: Option<String>) -> MarketOrderResult<Option<String>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim().to_string();
    if value.is_empty() {
        return Err(MarketOrderError::InvalidIdempotencyKey {
            reason: "key cannot be empty".to_string(),
        });
    }
    if value.len() > 128 {
        return Err(MarketOrderError::InvalidIdempotencyKey {
            reason: "key cannot exceed 128 bytes".to_string(),
        });
    }
    Ok(Some(value))
}

fn is_better_quote(
    order_kind: MarketOrderKindType,
    candidate: &ComposedMarketOrderQuote,
    current: &Option<ComposedMarketOrderQuote>,
) -> bool {
    let Some(current) = current else {
        return true;
    };

    let primary_cmp = match order_kind {
        MarketOrderKindType::ExactIn => parse_amount("amount_out", &candidate.amount_out)
            .ok()
            .cmp(&parse_amount("amount_out", &current.amount_out).ok()),
        MarketOrderKindType::ExactOut => parse_amount("amount_in", &current.amount_in)
            .ok()
            .cmp(&parse_amount("amount_in", &candidate.amount_in).ok()),
    };
    if primary_cmp.is_gt() {
        return true;
    }
    if primary_cmp.is_lt() {
        return false;
    }

    let candidate_hops = quote_hop_count(candidate);
    let current_hops = quote_hop_count(current);
    if candidate_hops < current_hops {
        return true;
    }
    if candidate_hops > current_hops {
        return false;
    }

    false
}

fn quote_hop_count(quote: &ComposedMarketOrderQuote) -> usize {
    quote
        .provider_quote
        .get("transition_decl_ids")
        .and_then(Value::as_array)
        .map_or(usize::MAX, Vec::len)
}
