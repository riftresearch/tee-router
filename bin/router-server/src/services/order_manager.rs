use crate::{
    api::{
        CreateOrderRequest, LimitOrderQuoteRequest, MarketOrderQuoteKind, MarketOrderQuoteRequest,
    },
    error::RouterServerError,
    services::{
        deposit_address::{derive_deposit_address_for_quote, DepositAddressError},
        provider_health::ProviderHealthService,
        provider_policy::ProviderPolicyService,
        route_minimums::{RouteMinimumError, RouteMinimumService},
    },
    telemetry,
};
use alloy::primitives::U256;
use chains::ChainRegistry;
use chrono::{Duration as ChronoDuration, Utc};
use router_core::{
    config::Settings,
    db::Database,
    error::RouterCoreError,
    models::{
        LimitOrderAction, LimitOrderQuote, LimitOrderResidualPolicy, MarketOrderAction,
        MarketOrderKind, MarketOrderKindType, MarketOrderQuote, OrderExecutionStepType,
        RouterOrder, RouterOrderAction, RouterOrderQuote, RouterOrderQuoteEnvelope,
        RouterOrderStatus, RouterOrderType,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId, DepositAsset},
    services::{
        action_providers::{
            ActionProviderRegistry, BridgeProvider, BridgeQuote, BridgeQuoteRequest,
            ExchangeProvider, ExchangeQuote, ExchangeQuoteRequest, UnitProvider,
        },
        asset_registry::{
            AssetRegistry, CanonicalAsset, MarketOrderNode, MarketOrderTransitionKind, ProviderId,
            TransitionDecl, TransitionPath,
        },
        gas_reimbursement::{
            optimized_paymaster_reimbursement_plan,
            optimized_paymaster_reimbursement_plan_with_pricing, try_transition_retention_amount,
            GasReimbursementError, GasReimbursementPlan,
        },
        quote_legs::{
            execution_step_type_for_transition_kind, QuoteLeg, QuoteLegAsset, QuoteLegSpec,
        },
        route_costs::{rank_transition_paths_structurally, RouteCostService},
        usd_valuation::{empty_usd_valuation, limit_quote_usd_valuation, quote_usd_valuation},
    },
};
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
// V1 limit orders have no user-facing expiry. The legacy order row requires a
// timestamp, so use a long internal sentinel instead of market-order timeout
// semantics.
const LIMIT_ORDER_ACTION_TIMEOUT: ChronoDuration = ChronoDuration::days(3650);
const MARKET_ORDER_PROVIDER_TIMEOUT: Duration = Duration::from_secs(10);
// HyperCore charges one quote token when the destination HyperCore account is
// not yet activated. Unit withdrawal protocol addresses are often fresh, so
// quote paths that end with Hyperliquid -> Unit need to leave 1 USDC available.
const HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW: u64 = 1_000_000;
const HYPERLIQUID_CORE_ACTIVATION_FEE_RAW: u64 = HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS: usize = 1;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS: usize = 2;
const BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS: u64 = 12_500;
const PROBE_MAX_AMOUNT_IN: &str = "340282366920938463463374607431768211455";
const MAX_U256_DECIMAL_DIGITS: usize = 78;

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

    fn database(source: impl Into<RouterServerError>) -> Self {
        Self::Database {
            source: Box::new(source.into()),
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

#[derive(Debug, Clone)]
struct ComposedLimitOrderQuote {
    pub provider_id: String,
    pub input_amount: String,
    pub output_amount: String,
    pub provider_quote: Value,
    pub expires_at: chrono::DateTime<Utc>,
}

struct ComposeTransitionPathQuoteRequest<'a> {
    request: &'a NormalizedMarketOrderQuoteRequest,
    order_kind: &'a MarketOrderKind,
    quote_id: Uuid,
    source_depositor_address: &'a str,
    path: &'a TransitionPath,
    gas_reimbursement_plan: Option<&'a GasReimbursementPlan>,
    provider_policy_snapshot: Option<&'a crate::services::ProviderPolicySnapshot>,
    provider_health_snapshot: Option<&'a crate::services::ProviderHealthSnapshot>,
    unit: Option<&'a dyn UnitProvider>,
}

struct ComposeLimitTransitionPathQuoteRequest<'a> {
    request: &'a NormalizedLimitOrderQuoteRequest,
    quote_id: Uuid,
    source_depositor_address: &'a str,
    path: &'a TransitionPath,
    limit_index: usize,
    provider_policy_snapshot: Option<&'a crate::services::ProviderPolicySnapshot>,
    provider_health_snapshot: Option<&'a crate::services::ProviderHealthSnapshot>,
    unit: Option<&'a dyn UnitProvider>,
    exchange: &'a dyn ExchangeProvider,
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
    provider_health: Option<Arc<ProviderHealthService>>,
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
            provider_health: None,
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

    #[must_use]
    pub fn with_provider_health(
        mut self,
        provider_health: Option<Arc<ProviderHealthService>>,
    ) -> Self {
        self.provider_health = provider_health;
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
        let mut quote = MarketOrderQuote {
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
            usd_valuation: empty_usd_valuation(),
            expires_at: provider_quote.expires_at,
            created_at: now,
        };
        let usd_pricing = self.usd_pricing_snapshot().await;
        quote.usd_valuation =
            quote_usd_valuation(&self.asset_registry, usd_pricing.as_ref(), &quote);

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

    pub async fn quote_limit_order(
        &self,
        request: LimitOrderQuoteRequest,
    ) -> MarketOrderResult<RouterOrderQuoteEnvelope> {
        let normalized_request = self.validate_and_normalize_limit_order_request(request)?;
        let quote_id = Uuid::now_v7();
        let (depositor_address, _) = derive_deposit_address_for_quote(
            self.chain_registry.as_ref(),
            &self.settings.master_key_bytes(),
            quote_id,
            &normalized_request.source_asset.chain,
        )
        .map_err(MarketOrderError::deposit_address)?;
        let provider_quote = self
            .best_limit_order_quote(&normalized_request, quote_id, &depositor_address)
            .await?;
        let now = Utc::now();
        let mut quote = LimitOrderQuote {
            id: quote_id,
            order_id: None,
            source_asset: normalized_request.source_asset,
            destination_asset: normalized_request.destination_asset,
            recipient_address: normalized_request.recipient_address,
            provider_id: provider_quote.provider_id,
            input_amount: provider_quote.input_amount,
            output_amount: provider_quote.output_amount,
            residual_policy: LimitOrderResidualPolicy::Refund,
            provider_quote: provider_quote.provider_quote,
            usd_valuation: empty_usd_valuation(),
            expires_at: provider_quote.expires_at,
            created_at: now,
        };
        let usd_pricing = self.usd_pricing_snapshot().await;
        quote.usd_valuation =
            limit_quote_usd_valuation(&self.asset_registry, usd_pricing.as_ref(), &quote);

        self.db
            .orders()
            .create_limit_order_quote(&quote)
            .await
            .map_err(MarketOrderError::database)?;

        Ok(RouterOrderQuoteEnvelope {
            quote: quote.into(),
        })
    }

    async fn usd_pricing_snapshot(
        &self,
    ) -> Option<router_core::services::pricing::PricingSnapshot> {
        self.route_costs
            .as_ref()?
            .current_or_refresh_live_pricing_snapshot()
            .await
    }

    pub async fn get_quote(&self, quote_id: Uuid) -> MarketOrderResult<RouterOrderQuote> {
        self.db
            .orders()
            .get_router_order_quote_by_id(quote_id)
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

    pub async fn get_quote_for_order(&self, order_id: Uuid) -> MarketOrderResult<RouterOrderQuote> {
        self.db
            .orders()
            .get_router_order_quote(order_id)
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
            | RouterOrderStatus::ManualInterventionRequired
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
    ) -> MarketOrderResult<(RouterOrder, RouterOrderQuote)> {
        let quote = self.get_quote(request.quote_id).await?;
        match quote {
            RouterOrderQuote::MarketOrder(quote) => {
                if quote.order_id.is_some() {
                    let (order, quote) =
                        self.resume_market_order_from_quote(request, quote).await?;
                    return Ok((order, quote.into()));
                }
                self.create_market_order_from_orderable_quote(request, quote)
                    .await
                    .map(|(order, quote)| (order, quote.into()))
            }
            RouterOrderQuote::LimitOrder(quote) => {
                if quote.order_id.is_some() {
                    let (order, quote) = self.resume_limit_order_from_quote(request, quote).await?;
                    return Ok((order, quote.into()));
                }
                self.create_limit_order_from_orderable_quote(request, quote)
                    .await
                    .map(|(order, quote)| (order, quote.into()))
            }
        }
    }

    async fn create_market_order_from_orderable_quote(
        &self,
        request: CreateOrderRequest,
        quote: MarketOrderQuote,
    ) -> MarketOrderResult<(RouterOrder, MarketOrderQuote)> {
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::QuoteExpired);
        }
        let now = Utc::now();
        let order_id = Uuid::now_v7();
        let workflow_trace = observability::current_workflow_trace_context()
            .unwrap_or_else(|| fallback_workflow_trace_context(order_id));
        let refund_address = self.validate_and_normalize_refund_address(
            &quote.source_asset.chain,
            &request.refund_address,
        )?;
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Quoted,
            funding_vault_id: None,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address: quote.recipient_address.clone(),
            refund_address,
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: market_order_kind_from_quote(&quote),
                slippage_bps: quote.slippage_bps,
            }),
            action_timeout_at: now + MARKET_ORDER_ACTION_TIMEOUT,
            idempotency_key: normalize_idempotency_key(request.idempotency_key.clone())?,
            workflow_trace_id: workflow_trace.trace_id,
            workflow_parent_span_id: workflow_trace.parent_span_id,
            created_at: now,
            updated_at: now,
        };

        let result = self
            .db
            .orders()
            .create_market_order_from_quote(&order, quote.id)
            .await;
        let quote = match result {
            Ok(quote) => quote,
            Err(RouterCoreError::Conflict { .. }) => {
                return self
                    .resume_market_order_after_create_conflict(request, quote.id)
                    .await;
            }
            Err(err) => return Err(MarketOrderError::database(err)),
        };
        telemetry::record_order_workflow_event(&order, "order.created");

        Ok((order, quote))
    }

    async fn create_limit_order_from_orderable_quote(
        &self,
        request: CreateOrderRequest,
        quote: LimitOrderQuote,
    ) -> MarketOrderResult<(RouterOrder, LimitOrderQuote)> {
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::QuoteExpired);
        }
        let now = Utc::now();
        let order_id = Uuid::now_v7();
        let workflow_trace = observability::current_workflow_trace_context()
            .unwrap_or_else(|| fallback_workflow_trace_context(order_id));
        let refund_address = self.validate_and_normalize_refund_address(
            &quote.source_asset.chain,
            &request.refund_address,
        )?;
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::LimitOrder,
            status: RouterOrderStatus::Quoted,
            funding_vault_id: None,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address: quote.recipient_address.clone(),
            refund_address,
            action: RouterOrderAction::LimitOrder(LimitOrderAction {
                input_amount: quote.input_amount.clone(),
                output_amount: quote.output_amount.clone(),
                residual_policy: quote.residual_policy,
            }),
            action_timeout_at: now + LIMIT_ORDER_ACTION_TIMEOUT,
            idempotency_key: normalize_idempotency_key(request.idempotency_key.clone())?,
            workflow_trace_id: workflow_trace.trace_id,
            workflow_parent_span_id: workflow_trace.parent_span_id,
            created_at: now,
            updated_at: now,
        };

        let result = self
            .db
            .orders()
            .create_limit_order_from_quote(&order, quote.id)
            .await;
        let quote = match result {
            Ok(quote) => quote,
            Err(RouterCoreError::Conflict { .. }) => {
                return self
                    .resume_limit_order_after_create_conflict(request, quote.id)
                    .await;
            }
            Err(err) => return Err(MarketOrderError::database(err)),
        };
        telemetry::record_order_workflow_event(&order, "order.created");

        Ok((order, quote))
    }

    async fn resume_market_order_from_quote(
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
        if order.idempotency_key == requested_idempotency_key
            && order.refund_address == requested_refund_address
        {
            return Ok((order, quote));
        }

        Err(MarketOrderError::QuoteAlreadyOrdered)
    }

    async fn resume_market_order_after_create_conflict(
        &self,
        request: CreateOrderRequest,
        quote_id: Uuid,
    ) -> MarketOrderResult<(RouterOrder, MarketOrderQuote)> {
        let quote = self
            .db
            .orders()
            .get_market_order_quote_by_id(quote_id)
            .await
            .map_err(MarketOrderError::database)?;
        if quote.order_id.is_some() {
            return self.resume_market_order_from_quote(request, quote).await;
        }
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::QuoteExpired);
        }
        Err(MarketOrderError::QuoteAlreadyOrdered)
    }

    async fn resume_limit_order_from_quote(
        &self,
        request: CreateOrderRequest,
        quote: LimitOrderQuote,
    ) -> MarketOrderResult<(RouterOrder, LimitOrderQuote)> {
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
        if order.idempotency_key == requested_idempotency_key
            && order.refund_address == requested_refund_address
        {
            return Ok((order, quote));
        }

        Err(MarketOrderError::QuoteAlreadyOrdered)
    }

    async fn resume_limit_order_after_create_conflict(
        &self,
        request: CreateOrderRequest,
        quote_id: Uuid,
    ) -> MarketOrderResult<(RouterOrder, LimitOrderQuote)> {
        let quote = self
            .db
            .orders()
            .get_limit_order_quote_by_id(quote_id)
            .await
            .map_err(MarketOrderError::database)?;
        if quote.order_id.is_some() {
            return self.resume_limit_order_from_quote(request, quote).await;
        }
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::QuoteExpired);
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
        paths.retain(|path| path_has_configured_provider_set(self.action_providers.as_ref(), path));
        prefer_same_chain_evm_paths(request, &mut paths);
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
        let provider_health_snapshot = if let Some(service) = self.provider_health.as_ref() {
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
                    provider_health_snapshot.as_ref(),
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

    async fn best_limit_order_quote(
        &self,
        request: &NormalizedLimitOrderQuoteRequest,
        quote_id: Uuid,
        source_depositor_address: &str,
    ) -> MarketOrderResult<ComposedLimitOrderQuote> {
        const MAX_PATH_DEPTH: usize = 5;
        const TOP_K_PATHS: usize = 8;

        let source_canonical = self
            .asset_registry
            .canonical_for(&request.source_asset)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: "limit-order source asset is not registered".to_string(),
            })?;
        let destination_canonical = self
            .asset_registry
            .canonical_for(&request.destination_asset)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: "limit-order destination asset is not registered".to_string(),
            })?;

        let mut paths = self.asset_registry.select_transition_paths(
            &request.source_asset,
            &request.destination_asset,
            MAX_PATH_DEPTH,
        );
        paths.retain(|path| {
            limit_order_transition_index(
                self.asset_registry.as_ref(),
                path,
                source_canonical,
                destination_canonical,
            )
            .is_some()
        });
        paths.retain(|path| path_has_configured_provider_set(self.action_providers.as_ref(), path));
        paths.retain(|path| {
            path.transitions
                .iter()
                .all(|transition| transition.kind != MarketOrderTransitionKind::UniversalRouterSwap)
        });
        if paths.is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: format!(
                    "no V1 limit-order route from {} {} to {} {}; V1 supports BTC/USDC and ETH/USDC routes without universal-router hops",
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
                    "limit-order route-cost ranking failed; falling back to structural ordering"
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
        let provider_health_snapshot = if let Some(service) = self.provider_health.as_ref() {
            Some(
                service
                    .snapshot()
                    .await
                    .map_err(MarketOrderError::database)?,
            )
        } else {
            None
        };

        let exchange_candidates: Vec<Arc<dyn ExchangeProvider>> = self
            .action_providers
            .exchanges()
            .iter()
            .filter(|exchange| {
                exchange.id() == ProviderId::Hyperliquid.as_str()
                    && provider_allowed_for_new_routes(
                        provider_policy_snapshot.as_ref(),
                        provider_health_snapshot.as_ref(),
                        exchange.id(),
                    )
            })
            .cloned()
            .collect();
        if exchange_candidates.is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: "hyperliquid exchange provider is not configured for limit orders"
                    .to_string(),
            });
        }

        let requires_unit = paths.iter().any(|path| {
            path.transitions.iter().any(|transition| {
                matches!(
                    transition.kind,
                    MarketOrderTransitionKind::UnitDeposit
                        | MarketOrderTransitionKind::UnitWithdrawal
                )
            })
        });
        let unit_candidates: Vec<Option<Arc<dyn UnitProvider>>> = if requires_unit {
            self.action_providers
                .units()
                .iter()
                .filter(|unit| {
                    provider_allowed_for_new_routes(
                        provider_policy_snapshot.as_ref(),
                        provider_health_snapshot.as_ref(),
                        unit.id(),
                    )
                })
                .cloned()
                .map(Some)
                .collect()
        } else {
            vec![None]
        };
        if unit_candidates.is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: "unit provider is required but not configured for limit-order route"
                    .to_string(),
            });
        }

        let mut last_error: Option<MarketOrderError> = None;
        for path in paths {
            let limit_index = limit_order_transition_index(
                self.asset_registry.as_ref(),
                &path,
                source_canonical,
                destination_canonical,
            )
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: format!(
                    "retained path {} has no limit-order transition for {} -> {}",
                    path.id,
                    source_canonical.as_str(),
                    destination_canonical.as_str()
                ),
            })?;
            for unit in &unit_candidates {
                for exchange in &exchange_candidates {
                    let candidate = self
                        .compose_limit_transition_path_quote(
                            ComposeLimitTransitionPathQuoteRequest {
                                request,
                                quote_id,
                                source_depositor_address,
                                path: &path,
                                limit_index,
                                provider_policy_snapshot: provider_policy_snapshot.as_ref(),
                                provider_health_snapshot: provider_health_snapshot.as_ref(),
                                unit: unit.as_deref(),
                                exchange: exchange.as_ref(),
                            },
                        )
                        .await;
                    match candidate {
                        Ok(Some(quote)) => return Ok(quote),
                        Ok(None) => {}
                        Err(err) => {
                            warn!(
                                path_id = %path.id,
                                error = %err,
                                "limit-order transition-path quote failed"
                            );
                            last_error = Some(err);
                        }
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| MarketOrderError::NoRoute {
            reason: "no executable V1 limit-order path satisfied the requested constraints"
                .to_string(),
        }))
    }

    async fn compose_limit_transition_path_quote(
        &self,
        spec: ComposeLimitTransitionPathQuoteRequest<'_>,
    ) -> MarketOrderResult<Option<ComposedLimitOrderQuote>> {
        let ComposeLimitTransitionPathQuoteRequest {
            request,
            quote_id,
            source_depositor_address,
            path,
            limit_index,
            provider_policy_snapshot,
            provider_health_snapshot,
            unit,
            exchange,
        } = spec;
        let limit_transition = &path.transitions[limit_index];
        let gas_reimbursement_plan = if let Some(route_costs) = self.route_costs.as_ref() {
            let pricing = route_costs
                .current_or_refresh_live_pricing_snapshot()
                .await
                .ok_or(GasReimbursementError::PricingUnavailable)
                .map_err(MarketOrderError::gas_reimbursement)?;
            optimized_paymaster_reimbursement_plan_with_pricing(
                self.asset_registry.as_ref(),
                path,
                &pricing,
            )
        } else {
            optimized_paymaster_reimbursement_plan(self.asset_registry.as_ref(), path)
        };
        let Some(gas_reimbursement_plan) =
            gas_reimbursement_plan_or_path_ineligible(gas_reimbursement_plan)?
        else {
            return Ok(None);
        };

        let mut expires_at = Utc::now() + ChronoDuration::minutes(10);
        let prefix = TransitionPath {
            id: transition_path_id(&path.transitions[..limit_index]),
            transitions: path.transitions[..limit_index].to_vec(),
        };
        let suffix = TransitionPath {
            id: transition_path_id(&path.transitions[limit_index + 1..]),
            transitions: path.transitions[limit_index + 1..].to_vec(),
        };

        let (limit_output_amount, suffix_legs) = if suffix.transitions.is_empty() {
            (request.output_amount.clone(), Vec::new())
        } else {
            let suffix_request = NormalizedMarketOrderQuoteRequest {
                source_asset: limit_transition.output.asset.clone(),
                destination_asset: request.destination_asset.clone(),
                recipient_address: request.recipient_address.clone(),
                order_kind: MarketOrderQuoteKind::ExactOut {
                    amount_out: request.output_amount.clone(),
                    slippage_bps: Some(0),
                },
            };
            let suffix_order_kind = MarketOrderKind::ExactOut {
                amount_out: request.output_amount.clone(),
                max_amount_in: Some(U256::MAX.to_string()),
            };
            let quote = self
                .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                    request: &suffix_request,
                    order_kind: &suffix_order_kind,
                    quote_id,
                    source_depositor_address,
                    path: &suffix,
                    gas_reimbursement_plan: Some(&gas_reimbursement_plan),
                    provider_policy_snapshot,
                    provider_health_snapshot,
                    unit,
                })
                .await?;
            let Some(quote) = quote else {
                return Ok(None);
            };
            expires_at = expires_at.min(quote.expires_at);
            (
                quote.amount_in,
                quote_legs_from_provider_quote(&quote.provider_quote)?,
            )
        };

        let limit_input_amount = request.input_amount.clone();
        let mut required_limit_input_amount = add_transition_retention(
            &gas_reimbursement_plan,
            &limit_transition.id,
            "limit_order.required_input_amount",
            &limit_input_amount,
        )?;
        if limit_index > 0
            && path.transitions[limit_index - 1].kind
                == MarketOrderTransitionKind::HyperliquidBridgeDeposit
        {
            required_limit_input_amount = add_hyperliquid_spot_send_quote_gas_reserve(
                "limit_order.required_input_amount",
                &required_limit_input_amount,
            )?;
        }

        let (quote_input_amount, mut legs) = if prefix.transitions.is_empty() {
            (required_limit_input_amount.clone(), Vec::new())
        } else {
            let prefix_request = NormalizedMarketOrderQuoteRequest {
                source_asset: request.source_asset.clone(),
                destination_asset: limit_transition.input.asset.clone(),
                recipient_address: self
                    .quote_address_for_chain(quote_id, &limit_transition.input.asset.chain)?,
                order_kind: MarketOrderQuoteKind::ExactOut {
                    amount_out: required_limit_input_amount.clone(),
                    slippage_bps: Some(0),
                },
            };
            let prefix_order_kind = MarketOrderKind::ExactOut {
                amount_out: required_limit_input_amount,
                max_amount_in: Some(U256::MAX.to_string()),
            };
            let quote = self
                .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                    request: &prefix_request,
                    order_kind: &prefix_order_kind,
                    quote_id,
                    source_depositor_address,
                    path: &prefix,
                    gas_reimbursement_plan: Some(&gas_reimbursement_plan),
                    provider_policy_snapshot,
                    provider_health_snapshot,
                    unit,
                })
                .await?;
            let Some(quote) = quote else {
                return Ok(None);
            };
            expires_at = expires_at.min(quote.expires_at);
            (
                quote.amount_in,
                quote_legs_from_provider_quote(&quote.provider_quote)?,
            )
        };
        validate_positive_amount("limit_order.input_amount", &limit_input_amount)?;
        validate_positive_amount("limit_order.output_amount", &limit_output_amount)?;

        legs.push(transition_leg(QuoteLegSpec {
            transition_decl_id: &limit_transition.id,
            transition_kind: limit_transition.kind,
            provider: limit_transition.provider,
            input_asset: &limit_transition.input.asset,
            output_asset: &limit_transition.output.asset,
            amount_in: &limit_input_amount,
            amount_out: &limit_output_amount,
            expires_at,
            raw: json!({
                "schema_version": 1,
                "kind": "hyperliquid_limit_order",
                "residual_policy": LimitOrderResidualPolicy::Refund,
            }),
        }));
        legs.extend(suffix_legs);

        let provider_id =
            composed_provider_id(path, unit.map(|provider| provider.id()), &[exchange.id()]);
        let mut provider_quote = transition_path_quote_blob(path, &legs, &gas_reimbursement_plan);
        if let Some(obj) = provider_quote.as_object_mut() {
            obj.insert("kind".to_string(), json!("limit_order_route"));
            obj.insert(
                "limit_order".to_string(),
                json!({
                    "schema_version": 1,
                    "limit_transition_decl_id": limit_transition.id,
                    "input_amount": limit_input_amount,
                    "output_amount": limit_output_amount,
                    "residual_policy": LimitOrderResidualPolicy::Refund,
                }),
            );
        }

        Ok(Some(ComposedLimitOrderQuote {
            provider_id,
            input_amount: quote_input_amount,
            output_amount: request.output_amount.clone(),
            provider_quote,
            expires_at,
        }))
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
            Ok(Err(RouteMinimumError::Unsupported { reason })) => {
                if route_has_configured_universal_router_path(
                    self.asset_registry.as_ref(),
                    self.action_providers.as_ref(),
                    &request.source_asset,
                    &request.destination_asset,
                ) {
                    return Err(MarketOrderError::RouteMinimum {
                        reason: format!("universal-router route minimum unsupported: {reason}"),
                    });
                }
                return Ok(());
            }
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
        provider_health_snapshot: Option<&crate::services::ProviderHealthSnapshot>,
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
                    provider_allowed_for_new_routes(
                        provider_policy_snapshot,
                        provider_health_snapshot,
                        unit.id(),
                    ) && unit_path_compatible(unit.as_ref(), path)
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

        let has_required_exchanges = !requires_exchange
            || path.transitions.iter().all(|transition| {
                if !matches!(
                    transition.kind,
                    MarketOrderTransitionKind::HyperliquidTrade
                        | MarketOrderTransitionKind::UniversalRouterSwap
                ) {
                    return true;
                }
                let exchange_id = transition.provider.as_str();
                provider_allowed_for_new_routes(
                    provider_policy_snapshot,
                    provider_health_snapshot,
                    exchange_id,
                ) && self.action_providers.exchange(exchange_id).is_some()
            });
        if !has_required_exchanges {
            return Ok(None);
        }

        let mut best_for_path: Option<ComposedMarketOrderQuote> = None;
        let mut last_error: Option<MarketOrderError> = None;
        for unit in &unit_candidates {
            let probe_order_kind = request.order_kind.probe_order_kind();
            let probe_quote = self
                .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                    request,
                    order_kind: &probe_order_kind,
                    quote_id,
                    source_depositor_address,
                    path,
                    gas_reimbursement_plan: None,
                    provider_policy_snapshot,
                    provider_health_snapshot,
                    unit: unit.as_deref(),
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
                    gas_reimbursement_plan: None,
                    provider_policy_snapshot,
                    provider_health_snapshot,
                    unit: unit.as_deref(),
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
            gas_reimbursement_plan,
            provider_policy_snapshot,
            provider_health_snapshot,
            unit,
        } = spec;
        let mut expires_at = Utc::now() + ChronoDuration::minutes(10);
        let mut legs_per_transition: Vec<Vec<QuoteLeg>> = vec![Vec::new(); path.transitions.len()];
        let gas_reimbursement_plan = if let Some(plan) = gas_reimbursement_plan {
            plan.clone()
        } else {
            let planned = if let Some(route_costs) = self.route_costs.as_ref() {
                let pricing = route_costs
                    .current_or_refresh_live_pricing_snapshot()
                    .await
                    .ok_or(GasReimbursementError::PricingUnavailable)
                    .map_err(MarketOrderError::gas_reimbursement)?;
                optimized_paymaster_reimbursement_plan_with_pricing(
                    self.asset_registry.as_ref(),
                    path,
                    &pricing,
                )
            } else {
                optimized_paymaster_reimbursement_plan(self.asset_registry.as_ref(), path)
            };
            let Some(plan) = gas_reimbursement_plan_or_path_ineligible(planned)? else {
                return Ok(None);
            };
            plan
        };

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
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                            let bridge_id = transition.provider.as_str();
                            if !provider_allowed_for_new_routes(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                bridge_id,
                            ) {
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
                                        min_amount_out: Some("1".to_string()),
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
                            legs_per_transition[index] =
                                bridge_quote_transition_legs(transition, &bridge_quote);
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
                            let Some(exchange) = self.exchange_for_transition(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                transition,
                            )?
                            else {
                                return Ok(None);
                            };
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
                                exchange.as_ref(),
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
                            let Some(exchange) = self.exchange_for_transition(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                transition,
                            )?
                            else {
                                return Ok(None);
                            };
                            let input_decimals = self
                                .exchange_asset_decimals(&transition.input.asset)
                                .await?;
                            let output_decimals = self
                                .exchange_asset_decimals(&transition.output.asset)
                                .await?;
                            let exchange_quote = quote_exchange(
                                exchange.as_ref(),
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
                            let recipient_address = self.unit_withdrawal_recipient_address(
                                request, quote_id, path, index, transition,
                            )?;
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &provider_amount_in,
                                amount_out: &provider_amount_in,
                                expires_at,
                                raw: unit_withdrawal_quote_raw(&recipient_address),
                            }));
                            cursor_amount = provider_amount_in;
                        }
                    }
                }

                let legs = flatten_transition_legs(legs_per_transition);
                let provider_id = composed_provider_id(
                    path,
                    unit.map(|provider| provider.id()),
                    &transition_exchange_provider_ids(path),
                );
                Ok(Some(ComposedMarketOrderQuote {
                    provider_id,
                    amount_in: amount_in.clone(),
                    amount_out: cursor_amount,
                    min_amount_out: min_amount_out.clone(),
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
                            let recipient_address = self.unit_withdrawal_recipient_address(
                                request, quote_id, path, index, transition,
                            )?;
                            legs_per_transition[index].push(transition_leg(QuoteLegSpec {
                                transition_decl_id: &transition.id,
                                transition_kind: transition.kind,
                                provider: transition.provider,
                                input_asset: &transition.input.asset,
                                output_asset: &transition.output.asset,
                                amount_in: &required_output,
                                amount_out: &required_output,
                                expires_at,
                                raw: unit_withdrawal_quote_raw(&recipient_address),
                            }));
                        }
                        MarketOrderTransitionKind::HyperliquidTrade => {
                            let Some(exchange) = self.exchange_for_transition(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                transition,
                            )?
                            else {
                                return Ok(None);
                            };
                            let max_in_cap = if index == 0 {
                                max_amount_in.clone()
                            } else {
                                Some(practical_max_input_for_asset(&transition.input.asset))
                            };
                            let exchange_quote = quote_exchange(
                                exchange.as_ref(),
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
                            let Some(exchange) = self.exchange_for_transition(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                transition,
                            )?
                            else {
                                return Ok(None);
                            };
                            let max_in_cap = if index == 0 {
                                max_amount_in.clone()
                            } else {
                                Some(practical_max_input_for_asset(&transition.input.asset))
                            };
                            let input_decimals = self
                                .exchange_asset_decimals(&transition.input.asset)
                                .await?;
                            let output_decimals = self
                                .exchange_asset_decimals(&transition.output.asset)
                                .await?;
                            let exchange_quote = quote_exchange(
                                exchange.as_ref(),
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
                        | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                        | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => {
                            let bridge_id = transition.provider.as_str();
                            if !provider_allowed_for_new_routes(
                                provider_policy_snapshot,
                                provider_health_snapshot,
                                bridge_id,
                            ) {
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
                                Some(practical_max_input_for_asset(&transition.input.asset))
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
                            legs_per_transition[index] =
                                bridge_quote_transition_legs(transition, &bridge_quote);
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
                        "amount_in",
                        &required_output,
                    )?;
                }

                let legs = flatten_transition_legs(legs_per_transition);
                let provider_id = composed_provider_id(
                    path,
                    unit.map(|provider| provider.id()),
                    &transition_exchange_provider_ids(path),
                );
                Ok(Some(ComposedMarketOrderQuote {
                    provider_id,
                    amount_in: required_output,
                    amount_out: amount_out.clone(),
                    min_amount_out: None,
                    max_amount_in: max_amount_in.clone(),
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

    fn exchange_for_transition(
        &self,
        provider_policy_snapshot: Option<&crate::services::ProviderPolicySnapshot>,
        provider_health_snapshot: Option<&crate::services::ProviderHealthSnapshot>,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<Option<Arc<dyn ExchangeProvider>>> {
        let exchange_id = transition.provider.as_str();
        if !provider_allowed_for_new_routes(
            provider_policy_snapshot,
            provider_health_snapshot,
            exchange_id,
        ) {
            return Ok(None);
        }
        self.action_providers
            .exchange(exchange_id)
            .map(Some)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: format!("exchange provider {exchange_id} is not configured"),
            })
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
            MarketOrderTransitionKind::AcrossBridge
                | MarketOrderTransitionKind::CctpBridge
                | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal
        ) && index + 1 == path.transitions.len()
        {
            Ok(request.recipient_address.clone())
        } else {
            self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
        }
    }

    fn unit_withdrawal_recipient_address(
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

    fn validate_and_normalize_limit_order_request(
        &self,
        request: LimitOrderQuoteRequest,
    ) -> MarketOrderResult<NormalizedLimitOrderQuoteRequest> {
        validate_positive_amount("input_amount", &request.input_amount)?;
        validate_positive_amount("output_amount", &request.output_amount)?;
        let source_asset = self.validate_and_normalize_asset(&request.from_asset)?;
        let destination_asset = self.validate_and_normalize_asset(&request.to_asset)?;
        let source_canonical = self
            .asset_registry
            .canonical_for(&source_asset)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: "V1 limit orders require a router-declared source asset".to_string(),
            })?;
        let destination_canonical = self
            .asset_registry
            .canonical_for(&destination_asset)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: "V1 limit orders require a router-declared destination asset".to_string(),
            })?;
        if !v1_limit_order_pair_supported(source_canonical, destination_canonical) {
            return Err(MarketOrderError::NoRoute {
                reason: format!(
                    "V1 limit orders support only BTC/USDC and ETH/USDC pairs, got {}/{}",
                    source_canonical.as_str(),
                    destination_canonical.as_str()
                ),
            });
        }
        let recipient_address = self.validate_and_normalize_recipient_address(
            &destination_asset.chain,
            &request.recipient_address,
        )?;
        Ok(NormalizedLimitOrderQuoteRequest {
            source_asset,
            destination_asset,
            recipient_address,
            input_amount: request.input_amount,
            output_amount: request.output_amount,
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
                if let Some(min_amount_out) = quote.min_amount_out.as_deref() {
                    let quoted_amount_out = parse_amount("amount_out", &quote.amount_out)?;
                    let requested_min_out = parse_amount("min_amount_out", min_amount_out)?;
                    if quoted_amount_out < requested_min_out {
                        return Err(MarketOrderError::NoRoute {
                            reason: "provider exact-in quote is below min_amount_out".to_string(),
                        });
                    }
                }
            }
            MarketOrderQuoteKind::ExactOut { amount_out, .. } => {
                if quote.amount_out != *amount_out {
                    return Err(MarketOrderError::NoRoute {
                        reason: "provider exact-out quote changed amount_out".to_string(),
                    });
                }
                if let Some(max_amount_in) = quote.max_amount_in.as_deref() {
                    let quoted_amount_in = parse_amount("amount_in", &quote.amount_in)?;
                    let requested_max_in = parse_amount("max_amount_in", max_amount_in)?;
                    if quoted_amount_in > requested_max_in {
                        return Err(MarketOrderError::NoRoute {
                            reason: "provider exact-out quote is above max_amount_in".to_string(),
                        });
                    }
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
        let estimated_fee = bitcoin_chain
            .estimate_p2wpkh_transfer_fee_sats(
                BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_INPUTS,
                BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_OUTPUTS,
            )
            .await
            .map_err(|err| MarketOrderError::NoRoute {
                reason: format!("failed to estimate bitcoin Unit deposit miner fee: {err}"),
            })?;
        let reserved_fee = apply_bps_u64(estimated_fee, BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS)?;
        Ok(Some(U256::from(reserved_fee)))
    }
}

fn provider_allowed_for_new_routes(
    snapshot: Option<&crate::services::ProviderPolicySnapshot>,
    health: Option<&crate::services::ProviderHealthSnapshot>,
    provider: &str,
) -> bool {
    if let Some(snapshot) = snapshot {
        let policy = snapshot.policy(provider);
        if !policy.allows_new_routes() {
            let reason = if policy.reason.is_empty() {
                "provider_policy".to_string()
            } else {
                policy.reason.clone()
            };
            telemetry::record_provider_quote_blocked(provider, &reason);
            return false;
        }
    }

    if let Some(health) = health {
        if !health.allows_new_routes(provider) {
            telemetry::record_provider_quote_blocked(provider, "provider_health_down");
            return false;
        }
    }

    true
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

fn prefer_same_chain_evm_paths(
    request: &NormalizedMarketOrderQuoteRequest,
    paths: &mut Vec<TransitionPath>,
) {
    if request.source_asset.chain != request.destination_asset.chain
        || !request.source_asset.chain.as_str().starts_with("evm:")
    {
        return;
    }

    let chain = &request.source_asset.chain;
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

fn path_has_configured_provider_set(
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

fn route_has_configured_universal_router_path(
    registry: &AssetRegistry,
    action_providers: &ActionProviderRegistry,
    source_asset: &DepositAsset,
    destination_asset: &DepositAsset,
) -> bool {
    registry
        .select_transition_paths(source_asset, destination_asset, 5)
        .into_iter()
        .filter(|path| is_executable_transition_path(registry, path))
        .filter(|path| path_has_configured_provider_set(action_providers, path))
        .any(|path| {
            path.transitions
                .iter()
                .any(|transition| transition.kind == MarketOrderTransitionKind::UniversalRouterSwap)
        })
}

fn transition_leg(spec: QuoteLegSpec<'_>) -> QuoteLeg {
    QuoteLeg::new(spec)
}

fn bridge_quote_transition_legs(
    transition: &TransitionDecl,
    bridge_quote: &BridgeQuote,
) -> Vec<QuoteLeg> {
    if transition.kind == MarketOrderTransitionKind::CctpBridge {
        return cctp_quote_transition_legs(transition, bridge_quote);
    }

    vec![transition_leg(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: &bridge_quote.amount_in,
        amount_out: &bridge_quote.amount_out,
        expires_at: bridge_quote.expires_at,
        raw: bridge_quote.provider_quote.clone(),
    })]
}

fn cctp_quote_transition_legs(
    transition: &TransitionDecl,
    bridge_quote: &BridgeQuote,
) -> Vec<QuoteLeg> {
    let burn_raw = cctp_quote_phase_raw(
        &bridge_quote.provider_quote,
        OrderExecutionStepType::CctpBurn,
    );
    let receive_raw = cctp_quote_phase_raw(
        &bridge_quote.provider_quote,
        OrderExecutionStepType::CctpReceive,
    );

    let burn = transition_leg(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in: &bridge_quote.amount_in,
        amount_out: &bridge_quote.amount_out,
        expires_at: bridge_quote.expires_at,
        raw: burn_raw,
    })
    .with_execution_step_type(OrderExecutionStepType::CctpBurn);

    let receive_transition_id = format!("{}:receive", transition.id);
    let receive = transition_leg(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.output.asset,
        output_asset: &transition.output.asset,
        amount_in: &bridge_quote.amount_out,
        amount_out: &bridge_quote.amount_out,
        expires_at: bridge_quote.expires_at,
        raw: receive_raw,
    })
    .with_child_transition_id(receive_transition_id)
    .with_execution_step_type(OrderExecutionStepType::CctpReceive);

    vec![burn, receive]
}

fn cctp_quote_phase_raw(
    provider_quote: &Value,
    execution_step_type: OrderExecutionStepType,
) -> Value {
    let mut raw = provider_quote.clone();
    if let Some(object) = raw.as_object_mut() {
        object.insert("bridge_kind".to_string(), json!("cctp_bridge"));
        object.insert(
            "kind".to_string(),
            json!(execution_step_type.to_db_string()),
        );
        object.insert(
            "execution_step_type".to_string(),
            json!(execution_step_type.to_db_string()),
        );
    }
    raw
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
                        execution_step_type: execution_step_type_for_transition_kind(
                            transition_kind,
                        ),
                        provider,
                        input_asset,
                        output_asset,
                        amount_in: required_quote_leg_amount(leg, "amount_in")?,
                        amount_out: required_quote_leg_amount(leg, "amount_out")?,
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

fn required_quote_leg_amount(leg: &Value, field: &'static str) -> MarketOrderResult<String> {
    let Some(amount) = leg.get(field).and_then(Value::as_str) else {
        return Err(MarketOrderError::NoRoute {
            reason: format!("hyperliquid cross-token leg missing {field}"),
        });
    };
    if amount.is_empty() {
        return Err(MarketOrderError::NoRoute {
            reason: format!("hyperliquid cross-token leg has empty {field}"),
        });
    }
    Ok(amount.to_string())
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

fn transition_exchange_provider_ids(path: &TransitionPath) -> Vec<&str> {
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

fn gas_reimbursement_plan_or_path_ineligible(
    result: Result<GasReimbursementPlan, GasReimbursementError>,
) -> MarketOrderResult<Option<GasReimbursementPlan>> {
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
        ) => Err(MarketOrderError::gas_reimbursement(err)),
    }
}

fn transition_path_id(transitions: &[TransitionDecl]) -> String {
    transitions
        .iter()
        .map(|transition| transition.id.as_str())
        .collect::<Vec<_>>()
        .join(">")
}

fn quote_legs_from_provider_quote(provider_quote: &Value) -> MarketOrderResult<Vec<QuoteLeg>> {
    provider_quote
        .get("legs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|value| {
            serde_json::from_value(value.clone()).map_err(|err| MarketOrderError::NoRoute {
                reason: format!("quoted transition leg is invalid: {err}"),
            })
        })
        .collect()
}

fn v1_limit_order_pair_supported(source: CanonicalAsset, destination: CanonicalAsset) -> bool {
    matches!(
        (source, destination),
        (CanonicalAsset::Usdc, CanonicalAsset::Btc)
            | (CanonicalAsset::Btc, CanonicalAsset::Usdc)
            | (CanonicalAsset::Usdc, CanonicalAsset::Eth)
            | (CanonicalAsset::Eth, CanonicalAsset::Usdc)
    )
}

fn limit_order_transition_index(
    registry: &AssetRegistry,
    path: &TransitionPath,
    source_canonical: CanonicalAsset,
    destination_canonical: CanonicalAsset,
) -> Option<usize> {
    let mut matching_index = None;
    for (index, transition) in path.transitions.iter().enumerate() {
        if transition.kind == MarketOrderTransitionKind::UniversalRouterSwap {
            return None;
        }
        if transition.kind != MarketOrderTransitionKind::HyperliquidTrade {
            continue;
        }
        if transition.from
            != (MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical: source_canonical,
            })
            || transition.to
                != (MarketOrderNode::Venue {
                    provider: ProviderId::Hyperliquid,
                    canonical: destination_canonical,
                })
        {
            return None;
        }
        let input_canonical = registry.canonical_for(&transition.input.asset)?;
        let output_canonical = registry.canonical_for(&transition.output.asset)?;
        if input_canonical != source_canonical || output_canonical != destination_canonical {
            return None;
        }
        if !v1_limit_order_pair_supported(input_canonical, output_canonical) {
            return None;
        }
        if matching_index.replace(index).is_some() {
            return None;
        }
    }
    matching_index
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
    gross
        .checked_sub(fee_reserve)
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount minus bitcoin miner fee reserve underflowed for transition {transition_id}"
            ),
        })
        .map(|amount| amount.to_string())
}

fn add_bitcoin_fee_reserve(amount: &str, fee_reserve: U256) -> MarketOrderResult<String> {
    let amount = parse_amount("amount_in", amount)?;
    amount
        .checked_add(fee_reserve)
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field: "amount_in",
            reason: "amount plus bitcoin miner fee reserve overflowed".to_string(),
        })
        .map(|amount| amount.to_string())
}

fn apply_bps_u64(value: u64, bps: u64) -> MarketOrderResult<u64> {
    let numerator = u128::from(value)
        .checked_mul(u128::from(bps))
        .and_then(|value| value.checked_add(9_999))
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field: "bps",
            reason: "basis point multiplication overflowed".to_string(),
        })?;
    u64::try_from(numerator / 10_000).map_err(|_| MarketOrderError::InvalidAmount {
        field: "bps",
        reason: "basis point result overflowed u64".to_string(),
    })
}

fn apply_transition_retention(
    plan: &GasReimbursementPlan,
    transition_id: &str,
    field: &'static str,
    gross_amount: &str,
) -> MarketOrderResult<String> {
    let gross = parse_amount(field, gross_amount)?;
    let retention = try_transition_retention_amount(plan, transition_id).map_err(|source| {
        MarketOrderError::GasReimbursement {
            source: Box::new(source),
        }
    })?;
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
    gross
        .checked_sub(retention)
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount minus paymaster gas retention underflowed for transition {transition_id}"
            ),
        })
        .map(|amount| amount.to_string())
}

fn add_transition_retention(
    plan: &GasReimbursementPlan,
    transition_id: &str,
    field: &'static str,
    amount: &str,
) -> MarketOrderResult<String> {
    let amount = parse_amount(field, amount)?;
    let retention = try_transition_retention_amount(plan, transition_id).map_err(|source| {
        MarketOrderError::GasReimbursement {
            source: Box::new(source),
        }
    })?;
    amount
        .checked_add(retention)
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: format!(
                "amount plus paymaster gas retention overflowed for transition {transition_id}"
            ),
        })
        .map(|amount| amount.to_string())
}

fn practical_max_input_for_asset(_asset: &DepositAsset) -> String {
    PROBE_MAX_AMOUNT_IN.to_string()
}

fn transition_min_amount_out(
    path: &TransitionPath,
    index: usize,
    final_min_amount_out: &Option<String>,
) -> Option<String> {
    if index + 1 == path.transitions.len() {
        final_min_amount_out.clone()
    } else {
        Some("1".to_string())
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
    amount
        .checked_sub(reserve)
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: "amount minus Hyperliquid spot token transfer gas reserve underflowed"
                .to_string(),
        })
        .map(|amount| amount.to_string())
}

fn add_hyperliquid_spot_send_quote_gas_reserve(
    field: &'static str,
    value: &str,
) -> MarketOrderResult<String> {
    let amount = parse_amount(field, value)?;
    amount
        .checked_add(U256::from(HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW))
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: "amount plus Hyperliquid spot token transfer gas reserve overflowed"
                .to_string(),
        })
        .map(|amount| amount.to_string())
}

fn unit_withdrawal_quote_raw(recipient_address: &str) -> Value {
    json!({
        "recipient_address": recipient_address,
        "hyperliquid_core_activation_fee": hyperliquid_core_activation_fee_quote_json(),
    })
}

fn hyperliquid_core_activation_fee_quote_json() -> Value {
    json!({
        "kind": "first_transfer_to_new_hypercore_destination",
        "quote_asset": "USDC",
        "amount_raw": HYPERLIQUID_CORE_ACTIVATION_FEE_RAW.to_string(),
        "amount_decimal": "1",
        "source": "hyperliquid_activation_gas_fee",
    })
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

#[derive(Debug, Clone)]
struct NormalizedLimitOrderQuoteRequest {
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    recipient_address: String,
    input_amount: String,
    output_amount: String,
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
    fn slippage_bps(&self) -> Option<u64> {
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
                min_amount_out: Some("1".to_string()),
            },
            Self::ExactOut { amount_out, .. } => MarketOrderKind::ExactOut {
                amount_out: amount_out.clone(),
                max_amount_in: Some(PROBE_MAX_AMOUNT_IN.to_string()),
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
                min_amount_out: slippage_bps
                    .map(|slippage_bps| {
                        apply_slippage_floor("amount_out", &probe_quote.amount_out, slippage_bps)
                    })
                    .transpose()?,
            }),
            Self::ExactOut {
                amount_out,
                slippage_bps,
            } => Ok(MarketOrderKind::ExactOut {
                amount_out: amount_out.clone(),
                max_amount_in: slippage_bps
                    .map(|slippage_bps| {
                        apply_slippage_ceiling("amount_in", &probe_quote.amount_in, slippage_bps)
                    })
                    .transpose()?,
            }),
        }
    }
}

fn market_order_kind_from_quote(quote: &MarketOrderQuote) -> MarketOrderKind {
    match quote.order_kind {
        MarketOrderKindType::ExactIn => MarketOrderKind::ExactIn {
            amount_in: quote.amount_in.clone(),
            min_amount_out: quote.min_amount_out.clone(),
        },
        MarketOrderKindType::ExactOut => MarketOrderKind::ExactOut {
            amount_out: quote.amount_out.clone(),
            max_amount_in: quote.max_amount_in.clone(),
        },
    }
}

fn validate_market_order_quote_kind(order_kind: &MarketOrderQuoteKind) -> MarketOrderResult<()> {
    match order_kind {
        MarketOrderQuoteKind::ExactIn {
            amount_in,
            slippage_bps,
        } => {
            validate_positive_amount("amount_in", amount_in)?;
            if let Some(slippage_bps) = slippage_bps {
                validate_slippage_bps(*slippage_bps)?;
            }
        }
        MarketOrderQuoteKind::ExactOut {
            amount_out,
            slippage_bps,
        } => {
            validate_positive_amount("amount_out", amount_out)?;
            if let Some(slippage_bps) = slippage_bps {
                validate_slippage_bps(*slippage_bps)?;
            }
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
    let multiplier = U256::from(10_000_u64 - slippage_bps);
    let bounded =
        expected
            .checked_mul(multiplier)
            .ok_or_else(|| MarketOrderError::InvalidAmount {
                field,
                reason: "slippage-adjusted amount overflowed".to_string(),
            })?
            / U256::from(10_000_u64);
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
    let numerator = expected
        .checked_mul(U256::from(10_000_u64 + slippage_bps))
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: "slippage-adjusted amount overflowed".to_string(),
        })?;
    Ok(div_ceil_u256(field, numerator, U256::from(10_000_u64))?.to_string())
}

fn div_ceil_u256(
    field: &'static str,
    numerator: U256,
    denominator: U256,
) -> MarketOrderResult<U256> {
    if denominator.is_zero() {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: "division denominator must be greater than zero".to_string(),
        });
    }
    if numerator.is_zero() {
        return Ok(U256::ZERO);
    }
    let adjusted = numerator.checked_sub(U256::from(1_u64)).ok_or_else(|| {
        MarketOrderError::InvalidAmount {
            field,
            reason: "ceiling division numerator underflowed".to_string(),
        }
    })?;
    adjusted
        .checked_div(denominator)
        .and_then(|quotient| quotient.checked_add(U256::from(1_u64)))
        .ok_or_else(|| MarketOrderError::InvalidAmount {
            field,
            reason: "ceiling division overflowed".to_string(),
        })
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
    if value.len() > MAX_U256_DECIMAL_DIGITS {
        return Err(MarketOrderError::InvalidAmount {
            field,
            reason: format!("amount cannot exceed {MAX_U256_DECIMAL_DIGITS} decimal digits"),
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
    if !is_protocol_token(&value) {
        return Err(MarketOrderError::InvalidIdempotencyKey {
            reason: "key may only contain letters, numbers, '.', '_', ':', and '-'".to_string(),
        });
    }
    Ok(Some(value))
}

fn is_protocol_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b':' | b'-'
        )
    })
}

fn fallback_workflow_trace_context(order_id: Uuid) -> observability::WorkflowTraceContext {
    let trace_id = order_id.simple().to_string();
    let mut parent_span_id = trace_id[16..32].to_string();
    if parent_span_id == "0000000000000000" {
        parent_span_id = "0000000000000001".to_string();
    }

    observability::WorkflowTraceContext {
        trace_id,
        parent_span_id,
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_out_probe_uses_provider_parseable_max_input() {
        let probe = MarketOrderQuoteKind::ExactOut {
            amount_out: "1000".to_string(),
            slippage_bps: Some(100),
        }
        .probe_order_kind();

        let MarketOrderKind::ExactOut { max_amount_in, .. } = probe else {
            panic!("exact-out quote request must produce exact-out probe");
        };

        let max_amount_in = max_amount_in.expect("exact-out probe must have max input");
        assert_eq!(max_amount_in, PROBE_MAX_AMOUNT_IN);
        assert!(max_amount_in.parse::<u128>().is_ok());
        assert_ne!(max_amount_in, U256::MAX.to_string());
    }

    #[test]
    fn amount_parser_rejects_oversized_decimal_strings_before_u256_parse() {
        let error = parse_amount("amount_in", &"9".repeat(MAX_U256_DECIMAL_DIGITS + 1))
            .expect_err("oversized amount should fail");

        assert!(matches!(
            error,
            MarketOrderError::InvalidAmount {
                field: "amount_in",
                ..
            }
        ));
        assert!(error
            .to_string()
            .contains("amount cannot exceed 78 decimal digits"));
    }

    #[test]
    fn slippage_bounds_reject_overflow_instead_of_saturating() {
        let floor_error = apply_slippage_floor("amount_in", &U256::MAX.to_string(), 1)
            .expect_err("floor overflow should fail");
        let ceiling_error = apply_slippage_ceiling("amount_out", &U256::MAX.to_string(), 1)
            .expect_err("ceiling overflow should fail");

        assert!(floor_error
            .to_string()
            .contains("slippage-adjusted amount overflowed"));
        assert!(ceiling_error
            .to_string()
            .contains("slippage-adjusted amount overflowed"));
    }

    #[test]
    fn bounded_order_kind_keeps_slippage_bounds_optional() {
        let probe_quote = ComposedMarketOrderQuote {
            provider_id: "test-provider".to_string(),
            amount_in: "1000".to_string(),
            amount_out: "2000".to_string(),
            min_amount_out: None,
            max_amount_in: None,
            provider_quote: json!({}),
            expires_at: Utc::now(),
        };

        let exact_in = MarketOrderQuoteKind::ExactIn {
            amount_in: "1000".to_string(),
            slippage_bps: None,
        }
        .bounded_order_kind(&probe_quote)
        .unwrap();
        assert!(matches!(
            exact_in,
            MarketOrderKind::ExactIn {
                min_amount_out: None,
                ..
            }
        ));

        let exact_out = MarketOrderQuoteKind::ExactOut {
            amount_out: "2000".to_string(),
            slippage_bps: None,
        }
        .bounded_order_kind(&probe_quote)
        .unwrap();
        assert!(matches!(
            exact_out,
            MarketOrderKind::ExactOut {
                max_amount_in: None,
                ..
            }
        ));
    }

    #[test]
    fn reserve_additions_reject_overflow_instead_of_saturating() {
        let plan = GasReimbursementPlan {
            schema_version: 1,
            policy: "test".to_string(),
            quote_safety_multiplier_bps: 10_000,
            debts: vec![],
            retention_actions: vec![
                router_core::services::gas_reimbursement::GasRetentionAction {
                    id: "retention-1".to_string(),
                    transition_decl_id: "transition-1".to_string(),
                    settlement_chain_id: "evm:1".to_string(),
                    settlement_asset_id: "native".to_string(),
                    settlement_decimals: 18,
                    settlement_provider_asset: None,
                    amount: "1".to_string(),
                    estimated_usd_micro: "1".to_string(),
                    recipient_role: "paymaster_wallet".to_string(),
                    timing: "before_provider_action".to_string(),
                    debt_ids: vec![],
                },
            ],
        };

        assert!(add_bitcoin_fee_reserve(&U256::MAX.to_string(), U256::from(1_u64)).is_err());
        assert!(add_transition_retention(
            &plan,
            "transition-1",
            "amount_in",
            &U256::MAX.to_string()
        )
        .is_err());
        assert!(
            add_hyperliquid_spot_send_quote_gas_reserve("amount_in", &U256::MAX.to_string())
                .is_err()
        );
    }

    #[test]
    fn bps_u64_multiplier_rejects_overflow_instead_of_saturating() {
        assert_eq!(apply_bps_u64(10_000, 12_500).unwrap(), 12_500);

        let error = apply_bps_u64(u64::MAX, 12_500).unwrap_err();
        assert!(error.to_string().contains("overflowed u64"));
    }

    #[test]
    fn cross_token_quote_legs_require_explicit_amounts() {
        let quote = ExchangeQuote {
            provider_id: "hyperliquid".to_string(),
            amount_in: "100".to_string(),
            amount_out: "50".to_string(),
            min_amount_out: None,
            max_amount_in: None,
            provider_quote: json!({
                "kind": "spot_cross_token",
                "legs": [{
                    "input_asset": { "chain_id": "hyperliquid", "asset": "native" },
                    "output_asset": { "chain_id": "hyperliquid", "asset": "UBTC" },
                    "amount_in": "100"
                }]
            }),
            expires_at: Utc::now(),
        };

        let error = exchange_quote_transition_legs(
            "transition-1",
            MarketOrderTransitionKind::HyperliquidTrade,
            ProviderId::Hyperliquid,
            &quote,
        )
        .unwrap_err();

        assert!(error.to_string().contains("missing amount_out"), "{error}");
    }

    #[test]
    fn div_ceil_u256_handles_max_numerator_without_overflow() {
        let expected = (U256::MAX / U256::from(2_u64)) + U256::from(1_u64);
        assert_eq!(
            div_ceil_u256("amount", U256::MAX, U256::from(2_u64)).unwrap(),
            expected
        );
    }

    #[test]
    fn div_ceil_u256_rejects_zero_denominator() {
        let error = div_ceil_u256("amount", U256::from(1_u64), U256::ZERO).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("division denominator must be greater than zero"),
            "{error}"
        );
    }

    #[test]
    fn gas_reimbursement_path_ineligibility_is_not_a_quote_error() {
        let no_site = gas_reimbursement_plan_or_path_ineligible(Err(
            GasReimbursementError::NoSettlementSite {
                debt_ids: vec!["paymaster-gas:0".to_string()],
            },
        ))
        .unwrap();
        assert!(no_site.is_none());

        let unsupported = gas_reimbursement_plan_or_path_ineligible(Err(
            GasReimbursementError::UnsupportedSettlementAsset {
                asset: DepositAsset {
                    chain: ChainId::parse("hyperliquid").unwrap(),
                    asset: AssetId::reference("UBTC"),
                },
            },
        ))
        .unwrap();
        assert!(unsupported.is_none());

        let pricing = gas_reimbursement_plan_or_path_ineligible(Err(
            GasReimbursementError::PricingUnavailable,
        ));
        assert!(matches!(
            pricing,
            Err(MarketOrderError::GasReimbursement { .. })
        ));
    }

    #[test]
    fn order_service_idempotency_keys_are_trimmed_bounded_and_token_shaped() {
        assert_eq!(
            normalize_idempotency_key(Some("  order:abc_123.456-789  ".to_string())).unwrap(),
            Some("order:abc_123.456-789".to_string())
        );
        assert!(matches!(
            normalize_idempotency_key(Some("  ".to_string())),
            Err(MarketOrderError::InvalidIdempotencyKey { .. })
        ));
        assert!(matches!(
            normalize_idempotency_key(Some("order key/with/slashes".to_string())),
            Err(MarketOrderError::InvalidIdempotencyKey { .. })
        ));
        assert!(matches!(
            normalize_idempotency_key(Some("a".repeat(129))),
            Err(MarketOrderError::InvalidIdempotencyKey { .. })
        ));
    }
}
