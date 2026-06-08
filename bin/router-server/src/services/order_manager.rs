use crate::{
    api::{
        CreateOrderRequest, LimitOrderQuoteRequest, MarketOrderQuoteRequest, QuoteRoutingRequest,
    },
    error::RouterServerError,
    services::{
        deposit_address::{derive_deposit_address_for_quote, DepositAddressError},
        provider_health::ProviderHealthService,
        provider_policy::ProviderPolicyService,
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
        MarketOrderQuote, OrderExecutionStepType, ProviderOrderKind, RouterOrder,
        RouterOrderAction, RouterOrderQuote, RouterOrderQuoteEnvelope, RouterOrderStatus,
        RouterOrderType, SwapTimeRouteKey,
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
        custody_action_executor::PaymasterRegistry,
        gas_reimbursement::{
            optimized_paymaster_reimbursement_plan,
            optimized_paymaster_reimbursement_plan_with_pricing, try_transition_retention_amount,
            GasReimbursementError, GasReimbursementPlan,
        },
        pricing::apply_bps_multiplier,
        quote_legs::{
            execution_step_type_for_transition_kind, QuoteLeg, QuoteLegAsset, QuoteLegSpec,
        },
        route_costs::{
            confidence_band, rank_transition_paths_structurally, raw_amount_to_usd_micros,
            select_best_quote, LiveQuoteOutcome, RankedTransitionPath, RouteCostService,
            DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
        },
        usd_valuation::{empty_usd_valuation, limit_quote_usd_valuation, quote_usd_valuation},
    },
};
use router_primitives::ChainType;
use serde_json::{json, Value};
use snafu::Snafu;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};
use futures_util::future::join_all;
use std::str::FromStr;
use tracing::{info, warn};
use uuid::Uuid;

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
const MAX_PROVIDER_SEQUENCE_LEN: usize = 5;
const PAYMASTER_QUOTE_BALANCE_SAFETY_MULTIPLIER_BPS: u64 = 12_500;

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

    #[snafu(display("Invalid routing constraint: {}", reason))]
    InvalidRouting { reason: String },

    #[snafu(display("No market-order route found: {}", reason))]
    NoRoute { reason: String },

    #[snafu(display(
        "Estimated output {} is below the viable floor {} for {} {} (the order is too small to clear route fees)",
        estimated_amount_out,
        output_floor,
        destination_chain,
        destination_asset
    ))]
    OutputBelowFloor {
        estimated_amount_out: String,
        output_floor: String,
        destination_chain: Box<ChainId>,
        destination_asset: Box<AssetId>,
    },

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
    pub estimated_amount_out: String,
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
    order_kind: &'a ProviderOrderKind,
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
    route_costs: Option<Arc<RouteCostService>>,
    provider_policies: Option<Arc<ProviderPolicyService>>,
    provider_health: Option<Arc<ProviderHealthService>>,
    paymasters: PaymasterRegistry,
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
            route_costs: None,
            provider_policies: None,
            provider_health: None,
            paymasters: PaymasterRegistry::default(),
        }
    }

    #[must_use]
    pub fn with_route_costs(mut self, route_costs: Option<Arc<RouteCostService>>) -> Self {
        self.route_costs = route_costs;
        self
    }

    /// Drop paths that traverse HyperEVM when we cannot currently price its
    /// gas. Without a live HyperEVM gas price these routes can't be costed, so
    /// surfacing them would be misleading. Returns `true` if any path was
    /// removed.
    async fn filter_unpriceable_hyperevm_paths(&self, paths: &mut Vec<TransitionPath>) -> bool {
        if !paths.iter().any(path_uses_hyperevm) {
            return false;
        }
        let Some(route_costs) = self.route_costs.as_ref() else {
            paths.retain(|path| !path_uses_hyperevm(path));
            return true;
        };
        let pricing = route_costs.force_refresh_pricing_snapshot().await;
        let hyperevm_chain = ChainId::parse("evm:999").expect("static HyperEVM chain id");
        if pricing.supports_chain_gas_pricing(&hyperevm_chain) {
            return false;
        }
        paths.retain(|path| !path_uses_hyperevm(path));
        true
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
    #[must_use]
    pub fn with_paymasters(mut self, paymasters: PaymasterRegistry) -> Self {
        self.paymasters = paymasters;
        self
    }

    /// Shared static routing graph. Exposed for the admin route-graph
    /// visualizer endpoint.
    #[must_use]
    pub fn asset_registry(&self) -> &AssetRegistry {
        self.asset_registry.as_ref()
    }

    pub async fn quote_market_order(
        &self,
        request: MarketOrderQuoteRequest,
    ) -> MarketOrderResult<RouterOrderQuoteEnvelope> {
        self.quote_market_order_with_routing(request, QuoteRoutingRequest::default())
            .await
    }

    pub async fn quote_market_order_with_routing(
        &self,
        request: MarketOrderQuoteRequest,
        routing: QuoteRoutingRequest,
    ) -> MarketOrderResult<RouterOrderQuoteEnvelope> {
        let started = Instant::now();
        let normalized_request = self.validate_and_normalize_quote_request(request, routing)?;
        telemetry::record_market_order_quote_requested(&normalized_request.source_asset);

        let quote_id = Uuid::now_v7();
        let (depositor_address, _) = derive_deposit_address_for_quote(
            self.chain_registry.as_ref(),
            &self.settings.master_key_bytes(),
            quote_id,
            &normalized_request.source_asset.chain,
        )
        .map_err(MarketOrderError::deposit_address)?;

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

        // Economic-viability gate. Reject orders whose estimated output is
        // below the route's dust floor — too small to clear route fees, so the
        // depositor would receive dust (or strand funds on a deposit-then-
        // execute route). This replaces the old live route-minimum fan-out: it
        // reuses the quote we already computed instead of a separate
        // exact-out walk across every candidate path. Assets with no defined
        // floor are not gated, matching the previous lenient behaviour.
        if let Some(output_floor) =
            route_output_floor(&self.asset_registry, &normalized_request.destination_asset)
        {
            let estimated_out =
                parse_amount("estimated_amount_out", &provider_quote.estimated_amount_out)?;
            if estimated_out < output_floor {
                return Err(MarketOrderError::OutputBelowFloor {
                    estimated_amount_out: provider_quote.estimated_amount_out.clone(),
                    output_floor: output_floor.to_string(),
                    destination_chain: Box::new(normalized_request.destination_asset.chain.clone()),
                    destination_asset: Box::new(normalized_request.destination_asset.asset.clone()),
                });
            }
        }

        let now = Utc::now();
        let expected_swap_time_ms = self
            .expected_swap_time_ms_for_provider_quote(&provider_quote.provider_quote)
            .await?;
        let mut quote = MarketOrderQuote {
            id: quote_id,
            order_id: None,
            source_asset: normalized_request.source_asset,
            destination_asset: normalized_request.destination_asset,
            // Quotes are recipient-agnostic; the real recipient is supplied at
            // order time. This vestigial column is left NULL.
            recipient_address: None,
            provider_id: provider_quote.provider_id,
            amount_in: provider_quote.amount_in,
            estimated_amount_out: provider_quote.estimated_amount_out,
            provider_quote: provider_quote.provider_quote,
            usd_valuation: empty_usd_valuation(),
            expected_swap_time_ms,
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
        let expected_swap_time_ms = self
            .expected_swap_time_ms_for_provider_quote(&provider_quote.provider_quote)
            .await?;
        let mut quote = LimitOrderQuote {
            id: quote_id,
            order_id: None,
            source_asset: normalized_request.source_asset,
            destination_asset: normalized_request.destination_asset,
            // Quotes are recipient-agnostic; the real recipient is supplied at
            // order time. This vestigial column is left NULL.
            recipient_address: None,
            provider_id: provider_quote.provider_id,
            input_amount: provider_quote.input_amount,
            output_amount: provider_quote.output_amount,
            residual_policy: LimitOrderResidualPolicy::Refund,
            provider_quote: provider_quote.provider_quote,
            usd_valuation: empty_usd_valuation(),
            expected_swap_time_ms,
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
    async fn ensure_paymasters_can_fund_path(
        &self,
        gas_reimbursement_plan: &GasReimbursementPlan,
    ) -> MarketOrderResult<()> {
        let required_by_chain = required_paymaster_native_balances(gas_reimbursement_plan)?;
        for (chain, required_balance) in required_by_chain {
            let backend_chain =
                backend_chain_for_id(&chain).ok_or(MarketOrderError::ChainNotSupported {
                    chain: chain.clone(),
                })?;
            let paymaster_address =
                self.paymasters.address_for(backend_chain).ok_or_else(|| {
                    MarketOrderError::NoRoute {
                        reason: format!("missing paymaster for {}", chain),
                    }
                })?;
            let evm_chain = self.chain_registry.get_evm(&backend_chain).ok_or_else(|| {
                MarketOrderError::NoRoute {
                    reason: format!("no EVM chain implementation is configured for {}", chain),
                }
            })?;
            let balance = evm_chain
                .native_balance(paymaster_address)
                .await
                .map_err(|err| MarketOrderError::NoRoute {
                    reason: format!(
                        "failed to read paymaster balance on {} for {}: {err}",
                        chain, paymaster_address
                    ),
                })?;
            if balance < required_balance {
                return Err(MarketOrderError::NoRoute {
                    reason: format!(
                        "paymaster {} on {} is underfunded: balance {} below required {}",
                        paymaster_address, chain, balance, required_balance
                    ),
                });
            }
        }
        Ok(())
    }

    async fn expected_swap_time_ms_for_provider_quote(
        &self,
        provider_quote: &Value,
    ) -> MarketOrderResult<Option<u64>> {
        let keys = match swap_time_route_keys_from_provider_quote(provider_quote) {
            Ok(keys) => keys,
            Err(err) => {
                warn!(error = %err, "unable to derive swap-time route keys for quote");
                return Ok(None);
            }
        };
        if keys.is_empty() {
            return Ok(None);
        }

        let averages = self
            .db
            .swap_times()
            .get_averages_for_keys(&keys)
            .await
            .map_err(MarketOrderError::database)?;
        let averages_by_key = averages
            .into_iter()
            .map(|average| (average.key, average.avg_duration_ms))
            .collect::<HashMap<_, _>>();

        let mut expected_swap_time_ms = 0u64;
        for key in &keys {
            let Some(avg_duration_ms) = averages_by_key.get(key).copied() else {
                return Ok(None);
            };
            expected_swap_time_ms = match expected_swap_time_ms.checked_add(avg_duration_ms) {
                Some(value) => value,
                None => {
                    warn!("swap-time estimate exceeded u64 range");
                    return Ok(None);
                }
            };
        }

        Ok(Some(expected_swap_time_ms))
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
            RouterOrderStatus::Refunding | RouterOrderStatus::Refunded => Ok(order.clone()),
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
        let (workflow_trace_id, workflow_parent_span_id) = fallback_workflow_trace_ids(order_id);
        let refund_address = self.validate_and_normalize_refund_address(
            &quote.source_asset.chain,
            &request.refund_address,
        )?;
        // Funds-safety invariant: the real recipient comes from the ORDER
        // request, never the (recipient-agnostic) quote.
        let recipient_address = self.validate_and_normalize_recipient_address(
            &quote.destination_asset.chain,
            &request.recipient_address,
        )?;
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status: RouterOrderStatus::Quoted,
            funding_vault_id: None,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address,
            refund_address,
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                amount_in: quote.amount_in.clone(),
            }),
            idempotency_key: normalize_idempotency_key(request.idempotency_key.clone())?,
            workflow_trace_id,
            workflow_parent_span_id,
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
        let (workflow_trace_id, workflow_parent_span_id) = fallback_workflow_trace_ids(order_id);
        let refund_address = self.validate_and_normalize_refund_address(
            &quote.source_asset.chain,
            &request.refund_address,
        )?;
        // Funds-safety invariant: the real recipient comes from the ORDER
        // request, never the (recipient-agnostic) quote.
        let recipient_address = self.validate_and_normalize_recipient_address(
            &quote.destination_asset.chain,
            &request.recipient_address,
        )?;
        let order = RouterOrder {
            id: order_id,
            order_type: RouterOrderType::LimitOrder,
            status: RouterOrderStatus::Quoted,
            funding_vault_id: None,
            source_asset: quote.source_asset.clone(),
            destination_asset: quote.destination_asset.clone(),
            recipient_address,
            refund_address,
            action: RouterOrderAction::LimitOrder(LimitOrderAction {
                input_amount: quote.input_amount.clone(),
                output_amount: quote.output_amount.clone(),
                residual_policy: quote.residual_policy,
            }),
            idempotency_key: normalize_idempotency_key(request.idempotency_key.clone())?,
            workflow_trace_id,
            workflow_parent_span_id,
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
        paths.retain(is_executable_transition_path);
        paths.retain(|path| path_has_configured_provider_set(self.action_providers.as_ref(), path));
        if request.routing.provider_sequence.is_none() {
            prefer_same_chain_evm_paths(request, &mut paths);
        }
        let provider_sequence_paths_filtered = filter_provider_sequence_paths(
            &mut paths,
            request.routing.provider_sequence.as_deref(),
        );
        if paths.is_empty() {
            return Err(MarketOrderError::NoRoute {
                reason: if provider_sequence_paths_filtered {
                    format!(
                        "no executable transition path matches providerSequence {}",
                        format_provider_sequence(
                            request
                                .routing
                                .provider_sequence
                                .as_deref()
                                .expect("provider sequence filter only runs when configured"),
                        )
                    )
                } else {
                    format!(
                        "no executable transition path from {} {} to {} {}",
                        request.source_asset.chain,
                        request.source_asset.asset,
                        request.destination_asset.chain,
                        request.destination_asset.asset
                    )
                },
            });
        }

        let request_usd_micros = self.market_request_usd_micros(request).await;
        let mut ranked = self
            .rank_market_paths(&mut paths, request_usd_micros)
            .await;
        // Truncate both the path list and the ranked summary in lockstep
        // so confidence_band sees the same set we will quote against.
        if ranked.len() > TOP_K_PATHS {
            ranked.truncate(TOP_K_PATHS);
        }
        if paths.len() > TOP_K_PATHS {
            paths.truncate(TOP_K_PATHS);
        }
        let band_count = confidence_band(&ranked, request_usd_micros, TOP_K_PATHS);
        telemetry::record_route_select_band_size(band_count);

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

        // Confidence-band fanout: live-quote `band_count` paths in parallel
        // and pick the highest `estimated_amount_out`. Outside the band we
        // trust the cached/structural leader because we have no evidence
        // any deeper peer will beat it.
        let band_paths: Vec<&TransitionPath> = ranked
            .iter()
            .take(band_count)
            .map(|ranked_path| &ranked_path.path)
            .collect();
        let band_quote_futures = band_paths.iter().map(|path| {
            self.quote_transition_path(
                request,
                quote_id,
                source_depositor_address,
                path,
                provider_policy_snapshot.as_ref(),
                provider_health_snapshot.as_ref(),
            )
        });
        let band_results = join_all(band_quote_futures).await;

        let mut outcomes: Vec<LiveQuoteOutcome> = Vec::with_capacity(band_results.len());
        let mut survivors: Vec<ComposedMarketOrderQuote> = Vec::with_capacity(band_results.len());
        let mut last_error: Option<MarketOrderError> = None;
        for (candidate, path) in band_results.into_iter().zip(band_paths.iter()) {
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
            let estimated_amount_out = U256::from_str(&quote.estimated_amount_out).unwrap_or_default();
            outcomes.push(LiveQuoteOutcome {
                quote_index: survivors.len(),
                path_id: path.id.clone(),
                hop_count: path.transitions.len(),
                estimated_amount_out,
            });
            survivors.push(quote);
        }

        if survivors.is_empty() {
            return match last_error {
                Some(err) => Err(err),
                None => Err(MarketOrderError::NoRoute {
                    reason: "no executable transition path satisfied the requested constraints"
                        .to_string(),
                }),
            };
        }

        let winning_index = select_best_quote(&outcomes).unwrap_or(0);
        let winning_outcome = outcomes[winning_index].clone();
        let chosen = survivors[winning_outcome.quote_index].clone();
        if let Some(leader_outcome) = outcomes.first() {
            if leader_outcome.path_id != winning_outcome.path_id {
                telemetry::record_route_select_best_path_not_leader();
            }
            let leader_out = leader_outcome.estimated_amount_out;
            let chosen_out = winning_outcome.estimated_amount_out;
            let delta_bps = compute_route_select_delta_bps(leader_out, chosen_out);
            telemetry::record_route_select_best_output_delta_bps(delta_bps);
            info!(
                quote_id = %quote_id,
                request_usd_micros,
                band_size = band_count,
                ranked_count = ranked.len(),
                leader_path_id = %leader_outcome.path_id,
                chosen_path_id = %winning_outcome.path_id,
                delta_bps,
                "best_provider_quote selected"
            );
        }
        Ok(chosen)
    }

    /// Dry-run explainer used by the admin dashboard route-graph visualizer.
    /// Mirrors the staged pipeline of [`Self::best_provider_quote`] (BFS ->
    /// filters -> rank) but persists nothing and is instrumented with per-stage
    /// counts and timings. When `live_quote` is set it additionally live-quotes
    /// the hardcoded top-N ranked paths to report real per-path outputs and the
    /// winning path. Cache-only by default.
    pub async fn explain_market_order_route(
        &self,
        request: crate::api::RouteExplainRequest,
    ) -> MarketOrderResult<crate::api::RouteExplain> {
        use crate::api::{
            RankedPathView, RouteExplain, RouteExplainCounts, RouteExplainTimings,
            RouteTransitionView,
        };

        const MAX_PATH_DEPTH: usize = 5;
        const TOP_K_PATHS: usize = 8;
        // Live quoting (when requested) always targets the top-N ranked paths.
        const LIVE_QUOTE_TOP_N: usize = 3;

        let total_started = Instant::now();
        validate_positive_amount("amount_in", &request.amount_in)?;
        let source_asset = self.validate_and_normalize_asset(&request.from_asset)?;
        let destination_asset = self.validate_and_normalize_asset(&request.to_asset)?;

        let bfs_started = Instant::now();
        let mut paths = self.asset_registry.select_transition_paths(
            &source_asset,
            &destination_asset,
            MAX_PATH_DEPTH,
        );
        let paths_enumerated = paths.len();
        let bfs_ms = bfs_started.elapsed().as_millis() as u64;

        // Apply the same retain filters as `best_provider_quote`, capturing the
        // surviving count after each stage so the UI can show how the candidate
        // set narrows.
        let mut normalized_request = NormalizedMarketOrderQuoteRequest {
            source_asset: source_asset.clone(),
            destination_asset: destination_asset.clone(),
            amount_in: request.amount_in.clone(),
            routing: NormalizedMarketOrderRouting {
                provider_sequence: None,
            },
        };

        paths.retain(|path| is_executable_transition_path(path));
        let paths_after_executable = paths.len();
        paths.retain(|path| path_has_configured_provider_set(self.action_providers.as_ref(), path));
        let paths_after_provider = paths.len();
        self.filter_unpriceable_hyperevm_paths(&mut paths).await;
        let paths_after_hyperevm = paths.len();
        prefer_same_chain_evm_paths(&normalized_request, &mut paths);
        let paths_after_same_chain = paths.len();

        let request_usd_micros = self
            .request_usd_micros(&source_asset, &request.amount_in)
            .await;
        let tier_label =
            router_core::services::route_costs::select_route_cost_tier(request_usd_micros)
                .label
                .to_string();

        let rank_started = Instant::now();
        let mut ranked = self.rank_market_paths(&mut paths, request_usd_micros).await;
        if ranked.len() > TOP_K_PATHS {
            ranked.truncate(TOP_K_PATHS);
        }
        if paths.len() > TOP_K_PATHS {
            paths.truncate(TOP_K_PATHS);
        }
        let rank_ms = rank_started.elapsed().as_millis() as u64;
        let ranked_count = ranked.len();
        // Top-N ranked paths are the live-quote set (hardcoded), replacing the
        // old variable-width confidence band.
        let top_paths = ranked_count.min(LIVE_QUOTE_TOP_N);

        // Source->destination corridor for the visualizer: the full declared
        // topology (incl. curated same-chain Velora swaps) restricted to the
        // nodes between source and destination. The ranked routes above are
        // overlaid as highlights by the dashboard; this is purely what we draw.
        let graph = self.build_explain_corridor(&source_asset, &destination_asset);

        // Optional live-quote stage: fan out real quotes for the top-N paths.
        // Failures here are non-fatal for the explainer (we still return the
        // ranked dry-run view).
        let mut estimated_out_by_path: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut winner_path_id: Option<String> = None;
        let mut live_quote_ms = 0_u64;
        if request.live_quote && top_paths > 0 {
            let live_started = Instant::now();
            match self
                .live_quote_top_paths(&mut normalized_request, &ranked, top_paths)
                .await
            {
                Ok((outputs, winner)) => {
                    estimated_out_by_path = outputs;
                    winner_path_id = winner;
                }
                Err(err) => {
                    warn!(error = %err, "route-explain live quote failed; returning dry-run ranking");
                }
            }
            live_quote_ms = live_started.elapsed().as_millis() as u64;
        }

        let ranked_views = ranked
            .iter()
            .enumerate()
            .map(|(index, ranked_path)| {
                let path = &ranked_path.path;
                let transitions = path
                    .transitions
                    .iter()
                    .map(|transition| RouteTransitionView {
                        id: transition.id.clone(),
                        provider: transition.provider.as_str().to_string(),
                        kind: transition.kind.as_str().to_string(),
                        edge_kind: transition.route_edge_kind().as_str().to_string(),
                        from_chain: transition.input.asset.chain.as_str().to_string(),
                        from_asset: transition.input.asset.asset.as_str().to_string(),
                        to_chain: transition.output.asset.chain.as_str().to_string(),
                        to_asset: transition.output.asset.asset.as_str().to_string(),
                    })
                    .collect();
                RankedPathView {
                    rank: index + 1,
                    path_id: path.id.clone(),
                    top_path: index < top_paths,
                    winner: winner_path_id.as_deref() == Some(path.id.as_str()),
                    hop_count: path.transitions.len(),
                    missing_edges: ranked_path.score.missing_edges,
                    total_effective_cost_usd_micros: ranked_path
                        .score
                        .total_effective_cost_usd_micros,
                    total_latency_ms: ranked_path.score.total_latency_ms,
                    transitions,
                    estimated_amount_out: estimated_out_by_path.get(&path.id).cloned(),
                }
            })
            .collect();

        Ok(RouteExplain {
            from_asset: source_asset,
            to_asset: destination_asset,
            amount_in: request.amount_in,
            request_usd_micros,
            tier_label,
            live_quote: request.live_quote,
            counts: RouteExplainCounts {
                paths_enumerated,
                paths_after_executable,
                paths_after_provider,
                paths_after_hyperevm,
                paths_after_same_chain,
                ranked_count,
                top_paths,
            },
            timings: RouteExplainTimings {
                bfs_ms,
                rank_ms,
                live_quote_ms,
                total_ms: total_started.elapsed().as_millis() as u64,
            },
            ranked: ranked_views,
            winner_path_id,
            graph,
        })
    }

    /// Build the source->destination *corridor*: the full declared display
    /// topology (every bridge/Unit/HyperCore edge plus the curated same-chain
    /// Velora swaps) restricted to nodes that are both forward-reachable from
    /// the source and backward-reachable to the destination. Nodes are laid out
    /// by hop depth from the source (destination pinned to `max_depth`). This is
    /// purely the visual topology - the engine's actual ranked routes are
    /// overlaid as highlights by the dashboard via the edge ids. Independent of
    /// `select_transition_paths`, so it includes same-chain swap branches the
    /// router never traverses as intermediate hops.
    fn build_explain_corridor(
        &self,
        source: &DepositAsset,
        destination: &DepositAsset,
    ) -> crate::api::RouteExplainGraph {
        use crate::api::{RouteExplainGraph, RouteExplainGraphEdge, RouteExplainGraphNode};
        use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

        let source_key = format!("external:{}:{}", source.chain.as_str(), source.asset.as_str());
        let dest_key = format!(
            "external:{}:{}",
            destination.chain.as_str(),
            destination.asset.as_str()
        );

        // Full display edge universe keyed by node, in both directions.
        let declarations = self.asset_registry.display_transition_declarations();
        let mut forward: HashMap<String, Vec<String>> = HashMap::new();
        let mut backward: HashMap<String, Vec<String>> = HashMap::new();
        let mut node_meta: HashMap<String, RouteExplainGraphNode> = HashMap::new();
        for decl in &declarations {
            let (from_key, from_node) = self.explain_graph_node(&decl.from);
            let (to_key, to_node) = self.explain_graph_node(&decl.to);
            forward.entry(from_key.clone()).or_default().push(to_key.clone());
            backward.entry(to_key.clone()).or_default().push(from_key.clone());
            node_meta.entry(from_key).or_insert(from_node);
            node_meta.entry(to_key).or_insert(to_node);
        }

        // Corridor = forward-reachable(source) INTERSECT backward-reachable(dest).
        let reachable_from_source = reachable_set(&forward, &source_key);
        let reaches_dest = reachable_set(&backward, &dest_key);
        let mut corridor: HashSet<String> = reachable_from_source
            .intersection(&reaches_dest)
            .cloned()
            .collect();
        corridor.insert(source_key.clone());
        corridor.insert(dest_key.clone());

        // Min hop-depth from the source within the corridor (BFS over corridor
        // forward edges).
        let mut depth: HashMap<String, usize> = HashMap::new();
        if corridor.contains(&source_key) {
            depth.insert(source_key.clone(), 0);
            let mut queue = VecDeque::from([source_key.clone()]);
            while let Some(current) = queue.pop_front() {
                let current_depth = depth[&current];
                if let Some(neighbours) = forward.get(&current) {
                    for neighbour in neighbours {
                        if corridor.contains(neighbour) && !depth.contains_key(neighbour) {
                            depth.insert(neighbour.clone(), current_depth + 1);
                            queue.push_back(neighbour.clone());
                        }
                    }
                }
            }
        }
        let max_depth = depth.values().copied().max().unwrap_or(0);

        // Emit corridor nodes (sorted for determinism) with depth + role.
        let mut nodes: Vec<RouteExplainGraphNode> = BTreeMap::from_iter(
            corridor
                .iter()
                .filter_map(|key| node_meta.get(key).map(|node| (key.clone(), node.clone()))),
        )
        .into_values()
        .map(|mut node| {
            node.depth = depth.get(&node.key).copied().unwrap_or(max_depth);
            if node.key == source_key {
                node.role = "source".to_string();
                node.depth = 0;
            } else if node.key == dest_key {
                node.role = "destination".to_string();
                node.depth = max_depth;
            }
            node
        })
        .collect();
        nodes.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.key.cmp(&b.key)));

        // Corridor edges: declared edges whose endpoints are both in the corridor.
        let mut seen_edges: HashSet<String> = HashSet::new();
        let mut edges: Vec<RouteExplainGraphEdge> = Vec::new();
        for decl in &declarations {
            let (from_key, _) = self.explain_graph_node(&decl.from);
            let (to_key, _) = self.explain_graph_node(&decl.to);
            if !corridor.contains(&from_key) || !corridor.contains(&to_key) {
                continue;
            }
            if !seen_edges.insert(decl.id.clone()) {
                continue;
            }
            edges.push(RouteExplainGraphEdge {
                id: decl.id.clone(),
                from: from_key,
                to: to_key,
                provider: decl.provider.as_str().to_string(),
                kind: decl.kind.as_str().to_string(),
                edge_kind: decl.route_edge_kind().as_str().to_string(),
            });
        }

        RouteExplainGraph {
            nodes,
            edges,
            max_depth,
        }
    }

    /// Resolve a routing-graph node into its stable key + display metadata,
    /// mirroring the keying used by the static `/route-graph` export so edge
    /// `from`/`to` references line up across both endpoints.
    fn explain_graph_node(
        &self,
        node: &router_core::services::asset_registry::MarketOrderNode,
    ) -> (String, crate::api::RouteExplainGraphNode) {
        use crate::api::RouteExplainGraphNode;
        use router_core::services::asset_registry::MarketOrderNode;

        match node {
            MarketOrderNode::External(asset) => {
                let key = format!("external:{}:{}", asset.chain.as_str(), asset.asset.as_str());
                let canonical = self
                    .asset_registry
                    .canonical_for(asset)
                    .map(|canonical| canonical.as_str().to_string())
                    .unwrap_or_default();
                (
                    key.clone(),
                    RouteExplainGraphNode {
                        key,
                        kind: "external".to_string(),
                        chain: asset.chain.as_str().to_string(),
                        asset: asset.asset.as_str().to_string(),
                        canonical,
                        depth: 0,
                        role: "intermediate".to_string(),
                    },
                )
            }
            MarketOrderNode::Venue {
                provider,
                canonical,
            } => {
                let key = format!("venue:{}:{}", provider.as_str(), canonical.as_str());
                (
                    key.clone(),
                    RouteExplainGraphNode {
                        key,
                        kind: "venue".to_string(),
                        chain: format!("venue:{}", provider.as_str()),
                        asset: canonical.as_str().to_string(),
                        canonical: canonical.as_str().to_string(),
                        depth: 0,
                        role: "intermediate".to_string(),
                    },
                )
            }
        }
    }

    /// Live-quote the top `top_count` ranked paths (mirroring the
    /// `best_provider_quote` fan-out) and return a map of `path_id ->
    /// estimated_amount_out` plus the winning path id. Used only by the
    /// route-explain dry run; it derives throwaway deposit/recipient addresses
    /// so it never touches persisted order state.
    async fn live_quote_top_paths(
        &self,
        request: &mut NormalizedMarketOrderQuoteRequest,
        ranked: &[RankedTransitionPath],
        top_count: usize,
    ) -> MarketOrderResult<(std::collections::HashMap<String, String>, Option<String>)> {
        let quote_id = Uuid::now_v7();
        let (source_depositor_address, _) = derive_deposit_address_for_quote(
            self.chain_registry.as_ref(),
            &self.settings.master_key_bytes(),
            quote_id,
            &request.source_asset.chain,
        )
        .map_err(MarketOrderError::deposit_address)?;

        let provider_policy_snapshot = if let Some(service) = self.provider_policies.as_ref() {
            Some(service.snapshot().await.map_err(MarketOrderError::database)?)
        } else {
            None
        };
        let provider_health_snapshot = if let Some(service) = self.provider_health.as_ref() {
            Some(service.snapshot().await.map_err(MarketOrderError::database)?)
        } else {
            None
        };

        let top_paths: Vec<&TransitionPath> = ranked
            .iter()
            .take(top_count)
            .map(|ranked_path| &ranked_path.path)
            .collect();
        let top_quote_futures = top_paths.iter().map(|path| {
            self.quote_transition_path(
                request,
                quote_id,
                &source_depositor_address,
                path,
                provider_policy_snapshot.as_ref(),
                provider_health_snapshot.as_ref(),
            )
        });
        let top_results = join_all(top_quote_futures).await;

        let mut outputs: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut outcomes: Vec<LiveQuoteOutcome> = Vec::new();
        for (candidate, path) in top_results.into_iter().zip(top_paths.iter()) {
            let quote = match candidate {
                Ok(Some(quote)) => quote,
                Ok(None) => continue,
                Err(err) => {
                    warn!(error = %err, path_id = %path.id, "route-explain top-path leg failed");
                    continue;
                }
            };
            if self.validate_provider_quote(request, &quote).is_err() {
                continue;
            }
            let estimated_amount_out =
                U256::from_str(&quote.estimated_amount_out).unwrap_or_default();
            outputs.insert(path.id.clone(), quote.estimated_amount_out.clone());
            outcomes.push(LiveQuoteOutcome {
                quote_index: outcomes.len(),
                path_id: path.id.clone(),
                hop_count: path.transitions.len(),
                estimated_amount_out,
            });
        }

        let winner_path_id =
            select_best_quote(&outcomes).map(|index| outcomes[index].path_id.clone());
        Ok((outputs, winner_path_id))
    }

    /// Best-effort conversion of `request.amount_in` to USD micros for
    /// amount-aware ranking. Falls back to `DEFAULT_SAMPLE_AMOUNT_USD_MICROS`
    /// when the source asset has no pricing row, so ranking degrades to
    /// today's behaviour rather than failing the request.
    async fn market_request_usd_micros(
        &self,
        request: &NormalizedMarketOrderQuoteRequest,
    ) -> u64 {
        self.request_usd_micros(&request.source_asset, &request.amount_in).await
    }

    async fn request_usd_micros(&self, source_asset: &DepositAsset, amount_in: &str) -> u64 {
        let Some(chain_asset) = self.asset_registry.chain_asset(source_asset) else {
            return DEFAULT_SAMPLE_AMOUNT_USD_MICROS;
        };
        let Ok(raw_amount) = U256::from_str(amount_in) else {
            return DEFAULT_SAMPLE_AMOUNT_USD_MICROS;
        };
        let pricing = if let Some(route_costs) = self.route_costs.as_ref() {
            route_costs.current_or_refresh_pricing_snapshot().await
        } else {
            router_core::services::pricing::PricingSnapshot::static_bootstrap(Utc::now())
        };
        match raw_amount_to_usd_micros(raw_amount, chain_asset, &pricing) {
            Some(value) if value > 0 => value,
            _ => DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
        }
    }

    /// Rank `paths` against `request_usd_micros` using the route-cost
    /// service when available, falling back to the structural sort
    /// otherwise. The returned `RankedTransitionPath` list is aligned with
    /// the sorted `paths` and includes per-path scores for `confidence_band`.
    async fn rank_market_paths(
        &self,
        paths: &mut Vec<TransitionPath>,
        request_usd_micros: u64,
    ) -> Vec<RankedTransitionPath> {
        if let Some(route_costs) = self.route_costs.as_ref() {
            match route_costs
                .rank_transition_paths_for_request(paths, request_usd_micros)
                .await
            {
                Ok(ranked) => return ranked,
                Err(err) => {
                    warn!(
                        error = %err,
                        "route-cost ranking failed; falling back to structural route ordering"
                    );
                }
            }
        }
        rank_transition_paths_structurally(paths, request_usd_micros);
        let pricing = router_core::services::pricing::PricingSnapshot::static_bootstrap(Utc::now());
        paths
            .iter()
            .map(|path| RankedTransitionPath {
                path: path.clone(),
                score: router_core::services::route_costs::structural_path_score(
                    path,
                    &pricing,
                    request_usd_micros,
                ),
            })
            .collect()
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
        // V1 limit orders intentionally do not invest in amount-aware
        // ranking (audit S1.10): no request-size math, no confidence-band
        // fanout, no best-output selection. Pinning to the explicit
        // `DEFAULT_SAMPLE_AMOUNT_USD_MICROS` anchor keeps limit's structural
        // ordering identical to its pre-deprecation behaviour while still
        // funnelling through the tier-aware ranking surface.
        if let Some(route_costs) = self.route_costs.as_ref() {
            if let Err(err) = route_costs
                .rank_transition_paths_for_request(&mut paths, DEFAULT_SAMPLE_AMOUNT_USD_MICROS)
                .await
            {
                warn!(
                    error = %err,
                    "limit-order route-cost ranking failed; falling back to structural ordering"
                );
                rank_transition_paths_structurally(&mut paths, DEFAULT_SAMPLE_AMOUNT_USD_MICROS);
            }
        } else {
            rank_transition_paths_structurally(&mut paths, DEFAULT_SAMPLE_AMOUNT_USD_MICROS);
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
                exchange.id() == ProviderId::HyperliquidSpot.as_str()
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
                reason: "hyperliquid_spot exchange provider is not configured for limit orders"
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
                amount_in: String::new(),
                routing: NormalizedMarketOrderRouting::default(),
            };
            let suffix_order_kind = ProviderOrderKind::ExactOut {
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
                amount_in: String::new(),
                routing: NormalizedMarketOrderRouting::default(),
            };
            let prefix_order_kind = ProviderOrderKind::ExactOut {
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
            let order_kind = ProviderOrderKind::ExactIn {
                amount_in: request.amount_in.clone(),
                min_amount_out: Some("1".to_string()),
            };
            let quote = self
                .compose_transition_path_quote(ComposeTransitionPathQuoteRequest {
                    request,
                    order_kind: &order_kind,
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
        self.ensure_paymasters_can_fund_path(&gas_reimbursement_plan)
            .await?;

        match order_kind {
            ProviderOrderKind::ExactIn {
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
                                    order_kind: ProviderOrderKind::ExactIn {
                                        amount_in: provider_amount_in.clone(),
                                        min_amount_out: Some("1".to_string()),
                                    },
                                    recipient_address: self
                                        .bridge_quote_recipient_address(quote_id, transition)?,
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
                                    order_kind: ProviderOrderKind::ExactIn {
                                        amount_in: quote_amount_in,
                                        min_amount_out: transition_min_amount_out(
                                            path,
                                            index,
                                            min_amount_out,
                                        ),
                                    },
                                    sender_address: None,
                                    recipient_address: self
                                        .exchange_quote_recipient_address(quote_id, transition)?,
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
                                    order_kind: ProviderOrderKind::ExactIn {
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
                                    recipient_address: self
                                        .exchange_quote_recipient_address(quote_id, transition)?,
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
                            let recipient_address =
                                self.unit_withdrawal_recipient_address(quote_id, transition)?;
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
                    estimated_amount_out: cursor_amount,
                    provider_quote: transition_path_quote_blob(
                        path,
                        &legs,
                        &gas_reimbursement_plan,
                    ),
                    expires_at,
                }))
            }
            ProviderOrderKind::ExactOut {
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
                            let recipient_address =
                                self.unit_withdrawal_recipient_address(quote_id, transition)?;
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
                                    order_kind: ProviderOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_in_cap,
                                    },
                                    sender_address: None,
                                    recipient_address: self
                                        .exchange_quote_recipient_address(quote_id, transition)?,
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
                                    order_kind: ProviderOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_in_cap,
                                    },
                                    sender_address: Some(self.exchange_quote_sender_address(
                                        request,
                                        quote_id,
                                        source_depositor_address,
                                        transition,
                                    )?),
                                    recipient_address: self
                                        .exchange_quote_recipient_address(quote_id, transition)?,
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
                                    order_kind: ProviderOrderKind::ExactOut {
                                        amount_out: required_output.clone(),
                                        max_amount_in: max_input,
                                    },
                                    recipient_address: self
                                        .bridge_quote_recipient_address(quote_id, transition)?,
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
                    estimated_amount_out: amount_out.clone(),
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
        quote_id: Uuid,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        // Quotes are recipient-agnostic: even the final leg targets an
        // ephemeral per-quote address. The real recipient is applied at order
        // time via `order.recipient_address`, not the quote.
        self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
    }

    fn unit_withdrawal_recipient_address(
        &self,
        quote_id: Uuid,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        // See `bridge_quote_recipient_address`: always an ephemeral per-quote
        // address, including the final leg.
        self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
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
        quote_id: Uuid,
        transition: &TransitionDecl,
    ) -> MarketOrderResult<String> {
        // See `bridge_quote_recipient_address`: always an ephemeral per-quote
        // address, including the final leg.
        self.quote_address_for_chain(quote_id, &transition.output.asset.chain)
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
        quote_address_for_chain(
            self.chain_registry.as_ref(),
            self.settings.as_ref(),
            quote_id,
            chain_id,
        )
    }

    fn validate_and_normalize_quote_request(
        &self,
        request: MarketOrderQuoteRequest,
        routing: QuoteRoutingRequest,
    ) -> MarketOrderResult<NormalizedMarketOrderQuoteRequest> {
        validate_positive_amount("amount_in", &request.amount_in)?;
        let source_asset = self.validate_and_normalize_asset(&request.from_asset)?;
        let destination_asset = self.validate_and_normalize_destination_asset(&request.to_asset)?;
        // Quotes are recipient-agnostic: the recipient is validated/normalized
        // on the order path, not here.
        let routing = validate_and_normalize_quote_routing(routing)?;
        Ok(NormalizedMarketOrderQuoteRequest {
            source_asset,
            destination_asset,
            amount_in: request.amount_in,
            routing,
        })
    }

    fn validate_and_normalize_limit_order_request(
        &self,
        request: LimitOrderQuoteRequest,
    ) -> MarketOrderResult<NormalizedLimitOrderQuoteRequest> {
        validate_positive_amount("input_amount", &request.input_amount)?;
        validate_positive_amount("output_amount", &request.output_amount)?;
        let source_asset = self.validate_and_normalize_asset(&request.from_asset)?;
        let destination_asset = self.validate_and_normalize_destination_asset(&request.to_asset)?;
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
        // Quotes are recipient-agnostic: the recipient is validated/normalized
        // on the order path, not here.
        Ok(NormalizedLimitOrderQuoteRequest {
            source_asset,
            destination_asset,
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
            ChainType::Hyperliquid => validate_hyperliquid_spot_asset(asset).map_err(|reason| {
                MarketOrderError::InvalidAssetId {
                    asset: asset.asset.as_str().to_string(),
                    chain: asset.chain.clone(),
                    reason,
                }
            }),
        }
    }
    fn validate_and_normalize_destination_asset(
        &self,
        asset: &DepositAsset,
    ) -> MarketOrderResult<DepositAsset> {
        self.validate_and_normalize_asset(asset)
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
        validate_positive_amount("estimated_amount_out", &quote.estimated_amount_out)?;
        if quote.expires_at <= Utc::now() {
            return Err(MarketOrderError::NoRoute {
                reason: "provider returned an expired quote".to_string(),
            });
        }

        if quote.amount_in != request.amount_in {
            return Err(MarketOrderError::NoRoute {
                reason: "provider exact-in quote changed amount_in".to_string(),
            });
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

/// BFS reachability over an adjacency map, returning every node reachable from
/// `start` (inclusive). Used to compute the source->destination corridor.
fn reachable_set(
    adjacency: &std::collections::HashMap<String, Vec<String>>,
    start: &str,
) -> std::collections::HashSet<String> {
    use std::collections::{HashSet, VecDeque};
    let mut visited: HashSet<String> = HashSet::new();
    visited.insert(start.to_string());
    let mut queue = VecDeque::from([start.to_string()]);
    while let Some(current) = queue.pop_front() {
        if let Some(neighbours) = adjacency.get(&current) {
            for neighbour in neighbours {
                if visited.insert(neighbour.clone()) {
                    queue.push_back(neighbour.clone());
                }
            }
        }
    }
    visited
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

/// Returns `(chosen_out - leader_out) / leader_out` in bps, clamped to
/// `i64`. Positive means fanout chose a better-output peer than the cached
/// leader predicted; zero means the leader won; negative is unreachable
/// today since `select_best_quote` never picks a smaller output, but the
/// function tolerates it for telemetry hygiene.
fn compute_route_select_delta_bps(leader_out: U256, chosen_out: U256) -> i64 {
    if leader_out.is_zero() {
        return 0;
    }
    let (diff, sign) = if chosen_out >= leader_out {
        (chosen_out - leader_out, 1_i64)
    } else {
        (leader_out - chosen_out, -1_i64)
    };
    let numerator = alloy::primitives::U512::from(diff)
        * alloy::primitives::U512::from(router_core::services::pricing::BPS_DENOMINATOR);
    let denom = alloy::primitives::U512::from(leader_out);
    if denom.is_zero() {
        return 0;
    }
    let magnitude = (numerator / denom)
        .to_string()
        .parse::<i64>()
        .unwrap_or(i64::MAX);
    sign.saturating_mul(magnitude)
}

fn validate_hyperliquid_spot_asset(asset: &DepositAsset) -> Result<DepositAsset, String> {
    if asset.chain.as_str() != "hyperliquid" {
        return Err("expected hyperliquid chain".to_string());
    }
    match asset.asset.as_str() {
        "native" | "UBTC" | "USDT" | "UETH" | "HYPE" => Ok(asset.clone()),
        other => Err(format!(
            "hyperliquid spot supports only UBTC, USDT, USDC, UETH, and HYPE, not {other}"
        )),
    }
}

fn is_executable_transition_path(path: &TransitionPath) -> bool {
    // `select_transition_paths` only returns paths that already reach the
    // requested destination node. The terminal provider does not need a second
    // allow-list here: a bridge exit to a registered destination asset is just
    // as executable as a Velora or Unit exit. Provider availability and per-leg
    // quote support are checked in the following filters/quote pass.
    !path.transitions.is_empty()
}

fn quote_address_for_chain(
    chain_registry: &ChainRegistry,
    settings: &Settings,
    quote_id: Uuid,
    chain_id: &ChainId,
) -> MarketOrderResult<String> {
    derive_deposit_address_for_quote(
        chain_registry,
        &settings.master_key_bytes(),
        quote_id,
        chain_id,
    )
    .map(|(address, _)| address)
    .map_err(MarketOrderError::deposit_address)
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

fn filter_provider_sequence_paths(
    paths: &mut Vec<TransitionPath>,
    provider_sequence: Option<&[ProviderId]>,
) -> bool {
    let Some(provider_sequence) = provider_sequence else {
        return false;
    };
    let before = paths.len();
    paths.retain(|path| transition_path_matches_provider_sequence(path, provider_sequence));
    before != paths.len()
}

fn transition_path_matches_provider_sequence(
    path: &TransitionPath,
    provider_sequence: &[ProviderId],
) -> bool {
    path.transitions.len() == provider_sequence.len()
        && path
            .transitions
            .iter()
            .zip(provider_sequence.iter())
            .all(|(transition, provider)| transition.provider == *provider)
}

fn format_provider_sequence(provider_sequence: &[ProviderId]) -> String {
    let mut output = String::new();
    for provider in provider_sequence {
        if !output.is_empty() {
            output.push(',');
        }
        output.push_str(provider.as_str());
    }
    output
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

fn path_uses_hyperevm(path: &TransitionPath) -> bool {
    path.transitions.iter().any(|transition| {
        transition.input.asset.chain.as_str() == "evm:999"
            || transition.output.asset.chain.as_str() == "evm:999"
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
        "spot_transfer" => {
            let leg = quote
                .provider_quote
                .get("leg")
                .ok_or_else(|| MarketOrderError::NoRoute {
                    reason: "hyperliquid spot_transfer quote missing leg".to_string(),
                })?;
            let input_asset = leg
                .get("input_asset")
                .ok_or_else(|| MarketOrderError::NoRoute {
                    reason: "hyperliquid spot_transfer leg missing input_asset".to_string(),
                })
                .and_then(|value| {
                    QuoteLegAsset::from_value(value, "input_asset")
                        .map_err(|reason| MarketOrderError::NoRoute { reason })
                })?;
            let output_asset = leg
                .get("output_asset")
                .ok_or_else(|| MarketOrderError::NoRoute {
                    reason: "hyperliquid spot_transfer leg missing output_asset".to_string(),
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
                amount_in: required_quote_leg_amount(leg, "amount_in")?,
                amount_out: required_quote_leg_amount(leg, "amount_out")?,
                expires_at: quote.expires_at,
                raw: leg.clone(),
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

fn required_paymaster_native_balances(
    gas_reimbursement_plan: &GasReimbursementPlan,
) -> MarketOrderResult<HashMap<ChainId, U256>> {
    let mut required_by_chain = HashMap::<ChainId, U256>::new();
    for debt in &gas_reimbursement_plan.debts {
        let chain =
            ChainId::parse(&debt.spend_chain_id).map_err(|reason| MarketOrderError::NoRoute {
                reason: format!(
                    "paymaster reimbursement plan has invalid spend chain {}: {reason}",
                    debt.spend_chain_id
                ),
            })?;
        let required = parse_amount("estimated_native_gas_wei", &debt.estimated_native_gas_wei)?;
        let entry = required_by_chain.entry(chain).or_insert(U256::ZERO);
        *entry = entry.checked_add(required).ok_or_else(|| {
            MarketOrderError::gas_reimbursement(GasReimbursementError::NumericOverflow {
                context: "paymaster quote native gas aggregation",
            })
        })?;
    }
    for required in required_by_chain.values_mut() {
        *required = apply_bps_multiplier(*required, PAYMASTER_QUOTE_BALANCE_SAFETY_MULTIPLIER_BPS)
            .ok_or_else(|| {
                MarketOrderError::gas_reimbursement(GasReimbursementError::NumericOverflow {
                    context: "paymaster quote native gas safety multiplier",
                })
            })?;
    }
    Ok(required_by_chain)
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

fn swap_time_route_keys_from_provider_quote(
    provider_quote: &Value,
) -> MarketOrderResult<Vec<SwapTimeRouteKey>> {
    let transitions = provider_quote
        .get("transitions")
        .ok_or_else(|| MarketOrderError::NoRoute {
            reason: "quoted route is missing provider_quote.transitions".to_string(),
        })?
        .as_array()
        .ok_or_else(|| MarketOrderError::NoRoute {
            reason: "quoted route provider_quote.transitions must be an array".to_string(),
        })?
        .iter()
        .map(|value| {
            serde_json::from_value::<TransitionDecl>(value.clone()).map_err(|err| {
                MarketOrderError::NoRoute {
                    reason: format!("quoted transition is invalid: {err}"),
                }
            })
        })
        .collect::<MarketOrderResult<Vec<_>>>()?;

    let mut legs_by_transition_id: HashMap<String, Vec<QuoteLeg>> = HashMap::new();
    for leg in quote_legs_from_provider_quote(provider_quote)? {
        legs_by_transition_id
            .entry(leg.parent_transition_id().to_string())
            .or_default()
            .push(leg);
    }

    let mut keys = Vec::with_capacity(transitions.len());
    for transition in transitions {
        let legs = legs_by_transition_id
            .remove(&transition.id)
            .ok_or_else(|| MarketOrderError::NoRoute {
                reason: format!(
                    "quoted transition {} ({}) has no legs for swap-time estimate",
                    transition.id,
                    transition.kind.as_str()
                ),
            })?;
        keys.push(swap_time_route_key_from_transition_legs(
            &transition,
            &legs,
        )?);
    }

    if !legs_by_transition_id.is_empty() {
        let mut unconsumed = legs_by_transition_id.into_keys().collect::<Vec<_>>();
        unconsumed.sort();
        return Err(MarketOrderError::NoRoute {
            reason: format!(
                "quoted route has unconsumed legs for swap-time estimate: {}",
                unconsumed.join(", ")
            ),
        });
    }

    Ok(keys)
}

fn swap_time_route_key_from_transition_legs(
    transition: &TransitionDecl,
    legs: &[QuoteLeg],
) -> MarketOrderResult<SwapTimeRouteKey> {
    let first = legs.first().ok_or_else(|| MarketOrderError::NoRoute {
        reason: format!(
            "quoted transition {} ({}) has no legs for swap-time estimate",
            transition.id,
            transition.kind.as_str()
        ),
    })?;
    let last = legs.last().ok_or_else(|| MarketOrderError::NoRoute {
        reason: format!(
            "quoted transition {} ({}) has no legs for swap-time estimate",
            transition.id,
            transition.kind.as_str()
        ),
    })?;
    let input_asset = first
        .input_deposit_asset()
        .map_err(|reason| MarketOrderError::NoRoute {
            reason: format!(
                "quoted transition {} ({}) has invalid swap-time input asset: {reason}",
                transition.id,
                transition.kind.as_str()
            ),
        })?;
    let output_asset = last
        .output_deposit_asset()
        .map_err(|reason| MarketOrderError::NoRoute {
            reason: format!(
                "quoted transition {} ({}) has invalid swap-time output asset: {reason}",
                transition.id,
                transition.kind.as_str()
            ),
        })?;
    let provider = legs
        .iter()
        .map(|leg| leg.provider.as_str())
        .reduce(|first, next| if first == next { first } else { "multi" })
        .unwrap_or("unknown")
        .to_string();

    Ok(SwapTimeRouteKey {
        provider,
        leg_type: transition.kind.as_str().to_string(),
        transition_decl_id: transition.id.clone(),
        input_chain_id: input_asset.chain.as_str().to_string(),
        input_asset_id: input_asset.asset.as_str().to_string(),
        output_chain_id: output_asset.chain.as_str().to_string(),
        output_asset_id: output_asset.asset.as_str().to_string(),
    })
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
                provider: ProviderId::HyperliquidSpot,
                canonical: source_canonical,
            })
            || transition.to
                != (MarketOrderNode::Venue {
                    provider: ProviderId::HyperliquidSpot,
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
    _request: &NormalizedMarketOrderQuoteRequest,
    candidate: ComposedMarketOrderQuote,
    current: Option<ComposedMarketOrderQuote>,
) -> Option<ComposedMarketOrderQuote> {
    if is_better_quote(&candidate, &current) {
        Some(candidate)
    } else {
        current
    }
}

#[derive(Debug, Clone, Default)]
struct NormalizedMarketOrderRouting {
    provider_sequence: Option<Vec<ProviderId>>,
}

#[derive(Debug, Clone)]
struct NormalizedMarketOrderQuoteRequest {
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    amount_in: String,
    routing: NormalizedMarketOrderRouting,
}

#[derive(Debug, Clone)]
struct NormalizedLimitOrderQuoteRequest {
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    input_amount: String,
    output_amount: String,
}

fn validate_and_normalize_quote_routing(
    routing: QuoteRoutingRequest,
) -> MarketOrderResult<NormalizedMarketOrderRouting> {
    let Some(provider_sequence) = routing.provider_sequence else {
        return Ok(NormalizedMarketOrderRouting::default());
    };
    if provider_sequence.is_empty() {
        return Err(MarketOrderError::InvalidRouting {
            reason: "providerSequence must contain at least one provider".to_string(),
        });
    }
    if provider_sequence.len() > MAX_PROVIDER_SEQUENCE_LEN {
        return Err(MarketOrderError::InvalidRouting {
            reason: format!(
                "providerSequence cannot contain more than {MAX_PROVIDER_SEQUENCE_LEN} providers"
            ),
        });
    }
    Ok(NormalizedMarketOrderRouting {
        provider_sequence: Some(provider_sequence),
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

/// Dust floors per canonical destination asset — the smallest output below
/// which a market order isn't economically viable (the fees would eat it).
const ROUTE_OUTPUT_FLOOR_BTC_SATS: u64 = 30_000;
const ROUTE_OUTPUT_FLOOR_ETH_WEI: u128 = 7_000_000_000_000_000;
const ROUTE_OUTPUT_FLOOR_STABLECOIN_RAW: u64 = 1_000_000;

/// The minimum viable output for a route, by destination asset. `None` for
/// assets with no defined floor (e.g. HYPE) — those routes are not gated.
fn route_output_floor(registry: &AssetRegistry, destination_asset: &DepositAsset) -> Option<U256> {
    match registry.canonical_for(destination_asset)? {
        CanonicalAsset::Btc => Some(U256::from(ROUTE_OUTPUT_FLOOR_BTC_SATS)),
        CanonicalAsset::Eth => Some(U256::from(ROUTE_OUTPUT_FLOOR_ETH_WEI)),
        CanonicalAsset::Usdc | CanonicalAsset::Usdt => {
            Some(U256::from(ROUTE_OUTPUT_FLOOR_STABLECOIN_RAW))
        }
        CanonicalAsset::Hype => None,
    }
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

fn fallback_workflow_trace_ids(order_id: Uuid) -> (String, String) {
    let trace_id = order_id.simple().to_string();
    let mut parent_span_id = trace_id[16..32].to_string();
    if parent_span_id == "0000000000000000" {
        parent_span_id = "0000000000000001".to_string();
    }

    (trace_id, parent_span_id)
}

fn is_better_quote(
    candidate: &ComposedMarketOrderQuote,
    current: &Option<ComposedMarketOrderQuote>,
) -> bool {
    let Some(current) = current else {
        return true;
    };

    let primary_cmp = parse_amount("estimated_amount_out", &candidate.estimated_amount_out)
        .ok()
        .cmp(&parse_amount("estimated_amount_out", &current.estimated_amount_out).ok());
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

    fn floor_asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).expect("valid static chain id"),
            asset,
        }
    }
    #[test]
    fn hyperliquid_spot_assets_are_valid_as_source_and_destination() {
        let ubtc = floor_asset("hyperliquid", AssetId::reference("UBTC"));
        let hype = floor_asset("hyperliquid", AssetId::reference("HYPE"));
        let unsupported = floor_asset("hyperliquid", AssetId::reference("UNKNOWN"));

        assert!(validate_hyperliquid_spot_asset(&ubtc).is_ok());
        assert!(validate_hyperliquid_spot_asset(&hype).is_ok());
        let error = validate_hyperliquid_spot_asset(&unsupported)
            .expect_err("unsupported Hyperliquid assets must be rejected");

        assert!(
            error.contains("UBTC, USDT, USDC, UETH, and HYPE"),
            "{error}"
        );
    }

    #[test]
    fn terminal_bridge_exits_to_registered_assets_are_executable() {
        let registry = AssetRegistry::default();
        let bitcoin = floor_asset("bitcoin", AssetId::Native);
        let arbitrum_usdc = floor_asset(
            "evm:42161",
            AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
        );
        let base_usdc = floor_asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );

        let arbitrum_exit_paths = registry.select_transition_paths(&bitcoin, &arbitrum_usdc, 5);
        let hyperliquid_bridge_exit = arbitrum_exit_paths
            .iter()
            .find(|path| {
                matches!(
                    path.transitions.last().map(|transition| transition.kind),
                    Some(MarketOrderTransitionKind::HyperliquidBridgeWithdrawal)
                )
            })
            .expect(
                "BTC -> Arbitrum USDC should be able to end with Hyperliquid bridge withdrawal",
            );
        assert!(is_executable_transition_path(hyperliquid_bridge_exit));

        let base_exit_paths = registry.select_transition_paths(&bitcoin, &base_usdc, 5);
        let cctp_or_across_exit = base_exit_paths
            .iter()
            .find(|path| {
                matches!(
                    path.transitions.last().map(|transition| transition.kind),
                    Some(
                        MarketOrderTransitionKind::CctpBridge
                            | MarketOrderTransitionKind::AcrossBridge
                    )
                ) && path.transitions.iter().any(|transition| {
                    transition.kind == MarketOrderTransitionKind::HyperliquidBridgeWithdrawal
                })
            })
            .expect("BTC -> Base USDC should be able to end with a registered-asset bridge exit");
        assert!(is_executable_transition_path(cctp_or_across_exit));
    }

    #[test]
    fn provider_sequence_filters_exact_transition_provider_order() {
        let registry = AssetRegistry::default();
        let arbitrum_usdt = floor_asset(
            "evm:42161",
            AssetId::reference("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"),
        );
        let base_usdc = floor_asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );

        let mut paths = registry.select_transition_paths(&arbitrum_usdt, &base_usdc, 5);
        paths.retain(is_executable_transition_path);
        assert!(
            !paths.is_empty(),
            "test route pair should enumerate candidates"
        );

        assert!(filter_provider_sequence_paths(
            &mut paths,
            Some(&[ProviderId::Velora, ProviderId::Cctp])
        ));
        assert!(
            paths
                .iter()
                .all(|path| transition_path_matches_provider_sequence(
                    path,
                    &[ProviderId::Velora, ProviderId::Cctp]
                )),
            "every surviving path must match the requested provider sequence"
        );
        assert!(
            !paths.is_empty(),
            "Arbitrum USDT -> Base USDC should support Velora then CCTP"
        );

        assert!(filter_provider_sequence_paths(
            &mut paths,
            Some(&[ProviderId::Across, ProviderId::Velora])
        ));
        assert!(paths.is_empty());
    }

    #[test]
    fn provider_sequence_routing_rejects_empty_sequence() {
        let error = validate_and_normalize_quote_routing(QuoteRoutingRequest {
            provider_sequence: Some(Vec::new()),
        })
        .expect_err("empty providerSequence must be rejected");

        assert!(matches!(error, MarketOrderError::InvalidRouting { .. }));
    }

    #[test]
    fn route_output_floor_resolves_canonical_destination_assets() {
        let registry = AssetRegistry::default();
        assert_eq!(
            route_output_floor(&registry, &floor_asset("bitcoin", AssetId::Native)),
            Some(U256::from(ROUTE_OUTPUT_FLOOR_BTC_SATS))
        );
        assert_eq!(
            route_output_floor(&registry, &floor_asset("evm:1", AssetId::Native)),
            Some(U256::from(ROUTE_OUTPUT_FLOOR_ETH_WEI))
        );
        assert_eq!(
            route_output_floor(&registry, &floor_asset("evm:8453", AssetId::Native)),
            Some(U256::from(ROUTE_OUTPUT_FLOOR_ETH_WEI))
        );
        assert_eq!(
            route_output_floor(
                &registry,
                &floor_asset(
                    "evm:8453",
                    AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
                ),
            ),
            Some(U256::from(ROUTE_OUTPUT_FLOOR_STABLECOIN_RAW))
        );
    }

    #[test]
    fn route_output_floor_is_none_for_unknown_destination_asset() {
        // An asset the registry can't canonicalize is not gated.
        assert_eq!(
            route_output_floor(
                &AssetRegistry::default(),
                &floor_asset(
                    "evm:1",
                    AssetId::reference("0x000000000000000000000000000000000000dead"),
                ),
            ),
            None
        );
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
    fn spot_transfer_quote_materializes_a_hyperliquid_transfer_leg() {
        let quote = ExchangeQuote {
            provider_id: ProviderId::HyperliquidSpot.as_str().to_string(),
            amount_in: "50000".to_string(),
            amount_out: "50000".to_string(),
            min_amount_out: Some("50000".to_string()),
            max_amount_in: None,
            provider_quote: json!({
                "kind": "spot_transfer",
                "leg": {
                    "order_kind": "exact_in",
                    "input_asset": { "chain_id": "hyperliquid", "asset": "UBTC" },
                    "output_asset": { "chain_id": "hyperliquid", "asset": "UBTC" },
                    "amount_in": "50000",
                    "amount_out": "50000",
                    "recipient_address": "0x1111111111111111111111111111111111111111"
                }
            }),
            expires_at: Utc::now(),
        };

        let legs = exchange_quote_transition_legs(
            "transition-1",
            MarketOrderTransitionKind::HyperliquidTrade,
            ProviderId::HyperliquidSpot,
            &quote,
        )
        .expect("spot transfer quote should materialize");

        assert_eq!(legs.len(), 1);
        assert_eq!(legs[0].amount_in, "50000");
        assert_eq!(
            legs[0].raw["recipient_address"],
            json!("0x1111111111111111111111111111111111111111")
        );
    }
    #[test]
    fn cross_token_quote_legs_require_explicit_amounts() {
        let quote = ExchangeQuote {
            provider_id: ProviderId::HyperliquidSpot.as_str().to_string(),
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
            ProviderId::HyperliquidSpot,
            &quote,
        )
        .unwrap_err();

        assert!(error.to_string().contains("missing amount_out"), "{error}");
    }

    #[test]
    fn swap_time_route_key_groups_child_quote_legs_by_transition() {
        use router_core::services::asset_registry::{AssetSlot, RequiredCustodyRole};

        let now = Utc::now();
        let input = DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::reference("USDC"),
        };
        let intermediate = DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::reference("HYPE"),
        };
        let output = DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::reference("UBTC"),
        };
        let transition = TransitionDecl {
            id: "hl-trade".to_string(),
            kind: MarketOrderTransitionKind::HyperliquidTrade,
            provider: ProviderId::HyperliquidSpot,
            input: AssetSlot {
                asset: input.clone(),
                required_custody_role: RequiredCustodyRole::HyperliquidSpot,
            },
            output: AssetSlot {
                asset: output.clone(),
                required_custody_role: RequiredCustodyRole::DestinationPayout,
            },
            from: MarketOrderNode::External(input.clone()),
            to: MarketOrderNode::External(output.clone()),
        };
        let first_leg = QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &input,
            output_asset: &intermediate,
            amount_in: "100",
            amount_out: "50",
            expires_at: now,
            raw: json!({}),
        })
        .with_child_transition_id("hl-trade:leg:0");
        let second_leg = QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &intermediate,
            output_asset: &output,
            amount_in: "50",
            amount_out: "25",
            expires_at: now,
            raw: json!({}),
        })
        .with_child_transition_id("hl-trade:leg:1");

        let provider_quote = json!({
            "transitions": [transition],
            "legs": [first_leg, second_leg],
        });

        let keys = swap_time_route_keys_from_provider_quote(&provider_quote).unwrap();
        assert_eq!(
            keys,
            vec![SwapTimeRouteKey {
                provider: "hyperliquid_spot".to_string(),
                leg_type: "hyperliquid_trade".to_string(),
                transition_decl_id: "hl-trade".to_string(),
                input_chain_id: input.chain.as_str().to_string(),
                input_asset_id: input.asset.as_str().to_string(),
                output_chain_id: output.chain.as_str().to_string(),
                output_asset_id: output.asset.as_str().to_string(),
            }]
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
    fn required_paymaster_native_balances_aggregate_per_chain_with_safety_margin() {
        let plan = GasReimbursementPlan {
            schema_version: 1,
            policy: "test".to_string(),
            quote_safety_multiplier_bps: 10_000,
            debts: vec![
                router_core::services::gas_reimbursement::GasReimbursementDebt {
                    id: "debt-1".to_string(),
                    transition_decl_id: "transition-1".to_string(),
                    transition_kind: "cctp_bridge".to_string(),
                    spend_chain_id: "evm:8453".to_string(),
                    payment_model:
                        router_core::services::gas_reimbursement::GasPaymentModel::PaymasterAdvanced,
                    estimated_native_gas_wei: "100".to_string(),
                    estimated_usd_micro: "1".to_string(),
                },
                router_core::services::gas_reimbursement::GasReimbursementDebt {
                    id: "debt-3".to_string(),
                    transition_decl_id: "transition-3".to_string(),
                    transition_kind: "cctp_bridge".to_string(),
                    spend_chain_id: "evm:8453".to_string(),
                    payment_model:
                        router_core::services::gas_reimbursement::GasPaymentModel::PaymasterAdvanced,
                    estimated_native_gas_wei: "40".to_string(),
                    estimated_usd_micro: "1".to_string(),
                },
            ],
            retention_actions: vec![],
        };

        let required = required_paymaster_native_balances(&plan).unwrap();
        assert_eq!(
            required.get(&ChainId::parse("evm:8453").unwrap()),
            Some(&U256::from(175_u64))
        );
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
