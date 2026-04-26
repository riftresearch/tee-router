use crate::{
    api::{
        CreateOrderCancellationRequest, CreateOrderRequest, CreateQuoteRequest, CreateVaultRequest,
        DetectorHintEnvelope, DetectorHintRequest, DetectorHintTarget,
        ProviderOperationHintEnvelope, ProviderOperationHintRequest,
        ProviderOperationObserveRequest, ProviderPolicyEnvelope, ProviderPolicyListEnvelope,
        QuoteRequestType, UpdateProviderPolicyRequest, VaultFundingHintEnvelope,
        VaultFundingHintRequest,
    },
    app::{initialize_components, PaymasterMode, RouterComponents},
    error::{RouterServerError, RouterServerResult},
    models::{
        DepositRequirements, DepositVault, DepositVaultEnvelope, DepositVaultFundingHint,
        OrderProviderOperationHint, ProviderOperationHintStatus, RouterOrder, RouterOrderEnvelope,
        RouterOrderQuoteEnvelope, StatusResponse, VaultAction,
    },
    protocol::{backend_chain_for_id, supported_chain_ids, ChainId},
    services::{
        action_providers::ProviderOperationObservation, AddressScreeningPurpose,
        AddressScreeningService, OrderExecutionManager, OrderManager, ProviderPolicyService,
        VaultManager,
    },
    Result, RouterServerArgs,
};
use axum::{
    body::Body,
    extract::{MatchedPath, Path, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post, put},
    Json, Router,
};
use chains::ChainRegistry;
use snafu::ResultExt;
use std::{net::SocketAddr, sync::Arc, time::Instant};
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct InternalApiAuth {
    api_key: String,
}

impl InternalApiAuth {
    #[must_use]
    pub fn from_args(args: &RouterServerArgs) -> Option<Self> {
        let api_key = args.router_detector_api_key.as_deref()?.trim();
        if api_key.is_empty() {
            return None;
        }
        Some(Self {
            api_key: api_key.to_string(),
        })
    }

    fn verify_headers(&self, headers: &HeaderMap) -> RouterServerResult<()> {
        verify_bearer_api_key(headers, &self.api_key, "invalid detector API key")
    }
}

#[derive(Clone)]
pub struct AdminApiAuth {
    api_key: String,
}

impl AdminApiAuth {
    #[must_use]
    pub fn from_args(args: &RouterServerArgs) -> Option<Self> {
        let api_key = args.router_admin_api_key.as_deref()?.trim();
        if api_key.is_empty() {
            return None;
        }
        Some(Self {
            api_key: api_key.to_string(),
        })
    }

    fn verify_headers(&self, headers: &HeaderMap) -> RouterServerResult<()> {
        verify_bearer_api_key(headers, &self.api_key, "invalid admin API key")
    }
}

#[derive(Clone)]
pub struct AppState {
    pub vault_manager: Arc<VaultManager>,
    pub order_manager: Arc<OrderManager>,
    pub order_execution_manager: Arc<OrderExecutionManager>,
    pub provider_policies: Arc<ProviderPolicyService>,
    pub address_screener: Option<Arc<AddressScreeningService>>,
    pub chain_registry: Arc<ChainRegistry>,
    pub internal_api_auth: Option<InternalApiAuth>,
    pub admin_api_auth: Option<AdminApiAuth>,
}

pub async fn run_api(args: RouterServerArgs) -> Result<()> {
    info!("Starting router-api...");
    let components = initialize_components(&args, None, PaymasterMode::Disabled).await?;
    serve_api(args, components).await
}

async fn serve_api(args: RouterServerArgs, components: RouterComponents) -> Result<()> {
    let addr = SocketAddr::from((args.host, args.port));
    let state = AppState {
        vault_manager: components.vault_manager,
        order_manager: components.order_manager,
        order_execution_manager: components.order_execution_manager,
        provider_policies: components.provider_policies,
        address_screener: components.address_screener,
        chain_registry: components.chain_registry,
        internal_api_auth: InternalApiAuth::from_args(&args),
        admin_api_auth: AdminApiAuth::from_args(&args),
    };

    let app = build_api_router(state, args.cors_domain.clone());

    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context(crate::ServerBindSnafu)?;

    axum::serve(listener, app)
        .await
        .context(crate::ServerStartSnafu)
}

pub fn build_api_router(state: AppState, cors_domain: Option<String>) -> Router {
    let mut app = Router::new()
        .route("/status", get(status_handler))
        .route("/api/v1/quotes", post(create_quote))
        .route("/api/v1/quotes/:id", get(get_quote))
        .route("/api/v1/orders", post(create_order))
        .route("/api/v1/orders/:id", get(get_order))
        .route(
            "/api/v1/orders/:id/cancellations",
            post(create_order_cancellation),
        )
        .route(
            "/api/v1/provider-operations/hints",
            post(record_provider_operation_hint),
        )
        .route("/api/v1/hints", post(record_detector_hint))
        .route(
            "/api/v1/vaults/:id/funding-hints",
            post(record_vault_funding_hint),
        )
        .route(
            "/api/v1/provider-operations/:id/observe",
            post(observe_provider_operation),
        )
        .route(
            "/internal/v1/provider-policies",
            get(list_provider_policies),
        )
        .route(
            "/internal/v1/provider-policies/:provider",
            put(update_provider_policy),
        )
        .route("/api/v1/chains/:chain/tip", get(get_chain_tip))
        .with_state(state)
        .layer(middleware::from_fn(track_http_metrics));

    if let Some(cors_domain_pattern) = cors_domain {
        let cors_domain = cors_domain_pattern.clone();
        let cors = if cors_domain == "*" {
            CorsLayer::new()
                .allow_origin(tower_http::cors::Any)
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        } else {
            CorsLayer::new()
                .allow_origin(AllowOrigin::predicate(move |origin, _request_parts| {
                    let origin_str = origin.to_str().unwrap_or("");
                    if cors_domain.contains('*') {
                        let pattern = cors_domain.replace('*', "");
                        if cors_domain.starts_with('*') {
                            origin_str.ends_with(&pattern)
                        } else if cors_domain.ends_with('*') {
                            origin_str.starts_with(&pattern[..pattern.len() - 1])
                        } else {
                            let parts: Vec<&str> = cors_domain.split('*').collect();
                            parts.iter().all(|part| origin_str.contains(part))
                        }
                    } else {
                        origin_str == cors_domain
                    }
                }))
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        };

        app = app.layer(cors);
        info!("CORS enabled for domain: {}", cors_domain_pattern);
    }

    app
}

async fn status_handler() -> Json<StatusResponse> {
    Json(StatusResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        supported_chains: supported_chain_ids(),
        supported_actions: vec![VaultAction::Null],
    })
}

async fn track_http_metrics(request: Request<Body>, next: Next) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
        .unwrap_or("unknown")
        .to_string();
    let method = request.method().as_str().to_string();
    let started = Instant::now();

    let response = next.run(request).await;
    crate::telemetry::record_http_response(
        &route,
        &method,
        response.status().as_u16(),
        started.elapsed(),
    );

    response
}

async fn create_quote(
    State(state): State<AppState>,
    Json(request): Json<CreateQuoteRequest>,
) -> RouterServerResult<(StatusCode, Json<RouterOrderQuoteEnvelope>)> {
    screen_user_address(
        &state,
        AddressScreeningPurpose::Recipient,
        &request.market_order.to_asset.chain,
        &request.market_order.recipient_address,
    )
    .await?;
    let envelope = match request.quote_type {
        QuoteRequestType::MarketOrder => {
            state
                .order_manager
                .quote_market_order(request.market_order)
                .await?
        }
    };
    Ok((StatusCode::CREATED, Json(envelope)))
}

async fn get_quote(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> RouterServerResult<Json<RouterOrderQuoteEnvelope>> {
    let quote = state.order_manager.get_quote(id).await?;
    Ok(Json(RouterOrderQuoteEnvelope {
        quote: quote.into(),
    }))
}

async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> RouterServerResult<(StatusCode, Json<RouterOrderEnvelope>)> {
    let quote_for_screening = state.order_manager.get_quote(request.quote_id).await?;
    screen_user_address(
        &state,
        AddressScreeningPurpose::Recipient,
        &quote_for_screening.destination_asset.chain,
        &quote_for_screening.recipient_address,
    )
    .await?;
    screen_user_address(
        &state,
        AddressScreeningPurpose::Refund,
        &quote_for_screening.source_asset.chain,
        &request.refund_address,
    )
    .await?;

    let (order, quote) = state
        .order_manager
        .create_order_from_quote(request.clone())
        .await?;
    let vault_request = CreateVaultRequest {
        order_id: Some(order.id),
        deposit_asset: order.source_asset.clone(),
        action: VaultAction::Null,
        recovery_address: request.refund_address,
        cancellation_commitment: request.cancellation_commitment,
        cancel_after: request.cancel_after,
        metadata: request.metadata,
    };
    let vault = match state.vault_manager.create_vault(vault_request).await {
        Ok(vault) => vault,
        Err(err) => {
            crate::telemetry::record_vault_create_failed(&order.source_asset, &err);
            if let Err(rollback_err) = state
                .order_manager
                .release_quote_after_vault_creation_failure(order.id, quote.id)
                .await
            {
                warn!(
                    order_id = %order.id,
                    quote_id = %quote.id,
                    error = %rollback_err,
                    "Failed to release quote after vault creation failure"
                );
            }
            return Err(err.into());
        }
    };
    let order = state.order_manager.get_order(order.id).await?;
    let response = order_response(state.clone(), order, Some(vault)).await?;
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_order(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> RouterServerResult<Json<RouterOrderEnvelope>> {
    let order = state.order_manager.get_order(id).await?;
    let response = order_response(state, order, None).await?;
    Ok(Json(response))
}

async fn create_order_cancellation(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<CreateOrderCancellationRequest>,
) -> RouterServerResult<Json<RouterOrderEnvelope>> {
    let order = state.order_manager.get_order(id).await?;
    let Some(vault_id) = order.funding_vault_id else {
        return Err(RouterServerError::Validation {
            message: "order does not have a funding vault".to_string(),
        });
    };
    let vault = match state
        .vault_manager
        .cancel_vault(vault_id, &request.cancellation_secret)
        .await
    {
        Ok(vault) => vault,
        Err(err) => {
            crate::telemetry::record_vault_cancel_failed(&err);
            return Err(err.into());
        }
    };
    let order = state.order_manager.mark_order_refunding(&order).await?;
    let response = order_response(state, order, Some(vault)).await?;
    Ok(Json(response))
}

async fn record_provider_operation_hint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ProviderOperationHintRequest>,
) -> RouterServerResult<Json<ProviderOperationHintEnvelope>> {
    authorize_internal_api_request(&state, &headers)?;
    let hint = record_provider_operation_hint_inner(state, request).await?;
    Ok(Json(ProviderOperationHintEnvelope { hint }))
}

async fn record_detector_hint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<DetectorHintRequest>,
) -> RouterServerResult<Json<DetectorHintEnvelope>> {
    authorize_internal_api_request(&state, &headers)?;
    match request.target {
        DetectorHintTarget::ProviderOperation { id } => {
            let hint = record_provider_operation_hint_inner(
                state,
                ProviderOperationHintRequest {
                    provider_operation_id: id,
                    source: request.source,
                    hint_kind: request.hint_kind,
                    evidence: request.evidence,
                    idempotency_key: request.idempotency_key,
                },
            )
            .await?;
            Ok(Json(DetectorHintEnvelope::ProviderOperation { hint }))
        }
        DetectorHintTarget::FundingVault { id } => {
            let hint = record_vault_funding_hint_inner(
                state,
                id,
                VaultFundingHintRequest {
                    source: request.source,
                    hint_kind: request.hint_kind,
                    evidence: request.evidence,
                    idempotency_key: request.idempotency_key,
                },
            )
            .await?;
            Ok(Json(DetectorHintEnvelope::FundingVault { hint }))
        }
    }
}

async fn record_vault_funding_hint(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<VaultFundingHintRequest>,
) -> RouterServerResult<Json<VaultFundingHintEnvelope>> {
    authorize_internal_api_request(&state, &headers)?;
    let hint = record_vault_funding_hint_inner(state, id, request).await?;
    Ok(Json(VaultFundingHintEnvelope { hint }))
}

async fn record_provider_operation_hint_inner(
    state: AppState,
    request: ProviderOperationHintRequest,
) -> RouterServerResult<OrderProviderOperationHint> {
    let now = chrono::Utc::now();
    let hint = state
        .order_execution_manager
        .record_provider_operation_hint(OrderProviderOperationHint {
            id: Uuid::now_v7(),
            provider_operation_id: request.provider_operation_id,
            source: request.source,
            hint_kind: request.hint_kind,
            evidence: request.evidence,
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: request.idempotency_key,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await?;
    Ok(hint)
}

async fn record_vault_funding_hint_inner(
    state: AppState,
    id: Uuid,
    request: VaultFundingHintRequest,
) -> RouterServerResult<DepositVaultFundingHint> {
    let now = chrono::Utc::now();
    let hint = state
        .vault_manager
        .record_funding_hint(DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: id,
            source: request.source,
            hint_kind: request.hint_kind,
            evidence: request.evidence,
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: request.idempotency_key,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await?;
    Ok(hint)
}

async fn observe_provider_operation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
    Json(request): Json<ProviderOperationObserveRequest>,
) -> RouterServerResult<Json<Option<ProviderOperationObservation>>> {
    authorize_internal_api_request(&state, &headers)?;
    let observation = state
        .order_execution_manager
        .observe_provider_operation(id, request.hint_evidence)
        .await?;
    Ok(Json(observation))
}

async fn list_provider_policies(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> RouterServerResult<Json<ProviderPolicyListEnvelope>> {
    authorize_admin_api_request(&state, &headers)?;
    let policies = state.provider_policies.list().await?;
    Ok(Json(ProviderPolicyListEnvelope { policies }))
}

async fn update_provider_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(provider): Path<String>,
    Json(request): Json<UpdateProviderPolicyRequest>,
) -> RouterServerResult<Json<ProviderPolicyEnvelope>> {
    authorize_admin_api_request(&state, &headers)?;
    let provider = normalized_provider_id(&provider)?;
    let updated_by = request
        .updated_by
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("internal_api");
    let policy = state
        .provider_policies
        .upsert(
            provider,
            request.quote_state,
            request.execution_state,
            request.reason.trim(),
            updated_by,
        )
        .await?;
    Ok(Json(ProviderPolicyEnvelope { policy }))
}

fn authorize_internal_api_request(state: &AppState, headers: &HeaderMap) -> RouterServerResult<()> {
    let Some(auth) = state.internal_api_auth.as_ref() else {
        return Err(RouterServerError::Unauthorized {
            message: "detector API key is not configured".to_string(),
        });
    };
    auth.verify_headers(headers)
}

fn authorize_admin_api_request(state: &AppState, headers: &HeaderMap) -> RouterServerResult<()> {
    let Some(auth) = state.admin_api_auth.as_ref() else {
        return Err(RouterServerError::Unauthorized {
            message: "admin API key is not configured".to_string(),
        });
    };
    auth.verify_headers(headers)
}

fn verify_bearer_api_key(
    headers: &HeaderMap,
    expected_key: &str,
    error_message: &'static str,
) -> RouterServerResult<()> {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Err(RouterServerError::Unauthorized {
            message: error_message.to_string(),
        });
    };
    let Some(value) = value.to_str().ok() else {
        return Err(RouterServerError::Unauthorized {
            message: error_message.to_string(),
        });
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(RouterServerError::Unauthorized {
            message: error_message.to_string(),
        });
    };
    if !scheme.eq_ignore_ascii_case("bearer") || token.trim() != expected_key {
        return Err(RouterServerError::Unauthorized {
            message: error_message.to_string(),
        });
    }
    Ok(())
}

fn normalized_provider_id(provider: &str) -> RouterServerResult<String> {
    let provider = provider.trim();
    if provider.is_empty() {
        return Err(RouterServerError::Validation {
            message: "provider path must not be empty".to_string(),
        });
    }
    Ok(provider.to_ascii_lowercase())
}

async fn screen_user_address(
    state: &AppState,
    purpose: AddressScreeningPurpose,
    chain_id: &ChainId,
    address: &str,
) -> RouterServerResult<()> {
    let Some(address_screener) = state.address_screener.as_ref() else {
        return Ok(());
    };
    let normalized =
        normalize_screened_address(state.chain_registry.as_ref(), purpose, chain_id, address)?;
    address_screener
        .screen_address(purpose, chain_id, &normalized)
        .await
        .map_err(RouterServerError::from)
}

fn normalize_screened_address(
    chain_registry: &ChainRegistry,
    purpose: AddressScreeningPurpose,
    chain_id: &ChainId,
    address: &str,
) -> RouterServerResult<String> {
    let backend_chain = backend_chain_for_id(chain_id).ok_or(RouterServerError::Validation {
        message: format!("chain not configured for {purpose} screening: {chain_id}"),
    })?;
    let chain = chain_registry
        .get(&backend_chain)
        .ok_or(RouterServerError::Validation {
            message: format!("chain not configured for {purpose} screening: {chain_id}"),
        })?;
    let normalized = if chain_id.evm_chain_id().is_some() {
        address.to_lowercase()
    } else {
        address.to_string()
    };
    if chain.validate_address(&normalized) {
        Ok(normalized)
    } else {
        Err(RouterServerError::Validation {
            message: format!("Invalid {purpose} address {address} for {chain_id}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn internal_api_auth_accepts_matching_bearer_header() {
        let auth = InternalApiAuth {
            api_key: "detector-secret".to_string(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer detector-secret"),
        );

        auth.verify_headers(&headers).unwrap();
    }

    #[test]
    fn internal_api_auth_rejects_missing_or_invalid_bearer_header() {
        let auth = InternalApiAuth {
            api_key: "detector-secret".to_string(),
        };
        assert!(matches!(
            auth.verify_headers(&HeaderMap::new()),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong"),
        );
        assert!(matches!(
            auth.verify_headers(&headers),
            Err(RouterServerError::Unauthorized { .. })
        ));
    }

    #[test]
    fn admin_api_auth_accepts_matching_bearer_header() {
        let auth = AdminApiAuth {
            api_key: "admin-secret".to_string(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer admin-secret"),
        );

        auth.verify_headers(&headers).unwrap();
    }

    #[test]
    fn admin_api_auth_rejects_missing_or_invalid_bearer_header() {
        let auth = AdminApiAuth {
            api_key: "admin-secret".to_string(),
        };
        assert!(matches!(
            auth.verify_headers(&HeaderMap::new()),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut wrong_scheme = HeaderMap::new();
        wrong_scheme.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic admin-secret"),
        );
        assert!(matches!(
            auth.verify_headers(&wrong_scheme),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut wrong_key = HeaderMap::new();
        wrong_key.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong"),
        );
        assert!(matches!(
            auth.verify_headers(&wrong_key),
            Err(RouterServerError::Unauthorized { .. })
        ));
    }
}

async fn get_chain_tip(
    State(state): State<AppState>,
    Path(chain): Path<String>,
) -> RouterServerResult<Json<serde_json::Value>> {
    let chain = ChainId::parse(chain).map_err(|err| RouterServerError::Validation {
        message: format!("unsupported chain path: {err}"),
    })?;
    let backend_chain = backend_chain_for_id(&chain).ok_or(RouterServerError::Validation {
        message: format!("chain not configured: {chain}"),
    })?;
    let chain_impl =
        state
            .chain_registry
            .get(&backend_chain)
            .ok_or(RouterServerError::Validation {
                message: format!("chain not configured: {chain}"),
            })?;
    let block_hash = chain_impl.get_best_hash().await?;
    Ok(Json(serde_json::json!({ "block_hash": block_hash })))
}

fn vault_response(
    chain_registry: &ChainRegistry,
    vault: DepositVault,
) -> RouterServerResult<DepositVaultEnvelope> {
    let backend_chain =
        backend_chain_for_id(&vault.deposit_asset.chain).ok_or(RouterServerError::Validation {
            message: format!("chain not configured: {}", vault.deposit_asset.chain),
        })?;
    let chain = chain_registry
        .get(&backend_chain)
        .ok_or(RouterServerError::Validation {
            message: format!("chain not configured: {}", vault.deposit_asset.chain),
        })?;

    Ok(DepositVaultEnvelope {
        deposit_requirements: DepositRequirements {
            minimum_confirmations: chain.minimum_block_confirmations(),
            estimated_block_time_seconds: chain.estimated_block_time().as_secs(),
        },
        vault,
    })
}

async fn order_response(
    state: AppState,
    order: RouterOrder,
    funding_vault: Option<DepositVault>,
) -> RouterServerResult<RouterOrderEnvelope> {
    let quote = state.order_manager.get_quote_for_order(order.id).await?;
    let funding_vault = match (funding_vault, order.funding_vault_id) {
        (Some(vault), _) => Some(vault_response(state.chain_registry.as_ref(), vault)?),
        (None, Some(vault_id)) => {
            let vault = state.vault_manager.get_vault(vault_id).await?;
            Some(vault_response(state.chain_registry.as_ref(), vault)?)
        }
        (None, None) => None,
    };

    Ok(RouterOrderEnvelope {
        order,
        quote: quote.into(),
        funding_vault,
    })
}
