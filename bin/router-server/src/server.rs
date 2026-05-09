use crate::{
    api::{
        CreateOrderCancellationRequest, CreateOrderRequest, CreateQuoteRequest, CreateVaultRequest,
        DetectorHintEnvelope, DetectorHintRequest, DetectorHintTarget, ProviderHealthEnvelope,
        ProviderOperationHintEnvelope, ProviderOperationHintRequest, ProviderPolicyEnvelope,
        ProviderPolicyListEnvelope, UpdateProviderPolicyRequest, VaultFundingHintEnvelope,
        VaultFundingHintRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN,
    },
    app::{initialize_components, PaymasterMode, RouterComponents},
    error::{RouterServerError, RouterServerResult},
    services::{
        AddressScreeningPurpose, AddressScreeningService, OrderManager, ProviderHealthService,
        ProviderPolicyService, VaultManager,
    },
    Error, Result, RouterServerArgs,
};
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{
        rejection::{JsonRejection, PathRejection},
        DefaultBodyLimit, FromRequest, FromRequestParts, MatchedPath, Path as AxumPath, State,
    },
    http::{header, request::Parts, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use chains::ChainRegistry;
use chrono::Utc;
use router_core::{
    models::{
        DepositRequirements, DepositVault, DepositVaultEnvelope, DepositVaultFundingHint,
        OrderExecutionAttempt, OrderExecutionAttemptKind, OrderProviderOperation,
        OrderProviderOperationHint, ProviderOperationHintStatus, ProviderOperationType,
        RouterOrder, RouterOrderEnvelope, RouterOrderQuoteEnvelope, StatusResponse, VaultAction,
    },
    protocol::{backend_chain_for_id, supported_chain_ids, ChainId},
    services::asset_registry::ProviderId,
};
use router_temporal::{
    boxed as boxed_temporal_error, OrderWorkflowClient, ProviderHintKind, ProviderKind,
    ProviderOperationHintEvidence, ProviderOperationHintSignal, RouterTemporalError,
    TemporalConnection,
};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use snafu::{FromString, ResultExt, Whatever};
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Instant,
};
use tower_http::{
    cors::{AllowOrigin, CorsLayer},
    trace::{DefaultMakeSpan, TraceLayer},
};
use tracing::{info, warn, Level};
use uuid::Uuid;

const MIN_INTERNAL_API_KEY_LEN: usize = 32;
const MAX_PROVIDER_POLICY_ID_LEN: usize = 64;
const MAX_PROVIDER_POLICY_REASON_LEN: usize = 512;
const MAX_PROVIDER_POLICY_UPDATED_BY_LEN: usize = 128;
const MIN_ORDER_IDEMPOTENCY_KEY_LEN: usize = 16;
const MAX_ORDER_IDEMPOTENCY_KEY_LEN: usize = 128;
const MAX_HINT_SOURCE_LEN: usize = 64;
const MAX_HINT_EVIDENCE_JSON_BYTES: usize = 16 * 1024;
pub const MAX_ROUTER_JSON_BODY_BYTES: usize = 256 * 1024;
const _: () = assert!(MAX_ROUTER_JSON_BODY_BYTES >= MAX_HINT_EVIDENCE_JSON_BYTES * 2);
const _: () = assert!(MAX_ROUTER_JSON_BODY_BYTES <= 1024 * 1024);

struct RouterJson<T>(T);
struct RouterPath<T>(T);

#[async_trait]
impl<S, T> FromRequest<S> for RouterJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = Response;

    async fn from_request(req: Request<Body>, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(json_rejection_response)
    }
}

#[async_trait]
impl<S, T> FromRequestParts<S> for RouterPath<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        AxumPath::<T>::from_request_parts(parts, state)
            .await
            .map(|AxumPath(value)| Self(value))
            .map_err(path_rejection_response)
    }
}

#[derive(Clone)]
pub struct InternalApiAuth {
    api_key: String,
}

impl InternalApiAuth {
    #[must_use]
    pub fn from_args(args: &RouterServerArgs) -> Option<Self> {
        let api_key = normalize_configured_api_key(args.router_detector_api_key.as_deref())?;
        Some(Self {
            api_key: api_key.to_string(),
        })
    }

    fn verify_headers(&self, headers: &HeaderMap) -> RouterServerResult<()> {
        verify_bearer_api_key(headers, &self.api_key, "invalid detector API key")
    }
}

#[derive(Clone)]
pub struct GatewayApiAuth {
    api_key: String,
}

impl GatewayApiAuth {
    #[must_use]
    pub fn from_args(args: &RouterServerArgs) -> Option<Self> {
        let api_key = normalize_configured_api_key(args.router_gateway_api_key.as_deref())?;
        Some(Self {
            api_key: api_key.to_string(),
        })
    }

    fn verify_headers(&self, headers: &HeaderMap) -> RouterServerResult<()> {
        verify_bearer_api_key(headers, &self.api_key, "invalid gateway API key")
    }
}

#[derive(Clone)]
pub struct AdminApiAuth {
    api_key: String,
}

impl AdminApiAuth {
    #[must_use]
    pub fn from_args(args: &RouterServerArgs) -> Option<Self> {
        let api_key = normalize_configured_api_key(args.router_admin_api_key.as_deref())?;
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
    pub db: router_core::db::Database,
    pub vault_manager: Arc<VaultManager>,
    pub order_manager: Arc<OrderManager>,
    pub provider_health: Arc<ProviderHealthService>,
    pub provider_policies: Arc<ProviderPolicyService>,
    pub address_screener: Option<Arc<AddressScreeningService>>,
    pub chain_registry: Arc<ChainRegistry>,
    pub order_workflow_client: Option<Arc<OrderWorkflowClient>>,
    pub internal_api_auth: Option<InternalApiAuth>,
    pub gateway_api_auth: Option<GatewayApiAuth>,
    pub admin_api_auth: Option<AdminApiAuth>,
}

pub async fn run_api(args: RouterServerArgs) -> Result<()> {
    validate_public_api_auth_config(&args)?;
    info!("Starting router-api...");
    let components = initialize_components(&args, None, PaymasterMode::Disabled).await?;
    serve_api(args, components).await
}

async fn serve_api(args: RouterServerArgs, components: RouterComponents) -> Result<()> {
    let addr = SocketAddr::from((args.host, args.port));
    let order_workflow_client = Arc::new(
        OrderWorkflowClient::connect(
            &TemporalConnection {
                temporal_address: args.temporal_address.clone(),
                namespace: args.temporal_namespace.clone(),
            },
            args.temporal_task_queue.clone(),
        )
        .await
        .map_err(|source| Error::Generic {
            source: Whatever::without_source(format!(
                "failed to initialize Temporal order workflow client: {source}"
            )),
        })?,
    );
    let state = AppState {
        db: components.db,
        vault_manager: components.vault_manager,
        order_manager: components.order_manager,
        provider_health: components.provider_health,
        provider_policies: components.provider_policies,
        address_screener: components.address_screener,
        chain_registry: components.chain_registry,
        order_workflow_client: Some(order_workflow_client),
        internal_api_auth: InternalApiAuth::from_args(&args),
        gateway_api_auth: GatewayApiAuth::from_args(&args),
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
        .route("/api/v1/provider-health", get(get_provider_health))
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
            "/internal/v1/provider-policies",
            get(list_provider_policies),
        )
        .route(
            "/internal/v1/provider-policies/:provider",
            put(update_provider_policy),
        )
        .route("/internal/v1/orders/:id/flow", get(get_order_flow))
        .route("/api/v1/chains/:chain/tip", get(get_chain_tip))
        .with_state(state)
        .layer(
            TraceLayer::new_for_http().make_span_with(
                DefaultMakeSpan::new()
                    .level(Level::INFO)
                    .include_headers(false),
            ),
        )
        .layer(DefaultBodyLimit::max(MAX_ROUTER_JSON_BODY_BYTES))
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
                    let Ok(origin_str) = origin.to_str() else {
                        return false;
                    };
                    origin_matches_cors_pattern(origin_str, &cors_domain)
                }))
                .allow_methods(tower_http::cors::Any)
                .allow_headers(tower_http::cors::Any)
        };

        app = app.layer(cors);
        info!("CORS enabled for domain: {}", cors_domain_pattern);
    }

    app
}

fn origin_matches_cors_pattern(origin: &str, pattern: &str) -> bool {
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return false;
    }
    if pattern == "*" {
        return true;
    }

    match pattern.split_once('*') {
        None => origin == pattern,
        Some((prefix, suffix)) => {
            if suffix.contains('*') {
                return false;
            }
            if !origin.starts_with(prefix) || !origin.ends_with(suffix) {
                return false;
            }
            let wildcard_len = origin
                .len()
                .saturating_sub(prefix.len())
                .saturating_sub(suffix.len());
            wildcard_len > 0
        }
    }
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

fn json_rejection_response(rejection: JsonRejection) -> Response {
    let status = rejection.status();
    let message = match status {
        StatusCode::PAYLOAD_TOO_LARGE => "Request body is too large",
        StatusCode::UNSUPPORTED_MEDIA_TYPE => {
            "Expected request with Content-Type: application/json"
        }
        StatusCode::BAD_REQUEST => "Malformed JSON request body",
        StatusCode::UNPROCESSABLE_ENTITY => "Invalid JSON request body",
        _ => "Invalid JSON request body",
    };

    (
        status,
        Json(serde_json::json!({
            "error": {
                "code": status.as_u16(),
                "message": message,
            }
        })),
    )
        .into_response()
}

fn path_rejection_response(rejection: PathRejection) -> Response {
    let status = rejection.status();
    (
        status,
        Json(serde_json::json!({
            "error": {
                "code": status.as_u16(),
                "message": "Invalid path parameter",
            }
        })),
    )
        .into_response()
}

async fn create_quote(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterJson(request): RouterJson<CreateQuoteRequest>,
) -> RouterServerResult<(StatusCode, Json<RouterOrderQuoteEnvelope>)> {
    authorize_gateway_api_request(&state, &headers)?;
    let envelope = match request {
        CreateQuoteRequest::MarketOrder(market_order) => {
            screen_user_address(
                &state,
                AddressScreeningPurpose::Recipient,
                &market_order.to_asset.chain,
                &market_order.recipient_address,
            )
            .await?;
            state.order_manager.quote_market_order(market_order).await?
        }
        CreateQuoteRequest::LimitOrder(limit_order) => {
            screen_user_address(
                &state,
                AddressScreeningPurpose::Recipient,
                &limit_order.to_asset.chain,
                &limit_order.recipient_address,
            )
            .await?;
            state.order_manager.quote_limit_order(limit_order).await?
        }
    };
    Ok((StatusCode::CREATED, Json(envelope)))
}

async fn get_quote(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(id): RouterPath<Uuid>,
) -> RouterServerResult<Json<RouterOrderQuoteEnvelope>> {
    authorize_gateway_api_request(&state, &headers)?;
    let quote = state.order_manager.get_quote(id).await?;
    Ok(Json(RouterOrderQuoteEnvelope { quote }))
}

async fn create_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterJson(request): RouterJson<CreateOrderRequest>,
) -> RouterServerResult<(StatusCode, Json<RouterOrderEnvelope>)> {
    authorize_gateway_api_request(&state, &headers)?;
    require_order_idempotency_key(&request)?;
    let quote_for_screening = state.order_manager.get_quote(request.quote_id).await?;
    let cancellation = state
        .vault_manager
        .order_cancellation_material(request.quote_id);
    screen_user_address(
        &state,
        AddressScreeningPurpose::Recipient,
        &quote_for_screening.destination_asset().chain,
        quote_for_screening.recipient_address(),
    )
    .await?;
    screen_user_address(
        &state,
        AddressScreeningPurpose::Refund,
        &quote_for_screening.source_asset().chain,
        &request.refund_address,
    )
    .await?;

    let (order, quote) = state
        .order_manager
        .create_order_from_quote(request.clone())
        .await?;
    if let Some(vault_id) = order.funding_vault_id {
        let vault = state.vault_manager.get_vault(vault_id).await?;
        ensure_recoverable_cancellation_secret(&vault, &cancellation.commitment)?;
        let mut response = order_response(state.clone(), order, Some(vault)).await?;
        response.cancellation_secret = Some(cancellation.secret);
        return Ok((StatusCode::OK, Json(response)));
    }

    let vault_request = CreateVaultRequest {
        order_id: Some(order.id),
        deposit_asset: order.source_asset.clone(),
        action: VaultAction::Null,
        recovery_address: request.refund_address,
        cancellation_commitment: cancellation.commitment,
        cancel_after: None,
        metadata: request.metadata,
    };
    let vault = match state.vault_manager.create_vault(vault_request).await {
        Ok(vault) => vault,
        Err(err) => {
            crate::telemetry::record_vault_create_failed(&order.source_asset, &err);
            if let Err(rollback_err) = state
                .order_manager
                .release_quote_after_vault_creation_failure(order.id, quote.id())
                .await
            {
                warn!(
                    order_id = %order.id,
                    quote_id = %quote.id(),
                    error = %rollback_err,
                    "Failed to release quote after vault creation failure"
                );
            }
            return Err(err.into());
        }
    };
    let order = state.order_manager.get_order(order.id).await?;
    crate::telemetry::record_order_workflow_event(&order, "order.funding_vault_created");
    let mut response = order_response(state.clone(), order, Some(vault)).await?;
    response.cancellation_secret = Some(cancellation.secret);
    Ok((StatusCode::CREATED, Json(response)))
}

async fn get_order(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(id): RouterPath<Uuid>,
) -> RouterServerResult<Json<RouterOrderEnvelope>> {
    authorize_gateway_api_request(&state, &headers)?;
    let order = state.order_manager.get_order(id).await?;
    let response = order_response(state, order, None).await?;
    Ok(Json(response))
}

async fn create_order_cancellation(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(id): RouterPath<Uuid>,
    RouterJson(request): RouterJson<CreateOrderCancellationRequest>,
) -> RouterServerResult<Json<RouterOrderEnvelope>> {
    authorize_gateway_api_request(&state, &headers)?;
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

fn require_order_idempotency_key(request: &CreateOrderRequest) -> RouterServerResult<()> {
    let Some(idempotency_key) = request.idempotency_key.as_deref() else {
        return Err(RouterServerError::Validation {
            message: "idempotency_key is required".to_string(),
        });
    };
    let idempotency_key = idempotency_key.trim();
    if idempotency_key.is_empty() {
        return Err(RouterServerError::Validation {
            message: "idempotency_key must not be empty".to_string(),
        });
    }
    if idempotency_key.len() < MIN_ORDER_IDEMPOTENCY_KEY_LEN {
        return Err(RouterServerError::Validation {
            message: format!(
                "idempotency_key must be at least {MIN_ORDER_IDEMPOTENCY_KEY_LEN} bytes"
            ),
        });
    }
    if idempotency_key.len() > MAX_ORDER_IDEMPOTENCY_KEY_LEN {
        return Err(RouterServerError::Validation {
            message: format!(
                "idempotency_key must be at most {MAX_ORDER_IDEMPOTENCY_KEY_LEN} bytes"
            ),
        });
    }
    if !is_protocol_token(idempotency_key) {
        return Err(RouterServerError::Validation {
            message: "idempotency_key may only contain letters, numbers, '.', '_', ':', and '-'"
                .to_string(),
        });
    }
    Ok(())
}

fn ensure_recoverable_cancellation_secret(
    vault: &DepositVault,
    expected_commitment: &str,
) -> RouterServerResult<()> {
    if vault.cancellation_commitment == expected_commitment {
        return Ok(());
    }

    Err(RouterServerError::Internal {
        message: format!(
            "order {} has funding vault {} with unrecoverable cancellation commitment",
            vault
                .order_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "<unbound>".to_string()),
            vault.id
        ),
    })
}

async fn record_provider_operation_hint(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterJson(request): RouterJson<ProviderOperationHintRequest>,
) -> RouterServerResult<Json<ProviderOperationHintEnvelope>> {
    authorize_internal_api_request(&state, &headers)?;
    let hint = record_provider_operation_hint_inner(state, request).await?;
    Ok(Json(ProviderOperationHintEnvelope { hint }))
}

async fn record_detector_hint(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterJson(request): RouterJson<DetectorHintRequest>,
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
    RouterPath(id): RouterPath<Uuid>,
    RouterJson(request): RouterJson<VaultFundingHintRequest>,
) -> RouterServerResult<Json<VaultFundingHintEnvelope>> {
    authorize_internal_api_request(&state, &headers)?;
    let hint = record_vault_funding_hint_inner(state, id, request).await?;
    Ok(Json(VaultFundingHintEnvelope { hint }))
}

async fn record_provider_operation_hint_inner(
    state: AppState,
    request: ProviderOperationHintRequest,
) -> RouterServerResult<OrderProviderOperationHint> {
    let order_workflow_client =
        state
            .order_workflow_client
            .as_ref()
            .ok_or_else(|| RouterServerError::NotReady {
                message: "Temporal order workflow client is not configured".to_string(),
            })?;
    let evidence = normalize_hint_evidence(request.evidence)?;
    let operation = state
        .db
        .orders()
        .get_provider_operation(request.provider_operation_id)
        .await?;
    let now = Utc::now();
    let hint = state
        .db
        .orders()
        .create_provider_operation_hint(&OrderProviderOperationHint {
            id: Uuid::now_v7(),
            provider_operation_id: request.provider_operation_id,
            source: normalize_hint_source(request.source)?,
            hint_kind: request.hint_kind,
            evidence,
            status: ProviderOperationHintStatus::Processing,
            idempotency_key: normalize_hint_idempotency_key(request.idempotency_key)?,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await?;
    record_provider_operation_hint_event(
        &state,
        operation.order_id,
        &hint,
        &operation,
        "provider_operation.hint_recorded",
    )
    .await;

    if hint.status != ProviderOperationHintStatus::Processing {
        return Ok(hint);
    }

    let attempt = load_provider_operation_attempt(&state, &operation).await?;
    let signal = provider_operation_hint_signal(&operation, &hint)?;
    match signal_provider_operation_hint(
        order_workflow_client,
        &attempt,
        operation.order_id,
        signal,
    )
    .await
    {
        Ok(()) => {
            let completed = state
                .db
                .orders()
                .complete_provider_operation_hint(
                    hint.id,
                    hint.claimed_at,
                    ProviderOperationHintStatus::Processed,
                    serde_json::json!({}),
                    Utc::now(),
                )
                .await?;
            record_provider_operation_hint_event(
                &state,
                operation.order_id,
                &completed,
                &operation,
                "provider_operation.hint_signaled",
            )
            .await;
            Ok(completed)
        }
        Err(RouterTemporalError::WorkflowSignalUnavailable {
            workflow_id,
            message,
        }) => {
            let ignored = state
                .db
                .orders()
                .complete_provider_operation_hint(
                    hint.id,
                    hint.claimed_at,
                    ProviderOperationHintStatus::Ignored,
                    serde_json::json!({
                        "reason": "workflow_unavailable",
                        "workflow_id": workflow_id,
                        "message": message,
                    }),
                    Utc::now(),
                )
                .await?;
            record_provider_operation_hint_event(
                &state,
                operation.order_id,
                &ignored,
                &operation,
                "provider_operation.hint_ignored_workflow_unavailable",
            )
            .await;
            Ok(ignored)
        }
        Err(source) => {
            let _ = state
                .db
                .orders()
                .complete_provider_operation_hint(
                    hint.id,
                    hint.claimed_at,
                    ProviderOperationHintStatus::Failed,
                    serde_json::json!({
                        "reason": "temporal_signal_failed",
                        "error": source.to_string(),
                    }),
                    Utc::now(),
                )
                .await;
            Err(RouterServerError::NotReady {
                message: format!("failed to signal provider-operation hint workflow: {source}"),
            })
        }
    }
}

async fn load_provider_operation_attempt(
    state: &AppState,
    operation: &OrderProviderOperation,
) -> RouterServerResult<OrderExecutionAttempt> {
    let Some(attempt_id) = operation.execution_attempt_id else {
        return Err(RouterServerError::InvalidData {
            message: format!(
                "provider operation {} is not attached to an execution attempt",
                operation.id
            ),
        });
    };
    Ok(state.db.orders().get_execution_attempt(attempt_id).await?)
}

fn provider_operation_hint_signal(
    operation: &OrderProviderOperation,
    hint: &OrderProviderOperationHint,
) -> RouterServerResult<ProviderOperationHintSignal> {
    let (provider, hint_kind) = provider_hint_shape_for_operation(operation.operation_type);
    Ok(ProviderOperationHintSignal {
        order_id: operation.order_id,
        hint_id: hint.id,
        provider_operation_id: Some(operation.id),
        provider,
        hint_kind,
        provider_ref: operation.provider_ref.clone(),
        evidence: provider_hint_evidence_for_signal(hint_kind, &hint.evidence)?,
    })
}

fn provider_hint_shape_for_operation(
    operation_type: ProviderOperationType,
) -> (ProviderKind, ProviderHintKind) {
    match operation_type {
        ProviderOperationType::AcrossBridge => (ProviderKind::Bridge, ProviderHintKind::AcrossFill),
        ProviderOperationType::CctpBridge => {
            (ProviderKind::Bridge, ProviderHintKind::CctpAttestation)
        }
        ProviderOperationType::UnitDeposit => (ProviderKind::Unit, ProviderHintKind::UnitDeposit),
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder => {
            (ProviderKind::Exchange, ProviderHintKind::HyperliquidTrade)
        }
        ProviderOperationType::UnitWithdrawal => {
            (ProviderKind::Unit, ProviderHintKind::ProviderObservation)
        }
        ProviderOperationType::HyperliquidBridgeDeposit
        | ProviderOperationType::HyperliquidBridgeWithdrawal => {
            (ProviderKind::Bridge, ProviderHintKind::ProviderObservation)
        }
        ProviderOperationType::UniversalRouterSwap => (
            ProviderKind::Exchange,
            ProviderHintKind::ProviderObservation,
        ),
    }
}

fn provider_hint_evidence_for_signal(
    hint_kind: ProviderHintKind,
    evidence: &serde_json::Value,
) -> RouterServerResult<Option<ProviderOperationHintEvidence>> {
    let tx_hash = evidence
        .get("tx_hash")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let address = evidence
        .get("address")
        .or_else(|| evidence.get("recipient_address"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let transfer_index = evidence
        .get("transfer_index")
        .or_else(|| evidence.get("vout"))
        .and_then(json_u64_or_string);

    match (tx_hash, address, transfer_index) {
        (Some(tx_hash), Some(address), Some(transfer_index)) => {
            let amount = evidence.get("amount").and_then(|value| match value {
                serde_json::Value::String(value) => Some(value.clone()),
                serde_json::Value::Number(value) => Some(value.to_string()),
                _ => None,
            });
            Ok(Some(ProviderOperationHintEvidence {
                tx_hash,
                address,
                transfer_index,
                amount,
            }))
        }
        (None, None, None) => Ok(None),
        _ if hint_kind != ProviderHintKind::UnitDeposit => Ok(None),
        _ => Err(RouterServerError::Validation {
            message:
                "UnitDeposit provider-operation hints require tx_hash, address, and transfer_index"
                    .to_string(),
        }),
    }
}

fn json_u64_or_string(value: &serde_json::Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

async fn signal_provider_operation_hint(
    order_workflow_client: &OrderWorkflowClient,
    attempt: &OrderExecutionAttempt,
    order_id: Uuid,
    signal: ProviderOperationHintSignal,
) -> std::result::Result<(), RouterTemporalError> {
    match attempt.attempt_kind {
        OrderExecutionAttemptKind::PrimaryExecution
        | OrderExecutionAttemptKind::RetryExecution
        | OrderExecutionAttemptKind::RefreshedExecution => {
            order_workflow_client
                .signal_provider_hint(order_id, signal)
                .await
        }
        OrderExecutionAttemptKind::RefundRecovery => {
            let parent_attempt_id = attempt
                .failure_reason
                .get("failed_attempt_id")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse::<Uuid>().ok())
                .ok_or_else(|| RouterTemporalError::Temporal {
                    action: "resolve RefundWorkflow parent attempt",
                    source: boxed_temporal_error(format!(
                        "RefundRecovery attempt {} is missing failure_reason.failed_attempt_id",
                        attempt.id
                    )),
                })?;
            order_workflow_client
                .signal_refund_provider_hint(order_id, parent_attempt_id, signal)
                .await
        }
    }
}

async fn record_provider_operation_hint_event(
    state: &AppState,
    order_id: Uuid,
    hint: &OrderProviderOperationHint,
    operation: &OrderProviderOperation,
    event: &'static str,
) {
    if let Ok(order) = state.db.orders().get(order_id).await {
        crate::telemetry::record_provider_operation_hint_workflow_event(
            &order, hint, operation, event,
        );
    }
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
            source: normalize_hint_source(request.source)?,
            hint_kind: request.hint_kind,
            evidence: normalize_hint_evidence(request.evidence)?,
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: normalize_hint_idempotency_key(request.idempotency_key)?,
            error: serde_json::json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await?;
    if let Ok(vault) = state.db.vaults().get(id).await {
        if let Some(order_id) = vault.order_id {
            if let Ok(order) = state.db.orders().get(order_id).await {
                crate::telemetry::record_order_workflow_event(
                    &order,
                    "funding_vault.hint_recorded",
                );
            }
        }
    }
    Ok(hint)
}

async fn list_provider_policies(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> RouterServerResult<Json<ProviderPolicyListEnvelope>> {
    authorize_admin_api_request(&state, &headers)?;
    let policies = state.provider_policies.list().await?;
    Ok(Json(ProviderPolicyListEnvelope { policies }))
}

async fn get_provider_health(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> RouterServerResult<Json<ProviderHealthEnvelope>> {
    authorize_gateway_api_request(&state, &headers)?;
    let providers = state.provider_health.list().await?;
    Ok(Json(ProviderHealthEnvelope {
        status: router_core::models::ProviderHealthSummaryStatus::from_checks(&providers),
        timestamp: Utc::now(),
        providers,
    }))
}

async fn update_provider_policy(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(provider): RouterPath<String>,
    RouterJson(request): RouterJson<UpdateProviderPolicyRequest>,
) -> RouterServerResult<Json<ProviderPolicyEnvelope>> {
    authorize_admin_api_request(&state, &headers)?;
    let provider = normalized_provider_id(&provider)?;
    let reason = normalized_provider_policy_reason(&request.reason)?;
    let updated_by = normalized_provider_policy_updated_by(request.updated_by.as_deref())?;
    let policy = state
        .provider_policies
        .upsert(
            provider,
            request.quote_state,
            request.execution_state,
            &reason,
            &updated_by,
        )
        .await?;
    Ok(Json(ProviderPolicyEnvelope { policy }))
}

async fn get_order_flow(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(id): RouterPath<Uuid>,
) -> RouterServerResult<Json<crate::api::OrderFlowEnvelope>> {
    authorize_admin_api_request(&state, &headers)?;
    Ok(Json(crate::query_api::get_order_flow(&state.db, id).await?))
}

fn authorize_internal_api_request(state: &AppState, headers: &HeaderMap) -> RouterServerResult<()> {
    let Some(auth) = state.internal_api_auth.as_ref() else {
        return Err(RouterServerError::Unauthorized {
            message: "detector API key is not configured".to_string(),
        });
    };
    auth.verify_headers(headers)
}

fn authorize_gateway_api_request(state: &AppState, headers: &HeaderMap) -> RouterServerResult<()> {
    let Some(auth) = state.gateway_api_auth.as_ref() else {
        return Ok(());
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

fn normalize_configured_api_key(value: Option<&str>) -> Option<&str> {
    let api_key = value?.trim();
    if api_key.len() < MIN_INTERNAL_API_KEY_LEN {
        return None;
    }
    Some(api_key)
}

fn validate_public_api_auth_config(args: &RouterServerArgs) -> Result<()> {
    validate_bound_api_auth_config(
        args.host,
        normalize_configured_api_key(args.router_detector_api_key.as_deref()),
        normalize_configured_api_key(args.router_gateway_api_key.as_deref()),
        normalize_configured_api_key(args.router_admin_api_key.as_deref()),
    )
}

fn validate_bound_api_auth_config(
    host: IpAddr,
    detector_api_key: Option<&str>,
    gateway_api_key: Option<&str>,
    admin_api_key: Option<&str>,
) -> Result<()> {
    validate_distinct_api_auth_keys(detector_api_key, gateway_api_key, admin_api_key)?;
    if host.is_loopback() {
        return Ok(());
    }
    if detector_api_key.is_none() {
        return Err(config_error(
            "ROUTER_DETECTOR_API_KEY must be configured when router-api binds a non-loopback host",
        ));
    }
    if gateway_api_key.is_none() {
        return Err(config_error(
            "ROUTER_GATEWAY_API_KEY must be configured when router-api binds a non-loopback host",
        ));
    }
    if admin_api_key.is_none() {
        return Err(config_error(
            "ROUTER_ADMIN_API_KEY must be configured when router-api binds a non-loopback host",
        ));
    }
    Ok(())
}

fn validate_distinct_api_auth_keys(
    detector_api_key: Option<&str>,
    gateway_api_key: Option<&str>,
    admin_api_key: Option<&str>,
) -> Result<()> {
    if configured_keys_match(detector_api_key, gateway_api_key) {
        return Err(config_error(
            "ROUTER_DETECTOR_API_KEY and ROUTER_GATEWAY_API_KEY must be distinct",
        ));
    }
    if configured_keys_match(detector_api_key, admin_api_key) {
        return Err(config_error(
            "ROUTER_DETECTOR_API_KEY and ROUTER_ADMIN_API_KEY must be distinct",
        ));
    }
    if configured_keys_match(gateway_api_key, admin_api_key) {
        return Err(config_error(
            "ROUTER_GATEWAY_API_KEY and ROUTER_ADMIN_API_KEY must be distinct",
        ));
    }
    Ok(())
}

fn configured_keys_match(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left == right)
}

fn config_error(message: impl Into<String>) -> Error {
    Error::Generic {
        source: Whatever::without_source(message.into()),
    }
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
    if !scheme.eq_ignore_ascii_case("bearer")
        || !constant_time_api_key_eq(token.trim(), expected_key)
    {
        return Err(RouterServerError::Unauthorized {
            message: error_message.to_string(),
        });
    }
    Ok(())
}

fn constant_time_api_key_eq(candidate: &str, expected: &str) -> bool {
    let candidate_digest = Sha256::digest(candidate.as_bytes());
    let expected_digest = Sha256::digest(expected.as_bytes());
    let mut diff = 0_u8;
    for (candidate_byte, expected_byte) in candidate_digest.iter().zip(expected_digest.iter()) {
        diff |= candidate_byte ^ expected_byte;
    }
    diff == 0
}

fn normalized_provider_id(provider: &str) -> RouterServerResult<String> {
    let provider = provider.trim();
    if provider.is_empty() {
        return Err(RouterServerError::Validation {
            message: "provider path must not be empty".to_string(),
        });
    }
    if provider.len() > MAX_PROVIDER_POLICY_ID_LEN {
        return Err(RouterServerError::Validation {
            message: format!("provider path must be at most {MAX_PROVIDER_POLICY_ID_LEN} bytes"),
        });
    }

    let normalized = provider.to_ascii_lowercase();
    let provider_id =
        ProviderId::parse(&normalized).ok_or_else(|| RouterServerError::Validation {
            message: format!("unsupported provider path: {provider}"),
        })?;
    Ok(provider_id.as_str().to_string())
}

fn normalized_provider_policy_reason(reason: &str) -> RouterServerResult<String> {
    let reason = reason.trim();
    if reason.len() > MAX_PROVIDER_POLICY_REASON_LEN {
        return Err(RouterServerError::Validation {
            message: format!(
                "provider policy reason must be at most {MAX_PROVIDER_POLICY_REASON_LEN} bytes"
            ),
        });
    }
    Ok(reason.to_string())
}

fn normalized_provider_policy_updated_by(updated_by: Option<&str>) -> RouterServerResult<String> {
    let updated_by = updated_by.map(str::trim).filter(|value| !value.is_empty());
    let Some(updated_by) = updated_by else {
        return Ok("internal_api".to_string());
    };
    if updated_by.len() > MAX_PROVIDER_POLICY_UPDATED_BY_LEN {
        return Err(RouterServerError::Validation {
            message: format!(
                "provider policy updated_by must be at most {MAX_PROVIDER_POLICY_UPDATED_BY_LEN} bytes"
            ),
        });
    }
    Ok(updated_by.to_string())
}

fn normalize_hint_source(source: String) -> RouterServerResult<String> {
    let source = source.trim();
    if source.is_empty() {
        return Err(RouterServerError::Validation {
            message: "hint source must not be empty".to_string(),
        });
    }
    if source.len() > MAX_HINT_SOURCE_LEN {
        return Err(RouterServerError::Validation {
            message: format!("hint source must be at most {MAX_HINT_SOURCE_LEN} bytes"),
        });
    }
    if !is_protocol_token(source) {
        return Err(RouterServerError::Validation {
            message: "hint source may only contain letters, numbers, '.', '_', ':', and '-'"
                .to_string(),
        });
    }
    Ok(source.to_string())
}

fn normalize_hint_idempotency_key(
    idempotency_key: Option<String>,
) -> RouterServerResult<Option<String>> {
    let Some(idempotency_key) = idempotency_key else {
        return Ok(None);
    };
    let idempotency_key = idempotency_key.trim();
    if idempotency_key.is_empty() {
        return Err(RouterServerError::Validation {
            message: "hint idempotency_key must not be empty".to_string(),
        });
    }
    if idempotency_key.len() > MAX_HINT_IDEMPOTENCY_KEY_LEN {
        return Err(RouterServerError::Validation {
            message: format!(
                "hint idempotency_key must be at most {MAX_HINT_IDEMPOTENCY_KEY_LEN} bytes"
            ),
        });
    }
    if !is_protocol_token(idempotency_key) {
        return Err(RouterServerError::Validation {
            message:
                "hint idempotency_key may only contain letters, numbers, '.', '_', ':', and '-'"
                    .to_string(),
        });
    }
    Ok(Some(idempotency_key.to_string()))
}

fn is_protocol_token(value: &str) -> bool {
    value.bytes().all(|byte| {
        matches!(
            byte,
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'.' | b'_' | b':' | b'-'
        )
    })
}

fn normalize_hint_evidence(evidence: serde_json::Value) -> RouterServerResult<serde_json::Value> {
    if !evidence.is_object() {
        return Err(RouterServerError::Validation {
            message: "hint evidence must be a JSON object".to_string(),
        });
    }
    let encoded = serde_json::to_vec(&evidence).map_err(|source| RouterServerError::Internal {
        message: format!("failed to encode hint evidence: {source}"),
    })?;
    if encoded.len() > MAX_HINT_EVIDENCE_JSON_BYTES {
        return Err(RouterServerError::Validation {
            message: format!(
                "hint evidence must be at most {MAX_HINT_EVIDENCE_JSON_BYTES} encoded bytes"
            ),
        });
    }
    Ok(evidence)
}

async fn screen_user_address(
    state: &AppState,
    purpose: AddressScreeningPurpose,
    chain_id: &ChainId,
    address: &str,
) -> RouterServerResult<()> {
    let normalized =
        normalize_screened_address(state.chain_registry.as_ref(), purpose, chain_id, address)?;
    let Some(address_screener) = state.address_screener.as_ref() else {
        return Ok(());
    };
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

async fn get_chain_tip(
    State(state): State<AppState>,
    headers: HeaderMap,
    RouterPath(chain): RouterPath<String>,
) -> RouterServerResult<Json<serde_json::Value>> {
    authorize_internal_api_request(&state, &headers)?;
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
        quote,
        funding_vault,
        cancellation_secret: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{header::CONTENT_TYPE, HeaderValue, Request},
    };
    use router_primitives::ChainType;
    use std::{sync::Arc, time::Duration};

    #[test]
    fn create_order_request_rejects_cancellation_commitment() {
        let request = serde_json::json!({
            "quote_id": Uuid::now_v7(),
            "refund_address": "refund-address",
            "cancellation_commitment": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        });

        let error = serde_json::from_value::<CreateOrderRequest>(request)
            .expect_err("cancellation_commitment should not be accepted");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn create_order_request_rejects_cancel_after() {
        let request = serde_json::json!({
            "quote_id": Uuid::now_v7(),
            "refund_address": "refund-address",
            "cancel_after": "2026-05-06T00:00:00Z"
        });

        let error = serde_json::from_value::<CreateOrderRequest>(request)
            .expect_err("cancel_after should not be accepted for router orders");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn create_order_requires_idempotency_key() {
        let request = CreateOrderRequest {
            quote_id: Uuid::now_v7(),
            refund_address: "refund-address".to_string(),
            idempotency_key: None,
            metadata: serde_json::json!({}),
        };

        assert!(matches!(
            require_order_idempotency_key(&request),
            Err(RouterServerError::Validation { .. })
        ));
    }

    #[test]
    fn create_order_rejects_weak_idempotency_keys() {
        let request = CreateOrderRequest {
            quote_id: Uuid::now_v7(),
            refund_address: "refund-address".to_string(),
            idempotency_key: Some("short-key".to_string()),
            metadata: serde_json::json!({}),
        };

        let err = require_order_idempotency_key(&request)
            .expect_err("short idempotency key should be rejected");
        assert!(err
            .to_string()
            .contains("idempotency_key must be at least 16 bytes"));
    }

    #[test]
    fn create_order_rejects_malformed_idempotency_keys() {
        let request = CreateOrderRequest {
            quote_id: Uuid::now_v7(),
            refund_address: "refund-address".to_string(),
            idempotency_key: Some("market-order key/with/slashes".to_string()),
            metadata: serde_json::json!({}),
        };

        let err = require_order_idempotency_key(&request)
            .expect_err("non-token idempotency key should be rejected");
        assert!(err
            .to_string()
            .contains("idempotency_key may only contain letters"));
    }

    #[tokio::test]
    async fn router_json_rejection_uses_error_envelope_for_malformed_json() {
        let request = Request::builder()
            .method("POST")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("not-json"))
            .expect("request");

        let Err(response) = RouterJson::<CreateOrderRequest>::from_request(request, &()).await
        else {
            panic!("malformed JSON should be rejected");
        };
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], StatusCode::BAD_REQUEST.as_u16());
        assert_eq!(body["error"]["message"], "Malformed JSON request body");
    }

    #[tokio::test]
    async fn router_json_rejection_uses_error_envelope_for_schema_errors() {
        let request = Request::builder()
            .method("POST")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::json!({
                    "quote_id": Uuid::now_v7(),
                    "refund_address": "refund-address",
                    "ignored": true
                })
                .to_string(),
            ))
            .expect("request");

        let Err(response) = RouterJson::<CreateOrderRequest>::from_request(request, &()).await
        else {
            panic!("schema error should be rejected");
        };
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            body["error"]["code"],
            StatusCode::UNPROCESSABLE_ENTITY.as_u16()
        );
        assert_eq!(body["error"]["message"], "Invalid JSON request body");
    }

    #[tokio::test]
    async fn router_path_rejection_uses_error_envelope() {
        let request = Request::builder()
            .uri("/api/v1/orders/not-a-uuid")
            .body(Body::empty())
            .expect("request");
        let (mut parts, _body) = request.into_parts();

        let Err(response) = RouterPath::<Uuid>::from_request_parts(&mut parts, &()).await else {
            panic!("missing path params should be rejected");
        };
        let status = response.status();
        let body = to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("response body");
        let body: serde_json::Value = serde_json::from_slice(&body).expect("json body");

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            body["error"]["code"],
            StatusCode::INTERNAL_SERVER_ERROR.as_u16()
        );
        assert_eq!(body["error"]["message"], "Invalid path parameter");
    }

    #[test]
    fn cors_origin_patterns_are_anchored() {
        assert!(origin_matches_cors_pattern(
            "https://admin.rift.trade",
            "https://*.rift.trade"
        ));
        assert!(origin_matches_cors_pattern(
            "https://app.rift.trade",
            "https://*.rift.trade"
        ));
        assert!(!origin_matches_cors_pattern(
            "https://rift.trade",
            "https://*.rift.trade"
        ));
        assert!(!origin_matches_cors_pattern(
            "https://admin.rift.trade.evil.com",
            "https://*.rift.trade"
        ));
        assert!(!origin_matches_cors_pattern(
            "https://evil.com",
            "https://*.rift.trade"
        ));
    }

    #[test]
    fn cors_origin_patterns_reject_multiple_wildcards() {
        assert!(!origin_matches_cors_pattern(
            "https://admin.rift.trade",
            "https://*.rift.*"
        ));
    }

    #[test]
    fn provider_policy_path_accepts_known_providers_case_insensitively() {
        assert_eq!(
            normalized_provider_id("  HyperLiquid_Bridge  ").unwrap(),
            "hyperliquid_bridge"
        );
        assert_eq!(normalized_provider_id("VELORA").unwrap(), "velora");
    }

    #[test]
    fn provider_policy_path_rejects_empty_oversized_or_unknown_provider() {
        assert!(matches!(
            normalized_provider_id("  "),
            Err(RouterServerError::Validation { .. })
        ));

        let oversized = "a".repeat(MAX_PROVIDER_POLICY_ID_LEN + 1);
        assert!(matches!(
            normalized_provider_id(&oversized),
            Err(RouterServerError::Validation { .. })
        ));

        assert!(matches!(
            normalized_provider_id("hyperliquid-bridge"),
            Err(RouterServerError::Validation { .. })
        ));
    }

    #[test]
    fn provider_policy_text_fields_are_trimmed_defaulted_and_bounded() {
        assert_eq!(
            normalized_provider_policy_reason("  planned maintenance  ").unwrap(),
            "planned maintenance"
        );
        assert_eq!(
            normalized_provider_policy_updated_by(Some("  ops@example.com  ")).unwrap(),
            "ops@example.com"
        );
        assert_eq!(
            normalized_provider_policy_updated_by(Some("  ")).unwrap(),
            "internal_api"
        );
        assert_eq!(
            normalized_provider_policy_updated_by(None).unwrap(),
            "internal_api"
        );

        assert!(matches!(
            normalized_provider_policy_reason(&"a".repeat(MAX_PROVIDER_POLICY_REASON_LEN + 1)),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalized_provider_policy_updated_by(Some(
                &"a".repeat(MAX_PROVIDER_POLICY_UPDATED_BY_LEN + 1)
            )),
            Err(RouterServerError::Validation { .. })
        ));
    }

    #[test]
    fn hint_request_fields_are_trimmed_bounded_and_object_only() {
        assert_eq!(
            normalize_hint_source("  sauron  ".to_string()).unwrap(),
            "sauron"
        );
        assert_eq!(
            normalize_hint_source("  sauron:evm_1.detector  ".to_string()).unwrap(),
            "sauron:evm_1.detector"
        );
        assert_eq!(
            normalize_hint_idempotency_key(Some("  hint-1  ".to_string())).unwrap(),
            Some("hint-1".to_string())
        );
        assert_eq!(normalize_hint_idempotency_key(None).unwrap(), None);
        assert_eq!(
            normalize_hint_evidence(serde_json::json!({ "tx_hash": "0xabc" })).unwrap(),
            serde_json::json!({ "tx_hash": "0xabc" })
        );

        assert!(matches!(
            normalize_hint_source("  ".to_string()),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_source("a".repeat(MAX_HINT_SOURCE_LEN + 1)),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_source("sauron detector".to_string()),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_idempotency_key(Some("  ".to_string())),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_idempotency_key(Some("a".repeat(MAX_HINT_IDEMPOTENCY_KEY_LEN + 1))),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_idempotency_key(Some("hint/key".to_string())),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_evidence(serde_json::json!("not-an-object")),
            Err(RouterServerError::Validation { .. })
        ));
        assert!(matches!(
            normalize_hint_evidence(serde_json::json!({
                "payload": "a".repeat(MAX_HINT_EVIDENCE_JSON_BYTES)
            })),
            Err(RouterServerError::Validation { .. })
        ));
    }

    #[test]
    fn address_validation_is_available_without_screening_provider() {
        let mut registry = ChainRegistry::new();
        registry.register(
            ChainType::Ethereum,
            Arc::new(chains::hyperliquid::HyperliquidChain::new(
                b"test-ethereum-address-validator",
                1,
                Duration::from_secs(1),
            )),
        );
        let chain_id = ChainId::parse("evm:1").unwrap();

        assert_eq!(
            normalize_screened_address(
                &registry,
                AddressScreeningPurpose::Recipient,
                &chain_id,
                "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7",
            )
            .unwrap(),
            "0x742d35cc6634c0532925a3b844bc9e7595f0beb7"
        );
        assert!(matches!(
            normalize_screened_address(
                &registry,
                AddressScreeningPurpose::Recipient,
                &chain_id,
                "not-an-address",
            ),
            Err(RouterServerError::Validation { .. })
        ));
    }

    #[test]
    fn internal_api_auth_accepts_matching_bearer_header() {
        let auth = InternalApiAuth {
            api_key: "detector-secret-0000000000000000".to_string(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer detector-secret-0000000000000000"),
        );

        auth.verify_headers(&headers).unwrap();
    }

    #[test]
    fn internal_api_auth_rejects_missing_or_invalid_bearer_header() {
        let auth = InternalApiAuth {
            api_key: "detector-secret-0000000000000000".to_string(),
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
    fn gateway_api_auth_accepts_matching_bearer_header() {
        let auth = GatewayApiAuth {
            api_key: "gateway-secret-00000000000000000".to_string(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer gateway-secret-00000000000000000"),
        );

        auth.verify_headers(&headers).unwrap();
    }

    #[test]
    fn gateway_api_auth_rejects_missing_or_invalid_bearer_header() {
        let auth = GatewayApiAuth {
            api_key: "gateway-secret-00000000000000000".to_string(),
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
            api_key: "admin-secret-00000000000000000000".to_string(),
        };
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer admin-secret-00000000000000000000"),
        );

        auth.verify_headers(&headers).unwrap();
    }

    #[test]
    fn admin_api_auth_rejects_missing_or_invalid_bearer_header() {
        let auth = AdminApiAuth {
            api_key: "admin-secret-00000000000000000000".to_string(),
        };
        assert!(matches!(
            auth.verify_headers(&HeaderMap::new()),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut wrong_scheme = HeaderMap::new();
        wrong_scheme.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic admin-secret-00000000000000000000"),
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

    #[test]
    fn configured_api_keys_must_have_enough_entropy() {
        assert_eq!(normalize_configured_api_key(None), None);
        assert_eq!(normalize_configured_api_key(Some("   ")), None);
        assert_eq!(normalize_configured_api_key(Some("short-secret")), None);
        assert_eq!(
            normalize_configured_api_key(Some("  detector-secret-0000000000000000  ")),
            Some("detector-secret-0000000000000000")
        );
    }

    #[test]
    fn public_api_bind_requires_detector_gateway_and_admin_api_keys() {
        assert!(
            validate_bound_api_auth_config(IpAddr::from([127, 0, 0, 1]), None, None, None).is_ok()
        );
        assert!(validate_bound_api_auth_config(
            IpAddr::from([0, 0, 0, 0]),
            Some("detector-secret-0000000000000000"),
            Some("gateway-secret-00000000000000000"),
            Some("admin-secret-00000000000000000000")
        )
        .is_ok());

        let missing_detector = validate_bound_api_auth_config(
            IpAddr::from([0, 0, 0, 0]),
            None,
            Some("gateway-secret-00000000000000000"),
            Some("admin-secret-00000000000000000000"),
        )
        .expect_err("missing detector key");
        assert!(missing_detector
            .to_string()
            .contains("ROUTER_DETECTOR_API_KEY must be configured"));

        let missing_gateway = validate_bound_api_auth_config(
            IpAddr::from([0, 0, 0, 0]),
            Some("detector-secret-0000000000000000"),
            None,
            Some("admin-secret-00000000000000000000"),
        )
        .expect_err("missing gateway key");
        assert!(missing_gateway
            .to_string()
            .contains("ROUTER_GATEWAY_API_KEY must be configured"));

        let missing_admin = validate_bound_api_auth_config(
            IpAddr::from([0, 0, 0, 0]),
            Some("detector-secret-0000000000000000"),
            Some("gateway-secret-00000000000000000"),
            None,
        )
        .expect_err("missing admin key");
        assert!(missing_admin
            .to_string()
            .contains("ROUTER_ADMIN_API_KEY must be configured"));
    }

    #[test]
    fn configured_api_keys_must_be_role_distinct() {
        let shared = "shared-router-secret-0000000000000";

        let detector_gateway = validate_bound_api_auth_config(
            IpAddr::from([127, 0, 0, 1]),
            Some(shared),
            Some(shared),
            Some("admin-secret-00000000000000000000"),
        )
        .expect_err("detector and gateway keys must be distinct");
        assert!(detector_gateway
            .to_string()
            .contains("ROUTER_DETECTOR_API_KEY and ROUTER_GATEWAY_API_KEY"));

        let detector_admin = validate_bound_api_auth_config(
            IpAddr::from([127, 0, 0, 1]),
            Some(shared),
            Some("gateway-secret-00000000000000000"),
            Some(shared),
        )
        .expect_err("detector and admin keys must be distinct");
        assert!(detector_admin
            .to_string()
            .contains("ROUTER_DETECTOR_API_KEY and ROUTER_ADMIN_API_KEY"));

        let gateway_admin = validate_bound_api_auth_config(
            IpAddr::from([127, 0, 0, 1]),
            Some("detector-secret-0000000000000000"),
            Some(shared),
            Some(shared),
        )
        .expect_err("gateway and admin keys must be distinct");
        assert!(gateway_admin
            .to_string()
            .contains("ROUTER_GATEWAY_API_KEY and ROUTER_ADMIN_API_KEY"));
    }
}
