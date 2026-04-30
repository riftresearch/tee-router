use std::{net::IpAddr, net::SocketAddr};

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap},
    routing::get,
    Json, Router,
};
use clap::Parser;
use router_server::{
    db::Database,
    error::{RouterServerError, RouterServerResult},
    query_api, Error, Result,
};
use serde_json::json;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use uuid::Uuid;

#[derive(Parser, Clone)]
#[command(about = "Router query API backed by a router database replica")]
struct RouterQueryApiArgs {
    /// Host to bind to
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: IpAddr,

    /// Port to bind to
    #[arg(short, long, default_value = "4523")]
    port: u16,

    /// Database URL
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://router_user:router_password@localhost:5432/router_db"
    )]
    database_url: String,

    /// Database max connections
    #[arg(long, env = "DB_MAX_CONNECTIONS", default_value = "16")]
    db_max_connections: u32,

    /// Database min connections
    #[arg(long, env = "DB_MIN_CONNECTIONS", default_value = "2")]
    db_min_connections: u32,

    /// Log level
    #[arg(long, env = "RUST_LOG", default_value = "info")]
    log_level: String,

    /// Optional bearer token required by query endpoints
    #[arg(long, env = "ROUTER_QUERY_API_KEY")]
    query_api_key: Option<String>,
}

#[derive(Clone)]
struct QueryApiState {
    db: Database,
    query_api_key: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = RouterQueryApiArgs::parse();
    init_tracing(&args);
    run(args).await
}

async fn run(args: RouterQueryApiArgs) -> Result<()> {
    let db = Database::connect(
        &args.database_url,
        args.db_max_connections,
        args.db_min_connections,
    )
    .await
    .map_err(|source| Error::DatabaseInit { source })?;

    let addr = SocketAddr::from((args.host, args.port));
    let app = Router::new()
        .route("/status", get(status_handler))
        .route("/internal/v1/orders/:id/flow", get(get_order_flow))
        .with_state(QueryApiState {
            db,
            query_api_key: args
                .query_api_key
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string),
        })
        .layer(
            TraceLayer::new_for_http().make_span_with(
                DefaultMakeSpan::new()
                    .level(Level::INFO)
                    .include_headers(false),
            ),
        );

    info!("Listening on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|source| Error::ServerBind { source })?;

    axum::serve(listener, app)
        .with_graceful_shutdown(blockchain_utils::shutdown_signal())
        .await
        .map_err(|source| Error::ServerStart { source })
}

async fn status_handler() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "service": "router-query-api",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn get_order_flow(
    State(state): State<QueryApiState>,
    headers: HeaderMap,
    Path(id): Path<Uuid>,
) -> RouterServerResult<Json<router_server::api::OrderFlowEnvelope>> {
    authorize_query_request(&state, &headers)?;
    Ok(Json(query_api::get_order_flow(&state.db, id).await?))
}

fn authorize_query_request(state: &QueryApiState, headers: &HeaderMap) -> RouterServerResult<()> {
    let Some(expected_key) = state.query_api_key.as_deref() else {
        return Ok(());
    };

    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return Err(RouterServerError::Unauthorized {
            message: "missing query API authorization".to_string(),
        });
    };
    let Some(value) = value.to_str().ok() else {
        return Err(RouterServerError::Unauthorized {
            message: "invalid query API authorization".to_string(),
        });
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return Err(RouterServerError::Unauthorized {
            message: "invalid query API authorization".to_string(),
        });
    };
    if token != expected_key {
        return Err(RouterServerError::Unauthorized {
            message: "invalid query API authorization".to_string(),
        });
    }

    Ok(())
}

fn init_tracing(args: &RouterQueryApiArgs) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(EnvFilter::new(&args.log_level));

    tracing_subscriber::registry().with(fmt_layer).init();
}
