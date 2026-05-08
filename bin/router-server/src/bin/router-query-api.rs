use std::{net::IpAddr, net::SocketAddr};

use axum::{
    extract::{Path, State},
    http::{header, HeaderMap},
    routing::get,
    Json, Router,
};
use clap::Parser;
use router_core::db::Database;
use router_server::{
    error::{RouterServerError, RouterServerResult},
    query_api, Error, Result,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use snafu::{FromString, Whatever};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing::{info, Level};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};
use uuid::Uuid;

const MIN_QUERY_API_KEY_LEN: usize = 32;

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
    let query_api_key = configured_query_api_key(&args)?;
    validate_query_api_binding(args.host, query_api_key.as_deref())?;

    let db = Database::connect(
        &args.database_url,
        args.db_max_connections,
        args.db_min_connections,
    )
    .await
    .map_err(|source| Error::DatabaseInit {
        source: source.into(),
    })?;

    let addr = SocketAddr::from((args.host, args.port));
    let app = Router::new()
        .route("/status", get(status_handler))
        .route("/internal/v1/orders/:id/flow", get(get_order_flow))
        .with_state(QueryApiState { db, query_api_key })
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

    verify_query_api_key(headers, expected_key)
}

fn verify_query_api_key(headers: &HeaderMap, expected_key: &str) -> RouterServerResult<()> {
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
    let Some((scheme, token)) = value.split_once(' ') else {
        return Err(RouterServerError::Unauthorized {
            message: "invalid query API authorization".to_string(),
        });
    };
    if !scheme.eq_ignore_ascii_case("bearer")
        || !constant_time_query_api_key_eq(token.trim(), expected_key)
    {
        return Err(RouterServerError::Unauthorized {
            message: "invalid query API authorization".to_string(),
        });
    }

    Ok(())
}

fn configured_query_api_key(args: &RouterQueryApiArgs) -> Result<Option<String>> {
    let Some(value) = args.query_api_key.as_deref() else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.len() < MIN_QUERY_API_KEY_LEN {
        return Err(config_error(
            "ROUTER_QUERY_API_KEY must be at least 32 characters",
        ));
    }
    Ok(Some(value.to_string()))
}

fn validate_query_api_binding(host: IpAddr, query_api_key: Option<&str>) -> Result<()> {
    if query_api_key.is_none() && !host.is_loopback() {
        return Err(config_error(
            "ROUTER_QUERY_API_KEY must be configured when router-query-api binds a non-loopback host",
        ));
    }
    Ok(())
}

fn constant_time_query_api_key_eq(candidate: &str, expected: &str) -> bool {
    let candidate_digest = Sha256::digest(candidate.as_bytes());
    let expected_digest = Sha256::digest(expected.as_bytes());
    let mut diff = 0_u8;
    for (candidate_byte, expected_byte) in candidate_digest.iter().zip(expected_digest.iter()) {
        diff |= candidate_byte ^ expected_byte;
    }
    diff == 0
}

fn config_error(message: impl Into<String>) -> Error {
    Error::Generic {
        source: Whatever::without_source(message.into()),
    }
}

fn init_tracing(args: &RouterQueryApiArgs) {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_line_number(true)
        .with_filter(EnvFilter::new(&args.log_level));

    tracing_subscriber::registry().with(fmt_layer).init();
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn test_args(host: IpAddr, query_api_key: Option<&str>) -> RouterQueryApiArgs {
        RouterQueryApiArgs {
            host,
            port: 4523,
            database_url: "postgres://router_user:router_password@localhost:5432/router_db"
                .to_string(),
            db_max_connections: 16,
            db_min_connections: 2,
            log_level: "info".to_string(),
            query_api_key: query_api_key.map(str::to_string),
        }
    }

    #[test]
    fn configured_query_api_key_rejects_short_nonblank_keys() {
        let args = test_args(IpAddr::from([127, 0, 0, 1]), Some("short-key"));

        let error = configured_query_api_key(&args).expect_err("short key");

        assert!(error
            .to_string()
            .contains("ROUTER_QUERY_API_KEY must be at least 32 characters"));
    }

    #[test]
    fn configured_query_api_key_trims_and_accepts_long_keys() {
        let args = test_args(
            IpAddr::from([127, 0, 0, 1]),
            Some("  router-query-key-000000000000000  "),
        );

        assert_eq!(
            configured_query_api_key(&args).unwrap(),
            Some("router-query-key-000000000000000".to_string())
        );
    }

    #[test]
    fn non_loopback_bind_requires_query_api_key() {
        let error =
            validate_query_api_binding(IpAddr::from([0, 0, 0, 0]), None).expect_err("no key");

        assert!(error
            .to_string()
            .contains("ROUTER_QUERY_API_KEY must be configured"));
        validate_query_api_binding(
            IpAddr::from([0, 0, 0, 0]),
            Some("router-query-key-000000000000000"),
        )
        .unwrap();
        validate_query_api_binding(IpAddr::from([127, 0, 0, 1]), None).unwrap();
    }

    #[test]
    fn query_api_key_verifier_accepts_matching_bearer_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("bearer router-query-key-000000000000000"),
        );

        verify_query_api_key(&headers, "router-query-key-000000000000000").unwrap();
    }

    #[test]
    fn query_api_key_verifier_rejects_missing_or_invalid_bearer_header() {
        assert!(matches!(
            verify_query_api_key(&HeaderMap::new(), "router-query-key-000000000000000"),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut wrong_scheme = HeaderMap::new();
        wrong_scheme.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic router-query-key-000000000000000"),
        );
        assert!(matches!(
            verify_query_api_key(&wrong_scheme, "router-query-key-000000000000000"),
            Err(RouterServerError::Unauthorized { .. })
        ));

        let mut wrong_key = HeaderMap::new();
        wrong_key.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong-router-query-key-0000000"),
        );
        assert!(matches!(
            verify_query_api_key(&wrong_key, "router-query-key-000000000000000"),
            Err(RouterServerError::Unauthorized { .. })
        ));
    }
}
