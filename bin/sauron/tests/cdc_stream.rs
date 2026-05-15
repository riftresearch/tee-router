use std::time::Duration;

use pgwire_replication::ReplicationEvent;
use sauron::cdc::{
    connect_stream, parse_router_cdc_message, recv_stream_event, CdcConfig, RouterCdcRepository,
    ROUTER_CDC_MESSAGE_PREFIX,
};
use sqlx_postgres::PgPoolOptions;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use uuid::Uuid;

const POSTGRES_PORT: u16 = 5432;
const ROUTER_CDC_MIGRATION: &str = include_str!(
    "../../../crates/router-core/migrations/20260502120000_router_cdc_logical_messages.sql"
);
const ROUTER_CDC_DELETE_SAFE_MIGRATION: &str = include_str!(
    "../../../crates/router-core/migrations/20260504183000_router_cdc_delete_safe.sql"
);
const ROUTER_CDC_MESSAGE_ONLY_PUBLICATION_MIGRATION: &str = include_str!(
    "../../../crates/router-core/migrations/20260504190000_router_cdc_message_only_publication.sql"
);

#[tokio::test]
async fn streams_router_cdc_messages_from_pgoutput_slot() {
    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "postgres")
        .with_env_var("POSTGRES_PASSWORD", "password")
        .with_env_var("POSTGRES_DB", "postgres")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
        ]);

    let container = image.start().await.expect("start Postgres testcontainer");
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("read Postgres port");
    let database_url = format!("postgres://postgres:password@127.0.0.1:{port}/postgres");
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .expect("connect to Postgres");

    install_minimal_router_schema(&pool).await;
    sqlx_core::raw_sql::raw_sql(ROUTER_CDC_MIGRATION)
        .execute(&pool)
        .await
        .expect("install router CDC migration");
    sqlx_core::raw_sql::raw_sql(ROUTER_CDC_DELETE_SAFE_MIGRATION)
        .execute(&pool)
        .await
        .expect("install latest router CDC trigger migration");
    sqlx_core::raw_sql::raw_sql(ROUTER_CDC_MESSAGE_ONLY_PUBLICATION_MIGRATION)
        .execute(&pool)
        .await
        .expect("install message-only CDC publication migration");

    let publication_table_count: i64 = sqlx_core::query_scalar::query_scalar(
        "SELECT COUNT(*) FROM pg_catalog.pg_publication_tables WHERE pubname = 'router_cdc_publication'",
    )
    .fetch_one(&pool)
    .await
    .expect("count CDC publication tables");
    assert_eq!(publication_table_count, 0);

    let slot_name = format!("sauron_smoke_{}", Uuid::now_v7().simple());
    let repository = RouterCdcRepository::new(
        pool.clone(),
        pool.clone(),
        CdcConfig {
            database_url,
            slot_name,
            publication_name: "router_cdc_publication".to_string(),
            message_prefix: ROUTER_CDC_MESSAGE_PREFIX.to_string(),
            status_interval: Duration::from_secs(1),
            idle_wakeup_interval: Duration::from_secs(10),
        },
    );
    repository.ensure_slot().await.expect("ensure CDC slot");

    let mut client = connect_stream(
        repository
            .replication_config()
            .expect("build replication config"),
    )
    .await
    .expect("connect replication stream");

    let order_id = Uuid::now_v7();
    sqlx_core::query::query("INSERT INTO public.router_orders (id) VALUES ($1)")
        .bind(order_id)
        .execute(&pool)
        .await
        .expect("insert router order");

    let (message, raw_xlog_events) = tokio::time::timeout(Duration::from_secs(10), async {
        let mut message = None;
        let mut raw_xlog_events = 0usize;
        loop {
            match recv_stream_event(&mut client).await.expect("stream event") {
                Some(ReplicationEvent::Message {
                    prefix, content, ..
                }) if prefix == ROUTER_CDC_MESSAGE_PREFIX => {
                    message = Some(parse_router_cdc_message(&content).expect("parse CDC payload"));
                }
                Some(ReplicationEvent::Commit { end_lsn, .. }) => {
                    client.update_applied_lsn(end_lsn);
                    if let Some(message) = message {
                        break (message, raw_xlog_events);
                    }
                }
                Some(ReplicationEvent::XLogData { .. }) => raw_xlog_events += 1,
                Some(_) => {}
                None => panic!("replication stream ended"),
            }
        }
    })
    .await
    .expect("receive router CDC message");

    assert_eq!(message.table, "router_orders");
    assert_eq!(message.op, "INSERT");
    assert_eq!(message.order_id, Some(order_id));
    assert_eq!(raw_xlog_events, 0);
}

async fn install_minimal_router_schema(pool: &sqlx_postgres::PgPool) {
    for statement in [
        "CREATE TABLE public.router_orders (id uuid PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.market_order_quotes (id uuid PRIMARY KEY, order_id uuid, created_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.market_order_actions (order_id uuid PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.order_execution_steps (id uuid PRIMARY KEY, order_id uuid, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.order_provider_operations (id uuid PRIMARY KEY, order_id uuid, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.order_provider_addresses (id uuid PRIMARY KEY, order_id uuid, provider_operation_id uuid, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.deposit_vaults (id uuid PRIMARY KEY, updated_at timestamptz NOT NULL DEFAULT now())",
        "CREATE TABLE public.custody_vaults (id uuid PRIMARY KEY, order_id uuid, updated_at timestamptz NOT NULL DEFAULT now())",
    ] {
        sqlx_core::query::query(statement)
            .execute(pool)
            .await
            .expect("create minimal router table");
    }
}
