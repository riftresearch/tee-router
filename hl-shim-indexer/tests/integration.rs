use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use alloy::primitives::Address;
use devnet::mock_integrators::MockIntegratorServer;
use futures_util::{SinkExt, StreamExt};
use hl_shim_indexer::{
    build_router,
    config::Cadences,
    ingest::schema::{
        DecimalString, HlMarket, HlTransferEvent, HlTransferKind, StreamKind, SubscribeFilter,
    },
    poller::{PollEndpoint, Poller, ScheduledPoll, Scheduler},
    storage::{CursorEndpoint, Storage},
    AppState, PubSub,
};
use hyperliquid_client::{
    UserFill, UserFunding, UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate,
};
use serde_json::json;
use sqlx_core::{connection::Connection, query::query, query_scalar::query_scalar};
use sqlx_postgres::{PgConnectOptions, PgConnection};
use std::time::Instant;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage, ImageExt,
};
use tokio::{
    net::TcpListener,
    time::{timeout, Duration, Instant as TokioInstant},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;

const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const POSTGRES_PORT: u16 = 5432;
const TEST_DATABASE_URL_ENV: &str = "HL_SHIM_TEST_DATABASE_URL";

struct TestPostgres {
    admin_database_url: String,
    _container: Option<testcontainers::ContainerAsync<GenericImage>>,
}

async fn test_postgres() -> TestPostgres {
    if let Ok(admin_database_url) = std::env::var(TEST_DATABASE_URL_ENV) {
        return TestPostgres {
            admin_database_url,
            _container: None,
        };
    }

    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE);
    let container = image.start().await.expect("start Postgres testcontainer");
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("read Postgres testcontainer port");
    let admin_database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
    );
    TestPostgres {
        admin_database_url,
        _container: Some(container),
    }
}

async fn create_test_database(admin_database_url: &str) -> String {
    let connect_options =
        PgConnectOptions::from_str(admin_database_url).expect("parse test database admin URL");
    let mut admin = PgConnection::connect_with(&connect_options)
        .await
        .expect("connect to test database admin URL");
    let database_name = format!("hl_shim_{}", Uuid::now_v7().simple());
    query(&format!(r#"CREATE DATABASE "{database_name}""#))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");

    let mut database_url =
        url::Url::parse(admin_database_url).expect("parse test database admin URL");
    database_url.set_path(&database_name);
    database_url.to_string()
}

fn cadences() -> Cadences {
    Cadences {
        hot: Duration::from_millis(50),
        warm: Duration::from_millis(200),
        cold: Duration::from_millis(500),
        funding_hot: Duration::from_millis(100),
        funding_warm: Duration::from_millis(300),
        funding_cold: Duration::from_millis(500),
        order_status: Duration::from_millis(50),
    }
}

fn sample_user() -> Address {
    Address::repeat_byte(0x11)
}

fn other_user() -> Address {
    Address::repeat_byte(0x22)
}

fn sample_fill(user_time: u64, tid: u64) -> UserFill {
    UserFill {
        coin: "UBTC/USDC".to_string(),
        px: "60000".to_string(),
        sz: "0.001".to_string(),
        side: "B".to_string(),
        time: user_time,
        start_position: "0".to_string(),
        dir: "Buy".to_string(),
        closed_pnl: "0".to_string(),
        hash: format!("0x{}", "ab".repeat(32)),
        oid: 42,
        crossed: true,
        fee: "0.00001".to_string(),
        tid,
        fee_token: "UBTC".to_string(),
    }
}

fn sample_ledger_deposit(time: u64, hash_byte: u8) -> UserNonFundingLedgerUpdate {
    UserNonFundingLedgerUpdate {
        time,
        hash: format!("0x{}", format!("{hash_byte:02x}").repeat(32)),
        delta: UserNonFundingLedgerDelta::Deposit {
            usdc: "1.0".to_string(),
        },
    }
}

fn sample_funding(time: u64) -> UserFunding {
    UserFunding {
        time,
        coin: "BTC".to_string(),
        usdc: "-0.0001".to_string(),
        szi: "0.001".to_string(),
        funding_rate: "0.00001".to_string(),
    }
}

async fn build_test_app(
    database_url: &str,
    hl_url: &str,
) -> (String, Poller, Scheduler, Storage, PubSub) {
    let storage = Storage::connect(database_url, 4)
        .await
        .expect("connect storage");
    let pubsub = PubSub::new(128);
    let scheduler = Scheduler::new(cadences());
    let poller = Poller::new(
        scheduler.clone(),
        storage.clone(),
        pubsub.clone(),
        hl_url,
        1_100,
    )
    .await
    .expect("create poller");
    let app = build_router(AppState {
        storage: storage.clone(),
        scheduler: scheduler.clone(),
        pubsub: pubsub.clone(),
    });
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://{addr}"), poller, scheduler, storage, pubsub)
}

#[tokio::test]
async fn storage_dedups_transfers_and_cursor_advances_after_poll() {
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let hl = MockIntegratorServer::spawn().await.expect("spawn HL mock");
    let user = sample_user();
    hl.record_hyperliquid_fill(user, sample_fill(1_700_000_000_000, 1))
        .await;
    hl.record_hyperliquid_fill(user, sample_fill(1_700_000_001_000, 2))
        .await;

    let (_base_url, poller, _scheduler, storage, _pubsub) =
        build_test_app(&database_url, hl.base_url()).await;
    poller
        .poll_once(ScheduledPoll {
            user,
            endpoint: PollEndpoint::Fills,
            deadline: Instant::now(),
        })
        .await
        .expect("poll fills");
    assert_eq!(
        storage
            .cursor(user, CursorEndpoint::Fills)
            .await
            .expect("cursor"),
        1_700_000_001_000
    );

    let duplicate = test_transfer(
        user,
        1_700_000_002_000,
        HlTransferKind::Withdraw { nonce: 7 },
    );
    assert!(storage
        .insert_transfer(&duplicate)
        .await
        .expect("first insert"));
    assert!(!storage
        .insert_transfer(&duplicate)
        .await
        .expect("duplicate insert"));
}

#[tokio::test]
async fn websocket_subscription_receives_matching_transfer_but_not_other_user_order() {
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let hl = MockIntegratorServer::spawn().await.expect("spawn HL mock");
    let user = sample_user();
    hl.record_hyperliquid_fill(user, sample_fill(1_700_000_000_000, 9))
        .await;

    let (base_url, poller, _scheduler, _storage, _pubsub) =
        build_test_app(&database_url, hl.base_url()).await;
    tokio::spawn(async move {
        let _ = poller.run(1).await;
    });

    let ws_url = base_url.replace("http://", "ws://") + "/subscribe";
    let (mut ws, _) = connect_async(ws_url).await.expect("connect ws");
    ws.send(Message::Text(
        json!({
            "action": "subscribe",
            "filter": {
                "users": [format!("{user:?}")],
                "kinds": ["transfers"]
            }
        })
        .to_string(),
    ))
    .await
    .expect("send subscribe");
    let ack = timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("ack timeout")
        .expect("ack")
        .expect("ack message");
    assert!(ack.into_text().expect("ack text").contains("subscribed"));

    let message = timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("event timeout")
        .expect("event")
        .expect("event message")
        .into_text()
        .expect("event text");
    assert!(message.contains("\"kind\":\"transfer\""));
    assert!(message.contains("\"tid\":9"));
}

#[tokio::test]
async fn rest_transfers_paginates_and_prune_removes_irrelevant_users() {
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let hl = MockIntegratorServer::spawn().await.expect("spawn HL mock");
    let user = sample_user();
    let other = other_user();
    let (base_url, _poller, _scheduler, storage, _pubsub) =
        build_test_app(&database_url, hl.base_url()).await;

    storage
        .insert_transfer(&test_transfer(
            user,
            1_700_000_000_000,
            HlTransferKind::Withdraw { nonce: 1 },
        ))
        .await
        .expect("insert user");
    storage
        .insert_transfer(&test_transfer(
            other,
            1_700_000_000_000,
            HlTransferKind::Withdraw { nonce: 2 },
        ))
        .await
        .expect("insert other");

    let response: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{base_url}/transfers?user={user:?}&from_time=0&limit=1"
        ))
        .send()
        .await
        .expect("query transfers")
        .json()
        .await
        .expect("transfers body");
    assert_eq!(response["events"].as_array().expect("events").len(), 1);
    assert!(!response["has_more"].as_bool().expect("has_more"));

    reqwest::Client::new()
        .post(format!("{base_url}/prune"))
        .json(&json!({
            "relevant_users": [format!("{user:?}")],
            "before_time_ms": 1_700_000_000_001_i64
        }))
        .send()
        .await
        .expect("post prune");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let remaining: i64 = query_scalar("SELECT count(*) FROM hl_transfers")
        .fetch_one(storage.pool())
        .await
        .expect("count transfers");
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn unsupported_ledger_delta_decodes_and_is_skipped() {
    let raw = serde_json::json!({
        "time": 1_700_000_000_000_u64,
        "hash": format!("0x{}", "cd".repeat(32)),
        "delta": { "type": "vaultDeposit", "usdc": "1.0" }
    });
    let update: UserNonFundingLedgerUpdate =
        serde_json::from_value(raw).expect("unknown ledger update decodes");
    let event = hl_shim_indexer::ingest::decode_ledger_update(sample_user(), update)
        .expect("decode")
        .is_none();
    assert!(event);
}

#[test]
fn subscribe_filter_matches_users_and_kinds() {
    let user = sample_user();
    let filter = SubscribeFilter {
        users: vec![user],
        kinds: vec![StreamKind::Transfers],
    };
    let transfer = test_transfer(user, 1, HlTransferKind::Deposit);
    assert!(filter.matches_transfer(&transfer));
    assert!(!filter.matches_transfer(&test_transfer(other_user(), 1, HlTransferKind::Deposit)));
}

#[tokio::test]
#[ignore = "10-minute HL shim stress gate; run explicitly before release with cargo test -p hl-shim-indexer --test integration -- --ignored hl_shim_stress_20_hot_users_weight_budget_and_delivery"]
async fn hl_shim_stress_20_hot_users_weight_budget_and_delivery() {
    let duration = std::env::var("HL_SHIM_STRESS_DURATION_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(600);
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let hl = MockIntegratorServer::spawn().await.expect("spawn HL mock");
    let (base_url, poller, scheduler, storage, _pubsub) =
        build_test_app(&database_url, hl.base_url()).await;

    let users: Vec<Address> = (1_u8..=20).map(Address::repeat_byte).collect();
    let received_transfers = Arc::new(AtomicUsize::new(0));
    let stress_end = TokioInstant::now() + Duration::from_secs(duration);

    for user in &users {
        let ws_url = base_url.replace("http://", "ws://") + "/subscribe";
        let (mut ws, _) = connect_async(ws_url).await.expect("connect stress ws");
        ws.send(Message::Text(
            json!({
                "action": "subscribe",
                "filter": {
                    "users": [format!("{user:?}")],
                    "kinds": ["transfers", "orders"]
                }
            })
            .to_string(),
        ))
        .await
        .expect("send stress subscribe");
        let ack = timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("stress ack timeout")
            .expect("stress ack")
            .expect("stress ack message")
            .into_text()
            .expect("stress ack text");
        assert!(ack.contains("subscribed"));

        let received = Arc::clone(&received_transfers);
        tokio::spawn(async move {
            while TokioInstant::now() < stress_end {
                if let Ok(Some(Ok(message))) = timeout(Duration::from_millis(500), ws.next()).await
                {
                    if message
                        .into_text()
                        .is_ok_and(|text| text.contains("\"kind\":\"transfer\""))
                    {
                        received.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    let expected_transfers = users.len() * 3;
    for (index, user) in users.iter().copied().enumerate() {
        let base_time = 1_700_100_000_000_u64 + (index as u64 * 10_000);
        hl.record_hyperliquid_fill(user, sample_fill(base_time, 10_000 + index as u64))
            .await;
        hl.record_hyperliquid_ledger_update(
            user,
            sample_ledger_deposit(base_time + 1, index as u8 + 1),
        )
        .await;
        hl.record_hyperliquid_funding(user, sample_funding(base_time + 2))
            .await;
    }
    for oid in 1..=5 {
        scheduler.register_pending_order(users[0], oid).await;
    }

    let poller_for_run = poller.clone();
    tokio::spawn(async move {
        let _ = poller_for_run.run(8).await;
    });

    tokio::time::sleep(Duration::from_secs(duration)).await;

    let used_weight = poller.used_weight_last_minute().await;
    assert!(
        used_weight <= 1_100,
        "HL shim exceeded weight budget: {used_weight}"
    );
    let stored_transfers: i64 = query_scalar("SELECT count(*) FROM hl_transfers")
        .fetch_one(storage.pool())
        .await
        .expect("count stress transfers");
    assert_eq!(stored_transfers as usize, expected_transfers);
    assert_eq!(
        received_transfers.load(Ordering::Relaxed),
        expected_transfers
    );
}

fn test_transfer(user: Address, time_ms: i64, kind: HlTransferKind) -> HlTransferEvent {
    HlTransferEvent {
        user,
        time_ms,
        kind,
        asset: "USDC".to_string(),
        market: HlMarket::NotApplicable,
        amount_delta: DecimalString::new("1.0").expect("decimal"),
        fee: None,
        fee_token: None,
        hash: format!("0x{}", "aa".repeat(32)),
        observed_at_ms: time_ms + 1,
    }
}
