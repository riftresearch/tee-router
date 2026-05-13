use std::{
    collections::HashSet,
    future::Future,
    net::{IpAddr, SocketAddr, TcpStream},
    path::Path,
    process::Command,
    str::FromStr,
    time::{Duration, Instant},
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{ext::AnvilApi, Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
    sol,
};
use bitcoin::{Address as BitcoinAddress, Amount};
use bitcoin_indexer::{
    build_router as build_bitcoin_indexer_router, AppState as BitcoinIndexerAppState,
    BitcoinIndexer, Config as BitcoinIndexerConfig, IndexerPubSub as BitcoinIndexerPubSub,
};
use bitcoin_receipt_watcher::{
    build_router as build_bitcoin_receipt_watcher_router,
    AppState as BitcoinReceiptWatcherAppState, Config as BitcoinReceiptWatcherConfig,
    PendingWatches as BitcoinReceiptPendingWatches, ReceiptPubSub as BitcoinReceiptPubSub,
    Watcher as BitcoinReceiptWatcher,
};
use bitcoincore_rpc_async::Auth;
use devnet::{
    mock_integrators::{MockIntegratorConfig, MockIntegratorServer},
    RiftDevnet,
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use evm_receipt_watcher::{
    build_router as build_evm_receipt_watcher_router, AppState as EvmReceiptWatcherAppState,
    Config as EvmReceiptWatcherConfig, PendingWatches as EvmReceiptPendingWatches,
    ReceiptPubSub as EvmReceiptPubSub, Watcher as EvmReceiptWatcher,
};
use hl_shim_indexer::{
    build_router as build_hl_shim_router,
    config::Cadences as HlShimCadences,
    poller::{Poller as HlShimPoller, Scheduler as HlShimScheduler},
    storage::Storage as HlShimStorage,
    AppState as HlShimAppState, PubSub as HlShimPubSub,
};
use router_core::{
    models::{
        CustodyVaultRole, DepositVault, OrderProviderOperation, ProviderOperationStatus,
        ProviderOperationType, RouterOrderEnvelope, RouterOrderQuoteEnvelope, RouterOrderStatus,
    },
    protocol::AssetId,
    services::custody_action_executor::HyperliquidCallNetwork,
};
use router_server::{
    api::OrderFlowEnvelope, server::run_api, worker::run_worker, RouterServerArgs,
};
use router_temporal::DEFAULT_TASK_QUEUE;
use sauron::{run as run_sauron, SauronArgs};
use serde_json::{json, Value};
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection};
use temporal_worker::{
    order_execution::{activities::OrderActivities, build_worker},
    production::OrderWorkerRuntimeArgs,
    runtime::TemporalConnection,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::{
    net::TcpListener,
    sync::oneshot,
    task::{JoinHandle, LocalSet},
};
use uuid::Uuid;

const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ETHEREUM_USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const TESTNET_HYPERLIQUID_BRIDGE_ADDRESS: &str = "0x08cfc1b6b2dcf36a1480b99353a354aa8ac56f89";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";
const ROUTER_DETECTOR_API_KEY: &str = "router-e2e-detector-secret-000000";
const ROUTER_ADMIN_API_KEY: &str = "router-e2e-admin-secret-00000000";
const ACROSS_API_KEY: &str = "router-e2e-across-secret";
const ACROSS_INTEGRATOR_ID: &str = "router-e2e";
const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 1_000_000_000_000_000_000;
const BITCOIN_FEE_BUFFER_SATS: u64 = 10_000;

#[derive(Clone, Copy)]
struct RouteAsset {
    chain: &'static str,
    asset: &'static str,
}

impl RouteAsset {
    const fn native(chain: &'static str) -> Self {
        Self {
            chain,
            asset: "native",
        }
    }

    const fn token(chain: &'static str, asset: &'static str) -> Self {
        Self { chain, asset }
    }

    fn to_json(self) -> Value {
        json!({
            "chain": self.chain,
            "asset": self.asset
        })
    }
}

#[derive(Clone, Copy)]
struct RouteCase {
    name: &'static str,
    from: RouteAsset,
    to: RouteAsset,
    amount_in: &'static str,
    expected_provider_fragments: &'static [&'static str],
}

#[derive(Clone, Copy)]
struct LimitRouteCase {
    name: &'static str,
    from: RouteAsset,
    to: RouteAsset,
    input_amount: &'static str,
    output_amount: &'static str,
    expected_provider_fragments: &'static [&'static str],
}

const ROUTE_CASES: &[RouteCase] = &[
    RouteCase {
        name: "eth_to_bitcoin",
        from: RouteAsset::native("evm:1"),
        to: RouteAsset::native("bitcoin"),
        amount_in: "60000000000000000",
        expected_provider_fragments: &["unit_deposit:unit", "unit_withdrawal:unit"],
    },
    RouteCase {
        name: "bitcoin_to_eth",
        from: RouteAsset::native("bitcoin"),
        to: RouteAsset::native("evm:1"),
        amount_in: "10000000",
        expected_provider_fragments: &["unit_deposit:unit", "unit_withdrawal:unit"],
    },
    RouteCase {
        name: "bitcoin_to_usdc",
        from: RouteAsset::native("bitcoin"),
        to: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        amount_in: "10000000",
        expected_provider_fragments: &["unit_deposit:unit", "universal_router_swap:velora"],
    },
    RouteCase {
        name: "eth_to_usdc_single_chain",
        from: RouteAsset::native("evm:8453"),
        to: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        amount_in: "60000000000000000",
        expected_provider_fragments: &["universal_router_swap:velora"],
    },
    RouteCase {
        name: "usdc_to_eth_single_chain",
        from: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        to: RouteAsset::native("evm:8453"),
        amount_in: "60000000",
        expected_provider_fragments: &["universal_router_swap:velora"],
    },
    RouteCase {
        name: "eth_to_usdc_cross_chain",
        from: RouteAsset::native("evm:8453"),
        to: RouteAsset::token("evm:1", ETHEREUM_USDC_ADDRESS),
        amount_in: "60000000000000000",
        expected_provider_fragments: &["across_bridge:across", "universal_router_swap:velora"],
    },
    RouteCase {
        name: "usdc_to_eth_cross_chain",
        from: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        to: RouteAsset::native("evm:1"),
        amount_in: "60000000",
        expected_provider_fragments: &[
            "cctp_bridge:cctp",
            "hyperliquid_bridge_deposit:hyperliquid_bridge",
            "hyperliquid_trade:hyperliquid",
            "unit_withdrawal:unit",
        ],
    },
];

const LIMIT_ROUTE_CASES: &[LimitRouteCase] = &[
    LimitRouteCase {
        name: "base_usdc_to_bitcoin_limit",
        from: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        to: RouteAsset::native("bitcoin"),
        input_amount: "100000000",
        output_amount: "160000",
        expected_provider_fragments: &["hyperliquid_trade:hyperliquid", "unit_withdrawal:unit"],
    },
    LimitRouteCase {
        name: "bitcoin_to_arbitrum_usdc_limit",
        from: RouteAsset::native("bitcoin"),
        to: RouteAsset::token("evm:42161", ARBITRUM_USDC_ADDRESS),
        input_amount: "200000",
        output_amount: "100000000",
        expected_provider_fragments: &["unit_deposit:unit", "hyperliquid_trade:hyperliquid"],
    },
    LimitRouteCase {
        name: "ethereum_eth_to_base_usdc_limit",
        from: RouteAsset::native("evm:1"),
        to: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        input_amount: "50000000000000000",
        output_amount: "125000000",
        expected_provider_fragments: &["unit_deposit:unit", "hyperliquid_trade:hyperliquid"],
    },
    LimitRouteCase {
        name: "base_usdc_to_ethereum_eth_limit",
        from: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        to: RouteAsset::native("evm:1"),
        input_amount: "150000000",
        output_amount: "40000000000000000",
        expected_provider_fragments: &["hyperliquid_trade:hyperliquid", "unit_withdrawal:unit"],
    },
];

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
        function transfer(address recipient, uint256 amount) external returns (bool);
    }
}

struct TestPostgres {
    admin_database_url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

impl TestPostgres {
    async fn shutdown(&mut self) {
        if let Some(container) = self._container.take() {
            container.rm().await.expect("remove Postgres testcontainer");
        }
    }
}

struct RuntimeTask {
    handle: Option<JoinHandle<()>>,
    shutdown: Option<Box<dyn FnOnce()>>,
}

struct RouterRuntimeHarness {
    client: reqwest::Client,
    router_base_url: String,
    devnet: RiftDevnet,
    mocks: MockIntegratorServer,
    _postgres: TestPostgres,
    _config_dir: tempfile::TempDir,
    _api_task: RuntimeTask,
    _worker_task: RuntimeTask,
    _temporal_worker_task: RuntimeTask,
    _hl_shim_task: RuntimeTask,
    _receipt_watcher_tasks: Vec<RuntimeTask>,
    _sauron_task: RuntimeTask,
}

impl Drop for RuntimeTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl RuntimeTask {
    fn new(handle: JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
            shutdown: None,
        }
    }

    fn with_shutdown(handle: JoinHandle<()>, shutdown: impl FnOnce() + 'static) -> Self {
        Self {
            handle: Some(handle),
            shutdown: Some(Box::new(shutdown)),
        }
    }

    fn is_finished(&self) -> bool {
        self.handle
            .as_ref()
            .is_none_or(tokio::task::JoinHandle::is_finished)
    }

    async fn abort_and_join(&mut self) {
        let Some(mut handle) = self.handle.take() else {
            return;
        };
        if let Some(shutdown) = self.shutdown.take() {
            shutdown();
            tokio::select! {
                _ = &mut handle => {}
                _ = tokio::time::sleep(Duration::from_secs(5)) => {
                    handle.abort();
                    let _ = handle.await;
                }
            }
        } else {
            handle.abort();
            let _ = handle.await;
        }
    }
}

impl RouterRuntimeHarness {
    async fn shutdown(mut self) {
        self._sauron_task.abort_and_join().await;
        self._hl_shim_task.abort_and_join().await;
        for task in &mut self._receipt_watcher_tasks {
            task.abort_and_join().await;
        }
        self._worker_task.abort_and_join().await;
        self._temporal_worker_task.abort_and_join().await;
        self._api_task.abort_and_join().await;
        self._postgres.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "integration: spawns devnet stack"]
async fn router_api_worker_sauron_complete_mock_route_matrix() {
    LocalSet::new()
        .run_until(async {
            let runtime = spawn_mock_router_runtime().await;
            for route_case in ROUTE_CASES {
                run_route_case(
                    &runtime.client,
                    &runtime.router_base_url,
                    &runtime.devnet,
                    &runtime.mocks,
                    *route_case,
                )
                .await;
            }
            runtime.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "integration: spawns devnet stack"]
async fn router_api_worker_sauron_complete_mock_limit_order_matrix() {
    LocalSet::new()
        .run_until(async {
            let runtime = spawn_mock_router_runtime().await;
            for route_case in LIMIT_ROUTE_CASES {
                run_limit_route_case(
                    &runtime.client,
                    &runtime.router_base_url,
                    &runtime.devnet,
                    &runtime.mocks,
                    *route_case,
                )
                .await;
            }
            runtime.shutdown().await;
        })
        .await;
}

async fn spawn_mock_router_runtime() -> RouterRuntimeHarness {
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let sauron_state_database_url = create_test_database(&postgres.admin_database_url).await;
    let hl_shim_database_url = create_test_database(&postgres.admin_database_url).await;
    let (devnet, _) = RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(database_url.clone())
        .build()
        .await
        .expect("devnet setup failed");
    install_mock_usdc_clone(
        &devnet.ethereum,
        ETHEREUM_USDC_ADDRESS
            .parse()
            .expect("valid Ethereum USDC address"),
    )
    .await;
    install_mock_usdc_clone(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
    )
    .await;
    install_mock_usdc_clone(
        &devnet.arbitrum,
        ARBITRUM_USDC_ADDRESS
            .parse()
            .expect("valid Arbitrum USDC address"),
    )
    .await;

    let mocks = spawn_runtime_mocks(&devnet).await;
    let (hl_shim_base_url, _hl_shim_task) =
        spawn_hl_shim_indexer(&hl_shim_database_url, mocks.base_url()).await;
    let (ethereum_receipt_watcher_url, ethereum_receipt_watcher_task) = spawn_evm_receipt_watcher(
        "ethereum",
        devnet.ethereum.anvil.endpoint_url().to_string(),
        devnet.ethereum.anvil.ws_endpoint_url().to_string(),
    )
    .await;
    let (base_receipt_watcher_url, base_receipt_watcher_task) = spawn_evm_receipt_watcher(
        "base",
        devnet.base.anvil.endpoint_url().to_string(),
        devnet.base.anvil.ws_endpoint_url().to_string(),
    )
    .await;
    let (arbitrum_receipt_watcher_url, arbitrum_receipt_watcher_task) = spawn_evm_receipt_watcher(
        "arbitrum",
        devnet.arbitrum.anvil.endpoint_url().to_string(),
        devnet.arbitrum.anvil.ws_endpoint_url().to_string(),
    )
    .await;
    let _receipt_watcher_tasks = vec![
        ethereum_receipt_watcher_task,
        base_receipt_watcher_task,
        arbitrum_receipt_watcher_task,
    ];
    let (bitcoin_observer_urls, bitcoin_observer_tasks) = spawn_bitcoin_observers(&devnet).await;
    let mut _receipt_watcher_tasks = _receipt_watcher_tasks;
    _receipt_watcher_tasks.extend(bitcoin_observer_tasks);
    let config_dir = tempfile::tempdir().expect("router config dir");
    let args = router_args(&devnet, config_dir.path(), database_url.clone(), &mocks);

    ensure_temporal_up();
    let (router_base_url, _api_task) = spawn_router_api(args.clone()).await;
    let _temporal_worker_task = spawn_temporal_order_worker(args.clone()).await;
    let _worker_task = spawn_router_worker(args.clone()).await;
    let _sauron_task = spawn_sauron(
        &devnet,
        &database_url,
        &sauron_state_database_url,
        &router_base_url,
        &hl_shim_base_url,
        EvmReceiptWatcherUrls {
            ethereum: ethereum_receipt_watcher_url,
            base: base_receipt_watcher_url,
            arbitrum: arbitrum_receipt_watcher_url,
        },
        bitcoin_observer_urls,
        mocks.base_url().to_string(),
    )
    .await;

    RouterRuntimeHarness {
        client: reqwest::Client::new(),
        router_base_url,
        devnet,
        mocks,
        _postgres: postgres,
        _config_dir: config_dir,
        _api_task,
        _worker_task,
        _temporal_worker_task,
        _hl_shim_task,
        _receipt_watcher_tasks,
        _sauron_task,
    }
}

async fn test_postgres() -> TestPostgres {
    if let Ok(admin_database_url) = std::env::var(ROUTER_TEST_DATABASE_URL_ENV) {
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
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
        ]);

    let container = image.start().await.unwrap_or_else(|err| {
        panic!(
            "failed to start Postgres testcontainer; ensure Docker is running or set {ROUTER_TEST_DATABASE_URL_ENV}: {err}"
        )
    });
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
    let database_name = format!("router_runtime_e2e_{}", Uuid::now_v7().simple());

    sqlx_core::query::query(&format!(r#"CREATE DATABASE "{database_name}""#))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");

    let mut database_url =
        url::Url::parse(admin_database_url).expect("parse test database admin URL");
    database_url.set_path(&database_name);
    database_url.to_string()
}

async fn spawn_router_api(mut args: RouterServerArgs) -> (String, RuntimeTask) {
    args.host = IpAddr::from([127, 0, 0, 1]);
    args.port = reserve_local_port().await;
    let base_url = format!("http://{}:{}", args.host, args.port);
    let status_url = format!("{base_url}/status");
    let handle = tokio::spawn(async move {
        run_api(args)
            .await
            .expect("router API should keep running until test aborts");
    });
    let client = reqwest::Client::new();
    wait_until("router API status", Duration::from_secs(30), || {
        let client = client.clone();
        let status_url = status_url.clone();
        async move {
            client
                .get(&status_url)
                .send()
                .await
                .ok()
                .filter(|response| response.status().is_success())
                .map(|_| ())
        }
    })
    .await;

    (base_url, RuntimeTask::new(handle))
}

async fn spawn_router_worker(args: RouterServerArgs) -> RuntimeTask {
    let handle = tokio::spawn(async move {
        run_worker(args)
            .await
            .expect("router worker should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let task = RuntimeTask::new(handle);
    assert!(!task.is_finished(), "router worker exited during startup");
    task
}

async fn spawn_temporal_order_worker(args: RouterServerArgs) -> RuntimeTask {
    let runtime_args = order_worker_runtime_args_from_router_args(&args);
    let connection = TemporalConnection {
        temporal_address: args.temporal_address.clone(),
        namespace: args.temporal_namespace.clone(),
    };
    let task_queue = args.temporal_task_queue.clone();
    let order_activities = runtime_args
        .build_order_activities()
        .await
        .expect("build temporal-worker order activities");
    let mut built = build_worker(
        &connection,
        &task_queue,
        OrderActivities::new(order_activities),
    )
    .await
    .expect("build temporal order worker");
    let shutdown = built.worker.shutdown_handle();
    let handle = tokio::task::spawn_local(async move {
        built
            .worker
            .run()
            .await
            .expect("temporal order worker should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let task = RuntimeTask::with_shutdown(handle, shutdown);
    assert!(
        !task.is_finished(),
        "temporal order worker exited during startup"
    );
    task
}

fn order_worker_runtime_args_from_router_args(args: &RouterServerArgs) -> OrderWorkerRuntimeArgs {
    OrderWorkerRuntimeArgs {
        database_url: args.database_url.clone(),
        db_max_connections: args.db_max_connections,
        db_min_connections: args.db_min_connections,
        master_key_path: args.master_key_path.clone(),
        ethereum_mainnet_rpc_url: args.ethereum_mainnet_rpc_url.clone(),
        ethereum_reference_token: args.ethereum_reference_token.clone(),
        ethereum_paymaster_private_key: args.ethereum_paymaster_private_key.clone(),
        base_rpc_url: args.base_rpc_url.clone(),
        base_reference_token: args.base_reference_token.clone(),
        base_paymaster_private_key: args.base_paymaster_private_key.clone(),
        arbitrum_rpc_url: args.arbitrum_rpc_url.clone(),
        arbitrum_reference_token: args.arbitrum_reference_token.clone(),
        arbitrum_paymaster_private_key: args.arbitrum_paymaster_private_key.clone(),
        evm_paymaster_vault_gas_buffer_wei: args.evm_paymaster_vault_gas_buffer_wei.clone(),
        evm_paymaster_vault_gas_target_wei: args.evm_paymaster_vault_gas_target_wei.clone(),
        bitcoin_rpc_url: args.bitcoin_rpc_url.clone(),
        bitcoin_rpc_auth: args.bitcoin_rpc_auth.clone(),
        untrusted_esplora_http_server_url: args.untrusted_esplora_http_server_url.clone(),
        bitcoin_network: args.bitcoin_network,
        bitcoin_paymaster_private_key: args.bitcoin_paymaster_private_key.clone(),
        across_api_url: args.across_api_url.clone(),
        across_api_key: args.across_api_key.clone(),
        across_integrator_id: args.across_integrator_id.clone(),
        cctp_api_url: args.cctp_api_url.clone(),
        cctp_token_messenger_v2_address: args.cctp_token_messenger_v2_address.clone(),
        cctp_message_transmitter_v2_address: args.cctp_message_transmitter_v2_address.clone(),
        hyperunit_api_url: args.hyperunit_api_url.clone(),
        hyperunit_proxy_url: args.hyperunit_proxy_url.clone(),
        hyperliquid_api_url: args.hyperliquid_api_url.clone(),
        velora_api_url: args.velora_api_url.clone(),
        velora_partner: args.velora_partner.clone(),
        hyperliquid_execution_private_key: args.hyperliquid_execution_private_key.clone(),
        hyperliquid_account_address: args.hyperliquid_account_address.clone(),
        hyperliquid_vault_address: args.hyperliquid_vault_address.clone(),
        hyperliquid_paymaster_private_key: args.hyperliquid_paymaster_private_key.clone(),
        hyperliquid_network: args.hyperliquid_network,
        hyperliquid_order_timeout_ms: args.hyperliquid_order_timeout_ms,
        coinbase_price_api_base_url: args.coinbase_price_api_base_url.clone(),
    }
}

fn ensure_temporal_up() {
    let local_temporal = SocketAddr::from(([127, 0, 0, 1], 7233));
    if TcpStream::connect_timeout(&local_temporal, Duration::from_millis(500)).is_ok() {
        return;
    }

    let status = Command::new("just")
        .arg("temporal-up")
        .status()
        .expect("run `just temporal-up`; install just or start Temporal manually");
    assert!(status.success(), "`just temporal-up` failed with {status}");
}

async fn reserve_local_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral test port");
    let port = listener.local_addr().expect("test listener addr").port();
    drop(listener);
    port
}

fn hl_shim_cadences() -> HlShimCadences {
    HlShimCadences {
        hot: Duration::from_millis(100),
        warm: Duration::from_millis(250),
        cold: Duration::from_millis(500),
        funding_hot: Duration::from_millis(250),
        funding_warm: Duration::from_millis(500),
        funding_cold: Duration::from_millis(1_000),
        order_status: Duration::from_millis(100),
    }
}

async fn spawn_hl_shim_indexer(database_url: &str, hl_url: &str) -> (String, RuntimeTask) {
    let storage = HlShimStorage::connect(database_url, 4)
        .await
        .expect("connect HL shim storage");
    let pubsub = HlShimPubSub::new(512);
    let scheduler = HlShimScheduler::new(hl_shim_cadences());
    let poller = HlShimPoller::new(
        scheduler.clone(),
        storage.clone(),
        pubsub.clone(),
        hl_url,
        1_100,
    )
    .await
    .expect("create HL shim poller");
    let app = build_hl_shim_router(HlShimAppState {
        storage,
        scheduler,
        pubsub,
    });
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind HL shim listener");
    let addr = listener.local_addr().expect("read HL shim listener addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let poller_task = tokio::spawn(async move {
            poller
                .run(2)
                .await
                .expect("HL shim poller should run until test shutdown");
        });
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("HL shim server should run until test shutdown");
        poller_task.abort();
        let _ = poller_task.await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let task = RuntimeTask::with_shutdown(handle, move || {
        let _ = shutdown_tx.send(());
    });
    assert!(!task.is_finished(), "HL shim indexer exited during startup");
    (format!("http://{addr}"), task)
}

struct EvmReceiptWatcherUrls {
    ethereum: String,
    base: String,
    arbitrum: String,
}

async fn spawn_evm_receipt_watcher(
    chain: &str,
    http_rpc_url: String,
    ws_rpc_url: String,
) -> (String, RuntimeTask) {
    let bind = "127.0.0.1:0"
        .parse::<SocketAddr>()
        .expect("valid receipt watcher bind");
    let config = EvmReceiptWatcherConfig {
        chain: chain.to_string(),
        http_rpc_url,
        ws_rpc_url: Some(ws_rpc_url),
        bind,
        max_pending: 10_000,
        max_subscriber_lag: 1_000,
        poll_interval_ms: 100,
        ws_reconnect_delay_ms: 100,
        receipt_retry_count: 5,
        receipt_retry_delay_ms: 50,
    };
    let pending = EvmReceiptPendingWatches::new(config.chain.clone(), config.max_pending);
    let pubsub = EvmReceiptPubSub::new(config.max_subscriber_lag);
    let watcher = EvmReceiptWatcher::new(&config, pending.clone(), pubsub.clone())
        .await
        .expect("create EVM receipt watcher");
    let receipt_provider = watcher.receipt_provider();
    let app = build_evm_receipt_watcher_router(EvmReceiptWatcherAppState {
        chain: config.chain.clone(),
        pending,
        pubsub,
        receipt_provider,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind)
        .await
        .expect("bind EVM receipt watcher listener");
    let addr = listener
        .local_addr()
        .expect("read EVM receipt watcher listener addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let chain_label = chain.to_string();
    let handle = tokio::spawn(async move {
        let watcher_task = tokio::spawn(async move {
            watcher.run().await;
        });
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .unwrap_or_else(|error| {
                panic!("{chain_label} EVM receipt watcher server stopped: {error}")
            });
        watcher_task.abort();
        let _ = watcher_task.await;
    });
    let base_url = format!("http://{addr}");
    wait_for_http_health(&base_url, "/healthz", "EVM receipt watcher").await;
    let task = RuntimeTask::with_shutdown(handle, move || {
        let _ = shutdown_tx.send(());
    });
    assert!(
        !task.is_finished(),
        "{chain} EVM receipt watcher exited during startup"
    );
    (base_url, task)
}

struct BitcoinObserverUrls {
    indexer: String,
    receipt_watcher: String,
}

async fn spawn_bitcoin_observers(devnet: &RiftDevnet) -> (BitcoinObserverUrls, Vec<RuntimeTask>) {
    let (indexer_url, indexer_task) = spawn_bitcoin_indexer(devnet).await;
    let (receipt_watcher_url, receipt_watcher_task) = spawn_bitcoin_receipt_watcher(devnet).await;
    (
        BitcoinObserverUrls {
            indexer: indexer_url,
            receipt_watcher: receipt_watcher_url,
        },
        vec![indexer_task, receipt_watcher_task],
    )
}

async fn spawn_bitcoin_indexer(devnet: &RiftDevnet) -> (String, RuntimeTask) {
    let bind = "127.0.0.1:0"
        .parse::<SocketAddr>()
        .expect("valid Bitcoin indexer bind");
    let config = BitcoinIndexerConfig {
        network: bitcoin::Network::Regtest,
        rpc_url: devnet.bitcoin.rpc_url.clone(),
        rpc_auth: Some(format!("cookie:{}", devnet.bitcoin.cookie.display())),
        esplora_url: devnet
            .bitcoin
            .esplora_url
            .clone()
            .expect("devnet esplora URL"),
        zmq_rawblock_endpoint: devnet.bitcoin.zmq_rawblock_endpoint.clone(),
        zmq_rawtx_endpoint: devnet.bitcoin.zmq_rawtx_endpoint.clone(),
        bind,
        max_subscriber_lag: 1_000,
        poll_interval_ms: 100,
        zmq_reconnect_delay_ms: 100,
        reorg_rescan_depth: 6,
    };
    let pubsub = BitcoinIndexerPubSub::new(config.max_subscriber_lag);
    let indexer = BitcoinIndexer::new(&config, pubsub.clone())
        .await
        .expect("create Bitcoin indexer");
    indexer.clone().run().await;
    let app = build_bitcoin_indexer_router(BitcoinIndexerAppState {
        indexer,
        pubsub,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind)
        .await
        .expect("bind Bitcoin indexer listener");
    let addr = listener.local_addr().expect("read Bitcoin indexer addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("Bitcoin indexer server should run until test shutdown");
    });
    let base_url = format!("http://{addr}");
    wait_for_http_health(&base_url, "/healthz", "Bitcoin indexer").await;
    let task = RuntimeTask::with_shutdown(handle, move || {
        let _ = shutdown_tx.send(());
    });
    assert!(!task.is_finished(), "Bitcoin indexer exited during startup");
    (base_url, task)
}

async fn spawn_bitcoin_receipt_watcher(devnet: &RiftDevnet) -> (String, RuntimeTask) {
    let bind = "127.0.0.1:0"
        .parse::<SocketAddr>()
        .expect("valid Bitcoin receipt watcher bind");
    let config = BitcoinReceiptWatcherConfig {
        chain: "bitcoin".to_string(),
        rpc_url: devnet.bitcoin.rpc_url.clone(),
        rpc_auth: Some(format!("cookie:{}", devnet.bitcoin.cookie.display())),
        zmq_rawblock_endpoint: devnet.bitcoin.zmq_rawblock_endpoint.clone(),
        bind,
        max_pending: 10_000,
        max_subscriber_lag: 1_000,
        poll_interval_ms: 100,
        zmq_reconnect_delay_ms: 100,
    };
    let pending = BitcoinReceiptPendingWatches::new(config.chain.clone(), config.max_pending);
    let pubsub = BitcoinReceiptPubSub::new(config.max_subscriber_lag);
    let watcher = BitcoinReceiptWatcher::new(&config, pending.clone(), pubsub.clone())
        .await
        .expect("create Bitcoin receipt watcher");
    let rpc = watcher.rpc_client();
    watcher.clone().run().await;
    let app = build_bitcoin_receipt_watcher_router(BitcoinReceiptWatcherAppState {
        chain: config.chain.clone(),
        pending,
        pubsub,
        rpc,
        metrics: None,
    });
    let listener = TcpListener::bind(config.bind)
        .await
        .expect("bind Bitcoin receipt watcher listener");
    let addr = listener
        .local_addr()
        .expect("read Bitcoin receipt watcher addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("Bitcoin receipt watcher server should run until test shutdown");
    });
    let base_url = format!("http://{addr}");
    wait_for_http_health(&base_url, "/healthz", "Bitcoin receipt watcher").await;
    let task = RuntimeTask::with_shutdown(handle, move || {
        let _ = shutdown_tx.send(());
    });
    assert!(
        !task.is_finished(),
        "Bitcoin receipt watcher exited during startup"
    );
    (base_url, task)
}

async fn wait_for_http_health(base_url: &str, path: &str, label: &str) {
    let client = reqwest::Client::new();
    let url = format!("{base_url}{path}");
    for _ in 0..50 {
        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => return,
            Ok(_) | Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    panic!("{label} did not become healthy at {url}");
}

async fn spawn_sauron(
    devnet: &RiftDevnet,
    database_url: &str,
    sauron_state_database_url: &str,
    router_base_url: &str,
    hl_shim_base_url: &str,
    receipt_watcher_urls: EvmReceiptWatcherUrls,
    bitcoin_observer_urls: BitcoinObserverUrls,
    hyperunit_api_url: String,
) -> RuntimeTask {
    let token_indexer_api_key = [
        devnet.ethereum.token_indexer.as_ref(),
        devnet.base.token_indexer.as_ref(),
        devnet.arbitrum.token_indexer.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(|indexer| indexer.api_key.clone())
    .next();

    let args = SauronArgs {
        log_level: "warn".to_string(),
        router_replica_database_url: database_url.to_string(),
        sauron_state_database_url: sauron_state_database_url.to_string(),
        sauron_replica_event_source: sauron::config::SauronReplicaEventSource::Cdc,
        router_replica_database_name: "router_db".to_string(),
        sauron_cdc_slot_name: "sauron_watch_cdc".to_string(),
        router_cdc_publication_name: "router_cdc_publication".to_string(),
        router_cdc_message_prefix: "rift.router.change".to_string(),
        sauron_cdc_status_interval_ms: 1000,
        sauron_cdc_idle_wakeup_interval_ms: 10_000,
        router_internal_base_url: router_base_url.to_string(),
        router_detector_api_key: ROUTER_DETECTOR_API_KEY.to_string(),
        bitcoin_indexer_url: Some(bitcoin_observer_urls.indexer),
        bitcoin_receipt_watcher_url: Some(bitcoin_observer_urls.receipt_watcher),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint_url().to_string(),
        ethereum_token_indexer_url: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        ethereum_receipt_watcher_url: Some(receipt_watcher_urls.ethereum),
        base_rpc_url: devnet.base.anvil.endpoint_url().to_string(),
        base_token_indexer_url: devnet
            .base
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        base_receipt_watcher_url: Some(receipt_watcher_urls.base),
        arbitrum_rpc_url: devnet.arbitrum.anvil.endpoint_url().to_string(),
        arbitrum_token_indexer_url: devnet
            .arbitrum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        arbitrum_receipt_watcher_url: Some(receipt_watcher_urls.arbitrum),
        hl_shim_indexer_url: Some(hl_shim_base_url.to_string()),
        hyperunit_api_url: Some(hyperunit_api_url),
        hyperunit_proxy_url: None,
        sauron_hl_bridge_match_window_seconds: 1_800,
        sauron_hyperunit_observer_concurrency: 64,
        sauron_hu_poll_fast_seconds: 5,
        sauron_hu_poll_medium_seconds: 10,
        sauron_hu_poll_slow_seconds: 20,
        sauron_hyperliquid_observer_concurrency: 128,
        sauron_evm_receipt_observer_concurrency: 128,
        token_indexer_api_key,
        sauron_reconcile_interval_seconds: 1,
        sauron_bitcoin_scan_interval_seconds: 1,
        sauron_bitcoin_indexed_lookup_concurrency: 1,
        sauron_evm_indexed_lookup_concurrency: 1,
    };
    let handle = tokio::spawn(async move {
        run_sauron(args)
            .await
            .expect("Sauron should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let task = RuntimeTask::new(handle);
    assert!(!task.is_finished(), "Sauron exited during startup");
    task
}

fn router_args(
    devnet: &RiftDevnet,
    config_dir: &Path,
    database_url: String,
    mocks: &MockIntegratorServer,
) -> RouterServerArgs {
    RouterServerArgs {
        host: IpAddr::from([127, 0, 0, 1]),
        port: 0,
        database_url,
        db_max_connections: 8,
        db_min_connections: 1,
        log_level: "warn".to_string(),
        master_key_path: write_router_master_key(config_dir)
            .to_string_lossy()
            .to_string(),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint_url().to_string(),
        ethereum_reference_token: ETHEREUM_USDC_ADDRESS.to_string(),
        ethereum_paymaster_private_key: Some(anvil_private_key(&devnet.ethereum)),
        base_rpc_url: devnet.base.anvil.endpoint_url().to_string(),
        base_reference_token: BASE_USDC_ADDRESS.to_string(),
        base_paymaster_private_key: Some(anvil_private_key(&devnet.base)),
        arbitrum_rpc_url: devnet.arbitrum.anvil.endpoint_url().to_string(),
        arbitrum_reference_token: ARBITRUM_USDC_ADDRESS.to_string(),
        arbitrum_paymaster_private_key: Some(anvil_private_key(&devnet.arbitrum)),
        evm_paymaster_vault_gas_buffer_wei: "100000000000000".to_string(),
        evm_paymaster_vault_gas_target_wei: None,
        bitcoin_rpc_url: bitcoin_rpc_url(devnet),
        bitcoin_rpc_auth: Auth::CookieFile(devnet.bitcoin.cookie.clone()),
        untrusted_esplora_http_server_url: devnet
            .bitcoin
            .esplora_url
            .as_ref()
            .expect("esplora URL")
            .clone(),
        bitcoin_network: bitcoin::Network::Regtest,
        bitcoin_paymaster_private_key: None,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
        loki_url: None,
        across_api_url: Some(mocks.base_url().to_string()),
        across_api_key: Some(ACROSS_API_KEY.to_string()),
        across_integrator_id: Some(ACROSS_INTEGRATOR_ID.to_string()),
        cctp_api_url: Some(mocks.base_url().to_string()),
        cctp_token_messenger_v2_address: Some(
            devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS.to_string(),
        ),
        cctp_message_transmitter_v2_address: Some(
            devnet::evm_devnet::MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS.to_string(),
        ),
        hyperunit_api_url: Some(mocks.base_url().to_string()),
        hyperunit_proxy_url: None,
        hyperliquid_api_url: Some(mocks.base_url().to_string()),
        velora_api_url: Some(mocks.base_url().to_string()),
        velora_partner: Some("router-e2e".to_string()),
        hyperliquid_execution_private_key: None,
        hyperliquid_account_address: None,
        hyperliquid_vault_address: None,
        hyperliquid_paymaster_private_key: Some(test_hyperliquid_paymaster_private_key()),
        temporal_address: "http://127.0.0.1:7233".to_string(),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("{DEFAULT_TASK_QUEUE}-{}", Uuid::now_v7()),
        router_detector_api_key: Some(ROUTER_DETECTOR_API_KEY.to_string()),
        router_gateway_api_key: None,
        router_admin_api_key: Some(ROUTER_ADMIN_API_KEY.to_string()),
        hyperliquid_network: HyperliquidCallNetwork::Testnet,
        hyperliquid_order_timeout_ms: 30_000,
        worker_id: Some(format!("router-runtime-e2e-{}", Uuid::now_v7())),
        worker_refund_poll_seconds: 1,
        worker_order_execution_poll_seconds: 1,
        worker_route_cost_refresh_seconds: 300,
        worker_provider_health_poll_seconds: 120,
        provider_health_timeout_seconds: 10,
        worker_order_maintenance_pass_limit: 100,
        worker_order_planning_pass_limit: 100,
        worker_order_execution_pass_limit: 25,
        worker_order_execution_concurrency: 64,
        worker_vault_funding_hint_pass_limit: 100,
        coinbase_price_api_base_url: mocks.base_url().to_string(),
    }
}

async fn spawn_runtime_mocks(devnet: &RiftDevnet) -> MockIntegratorServer {
    let ethereum_spoke_pool = format!(
        "{:#x}",
        devnet.ethereum.mock_across_spoke_pool_contract.address()
    );
    let base_spoke_pool = format!(
        "{:#x}",
        devnet.base.mock_across_spoke_pool_contract.address()
    );
    let arbitrum_spoke_pool = format!(
        "{:#x}",
        devnet.arbitrum.mock_across_spoke_pool_contract.address()
    );
    let ethereum_ws_url = devnet.ethereum.anvil.ws_endpoint_url().to_string();
    let base_ws_url = devnet.base.anvil.ws_endpoint_url().to_string();
    let arbitrum_ws_url = devnet.arbitrum.anvil.ws_endpoint_url().to_string();
    let config = MockIntegratorConfig::default()
        .with_across_spoke_pool_address(base_spoke_pool.clone())
        .with_across_evm_rpc_url(base_ws_url.clone())
        .with_across_chain(
            devnet.ethereum.anvil.chain_id(),
            ethereum_spoke_pool,
            ethereum_ws_url,
        )
        .with_across_chain(devnet.base.anvil.chain_id(), base_spoke_pool, base_ws_url)
        .with_across_chain(
            devnet.arbitrum.anvil.chain_id(),
            arbitrum_spoke_pool,
            arbitrum_ws_url,
        )
        .with_unit_bitcoin_rpc(
            devnet.bitcoin.rpc_url.clone(),
            devnet.bitcoin.cookie.clone(),
        )
        .with_across_auth(ACROSS_API_KEY, ACROSS_INTEGRATOR_ID)
        .with_across_status_latency(Duration::from_secs(2))
        .with_cctp_chain(
            devnet.ethereum.anvil.chain_id(),
            devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
            devnet.ethereum.anvil.endpoint_url().to_string(),
        )
        .with_cctp_chain(
            devnet.base.anvil.chain_id(),
            devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
            devnet.base.anvil.endpoint_url().to_string(),
        )
        .with_cctp_chain(
            devnet.arbitrum.anvil.chain_id(),
            devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
            devnet.arbitrum.anvil.endpoint_url().to_string(),
        )
        .with_cctp_destination_token(devnet.ethereum.anvil.chain_id(), ETHEREUM_USDC_ADDRESS)
        .with_cctp_destination_token(devnet.base.anvil.chain_id(), BASE_USDC_ADDRESS)
        .with_cctp_destination_token(devnet.arbitrum.anvil.chain_id(), ARBITRUM_USDC_ADDRESS)
        .with_unit_evm_rpc_url(
            hyperunit_client::UnitChain::Ethereum,
            devnet.ethereum.anvil.endpoint_url().to_string(),
        )
        .with_unit_evm_rpc_url(
            hyperunit_client::UnitChain::Base,
            devnet.base.anvil.endpoint_url().to_string(),
        )
        .with_velora_swap_contract_address(
            devnet.ethereum.anvil.chain_id(),
            format!("{:#x}", devnet.ethereum.mock_velora_swap_contract.address()),
        )
        .with_velora_swap_contract_address(
            devnet.base.anvil.chain_id(),
            format!("{:#x}", devnet.base.mock_velora_swap_contract.address()),
        )
        .with_velora_swap_contract_address(
            devnet.arbitrum.anvil.chain_id(),
            format!("{:#x}", devnet.arbitrum.mock_velora_swap_contract.address()),
        )
        .with_hyperliquid_bridge_address(TESTNET_HYPERLIQUID_BRIDGE_ADDRESS)
        .with_hyperliquid_evm_rpc_url(devnet.arbitrum.anvil.endpoint_url().to_string())
        .with_hyperliquid_usdc_token_address(ARBITRUM_USDC_ADDRESS)
        .with_mainnet_hyperliquid(false);
    MockIntegratorServer::spawn_with_config(config)
        .await
        .expect("spawn mock integrators")
}

async fn run_route_case(
    client: &reqwest::Client,
    router_base_url: &str,
    devnet: &RiftDevnet,
    mocks: &MockIntegratorServer,
    route_case: RouteCase,
) {
    eprintln!("running router runtime route case {}", route_case.name);
    let quote = submit_exact_in_quote(client, router_base_url, route_case).await;
    let market_quote = quote.quote.as_market_order().expect("market order quote");
    assert_provider_fragments(route_case, &market_quote.provider_id);
    let quote_id = market_quote.id;
    let amount_in = market_quote.amount_in.clone();

    let order = submit_order(client, router_base_url, quote_id, route_case).await;
    assert_eq!(order.order.status, RouterOrderStatus::PendingFunding);
    let funding_vault = order
        .funding_vault
        .as_ref()
        .expect("order should include funding vault")
        .vault
        .clone();
    assert_funding_vault_matches(route_case, &funding_vault);
    fund_source_vault(devnet, &funding_vault, &amount_in).await;

    let completed =
        drive_order_to_completion(client, router_base_url, devnet, mocks, order.order.id).await;
    assert_eq!(
        completed.order.status,
        RouterOrderStatus::Completed,
        "route case {} should complete",
        route_case.name
    );
}

async fn run_limit_route_case(
    client: &reqwest::Client,
    router_base_url: &str,
    devnet: &RiftDevnet,
    mocks: &MockIntegratorServer,
    route_case: LimitRouteCase,
) {
    eprintln!(
        "running router runtime limit route case {}",
        route_case.name
    );
    let quote = submit_limit_quote(client, router_base_url, route_case).await;
    let limit_quote = quote.quote.as_limit_order().expect("limit order quote");
    assert_provider_fragments_for(
        route_case.name,
        route_case.from,
        route_case.to,
        route_case.expected_provider_fragments,
        &limit_quote.provider_id,
    );
    assert_limit_quote_has_hyperliquid_limit_leg(route_case.name, &limit_quote.provider_quote);
    let quote_id = limit_quote.id;
    let amount_in = limit_quote.input_amount.clone();

    let order = submit_limit_order(client, router_base_url, quote_id, route_case).await;
    assert_eq!(order.order.status, RouterOrderStatus::PendingFunding);
    let funding_vault = order
        .funding_vault
        .as_ref()
        .expect("limit order should include funding vault")
        .vault
        .clone();
    assert_limit_funding_vault_matches(route_case, &funding_vault);
    fund_source_vault(devnet, &funding_vault, &amount_in).await;

    let completed =
        drive_order_to_completion(client, router_base_url, devnet, mocks, order.order.id).await;
    assert_eq!(
        completed.order.status,
        RouterOrderStatus::Completed,
        "limit route case {} should complete",
        route_case.name
    );
}

async fn submit_exact_in_quote(
    client: &reqwest::Client,
    router_base_url: &str,
    route_case: RouteCase,
) -> RouterOrderQuoteEnvelope {
    let response = client
        .post(format!("{router_base_url}/api/v1/quotes"))
        .json(&json!({
            "type": "market_order",
            "from_asset": route_case.from.to_json(),
            "to_asset": route_case.to.to_json(),
            "recipient_address": recipient_address_for(route_case.to),
            "kind": "exact_in",
            "amount_in": route_case.amount_in,
            "slippage_bps": 100
        }))
        .send()
        .await
        .expect("quote request");
    let status = response.status();
    let body = response.text().await.expect("quote body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("quote response")
}

async fn submit_limit_quote(
    client: &reqwest::Client,
    router_base_url: &str,
    route_case: LimitRouteCase,
) -> RouterOrderQuoteEnvelope {
    let response = client
        .post(format!("{router_base_url}/api/v1/quotes"))
        .json(&json!({
            "type": "limit_order",
            "from_asset": route_case.from.to_json(),
            "to_asset": route_case.to.to_json(),
            "recipient_address": recipient_address_for(route_case.to),
            "input_amount": route_case.input_amount,
            "output_amount": route_case.output_amount
        }))
        .send()
        .await
        .expect("limit quote request");
    let status = response.status();
    let body = response.text().await.expect("limit quote body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("limit quote response")
}

async fn submit_order(
    client: &reqwest::Client,
    router_base_url: &str,
    quote_id: Uuid,
    route_case: RouteCase,
) -> RouterOrderEnvelope {
    let response = client
        .post(format!("{router_base_url}/api/v1/orders"))
        .json(&json!({
            "quote_id": quote_id,
            "idempotency_key": format!("router-runtime-e2e:{}:{quote_id}", route_case.name),
            "refund_address": refund_address_for(route_case.from),
            "metadata": {
                "test": route_case.name
            }
        }))
        .send()
        .await
        .expect("order request");
    let status = response.status();
    let body = response.text().await.expect("order body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("order response")
}

async fn submit_limit_order(
    client: &reqwest::Client,
    router_base_url: &str,
    quote_id: Uuid,
    route_case: LimitRouteCase,
) -> RouterOrderEnvelope {
    let response = client
        .post(format!("{router_base_url}/api/v1/orders"))
        .json(&json!({
            "quote_id": quote_id,
            "idempotency_key": format!("router-runtime-e2e:{}:{quote_id}", route_case.name),
            "refund_address": refund_address_for(route_case.from),
            "metadata": {
                "test": route_case.name
            }
        }))
        .send()
        .await
        .expect("limit order request");
    let status = response.status();
    let body = response.text().await.expect("limit order body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("limit order response")
}

fn assert_provider_fragments(route_case: RouteCase, provider_id: &str) {
    assert_provider_fragments_for(
        route_case.name,
        route_case.from,
        route_case.to,
        route_case.expected_provider_fragments,
        provider_id,
    );
}

fn assert_provider_fragments_for(
    name: &str,
    from: RouteAsset,
    to: RouteAsset,
    expected_provider_fragments: &[&str],
    provider_id: &str,
) {
    for fragment in expected_provider_fragments {
        assert!(
            provider_id.contains(fragment),
            "route case {name} expected provider id to contain {fragment:?}; provider id was {provider_id}"
        );
    }

    if from.chain != to.chain {
        let has_cross_chain_leg = [
            "across_bridge:across",
            "cctp_bridge:cctp",
            "hyperliquid_bridge_deposit:hyperliquid_bridge",
            "unit_deposit:unit",
            "unit_withdrawal:unit",
        ]
        .iter()
        .any(|fragment| provider_id.contains(fragment));
        assert!(
            has_cross_chain_leg,
            "route case {name} is cross-chain but provider id did not contain a cross-chain leg: {provider_id}"
        );
    }
}

fn assert_limit_quote_has_hyperliquid_limit_leg(name: &str, provider_quote: &Value) {
    let has_limit_leg = provider_quote
        .get("legs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|leg| {
            leg.get("raw")
                .and_then(|raw| raw.get("kind"))
                .and_then(Value::as_str)
                == Some("hyperliquid_limit_order")
        });
    assert!(
        has_limit_leg,
        "limit route case {name} should materialize a Hyperliquid limit-order quote leg: {provider_quote}"
    );
}

fn assert_funding_vault_matches(route_case: RouteCase, vault: &DepositVault) {
    assert_eq!(
        vault.deposit_asset.chain.to_string(),
        route_case.from.chain,
        "route case {} funding vault should use source chain",
        route_case.name
    );
    assert_eq!(
        vault.deposit_asset.asset.as_str().to_ascii_lowercase(),
        route_case.from.asset.to_ascii_lowercase(),
        "route case {} funding vault should use source asset",
        route_case.name
    );
}

fn assert_limit_funding_vault_matches(route_case: LimitRouteCase, vault: &DepositVault) {
    assert_eq!(
        vault.deposit_asset.chain.to_string(),
        route_case.from.chain,
        "limit route case {} funding vault should use source chain",
        route_case.name
    );
    assert_eq!(
        vault.deposit_asset.asset.as_str().to_ascii_lowercase(),
        route_case.from.asset.to_ascii_lowercase(),
        "limit route case {} funding vault should use source asset",
        route_case.name
    );
}

fn recipient_address_for(asset: RouteAsset) -> String {
    if asset.chain == "bitcoin" {
        valid_regtest_btc_address()
    } else {
        valid_evm_address()
    }
}

fn refund_address_for(asset: RouteAsset) -> String {
    if asset.chain == "bitcoin" {
        valid_regtest_btc_address()
    } else {
        valid_evm_address()
    }
}

async fn get_order_http(
    client: &reqwest::Client,
    router_base_url: &str,
    order_id: Uuid,
) -> RouterOrderEnvelope {
    let response = client
        .get(format!("{router_base_url}/api/v1/orders/{order_id}"))
        .send()
        .await
        .expect("order status request");
    let status = response.status();
    let body = response.text().await.expect("order status body");
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    serde_json::from_str(&body).expect("order status JSON")
}

fn is_terminal_order_status(status: RouterOrderStatus) -> bool {
    matches!(
        status,
        RouterOrderStatus::Completed
            | RouterOrderStatus::Expired
            | RouterOrderStatus::RefundRequired
            | RouterOrderStatus::Refunded
            | RouterOrderStatus::ManualInterventionRequired
            | RouterOrderStatus::RefundManualInterventionRequired
    )
}

async fn drive_order_to_completion(
    client: &reqwest::Client,
    router_base_url: &str,
    devnet: &RiftDevnet,
    mocks: &MockIntegratorServer,
    order_id: Uuid,
) -> RouterOrderEnvelope {
    let started = Instant::now();
    let mut completed_manual_operations = HashSet::new();

    loop {
        let order = get_order_http(client, router_base_url, order_id).await;
        if order.order.status == RouterOrderStatus::Completed {
            return order;
        }
        if is_terminal_order_status(order.order.status) {
            dump_order_flow(client, router_base_url, order_id).await;
            panic!(
                "order {order_id} reached terminal status {:?}, expected completion",
                order.order.status
            );
        }

        let flow = get_order_flow(client, router_base_url, order_id).await;
        for operation in &flow.flow.provider_operations {
            if completed_manual_operations.contains(&operation.id)
                || is_terminal_provider_status(operation.status)
            {
                continue;
            }

            match operation.operation_type {
                ProviderOperationType::AcrossBridge
                    if operation.status == ProviderOperationStatus::WaitingExternal =>
                {
                    fund_destination_execution_vault_from_across(devnet, &flow, operation).await;
                    completed_manual_operations.insert(operation.id);
                }
                ProviderOperationType::UnitDeposit if operation.provider_ref.is_some() => {
                    complete_unit_deposit(mocks, operation).await;
                    completed_manual_operations.insert(operation.id);
                }
                ProviderOperationType::UnitWithdrawal if operation.provider_ref.is_some() => {
                    mocks
                        .complete_unit_operation_with_source_amount(
                            operation
                                .provider_ref
                                .as_deref()
                                .expect("unit withdrawal provider ref"),
                            unit_operation_amount(operation).to_string(),
                        )
                        .await
                        .expect("complete mock Unit withdrawal operation");
                    completed_manual_operations.insert(operation.id);
                }
                _ => {}
            }
        }

        if started.elapsed() >= Duration::from_secs(240) {
            dump_order_flow(client, router_base_url, order_id).await;
            panic!("timed out waiting for order {order_id} to complete");
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn is_terminal_provider_status(status: ProviderOperationStatus) -> bool {
    matches!(
        status,
        ProviderOperationStatus::Completed
            | ProviderOperationStatus::Failed
            | ProviderOperationStatus::Expired
    )
}

async fn complete_unit_deposit(mocks: &MockIntegratorServer, operation: &OrderProviderOperation) {
    let provider_ref = operation
        .provider_ref
        .as_deref()
        .expect("unit deposit provider ref");
    let amount = unit_deposit_amount(operation);
    mocks
        .complete_unit_operation_with_source_amount(provider_ref, amount.to_string())
        .await
        .expect("complete mock Unit deposit operation");
}

fn unit_deposit_amount(operation: &OrderProviderOperation) -> U256 {
    unit_operation_amount(operation)
}

fn unit_operation_amount(operation: &OrderProviderOperation) -> U256 {
    operation
        .request
        .get("amount")
        .and_then(Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("UnitDeposit operation request should include amount")
}

async fn fund_destination_execution_vault_from_across(
    devnet: &RiftDevnet,
    flow: &OrderFlowEnvelope,
    operation: &OrderProviderOperation,
) {
    let destination_vault = flow
        .flow
        .custody_vaults
        .iter()
        .find(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .expect("Across step should create destination execution custody vault");
    let output_amount = operation
        .response
        .get("expectedOutputAmount")
        .or_else(|| operation.response.get("outputAmount"))
        .and_then(Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("Across response should include expected output amount");
    let destination_devnet = evm_devnet_for_chain(devnet, destination_vault.chain.as_str());
    let address = Address::from_str(&destination_vault.address)
        .expect("destination execution vault address should be EVM address");
    let gas_balance = U256::from(EVM_NATIVE_GAS_BUFFER_WEI);

    match destination_vault.asset.as_ref().unwrap_or(&AssetId::Native) {
        AssetId::Native => {
            send_native(destination_devnet, address, output_amount + gas_balance).await;
        }
        AssetId::Reference(token_address) => {
            send_native(destination_devnet, address, gas_balance).await;
            mint_erc20(
                destination_devnet,
                token_address.parse().expect("destination token address"),
                address,
                output_amount,
            )
            .await;
        }
    }
    mine_evm_confirmation_block(destination_devnet).await;
}

async fn get_order_flow(
    client: &reqwest::Client,
    router_base_url: &str,
    order_id: Uuid,
) -> OrderFlowEnvelope {
    let response = client
        .get(format!(
            "{router_base_url}/internal/v1/orders/{order_id}/flow"
        ))
        .bearer_auth(ROUTER_ADMIN_API_KEY)
        .send()
        .await
        .expect("order flow request");
    let status = response.status();
    let body = response.text().await.expect("order flow body");
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    serde_json::from_str(&body).expect("order flow JSON")
}

async fn dump_order_flow(client: &reqwest::Client, router_base_url: &str, order_id: Uuid) {
    let response = client
        .get(format!(
            "{router_base_url}/internal/v1/orders/{order_id}/flow"
        ))
        .bearer_auth(ROUTER_ADMIN_API_KEY)
        .send()
        .await;
    let Ok(response) = response else {
        eprintln!("failed to fetch order flow for {order_id}");
        return;
    };
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    eprintln!("order flow HTTP status={status} body={body}");
    if status.is_success() {
        if let Ok(flow) = serde_json::from_str::<OrderFlowEnvelope>(&body) {
            eprintln!(
                "flow progress total={} completed={} waiting={} running={} failed={} current={:?}",
                flow.flow.progress.total_steps,
                flow.flow.progress.completed_steps,
                flow.flow.progress.waiting_steps,
                flow.flow.progress.running_steps,
                flow.flow.progress.failed_steps,
                flow.flow.progress.current_step_type,
            );
        }
    }
}

async fn fund_source_vault(devnet: &RiftDevnet, vault: &DepositVault, amount_in: &str) {
    let chain = vault.deposit_asset.chain.as_str();
    if chain == "bitcoin" {
        // Give Sauron one reconcile/discovery tick to install the new Bitcoin
        // watch before the regtest tx is mined. Otherwise the one-shot indexed
        // lookup can run before funding and the block scanner can miss the
        // transaction because the script was not yet in the backend watch map.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let amount_sats = amount_in
            .parse::<u64>()
            .expect("bitcoin source amount should fit u64")
            .checked_add(BITCOIN_FEE_BUFFER_SATS)
            .expect("bitcoin source amount plus fee buffer should fit u64");
        send_bitcoin(devnet, &vault.deposit_vault_address, amount_sats).await;
        return;
    }

    let evm_devnet = evm_devnet_for_chain(devnet, chain);
    let vault_address = Address::from_str(&vault.deposit_vault_address)
        .expect("funding vault should be an EVM address");
    let amount = U256::from_str_radix(amount_in, 10).expect("source amount should parse");
    if vault.deposit_asset.asset.is_native() {
        send_native(
            evm_devnet,
            vault_address,
            amount + U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
        )
        .await;
    } else {
        let token_address = vault
            .deposit_asset
            .asset
            .as_str()
            .parse()
            .expect("funding vault token should be an EVM address");
        transfer_user_erc20_to_vault(evm_devnet, token_address, vault_address, amount).await;
    }
    mine_evm_confirmation_block(evm_devnet).await;
}

async fn send_bitcoin(devnet: &RiftDevnet, address: &str, amount_sats: u64) {
    let address = BitcoinAddress::from_str(address)
        .expect("valid bitcoin funding vault address")
        .require_network(bitcoin::Network::Regtest)
        .expect("bitcoin funding vault should be a regtest address");
    devnet
        .bitcoin
        .deal_bitcoin(&address, &Amount::from_sat(amount_sats))
        .await
        .expect("fund bitcoin source vault");
    devnet
        .bitcoin
        .wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync after bitcoin funding");
}

async fn send_native(devnet: &devnet::EthDevnet, recipient: Address, amount: U256) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url());
    let tx = TransactionRequest::default()
        .with_to(recipient)
        .with_value(amount);
    let receipt = provider
        .send_transaction(tx)
        .await
        .expect("send native transfer")
        .get_receipt()
        .await
        .expect("native transfer receipt");
    assert!(receipt.status(), "native transfer reverted");
}

async fn mine_evm_confirmation_block(devnet: &devnet::EthDevnet) {
    let provider = ProviderBuilder::new().connect_http(devnet.anvil.endpoint_url());
    provider
        .anvil_mine(Some(1), None)
        .await
        .expect("mine EVM confirmation block");
}

fn evm_devnet_for_chain<'a>(devnet: &'a RiftDevnet, chain: &str) -> &'a devnet::EthDevnet {
    match chain {
        "evm:1" => &devnet.ethereum,
        "evm:8453" => &devnet.base,
        "evm:42161" => &devnet.arbitrum,
        other => panic!("unsupported EVM chain {other}"),
    }
}

async fn transfer_user_erc20_to_vault(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    vault_address: Address,
    amount: U256,
) {
    let signer = anvil_signer(devnet);
    let user_address = signer.address();
    mint_erc20(devnet, token_address, user_address, amount).await;
    transfer_erc20(devnet, token_address, vault_address, amount).await;
}

async fn transfer_erc20(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    recipient: Address,
    amount: U256,
) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url());
    let token = IERC20::new(token_address, provider);
    let receipt = token
        .transfer(recipient, amount)
        .send()
        .await
        .expect("send ERC20 transfer")
        .get_receipt()
        .await
        .expect("ERC20 transfer receipt");
    assert!(receipt.status(), "ERC20 transfer reverted");
}

async fn mint_erc20(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    recipient: Address,
    amount: U256,
) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let token = GenericEIP3009ERC20Instance::new(token_address, provider);
    let receipt = token
        .mint(recipient, amount)
        .send()
        .await
        .expect("send mint")
        .get_receipt()
        .await
        .expect("mint receipt");
    assert!(receipt.status(), "ERC20 mint reverted");
}

async fn install_mock_usdc_clone(devnet: &devnet::EthDevnet, token_address: Address) {
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(anvil_signer(devnet)))
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let implementation_code = provider
        .get_code_at(*devnet.mock_erc20_contract.address())
        .await
        .expect("load mock ERC20 bytecode");
    provider
        .anvil_set_code(token_address, implementation_code)
        .await
        .expect("install mock ERC20 bytecode at canonical address");

    let token = GenericEIP3009ERC20Instance::new(token_address, provider.clone());
    let admin = devnet.funded_address;
    token
        .initialize(
            "Mock USD Coin".to_string(),
            "USDC".to_string(),
            "USDC".to_string(),
            6,
            admin,
            admin,
            admin,
            admin,
        )
        .send()
        .await
        .expect("send mock USDC initialize")
        .get_receipt()
        .await
        .expect("mock USDC initialize receipt");
    token
        .configureMinter(admin, U256::MAX)
        .send()
        .await
        .expect("send mock USDC configureMinter")
        .get_receipt()
        .await
        .expect("mock USDC configureMinter receipt");
}

async fn wait_until<T, Fut>(label: &str, timeout: Duration, mut check: impl FnMut() -> Fut) -> T
where
    Fut: Future<Output = Option<T>>,
{
    let started = Instant::now();
    loop {
        if let Some(value) = check().await {
            return value;
        }
        assert!(started.elapsed() < timeout, "timed out waiting for {label}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn write_router_master_key(config_dir: &Path) -> std::path::PathBuf {
    let path = config_dir.join("router-server-master-key.hex");
    if !path.exists() {
        std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    }
    path
}

fn anvil_signer(devnet: &devnet::EthDevnet) -> PrivateKeySigner {
    anvil_private_key(devnet)
        .parse::<PrivateKeySigner>()
        .expect("valid Anvil private key")
}

fn anvil_private_key(devnet: &devnet::EthDevnet) -> String {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[0].to_bytes())
    )
}

fn test_hyperliquid_paymaster_private_key() -> String {
    "0x59c6995e998f97a5a0044976f7ad0a7df4976fbe66f6cc18ff3c16f18a6b9e3f".to_string()
}

fn bitcoin_rpc_url(devnet: &RiftDevnet) -> String {
    format!(
        "http://{}:{}",
        devnet.bitcoin.regtest.params.rpc_socket.ip(),
        devnet.bitcoin.regtest.params.rpc_socket.port()
    )
}

fn valid_evm_address() -> String {
    "0x1111111111111111111111111111111111111111".to_string()
}

fn valid_regtest_btc_address() -> String {
    "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string()
}
