use std::{
    collections::HashSet,
    future::Future,
    net::IpAddr,
    path::Path,
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
use bitcoincore_rpc_async::Auth;
use devnet::{
    mock_integrators::{MockIntegratorConfig, MockIntegratorServer},
    RiftDevnet,
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use router_server::{
    api::OrderFlowEnvelope,
    models::{
        CustodyVaultRole, DepositVault, OrderProviderOperation, ProviderOperationStatus,
        ProviderOperationType, RouterOrderEnvelope, RouterOrderQuoteEnvelope, RouterOrderStatus,
    },
    protocol::AssetId,
    server::run_api,
    services::custody_action_executor::HyperliquidCallNetwork,
    worker::run_worker,
    RouterServerArgs,
};
use sauron::{run as run_sauron, SauronArgs};
use serde_json::{json, Value};
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::task::JoinHandle;
use uuid::Uuid;

const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ETHEREUM_USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const MAINNET_HYPERLIQUID_BRIDGE_ADDRESS: &str = "0x2df1c51e09aecf9cacb7bc98cb1742757f163df7";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";
const ROUTER_DETECTOR_API_KEY: &str = "router-e2e-detector-secret";
const ROUTER_ADMIN_API_KEY: &str = "router-e2e-admin-secret";
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

const ROUTE_CASES: &[RouteCase] = &[
    RouteCase {
        name: "eth_to_bitcoin",
        from: RouteAsset::native("evm:1"),
        to: RouteAsset::native("bitcoin"),
        amount_in: "30000000000000000",
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
        amount_in: "30000000000000000",
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
        amount_in: "30000000000000000",
        expected_provider_fragments: &["across_bridge:across", "universal_router_swap:velora"],
    },
    RouteCase {
        name: "usdc_to_eth_cross_chain",
        from: RouteAsset::token("evm:8453", BASE_USDC_ADDRESS),
        to: RouteAsset::native("evm:1"),
        amount_in: "60000000",
        expected_provider_fragments: &["across_bridge:across", "unit_withdrawal:unit"],
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

struct RuntimeTask {
    handle: JoinHandle<()>,
}

impl Drop for RuntimeTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_api_worker_sauron_complete_mock_route_matrix() {
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
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
    let config_dir = tempfile::tempdir().expect("router config dir");
    let args = router_args(&devnet, config_dir.path(), database_url.clone(), &mocks);

    let (router_base_url, _api_task) = spawn_router_api(args.clone()).await;
    let _worker_task = spawn_router_worker(args.clone()).await;
    let _sauron_task = spawn_sauron(&devnet, &database_url, &router_base_url).await;

    let client = reqwest::Client::new();
    for route_case in ROUTE_CASES {
        run_route_case(&client, &router_base_url, &devnet, &mocks, *route_case).await;
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
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE);

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

    (base_url, RuntimeTask { handle })
}

async fn spawn_router_worker(args: RouterServerArgs) -> RuntimeTask {
    let handle = tokio::spawn(async move {
        run_worker(args)
            .await
            .expect("router worker should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!handle.is_finished(), "router worker exited during startup");
    RuntimeTask { handle }
}

async fn reserve_local_port() -> u16 {
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind ephemeral test port");
    let port = listener.local_addr().expect("test listener addr").port();
    drop(listener);
    port
}

async fn spawn_sauron(
    devnet: &RiftDevnet,
    database_url: &str,
    router_base_url: &str,
) -> RuntimeTask {
    let args = SauronArgs {
        log_level: "warn".to_string(),
        router_replica_database_url: database_url.to_string(),
        sauron_state_database_url: None,
        sauron_replica_event_source: sauron::config::SauronReplicaEventSource::Notify,
        router_replica_database_name: "router_db".to_string(),
        router_replica_notification_channel: "sauron_watch_set_changed".to_string(),
        sauron_cdc_slot_name: "sauron_watch_cdc".to_string(),
        sauron_cdc_plugin: "test_decoding".to_string(),
        sauron_cdc_batch_size: 1000,
        sauron_cdc_poll_interval_ms: 1000,
        router_internal_base_url: router_base_url.to_string(),
        router_detector_api_key: ROUTER_DETECTOR_API_KEY.to_string(),
        electrum_http_server_url: devnet
            .bitcoin
            .esplora_url
            .as_ref()
            .expect("esplora URL")
            .clone(),
        bitcoin_rpc_url: bitcoin_rpc_url(devnet),
        bitcoin_rpc_auth: Auth::CookieFile(devnet.bitcoin.cookie.clone()),
        bitcoin_zmq_rawtx_endpoint: devnet.bitcoin.zmq_rawtx_endpoint.clone(),
        bitcoin_zmq_sequence_endpoint: devnet.bitcoin.zmq_sequence_endpoint.clone(),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint_url().to_string(),
        ethereum_token_indexer_url: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        ethereum_allowed_token: ETHEREUM_USDC_ADDRESS.to_string(),
        base_rpc_url: devnet.base.anvil.endpoint_url().to_string(),
        base_token_indexer_url: devnet
            .base
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        base_allowed_token: BASE_USDC_ADDRESS.to_string(),
        arbitrum_rpc_url: devnet.arbitrum.anvil.endpoint_url().to_string(),
        arbitrum_token_indexer_url: devnet
            .arbitrum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        arbitrum_allowed_token: ARBITRUM_USDC_ADDRESS.to_string(),
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
    assert!(!handle.is_finished(), "Sauron exited during startup");
    RuntimeTask { handle }
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
        router_detector_api_key: Some(ROUTER_DETECTOR_API_KEY.to_string()),
        router_admin_api_key: Some(ROUTER_ADMIN_API_KEY.to_string()),
        hyperliquid_network: HyperliquidCallNetwork::Mainnet,
        hyperliquid_order_timeout_ms: 30_000,
        worker_id: Some(format!("router-runtime-e2e-{}", Uuid::now_v7())),
        worker_lease_name: Some(format!("router-runtime-e2e-{}", Uuid::now_v7())),
        worker_lease_seconds: 30,
        worker_lease_renew_seconds: 5,
        worker_standby_poll_seconds: 1,
        worker_refund_poll_seconds: 1,
        worker_order_execution_poll_seconds: 1,
        worker_route_cost_refresh_seconds: 300,
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
        .with_across_auth(ACROSS_API_KEY, ACROSS_INTEGRATOR_ID)
        .with_across_status_latency(Duration::from_secs(2))
        .with_cctp_token_messenger_address(devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS)
        .with_cctp_evm_rpc_url(devnet.base.anvil.endpoint_url().to_string())
        .with_cctp_destination_token_address(ETHEREUM_USDC_ADDRESS)
        .with_hyperliquid_bridge_address(MAINNET_HYPERLIQUID_BRIDGE_ADDRESS)
        .with_hyperliquid_evm_rpc_url(devnet.arbitrum.anvil.endpoint_url().to_string())
        .with_hyperliquid_usdc_token_address(ARBITRUM_USDC_ADDRESS)
        .with_mainnet_hyperliquid(true);
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

fn assert_provider_fragments(route_case: RouteCase, provider_id: &str) {
    for fragment in route_case.expected_provider_fragments {
        assert!(
            provider_id.contains(fragment),
            "route case {} expected provider id to contain {fragment:?}; provider id was {provider_id}",
            route_case.name
        );
    }

    if route_case.from.chain != route_case.to.chain {
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
            "route case {} is cross-chain but provider id did not contain a cross-chain leg: {provider_id}",
            route_case.name
        );
    }
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
            | RouterOrderStatus::Failed
            | RouterOrderStatus::Expired
            | RouterOrderStatus::RefundRequired
            | RouterOrderStatus::Refunded
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
                        .complete_unit_operation(
                            operation
                                .provider_ref
                                .as_deref()
                                .expect("unit withdrawal provider ref"),
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
    let deposit_coin = hl_deposit_coin_for_operation(operation);
    let deposit_decimals = match deposit_coin.as_str() {
        "UBTC" => 8,
        "UETH" => 18,
        other => panic!("unsupported UnitDeposit coin for mock HL credit: {other:?}"),
    };
    let amount = unit_deposit_amount(operation);
    let hl_vault_address = hl_destination_for_unit_deposit(operation);
    mocks
        .credit_hyperliquid_balance(
            hl_vault_address,
            &deposit_coin,
            raw_units_to_f64(amount, deposit_decimals),
        )
        .await;
    mocks
        .complete_unit_operation(provider_ref)
        .await
        .expect("complete mock Unit deposit operation");
}

fn unit_deposit_amount(operation: &OrderProviderOperation) -> U256 {
    operation
        .request
        .get("amount")
        .and_then(Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok())
        .expect("UnitDeposit operation request should include amount")
}

fn hl_deposit_coin_for_operation(operation: &OrderProviderOperation) -> String {
    let asset = operation
        .request
        .get("asset")
        .and_then(Value::as_str)
        .unwrap_or("");
    match asset {
        "btc" => "UBTC".to_string(),
        "eth" => "UETH".to_string(),
        other => panic!("unsupported UnitDeposit asset for HL credit: {other:?}"),
    }
}

fn hl_destination_for_unit_deposit(operation: &OrderProviderOperation) -> Address {
    let destination = operation
        .request
        .get("dst_addr")
        .and_then(Value::as_str)
        .expect("UnitDeposit operation request should include dst_addr");
    Address::from_str(destination).expect("UnitDeposit dst_addr should be an EVM address")
}

fn raw_units_to_f64(amount: U256, decimals: u32) -> f64 {
    amount.to_string().parse::<f64>().expect("amount fits f64") / 10f64.powi(decimals as i32)
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
