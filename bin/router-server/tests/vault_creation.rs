use alloy::network::TransactionBuilder;
use alloy::primitives::{keccak256, Address, Bytes, U256};
use alloy::providers::{ext::AnvilApi, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionRequest;
use bitcoin::Amount;
use bitcoincore_rpc_async::Auth;
use chains::{
    bitcoin::BitcoinChain,
    evm::{EvmChain, EvmGasSponsorConfig},
    hyperliquid::HyperliquidChain,
    ChainRegistry,
};
use chrono::Utc;
use devnet::{
    mock_integrators::{
        MockIntegratorConfig, MockIntegratorServer, MockUnitGenerateAddressRequest,
        MockUnitOperationKind, MockUnitOperationRecord,
    },
    RiftDevnet,
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use router_primitives::ChainType;
use router_server::{
    api::{
        CreateOrderRequest, CreateVaultRequest, MarketOrderQuoteKind, MarketOrderQuoteRequest,
        OrderFlowEnvelope, ProviderPolicyEnvelope, ProviderPolicyListEnvelope,
    },
    app::{initialize_components, PaymasterMode, RouterComponents},
    config::Settings,
    db::Database,
    models::{
        CustodyVault, CustodyVaultControlType, CustodyVaultRole, CustodyVaultStatus,
        CustodyVaultVisibility, DepositVaultFundingHint, DepositVaultStatus, MarketOrderAction,
        MarketOrderKind, MarketOrderKindType, MarketOrderQuote, OrderExecutionAttempt,
        OrderExecutionAttemptKind, OrderExecutionAttemptStatus, OrderExecutionStep,
        OrderExecutionStepStatus, OrderExecutionStepType, OrderProviderAddress,
        OrderProviderOperation, OrderProviderOperationHint, ProviderAddressRole,
        ProviderExecutionPolicyState, ProviderOperationHintKind, ProviderOperationHintStatus,
        ProviderOperationStatus, ProviderOperationType, ProviderQuotePolicyState, RouterOrder,
        RouterOrderAction, RouterOrderEnvelope, RouterOrderQuoteEnvelope, RouterOrderStatus,
        RouterOrderType, VaultAction,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    server::{build_api_router, AdminApiAuth, AppState, InternalApiAuth},
    services::{
        action_providers::{
            BridgeExecutionRequest, BridgeProvider, BridgeQuote, BridgeQuoteRequest,
            ExchangeExecutionRequest, ExchangeProvider, ExchangeQuote, ExchangeQuoteRequest,
            ProviderFuture, UnitDepositStepRequest, UnitProvider, UnitWithdrawalStepRequest,
            VeloraProvider,
        },
        deposit_address::derive_deposit_address_for_quote,
        market_order_planner::MarketOrderRoutePlanner,
        order_manager::{MarketOrderError, OrderManager},
        ActionProviderRegistry, AssetRegistry, ChainCall, CustodyAction, CustodyActionExecutor,
        CustodyActionRequest, EvmCall, OrderExecutionCrashInjector, OrderExecutionCrashPoint,
        OrderExecutionManager, ProviderAddressIntent, ProviderExecutionIntent,
        ProviderExecutionState, ProviderOperationIntent, ProviderOperationStatusUpdate,
        RouteCostService, RouteMinimumService, VaultManager, VeloraHttpProviderConfig,
    },
    RouterServerArgs,
};
use serde_json::{json, Value};
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection};
use std::{
    net::IpAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use uuid::Uuid;

const CANCELLATION_COMMITMENT_DOMAIN: &[u8] = b"router-server-cancel-v1";
const MOCK_ERC20_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
const EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI: u64 = 100_000_000_000_000;
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";

// ---------------------------------------------------------------------------
// Shared devnet harness (initialized once across all tests)
// ---------------------------------------------------------------------------

struct TestHarness {
    chain_registry: Arc<ChainRegistry>,
    _devnet: std::sync::Mutex<Option<RiftDevnet>>,
}

unsafe impl Sync for TestHarness {}

impl TestHarness {
    fn with_devnet<T>(&self, f: impl FnOnce(&RiftDevnet) -> T) -> T {
        let guard = self._devnet.lock().expect("devnet mutex poisoned");
        let devnet = guard.as_ref().expect("test devnet already cleaned up");
        f(devnet)
    }

    fn ethereum_endpoint_url(&self) -> url::Url {
        self.with_devnet(|devnet| devnet.ethereum.anvil.endpoint_url())
    }

    fn base_endpoint_url(&self) -> url::Url {
        self.with_devnet(|devnet| devnet.base.anvil.endpoint_url())
    }

    fn arbitrum_endpoint_url(&self) -> url::Url {
        self.with_devnet(|devnet| devnet.arbitrum.anvil.endpoint_url())
    }

    fn ethereum_ws_url(&self) -> String {
        self.with_devnet(|devnet| devnet.ethereum.anvil.ws_endpoint_url().to_string())
    }

    fn base_ws_url(&self) -> String {
        self.with_devnet(|devnet| devnet.base.anvil.ws_endpoint_url().to_string())
    }

    fn arbitrum_ws_url(&self) -> String {
        self.with_devnet(|devnet| devnet.arbitrum.anvil.ws_endpoint_url().to_string())
    }

    fn ethereum_spoke_pool_address(&self) -> String {
        self.with_devnet(|devnet| {
            format!(
                "{:#x}",
                devnet.ethereum.mock_across_spoke_pool_contract.address()
            )
        })
    }

    fn base_spoke_pool_address(&self) -> String {
        self.with_devnet(|devnet| {
            format!(
                "{:#x}",
                devnet.base.mock_across_spoke_pool_contract.address()
            )
        })
    }

    fn arbitrum_spoke_pool_address(&self) -> String {
        self.with_devnet(|devnet| {
            format!(
                "{:#x}",
                devnet.arbitrum.mock_across_spoke_pool_contract.address()
            )
        })
    }

    fn bitcoin_rpc_url(&self) -> String {
        self.with_devnet(|devnet| {
            format!(
                "http://{}:{}",
                devnet.bitcoin.regtest.params.rpc_socket.ip(),
                devnet.bitcoin.regtest.params.rpc_socket.port()
            )
        })
    }

    fn bitcoin_auth(&self) -> Auth {
        self.with_devnet(|devnet| Auth::CookieFile(devnet.bitcoin.cookie.clone()))
    }

    fn esplora_url(&self) -> String {
        self.with_devnet(|devnet| {
            devnet
                .bitcoin
                .esplora_url
                .as_ref()
                .expect("esplora required for tests")
                .clone()
        })
    }

    fn ethereum_funded_address(&self) -> Address {
        self.with_devnet(|devnet| devnet.ethereum.funded_address)
    }

    fn ethereum_spawned_api_paymaster_private_key(&self) -> String {
        self.with_devnet(|devnet| anvil_spawned_api_paymaster_private_key(&devnet.ethereum))
    }

    fn base_spawned_api_paymaster_private_key(&self) -> String {
        self.with_devnet(|devnet| anvil_spawned_api_paymaster_private_key(&devnet.base))
    }

    fn arbitrum_spawned_api_paymaster_private_key(&self) -> String {
        self.with_devnet(|devnet| anvil_spawned_api_paymaster_private_key(&devnet.arbitrum))
    }

    async fn deal_bitcoin(
        &self,
        address: &bitcoin::Address<bitcoin::address::NetworkChecked>,
        amount: &Amount,
    ) -> Result<(), devnet::DevnetError> {
        let bitcoin_devnet = self.with_devnet(|devnet| devnet.bitcoin.clone());
        bitcoin_devnet
            .deal_bitcoin(address, amount)
            .await
            .map(|_| ())
    }

    async fn wait_for_esplora_sync(&self, timeout: Duration) -> Result<(), devnet::DevnetError> {
        let bitcoin_devnet = self.with_devnet(|devnet| devnet.bitcoin.clone());
        bitcoin_devnet.wait_for_esplora_sync(timeout).await
    }

    async fn mine_bitcoin_blocks(&self, blocks: u64) -> Result<(), devnet::DevnetError> {
        let bitcoin_devnet = self.with_devnet(|devnet| devnet.bitcoin.clone());
        bitcoin_devnet.mine_blocks(blocks).await
    }

    fn drop_devnet(&self) {
        if let Ok(mut devnet) = self._devnet.lock() {
            let _ = devnet.take();
        }
    }
}

static HARNESS: tokio::sync::OnceCell<TestHarness> = tokio::sync::OnceCell::const_new();

async fn harness() -> &'static TestHarness {
    HARNESS
        .get_or_init(|| async {
            let (devnet, _) = RiftDevnet::builder()
                .using_esplora(true)
                .build()
                .await
                .expect("devnet setup failed");

            let bitcoin_rpc_url = format!(
                "http://{}:{}",
                devnet.bitcoin.regtest.params.rpc_socket.ip(),
                devnet.bitcoin.regtest.params.rpc_socket.port(),
            );
            let bitcoin_auth = Auth::CookieFile(devnet.bitcoin.cookie.clone());
            let esplora_url = devnet
                .bitcoin
                .esplora_url
                .as_ref()
                .expect("esplora required for tests")
                .clone();

            let bitcoin_chain = BitcoinChain::new(
                &bitcoin_rpc_url,
                bitcoin_auth,
                &esplora_url,
                bitcoin::Network::Regtest,
            )
            .expect("bitcoin chain setup");

            let eth_rpc = devnet.ethereum.anvil.endpoint();
            let ethereum_chain = EvmChain::new_with_gas_sponsor(
                &eth_rpc,
                MOCK_ERC20_ADDRESS,
                ChainType::Ethereum,
                b"router-ethereum-wallet",
                4,
                Duration::from_secs(12),
                Some(EvmGasSponsorConfig {
                    private_key: anvil_paymaster_private_key(&devnet.ethereum),
                    vault_gas_buffer_wei: U256::from(EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI),
                }),
            )
            .await
            .expect("ethereum chain setup");

            let base_rpc = devnet.base.anvil.endpoint();
            let base_chain = EvmChain::new_with_gas_sponsor(
                &base_rpc,
                MOCK_ERC20_ADDRESS,
                ChainType::Base,
                b"router-base-wallet",
                2,
                Duration::from_secs(2),
                Some(EvmGasSponsorConfig {
                    private_key: anvil_paymaster_private_key(&devnet.base),
                    vault_gas_buffer_wei: U256::from(EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI),
                }),
            )
            .await
            .expect("base chain setup");

            let arbitrum_rpc = devnet.arbitrum.anvil.endpoint();
            let arbitrum_chain = EvmChain::new_with_gas_sponsor(
                &arbitrum_rpc,
                MOCK_ERC20_ADDRESS,
                ChainType::Arbitrum,
                b"router-arbitrum-wallet",
                2,
                Duration::from_secs(2),
                Some(EvmGasSponsorConfig {
                    private_key: anvil_paymaster_private_key(&devnet.arbitrum),
                    vault_gas_buffer_wei: U256::from(EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI),
                }),
            )
            .await
            .expect("arbitrum chain setup");

            let hyperliquid_chain = Arc::new(HyperliquidChain::new(
                b"router-hyperliquid-wallet",
                1,
                Duration::from_secs(1),
            ));

            let mut registry = ChainRegistry::new();
            registry.register_bitcoin(ChainType::Bitcoin, Arc::new(bitcoin_chain));
            registry.register_evm(ChainType::Ethereum, Arc::new(ethereum_chain));
            registry.register_evm(ChainType::Arbitrum, Arc::new(arbitrum_chain));
            registry.register_evm(ChainType::Base, Arc::new(base_chain));
            registry.register(ChainType::Hyperliquid, hyperliquid_chain);

            TestHarness {
                chain_registry: Arc::new(registry),
                _devnet: std::sync::Mutex::new(Some(devnet)),
            }
        })
        .await
}

struct TestPostgres {
    admin_database_url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

static POSTGRES: tokio::sync::OnceCell<TestPostgres> = tokio::sync::OnceCell::const_new();
static POSTGRES_CONTAINER_ID: OnceLock<String> = OnceLock::new();
static CREATED_DATABASES: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

#[ctor::ctor]
fn serialize_vault_creation_tests() {
    std::env::set_var("RUST_TEST_THREADS", "1");
}

#[ctor::dtor]
fn cleanup_test_devnet_temp_dirs() {
    if let Some(harness) = HARNESS.get() {
        harness.drop_devnet();
    }
    let _ = devnet::cleanup_current_process_temp_dirs();
}

#[ctor::dtor]
fn cleanup_test_postgres_container() {
    if matches!(
        std::env::var("TESTCONTAINERS_COMMAND").as_deref(),
        Ok("keep")
    ) {
        return;
    }

    if let Some(container_id) = POSTGRES_CONTAINER_ID.get() {
        let _ = std::process::Command::new("docker")
            .args(["rm", "-f", container_id])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
        return;
    }

    let databases = CREATED_DATABASES
        .lock()
        .map(|mut databases| std::mem::take(&mut *databases))
        .unwrap_or_default();
    if databases.is_empty() {
        return;
    }

    let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    else {
        return;
    };

    runtime.block_on(async move {
        for (admin_database_url, database_name) in databases {
            let Ok(connect_options) = PgConnectOptions::from_str(&admin_database_url) else {
                continue;
            };
            let Ok(mut admin) = PgConnection::connect_with(&connect_options).await else {
                continue;
            };

            let force_drop = format!(r#"DROP DATABASE IF EXISTS "{database_name}" WITH (FORCE)"#);
            if sqlx_core::query::query(&force_drop)
                .execute(&mut admin)
                .await
                .is_err()
            {
                let drop = format!(r#"DROP DATABASE IF EXISTS "{database_name}""#);
                let _ = sqlx_core::query::query(&drop).execute(&mut admin).await;
            }
        }
    });
}

async fn test_postgres() -> &'static TestPostgres {
    POSTGRES
        .get_or_init(|| async {
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
            let _ = POSTGRES_CONTAINER_ID.set(container.id().to_string());
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
        })
        .await
}

async fn create_test_database(admin_database_url: &str) -> String {
    let connect_options =
        PgConnectOptions::from_str(admin_database_url).expect("parse test database admin URL");
    let mut admin = PgConnection::connect_with(&connect_options)
        .await
        .expect("connect to test database admin URL");
    let database_name = format!("test_db_{}", Uuid::now_v7().simple());

    sqlx_core::query::query(&format!(r#"CREATE DATABASE "{database_name}""#))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");
    CREATED_DATABASES
        .lock()
        .expect("track isolated test database")
        .push((admin_database_url.to_string(), database_name.clone()));

    let mut database_url =
        url::Url::parse(admin_database_url).expect("parse test database admin URL");
    database_url.set_path(&database_name);
    database_url.to_string()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn test_db() -> Database {
    let postgres = test_postgres().await;
    let url = create_test_database(&postgres.admin_database_url).await;

    Database::connect(&url, 5, 1)
        .await
        .expect("test database connection failed")
}

fn write_test_master_key(config_dir: &std::path::Path) -> std::path::PathBuf {
    let path = config_dir.join("router-server-master-key.hex");
    if !path.exists() {
        std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write test master key");
    }
    path
}

fn test_settings(dir: &std::path::Path) -> Settings {
    Settings::load(write_test_master_key(dir)).expect("test settings")
}

fn test_router_args(
    harness: &TestHarness,
    config_dir: &std::path::Path,
    database_url: String,
) -> RouterServerArgs {
    RouterServerArgs {
        host: IpAddr::from([127, 0, 0, 1]),
        port: 0,
        database_url,
        db_max_connections: 5,
        db_min_connections: 1,
        log_level: "info".to_string(),
        master_key_path: write_test_master_key(config_dir)
            .to_string_lossy()
            .to_string(),
        ethereum_mainnet_rpc_url: harness.ethereum_endpoint_url().to_string(),
        ethereum_reference_token: MOCK_ERC20_ADDRESS.to_string(),
        ethereum_paymaster_private_key: Some(harness.ethereum_spawned_api_paymaster_private_key()),
        base_rpc_url: harness.base_endpoint_url().to_string(),
        base_reference_token: MOCK_ERC20_ADDRESS.to_string(),
        base_paymaster_private_key: Some(harness.base_spawned_api_paymaster_private_key()),
        arbitrum_rpc_url: harness.arbitrum_endpoint_url().to_string(),
        arbitrum_reference_token: MOCK_ERC20_ADDRESS.to_string(),
        arbitrum_paymaster_private_key: Some(harness.arbitrum_spawned_api_paymaster_private_key()),
        evm_paymaster_vault_gas_buffer_wei: EVM_PAYMASTER_VAULT_GAS_BUFFER_WEI.to_string(),
        evm_paymaster_vault_gas_target_wei: None,
        bitcoin_rpc_url: harness.bitcoin_rpc_url(),
        bitcoin_rpc_auth: harness.bitcoin_auth(),
        untrusted_esplora_http_server_url: harness.esplora_url(),
        bitcoin_network: bitcoin::Network::Regtest,
        bitcoin_paymaster_private_key: Some(regtest_paymaster_private_key()),
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
        loki_url: None,
        across_api_url: None,
        across_api_key: None,
        across_integrator_id: None,
        cctp_api_url: None,
        cctp_token_messenger_v2_address: None,
        cctp_message_transmitter_v2_address: None,
        hyperunit_api_url: None,
        hyperunit_proxy_url: None,
        hyperliquid_api_url: None,
        velora_api_url: None,
        velora_partner: None,
        hyperliquid_execution_private_key: None,
        hyperliquid_account_address: None,
        hyperliquid_vault_address: None,
        hyperliquid_paymaster_private_key: Some(test_hyperliquid_paymaster_private_key()),
        router_detector_api_key: Some("test-secret".to_string()),
        router_admin_api_key: None,
        hyperliquid_network:
            router_server::services::custody_action_executor::HyperliquidCallNetwork::Mainnet,
        hyperliquid_order_timeout_ms: 30_000,
        worker_id: None,
        worker_lease_name: Some("global-router-worker".to_string()),
        worker_lease_seconds: 300,
        worker_lease_renew_seconds: 30,
        worker_standby_poll_seconds: 5,
        worker_refund_poll_seconds: 60,
        worker_order_execution_poll_seconds: 5,
        worker_route_cost_refresh_seconds: 300,
        coinbase_price_api_base_url: "http://127.0.0.1:9".to_string(),
    }
}

async fn spawn_router_api(args: RouterServerArgs) -> (String, tokio::task::JoinHandle<()>) {
    let cors_domain = args.cors_domain.clone();
    let internal_api_auth = InternalApiAuth::from_args(&args);
    let admin_api_auth = AdminApiAuth::from_args(&args);
    let components = initialize_components(&args, None, PaymasterMode::Disabled)
        .await
        .expect("initialize router components");
    spawn_router_api_from_components_with_auth(
        components,
        cors_domain,
        internal_api_auth,
        admin_api_auth,
    )
    .await
}

async fn spawn_router_api_from_components(
    components: RouterComponents,
    cors_domain: Option<String>,
) -> (String, tokio::task::JoinHandle<()>) {
    spawn_router_api_from_components_with_auth(components, cors_domain, None, None).await
}

async fn spawn_router_api_from_components_with_auth(
    components: RouterComponents,
    cors_domain: Option<String>,
    internal_api_auth: Option<InternalApiAuth>,
    admin_api_auth: Option<AdminApiAuth>,
) -> (String, tokio::task::JoinHandle<()>) {
    let app = build_api_router(
        AppState {
            db: components.db,
            vault_manager: components.vault_manager,
            order_manager: components.order_manager,
            order_execution_manager: components.order_execution_manager,
            provider_policies: components.provider_policies,
            address_screener: components.address_screener,
            chain_registry: components.chain_registry,
            internal_api_auth,
            admin_api_auth,
        },
        cors_domain,
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind test router api");
    let addr = listener.local_addr().expect("test router api addr");
    let handle = tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, app).await {
            panic!("test router api failed: {error}");
        }
    });

    (format!("http://{addr}"), handle)
}

async fn test_vault_manager(config_dir: &std::path::Path) -> VaultManager {
    test_vault_manager_with_db(config_dir).await.0
}

async fn test_vault_manager_with_db(config_dir: &std::path::Path) -> (VaultManager, Database) {
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(config_dir));
    (
        VaultManager::new(db.clone(), settings, h.chain_registry.clone()),
        db,
    )
}

async fn test_order_manager(
    action_providers: Arc<ActionProviderRegistry>,
) -> (OrderManager, Database) {
    let h = harness().await;
    let db = test_db().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    (
        OrderManager::with_action_providers(
            db.clone(),
            settings,
            h.chain_registry.clone(),
            action_providers,
        ),
        db,
    )
}

async fn mock_order_manager(
    db: Database,
    chain_registry: Arc<ChainRegistry>,
) -> (OrderManager, MockIntegratorServer) {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let dir = tempfile::tempdir().unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let order_manager = OrderManager::with_action_providers(
        db,
        settings,
        chain_registry,
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
    );
    (order_manager, mocks)
}

/// Spawn a `MockIntegratorServer` wired to the shared harness devnet so the
/// real-path Across flow works end-to-end: the mock's `/swap/approval` response
/// points at the harness's on-chain `MockSpokePool`, and the mock's indexer
/// subscribes to that contract's `FundsDeposited` events on the harness's
/// Anvil ws endpoint. The origin chain determines which EVM devnet we wire:
/// `"evm:1"` → Ethereum, `"evm:8453"` → Base, `"evm:42161"` → Arbitrum.
async fn spawn_harness_mocks(h: &TestHarness, origin_chain_id: &str) -> MockIntegratorServer {
    let (spoke_pool, ws_url) = match origin_chain_id {
        "evm:1" => (h.ethereum_spoke_pool_address(), h.ethereum_ws_url()),
        "evm:8453" => (h.base_spoke_pool_address(), h.base_ws_url()),
        "evm:42161" => (h.arbitrum_spoke_pool_address(), h.arbitrum_ws_url()),
        other => panic!("spawn_harness_mocks: unsupported origin chain {other}"),
    };
    MockIntegratorServer::spawn_with_config(
        MockIntegratorConfig::default()
            .with_across_spoke_pool_address(spoke_pool)
            .with_across_evm_rpc_url(ws_url)
            .with_mainnet_hyperliquid(true),
    )
    .await
    .expect("spawn harness-wired mock integrator")
}

fn make_cancellation_pair() -> (String, String) {
    let mut secret = [0u8; 32];
    getrandom::getrandom(&mut secret).unwrap();
    let commitment: [u8; 32] =
        keccak256([CANCELLATION_COMMITMENT_DOMAIN, secret.as_slice()].concat()).into();
    (
        format!("0x{}", alloy::hex::encode(secret)),
        format!("0x{}", alloy::hex::encode(commitment)),
    )
}

/// Dedicated anvil account for the EVM paymaster in tests. Kept separate from
/// account #0 because test helpers like `anvil_mint_erc20` and
/// `anvil_send_native_on` submit from account #0 directly, which advances its
/// on-chain nonce outside the long-lived paymaster actor's cached NonceFiller
/// — producing "nonce too low" errors when the paymaster later tries to sponsor
/// gas.
fn anvil_paymaster_private_key(devnet: &devnet::EthDevnet) -> String {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[9].to_bytes())
    )
}

/// Dedicated anvil account for paymasters hosted inside tests that spawn their
/// own router-server API (via `test_router_args` + `spawn_router_api`). Each
/// such spawn creates its own paymaster actor, whose txs advance the on-chain
/// nonce for the sponsor account; if that account matched the long-lived
/// harness paymaster (`anvil_paymaster_private_key`, account #9), the harness
/// paymaster's cached NonceFiller would go stale at the next test.
fn anvil_spawned_api_paymaster_private_key(devnet: &devnet::EthDevnet) -> String {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[8].to_bytes())
    )
}

fn dummy_commitment() -> String {
    format!("0x{}", alloy::hex::encode([0xaa; 32]))
}

fn valid_evm_address() -> String {
    "0x1111111111111111111111111111111111111111".to_string()
}

fn valid_regtest_btc_address() -> String {
    "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string()
}

fn regtest_paymaster_private_key() -> String {
    let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&[7_u8; 32]).expect("secret key");
    bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest).to_wif()
}

fn test_hyperliquid_paymaster_private_key() -> String {
    "0x59c6995e998f97a5a0044976f7ad0a7df4976fbe66f6cc18ff3c16f18a6b9e3f".to_string()
}

fn assert_path_provider_id(provider_id: &str, expected_fragments: &[&str]) {
    assert!(
        provider_id.starts_with("path:"),
        "provider id should describe the selected transition path: {provider_id}"
    );
    for fragment in expected_fragments {
        assert!(
            provider_id.contains(fragment),
            "provider id {provider_id} should contain {fragment}"
        );
    }
}

/// Step-request mirror used when asserting what the planner persisted into
/// `execution_steps.request`. Matches `UnitDepositStepRequest` /
/// `UnitWithdrawalStepRequest` in `action_providers.rs` — redefined here so
/// test code can deserialize the JSON without importing internal types.
#[derive(Debug, serde::Deserialize)]
struct MockUnitDepositStepRequest {
    #[allow(dead_code)]
    #[serde(default)]
    order_id: Option<Uuid>,
    src_chain_id: String,
    dst_chain_id: String,
    asset_id: String,
    #[allow(dead_code)]
    amount: String,
    #[serde(default)]
    source_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    revert_custody_vault_id: Option<Uuid>,
    #[serde(default)]
    source_custody_vault_role: Option<String>,
    #[serde(default)]
    revert_custody_vault_role: Option<String>,
    #[serde(default)]
    hyperliquid_custody_vault_role: Option<String>,
}

fn filter_unit_requests_by_kind(
    requests: &[MockUnitGenerateAddressRequest],
    kind: MockUnitOperationKind,
) -> Vec<&MockUnitGenerateAddressRequest> {
    requests
        .iter()
        .filter(|r| match kind {
            MockUnitOperationKind::Deposit => r.dst_chain == "hyperliquid",
            MockUnitOperationKind::Withdrawal => r.src_chain == "hyperliquid",
        })
        .collect()
}

fn filter_unit_operations_by_kind(
    operations: &[MockUnitOperationRecord],
    kind: MockUnitOperationKind,
) -> Vec<&MockUnitOperationRecord> {
    operations.iter().filter(|op| op.kind == kind).collect()
}

#[derive(Clone)]
struct CustodyIntentUnitProvider {
    deposit_sink: String,
}

impl UnitProvider for CustodyIntentUnitProvider {
    fn id(&self) -> &str {
        "unit"
    }

    fn supports_deposit(&self, asset: &DepositAsset) -> bool {
        matches!(asset.chain.as_str(), "evm:1") && asset.asset.is_native()
    }

    fn supports_withdrawal(&self, asset: &DepositAsset) -> bool {
        matches!(asset.chain.as_str(), "evm:8453") && asset.asset.is_native()
    }

    fn execute_deposit<'a>(
        &'a self,
        request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let custody_vault_id = request.source_custody_vault_id.ok_or_else(|| {
                "unit deposit request is missing source_custody_vault_id".to_string()
            })?;
            let amount = request.amount.clone();

            Ok(ProviderExecutionIntent::CustodyAction {
                custody_vault_id,
                action: CustodyAction::Transfer {
                    to_address: self.deposit_sink.clone(),
                    amount,
                },
                provider_context: json!({
                    "mock_provider": "custody-intent-unit",
                    "operation": "deposit"
                }),
                state: Default::default(),
            })
        })
    }

    fn execute_withdrawal<'a>(
        &'a self,
        request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({
                    "mock_provider": "custody-intent-unit",
                    "operation": "withdrawal",
                    "request": request
                }),
                state: Default::default(),
            })
        })
    }

    fn observe_unit_operation<'a>(
        &'a self,
        request: router_server::services::ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<router_server::services::ProviderOperationObservation>> {
        Box::pin(async move {
            let Some(tx_hash) = request.hint_evidence.get("tx_hash").and_then(Value::as_str) else {
                return Ok(Some(
                    router_server::services::ProviderOperationObservation {
                        status: ProviderOperationStatus::WaitingExternal,
                        provider_ref: request.provider_ref,
                        observed_state: json!({
                            "status": "waiting_for_chain_evidence",
                            "hint_evidence": request.hint_evidence,
                        }),
                        response: None,
                        tx_hash: None,
                        error: None,
                    },
                ));
            };

            Ok(Some(
                router_server::services::ProviderOperationObservation {
                    status: ProviderOperationStatus::Completed,
                    provider_ref: request.provider_ref,
                    observed_state: json!({
                        "status": "done",
                        "hint_evidence": request.hint_evidence,
                    }),
                    response: None,
                    tx_hash: Some(tx_hash.to_string()),
                    error: None,
                },
            ))
        })
    }
}

#[derive(Clone)]
struct ObservationUnitProvider;

impl UnitProvider for ObservationUnitProvider {
    fn id(&self) -> &str {
        "unit"
    }

    fn supports_deposit(&self, asset: &DepositAsset) -> bool {
        matches!(asset.chain.as_str(), "evm:1") && asset.asset.is_native()
    }

    fn supports_withdrawal(&self, asset: &DepositAsset) -> bool {
        matches!(asset.chain.as_str(), "evm:8453") && asset.asset.is_native()
    }

    fn execute_deposit<'a>(
        &'a self,
        request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            let order_id = request.order_id;
            let provider_ref = format!("unit-observation-{order_id}");
            let deposit_address = "0x2000000000000000000000000000000000000001".to_string();
            let response = json!({
                "operation_id": provider_ref,
                "deposit_address": deposit_address,
                "status": "submitted"
            });

            Ok(ProviderExecutionIntent::ProviderOnly {
                response: response.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UnitDeposit,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(provider_ref),
                        request: None,
                        response: Some(response),
                        observed_state: None,
                    }),
                    addresses: vec![ProviderAddressIntent {
                        role: ProviderAddressRole::UnitDeposit,
                        chain: ChainId::parse("evm:1").unwrap(),
                        asset: Some(AssetId::Native),
                        address: deposit_address,
                        memo: None,
                        expires_at: None,
                        metadata: Some(json!({
                            "source": "observation_unit_provider"
                        })),
                    }],
                },
            })
        })
    }

    fn execute_withdrawal<'a>(
        &'a self,
        request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({
                    "mock_provider": "observation-unit",
                    "operation": "withdrawal",
                    "request": request
                }),
                state: Default::default(),
            })
        })
    }

    fn observe_unit_operation<'a>(
        &'a self,
        request: router_server::services::ProviderOperationObservationRequest,
    ) -> ProviderFuture<'a, Option<router_server::services::ProviderOperationObservation>> {
        Box::pin(async move {
            let Some(tx_hash) = request.hint_evidence.get("tx_hash").and_then(Value::as_str) else {
                return Ok(Some(
                    router_server::services::ProviderOperationObservation {
                        status: ProviderOperationStatus::WaitingExternal,
                        provider_ref: request.provider_ref,
                        observed_state: json!({
                            "status": "waiting_for_chain_evidence",
                            "hint_evidence": request.hint_evidence,
                        }),
                        response: None,
                        tx_hash: None,
                        error: None,
                    },
                ));
            };

            Ok(Some(
                router_server::services::ProviderOperationObservation {
                    status: ProviderOperationStatus::Completed,
                    provider_ref: request.provider_ref,
                    observed_state: json!({
                        "status": "done",
                        "hint_evidence": request.hint_evidence,
                    }),
                    response: None,
                    tx_hash: Some(tx_hash.to_string()),
                    error: None,
                },
            ))
        })
    }
}

#[derive(Clone)]
struct FailingRefundBridgeProvider;

impl BridgeProvider for FailingRefundBridgeProvider {
    fn id(&self) -> &str {
        "across"
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let amount_in = match &request.order_kind {
                MarketOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                MarketOrderKind::ExactOut { amount_out, .. } => amount_out.clone(),
            };
            Ok(Some(BridgeQuote {
                provider_id: self.id().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_in,
                provider_quote: json!({
                    "mock_provider": "failing_refund_bridge",
                }),
                expires_at: Utc::now() + chrono::Duration::seconds(30),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        _request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move { Err("mock refund route unavailable".to_string()) })
    }
}

#[derive(Clone)]
struct ProviderOnlyExchangeProvider;

impl ExchangeProvider for ProviderOnlyExchangeProvider {
    fn id(&self) -> &str {
        "hyperliquid"
    }

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>> {
        Box::pin(async move {
            let (amount_in, amount_out, min_amount_out, max_amount_in) = match request.order_kind {
                MarketOrderKind::ExactIn {
                    amount_in,
                    min_amount_out,
                } => (amount_in.clone(), amount_in, Some(min_amount_out), None),
                MarketOrderKind::ExactOut {
                    amount_out,
                    max_amount_in,
                } => (amount_out.clone(), amount_out, None, Some(max_amount_in)),
            };
            Ok(Some(ExchangeQuote {
                provider_id: "hyperliquid".to_string(),
                amount_in,
                amount_out,
                min_amount_out,
                max_amount_in,
                provider_quote: json!({
                    "kind": "spot_no_op",
                    "mock_provider": "provider-only-exchange"
                }),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
            }))
        })
    }

    fn execute_trade<'a>(
        &'a self,
        request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({
                    "mock_provider": "provider-only-exchange",
                    "operation": "trade",
                    "request": request
                }),
                state: Default::default(),
            })
        })
    }
}

#[derive(Clone)]
struct FailingBridgeProvider;

impl BridgeProvider for FailingBridgeProvider {
    fn id(&self) -> &str {
        "across"
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let (amount_in, amount_out) = match request.order_kind {
                MarketOrderKind::ExactIn {
                    amount_in,
                    min_amount_out: _,
                } => (amount_in.clone(), amount_in),
                MarketOrderKind::ExactOut {
                    amount_out,
                    max_amount_in: _,
                } => (amount_out.clone(), amount_out),
            };
            Ok(Some(BridgeQuote {
                provider_id: "across".to_string(),
                amount_in,
                amount_out,
                provider_quote: json!({
                    "kind": "failing_bridge",
                    "mock_provider": "failing-bridge"
                }),
                expires_at: Utc::now() + chrono::Duration::minutes(5),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        _request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async move { Err("bridge execution failed intentionally".to_string()) })
    }
}

struct PanicOnceCrashInjector {
    point: OrderExecutionCrashPoint,
    tripped: AtomicBool,
}

impl PanicOnceCrashInjector {
    fn new(point: OrderExecutionCrashPoint) -> Self {
        Self {
            point,
            tripped: AtomicBool::new(false),
        }
    }
}

impl OrderExecutionCrashInjector for PanicOnceCrashInjector {
    fn trigger(&self, point: OrderExecutionCrashPoint) {
        if point == self.point && !self.tripped.swap(true, Ordering::SeqCst) {
            panic!("test crash injector triggered at {point:?}");
        }
    }
}

async fn anvil_set_balance(h: &TestHarness, address: Address, amount: U256) {
    let provider = ProviderBuilder::new().connect_http(h.ethereum_endpoint_url());
    provider
        .anvil_set_balance(address, amount)
        .await
        .expect("anvil_set_balance");
}

async fn anvil_set_base_balance(h: &TestHarness, address: Address, amount: U256) {
    let provider = ProviderBuilder::new().connect_http(h.base_endpoint_url());
    provider
        .anvil_set_balance(address, amount)
        .await
        .expect("anvil_set_balance on base");
}

async fn anvil_set_arbitrum_balance(h: &TestHarness, address: Address, amount: U256) {
    let provider = ProviderBuilder::new().connect_http(h.arbitrum_endpoint_url());
    provider
        .anvil_set_balance(address, amount)
        .await
        .expect("anvil_set_balance on arbitrum");
}

async fn anvil_mint_erc20(h: &TestHarness, address: Address, amount: U256) {
    // Fresh random signer per call so parallel tests don't collide on nonces;
    // the mock ERC20's `mint` has no access control, so any funded EOA works.
    let signer = alloy::signers::local::PrivateKeySigner::random();
    let sender_address = signer.address();
    let wallet = alloy::network::EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(h.ethereum_endpoint_url());
    provider
        .anvil_set_balance(sender_address, U256::from(1_000_000_000_000_000_000_u128))
        .await
        .expect("anvil_set_balance for ephemeral erc20 minter");
    let contract = GenericEIP3009ERC20Instance::new(MOCK_ERC20_ADDRESS.parse().unwrap(), &provider);
    contract
        .mint(address, amount)
        .send()
        .await
        .expect("mint erc20")
        .get_receipt()
        .await
        .expect("mint erc20 receipt");
}

async fn anvil_clone_base_mock_erc20_code(h: &TestHarness, address: Address) {
    let provider = ProviderBuilder::new().connect_http(h.base_endpoint_url());
    let code = provider
        .get_code_at(MOCK_ERC20_ADDRESS.parse().unwrap())
        .await
        .expect("read base mock erc20 code");
    provider
        .anvil_set_code(address, code)
        .await
        .expect("anvil_set_code for arbitrary base erc20");
}

async fn anvil_send_ethereum_native(
    h: &TestHarness,
    recipient: Address,
    amount: U256,
) -> (String, u64) {
    anvil_send_native_on(h, h.ethereum_endpoint_url(), recipient, amount).await
}

async fn anvil_send_base_native(
    h: &TestHarness,
    recipient: Address,
    amount: U256,
) -> (String, u64) {
    anvil_send_native_on(h, h.base_endpoint_url(), recipient, amount).await
}

async fn anvil_send_arbitrum_native(
    h: &TestHarness,
    recipient: Address,
    amount: U256,
) -> (String, u64) {
    anvil_send_native_on(h, h.arbitrum_endpoint_url(), recipient, amount).await
}

async fn anvil_send_native_on(
    _h: &TestHarness,
    endpoint: url::Url,
    recipient: Address,
    amount: U256,
) -> (String, u64) {
    // Use a fresh random signer per call so parallel tests don't collide on nonces.
    let signer = alloy::signers::local::PrivateKeySigner::random();
    let sender_address = signer.address();
    let wallet = alloy::network::EthereumWallet::new(signer);
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(endpoint);
    provider
        .anvil_set_balance(
            sender_address,
            amount + U256::from(1_000_000_000_000_000_000_u128),
        )
        .await
        .expect("anvil_set_balance for ephemeral sender");
    let tx = TransactionRequest::default()
        .with_to(recipient)
        .with_value(amount);
    let receipt = provider
        .send_transaction(tx)
        .await
        .expect("send native deposit")
        .get_receipt()
        .await
        .expect("native deposit receipt");
    assert!(receipt.status(), "native deposit transaction reverted");
    (
        receipt.transaction_hash.to_string(),
        receipt.transaction_index.unwrap_or(0),
    )
}

fn evm_native_request() -> CreateVaultRequest {
    CreateVaultRequest {
        order_id: None,
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Native,
        },
        action: VaultAction::Null,
        recovery_address: valid_evm_address(),
        cancellation_commitment: dummy_commitment(),
        cancel_after: None,
        metadata: json!({}),
    }
}

async fn create_test_order_from_quote(
    order_manager: &OrderManager,
    quote_id: Uuid,
) -> router_server::models::RouterOrder {
    create_test_order_from_quote_with_refund(order_manager, quote_id, valid_evm_address()).await
}

async fn create_test_order_from_quote_with_refund(
    order_manager: &OrderManager,
    quote_id: Uuid,
    refund_address: String,
) -> router_server::models::RouterOrder {
    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id,
            refund_address,
            cancel_after: None,
            idempotency_key: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    order
}

struct FundedMarketOrderFixture {
    db: Database,
    order_id: Uuid,
    vault_id: Uuid,
    vault_address: Address,
    mocks: MockIntegratorServer,
    settings: Arc<Settings>,
    custody_action_executor: Arc<CustodyActionExecutor>,
    chain_registry: Arc<ChainRegistry>,
}

async fn funded_market_order_fixture(
    source_chain_id: &str,
    destination_chain_id: &str,
) -> FundedMarketOrderFixture {
    funded_market_order_fixture_with_amount(source_chain_id, destination_chain_id, "1000").await
}

async fn funded_market_order_fixture_with_amount(
    source_chain_id: &str,
    destination_chain_id: &str,
    amount_in: &str,
) -> FundedMarketOrderFixture {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, source_chain_id).await;
    let settings = Arc::new(test_settings(dir.path()));
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(mocks.base_url()));
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());
    let source_asset = DepositAsset {
        chain: ChainId::parse(source_chain_id).unwrap(),
        asset: AssetId::Native,
    };

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: DepositAsset {
                chain: ChainId::parse(destination_chain_id).unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            ..evm_native_request()
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid source vault address");
    let amount_in = U256::from_str_radix(amount_in, 10).expect("valid test amount_in");
    // Fund the source vault with the deposit amount plus a 1 ETH pad so it has
    // enough native balance to cover gas when it later signs UnitDeposit /
    // AcrossBridge transfers out.
    let source_balance = amount_in + U256::from(1_000_000_000_000_000_000_u128);
    match source_chain_id {
        "evm:1" => anvil_set_balance(h, vault_address, source_balance).await,
        "evm:8453" => anvil_set_base_balance(h, vault_address, source_balance).await,
        "evm:42161" => anvil_set_arbitrum_balance(h, vault_address, source_balance).await,
        other => panic!("funded_market_order_fixture: unsupported source chain {other}"),
    }
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let custody_action_executor = Arc::new(CustodyActionExecutor::new(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
    ));

    FundedMarketOrderFixture {
        db,
        order_id: order.id,
        vault_id: vault.id,
        vault_address,
        mocks,
        settings,
        custody_action_executor,
        chain_registry: h.chain_registry.clone(),
    }
}

async fn expect_task_panic<T: std::fmt::Debug>(handle: tokio::task::JoinHandle<T>) {
    let join_error = handle.await.expect_err("task should panic");
    assert!(
        join_error.is_panic(),
        "expected task panic, got {join_error}"
    );
}

async fn record_detector_provider_status_hint(
    execution_manager: &OrderExecutionManager,
    operation: &OrderProviderOperation,
) {
    let hint_id = Uuid::now_v7();
    execution_manager
        .record_provider_operation_hint(OrderProviderOperationHint {
            id: hint_id,
            provider_operation_id: operation.id,
            source: "test-detector".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({
                "source": "test-detector",
                "backend": "provider_operation",
                "provider": &operation.provider,
                "operation_type": operation.operation_type.to_db_string(),
                "provider_ref": &operation.provider_ref,
                "observed_at": Utc::now()
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some(format!(
                "test-detector-provider-status-hint-{}-{hint_id}",
                operation.id
            )),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

/// Wait for the mock Across indexer to observe a `FundsDeposited` event from
/// the specified depositor, confirming the on-chain deposit landed and the
/// mock's `/deposit/status` will now report `"filled"` for that deposit id.
/// Scoped by depositor so parallel tests sharing the anvil spoke pool don't
/// witness each other's deposits and return early.
async fn wait_for_across_deposit_indexed(
    mocks: &MockIntegratorServer,
    depositor: Address,
    timeout: Duration,
) {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if !mocks.across_deposits_from(depositor).await.is_empty() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "mock Across indexer never observed a FundsDeposited event from {depositor:#x} within {timeout:?}",
    );
}

/// Drive an Across → (optional Unit deposit) → (optional Unit withdrawal) →
/// (optional Hyperliquid) market order to terminal state. Each leg is advanced
/// by (a) a worker pass to kick off the external action, (b) completing the
/// mock-side effect for that leg, (c) recording a detector hint, and (d)
/// processing hints so the operation can transition out of WaitingExternal.
async fn drive_across_order_to_completion(
    execution_manager: &OrderExecutionManager,
    mocks: &MockIntegratorServer,
    db: &Database,
    order_id: Uuid,
) {
    // Across bridge leg.
    execution_manager.process_worker_pass(10).await.unwrap();
    let source_vault_address = source_deposit_vault_address_from_db(db, order_id).await;
    wait_for_across_deposit_indexed(mocks, source_vault_address, Duration::from_secs(10)).await;
    let across_operation =
        provider_operation_by_type(db, order_id, ProviderOperationType::AcrossBridge).await;
    record_detector_provider_status_hint(execution_manager, &across_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    // Simulate the Across relayer delivering funds to the destination-execution
    // custody vault on the destination chain. Real Across fulfills cross-chain;
    // the mock only records the source-chain deposit event, so tests must credit
    // the destination vault themselves before UnitDeposit tries to move funds.
    fund_destination_execution_vault_from_across(db, order_id).await;

    // Unit deposit leg (if the route includes one).
    execution_manager.process_worker_pass(10).await.unwrap();
    if let Some(unit_deposit) =
        maybe_provider_operation_by_type(db, order_id, ProviderOperationType::UnitDeposit).await
    {
        mocks
            .complete_unit_operation(provider_ref(&unit_deposit))
            .await
            .unwrap();
        record_chain_detector_unit_deposit_hint(execution_manager, db, &unit_deposit).await;
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap();
        let unit_deposit = db
            .orders()
            .get_provider_operation(unit_deposit.id)
            .await
            .unwrap();
        credit_hyperliquid_spot_from_unit_deposit(mocks, db, order_id, &unit_deposit).await;
    }

    // Unit withdrawal leg (if the route includes one).
    execution_manager.process_worker_pass(10).await.unwrap();
    if let Some(unit_withdrawal) =
        maybe_provider_operation_by_type(db, order_id, ProviderOperationType::UnitWithdrawal).await
    {
        mocks
            .complete_unit_operation(provider_ref(&unit_withdrawal))
            .await
            .unwrap();
        record_detector_provider_status_hint(execution_manager, &unit_withdrawal).await;
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap();
    }

    // Remaining legs (e.g., Hyperliquid exchange) drive themselves to completion
    // via the worker pass loop.
    let start = std::time::Instant::now();
    loop {
        execution_manager.process_worker_pass(10).await.unwrap();
        let order = db.orders().get(order_id).await.unwrap();
        if matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::Refunding
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::Failed
        ) {
            if !matches!(order.status, RouterOrderStatus::Completed) {
                dump_order_state(db, order_id).await;
            }
            return;
        }
        if start.elapsed() >= Duration::from_secs(10) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    dump_order_state(db, order_id).await;
    panic!(
        "order {} did not reach a terminal state within 10s after Across completion",
        order_id
    );
}

async fn dump_order_state(db: &Database, order_id: Uuid) {
    let steps = db.orders().get_execution_steps(order_id).await.unwrap();
    for step in &steps {
        eprintln!(
            "step #{} type={:?} status={:?} error={}",
            step.step_index, step.step_type, step.status, step.error
        );
    }
    let operations = db.orders().get_provider_operations(order_id).await.unwrap();
    for op in &operations {
        eprintln!(
            "operation type={:?} status={:?} provider_ref={:?} request={} response={:?} observed_state={}",
            op.operation_type, op.status, op.provider_ref, op.request, op.response, op.observed_state
        );
    }
}

async fn record_chain_detector_unit_deposit_hint(
    execution_manager: &OrderExecutionManager,
    db: &Database,
    operation: &OrderProviderOperation,
) {
    let h = harness().await;
    let address = db
        .orders()
        .get_provider_addresses_by_operation(operation.id)
        .await
        .unwrap()
        .into_iter()
        .find(|address| address.role == ProviderAddressRole::UnitDeposit)
        .expect("unit deposit operation should have a provider address");
    let amount_str = if let Some(amount) = operation
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
    {
        amount.to_string()
    } else if let Some(step_id) = operation.execution_step_id {
        db.orders()
            .get_execution_step(step_id)
            .await
            .unwrap()
            .amount_in
            .unwrap_or_else(|| "1000".to_string())
    } else {
        "1000".to_string()
    };
    let amount = U256::from_str_radix(&amount_str, 10).expect("expected_amount parses as U256");
    let recipient = Address::from_str(&address.address).expect("valid unit deposit address");
    let (tx_hash, transfer_index) = match address.chain.as_str() {
        "evm:1" => anvil_send_ethereum_native(h, recipient, amount).await,
        "evm:8453" => anvil_send_base_native(h, recipient, amount).await,
        "evm:42161" => anvil_send_arbitrum_native(h, recipient, amount).await,
        other => panic!(
            "record_chain_detector_unit_deposit_hint: unsupported chain {}",
            other
        ),
    };
    record_chain_detector_unit_deposit_hint_with_evidence(
        execution_manager,
        db,
        operation,
        tx_hash,
        transfer_index,
    )
    .await;
}

async fn record_chain_detector_unit_deposit_hint_with_evidence(
    execution_manager: &OrderExecutionManager,
    db: &Database,
    operation: &OrderProviderOperation,
    tx_hash: String,
    transfer_index: u64,
) {
    let address = db
        .orders()
        .get_provider_addresses_by_operation(operation.id)
        .await
        .unwrap()
        .into_iter()
        .find(|address| address.role == ProviderAddressRole::UnitDeposit)
        .expect("unit deposit operation should have a provider address");
    let amount = operation
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
        .unwrap_or("1000");
    let hint_id = Uuid::now_v7();
    execution_manager
        .record_provider_operation_hint(OrderProviderOperationHint {
            id: hint_id,
            provider_operation_id: operation.id,
            source: "test-chain-detector".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({
                "source": "test-chain-detector",
                "backend": "mock_chain_deposit",
                "chain": address.chain.as_str(),
                "address": address.address,
                "tx_hash": tx_hash,
                "transfer_index": transfer_index,
                "amount": amount,
                "observed_at": Utc::now()
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some(format!(
                "test-chain-detector-unit-deposit-hint-{}-{hint_id}",
                operation.id
            )),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

fn provider_ref(operation: &OrderProviderOperation) -> &str {
    operation
        .provider_ref
        .as_deref()
        .expect("mock provider operation should have a provider ref")
}

async fn provider_operation_by_type(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
) -> OrderProviderOperation {
    let operations = db.orders().get_provider_operations(order_id).await.unwrap();
    if let Some(operation) = operations
        .into_iter()
        .find(|operation| operation.operation_type == operation_type)
    {
        return operation;
    }
    dump_order_state(db, order_id).await;
    panic!(
        "expected provider operation {} for order {order_id}",
        operation_type.to_db_string()
    )
}

/// Credit the `DestinationExecution` custody vault on the destination chain
/// with the Across bridge's expected output amount. Mirrors what a real Across
/// relayer would do by fulfilling the cross-chain transfer.
async fn fund_destination_execution_vault_from_across(db: &Database, order_id: Uuid) {
    let vaults = db.orders().get_custody_vaults(order_id).await.unwrap();
    let destination_vault = vaults
        .into_iter()
        .find(|v| v.role == CustodyVaultRole::DestinationExecution)
        .expect("across step should have created a destination_execution custody vault");
    let across_op =
        provider_operation_by_type(db, order_id, ProviderOperationType::AcrossBridge).await;
    let amount_str = across_op
        .response
        .get("expectedOutputAmount")
        .and_then(Value::as_str)
        .expect("across response should include expectedOutputAmount");
    let output_amount =
        U256::from_str_radix(amount_str, 10).expect("expectedOutputAmount parses as U256");
    // Pad by 1 ETH so the vault has enough native balance to cover gas when it
    // signs the UnitDeposit transfer that follows.
    let balance = output_amount + U256::from(1_000_000_000_000_000_000_u128);
    let address = Address::from_str(&destination_vault.address)
        .expect("destination vault address is a valid EVM address");
    let h = harness().await;
    match destination_vault.chain.as_str() {
        "evm:1" => anvil_set_balance(h, address, balance).await,
        "evm:8453" => anvil_set_base_balance(h, address, balance).await,
        "evm:42161" => anvil_set_arbitrum_balance(h, address, balance).await,
        other => panic!(
            "fund_destination_execution_vault_from_across: unsupported chain {}",
            other
        ),
    }
}

async fn source_deposit_vault_address_from_db(db: &Database, order_id: Uuid) -> Address {
    let vaults = db.orders().get_custody_vaults(order_id).await.unwrap();
    let source_vault = vaults
        .into_iter()
        .find(|v| v.role == CustodyVaultRole::SourceDeposit)
        .expect("order should have a source_deposit custody vault");
    Address::from_str(&source_vault.address)
        .expect("source deposit vault address is a valid EVM address")
}

async fn zero_source_deposit_vault_balance(db: &Database, order_id: Uuid, chain_id: &str) {
    let address = source_deposit_vault_address_from_db(db, order_id).await;
    let h = harness().await;
    match chain_id {
        "evm:1" => anvil_set_balance(h, address, U256::ZERO).await,
        "evm:8453" => anvil_set_base_balance(h, address, U256::ZERO).await,
        "evm:42161" => anvil_set_arbitrum_balance(h, address, U256::ZERO).await,
        other => panic!("unsupported source chain for zeroing source deposit vault: {other}"),
    }
}

async fn internal_custody_vaults_for_order(db: &Database, order_id: Uuid) -> Vec<CustodyVault> {
    db.orders()
        .get_custody_vaults(order_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|vault| {
            vault.visibility == CustodyVaultVisibility::Internal
                && vault.role != CustodyVaultRole::SourceDeposit
        })
        .collect()
}

fn unit_asset_decimals(operation: &OrderProviderOperation) -> u32 {
    match operation
        .request
        .get("asset")
        .and_then(Value::as_str)
        .unwrap_or("")
    {
        "eth" => 18,
        "btc" => 8,
        other => panic!("unit_asset_decimals: unsupported UnitDeposit asset {other:?}"),
    }
}

fn base_units_to_natural(amount: &str, decimals: u32) -> f64 {
    let parsed = amount
        .parse::<f64>()
        .unwrap_or_else(|err| panic!("invalid base-unit amount {amount:?}: {err}"));
    parsed / 10_f64.powi(i32::try_from(decimals).unwrap())
}

async fn credit_hyperliquid_spot_from_unit_deposit(
    mocks: &MockIntegratorServer,
    db: &Database,
    order_id: Uuid,
    operation: &OrderProviderOperation,
) {
    let amount = if let Some(amount) = operation
        .response
        .get("amount")
        .and_then(Value::as_str)
        .or_else(|| {
            operation
                .request
                .get("expected_amount")
                .and_then(Value::as_str)
        }) {
        amount.to_string()
    } else if let Some(step_id) = operation.execution_step_id {
        db.orders()
            .get_execution_step(step_id)
            .await
            .unwrap()
            .amount_in
            .unwrap_or_else(|| "0".to_string())
    } else {
        panic!("unit deposit operation {} missing amount", operation.id)
    };
    let natural_amount = base_units_to_natural(&amount, unit_asset_decimals(operation));
    let credit_amount = if natural_amount > 0.0 {
        // Mock HL serializes balances with 8 decimals; keep tiny raw-unit
        // deposits visible to withdrawal preflight checks.
        natural_amount.max(1e-8)
    } else {
        natural_amount
    };
    mocks
        .credit_hyperliquid_balance(
            hyperliquid_spot_vault_address(db, order_id).await,
            &hl_deposit_coin_for_operation(operation),
            credit_amount,
        )
        .await;
}

async fn maybe_provider_operation_by_type(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
) -> Option<OrderProviderOperation> {
    db.orders()
        .get_provider_operations(order_id)
        .await
        .unwrap()
        .into_iter()
        .find(|operation| operation.operation_type == operation_type)
}

async fn drive_unit_deposit_failures_to_refund_required(
    fixture: &FundedMarketOrderFixture,
    error_message: &str,
) -> OrderExecutionManager {
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("across leg should start");
    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&execution_manager, &across_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    fixture
        .mocks
        .fail_next_unit_generate_address(error_message)
        .await;
    let first_err = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap_err();
    assert!(first_err.to_string().contains(error_message));

    fixture
        .mocks
        .fail_next_unit_generate_address(error_message)
        .await;
    let second_err = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap_err();
    assert!(second_err.to_string().contains(error_message));

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::RefundRequired);
    assert_eq!(vault.status, DepositVaultStatus::RefundRequired);

    execution_manager
}

#[tokio::test]
async fn test_database_harness_runs_migrations() {
    let _db = test_db().await;
}

#[tokio::test]
async fn materialize_plan_failure_does_not_create_execution_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let settings = Arc::new(test_settings(dir.path()));
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(mocks.base_url()));
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
        action_providers.clone(),
    );
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let _vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        action_providers,
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    let err = execution_manager
        .materialize_plan_for_order(order.id)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("not funded"),
        "unexpected planning error: {err}"
    );
    assert!(db
        .orders()
        .get_execution_attempts(order.id)
        .await
        .unwrap()
        .is_empty());
}

// ---------------------------------------------------------------------------
// Router order and market-order quote tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn quote_market_order_persists_ephemeral_quote_without_order() {
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let (order_manager, db) = test_order_manager(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )))
    .await;

    let response = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();

    assert_eq!(
        response
            .quote
            .as_market_order()
            .expect("market order quote")
            .source_asset
            .chain,
        ChainId::parse("evm:1").unwrap()
    );
    assert_eq!(
        response
            .quote
            .as_market_order()
            .expect("market order quote")
            .destination_asset
            .chain,
        ChainId::parse("evm:8453").unwrap()
    );
    assert!(response
        .quote
        .as_market_order()
        .expect("market order quote")
        .order_id
        .is_none());
    assert_path_provider_id(
        &response
            .quote
            .as_market_order()
            .expect("market order quote")
            .provider_id,
        &["unit_deposit:unit", "unit_withdrawal:unit"],
    );
    assert_eq!(
        response
            .quote
            .as_market_order()
            .expect("market order quote")
            .order_kind,
        MarketOrderKindType::ExactIn
    );
    assert_eq!(
        response
            .quote
            .as_market_order()
            .expect("market order quote")
            .amount_out,
        "1000"
    );

    let stored_quote = db
        .orders()
        .get_market_order_quote_by_id(
            response
                .quote
                .as_market_order()
                .expect("market order quote")
                .id,
        )
        .await
        .unwrap();
    assert!(stored_quote.order_id.is_none());
    assert_path_provider_id(
        &stored_quote.provider_id,
        &["unit_deposit:unit", "unit_withdrawal:unit"],
    );
    assert_eq!(stored_quote.slippage_bps, 100);
    assert_eq!(stored_quote.min_amount_out.as_deref(), Some("990"));
    let public_quote = serde_json::to_value(
        response
            .quote
            .as_market_order()
            .expect("market order quote"),
    )
    .unwrap();
    assert!(public_quote.get("execution_steps").is_none());
    assert!(public_quote.get("custody_vaults").is_none());
}

#[tokio::test]
async fn quote_market_order_supports_velora_arbitrary_evm_start_and_end() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let db = test_db().await;
    let dir = tempfile::tempdir().unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let action_providers = ActionProviderRegistry::http(
        None,
        None,
        None,
        None,
        None,
        Some(VeloraHttpProviderConfig::new(mocks.base_url())),
        router_server::services::custody_action_executor::HyperliquidCallNetwork::Mainnet,
    )
    .expect("velora-only action providers");
    let order_manager = OrderManager::with_action_providers(
        db,
        settings,
        h.chain_registry.clone(),
        Arc::new(action_providers),
    );
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::reference("0x3333333333333333333333333333333333333333"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::reference("0x4444444444444444444444444444444444444444"),
    };
    anvil_clone_base_mock_erc20_code(
        h,
        "0x3333333333333333333333333333333333333333"
            .parse()
            .unwrap(),
    )
    .await;
    anvil_clone_base_mock_erc20_code(
        h,
        "0x4444444444444444444444444444444444444444"
            .parse()
            .unwrap(),
    )
    .await;

    let response = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset.clone(),
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000000000000000000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    let expected_min_out = ((U256::from_str_radix(&quote.amount_out, 10).unwrap()
        * U256::from(9_900_u64))
        / U256::from(10_000_u64))
    .to_string();
    assert_eq!(quote.slippage_bps, 100);
    assert_eq!(
        quote.min_amount_out.as_deref(),
        Some(expected_min_out.as_str())
    );
    let transitions = quote.provider_quote["transitions"]
        .as_array()
        .expect("serialized transitions");
    let legs = quote.provider_quote["legs"].as_array().expect("quote legs");

    assert_eq!(transitions.len(), 2);
    assert_eq!(legs.len(), 2);
    assert!(transitions
        .iter()
        .all(|transition| transition["kind"] == json!("universal_router_swap")));
    assert!(legs.iter().all(|leg| leg["provider"] == json!("velora")
        && leg["raw"]["kind"] == json!("universal_router_swap")));
    assert_eq!(
        transitions[0]["input"]["asset"]["asset"],
        json!(source_asset.asset.as_str())
    );
    assert_eq!(
        transitions[1]["output"]["asset"]["asset"],
        json!(destination_asset.asset.as_str())
    );
    assert_eq!(
        transitions[0]["output"]["asset"]["chain"],
        json!(source_asset.chain.as_str())
    );
    assert_ne!(
        transitions[0]["output"]["asset"]["asset"],
        json!(source_asset.asset.as_str())
    );
    assert_ne!(
        transitions[0]["output"]["asset"]["asset"],
        json!(destination_asset.asset.as_str())
    );
    assert_eq!(
        transitions[0]["output"]["asset"],
        transitions[1]["input"]["asset"]
    );
    assert_eq!(
        quote.provider_quote["gas_reimbursement"]["retention_actions"]
            .as_array()
            .expect("retention actions")
            .len(),
        1
    );
}

#[tokio::test]
async fn mock_velora_transaction_mints_output_token_on_local_evm() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let signer = alloy::signers::local::PrivateKeySigner::random();
    let source_address = signer.address();
    let wallet = alloy::network::EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(h.base_endpoint_url());
    provider
        .anvil_set_balance(source_address, U256::from(1_000_000_000_000_000_000_u128))
        .await
        .expect("fund local Velora source wallet");

    let velora = VeloraProvider::new(mocks.base_url(), None);
    let input_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::Native,
    };
    let output_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::Reference(MOCK_ERC20_ADDRESS.to_lowercase()),
    };
    let amount_in = "1000000000000".to_string();
    let quote = velora
        .quote_trade(ExchangeQuoteRequest {
            input_asset: input_asset.clone(),
            output_asset: output_asset.clone(),
            input_decimals: Some(18),
            output_decimals: Some(6),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: amount_in.clone(),
                min_amount_out: "1".to_string(),
            },
            sender_address: Some(format!("{source_address:#x}")),
            recipient_address: format!("{source_address:#x}"),
        })
        .await
        .unwrap()
        .expect("mock Velora quote");

    let request = json!({
        "order_id": Uuid::now_v7(),
        "quote_id": Uuid::now_v7(),
        "order_kind": "exact_in",
        "amount_in": quote.amount_in,
        "amount_out": quote.amount_out,
        "min_amount_out": quote.min_amount_out,
        "max_amount_in": quote.max_amount_in,
        "input_asset": {
            "chain_id": input_asset.chain.as_str(),
            "asset": input_asset.asset.as_str(),
        },
        "output_asset": {
            "chain_id": output_asset.chain.as_str(),
            "asset": output_asset.asset.as_str(),
        },
        "input_decimals": 18,
        "output_decimals": 6,
        "price_route": quote.provider_quote["price_route"],
        "slippage_bps": quote.provider_quote["slippage_bps"],
        "source_custody_vault_id": Uuid::now_v7(),
        "source_custody_vault_address": format!("{source_address:#x}"),
        "recipient_address": format!("{source_address:#x}"),
        "recipient_custody_vault_id": null,
    });

    let base_chain = h.chain_registry.get_evm(&ChainType::Base).unwrap();
    let before = base_chain
        .erc20_balance(MOCK_ERC20_ADDRESS, &format!("{source_address:#x}"))
        .await
        .unwrap();
    let request = ExchangeExecutionRequest::universal_router_swap_from_value(&request).unwrap();
    let execution = velora.execute_trade(&request).await.unwrap();
    let actions = match execution {
        ProviderExecutionIntent::CustodyActions { actions, .. } => actions,
        ProviderExecutionIntent::CustodyAction { action, .. } => vec![action],
        ProviderExecutionIntent::ProviderOnly { .. } => {
            panic!("mock Velora swap should require an EVM custody action")
        }
    };
    assert_eq!(actions.len(), 1);

    for action in actions {
        let CustodyAction::Call(ChainCall::Evm(call)) = action else {
            panic!("mock Velora action should be an EVM call");
        };
        let tx = TransactionRequest::default()
            .with_to(Address::from_str(&call.to_address).unwrap())
            .with_value(U256::from_str_radix(&call.value, 10).unwrap())
            .with_input(Bytes::from_str(&call.calldata).unwrap());
        let receipt = provider
            .send_transaction(tx)
            .await
            .expect("submit mock Velora transaction")
            .get_receipt()
            .await
            .expect("mock Velora transaction receipt");
        assert!(receipt.status(), "mock Velora transaction reverted");
    }

    let after = base_chain
        .erc20_balance(MOCK_ERC20_ADDRESS, &format!("{source_address:#x}"))
        .await
        .unwrap();
    assert_eq!(
        after.saturating_sub(before),
        U256::from_str_radix(&quote.amount_out, 10).unwrap()
    );
}

#[tokio::test]
async fn quote_market_order_exact_out_uses_exchange_provider_quote() {
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let (order_manager, _db) = test_order_manager(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )))
    .await;

    let response = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactOut {
                amount_out: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();

    let market_quote = response
        .quote
        .as_market_order()
        .expect("market order quote");
    assert!(market_quote.provider_id.starts_with("path:"));
    assert!(market_quote.provider_id.contains("unit_deposit:unit"));
    assert!(market_quote.provider_id.contains("unit_withdrawal:unit"));
    assert_eq!(market_quote.order_kind, MarketOrderKindType::ExactOut);
    assert_eq!(market_quote.amount_in, "1000");
    assert_eq!(market_quote.slippage_bps, 100);
    assert_eq!(market_quote.max_amount_in.as_deref(), Some("1010"));
}

#[tokio::test]
async fn router_api_quote_and_order_flow_uses_production_component_initialization() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let mut args = test_router_args(h, dir.path(), database_url);
    args.across_api_url = Some(mocks.base_url().to_string());
    args.across_api_key = Some("mock-across-api-key".to_string());
    args.hyperunit_api_url = Some(mocks.base_url().to_string());
    args.hyperliquid_api_url = Some(mocks.base_url().to_string());
    args.router_admin_api_key = Some("test-admin-key".to_string());
    let (base_url, api_task) = spawn_router_api(args).await;
    let client = reqwest::Client::new();

    let quote_response = client
        .post(format!("{base_url}/api/v1/quotes"))
        .json(&json!({
            "type": "market_order",
            "from_asset": {
                "chain": "evm:1",
                "asset": "native"
            },
            "to_asset": {
                "chain": "evm:8453",
                "asset": "native"
            },
            "recipient_address": valid_evm_address(),
            "kind": "exact_in",
            "amount_in": "1000",
            "slippage_bps": 100
        }))
        .send()
        .await
        .unwrap();
    let status = quote_response.status();
    let body = quote_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let quote: RouterOrderQuoteEnvelope = serde_json::from_str(&body).unwrap();
    assert_path_provider_id(
        &quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .provider_id,
        &["unit_deposit:unit", "unit_withdrawal:unit"],
    );
    assert!(quote
        .quote
        .as_market_order()
        .expect("market order quote")
        .order_id
        .is_none());

    let fetched_quote: RouterOrderQuoteEnvelope = client
        .get(format!(
            "{base_url}/api/v1/quotes/{}",
            quote
                .quote
                .as_market_order()
                .expect("market order quote")
                .id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        fetched_quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id
    );
    assert!(fetched_quote
        .quote
        .as_market_order()
        .expect("market order quote")
        .order_id
        .is_none());

    let order_response = client
        .post(format!("{base_url}/api/v1/orders"))
        .json(&json!({
            "quote_id": quote.quote.as_market_order().expect("market order quote").id,
            "refund_address": valid_evm_address(),
            "metadata": {
                "test": "production_component_initialization"
            }
        }))
        .send()
        .await
        .unwrap();
    let status = order_response.status();
    let body = order_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let order: RouterOrderEnvelope = serde_json::from_str(&body).unwrap();
    assert!(order.cancellation_secret.is_some());
    assert_eq!(
        order
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id
    );
    assert_eq!(
        order
            .quote
            .as_market_order()
            .expect("market order quote")
            .order_id,
        Some(order.order.id)
    );
    assert_eq!(order.order.status, RouterOrderStatus::PendingFunding);
    match &order.order.action {
        RouterOrderAction::MarketOrder(action) => {
            assert_eq!(action.slippage_bps, 100);
            assert_eq!(
                action.order_kind,
                MarketOrderKind::ExactIn {
                    amount_in: "1000".to_string(),
                    min_amount_out: "990".to_string(),
                }
            );
        }
    }
    assert!(order.funding_vault.is_some());
    let funding_vault = order.funding_vault.as_ref().unwrap();
    assert_eq!(funding_vault.vault.order_id, Some(order.order.id));
    assert_eq!(funding_vault.vault.deposit_asset, order.order.source_asset);

    let fetched_order: RouterOrderEnvelope = client
        .get(format!("{base_url}/api/v1/orders/{}", order.order.id))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched_order.order.id, order.order.id);
    assert!(fetched_order.funding_vault.is_some());

    let unauthenticated_flow = client
        .get(format!(
            "{base_url}/internal/v1/orders/{}/flow",
            order.order.id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        unauthenticated_flow.status(),
        reqwest::StatusCode::UNAUTHORIZED
    );

    let flow_response = client
        .get(format!(
            "{base_url}/internal/v1/orders/{}/flow",
            order.order.id
        ))
        .bearer_auth("test-admin-key")
        .send()
        .await
        .unwrap();
    let status = flow_response.status();
    let body = flow_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    let flow: OrderFlowEnvelope = serde_json::from_str(&body).unwrap();
    assert_eq!(flow.flow.order.id, order.order.id);
    assert_eq!(flow.flow.trace.trace_id, order.order.workflow_trace_id);
    assert_eq!(
        flow.flow.trace.parent_span_id,
        order.order.workflow_parent_span_id
    );
    assert_eq!(flow.flow.progress.total_steps, 0);
    assert!(flow.flow.quote.is_some());

    api_task.abort();
}

#[tokio::test]
async fn router_admin_provider_policy_endpoint_requires_bearer_api_key_and_persists_updates() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let mut args = test_router_args(h, dir.path(), database_url);
    args.router_admin_api_key = Some("test-admin-key".to_string());
    let (base_url, api_task) = spawn_router_api(args).await;
    let client = reqwest::Client::new();

    let unauthenticated = client
        .get(format!("{base_url}/internal/v1/provider-policies"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), reqwest::StatusCode::UNAUTHORIZED);

    let wrong_key = client
        .get(format!("{base_url}/internal/v1/provider-policies"))
        .bearer_auth("wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(wrong_key.status(), reqwest::StatusCode::UNAUTHORIZED);

    let update_response = client
        .put(format!("{base_url}/internal/v1/provider-policies/Across"))
        .bearer_auth("test-admin-key")
        .json(&json!({
            "quote_state": "disabled",
            "execution_state": "drain",
            "reason": "incident response",
            "updated_by": "ops"
        }))
        .send()
        .await
        .unwrap();
    let status = update_response.status();
    let body = update_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    let policy: ProviderPolicyEnvelope = serde_json::from_str(&body).unwrap();
    assert_eq!(policy.policy.provider, "across");
    assert_eq!(policy.policy.reason, "incident response");

    let list_response = client
        .get(format!("{base_url}/internal/v1/provider-policies"))
        .bearer_auth("test-admin-key")
        .send()
        .await
        .unwrap();
    let status = list_response.status();
    let body = list_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");
    let policies: ProviderPolicyListEnvelope = serde_json::from_str(&body).unwrap();
    assert_eq!(policies.policies.len(), 1);
    assert_eq!(policies.policies[0].provider, "across");
    assert_eq!(policies.policies[0].updated_by, "ops");

    api_task.abort();
}

#[tokio::test]
async fn router_admin_provider_policy_drain_excludes_provider_from_new_quotes_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let mut args = test_router_args(h, dir.path(), database_url);
    args.across_api_url = Some(mocks.base_url().to_string());
    args.across_api_key = Some("mock-across-api-key".to_string());
    args.hyperunit_api_url = Some(mocks.base_url().to_string());
    args.hyperliquid_api_url = Some(mocks.base_url().to_string());
    args.router_admin_api_key = Some("test-admin-key".to_string());
    let (base_url, api_task) = spawn_router_api(args).await;
    let client = reqwest::Client::new();
    let quote_request = json!({
        "type": "market_order",
        "from_asset": {
            "chain": "evm:8453",
            "asset": "native"
        },
        "to_asset": {
            "chain": "evm:1",
            "asset": "native"
        },
        "recipient_address": valid_evm_address(),
        "kind": "exact_in",
        "amount_in": "1000",
        "slippage_bps": 100
    });

    let initial_quote = client
        .post(format!("{base_url}/api/v1/quotes"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();
    let status = initial_quote.status();
    let body = initial_quote.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let quote: RouterOrderQuoteEnvelope = serde_json::from_str(&body).unwrap();
    assert_path_provider_id(
        &quote.quote.as_market_order().unwrap().provider_id,
        &[
            "across_bridge:across",
            "unit_deposit:unit",
            "unit_withdrawal:unit",
        ],
    );

    let disable_response = client
        .put(format!("{base_url}/internal/v1/provider-policies/across"))
        .bearer_auth("test-admin-key")
        .json(&json!({
            "quote_state": "disabled",
            "execution_state": "drain",
            "reason": "maintenance window"
        }))
        .send()
        .await
        .unwrap();
    let status = disable_response.status();
    let body = disable_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");

    let drained_quote = client
        .post(format!("{base_url}/api/v1/quotes"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();
    let status = drained_quote.status();
    let body = drained_quote.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    let reenable_response = client
        .put(format!("{base_url}/internal/v1/provider-policies/across"))
        .bearer_auth("test-admin-key")
        .json(&json!({
            "quote_state": "enabled",
            "execution_state": "enabled",
            "reason": "incident cleared"
        }))
        .send()
        .await
        .unwrap();
    let status = reenable_response.status();
    let body = reenable_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::OK, "{body}");

    let restored_quote = client
        .post(format!("{base_url}/api/v1/quotes"))
        .json(&quote_request)
        .send()
        .await
        .unwrap();
    let status = restored_quote.status();
    let body = restored_quote.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");

    api_task.abort();
}

#[tokio::test]
async fn router_api_and_worker_complete_mock_across_unit_flow_with_production_components() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let mut args = test_router_args(h, dir.path(), database_url);
    args.across_api_url = Some(mocks.base_url().to_string());
    args.across_api_key = Some("mock-across-api-key".to_string());
    args.hyperunit_api_url = Some(mocks.base_url().to_string());
    args.hyperliquid_api_url = Some(mocks.base_url().to_string());
    let components = initialize_components(&args, None, PaymasterMode::Enabled)
        .await
        .expect("initialize router components");
    let (base_url, api_task) = spawn_router_api_from_components(components.clone(), None).await;
    let client = reqwest::Client::new();

    let quote_response = client
        .post(format!("{base_url}/api/v1/quotes"))
        .json(&json!({
            "type": "market_order",
            "from_asset": {
                "chain": "evm:8453",
                "asset": "native"
            },
            "to_asset": {
                "chain": "evm:1",
                "asset": "native"
            },
            "recipient_address": valid_evm_address(),
            "kind": "exact_in",
            "amount_in": "1000",
            "slippage_bps": 100
        }))
        .send()
        .await
        .unwrap();
    let status = quote_response.status();
    let body = quote_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let quote: RouterOrderQuoteEnvelope = serde_json::from_str(&body).unwrap();
    assert_path_provider_id(
        &quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .provider_id,
        &[
            "across_bridge:across",
            "unit_deposit:unit",
            "unit_withdrawal:unit",
        ],
    );

    let order_response = client
        .post(format!("{base_url}/api/v1/orders"))
        .json(&json!({
            "quote_id": quote.quote.as_market_order().expect("market order quote").id,
            "refund_address": valid_evm_address(),
            "metadata": {
                "test": "api_worker_mock_across_unit"
            }
        }))
        .send()
        .await
        .unwrap();
    let status = order_response.status();
    let body = order_response.text().await.unwrap();
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    let order: RouterOrderEnvelope = serde_json::from_str(&body).unwrap();
    assert!(order.cancellation_secret.is_some());
    let funding_vault = order
        .funding_vault
        .as_ref()
        .expect("order should include a funding vault")
        .vault
        .clone();
    let vault_address =
        Address::from_str(&funding_vault.deposit_vault_address).expect("valid base vault address");
    anvil_set_base_balance(h, vault_address, U256::from(1000_u64)).await;

    let funded = components
        .vault_manager
        .process_funding_pass()
        .await
        .unwrap();
    assert_eq!(funded, 1);
    drive_across_order_to_completion(
        &components.order_execution_manager,
        &mocks,
        &components.db,
        order.order.id,
    )
    .await;

    let completed_order = components.db.orders().get(order.order.id).await.unwrap();
    let completed_vault = components.db.vaults().get(funding_vault.id).await.unwrap();
    assert_eq!(completed_order.status, RouterOrderStatus::Completed);
    assert_eq!(completed_vault.status, DepositVaultStatus::Completed);

    let destination_vault = components
        .db
        .orders()
        .get_custody_vaults(order.order.id)
        .await
        .unwrap()
        .into_iter()
        .find(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .expect("destination execution vault should be materialized");
    assert_eq!(
        destination_vault.visibility,
        CustodyVaultVisibility::Internal
    );
    assert_eq!(destination_vault.chain, ChainId::parse("evm:1").unwrap());

    assert_eq!(mocks.across_deposits_from(vault_address).await.len(), 1);
    let gen_requests = mocks.unit_generate_address_requests().await;
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Deposit).len(),
        1
    );
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Withdrawal).len(),
        1
    );

    api_task.abort();
}

#[tokio::test]
async fn funding_pass_marks_order_vault_funded_after_detecting_evm_native_balance() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();

    assert_eq!(vault.status, DepositVaultStatus::PendingFunding);
    let marked = vault_manager.process_funding_pass().await.unwrap();
    assert_eq!(marked, 0);

    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    anvil_set_balance(h, vault_address, U256::from(1000)).await;

    let marked = vault_manager.process_funding_pass().await.unwrap();
    assert_eq!(marked, 1);
    let funded_vault = vault_manager.get_vault(vault.id).await.unwrap();
    assert_eq!(funded_vault.status, DepositVaultStatus::Funded);

    let order = db.orders().get(order.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::PendingFunding);
}

#[tokio::test]
async fn funding_hints_validate_balance_once_and_mark_vault_funded() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();

    let now = Utc::now();
    vault_manager
        .record_funding_hint(DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({ "tx_hash": "0xearly" }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some("early".to_string()),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let early_summary = vault_manager
        .process_funding_hints_detailed(10)
        .await
        .unwrap();
    assert_eq!(early_summary.processed, 1);
    assert!(early_summary.funded_order_ids.is_empty());
    assert_eq!(
        vault_manager.get_vault(vault.id).await.unwrap().status,
        DepositVaultStatus::PendingFunding
    );

    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    anvil_set_balance(h, vault_address, U256::from(1000)).await;
    let now = Utc::now();
    vault_manager
        .record_funding_hint(DepositVaultFundingHint {
            id: Uuid::now_v7(),
            vault_id: vault.id,
            source: "sauron".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({ "tx_hash": "0xfunded" }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some("funded".to_string()),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();

    let funded_summary = vault_manager
        .process_funding_hints_detailed(10)
        .await
        .unwrap();
    assert_eq!(funded_summary.processed, 1);
    assert_eq!(funded_summary.funded_order_ids, vec![order.id]);
    let funded_vault = vault_manager.get_vault(vault.id).await.unwrap();
    assert_eq!(funded_vault.status, DepositVaultStatus::Funded);
}

#[tokio::test]
async fn route_cost_refresh_persists_anchor_snapshots() {
    let db = test_db().await;
    let action_providers = Arc::new(ActionProviderRegistry::with_asset_registry(
        Arc::new(AssetRegistry::default()),
        vec![],
        vec![],
        vec![],
    ));
    let service = RouteCostService::new(db.clone(), action_providers);
    let summary = service.refresh_anchor_costs().await.unwrap();
    let snapshots = db
        .route_costs()
        .list_active("usd_1000", Utc::now())
        .await
        .unwrap();
    let base_usdt = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
    };
    let arbitrum_cbbtc = DepositAsset {
        chain: ChainId::parse("evm:42161").unwrap(),
        asset: AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
    };

    assert!(summary.candidate_edges > 0);
    assert_eq!(summary.provider_quotes_attempted, 0);
    assert_eq!(summary.provider_quotes_succeeded, 0);
    assert_eq!(summary.provider_quotes_failed, 0);
    assert_eq!(snapshots.len(), summary.snapshots_upserted);
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.source_asset == base_usdt
            || snapshot.destination_asset == base_usdt));
    assert!(snapshots.iter().any(|snapshot| {
        snapshot.source_asset == arbitrum_cbbtc || snapshot.destination_asset == arbitrum_cbbtc
    }));
    assert!(snapshots
        .iter()
        .all(|snapshot| snapshot.effective_cost_bps() < u64::MAX));
}

#[tokio::test]
async fn route_cost_refresh_samples_configured_bridge_providers() {
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let service = RouteCostService::new(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
    );

    let summary = service.refresh_anchor_costs().await.unwrap();
    let snapshots = db
        .route_costs()
        .list_active("usd_1000", Utc::now())
        .await
        .unwrap();

    assert!(summary.provider_quotes_attempted > 0);
    assert!(summary.provider_quotes_succeeded > 0);
    assert!(snapshots
        .iter()
        .any(|snapshot| snapshot.quote_source.starts_with("provider_quote:")));
}

#[tokio::test]
async fn route_minimum_service_computes_mock_base_usdc_to_btc_floor() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let service = RouteMinimumService::new(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )));
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("bitcoin").unwrap(),
        asset: AssetId::Native,
    };

    let snapshot = service
        .floor_for_route(
            &source_asset,
            &destination_asset,
            &valid_regtest_btc_address(),
            &valid_evm_address(),
        )
        .await
        .unwrap();
    let hard_min = snapshot.hard_min_input_u256().unwrap();
    let operational_min = snapshot.operational_min_input_u256().unwrap();

    assert!(snapshot.path.contains("cctp_bridge:cctp"));
    assert!(snapshot
        .path
        .contains("hyperliquid_bridge_deposit:hyperliquid_bridge"));
    assert!(snapshot.path.contains("hyperliquid_trade:hyperliquid"));
    assert!(snapshot.path.contains("unit_withdrawal:unit"));
    assert_eq!(snapshot.output_floor, "30000");
    assert!(hard_min > U256::ZERO);
    assert_eq!(operational_min, hard_min * U256::from(2));
}

#[tokio::test]
async fn quote_market_order_rejects_base_usdc_to_btc_below_operational_floor() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(mocks.base_url()));
    let (order_manager, _db) = test_order_manager(action_providers.clone()).await;
    let order_manager = order_manager
        .with_route_minimums(Some(Arc::new(RouteMinimumService::new(action_providers))));

    let err = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_regtest_btc_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        MarketOrderError::InputBelowRouteMinimum { .. }
    ));
}

#[tokio::test]
async fn quote_market_order_allows_base_usdc_to_btc_at_operational_floor() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(mocks.base_url()));
    let route_minimums = Arc::new(RouteMinimumService::new(action_providers.clone()));
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("bitcoin").unwrap(),
        asset: AssetId::Native,
    };
    let snapshot = route_minimums
        .floor_for_route(
            &source_asset,
            &destination_asset,
            &valid_regtest_btc_address(),
            &valid_evm_address(),
        )
        .await
        .unwrap();
    let (order_manager, _db) = test_order_manager(action_providers).await;
    let order_manager = order_manager.with_route_minimums(Some(route_minimums));

    let response = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset,
            to_asset: destination_asset,
            recipient_address: valid_regtest_btc_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: snapshot.operational_min_input,
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();

    assert_path_provider_id(
        &response
            .quote
            .as_market_order()
            .expect("market order quote")
            .provider_id,
        &[
            "cctp_bridge:cctp",
            "hyperliquid_bridge_deposit:hyperliquid_bridge",
            "hyperliquid_trade:hyperliquid",
            "unit_withdrawal:unit",
            "exchange:hyperliquid",
        ],
    );
}

#[tokio::test]
async fn quote_market_order_rejects_zero_amounts() {
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let (order_manager, _db) = test_order_manager(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )))
    .await;

    let err = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "0".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap_err();

    assert!(err.to_string().contains("greater than zero"));
}

#[tokio::test]
async fn create_vault_with_order_id_links_generic_order_to_vault() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;

    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();
    let order = db.orders().get(order.id).await.unwrap();

    assert_eq!(vault.order_id, Some(order.id));
    assert_eq!(order.funding_vault_id, Some(vault.id));
    assert_eq!(order.status, RouterOrderStatus::PendingFunding);
    assert_eq!(vault.action, VaultAction::from(order.action));

    let custody_vaults = db.orders().get_custody_vaults(order.id).await.unwrap();
    assert_eq!(custody_vaults.len(), 1);
    let source_vault = &custody_vaults[0];
    assert_eq!(source_vault.id, vault.id);
    assert_eq!(source_vault.role, CustodyVaultRole::SourceDeposit);
    assert_eq!(source_vault.visibility, CustodyVaultVisibility::UserFacing);
    assert_eq!(
        source_vault.control_type,
        CustodyVaultControlType::RouterDerivedKey
    );
    assert_eq!(source_vault.status, CustodyVaultStatus::Active);
    assert_eq!(source_vault.chain, vault.deposit_asset.chain);
    assert_eq!(source_vault.asset, Some(vault.deposit_asset.asset.clone()));
    assert_eq!(source_vault.address, vault.deposit_vault_address);
    assert_eq!(source_vault.derivation_salt, Some(vault.deposit_vault_salt));
    assert_eq!(source_vault.metadata["deposit_vault_id"], json!(vault.id));

    let execution_steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert_eq!(execution_steps.len(), 1);
    let wait_step = &execution_steps[0];
    assert_eq!(wait_step.step_index, 0);
    assert_eq!(wait_step.step_type, OrderExecutionStepType::WaitForDeposit);
    assert_eq!(wait_step.provider, "internal");
    assert_eq!(wait_step.status, OrderExecutionStepStatus::Waiting);
    assert_eq!(wait_step.input_asset, Some(vault.deposit_asset.clone()));
    assert_eq!(wait_step.details["vault_id"], json!(vault.id));
    assert_eq!(
        wait_step.details["custody_vault_id"],
        json!(source_vault.id)
    );
}

#[tokio::test]
async fn custody_action_executor_transfers_evm_native_from_router_vault() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let vault = vm.create_vault(evm_native_request()).await.unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    let recipient =
        Address::from_str("0x1000000000000000000000000000000000000002").expect("valid recipient");
    let recipient_address = format!("{recipient:?}");
    let transfer_amount = U256::from(1234_u64);
    let initial_vault_balance = U256::from(1_000_000_000_000_000_000_u64);

    anvil_set_balance(h, vault_address, initial_vault_balance).await;
    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    let recipient_before = ethereum_chain
        .native_balance(&recipient_address)
        .await
        .unwrap();

    let executor = CustodyActionExecutor::new(
        db,
        Arc::new(test_settings(dir.path())),
        h.chain_registry.clone(),
    );
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: vault.id,
            action: CustodyAction::Transfer {
                to_address: recipient_address.clone(),
                amount: transfer_amount.to_string(),
            },
        })
        .await
        .unwrap();

    assert!(receipt.tx_hash.starts_with("0x"));
    assert_eq!(receipt.custody_vault_id, vault.id);
    let recipient_after = ethereum_chain
        .native_balance(&recipient_address)
        .await
        .unwrap();
    assert_eq!(
        recipient_after.saturating_sub(recipient_before),
        transfer_amount
    );
    let vault_after = ethereum_chain
        .native_balance(&vault.deposit_vault_address)
        .await
        .unwrap();
    assert!(vault_after < initial_vault_balance.saturating_sub(transfer_amount));
}

#[tokio::test]
async fn custody_action_executor_transfers_evm_native_from_router_vault_with_paymaster_gas() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let recovery_address = h.ethereum_funded_address();
    let vault = vm
        .create_vault(CreateVaultRequest {
            order_id: None,
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            action: VaultAction::Null,
            recovery_address: format!("{recovery_address:?}"),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    let recipient =
        Address::from_str("0x1000000000000000000000000000000000000005").expect("valid recipient");
    let recipient_address = format!("{recipient:?}");
    let transfer_amount = U256::from(4321_u64);

    anvil_set_balance(h, vault_address, transfer_amount).await;
    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    assert_eq!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap(),
        transfer_amount
    );
    let recipient_before = ethereum_chain
        .native_balance(&recipient_address)
        .await
        .unwrap();

    let executor = CustodyActionExecutor::new(
        db,
        Arc::new(test_settings(dir.path())),
        h.chain_registry.clone(),
    );
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: vault.id,
            action: CustodyAction::Transfer {
                to_address: recipient_address.clone(),
                amount: transfer_amount.to_string(),
            },
        })
        .await
        .unwrap();

    assert!(receipt.tx_hash.starts_with("0x"));
    let recipient_after = ethereum_chain
        .native_balance(&recipient_address)
        .await
        .unwrap();
    assert_eq!(
        recipient_after.saturating_sub(recipient_before),
        transfer_amount
    );
    assert!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap()
            > U256::ZERO
    );
}

#[tokio::test]
async fn custody_action_executor_transfers_erc20_from_router_vault_with_paymaster_gas() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let recovery_address = h.ethereum_funded_address();
    let vault = vm
        .create_vault(CreateVaultRequest {
            order_id: None,
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Reference(MOCK_ERC20_ADDRESS.to_lowercase()),
            },
            action: VaultAction::Null,
            recovery_address: format!("{recovery_address:?}"),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    let recipient =
        Address::from_str("0x1000000000000000000000000000000000000003").expect("valid recipient");
    let recipient_address = format!("{recipient:?}");
    let mint_amount = U256::from(10_000_u64);
    let transfer_amount = U256::from(4321_u64);

    anvil_mint_erc20(h, vault_address, mint_amount).await;
    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    assert_eq!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap(),
        U256::ZERO
    );
    let recipient_before = ethereum_chain
        .erc20_balance(MOCK_ERC20_ADDRESS, &recipient_address)
        .await
        .unwrap();

    let executor = CustodyActionExecutor::new(
        db,
        Arc::new(test_settings(dir.path())),
        h.chain_registry.clone(),
    );
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: vault.id,
            action: CustodyAction::Transfer {
                to_address: recipient_address.clone(),
                amount: transfer_amount.to_string(),
            },
        })
        .await
        .unwrap();

    assert!(receipt.tx_hash.starts_with("0x"));
    let recipient_after = ethereum_chain
        .erc20_balance(MOCK_ERC20_ADDRESS, &recipient_address)
        .await
        .unwrap();
    assert_eq!(
        recipient_after.saturating_sub(recipient_before),
        transfer_amount
    );
    assert_eq!(
        ethereum_chain
            .erc20_balance(MOCK_ERC20_ADDRESS, &vault.deposit_vault_address)
            .await
            .unwrap(),
        mint_amount.saturating_sub(transfer_amount)
    );
    assert!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap()
            > U256::ZERO
    );
}

#[tokio::test]
async fn custody_action_executor_executes_evm_call_with_paymaster_gas() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let recovery_address = h.ethereum_funded_address();
    let vault = vm
        .create_vault(CreateVaultRequest {
            order_id: None,
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Reference(MOCK_ERC20_ADDRESS.to_lowercase()),
            },
            action: VaultAction::Null,
            recovery_address: format!("{recovery_address:?}"),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    let recipient =
        Address::from_str("0x1000000000000000000000000000000000000004").expect("valid recipient");
    let recipient_address = format!("{recipient:?}");
    let mint_amount = U256::from(10_000_u64);
    let transfer_amount = U256::from(1111_u64);

    anvil_mint_erc20(h, vault_address, mint_amount).await;
    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    assert_eq!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap(),
        U256::ZERO
    );

    let provider = ProviderBuilder::new().connect_http(h.ethereum_endpoint_url());
    let contract = GenericEIP3009ERC20Instance::new(MOCK_ERC20_ADDRESS.parse().unwrap(), &provider);
    let calldata = contract
        .transfer(recipient, transfer_amount)
        .calldata()
        .clone();

    let executor = CustodyActionExecutor::new(
        db,
        Arc::new(test_settings(dir.path())),
        h.chain_registry.clone(),
    );
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: vault.id,
            action: CustodyAction::Call(ChainCall::Evm(EvmCall {
                to_address: MOCK_ERC20_ADDRESS.to_string(),
                value: "0".to_string(),
                calldata: format!("0x{}", alloy::hex::encode(calldata.as_ref())),
            })),
        })
        .await
        .unwrap();

    assert!(receipt.tx_hash.starts_with("0x"));
    assert_eq!(
        ethereum_chain
            .erc20_balance(MOCK_ERC20_ADDRESS, &recipient_address)
            .await
            .unwrap(),
        transfer_amount
    );
    assert_eq!(
        ethereum_chain
            .erc20_balance(MOCK_ERC20_ADDRESS, &vault.deposit_vault_address)
            .await
            .unwrap(),
        mint_amount.saturating_sub(transfer_amount)
    );
    assert!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap()
            > U256::ZERO
    );
}

#[tokio::test]
async fn custody_action_executor_transfers_bitcoin_native_from_router_vault() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let vault = vm
        .create_vault(CreateVaultRequest {
            order_id: None,
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            action: VaultAction::Null,
            recovery_address: valid_regtest_btc_address(),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let vault_address = bitcoin::Address::from_str(&vault.deposit_vault_address)
        .expect("valid vault btc address")
        .assume_checked();
    h.deal_bitcoin(&vault_address, &Amount::from_sat(100_000))
        .await
        .expect("fund bitcoin vault");
    h.wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync after funding");

    let recipient_address = valid_regtest_btc_address();
    let bitcoin_chain = h.chain_registry.get_bitcoin(&ChainType::Bitcoin).unwrap();
    let recipient_before = bitcoin_chain
        .address_balance_sats(&recipient_address)
        .await
        .unwrap();

    let executor = CustodyActionExecutor::new(
        db,
        Arc::new(test_settings(dir.path())),
        h.chain_registry.clone(),
    );
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: vault.id,
            action: CustodyAction::Transfer {
                to_address: recipient_address.clone(),
                amount: "50000".to_string(),
            },
        })
        .await
        .unwrap();

    assert!(!receipt.tx_hash.is_empty());
    h.mine_bitcoin_blocks(1)
        .await
        .expect("mine bitcoin transfer");
    h.wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync after transfer");
    let recipient_after = bitcoin_chain
        .address_balance_sats(&recipient_address)
        .await
        .unwrap();
    assert_eq!(recipient_after.saturating_sub(recipient_before), 50_000);
    let vault_after = bitcoin_chain
        .address_balance_sats(&vault.deposit_vault_address)
        .await
        .unwrap();
    assert!(vault_after < 50_000);
}

#[tokio::test]
async fn worker_pass_sweeps_released_internal_evm_native_custody_to_paymaster() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let now = Utc::now();
    let derivation_salt = [0x44_u8; 32];
    let wallet = h
        .chain_registry
        .get(&ChainType::Ethereum)
        .expect("ethereum chain")
        .derive_wallet(&settings.master_key_bytes(), &derivation_salt)
        .expect("derive wallet");
    let vault = CustodyVault {
        id: Uuid::now_v7(),
        order_id: None,
        role: CustodyVaultRole::DestinationExecution,
        visibility: CustodyVaultVisibility::Internal,
        chain: ChainId::parse("evm:1").unwrap(),
        asset: Some(AssetId::Native),
        address: wallet.address.clone(),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: Some(derivation_salt),
        signer_ref: None,
        status: CustodyVaultStatus::Released,
        metadata: json!({
            "lifecycle_terminal_reason": "order_completed",
            "lifecycle_order_status": "completed",
        }),
        created_at: now,
        updated_at: now,
    };
    db.orders().create_custody_vault(&vault).await.unwrap();

    let vault_address = Address::from_str(&wallet.address).expect("vault address");
    anvil_set_balance(h, vault_address, U256::from(1_000_000_000_000_000_000_u128)).await;

    let paymaster_private_key = h.ethereum_spawned_api_paymaster_private_key();
    let paymaster_address =
        router_server::services::custody_action_executor::evm_address_from_private_key(
            &paymaster_private_key,
        )
        .expect("paymaster address");
    let mut paymasters = router_server::services::custody_action_executor::PaymasterRegistry::new();
    paymasters.register(ChainType::Ethereum, paymaster_address.clone());

    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    let paymaster_before = ethereum_chain
        .native_balance(&paymaster_address)
        .await
        .unwrap();

    let executor = Arc::new(
        CustodyActionExecutor::new(db.clone(), settings, h.chain_registry.clone())
            .with_paymasters(paymasters),
    );
    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::default()),
        executor,
        h.chain_registry.clone(),
    );

    let summary = execution_manager.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.executed_orders, 0);

    let updated_vault = db.orders().get_custody_vault(vault.id).await.unwrap();
    assert_eq!(updated_vault.status, CustodyVaultStatus::Released);
    assert_eq!(
        updated_vault.metadata["release_sweep_status"],
        json!("swept")
    );
    assert_eq!(
        updated_vault.metadata["release_sweep_terminal"],
        json!(true)
    );
    assert_eq!(
        updated_vault.metadata["release_sweep_target_address"],
        json!(paymaster_address.clone())
    );
    assert!(updated_vault.metadata["release_sweep_tx_hash"]
        .as_str()
        .unwrap_or_default()
        .starts_with("0x"));

    let paymaster_after = ethereum_chain
        .native_balance(&paymaster_address)
        .await
        .unwrap();
    assert!(paymaster_after > paymaster_before);
}

#[tokio::test]
async fn worker_pass_does_not_sweep_failed_internal_custody() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let now = Utc::now();
    let derivation_salt = [0x45_u8; 32];
    let wallet = h
        .chain_registry
        .get(&ChainType::Ethereum)
        .expect("ethereum chain")
        .derive_wallet(&settings.master_key_bytes(), &derivation_salt)
        .expect("derive wallet");
    let vault = CustodyVault {
        id: Uuid::now_v7(),
        order_id: None,
        role: CustodyVaultRole::DestinationExecution,
        visibility: CustodyVaultVisibility::Internal,
        chain: ChainId::parse("evm:1").unwrap(),
        asset: Some(AssetId::Native),
        address: wallet.address.clone(),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: Some(derivation_salt),
        signer_ref: None,
        status: CustodyVaultStatus::Failed,
        metadata: json!({
            "lifecycle_terminal_reason": "order_failed",
            "lifecycle_order_status": "failed",
        }),
        created_at: now,
        updated_at: now,
    };
    db.orders().create_custody_vault(&vault).await.unwrap();

    let paymaster_private_key = h.ethereum_spawned_api_paymaster_private_key();
    let paymaster_address =
        router_server::services::custody_action_executor::evm_address_from_private_key(
            &paymaster_private_key,
        )
        .expect("paymaster address");
    let mut paymasters = router_server::services::custody_action_executor::PaymasterRegistry::new();
    paymasters.register(ChainType::Ethereum, paymaster_address.clone());

    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    let paymaster_before = ethereum_chain
        .native_balance(&paymaster_address)
        .await
        .unwrap();

    let executor = Arc::new(
        CustodyActionExecutor::new(db.clone(), settings, h.chain_registry.clone())
            .with_paymasters(paymasters),
    );
    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::default()),
        executor,
        h.chain_registry.clone(),
    );

    let summary = execution_manager.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.executed_orders, 0);

    let updated_vault = db.orders().get_custody_vault(vault.id).await.unwrap();
    assert_eq!(updated_vault.status, CustodyVaultStatus::Failed);
    assert!(updated_vault.metadata.get("release_sweep_status").is_none());

    let paymaster_after = ethereum_chain
        .native_balance(&paymaster_address)
        .await
        .unwrap();
    assert_eq!(paymaster_after, paymaster_before);
}

#[tokio::test]
async fn order_executor_executes_provider_custody_action_intent() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let deposit_sink = Address::from_str("0x1000000000000000000000000000000000000005").unwrap();
    let deposit_sink_address = format!("{deposit_sink:?}");
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(CustodyIntentUnitProvider {
            deposit_sink: deposit_sink_address.clone(),
        })],
        vec![Arc::new(ProviderOnlyExchangeProvider)],
    ));
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
        action_providers.clone(),
    );
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());
    let amount_in = U256::from(1_000_000_000_000_u64);

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in.to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    anvil_set_balance(h, vault_address, U256::from(1_000_000_000_000_000_000_u64)).await;
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let provider = ProviderBuilder::new().connect_http(h.ethereum_endpoint_url());
    let sink_before = provider.get_balance(deposit_sink).await.unwrap();
    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        action_providers,
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    let summary = execution_manager.process_worker_pass(10).await.unwrap();

    assert_eq!(summary.planned_orders, 1);
    assert_eq!(summary.executed_orders, 1);
    let sink_after = provider.get_balance(deposit_sink).await.unwrap();
    assert_eq!(sink_after.saturating_sub(sink_before), amount_in);
    let order = db.orders().get(order.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .unwrap();
    assert_eq!(unit_step.status, OrderExecutionStepStatus::Completed);
    assert!(unit_step
        .tx_hash
        .as_deref()
        .unwrap_or_default()
        .starts_with("0x"));
    assert_eq!(unit_step.response["kind"], json!("custody_action"));
}

#[tokio::test]
async fn provider_operations_store_protocol_addresses_outside_custody_vaults() {
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let (order_manager, db) = test_order_manager(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )))
    .await;

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let now = Utc::now();
    let destination_vault = CustodyVault {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        role: CustodyVaultRole::DestinationExecution,
        visibility: CustodyVaultVisibility::Internal,
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: Some(AssetId::Native),
        address: valid_evm_address(),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: Some([0x11; 32]),
        signer_ref: Some("order-derived-key:test".to_string()),
        status: CustodyVaultStatus::Planned,
        metadata: json!({
            "reason": "across_destination_before_unit"
        }),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_custody_vault(&destination_vault)
        .await
        .unwrap();
    let execution_attempt = OrderExecutionAttempt {
        id: Uuid::now_v7(),
        order_id: order.id,
        attempt_index: 1,
        attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
        status: OrderExecutionAttemptStatus::Active,
        trigger_step_id: None,
        trigger_provider_operation_id: None,
        failure_reason: json!({}),
        input_custody_snapshot: json!({}),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_execution_attempt(&execution_attempt)
        .await
        .unwrap();

    let across_step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: Some(execution_attempt.id),
        transition_decl_id: None,
        step_index: 1,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider: "across".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: Some(order.source_asset.clone()),
        output_asset: Some(order.destination_asset.clone()),
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: Some(format!(
            "quote-{}",
            quote
                .quote
                .as_market_order()
                .expect("market order quote")
                .id
        )),
        idempotency_key: Some(format!("order:{}:across:1", order.id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({
            "schema_version": 1,
            "partial_fills_enabled": false,
            "refund_account_role": "source_deposit",
            "recipient_account_role": "destination_execution",
            "destination_custody_vault_id": destination_vault.id
        }),
        request: json!({
            "provider": "across",
            "trade_type": "exact_input"
        }),
        response: json!({}),
        error: json!({}),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_execution_step(&across_step)
        .await
        .unwrap();
    let provider_operation = OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_attempt_id: Some(execution_attempt.id),
        execution_step_id: Some(across_step.id),
        provider: "unit".to_string(),
        operation_type: ProviderOperationType::UnitDeposit,
        provider_ref: Some("unit-op-1".to_string()),
        status: ProviderOperationStatus::Submitted,
        request: json!({
            "asset": "native",
            "source_chain_id": "evm:1"
        }),
        response: json!({
            "deposit_address": "0x2000000000000000000000000000000000000001"
        }),
        observed_state: json!({}),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_provider_operation(&provider_operation)
        .await
        .unwrap();
    let provider_address = OrderProviderAddress {
        id: Uuid::now_v7(),
        order_id: order.id,
        execution_step_id: Some(across_step.id),
        provider_operation_id: Some(provider_operation.id),
        provider: "unit".to_string(),
        role: ProviderAddressRole::UnitDeposit,
        chain: ChainId::parse("evm:1").unwrap(),
        asset: Some(AssetId::Native),
        address: "0x2000000000000000000000000000000000000001".to_string(),
        memo: None,
        expires_at: None,
        metadata: json!({
            "source": "unit_generate_address"
        }),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_provider_address(&provider_address)
        .await
        .unwrap();

    let custody_vaults = db.orders().get_custody_vaults(order.id).await.unwrap();
    assert_eq!(custody_vaults.len(), 1);
    assert_eq!(
        custody_vaults[0].role,
        CustodyVaultRole::DestinationExecution
    );
    assert_eq!(
        custody_vaults[0].visibility,
        CustodyVaultVisibility::Internal
    );
    assert_eq!(
        custody_vaults[0].control_type,
        CustodyVaultControlType::RouterDerivedKey
    );

    let provider_operations = db.orders().get_provider_operations(order.id).await.unwrap();
    assert_eq!(provider_operations.len(), 1);
    assert_eq!(provider_operations[0].id, provider_operation.id);
    assert_eq!(
        provider_operations[0].operation_type,
        ProviderOperationType::UnitDeposit
    );
    assert_eq!(
        provider_operations[0].status,
        ProviderOperationStatus::Submitted
    );
    assert_eq!(
        provider_operations[0].provider_ref.as_deref(),
        Some("unit-op-1")
    );
    let provider_addresses = db.orders().get_provider_addresses(order.id).await.unwrap();
    assert_eq!(provider_addresses.len(), 1);
    assert_eq!(provider_addresses[0].id, provider_address.id);
    assert_eq!(provider_addresses[0].role, ProviderAddressRole::UnitDeposit);
    assert_eq!(provider_addresses[0].provider, "unit");
    assert_eq!(provider_addresses[0].address, provider_address.address);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_type, OrderExecutionStepType::AcrossBridge);
    assert_eq!(steps[0].provider, "across");
    assert_eq!(steps[0].details["partial_fills_enabled"], json!(false));
    assert_eq!(
        steps[0].details["destination_custody_vault_id"],
        json!(destination_vault.id)
    );

    let transitioned = db
        .orders()
        .transition_execution_step_status(
            across_step.id,
            OrderExecutionStepStatus::Planned,
            OrderExecutionStepStatus::Ready,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(transitioned.status, OrderExecutionStepStatus::Ready);
}

#[tokio::test]
async fn market_order_route_planner_uses_direct_unit_when_source_is_unit_ingress() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();
    let order = db.orders().get(order.id).await.unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();

    let plan = MarketOrderRoutePlanner::default()
        .plan(&order, &vault, &stored_quote, Utc::now())
        .unwrap();
    assert_eq!(
        plan.path_id,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(
        plan.steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::UnitDeposit,
            OrderExecutionStepType::UnitWithdrawal,
        ]
    );

    let inserted = db
        .orders()
        .create_execution_steps_idempotent(&plan.steps)
        .await
        .unwrap();
    assert_eq!(inserted, plan.steps.len() as u64);
    let duplicate_inserted = db
        .orders()
        .create_execution_steps_idempotent(&plan.steps)
        .await
        .unwrap();
    assert_eq!(duplicate_inserted, 0);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert_eq!(steps.len(), 1 + plan.steps.len());
    assert_eq!(steps[1].step_type, OrderExecutionStepType::UnitDeposit);
    assert_eq!(steps[1].details["transition_kind"], json!("unit_deposit"));
    assert_eq!(
        steps[1].details["unit_deposit_address_status"],
        json!("pending_provider_generation")
    );
    assert_eq!(
        steps[1].request["hyperliquid_custody_vault_role"],
        json!("hyperliquid_spot")
    );
    assert_eq!(
        steps[1].details["unit_deposit_provider_address_role"],
        json!("unit_deposit")
    );
    assert!(steps[1]
        .details
        .get("unit_deposit_custody_vault_role")
        .is_none());
}

#[tokio::test]
async fn release_quote_after_vault_creation_failure_allows_order_retry() {
    let h = harness().await;
    let db = test_db().await;
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let quote_id = quote
        .quote
        .as_market_order()
        .expect("market order quote")
        .id;
    let (order, linked_quote) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id,
            refund_address: valid_evm_address(),
            cancel_after: None,
            idempotency_key: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let (resumed_order, resumed_quote) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id,
            refund_address: valid_evm_address(),
            cancel_after: None,
            idempotency_key: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    assert_eq!(resumed_order.id, order.id);
    assert_eq!(resumed_quote.id, linked_quote.id);

    order_manager
        .release_quote_after_vault_creation_failure(order.id, linked_quote.id)
        .await
        .unwrap();

    let released_quote = db
        .orders()
        .get_market_order_quote_by_id(quote_id)
        .await
        .unwrap();
    assert_eq!(released_quote.order_id, None);
    assert!(db.orders().get(order.id).await.is_err());

    let (retry_order, retry_quote) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id,
            refund_address: valid_evm_address(),
            cancel_after: None,
            idempotency_key: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    assert_eq!(retry_quote.order_id, Some(retry_order.id));
    assert_ne!(retry_order.id, order.id);
}

#[tokio::test]
async fn expired_quote_cleanup_deletes_only_unassociated_quotes() {
    let db = test_db().await;
    let now = Utc::now();
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:1").unwrap(),
        asset: AssetId::Native,
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::Native,
    };

    let expired_unassociated = test_market_order_quote(
        source_asset.clone(),
        destination_asset.clone(),
        now - chrono::Duration::minutes(5),
    );
    let expired_associated = test_market_order_quote(
        source_asset.clone(),
        destination_asset.clone(),
        now - chrono::Duration::minutes(5),
    );
    let unexpired_unassociated = test_market_order_quote(
        source_asset.clone(),
        destination_asset.clone(),
        now + chrono::Duration::minutes(5),
    );

    db.orders()
        .create_market_order_quote(&expired_unassociated)
        .await
        .unwrap();
    db.orders()
        .create_market_order_quote(&expired_associated)
        .await
        .unwrap();
    db.orders()
        .create_market_order_quote(&unexpired_unassociated)
        .await
        .unwrap();

    let order_id = Uuid::now_v7();
    let order = RouterOrder {
        id: order_id,
        order_type: RouterOrderType::MarketOrder,
        status: RouterOrderStatus::Quoted,
        funding_vault_id: None,
        source_asset: source_asset.clone(),
        destination_asset: destination_asset.clone(),
        recipient_address: valid_evm_address(),
        refund_address: valid_evm_address(),
        action: RouterOrderAction::MarketOrder(MarketOrderAction {
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "1000".to_string(),
                min_amount_out: "1000".to_string(),
            },
            slippage_bps: 100,
        }),
        action_timeout_at: now + chrono::Duration::minutes(10),
        idempotency_key: None,
        workflow_trace_id: order_id.simple().to_string(),
        workflow_parent_span_id: "1111111111111111".to_string(),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_market_order_from_quote(&order, expired_associated.id)
        .await
        .unwrap();

    let deleted = db
        .orders()
        .delete_expired_unassociated_market_order_quotes(now)
        .await
        .unwrap();
    assert_eq!(deleted, 1);

    assert!(db
        .orders()
        .get_market_order_quote_by_id(expired_unassociated.id)
        .await
        .is_err());
    assert_eq!(
        db.orders()
            .get_market_order_quote_by_id(expired_associated.id)
            .await
            .unwrap()
            .order_id,
        Some(order.id)
    );
    assert_eq!(
        db.orders()
            .get_market_order_quote_by_id(unexpired_unassociated.id)
            .await
            .unwrap()
            .order_id,
        None
    );
    assert_eq!(db.orders().get(order.id).await.unwrap().id, order.id);
}

fn test_market_order_quote(
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    expires_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: None,
        source_asset,
        destination_asset,
        recipient_address: valid_evm_address(),
        provider_id: "test_provider".to_string(),
        order_kind: MarketOrderKindType::ExactIn,
        amount_in: "1000".to_string(),
        amount_out: "1000".to_string(),
        min_amount_out: Some("1000".to_string()),
        max_amount_in: None,
        slippage_bps: 100,
        provider_quote: json!({ "test": "quote_cleanup" }),
        expires_at,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn market_order_route_planner_uses_across_only_when_unit_needs_ingress_bridge() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    let order = db.orders().get(order.id).await.unwrap();
    let mut stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();
    if let Some(legs) = stored_quote
        .provider_quote
        .get_mut("legs")
        .and_then(Value::as_array_mut)
    {
        for leg in legs {
            if leg["transition_kind"] == json!("unit_deposit") {
                leg["amount_in"] = json!("997");
                leg["amount_out"] = json!("997");
            }
        }
    }

    let plan = MarketOrderRoutePlanner::default()
        .plan(&order, &vault, &stored_quote, Utc::now())
        .unwrap();
    assert_eq!(
        plan.steps[0].output_asset,
        Some(DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Native,
        })
    );
    assert_eq!(
        plan.path_id,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(
        plan.steps[1].input_asset,
        Some(DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Native,
        })
    );
    assert_eq!(plan.steps[1].output_asset, None);
    assert_eq!(
        plan.steps[2].output_asset,
        Some(DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Native,
        })
    );
    assert_eq!(
        plan.steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::AcrossBridge,
            OrderExecutionStepType::UnitDeposit,
            OrderExecutionStepType::UnitWithdrawal,
        ]
    );
    assert_eq!(plan.steps[0].details["partial_fills_enabled"], json!(false));
    assert_eq!(
        plan.steps[0].details["refund_custody_vault_id"],
        json!(vault.id)
    );

    db.orders()
        .create_execution_steps_idempotent(&plan.steps)
        .await
        .unwrap();
    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert_eq!(steps.len(), 1 + plan.steps.len());
    assert_eq!(steps[1].step_type, OrderExecutionStepType::AcrossBridge);
    assert_eq!(steps[1].details["transition_kind"], json!("across_bridge"));
    assert_eq!(steps[2].step_type, OrderExecutionStepType::UnitDeposit);
    assert_eq!(steps[2].amount_in.as_deref(), Some("997"));
    assert_eq!(steps[2].request["amount"], json!("997"));
    assert_eq!(
        steps[2].details["unit_deposit_provider_address_role"],
        json!("unit_deposit")
    );
    assert!(steps[2]
        .details
        .get("unit_deposit_custody_vault_role")
        .is_none());
}

#[tokio::test]
async fn market_order_route_planner_reserves_bitcoin_fee_for_unit_ingress() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote_with_refund(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
        valid_regtest_btc_address(),
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            action: VaultAction::Null,
            recovery_address: valid_regtest_btc_address(),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let order = db.orders().get(order.id).await.unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();
    assert_eq!(stored_quote.amount_in, "1000000");

    let legs = stored_quote.provider_quote["legs"].as_array().unwrap();
    let unit_leg = legs
        .iter()
        .find(|leg| leg["transition_kind"] == json!("unit_deposit"))
        .expect("bitcoin source route should include unit deposit");
    let reserve = unit_leg["raw"]["source_fee_reserve"]["amount"]
        .as_str()
        .unwrap()
        .parse::<u128>()
        .unwrap();
    let unit_amount = unit_leg["amount_in"]
        .as_str()
        .unwrap()
        .parse::<u128>()
        .unwrap();
    assert!(reserve > 0);
    assert_eq!(unit_amount + reserve, 1_000_000);
    assert_eq!(unit_leg["amount_out"], json!(unit_amount.to_string()));

    let plan = MarketOrderRoutePlanner::default()
        .plan(&order, &vault, &stored_quote, Utc::now())
        .unwrap();
    assert_eq!(
        plan.steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::UnitDeposit,
            OrderExecutionStepType::HyperliquidTrade,
            OrderExecutionStepType::HyperliquidTrade,
            OrderExecutionStepType::UnitWithdrawal,
        ]
    );
    let unit_amount_string = unit_amount.to_string();
    assert_eq!(
        plan.steps[0].amount_in.as_deref(),
        Some(unit_amount_string.as_str())
    );
    assert_eq!(plan.steps[0].request["amount"], json!(unit_amount_string));
    assert_eq!(
        plan.steps[0].request["source_custody_vault_id"],
        json!(vault.id)
    );
}

#[tokio::test]
async fn market_order_route_planner_supports_cctp_to_hyperliquid_bridge_path() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let base_usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string();
    let arbitrum_usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831".to_string();
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Reference(base_usdc.clone()),
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_regtest_btc_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "6000000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Reference(base_usdc),
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    let order = db.orders().get(order.id).await.unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();
    let gas_plan = &stored_quote.provider_quote["gas_reimbursement"];
    assert_eq!(
        gas_plan["policy"],
        json!("optimized_cross_chain_paymaster_v1")
    );
    assert_eq!(gas_plan["debts"].as_array().unwrap().len(), 3);
    assert_eq!(gas_plan["retention_actions"].as_array().unwrap().len(), 1);
    assert_eq!(
        gas_plan["retention_actions"][0]["settlement_chain_id"],
        json!("hyperliquid")
    );
    assert_eq!(
        gas_plan["retention_actions"][0]["settlement_asset_id"],
        json!("native")
    );
    assert_eq!(
        gas_plan["retention_actions"][0]["settlement_provider_asset"],
        json!("USDC")
    );
    let retained_usdc = gas_plan["retention_actions"][0]["amount"]
        .as_str()
        .unwrap()
        .parse::<u128>()
        .unwrap();
    let first_leg_amount = stored_quote.provider_quote["legs"][0]["amount_in"]
        .as_str()
        .unwrap()
        .parse::<u128>()
        .unwrap();
    assert_eq!(first_leg_amount, 6_000_000);
    assert!(retained_usdc > 0);

    let plan = MarketOrderRoutePlanner::default()
        .plan(&order, &vault, &stored_quote, Utc::now())
        .unwrap();
    assert_eq!(
        plan.steps[0].output_asset,
        Some(DepositAsset {
            chain: ChainId::parse("evm:42161").unwrap(),
            asset: AssetId::Reference(arbitrum_usdc.clone()),
        })
    );
    assert_eq!(
        plan.steps[1].output_asset,
        Some(DepositAsset {
            chain: ChainId::parse("evm:42161").unwrap(),
            asset: AssetId::Reference(arbitrum_usdc.clone()),
        })
    );
    assert_eq!(
        plan.steps[2].output_asset,
        Some(DepositAsset {
            chain: ChainId::parse("hyperliquid").unwrap(),
            asset: AssetId::Native,
        })
    );
    assert_eq!(
        plan.path_id,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(
        plan.steps
            .iter()
            .map(|step| step.step_type)
            .collect::<Vec<_>>(),
        vec![
            OrderExecutionStepType::CctpBurn,
            OrderExecutionStepType::CctpReceive,
            OrderExecutionStepType::HyperliquidBridgeDeposit,
            OrderExecutionStepType::HyperliquidTrade,
            OrderExecutionStepType::UnitWithdrawal,
        ]
    );
    assert_eq!(
        plan.steps[2].request["source_custody_vault_role"],
        json!("destination_execution")
    );
    assert!(plan.steps[0].request.get("retention_actions").is_none());
    assert!(plan.steps[1].request.get("retention_actions").is_none());
    assert!(plan.steps[2].request.get("retention_actions").is_none());
    assert_eq!(
        plan.steps[3].request["prefund_from_withdrawable"],
        json!(true)
    );
    assert_eq!(
        plan.steps[3].request["retention_actions"][0]["amount"],
        gas_plan["retention_actions"][0]["amount"]
    );
    assert_eq!(
        plan.steps[3].request["hyperliquid_custody_vault_role"],
        json!("destination_execution")
    );
    assert_eq!(
        plan.steps[3].request["hyperliquid_custody_vault_chain_id"],
        json!("evm:42161")
    );
    assert_eq!(
        plan.steps[3].request["hyperliquid_custody_vault_asset_id"],
        json!(arbitrum_usdc)
    );
}

#[tokio::test]
async fn provider_quotes_support_across_to_hyperliquid_bridge_path_under_mocks() {
    let h = harness().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let providers = ActionProviderRegistry::mock_http(mocks.base_url());

    let base_usdc = DepositAsset {
        chain: ChainId::parse("evm:8453").unwrap(),
        asset: AssetId::Reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string()),
    };
    let arbitrum_usdc = DepositAsset {
        chain: ChainId::parse("evm:42161").unwrap(),
        asset: AssetId::Reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831".to_string()),
    };
    let hyperliquid_usdc = DepositAsset {
        chain: ChainId::parse("hyperliquid").unwrap(),
        asset: AssetId::Native,
    };
    let bitcoin = DepositAsset {
        chain: ChainId::parse("bitcoin").unwrap(),
        asset: AssetId::Native,
    };

    let unit = providers.unit("unit").unwrap();
    assert!(unit.supports_withdrawal(&bitcoin));

    let across = providers.bridge("across").unwrap();
    let across_quote = across
        .quote_bridge(BridgeQuoteRequest {
            source_asset: base_usdc.clone(),
            destination_asset: arbitrum_usdc.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: "6000000".to_string(),
                min_amount_out: "1".to_string(),
            },
            recipient_address: valid_evm_address(),
            depositor_address: valid_evm_address(),
            partial_fills_enabled: false,
        })
        .await
        .unwrap()
        .expect("across quote should exist");

    let hyperliquid_bridge = providers.bridge("hyperliquid_bridge").unwrap();
    let hyperliquid_bridge_quote = hyperliquid_bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: arbitrum_usdc.clone(),
            destination_asset: hyperliquid_usdc.clone(),
            order_kind: MarketOrderKind::ExactIn {
                amount_in: across_quote.amount_out.clone(),
                min_amount_out: across_quote.amount_out.clone(),
            },
            recipient_address: valid_evm_address(),
            depositor_address: valid_evm_address(),
            partial_fills_enabled: false,
        })
        .await
        .unwrap()
        .expect("hyperliquid bridge quote should exist");

    let exchange = providers.exchange("hyperliquid").unwrap();
    let exchange_quote = exchange
        .quote_trade(ExchangeQuoteRequest {
            input_asset: hyperliquid_usdc,
            output_asset: bitcoin,
            input_decimals: None,
            output_decimals: None,
            order_kind: MarketOrderKind::ExactIn {
                amount_in: hyperliquid_bridge_quote.amount_out.clone(),
                min_amount_out: "1".to_string(),
            },
            sender_address: None,
            recipient_address: valid_regtest_btc_address(),
        })
        .await
        .unwrap()
        .expect("hyperliquid exchange quote should exist");

    assert_eq!(across_quote.amount_in, "6000000");
    assert!(across_quote.amount_out.parse::<u128>().unwrap() >= 5_000_000);
    assert!(hyperliquid_bridge_quote.amount_out.parse::<u128>().unwrap() >= 5_000_000);
    assert!(exchange_quote.amount_out.parse::<u128>().unwrap() >= 1);
}

#[tokio::test]
async fn market_order_planning_pass_materializes_direct_unit_and_matches_mock_unit_shape() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings, h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();

    let materialized = OrderExecutionManager::new(db.clone())
        .process_planning_pass(10)
        .await
        .unwrap();
    assert_eq!(materialized.len(), 1);
    assert_eq!(
        materialized[0].plan_kind,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(materialized[0].inserted_steps, 2);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .unwrap();
    let unit_request: MockUnitDepositStepRequest =
        serde_json::from_value(unit_step.request.clone()).unwrap();
    assert_eq!(unit_request.src_chain_id, "evm:1");
    assert_eq!(unit_request.dst_chain_id, "hyperliquid");
    assert_eq!(unit_request.asset_id, "native");
    assert_eq!(unit_request.source_custody_vault_id, Some(vault.id));
    assert_eq!(unit_request.revert_custody_vault_id, Some(vault.id));
    assert_eq!(
        unit_request.hyperliquid_custody_vault_role.as_deref(),
        Some("hyperliquid_spot")
    );
    let _ = mocks;
}

#[tokio::test]
async fn market_order_planning_pass_materializes_cctp_to_hyperliquid_bridge_and_hydrates_destination_execution_vault(
) {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let base_usdc = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string();
    let arbitrum_usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831".to_string();
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Reference(base_usdc.clone()),
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_regtest_btc_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "6000000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Reference(base_usdc),
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();
    let retained_usdc = stored_quote.provider_quote["gas_reimbursement"]["retention_actions"][0]
        ["amount"]
        .as_str()
        .unwrap()
        .parse::<u128>()
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    let materialized = execution_manager.process_planning_pass(10).await.unwrap();
    assert_eq!(materialized.len(), 1);
    assert_eq!(
        materialized[0].plan_kind,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(materialized[0].inserted_steps, 5);

    let custody_vaults = db.orders().get_custody_vaults(order.id).await.unwrap();
    let destination_vault = custody_vaults
        .iter()
        .find(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .expect("destination execution custody vault should be materialized");
    assert_eq!(
        destination_vault.visibility,
        CustodyVaultVisibility::Internal
    );
    assert_eq!(
        destination_vault.chain,
        ChainId::parse("evm:42161").unwrap()
    );
    assert_eq!(
        destination_vault.asset,
        Some(AssetId::Reference(arbitrum_usdc.clone()))
    );
    assert_eq!(
        destination_vault.control_type,
        CustodyVaultControlType::RouterDerivedKey
    );
    assert_eq!(destination_vault.status, CustodyVaultStatus::Active);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let cctp_burn_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::CctpBurn)
        .unwrap();
    let cctp_burn_request: router_server::services::action_providers::CctpBurnStepRequest =
        serde_json::from_value(cctp_burn_step.request.clone()).unwrap();
    assert_eq!(cctp_burn_request.source_chain_id, "evm:8453");
    assert_eq!(cctp_burn_request.destination_chain_id, "evm:42161");
    assert!(cctp_burn_step.request.get("retention_actions").is_none());
    assert_eq!(cctp_burn_step.amount_in.as_deref(), Some("6000000"));
    assert_eq!(
        cctp_burn_step.request["recipient_custody_vault_role"],
        json!("destination_execution")
    );
    assert_eq!(
        cctp_burn_request.recipient_address,
        destination_vault.address
    );
    assert_eq!(
        cctp_burn_request.source_custody_vault_address.as_deref(),
        Some(vault.deposit_vault_address.as_str())
    );

    let cctp_receive_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::CctpReceive)
        .unwrap();
    let cctp_receive_request: router_server::services::action_providers::CctpReceiveStepRequest =
        serde_json::from_value(cctp_receive_step.request.clone()).unwrap();
    assert_eq!(
        cctp_receive_request.burn_transition_decl_id,
        cctp_burn_request.transition_decl_id
    );
    assert_eq!(cctp_receive_request.destination_chain_id, "evm:42161");
    assert_eq!(cctp_receive_step.amount_in.as_deref(), Some("6000000"));
    assert_eq!(
        cctp_receive_request.source_custody_vault_address.as_deref(),
        Some(destination_vault.address.as_str())
    );
    assert_eq!(
        cctp_receive_request.source_custody_vault_id,
        Some(destination_vault.id)
    );

    let hyperliquid_bridge_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::HyperliquidBridgeDeposit)
        .unwrap();
    assert_eq!(
        hyperliquid_bridge_step
            .request
            .get("source_custody_vault_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(destination_vault.id)
    );
    assert_eq!(
        hyperliquid_bridge_step.request["source_custody_vault_address"],
        json!(destination_vault.address)
    );

    let hyperliquid_trade_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::HyperliquidTrade)
        .unwrap();
    assert_eq!(
        hyperliquid_trade_step.request["prefund_from_withdrawable"],
        json!(true)
    );
    assert_eq!(
        hyperliquid_trade_step.request["retention_actions"][0]["recipient_role"],
        json!("paymaster_wallet")
    );
    assert_eq!(
        hyperliquid_trade_step.request["retention_actions"][0]["settlement_chain_id"],
        json!("hyperliquid")
    );
    assert_eq!(
        hyperliquid_trade_step.request["retention_actions"][0]["settlement_provider_asset"],
        json!("USDC")
    );
    assert_eq!(
        hyperliquid_trade_step.request["retention_actions"][0]["amount"]
            .as_str()
            .unwrap()
            .parse::<u128>()
            .unwrap(),
        retained_usdc
    );
    assert_eq!(
        hyperliquid_trade_step
            .request
            .get("hyperliquid_custody_vault_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(destination_vault.id)
    );
    assert_eq!(
        hyperliquid_trade_step.request["hyperliquid_custody_vault_address"],
        json!(destination_vault.address)
    );

    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitWithdrawal)
        .unwrap();
    assert_eq!(
        unit_step
            .request
            .get("hyperliquid_custody_vault_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(destination_vault.id)
    );
    assert_eq!(
        unit_step.request["hyperliquid_custody_vault_address"],
        json!(destination_vault.address)
    );
}

#[tokio::test]
async fn market_order_planning_pass_materializes_across_to_unit_and_matches_mock_shapes() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings, h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            Arc::new(test_settings(dir.path())),
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    let materialized = execution_manager.process_planning_pass(10).await.unwrap();
    assert_eq!(materialized.len(), 1);
    assert_eq!(
        materialized[0].plan_kind,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(materialized[0].inserted_steps, 3);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .unwrap();
    let across_request: router_server::services::action_providers::AcrossExecuteStepRequest =
        serde_json::from_value(across_step.request.clone()).unwrap();
    assert_eq!(across_request.origin_chain_id, "evm:8453");
    assert_eq!(across_request.destination_chain_id, "evm:1");
    assert_eq!(across_request.refund_address, vault.deposit_vault_address);
    assert_eq!(
        across_request.depositor_address,
        vault.deposit_vault_address
    );
    assert_eq!(across_request.depositor_custody_vault_id, vault.id);
    assert_eq!(
        across_step.request["recipient_custody_vault_role"],
        json!("destination_execution")
    );

    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .unwrap();
    let unit_request: MockUnitDepositStepRequest =
        serde_json::from_value(unit_step.request.clone()).unwrap();
    assert_eq!(unit_request.src_chain_id, "evm:1");
    assert_eq!(unit_request.dst_chain_id, "hyperliquid");
    assert_eq!(unit_request.asset_id, "native");
    assert_eq!(
        unit_request.source_custody_vault_role.as_deref(),
        Some("destination_execution")
    );
    assert_eq!(
        unit_request.revert_custody_vault_role.as_deref(),
        Some("destination_execution")
    );
    assert_eq!(
        unit_request.hyperliquid_custody_vault_role.as_deref(),
        Some("hyperliquid_spot")
    );
    let _ = mocks;
}

#[tokio::test]
async fn market_order_planning_hydrates_destination_execution_custody_vault() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();
    let stored_quote = db.orders().get_market_order_quote(order.id).await.unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    let materialized = execution_manager.process_planning_pass(10).await.unwrap();
    assert_eq!(materialized.len(), 1);
    assert_eq!(
        materialized[0].plan_kind,
        stored_quote.provider_quote["path_id"].as_str().unwrap()
    );
    assert_eq!(materialized[0].inserted_steps, 3);

    let custody_vaults = db.orders().get_custody_vaults(order.id).await.unwrap();
    let destination_vault = custody_vaults
        .iter()
        .find(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .expect("destination execution custody vault should be materialized");
    assert_eq!(
        destination_vault.visibility,
        CustodyVaultVisibility::Internal
    );
    assert_eq!(destination_vault.chain, ChainId::parse("evm:1").unwrap());
    assert_eq!(destination_vault.asset, Some(AssetId::Native));
    assert_eq!(
        destination_vault.control_type,
        CustodyVaultControlType::RouterDerivedKey
    );
    assert_eq!(destination_vault.status, CustodyVaultStatus::Active);
    assert!(destination_vault.derivation_salt.is_some());
    assert!(!destination_vault.address.is_empty());

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .unwrap();
    let across_request: router_server::services::action_providers::AcrossExecuteStepRequest =
        serde_json::from_value(across_step.request.clone()).unwrap();
    assert_eq!(across_request.recipient, destination_vault.address);
    assert_eq!(
        across_step
            .request
            .get("recipient_custody_vault_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(destination_vault.id)
    );

    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .unwrap();
    let unit_request: MockUnitDepositStepRequest =
        serde_json::from_value(unit_step.request.clone()).unwrap();
    assert_eq!(
        unit_request.source_custody_vault_id,
        Some(destination_vault.id)
    );
    assert_eq!(
        unit_request.revert_custody_vault_id,
        Some(destination_vault.id)
    );
    assert_eq!(
        unit_request.source_custody_vault_role.as_deref(),
        Some("destination_execution")
    );
    assert_eq!(
        unit_request.revert_custody_vault_role.as_deref(),
        Some("destination_execution")
    );
    assert_eq!(
        unit_request.hyperliquid_custody_vault_role.as_deref(),
        Some("hyperliquid_spot")
    );
}

#[tokio::test]
async fn order_executor_completes_mock_across_unit_hyperliquid_flow() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid base vault address");
    anvil_set_base_balance(h, vault_address, U256::from(1000_u64)).await;
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    drive_across_order_to_completion(&execution_manager, &mocks, &db, order.id).await;

    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
    let deposits = mocks.across_deposits_from(vault_address).await;
    assert_eq!(deposits.len(), 1);
    assert_eq!(deposits[0].input_amount, U256::from(1000_u64));
    assert_eq!(deposits[0].output_amount, U256::from(1000_u64));
    let gen_requests = mocks.unit_generate_address_requests().await;
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Deposit).len(),
        1
    );
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Withdrawal).len(),
        1
    );
    let ledger = mocks.ledger_snapshot().await;
    assert!(ledger.bridged_balances.is_empty());
    assert!(ledger.hyperliquid_balances.is_empty());
    assert_eq!(
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Deposit)
            .len(),
        1
    );
    assert_eq!(
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Withdrawal)
            .len(),
        1
    );
    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert!(steps
        .iter()
        .filter(|step| step.step_index > 0)
        .all(|step| step.status == OrderExecutionStepStatus::Completed));
}

/// End-to-end cross-token flow: Base ETH → Across → evm:1 UETH → HyperUnit
/// deposit → HL two-leg swap (UETH → USDC → UBTC) → HyperUnit withdrawal →
/// bitcoin regtest. Exercises the path where HL's `quote_trade` returns two
/// legs and the planner materializes two `HyperliquidTrade` steps in sequence
/// between UnitDeposit and UnitWithdrawal.
#[tokio::test]
async fn order_executor_completes_base_eth_to_btc_cross_token_flow() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    // 0.01 Base ETH. Picked so HL leg sizes (0.01 UETH, 30 USDC, 0.0005 UBTC)
    // land cleanly above sz_decimals precision for both UETH (4) and UBTC (5)
    // at the default mock rates (UETH/USDC=3000, UBTC/USDC=60000).
    let amount_in_wei = "10000000000000000".to_string();

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_regtest_btc_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in_wei.clone(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid base vault address");
    // Fund with the 0.01 ETH deposit + 1 ETH gas pad so the vault can cover
    // the AcrossBridge transfer's gas costs.
    let source_balance = U256::from_str_radix(&amount_in_wei, 10).unwrap()
        + U256::from(1_000_000_000_000_000_000_u128);
    anvil_set_base_balance(h, vault_address, source_balance).await;
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    drive_cross_token_across_order_to_completion(&execution_manager, &mocks, &db, order.id, 0.01)
        .await;

    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
    let internal_vaults = internal_custody_vaults_for_order(&db, order.id).await;
    assert!(
        !internal_vaults.is_empty(),
        "completed cross-token route should have internal custody vaults to release"
    );
    assert!(internal_vaults.iter().all(|vault| {
        vault.status == CustodyVaultStatus::Released
            && vault.metadata["lifecycle_terminal_reason"] == json!("order_completed")
            && vault.metadata["lifecycle_order_status"] == json!("completed")
            && vault.metadata["lifecycle_balance_verified"] == json!(false)
    }));

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert!(steps
        .iter()
        .filter(|step| step.step_index > 0)
        .all(|step| step.status == OrderExecutionStepStatus::Completed));
    let hl_steps: Vec<_> = steps
        .iter()
        .filter(|step| step.step_type == OrderExecutionStepType::HyperliquidTrade)
        .collect();
    assert_eq!(
        hl_steps.len(),
        2,
        "cross-token ETH→BTC flow should materialize two HL trade legs"
    );

    let gen_requests = mocks.unit_generate_address_requests().await;
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Deposit).len(),
        1
    );
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Withdrawal).len(),
        1
    );
}

#[tokio::test]
async fn order_executor_completes_btc_to_eth_unit_hyperliquid_flow() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let amount_in_sats = "1000000".to_string();
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: amount_in_sats.clone(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote_with_refund(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
        valid_regtest_btc_address(),
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            action: VaultAction::Null,
            recovery_address: valid_regtest_btc_address(),
            cancellation_commitment: dummy_commitment(),
            cancel_after: None,
            metadata: json!({}),
        })
        .await
        .unwrap();
    let vault_address = bitcoin::Address::from_str(&vault.deposit_vault_address)
        .expect("valid bitcoin source vault address")
        .assume_checked();
    h.deal_bitcoin(
        &vault_address,
        &Amount::from_sat(amount_in_sats.parse::<u64>().unwrap()),
    )
    .await
    .expect("fund bitcoin source vault");
    h.wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync after bitcoin funding");
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    drive_unit_ingress_order_to_completion(&execution_manager, &mocks, &db, order.id).await;

    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);

    let unit_deposit =
        provider_operation_by_type(&db, order.id, ProviderOperationType::UnitDeposit).await;
    let unit_amount = unit_deposit
        .request
        .get("expected_amount")
        .and_then(Value::as_str)
        .or_else(|| unit_deposit.request.get("amount").and_then(Value::as_str))
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert!(unit_amount < amount_in_sats.parse::<u64>().unwrap());
}

/// Variant of `drive_across_order_to_completion` that accounts for HL having
/// real balance dependencies between UnitDeposit and the first HL leg. The
/// real HyperUnit guardian credits the user's HL spot balance after observing
/// the source-chain deposit; in the mock we do it explicitly once the router
/// has signed the transfer and before the HL leg tries to sell that balance.
///
/// `unit_deposit_natural_amount` is the amount to credit on the HL side, in
/// natural (human-readable) units. The coin symbol is inferred from the unit
/// ingress asset: Ethereum/Base Native → UETH; Bitcoin Native → UBTC.
async fn drive_cross_token_across_order_to_completion(
    execution_manager: &OrderExecutionManager,
    mocks: &MockIntegratorServer,
    db: &Database,
    order_id: Uuid,
    unit_deposit_natural_amount: f64,
) {
    // Across bridge leg — identical to the single-canonical driver.
    execution_manager.process_worker_pass(10).await.unwrap();
    let source_vault_address = source_deposit_vault_address_from_db(db, order_id).await;
    wait_for_across_deposit_indexed(mocks, source_vault_address, Duration::from_secs(10)).await;
    let across_operation =
        provider_operation_by_type(db, order_id, ProviderOperationType::AcrossBridge).await;
    record_detector_provider_status_hint(execution_manager, &across_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    fund_destination_execution_vault_from_across(db, order_id).await;

    // Unit deposit leg.
    execution_manager.process_worker_pass(10).await.unwrap();
    let unit_deposit =
        provider_operation_by_type(db, order_id, ProviderOperationType::UnitDeposit).await;
    mocks
        .complete_unit_operation(provider_ref(&unit_deposit))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(execution_manager, db, &unit_deposit).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    // Guardian credit: mirror the real HyperUnit flow by crediting the router's
    // HL spot vault with the deposited coin. Without this, the HL SELL leg will
    // fail with an insufficient-balance error.
    let hl_vault_address = hyperliquid_spot_vault_address(db, order_id).await;
    let deposit_coin = hl_deposit_coin_for_operation(&unit_deposit);
    mocks
        .credit_hyperliquid_balance(hl_vault_address, &deposit_coin, unit_deposit_natural_amount)
        .await;

    // One worker pass advances through both HL trade legs synchronously
    // (marketable mock GTC orders are fully resolved inside execute_step), then kicks
    // off the async UnitWithdrawal which resolves via spotSend.
    execution_manager.process_worker_pass(10).await.unwrap();
    let unit_withdrawal =
        provider_operation_by_type(db, order_id, ProviderOperationType::UnitWithdrawal).await;
    mocks
        .complete_unit_operation(provider_ref(&unit_withdrawal))
        .await
        .unwrap();
    record_detector_provider_status_hint(execution_manager, &unit_withdrawal).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        execution_manager.process_worker_pass(10).await.unwrap();
        let order = db.orders().get(order_id).await.unwrap();
        if matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::Refunding
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::Failed
        ) {
            if !matches!(order.status, RouterOrderStatus::Completed) {
                dump_order_state(db, order_id).await;
            }
            return;
        }
        if start.elapsed() >= Duration::from_secs(10) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    dump_order_state(db, order_id).await;
    panic!(
        "cross-token order {order_id} did not reach a terminal state within 10s after UnitWithdrawal"
    );
}

async fn drive_unit_ingress_order_to_completion(
    execution_manager: &OrderExecutionManager,
    mocks: &MockIntegratorServer,
    db: &Database,
    order_id: Uuid,
) {
    execution_manager.process_worker_pass(10).await.unwrap();
    let unit_deposit =
        provider_operation_by_type(db, order_id, ProviderOperationType::UnitDeposit).await;

    let h = harness().await;
    h.mine_bitcoin_blocks(1)
        .await
        .expect("mine bitcoin unit deposit transfer");
    h.wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync after bitcoin unit deposit");

    mocks
        .complete_unit_operation(provider_ref(&unit_deposit))
        .await
        .unwrap();
    record_detector_provider_status_hint(execution_manager, &unit_deposit).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    let unit_deposit = db
        .orders()
        .get_provider_operation(unit_deposit.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(mocks, db, order_id, &unit_deposit).await;

    execution_manager.process_worker_pass(10).await.unwrap();
    let unit_withdrawal =
        provider_operation_by_type(db, order_id, ProviderOperationType::UnitWithdrawal).await;
    mocks
        .complete_unit_operation(provider_ref(&unit_withdrawal))
        .await
        .unwrap();
    record_detector_provider_status_hint(execution_manager, &unit_withdrawal).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        execution_manager.process_worker_pass(10).await.unwrap();
        let order = db.orders().get(order_id).await.unwrap();
        if matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::Refunding
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::Failed
        ) {
            if !matches!(order.status, RouterOrderStatus::Completed) {
                dump_order_state(db, order_id).await;
            }
            return;
        }
        if start.elapsed() >= Duration::from_secs(10) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    dump_order_state(db, order_id).await;
    panic!("unit-ingress order {order_id} did not reach a terminal state within 10s");
}

async fn hyperliquid_spot_vault_address(db: &Database, order_id: Uuid) -> Address {
    let vaults = db.orders().get_custody_vaults(order_id).await.unwrap();
    let hl_vault = vaults
        .into_iter()
        .find(|v| v.role == CustodyVaultRole::HyperliquidSpot)
        .expect("cross-token flow should have derived a hyperliquid_spot custody vault");
    Address::from_str(&hl_vault.address).expect("valid HL spot vault EVM address")
}

fn hl_deposit_coin_for_operation(operation: &OrderProviderOperation) -> String {
    // The UnitDeposit operation request uses unit-wire keys (`src_chain`,
    // `asset`) populated from `UnitChain::as_wire_str()` and the unit asset
    // identifier. Map the unit asset directly to its HL spot coin symbol.
    let asset = operation
        .request
        .get("asset")
        .and_then(Value::as_str)
        .unwrap_or("");
    match asset {
        "btc" => "UBTC".to_string(),
        "eth" => "UETH".to_string(),
        other => panic!("hl_deposit_coin_for_operation: unsupported UnitDeposit asset {other:?}"),
    }
}

#[tokio::test]
async fn order_executor_advances_async_provider_operations_after_detector_hints() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 0);
    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    assert_eq!(
        across_operation.status,
        ProviderOperationStatus::WaitingExternal
    );

    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&execution_manager, &across_operation).await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .db
            .orders()
            .get_provider_operation(across_operation.id)
            .await
            .unwrap()
            .status,
        ProviderOperationStatus::Completed
    );
    assert_eq!(
        fixture
            .mocks
            .across_deposits_from(fixture.vault_address)
            .await
            .len(),
        1
    );

    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    if let Some(summary) = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
    {
        assert_eq!(summary.completed_steps, 1);
    }
    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    assert_eq!(
        unit_deposit_operation.status,
        ProviderOperationStatus::WaitingExternal
    );
    assert!(filter_unit_operations_by_kind(
        &fixture.mocks.ledger_snapshot().await.unit_operations,
        MockUnitOperationKind::Deposit
    )
    .iter()
    .all(|op| op.state != "done"));

    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .db
            .orders()
            .get_provider_operation(unit_deposit_operation.id)
            .await
            .unwrap()
            .status,
        ProviderOperationStatus::Completed
    );
    let unit_deposit_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(
        &fixture.mocks,
        &fixture.db,
        fixture.order_id,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        filter_unit_operations_by_kind(
            &fixture.mocks.ledger_snapshot().await.unit_operations,
            MockUnitOperationKind::Deposit
        )
        .len(),
        1
    );

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 2);
    let unit_withdrawal_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitWithdrawal,
    )
    .await;
    assert_eq!(
        unit_withdrawal_operation.status,
        ProviderOperationStatus::WaitingExternal
    );
    assert!(filter_unit_operations_by_kind(
        &fixture.mocks.ledger_snapshot().await.unit_operations,
        MockUnitOperationKind::Withdrawal
    )
    .iter()
    .all(|op| op.state != "done"));

    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_withdrawal_operation))
        .await
        .unwrap();
    record_detector_provider_status_hint(&execution_manager, &unit_withdrawal_operation).await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .db
            .orders()
            .get_provider_operation(unit_withdrawal_operation.id)
            .await
            .unwrap()
            .status,
        ProviderOperationStatus::Completed
    );
    assert_eq!(
        filter_unit_operations_by_kind(
            &fixture.mocks.ledger_snapshot().await.unit_operations,
            MockUnitOperationKind::Withdrawal
        )
        .len(),
        1
    );

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
}

#[tokio::test]
async fn order_executor_rejects_tampered_nonfinal_intermediate_custody_route() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let now = Utc::now();
    let hyperliquid_vault = CustodyVault {
        id: Uuid::now_v7(),
        order_id: Some(fixture.order_id),
        role: CustodyVaultRole::HyperliquidSpot,
        visibility: CustodyVaultVisibility::Internal,
        chain: ChainId::parse("hyperliquid").unwrap(),
        asset: None,
        address: "0x2000000000000000000000000000000000000002".to_string(),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: Some([0x44; 32]),
        signer_ref: None,
        status: CustodyVaultStatus::Active,
        metadata: json!({ "source": "tamper_test" }),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_custody_vault(&hyperliquid_vault)
        .await
        .unwrap();
    let execution_attempt = OrderExecutionAttempt {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        attempt_index: 1,
        attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
        status: OrderExecutionAttemptStatus::Active,
        trigger_step_id: None,
        trigger_provider_operation_id: None,
        failure_reason: json!({}),
        input_custody_snapshot: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_attempt(&execution_attempt)
        .await
        .unwrap();

    let tampered_across = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        transition_decl_id: None,
        step_index: 1,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider: "across".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("order:{}:tampered:1", fixture.order_id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({ "source": "tamper_test" }),
        request: json!({
            "recipient_custody_vault_role": "hyperliquid_spot",
            "recipient_custody_vault_id": hyperliquid_vault.id,
            "recipient": hyperliquid_vault.address,
        }),
        response: json!({}),
        error: json!({}),
        created_at: now,
        updated_at: now,
    };
    let final_withdrawal = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        transition_decl_id: None,
        step_index: 2,
        step_type: OrderExecutionStepType::UnitWithdrawal,
        provider: "unit".to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("order:{}:tampered:2", fixture.order_id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: None,
        completed_at: None,
        details: json!({ "source": "tamper_test" }),
        request: json!({
            "hyperliquid_custody_vault_role": "hyperliquid_spot",
            "hyperliquid_custody_vault_id": hyperliquid_vault.id,
            "hyperliquid_custody_vault_address": hyperliquid_vault.address,
        }),
        response: json!({}),
        error: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_steps_idempotent(&[tampered_across.clone(), final_withdrawal])
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let err = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .expect_err("tampered route must reject before execution");
    let err_text = err.to_string();
    assert!(
        err_text.contains("intermediate custody invariant"),
        "{err:?}"
    );
    assert!(fixture
        .db
        .orders()
        .get_provider_operations(fixture.order_id)
        .await
        .unwrap()
        .is_empty());
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.id == tampered_across.id)
        .expect("tampered step should still exist");
    assert_eq!(across_step.status, OrderExecutionStepStatus::Planned);
}

#[tokio::test]
async fn order_executor_retries_when_async_provider_operation_fails_after_detector_hint() {
    let fixture =
        funded_market_order_fixture_with_amount("evm:8453", "evm:1", "1000000000000000").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&execution_manager, &across_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    let unit_deposit_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(
        &fixture.mocks,
        &fixture.db,
        fixture.order_id,
        &unit_deposit_operation,
    )
    .await;

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    let unit_withdrawal_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitWithdrawal,
    )
    .await;
    fixture
        .mocks
        .fail_unit_operation(
            provider_ref(&unit_withdrawal_operation),
            "mock unit withdrawal async status failed",
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .mocks
            .unit_operation_state(provider_ref(&unit_withdrawal_operation))
            .await
            .as_deref(),
        Some("failure")
    );
    record_detector_provider_status_hint(&execution_manager, &unit_withdrawal_operation).await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );

    let failed_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_withdrawal_operation.id)
        .await
        .unwrap();
    assert_eq!(failed_operation.status, ProviderOperationStatus::Failed);
    let failed_step = fixture
        .db
        .orders()
        .get_execution_step(unit_withdrawal_operation.execution_step_id.unwrap())
        .await
        .unwrap();
    assert_eq!(failed_step.status, OrderExecutionStepStatus::Failed);
    // The real HyperUnit API exposes terminal failures via `state: "failure"`
    // on the UnitOperation — it doesn't return structured error messages — so
    // the router's fallback error JSON surfaces the provider status + observed
    // state. Assert on the observable facts the mock faithfully reports.
    let failed_error = failed_step.error.to_string();
    assert!(
        failed_error.contains("\"failed\""),
        "expected step error to record failed status, got {failed_error}"
    );
    assert!(
        failed_error.contains("\"failure\""),
        "expected step error to carry observed UnitOperation state=failure, got {failed_error}"
    );
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);
    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Active);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RetryExecution
    );
    let retry_steps = fixture
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .unwrap();
    assert_eq!(retry_steps.len(), 1);
    assert_eq!(
        retry_steps[0].step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(retry_steps[0].status, OrderExecutionStepStatus::Planned);
}

#[tokio::test]
async fn order_executor_advances_async_direct_unit_route_after_detector_hints() {
    let fixture = funded_market_order_fixture("evm:1", "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 0);
    assert!(fixture
        .mocks
        .across_deposits_from(fixture.vault_address)
        .await
        .is_empty());

    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    assert_eq!(
        unit_deposit_operation.status,
        ProviderOperationStatus::WaitingExternal
    );
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );
    let unit_deposit_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(
        &fixture.mocks,
        &fixture.db,
        fixture.order_id,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        filter_unit_operations_by_kind(
            &fixture.mocks.ledger_snapshot().await.unit_operations,
            MockUnitOperationKind::Deposit
        )
        .len(),
        1
    );

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 1);
    let unit_withdrawal_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitWithdrawal,
    )
    .await;
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_withdrawal_operation))
        .await
        .unwrap();
    record_detector_provider_status_hint(&execution_manager, &unit_withdrawal_operation).await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
    assert_eq!(
        filter_unit_operations_by_kind(
            &fixture.mocks.ledger_snapshot().await.unit_operations,
            MockUnitOperationKind::Withdrawal
        )
        .len(),
        1
    );
}

#[tokio::test]
async fn unit_deposit_chain_hint_does_not_complete_before_provider_credit() {
    let fixture = funded_market_order_fixture("evm:1", "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 0);

    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    assert_eq!(
        unit_deposit_operation.status,
        ProviderOperationStatus::WaitingExternal
    );

    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );

    let refreshed = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    assert_eq!(refreshed.status, ProviderOperationStatus::WaitingExternal);
}

#[tokio::test]
async fn worker_poll_completes_unit_deposit_after_provider_credit() {
    let fixture = funded_market_order_fixture("evm:1", "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");

    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();

    record_detector_provider_status_hint(&execution_manager, &unit_deposit_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    let refreshed = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    assert_eq!(refreshed.status, ProviderOperationStatus::Completed);
}

#[tokio::test]
async fn order_executor_completes_mock_direct_unit_hyperliquid_flow() {
    let fixture = funded_market_order_fixture("evm:1", "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 0);

    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    assert_eq!(
        unit_deposit_operation.status,
        ProviderOperationStatus::WaitingExternal
    );
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );
    let unit_deposit_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(
        &fixture.mocks,
        &fixture.db,
        fixture.order_id,
        &unit_deposit_operation,
    )
    .await;

    let summary = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 1);

    let unit_withdrawal_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitWithdrawal,
    )
    .await;
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_withdrawal_operation))
        .await
        .unwrap();
    record_detector_provider_status_hint(&execution_manager, &unit_withdrawal_operation).await;
    assert_eq!(
        execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap(),
        1
    );

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
    assert!(fixture
        .mocks
        .across_deposits_from(fixture.vault_address)
        .await
        .is_empty());
    let gen_requests = fixture.mocks.unit_generate_address_requests().await;
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Deposit).len(),
        1
    );
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Withdrawal).len(),
        1
    );
    let ledger = fixture.mocks.ledger_snapshot().await;
    assert!(ledger.bridged_balances.is_empty());
    assert!(ledger.hyperliquid_balances.is_empty());
    let deposit_ops =
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Deposit);
    let withdrawal_ops =
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Withdrawal);
    assert_eq!(deposit_ops.len(), 1);
    assert_eq!(withdrawal_ops.len(), 1);

    let provider_operations = fixture
        .db
        .orders()
        .get_provider_operations(fixture.order_id)
        .await
        .unwrap();
    let unit_operation = provider_operations
        .iter()
        .find(|operation| operation.operation_type == ProviderOperationType::UnitDeposit)
        .expect("unit deposit provider operation should be persisted");
    assert_eq!(unit_operation.provider, "unit");
    assert_eq!(unit_operation.status, ProviderOperationStatus::Completed);

    let provider_addresses = fixture
        .db
        .orders()
        .get_provider_addresses(fixture.order_id)
        .await
        .unwrap();
    let unit_address = provider_addresses
        .iter()
        .find(|address| address.role == ProviderAddressRole::UnitDeposit)
        .expect("unit deposit provider address should be persisted");
    assert_eq!(unit_address.provider_operation_id, Some(unit_operation.id));
    assert_eq!(unit_address.provider, "unit");
    assert_eq!(unit_address.address, deposit_ops[0].protocol_address);
}

#[tokio::test]
async fn order_executor_validates_chain_deposit_hint_before_next_step() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let settings = Arc::new(test_settings(dir.path()));
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(ObservationUnitProvider)],
        vec![Arc::new(ProviderOnlyExchangeProvider)],
    ));
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
        action_providers.clone(),
    );
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        action_providers,
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();
    let summary = execution_manager
        .execute_materialized_order(order.id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 0);

    let order_after_submit = db.orders().get(order.id).await.unwrap();
    let vault_after_submit = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order_after_submit.status, RouterOrderStatus::Executing);
    assert_eq!(vault_after_submit.status, DepositVaultStatus::Executing);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .unwrap();
    let withdrawal_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitWithdrawal)
        .unwrap();
    assert_eq!(unit_step.status, OrderExecutionStepStatus::Waiting);
    assert_eq!(withdrawal_step.status, OrderExecutionStepStatus::Planned);

    let provider_operations = db.orders().get_provider_operations(order.id).await.unwrap();
    let unit_operation = provider_operations
        .iter()
        .find(|operation| operation.operation_type == ProviderOperationType::UnitDeposit)
        .unwrap();
    assert_eq!(unit_operation.status, ProviderOperationStatus::Submitted);
    let provider_ref = unit_operation.provider_ref.clone().unwrap();

    let fake_tx_hash = format!("0x{}", "33".repeat(32));
    let fake_hint = execution_manager
        .record_provider_operation_hint(OrderProviderOperationHint {
            id: Uuid::now_v7(),
            provider_operation_id: unit_operation.id,
            source: "test-chain-detector".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({
                "source": "test-chain-detector",
                "backend": "ethereum_evm",
                "chain": "ethereum",
                "address": "0x2000000000000000000000000000000000000001",
                "tx_hash": fake_tx_hash,
                "transfer_index": 0,
                "amount": "1000",
                "observed_at": Utc::now()
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some(format!("test-chain-detector-fake-hint-{provider_ref}")),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    assert_eq!(fake_hint.status, ProviderOperationHintStatus::Pending);

    let processed_hints = execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    assert_eq!(processed_hints, 1);
    let operation_after_fake_hint = db
        .orders()
        .get_provider_operation(unit_operation.id)
        .await
        .unwrap();
    assert_eq!(
        operation_after_fake_hint.status,
        ProviderOperationStatus::Submitted
    );
    let unit_step_after_fake_hint = db.orders().get_execution_step(unit_step.id).await.unwrap();
    assert_eq!(
        unit_step_after_fake_hint.status,
        OrderExecutionStepStatus::Waiting
    );

    let deposit_address = Address::from_str("0x2000000000000000000000000000000000000001").unwrap();
    let (observed_tx_hash, transfer_index) =
        anvil_send_ethereum_native(h, deposit_address, U256::from(1000_u64)).await;
    let hint = execution_manager
        .record_provider_operation_hint(OrderProviderOperationHint {
            id: Uuid::now_v7(),
            provider_operation_id: unit_operation.id,
            source: "test-chain-detector".to_string(),
            hint_kind: ProviderOperationHintKind::PossibleProgress,
            evidence: json!({
                "source": "test-chain-detector",
                "backend": "ethereum_evm",
                "chain": "ethereum",
                "address": "0x2000000000000000000000000000000000000001",
                "tx_hash": observed_tx_hash.clone(),
                "transfer_index": transfer_index,
                "amount": "1000",
                "observed_at": Utc::now()
            }),
            status: ProviderOperationHintStatus::Pending,
            idempotency_key: Some(format!("test-chain-detector-hint-{provider_ref}")),
            error: json!({}),
            claimed_at: None,
            processed_at: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    assert_eq!(hint.status, ProviderOperationHintStatus::Pending);

    let operation_after_hint_insert = db
        .orders()
        .get_provider_operation(unit_operation.id)
        .await
        .unwrap();
    assert_eq!(
        operation_after_hint_insert.status,
        ProviderOperationStatus::Submitted
    );
    let unit_step_after_hint_insert = db.orders().get_execution_step(unit_step.id).await.unwrap();
    assert_eq!(
        unit_step_after_hint_insert.status,
        OrderExecutionStepStatus::Waiting
    );

    let processed_hints = execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    assert_eq!(processed_hints, 1);
    let operation_after_hint_processing = db
        .orders()
        .get_provider_operation(unit_operation.id)
        .await
        .unwrap();
    assert_eq!(
        operation_after_hint_processing.status,
        ProviderOperationStatus::Completed
    );
    let unit_step_after_hint_processing =
        db.orders().get_execution_step(unit_step.id).await.unwrap();
    assert_eq!(
        unit_step_after_hint_processing.status,
        OrderExecutionStepStatus::Completed
    );

    let summary = execution_manager
        .execute_materialized_order(order.id)
        .await
        .unwrap()
        .expect("execute_materialized_order should report progress in single-worker tests");
    assert_eq!(summary.completed_steps, 2);
    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
}

#[tokio::test]
async fn order_executor_processes_provider_operation_status_hints() {
    let fixture = funded_market_order_fixture("evm:1", "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    // Across lives on a separate verifier path (chain-indexer deposit status,
    // exercised by `drive_across_order_to_completion` tests); this test covers
    // the generic provider-status hint path used by Unit.
    let cases = [(
        OrderExecutionStepType::UnitWithdrawal,
        ProviderOperationType::UnitWithdrawal,
        "unit",
    )];

    for (index, (step_type, operation_type, provider)) in cases.into_iter().enumerate() {
        let now = Utc::now();
        let provider_ref = format!("{provider}-status-op-{index}");
        let execution_attempt = OrderExecutionAttempt {
            id: Uuid::now_v7(),
            order_id: fixture.order_id,
            attempt_index: 100 + index as i32,
            attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
            status: OrderExecutionAttemptStatus::Active,
            trigger_step_id: None,
            trigger_provider_operation_id: None,
            failure_reason: json!({}),
            input_custody_snapshot: json!({}),
            created_at: now,
            updated_at: now,
        };
        fixture
            .db
            .orders()
            .create_execution_attempt(&execution_attempt)
            .await
            .unwrap();
        let step = OrderExecutionStep {
            id: Uuid::now_v7(),
            order_id: fixture.order_id,
            execution_attempt_id: Some(execution_attempt.id),
            transition_decl_id: None,
            step_index: 20 + index as i32,
            step_type,
            provider: provider.to_string(),
            status: OrderExecutionStepStatus::Waiting,
            input_asset: None,
            output_asset: None,
            amount_in: Some("1000".to_string()),
            min_amount_out: Some("1000".to_string()),
            tx_hash: None,
            provider_ref: Some(provider_ref.clone()),
            idempotency_key: Some(format!("order:{}:{provider}:status-test", fixture.order_id)),
            attempt_count: 0,
            next_attempt_at: None,
            started_at: Some(now),
            completed_at: None,
            details: json!({ "source": "provider_status_hint_test" }),
            request: json!({
                "endpoint": "/status-test",
                "provider": provider,
                "operation_type": operation_type.to_db_string()
            }),
            response: json!({}),
            error: json!({}),
            created_at: now,
            updated_at: now,
        };
        fixture
            .db
            .orders()
            .create_execution_step(&step)
            .await
            .unwrap();
        let operation = OrderProviderOperation {
            id: Uuid::now_v7(),
            order_id: fixture.order_id,
            execution_attempt_id: Some(execution_attempt.id),
            execution_step_id: Some(step.id),
            provider: provider.to_string(),
            operation_type,
            provider_ref: Some(provider_ref.clone()),
            status: ProviderOperationStatus::WaitingExternal,
            request: step.request.clone(),
            response: json!({ "provider_ref": provider_ref }),
            observed_state: json!({}),
            created_at: now,
            updated_at: now,
        };
        fixture
            .db
            .orders()
            .create_provider_operation(&operation)
            .await
            .unwrap();
        let mock_kind = match operation_type {
            ProviderOperationType::UnitDeposit => MockUnitOperationKind::Deposit,
            ProviderOperationType::UnitWithdrawal => MockUnitOperationKind::Withdrawal,
            _ => panic!("status-hint test only covers Unit provider operations"),
        };
        fixture
            .mocks
            .seed_completed_unit_operation(&provider_ref, mock_kind)
            .await;
        execution_manager
            .record_provider_operation_hint(OrderProviderOperationHint {
                id: Uuid::now_v7(),
                provider_operation_id: operation.id,
                source: "test-detector".to_string(),
                hint_kind: ProviderOperationHintKind::PossibleProgress,
                evidence: json!({
                    "source": "test-detector",
                    "backend": "provider_operation",
                    "provider": provider,
                    "operation_type": operation_type.to_db_string(),
                    "provider_ref": provider_ref,
                    "observed_status": "completed",
                    "observed_at": Utc::now()
                }),
                status: ProviderOperationHintStatus::Pending,
                idempotency_key: Some(format!("test-provider-status-hint-{provider}-{index}")),
                error: json!({}),
                claimed_at: None,
                processed_at: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .await
            .unwrap();

        let processed_hints = execution_manager
            .process_provider_operation_hints(10)
            .await
            .unwrap();
        assert_eq!(processed_hints, 1);
        let operation_after_hint = fixture
            .db
            .orders()
            .get_provider_operation(operation.id)
            .await
            .unwrap();
        assert_eq!(
            operation_after_hint.status,
            ProviderOperationStatus::Completed
        );
        let step_after_hint = fixture
            .db
            .orders()
            .get_execution_step(step.id)
            .await
            .unwrap();
        assert_eq!(step_after_hint.status, OrderExecutionStepStatus::Completed);
    }
}

#[tokio::test]
async fn order_executor_retries_when_across_fails() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings, h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            Arc::new(test_settings(dir.path())),
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();
    mocks
        .fail_next_across_swap_approval("mock across unavailable")
        .await;
    let err = execution_manager
        .execute_materialized_order(order.id)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("mock across unavailable"));

    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);
    let attempts = db.orders().get_execution_attempts(order.id).await.unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Active);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RetryExecution
    );
    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let across_step = steps
        .iter()
        .find(|step| {
            step.step_type == OrderExecutionStepType::AcrossBridge
                && step.execution_attempt_id == Some(attempts[0].id)
        })
        .unwrap();
    assert_eq!(across_step.status, OrderExecutionStepStatus::Failed);
    assert!(across_step
        .error
        .to_string()
        .contains("mock across unavailable"));
    let retry_steps = db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .unwrap();
    assert_eq!(retry_steps.len(), 3);
    assert_eq!(
        retry_steps[0].step_type,
        OrderExecutionStepType::AcrossBridge
    );
    assert_eq!(retry_steps[0].status, OrderExecutionStepStatus::Planned);
}

#[tokio::test]
async fn order_executor_marks_order_refund_required_when_unit_deposit_fails() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    drive_unit_deposit_failures_to_refund_required(&fixture, "mock unit deposit unavailable").await;
}

#[tokio::test]
async fn order_executor_marks_order_refund_required_when_unit_withdrawal_fails_twice() {
    let fixture =
        funded_market_order_fixture_with_amount("evm:8453", "evm:1", "1000000000000000").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("across leg should start");
    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&execution_manager, &across_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("unit deposit should start");
    let unit_deposit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_deposit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(
        &execution_manager,
        &fixture.db,
        &unit_deposit_operation,
    )
    .await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    let unit_deposit_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_deposit_operation.id)
        .await
        .unwrap();
    credit_hyperliquid_spot_from_unit_deposit(
        &fixture.mocks,
        &fixture.db,
        fixture.order_id,
        &unit_deposit_operation,
    )
    .await;

    fixture
        .mocks
        .fail_next_hyperliquid_exchange("mock unit withdrawal unavailable")
        .await;
    let first_err = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap_err();
    assert!(first_err
        .to_string()
        .contains("mock unit withdrawal unavailable"));

    fixture
        .mocks
        .fail_next_hyperliquid_exchange("mock unit withdrawal unavailable")
        .await;
    let second_err = execution_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap_err();
    assert!(second_err
        .to_string()
        .contains("mock unit withdrawal unavailable"));

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::RefundRequired);
    assert_eq!(vault.status, DepositVaultStatus::RefundRequired);
    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
    assert_eq!(
        attempts[2].status,
        OrderExecutionAttemptStatus::RefundRequired
    );
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let failed_withdrawals: Vec<_> = steps
        .iter()
        .filter(|step| {
            step.step_type == OrderExecutionStepType::UnitWithdrawal
                && step.status == OrderExecutionStepStatus::Failed
        })
        .collect();
    assert_eq!(failed_withdrawals.len(), 2);
    assert!(failed_withdrawals.iter().all(|step| {
        step.error
            .to_string()
            .contains("mock unit withdrawal unavailable")
    }));
}

#[tokio::test]
async fn refund_required_funding_vault_refund_completes_and_finalizes_order() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let vault_manager = VaultManager::new(
        fixture.db.clone(),
        fixture.settings.clone(),
        fixture.chain_registry.clone(),
    );
    let now = Utc::now();
    let refund_attempt = OrderExecutionAttempt {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        attempt_index: 1,
        attempt_kind: OrderExecutionAttemptKind::RefundRecovery,
        status: OrderExecutionAttemptStatus::RefundRequired,
        trigger_step_id: None,
        trigger_provider_operation_id: None,
        failure_reason: json!({
            "reason": "test_direct_funding_vault_refund"
        }),
        input_custody_snapshot: json!({
            "source_kind": "funding_vault",
            "vault_id": fixture.vault_id,
        }),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_attempt(&refund_attempt)
        .await
        .unwrap();
    let initial_order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            initial_order.status,
            RouterOrderStatus::RefundRequired,
            now,
        )
        .await
        .unwrap();
    fixture
        .db
        .vaults()
        .transition_status(
            fixture.vault_id,
            DepositVaultStatus::Funded,
            DepositVaultStatus::RefundRequired,
            now,
        )
        .await
        .unwrap();

    execution_manager.process_worker_pass(10).await.unwrap();

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    let refund_attempt = fixture
        .db
        .orders()
        .get_execution_attempt(refund_attempt.id)
        .await
        .unwrap();
    assert_eq!(order.status, RouterOrderStatus::Refunding);
    assert_eq!(vault.status, DepositVaultStatus::Refunding);
    assert_eq!(refund_attempt.status, OrderExecutionAttemptStatus::Active);
    let refund_steps = fixture
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt.id)
        .await
        .unwrap();
    assert!(
        refund_steps.is_empty(),
        "direct funding-vault refunds should not materialize execution steps"
    );

    vault_manager.process_refund_pass().await;
    execution_manager.process_worker_pass(10).await.unwrap();

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    let refund_attempt = attempts
        .iter()
        .find(|attempt| attempt.id == refund_attempt.id)
        .unwrap();
    assert_eq!(order.status, RouterOrderStatus::Refunded);
    assert_eq!(vault.status, DepositVaultStatus::Refunded);
    assert!(vault.refund_tx_hash.is_some());
    assert_eq!(
        refund_attempt.status,
        OrderExecutionAttemptStatus::Completed
    );
}

#[tokio::test]
async fn refund_required_order_materializes_and_completes_automatic_refund_route() {
    let fixture =
        funded_market_order_fixture_with_amount("evm:8453", "evm:1", "1000000000000000000").await;
    let _forward_execution_manager =
        drive_unit_deposit_failures_to_refund_required(&fixture, "mock unit deposit unavailable")
            .await;
    zero_source_deposit_vault_balance(&fixture.db, fixture.order_id, "evm:8453").await;
    let h = harness().await;
    let refund_mocks = spawn_harness_mocks(h, "evm:1").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(refund_mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );

    execution_manager.process_worker_pass(10).await.unwrap();

    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    let refund_attempt = attempts
        .iter()
        .find(|attempt| attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery)
        .unwrap();
    assert_eq!(
        refund_attempt.status,
        OrderExecutionAttemptStatus::Active,
        "refund attempt failure_reason={} snapshot={}",
        refund_attempt.failure_reason,
        refund_attempt.input_custody_snapshot
    );

    let refund_steps = fixture
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt.id)
        .await
        .unwrap();
    assert_eq!(refund_steps.len(), 1);
    assert_eq!(
        refund_steps[0].step_type,
        OrderExecutionStepType::AcrossBridge
    );

    let refund_operation = fixture
        .db
        .orders()
        .get_provider_operations(fixture.order_id)
        .await
        .unwrap()
        .into_iter()
        .filter(|operation| {
            operation.execution_attempt_id == Some(refund_attempt.id)
                && operation.operation_type == ProviderOperationType::AcrossBridge
        })
        .max_by_key(|operation| operation.created_at)
        .expect("refund attempt should submit an across bridge operation");
    let depositor = refund_steps[0]
        .request
        .get("depositor_address")
        .and_then(Value::as_str)
        .expect("refund across request should include depositor_address");
    let depositor = Address::from_str(depositor).expect("refund depositor address is valid");

    wait_for_across_deposit_indexed(&refund_mocks, depositor, Duration::from_secs(10)).await;
    record_detector_provider_status_hint(&execution_manager, &refund_operation).await;
    execution_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();

    let start = std::time::Instant::now();
    loop {
        let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
        if order.status == RouterOrderStatus::Refunded {
            let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
            assert_eq!(vault.status, DepositVaultStatus::Refunded);
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "refund route did not complete within 10s"
        );
        execution_manager.process_worker_pass(10).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    let refund_attempt = attempts
        .iter()
        .find(|attempt| attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery)
        .unwrap();
    assert_eq!(
        refund_attempt.status,
        OrderExecutionAttemptStatus::Completed
    );
}

#[tokio::test]
async fn failed_refund_attempt_requires_manual_intervention_instead_of_recursing() {
    let fixture =
        funded_market_order_fixture_with_amount("evm:8453", "evm:1", "1000000000000000000").await;
    let _forward_execution_manager =
        drive_unit_deposit_failures_to_refund_required(&fixture, "mock unit deposit unavailable")
            .await;
    zero_source_deposit_vault_balance(&fixture.db, fixture.order_id, "evm:8453").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::new(
            vec![Arc::new(FailingRefundBridgeProvider)],
            vec![],
            vec![],
        )),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );

    let start = std::time::Instant::now();
    loop {
        execution_manager.process_worker_pass(10).await.unwrap();
        let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
        let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
        if order.status == RouterOrderStatus::RefundManualInterventionRequired
            && vault.status == DepositVaultStatus::RefundManualInterventionRequired
        {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "refund attempt did not escalate to manual intervention within 10s"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 3);
    let refund_attempt = attempts
        .iter()
        .find(|attempt| attempt.attempt_kind == OrderExecutionAttemptKind::RefundRecovery)
        .unwrap();
    assert_eq!(
        refund_attempt.status,
        OrderExecutionAttemptStatus::ManualInterventionRequired
    );

    let refund_steps = fixture
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt.id)
        .await
        .unwrap();
    assert_eq!(refund_steps.len(), 1);
    assert_eq!(
        refund_steps[0].step_type,
        OrderExecutionStepType::AcrossBridge
    );
    assert_eq!(refund_steps[0].status, OrderExecutionStepStatus::Failed);
    assert!(refund_steps[0]
        .error
        .to_string()
        .contains("mock refund route unavailable"));
}

#[tokio::test]
async fn worker_pass_releases_terminal_internal_custody_vaults_backstop() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    execution_manager.process_planning_pass(10).await.unwrap();

    let internal_vaults_before =
        internal_custody_vaults_for_order(&fixture.db, fixture.order_id).await;
    assert!(
        !internal_vaults_before.is_empty(),
        "planning pass should hydrate internal custody vaults"
    );
    assert!(internal_vaults_before
        .iter()
        .all(|vault| vault.status == CustodyVaultStatus::Active));

    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            RouterOrderStatus::Funded,
            RouterOrderStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            RouterOrderStatus::Executing,
            RouterOrderStatus::Completed,
            Utc::now(),
        )
        .await
        .unwrap();

    execution_manager.process_worker_pass(10).await.unwrap();

    let internal_vaults_after =
        internal_custody_vaults_for_order(&fixture.db, fixture.order_id).await;
    assert!(internal_vaults_after.iter().all(|vault| {
        vault.status == CustodyVaultStatus::Released
            && vault.metadata["lifecycle_terminal_reason"] == json!("order_completed")
            && vault.metadata["lifecycle_order_status"] == json!("completed")
            && vault.metadata["lifecycle_balance_verified"] == json!(false)
    }));
}

// ---------------------------------------------------------------------------
// Vault creation tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_vault_evm_native() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let vault = vm.create_vault(evm_native_request()).await.unwrap();

    assert_eq!(vault.status, DepositVaultStatus::PendingFunding);
    assert_eq!(vault.deposit_asset.chain, ChainId::parse("evm:1").unwrap());
    assert!(vault.deposit_asset.asset.is_native());
    assert_eq!(vault.recovery_address, valid_evm_address());
    assert!(!vault.deposit_vault_address.is_empty());
    assert!(vault.refund_requested_at.is_none());
    assert!(vault.refunded_at.is_none());
}

#[tokio::test]
async fn create_vault_base_native() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:8453").unwrap(),
            asset: AssetId::Native,
        },
        recovery_address: valid_evm_address(),
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(
        vault.deposit_asset.chain,
        ChainId::parse("evm:8453").unwrap()
    );
    assert!(vault.deposit_asset.asset.is_native());
}

#[tokio::test]
async fn create_vault_evm_erc20_token() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let token_address = "0x00000000000000000000000000000000000000A0";
    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Reference(token_address.to_string()),
        },
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(
        vault.deposit_asset.asset,
        AssetId::Reference(token_address.to_lowercase())
    );
}

#[tokio::test]
async fn create_vault_bitcoin_native() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        },
        recovery_address: valid_regtest_btc_address(),
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(
        vault.deposit_asset.chain,
        ChainId::parse("bitcoin").unwrap()
    );
    assert!(vault.deposit_asset.asset.is_native());
    assert_eq!(vault.recovery_address, valid_regtest_btc_address());
}

#[tokio::test]
async fn create_vault_unsupported_chain_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:10").unwrap(),
            asset: AssetId::Native,
        },
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("not supported"));
}

#[tokio::test]
async fn create_vault_bitcoin_rejects_erc20_asset() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Reference("0x0000000000000000000000000000000000000001".to_string()),
        },
        recovery_address: valid_regtest_btc_address(),
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("native asset"));
}

#[tokio::test]
async fn create_vault_invalid_recovery_address_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        recovery_address: "not-an-address".to_string(),
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("recovery address"));
}

#[tokio::test]
async fn create_vault_invalid_cancellation_commitment_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        cancellation_commitment: "0xdead".to_string(),
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("cancellation commitment"));
}

#[tokio::test]
async fn create_vault_cancel_after_in_past_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        cancel_after: Some(Utc::now() - chrono::Duration::hours(1)),
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("future"));
}

#[tokio::test]
async fn create_vault_non_object_metadata_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    for bad_metadata in [json!([]), json!("str"), json!(42), json!(null), json!(true)] {
        let request = CreateVaultRequest {
            metadata: bad_metadata,
            ..evm_native_request()
        };
        let err = vm.create_vault(request).await.unwrap_err();
        assert!(
            err.to_string().contains("metadata"),
            "expected metadata error"
        );
    }
}

#[tokio::test]
async fn create_then_get_vault_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let created = vm.create_vault(evm_native_request()).await.unwrap();
    let fetched = vm.get_vault(created.id).await.unwrap();

    assert_eq!(created.id, fetched.id);
    assert_eq!(created.deposit_asset, fetched.deposit_asset);
    assert_eq!(created.deposit_vault_address, fetched.deposit_vault_address);
    assert_eq!(created.recovery_address, fetched.recovery_address);
    assert_eq!(
        created.cancellation_commitment,
        fetched.cancellation_commitment
    );
    assert_eq!(created.status, fetched.status);
}

#[tokio::test]
async fn create_vault_generates_unique_addresses() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let v1 = vm.create_vault(evm_native_request()).await.unwrap();
    let v2 = vm.create_vault(evm_native_request()).await.unwrap();

    assert_ne!(v1.id, v2.id);
    assert_ne!(v1.deposit_vault_address, v2.deposit_vault_address);
    assert_ne!(v1.deposit_vault_salt, v2.deposit_vault_salt);
}

#[tokio::test]
async fn create_vault_default_cancel_after_is_future() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let before = Utc::now();
    let vault = vm.create_vault(evm_native_request()).await.unwrap();
    let after = Utc::now();

    assert!(vault.cancel_after > before);
    let expected_min = before + chrono::Duration::hours(23);
    let expected_max = after + chrono::Duration::hours(25);
    assert!(vault.cancel_after > expected_min);
    assert!(vault.cancel_after < expected_max);
}

#[tokio::test]
async fn create_vault_preserves_custom_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let metadata = json!({"order_id": "abc-123", "user": "test"});
    let request = CreateVaultRequest {
        metadata: metadata.clone(),
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(vault.metadata, metadata);

    let fetched = vm.get_vault(vault.id).await.unwrap();
    assert_eq!(fetched.metadata, metadata);
}

#[tokio::test]
async fn create_vault_normalizes_evm_token_address_to_lowercase() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let mixed_case = "0x00000000000000000000000000000000000000Ab";
    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Reference(mixed_case.to_string()),
        },
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(
        vault.deposit_asset.asset,
        AssetId::Reference(mixed_case.to_lowercase())
    );
}

#[tokio::test]
async fn create_vault_normalizes_evm_recovery_address_to_lowercase() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let mixed_case = "0x0000000000000000000000000000000000000001";
    let request = CreateVaultRequest {
        recovery_address: mixed_case.to_uppercase().replacen("0X", "0x", 1),
        ..evm_native_request()
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(vault.recovery_address, mixed_case.to_lowercase());
}

#[tokio::test]
async fn create_vault_invalid_evm_token_address_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let request = CreateVaultRequest {
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Reference("not-a-token-address".to_string()),
        },
        ..evm_native_request()
    };

    let err = vm.create_vault(request).await.unwrap_err();
    assert!(err.to_string().contains("asset id"));
}

#[tokio::test]
async fn cancel_vault_rejects_executing_status() {
    let dir = tempfile::tempdir().unwrap();
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let (secret_hex, commitment_hex) = make_cancellation_pair();

    let request = CreateVaultRequest {
        cancellation_commitment: commitment_hex,
        ..evm_native_request()
    };
    let vault = vm.create_vault(request).await.unwrap();
    let executing = db
        .vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    assert_eq!(executing.status, DepositVaultStatus::Executing);

    let err = vm.cancel_vault(vault.id, &secret_hex).await.unwrap_err();
    assert!(err.to_string().contains("executing"));

    let fetched = vm.get_vault(vault.id).await.unwrap();
    assert_eq!(fetched.status, DepositVaultStatus::Executing);
    assert!(fetched.refund_requested_at.is_none());
}

#[tokio::test]
async fn refund_claim_lease_blocks_duplicate_workers() {
    let dir = tempfile::tempdir().unwrap();
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let (_, commitment_hex) = make_cancellation_pair();

    let request = CreateVaultRequest {
        cancellation_commitment: commitment_hex,
        ..evm_native_request()
    };
    let vault = vm.create_vault(request).await.unwrap();
    let now = Utc::now();
    let refunding = db.vaults().request_refund(vault.id, now).await.unwrap();
    assert_eq!(refunding.status, DepositVaultStatus::Refunding);

    let first_claim = db
        .vaults()
        .claim_refund(
            vault.id,
            now,
            now + chrono::Duration::minutes(5),
            "worker-a",
        )
        .await
        .unwrap();
    assert!(first_claim.is_some());

    let blocked_claim = db
        .vaults()
        .claim_refund(
            vault.id,
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::minutes(5),
            "worker-b",
        )
        .await
        .unwrap();
    assert!(blocked_claim.is_none());

    let expired_claim = db
        .vaults()
        .claim_refund(
            vault.id,
            now + chrono::Duration::minutes(6),
            now + chrono::Duration::minutes(11),
            "worker-b",
        )
        .await
        .unwrap();
    assert!(expired_claim.is_some());
}

#[tokio::test]
async fn refunding_retry_claims_are_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let (vm, db) = test_vault_manager_with_db(dir.path()).await;
    let (_, commitment_hex) = make_cancellation_pair();

    let request = CreateVaultRequest {
        cancellation_commitment: commitment_hex,
        ..evm_native_request()
    };
    let vault = vm.create_vault(request).await.unwrap();
    let now = Utc::now();
    db.vaults().request_refund(vault.id, now).await.unwrap();

    let first_batch = db
        .vaults()
        .claim_refunding(now, now + chrono::Duration::minutes(5), "worker-a", 10)
        .await
        .unwrap();
    assert_eq!(first_batch.len(), 1);
    assert_eq!(first_batch[0].id, vault.id);
    assert_eq!(first_batch[0].status, DepositVaultStatus::Refunding);

    let second_batch = db
        .vaults()
        .claim_refunding(
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::minutes(5),
            "worker-b",
            10,
        )
        .await
        .unwrap();
    assert!(second_batch.is_empty());
}

#[tokio::test]
async fn worker_lease_allows_one_active_worker_until_expiry() {
    let db = test_db().await;
    let now = Utc::now();

    let first = db
        .worker_leases()
        .try_acquire(
            "global-router-worker",
            "worker-a",
            now,
            now + chrono::Duration::seconds(30),
        )
        .await
        .unwrap()
        .expect("first worker should acquire lease");
    assert_eq!(first.owner_id, "worker-a");
    assert_eq!(first.fencing_token, 1);

    let blocked = db
        .worker_leases()
        .try_acquire(
            "global-router-worker",
            "worker-b",
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(31),
        )
        .await
        .unwrap();
    assert!(blocked.is_none());

    let duplicate_same_owner = db
        .worker_leases()
        .try_acquire(
            "global-router-worker",
            "worker-a",
            now + chrono::Duration::seconds(1),
            now + chrono::Duration::seconds(31),
        )
        .await
        .unwrap();
    assert!(duplicate_same_owner.is_none());

    let renewed = db
        .worker_leases()
        .renew(
            "global-router-worker",
            "worker-a",
            first.fencing_token,
            now + chrono::Duration::seconds(2),
            now + chrono::Duration::seconds(32),
        )
        .await
        .unwrap()
        .expect("active worker should renew lease");
    assert_eq!(renewed.owner_id, "worker-a");
    assert_eq!(renewed.fencing_token, 1);

    let takeover = db
        .worker_leases()
        .try_acquire(
            "global-router-worker",
            "worker-b",
            now + chrono::Duration::seconds(33),
            now + chrono::Duration::seconds(63),
        )
        .await
        .unwrap()
        .expect("standby should acquire after lease expiry");
    assert_eq!(takeover.owner_id, "worker-b");
    assert_eq!(takeover.fencing_token, 2);

    let stale_renew = db
        .worker_leases()
        .renew(
            "global-router-worker",
            "worker-a",
            first.fencing_token,
            now + chrono::Duration::seconds(34),
            now + chrono::Duration::seconds(64),
        )
        .await
        .unwrap();
    assert!(stale_renew.is_none());
}

// ---------------------------------------------------------------------------
// EVM native (ETH) refund test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_vault_requests_worker_refund_for_evm_native_eth() {
    let h = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let (secret_hex, commitment_hex) = make_cancellation_pair();
    let recovery_address = h.ethereum_funded_address();

    let request = CreateVaultRequest {
        order_id: None,
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Native,
        },
        action: VaultAction::Null,
        recovery_address: format!("{recovery_address:?}"),
        cancellation_commitment: commitment_hex,
        cancel_after: None,
        metadata: json!({}),
    };

    let vault = vm.create_vault(request).await.unwrap();
    assert_eq!(vault.status, DepositVaultStatus::PendingFunding);

    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");
    let deposit_amount = U256::from(1_000_000_000_000_000_000u128); // 1 ETH
    anvil_set_balance(h, vault_address, deposit_amount).await;

    let requested = vm.cancel_vault(vault.id, &secret_hex).await.unwrap();
    assert_eq!(requested.status, DepositVaultStatus::Refunding);
    assert!(requested.refund_requested_at.is_some());

    vm.process_refund_pass().await;
    let refunded = vm.get_vault(vault.id).await.unwrap();

    assert_eq!(
        refunded.status,
        DepositVaultStatus::Refunded,
        "last refund error: {:?}",
        refunded.last_refund_error
    );
    assert!(refunded.refund_tx_hash.is_some());
    assert!(refunded.refunded_at.is_some());
}

// ---------------------------------------------------------------------------
// EVM ERC-20 refund test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_vault_requests_worker_refund_for_evm_erc20() {
    let h = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let (secret_hex, commitment_hex) = make_cancellation_pair();
    let recovery_address = h.ethereum_funded_address();

    let request = CreateVaultRequest {
        order_id: None,
        deposit_asset: DepositAsset {
            chain: ChainId::parse("evm:1").unwrap(),
            asset: AssetId::Reference(MOCK_ERC20_ADDRESS.to_lowercase()),
        },
        action: VaultAction::Null,
        recovery_address: format!("{recovery_address:?}"),
        cancellation_commitment: commitment_hex,
        cancel_after: None,
        metadata: json!({}),
    };

    let vault = vm.create_vault(request).await.unwrap();

    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid vault evm address");

    let mint_amount = U256::from(1_000_000_000u128); // 1000 tokens (assuming 6 decimals)
    anvil_mint_erc20(h, vault_address, mint_amount).await;

    let ethereum_chain = h.chain_registry.get_evm(&ChainType::Ethereum).unwrap();
    assert_eq!(
        ethereum_chain
            .native_balance(&vault.deposit_vault_address)
            .await
            .unwrap(),
        U256::ZERO
    );

    let requested = vm.cancel_vault(vault.id, &secret_hex).await.unwrap();
    assert_eq!(requested.status, DepositVaultStatus::Refunding);
    assert!(requested.refund_requested_at.is_some());

    vm.process_refund_pass().await;
    let refunded = vm.get_vault(vault.id).await.unwrap();

    assert_eq!(
        refunded.status,
        DepositVaultStatus::Refunded,
        "last refund error: {:?}",
        refunded.last_refund_error
    );
    assert!(refunded.refund_tx_hash.is_some());
    assert_eq!(
        ethereum_chain
            .erc20_balance(MOCK_ERC20_ADDRESS, &vault.deposit_vault_address)
            .await
            .unwrap(),
        U256::ZERO
    );
    let vault_dust = ethereum_chain
        .native_balance(&vault.deposit_vault_address)
        .await
        .unwrap();
    assert!(vault_dust > U256::ZERO);
    let max_reasonable_dust = U256::from(10_000_000_000_000_000u64); // 0.01 ETH
    assert!(
        vault_dust < max_reasonable_dust,
        "vault dust {vault_dust} exceeded {max_reasonable_dust}"
    );
}

// ---------------------------------------------------------------------------
// Bitcoin refund test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cancel_vault_requests_worker_refund_for_bitcoin() {
    let h = harness().await;
    let dir = tempfile::tempdir().unwrap();
    let vm = test_vault_manager(dir.path()).await;

    let (secret_hex, commitment_hex) = make_cancellation_pair();

    let request = CreateVaultRequest {
        order_id: None,
        deposit_asset: DepositAsset {
            chain: ChainId::parse("bitcoin").unwrap(),
            asset: AssetId::Native,
        },
        action: VaultAction::Null,
        recovery_address: valid_regtest_btc_address(),
        cancellation_commitment: commitment_hex,
        cancel_after: None,
        metadata: json!({}),
    };

    let vault = vm.create_vault(request).await.unwrap();

    let vault_btc_address = bitcoin::Address::from_str(&vault.deposit_vault_address)
        .expect("valid vault btc address")
        .assume_checked();

    h.deal_bitcoin(&vault_btc_address, &Amount::from_sat(100_000))
        .await
        .expect("fund vault with btc");

    h.wait_for_esplora_sync(Duration::from_secs(30))
        .await
        .expect("esplora sync");

    let requested = vm.cancel_vault(vault.id, &secret_hex).await.unwrap();
    assert_eq!(requested.status, DepositVaultStatus::Refunding);
    assert!(requested.refund_requested_at.is_some());

    vm.process_refund_pass().await;
    let refunded = vm.get_vault(vault.id).await.unwrap();

    assert_eq!(refunded.status, DepositVaultStatus::Refunded);
    assert!(refunded.refund_tx_hash.is_some());
    assert!(refunded.refunded_at.is_some());
}

#[tokio::test]
async fn two_concurrent_worker_passes_drive_same_order_to_completion_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = spawn_harness_mocks(h, "evm:8453").await;
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    let vault_address =
        Address::from_str(&vault.deposit_vault_address).expect("valid base vault address");
    anvil_set_base_balance(h, vault_address, U256::from(1000_u64)).await;
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = Arc::new(OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    ));

    let worker_a = execution_manager.clone();
    let worker_b = execution_manager.clone();
    let (result_a, result_b) = tokio::join!(
        worker_a.process_worker_pass(10),
        worker_b.process_worker_pass(10)
    );
    let summary_a = result_a.expect("worker A pass should not return a fatal error");
    let summary_b = result_b.expect("worker B pass should not return a fatal error");

    assert_eq!(
        summary_a.executed_orders + summary_b.executed_orders,
        1,
        "exactly one worker should drive the order, got a={} b={}",
        summary_a.executed_orders,
        summary_b.executed_orders,
    );

    drive_across_order_to_completion(&execution_manager, &mocks, &db, order.id).await;

    let order = db.orders().get(order.id).await.unwrap();
    let vault = db.vaults().get(vault.id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);

    assert_eq!(mocks.across_deposits_from(vault_address).await.len(), 1);
    let gen_requests = mocks.unit_generate_address_requests().await;
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Deposit).len(),
        1
    );
    assert_eq!(
        filter_unit_requests_by_kind(&gen_requests, MockUnitOperationKind::Withdrawal).len(),
        1
    );
    let ledger = mocks.ledger_snapshot().await;
    assert_eq!(
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Deposit)
            .len(),
        1
    );
    assert_eq!(
        filter_unit_operations_by_kind(&ledger.unit_operations, MockUnitOperationKind::Withdrawal)
            .len(),
        1
    );

    let destination_vaults = db
        .orders()
        .get_custody_vaults(order.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .count();
    assert_eq!(destination_vaults, 1);

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    assert!(steps
        .iter()
        .filter(|step| step.step_index > 0)
        .all(|step| step.status == OrderExecutionStepStatus::Completed));
}

#[tokio::test]
async fn two_concurrent_planning_passes_produce_single_destination_execution_vault() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let (order_manager, _mocks) = mock_order_manager(db.clone(), h.chain_registry.clone()).await;
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:1").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let order = create_test_order_from_quote(
        &order_manager,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
    )
    .await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            ..evm_native_request()
        })
        .await
        .unwrap();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            Utc::now(),
        )
        .await
        .unwrap();

    let execution_manager = Arc::new(OrderExecutionManager::with_dependencies(
        db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(mocks.base_url())),
        Arc::new(CustodyActionExecutor::new(
            db.clone(),
            settings,
            h.chain_registry.clone(),
        )),
        h.chain_registry.clone(),
    ));

    let planner_a = execution_manager.clone();
    let planner_b = execution_manager.clone();
    let (result_a, result_b) = tokio::join!(
        planner_a.process_planning_pass(10),
        planner_b.process_planning_pass(10)
    );
    result_a.expect("planner A should not fatally error");
    result_b.expect("planner B should not fatally error");

    let destination_vaults = db
        .orders()
        .get_custody_vaults(order.id)
        .await
        .unwrap()
        .into_iter()
        .filter(|vault| vault.role == CustodyVaultRole::DestinationExecution)
        .collect::<Vec<_>>();
    assert_eq!(
        destination_vaults.len(),
        1,
        "expected exactly one destination execution vault under concurrent planning"
    );

    let steps = db.orders().get_execution_steps(order.id).await.unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("planning should materialize an across step");
    assert_eq!(
        across_step
            .request
            .get("recipient_custody_vault_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok()),
        Some(destination_vaults[0].id),
        "planned step must point at the single destination execution vault"
    );
}

#[tokio::test]
async fn two_concurrent_worker_passes_on_failing_order_produce_single_refund() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    fixture
        .mocks
        .fail_next_across_swap_approval("mock across unavailable")
        .await;

    let execution_manager = Arc::new(OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    ));

    let worker_a = execution_manager.clone();
    let worker_b = execution_manager.clone();
    let (result_a, result_b) = tokio::join!(
        worker_a.process_worker_pass(10),
        worker_b.process_worker_pass(10)
    );
    result_a.expect("worker A should not fatally error on step failure");
    result_b.expect("worker B should not fatally error on step failure");

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);

    fixture
        .mocks
        .fail_next_across_swap_approval("mock across unavailable")
        .await;
    let worker_a = execution_manager.clone();
    let worker_b = execution_manager.clone();
    let (result_a, result_b) = tokio::join!(
        worker_a.process_worker_pass(10),
        worker_b.process_worker_pass(10)
    );
    result_a.expect("worker A should not fatally error on retry step failure");
    result_b.expect("worker B should not fatally error on retry step failure");

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::RefundRequired);
    assert_eq!(vault.status, DepositVaultStatus::RefundRequired);

    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .unwrap();
    assert_eq!(across_step.status, OrderExecutionStepStatus::Failed);
    assert!(across_step
        .error
        .to_string()
        .contains("mock across unavailable"));
}

#[tokio::test]
async fn planning_pass_blocks_drained_provider_and_requests_refund() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let provider_policies = Arc::new(router_server::services::ProviderPolicyService::new(
        fixture.db.clone(),
    ));
    provider_policies
        .upsert(
            "across",
            ProviderQuotePolicyState::Enabled,
            ProviderExecutionPolicyState::Drain,
            "maintenance window",
            "test",
        )
        .await
        .unwrap();

    let execution_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    )
    .with_provider_policies(Some(provider_policies));

    let materialized = execution_manager.process_planning_pass(10).await.unwrap();
    assert!(materialized.is_empty());

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Refunding);
    assert_eq!(vault.status, DepositVaultStatus::Refunding);

    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].step_type, OrderExecutionStepType::WaitForDeposit);
    assert_eq!(steps[0].provider, "internal");
}

#[tokio::test]
async fn worker_restart_processes_pending_hint_and_completes_order() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let manager_a = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    manager_a.process_planning_pass(10).await.unwrap();
    manager_a
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("first manager should submit the across step");

    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&manager_a, &across_operation).await;
    manager_a
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    manager_a
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("first manager should submit the unit deposit step");

    let unit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;
    fixture
        .mocks
        .complete_unit_operation(provider_ref(&unit_operation))
        .await
        .unwrap();
    record_chain_detector_unit_deposit_hint(&manager_a, &fixture.db, &unit_operation).await;

    let manager_b = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let summary = manager_b.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.processed_provider_hints, 1);
    let updated_operation = fixture
        .db
        .orders()
        .get_provider_operation(unit_operation.id)
        .await
        .unwrap();
    assert_eq!(updated_operation.status, ProviderOperationStatus::Completed);
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let unit_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitDeposit)
        .expect("unit deposit step should exist");
    assert_eq!(unit_step.status, OrderExecutionStepStatus::Completed);
}

#[tokio::test]
async fn worker_restart_recovers_terminal_provider_operation_for_running_step() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let manager_a = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    manager_a.process_planning_pass(10).await.unwrap();

    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("planned order must include across step")
        .clone();

    fixture
        .db
        .vaults()
        .transition_status(
            fixture.vault_id,
            DepositVaultStatus::Funded,
            DepositVaultStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            order.status,
            RouterOrderStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .transition_execution_step_status(
            across_step.id,
            OrderExecutionStepStatus::Planned,
            OrderExecutionStepStatus::Running,
            Utc::now(),
        )
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .upsert_provider_operation(&OrderProviderOperation {
            id: Uuid::now_v7(),
            order_id: fixture.order_id,
            execution_attempt_id: across_step.execution_attempt_id,
            execution_step_id: Some(across_step.id),
            provider: "across".to_string(),
            operation_type: ProviderOperationType::AcrossBridge,
            provider_ref: Some(format!("restart-recovered-{}", fixture.order_id)),
            status: ProviderOperationStatus::Completed,
            request: across_step.request.clone(),
            response: json!({
                "expectedOutputAmount": "1000",
                "status": "filled"
            }),
            observed_state: json!({
                "source": "restart_recovery_test",
                "status": "filled"
            }),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;

    let manager_b = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let summary = manager_b.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.executed_orders, 1);

    let across_step = fixture
        .db
        .orders()
        .get_execution_step(across_step.id)
        .await
        .unwrap();
    assert_eq!(across_step.status, OrderExecutionStepStatus::Completed);
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    assert!(steps
        .iter()
        .any(|step| step.step_type == OrderExecutionStepType::UnitDeposit
            && matches!(
                step.status,
                OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Completed
            )));
}

#[tokio::test]
async fn worker_restart_reconciles_failed_step_into_refund() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let manager_a = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    manager_a.process_planning_pass(10).await.unwrap();

    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("planned order must include across step")
        .clone();

    fixture
        .db
        .vaults()
        .transition_status(
            fixture.vault_id,
            DepositVaultStatus::Funded,
            DepositVaultStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            order.status,
            RouterOrderStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .transition_execution_step_status(
            across_step.id,
            OrderExecutionStepStatus::Planned,
            OrderExecutionStepStatus::Running,
            Utc::now(),
        )
        .await
        .unwrap();
    fixture
        .db
        .orders()
        .fail_execution_step(
            across_step.id,
            json!({
                "error": "crash-before-inline-reconcile"
            }),
            Utc::now(),
        )
        .await
        .unwrap();

    let manager_b = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url())),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let summary = manager_b.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.reconciled_failed_orders, 1);

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);
    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Active);
    let retry_steps = fixture
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .unwrap();
    assert!(retry_steps.iter().any(|step| {
        step.step_type == OrderExecutionStepType::AcrossBridge
            && matches!(
                step.status,
                OrderExecutionStepStatus::Waiting
                    | OrderExecutionStepStatus::Completed
                    | OrderExecutionStepStatus::Planned
                    | OrderExecutionStepStatus::Running
            )
    }));
}

#[tokio::test]
async fn worker_restart_after_execution_step_persist_crash_recovers_waiting_step() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url()));
    let crashing_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers.clone(),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    )
    .with_crash_injector(Arc::new(PanicOnceCrashInjector::new(
        OrderExecutionCrashPoint::AfterExecutionStepStatusPersisted,
    )));
    crashing_manager.process_planning_pass(10).await.unwrap();
    let order_id = fixture.order_id;

    expect_task_panic(tokio::spawn(async move {
        let _ = crashing_manager.execute_materialized_order(order_id).await;
    }))
    .await;

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);
    let across_step = steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("across step should exist");
    assert_eq!(across_step.status, OrderExecutionStepStatus::Waiting);

    let recovery_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers.clone(),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let across_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::AcrossBridge,
    )
    .await;
    wait_for_across_deposit_indexed(
        &fixture.mocks,
        fixture.vault_address,
        Duration::from_secs(10),
    )
    .await;
    record_detector_provider_status_hint(&recovery_manager, &across_operation).await;
    recovery_manager
        .process_provider_operation_hints(10)
        .await
        .unwrap();
    fund_destination_execution_vault_from_across(&fixture.db, fixture.order_id).await;
    recovery_manager
        .execute_materialized_order(fixture.order_id)
        .await
        .unwrap()
        .expect("recovery manager should submit the unit deposit step");
    let unit_operation = provider_operation_by_type(
        &fixture.db,
        fixture.order_id,
        ProviderOperationType::UnitDeposit,
    )
    .await;

    let across_step = fixture
        .db
        .orders()
        .get_execution_step(across_step.id)
        .await
        .unwrap();
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(across_step.status, OrderExecutionStepStatus::Completed);
    assert_eq!(
        unit_operation.status,
        ProviderOperationStatus::WaitingExternal
    );
    assert!(steps.iter().any(|step| {
        step.step_type == OrderExecutionStepType::UnitDeposit
            && matches!(
                step.status,
                OrderExecutionStepStatus::Waiting | OrderExecutionStepStatus::Completed
            )
    }));
}

#[tokio::test]
async fn worker_restart_after_provider_operation_status_persist_crash_recovers_terminal_step() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    fixture
        .db
        .vaults()
        .transition_status(
            fixture.vault_id,
            DepositVaultStatus::Funded,
            DepositVaultStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            order.status,
            RouterOrderStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();

    let now = Utc::now();
    let execution_attempt = OrderExecutionAttempt {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        attempt_index: 1,
        attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
        status: OrderExecutionAttemptStatus::Active,
        trigger_step_id: None,
        trigger_provider_operation_id: None,
        failure_reason: json!({}),
        input_custody_snapshot: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_attempt(&execution_attempt)
        .await
        .unwrap();
    let step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        transition_decl_id: None,
        step_index: 1,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider: "across".to_string(),
        status: OrderExecutionStepStatus::Waiting,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: Some("crash-recovery-op".to_string()),
        idempotency_key: Some(format!("order:{}:crash-provider-status", fixture.order_id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: Some(now),
        completed_at: None,
        details: json!({ "source": "crash_recovery_test" }),
        request: json!({ "source": "crash_recovery_test" }),
        response: json!({}),
        error: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_step(&step)
        .await
        .unwrap();
    let operation = OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        execution_step_id: Some(step.id),
        provider: "across".to_string(),
        operation_type: ProviderOperationType::AcrossBridge,
        provider_ref: Some("crash-recovery-op".to_string()),
        status: ProviderOperationStatus::WaitingExternal,
        request: step.request.clone(),
        response: json!({ "provider_ref": "crash-recovery-op" }),
        observed_state: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_provider_operation(&operation)
        .await
        .unwrap();

    let action_providers = Arc::new(ActionProviderRegistry::default());
    let crashing_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers.clone(),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let operation_id = operation.id;
    let crashing_manager =
        crashing_manager.with_crash_injector(Arc::new(PanicOnceCrashInjector::new(
            OrderExecutionCrashPoint::AfterProviderOperationStatusPersisted,
        )));

    expect_task_panic(tokio::spawn(async move {
        crashing_manager
            .apply_provider_operation_status_update(ProviderOperationStatusUpdate {
                provider_operation_id: Some(operation.id),
                provider: Some("across".to_string()),
                provider_ref: None,
                status: ProviderOperationStatus::Completed,
                observed_state: json!({
                    "status": "filled"
                }),
                response: Some(json!({
                    "status": "filled",
                    "expectedOutputAmount": "1000"
                })),
                tx_hash: Some("0xproviderstatuspersist".to_string()),
                error: None,
            })
            .await
            .unwrap();
    }))
    .await;

    let operation = fixture
        .db
        .orders()
        .get_provider_operation(operation_id)
        .await
        .unwrap();
    let step = fixture
        .db
        .orders()
        .get_execution_step(operation.execution_step_id.unwrap())
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    assert_eq!(operation.status, ProviderOperationStatus::Completed);
    assert_eq!(step.status, OrderExecutionStepStatus::Waiting);
    assert_eq!(order.status, RouterOrderStatus::Executing);

    let recovery_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers,
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    recovery_manager.process_worker_pass(10).await.unwrap();

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    let step = fixture
        .db
        .orders()
        .get_execution_step(step.id)
        .await
        .unwrap();
    assert_eq!(step.status, OrderExecutionStepStatus::Completed);
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
}

#[tokio::test]
async fn worker_restart_after_provider_step_settlement_crash_finalizes_order() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    fixture
        .db
        .vaults()
        .transition_status(
            fixture.vault_id,
            DepositVaultStatus::Funded,
            DepositVaultStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    fixture
        .db
        .orders()
        .transition_status(
            fixture.order_id,
            order.status,
            RouterOrderStatus::Executing,
            Utc::now(),
        )
        .await
        .unwrap();

    let now = Utc::now();
    let execution_attempt = OrderExecutionAttempt {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        attempt_index: 1,
        attempt_kind: OrderExecutionAttemptKind::PrimaryExecution,
        status: OrderExecutionAttemptStatus::Active,
        trigger_step_id: None,
        trigger_provider_operation_id: None,
        failure_reason: json!({}),
        input_custody_snapshot: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_attempt(&execution_attempt)
        .await
        .unwrap();
    let step = OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        transition_decl_id: None,
        step_index: 1,
        step_type: OrderExecutionStepType::AcrossBridge,
        provider: "across".to_string(),
        status: OrderExecutionStepStatus::Waiting,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        min_amount_out: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: Some("crash-settlement-op".to_string()),
        idempotency_key: Some(format!("order:{}:crash-step-settlement", fixture.order_id)),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: Some(now),
        completed_at: None,
        details: json!({ "source": "crash_recovery_test" }),
        request: json!({ "source": "crash_recovery_test" }),
        response: json!({}),
        error: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_execution_step(&step)
        .await
        .unwrap();
    let operation = OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id: fixture.order_id,
        execution_attempt_id: Some(execution_attempt.id),
        execution_step_id: Some(step.id),
        provider: "across".to_string(),
        operation_type: ProviderOperationType::AcrossBridge,
        provider_ref: Some("crash-settlement-op".to_string()),
        status: ProviderOperationStatus::WaitingExternal,
        request: step.request.clone(),
        response: json!({ "provider_ref": "crash-settlement-op" }),
        observed_state: json!({}),
        created_at: now,
        updated_at: now,
    };
    fixture
        .db
        .orders()
        .create_provider_operation(&operation)
        .await
        .unwrap();

    let action_providers = Arc::new(ActionProviderRegistry::default());
    let crashing_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers.clone(),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let step_id = step.id;
    let crashing_manager = crashing_manager.with_crash_injector(Arc::new(
        PanicOnceCrashInjector::new(OrderExecutionCrashPoint::AfterProviderStepSettlement),
    ));

    expect_task_panic(tokio::spawn(async move {
        crashing_manager
            .apply_provider_operation_status_update(ProviderOperationStatusUpdate {
                provider_operation_id: Some(operation.id),
                provider: Some("across".to_string()),
                provider_ref: None,
                status: ProviderOperationStatus::Completed,
                observed_state: json!({
                    "status": "filled"
                }),
                response: Some(json!({
                    "status": "filled",
                    "expectedOutputAmount": "1000"
                })),
                tx_hash: Some("0xproviderstepsettled".to_string()),
                error: None,
            })
            .await
            .unwrap();
    }))
    .await;

    let step = fixture
        .db
        .orders()
        .get_execution_step(step_id)
        .await
        .unwrap();
    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    assert_eq!(step.status, OrderExecutionStepStatus::Completed);
    assert_eq!(order.status, RouterOrderStatus::Executing);

    let recovery_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        action_providers,
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    recovery_manager.process_worker_pass(10).await.unwrap();

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::Completed);
    assert_eq!(vault.status, DepositVaultStatus::Completed);
}

#[tokio::test]
async fn worker_restart_after_step_mark_failed_crash_reconciles_refund() {
    let fixture = funded_market_order_fixture("evm:8453", "evm:1").await;
    let planner_action_providers =
        Arc::new(ActionProviderRegistry::mock_http(fixture.mocks.base_url()));
    let planner_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        planner_action_providers,
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    planner_manager.process_planning_pass(10).await.unwrap();
    let order_id = fixture.order_id;
    let failing_action_providers = Arc::new(ActionProviderRegistry::new(
        vec![Arc::new(FailingBridgeProvider)],
        vec![],
        vec![],
    ));

    let crashing_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        failing_action_providers.clone(),
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    )
    .with_crash_injector(Arc::new(PanicOnceCrashInjector::new(
        OrderExecutionCrashPoint::AfterStepMarkedFailed,
    )));

    expect_task_panic(tokio::spawn(async move {
        let _ = crashing_manager.execute_materialized_order(order_id).await;
    }))
    .await;

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    let steps = fixture
        .db
        .orders()
        .get_execution_steps(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(order.status, RouterOrderStatus::Executing);
    assert_eq!(vault.status, DepositVaultStatus::Executing);
    assert!(steps
        .iter()
        .any(|step| step.step_index > 0 && step.status == OrderExecutionStepStatus::Failed));

    let recovery_manager = OrderExecutionManager::with_dependencies(
        fixture.db.clone(),
        failing_action_providers,
        fixture.custody_action_executor.clone(),
        fixture.chain_registry.clone(),
    );
    let summary = recovery_manager.process_worker_pass(10).await.unwrap();
    assert_eq!(summary.reconciled_failed_orders, 1);

    let order = fixture.db.orders().get(fixture.order_id).await.unwrap();
    let vault = fixture.db.vaults().get(fixture.vault_id).await.unwrap();
    assert_eq!(order.status, RouterOrderStatus::RefundRequired);
    assert_eq!(vault.status, DepositVaultStatus::RefundRequired);
    let attempts = fixture
        .db
        .orders()
        .get_execution_attempts(fixture.order_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[2].attempt_index, 3);
    assert_eq!(
        attempts[2].status,
        OrderExecutionAttemptStatus::RefundRequired
    );
    assert_eq!(
        attempts[2].attempt_kind,
        OrderExecutionAttemptKind::RefundRecovery
    );
}

#[tokio::test]
async fn vault_address_matches_deterministic_derivation_from_quote() {
    let dir = tempfile::tempdir().unwrap();
    let h = harness().await;
    let db = test_db().await;
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let settings = Arc::new(test_settings(dir.path()));
    let action_providers = Arc::new(ActionProviderRegistry::mock_http(mocks.base_url()));
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        h.chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings.clone(), h.chain_registry.clone());

    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:1").unwrap(),
        asset: AssetId::Native,
    };

    let envelope = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();
    let quote = envelope
        .quote
        .as_market_order()
        .expect("market order quote")
        .clone();

    let order = create_test_order_from_quote(&order_manager, quote.id).await;
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset.clone(),
            ..evm_native_request()
        })
        .await
        .unwrap();

    let (expected_address, _) = derive_deposit_address_for_quote(
        h.chain_registry.as_ref(),
        &settings.master_key_bytes(),
        quote.id,
        &source_asset.chain,
    )
    .expect("derivation succeeds");

    assert_eq!(
        vault.deposit_vault_address, expected_address,
        "vault address must match the deterministic derivation from (master_key, quote_id)"
    );
}

#[tokio::test]
async fn quote_envelope_never_exposes_deposit_address() {
    let mocks = MockIntegratorServer::spawn().await.unwrap();
    let (order_manager, _db) = test_order_manager(Arc::new(ActionProviderRegistry::mock_http(
        mocks.base_url(),
    )))
    .await;

    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:1").unwrap(),
        asset: AssetId::Native,
    };

    let envelope = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset,
            to_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "1000".to_string(),
                slippage_bps: 100,
            },
        })
        .await
        .unwrap();

    let envelope_value = serde_json::to_value(&envelope).expect("envelope serializes");
    let quote_obj = envelope_value["quote"]["payload"]
        .as_object()
        .expect("market-order payload is an object");
    let keys: Vec<&str> = quote_obj.keys().map(String::as_str).collect();

    for forbidden in [
        "deposit_address",
        "depositor_address",
        "deposit_vault_address",
    ] {
        assert!(
            !keys.contains(&forbidden),
            "quote envelope must not expose {forbidden}; saw keys = {keys:?}"
        );
    }
}
