use std::{process::Command, str::FromStr, sync::Arc, time::Duration};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{ext::AnvilApi, Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::PrivateKeySigner,
};
use chains::{evm::EvmChain, ChainRegistry};
use chrono::Utc;
use devnet::{
    mock_integrators::{MockIntegratorConfig, MockIntegratorServer},
    RiftDevnet,
};
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use router_core::{
    config::Settings,
    db::Database,
    models::{
        DepositVaultStatus, OrderExecutionAttemptKind, OrderExecutionAttemptStatus,
        OrderExecutionStepStatus, RouterOrderQuote, RouterOrderStatus, VaultAction,
    },
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        custody_action_executor::{CustodyActionExecutor, HyperliquidCallNetwork},
        ActionProviderHttpOptions, ActionProviderRegistry, VeloraHttpProviderConfig,
    },
};
use router_primitives::ChainType;
use router_server::{
    api::{CreateOrderRequest, CreateVaultRequest, MarketOrderQuoteKind, MarketOrderQuoteRequest},
    services::{order_manager::OrderManager, vault_manager::VaultManager},
};
use serde_json::json;
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection, PgPool};
use temporal_worker::{
    order_execution::{
        activities::{OrderActivities, OrderActivityDeps},
        build_worker, order_workflow_id,
        types::{OrderTerminalStatus, OrderWorkflowInput, OrderWorkflowOutput},
        workflow_start_options,
        workflows::OrderWorkflow,
        DEFAULT_TASK_QUEUE,
    },
    runtime::{connect_client, TemporalConnection},
};
use temporalio_client::WorkflowGetResultOptions;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";
const EVM_NATIVE_GAS_BUFFER_WEI: u128 = 1_000_000_000_000_000_000;
const EXECUTE_STEP_TEMPORAL_ATTEMPTS: usize = 3;
const ORDER_WORKFLOW_TEST_TIMEOUT: Duration = Duration::from_secs(240);

struct TestPostgres {
    admin_database_url: String,
    _container: Option<ContainerAsync<GenericImage>>,
}

struct WorkflowRun {
    _postgres: TestPostgres,
    db: Database,
    order_id: Uuid,
    output: OrderWorkflowOutput,
}

#[derive(Default)]
struct WorkflowOptions {
    velora_transaction_failures: usize,
    velora_stale_quote_failures: usize,
    expire_quote_legs: bool,
    refreshed_eth_usd_micro: Option<u128>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_completes_funded_single_step_order() {
    let run = run_order_workflow(WorkflowOptions::default()).await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_retries_failed_step_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let first_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load first attempt steps");
    assert!(
        first_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Superseded),
        "first attempt failed suffix should be superseded"
    );
    let retry_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load retry attempt steps");
    assert!(
        retry_attempt_steps
            .iter()
            .all(|step| step.status == OrderExecutionStepStatus::Completed),
        "retry attempt should complete every cloned step"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_marks_refund_required_after_second_failed_attempt() {
    let run = run_order_workflow(WorkflowOptions {
        velora_transaction_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS * 2,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(
        run.output.terminal_status,
        OrderTerminalStatus::RefundRequired
    );
    let refund_required = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refund-required order");
    assert_eq!(refund_required.status, RouterOrderStatus::RefundRequired);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].attempt_index, 1);
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(attempts[1].attempt_index, 2);
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Failed);

    let first_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load first attempt steps");
    assert!(
        first_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Superseded),
        "first attempt failed suffix should be superseded"
    );
    let second_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[1].id)
        .await
        .expect("load second attempt steps");
    assert!(
        second_attempt_steps
            .iter()
            .any(|step| step.status == OrderExecutionStepStatus::Failed),
        "second attempt should retain the terminal failed step"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refreshes_stale_quote_then_completes() {
    let run = run_order_workflow(WorkflowOptions {
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
        expire_quote_legs: true,
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(run.output.terminal_status, OrderTerminalStatus::Completed);
    let completed = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load completed order");
    assert_eq!(completed.status, RouterOrderStatus::Completed);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
    assert_eq!(
        attempts[1].attempt_kind,
        OrderExecutionAttemptKind::RefreshedExecution
    );
    assert_eq!(attempts[1].status, OrderExecutionAttemptStatus::Completed);

    let stale_attempt_steps = run
        .db
        .orders()
        .get_execution_steps_for_attempt(attempts[0].id)
        .await
        .expect("load stale attempt steps");
    assert!(
        stale_attempt_steps.iter().any(|step| {
            step.status == OrderExecutionStepStatus::Superseded
                && step.error.get("reason").and_then(serde_json::Value::as_str)
                    == Some("superseded_by_stale_provider_quote_refresh")
        }),
        "stale attempt failed suffix should be superseded by quote refresh"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn order_workflow_refresh_untenable_routes_to_refund() {
    let run = run_order_workflow(WorkflowOptions {
        velora_stale_quote_failures: EXECUTE_STEP_TEMPORAL_ATTEMPTS,
        expire_quote_legs: true,
        refreshed_eth_usd_micro: Some(1_000_000),
        ..WorkflowOptions::default()
    })
    .await;

    assert_eq!(
        run.output.terminal_status,
        OrderTerminalStatus::RefundRequired
    );
    let refund_required = run
        .db
        .orders()
        .get(run.order_id)
        .await
        .expect("load refund-required order");
    assert_eq!(refund_required.status, RouterOrderStatus::RefundRequired);

    let attempts = run
        .db
        .orders()
        .get_execution_attempts(run.order_id)
        .await
        .expect("load execution attempts");
    assert_eq!(attempts.len(), 1);
    assert_eq!(
        attempts[0].attempt_kind,
        OrderExecutionAttemptKind::PrimaryExecution
    );
    assert_eq!(attempts[0].status, OrderExecutionAttemptStatus::Failed);
}

async fn run_order_workflow(options: WorkflowOptions) -> WorkflowRun {
    ensure_temporal_up();

    let postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let db = Database::connect(&database_url, 8, 1)
        .await
        .expect("connect test database");

    let (devnet, _) = RiftDevnet::builder()
        .using_esplora(true)
        .build()
        .await
        .expect("devnet setup failed");
    install_mock_usdc_clone(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid Base USDC address"),
    )
    .await;
    let mocks = spawn_velora_mocks(&devnet, &options).await;
    let settings = Arc::new(test_settings());
    let chain_registry = Arc::new(test_chain_registry(&devnet).await);
    let action_providers = Arc::new(test_action_providers(&mocks));
    let custody_executor = Arc::new(CustodyActionExecutor::new(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
    ));

    let order_id = seed_funded_single_step_order(
        &db,
        settings,
        chain_registry,
        action_providers.clone(),
        &devnet,
    )
    .await;
    if options.expire_quote_legs {
        expire_market_order_quote_legs(&database_url, order_id).await;
    }
    if let Some(usd_micro) = options.refreshed_eth_usd_micro {
        mocks.set_velora_usd_price_micro("ETH", usd_micro).await;
    }

    let task_queue = format!("{DEFAULT_TASK_QUEUE}-{}", Uuid::now_v7());
    let connection = TemporalConnection {
        temporal_address: "http://127.0.0.1:7233".to_string(),
        namespace: "default".to_string(),
    };
    let activity_deps = OrderActivityDeps::new(db.clone(), action_providers, custody_executor);

    let local = tokio::task::LocalSet::new();
    let output = local
        .run_until(async move {
            let mut built = build_worker(
                &connection,
                &task_queue,
                OrderActivities::new(activity_deps),
            )
            .await
            .expect("build Temporal worker");
            let shutdown_worker = built.worker.shutdown_handle();
            let worker_task = tokio::task::spawn_local(async move { built.worker.run().await });
            sleep(Duration::from_millis(500)).await;

            let client = connect_client(&connection)
                .await
                .expect("connect Temporal client");
            let workflow_id = order_workflow_id(order_id);
            let handle = client
                .start_workflow(
                    OrderWorkflow::run,
                    OrderWorkflowInput { order_id },
                    workflow_start_options(&task_queue, &workflow_id),
                )
                .await
                .expect("start OrderWorkflow");
            let output: OrderWorkflowOutput = timeout(
                ORDER_WORKFLOW_TEST_TIMEOUT,
                handle.get_result(WorkflowGetResultOptions::default()),
            )
            .await
            .expect("OrderWorkflow timed out")
            .expect("OrderWorkflow result");

            shutdown_worker();
            timeout(Duration::from_secs(10), worker_task)
                .await
                .expect("worker shutdown timed out")
                .expect("worker task join")
                .expect("worker run");
            output
        })
        .await;

    WorkflowRun {
        _postgres: postgres,
        db,
        order_id,
        output,
    }
}

fn ensure_temporal_up() {
    let status = Command::new("just")
        .arg("temporal-up")
        .status()
        .expect("run `just temporal-up`; install just or start Temporal manually");
    assert!(status.success(), "`just temporal-up` failed with {status}");
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
    let database_name = format!("temporal_order_workflow_{}", Uuid::now_v7().simple());

    sqlx_core::query::query(&format!(r#"CREATE DATABASE "{database_name}""#))
        .execute(&mut admin)
        .await
        .expect("create isolated test database");

    let mut database_url =
        url::Url::parse(admin_database_url).expect("parse test database admin URL");
    database_url.set_path(&database_name);
    database_url.to_string()
}

fn test_settings() -> Settings {
    let dir = tempfile::tempdir().expect("settings tempdir");
    let path = dir.path().join("router-master-key.hex");
    std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    Settings::load(&path).expect("load test settings")
}

async fn test_chain_registry(devnet: &RiftDevnet) -> ChainRegistry {
    let base_chain = EvmChain::new(
        &devnet.base.anvil.endpoint(),
        BASE_USDC_ADDRESS,
        ChainType::Base,
        b"temporal-worker-base-wallet",
        1,
        Duration::from_secs(1),
    )
    .await
    .expect("base chain setup");
    let mut registry = ChainRegistry::new();
    registry.register_evm(ChainType::Base, Arc::new(base_chain));
    registry
}

fn test_action_providers(mocks: &MockIntegratorServer) -> ActionProviderRegistry {
    ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across: None,
        cctp: None,
        hyperunit_base_url: None,
        hyperunit_proxy_url: None,
        hyperliquid_base_url: None,
        velora: Some(VeloraHttpProviderConfig {
            base_url: mocks.base_url().to_string(),
            partner: Some("temporal-worker-e2e".to_string()),
        }),
        hyperliquid_network: HyperliquidCallNetwork::Testnet,
        hyperliquid_order_timeout_ms: 30_000,
    })
    .expect("action providers")
}

async fn spawn_velora_mocks(
    devnet: &RiftDevnet,
    options: &WorkflowOptions,
) -> MockIntegratorServer {
    let config = MockIntegratorConfig::default()
        .with_velora_swap_contract_address(
            devnet.base.anvil.chain_id(),
            format!("{:#x}", devnet.base.mock_velora_swap_contract.address()),
        )
        .with_velora_transaction_fail_next_n(options.velora_transaction_failures)
        .with_velora_transaction_stale_quote_fail_next_n(options.velora_stale_quote_failures);
    MockIntegratorServer::spawn_with_config(config)
        .await
        .expect("spawn mock integrators")
}

async fn expire_market_order_quote_legs(database_url: &str, order_id: Uuid) {
    let pool = PgPool::connect(database_url)
        .await
        .expect("connect raw pool for quote expiry");
    let expired_at = (Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
    sqlx_core::query::query(
        r#"
        UPDATE market_order_quotes
        SET provider_quote = jsonb_set(
            provider_quote,
            '{legs}',
            (
                SELECT jsonb_agg(jsonb_set(leg.value, '{expires_at}', to_jsonb($2::text), false))
                FROM jsonb_array_elements(provider_quote->'legs') AS leg(value)
            ),
            false
        )
        WHERE order_id = $1
        "#,
    )
    .bind(order_id)
    .bind(expired_at)
    .execute(&pool)
    .await
    .expect("expire market quote legs");
}

async fn seed_funded_single_step_order(
    db: &Database,
    settings: Arc<Settings>,
    chain_registry: Arc<ChainRegistry>,
    action_providers: Arc<ActionProviderRegistry>,
    devnet: &RiftDevnet,
) -> Uuid {
    let order_manager = OrderManager::with_action_providers(
        db.clone(),
        settings.clone(),
        chain_registry.clone(),
        action_providers,
    );
    let vault_manager = VaultManager::new(db.clone(), settings, chain_registry);
    let source_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::Native,
    };
    let destination_asset = DepositAsset {
        chain: ChainId::parse("evm:8453").expect("base chain id"),
        asset: AssetId::parse(BASE_USDC_ADDRESS).expect("base USDC asset"),
    };
    let quote = order_manager
        .quote_market_order(MarketOrderQuoteRequest {
            from_asset: source_asset.clone(),
            to_asset: destination_asset,
            recipient_address: valid_evm_address(),
            order_kind: MarketOrderQuoteKind::ExactIn {
                amount_in: "60000000000000000".to_string(),
                slippage_bps: Some(100),
            },
        })
        .await
        .expect("quote single-step order");
    let market_quote = match quote.quote {
        RouterOrderQuote::MarketOrder(quote) => quote,
        RouterOrderQuote::LimitOrder(_) => panic!("expected market quote"),
    };
    assert!(
        market_quote
            .provider_id
            .contains("universal_router_swap:velora"),
        "expected a single Velora step, got {}",
        market_quote.provider_id
    );

    let (order, _) = order_manager
        .create_order_from_quote(CreateOrderRequest {
            quote_id: market_quote.id,
            refund_address: valid_evm_address(),
            idempotency_key: None,
            metadata: json!({ "test": "temporal_order_workflow" }),
        })
        .await
        .expect("create order from quote");
    let vault = vault_manager
        .create_vault(CreateVaultRequest {
            order_id: Some(order.id),
            deposit_asset: source_asset,
            action: VaultAction::Null,
            recovery_address: valid_evm_address(),
            cancellation_commitment:
                "0x1111111111111111111111111111111111111111111111111111111111111111".to_string(),
            cancel_after: None,
            metadata: json!({ "test": "temporal_order_workflow" }),
        })
        .await
        .expect("create funding vault");
    fund_source_vault(
        devnet,
        &vault.deposit_vault_address,
        &market_quote.amount_in,
    )
    .await;

    let now = Utc::now();
    db.vaults()
        .transition_status(
            vault.id,
            DepositVaultStatus::PendingFunding,
            DepositVaultStatus::Funded,
            now,
        )
        .await
        .expect("mark vault funded");
    db.orders()
        .transition_status(
            order.id,
            RouterOrderStatus::PendingFunding,
            RouterOrderStatus::Funded,
            now,
        )
        .await
        .expect("mark order funded");
    order.id
}

async fn fund_source_vault(devnet: &RiftDevnet, vault_address: &str, amount_in: &str) {
    let vault_address = Address::from_str(vault_address).expect("funding vault EVM address");
    let amount = U256::from_str_radix(amount_in, 10).expect("amount parses");
    send_native(
        &devnet.base,
        vault_address,
        amount + U256::from(EVM_NATIVE_GAS_BUFFER_WEI),
    )
    .await;
    mine_evm_confirmation_block(&devnet.base).await;
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

fn anvil_signer(devnet: &devnet::EthDevnet) -> PrivateKeySigner {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[0].to_bytes())
    )
    .parse()
    .expect("valid Anvil private key")
}

fn valid_evm_address() -> String {
    "0x1111111111111111111111111111111111111111".to_string()
}
