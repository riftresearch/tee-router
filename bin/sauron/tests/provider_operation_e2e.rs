use std::{
    env,
    error::Error,
    future::Future,
    net::{IpAddr, SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    str::FromStr,
    sync::OnceLock,
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
use chains::ChainRegistry;
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
    db::Database,
    models::{
        CustodyVaultControlType, DepositVaultStatus, OrderExecutionStepType,
        OrderProviderOperation, ProviderOperationStatus, ProviderOperationType,
        RouterOrderEnvelope, RouterOrderQuoteEnvelope, RouterOrderStatus,
        SAURON_EVM_RECEIPT_OBSERVER_HINT_SOURCE, SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE,
        SAURON_HYPERUNIT_OBSERVER_HINT_SOURCE,
    },
    protocol::{backend_chain_for_id, AssetId, ChainId},
};
use router_server::{
    app::{initialize_components, PaymasterMode},
    server::run_api,
    services::deposit_address::derive_deposit_salt_for_quote,
    worker::{run_worker_loop, RouterWorkerConfig},
    RouterServerArgs,
};
use router_temporal::{OrderWorkflowClient, TemporalConnection as RouterTemporalConnection};
use sauron::discovery::{evm_indexer::EvmIndexerDiscoveryBackend, DiscoveryBackend};
use sauron::{run as run_sauron, SauronArgs};
use serde_json::{json, Value};
use sqlx_core::connection::Connection;
use sqlx_postgres::{PgConnectOptions, PgConnection, PgPoolOptions};
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

const MOCK_ERC20_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const MAINNET_HYPERLIQUID_BRIDGE_ADDRESS: &str = "0x2df1c51e09aecf9cacb7bc98cb1742757f163df7";
const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";
const ROUTER_TEST_DATABASE_URL_ENV: &str = "ROUTER_TEST_DATABASE_URL";
const ROUTER_DETECTOR_API_KEY: &str = "test-detector-secret-000000000000";
const LIVE_RUNTIME_RUN_FLAG: &str = "ROUTER_LIVE_RUNTIME_E2E";
const LIVE_SPEND_CONFIRM_ENV: &str = "ROUTER_LIVE_CONFIRM_SPEND";
const LIVE_SPEND_CONFIRM_VALUE: &str = "I_UNDERSTAND_THIS_SPENDS_REAL_FUNDS";
const LIVE_TEST_PRIVATE_KEY_ENV: &str = "LIVE_TEST_PRIVATE_KEY";
const ROUTER_LIVE_SOURCE_PRIVATE_KEY_ENV: &str = "ROUTER_LIVE_SOURCE_PRIVATE_KEY";
const ETH_RPC_URL_ENV: &str = "ETH_RPC_URL";
const BASE_RPC_URL_ENV: &str = "BASE_RPC_URL";
const BASE_FUNDING_RPC_URL_ENV: &str = "ROUTER_LIVE_BASE_FUNDING_RPC_URL";
const ARBITRUM_RPC_URL_ENV: &str = "ARBITRUM_RPC_URL";
const ELECTRUM_HTTP_SERVER_URL_ENV: &str = "ELECTRUM_HTTP_SERVER_URL";
const BITCOIN_RPC_URL_ENV: &str = "BITCOIN_RPC_URL";
const BITCOIN_RPC_AUTH_ENV: &str = "BITCOIN_RPC_AUTH";
const BITCOIN_INDEXER_URL_ENV: &str = "BITCOIN_INDEXER_URL";
const BITCOIN_RECEIPT_WATCHER_URL_ENV: &str = "BITCOIN_RECEIPT_WATCHER_URL";
const ACROSS_API_URL_ENV: &str = "ACROSS_API_URL";
const ACROSS_API_KEY_ENV: &str = "ACROSS_API_KEY";
const ACROSS_INTEGRATOR_ID_ENV: &str = "ACROSS_INTEGRATOR_ID";
const HYPERUNIT_API_URL_ENV: &str = "HYPERUNIT_API_URL";
const HYPERUNIT_PROXY_URL_ENV: &str = "HYPERUNIT_PROXY_URL";
const HYPERLIQUID_API_URL_ENV: &str = "HYPERLIQUID_API_URL";
const HYPERLIQUID_NETWORK_ENV: &str = "HYPERLIQUID_NETWORK";
const HYPERLIQUID_EXECUTION_PRIVATE_KEY_ENV: &str = "HYPERLIQUID_EXECUTION_PRIVATE_KEY";
const HYPERLIQUID_ACCOUNT_ADDRESS_ENV: &str = "HYPERLIQUID_ACCOUNT_ADDRESS";
const HYPERLIQUID_VAULT_ADDRESS_ENV: &str = "HYPERLIQUID_VAULT_ADDRESS";
const ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY_ENV: &str = "ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY";
const ROUTER_LIVE_HYPERLIQUID_DESTINATION_ADDRESS_ENV: &str =
    "ROUTER_LIVE_HYPERLIQUID_DESTINATION_ADDRESS";
const ROUTER_LIVE_HYPERLIQUID_ACCOUNT_ADDRESS_ENV: &str = "ROUTER_LIVE_HYPERLIQUID_ACCOUNT_ADDRESS";
const ROUTER_LIVE_HYPERLIQUID_VAULT_ADDRESS_ENV: &str = "ROUTER_LIVE_HYPERLIQUID_VAULT_ADDRESS";
const ETHEREUM_PAYMASTER_PRIVATE_KEY_ENV: &str = "ETHEREUM_PAYMASTER_PRIVATE_KEY";
const BASE_PAYMASTER_PRIVATE_KEY_ENV: &str = "BASE_PAYMASTER_PRIVATE_KEY";
const ARBITRUM_PAYMASTER_PRIVATE_KEY_ENV: &str = "ARBITRUM_PAYMASTER_PRIVATE_KEY";
const ROUTER_LIVE_BTC_RECIPIENT_ADDRESS_ENV: &str = "ROUTER_LIVE_BTC_RECIPIENT_ADDRESS";
const ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS_ENV: &str = "ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS";
const ETHEREUM_TOKEN_INDEXER_URL_ENV: &str = "ETHEREUM_TOKEN_INDEXER_URL";
const BASE_TOKEN_INDEXER_URL_ENV: &str = "BASE_TOKEN_INDEXER_URL";
const ARBITRUM_TOKEN_INDEXER_URL_ENV: &str = "ARBITRUM_TOKEN_INDEXER_URL";
const TOKEN_INDEXER_API_KEY_ENV: &str = "TOKEN_INDEXER_API_KEY";
const LIVE_BASE_ETH_AMOUNT_WEI_ENV: &str = "ROUTER_LIVE_BASE_ETH_TO_BTC_AMOUNT_WEI";
const LIVE_BASE_USDC_AMOUNT_ENV: &str = "ROUTER_LIVE_BASE_USDC_TO_BTC_AMOUNT";
const LIVE_RECOVERY_DIR_ENV: &str = "ROUTER_LIVE_RECOVERY_DIR";
const DEFAULT_LIVE_ACROSS_API_URL: &str = "https://app.across.to/api";
const DEFAULT_LIVE_HYPERUNIT_API_URL: &str = "https://api.hyperunit.xyz";
const DEFAULT_LIVE_HYPERLIQUID_API_URL: &str = "https://api.hyperliquid.xyz";
const DEFAULT_LIVE_BASE_FUNDING_RPC_URL: &str = "https://mainnet.base.org";
const DEFAULT_LIVE_BASE_ETH_AMOUNT_WEI: &str = "60000000000000000";
const DEFAULT_LIVE_BASE_USDC_AMOUNT: &str = "60000000";
const LIVE_TERMINAL_TIMEOUT: Duration = Duration::from_secs(45 * 60);
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const LIVE_RPC_RETRY_ATTEMPTS: usize = 10;
const LIVE_RPC_RETRY_INITIAL_DELAY: Duration = Duration::from_secs(2);
const LIVE_NATIVE_SOURCE_GAS_BUFFER_WEI: u128 = 1_200_000_000_000_000;
const LIVE_RECOVERY_TABLES: &[(&str, &str)] = &[
    ("router_orders", "created_at ASC, id ASC"),
    ("market_order_quotes", "created_at ASC, id ASC"),
    ("market_order_actions", "created_at ASC, order_id ASC"),
    ("deposit_vaults", "created_at ASC, id ASC"),
    ("custody_vaults", "created_at ASC, id ASC"),
    ("order_execution_steps", "step_index ASC, id ASC"),
    ("order_provider_operations", "created_at ASC, id ASC"),
    ("order_provider_addresses", "created_at ASC, id ASC"),
    ("order_provider_operation_hints", "created_at ASC, id ASC"),
    ("order_custody_accounts", "created_at ASC, id ASC"),
];

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address owner) external view returns (uint256);
        function transfer(address to, uint256 amount) external returns (bool);
    }
}

#[derive(Clone, Copy, Debug)]
enum RuntimeRoute {
    BaseEthToBtc,
    BaseUsdcToBtc,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeIngressBridge {
    Across,
    Cctp,
}

#[derive(Clone, Copy)]
struct RuntimeChainTokens {
    ethereum_reference_token: &'static str,
    base_reference_token: &'static str,
    arbitrum_reference_token: &'static str,
}

#[derive(Clone, Debug, Default)]
struct RuntimeTokenIndexerUrls {
    ethereum: Option<String>,
    base: Option<String>,
    arbitrum: Option<String>,
    api_key: Option<String>,
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

impl Drop for RuntimeTask {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown();
        }
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

#[derive(Clone)]
struct LiveRuntimeConfig {
    private_key: String,
    ethereum_rpc_url: String,
    base_rpc_url: String,
    arbitrum_rpc_url: String,
    bitcoin_rpc_url: String,
    bitcoin_rpc_auth: Auth,
    electrum_http_server_url: String,
    bitcoin_indexer_url: Option<String>,
    bitcoin_receipt_watcher_url: Option<String>,
    across_api_url: String,
    across_api_key: String,
    across_integrator_id: Option<String>,
    hyperunit_api_url: String,
    hyperunit_proxy_url: Option<String>,
    hyperliquid_api_url: String,
    hyperliquid_network: router_core::services::custody_action_executor::HyperliquidCallNetwork,
    hyperliquid_execution_private_key: Option<String>,
    hyperliquid_account_address: Option<String>,
    hyperliquid_vault_address: Option<String>,
    ethereum_paymaster_private_key: Option<String>,
    base_paymaster_private_key: Option<String>,
    arbitrum_paymaster_private_key: Option<String>,
    bitcoin_recipient_address: String,
    ethereum_token_indexer_url: Option<String>,
    base_token_indexer_url: Option<String>,
    arbitrum_token_indexer_url: Option<String>,
    token_indexer_api_key: Option<String>,
}

impl LiveRuntimeConfig {
    fn from_env() -> Self {
        load_repo_env_once();
        Self {
            private_key: env_var_any(&[
                ROUTER_LIVE_SOURCE_PRIVATE_KEY_ENV,
                LIVE_TEST_PRIVATE_KEY_ENV,
            ])
            .expect("live runtime requires a source private key"),
            ethereum_rpc_url: env_var_optional(ETH_RPC_URL_ENV)
                .expect("live runtime requires ETH_RPC_URL"),
            base_rpc_url: env_var_required(BASE_RPC_URL_ENV),
            arbitrum_rpc_url: env_var_required(ARBITRUM_RPC_URL_ENV),
            bitcoin_rpc_url: env_var_required(BITCOIN_RPC_URL_ENV),
            bitcoin_rpc_auth: parse_auth_env(BITCOIN_RPC_AUTH_ENV),
            electrum_http_server_url: env_var_required(ELECTRUM_HTTP_SERVER_URL_ENV),
            bitcoin_indexer_url: env_var_optional(BITCOIN_INDEXER_URL_ENV),
            bitcoin_receipt_watcher_url: env_var_optional(BITCOIN_RECEIPT_WATCHER_URL_ENV),
            across_api_url: env_var_optional(ACROSS_API_URL_ENV)
                .unwrap_or_else(|| DEFAULT_LIVE_ACROSS_API_URL.to_string()),
            across_api_key: env_var_required(ACROSS_API_KEY_ENV),
            across_integrator_id: env_var_optional(ACROSS_INTEGRATOR_ID_ENV),
            hyperunit_api_url: env_var_optional(HYPERUNIT_API_URL_ENV)
                .unwrap_or_else(|| DEFAULT_LIVE_HYPERUNIT_API_URL.to_string()),
            hyperunit_proxy_url: env_var_optional(HYPERUNIT_PROXY_URL_ENV),
            hyperliquid_api_url: env_var_optional(HYPERLIQUID_API_URL_ENV)
                .unwrap_or_else(|| DEFAULT_LIVE_HYPERLIQUID_API_URL.to_string()),
            hyperliquid_network: parse_hyperliquid_network(
                &env_var_optional(HYPERLIQUID_NETWORK_ENV).unwrap_or_else(|| "mainnet".to_string()),
            ),
            hyperliquid_execution_private_key: env_var_any(&[
                HYPERLIQUID_EXECUTION_PRIVATE_KEY_ENV,
                ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY_ENV,
            ]),
            hyperliquid_account_address: env_var_any(&[
                HYPERLIQUID_ACCOUNT_ADDRESS_ENV,
                ROUTER_LIVE_HYPERLIQUID_ACCOUNT_ADDRESS_ENV,
                ROUTER_LIVE_HYPERLIQUID_DESTINATION_ADDRESS_ENV,
            ]),
            hyperliquid_vault_address: env_var_any(&[
                HYPERLIQUID_VAULT_ADDRESS_ENV,
                ROUTER_LIVE_HYPERLIQUID_VAULT_ADDRESS_ENV,
            ]),
            ethereum_paymaster_private_key: env_var_optional(ETHEREUM_PAYMASTER_PRIVATE_KEY_ENV),
            base_paymaster_private_key: env_var_optional(BASE_PAYMASTER_PRIVATE_KEY_ENV),
            arbitrum_paymaster_private_key: env_var_optional(ARBITRUM_PAYMASTER_PRIVATE_KEY_ENV),
            bitcoin_recipient_address: env_var_optional(ROUTER_LIVE_BTC_RECIPIENT_ADDRESS_ENV)
                .or_else(|| env_var_optional(ROUTER_LIVE_WITHDRAW_RECIPIENT_ADDRESS_ENV))
                .expect("live runtime requires a BTC recipient address"),
            ethereum_token_indexer_url: env_var_optional(ETHEREUM_TOKEN_INDEXER_URL_ENV),
            base_token_indexer_url: env_var_optional(BASE_TOKEN_INDEXER_URL_ENV),
            arbitrum_token_indexer_url: env_var_optional(ARBITRUM_TOKEN_INDEXER_URL_ENV),
            token_indexer_api_key: env_var_optional(TOKEN_INDEXER_API_KEY_ENV),
        }
    }
}

fn load_repo_env_once() {
    static DOTENV: OnceLock<()> = OnceLock::new();
    DOTENV.get_or_init(|| {
        let _ = dotenvy::from_filename(".env");
    });
}

fn env_var_required(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("missing required env var {key}"))
}

fn env_var_optional(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| env_var_optional(key))
}

fn parse_auth_env(key: &str) -> Auth {
    let raw = env_var_required(key);
    if raw.eq_ignore_ascii_case("none") {
        Auth::None
    } else {
        let mut split = raw.splitn(2, ':');
        let username = split
            .next()
            .filter(|part| !part.is_empty())
            .unwrap_or_else(|| panic!("{key} must contain username:password"));
        let password = split
            .next()
            .filter(|part| !part.is_empty())
            .unwrap_or_else(|| panic!("{key} must contain username:password"));
        Auth::UserPass(username.to_string(), password.to_string())
    }
}

fn parse_hyperliquid_network(
    raw: &str,
) -> router_core::services::custody_action_executor::HyperliquidCallNetwork {
    use router_core::services::custody_action_executor::HyperliquidCallNetwork;

    match raw.trim().to_ascii_lowercase().as_str() {
        "mainnet" => HyperliquidCallNetwork::Mainnet,
        "testnet" => HyperliquidCallNetwork::Testnet,
        other => panic!("invalid {HYPERLIQUID_NETWORK_ENV} value {other:?}"),
    }
}

fn require_live_spend_confirmation() {
    let confirmation = env_var_required(LIVE_SPEND_CONFIRM_ENV);
    assert_eq!(
        confirmation, LIVE_SPEND_CONFIRM_VALUE,
        "refusing to run a live Soron runtime route without {LIVE_SPEND_CONFIRM_ENV}={LIVE_SPEND_CONFIRM_VALUE}"
    );
}

struct LiveRecoverySnapshotSpec<'a> {
    label: &'a str,
    route: RuntimeRoute,
    live: &'a LiveRuntimeConfig,
    database_url: &'a str,
    config_dir: &'a Path,
    db: &'a Database,
    chain_registry: &'a ChainRegistry,
    order_id: Option<Uuid>,
    quote_id: Option<Uuid>,
    extra: Value,
}

async fn write_live_recovery_snapshot(spec: LiveRecoverySnapshotSpec<'_>) {
    match try_write_live_recovery_snapshot(spec).await {
        Ok(path) => eprintln!("live recovery snapshot written to {}", path.display()),
        Err(error) => eprintln!("failed to write live recovery snapshot: {error}"),
    }
}

async fn try_write_live_recovery_snapshot(
    spec: LiveRecoverySnapshotSpec<'_>,
) -> Result<PathBuf, Box<dyn Error + Send + Sync>> {
    let LiveRecoverySnapshotSpec {
        label,
        route,
        live,
        database_url,
        config_dir,
        db,
        chain_registry,
        order_id,
        quote_id,
        extra,
    } = spec;
    let master_key_hex = read_router_master_key_hex(config_dir);
    let master_key_bytes = master_key_hex
        .as_deref()
        .and_then(decode_router_master_key_hex);
    let source_address = live
        .private_key
        .parse::<PrivateKeySigner>()
        .map(|signer| format!("{:#x}", signer.address()))
        .unwrap_or_else(|error| format!("invalid live source private key: {error}"));
    let order_context = collect_order_recovery_context(
        db,
        chain_registry,
        master_key_bytes.as_ref(),
        order_id,
        quote_id,
    )
    .await;
    let database_snapshot = collect_live_database_snapshot(database_url).await;
    let created_at = chrono::Utc::now();
    let snapshot = json!({
        "schema_version": 1,
        "kind": "router_live_test_recovery_snapshot",
        "label": label,
        "created_at": created_at,
        "route": route_order_metadata(route),
        "order_id": order_id,
        "quote_id": quote_id,
        "database": {
            "url_redacted": redact_database_url(database_url),
            "snapshot": database_snapshot,
        },
        "router_key_material": {
            "config_dir": config_dir.display().to_string(),
            "master_key_hex": master_key_hex,
            "master_key_available": master_key_bytes.is_some(),
            "note": "master_key_hex plus each salt can recreate router-derived vault keys; derived_private_key entries are included for immediate recovery."
        },
        "live_context": {
            "source_address": source_address,
            "source_private_key_env_candidates": [
                ROUTER_LIVE_SOURCE_PRIVATE_KEY_ENV,
                LIVE_TEST_PRIVATE_KEY_ENV
            ],
            "paymaster_private_key_envs": {
                "ethereum": ETHEREUM_PAYMASTER_PRIVATE_KEY_ENV,
                "base": BASE_PAYMASTER_PRIVATE_KEY_ENV,
                "arbitrum": ARBITRUM_PAYMASTER_PRIVATE_KEY_ENV,
            },
            "configured_private_keys_present": {
                "ethereum_paymaster": live.ethereum_paymaster_private_key.is_some(),
                "base_paymaster": live.base_paymaster_private_key.is_some(),
                "arbitrum_paymaster": live.arbitrum_paymaster_private_key.is_some(),
                "hyperliquid_execution": live.hyperliquid_execution_private_key.is_some(),
            },
            "hyperliquid": {
                "api_url": &live.hyperliquid_api_url,
                "network": format!("{:?}", live.hyperliquid_network),
                "execution_private_key_env_candidates": [
                    HYPERLIQUID_EXECUTION_PRIVATE_KEY_ENV,
                    ROUTER_LIVE_HYPERLIQUID_PRIVATE_KEY_ENV
                ],
                "account_address": &live.hyperliquid_account_address,
                "vault_address": &live.hyperliquid_vault_address,
            },
            "providers": {
                "across_api_url": &live.across_api_url,
                "across_api_key_configured": !live.across_api_key.trim().is_empty(),
                "across_integrator_id": &live.across_integrator_id,
                "hyperunit_api_url": &live.hyperunit_api_url,
                "hyperunit_proxy_configured": live.hyperunit_proxy_url.is_some(),
            },
            "btc_recipient_address": &live.bitcoin_recipient_address,
        },
        "order_context": order_context,
        "extra": extra,
    });

    let dir = live_recovery_dir();
    std::fs::create_dir_all(&dir)?;
    let file_name = format!(
        "{}-{}-{}-{}.json",
        created_at.format("%Y%m%dT%H%M%S%.3fZ"),
        route_order_metadata(route),
        sanitize_snapshot_label(label),
        order_id
            .map(|id| id.simple().to_string())
            .or_else(|| quote_id.map(|id| id.simple().to_string()))
            .unwrap_or_else(|| "no-id".to_string())
    );
    let path = dir.join(file_name);
    std::fs::write(&path, serde_json::to_vec_pretty(&snapshot)?)?;
    Ok(path)
}

async fn collect_order_recovery_context(
    db: &Database,
    chain_registry: &ChainRegistry,
    master_key: Option<&[u8; 64]>,
    order_id: Option<Uuid>,
    quote_id: Option<Uuid>,
) -> Value {
    let mut derived_wallets = Vec::new();
    let mut order_value = Value::Null;
    let mut quote_value = Value::Null;
    let mut funding_vault_value = Value::Null;
    let mut custody_vaults_value = Value::Null;
    let mut execution_steps_value = Value::Null;
    let mut provider_operations_value = Value::Null;
    let mut provider_addresses_value = Value::Null;

    if let Some(order_id) = order_id {
        match db.orders().get(order_id).await {
            Ok(order) => {
                if let Some(vault_id) = order.funding_vault_id {
                    match db.vaults().get(vault_id).await {
                        Ok(vault) => {
                            if let Some(master_key) = master_key {
                                derived_wallets.push(derive_recovery_wallet_snapshot(
                                    chain_registry,
                                    master_key,
                                    &vault.deposit_asset.chain,
                                    &vault.deposit_vault_salt,
                                    &vault.deposit_vault_address,
                                    json!({
                                        "kind": "funding_deposit_vault",
                                        "vault_id": vault.id,
                                        "order_id": vault.order_id,
                                        "asset": &vault.deposit_asset,
                                    }),
                                ));
                            }
                            funding_vault_value = json_value(vault);
                        }
                        Err(error) => {
                            funding_vault_value =
                                json!({"error": format!("failed to load funding vault: {error}")});
                        }
                    }
                }
                order_value = json_value(order);
            }
            Err(error) => {
                order_value = json!({"error": format!("failed to load order: {error}")});
            }
        }

        match db.orders().get_market_order_quote(order_id).await {
            Ok(quote) => {
                if let Some(master_key) = master_key {
                    let salt = derive_deposit_salt_for_quote(master_key, quote.id);
                    derived_wallets.push(derive_recovery_wallet_snapshot(
                        chain_registry,
                        master_key,
                        &quote.source_asset.chain,
                        &salt,
                        "",
                        json!({
                            "kind": "quote_source_deposit_address",
                            "quote_id": quote.id,
                            "order_id": quote.order_id,
                            "asset": &quote.source_asset,
                        }),
                    ));
                }
                quote_value = json_value(quote);
            }
            Err(error) => {
                quote_value = json!({"error": format!("failed to load quote by order: {error}")});
            }
        }

        match db.orders().get_custody_vaults(order_id).await {
            Ok(vaults) => {
                if let Some(master_key) = master_key {
                    for vault in vaults.iter().filter(|vault| {
                        vault.control_type == CustodyVaultControlType::RouterDerivedKey
                    }) {
                        if let Some(salt) = vault.derivation_salt {
                            derived_wallets.push(derive_recovery_wallet_snapshot(
                                chain_registry,
                                master_key,
                                &vault.chain,
                                &salt,
                                &vault.address,
                                json!({
                                    "kind": "custody_vault",
                                    "vault_id": vault.id,
                                    "order_id": vault.order_id,
                                    "role": vault.role,
                                    "asset": &vault.asset,
                                }),
                            ));
                        }
                    }
                }
                custody_vaults_value = json_value(vaults);
            }
            Err(error) => {
                custody_vaults_value =
                    json!({"error": format!("failed to load custody vaults: {error}")});
            }
        }

        execution_steps_value = json_result(db.orders().get_execution_steps(order_id).await);
        provider_operations_value =
            json_result(db.orders().get_provider_operations(order_id).await);
        provider_addresses_value = json_result(db.orders().get_provider_addresses(order_id).await);
    } else if let Some(quote_id) = quote_id {
        match db.orders().get_market_order_quote_by_id(quote_id).await {
            Ok(quote) => {
                if let Some(master_key) = master_key {
                    let salt = derive_deposit_salt_for_quote(master_key, quote.id);
                    derived_wallets.push(derive_recovery_wallet_snapshot(
                        chain_registry,
                        master_key,
                        &quote.source_asset.chain,
                        &salt,
                        "",
                        json!({
                            "kind": "quote_source_deposit_address",
                            "quote_id": quote.id,
                            "order_id": quote.order_id,
                            "asset": &quote.source_asset,
                        }),
                    ));
                }
                quote_value = json_value(quote);
            }
            Err(error) => {
                quote_value = json!({"error": format!("failed to load quote by id: {error}")});
            }
        }
    }

    json!({
        "order": order_value,
        "quote": quote_value,
        "funding_vault": funding_vault_value,
        "custody_vaults": custody_vaults_value,
        "execution_steps": execution_steps_value,
        "provider_operations": provider_operations_value,
        "provider_addresses": provider_addresses_value,
        "derived_wallets": derived_wallets,
    })
}

fn derive_recovery_wallet_snapshot(
    chain_registry: &ChainRegistry,
    master_key: &[u8; 64],
    chain_id: &ChainId,
    salt: &[u8; 32],
    expected_address: &str,
    context: Value,
) -> Value {
    let Some(backend_chain) = backend_chain_for_id(chain_id) else {
        return json!({
            "context": context,
            "chain": chain_id,
            "salt_hex": format!("0x{}", alloy::hex::encode(salt)),
            "error": "unsupported chain",
        });
    };
    let Some(chain) = chain_registry.get(&backend_chain) else {
        return json!({
            "context": context,
            "chain": chain_id,
            "salt_hex": format!("0x{}", alloy::hex::encode(salt)),
            "error": "chain is not registered",
        });
    };
    match chain.derive_wallet(master_key, salt) {
        Ok(wallet) => {
            let address = wallet.address.clone();
            let address_matches_expected =
                expected_address.is_empty() || address.eq_ignore_ascii_case(expected_address);
            json!({
            "context": context,
            "chain": chain_id,
            "salt_hex": format!("0x{}", alloy::hex::encode(salt)),
            "address": address,
            "expected_address": expected_address,
            "address_matches_expected": address_matches_expected,
            "private_key": wallet.private_key(),
            })
        }
        Err(error) => json!({
            "context": context,
            "chain": chain_id,
            "salt_hex": format!("0x{}", alloy::hex::encode(salt)),
            "error": format!("failed to derive wallet: {error}"),
        }),
    }
}

async fn collect_live_database_snapshot(database_url: &str) -> Value {
    let pool = match PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect(database_url)
        .await
    {
        Ok(pool) => pool,
        Err(error) => return json!({"error": format!("failed to connect for snapshot: {error}")}),
    };

    let mut tables = serde_json::Map::new();
    for (table, order_by) in LIVE_RECOVERY_TABLES {
        let query = format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(t)), '[]'::jsonb) \
             FROM (SELECT * FROM {table} ORDER BY {order_by}) AS t"
        );
        let value = match sqlx_core::query_scalar::query_scalar::<_, Value>(&query)
            .fetch_one(&pool)
            .await
        {
            Ok(value) => value,
            Err(error) => json!({"error": error.to_string()}),
        };
        tables.insert((*table).to_string(), value);
    }

    Value::Object(tables)
}

fn json_result<T, E>(result: Result<T, E>) -> Value
where
    T: serde::Serialize,
    E: std::fmt::Display,
{
    match result {
        Ok(value) => serde_json::to_value(value)
            .unwrap_or_else(|error| json!({"serialization_error": error.to_string()})),
        Err(error) => json!({"error": error.to_string()}),
    }
}

fn json_value<T>(value: T) -> Value
where
    T: serde::Serialize,
{
    serde_json::to_value(value)
        .unwrap_or_else(|error| json!({"serialization_error": error.to_string()}))
}

fn live_recovery_dir() -> PathBuf {
    env_var_optional(LIVE_RECOVERY_DIR_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/live-recovery")
        })
}

fn read_router_master_key_hex(config_dir: &Path) -> Option<String> {
    std::fs::read_to_string(config_dir.join("router-server-master-key.hex"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn write_router_master_key(config_dir: &Path) -> PathBuf {
    let path = config_dir.join("router-server-master-key.hex");
    if !path.exists() {
        std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    }
    path
}

fn decode_router_master_key_hex(master_key_hex: &str) -> Option<[u8; 64]> {
    let decoded = alloy::hex::decode(master_key_hex).ok()?;
    decoded.try_into().ok()
}

fn redact_database_url(database_url: &str) -> String {
    let Ok(mut url) = url::Url::parse(database_url) else {
        return "<invalid database url>".to_string();
    };
    if url.password().is_some() {
        let _ = url.set_password(Some("REDACTED"));
    }
    url.to_string()
}

fn sanitize_snapshot_label(label: &str) -> String {
    label
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect()
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
    let database_name = format!("sauron_e2e_{}", Uuid::now_v7().simple());

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
    wait_until("router API status", Duration::from_secs(20), || {
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

async fn spawn_temporal_order_worker(args: &RouterServerArgs) -> RuntimeTask {
    let runtime_args = order_worker_runtime_args_from_router_args(args);
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
    assert!(
        !handle.is_finished(),
        "temporal order worker exited during startup"
    );
    RuntimeTask::with_shutdown(handle, shutdown)
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

fn route_chain_tokens(route: RuntimeRoute) -> RuntimeChainTokens {
    match route {
        RuntimeRoute::BaseEthToBtc => RuntimeChainTokens {
            ethereum_reference_token: MOCK_ERC20_ADDRESS,
            base_reference_token: MOCK_ERC20_ADDRESS,
            arbitrum_reference_token: MOCK_ERC20_ADDRESS,
        },
        RuntimeRoute::BaseUsdcToBtc => RuntimeChainTokens {
            ethereum_reference_token: MOCK_ERC20_ADDRESS,
            base_reference_token: BASE_USDC_ADDRESS,
            arbitrum_reference_token: ARBITRUM_USDC_ADDRESS,
        },
    }
}

fn live_route_chain_tokens(route: RuntimeRoute) -> RuntimeChainTokens {
    match route {
        RuntimeRoute::BaseEthToBtc => RuntimeChainTokens {
            ethereum_reference_token: ZERO_ADDRESS,
            base_reference_token: ZERO_ADDRESS,
            arbitrum_reference_token: ZERO_ADDRESS,
        },
        RuntimeRoute::BaseUsdcToBtc => RuntimeChainTokens {
            ethereum_reference_token: ZERO_ADDRESS,
            base_reference_token: BASE_USDC_ADDRESS,
            arbitrum_reference_token: ARBITRUM_USDC_ADDRESS,
        },
    }
}

fn router_args(
    devnet: &RiftDevnet,
    config_dir: &std::path::Path,
    database_url: String,
    chain_tokens: RuntimeChainTokens,
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
        ethereum_reference_token: chain_tokens.ethereum_reference_token.to_string(),
        ethereum_paymaster_private_key: Some(anvil_private_key(&devnet.ethereum)),
        base_rpc_url: devnet.base.anvil.endpoint_url().to_string(),
        base_reference_token: chain_tokens.base_reference_token.to_string(),
        base_paymaster_private_key: Some(anvil_private_key(&devnet.base)),
        arbitrum_rpc_url: devnet.arbitrum.anvil.endpoint_url().to_string(),
        arbitrum_reference_token: chain_tokens.arbitrum_reference_token.to_string(),
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
        across_api_key: Some("mock-across-api-key".to_string()),
        across_integrator_id: None,
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
        velora_api_url: None,
        velora_partner: None,
        hyperliquid_execution_private_key: None,
        hyperliquid_account_address: None,
        hyperliquid_vault_address: None,
        hyperliquid_paymaster_private_key: Some(test_hyperliquid_paymaster_private_key()),
        temporal_address: "http://127.0.0.1:7233".to_string(),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("tee-router-order-execution-{}", Uuid::now_v7()),
        router_detector_api_key: Some(ROUTER_DETECTOR_API_KEY.to_string()),
        router_gateway_api_key: None,
        router_admin_api_key: None,
        hyperliquid_network:
            router_core::services::custody_action_executor::HyperliquidCallNetwork::Mainnet,
        hyperliquid_order_timeout_ms: 30_000,
        worker_id: Some(format!("router-worker-{}", Uuid::now_v7())),
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

fn live_router_args(
    config_dir: &std::path::Path,
    database_url: String,
    chain_tokens: RuntimeChainTokens,
    live: &LiveRuntimeConfig,
) -> RouterServerArgs {
    RouterServerArgs {
        host: IpAddr::from([127, 0, 0, 1]),
        port: 0,
        database_url,
        db_max_connections: 8,
        db_min_connections: 1,
        log_level: "info".to_string(),
        master_key_path: write_router_master_key(config_dir)
            .to_string_lossy()
            .to_string(),
        ethereum_mainnet_rpc_url: live.ethereum_rpc_url.clone(),
        ethereum_reference_token: chain_tokens.ethereum_reference_token.to_string(),
        ethereum_paymaster_private_key: live.ethereum_paymaster_private_key.clone(),
        base_rpc_url: live.base_rpc_url.clone(),
        base_reference_token: chain_tokens.base_reference_token.to_string(),
        base_paymaster_private_key: live.base_paymaster_private_key.clone(),
        arbitrum_rpc_url: live.arbitrum_rpc_url.clone(),
        arbitrum_reference_token: chain_tokens.arbitrum_reference_token.to_string(),
        arbitrum_paymaster_private_key: live.arbitrum_paymaster_private_key.clone(),
        evm_paymaster_vault_gas_buffer_wei: "100000000000000".to_string(),
        evm_paymaster_vault_gas_target_wei: None,
        bitcoin_rpc_url: live.bitcoin_rpc_url.clone(),
        bitcoin_rpc_auth: live.bitcoin_rpc_auth.clone(),
        untrusted_esplora_http_server_url: live.electrum_http_server_url.clone(),
        bitcoin_network: bitcoin::Network::Bitcoin,
        bitcoin_paymaster_private_key: None,
        cors_domain: None,
        chainalysis_host: None,
        chainalysis_token: None,
        loki_url: None,
        across_api_url: Some(live.across_api_url.clone()),
        across_api_key: Some(live.across_api_key.clone()),
        across_integrator_id: live.across_integrator_id.clone(),
        cctp_api_url: None,
        cctp_token_messenger_v2_address: None,
        cctp_message_transmitter_v2_address: None,
        hyperunit_api_url: Some(live.hyperunit_api_url.clone()),
        hyperunit_proxy_url: live.hyperunit_proxy_url.clone(),
        hyperliquid_api_url: Some(live.hyperliquid_api_url.clone()),
        velora_api_url: None,
        velora_partner: None,
        hyperliquid_execution_private_key: live.hyperliquid_execution_private_key.clone(),
        hyperliquid_account_address: live.hyperliquid_account_address.clone(),
        hyperliquid_vault_address: live.hyperliquid_vault_address.clone(),
        hyperliquid_paymaster_private_key: None,
        temporal_address: "http://127.0.0.1:7233".to_string(),
        temporal_namespace: "default".to_string(),
        temporal_task_queue: format!("tee-router-order-execution-{}", Uuid::now_v7()),
        router_detector_api_key: Some(ROUTER_DETECTOR_API_KEY.to_string()),
        router_gateway_api_key: None,
        router_admin_api_key: None,
        hyperliquid_network: live.hyperliquid_network,
        hyperliquid_order_timeout_ms: 30_000,
        worker_id: Some(format!("router-worker-{}", Uuid::now_v7())),
        worker_refund_poll_seconds: 5,
        worker_order_execution_poll_seconds: 1,
        worker_route_cost_refresh_seconds: 300,
        worker_provider_health_poll_seconds: 120,
        provider_health_timeout_seconds: 10,
        worker_order_maintenance_pass_limit: 100,
        worker_order_planning_pass_limit: 100,
        worker_order_execution_pass_limit: 25,
        worker_order_execution_concurrency: 64,
        worker_vault_funding_hint_pass_limit: 100,
        coinbase_price_api_base_url: "https://api.coinbase.com".to_string(),
    }
}

async fn spawn_sauron(
    devnet: &RiftDevnet,
    database_url: &str,
    state_database_url: &str,
    router_base_url: &str,
    token_indexer_urls: RuntimeTokenIndexerUrls,
    receipt_watcher_urls: EvmReceiptWatcherUrls,
    hl_shim_indexer_url: Option<String>,
    bitcoin_observer_urls: Option<BitcoinObserverUrls>,
    hyperunit_api_url: Option<String>,
) -> RuntimeTask {
    let args = SauronArgs {
        log_level: "warn".to_string(),
        router_replica_database_url: database_url.to_string(),
        sauron_state_database_url: state_database_url.to_string(),
        sauron_replica_event_source: sauron::config::SauronReplicaEventSource::Cdc,
        router_replica_database_name: "router_db".to_string(),
        sauron_cdc_slot_name: "sauron_watch_cdc".to_string(),
        router_cdc_publication_name: "router_cdc_publication".to_string(),
        router_cdc_message_prefix: "rift.router.change".to_string(),
        sauron_cdc_status_interval_ms: 1000,
        sauron_cdc_idle_wakeup_interval_ms: 10_000,
        router_internal_base_url: router_base_url.to_string(),
        router_detector_api_key: ROUTER_DETECTOR_API_KEY.to_string(),
        bitcoin_indexer_url: bitcoin_observer_urls
            .as_ref()
            .map(|urls| urls.indexer.clone()),
        bitcoin_receipt_watcher_url: bitcoin_observer_urls
            .as_ref()
            .map(|urls| urls.receipt_watcher.clone()),
        ethereum_mainnet_rpc_url: devnet.ethereum.anvil.endpoint_url().to_string(),
        ethereum_token_indexer_url: token_indexer_urls.ethereum,
        ethereum_receipt_watcher_url: Some(receipt_watcher_urls.ethereum),
        base_rpc_url: devnet.base.anvil.endpoint_url().to_string(),
        base_token_indexer_url: token_indexer_urls.base,
        base_receipt_watcher_url: Some(receipt_watcher_urls.base),
        arbitrum_rpc_url: devnet.arbitrum.anvil.endpoint_url().to_string(),
        arbitrum_token_indexer_url: token_indexer_urls.arbitrum,
        arbitrum_receipt_watcher_url: Some(receipt_watcher_urls.arbitrum),
        hl_shim_indexer_url,
        hyperunit_api_url,
        hyperunit_proxy_url: None,
        sauron_hl_bridge_match_window_seconds: 1_800,
        token_indexer_api_key: token_indexer_urls.api_key,
        sauron_reconcile_interval_seconds: 2,
        sauron_bitcoin_scan_interval_seconds: 60,
        sauron_bitcoin_indexed_lookup_concurrency: 1,
        sauron_evm_indexed_lookup_concurrency: 1,
    };
    let handle = tokio::spawn(async move {
        run_sauron(args)
            .await
            .expect("Sauron should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    if handle.is_finished() {
        handle
            .await
            .expect("Sauron task should not panic before startup");
        panic!("Sauron exited during startup");
    }
    RuntimeTask::new(handle)
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

fn token_indexer_urls_from_devnet(devnet: &RiftDevnet) -> RuntimeTokenIndexerUrls {
    RuntimeTokenIndexerUrls {
        ethereum: devnet
            .ethereum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        base: devnet
            .base
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        arbitrum: devnet
            .arbitrum
            .token_indexer
            .as_ref()
            .map(|indexer| indexer.api_server_url.clone()),
        api_key: [
            devnet.ethereum.token_indexer.as_ref(),
            devnet.base.token_indexer.as_ref(),
            devnet.arbitrum.token_indexer.as_ref(),
        ]
        .into_iter()
        .flatten()
        .map(|indexer| indexer.api_key.clone())
        .next(),
    }
}

async fn spawn_live_sauron(
    database_url: &str,
    state_database_url: &str,
    router_base_url: &str,
    live: &LiveRuntimeConfig,
) -> RuntimeTask {
    let args = live_sauron_args(database_url, state_database_url, router_base_url, live);
    let handle = tokio::spawn(async move {
        run_sauron(args)
            .await
            .expect("live Sauron should keep running until test aborts");
    });
    tokio::time::sleep(Duration::from_secs(2)).await;
    if handle.is_finished() {
        handle
            .await
            .expect("live Sauron task should not panic before startup");
        panic!("live Sauron exited during startup");
    }
    RuntimeTask::new(handle)
}

fn live_sauron_args(
    database_url: &str,
    state_database_url: &str,
    router_base_url: &str,
    live: &LiveRuntimeConfig,
) -> SauronArgs {
    SauronArgs {
        log_level: "info".to_string(),
        router_replica_database_url: database_url.to_string(),
        sauron_state_database_url: state_database_url.to_string(),
        sauron_replica_event_source: sauron::config::SauronReplicaEventSource::Cdc,
        router_replica_database_name: "router_db".to_string(),
        sauron_cdc_slot_name: "sauron_watch_cdc".to_string(),
        router_cdc_publication_name: "router_cdc_publication".to_string(),
        router_cdc_message_prefix: "rift.router.change".to_string(),
        sauron_cdc_status_interval_ms: 1000,
        sauron_cdc_idle_wakeup_interval_ms: 10_000,
        router_internal_base_url: router_base_url.to_string(),
        router_detector_api_key: ROUTER_DETECTOR_API_KEY.to_string(),
        bitcoin_indexer_url: live.bitcoin_indexer_url.clone(),
        bitcoin_receipt_watcher_url: live.bitcoin_receipt_watcher_url.clone(),
        ethereum_mainnet_rpc_url: live.ethereum_rpc_url.clone(),
        ethereum_token_indexer_url: live.ethereum_token_indexer_url.clone(),
        ethereum_receipt_watcher_url: None,
        base_rpc_url: live.base_rpc_url.clone(),
        base_token_indexer_url: live.base_token_indexer_url.clone(),
        base_receipt_watcher_url: None,
        arbitrum_rpc_url: live.arbitrum_rpc_url.clone(),
        arbitrum_token_indexer_url: live.arbitrum_token_indexer_url.clone(),
        arbitrum_receipt_watcher_url: None,
        hl_shim_indexer_url: None,
        hyperunit_api_url: Some(live.hyperunit_api_url.clone()),
        hyperunit_proxy_url: live.hyperunit_proxy_url.clone(),
        sauron_hl_bridge_match_window_seconds: 1_800,
        token_indexer_api_key: live.token_indexer_api_key.clone(),
        sauron_reconcile_interval_seconds: 30,
        sauron_bitcoin_scan_interval_seconds: 15,
        sauron_bitcoin_indexed_lookup_concurrency: 1,
        sauron_evm_indexed_lookup_concurrency: 1,
    }
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

fn boxed_error<E>(error: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(error)
}

async fn retry_live_rpc<T, Fut, Op>(
    label: &str,
    mut op: Op,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error + Send + Sync>>>,
{
    let mut delay = LIVE_RPC_RETRY_INITIAL_DELAY;
    for attempt in 1..=LIVE_RPC_RETRY_ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error)
                if attempt < LIVE_RPC_RETRY_ATTEMPTS
                    && is_retryable_live_rpc_error(error.as_ref()) =>
            {
                eprintln!(
                    "retrying live RPC {label} after attempt {attempt}/{LIVE_RPC_RETRY_ATTEMPTS}: {error}"
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("live RPC retry loop always returns before exhausting attempts")
}

async fn wait_for_live_receipt<P>(provider: &P, tx_hash: alloy::primitives::B256, label: &str)
where
    P: Provider,
{
    for _ in 0..120 {
        let receipt = retry_live_rpc(&format!("{label} receipt"), || async {
            provider
                .get_transaction_receipt(tx_hash)
                .await
                .map_err(boxed_error)
        })
        .await
        .expect("live receipt lookup");

        if let Some(receipt) = receipt {
            assert!(receipt.status(), "{label} transaction {tx_hash} reverted");
            return;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    panic!("{label} transaction {tx_hash} was not mined before timeout");
}

fn is_retryable_live_rpc_error(error: &(dyn Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        let message = error.to_string().to_ascii_lowercase();
        if message.contains("429")
            || message.contains("rate limit")
            || message.contains("rate-limited")
            || message.contains("too many requests")
            || message.contains("timeout")
            || message.contains("timed out")
            || message.contains("temporarily unavailable")
            || message.contains("temporary internal error")
            || message.contains("connection reset")
            || message.contains("connection closed")
            || message.contains("connection refused")
            || message.contains("500")
            || message.contains("502")
            || message.contains("503")
            || message.contains("504")
        {
            return true;
        }
        current = error.source();
    }
    false
}

async fn wait_for_operation(
    db: &Database,
    order_id: Uuid,
    operation_type: ProviderOperationType,
) -> OrderProviderOperation {
    let started = Instant::now();
    loop {
        let operations = db
            .orders()
            .get_provider_operations(order_id)
            .await
            .expect("load provider operations");
        if let Some(operation) = operations
            .into_iter()
            .find(|operation| operation.operation_type == operation_type)
        {
            return operation;
        }
        if started.elapsed() >= Duration::from_secs(30) {
            dump_order_state(db, order_id).await;
            panic!("timed out waiting for {}", operation_type.to_db_string());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn dump_order_state(db: &Database, order_id: Uuid) {
    let steps = db
        .orders()
        .get_execution_steps(order_id)
        .await
        .expect("load execution steps");
    for step in &steps {
        eprintln!(
            "step #{} type={:?} status={:?} error={}",
            step.step_index, step.step_type, step.status, step.error
        );
    }
    let operations = db
        .orders()
        .get_provider_operations(order_id)
        .await
        .expect("load provider operations");
    for operation in &operations {
        eprintln!(
            "operation type={:?} status={:?} provider_ref={:?} request={} response={} observed_state={}",
            operation.operation_type,
            operation.status,
            operation.provider_ref,
            operation.request,
            operation.response,
            operation.observed_state
        );
    }
}

async fn wait_for_operation_status(
    db: &Database,
    operation_id: Uuid,
    status: ProviderOperationStatus,
) -> OrderProviderOperation {
    wait_for_operation_status_with_timeout(db, operation_id, status, Duration::from_secs(45)).await
}

async fn wait_for_operation_status_with_timeout(
    db: &Database,
    operation_id: Uuid,
    status: ProviderOperationStatus,
    timeout: Duration,
) -> OrderProviderOperation {
    wait_until(status.to_db_string(), timeout, || async {
        let operation = db
            .orders()
            .get_provider_operation(operation_id)
            .await
            .expect("load provider operation");
        (operation.status == status).then_some(operation)
    })
    .await
}

async fn wait_for_order_status(
    db: &Database,
    order_id: Uuid,
    status: RouterOrderStatus,
) -> RouterOrderStatus {
    wait_for_order_status_with_timeout(db, order_id, status, Duration::from_secs(30)).await
}

async fn wait_for_order_status_with_timeout(
    db: &Database,
    order_id: Uuid,
    status: RouterOrderStatus,
    timeout: Duration,
) -> RouterOrderStatus {
    wait_until(status.to_db_string(), timeout, || async {
        let order = db.orders().get(order_id).await.expect("load router order");
        (order.status == status).then_some(order.status)
    })
    .await
}

async fn wait_for_terminal_order_status(
    db: &Database,
    order_id: Uuid,
    timeout: Duration,
) -> RouterOrderStatus {
    wait_until("terminal router order status", timeout, || async {
        let order = db.orders().get(order_id).await.expect("load router order");
        matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::Expired
                | RouterOrderStatus::RefundRequired
                | RouterOrderStatus::Refunding
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::ManualInterventionRequired
                | RouterOrderStatus::RefundManualInterventionRequired
        )
        .then_some(order.status)
    })
    .await
}

async fn mint_erc20(
    devnet: &devnet::EthDevnet,
    token_address: Address,
    recipient: Address,
    amount: U256,
) {
    let signer = anvil_private_key(devnet)
        .parse::<alloy::signers::local::PrivateKeySigner>()
        .expect("valid anvil private key");
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
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
    assert!(receipt.status(), "erc20 mint reverted");
}

async fn install_mock_usdc_clone(devnet: &devnet::EthDevnet, token_address: Address) {
    let signer = anvil_private_key(devnet)
        .parse::<alloy::signers::local::PrivateKeySigner>()
        .expect("valid anvil private key");
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(devnet.anvil.endpoint_url())
        .erased();
    let implementation_code = provider
        .get_code_at(*devnet.mock_erc20_contract.address())
        .await
        .expect("load mock erc20 bytecode");
    provider
        .anvil_set_code(token_address, implementation_code)
        .await
        .expect("install mock erc20 bytecode at canonical address");

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
        .expect("send mock usdc initialize")
        .get_receipt()
        .await
        .expect("mock usdc initialize receipt");
    token
        .configureMinter(admin, U256::MAX)
        .send()
        .await
        .expect("send mock usdc configureMinter")
        .get_receipt()
        .await
        .expect("mock usdc configureMinter receipt");
}

async fn prepare_route_chain_state(devnet: &RiftDevnet, route: RuntimeRoute) {
    if !matches!(route, RuntimeRoute::BaseUsdcToBtc) {
        return;
    }

    install_mock_usdc_clone(
        &devnet.base,
        BASE_USDC_ADDRESS.parse().expect("valid base usdc address"),
    )
    .await;
    install_mock_usdc_clone(
        &devnet.arbitrum,
        ARBITRUM_USDC_ADDRESS
            .parse()
            .expect("valid arbitrum usdc address"),
    )
    .await;
}

async fn send_native(devnet: &devnet::EthDevnet, recipient: Address, amount: U256) {
    let signer = anvil_private_key(devnet)
        .parse::<alloy::signers::local::PrivateKeySigner>()
        .expect("valid anvil private key");
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
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
        .expect("mine evm confirmation block");
}

async fn send_live_native(
    rpc_url: &str,
    private_key: &str,
    recipient: Address,
    amount: U256,
) -> String {
    let signer = private_key
        .parse::<PrivateKeySigner>()
        .expect("valid live private key");
    let sender = signer.address();
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse().expect("valid live rpc url"));
    let sender_balance = retry_live_rpc("read live native balance", || async {
        provider.get_balance(sender).await.map_err(boxed_error)
    })
    .await
    .expect("read live native balance");
    assert!(
        sender_balance >= amount,
        "live funding wallet {sender:#x} has insufficient native balance on {rpc_url}; need {} wei, have {} wei",
        amount,
        sender_balance
    );
    let tx = TransactionRequest::default()
        .with_to(recipient)
        .with_value(amount);
    let pending = retry_live_rpc("send live native transfer", || async {
        provider
            .send_transaction(tx.clone())
            .await
            .map_err(boxed_error)
    })
    .await
    .expect("send live native transfer");
    let tx_hash = pending.tx_hash().to_string();
    wait_for_live_receipt(&provider, *pending.tx_hash(), "live native transfer").await;
    tx_hash
}

async fn send_live_erc20(
    rpc_url: &str,
    private_key: &str,
    token_address: Address,
    recipient: Address,
    amount: U256,
) -> String {
    let signer = private_key
        .parse::<PrivateKeySigner>()
        .expect("valid live private key");
    let sender = signer.address();
    let wallet = EthereumWallet::new(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse().expect("valid live rpc url"));
    let token = IERC20::new(token_address, provider.clone());
    let token_balance = retry_live_rpc("read live erc20 balance", || async {
        token.balanceOf(sender).call().await.map_err(boxed_error)
    })
    .await
    .expect("read live erc20 balance");
    assert!(
        token_balance >= amount,
        "live funding wallet {sender:#x} has insufficient ERC20 balance for token {token_address:#x} on {rpc_url}; need {}, have {}",
        amount,
        token_balance
    );
    let pending = retry_live_rpc("send live erc20 transfer", || async {
        token
            .transfer(recipient, amount)
            .send()
            .await
            .map_err(boxed_error)
    })
    .await
    .expect("send live erc20 transfer");
    let tx_hash = pending.tx_hash().to_string();
    wait_for_live_receipt(&provider, *pending.tx_hash(), "live erc20 transfer").await;
    tx_hash
}

fn anvil_private_key(devnet: &devnet::EthDevnet) -> String {
    format!(
        "0x{}",
        alloy::hex::encode(devnet.anvil.keys()[0].to_bytes())
    )
}

fn evm_address_from_private_key(private_key: &str) -> Address {
    private_key
        .parse::<PrivateKeySigner>()
        .expect("valid EVM private key")
        .address()
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
    "0x0000000000000000000000000000000000000001".to_string()
}

fn valid_regtest_btc_address() -> String {
    "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string()
}

fn route_amount_in(route: RuntimeRoute) -> &'static str {
    match route {
        RuntimeRoute::BaseEthToBtc => "60000000000000000",
        RuntimeRoute::BaseUsdcToBtc => "60000000",
    }
}

fn live_route_amount_in(route: RuntimeRoute) -> String {
    match route {
        RuntimeRoute::BaseEthToBtc => env_var_optional(LIVE_BASE_ETH_AMOUNT_WEI_ENV)
            .unwrap_or_else(|| DEFAULT_LIVE_BASE_ETH_AMOUNT_WEI.to_string()),
        RuntimeRoute::BaseUsdcToBtc => env_var_optional(LIVE_BASE_USDC_AMOUNT_ENV)
            .unwrap_or_else(|| DEFAULT_LIVE_BASE_USDC_AMOUNT.to_string()),
    }
}

fn assert_route_provider_id(route: RuntimeRoute, provider_id: &str) -> RuntimeIngressBridge {
    assert!(
        provider_id.starts_with("path:"),
        "provider id should describe the selected transition path: {provider_id}"
    );

    let (ingress, expected_fragments): (RuntimeIngressBridge, &[&str]) = match route {
        RuntimeRoute::BaseEthToBtc => (
            RuntimeIngressBridge::Across,
            &[
                "across_bridge:across",
                "unit_deposit:unit",
                "hyperliquid_trade:hyperliquid",
                "unit_withdrawal:unit",
                "exchange:hyperliquid",
            ],
        ),
        RuntimeRoute::BaseUsdcToBtc if provider_id.contains("cctp_bridge:cctp") => (
            RuntimeIngressBridge::Cctp,
            &[
                "cctp_bridge:cctp",
                "hyperliquid_bridge_deposit:hyperliquid_bridge",
                "hyperliquid_trade:hyperliquid",
                "unit_withdrawal:unit",
                "exchange:hyperliquid",
            ],
        ),
        RuntimeRoute::BaseUsdcToBtc if provider_id.contains("across_bridge:across") => (
            RuntimeIngressBridge::Across,
            &[
                "across_bridge:across",
                "hyperliquid_bridge_deposit:hyperliquid_bridge",
                "hyperliquid_trade:hyperliquid",
                "unit_withdrawal:unit",
                "exchange:hyperliquid",
            ],
        ),
        RuntimeRoute::BaseUsdcToBtc => {
            panic!("Base USDC route should use Across or CCTP ingress into Arbitrum: {provider_id}")
        }
    };
    for fragment in expected_fragments {
        assert!(
            provider_id.contains(fragment),
            "provider id {provider_id} should contain {fragment}"
        );
    }
    ingress
}

fn route_expected_funding_asset(route: RuntimeRoute) -> AssetId {
    match route {
        RuntimeRoute::BaseEthToBtc => AssetId::Native,
        RuntimeRoute::BaseUsdcToBtc => AssetId::Reference(BASE_USDC_ADDRESS.to_string()),
    }
}

fn route_order_metadata(route: RuntimeRoute) -> &'static str {
    match route {
        RuntimeRoute::BaseEthToBtc => "sauron_runtime_base_eth_btc",
        RuntimeRoute::BaseUsdcToBtc => "sauron_runtime_base_usdc_btc",
    }
}

fn route_expected_hl_trade_steps(route: RuntimeRoute) -> usize {
    match route {
        RuntimeRoute::BaseEthToBtc => 2,
        RuntimeRoute::BaseUsdcToBtc => 1,
    }
}

async fn spawn_runtime_mocks(devnet: &RiftDevnet, route: RuntimeRoute) -> MockIntegratorServer {
    let mut config = MockIntegratorConfig::default()
        .with_across_chain(
            1,
            format!(
                "{:#x}",
                devnet.ethereum.mock_across_spoke_pool_contract.address()
            ),
            devnet.ethereum.anvil.ws_endpoint_url().to_string(),
        )
        .with_across_chain(
            8453,
            format!(
                "{:#x}",
                devnet.base.mock_across_spoke_pool_contract.address()
            ),
            devnet.base.anvil.ws_endpoint_url().to_string(),
        )
        .with_across_chain(
            42161,
            format!(
                "{:#x}",
                devnet.arbitrum.mock_across_spoke_pool_contract.address()
            ),
            devnet.arbitrum.anvil.ws_endpoint_url().to_string(),
        )
        .with_unit_evm_rpc_url(
            hyperunit_client::UnitChain::Ethereum,
            devnet.ethereum.anvil.endpoint(),
        )
        .with_unit_bitcoin_rpc(
            devnet.bitcoin.rpc_url.clone(),
            devnet.bitcoin.cookie.clone(),
        )
        .with_across_status_latency(Duration::from_secs(2))
        .with_mainnet_hyperliquid(true);
    if matches!(route, RuntimeRoute::BaseUsdcToBtc) {
        config = config
            .with_cctp_token_messenger_address(
                devnet::evm_devnet::MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
            )
            .with_cctp_evm_rpc_url(devnet.base.anvil.endpoint_url().to_string())
            .with_cctp_destination_token_address(ARBITRUM_USDC_ADDRESS)
            .with_hyperliquid_bridge_address(MAINNET_HYPERLIQUID_BRIDGE_ADDRESS)
            .with_hyperliquid_evm_rpc_url(devnet.arbitrum.anvil.endpoint_url().to_string())
            .with_hyperliquid_usdc_token_address(ARBITRUM_USDC_ADDRESS);
    }
    MockIntegratorServer::spawn_with_config(config)
        .await
        .expect("spawn mock integrators")
}

async fn submit_quote_request(
    client: &reqwest::Client,
    router_base_url: &str,
    route: RuntimeRoute,
) -> RouterOrderQuoteEnvelope {
    submit_quote_request_custom(
        client,
        router_base_url,
        route,
        route_amount_in(route).to_string(),
        valid_regtest_btc_address(),
    )
    .await
}

async fn submit_quote_request_custom(
    client: &reqwest::Client,
    router_base_url: &str,
    route: RuntimeRoute,
    amount_in: String,
    recipient_address: String,
) -> RouterOrderQuoteEnvelope {
    let from_asset = match route {
        RuntimeRoute::BaseEthToBtc => json!({
            "chain": "evm:8453",
            "asset": "native"
        }),
        RuntimeRoute::BaseUsdcToBtc => json!({
            "chain": "evm:8453",
            "asset": BASE_USDC_ADDRESS
        }),
    };

    let quote_response = client
        .post(format!("{router_base_url}/api/v1/quotes"))
        .json(&json!({
            "type": "market_order",
            "from_asset": from_asset,
            "to_asset": {
                "chain": "bitcoin",
                "asset": "native"
            },
            "recipient_address": recipient_address,
            "kind": "exact_in",
            "amount_in": amount_in,
            "slippage_bps": 100
        }))
        .send()
        .await
        .expect("quote request");
    let status = quote_response.status();
    let body = quote_response.text().await.expect("quote body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("quote response")
}

async fn submit_order_request(
    client: &reqwest::Client,
    router_base_url: &str,
    quote_id: Uuid,
    route: RuntimeRoute,
) -> RouterOrderEnvelope {
    let order_response = client
        .post(format!("{router_base_url}/api/v1/orders"))
        .json(&json!({
            "quote_id": quote_id,
            "idempotency_key": format!("sauron-provider-operation-e2e:{route:?}:{quote_id}"),
            "refund_address": valid_evm_address(),
            "metadata": {
                "test": route_order_metadata(route)
            }
        }))
        .send()
        .await
        .expect("order request");
    let status = order_response.status();
    let body = order_response.text().await.expect("order body");
    assert_eq!(status, reqwest::StatusCode::CREATED, "{body}");
    serde_json::from_str(&body).expect("order response")
}

async fn fund_source_vault(devnet: &RiftDevnet, route: RuntimeRoute, vault_address: Address) {
    match route {
        RuntimeRoute::BaseEthToBtc => {
            let source_balance = U256::from_str_radix(route_amount_in(route), 10).unwrap();
            send_native(&devnet.base, vault_address, source_balance).await;
        }
        RuntimeRoute::BaseUsdcToBtc => {
            let amount = U256::from_str_radix(route_amount_in(route), 10).unwrap();
            mint_erc20(
                &devnet.base,
                BASE_USDC_ADDRESS.parse().expect("valid base usdc"),
                vault_address,
                amount,
            )
            .await;
        }
    }
    mine_evm_confirmation_block(&devnet.base).await;
}

async fn base_native_balance(devnet: &RiftDevnet, address: Address) -> U256 {
    devnet
        .base
        .funded_provider
        .get_balance(address)
        .await
        .expect("read base native balance")
}

async fn fund_live_source_vault(
    live: &LiveRuntimeConfig,
    route: RuntimeRoute,
    vault_address: Address,
) -> String {
    let base_funding_rpc_url = env_var_optional(BASE_FUNDING_RPC_URL_ENV)
        .unwrap_or_else(|| DEFAULT_LIVE_BASE_FUNDING_RPC_URL.to_string());
    match route {
        RuntimeRoute::BaseEthToBtc => {
            let amount = U256::from_str_radix(&live_route_amount_in(route), 10).unwrap()
                + U256::from(LIVE_NATIVE_SOURCE_GAS_BUFFER_WEI);
            send_live_native(
                &base_funding_rpc_url,
                &live.private_key,
                vault_address,
                amount,
            )
            .await
        }
        RuntimeRoute::BaseUsdcToBtc => {
            let amount = U256::from_str_radix(&live_route_amount_in(route), 10).unwrap();
            send_live_erc20(
                &base_funding_rpc_url,
                &live.private_key,
                BASE_USDC_ADDRESS.parse().expect("valid base usdc"),
                vault_address,
                amount,
            )
            .await
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "integration: spawns devnet stack"]
async fn sauron_runtime_drives_router_worker_through_mock_base_eth_btc_progress() {
    LocalSet::new()
        .run_until(run_mock_runtime_route(RuntimeRoute::BaseEthToBtc))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "integration: spawns devnet stack"]
async fn sauron_runtime_drives_router_worker_through_mock_base_usdc_btc_progress() {
    LocalSet::new()
        .run_until(run_mock_runtime_route(RuntimeRoute::BaseUsdcToBtc))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Validates the live Soron runtime environment without spending funds"]
async fn live_sauron_runtime_environment_contract() {
    load_repo_env_once();
    if env::var(LIVE_RUNTIME_RUN_FLAG).as_deref() != Ok("1") {
        return;
    }

    let live = LiveRuntimeConfig::from_env();
    let signer = live
        .private_key
        .parse::<PrivateKeySigner>()
        .expect("valid live runtime private key");
    let _source_address = signer.address();

    let ethereum_provider = ProviderBuilder::new().connect_http(
        live.ethereum_rpc_url
            .parse()
            .expect("valid Ethereum RPC URL for live runtime"),
    );
    let base_provider = ProviderBuilder::new().connect_http(
        live.base_rpc_url
            .parse()
            .expect("valid Base RPC URL for live runtime"),
    );
    let arbitrum_provider = ProviderBuilder::new().connect_http(
        live.arbitrum_rpc_url
            .parse()
            .expect("valid Arbitrum RPC URL for live runtime"),
    );

    assert_eq!(
        ethereum_provider
            .get_chain_id()
            .await
            .expect("ethereum chain id"),
        1
    );
    assert_eq!(
        base_provider.get_chain_id().await.expect("base chain id"),
        8453
    );
    assert_eq!(
        arbitrum_provider
            .get_chain_id()
            .await
            .expect("arbitrum chain id"),
        42161
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Probes the live Soron EVM backend cursor path without spending funds"]
async fn live_sauron_evm_backend_cursor_contract() {
    load_repo_env_once();
    if env::var(LIVE_RUNTIME_RUN_FLAG).as_deref() != Ok("1") {
        return;
    }

    let live = LiveRuntimeConfig::from_env();
    let args = live_sauron_args(
        "postgres://unused",
        "postgres://unused-state",
        "http://127.0.0.1:9",
        &live,
    );

    let ethereum = EvmIndexerDiscoveryBackend::new_ethereum(&args).expect("build ethereum backend");
    let ethereum_cursor = ethereum
        .current_cursor()
        .await
        .expect("ethereum current cursor");
    eprintln!(
        "ethereum cursor height={} hash={}",
        ethereum_cursor.height, ethereum_cursor.hash
    );

    let base = EvmIndexerDiscoveryBackend::new_base(&args).expect("build base backend");
    let base_cursor = base.current_cursor().await.expect("base current cursor");
    eprintln!(
        "base cursor height={} hash={}",
        base_cursor.height, base_cursor.hash
    );

    let arbitrum = EvmIndexerDiscoveryBackend::new_arbitrum(&args).expect("build arbitrum backend");
    let arbitrum_cursor = arbitrum
        .current_cursor()
        .await
        .expect("arbitrum current cursor");
    eprintln!(
        "arbitrum cursor height={} hash={}",
        arbitrum_cursor.height, arbitrum_cursor.hash
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "Probes concurrent live Soron EVM backend cursor calls without spending funds"]
async fn live_sauron_evm_backend_cursor_concurrency_contract() {
    load_repo_env_once();
    if env::var(LIVE_RUNTIME_RUN_FLAG).as_deref() != Ok("1") {
        return;
    }

    let live = LiveRuntimeConfig::from_env();
    let args = live_sauron_args(
        "postgres://unused",
        "postgres://unused-state",
        "http://127.0.0.1:9",
        &live,
    );

    let ethereum = EvmIndexerDiscoveryBackend::new_ethereum(&args).expect("build ethereum backend");
    let base = EvmIndexerDiscoveryBackend::new_base(&args).expect("build base backend");
    let arbitrum = EvmIndexerDiscoveryBackend::new_arbitrum(&args).expect("build arbitrum backend");

    for round in 0..10 {
        let (ethereum_cursor, base_cursor, arbitrum_cursor) = tokio::join!(
            ethereum.current_cursor(),
            base.current_cursor(),
            arbitrum.current_cursor()
        );
        let ethereum_cursor = ethereum_cursor.unwrap_or_else(|error| {
            panic!("ethereum current cursor failed in round {round}: {error}")
        });
        let base_cursor = base_cursor
            .unwrap_or_else(|error| panic!("base current cursor failed in round {round}: {error}"));
        let arbitrum_cursor = arbitrum_cursor.unwrap_or_else(|error| {
            panic!("arbitrum current cursor failed in round {round}: {error}")
        });
        eprintln!(
            "round={round} ethereum={} base={} arbitrum={}",
            ethereum_cursor.height, base_cursor.height, arbitrum_cursor.height
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "SPENDS REAL FUNDS through router-api + router-worker + Sauron on live providers"]
async fn live_sauron_runtime_drives_router_worker_through_base_eth_btc_progress() {
    load_repo_env_once();
    if env::var(LIVE_RUNTIME_RUN_FLAG).as_deref() != Ok("1") {
        return;
    }
    require_live_spend_confirmation();
    LocalSet::new()
        .run_until(run_live_runtime_route(RuntimeRoute::BaseEthToBtc))
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "SPENDS REAL FUNDS through router-api + router-worker + Sauron on live providers"]
async fn live_sauron_runtime_drives_router_worker_through_base_usdc_btc_progress() {
    load_repo_env_once();
    if env::var(LIVE_RUNTIME_RUN_FLAG).as_deref() != Ok("1") {
        return;
    }
    require_live_spend_confirmation();
    LocalSet::new()
        .run_until(run_live_runtime_route(RuntimeRoute::BaseUsdcToBtc))
        .await;
}

async fn run_live_runtime_route(route: RuntimeRoute) {
    let live = LiveRuntimeConfig::from_env();
    let signer = live
        .private_key
        .parse::<PrivateKeySigner>()
        .expect("valid live runtime private key");
    let source_address = signer.address();

    let mut postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let state_database_url = create_test_database(&postgres.admin_database_url).await;
    let config_dir = tempfile::tempdir().expect("router live config dir");
    let chain_tokens = live_route_chain_tokens(route);
    let args = live_router_args(config_dir.path(), database_url.clone(), chain_tokens, &live);
    let worker_config = RouterWorkerConfig::from_args(&args).expect("live worker config");
    let worker_components = initialize_components(
        &args,
        Some(worker_config.worker_id.clone()),
        PaymasterMode::Enabled,
    )
    .await
    .expect("initialize live worker components");
    let db = worker_components.db.clone();
    let chain_registry = worker_components.chain_registry.clone();
    let (router_base_url, _api_task) = spawn_router_api(args.clone()).await;
    ensure_temporal_up();
    let mut temporal_worker_task = spawn_temporal_order_worker(&args).await;
    let order_workflow_client = std::sync::Arc::new(
        OrderWorkflowClient::connect(
            &RouterTemporalConnection {
                temporal_address: args.temporal_address.clone(),
                namespace: args.temporal_namespace.clone(),
            },
            args.temporal_task_queue.clone(),
        )
        .await
        .expect("connect Temporal order workflow client"),
    );
    let _worker_task = RuntimeTask::new(tokio::spawn(async move {
        run_worker_loop(
            worker_components.db,
            worker_components.vault_manager,
            order_workflow_client,
            worker_components.route_costs,
            worker_components.provider_health_poller,
            worker_config,
        )
        .await
        .expect("live router worker loop should keep running until test aborts");
    }));
    let _sauron =
        spawn_live_sauron(&database_url, &state_database_url, &router_base_url, &live).await;
    let client = reqwest::Client::new();

    let amount_in = live_route_amount_in(route);
    let quote = submit_quote_request_custom(
        &client,
        &router_base_url,
        route,
        amount_in.clone(),
        live.bitcoin_recipient_address.clone(),
    )
    .await;
    let market_quote = quote.quote.as_market_order().expect("market order quote");
    assert_route_provider_id(route, &market_quote.provider_id);
    write_live_recovery_snapshot(LiveRecoverySnapshotSpec {
        label: "after_quote",
        route,
        live: &live,
        database_url: &database_url,
        config_dir: config_dir.path(),
        db: &db,
        chain_registry: chain_registry.as_ref(),
        order_id: None,
        quote_id: Some(market_quote.id),
        extra: json!({
            "amount_in": amount_in,
            "source_address": format!("{:#x}", source_address),
            "router_base_url": &router_base_url,
        }),
    })
    .await;

    let order = submit_order_request(&client, &router_base_url, market_quote.id, route).await;
    let funding_vault = order
        .funding_vault
        .as_ref()
        .expect("live order should include funding vault")
        .vault
        .clone();
    assert_eq!(
        funding_vault.deposit_asset.chain,
        ChainId::parse("evm:8453").unwrap()
    );
    assert_eq!(
        funding_vault.deposit_asset.asset,
        route_expected_funding_asset(route)
    );

    let funding_vault_address = Address::from_str(&funding_vault.deposit_vault_address)
        .expect("funding vault should be an EVM address");
    write_live_recovery_snapshot(LiveRecoverySnapshotSpec {
        label: "before_live_funding",
        route,
        live: &live,
        database_url: &database_url,
        config_dir: config_dir.path(),
        db: &db,
        chain_registry: chain_registry.as_ref(),
        order_id: Some(order.order.id),
        quote_id: Some(market_quote.id),
        extra: json!({
            "amount_in": live_route_amount_in(route),
            "source_address": format!("{:#x}", source_address),
            "funding_vault_address": &funding_vault.deposit_vault_address,
            "router_base_url": &router_base_url,
        }),
    })
    .await;
    let funding_tx_hash = fund_live_source_vault(&live, route, funding_vault_address).await;
    eprintln!(
        "live runtime funded source vault {} from {} via tx {}",
        funding_vault.deposit_vault_address, source_address, funding_tx_hash
    );
    write_live_recovery_snapshot(LiveRecoverySnapshotSpec {
        label: "after_live_funding",
        route,
        live: &live,
        database_url: &database_url,
        config_dir: config_dir.path(),
        db: &db,
        chain_registry: chain_registry.as_ref(),
        order_id: Some(order.order.id),
        quote_id: Some(market_quote.id),
        extra: json!({
            "funding_tx_hash": &funding_tx_hash,
            "source_address": format!("{:#x}", source_address),
            "funding_vault_address": &funding_vault.deposit_vault_address,
        }),
    })
    .await;

    let final_status =
        wait_for_terminal_order_status(&db, order.order.id, LIVE_TERMINAL_TIMEOUT).await;
    write_live_recovery_snapshot(LiveRecoverySnapshotSpec {
        label: "terminal_order_status",
        route,
        live: &live,
        database_url: &database_url,
        config_dir: config_dir.path(),
        db: &db,
        chain_registry: chain_registry.as_ref(),
        order_id: Some(order.order.id),
        quote_id: Some(market_quote.id),
        extra: json!({
            "final_status": final_status,
            "funding_tx_hash": &funding_tx_hash,
        }),
    })
    .await;
    if final_status != RouterOrderStatus::Completed {
        dump_order_state(&db, order.order.id).await;
    }
    assert_eq!(final_status, RouterOrderStatus::Completed);

    let completed_vault = db
        .vaults()
        .get(funding_vault.id)
        .await
        .expect("completed live funding vault");
    assert_eq!(completed_vault.status, DepositVaultStatus::Completed);

    let steps = db
        .orders()
        .get_execution_steps(order.order.id)
        .await
        .expect("load live execution steps");
    let hl_steps: Vec<_> = steps
        .iter()
        .filter(|step| step.step_type == OrderExecutionStepType::HyperliquidTrade)
        .collect();
    assert_eq!(
        hl_steps.len(),
        route_expected_hl_trade_steps(route),
        "live runtime flow should materialize the expected number of Hyperliquid trade legs"
    );

    let operations = db
        .orders()
        .get_provider_operations(order.order.id)
        .await
        .expect("load live provider operations");
    assert!(
        operations
            .iter()
            .all(|operation| operation.status == ProviderOperationStatus::Completed),
        "all provider operations should be completed for a successful live route"
    );
    temporal_worker_task.abort_and_join().await;
    postgres.shutdown().await;
}

async fn run_mock_runtime_route(route: RuntimeRoute) {
    let mut postgres = test_postgres().await;
    let database_url = create_test_database(&postgres.admin_database_url).await;
    let state_database_url = create_test_database(&postgres.admin_database_url).await;
    let hl_shim_database_url = create_test_database(&postgres.admin_database_url).await;
    let (devnet, _) = RiftDevnet::builder()
        .using_esplora(true)
        .using_token_indexer(database_url.clone())
        .build()
        .await
        .expect("devnet setup failed");
    prepare_route_chain_state(&devnet, route).await;
    let token_indexer_urls = token_indexer_urls_from_devnet(&devnet);
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
    let _receipt_watcher_tasks = [
        ethereum_receipt_watcher_task,
        base_receipt_watcher_task,
        arbitrum_receipt_watcher_task,
    ];
    let config_dir = tempfile::tempdir().expect("router config dir");
    let mocks = spawn_runtime_mocks(&devnet, route).await;
    let chain_tokens = route_chain_tokens(route);
    let args = router_args(
        &devnet,
        config_dir.path(),
        database_url.clone(),
        chain_tokens,
        &mocks,
    );
    let worker_config = RouterWorkerConfig::from_args(&args).expect("worker config");
    let worker_components = initialize_components(
        &args,
        Some(worker_config.worker_id.clone()),
        PaymasterMode::Enabled,
    )
    .await
    .expect("initialize worker components");
    let db = worker_components.db.clone();
    let (router_base_url, _api_task) = spawn_router_api(args.clone()).await;
    ensure_temporal_up();
    let (hl_shim_base_url, _hl_shim_task) =
        spawn_hl_shim_indexer(&hl_shim_database_url, mocks.base_url()).await;
    let (bitcoin_observer_urls, _bitcoin_observer_tasks) = spawn_bitcoin_observers(&devnet).await;
    let mut temporal_worker_task = spawn_temporal_order_worker(&args).await;
    let order_workflow_client = std::sync::Arc::new(
        OrderWorkflowClient::connect(
            &RouterTemporalConnection {
                temporal_address: args.temporal_address.clone(),
                namespace: args.temporal_namespace.clone(),
            },
            args.temporal_task_queue.clone(),
        )
        .await
        .expect("connect Temporal order workflow client"),
    );
    let _worker_task = RuntimeTask::new(tokio::spawn(async move {
        run_worker_loop(
            worker_components.db,
            worker_components.vault_manager,
            order_workflow_client,
            worker_components.route_costs,
            worker_components.provider_health_poller,
            worker_config,
        )
        .await
        .expect("router worker loop should keep running until test aborts");
    }));
    let _sauron = spawn_sauron(
        &devnet,
        &database_url,
        &state_database_url,
        &router_base_url,
        token_indexer_urls,
        EvmReceiptWatcherUrls {
            ethereum: ethereum_receipt_watcher_url,
            base: base_receipt_watcher_url,
            arbitrum: arbitrum_receipt_watcher_url,
        },
        Some(hl_shim_base_url),
        Some(bitcoin_observer_urls),
        Some(mocks.base_url().to_string()),
    )
    .await;
    let client = reqwest::Client::new();
    let quote = submit_quote_request(&client, &router_base_url, route).await;
    let ingress = assert_route_provider_id(
        route,
        &quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .provider_id,
    );

    let order = submit_order_request(
        &client,
        &router_base_url,
        quote
            .quote
            .as_market_order()
            .expect("market order quote")
            .id,
        route,
    )
    .await;
    let funding_vault = order
        .funding_vault
        .as_ref()
        .expect("order should include funding vault")
        .vault
        .clone();
    assert_eq!(
        funding_vault.deposit_asset.chain,
        ChainId::parse("evm:8453").unwrap()
    );
    assert_eq!(
        funding_vault.deposit_asset.asset,
        route_expected_funding_asset(route)
    );

    let funding_vault_address = Address::from_str(&funding_vault.deposit_vault_address)
        .expect("funding vault should be an EVM address");
    assert_eq!(
        base_native_balance(&devnet, funding_vault_address).await,
        U256::ZERO,
        "fresh source vault should not start with direct native gas padding"
    );
    fund_source_vault(&devnet, route, funding_vault_address).await;
    let funded_source_vault_native = base_native_balance(&devnet, funding_vault_address).await;
    match route {
        RuntimeRoute::BaseEthToBtc => assert_eq!(
            funded_source_vault_native,
            U256::from_str_radix(route_amount_in(route), 10).unwrap(),
            "native source route should deposit only the quoted input; gas must come from the paymaster"
        ),
        RuntimeRoute::BaseUsdcToBtc => assert_eq!(
            funded_source_vault_native,
            U256::ZERO,
            "ERC-20 source route should not receive native gas before worker execution"
        ),
    }
    let base_paymaster_address = evm_address_from_private_key(&anvil_private_key(&devnet.base));
    let base_paymaster_before_execution =
        base_native_balance(&devnet, base_paymaster_address).await;
    wait_for_order_status(&db, order.order.id, RouterOrderStatus::Executing).await;
    let funded_vault = db
        .vaults()
        .get(funding_vault.id)
        .await
        .expect("funding vault after worker funding pass");
    assert_eq!(funded_vault.status, DepositVaultStatus::Executing);

    match route {
        RuntimeRoute::BaseEthToBtc => {
            let across_operation =
                wait_for_operation(&db, order.order.id, ProviderOperationType::AcrossBridge).await;
            assert_async_provider_operation_started(&across_operation);
            wait_for_operation_status(&db, across_operation.id, ProviderOperationStatus::Completed)
                .await;
        }
        RuntimeRoute::BaseUsdcToBtc => match ingress {
            RuntimeIngressBridge::Cctp => {
                let cctp_operation =
                    wait_for_operation(&db, order.order.id, ProviderOperationType::CctpBridge)
                        .await;
                assert_async_provider_operation_started(&cctp_operation);
                wait_for_operation_status(
                    &db,
                    cctp_operation.id,
                    ProviderOperationStatus::Completed,
                )
                .await;
            }
            RuntimeIngressBridge::Across => {
                let across_operation =
                    wait_for_operation(&db, order.order.id, ProviderOperationType::AcrossBridge)
                        .await;
                assert_async_provider_operation_started(&across_operation);
                wait_for_operation_status(
                    &db,
                    across_operation.id,
                    ProviderOperationStatus::Completed,
                )
                .await;
            }
        },
    }
    assert_source_vault_received_paymaster_gas(
        &devnet,
        funding_vault_address,
        base_paymaster_address,
        base_paymaster_before_execution,
    )
    .await;

    if matches!(route, RuntimeRoute::BaseEthToBtc) {
        let unit_deposit_operation =
            wait_for_operation(&db, order.order.id, ProviderOperationType::UnitDeposit).await;
        wait_for_operation_status(
            &db,
            unit_deposit_operation.id,
            ProviderOperationStatus::Completed,
        )
        .await;
    } else {
        let bridge_operation = wait_for_operation(
            &db,
            order.order.id,
            ProviderOperationType::HyperliquidBridgeDeposit,
        )
        .await;
        wait_for_operation_status(&db, bridge_operation.id, ProviderOperationStatus::Completed)
            .await;
    }

    let unit_withdrawal_operation =
        wait_for_operation(&db, order.order.id, ProviderOperationType::UnitWithdrawal).await;
    wait_for_operation_status(
        &db,
        unit_withdrawal_operation.id,
        ProviderOperationStatus::Completed,
    )
    .await;

    wait_for_order_status(&db, order.order.id, RouterOrderStatus::Completed).await;
    let completed_vault = db
        .vaults()
        .get(funding_vault.id)
        .await
        .expect("completed funding vault");
    assert_eq!(completed_vault.status, DepositVaultStatus::Completed);
    assert_runtime_hints_are_production_driven(&database_url, order.order.id, funding_vault.id)
        .await;

    let steps = db
        .orders()
        .get_execution_steps(order.order.id)
        .await
        .expect("load execution steps");
    let hl_steps: Vec<_> = steps
        .iter()
        .filter(|step| step.step_type == OrderExecutionStepType::HyperliquidTrade)
        .collect();
    assert_eq!(
        hl_steps.len(),
        route_expected_hl_trade_steps(route),
        "runtime flow should materialize the expected number of Hyperliquid trade legs"
    );

    assert_eq!(
        mocks.across_deposits().await.len(),
        usize::from(matches!(ingress, RuntimeIngressBridge::Across))
    );
    assert_eq!(
        mocks.cctp_burns().await.len(),
        usize::from(matches!(ingress, RuntimeIngressBridge::Cctp))
    );
    let ledger = mocks.ledger_snapshot().await;
    let deposit_ops: Vec<_> = ledger
        .unit_operations
        .iter()
        .filter(|op| {
            matches!(
                op.kind,
                devnet::mock_integrators::MockUnitOperationKind::Deposit
            )
        })
        .collect();
    let withdrawal_ops: Vec<_> = ledger
        .unit_operations
        .iter()
        .filter(|op| {
            matches!(
                op.kind,
                devnet::mock_integrators::MockUnitOperationKind::Withdrawal
            )
        })
        .collect();
    assert_eq!(
        deposit_ops.len(),
        usize::from(matches!(route, RuntimeRoute::BaseEthToBtc))
    );
    assert_eq!(withdrawal_ops.len(), 1);
    temporal_worker_task.abort_and_join().await;
    postgres.shutdown().await;
}

async fn assert_source_vault_received_paymaster_gas(
    devnet: &RiftDevnet,
    funding_vault_address: Address,
    paymaster_address: Address,
    paymaster_before_execution: U256,
) {
    let source_vault_native = base_native_balance(devnet, funding_vault_address).await;
    assert!(
        source_vault_native > U256::ZERO,
        "source vault should retain paymaster-provided native gas after the first EVM action"
    );

    let paymaster_after_execution = base_native_balance(devnet, paymaster_address).await;
    assert!(
        paymaster_after_execution < paymaster_before_execution,
        "base paymaster balance should decrease when it tops up the source vault"
    );
}

async fn assert_runtime_hints_are_production_driven(
    database_url: &str,
    order_id: Uuid,
    funding_vault_id: Uuid,
) {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .min_connections(0)
        .connect(database_url)
        .await
        .expect("connect to runtime route database for hint assertions");

    let funding_hint_count = sqlx_core::query_scalar::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM deposit_vault_funding_hints
            WHERE vault_id = $1
            "#,
    )
    .bind(funding_vault_id)
    .fetch_one(&pool)
    .await
    .expect("count funding hints");

    let non_sauron_funding_hints = sqlx_core::query_scalar::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM deposit_vault_funding_hints
            WHERE vault_id = $1
              AND source <> 'sauron'
            "#,
    )
    .bind(funding_vault_id)
    .fetch_one(&pool)
    .await
    .expect("count non-Sauron funding hints");
    assert_eq!(
        non_sauron_funding_hints, 0,
        "production-shaped runtime route should not insert funding hints from test helpers"
    );

    if funding_hint_count > 0 {
        let processed_funding_hints = sqlx_core::query_scalar::query_scalar::<_, i64>(
            r#"
                SELECT COUNT(*)::bigint
                FROM deposit_vault_funding_hints
                WHERE vault_id = $1
                  AND source = 'sauron'
                  AND status = 'processed'
                "#,
        )
        .bind(funding_vault_id)
        .fetch_one(&pool)
        .await
        .expect("count processed Sauron funding hints");
        assert!(
            processed_funding_hints > 0,
            "runtime route should process Sauron funding hints when they win the funding race"
        );
    } else {
        let funding_source = sqlx_core::query_scalar::query_scalar::<_, Option<String>>(
            r#"
                SELECT funding_evidence_json->>'source'
                FROM deposit_vaults
                WHERE id = $1
                "#,
        )
        .bind(funding_vault_id)
        .fetch_one(&pool)
        .await
        .expect("read funding observation source");
        assert_eq!(
            funding_source.as_deref(),
            Some("router_balance_reconciliation"),
            "runtime route funding should be advanced by Sauron or router balance reconciliation, not test helpers"
        );
    }

    let provider_hint_count = sqlx_core::query_scalar::query_scalar::<_, i64>(
        r#"
            SELECT COUNT(*)::bigint
            FROM order_provider_operation_hints hints
            JOIN order_provider_operations operations
              ON operations.id = hints.provider_operation_id
            WHERE operations.order_id = $1
            "#,
    )
    .bind(order_id)
    .fetch_one(&pool)
    .await
    .expect("count provider operation hints");
    if provider_hint_count > 0 {
        let non_sauron_provider_hints = sqlx_core::query_scalar::query_scalar::<_, i64>(
            r#"
                SELECT COUNT(*)::bigint
                FROM order_provider_operation_hints hints
                JOIN order_provider_operations operations
                  ON operations.id = hints.provider_operation_id
                WHERE operations.order_id = $1
                  AND hints.source NOT IN (
                    'sauron',
                    'sauron_provider_operation_observation',
                    $2,
                    $3,
                    $4
                  )
                "#,
        )
        .bind(order_id)
        .bind(SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE)
        .bind(SAURON_EVM_RECEIPT_OBSERVER_HINT_SOURCE)
        .bind(SAURON_HYPERUNIT_OBSERVER_HINT_SOURCE)
        .fetch_one(&pool)
        .await
        .expect("count non-Sauron provider operation hints");
        assert_eq!(
            non_sauron_provider_hints, 0,
            "production-shaped runtime route should not insert provider-operation hints from test helpers"
        );

        let processed_provider_hints = sqlx_core::query_scalar::query_scalar::<_, i64>(
            r#"
                SELECT COUNT(*)::bigint
                FROM order_provider_operation_hints hints
                JOIN order_provider_operations operations
                  ON operations.id = hints.provider_operation_id
                WHERE operations.order_id = $1
                  AND hints.status = 'processed'
                "#,
        )
        .bind(order_id)
        .fetch_one(&pool)
        .await
        .expect("count processed provider operation hints");
        assert!(
            processed_provider_hints > 0,
            "runtime route should process provider-operation hints when Sauron wins the observation race"
        );
    }
}

fn assert_async_provider_operation_started(operation: &OrderProviderOperation) {
    assert!(
        matches!(
            operation.status,
            ProviderOperationStatus::Submitted
                | ProviderOperationStatus::WaitingExternal
                | ProviderOperationStatus::Completed
        ),
        "async provider operation should be submitted or externally observable, got {:?}",
        operation.status
    );
}
