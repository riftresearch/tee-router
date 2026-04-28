use alloy::{
    hex,
    network::TransactionBuilder,
    primitives::{Address, Bytes, FixedBytes, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::{Filter, TransactionRequest},
    sol,
    sol_types::{SolCall, SolEvent, SolValue},
};
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use hyperliquid_client::{
    recover_l1_signer, recover_typed_signer, Actions, SpotMeta, SpotSend, UsdClassTransfer,
    Withdraw3, MINIMUM_BRIDGE_DEPOSIT_USDC, SPOT_ASSET_INDEX_OFFSET,
};
use hyperunit_client::{
    UnitAsset, UnitChain, UnitGenerateAddressResponse, UnitOperation, UnitOperationsResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
use url::Url;
use uuid::Uuid;

use crate::{
    across_spoke_pool_mock::MockSpokePool::{depositCall, FundsDeposited},
    cctp_mock::MockCctpTokenMessengerV2::DepositForBurn,
    hyperliquid_bridge_mock::MockHyperliquidBridge2,
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
        function balanceOf(address account) external view returns (uint256);
        function transfer(address recipient, uint256 amount) external returns (bool);

        event Transfer(address indexed from, address indexed to, uint256 value);
    }
}

sol! {
    interface IMockMintableERC20 {
        function mint(address recipient, uint256 amount) external;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct MockAssetRef {
    pub chain_id: String,
    pub asset: String,
}

/// Path parameters captured when the router calls `GET /gen/{src}/{dst}/{asset}/{dst_addr}`.
/// Mirrors `hyperunit_client::UnitGenerateAddressRequest` but uses strings so
/// tests can assert against raw wire values without importing the client enums.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockUnitGenerateAddressRequest {
    pub src_chain: String,
    pub dst_chain: String,
    pub asset: String,
    pub dst_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockProviderQuoteResponse {
    pub provider_id: String,
    pub amount_in: String,
    pub amount_out: String,
    pub min_amount_out: Option<String>,
    pub max_amount_in: Option<String>,
    pub provider_quote: Value,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockIntegratorErrorResponse {
    pub error: String,
}

/// Operation the mock tracks for each protocol address returned from `/gen`.
/// Assertions against `ledger_snapshot().unit_operations` let tests observe
/// the full lifecycle — what chain/asset/amount the operation represents,
/// which HL account ended up holding the tokens (for deposits), and which
/// recipient the tokens were sent to (for withdrawals).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockUnitOperationRecord {
    pub protocol_address: String,
    pub src_chain: String,
    pub dst_chain: String,
    pub asset: String,
    pub dst_addr: String,
    pub kind: MockUnitOperationKind,
    pub state: String,
    pub source_amount: Option<String>,
    pub source_tx_hash: Option<String>,
    pub destination_tx_hash: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MockUnitOperationKind {
    Deposit,
    Withdrawal,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockIntegratorLedgerSnapshot {
    pub bridged_balances: BTreeMap<String, String>,
    pub hyperliquid_balances: BTreeMap<String, String>,
    pub recipient_balances: BTreeMap<String, String>,
    pub unit_operations: Vec<MockUnitOperationRecord>,
    pub cctp_burns: Vec<MockCctpBurnRecord>,
    pub hyperliquid_exchange_submissions: Vec<Value>,
}

#[derive(Debug, Clone, Default)]
pub struct MockIntegratorConfig {
    /// Address of the mock SpokePool contract deployed to the test Anvil.
    /// When set, the mock GET `/swap/approval` handler encodes a real
    /// `deposit(...)` call targeting this address (matching real Across
    /// semantics: production submits the returned `swapTx` on-chain).
    pub across_spoke_pool_address: Option<String>,
    /// RPC URL (ws:// or http://) of the chain hosting the mock SpokePool.
    /// When set alongside `across_spoke_pool_address`, a background indexer
    /// polls `FundsDeposited` logs and serves them via `GET /deposit/status`
    /// in the same shape as the real Across API.
    pub across_evm_rpc_url: Option<String>,
    /// Whether Hyperliquid L1 actions must be signed under the mainnet
    /// source-byte ("a") vs testnet ("b"). Defaults to testnet so tests
    /// default to the non-mainnet signing path the router uses in devnet.
    pub mainnet_hyperliquid: bool,
    /// Optional bearer token the Across mock requires on `/swap/approval` and
    /// `/deposit/status`. When unset, auth is not enforced.
    pub across_api_key: Option<String>,
    /// Optional integrator id the Across mock requires in `/swap/approval`.
    /// When unset, any non-empty or absent integrator id is accepted.
    pub across_integrator_id: Option<String>,
    /// Address of the mock Hyperliquid Bridge2 contract deployed to an EVM
    /// test chain. Native Hyperliquid USDC deposits are modeled as ERC20
    /// transfers into this address.
    pub hyperliquid_bridge_address: Option<String>,
    /// RPC URL (ws:// or http://) of the chain hosting the mock Hyperliquid
    /// bridge and its USDC token. When set with `hyperliquid_bridge_address`
    /// and `hyperliquid_usdc_token_address`, a background indexer watches
    /// token `Transfer` logs into the bridge and credits the sender's mock
    /// Hyperliquid USDC balance.
    pub hyperliquid_evm_rpc_url: Option<String>,
    /// ERC20 token address used as "native Arbitrum USDC" for the mock
    /// Hyperliquid bridge.
    pub hyperliquid_usdc_token_address: Option<String>,
    /// Address of the mock CCTP TokenMessengerV2 contract deployed to the
    /// source EVM test chain. When set with `cctp_evm_rpc_url`, the mock Iris
    /// endpoint indexes burn events from this contract.
    pub cctp_token_messenger_address: Option<String>,
    /// RPC URL of the source chain hosting the mock CCTP TokenMessengerV2.
    pub cctp_evm_rpc_url: Option<String>,
    /// Token address that mock CCTP receiveMessage should mint on the
    /// destination chain.
    pub cctp_destination_token_address: Option<String>,
    /// Artificial delay before the mock Bridge2 releases the net `withdraw3`
    /// payout onto the EVM chain. Defaults to zero.
    pub hyperliquid_withdrawal_latency: Duration,
    /// Deterministic base fee applied to mock Across quotes, in basis points.
    pub across_quote_fee_bps: u16,
    /// Deterministic per-quote jitter added on top of `across_quote_fee_bps`,
    /// in basis points. The actual jitter is derived from the request.
    pub across_quote_jitter_bps: u16,
    /// Artificial delay after a mock Across deposit is indexed before
    /// `/deposit/status` can leave `pending`.
    pub across_status_latency: Duration,
    /// Deterministic probability that an indexed mock Across deposit resolves
    /// as `refunded` instead of `filled`, in basis points.
    pub across_refund_probability_bps: u16,
}

impl MockIntegratorConfig {
    #[must_use]
    pub fn with_across_spoke_pool_address(mut self, address: impl Into<String>) -> Self {
        self.across_spoke_pool_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_across_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.across_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_mainnet_hyperliquid(mut self, is_mainnet: bool) -> Self {
        self.mainnet_hyperliquid = is_mainnet;
        self
    }

    #[must_use]
    pub fn with_across_auth(
        mut self,
        api_key: impl Into<String>,
        integrator_id: impl Into<String>,
    ) -> Self {
        self.across_api_key = Some(api_key.into());
        self.across_integrator_id = Some(integrator_id.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_bridge_address(mut self, address: impl Into<String>) -> Self {
        self.hyperliquid_bridge_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.hyperliquid_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_usdc_token_address(mut self, address: impl Into<String>) -> Self {
        self.hyperliquid_usdc_token_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_cctp_token_messenger_address(mut self, address: impl Into<String>) -> Self {
        self.cctp_token_messenger_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_cctp_evm_rpc_url(mut self, url: impl Into<String>) -> Self {
        self.cctp_evm_rpc_url = Some(url.into());
        self
    }

    #[must_use]
    pub fn with_cctp_destination_token_address(mut self, address: impl Into<String>) -> Self {
        self.cctp_destination_token_address = Some(address.into());
        self
    }

    #[must_use]
    pub fn with_hyperliquid_withdrawal_latency(mut self, latency: Duration) -> Self {
        self.hyperliquid_withdrawal_latency = latency;
        self
    }

    #[must_use]
    pub fn with_across_quote_fee_bps(mut self, fee_bps: u16) -> Self {
        self.across_quote_fee_bps = fee_bps.min(9_999);
        self
    }

    #[must_use]
    pub fn with_across_quote_jitter_bps(mut self, jitter_bps: u16) -> Self {
        self.across_quote_jitter_bps = jitter_bps.min(9_999);
        self
    }

    #[must_use]
    pub fn with_across_status_latency(mut self, latency: Duration) -> Self {
        self.across_status_latency = latency;
        self
    }

    #[must_use]
    pub fn with_across_refund_probability_bps(mut self, probability_bps: u16) -> Self {
        self.across_refund_probability_bps = probability_bps.min(10_000);
        self
    }
}

struct MockIntegratorState {
    across_spoke_pool_address: Option<String>,
    across_evm_rpc_url: Option<String>,
    hyperliquid_bridge_address: Option<String>,
    hyperliquid_evm_rpc_url: Option<String>,
    unit_generate_address_requests: Mutex<Vec<MockUnitGenerateAddressRequest>>,
    /// Tracked by `protocol_address` — the mock address returned from `/gen`
    /// that both the deposit source and the withdrawal spotSend target.
    ///
    /// Real HyperUnit reuses protocol addresses and returns historical
    /// operations for that address, so the mock keeps a per-address history
    /// instead of only the latest row.
    unit_operations: Mutex<BTreeMap<String, Vec<MockUnitOperationEntry>>>,
    ledger: Mutex<MockIntegratorLedger>,
    across_deposits: Mutex<BTreeMap<AcrossDepositKey, MockAcrossDepositRecord>>,
    cctp_burns: Mutex<BTreeMap<String, MockCctpBurnRecord>>,
    hyperliquid_exchange_submissions: Mutex<Vec<Value>>,
    hyperliquid: Mutex<HyperliquidMockState>,
    next_across_swap_approval_error: Mutex<Option<String>>,
    next_unit_generate_address_error: Mutex<Option<String>>,
    next_hyperliquid_exchange_error: Mutex<Option<String>>,
    mainnet_hyperliquid: bool,
    across_api_key: Option<String>,
    across_integrator_id: Option<String>,
    across_quote_fee_bps: u16,
    across_quote_jitter_bps: u16,
    hyperliquid_withdrawal_latency: Duration,
    across_status_latency: Duration,
    across_refund_probability_bps: u16,
}

type AcrossDepositKey = (u64, String);

#[derive(Debug, Clone)]
pub struct MockAcrossDepositRecord {
    pub origin_chain_id: u64,
    pub destination_chain_id: U256,
    pub deposit_id: U256,
    pub depositor: FixedBytes<32>,
    pub recipient: FixedBytes<32>,
    pub input_token: FixedBytes<32>,
    pub output_token: FixedBytes<32>,
    pub input_amount: U256,
    pub output_amount: U256,
    pub fill_deadline: u32,
    pub deposit_tx_hash: String,
    pub block_number: u64,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockCctpBurnRecord {
    pub source_domain: u32,
    pub destination_domain: u32,
    pub nonce: u64,
    pub burn_token: Address,
    pub destination_token: Address,
    pub depositor: Address,
    pub mint_recipient: Address,
    pub amount: U256,
    pub burn_tx_hash: String,
    pub block_number: u64,
    pub indexed_at: DateTime<Utc>,
}

#[derive(Default)]
struct MockIntegratorLedger {
    bridged_balances: BTreeMap<String, u128>,
    hyperliquid_balances: BTreeMap<String, u128>,
    recipient_balances: BTreeMap<String, u128>,
}

/// Mock's internal Hyperliquid state: per-user spot balances, per-user
/// clearinghouse balances, a hardcoded exchange-rate table per pair, and
/// historical / resting orders.
///
/// The mock still uses a synthetic external book at the configured rate, but
/// it now tracks production-relevant order lifecycle semantics:
/// - IoC orders fill immediately when marketable.
/// - Non-marketable Gtc / Alo orders rest on the book.
/// - Resting orders reserve `hold` balances and appear in `openOrders`.
/// - Cancels remove resting orders and move them into terminal history.
///
/// That is enough for the router and client integration tests to exercise the
/// same shapes production uses without implementing a full matching engine.
#[derive(Default)]
struct HyperliquidMockState {
    next_oid: u64,
    next_tid: u64,
    /// `user -> coin -> total spot balance (natural units, e.g. 1.0 UBTC)`.
    spot_balances: BTreeMap<Address, BTreeMap<String, f64>>,
    /// `user -> coin -> clearinghouse balance (currently only USDC matters
    /// for the mock). Bridge deposits land here as withdrawable collateral.
    clearinghouse_balances: BTreeMap<Address, BTreeMap<String, f64>>,
    /// `(base, quote) -> rate`. Rate semantics: `1 base = rate * quote`. A
    /// buy of `sz` base costs `sz * rate` quote; a sell of `sz` base yields
    /// `sz * rate` quote. Tests install pairs via
    /// [`MockIntegratorServer::set_hyperliquid_rate`].
    rates: BTreeMap<(String, String), f64>,
    /// `(base, quote) -> top-of-book depth` advertised on the synthesized
    /// bid/ask levels. Immediate marketable orders can only consume this much
    /// liquidity before either resting (Gtc) or dropping the remainder (Ioc).
    book_depths: BTreeMap<(String, String), HyperliquidBookDepth>,
    /// Orders that have left the open-order set and reached a terminal state.
    /// Queryable via `/info { type: "orderStatus" }`.
    terminal_orders: BTreeMap<u64, TerminalOrder>,
    /// `user -> fills`, newest first (real API ordering).
    fills: BTreeMap<Address, Vec<HyperliquidFillRecord>>,
    /// Currently resting open orders, keyed by oid.
    open_orders: BTreeMap<u64, HyperliquidSubmittedOrder>,
    /// Per-user dead-man switch deadline, in unix milliseconds. Once the
    /// scheduled time is reached the mock cancels all currently open orders
    /// for that user and clears the schedule.
    scheduled_cancels: BTreeMap<Address, u64>,
    /// Spot meta served from `/info { type: "spotMeta" }` and also used to
    /// resolve asset ids → pair names for rate lookup.
    spot_meta: Option<SpotMeta>,
}

#[derive(Debug, Clone, Copy)]
struct HyperliquidBookDepth {
    bid: f64,
    ask: f64,
}

#[derive(Debug, Clone)]
struct HyperliquidSubmittedOrder {
    oid: u64,
    user: Address,
    asset: u32,
    coin: String,
    is_buy: bool,
    limit_px: f64,
    sz: f64,
    orig_sz: f64,
    tif: String,
    cloid: Option<String>,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct TerminalOrder {
    order: HyperliquidSubmittedOrder,
    /// Final lifecycle status string returned by `/info { type: "orderStatus" }`
    /// — one of `"filled"`, `"rejected"`, `"canceled"`, `"scheduledCancel"`.
    status: String,
    status_timestamp: u64,
}

#[derive(Debug, Clone)]
struct HyperliquidFillRecord {
    oid: u64,
    tid: u64,
    coin: String,
    is_buy: bool,
    px: f64,
    sz: f64,
    time: u64,
    fee: f64,
    fee_token: String,
}

/// Mock's internal Unit operation record. Composes the wire-level
/// [`UnitOperation`] (what `/operations/:address` returns) with a `kind`
/// discriminator and an optional order-tag the router passes via the
/// `dst_addr` (for deposits, the router's HL custody address; for
/// withdrawals, the user's BTC/ETH address). Advancing the `operation.state`
/// is how the mock simulates HyperUnit's lifecycle machine.
#[derive(Debug, Clone)]
struct MockUnitOperationEntry {
    kind: MockUnitOperationKind,
    src_chain: UnitChain,
    dst_chain: UnitChain,
    asset: UnitAsset,
    dst_addr: String,
    visible: bool,
    operation: UnitOperation,
}

pub struct MockIntegratorServer {
    base_url: String,
    state: Arc<MockIntegratorState>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    across_indexer_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    across_indexer_handle: Option<JoinHandle<()>>,
    hyperliquid_indexer_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    hyperliquid_indexer_handle: Option<JoinHandle<()>>,
    cctp_indexer_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    cctp_indexer_handle: Option<JoinHandle<()>>,
}

impl MockIntegratorServer {
    pub async fn spawn() -> eyre::Result<Self> {
        Self::spawn_with_config(MockIntegratorConfig::default()).await
    }

    pub async fn spawn_with_config(config: MockIntegratorConfig) -> eyre::Result<Self> {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let state = Arc::new(MockIntegratorState::new(&config));

        let (across_indexer_shutdown, across_indexer_handle) =
            maybe_spawn_across_deposit_indexer(&config, state.clone()).await?;
        let (hyperliquid_indexer_shutdown, hyperliquid_indexer_handle) =
            maybe_spawn_hyperliquid_bridge_indexer(&config, state.clone()).await?;
        let (cctp_indexer_shutdown, cctp_indexer_handle) =
            maybe_spawn_cctp_burn_indexer(&config, state.clone()).await?;

        let app = Router::new()
            .route("/swap/approval", get(mock_across_real_swap_approval))
            .route("/deposit/status", get(mock_across_deposit_status))
            .route("/prices", get(mock_velora_prices))
            .route(
                "/v2/prices/:currency_pair/spot",
                get(mock_coinbase_spot_price),
            )
            .route("/transactions/:network", post(mock_velora_transaction))
            .route(
                "/gen/:src_chain/:dst_chain/:asset/:dst_addr",
                get(mock_unit_gen),
            )
            .route("/operations/:address", get(mock_unit_operations))
            .route("/v2/messages/:source_domain", get(mock_cctp_messages))
            .route(
                "/v2/burn/USDC/fees/:source_domain/:destination_domain",
                get(mock_cctp_burn_fees),
            )
            .route("/info", post(mock_hyperliquid_info))
            .route("/exchange", post(mock_hyperliquid_exchange))
            .with_state(state.clone());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::warn!(%error, "mock integrator server exited with error");
            }
        });

        Ok(Self {
            base_url: base_url(addr),
            state,
            shutdown: Some(shutdown_tx),
            handle,
            across_indexer_shutdown,
            across_indexer_handle,
            hyperliquid_indexer_shutdown,
            hyperliquid_indexer_handle,
            cctp_indexer_shutdown,
            cctp_indexer_handle,
        })
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub async fn unit_generate_address_requests(&self) -> Vec<MockUnitGenerateAddressRequest> {
        self.state
            .unit_generate_address_requests
            .lock()
            .await
            .clone()
    }

    pub async fn hyperliquid_exchange_submissions(&self) -> Vec<Value> {
        self.state
            .hyperliquid_exchange_submissions
            .lock()
            .await
            .clone()
    }

    pub async fn unit_operations(&self) -> Vec<MockUnitOperationRecord> {
        snapshot_unit_operations(&*self.state.unit_operations.lock().await)
    }

    pub async fn ledger_snapshot(&self) -> MockIntegratorLedgerSnapshot {
        let exchange_submissions = self
            .state
            .hyperliquid_exchange_submissions
            .lock()
            .await
            .clone();
        let unit_operations = snapshot_unit_operations(&*self.state.unit_operations.lock().await);
        let cctp_burns = self
            .state
            .cctp_burns
            .lock()
            .await
            .values()
            .cloned()
            .collect();
        self.state
            .ledger
            .lock()
            .await
            .snapshot(exchange_submissions, unit_operations, cctp_burns)
    }

    pub async fn fail_next_across_swap_approval(&self, error: impl Into<String>) {
        *self.state.next_across_swap_approval_error.lock().await = Some(error.into());
    }

    pub async fn fail_next_unit_generate_address(&self, error: impl Into<String>) {
        *self.state.next_unit_generate_address_error.lock().await = Some(error.into());
    }

    pub async fn fail_next_hyperliquid_exchange(&self, error: impl Into<String>) {
        *self.state.next_hyperliquid_exchange_error.lock().await = Some(error.into());
    }

    /// Advance a tracked unit operation to `done`. Tests use this to simulate
    /// HyperUnit finishing a deposit (funds credited to the HL protocol_address)
    /// or a withdrawal (tokens paid out to `dst_addr` after the spotSend).
    pub async fn complete_unit_operation(&self, protocol_address: &str) -> Result<(), String> {
        complete_mock_unit_operation(&self.state, protocol_address).await
    }

    /// Move a tracked unit operation into terminal `failure`.
    pub async fn fail_unit_operation(
        &self,
        protocol_address: &str,
        error: impl Into<String>,
    ) -> Result<(), String> {
        fail_mock_unit_operation(&self.state, protocol_address, error.into()).await
    }

    /// Directly seed a completed Unit operation without going through `/gen`.
    /// Tests that construct synthetic provider operations use this to make the
    /// mock's `/operations/{protocol_address}` return a `done` entry so the
    /// status-hint verifier transitions the operation to `Completed`.
    pub async fn seed_completed_unit_operation(
        &self,
        protocol_address: &str,
        kind: MockUnitOperationKind,
    ) {
        let now = Utc::now().to_rfc3339();
        let entry = MockUnitOperationEntry {
            kind,
            src_chain: UnitChain::Ethereum,
            dst_chain: UnitChain::Hyperliquid,
            asset: UnitAsset::Eth,
            dst_addr: String::new(),
            visible: true,
            operation: UnitOperation {
                operation_id: Some(format!("0x{}:0", "11".repeat(32))),
                op_created_at: Some(now.clone()),
                protocol_address: Some(protocol_address.to_string()),
                source_address: None,
                destination_address: None,
                source_chain: Some("ethereum".to_string()),
                destination_chain: Some("hyperliquid".to_string()),
                source_amount: Some("1000".to_string()),
                destination_fee_amount: Some("1".to_string()),
                sweep_fee_amount: Some("1".to_string()),
                state: Some("done".to_string()),
                source_tx_hash: Some(format!("0x{}:0", "11".repeat(32))),
                destination_tx_hash: Some(format!(
                    "0x{}:{}",
                    "22".repeat(20),
                    Utc::now().timestamp_millis()
                )),
                source_tx_confirmations: None,
                destination_tx_confirmations: Some(10),
                position_in_withdraw_queue: None,
                broadcast_at: Some(now.clone()),
                asset: Some("eth".to_string()),
                state_started_at: Some(now.clone()),
                state_updated_at: Some(now),
                state_next_attempt_at: None,
            },
        };
        self.state
            .unit_operations
            .lock()
            .await
            .entry(protocol_address.to_string())
            .or_default()
            .push(entry);
    }

    pub async fn unit_operation_state(&self, protocol_address: &str) -> Option<String> {
        self.state
            .unit_operations
            .lock()
            .await
            .get(protocol_address)
            .and_then(|entries| entries.last())
            .and_then(|entry| entry.operation.state.clone())
    }

    pub async fn across_deposits(&self) -> Vec<MockAcrossDepositRecord> {
        self.state
            .across_deposits
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn cctp_burns(&self) -> Vec<MockCctpBurnRecord> {
        self.state
            .cctp_burns
            .lock()
            .await
            .values()
            .cloned()
            .collect()
    }

    /// Returns only the deposits whose `depositor` matches the provided EVM
    /// address. Use this when parallel tests share an anvil spoke pool — each
    /// test's source custody vault has a unique address.
    pub async fn across_deposits_from(&self, depositor: Address) -> Vec<MockAcrossDepositRecord> {
        let padded = FixedBytes::<32>::left_padding_from(depositor.as_slice());
        self.state
            .across_deposits
            .lock()
            .await
            .values()
            .filter(|record| record.depositor == padded)
            .cloned()
            .collect()
    }

    /// Credit `amount` of `coin` (e.g. "USDC", "UBTC") to `user`'s mock
    /// Hyperliquid spot balance. Used by tests to fund an account before
    /// placing orders, without going through a deposit flow.
    pub async fn credit_hyperliquid_balance(&self, user: Address, coin: &str, amount: f64) {
        self.state
            .hyperliquid
            .lock()
            .await
            .credit_spot(user, coin, amount);
    }

    /// Credit `amount` of `coin` to the mock clearinghouse ledger. This is
    /// the balance surface real Bridge2 deposits land on before a
    /// `usdClassTransfer` moves funds into spot.
    pub async fn credit_hyperliquid_clearinghouse_balance(
        &self,
        user: Address,
        coin: &str,
        amount: f64,
    ) {
        self.state
            .hyperliquid
            .lock()
            .await
            .credit_clearinghouse(user, coin, amount);
    }

    /// Read the `total` balance for `user`/`coin`. Returns 0.0 when absent.
    pub async fn hyperliquid_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.state.hyperliquid.lock().await.spot_total(user, coin)
    }

    /// Read the clearinghouse balance for `user`/`coin`. Returns 0.0 when
    /// absent.
    pub async fn hyperliquid_clearinghouse_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.state
            .hyperliquid
            .lock()
            .await
            .clearinghouse_total(user, coin)
    }

    /// Install (or overwrite) the exchange rate for a spot pair. `rate` is the
    /// price of one `base` unit in `quote` units (e.g. `set_hyperliquid_rate
    /// ("UBTC", "USDC", 60_000.0)` means 1 UBTC = 60 000 USDC).
    ///
    /// An IoC buy of `sz` base at limit_px ≥ rate fills at exactly `rate`,
    /// debiting `sz * rate` quote and crediting `sz` base. A sell mirrors the
    /// direction.
    pub async fn set_hyperliquid_rate(&self, base: &str, quote: &str, rate: f64) {
        self.state
            .hyperliquid
            .lock()
            .await
            .set_rate(base, quote, rate);
    }

    /// Override the synthesized top-of-book depth for both bid and ask sides
    /// of a spot pair. Useful for testing partial-fill semantics.
    pub async fn set_hyperliquid_book_depth(&self, base: &str, quote: &str, depth: f64) {
        self.state
            .hyperliquid
            .lock()
            .await
            .set_book_depth(base, quote, depth);
    }

    /// Inspect the configured rate for a pair (returns `None` if none installed).
    pub async fn hyperliquid_rate(&self, base: &str, quote: &str) -> Option<f64> {
        self.state.hyperliquid.lock().await.rate_for(base, quote)
    }
}

impl MockIntegratorState {
    fn new(config: &MockIntegratorConfig) -> Self {
        Self {
            across_spoke_pool_address: config.across_spoke_pool_address.clone(),
            across_evm_rpc_url: config.across_evm_rpc_url.clone(),
            hyperliquid_bridge_address: config.hyperliquid_bridge_address.clone(),
            hyperliquid_evm_rpc_url: config.hyperliquid_evm_rpc_url.clone(),
            unit_generate_address_requests: Mutex::default(),
            unit_operations: Mutex::default(),
            ledger: Mutex::default(),
            across_deposits: Mutex::default(),
            cctp_burns: Mutex::default(),
            hyperliquid_exchange_submissions: Mutex::default(),
            hyperliquid: Mutex::new(HyperliquidMockState::new()),
            next_across_swap_approval_error: Mutex::default(),
            next_unit_generate_address_error: Mutex::default(),
            next_hyperliquid_exchange_error: Mutex::default(),
            mainnet_hyperliquid: config.mainnet_hyperliquid,
            across_api_key: config.across_api_key.clone(),
            across_integrator_id: config.across_integrator_id.clone(),
            across_quote_fee_bps: config.across_quote_fee_bps.min(9_999),
            across_quote_jitter_bps: config.across_quote_jitter_bps.min(9_999),
            hyperliquid_withdrawal_latency: config.hyperliquid_withdrawal_latency,
            across_status_latency: config.across_status_latency,
            across_refund_probability_bps: config.across_refund_probability_bps.min(10_000),
        }
    }
}

impl Drop for MockIntegratorServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.across_indexer_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.hyperliquid_indexer_shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.cctp_indexer_shutdown.take() {
            let _ = shutdown.send(());
        }
        self.handle.abort();
        if let Some(handle) = self.across_indexer_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.hyperliquid_indexer_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.cctp_indexer_handle.take() {
            handle.abort();
        }
    }
}

impl MockIntegratorLedger {
    fn snapshot(
        &self,
        hyperliquid_exchange_submissions: Vec<Value>,
        unit_operations: Vec<MockUnitOperationRecord>,
        cctp_burns: Vec<MockCctpBurnRecord>,
    ) -> MockIntegratorLedgerSnapshot {
        MockIntegratorLedgerSnapshot {
            bridged_balances: stringify_balances(&self.bridged_balances),
            hyperliquid_balances: stringify_balances(&self.hyperliquid_balances),
            recipient_balances: stringify_balances(&self.recipient_balances),
            unit_operations,
            cctp_burns,
            hyperliquid_exchange_submissions,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossRealSwapApprovalQuery {
    trade_type: String,
    origin_chain_id: u64,
    #[allow(dead_code)]
    destination_chain_id: u64,
    #[allow(dead_code)]
    input_token: String,
    #[allow(dead_code)]
    output_token: String,
    amount: String,
    #[allow(dead_code)]
    depositor: String,
    #[allow(dead_code)]
    recipient: String,
    #[allow(dead_code)]
    refund_address: Option<String>,
    #[allow(dead_code)]
    refund_on_origin: Option<bool>,
    #[allow(dead_code)]
    strict_trade_type: Option<bool>,
    #[allow(dead_code)]
    integrator_id: Option<String>,
    #[allow(dead_code)]
    slippage: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossRealTransaction {
    chain_id: u64,
    to: String,
    data: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_priority_fee_per_gas: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ecosystem: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    simulation_success: Option<bool>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossRealSwapApprovalResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_txns: Option<Vec<MockAcrossRealTransaction>>,
    swap_tx: MockAcrossRealTransaction,
    input_amount: String,
    max_input_amount: String,
    expected_output_amount: String,
    min_output_amount: String,
    quote_expiry_timestamp: i64,
    id: String,
    amount_type: String,
    cross_swap_type: String,
    expected_fill_time: u64,
    fees: Value,
    checks: Value,
    steps: Value,
    input_token: Value,
    output_token: Value,
    refund_token: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MockVeloraPricesQuery {
    src_token: String,
    dest_token: String,
    src_decimals: u8,
    dest_decimals: u8,
    amount: String,
    side: String,
    network: u64,
    #[allow(dead_code)]
    version: Option<String>,
    #[allow(dead_code)]
    ignore_bad_usd_price: Option<bool>,
    #[allow(dead_code)]
    exclude_rfq: Option<bool>,
    #[allow(dead_code)]
    user_address: Option<String>,
    #[allow(dead_code)]
    receiver: Option<String>,
    #[allow(dead_code)]
    partner: Option<String>,
}

async fn mock_velora_prices(Query(query): Query<MockVeloraPricesQuery>) -> impl IntoResponse {
    let amount = match parse_amount("amount", &query.amount) {
        Ok(amount) => amount,
        Err(error) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "invalid_amount",
                    "message": error,
                })),
            )
                .into_response();
        }
    };
    let (src_amount, dest_amount) = match query.side.as_str() {
        "SELL" => (amount, amount),
        "BUY" => (amount, amount),
        other => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({
                    "error": "unsupported_side",
                    "message": format!("unsupported side: {other}"),
                })),
            )
                .into_response();
        }
    };

    Json(json!({
        "priceRoute": {
            "network": query.network,
            "srcToken": query.src_token,
            "destToken": query.dest_token,
            "srcDecimals": query.src_decimals,
            "destDecimals": query.dest_decimals,
            "srcAmount": src_amount.to_string(),
            "destAmount": dest_amount.to_string(),
            "tokenTransferProxy": "0x0000000000000000000000000000000000000001",
            "contractAddress": "0x0000000000000000000000000000000000000002",
            "bestRoute": [],
        }
    }))
    .into_response()
}

async fn mock_velora_transaction(
    Path(network): Path<u64>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let dest_token = body
        .get("destToken")
        .and_then(Value::as_str)
        .unwrap_or("0x0000000000000000000000000000000000000002");
    let receiver = body
        .get("receiver")
        .and_then(Value::as_str)
        .or_else(|| body.get("userAddress").and_then(Value::as_str))
        .unwrap_or("0x0000000000000000000000000000000000000000");
    let dest_amount = body
        .get("priceRoute")
        .and_then(|value| value.get("destAmount"))
        .and_then(|value| mock_velora_u128(value).ok())
        .unwrap_or_default();
    let (to, data) = match (Address::from_str(dest_token), Address::from_str(receiver)) {
        (Ok(dest_token), Ok(receiver)) if dest_token != Address::ZERO => {
            let call = IMockMintableERC20::mintCall {
                recipient: receiver,
                amount: U256::from(dest_amount),
            };
            (
                format!("{dest_token:#x}"),
                format!("0x{}", hex::encode(call.abi_encode())),
            )
        }
        _ => (
            "0x0000000000000000000000000000000000000002".to_string(),
            "0x".to_string(),
        ),
    };
    Json(json!({
        "network": network,
        "to": to,
        "data": data,
        "value": "0",
        "request": body,
    }))
}

fn mock_velora_u128(value: &Value) -> Result<u128, String> {
    match value {
        Value::String(raw) => parse_amount("velora amount", raw),
        Value::Number(number) => number
            .as_u64()
            .map(u128::from)
            .ok_or_else(|| format!("velora amount is not a u64: {number}")),
        other => Err(format!("velora amount is not a string/number: {other}")),
    }
}

async fn mock_across_real_swap_approval(
    State(state): State<Arc<MockIntegratorState>>,
    headers: HeaderMap,
    Query(query): Query<MockAcrossRealSwapApprovalQuery>,
) -> impl IntoResponse {
    if let Err(response) = validate_across_request(&state, &headers, query.integrator_id.as_deref())
    {
        return *response;
    }
    if let Some(error) = state.next_across_swap_approval_error.lock().await.take() {
        return mock_across_error_response(
            StatusCode::BAD_GATEWAY,
            "bad_gateway",
            "mock_across_upstream_error",
            error,
            None,
        );
    }
    let amount = match parse_amount("amount", &query.amount) {
        Ok(amount) => amount,
        Err(error) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_amount",
                error,
                Some("amount"),
            );
        }
    };
    match query.trade_type.as_str() {
        "exactInput" | "minOutput" | "exactOutput" => {}
        other => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "unsupported_trade_type",
                format!("unsupported tradeType: {other}"),
                Some("tradeType"),
            );
        }
    }
    let Some(spoke_pool_address_str) = state.across_spoke_pool_address.as_ref() else {
        return mock_across_error_response(
            StatusCode::FAILED_DEPENDENCY,
            "configuration_error",
            "missing_spoke_pool",
            "mock Across: across_spoke_pool_address is not configured",
            None,
        );
    };
    let spoke_pool_address = match Address::from_str(spoke_pool_address_str) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "configuration_error",
                "invalid_spoke_pool",
                format!("mock Across: invalid spoke_pool_address: {err}"),
                None,
            );
        }
    };
    let input_token_addr = match Address::from_str(&query.input_token) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_input_token",
                format!("inputToken is not a valid address: {err}"),
                Some("inputToken"),
            );
        }
    };
    let output_token_addr = match Address::from_str(&query.output_token) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_output_token",
                format!("outputToken is not a valid address: {err}"),
                Some("outputToken"),
            );
        }
    };
    let depositor_addr = match Address::from_str(&query.depositor) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_depositor",
                format!("depositor is not a valid address: {err}"),
                Some("depositor"),
            );
        }
    };
    let recipient_addr = match Address::from_str(&query.recipient) {
        Ok(addr) => addr,
        Err(err) => {
            return mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_recipient",
                format!("recipient is not a valid address: {err}"),
                Some("recipient"),
            );
        }
    };

    let quote_amounts = mock_across_quote_amounts(&state, &query, amount);
    let input_amount_u256 = U256::from(quote_amounts.input_amount);
    let output_amount_u256 = U256::from(quote_amounts.output_amount);
    let now_secs = Utc::now().timestamp();
    let quote_timestamp = u32::try_from(now_secs.max(0)).unwrap_or(u32::MAX);
    let fill_deadline = u32::try_from((now_secs + 300).max(0)).unwrap_or(u32::MAX);

    let deposit_call = depositCall {
        depositor: address_to_bytes32(depositor_addr),
        recipient: address_to_bytes32(recipient_addr),
        inputToken: address_to_bytes32(input_token_addr),
        outputToken: address_to_bytes32(output_token_addr),
        inputAmount: input_amount_u256,
        outputAmount: output_amount_u256,
        destinationChainId: U256::from(query.destination_chain_id),
        exclusiveRelayer: FixedBytes::<32>::ZERO,
        quoteTimestamp: quote_timestamp,
        fillDeadline: fill_deadline,
        exclusivityDeadline: 0,
        message: alloy::primitives::Bytes::new(),
    };
    let deposit_calldata = format!("0x{}", hex::encode(deposit_call.abi_encode()));

    let allowance = if input_token_addr == Address::ZERO {
        None
    } else {
        mock_across_allowance(&state, input_token_addr, depositor_addr, spoke_pool_address).await
    };
    let needs_approval =
        input_token_addr != Address::ZERO && allowance.unwrap_or_default() < input_amount_u256;
    let balance = mock_across_balance(&state, input_token_addr, depositor_addr)
        .await
        .unwrap_or(input_amount_u256);

    let (approval_txns, swap_value) = if input_token_addr == Address::ZERO {
        (None, Some(input_amount_u256.to_string()))
    } else if needs_approval {
        let approve_call = IERC20::approveCall {
            spender: spoke_pool_address,
            amount: U256::MAX,
        };
        let approve_calldata = format!("0x{}", hex::encode(approve_call.abi_encode()));
        let approval_tx = MockAcrossRealTransaction {
            chain_id: query.origin_chain_id,
            to: format!("{input_token_addr:#x}"),
            data: approve_calldata,
            value: None,
            gas: None,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            ecosystem: None,
            simulation_success: None,
        };
        (Some(vec![approval_tx]), None)
    } else {
        (None, None)
    };

    let approval_required = approval_txns.is_some();
    let swap_tx = MockAcrossRealTransaction {
        chain_id: query.origin_chain_id,
        to: format!("{spoke_pool_address:#x}"),
        data: deposit_calldata,
        value: swap_value,
        gas: Some(if approval_required { "0" } else { "84804" }.to_string()),
        max_fee_per_gas: (!approval_required).then(|| "13500000".to_string()),
        max_priority_fee_per_gas: (!approval_required).then(|| "1000000".to_string()),
        ecosystem: Some("evm".to_string()),
        simulation_success: Some(true),
    };

    let quote_expiry = (Utc::now() + ChronoDuration::seconds(60)).timestamp();
    let input_token_meta = mock_across_token_metadata(input_token_addr, query.origin_chain_id);
    let output_token_meta =
        mock_across_token_metadata(output_token_addr, query.destination_chain_id);
    let refund_token_meta = input_token_meta.clone();
    let fee_amount = quote_amounts
        .input_amount
        .saturating_sub(quote_amounts.expected_output_amount);
    let response = MockAcrossRealSwapApprovalResponse {
        approval_txns,
        swap_tx,
        input_amount: quote_amounts.input_amount.to_string(),
        max_input_amount: quote_amounts.max_input_amount.to_string(),
        expected_output_amount: quote_amounts.expected_output_amount.to_string(),
        min_output_amount: quote_amounts.min_output_amount.to_string(),
        quote_expiry_timestamp: quote_expiry,
        id: format!("mock-across-{}", Uuid::now_v7()),
        amount_type: query.trade_type.clone(),
        cross_swap_type: mock_across_cross_swap_type(input_token_addr, output_token_addr),
        expected_fill_time: 2,
        fees: mock_across_fee_breakdown(
            quote_amounts.input_amount,
            fee_amount,
            &input_token_meta,
            &output_token_meta,
            query.origin_chain_id,
        ),
        checks: mock_across_checks(
            input_token_addr,
            allowance,
            balance,
            input_amount_u256,
            spoke_pool_address,
        ),
        steps: mock_across_steps(
            quote_amounts.input_amount,
            quote_amounts.expected_output_amount,
            fee_amount,
            &input_token_meta,
            &output_token_meta,
        ),
        input_token: input_token_meta,
        output_token: output_token_meta,
        refund_token: refund_token_meta,
    };
    Json(response).into_response()
}

async fn mock_across_allowance(
    state: &MockIntegratorState,
    token: Address,
    owner: Address,
    spender: Address,
) -> Option<U256> {
    let rpc_url = state.across_evm_rpc_url.as_deref()?;
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            tracing::warn!(%err, "mock across swap approval: failed to connect RPC for allowance check");
            return None;
        }
    };
    let token = IERC20::new(token, provider);
    match token.allowance(owner, spender).call().await {
        Ok(allowance) => Some(allowance),
        Err(err) => {
            tracing::warn!(%err, "mock across swap approval: allowance check failed");
            None
        }
    }
}

async fn mock_across_balance(
    state: &MockIntegratorState,
    token: Address,
    owner: Address,
) -> Option<U256> {
    let rpc_url = state.across_evm_rpc_url.as_deref()?;
    let provider = match ProviderBuilder::new().connect(rpc_url).await {
        Ok(provider) => provider,
        Err(err) => {
            tracing::warn!(%err, "mock across swap approval: failed to connect RPC for balance check");
            return None;
        }
    };

    if token == Address::ZERO {
        match provider.get_balance(owner).await {
            Ok(balance) => Some(balance),
            Err(err) => {
                tracing::warn!(%err, "mock across swap approval: native balance check failed");
                None
            }
        }
    } else {
        let token = IERC20::new(token, provider);
        match token.balanceOf(owner).call().await {
            Ok(balance) => Some(balance),
            Err(err) => {
                tracing::warn!(%err, "mock across swap approval: token balance check failed");
                None
            }
        }
    }
}

fn mock_across_cross_swap_type(input_token: Address, output_token: Address) -> String {
    match (input_token == Address::ZERO, output_token == Address::ZERO) {
        (true, true) => "nativeToNative",
        (true, false) => "nativeToBridgeable",
        (false, true) => "bridgeableToNative",
        (false, false) => "bridgeableToBridgeable",
    }
    .to_string()
}

fn mock_across_checks(
    input_token: Address,
    allowance: Option<U256>,
    balance: U256,
    required_amount: U256,
    spender: Address,
) -> Value {
    let mut checks = serde_json::Map::new();
    if input_token != Address::ZERO {
        checks.insert(
            "allowance".to_string(),
            json!({
                "actual": allowance.unwrap_or_default().to_string(),
                "expected": required_amount.to_string(),
                "spender": format!("{spender:#x}"),
                "token": format!("{input_token:#x}"),
            }),
        );
    }
    checks.insert(
        "balance".to_string(),
        json!({
            "actual": balance.to_string(),
            "expected": required_amount.to_string(),
            "token": format!("{input_token:#x}"),
        }),
    );
    Value::Object(checks)
}

fn mock_across_steps(
    input_amount: u128,
    output_amount: u128,
    fee_amount: u128,
    input_token: &Value,
    output_token: &Value,
) -> Value {
    let mut steps = serde_json::Map::new();
    steps.insert(
        "bridge".to_string(),
        json!({
            "provider": "across",
            "inputAmount": input_amount.to_string(),
            "outputAmount": output_amount.to_string(),
            "tokenIn": input_token,
            "tokenOut": output_token,
            "fees": {
                "amount": fee_amount.to_string(),
                "pct": mock_across_fee_pct_string(input_amount, fee_amount),
                "token": input_token,
                "details": {
                    "destinationGas": {
                        "amount": fee_amount.saturating_sub(140).to_string(),
                        "pct": mock_across_fee_pct_string(input_amount, fee_amount.saturating_sub(140)),
                        "token": input_token,
                    },
                    "lp": {
                        "amount": fee_amount.min(40).to_string(),
                        "pct": mock_across_fee_pct_string(input_amount, fee_amount.min(40)),
                        "token": input_token,
                    },
                    "relayerCapital": {
                        "amount": fee_amount.saturating_sub(fee_amount.saturating_sub(100)).to_string(),
                        "pct": mock_across_fee_pct_string(
                            input_amount,
                            fee_amount.saturating_sub(fee_amount.saturating_sub(100)),
                        ),
                        "token": input_token,
                    },
                    "type": "across",
                },
            },
        }),
    );
    Value::Object(steps)
}

fn mock_across_fee_breakdown(
    input_amount: u128,
    fee_amount: u128,
    input_token: &Value,
    output_token: &Value,
    origin_chain_id: u64,
) -> Value {
    let fee_pct = mock_across_fee_pct_string(input_amount, fee_amount);
    let origin_gas_token = mock_across_token_metadata(Address::ZERO, origin_chain_id);
    let destination_gas_amount = fee_amount.saturating_sub(140);
    let lp_amount = fee_amount.min(40);
    let relayer_capital_amount = fee_amount.saturating_sub(destination_gas_amount + lp_amount);
    json!({
        "originGas": {
            "amount": "0",
            "amountUsd": "0.0",
            "token": origin_gas_token,
        },
        "total": {
            "amount": fee_amount.to_string(),
            "amountUsd": "0.0",
            "pct": fee_pct,
            "token": input_token,
            "details": {
                "app": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": output_token,
                },
                "bridge": {
                    "amount": fee_amount.to_string(),
                    "amountUsd": "0.0",
                    "pct": fee_pct,
                    "token": input_token,
                    "details": {
                        "destinationGas": {
                            "amount": destination_gas_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, destination_gas_amount),
                            "token": input_token,
                        },
                        "lp": {
                            "amount": lp_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, lp_amount),
                            "token": input_token,
                        },
                        "relayerCapital": {
                            "amount": relayer_capital_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, relayer_capital_amount),
                            "token": input_token,
                        },
                        "type": "across",
                    },
                },
                "swapImpact": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": input_token,
                },
                "type": "total-breakdown",
            },
        },
        "totalMax": {
            "amount": fee_amount.to_string(),
            "amountUsd": "0.0",
            "pct": fee_pct,
            "token": input_token,
            "details": {
                "app": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": output_token,
                },
                "bridge": {
                    "amount": fee_amount.to_string(),
                    "amountUsd": "0.0",
                    "pct": fee_pct,
                    "token": input_token,
                    "details": {
                        "destinationGas": {
                            "amount": destination_gas_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, destination_gas_amount),
                            "token": input_token,
                        },
                        "lp": {
                            "amount": lp_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, lp_amount),
                            "token": input_token,
                        },
                        "relayerCapital": {
                            "amount": relayer_capital_amount.to_string(),
                            "amountUsd": "0.0",
                            "pct": mock_across_fee_pct_string(input_amount, relayer_capital_amount),
                            "token": input_token,
                        },
                        "type": "across",
                    },
                },
                "maxSwapImpact": {
                    "amount": "0",
                    "amountUsd": "0.0",
                    "pct": "0",
                    "token": input_token,
                },
                "type": "max-total-breakdown",
            },
        },
    })
}

fn mock_across_fee_pct_string(input_amount: u128, fee_amount: u128) -> String {
    if input_amount == 0 {
        return "0".to_string();
    }
    ((fee_amount.saturating_mul(1_000_000_000_000_000_000_u128)) / input_amount).to_string()
}

fn mock_across_token_metadata(address: Address, chain_id: u64) -> Value {
    match format!("{address:#x}").as_str() {
        "0x0000000000000000000000000000000000000000" => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 18,
            "symbol": "ETH",
        }),
        "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"
        | "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48" => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 6,
            "name": "USD Coin",
            "symbol": "USDC",
        }),
        _ => json!({
            "address": format!("{address:#x}"),
            "chainId": chain_id,
            "decimals": 18,
            "name": "Mock Token",
            "symbol": "MOCK",
        }),
    }
}

#[derive(Debug)]
struct MockAcrossQuoteAmounts {
    input_amount: u128,
    max_input_amount: u128,
    expected_output_amount: u128,
    min_output_amount: u128,
    output_amount: u128,
}

fn mock_across_quote_amounts(
    state: &MockIntegratorState,
    query: &MockAcrossRealSwapApprovalQuery,
    amount: u128,
) -> MockAcrossQuoteAmounts {
    let fee_bps = mock_across_quote_fee_bps(state, query);
    match query.trade_type.as_str() {
        "exactInput" => {
            let output = apply_mock_across_discount(amount, fee_bps);
            MockAcrossQuoteAmounts {
                input_amount: amount,
                max_input_amount: amount,
                expected_output_amount: output,
                min_output_amount: output,
                output_amount: output,
            }
        }
        "minOutput" | "exactOutput" => {
            let input = gross_up_mock_across_input(amount, fee_bps);
            MockAcrossQuoteAmounts {
                input_amount: input,
                max_input_amount: input,
                expected_output_amount: amount,
                min_output_amount: amount,
                output_amount: amount,
            }
        }
        _ => unreachable!("trade type was already validated"),
    }
}

fn mock_across_quote_fee_bps(
    state: &MockIntegratorState,
    query: &MockAcrossRealSwapApprovalQuery,
) -> u16 {
    let base = state.across_quote_fee_bps;
    let jitter = deterministic_bps(
        &format!(
            "across-quote:{}:{}:{}:{}:{}:{}",
            query.trade_type,
            query.origin_chain_id,
            query.destination_chain_id,
            query.input_token,
            query.output_token,
            query.amount
        ),
        state.across_quote_jitter_bps,
    );
    base.saturating_add(jitter).min(9_999)
}

fn apply_mock_across_discount(amount: u128, fee_bps: u16) -> u128 {
    let keep_bps = 10_000_u128.saturating_sub(u128::from(fee_bps));
    amount.saturating_mul(keep_bps) / 10_000_u128
}

fn gross_up_mock_across_input(amount: u128, fee_bps: u16) -> u128 {
    let keep_bps = 10_000_u128.saturating_sub(u128::from(fee_bps)).max(1);
    amount.saturating_mul(10_000_u128).div_ceil(keep_bps)
}

fn mock_across_should_refund(
    state: &MockIntegratorState,
    record: &MockAcrossDepositRecord,
) -> bool {
    deterministic_bps(
        &format!(
            "across-status:{}:{}:{}",
            record.origin_chain_id, record.deposit_id, record.deposit_tx_hash
        ),
        9_999,
    ) < state.across_refund_probability_bps
}

fn deterministic_bps(seed: &str, max_bps: u16) -> u16 {
    if max_bps == 0 {
        return 0;
    }
    let digest = Sha256::digest(seed.as_bytes());
    let raw = u16::from_be_bytes([digest[0], digest[1]]);
    raw % (max_bps.saturating_add(1))
}

fn address_to_bytes32(address: Address) -> FixedBytes<32> {
    let mut bytes = [0u8; 32];
    bytes[12..].copy_from_slice(address.as_slice());
    FixedBytes::<32>::from(bytes)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossDepositStatusQuery {
    origin_chain_id: Option<u64>,
    deposit_id: Option<String>,
    deposit_txn_ref: Option<String>,
}

/// Shape mirrors Across' real `GET /deposit/status` response (camelCase). The
/// mock serves `"filled"` once the FundsDeposited event has been observed
/// on-chain. Queries by `depositTxnRef` can temporarily return the same
/// `DepositNotFoundException` 404 that real Across returns before indexing
/// catches up.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossDepositStatusResponse {
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    origin_chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination_chain_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deposit_refund_txn_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fill_deadline: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pagination: Option<Value>,
}

async fn mock_across_deposit_status(
    State(state): State<Arc<MockIntegratorState>>,
    headers: HeaderMap,
    Query(query): Query<MockAcrossDepositStatusQuery>,
) -> impl IntoResponse {
    if let Err(response) = validate_across_request(&state, &headers, None) {
        return *response;
    }
    let deposits = state.across_deposits.lock().await;
    let record = if let (Some(origin_chain_id), Some(deposit_id)) =
        (query.origin_chain_id, query.deposit_id.as_deref())
    {
        deposits.get(&(origin_chain_id, deposit_id.to_string()))
    } else if let Some(deposit_txn_ref) = query.deposit_txn_ref.as_deref() {
        deposits
            .values()
            .find(|record| record.deposit_tx_hash.eq_ignore_ascii_case(deposit_txn_ref))
    } else {
        return mock_across_error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "validation_error",
            "missing_deposit_lookup",
            "deposit/status requires depositTxnRef or originChainId + depositId",
            None,
        );
    };
    let Some(record) = record else {
        if query.deposit_txn_ref.is_some() {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({
                    "error": "DepositNotFoundException",
                    "message": "Deposit not found given the provided constraints",
                })),
            )
                .into_response();
        }
        return Json(MockAcrossDepositStatusResponse {
            status: "pending".to_string(),
            origin_chain_id: query.origin_chain_id,
            destination_chain_id: None,
            deposit_id: query.deposit_id,
            deposit_txn_ref: query.deposit_txn_ref,
            fill_txn_ref: None,
            deposit_refund_txn_ref: None,
            fill_deadline: None,
            pagination: Some(mock_across_pagination()),
        })
        .into_response();
    };
    if Utc::now()
        .signed_duration_since(record.indexed_at)
        .to_std()
        .unwrap_or_default()
        < state.across_status_latency
    {
        return Json(MockAcrossDepositStatusResponse {
            status: "pending".to_string(),
            origin_chain_id: Some(record.origin_chain_id),
            destination_chain_id: Some(
                u64::try_from(record.destination_chain_id).unwrap_or_default(),
            ),
            deposit_id: Some(record.deposit_id.to_string()),
            deposit_txn_ref: Some(record.deposit_tx_hash.clone()),
            fill_txn_ref: None,
            deposit_refund_txn_ref: None,
            fill_deadline: None,
            pagination: Some(mock_across_pagination()),
        })
        .into_response();
    }

    let should_refund = mock_across_should_refund(&state, record);
    let (status, fill_txn_ref, deposit_refund_txn_ref) = if should_refund {
        (
            "refunded".to_string(),
            None,
            Some(format!("0x{}", "de".repeat(32))),
        )
    } else {
        (
            "filled".to_string(),
            Some(format!("0x{}", "fa".repeat(32))),
            None,
        )
    };
    Json(MockAcrossDepositStatusResponse {
        status,
        origin_chain_id: Some(record.origin_chain_id),
        destination_chain_id: Some(u64::try_from(record.destination_chain_id).unwrap_or_default()),
        deposit_id: Some(record.deposit_id.to_string()),
        deposit_txn_ref: Some(record.deposit_tx_hash.clone()),
        fill_txn_ref,
        deposit_refund_txn_ref,
        fill_deadline: None,
        pagination: Some(mock_across_pagination()),
    })
    .into_response()
}

fn mock_across_pagination() -> Value {
    json!({
        "currentIndex": 0,
        "maxIndex": 0,
    })
}

#[derive(Debug, Deserialize)]
struct MockCctpMessagesQuery {
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    nonce: Option<String>,
}

async fn mock_cctp_messages(
    State(state): State<Arc<MockIntegratorState>>,
    Path(source_domain): Path<u32>,
    Query(query): Query<MockCctpMessagesQuery>,
) -> impl IntoResponse {
    let burns = state.cctp_burns.lock().await;
    let record = if let Some(tx_hash) = query.transaction_hash.as_deref() {
        burns.get(tx_hash).cloned()
    } else if let Some(nonce) = query.nonce.as_deref() {
        burns
            .values()
            .find(|record| record.nonce.to_string() == nonce)
            .cloned()
    } else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "mock CCTP /v2/messages requires transactionHash or nonce".to_string(),
        );
    };
    let Some(record) = record.filter(|record| record.source_domain == source_domain) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "not found"
            })),
        )
            .into_response();
    };
    let message = (
        record.destination_token,
        record.mint_recipient,
        record.amount,
    )
        .abi_encode();
    (
        StatusCode::OK,
        Json(json!({
            "messages": [{
                "message": format!("0x{}", hex::encode(message)),
                "attestation": format!("0x{}", "11".repeat(65)),
                "eventNonce": record.nonce.to_string(),
                "cctpVersion": 2,
                "status": "complete",
                "decodedMessageBody": {
                    "burnToken": format!("{:#x}", record.burn_token),
                    "mintRecipient": format!("{:#x}", record.mint_recipient),
                    "amount": record.amount.to_string(),
                    "messageSender": format!("{:#x}", record.depositor)
                }
            }]
        })),
    )
        .into_response()
}

async fn mock_cctp_burn_fees(
    Path((_source_domain, _destination_domain)): Path<(u32, u32)>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!([
            {
                "finalityThreshold": 1000,
                "minimumFee": 1.3
            },
            {
                "finalityThreshold": 2000,
                "minimumFee": 0
            }
        ])),
    )
        .into_response()
}

async fn mock_coinbase_spot_price(Path(currency_pair): Path<String>) -> impl IntoResponse {
    let (base, amount) = match currency_pair.as_str() {
        "ETH-USD" => ("ETH", "3000"),
        "BTC-USD" => ("BTC", "100000"),
        "USDC-USD" => ("USDC", "1"),
        "USDT-USD" => ("USDT", "1"),
        _ => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!("mock Coinbase spot price not found for {currency_pair}"),
            );
        }
    };
    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "amount": amount,
                "base": base,
                "currency": "USD"
            }
        })),
    )
        .into_response()
}

async fn maybe_spawn_across_deposit_indexer(
    config: &MockIntegratorConfig,
    state: Arc<MockIntegratorState>,
) -> eyre::Result<(
    Option<tokio::sync::oneshot::Sender<()>>,
    Option<JoinHandle<()>>,
)> {
    let (Some(rpc_url), Some(spoke_pool_str)) = (
        config.across_evm_rpc_url.as_deref(),
        config.across_spoke_pool_address.as_deref(),
    ) else {
        return Ok((None, None));
    };
    let spoke_pool_address: Address = spoke_pool_str.parse().map_err(|err| {
        eyre::eyre!("mock across indexer: invalid spoke pool address {spoke_pool_str}: {err}")
    })?;
    let provider: DynProvider = ProviderBuilder::new().connect(rpc_url).await?.erased();
    let chain_id = provider.get_chain_id().await?;

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_across_deposit_indexer(
        provider,
        spoke_pool_address,
        chain_id,
        state,
        shutdown_rx,
    ));
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn maybe_spawn_hyperliquid_bridge_indexer(
    config: &MockIntegratorConfig,
    state: Arc<MockIntegratorState>,
) -> eyre::Result<(
    Option<tokio::sync::oneshot::Sender<()>>,
    Option<JoinHandle<()>>,
)> {
    let (Some(rpc_url), Some(bridge_str), Some(token_str)) = (
        config.hyperliquid_evm_rpc_url.as_deref(),
        config.hyperliquid_bridge_address.as_deref(),
        config.hyperliquid_usdc_token_address.as_deref(),
    ) else {
        return Ok((None, None));
    };
    let bridge_address: Address = bridge_str.parse().map_err(|err| {
        eyre::eyre!("mock hyperliquid bridge indexer: invalid bridge address {bridge_str}: {err}")
    })?;
    let token_address: Address = token_str.parse().map_err(|err| {
        eyre::eyre!("mock hyperliquid bridge indexer: invalid token address {token_str}: {err}")
    })?;
    let provider: DynProvider = ProviderBuilder::new().connect(rpc_url).await?.erased();

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_hyperliquid_bridge_indexer(
        provider,
        token_address,
        bridge_address,
        state,
        shutdown_rx,
    ));
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn maybe_spawn_cctp_burn_indexer(
    config: &MockIntegratorConfig,
    state: Arc<MockIntegratorState>,
) -> eyre::Result<(
    Option<tokio::sync::oneshot::Sender<()>>,
    Option<JoinHandle<()>>,
)> {
    let (Some(rpc_url), Some(token_messenger_str), Some(destination_token_str)) = (
        config.cctp_evm_rpc_url.as_deref(),
        config.cctp_token_messenger_address.as_deref(),
        config.cctp_destination_token_address.as_deref(),
    ) else {
        return Ok((None, None));
    };
    let token_messenger_address: Address = token_messenger_str.parse().map_err(|err| {
        eyre::eyre!(
            "mock cctp indexer: invalid TokenMessengerV2 address {token_messenger_str}: {err}"
        )
    })?;
    let destination_token_address: Address = destination_token_str.parse().map_err(|err| {
        eyre::eyre!(
            "mock cctp indexer: invalid destination token address {destination_token_str}: {err}"
        )
    })?;
    let provider: DynProvider = ProviderBuilder::new().connect(rpc_url).await?.erased();
    let chain_id = provider.get_chain_id().await?;
    let source_domain = cctp_domain_for_chain_id(chain_id).unwrap_or(6);

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(run_cctp_burn_indexer(
        provider,
        token_messenger_address,
        destination_token_address,
        source_domain,
        state,
        shutdown_rx,
    ));
    Ok((Some(shutdown_tx), Some(handle)))
}

async fn run_across_deposit_indexer(
    provider: DynProvider,
    spoke_pool_address: Address,
    chain_id: u64,
    state: Arc<MockIntegratorState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = FundsDeposited::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = provider.get_block_number().await.unwrap_or(0);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock across indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(spoke_pool_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock across indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match FundsDeposited::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock across indexer: decode FundsDeposited failed");
                    continue;
                }
            };
            let event = decoded.data;
            let record = MockAcrossDepositRecord {
                origin_chain_id: chain_id,
                destination_chain_id: event.destinationChainId,
                deposit_id: event.depositId,
                depositor: event.depositor,
                recipient: event.recipient,
                input_token: event.inputToken,
                output_token: event.outputToken,
                input_amount: event.inputAmount,
                output_amount: event.outputAmount,
                fill_deadline: event.fillDeadline,
                deposit_tx_hash: log
                    .transaction_hash
                    .map(|h| format!("{h:#x}"))
                    .unwrap_or_default(),
                block_number: log.block_number.unwrap_or_default(),
                indexed_at: Utc::now(),
            };
            let key = (chain_id, record.deposit_id.to_string());
            state.across_deposits.lock().await.insert(key, record);
        }
        last_scanned = head;
    }
}

async fn run_cctp_burn_indexer(
    provider: DynProvider,
    token_messenger_address: Address,
    destination_token_address: Address,
    source_domain: u32,
    state: Arc<MockIntegratorState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = DepositForBurn::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = provider.get_block_number().await.unwrap_or(0);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock cctp indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(token_messenger_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock cctp indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match DepositForBurn::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock cctp indexer: decode DepositForBurn failed");
                    continue;
                }
            };
            let event = decoded.data;
            let burn_tx_hash = log
                .transaction_hash
                .map(|h| format!("{h:#x}"))
                .unwrap_or_default();
            let record = MockCctpBurnRecord {
                source_domain,
                destination_domain: event.destinationDomain,
                nonce: event.nonce,
                burn_token: event.burnToken,
                destination_token: destination_token_address,
                depositor: event.depositor,
                mint_recipient: bytes32_to_evm_address(event.mintRecipient),
                amount: event.amount,
                burn_tx_hash: burn_tx_hash.clone(),
                block_number: log.block_number.unwrap_or_default(),
                indexed_at: Utc::now(),
            };
            state.cctp_burns.lock().await.insert(burn_tx_hash, record);
        }
        last_scanned = head;
    }
}

async fn run_hyperliquid_bridge_indexer(
    provider: DynProvider,
    token_address: Address,
    bridge_address: Address,
    state: Arc<MockIntegratorState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = IERC20::Transfer::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = provider.get_block_number().await.unwrap_or(0);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock hyperliquid bridge indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(token_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock hyperliquid bridge indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match IERC20::Transfer::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock hyperliquid bridge indexer: decode Transfer failed");
                    continue;
                }
            };
            let event = decoded.data;
            if event.to != bridge_address {
                continue;
            }
            if event.value < U256::from(MINIMUM_BRIDGE_DEPOSIT_USDC) {
                tracing::debug!(
                    from = %event.from,
                    amount = %event.value,
                    "mock hyperliquid bridge indexer: ignoring sub-minimum bridge deposit"
                );
                continue;
            }
            let Some(amount) = raw_usdc_to_natural_f64(event.value) else {
                tracing::warn!(
                    from = %event.from,
                    amount = %event.value,
                    "mock hyperliquid bridge indexer: failed to convert transfer value"
                );
                continue;
            };
            state
                .hyperliquid
                .lock()
                .await
                .credit_clearinghouse(event.from, "USDC", amount);
        }
        last_scanned = head;
    }
}

fn raw_usdc_to_natural_f64(value: U256) -> Option<f64> {
    let raw: u128 = value.try_into().ok()?;
    Some(raw as f64 / 1_000_000.0)
}

fn cctp_domain_for_chain_id(chain_id: u64) -> Option<u32> {
    match chain_id {
        1 => Some(0),
        42161 => Some(3),
        8453 => Some(6),
        _ => None,
    }
}

fn bytes32_to_evm_address(value: FixedBytes<32>) -> Address {
    Address::from_slice(&value.as_slice()[12..])
}

/// Handles `GET /gen/{src_chain}/{dst_chain}/{asset}/{dst_addr}` — HyperUnit's
/// "generate protocol address" endpoint. The response mirrors
/// `hyperunit_client::UnitGenerateAddressResponse` byte-for-byte so production
/// and test code paths use the same parse logic.
///
/// The mock derives the protocol address deterministically from the path
/// parameters (sha256-based) so tests can predict what address the router will
/// observe. It also records the generated operation in the state's
/// `unit_operations` map, seeded at `sourceTxDiscovered` — tests advance the
/// state via `complete_unit_operation` / `fail_unit_operation`.
async fn mock_unit_gen(
    State(state): State<Arc<MockIntegratorState>>,
    Path((src_chain, dst_chain, asset, dst_addr)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    if let Some(error) = state.next_unit_generate_address_error.lock().await.take() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(MockIntegratorErrorResponse { error }),
        )
            .into_response();
    }
    let Some(src) = UnitChain::parse(&src_chain) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Unit /gen: unsupported src_chain {src_chain}"),
        );
    };
    let Some(dst) = UnitChain::parse(&dst_chain) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Unit /gen: unsupported dst_chain {dst_chain}"),
        );
    };
    let Some(unit_asset) = UnitAsset::parse(&asset) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Unit /gen: unsupported asset {asset}"),
        );
    };

    let request = MockUnitGenerateAddressRequest {
        src_chain: src_chain.clone(),
        dst_chain: dst_chain.clone(),
        asset: asset.clone(),
        dst_addr: dst_addr.clone(),
    };
    state
        .unit_generate_address_requests
        .lock()
        .await
        .push(request);

    // A deposit operation has the user's on-chain funds flowing INTO Hyperliquid:
    // src_chain is the external chain (bitcoin/ethereum), dst_chain is hyperliquid.
    // A withdrawal operation has HL funds flowing OUT to an external chain.
    let kind = if matches!(dst, UnitChain::Hyperliquid) {
        MockUnitOperationKind::Deposit
    } else if matches!(src, UnitChain::Hyperliquid) {
        MockUnitOperationKind::Withdrawal
    } else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Unit /gen: one of src/dst must be hyperliquid (got {src}/{dst})"),
        );
    };

    let protocol_address = derive_mock_protocol_address(&src_chain, &dst_chain, &asset, &dst_addr);
    {
        let mut operations = state.unit_operations.lock().await;
        operations
            .entry(protocol_address.clone())
            .or_default()
            .push(MockUnitOperationEntry {
                kind,
                src_chain: src,
                dst_chain: dst,
                asset: unit_asset,
                dst_addr: dst_addr.clone(),
                visible: false,
                operation: UnitOperation {
                    operation_id: None,
                    op_created_at: Some(Utc::now().to_rfc3339()),
                    protocol_address: Some(protocol_address.clone()),
                    source_address: None,
                    destination_address: Some(dst_addr.clone()),
                    source_chain: Some(src.as_wire_str().to_string()),
                    destination_chain: Some(dst.as_wire_str().to_string()),
                    source_amount: None,
                    destination_fee_amount: None,
                    sweep_fee_amount: None,
                    state: Some("sourceTxDiscovered".to_string()),
                    source_tx_hash: None,
                    destination_tx_hash: None,
                    source_tx_confirmations: None,
                    destination_tx_confirmations: None,
                    position_in_withdraw_queue: None,
                    broadcast_at: None,
                    asset: Some(unit_asset.as_wire_str().to_string()),
                    state_started_at: Some(Utc::now().to_rfc3339()),
                    state_updated_at: Some(Utc::now().to_rfc3339()),
                    state_next_attempt_at: None,
                },
            });
    }

    Json(UnitGenerateAddressResponse {
        address: protocol_address,
        status: Some("OK".to_string()),
        signatures: mock_unit_signatures(kind),
    })
    .into_response()
}

/// Handles `GET /operations/{address}` — HyperUnit's "look up operations by
/// protocol address" endpoint. Returns the tracked operation (if any) in the
/// exact `UnitOperationsResponse` shape production code parses.
async fn mock_unit_operations(
    State(state): State<Arc<MockIntegratorState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let operations = state.unit_operations.lock().await;
    let matched: Vec<_> = operations
        .values()
        .flat_map(|entries| entries.iter())
        .filter(|entry| entry.visible)
        .filter(|entry| unit_operation_matches_query(entry, &address))
        .collect();
    let query_is_protocol_address =
        operations
            .values()
            .flat_map(|entries| entries.iter())
            .any(|entry| {
                entry
                    .operation
                    .protocol_address
                    .as_deref()
                    .is_some_and(|protocol_address| addresses_match(protocol_address, &address))
            });
    let response = if matched.is_empty() {
        UnitOperationsResponse::default()
    } else {
        UnitOperationsResponse {
            addresses: if query_is_protocol_address {
                Vec::new()
            } else {
                let mut seen_protocol_addresses = BTreeSet::new();
                matched
                    .iter()
                    .filter(|entry| {
                        seen_protocol_addresses
                            .insert(entry.operation.protocol_address.clone().unwrap_or_default())
                    })
                    .map(|entry| {
                        json!({
                            "sourceCoinType": entry.src_chain.as_wire_str(),
                            "destinationChain": entry.dst_chain.as_wire_str(),
                            "address": entry.operation.protocol_address.clone().unwrap_or_default(),
                            "signatures": mock_unit_signatures(entry.kind)
                        })
                    })
                    .collect()
            },
            operations: matched
                .iter()
                .map(|entry| entry.operation.clone())
                .collect(),
        }
    };
    Json(response).into_response()
}

fn unit_operation_matches_query(entry: &MockUnitOperationEntry, query: &str) -> bool {
    entry
        .operation
        .protocol_address
        .as_deref()
        .is_some_and(|protocol_address| addresses_match(protocol_address, query))
        || addresses_match(&entry.dst_addr, query)
}

fn mock_unit_signatures(kind: MockUnitOperationKind) -> Value {
    match kind {
        MockUnitOperationKind::Deposit => json!({
            "field-node": "mock-field-node-sig",
            "hl-node": "mock-hl-node-sig",
            "unit-node": "mock-unit-node-sig"
        }),
        MockUnitOperationKind::Withdrawal => json!({
            "field-node": "mock-field-node-sig",
            "unit-node": "mock-unit-node-sig"
        }),
    }
}

fn addresses_match(left: &str, right: &str) -> bool {
    if left.starts_with("0x") && right.starts_with("0x") {
        left.eq_ignore_ascii_case(right)
    } else {
        left == right
    }
}

/// Mocks Hyperliquid's `POST /info` read-only endpoint. Dispatches on the
/// `type` discriminator — every supported response is round-trippable into
/// the matching `hyperliquid_client` type so clients exercising this mock
/// speak the real wire format.
async fn mock_hyperliquid_info(
    State(state): State<Arc<MockIntegratorState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let Some(kind) = request.get("type").and_then(Value::as_str) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "mock Hyperliquid /info requires a `type` field",
        );
    };
    let mut hl = state.hyperliquid.lock().await;
    hl.run_due_scheduled_cancels();
    match kind {
        "spotMeta" => Json(hl.spot_meta_value()).into_response(),
        "l2Book" => {
            let coin = request
                .get("coin")
                .and_then(Value::as_str)
                .unwrap_or("UBTC/USDC");
            Json(hl.l2_book_snapshot(coin)).into_response()
        }
        "orderStatus" => {
            let oid = request.get("oid").and_then(Value::as_u64).unwrap_or(0);
            Json(hl.order_status_snapshot(oid)).into_response()
        }
        "spotClearinghouseState" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "spotClearinghouseState requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.spot_clearinghouse_snapshot(user)).into_response()
        }
        "clearinghouseState" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "clearinghouseState requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.clearinghouse_snapshot(user)).into_response()
        }
        "openOrders" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "openOrders requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.open_orders_for(user)).into_response()
        }
        "userFills" => {
            let Some(user) = request
                .get("user")
                .and_then(Value::as_str)
                .and_then(parse_user_address)
            else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "userFills requires a 0x-prefixed `user` field",
                );
            };
            Json(hl.fills_for(user)).into_response()
        }
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Hyperliquid /info does not support type {other}"),
        ),
    }
}

/// Mocks Hyperliquid's `POST /exchange` signed-action endpoint. Parses the
/// action, recovers the signing address, and mutates the shared state —
/// fills against the configured L2 book, rests remainders in the user's
/// open-orders list, cancels, or moves spot balances between users — then
/// returns the real API's response shape.
async fn mock_hyperliquid_exchange(
    State(state): State<Arc<MockIntegratorState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    if let Some(error) = state.next_hyperliquid_exchange_error.lock().await.take() {
        return (
            StatusCode::BAD_GATEWAY,
            Json(MockIntegratorErrorResponse { error }),
        )
            .into_response();
    }

    state
        .hyperliquid_exchange_submissions
        .lock()
        .await
        .push(request.clone());

    let action_type = request
        .get("action")
        .and_then(|a| a.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    match action_type.as_str() {
        "order" | "cancel" | "scheduleCancel" => {
            handle_l1_action(&state, &request, &action_type).await
        }
        "spotSend" => handle_spot_send(&state, &request).await,
        "usdClassTransfer" => handle_usd_class_transfer(&state, &request).await,
        "withdraw3" => handle_withdraw3(&state, &request).await,
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("mock Hyperliquid /exchange does not support action type {other}"),
        ),
    }
}

async fn handle_l1_action(
    state: &Arc<MockIntegratorState>,
    request: &Value,
    action_type: &str,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let action: Actions = match serde_json::from_value(action_value.clone()) {
        Ok(a) => a,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize {action_type} action: {err}"),
            );
        }
    };
    let nonce = match request.get("nonce").and_then(Value::as_u64) {
        Some(n) => n,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "/exchange body is missing `nonce`",
            );
        }
    };
    let vault_address = request
        .get("vaultAddress")
        .and_then(Value::as_str)
        .and_then(|s| Address::from_str(s).ok());
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let connection_id = match action.hash(nonce, vault_address) {
        Ok(id) => id,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to hash {action_type} action: {err}"),
            );
        }
    };
    let signer = match recover_l1_signer(connection_id, state.mainnet_hyperliquid, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("signature recovery failed: {err}"),
            );
        }
    };
    let user = vault_address.unwrap_or(signer);

    let mut hl = state.hyperliquid.lock().await;
    hl.run_due_scheduled_cancels();
    match action {
        Actions::Order(bulk) => {
            let statuses = place_orders_on_mock(&mut hl, user, &bulk);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "order",
                    "data": { "statuses": statuses }
                }
            }))
            .into_response()
        }
        Actions::Cancel(bulk) => {
            let statuses = cancel_orders_on_mock(&mut hl, user, &bulk);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "cancel",
                    "data": { "statuses": statuses }
                }
            }))
            .into_response()
        }
        Actions::ScheduleCancel(schedule) => {
            hl.set_schedule_cancel(user, schedule.time);
            Json(json!({
                "status": "ok",
                "response": {
                    "type": "scheduleCancel"
                }
            }))
            .into_response()
        }
    }
}

async fn handle_spot_send(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    // The wire envelope carries `type: "spotSend"`; SpotSend itself doesn't
    // serialize a type tag, so strip it before deserializing.
    let mut stripped = action_value.clone();
    if let Some(map) = stripped.as_object_mut() {
        map.remove("type");
    }
    let payload: SpotSend = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize spotSend: {err}"),
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("spotSend signature recovery failed: {err}"),
            );
        }
    };
    let nonce = request.get("nonce").and_then(Value::as_u64);

    // The token wire form is "SYMBOL:0x..."; the plain symbol is what we
    // key balances by.
    let token_symbol = payload
        .token
        .split(':')
        .next()
        .unwrap_or(&payload.token)
        .to_string();
    let amount = match payload.amount.parse::<f64>() {
        Ok(v) if v > 0.0 => v,
        _ => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "spotSend amount must be a positive decimal, got {}",
                    payload.amount
                ),
            );
        }
    };

    {
        let mut hl = state.hyperliquid.lock().await;
        let available = hl.available_spot(signer, &token_symbol);
        if available + HYPERLIQUID_AMOUNT_EPSILON < amount {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("spotSend: {signer:?} has {available} {token_symbol}, needs {amount}"),
            );
        }
        hl.debit_spot_total(signer, &token_symbol, amount);
        if let Ok(dst) = Address::from_str(&payload.destination) {
            hl.credit_spot(dst, &token_symbol, amount);
        }
    }

    // Keep the legacy HyperUnit-withdrawal lifecycle hook: a spotSend whose
    // destination matches a tracked unit operation advances it into the first
    // post-discovery state a real withdrawal would expose after Hyperliquid
    // observes the source transfer.
    {
        let mut operations = state.unit_operations.lock().await;
        if let Some(entry) = operations
            .get_mut(&payload.destination)
            .and_then(|entries| latest_active_unit_operation_mut(entries))
        {
            entry.visible = true;
            entry.operation.state = Some("waitForSrcTxFinalization".to_string());
            entry.operation.state_started_at = Some(Utc::now().to_rfc3339());
            entry.operation.state_updated_at = Some(Utc::now().to_rfc3339());
            entry.operation.source_address = Some(format!("{signer:#x}"));
            entry.operation.source_amount = Some(payload.amount.clone());
            if entry.operation.source_tx_hash.is_none() {
                entry.operation.source_tx_hash = Some(match nonce {
                    Some(nonce) => format!("{signer:#x}:{nonce}"),
                    None => format!("{signer:#x}:0"),
                });
            }
            if entry.operation.operation_id.is_none() {
                entry.operation.operation_id = entry.operation.source_tx_hash.clone();
            }
        }
    }

    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

async fn handle_usd_class_transfer(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action") else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let amount_with_target = match action_value.get("amount").and_then(Value::as_str) {
        Some(amount) => amount.to_string(),
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires string field `amount`",
            );
        }
    };
    let to_perp = match action_value.get("toPerp").and_then(Value::as_bool) {
        Some(to_perp) => to_perp,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires bool field `toPerp`",
            );
        }
    };
    let nonce = match action_value.get("nonce").and_then(Value::as_u64) {
        Some(nonce) => nonce,
        None => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "usdClassTransfer requires u64 field `nonce`",
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let payload = UsdClassTransfer {
        signature_chain_id: if state.mainnet_hyperliquid {
            42_161
        } else {
            421_614
        },
        hyperliquid_chain: if state.mainnet_hyperliquid {
            "Mainnet".to_string()
        } else {
            "Testnet".to_string()
        },
        amount: amount_with_target.clone(),
        to_perp,
        nonce,
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("usdClassTransfer signature recovery failed: {err}"),
            );
        }
    };
    let (amount, maybe_subaccount) = match parse_usd_class_transfer_amount(&amount_with_target) {
        Ok(parsed) => parsed,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let user = maybe_subaccount.unwrap_or(signer);

    let mut hl = state.hyperliquid.lock().await;
    let available = if to_perp {
        hl.available_spot(user, "USDC")
    } else {
        hl.clearinghouse_total(user, "USDC")
    };
    if available + HYPERLIQUID_AMOUNT_EPSILON < amount {
        let source = if to_perp { "spot" } else { "clearinghouse" };
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("usdClassTransfer: {user:?} has {available} USDC in {source}, needs {amount}"),
        );
    }

    if to_perp {
        hl.debit_spot_total(user, "USDC", amount);
        hl.credit_clearinghouse(user, "USDC", amount);
    } else {
        hl.debit_clearinghouse_total(user, "USDC", amount);
        hl.credit_spot(user, "USDC", amount);
    }

    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

async fn handle_withdraw3(
    state: &Arc<MockIntegratorState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let mut stripped = action_value;
    if let Some(map) = stripped.as_object_mut() {
        map.remove("type");
    }
    let payload: Withdraw3 = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize withdraw3: {err}"),
            );
        }
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let signer = match recover_typed_signer(&payload, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("withdraw3 signature recovery failed: {err}"),
            );
        }
    };
    let user = request
        .get("vaultAddress")
        .and_then(Value::as_str)
        .and_then(|raw| Address::from_str(raw).ok())
        .unwrap_or(signer);
    let destination = match Address::from_str(&payload.destination) {
        Ok(destination) => destination,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("withdraw3 destination must be a 0x-prefixed address: {err}"),
            );
        }
    };
    let amount_raw = match parse_usdc_decimal_to_raw_u64(&payload.amount) {
        Ok(amount_raw) => amount_raw,
        Err(err) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, err),
    };
    if amount_raw <= HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "withdraw3 amount must exceed the {} raw USDC fee",
                HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW
            ),
        );
    }
    let gross_amount = amount_raw as f64 / 1_000_000.0;
    {
        let mut hl = state.hyperliquid.lock().await;
        let available = hl.clearinghouse_total(user, "USDC");
        if available + HYPERLIQUID_AMOUNT_EPSILON < gross_amount {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!(
                    "withdraw3: {user:?} has {available} USDC withdrawable, needs {gross_amount}"
                ),
            );
        }
        hl.debit_clearinghouse_total(user, "USDC", gross_amount);
    }
    schedule_mock_hyperliquid_withdrawal_release(
        state,
        destination,
        amount_raw - HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW,
    );
    Json(json!({
        "status": "ok",
        "response": { "type": "default" }
    }))
    .into_response()
}

/// Size difference below this threshold is treated as "zero remaining" —
/// guards against f64 drift when partial fills consume a level exactly.
const HYPERLIQUID_AMOUNT_EPSILON: f64 = 1e-12;
const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW: u64 = 1_000_000;

fn parse_user_address(value: &str) -> Option<Address> {
    Address::from_str(value).ok()
}

fn parse_usd_class_transfer_amount(value: &str) -> Result<(f64, Option<Address>), String> {
    let (amount, maybe_subaccount) = match value.split_once(" subaccount:") {
        Some((amount, subaccount)) => (
            amount,
            Some(Address::from_str(subaccount).map_err(|err| {
                format!("invalid usdClassTransfer subaccount {subaccount:?}: {err}")
            })?),
        ),
        None => (value, None),
    };
    let amount = amount
        .parse::<f64>()
        .map_err(|_| format!("usdClassTransfer amount must be a positive decimal, got {value}"))?;
    if amount <= 0.0 {
        return Err(format!(
            "usdClassTransfer amount must be a positive decimal, got {value}"
        ));
    }
    Ok((amount, maybe_subaccount))
}

fn parse_usdc_decimal_to_raw_u64(value: &str) -> Result<u64, String> {
    let (whole, fractional) = match value.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (value, ""),
    };
    if fractional.len() > 6 {
        return Err(format!(
            "USDC amount {value:?} has more than 6 decimal places"
        ));
    }
    let whole = if whole.is_empty() { "0" } else { whole };
    let padded_fractional = format!("{fractional:0<6}");
    let whole: u64 = whole
        .parse()
        .map_err(|err| format!("invalid USDC whole amount {whole:?}: {err}"))?;
    let fractional: u64 = padded_fractional
        .parse()
        .map_err(|err| format!("invalid USDC fractional amount {fractional:?}: {err}"))?;
    whole
        .checked_mul(HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW)
        .and_then(|shifted| shifted.checked_add(fractional))
        .ok_or_else(|| format!("USDC amount {value:?} overflows u64 raw units"))
}

fn schedule_mock_hyperliquid_withdrawal_release(
    state: &Arc<MockIntegratorState>,
    destination: Address,
    payout_raw: u64,
) {
    let Some(rpc_url) = state.hyperliquid_evm_rpc_url.clone() else {
        return;
    };
    let Some(bridge_address) = state.hyperliquid_bridge_address.clone() else {
        return;
    };
    let latency = state.hyperliquid_withdrawal_latency;
    tokio::spawn(async move {
        if latency > Duration::ZERO {
            tokio::time::sleep(latency).await;
        }
        if let Err(err) = send_mock_hyperliquid_withdrawal_release(
            &rpc_url,
            &bridge_address,
            destination,
            payout_raw,
        )
        .await
        {
            tracing::warn!(%err, %destination, payout_raw, "mock hyperliquid withdraw release failed");
        }
    });
}

async fn send_mock_hyperliquid_withdrawal_release(
    rpc_url: &str,
    bridge_address: &str,
    destination: Address,
    payout_raw: u64,
) -> Result<(), String> {
    let rpc_url: Url = rpc_url
        .parse()
        .map_err(|err| format!("invalid hyperliquid EVM RPC URL {rpc_url:?}: {err}"))?;
    let bridge_address = Address::from_str(bridge_address)
        .map_err(|err| format!("invalid hyperliquid bridge address {bridge_address:?}: {err}"))?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let sender = provider
        .get_accounts()
        .await
        .map_err(|err| format!("get_accounts failed: {err}"))?
        .into_iter()
        .next()
        .ok_or_else(|| {
            "hyperliquid withdrawal release requires one unlocked account".to_string()
        })?;
    let calldata = Bytes::from(
        MockHyperliquidBridge2::releaseCall {
            user: destination,
            usd: payout_raw,
        }
        .abi_encode(),
    );
    let tx = TransactionRequest::default()
        .with_from(sender)
        .with_to(bridge_address)
        .with_input(calldata);
    let pending = provider
        .send_transaction(tx)
        .await
        .map_err(|err| format!("bridge release send_transaction failed: {err}"))?;
    let receipt = pending
        .get_receipt()
        .await
        .map_err(|err| format!("bridge release receipt failed: {err}"))?;
    if receipt.status() {
        Ok(())
    } else {
        Err(format!(
            "bridge release transaction reverted: {}",
            receipt.transaction_hash
        ))
    }
}

fn parse_signature(value: Option<&Value>) -> Result<alloy::primitives::Signature, String> {
    let obj = value
        .and_then(Value::as_object)
        .ok_or_else(|| "/exchange body is missing `signature`".to_string())?;
    let r = obj
        .get("r")
        .and_then(Value::as_str)
        .ok_or_else(|| "signature.r missing".to_string())?;
    let s = obj
        .get("s")
        .and_then(Value::as_str)
        .ok_or_else(|| "signature.s missing".to_string())?;
    let v = obj
        .get("v")
        .and_then(Value::as_u64)
        .ok_or_else(|| "signature.v missing".to_string())?;
    let r = U256::from_str_radix(r.trim_start_matches("0x"), 16)
        .map_err(|err| format!("signature.r not hex: {err}"))?;
    let s = U256::from_str_radix(s.trim_start_matches("0x"), 16)
        .map_err(|err| format!("signature.s not hex: {err}"))?;
    let parity = v.saturating_sub(27) != 0;
    Ok(alloy::primitives::Signature::new(r, s, parity))
}

/// Apply each order in the bulk against the hardcoded rate table.
///
/// The mock exposes just enough lifecycle behavior for production-equivalent
/// client integration:
/// - `Ioc` fills immediately when marketable and errors otherwise.
/// - `Gtc` fills immediately when marketable, otherwise rests on the book.
/// - `Alo` rests when non-marketable and errors if it would cross.
///
/// All fills execute at the configured synthetic rate. Resting orders reserve
/// `hold` balance but do not otherwise match until they are cancelled.
fn place_orders_on_mock(
    hl: &mut HyperliquidMockState,
    user: Address,
    bulk: &hyperliquid_client::BulkOrder,
) -> Vec<Value> {
    bulk.orders
        .iter()
        .map(|order| {
            let Some(resolution) = hl.resolve_asset(order.asset) else {
                return json!({ "error": format!("unknown asset {}", order.asset) });
            };
            let limit_px = match order.limit_px.parse::<f64>() {
                Ok(v) if v > 0.0 => v,
                _ => {
                    return json!({
                        "error": format!("invalid limit_px {}", order.limit_px)
                    });
                }
            };
            let sz = match order.sz.parse::<f64>() {
                Ok(v) if v > 0.0 => v,
                _ => return json!({ "error": format!("invalid sz {}", order.sz) }),
            };
            let tif = match &order.order_type {
                hyperliquid_client::Order::Limit(limit) => limit.tif.clone(),
            };

            let base_coin = resolution.base_symbol.clone();
            let quote_coin = resolution.quote_symbol.clone();
            let Some(rate) = hl.rate_for(&base_coin, &quote_coin) else {
                return json!({
                    "error": format!(
                        "no rate configured for {base_coin}/{quote_coin}; call \
                         set_hyperliquid_rate before placing orders"
                    )
                });
            };

            let crosses = if order.is_buy {
                limit_px + HYPERLIQUID_AMOUNT_EPSILON >= rate
            } else {
                limit_px <= rate + HYPERLIQUID_AMOUNT_EPSILON
            };

            if tif == "Alo" && crosses {
                return json!({
                    "error": format!(
                        "add-liquidity-only order would cross rate {rate} for \
                         {base_coin}/{quote_coin}"
                    )
                });
            }

            if !crosses {
                if tif == "Ioc" {
                    return json!({
                        "error": format!(
                            "limit price {limit_px} does not cross rate {rate} for \
                             {base_coin}/{quote_coin} ({})",
                            if order.is_buy { "buy needs limit_px ≥ rate" } else { "sell needs limit_px ≤ rate" }
                        )
                    });
                }

                if tif != "Gtc" && tif != "Alo" {
                    return json!({
                        "error": format!("unsupported time-in-force {tif}")
                    });
                }

                let reserve_coin = if order.is_buy {
                    quote_coin.clone()
                } else {
                    base_coin.clone()
                };
                let reserve_amount = if order.is_buy { sz * limit_px } else { sz };
                let available = hl.available_spot(user, &reserve_coin);
                if available + HYPERLIQUID_AMOUNT_EPSILON < reserve_amount {
                    return json!({
                        "error": format!(
                            "insufficient {reserve_coin} balance: have {available}, need {reserve_amount}",
                        )
                    });
                }

                let oid = hl.allocate_oid();
                let timestamp = Utc::now().timestamp_millis().max(0) as u64;
                let submitted = HyperliquidSubmittedOrder {
                    oid,
                    user,
                    asset: order.asset,
                    coin: resolution.pair_name.clone(),
                    is_buy: order.is_buy,
                    limit_px,
                    sz,
                    orig_sz: sz,
                    tif: tif.clone(),
                    cloid: order.cloid.clone(),
                    timestamp,
                };
                hl.open_orders.insert(oid, submitted);
                return json!({
                    "resting": {
                        "oid": oid,
                    }
                });
            }

            if tif != "Ioc" && tif != "Gtc" {
                return json!({
                    "error": format!("unsupported time-in-force {tif}")
                });
            }

            let oid = hl.allocate_oid();
            let timestamp = Utc::now().timestamp_millis().max(0) as u64;
            let submitted = HyperliquidSubmittedOrder {
                oid,
                user,
                asset: order.asset,
                coin: resolution.pair_name.clone(),
                is_buy: order.is_buy,
                limit_px,
                sz,
                orig_sz: sz,
                tif: tif.clone(),
                cloid: order.cloid.clone(),
                timestamp,
            };
            let top_of_book_depth = hl.book_depth_for(&base_coin, &quote_coin);
            let fill_sz = if order.is_buy {
                sz.min(top_of_book_depth.ask.max(0.0))
            } else {
                sz.min(top_of_book_depth.bid.max(0.0))
            };
            let remaining_sz = (sz - fill_sz).max(0.0);

            if fill_sz <= HYPERLIQUID_AMOUNT_EPSILON {
                if tif == "Ioc" {
                    return json!({
                        "error": format!(
                            "no liquidity available at or better than limit_px {limit_px} for \
                             {base_coin}/{quote_coin}"
                        )
                    });
                }
                hl.open_orders.insert(oid, submitted);
                return json!({
                    "resting": {
                        "oid": oid,
                    }
                });
            }

            let reserve_coin = if order.is_buy {
                quote_coin.clone()
            } else {
                base_coin.clone()
            };
            let reserve_amount = if order.is_buy {
                if tif == "Gtc" {
                    (fill_sz * rate) + (remaining_sz * limit_px)
                } else {
                    fill_sz * rate
                }
            } else {
                sz
            };
            let available = hl.available_spot(user, &reserve_coin);
            if available + HYPERLIQUID_AMOUNT_EPSILON < reserve_amount {
                return json!({
                    "error": format!(
                        "insufficient {reserve_coin} balance: have {available}, need {reserve_amount}",
                    )
                });
            }

            let (debit_coin, debit_amount, credit_coin, credit_amount) = if order.is_buy {
                (
                    quote_coin.clone(),
                    fill_sz * rate,
                    base_coin.clone(),
                    fill_sz,
                )
            } else {
                (
                    base_coin.clone(),
                    fill_sz,
                    quote_coin.clone(),
                    fill_sz * rate,
                )
            };

            hl.debit_spot_total(user, &debit_coin, debit_amount);
            hl.credit_spot(user, &credit_coin, credit_amount);

            let tid = hl.allocate_tid();
            let fill = HyperliquidFillRecord {
                oid,
                tid,
                coin: resolution.pair_name.clone(),
                is_buy: order.is_buy,
                px: rate,
                sz: fill_sz,
                time: timestamp,
                fee: 0.0,
                fee_token: if order.is_buy {
                    base_coin.clone()
                } else {
                    quote_coin.clone()
                },
            };
            hl.fills.entry(user).or_default().insert(0, fill);

            if tif == "Gtc" && remaining_sz > HYPERLIQUID_AMOUNT_EPSILON {
                let mut resting = submitted;
                resting.sz = remaining_sz;
                hl.open_orders.insert(oid, resting);
                return json!({
                    "resting": {
                        "oid": oid,
                    }
                });
            }

            let mut terminal_order = submitted;
            if remaining_sz > HYPERLIQUID_AMOUNT_EPSILON {
                terminal_order.sz = remaining_sz;
            }
            hl.terminal_orders.insert(
                oid,
                TerminalOrder {
                    order: terminal_order,
                    status: "filled".to_string(),
                    status_timestamp: timestamp,
                },
            );

            json!({
                "filled": {
                    "totalSz": format_hl_amount(fill_sz),
                    "avgPx": format_hl_amount(rate),
                    "oid": oid,
                }
            })
        })
        .collect()
}

fn cancel_orders_on_mock(
    hl: &mut HyperliquidMockState,
    user: Address,
    bulk: &hyperliquid_client::BulkCancel,
) -> Vec<Value> {
    bulk.cancels
        .iter()
        .map(|cancel| match hl.open_orders.remove(&cancel.oid) {
            Some(order) if order.user == user && order.asset == cancel.asset => {
                hl.terminal_orders.insert(
                    order.oid,
                    TerminalOrder {
                        order,
                        status: "canceled".to_string(),
                        status_timestamp: Utc::now().timestamp_millis().max(0) as u64,
                    },
                );
                json!("success")
            }
            Some(order) => {
                hl.open_orders.insert(order.oid, order);
                json!({
                    "error": format!(
                        "oid {} is not an open order for this user/asset",
                        cancel.oid
                    )
                })
            }
            None => json!({
                "error": format!(
                    "devnet HL mock has no resting order for oid {}",
                    cancel.oid
                )
            }),
        })
        .collect()
}

fn format_hl_amount(value: f64) -> String {
    // Canonical HL wire form trims trailing zeros from an 8-decimal string
    // ("1.5" not "1.50000000"). `float_to_wire` does exactly this.
    hyperliquid_client::float_to_wire(value)
}

/// Default spot meta served by the mock's `/info { type: "spotMeta" }`. Same
/// token universe the live devnet uses: USDC + UBTC + UETH, with the two
/// canonical pairs at indices 140 / 141.
fn default_hyperliquid_spot_meta() -> SpotMeta {
    serde_json::from_value(json!({
        "tokens": [
            {
                "name": "USDC",
                "szDecimals": 8,
                "weiDecimals": 8,
                "index": 0,
                "tokenId": "0x6d1e7cde53ba9467b783cb7c530ce054",
                "isCanonical": true
            },
            {
                "name": "UBTC",
                "szDecimals": 5,
                "weiDecimals": 8,
                "index": 1,
                "tokenId": "0x11111111111111111111111111111111",
                "isCanonical": true
            },
            {
                "name": "UETH",
                "szDecimals": 4,
                "weiDecimals": 18,
                "index": 2,
                "tokenId": "0x22222222222222222222222222222222",
                "isCanonical": true
            }
        ],
        "universe": [
            {
                "tokens": [1, 0],
                "name": "@140",
                "index": 140,
                "isCanonical": true
            },
            {
                "tokens": [2, 0],
                "name": "@141",
                "index": 141,
                "isCanonical": true
            }
        ]
    }))
    .expect("default spot meta fixture is valid")
}

struct AssetResolution {
    pair_name: String,
    base_symbol: String,
    quote_symbol: String,
}

/// Default rates seeded in `HyperliquidMockState::new`. Picks for devnet so
/// the "typical" UBTC / UETH round-trip works out of the box; tests needing
/// a specific fill price install their own rate via `set_hyperliquid_rate`.
const DEFAULT_UBTC_USDC_RATE: f64 = 60_000.0;
const DEFAULT_UETH_USDC_RATE: f64 = 3_000.0;

/// Default size advertised on each synthesized book level. Tests that care
/// about partial fills can override this per pair.
const SYNTHESIZED_BOOK_DEPTH: f64 = 1_000_000.0;

impl HyperliquidMockState {
    fn new() -> Self {
        let mut state = Self {
            next_oid: 1000,
            next_tid: 1,
            spot_meta: Some(default_hyperliquid_spot_meta()),
            ..Default::default()
        };
        state.set_rate("UBTC", "USDC", DEFAULT_UBTC_USDC_RATE);
        state.set_rate("UETH", "USDC", DEFAULT_UETH_USDC_RATE);
        state
    }

    fn spot_meta_value(&self) -> Value {
        serde_json::to_value(
            self.spot_meta
                .as_ref()
                .expect("spot meta initialized in `new`"),
        )
        .expect("spot meta serializes")
    }

    fn allocate_oid(&mut self) -> u64 {
        let oid = self.next_oid;
        self.next_oid += 1;
        oid
    }

    fn allocate_tid(&mut self) -> u64 {
        let tid = self.next_tid;
        self.next_tid += 1;
        tid
    }

    fn credit_spot(&mut self, user: Address, coin: &str, amount: f64) {
        *self
            .spot_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0) += amount;
    }

    fn credit_clearinghouse(&mut self, user: Address, coin: &str, amount: f64) {
        *self
            .clearinghouse_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0) += amount;
    }

    fn debit_spot_total(&mut self, user: Address, coin: &str, amount: f64) {
        let entry = self
            .spot_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0);
        *entry -= amount;
        if *entry < HYPERLIQUID_AMOUNT_EPSILON {
            *entry = 0.0;
        }
    }

    fn debit_clearinghouse_total(&mut self, user: Address, coin: &str, amount: f64) {
        let entry = self
            .clearinghouse_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0);
        *entry -= amount;
        if *entry < HYPERLIQUID_AMOUNT_EPSILON {
            *entry = 0.0;
        }
    }

    fn spot_total(&self, user: Address, coin: &str) -> f64 {
        self.spot_balances
            .get(&user)
            .and_then(|m| m.get(coin))
            .copied()
            .unwrap_or(0.0)
    }

    fn clearinghouse_total(&self, user: Address, coin: &str) -> f64 {
        self.clearinghouse_balances
            .get(&user)
            .and_then(|m| m.get(coin))
            .copied()
            .unwrap_or(0.0)
    }

    fn available_spot(&self, user: Address, coin: &str) -> f64 {
        (self.spot_total(user, coin) - self.spot_hold_total(user, coin)).max(0.0)
    }

    fn set_book_depth(&mut self, base: &str, quote: &str, depth: f64) {
        let depth = depth.max(0.0);
        self.book_depths.insert(
            (base.to_string(), quote.to_string()),
            HyperliquidBookDepth {
                bid: depth,
                ask: depth,
            },
        );
    }

    fn book_depth_for(&self, base: &str, quote: &str) -> HyperliquidBookDepth {
        self.book_depths
            .get(&(base.to_string(), quote.to_string()))
            .copied()
            .unwrap_or(HyperliquidBookDepth {
                bid: SYNTHESIZED_BOOK_DEPTH,
                ask: SYNTHESIZED_BOOK_DEPTH,
            })
    }

    fn spot_hold_total(&self, user: Address, coin: &str) -> f64 {
        self.spot_holds_for(user)
            .get(coin)
            .copied()
            .unwrap_or_default()
    }

    fn spot_holds_for(&self, user: Address) -> BTreeMap<String, f64> {
        let mut holds = BTreeMap::new();
        for order in self.open_orders.values().filter(|order| order.user == user) {
            let Some(resolution) = self.resolve_asset(order.asset) else {
                continue;
            };
            let (coin, amount) = if order.is_buy {
                (resolution.quote_symbol, order.limit_px * order.sz)
            } else {
                (resolution.base_symbol, order.sz)
            };
            *holds.entry(coin).or_default() += amount;
        }
        holds
    }

    fn set_rate(&mut self, base: &str, quote: &str, rate: f64) {
        self.run_due_scheduled_cancels();
        self.rates
            .insert((base.to_string(), quote.to_string()), rate);
        self.book_depths
            .entry((base.to_string(), quote.to_string()))
            .or_insert(HyperliquidBookDepth {
                bid: SYNTHESIZED_BOOK_DEPTH,
                ask: SYNTHESIZED_BOOK_DEPTH,
            });
        self.fill_crossed_resting_orders(base, quote, rate);
    }

    fn rate_for(&self, base: &str, quote: &str) -> Option<f64> {
        self.rates
            .get(&(base.to_string(), quote.to_string()))
            .copied()
    }

    fn resolve_asset(&self, asset: u32) -> Option<AssetResolution> {
        let meta = self.spot_meta.as_ref()?;
        let pair_index = asset.checked_sub(SPOT_ASSET_INDEX_OFFSET)?;
        let pair = meta
            .universe
            .iter()
            .find(|p| p.index as u32 == pair_index)?;
        let base = meta.tokens.iter().find(|t| t.index == pair.tokens[0])?;
        let quote = meta.tokens.iter().find(|t| t.index == pair.tokens[1])?;
        Some(AssetResolution {
            pair_name: pair.name.clone(),
            base_symbol: base.name.clone(),
            quote_symbol: quote.name.clone(),
        })
    }

    /// Walk the spot-meta universe to find the base/quote token pair for a
    /// wire coin string (`"UBTC/USDC"` or `"@140"`). Used by the synthesized
    /// L2 book so clients that query either alias see the same levels.
    fn pair_tokens(&self, coin: &str) -> Option<(String, String)> {
        let meta = self.spot_meta.as_ref()?;
        let pair = meta.universe.iter().find(|p| p.name == coin).or_else(|| {
            let (base_name, quote_name) = coin.split_once('/')?;
            let base = meta.tokens.iter().find(|t| t.name == base_name)?;
            let quote = meta.tokens.iter().find(|t| t.name == quote_name)?;
            meta.universe
                .iter()
                .find(|p| p.tokens[0] == base.index && p.tokens[1] == quote.index)
        })?;
        let base = meta.tokens.iter().find(|t| t.index == pair.tokens[0])?;
        let quote = meta.tokens.iter().find(|t| t.index == pair.tokens[1])?;
        Some((base.name.clone(), quote.name.clone()))
    }

    /// Synthesize a one-level L2 book from the configured rate: bid == ask ==
    /// rate. Depth is configurable per pair so tests can exercise partial-fill
    /// behavior without a full matching engine.
    fn l2_book_snapshot(&self, coin: &str) -> Value {
        let levels = self
            .pair_tokens(coin)
            .and_then(|(base, quote)| {
                self.rate_for(&base, &quote).map(|rate| {
                    let depth = self.book_depth_for(&base, &quote);
                    let bid = json!({
                        "n": 1,
                        "px": format_hl_amount(rate),
                        "sz": format_hl_amount(depth.bid),
                    });
                    let ask = json!({
                        "n": 1,
                        "px": format_hl_amount(rate),
                        "sz": format_hl_amount(depth.ask),
                    });
                    (bid, ask)
                })
            })
            .map(|(bid, ask)| {
                let level = json!({
                    "n": 1,
                    "px": bid["px"],
                    "sz": bid["sz"],
                });
                let ask_level = json!({
                    "n": 1,
                    "px": ask["px"],
                    "sz": ask["sz"],
                });
                (level, ask_level)
            });
        let (bids, asks) = match levels {
            Some((bid, ask)) => (vec![bid], vec![ask]),
            None => (vec![], vec![]),
        };
        json!({
            "coin": coin,
            "time": Utc::now().timestamp_millis(),
            "levels": [bids, asks]
        })
    }

    fn order_status_snapshot(&self, oid: u64) -> Value {
        if let Some(open) = self.open_orders.get(&oid) {
            json!({
                "status": "order",
                "order": self.order_info_envelope(open, "open", open.timestamp),
            })
        } else if let Some(terminal) = self.terminal_orders.get(&oid) {
            json!({
                "status": "order",
                "order": self.order_info_envelope(
                    &terminal.order,
                    &terminal.status,
                    terminal.status_timestamp,
                ),
            })
        } else {
            json!({ "status": "unknownOid" })
        }
    }

    fn order_info_envelope(
        &self,
        order: &HyperliquidSubmittedOrder,
        status: &str,
        status_timestamp: u64,
    ) -> Value {
        json!({
            "order": {
                "coin": order.coin,
                "side": if order.is_buy { "B" } else { "A" },
                "limitPx": format_hl_amount(order.limit_px),
                "sz": format_hl_amount(order.sz),
                "oid": order.oid,
                "timestamp": order.timestamp,
                "origSz": format_hl_amount(order.orig_sz),
                "cloid": order.cloid,
                "tif": order.tif,
            },
            "status": status,
            "statusTimestamp": status_timestamp,
        })
    }

    fn spot_clearinghouse_snapshot(&self, user: Address) -> Value {
        let default = BTreeMap::new();
        let totals = self.spot_balances.get(&user).unwrap_or(&default);
        let Some(meta) = self.spot_meta.as_ref() else {
            return json!({ "balances": [] });
        };
        let holds = self.spot_holds_for(user);
        let balances: Vec<Value> = totals
            .iter()
            .filter(|(coin, total)| {
                **total > HYPERLIQUID_AMOUNT_EPSILON
                    || holds.get(*coin).copied().unwrap_or_default() > HYPERLIQUID_AMOUNT_EPSILON
            })
            .map(|(coin, total)| {
                let token_index = meta
                    .tokens
                    .iter()
                    .find(|t| &t.name == coin)
                    .map_or(0u64, |t| t.index as u64);
                let hold = holds.get(coin).copied().unwrap_or_default();
                json!({
                    "coin": coin,
                    "token": token_index,
                    "hold": format_hl_amount(hold),
                    "total": format_hl_amount(*total),
                })
            })
            .collect();
        json!({ "balances": balances })
    }

    fn clearinghouse_snapshot(&self, user: Address) -> Value {
        let withdrawable = self.clearinghouse_total(user, "USDC");
        let withdrawable = format_hl_amount(withdrawable);
        json!({
            "marginSummary": {
                "accountValue": withdrawable,
                "totalNtlPos": "0",
                "totalRawUsd": withdrawable,
                "totalMarginUsed": "0",
            },
            "crossMarginSummary": {
                "accountValue": withdrawable,
                "totalNtlPos": "0",
                "totalRawUsd": withdrawable,
                "totalMarginUsed": "0",
            },
            "crossMaintenanceMarginUsed": "0",
            "withdrawable": withdrawable,
            "assetPositions": [],
            "time": Utc::now().timestamp_millis(),
        })
    }

    fn open_orders_for(&self, user: Address) -> Value {
        Value::Array(
            self.open_orders
                .values()
                .filter(|order| order.user == user)
                .map(|order| {
                    json!({
                        "coin": order.coin,
                        "side": if order.is_buy { "B" } else { "A" },
                        "limitPx": format_hl_amount(order.limit_px),
                        "sz": format_hl_amount(order.sz),
                        "oid": order.oid,
                        "timestamp": order.timestamp,
                        "origSz": format_hl_amount(order.orig_sz),
                        "cloid": order.cloid,
                    })
                })
                .collect(),
        )
    }

    fn fills_for(&self, user: Address) -> Value {
        let rows = self.fills.get(&user).cloned().unwrap_or_default();
        Value::Array(
            rows.into_iter()
                .map(|fill| {
                    json!({
                        "coin": fill.coin,
                        "px": format_hl_amount(fill.px),
                        "sz": format_hl_amount(fill.sz),
                        "side": if fill.is_buy { "B" } else { "A" },
                        "time": fill.time,
                        "startPosition": "0",
                        "dir": if fill.is_buy { "Buy" } else { "Sell" },
                        "closedPnl": "0",
                        "hash": format!("0x{}", "ab".repeat(32)),
                        "oid": fill.oid,
                        "crossed": true,
                        "fee": format_hl_amount(fill.fee),
                        "tid": fill.tid,
                        "feeToken": fill.fee_token,
                    })
                })
                .collect(),
        )
    }

    fn fill_crossed_resting_orders(&mut self, base: &str, quote: &str, rate: f64) {
        let depth = self.book_depth_for(base, quote);
        let mut remaining_bid_depth = depth.bid.max(0.0);
        let mut remaining_ask_depth = depth.ask.max(0.0);
        let matching_oids: Vec<u64> = self
            .open_orders
            .iter()
            .filter_map(|(oid, order)| {
                let resolution = self.resolve_asset(order.asset)?;
                if resolution.base_symbol != base || resolution.quote_symbol != quote {
                    return None;
                }
                let crosses = if order.is_buy {
                    order.limit_px + HYPERLIQUID_AMOUNT_EPSILON >= rate
                } else {
                    order.limit_px <= rate + HYPERLIQUID_AMOUNT_EPSILON
                };
                crosses.then_some(*oid)
            })
            .collect();

        for oid in matching_oids {
            let Some(mut order) = self.open_orders.remove(&oid) else {
                continue;
            };
            let Some(resolution) = self.resolve_asset(order.asset) else {
                continue;
            };
            let side_depth = if order.is_buy {
                &mut remaining_ask_depth
            } else {
                &mut remaining_bid_depth
            };
            let fill_sz = order.sz.min((*side_depth).max(0.0));
            if fill_sz <= HYPERLIQUID_AMOUNT_EPSILON {
                self.open_orders.insert(order.oid, order);
                continue;
            }

            let (debit_coin, debit_amount, credit_coin, credit_amount) = if order.is_buy {
                (
                    resolution.quote_symbol.clone(),
                    fill_sz * rate,
                    resolution.base_symbol.clone(),
                    fill_sz,
                )
            } else {
                (
                    resolution.base_symbol.clone(),
                    fill_sz,
                    resolution.quote_symbol.clone(),
                    fill_sz * rate,
                )
            };
            if self.spot_total(order.user, &debit_coin) + HYPERLIQUID_AMOUNT_EPSILON < debit_amount
            {
                self.terminal_orders.insert(
                    order.oid,
                    TerminalOrder {
                        order,
                        status: "rejected".to_string(),
                        status_timestamp: Utc::now().timestamp_millis().max(0) as u64,
                    },
                );
                continue;
            }

            self.debit_spot_total(order.user, &debit_coin, debit_amount);
            self.credit_spot(order.user, &credit_coin, credit_amount);

            let timestamp = Utc::now().timestamp_millis().max(0) as u64;
            let tid = self.allocate_tid();
            *side_depth = (*side_depth - fill_sz).max(0.0);
            self.fills.entry(order.user).or_default().insert(
                0,
                HyperliquidFillRecord {
                    oid: order.oid,
                    tid,
                    coin: resolution.pair_name.clone(),
                    is_buy: order.is_buy,
                    px: rate,
                    sz: fill_sz,
                    time: timestamp,
                    fee: 0.0,
                    fee_token: if order.is_buy {
                        resolution.base_symbol
                    } else {
                        resolution.quote_symbol
                    },
                },
            );
            order.sz = (order.sz - fill_sz).max(0.0);
            if order.sz <= HYPERLIQUID_AMOUNT_EPSILON {
                self.terminal_orders.insert(
                    order.oid,
                    TerminalOrder {
                        order,
                        status: "filled".to_string(),
                        status_timestamp: timestamp,
                    },
                );
            } else {
                self.open_orders.insert(order.oid, order);
            }
        }
    }

    fn set_schedule_cancel(&mut self, user: Address, time: Option<u64>) {
        match time {
            Some(time) => {
                self.scheduled_cancels.insert(user, time);
            }
            None => {
                self.scheduled_cancels.remove(&user);
            }
        }
    }

    fn run_due_scheduled_cancels(&mut self) {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        let due_users: Vec<Address> = self
            .scheduled_cancels
            .iter()
            .filter_map(|(user, time)| (*time <= now).then_some(*user))
            .collect();
        for user in due_users {
            self.cancel_all_open_orders_for_user(user, "scheduledCancel");
            self.scheduled_cancels.remove(&user);
        }
    }

    fn cancel_all_open_orders_for_user(&mut self, user: Address, status: &str) {
        let matching_oids: Vec<u64> = self
            .open_orders
            .iter()
            .filter_map(|(oid, order)| (order.user == user).then_some(*oid))
            .collect();
        let timestamp = Utc::now().timestamp_millis().max(0) as u64;
        for oid in matching_oids {
            let Some(order) = self.open_orders.remove(&oid) else {
                continue;
            };
            self.terminal_orders.insert(
                order.oid,
                TerminalOrder {
                    order,
                    status: status.to_string(),
                    status_timestamp: timestamp,
                },
            );
        }
    }
}

/// Advance a tracked Unit operation to terminal `done`. Deposit operations
/// also credit the mock HL ledger and populate a synthetic source tx hash,
/// so downstream balance/history assertions still work once the real router
/// API no longer routes funds through `POST /unit/*` endpoints.
async fn complete_mock_unit_operation(
    state: &Arc<MockIntegratorState>,
    protocol_address: &str,
) -> Result<(), String> {
    let maybe_deposit_credit = {
        let mut operations = state.unit_operations.lock().await;
        let entries = operations
            .get_mut(protocol_address)
            .ok_or_else(|| format!("mock unit operation {protocol_address} was not found"))?;
        let Some(entry) = latest_active_unit_operation_mut(entries) else {
            return match entries
                .last()
                .and_then(|entry| entry.operation.state.as_deref())
            {
                Some("done") => Ok(()),
                Some("failure") => {
                    Err(format!("mock unit operation {protocol_address} has failed"))
                }
                _ => Err(format!(
                    "mock unit operation {protocol_address} has no active entry"
                )),
            };
        };
        if matches!(entry.operation.state.as_deref(), Some("failure")) {
            return Err(format!("mock unit operation {protocol_address} has failed"));
        }
        if matches!(entry.operation.state.as_deref(), Some("done")) {
            return Ok(());
        }
        entry.visible = true;
        entry.operation.state = Some("done".to_string());
        entry
            .operation
            .state_started_at
            .get_or_insert_with(|| Utc::now().to_rfc3339());
        entry.operation.state_updated_at = Some(Utc::now().to_rfc3339());
        entry
            .operation
            .broadcast_at
            .get_or_insert_with(|| Utc::now().to_rfc3339());
        entry
            .operation
            .state_next_attempt_at
            .get_or_insert_with(|| Utc::now().to_rfc3339());
        if entry.operation.source_address.is_none()
            && matches!(entry.kind, MockUnitOperationKind::Deposit)
        {
            entry.operation.source_address = Some(entry.dst_addr.clone());
        }
        if entry.operation.source_tx_hash.is_none() {
            entry.operation.source_tx_hash = Some(default_mock_unit_source_tx_hash(entry));
        }
        if entry.operation.destination_tx_hash.is_none() {
            entry.operation.destination_tx_hash =
                Some(default_mock_unit_destination_tx_hash(entry));
        }
        if entry.operation.operation_id.is_none() {
            entry.operation.operation_id = entry.operation.source_tx_hash.clone();
        }
        if entry.operation.destination_fee_amount.is_none()
            || entry.operation.sweep_fee_amount.is_none()
        {
            let (destination_fee_amount, sweep_fee_amount) = default_mock_unit_fee_amounts(entry);
            entry.operation.destination_fee_amount = Some(destination_fee_amount);
            entry.operation.sweep_fee_amount = Some(sweep_fee_amount);
        }
        entry.operation.source_tx_confirmations = None;
        entry.operation.destination_tx_confirmations = Some(10);
        match entry.kind {
            MockUnitOperationKind::Deposit if matches!(entry.dst_chain, UnitChain::Hyperliquid) => {
                let Some(user) = Address::from_str(&entry.dst_addr).ok() else {
                    return Ok(());
                };
                let coin = hyperliquid_coin_for_unit_asset(entry.asset);
                let net_amount = net_hyperunit_credit_amount(
                    entry.asset,
                    entry.operation.source_amount.as_deref(),
                    entry.operation.destination_fee_amount.as_deref(),
                    entry.operation.sweep_fee_amount.as_deref(),
                );
                Some((user, coin, net_amount))
            }
            _ => None,
        }
    };

    if let Some((user, coin, amount)) = maybe_deposit_credit {
        if amount > HYPERLIQUID_AMOUNT_EPSILON {
            let mut hl = state.hyperliquid.lock().await;
            hl.credit_spot(user, coin, amount);
        }
    }

    Ok(())
}

fn default_mock_unit_source_tx_hash(entry: &MockUnitOperationEntry) -> String {
    let entropy = mock_unit_operation_entropy(entry);
    match entry.src_chain {
        UnitChain::Bitcoin => format!("{}:0", hex::encode(&entropy[..32])),
        UnitChain::Ethereum | UnitChain::Base => format!("0x{}:0", hex::encode(&entropy[..32])),
        UnitChain::Hyperliquid => {
            let source_address = entry
                .operation
                .source_address
                .as_deref()
                .unwrap_or("0x0000000000000000000000000000000000000000");
            let nonce = u64::from_be_bytes(entropy[..8].try_into().unwrap_or([0; 8]));
            format!("{source_address}:{}", nonce.max(1))
        }
        _ => format!("0x{}:0", hex::encode(&entropy[..32])),
    }
}

fn default_mock_unit_destination_tx_hash(entry: &MockUnitOperationEntry) -> String {
    let entropy = mock_unit_operation_entropy(entry);
    match entry.dst_chain {
        UnitChain::Hyperliquid => format!("{}:{}", entry.dst_addr, Utc::now().timestamp_millis()),
        UnitChain::Bitcoin => format!("{}:0", hex::encode(&entropy[..32])),
        UnitChain::Ethereum | UnitChain::Base => format!("0x{}", hex::encode(&entropy[..32])),
        _ => format!("0x{}", hex::encode(&entropy[..32])),
    }
}

fn mock_unit_operation_entropy(entry: &MockUnitOperationEntry) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(entry.src_chain.as_wire_str().as_bytes());
    hasher.update(b"|");
    hasher.update(entry.dst_chain.as_wire_str().as_bytes());
    hasher.update(b"|");
    hasher.update(entry.asset.as_wire_str().as_bytes());
    hasher.update(b"|");
    hasher.update(entry.dst_addr.as_bytes());
    hasher.update(b"|");
    hasher.update(
        entry
            .operation
            .op_created_at
            .as_deref()
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.finalize().into()
}

fn default_mock_unit_fee_amounts(entry: &MockUnitOperationEntry) -> (String, String) {
    let source_amount = entry
        .operation
        .source_amount
        .as_deref()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or_default();
    if source_amount <= 0.0 {
        return ("0".to_string(), "0".to_string());
    }

    match (entry.kind, entry.asset) {
        (MockUnitOperationKind::Deposit, UnitAsset::Eth) => (
            trim_float_string(source_amount * 0.049_758_464_122_074_1),
            trim_float_string(source_amount * 0.014_217_524_924_4),
        ),
        (MockUnitOperationKind::Deposit, UnitAsset::Btc) => (
            trim_float_string(source_amount * 0.005),
            trim_float_string(source_amount * 0.001),
        ),
        (MockUnitOperationKind::Deposit, _) => (
            trim_float_string(source_amount * 0.005),
            trim_float_string(source_amount * 0.001),
        ),
        (MockUnitOperationKind::Withdrawal, _) => ("0".to_string(), "0".to_string()),
    }
}

fn net_hyperunit_credit_amount(
    asset: UnitAsset,
    source_amount: Option<&str>,
    destination_fee_amount: Option<&str>,
    sweep_fee_amount: Option<&str>,
) -> f64 {
    let decimals = match asset {
        UnitAsset::Btc => 8,
        UnitAsset::Eth => 18,
        _ => 18,
    };
    let source_amount = parse_scaled_decimal(source_amount, decimals);
    let destination_fee_amount = parse_scaled_decimal(destination_fee_amount, decimals);
    let sweep_fee_amount = parse_scaled_decimal(sweep_fee_amount, decimals);
    (source_amount - destination_fee_amount - sweep_fee_amount).max(0.0)
}

fn parse_scaled_decimal(value: Option<&str>, decimals: u32) -> f64 {
    let Some(value) = value else {
        return 0.0;
    };
    let Ok(raw) = value.parse::<f64>() else {
        return 0.0;
    };
    raw / 10f64.powi(decimals as i32)
}

fn hyperliquid_coin_for_unit_asset(asset: UnitAsset) -> &'static str {
    match asset {
        UnitAsset::Btc => "UBTC",
        UnitAsset::Eth => "UETH",
        _ => "UETH",
    }
}

fn trim_float_string(value: f64) -> String {
    let mut rendered = format!("{value:.18}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    if rendered.is_empty() {
        "0".to_string()
    } else {
        rendered
    }
}

/// Move a tracked Unit operation into terminal `failure`. Leaves the operation
/// in place so tests can still observe its final state via
/// `/operations/{protocol_address}`.
async fn fail_mock_unit_operation(
    state: &Arc<MockIntegratorState>,
    protocol_address: &str,
    error: String,
) -> Result<(), String> {
    let mut operations = state.unit_operations.lock().await;
    let entries = operations
        .get_mut(protocol_address)
        .ok_or_else(|| format!("mock unit operation {protocol_address} was not found"))?;
    let Some(entry) = latest_active_unit_operation_mut(entries) else {
        return Err(format!(
            "mock unit operation {protocol_address} has no active entry: {error}"
        ));
    };
    if matches!(entry.operation.state.as_deref(), Some("done")) {
        return Err(format!(
            "mock unit operation {protocol_address} is already completed: {error}"
        ));
    }
    entry.visible = true;
    entry.operation.state = Some("failure".to_string());
    entry.operation.state_updated_at = Some(Utc::now().to_rfc3339());
    Ok(())
}

fn snapshot_unit_operations(
    operations: &BTreeMap<String, Vec<MockUnitOperationEntry>>,
) -> Vec<MockUnitOperationRecord> {
    operations
        .values()
        .flat_map(|entries| entries.iter())
        .map(|entry| MockUnitOperationRecord {
            protocol_address: entry.operation.protocol_address.clone().unwrap_or_default(),
            src_chain: entry.src_chain.as_wire_str().to_string(),
            dst_chain: entry.dst_chain.as_wire_str().to_string(),
            asset: entry.asset.as_wire_str().to_string(),
            dst_addr: entry.dst_addr.clone(),
            kind: entry.kind,
            state: entry.operation.state.clone().unwrap_or_default(),
            source_amount: entry.operation.source_amount.clone(),
            source_tx_hash: entry.operation.source_tx_hash.clone(),
            destination_tx_hash: entry.operation.destination_tx_hash.clone(),
        })
        .collect()
}

fn latest_active_unit_operation_mut(
    entries: &mut [MockUnitOperationEntry],
) -> Option<&mut MockUnitOperationEntry> {
    entries
        .iter_mut()
        .rev()
        .find(|entry| !entry.operation.classified_state().is_terminal())
}

/// Deterministic "protocol address" the mock returns from `/gen`. The real
/// HyperUnit service returns a fresh random address per request; for tests the
/// same `(src, dst, asset, dst_addr)` tuple must resolve to the same mock
/// address so assertions can match up input and observed state.
fn derive_mock_protocol_address(
    src_chain: &str,
    dst_chain: &str,
    asset: &str,
    dst_addr: &str,
) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(src_chain.as_bytes());
    hasher.update(b"|");
    hasher.update(dst_chain.as_bytes());
    hasher.update(b"|");
    hasher.update(asset.as_bytes());
    hasher.update(b"|");
    hasher.update(dst_addr.as_bytes());
    let digest: [u8; 32] = hasher.finalize().into();
    if src_chain == UnitChain::Bitcoin.as_wire_str()
        && dst_chain == UnitChain::Hyperliquid.as_wire_str()
    {
        return derive_mock_regtest_bitcoin_address(digest);
    }
    format!("0x{}", hex::encode(&digest[..20]))
}

fn derive_mock_regtest_bitcoin_address(seed: [u8; 32]) -> String {
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let mut secret = seed;
    for tweak in 0..=u8::MAX {
        secret[31] = seed[31].wrapping_add(tweak);
        if let Ok(secret_key) = bitcoin::secp256k1::SecretKey::from_slice(&secret) {
            let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
            let public_key = bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key)
                .expect("secret key should derive a compressed public key");
            return bitcoin::Address::p2wpkh(&public_key, bitcoin::Network::Regtest).to_string();
        }
    }
    "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080".to_string()
}

fn base_url(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

fn parse_amount(field: &str, value: &str) -> Result<u128, String> {
    let amount = value
        .parse::<u128>()
        .map_err(|error| format!("{field} must be an integer base-unit amount: {error}"))?;
    if amount == 0 {
        return Err(format!("{field} must be nonzero"));
    }
    Ok(amount)
}

fn stringify_balances(balances: &BTreeMap<String, u128>) -> BTreeMap<String, String> {
    balances
        .iter()
        .map(|(key, value)| (key.clone(), value.to_string()))
        .collect()
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct MockAcrossErrorResponse {
    #[serde(rename = "type")]
    error_type: String,
    code: String,
    status: u16,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    param: Option<String>,
}

fn validate_across_request(
    state: &MockIntegratorState,
    headers: &HeaderMap,
    integrator_id: Option<&str>,
) -> Result<(), Box<axum::response::Response>> {
    if let Some(expected_api_key) = state.across_api_key.as_deref() {
        let expected = format!("Bearer {expected_api_key}");
        let actual = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok());
        if actual != Some(expected.as_str()) {
            return Err(Box::new(mock_across_error_response(
                StatusCode::UNAUTHORIZED,
                "auth_error",
                "missing_or_invalid_auth",
                "Across API key is required",
                None,
            )));
        }
    }

    if let Some(expected_integrator_id) = state.across_integrator_id.as_deref() {
        if integrator_id != Some(expected_integrator_id) {
            return Err(Box::new(mock_across_error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation_error",
                "invalid_integrator_id",
                "integratorId is required",
                Some("integratorId"),
            )));
        }
    }

    Ok(())
}

fn mock_across_error_response(
    status: StatusCode,
    error_type: impl Into<String>,
    code: impl Into<String>,
    message: impl Into<String>,
    param: Option<&'static str>,
) -> axum::response::Response {
    (
        status,
        Json(MockAcrossErrorResponse {
            error_type: error_type.into(),
            code: code.into(),
            status: status.as_u16(),
            message: message.into(),
            param: param.map(str::to_string),
        }),
    )
        .into_response()
}

fn error_response(status: StatusCode, error: impl Into<String>) -> axum::response::Response {
    (
        status,
        Json(MockIntegratorErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::sol_types::SolCall;
    use blockchain_utils::create_websocket_wallet_provider;
    use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;

    /// The mock `GET /swap/approval` returns a `swapTx` whose calldata must
    /// decode back to a real `deposit(...)` call with the exact args derived
    /// from the query — proving the mock is semantically equivalent to real
    /// Across and that production code submitting the returned calldata
    /// on-chain will invoke `MockSpokePool.deposit(...)` with the same args.
    #[tokio::test]
    async fn mock_swap_approval_returns_decodable_deposit_calldata_for_erc20() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let input_token = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
        let output_token = "0x2222222222222222222222222222222222222222";
        let depositor = "0x3333333333333333333333333333333333333333";
        let recipient = "0x4444444444444444444444444444444444444444";
        let amount = 1_000_000u128;
        let origin_chain_id = 1u64;
        let destination_chain_id = 42161u64;

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_spoke_pool_address(spoke_pool),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId={}&destinationChainId={}&inputToken={}&outputToken={}&amount={}&depositor={}&recipient={}",
            server.base_url(),
            origin_chain_id,
            destination_chain_id,
            input_token,
            output_token,
            amount,
            depositor,
            recipient
        );
        let resp = reqwest::get(&url).await.expect("http get");
        let status = resp.status();
        let text = resp.text().await.expect("body");
        assert!(status.is_success(), "status={status} body={text}");
        let body: Value = serde_json::from_str(&text).expect("json body");

        let swap_tx_to = body["swapTx"]["to"].as_str().expect("swapTx.to");
        assert_eq!(swap_tx_to.to_lowercase(), spoke_pool.to_lowercase());
        assert!(body.get("swapTx").is_some());
        assert!(body["swapTx"].get("value").is_none() || body["swapTx"]["value"].is_null());

        let data_hex = body["swapTx"]["data"].as_str().expect("swapTx.data");
        let calldata = hex::decode(data_hex.trim_start_matches("0x")).expect("hex decode");
        let decoded = depositCall::abi_decode(&calldata).expect("decode deposit call");
        assert_eq!(
            decoded.depositor,
            address_to_bytes32(Address::from_str(depositor).unwrap())
        );
        assert_eq!(
            decoded.recipient,
            address_to_bytes32(Address::from_str(recipient).unwrap())
        );
        assert_eq!(
            decoded.inputToken,
            address_to_bytes32(Address::from_str(input_token).unwrap())
        );
        assert_eq!(
            decoded.outputToken,
            address_to_bytes32(Address::from_str(output_token).unwrap())
        );
        assert_eq!(decoded.inputAmount, U256::from(amount));
        assert_eq!(decoded.outputAmount, U256::from(amount));
        assert_eq!(decoded.destinationChainId, U256::from(destination_chain_id));

        let approval_txns = body["approvalTxns"].as_array().expect("approvalTxns");
        assert_eq!(approval_txns.len(), 1);
        let approval_to = approval_txns[0]["to"].as_str().unwrap();
        assert_eq!(approval_to.to_lowercase(), input_token.to_lowercase());
        assert!(approval_txns[0]["gas"].is_null());
        let approval_data = approval_txns[0]["data"].as_str().unwrap();
        let approval_bytes =
            hex::decode(approval_data.trim_start_matches("0x")).expect("approval hex decode");
        let approve = IERC20::approveCall::abi_decode(&approval_bytes).expect("decode approve");
        assert_eq!(approve.spender, Address::from_str(spoke_pool).unwrap());
        assert_eq!(approve.amount, U256::MAX);
        assert_eq!(body["amountType"].as_str(), Some("exactInput"));
        assert_eq!(
            body["crossSwapType"].as_str(),
            Some("bridgeableToBridgeable")
        );
        assert_eq!(body["expectedFillTime"].as_u64(), Some(2));
        assert_eq!(
            body["inputToken"]["address"].as_str(),
            Some(input_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["outputToken"]["address"].as_str(),
            Some(output_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["refundToken"]["address"].as_str(),
            Some(input_token.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["checks"]["allowance"]["expected"].as_str(),
            Some("1000000")
        );
        assert_eq!(
            body["checks"]["allowance"]["spender"].as_str(),
            Some(spoke_pool.to_lowercase()).as_deref()
        );
        assert_eq!(
            body["checks"]["balance"]["expected"].as_str(),
            Some("1000000")
        );
        assert!(body["steps"].get("approve").is_none());
        assert_eq!(body["steps"]["bridge"]["provider"].as_str(), Some("across"));
        assert_eq!(
            body["steps"]["bridge"]["inputAmount"].as_str(),
            Some("1000000")
        );
        assert_eq!(body["swapTx"]["gas"].as_str(), Some("0"));
        assert!(body["swapTx"].get("maxFeePerGas").is_none());
        assert!(body["swapTx"].get("maxPriorityFeePerGas").is_none());
        assert_eq!(body["swapTx"]["ecosystem"].as_str(), Some("evm"));
        assert_eq!(body["swapTx"]["simulationSuccess"].as_bool(), Some(true));
        assert_eq!(body["fees"]["total"]["amount"].as_str(), Some("0"));
    }

    #[tokio::test]
    async fn mock_swap_approval_skips_approval_for_native_input() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let native_zero = "0x0000000000000000000000000000000000000000";
        let amount = 42u128;
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_spoke_pool_address(spoke_pool),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken={native_zero}&outputToken={native_zero}&amount={amount}&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.base_url()
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200 ok")
            .json()
            .await
            .expect("json body");

        assert_eq!(
            body["swapTx"]["value"].as_str(),
            Some(amount.to_string()).as_deref()
        );
        assert!(
            body.get("approvalTxns").is_none() || body["approvalTxns"].is_null(),
            "native input must not include approvalTxns, got: {body}"
        );
    }

    #[tokio::test]
    async fn mock_swap_approval_can_require_bearer_auth_and_integrator_id() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(spoke_pool)
                .with_across_auth("test-key", "rift-test"),
        )
        .await
        .expect("spawn mock integrator");
        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken=0x0000000000000000000000000000000000000000&outputToken=0x0000000000000000000000000000000000000000&amount=42&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.base_url()
        );
        let client = reqwest::Client::new();

        let unauth = client.get(&url).send().await.expect("unauth request");
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
        let unauth_body: Value = unauth.json().await.expect("unauth json");
        assert_eq!(unauth_body["code"], "missing_or_invalid_auth");

        let missing_integrator = client
            .get(&url)
            .bearer_auth("test-key")
            .send()
            .await
            .expect("missing integrator request");
        assert_eq!(
            missing_integrator.status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        let missing_integrator_body: Value =
            missing_integrator.json().await.expect("integrator json");
        assert_eq!(missing_integrator_body["param"], "integratorId");

        let ok: Value = client
            .get(format!("{url}&integratorId=rift-test"))
            .bearer_auth("test-key")
            .send()
            .await
            .expect("authenticated request")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json");
        assert_eq!(ok["inputAmount"].as_str(), Some("42"));
    }

    #[tokio::test]
    async fn mock_swap_approval_supports_min_output_with_deterministic_fee() {
        let spoke_pool = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
        let native_zero = "0x0000000000000000000000000000000000000000";
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(spoke_pool)
                .with_across_quote_fee_bps(100),
        )
        .await
        .expect("spawn mock integrator");
        let url = format!(
            "{}/swap/approval?tradeType=minOutput&originChainId=1&destinationChainId=42161&inputToken={native_zero}&outputToken={native_zero}&amount=990&depositor=0x3333333333333333333333333333333333333333&recipient=0x4444444444444444444444444444444444444444",
            server.base_url()
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json body");

        assert_eq!(body["inputAmount"].as_str(), Some("1000"));
        assert_eq!(body["maxInputAmount"].as_str(), Some("1000"));
        assert_eq!(body["expectedOutputAmount"].as_str(), Some("990"));
        assert_eq!(body["minOutputAmount"].as_str(), Some("990"));
        assert_eq!(body["swapTx"]["value"].as_str(), Some("1000"));

        let data_hex = body["swapTx"]["data"].as_str().expect("swapTx.data");
        let calldata = hex::decode(data_hex.trim_start_matches("0x")).expect("hex decode");
        let decoded = depositCall::abi_decode(&calldata).expect("decode deposit call");
        assert_eq!(decoded.inputAmount, U256::from(1000_u64));
        assert_eq!(decoded.outputAmount, U256::from(990_u64));
    }

    #[tokio::test]
    async fn mock_swap_approval_skips_approval_when_allowance_already_exists() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().block_time(1).try_spawn().expect("anvil spawn");
        let ws_url = anvil.ws_endpoint_url().to_string();
        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&ws_url, private_key)
            .await
            .expect("ws wallet provider");
        let provider: DynProvider = provider.erased();

        let token = GenericEIP3009ERC20Instance::deploy(provider.clone())
            .await
            .expect("deploy token");
        let token_address = *token.address();
        let spoke_pool =
            Address::from_str("0xACE055C0C055D0C035E47055D05E7055055BACE0").expect("spoke pool");
        let depositor = anvil.addresses()[0];
        let recipient = anvil.addresses()[1];
        let amount = U256::from(1_000_000_u64);

        token
            .mint(depositor, amount)
            .send()
            .await
            .expect("mint send")
            .get_receipt()
            .await
            .expect("mint receipt");

        token
            .approve(spoke_pool, amount)
            .send()
            .await
            .expect("approve send")
            .get_receipt()
            .await
            .expect("approve receipt");

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{spoke_pool:#x}"))
                .with_across_evm_rpc_url(ws_url),
        )
        .await
        .expect("spawn mock integrator");

        let url = format!(
            "{}/swap/approval?tradeType=exactInput&originChainId=1&destinationChainId=42161&inputToken={:#x}&outputToken={:#x}&amount={}&depositor={:#x}&recipient={:#x}",
            server.base_url(),
            token_address,
            Address::ZERO,
            amount,
            depositor,
            recipient,
        );
        let body: Value = reqwest::get(&url)
            .await
            .expect("http get")
            .error_for_status()
            .expect("200")
            .json()
            .await
            .expect("json body");

        assert!(
            body.get("approvalTxns").is_none() || body["approvalTxns"].is_null(),
            "existing allowance should skip approvalTxns, got: {body}"
        );
        assert_eq!(
            body["checks"]["allowance"]["actual"].as_str(),
            Some(amount.to_string()).as_deref()
        );
        assert!(body["steps"].get("approve").is_none());
    }

    /// Indexer-backed `GET /deposit/status` must report `"filled"` once the
    /// MockSpokePool emits `FundsDeposited`, surfacing the decoded tx hash and
    /// deposit fields in the same shape the real Across `/deposit/status`
    /// endpoint returns. Production code polls this endpoint identically for
    /// mock and real Across — this test proves the mock completes the loop.
    #[tokio::test]
    async fn deposit_status_indexer_reports_filled_after_deposit() {
        use crate::across_spoke_pool_mock::MockSpokePool;
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().block_time(1).try_spawn().expect("anvil spawn");
        let ws_url = anvil.ws_endpoint_url().to_string();
        let chain_id = anvil.chain_id();
        let depositor = anvil.addresses()[0];
        let recipient_addr = anvil.addresses()[1];

        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let provider = create_websocket_wallet_provider(&ws_url, private_key)
            .await
            .expect("ws wallet provider");
        let provider: DynProvider = provider.erased();

        let spoke_pool = MockSpokePool::deploy(provider).await.expect("deploy");
        let spoke_pool_address = *spoke_pool.address();

        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_across_spoke_pool_address(format!("{spoke_pool_address:#x}"))
                .with_across_evm_rpc_url(ws_url),
        )
        .await
        .expect("spawn mock integrator");

        let amount = U256::from(1_000_u64);
        let destination_chain_id = 42161_u64;
        let fill_deadline: u32 = u32::MAX;
        let receipt = spoke_pool
            .deposit(
                address_to_bytes32(depositor),
                address_to_bytes32(recipient_addr),
                FixedBytes::<32>::ZERO,
                FixedBytes::<32>::ZERO,
                amount,
                amount,
                U256::from(destination_chain_id),
                FixedBytes::<32>::ZERO,
                0,
                fill_deadline,
                0,
                alloy::primitives::Bytes::new(),
            )
            .value(amount)
            .send()
            .await
            .expect("send deposit")
            .get_receipt()
            .await
            .expect("receipt");
        let expected_deposit_tx = format!("{:#x}", receipt.transaction_hash).to_lowercase();

        let status_url = format!(
            "{}/deposit/status?originChainId={}&depositId=0",
            server.base_url(),
            chain_id
        );
        let mut filled_body: Option<Value> = None;
        for _ in 0..50 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let body: Value = reqwest::get(&status_url)
                .await
                .expect("http")
                .json()
                .await
                .expect("json body");
            if body["status"] == "filled" {
                filled_body = Some(body);
                break;
            }
        }
        let body = filled_body.expect("deposit status never transitioned to filled");
        assert_eq!(body["status"], "filled");
        assert_eq!(body["originChainId"].as_u64(), Some(chain_id));
        assert_eq!(body["depositId"].as_str(), Some("0"));
        assert_eq!(
            body["destinationChainId"].as_u64(),
            Some(destination_chain_id)
        );
        assert_eq!(
            body["depositTxnRef"].as_str().unwrap().to_lowercase(),
            expected_deposit_tx
        );
        assert!(body["fillTxnRef"].as_str().unwrap().starts_with("0x"));
        assert!(body.get("fillDeadline").is_none() || body["fillDeadline"].is_null());
        assert_eq!(body["pagination"]["currentIndex"].as_u64(), Some(0));
        assert_eq!(body["pagination"]["maxIndex"].as_u64(), Some(0));

        let status_url_by_tx = format!(
            "{}/deposit/status?depositTxnRef={}",
            server.base_url(),
            expected_deposit_tx
        );
        let body_by_tx: Value = reqwest::get(&status_url_by_tx)
            .await
            .expect("http")
            .json()
            .await
            .expect("json body");
        assert_eq!(body_by_tx["status"], "filled");
        assert_eq!(body_by_tx["depositId"].as_str(), Some("0"));
        assert_eq!(
            body_by_tx["depositTxnRef"]
                .as_str()
                .expect("depositTxnRef")
                .to_lowercase(),
            expected_deposit_tx
        );
        assert_eq!(body_by_tx["pagination"]["currentIndex"].as_u64(), Some(0));
        assert_eq!(body_by_tx["pagination"]["maxIndex"].as_u64(), Some(0));
    }

    #[tokio::test]
    async fn deposit_status_by_tx_ref_returns_not_found_before_indexing() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let response = reqwest::get(format!(
            "{}/deposit/status?depositTxnRef=0x{}",
            server.base_url(),
            "ab".repeat(32)
        ))
        .await
        .expect("http");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: Value = response.json().await.expect("json body");
        assert_eq!(body["error"].as_str(), Some("DepositNotFoundException"));
        assert_eq!(
            body["message"].as_str(),
            Some("Deposit not found given the provided constraints")
        );
    }

    #[tokio::test]
    async fn deposit_status_can_delay_and_refund_deterministically() {
        let delayed = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_status_latency(Duration::from_secs(3600)),
        )
        .await
        .expect("spawn delayed mock integrator");
        insert_mock_across_record(
            &delayed,
            7,
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .await;
        let delayed_url = format!(
            "{}/deposit/status?originChainId=1&depositId=7",
            delayed.base_url()
        );
        let delayed_body: Value = reqwest::get(&delayed_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(delayed_body["status"], "pending");
        assert_eq!(
            delayed_body["depositTxnRef"].as_str(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        );

        let refunded = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_across_refund_probability_bps(10_000),
        )
        .await
        .expect("spawn refunded mock integrator");
        insert_mock_across_record(
            &refunded,
            8,
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        )
        .await;
        let refunded_url = format!(
            "{}/deposit/status?depositTxnRef=0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            refunded.base_url()
        );
        let refunded_body: Value = reqwest::get(&refunded_url)
            .await
            .expect("http")
            .json()
            .await
            .expect("json");
        assert_eq!(refunded_body["status"], "refunded");
        assert!(refunded_body["depositRefundTxnRef"]
            .as_str()
            .expect("refund tx")
            .starts_with("0x"));
        assert!(refunded_body.get("fillTxnRef").is_none());
    }

    #[tokio::test]
    async fn unit_mock_reuses_protocol_address_and_preserves_history_shape() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let destination = "0x33f65788aca48d733c2c2444ac9f79b18206aa92";
        let gen_url = format!(
            "{}/gen/ethereum/hyperliquid/eth/{}",
            server.base_url(),
            destination
        );

        let first: UnitGenerateAddressResponse = reqwest::get(&gen_url)
            .await
            .expect("first gen")
            .error_for_status()
            .expect("first gen 200")
            .json()
            .await
            .expect("first gen json");
        server
            .complete_unit_operation(&first.address)
            .await
            .expect("complete first op");

        let second: UnitGenerateAddressResponse = reqwest::get(&gen_url)
            .await
            .expect("second gen")
            .error_for_status()
            .expect("second gen 200")
            .json()
            .await
            .expect("second gen json");
        assert_eq!(second.address.to_lowercase(), first.address.to_lowercase());

        let by_protocol_before: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.base_url(),
            first.address
        ))
        .await
        .expect("protocol ops before")
        .error_for_status()
        .expect("protocol ops before 200")
        .json()
        .await
        .expect("protocol ops before json");
        assert!(by_protocol_before.addresses.is_empty());
        assert_eq!(by_protocol_before.operations.len(), 1);

        server
            .complete_unit_operation(&first.address)
            .await
            .expect("complete second op");

        let by_protocol_after: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.base_url(),
            first.address
        ))
        .await
        .expect("protocol ops after")
        .error_for_status()
        .expect("protocol ops after 200")
        .json()
        .await
        .expect("protocol ops after json");
        assert!(by_protocol_after.addresses.is_empty());
        assert_eq!(by_protocol_after.operations.len(), 2);
        assert!(by_protocol_after
            .operations
            .iter()
            .all(|op| op.matches_protocol_address(&first.address)));
        assert_ne!(
            by_protocol_after.operations[0].source_tx_hash,
            by_protocol_after.operations[1].source_tx_hash
        );
        assert_ne!(
            by_protocol_after.operations[0].operation_id,
            by_protocol_after.operations[1].operation_id
        );

        let by_destination: UnitOperationsResponse =
            reqwest::get(format!("{}/operations/{}", server.base_url(), destination))
                .await
                .expect("destination ops")
                .error_for_status()
                .expect("destination ops 200")
                .json()
                .await
                .expect("destination ops json");
        assert_eq!(by_destination.addresses.len(), 1);
        assert_eq!(by_destination.operations.len(), 2);
        assert_eq!(
            by_destination.addresses[0]["address"].as_str(),
            Some(first.address.as_str())
        );
    }

    #[tokio::test]
    async fn unit_mock_withdrawal_gen_uses_partial_signature_set() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/hyperliquid/ethereum/eth/0x33f65788aca48d733c2c2444ac9f79b18206aa92",
            server.base_url()
        ))
        .await
        .expect("withdraw gen")
        .error_for_status()
        .expect("withdraw gen 200")
        .json()
        .await
        .expect("withdraw gen json");

        assert!(body.signatures.get("field-node").is_some());
        assert!(body.signatures.get("unit-node").is_some());
        assert!(body.signatures.get("hl-node").is_none());
    }

    async fn insert_mock_across_record(
        server: &MockIntegratorServer,
        deposit_id: u64,
        deposit_tx_hash: &str,
    ) {
        let record = MockAcrossDepositRecord {
            origin_chain_id: 1,
            destination_chain_id: U256::from(42161_u64),
            deposit_id: U256::from(deposit_id),
            depositor: FixedBytes::<32>::ZERO,
            recipient: FixedBytes::<32>::ZERO,
            input_token: FixedBytes::<32>::ZERO,
            output_token: FixedBytes::<32>::ZERO,
            input_amount: U256::from(1000_u64),
            output_amount: U256::from(990_u64),
            fill_deadline: u32::MAX,
            deposit_tx_hash: deposit_tx_hash.to_string(),
            block_number: 1,
            indexed_at: Utc::now(),
        };
        server
            .state
            .across_deposits
            .lock()
            .await
            .insert((1, deposit_id.to_string()), record);
    }

    /// The mock `POST /info` + `POST /exchange` endpoints must return JSON
    /// that deserializes back into the real `hyperliquid_client` response
    /// types. This guards the "speak the real API shape byte-for-byte"
    /// invariant: production code paths expecting HL's wire format will also
    /// work against the mock.
    #[tokio::test]
    async fn hyperliquid_info_spot_meta_matches_real_shape() {
        use hyperliquid_client::meta::SpotMeta;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: SpotMeta = reqwest::Client::new()
            .post(format!("{}/info", server.base_url()))
            .json(&json!({ "type": "spotMeta" }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize SpotMeta");

        let map = body.coin_to_asset_map();
        assert_eq!(map.get("UBTC/USDC"), Some(&10_140));
        assert_eq!(map.get("@140"), Some(&10_140));
    }

    #[tokio::test]
    async fn hyperliquid_info_l2_book_matches_real_shape() {
        use hyperliquid_client::info::L2BookSnapshot;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: L2BookSnapshot = reqwest::Client::new()
            .post(format!("{}/info", server.base_url()))
            .json(&json!({ "type": "l2Book", "coin": "UBTC/USDC" }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize L2BookSnapshot");

        assert_eq!(body.coin, "UBTC/USDC");
        assert!(body.best_bid().is_some());
        assert!(body.best_ask().is_some());
    }

    #[tokio::test]
    async fn hyperliquid_info_order_status_matches_real_shape() {
        use hyperliquid_client::info::OrderStatusResponse;

        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: OrderStatusResponse = reqwest::Client::new()
            .post(format!("{}/info", server.base_url()))
            .json(&json!({ "type": "orderStatus", "user": "0x0", "oid": 42 }))
            .send()
            .await
            .expect("http")
            .json()
            .await
            .expect("deserialize OrderStatusResponse");

        assert_eq!(body.status, "unknownOid");
        assert!(body.order.is_none());
    }

    #[tokio::test]
    async fn hyperliquid_exchange_order_persists_submission() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        // Raw JSON submission — signature is invalid but the exchange handler
        // still persists the raw envelope before attempting recovery, which is
        // what this test actually asserts.
        let submission = json!({
            "action": {
                "type": "order",
                "orders": [{
                    "a": 10_140,
                    "b": true,
                    "p": "68000",
                    "s": "0.001",
                    "r": false,
                    "t": { "limit": { "tif": "Ioc" } }
                }],
                "grouping": "na"
            },
            "nonce": 1_700_000_000_000_u64,
            "signature": { "r": "0x0", "s": "0x0", "v": 27 }
        });
        let _ = reqwest::Client::new()
            .post(format!("{}/exchange", server.base_url()))
            .json(&submission)
            .send()
            .await
            .expect("http");

        let submissions = server.hyperliquid_exchange_submissions().await;
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0]["action"]["type"], "order");
    }

    // Driving a real `HyperliquidClient` against the mock server exercises
    // signing, msgpack hashing, serialization, and mock dispatch end-to-end.
    // If either side's wire format drifts, these round-trip tests break before
    // production does.
    mod hyperliquid_client_round_trip {
        use super::super::*;
        use alloy::signers::local::PrivateKeySigner;
        use hyperliquid_client::{
            client::Network, CancelRequest, HyperliquidClient, Limit, Order, OrderRequest,
        };

        const TEST_WALLET: &str =
            "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e";

        fn new_client(base_url: &str) -> HyperliquidClient {
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            HyperliquidClient::new(base_url, wallet, None, Network::Testnet).expect("client")
        }

        #[tokio::test]
        async fn client_refresh_spot_meta_populates_asset_index() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(server.base_url());

            let meta = client.refresh_spot_meta().await.expect("spot meta");

            assert!(meta.tokens.iter().any(|t| t.name == "UBTC"));
            assert_eq!(client.asset_index("UBTC/USDC").expect("asset"), 10_140);
            assert_eq!(client.asset_index("@140").expect("asset alias"), 10_140);
        }

        #[tokio::test]
        async fn client_l2_book_returns_bid_and_ask_levels() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            server.set_hyperliquid_rate("UBTC", "USDC", 68_000.0).await;
            let client = new_client(server.base_url());

            let book = client.l2_book("UBTC/USDC").await.expect("l2 book");

            assert_eq!(book.coin, "UBTC/USDC");
            assert_eq!(book.best_bid().expect("bid").px, "68000");
            assert_eq!(book.best_ask().expect("ask").px, "68000");
            assert_eq!(book.bids().len(), 1);
            assert_eq!(book.asks().len(), 1);
        }

        #[tokio::test]
        async fn client_order_status_returns_filled_envelope_for_oid() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(server.base_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server
                .credit_hyperliquid_balance(user, "USDC", 10_000.0)
                .await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let place = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "60000".to_string(),
                        sz: "0.1".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Ioc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect("place orders");
            let oid = place["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("oid");

            let status = client.order_status(user, oid).await.expect("order status");

            assert_eq!(status.status, "order");
            let envelope = status.order.expect("order envelope");
            assert_eq!(envelope.order.oid, oid);
            assert_eq!(envelope.status, "filled");
        }

        #[tokio::test]
        async fn client_place_orders_signs_and_mock_persists_submission() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(server.base_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server.credit_hyperliquid_balance(user, "USDC", 100.0).await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let order = OrderRequest {
                asset: client.asset_index("UBTC/USDC").expect("asset"),
                is_buy: true,
                limit_px: "60000".to_string(),
                sz: "0.001".to_string(),
                reduce_only: false,
                order_type: Order::Limit(Limit {
                    tif: "Ioc".to_string(),
                }),
                cloid: None,
            };
            let resp = client
                .place_orders(vec![order], "na")
                .await
                .expect("place orders");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "order");
            let oid = resp["response"]["data"]["statuses"][0]["filled"]["oid"]
                .as_u64()
                .expect("oid");
            assert_eq!(oid, 1000);
            assert_eq!(
                resp["response"]["data"]["statuses"][0]["filled"]["avgPx"],
                "60000"
            );

            // Balances moved: 60 USDC debited, 0.001 UBTC credited.
            let ubtc = server.hyperliquid_balance_of(user, "UBTC").await;
            let usdc = server.hyperliquid_balance_of(user, "USDC").await;
            assert!((ubtc - 0.001).abs() < 1e-9, "UBTC balance {ubtc}");
            assert!((usdc - 40.0).abs() < 1e-9, "USDC balance {usdc}");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "order");
            assert_eq!(submitted["action"]["grouping"], "na");
            assert_eq!(submitted["action"]["orders"][0]["a"], 10_140);
            assert_eq!(submitted["action"]["orders"][0]["b"], true);
            assert!(submitted["signature"]["r"].is_string());
            assert!(submitted["signature"]["s"].is_string());
            assert!(submitted["nonce"].is_u64());
        }

        #[tokio::test]
        async fn client_cancel_orders_succeeds_for_resting_order() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let mut client = new_client(server.base_url());
            client.refresh_spot_meta().await.expect("spot meta");
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();
            server.credit_hyperliquid_balance(user, "USDC", 100.0).await;
            server.set_hyperliquid_rate("UBTC", "USDC", 60_000.0).await;

            let place = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "30000".to_string(),
                        sz: "0.001".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Gtc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect("place resting order");
            let oid = place["response"]["data"]["statuses"][0]["resting"]["oid"]
                .as_u64()
                .expect("resting oid");

            let resp = client
                .cancel_orders(vec![CancelRequest { asset: 10_140, oid }])
                .await
                .expect("cancel orders");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "cancel");
            assert_eq!(resp["response"]["data"]["statuses"][0], json!("success"));

            let open_orders = client.open_orders(user).await.expect("open orders");
            assert!(open_orders.iter().all(|order| order.oid != oid));

            let status = client.order_status(user, oid).await.expect("order status");
            assert_eq!(status.status, "order");
            assert_eq!(status.order.expect("order envelope").status, "canceled");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 2);
            assert_eq!(submissions[1]["action"]["type"], "cancel");
            assert_eq!(submissions[1]["action"]["cancels"][0]["o"], oid);
        }

        #[tokio::test]
        async fn client_withdraw_to_bridge_signs_and_posts_withdraw3() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(server.base_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            server
                .credit_hyperliquid_clearinghouse_balance(wallet.address(), "USDC", 20.0)
                .await;
            let destination = "0x000000000000000000000000000000000000dead".to_string();

            let resp = client
                .withdraw_to_bridge(destination.clone(), "12.34".to_string(), 1_700_000_000_000)
                .await
                .expect("withdraw");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "default");

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "withdraw3");
            assert_eq!(submitted["action"]["destination"], destination);
            assert_eq!(submitted["action"]["amount"], "12.34");
            assert_eq!(submitted["action"]["hyperliquidChain"], "Testnet");
            assert_eq!(submitted["action"]["signatureChainId"], "0x66eee");
            assert_eq!(submitted["nonce"], 1_700_000_000_000_u64);
        }

        #[tokio::test]
        async fn client_clearinghouse_state_round_trips_withdrawable_balance() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(server.base_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();

            server
                .credit_hyperliquid_clearinghouse_balance(user, "USDC", 7.25)
                .await;

            let state = client
                .clearinghouse_state(user)
                .await
                .expect("clearinghouse state");

            assert_eq!(state.withdrawable, "7.25");
            assert_eq!(state.margin_summary.account_value, "7.25");
            assert_eq!(state.cross_margin_summary.account_value, "7.25");
        }

        #[tokio::test]
        async fn client_usd_class_transfer_signs_and_posts_action() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            let client = new_client(server.base_url());
            let wallet = TEST_WALLET.parse::<PrivateKeySigner>().expect("wallet");
            let user = wallet.address();

            server
                .credit_hyperliquid_clearinghouse_balance(user, "USDC", 12.34)
                .await;

            let resp = client
                .usd_class_transfer("12.34".to_string(), false, 1_700_000_000_000)
                .await
                .expect("usd class transfer");

            assert_eq!(resp["status"], "ok");
            assert_eq!(resp["response"]["type"], "default");
            assert!((server.hyperliquid_balance_of(user, "USDC").await - 12.34).abs() < 1e-9);
            assert!(
                server
                    .hyperliquid_clearinghouse_balance_of(user, "USDC")
                    .await
                    .abs()
                    < 1e-9
            );

            let submissions = server.hyperliquid_exchange_submissions().await;
            assert_eq!(submissions.len(), 1);
            let submitted = &submissions[0];
            assert_eq!(submitted["action"]["type"], "usdClassTransfer");
            assert_eq!(submitted["action"]["amount"], "12.34");
            assert_eq!(submitted["action"]["toPerp"], false);
            assert_eq!(submitted["action"]["nonce"], 1_700_000_000_000_u64);
            assert_eq!(submitted["action"]["hyperliquidChain"], "Testnet");
            assert_eq!(submitted["action"]["signatureChainId"], "0x66eee");
            assert!(submitted.get("vaultAddress").is_none());
        }

        #[tokio::test]
        async fn client_place_orders_surfaces_mock_exchange_error() {
            let server = MockIntegratorServer::spawn().await.expect("spawn");
            server
                .fail_next_hyperliquid_exchange("simulated HL outage")
                .await;
            let mut client = new_client(server.base_url());
            client.refresh_spot_meta().await.expect("spot meta");

            let err = client
                .place_orders(
                    vec![OrderRequest {
                        asset: client.asset_index("UBTC/USDC").expect("asset"),
                        is_buy: true,
                        limit_px: "68000".to_string(),
                        sz: "0.001".to_string(),
                        reduce_only: false,
                        order_type: Order::Limit(Limit {
                            tif: "Ioc".to_string(),
                        }),
                        cloid: None,
                    }],
                    "na",
                )
                .await
                .expect_err("enqueued error should surface");
            let rendered = format!("{err:?}");
            assert!(
                rendered.contains("simulated HL outage") || rendered.contains("502"),
                "expected mock-injected error to propagate, got {rendered}"
            );
        }
    }
}
