//! `hyperliquid_devnet` — the Hyperliquid devnet **node**: the standalone,
//! `RiftDevnet`-owned HTTP service that speaks Hyperliquid's `POST /info` and
//! `POST /exchange` wire shapes, peer to [`bitcoin_devnet`](crate::bitcoin_devnet)
//! and [`evm_devnet`](crate::evm_devnet). This is the **sole** Hyperliquid in
//! the devnet — there is no longer a venue mock under `/hyperliquid`.
//!
//! The node is **independent**. It owns its *own*
//! [`HyperliquidCore`](crate::hyperliquid_core::HyperliquidCore) instance and a
//! dedicated axum server bound to its own [`TcpListener`]. The node knows
//! nothing about Unit: its `spotSend` / `sendAsset` perform only the spot
//! transfer (debit signer, credit destination) with no Unit-withdrawal
//! trigger (the Unit mock's node-withdrawal poller observes those transfers),
//! and its `withdraw3` debits the clearinghouse and submits the on-chain
//! Bridge2 release when the node is wired to a chain. It also owns its OWN
//! Bridge2 deposit indexer, crediting its clearinghouse from on-chain deposits.
//!
//! The HTTP handlers here are *thin glue*: every matching / ledger operation
//! goes through [`HyperliquidCore`](crate::hyperliquid_core::HyperliquidCore)'s
//! deliberate `pub(crate)` interface. The matching engine itself lives in
//! `hyperliquid_core.rs` and is **not** re-implemented here.

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use alloy::{
    hex,
    network::EthereumWallet,
    primitives::{Address, Bytes, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    signers::local::PrivateKeySigner,
    sol_types::{SolCall, SolEvent},
};
use axum::{extract::State, http::StatusCode, response::IntoResponse, routing::post, Json, Router};
use chrono::Utc;
use hyperliquid_client::{
    recover_l1_signer, recover_typed_signer, Actions, SendAsset, SpotSend, UsdClassTransfer,
    UserFill, UserFunding, UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate, Withdraw3,
    MINIMUM_BRIDGE_DEPOSIT_USDC,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{net::TcpListener, task::JoinHandle};

pub(crate) mod contract;

use crate::hyperliquid_core::{
    format_hl_amount, hyperliquid_has_sufficient_amount, HyperliquidCore,
};
use crate::hyperliquid_devnet::contract::MockHyperliquidBridge2;
use crate::mock_integrators::{mock_evm_indexer_initial_last_scanned, IMockMintableERC20, IERC20};

/// The Unit "guardian" HL account seeded at node genesis. This account is
/// credited with effectively unbounded spot UBTC / UETH so that — in a later
/// phase — the Unit mock can `spotSend` deposit credits to user accounts
/// indefinitely without ever draining. The address and key are fixed/known so
/// callers can reproduce its signature off-chain.
///
/// Private key: 32 bytes of `0x11` (a well-known devnet test key, never used in
/// production). The corresponding address is computed once at startup.
pub const HYPERLIQUID_GUARDIAN_PRIVATE_KEY: [u8; 32] = [0x11u8; 32];

/// Per-coin genesis balance handed to the guardian account. `u32::MAX` natural
/// units of each spot token is "effectively unbounded" for devnet purposes
/// while staying comfortably within `f64` exact-integer range.
const GUARDIAN_GENESIS_BALANCE: f64 = u32::MAX as f64;

/// Bridge2 withdrawal fee in raw (6-decimal) USDC units, mirroring the real
/// Hyperliquid bridge. The node debits the gross amount from the clearinghouse;
/// the net release (gross − fee) is a phase-2 concern.
const HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW: u64 = 1_000_000;

/// Configuration for a [`HyperliquidNode`].
///
/// The bridge-wiring fields ([`arbitrum_rpc_url`](Self::arbitrum_rpc_url),
/// [`bridge_address`](Self::bridge_address),
/// [`usdc_token_address`](Self::usdc_token_address),
/// [`release_signer_key`](Self::release_signer_key)) are all `Option` so a
/// Phase-1 caller (or a unit test) without a chain still gets a fully working
/// node — it just runs *without* its own Bridge2 deposit indexer and `withdraw3`
/// stays a clearinghouse-only debit. When **all four** are present, the node
/// spawns its OWN Bridge2 deposit indexer and completes `withdraw3` with the
/// on-chain Bridge2 `release`. This is independent of (and dual to) the venue
/// mock's bridge wiring: both observe the same Bridge2 on the Arbitrum Anvil
/// chain, each crediting its own [`HyperliquidCore`].
#[derive(Clone, Default)]
pub struct HyperliquidNodeConfig {
    /// TCP port to bind. `None` binds an ephemeral OS-assigned port (mirroring
    /// the venue mock); `Some(p)` binds that fixed port (mirroring
    /// [`EthDevnet`](crate::evm_devnet::EthDevnet)'s `port` handling).
    pub port: Option<u16>,
    /// Treat signatures as mainnet (`true`) or testnet (`false`). Devnet runs
    /// testnet.
    pub mainnet: bool,
    /// JSON-RPC URL of the Arbitrum Anvil chain hosting the Bridge2 contract.
    pub arbitrum_rpc_url: Option<String>,
    /// The deterministic Bridge2 (`MockHyperliquidBridge2`) address on Arbitrum.
    pub bridge_address: Option<Address>,
    /// The Arbitrum USDC token address Bridge2 deposits / releases move.
    pub usdc_token_address: Option<Address>,
    /// Raw secp256k1 scalar of the on-chain release signer. Mirrors the mock's
    /// signer mechanism: a *known funded key* (the Arbitrum Anvil account-0,
    /// which is pre-funded with ETH and is the mock-USDC master minter) used to
    /// mint the payout to the bridge and submit the Bridge2 `release` tx.
    pub release_signer_key: Option<[u8; 32]>,
}

impl std::fmt::Debug for HyperliquidNodeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HyperliquidNodeConfig")
            .field("port", &self.port)
            .field("mainnet", &self.mainnet)
            .field("arbitrum_rpc_url", &self.arbitrum_rpc_url)
            .field("bridge_address", &self.bridge_address)
            .field("usdc_token_address", &self.usdc_token_address)
            .field(
                "release_signer_key",
                &self.release_signer_key.map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// The bridge-release wiring the node's `withdraw3` handler needs to submit the
/// on-chain Bridge2 `release` after debiting the clearinghouse. Cloned into the
/// HTTP state so handlers can reach it; `None` when the node was spawned without
/// a chain (Phase-1 callers / unit tests).
#[derive(Clone)]
struct NodeBridgeReleaseWiring {
    arbitrum_rpc_url: String,
    bridge_address: Address,
    usdc_token_address: Address,
    /// Shared release provider (wallet + nonce-managed), built ONCE at node
    /// spawn so a single `CachedNonceManager` hands out sequential nonces across
    /// concurrent `withdraw3` releases. A fresh provider per release gives each
    /// its own nonce cache, so concurrent releases read the same chain nonce and
    /// collide ("nonce too low"), failing the release and stranding the order's
    /// bridge-withdrawal step in `waiting_external`.
    release_provider: DynProvider,
}

/// The Hyperliquid devnet node: its own [`HyperliquidCore`] ledger plus a
/// dedicated axum server serving `GET/POST /info` and `POST /exchange`.
pub struct HyperliquidNode {
    /// This node's own ledger instance. Not shared with the venue mock's core.
    /// Held to keep the ledger alive for the node's lifetime and reached via
    /// [`HyperliquidNode::core`] for in-crate tests / seeding; the serving task
    /// holds its own clone of this same `Arc`.
    #[allow(dead_code)]
    core: Arc<HyperliquidCore>,
    /// The guardian HL account seeded with effectively unbounded spot balance.
    guardian_address: Address,
    /// `http://<host>:<port>` base URL the node is reachable at.
    url: String,
    /// Graceful-shutdown trigger for the axum server (sent on `Drop`).
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// The serving task handle, kept alive for the node's lifetime.
    #[allow(dead_code)]
    handle: JoinHandle<()>,
    /// Graceful-shutdown trigger for the node's OWN Bridge2 deposit indexer
    /// (sent on `Drop`). `None` when spawned without a chain.
    bridge_indexer_shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    /// The node's OWN Bridge2 deposit-indexer task handle, kept alive for the
    /// node's lifetime. `None` when spawned without a chain.
    #[allow(dead_code)]
    bridge_indexer_handle: Option<JoinHandle<()>>,
}

/// Shared HTTP state for the node's handlers: the node's own ledger, the
/// mainnet-signing flag, and (when wired) the Bridge2 release wiring its
/// `withdraw3` handler uses to perform the on-chain payout. The node has no Unit
/// wiring; the bridge wiring is the node's OWN, dual to the venue mock's.
struct HyperliquidNodeState {
    core: Arc<HyperliquidCore>,
    mainnet: bool,
    /// `None` when spawned without a chain — `withdraw3` then stays a
    /// clearinghouse-only debit (Phase-1 behavior).
    bridge_release: Option<NodeBridgeReleaseWiring>,
}

impl HyperliquidNode {
    /// Construct and spawn a node with all defaults (ephemeral port, testnet).
    pub async fn spawn() -> Result<Self, std::io::Error> {
        Self::spawn_with_config(HyperliquidNodeConfig::default()).await
    }

    /// Construct and spawn a node, binding per `config.port` and seeding the
    /// guardian genesis balance before the server accepts requests.
    pub async fn spawn_with_config(config: HyperliquidNodeConfig) -> Result<Self, std::io::Error> {
        // A fixed port means the interactive devnet-cli/compose stack, where the
        // router reaches the node cross-container — bind 0.0.0.0. An ephemeral
        // port means an in-process test — bind loopback.
        let bind_ip = if config.port.is_some() {
            Ipv4Addr::UNSPECIFIED
        } else {
            Ipv4Addr::LOCALHOST
        };
        let bind_addr = SocketAddr::new(IpAddr::V4(bind_ip), config.port.unwrap_or(0));
        let listener = TcpListener::bind(bind_addr).await?;
        let addr = listener.local_addr()?;

        let core = Arc::new(HyperliquidCore::new());

        // GENESIS: seed the guardian with effectively unbounded spot UBTC /
        // UETH so it can spotSend deposit credits indefinitely in a later phase.
        let guardian_address = guardian_address();
        {
            let mut hl = core.lock().await;
            hl.credit_spot(guardian_address, "UBTC", GUARDIAN_GENESIS_BALANCE);
            hl.credit_spot(guardian_address, "UETH", GUARDIAN_GENESIS_BALANCE);
        }

        // The node's OWN Bridge2 release wiring. Present only when the caller
        // supplied all four pieces (RPC, bridge, USDC, signer key). This is
        // independent of the venue mock's wiring — both watch the same Bridge2.
        let bridge_release = match (
            config.arbitrum_rpc_url.clone(),
            config.bridge_address,
            config.usdc_token_address,
            config.release_signer_key,
        ) {
            (
                Some(arbitrum_rpc_url),
                Some(bridge_address),
                Some(usdc_token_address),
                Some(release_signer_key),
            ) => {
                let signer =
                    PrivateKeySigner::from_bytes(&release_signer_key.into()).map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("invalid hyperliquid node release signer key: {err}"),
                        )
                    })?;
                let release_provider: DynProvider = ProviderBuilder::new()
                    .wallet(EthereumWallet::new(signer))
                    .connect(&arbitrum_rpc_url)
                    .await
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("hyperliquid node release RPC init failed: {err}"),
                        )
                    })?
                    .erased();
                Some(NodeBridgeReleaseWiring {
                    arbitrum_rpc_url,
                    bridge_address,
                    usdc_token_address,
                    release_provider,
                })
            }
            _ => None,
        };

        // Spawn the node's OWN Bridge2 deposit indexer when the bridge is wired.
        // It watches the same Bridge2 the venue mock watches, but credits THIS
        // node's core. Managed (shutdown + handle) like the axum server below.
        let (bridge_indexer_shutdown, bridge_indexer_handle) = match &bridge_release {
            Some(wiring) => {
                let provider: DynProvider = ProviderBuilder::new()
                    .connect(&wiring.arbitrum_rpc_url)
                    .await
                    .map_err(|err| {
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("hyperliquid node bridge indexer RPC init failed: {err}"),
                        )
                    })?
                    .erased();
                let (tx, rx) = tokio::sync::oneshot::channel();
                let handle = tokio::spawn(run_node_bridge_deposit_indexer(
                    provider,
                    wiring.usdc_token_address,
                    wiring.bridge_address,
                    core.clone(),
                    rx,
                ));
                (Some(tx), Some(handle))
            }
            None => (None, None),
        };

        let state = Arc::new(HyperliquidNodeState {
            core: core.clone(),
            mainnet: config.mainnet,
            bridge_release,
        });
        let app = router().with_state(state);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                tracing::warn!(%error, "hyperliquid node server exited with error");
            }
        });

        // In-process consumers (the Unit mock, same container) connect via url();
        // when bound to 0.0.0.0 for cross-container reach, advertise loopback so
        // the in-process connect target is a real address, not the wildcard.
        let advertise = if addr.ip().is_unspecified() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        } else {
            addr
        };

        Ok(Self {
            core,
            guardian_address,
            url: format!("http://{advertise}"),
            shutdown: Some(shutdown_tx),
            handle,
            bridge_indexer_shutdown,
            bridge_indexer_handle,
        })
    }

    /// The `http://<host>:<port>` base the node serves `/info` + `/exchange` at.
    #[must_use]
    pub fn url(&self) -> String {
        self.url.clone()
    }

    /// The guardian HL account seeded with unbounded spot balance at genesis.
    #[must_use]
    pub fn guardian_address(&self) -> Address {
        self.guardian_address
    }

    /// The guardian HL account's raw secp256k1 key. The Unit mock signs the
    /// deposit `spotSend(guardian -> user)` with this when pointed at the node.
    #[must_use]
    pub fn guardian_key(&self) -> [u8; 32] {
        HYPERLIQUID_GUARDIAN_PRIVATE_KEY
    }

    /// Access the node's own [`HyperliquidCore`] ledger (tests / seeding).
    #[must_use]
    #[allow(dead_code)]
    pub(crate) fn core(&self) -> &Arc<HyperliquidCore> {
        &self.core
    }

    // --- Public test-helper accessors over the node's own HyperliquidCore. ---
    //
    // These are the seeding / inspection surface external-crate test suites use
    // to drive the node directly (rates, book depth, balances, historical
    // fills/funding/ledger updates) without going through a full settlement
    // flow. All amounts/balances are NATURAL units (e.g. `1.0` UBTC, `100.0`
    // USDC), matching `credit_spot` / `spot_total` / `credit_clearinghouse` /
    // `clearinghouse_total` on `HyperliquidCore`.

    /// Seed `amount` of `coin` (e.g. "UETH", "UBTC", "USDC") into `user`'s spot
    /// balance. Mirrors `MockIntegratorServer::credit_hyperliquid_balance`.
    /// Used to fund an account before placing orders without a deposit flow.
    pub async fn seed_spot(&self, user: Address, coin: &str, amount: f64) {
        self.core.lock().await.credit_spot(user, coin, amount);
    }

    /// Seed `amount` of USDC collateral into `user`'s clearinghouse balance —
    /// the surface real Bridge2 deposits land on. Mirrors
    /// `MockIntegratorServer::credit_hyperliquid_clearinghouse_balance` with the
    /// coin fixed to "USDC" (the only clearinghouse coin that matters).
    pub async fn seed_clearinghouse(&self, user: Address, amount: f64) {
        self.core
            .lock()
            .await
            .credit_clearinghouse(user, "USDC", amount);
    }

    /// Read `user`'s `coin` spot `total` balance (natural units). Returns 0.0
    /// when absent. Mirrors `MockIntegratorServer::hyperliquid_balance_of`.
    pub async fn spot_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.core.lock().await.spot_total(user, coin)
    }

    /// Read `user`'s `coin` clearinghouse balance (natural units). Returns 0.0
    /// when absent. Mirrors
    /// `MockIntegratorServer::hyperliquid_clearinghouse_balance_of` (which takes
    /// the coin explicitly; callers pass "USDC").
    pub async fn clearinghouse_balance_of(&self, user: Address, coin: &str) -> f64 {
        self.core.lock().await.clearinghouse_total(user, coin)
    }

    /// Install (or overwrite) the exchange rate for a spot pair. `rate` is the
    /// price of one `base` unit in `quote` units (e.g.
    /// `set_rate("UBTC", "USDC", 60_000.0)` means 1 UBTC = 60 000 USDC). An IoC
    /// buy of `sz` base at limit_px ≥ rate fills at exactly `rate`. Mirrors the
    /// former `MockIntegratorServer::set_hyperliquid_rate`.
    pub async fn set_rate(&self, base: &str, quote: &str, rate: f64) {
        self.core.lock().await.set_rate(base, quote, rate);
    }

    /// Inspect the configured rate for a pair (`None` if none installed).
    pub async fn rate_for(&self, base: &str, quote: &str) -> Option<f64> {
        self.core.lock().await.rate_for(base, quote)
    }

    /// Override the synthesized top-of-book depth for both sides of a spot pair.
    /// Useful for partial-fill semantics. Mirrors the former
    /// `MockIntegratorServer::set_hyperliquid_book_depth`.
    pub async fn set_book_depth(&self, base: &str, quote: &str, depth: f64) {
        self.core.lock().await.set_book_depth(base, quote, depth);
    }

    /// Seed a historical fill for `/info { type: "userFills" }` /
    /// `userFillsByTime` shape tests. Mirrors the former
    /// `MockIntegratorServer::record_hyperliquid_fill`.
    pub async fn record_fill(&self, user: Address, fill: UserFill) {
        self.core.lock().await.record_fill(user, fill);
    }

    /// Seed a non-funding ledger update for `/info { type:
    /// "userNonFundingLedgerUpdates" }`. Mirrors the former
    /// `MockIntegratorServer::record_hyperliquid_ledger_update`.
    pub async fn record_ledger_update(&self, user: Address, update: UserNonFundingLedgerUpdate) {
        self.core.lock().await.record_ledger_update(user, update);
    }

    /// Seed a funding payment for `/info { type: "userFunding" }`. Mirrors the
    /// former `MockIntegratorServer::record_hyperliquid_funding`.
    pub async fn record_funding(&self, user: Address, funding: UserFunding) {
        self.core.lock().await.record_funding(user, funding);
    }
}

impl Drop for HyperliquidNode {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(shutdown) = self.bridge_indexer_shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

/// The guardian HL account address, derived once from the well-known devnet key.
fn guardian_address() -> Address {
    let signer = PrivateKeySigner::from_bytes(&HYPERLIQUID_GUARDIAN_PRIVATE_KEY.into())
        .expect("static guardian key bytes are a valid secp256k1 scalar");
    signer.address()
}

/// The node's OWN Bridge2 deposit indexer. Watches the USDC `Transfer` log on
/// the Arbitrum Anvil chain, filters for transfers *into* the Bridge2 address,
/// and credits THIS node's [`HyperliquidCore`] clearinghouse (plus a ledger
/// update). The node is a plain observer: no latency / failure injection (real
/// Hyperliquid has none either). Returns on `shutdown`.
async fn run_node_bridge_deposit_indexer(
    provider: DynProvider,
    token_address: Address,
    bridge_address: Address,
    core: Arc<HyperliquidCore>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = IERC20::Transfer::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = mock_evm_indexer_initial_last_scanned();
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "hyperliquid node bridge indexer: get_block_number failed");
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
                tracing::warn!(%err, "hyperliquid node bridge indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match IERC20::Transfer::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "hyperliquid node bridge indexer: decode Transfer failed");
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
                    "hyperliquid node bridge indexer: ignoring sub-minimum bridge deposit"
                );
                continue;
            }
            let Some(amount) = raw_usdc_to_natural_f64(event.value) else {
                tracing::warn!(
                    from = %event.from,
                    amount = %event.value,
                    "hyperliquid node bridge indexer: failed to convert transfer value"
                );
                continue;
            };
            let tx_hash = log
                .transaction_hash
                .map(|hash| format!("{hash:#x}"))
                .unwrap_or_else(|| {
                    format!(
                        "0x{}",
                        hex::encode(Sha256::digest(format!(
                            "node-hl-deposit:{}:{}",
                            event.from, event.value
                        )))
                    )
                });
            let mut hl = core.lock().await;
            hl.credit_clearinghouse(event.from, "USDC", amount);
            hl.record_ledger_update(
                event.from,
                UserNonFundingLedgerUpdate {
                    time: Utc::now().timestamp_millis().max(0) as u64,
                    hash: tx_hash.clone(),
                    delta: UserNonFundingLedgerDelta::Deposit {
                        usdc: format_hl_amount(amount),
                    },
                },
            );
            drop(hl);
            tracing::debug!(
                user = %event.from,
                amount,
                tx_hash,
                "hyperliquid node bridge indexer: credited clearinghouse deposit"
            );
        }
        last_scanned = head;
    }
}

fn raw_usdc_to_natural_f64(value: U256) -> Option<f64> {
    let raw: u128 = value.try_into().ok()?;
    Some(raw as f64 / 1_000_000.0)
}

/// Builds the node's router: the same `/info` + `/exchange` routes Hyperliquid
/// exposes, backed by the node's own state.
fn router() -> Router<Arc<HyperliquidNodeState>> {
    Router::new()
        .route("/info", post(node_info).get(node_info))
        .route("/exchange", post(node_exchange))
}

fn error_response(status: StatusCode, message: impl Into<String>) -> axum::response::Response {
    (status, Json(json!({ "error": message.into() }))).into_response()
}

fn parse_user_address(value: &str) -> Option<Address> {
    value
        .starts_with("0x")
        .then(|| Address::from_str(value).ok())
        .flatten()
}

fn required_user(request: &Value, endpoint: &str) -> Result<Address, String> {
    request
        .get("user")
        .and_then(Value::as_str)
        .and_then(parse_user_address)
        .ok_or_else(|| format!("{endpoint} requires a 0x-prefixed `user` field"))
}

fn required_u64(request: &Value, endpoint: &str, field: &str) -> Result<u64, String> {
    request
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("{endpoint} requires a numeric `{field}` field"))
}

fn optional_u64(request: &Value, field: &str) -> Option<u64> {
    request.get(field).and_then(Value::as_u64)
}

fn validation_error(message: String) -> axum::response::Response {
    error_response(StatusCode::UNPROCESSABLE_ENTITY, message)
}

/// The node's `/info` read-only endpoint. Dispatches on the `type`
/// discriminator and reuses the core's snapshot methods. Mirrors the venue
/// mock's `/info` so clients speak the same wire shape, but reads from this
/// node's own ledger.
async fn node_info(
    State(state): State<Arc<HyperliquidNodeState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
    let Some(kind) = request.get("type").and_then(Value::as_str) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "hyperliquid node /info requires a `type` field",
        );
    };
    let mut hl = state.core.lock().await;
    hl.run_due_scheduled_cancels();
    match kind {
        "spotMeta" => match hl.spot_meta_value() {
            Ok(value) => Json(value).into_response(),
            Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        },
        "meta" => Json(hl.perp_meta()).into_response(),
        "l2Book" => {
            let Some(coin) = request.get("coin").and_then(Value::as_str) else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "l2Book requires a `coin` field",
                );
            };
            Json(hl.l2_book_snapshot(coin)).into_response()
        }
        "orderStatus" => {
            let user = match required_user(&request, "orderStatus") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            let Some(oid) = request.get("oid").and_then(Value::as_u64) else {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "orderStatus requires a numeric `oid` field",
                );
            };
            Json(hl.order_status_snapshot(user, oid)).into_response()
        }
        "spotClearinghouseState" => {
            let user = match required_user(&request, "spotClearinghouseState") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            Json(hl.spot_clearinghouse_snapshot(user)).into_response()
        }
        "clearinghouseState" => {
            let user = match required_user(&request, "clearinghouseState") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            Json(hl.clearinghouse_snapshot(user)).into_response()
        }
        "openOrders" => {
            let user = match required_user(&request, "openOrders") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            Json(hl.open_orders_for(user)).into_response()
        }
        "userFills" => {
            let user = match required_user(&request, "userFills") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            Json(hl.fills_for(user)).into_response()
        }
        "userFillsByTime" => {
            let user = match required_user(&request, "userFillsByTime") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            let start_time = match required_u64(&request, "userFillsByTime", "startTime") {
                Ok(start_time) => start_time,
                Err(message) => return validation_error(message),
            };
            let end_time = optional_u64(&request, "endTime");
            let aggregate_by_time = request
                .get("aggregateByTime")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Json(hl.fills_by_time(user, start_time, end_time, aggregate_by_time)).into_response()
        }
        "userNonFundingLedgerUpdates" => {
            let user = match required_user(&request, "userNonFundingLedgerUpdates") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            let start_time =
                match required_u64(&request, "userNonFundingLedgerUpdates", "startTime") {
                    Ok(start_time) => start_time,
                    Err(message) => return validation_error(message),
                };
            let end_time = optional_u64(&request, "endTime");
            Json(hl.ledger_updates_by_time(user, start_time, end_time)).into_response()
        }
        "userFunding" => {
            let user = match required_user(&request, "userFunding") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            let start_time = match required_u64(&request, "userFunding", "startTime") {
                Ok(start_time) => start_time,
                Err(message) => return validation_error(message),
            };
            let end_time = optional_u64(&request, "endTime");
            Json(hl.fundings_by_time(user, start_time, end_time)).into_response()
        }
        "userRateLimit" => {
            let user = match required_user(&request, "userRateLimit") {
                Ok(user) => user,
                Err(message) => return validation_error(message),
            };
            Json(hl.user_rate_limit(user)).into_response()
        }
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("hyperliquid node /info does not support type {other}"),
        ),
    }
}

/// The node's `/exchange` signed-action endpoint. Recovers the signer, then
/// reuses the core's mutation methods. The node performs *only* on-HL effects:
/// `spotSend` / `sendAsset` do the spot transfer with **no** Unit-withdrawal
/// trigger, and `withdraw3` debits the clearinghouse with the on-chain Bridge2
/// release deferred to phase 2.
async fn node_exchange(
    State(state): State<Arc<HyperliquidNodeState>>,
    Json(request): Json<Value>,
) -> impl IntoResponse {
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
        "sendAsset" => handle_send_asset(&state, &request).await,
        "usdClassTransfer" => handle_usd_class_transfer(&state, &request).await,
        "withdraw3" => handle_withdraw3(&state, &request).await,
        other => error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("hyperliquid node /exchange does not support action type {other}"),
        ),
    }
}

async fn handle_l1_action(
    state: &Arc<HyperliquidNodeState>,
    request: &Value,
    action_type: &str,
) -> axum::response::Response {
    let Some(action_value) = request.get("action").cloned() else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let action: Actions = match serde_json::from_value(action_value) {
        Ok(a) => a,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize {action_type} action: {err}"),
            );
        }
    };
    let Some(nonce) = request.get("nonce").and_then(Value::as_u64) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `nonce`",
        );
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
    let signer = match recover_l1_signer(connection_id, state.mainnet, &signature) {
        Ok(addr) => addr,
        Err(err) => {
            return error_response(
                StatusCode::UNAUTHORIZED,
                format!("signature recovery failed: {err}"),
            );
        }
    };
    let user = vault_address.unwrap_or(signer);

    let mut hl = state.core.lock().await;
    hl.run_due_scheduled_cancels();
    match action {
        Actions::Order(bulk) => {
            let statuses = hl.place_spot_orders(user, &bulk);
            Json(json!({
                "status": "ok",
                "response": { "type": "order", "data": { "statuses": statuses } }
            }))
            .into_response()
        }
        Actions::Cancel(bulk) => {
            let statuses = hl.cancel_spot_orders(user, &bulk);
            Json(json!({
                "status": "ok",
                "response": { "type": "cancel", "data": { "statuses": statuses } }
            }))
            .into_response()
        }
        Actions::ScheduleCancel(schedule) => {
            hl.set_schedule_cancel(user, schedule.time);
            Json(json!({
                "status": "ok",
                "response": { "type": "scheduleCancel" }
            }))
            .into_response()
        }
    }
}

async fn handle_spot_send(
    state: &Arc<HyperliquidNodeState>,
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
    let mut stripped = action_value;
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

    let token_symbol = token_symbol(&payload.token);
    let amount = match parse_positive_amount(&payload.amount, "spotSend") {
        Ok(amount) => amount,
        Err(message) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, message),
    };

    // NOTE(phase 1): the node does NOT know about Unit. This is just the spot
    // transfer (debit signer, credit destination). The Unit-withdrawal trigger
    // becomes the Unit mock watching this transfer in a later phase.
    let mut hl = state.core.lock().await;
    let available = hl.available_spot(signer, &token_symbol);
    if !hyperliquid_has_sufficient_amount(available, amount) {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("spotSend: {signer:?} has {available} {token_symbol}, needs {amount}"),
        );
    }
    hl.debit_spot_total(signer, &token_symbol, amount);
    if let Ok(dst) = Address::from_str(&payload.destination) {
        hl.credit_spot(dst, &token_symbol, amount);
        // Record the spotSend as a `SpotTransfer` ledger update on both the
        // sender and the recipient — exactly what real Hyperliquid does. The
        // hl-shim indexes these and downstream observers (e.g. sauron's Unit
        // deposit credit detector, which watches the recipient for an incoming
        // transfer) rely on them. Without this, a node spotSend moves the
        // balance silently and the credit is invisible to the indexer.
        record_node_spot_transfer(
            &mut hl,
            signer,
            dst,
            &token_symbol,
            &payload.amount,
            payload.time,
        );
    }

    Json(json!({ "status": "ok", "response": { "type": "default" } })).into_response()
}

async fn handle_send_asset(
    state: &Arc<HyperliquidNodeState>,
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
    let payload: SendAsset = match serde_json::from_value(stripped) {
        Ok(p) => p,
        Err(err) => {
            return error_response(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("failed to deserialize sendAsset: {err}"),
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
                format!("sendAsset signature recovery failed: {err}"),
            );
        }
    };
    if payload.source_dex != "spot" || payload.destination_dex != "spot" {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "node sendAsset only supports spot->spot transfers, got {}->{}",
                payload.source_dex, payload.destination_dex
            ),
        );
    }
    let source_user = if payload.from_sub_account.trim().is_empty() {
        signer
    } else {
        match Address::from_str(&payload.from_sub_account) {
            Ok(address) => address,
            Err(err) => {
                return error_response(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    format!("sendAsset fromSubAccount is not an address: {err}"),
                );
            }
        }
    };
    let token_symbol = token_symbol(&payload.token);
    let amount = match parse_positive_amount(&payload.amount, "sendAsset") {
        Ok(amount) => amount,
        Err(message) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, message),
    };

    // NOTE(phase 1): no Unit-withdrawal trigger — just the spot transfer.
    let mut hl = state.core.lock().await;
    let available = hl.available_spot(source_user, &token_symbol);
    if !hyperliquid_has_sufficient_amount(available, amount) {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("sendAsset: {source_user:?} has {available} {token_symbol}, needs {amount}"),
        );
    }
    hl.debit_spot_total(source_user, &token_symbol, amount);
    if let Ok(dst) = Address::from_str(&payload.destination) {
        hl.credit_spot(dst, &token_symbol, amount);
        // Faithful to real Hyperliquid: a spot->spot sendAsset records a
        // `SpotTransfer` ledger update on both parties (see `handle_spot_send`).
        record_node_spot_transfer(
            &mut hl,
            source_user,
            dst,
            &token_symbol,
            &payload.amount,
            payload.nonce,
        );
    }

    Json(json!({ "status": "ok", "response": { "type": "default" } })).into_response()
}

/// Record a `SpotTransfer` ledger update on both the sender and the recipient
/// of a node spot transfer (`spotSend` / spot->spot `sendAsset`), mirroring
/// Hyperliquid's real behavior. The hl-shim indexes each side's update; the
/// recipient's positive-delta entry is what the Unit deposit-credit observer
/// watches for. `nonce` is the action's time/nonce (the transfer's identity).
fn record_node_spot_transfer(
    hl: &mut crate::hyperliquid_core::HyperliquidCoreState,
    sender: Address,
    recipient: Address,
    token_symbol: &str,
    amount: &str,
    nonce: u64,
) {
    let time_ms = Utc::now().timestamp_millis().max(0) as u64;
    let hash = format!(
        "0x{}",
        hex::encode(Sha256::digest(format!(
            "node-spot-transfer:{sender}:{recipient}:{token_symbol}:{amount}:{nonce}"
        )))
    );
    let delta = UserNonFundingLedgerDelta::SpotTransfer {
        token: token_symbol.to_string(),
        amount: amount.to_string(),
        usdc_value: "0".to_string(),
        user: format!("{sender:#x}"),
        destination: format!("{recipient:#x}"),
        fee: "0".to_string(),
        native_token_fee: "0".to_string(),
        nonce,
    };
    // Both ledgers carry the same SpotTransfer delta; the hl-shim signs the
    // amount per-owner (negative for the sender, positive for the recipient).
    hl.record_ledger_update(
        sender,
        UserNonFundingLedgerUpdate {
            time: time_ms,
            hash: hash.clone(),
            delta: delta.clone(),
        },
    );
    hl.record_ledger_update(
        recipient,
        UserNonFundingLedgerUpdate {
            time: time_ms,
            hash,
            delta,
        },
    );
}

async fn handle_usd_class_transfer(
    state: &Arc<HyperliquidNodeState>,
    request: &Value,
) -> axum::response::Response {
    let Some(action_value) = request.get("action") else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "/exchange body is missing `action`",
        );
    };
    let Some(amount_with_target) = action_value.get("amount").and_then(Value::as_str) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "usdClassTransfer requires string field `amount`",
        );
    };
    let amount_with_target = amount_with_target.to_string();
    let Some(to_perp) = action_value.get("toPerp").and_then(Value::as_bool) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "usdClassTransfer requires bool field `toPerp`",
        );
    };
    let Some(nonce) = action_value.get("nonce").and_then(Value::as_u64) else {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            "usdClassTransfer requires u64 field `nonce`",
        );
    };
    let signature = match parse_signature(request.get("signature")) {
        Ok(sig) => sig,
        Err(msg) => return error_response(StatusCode::UNPROCESSABLE_ENTITY, msg),
    };
    let payload = UsdClassTransfer {
        signature_chain_id: if state.mainnet { 42_161 } else { 421_614 },
        hyperliquid_chain: if state.mainnet { "Mainnet" } else { "Testnet" }.to_string(),
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

    let mut hl = state.core.lock().await;
    let available = if to_perp {
        hl.available_spot(user, "USDC")
    } else {
        hl.clearinghouse_total(user, "USDC")
    };
    if !hyperliquid_has_sufficient_amount(available, amount) {
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

    Json(json!({ "status": "ok", "response": { "type": "default" } })).into_response()
}

async fn handle_withdraw3(
    state: &Arc<HyperliquidNodeState>,
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
                "withdraw3 amount must exceed the {HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW} raw USDC fee"
            ),
        );
    }
    let gross_amount = amount_raw as f64 / 1_000_000.0;

    let mut hl = state.core.lock().await;
    let available = hl.clearinghouse_total(user, "USDC");
    if !hyperliquid_has_sufficient_amount(available, gross_amount) {
        return error_response(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("withdraw3: {user:?} has {available} USDC withdrawable, needs {gross_amount}"),
        );
    }
    hl.debit_clearinghouse_total(user, "USDC", gross_amount);
    hl.record_ledger_update(
        user,
        UserNonFundingLedgerUpdate {
            time: payload.time,
            hash: format!(
                "0x{}",
                hex::encode(Sha256::digest(format!(
                    "node-hl-withdraw:{user}:{destination}:{amount_raw}:{}",
                    payload.time
                )))
            ),
            delta: UserNonFundingLedgerDelta::Withdraw {
                usdc: payload.amount.clone(),
                nonce: payload.time,
                fee: "1".to_string(),
            },
        },
    );
    drop(hl);

    // Phase 2: release `amount_raw - fee` to `destination` on the Arbitrum
    // Bridge2 contract — the node now owns this on-chain effect. Mirrors the
    // venue mock's `send_mock_hyperliquid_withdrawal_release`: mint the payout
    // to the bridge, then submit the Bridge2 `release` tx. The node uses its OWN
    // known-funded release signer (instead of the mock's EIP-7702 paymaster). No
    // bridge wiring => clearinghouse-only debit (Phase-1 behavior preserved).
    let payout_raw = amount_raw - HYPERLIQUID_BRIDGE_WITHDRAW_FEE_RAW;
    if let Some(wiring) = state.bridge_release.clone() {
        tokio::spawn(async move {
            if let Err(err) =
                send_node_bridge_withdrawal_release(&wiring, destination, payout_raw).await
            {
                tracing::warn!(
                    %err,
                    %destination,
                    payout_raw,
                    "hyperliquid node withdraw release failed"
                );
            }
        });
    }

    Json(json!({ "status": "ok", "response": { "type": "default" } })).into_response()
}

/// Submits the node's OWN on-chain Bridge2 withdrawal release: mints `payout_raw`
/// USDC to the bridge (the bridge must hold the funds it `transfer`s out), then
/// calls Bridge2 `release(destination, payout_raw)` against the
/// [`MockHyperliquidBridge2`] bindings, signing with the node's known-funded
/// release key (the Arbitrum Anvil account-0, the mock-USDC master minter) via a
/// plain wallet provider.
async fn send_node_bridge_withdrawal_release(
    wiring: &NodeBridgeReleaseWiring,
    destination: Address,
    payout_raw: u64,
) -> Result<(), String> {
    // The shared, nonce-managed release provider (built once at node spawn) so
    // concurrent withdraw3 releases get sequential nonces instead of colliding.
    let provider = wiring.release_provider.clone();

    // 1) Fund the bridge with the payout so its `release` `transfer` succeeds.
    let mint_calldata = Bytes::from(
        IMockMintableERC20::mintCall {
            recipient: wiring.bridge_address,
            amount: U256::from(payout_raw),
        }
        .abi_encode(),
    );
    let mint = alloy::rpc::types::TransactionRequest::default()
        .to(wiring.usdc_token_address)
        .input(mint_calldata.into());
    provider
        .send_transaction(mint)
        .await
        .map_err(|err| format!("hyperliquid node release mint submit failed: {err}"))?
        .get_receipt()
        .await
        .map_err(|err| format!("hyperliquid node release mint receipt failed: {err}"))?;

    // 2) Call Bridge2 `release(destination, payout_raw)` via the shared bindings.
    let bridge = MockHyperliquidBridge2::new(wiring.bridge_address, provider);
    bridge
        .release(destination, payout_raw)
        .send()
        .await
        .map_err(|err| format!("hyperliquid node bridge release submit failed: {err}"))?
        .get_receipt()
        .await
        .map_err(|err| format!("hyperliquid node bridge release receipt failed: {err}"))?;
    Ok(())
}

/// The plain token symbol from a HL wire token string (`"SYMBOL:0x..."`).
fn token_symbol(token: &str) -> String {
    token.split(':').next().unwrap_or(token).to_string()
}

fn parse_positive_amount(value: &str, action: &str) -> Result<f64, String> {
    match value.parse::<f64>() {
        Ok(v) if v > 0.0 => Ok(v),
        _ => Err(format!(
            "{action} amount must be a positive decimal, got {value}"
        )),
    }
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
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
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

#[cfg(test)]
mod tests {
    use super::*;
    use hyperliquid_client::{client::Network, HyperliquidExchangeClient, HyperliquidInfoClient};

    /// End-to-end node smoke test: spawn the node, hit `/info` (GET + POST),
    /// seed a guardian-style spot credit, perform a signed `spotSend` via
    /// `/exchange`, then read `/info` back to confirm the balance moved. Drives
    /// a real `HyperliquidClient` so the test exercises the same signing /
    /// wire-format path production uses.
    #[tokio::test]
    async fn node_spot_send_moves_balance_end_to_end() {
        let node = HyperliquidNode::spawn().await.expect("spawn node");

        // GET /info { type: spotMeta } — confirm the node serves both verbs.
        let get_meta: Value = reqwest::Client::new()
            .get(format!("{}/info", node.url()))
            .json(&json!({ "type": "spotMeta" }))
            .send()
            .await
            .expect("GET /info")
            .json()
            .await
            .expect("spotMeta json");
        assert!(get_meta["tokens"].is_array(), "GET /info returns spotMeta");

        // POST /info { type: spotMeta } via the real info client.
        let info = HyperliquidInfoClient::new(&node.url()).expect("info client");
        let meta = info.fetch_spot_meta().await.expect("spot meta");
        assert!(meta.tokens.iter().any(|t| t.name == "UBTC"));

        // Genesis: the guardian was seeded with unbounded UBTC.
        let guardian = node.guardian_address();
        assert!(
            node.core().lock().await.spot_total(guardian, "UBTC") >= GUARDIAN_GENESIS_BALANCE,
            "guardian seeded with genesis UBTC"
        );

        // Seed a sender with spot UBTC directly on the node's core, then drive a
        // signed spotSend to a fresh destination via /exchange.
        let sender_key = [0x42u8; 32];
        let sender = alloy::signers::local::PrivateKeySigner::from_bytes(&sender_key.into())
            .expect("sender key");
        let sender_address = sender.address();
        let destination = Address::repeat_byte(0x99);

        node.core()
            .lock()
            .await
            .credit_spot(sender_address, "UBTC", 5.0);

        let exchange = HyperliquidExchangeClient::new(&node.url(), sender, None, Network::Testnet)
            .expect("exchange client");
        let time_ms = 1_700_000_000_000;
        let response = exchange
            .spot_send(
                format!("{destination:#x}"),
                "UBTC:0x11111111111111111111111111111111".to_string(),
                "2".to_string(),
                time_ms,
            )
            .await
            .expect("spotSend");
        assert_eq!(response["status"], "ok", "spotSend accepted: {response}");

        // Read /info { type: spotClearinghouseState } back over raw HTTP:
        // sender debited, destination credited.
        let sender_state = spot_clearinghouse_state(&node.url(), sender_address).await;
        let sender_ubtc = balance_of(&sender_state, "UBTC");
        assert!(
            (sender_ubtc - 3.0).abs() < 1e-9,
            "sender UBTC should be 3.0 after sending 2.0, got {sender_ubtc}"
        );

        let dst_state = spot_clearinghouse_state(&node.url(), destination).await;
        let dst_ubtc = balance_of(&dst_state, "UBTC");
        assert!(
            (dst_ubtc - 2.0).abs() < 1e-9,
            "destination UBTC should be 2.0, got {dst_ubtc}"
        );
    }

    async fn spot_clearinghouse_state(base_url: &str, user: Address) -> Value {
        reqwest::Client::new()
            .post(format!("{base_url}/info"))
            .json(&json!({
                "type": "spotClearinghouseState",
                "user": format!("{user:#x}"),
            }))
            .send()
            .await
            .expect("POST /info spotClearinghouseState")
            .json()
            .await
            .expect("spotClearinghouseState json")
    }

    fn balance_of(state: &Value, coin: &str) -> f64 {
        state["balances"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|entry| entry["coin"].as_str() == Some(coin))
            .and_then(|entry| entry["total"].as_str())
            .and_then(|total| total.parse::<f64>().ok())
            .unwrap_or(0.0)
    }

    // --- `/info` wire-shape + validation coverage (ported from the deleted
    //     `hyperliquid_spot` venue tests; the node is now the sole Hyperliquid).
    //     These assert the node's `/info` responses round-trip into the real
    //     `hyperliquid_client` types and that the handler's required-field
    //     validations reject malformed requests with UNPROCESSABLE_ENTITY. ---

    async fn info_post(node: &HyperliquidNode, body: Value) -> reqwest::Response {
        reqwest::Client::new()
            .post(format!("{}/info", node.url()))
            .json(&body)
            .send()
            .await
            .expect("POST /info")
    }

    async fn assert_info_error(node: &HyperliquidNode, body: Value, expected_error: &str) {
        let response = info_post(node, body).await;
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let body: Value = response.json().await.expect("error body");
        assert_eq!(body["error"].as_str(), Some(expected_error));
    }

    fn shape_test_user() -> Address {
        "0x1111111111111111111111111111111111111111"
            .parse()
            .expect("static address parses")
    }

    fn sample_user_fill(time: u64, tid: u64) -> UserFill {
        UserFill {
            coin: "UBTC/USDC".to_string(),
            px: "60000".to_string(),
            sz: "0.001".to_string(),
            side: "B".to_string(),
            time,
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

    #[tokio::test]
    async fn info_spot_meta_matches_real_shape() {
        use hyperliquid_client::meta::SpotMeta;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let body: SpotMeta = info_post(&node, json!({ "type": "spotMeta" }))
            .await
            .json()
            .await
            .expect("deserialize SpotMeta");
        let map = body
            .coin_to_asset_map()
            .expect("node spotMeta should produce valid wire asset ids");
        assert_eq!(map.get("UBTC/USDC"), Some(&10_140));
        assert_eq!(map.get("@140"), Some(&10_140));
    }

    #[tokio::test]
    async fn info_meta_matches_real_shape() {
        use hyperliquid_client::info::PerpMeta;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let raw: Value = info_post(&node, json!({ "type": "meta" }))
            .await
            .json()
            .await
            .expect("json");
        assert!(raw["universe"][0]["szDecimals"].is_number());
        assert!(raw["universe"][0]["maxLeverage"].is_number());
        let body: PerpMeta = serde_json::from_value(raw).expect("deserialize PerpMeta");
        assert!(body.universe.iter().any(|asset| asset.name == "BTC"));
        assert!(body.universe.iter().any(|asset| asset.name == "ETH"));
    }

    #[tokio::test]
    async fn info_user_fills_by_time_filters_and_matches_real_shape() {
        use hyperliquid_client::info::UserFill as ClientUserFill;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let user = shape_test_user();
        node.record_fill(user, sample_user_fill(1_700_000_000_000, 1))
            .await;
        node.record_fill(user, sample_user_fill(1_700_000_010_000, 2))
            .await;
        node.record_fill(user, sample_user_fill(1_700_000_020_000, 3))
            .await;

        let raw: Value = info_post(
            &node,
            json!({
                "type": "userFillsByTime",
                "user": format!("{user:#x}"),
                "startTime": 1_700_000_005_000_u64,
                "endTime": 1_700_000_020_000_u64,
                "aggregateByTime": false
            }),
        )
        .await
        .json()
        .await
        .expect("json");
        assert_eq!(raw.as_array().expect("array").len(), 2);
        assert!(raw[0]["startPosition"].is_string());
        assert!(raw[0]["feeToken"].is_string());
        let body: Vec<ClientUserFill> = serde_json::from_value(raw).expect("deserialize UserFill");
        assert_eq!(
            body.iter().map(|fill| fill.tid).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[tokio::test]
    async fn info_user_fills_by_time_rejects_missing_start_time_and_bad_user() {
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        assert_info_error(
            &node,
            json!({ "type": "userFillsByTime", "user": "0x1111111111111111111111111111111111111111" }),
            "userFillsByTime requires a numeric `startTime` field",
        )
        .await;
        assert_info_error(
            &node,
            json!({ "type": "userFillsByTime", "user": "1111111111111111111111111111111111111111", "startTime": 1_u64 }),
            "userFillsByTime requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn info_user_non_funding_ledger_updates_matches_real_shape() {
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let user = shape_test_user();
        let hash = format!("0x{}", "cd".repeat(32));
        for (time, delta) in [
            (
                1_700_000_001_000,
                UserNonFundingLedgerDelta::Withdraw {
                    usdc: "1.5".to_string(),
                    nonce: 7,
                    fee: "1".to_string(),
                },
            ),
            (
                1_700_000_004_000,
                UserNonFundingLedgerDelta::SpotTransfer {
                    token: "UBTC:0x11111111111111111111111111111111".to_string(),
                    amount: "0.001".to_string(),
                    usdc_value: "60".to_string(),
                    user: format!("{user:#x}"),
                    destination: "0x2222222222222222222222222222222222222222".to_string(),
                    fee: "0".to_string(),
                    native_token_fee: "0.00001".to_string(),
                    nonce: 8,
                },
            ),
        ] {
            node.record_ledger_update(
                user,
                UserNonFundingLedgerUpdate {
                    time,
                    hash: hash.clone(),
                    delta,
                },
            )
            .await;
        }
        let raw: Value = info_post(
            &node,
            json!({
                "type": "userNonFundingLedgerUpdates",
                "user": format!("{user:#x}"),
                "startTime": 1_700_000_001_000_u64,
                "endTime": 1_700_000_004_000_u64
            }),
        )
        .await
        .json()
        .await
        .expect("json");
        assert_eq!(raw.as_array().expect("array").len(), 2);
        assert_eq!(raw[0]["delta"]["type"], "withdraw");
        assert!(raw[1]["delta"]["usdcValue"].is_string());
        let body: Vec<UserNonFundingLedgerUpdate> =
            serde_json::from_value(raw).expect("deserialize ledger updates");
        assert_eq!(body.len(), 2);
    }

    #[tokio::test]
    async fn info_user_funding_matches_real_shape_and_rejects_bad_input() {
        use hyperliquid_client::info::UserFunding as ClientUserFunding;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let user = shape_test_user();
        node.record_funding(
            user,
            UserFunding {
                time: 1_700_000_000_000,
                coin: "ETH".to_string(),
                usdc: "-0.0123".to_string(),
                szi: "1.25".to_string(),
                funding_rate: "0.0000125".to_string(),
            },
        )
        .await;
        let raw: Value = info_post(
            &node,
            json!({ "type": "userFunding", "user": format!("{user:#x}"), "startTime": 1_699_999_999_999_u64 }),
        )
        .await
        .json()
        .await
        .expect("json");
        let body: Vec<ClientUserFunding> =
            serde_json::from_value(raw).expect("deserialize funding");
        assert_eq!(body[0].usdc, "-0.0123");

        assert_info_error(
            &node,
            json!({ "type": "userFunding", "user": "0x1111111111111111111111111111111111111111" }),
            "userFunding requires a numeric `startTime` field",
        )
        .await;
    }

    #[tokio::test]
    async fn info_user_rate_limit_matches_real_shape_and_rejects_bad_user() {
        use hyperliquid_client::info::UserRateLimit;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let raw: Value = info_post(
            &node,
            json!({ "type": "userRateLimit", "user": format!("{:#x}", shape_test_user()) }),
        )
        .await
        .json()
        .await
        .expect("json");
        let body: UserRateLimit = serde_json::from_value(raw).expect("deserialize rate limit");
        assert_eq!(body.n_requests_cap, 1200);

        assert_info_error(
            &node,
            json!({ "type": "userRateLimit" }),
            "userRateLimit requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn info_l2_book_rejects_missing_coin() {
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        assert_info_error(
            &node,
            json!({ "type": "l2Book" }),
            "l2Book requires a `coin` field",
        )
        .await;
    }

    #[tokio::test]
    async fn info_order_status_rejects_missing_oid_and_user() {
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        assert_info_error(
            &node,
            json!({ "type": "orderStatus", "user": "0x0000000000000000000000000000000000000000" }),
            "orderStatus requires a numeric `oid` field",
        )
        .await;
        assert_info_error(
            &node,
            json!({ "type": "orderStatus", "oid": 42 }),
            "orderStatus requires a 0x-prefixed `user` field",
        )
        .await;
    }

    #[tokio::test]
    async fn info_order_status_unknown_oid_is_scoped_to_user() {
        use hyperliquid_client::info::OrderStatusResponse;
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        // An order placed by one user is `unknownOid` when queried for another.
        let placer = PrivateKeySigner::from_bytes(&[0x42u8; 32].into()).expect("placer key");
        let placer_address = placer.address();
        node.seed_spot(placer_address, "USDC", 10_000.0).await;
        node.set_rate("UBTC", "USDC", 60_000.0).await;
        let mut client =
            hyperliquid_client::HyperliquidClient::new(&node.url(), placer, None, Network::Testnet)
                .expect("client");
        client.refresh_spot_meta().await.expect("spot meta");
        let place = client
            .place_orders(
                vec![hyperliquid_client::OrderRequest {
                    asset: client.asset_index("UBTC/USDC").expect("asset"),
                    is_buy: true,
                    limit_px: "60000".to_string(),
                    sz: "0.1".to_string(),
                    reduce_only: false,
                    order_type: hyperliquid_client::Order::Limit(hyperliquid_client::Limit {
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

        // The placer sees the filled order; a different user gets unknownOid.
        let other = Address::repeat_byte(0x77);
        let response: OrderStatusResponse = info_post(
            &node,
            json!({ "type": "orderStatus", "user": format!("{other:#x}"), "oid": oid }),
        )
        .await
        .json()
        .await
        .expect("order status");
        assert_eq!(response.status, "unknownOid");
        assert!(response.order.is_none());

        let own: OrderStatusResponse = info_post(
            &node,
            json!({ "type": "orderStatus", "user": format!("{placer_address:#x}"), "oid": oid }),
        )
        .await
        .json()
        .await
        .expect("order status");
        assert_eq!(own.order.expect("envelope").status, "filled");
    }

    /// Ported from `hyperliquid_spot`'s `chained_sell_then_buy_credits_both_legs`:
    /// a vault-scoped client whose buy is rejected (insufficient balance →
    /// `rejected` status, no fill), then a funded sell→buy chain crediting both
    /// legs. Exercises vault-address routing through the node's `/exchange`.
    #[tokio::test]
    async fn vault_scoped_chained_sell_then_buy_credits_both_legs() {
        use hyperliquid_client::{HyperliquidClient, Limit, Order, OrderRequest};
        let node = HyperliquidNode::spawn().await.expect("spawn node");
        let wallet = PrivateKeySigner::from_bytes(&[0x24u8; 32].into()).expect("wallet");
        let vault = Address::repeat_byte(0xc2);
        let mut client = HyperliquidClient::new(&node.url(), wallet, Some(vault), Network::Testnet)
            .expect("client");
        client.refresh_spot_meta().await.expect("spot meta");
        node.set_rate("UBTC", "USDC", 60_000.0).await;
        node.set_rate("UETH", "USDC", 3_000.0).await;

        let rejected = client
            .place_orders(
                vec![OrderRequest {
                    asset: client.asset_index("UETH/USDC").expect("ueth asset"),
                    is_buy: true,
                    limit_px: "3000".to_string(),
                    sz: "1".to_string(),
                    reduce_only: false,
                    order_type: Order::Limit(Limit {
                        tif: "Ioc".to_string(),
                    }),
                    cloid: None,
                }],
                "na",
            )
            .await
            .expect("prefundless buy");
        let err = rejected["response"]["data"]["statuses"][0]["error"]
            .as_str()
            .expect("insufficient balance error");
        assert!(err.contains("insufficient"), "unexpected rejection: {err}");
        let rejected_status = client
            .order_status(vault, 1000)
            .await
            .expect("rejected status");
        assert_eq!(
            rejected_status.order.expect("rejected order").status,
            "rejected"
        );

        node.seed_spot(vault, "UBTC", 1.0).await;
        let sell = client
            .place_orders(
                vec![OrderRequest {
                    asset: client.asset_index("UBTC/USDC").expect("ubtc asset"),
                    is_buy: false,
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
            .expect("sell UBTC");
        assert!(sell["response"]["data"]["statuses"][0]
            .get("filled")
            .is_some());

        let buy = client
            .place_orders(
                vec![OrderRequest {
                    asset: client.asset_index("UETH/USDC").expect("ueth asset"),
                    is_buy: true,
                    limit_px: "3000".to_string(),
                    sz: "1".to_string(),
                    reduce_only: false,
                    order_type: Order::Limit(Limit {
                        tif: "Ioc".to_string(),
                    }),
                    cloid: None,
                }],
                "na",
            )
            .await
            .expect("buy UETH");
        assert!(buy["response"]["data"]["statuses"][0]
            .get("filled")
            .is_some());

        let state = client
            .spot_clearinghouse_state(vault)
            .await
            .expect("post-buy spot state");
        assert_eq!(state.balance_of("USDC"), "3000");
        assert_eq!(state.balance_of("UETH"), "1");
        assert_eq!(state.balance_of("UBTC"), "0.9");
    }
}
