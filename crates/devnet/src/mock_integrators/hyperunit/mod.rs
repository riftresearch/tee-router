use alloy::{
    hex,
    primitives::{Address, Bytes, U256},
    providers::{ext::AnvilApi, DynProvider, Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use bitcoincore_rpc_async::{Auth as BitcoinRpcAuth, Client as BitcoinRpcClient, RpcApi};
use chrono::Utc;
use hyperliquid_client::{
    client::Network as HyperliquidClientNetwork, HyperliquidExchangeClient, HyperliquidInfoClient,
    UserNonFundingLedgerDelta,
};
use hyperunit_client::{
    UnitAsset, UnitChain, UnitGenerateAddressResponse, UnitOperation, UnitOperationsResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::Mutex, task::JoinHandle};

use eip7702_paymaster::Execution;

use crate::mock_integrators::{
    error_response, MockIntegratorConfig, MockIntegratorErrorResponse, MockService,
    MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS,
};

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

/// Mock's internal Unit operation record. Composes the wire-level
/// [`UnitOperation`] (what `/operations/:address` returns) with a `kind`
/// discriminator and an optional order-tag the router passes via the
/// `dst_addr` (for deposits, the router's HL custody address; for
/// withdrawals, the user's BTC/ETH address). Advancing the `operation.state`
/// is how the mock simulates HyperUnit's lifecycle machine.
#[derive(Debug, Clone)]
pub(crate) struct MockUnitOperationEntry {
    pub(crate) kind: MockUnitOperationKind,
    pub(crate) src_chain: UnitChain,
    pub(crate) dst_chain: UnitChain,
    pub(crate) asset: UnitAsset,
    pub(crate) dst_addr: String,
    pub(crate) visible: bool,
    pub(crate) operation: UnitOperation,
}

#[derive(Clone)]
pub(crate) struct MockUnitProtocolKey {
    pub(crate) private_key: String,
}

impl fmt::Debug for MockUnitProtocolKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MockUnitProtocolKey")
            .field("private_key", &"redacted")
            .finish()
    }
}

/// Per-venue state for the HyperUnit mock: the `/gen` request log, the tracked
/// operation lifecycle (keyed by `protocol_address`), the protocol private
/// keys, the per-chain EVM RPC URLs and Bitcoin RPC connection used by the
/// deposit indexers, plus the withdrawal-release timeout and injectable
/// error hooks.
pub(crate) struct UnitMockState {
    pub(crate) generate_address_requests: Mutex<Vec<MockUnitGenerateAddressRequest>>,
    /// Tracked by `protocol_address` — the fresh mock address returned from
    /// `/gen` that acts as either the deposit source or the withdrawal spotSend
    /// target. Destination-address queries can still return operation history
    /// across many generated protocol addresses.
    pub(crate) operations: Mutex<BTreeMap<String, Vec<MockUnitOperationEntry>>>,
    pub(crate) protocol_private_keys: Mutex<BTreeMap<String, MockUnitProtocolKey>>,
    pub(crate) evm_rpc_urls: BTreeMap<String, String>,
    /// When `Some`, the Unit mock acts as a real HL guardian client against the
    /// standalone Hyperliquid node at this base URL: deposits credit the user's
    /// spot ON THE NODE via a guardian `spotSend` instead of crediting the
    /// in-process [`core`](Self::core), and the withdrawal poller watches the
    /// node for the router's incoming spotSends. `None` keeps the in-process
    /// path (standalone venue tests rely on it).
    pub(crate) node_api_url: Option<String>,
    /// The node's HL guardian key (`HYPERLIQUID_GUARDIAN_PRIVATE_KEY`). Used to
    /// sign the deposit `spotSend` when [`node_api_url`](Self::node_api_url) is
    /// set. The guardian is seeded `u32::MAX` UBTC/UETH at node genesis.
    pub(crate) node_guardian_key: Option<[u8; 32]>,
    pub(crate) withdrawal_release_timeout: Duration,
    pub(crate) bitcoin_rpc_url: Option<String>,
    pub(crate) bitcoin_cookie_path: Option<PathBuf>,
    pub(crate) next_generate_address_error: Mutex<Option<String>>,
    pub(crate) next_withdrawal_completion_error: Mutex<Option<String>>,
    /// Cross-cutting infra the EVM withdrawal release needs (mock-service
    /// paymasters keyed by `(service, chain)`). Shared `Arc` instance.
    pub(crate) shared: Arc<crate::mock_integrators::SharedMockState>,
}

/// Builds the HyperUnit mock router. Mounted under `/hyperunit`; receives its
/// own [`UnitMockState`] substate at nest time.
pub(crate) fn router() -> Router<Arc<UnitMockState>> {
    Router::new()
        .route(
            "/gen/:src_chain/:dst_chain/:asset/:dst_addr",
            get(mock_unit_gen),
        )
        .route("/operations/:address", get(mock_unit_operations))
}

pub(crate) async fn maybe_spawn_unit_deposit_indexers(
    config: &MockIntegratorConfig,
    state: Arc<UnitMockState>,
) -> eyre::Result<(Vec<tokio::sync::oneshot::Sender<()>>, Vec<JoinHandle<()>>)> {
    let mut shutdowns = Vec::new();
    let mut handles = Vec::new();

    for (chain, rpc_url) in &config.unit_evm_rpc_urls {
        let Some(unit_chain) = UnitChain::parse(chain) else {
            tracing::warn!(chain, "mock Unit EVM indexer: unsupported Unit chain");
            continue;
        };
        let provider: DynProvider = ProviderBuilder::new().connect(rpc_url).await?.erased();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(run_unit_evm_deposit_indexer(
            provider,
            unit_chain,
            state.clone(),
            shutdown_rx,
        ));
        shutdowns.push(shutdown_tx);
        handles.push(handle);
    }

    if let (Some(rpc_url), Some(cookie_path)) = (
        config.unit_bitcoin_rpc_url.clone(),
        config.unit_bitcoin_cookie_path.clone(),
    ) {
        let client = BitcoinRpcClient::new(rpc_url, BitcoinRpcAuth::CookieFile(cookie_path))
            .await
            .map_err(|err| {
                eyre::eyre!("mock Unit bitcoin indexer: RPC client init failed: {err}")
            })?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(run_unit_bitcoin_deposit_indexer(
            client,
            state.clone(),
            shutdown_rx,
        ));
        shutdowns.push(shutdown_tx);
        handles.push(handle);
    }

    // When pointed at the node, the node's `spotSend`/`sendAsset` is Unit-unaware
    // (it only moves the spot balance, no Unit-withdrawal trigger). Spawn a
    // poller that watches the node for the router's incoming withdrawal
    // transfers and settles the matching Unit withdrawal via the native-release
    // logic. In-process mode (None) keeps the trigger inline in the spot mock.
    if let Some(node_api_url) = config.unit_node_api_url.clone() {
        let info = HyperliquidInfoClient::new(&node_api_url).map_err(|err| {
            eyre::eyre!("mock Unit node withdrawal poller: info client init failed: {err}")
        })?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(run_unit_node_withdrawal_poller(info, state, shutdown_rx));
        shutdowns.push(shutdown_tx);
        handles.push(handle);
    }

    Ok((shutdowns, handles))
}

/// A pending Unit withdrawal whose on-HL source transfer has not yet been
/// observed on the node. The poller watches `protocol_address`'s spot `coin`
/// balance and, once it reaches the router's transferred amount, settles the
/// withdrawal. The match key is the destination (protocol) address + the
/// credited amount.
#[derive(Debug, Clone)]
struct UnitNodeWithdrawalWatch {
    protocol_address: String,
    coin: &'static str,
}

/// Polls the node for incoming spotSends that settle pending Unit withdrawals.
/// The node's `spotSend`/`sendAsset` credits the withdrawal protocol address's
/// spot balance but knows nothing about Unit, so this poller is the
/// node-equivalent of the in-process spot mock's inline
/// [`complete_unit_withdrawal_after_hyperliquid_transfer`] trigger: for each
/// pending withdrawal it reads the protocol address's `userNonFundingLedger`
/// updates and, once an incoming `SpotTransfer` of the matching coin appears,
/// runs the existing native-release logic.
///
/// Crucially it threads the transfer's `user` (the HL account that signed the
/// router's `sendAsset` — the Hyperliquid custody vault) through as the
/// operation's `source_address`. The real HyperUnit `/operations` surfaces that
/// HL source, and the router's withdrawal observer matches the operation on it
/// (`expected_source == hyperliquid_custody_vault_address`). The in-process spot
/// mock learns the same address directly from the `sendAsset` signer; on the
/// node path the ledger update is the only place it survives. Using the
/// `spotClearinghouseState` balance instead would leave `source_address` unknown
/// (or wrong), so the observer would never match and the step would stall.
async fn run_unit_node_withdrawal_poller(
    info: HyperliquidInfoClient,
    state: Arc<UnitMockState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let poll = Duration::from_millis(250);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }

        for watch in active_unit_node_withdrawal_watches(&state).await {
            let Ok(protocol_address) = Address::from_str(&watch.protocol_address) else {
                continue;
            };
            let updates = match info
                .user_non_funding_ledger_updates(protocol_address, 0, None)
                .await
            {
                Ok(updates) => updates,
                Err(err) => {
                    tracing::warn!(
                        %err,
                        protocol_address = %watch.protocol_address,
                        "mock Unit node withdrawal poller: userNonFundingLedgerUpdates failed"
                    );
                    continue;
                }
            };
            // Find the router's incoming `sendAsset`/`spotSend` of the watched
            // coin INTO the protocol address. `user` is the HL source (custody
            // vault) that signed it; `amount` is exactly what the router sent.
            let Some((source_address, source_amount, nonce)) =
                updates.iter().rev().find_map(|update| match &update.delta {
                    UserNonFundingLedgerDelta::SpotTransfer {
                        token,
                        amount,
                        user,
                        destination,
                        nonce,
                        ..
                    } if token.eq_ignore_ascii_case(watch.coin)
                        && addresses_match(destination, &watch.protocol_address)
                        && amount.parse::<f64>().map(|v| v > 0.0).unwrap_or(false) =>
                    {
                        Some((user.clone(), amount.clone(), *nonce))
                    }
                    _ => None,
                })
            else {
                continue;
            };
            // Settle through the same native-release path the in-process spot
            // mock uses inline, threading the real HL source so the router's
            // withdrawal observer matches `expected_source`.
            if let Err(response) = complete_unit_withdrawal_after_hyperliquid_transfer(
                &state,
                &watch.protocol_address,
                source_address,
                &source_amount,
                format!("node-spotsend:{}:{nonce}", watch.protocol_address),
            )
            .await
            {
                tracing::warn!(
                    protocol_address = %watch.protocol_address,
                    status = %response.status(),
                    "mock Unit node withdrawal poller: settling withdrawal failed"
                );
            }
        }
    }
}

/// Collects pending Unit withdrawals whose on-HL source transfer has not yet
/// been observed on the node — i.e. withdrawal operations that are not terminal
/// and have no `source_amount` recorded. (Once observed,
/// [`complete_unit_withdrawal_after_hyperliquid_transfer`] sets `source_amount`
/// and advances the state, so they drop out of this set.)
async fn active_unit_node_withdrawal_watches(
    state: &Arc<UnitMockState>,
) -> Vec<UnitNodeWithdrawalWatch> {
    let operations = state.operations.lock().await;
    operations
        .values()
        .flat_map(|entries| entries.iter())
        .filter(|entry| {
            entry.kind == MockUnitOperationKind::Withdrawal
                && entry.src_chain == UnitChain::Hyperliquid
                && entry.operation.source_amount.is_none()
                && !matches!(entry.operation.state.as_deref(), Some("done" | "failure"))
        })
        .filter_map(|entry| {
            Some(UnitNodeWithdrawalWatch {
                protocol_address: entry.operation.protocol_address.clone()?,
                coin: hyperliquid_coin_for_unit_asset(entry.asset),
            })
        })
        .collect()
}

#[derive(Debug, Clone)]
struct UnitDepositWatch {
    protocol_address: String,
    min_amount_raw: Option<U256>,
}

async fn run_unit_evm_deposit_indexer(
    provider: DynProvider,
    unit_chain: UnitChain,
    state: Arc<UnitMockState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let poll = Duration::from_millis(100);
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }

        let watches = active_unit_deposit_watches(&state, unit_chain).await;
        for watch in watches {
            let Ok(address) = Address::from_str(&watch.protocol_address) else {
                continue;
            };
            let balance = match provider.get_balance(address).await {
                Ok(balance) => balance,
                Err(err) => {
                    tracing::warn!(
                        %err,
                        unit_chain = %unit_chain,
                        protocol_address = %watch.protocol_address,
                        "mock Unit EVM deposit indexer: balance lookup failed"
                    );
                    continue;
                }
            };
            if balance.is_zero() || watch.min_amount_raw.is_some_and(|amount| balance < amount) {
                continue;
            }
            if let Err(err) = complete_mock_unit_operation_with_observation(
                &state,
                &watch.protocol_address,
                Some(UnitOperationObservation {
                    source_amount: balance.to_string(),
                    source_tx_hash: None,
                }),
            )
            .await
            {
                tracing::warn!(
                    %err,
                    unit_chain = %unit_chain,
                    protocol_address = %watch.protocol_address,
                    "mock Unit EVM deposit indexer: completing operation failed"
                );
            }
        }
    }
}

async fn run_unit_bitcoin_deposit_indexer(
    client: BitcoinRpcClient,
    state: Arc<UnitMockState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let poll = Duration::from_millis(100);
    let mut last_scanned_block = match client.get_block_count().await {
        Ok(height) => height,
        Err(err) => {
            tracing::warn!(%err, "mock Unit bitcoin deposit indexer: getblockcount failed");
            0
        }
    };
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }

        let watches = active_unit_deposit_watches(&state, UnitChain::Bitcoin).await;
        if watches.is_empty() {
            continue;
        }
        scan_unit_bitcoin_mempool(&client, &state, &watches).await;

        let head = match client.get_block_count().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock Unit bitcoin deposit indexer: getblockcount failed");
                continue;
            }
        };
        if head <= last_scanned_block {
            continue;
        }

        for height in last_scanned_block.saturating_add(1)..=head {
            let block_hash = match client.get_block_hash(height).await {
                Ok(block_hash) => block_hash,
                Err(err) => {
                    tracing::warn!(%err, height, "mock Unit bitcoin deposit indexer: getblockhash failed");
                    continue;
                }
            };
            let block = match client.get_block(&block_hash).await {
                Ok(block) => block,
                Err(err) => {
                    tracing::warn!(%err, %block_hash, height, "mock Unit bitcoin deposit indexer: getblock failed");
                    continue;
                }
            };
            for tx in block.txdata {
                let txid = tx.compute_txid();
                process_unit_bitcoin_transaction_outputs(
                    &state,
                    &watches,
                    &txid.to_string(),
                    tx.output.into_iter().enumerate(),
                )
                .await;
            }
        }
        last_scanned_block = head;
    }
}

async fn scan_unit_bitcoin_mempool(
    client: &BitcoinRpcClient,
    state: &Arc<UnitMockState>,
    watches: &[UnitDepositWatch],
) {
    let mempool = match client.get_raw_mempool().await {
        Ok(mempool) => mempool,
        Err(err) => {
            tracing::warn!(%err, "mock Unit bitcoin deposit indexer: getrawmempool failed");
            return;
        }
    };
    for txid in mempool {
        let tx = match client.get_raw_transaction_verbose(&txid).await {
            Ok(tx) => tx,
            Err(err) => {
                tracing::warn!(%err, %txid, "mock Unit bitcoin deposit indexer: getrawtransaction failed");
                continue;
            }
        };
        let outputs: Vec<_> = tx
            .outputs
            .iter()
            .enumerate()
            .filter_map(|(vout, output)| match output.to_output() {
                Ok(output) => Some((vout, output)),
                Err(err) => {
                    tracing::warn!(
                        %err,
                        %txid,
                        vout,
                        "mock Unit bitcoin deposit indexer: decode output failed"
                    );
                    None
                }
            })
            .collect();
        process_unit_bitcoin_transaction_outputs(state, watches, &txid.to_string(), outputs).await;
    }
}

async fn process_unit_bitcoin_transaction_outputs<I>(
    state: &Arc<UnitMockState>,
    watches: &[UnitDepositWatch],
    txid: &str,
    outputs: I,
) where
    I: IntoIterator<Item = (usize, bitcoin::TxOut)>,
{
    for (vout, output) in outputs {
        for watch in watches {
            let Some(script_pubkey) =
                bitcoin_script_pubkey_for_regtest_address(&watch.protocol_address)
            else {
                continue;
            };
            if output.script_pubkey != script_pubkey {
                continue;
            }
            let amount_sats = output.value.to_sat();
            if amount_sats == 0
                || watch
                    .min_amount_raw
                    .is_some_and(|amount| U256::from(amount_sats) < amount)
            {
                continue;
            }
            let source_tx_hash = format!("{txid}:{vout}");
            if let Err(err) = complete_mock_unit_operation_with_observation(
                state,
                &watch.protocol_address,
                Some(UnitOperationObservation {
                    source_amount: amount_sats.to_string(),
                    source_tx_hash: Some(source_tx_hash),
                }),
            )
            .await
            {
                tracing::warn!(
                    %err,
                    protocol_address = %watch.protocol_address,
                    "mock Unit bitcoin deposit indexer: completing operation failed"
                );
            }
        }
    }
}

async fn active_unit_deposit_watches(
    state: &Arc<UnitMockState>,
    src_chain: UnitChain,
) -> Vec<UnitDepositWatch> {
    let operations = state.operations.lock().await;
    operations
        .values()
        .flat_map(|entries| entries.iter())
        .filter(|entry| {
            entry.kind == MockUnitOperationKind::Deposit
                && entry.src_chain == src_chain
                && !matches!(entry.operation.state.as_deref(), Some("done" | "failure"))
        })
        .filter_map(|entry| {
            let protocol_address = entry.operation.protocol_address.clone()?;
            let min_amount_raw = entry
                .operation
                .source_amount
                .as_deref()
                .and_then(|raw| U256::from_str_radix(raw, 10).ok());
            Some(UnitDepositWatch {
                protocol_address,
                min_amount_raw,
            })
        })
        .collect()
}

fn bitcoin_script_pubkey_for_regtest_address(address: &str) -> Option<bitcoin::ScriptBuf> {
    bitcoin::Address::from_str(address)
        .ok()?
        .require_network(bitcoin::Network::Regtest)
        .ok()
        .map(|address| address.script_pubkey())
}

/// Handles `GET /gen/{src_chain}/{dst_chain}/{asset}/{dst_addr}` — HyperUnit's
/// "generate protocol address" endpoint. The response mirrors
/// `hyperunit_client::UnitGenerateAddressResponse` byte-for-byte so production
/// and test code paths use the same parse logic.
///
/// The mock allocates a fresh protocol address and private key per request,
/// then records the generated operation in the state's `unit_operations` map,
/// seeded at `sourceTxDiscovered` — tests advance the state via
/// `complete_unit_operation` / `fail_unit_operation`.
pub(crate) async fn mock_unit_gen(
    State(state): State<Arc<UnitMockState>>,
    Path((src_chain, dst_chain, asset, dst_addr)): Path<(String, String, String, String)>,
) -> impl IntoResponse {
    if let Some(error) = state.next_generate_address_error.lock().await.take() {
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
    state.generate_address_requests.lock().await.push(request);

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

    let protocol_address = loop {
        let protocol_wallet = fresh_mock_protocol_wallet(src, dst);
        let mut private_keys = state.protocol_private_keys.lock().await;
        if private_keys.contains_key(&protocol_wallet.address) {
            continue;
        }
        let protocol_address = protocol_wallet.address;
        private_keys.insert(protocol_address.clone(), protocol_wallet.key);
        break protocol_address;
    };
    {
        let mut operations = state.operations.lock().await;
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
pub(crate) async fn mock_unit_operations(
    State(state): State<Arc<UnitMockState>>,
    Path(address): Path<String>,
) -> impl IntoResponse {
    let operations = state.operations.lock().await;
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

// NOTE: previously this file had mock handlers for
// `/v2/operations/deposit/{id}` and `/v2/operations/withdrawal/{id}`, but
// those endpoints are NOT documented by Hyperunit and probing the real API
// shows they return HTTP 200 with empty body for synthetic IDs — they
// don't actually serve operation data. All production callers route through
// `/operations/:address` (`mock_unit_operations` above), so the handlers
// and their helpers (`mock_unit_operation_by_id`,
// `unit_operation_matches_identifier`) have been removed.

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

pub(crate) async fn complete_unit_withdrawal_after_hyperliquid_transfer(
    state: &Arc<UnitMockState>,
    destination: &str,
    source_address: String,
    decimal_amount: &str,
    source_tx_hash: String,
) -> Result<(), axum::response::Response> {
    let mut unit_withdrawal_to_complete = None;
    {
        let mut operations = state.operations.lock().await;
        if let Some(entry) = operations
            .get_mut(destination)
            .and_then(|entries| latest_active_unit_operation_mut(entries))
        {
            let source_amount = match parse_unit_decimal_amount_to_raw(
                decimal_amount,
                unit_asset_decimals(entry.asset),
            ) {
                Ok(amount) => amount.to_string(),
                Err(err) => return Err(error_response(StatusCode::BAD_REQUEST, err)),
            };
            entry.visible = true;
            entry.operation.state = Some("waitForSrcTxFinalization".to_string());
            entry.operation.state_started_at = Some(Utc::now().to_rfc3339());
            entry.operation.state_updated_at = Some(Utc::now().to_rfc3339());
            entry.operation.source_address = Some(source_address);
            entry.operation.source_amount = Some(source_amount.clone());
            if entry.operation.source_tx_hash.is_none() {
                entry.operation.source_tx_hash = Some(source_tx_hash.clone());
            }
            if entry.operation.operation_id.is_none() {
                entry.operation.operation_id = entry.operation.source_tx_hash.clone();
            }
            if matches!(entry.kind, MockUnitOperationKind::Withdrawal) {
                unit_withdrawal_to_complete = Some((destination.to_string(), source_amount));
            }
        }
    }
    if let Some((protocol_address, source_amount)) = unit_withdrawal_to_complete {
        spawn_mock_unit_withdrawal_completion(
            Arc::clone(state),
            protocol_address,
            source_amount,
            source_tx_hash,
        );
    }
    Ok(())
}

fn spawn_mock_unit_withdrawal_completion(
    state: Arc<UnitMockState>,
    protocol_address: String,
    source_amount: String,
    source_tx_hash: String,
) {
    tokio::spawn(async move {
        let completion_result =
            if let Some(error) = state.next_withdrawal_completion_error.lock().await.take() {
                fail_mock_unit_operation(&state, &protocol_address, error).await
            } else {
                complete_mock_unit_operation_with_observation(
                    &state,
                    &protocol_address,
                    Some(UnitOperationObservation {
                        source_amount: source_amount.clone(),
                        source_tx_hash: Some(source_tx_hash),
                    }),
                )
                .await
            };
        if let Err(err) = completion_result {
            mark_mock_unit_operation_failed(&state, &protocol_address).await;
            tracing::warn!(
                target: "mock_hu",
                %protocol_address,
                %source_amount,
                %err,
                "mock Unit withdrawal completion failed after Hyperliquid source transfer; marked operation failed"
            );
        }
    });
}

/// Advance a tracked Unit operation to terminal `done`. Deposit operations
/// also credit the mock HL ledger and populate a synthetic source tx hash,
/// so downstream balance/history assertions still work once the real router
/// API no longer routes funds through `POST /unit/*` endpoints.
pub(crate) async fn complete_mock_unit_operation(
    state: &Arc<UnitMockState>,
    protocol_address: &str,
) -> Result<(), String> {
    complete_mock_unit_operation_with_observation(state, protocol_address, None).await
}

pub(crate) struct UnitOperationObservation {
    pub(crate) source_amount: String,
    pub(crate) source_tx_hash: Option<String>,
}

struct MockHyperunitDepositCredit {
    user: Address,
    coin: &'static str,
    /// The 8-decimal (`WIRE_DECIMALS`), half-to-even rounded credit amount — the
    /// exact value real Hyperliquid records for the deposit. The guardian
    /// `spotSend`s this string to the user on the node. `> 0` is the gate for
    /// actually issuing the credit.
    amount: String,
    /// The nonce stamped on the node-path guardian `spotSend`. Set to the trailing
    /// `:<n>` of the operation's `destination_tx_hash` so the resulting node
    /// `SpotTransfer` ledger update carries the SAME nonce the Unit deposit
    /// observer derives from `destination_tx_hash` — letting the observer match
    /// the credit.
    nonce: u64,
}

struct MockUnitEvmWithdrawalRelease {
    protocol_address: String,
    dst_chain: UnitChain,
    dst_addr: String,
    amount: String,
}

struct MockUnitBitcoinWithdrawalRelease {
    protocol_address: String,
    dst_addr: String,
    amount: String,
}

pub(crate) async fn complete_mock_unit_operation_with_observation(
    state: &Arc<UnitMockState>,
    protocol_address: &str,
    observation: Option<UnitOperationObservation>,
) -> Result<(), String> {
    let (maybe_deposit_credit, maybe_evm_withdrawal_release, maybe_bitcoin_withdrawal_release) = {
        tracing::info!(
            target: "mock_hu",
            %protocol_address,
            "acquiring unit_operations lock"
        );
        let mut operations = state.operations.lock().await;
        tracing::info!(
            target: "mock_hu",
            %protocol_address,
            "acquired unit_operations lock"
        );
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
        if let Some(observation) = observation {
            entry.operation.source_amount = Some(observation.source_amount);
            if let Some(source_tx_hash) = observation.source_tx_hash {
                entry.operation.source_tx_hash = Some(source_tx_hash);
            }
        }
        if entry.operation.source_amount.is_none() {
            return Err(format!(
                "mock unit operation {protocol_address} cannot complete without source_amount evidence"
            ));
        }
        if entry.operation.source_address.is_none()
            && matches!(entry.kind, MockUnitOperationKind::Deposit)
        {
            entry.operation.source_address = Some(entry.dst_addr.clone());
        }
        if entry.operation.source_tx_hash.is_none() {
            entry.operation.source_tx_hash = Some(default_mock_unit_source_tx_hash(entry));
        }
        let release_requires_real_destination_tx =
            matches!(entry.kind, MockUnitOperationKind::Withdrawal)
                && (matches!(entry.dst_chain, UnitChain::Ethereum | UnitChain::Base)
                    && matches!(entry.asset, UnitAsset::Eth)
                    || entry.dst_chain == UnitChain::Bitcoin && entry.asset == UnitAsset::Btc);
        if entry.operation.destination_tx_hash.is_none() && !release_requires_real_destination_tx {
            entry.operation.destination_tx_hash =
                Some(default_mock_unit_destination_tx_hash(entry));
        }
        if entry.operation.operation_id.is_none() {
            entry.operation.operation_id = entry.operation.source_tx_hash.clone();
        }
        if entry.operation.destination_fee_amount.is_none()
            || entry.operation.sweep_fee_amount.is_none()
        {
            let (destination_fee_amount, sweep_fee_amount) = default_mock_unit_fee_amounts();
            entry.operation.destination_fee_amount = Some(destination_fee_amount);
            entry.operation.sweep_fee_amount = Some(sweep_fee_amount);
        }
        entry.operation.source_tx_confirmations = None;
        entry.operation.destination_tx_confirmations = Some(MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS);
        let maybe_deposit_credit = match entry.kind {
            MockUnitOperationKind::Deposit if matches!(entry.dst_chain, UnitChain::Hyperliquid) => {
                let user = Address::from_str(&entry.dst_addr).map_err(|err| {
                    format!(
                        "mock unit operation {protocol_address} has invalid Hyperliquid destination {}: {err}",
                        entry.dst_addr
                    )
                })?;
                let coin = hyperliquid_coin_for_unit_asset(entry.asset);
                let amount = net_hyperunit_credit_amount(
                    entry.asset,
                    entry
                        .operation
                        .source_amount
                        .as_deref()
                        .expect("source_amount checked above"),
                    entry.operation.destination_fee_amount.as_deref(),
                    entry.operation.sweep_fee_amount.as_deref(),
                )?;
                // Derive the spotSend nonce from the operation's
                // `destination_tx_hash` (`"{user}:{nonce}"`), which is exactly
                // what sauron's Unit deposit observer parses as the expected
                // destination nonce. Aligning them lets the observer match the
                // node `SpotTransfer` this credit produces.
                let nonce = entry
                    .operation
                    .destination_tx_hash
                    .as_deref()
                    .and_then(|hash| hash.rsplit_once(':'))
                    .and_then(|(_, nonce)| nonce.parse::<u64>().ok())
                    .unwrap_or_else(|| Utc::now().timestamp_millis().max(0) as u64);
                Some(MockHyperunitDepositCredit {
                    user,
                    coin,
                    amount,
                    nonce,
                })
            }
            _ => None,
        };
        let maybe_evm_withdrawal_release = match entry.kind {
            MockUnitOperationKind::Withdrawal
                if matches!(entry.dst_chain, UnitChain::Ethereum | UnitChain::Base)
                    && matches!(entry.asset, UnitAsset::Eth) =>
            {
                let amount = entry.operation.source_amount.clone().ok_or_else(|| {
                    format!(
                        "mock unit operation {protocol_address} cannot release withdrawal without source_amount evidence"
                    )
                })?;
                Some(MockUnitEvmWithdrawalRelease {
                    protocol_address: protocol_address.to_string(),
                    dst_chain: entry.dst_chain,
                    dst_addr: entry.dst_addr.clone(),
                    amount,
                })
            }
            _ => None,
        };
        let maybe_bitcoin_withdrawal_release = match entry.kind {
            MockUnitOperationKind::Withdrawal
                if entry.dst_chain == UnitChain::Bitcoin && entry.asset == UnitAsset::Btc =>
            {
                let amount = entry.operation.source_amount.clone().ok_or_else(|| {
                    format!(
                        "mock unit operation {protocol_address} cannot release Bitcoin withdrawal without source_amount evidence"
                    )
                })?;
                Some(MockUnitBitcoinWithdrawalRelease {
                    protocol_address: protocol_address.to_string(),
                    dst_addr: entry.dst_addr.clone(),
                    amount,
                })
            }
            _ => None,
        };
        (
            maybe_deposit_credit,
            maybe_evm_withdrawal_release,
            maybe_bitcoin_withdrawal_release,
        )
    };

    if let Some(credit) = maybe_deposit_credit {
        let positive = credit
            .amount
            .parse::<f64>()
            .map(|v| v > 0.0)
            .unwrap_or(false);
        if positive {
            // The deposit credit lands on the standalone Hyperliquid node (the
            // sole Hyperliquid): the guardian — seeded `u32::MAX` UBTC/UETH at
            // node genesis — signs a real `spotSend(guardian -> user)` against
            // the node's `/exchange`. `node_api_url` is required.
            let node_api_url = state.node_api_url.as_deref().ok_or_else(|| {
                "mock Unit deposit: node_api_url is required (the node is the sole \
                 Hyperliquid); call MockIntegratorConfig::with_unit_node"
                    .to_string()
            })?;
            credit_deposit_via_node_guardian(state, node_api_url, &credit).await?;
        }
    }
    if let Some(release) = maybe_evm_withdrawal_release {
        let timeout = state.withdrawal_release_timeout;
        let tx_hash = match tokio::time::timeout(
            timeout,
            send_mock_unit_evm_withdrawal_release(state, &release),
        )
        .await
        {
            Ok(Ok(tx_hash)) => tx_hash,
            Ok(Err(error)) => {
                mark_mock_unit_operation_failed(state, &release.protocol_address).await;
                return Err(error);
            }
            Err(_) => {
                mark_mock_unit_operation_failed(state, &release.protocol_address).await;
                return Err(format!(
                    "mock Unit EVM withdrawal release timed out after {}",
                    format_duration_for_log(timeout)
                ));
            }
        };
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            %tx_hash,
            "setting destination_tx_hash"
        );
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            "acquiring unit_operations lock"
        );
        let mut operations = state.operations.lock().await;
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            "acquired unit_operations lock"
        );
        if let Some(entry) = operations
            .get_mut(&release.protocol_address)
            .and_then(|entries| entries.last_mut())
        {
            entry.operation.destination_tx_hash = Some(tx_hash.clone());
        }
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            %tx_hash,
            "set destination_tx_hash"
        );
    }
    if let Some(release) = maybe_bitcoin_withdrawal_release {
        let timeout = state.withdrawal_release_timeout;
        let tx_hash = match tokio::time::timeout(
            timeout,
            send_mock_unit_bitcoin_withdrawal_release(state, &release),
        )
        .await
        {
            Ok(Ok(tx_hash)) => tx_hash,
            Ok(Err(error)) => {
                mark_mock_unit_operation_failed(state, &release.protocol_address).await;
                return Err(error);
            }
            Err(_) => {
                mark_mock_unit_operation_failed(state, &release.protocol_address).await;
                return Err(format!(
                    "mock Unit Bitcoin withdrawal release timed out after {}",
                    format_duration_for_log(timeout)
                ));
            }
        };
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            %tx_hash,
            "setting destination_tx_hash"
        );
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            "acquiring unit_operations lock"
        );
        let mut operations = state.operations.lock().await;
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            "acquired unit_operations lock"
        );
        if let Some(entry) = operations
            .get_mut(&release.protocol_address)
            .and_then(|entries| entries.last_mut())
        {
            entry.operation.destination_tx_hash = Some(tx_hash.clone());
        }
        tracing::info!(
            target: "mock_hu",
            protocol_address = %release.protocol_address,
            %tx_hash,
            "set destination_tx_hash"
        );
    }

    Ok(())
}

fn format_duration_for_log(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        format!("{}s", duration.as_secs())
    } else {
        format!("{duration:?}")
    }
}

/// Credit a Unit deposit on the node by having the guardian sign a real
/// `spotSend(guardian -> user, coin, spot_amount)` against the node's
/// `/exchange`. This is the node-path replacement for the in-process
/// `hl.credit_spot`: the consumers now read the node's core, so the deposited
/// balance must land there. The guardian is seeded `u32::MAX` UBTC/UETH at node
/// genesis, so it never drains.
async fn credit_deposit_via_node_guardian(
    state: &Arc<UnitMockState>,
    node_api_url: &str,
    credit: &MockHyperunitDepositCredit,
) -> Result<(), String> {
    let guardian_key = state.node_guardian_key.ok_or_else(|| {
        "mock Unit deposit: node_api_url is set but the guardian key is missing".to_string()
    })?;
    let guardian = PrivateKeySigner::from_bytes(&guardian_key.into())
        .map_err(|err| format!("mock Unit deposit: invalid guardian key: {err}"))?;
    let exchange = HyperliquidExchangeClient::new(
        node_api_url,
        guardian,
        None,
        HyperliquidClientNetwork::Testnet,
    )
    .map_err(|err| format!("mock Unit deposit: node exchange client init failed: {err}"))?;
    // The node splits the token wire form on ':' and keys balances by the plain
    // symbol, so the token-id suffix is cosmetic; use the guardian-seeded coin.
    let token_wire = format!("{}:0x0000000000000000000000000000000000000000", credit.coin);
    // Use the operation's destination nonce as the spotSend nonce so the node's
    // resulting `SpotTransfer` ledger update matches what the Unit deposit
    // observer expects (see `MockHyperunitDepositCredit::nonce`).
    let response = exchange
        .spot_send(
            format!("{:#x}", credit.user),
            token_wire,
            credit.amount.clone(),
            credit.nonce,
        )
        .await
        .map_err(|err| format!("mock Unit deposit: node guardian spotSend failed: {err}"))?;
    if response.get("status").and_then(|status| status.as_str()) != Some("ok") {
        return Err(format!(
            "mock Unit deposit: node guardian spotSend rejected: {response}"
        ));
    }
    Ok(())
}

async fn mark_mock_unit_operation_failed(state: &Arc<UnitMockState>, protocol_address: &str) {
    let mut operations = state.operations.lock().await;
    if let Some(entry) = operations
        .get_mut(protocol_address)
        .and_then(|entries| entries.last_mut())
    {
        entry.visible = true;
        entry.operation.state = Some("failure".to_string());
        entry.operation.destination_tx_hash = None;
        entry.operation.state_updated_at = Some(Utc::now().to_rfc3339());
        entry.operation.state_next_attempt_at = None;
    }
}

async fn send_mock_unit_evm_withdrawal_release(
    state: &Arc<UnitMockState>,
    release: &MockUnitEvmWithdrawalRelease,
) -> Result<String, String> {
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        dst_chain = %release.dst_chain,
        dst_addr = %release.dst_addr,
        amount = %release.amount,
        "submitting mock Unit EVM withdrawal release"
    );
    let Some(rpc_url) = state
        .evm_rpc_urls
        .get(release.dst_chain.as_wire_str())
        .cloned()
    else {
        return Err(format!(
            "mock Unit withdrawal release has no EVM RPC URL for {}",
            release.dst_chain
        ));
    };
    let destination = Address::from_str(&release.dst_addr).map_err(|err| {
        format!(
            "mock Unit withdrawal release destination {:?} is invalid: {err}",
            release.dst_addr
        )
    })?;
    let amount = parse_unit_raw_amount(&release.amount)?;
    if amount == U256::ZERO {
        return Err(format!(
            "mock Unit withdrawal release amount {:?} is zero",
            release.amount
        ));
    }

    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        %rpc_url,
        "connecting EVM provider for mock Unit withdrawal release"
    );
    let provider = ProviderBuilder::new()
        .connect(&rpc_url)
        .await
        .map_err(|err| format!("mock Unit withdrawal release RPC init failed: {err}"))?;
    let chain_id = provider
        .get_chain_id()
        .await
        .map_err(|err| format!("mock Unit withdrawal release get_chain_id failed: {err}"))?;
    let handle = state
        .shared
        .mock_service_paymaster(MockService::Hyperunit, chain_id)?;
    let execution = Execution {
        target: destination,
        value: amount,
        callData: Bytes::new(),
    };
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        chain_id,
        "calling paymaster.submit"
    );
    let submit_result =
        tokio::time::timeout(state.withdrawal_release_timeout, handle.submit(execution)).await;
    let tx_hash = match submit_result {
        Ok(Ok(tx_hash)) => {
            tracing::info!(
                target: "mock_hu",
                protocol_address = %release.protocol_address,
                tx_hash = %format!("{tx_hash:#x}"),
                "paymaster.submit returned: ok"
            );
            tx_hash
        }
        Ok(Err(err)) => {
            tracing::info!(
                target: "mock_hu",
                protocol_address = %release.protocol_address,
                error = %err,
                "paymaster.submit returned: err"
            );
            return Err(format!("mock Unit withdrawal release failed: {err}"));
        }
        Err(_) => {
            tracing::info!(
                target: "mock_hu",
                protocol_address = %release.protocol_address,
                timeout = %format_duration_for_log(state.withdrawal_release_timeout),
                "paymaster.submit returned: err"
            );
            return Err(format!(
                "mock Unit withdrawal release paymaster.submit timed out after {}",
                format_duration_for_log(state.withdrawal_release_timeout)
            ));
        }
    };
    if MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS > 1 {
        provider
            .anvil_mine(Some(MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS - 1), None)
            .await
            .map_err(|err| {
                format!("mock Unit withdrawal release confirmation mining failed: {err}")
            })?;
    }

    Ok(format!("{tx_hash:#x}"))
}

async fn send_mock_unit_bitcoin_withdrawal_release(
    state: &Arc<UnitMockState>,
    release: &MockUnitBitcoinWithdrawalRelease,
) -> Result<String, String> {
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        dst_addr = %release.dst_addr,
        amount = %release.amount,
        "submitting mock Unit Bitcoin withdrawal release"
    );
    let (Some(rpc_url), Some(cookie_path)) = (
        state.bitcoin_rpc_url.clone(),
        state.bitcoin_cookie_path.clone(),
    ) else {
        return Err(
            "mock Unit Bitcoin withdrawal release has no Bitcoin RPC configuration".to_string(),
        );
    };
    let client = BitcoinRpcClient::new(rpc_url, BitcoinRpcAuth::CookieFile(cookie_path))
        .await
        .map_err(|err| format!("mock Unit Bitcoin withdrawal RPC init failed: {err}"))?;
    let destination = bitcoin::Address::from_str(&release.dst_addr)
        .map_err(|err| {
            format!(
                "mock Unit Bitcoin withdrawal destination {:?} is invalid: {err}",
                release.dst_addr
            )
        })?
        .require_network(bitcoin::Network::Regtest)
        .map_err(|err| {
            format!(
                "mock Unit Bitcoin withdrawal destination {:?} is not regtest: {err}",
                release.dst_addr
            )
        })?;
    let amount_sats = release.amount.parse::<u64>().map_err(|err| {
        format!(
            "mock Unit Bitcoin amount {:?} is invalid: {err}",
            release.amount
        )
    })?;
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        amount_sats,
        "calling Bitcoin send_to_address"
    );
    let sent = client
        .send_to_address(&destination, bitcoin::Amount::from_sat(amount_sats))
        .await
        .map_err(|err| format!("mock Unit Bitcoin withdrawal send failed: {err}"))?;
    let txid = sent
        .txid()
        .map_err(|err| format!("mock Unit Bitcoin withdrawal txid decode failed: {err}"))?;
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        %txid,
        "Bitcoin send_to_address returned: ok"
    );
    let miner = client
        .get_new_address(None, None)
        .await
        .map_err(|err| format!("mock Unit Bitcoin miner address allocation failed: {err}"))?
        .require_network(bitcoin::Network::Regtest)
        .map_err(|err| format!("mock Unit Bitcoin miner address network mismatch: {err}"))?;
    tracing::info!(
        target: "mock_hu",
        protocol_address = %release.protocol_address,
        "mining mock Unit Bitcoin withdrawal release"
    );
    client
        .generate_to_address(1, &miner)
        .await
        .map_err(|err| format!("mock Unit Bitcoin withdrawal mining failed: {err}"))?;
    Ok(txid.to_string())
}

fn parse_unit_decimal_amount_to_raw(value: &str, decimals: u8) -> Result<U256, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("mock Unit amount is empty".to_string());
    }
    let (whole, fractional) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty() || !whole.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!("mock Unit amount {value:?} has invalid whole part"));
    }
    if !fractional.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!(
            "mock Unit amount {value:?} has invalid fractional part"
        ));
    }
    let decimals = usize::from(decimals);
    if fractional.len() > decimals {
        return Err(format!(
            "mock Unit amount {value:?} has more than {decimals} decimal places"
        ));
    }

    let mut digits = String::with_capacity(whole.len() + decimals);
    digits.push_str(whole);
    digits.push_str(fractional);
    digits.extend(std::iter::repeat_n('0', decimals - fractional.len()));
    U256::from_str_radix(&digits, 10)
        .map_err(|err| format!("mock Unit amount {value:?} does not fit U256: {err}"))
}

fn parse_unit_raw_amount(value: &str) -> Result<U256, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("mock Unit raw amount is empty".to_string());
    }
    if !value.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!("mock Unit raw amount {value:?} is not digits"));
    }
    U256::from_str_radix(value, 10)
        .map_err(|err| format!("mock Unit raw amount {value:?} does not fit U256: {err}"))
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

fn default_mock_unit_fee_amounts() -> (String, String) {
    ("0".to_string(), "0".to_string())
}

fn net_hyperunit_credit_amount(
    asset: UnitAsset,
    source_amount: &str,
    destination_fee_amount: Option<&str>,
    sweep_fee_amount: Option<&str>,
) -> Result<String, String> {
    let net_raw =
        net_hyperunit_credit_raw(source_amount, destination_fee_amount, sweep_fee_amount)?;
    Ok(hyperunit_credit_amount_from_raw(asset, net_raw))
}

fn net_hyperunit_credit_raw(
    source_amount: &str,
    destination_fee_amount: Option<&str>,
    sweep_fee_amount: Option<&str>,
) -> Result<u128, String> {
    let source_amount = parse_required_raw_amount(source_amount, "source_amount")?;
    let destination_fee_amount =
        parse_optional_raw_amount(destination_fee_amount, "destination_fee_amount")?;
    let sweep_fee_amount = parse_optional_raw_amount(sweep_fee_amount, "sweep_fee_amount")?;
    Ok(source_amount
        .saturating_sub(destination_fee_amount)
        .saturating_sub(sweep_fee_amount))
}

fn unit_asset_decimals(asset: UnitAsset) -> u8 {
    match asset {
        UnitAsset::Btc => 8,
        UnitAsset::Eth => 18,
        _ => 18,
    }
}

fn hyperunit_credit_amount_from_raw(asset: UnitAsset, raw: u128) -> String {
    let decimal = raw_to_decimal(raw, unit_asset_decimals(asset));
    round_decimal_half_to_even(
        &decimal,
        usize::from(hyperliquid_client::wire::WIRE_DECIMALS),
    )
}

fn parse_optional_raw_amount(value: Option<&str>, field: &'static str) -> Result<u128, String> {
    let Some(value) = value else {
        return Ok(0);
    };
    parse_required_raw_amount(value, field)
}

fn parse_required_raw_amount(value: &str, field: &'static str) -> Result<u128, String> {
    value
        .parse::<u128>()
        .map_err(|err| format!("mock Unit {field} {value:?} is not a raw integer amount: {err}"))
}

fn raw_to_decimal(raw: u128, decimals: u8) -> String {
    let mut raw = raw.to_string();
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return raw;
    }
    if raw.len() <= decimals {
        let mut padded = "0".repeat(decimals + 1 - raw.len());
        padded.push_str(&raw);
        raw = padded;
    }
    let split = raw.len() - decimals;
    let whole = &raw[..split];
    let fraction = raw[split..].trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

fn round_decimal_half_to_even(value: &str, max_fraction: usize) -> String {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return value.to_string();
    }
    if fraction.len() <= max_fraction {
        return trim_decimal_fraction(value);
    }

    let (keep, drop) = fraction.split_at(max_fraction);
    let first_drop = drop.as_bytes()[0];
    let rest_drop = &drop[1..];
    let round_up = match first_drop {
        b'0'..=b'4' => false,
        b'6'..=b'9' => true,
        b'5' => {
            if rest_drop.bytes().all(|byte| byte == b'0') {
                let last_keep_digit = keep
                    .bytes()
                    .next_back()
                    .or_else(|| whole.bytes().next_back())
                    .unwrap_or(b'0')
                    - b'0';
                last_keep_digit % 2 == 1
            } else {
                true
            }
        }
        _ => return value.to_string(),
    };

    let rounded = if round_up {
        increment_decimal_string(whole, keep)
    } else if keep.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{keep}")
    };
    trim_decimal_fraction(&rounded)
}

fn increment_decimal_string(whole: &str, fraction: &str) -> String {
    let mut digits = whole.bytes().chain(fraction.bytes()).collect::<Vec<u8>>();
    let mut carry = 1;
    for digit in digits.iter_mut().rev() {
        if carry == 0 {
            break;
        }
        let value = (*digit - b'0') + carry;
        *digit = b'0' + (value % 10);
        carry = value / 10;
    }
    if carry > 0 {
        digits.insert(0, b'0' + carry);
    }

    let whole_len = whole.len() + usize::from(carry > 0);
    let new_whole = std::str::from_utf8(&digits[..whole_len]).unwrap_or("");
    let new_fraction = std::str::from_utf8(&digits[whole_len..]).unwrap_or("");
    if new_fraction.is_empty() {
        new_whole.to_string()
    } else {
        format!("{new_whole}.{new_fraction}")
    }
}

fn trim_decimal_fraction(value: &str) -> String {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if fraction.is_empty() {
        return if whole.is_empty() {
            "0".to_string()
        } else {
            whole.to_string()
        };
    }
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

fn hyperliquid_coin_for_unit_asset(asset: UnitAsset) -> &'static str {
    match asset {
        UnitAsset::Btc => "UBTC",
        UnitAsset::Eth => "UETH",
        _ => "UETH",
    }
}

/// Move a tracked Unit operation into terminal `failure`. Leaves the operation
/// in place so tests can still observe its final state via
/// `/operations/{protocol_address}`.
pub(crate) async fn fail_mock_unit_operation(
    state: &Arc<UnitMockState>,
    protocol_address: &str,
    error: String,
) -> Result<(), String> {
    let mut operations = state.operations.lock().await;
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

pub(crate) fn snapshot_unit_operations(
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

struct MockUnitProtocolWallet {
    address: String,
    key: MockUnitProtocolKey,
}

/// Fresh mock protocol address returned from `/gen`. Bitcoin deposits receive
/// a regtest BTC address; all other Unit flows use an EVM/Hyperliquid address.
fn fresh_mock_protocol_wallet(
    src_chain: UnitChain,
    dst_chain: UnitChain,
) -> MockUnitProtocolWallet {
    if src_chain == UnitChain::Bitcoin && dst_chain == UnitChain::Hyperliquid {
        return fresh_mock_regtest_bitcoin_wallet();
    }

    let signer = PrivateKeySigner::random();
    MockUnitProtocolWallet {
        address: format!("{:#x}", signer.address()),
        key: MockUnitProtocolKey {
            private_key: format!("0x{}", hex::encode(signer.to_bytes())),
        },
    }
}

fn fresh_mock_regtest_bitcoin_wallet() -> MockUnitProtocolWallet {
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let mut rng = bitcoin::key::rand::thread_rng();
    loop {
        let secret_key = bitcoin::secp256k1::SecretKey::new(&mut rng);
        let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
        let public_key = match bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key) {
            Ok(public_key) => public_key,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "generated mock Unit Bitcoin private key without compressed public key"
                );
                continue;
            }
        };
        return MockUnitProtocolWallet {
            address: bitcoin::Address::p2wpkh(&public_key, bitcoin::Network::Regtest).to_string(),
            key: MockUnitProtocolKey {
                private_key: private_key.to_wif(),
            },
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock_integrators::{
        MockIntegratorConfig, MockIntegratorServer, MockIntegratorState, MockServicePaymaster,
    };
    use alloy::primitives::B256;

    #[test]
    fn unit_protocol_key_debug_redacts_private_key() {
        let key = MockUnitProtocolKey {
            private_key: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
        };

        let rendered = format!("{key:?}");

        assert!(!rendered.contains("aaaaaaaa"));
        assert!(rendered.contains("redacted"));
    }

    #[test]
    fn mock_credit_amount_rounds_like_sauron_edge_cases() {
        struct Case {
            asset: UnitAsset,
            source_amount: &'static str,
            destination_fee_amount: Option<&'static str>,
            sweep_fee_amount: Option<&'static str>,
            expected: &'static str,
        }

        let cases = [
            Case {
                asset: UnitAsset::Eth,
                source_amount: "69613175000000000",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0.06961318",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "57507271000000000",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0.05750727",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "77158107666666666",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0.07715811",
            },
            Case {
                asset: UnitAsset::Btc,
                source_amount: "12345678",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0.12345678",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "5000000000",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "15000000000",
                destination_fee_amount: None,
                sweep_fee_amount: None,
                expected: "0.00000002",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "69613176000000000",
                destination_fee_amount: Some("1000000000"),
                sweep_fee_amount: None,
                expected: "0.06961318",
            },
            Case {
                asset: UnitAsset::Eth,
                source_amount: "1000",
                destination_fee_amount: Some("600"),
                sweep_fee_amount: Some("500"),
                expected: "0",
            },
        ];

        for case in cases {
            assert_eq!(
                net_hyperunit_credit_amount(
                    case.asset,
                    case.source_amount,
                    case.destination_fee_amount,
                    case.sweep_fee_amount
                )
                .as_deref(),
                Ok(case.expected),
                "unexpected rounded credit for {:?} source_amount={}",
                case.asset,
                case.source_amount
            );
        }
    }

    #[tokio::test]
    async fn unit_mock_generates_fresh_protocol_addresses_and_preserves_destination_history() {
        // Completing the deposit ops credits the user's spot on the node (the
        // sole Hyperliquid), so the Unit mock is pointed at a node guardian.
        let node = crate::hyperliquid_devnet::HyperliquidNode::spawn()
            .await
            .expect("spawn hyperliquid node");
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default().with_unit_node(node.url(), node.guardian_key()),
        )
        .await
        .expect("spawn");
        let destination = "0x33f65788aca48d733c2c2444ac9f79b18206aa92";
        let gen_url = format!(
            "{}/gen/ethereum/hyperliquid/eth/{}",
            server.hyperunit_url(),
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
        complete_mock_unit_operation_with_observation(
            &server.state.unit,
            &first.address,
            Some(UnitOperationObservation {
                source_amount: "1000000000000000000".to_string(),
                source_tx_hash: Some("0xunitfirst".to_string()),
            }),
        )
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
        assert_ne!(second.address.to_lowercase(), first.address.to_lowercase());
        assert!(server
            .unit_protocol_private_key(&first.address)
            .await
            .is_some());
        assert!(server
            .unit_protocol_private_key(&second.address)
            .await
            .is_some());

        let by_protocol_before: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.hyperunit_url(),
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

        complete_mock_unit_operation_with_observation(
            &server.state.unit,
            &second.address,
            Some(UnitOperationObservation {
                source_amount: "2000000000000000000".to_string(),
                source_tx_hash: Some("0xunitsecond".to_string()),
            }),
        )
        .await
        .expect("complete second op");

        let by_protocol_after: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.hyperunit_url(),
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
        assert_eq!(by_protocol_after.operations.len(), 1);
        assert!(by_protocol_after
            .operations
            .iter()
            .all(|op| op.matches_protocol_address(&first.address)));

        let by_destination: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.hyperunit_url(),
            destination
        ))
        .await
        .expect("destination ops")
        .error_for_status()
        .expect("destination ops 200")
        .json()
        .await
        .expect("destination ops json");
        assert_eq!(by_destination.addresses.len(), 2);
        assert_eq!(by_destination.operations.len(), 2);
        let addresses = by_destination
            .addresses
            .iter()
            .filter_map(|entry| entry["address"].as_str())
            .collect::<BTreeSet<_>>();
        assert!(addresses.contains(first.address.as_str()));
        assert!(addresses.contains(second.address.as_str()));
    }

    #[tokio::test]
    async fn unit_mock_refuses_completion_without_source_amount_evidence() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let destination = "0x33f65788aca48d733c2c2444ac9f79b18206aa92";
        let generated: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/ethereum/hyperliquid/eth/{}",
            server.hyperunit_url(),
            destination
        ))
        .await
        .expect("gen")
        .error_for_status()
        .expect("gen 200")
        .json()
        .await
        .expect("gen json");

        let error = server
            .complete_unit_operation(&generated.address)
            .await
            .expect_err("completion without amount evidence must fail");
        assert!(
            error.contains("source_amount evidence"),
            "unexpected error: {error}"
        );
    }

    /// The Unit deposit-credit amount computation (raw Unit source amount, less
    /// fees, scaled to the HL spot ledger amount) must round to the 8-decimal
    /// value sauron's deposit observer expects. This exercises
    /// `net_hyperunit_credit_amount` directly — the precision guard survives
    /// without an in-process Hyperliquid core.
    #[test]
    fn mock_credit_amount_matches_sauron_expectation() {
        let ledger_amount =
            net_hyperunit_credit_amount(UnitAsset::Eth, "69613175000000000", None, None)
                .expect("credit amount");
        assert_eq!(ledger_amount, "0.06961318");
    }

    #[tokio::test]
    async fn unit_mock_eth_withdrawal_releases_native_funds_with_real_destination_tx() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().prague().try_spawn().expect("anvil spawn");
        let recipient = Address::repeat_byte(0x77);
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_unit_evm_rpc_url(UnitChain::Base, anvil.endpoint())
                .with_mock_service_evm_chain(anvil.chain_id(), anvil.endpoint()),
        )
        .await
        .expect("spawn mock integrator");
        let generated: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/hyperliquid/base/eth/{recipient:#x}",
            server.hyperunit_url()
        ))
        .await
        .expect("gen request")
        .error_for_status()
        .expect("gen 200")
        .json()
        .await
        .expect("gen json");

        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        complete_mock_unit_operation_with_observation(
            &server.state.unit,
            &generated.address,
            Some(UnitOperationObservation {
                source_amount: "1250000000000000000".to_string(),
                source_tx_hash: Some("hl:oid:1".to_string()),
            }),
        )
        .await
        .expect("complete withdrawal");

        let balance = provider
            .get_balance(recipient)
            .await
            .expect("recipient balance");
        assert_eq!(balance, U256::from(1_250_000_000_000_000_000_u128));

        let operations: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.hyperunit_url(),
            generated.address
        ))
        .await
        .expect("operations request")
        .error_for_status()
        .expect("operations 200")
        .json()
        .await
        .expect("operations json");
        let tx_hash = operations.operations[0]
            .destination_tx_hash
            .as_deref()
            .expect("destination tx hash");
        assert!(tx_hash.starts_with("0x"));
        assert_eq!(tx_hash.len(), 66);
        assert_eq!(
            operations.operations[0].destination_tx_confirmations,
            Some(MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS)
        );

        let tx_hash = B256::from_str(tx_hash).expect("destination tx hash parses");
        let receipt = provider
            .get_transaction_receipt(tx_hash)
            .await
            .expect("receipt lookup")
            .expect("destination receipt exists");
        assert!(receipt.status());
        let tx_block = receipt.block_number.expect("receipt block number");
        let current_block = provider.get_block_number().await.expect("current block");
        assert_eq!(
            current_block - tx_block + 1,
            MOCK_UNIT_DESTINATION_TX_CONFIRMATIONS
        );
    }

    #[tokio::test]
    async fn unit_mock_eth_withdrawal_marks_failure_when_release_cannot_credit() {
        let recipient = Address::repeat_byte(0x88);
        let server = MockIntegratorServer::spawn()
            .await
            .expect("spawn mock integrator");
        let generated: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/hyperliquid/base/eth/{recipient:#x}",
            server.hyperunit_url()
        ))
        .await
        .expect("gen request")
        .error_for_status()
        .expect("gen 200")
        .json()
        .await
        .expect("gen json");

        let error = complete_mock_unit_operation_with_observation(
            &server.state.unit,
            &generated.address,
            Some(UnitOperationObservation {
                source_amount: "1250000000000000000".to_string(),
                source_tx_hash: Some("hl:oid:1".to_string()),
            }),
        )
        .await
        .expect_err("missing destination-chain RPC should fail release");
        assert!(error.contains("no EVM RPC URL"), "{error}");

        let operations: UnitOperationsResponse = reqwest::get(format!(
            "{}/operations/{}",
            server.hyperunit_url(),
            generated.address
        ))
        .await
        .expect("operations request")
        .error_for_status()
        .expect("operations 200")
        .json()
        .await
        .expect("operations json");

        assert_eq!(operations.operations.len(), 1);
        assert_eq!(operations.operations[0].state.as_deref(), Some("failure"));
        assert!(operations.operations[0].destination_tx_hash.is_none());
    }

    #[tokio::test]
    async fn unit_mock_eth_withdrawal_times_out_hung_paymaster_and_marks_failure() {
        use alloy::node_bindings::Anvil;

        let anvil = Anvil::new().prague().try_spawn().expect("anvil spawn");
        let chain_id = anvil.chain_id();
        let recipient = Address::repeat_byte(0x99);
        let mut paymasters = BTreeMap::new();
        paymasters.insert(
            (MockService::Hyperunit, chain_id),
            MockServicePaymaster::hanging(
                MockService::Hyperunit,
                chain_id,
                Duration::from_secs(10),
            ),
        );
        let server = MockIntegratorServer::spawn_with_config(
            MockIntegratorConfig::default()
                .with_unit_evm_rpc_url(UnitChain::Base, anvil.endpoint())
                .with_unit_withdrawal_release_timeout(Duration::from_millis(100))
                .with_mock_service_evm_chain(chain_id, anvil.endpoint())
                .with_mock_service_paymasters(paymasters),
        )
        .await
        .expect("spawn mock integrator");
        let generated: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/hyperliquid/base/eth/{recipient:#x}",
            server.hyperunit_url()
        ))
        .await
        .expect("gen request")
        .error_for_status()
        .expect("gen 200")
        .json()
        .await
        .expect("gen json");

        let started_at = std::time::Instant::now();
        let error = complete_mock_unit_operation_with_observation(
            &server.state.unit,
            &generated.address,
            Some(UnitOperationObservation {
                source_amount: "1250000000000000000".to_string(),
                source_tx_hash: Some("hl:oid:1".to_string()),
            }),
        )
        .await
        .expect_err("hung paymaster should time out");
        assert!(
            started_at.elapsed() < Duration::from_secs(2),
            "completion did not respect short timeout"
        );
        assert!(error.contains("timed out"), "{error}");

        let operations = server.unit_operations().await;
        let operation = operations
            .iter()
            .find(|operation| operation.protocol_address == generated.address)
            .expect("unit operation");
        assert_eq!(operation.state, "failure");
        assert!(operation.destination_tx_hash.is_none());
    }

    #[tokio::test]
    async fn unit_bitcoin_indexer_completes_confirmed_block_outputs() {
        // The deposit credit lands on the node (the sole Hyperliquid); point the
        // Unit mock at a node guardian so completion's spot credit succeeds.
        let node = crate::hyperliquid_devnet::HyperliquidNode::spawn()
            .await
            .expect("spawn hyperliquid node");
        let state = Arc::new(MockIntegratorState::new(
            &MockIntegratorConfig::default().with_unit_node(node.url(), node.guardian_key()),
        ));
        let protocol_wallet =
            fresh_mock_protocol_wallet(UnitChain::Bitcoin, UnitChain::Hyperliquid);
        let protocol_address = protocol_wallet.address;
        let destination = "0x33f65788aca48d733c2c2444ac9f79b18206aa92".to_string();
        {
            let mut operations = state.unit.operations.lock().await;
            operations.insert(
                protocol_address.clone(),
                vec![MockUnitOperationEntry {
                    kind: MockUnitOperationKind::Deposit,
                    src_chain: UnitChain::Bitcoin,
                    dst_chain: UnitChain::Hyperliquid,
                    asset: UnitAsset::Btc,
                    dst_addr: destination,
                    visible: false,
                    operation: UnitOperation {
                        operation_id: None,
                        op_created_at: Some(Utc::now().to_rfc3339()),
                        protocol_address: Some(protocol_address.clone()),
                        source_address: None,
                        destination_address: None,
                        source_chain: Some("bitcoin".to_string()),
                        destination_chain: Some("hyperliquid".to_string()),
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
                        asset: Some("btc".to_string()),
                        state_started_at: Some(Utc::now().to_rfc3339()),
                        state_updated_at: Some(Utc::now().to_rfc3339()),
                        state_next_attempt_at: None,
                    },
                }],
            );
        }
        let script_pubkey = bitcoin_script_pubkey_for_regtest_address(&protocol_address)
            .expect("protocol address script pubkey");
        let tx_out = bitcoin::TxOut {
            value: bitcoin::Amount::from_sat(50_000),
            script_pubkey,
        };
        let watches = vec![UnitDepositWatch {
            protocol_address: protocol_address.clone(),
            min_amount_raw: Some(U256::from(50_000)),
        }];

        process_unit_bitcoin_transaction_outputs(
            &state.unit,
            &watches,
            "1234",
            vec![(2usize, tx_out)],
        )
        .await;

        let operations = state.unit.operations.lock().await;
        let entry = operations
            .get(&protocol_address)
            .and_then(|entries| entries.first())
            .expect("unit operation");
        assert!(entry.visible);
        assert_eq!(entry.operation.state.as_deref(), Some("done"));
        assert_eq!(entry.operation.source_amount.as_deref(), Some("50000"));
        assert_eq!(entry.operation.source_tx_hash.as_deref(), Some("1234:2"));
    }

    #[tokio::test]
    async fn unit_mock_withdrawal_gen_uses_partial_signature_set() {
        let server = MockIntegratorServer::spawn().await.expect("spawn");
        let body: UnitGenerateAddressResponse = reqwest::get(format!(
            "{}/gen/hyperliquid/ethereum/eth/0x33f65788aca48d733c2c2444ac9f79b18206aa92",
            server.hyperunit_url()
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
}
