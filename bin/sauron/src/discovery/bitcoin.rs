use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use bitcoin::{
    base64::{engine::general_purpose::STANDARD, Engine},
    consensus::deserialize,
    hashes::Hash,
    hex::FromHex,
    Address, Block, BlockHash, ScriptBuf, Transaction, Txid,
};
use bitcoincore_rpc_async::{jsonrpc, Client as BitcoinRpcClient, RpcApi};
use chrono::Utc;
use reqwest::Url;
use router_primitives::{ChainType, TokenIdentifier};
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use tokio::{
    sync::{Mutex, RwLock},
    task::JoinSet,
    time::sleep,
};
use tracing::{info, warn};
use zeromq::{Socket, SocketRecv, ZmqMessage};

use crate::{
    config::SauronArgs,
    discovery::{
        BlockCursor, BlockScan, DepositConfirmationState, DetectedDeposit, DiscoveryBackend,
    },
    error::{BitcoinEsploraSnafu, BitcoinRpcSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry, WatchTarget},
};

const BITCOIN_REORG_RESCAN_DEPTH: u64 = 6;
const BITCOIN_ZMQ_RECONNECT_DELAY: Duration = Duration::from_secs(5);
const BITCOIN_RPC_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const BITCOIN_RPC_MAX_RESPONSE_BODY_BYTES: usize = 256 * 1024;
const BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES: usize = 2 * 1024;
const MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE: usize = 10_000;

pub struct BitcoinDiscoveryBackend {
    esplora_client: esplora_client::AsyncClient,
    rpc_client: Arc<BitcoinRpcClient>,
    mempool_support: Arc<BitcoinMempoolSupport>,
    mempool_listener_tasks: Mutex<JoinSet<&'static str>>,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

impl BitcoinDiscoveryBackend {
    pub async fn new(args: &SauronArgs) -> Result<Self> {
        let electrum_http_server_url = args.electrum_http_server_url.trim_end_matches('/');
        let esplora_client = esplora_client::Builder::new(electrum_http_server_url)
            .build_async()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: "bitcoin".to_string(),
                message: error.to_string(),
            })?;

        let rpc_transport =
            ReqwestRpcTransport::new(&args.bitcoin_rpc_url, args.bitcoin_rpc_auth.clone())
                .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                    backend: "bitcoin".to_string(),
                    message: error,
                })?;
        let jsonrpc_client = jsonrpc::Client::with_transport(rpc_transport);
        let rpc_client = Arc::new(BitcoinRpcClient::from_jsonrpc(jsonrpc_client));

        info!("Bitcoin Core RPC support enabled for Sauron block scanning");
        info!(
            rawtx_endpoint = %args.bitcoin_zmq_rawtx_endpoint,
            sequence_endpoint = %args.bitcoin_zmq_sequence_endpoint,
            "Bitcoin Core ZMQ mempool support enabled for Sauron"
        );
        let mempool_support = BitcoinMempoolSupport::new();
        let mut mempool_listener_tasks = JoinSet::new();
        let rawtx_endpoint = args.bitcoin_zmq_rawtx_endpoint.clone();
        let rawtx_support = mempool_support.clone();
        mempool_listener_tasks.spawn(async move {
            run_rawtx_listener(rawtx_endpoint, rawtx_support).await;
            "rawtx"
        });
        let sequence_endpoint = args.bitcoin_zmq_sequence_endpoint.clone();
        let sequence_support = mempool_support.clone();
        mempool_listener_tasks.spawn(async move {
            run_sequence_listener(sequence_endpoint, sequence_support).await;
            "sequence"
        });

        Ok(Self {
            esplora_client,
            rpc_client,
            mempool_support,
            mempool_listener_tasks: Mutex::new(mempool_listener_tasks),
            poll_interval: Duration::from_secs(args.sauron_bitcoin_scan_interval_seconds),
            indexed_lookup_concurrency: args.sauron_bitcoin_indexed_lookup_concurrency,
        })
    }

    fn script_map(
        &self,
        watches: &[SharedWatchEntry],
    ) -> HashMap<ScriptBuf, Vec<SharedWatchEntry>> {
        let mut scripts: HashMap<ScriptBuf, Vec<SharedWatchEntry>> = HashMap::new();

        for watch in watches {
            let parsed = Address::from_str(&watch.address)
                .map(|address| address.assume_checked().script_pubkey());
            match parsed {
                Ok(script) => scripts.entry(script).or_default().push(watch.clone()),
                Err(error) => {
                    warn!(
                        watch_id = %watch.watch_id,
                        address = %watch.address,
                        %error,
                        "Skipping invalid Bitcoin watch address"
                    );
                }
            }
        }

        scripts
    }

    async fn current_tip_height(&self) -> Result<u64> {
        match self.rpc_client.get_block_count().await {
            Ok(height) => return Ok(height),
            Err(error) => {
                warn!(
                    %error,
                    "Bitcoin RPC tip-height lookup failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_height()
            .await
            .map(u64::from)
            .context(BitcoinEsploraSnafu)
    }

    async fn current_tip_hash(&self) -> Result<BlockHash> {
        match self.rpc_client.get_best_block_hash().await {
            Ok(hash) => return Ok(hash),
            Err(error) => {
                warn!(
                    %error,
                    "Bitcoin RPC tip-hash lookup failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_tip_hash()
            .await
            .context(BitcoinEsploraSnafu)
    }

    async fn block_hash_at_height(&self, height: u64) -> Result<BlockHash> {
        match self.rpc_client.get_block_hash(height).await {
            Ok(hash) => return Ok(hash),
            Err(error) => {
                warn!(
                    height,
                    %error,
                    "Bitcoin RPC block-hash lookup failed; falling back to Esplora"
                );
            }
        }

        let height = u32::try_from(height).map_err(|_| crate::error::Error::ChainInit {
            chain: ChainType::Bitcoin.to_db_string().to_string(),
            message: format!("block height {height} exceeded u32 range"),
        })?;

        self.esplora_client
            .get_block_hash(height)
            .await
            .context(BitcoinEsploraSnafu)
    }

    async fn block_by_hash(&self, block_hash: &BlockHash) -> Result<Block> {
        match self.rpc_client.get_block(block_hash).await {
            Ok(block) => return Ok(block),
            Err(error) => {
                warn!(
                    %block_hash,
                    %error,
                    "Bitcoin RPC block fetch failed; falling back to Esplora"
                );
            }
        }

        self.esplora_client
            .get_block_by_hash(block_hash)
            .await
            .context(BitcoinEsploraSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: ChainType::Bitcoin.to_db_string().to_string(),
                message: format!("block {block_hash} was unavailable from Esplora"),
            })
    }

    async fn resync_mempool_watches_from_esplora(
        &self,
        watches: &[SharedWatchEntry],
    ) -> Vec<DetectedDeposit> {
        let mut detections = Vec::new();

        for watch in watches {
            match self.indexed_lookup(watch.as_ref()).await {
                Ok(Some(detected)) => detections.push(detected),
                Ok(None) => {}
                Err(error) => {
                    warn!(
                        watch_id = %watch.watch_id,
                        %error,
                        "Bitcoin mempool resync lookup failed for active watch"
                    );
                }
            }
        }

        detections
    }

    async fn enrich_sender_addresses(&self, detections: &mut [DetectedDeposit]) {
        for detected in detections {
            if !detected.sender_addresses.is_empty() {
                continue;
            }

            match self
                .sender_addresses_for_transaction(&detected.tx_hash, &detected.address)
                .await
            {
                Ok(sender_addresses) => detected.sender_addresses = sender_addresses,
                Err(error) => {
                    warn!(
                        tx_hash = %detected.tx_hash,
                        watch_id = %detected.watch_id,
                        %error,
                        "Bitcoin sender-address enrichment failed"
                    );
                }
            }
        }
    }

    async fn sender_addresses_for_transaction(
        &self,
        tx_hash: &str,
        recipient_address: &str,
    ) -> Result<Vec<String>> {
        let txid =
            Txid::from_str(tx_hash).map_err(|error| crate::error::Error::InvalidWatchRow {
                message: format!("invalid Bitcoin transaction hash {tx_hash}: {error}"),
            })?;
        let Some(tx) = self
            .esplora_client
            .get_tx_info(&txid)
            .await
            .context(BitcoinEsploraSnafu)?
        else {
            return Ok(Vec::new());
        };

        Ok(sender_addresses_from_esplora_tx(
            &tx,
            bitcoin_network_for_address(recipient_address),
        ))
    }

    async fn drain_mempool_detections(
        &self,
        watches: &[SharedWatchEntry],
        current_height: u64,
    ) -> Vec<DetectedDeposit> {
        let resync_requested = self.mempool_support.take_resync_request();
        let snapshot_height = self.mempool_support.current_snapshot_height().await;
        let snapshot_outdated = snapshot_height != Some(current_height);
        let mut detections = Vec::new();

        if resync_requested || snapshot_outdated {
            if let Err(error) = self
                .mempool_support
                .sync_from_rpc(&self.rpc_client, current_height)
                .await
            {
                warn!(
                    current_height,
                    %error,
                    "Bitcoin mempool RPC sync failed; falling back to Esplora watch resync"
                );
                detections.extend(self.resync_mempool_watches_from_esplora(watches).await);
            }
        }

        detections.extend(self.mempool_support.drain_detection_queue().await);
        detections
    }

    async fn ensure_mempool_listeners_alive(&self) -> Result<()> {
        let mut tasks = self.mempool_listener_tasks.lock().await;
        let Some(join_result) = tasks.try_join_next() else {
            return Ok(());
        };

        match join_result {
            Ok(listener) => Err(crate::error::Error::ChainInit {
                chain: ChainType::Bitcoin.to_db_string().to_string(),
                message: format!("Bitcoin {listener} ZMQ listener exited unexpectedly"),
            }),
            Err(error) => Err(crate::error::Error::ChainInit {
                chain: ChainType::Bitcoin.to_db_string().to_string(),
                message: format!("Bitcoin ZMQ listener panicked or was cancelled: {error}"),
            }),
        }
    }
}

#[async_trait]
impl DiscoveryBackend for BitcoinDiscoveryBackend {
    fn name(&self) -> &'static str {
        "bitcoin"
    }

    fn chain(&self) -> ChainType {
        ChainType::Bitcoin
    }

    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    fn indexed_lookup_concurrency(&self) -> usize {
        self.indexed_lookup_concurrency
    }

    async fn sync_watches(&self, watches: &[SharedWatchEntry]) -> Result<()> {
        self.mempool_support
            .replace_watches(self.script_map(watches))
            .await;
        Ok(())
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        if watch.source_token != TokenIdentifier::Native {
            return Ok(None);
        }

        let address = Address::from_str(&watch.address)
            .map_err(|error| crate::error::Error::InvalidWatchRow {
                message: format!(
                    "Bitcoin watch {} had invalid address {}: {}",
                    watch.watch_id, watch.address, error
                ),
            })?
            .assume_checked();

        let utxos = self
            .esplora_client
            .get_address_utxo(&address)
            .await
            .context(BitcoinEsploraSnafu)?;
        let current_height = self.current_tip_height().await?;

        let mut best_match: Option<(DetectedDeposit, u64)> = None;

        for utxo in utxos {
            let amount = alloy::primitives::U256::from(utxo.value);
            if amount < watch.min_amount || amount > watch.max_amount {
                continue;
            }

            let Some(confirmation) = bitcoin_utxo_confirmation_metadata(
                utxo.status.confirmed,
                utxo.status.block_height.map(u64::from),
                utxo.status.block_hash.as_ref().map(ToString::to_string),
                current_height,
            ) else {
                warn!(
                    txid = %utxo.txid,
                    confirmed = utxo.status.confirmed,
                    block_height = ?utxo.status.block_height,
                    current_height,
                    "Skipping Bitcoin UTXO with inconsistent confirmation metadata"
                );
                continue;
            };

            let candidate = DetectedDeposit {
                watch_target: watch.watch_target,
                watch_id: watch.watch_id,
                source_chain: ChainType::Bitcoin,
                source_token: TokenIdentifier::Native,
                address: watch.address.clone(),
                sender_addresses: Vec::new(),
                tx_hash: utxo.txid.to_string(),
                transfer_index: utxo.vout as u64,
                amount,
                confirmation_state: confirmation.state,
                block_height: confirmation.block_height,
                block_hash: confirmation.block_hash,
                observed_at: Utc::now(),
                indexer_candidate_id: None,
            };

            if best_match.as_ref().is_none_or(|(_, best_confirmations)| {
                confirmation.confirmations > *best_confirmations
            }) {
                best_match = Some((candidate, confirmation.confirmations));
            }
        }

        let Some((mut candidate, _)) = best_match else {
            return Ok(None);
        };
        self.enrich_sender_addresses(std::slice::from_mut(&mut candidate))
            .await;
        Ok(Some(candidate))
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        let height = self.current_tip_height().await?;
        let hash = self.current_tip_hash().await?;
        Ok(BlockCursor {
            height,
            hash: hash.to_string(),
        })
    }

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        self.ensure_mempool_listeners_alive().await?;
        let current_height = self.current_tip_height().await?;
        let current_tip_hash = self.current_tip_hash().await?;
        let mut detections = self.drain_mempool_detections(watches, current_height).await;

        if watches.is_empty() || current_height <= from_exclusive.height {
            self.enrich_sender_addresses(&mut detections).await;
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: current_height,
                    hash: current_tip_hash.to_string(),
                },
                detections,
            });
        }

        if from_exclusive.height > 0 {
            let expected_hash = self.block_hash_at_height(from_exclusive.height).await?;
            if expected_hash.to_string() != from_exclusive.hash {
                let rewind_height = from_exclusive
                    .height
                    .saturating_sub(BITCOIN_REORG_RESCAN_DEPTH);
                let rewind_hash = if rewind_height == 0 {
                    String::new()
                } else {
                    self.block_hash_at_height(rewind_height).await?.to_string()
                };
                warn!(
                    stored_height = from_exclusive.height,
                    stored_hash = %from_exclusive.hash,
                    current_hash = %expected_hash,
                    rewind_height,
                    "Bitcoin discovery cursor reorg detected; rewinding cursor"
                );
                self.enrich_sender_addresses(&mut detections).await;
                return Ok(BlockScan {
                    new_cursor: BlockCursor {
                        height: rewind_height,
                        hash: rewind_hash,
                    },
                    detections,
                });
            }
        }

        let scripts = self.script_map(watches);
        let mut last_hash = from_exclusive.hash.clone();

        for height in (from_exclusive.height + 1)..=current_height {
            let block_hash = self.block_hash_at_height(height).await?;
            let block = self.block_by_hash(&block_hash).await?;
            last_hash = block_hash.to_string();
            detections.extend(match_transaction_against_scripts(
                &txs_from_block(block),
                &scripts,
                height,
                &block_hash.to_string(),
            ));
        }

        self.enrich_sender_addresses(&mut detections).await;
        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: current_height,
                hash: last_hash,
            },
            detections,
        })
    }
}

fn txs_from_block(block: Block) -> Vec<Transaction> {
    block.txdata
}

fn match_transaction_against_scripts(
    transactions: &[Transaction],
    scripts: &HashMap<ScriptBuf, Vec<SharedWatchEntry>>,
    block_height: u64,
    block_hash: &str,
) -> Vec<DetectedDeposit> {
    let mut detections = Vec::new();

    for tx in transactions {
        detections.extend(match_single_transaction(
            tx,
            scripts,
            Utc::now(),
            DepositConfirmationState::Confirmed,
            Some(block_height),
            Some(block_hash.to_string()),
        ));
    }

    detections
}

fn match_single_transaction(
    tx: &Transaction,
    scripts: &HashMap<ScriptBuf, Vec<SharedWatchEntry>>,
    observed_at: chrono::DateTime<chrono::Utc>,
    confirmation_state: DepositConfirmationState,
    block_height: Option<u64>,
    block_hash: Option<String>,
) -> Vec<DetectedDeposit> {
    let tx_hash = tx.compute_txid().to_string();
    let mut detections = Vec::new();

    for (vout, output) in tx.output.iter().enumerate() {
        let Some(candidates) = scripts.get(&output.script_pubkey) else {
            continue;
        };

        let amount = alloy::primitives::U256::from(output.value.to_sat());
        for watch in candidates {
            if amount < watch.min_amount || amount > watch.max_amount {
                continue;
            }

            detections.push(DetectedDeposit {
                watch_target: watch.watch_target,
                watch_id: watch.watch_id,
                source_chain: ChainType::Bitcoin,
                source_token: TokenIdentifier::Native,
                address: watch.address.clone(),
                sender_addresses: Vec::new(),
                tx_hash: tx_hash.clone(),
                transfer_index: vout as u64,
                amount,
                confirmation_state,
                block_height,
                block_hash: block_hash.clone(),
                observed_at,
                indexer_candidate_id: None,
            });
        }
    }

    detections
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BitcoinUtxoConfirmationMetadata {
    state: DepositConfirmationState,
    confirmations: u64,
    block_height: Option<u64>,
    block_hash: Option<String>,
}

fn bitcoin_utxo_confirmation_metadata(
    confirmed: bool,
    block_height: Option<u64>,
    block_hash: Option<String>,
    current_height: u64,
) -> Option<BitcoinUtxoConfirmationMetadata> {
    if !confirmed {
        return Some(BitcoinUtxoConfirmationMetadata {
            state: DepositConfirmationState::Mempool,
            confirmations: 0,
            block_height: None,
            block_hash: None,
        });
    }

    let block_height = block_height?;
    let confirmations = current_height.checked_sub(block_height)?.checked_add(1)?;
    Some(BitcoinUtxoConfirmationMetadata {
        state: DepositConfirmationState::Confirmed,
        confirmations,
        block_height: Some(block_height),
        block_hash,
    })
}

fn sender_addresses_from_esplora_tx(
    tx: &esplora_client::api::Tx,
    network: bitcoin::Network,
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut addresses = Vec::new();

    for input in &tx.vin {
        let Some(prevout) = input.prevout.as_ref() else {
            continue;
        };
        let Ok(address) = Address::from_script(&prevout.scriptpubkey, network) else {
            continue;
        };
        let address = address.to_string();
        if seen.insert(address.clone()) {
            addresses.push(address);
        }
    }

    addresses
}

fn bitcoin_network_for_address(address: &str) -> bitcoin::Network {
    let normalized = address.to_ascii_lowercase();
    if normalized.starts_with("bc1") || normalized.starts_with('1') || normalized.starts_with('3') {
        bitcoin::Network::Bitcoin
    } else if normalized.starts_with("bcrt") {
        bitcoin::Network::Regtest
    } else {
        bitcoin::Network::Testnet
    }
}

struct BitcoinMempoolSupport {
    watches_by_script: RwLock<HashMap<ScriptBuf, Vec<SharedWatchEntry>>>,
    detection_queue: Mutex<VecDeque<DetectedDeposit>>,
    mempool_transactions: Mutex<HashMap<Txid, Transaction>>,
    matched_detections: Mutex<HashSet<MempoolDetectionKey>>,
    rawtx_message_sequence: Mutex<Option<u32>>,
    mempool_sequence: Mutex<Option<u64>>,
    snapshot_height: Mutex<Option<u64>>,
    resync_requested: AtomicBool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct MempoolDetectionKey {
    watch_target: WatchTarget,
    watch_id: uuid::Uuid,
    txid: Txid,
    vout: u64,
}

impl BitcoinMempoolSupport {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            watches_by_script: RwLock::new(HashMap::new()),
            detection_queue: Mutex::new(VecDeque::new()),
            mempool_transactions: Mutex::new(HashMap::new()),
            matched_detections: Mutex::new(HashSet::new()),
            rawtx_message_sequence: Mutex::new(None),
            mempool_sequence: Mutex::new(None),
            snapshot_height: Mutex::new(None),
            resync_requested: AtomicBool::new(false),
        })
    }

    async fn replace_watches(&self, watches_by_script: HashMap<ScriptBuf, Vec<SharedWatchEntry>>) {
        *self.watches_by_script.write().await = watches_by_script;
        self.prune_stale_detections().await;
        self.queue_detections_from_snapshot().await;
    }

    async fn drain_detection_queue(&self) -> Vec<DetectedDeposit> {
        self.detection_queue.lock().await.drain(..).collect()
    }

    fn request_resync(&self) {
        self.resync_requested.store(true, Ordering::Release);
    }

    fn take_resync_request(&self) -> bool {
        self.resync_requested.swap(false, Ordering::AcqRel)
    }

    async fn current_snapshot_height(&self) -> Option<u64> {
        *self.snapshot_height.lock().await
    }

    async fn sync_from_rpc(
        &self,
        rpc_client: &BitcoinRpcClient,
        current_height: u64,
    ) -> Result<()> {
        let mempool_txids = rpc_client
            .get_raw_mempool()
            .await
            .context(BitcoinRpcSnafu)?;
        let expected_txids = mempool_txids.into_iter().collect::<HashSet<_>>();

        let removed_txids = {
            let mut transactions = self.mempool_transactions.lock().await;
            let removed = transactions
                .keys()
                .filter(|txid| !expected_txids.contains(*txid))
                .copied()
                .collect::<Vec<_>>();
            transactions.retain(|txid, _| expected_txids.contains(txid));
            removed
        };

        if !removed_txids.is_empty() {
            let mut matched_detections = self.matched_detections.lock().await;
            for txid in removed_txids {
                matched_detections.retain(|key| key.txid != txid);
            }
        }

        let missing_txids = {
            let transactions = self.mempool_transactions.lock().await;
            expected_txids
                .iter()
                .filter(|txid| !transactions.contains_key(*txid))
                .copied()
                .collect::<Vec<_>>()
        };

        if !missing_txids.is_empty() {
            let mut fetched_transactions = Vec::with_capacity(missing_txids.len());
            for txid in &missing_txids {
                let tx_bytes = Vec::<u8>::from_hex(
                    &rpc_client
                        .get_raw_transaction_hex(txid, None)
                        .await
                        .context(BitcoinRpcSnafu)?,
                )
                .map_err(|error| crate::error::Error::ChainInit {
                    chain: ChainType::Bitcoin.to_db_string().to_string(),
                    message: format!(
                        "failed to decode Bitcoin mempool snapshot transaction {txid}: {error}"
                    ),
                })?;
                fetched_transactions.push(
                    deserialize::<Transaction>(&tx_bytes).map_err(|error| {
                        crate::error::Error::ChainInit {
                            chain: ChainType::Bitcoin.to_db_string().to_string(),
                            message: format!(
                                "failed to deserialize Bitcoin mempool snapshot transaction {txid}: {error}"
                            ),
                        }
                    })?,
                );
            }

            let mut transactions = self.mempool_transactions.lock().await;
            for tx in fetched_transactions {
                transactions.insert(tx.compute_txid(), tx);
            }
        }

        *self.snapshot_height.lock().await = Some(current_height);
        self.prune_stale_detections().await;
        self.queue_detections_from_snapshot().await;
        Ok(())
    }

    async fn observe_rawtx_sequence(&self, sequence: u32) {
        let mut previous = self.rawtx_message_sequence.lock().await;
        if let Some(last_sequence) = *previous {
            if sequence != last_sequence.wrapping_add(1) {
                info!(
                    last_sequence,
                    current_sequence = sequence,
                    "Bitcoin rawtx ZMQ sequence advanced non-contiguously; requesting RPC mempool resync"
                );
                self.request_resync();
            }
        }
        *previous = Some(sequence);
    }

    async fn observe_mempool_sequence(&self, txid: Option<Txid>, sequence: u64, removed: bool) {
        let mut previous = self.mempool_sequence.lock().await;
        if let Some(last_sequence) = *previous {
            if sequence != last_sequence.saturating_add(1) {
                info!(
                    last_sequence,
                    current_sequence = sequence,
                    "Bitcoin mempool sequence advanced non-contiguously; requesting RPC mempool resync"
                );
                self.request_resync();
            }
        }
        *previous = Some(sequence);

        if removed {
            if let Some(txid) = txid {
                self.mempool_transactions.lock().await.remove(&txid);
                self.matched_detections
                    .lock()
                    .await
                    .retain(|key| key.txid != txid);
                self.prune_stale_detections().await;
            }
        }
    }

    async fn observe_rawtx_transaction(&self, _tx: &Transaction) {
        self.request_resync();
    }

    async fn queue_detections_from_snapshot(&self) {
        let transactions = self
            .mempool_transactions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        if transactions.is_empty() {
            return;
        }

        let scripts = self.watches_by_script.read().await.clone();
        if scripts.is_empty() {
            return;
        }

        let mut matched_detections = self.matched_detections.lock().await;
        let mut detection_queue = self.detection_queue.lock().await;

        for tx in transactions {
            let txid = tx.compute_txid();
            let matches = match_single_transaction(
                &tx,
                &scripts,
                Utc::now(),
                DepositConfirmationState::Mempool,
                None,
                None,
            );
            if matches.is_empty() {
                continue;
            }

            let mut new_matches = Vec::new();
            let mut inserted_keys = Vec::new();
            for detected in matches {
                let key = mempool_detection_key(&detected, txid);
                if matched_detections.insert(key) {
                    inserted_keys.push(key);
                    new_matches.push(detected);
                }
            }

            if new_matches.is_empty() {
                continue;
            }

            if detection_queue.len().saturating_add(new_matches.len())
                > MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE
            {
                for key in inserted_keys {
                    matched_detections.remove(&key);
                }
                self.request_resync();
                warn!(
                    txid = %txid,
                    queued = detection_queue.len(),
                    matches = new_matches.len(),
                    max_queue = MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE,
                    "Bitcoin mempool detection queue full; deferring detection until the next RPC resync"
                );
                break;
            }

            detection_queue.extend(new_matches);
        }
    }

    async fn prune_stale_detections(&self) {
        let transactions = self
            .mempool_transactions
            .lock()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let scripts = self.watches_by_script.read().await;
        let now = Utc::now();
        let mut valid_keys = HashSet::new();
        for tx in &transactions {
            let txid = tx.compute_txid();
            for detected in match_single_transaction(
                tx,
                &scripts,
                now,
                DepositConfirmationState::Mempool,
                None,
                None,
            ) {
                valid_keys.insert(mempool_detection_key(&detected, txid));
            }
        }

        self.matched_detections
            .lock()
            .await
            .retain(|key| valid_keys.contains(key));
        self.detection_queue.lock().await.retain(|detected| {
            let Ok(txid) = Txid::from_str(&detected.tx_hash) else {
                return false;
            };
            valid_keys.contains(&mempool_detection_key(detected, txid))
                && detection_matches_active_watch(detected, &scripts)
        });
    }
}

fn mempool_detection_key(detected: &DetectedDeposit, txid: Txid) -> MempoolDetectionKey {
    MempoolDetectionKey {
        watch_target: detected.watch_target,
        watch_id: detected.watch_id,
        txid,
        vout: detected.transfer_index,
    }
}

fn detection_matches_active_watch(
    detected: &DetectedDeposit,
    scripts: &HashMap<ScriptBuf, Vec<SharedWatchEntry>>,
) -> bool {
    if detected.confirmation_state != DepositConfirmationState::Mempool {
        return false;
    }
    scripts.values().flatten().any(|watch| {
        watch.watch_target == detected.watch_target
            && watch.watch_id == detected.watch_id
            && watch.source_chain == detected.source_chain
            && watch.source_token == detected.source_token
            && watch.address == detected.address
            && detected.amount >= watch.min_amount
            && detected.amount <= watch.max_amount
    })
}

async fn run_rawtx_listener(endpoint: String, support: Arc<BitcoinMempoolSupport>) {
    loop {
        let mut socket = zeromq::SubSocket::new();

        if let Err(error) = socket.connect(&endpoint).await {
            warn!(
                endpoint,
                %error,
                "Failed to connect to Bitcoin rawtx ZMQ endpoint; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        if let Err(error) = socket.subscribe("rawtx").await {
            warn!(
                endpoint,
                %error,
                "Failed to subscribe to Bitcoin rawtx ZMQ topic; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        loop {
            match socket.recv().await {
                Ok(message) => {
                    if let Err(error) = handle_rawtx_message(message, support.as_ref()).await {
                        warn!(
                            endpoint,
                            %error,
                            "Failed to process Bitcoin rawtx ZMQ message"
                        );
                        support.request_resync();
                    }
                }
                Err(error) => {
                    warn!(
                        endpoint,
                        %error,
                        "Bitcoin rawtx ZMQ stream disconnected; retrying"
                    );
                    support.request_resync();
                    break;
                }
            }
        }

        sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
    }
}

async fn run_sequence_listener(endpoint: String, support: Arc<BitcoinMempoolSupport>) {
    loop {
        let mut socket = zeromq::SubSocket::new();

        if let Err(error) = socket.connect(&endpoint).await {
            warn!(
                endpoint,
                %error,
                "Failed to connect to Bitcoin sequence ZMQ endpoint; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        if let Err(error) = socket.subscribe("sequence").await {
            warn!(
                endpoint,
                %error,
                "Failed to subscribe to Bitcoin sequence ZMQ topic; retrying"
            );
            support.request_resync();
            sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
            continue;
        }

        loop {
            match socket.recv().await {
                Ok(message) => {
                    if let Err(error) = handle_sequence_message(message, support.as_ref()).await {
                        warn!(
                            endpoint,
                            %error,
                            "Failed to process Bitcoin sequence ZMQ message"
                        );
                        support.request_resync();
                    }
                }
                Err(error) => {
                    warn!(
                        endpoint,
                        %error,
                        "Bitcoin sequence ZMQ stream disconnected; retrying"
                    );
                    support.request_resync();
                    break;
                }
            }
        }

        sleep(BITCOIN_ZMQ_RECONNECT_DELAY).await;
    }
}

async fn handle_rawtx_message(
    message: ZmqMessage,
    support: &BitcoinMempoolSupport,
) -> std::result::Result<(), String> {
    let frames = message.into_vec();
    if frames.len() != 3 {
        return Err(format!(
            "expected 3 rawtx frames but received {}",
            frames.len()
        ));
    }

    if frames[0].as_ref() != b"rawtx" {
        return Ok(());
    }

    let sequence = parse_u32_le(&frames[2])?;
    support.observe_rawtx_sequence(sequence).await;

    let tx = deserialize::<Transaction>(&frames[1])
        .map_err(|error| format!("failed to deserialize rawtx payload: {error}"))?;
    support.observe_rawtx_transaction(&tx).await;
    Ok(())
}

async fn handle_sequence_message(
    message: ZmqMessage,
    support: &BitcoinMempoolSupport,
) -> std::result::Result<(), String> {
    let frames = message.into_vec();
    if !(2..=3).contains(&frames.len()) {
        return Err(format!(
            "expected 2 or 3 sequence frames but received {}",
            frames.len()
        ));
    }

    if frames[0].as_ref() != b"sequence" {
        return Ok(());
    }

    let body = frames[1].as_ref();
    if body.len() < 33 {
        return Err(format!(
            "sequence message body too short: expected at least 33 bytes but received {}",
            body.len()
        ));
    }

    let event = body[32];
    match event {
        b'A' | b'R' => {
            if body.len() != 41 {
                return Err(format!(
                    "expected 41-byte sequence message for mempool event but received {}",
                    body.len()
                ));
            }

            let txid = parse_hash_le::<Txid>(&body[..32])?;
            let sequence = parse_u64_le(&body[33..41])?;
            support
                .observe_mempool_sequence(Some(txid), sequence, event == b'R')
                .await;
        }
        b'C' | b'D' => {}
        other => {
            return Err(format!("unknown sequence event byte: {other}"));
        }
    }

    Ok(())
}

fn parse_u32_le(bytes: &[u8]) -> std::result::Result<u32, String> {
    let array: [u8; 4] = bytes
        .try_into()
        .map_err(|_| format!("expected 4-byte little-endian integer, got {}", bytes.len()))?;
    Ok(u32::from_le_bytes(array))
}

fn parse_u64_le(bytes: &[u8]) -> std::result::Result<u64, String> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| format!("expected 8-byte little-endian integer, got {}", bytes.len()))?;
    Ok(u64::from_le_bytes(array))
}

fn parse_hash_le<T>(bytes: &[u8]) -> std::result::Result<T, String>
where
    T: Hash,
{
    let mut reversed = bytes.to_vec();
    reversed.reverse();
    T::from_slice(&reversed).map_err(|error| error.to_string())
}

struct ReqwestRpcTransport {
    url: Url,
    client: reqwest::Client,
    auth_header: Option<String>,
}

impl ReqwestRpcTransport {
    fn new(url: &str, auth: bitcoincore_rpc_async::Auth) -> std::result::Result<Self, String> {
        let url = Url::parse(url).map_err(|error| error.to_string())?;
        let auth_header = match auth.get_user_pass() {
            Ok(Some((user, password))) => Some(format!(
                "Basic {}",
                STANDARD.encode(format!("{user}:{password}"))
            )),
            Ok(None) => None,
            Err(error) => return Err(error.to_string()),
        };

        Ok(Self {
            url,
            client: reqwest::Client::builder()
                .use_rustls_tls()
                .timeout(BITCOIN_RPC_HTTP_TIMEOUT)
                .build()
                .map_err(|error| error.to_string())?,
            auth_header,
        })
    }
}

#[async_trait]
impl jsonrpc::Transport for ReqwestRpcTransport {
    async fn send_request(
        &self,
        request: jsonrpc::Request<'_>,
    ) -> std::result::Result<jsonrpc::Response, jsonrpc::Error> {
        let mut builder = self.client.post(self.url.clone()).json(&request);
        if let Some(auth_header) = &self.auth_header {
            builder = builder.header("Authorization", auth_header);
        }

        let response = builder
            .send()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))?;
        read_json_rpc_response(response).await
    }

    async fn send_batch(
        &self,
        requests: &[jsonrpc::Request<'_>],
    ) -> std::result::Result<Vec<jsonrpc::Response>, jsonrpc::Error> {
        let mut builder = self.client.post(self.url.clone()).json(requests);
        if let Some(auth_header) = &self.auth_header {
            builder = builder.header("Authorization", auth_header);
        }

        let response = builder
            .send()
            .await
            .map_err(|error| jsonrpc::Error::Transport(error.into()))?;
        read_json_rpc_response(response).await
    }

    fn fmt_target(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", redacted_url_for_debug(&self.url))
    }
}

async fn read_json_rpc_response<T>(
    response: reqwest::Response,
) -> std::result::Result<T, jsonrpc::Error>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let body = read_limited_response_body(response, BITCOIN_RPC_MAX_RESPONSE_BODY_BYTES).await?;
    if !status.is_success() {
        return Err(jsonrpc_transport_error(format!(
            "Bitcoin RPC returned HTTP {}: {}",
            status.as_u16(),
            response_body_preview(&body)
        )));
    }

    serde_json::from_slice(&body).map_err(jsonrpc::Error::Json)
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> std::result::Result<Vec<u8>, jsonrpc::Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| jsonrpc::Error::Transport(error.into()))?
    {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(jsonrpc_transport_error(format!(
                "Bitcoin RPC response body exceeded {max_bytes} bytes"
            )));
        }
    }
    Ok(body)
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

fn jsonrpc_transport_error(message: String) -> jsonrpc::Error {
    jsonrpc::Error::Transport(io::Error::other(message).into())
}

fn response_body_preview(body: &[u8]) -> String {
    let truncated = body.len() > BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES;
    let preview = if truncated {
        &body[..BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES]
    } else {
        body
    };
    let mut text = String::from_utf8_lossy(preview).into_owned();
    if truncated {
        text.push_str("...<truncated>");
    }
    text
}

fn redacted_url_for_debug(url: &Url) -> String {
    let host = url.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if url.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if url.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limited_body_chunk_rejects_oversized_append_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn limited_body_chunk_accepts_exact_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn response_body_preview_marks_truncated_large_bodies() {
        let body = vec![b'a'; BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES + 1];

        let preview = response_body_preview(&body);

        assert_eq!(
            preview.len(),
            BITCOIN_RPC_ERROR_BODY_PREVIEW_BYTES + "...<truncated>".len()
        );
        assert!(preview.ends_with("...<truncated>"));
    }

    #[test]
    fn bitcoin_rpc_target_debug_redacts_url_credentials() {
        let url = Url::parse(
            "http://rpc-user:rpc-pass@bitcoin.example:18443/wallet/path-secret?token=query-secret#fragment-secret",
        )
        .expect("url");

        let redacted = redacted_url_for_debug(&url);

        assert_eq!(
            redacted,
            "http://bitcoin.example:18443/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!redacted.contains("rpc-user"));
        assert!(!redacted.contains("rpc-pass"));
        assert!(!redacted.contains("path-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(!redacted.contains("fragment-secret"));
    }

    #[test]
    fn bitcoin_utxo_confirmation_metadata_counts_current_tip_as_one_confirmation() {
        let metadata = bitcoin_utxo_confirmation_metadata(
            true,
            Some(100),
            Some("block-hash".to_string()),
            100,
        )
        .expect("confirmed UTXO at tip should be valid");

        assert_eq!(metadata.state, DepositConfirmationState::Confirmed);
        assert_eq!(metadata.confirmations, 1);
        assert_eq!(metadata.block_height, Some(100));
        assert_eq!(metadata.block_hash.as_deref(), Some("block-hash"));
    }

    #[test]
    fn bitcoin_utxo_confirmation_metadata_rejects_inconsistent_confirmed_utxos() {
        assert!(bitcoin_utxo_confirmation_metadata(true, None, None, 100).is_none());
        assert!(bitcoin_utxo_confirmation_metadata(true, Some(101), None, 100).is_none());
        assert!(bitcoin_utxo_confirmation_metadata(true, Some(0), None, u64::MAX).is_none());
    }

    #[test]
    fn bitcoin_utxo_confirmation_metadata_clears_mempool_block_fields() {
        let metadata =
            bitcoin_utxo_confirmation_metadata(false, Some(100), Some("stale".to_string()), 100)
                .expect("mempool UTXO should be valid");

        assert_eq!(metadata.state, DepositConfirmationState::Mempool);
        assert_eq!(metadata.confirmations, 0);
        assert_eq!(metadata.block_height, None);
        assert_eq!(metadata.block_hash, None);
    }

    fn test_support() -> BitcoinMempoolSupport {
        BitcoinMempoolSupport {
            watches_by_script: RwLock::new(HashMap::new()),
            detection_queue: Mutex::new(VecDeque::new()),
            mempool_transactions: Mutex::new(HashMap::new()),
            matched_detections: Mutex::new(HashSet::new()),
            rawtx_message_sequence: Mutex::new(None),
            mempool_sequence: Mutex::new(None),
            snapshot_height: Mutex::new(None),
            resync_requested: AtomicBool::new(false),
        }
    }

    fn sequence_message(body: Vec<u8>, trailing_sequence: Option<[u8; 4]>) -> ZmqMessage {
        let mut message = ZmqMessage::from("sequence");
        message.push_back(body.into());
        if let Some(counter) = trailing_sequence {
            message.push_back(counter.to_vec().into());
        }
        message
    }

    fn mempool_add_body(sequence: u64) -> Vec<u8> {
        let mut body = vec![0u8; 32];
        body.push(b'A');
        body.extend_from_slice(&sequence.to_le_bytes());
        body
    }

    #[tokio::test]
    async fn handle_sequence_message_accepts_two_frame_form() {
        let support = test_support();
        let message = sequence_message(mempool_add_body(42), None);

        handle_sequence_message(message, &support)
            .await
            .expect("two-frame sequence message should parse");

        assert_eq!(*support.mempool_sequence.lock().await, Some(42));
    }

    #[tokio::test]
    async fn handle_sequence_message_accepts_three_frame_form() {
        let support = test_support();
        let message = sequence_message(mempool_add_body(43), Some(7u32.to_le_bytes()));

        handle_sequence_message(message, &support)
            .await
            .expect("three-frame sequence message should parse");

        assert_eq!(*support.mempool_sequence.lock().await, Some(43));
    }

    #[tokio::test]
    async fn rawtx_observation_defers_detection_until_rpc_snapshot() {
        let support = test_support();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&[1_u8; 32]).unwrap();
        let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let address = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            bitcoin::Network::Regtest,
        );
        let watch = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(42_u64),
            deposit_deadline: Utc::now() + chrono::Duration::minutes(5),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        support
            .replace_watches(HashMap::from([(
                address.script_pubkey(),
                vec![watch.clone()],
            )]))
            .await;
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: Vec::new(),
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(42),
                script_pubkey: address.script_pubkey(),
            }],
        };

        support.observe_rawtx_transaction(&tx).await;

        assert!(support.take_resync_request());
        assert!(support.drain_detection_queue().await.is_empty());

        support
            .mempool_transactions
            .lock()
            .await
            .insert(tx.compute_txid(), tx);
        support.queue_detections_from_snapshot().await;

        let detections = support.drain_detection_queue().await;
        let detected = detections.first().expect("snapshot should queue detection");
        assert_eq!(detected.watch_id, watch.watch_id);
        assert_eq!(
            detected.confirmation_state,
            DepositConfirmationState::Mempool
        );
    }

    #[tokio::test]
    async fn mempool_detection_queue_is_bounded_and_retries_overflow_after_drain() {
        let support = test_support();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&[1_u8; 32]).unwrap();
        let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let address = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            bitcoin::Network::Regtest,
        );
        let watch = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(42_u64),
            deposit_deadline: Utc::now() + chrono::Duration::minutes(5),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        support
            .replace_watches(HashMap::from([(
                address.script_pubkey(),
                vec![watch.clone()],
            )]))
            .await;

        for nonce in 0..=MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE {
            let tx = Transaction {
                version: bitcoin::transaction::Version::TWO,
                lock_time: bitcoin::absolute::LockTime::ZERO,
                input: vec![bitcoin::TxIn {
                    previous_output: bitcoin::OutPoint {
                        txid: Txid::from_byte_array([nonce as u8; 32]),
                        vout: nonce as u32,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: bitcoin::Witness::default(),
                }],
                output: vec![bitcoin::TxOut {
                    value: bitcoin::Amount::from_sat(42),
                    script_pubkey: address.script_pubkey(),
                }],
            };
            support
                .mempool_transactions
                .lock()
                .await
                .insert(tx.compute_txid(), tx);
        }

        support.queue_detections_from_snapshot().await;

        assert!(support.take_resync_request());
        assert_eq!(
            support.detection_queue.lock().await.len(),
            MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE
        );
        assert_eq!(
            support.matched_detections.lock().await.len(),
            MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE
        );

        assert_eq!(
            support.drain_detection_queue().await.len(),
            MAX_BITCOIN_MEMPOOL_DETECTION_QUEUE
        );
        support.queue_detections_from_snapshot().await;

        let detections = support.drain_detection_queue().await;
        assert_eq!(detections.len(), 1);
        assert_eq!(detections[0].watch_id, watch.watch_id);
    }

    #[tokio::test]
    async fn mempool_detection_queue_prunes_removed_watches_and_transactions() {
        let support = test_support();
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&[1_u8; 32]).unwrap();
        let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let address = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            bitcoin::Network::Regtest,
        );
        let watch = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(42_u64),
            deposit_deadline: Utc::now() + chrono::Duration::minutes(5),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: bitcoin::OutPoint {
                    txid: Txid::from_byte_array([9_u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: bitcoin::Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: bitcoin::Witness::default(),
            }],
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(42),
                script_pubkey: address.script_pubkey(),
            }],
        };
        let txid = tx.compute_txid();

        support
            .mempool_transactions
            .lock()
            .await
            .insert(txid, tx.clone());
        support
            .replace_watches(HashMap::from([(
                address.script_pubkey(),
                vec![watch.clone()],
            )]))
            .await;
        assert_eq!(support.detection_queue.lock().await.len(), 1);

        support.replace_watches(HashMap::new()).await;
        assert!(support.drain_detection_queue().await.is_empty());
        assert!(support.matched_detections.lock().await.is_empty());

        support
            .replace_watches(HashMap::from([(address.script_pubkey(), vec![watch])]))
            .await;
        assert_eq!(support.detection_queue.lock().await.len(), 1);

        support.observe_mempool_sequence(Some(txid), 1, true).await;
        assert!(support.drain_detection_queue().await.is_empty());
        assert!(support.matched_detections.lock().await.is_empty());
    }

    #[tokio::test]
    async fn mempool_snapshot_dedupe_is_per_watch_outpoint_not_whole_transaction() {
        let support = test_support();
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let private_key_a = bitcoin::PrivateKey::new(
            bitcoin::secp256k1::SecretKey::from_slice(&[1_u8; 32]).unwrap(),
            bitcoin::Network::Regtest,
        );
        let private_key_b = bitcoin::PrivateKey::new(
            bitcoin::secp256k1::SecretKey::from_slice(&[2_u8; 32]).unwrap(),
            bitcoin::Network::Regtest,
        );
        let address_a = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key_a).unwrap(),
            bitcoin::Network::Regtest,
        );
        let address_b = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key_b).unwrap(),
            bitcoin::Network::Regtest,
        );
        let now = Utc::now();
        let watch_a = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address_a.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(42_u64),
            deposit_deadline: now + chrono::Duration::minutes(5),
            created_at: now,
            updated_at: now,
        });
        let watch_b = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address_b.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(43_u64),
            deposit_deadline: now + chrono::Duration::minutes(5),
            created_at: now,
            updated_at: now,
        });
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: Vec::new(),
            output: vec![
                bitcoin::TxOut {
                    value: bitcoin::Amount::from_sat(42),
                    script_pubkey: address_a.script_pubkey(),
                },
                bitcoin::TxOut {
                    value: bitcoin::Amount::from_sat(43),
                    script_pubkey: address_b.script_pubkey(),
                },
            ],
        };
        support
            .mempool_transactions
            .lock()
            .await
            .insert(tx.compute_txid(), tx);

        support
            .replace_watches(HashMap::from([(
                address_a.script_pubkey(),
                vec![watch_a.clone()],
            )]))
            .await;
        let first = support.drain_detection_queue().await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].watch_id, watch_a.watch_id);

        support
            .replace_watches(HashMap::from([
                (address_a.script_pubkey(), vec![watch_a.clone()]),
                (address_b.script_pubkey(), vec![watch_b.clone()]),
            ]))
            .await;
        let second = support.drain_detection_queue().await;

        assert_eq!(second.len(), 1);
        assert_eq!(second[0].watch_id, watch_b.watch_id);
        assert_eq!(second[0].transfer_index, 1);
    }

    #[test]
    fn match_single_transaction_marks_mempool_outpoint() {
        let secret_key = bitcoin::secp256k1::SecretKey::from_slice(&[1_u8; 32]).unwrap();
        let private_key = bitcoin::PrivateKey::new(secret_key, bitcoin::Network::Regtest);
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let address = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key).unwrap(),
            bitcoin::Network::Regtest,
        );
        let watch = Arc::new(WatchEntry {
            watch_target: crate::watch::WatchTarget::FundingVault,
            watch_id: uuid::Uuid::now_v7(),
            order_id: uuid::Uuid::now_v7(),
            source_chain: ChainType::Bitcoin,
            source_token: TokenIdentifier::Native,
            address: address.to_string(),
            min_amount: alloy::primitives::U256::from(1_u64),
            max_amount: alloy::primitives::U256::from(100_000_u64),
            required_amount: alloy::primitives::U256::from(42_u64),
            deposit_deadline: Utc::now() + chrono::Duration::minutes(5),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        });
        let mut scripts = HashMap::new();
        scripts.insert(address.script_pubkey(), vec![watch.clone()]);
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: Vec::new(),
            output: vec![bitcoin::TxOut {
                value: bitcoin::Amount::from_sat(42),
                script_pubkey: address.script_pubkey(),
            }],
        };

        let detected = match_single_transaction(
            &tx,
            &scripts,
            Utc::now(),
            DepositConfirmationState::Mempool,
            None,
            None,
        );

        assert_eq!(detected.len(), 1);
        assert_eq!(detected[0].watch_id, watch.watch_id);
        assert_eq!(detected[0].transfer_index, 0);
        assert_eq!(detected[0].amount, alloy::primitives::U256::from(42_u64));
        assert_eq!(
            detected[0].confirmation_state,
            DepositConfirmationState::Mempool
        );
        assert_eq!(detected[0].block_height, None);
        assert_eq!(detected[0].block_hash, None);
    }

    #[test]
    fn esplora_sender_extraction_keeps_all_unique_input_addresses() {
        let secp = bitcoin::secp256k1::Secp256k1::new();
        let private_key_a = bitcoin::PrivateKey::new(
            bitcoin::secp256k1::SecretKey::from_slice(&[2_u8; 32]).unwrap(),
            bitcoin::Network::Regtest,
        );
        let private_key_b = bitcoin::PrivateKey::new(
            bitcoin::secp256k1::SecretKey::from_slice(&[3_u8; 32]).unwrap(),
            bitcoin::Network::Regtest,
        );
        let address_a = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key_a).unwrap(),
            bitcoin::Network::Regtest,
        );
        let address_b = Address::p2wpkh(
            &bitcoin::CompressedPublicKey::from_private_key(&secp, &private_key_b).unwrap(),
            bitcoin::Network::Regtest,
        );
        let previous_txid =
            Txid::from_str("1111111111111111111111111111111111111111111111111111111111111111")
                .unwrap();
        let txid =
            Txid::from_str("2222222222222222222222222222222222222222222222222222222222222222")
                .unwrap();
        let tx = esplora_client::api::Tx {
            txid,
            version: 2,
            locktime: 0,
            vin: vec![
                esplora_client::api::Vin {
                    txid: previous_txid,
                    vout: 0,
                    prevout: Some(esplora_client::api::PrevOut {
                        value: 100,
                        scriptpubkey: address_a.script_pubkey(),
                    }),
                    scriptsig: ScriptBuf::new(),
                    witness: Vec::new(),
                    sequence: 0xffff_fffd,
                    is_coinbase: false,
                },
                esplora_client::api::Vin {
                    txid: previous_txid,
                    vout: 1,
                    prevout: Some(esplora_client::api::PrevOut {
                        value: 200,
                        scriptpubkey: address_b.script_pubkey(),
                    }),
                    scriptsig: ScriptBuf::new(),
                    witness: Vec::new(),
                    sequence: 0xffff_fffd,
                    is_coinbase: false,
                },
                esplora_client::api::Vin {
                    txid: previous_txid,
                    vout: 2,
                    prevout: Some(esplora_client::api::PrevOut {
                        value: 300,
                        scriptpubkey: address_a.script_pubkey(),
                    }),
                    scriptsig: ScriptBuf::new(),
                    witness: Vec::new(),
                    sequence: 0xffff_fffd,
                    is_coinbase: false,
                },
            ],
            vout: Vec::new(),
            size: 0,
            weight: 0,
            status: esplora_client::api::TxStatus {
                confirmed: false,
                block_height: None,
                block_hash: None,
                block_time: None,
            },
            fee: 0,
        };

        assert_eq!(
            sender_addresses_from_esplora_tx(&tx, bitcoin::Network::Regtest),
            vec![address_a.to_string(), address_b.to_string()]
        );
    }
}
