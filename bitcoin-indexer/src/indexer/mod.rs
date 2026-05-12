use std::{collections::VecDeque, str::FromStr, sync::Arc, time::Duration};

use bitcoin::{
    consensus::deserialize, Address, Block, BlockHash, Network, ScriptBuf, Transaction, Txid,
};
use bitcoin_indexer_client::{TxOutput, TxOutputPage};
use bitcoincore_rpc_async::{Client as BitcoinRpcClient, RpcApi};
use serde::Serialize;
use snafu::ResultExt;
use tokio::{
    sync::{broadcast, Mutex},
    time::sleep,
};
use zeromq::{Socket, SocketRecv, ZmqMessage};

use crate::{config::Config, Error, Result};

const MAX_QUERY_LIMIT: usize = 1_000;
const DEFAULT_QUERY_LIMIT: usize = 100;

#[derive(Clone)]
pub struct IndexerPubSub {
    sender: broadcast::Sender<TxOutput>,
}

impl IndexerPubSub {
    pub fn new(max_subscriber_lag: usize) -> Self {
        let (sender, _) = broadcast::channel(max_subscriber_lag);
        Self { sender }
    }

    pub fn publish(&self, event: TxOutput) {
        let _ = self.sender.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TxOutput> {
        self.sender.subscribe()
    }
}

#[derive(Clone)]
pub struct BitcoinIndexer {
    rpc: Arc<BitcoinRpcClient>,
    esplora: esplora_client::AsyncClient,
    network: Network,
    rawblock_endpoint: String,
    rawtx_endpoint: String,
    pubsub: IndexerPubSub,
    cursor: Arc<Mutex<BlockCursorState>>,
    poll_interval: Duration,
    zmq_reconnect_delay: Duration,
}

impl BitcoinIndexer {
    pub async fn new(config: &Config, pubsub: IndexerPubSub) -> Result<Self> {
        let rpc = BitcoinRpcClient::new(config.rpc_url.clone(), config.rpc_auth()?)
            .await
            .context(crate::error::BitcoinRpcInitSnafu)?;
        let esplora = esplora_client::Builder::new(config.esplora_url.trim_end_matches('/'))
            .build_async()
            .map_err(|source| Error::Esplora { source })?;

        Ok(Self {
            rpc: Arc::new(rpc),
            esplora,
            network: config.network,
            rawblock_endpoint: config.zmq_rawblock_endpoint.clone(),
            rawtx_endpoint: config.zmq_rawtx_endpoint.clone(),
            pubsub,
            cursor: Arc::new(Mutex::new(BlockCursorState::new(config.reorg_rescan_depth))),
            poll_interval: config.poll_interval(),
            zmq_reconnect_delay: config.zmq_reconnect_delay(),
        })
    }

    pub fn pubsub(&self) -> IndexerPubSub {
        self.pubsub.clone()
    }

    pub async fn last_block_height(&self) -> Option<u64> {
        self.cursor.lock().await.last_height()
    }

    pub async fn query_outputs(
        &self,
        address: &str,
        from_block: u64,
        min_amount: u64,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<TxOutputPage> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT).min(MAX_QUERY_LIMIT);
        let address = parse_checked_address(address, self.network)?;
        let cursor = cursor.map(OutputCursor::parse).transpose()?;
        let tip_height = self.current_tip_height().await?;
        let mut last_seen = None;
        let mut outputs = Vec::new();

        loop {
            let txs = self
                .esplora
                .get_address_txs(&address, last_seen)
                .await
                .map_err(|source| Error::Esplora { source })?;
            if txs.is_empty() {
                break;
            }

            let mut saw_older_confirmed = false;
            for tx in &txs {
                if let Some(height) = tx.status.block_height {
                    if u64::from(height) < from_block {
                        saw_older_confirmed = true;
                        continue;
                    }
                } else if from_block > 0 {
                    continue;
                }

                outputs.extend(tx_outputs_from_esplora_tx(
                    tx,
                    &address.script_pubkey(),
                    self.network,
                    tip_height,
                    min_amount,
                )?);
            }

            last_seen = txs.last().map(|tx| tx.txid);
            if txs.len() < 25 || saw_older_confirmed {
                break;
            }
        }

        outputs.sort_by_key(output_ordering_key);
        if let Some(cursor) = cursor {
            outputs.retain(|output| OutputCursor::from_output(output) > cursor);
        }

        let has_more = outputs.len() > limit;
        if has_more {
            outputs.truncate(limit);
        }
        let next_cursor = if has_more {
            outputs
                .last()
                .map(OutputCursor::from_output)
                .map(|cursor| cursor.to_string())
        } else {
            None
        };

        Ok(TxOutputPage {
            outputs,
            next_cursor,
            has_more,
        })
    }

    pub async fn run(self) {
        tokio::spawn(self.clone().run_rawtx_loop());
        tokio::spawn(self.clone().run_rawblock_loop());
        tokio::spawn(self.run_poll_loop());
    }

    async fn run_rawtx_loop(self) {
        loop {
            let mut socket = zeromq::SubSocket::new();
            if let Err(source) = socket.connect(&self.rawtx_endpoint).await {
                tracing::warn!(
                    endpoint = self.rawtx_endpoint,
                    %source,
                    "failed to connect to Bitcoin rawtx ZMQ endpoint"
                );
                sleep(self.zmq_reconnect_delay).await;
                continue;
            }
            if let Err(source) = socket.subscribe("rawtx").await {
                tracing::warn!(
                    endpoint = self.rawtx_endpoint,
                    %source,
                    "failed to subscribe to Bitcoin rawtx ZMQ topic"
                );
                sleep(self.zmq_reconnect_delay).await;
                continue;
            }

            while let Ok(message) = socket.recv().await {
                match rawtx_from_message(message) {
                    Ok(Some(tx)) => self.publish_transaction_outputs(&tx),
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, "failed to process Bitcoin rawtx ZMQ message");
                    }
                }
            }

            tracing::warn!(
                endpoint = self.rawtx_endpoint,
                "Bitcoin rawtx ZMQ stream disconnected"
            );
            sleep(self.zmq_reconnect_delay).await;
        }
    }

    async fn run_rawblock_loop(self) {
        loop {
            let mut socket = zeromq::SubSocket::new();
            if let Err(source) = socket.connect(&self.rawblock_endpoint).await {
                tracing::warn!(
                    endpoint = self.rawblock_endpoint,
                    %source,
                    "failed to connect to Bitcoin rawblock ZMQ endpoint"
                );
                sleep(self.zmq_reconnect_delay).await;
                continue;
            }
            if let Err(source) = socket.subscribe("rawblock").await {
                tracing::warn!(
                    endpoint = self.rawblock_endpoint,
                    %source,
                    "failed to subscribe to Bitcoin rawblock ZMQ topic"
                );
                sleep(self.zmq_reconnect_delay).await;
                continue;
            }

            while let Ok(message) = socket.recv().await {
                match block_from_message(message) {
                    Ok(Some(block)) => {
                        if let Err(error) = self.apply_block(block).await {
                            tracing::warn!(%error, "failed to index Bitcoin rawblock ZMQ message");
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%error, "failed to process Bitcoin rawblock ZMQ message");
                    }
                }
            }

            tracing::warn!(
                endpoint = self.rawblock_endpoint,
                "Bitcoin rawblock ZMQ stream disconnected"
            );
            sleep(self.zmq_reconnect_delay).await;
        }
    }

    async fn run_poll_loop(self) {
        loop {
            if let Err(error) = self.poll_tip().await {
                tracing::warn!(%error, "Bitcoin indexer polling tick failed");
            }
            sleep(self.poll_interval).await;
        }
    }

    async fn poll_tip(&self) -> Result<()> {
        let tip_height = self.current_tip_height().await?;
        let last_height = self.cursor.lock().await.last_height();
        let start = match last_height {
            Some(last_height) if tip_height <= last_height => return Ok(()),
            Some(last_height) => last_height.saturating_add(1),
            None => tip_height,
        };
        self.rescan_canonical_from(start).await
    }

    async fn apply_block(&self, block: Block) -> Result<()> {
        let indexed = self.indexed_block(block).await?;
        let update = self.cursor.lock().await.observe_block(indexed);
        match update {
            CursorUpdate::Accepted(outputs) => {
                for output in outputs {
                    self.pubsub.publish(output);
                }
            }
            CursorUpdate::Ignored => {}
            CursorUpdate::Reorg {
                retracted_outputs,
                rescan_from,
            } => {
                for output in retracted_outputs {
                    self.pubsub.publish(output);
                }
                self.rescan_canonical_from(rescan_from).await?;
            }
        }
        Ok(())
    }

    async fn rescan_canonical_from(&self, mut start_height: u64) -> Result<()> {
        loop {
            let tip_height = self.current_tip_height().await?;
            if start_height > tip_height {
                return Ok(());
            }

            let mut restart_from = None;
            for height in start_height..=tip_height {
                let block = self.block_at_height(height).await?;
                let indexed = self.indexed_block(block).await?;
                let update = self.cursor.lock().await.observe_block(indexed);
                match update {
                    CursorUpdate::Accepted(outputs) => {
                        for output in outputs {
                            self.pubsub.publish(output);
                        }
                    }
                    CursorUpdate::Ignored => {}
                    CursorUpdate::Reorg {
                        retracted_outputs,
                        rescan_from,
                    } => {
                        for output in retracted_outputs {
                            self.pubsub.publish(output);
                        }
                        restart_from = Some(rescan_from);
                        break;
                    }
                }
            }

            match restart_from {
                Some(next_start) => start_height = next_start,
                None => return Ok(()),
            }
        }
    }

    async fn block_at_height(&self, height: u64) -> Result<Block> {
        let hash = self
            .rpc
            .get_block_hash(height)
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblockhash",
                source,
            })?;
        self.rpc
            .get_block(&hash)
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblock",
                source,
            })
    }

    async fn indexed_block(&self, block: Block) -> Result<IndexedBlock> {
        let hash = block.block_hash();
        let header = self
            .rpc
            .get_block_verbose_one(&hash)
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblock",
                source,
            })?;
        let height = nonnegative_i64_to_u64(header.height, "getblock.height")?;
        let block_time = nonnegative_i64_to_u64(header.time, "getblock.time")?;
        let meta = BlockMeta {
            height,
            hash,
            block_time,
            confirmations: header.confirmations.max(0) as u64,
        };
        let outputs = tx_outputs_from_block(&block, &meta, self.network)?;
        Ok(IndexedBlock {
            height: meta.height,
            hash,
            prev_hash: block.header.prev_blockhash,
            outputs,
        })
    }

    async fn current_tip_height(&self) -> Result<u64> {
        self.rpc
            .get_block_count()
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblockcount",
                source,
            })
    }

    fn publish_transaction_outputs(&self, tx: &Transaction) {
        for output in tx_outputs_from_transaction(tx, self.network, None, false) {
            self.pubsub.publish(output);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BlockMeta {
    height: u64,
    hash: BlockHash,
    block_time: u64,
    confirmations: u64,
}

#[derive(Debug, Clone)]
struct IndexedBlock {
    height: u64,
    hash: BlockHash,
    prev_hash: BlockHash,
    outputs: Vec<TxOutput>,
}

#[derive(Debug, Clone, Copy)]
struct BlockCursor {
    height: u64,
    hash: BlockHash,
}

#[derive(Debug)]
struct BlockCursorState {
    reorg_depth: u64,
    last: Option<BlockCursor>,
    recent: VecDeque<IndexedBlock>,
}

#[derive(Debug, PartialEq, Eq)]
enum CursorUpdate {
    Accepted(Vec<TxOutput>),
    Reorg {
        retracted_outputs: Vec<TxOutput>,
        rescan_from: u64,
    },
    Ignored,
}

impl BlockCursorState {
    fn new(reorg_depth: u64) -> Self {
        Self {
            reorg_depth,
            last: None,
            recent: VecDeque::new(),
        }
    }

    fn last_height(&self) -> Option<u64> {
        self.last.map(|cursor| cursor.height)
    }

    fn observe_block(&mut self, block: IndexedBlock) -> CursorUpdate {
        if self
            .recent
            .iter()
            .any(|recent| recent.height == block.height && recent.hash == block.hash)
        {
            return CursorUpdate::Ignored;
        }

        if let Some(last) = self.last {
            if block.height <= last.height || block.prev_hash != last.hash {
                let floor = block.height.saturating_sub(self.reorg_depth);
                return self.reorg_from(floor);
            }
        }

        let outputs = block.outputs.clone();
        self.push_block(block);
        CursorUpdate::Accepted(outputs)
    }

    fn reorg_from(&mut self, floor: u64) -> CursorUpdate {
        let mut retained = VecDeque::new();
        let mut retracted_outputs = Vec::new();

        for block in self.recent.drain(..) {
            if block.height >= floor {
                retracted_outputs.extend(block.outputs.into_iter().map(mark_removed));
            } else {
                retained.push_back(block);
            }
        }

        self.recent = retained;
        self.last = self.recent.back().map(|block| BlockCursor {
            height: block.height,
            hash: block.hash,
        });
        CursorUpdate::Reorg {
            retracted_outputs,
            rescan_from: floor,
        }
    }

    fn push_block(&mut self, block: IndexedBlock) {
        self.last = Some(BlockCursor {
            height: block.height,
            hash: block.hash,
        });
        self.recent.push_back(block);
        let max_recent = self.reorg_depth.saturating_add(2) as usize;
        while self.recent.len() > max_recent {
            self.recent.pop_front();
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd)]
struct OutputCursor {
    height_sort: u64,
    txid: Txid,
    vout: u32,
}

impl OutputCursor {
    fn from_output(output: &TxOutput) -> Self {
        Self {
            height_sort: output.block_height.unwrap_or(u64::MAX),
            txid: output.txid,
            vout: output.vout,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        let parts = value.split(':').collect::<Vec<_>>();
        match parts.as_slice() {
            ["c", height, txid, vout] => Ok(Self {
                height_sort: height
                    .parse::<u64>()
                    .map_err(|source| Error::InvalidCursor {
                        value: value.to_string(),
                        reason: source.to_string(),
                    })?,
                txid: txid
                    .parse::<Txid>()
                    .map_err(|source| Error::InvalidCursor {
                        value: value.to_string(),
                        reason: source.to_string(),
                    })?,
                vout: vout.parse::<u32>().map_err(|source| Error::InvalidCursor {
                    value: value.to_string(),
                    reason: source.to_string(),
                })?,
            }),
            ["m", txid, vout] => Ok(Self {
                height_sort: u64::MAX,
                txid: txid
                    .parse::<Txid>()
                    .map_err(|source| Error::InvalidCursor {
                        value: value.to_string(),
                        reason: source.to_string(),
                    })?,
                vout: vout.parse::<u32>().map_err(|source| Error::InvalidCursor {
                    value: value.to_string(),
                    reason: source.to_string(),
                })?,
            }),
            _ => Err(Error::InvalidCursor {
                value: value.to_string(),
                reason: "expected c:<height>:<txid>:<vout> or m:<txid>:<vout>".to_string(),
            }),
        }
    }
}

impl std::fmt::Display for OutputCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.height_sort == u64::MAX {
            write!(f, "m:{}:{}", self.txid, self.vout)
        } else {
            write!(f, "c:{}:{}:{}", self.height_sort, self.txid, self.vout)
        }
    }
}

fn output_ordering_key(output: &TxOutput) -> OutputCursor {
    OutputCursor::from_output(output)
}

fn nonnegative_i64_to_u64(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|source| Error::InvalidConfiguration {
        message: format!("{field} was negative or too large for u64: {source}"),
    })
}

fn mark_removed(mut output: TxOutput) -> TxOutput {
    output.removed = true;
    output
}

fn tx_outputs_from_block(
    block: &Block,
    meta: &BlockMeta,
    network: Network,
) -> Result<Vec<TxOutput>> {
    let mut outputs = Vec::new();
    for tx in &block.txdata {
        outputs.extend(tx_outputs_from_transaction(tx, network, Some(*meta), false));
    }
    Ok(outputs)
}

fn tx_outputs_from_transaction(
    tx: &Transaction,
    network: Network,
    meta: Option<BlockMeta>,
    removed: bool,
) -> Vec<TxOutput> {
    let txid = tx.compute_txid();
    tx.output
        .iter()
        .enumerate()
        .filter_map(|(vout, output)| {
            let vout = u32::try_from(vout).ok()?;
            let address = Address::from_script(&output.script_pubkey, network)
                .ok()?
                .into_unchecked();
            Some(TxOutput {
                txid,
                vout,
                address,
                amount_sats: output.value.to_sat(),
                block_height: meta.map(|meta| meta.height),
                block_hash: meta.map(|meta| meta.hash),
                block_time: meta.map(|meta| meta.block_time),
                confirmations: meta.map(|meta| meta.confirmations).unwrap_or(0),
                removed,
            })
        })
        .collect()
}

fn tx_outputs_from_esplora_tx(
    tx: &esplora_client::api::Tx,
    script_pubkey: &ScriptBuf,
    network: Network,
    tip_height: u64,
    min_amount: u64,
) -> Result<Vec<TxOutput>> {
    let block_height = tx.status.block_height.map(u64::from);
    let confirmations = block_height
        .map(|height| tip_height.saturating_sub(height).saturating_add(1))
        .unwrap_or(0);
    let mut outputs = Vec::new();

    for (vout, output) in tx.vout.iter().enumerate() {
        if &output.scriptpubkey != script_pubkey || output.value < min_amount {
            continue;
        }
        let vout = u32::try_from(vout).map_err(|source| Error::InvalidConfiguration {
            message: format!("Bitcoin output index exceeded u32: {source}"),
        })?;
        let address = Address::from_script(&output.scriptpubkey, network)
            .map_err(|source| Error::InvalidAddress {
                value: output.scriptpubkey.to_string(),
                source: source.to_string(),
            })?
            .into_unchecked();
        outputs.push(TxOutput {
            txid: tx.txid,
            vout,
            address,
            amount_sats: output.value,
            block_height,
            block_hash: tx.status.block_hash,
            block_time: tx.status.block_time,
            confirmations,
            removed: false,
        });
    }

    Ok(outputs)
}

fn parse_checked_address(value: &str, network: Network) -> Result<Address> {
    Address::from_str(value.trim())
        .map_err(|source| Error::InvalidAddress {
            value: value.to_string(),
            source: source.to_string(),
        })?
        .require_network(network)
        .map_err(|source| Error::InvalidAddress {
            value: value.to_string(),
            source: source.to_string(),
        })
}

fn rawtx_from_message(message: ZmqMessage) -> Result<Option<Transaction>> {
    let frames = message.into_vec();
    if frames.len() != 3 {
        return Err(Error::Zmq {
            topic: "rawtx",
            source: format!("expected 3 frames but received {}", frames.len()),
        });
    }
    if frames[0].as_ref() != b"rawtx" {
        return Ok(None);
    }
    let tx =
        deserialize::<Transaction>(&frames[1]).context(crate::error::ConsensusDecodeSnafu {
            context: "rawtx payload",
        })?;
    Ok(Some(tx))
}

fn block_from_message(message: ZmqMessage) -> Result<Option<Block>> {
    let frames = message.into_vec();
    if frames.len() != 3 {
        return Err(Error::Zmq {
            topic: "rawblock",
            source: format!("expected 3 frames but received {}", frames.len()),
        });
    }
    if frames[0].as_ref() != b"rawblock" {
        return Ok(None);
    }
    let block = deserialize::<Block>(&frames[1]).context(crate::error::ConsensusDecodeSnafu {
        context: "rawblock payload",
    })?;
    Ok(Some(block))
}

#[derive(Debug, Serialize)]
pub struct HealthSnapshot {
    pub network: Network,
    pub last_block_height: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(byte: u8) -> BlockHash {
        let hex = format!("{byte:02x}").repeat(32);
        hex.parse().unwrap()
    }

    fn txid(byte: u8) -> Txid {
        let hex = format!("{byte:02x}").repeat(32);
        hex.parse().unwrap()
    }

    fn output(byte: u8, height: u64) -> TxOutput {
        TxOutput {
            txid: txid(byte),
            vout: 0,
            address: "bcrt1q2nfxmhd4n3c8834pj72xagvyr9gl57n5r94fsl"
                .parse()
                .unwrap(),
            amount_sats: 50_000,
            block_height: Some(height),
            block_hash: Some(hash(byte)),
            block_time: Some(1_710_000_000),
            confirmations: 1,
            removed: false,
        }
    }

    fn indexed(height: u64, hash_byte: u8, prev_byte: u8, outputs: Vec<TxOutput>) -> IndexedBlock {
        IndexedBlock {
            height,
            hash: hash(hash_byte),
            prev_hash: hash(prev_byte),
            outputs,
        }
    }

    #[test]
    fn cursor_paginates_after_last_seen_output() {
        let first = output(1, 101);
        let second = output(2, 102);
        let cursor = OutputCursor::from_output(&first).to_string();
        let parsed = OutputCursor::parse(&cursor).unwrap();

        assert!(OutputCursor::from_output(&second) > parsed);
        assert_eq!(cursor, format!("c:101:{}:0", first.txid));
    }

    #[test]
    fn block_cursor_ignores_duplicate_block() {
        let mut state = BlockCursorState::new(6);
        let block = indexed(1, 1, 0, vec![output(1, 1)]);

        assert!(matches!(
            state.observe_block(block.clone()),
            CursorUpdate::Accepted(_)
        ));
        assert!(matches!(state.observe_block(block), CursorUpdate::Ignored));
    }

    #[test]
    fn block_cursor_retracts_recent_outputs_on_reorg() {
        let mut state = BlockCursorState::new(6);
        let first = output(1, 1);
        let second = output(2, 2);
        assert!(matches!(
            state.observe_block(indexed(1, 1, 0, vec![first])),
            CursorUpdate::Accepted(_)
        ));
        assert!(matches!(
            state.observe_block(indexed(2, 2, 1, vec![second])),
            CursorUpdate::Accepted(_)
        ));

        let update = state.observe_block(indexed(3, 3, 9, vec![output(3, 3)]));

        match update {
            CursorUpdate::Reorg {
                retracted_outputs,
                rescan_from,
            } => {
                assert_eq!(rescan_from, 0);
                assert_eq!(retracted_outputs.len(), 2);
                assert!(retracted_outputs.iter().all(|output| output.removed));
            }
            other => panic!("expected reorg, got {other:?}"),
        }
    }
}
