use std::{sync::Arc, time::Duration};

use bitcoin::{consensus::deserialize, Block, BlockHash, Transaction, Txid};
use bitcoin_receipt_watcher_client::{Receipt, WatchEvent, WatchStatus};
use bitcoincore_rpc_async::{Client as BitcoinRpcClient, RpcApi};
use snafu::ResultExt;
use tokio::{
    sync::{broadcast, Mutex},
    time::sleep,
};
use zeromq::{Socket, SocketRecv, ZmqMessage};

use crate::{config::Config, Error, Result};

pub mod head_stream;
pub mod pending;

pub use pending::{PendingWatches, RegisteredWatch};

#[derive(Clone)]
pub struct ReceiptPubSub {
    sender: broadcast::Sender<WatchEvent>,
}

impl ReceiptPubSub {
    pub fn new(max_subscriber_lag: usize) -> Self {
        let (sender, _) = broadcast::channel(max_subscriber_lag);
        Self { sender }
    }

    pub fn publish(&self, event: WatchEvent) {
        let _ = self.sender.send(event);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent> {
        self.sender.subscribe()
    }
}

#[derive(Clone)]
pub struct Watcher {
    rpc: Arc<BitcoinRpcClient>,
    rawblock_endpoint: String,
    pending: PendingWatches,
    pubsub: ReceiptPubSub,
    poll_interval: Duration,
    zmq_reconnect_delay: Duration,
    last_observed_height: Arc<Mutex<Option<u64>>>,
}

impl Watcher {
    pub async fn new(
        config: &Config,
        pending: PendingWatches,
        pubsub: ReceiptPubSub,
    ) -> Result<Self> {
        let rpc = BitcoinRpcClient::new(config.rpc_url.clone(), config.rpc_auth()?)
            .await
            .context(crate::error::BitcoinRpcInitSnafu)?;
        Ok(Self {
            rpc: Arc::new(rpc),
            rawblock_endpoint: config.zmq_rawblock_endpoint.clone(),
            pending,
            pubsub,
            poll_interval: config.poll_interval(),
            zmq_reconnect_delay: config.zmq_reconnect_delay(),
            last_observed_height: Arc::new(Mutex::new(None)),
        })
    }

    pub fn rpc_client(&self) -> Arc<BitcoinRpcClient> {
        self.rpc.clone()
    }

    pub async fn run(self) {
        tokio::spawn(self.clone().run_rawblock_loop());
        tokio::spawn(self.run_polling_loop());
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
                        if let Err(error) = self.scan_block(block).await {
                            tracing::warn!(%error, "Bitcoin receipt watcher rawblock scan failed");
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

    async fn run_polling_loop(self) {
        loop {
            if let Err(error) = self.run_polling_tick().await {
                tracing::warn!(%error, "Bitcoin receipt watcher polling tick failed");
            }
            sleep(self.poll_interval).await;
        }
    }

    async fn run_polling_tick(&self) -> Result<()> {
        let current_head = self.current_head().await?;
        let mut last_observed = self.last_observed_height.lock().await;
        for height in head_stream::heights_to_scan(*last_observed, current_head) {
            let block = self.block_at_height(height).await?;
            self.scan_block(block).await?;
            *last_observed = Some(height);
        }
        Ok(())
    }

    pub async fn scan_block(&self, block: Block) -> Result<()> {
        let txids = block
            .txdata
            .iter()
            .map(Transaction::compute_txid)
            .collect::<Vec<_>>();
        let matches = self.pending.matches_in_txids(&txids).await;
        if matches.is_empty() {
            self.observe_height(&block).await?;
            return Ok(());
        }

        let block_hash = block.block_hash();
        let block_height = self.block_height(block_hash).await?;
        let tip_height = self.current_head().await?;
        let confirmations = tip_height.saturating_sub(block_height).saturating_add(1);

        for watch in matches {
            if let Some(tx) = find_tx(&block, watch.txid) {
                self.confirm_watch(watch, tx.clone(), block_hash, block_height, confirmations)
                    .await?;
            }
        }
        self.observe_height(&block).await?;
        Ok(())
    }

    async fn confirm_watch(
        &self,
        watch: RegisteredWatch,
        tx: Transaction,
        block_hash: BlockHash,
        block_height: u64,
        confirmations: u64,
    ) -> Result<()> {
        let Some(current_watch) = self.pending.take(&watch.txid).await else {
            return Ok(());
        };

        self.pubsub.publish(WatchEvent {
            chain: current_watch.chain,
            txid: current_watch.txid,
            requesting_operation_id: current_watch.requesting_operation_id,
            status: WatchStatus::Confirmed,
            receipt: Some(Receipt {
                tx,
                block_hash,
                block_height,
                confirmations,
            }),
        });
        Ok(())
    }

    async fn observe_height(&self, block: &Block) -> Result<()> {
        let height = self.block_height(block.block_hash()).await?;
        let mut last_observed = self.last_observed_height.lock().await;
        if last_observed.map(|last| height > last).unwrap_or(true) {
            *last_observed = Some(height);
        }
        Ok(())
    }

    async fn current_head(&self) -> Result<u64> {
        let info = self
            .rpc
            .get_blockchain_info()
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblockchaininfo",
                source,
            })?;
        nonnegative_i64_to_u64(info.blocks, "getblockchaininfo.blocks")
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

    async fn block_height(&self, hash: BlockHash) -> Result<u64> {
        let block = self
            .rpc
            .get_block_verbose_one(&hash)
            .await
            .map_err(|source| Error::BitcoinRpc {
                method: "getblock",
                source,
            })?;
        nonnegative_i64_to_u64(block.height, "getblock.height")
    }
}

fn nonnegative_i64_to_u64(value: i64, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|source| Error::InvalidConfiguration {
        message: format!("{field} was negative or too large for u64: {source}"),
    })
}

fn find_tx(block: &Block, txid: Txid) -> Option<&Transaction> {
    block.txdata.iter().find(|tx| tx.compute_txid() == txid)
}

fn block_from_message(message: ZmqMessage) -> Result<Option<Block>> {
    let frames = message.into_vec();
    if frames.len() != 3 {
        return Err(Error::Zmq {
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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;

    fn txid(byte: u8) -> Txid {
        let hex = format!("{byte:02x}").repeat(32);
        hex.parse().unwrap()
    }

    fn block_hash(byte: u8) -> BlockHash {
        let hex = format!("{byte:02x}").repeat(32);
        hex.parse().unwrap()
    }

    #[test]
    fn find_tx_returns_matching_transaction() {
        let tx = Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: bitcoin::absolute::LockTime::ZERO,
            input: Vec::new(),
            output: Vec::new(),
        };
        let computed_txid = tx.compute_txid();
        let block = Block {
            header: bitcoin::block::Header {
                version: bitcoin::block::Version::NO_SOFT_FORK_SIGNALLING,
                prev_blockhash: block_hash(1),
                merkle_root: bitcoin::TxMerkleNode::all_zeros(),
                time: 1,
                bits: bitcoin::CompactTarget::from_consensus(0),
                nonce: 0,
            },
            txdata: vec![tx],
        };

        assert!(find_tx(&block, computed_txid).is_some());
        assert!(find_tx(&block, txid(9)).is_none());
    }
}
