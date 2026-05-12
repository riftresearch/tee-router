use std::time::{Duration, Instant};

use alloy::{
    eips::BlockNumberOrTag,
    network::TransactionResponse,
    primitives::TxHash,
    providers::{DynProvider, Provider, ProviderBuilder, WsConnect},
};
use evm_receipt_watcher_client::{WatchEvent, WatchStatus};
use futures_util::StreamExt;
use tokio::{sync::broadcast, time::sleep};
use url::Url;

use crate::{config::Config, telemetry, Error, Result};

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
    chain: String,
    http_provider: DynProvider,
    ws_rpc_url: Option<String>,
    pending: PendingWatches,
    pubsub: ReceiptPubSub,
    poll_interval: Duration,
    ws_reconnect_delay: Duration,
    receipt_retry_count: u32,
    receipt_retry_delay: Duration,
}

impl Watcher {
    pub async fn new(
        config: &Config,
        pending: PendingWatches,
        pubsub: ReceiptPubSub,
    ) -> Result<Self> {
        let http_url = Url::parse(config.http_rpc_url.trim()).map_err(|source| {
            Error::InvalidConfiguration {
                message: format!("invalid EVM_RECEIPT_WATCHER_HTTP_RPC_URL: {source}"),
            }
        })?;
        let http_provider = ProviderBuilder::new().connect_http(http_url).erased();
        Ok(Self {
            chain: config.chain.clone(),
            http_provider,
            ws_rpc_url: config.ws_rpc_url.clone(),
            pending,
            pubsub,
            poll_interval: config.poll_interval(),
            ws_reconnect_delay: config.ws_reconnect_delay(),
            receipt_retry_count: config.receipt_retry_count,
            receipt_retry_delay: config.receipt_retry_delay(),
        })
    }

    pub async fn run(self) {
        let mut last_observed_height = None;

        loop {
            if let Some(ws_rpc_url) = self.ws_rpc_url.as_deref() {
                match self
                    .run_ws_session(ws_rpc_url, &mut last_observed_height)
                    .await
                {
                    Ok(()) => {
                        tracing::warn!(chain = self.chain, "EVM head websocket ended");
                    }
                    Err(error) => {
                        telemetry::record_mode(&self.chain, true);
                        tracing::warn!(
                            chain = self.chain,
                            %error,
                            "EVM head websocket unavailable; running degraded polling tick"
                        );
                        if let Err(poll_error) =
                            self.run_polling_tick(&mut last_observed_height).await
                        {
                            tracing::warn!(
                                chain = self.chain,
                                %poll_error,
                                "EVM receipt watcher degraded polling tick failed"
                            );
                        }
                    }
                }
                sleep(self.ws_reconnect_delay).await;
            } else if let Err(error) = self.run_polling_loop(&mut last_observed_height).await {
                telemetry::record_mode(&self.chain, true);
                tracing::warn!(
                    chain = self.chain,
                    %error,
                    "EVM receipt watcher polling loop failed; restarting"
                );
                sleep(self.ws_reconnect_delay).await;
            }
        }
    }

    async fn run_ws_session(
        &self,
        ws_rpc_url: &str,
        last_observed_height: &mut Option<u64>,
    ) -> Result<()> {
        let ws_provider = ProviderBuilder::new()
            .connect_ws(WsConnect::new(ws_rpc_url.to_string()))
            .await
            .map_err(|source| Error::WebSocket {
                source: source.to_string(),
            })?
            .erased();

        telemetry::record_mode(&self.chain, false);
        let current_head = self.current_head().await?;
        self.scan_to_height(last_observed_height, current_head)
            .await?;

        let subscription =
            ws_provider
                .subscribe_blocks()
                .await
                .map_err(|source| Error::WebSocket {
                    source: source.to_string(),
                })?;
        let mut stream = subscription.into_stream();

        while let Some(header) = stream.next().await {
            self.scan_to_height(last_observed_height, header.number)
                .await?;
        }

        Err(Error::WebSocket {
            source: "newHeads stream ended".to_string(),
        })
    }

    async fn run_polling_loop(&self, last_observed_height: &mut Option<u64>) -> Result<()> {
        telemetry::record_mode(&self.chain, true);
        loop {
            self.run_polling_tick(last_observed_height).await?;
            sleep(self.poll_interval).await;
        }
    }

    async fn run_polling_tick(&self, last_observed_height: &mut Option<u64>) -> Result<()> {
        let current_head = self.current_head().await?;
        self.scan_to_height(last_observed_height, current_head)
            .await
    }

    pub async fn scan_to_height(
        &self,
        last_observed_height: &mut Option<u64>,
        new_head_height: u64,
    ) -> Result<()> {
        for height in head_stream::heights_to_scan(*last_observed_height, new_head_height) {
            self.scan_block(height).await?;
            *last_observed_height = Some(height);
        }
        Ok(())
    }

    pub async fn scan_block(&self, height: u64) -> Result<()> {
        let started = Instant::now();
        let tx_hashes = self.block_tx_hashes(height).await?;
        let matches = self.pending.matches_in_hashes(&tx_hashes).await;
        telemetry::record_block_scanned(
            &self.chain,
            tx_hashes.len(),
            matches.len(),
            started.elapsed(),
        );

        for watch in matches {
            self.confirm_watch(watch).await?;
        }
        Ok(())
    }

    async fn current_head(&self) -> Result<u64> {
        let started = Instant::now();
        match self.http_provider.get_block_number().await {
            Ok(height) => {
                telemetry::record_rpc(&self.chain, "eth_blockNumber", "ok", started.elapsed());
                Ok(height)
            }
            Err(source) => {
                telemetry::record_rpc(&self.chain, "eth_blockNumber", "error", started.elapsed());
                Err(Error::EvmRpc {
                    method: "eth_blockNumber",
                    source: source.to_string(),
                })
            }
        }
    }

    async fn block_tx_hashes(&self, height: u64) -> Result<Vec<TxHash>> {
        let started = Instant::now();
        let block = match self
            .http_provider
            .get_block_by_number(BlockNumberOrTag::Number(height))
            .await
        {
            Ok(Some(block)) => {
                telemetry::record_rpc(&self.chain, "eth_getBlockByNumber", "ok", started.elapsed());
                block
            }
            Ok(None) => {
                telemetry::record_rpc(
                    &self.chain,
                    "eth_getBlockByNumber",
                    "missing",
                    started.elapsed(),
                );
                return Err(Error::MissingBlock { height });
            }
            Err(source) => {
                telemetry::record_rpc(
                    &self.chain,
                    "eth_getBlockByNumber",
                    "error",
                    started.elapsed(),
                );
                return Err(Error::EvmRpc {
                    method: "eth_getBlockByNumber",
                    source: source.to_string(),
                });
            }
        };

        if let Some(hashes) = block.transactions.as_hashes() {
            return Ok(hashes.to_vec());
        }

        Ok(block
            .into_transactions_vec()
            .into_iter()
            .map(|transaction| transaction.tx_hash())
            .collect())
    }

    async fn confirm_watch(&self, watch: RegisteredWatch) -> Result<()> {
        let Some(receipt) = self.receipt_with_retry(watch.tx_hash).await? else {
            telemetry::record_receipt(&self.chain, "missing");
            tracing::warn!(
                chain = self.chain,
                tx_hash = %watch.tx_hash,
                "matched transaction in block but receipt was unavailable after retries"
            );
            return Ok(());
        };

        let Some(current_watch) = self.pending.take(&watch.tx_hash).await else {
            return Ok(());
        };

        let logs = receipt.logs().to_vec();
        telemetry::record_receipt(&self.chain, "confirmed");
        self.pubsub.publish(WatchEvent {
            chain: current_watch.chain,
            tx_hash: current_watch.tx_hash,
            requesting_operation_id: current_watch.requesting_operation_id,
            status: WatchStatus::Confirmed,
            receipt: Some(receipt),
            logs,
        });
        Ok(())
    }

    async fn receipt_with_retry(
        &self,
        tx_hash: TxHash,
    ) -> Result<Option<evm_receipt_watcher_client::Receipt>> {
        let mut attempt = 0;
        loop {
            let started = Instant::now();
            match self.http_provider.get_transaction_receipt(tx_hash).await {
                Ok(receipt) => {
                    telemetry::record_rpc(
                        &self.chain,
                        "eth_getTransactionReceipt",
                        "ok",
                        started.elapsed(),
                    );
                    return Ok(receipt);
                }
                Err(source) => {
                    telemetry::record_rpc(
                        &self.chain,
                        "eth_getTransactionReceipt",
                        "error",
                        started.elapsed(),
                    );
                    if attempt >= self.receipt_retry_count {
                        return Err(Error::EvmRpc {
                            method: "eth_getTransactionReceipt",
                            source: source.to_string(),
                        });
                    }
                }
            }

            attempt += 1;
            sleep(self.receipt_retry_delay).await;
        }
    }
}
