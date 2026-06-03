use std::{sync::Arc, time::Duration};

use alloy::primitives::U256;
use async_trait::async_trait;
use bitcoin::address::NetworkUnchecked;
use bitcoin::Address;
use bitcoin_indexer_client::{BitcoinIndexerClient, TxOutput, TxOutputQuery};
use bitcoin_receipt_watcher_client::BitcoinReceiptWatcherClient;
use chrono::Utc;
use router_primitives::{ChainType, TokenIdentifier};
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{
        BlockCursor, BlockScan, DepositConfirmationState, DetectedDeposit, DetectedUtxo,
        DiscoveryBackend,
    },
    error::{Error, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

#[derive(Clone)]
pub struct BitcoinClients {
    pub indexer: Arc<BitcoinIndexerClient>,
    pub receipt_watcher: Option<Arc<BitcoinReceiptWatcherClient>>,
}

impl BitcoinClients {
    pub fn from_args(args: &SauronArgs) -> Result<Option<Self>> {
        let Some(indexer_url) = args.bitcoin_indexer_url.as_deref() else {
            return Ok(None);
        };
        let indexer = Arc::new(
            BitcoinIndexerClient::new(indexer_url)
                .map_err(|source| Error::BitcoinIndexer { source })?,
        );
        let receipt_watcher = args
            .bitcoin_receipt_watcher_url
            .as_deref()
            .map(|url| {
                BitcoinReceiptWatcherClient::new(url, ChainType::Bitcoin.to_db_string())
                    .map(Arc::new)
                    .map_err(|source| Error::BitcoinReceiptWatcher { source })
            })
            .transpose()?;
        Ok(Some(Self {
            indexer,
            receipt_watcher,
        }))
    }
}

pub struct BitcoinDiscoveryBackend {
    clients: BitcoinClients,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

impl BitcoinDiscoveryBackend {
    pub fn new(clients: BitcoinClients, args: &SauronArgs) -> Self {
        Self {
            clients,
            poll_interval: Duration::from_secs(args.sauron_bitcoin_scan_interval_seconds),
            indexed_lookup_concurrency: args.sauron_bitcoin_indexed_lookup_concurrency,
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

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        output_for_watch(&self.clients.indexer, watch).await
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        Ok(BlockCursor {
            height: 0,
            hash: "bitcoin-indexer".to_string(),
        })
    }

    async fn scan_new_blocks(
        &self,
        _from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        let mut detections = Vec::new();
        for watch in watches {
            match output_for_watch(&self.clients.indexer, watch.as_ref()).await {
                Ok(Some(detected)) => detections.push(detected),
                Ok(None) => {}
                Err(error) => warn!(
                    watch_id = %watch.watch_id,
                    %error,
                    "Bitcoin indexer lookup failed for active watch"
                ),
            }
        }
        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: 0,
                hash: "bitcoin-indexer".to_string(),
            },
            detections,
        })
    }
}

pub async fn output_for_watch(
    client: &BitcoinIndexerClient,
    watch: &WatchEntry,
) -> Result<Option<DetectedDeposit>> {
    if watch.source_token != TokenIdentifier::Native {
        return Ok(None);
    }
    let min_amount = u256_to_u64(watch.min_amount, "min_amount")?;
    let max_amount = u256_to_u64(watch.max_amount, "max_amount").unwrap_or(u64::MAX);
    let address = parse_bitcoin_address(&watch.address)?;
    let outputs = spendable_outputs(client, address, min_amount, max_amount).await?;
    let Some(best) = outputs.iter().cloned().max_by_key(|output| {
        (
            output.confirmations,
            output.block_height.unwrap_or_default(),
        )
    }) else {
        return Ok(None);
    };
    Ok(Some(detected_from_output(watch, best, &outputs)))
}

/// Fetch the full set of spendable (non-removed, in-range) outputs at the
/// address. The router stores this complete set; the caller still selects a
/// single "best" output for the primary funding fields.
pub async fn spendable_outputs(
    client: &BitcoinIndexerClient,
    address: Address<NetworkUnchecked>,
    min_amount: u64,
    max_amount: u64,
) -> Result<Vec<TxOutput>> {
    let mut query = TxOutputQuery::new(address);
    query.min_amount = Some(min_amount);
    query.limit = Some(250);
    let page = client
        .tx_outputs(query)
        .await
        .map_err(|source| Error::BitcoinIndexer { source })?;
    Ok(page
        .outputs
        .into_iter()
        .filter(|output| !output.removed)
        .filter(|output| output.amount_sats >= min_amount && output.amount_sats <= max_amount)
        .collect())
}

pub async fn best_output(
    client: &BitcoinIndexerClient,
    address: Address<NetworkUnchecked>,
    min_amount: u64,
    max_amount: u64,
) -> Result<Option<TxOutput>> {
    Ok(spendable_outputs(client, address, min_amount, max_amount)
        .await?
        .into_iter()
        .max_by_key(|output| {
            (
                output.confirmations,
                output.block_height.unwrap_or_default(),
            )
        }))
}

fn output_confirmation_state(output: &TxOutput) -> DepositConfirmationState {
    if output.confirmations > 0 && output.block_height.is_some() {
        DepositConfirmationState::Confirmed
    } else {
        DepositConfirmationState::Mempool
    }
}

fn detected_utxo_from_output(output: &TxOutput) -> DetectedUtxo {
    DetectedUtxo {
        tx_hash: output.txid.to_string(),
        vout: u64::from(output.vout),
        amount: U256::from(output.amount_sats),
        confirmation_state: output_confirmation_state(output),
        block_height: output.block_height,
        block_hash: output.block_hash.as_ref().map(|hash| hash.to_string()),
    }
}

fn detected_from_output(
    watch: &WatchEntry,
    output: TxOutput,
    all_outputs: &[TxOutput],
) -> DetectedDeposit {
    DetectedDeposit {
        watch_target: watch.watch_target,
        watch_id: watch.watch_id,
        execution_step_id: watch.execution_step_id,
        source_chain: ChainType::Bitcoin,
        source_token: TokenIdentifier::Native,
        source_asset_id: "native".to_string(),
        address: watch.address.clone(),
        sender_addresses: Vec::new(),
        tx_hash: output.txid.to_string(),
        transfer_index: u64::from(output.vout),
        amount: U256::from(output.amount_sats),
        confirmation_state: output_confirmation_state(&output),
        block_height: output.block_height,
        block_hash: output.block_hash.map(|hash| hash.to_string()),
        observed_at: Utc::now(),
        indexer_candidate_id: None,
        utxos: all_outputs.iter().map(detected_utxo_from_output).collect(),
    }
}

fn parse_bitcoin_address(raw: &str) -> Result<Address<NetworkUnchecked>> {
    raw.parse().map_err(|source| Error::InvalidWatchRow {
        message: format!("invalid Bitcoin address {raw}: {source}"),
    })
}

fn u256_to_u64(value: U256, field: &'static str) -> Result<u64> {
    u64::try_from(value).map_err(|source| Error::InvalidWatchRow {
        message: format!("Bitcoin watch {field} exceeds u64 satoshi range: {source}"),
    })
}
