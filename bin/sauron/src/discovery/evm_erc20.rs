use std::{
    collections::HashMap,
    future::IntoFuture,
    str::FromStr,
    time::{Duration, Instant},
};

use alloy::{
    consensus::Transaction as _,
    eips::BlockNumberOrTag,
    network::TransactionResponse,
    primitives::{keccak256, Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    sol,
    transports::{RpcError, TransportErrorKind},
};
use async_trait::async_trait;
use chrono::Utc;
use evm_token_indexer_client::{DepositCandidate, TokenIndexerClient, TokenIndexerWatch};
use metrics::{counter, histogram};
use router_primitives::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tokio::time::sleep;
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{
        BlockCursor, BlockScan, DepositConfirmationState, DetectedDeposit, DiscoveryBackend,
    },
    error::{EvmRpcSnafu, EvmTokenIndexerSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);
}

const EVM_REORG_RESCAN_DEPTH: u64 = 32;
const EVM_MAX_LOG_SCAN_BLOCK_SPAN: u64 = 128;
const EVM_REQUIRED_CONFIRMATION_BLOCKS: u64 = 1;
const EVM_RPC_MAX_ATTEMPTS: usize = 5;
const EVM_RPC_RETRY_BASE_DELAY_MILLIS: u64 = 250;
const EVM_RPC_RETRY_MAX_DELAY_MILLIS: u64 = 5_000;
const SAURON_EVM_RPC_REQUEST_DURATION_SECONDS: &str = "sauron_evm_rpc_request_duration_seconds";
const SAURON_EVM_RPC_ERRORS_TOTAL: &str = "sauron_evm_rpc_errors_total";
const SAURON_EVM_RPC_RETRIES_TOTAL: &str = "sauron_evm_rpc_retries_total";

type EvmRpcError = RpcError<TransportErrorKind>;

pub struct EvmErc20DiscoveryBackend {
    name: &'static str,
    chain_type: ChainType,
    provider: DynProvider,
    token_indexer: Option<TokenIndexerClient>,
    transfer_signature: B256,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

type RecipientWatchMap<'a> = HashMap<Address, Vec<&'a WatchEntry>>;
type Erc20WatchMap<'a> = HashMap<Address, RecipientWatchMap<'a>>;

impl EvmErc20DiscoveryBackend {
    pub async fn new_ethereum(args: &SauronArgs) -> Result<Self> {
        Self::new(
            "ethereum_evm",
            ChainType::Ethereum,
            &args.ethereum_mainnet_rpc_url,
            args.ethereum_token_indexer_url.as_deref(),
            &args.ethereum_allowed_token,
            args.sauron_evm_indexed_lookup_concurrency,
        )
        .await
    }

    pub async fn new_base(args: &SauronArgs) -> Result<Self> {
        Self::new(
            "base_evm",
            ChainType::Base,
            &args.base_rpc_url,
            args.base_token_indexer_url.as_deref(),
            &args.base_allowed_token,
            args.sauron_evm_indexed_lookup_concurrency,
        )
        .await
    }

    pub async fn new_arbitrum(args: &SauronArgs) -> Result<Self> {
        Self::new(
            "arbitrum_evm",
            ChainType::Arbitrum,
            &args.arbitrum_rpc_url,
            args.arbitrum_token_indexer_url.as_deref(),
            &args.arbitrum_allowed_token,
            args.sauron_evm_indexed_lookup_concurrency,
        )
        .await
    }

    async fn new(
        name: &'static str,
        chain_type: ChainType,
        rpc_url: &str,
        token_indexer_url: Option<&str>,
        _legacy_allowed_token: &str,
        indexed_lookup_concurrency: usize,
    ) -> Result<Self> {
        let url = rpc_url
            .parse()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: name.to_string(),
                message: format!("invalid RPC URL {rpc_url}: {error}"),
            })?;
        let client = alloy::rpc::client::ClientBuilder::default().http(url);
        let provider = ProviderBuilder::new().connect_client(client).erased();
        let token_indexer = match token_indexer_url {
            Some(token_indexer_url) => {
                Some(TokenIndexerClient::new(token_indexer_url).map_err(|error| {
                    crate::error::Error::DiscoveryBackendInit {
                        backend: name.to_string(),
                        message: format!("invalid token indexer URL {token_indexer_url}: {error}"),
                    }
                })?)
            }
            None => {
                warn!(
                    backend = name,
                    "No token indexer configured; ERC-20 indexed lookups are disabled and discovery will rely on RPC log scans"
                );
                None
            }
        };
        Ok(Self {
            name,
            chain_type,
            provider,
            token_indexer,
            transfer_signature: keccak256("Transfer(address,address,uint256)"),
            poll_interval: Duration::from_secs(5),
            indexed_lookup_concurrency,
        })
    }

    fn watch_erc20_token_address(&self, watch: &WatchEntry) -> Option<Address> {
        match &watch.source_token {
            TokenIdentifier::Native => None,
            TokenIdentifier::Address(token_address) => match Address::from_str(token_address) {
                Ok(address) => Some(address),
                Err(error) => {
                    warn!(
                        backend = self.name,
                        watch_id = %watch.watch_id,
                        token = %token_address,
                        %error,
                        "Skipping EVM ERC-20 watch with invalid token address"
                    );
                    None
                }
            },
        }
    }

    fn watch_native_token_matches(&self, watch: &WatchEntry) -> bool {
        watch.source_token == TokenIdentifier::Native
    }

    fn erc20_watch_map<'a>(&self, watches: &'a [SharedWatchEntry]) -> Erc20WatchMap<'a> {
        let mut tokens: Erc20WatchMap<'a> = HashMap::new();

        for watch in watches {
            let Some(token_address) = self.watch_erc20_token_address(watch) else {
                continue;
            };
            let recipient = match Address::from_str(&watch.address) {
                Ok(address) => address,
                Err(error) => {
                    warn!(
                        backend = self.name,
                        watch_id = %watch.watch_id,
                        address = %watch.address,
                        %error,
                        "Skipping invalid EVM watch address"
                    );
                    continue;
                }
            };

            tokens
                .entry(token_address)
                .or_default()
                .entry(recipient)
                .or_default()
                .push(watch.as_ref());
        }

        tokens
    }

    fn native_address_map<'a>(
        &self,
        watches: &'a [SharedWatchEntry],
    ) -> HashMap<Address, Vec<&'a WatchEntry>> {
        self.address_map(watches, |watch| self.watch_native_token_matches(watch))
    }

    fn address_map<'a>(
        &self,
        watches: &'a [SharedWatchEntry],
        token_matches: impl Fn(&WatchEntry) -> bool,
    ) -> HashMap<Address, Vec<&'a WatchEntry>> {
        let mut addresses: HashMap<Address, Vec<&WatchEntry>> = HashMap::new();

        for watch in watches {
            if !token_matches(watch) {
                continue;
            }

            match Address::from_str(&watch.address) {
                Ok(address) => {
                    addresses.entry(address).or_default().push(watch.as_ref());
                }
                Err(error) => {
                    warn!(
                        backend = self.name,
                        watch_id = %watch.watch_id,
                        address = %watch.address,
                        %error,
                        "Skipping invalid EVM watch address"
                    );
                }
            }
        }

        addresses
    }

    fn indexer_watches(&self, watches: &[SharedWatchEntry]) -> Vec<TokenIndexerWatch> {
        watches
            .iter()
            .filter_map(|watch| {
                let token_address = self.watch_erc20_token_address(watch)?;
                let deposit_address = match Address::from_str(&watch.address) {
                    Ok(address) => address,
                    Err(error) => {
                        warn!(
                            backend = self.name,
                            watch_id = %watch.watch_id,
                            address = %watch.address,
                            %error,
                            "Skipping invalid EVM watch address while syncing token indexer"
                        );
                        return None;
                    }
                };

                Some(TokenIndexerWatch {
                    watch_id: watch.watch_id.to_string(),
                    watch_target: watch.watch_target.as_str().to_string(),
                    token_address,
                    deposit_address,
                    min_amount: watch.min_amount.to_string(),
                    max_amount: watch.max_amount.to_string(),
                    required_amount: watch.required_amount.to_string(),
                    created_at: timestamp_seconds(watch.created_at).to_string(),
                    updated_at: timestamp_seconds(watch.updated_at).to_string(),
                    expires_at: timestamp_seconds(watch.deposit_deadline).to_string(),
                })
            })
            .collect()
    }

    async fn drain_indexer_candidates(
        &self,
        confirmed_tip_height: u64,
    ) -> Result<Vec<DetectedDeposit>> {
        let Some(token_indexer) = &self.token_indexer else {
            return Ok(Vec::new());
        };

        token_indexer
            .materialize_candidates()
            .await
            .context(EvmTokenIndexerSnafu)?;
        let candidates = token_indexer
            .pending_candidates(Some(250))
            .await
            .context(EvmTokenIndexerSnafu)?;

        candidates
            .into_iter()
            .filter_map(
                |candidate| match self.detected_from_indexer_candidate(candidate) {
                    Ok(Some(detected))
                        if detected
                            .block_height
                            .is_some_and(|height| height <= confirmed_tip_height) =>
                    {
                        Some(Ok(detected))
                    }
                    Ok(Some(_)) | Ok(None) => None,
                    Err(error) => Some(Err(error)),
                },
            )
            .collect()
    }

    fn detected_from_indexer_candidate(
        &self,
        candidate: DepositCandidate,
    ) -> Result<Option<DetectedDeposit>> {
        let watch_id = match uuid::Uuid::parse_str(&candidate.watch_id) {
            Ok(watch_id) => watch_id,
            Err(error) => {
                warn!(
                    backend = self.name,
                    candidate_id = %candidate.id,
                    watch_id = %candidate.watch_id,
                    %error,
                    "Skipping token-indexer candidate with invalid watch id"
                );
                return Ok(None);
            }
        };
        let watch_target = match candidate.watch_target.as_str() {
            "provider_operation" => crate::watch::WatchTarget::ProviderOperation,
            "funding_vault" => crate::watch::WatchTarget::FundingVault,
            other => {
                warn!(
                    backend = self.name,
                    candidate_id = %candidate.id,
                    watch_target = other,
                    "Skipping token-indexer candidate with invalid watch target"
                );
                return Ok(None);
            }
        };
        let amount = U256::from_str_radix(&candidate.amount, 10).map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} had invalid amount {}: {}",
                    self.name, candidate.id, candidate.amount, error
                ),
            }
        })?;

        let block_height = candidate.block_number.parse::<u64>().map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} had invalid block_number {}: {}",
                    self.name, candidate.id, candidate.block_number, error
                ),
            }
        })?;

        Ok(Some(DetectedDeposit {
            watch_target,
            watch_id,
            source_chain: self.chain_type,
            source_token: TokenIdentifier::address(candidate.token_address.to_string()),
            address: candidate.deposit_address.to_string(),
            tx_hash: candidate.transaction_hash.to_string(),
            transfer_index: candidate.transfer_index,
            amount,
            confirmation_state: DepositConfirmationState::Confirmed,
            block_height: Some(block_height),
            block_hash: Some(candidate.block_hash.to_string()),
            observed_at: Utc::now(),
            indexer_candidate_id: Some(candidate.id),
        }))
    }

    async fn rpc_call<T, F, Fut>(
        &self,
        method: &'static str,
        mut op: F,
    ) -> std::result::Result<T, EvmRpcError>
    where
        F: FnMut() -> Fut,
        Fut: IntoFuture<Output = std::result::Result<T, EvmRpcError>>,
    {
        for attempt in 1..=EVM_RPC_MAX_ATTEMPTS {
            let started = Instant::now();
            match op().into_future().await {
                Ok(value) => {
                    histogram!(
                        SAURON_EVM_RPC_REQUEST_DURATION_SECONDS,
                        "backend" => self.name.to_string(),
                        "method" => method.to_string(),
                        "outcome" => "ok".to_string(),
                    )
                    .record(started.elapsed().as_secs_f64());
                    return Ok(value);
                }
                Err(error) => {
                    let retryable = evm_rpc_error_is_retryable(&error);
                    histogram!(
                        SAURON_EVM_RPC_REQUEST_DURATION_SECONDS,
                        "backend" => self.name.to_string(),
                        "method" => method.to_string(),
                        "outcome" => "error".to_string(),
                    )
                    .record(started.elapsed().as_secs_f64());
                    counter!(
                        SAURON_EVM_RPC_ERRORS_TOTAL,
                        "backend" => self.name.to_string(),
                        "method" => method.to_string(),
                        "retryable" => retryable.to_string(),
                    )
                    .increment(1);

                    if retryable && attempt < EVM_RPC_MAX_ATTEMPTS {
                        let delay = evm_rpc_retry_delay(attempt);
                        counter!(
                            SAURON_EVM_RPC_RETRIES_TOTAL,
                            "backend" => self.name.to_string(),
                            "method" => method.to_string(),
                        )
                        .increment(1);
                        warn!(
                            backend = self.name,
                            method,
                            attempt,
                            max_attempts = EVM_RPC_MAX_ATTEMPTS,
                            delay_ms = delay.as_millis(),
                            error = %error,
                            "Retrying EVM RPC request after retryable error"
                        );
                        sleep(delay).await;
                        continue;
                    }

                    return Err(error);
                }
            }
        }

        unreachable!("EVM RPC retry loop must return or error");
    }

    async fn current_tip_cursor(&self) -> Result<BlockCursor> {
        let latest_block = self
            .rpc_call("eth_getBlockByNumber", || {
                self.provider.get_block_by_number(BlockNumberOrTag::Latest)
            })
            .await
            .context(EvmRpcSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: self.chain_type.to_db_string().to_string(),
                message: "latest block was unavailable".to_string(),
            })?;
        let height = latest_block.number();

        Ok(BlockCursor {
            height,
            hash: alloy::hex::encode(latest_block.hash()),
        })
    }

    async fn block_hash_at(&self, height: u64) -> Result<Option<String>> {
        let block = self
            .rpc_call("eth_getBlockByNumber", || {
                self.provider.get_block_by_number(height.into())
            })
            .await
            .context(EvmRpcSnafu)?;
        Ok(block.map(|block| alloy::hex::encode(block.hash())))
    }

    async fn scan_native_transfers(
        &self,
        height: u64,
        addresses: &HashMap<Address, Vec<&WatchEntry>>,
    ) -> Result<Vec<DetectedDeposit>> {
        let block = self
            .rpc_call("eth_getBlockByNumber", || {
                self.provider.get_block_by_number(height.into()).full()
            })
            .await
            .context(EvmRpcSnafu)?
            .ok_or_else(|| crate::error::Error::ChainInit {
                chain: self.chain_type.to_db_string().to_string(),
                message: format!("missing full block while scanning native transfers at {height}"),
            })?;
        let block_hash = alloy::hex::encode(block.hash());
        let mut detections = Vec::new();

        for transaction in block.into_transactions_vec() {
            let Some(to) = transaction.to() else {
                continue;
            };
            let Some(watches) = addresses.get(&to) else {
                continue;
            };
            let amount = transaction.value();
            if amount.is_zero() {
                continue;
            }
            let receipt = self
                .rpc_call("eth_getTransactionReceipt", || {
                    self.provider.get_transaction_receipt(transaction.tx_hash())
                })
                .await
                .context(EvmRpcSnafu)?;
            let Some(receipt) = receipt else {
                continue;
            };
            if !receipt.status() {
                continue;
            }
            let transfer_index = transaction.transaction_index().unwrap_or(0);

            for watch in watches {
                if amount < watch.min_amount || amount > watch.max_amount {
                    continue;
                }

                detections.push(DetectedDeposit {
                    watch_target: watch.watch_target,
                    watch_id: watch.watch_id,
                    source_chain: self.chain_type,
                    source_token: TokenIdentifier::Native,
                    address: watch.address.clone(),
                    tx_hash: transaction.tx_hash().to_string(),
                    transfer_index,
                    amount,
                    confirmation_state: DepositConfirmationState::Confirmed,
                    block_height: Some(height),
                    block_hash: Some(block_hash.clone()),
                    observed_at: Utc::now(),
                    indexer_candidate_id: None,
                });
            }
        }

        Ok(detections)
    }
}

#[async_trait]
impl DiscoveryBackend for EvmErc20DiscoveryBackend {
    fn name(&self) -> &'static str {
        self.name
    }

    fn chain(&self) -> ChainType {
        self.chain_type
    }

    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    fn indexed_lookup_concurrency(&self) -> usize {
        self.indexed_lookup_concurrency
    }

    async fn sync_watches(&self, watches: &[SharedWatchEntry]) -> Result<()> {
        let Some(token_indexer) = &self.token_indexer else {
            return Ok(());
        };
        token_indexer
            .sync_watches(self.indexer_watches(watches))
            .await
            .context(EvmTokenIndexerSnafu)?;
        Ok(())
    }

    async fn indexed_lookup(&self, _watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        Ok(None)
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        self.current_tip_cursor().await
    }

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        let current_cursor = self.current_tip_cursor().await?;
        let current_height = current_cursor.height;
        let current_hash = current_cursor.hash.clone();
        let confirmed_height = confirmed_tip_height(current_height);
        let mut detections = self.drain_indexer_candidates(confirmed_height).await?;
        let erc20_watches = self.erc20_watch_map(watches);
        let native_addresses = self.native_address_map(watches);

        if self.token_indexer.is_some() && native_addresses.is_empty() {
            return Ok(BlockScan {
                new_cursor: from_exclusive.clone(),
                detections,
            });
        }

        let should_scan_erc20_with_rpc = self.token_indexer.is_none();
        if ((erc20_watches.is_empty() || !should_scan_erc20_with_rpc)
            && native_addresses.is_empty())
            || confirmed_height <= from_exclusive.height
        {
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: confirmed_height,
                    hash: confirmed_tip_hash(current_height, confirmed_height, &current_hash, self)
                        .await?,
                },
                detections,
            });
        }

        if from_exclusive.height > 0 {
            let expected_hash = self.block_hash_at(from_exclusive.height).await?;
            if expected_hash.as_deref() != Some(from_exclusive.hash.as_str()) {
                let rewind_height = from_exclusive.height.saturating_sub(EVM_REORG_RESCAN_DEPTH);
                let rewind_hash = if rewind_height == 0 {
                    String::new()
                } else {
                    self.block_hash_at(rewind_height).await?.ok_or_else(|| {
                        crate::error::Error::ChainInit {
                            chain: self.chain_type.to_db_string().to_string(),
                            message: format!(
                                "missing block hash while rewinding discovery cursor at height {rewind_height}"
                            ),
                        }
                    })?
                };
                warn!(
                    backend = self.name,
                    stored_height = from_exclusive.height,
                    stored_hash = %from_exclusive.hash,
                    current_hash = ?expected_hash,
                    rewind_height,
                    "EVM discovery cursor reorg detected; rewinding cursor"
                );
                return Ok(BlockScan {
                    new_cursor: BlockCursor {
                        height: rewind_height,
                        hash: rewind_hash,
                    },
                    detections: Vec::new(),
                });
            }
        }

        let scan_from_height = from_exclusive.height.saturating_add(1);
        let scan_to_height = next_scan_to_height(from_exclusive.height, confirmed_height);
        let scan_to_hash = if scan_to_height == current_height {
            current_hash.clone()
        } else {
            self.block_hash_at(scan_to_height).await?.ok_or_else(|| {
                crate::error::Error::ChainInit {
                    chain: self.chain_type.to_db_string().to_string(),
                    message: format!(
                        "missing block hash while advancing discovery cursor at height {scan_to_height}"
                    ),
                }
            })?
        };

        if should_scan_erc20_with_rpc {
            for (token_address, token_watches) in &erc20_watches {
                let filter = Filter::new()
                    .address(*token_address)
                    .event_signature(self.transfer_signature)
                    .from_block(scan_from_height)
                    .to_block(scan_to_height);
                let logs = self
                    .rpc_call("eth_getLogs", || self.provider.get_logs(&filter))
                    .await
                    .map_err(|source| crate::error::Error::EvmLogScan {
                        from_height: scan_from_height,
                        to_height: scan_to_height,
                        source,
                    })?;

                for log in logs {
                    if log.removed {
                        continue;
                    }

                    let Ok(decoded) = log.log_decode::<Transfer>() else {
                        continue;
                    };
                    if decoded.address() != *token_address {
                        continue;
                    }
                    let Some(candidates) = token_watches.get(&decoded.inner.data.to) else {
                        continue;
                    };
                    let Some(transaction_hash) = decoded.transaction_hash else {
                        continue;
                    };
                    let Some(transfer_index) = decoded.log_index else {
                        continue;
                    };

                    for watch in candidates {
                        if decoded.inner.data.value < watch.min_amount
                            || decoded.inner.data.value > watch.max_amount
                        {
                            continue;
                        }

                        detections.push(DetectedDeposit {
                            watch_target: watch.watch_target,
                            watch_id: watch.watch_id,
                            source_chain: self.chain_type,
                            source_token: watch.source_token.clone(),
                            address: watch.address.clone(),
                            tx_hash: transaction_hash.to_string(),
                            transfer_index,
                            amount: decoded.inner.data.value,
                            confirmation_state: DepositConfirmationState::Confirmed,
                            block_height: decoded.block_number,
                            block_hash: decoded.block_hash.map(|hash| alloy::hex::encode(hash)),
                            observed_at: Utc::now(),
                            indexer_candidate_id: None,
                        });
                    }
                }
            }
        }

        if !native_addresses.is_empty() {
            for height in scan_from_height..=scan_to_height {
                detections.extend(
                    self.scan_native_transfers(height, &native_addresses)
                        .await?,
                );
            }
        }

        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: scan_to_height,
                hash: scan_to_hash,
            },
            detections,
        })
    }

    async fn mark_detection_submitted(&self, detected: &DetectedDeposit) -> Result<()> {
        let (Some(token_indexer), Some(candidate_id)) = (
            &self.token_indexer,
            detected.indexer_candidate_id.as_deref(),
        ) else {
            return Ok(());
        };
        token_indexer
            .mark_candidate_submitted(candidate_id)
            .await
            .context(EvmTokenIndexerSnafu)
    }

    async fn release_detection(&self, detected: &DetectedDeposit, error: &str) -> Result<()> {
        let (Some(token_indexer), Some(candidate_id)) = (
            &self.token_indexer,
            detected.indexer_candidate_id.as_deref(),
        ) else {
            return Ok(());
        };
        token_indexer
            .release_candidate(candidate_id, Some(error.to_string()))
            .await
            .context(EvmTokenIndexerSnafu)
    }
}

fn next_scan_to_height(from_exclusive_height: u64, current_height: u64) -> u64 {
    current_height.min(from_exclusive_height.saturating_add(EVM_MAX_LOG_SCAN_BLOCK_SPAN))
}

fn confirmed_tip_height(current_height: u64) -> u64 {
    current_height.saturating_sub(EVM_REQUIRED_CONFIRMATION_BLOCKS)
}

async fn confirmed_tip_hash(
    current_height: u64,
    confirmed_height: u64,
    current_hash: &str,
    backend: &EvmErc20DiscoveryBackend,
) -> Result<String> {
    if confirmed_height == 0 {
        return Ok(String::new());
    }
    if confirmed_height == current_height {
        return Ok(current_hash.to_string());
    }
    backend.block_hash_at(confirmed_height).await?.ok_or_else(|| {
        crate::error::Error::ChainInit {
            chain: backend.chain_type.to_db_string().to_string(),
            message: format!(
                "missing block hash while advancing confirmed discovery cursor at height {confirmed_height}"
            ),
        }
    })
}

fn timestamp_seconds(timestamp: chrono::DateTime<Utc>) -> i64 {
    timestamp.timestamp().max(0)
}

fn evm_rpc_error_is_retryable(error: &EvmRpcError) -> bool {
    match error {
        RpcError::Transport(kind) => evm_transport_error_is_retryable(kind),
        RpcError::ErrorResp(payload) => {
            payload.is_retry_err() || evm_rpc_message_is_retryable(payload.message.as_ref())
        }
        RpcError::DeserError { text, .. } => evm_rpc_message_is_retryable(text),
        RpcError::NullResp => true,
        _ => false,
    }
}

fn evm_transport_error_is_retryable(error: &TransportErrorKind) -> bool {
    match error {
        TransportErrorKind::MissingBatchResponse(_) => true,
        TransportErrorKind::HttpError(http_error) => {
            http_error.is_rate_limit_err()
                || http_error.is_temporarily_unavailable()
                || matches!(http_error.status, 408 | 500 | 502 | 504)
                || evm_rpc_message_is_retryable(&http_error.body)
        }
        TransportErrorKind::Custom(error) => evm_rpc_message_is_retryable(&error.to_string()),
        _ => false,
    }
}

fn evm_rpc_message_is_retryable(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("rate limit")
        || lowered.contains("too many requests")
        || lowered.contains("temporarily unavailable")
        || lowered.contains("temporary internal error")
        || lowered.contains("timeout")
        || lowered.contains("incorrect response body")
        || lowered.contains("wrong json-rpc response")
}

fn evm_rpc_retry_delay(attempt: usize) -> Duration {
    let shift = attempt.saturating_sub(1).min(8) as u32;
    let multiplier = 1u64 << shift;
    Duration::from_millis(
        (EVM_RPC_RETRY_BASE_DELAY_MILLIS.saturating_mul(multiplier))
            .min(EVM_RPC_RETRY_MAX_DELAY_MILLIS),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::{
        primitives::{address, b256, Address, U256},
        providers::{Provider, ProviderBuilder},
    };
    use chrono::{Duration, Utc};
    use evm_token_indexer_client::DepositCandidate;
    use router_primitives::{ChainType, TokenIdentifier};
    use uuid::Uuid;

    use crate::watch::{WatchEntry, WatchTarget};

    use crate::discovery::DepositConfirmationState;

    use super::{
        confirmed_tip_height, next_scan_to_height, EvmErc20DiscoveryBackend,
        EVM_MAX_LOG_SCAN_BLOCK_SPAN,
    };

    #[test]
    fn caps_scan_window_to_max_span() {
        assert_eq!(
            next_scan_to_height(10, 10 + EVM_MAX_LOG_SCAN_BLOCK_SPAN + 500),
            10 + EVM_MAX_LOG_SCAN_BLOCK_SPAN
        );
    }

    #[test]
    fn uses_tip_when_backlog_fits_in_one_window() {
        assert_eq!(next_scan_to_height(10, 42), 42);
    }

    #[test]
    fn confirmed_tip_requires_one_child_block() {
        assert_eq!(confirmed_tip_height(0), 0);
        assert_eq!(confirmed_tip_height(1), 0);
        assert_eq!(confirmed_tip_height(42), 41);
    }

    #[test]
    fn erc20_watch_map_tracks_each_active_token_contract() {
        let backend = test_backend();
        let token_a = address!("1111111111111111111111111111111111111111");
        let token_b = address!("2222222222222222222222222222222222222222");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let native_recipient = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let watches = vec![
            watch_entry(TokenIdentifier::address(token_a.to_string()), recipient),
            watch_entry(TokenIdentifier::address(token_b.to_string()), recipient),
            watch_entry(TokenIdentifier::Native, native_recipient),
        ];

        let map = backend.erc20_watch_map(&watches);

        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get(&token_a)
                .and_then(|recipients| recipients.get(&recipient))
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            map.get(&token_b)
                .and_then(|recipients| recipients.get(&recipient))
                .map(Vec::len),
            Some(1)
        );
        assert!(!map
            .values()
            .any(|recipients| recipients.contains_key(&native_recipient)));
    }

    #[test]
    fn erc20_watch_map_drops_tokens_without_active_watches() {
        let backend = test_backend();
        let token_a = address!("1111111111111111111111111111111111111111");
        let token_b = address!("2222222222222222222222222222222222222222");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let all_watches = [
            watch_entry(TokenIdentifier::address(token_a.to_string()), recipient),
            watch_entry(TokenIdentifier::address(token_b.to_string()), recipient),
        ];
        let active_watches = vec![all_watches[0].clone()];

        let map = backend.erc20_watch_map(&active_watches);

        assert!(map.contains_key(&token_a));
        assert!(!map.contains_key(&token_b));
    }

    #[test]
    fn erc20_watch_map_skips_invalid_token_addresses() {
        let backend = test_backend();
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let watches = vec![watch_entry(
            TokenIdentifier::Address("not-an-address".to_string()),
            recipient,
        )];

        let map = backend.erc20_watch_map(&watches);

        assert!(map.is_empty());
    }

    #[test]
    fn indexer_watches_include_only_erc20_watches() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let native_recipient = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let watches = vec![
            watch_entry(TokenIdentifier::address(token.to_string()), recipient),
            watch_entry(TokenIdentifier::Native, native_recipient),
        ];

        let indexer_watches = backend.indexer_watches(&watches);

        assert_eq!(indexer_watches.len(), 1);
        assert_eq!(indexer_watches[0].token_address, token);
        assert_eq!(indexer_watches[0].deposit_address, recipient);
        assert_eq!(indexer_watches[0].watch_target, "funding_vault");
    }

    #[test]
    fn maps_token_indexer_candidate_to_detected_deposit() {
        let backend = test_backend();
        let watch_id = Uuid::new_v4();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!("2222222222222222222222222222222222222222222222222222222222222222");

        let candidate = DepositCandidate {
            id: "candidate-1".to_string(),
            watch_id: watch_id.to_string(),
            watch_target: "funding_vault".to_string(),
            chain_id: 8453,
            token_address: token,
            deposit_address: recipient,
            amount: "42".to_string(),
            required_amount: "42".to_string(),
            transaction_hash: tx_hash,
            transfer_index: 7,
            block_number: "100".to_string(),
            block_hash: b256!("3333333333333333333333333333333333333333333333333333333333333333"),
            block_timestamp: "1700000000".to_string(),
            status: "pending".to_string(),
            attempt_count: 0,
            last_error: None,
            created_at: "1700000001".to_string(),
            delivered_at: None,
        };

        let detected = backend
            .detected_from_indexer_candidate(candidate)
            .expect("candidate should parse")
            .expect("candidate should map");

        assert_eq!(detected.watch_id, watch_id);
        assert_eq!(detected.watch_target, WatchTarget::FundingVault);
        assert_eq!(detected.source_chain, ChainType::Base);
        assert_eq!(
            detected.source_token,
            TokenIdentifier::address(token.to_string())
        );
        assert_eq!(detected.address, recipient.to_string());
        assert_eq!(detected.tx_hash, tx_hash.to_string());
        assert_eq!(detected.transfer_index, 7);
        assert_eq!(detected.amount, U256::from(42_u64));
        assert_eq!(
            detected.confirmation_state,
            DepositConfirmationState::Confirmed
        );
        assert_eq!(detected.block_height, Some(100));
        assert_eq!(
            detected.block_hash.as_deref(),
            Some("0x3333333333333333333333333333333333333333333333333333333333333333")
        );
        assert_eq!(
            detected.indexer_candidate_id.as_deref(),
            Some("candidate-1")
        );
    }

    fn test_backend() -> EvmErc20DiscoveryBackend {
        let url = "http://127.0.0.1:1"
            .parse()
            .expect("test provider URL must parse");
        let client = alloy::rpc::client::ClientBuilder::default().http(url);
        let provider = ProviderBuilder::new().connect_client(client).erased();

        EvmErc20DiscoveryBackend {
            name: "test_evm",
            chain_type: ChainType::Base,
            provider,
            token_indexer: None,
            transfer_signature: alloy::primitives::keccak256("Transfer(address,address,uint256)"),
            poll_interval: std::time::Duration::from_secs(5),
            indexed_lookup_concurrency: 1,
        }
    }

    fn watch_entry(token: TokenIdentifier, address: Address) -> Arc<WatchEntry> {
        Arc::new(WatchEntry {
            watch_target: WatchTarget::FundingVault,
            watch_id: Uuid::new_v4(),
            order_id: Uuid::new_v4(),
            source_chain: ChainType::Base,
            source_token: token,
            address: address.to_string(),
            min_amount: U256::from(1_u64),
            max_amount: U256::from(1_000_u64),
            required_amount: U256::from(1_000_u64),
            deposit_deadline: Utc::now() + Duration::minutes(5),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }
}
