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
use evm_token_indexer_client::{
    DepositCandidate, TokenIndexerClient, TokenIndexerWatch, TransferEvent,
};
use metrics::{counter, histogram};
use router_primitives::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tokio::time::sleep;
use tracing::{debug, warn};
use url::Url;

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
const EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS: u64 = 256;
const EVM_INDEXED_LOOKUP_WATCH_CREATED_AT_SAFETY_SECONDS: i64 = 5 * 60;
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
            args.token_indexer_api_key.as_deref(),
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
            args.token_indexer_api_key.as_deref(),
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
            args.token_indexer_api_key.as_deref(),
            args.sauron_evm_indexed_lookup_concurrency,
        )
        .await
    }

    async fn new(
        name: &'static str,
        chain_type: ChainType,
        rpc_url: &str,
        token_indexer_url: Option<&str>,
        token_indexer_api_key: Option<&str>,
        indexed_lookup_concurrency: usize,
    ) -> Result<Self> {
        let url = rpc_url
            .parse()
            .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                backend: name.to_string(),
                message: format!(
                    "invalid RPC URL {}: {error}",
                    sanitize_url_for_error(rpc_url)
                ),
            })?;
        let client = alloy::rpc::client::ClientBuilder::default().http(url);
        let provider = ProviderBuilder::new().connect_client(client).erased();
        let token_indexer = match token_indexer_url {
            Some(token_indexer_url) => Some(
                TokenIndexerClient::new_with_api_key(
                    token_indexer_url,
                    token_indexer_api_key.map(ToOwned::to_owned),
                )
                .map_err(|error| crate::error::Error::DiscoveryBackendInit {
                    backend: name.to_string(),
                    message: format!(
                        "invalid token indexer URL {}: {error}",
                        sanitize_url_for_error(token_indexer_url)
                    ),
                })?,
            ),
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
        watches: &[SharedWatchEntry],
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

        let active_watches = watches
            .iter()
            .map(|watch| (watch.watch_id, watch.clone()))
            .collect::<HashMap<_, _>>();
        let mut detections = Vec::new();
        for candidate in candidates {
            match self.detected_from_indexer_candidate(&candidate, &active_watches) {
                Ok(Some(detected))
                    if detected
                        .block_height
                        .is_some_and(|height| height <= confirmed_tip_height) =>
                {
                    detections.push(detected);
                }
                Ok(Some(_)) | Ok(None) => {}
                Err(error) => {
                    let error_message = error.to_string();
                    if error_message.contains("no longer active") {
                        debug!(
                            backend = self.name,
                            candidate_id = %candidate.id,
                            error = %error_message,
                            "Discarding stale token-indexer deposit candidate for inactive watch"
                        );
                    } else {
                        warn!(
                            backend = self.name,
                            candidate_id = %candidate.id,
                            error = %error_message,
                            "Discarding malformed token-indexer deposit candidate"
                        );
                    }
                    token_indexer
                        .discard_candidate(&candidate.id, Some(error_message))
                        .await
                        .context(EvmTokenIndexerSnafu)?;
                }
            }
        }
        Ok(detections)
    }

    fn detected_from_indexer_candidate(
        &self,
        candidate: &DepositCandidate,
        active_watches: &HashMap<uuid::Uuid, SharedWatchEntry>,
    ) -> Result<Option<DetectedDeposit>> {
        let watch_id = match uuid::Uuid::parse_str(&candidate.watch_id) {
            Ok(watch_id) => watch_id,
            Err(error) => {
                return Err(crate::error::Error::InvalidWatchRow {
                    message: format!(
                        "{} candidate {} had invalid watch_id {}: {}",
                        self.name, candidate.id, candidate.watch_id, error
                    ),
                });
            }
        };
        let watch_target = match candidate.watch_target.as_str() {
            "provider_operation" => crate::watch::WatchTarget::ProviderOperation,
            "funding_vault" => crate::watch::WatchTarget::FundingVault,
            other => {
                return Err(crate::error::Error::InvalidWatchRow {
                    message: format!(
                        "{} candidate {} had invalid watch_target {}",
                        self.name, candidate.id, other
                    ),
                });
            }
        };
        let Some(watch) = active_watches.get(&watch_id) else {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} referenced watch {} that is no longer active",
                    self.name, candidate.id, watch_id
                ),
            });
        };
        if watch.watch_target != watch_target {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} watch_target {} no longer matches active watch target {}",
                    self.name,
                    candidate.id,
                    candidate.watch_target,
                    watch.watch_target.as_str()
                ),
            });
        }
        if watch.source_chain != self.chain_type {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} chain does not match active watch chain",
                    self.name, candidate.id
                ),
            });
        }
        if evm_chain_id(self.chain_type) != Some(candidate.chain_id) {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} chain_id {} does not match backend chain",
                    self.name, candidate.id, candidate.chain_id
                ),
            });
        }
        let amount = U256::from_str_radix(&candidate.amount, 10).map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} had invalid amount {}: {}",
                    self.name, candidate.id, candidate.amount, error
                ),
            }
        })?;
        if amount < watch.min_amount || amount > watch.max_amount {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} amount {} was outside active watch bounds {}..={}",
                    self.name, candidate.id, amount, watch.min_amount, watch.max_amount
                ),
            });
        }
        let source_token =
            TokenIdentifier::address(candidate.token_address.to_string()).normalize();
        if source_token != watch.source_token {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} token does not match active watch token",
                    self.name, candidate.id
                ),
            });
        }
        let watch_deposit_address = Address::from_str(&watch.address).map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} active watch had invalid deposit address {}: {}",
                    self.name, candidate.id, watch.address, error
                ),
            }
        })?;
        if candidate.deposit_address != watch_deposit_address {
            return Err(crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} candidate {} deposit address {} does not match active watch address {}",
                    self.name, candidate.id, candidate.deposit_address, watch.address
                ),
            });
        }

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
            source_token,
            address: watch.address.clone(),
            sender_addresses: vec![candidate.from_address.to_string()],
            tx_hash: candidate.transaction_hash.to_string(),
            transfer_index: candidate.transfer_index,
            amount,
            confirmation_state: DepositConfirmationState::Confirmed,
            block_height: Some(block_height),
            block_hash: Some(candidate.block_hash.to_string()),
            observed_at: Utc::now(),
            indexer_candidate_id: Some(candidate.id.clone()),
        }))
    }

    fn detected_from_indexer_transfer(
        &self,
        watch: &WatchEntry,
        token_address: Address,
        deposit_address: Address,
        confirmed_tip_height: u64,
        transfer: TransferEvent,
    ) -> Result<Option<DetectedDeposit>> {
        if transfer.to != deposit_address {
            return Ok(None);
        }
        if transfer.token_address != Some(token_address) {
            return Ok(None);
        }
        let Some(transfer_index) = transfer.log_index else {
            return Ok(None);
        };
        let amount = U256::from_str_radix(&transfer.amount, 10).map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} indexed transfer {} had invalid amount {}: {}",
                    self.name, transfer.id, transfer.amount, error
                ),
            }
        })?;
        if amount < watch.min_amount || amount > watch.max_amount {
            return Ok(None);
        }
        let block_height = transfer.block_number.parse::<u64>().map_err(|error| {
            crate::error::Error::InvalidWatchRow {
                message: format!(
                    "{} indexed transfer {} had invalid block_number {}: {}",
                    self.name, transfer.id, transfer.block_number, error
                ),
            }
        })?;
        if block_height > confirmed_tip_height {
            return Ok(None);
        }

        Ok(Some(DetectedDeposit {
            watch_target: watch.watch_target,
            watch_id: watch.watch_id,
            source_chain: self.chain_type,
            source_token: watch.source_token.clone(),
            address: watch.address.clone(),
            sender_addresses: vec![transfer.from.to_string()],
            tx_hash: transfer.transaction_hash.to_string(),
            transfer_index,
            amount,
            confirmation_state: DepositConfirmationState::Confirmed,
            block_height: Some(block_height),
            block_hash: Some(transfer.block_hash.to_string()),
            observed_at: Utc::now(),
            indexer_candidate_id: None,
        }))
    }

    async fn rpc_indexed_lookup_erc20_transfer(
        &self,
        watch: &WatchEntry,
        token_address: Address,
        confirmed_tip_height: u64,
    ) -> Result<Option<DetectedDeposit>> {
        if confirmed_tip_height == 0 {
            return Ok(None);
        }
        let recipient = match Address::from_str(&watch.address) {
            Ok(address) => address,
            Err(error) => {
                warn!(
                    backend = self.name,
                    watch_id = %watch.watch_id,
                    address = %watch.address,
                    %error,
                    "Skipping invalid EVM watch address during RPC indexed lookup"
                );
                return Ok(None);
            }
        };

        let from_height = self
            .indexed_lookup_from_height(watch, confirmed_tip_height)
            .await?;
        let mut best = None;
        let mut chunk_from_height = from_height;
        while chunk_from_height <= confirmed_tip_height {
            let chunk_to_height =
                next_scan_to_height(chunk_from_height.saturating_sub(1), confirmed_tip_height);
            let filter = Filter::new()
                .address(token_address)
                .event_signature(self.transfer_signature)
                .from_block(chunk_from_height)
                .to_block(chunk_to_height);
            let logs = self
                .rpc_call("eth_getLogs", || self.provider.get_logs(&filter))
                .await
                .map_err(|source| crate::error::Error::EvmLogScan {
                    from_height: chunk_from_height,
                    to_height: chunk_to_height,
                    source,
                })?;

            for log in logs {
                if log.removed {
                    continue;
                }
                let Ok(decoded) = log.log_decode::<Transfer>() else {
                    continue;
                };
                if decoded.address() != token_address || decoded.inner.data.to != recipient {
                    continue;
                }
                let Some(transaction_hash) = decoded.transaction_hash else {
                    continue;
                };
                let Some(transfer_index) = decoded.log_index else {
                    continue;
                };
                if decoded.inner.data.value < watch.min_amount
                    || decoded.inner.data.value > watch.max_amount
                {
                    continue;
                }

                let detected = DetectedDeposit {
                    watch_target: watch.watch_target,
                    watch_id: watch.watch_id,
                    source_chain: self.chain_type,
                    source_token: watch.source_token.clone(),
                    address: watch.address.clone(),
                    sender_addresses: vec![decoded.inner.data.from.to_string()],
                    tx_hash: transaction_hash.to_string(),
                    transfer_index,
                    amount: decoded.inner.data.value,
                    confirmation_state: DepositConfirmationState::Confirmed,
                    block_height: decoded.block_number,
                    block_hash: decoded.block_hash.map(alloy::hex::encode),
                    observed_at: Utc::now(),
                    indexer_candidate_id: None,
                };
                if should_replace_detected(best.as_ref(), &detected) {
                    best = Some(detected);
                }
            }

            chunk_from_height = chunk_to_height.saturating_add(1);
        }

        Ok(best)
    }

    async fn rpc_indexed_lookup_native_transfer(
        &self,
        watch: &WatchEntry,
        confirmed_tip_height: u64,
    ) -> Result<Option<DetectedDeposit>> {
        if confirmed_tip_height == 0 {
            return Ok(None);
        }
        let recipient = match Address::from_str(&watch.address) {
            Ok(address) => address,
            Err(error) => {
                warn!(
                    backend = self.name,
                    watch_id = %watch.watch_id,
                    address = %watch.address,
                    %error,
                    "Skipping invalid EVM native watch address during RPC indexed lookup"
                );
                return Ok(None);
            }
        };

        let mut addresses = HashMap::new();
        addresses.insert(recipient, vec![watch]);
        let from_height = self
            .indexed_lookup_from_height(watch, confirmed_tip_height)
            .await?;
        let mut best = None;
        for height in from_height..=confirmed_tip_height {
            for detected in self.scan_native_transfers(height, &addresses).await? {
                if should_replace_detected(best.as_ref(), &detected) {
                    best = Some(detected);
                }
            }
        }

        Ok(best)
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
        let mut attempt = 1;
        loop {
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
                        attempt += 1;
                        continue;
                    }

                    return Err(error);
                }
            }
        }
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

    async fn block_timestamp_at(&self, height: u64) -> Result<Option<u64>> {
        let block = self
            .rpc_call("eth_getBlockByNumber", || {
                self.provider.get_block_by_number(height.into())
            })
            .await
            .context(EvmRpcSnafu)?;
        Ok(block.map(|block| block.header.timestamp))
    }

    async fn indexed_lookup_from_height(
        &self,
        watch: &WatchEntry,
        confirmed_tip_height: u64,
    ) -> Result<u64> {
        if confirmed_tip_height == 0 {
            return Ok(0);
        }

        let fixed_window_from_height = confirmed_tip_height
            .saturating_sub(EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS)
            .max(1);
        let target_timestamp = indexed_lookup_watch_start_timestamp(watch);
        let Some(timestamp_window_from_height) = self
            .first_block_at_or_after_timestamp(target_timestamp, confirmed_tip_height)
            .await?
        else {
            return Ok(fixed_window_from_height);
        };

        Ok(fixed_window_from_height.min(timestamp_window_from_height.max(1)))
    }

    async fn first_block_at_or_after_timestamp(
        &self,
        target_timestamp: u64,
        confirmed_tip_height: u64,
    ) -> Result<Option<u64>> {
        if confirmed_tip_height == 0 {
            return Ok(None);
        }

        let Some(tip_timestamp) = self.block_timestamp_at(confirmed_tip_height).await? else {
            return Ok(None);
        };
        if tip_timestamp < target_timestamp {
            return Ok(Some(confirmed_tip_height));
        }

        let Some(first_timestamp) = self.block_timestamp_at(1).await? else {
            return Ok(None);
        };
        if first_timestamp >= target_timestamp {
            return Ok(Some(1));
        }

        let mut low = 1_u64;
        let mut high = confirmed_tip_height;
        while low < high {
            let mid = low + (high - low) / 2;
            let Some(mid_timestamp) = self.block_timestamp_at(mid).await? else {
                return Ok(None);
            };
            if mid_timestamp >= target_timestamp {
                high = mid;
            } else {
                low = mid.saturating_add(1);
            }
        }

        Ok(Some(low))
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
            let Some(transfer_index) = transaction.transaction_index() else {
                continue;
            };

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
                    sender_addresses: vec![transaction.from().to_string()],
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

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        let confirmed_tip_height = confirmed_tip_height(self.current_tip_cursor().await?.height);
        if watch.source_token == TokenIdentifier::Native {
            return self
                .rpc_indexed_lookup_native_transfer(watch, confirmed_tip_height)
                .await;
        }

        let Some(token_address) = self.watch_erc20_token_address(watch) else {
            return Ok(None);
        };
        let deposit_address = match Address::from_str(&watch.address) {
            Ok(address) => address,
            Err(error) => {
                warn!(
                    backend = self.name,
                    watch_id = %watch.watch_id,
                    address = %watch.address,
                    %error,
                    "Skipping invalid EVM watch address during indexed lookup"
                );
                return Ok(None);
            }
        };

        let mut best = None;
        if let Some(token_indexer) = &self.token_indexer {
            let response = token_indexer
                .get_transfers_to(
                    deposit_address,
                    Some(token_address),
                    Some(watch.min_amount),
                    None,
                )
                .await
                .context(EvmTokenIndexerSnafu)?;

            for transfer in response.transfers {
                let Some(detected) = self.detected_from_indexer_transfer(
                    watch,
                    token_address,
                    deposit_address,
                    confirmed_tip_height,
                    transfer,
                )?
                else {
                    continue;
                };
                if should_replace_detected(best.as_ref(), &detected) {
                    best = Some(detected);
                }
            }
        }

        if best.is_none() {
            best = self
                .rpc_indexed_lookup_erc20_transfer(watch, token_address, confirmed_tip_height)
                .await?;
        }

        Ok(best)
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
        let mut detections = self
            .drain_indexer_candidates(confirmed_height, watches)
            .await?;
        let erc20_watches = self.erc20_watch_map(watches);
        let native_addresses = self.native_address_map(watches);

        if self.token_indexer.is_some() && native_addresses.is_empty() {
            return Ok(BlockScan {
                new_cursor: BlockCursor {
                    height: confirmed_height,
                    hash: confirmed_tip_hash(current_height, confirmed_height, &current_hash, self)
                        .await?,
                },
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
                            sender_addresses: vec![decoded.inner.data.from.to_string()],
                            tx_hash: transaction_hash.to_string(),
                            transfer_index,
                            amount: decoded.inner.data.value,
                            confirmation_state: DepositConfirmationState::Confirmed,
                            block_height: decoded.block_number,
                            block_hash: decoded.block_hash.map(alloy::hex::encode),
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

fn evm_chain_id(chain: ChainType) -> Option<u64> {
    match chain {
        ChainType::Ethereum => Some(1),
        ChainType::Arbitrum => Some(42_161),
        ChainType::Base => Some(8_453),
        ChainType::Bitcoin | ChainType::Hyperliquid => None,
    }
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

fn indexed_lookup_watch_start_timestamp(watch: &WatchEntry) -> u64 {
    let start = watch
        .created_at
        .checked_sub_signed(chrono::Duration::seconds(
            EVM_INDEXED_LOOKUP_WATCH_CREATED_AT_SAFETY_SECONDS,
        ))
        .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC);
    u64::try_from(timestamp_seconds(start)).unwrap_or(0)
}

fn sanitize_url_for_error(value: &str) -> String {
    let Ok(parsed) = Url::parse(value.trim()) else {
        return "<invalid url>".to_string();
    };

    let host = parsed.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", parsed.scheme(), host);
    if let Some(port) = parsed.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if parsed.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if parsed.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    if parsed.fragment().is_some() {
        redacted.push_str("#<redacted-fragment>");
    }
    redacted
}

fn should_replace_detected(
    existing: Option<&DetectedDeposit>,
    candidate: &DetectedDeposit,
) -> bool {
    let candidate_order = (
        candidate.block_height.unwrap_or(u64::MAX),
        candidate.transfer_index,
        candidate.tx_hash.as_str(),
    );
    existing
        .map(|existing| {
            candidate_order
                < (
                    existing.block_height.unwrap_or(u64::MAX),
                    existing.transfer_index,
                    existing.tx_hash.as_str(),
                )
        })
        .unwrap_or(true)
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
    use std::{collections::HashMap, str::FromStr, sync::Arc};

    use alloy::{
        network::{EthereumWallet, TransactionBuilder},
        node_bindings::Anvil,
        primitives::{address, b256, Address, U256},
        providers::{ext::AnvilApi, Provider, ProviderBuilder},
        rpc::types::TransactionRequest,
        signers::local::PrivateKeySigner,
    };
    use chrono::{Duration, Utc};
    use evm_token_indexer_client::{DepositCandidate, TokenIndexerClient, TransferEvent};
    use router_primitives::{ChainType, TokenIdentifier};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use uuid::Uuid;

    use crate::watch::{WatchEntry, WatchTarget};

    use crate::discovery::{DepositConfirmationState, DiscoveryBackend};

    use super::{
        confirmed_tip_height, indexed_lookup_watch_start_timestamp, next_scan_to_height,
        sanitize_url_for_error, EvmErc20DiscoveryBackend, EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS,
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
    fn discovery_init_url_errors_redact_secret_material() {
        let redacted = sanitize_url_for_error(
            "https://rpc-user:rpc-pass@rpc.example/v2/path-secret?api_key=query-secret#fragment-secret",
        );

        assert_eq!(
            redacted,
            "https://rpc.example/<redacted-path>?<redacted-query>#<redacted-fragment>"
        );
        assert!(!redacted.contains("rpc-user"));
        assert!(!redacted.contains("rpc-pass"));
        assert!(!redacted.contains("path-secret"));
        assert!(!redacted.contains("query-secret"));
        assert!(!redacted.contains("fragment-secret"));

        let invalid = sanitize_url_for_error("not a url with secret-token");
        assert_eq!(invalid, "<invalid url>");
        assert!(!invalid.contains("secret-token"));
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
            from_address: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
        let mut watch =
            (*watch_entry(TokenIdentifier::address(token.to_string()), recipient)).clone();
        watch.watch_id = watch_id;
        watch.required_amount = U256::from(42_u64);
        let active_watches = active_watch_map(&[Arc::new(watch)]);

        let detected = backend
            .detected_from_indexer_candidate(&candidate, &active_watches)
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
        assert_eq!(
            detected.sender_addresses,
            vec![address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").to_string()]
        );
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

    #[test]
    fn malformed_token_indexer_candidate_returns_error_for_discard() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut candidate = DepositCandidate {
            id: "candidate-1".to_string(),
            watch_id: "not-a-uuid".to_string(),
            watch_target: "funding_vault".to_string(),
            chain_id: 8453,
            token_address: token,
            from_address: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            deposit_address: recipient,
            amount: "42".to_string(),
            required_amount: "42".to_string(),
            transaction_hash: b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            ),
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

        let error = backend
            .detected_from_indexer_candidate(&candidate, &HashMap::new())
            .expect_err("invalid watch id should be discardable");
        assert!(error.to_string().contains("invalid watch_id"));

        candidate.watch_id = Uuid::new_v4().to_string();
        candidate.watch_target = "bad_target".to_string();
        let error = backend
            .detected_from_indexer_candidate(&candidate, &HashMap::new())
            .expect_err("invalid watch target should be discardable");
        assert!(error.to_string().contains("invalid watch_target"));
    }

    #[test]
    fn token_indexer_candidate_must_match_current_active_watch() {
        let backend = test_backend();
        let watch_id = Uuid::new_v4();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!("2222222222222222222222222222222222222222222222222222222222222222");
        let mut candidate = DepositCandidate {
            id: "candidate-1".to_string(),
            watch_id: watch_id.to_string(),
            watch_target: "funding_vault".to_string(),
            chain_id: 8453,
            token_address: token,
            from_address: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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

        let error = backend
            .detected_from_indexer_candidate(&candidate, &HashMap::new())
            .expect_err("inactive watch candidate should be discardable");
        assert!(error.to_string().contains("no longer active"));

        let mut watch =
            (*watch_entry(TokenIdentifier::address(token.to_string()), recipient)).clone();
        watch.watch_id = watch_id;
        watch.min_amount = U256::from(50_u64);
        let active_watches = active_watch_map(&[Arc::new(watch.clone())]);
        let error = backend
            .detected_from_indexer_candidate(&candidate, &active_watches)
            .expect_err("out-of-bounds candidate should be discardable");
        assert!(error.to_string().contains("outside active watch bounds"));

        watch.min_amount = U256::from(1_u64);
        let other_token = address!("2222222222222222222222222222222222222222");
        watch.source_token = TokenIdentifier::address(other_token.to_string());
        let active_watches = active_watch_map(&[Arc::new(watch)]);
        let error = backend
            .detected_from_indexer_candidate(&candidate, &active_watches)
            .expect_err("wrong token candidate should be discardable");
        assert!(error
            .to_string()
            .contains("does not match active watch token"));

        candidate.chain_id = 1;
        let error = backend
            .detected_from_indexer_candidate(&candidate, &active_watches)
            .expect_err("wrong chain candidate should be discardable");
        assert!(error.to_string().contains("does not match backend chain"));
    }

    #[test]
    fn token_indexer_candidate_matches_active_watch_address_semantically() {
        let backend = test_backend();
        let watch_id = Uuid::new_v4();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("b6f6ea6e56175eeda9265a2c50bb5071d2d5f385");
        let tx_hash = b256!("2222222222222222222222222222222222222222222222222222222222222222");
        let candidate = DepositCandidate {
            id: "candidate-1".to_string(),
            watch_id: watch_id.to_string(),
            watch_target: "funding_vault".to_string(),
            chain_id: 8453,
            token_address: token,
            from_address: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
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
        let mut watch =
            (*watch_entry(TokenIdentifier::address(token.to_string()), recipient)).clone();
        watch.watch_id = watch_id;
        watch.required_amount = U256::from(42_u64);
        watch.address = format!("0x{}", watch.address[2..].to_ascii_uppercase());
        let active_watches = active_watch_map(&[Arc::new(watch)]);

        let detected = backend
            .detected_from_indexer_candidate(&candidate, &active_watches)
            .expect("address formatting differences must not discard the candidate")
            .expect("candidate should map");

        assert_eq!(detected.watch_id, watch_id);
        assert_eq!(Address::from_str(&detected.address).unwrap(), recipient);
    }

    #[test]
    fn maps_confirmed_indexer_transfer_to_detected_deposit() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!("2222222222222222222222222222222222222222222222222222222222222222");
        let watch = watch_entry(TokenIdentifier::address(token.to_string()), recipient);
        let transfer = TransferEvent {
            id: "transfer-1".to_string(),
            amount: "42".to_string(),
            timestamp: 1_700_000_000,
            from: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to: recipient,
            token_address: Some(token),
            transaction_hash: tx_hash,
            block_number: "100".to_string(),
            block_hash: b256!("3333333333333333333333333333333333333333333333333333333333333333"),
            log_index: Some(7),
        };

        let detected = backend
            .detected_from_indexer_transfer(&watch, token, recipient, 100, transfer)
            .expect("transfer should parse")
            .expect("transfer should map");

        assert_eq!(detected.watch_id, watch.watch_id);
        assert_eq!(detected.watch_target, WatchTarget::FundingVault);
        assert_eq!(detected.source_chain, ChainType::Base);
        assert_eq!(
            detected.source_token,
            TokenIdentifier::address(token.to_string())
        );
        assert_eq!(detected.address, recipient.to_string());
        assert_eq!(
            detected.sender_addresses,
            vec![address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").to_string()]
        );
        assert_eq!(detected.tx_hash, tx_hash.to_string());
        assert_eq!(detected.transfer_index, 7);
        assert_eq!(detected.amount, U256::from(42_u64));
        assert_eq!(detected.block_height, Some(100));
        assert_eq!(
            detected.block_hash.as_deref(),
            Some("0x3333333333333333333333333333333333333333333333333333333333333333")
        );
        assert!(detected.indexer_candidate_id.is_none());
    }

    #[test]
    fn indexed_transfer_mapper_requires_confirmed_matching_token_and_amount() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let watch = watch_entry(TokenIdentifier::address(token.to_string()), recipient);
        let transfer = TransferEvent {
            id: "transfer-1".to_string(),
            amount: "42".to_string(),
            timestamp: 1_700_000_000,
            from: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to: recipient,
            token_address: Some(address!("2222222222222222222222222222222222222222")),
            transaction_hash: b256!(
                "3333333333333333333333333333333333333333333333333333333333333333"
            ),
            block_number: "100".to_string(),
            block_hash: b256!("4444444444444444444444444444444444444444444444444444444444444444"),
            log_index: Some(7),
        };

        assert!(backend
            .detected_from_indexer_transfer(&watch, token, recipient, 100, transfer.clone())
            .expect("transfer should parse")
            .is_none());

        let unconfirmed = TransferEvent {
            token_address: Some(token),
            ..transfer.clone()
        };
        assert!(backend
            .detected_from_indexer_transfer(&watch, token, recipient, 99, unconfirmed)
            .expect("transfer should parse")
            .is_none());

        let too_large = TransferEvent {
            token_address: Some(token),
            amount: "1001".to_string(),
            ..transfer
        };
        assert!(backend
            .detected_from_indexer_transfer(&watch, token, recipient, 100, too_large)
            .expect("transfer should parse")
            .is_none());
    }

    #[tokio::test]
    async fn indexed_lookup_finds_recent_native_transfer_for_late_watch() {
        let anvil = Anvil::new().try_spawn().expect("spawn anvil");
        let rpc_url = anvil.endpoint_url();
        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let signer = format!("0x{}", alloy::hex::encode(private_key))
            .parse::<PrivateKeySigner>()
            .expect("parse anvil signer");
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(signer))
            .connect_http(rpc_url.clone());
        let recipient = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let amount = U256::from(42_u64);
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
        provider
            .anvil_mine(Some(1), None)
            .await
            .expect("mine confirmation block");

        let backend = EvmErc20DiscoveryBackend {
            provider: ProviderBuilder::new().connect_http(rpc_url).erased(),
            ..test_backend()
        };
        let watch = watch_entry(TokenIdentifier::Native, recipient);
        let confirmed_tip_height =
            confirmed_tip_height(backend.current_tip_cursor().await.expect("tip").height);

        let detected = backend
            .rpc_indexed_lookup_native_transfer(&watch, confirmed_tip_height)
            .await
            .expect("native indexed lookup")
            .expect("native transfer should be detected");

        assert_eq!(detected.watch_id, watch.watch_id);
        assert_eq!(detected.source_token, TokenIdentifier::Native);
        assert_eq!(detected.address, recipient.to_string());
        assert_eq!(detected.amount, amount);
        assert_eq!(
            detected.sender_addresses,
            vec![anvil.addresses()[0].to_string()]
        );
        assert_eq!(
            detected.confirmation_state,
            DepositConfirmationState::Confirmed
        );
    }

    #[tokio::test]
    async fn indexed_lookup_finds_native_transfer_outside_fixed_backfill_window() {
        let anvil = Anvil::new().try_spawn().expect("spawn anvil");
        let rpc_url = anvil.endpoint_url();
        let private_key: [u8; 32] = anvil.keys()[0].clone().to_bytes().into();
        let signer = format!("0x{}", alloy::hex::encode(private_key))
            .parse::<PrivateKeySigner>()
            .expect("parse anvil signer");
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::new(signer))
            .connect_http(rpc_url.clone());
        let recipient = address!("cccccccccccccccccccccccccccccccccccccccc");
        let amount = U256::from(42_u64);
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
        provider
            .anvil_mine(Some(EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS + 10), None)
            .await
            .expect("mine enough blocks to move transfer outside fixed backfill");

        let backend = EvmErc20DiscoveryBackend {
            provider: ProviderBuilder::new().connect_http(rpc_url).erased(),
            ..test_backend()
        };
        let watch = watch_entry(TokenIdentifier::Native, recipient);
        let confirmed_tip_height =
            confirmed_tip_height(backend.current_tip_cursor().await.expect("tip").height);
        let transfer_block = receipt
            .block_number
            .expect("native transfer receipt should include block number");
        let fixed_window_from_height = confirmed_tip_height
            .saturating_sub(EVM_INDEXED_LOOKUP_RPC_BACKFILL_BLOCKS)
            .max(1);
        assert!(
            transfer_block < fixed_window_from_height,
            "test setup must put transfer outside the old fixed backfill window"
        );

        let detected = backend
            .rpc_indexed_lookup_native_transfer(&watch, confirmed_tip_height)
            .await
            .expect("native indexed lookup")
            .expect("native transfer should be detected");

        assert_eq!(detected.watch_id, watch.watch_id);
        assert_eq!(detected.amount, amount);
        assert_eq!(detected.block_height, Some(transfer_block));
    }

    #[test]
    fn indexed_lookup_watch_start_timestamp_allows_clock_skew() {
        let recipient = address!("cccccccccccccccccccccccccccccccccccccccc");
        let mut watch = (*watch_entry(TokenIdentifier::Native, recipient)).clone();
        watch.created_at = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();

        assert_eq!(
            indexed_lookup_watch_start_timestamp(&watch),
            1_700_000_000 - 300
        );
    }

    #[tokio::test]
    async fn token_indexer_only_scan_advances_confirmed_cursor_without_rpc_log_scan() {
        let anvil = Anvil::new().try_spawn().expect("spawn anvil");
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        provider
            .anvil_mine(Some(3), None)
            .await
            .expect("mine confirmed blocks");
        let current_height = provider
            .get_block_number()
            .await
            .expect("read anvil block number");
        let confirmed_height = confirmed_tip_height(current_height);
        assert!(confirmed_height > 0);

        let token_indexer_url = spawn_empty_token_indexer().await;
        let backend = EvmErc20DiscoveryBackend {
            provider: ProviderBuilder::new()
                .connect_http(anvil.endpoint_url())
                .erased(),
            token_indexer: Some(
                TokenIndexerClient::new(&token_indexer_url).expect("token indexer client"),
            ),
            ..test_backend()
        };
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let watches = vec![watch_entry(
            TokenIdentifier::address(token.to_string()),
            recipient,
        )];

        let scan = backend
            .scan_new_blocks(
                &super::BlockCursor {
                    height: 0,
                    hash: String::new(),
                },
                &watches,
            )
            .await
            .expect("scan token-indexer-only backend");

        assert_eq!(scan.new_cursor.height, confirmed_height);
        assert!(!scan.new_cursor.hash.is_empty());
        assert!(scan.detections.is_empty());
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

    fn active_watch_map(watches: &[Arc<WatchEntry>]) -> HashMap<Uuid, Arc<WatchEntry>> {
        watches
            .iter()
            .map(|watch| (watch.watch_id, watch.clone()))
            .collect()
    }

    async fn spawn_empty_token_indexer() -> String {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind token indexer mock");
        let address = listener.local_addr().expect("token indexer mock address");

        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.expect("accept token indexer mock");
                let mut buffer = [0_u8; 4096];
                let read = socket
                    .read(&mut buffer)
                    .await
                    .expect("read token indexer mock request");
                let request = String::from_utf8_lossy(&buffer[..read]);
                let body = if request.starts_with("GET /candidates/pending") {
                    r#"{"candidates":[]}"#
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket
                    .write_all(response.as_bytes())
                    .await
                    .expect("write token indexer mock response");
            }
        });

        format!("http://{address}/")
    }
}
