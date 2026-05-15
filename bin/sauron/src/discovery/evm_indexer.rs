use std::{
    collections::{BTreeSet, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use chrono::Utc;
use evm_indexer_client::{
    EvmIndexerClient, EvmTransfer, EvmTransferFilter, FilterStream, TransferQuery,
};
use router_primitives::{ChainType, TokenIdentifier};
use snafu::ResultExt;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tracing::{debug, warn};
use uuid::Uuid;

use crate::{
    config::SauronArgs,
    discovery::{
        BlockCursor, BlockScan, DepositConfirmationState, DetectedDeposit, DiscoveryBackend,
    },
    error::{Error, EvmIndexerSnafu, Result},
    watch::{SharedWatchEntry, WatchEntry, WatchTarget},
};

const EVM_INDEXER_DISCOVERY_POLL_INTERVAL: Duration = Duration::from_secs(5);
const EVM_INDEXER_TRANSFER_PAGE_LIMIT: u32 = 1_000;
const EVM_INDEXER_PENDING_DETECTIONS: usize = 16_384;

type WatchBuckets = HashMap<(Address, Address), Vec<EvmWatchSnapshot>>;

pub struct EvmIndexerDiscoveryBackend {
    name: &'static str,
    chain_type: ChainType,
    chain_id: u64,
    client: EvmIndexerClient,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
    subscription: Mutex<SubscriptionState>,
    pending_tx: mpsc::Sender<DetectedDeposit>,
    pending_rx: Mutex<mpsc::Receiver<DetectedDeposit>>,
}

#[derive(Default)]
struct SubscriptionState {
    key: Option<String>,
    task: Option<JoinHandle<()>>,
}

#[derive(Clone)]
struct SubscriptionPlan {
    key: String,
    filter: EvmTransferFilter,
    matcher: Arc<EvmTransferMatcher>,
}

#[derive(Clone)]
struct EvmTransferMatcher {
    chain_type: ChainType,
    chain_id: u64,
    buckets: WatchBuckets,
}

#[derive(Debug, Clone)]
struct EvmWatchSnapshot {
    watch_target: WatchTarget,
    watch_id: Uuid,
    execution_step_id: Option<router_temporal::WorkflowStepId>,
    source_token: TokenIdentifier,
    address: String,
    token_address: Address,
    deposit_address: Address,
    min_amount: U256,
    max_amount: U256,
    created_at_seconds: u64,
    expires_at_seconds: u64,
}

impl EvmIndexerDiscoveryBackend {
    pub fn new_ethereum(args: &SauronArgs) -> Result<Self> {
        let url = required_indexer_url("ethereum_evm", args.ethereum_token_indexer_url.as_deref())?;
        Self::new(
            "ethereum_evm",
            ChainType::Ethereum,
            url,
            args.token_indexer_api_key.clone(),
            args.sauron_evm_indexed_lookup_concurrency,
        )
    }

    pub fn new_base(args: &SauronArgs) -> Result<Self> {
        let url = required_indexer_url("base_evm", args.base_token_indexer_url.as_deref())?;
        Self::new(
            "base_evm",
            ChainType::Base,
            url,
            args.token_indexer_api_key.clone(),
            args.sauron_evm_indexed_lookup_concurrency,
        )
    }

    pub fn new_arbitrum(args: &SauronArgs) -> Result<Self> {
        let url = required_indexer_url("arbitrum_evm", args.arbitrum_token_indexer_url.as_deref())?;
        Self::new(
            "arbitrum_evm",
            ChainType::Arbitrum,
            url,
            args.token_indexer_api_key.clone(),
            args.sauron_evm_indexed_lookup_concurrency,
        )
    }

    fn new(
        name: &'static str,
        chain_type: ChainType,
        indexer_url: &str,
        api_key: Option<String>,
        indexed_lookup_concurrency: usize,
    ) -> Result<Self> {
        let chain_id = evm_chain_id(chain_type).ok_or_else(|| Error::DiscoveryBackendInit {
            backend: name.to_string(),
            message: format!(
                "chain {} is not supported by the EVM token indexer",
                chain_type.to_db_string()
            ),
        })?;
        let client =
            EvmIndexerClient::new_with_api_key(indexer_url, api_key).map_err(|source| {
                Error::DiscoveryBackendInit {
                    backend: name.to_string(),
                    message: format!("invalid EVM indexer URL: {source}"),
                }
            })?;
        let (pending_tx, pending_rx) = mpsc::channel(EVM_INDEXER_PENDING_DETECTIONS);

        Ok(Self {
            name,
            chain_type,
            chain_id,
            client,
            poll_interval: EVM_INDEXER_DISCOVERY_POLL_INTERVAL,
            indexed_lookup_concurrency,
            subscription: Mutex::new(SubscriptionState::default()),
            pending_tx,
            pending_rx: Mutex::new(pending_rx),
        })
    }

    pub fn maybe_ethereum(args: &SauronArgs) -> Result<Option<Self>> {
        args.ethereum_token_indexer_url
            .as_ref()
            .map(|_| Self::new_ethereum(args))
            .transpose()
    }

    pub fn maybe_base(args: &SauronArgs) -> Result<Option<Self>> {
        args.base_token_indexer_url
            .as_ref()
            .map(|_| Self::new_base(args))
            .transpose()
    }

    pub fn maybe_arbitrum(args: &SauronArgs) -> Result<Option<Self>> {
        args.arbitrum_token_indexer_url
            .as_ref()
            .map(|_| Self::new_arbitrum(args))
            .transpose()
    }

    fn watch_snapshot(&self, watch: &WatchEntry) -> Option<EvmWatchSnapshot> {
        let token_address = match &watch.source_token {
            TokenIdentifier::Native => return None,
            TokenIdentifier::Address(token_address) => match Address::from_str(token_address) {
                Ok(address) => address,
                Err(error) => {
                    warn!(
                        backend = self.name,
                        watch_id = %watch.watch_id,
                        token = %token_address,
                        %error,
                        "Skipping EVM indexer watch with invalid token address"
                    );
                    return None;
                }
            },
        };
        let deposit_address = match Address::from_str(&watch.address) {
            Ok(address) => address,
            Err(error) => {
                warn!(
                    backend = self.name,
                    watch_id = %watch.watch_id,
                    address = %watch.address,
                    %error,
                    "Skipping EVM indexer watch with invalid deposit address"
                );
                return None;
            }
        };

        Some(EvmWatchSnapshot {
            watch_target: watch.watch_target,
            watch_id: watch.watch_id,
            execution_step_id: watch.execution_step_id,
            source_token: watch.source_token.clone(),
            address: watch.address.clone(),
            token_address,
            deposit_address,
            min_amount: watch.min_amount,
            max_amount: watch.max_amount,
            created_at_seconds: timestamp_seconds(watch.created_at),
            expires_at_seconds: timestamp_seconds(watch.deposit_deadline),
        })
    }

    fn subscription_plan(&self, watches: &[SharedWatchEntry]) -> Option<SubscriptionPlan> {
        let mut token_addresses = BTreeSet::new();
        let mut recipient_addresses = BTreeSet::new();
        let mut buckets: WatchBuckets = HashMap::new();
        let mut min_amount = None::<U256>;
        let mut max_amount = None::<U256>;

        for watch in watches {
            let Some(snapshot) = self.watch_snapshot(watch.as_ref()) else {
                continue;
            };
            min_amount = Some(
                min_amount
                    .map(|current| current.min(snapshot.min_amount))
                    .unwrap_or(snapshot.min_amount),
            );
            max_amount = Some(
                max_amount
                    .map(|current| current.max(snapshot.max_amount))
                    .unwrap_or(snapshot.max_amount),
            );
            token_addresses.insert(snapshot.token_address);
            recipient_addresses.insert(snapshot.deposit_address);
            buckets
                .entry((snapshot.token_address, snapshot.deposit_address))
                .or_default()
                .push(snapshot);
        }

        if token_addresses.is_empty() || recipient_addresses.is_empty() {
            return None;
        }

        let token_addresses = token_addresses.into_iter().collect::<Vec<_>>();
        let recipient_addresses = recipient_addresses.into_iter().collect::<Vec<_>>();
        let key = subscription_key(
            &token_addresses,
            &recipient_addresses,
            min_amount,
            max_amount,
        );
        Some(SubscriptionPlan {
            key,
            filter: (token_addresses, recipient_addresses, min_amount, max_amount),
            matcher: Arc::new(EvmTransferMatcher {
                chain_type: self.chain_type,
                chain_id: self.chain_id,
                buckets,
            }),
        })
    }

    async fn start_subscription(&self, plan: SubscriptionPlan) {
        let mut state = self.subscription.lock().await;
        let task_is_live = state.task.as_ref().is_some_and(|task| !task.is_finished());
        if state.key.as_deref() == Some(plan.key.as_str()) && task_is_live {
            return;
        }

        if let Some(task) = state.task.take() {
            task.abort();
        }

        let client = self.client.clone();
        let pending_tx = self.pending_tx.clone();
        let backend_name = self.name;
        let task = tokio::spawn(async move {
            run_subscription_task(backend_name, client, plan.filter, plan.matcher, pending_tx)
                .await;
        });
        state.key = Some(plan.key);
        state.task = Some(task);
    }

    async fn stop_subscription(&self) {
        let mut state = self.subscription.lock().await;
        state.key = None;
        if let Some(task) = state.task.take() {
            task.abort();
        }
    }

    async fn lookup_transfer_for_snapshot(
        &self,
        snapshot: &EvmWatchSnapshot,
    ) -> Result<Option<DetectedDeposit>> {
        let mut query = TransferQuery::new(snapshot.deposit_address);
        query.token = Some(snapshot.token_address);
        query.min_amount = Some(snapshot.min_amount);
        query.max_amount = Some(snapshot.max_amount);
        query.limit = Some(EVM_INDEXER_TRANSFER_PAGE_LIMIT);

        let matcher = EvmTransferMatcher {
            chain_type: self.chain_type,
            chain_id: self.chain_id,
            buckets: HashMap::new(),
        };
        let mut best = None;
        loop {
            let page = self
                .client
                .transfers(query.clone())
                .await
                .context(EvmIndexerSnafu)?;

            for transfer in page.transfers {
                let Some(detected) =
                    matcher.detected_from_transfer_for_snapshot(snapshot, &transfer)
                else {
                    continue;
                };
                if should_replace_detected(best.as_ref(), &detected) {
                    best = Some(detected);
                }
            }

            if !page.has_more {
                break;
            }
            let Some(cursor) = page.next_cursor else {
                warn!(
                    backend = self.name,
                    watch_id = %snapshot.watch_id,
                    "EVM indexer indicated more transfer pages without a cursor"
                );
                break;
            };
            query.cursor = Some(cursor);
        }

        Ok(best)
    }
}

#[async_trait]
impl DiscoveryBackend for EvmIndexerDiscoveryBackend {
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
        match self.subscription_plan(watches) {
            Some(plan) => self.start_subscription(plan).await,
            None => self.stop_subscription().await,
        }
        Ok(())
    }

    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        let Some(snapshot) = self.watch_snapshot(watch) else {
            return Ok(None);
        };
        self.lookup_transfer_for_snapshot(&snapshot).await
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        Ok(evm_indexer_cursor(0))
    }

    async fn scan_new_blocks(
        &self,
        from_exclusive: &BlockCursor,
        _watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        let mut rx = self.pending_rx.lock().await;
        let mut detections = Vec::new();
        while let Ok(detected) = rx.try_recv() {
            detections.push(detected);
        }
        let height = detections
            .iter()
            .filter_map(|detected| detected.block_height)
            .max()
            .unwrap_or(from_exclusive.height);

        Ok(BlockScan {
            new_cursor: evm_indexer_cursor(height),
            detections,
        })
    }
}

impl EvmTransferMatcher {
    fn detections_from_transfer(&self, transfer: &EvmTransfer) -> Vec<DetectedDeposit> {
        if transfer.chain_id != self.chain_id {
            return Vec::new();
        }
        self.buckets
            .get(&(transfer.token_address, transfer.to_address))
            .into_iter()
            .flat_map(|watches| watches.iter())
            .filter_map(|watch| self.detected_from_transfer_for_snapshot(watch, transfer))
            .collect()
    }

    fn detected_from_transfer_for_snapshot(
        &self,
        watch: &EvmWatchSnapshot,
        transfer: &EvmTransfer,
    ) -> Option<DetectedDeposit> {
        if transfer.chain_id != self.chain_id
            || transfer.token_address != watch.token_address
            || transfer.to_address != watch.deposit_address
            || transfer.amount < watch.min_amount
            || transfer.amount > watch.max_amount
            || transfer.block_timestamp < watch.created_at_seconds
            || transfer.block_timestamp > watch.expires_at_seconds
        {
            return None;
        }

        Some(DetectedDeposit {
            watch_target: watch.watch_target,
            watch_id: watch.watch_id,
            execution_step_id: watch.execution_step_id,
            source_chain: self.chain_type,
            source_token: watch.source_token.clone(),
            address: watch.address.clone(),
            sender_addresses: vec![transfer.from_address.to_string()],
            tx_hash: transfer.transaction_hash.to_string(),
            transfer_index: transfer.log_index,
            amount: transfer.amount,
            confirmation_state: DepositConfirmationState::Confirmed,
            block_height: Some(transfer.block_number),
            block_hash: Some(transfer.block_hash.to_string()),
            observed_at: Utc::now(),
            indexer_candidate_id: None,
        })
    }
}

async fn run_subscription_task(
    backend_name: &'static str,
    client: EvmIndexerClient,
    filter: EvmTransferFilter,
    matcher: Arc<EvmTransferMatcher>,
    pending_tx: mpsc::Sender<DetectedDeposit>,
) {
    let Ok(mut transfers) = client.stream_filter(filter).await.map_err(|error| {
        warn!(
            backend = backend_name,
            %error,
            "EVM indexer transfer subscription failed to start"
        );
    }) else {
        return;
    };

    while let Some(event) = transfers.recv().await {
        match event {
            Ok(transfer) => {
                for detected in matcher.detections_from_transfer(&transfer) {
                    if pending_tx.send(detected).await.is_err() {
                        debug!(
                            backend = backend_name,
                            "EVM indexer subscription receiver was dropped"
                        );
                        return;
                    }
                }
            }
            Err(error) => {
                warn!(
                    backend = backend_name,
                    %error,
                    "EVM indexer transfer subscription returned an error"
                );
                return;
            }
        }
    }

    warn!(
        backend = backend_name,
        "EVM indexer transfer subscription ended"
    );
}

fn required_indexer_url<'a>(backend: &'static str, value: Option<&'a str>) -> Result<&'a str> {
    value.ok_or_else(|| Error::DiscoveryBackendInit {
        backend: backend.to_string(),
        message: "EVM token indexer URL is not configured".to_string(),
    })
}

fn evm_chain_id(chain: ChainType) -> Option<u64> {
    match chain {
        ChainType::Ethereum => Some(1),
        ChainType::Arbitrum => Some(42_161),
        ChainType::Base => Some(8_453),
        ChainType::Bitcoin | ChainType::Hyperliquid => None,
    }
}

fn evm_indexer_cursor(height: u64) -> BlockCursor {
    BlockCursor {
        height,
        hash: "evm-indexer".to_string(),
    }
}

fn timestamp_seconds(timestamp: chrono::DateTime<Utc>) -> u64 {
    u64::try_from(timestamp.timestamp().max(0)).unwrap_or(0)
}

fn subscription_key(
    tokens: &[Address],
    recipients: &[Address],
    min_amount: Option<U256>,
    max_amount: Option<U256>,
) -> String {
    format!(
        "tokens={};recipients={};min={};max={}",
        tokens
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
        recipients
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(","),
        min_amount
            .map(|amount| amount.to_string())
            .unwrap_or_else(|| "*".to_string()),
        max_amount
            .map(|amount| amount.to_string())
            .unwrap_or_else(|| "*".to_string())
    )
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use alloy::primitives::{address, b256};
    use chrono::{Duration, Utc};
    use router_temporal::WorkflowStepId;

    use super::*;

    #[test]
    fn subscription_plan_groups_erc20_watches_and_excludes_native() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let native_recipient = address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let watches = vec![
            watch_entry(TokenIdentifier::address(token.to_string()), recipient),
            watch_entry(TokenIdentifier::Native, native_recipient),
        ];

        let plan = backend
            .subscription_plan(&watches)
            .expect("ERC-20 watch should produce a subscription plan");

        assert_eq!(plan.filter.0, vec![token]);
        assert_eq!(plan.filter.1, vec![recipient]);
        assert_eq!(
            plan.matcher.buckets.get(&(token, recipient)).map(Vec::len),
            Some(1)
        );
    }

    #[test]
    fn transfer_mapper_requires_token_recipient_amount_and_watch_window() {
        let backend = test_backend();
        let token = address!("1111111111111111111111111111111111111111");
        let recipient = address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let watch = backend
            .watch_snapshot(&watch_entry(
                TokenIdentifier::address(token.to_string()),
                recipient,
            ))
            .expect("valid watch");
        let matcher = EvmTransferMatcher {
            chain_type: ChainType::Base,
            chain_id: 8_453,
            buckets: HashMap::new(),
        };
        let transfer = EvmTransfer {
            id: "8453:tx:7".to_string(),
            chain_id: 8_453,
            token_address: token,
            from_address: address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            to_address: recipient,
            amount: U256::from(42_u64),
            transaction_hash: b256!(
                "2222222222222222222222222222222222222222222222222222222222222222"
            ),
            block_number: 100,
            block_hash: b256!("3333333333333333333333333333333333333333333333333333333333333333"),
            log_index: 7,
            block_timestamp: watch.created_at_seconds,
        };

        let detected = matcher
            .detected_from_transfer_for_snapshot(&watch, &transfer)
            .expect("transfer should match watch");

        assert_eq!(detected.watch_target, WatchTarget::FundingVault);
        assert_eq!(detected.source_chain, ChainType::Base);
        assert_eq!(
            detected.source_token,
            TokenIdentifier::address(token.to_string())
        );
        assert_eq!(detected.address, recipient.to_string());
        assert_eq!(detected.amount, U256::from(42_u64));
        assert_eq!(detected.transfer_index, 7);
        assert_eq!(detected.block_height, Some(100));

        let wrong_recipient = EvmTransfer {
            to_address: address!("cccccccccccccccccccccccccccccccccccccccc"),
            ..transfer.clone()
        };
        assert!(matcher
            .detected_from_transfer_for_snapshot(&watch, &wrong_recipient)
            .is_none());

        let too_early = EvmTransfer {
            block_timestamp: watch.created_at_seconds.saturating_sub(1),
            ..transfer
        };
        assert!(matcher
            .detected_from_transfer_for_snapshot(&watch, &too_early)
            .is_none());
    }

    fn test_backend() -> EvmIndexerDiscoveryBackend {
        EvmIndexerDiscoveryBackend::new("test_evm", ChainType::Base, "http://127.0.0.1:1", None, 1)
            .expect("test backend")
    }

    fn watch_entry(token: TokenIdentifier, address: Address) -> Arc<WatchEntry> {
        Arc::new(WatchEntry {
            watch_target: WatchTarget::FundingVault,
            watch_id: Uuid::new_v4(),
            execution_step_id: Some(WorkflowStepId::from(Uuid::new_v4())),
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
