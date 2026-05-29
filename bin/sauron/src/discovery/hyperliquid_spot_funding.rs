use std::{collections::HashSet, str::FromStr, time::Duration};

use alloy::primitives::{Address, U256};
use async_trait::async_trait;
use chrono::Utc;
use hl_shim_client::{HlShimClient, HlTransferEvent, HlTransferKind};
use router_primitives::{ChainType, TokenIdentifier};
use tracing::warn;

use crate::{
    config::SauronArgs,
    discovery::{
        BlockCursor, BlockScan, DepositConfirmationState, DetectedDeposit, DiscoveryBackend,
    },
    error::{Error, Result},
    watch::{SharedWatchEntry, WatchEntry},
};

const HYPERLIQUID_FUNDING_LOOKBACK_MS: i64 = 60_000;
const HYPERLIQUID_TRANSFER_LOOKUP_LIMIT: u32 = 2_000;

pub struct HyperliquidSpotFundingDiscoveryBackend {
    client: HlShimClient,
    poll_interval: Duration,
    indexed_lookup_concurrency: usize,
}

impl HyperliquidSpotFundingDiscoveryBackend {
    pub fn new(client: HlShimClient, args: &SauronArgs) -> Self {
        Self {
            client,
            poll_interval: Duration::from_millis(args.sauron_hu_poll_fast_millis),
            indexed_lookup_concurrency: args.sauron_hyperliquid_observer_concurrency,
        }
    }
}

#[async_trait]
impl DiscoveryBackend for HyperliquidSpotFundingDiscoveryBackend {
    fn name(&self) -> &'static str {
        "hyperliquid_spot_funding"
    }

    fn chain(&self) -> ChainType {
        ChainType::Hyperliquid
    }

    fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    fn indexed_lookup_concurrency(&self) -> usize {
        self.indexed_lookup_concurrency
    }

    async fn sync_watches(&self, watches: &[SharedWatchEntry]) -> Result<()> {
        let mut registered = HashSet::with_capacity(watches.len());
        for watch in watches {
            let user =
                Address::from_str(&watch.address).map_err(|source| Error::InvalidWatchRow {
                    message: format!(
                        "watch {} had invalid Hyperliquid address: {source}",
                        watch.watch_id
                    ),
                })?;
            if registered.insert(user) {
                self.client
                    .watch_user(user)
                    .await
                    .map_err(|source| Error::HlShim { source })?;
            }
        }
        Ok(())
    }
    async fn indexed_lookup(&self, watch: &WatchEntry) -> Result<Option<DetectedDeposit>> {
        lookup_hyperliquid_spot_deposit(&self.client, watch).await
    }

    async fn current_cursor(&self) -> Result<BlockCursor> {
        Ok(BlockCursor {
            height: 0,
            hash: "hyperliquid-shim".to_string(),
        })
    }

    async fn scan_new_blocks(
        &self,
        _from_exclusive: &BlockCursor,
        watches: &[SharedWatchEntry],
    ) -> Result<BlockScan> {
        let mut detections = Vec::new();
        for watch in watches {
            match lookup_hyperliquid_spot_deposit(&self.client, watch.as_ref()).await {
                Ok(Some(detected)) => detections.push(detected),
                Ok(None) => {}
                Err(error) => warn!(
                    watch_id = %watch.watch_id,
                    %error,
                    "Hyperliquid spot funding lookup failed for active watch"
                ),
            }
        }
        Ok(BlockScan {
            new_cursor: BlockCursor {
                height: 0,
                hash: "hyperliquid-shim".to_string(),
            },
            detections,
        })
    }
}

async fn lookup_hyperliquid_spot_deposit(
    client: &HlShimClient,
    watch: &WatchEntry,
) -> Result<Option<DetectedDeposit>> {
    let recipient = Address::from_str(&watch.address).map_err(|source| Error::InvalidWatchRow {
        message: format!(
            "watch {} had invalid Hyperliquid address: {source}",
            watch.watch_id
        ),
    })?;
    let expected_asset = hyperliquid_event_asset(&watch.source_asset_id)?;
    let from_time = watch
        .created_at
        .timestamp_millis()
        .saturating_sub(HYPERLIQUID_FUNDING_LOOKBACK_MS)
        .max(0);
    let page = client
        .transfers(
            recipient,
            Some(from_time),
            Some(HYPERLIQUID_TRANSFER_LOOKUP_LIMIT),
            None,
        )
        .await
        .map_err(|source| Error::HlShim { source })?;

    let mut best: Option<HlTransferEvent> = None;
    for event in page.events {
        if !hyperliquid_event_matches_watch(&event, watch, recipient, expected_asset)? {
            continue;
        }
        if best
            .as_ref()
            .is_none_or(|current| event.time_ms > current.time_ms)
        {
            best = Some(event);
        }
    }

    Ok(best.map(|event| detected_from_hyperliquid_event(watch, event, expected_asset)))
}

fn hyperliquid_event_matches_watch(
    event: &HlTransferEvent,
    watch: &WatchEntry,
    recipient: Address,
    expected_asset: &str,
) -> Result<bool> {
    if event.market.as_db_str() != Some("spot") || !event.asset.eq_ignore_ascii_case(expected_asset)
    {
        return Ok(false);
    }
    if event.time_ms
        < watch
            .created_at
            .timestamp_millis()
            .saturating_sub(HYPERLIQUID_FUNDING_LOOKBACK_MS)
    {
        return Ok(false);
    }
    if !matches_hyperliquid_spot_destination(event, recipient) {
        return Ok(false);
    }
    let amount = decimal_to_raw_floor(
        event.amount_delta.as_str(),
        hyperliquid_asset_decimals(&watch.source_asset_id)?,
    )?;
    Ok(amount >= watch.min_amount && amount <= watch.max_amount)
}

fn matches_hyperliquid_spot_destination(event: &HlTransferEvent, recipient: Address) -> bool {
    match &event.kind {
        HlTransferKind::SpotTransfer { destination, .. } => {
            Address::from_str(destination).is_ok_and(|destination| destination == recipient)
        }
        _ => false,
    }
}

fn detected_from_hyperliquid_event(
    watch: &WatchEntry,
    event: HlTransferEvent,
    _expected_asset: &str,
) -> DetectedDeposit {
    let transfer_index = match event.kind {
        HlTransferKind::SpotTransfer { nonce, .. } => nonce,
        _ => event.time_ms.max(0) as u64,
    };
    let amount = decimal_to_raw_floor(
        event.amount_delta.as_str(),
        hyperliquid_asset_decimals(&watch.source_asset_id).unwrap_or(0),
    )
    .unwrap_or(U256::ZERO);
    DetectedDeposit {
        watch_target: watch.watch_target,
        watch_id: watch.watch_id,
        execution_step_id: watch.execution_step_id,
        source_chain: ChainType::Hyperliquid,
        source_token: TokenIdentifier::Native,
        source_asset_id: watch.source_asset_id.clone(),
        address: watch.address.clone(),
        sender_addresses: vec![format!("{:#x}", event.user)],
        tx_hash: event.hash,
        transfer_index,
        amount,
        confirmation_state: DepositConfirmationState::Confirmed,
        block_height: None,
        block_hash: None,
        observed_at: Utc::now(),
        indexer_candidate_id: None,
    }
}

fn hyperliquid_event_asset(asset_id: &str) -> Result<&'static str> {
    match asset_id {
        "native" => Ok("USDC"),
        "UBTC" => Ok("UBTC"),
        "USDT" => Ok("USDT0"),
        "UETH" => Ok("UETH"),
        "HYPE" => Ok("HYPE"),
        other => Err(Error::InvalidWatchRow {
            message: format!("unsupported Hyperliquid spot asset_id {other}"),
        }),
    }
}

fn hyperliquid_asset_decimals(asset_id: &str) -> Result<u8> {
    match asset_id {
        "native" => Ok(6),
        "UBTC" => Ok(8),
        "USDT" => Ok(6),
        "UETH" => Ok(18),
        "HYPE" => Ok(8),
        other => Err(Error::InvalidWatchRow {
            message: format!("unsupported Hyperliquid spot asset_id {other}"),
        }),
    }
}

fn decimal_to_raw_floor(value: &str, decimals: u8) -> Result<U256> {
    let value = value.trim();
    if value.starts_with('-') {
        return Err(Error::InvalidWatchRow {
            message: format!("negative Hyperliquid transfer amount {value}"),
        });
    }
    let mut parts = value.split('.');
    let whole = parts.next().unwrap_or("0");
    let fractional = parts.next().unwrap_or("");
    if parts.next().is_some()
        || whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(Error::InvalidWatchRow {
            message: format!("invalid Hyperliquid decimal amount {value}"),
        });
    }
    let mut digits = whole.trim_start_matches('0').to_string();
    let decimal_places = decimals as usize;
    if decimal_places > 0 {
        let kept = fractional
            .get(..fractional.len().min(decimal_places))
            .unwrap_or("");
        digits.push_str(kept);
        for _ in kept.len()..decimal_places {
            digits.push('0');
        }
    }
    if digits.is_empty() {
        return Ok(U256::ZERO);
    }
    U256::from_str_radix(&digits, 10).map_err(|source| Error::InvalidWatchRow {
        message: format!("Hyperliquid amount {value} exceeds uint256: {source}"),
    })
}

#[cfg(test)]
mod tests {
    use super::{decimal_to_raw_floor, hyperliquid_event_asset, hyperliquid_event_matches_watch};
    use crate::watch::{WatchEntry, WatchTarget};
    use alloy::primitives::{Address, U256};
    use chrono::{Duration, Utc};
    use hl_shim_client::HlTransferEvent;
    use router_primitives::{ChainType, TokenIdentifier};
    use router_temporal::WorkflowStepId;
    use serde_json::json;
    use std::str::FromStr;
    use uuid::Uuid;

    #[test]
    fn maps_public_usdt_to_hyperliquid_usdt0() {
        assert_eq!(hyperliquid_event_asset("USDT").unwrap(), "USDT0");
    }
    #[test]
    fn maps_hype_to_hyperliquid_hype() {
        assert_eq!(hyperliquid_event_asset("HYPE").unwrap(), "HYPE");
    }

    #[test]
    fn floors_hyperliquid_decimal_amounts_to_router_raw_units() {
        assert_eq!(
            decimal_to_raw_floor("1.23456789", 6).unwrap(),
            U256::from(1_234_567_u64)
        );
        assert_eq!(
            decimal_to_raw_floor("0.0000098941", 8).unwrap(),
            U256::from(989_u64)
        );
    }

    #[test]
    fn matches_hyperliquid_spot_transfer_to_watch_address_and_asset() {
        let watch_id = Uuid::now_v7();
        let address = "0x1111111111111111111111111111111111111111";
        let now = Utc::now();
        let watch = WatchEntry {
            watch_target: WatchTarget::FundingVault,
            watch_id,
            execution_step_id: Some(WorkflowStepId::from(watch_id)),
            order_id: Uuid::now_v7(),
            source_chain: ChainType::Hyperliquid,
            source_token: TokenIdentifier::Native,
            source_asset_id: "USDT".to_string(),
            address: address.to_string(),
            min_amount: U256::from(1_000_000_u64),
            max_amount: U256::from(2_000_000_u64),
            required_amount: U256::from(1_000_000_u64),
            deposit_deadline: now + Duration::minutes(10),
            created_at: now,
            updated_at: now,
        };
        let event: HlTransferEvent = serde_json::from_value(json!({
            "user": "0x2222222222222222222222222222222222222222",
            "time_ms": now.timestamp_millis(),
            "kind": {
                "type": "spot_transfer",
                "destination": address,
                "native_token_fee": "0.0",
                "nonce": 42,
                "usdc_value": "1.5"
            },
            "asset": "USDT0",
            "market": "spot",
            "amount_delta": "1.5",
            "fee": null,
            "fee_token": null,
            "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "observed_at_ms": now.timestamp_millis()
        }))
        .expect("valid HL transfer event");

        assert!(hyperliquid_event_matches_watch(
            &event,
            &watch,
            Address::from_str(address).unwrap(),
            "USDT0"
        )
        .unwrap());
    }
}
