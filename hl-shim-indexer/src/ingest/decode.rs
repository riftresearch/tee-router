use alloy::primitives::Address;
use chrono::Utc;
use hyperliquid_client::{
    UserFill, UserFunding, UserNonFundingLedgerDelta, UserNonFundingLedgerUpdate,
};
use tracing::warn;

use crate::{
    ingest::schema::{DecimalString, HlMarket, HlTransferEvent, HlTransferKind},
    metadata::MetadataSnapshot,
    telemetry, Result,
};

const ZERO_HASH: &str = "0x0000000000000000000000000000000000000000000000000000000000000000";

pub fn decode_fills(
    user: Address,
    fills: Vec<UserFill>,
    metadata: &MetadataSnapshot,
) -> Result<Vec<HlTransferEvent>> {
    fills
        .into_iter()
        .map(|fill| decode_fill(user, fill, metadata))
        .collect()
}

pub fn decode_funding(user: Address, fundings: Vec<UserFunding>) -> Result<Vec<HlTransferEvent>> {
    fundings
        .into_iter()
        .map(|funding| {
            let observed_at_ms = now_ms();
            Ok(HlTransferEvent {
                user,
                time_ms: i64::try_from(funding.time).unwrap_or(i64::MAX),
                kind: HlTransferKind::Funding {
                    szi: funding.szi,
                    funding_rate: funding.funding_rate,
                },
                asset: funding.coin,
                market: HlMarket::Perp,
                amount_delta: DecimalString::new(funding.usdc)?,
                fee: None,
                fee_token: None,
                hash: ZERO_HASH.to_string(),
                observed_at_ms,
            })
        })
        .collect()
}

pub fn decode_ledger_update(
    user: Address,
    update: UserNonFundingLedgerUpdate,
) -> Result<Option<HlTransferEvent>> {
    let observed_at_ms = now_ms();
    let time_ms = i64::try_from(update.time).unwrap_or(i64::MAX);
    let hash = update.hash;
    match update.delta {
        UserNonFundingLedgerDelta::Deposit { usdc } => Ok(Some(HlTransferEvent {
            user,
            time_ms,
            kind: HlTransferKind::Deposit,
            asset: "USDC".to_string(),
            market: HlMarket::NotApplicable,
            amount_delta: DecimalString::new(usdc)?,
            fee: None,
            fee_token: None,
            hash,
            observed_at_ms,
        })),
        UserNonFundingLedgerDelta::Withdraw { usdc, nonce, fee } => Ok(Some(HlTransferEvent {
            user,
            time_ms,
            kind: HlTransferKind::Withdraw { nonce },
            asset: "USDC".to_string(),
            market: HlMarket::NotApplicable,
            amount_delta: DecimalString::new(negate_decimal(&usdc))?,
            fee: Some(DecimalString::new(fee)?),
            fee_token: Some("USDC".to_string()),
            hash,
            observed_at_ms,
        })),
        UserNonFundingLedgerDelta::InternalTransfer {
            usdc,
            user: source,
            destination,
            fee,
        } => Ok(Some(HlTransferEvent {
            user,
            time_ms,
            kind: HlTransferKind::InternalTransfer { destination },
            asset: "USDC".to_string(),
            market: HlMarket::NotApplicable,
            amount_delta: DecimalString::new(signed_for_user(user, &source, &usdc))?,
            fee: Some(DecimalString::new(fee)?),
            fee_token: Some("USDC".to_string()),
            hash,
            observed_at_ms,
        })),
        UserNonFundingLedgerDelta::AccountClassTransfer { usdc, to_perp } => {
            Ok(Some(HlTransferEvent {
                user,
                time_ms,
                kind: HlTransferKind::AccountClassTransfer { to_perp },
                asset: "USDC".to_string(),
                market: HlMarket::NotApplicable,
                amount_delta: DecimalString::new(usdc)?,
                fee: None,
                fee_token: None,
                hash,
                observed_at_ms,
            }))
        }
        UserNonFundingLedgerDelta::SpotTransfer {
            token,
            amount,
            usdc_value,
            user: source,
            destination,
            fee,
            native_token_fee,
            nonce,
        } => {
            let amount_delta = signed_for_user(user, &source, &amount);
            Ok(Some(HlTransferEvent {
                user,
                time_ms,
                kind: HlTransferKind::SpotTransfer {
                    destination,
                    native_token_fee,
                    nonce,
                    usdc_value,
                },
                asset: token,
                market: HlMarket::Spot,
                amount_delta: DecimalString::new(amount_delta)?,
                fee: Some(DecimalString::new(fee)?),
                fee_token: None,
                hash,
                observed_at_ms,
            }))
        }
        UserNonFundingLedgerDelta::Unsupported { type_name, raw } => {
            telemetry::record_unsupported_ledger_delta(&type_name);
            warn!(
                type_name,
                raw = %truncate_raw(&raw.to_string()),
                "skipping unsupported Hyperliquid ledger delta"
            );
            Ok(None)
        }
    }
}

fn decode_fill(
    user: Address,
    fill: UserFill,
    metadata: &MetadataSnapshot,
) -> Result<HlTransferEvent> {
    let observed_at_ms = now_ms();
    let amount_delta = if fill.side == "A" {
        negate_decimal(&fill.sz)
    } else {
        fill.sz.clone()
    };
    Ok(HlTransferEvent {
        user,
        time_ms: i64::try_from(fill.time).unwrap_or(i64::MAX),
        kind: HlTransferKind::Fill {
            oid: fill.oid,
            tid: fill.tid,
            side: fill.side,
            px: fill.px,
            sz: fill.sz,
            crossed: fill.crossed,
            dir: fill.dir,
            closed_pnl: fill.closed_pnl,
            start_position: fill.start_position,
            builder_fee: None,
        },
        asset: fill.coin.clone(),
        market: metadata.classify_coin(&fill.coin),
        amount_delta: DecimalString::new(amount_delta)?,
        fee: Some(DecimalString::new(fill.fee)?),
        fee_token: Some(fill.fee_token),
        hash: fill.hash,
        observed_at_ms,
    })
}

fn signed_for_user(user: Address, source: &str, amount: &str) -> String {
    if source.eq_ignore_ascii_case(&format!("{user:?}")) {
        negate_decimal(amount)
    } else {
        amount.to_string()
    }
}

fn negate_decimal(value: &str) -> String {
    if value == "0" || value.starts_with('-') {
        value.to_string()
    } else {
        format!("-{value}")
    }
}

fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}

fn truncate_raw(raw: &str) -> String {
    const MAX_RAW_LOG_CHARS: usize = 512;
    if raw.len() <= MAX_RAW_LOG_CHARS {
        raw.to_string()
    } else {
        format!("{}...", &raw[..MAX_RAW_LOG_CHARS])
    }
}
