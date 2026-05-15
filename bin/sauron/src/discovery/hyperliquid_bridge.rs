use std::{str::FromStr, sync::Arc};

use alloy::primitives::{Address, U256};
use evm_token_indexer_client::{TokenIndexerClient, TransferEvent};
use hl_shim_client::{HlShimClient, HlTransferEvent, HlTransferKind};
use router_core::models::{
    ProviderOperationHintKind, ProviderOperationType, SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE,
};
use router_server::api::{ProviderOperationHintRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN};
use router_temporal::{
    HlBridgeDepositCreditedEvidence, HlBridgeDepositObservedEvidence,
    HlWithdrawalAcknowledgedEvidence, HlWithdrawalSettledEvidence,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::debug;

use crate::{error::Error, provider_operations::ProviderOperationWatchEntry};

pub async fn bridge_hint(
    hl: &HlShimClient,
    arb: Option<&Arc<TokenIndexerClient>>,
    operation: &ProviderOperationWatchEntry,
    match_window_seconds: i64,
) -> crate::error::Result<Option<ProviderOperationHintRequest>> {
    match operation.operation_type {
        ProviderOperationType::HyperliquidBridgeDeposit => {
            deposit_hint(hl, arb, operation, match_window_seconds).await
        }
        ProviderOperationType::HyperliquidBridgeWithdrawal => {
            withdrawal_hint(hl, arb, operation, match_window_seconds).await
        }
        _ => Ok(None),
    }
}

async fn deposit_hint(
    hl: &HlShimClient,
    arb: Option<&Arc<TokenIndexerClient>>,
    operation: &ProviderOperationWatchEntry,
    match_window_seconds: i64,
) -> crate::error::Result<Option<ProviderOperationHintRequest>> {
    let Some(user) = operation_user(operation) else {
        debug!(operation_id = %operation.operation_id, "HL bridge deposit operation is missing user");
        return Ok(None);
    };
    let Some(raw_amount) = json_str(&operation.request, "amount") else {
        debug!(operation_id = %operation.operation_id, "HL bridge deposit operation is missing amount");
        return Ok(None);
    };
    let decimal_amount = raw_usdc_to_decimal(raw_amount)?;
    let Some(arb) = arb else {
        debug!(
            operation_id = %operation.operation_id,
            "HL bridge deposit observer has no Arbitrum token indexer"
        );
        return Ok(None);
    };
    let Some(bridge) = json_address(&operation.request, "bridge_address") else {
        debug!(operation_id = %operation.operation_id, "HL bridge deposit operation is missing bridge_address");
        return Ok(None);
    };
    let token = json_address(&operation.request, "input_asset");
    let transfer =
        bridge_transfer_to(arb, bridge, token, raw_amount, operation_tx_hash(operation)).await?;
    let Some(transfer) = transfer else {
        debug!(
            operation_id = %operation.operation_id,
            %bridge,
            raw_amount,
            "HL bridge deposit observer has not found Arbitrum bridge transfer"
        );
        return Ok(None);
    };
    let credit = hl_ledger_event(
        hl,
        user,
        operation.updated_at.timestamp_millis() - match_window_seconds * 1_000,
        |event| {
            matches!(event.kind, HlTransferKind::Deposit)
                && decimal_strings_equal(event.amount_delta.as_str(), &decimal_amount)
                && within_window_ms(
                    event.time_ms,
                    transfer.timestamp as i64 * 1_000,
                    match_window_seconds,
                )
        },
    )
    .await?;
    if let Some(credit) = credit {
        return Ok(Some(hint_request(
            operation,
            ProviderOperationHintKind::HlBridgeDepositCredited,
            hl_bridge_deposit_credited_evidence(user, decimal_amount, &credit),
            "hl-bridge-deposit",
        )));
    };
    debug!(
        operation_id = %operation.operation_id,
        %user,
        decimal_amount,
        "HL bridge deposit observer has not found HL credit ledger event"
    );
    Ok(Some(hint_request(
        operation,
        ProviderOperationHintKind::HlBridgeDepositObserved,
        hl_bridge_deposit_observed_evidence(
            user,
            decimal_amount,
            format!("{:?}", transfer.transaction_hash),
            transfer.log_index.unwrap_or_default(),
            parse_u64_or_zero(&transfer.block_number),
        ),
        "hl-bridge-deposit",
    )))
}

async fn withdrawal_hint(
    hl: &HlShimClient,
    arb: Option<&Arc<TokenIndexerClient>>,
    operation: &ProviderOperationWatchEntry,
    match_window_seconds: i64,
) -> crate::error::Result<Option<ProviderOperationHintRequest>> {
    let Some(user) = withdrawal_user(operation) else {
        debug!(operation_id = %operation.operation_id, "HL bridge withdrawal operation is missing user");
        return Ok(None);
    };
    let Some(decimal_amount) = withdrawal_decimal_amount(operation) else {
        debug!(operation_id = %operation.operation_id, "HL bridge withdrawal operation is missing amount");
        return Ok(None);
    };
    let Some(payout_raw_amount) = withdrawal_payout_raw_amount(operation)? else {
        debug!(operation_id = %operation.operation_id, "HL bridge withdrawal operation is missing payout amount");
        return Ok(None);
    };
    let payout_decimal_amount = raw_usdc_to_decimal(&payout_raw_amount)?;
    let withdraw = hl_ledger_event(
        hl,
        user,
        operation.updated_at.timestamp_millis() - match_window_seconds * 1_000,
        |event| {
            matches!(event.kind, HlTransferKind::Withdraw { .. })
                && decimal_strings_abs_equal(event.amount_delta.as_str(), &decimal_amount)
        },
    )
    .await?;
    let Some(withdraw) = withdraw else {
        debug!(
            operation_id = %operation.operation_id,
            %user,
            decimal_amount,
            "HL bridge withdrawal observer has not found HL withdrawal ledger event"
        );
        return Ok(None);
    };
    let nonce = match withdraw.kind {
        HlTransferKind::Withdraw { nonce } => nonce,
        _ => return Ok(None),
    };
    if let Some(arb) = arb {
        if let Some(recipient) = json_address(&operation.request, "recipient_address") {
            let token = json_address(&operation.request, "output_asset");
            let payout = payout_transfer_to(
                arb,
                recipient,
                token,
                &payout_raw_amount,
                json_address(&operation.request, "bridge_address"),
            )
            .await?;
            if let Some(payout) = payout {
                if within_window_ms(
                    withdraw.time_ms,
                    payout.timestamp as i64 * 1_000,
                    match_window_seconds,
                ) {
                    return Ok(Some(hint_request(
                        operation,
                        ProviderOperationHintKind::HlWithdrawalSettled,
                        hl_withdrawal_settled_evidence(
                            user,
                            payout_decimal_amount,
                            format!("{:?}", payout.transaction_hash),
                            payout.log_index.unwrap_or_default(),
                            parse_u64_or_zero(&payout.block_number),
                            nonce,
                        ),
                        "hl-withdrawal-settled",
                    )));
                }
                tracing::warn!(
                    operation_id = %operation.operation_id,
                    nonce,
                    withdraw_time_ms = withdraw.time_ms,
                    payout_timestamp = payout.timestamp,
                    match_window_seconds,
                    "HL bridge withdrawal payout match is near/outside configured window"
                );
            }
        }
    }
    Ok(Some(hint_request(
        operation,
        ProviderOperationHintKind::HlWithdrawalAcknowledged,
        hl_withdrawal_ack_evidence(user, decimal_amount, nonce, &withdraw),
        "hl-withdrawal-ack",
    )))
}

fn hl_bridge_deposit_credited_evidence(
    user: Address,
    usdc: String,
    credit: &HlTransferEvent,
) -> Value {
    typed_evidence(HlBridgeDepositCreditedEvidence {
        user: format!("{user:?}"),
        usdc,
        hl_credit_hash: credit.hash.clone(),
        hl_credit_time_ms: credit.time_ms,
    })
}

fn hl_bridge_deposit_observed_evidence(
    user: Address,
    usdc: String,
    arb_tx_hash: String,
    log_index: u64,
    block_number: u64,
) -> Value {
    typed_evidence(HlBridgeDepositObservedEvidence {
        user: format!("{user:?}"),
        usdc,
        arb_tx_hash,
        log_index,
        block_number,
    })
}

fn hl_withdrawal_ack_evidence(
    user: Address,
    usdc: String,
    nonce: u64,
    withdraw: &HlTransferEvent,
) -> Value {
    typed_evidence(HlWithdrawalAcknowledgedEvidence {
        user: format!("{user:?}"),
        usdc,
        nonce,
        hl_request_hash: withdraw.hash.clone(),
        hl_request_time_ms: withdraw.time_ms,
    })
}

fn hl_withdrawal_settled_evidence(
    user: Address,
    usdc: String,
    arb_payout_tx_hash: String,
    log_index: u64,
    block_number: u64,
    time_window_match_to_nonce: u64,
) -> Value {
    typed_evidence(HlWithdrawalSettledEvidence {
        user: format!("{user:?}"),
        usdc,
        arb_payout_tx_hash,
        log_index,
        block_number,
        time_window_match_to_nonce,
    })
}

fn typed_evidence<T: serde::Serialize>(evidence: T) -> Value {
    serde_json::to_value(evidence).expect("typed provider-operation evidence serializes")
}

fn hint_request(
    operation: &ProviderOperationWatchEntry,
    kind: ProviderOperationHintKind,
    evidence: Value,
    key_prefix: &'static str,
) -> ProviderOperationHintRequest {
    let idempotency_key = hint_idempotency_key(operation, kind, &evidence, key_prefix);
    ProviderOperationHintRequest {
        provider_operation_id: operation.operation_id,
        execution_step_id: operation.execution_step_id,
        source: SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE.to_string(),
        hint_kind: kind,
        idempotency_key: Some(idempotency_key),
        evidence,
    }
}

fn hint_idempotency_key(
    operation: &ProviderOperationWatchEntry,
    kind: ProviderOperationHintKind,
    evidence: &Value,
    key_prefix: &'static str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(operation.operation_id.as_bytes());
    hasher.update(operation.execution_step_id.inner().as_bytes());
    hasher.update(kind.to_db_string().as_bytes());
    hasher.update(evidence.to_string().as_bytes());
    let digest = hasher.finalize()[..8]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let key = format!(
        "{key_prefix}:{}:{}:{}",
        operation.operation_id,
        kind.to_db_string(),
        digest
    );
    debug_assert!(key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
    key
}

async fn bridge_transfer_to(
    client: &TokenIndexerClient,
    bridge: Address,
    token: Option<Address>,
    raw_amount: &str,
    expected_tx_hash: Option<String>,
) -> crate::error::Result<Option<TransferEvent>> {
    let amount = U256::from_str(raw_amount).map_err(|source| Error::InvalidWatchRow {
        message: format!("invalid HL bridge raw amount {raw_amount}: {source}"),
    })?;
    let response = client
        .get_transfers_to(bridge, token, Some(amount), Some(250))
        .await
        .map_err(|source| Error::EvmTokenIndexer { source })?;
    Ok(response.transfers.into_iter().find(|transfer| {
        transfer.amount == raw_amount
            && expected_tx_hash.as_deref().is_none_or(|hash| {
                format!("{:?}", transfer.transaction_hash).eq_ignore_ascii_case(hash)
            })
    }))
}

async fn payout_transfer_to(
    client: &TokenIndexerClient,
    recipient: Address,
    token: Option<Address>,
    raw_amount: &str,
    bridge: Option<Address>,
) -> crate::error::Result<Option<TransferEvent>> {
    let amount = U256::from_str(raw_amount).map_err(|source| Error::InvalidWatchRow {
        message: format!("invalid HL withdrawal raw amount {raw_amount}: {source}"),
    })?;
    let response = client
        .get_transfers_to(recipient, token, Some(amount), Some(250))
        .await
        .map_err(|source| Error::EvmTokenIndexer { source })?;
    Ok(response.transfers.into_iter().find(|transfer| {
        transfer.amount == raw_amount && bridge.is_none_or(|bridge| transfer.from == bridge)
    }))
}

async fn hl_ledger_event<F>(
    client: &HlShimClient,
    user: Address,
    from_time_ms: i64,
    matches: F,
) -> crate::error::Result<Option<HlTransferEvent>>
where
    F: Fn(&HlTransferEvent) -> bool,
{
    let response = client
        .transfers(user, Some(from_time_ms.max(0)), Some(2_000), None)
        .await
        .map_err(|source| Error::HlShim { source })?;
    Ok(response.events.into_iter().find(matches))
}

fn operation_user(operation: &ProviderOperationWatchEntry) -> Option<Address> {
    operation
        .provider_ref
        .as_deref()
        .or_else(|| json_str(&operation.request, "hyperliquid_user"))
        .and_then(|value| value.parse().ok())
}

fn withdrawal_user(operation: &ProviderOperationWatchEntry) -> Option<Address> {
    json_str(&operation.request, "hyperliquid_custody_vault_address")
        .or_else(|| {
            json_str(
                &operation.execution_step_request,
                "hyperliquid_custody_vault_address",
            )
        })
        .and_then(|value| value.parse().ok())
}

fn operation_tx_hash(operation: &ProviderOperationWatchEntry) -> Option<String> {
    operation
        .observed_state
        .get("deposit_tx_hash")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn withdrawal_decimal_amount(operation: &ProviderOperationWatchEntry) -> Option<String> {
    operation
        .request
        .get("amount_decimal")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            operation
                .request
                .get("amount")
                .and_then(Value::as_str)
                .and_then(|raw| raw_usdc_to_decimal(raw).ok())
        })
}

fn withdrawal_payout_raw_amount(
    operation: &ProviderOperationWatchEntry,
) -> crate::error::Result<Option<String>> {
    let Some(amount) = json_str(&operation.request, "amount") else {
        return Ok(None);
    };
    let gross = U256::from_str(amount).map_err(|source| Error::InvalidWatchRow {
        message: format!("invalid HL withdrawal raw amount {amount}: {source}"),
    })?;
    let fee = match json_str(&operation.request, "withdraw_fee_raw") {
        Some(raw) => U256::from_str(raw).map_err(|source| Error::InvalidWatchRow {
            message: format!("invalid HL withdrawal fee raw amount {raw}: {source}"),
        })?,
        None => U256::ZERO,
    };
    if gross < fee {
        return Err(Error::InvalidWatchRow {
            message: format!("HL withdrawal amount {amount} is smaller than fee {fee}"),
        });
    }
    Ok(Some((gross - fee).to_string()))
}

fn json_str<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn json_address(value: &Value, field: &str) -> Option<Address> {
    json_str(value, field).and_then(|value| value.parse().ok())
}

fn within_window_ms(left_ms: i64, right_ms: i64, window_seconds: i64) -> bool {
    left_ms.saturating_sub(right_ms).abs() <= window_seconds.saturating_mul(1_000)
}

fn parse_u64_or_zero(value: &str) -> u64 {
    value.parse().unwrap_or(0)
}

fn raw_usdc_to_decimal(raw: &str) -> crate::error::Result<String> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::InvalidWatchRow {
            message: format!("invalid raw USDC amount {raw:?}"),
        });
    }
    let raw = raw.trim_start_matches('0');
    let raw = if raw.is_empty() { "0" } else { raw };
    if raw.len() <= 6 {
        let mut fraction = "0".repeat(6 - raw.len());
        fraction.push_str(raw);
        let fraction = fraction.trim_end_matches('0');
        return Ok(if fraction.is_empty() {
            "0".to_string()
        } else {
            format!("0.{fraction}")
        });
    }
    let (whole, fraction) = raw.split_at(raw.len() - 6);
    let fraction = fraction.trim_end_matches('0');
    Ok(if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    })
}

fn decimal_strings_equal(left: &str, right: &str) -> bool {
    normalize_decimal(left)
        .is_some_and(|left| normalize_decimal(right).is_some_and(|right| left == right))
}

fn decimal_strings_abs_equal(left: &str, right: &str) -> bool {
    decimal_strings_equal(
        left.strip_prefix('-').unwrap_or(left),
        right.strip_prefix('-').unwrap_or(right),
    )
}

fn normalize_decimal(value: &str) -> Option<String> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = {
        let trimmed = whole.trim_start_matches('0');
        if trimmed.is_empty() {
            "0"
        } else {
            trimmed
        }
    };
    let fraction = fraction.trim_end_matches('0');
    Some(if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use router_core::models::{ProviderOperationStatus, ProviderOperationType};
    use router_temporal::WorkflowStepId;
    use serde::de::DeserializeOwned;
    use serde_json::json;
    use std::collections::BTreeSet;
    use uuid::Uuid;

    fn watch() -> ProviderOperationWatchEntry {
        ProviderOperationWatchEntry {
            operation_id: Uuid::now_v7(),
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "hyperliquid_bridge".to_string(),
            operation_type: ProviderOperationType::HyperliquidBridgeDeposit,
            provider_ref: Some("0x1111111111111111111111111111111111111111".to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({}),
            execution_step_request: json!({}),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn raw_usdc_to_decimal_formats_six_decimals() {
        assert_eq!(raw_usdc_to_decimal("0").unwrap(), "0");
        assert_eq!(raw_usdc_to_decimal("1").unwrap(), "0.000001");
        assert_eq!(raw_usdc_to_decimal("1000000").unwrap(), "1");
        assert_eq!(raw_usdc_to_decimal("123456789").unwrap(), "123.456789");
    }

    #[test]
    fn window_match_is_symmetric() {
        assert!(within_window_ms(1_000, 31_000, 30));
        assert!(within_window_ms(31_000, 1_000, 30));
        assert!(!within_window_ms(1_000, 31_001, 30));
    }

    #[test]
    fn decimal_abs_match_accepts_signed_withdrawal_amounts() {
        assert!(decimal_strings_abs_equal("-101.000000000000000000", "101"));
        assert!(!decimal_strings_abs_equal("-101.000001", "101"));
    }

    #[test]
    fn withdrawal_payout_amount_subtracts_bridge_fee() {
        let mut watch = watch();
        watch.operation_type = ProviderOperationType::HyperliquidBridgeWithdrawal;
        watch.request = json!({
            "amount": "101000000",
            "withdraw_fee_raw": "1000000"
        });

        assert_eq!(
            withdrawal_payout_raw_amount(&watch).unwrap().as_deref(),
            Some("100000000")
        );
    }

    #[test]
    fn hint_idempotency_key_stays_within_router_limit() {
        let key = hint_idempotency_key(
            &watch(),
            ProviderOperationHintKind::HlBridgeDepositCredited,
            &json!({
                "user": "0x1111111111111111111111111111111111111111",
                "usdc": "105.362953",
                "hl_credit_hash": "0x832fd0f4639c39c05011217e5b28840f9376cc24c4366660595a0cc158a88034",
                "hl_credit_time_ms": 1_778_522_898_534i64,
            }),
            "hl-bridge-deposit",
        );

        assert!(key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN);
    }

    #[test]
    fn hl_bridge_deposit_observed_evidence_matches_router_typed_shape() {
        let evidence = hl_bridge_deposit_observed_evidence(
            user(),
            "105.362953".to_string(),
            "0xarb".to_string(),
            7,
            123,
        );

        assert_typed_evidence::<HlBridgeDepositObservedEvidence>(
            &evidence,
            &["user", "usdc", "arb_tx_hash", "log_index", "block_number"],
        );
    }

    #[test]
    fn hl_bridge_deposit_credited_evidence_matches_router_typed_shape() {
        let credit = hl_transfer_event("deposit", None);

        let evidence =
            hl_bridge_deposit_credited_evidence(user(), "105.362953".to_string(), &credit);

        assert_typed_evidence::<HlBridgeDepositCreditedEvidence>(
            &evidence,
            &["user", "usdc", "hl_credit_hash", "hl_credit_time_ms"],
        );
    }

    #[test]
    fn hl_withdrawal_ack_evidence_matches_router_typed_shape() {
        let withdraw = hl_transfer_event("withdraw", Some(99));

        let evidence = hl_withdrawal_ack_evidence(user(), "105.362953".to_string(), 99, &withdraw);

        assert_typed_evidence::<HlWithdrawalAcknowledgedEvidence>(
            &evidence,
            &[
                "user",
                "usdc",
                "nonce",
                "hl_request_hash",
                "hl_request_time_ms",
            ],
        );
    }

    #[test]
    fn hl_withdrawal_settled_evidence_matches_router_typed_shape() {
        let evidence = hl_withdrawal_settled_evidence(
            user(),
            "105.362953".to_string(),
            "0xarb".to_string(),
            7,
            123,
            99,
        );

        assert_typed_evidence::<HlWithdrawalSettledEvidence>(
            &evidence,
            &[
                "user",
                "usdc",
                "arb_payout_tx_hash",
                "log_index",
                "block_number",
                "time_window_match_to_nonce",
            ],
        );
    }

    fn user() -> Address {
        "0x1111111111111111111111111111111111111111"
            .parse()
            .expect("address")
    }

    fn hl_transfer_event(kind: &str, nonce: Option<u64>) -> HlTransferEvent {
        let kind = match nonce {
            Some(nonce) => json!({ "type": kind, "nonce": nonce }),
            None => json!({ "type": kind }),
        };
        serde_json::from_value(json!({
            "user": "0x1111111111111111111111111111111111111111",
            "time_ms": 1_778_522_898_534i64,
            "kind": kind,
            "asset": "USDC",
            "market": "spot",
            "amount_delta": "105.362953",
            "fee": null,
            "fee_token": null,
            "hash": "0x832fd0f4639c39c05011217e5b28840f9376cc24c4366660595a0cc158a88034",
            "observed_at_ms": 1_778_522_898_535i64
        }))
        .expect("HL transfer event")
    }

    fn assert_typed_evidence<T: DeserializeOwned>(value: &Value, expected: &[&str]) {
        let actual = value
            .as_object()
            .expect("evidence should be an object")
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let expected = expected.iter().copied().collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);
        serde_json::from_value::<T>(value.clone()).expect("typed evidence should deserialize");
    }
}
