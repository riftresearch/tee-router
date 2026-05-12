use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::{Duration, Instant},
};

use alloy::primitives::Address;
use bitcoin::{address::NetworkUnchecked, Address as BitcoinAddress};
use bitcoin_indexer_client::TxOutput;
use bitcoin_receipt_watcher_client::{parse_txid, ByIdLookup};
use hl_shim_client::{HlShimClient, HlTransferEvent, HlTransferKind};
use hyperunit_client::{
    HyperUnitClient, HyperUnitClientError, UnitOperation, UnitOperationsRequest,
};
use router_core::models::{
    ProviderOperationHintKind, ProviderOperationType, SAURON_HYPERUNIT_OBSERVER_HINT_SOURCE,
};
use router_server::api::{ProviderOperationHintRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN};
use router_temporal::{
    BtcDepositObservedEvidence, HyperUnitDepositCreditedEvidence,
    HyperUnitWithdrawalAcknowledgedEvidence, HyperUnitWithdrawalSettledEvidence,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::{timeout, MissedTickBehavior};
use tracing::warn;
use uuid::Uuid;

use crate::{
    discovery::btc::{best_output, BitcoinClients},
    error::{Error, Result},
    provider_operations::{ProviderOperationWatchStore, SharedProviderOperationWatchEntry},
    router_client::RouterClient,
};

const HYPERUNIT_OBSERVER_TICK: Duration = Duration::from_secs(1);
const HYPERUNIT_MAX_WAIT: Duration = Duration::from_secs(2 * 60 * 60);
const HYPERUNIT_STATUS_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERUNIT_HL_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERUNIT_BTC_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct PollState {
    first_seen: Instant,
    next_poll_at: Instant,
}

pub async fn run_hyperunit_observer_loop(
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
    unit: HyperUnitClient,
    hl: HlShimClient,
    btc: BitcoinClients,
) -> Result<()> {
    let mut poll_state = HashMap::<Uuid, PollState>::new();
    let mut submitted = HashSet::<(Uuid, String)>::new();
    let mut ticker = tokio::time::interval(HYPERUNIT_OBSERVER_TICK);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        let operations = store.snapshot().await;
        let active = operations
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<HashSet<_>>();
        poll_state.retain(|operation_id, _| active.contains(operation_id));
        submitted.retain(|(operation_id, _)| active.contains(operation_id));

        for operation in operations {
            if !is_hyperunit_operation(operation.operation_type) {
                continue;
            }
            let now = Instant::now();
            let state = poll_state
                .entry(operation.operation_id)
                .or_insert(PollState {
                    first_seen: now,
                    next_poll_at: now,
                });
            if state.first_seen.elapsed() > HYPERUNIT_MAX_WAIT {
                warn!(
                    operation_id = %operation.operation_id,
                    operation_type = operation.operation_type.to_db_string(),
                    "HyperUnit observer reached max wait; leaving operation for router timeout/MIR"
                );
                state.next_poll_at = now + Duration::from_secs(60);
                continue;
            }
            if state.next_poll_at > now {
                continue;
            }
            state.next_poll_at = now + hyperunit_poll_interval(state.first_seen.elapsed());

            let hints = match hyperunit_hints(&unit, &hl, &btc, &operation).await {
                Ok(hints) => hints,
                Err(error) => {
                    warn!(
                        operation_id = %operation.operation_id,
                        operation_type = operation.operation_type.to_db_string(),
                        %error,
                        "HyperUnit observer failed to build hints"
                    );
                    continue;
                }
            };
            for hint in hints {
                submit_hint(&router_client, &mut submitted, &operation, hint).await;
            }
        }

        ticker.tick().await;
    }
}

async fn hyperunit_hints(
    unit: &HyperUnitClient,
    hl: &HlShimClient,
    btc: &BitcoinClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Vec<ProviderOperationHintRequest>> {
    match operation.operation_type {
        ProviderOperationType::UnitDeposit => deposit_hints(unit, hl, btc, operation).await,
        ProviderOperationType::UnitWithdrawal => withdrawal_hints(unit, btc, operation).await,
        _ => Ok(Vec::new()),
    }
}

async fn deposit_hints(
    unit: &HyperUnitClient,
    hl: &HlShimClient,
    btc: &BitcoinClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Vec<ProviderOperationHintRequest>> {
    let Some(protocol_address) = protocol_address(operation) else {
        return Ok(Vec::new());
    };
    let source_is_bitcoin = unit_operation_source_is_bitcoin(operation);
    let output = if source_is_bitcoin {
        let amount = request_str(&operation.request, "amount").and_then(parse_u64);
        bitcoin_output(btc, protocol_address, amount.unwrap_or(1), u64::MAX).await?
    } else {
        None
    };
    if source_is_bitcoin && output.is_none() {
        return Ok(Vec::new());
    }

    let mut hints = Vec::new();
    if let Some(output) = output.as_ref() {
        hints.push(hint_request(
            operation,
            ProviderOperationHintKind::BtcDepositObserved,
            btc_deposit_evidence(protocol_address, output),
            "btc-deposit",
        ));
    }

    let Some(status) = lookup_deposit_status(unit, protocol_address).await? else {
        return Ok(hints);
    };
    if !status_matches_protocol(&status, protocol_address)
        || !hyperunit_deposit_is_credited(&status)
        || output
            .as_ref()
            .is_some_and(|output| output.confirmations == 0)
    {
        return Ok(hints);
    }
    let Some(user) = request_str(&operation.request, "dst_addr").and_then(parse_hl_address) else {
        return Ok(hints);
    };
    let credit = hl_credit_event(hl, operation, user, &status).await?;
    if let Some(credit) = credit {
        hints.push(hint_request(
            operation,
            ProviderOperationHintKind::HyperUnitDepositCredited,
            hyperunit_deposit_credited_evidence(
                protocol_address,
                output.as_ref(),
                &status,
                &credit,
            ),
            "unit-deposit-credited",
        ));
    }
    Ok(hints)
}

async fn withdrawal_hints(
    unit: &HyperUnitClient,
    btc: &BitcoinClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Vec<ProviderOperationHintRequest>> {
    let Some(protocol_address) = protocol_address(operation) else {
        return Ok(Vec::new());
    };
    let Some(status) = lookup_withdrawal_status(unit, protocol_address).await? else {
        return Ok(Vec::new());
    };
    if !status_matches_protocol(&status, protocol_address) {
        return Ok(Vec::new());
    }
    let Some(btc_txid) = status.destination_tx_hash.as_deref() else {
        return Ok(Vec::new());
    };
    let mut hints = vec![hint_request(
        operation,
        ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
        hyperunit_withdrawal_ack_evidence(protocol_address, &status),
        "unit-withdrawal-ack",
    )];
    let Some(receipt_watcher) = btc.receipt_watcher.as_ref() else {
        return Ok(hints);
    };
    let Ok(txid) = parse_txid(btc_txid) else {
        return Ok(hints);
    };
    let receipt = match timeout(
        HYPERUNIT_BTC_LOOKUP_TIMEOUT,
        receipt_watcher.lookup_by_id(txid),
    )
    .await
    {
        Ok(Ok(receipt)) => receipt,
        Ok(Err(source)) => return Err(Error::BitcoinReceiptWatcher { source }),
        Err(_) => None,
    };
    let Some((tx, confirmations)) = receipt else {
        return Ok(hints);
    };
    let Some(destination_address) = request_str(&operation.request, "dst_addr") else {
        return Ok(hints);
    };
    let Some((vout, amount_sats)) = payout_output(&tx, destination_address) else {
        return Ok(hints);
    };
    hints.push(hint_request(
        operation,
        ProviderOperationHintKind::HyperUnitWithdrawalSettled,
        hyperunit_withdrawal_settled_evidence(
            protocol_address,
            &status,
            destination_address,
            vout,
            amount_sats,
            confirmations,
        ),
        "unit-withdrawal-settled",
    ));
    Ok(hints)
}

async fn submit_hint(
    router_client: &RouterClient,
    submitted: &mut HashSet<(Uuid, String)>,
    operation: &SharedProviderOperationWatchEntry,
    hint: ProviderOperationHintRequest,
) {
    let key_text = hint
        .idempotency_key
        .clone()
        .unwrap_or_else(|| operation.operation_id.to_string());
    let key = (operation.operation_id, key_text);
    if submitted.contains(&key) {
        return;
    }
    match router_client.submit_provider_operation_hint(&hint).await {
        Ok(_) => {
            submitted.insert(key);
        }
        Err(error) => {
            warn!(
                operation_id = %operation.operation_id,
                hint_kind = hint.hint_kind.to_db_string(),
                %error,
                "failed to submit HyperUnit provider-operation hint"
            );
        }
    }
}

async fn bitcoin_output(
    btc: &BitcoinClients,
    address: &str,
    min_amount: u64,
    max_amount: u64,
) -> Result<Option<TxOutput>> {
    let address = address
        .parse::<BitcoinAddress<NetworkUnchecked>>()
        .map_err(|source| Error::InvalidWatchRow {
            message: format!("invalid HyperUnit Bitcoin protocol address {address}: {source}"),
        })?;
    timeout(
        HYPERUNIT_BTC_LOOKUP_TIMEOUT,
        best_output(&btc.indexer, address, min_amount, max_amount),
    )
    .await
    .unwrap_or(Ok(None))
}

async fn lookup_deposit_status(
    unit: &HyperUnitClient,
    protocol_address: &str,
) -> Result<Option<UnitOperation>> {
    let operations = lookup_operations(unit, protocol_address).await?;
    Ok(select_deposit_operation(operations, protocol_address))
}

async fn lookup_withdrawal_status(
    unit: &HyperUnitClient,
    protocol_address: &str,
) -> Result<Option<UnitOperation>> {
    let operations = lookup_operations(unit, protocol_address).await?;
    Ok(select_withdrawal_operation(operations, protocol_address))
}

async fn lookup_operations(
    unit: &HyperUnitClient,
    protocol_address: &str,
) -> Result<Vec<UnitOperation>> {
    let request = UnitOperationsRequest {
        address: protocol_address.to_string(),
    };
    match timeout(HYPERUNIT_STATUS_TIMEOUT, unit.operations(request)).await {
        Ok(Ok(response)) => Ok(response.operations),
        Ok(Err(HyperUnitClientError::HttpStatus { status: 404, .. })) => Ok(Vec::new()),
        Ok(Err(source)) => Err(Error::HyperUnit { source }),
        Err(_) => Ok(Vec::new()),
    }
}

fn select_deposit_operation(
    operations: Vec<UnitOperation>,
    protocol_address: &str,
) -> Option<UnitOperation> {
    operations
        .into_iter()
        .find(|op| op.matches_protocol_address(protocol_address) && is_deposit_direction(op))
}

fn select_withdrawal_operation(
    operations: Vec<UnitOperation>,
    protocol_address: &str,
) -> Option<UnitOperation> {
    operations
        .into_iter()
        .find(|op| op.matches_protocol_address(protocol_address) && is_withdrawal_direction(op))
}

fn is_deposit_direction(op: &UnitOperation) -> bool {
    op.destination_chain
        .as_deref()
        .is_some_and(|chain| chain.eq_ignore_ascii_case("hyperliquid"))
}

fn is_withdrawal_direction(op: &UnitOperation) -> bool {
    op.source_chain
        .as_deref()
        .is_some_and(|chain| chain.eq_ignore_ascii_case("hyperliquid"))
}

async fn hl_credit_event(
    hl: &HlShimClient,
    operation: &SharedProviderOperationWatchEntry,
    user: Address,
    unit_operation: &UnitOperation,
) -> Result<Option<HlTransferEvent>> {
    let from_time_ms = operation
        .updated_at
        .timestamp_millis()
        .saturating_sub(30 * 60 * 1_000)
        .max(0);
    let expected = expected_hl_credit_amount(unit_operation);
    let page = timeout(
        HYPERUNIT_HL_LOOKUP_TIMEOUT,
        hl.transfers(user, Some(from_time_ms), Some(2_000), None),
    )
    .await
    .map_err(|_| Error::InvalidConfiguration {
        message: "HL shim transfer lookup timed out".to_string(),
    })?
    .map_err(|source| Error::HlShim { source })?;
    Ok(page.events.into_iter().find(|event| {
        matches!(event.kind, HlTransferKind::Deposit)
            && expected
                .as_deref()
                .is_none_or(|amount| decimal_strings_equal(event.amount_delta.as_str(), amount))
    }))
}

fn hint_request(
    operation: &SharedProviderOperationWatchEntry,
    hint_kind: ProviderOperationHintKind,
    evidence: Value,
    key_prefix: &'static str,
) -> ProviderOperationHintRequest {
    ProviderOperationHintRequest {
        provider_operation_id: operation.operation_id,
        execution_step_id: operation.execution_step_id,
        source: SAURON_HYPERUNIT_OBSERVER_HINT_SOURCE.to_string(),
        hint_kind,
        idempotency_key: Some(hint_idempotency_key(
            operation, hint_kind, &evidence, key_prefix,
        )),
        evidence,
    }
}

fn hint_idempotency_key(
    operation: &SharedProviderOperationWatchEntry,
    kind: ProviderOperationHintKind,
    evidence: &Value,
    key_prefix: &'static str,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(operation.operation_id.as_bytes());
    hasher.update(operation.execution_step_id.inner().as_bytes());
    hasher.update(kind.to_db_string().as_bytes());
    hasher.update(evidence.to_string().as_bytes());
    let digest = hasher.finalize();
    let key = format!(
        "{key_prefix}:{}:{}:{}",
        operation.operation_id,
        kind.to_db_string(),
        &alloy::hex::encode(digest)[..16]
    );
    if key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN {
        key
    } else {
        key[..MAX_HINT_IDEMPOTENCY_KEY_LEN].to_string()
    }
}

fn btc_deposit_evidence(address: &str, output: &TxOutput) -> Value {
    typed_evidence(BtcDepositObservedEvidence {
        tx_hash: output.txid.to_string(),
        address: address.to_string(),
        transfer_index: u64::from(output.vout),
        amount: output.amount_sats.to_string(),
        confirmation_state: if output.confirmations > 0 {
            "confirmed"
        } else {
            "mempool"
        }
        .to_string(),
        block_height: output.block_height,
        block_hash: output.block_hash.as_ref().map(|hash| hash.to_string()),
    })
}

fn hyperunit_deposit_credited_evidence(
    protocol_address: &str,
    output: Option<&TxOutput>,
    status: &UnitOperation,
    credit: &HlTransferEvent,
) -> Value {
    let (btc_tx_hash, btc_vout, btc_amount, btc_confirmations, btc_block_height, btc_block_hash) =
        output
            .map(|output| {
                (
                    Some(output.txid.to_string()),
                    Some(u64::from(output.vout)),
                    Some(output.amount_sats.to_string()),
                    Some(output.confirmations),
                    output.block_height,
                    output.block_hash.as_ref().map(|hash| hash.to_string()),
                )
            })
            .unwrap_or((None, None, None, None, None, None));
    typed_evidence(HyperUnitDepositCreditedEvidence {
        protocol_address: protocol_address.to_string(),
        btc_tx_hash,
        btc_vout,
        btc_amount,
        btc_confirmations,
        btc_block_height,
        btc_block_hash,
        hyperunit_operation_id: status.operation_id.clone(),
        hyperunit_status: status.state.clone(),
        hyperunit_source_tx_hash: status.source_tx_hash.clone(),
        hyperunit_destination_tx_hash: status.destination_tx_hash.clone(),
        hl_user: format!("{:#x}", credit.user),
        hl_amount: credit.amount_delta.as_str().to_string(),
        hl_credit_hash: credit.hash.clone(),
        hl_credit_time_ms: credit.time_ms,
    })
}

fn hyperunit_withdrawal_ack_evidence(protocol_address: &str, status: &UnitOperation) -> Value {
    typed_evidence(HyperUnitWithdrawalAcknowledgedEvidence {
        protocol_address: protocol_address.to_string(),
        hyperunit_operation_id: status.operation_id.clone(),
        hyperunit_status: status.state.clone(),
        destination_address: status.destination_address.clone(),
        amount: status.source_amount.clone(),
        btc_tx_hash: status.destination_tx_hash.clone(),
        broadcast_at: status.broadcast_at.clone(),
    })
}

fn hyperunit_withdrawal_settled_evidence(
    protocol_address: &str,
    status: &UnitOperation,
    destination_address: &str,
    vout: u32,
    amount_sats: u64,
    confirmations: u64,
) -> Value {
    typed_evidence(HyperUnitWithdrawalSettledEvidence {
        protocol_address: protocol_address.to_string(),
        hyperunit_operation_id: status.operation_id.clone(),
        hyperunit_status: status.state.clone(),
        destination_address: destination_address.to_string(),
        amount: status.source_amount.clone(),
        btc_tx_hash: status.destination_tx_hash.clone(),
        btc_vout: u64::from(vout),
        btc_amount: amount_sats.to_string(),
        btc_confirmations: confirmations,
    })
}

fn typed_evidence<T: serde::Serialize>(evidence: T) -> Value {
    serde_json::to_value(evidence).expect("typed provider-operation evidence serializes")
}

fn payout_output(tx: &bitcoin::Transaction, destination_address: &str) -> Option<(u32, u64)> {
    let address = destination_address
        .parse::<BitcoinAddress<NetworkUnchecked>>()
        .ok()?
        .assume_checked();
    let script = address.script_pubkey();
    tx.output
        .iter()
        .enumerate()
        .find(|(_, output)| output.script_pubkey == script)
        .and_then(|(index, output)| Some((u32::try_from(index).ok()?, output.value.to_sat())))
}

fn protocol_address(operation: &SharedProviderOperationWatchEntry) -> Option<&str> {
    operation
        .provider_ref
        .as_deref()
        .or_else(|| request_str(&operation.request, "protocol_address"))
}

fn request_str<'a>(value: &'a Value, field: &str) -> Option<&'a str> {
    value.get(field).and_then(Value::as_str)
}

fn unit_operation_source_is_bitcoin(operation: &SharedProviderOperationWatchEntry) -> bool {
    request_str(&operation.request, "src_chain")
        .or_else(|| request_str(&operation.request, "source_chain"))
        .is_some_and(|chain| chain.eq_ignore_ascii_case("bitcoin"))
}

fn status_matches_protocol(status: &UnitOperation, protocol_address: &str) -> bool {
    status
        .protocol_address
        .as_deref()
        .is_none_or(|candidate| candidate.eq_ignore_ascii_case(protocol_address))
}

fn parse_u64(value: &str) -> Option<u64> {
    value.parse().ok()
}

fn parse_hl_address(value: &str) -> Option<Address> {
    Address::from_str(value).ok()
}

fn is_hyperunit_operation(operation_type: ProviderOperationType) -> bool {
    matches!(
        operation_type,
        ProviderOperationType::UnitDeposit | ProviderOperationType::UnitWithdrawal
    )
}

fn hyperunit_deposit_is_credited(operation: &UnitOperation) -> bool {
    operation.classified_state().is_terminal_success()
        || operation
            .state
            .as_deref()
            .is_some_and(|state| state.eq_ignore_ascii_case("credited"))
}

fn expected_hl_credit_amount(operation: &UnitOperation) -> Option<String> {
    let source = operation.source_amount.as_deref()?;
    let source = u128::from_str(source).ok()?;
    let destination_fee = operation
        .destination_fee_amount
        .as_deref()
        .and_then(|value| u128::from_str(value).ok())
        .unwrap_or(0);
    let sweep_fee = operation
        .sweep_fee_amount
        .as_deref()
        .and_then(|value| u128::from_str(value).ok())
        .unwrap_or(0);
    let net = source
        .saturating_sub(destination_fee)
        .saturating_sub(sweep_fee);
    Some(raw_to_decimal(
        net,
        unit_asset_decimals(operation.asset.as_deref()?),
    ))
}

fn unit_asset_decimals(asset: &str) -> u8 {
    match asset {
        "btc" => 8,
        "eth" => 18,
        _ => 8,
    }
}

fn raw_to_decimal(raw: u128, decimals: u8) -> String {
    let mut raw = raw.to_string();
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return raw;
    }
    if raw.len() <= decimals {
        let mut padded = "0".repeat(decimals + 1 - raw.len());
        padded.push_str(&raw);
        raw = padded;
    }
    let split = raw.len() - decimals;
    let whole = &raw[..split];
    let fraction = raw[split..].trim_end_matches('0');
    if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    }
}

fn decimal_strings_equal(left: &str, right: &str) -> bool {
    normalize_decimal(left)
        .is_some_and(|left| normalize_decimal(right).is_some_and(|right| left == right))
}

fn normalize_decimal(value: &str) -> Option<String> {
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let whole = whole.trim_start_matches('0');
    let whole = if whole.is_empty() { "0" } else { whole };
    let fraction = fraction.trim_end_matches('0');
    Some(if fraction.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{fraction}")
    })
}

fn hyperunit_poll_interval(elapsed: Duration) -> Duration {
    if elapsed < Duration::from_secs(5 * 60) {
        Duration::from_secs(5)
    } else if elapsed < Duration::from_secs(30 * 60) {
        Duration::from_secs(30)
    } else {
        Duration::from_secs(60)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        extract::{Path, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
        Json, Router,
    };
    use serde::de::DeserializeOwned;
    use std::collections::BTreeSet;
    use tokio::{net::TcpListener, task::JoinHandle};

    #[test]
    fn btc_deposit_evidence_matches_router_typed_shape() {
        let output = tx_output();

        let evidence = btc_deposit_evidence("1BoatSLRHtKNngkdXEeobR76b53LETtpyT", &output);

        assert_typed_evidence::<BtcDepositObservedEvidence>(
            &evidence,
            &[
                "tx_hash",
                "address",
                "transfer_index",
                "amount",
                "confirmation_state",
                "block_height",
                "block_hash",
            ],
        );
    }

    #[test]
    fn hyperunit_deposit_credited_evidence_matches_router_typed_shape() {
        let output = tx_output();
        let status = unit_operation();
        let credit = hl_credit_event();

        let evidence = hyperunit_deposit_credited_evidence(
            "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            Some(&output),
            &status,
            &credit,
        );

        assert_typed_evidence::<HyperUnitDepositCreditedEvidence>(
            &evidence,
            &[
                "protocol_address",
                "btc_tx_hash",
                "btc_vout",
                "btc_amount",
                "btc_confirmations",
                "btc_block_height",
                "btc_block_hash",
                "hyperunit_operation_id",
                "hyperunit_status",
                "hyperunit_source_tx_hash",
                "hyperunit_destination_tx_hash",
                "hl_user",
                "hl_amount",
                "hl_credit_hash",
                "hl_credit_time_ms",
            ],
        );
    }

    #[test]
    fn hyperunit_withdrawal_ack_evidence_matches_router_typed_shape() {
        let status = unit_operation();

        let evidence =
            hyperunit_withdrawal_ack_evidence("1BoatSLRHtKNngkdXEeobR76b53LETtpyT", &status);

        assert_typed_evidence::<HyperUnitWithdrawalAcknowledgedEvidence>(
            &evidence,
            &[
                "protocol_address",
                "hyperunit_operation_id",
                "hyperunit_status",
                "destination_address",
                "amount",
                "btc_tx_hash",
                "broadcast_at",
            ],
        );
    }

    #[test]
    fn hyperunit_withdrawal_settled_evidence_matches_router_typed_shape() {
        let status = unit_operation();

        let evidence = hyperunit_withdrawal_settled_evidence(
            "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            &status,
            "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            1,
            50_000,
            6,
        );

        assert_typed_evidence::<HyperUnitWithdrawalSettledEvidence>(
            &evidence,
            &[
                "protocol_address",
                "hyperunit_operation_id",
                "hyperunit_status",
                "destination_address",
                "amount",
                "btc_tx_hash",
                "btc_vout",
                "btc_amount",
                "btc_confirmations",
            ],
        );
    }

    #[tokio::test]
    async fn lookup_statuses_select_operations_from_listing_endpoint() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let deposit = unit_operation_with_direction(
            "deposit-op",
            "0x73C1D4B7ADD80C7CFEA60A997C615064A424A844",
            "bitcoin",
            "hyperliquid",
        );
        let withdrawal = unit_operation_with_direction(
            "withdrawal-op",
            "0x73C1D4B7ADD80C7CFEA60A997C615064A424A844",
            "hyperliquid",
            "bitcoin",
        );
        let server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({
                "addresses": [],
                "operations": [deposit.clone(), withdrawal.clone()]
            }),
        )
        .await;
        let unit = HyperUnitClient::new(server.base_url()).expect("HyperUnit client");

        let deposit_status = lookup_deposit_status(&unit, protocol_address)
            .await
            .expect("deposit lookup");
        let withdrawal_status = lookup_withdrawal_status(&unit, protocol_address)
            .await
            .expect("withdrawal lookup");

        assert_eq!(deposit_status, Some(deposit));
        assert_eq!(withdrawal_status, Some(withdrawal));
    }

    #[tokio::test]
    async fn lookup_statuses_return_none_for_empty_operations_listing() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({
                "addresses": [],
                "operations": []
            }),
        )
        .await;
        let unit = HyperUnitClient::new(server.base_url()).expect("HyperUnit client");

        let deposit_status = lookup_deposit_status(&unit, protocol_address)
            .await
            .expect("deposit lookup");
        let withdrawal_status = lookup_withdrawal_status(&unit, protocol_address)
            .await
            .expect("withdrawal lookup");

        assert!(deposit_status.is_none());
        assert!(withdrawal_status.is_none());
    }

    #[tokio::test]
    async fn lookup_statuses_return_none_for_operations_404() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::NOT_FOUND,
            serde_json::json!({ "error": "not found" }),
        )
        .await;
        let unit = HyperUnitClient::new(server.base_url()).expect("HyperUnit client");

        let deposit_status = lookup_deposit_status(&unit, protocol_address)
            .await
            .expect("deposit lookup");
        let withdrawal_status = lookup_withdrawal_status(&unit, protocol_address)
            .await
            .expect("withdrawal lookup");

        assert!(deposit_status.is_none());
        assert!(withdrawal_status.is_none());
    }

    fn tx_output() -> TxOutput {
        TxOutput {
            txid: "0000000000000000000000000000000000000000000000000000000000000001"
                .parse()
                .expect("txid"),
            vout: 1,
            address: "1BoatSLRHtKNngkdXEeobR76b53LETtpyT"
                .parse()
                .expect("bitcoin address"),
            amount_sats: 50_000,
            block_height: Some(840_000),
            block_hash: Some(
                "0000000000000000000000000000000000000000000000000000000000000002"
                    .parse()
                    .expect("block hash"),
            ),
            block_time: None,
            confirmations: 6,
            removed: false,
        }
    }

    fn unit_operation() -> UnitOperation {
        serde_json::from_value(serde_json::json!({
            "operationId": "unit-op-1",
            "protocolAddress": "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            "destinationAddress": "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            "sourceAmount": "50000",
            "state": "credited",
            "sourceTxHash": "btc-source-tx",
            "destinationTxHash": "btc-destination-tx",
            "broadcastAt": "2026-05-12T00:00:00Z"
        }))
        .expect("unit operation")
    }

    fn unit_operation_with_direction(
        operation_id: &str,
        protocol_address: &str,
        source_chain: &str,
        destination_chain: &str,
    ) -> UnitOperation {
        serde_json::from_value(serde_json::json!({
            "operationId": operation_id,
            "protocolAddress": protocol_address,
            "sourceChain": source_chain,
            "destinationChain": destination_chain,
            "state": "done",
            "destinationTxHash": "0x9e36"
        }))
        .expect("unit operation")
    }

    fn hl_credit_event() -> HlTransferEvent {
        serde_json::from_value(serde_json::json!({
            "user": "0x1111111111111111111111111111111111111111",
            "time_ms": 1_778_522_898_534i64,
            "kind": { "type": "deposit" },
            "asset": "UBTC",
            "market": "spot",
            "amount_delta": "0.0005",
            "fee": null,
            "fee_token": null,
            "hash": "0x832fd0f4639c39c05011217e5b28840f9376cc24c4366660595a0cc158a88034",
            "observed_at_ms": 1_778_522_898_535i64
        }))
        .expect("HL credit event")
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

    #[derive(Clone)]
    struct HyperUnitOperationsMockState {
        expected_address: String,
        status: StatusCode,
        body: Value,
    }

    struct HyperUnitOperationsMockServer {
        base_url: String,
        handle: JoinHandle<()>,
    }

    impl HyperUnitOperationsMockServer {
        fn base_url(&self) -> &str {
            &self.base_url
        }
    }

    impl Drop for HyperUnitOperationsMockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_hyperunit_operations_server(
        expected_address: &str,
        status: StatusCode,
        body: Value,
    ) -> HyperUnitOperationsMockServer {
        let state = HyperUnitOperationsMockState {
            expected_address: expected_address.to_string(),
            status,
            body,
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HyperUnit mock server");
        let addr = listener.local_addr().expect("HyperUnit mock server addr");
        let app = Router::new()
            .route("/operations/:address", get(hyperunit_operations_handler))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("HyperUnit mock server should run");
        });
        HyperUnitOperationsMockServer {
            base_url: format!("http://{addr}"),
            handle,
        }
    }

    async fn hyperunit_operations_handler(
        State(state): State<HyperUnitOperationsMockState>,
        Path(address): Path<String>,
    ) -> Response {
        if address != state.expected_address {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("unexpected address {address}")
                })),
            )
                .into_response();
        }
        (state.status, Json(state.body)).into_response()
    }
}
