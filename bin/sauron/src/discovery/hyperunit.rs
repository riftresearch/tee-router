use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, TxHash};
use bitcoin::{address::NetworkUnchecked, Address as BitcoinAddress};
use bitcoin_indexer_client::TxOutput;
use bitcoin_receipt_watcher_client::{parse_txid, ByIdLookup as BitcoinByIdLookup};
use evm_receipt_watcher_client::{parse_tx_hash, ByIdLookup as EvmByIdLookup};
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
    provider_evm_receipts::EvmReceiptObserverClients,
    provider_operations::{ProviderOperationWatchStore, SharedProviderOperationWatchEntry},
    router_client::RouterClient,
};

const HYPERUNIT_OBSERVER_TICK: Duration = Duration::from_secs(1);
const HYPERUNIT_MAX_WAIT: Duration = Duration::from_secs(2 * 60 * 60);
const HYPERUNIT_STATUS_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERUNIT_HL_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERUNIT_BTC_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const HYPERUNIT_EVM_LOOKUP_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HyperUnitChain {
    Bitcoin,
    Ethereum,
    Base,
    Arbitrum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvmTxHashParseMode {
    Plain,
    HyperUnitSourceRef,
}

impl HyperUnitChain {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "bitcoin" => Some(Self::Bitcoin),
            "ethereum" | "eth" => Some(Self::Ethereum),
            "base" => Some(Self::Base),
            "arbitrum" | "arb" => Some(Self::Arbitrum),
            _ => None,
        }
    }

    fn as_hyperunit_str(self) -> &'static str {
        match self {
            Self::Bitcoin => "bitcoin",
            Self::Ethereum => "ethereum",
            Self::Base => "base",
            Self::Arbitrum => "arbitrum",
        }
    }

    fn evm_chain_id(self) -> Option<&'static str> {
        match self {
            Self::Bitcoin => None,
            Self::Ethereum => Some("evm:1"),
            Self::Base => Some("evm:8453"),
            Self::Arbitrum => Some("evm:42161"),
        }
    }
}

#[derive(Debug, Clone)]
struct EvmReceiptEvidence {
    chain_id: String,
    tx_hash: String,
    block_number: Option<u64>,
    block_hash: Option<String>,
    status: bool,
}

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
    evm: Option<EvmReceiptObserverClients>,
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

            let hints = match hyperunit_hints(&unit, &hl, &btc, evm.as_ref(), &operation).await {
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
    evm: Option<&EvmReceiptObserverClients>,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Vec<ProviderOperationHintRequest>> {
    match operation.operation_type {
        ProviderOperationType::UnitDeposit => deposit_hints(unit, hl, btc, evm, operation).await,
        ProviderOperationType::UnitWithdrawal => withdrawal_hints(unit, btc, evm, operation).await,
        _ => Ok(Vec::new()),
    }
}

async fn deposit_hints(
    unit: &HyperUnitClient,
    hl: &HlShimClient,
    btc: &BitcoinClients,
    evm: Option<&EvmReceiptObserverClients>,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Vec<ProviderOperationHintRequest>> {
    let Some(protocol_address) = protocol_address(operation) else {
        return Ok(Vec::new());
    };
    let request_source_chain = unit_operation_source_chain(operation);
    let request_source_is_bitcoin = request_source_chain
        .and_then(HyperUnitChain::parse)
        .is_some_and(|chain| chain == HyperUnitChain::Bitcoin);
    let output = if request_source_is_bitcoin {
        let amount = request_str(&operation.request, "amount").and_then(parse_u64);
        bitcoin_output(btc, protocol_address, amount.unwrap_or(1), u64::MAX).await?
    } else {
        None
    };
    if request_source_is_bitcoin && output.is_none() {
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
    let source_chain = match hyperunit_chain_from_status_or_request(
        status.source_chain.as_deref(),
        request_source_chain,
        operation,
        "source",
    ) {
        Some(chain) => chain,
        None => return Ok(hints),
    };
    let evm_source = match source_chain {
        HyperUnitChain::Bitcoin => None,
        HyperUnitChain::Ethereum | HyperUnitChain::Base | HyperUnitChain::Arbitrum => {
            match status.source_tx_hash.as_deref() {
                Some(source_tx_hash) => {
                    match lookup_evm_receipt_evidence(
                        evm,
                        source_chain,
                        source_tx_hash,
                        operation,
                        EvmTxHashParseMode::HyperUnitSourceRef,
                    )
                    .await
                    {
                        Ok(evidence) => evidence,
                        Err(error) => {
                            warn!(
                                operation_id = %operation.operation_id,
                                chain = source_chain.as_hyperunit_str(),
                                %error,
                                "HyperUnit deposit EVM source receipt lookup failed; emitting credited hint without EVM receipt evidence"
                            );
                            None
                        }
                    }
                }
                None => None,
            }
        }
    };
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
                source_chain,
                evm_source.as_ref(),
            ),
            "unit-deposit-credited",
        ));
    }
    Ok(hints)
}

async fn withdrawal_hints(
    unit: &HyperUnitClient,
    btc: &BitcoinClients,
    evm: Option<&EvmReceiptObserverClients>,
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
    let destination_chain = hyperunit_chain_from_status_or_request(
        status.destination_chain.as_deref(),
        unit_operation_destination_chain(operation),
        operation,
        "destination",
    );
    let mut hints = vec![hint_request(
        operation,
        ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
        hyperunit_withdrawal_ack_evidence(protocol_address, &status, destination_chain),
        "unit-withdrawal-ack",
    )];
    let Some(destination_chain) = destination_chain else {
        return Ok(hints);
    };
    let Some(destination_tx_hash) = status.destination_tx_hash.as_deref() else {
        return Ok(hints);
    };
    let Some(destination_address) = request_str(&operation.request, "dst_addr") else {
        return Ok(hints);
    };

    match destination_chain {
        HyperUnitChain::Bitcoin => {
            let Some(receipt_watcher) = btc.receipt_watcher.as_ref() else {
                warn!(
                    operation_id = %operation.operation_id,
                    destination_chain = destination_chain.as_hyperunit_str(),
                    "HyperUnit withdrawal destination is Bitcoin but no Bitcoin receipt watcher is configured"
                );
                return Ok(hints);
            };
            let Ok(txid) = parse_txid(destination_tx_hash) else {
                warn!(
                    operation_id = %operation.operation_id,
                    destination_tx_hash,
                    "HyperUnit withdrawal destination transaction hash is not a valid Bitcoin txid"
                );
                return Ok(hints);
            };
            let receipt = match timeout(
                HYPERUNIT_BTC_LOOKUP_TIMEOUT,
                receipt_watcher.lookup_by_id(txid),
            )
            .await
            {
                Ok(Ok(receipt)) => receipt,
                Ok(Err(source)) => {
                    warn!(
                        operation_id = %operation.operation_id,
                        destination_chain = destination_chain.as_hyperunit_str(),
                        %source,
                        "HyperUnit withdrawal Bitcoin receipt lookup failed; emitting acknowledgment without settled hint"
                    );
                    return Ok(hints);
                }
                Err(_) => None,
            };
            let Some((tx, confirmations)) = receipt else {
                return Ok(hints);
            };
            let Some((vout, amount_sats)) = payout_output(&tx, destination_address) else {
                return Ok(hints);
            };
            hints.push(hint_request(
                operation,
                ProviderOperationHintKind::HyperUnitWithdrawalSettled,
                hyperunit_withdrawal_btc_settled_evidence(
                    protocol_address,
                    &status,
                    destination_address,
                    vout,
                    amount_sats,
                    confirmations,
                ),
                "unit-withdrawal-settled",
            ));
        }
        HyperUnitChain::Ethereum | HyperUnitChain::Base | HyperUnitChain::Arbitrum => {
            let evm_receipt = match lookup_evm_receipt_evidence(
                evm,
                destination_chain,
                destination_tx_hash,
                operation,
                EvmTxHashParseMode::Plain,
            )
            .await
            {
                Ok(Some(evm_receipt)) => evm_receipt,
                Ok(None) => return Ok(hints),
                Err(error) => {
                    warn!(
                        operation_id = %operation.operation_id,
                        destination_chain = destination_chain.as_hyperunit_str(),
                        %error,
                        "HyperUnit withdrawal EVM receipt lookup failed; emitting acknowledgment without settled hint"
                    );
                    return Ok(hints);
                }
            };
            hints.push(hint_request(
                operation,
                ProviderOperationHintKind::HyperUnitWithdrawalSettled,
                hyperunit_withdrawal_evm_settled_evidence(
                    protocol_address,
                    &status,
                    destination_address,
                    destination_chain,
                    &evm_receipt,
                ),
                "unit-withdrawal-settled",
            ));
        }
    };
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
    source_chain: HyperUnitChain,
    evm_source: Option<&EvmReceiptEvidence>,
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
        source_chain: Some(source_chain.as_hyperunit_str().to_string()),
        evm_source_chain_id: evm_source.map(|evidence| evidence.chain_id.clone()),
        evm_source_tx_hash: evm_source.map(|evidence| evidence.tx_hash.clone()),
        evm_source_block_number: evm_source.and_then(|evidence| evidence.block_number),
        evm_source_block_hash: evm_source.and_then(|evidence| evidence.block_hash.clone()),
        evm_source_status: evm_source.map(|evidence| evidence.status),
        hl_user: format!("{:#x}", credit.user),
        hl_amount: credit.amount_delta.as_str().to_string(),
        hl_credit_hash: credit.hash.clone(),
        hl_credit_time_ms: credit.time_ms,
    })
}

fn hyperunit_withdrawal_ack_evidence(
    protocol_address: &str,
    status: &UnitOperation,
    destination_chain: Option<HyperUnitChain>,
) -> Value {
    typed_evidence(HyperUnitWithdrawalAcknowledgedEvidence {
        protocol_address: protocol_address.to_string(),
        hyperunit_operation_id: status.operation_id.clone(),
        hyperunit_status: status.state.clone(),
        destination_address: status.destination_address.clone(),
        amount: status.source_amount.clone(),
        destination_chain: destination_chain.map(|chain| chain.as_hyperunit_str().to_string()),
        destination_tx_hash: status.destination_tx_hash.clone(),
        btc_tx_hash: destination_chain
            .is_some_and(|chain| chain == HyperUnitChain::Bitcoin)
            .then(|| status.destination_tx_hash.clone())
            .flatten(),
        broadcast_at: status.broadcast_at.clone(),
    })
}

fn hyperunit_withdrawal_btc_settled_evidence(
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
        destination_chain: Some(HyperUnitChain::Bitcoin.as_hyperunit_str().to_string()),
        destination_tx_hash: status.destination_tx_hash.clone(),
        btc_tx_hash: status.destination_tx_hash.clone(),
        btc_vout: Some(u64::from(vout)),
        btc_amount: Some(amount_sats.to_string()),
        btc_confirmations: Some(confirmations),
        evm_chain_id: None,
        evm_tx_hash: None,
        evm_block_number: None,
        evm_block_hash: None,
        evm_status: None,
    })
}

fn hyperunit_withdrawal_evm_settled_evidence(
    protocol_address: &str,
    status: &UnitOperation,
    destination_address: &str,
    destination_chain: HyperUnitChain,
    evm: &EvmReceiptEvidence,
) -> Value {
    typed_evidence(HyperUnitWithdrawalSettledEvidence {
        protocol_address: protocol_address.to_string(),
        hyperunit_operation_id: status.operation_id.clone(),
        hyperunit_status: status.state.clone(),
        destination_address: destination_address.to_string(),
        amount: status.source_amount.clone(),
        destination_chain: Some(destination_chain.as_hyperunit_str().to_string()),
        destination_tx_hash: status.destination_tx_hash.clone(),
        btc_tx_hash: None,
        btc_vout: None,
        btc_amount: None,
        btc_confirmations: None,
        evm_chain_id: Some(evm.chain_id.clone()),
        evm_tx_hash: Some(evm.tx_hash.clone()),
        evm_block_number: evm.block_number,
        evm_block_hash: evm.block_hash.clone(),
        evm_status: Some(evm.status),
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

fn unit_operation_source_chain(operation: &SharedProviderOperationWatchEntry) -> Option<&str> {
    request_str(&operation.request, "src_chain")
        .or_else(|| request_str(&operation.request, "source_chain"))
}

fn unit_operation_destination_chain(operation: &SharedProviderOperationWatchEntry) -> Option<&str> {
    request_str(&operation.request, "dst_chain")
        .or_else(|| request_str(&operation.request, "destination_chain"))
}

fn hyperunit_chain_from_status_or_request(
    status_chain: Option<&str>,
    request_chain: Option<&str>,
    operation: &SharedProviderOperationWatchEntry,
    label: &'static str,
) -> Option<HyperUnitChain> {
    let chain = status_chain.or(request_chain);
    let Some(chain) = chain else {
        warn!(
            operation_id = %operation.operation_id,
            chain_label = label,
            "HyperUnit operation is missing chain name; emitting only currently available hints"
        );
        return None;
    };
    let parsed = HyperUnitChain::parse(chain);
    if parsed.is_none() {
        warn!(
            operation_id = %operation.operation_id,
            chain_label = label,
            chain,
            "HyperUnit operation used an unrecognized chain; emitting only currently available hints"
        );
    }
    parsed
}

async fn lookup_evm_receipt_evidence(
    clients: Option<&EvmReceiptObserverClients>,
    chain: HyperUnitChain,
    tx_hash: &str,
    operation: &SharedProviderOperationWatchEntry,
    parse_mode: EvmTxHashParseMode,
) -> Result<Option<EvmReceiptEvidence>> {
    let Some(chain_id) = chain.evm_chain_id() else {
        return Ok(None);
    };
    let Some(clients) = clients else {
        warn!(
            operation_id = %operation.operation_id,
            chain = chain.as_hyperunit_str(),
            chain_id,
            "HyperUnit operation needs EVM receipt lookup but no EVM receipt watcher clients are configured"
        );
        return Ok(None);
    };
    let Some(client) = clients.client_for_chain(chain_id) else {
        warn!(
            operation_id = %operation.operation_id,
            chain = chain.as_hyperunit_str(),
            chain_id,
            "HyperUnit operation needs EVM receipt lookup but no receipt watcher client is configured for this chain"
        );
        return Ok(None);
    };
    let Some((parsed_hash, _log_index)) = parse_evm_tx_hash(tx_hash, parse_mode) else {
        warn!(
            operation_id = %operation.operation_id,
            chain = chain.as_hyperunit_str(),
            tx_hash,
            "HyperUnit operation EVM transaction hash is invalid"
        );
        return Ok(None);
    };
    let receipt = match timeout(
        HYPERUNIT_EVM_LOOKUP_TIMEOUT,
        client.lookup_by_id(parsed_hash),
    )
    .await
    {
        Ok(Ok(receipt)) => receipt,
        Ok(Err(source)) => return Err(Error::EvmReceiptWatcher { source }),
        Err(_) => None,
    };
    let Some((receipt, _logs)) = receipt else {
        return Ok(None);
    };
    if !receipt.status() {
        return Ok(None);
    }
    Ok(Some(EvmReceiptEvidence {
        chain_id: chain_id.to_string(),
        tx_hash: format!("{:#x}", receipt.transaction_hash),
        block_number: receipt.block_number,
        block_hash: receipt.block_hash.map(|hash| format!("{hash:#x}")),
        status: receipt.status(),
    }))
}

fn parse_evm_tx_hash(value: &str, mode: EvmTxHashParseMode) -> Option<(TxHash, Option<u64>)> {
    match mode {
        EvmTxHashParseMode::Plain => parse_tx_hash(value).ok().map(|hash| (hash, None)),
        EvmTxHashParseMode::HyperUnitSourceRef => {
            let (tx_hash, log_index) = match value.split_once(':') {
                Some((tx_hash, log_index)) => (tx_hash, Some(log_index.parse::<u64>().ok()?)),
                None => (value, None),
            };
            parse_tx_hash(tx_hash).ok().map(|hash| (hash, log_index))
        }
    }
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
    let expected = raw_to_decimal(
        net,
        unit_asset_decimals(operation.asset.as_deref()?),
    );
    Some(truncate_decimal(
        &expected,
        usize::from(hyperliquid_client::wire::WIRE_DECIMALS),
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

fn truncate_decimal(value: &str, max_fraction: usize) -> String {
    match value.split_once('.') {
        Some((whole, fraction)) if fraction.len() > max_fraction => {
            let fraction = fraction[..max_fraction].trim_end_matches('0');
            if fraction.is_empty() {
                whole.to_string()
            } else {
                format!("{whole}.{fraction}")
            }
        }
        _ => value.to_string(),
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
        extract::{
            ws::{Message as WsMessage, WebSocketUpgrade},
            Path, State,
        },
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router,
    };
    use bitcoin::{
        absolute::LockTime, transaction::Version, Amount, OutPoint, ScriptBuf, Sequence,
        Transaction, TxIn, TxOut, Witness,
    };
    use router_core::models::ProviderOperationStatus;
    use router_temporal::WorkflowStepId;
    use serde::de::DeserializeOwned;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use tokio::{net::TcpListener, sync::watch, task::JoinHandle};

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
            HyperUnitChain::Bitcoin,
            None,
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
                "source_chain",
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

        let evidence = hyperunit_withdrawal_ack_evidence(
            "1BoatSLRHtKNngkdXEeobR76b53LETtpyT",
            &status,
            Some(HyperUnitChain::Bitcoin),
        );

        assert_typed_evidence::<HyperUnitWithdrawalAcknowledgedEvidence>(
            &evidence,
            &[
                "protocol_address",
                "hyperunit_operation_id",
                "hyperunit_status",
                "destination_address",
                "amount",
                "destination_chain",
                "destination_tx_hash",
                "btc_tx_hash",
                "broadcast_at",
            ],
        );
    }

    #[test]
    fn hyperunit_withdrawal_settled_evidence_matches_router_typed_shape() {
        let status = unit_operation();

        let evidence = hyperunit_withdrawal_btc_settled_evidence(
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
                "destination_chain",
                "destination_tx_hash",
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

    #[test]
    fn parse_evm_tx_hash_accepts_hyperunit_source_log_index_suffix() {
        let tx_hash = "0x471a57d43c1130735cb2c18a857cb284b67003bcccd62eb981d4e26264b822cb";
        let (parsed_hash, log_index) = parse_evm_tx_hash(
            &format!("{tx_hash}:0"),
            EvmTxHashParseMode::HyperUnitSourceRef,
        )
        .expect("HyperUnit source tx ref should parse");

        assert_eq!(format!("{parsed_hash:#x}"), tx_hash);
        assert_eq!(log_index, Some(0));
        assert!(parse_evm_tx_hash(&format!("{tx_hash}:0"), EvmTxHashParseMode::Plain).is_none());
    }

    #[test]
    fn expected_hl_credit_amount_truncates_eth_to_hl_wire_precision() {
        let operation = unit_operation_with_amount("eth", "57507271000000000");

        assert_eq!(
            expected_hl_credit_amount(&operation).as_deref(),
            Some("0.05750727")
        );
    }

    #[test]
    fn expected_hl_credit_amount_preserves_btc_wire_precision() {
        let operation = unit_operation_with_amount("btc", "12345678");

        assert_eq!(
            expected_hl_credit_amount(&operation).as_deref(),
            Some("0.12345678")
        );
    }

    #[test]
    fn truncate_decimal_limits_fraction_and_trims_trailing_zeroes() {
        assert_eq!(truncate_decimal("0.057507271", 8), "0.05750727");
        assert_eq!(truncate_decimal("0.0575072700000", 8), "0.05750727");
    }

    #[test]
    fn btc_source_deposit_credit_comparison_matches_exact_wire_amount() {
        let operation = unit_operation_with_amount("btc", "12345678");
        let expected = expected_hl_credit_amount(&operation).expect("expected amount");

        assert!(decimal_strings_equal("0.12345678", &expected));
    }

    #[tokio::test]
    async fn withdrawal_hints_emit_ack_and_settled_for_base_destination() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let destination = "0x1111111111111111111111111111111111111111";
        let tx_hash = "0xa4b0ef0fc686e5b300337b27ca6432bcd1e0879ca825c5c0bbe637dba62190ef";
        let status = unit_operation_with_tx_hashes(
            "withdrawal-base",
            protocol_address,
            "hyperliquid",
            "base",
            None,
            Some(tx_hash),
            Some(destination),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let evm_server = spawn_evm_receipt_watcher_server("base").await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let btc = dummy_bitcoin_clients(None);
        let evm = evm_clients_for(HyperUnitChain::Base, evm_server.base_url());
        let operation = provider_operation(
            ProviderOperationType::UnitWithdrawal,
            protocol_address,
            serde_json::json!({
                "dst_addr": destination,
                "dst_chain": "base",
                "amount": "1000000000000000000"
            }),
        );

        let hints = withdrawal_hints(&unit, &btc, Some(&evm), &operation)
            .await
            .expect("withdrawal hints");

        assert_hint_kinds(
            &hints,
            &[
                ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
                ProviderOperationHintKind::HyperUnitWithdrawalSettled,
            ],
        );
        let settled = hint_evidence(
            &hints,
            ProviderOperationHintKind::HyperUnitWithdrawalSettled,
        );
        assert_eq!(settled["destination_chain"], "base");
        assert_eq!(settled["destination_tx_hash"], tx_hash);
        assert_eq!(settled["evm_chain_id"], "evm:8453");
        assert_eq!(settled["evm_tx_hash"], tx_hash);
    }

    #[tokio::test]
    async fn withdrawal_hints_emit_ack_and_settled_for_ethereum_destination() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let destination = "0x2222222222222222222222222222222222222222";
        let tx_hash = "0xb4b0ef0fc686e5b300337b27ca6432bcd1e0879ca825c5c0bbe637dba62190ef";
        let status = unit_operation_with_tx_hashes(
            "withdrawal-ethereum",
            protocol_address,
            "hyperliquid",
            "ethereum",
            None,
            Some(tx_hash),
            Some(destination),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let evm_server = spawn_evm_receipt_watcher_server("ethereum").await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let btc = dummy_bitcoin_clients(None);
        let evm = evm_clients_for(HyperUnitChain::Ethereum, evm_server.base_url());
        let operation = provider_operation(
            ProviderOperationType::UnitWithdrawal,
            protocol_address,
            serde_json::json!({
                "dst_addr": destination,
                "dst_chain": "ethereum",
                "amount": "1000000000000000000"
            }),
        );

        let hints = withdrawal_hints(&unit, &btc, Some(&evm), &operation)
            .await
            .expect("withdrawal hints");

        assert_hint_kinds(
            &hints,
            &[
                ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
                ProviderOperationHintKind::HyperUnitWithdrawalSettled,
            ],
        );
        let settled = hint_evidence(
            &hints,
            ProviderOperationHintKind::HyperUnitWithdrawalSettled,
        );
        assert_eq!(settled["destination_chain"], "ethereum");
        assert_eq!(settled["evm_chain_id"], "evm:1");
    }

    #[tokio::test]
    async fn withdrawal_hints_emit_ack_when_evm_receipt_lookup_returns_none() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let destination = "0x2222222222222222222222222222222222222222";
        let tx_hash = "0xb4b0ef0fc686e5b300337b27ca6432bcd1e0879ca825c5c0bbe637dba62190ef";
        let status = unit_operation_with_tx_hashes(
            "withdrawal-ethereum-no-receipt",
            protocol_address,
            "hyperliquid",
            "ethereum",
            None,
            Some(tx_hash),
            Some(destination),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let btc = dummy_bitcoin_clients(None);
        let evm = evm_clients_without_configured_chains();
        let operation = provider_operation(
            ProviderOperationType::UnitWithdrawal,
            protocol_address,
            serde_json::json!({
                "dst_addr": destination,
                "dst_chain": "ethereum",
                "amount": "1000000000000000000"
            }),
        );

        let hints = withdrawal_hints(&unit, &btc, Some(&evm), &operation)
            .await
            .expect("withdrawal hints");

        assert_hint_kinds(
            &hints,
            &[ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged],
        );
        let ack = hint_evidence(
            &hints,
            ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
        );
        assert_eq!(ack["destination_chain"], "ethereum");
        assert_eq!(ack["destination_tx_hash"], tx_hash);
    }

    #[tokio::test]
    async fn withdrawal_hints_emit_ack_and_settled_for_bitcoin_destination() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let destination = "1BoatSLRHtKNngkdXEeobR76b53LETtpyT";
        let txid = "0000000000000000000000000000000000000000000000000000000000000001";
        let status = unit_operation_with_tx_hashes(
            "withdrawal-bitcoin",
            protocol_address,
            "hyperliquid",
            "bitcoin",
            None,
            Some(txid),
            Some(destination),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let tx = bitcoin_payout_tx(destination, 50_000);
        let btc_server = spawn_bitcoin_receipt_watcher_server(tx).await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let btc = dummy_bitcoin_clients(Some(btc_server.base_url()));
        let operation = provider_operation(
            ProviderOperationType::UnitWithdrawal,
            protocol_address,
            serde_json::json!({
                "dst_addr": destination,
                "dst_chain": "bitcoin",
                "amount": "50000"
            }),
        );

        let hints = withdrawal_hints(&unit, &btc, None, &operation)
            .await
            .expect("withdrawal hints");

        assert_hint_kinds(
            &hints,
            &[
                ProviderOperationHintKind::HyperUnitWithdrawalAcknowledged,
                ProviderOperationHintKind::HyperUnitWithdrawalSettled,
            ],
        );
        let settled = hint_evidence(
            &hints,
            ProviderOperationHintKind::HyperUnitWithdrawalSettled,
        );
        assert_eq!(settled["destination_chain"], "bitcoin");
        assert_eq!(settled["btc_tx_hash"], txid);
        assert_eq!(settled["btc_vout"], 0);
    }

    #[tokio::test]
    async fn deposit_hints_emit_credited_for_base_source() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let user = "0x1111111111111111111111111111111111111111";
        let tx_hash = "0xc4b0ef0fc686e5b300337b27ca6432bcd1e0879ca825c5c0bbe637dba62190ef";
        let source_tx_ref = format!("{tx_hash}:0");
        let status = unit_operation_with_tx_hashes(
            "deposit-base",
            protocol_address,
            "base",
            "hyperliquid",
            Some(&source_tx_ref),
            None,
            Some(user),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let evm_server = spawn_evm_receipt_watcher_server("base").await;
        let hl_server = spawn_hl_transfers_server(hl_credit_event()).await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let hl = HlShimClient::new(hl_server.base_url()).expect("HL shim client");
        let btc = dummy_bitcoin_clients(None);
        let evm = evm_clients_for(HyperUnitChain::Base, evm_server.base_url());
        let operation = provider_operation(
            ProviderOperationType::UnitDeposit,
            protocol_address,
            serde_json::json!({
                "src_chain": "base",
                "dst_chain": "hyperliquid",
                "dst_addr": user,
                "amount": "50000"
            }),
        );

        let hints = deposit_hints(&unit, &hl, &btc, Some(&evm), &operation)
            .await
            .expect("deposit hints");

        assert_hint_kinds(
            &hints,
            &[ProviderOperationHintKind::HyperUnitDepositCredited],
        );
        let credited = hint_evidence(&hints, ProviderOperationHintKind::HyperUnitDepositCredited);
        assert_eq!(credited["source_chain"], "base");
        assert_eq!(credited["hyperunit_source_tx_hash"], source_tx_ref.as_str());
        assert_eq!(credited["evm_source_chain_id"], "evm:8453");
        assert_eq!(credited["evm_source_tx_hash"], tx_hash);
    }

    #[tokio::test]
    async fn deposit_hints_emit_credited_when_evm_source_receipt_lookup_returns_none() {
        let protocol_address = "0x73c1d4b7add80c7cfea60a997c615064a424a844";
        let user = "0x1111111111111111111111111111111111111111";
        let tx_hash = "0xc4b0ef0fc686e5b300337b27ca6432bcd1e0879ca825c5c0bbe637dba62190ef";
        let status = unit_operation_with_tx_hashes(
            "deposit-base-no-receipt",
            protocol_address,
            "base",
            "hyperliquid",
            Some(tx_hash),
            None,
            Some(user),
        );
        let unit_server = spawn_hyperunit_operations_server(
            protocol_address,
            StatusCode::OK,
            serde_json::json!({ "addresses": [], "operations": [status] }),
        )
        .await;
        let hl_server = spawn_hl_transfers_server(hl_credit_event()).await;
        let unit = HyperUnitClient::new(unit_server.base_url()).expect("HyperUnit client");
        let hl = HlShimClient::new(hl_server.base_url()).expect("HL shim client");
        let btc = dummy_bitcoin_clients(None);
        let evm = evm_clients_without_configured_chains();
        let operation = provider_operation(
            ProviderOperationType::UnitDeposit,
            protocol_address,
            serde_json::json!({
                "src_chain": "base",
                "dst_chain": "hyperliquid",
                "dst_addr": user,
                "amount": "50000"
            }),
        );

        let hints = deposit_hints(&unit, &hl, &btc, Some(&evm), &operation)
            .await
            .expect("deposit hints");

        assert_hint_kinds(
            &hints,
            &[ProviderOperationHintKind::HyperUnitDepositCredited],
        );
        let credited = hint_evidence(&hints, ProviderOperationHintKind::HyperUnitDepositCredited);
        assert_eq!(credited["source_chain"], "base");
        assert!(credited.get("evm_source_tx_hash").is_none());
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

    fn unit_operation_with_tx_hashes(
        operation_id: &str,
        protocol_address: &str,
        source_chain: &str,
        destination_chain: &str,
        source_tx_hash: Option<&str>,
        destination_tx_hash: Option<&str>,
        destination_address: Option<&str>,
    ) -> UnitOperation {
        serde_json::from_value(serde_json::json!({
            "operationId": operation_id,
            "protocolAddress": protocol_address,
            "sourceChain": source_chain,
            "destinationChain": destination_chain,
            "sourceAddress": protocol_address,
            "destinationAddress": destination_address,
            "sourceAmount": "50000",
            "state": "done",
            "sourceTxHash": source_tx_hash,
            "destinationTxHash": destination_tx_hash,
            "broadcastAt": "2026-05-12T00:00:00Z"
        }))
        .expect("unit operation")
    }

    fn unit_operation_with_amount(asset: &str, source_amount: &str) -> UnitOperation {
        serde_json::from_value(serde_json::json!({
            "operationId": "unit-op-amount",
            "protocolAddress": "0x73c1d4b7add80c7cfea60a997c615064a424a844",
            "sourceChain": "ethereum",
            "destinationChain": "hyperliquid",
            "sourceAmount": source_amount,
            "asset": asset,
            "state": "done"
        }))
        .expect("unit operation")
    }

    fn provider_operation(
        operation_type: ProviderOperationType,
        protocol_address: &str,
        request: Value,
    ) -> SharedProviderOperationWatchEntry {
        Arc::new(crate::provider_operations::ProviderOperationWatchEntry {
            operation_id: Uuid::now_v7(),
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "unit".to_string(),
            operation_type,
            provider_ref: Some(protocol_address.to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request,
            response: serde_json::json!({}),
            observed_state: serde_json::json!({}),
            execution_step_request: serde_json::json!({}),
            updated_at: chrono::DateTime::parse_from_rfc3339("2026-05-12T00:00:00Z")
                .expect("timestamp")
                .with_timezone(&chrono::Utc),
        })
    }

    fn dummy_bitcoin_clients(receipt_watcher_url: Option<&str>) -> BitcoinClients {
        BitcoinClients {
            indexer: Arc::new(
                bitcoin_indexer_client::BitcoinIndexerClient::new("http://127.0.0.1:1")
                    .expect("dummy Bitcoin indexer client"),
            ),
            receipt_watcher: receipt_watcher_url.map(|url| {
                Arc::new(
                    bitcoin_receipt_watcher_client::BitcoinReceiptWatcherClient::new(
                        url, "bitcoin",
                    )
                    .expect("Bitcoin receipt watcher client"),
                )
            }),
        }
    }

    fn evm_clients_for(chain: HyperUnitChain, url: &str) -> EvmReceiptObserverClients {
        let client = Arc::new(
            evm_receipt_watcher_client::EvmReceiptWatcherClient::new(url, chain.as_hyperunit_str())
                .expect("EVM receipt watcher client"),
        );
        EvmReceiptObserverClients {
            ethereum: (chain == HyperUnitChain::Ethereum).then(|| client.clone()),
            base: (chain == HyperUnitChain::Base).then(|| client.clone()),
            arbitrum: (chain == HyperUnitChain::Arbitrum).then_some(client),
        }
    }

    fn evm_clients_without_configured_chains() -> EvmReceiptObserverClients {
        EvmReceiptObserverClients {
            ethereum: None,
            base: None,
            arbitrum: None,
        }
    }

    fn bitcoin_payout_tx(destination_address: &str, amount_sats: u64) -> Transaction {
        let address = destination_address
            .parse::<BitcoinAddress<NetworkUnchecked>>()
            .expect("Bitcoin destination")
            .assume_checked();
        Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(amount_sats),
                script_pubkey: address.script_pubkey(),
            }],
        }
    }

    fn assert_hint_kinds(
        hints: &[ProviderOperationHintRequest],
        expected: &[ProviderOperationHintKind],
    ) {
        let mut actual = hints.iter().map(|hint| hint.hint_kind).collect::<Vec<_>>();
        actual.sort_by_key(|kind| kind.to_db_string());
        let mut expected = expected.to_vec();
        expected.sort_by_key(|kind| kind.to_db_string());
        assert_eq!(actual, expected);
    }

    fn hint_evidence(
        hints: &[ProviderOperationHintRequest],
        kind: ProviderOperationHintKind,
    ) -> &Value {
        &hints
            .iter()
            .find(|hint| hint.hint_kind == kind)
            .expect("hint kind should be emitted")
            .evidence
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

    #[derive(Clone)]
    struct EvmReceiptWatcherMockState {
        chain: String,
        events: watch::Sender<Option<String>>,
    }

    struct EvmReceiptWatcherMockServer {
        base_url: String,
        handle: JoinHandle<()>,
    }

    impl EvmReceiptWatcherMockServer {
        fn base_url(&self) -> &str {
            &self.base_url
        }
    }

    impl Drop for EvmReceiptWatcherMockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_evm_receipt_watcher_server(chain: &str) -> EvmReceiptWatcherMockServer {
        let (events, _) = watch::channel(None);
        let state = EvmReceiptWatcherMockState {
            chain: chain.to_string(),
            events,
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind EVM receipt watcher mock server");
        let addr = listener.local_addr().expect("EVM receipt watcher addr");
        let app = Router::new()
            .route("/subscribe", get(evm_receipt_subscribe_handler))
            .route("/watch", post(evm_receipt_watch_handler))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("EVM receipt watcher mock server should run");
        });
        EvmReceiptWatcherMockServer {
            base_url: format!("http://{addr}"),
            handle,
        }
    }

    async fn evm_receipt_subscribe_handler(
        State(state): State<EvmReceiptWatcherMockState>,
        ws: WebSocketUpgrade,
    ) -> Response {
        ws.on_upgrade(move |mut socket| async move {
            let mut events = state.events.subscribe();
            let initial_event = events.borrow().clone();
            if let Some(event) = initial_event {
                let _ = socket.send(WsMessage::Text(event)).await;
            }
            while events.changed().await.is_ok() {
                let event = events.borrow().clone();
                let Some(event) = event else {
                    continue;
                };
                if socket.send(WsMessage::Text(event)).await.is_err() {
                    break;
                }
            }
        })
        .into_response()
    }

    async fn evm_receipt_watch_handler(
        State(state): State<EvmReceiptWatcherMockState>,
        Json(request): Json<evm_receipt_watcher_client::WatchRequest>,
    ) -> Response {
        if request.chain != state.chain {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("unexpected chain {}", request.chain)
                })),
            )
                .into_response();
        }
        let tx_hash = format!("{:#x}", request.tx_hash);
        let event = serde_json::json!({
            "chain": request.chain,
            "tx_hash": tx_hash,
            "requesting_operation_id": request.requesting_operation_id,
            "status": "confirmed",
            "receipt": evm_receipt_json(&tx_hash),
            "logs": []
        });
        state
            .events
            .send(Some(event.to_string()))
            .expect("send EVM receipt event");
        Json(evm_receipt_watcher_client::WatchResponse {
            chain: state.chain,
            tx_hash: request.tx_hash,
            requesting_operation_id: request.requesting_operation_id,
            watched: true,
        })
        .into_response()
    }

    fn evm_receipt_json(tx_hash: &str) -> Value {
        serde_json::json!({
            "transactionHash": tx_hash,
            "blockHash": "0x4acbdefb861ef4adedb135ca52865f6743451bfbfa35db78076f881a40401a5e",
            "blockNumber": "0x129f4b9",
            "logsBloom": format!("0x{}", "0".repeat(512)),
            "gasUsed": "0xbde1",
            "contractAddress": null,
            "cumulativeGasUsed": "0xa42aec",
            "transactionIndex": "0x7f",
            "from": "0x9a53bfba35269414f3b2d20b52ca01b15932c7b2",
            "to": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "type": "0x2",
            "effectiveGasPrice": "0xfb0f6e8c9",
            "logs": [],
            "status": "0x1"
        })
    }

    #[derive(Clone)]
    struct BitcoinReceiptWatcherMockState {
        tx: Transaction,
        events: watch::Sender<Option<String>>,
    }

    struct BitcoinReceiptWatcherMockServer {
        base_url: String,
        handle: JoinHandle<()>,
    }

    impl BitcoinReceiptWatcherMockServer {
        fn base_url(&self) -> &str {
            &self.base_url
        }
    }

    impl Drop for BitcoinReceiptWatcherMockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_bitcoin_receipt_watcher_server(
        tx: Transaction,
    ) -> BitcoinReceiptWatcherMockServer {
        let (events, _) = watch::channel(None);
        let state = BitcoinReceiptWatcherMockState { tx, events };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Bitcoin receipt watcher mock server");
        let addr = listener.local_addr().expect("Bitcoin receipt watcher addr");
        let app = Router::new()
            .route("/subscribe", get(bitcoin_receipt_subscribe_handler))
            .route("/watch", post(bitcoin_receipt_watch_handler))
            .with_state(state);
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("Bitcoin receipt watcher mock server should run");
        });
        BitcoinReceiptWatcherMockServer {
            base_url: format!("http://{addr}"),
            handle,
        }
    }

    async fn bitcoin_receipt_subscribe_handler(
        State(state): State<BitcoinReceiptWatcherMockState>,
        ws: WebSocketUpgrade,
    ) -> Response {
        ws.on_upgrade(move |mut socket| async move {
            let mut events = state.events.subscribe();
            let initial_event = events.borrow().clone();
            if let Some(event) = initial_event {
                let _ = socket.send(WsMessage::Text(event)).await;
            }
            while events.changed().await.is_ok() {
                let event = events.borrow().clone();
                let Some(event) = event else {
                    continue;
                };
                if socket.send(WsMessage::Text(event)).await.is_err() {
                    break;
                }
            }
        })
        .into_response()
    }

    async fn bitcoin_receipt_watch_handler(
        State(state): State<BitcoinReceiptWatcherMockState>,
        Json(request): Json<bitcoin_receipt_watcher_client::WatchRequest>,
    ) -> Response {
        if request.chain != "bitcoin" {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": format!("unexpected chain {}", request.chain)
                })),
            )
                .into_response();
        }
        let event = bitcoin_receipt_watcher_client::WatchEvent {
            chain: request.chain.clone(),
            txid: request.txid,
            requesting_operation_id: request.requesting_operation_id.clone(),
            status: bitcoin_receipt_watcher_client::WatchStatus::Confirmed,
            receipt: Some(bitcoin_receipt_watcher_client::Receipt {
                tx: state.tx,
                block_hash: "0000000000000000000000000000000000000000000000000000000000000002"
                    .parse()
                    .expect("block hash"),
                block_height: 840_000,
                confirmations: 6,
            }),
        };
        state
            .events
            .send(Some(
                serde_json::to_string(&event).expect("serialize Bitcoin receipt event"),
            ))
            .expect("send Bitcoin receipt event");
        Json(bitcoin_receipt_watcher_client::WatchResponse {
            chain: request.chain,
            txid: request.txid,
            requesting_operation_id: request.requesting_operation_id,
            watched: true,
        })
        .into_response()
    }

    #[derive(Clone)]
    struct HlTransfersMockState {
        event: HlTransferEvent,
    }

    struct HlTransfersMockServer {
        base_url: String,
        handle: JoinHandle<()>,
    }

    impl HlTransfersMockServer {
        fn base_url(&self) -> &str {
            &self.base_url
        }
    }

    impl Drop for HlTransfersMockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_hl_transfers_server(event: HlTransferEvent) -> HlTransfersMockServer {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HL transfers mock server");
        let addr = listener.local_addr().expect("HL transfers addr");
        let app = Router::new()
            .route("/transfers", get(hl_transfers_handler))
            .with_state(HlTransfersMockState { event });
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("HL transfers mock server should run");
        });
        HlTransfersMockServer {
            base_url: format!("http://{addr}"),
            handle,
        }
    }

    async fn hl_transfers_handler(State(state): State<HlTransfersMockState>) -> Response {
        Json(serde_json::json!({
            "events": [state.event],
            "next_cursor": "",
            "has_more": false
        }))
        .into_response()
    }
}
