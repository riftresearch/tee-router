use alloy::{
    hex,
    primitives::{Address, B256, U256},
    providers::{DynProvider, Provider, ProviderBuilder},
    rpc::types::Filter,
    sol_types::{SolEvent, SolValue},
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{sync::Mutex, task::JoinHandle};

pub(crate) mod contract;

use crate::mock_integrators::cctp::contract::MockCctpTokenMessengerV2::DepositForBurn;
use crate::mock_integrators::{
    bytes32_to_evm_address, error_response, mock_evm_indexer_initial_last_scanned,
    MockIntegratorConfig,
};

#[derive(Debug, Clone)]
pub struct MockCctpChainConfig {
    pub token_messenger_address: String,
    pub evm_rpc_url: String,
}

/// Iris V2 `delayReason` values the mock can simulate. Real Iris emits one of
/// these on a `pending_confirmations` response when a burn is stalled.
///
/// `AmountAboveMax` is the only terminal reason (the burn amount exceeds
/// Circle's per-tx max); the others mean Iris is still waiting on fee bumps
/// or daily-allowance windows and will eventually attest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockCctpDelayReason {
    AmountAboveMax,
    InsufficientFee,
    InsufficientAllowanceAvailable,
}

impl MockCctpDelayReason {
    /// Snake-case wire string matching Circle's `delayReason` enum.
    pub fn as_iris_str(self) -> &'static str {
        match self {
            Self::AmountAboveMax => "amount_above_max",
            Self::InsufficientFee => "insufficient_fee",
            Self::InsufficientAllowanceAvailable => "insufficient_allowance_available",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MockCctpBurnRecord {
    pub source_domain: u32,
    pub destination_domain: u32,
    /// CCTP v2 event nonce. The real `DepositForBurn` event carries no nonce
    /// (the nonce lives in the message body and is surfaced by Circle's Iris
    /// API as a hex-string `eventNonce`), so the mock derives a deterministic
    /// nonce from the burn transaction hash.
    pub nonce: String,
    pub burn_token: Address,
    pub destination_token: Address,
    pub depositor: Address,
    pub mint_recipient: Address,
    pub amount: U256,
    pub burn_tx_hash: String,
    pub block_number: u64,
    pub indexed_at: DateTime<Utc>,
}

/// Per-venue state for the CCTP mock: the recorded burns and the attestation
/// timing/delay tunables.
pub(crate) struct CctpMockState {
    pub(crate) burns: Mutex<BTreeMap<String, MockCctpBurnRecord>>,
    pub(crate) attestation_latency: Duration,
    pub(crate) attestation_delay_reason: Option<MockCctpDelayReason>,
}

/// Builds the CCTP (Iris) mock router. Mounted under `/cctp`; receives its own
/// [`CctpMockState`] substate at nest time.
pub(crate) fn router() -> Router<Arc<CctpMockState>> {
    Router::new()
        .route("/v2/messages/:source_domain", get(mock_cctp_messages))
        .route(
            "/v2/burn/USDC/fees/:source_domain/:destination_domain",
            get(mock_cctp_burn_fees),
        )
}

#[derive(Debug, Deserialize)]
pub(crate) struct MockCctpMessagesQuery {
    #[serde(rename = "transactionHash")]
    transaction_hash: Option<String>,
    nonce: Option<String>,
}

pub(crate) async fn mock_cctp_messages(
    State(state): State<Arc<CctpMockState>>,
    Path(source_domain): Path<u32>,
    Query(query): Query<MockCctpMessagesQuery>,
) -> impl IntoResponse {
    let burns = state.burns.lock().await;
    let record = if let Some(tx_hash) = query.transaction_hash.as_deref() {
        burns.get(tx_hash).cloned()
    } else if let Some(nonce) = query.nonce.as_deref() {
        burns.values().find(|record| record.nonce == nonce).cloned()
    } else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "mock CCTP /v2/messages requires transactionHash or nonce".to_string(),
        );
    };
    let Some(record) = record.filter(|record| record.source_domain == source_domain) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "not found"
            })),
        )
            .into_response();
    };
    let message = (
        record.destination_token,
        record.mint_recipient,
        record.amount,
    )
        .abi_encode();
    let message_hex = format!("0x{}", hex::encode(message));
    let decoded_message = json!({
        "decodedMessageBody": {
            "burnToken": format!("{:#x}", record.burn_token),
            "mintRecipient": format!("{:#x}", record.mint_recipient),
            "amount": record.amount.to_string(),
            "messageSender": format!("{:#x}", record.depositor)
        }
    });
    if let Some(reason) = state.attestation_delay_reason {
        // Real Iris V2 leaves the status as `pending_confirmations` and signals
        // failure via `delayReason`. The only terminal reason is
        // `amount_above_max`; the others mean Iris is still waiting on fee
        // bumps or daily-allowance windows and may eventually attest.
        return (
            StatusCode::OK,
            Json(json!({
                "messages": [{
                    "message": message_hex,
                    "attestation": null,
                    "eventNonce": record.nonce.clone(),
                    "cctpVersion": 2,
                    "status": "pending_confirmations",
                    "delayReason": reason.as_iris_str(),
                    "decodedMessage": decoded_message
                }]
            })),
        )
            .into_response();
    }
    let attestation_ready = state.attestation_latency.is_zero()
        || Utc::now()
            .signed_duration_since(record.indexed_at)
            .to_std()
            .is_ok_and(|age| age >= state.attestation_latency);
    if !attestation_ready {
        return (
            StatusCode::OK,
            Json(json!({
                "messages": [{
                    "message": message_hex,
                    "attestation": null,
                    "eventNonce": record.nonce.clone(),
                    "cctpVersion": 2,
                    "status": "pending_confirmations",
                    "decodedMessage": decoded_message
                }]
            })),
        )
            .into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "messages": [{
                "message": message_hex,
                "attestation": format!("0x{}", "11".repeat(65)),
                "eventNonce": record.nonce.clone(),
                "cctpVersion": 2,
                "status": "complete",
                "decodedMessage": decoded_message
            }]
        })),
    )
        .into_response()
}

pub(crate) async fn mock_cctp_burn_fees(
    Path((_source_domain, _destination_domain)): Path<(u32, u32)>,
) -> impl IntoResponse {
    (
        StatusCode::OK,
        Json(json!([
            {
                "finalityThreshold": 1000,
                "minimumFee": 1.3
            },
            {
                "finalityThreshold": 2000,
                "minimumFee": 0
            }
        ])),
    )
        .into_response()
}

pub(crate) async fn maybe_spawn_cctp_burn_indexer(
    config: &MockIntegratorConfig,
    state: Arc<CctpMockState>,
) -> eyre::Result<(Vec<tokio::sync::oneshot::Sender<()>>, Vec<JoinHandle<()>>)> {
    let mut destination_tokens = BTreeMap::<u32, Address>::new();
    for (domain, token_str) in &config.cctp_destination_token_addresses {
        let token = token_str.parse().map_err(|err| {
            eyre::eyre!("mock cctp indexer: invalid destination token address {token_str}: {err}")
        })?;
        destination_tokens.insert(*domain, token);
    }

    if let Some(destination_token_str) = config.cctp_destination_token_address.as_deref() {
        let destination_token = destination_token_str.parse().map_err(|err| {
            eyre::eyre!(
                "mock cctp indexer: invalid legacy destination token address {destination_token_str}: {err}"
            )
        })?;
        for domain in [0_u32, 3, 6] {
            destination_tokens
                .entry(domain)
                .or_insert(destination_token);
        }
    }

    if destination_tokens.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut source_chains = config.cctp_chains.clone();
    if source_chains.is_empty() {
        if let (Some(rpc_url), Some(token_messenger_address)) = (
            config.cctp_evm_rpc_url.clone(),
            config.cctp_token_messenger_address.clone(),
        ) {
            source_chains.insert(
                0,
                MockCctpChainConfig {
                    token_messenger_address,
                    evm_rpc_url: rpc_url,
                },
            );
        }
    }

    let mut shutdowns = Vec::new();
    let mut handles = Vec::new();
    for (configured_chain_id, chain_config) in source_chains {
        let token_messenger_address: Address = chain_config
            .token_messenger_address
            .parse()
            .map_err(|err| {
                eyre::eyre!(
                    "mock cctp indexer: invalid TokenMessengerV2 address {}: {err}",
                    chain_config.token_messenger_address
                )
            })?;
        let provider: DynProvider = ProviderBuilder::new()
            .connect(&chain_config.evm_rpc_url)
            .await?
            .erased();
        let chain_id = provider.get_chain_id().await?;
        let source_domain = cctp_domain_for_chain_id(chain_id).ok_or_else(|| {
            eyre::eyre!("mock cctp indexer: unsupported source chain id {chain_id}")
        })?;
        if configured_chain_id != 0 && configured_chain_id != chain_id {
            tracing::warn!(
                configured_chain_id,
                rpc_chain_id = chain_id,
                "mock cctp indexer source chain id differs from RPC chain id"
            );
        }

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(run_cctp_burn_indexer(
            provider,
            token_messenger_address,
            destination_tokens.clone(),
            source_domain,
            state.clone(),
            shutdown_rx,
        ));
        shutdowns.push(shutdown_tx);
        handles.push(handle);
    }

    Ok((shutdowns, handles))
}

async fn run_cctp_burn_indexer(
    provider: DynProvider,
    token_messenger_address: Address,
    destination_token_addresses: BTreeMap<u32, Address>,
    source_domain: u32,
    state: Arc<CctpMockState>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    let signature: B256 = DepositForBurn::SIGNATURE_HASH;
    let poll = Duration::from_millis(100);
    let mut last_scanned: u64 = mock_evm_indexer_initial_last_scanned();
    loop {
        tokio::select! {
            _ = &mut shutdown => return,
            _ = tokio::time::sleep(poll) => {}
        }
        let head = match provider.get_block_number().await {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(%err, "mock cctp indexer: get_block_number failed");
                continue;
            }
        };
        if head <= last_scanned {
            continue;
        }
        let filter = Filter::new()
            .address(token_messenger_address)
            .event_signature(signature)
            .from_block(last_scanned + 1)
            .to_block(head);
        let logs = match provider.get_logs(&filter).await {
            Ok(logs) => logs,
            Err(err) => {
                tracing::warn!(%err, "mock cctp indexer: get_logs failed");
                continue;
            }
        };
        for log in logs {
            let decoded = match DepositForBurn::decode_log(&log.inner) {
                Ok(event) => event,
                Err(err) => {
                    tracing::warn!(%err, "mock cctp indexer: decode DepositForBurn failed");
                    continue;
                }
            };
            let event = decoded.data;
            let Some(destination_token_address) = destination_token_addresses
                .get(&event.destinationDomain)
                .copied()
            else {
                tracing::warn!(
                    source_domain,
                    destination_domain = event.destinationDomain,
                    "mock cctp indexer: no destination token configured for burn"
                );
                continue;
            };
            let Some(burn_tx_hash) = log.transaction_hash.map(|h| format!("{h:#x}")) else {
                tracing::warn!("mock cctp indexer: DepositForBurn log missing transaction hash");
                continue;
            };
            let Some(block_number) = log.block_number else {
                tracing::warn!("mock cctp indexer: DepositForBurn log missing block number");
                continue;
            };
            let record = MockCctpBurnRecord {
                source_domain,
                destination_domain: event.destinationDomain,
                nonce: burn_tx_hash.clone(),
                burn_token: event.burnToken,
                destination_token: destination_token_address,
                depositor: event.depositor,
                mint_recipient: bytes32_to_evm_address(event.mintRecipient),
                amount: event.amount,
                burn_tx_hash: burn_tx_hash.clone(),
                block_number,
                indexed_at: Utc::now(),
            };
            state.burns.lock().await.insert(burn_tx_hash, record);
        }
        last_scanned = head;
    }
}

pub(crate) fn cctp_domain_for_chain_id(chain_id: u64) -> Option<u32> {
    match chain_id {
        1 => Some(0),
        42161 => Some(3),
        8453 => Some(6),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use chrono::Duration as ChronoDuration;
    use serde_json::Value;

    fn cctp_state(
        attestation_latency: Duration,
        attestation_delay_reason: Option<MockCctpDelayReason>,
    ) -> Arc<CctpMockState> {
        Arc::new(CctpMockState {
            burns: Mutex::default(),
            attestation_latency,
            attestation_delay_reason,
        })
    }

    #[tokio::test]
    async fn mock_cctp_messages_honor_attestation_latency() {
        let state = cctp_state(Duration::from_secs(60), None);
        state
            .burns
            .lock()
            .await
            .insert("0xburn".to_string(), mock_cctp_burn_record(Utc::now()));

        let pending = mock_cctp_message_body(state.clone(), "0xburn").await;
        assert_eq!(pending["messages"][0]["status"], "pending_confirmations");
        assert!(pending["messages"][0]["attestation"].is_null());

        state.burns.lock().await.insert(
            "0xburn".to_string(),
            mock_cctp_burn_record(Utc::now() - ChronoDuration::seconds(61)),
        );
        let complete = mock_cctp_message_body(state, "0xburn").await;
        assert_eq!(complete["messages"][0]["status"], "complete");
        assert!(complete["messages"][0]["attestation"]
            .as_str()
            .is_some_and(|value| value.starts_with("0x")));
    }

    #[tokio::test]
    async fn mock_cctp_messages_can_return_amount_above_max_delay_reason() {
        let state = cctp_state(Duration::ZERO, Some(MockCctpDelayReason::AmountAboveMax));
        state
            .burns
            .lock()
            .await
            .insert("0xburn".to_string(), mock_cctp_burn_record(Utc::now()));

        let body = mock_cctp_message_body(state, "0xburn").await;
        // Real Iris V2 keeps status as `pending_confirmations` and surfaces
        // terminal failure via `delayReason: amount_above_max`.
        assert_eq!(body["messages"][0]["status"], "pending_confirmations");
        assert_eq!(body["messages"][0]["delayReason"], "amount_above_max");
        assert!(body["messages"][0]["attestation"].is_null());
        // The legacy free-form `error` field is no longer emitted.
        assert!(body["messages"][0].get("error").is_none());
    }

    async fn mock_cctp_message_body(state: Arc<CctpMockState>, tx_hash: &str) -> Value {
        let response = mock_cctp_messages(
            State(state),
            Path(0),
            Query(MockCctpMessagesQuery {
                transaction_hash: Some(tx_hash.to_string()),
                nonce: None,
            }),
        )
        .await
        .into_response();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        assert_eq!(
            status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("cctp message json")
    }

    fn mock_cctp_burn_record(indexed_at: DateTime<Utc>) -> MockCctpBurnRecord {
        MockCctpBurnRecord {
            source_domain: 0,
            destination_domain: 3,
            nonce: "0xburn".to_string(),
            burn_token: Address::repeat_byte(0x11),
            destination_token: Address::repeat_byte(0x22),
            depositor: Address::repeat_byte(0x33),
            mint_recipient: Address::repeat_byte(0x44),
            amount: U256::from(1_000_000_u64),
            burn_tx_hash: "0xburn".to_string(),
            block_number: 42,
            indexed_at,
        }
    }
}
