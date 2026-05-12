use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use alloy::{
    primitives::{Address, TxHash, U256},
    sol,
};
use evm_receipt_watcher_client::{parse_tx_hash, ByIdLookup, EvmReceiptWatcherClient};
use router_core::{
    models::{
        ProviderOperationHintKind, ProviderOperationType, SAURON_EVM_RECEIPT_OBSERVER_HINT_SOURCE,
    },
    protocol::{AssetId, ChainId, DepositAsset},
};
use router_server::api::{ProviderOperationHintRequest, MAX_HINT_IDEMPOTENCY_KEY_LEN};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::{timeout, MissedTickBehavior};
use tracing::{debug, warn};

use crate::{
    config::SauronArgs,
    error::Result,
    provider_operations::{ProviderOperationWatchStore, SharedProviderOperationWatchEntry},
    router_client::RouterClient,
};

const EVM_RECEIPT_OBSERVER_INTERVAL: Duration = Duration::from_secs(5);
const EVM_RECEIPT_LOOKUP_TIMEOUT: Duration = Duration::from_secs(20);

sol! {
    #[derive(Debug)]
    event Transfer(address indexed from, address indexed to, uint256 value);

    #[derive(Debug)]
    event MessageReceived(address indexed token, address indexed recipient, uint256 amount);

    #[derive(Debug)]
    event Swap(
        address indexed sender,
        address indexed recipient,
        address srcToken,
        address destToken,
        uint256 srcAmount,
        uint256 destAmount
    );
}

#[derive(Clone)]
pub struct EvmReceiptObserverClients {
    ethereum: Option<Arc<EvmReceiptWatcherClient>>,
    base: Option<Arc<EvmReceiptWatcherClient>>,
    arbitrum: Option<Arc<EvmReceiptWatcherClient>>,
}

impl EvmReceiptObserverClients {
    pub fn from_args(args: &SauronArgs) -> Result<Option<Self>> {
        let ethereum = receipt_client("ethereum", args.ethereum_receipt_watcher_url.as_deref())?;
        let base = receipt_client("base", args.base_receipt_watcher_url.as_deref())?;
        let arbitrum = receipt_client("arbitrum", args.arbitrum_receipt_watcher_url.as_deref())?;
        if ethereum.is_none() && base.is_none() && arbitrum.is_none() {
            return Ok(None);
        }
        Ok(Some(Self {
            ethereum,
            base,
            arbitrum,
        }))
    }

    fn client_for_chain(&self, chain_id: &str) -> Option<Arc<EvmReceiptWatcherClient>> {
        match evm_chain_number(chain_id) {
            Some(1) => self.ethereum.clone(),
            Some(8453) => self.base.clone(),
            Some(42161) => self.arbitrum.clone(),
            _ => None,
        }
    }
}

fn receipt_client(
    chain: &'static str,
    url: Option<&str>,
) -> Result<Option<Arc<EvmReceiptWatcherClient>>> {
    let Some(url) = url else {
        return Ok(None);
    };
    Ok(Some(Arc::new(
        EvmReceiptWatcherClient::new(url, chain).map_err(|source| {
            crate::error::Error::InvalidConfiguration {
                message: format!("invalid {chain} receipt watcher URL: {source}"),
            }
        })?,
    )))
}

pub async fn run_evm_receipt_observer_loop(
    clients: EvmReceiptObserverClients,
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
) -> Result<()> {
    let mut submitted: HashSet<(uuid::Uuid, String)> = HashSet::new();
    let mut ticker = tokio::time::interval(EVM_RECEIPT_OBSERVER_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.tick().await;

    loop {
        let operations = store.snapshot().await;
        let active = operations
            .iter()
            .map(|operation| operation.operation_id)
            .collect::<HashSet<_>>();
        submitted.retain(|(operation_id, _)| active.contains(operation_id));

        for operation in operations {
            let request = match evm_receipt_hint_request(&clients, &operation).await {
                Ok(request) => request,
                Err(error) => {
                    warn!(
                        operation_id = %operation.operation_id,
                        operation_type = %operation.operation_type.to_db_string(),
                        %error,
                        "failed to inspect EVM receipt for provider-operation hint"
                    );
                    continue;
                }
            };
            if let Some(request) = request {
                let key_text = request
                    .idempotency_key
                    .clone()
                    .unwrap_or_else(|| operation.operation_id.to_string());
                let key = (operation.operation_id, key_text);
                if submitted.contains(&key) {
                    continue;
                }
                match router_client.submit_provider_operation_hint(&request).await {
                    Ok(_) => {
                        submitted.insert(key);
                    }
                    Err(error) => {
                        warn!(
                            operation_id = %operation.operation_id,
                            operation_type = %operation.operation_type.to_db_string(),
                            %error,
                            "failed to submit EVM receipt provider-operation hint"
                        );
                    }
                }
            }
        }

        ticker.tick().await;
    }
}

async fn evm_receipt_hint_request(
    clients: &EvmReceiptObserverClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Option<ProviderOperationHintRequest>> {
    match operation.operation_type {
        ProviderOperationType::UniversalRouterSwap => {
            velora_swap_settled_hint(clients, operation).await
        }
        ProviderOperationType::CctpReceive => cctp_receive_observed_hint(clients, operation).await,
        _ => Ok(None),
    }
}

async fn lookup_receipt(
    client: Arc<EvmReceiptWatcherClient>,
    tx_hash: TxHash,
) -> Result<Option<evm_receipt_watcher_client::EvmReceipt>> {
    match timeout(EVM_RECEIPT_LOOKUP_TIMEOUT, client.lookup_by_id(tx_hash)).await {
        Ok(result) => result.map_err(|source| crate::error::Error::EvmReceiptWatcher { source }),
        Err(_) => Ok(None),
    }
}

async fn velora_swap_settled_hint(
    clients: &EvmReceiptObserverClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Option<ProviderOperationHintRequest>> {
    let Some(tx_hash) = operation
        .provider_ref
        .as_deref()
        .and_then(|value| parse_tx_hash(value).ok())
    else {
        return Ok(None);
    };
    let Some(chain_id) = operation
        .request
        .pointer("/output_asset/chain_id")
        .and_then(Value::as_str)
        .or_else(|| {
            operation
                .request
                .pointer("/input_asset/chain_id")
                .and_then(Value::as_str)
        })
    else {
        return Ok(None);
    };
    let Some(client) = clients.client_for_chain(chain_id) else {
        debug!(
            operation_id = %operation.operation_id,
            chain_id,
            "no receipt watcher client configured for Velora operation chain"
        );
        return Ok(None);
    };
    let Some((receipt, logs)) = lookup_receipt(client, tx_hash).await? else {
        return Ok(None);
    };
    if !receipt.status() {
        return Ok(None);
    }
    let recipient = operation
        .request
        .get("recipient_address")
        .and_then(Value::as_str)
        .and_then(|value| Address::from_str(value).ok());
    let expected_token = operation
        .request
        .pointer("/output_asset/asset")
        .and_then(Value::as_str)
        .and_then(|asset| normalized_reference_asset(chain_id, asset));
    let expected_min = operation
        .request
        .get("min_amount_out")
        .and_then(Value::as_str)
        .or_else(|| operation.request.get("amount_out").and_then(Value::as_str))
        .and_then(|amount| U256::from_str_radix(amount, 10).ok());

    for log in &logs {
        if log.removed {
            continue;
        }
        let Ok(decoded) = log.log_decode::<Swap>() else {
            continue;
        };
        if let Some(recipient) = recipient {
            if decoded.inner.data.recipient != recipient {
                continue;
            }
        }
        if let Some(expected_token) = expected_token {
            if decoded.inner.data.destToken != expected_token {
                continue;
            }
        }
        if let Some(expected_min) = expected_min {
            if decoded.inner.data.destAmount < expected_min {
                continue;
            }
        }
        let Some(log_index) = decoded.log_index else {
            continue;
        };
        let mut evidence = json!({
            "tx_hash": format!("{tx_hash:?}"),
            "log_index": log_index,
            "amount_out": decoded.inner.data.destAmount.to_string(),
            "recipient": format!("{:#x}", decoded.inner.data.recipient),
            "executor": format!("{:#x}", decoded.inner.data.sender),
            "block_number": decoded.block_number,
        });
        if decoded.inner.data.destToken != Address::ZERO {
            evidence["token_address"] = json!(format!("{:#x}", decoded.inner.data.destToken));
        }
        return Ok(Some(hint_request(
            operation,
            ProviderOperationHintKind::VeloraSwapSettled,
            evidence,
            log_index,
        )));
    }

    for log in logs {
        if log.removed {
            continue;
        }
        let Ok(decoded) = log.log_decode::<Transfer>() else {
            continue;
        };
        if let Some(expected_token) = expected_token {
            if decoded.address() != expected_token {
                continue;
            }
        }
        if let Some(recipient) = recipient {
            if decoded.inner.data.to != recipient {
                continue;
            }
        }
        if let Some(expected_min) = expected_min {
            if decoded.inner.data.value < expected_min {
                continue;
            }
        }
        let Some(log_index) = decoded.log_index else {
            continue;
        };
        return Ok(Some(hint_request(
            operation,
            ProviderOperationHintKind::VeloraSwapSettled,
            json!({
                "tx_hash": format!("{tx_hash:?}"),
                "log_index": log_index,
                "amount_out": decoded.inner.data.value.to_string(),
                "recipient": format!("{:#x}", decoded.inner.data.to),
                "executor": format!("{:#x}", decoded.inner.data.from),
                "token_address": format!("{:#x}", decoded.address()),
                "block_number": decoded.block_number,
            }),
            log_index,
        )));
    }
    Ok(None)
}

async fn cctp_receive_observed_hint(
    clients: &EvmReceiptObserverClients,
    operation: &SharedProviderOperationWatchEntry,
) -> Result<Option<ProviderOperationHintRequest>> {
    let Some(tx_hash) = operation
        .provider_ref
        .as_deref()
        .and_then(|value| parse_tx_hash(value).ok())
    else {
        return Ok(None);
    };
    let Some(chain_id) = operation
        .request
        .get("destination_chain_id")
        .and_then(Value::as_str)
    else {
        return Ok(None);
    };
    let Some(client) = clients.client_for_chain(chain_id) else {
        debug!(
            operation_id = %operation.operation_id,
            chain_id,
            "no receipt watcher client configured for CCTP receive chain"
        );
        return Ok(None);
    };
    let Some((receipt, logs)) = lookup_receipt(client, tx_hash).await? else {
        return Ok(None);
    };
    if !receipt.status() {
        return Ok(None);
    }
    let expected_transmitter = operation
        .request
        .get("message_transmitter_v2")
        .and_then(Value::as_str)
        .and_then(|value| Address::from_str(value).ok());
    let expected_recipient = operation
        .request
        .get("recipient_address")
        .and_then(Value::as_str)
        .and_then(|value| Address::from_str(value).ok());
    let expected_token = operation
        .request
        .get("output_asset")
        .and_then(Value::as_str)
        .and_then(|asset| normalized_reference_asset(chain_id, asset));
    let expected_amount = operation
        .request
        .get("amount")
        .and_then(Value::as_str)
        .and_then(|amount| U256::from_str_radix(amount, 10).ok());

    for log in logs {
        if log.removed {
            continue;
        }
        let Ok(decoded) = log.log_decode::<MessageReceived>() else {
            continue;
        };
        if let Some(expected_transmitter) = expected_transmitter {
            if decoded.address() != expected_transmitter {
                continue;
            }
        }
        if let Some(expected_recipient) = expected_recipient {
            if decoded.inner.data.recipient != expected_recipient {
                continue;
            }
        }
        if let Some(expected_token) = expected_token {
            if decoded.inner.data.token != expected_token {
                continue;
            }
        }
        if let Some(expected_amount) = expected_amount {
            if decoded.inner.data.amount != expected_amount {
                continue;
            }
        }
        let Some(log_index) = decoded.log_index else {
            continue;
        };
        return Ok(Some(hint_request(
            operation,
            ProviderOperationHintKind::CctpReceiveObserved,
            json!({
                "tx_hash": format!("{tx_hash:?}"),
                "log_index": log_index,
                "token": format!("{:#x}", decoded.inner.data.token),
                "recipient": format!("{:#x}", decoded.inner.data.recipient),
                "amount": decoded.inner.data.amount.to_string(),
                "block_number": decoded.block_number,
            }),
            log_index,
        )));
    }
    Ok(None)
}

fn hint_request(
    operation: &SharedProviderOperationWatchEntry,
    hint_kind: ProviderOperationHintKind,
    evidence: Value,
    log_index: u64,
) -> ProviderOperationHintRequest {
    ProviderOperationHintRequest {
        provider_operation_id: operation.operation_id,
        execution_step_id: operation.execution_step_id,
        source: SAURON_EVM_RECEIPT_OBSERVER_HINT_SOURCE.to_string(),
        hint_kind,
        evidence,
        idempotency_key: Some(evm_receipt_idempotency_key(
            operation.operation_id,
            hint_kind,
            operation.provider_ref.as_deref().unwrap_or_default(),
            log_index,
        )),
    }
}

fn evm_receipt_idempotency_key(
    operation_id: uuid::Uuid,
    hint_kind: ProviderOperationHintKind,
    tx_hash: &str,
    log_index: u64,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(operation_id.as_bytes());
    hasher.update(hint_kind.to_db_string().as_bytes());
    hasher.update(tx_hash.as_bytes());
    hasher.update(log_index.to_be_bytes());
    let digest = hasher.finalize();
    let key = format!(
        "evm-receipt:{}:{}:{}",
        operation_id,
        hint_kind.to_db_string(),
        &alloy::hex::encode(digest)[..16]
    );
    if key.len() <= MAX_HINT_IDEMPOTENCY_KEY_LEN {
        key
    } else {
        key[..MAX_HINT_IDEMPOTENCY_KEY_LEN].to_string()
    }
}

fn normalized_reference_asset(chain_id: &str, asset: &str) -> Option<Address> {
    let deposit_asset = DepositAsset {
        chain: ChainId::parse(chain_id).ok()?,
        asset: AssetId::parse(asset).ok()?,
    }
    .normalized_asset_identity()
    .ok()?;
    match deposit_asset.asset {
        AssetId::Native => None,
        AssetId::Reference(address) => Address::from_str(&address).ok(),
    }
}

fn evm_chain_number(chain_id: &str) -> Option<u64> {
    ChainId::parse(chain_id).ok()?.evm_chain_id()
}
