use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::primitives::Address;
use evm_token_indexer_client::TokenIndexerClient;
use hl_shim_client::{HlShimClient, HlShimStreamEvent, StreamKind, SubscribeFilter};
use router_core::models::ProviderOperationType;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout};
use tracing::{debug, warn};

use crate::{
    discovery::{hyperliquid_bridge, hyperliquid_spot_trade, hyperliquid_trade},
    error::Result,
    provider_operations::ProviderOperationWatchStore,
    router_client::RouterClient,
};

const HL_OBSERVER_INTERVAL: Duration = Duration::from_secs(2);
const HL_SUBSCRIBE_TIMEOUT: Duration = Duration::from_secs(2);

pub async fn run_hyperliquid_observer_loop(
    store: ProviderOperationWatchStore,
    router_client: RouterClient,
    hl_client: HlShimClient,
    arbitrum_token_indexer: Option<Arc<TokenIndexerClient>>,
    bridge_match_window_seconds: i64,
) -> Result<()> {
    let mut submitted = HashSet::new();
    let mut subscriptions = HashMap::<
        Address,
        mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
    >::new();
    loop {
        let operations = store.snapshot().await;
        for operation in &operations {
            if let Some(user) = operation_user(operation) {
                ensure_user_subscription(&hl_client, &mut subscriptions, user).await;
            }
        }
        drain_subscriptions(&mut subscriptions);

        for operation in operations {
            if !is_hyperliquid_operation(operation.operation_type) {
                continue;
            }
            debug!(
                operation_id = %operation.operation_id,
                operation_type = operation.operation_type.to_db_string(),
                "Sauron HL observer evaluating provider operation"
            );
            let hint_result = match operation.operation_type {
                ProviderOperationType::HyperliquidTrade
                | ProviderOperationType::HyperliquidLimitOrder => {
                    if hyperliquid_spot_trade::is_spot_trade_candidate(&operation) {
                        debug!(
                            operation_id = %operation.operation_id,
                            "observing HL spot trade operation"
                        );
                    }
                    hyperliquid_trade::trade_hint(&hl_client, &operation).await
                }
                ProviderOperationType::HyperliquidBridgeDeposit
                | ProviderOperationType::HyperliquidBridgeWithdrawal => {
                    hyperliquid_bridge::bridge_hint(
                        &hl_client,
                        arbitrum_token_indexer.as_ref(),
                        &operation,
                        bridge_match_window_seconds,
                    )
                    .await
                }
                _ => Ok(None),
            };
            let hint = match hint_result {
                Ok(hint) => hint,
                Err(error) => {
                    warn!(
                        operation_id = %operation.operation_id,
                        operation_type = operation.operation_type.to_db_string(),
                        %error,
                        "HL observer failed to build provider-operation hint"
                    );
                    continue;
                }
            };
            let Some(hint) = hint else {
                continue;
            };
            let key = hint.idempotency_key.clone().unwrap_or_else(|| {
                format!(
                    "{}:{}",
                    hint.provider_operation_id,
                    hint.hint_kind.to_db_string()
                )
            });
            if !submitted.insert(key.clone()) {
                continue;
            }
            if let Err(error) = router_client.submit_provider_operation_hint(&hint).await {
                submitted.remove(&key);
                warn!(
                    operation_id = %operation.operation_id,
                    hint_kind = hint.hint_kind.to_db_string(),
                    %error,
                    "HL observer failed to submit provider-operation hint"
                );
            }
        }
        sleep(HL_OBSERVER_INTERVAL).await;
    }
}

async fn ensure_user_subscription(
    client: &HlShimClient,
    subscriptions: &mut HashMap<
        Address,
        mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
    >,
    user: Address,
) {
    if subscriptions.contains_key(&user) {
        return;
    }
    let filter = SubscribeFilter {
        users: vec![user],
        kinds: vec![StreamKind::Transfers, StreamKind::Orders],
    };
    match timeout(HL_SUBSCRIBE_TIMEOUT, client.subscribe(filter)).await {
        Ok(Ok(receiver)) => {
            subscriptions.insert(user, receiver);
            debug!(%user, "subscribed Sauron HL observer to shim user stream");
        }
        Ok(Err(error)) => {
            warn!(%user, %error, "failed to subscribe Sauron HL observer to shim user stream");
        }
        Err(_) => {
            warn!(%user, "timed out subscribing Sauron HL observer to shim user stream");
        }
    }
}

fn drain_subscriptions(
    subscriptions: &mut HashMap<
        Address,
        mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
    >,
) {
    subscriptions.retain(|user, receiver| loop {
        match receiver.try_recv() {
            Ok(Ok(_event)) => {}
            Ok(Err(error)) => {
                warn!(%user, %error, "HL shim subscription delivered an error");
            }
            Err(mpsc::error::TryRecvError::Empty) => return true,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                warn!(%user, "HL shim subscription disconnected");
                return false;
            }
        }
    });
}

fn is_hyperliquid_operation(operation_type: ProviderOperationType) -> bool {
    matches!(
        operation_type,
        ProviderOperationType::HyperliquidTrade
            | ProviderOperationType::HyperliquidLimitOrder
            | ProviderOperationType::HyperliquidBridgeDeposit
            | ProviderOperationType::HyperliquidBridgeWithdrawal
    )
}

fn operation_user(
    operation: &crate::provider_operations::ProviderOperationWatchEntry,
) -> Option<Address> {
    match operation.operation_type {
        ProviderOperationType::HyperliquidTrade | ProviderOperationType::HyperliquidLimitOrder => {
            operation
                .request
                .get("user")
                .and_then(serde_json::Value::as_str)
                .and_then(|value| value.parse().ok())
        }
        ProviderOperationType::HyperliquidBridgeDeposit => operation
            .provider_ref
            .as_deref()
            .and_then(|value| value.parse().ok())
            .or_else(|| {
                operation
                    .request
                    .get("hyperliquid_user")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| value.parse().ok())
            }),
        ProviderOperationType::HyperliquidBridgeWithdrawal => operation
            .request
            .get("hyperliquid_custody_vault_address")
            .or_else(|| {
                operation
                    .execution_step_request
                    .get("hyperliquid_custody_vault_address")
            })
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok()),
        _ => None,
    }
}
