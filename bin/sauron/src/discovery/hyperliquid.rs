use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use alloy::primitives::Address;
use evm_token_indexer_client::TokenIndexerClient;
use hl_shim_client::{HlShimClient, HlShimStreamEvent, StreamKind, SubscribeFilter};
use router_core::models::ProviderOperationType;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tracing::{debug, warn};

use crate::{
    discovery::{hyperliquid_bridge, hyperliquid_spot_trade, hyperliquid_trade},
    error::Result,
    provider_operations::{ProviderOperationWatchStore, SharedProviderOperationWatchEntry},
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
    concurrency_limit: usize,
) -> Result<()> {
    let router_client = Arc::new(router_client);
    let hl_client = Arc::new(hl_client);
    let submitted = Arc::new(Mutex::new(HashSet::new()));
    let mut subscriptions = HashMap::<
        Address,
        mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
    >::new();
    loop {
        let operations = store.snapshot().await;
        ensure_user_subscriptions_concurrent(
            Arc::clone(&hl_client),
            &mut subscriptions,
            &operations,
            concurrency_limit,
        )
        .await;
        drain_subscriptions(&mut subscriptions);
        run_hyperliquid_observer_cycle(
            operations,
            Arc::clone(&router_client),
            Arc::clone(&hl_client),
            arbitrum_token_indexer.clone(),
            bridge_match_window_seconds,
            concurrency_limit,
            Arc::clone(&submitted),
        )
        .await;
        sleep(HL_OBSERVER_INTERVAL).await;
    }
}

async fn ensure_user_subscriptions_concurrent(
    client: Arc<HlShimClient>,
    subscriptions: &mut HashMap<
        Address,
        mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
    >,
    operations: &[SharedProviderOperationWatchEntry],
    concurrency_limit: usize,
) {
    let users = operations
        .iter()
        .filter_map(|operation| operation_user(operation))
        .filter(|user| !subscriptions.contains_key(user))
        .collect::<HashSet<_>>();
    let concurrency_limit = concurrency_limit.max(1);
    let mut users = users.into_iter();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < concurrency_limit {
            let Some(user) = users.next() else {
                break;
            };
            let client = Arc::clone(&client);
            tasks.spawn(async move { subscribe_user(&client, user).await });
        }

        if tasks.is_empty() {
            break;
        }
        match tasks.join_next().await {
            Some(Ok(Some((user, receiver)))) => {
                subscriptions.entry(user).or_insert(receiver);
            }
            Some(Ok(None)) => {}
            Some(Err(error)) => {
                warn!(%error, "HL observer subscription task failed");
            }
            None => break,
        }
    }
}

async fn subscribe_user(
    client: &HlShimClient,
    user: Address,
) -> Option<(
    Address,
    mpsc::Receiver<std::result::Result<HlShimStreamEvent, hl_shim_client::Error>>,
)> {
    let filter = SubscribeFilter {
        users: vec![user],
        kinds: vec![StreamKind::Transfers, StreamKind::Orders],
    };
    match timeout(HL_SUBSCRIBE_TIMEOUT, client.subscribe(filter)).await {
        Ok(Ok(receiver)) => {
            debug!(%user, "subscribed Sauron HL observer to shim user stream");
            Some((user, receiver))
        }
        Ok(Err(error)) => {
            warn!(%user, %error, "failed to subscribe Sauron HL observer to shim user stream");
            None
        }
        Err(_) => {
            warn!(%user, "timed out subscribing Sauron HL observer to shim user stream");
            None
        }
    }
}

async fn run_hyperliquid_observer_cycle(
    operations: Vec<SharedProviderOperationWatchEntry>,
    router_client: Arc<RouterClient>,
    hl_client: Arc<HlShimClient>,
    arbitrum_token_indexer: Option<Arc<TokenIndexerClient>>,
    bridge_match_window_seconds: i64,
    concurrency_limit: usize,
    submitted: Arc<Mutex<HashSet<String>>>,
) {
    let due = operations
        .into_iter()
        .filter(|operation| is_hyperliquid_operation(operation.operation_type))
        .collect::<Vec<_>>();
    let concurrency_limit = concurrency_limit.max(1);
    let mut operations = due.into_iter();
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < concurrency_limit {
            let Some(operation) = operations.next() else {
                break;
            };
            let router_client = Arc::clone(&router_client);
            let hl_client = Arc::clone(&hl_client);
            let arbitrum_token_indexer = arbitrum_token_indexer.clone();
            let submitted = Arc::clone(&submitted);
            tasks.spawn(async move {
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
                        return;
                    }
                };
                let Some(hint) = hint else {
                    return;
                };
                submit_hyperliquid_hint(&router_client, &submitted, &operation, hint).await;
            });
        }

        if tasks.is_empty() {
            break;
        }
        match tasks.join_next().await {
            Some(Ok(())) => {}
            Some(Err(error)) => {
                warn!(%error, "HL observer task failed");
            }
            None => break,
        }
    }
}

async fn submit_hyperliquid_hint(
    router_client: &RouterClient,
    submitted: &Mutex<HashSet<String>>,
    operation: &SharedProviderOperationWatchEntry,
    hint: router_server::api::ProviderOperationHintRequest,
) {
    let key = hint.idempotency_key.clone().unwrap_or_else(|| {
        format!(
            "{}:{}",
            hint.provider_operation_id,
            hint.hint_kind.to_db_string()
        )
    });
    {
        let mut submitted = submitted.lock().await;
        if !submitted.insert(key.clone()) {
            return;
        }
    }
    if let Err(error) = router_client.submit_provider_operation_hint(&hint).await {
        submitted.lock().await.remove(&key);
        warn!(
            operation_id = %operation.operation_id,
            hint_kind = hint.hint_kind.to_db_string(),
            %error,
            "HL observer failed to submit provider-operation hint"
        );
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
        ProviderOperationType::UnitDeposit => operation
            .request
            .get("dst_addr")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok()),
        ProviderOperationType::UnitWithdrawal => operation
            .request
            .get("protocol_address")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| value.parse().ok()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provider_operations::ProviderOperationWatchEntry;
    use axum::{
        extract::State,
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router,
    };
    use chrono::Utc;
    use router_core::models::ProviderOperationStatus;
    use router_temporal::WorkflowStepId;
    use serde_json::{json, Value};
    use std::time::Instant;
    use std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };
    use tokio::{net::TcpListener, sync::Mutex, task::JoinHandle};
    use uuid::Uuid;

    fn watch(operation_type: ProviderOperationType, request: Value) -> ProviderOperationWatchEntry {
        watch_with_observed_state(operation_type, request, json!({}))
    }

    fn watch_with_observed_state(
        operation_type: ProviderOperationType,
        request: Value,
        observed_state: Value,
    ) -> ProviderOperationWatchEntry {
        ProviderOperationWatchEntry {
            operation_id: Uuid::now_v7(),
            execution_step_id: WorkflowStepId::from(Uuid::now_v7()),
            provider: "hyperliquid".to_string(),
            operation_type,
            provider_ref: None,
            status: ProviderOperationStatus::WaitingExternal,
            request,
            response: json!({}),
            observed_state,
            execution_step_request: json!({}),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn hyperliquid_observer_cycle_polls_due_operations_concurrently() {
        let server = spawn_hl_orders_server(Duration::from_millis(50)).await;
        let user = "0x1111111111111111111111111111111111111111";
        let operation_count = 100;
        let operations = (0..operation_count)
            .map(|oid| {
                Arc::new(watch_with_observed_state(
                    ProviderOperationType::HyperliquidTrade,
                    json!({
                        "user": user,
                        "coin": "BTC"
                    }),
                    json!({ "oid": oid }),
                ))
            })
            .collect::<Vec<_>>();
        let started = Instant::now();
        run_hyperliquid_observer_cycle(
            operations,
            Arc::new(RouterClient::new_for_test("http://127.0.0.1:1")),
            Arc::new(HlShimClient::new(server.base_url()).expect("HL client")),
            None,
            1_800,
            25,
            Arc::new(Mutex::new(HashSet::new())),
        )
        .await;

        assert_eq!(server.watch_request_count(), operation_count);
        assert_eq!(server.orders_request_count(), operation_count);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "bounded-concurrent HL cycle should complete well below serial 10s path, took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn operation_user_reads_unit_deposit_dst_addr() {
        let user = "0xd1e351af26521ca53aa6efbdf6f690b16ba12e5d";
        let operation = watch(
            ProviderOperationType::UnitDeposit,
            json!({
                "asset": "eth",
                "amount": "1",
                "dst_addr": user,
                "dst_chain": "hyperliquid",
                "src_chain": "ethereum",
                "protocol_address": "0xc0d3b8fddafa7e82d221d512df8b7cf04bcbb4eb"
            }),
        );

        assert_eq!(operation_user(&operation), Some(user.parse().unwrap()));
    }

    #[test]
    fn operation_user_reads_unit_withdrawal_protocol_address() {
        let user = "0xbc6faa3475c0234dff0a904b28dcef417be60281";
        let operation = watch(
            ProviderOperationType::UnitWithdrawal,
            json!({
                "asset": "eth",
                "amount": "1",
                "dst_addr": "0x2222222222222222222222222222222222222222",
                "dst_chain": "base",
                "src_chain": "hyperliquid",
                "protocol_address": user
            }),
        );

        assert_eq!(operation_user(&operation), Some(user.parse().unwrap()));
    }

    #[test]
    fn operation_user_reads_hyperliquid_trade_user() {
        let user = "0x1111111111111111111111111111111111111111";
        let operation = watch(
            ProviderOperationType::HyperliquidTrade,
            json!({
                "user": user,
                "coin": "BTC"
            }),
        );

        assert_eq!(operation_user(&operation), Some(user.parse().unwrap()));
    }

    #[derive(Clone)]
    struct HlOrdersMockState {
        delay: Duration,
        watch_requests: Arc<AtomicUsize>,
        orders_requests: Arc<AtomicUsize>,
    }

    struct HlOrdersMockServer {
        base_url: String,
        handle: JoinHandle<()>,
        watch_requests: Arc<AtomicUsize>,
        orders_requests: Arc<AtomicUsize>,
    }

    impl HlOrdersMockServer {
        fn base_url(&self) -> &str {
            &self.base_url
        }

        fn watch_request_count(&self) -> usize {
            self.watch_requests.load(Ordering::SeqCst)
        }

        fn orders_request_count(&self) -> usize {
            self.orders_requests.load(Ordering::SeqCst)
        }
    }

    impl Drop for HlOrdersMockServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_hl_orders_server(delay: Duration) -> HlOrdersMockServer {
        let watch_requests = Arc::new(AtomicUsize::new(0));
        let orders_requests = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind HL orders mock server");
        let addr = listener.local_addr().expect("HL orders mock server addr");
        let app = Router::new()
            .route("/orders/watch", post(hl_orders_watch_handler))
            .route("/orders", get(hl_orders_handler))
            .with_state(HlOrdersMockState {
                delay,
                watch_requests: Arc::clone(&watch_requests),
                orders_requests: Arc::clone(&orders_requests),
            });
        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("HL orders mock server should run");
        });
        HlOrdersMockServer {
            base_url: format!("http://{addr}"),
            handle,
            watch_requests,
            orders_requests,
        }
    }

    async fn hl_orders_watch_handler(
        State(state): State<HlOrdersMockState>,
        Json(request): Json<hl_shim_client::OrderWatchRequest>,
    ) -> Response {
        state.watch_requests.fetch_add(1, Ordering::SeqCst);
        if !state.delay.is_zero() {
            sleep(state.delay).await;
        }
        Json(hl_shim_client::OrderWatchResponse {
            user: request.user,
            oid: request.oid,
            watched: true,
        })
        .into_response()
    }

    async fn hl_orders_handler(State(state): State<HlOrdersMockState>) -> Response {
        state.orders_requests.fetch_add(1, Ordering::SeqCst);
        if !state.delay.is_zero() {
            sleep(state.delay).await;
        }
        Json(serde_json::json!({
            "orders": [],
            "next_cursor": "",
            "has_more": false
        }))
        .into_response()
    }
}
