use super::*;
use crate::order_execution::types::{ProviderKind, WorkflowHintId};

use std::sync::atomic::{AtomicUsize, Ordering};

use chains::{ChainOperations, ChainRegistry, evm::EvmChain, hyperliquid::HyperliquidChain};
use router_core::{
    config::Settings,
    models::{
        CustodyVaultControlType, CustodyVaultStatus, MarketOrderAction, RouterOrderAction,
        RouterOrderStatus, RouterOrderType,
    },
    services::{
        ActionProviderHttpOptions,
        action_providers::{ExchangeProvider, ProviderAddressIntent, ProviderFuture, UnitProvider},
        asset_registry::{AssetSlot, RequiredCustodyRole},
        custody_action_executor::{
            ChainCall, HyperliquidCall, HyperliquidCallNetwork, HyperliquidCallPayload,
            HyperliquidRuntimeConfig,
        },
    },
};
use router_primitives::ChainType;
use sqlx_postgres::PgPool;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

const POSTGRES_PORT: u16 = 5432;
const POSTGRES_USER: &str = "postgres";
const POSTGRES_PASSWORD: &str = "password";
const POSTGRES_DATABASE: &str = "postgres";

struct TestDatabase {
    _container: ContainerAsync<GenericImage>,
    db: Database,
    pool: PgPool,
}

struct SeededRunningStep {
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
}

struct TestUnitProvider;

impl UnitProvider for TestUnitProvider {
    fn id(&self) -> &str {
        ProviderId::Unit.as_str()
    }

    fn supports_deposit(&self, _asset: &DepositAsset) -> bool {
        true
    }

    fn supports_withdrawal(&self, asset: &DepositAsset) -> bool {
        asset.chain.as_str() == "bitcoin" && asset.asset.is_native()
    }

    fn execute_deposit<'a>(
        &'a self,
        _request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_unit_deposit" }),
                state: ProviderExecutionState::default(),
            })
        })
    }

    fn execute_withdrawal<'a>(
        &'a self,
        _request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_unit_withdrawal" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

struct CountingUnitDepositProvider {
    gen_calls: Arc<AtomicUsize>,
    transfer_actions_built: Arc<AtomicUsize>,
}

impl CountingUnitDepositProvider {
    fn new() -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let gen_calls = Arc::new(AtomicUsize::new(0));
        let transfer_actions_built = Arc::new(AtomicUsize::new(0));
        (
            Self {
                gen_calls: gen_calls.clone(),
                transfer_actions_built: transfer_actions_built.clone(),
            },
            gen_calls,
            transfer_actions_built,
        )
    }
}

impl UnitProvider for CountingUnitDepositProvider {
    fn id(&self) -> &str {
        ProviderId::Unit.as_str()
    }

    fn supports_deposit(&self, _asset: &DepositAsset) -> bool {
        true
    }

    fn supports_withdrawal(&self, _asset: &DepositAsset) -> bool {
        true
    }

    fn execute_deposit<'a>(
        &'a self,
        request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        let gen_calls = self.gen_calls.clone();
        let transfer_actions_built = self.transfer_actions_built.clone();
        Box::pin(async move {
            let gen_call = gen_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let protocol_address = format!("0x{gen_call:040x}");
            let custody_vault_id = request
                .source_custody_vault_id
                .ok_or_else(|| "test deposit missing custody vault id".to_string())?;
            let src_chain_id = ChainId::parse(&request.src_chain_id)
                .map_err(|err| format!("invalid test deposit src_chain_id: {err}"))?;
            let source_asset_id = AssetId::parse(&request.asset_id)
                .map_err(|err| format!("invalid test deposit asset_id: {err}"))?;
            transfer_actions_built.fetch_add(1, Ordering::SeqCst);
            let operation_request = json!({
                "protocol_address": protocol_address,
                "amount": request.amount,
            });
            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id,
                actions: vec![CustodyAction::Transfer {
                    to_address: protocol_address.clone(),
                    amount: request.amount.clone(),
                    bitcoin_fee_budget_sats: None,
                }],
                provider_context: operation_request.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UnitDeposit,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(protocol_address.clone()),
                        idempotency_key: request.idempotency_key.clone(),
                        request: Some(operation_request),
                        response: Some(json!({
                            "kind": "test_hyperunit_gen_response",
                            "call": gen_call,
                        })),
                        observed_state: None,
                    }),
                    addresses: vec![ProviderAddressIntent {
                        role: ProviderAddressRole::UnitDeposit,
                        chain: src_chain_id,
                        asset: Some(source_asset_id),
                        address: protocol_address,
                        memo: None,
                        expires_at: None,
                        metadata: None,
                    }],
                },
            })
        })
    }

    fn execute_withdrawal<'a>(
        &'a self,
        _request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "counting_unit_withdrawal" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

struct CountingUnitWithdrawalProvider {
    hyperliquid_base_url: String,
    gen_calls: Arc<AtomicUsize>,
}

impl CountingUnitWithdrawalProvider {
    fn new(hyperliquid_base_url: String) -> (Self, Arc<AtomicUsize>) {
        let gen_calls = Arc::new(AtomicUsize::new(0));
        (
            Self {
                hyperliquid_base_url,
                gen_calls: gen_calls.clone(),
            },
            gen_calls,
        )
    }
}

impl UnitProvider for CountingUnitWithdrawalProvider {
    fn id(&self) -> &str {
        ProviderId::Unit.as_str()
    }

    fn supports_deposit(&self, _asset: &DepositAsset) -> bool {
        true
    }

    fn supports_withdrawal(&self, _asset: &DepositAsset) -> bool {
        true
    }

    fn execute_deposit<'a>(
        &'a self,
        _request: &'a UnitDepositStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "counting_unit_deposit" }),
                state: ProviderExecutionState::default(),
            })
        })
    }

    fn execute_withdrawal<'a>(
        &'a self,
        request: &'a UnitWithdrawalStepRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        let hyperliquid_base_url = self.hyperliquid_base_url.clone();
        let gen_calls = self.gen_calls.clone();
        Box::pin(async move {
            let gen_call = gen_calls.fetch_add(1, Ordering::SeqCst) + 1;
            let protocol_address = format!("0x{gen_call:040x}");
            let custody_vault_id = request
                .hyperliquid_custody_vault_id
                .ok_or_else(|| "test withdrawal missing custody vault id".to_string())?;
            let operation_request = json!({
                "protocol_address": protocol_address,
                "amount": request.amount,
                "requested_amount": request.amount,
            });
            Ok(ProviderExecutionIntent::CustodyActions {
                custody_vault_id,
                actions: vec![CustodyAction::Call(ChainCall::Hyperliquid(
                    HyperliquidCall {
                        target_base_url: hyperliquid_base_url,
                        network: HyperliquidCallNetwork::Testnet,
                        vault_address: None,
                        payload: HyperliquidCallPayload::SendAsset {
                            destination: protocol_address.clone(),
                            source_dex: "spot".to_string(),
                            destination_dex: "spot".to_string(),
                            token: "UETH:0x0000000000000000000000000000000000000000".to_string(),
                            amount: "0.0391".to_string(),
                        },
                    },
                ))],
                provider_context: operation_request.clone(),
                state: ProviderExecutionState {
                    operation: Some(ProviderOperationIntent {
                        operation_type: ProviderOperationType::UnitWithdrawal,
                        status: ProviderOperationStatus::Submitted,
                        provider_ref: Some(protocol_address),
                        idempotency_key: None,
                        request: Some(operation_request),
                        response: Some(json!({
                            "kind": "test_hyperunit_gen_response",
                            "call": gen_call,
                        })),
                        observed_state: None,
                    }),
                    addresses: vec![],
                },
            })
        })
    }
}

struct TestHyperliquidExchangeProvider;

struct TestVeloraExchangeProvider;

struct TestHyperliquidBridgeProvider;

impl router_core::services::action_providers::BridgeProvider for TestHyperliquidBridgeProvider {
    fn id(&self) -> &str {
        ProviderId::HyperliquidBridge.as_str()
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        Box::pin(async move {
            let amount_in = match &request.order_kind {
                ProviderOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                ProviderOrderKind::ExactOut { amount_out, .. } => amount_out.clone(),
            };
            Ok(Some(BridgeQuote {
                provider_id: ProviderId::HyperliquidBridge.as_str().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_in,
                provider_quote: json!({ "kind": "test_hyperliquid_bridge" }),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        _request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_hyperliquid_bridge" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

struct TestBridgeProvider {
    provider_id: ProviderId,
}

impl router_core::services::action_providers::BridgeProvider for TestBridgeProvider {
    fn id(&self) -> &str {
        self.provider_id.as_str()
    }

    fn quote_bridge<'a>(
        &'a self,
        request: BridgeQuoteRequest,
    ) -> ProviderFuture<'a, Option<BridgeQuote>> {
        let provider_id = self.provider_id;
        Box::pin(async move {
            let amount_in = match &request.order_kind {
                ProviderOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                ProviderOrderKind::ExactOut { amount_out, .. } => amount_out.clone(),
            };
            Ok(Some(BridgeQuote {
                provider_id: provider_id.as_str().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_in,
                provider_quote: json!({ "kind": "test_bridge", "provider": provider_id.as_str() }),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
            }))
        })
    }

    fn execute_bridge<'a>(
        &'a self,
        _request: &'a BridgeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_bridge" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

impl ExchangeProvider for TestHyperliquidExchangeProvider {
    fn id(&self) -> &str {
        ProviderId::Hyperliquid.as_str()
    }

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>> {
        Box::pin(async move {
            let amount_in = match &request.order_kind {
                ProviderOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                ProviderOrderKind::ExactOut { amount_out, .. } => amount_out.clone(),
            };
            let amount_out = if request.output_asset.chain.as_str() == "hyperliquid"
                && request.output_asset.asset.as_str() == "UBTC"
            {
                "50000".to_string()
            } else if request.output_asset.chain.as_str() == "hyperliquid"
                && request.output_asset.asset.is_native()
            {
                "5000000000".to_string()
            } else {
                amount_in.clone()
            };

            Ok(Some(ExchangeQuote {
                provider_id: self.id().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_out.clone(),
                min_amount_out: Some("1".to_string()),
                max_amount_in: None,
                provider_quote: json!({
                    "schema_version": 1,
                    "kind": "spot_cross_token",
                    "legs": [{
                        "input_asset": QuoteLegAsset::from_deposit_asset(&request.input_asset),
                        "output_asset": QuoteLegAsset::from_deposit_asset(&request.output_asset),
                        "amount_in": amount_in,
                        "amount_out": amount_out,
                        "order_kind": match &request.order_kind {
                            ProviderOrderKind::ExactIn { .. } => "exact_in",
                            ProviderOrderKind::ExactOut { .. } => "exact_out",
                        },
                        "min_amount_out": "1",
                    }],
                }),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
            }))
        })
    }

    fn execute_trade<'a>(
        &'a self,
        _request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_hyperliquid_trade" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

impl ExchangeProvider for TestVeloraExchangeProvider {
    fn id(&self) -> &str {
        ProviderId::Velora.as_str()
    }

    fn quote_trade<'a>(
        &'a self,
        request: ExchangeQuoteRequest,
    ) -> ProviderFuture<'a, Option<ExchangeQuote>> {
        Box::pin(async move {
            let amount_in = match &request.order_kind {
                ProviderOrderKind::ExactIn { amount_in, .. } => amount_in.clone(),
                ProviderOrderKind::ExactOut { amount_out, .. } => amount_out.clone(),
            };
            let amount_out = if request.output_asset.asset.is_native() {
                amount_in.clone()
            } else {
                "1000000".to_string()
            };

            Ok(Some(ExchangeQuote {
                provider_id: self.id().to_string(),
                amount_in: amount_in.clone(),
                amount_out: amount_out.clone(),
                min_amount_out: Some("1".to_string()),
                max_amount_in: None,
                provider_quote: json!({
                    "schema_version": 1,
                    "kind": "universal_router_swap",
                    "input_asset": QuoteLegAsset::from_deposit_asset(&request.input_asset),
                    "output_asset": QuoteLegAsset::from_deposit_asset(&request.output_asset),
                    "order_kind": match &request.order_kind {
                        ProviderOrderKind::ExactIn { .. } => "exact_in",
                        ProviderOrderKind::ExactOut { .. } => "exact_out",
                    },
                    "amount_in": amount_in,
                    "amount_out": amount_out,
                    "min_amount_out": "1",
                    "src_decimals": request.input_decimals.unwrap_or(18),
                    "dest_decimals": request.output_decimals.unwrap_or(6),
                    "price_route": {
                        "kind": "test_velora_route",
                    },
                }),
                expires_at: Utc::now() + chrono::Duration::minutes(10),
            }))
        })
    }

    fn execute_trade<'a>(
        &'a self,
        _request: &'a ExchangeExecutionRequest,
    ) -> ProviderFuture<'a, ProviderExecutionIntent> {
        Box::pin(async {
            Ok(ProviderExecutionIntent::ProviderOnly {
                response: json!({ "kind": "test_velora_swap" }),
                state: ProviderExecutionState::default(),
            })
        })
    }
}

struct TestHyperliquidInfoServer {
    base_url: String,
    handle: tokio::task::JoinHandle<()>,
}

struct TestHyperliquidExchangeServer {
    base_url: String,
    send_asset_calls: Arc<AtomicUsize>,
    handle: tokio::task::JoinHandle<()>,
}

struct TestEvmRpcServer {
    base_url: String,
    handle: tokio::task::JoinHandle<()>,
}

impl TestEvmRpcServer {
    async fn spawn_with_balance(raw_balance: &str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test EVM RPC server");
        let addr = listener.local_addr().expect("read test EVM RPC address");
        let balance = U256::from_str_radix(raw_balance, 10).expect("test EVM balance must parse");
        let balance_hex = format!("{balance:x}");
        let balance_quantity = Arc::new(format!("0x{balance_hex}"));
        let balance_word = Arc::new(format!("0x{balance_hex:0>64}"));
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(handle_test_evm_rpc_connection(
                    stream,
                    balance_quantity.clone(),
                    balance_word.clone(),
                ));
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            handle,
        }
    }
}

impl Drop for TestEvmRpcServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl TestHyperliquidExchangeServer {
    async fn spawn() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test Hyperliquid exchange server");
        let addr = listener
            .local_addr()
            .expect("read test Hyperliquid exchange server address");
        let send_asset_calls = Arc::new(AtomicUsize::new(0));
        let server_calls = send_asset_calls.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(handle_test_hyperliquid_exchange_connection(
                    stream,
                    server_calls.clone(),
                ));
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            send_asset_calls,
            handle,
        }
    }

    fn send_asset_call_count(&self) -> usize {
        self.send_asset_calls.load(Ordering::SeqCst)
    }
}

impl Drop for TestHyperliquidExchangeServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl TestHyperliquidInfoServer {
    async fn spawn() -> Self {
        Self::spawn_with_balances(vec![test_hyperliquid_spot_balance("UETH", 0, "1", "0")]).await
    }

    async fn spawn_with_balances(balances: Vec<Value>) -> Self {
        Self::spawn_with_balances_and_clearinghouse(balances, "0").await
    }

    async fn spawn_with_balances_and_clearinghouse(
        balances: Vec<Value>,
        clearinghouse_account_value: &str,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test Hyperliquid info server");
        let addr = listener
            .local_addr()
            .expect("read test Hyperliquid info server address");
        let balances = Arc::new(balances);
        let clearinghouse_account_value = Arc::new(clearinghouse_account_value.to_string());
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(handle_test_hyperliquid_info_connection(
                    stream,
                    balances.clone(),
                    clearinghouse_account_value.clone(),
                ));
            }
        });
        Self {
            base_url: format!("http://{addr}"),
            handle,
        }
    }
}

async fn handle_test_hyperliquid_exchange_connection(
    mut stream: tokio::net::TcpStream,
    send_asset_calls: Arc<AtomicUsize>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if test_http_request_complete(&buffer) {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer);
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    let payload = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
    if payload.pointer("/action/type").and_then(Value::as_str) == Some("sendAsset") {
        send_asset_calls.fetch_add(1, Ordering::SeqCst);
    }
    let response_body = json!({
        "status": "ok",
        "response": {
            "type": "default"
        }
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

impl Drop for TestHyperliquidInfoServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn handle_test_evm_rpc_connection(
    mut stream: tokio::net::TcpStream,
    balance_quantity: Arc<String>,
    balance_word: Arc<String>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if test_http_request_complete(&buffer) {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer);
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    let payload = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
    let id = payload.get("id").cloned().unwrap_or_else(|| json!(1));
    let result = match payload.get("method").and_then(Value::as_str) {
        Some("eth_getBalance") => json!(balance_quantity.as_str()),
        Some("eth_call") => json!(balance_word.as_str()),
        Some("eth_chainId") => json!("0xa4b1"),
        other => json!({ "error": format!("unexpected test EVM RPC request {other:?}") }),
    };
    let response_body = json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

async fn handle_test_hyperliquid_info_connection(
    mut stream: tokio::net::TcpStream,
    balances: Arc<Vec<Value>>,
    clearinghouse_account_value: Arc<String>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let Ok(read) = stream.read(&mut chunk).await else {
            return;
        };
        if read == 0 {
            return;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if test_http_request_complete(&buffer) {
            break;
        }
    }

    let request = String::from_utf8_lossy(&buffer);
    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
    let payload = serde_json::from_str::<Value>(body).unwrap_or_else(|_| json!({}));
    let response_body = match payload.get("type").and_then(Value::as_str) {
        Some("spotMeta") => test_hyperliquid_spot_meta(),
        Some("spotClearinghouseState") => json!({
            "balances": balances.as_ref().clone(),
        }),
        Some("clearinghouseState") => {
            test_hyperliquid_clearinghouse_state(clearinghouse_account_value.as_str())
        }
        other => json!({ "error": format!("unexpected test Hyperliquid info request {other:?}") }),
    }
    .to_string();
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = stream.write_all(response.as_bytes()).await;
}

fn test_hyperliquid_spot_balance(coin: &str, token: u64, total: &str, hold: &str) -> Value {
    json!({
        "coin": coin,
        "token": token,
        "hold": hold,
        "total": total,
    })
}

fn test_hyperliquid_clearinghouse_state(account_value: &str) -> Value {
    json!({
        "marginSummary": {
            "accountValue": account_value,
            "totalNtlPos": "0",
            "totalRawUsd": account_value,
            "totalMarginUsed": "0",
        },
        "crossMarginSummary": {
            "accountValue": account_value,
            "totalNtlPos": "0",
            "totalRawUsd": account_value,
            "totalMarginUsed": "0",
        },
        "crossMaintenanceMarginUsed": "0",
        "withdrawable": account_value,
        "assetPositions": [],
    })
}

fn test_http_request_complete(buffer: &[u8]) -> bool {
    let Some(header_end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    buffer.len() >= header_end + 4 + content_length
}

fn test_hyperliquid_spot_meta() -> Value {
    json!({
        "universe": [
            {
                "tokens": [0, 1],
                "name": "UETH/USDC",
                "index": 0,
                "isCanonical": true,
            },
            {
                "tokens": [2, 1],
                "name": "UBTC/USDC",
                "index": 1,
                "isCanonical": true,
            },
        ],
        "tokens": [
            {
                "name": "UETH",
                "szDecimals": 4,
                "weiDecimals": 18,
                "index": 0,
                "isCanonical": true,
            },
            {
                "name": "USDC",
                "szDecimals": 2,
                "weiDecimals": 6,
                "index": 1,
                "isCanonical": true,
            },
            {
                "name": "UBTC",
                "szDecimals": 5,
                "weiDecimals": 8,
                "index": 2,
                "isCanonical": true,
            },
        ],
    })
}

#[test]
fn same_leg_completed_step_blocks_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let completed_same_leg = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Completed,
    );

    assert!(leg_already_crossed_provider_boundary(
        &current_step,
        &[completed_same_leg, current_step.clone()],
        &[]
    ));
}

#[test]
fn same_leg_provider_operation_blocks_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let submitted_same_leg = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Running,
    );
    let provider_operation = test_provider_operation(order_id, attempt_id, submitted_same_leg.id);

    assert!(leg_already_crossed_provider_boundary(
        &current_step,
        &[submitted_same_leg, current_step.clone()],
        &[provider_operation]
    ));
}

#[test]
fn different_leg_completed_step_does_not_block_pre_execution_refresh() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let current_leg_id = Uuid::now_v7();
    let other_leg_id = Uuid::now_v7();
    let current_step = test_execution_step(
        order_id,
        attempt_id,
        current_leg_id,
        1,
        OrderExecutionStepStatus::Running,
    );
    let completed_other_leg = test_execution_step(
        order_id,
        attempt_id,
        other_leg_id,
        0,
        OrderExecutionStepStatus::Completed,
    );

    assert!(!leg_already_crossed_provider_boundary(
        &current_step,
        &[completed_other_leg, current_step.clone()],
        &[]
    ));
}

#[test]
fn stale_quote_refresh_budget_refreshes_first_pre_execution_stale_quote() {
    assert_eq!(
        stale_quote_refresh_budget_decision(0, DEFAULT_QUOTE_REFRESH_MAX_ATTEMPTS),
        StaleQuoteRefreshBudgetDecision::Refresh
    );
    assert_eq!(
        stale_quote_step_failure_decision(0, DEFAULT_QUOTE_REFRESH_MAX_ATTEMPTS),
        StepFailureDecision::RefreshQuote
    );
}

#[test]
fn stale_quote_refresh_budget_refunds_after_three_refresh_attempts() {
    assert_eq!(
        stale_quote_refresh_budget_decision(3, DEFAULT_QUOTE_REFRESH_MAX_ATTEMPTS),
        StaleQuoteRefreshBudgetDecision::RefundRequired
    );
    assert_eq!(
        stale_quote_step_failure_decision(3, DEFAULT_QUOTE_REFRESH_MAX_ATTEMPTS),
        StepFailureDecision::StartRefund
    );
}

#[tokio::test]
async fn classify_stale_running_step_records_all_decisions() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());

    let durable = seed_running_step(&test_db.pool, true, true).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: durable.order_id.into(),
            attempt_id: durable.attempt_id.into(),
            step_id: durable.step_id.into(),
        },
    )
    .await
    .expect("classify durable progress");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::DurableProviderOperationWaitingExternalProgress
    );
    assert_step_classification(
        &test_db.db,
        durable.step_id,
        "durable_provider_operation_waiting_external_progress",
    )
    .await;

    let ambiguous = seed_running_step(&test_db.pool, true, false).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: ambiguous.order_id.into(),
            attempt_id: ambiguous.attempt_id.into(),
            step_id: ambiguous.step_id.into(),
        },
    )
    .await
    .expect("classify ambiguous external window");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::AmbiguousExternalSideEffectWindow
    );
    assert_step_classification(
        &test_db.db,
        ambiguous.step_id,
        "ambiguous_external_side_effect_window",
    )
    .await;

    let missing_checkpoint = seed_running_step(&test_db.pool, false, false).await;
    let classified = classify_stale_running_step_for_deps(
        &deps,
        ClassifyStaleRunningStepInput {
            order_id: missing_checkpoint.order_id.into(),
            attempt_id: missing_checkpoint.attempt_id.into(),
            step_id: missing_checkpoint.step_id.into(),
        },
    )
    .await
    .expect("classify missing checkpoint");
    assert_eq!(
        classified.decision,
        StaleRunningStepDecision::StaleRunningStepWithoutCheckpoint
    );
    assert_step_classification(
        &test_db.db,
        missing_checkpoint.step_id,
        "stale_running_step_without_checkpoint",
    )
    .await;
}

#[tokio::test]
async fn waiting_external_transition_records_latency_timestamp() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    let waiting_at = Utc::now();

    let step = test_db
        .db
        .orders()
        .wait_execution_step(seeded.step_id, json!({"kind": "waiting"}), None, waiting_at)
        .await
        .expect("mark step waiting external");
    assert_eq!(step.status, OrderExecutionStepStatus::Waiting);

    let latency = test_db
        .db
        .orders()
        .get_execution_step_latency_record(seeded.step_id)
        .await
        .expect("load latency record");
    assert!(latency.waiting_external_at.is_some());
    assert_eq!(latency.hint_arrived_at, None);
}

#[tokio::test]
async fn hint_arrival_records_latency_timestamp() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    let arrived_at = Utc::now();

    test_db
        .db
        .orders()
        .record_execution_step_hint_arrival(seeded.step_id, arrived_at)
        .await
        .expect("record hint arrival");

    let latency = test_db
        .db
        .orders()
        .get_execution_step_latency_record(seeded.step_id)
        .await
        .expect("load latency record");
    assert!(latency.hint_arrived_at.is_some());
}

#[tokio::test]
async fn unit_withdrawal_retry_reuses_existing_protocol_address_without_second_gen() {
    let (gen_calls, _send_asset_calls) = run_unit_withdrawal_retry_idempotency_scenario().await;

    assert_eq!(
        gen_calls, 1,
        "unit_withdrawal retry must not call HyperUnit /gen after a prior attempt persisted the protocol address"
    );
}

#[tokio::test]
async fn unit_withdrawal_retry_does_not_resend_hyperliquid_send_asset() {
    let (_gen_calls, send_asset_calls) = run_unit_withdrawal_retry_idempotency_scenario().await;

    assert_eq!(
        send_asset_calls, 1,
        "unit_withdrawal retry must observe the existing operation instead of firing sendAsset again"
    );
}

#[tokio::test]
async fn unit_deposit_first_attempt_persists_idempotency_key_and_transfer_action() {
    let test_db = test_database().await;
    let vault_id = Uuid::now_v7();
    let (first_step, _) =
        seed_unit_deposit_retry_steps(&test_db.db, &test_db.pool, vault_id, vault_id).await;
    let (deps, gen_calls, transfer_actions_built) =
        unit_deposit_idempotency_test_deps(test_db.db.clone());
    let expected_key = unit_deposit_expected_idempotency_key(&first_step, vault_id);
    let expected_protocol_address = "0x0000000000000000000000000000000000000001";

    let result = execute_unit_deposit_step(&deps, &first_step)
        .await
        .expect("execute first unit deposit dispatch");

    assert_eq!(gen_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transfer_actions_built.load(Ordering::SeqCst), 1);
    let StepDispatchResult::ProviderIntent(ProviderExecutionIntent::CustodyActions {
        actions,
        state,
        ..
    }) = result
    else {
        panic!("first unit_deposit dispatch should return a custody transfer intent");
    };
    assert_eq!(actions.len(), 1);
    let CustodyAction::Transfer {
        to_address,
        amount,
        bitcoin_fee_budget_sats,
    } = &actions[0]
    else {
        panic!("unit_deposit should build a transfer action");
    };
    assert_eq!(to_address, expected_protocol_address);
    assert_eq!(amount, "100");
    assert_eq!(bitcoin_fee_budget_sats, &None);
    let operation_intent = state
        .operation
        .as_ref()
        .expect("unit_deposit intent should carry provider operation");
    assert_eq!(
        operation_intent.idempotency_key.as_deref(),
        Some(expected_key.as_str())
    );

    let operation = load_unit_deposit_operation(&test_db.db, &expected_key).await;
    assert_eq!(
        operation.idempotency_key.as_deref(),
        Some(expected_key.as_str())
    );
    assert_eq!(
        operation.provider_ref.as_deref(),
        Some(expected_protocol_address)
    );
    let addresses = test_db
        .db
        .orders()
        .get_provider_addresses_by_operation(operation.id)
        .await
        .expect("load unit deposit provider addresses");
    assert_eq!(addresses.len(), 1);
    assert_eq!(addresses[0].role, ProviderAddressRole::UnitDeposit);
    assert_eq!(addresses[0].address, expected_protocol_address);
}

#[tokio::test]
async fn unit_deposit_retry_reuses_existing_protocol_address_without_second_gen_or_transfer() {
    let test_db = test_database().await;
    let vault_id = Uuid::now_v7();
    let (first_step, retry_step) =
        seed_unit_deposit_retry_steps(&test_db.db, &test_db.pool, vault_id, vault_id).await;
    let (deps, gen_calls, transfer_actions_built) =
        unit_deposit_idempotency_test_deps(test_db.db.clone());
    let expected_key = unit_deposit_expected_idempotency_key(&first_step, vault_id);

    execute_unit_deposit_step(&deps, &first_step)
        .await
        .expect("execute first unit deposit dispatch");
    let first_operation = load_unit_deposit_operation(&test_db.db, &expected_key).await;

    let retry_result = execute_unit_deposit_step(&deps, &retry_step)
        .await
        .expect("execute retry unit deposit dispatch");

    assert_eq!(
        gen_calls.load(Ordering::SeqCst),
        1,
        "unit_deposit retry must not call HyperUnit /gen after a prior attempt persisted the protocol address"
    );
    assert_eq!(
        transfer_actions_built.load(Ordering::SeqCst),
        1,
        "unit_deposit retry must observe the existing operation instead of building a second transfer"
    );
    let StepDispatchResult::Complete(completion) = retry_result else {
        panic!("retry unit_deposit dispatch should reuse the existing provider operation");
    };
    assert_eq!(
        completion.response.get("kind").and_then(Value::as_str),
        Some("unit_deposit_idempotency_reuse")
    );
    assert_eq!(completion.outcome, StepExecutionOutcome::Waiting);
    let retry_operation = completion
        .provider_state
        .operation
        .expect("retry completion should carry provider operation state");
    assert_eq!(retry_operation.provider_ref, first_operation.provider_ref);
    assert_eq!(
        retry_operation.idempotency_key.as_deref(),
        Some(expected_key.as_str())
    );
}

#[tokio::test]
async fn unit_deposit_idempotency_key_collapses_retry_attempts_onto_same_operation() {
    // Under the uniform `(order_id, step_type, step_index)` key shape, a retry
    // attempt re-enters with a different `step_id` / `attempt_id` but the same
    // `(order_id, step_index)` and therefore the same idempotency key. This is
    // the whole point of the guard: the second dispatch must short-circuit on
    // the first attempt's persisted operation rather than fire a second `/gen`
    // and a second on-chain transfer — even if other request fields (like the
    // source custody vault id) happen to differ.
    let test_db = test_database().await;
    let first_vault_id = Uuid::now_v7();
    let retry_vault_id = Uuid::now_v7();
    let (first_step, retry_step) =
        seed_unit_deposit_retry_steps(&test_db.db, &test_db.pool, first_vault_id, retry_vault_id)
            .await;
    let (deps, gen_calls, transfer_actions_built) =
        unit_deposit_idempotency_test_deps(test_db.db.clone());
    let first_key = unit_deposit_expected_idempotency_key(&first_step, first_vault_id);
    let retry_key = unit_deposit_expected_idempotency_key(&retry_step, retry_vault_id);

    assert_eq!(
        first_key, retry_key,
        "uniform key shape must collapse retries onto the same idempotency key"
    );

    execute_unit_deposit_step(&deps, &first_step)
        .await
        .expect("execute first unit deposit dispatch");
    let retry_result = execute_unit_deposit_step(&deps, &retry_step)
        .await
        .expect("execute retry unit deposit dispatch");

    assert_eq!(gen_calls.load(Ordering::SeqCst), 1);
    assert_eq!(transfer_actions_built.load(Ordering::SeqCst), 1);
    let StepDispatchResult::Complete(_) = retry_result else {
        panic!("retry dispatch must short-circuit on the first attempt's operation");
    };
    let first_operation = load_unit_deposit_operation(&test_db.db, &first_key).await;
    assert_eq!(
        first_operation.provider_ref.as_deref(),
        Some("0x0000000000000000000000000000000000000001")
    );
}

/// Tier 1's `execute_idempotent_step` retry guard must still hold after the
/// Tier 2 consolidation that wraps everything in `run_step_dispatch`. We
/// invoke the consolidated dispatch body directly with two attempts that share
/// `(order_id, step_index)` and assert the provider's `/gen` call count stays
/// at 1 — the second attempt must short-circuit on the first attempt's
/// persisted `order_provider_operations` row instead of re-firing the side
/// effect.
///
/// Uses `unit_withdrawal` because its provider returns
/// `ProviderExecutionIntent::ProviderOnly` (no custody-action side effect to
/// stage in test infrastructure), keeping the test focused on the idempotency
/// guard rather than the custody executor wiring.
#[tokio::test]
async fn dispatch_step_retry_short_circuits_on_persisted_operation() {
    let test_db = test_database().await;
    let exchange_server = TestHyperliquidExchangeServer::spawn().await;
    let (unit_provider, gen_calls) =
        CountingUnitWithdrawalProvider::new(exchange_server.base_url.clone());
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(unit_provider)],
        vec![],
    ));
    let settings = test_settings();
    let derivation_salt = [0x55_u8; 32];
    let hyperliquid_chain = Arc::new(HyperliquidChain::new(
        b"hyperliquid-wallet",
        1,
        std::time::Duration::from_secs(1),
    ));
    let vault_address = hyperliquid_chain
        .derive_wallet(&settings.master_key_bytes(), &derivation_salt)
        .expect("derive test Hyperliquid wallet")
        .address
        .clone();
    let mut chain_registry = ChainRegistry::new();
    chain_registry.register(ChainType::Hyperliquid, hyperliquid_chain);
    let deps = test_deps_with_action_providers_and_chain_registry(
        test_db.db.clone(),
        action_providers,
        Some(exchange_server.base_url.clone()),
        Arc::new(chain_registry),
    );
    let (first_step, retry_step) = seed_unit_withdrawal_retry_steps(
        &test_db.db,
        &test_db.pool,
        Uuid::now_v7(),
        vault_address,
        derivation_salt,
    )
    .await;

    let first = run_step_dispatch(
        &deps,
        DispatchStepInput {
            order_id: first_step.order_id.into(),
            attempt_id: first_step
                .execution_attempt_id
                .expect("first step has attempt id")
                .into(),
            step_id: first_step.id.into(),
        },
        OrderExecutionStepType::UnitWithdrawal,
    )
    .await
    .expect("first dispatch_unit_withdrawal_step");
    assert!(matches!(first.outcome, DispatchOutcome::Waiting));

    // Mark the first step cancelled so the retry can re-enter dispatch on a
    // fresh row (mirrors how `materialize_retry_attempt` re-creates the step).
    sqlx_core::query::query(
        r#"
        UPDATE order_execution_steps
        SET status = 'cancelled',
            completed_at = $2,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(first_step.id)
    .bind(Utc::now())
    .execute(&test_db.pool)
    .await
    .expect("mark first unit withdrawal step cancelled");

    let retry = run_step_dispatch(
        &deps,
        DispatchStepInput {
            order_id: retry_step.order_id.into(),
            attempt_id: retry_step
                .execution_attempt_id
                .expect("retry step has attempt id")
                .into(),
            step_id: retry_step.id.into(),
        },
        OrderExecutionStepType::UnitWithdrawal,
    )
    .await
    .expect("retry dispatch_unit_withdrawal_step");
    assert!(matches!(retry.outcome, DispatchOutcome::Waiting));

    assert_eq!(
        gen_calls.load(Ordering::SeqCst),
        1,
        "consolidated dispatch must keep the Tier 1 idempotency guard intact: \
         a retry sharing (order_id, step_index) cannot re-fire HyperUnit /gen"
    );
    assert_eq!(
        exchange_server.send_asset_call_count(),
        1,
        "consolidated dispatch must not re-fire the Hyperliquid sendAsset side effect on retry"
    );
}

/// FIX A part 2: when a prior attempt's `order_provider_operations` row was
/// marked `Failed` (its side effect failed to land), the retry's
/// `execute_idempotent_step` must NOT route it through
/// `existing_operation_completion` — that would yield a `Waiting` outcome and
/// the workflow would wait forever for an external event that never arrives.
/// Instead the retry must surface a non-retryable `ProviderOperationLost`
/// error so `classify_step_failure` routes the order to a refund.
#[tokio::test]
async fn idempotent_step_retry_surfaces_failed_operation_as_non_retryable() {
    let test_db = test_database().await;
    let exchange_server = TestHyperliquidExchangeServer::spawn().await;
    let (unit_provider, _gen_calls) =
        CountingUnitWithdrawalProvider::new(exchange_server.base_url.clone());
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(unit_provider)],
        vec![],
    ));
    let settings = test_settings();
    let derivation_salt = [0x66_u8; 32];
    let hyperliquid_chain = Arc::new(HyperliquidChain::new(
        b"hyperliquid-wallet",
        1,
        std::time::Duration::from_secs(1),
    ));
    let vault_address = hyperliquid_chain
        .derive_wallet(&settings.master_key_bytes(), &derivation_salt)
        .expect("derive test Hyperliquid wallet")
        .address
        .clone();
    let mut chain_registry = ChainRegistry::new();
    chain_registry.register(ChainType::Hyperliquid, hyperliquid_chain);
    let deps = test_deps_with_action_providers_and_chain_registry(
        test_db.db.clone(),
        action_providers,
        Some(exchange_server.base_url.clone()),
        Arc::new(chain_registry),
    );
    let (first_step, retry_step) = seed_unit_withdrawal_retry_steps(
        &test_db.db,
        &test_db.pool,
        Uuid::now_v7(),
        vault_address,
        derivation_salt,
    )
    .await;

    // First dispatch persists the provider operation row and waits.
    let first = run_step_dispatch(
        &deps,
        DispatchStepInput {
            order_id: first_step.order_id.into(),
            attempt_id: first_step
                .execution_attempt_id
                .expect("first step has attempt id")
                .into(),
            step_id: first_step.id.into(),
        },
        OrderExecutionStepType::UnitWithdrawal,
    )
    .await
    .expect("first dispatch_unit_withdrawal_step");
    assert!(matches!(first.outcome, DispatchOutcome::Waiting));

    // Simulate FIX A part 1's outcome: the side effect failed to land, so the
    // persisted operation row was marked `Failed`.
    sqlx_core::query::query(
        r#"
        UPDATE order_provider_operations
        SET status = 'failed', updated_at = $2
        WHERE order_id = $1
        "#,
    )
    .bind(first_step.order_id)
    .bind(Utc::now())
    .execute(&test_db.pool)
    .await
    .expect("mark provider operation failed");

    // The retry must surface the dead operation as a non-retryable failure.
    let retry_result = execute_idempotent_step(
        &deps,
        &retry_step,
        BridgeOrUnitNoop, // unused: lookup short-circuits before build_intent
    )
    .await;
    let retry_error = match retry_result {
        Ok(_) => panic!("retry must fail when the prior operation is Failed"),
        Err(error) => error,
    };
    assert!(
        matches!(
            retry_error,
            OrderActivityError::ProviderOperationLost { .. }
        ),
        "expected ProviderOperationLost, got {retry_error:?}"
    );
    assert!(
        retry_error.is_non_retryable(),
        "ProviderOperationLost must be non-retryable so the workflow refunds"
    );
}

/// A no-op `IdempotentStepExecutor` whose `operation_type` matches the unit
/// withdrawal row. `build_intent` is unreachable in the test above because the
/// `Failed`-row lookup short-circuits first; it panics to make any regression
/// (lookup no longer short-circuiting) loud rather than silent.
struct BridgeOrUnitNoop;

impl IdempotentStepExecutor for BridgeOrUnitNoop {
    fn operation_type(&self) -> ProviderOperationType {
        ProviderOperationType::UnitWithdrawal
    }

    async fn build_intent(
        self,
        _deps: &OrderActivityDeps,
        _step: &OrderExecutionStep,
        _idempotency_key: &str,
    ) -> Result<ProviderExecutionIntent, OrderActivityError> {
        panic!("build_intent must not run: the Failed-row lookup short-circuits first");
    }
}

/// After Tier 2, the `verify_<hint_kind>_hint` activities own the step
/// completion settle that previously lived as a separate `settle_provider_step`
/// round-trip in the workflow. This test seeds a step in `waiting` with a
/// `Completed` provider operation and asserts that running through
/// `run_hint_verify` (with a stub verifier that mimics the venue helper's
/// "update operation row + return Accept" behaviour) drives the step to
/// `completed`.
#[tokio::test]
async fn run_hint_verify_settles_step_to_completed_on_accept() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;

    // Transition the step into `waiting` so the hint-verify settle path's
    // `complete_observed_execution_step` branch is the one we exercise.
    test_db
        .db
        .orders()
        .wait_execution_step(seeded.step_id, json!({"kind": "waiting"}), None, Utc::now())
        .await
        .expect("seed waiting step");

    // Insert a completed provider operation linked to the step. The settle
    // path reads back the latest operation for the step and uses its status to
    // pick `complete_observed_execution_step` vs `wait_execution_step`.
    let provider_operation_id = Uuid::now_v7();
    sqlx_core::query::query(
        r#"
        INSERT INTO order_provider_operations (
            id, order_id, execution_attempt_id, execution_step_id,
            provider, operation_type, provider_ref, idempotency_key,
            status, request_json, response_json, observed_state_json,
            created_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, 'velora', 'universal_router_swap', '0xfeed',
            'order:test:run_hint_verify_settles', 'completed',
            '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $5, $5
        )
        "#,
    )
    .bind(provider_operation_id)
    .bind(seeded.order_id)
    .bind(seeded.attempt_id)
    .bind(seeded.step_id)
    .bind(Utc::now())
    .execute(&test_db.pool)
    .await
    .expect("insert completed provider operation");

    let deps = test_deps(test_db.db.clone());

    // Stub verifier: mimics a typed verifier returning Accept without further
    // mutating the operation (the operation row already says completed). The
    // settle in `run_hint_verify` is the behaviour under test.
    async fn stub_accept_verifier(
        _deps: &OrderActivityDeps,
        _input: VerifyProviderOperationHintInput,
    ) -> Result<ProviderOperationHintVerified, OrderActivityError> {
        Ok(ProviderOperationHintVerified {
            provider_operation_id: None,
            decision: ProviderOperationHintDecision::Accept,
            reason: None,
        })
    }

    let verified = run_hint_verify(
        &deps,
        VerifyProviderOperationHintInput {
            order_id: seeded.order_id.into(),
            attempt_id: seeded.attempt_id.into(),
            step_id: seeded.step_id.into(),
            signal: ProviderOperationHintSignal {
                hint_id: WorkflowHintId::from(Uuid::now_v7()),
                order_id: seeded.order_id.into(),
                provider_operation_id: Some(provider_operation_id.into()),
                execution_step_id: seeded.step_id.into(),
                hint_kind: ProviderHintKind::VeloraSwapSettled,
                provider: ProviderKind::Exchange,
                provider_ref: Some("0xfeed".to_string()),
                evidence: None,
            },
        },
        ExpectedHintKinds::Single(ProviderHintKind::VeloraSwapSettled),
        stub_accept_verifier,
    )
    .await
    .expect("run_hint_verify should accept and settle");

    assert_eq!(verified.decision, ProviderOperationHintDecision::Accept);

    let step = test_db
        .db
        .orders()
        .get_execution_step(seeded.step_id)
        .await
        .expect("load step after hint settle");
    assert_eq!(
        step.status,
        OrderExecutionStepStatus::Completed,
        "verify_*_hint Accept must settle the step to completed"
    );
}

#[tokio::test]
async fn hyperliquid_bridge_quotes_build_refund_quote_leg_shapes() {
    let registry = ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
        across: None,
        cctp: None,
        hyperunit_base_url: None,
        hyperunit_proxy_url: None,
        hyperliquid_base_url: Some("http://127.0.0.1:1".to_string()),
        velora: None,
        hyperliquid_network: HyperliquidCallNetwork::Testnet,
        hyperliquid_order_timeout_ms: 30_000,
        hypercore_bridge_enabled: false,
    })
    .expect("hyperliquid bridge provider registry");
    let bridge = registry
        .bridge(ProviderId::HyperliquidBridge.as_str())
        .expect("hyperliquid bridge provider");
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());

    let deposit_quote = bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: external_usdc.clone(),
            destination_asset: hl_usdc.clone(),
            order_kind: ProviderOrderKind::ExactIn {
                amount_in: "150000000".to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: test_address(1),
            depositor_address: test_address(2),
            partial_fills_enabled: false,
        })
        .await
        .expect("quote HL deposit")
        .expect("HL deposit quote");
    let deposit_legs =
        refund_bridge_quote_legs(&deposit, &deposit_quote).expect("deposit quote legs");
    assert_eq!(deposit_legs.len(), 1);
    assert_eq!(
        deposit_legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidBridgeDeposit
    );
    assert_eq!(deposit_legs[0].amount_in, "150000000");
    assert_eq!(deposit_legs[0].amount_out, "150000000");

    let withdrawal_quote = bridge
        .quote_bridge(BridgeQuoteRequest {
            source_asset: hl_usdc,
            destination_asset: external_usdc,
            order_kind: ProviderOrderKind::ExactIn {
                amount_in: "150000000".to_string(),
                min_amount_out: Some("1".to_string()),
            },
            recipient_address: test_address(1),
            depositor_address: test_address(2),
            partial_fills_enabled: false,
        })
        .await
        .expect("quote HL withdrawal")
        .expect("HL withdrawal quote");
    let withdrawal_legs =
        refund_bridge_quote_legs(&withdrawal, &withdrawal_quote).expect("withdrawal quote legs");
    assert_eq!(withdrawal_legs.len(), 1);
    assert_eq!(
        withdrawal_legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal
    );
    assert_eq!(withdrawal_legs[0].amount_in, "150000000");
    assert_eq!(withdrawal_legs[0].amount_out, "149000000");
}

#[test]
fn external_custody_hyperliquid_bridge_path_materializes_steps() {
    let planned_at = Utc::now();
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let order = test_order(external_usdc.clone(), planned_at);
    let vault = test_custody_vault(
        &order,
        CustodyVaultRole::DestinationExecution,
        &external_usdc,
    );
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let quoted_path = RefundQuotedPath {
        path: TransitionPath {
            id: "hl-deposit>hl-withdrawal".to_string(),
            transitions: vec![deposit.clone(), withdrawal.clone()],
        },
        amount_out: "149000000".to_string(),
        legs: vec![
            quote_leg_for_transition(&deposit, "150000000", "150000000", planned_at),
            quote_leg_for_transition(&withdrawal, "150000000", "149000000", planned_at),
        ],
    };

    let (legs, steps) =
        materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
            .expect("materialize HL bridge refund path");

    assert_eq!(legs.len(), 2);
    assert_eq!(steps.len(), 2);
    assert_eq!(
        steps[0].step_type,
        OrderExecutionStepType::HyperliquidBridgeDeposit
    );
    assert_eq!(steps[0].provider, ProviderId::HyperliquidBridge.as_str());
    assert_eq!(steps[0].input_asset, Some(external_usdc.clone()));
    assert_eq!(steps[0].output_asset, Some(hl_usdc.clone()));
    assert_eq!(steps[0].amount_in.as_deref(), Some("150000000"));
    assert_eq!(
        steps[0].request.get("source_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0]
            .request
            .get("source_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );

    assert_eq!(
        steps[1].step_type,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal
    );
    assert_eq!(steps[1].provider, ProviderId::HyperliquidBridge.as_str());
    assert_eq!(steps[1].input_asset, Some(hl_usdc));
    assert_eq!(steps[1].output_asset, Some(external_usdc.clone()));
    assert_eq!(steps[1].amount_in.as_deref(), Some("150000000"));
    assert_eq!(
        steps[1].request.get("hyperliquid_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_chain_id")
            .and_then(Value::as_str),
        Some("evm:42161")
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_asset_id")
            .and_then(Value::as_str),
        Some("0xaf88d065e77c8cc2239327c5edb3a432268e5831")
    );
    assert_eq!(
        steps[1]
            .request
            .get("transfer_from_spot")
            .and_then(Value::as_bool),
        Some(false)
    );
}

#[test]
fn external_custody_hyperliquid_bridge_role_gate_requires_destination_execution() {
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let deposit = hyperliquid_bridge_deposit_transition(external_usdc.clone(), hl_usdc.clone());
    let withdrawal =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let path = TransitionPath {
        id: "hl-deposit>hl-withdrawal".to_string(),
        transitions: vec![deposit, withdrawal.clone()],
    };

    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::DestinationExecution),
        &path,
    ));
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &path,
    ));

    let first_hop_withdrawal = TransitionPath {
        id: "hl-withdrawal".to_string(),
        transitions: vec![withdrawal],
    };
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::DestinationExecution),
        &first_hop_withdrawal,
    ));
}

#[test]
fn hyperliquid_bridge_withdrawal_marks_derived_destination_for_hydration() {
    let planned_at = Utc::now();
    let external_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let order = test_order(external_usdc.clone(), planned_at);
    let transition =
        hyperliquid_bridge_withdrawal_transition(hl_usdc.clone(), external_usdc.clone());
    let leg = quote_leg_for_transition(&transition, "150000000", "149000000", planned_at);
    let custody = RefundHyperliquidBinding::DerivedDestinationExecution {
        asset: external_usdc,
    };

    let step = refund_transition_hyperliquid_bridge_withdrawal_step(
        &order,
        &transition,
        &leg,
        &custody,
        false,
        3,
        planned_at,
    )
    .expect("build derived HL withdrawal step");

    assert_eq!(
        step.request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        step.request.get("hyperliquid_custody_vault_address"),
        Some(&Value::Null)
    );
    assert_eq!(
        step.request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        step.details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("pending_derivation")
    );
    assert_eq!(
        step.request
            .get("recipient_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
}

#[test]
fn unit_refund_quote_legs_are_passthrough_shapes() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

    let deposit_leg = refund_unit_deposit_quote_leg(&deposit, "30000", planned_at);
    assert_eq!(
        deposit_leg.execution_step_type,
        OrderExecutionStepType::UnitDeposit
    );
    assert_eq!(deposit_leg.amount_in, "30000");
    assert_eq!(deposit_leg.amount_out, "30000");
    assert_eq!(deposit_leg.raw, json!({}));

    let withdrawal_leg = refund_unit_withdrawal_quote_leg(&order, &withdrawal, "30000", planned_at);
    assert_eq!(
        withdrawal_leg.execution_step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(withdrawal_leg.amount_in, "30000");
    assert_eq!(withdrawal_leg.amount_out, "30000");
    assert_eq!(
        withdrawal_leg
            .raw
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[test]
fn external_custody_unit_path_materializes_deposit_and_withdrawal_steps() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &btc);
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let quoted_path = RefundQuotedPath {
        path: TransitionPath {
            id: "unit-deposit>unit-withdrawal".to_string(),
            transitions: vec![deposit.clone(), withdrawal.clone()],
        },
        amount_out: "30000".to_string(),
        legs: vec![
            quote_leg_for_transition(&deposit, "30000", "30000", planned_at),
            quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at),
        ],
    };

    let (legs, steps) =
        materialize_external_custody_refund_path(&order, &vault, &quoted_path, planned_at)
            .expect("materialize Unit refund path");

    assert_eq!(legs.len(), 2);
    assert_eq!(steps.len(), 2);
    assert_eq!(steps[0].step_type, OrderExecutionStepType::UnitDeposit);
    assert_eq!(steps[0].provider, ProviderId::Unit.as_str());
    assert_eq!(steps[0].input_asset, Some(btc.clone()));
    assert_eq!(steps[0].output_asset, None);
    assert_eq!(steps[0].amount_in.as_deref(), Some("30000"));
    assert_eq!(
        steps[0].request.get("source_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0].request.get("revert_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        steps[0]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );

    assert_eq!(steps[1].step_type, OrderExecutionStepType::UnitWithdrawal);
    assert_eq!(steps[1].provider, ProviderId::Unit.as_str());
    assert_eq!(steps[1].input_asset, Some(hl_btc));
    assert_eq!(steps[1].output_asset, Some(btc));
    assert_eq!(steps[1].amount_in.as_deref(), Some("30000"));
    assert_eq!(
        steps[1].request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        steps[1]
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );
    assert_eq!(
        steps[1]
            .details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("pending_derivation")
    );
    assert_eq!(
        steps[1]
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[test]
fn hyperliquid_spot_refund_path_to_bitcoin_materializes_final_unit_withdrawal() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let mut order = test_order(btc.clone(), planned_at);
    order.refund_address = "bc1qpr6b7atestrefund0000000000000000000000".to_string();
    let vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_eth);
    let eth_to_usdc = hyperliquid_trade_transition(
        hl_eth.clone(),
        hl_usdc.clone(),
        CanonicalAsset::Eth,
        CanonicalAsset::Usdc,
    );
    let usdc_to_btc = hyperliquid_trade_transition(
        hl_usdc.clone(),
        hl_btc.clone(),
        CanonicalAsset::Usdc,
        CanonicalAsset::Btc,
    );
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let quoted_path = RefundQuotedPath {
        path: TransitionPath {
            id: "hl-eth-to-usdc>hl-usdc-to-btc>unit-withdrawal".to_string(),
            transitions: vec![eth_to_usdc.clone(), usdc_to_btc.clone(), withdrawal.clone()],
        },
        amount_out: "50000".to_string(),
        legs: vec![
            hyperliquid_trade_quote_leg(
                &eth_to_usdc,
                "1000000000000000000",
                "5000000000",
                planned_at,
            ),
            hyperliquid_trade_quote_leg(&usdc_to_btc, "5000000000", "50000", planned_at),
            refund_unit_withdrawal_quote_leg(&order, &withdrawal, "50000", planned_at),
        ],
    };

    let (_legs, steps) = materialize_hyperliquid_spot_refund_path(
        &order,
        &vault,
        &quoted_path,
        "1000000000000000000",
        false,
        planned_at,
    )
    .expect("materialize HyperliquidSpot refund path");

    assert!(!steps.is_empty());
    let last_step = steps.last().expect("final refund step");
    assert_eq!(last_step.step_type, OrderExecutionStepType::UnitWithdrawal);
    assert_eq!(last_step.input_asset, Some(hl_btc));
    assert_eq!(last_step.output_asset, Some(btc));
    assert_eq!(
        last_step
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[test]
fn hyperliquid_spot_refund_quote_reserves_activation_fee_before_unit_withdrawal() {
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let eth = test_asset("evm:1", "native");
    let trade = hyperliquid_trade_transition(
        hl_usdc.clone(),
        hl_eth.clone(),
        CanonicalAsset::Usdc,
        CanonicalAsset::Eth,
    );
    let withdrawal = unit_withdrawal_transition(hl_eth, eth, CanonicalAsset::Eth);
    let path = TransitionPath {
        id: "hl-usdc-to-ueth>unit-withdrawal".to_string(),
        transitions: vec![trade.clone(), withdrawal],
    };

    let quoted = quote_amount_for_hyperliquid_spot_refund_transition(
        &path,
        0,
        &trade,
        "38994800",
    )
    .expect("quote reserve logic")
    .expect("amount should remain quoteable");

    assert_eq!(quoted, "37994800");
}

#[test]
fn hyperliquid_spot_refund_quote_does_not_reserve_without_downstream_unit_withdrawal() {
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let trade = hyperliquid_trade_transition(
        hl_usdc,
        hl_eth.clone(),
        CanonicalAsset::Usdc,
        CanonicalAsset::Eth,
    );
    let path = TransitionPath {
        id: "hl-usdc-to-ueth".to_string(),
        transitions: vec![trade.clone()],
    };

    let quoted = quote_amount_for_hyperliquid_spot_refund_transition(
        &path,
        0,
        &trade,
        "38994800",
    )
    .expect("quote reserve logic")
    .expect("amount should remain quoteable");

    assert_eq!(quoted, "38994800");
}

#[tokio::test]
async fn hyperliquid_spot_refund_plan_to_bitcoin_is_quotable_and_materialized() {
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn().await;
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(TestUnitProvider) as Arc<dyn UnitProvider>],
        vec![Arc::new(TestHyperliquidExchangeProvider) as Arc<dyn ExchangeProvider>],
    ));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let mut order = test_order(btc.clone(), planned_at);
    order.refund_address = "bc1qpr6b7aplanrefund000000000000000000000".to_string();
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_eth);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &vault,
    )
    .await;

    let shape = materialize_hyperliquid_spot_refund_plan(
        &deps,
        MaterializeRefundPlanInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
            position: SingleRefundPosition {
                position_kind: RecoverablePositionKind::HyperliquidSpot,
                owning_step_id: None,
                funding_vault_id: None,
                custody_vault_id: Some(vault.id.into()),
                asset: hl_eth,
                amount: RawAmount::new("1000000000000000000").expect("valid raw amount"),
                hyperliquid_coin: Some("UETH".to_string()),
                hyperliquid_canonical: Some(CanonicalAsset::Eth),
                requires_clearinghouse_unwrap: false,
            },
        },
    )
    .await
    .expect("materialize HyperliquidSpot refund plan");

    let (refund_attempt_id, workflow_steps) = match shape.outcome {
        RefundPlanOutcome::Materialized {
            refund_attempt_id,
            steps,
        } => (refund_attempt_id, steps),
        RefundPlanOutcome::Untenable { reason } => {
            panic!("expected materialized refund plan, got {reason:?}")
        }
    };
    assert!(!workflow_steps.is_empty());

    let persisted_steps = deps
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt_id.inner())
        .await
        .expect("load persisted refund steps");
    let last_step = persisted_steps.last().expect("persisted final refund step");
    assert_eq!(last_step.step_type, OrderExecutionStepType::UnitWithdrawal);
    assert_eq!(last_step.output_asset, Some(btc));
    assert_eq!(
        last_step
            .request
            .get("recipient_address")
            .and_then(Value::as_str),
        Some(order.refund_address.as_str())
    );
}

#[tokio::test]
async fn hyperliquid_spot_refund_plan_hydrates_nonfinal_unit_withdrawal_recipient() {
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn().await;
    let (unit_provider, _gen_calls) =
        CountingUnitWithdrawalProvider::new(hyperliquid_info.base_url.clone());
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![
            Arc::new(TestBridgeProvider {
                provider_id: ProviderId::Across,
            }) as Arc<dyn router_core::services::action_providers::BridgeProvider>,
            Arc::new(TestBridgeProvider {
                provider_id: ProviderId::Cctp,
            }) as Arc<dyn router_core::services::action_providers::BridgeProvider>,
        ],
        vec![Arc::new(unit_provider) as Arc<dyn UnitProvider>],
        vec![
            Arc::new(TestHyperliquidExchangeProvider) as Arc<dyn ExchangeProvider>,
            Arc::new(TestVeloraExchangeProvider) as Arc<dyn ExchangeProvider>,
        ],
    ));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let native_ethereum = test_asset("evm:1", "native");
    let native_base = test_asset("evm:8453", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let vault = test_custody_vault(&order, CustodyVaultRole::DestinationExecution, &source_usdc);
    let mut native_ethereum_vault = test_custody_vault(
        &order,
        CustodyVaultRole::DestinationExecution,
        &native_ethereum,
    );
    native_ethereum_vault.address = test_address(10);
    let mut native_base_vault =
        test_custody_vault(&order, CustodyVaultRole::DestinationExecution, &native_base);
    native_base_vault.address = test_address(11);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &vault,
    )
    .await;
    test_db
        .db
        .orders()
        .create_custody_vault(&native_ethereum_vault)
        .await
        .expect("insert native Ethereum destination execution vault");
    test_db
        .db
        .orders()
        .create_custody_vault(&native_base_vault)
        .await
        .expect("insert native Base destination execution vault");

    let shape = materialize_hyperliquid_spot_refund_plan(
        &deps,
        MaterializeRefundPlanInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
            position: SingleRefundPosition {
                position_kind: RecoverablePositionKind::HyperliquidSpot,
                owning_step_id: None,
                funding_vault_id: None,
                custody_vault_id: Some(vault.id.into()),
                asset: hl_eth.clone(),
                amount: RawAmount::new("1000000000000000000").expect("valid raw amount"),
                hyperliquid_coin: Some("UETH".to_string()),
                hyperliquid_canonical: Some(CanonicalAsset::Eth),
                requires_clearinghouse_unwrap: false,
            },
        },
    )
    .await
    .expect("materialize HyperliquidSpot refund plan");

    let refund_attempt_id = match shape.outcome {
        RefundPlanOutcome::Materialized {
            refund_attempt_id, ..
        } => refund_attempt_id,
        RefundPlanOutcome::Untenable { reason } => {
            panic!("expected materialized refund plan, got {reason:?}")
        }
    };
    let persisted_steps = deps
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt_id.inner())
        .await
        .expect("load persisted refund steps");
    let unit_withdrawal = persisted_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::UnitWithdrawal)
        .expect("non-final Unit withdrawal refund step");
    assert_eq!(unit_withdrawal.output_asset, Some(native_ethereum.clone()));
    UnitWithdrawalStepRequest::from_value(&unit_withdrawal.request)
        .expect("hydrated Unit withdrawal request decodes");
    let recipient_address = unit_withdrawal
        .request
        .get("recipient_address")
        .and_then(Value::as_str)
        .expect("hydrated recipient address");
    let recipient_vault_id = unit_withdrawal
        .request
        .get("recipient_custody_vault_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok())
        .expect("hydrated recipient vault id");
    let recipient_vault = deps
        .db
        .orders()
        .get_custody_vault(recipient_vault_id)
        .await
        .expect("load hydrated recipient vault");
    assert_eq!(recipient_vault.role, CustodyVaultRole::DestinationExecution);
    assert_eq!(recipient_vault.chain, native_ethereum.chain);
    assert_eq!(recipient_vault.asset.as_ref(), Some(&native_ethereum.asset));
    assert_eq!(recipient_vault.address, recipient_address);

    let bridge = persisted_steps
        .iter()
        .find(|step| step.step_type == OrderExecutionStepType::AcrossBridge)
        .expect("bridge step after non-final Unit withdrawal");
    assert_eq!(
        bridge.request.get("depositor_custody_vault_id"),
        unit_withdrawal.request.get("recipient_custody_vault_id")
    );
    assert_eq!(
        bridge.request.get("depositor_address"),
        unit_withdrawal.request.get("recipient_address")
    );
}

#[tokio::test]
async fn hyperliquid_clearinghouse_refund_plan_prepends_unwrap_step() {
    let test_db = test_database().await;
    let hyperliquid_info =
        TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(vec![], "100").await;
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![Arc::new(TestHyperliquidBridgeProvider)
            as Arc<
                dyn router_core::services::action_providers::BridgeProvider,
            >],
        vec![],
        vec![Arc::new(TestHyperliquidExchangeProvider) as Arc<dyn ExchangeProvider>],
    ));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &vault,
    )
    .await;

    let shape = materialize_hyperliquid_spot_refund_plan(
        &deps,
        MaterializeRefundPlanInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
            position: SingleRefundPosition {
                position_kind: RecoverablePositionKind::HyperliquidSpot,
                owning_step_id: None,
                funding_vault_id: None,
                custody_vault_id: Some(vault.id.into()),
                asset: source_usdc,
                amount: RawAmount::new("100000000").expect("valid raw amount"),
                hyperliquid_coin: Some("USDC".to_string()),
                hyperliquid_canonical: Some(CanonicalAsset::Usdc),
                requires_clearinghouse_unwrap: true,
            },
        },
    )
    .await
    .expect("materialize clearinghouse-origin HyperliquidSpot refund plan");

    let refund_attempt_id = match shape.outcome {
        RefundPlanOutcome::Materialized {
            refund_attempt_id, ..
        } => refund_attempt_id,
        RefundPlanOutcome::Untenable { reason } => {
            panic!("expected materialized refund plan, got {reason:?}")
        }
    };
    let persisted_steps = deps
        .db
        .orders()
        .get_execution_steps_for_attempt(refund_attempt_id.inner())
        .await
        .expect("load persisted clearinghouse refund steps");
    let first_step = persisted_steps.first().expect("prep step");
    assert_eq!(
        first_step.step_type,
        OrderExecutionStepType::HyperliquidClearinghouseToSpot
    );
    assert_eq!(first_step.step_index, 0);
    assert_eq!(first_step.amount_in.as_deref(), Some("100000000"));
    assert_eq!(
        first_step
            .request
            .get("hyperliquid_custody_vault_id")
            .and_then(Value::as_str)
            .map(str::to_owned),
        Some(vault.id.to_string())
    );
    let last_step = persisted_steps.last().expect("final refund step");
    assert_eq!(
        last_step.step_type,
        OrderExecutionStepType::HyperliquidBridgeWithdrawal
    );
}

#[tokio::test]
async fn refund_position_discovery_reads_hl_spot_balance_from_source_deposit_vault() {
    let test_db = test_database().await;
    let hyperliquid_info =
        TestHyperliquidInfoServer::spawn_with_balances(vec![test_hyperliquid_spot_balance(
            "USDC", 1, "1", "0",
        )])
        .await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let source_deposit_vault =
        test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &source_deposit_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position from source_deposit HL spot balance");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::HyperliquidSpot
            );
            assert_eq!(
                position.custody_vault_id,
                Some(source_deposit_vault.id.into())
            );
            assert_eq!(position.funding_vault_id, None);
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "1000000");
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("USDC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Usdc));
            assert!(!position.requires_clearinghouse_unwrap);
        }
        other => panic!("expected source_deposit HyperliquidSpot refund position, got {other:?}"),
    }
}

#[tokio::test]
async fn refund_position_discovery_reads_hl_clearinghouse_balance_from_source_deposit_vault() {
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(
        vec![test_hyperliquid_spot_balance("USDC", 1, "0", "0")],
        "100",
    )
    .await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let source_deposit_vault =
        test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &source_deposit_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position from source_deposit HL clearinghouse balance");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::HyperliquidSpot
            );
            assert_eq!(
                position.custody_vault_id,
                Some(source_deposit_vault.id.into())
            );
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "100000000");
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("USDC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Usdc));
            assert!(position.requires_clearinghouse_unwrap);
        }
        other => {
            panic!(
                "expected source_deposit Hyperliquid clearinghouse refund position, got {other:?}"
            )
        }
    }
}

#[tokio::test]
async fn refund_position_discovery_reads_hl_spot_balance_from_destination_execution_vault() {
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(
        vec![test_hyperliquid_spot_balance("USDC", 1, "1", "0")],
        "100",
    )
    .await;
    let (deps, _evm_rpc) = test_deps_with_arbitrum_evm_balance(
        test_db.db.clone(),
        hyperliquid_info.base_url.clone(),
        "0",
    )
    .await;
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let destination_execution_vault =
        test_custody_vault(&order, CustodyVaultRole::DestinationExecution, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &destination_execution_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position from destination_execution HL spot balance");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::HyperliquidSpot
            );
            assert_eq!(
                position.custody_vault_id,
                Some(destination_execution_vault.id.into())
            );
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "1000000");
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("USDC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Usdc));
            assert!(!position.requires_clearinghouse_unwrap);
        }
        other => {
            panic!("expected destination_execution HyperliquidSpot refund position, got {other:?}")
        }
    }
}

#[tokio::test]
async fn refund_position_discovery_reads_hl_clearinghouse_balance_from_destination_execution_vault()
{
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(
        vec![test_hyperliquid_spot_balance("USDC", 1, "0", "0")],
        "100",
    )
    .await;
    let (deps, _evm_rpc) = test_deps_with_arbitrum_evm_balance(
        test_db.db.clone(),
        hyperliquid_info.base_url.clone(),
        "0",
    )
    .await;
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let destination_execution_vault =
        test_custody_vault(&order, CustodyVaultRole::DestinationExecution, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &destination_execution_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position from destination_execution HL clearinghouse balance");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::HyperliquidSpot
            );
            assert_eq!(
                position.custody_vault_id,
                Some(destination_execution_vault.id.into())
            );
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "100000000");
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("USDC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Usdc));
            assert!(position.requires_clearinghouse_unwrap);
        }
        other => {
            panic!(
                "expected destination_execution Hyperliquid clearinghouse refund position, got {other:?}"
            )
        }
    }
}

#[tokio::test]
async fn refund_position_discovery_preserves_destination_execution_evm_balance_without_hl_balance()
{
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(
        vec![test_hyperliquid_spot_balance("USDC", 1, "0", "0")],
        "0",
    )
    .await;
    let (deps, _evm_rpc) = test_deps_with_arbitrum_evm_balance(
        test_db.db.clone(),
        hyperliquid_info.base_url.clone(),
        "2500",
    )
    .await;
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let destination_execution_vault =
        test_custody_vault(&order, CustodyVaultRole::DestinationExecution, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &destination_execution_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position from destination_execution EVM balance");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::ExternalCustody
            );
            assert_eq!(
                position.custody_vault_id,
                Some(destination_execution_vault.id.into())
            );
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "2500");
            assert_eq!(position.hyperliquid_coin, None);
            assert_eq!(position.hyperliquid_canonical, None);
            assert!(!position.requires_clearinghouse_unwrap);
        }
        other => panic!("expected destination_execution ExternalCustody position, got {other:?}"),
    }
}

#[tokio::test]
async fn refund_position_discovery_handles_hl_spot_and_clearinghouse_balances() {
    let test_db = test_database().await;
    let hyperliquid_info = TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(
        vec![test_hyperliquid_spot_balance("UBTC", 0, "0.0005", "0")],
        "100",
    )
    .await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let mut order = test_order(btc.clone(), planned_at);
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let source_deposit_vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &btc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &source_deposit_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover refund position with HL spot and clearinghouse balances");

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::HyperliquidSpot
            );
            assert_eq!(
                position.custody_vault_id,
                Some(source_deposit_vault.id.into())
            );
            assert_eq!(position.asset, btc);
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("UBTC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Btc));
            assert!(!position.requires_clearinghouse_unwrap);
        }
        other => panic!("expected selected HyperliquidSpot refund position, got {other:?}"),
    }
}

#[tokio::test]
async fn refund_position_discovery_ignores_zero_hl_spot_source_deposit_balance() {
    let test_db = test_database().await;
    let hyperliquid_info =
        TestHyperliquidInfoServer::spawn_with_balances(vec![test_hyperliquid_spot_balance(
            "USDC", 1, "0", "0",
        )])
        .await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let source_deposit_vault =
        test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &source_deposit_vault,
    )
    .await;

    let discovery = discover_single_refund_position_with_deps(
        &deps,
        DiscoverSingleRefundPositionInput {
            order_id: order.id.into(),
            failed_attempt_id: failed_attempt_id.into(),
        },
    )
    .await
    .expect("discover zero source_deposit HL spot refund position");

    match discovery.outcome {
        SingleRefundPositionOutcome::Untenable {
            reason:
                RefundUntenableReason::RefundRequiresSingleRecoverablePosition {
                    position_count,
                    recoverable_position_count,
                },
        } => {
            assert_eq!(position_count, 0);
            assert_eq!(recoverable_position_count, 0);
        }
        other => panic!("expected zero-position discovery, got {other:?}"),
    }
}

#[test]
fn refund_position_discovery_zero_positions_is_untenable() {
    let discovery = refund_position_discovery_from_positions(Uuid::now_v7(), vec![]);

    match discovery.outcome {
        SingleRefundPositionOutcome::Untenable {
            reason:
                RefundUntenableReason::RefundRequiresSingleRecoverablePosition {
                    position_count,
                    recoverable_position_count,
                },
        } => {
            assert_eq!(position_count, 0);
            assert_eq!(recoverable_position_count, 0);
        }
        other => panic!("expected untenable zero-position discovery, got {other:?}"),
    }
}

/// Seeds a `router_orders` row plus the given execution attempts (each tuple
/// is `(attempt_id, attempt_index)`) and returns the order id. Used by the
/// `resolve_latest_execution_attempt` tests.
async fn seed_order_with_execution_attempts(pool: &PgPool, attempts: &[(Uuid, i32)]) -> Uuid {
    let specs = attempts
        .iter()
        .map(|(attempt_id, attempt_index)| {
            (
                *attempt_id,
                *attempt_index,
                OrderExecutionAttemptKind::PrimaryExecution,
                OrderExecutionAttemptStatus::Cancelled,
            )
        })
        .collect::<Vec<_>>();
    seed_order_with_execution_attempt_specs(pool, &specs).await
}

async fn seed_order_with_execution_attempt_specs(
    pool: &PgPool,
    attempts: &[(
        Uuid,
        i32,
        OrderExecutionAttemptKind,
        OrderExecutionAttemptStatus,
    )],
) -> Uuid {
    let order_id = Uuid::now_v7();
    let now = Utc::now();
    let trace_id = order_id.simple().to_string();
    let parent_span_id = trace_id[..16].to_string();

    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id, order_type, status, source_chain_id, source_asset_id,
                destination_chain_id, destination_asset_id, recipient_address,
                refund_address, workflow_trace_id, workflow_parent_span_id,
                created_at, updated_at
            )
            VALUES (
                $1, 'market_order', 'refund_required', 'evm:8453', 'native',
                'evm:8453', 'native', '0x0000000000000000000000000000000000000001',
                '0x0000000000000000000000000000000000000002', $2, $3, $4, $4
            )
            "#,
    )
    .bind(order_id)
    .bind(trace_id)
    .bind(parent_span_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert order for execution-attempt resolution test");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (order_id, amount_in, created_at, updated_at)
            VALUES ($1, '100', $2, $2)
            "#,
    )
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert market order action for execution-attempt resolution test");

    for (attempt_id, attempt_index, attempt_kind, status) in attempts {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_attempts (
                    id, order_id, attempt_index, attempt_kind, status,
                    failure_reason_json, input_custody_snapshot_json,
                    created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5,
                        '{}'::jsonb, '{}'::jsonb, $6, $6)
                "#,
        )
        .bind(attempt_id)
        .bind(order_id)
        .bind(attempt_index)
        .bind(attempt_kind.to_db_string())
        .bind(status.to_db_string())
        .bind(now)
        .execute(pool)
        .await
        .expect("insert execution attempt for resolution test");
    }

    order_id
}

#[tokio::test]
async fn resolve_latest_execution_attempt_picks_most_recent_attempt() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let first_attempt = Uuid::now_v7();
    let latest_attempt = Uuid::now_v7();
    let order_id = seed_order_with_execution_attempts(
        &test_db.pool,
        &[(first_attempt, 1), (latest_attempt, 2)],
    )
    .await;

    let resolved = resolve_latest_execution_attempt_with_deps(
        &deps,
        ResolveLatestExecutionAttemptInput {
            order_id: order_id.into(),
        },
    )
    .await
    .expect("resolve latest execution attempt");

    assert_eq!(resolved.attempt_id.inner(), latest_attempt);
}

#[tokio::test]
async fn resolve_latest_execution_attempt_skips_failed_refund_attempts() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let forward_attempt = Uuid::now_v7();
    let failed_refund_attempt = Uuid::now_v7();
    let order_id = seed_order_with_execution_attempt_specs(
        &test_db.pool,
        &[
            (
                forward_attempt,
                4,
                OrderExecutionAttemptKind::RefreshedExecution,
                OrderExecutionAttemptStatus::Failed,
            ),
            (
                failed_refund_attempt,
                5,
                OrderExecutionAttemptKind::RefundRecovery,
                OrderExecutionAttemptStatus::Failed,
            ),
        ],
    )
    .await;

    let resolved = resolve_latest_execution_attempt_with_deps(
        &deps,
        ResolveLatestExecutionAttemptInput {
            order_id: order_id.into(),
        },
    )
    .await
    .expect("resolve latest forward execution attempt");

    assert_eq!(resolved.attempt_id.inner(), forward_attempt);
}

#[tokio::test]
async fn resolve_latest_execution_attempt_uses_side_effectful_failed_refund_attempt() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let forward_attempt = Uuid::now_v7();
    let failed_refund_attempt = Uuid::now_v7();
    let order_id = seed_order_with_execution_attempt_specs(
        &test_db.pool,
        &[
            (
                forward_attempt,
                4,
                OrderExecutionAttemptKind::RefreshedExecution,
                OrderExecutionAttemptStatus::Failed,
            ),
            (
                failed_refund_attempt,
                5,
                OrderExecutionAttemptKind::RefundRecovery,
                OrderExecutionAttemptStatus::Failed,
            ),
        ],
    )
    .await;
    let now = Utc::now();
    let leg_id = Uuid::now_v7();
    let step_id = Uuid::now_v7();
    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_legs (
                id, order_id, execution_attempt_id, transition_decl_id,
                leg_index, leg_type, provider, status,
                input_chain_id, input_asset_id, output_chain_id, output_asset_id,
                amount_in, estimated_amount_out, completed_at,
                details_json, usd_valuation_json, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, 'refund-transition', 0, 'swap', 'hyperliquid',
                'completed', 'hyperliquid', 'UBTC', 'hyperliquid', 'native',
                '49965', '37585582', $4, '{}'::jsonb, '{}'::jsonb, $4, $4
            )
            "#,
    )
    .bind(leg_id)
    .bind(order_id)
    .bind(failed_refund_attempt)
    .bind(now)
    .execute(&test_db.pool)
    .await
    .expect("insert side-effectful refund leg");
    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_steps (
                id, order_id, execution_attempt_id, execution_leg_id,
                transition_decl_id, step_index, step_type, provider, status,
                input_chain_id, input_asset_id, output_chain_id, output_asset_id,
                amount_in, idempotency_key, attempt_count, started_at, completed_at,
                details_json, request_json, response_json, error_json,
                usd_valuation_json, created_at, updated_at
            )
            VALUES (
                $1, $2, $3, $4, 'refund-transition', 0, 'hyperliquid_trade',
                'hyperliquid', 'completed', 'hyperliquid', 'UBTC',
                'hyperliquid', 'native', '49965', 'refund-step', 1, $5, $5,
                '{"refund_kind":"transition_path"}'::jsonb,
                '{"amount_in":"49965"}'::jsonb,
                '{"observed_state":{"amount_out":"37585582"}}'::jsonb,
                '{}'::jsonb, '{}'::jsonb, $5, $5
            )
            "#,
    )
    .bind(step_id)
    .bind(order_id)
    .bind(failed_refund_attempt)
    .bind(leg_id)
    .bind(now)
    .execute(&test_db.pool)
    .await
    .expect("insert completed refund step");

    let resolved = resolve_latest_execution_attempt_with_deps(
        &deps,
        ResolveLatestExecutionAttemptInput {
            order_id: order_id.into(),
        },
    )
    .await
    .expect("resolve latest side-effectful refund attempt");

    assert_eq!(resolved.attempt_id.inner(), failed_refund_attempt);
}

#[tokio::test]
async fn hyperliquid_refund_materialization_creates_fresh_attempt_after_failed_refund() {
    let test_db = test_database().await;
    let planned_at = Utc::now();
    let source_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let mut order = test_order(source_usdc.clone(), planned_at);
    order.destination_asset = test_asset("bitcoin", "native");
    order.workflow_parent_span_id = order.workflow_trace_id[..16].to_string();
    let failed_attempt_id = Uuid::now_v7();
    let hyperliquid_vault =
        test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &source_usdc);
    seed_hyperliquid_spot_refund_order(
        &test_db.db,
        &test_db.pool,
        &order,
        failed_attempt_id,
        &hyperliquid_vault,
    )
    .await;

    let failed_refund_attempt_id = Uuid::now_v7();
    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_attempts (
                id, order_id, attempt_index, attempt_kind, status,
                failure_reason_json, input_custody_snapshot_json,
                created_at, updated_at
            )
            VALUES ($1, $2, 2, 'refund_recovery', 'failed',
                    '{"reason":"prior_refund_failed"}'::jsonb, '{}'::jsonb, $3, $3)
            "#,
    )
    .bind(failed_refund_attempt_id)
    .bind(order.id)
    .bind(planned_at)
    .execute(&test_db.pool)
    .await
    .expect("insert failed prior refund attempt");

    let leg_id = Uuid::now_v7();
    let leg = test_execution_leg(
        order.id,
        leg_id,
        0,
        source_usdc.clone(),
        source_usdc.clone(),
    );
    let step = test_execution_step(
        order.id,
        failed_refund_attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Planned,
    );
    let record = test_db
        .db
        .orders()
        .create_refund_attempt_from_hyperliquid_spot(
            order.id,
            failed_attempt_id,
            HyperliquidSpotRefundAttemptPlan {
                source_custody_vault_id: hyperliquid_vault.id,
                legs: vec![leg],
                steps: vec![step],
                failure_reason: json!({"reason": "retry_refund"}),
                input_custody_snapshot: json!({}),
            },
            planned_at,
        )
        .await
        .expect("materialize fresh Hyperliquid refund attempt");

    assert_eq!(record.attempt.attempt_index, 3);
    assert_ne!(record.attempt.id, failed_refund_attempt_id);
    assert_eq!(record.steps.len(), 1);
}

#[tokio::test]
async fn resolve_latest_execution_attempt_fails_cleanly_without_attempts() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let order_id = seed_order_with_execution_attempts(&test_db.pool, &[]).await;

    let error = resolve_latest_execution_attempt_with_deps(
        &deps,
        ResolveLatestExecutionAttemptInput {
            order_id: order_id.into(),
        },
    )
    .await
    .expect_err("an order with no execution attempts must fail cleanly");

    assert!(
        error
            .to_string()
            .contains("manual_refund_requires_execution_attempt"),
        "expected a clear no-attempts error, got: {error}"
    );
}

#[test]
fn refund_position_discovery_single_position_still_returns_position() {
    let discovery = refund_position_discovery_from_positions(
        Uuid::now_v7(),
        vec![test_refund_position(
            RecoverablePositionKind::ExternalCustody,
            "2500",
        )],
    );

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::ExternalCustody
            );
            assert_eq!(position.amount.as_str(), "2500");
        }
        other => panic!("expected single refund position, got {other:?}"),
    }
}

#[test]
fn refund_position_discovery_multiple_positions_selects_highest_priority() {
    let discovery = refund_position_discovery_from_positions(
        Uuid::now_v7(),
        vec![
            test_refund_position(RecoverablePositionKind::HyperliquidSpot, "100"),
            test_refund_position(RecoverablePositionKind::FundingVault, "50"),
        ],
    );

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(
                position.position_kind,
                RecoverablePositionKind::FundingVault
            );
            assert_eq!(position.amount.as_str(), "50");
        }
        other => panic!("expected best-priority refund position, got {other:?}"),
    }
}

#[test]
fn refund_position_discovery_prefers_expected_hyperliquid_asset_over_dust() {
    let order_id = Uuid::now_v7();
    let source_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let expected_ubtc = test_asset("hyperliquid", "UBTC");
    let mut usdc_dust = test_refund_position(RecoverablePositionKind::HyperliquidSpot, "463000");
    usdc_dust.asset = source_usdc.clone();
    usdc_dust.hyperliquid_coin = Some("USDC".to_string());
    usdc_dust.hyperliquid_canonical = Some(CanonicalAsset::Usdc);
    let mut ubtc_position = test_refund_position(RecoverablePositionKind::HyperliquidSpot, "49965");
    ubtc_position.asset = source_usdc;
    ubtc_position.hyperliquid_coin = Some("UBTC".to_string());
    ubtc_position.hyperliquid_canonical = Some(CanonicalAsset::Btc);

    let discovery = refund_position_discovery_from_positions_with_expected(
        order_id,
        vec![usdc_dust, ubtc_position],
        None,
        Some(&expected_ubtc),
        Some(CanonicalAsset::Btc),
    );

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(position.amount.as_str(), "49965");
            assert_eq!(position.hyperliquid_coin.as_deref(), Some("UBTC"));
            assert_eq!(position.hyperliquid_canonical, Some(CanonicalAsset::Btc));
        }
        other => panic!("expected expected-asset refund position, got {other:?}"),
    }
}

#[test]
fn refund_position_discovery_prefers_original_refund_asset_over_stale_expected_dust() {
    let order_id = Uuid::now_v7();
    let source_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let stale_expected_eth = test_asset("evm:1", "native");
    let mut eth_dust =
        test_refund_position(RecoverablePositionKind::ExternalCustody, "42544910985380");
    eth_dust.asset = stale_expected_eth.clone();
    let mut base_usdc = test_refund_position(RecoverablePositionKind::ExternalCustody, "37025459");
    base_usdc.asset = source_usdc.clone();

    let discovery = refund_position_discovery_from_positions_with_expected(
        order_id,
        vec![eth_dust, base_usdc],
        Some(&source_usdc),
        Some(&stale_expected_eth),
        None,
    );

    match discovery.outcome {
        SingleRefundPositionOutcome::Position(position) => {
            assert_eq!(position.asset, source_usdc);
            assert_eq!(position.amount.as_str(), "37025459");
        }
        other => panic!("expected original refund asset position, got {other:?}"),
    }
}

#[test]
fn refund_recovery_progress_asset_prefers_latest_completed_or_waiting_output() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let ueth = test_asset("hyperliquid", "UETH");
    let eth = test_asset("evm:1", "native");
    let base_eth = test_asset("evm:8453", "native");

    let mut completed_trade = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Completed,
    );
    completed_trade.output_asset = Some(ueth);
    let mut waiting_withdrawal = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        2,
        OrderExecutionStepStatus::Waiting,
    );
    waiting_withdrawal.output_asset = Some(eth.clone());
    let mut planned_next = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        3,
        OrderExecutionStepStatus::Planned,
    );
    planned_next.output_asset = Some(base_eth);

    assert_eq!(
        latest_refund_recovery_progress_asset(&[completed_trade, waiting_withdrawal, planned_next]),
        Some(eth)
    );
}

#[test]
fn completed_step_amount_out_extracts_cctp_claim_observation() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let mut step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Completed,
    );
    step.response = json!({
        "kind": "custody_actions",
        "claim_observation": {
            "amount_out_source": "cctp_attested_message",
            "claim_kind": "cctp_receive_claim",
            "amount": "37111765"
        }
    });

    assert_eq!(
        completed_step_amount_out(&step).as_deref(),
        Some("37111765")
    );
}

#[test]
fn completed_step_amount_out_prefers_balance_observation_over_provider_output() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let mut step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Completed,
    );
    step.response = json!({
        "amount_out": "50000",
        "observed_state": {
            "amount_out": "50000"
        },
        "balance_observation": {
            "output": {
                "spendable_balance": "49965"
            },
            "probes": [{
                "role": "destination",
                "spendable_balance": "49965"
            }]
        }
    });

    assert_eq!(completed_step_amount_out(&step).as_deref(), Some("49965"));
}

#[test]
fn completed_step_amount_out_ignores_provider_output_without_balance_observation() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let mut step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        1,
        OrderExecutionStepStatus::Completed,
    );
    step.response = json!({
        "amount_out": "50000",
        "observed_state": {
            "amount_out": "50000",
            "provider_observed_state": {
                "actual_amount_out": "50000"
            }
        },
        "provider_context": {
            "amount_out": "50000"
        }
    });

    assert_eq!(completed_step_amount_out(&step), None);
}

#[tokio::test]
async fn completed_step_without_balance_observation_strips_provider_amount_out() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let mut step = test_execution_step(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
        OrderExecutionStepStatus::Running,
    );
    step.step_type = OrderExecutionStepType::UniversalRouterSwap;
    step.provider = ProviderId::Velora.as_str().to_string();
    step.output_asset = Some(test_asset("evm:8453", "native"));
    step.request = json!({
        "recipient_address": "0x0000000000000000000000000000000000000001",
        "recipient_custody_vault_id": null
    });
    let completion = StepCompletion {
        response: json!({
            "amount_out": "50000",
            "observed_state": {
                "amount_out": "50000"
            }
        }),
        tx_hash: None,
        provider_state: ProviderExecutionState::default(),
        outcome: StepExecutionOutcome::Completed,
    };

    let completion = apply_authoritative_output_balance(&deps, &step, completion)
        .await
        .expect("sanitize completion without authoritative balance");

    assert!(completion.response.get("amount_out").is_none());
    assert_eq!(
        completion.response["provider_reported_output_amounts"]["amount_out"],
        json!("50000")
    );
    assert_eq!(
        completion.response["observed_state"]["amount_out"],
        json!("50000"),
        "venue output remains available for analysis"
    );
}

#[tokio::test]
async fn completed_cctp_burn_records_claim_observation_without_amount_out() {
    let test_db = test_database().await;
    let deps = test_deps(test_db.db.clone());
    let mut step = test_execution_step(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
        OrderExecutionStepStatus::Running,
    );
    step.step_type = OrderExecutionStepType::CctpBurn;
    step.provider = ProviderId::Cctp.as_str().to_string();
    step.output_asset = Some(test_asset(
        "evm:42161",
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    ));
    let completion = StepCompletion {
        response: json!({
            "messages": [{
                "decodedMessage": {
                    "decodedMessageBody": {
                        "amount": "37111765"
                    }
                }
            }],
            "amount_out": "37111765"
        }),
        tx_hash: None,
        provider_state: ProviderExecutionState::default(),
        outcome: StepExecutionOutcome::Completed,
    };

    let completion = apply_authoritative_output_balance(&deps, &step, completion)
        .await
        .expect("apply CCTP claim observation");

    assert!(completion.response.get("amount_out").is_none());
    assert_eq!(
        completion.response["claim_observation"]["amount"],
        json!("37111765")
    );
    assert_eq!(
        completed_step_amount_out(&OrderExecutionStep {
            response: completion.response,
            ..step
        })
        .as_deref(),
        Some("37111765")
    );
}

#[tokio::test]
async fn completed_hyperliquid_step_amount_out_uses_spendable_spot_balance() {
    let test_db = test_database().await;
    let hyperliquid_info =
        TestHyperliquidInfoServer::spawn_with_balances(vec![test_hyperliquid_spot_balance(
            "UBTC",
            2,
            "0.00050000",
            "0.00000035",
        )])
        .await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let now = Utc::now();
    let order = test_order(test_asset("evm:8453", "native"), now);
    let mut vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_btc);
    vault.order_id = None;
    test_db
        .db
        .orders()
        .create_custody_vault(&vault)
        .await
        .expect("insert Hyperliquid accounting vault");

    let mut step = test_execution_step(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
        OrderExecutionStepStatus::Running,
    );
    step.step_type = OrderExecutionStepType::HyperliquidTrade;
    step.provider = ProviderId::Hyperliquid.as_str().to_string();
    step.output_asset = Some(hl_btc.clone());
    step.request = json!({
        "hyperliquid_custody_vault_id": vault.id,
    });
    let completion = StepCompletion {
        response: json!({
            "kind": "custody_actions",
            "observed_state": {
                "amount_out": "50000"
            }
        }),
        tx_hash: None,
        provider_state: ProviderExecutionState::default(),
        outcome: StepExecutionOutcome::Completed,
    };

    let completion = apply_authoritative_output_balance(&deps, &step, completion)
        .await
        .expect("apply authoritative Hyperliquid output balance");

    assert_eq!(completion.response["amount_out"], json!("49965"));
    assert_eq!(
        completion.response["observed_state"]["amount_out"],
        json!("50000"),
        "venue output remains available for analysis"
    );
    assert_eq!(
        completion.response["balance_observation"]["output"]["spendable_balance"],
        json!("49965")
    );
}

#[tokio::test]
async fn completed_hyperliquid_bridge_deposit_amount_out_uses_clearinghouse_balance() {
    let test_db = test_database().await;
    let hyperliquid_info =
        TestHyperliquidInfoServer::spawn_with_balances_and_clearinghouse(vec![], "40").await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let deps = test_deps_with_action_providers(
        test_db.db.clone(),
        action_providers,
        Some(hyperliquid_info.base_url.clone()),
    );
    let source_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let hl_usdc = test_asset("hyperliquid", "native");
    let now = Utc::now();
    let order = test_order(source_usdc.clone(), now);
    let mut vault = test_custody_vault(&order, CustodyVaultRole::SourceDeposit, &source_usdc);
    vault.order_id = None;
    test_db
        .db
        .orders()
        .create_custody_vault(&vault)
        .await
        .expect("insert Hyperliquid bridge accounting vault");

    let mut step = test_execution_step(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        1,
        OrderExecutionStepStatus::Running,
    );
    step.step_type = OrderExecutionStepType::HyperliquidBridgeDeposit;
    step.provider = ProviderId::HyperliquidBridge.as_str().to_string();
    step.output_asset = Some(hl_usdc);
    step.request = json!({
        "source_custody_vault_id": vault.id,
    });
    let completion = StepCompletion {
        response: json!({
            "kind": "provider_only",
            "observed_state": {
                "amount_out": "39999999"
            }
        }),
        tx_hash: None,
        provider_state: ProviderExecutionState::default(),
        outcome: StepExecutionOutcome::Completed,
    };

    let completion = apply_authoritative_output_balance(&deps, &step, completion)
        .await
        .expect("apply authoritative Hyperliquid clearinghouse output balance");

    assert_eq!(completion.response["amount_out"], json!("40000000"));
    assert_eq!(
        completion.response["balance_observation"]["output"]["location"],
        json!("hyperliquid_clearinghouse")
    );
}

#[tokio::test]
async fn persisted_completion_rolls_up_balance_amount_out_to_leg() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    test_db
        .db
        .orders()
        .persist_execution_step_completion(PersistStepCompletionRecord {
            step_id: seeded.step_id,
            operation: None,
            addresses: Vec::new(),
            response: json!({
                "amount_out": "100",
                "observed_state": {
                    "amount_out": "100"
                },
                "balance_observation": {
                    "output": {
                        "spendable_balance": "88"
                    },
                    "probes": [{
                        "role": "destination",
                        "spendable_balance": "88"
                    }]
                }
            }),
            tx_hash: None,
            usd_valuation: json!({}),
            completed_at: Utc::now(),
        })
        .await
        .expect("persist completed step");

    let legs = test_db
        .db
        .orders()
        .get_execution_legs(seeded.order_id)
        .await
        .expect("load execution legs");
    assert_eq!(legs.len(), 1);
    assert_eq!(legs[0].actual_amount_out.as_deref(), Some("88"));
}

#[tokio::test]
async fn persisted_completion_does_not_roll_up_provider_or_estimated_amount_out() {
    let test_db = test_database().await;
    let seeded = seed_running_step(&test_db.pool, false, false).await;
    test_db
        .db
        .orders()
        .persist_execution_step_completion(PersistStepCompletionRecord {
            step_id: seeded.step_id,
            operation: None,
            addresses: Vec::new(),
            response: json!({
                "amount_out": "100",
                "observed_state": {
                    "amount_out": "100",
                    "provider_observed_state": {
                        "actual_amount_out": "100"
                    }
                },
                "provider_context": {
                    "amount_out": "100"
                }
            }),
            tx_hash: None,
            usd_valuation: json!({}),
            completed_at: Utc::now(),
        })
        .await
        .expect("persist completed step");

    let legs = test_db
        .db
        .orders()
        .get_execution_legs(seeded.order_id)
        .await
        .expect("load execution legs");
    assert_eq!(legs.len(), 1);
    assert_eq!(legs[0].actual_amount_out, None);
}

#[test]
fn cctp_receive_claim_position_detects_failed_receive_after_attested_burn() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let vault_id = Uuid::now_v7();
    let base_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let mut receive_step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        2,
        OrderExecutionStepStatus::Failed,
    );
    receive_step.step_type = OrderExecutionStepType::CctpReceive;
    receive_step.input_asset = Some(base_usdc.clone());
    receive_step.output_asset = Some(base_usdc.clone());
    receive_step.request = json!({
        "burn_transition_decl_id": "cctp",
        "amount": "37185782",
        "source_custody_vault_id": vault_id,
    });
    let now = Utc::now();
    let burn_operation = OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: Some(attempt_id),
        execution_step_id: None,
        provider: ProviderId::Cctp.as_str().to_string(),
        operation_type: ProviderOperationType::CctpBridge,
        provider_ref: Some("0xburn".to_string()),
        idempotency_key: None,
        status: ProviderOperationStatus::Completed,
        request: json!({
            "transition_decl_id": "cctp",
            "amount": "37111765",
        }),
        response: json!({}),
        observed_state: json!({
            "provider_observed_state": {
                "decoded_message_body": {
                    "amount": "37111765"
                }
            }
        }),
        created_at: now,
        updated_at: now,
    };

    let positions =
        cctp_receive_claim_positions_from_steps_and_operations(&[receive_step], &[burn_operation]);

    assert_eq!(positions.len(), 1);
    assert_eq!(
        positions[0].position_kind,
        RecoverablePositionKind::CctpReceiveClaim
    );
    assert_eq!(positions[0].amount.as_str(), "37111765");
    assert_eq!(positions[0].asset, base_usdc);
    assert_eq!(positions[0].custody_vault_id, Some(vault_id.into()));
}

#[test]
fn cctp_receive_existing_balance_completion_marks_operation_completed() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let vault_id = Uuid::now_v7();
    let asset = test_asset(
        "evm:999",
        "0xb88339cb7199b77e23db6e890353e22632ba630f",
    );
    let mut step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        2,
        OrderExecutionStepStatus::Running,
    );
    step.step_type = OrderExecutionStepType::CctpReceive;
    step.provider = ProviderId::Cctp.as_str().to_string();
    step.output_asset = Some(asset.clone());
    step.request = json!({ "amount": "40000000" });

    let state = ProviderExecutionState {
        operation: Some(ProviderOperationIntent {
            operation_type: ProviderOperationType::CctpReceive,
            status: ProviderOperationStatus::Submitted,
            provider_ref: None,
            idempotency_key: Some("receive-key".to_string()),
            request: Some(json!({ "amount": "40000000" })),
            response: None,
            observed_state: None,
        }),
        addresses: Vec::new(),
    };
    let balance = AuthoritativeOutputBalance {
        amount: "40000000".to_string(),
        location: "custody_vault",
        custody_vault_id: Some(vault_id),
        address: Some(test_address(4)),
        chain_id: asset.chain.to_string(),
        asset_id: asset.asset.to_string(),
        details: json!({}),
    };

    let completion = cctp_receive_existing_balance_completion(
        &step,
        vault_id,
        &CustodyAction::Transfer {
            to_address: test_address(5),
            amount: "40000000".to_string(),
            bitcoin_fee_budget_sats: None,
        },
        &json!({}),
        state,
        &balance,
        "execution reverted: Nonce already used",
    );

    assert_eq!(completion.outcome, StepExecutionOutcome::Completed);
    assert_eq!(completion.tx_hash, None);
    let operation = completion
        .provider_state
        .operation
        .expect("provider operation");
    assert_eq!(operation.status, ProviderOperationStatus::Completed);
    assert!(operation.provider_ref.as_deref().is_some_and(|provider_ref| {
        provider_ref.starts_with("cctp_receive_already_claimed:")
    }));
    assert_eq!(
        operation
            .observed_state
            .as_ref()
            .and_then(|state| state.get("balance"))
            .and_then(Value::as_str),
        Some("40000000")
    );
}

#[test]
fn hyperliquid_refund_balance_parser_floors_overprecision_decimal_dust() {
    let amount = hyperliquid_refund_balance_amount_raw("0.50688167", "0.0", 6)
        .expect("over-precision Hyperliquid USDC dust should parse");
    assert_eq!(amount.as_deref(), Some("506881"));

    let sub_raw_unit = hyperliquid_refund_balance_amount_raw("0.00000067", "0", 6)
        .expect("sub-raw-unit Hyperliquid dust should parse");
    assert_eq!(sub_raw_unit, None);
}

#[test]
fn refund_step_idempotency_key_includes_attempt_id() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let mut step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        0,
        OrderExecutionStepStatus::Planned,
    );
    step.details = json!({ "refund_kind": "transition_path" });

    let key = step_idempotency_key(&step, ProviderOperationType::HyperliquidTrade);

    assert_eq!(
        key,
        format!("order:{order_id}:attempt:{attempt_id}:hyperliquid_trade:step:0")
    );
}

#[test]
fn non_refund_step_idempotency_key_preserves_retry_scope() {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let step = test_execution_step(
        order_id,
        attempt_id,
        leg_id,
        2,
        OrderExecutionStepStatus::Planned,
    );

    let key = step_idempotency_key(&step, ProviderOperationType::UniversalRouterSwap);

    assert_eq!(
        key,
        format!("order:{order_id}:universal_router_swap:step:2")
    );
}

#[test]
fn unit_withdrawal_builder_accepts_all_hyperliquid_binding_flavors() {
    let planned_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let order = test_order(btc.clone(), planned_at);
    let vault = test_custody_vault(&order, CustodyVaultRole::HyperliquidSpot, &hl_btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let leg = quote_leg_for_transition(&withdrawal, "30000", "30000", planned_at);

    let explicit = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::Explicit {
            vault_id: vault.id,
            address: vault.address.clone(),
            role: CustodyVaultRole::HyperliquidSpot,
            asset: Some(hl_btc),
        },
        true,
        0,
        planned_at,
    )
    .expect("explicit UnitWithdrawal");
    assert_eq!(
        explicit.request.get("hyperliquid_custody_vault_id"),
        Some(&json!(vault.id))
    );
    assert_eq!(
        explicit
            .details
            .get("hyperliquid_custody_vault_status")
            .and_then(Value::as_str),
        Some("bound")
    );

    let derived_spot = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::DerivedSpot,
        false,
        1,
        planned_at,
    )
    .expect("derived spot UnitWithdrawal");
    assert_eq!(
        derived_spot.request.get("hyperliquid_custody_vault_id"),
        Some(&Value::Null)
    );
    assert_eq!(
        derived_spot
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::HyperliquidSpot.to_db_string())
    );
    assert_eq!(
        derived_spot
            .request
            .get("recipient_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );

    let derived_destination = refund_transition_unit_withdrawal_step(
        &order,
        &withdrawal,
        &leg,
        &RefundHyperliquidBinding::DerivedDestinationExecution { asset: btc },
        false,
        2,
        planned_at,
    )
    .expect("derived destination UnitWithdrawal");
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_role")
            .and_then(Value::as_str),
        Some(CustodyVaultRole::DestinationExecution.to_db_string())
    );
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_chain_id")
            .and_then(Value::as_str),
        Some("bitcoin")
    );
    assert_eq!(
        derived_destination
            .request
            .get("hyperliquid_custody_vault_asset_id")
            .and_then(Value::as_str),
        Some("native")
    );
}

#[test]
fn unit_refund_compat_gate_accepts_external_deposit_and_hyperliquid_withdrawal() {
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &TransitionPath {
            id: "unit-deposit>unit-withdrawal".to_string(),
            transitions: vec![deposit, withdrawal.clone()],
        },
    ));
    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::HyperliquidSpot,
        None,
        &TransitionPath {
            id: "unit-withdrawal".to_string(),
            transitions: vec![withdrawal.clone()],
        },
    ));
    assert!(!refund_path_compatible_with_position(
        RecoverablePositionKind::ExternalCustody,
        Some(CustodyVaultRole::SourceDeposit),
        &TransitionPath {
            id: "unit-withdrawal".to_string(),
            transitions: vec![withdrawal],
        },
    ));
}

#[test]
fn hyperliquid_spot_compat_gate_accepts_trade_first_multi_hop_path() {
    let btc = test_asset("bitcoin", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let eth_to_usdc = hyperliquid_trade_transition(
        hl_eth,
        hl_usdc.clone(),
        CanonicalAsset::Eth,
        CanonicalAsset::Usdc,
    );
    let usdc_to_btc = hyperliquid_trade_transition(
        hl_usdc,
        hl_btc.clone(),
        CanonicalAsset::Usdc,
        CanonicalAsset::Btc,
    );
    let withdrawal = unit_withdrawal_transition(hl_btc, btc, CanonicalAsset::Btc);

    assert!(refund_path_compatible_with_position(
        RecoverablePositionKind::HyperliquidSpot,
        None,
        &TransitionPath {
            id: "hl-eth-to-usdc>hl-usdc-to-btc>unit-withdrawal".to_string(),
            transitions: vec![eth_to_usdc, usdc_to_btc, withdrawal],
        },
    ));
}

#[test]
fn refresh_cctp_quote_legs_materialize_burn_and_receive() {
    let expires_at = Utc::now();
    let base_usdc = test_asset("evm:8453", "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913");
    let arbitrum_usdc = test_asset("evm:42161", "0xaf88d065e77c8cc2239327c5edb3a432268e5831");
    let transition = cctp_transition(base_usdc.clone(), arbitrum_usdc.clone());
    let quote = BridgeQuote {
        provider_id: "cctp".to_string(),
        amount_in: "1000000".to_string(),
        amount_out: "999000".to_string(),
        provider_quote: json!({ "message_hash": "0xcctp" }),
        expires_at,
    };

    let legs = refresh_cctp_quote_transition_legs(&transition, &quote);

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].transition_decl_id, "cctp");
    assert_eq!(legs[0].parent_transition_id(), "cctp");
    assert_eq!(
        legs[0].execution_step_type,
        OrderExecutionStepType::CctpBurn
    );
    assert_eq!(
        legs[0].input_asset,
        QuoteLegAsset::from_deposit_asset(&base_usdc)
    );
    assert_eq!(
        legs[0].output_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(legs[0].amount_in, "1000000");
    assert_eq!(legs[0].amount_out, "999000");
    assert_eq!(
        legs[0]
            .raw
            .get("execution_step_type")
            .and_then(Value::as_str),
        Some(OrderExecutionStepType::CctpBurn.to_db_string())
    );

    assert_eq!(legs[1].transition_decl_id, "cctp:receive");
    assert_eq!(legs[1].parent_transition_id(), "cctp");
    assert_eq!(
        legs[1].execution_step_type,
        OrderExecutionStepType::CctpReceive
    );
    assert_eq!(
        legs[1].input_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(
        legs[1].output_asset,
        QuoteLegAsset::from_deposit_asset(&arbitrum_usdc)
    );
    assert_eq!(legs[1].amount_in, "999000");
    assert_eq!(legs[1].amount_out, "999000");
    assert_eq!(
        legs[1].raw.get("kind").and_then(Value::as_str),
        Some(OrderExecutionStepType::CctpReceive.to_db_string())
    );
    assert_eq!(
        legs[1].raw.get("bridge_kind").and_then(Value::as_str),
        Some("cctp_bridge")
    );
}

#[test]
fn refresh_spot_cross_token_quote_legs_parse_hyperliquid_legs() {
    let expires_at = Utc::now();
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let hl_usdc = test_asset("hyperliquid", "native");
    let hl_eth = test_asset("hyperliquid", "UETH");
    let quote = ExchangeQuote {
        provider_id: "hyperliquid".to_string(),
        amount_in: "30000".to_string(),
        amount_out: "40000000000000000".to_string(),
        min_amount_out: Some("1".to_string()),
        max_amount_in: None,
        provider_quote: json!({
            "kind": "spot_cross_token",
            "legs": [
                {
                    "input_asset": QuoteLegAsset::from_deposit_asset(&hl_btc),
                    "output_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                    "amount_in": "30000",
                    "amount_out": "20000000"
                },
                {
                    "input_asset": QuoteLegAsset::from_deposit_asset(&hl_usdc),
                    "output_asset": QuoteLegAsset::from_deposit_asset(&hl_eth),
                    "amount_in": "20000000",
                    "amount_out": "40000000000000000"
                }
            ]
        }),
        expires_at,
    };

    let legs = refresh_spot_cross_token_quote_transition_legs(
        "hl-trade",
        MarketOrderTransitionKind::HyperliquidTrade,
        ProviderId::Hyperliquid,
        &quote,
    )
    .expect("parse spot_cross_token legs");

    assert_eq!(legs.len(), 2);
    assert_eq!(legs[0].transition_decl_id, "hl-trade:leg:0");
    assert_eq!(legs[0].parent_transition_id(), "hl-trade");
    assert_eq!(
        legs[0].execution_step_type,
        OrderExecutionStepType::HyperliquidTrade
    );
    assert_eq!(
        legs[0].input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(
        legs[0].output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_usdc)
    );
    assert_eq!(legs[0].amount_in, "30000");
    assert_eq!(legs[0].amount_out, "20000000");
    assert_eq!(legs[1].transition_decl_id, "hl-trade:leg:1");
    assert_eq!(legs[1].parent_transition_id(), "hl-trade");
    assert_eq!(
        legs[1].input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_usdc)
    );
    assert_eq!(
        legs[1].output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_eth)
    );
    assert_eq!(legs[1].amount_in, "20000000");
    assert_eq!(legs[1].amount_out, "40000000000000000");
}

#[test]
fn refresh_unit_quote_legs_are_passthrough_shapes() {
    let expires_at = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let deposit = unit_deposit_transition(btc.clone(), hl_btc.clone(), CanonicalAsset::Btc);
    let withdrawal = unit_withdrawal_transition(hl_btc.clone(), btc.clone(), CanonicalAsset::Btc);
    let reserve_quote = refresh_bitcoin_fee_reserve_quote(U256::from(2500_u64));

    let deposit_leg =
        refresh_unit_deposit_quote_leg(&deposit, "30000", expires_at, reserve_quote.clone());
    assert_eq!(
        deposit_leg.execution_step_type,
        OrderExecutionStepType::UnitDeposit
    );
    assert_eq!(
        deposit_leg.input_asset,
        QuoteLegAsset::from_deposit_asset(&btc)
    );
    assert_eq!(
        deposit_leg.output_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(deposit_leg.amount_in, "30000");
    assert_eq!(deposit_leg.amount_out, "30000");
    assert_eq!(deposit_leg.raw, reserve_quote);
    assert_eq!(
        deposit_leg
            .raw
            .pointer("/source_fee_reserve/reserve_bps")
            .and_then(Value::as_u64),
        Some(REFRESH_BITCOIN_UNIT_DEPOSIT_FEE_RESERVE_BPS)
    );

    let withdrawal_leg =
        refresh_unit_withdrawal_quote_leg(&withdrawal, "30000", "0xrecipient", expires_at);
    assert_eq!(
        withdrawal_leg.execution_step_type,
        OrderExecutionStepType::UnitWithdrawal
    );
    assert_eq!(
        withdrawal_leg.input_asset,
        QuoteLegAsset::from_deposit_asset(&hl_btc)
    );
    assert_eq!(
        withdrawal_leg.output_asset,
        QuoteLegAsset::from_deposit_asset(&btc)
    );
    assert_eq!(withdrawal_leg.amount_in, "30000");
    assert_eq!(withdrawal_leg.amount_out, "30000");
    assert_eq!(
        withdrawal_leg
            .raw
            .get("recipient_address")
            .and_then(Value::as_str),
        Some("0xrecipient")
    );
    assert_eq!(
        withdrawal_leg
            .raw
            .pointer("/hyperliquid_core_activation_fee/amount_raw")
            .and_then(Value::as_str),
        Some(
            REFRESH_HYPERLIQUID_SPOT_SEND_QUOTE_GAS_RESERVE_RAW
                .to_string()
                .as_str()
        )
    );
}

#[test]
fn refresh_unit_deposit_fee_reserve_reads_original_quote_leg() {
    let now = Utc::now();
    let btc = test_asset("bitcoin", "native");
    let hl_btc = test_asset("hyperliquid", "UBTC");
    let transition = unit_deposit_transition(btc.clone(), hl_btc, CanonicalAsset::Btc);
    let fee_reserve = U256::from(42_000_u64);
    let leg = refresh_unit_deposit_quote_leg(
        &transition,
        "100000",
        now,
        refresh_bitcoin_fee_reserve_quote(fee_reserve),
    );
    let quote = test_market_order_quote(
        btc,
        transition.output.asset.clone(),
        vec![leg],
        now + chrono::Duration::minutes(5),
        now,
    );

    assert_eq!(
        refresh_unit_deposit_fee_reserve(&quote, &transition).expect("read fee reserve"),
        Some(fee_reserve)
    );

    let mut no_reserve_quote = quote.clone();
    no_reserve_quote.provider_quote = json!({ "legs": [] });
    assert_eq!(
        refresh_unit_deposit_fee_reserve(&no_reserve_quote, &transition)
            .expect("missing fee reserve is allowed"),
        None
    );
}

#[test]
fn refresh_hyperliquid_spot_send_reserve_math_matches_legacy() {
    assert_eq!(
        refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000001")
            .expect("subtract reserve"),
        "1"
    );
    assert!(
        refresh_reserve_hyperliquid_spot_send_quote_gas("amount_in", "1000000").is_err(),
        "amount must strictly exceed the reserve"
    );
}

async fn test_database() -> TestDatabase {
    let image = GenericImage::new("postgres", "18-alpine")
        .with_exposed_port(POSTGRES_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DATABASE)
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=10",
            "-c",
            "max_replication_slots=10",
        ]);

    let container = image.start().await.expect("start Postgres testcontainer");
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT.tcp())
        .await
        .expect("read Postgres testcontainer port");
    let database_url = format!(
        "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DATABASE}"
    );
    let db = Database::connect(&database_url, 4, 1)
        .await
        .expect("connect migrated test database");
    let pool = PgPool::connect(&database_url)
        .await
        .expect("connect raw test pool");

    TestDatabase {
        _container: container,
        db,
        pool,
    }
}

fn test_deps(db: Database) -> OrderActivityDeps {
    let action_providers = Arc::new(
        ActionProviderRegistry::http_from_options(ActionProviderHttpOptions {
            across: None,
            cctp: None,
            hyperunit_base_url: None,
            hyperunit_proxy_url: None,
            hyperliquid_base_url: None,
            velora: None,
            hyperliquid_network: HyperliquidCallNetwork::Testnet,
            hyperliquid_order_timeout_ms: 30_000,
            hypercore_bridge_enabled: false,
        })
        .expect("empty action provider registry"),
    );
    test_deps_with_action_providers(db, action_providers, None)
}

fn test_deps_with_action_providers(
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
    hyperliquid_base_url: Option<String>,
) -> OrderActivityDeps {
    test_deps_with_action_providers_and_chain_registry(
        db,
        action_providers,
        hyperliquid_base_url,
        Arc::new(ChainRegistry::new()),
    )
}

fn test_deps_with_action_providers_and_chain_registry(
    db: Database,
    action_providers: Arc<ActionProviderRegistry>,
    hyperliquid_base_url: Option<String>,
    chain_registry: Arc<ChainRegistry>,
) -> OrderActivityDeps {
    let settings = Arc::new(test_settings());
    let custody_executor = CustodyActionExecutor::new(db.clone(), settings, chain_registry.clone())
        .with_hyperliquid_runtime(hyperliquid_base_url.map(|base_url| {
            HyperliquidRuntimeConfig::new(base_url, HyperliquidCallNetwork::Testnet)
        }));
    let custody_executor = Arc::new(custody_executor);
    let pricing_provider = Arc::new(router_core::services::RouteCostService::new(
        db.clone(),
        action_providers.clone(),
    ));
    OrderActivityDeps::new(
        db,
        action_providers,
        custody_executor,
        chain_registry,
        pricing_provider,
    )
}

async fn test_deps_with_arbitrum_evm_balance(
    db: Database,
    hyperliquid_base_url: String,
    raw_evm_balance: &str,
) -> (OrderActivityDeps, TestEvmRpcServer) {
    let evm_rpc = TestEvmRpcServer::spawn_with_balance(raw_evm_balance).await;
    let action_providers = Arc::new(ActionProviderRegistry::new(vec![], vec![], vec![]));
    let mut chain_registry = ChainRegistry::new();
    let evm_chain = Arc::new(
        EvmChain::new(
            &evm_rpc.base_url,
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            ChainType::Arbitrum,
            b"arbitrum-wallet",
            1,
            std::time::Duration::from_secs(1),
        )
        .await
        .expect("construct test EVM chain"),
    );
    chain_registry.register_evm(ChainType::Arbitrum, evm_chain);
    let deps = test_deps_with_action_providers_and_chain_registry(
        db,
        action_providers,
        Some(hyperliquid_base_url),
        Arc::new(chain_registry),
    );
    (deps, evm_rpc)
}

fn test_settings() -> Settings {
    let dir = tempfile::tempdir().expect("settings tempdir");
    let path = dir.path().join("router-master-key.hex");
    std::fs::write(&path, alloy::hex::encode([0x42_u8; 64])).expect("write router master key");
    Settings::load(&path).expect("load test settings")
}

fn test_asset(chain: &str, asset: &str) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid test chain"),
        asset: AssetId::parse(asset).expect("valid test asset"),
    }
}

fn test_address(byte: u8) -> String {
    format!(
        "0x{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}{byte:02x}"
    )
}

fn test_order(source_asset: DepositAsset, now: chrono::DateTime<Utc>) -> RouterOrder {
    RouterOrder {
        id: Uuid::now_v7(),
        order_type: RouterOrderType::MarketOrder,
        status: RouterOrderStatus::Refunding,
        funding_vault_id: None,
        source_asset: source_asset.clone(),
        destination_asset: source_asset,
        recipient_address: test_address(9),
        refund_address: test_address(8),
        action: RouterOrderAction::MarketOrder(MarketOrderAction {
            amount_in: "150000000".to_string(),
        }),
        idempotency_key: None,
        workflow_trace_id: Uuid::now_v7().simple().to_string(),
        workflow_parent_span_id: "0000000000000000".to_string(),
        created_at: now,
        updated_at: now,
    }
}

fn test_custody_vault(
    order: &RouterOrder,
    role: CustodyVaultRole,
    asset: &DepositAsset,
) -> CustodyVault {
    CustodyVault {
        id: Uuid::now_v7(),
        order_id: Some(order.id),
        role,
        visibility: CustodyVaultVisibility::Internal,
        chain: asset.chain.clone(),
        asset: Some(asset.asset.clone()),
        address: test_address(7),
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: None,
        signer_ref: None,
        status: CustodyVaultStatus::Active,
        metadata: json!({}),
        created_at: order.created_at,
        updated_at: order.created_at,
    }
}

fn test_refund_position(
    position_kind: RecoverablePositionKind,
    amount: &str,
) -> SingleRefundPosition {
    let vault_id = Uuid::now_v7();
    let asset = match position_kind {
        RecoverablePositionKind::HyperliquidSpot => test_asset("hyperliquid", "UBTC"),
        RecoverablePositionKind::CctpReceiveClaim
        | RecoverablePositionKind::FundingVault
        | RecoverablePositionKind::ExternalCustody => test_asset("bitcoin", "native"),
    };

    SingleRefundPosition {
        position_kind,
        owning_step_id: None,
        funding_vault_id: (position_kind == RecoverablePositionKind::FundingVault)
            .then_some(vault_id.into()),
        custody_vault_id: (position_kind != RecoverablePositionKind::FundingVault)
            .then_some(vault_id.into()),
        asset,
        amount: RawAmount::new(amount).expect("valid raw amount"),
        hyperliquid_coin: (position_kind == RecoverablePositionKind::HyperliquidSpot)
            .then_some("UBTC".to_string()),
        hyperliquid_canonical: (position_kind == RecoverablePositionKind::HyperliquidSpot)
            .then_some(CanonicalAsset::Btc),
        requires_clearinghouse_unwrap: false,
    }
}

fn hyperliquid_bridge_deposit_transition(
    input: DepositAsset,
    output: DepositAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "hl-deposit".to_string(),
        kind: MarketOrderTransitionKind::HyperliquidBridgeDeposit,
        provider: ProviderId::HyperliquidBridge,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: CanonicalAsset::Usdc,
        },
    }
}

fn hyperliquid_bridge_withdrawal_transition(
    input: DepositAsset,
    output: DepositAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "hl-withdrawal".to_string(),
        kind: MarketOrderTransitionKind::HyperliquidBridgeWithdrawal,
        provider: ProviderId::HyperliquidBridge,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: CanonicalAsset::Usdc,
        },
        to: MarketOrderNode::External(output),
    }
}

fn hyperliquid_trade_transition(
    input: DepositAsset,
    output: DepositAsset,
    input_canonical: CanonicalAsset,
    output_canonical: CanonicalAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: format!(
            "hl-trade-{}-to-{}",
            input_canonical.as_str(),
            output_canonical.as_str()
        ),
        kind: MarketOrderTransitionKind::HyperliquidTrade,
        provider: ProviderId::Hyperliquid,
        input: AssetSlot {
            asset: input,
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        output: AssetSlot {
            asset: output,
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: input_canonical,
        },
        to: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical: output_canonical,
        },
    }
}

fn unit_deposit_transition(
    input: DepositAsset,
    output: DepositAsset,
    canonical: CanonicalAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "unit-deposit".to_string(),
        kind: MarketOrderTransitionKind::UnitDeposit,
        provider: ProviderId::Unit,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        output: AssetSlot {
            asset: output,
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical,
        },
    }
}

fn unit_withdrawal_transition(
    input: DepositAsset,
    output: DepositAsset,
    canonical: CanonicalAsset,
) -> TransitionDecl {
    TransitionDecl {
        id: "unit-withdrawal".to_string(),
        kind: MarketOrderTransitionKind::UnitWithdrawal,
        provider: ProviderId::Unit,
        input: AssetSlot {
            asset: input,
            required_custody_role: RequiredCustodyRole::HyperliquidSpot,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::Venue {
            provider: ProviderId::Hyperliquid,
            canonical,
        },
        to: MarketOrderNode::External(output),
    }
}

fn cctp_transition(input: DepositAsset, output: DepositAsset) -> TransitionDecl {
    TransitionDecl {
        id: "cctp".to_string(),
        kind: MarketOrderTransitionKind::CctpBridge,
        provider: ProviderId::Cctp,
        input: AssetSlot {
            asset: input.clone(),
            required_custody_role: RequiredCustodyRole::SourceOrIntermediate,
        },
        output: AssetSlot {
            asset: output.clone(),
            required_custody_role: RequiredCustodyRole::IntermediateExecution,
        },
        from: MarketOrderNode::External(input),
        to: MarketOrderNode::External(output),
    }
}

fn quote_leg_for_transition(
    transition: &TransitionDecl,
    amount_in: &str,
    amount_out: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
            transition_decl_id: &transition.id,
            transition_kind: transition.kind,
            provider: transition.provider,
            input_asset: &transition.input.asset,
            output_asset: &transition.output.asset,
            amount_in,
            amount_out,
            expires_at,
            raw: json!({
                "kind": match transition.kind {
                    MarketOrderTransitionKind::HyperliquidBridgeDeposit => "hyperliquid_native_bridge",
                    MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
                    _ => transition.kind.as_str(),
                },
            }),
        })
        .with_execution_step_type(execution_step_type_for_transition_kind(
            transition.kind,
        ))
}

fn hyperliquid_trade_quote_leg(
    transition: &TransitionDecl,
    amount_in: &str,
    amount_out: &str,
    expires_at: chrono::DateTime<Utc>,
) -> QuoteLeg {
    QuoteLeg::new(QuoteLegSpec {
        transition_decl_id: &transition.id,
        transition_kind: transition.kind,
        provider: transition.provider,
        input_asset: &transition.input.asset,
        output_asset: &transition.output.asset,
        amount_in,
        amount_out,
        expires_at,
        raw: json!({
            "kind": "spot_cross_token",
            "order_kind": "exact_in",
            "min_amount_out": "1",
            "input_asset": QuoteLegAsset::from_deposit_asset(&transition.input.asset),
            "output_asset": QuoteLegAsset::from_deposit_asset(&transition.output.asset),
            "amount_in": amount_in,
            "amount_out": amount_out,
        }),
    })
}

fn test_market_order_quote(
    source_asset: DepositAsset,
    destination_asset: DepositAsset,
    legs: Vec<QuoteLeg>,
    expires_at: chrono::DateTime<Utc>,
    created_at: chrono::DateTime<Utc>,
) -> MarketOrderQuote {
    MarketOrderQuote {
        id: Uuid::now_v7(),
        order_id: None,
        source_asset,
        destination_asset,
        recipient_address: test_address(1),
        provider_id: "path:test".to_string(),
        amount_in: "100000".to_string(),
        estimated_amount_out: "100000".to_string(),
        provider_quote: json!({ "legs": legs }),
        usd_valuation: json!({}),
        expires_at,
        created_at,
    }
}

fn test_execution_step(
    order_id: Uuid,
    attempt_id: Uuid,
    leg_id: Uuid,
    step_index: i32,
    status: OrderExecutionStepStatus,
) -> OrderExecutionStep {
    let now = Utc::now();
    OrderExecutionStep {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: Some(attempt_id),
        execution_leg_id: Some(leg_id),
        transition_decl_id: Some(format!("test-transition-{step_index}")),
        step_index,
        step_type: OrderExecutionStepType::UniversalRouterSwap,
        provider: ProviderId::Velora.as_str().to_string(),
        status,
        input_asset: None,
        output_asset: None,
        amount_in: Some("1000".to_string()),
        tx_hash: None,
        provider_ref: None,
        idempotency_key: Some(format!("test-step-{step_index}")),
        attempt_count: 0,
        next_attempt_at: None,
        started_at: Some(now),
        completed_at: if status == OrderExecutionStepStatus::Completed {
            Some(now)
        } else {
            None
        },
        details: json!({}),
        request: json!({}),
        response: json!({}),
        error: json!({}),
        usd_valuation: json!({}),
        created_at: now,
        updated_at: now,
    }
}

fn test_execution_leg(
    order_id: Uuid,
    leg_id: Uuid,
    leg_index: i32,
    input_asset: DepositAsset,
    output_asset: DepositAsset,
) -> OrderExecutionLeg {
    let now = Utc::now();
    OrderExecutionLeg {
        id: leg_id,
        order_id,
        execution_attempt_id: None,
        transition_decl_id: Some(format!("test-transition-{leg_index}")),
        leg_index,
        leg_type: "swap".to_string(),
        provider: ProviderId::Velora.as_str().to_string(),
        status: OrderExecutionStepStatus::Planned,
        input_asset,
        output_asset,
        amount_in: "1000".to_string(),
        estimated_amount_out: "1000".to_string(),
        actual_amount_in: None,
        actual_amount_out: None,
        started_at: None,
        completed_at: None,
        provider_quote_expires_at: None,
        details: json!({}),
        usd_valuation: json!({}),
        created_at: now,
        updated_at: now,
    }
}

fn test_provider_operation(
    order_id: Uuid,
    attempt_id: Uuid,
    step_id: Uuid,
) -> OrderProviderOperation {
    let now = Utc::now();
    OrderProviderOperation {
        id: Uuid::now_v7(),
        order_id,
        execution_attempt_id: Some(attempt_id),
        execution_step_id: Some(step_id),
        provider: ProviderId::Velora.as_str().to_string(),
        operation_type: ProviderOperationType::UniversalRouterSwap,
        provider_ref: Some("test-provider-ref".to_string()),
        idempotency_key: None,
        status: ProviderOperationStatus::Submitted,
        request: json!({}),
        response: json!({}),
        observed_state: json!({}),
        created_at: now,
        updated_at: now,
    }
}

async fn seed_hyperliquid_spot_refund_order(
    db: &Database,
    pool: &PgPool,
    order: &RouterOrder,
    failed_attempt_id: Uuid,
    vault: &CustodyVault,
) {
    let RouterOrderAction::MarketOrder(action) = &order.action else {
        panic!("test refund order must be a market order")
    };
    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                funding_vault_id,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                idempotency_key,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
    )
    .bind(order.id)
    .bind(order.order_type.to_db_string())
    .bind(order.status.to_db_string())
    .bind(order.funding_vault_id)
    .bind(order.source_asset.chain.as_str())
    .bind(order.source_asset.asset.as_str())
    .bind(order.destination_asset.chain.as_str())
    .bind(order.destination_asset.asset.as_str())
    .bind(&order.recipient_address)
    .bind(&order.refund_address)
    .bind(order.idempotency_key.clone())
    .bind(&order.workflow_trace_id)
    .bind(&order.workflow_parent_span_id)
    .bind(order.created_at)
    .bind(order.updated_at)
    .execute(pool)
    .await
    .expect("insert refund test order");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (
                order_id,
                amount_in,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $3)
            "#,
    )
    .bind(order.id)
    .bind(action.amount_in.as_str())
    .bind(order.created_at)
    .execute(pool)
    .await
    .expect("insert refund test market action");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_attempts (
                id,
                order_id,
                attempt_index,
                attempt_kind,
                status,
                failure_reason_json,
                input_custody_snapshot_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, 1, $3, $4, '{}'::jsonb, '{}'::jsonb, $5, $5)
            "#,
    )
    .bind(failed_attempt_id)
    .bind(order.id)
    .bind(OrderExecutionAttemptKind::PrimaryExecution.to_db_string())
    .bind(OrderExecutionAttemptStatus::Failed.to_db_string())
    .bind(order.created_at)
    .execute(pool)
    .await
    .expect("insert refund test failed attempt");

    db.orders()
        .create_custody_vault(vault)
        .await
        .expect("insert refund test HyperliquidSpot custody vault");
}

async fn seed_running_step(
    pool: &PgPool,
    with_checkpoint: bool,
    with_durable_provider_operation: bool,
) -> SeededRunningStep {
    let order_id = Uuid::now_v7();
    let attempt_id = Uuid::now_v7();
    let leg_id = Uuid::now_v7();
    let step_id = Uuid::now_v7();
    let now = Utc::now();
    let started_at = now - chrono::Duration::minutes(10);
    let trace_id = order_id.simple().to_string();
    let parent_span_id = trace_id[..16].to_string();
    let details = if with_checkpoint {
        json!({
            "provider_side_effect_checkpoint": {
                "kind": "provider_side_effect_about_to_fire",
                "reason": "about_to_fire_provider_side_effect",
                "recorded_at": started_at.to_rfc3339(),
                "scar_tissue": "§6"
            }
        })
    } else {
        json!({})
    };

    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, 'market_order', 'executing', 'evm:8453', 'native',
                'evm:8453', 'native', '0x0000000000000000000000000000000000000001',
                '0x0000000000000000000000000000000000000002',
                $2, $3, $4, $4
            )
            "#,
    )
    .bind(order_id)
    .bind(trace_id)
    .bind(parent_span_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert router order");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (
                order_id,
                amount_in,
                created_at,
                updated_at
            )
            VALUES ($1, '100', $2, $2)
            "#,
    )
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert market order action");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_attempts (
                id,
                order_id,
                attempt_index,
                attempt_kind,
                status,
                failure_reason_json,
                input_custody_snapshot_json,
                created_at,
                updated_at
            )
            VALUES ($1, $2, 1, 'primary_execution', 'active', '{}'::jsonb, '{}'::jsonb, $3, $3)
            "#,
    )
    .bind(attempt_id)
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution attempt");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_legs (
                id,
                order_id,
                execution_attempt_id,
                transition_decl_id,
                leg_index,
                leg_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                estimated_amount_out,
                started_at,
                details_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, 'test-transition', 0, 'swap', 'velora',
                'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', '100', $4, '{}'::jsonb, '{}'::jsonb, $5, $5
            )
            "#,
    )
    .bind(leg_id)
    .bind(order_id)
    .bind(attempt_id)
    .bind(started_at)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution leg");

    sqlx_core::query::query(
        r#"
            INSERT INTO order_execution_steps (
                id,
                order_id,
                execution_attempt_id,
                execution_leg_id,
                transition_decl_id,
                step_index,
                step_type,
                provider,
                status,
                input_chain_id,
                input_asset_id,
                output_chain_id,
                output_asset_id,
                amount_in,
                idempotency_key,
                attempt_count,
                started_at,
                details_json,
                request_json,
                response_json,
                error_json,
                usd_valuation_json,
                created_at,
                updated_at
            )
            VALUES (
                $1, $2, $3, $4, 'test-transition', 0, 'universal_router_swap',
                'velora', 'running', 'evm:8453', 'native', 'evm:8453', 'native',
                '100', $5, 1, $6, $7, '{}'::jsonb, '{}'::jsonb,
                '{}'::jsonb, '{}'::jsonb, $8, $8
            )
            "#,
    )
    .bind(step_id)
    .bind(order_id)
    .bind(attempt_id)
    .bind(leg_id)
    .bind(format!("order:{order_id}:execution:0"))
    .bind(started_at)
    .bind(details)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert execution step");

    if with_durable_provider_operation {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_provider_operations (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_step_id,
                    provider,
                    operation_type,
                    provider_ref,
                    status,
                    request_json,
                    response_json,
                    observed_state_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'velora', 'universal_router_swap',
                    'provider-ref', 'waiting_external', '{}'::jsonb,
                    '{"receipt":"recorded"}'::jsonb, '{}'::jsonb, $5, $5
                )
                "#,
        )
        .bind(Uuid::now_v7())
        .bind(order_id)
        .bind(attempt_id)
        .bind(step_id)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert durable provider operation");
    }

    SeededRunningStep {
        order_id,
        attempt_id,
        step_id,
    }
}

fn unit_deposit_idempotency_test_deps(
    db: Database,
) -> (OrderActivityDeps, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let (unit_provider, gen_calls, transfer_actions_built) = CountingUnitDepositProvider::new();
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(unit_provider)],
        vec![],
    ));
    let deps = test_deps_with_action_providers(db, action_providers, None);
    (deps, gen_calls, transfer_actions_built)
}

fn unit_deposit_expected_idempotency_key(step: &OrderExecutionStep, _vault_id: Uuid) -> String {
    format!(
        "order:{}:unit_deposit:step:{}",
        step.order_id, step.step_index,
    )
}

async fn load_unit_deposit_operation(
    db: &Database,
    idempotency_key: &str,
) -> OrderProviderOperation {
    db.orders()
        .get_provider_operation_by_idempotency_key(
            ProviderId::Unit.as_str(),
            ProviderOperationType::UnitDeposit,
            idempotency_key,
        )
        .await
        .expect("load unit deposit operation by idempotency key")
        .expect("unit deposit operation should exist")
}

async fn seed_unit_deposit_retry_steps(
    db: &Database,
    pool: &PgPool,
    first_vault_id: Uuid,
    retry_vault_id: Uuid,
) -> (OrderExecutionStep, OrderExecutionStep) {
    let order_id = Uuid::now_v7();
    let first_attempt_id = Uuid::now_v7();
    let retry_attempt_id = Uuid::now_v7();
    let first_leg_id = Uuid::now_v7();
    let retry_leg_id = Uuid::now_v7();
    let first_step_id = Uuid::now_v7();
    let retry_step_id = Uuid::now_v7();
    let now = Utc::now();
    let trace_id = order_id.simple().to_string();
    let parent_span_id = trace_id[..16].to_string();

    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, 'market_order', 'executing', 'evm:8453', 'native',
                'hyperliquid', 'UETH', $2, $3, $4, $5, $6, $6
            )
            "#,
    )
    .bind(order_id)
    .bind(test_address(5))
    .bind(test_address(6))
    .bind(trace_id)
    .bind(parent_span_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert unit deposit test order");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (
                order_id,
                amount_in,
                created_at,
                updated_at
            )
            VALUES ($1, '100', $2, $2)
            "#,
    )
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert unit deposit test market action");

    for (attempt_id, attempt_index, attempt_kind, status) in [
        (
            first_attempt_id,
            1_i32,
            OrderExecutionAttemptKind::PrimaryExecution,
            OrderExecutionAttemptStatus::Cancelled,
        ),
        (
            retry_attempt_id,
            2_i32,
            OrderExecutionAttemptKind::RetryExecution,
            OrderExecutionAttemptStatus::Active,
        ),
    ] {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, '{}'::jsonb, $7, $7)
                "#,
        )
        .bind(attempt_id)
        .bind(order_id)
        .bind(attempt_index)
        .bind(attempt_kind.to_db_string())
        .bind(status.to_db_string())
        .bind(None::<Uuid>)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert unit deposit test attempt");
    }

    for (attempt_id, leg_id, step_id, vault_id, details) in [
        (
            first_attempt_id,
            first_leg_id,
            first_step_id,
            first_vault_id,
            json!({}),
        ),
        (
            retry_attempt_id,
            retry_leg_id,
            retry_step_id,
            retry_vault_id,
            json!({
                "retry_attempt_from_step_id": first_step_id,
                "retry_attempt_from_attempt_id": first_attempt_id,
            }),
        ),
    ] {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_legs (
                    id,
                    order_id,
                    execution_attempt_id,
                    transition_decl_id,
                    leg_index,
                    leg_type,
                    provider,
                    status,
                    input_chain_id,
                    input_asset_id,
                    output_chain_id,
                    output_asset_id,
                    amount_in,
                    estimated_amount_out,
                    started_at,
                    details_json,
                    usd_valuation_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, 'unit-deposit', 5, 'unit_deposit',
                    'unit', 'running', 'evm:8453', 'native', 'hyperliquid', 'UETH',
                    '100', '100', $4, $5, '{}'::jsonb, $4, $4
                )
                "#,
        )
        .bind(leg_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(now)
        .bind(details.clone())
        .execute(pool)
        .await
        .expect("insert unit deposit test leg");

        let request = json!({
            "order_id": order_id,
            "src_chain_id": "evm:8453",
            "dst_chain_id": "hyperliquid",
            "asset_id": "native",
            "amount": "100",
            "source_custody_vault_id": vault_id,
            "hyperliquid_custody_vault_address": test_address(7),
        });
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_steps (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_leg_id,
                    transition_decl_id,
                    step_index,
                    step_type,
                    provider,
                    status,
                    input_chain_id,
                    input_asset_id,
                    output_chain_id,
                    output_asset_id,
                    amount_in,
                    idempotency_key,
                    attempt_count,
                    started_at,
                    details_json,
                    request_json,
                    response_json,
                    error_json,
                    usd_valuation_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'unit-deposit', 5, 'unit_deposit',
                    'unit', 'running', 'evm:8453', 'native', 'hyperliquid', 'UETH',
                    '100', $5, 1, $6, $7, $8,
                    '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $6, $6
                )
                "#,
        )
        .bind(step_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(leg_id)
        .bind(format!("attempt:{attempt_id}:unit-deposit"))
        .bind(now)
        .bind(details)
        .bind(request)
        .execute(pool)
        .await
        .expect("insert unit deposit test step");
    }

    let first = db
        .orders()
        .get_execution_step(first_step_id)
        .await
        .expect("load first unit deposit step");
    let retry = db
        .orders()
        .get_execution_step(retry_step_id)
        .await
        .expect("load retry unit deposit step");
    (first, retry)
}

async fn seed_unit_withdrawal_retry_steps(
    db: &Database,
    pool: &PgPool,
    vault_id: Uuid,
    vault_address: String,
    derivation_salt: [u8; 32],
) -> (OrderExecutionStep, OrderExecutionStep) {
    let order_id = Uuid::now_v7();
    let first_attempt_id = Uuid::now_v7();
    let retry_attempt_id = Uuid::now_v7();
    let first_leg_id = Uuid::now_v7();
    let retry_leg_id = Uuid::now_v7();
    let first_step_id = Uuid::now_v7();
    let retry_step_id = Uuid::now_v7();
    let now = Utc::now();
    let trace_id = order_id.simple().to_string();
    let parent_span_id = trace_id[..16].to_string();
    let request = json!({
        "order_id": order_id,
        "input_chain_id": "hyperliquid",
        "input_asset": "UETH",
        "dst_chain_id": "evm:8453",
        "asset_id": "native",
        "amount": "39000000000000000",
        "min_amount_out": "1",
        "recipient_address": test_address(3),
        "hyperliquid_custody_vault_id": vault_id,
        "hyperliquid_custody_vault_address": vault_address,
    });

    sqlx_core::query::query(
        r#"
            INSERT INTO router_orders (
                id,
                order_type,
                status,
                source_chain_id,
                source_asset_id,
                destination_chain_id,
                destination_asset_id,
                recipient_address,
                refund_address,
                workflow_trace_id,
                workflow_parent_span_id,
                created_at,
                updated_at
            )
            VALUES (
                $1, 'market_order', 'executing', 'hyperliquid', 'UETH',
                'evm:8453', 'native', $2, $3, $4, $5, $6, $6
            )
            "#,
    )
    .bind(order_id)
    .bind(test_address(3))
    .bind(test_address(4))
    .bind(trace_id)
    .bind(parent_span_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert unit withdrawal test order");

    sqlx_core::query::query(
        r#"
            INSERT INTO market_order_actions (
                order_id,
                amount_in,
                created_at,
                updated_at
            )
            VALUES ($1, '39000000000000000', $2, $2)
            "#,
    )
    .bind(order_id)
    .bind(now)
    .execute(pool)
    .await
    .expect("insert unit withdrawal test market action");

    for (attempt_id, attempt_index, attempt_kind, status) in [
        (
            first_attempt_id,
            1_i32,
            OrderExecutionAttemptKind::PrimaryExecution,
            OrderExecutionAttemptStatus::Cancelled,
        ),
        (
            retry_attempt_id,
            2_i32,
            OrderExecutionAttemptKind::RetryExecution,
            OrderExecutionAttemptStatus::Active,
        ),
    ] {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_attempts (
                    id,
                    order_id,
                    attempt_index,
                    attempt_kind,
                    status,
                    trigger_step_id,
                    failure_reason_json,
                    input_custody_snapshot_json,
                    created_at,
                    updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, '{}'::jsonb, $7, $7)
                "#,
        )
        .bind(attempt_id)
        .bind(order_id)
        .bind(attempt_index)
        .bind(attempt_kind.to_db_string())
        .bind(status.to_db_string())
        .bind(None::<Uuid>)
        .bind(now)
        .execute(pool)
        .await
        .expect("insert unit withdrawal test attempt");
    }

    let vault = CustodyVault {
        id: vault_id,
        order_id: Some(order_id),
        role: CustodyVaultRole::HyperliquidSpot,
        visibility: CustodyVaultVisibility::Internal,
        chain: ChainId::parse("hyperliquid").expect("valid hyperliquid chain"),
        asset: Some(AssetId::parse("UETH").expect("valid UETH asset")),
        address: vault_address,
        control_type: CustodyVaultControlType::RouterDerivedKey,
        derivation_salt: Some(derivation_salt),
        signer_ref: None,
        status: CustodyVaultStatus::Active,
        metadata: json!({}),
        created_at: now,
        updated_at: now,
    };
    db.orders()
        .create_custody_vault(&vault)
        .await
        .expect("insert unit withdrawal test custody vault");

    for (attempt_id, leg_id, step_id, details) in [
        (first_attempt_id, first_leg_id, first_step_id, json!({})),
        (
            retry_attempt_id,
            retry_leg_id,
            retry_step_id,
            json!({
                "retry_attempt_from_step_id": first_step_id,
                "retry_attempt_from_attempt_id": first_attempt_id,
            }),
        ),
    ] {
        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_legs (
                    id,
                    order_id,
                    execution_attempt_id,
                    transition_decl_id,
                    leg_index,
                    leg_type,
                    provider,
                    status,
                    input_chain_id,
                    input_asset_id,
                    output_chain_id,
                    output_asset_id,
                    amount_in,
                    estimated_amount_out,
                    started_at,
                    details_json,
                    usd_valuation_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, 'unit-withdrawal', 5, 'unit_withdrawal',
                    'unit', 'running', 'hyperliquid', 'UETH', 'evm:8453', 'native',
                    '39000000000000000', '39000000000000000', $4, $5,
                    '{}'::jsonb, $4, $4
                )
                "#,
        )
        .bind(leg_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(now)
        .bind(details.clone())
        .execute(pool)
        .await
        .expect("insert unit withdrawal test leg");

        sqlx_core::query::query(
            r#"
                INSERT INTO order_execution_steps (
                    id,
                    order_id,
                    execution_attempt_id,
                    execution_leg_id,
                    transition_decl_id,
                    step_index,
                    step_type,
                    provider,
                    status,
                    input_chain_id,
                    input_asset_id,
                    output_chain_id,
                    output_asset_id,
                    amount_in,
                    idempotency_key,
                    attempt_count,
                    started_at,
                    details_json,
                    request_json,
                    response_json,
                    error_json,
                    usd_valuation_json,
                    created_at,
                    updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'unit-withdrawal', 5, 'unit_withdrawal',
                    'unit', 'running', 'hyperliquid', 'UETH', 'evm:8453', 'native',
                    '39000000000000000', $5, 1, $6, $7, $8,
                    '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, $6, $6
                )
                "#,
        )
        .bind(step_id)
        .bind(order_id)
        .bind(attempt_id)
        .bind(leg_id)
        .bind(format!("attempt:{attempt_id}:unit-withdrawal"))
        .bind(now)
        .bind(details)
        .bind(request.clone())
        .execute(pool)
        .await
        .expect("insert unit withdrawal test step");
    }

    let first = db
        .orders()
        .get_execution_step(first_step_id)
        .await
        .expect("load first unit withdrawal step");
    let retry = db
        .orders()
        .get_execution_step(retry_step_id)
        .await
        .expect("load retry unit withdrawal step");
    (first, retry)
}

async fn run_unit_withdrawal_retry_idempotency_scenario() -> (usize, usize) {
    let test_db = test_database().await;
    let exchange_server = TestHyperliquidExchangeServer::spawn().await;
    let (unit_provider, gen_calls) =
        CountingUnitWithdrawalProvider::new(exchange_server.base_url.clone());
    let action_providers = Arc::new(ActionProviderRegistry::new(
        vec![],
        vec![Arc::new(unit_provider)],
        vec![],
    ));
    let settings = test_settings();
    let derivation_salt = [0x55_u8; 32];
    let hyperliquid_chain = Arc::new(HyperliquidChain::new(
        b"hyperliquid-wallet",
        1,
        std::time::Duration::from_secs(1),
    ));
    let vault_address = hyperliquid_chain
        .derive_wallet(&settings.master_key_bytes(), &derivation_salt)
        .expect("derive test Hyperliquid wallet")
        .address
        .clone();
    let mut chain_registry = ChainRegistry::new();
    chain_registry.register(ChainType::Hyperliquid, hyperliquid_chain);
    let deps = test_deps_with_action_providers_and_chain_registry(
        test_db.db.clone(),
        action_providers,
        Some(exchange_server.base_url.clone()),
        Arc::new(chain_registry),
    );
    let (first_step, retry_step) = seed_unit_withdrawal_retry_steps(
        &test_db.db,
        &test_db.pool,
        Uuid::now_v7(),
        vault_address,
        derivation_salt,
    )
    .await;

    execute_running_step(&deps, &first_step)
        .await
        .expect("execute first unit withdrawal attempt");
    sqlx_core::query::query(
        r#"
        UPDATE order_execution_steps
        SET status = 'cancelled',
            completed_at = $2,
            updated_at = $2
        WHERE id = $1
        "#,
    )
    .bind(first_step.id)
    .bind(Utc::now())
    .execute(&test_db.pool)
    .await
    .expect("mark first unit withdrawal step cancelled");

    execute_running_step(&deps, &retry_step)
        .await
        .expect("execute retry unit withdrawal attempt");

    (
        gen_calls.load(Ordering::SeqCst),
        exchange_server.send_asset_call_count(),
    )
}

async fn assert_step_classification(db: &Database, step_id: Uuid, expected_reason: &str) {
    let step = db
        .orders()
        .get_execution_step(step_id)
        .await
        .expect("load classified step");
    assert_eq!(
        step.details
            .get("stale_running_step_classification")
            .and_then(|classification| classification.get("reason"))
            .and_then(Value::as_str),
        Some(expected_reason)
    );
}
