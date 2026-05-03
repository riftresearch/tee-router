use crate::{
    models::{
        empty_metadata, CustodyVault, DepositVaultFundingHint, OrderExecutionAttempt,
        OrderExecutionStep, OrderExecutionStepStatus, OrderProviderOperation,
        OrderProviderOperationHint, ProviderExecutionPolicyState, ProviderHealthCheck,
        ProviderHealthSummaryStatus, ProviderOperationHintKind, ProviderPolicy,
        ProviderQuotePolicyState, RouterOrder, RouterOrderQuote, RouterOrderStatus, VaultAction,
    },
    protocol::DepositAsset,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateVaultRequest {
    #[serde(default)]
    pub order_id: Option<Uuid>,
    pub deposit_asset: DepositAsset,
    #[serde(default)]
    pub action: VaultAction,
    pub recovery_address: String,
    pub cancellation_commitment: String,
    #[serde(default)]
    pub cancel_after: Option<DateTime<Utc>>,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelVaultRequest {
    pub cancellation_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CreateQuoteRequest {
    MarketOrder(MarketOrderQuoteRequest),
    LimitOrder(LimitOrderQuoteRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOrderRequest {
    pub quote_id: Uuid,
    pub refund_address: String,
    #[serde(default)]
    pub cancel_after: Option<DateTime<Utc>>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderCancellationRequest {
    pub cancellation_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketOrderQuoteRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub recipient_address: String,
    #[serde(flatten)]
    pub order_kind: MarketOrderQuoteKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LimitOrderQuoteRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub recipient_address: String,
    pub input_amount: String,
    pub output_amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketOrderQuoteKind {
    ExactIn {
        amount_in: String,
        slippage_bps: u64,
    },
    ExactOut {
        amount_out: String,
        slippage_bps: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintRequest {
    pub provider_operation_id: Uuid,
    #[serde(default = "default_hint_source")]
    pub source: String,
    #[serde(default = "default_hint_kind")]
    pub hint_kind: ProviderOperationHintKind,
    #[serde(default = "empty_metadata")]
    pub evidence: Value,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderOperationHintEnvelope {
    pub hint: OrderProviderOperationHint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultFundingHintRequest {
    #[serde(default = "default_hint_source")]
    pub source: String,
    #[serde(default = "default_hint_kind")]
    pub hint_kind: ProviderOperationHintKind,
    #[serde(default = "empty_metadata")]
    pub evidence: Value,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VaultFundingHintEnvelope {
    pub hint: DepositVaultFundingHint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetectorHintRequest {
    pub target: DetectorHintTarget,
    #[serde(default = "default_hint_source")]
    pub source: String,
    #[serde(default = "default_hint_kind")]
    pub hint_kind: ProviderOperationHintKind,
    #[serde(default = "empty_metadata")]
    pub evidence: Value,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DetectorHintTarget {
    ProviderOperation { id: Uuid },
    FundingVault { id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DetectorHintEnvelope {
    ProviderOperation { hint: OrderProviderOperationHint },
    FundingVault { hint: DepositVaultFundingHint },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProviderOperationObserveRequest {
    #[serde(default = "empty_metadata")]
    pub hint_evidence: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateProviderPolicyRequest {
    pub quote_state: ProviderQuotePolicyState,
    pub execution_state: ProviderExecutionPolicyState,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub updated_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderPolicyEnvelope {
    pub policy: ProviderPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderPolicyListEnvelope {
    pub policies: Vec<ProviderPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderHealthEnvelope {
    pub status: ProviderHealthSummaryStatus,
    pub timestamp: DateTime<Utc>,
    pub providers: Vec<ProviderHealthCheck>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFlowEnvelope {
    pub flow: OrderFlow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFlow {
    pub order: RouterOrder,
    pub trace: OrderFlowTrace,
    pub progress: OrderFlowProgress,
    pub quote: Option<RouterOrderQuote>,
    pub attempts: Vec<OrderExecutionAttempt>,
    pub steps: Vec<OrderExecutionStep>,
    pub provider_operations: Vec<OrderProviderOperation>,
    pub custody_vaults: Vec<CustodyVault>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFlowTrace {
    pub trace_id: String,
    pub parent_span_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFlowProgress {
    pub terminal: bool,
    pub percent_complete: u8,
    pub completed_steps: usize,
    pub total_steps: usize,
    pub waiting_steps: usize,
    pub running_steps: usize,
    pub failed_steps: usize,
    pub retry_count: i32,
    pub current_step_id: Option<Uuid>,
    pub current_step_index: Option<i32>,
    pub current_step_type: Option<String>,
    pub current_provider: Option<String>,
    pub current_provider_operation_id: Option<Uuid>,
}

impl OrderFlowProgress {
    #[must_use]
    pub fn from_parts(
        order: &RouterOrder,
        steps: &[OrderExecutionStep],
        provider_operations: &[OrderProviderOperation],
    ) -> Self {
        let executable_steps: Vec<&OrderExecutionStep> =
            steps.iter().filter(|step| step.step_index > 0).collect();
        let total_steps = executable_steps.len();
        let completed_steps = executable_steps
            .iter()
            .filter(|step| step.status == OrderExecutionStepStatus::Completed)
            .count();
        let waiting_steps = executable_steps
            .iter()
            .filter(|step| step.status == OrderExecutionStepStatus::Waiting)
            .count();
        let running_steps = executable_steps
            .iter()
            .filter(|step| step.status == OrderExecutionStepStatus::Running)
            .count();
        let failed_steps = executable_steps
            .iter()
            .filter(|step| step.status == OrderExecutionStepStatus::Failed)
            .count();
        let retry_count = executable_steps
            .iter()
            .map(|step| step.attempt_count)
            .sum::<i32>();
        let current_step = executable_steps
            .iter()
            .find(|step| {
                matches!(
                    step.status,
                    OrderExecutionStepStatus::Running
                        | OrderExecutionStepStatus::Waiting
                        | OrderExecutionStepStatus::Ready
                        | OrderExecutionStepStatus::Planned
                )
            })
            .copied();
        let current_provider_operation_id = current_step.and_then(|step| {
            provider_operations
                .iter()
                .rev()
                .find(|operation| operation.execution_step_id == Some(step.id))
                .map(|operation| operation.id)
        });
        let terminal = matches!(
            order.status,
            RouterOrderStatus::Completed
                | RouterOrderStatus::Refunded
                | RouterOrderStatus::RefundManualInterventionRequired
                | RouterOrderStatus::Failed
                | RouterOrderStatus::Expired
        );
        let percent_complete = if terminal && order.status == RouterOrderStatus::Completed {
            100
        } else if total_steps == 0 {
            0
        } else {
            let pct = completed_steps.saturating_mul(100) / total_steps;
            u8::try_from(pct.min(100)).unwrap_or(100)
        };

        Self {
            terminal,
            percent_complete,
            completed_steps,
            total_steps,
            waiting_steps,
            running_steps,
            failed_steps,
            retry_count,
            current_step_id: current_step.map(|step| step.id),
            current_step_index: current_step.map(|step| step.step_index),
            current_step_type: current_step.map(|step| step.step_type.to_db_string().to_string()),
            current_provider: current_step.map(|step| step.provider.clone()),
            current_provider_operation_id,
        }
    }
}

fn default_hint_source() -> String {
    "unknown".to_string()
}

fn default_hint_kind() -> ProviderOperationHintKind {
    ProviderOperationHintKind::PossibleProgress
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        models::{
            MarketOrderAction, MarketOrderKind, ProviderOperationStatus, ProviderOperationType,
            RouterOrderAction, RouterOrderType,
        },
        protocol::{AssetId, ChainId},
    };
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn create_quote_request_deserializes_market_order_shape() {
        let request: CreateQuoteRequest = serde_json::from_value(json!({
            "type": "market_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "recipient_address": "bc1qrecipient0000000000000000000000000000000",
            "kind": "exact_in",
            "amount_in": "1000",
            "slippage_bps": 100
        }))
        .expect("market order request");

        let CreateQuoteRequest::MarketOrder(request) = request else {
            panic!("expected market order request");
        };
        assert_eq!(request.from_asset.chain.as_str(), "evm:8453");
        assert_eq!(request.to_asset.chain.as_str(), "bitcoin");
        assert!(matches!(
            request.order_kind,
            MarketOrderQuoteKind::ExactIn { .. }
        ));
    }

    #[test]
    fn create_quote_request_deserializes_limit_order_shape() {
        let request: CreateQuoteRequest = serde_json::from_value(json!({
            "type": "limit_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "recipient_address": "bc1qrecipient0000000000000000000000000000000",
            "input_amount": "100000000000",
            "output_amount": "100000000"
        }))
        .expect("limit order request");

        let CreateQuoteRequest::LimitOrder(request) = request else {
            panic!("expected limit order request");
        };
        assert_eq!(request.from_asset.chain.as_str(), "evm:8453");
        assert_eq!(request.to_asset.chain.as_str(), "bitcoin");
        assert_eq!(request.input_amount, "100000000000");
        assert_eq!(request.output_amount, "100000000");
    }

    #[test]
    fn order_flow_progress_counts_steps_and_current_provider_operation() {
        let order_id = Uuid::now_v7();
        let step_1_id = Uuid::now_v7();
        let step_2_id = Uuid::now_v7();
        let operation_id = Uuid::now_v7();
        let order = test_order(order_id, RouterOrderStatus::Executing);
        let steps = vec![
            test_step(order_id, 0, OrderExecutionStepStatus::Waiting),
            test_step_with_id(
                order_id,
                step_1_id,
                1,
                OrderExecutionStepStatus::Completed,
                0,
            ),
            test_step_with_id(order_id, step_2_id, 2, OrderExecutionStepStatus::Waiting, 2),
        ];
        let provider_operations = vec![test_provider_operation(order_id, operation_id, step_2_id)];

        let progress = OrderFlowProgress::from_parts(&order, &steps, &provider_operations);

        assert!(!progress.terminal);
        assert_eq!(progress.total_steps, 2);
        assert_eq!(progress.completed_steps, 1);
        assert_eq!(progress.waiting_steps, 1);
        assert_eq!(progress.retry_count, 2);
        assert_eq!(progress.percent_complete, 50);
        assert_eq!(progress.current_step_id, Some(step_2_id));
        assert_eq!(progress.current_provider_operation_id, Some(operation_id));
    }

    #[test]
    fn completed_order_flow_reports_full_progress() {
        let order_id = Uuid::now_v7();
        let order = test_order(order_id, RouterOrderStatus::Completed);
        let steps = vec![test_step(order_id, 1, OrderExecutionStepStatus::Completed)];

        let progress = OrderFlowProgress::from_parts(&order, &steps, &[]);

        assert!(progress.terminal);
        assert_eq!(progress.percent_complete, 100);
    }

    fn test_order(order_id: Uuid, status: RouterOrderStatus) -> RouterOrder {
        let now = Utc::now();
        RouterOrder {
            id: order_id,
            order_type: RouterOrderType::MarketOrder,
            status,
            funding_vault_id: None,
            source_asset: DepositAsset {
                chain: ChainId::parse("evm:8453").unwrap(),
                asset: AssetId::Native,
            },
            destination_asset: DepositAsset {
                chain: ChainId::parse("bitcoin").unwrap(),
                asset: AssetId::Native,
            },
            recipient_address: "bc1qrecipient0000000000000000000000000000000".to_string(),
            refund_address: "0x1111111111111111111111111111111111111111".to_string(),
            action: RouterOrderAction::MarketOrder(MarketOrderAction {
                order_kind: MarketOrderKind::ExactIn {
                    amount_in: "1000".to_string(),
                    min_amount_out: "1".to_string(),
                },
                slippage_bps: 100,
            }),
            action_timeout_at: now,
            idempotency_key: None,
            workflow_trace_id: order_id.simple().to_string(),
            workflow_parent_span_id: "1111111111111111".to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    fn test_step(
        order_id: Uuid,
        step_index: i32,
        status: OrderExecutionStepStatus,
    ) -> OrderExecutionStep {
        test_step_with_id(order_id, Uuid::now_v7(), step_index, status, 0)
    }

    fn test_step_with_id(
        order_id: Uuid,
        step_id: Uuid,
        step_index: i32,
        status: OrderExecutionStepStatus,
        attempt_count: i32,
    ) -> OrderExecutionStep {
        let now = Utc::now();
        OrderExecutionStep {
            id: step_id,
            order_id,
            execution_attempt_id: None,
            transition_decl_id: None,
            step_index,
            step_type: crate::models::OrderExecutionStepType::AcrossBridge,
            provider: "across".to_string(),
            status,
            input_asset: None,
            output_asset: None,
            amount_in: None,
            min_amount_out: None,
            tx_hash: None,
            provider_ref: None,
            idempotency_key: None,
            attempt_count,
            next_attempt_at: None,
            started_at: None,
            completed_at: None,
            details: json!({}),
            request: json!({}),
            response: json!({}),
            error: json!({}),
            created_at: now,
            updated_at: now,
        }
    }

    fn test_provider_operation(
        order_id: Uuid,
        operation_id: Uuid,
        step_id: Uuid,
    ) -> OrderProviderOperation {
        let now = Utc::now();
        OrderProviderOperation {
            id: operation_id,
            order_id,
            execution_attempt_id: None,
            execution_step_id: Some(step_id),
            provider: "across".to_string(),
            operation_type: ProviderOperationType::AcrossBridge,
            provider_ref: Some("provider-ref".to_string()),
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({}),
            created_at: now,
            updated_at: now,
        }
    }
}
