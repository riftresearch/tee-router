use chrono::{DateTime, Utc};
use router_core::{
    models::{
        empty_metadata, CustodyVault, DepositVaultFundingHint, OrderExecutionAttempt,
        OrderExecutionLeg, OrderExecutionStep, OrderExecutionStepStatus, OrderProviderOperation,
        OrderProviderOperationHint, ProviderExecutionPolicyState, ProviderHealthCheck,
        ProviderHealthSummaryStatus, ProviderOperationHintKind, ProviderPolicy,
        ProviderQuotePolicyState, RouterOrder, RouterOrderQuote, RouterOrderStatus, VaultAction,
    },
    protocol::DepositAsset,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use uuid::Uuid;

pub const MAX_HINT_IDEMPOTENCY_KEY_LEN: usize = 128;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelVaultRequest {
    pub cancellation_secret: String,
}

impl fmt::Debug for CancelVaultRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancelVaultRequest")
            .field("cancellation_secret", &"<redacted>")
            .finish()
    }
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
    pub idempotency_key: Option<String>,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOrderCancellationRequest {
    pub cancellation_secret: String,
}

impl fmt::Debug for CreateOrderCancellationRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateOrderCancellationRequest")
            .field("cancellation_secret", &"<redacted>")
            .finish()
    }
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
#[serde(deny_unknown_fields)]
pub struct LimitOrderQuoteRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub recipient_address: String,
    pub input_amount: String,
    pub output_amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum MarketOrderQuoteKind {
    ExactIn {
        amount_in: String,
        #[serde(default)]
        slippage_bps: Option<u64>,
    },
    ExactOut {
        amount_out: String,
        #[serde(default)]
        slippage_bps: Option<u64>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
    pub legs: Vec<OrderExecutionLeg>,
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
        let executable_steps: Vec<&OrderExecutionStep> = steps
            .iter()
            .filter(|step| {
                step.step_index > 0 && step.status != OrderExecutionStepStatus::Superseded
            })
            .collect();
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
                | RouterOrderStatus::ManualInterventionRequired
                | RouterOrderStatus::RefundManualInterventionRequired
                | RouterOrderStatus::Expired
        );
        let percent_complete = if terminal && order.status == RouterOrderStatus::Completed {
            100
        } else if total_steps == 0 {
            0
        } else {
            percent_complete_from_counts(completed_steps, total_steps)
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

fn percent_complete_from_counts(completed_steps: usize, total_steps: usize) -> u8 {
    if total_steps == 0 {
        return 0;
    }
    let percent = ((completed_steps as u128) * 100) / (total_steps as u128);
    percent.min(100) as u8
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
    use chrono::Utc;
    use router_core::{
        models::{
            MarketOrderAction, MarketOrderKind, MarketOrderKindType, MarketOrderQuote,
            ProviderOperationStatus, ProviderOperationType, RouterOrderAction, RouterOrderEnvelope,
            RouterOrderQuote, RouterOrderType,
        },
        protocol::{AssetId, ChainId},
    };
    use serde_json::json;

    #[test]
    fn cancellation_request_debug_redacts_secret_values() {
        let secret = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let cancel_vault = CancelVaultRequest {
            cancellation_secret: secret.to_string(),
        };
        let rendered = format!("{cancel_vault:?}");
        assert!(rendered.contains("cancellation_secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains(secret));

        let cancel_order = CreateOrderCancellationRequest {
            cancellation_secret: secret.to_string(),
        };
        let rendered = format!("{cancel_order:?}");
        assert!(rendered.contains("cancellation_secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains(secret));
    }

    #[test]
    fn order_envelope_debug_redacts_cancellation_secret() {
        let order_id = Uuid::now_v7();
        let order = test_order(order_id, RouterOrderStatus::PendingFunding);
        let quote = RouterOrderQuote::MarketOrder(MarketOrderQuote {
            id: Uuid::now_v7(),
            order_id: Some(order_id),
            source_asset: order.source_asset.clone(),
            destination_asset: order.destination_asset.clone(),
            recipient_address: order.recipient_address.clone(),
            provider_id: "test".to_string(),
            order_kind: MarketOrderKindType::ExactIn,
            amount_in: "1000".to_string(),
            amount_out: "900".to_string(),
            min_amount_out: Some("891".to_string()),
            max_amount_in: None,
            slippage_bps: Some(100),
            provider_quote: json!({}),
            usd_valuation: json!({}),
            expires_at: order.created_at + chrono::Duration::minutes(1),
            created_at: order.created_at,
        });
        let secret = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let envelope = RouterOrderEnvelope {
            order,
            quote,
            funding_vault: None,
            cancellation_secret: Some(secret.to_string()),
        };

        let rendered = format!("{envelope:?}");
        assert!(rendered.contains("cancellation_secret"));
        assert!(rendered.contains("<redacted>"));
        assert!(!rendered.contains(secret));
    }

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
    fn limit_order_quote_request_rejects_unknown_fields() {
        let error = serde_json::from_value::<CreateQuoteRequest>(json!({
            "type": "limit_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "recipient_address": "bc1qrecipient0000000000000000000000000000000",
            "input_amount": "100000000000",
            "output_amount": "100000000",
            "ignored": true
        }))
        .expect_err("limit order quotes should reject unknown fields");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn market_order_quote_kind_rejects_unknown_fields() {
        let error = serde_json::from_value::<MarketOrderQuoteKind>(json!({
            "kind": "exact_in",
            "amount_in": "1000",
            "amount_out": "2000",
            "slippage_bps": 100
        }))
        .expect_err("market order kind should reject irrelevant fields");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn market_order_quote_request_rejects_unknown_outer_fields() {
        let error = serde_json::from_value::<CreateQuoteRequest>(json!({
            "type": "market_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "recipient_address": "bc1qrecipient0000000000000000000000000000000",
            "kind": "exact_in",
            "amount_in": "1000",
            "slippage_bps": 100,
            "ignored": true
        }))
        .expect_err("market order quotes should reject unknown fields");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn internal_hint_request_rejects_unknown_fields() {
        let error = serde_json::from_value::<ProviderOperationHintRequest>(json!({
            "provider_operation_id": Uuid::now_v7(),
            "source": "sauron",
            "hint_kind": "possible_progress",
            "evidence": {},
            "ignored": true
        }))
        .expect_err("hint requests should reject unknown fields");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn detector_hint_target_rejects_unknown_fields() {
        let error = serde_json::from_value::<DetectorHintRequest>(json!({
            "target": {
                "kind": "provider_operation",
                "id": Uuid::now_v7(),
                "ignored": true
            },
            "source": "sauron",
            "hint_kind": "possible_progress",
            "evidence": {}
        }))
        .expect_err("detector hint targets should reject unknown fields");

        assert!(error.to_string().contains("unknown field"));
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

    #[test]
    fn percent_complete_uses_wide_math_and_clamps_overcomplete_counts() {
        assert_eq!(percent_complete_from_counts(usize::MAX, 1), 100);
        assert_eq!(percent_complete_from_counts(usize::MAX / 2, usize::MAX), 49);
        assert_eq!(percent_complete_from_counts(0, usize::MAX), 0);
        assert_eq!(percent_complete_from_counts(1, 0), 0);
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
                    min_amount_out: Some("1".to_string()),
                },
                slippage_bps: Some(100),
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
            execution_leg_id: None,
            transition_decl_id: None,
            step_index,
            step_type: router_core::models::OrderExecutionStepType::AcrossBridge,
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
            usd_valuation: json!({}),
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
