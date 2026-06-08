use chrono::{DateTime, Utc};
use router_core::{
    models::{
        empty_metadata, CustodyVault, DepositVaultFundingHint, OrderExecutionAttempt,
        OrderExecutionLeg, OrderExecutionStep, OrderExecutionStepStatus, OrderProviderOperation,
        OrderProviderOperationHint, ProviderExecutionPolicyState, ProviderHealthCheck,
        ProviderHealthSummaryStatus, ProviderOperationHintKind, ProviderPolicy,
        ProviderQuotePolicyState, RouterOrder, RouterOrderQuote, RouterOrderStatus, RouterSwitch,
        VaultAction,
    },
    protocol::DepositAsset,
    services::asset_registry::ProviderId,
};
use router_temporal::WorkflowStepId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CreateQuoteRequest {
    MarketOrder(ApiMarketOrderQuoteRequest),
    LimitOrder(LimitOrderQuoteRequest),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOrderRequest {
    pub quote_id: Uuid,
    /// Recipient address on the destination chain. Supplied at order time
    /// (quotes are recipient-agnostic); this becomes `order.recipient_address`.
    pub recipient_address: String,
    pub refund_address: String,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiMarketOrderQuoteRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub amount_in: String,
    #[serde(default)]
    pub routing: QuoteRoutingRequest,
}

impl ApiMarketOrderQuoteRequest {
    #[must_use]
    pub fn into_parts(self) -> (MarketOrderQuoteRequest, QuoteRoutingRequest) {
        (
            MarketOrderQuoteRequest {
                from_asset: self.from_asset,
                to_asset: self.to_asset,
                amount_in: self.amount_in,
            },
            self.routing,
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketOrderQuoteRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub amount_in: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuoteRoutingRequest {
    #[serde(default)]
    pub provider_sequence: Option<Vec<ProviderId>>,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderOperationHintRequest {
    pub provider_operation_id: Uuid,
    pub execution_step_id: WorkflowStepId,
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
    ProviderOperation {
        id: Uuid,
        execution_step_id: WorkflowStepId,
    },
    FundingVault {
        id: Uuid,
    },
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
#[serde(deny_unknown_fields)]
pub struct UpdateRouterSwitchRequest {
    pub enabled: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub updated_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterSwitchEnvelope {
    pub switch: RouterSwitch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterSwitchListEnvelope {
    pub refund_only_mode: RouterSwitch,
    pub provider_policies: Vec<ProviderPolicy>,
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

/// Response body for `GET /internal/v1/route-graph`. A static snapshot of the
/// `AssetRegistry` routing graph (every declared transition plus the curated
/// Velora same-chain swap edges) used by the admin dashboard visualizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteGraphEnvelope {
    pub nodes: Vec<RouteGraphNode>,
    pub edges: Vec<RouteGraphEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteGraphNode {
    /// Stable node key shared by the edge `from`/`to` references.
    pub key: String,
    /// `external` (a chain asset) or `venue` (a Hyperliquid spot node).
    pub kind: String,
    pub chain: String,
    pub asset: String,
    pub canonical: String,
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteGraphEdge {
    /// Matches the route-cost `transition_id` so cached bps can be joined.
    pub id: String,
    pub provider: String,
    pub kind: String,
    pub edge_kind: String,
    pub from: String,
    pub to: String,
    pub from_chain: String,
    pub from_asset: String,
    pub to_chain: String,
    pub to_asset: String,
    /// True when this edge is in the curated route-cost sampling set, i.e. the
    /// refresher persists a measured bps row for it. The dashboard uses this to
    /// scaffold the route-cost tables (so curated routes still render with
    /// dashes when no snapshot exists yet).
    pub curated: bool,
}

/// Request body for `POST /internal/v1/route-explain`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteExplainRequest {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub amount_in: String,
    /// When true, additionally live-quotes the top ranked paths to report real
    /// per-path outputs and the true winner. Defaults to cache-only dry run.
    #[serde(default)]
    pub live_quote: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteExplainEnvelope {
    pub explain: RouteExplain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteExplain {
    pub from_asset: DepositAsset,
    pub to_asset: DepositAsset,
    pub amount_in: String,
    pub request_usd_micros: u64,
    pub tier_label: String,
    pub live_quote: bool,
    pub counts: RouteExplainCounts,
    pub timings: RouteExplainTimings,
    pub ranked: Vec<RankedPathView>,
    pub winner_path_id: Option<String>,
    /// Source->destination candidate subgraph: exactly the nodes and edges that
    /// participate in a viable ranked path, laid out left-to-right by hop depth
    /// from the source (source at depth 0, destination forced to `max_depth`).
    pub graph: RouteExplainGraph,
}

/// Source->destination corridor for the route-explain visualizer: the full
/// declared display topology (incl. curated same-chain Velora swaps) restricted
/// to nodes between source and destination. Ranked routes are overlaid as
/// highlights by the dashboard via edge ids.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteExplainGraph {
    pub nodes: Vec<RouteExplainGraphNode>,
    pub edges: Vec<RouteExplainGraphEdge>,
    pub max_depth: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteExplainGraphNode {
    /// Stable key shared with edge `from`/`to` references.
    pub key: String,
    /// `external` (a real chain asset) or `venue` (an internal venue node).
    pub kind: String,
    pub chain: String,
    pub asset: String,
    pub canonical: String,
    /// Min hop distance from the source node (source = 0). The destination is
    /// pinned to `max_depth` so it renders in the rightmost column.
    pub depth: usize,
    /// `source`, `destination`, or `intermediate`.
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteExplainGraphEdge {
    /// Transition id; matches `RouteTransitionView.id` and the route-cost
    /// `transition_id`, so the dashboard can join cached bps and highlight.
    pub id: String,
    pub from: String,
    pub to: String,
    pub provider: String,
    pub kind: String,
    pub edge_kind: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RouteExplainCounts {
    pub paths_enumerated: usize,
    pub paths_after_executable: usize,
    pub paths_after_provider: usize,
    pub paths_after_hyperevm: usize,
    pub paths_after_same_chain: usize,
    pub ranked_count: usize,
    /// Number of top-ranked paths selected for live quoting (hardcoded top-N).
    pub top_paths: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct RouteExplainTimings {
    pub bfs_ms: u64,
    pub rank_ms: u64,
    pub live_quote_ms: u64,
    pub total_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedPathView {
    pub rank: usize,
    pub path_id: String,
    /// True when this path is in the top-N set selected for live quoting.
    pub top_path: bool,
    pub winner: bool,
    pub hop_count: usize,
    pub missing_edges: usize,
    pub total_effective_cost_usd_micros: u64,
    pub total_latency_ms: u64,
    pub transitions: Vec<RouteTransitionView>,
    /// Present only when `live_quote` was requested and this path was quoted.
    pub estimated_amount_out: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteTransitionView {
    pub id: String,
    pub provider: String,
    pub kind: String,
    pub edge_kind: String,
    pub from_chain: String,
    pub from_asset: String,
    pub to_chain: String,
    pub to_asset: String,
}

/// Response body for the admin manual-refund endpoint
/// (`POST /internal/v1/orders/:id/refund`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRefundEnvelope {
    pub order_id: Uuid,
    /// Workflow id of the standalone `RefundWorkflow` — watch it in the
    /// Temporal UI.
    pub workflow_id: String,
    pub outcome: OrderRefundOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderRefundOutcome {
    /// A new manual `RefundWorkflow` run was started.
    RefundStarted,
    /// A manual `RefundWorkflow` for this order was already running; the
    /// repeated trigger was a no-op (idempotent).
    RefundAlreadyInProgress,
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
                step.step_index > 0 && step.status != OrderExecutionStepStatus::Cancelled
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
                | RouterOrderStatus::RefundRequired
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
            MarketOrderAction, ProviderOperationStatus, ProviderOperationType, RouterOrderAction,
            RouterOrderType,
        },
        protocol::{AssetId, ChainId},
    };
    use serde_json::json;

    #[test]
    fn create_quote_request_deserializes_market_order_shape() {
        let request: CreateQuoteRequest = serde_json::from_value(json!({
            "type": "market_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "amount_in": "1000"
        }))
        .expect("market order request");

        let CreateQuoteRequest::MarketOrder(request) = request else {
            panic!("expected market order request");
        };
        assert_eq!(request.from_asset.chain.as_str(), "evm:8453");
        assert_eq!(request.to_asset.chain.as_str(), "bitcoin");
        assert_eq!(request.amount_in, "1000");
        assert!(request.routing.provider_sequence.is_none());
    }

    #[test]
    fn create_quote_request_deserializes_provider_sequence_routing() {
        let request: CreateQuoteRequest = serde_json::from_value(json!({
            "type": "market_order",
            "from_asset": {"chain": "evm:42161", "asset": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"},
            "to_asset": {"chain": "evm:8453", "asset": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"},
            "amount_in": "5000000",
            "routing": {
                "provider_sequence": ["velora", "hyperliquid_spot"]
            }
        }))
        .expect("market order request with routing");

        let CreateQuoteRequest::MarketOrder(request) = request else {
            panic!("expected market order request");
        };
        assert_eq!(
            request.routing.provider_sequence,
            Some(vec![ProviderId::Velora, ProviderId::HyperliquidSpot])
        );
    }

    #[test]
    fn create_quote_request_rejects_old_hyperliquid_provider_id() {
        let result = serde_json::from_value::<CreateQuoteRequest>(json!({
            "type": "market_order",
            "from_asset": {"chain": "hyperliquid", "asset": "UBTC"},
            "to_asset": {"chain": "hyperliquid", "asset": "native"},
            "amount_in": "50000",
            "routing": {
                "provider_sequence": ["hyperliquid"]
            }
        }));

        assert!(result.is_err());
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
    fn market_order_quote_request_rejects_unknown_outer_fields() {
        let error = serde_json::from_value::<CreateQuoteRequest>(json!({
            "type": "market_order",
            "from_asset": {"chain": "evm:8453", "asset": "native"},
            "to_asset": {"chain": "bitcoin", "asset": "native"},
            "recipient_address": "bc1qrecipient0000000000000000000000000000000",
            "amount_in": "1000",
            "ignored": true
        }))
        .expect_err("market order quotes should reject unknown fields");

        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn internal_hint_request_rejects_unknown_fields() {
        let error = serde_json::from_value::<ProviderOperationHintRequest>(json!({
            "provider_operation_id": Uuid::now_v7(),
            "execution_step_id": Uuid::now_v7(),
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
                "execution_step_id": Uuid::now_v7(),
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
                amount_in: "1000".to_string(),
            }),
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
            idempotency_key: None,
            status: ProviderOperationStatus::WaitingExternal,
            request: json!({}),
            response: json!({}),
            observed_state: json!({}),
            created_at: now,
            updated_at: now,
        }
    }
}
