use crate::{
    models::{
        empty_metadata, DepositVaultFundingHint, MarketOrderKind, OrderProviderOperationHint,
        ProviderExecutionPolicyState, ProviderOperationHintKind, ProviderPolicy,
        ProviderQuotePolicyState, VaultAction,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QuoteRequestType {
    MarketOrder,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateQuoteRequest {
    #[serde(rename = "type")]
    pub quote_type: QuoteRequestType,
    #[serde(flatten)]
    pub market_order: MarketOrderQuoteRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateOrderRequest {
    pub quote_id: Uuid,
    pub refund_address: String,
    pub cancellation_commitment: String,
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
    pub order_kind: MarketOrderKind,
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

fn default_hint_source() -> String {
    "unknown".to_string()
}

fn default_hint_kind() -> ProviderOperationHintKind {
    ProviderOperationHintKind::PossibleProgress
}
