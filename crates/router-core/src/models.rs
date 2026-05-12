use crate::protocol::{AssetId, ChainId, DepositAsset};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;
use uuid::Uuid;

pub fn empty_metadata() -> Value {
    json!({})
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum VaultAction {
    #[default]
    Null,
    MarketOrder(MarketOrderAction),
    LimitOrder(LimitOrderAction),
}

impl From<RouterOrderAction> for VaultAction {
    fn from(value: RouterOrderAction) -> Self {
        match value {
            RouterOrderAction::MarketOrder(action) => Self::MarketOrder(action),
            RouterOrderAction::LimitOrder(action) => Self::LimitOrder(action),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouterOrderType {
    MarketOrder,
    LimitOrder,
}

impl RouterOrderType {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::MarketOrder => "market_order",
            Self::LimitOrder => "limit_order",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "market_order" => Some(Self::MarketOrder),
            "limit_order" => Some(Self::LimitOrder),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouterOrderStatus {
    Quoted,
    PendingFunding,
    Funded,
    Executing,
    Completed,
    RefundRequired,
    Refunding,
    Refunded,
    ManualInterventionRequired,
    RefundManualInterventionRequired,
    Expired,
}

impl RouterOrderStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Quoted => "quoted",
            Self::PendingFunding => "pending_funding",
            Self::Funded => "funded",
            Self::Executing => "executing",
            Self::Completed => "completed",
            Self::RefundRequired => "refund_required",
            Self::Refunding => "refunding",
            Self::Refunded => "refunded",
            Self::ManualInterventionRequired => "manual_intervention_required",
            Self::RefundManualInterventionRequired => "refund_manual_intervention_required",
            Self::Expired => "expired",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "quoted" => Some(Self::Quoted),
            "pending_funding" => Some(Self::PendingFunding),
            "funded" => Some(Self::Funded),
            "executing" => Some(Self::Executing),
            "completed" => Some(Self::Completed),
            "refund_required" => Some(Self::RefundRequired),
            "refunding" => Some(Self::Refunding),
            "refunded" => Some(Self::Refunded),
            "manual_intervention_required" => Some(Self::ManualInterventionRequired),
            "refund_manual_intervention_required" => Some(Self::RefundManualInterventionRequired),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum RouterOrderAction {
    MarketOrder(MarketOrderAction),
    LimitOrder(LimitOrderAction),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketOrderAction {
    #[serde(flatten)]
    pub order_kind: MarketOrderKind,
    pub slippage_bps: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketOrderKind {
    ExactIn {
        amount_in: String,
        min_amount_out: Option<String>,
    },
    ExactOut {
        amount_out: String,
        max_amount_in: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketOrderKindType {
    ExactIn,
    ExactOut,
}

impl MarketOrderKindType {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::ExactIn => "exact_in",
            Self::ExactOut => "exact_out",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "exact_in" => Some(Self::ExactIn),
            "exact_out" => Some(Self::ExactOut),
            _ => None,
        }
    }
}

impl MarketOrderKind {
    #[must_use]
    pub fn kind_type(&self) -> MarketOrderKindType {
        match self {
            Self::ExactIn { .. } => MarketOrderKindType::ExactIn,
            Self::ExactOut { .. } => MarketOrderKindType::ExactOut,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LimitOrderAction {
    pub input_amount: String,
    pub output_amount: String,
    pub residual_policy: LimitOrderResidualPolicy,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LimitOrderResidualPolicy {
    Refund,
}

impl LimitOrderResidualPolicy {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Refund => "refund",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "refund" => Some(Self::Refund),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouterOrder {
    pub id: Uuid,
    pub order_type: RouterOrderType,
    pub status: RouterOrderStatus,
    pub funding_vault_id: Option<Uuid>,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub recipient_address: String,
    pub refund_address: String,
    pub action: RouterOrderAction,
    pub action_timeout_at: DateTime<Utc>,
    pub idempotency_key: Option<String>,
    pub workflow_trace_id: String,
    pub workflow_parent_span_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketOrderQuote {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub recipient_address: String,
    pub provider_id: String,
    pub order_kind: MarketOrderKindType,
    pub amount_in: String,
    pub amount_out: String,
    pub min_amount_out: Option<String>,
    pub max_amount_in: Option<String>,
    pub slippage_bps: Option<u64>,
    pub provider_quote: Value,
    pub usd_valuation: Value,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LimitOrderQuote {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub source_asset: DepositAsset,
    pub destination_asset: DepositAsset,
    pub recipient_address: String,
    pub provider_id: String,
    pub input_amount: String,
    pub output_amount: String,
    pub residual_policy: LimitOrderResidualPolicy,
    pub provider_quote: Value,
    pub usd_valuation: Value,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum RouterOrderQuote {
    MarketOrder(MarketOrderQuote),
    LimitOrder(LimitOrderQuote),
}

impl RouterOrderQuote {
    #[must_use]
    pub fn id(&self) -> Uuid {
        match self {
            Self::MarketOrder(quote) => quote.id,
            Self::LimitOrder(quote) => quote.id,
        }
    }

    #[must_use]
    pub fn source_asset(&self) -> &DepositAsset {
        match self {
            Self::MarketOrder(quote) => &quote.source_asset,
            Self::LimitOrder(quote) => &quote.source_asset,
        }
    }

    #[must_use]
    pub fn destination_asset(&self) -> &DepositAsset {
        match self {
            Self::MarketOrder(quote) => &quote.destination_asset,
            Self::LimitOrder(quote) => &quote.destination_asset,
        }
    }

    #[must_use]
    pub fn recipient_address(&self) -> &str {
        match self {
            Self::MarketOrder(quote) => &quote.recipient_address,
            Self::LimitOrder(quote) => &quote.recipient_address,
        }
    }

    #[must_use]
    pub fn as_market_order(&self) -> Option<&MarketOrderQuote> {
        match self {
            Self::MarketOrder(quote) => Some(quote),
            Self::LimitOrder(_) => None,
        }
    }

    #[must_use]
    pub fn as_limit_order(&self) -> Option<&LimitOrderQuote> {
        match self {
            Self::LimitOrder(quote) => Some(quote),
            Self::MarketOrder(_) => None,
        }
    }
}

impl From<MarketOrderQuote> for RouterOrderQuote {
    fn from(quote: MarketOrderQuote) -> Self {
        Self::MarketOrder(quote)
    }
}

impl From<LimitOrderQuote> for RouterOrderQuote {
    fn from(quote: LimitOrderQuote) -> Self {
        Self::LimitOrder(quote)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RouterOrderQuoteEnvelope {
    pub quote: RouterOrderQuote,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct RouterOrderEnvelope {
    pub order: RouterOrder,
    pub quote: RouterOrderQuote,
    pub funding_vault: Option<DepositVaultEnvelope>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_secret: Option<String>,
}

impl fmt::Debug for RouterOrderEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RouterOrderEnvelope")
            .field("order", &self.order)
            .field("quote", &self.quote)
            .field("funding_vault", &self.funding_vault)
            .field(
                "cancellation_secret",
                &self.cancellation_secret.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderQuotePolicyState {
    Enabled,
    Disabled,
}

impl ProviderQuotePolicyState {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderExecutionPolicyState {
    Enabled,
    Drain,
    Disabled,
}

impl ProviderExecutionPolicyState {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Enabled => "enabled",
            Self::Drain => "drain",
            Self::Disabled => "disabled",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "enabled" => Some(Self::Enabled),
            "drain" => Some(Self::Drain),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    #[must_use]
    pub fn allows_new_execution(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderPolicy {
    pub provider: String,
    pub quote_state: ProviderQuotePolicyState,
    pub execution_state: ProviderExecutionPolicyState,
    pub reason: String,
    pub updated_by: String,
    pub updated_at: DateTime<Utc>,
}

impl ProviderPolicy {
    #[must_use]
    pub fn enabled(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            quote_state: ProviderQuotePolicyState::Enabled,
            execution_state: ProviderExecutionPolicyState::Enabled,
            reason: String::new(),
            updated_by: "default".to_string(),
            updated_at: Utc::now(),
        }
    }

    #[must_use]
    pub fn allows_new_routes(&self) -> bool {
        self.quote_state == ProviderQuotePolicyState::Enabled
            && self.execution_state.allows_new_execution()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthStatus {
    Ok,
    Down,
    Unknown,
}

impl ProviderHealthStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Down => "down",
            Self::Unknown => "unknown",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "ok" => Some(Self::Ok),
            "down" => Some(Self::Down),
            "unknown" => Some(Self::Unknown),
            _ => None,
        }
    }

    #[must_use]
    pub fn allows_new_routes(self) -> bool {
        !matches!(self, Self::Down)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderHealthCheck {
    pub provider: String,
    pub status: ProviderHealthStatus,
    pub checked_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub updated_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderHealthSummaryStatus {
    Ok,
    Degraded,
}

impl ProviderHealthSummaryStatus {
    #[must_use]
    pub fn from_checks(checks: &[ProviderHealthCheck]) -> Self {
        if checks
            .iter()
            .any(|check| matches!(check.status, ProviderHealthStatus::Down))
        {
            Self::Degraded
        } else {
            Self::Ok
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyVaultRole {
    SourceDeposit,
    DestinationExecution,
    /// Hyperliquid execution identity used for Unit deposits, spot trading,
    /// and Unit withdrawals. This is always per-order router custody.
    HyperliquidSpot,
}

impl CustodyVaultRole {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::SourceDeposit => "source_deposit",
            Self::DestinationExecution => "destination_execution",
            Self::HyperliquidSpot => "hyperliquid_spot",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "source_deposit" => Some(Self::SourceDeposit),
            "destination_execution" => Some(Self::DestinationExecution),
            "hyperliquid_spot" => Some(Self::HyperliquidSpot),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyVaultVisibility {
    UserFacing,
    Internal,
    WatchOnly,
}

impl CustodyVaultVisibility {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::UserFacing => "user_facing",
            Self::Internal => "internal",
            Self::WatchOnly => "watch_only",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "user_facing" => Some(Self::UserFacing),
            "internal" => Some(Self::Internal),
            "watch_only" => Some(Self::WatchOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyVaultControlType {
    RouterDerivedKey,
    PaymasterWallet,
    HyperliquidMasterSigner,
    ExternalUser,
    WatchOnly,
}

impl CustodyVaultControlType {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::RouterDerivedKey => "router_derived_key",
            Self::PaymasterWallet => "paymaster_wallet",
            Self::HyperliquidMasterSigner => "hyperliquid_master_signer",
            Self::ExternalUser => "external_user",
            Self::WatchOnly => "watch_only",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "router_derived_key" => Some(Self::RouterDerivedKey),
            "paymaster_wallet" => Some(Self::PaymasterWallet),
            "hyperliquid_master_signer" => Some(Self::HyperliquidMasterSigner),
            "external_user" => Some(Self::ExternalUser),
            "watch_only" => Some(Self::WatchOnly),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CustodyVaultStatus {
    Planned,
    Active,
    /// No longer reserved for order execution. This is a logical lifecycle
    /// state, not a proof that the underlying account is empty.
    Released,
    /// Terminal non-success state. Funds may still require manual or automated
    /// recovery from the underlying account.
    Failed,
}

impl CustodyVaultStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Active => "active",
            Self::Released => "released",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "planned" => Some(Self::Planned),
            "active" => Some(Self::Active),
            "released" => Some(Self::Released),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CustodyVault {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub role: CustodyVaultRole,
    pub visibility: CustodyVaultVisibility,
    pub chain: ChainId,
    pub asset: Option<AssetId>,
    pub address: String,
    pub control_type: CustodyVaultControlType,
    pub derivation_salt: Option<[u8; 32]>,
    pub signer_ref: Option<String>,
    pub status: CustodyVaultStatus,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationType {
    AcrossBridge,
    CctpBridge,
    CctpReceive,
    HyperliquidBridgeDeposit,
    HyperliquidBridgeWithdrawal,
    UnitDeposit,
    UnitWithdrawal,
    HyperliquidTrade,
    HyperliquidLimitOrder,
    UniversalRouterSwap,
}

impl ProviderOperationType {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::AcrossBridge => "across_bridge",
            Self::CctpBridge => "cctp_bridge",
            Self::CctpReceive => "cctp_receive",
            Self::HyperliquidBridgeDeposit => "hyperliquid_bridge_deposit",
            Self::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
            Self::UnitDeposit => "unit_deposit",
            Self::UnitWithdrawal => "unit_withdrawal",
            Self::HyperliquidTrade => "hyperliquid_trade",
            Self::HyperliquidLimitOrder => "hyperliquid_limit_order",
            Self::UniversalRouterSwap => "universal_router_swap",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "across_bridge" => Some(Self::AcrossBridge),
            "cctp_bridge" => Some(Self::CctpBridge),
            "cctp_receive" => Some(Self::CctpReceive),
            "hyperliquid_bridge_deposit" => Some(Self::HyperliquidBridgeDeposit),
            "hyperliquid_bridge_withdrawal" => Some(Self::HyperliquidBridgeWithdrawal),
            "unit_deposit" => Some(Self::UnitDeposit),
            "unit_withdrawal" => Some(Self::UnitWithdrawal),
            "hyperliquid_trade" => Some(Self::HyperliquidTrade),
            "hyperliquid_limit_order" => Some(Self::HyperliquidLimitOrder),
            "universal_router_swap" => Some(Self::UniversalRouterSwap),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationStatus {
    Planned,
    Submitted,
    WaitingExternal,
    Completed,
    Failed,
    Expired,
}

impl ProviderOperationStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Submitted => "submitted",
            Self::WaitingExternal => "waiting_external",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "planned" => Some(Self::Planned),
            "submitted" => Some(Self::Submitted),
            "waiting_external" => Some(Self::WaitingExternal),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationHintKind {
    PossibleProgress,
    BtcDepositObserved,
    AcrossDestinationFilled,
    CctpReceiveObserved,
    VeloraSwapSettled,
    HyperUnitDepositCredited,
    HyperUnitWithdrawalAcknowledged,
    HyperUnitWithdrawalSettled,
    HlTradeFilled,
    HlTradeCanceled,
    HlBridgeDepositObserved,
    HlBridgeDepositCredited,
    HlWithdrawalAcknowledged,
    HlWithdrawalSettled,
}

impl ProviderOperationHintKind {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::PossibleProgress => "possible_progress",
            Self::BtcDepositObserved => "btc_deposit_observed",
            Self::AcrossDestinationFilled => "across_destination_filled",
            Self::CctpReceiveObserved => "cctp_receive_observed",
            Self::VeloraSwapSettled => "velora_swap_settled",
            Self::HyperUnitDepositCredited => "hyperunit_deposit_credited",
            Self::HyperUnitWithdrawalAcknowledged => "hyperunit_withdrawal_acknowledged",
            Self::HyperUnitWithdrawalSettled => "hyperunit_withdrawal_settled",
            Self::HlTradeFilled => "hl_trade_filled",
            Self::HlTradeCanceled => "hl_trade_canceled",
            Self::HlBridgeDepositObserved => "hl_bridge_deposit_observed",
            Self::HlBridgeDepositCredited => "hl_bridge_deposit_credited",
            Self::HlWithdrawalAcknowledged => "hl_withdrawal_acknowledged",
            Self::HlWithdrawalSettled => "hl_withdrawal_settled",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "possible_progress" => Some(Self::PossibleProgress),
            "btc_deposit_observed" => Some(Self::BtcDepositObserved),
            "across_destination_filled" => Some(Self::AcrossDestinationFilled),
            "cctp_receive_observed" => Some(Self::CctpReceiveObserved),
            "velora_swap_settled" => Some(Self::VeloraSwapSettled),
            "hyperunit_deposit_credited" => Some(Self::HyperUnitDepositCredited),
            "hyperunit_withdrawal_acknowledged" => Some(Self::HyperUnitWithdrawalAcknowledged),
            "hyperunit_withdrawal_settled" => Some(Self::HyperUnitWithdrawalSettled),
            "hl_trade_filled" => Some(Self::HlTradeFilled),
            "hl_trade_canceled" => Some(Self::HlTradeCanceled),
            "hl_bridge_deposit_observed" => Some(Self::HlBridgeDepositObserved),
            "hl_bridge_deposit_credited" => Some(Self::HlBridgeDepositCredited),
            "hl_withdrawal_acknowledged" => Some(Self::HlWithdrawalAcknowledged),
            "hl_withdrawal_settled" => Some(Self::HlWithdrawalSettled),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BtcDepositObservedEvidence {
    pub tx_hash: String,
    pub address: String,
    pub transfer_index: u64,
    pub amount: String,
    pub confirmation_state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HyperUnitDepositCreditedEvidence {
    pub protocol_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_vout: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_confirmations: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_block_height: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_block_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_source_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_destination_tx_hash: Option<String>,
    pub hl_user: String,
    pub hl_amount: String,
    pub hl_credit_hash: String,
    pub hl_credit_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HyperUnitWithdrawalAcknowledgedEvidence {
    pub protocol_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination_address: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_tx_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broadcast_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HyperUnitWithdrawalSettledEvidence {
    pub protocol_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hyperunit_status: Option<String>,
    pub destination_address: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amount: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub btc_tx_hash: Option<String>,
    pub btc_vout: u64,
    pub btc_amount: String,
    pub btc_confirmations: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlTradeFilledEvidence {
    pub user: String,
    pub oid: u64,
    pub tid: u64,
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub crossed: bool,
    pub hash: String,
    pub time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlTradeCanceledEvidence {
    pub user: String,
    pub oid: u64,
    pub coin: String,
    pub status: String,
    pub status_timestamp_ms: i64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlBridgeDepositObservedEvidence {
    pub user: String,
    pub usdc: String,
    pub arb_tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlBridgeDepositCreditedEvidence {
    pub user: String,
    pub usdc: String,
    pub hl_credit_hash: String,
    pub hl_credit_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlWithdrawalAcknowledgedEvidence {
    pub user: String,
    pub usdc: String,
    pub nonce: u64,
    pub hl_request_hash: String,
    pub hl_request_time_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HlWithdrawalSettledEvidence {
    pub user: String,
    pub usdc: String,
    pub arb_payout_tx_hash: String,
    pub log_index: u64,
    pub block_number: u64,
    pub time_window_match_to_nonce: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderOperationHintStatus {
    Pending,
    Processing,
    Processed,
    Ignored,
    Failed,
}

impl ProviderOperationHintStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Processing => "processing",
            Self::Processed => "processed",
            Self::Ignored => "ignored",
            Self::Failed => "failed",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "processing" => Some(Self::Processing),
            "processed" => Some(Self::Processed),
            "ignored" => Some(Self::Ignored),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAddressRole {
    AcrossRecipient,
    AcrossRefund,
    UnitDeposit,
    UnitRevert,
    HyperliquidDestination,
}

impl ProviderAddressRole {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::AcrossRecipient => "across_recipient",
            Self::AcrossRefund => "across_refund",
            Self::UnitDeposit => "unit_deposit",
            Self::UnitRevert => "unit_revert",
            Self::HyperliquidDestination => "hyperliquid_destination",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "across_recipient" => Some(Self::AcrossRecipient),
            "across_refund" => Some(Self::AcrossRefund),
            "unit_deposit" => Some(Self::UnitDeposit),
            "unit_revert" => Some(Self::UnitRevert),
            "hyperliquid_destination" => Some(Self::HyperliquidDestination),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderExecutionAttemptKind {
    PrimaryExecution,
    RetryExecution,
    RefreshedExecution,
    RefundRecovery,
}

impl OrderExecutionAttemptKind {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::PrimaryExecution => "primary_execution",
            Self::RetryExecution => "retry_execution",
            Self::RefreshedExecution => "refreshed_execution",
            Self::RefundRecovery => "refund_recovery",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "primary_execution" => Some(Self::PrimaryExecution),
            "retry_execution" => Some(Self::RetryExecution),
            "refreshed_execution" => Some(Self::RefreshedExecution),
            "refund_recovery" => Some(Self::RefundRecovery),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderExecutionAttemptStatus {
    Planning,
    Active,
    Completed,
    Failed,
    RefundRequired,
    ManualInterventionRequired,
    Superseded,
}

impl OrderExecutionAttemptStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::RefundRequired => "refund_required",
            Self::ManualInterventionRequired => "manual_intervention_required",
            Self::Superseded => "superseded",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "planning" => Some(Self::Planning),
            "active" => Some(Self::Active),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "refund_required" => Some(Self::RefundRequired),
            "manual_intervention_required" => Some(Self::ManualInterventionRequired),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderExecutionAttempt {
    pub id: Uuid,
    pub order_id: Uuid,
    pub attempt_index: i32,
    pub attempt_kind: OrderExecutionAttemptKind,
    pub status: OrderExecutionAttemptStatus,
    pub trigger_step_id: Option<Uuid>,
    pub trigger_provider_operation_id: Option<Uuid>,
    pub failure_reason: Value,
    pub input_custody_snapshot: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderProviderOperation {
    pub id: Uuid,
    pub order_id: Uuid,
    pub execution_attempt_id: Option<Uuid>,
    pub execution_step_id: Option<Uuid>,
    pub provider: String,
    pub operation_type: ProviderOperationType,
    pub provider_ref: Option<String>,
    pub status: ProviderOperationStatus,
    pub request: Value,
    pub response: Value,
    pub observed_state: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderProviderOperationHint {
    pub id: Uuid,
    pub provider_operation_id: Uuid,
    pub source: String,
    pub hint_kind: ProviderOperationHintKind,
    pub evidence: Value,
    pub status: ProviderOperationHintStatus,
    pub idempotency_key: Option<String>,
    pub error: Value,
    pub claimed_at: Option<DateTime<Utc>>,
    pub processed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

pub const PROVIDER_OPERATION_OBSERVATION_HINT_SOURCE: &str =
    "sauron_provider_operation_observation";
pub const SAURON_DETECTOR_HINT_SOURCE: &str = "sauron";
pub const SAURON_EVM_RECEIPT_OBSERVER_HINT_SOURCE: &str = "sauron_evm_receipt_observer";
pub const SAURON_HYPERLIQUID_OBSERVER_HINT_SOURCE: &str = "sauron_hyperliquid_observer";
pub const SAURON_HYPERUNIT_OBSERVER_HINT_SOURCE: &str = "sauron_hyperunit_observer";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DepositVaultFundingHint {
    pub id: Uuid,
    pub vault_id: Uuid,
    pub source: String,
    pub hint_kind: ProviderOperationHintKind,
    pub evidence: Value,
    pub status: ProviderOperationHintStatus,
    pub idempotency_key: Option<String>,
    pub error: Value,
    pub claimed_at: Option<DateTime<Utc>>,
    pub processed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderProviderAddress {
    pub id: Uuid,
    pub order_id: Uuid,
    pub execution_step_id: Option<Uuid>,
    pub provider_operation_id: Option<Uuid>,
    pub provider: String,
    pub role: ProviderAddressRole,
    pub chain: ChainId,
    pub asset: Option<AssetId>,
    pub address: String,
    pub memo: Option<String>,
    pub expires_at: Option<DateTime<Utc>>,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderExecutionStepType {
    WaitForDeposit,
    AcrossBridge,
    CctpBurn,
    CctpReceive,
    HyperliquidBridgeDeposit,
    HyperliquidBridgeWithdrawal,
    UnitDeposit,
    UnitWithdrawal,
    HyperliquidTrade,
    HyperliquidLimitOrder,
    UniversalRouterSwap,
    Refund,
}

impl OrderExecutionStepType {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::WaitForDeposit => "wait_for_deposit",
            Self::AcrossBridge => "across_bridge",
            Self::CctpBurn => "cctp_burn",
            Self::CctpReceive => "cctp_receive",
            Self::HyperliquidBridgeDeposit => "hyperliquid_bridge_deposit",
            Self::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
            Self::UnitDeposit => "unit_deposit",
            Self::UnitWithdrawal => "unit_withdrawal",
            Self::HyperliquidTrade => "hyperliquid_trade",
            Self::HyperliquidLimitOrder => "hyperliquid_limit_order",
            Self::UniversalRouterSwap => "universal_router_swap",
            Self::Refund => "refund",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "wait_for_deposit" => Some(Self::WaitForDeposit),
            "across_bridge" => Some(Self::AcrossBridge),
            "cctp_burn" => Some(Self::CctpBurn),
            "cctp_receive" => Some(Self::CctpReceive),
            "hyperliquid_bridge_deposit" => Some(Self::HyperliquidBridgeDeposit),
            "hyperliquid_bridge_withdrawal" => Some(Self::HyperliquidBridgeWithdrawal),
            "unit_deposit" => Some(Self::UnitDeposit),
            "unit_withdrawal" => Some(Self::UnitWithdrawal),
            "hyperliquid_trade" => Some(Self::HyperliquidTrade),
            "hyperliquid_limit_order" => Some(Self::HyperliquidLimitOrder),
            "universal_router_swap" => Some(Self::UniversalRouterSwap),
            "refund" => Some(Self::Refund),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OrderExecutionStepStatus {
    Planned,
    Waiting,
    Ready,
    Running,
    Completed,
    Failed,
    Skipped,
    Cancelled,
    Superseded,
}

impl OrderExecutionStepStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Waiting => "waiting",
            Self::Ready => "ready",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
            Self::Cancelled => "cancelled",
            Self::Superseded => "superseded",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "planned" => Some(Self::Planned),
            "waiting" => Some(Self::Waiting),
            "ready" => Some(Self::Ready),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            "cancelled" => Some(Self::Cancelled),
            "superseded" => Some(Self::Superseded),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderExecutionStep {
    pub id: Uuid,
    pub order_id: Uuid,
    pub execution_attempt_id: Option<Uuid>,
    pub execution_leg_id: Option<Uuid>,
    pub transition_decl_id: Option<String>,
    pub step_index: i32,
    pub step_type: OrderExecutionStepType,
    pub provider: String,
    pub status: OrderExecutionStepStatus,
    pub input_asset: Option<DepositAsset>,
    pub output_asset: Option<DepositAsset>,
    pub amount_in: Option<String>,
    pub min_amount_out: Option<String>,
    pub tx_hash: Option<String>,
    pub provider_ref: Option<String>,
    pub idempotency_key: Option<String>,
    pub attempt_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub details: Value,
    pub request: Value,
    pub response: Value,
    pub error: Value,
    pub usd_valuation: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderExecutionLeg {
    pub id: Uuid,
    pub order_id: Uuid,
    pub execution_attempt_id: Option<Uuid>,
    pub transition_decl_id: Option<String>,
    pub leg_index: i32,
    pub leg_type: String,
    pub provider: String,
    pub status: OrderExecutionStepStatus,
    pub input_asset: DepositAsset,
    pub output_asset: DepositAsset,
    pub amount_in: String,
    pub expected_amount_out: String,
    pub min_amount_out: Option<String>,
    pub actual_amount_in: Option<String>,
    pub actual_amount_out: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub provider_quote_expires_at: Option<DateTime<Utc>>,
    pub details: Value,
    pub usd_valuation: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DepositVaultStatus {
    PendingFunding,
    Funded,
    Executing,
    Completed,
    RefundRequired,
    Refunding,
    Refunded,
    ManualInterventionRequired,
    RefundManualInterventionRequired,
}

impl DepositVaultStatus {
    #[must_use]
    pub fn to_db_string(self) -> &'static str {
        match self {
            Self::PendingFunding => "pending_funding",
            Self::Funded => "funded",
            Self::Executing => "executing",
            Self::Completed => "completed",
            Self::RefundRequired => "refund_required",
            Self::Refunding => "refunding",
            Self::Refunded => "refunded",
            Self::ManualInterventionRequired => "manual_intervention_required",
            Self::RefundManualInterventionRequired => "refund_manual_intervention_required",
        }
    }

    pub fn from_db_string(value: &str) -> Option<Self> {
        match value {
            "pending_funding" => Some(Self::PendingFunding),
            "funded" => Some(Self::Funded),
            "executing" => Some(Self::Executing),
            "completed" => Some(Self::Completed),
            "refund_required" => Some(Self::RefundRequired),
            "refunding" => Some(Self::Refunding),
            "refunded" => Some(Self::Refunded),
            "manual_intervention_required" => Some(Self::ManualInterventionRequired),
            "refund_manual_intervention_required" => Some(Self::RefundManualInterventionRequired),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositVault {
    pub id: Uuid,
    pub order_id: Option<Uuid>,
    pub deposit_asset: DepositAsset,
    pub action: VaultAction,
    pub metadata: Value,
    #[serde(with = "alloy::hex::serde")]
    pub deposit_vault_salt: [u8; 32],
    pub deposit_vault_address: String,
    pub recovery_address: String,
    pub cancellation_commitment: String,
    pub cancel_after: DateTime<Utc>,
    pub status: DepositVaultStatus,
    pub refund_requested_at: Option<DateTime<Utc>>,
    pub refunded_at: Option<DateTime<Utc>>,
    pub refund_tx_hash: Option<String>,
    pub last_refund_error: Option<String>,
    pub funding_observation: Option<DepositVaultFundingObservation>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositVaultFundingObservation {
    pub tx_hash: Option<String>,
    pub sender_address: Option<String>,
    pub sender_addresses: Vec<String>,
    pub recipient_address: Option<String>,
    pub transfer_index: Option<u64>,
    pub observed_amount: Option<String>,
    pub confirmation_state: Option<String>,
    pub observed_at: Option<DateTime<Utc>>,
    pub evidence: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositRequirements {
    pub minimum_confirmations: u32,
    pub estimated_block_time_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositVaultEnvelope {
    pub vault: DepositVault,
    pub deposit_requirements: DepositRequirements,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    pub status: String,
    pub version: String,
    pub supported_chains: Vec<ChainId>,
    pub supported_actions: Vec<VaultAction>,
}
