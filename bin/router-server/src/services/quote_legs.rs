use crate::{
    models::OrderExecutionStepType,
    protocol::{AssetId, ChainId, DepositAsset},
    services::asset_registry::{MarketOrderTransitionKind, ProviderId},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuoteLegAsset {
    pub chain_id: String,
    pub asset: String,
}

impl QuoteLegAsset {
    #[must_use]
    pub fn from_deposit_asset(asset: &DepositAsset) -> Self {
        Self {
            chain_id: asset.chain.as_str().to_string(),
            asset: asset.asset.as_str().to_string(),
        }
    }

    pub fn from_value(value: &Value, field: &'static str) -> Result<Self, String> {
        let obj = value
            .as_object()
            .ok_or_else(|| format!("quote leg {field} must be an object"))?;
        let chain_id = obj
            .get("chain_id")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("quote leg {field} missing chain_id"))?;
        let asset = obj
            .get("asset")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("quote leg {field} missing asset"))?;
        Ok(Self {
            chain_id: chain_id.to_string(),
            asset: asset.to_string(),
        })
    }

    pub fn deposit_asset(&self) -> Result<DepositAsset, String> {
        DepositAsset {
            chain: ChainId::parse(&self.chain_id)
                .map_err(|err| format!("quote leg asset chain_id {:?}: {err}", self.chain_id))?,
            asset: AssetId::parse(&self.asset)
                .map_err(|err| format!("quote leg asset {:?}: {err}", self.asset))?,
        }
        .normalized_asset_identity()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteLeg {
    pub transition_decl_id: String,
    pub transition_parent_decl_id: String,
    pub transition_kind: MarketOrderTransitionKind,
    pub execution_step_type: OrderExecutionStepType,
    pub provider: ProviderId,
    pub input_asset: QuoteLegAsset,
    pub output_asset: QuoteLegAsset,
    pub amount_in: String,
    pub amount_out: String,
    pub expires_at: DateTime<Utc>,
    pub raw: Value,
}

pub struct QuoteLegSpec<'a> {
    pub transition_decl_id: &'a str,
    pub transition_kind: MarketOrderTransitionKind,
    pub provider: ProviderId,
    pub input_asset: &'a DepositAsset,
    pub output_asset: &'a DepositAsset,
    pub amount_in: &'a str,
    pub amount_out: &'a str,
    pub expires_at: DateTime<Utc>,
    pub raw: Value,
}

impl QuoteLeg {
    #[must_use]
    pub fn new(spec: QuoteLegSpec<'_>) -> Self {
        let transition_decl_id = spec.transition_decl_id.to_string();
        Self {
            transition_parent_decl_id: transition_decl_id.clone(),
            transition_decl_id,
            transition_kind: spec.transition_kind,
            execution_step_type: execution_step_type_for_transition_kind(spec.transition_kind),
            provider: spec.provider,
            input_asset: QuoteLegAsset::from_deposit_asset(spec.input_asset),
            output_asset: QuoteLegAsset::from_deposit_asset(spec.output_asset),
            amount_in: spec.amount_in.to_string(),
            amount_out: spec.amount_out.to_string(),
            expires_at: spec.expires_at,
            raw: spec.raw,
        }
    }

    #[must_use]
    pub fn with_child_transition_id(mut self, child_transition_decl_id: impl Into<String>) -> Self {
        self.transition_decl_id = child_transition_decl_id.into();
        self
    }

    #[must_use]
    pub fn with_execution_step_type(mut self, execution_step_type: OrderExecutionStepType) -> Self {
        self.execution_step_type = execution_step_type;
        self
    }

    #[must_use]
    pub fn parent_transition_id(&self) -> &str {
        &self.transition_parent_decl_id
    }

    pub fn input_deposit_asset(&self) -> Result<DepositAsset, String> {
        self.input_asset.deposit_asset()
    }

    pub fn output_deposit_asset(&self) -> Result<DepositAsset, String> {
        self.output_asset.deposit_asset()
    }
}

#[must_use]
pub fn execution_step_type_for_transition_kind(
    transition_kind: MarketOrderTransitionKind,
) -> OrderExecutionStepType {
    match transition_kind {
        MarketOrderTransitionKind::AcrossBridge => OrderExecutionStepType::AcrossBridge,
        MarketOrderTransitionKind::CctpBridge => OrderExecutionStepType::CctpBurn,
        MarketOrderTransitionKind::UnitDeposit => OrderExecutionStepType::UnitDeposit,
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => {
            OrderExecutionStepType::HyperliquidBridgeDeposit
        }
        MarketOrderTransitionKind::HyperliquidTrade => OrderExecutionStepType::HyperliquidTrade,
        MarketOrderTransitionKind::UniversalRouterSwap => {
            OrderExecutionStepType::UniversalRouterSwap
        }
        MarketOrderTransitionKind::UnitWithdrawal => OrderExecutionStepType::UnitWithdrawal,
    }
}
